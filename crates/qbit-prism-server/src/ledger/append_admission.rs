//! Keep share transactions from occupying every connection while they wait
//! for ORDER_LOCK. This is admission to one pool, not an accounting lock.
use sqlx::{pool::PoolConnection, PgPool, Postgres, Transaction};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct PoolAdmission {
    pool: PgPool,
    permits: Arc<Semaphore>,
}

// Ledger::pool is public: independently constructed ledgers can be pointed
// at the same pool. Key by the shared pool itself, not Ledger or URL (two
// independently sized pools may use the same URL). Only outstanding appends
// and their cleanup own entries; the registry never keeps an idle pool alive.
// Ceiling: one scan of this list per append under a std mutex, and the list
// holds one entry per pool with an outstanding append, which is a handful.
// If Ledger::pool ever becomes private, replace this with a per-ledger field.
static POOLS: Mutex<Vec<Weak<PoolAdmission>>> = Mutex::new(Vec::new());

fn shared(pool: &PgPool) -> Arc<PoolAdmission> {
    let mut pools = POOLS.lock().expect("append admission registry");
    pools.retain(|entry| entry.strong_count() != 0);
    for admission in pools.iter().filter_map(Weak::upgrade) {
        // SQLx stores PoolOptions inside the shared PoolInner. Holding the
        // pool in each live entry keeps this identity valid until cleanup.
        if std::ptr::eq(admission.pool.options(), pool.options()) {
            return admission;
        }
    }
    // Frontend construction enforces at least two connections. Reserve one
    // for non-append work rather than tuning a throughput-dependent constant.
    // A caller-supplied single-connection pool can still append serially,
    // although such a pool cannot provide concurrent read headroom.
    let capacity = pool
        .options()
        .get_max_connections()
        .saturating_sub(1)
        .max(1);
    let admission = Arc::new(PoolAdmission {
        pool: pool.clone(),
        permits: Arc::new(Semaphore::new(capacity as usize)),
    });
    pools.push(Arc::downgrade(&admission));
    admission
}

pub(super) struct Admission {
    _pool: Arc<PoolAdmission>,
    _permit: OwnedSemaphorePermit,
    // The runtime that polled this append. Cancellation can drop the guard
    // below from a thread without an entered runtime context, where an
    // ambient tokio::spawn panics; the captured handle cannot.
    runtime: tokio::runtime::Handle,
}

impl Admission {
    /// This wait remains inside the caller's original submission deadline.
    /// Cancellation before checkout releases only the queued admission; no
    /// SQL has started. In particular, retry does not create a new deadline.
    pub(super) async fn acquire(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let admission = shared(pool);
        let permit = tokio::select! {
            _ = pool.close_event() => return Err(sqlx::Error::PoolClosed),
            permit = admission.permits.clone().acquire_owned() => {
                permit.expect("append admission semaphore is never closed")
            }
        };
        Ok(Self {
            _pool: admission,
            _permit: permit,
            // Always polled on a runtime, so this cannot panic here.
            runtime: tokio::runtime::Handle::current(),
        })
    }

    pub(super) fn attach(self, connection: PoolConnection<Postgres>) -> AppendConnection {
        AppendConnection {
            connection: Some(connection),
            began: false,
            admission: Some(self),
        }
    }
}

pub(super) struct AppendConnection {
    // The transaction borrows this connection. Its drop must queue rollback
    // before this guard hands the connection and permit to SQLx cleanup.
    // Taken by `Drop`; present for the guard's whole life before that.
    connection: Option<PoolConnection<Postgres>>,
    // Whether `BEGIN` completed. From then on the transaction's own drop
    // queues its `ROLLBACK`; before that, the server may hold a transaction
    // this client has not counted (see `begin`).
    began: bool,
    admission: Option<Admission>,
}

impl AppendConnection {
    /// `statement` opens the transaction on this connection, borrowed so
    /// that the guard keeps owning the checkout and its permit.
    ///
    /// SQLx counts the transaction only once the reply to `BEGIN` is read
    /// (`sqlx-postgres` 0.8.6, `PgTransactionManager::begin`), and its
    /// cancellation guard rolls back nothing before that. A persist task
    /// aborted at the share-commit deadline, or dropped by a miner
    /// disconnect, between the write and the reply would therefore hand the
    /// pool a connection whose server side is still in a transaction, and
    /// the next checkout's `BEGIN` would land inside it (#482). The guard
    /// records completion here; until then, `Drop` retires the connection
    /// instead of returning it.
    ///
    /// `statement` may carry more than `BEGIN` (the append sends its
    /// `SET LOCAL` in the same simple query). SQLx sends it as one simple
    /// query and reads every reply up to its `ReadyForQuery` before it
    /// returns, so completion is recorded only once the whole round trip is
    /// in; a cancel between any two of its replies still retires the
    /// connection.
    pub(super) async fn begin(
        &mut self,
        statement: &'static str,
    ) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
        let connection = self.connection.as_mut().expect("append connection");
        let transaction = Transaction::begin(&mut **connection, Some(statement.into())).await?;
        self.began = true;
        Ok(transaction)
    }
}

impl Drop for AppendConnection {
    fn drop(&mut self) {
        let admission = self.admission.take().expect("append connection admission");
        let runtime = admission.runtime.clone();
        let Some(mut connection) = self.connection.take() else {
            return;
        };
        if !self.began {
            // `BEGIN` never completed: its reply was not read (the append was
            // cancelled between the write and the reply) or the server
            // refused it. The server may be inside a transaction SQLx would
            // not roll back, so this connection must not be offered to the
            // next checkout. `detach` leaves the pool synchronously, size
            // and permit included, so the pool opens a replacement and can
            // never see this connection again, whether or not the close
            // below ever runs. The graceful close sends `Terminate`; a close
            // that is never polled (runtime shut down) drops the raw
            // connection, which closes the socket, and the server aborts
            // whatever it held. The admission permit is released with it.
            let raw = connection.detach();
            runtime.spawn(async move {
                let _ = sqlx::Connection::close(raw).await;
                drop(admission);
            });
            return;
        }
        // Transaction::drop only QUEUES rollback. SQLx's normal pool return
        // flushes the pending protocol, including rollback or an uncertain
        // COMMIT reply, before releasing/closing the connection. Keep the
        // permit until that same cleanup finishes, even if the caller was
        // cancelled. Do not alter its errors, warnings or transaction state.
        //
        // `return_to_pool` is SQLx's own drop path made callable (0.8.x,
        // `#[doc(hidden)]`): it floats the connection out of the pool
        // handle synchronously, so a cleanup future that is never polled
        // (runtime already shut down) still releases the pool slot and, by
        // ownership, the permit. Re-check this on any SQLx upgrade. SQLx's
        // own PoolConnection::drop then sees no live connection and spawns
        // nothing for the ledger pool, which sets no min_connections.
        let cleanup = connection.return_to_pool();
        runtime.spawn(async move {
            cleanup.await;
            drop(admission);
        });
    }
}

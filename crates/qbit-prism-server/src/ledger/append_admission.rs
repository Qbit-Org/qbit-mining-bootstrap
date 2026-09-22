//! Keep share transactions from occupying every connection while they wait
//! for ORDER_LOCK. This is admission to one pool, not an accounting lock.
use sqlx::{pool::PoolConnection, PgPool, Postgres};
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
        })
    }

    pub(super) fn attach(self, connection: PoolConnection<Postgres>) -> AppendConnection {
        AppendConnection {
            connection,
            admission: Some(self),
        }
    }
}

pub(super) struct AppendConnection {
    // The transaction borrows this connection. Its drop must queue rollback
    // before this guard hands the connection and permit to SQLx cleanup.
    pub(super) connection: PoolConnection<Postgres>,
    admission: Option<Admission>,
}

impl Drop for AppendConnection {
    fn drop(&mut self) {
        let admission = self.admission.take().expect("append connection admission");
        // Transaction::drop only QUEUES rollback. SQLx's normal pool return
        // flushes the pending protocol, including rollback or an uncertain
        // COMMIT reply, before releasing/closing the connection. Keep the
        // permit until that same cleanup finishes, even if the caller was
        // cancelled. Do not alter its errors, warnings or transaction state.
        let cleanup = self.connection.return_to_pool();
        tokio::spawn(async move {
            cleanup.await;
            drop(admission);
        });
    }
}

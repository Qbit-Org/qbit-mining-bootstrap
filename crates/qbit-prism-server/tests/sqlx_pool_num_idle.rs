//! sqlx-core's pool counts a released connection as idle before another task
//! can take it: the launchbadge/sqlx#4289 backport in vendor/sqlx-core.
//!
//! 0.8.6's `release()` pushed the connection onto the idle queue and released
//! its permit before it added one to `num_idle`. A task that took the
//! connection in between subtracted first, and with the count at 0 it wrapped
//! to `usize::MAX` until the add landed. The pool's maintenance task reads
//! the count once as the bound of its sweep, and a sweep that finds nothing
//! to check never awaits, so one that read the wrapped count kept its runtime
//! worker for good.
//!
//! These tests open that window without a race. The pool's semaphore wakes a
//! waiting `acquire()` from inside `release()`, as it hands over the permit:
//! after the push and, in 0.8.6, before the add. The tests poll a pending
//! `acquire()` by hand with their own waker, and that waker polls it again
//! where it is called, so the waiter takes the connection at the very point
//! where another worker's acquire could. The pool is sqlx's Postgres pool, as
//! the server uses, against a socket that answers only what a pool asks.
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{PgPool, Postgres};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const AUTHENTICATION_OK: &[u8] = b"R\0\0\0\x08\0\0\0\0";
const READY_FOR_QUERY: &[u8] = b"Z\0\0\0\x05I";
/// Bounds each wait for something a working pool does within milliseconds.
const PATIENCE: Duration = Duration::from_secs(30);

/// Serves the PostgreSQL protocol as far as a pool goes, and returns its port
/// and the count of connections it has accepted.
async fn serve_postgres() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(serve_connection(socket));
        }
    });
    (port, accepted)
}

/// Accepts any startup, then answers each Sync, which is sqlx's ping, with
/// ReadyForQuery, until the client sends Terminate or hangs up.
async fn serve_connection(mut socket: TcpStream) -> io::Result<()> {
    // The startup message has no type byte: a length that counts itself, then
    // the parameters.
    let length = socket.read_u32().await? as usize;
    socket
        .read_exact(&mut vec![0; length.saturating_sub(4)])
        .await?;
    socket.write_all(AUTHENTICATION_OK).await?;
    socket.write_all(READY_FOR_QUERY).await?;
    loop {
        let kind = socket.read_u8().await?;
        let length = socket.read_u32().await? as usize;
        socket
            .read_exact(&mut vec![0; length.saturating_sub(4)])
            .await?;
        if kind != b'S' {
            return Ok(());
        }
        socket.write_all(READY_FOR_QUERY).await?;
    }
}

/// A pool of one connection. With `idle_timeout` it runs a maintenance task
/// at that period, and without one it runs none.
fn pool_of_one(port: u16, idle_timeout: Option<Duration>) -> PgPool {
    let server = PgConnectOptions::new()
        .host("127.0.0.1")
        .port(port)
        .username("prism")
        .database("prism")
        .ssl_mode(PgSslMode::Disable);
    PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        // Taking an idle connection then awaits nothing, so a waiter finishes
        // inside the waker that polls it.
        .test_before_acquire(false)
        .max_lifetime(None)
        .idle_timeout(idle_timeout)
        .connect_lazy_with(server)
}

type Acquire = Pin<Box<dyn Future<Output = sqlx::Result<PoolConnection<Postgres>>> + Send>>;

/// A waiting `acquire()` that is its own waker. Waking it polls the acquire
/// where the waker is called, which for the pool's semaphore is inside
/// `release()`.
struct Handoff {
    pool: PgPool,
    /// How long the waker stays in its caller once the acquire has finished.
    hold: Duration,
    acquire: Mutex<Option<Acquire>>,
    taken: Mutex<Option<(usize, sqlx::Result<PoolConnection<Postgres>>)>>,
}

impl Handoff {
    /// Starts an `acquire()` on a pool whose one connection is checked out.
    fn waiting_on(pool: &PgPool, hold: Duration) -> Arc<Self> {
        let handoff = Arc::new(Self {
            pool: pool.clone(),
            hold,
            acquire: Mutex::new(None),
            taken: Mutex::new(None),
        });
        let mut acquire: Acquire = Box::pin(pool.acquire());
        let waker = Waker::from(handoff.clone());
        assert!(
            acquire
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending(),
            "the pool's one connection is checked out, so a second acquire waits"
        );
        *handoff.acquire.lock().unwrap() = Some(acquire);
        handoff
    }

    /// What the pool counted as idle when the waiter took the connection, and
    /// the connection, once the waker is about to return to `release()`.
    fn try_taken(&self) -> Option<(usize, PoolConnection<Postgres>)> {
        let (idle, connection) = self.taken.lock().unwrap().take()?;
        Some((idle, connection.expect("the waiter takes the connection")))
    }
}

impl Wake for Handoff {
    fn wake(self: Arc<Self>) {
        let Some(mut acquire) = self.acquire.lock().unwrap().take() else {
            return;
        };
        let waker = Waker::from(self.clone());
        match acquire.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(connection) => {
                let idle = self.pool.num_idle();
                std::thread::sleep(self.hold);
                *self.taken.lock().unwrap() = Some((idle, connection));
            }
            Poll::Pending => *self.acquire.lock().unwrap() = Some(acquire),
        }
    }
}

/// A waiter that takes a connection from inside `release()` leaves the pool
/// counting no idle connection. 0.8.6 counted `usize::MAX` there.
#[tokio::test]
async fn a_connection_taken_inside_release_leaves_the_idle_count_at_zero() {
    let (port, accepted) = serve_postgres().await;
    let pool = pool_of_one(port, None);
    let held = pool.acquire().await.unwrap();
    let handoff = Handoff::waiting_on(&pool, Duration::ZERO);

    drop(held);
    let taken = async {
        loop {
            if let Some(taken) = handoff.try_taken() {
                return taken;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let (idle, _connection) = tokio::time::timeout(PATIENCE, taken)
        .await
        .expect("releasing the pool's connection wakes the waiting acquire");

    assert_eq!(
        idle, 0,
        "the pool has one connection and the waiter holds it, so none is idle"
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        1,
        "the waiter took the released connection; it did not open another"
    );
}

/// Polls `done` until it holds, for as long as [`PATIENCE`].
fn within_patience(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + PATIENCE;
    while !done() {
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    true
}

/// The maintenance task still closes an idle connection after one of its
/// sweeps has run while a waiter was taking a connection inside `release()`.
/// In 0.8.6 that sweep read `usize::MAX` as its bound and never ended: it
/// kept the worker that had been driving the runtime's timers and sockets,
/// and nothing on that runtime ran again.
#[test]
fn maintenance_still_reaps_after_a_sweep_during_a_handoff() {
    const IDLE_TIMEOUT: Duration = Duration::from_millis(200);
    let runtime = || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    };
    // The pool, its maintenance task and the server run on one runtime. The
    // connection is released on another, so that the waker holds that one's
    // worker inside `release()` and the pool's runtime keeps its timers.
    let (pools, releases) = (runtime(), runtime());

    let (pool, held) = pools.block_on(async {
        let (port, _) = serve_postgres().await;
        let pool = pool_of_one(port, Some(IDLE_TIMEOUT));
        let held = pool.acquire().await.unwrap();
        (pool, held)
    });
    // The maintenance task sweeps once per idle timeout, so it sweeps while
    // the waker holds.
    let handoff = {
        let _pools = pools.enter();
        Handoff::waiting_on(&pool, 3 * IDLE_TIMEOUT)
    };

    releases.spawn(async move { drop(held) });
    let mut connection = None;
    let handed_over = within_patience(|| {
        connection = handoff.try_taken();
        connection.is_some()
    });
    releases.spawn(async move { drop(connection) });
    // Nothing uses the pool now, so only the maintenance task can close its
    // idle connection.
    let reaped = within_patience(|| pool.size() == 0);
    // A sweep that never ends never yields, and dropping its runtime would
    // wait for it.
    pools.shutdown_background();
    releases.shutdown_background();

    assert!(
        handed_over,
        "releasing the pool's connection wakes the waiting acquire"
    );
    assert!(
        reaped,
        "the maintenance task closes a connection left idle past the idle timeout"
    );
}

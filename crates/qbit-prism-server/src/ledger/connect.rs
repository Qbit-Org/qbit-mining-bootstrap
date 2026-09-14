use super::*;
use crate::metrics::{LockKind, Metrics, Outcome};

const SESSION_ALLOCATION_ATTEMPTS: usize = 1024;

#[derive(Debug, thiserror::Error)]
#[error("no free session extranonce found after 1024 allocation attempts; retry subscription")]
pub struct SessionAllocationExhausted;

#[derive(Debug, Default)]
struct SessionOwnerState {
    stopped: bool,
    active: usize,
}

#[derive(Debug)]
pub(super) struct SessionOwner {
    token: String,
    state: std::sync::Mutex<SessionOwnerState>,
}

#[derive(Debug)]
struct ActiveSession(std::sync::Arc<SessionOwner>);

impl SessionOwner {
    #[cfg(test)]
    fn new_for_tests() -> Self {
        Self {
            token: Uuid::new_v4().to_string(),
            state: std::sync::Mutex::default(),
        }
    }
    fn start(self: &std::sync::Arc<Self>) -> Result<ActiveSession> {
        let mut state = self.state.lock().unwrap();
        ensure!(!state.stopped, "session allocator is stopped");
        state.active += 1;
        Ok(ActiveSession(self.clone()))
    }

    fn stop(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        ensure!(
            state.active == 0,
            "cannot report stopped with active or pending sessions"
        );
        state.stopped = true;
        Ok(())
    }
}

impl Drop for ActiveSession {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().active -= 1;
    }
}

/// Owns a four-byte extranonce for the lifetime of a subscribed connection.
/// This is deliberately not Clone: dropping the owner releases its reservation.
#[derive(Debug)]
#[must_use = "retain the reservation guard for the entire subscribed session"]
pub struct SessionId {
    id: u32,
    reservation: Option<(PgPool, String)>,
    _active: Option<ActiveSession>,
}

impl SessionId {
    pub fn value(&self) -> u32 {
        self.id
    }

    /// Release after a caller has finished using this session, awaiting the
    /// durable cleanup. Drop provides the cancellation/disconnect fallback.
    pub async fn release(mut self) -> Result<()> {
        if let Some((pool, token)) = &self.reservation {
            sqlx::query("DELETE FROM qbit_prism_session_reservations WHERE extranonce1=$1 AND reservation_token=$2")
                .bind(i64::from(self.id)).bind(token).execute(pool).await?;
            self.reservation = None;
        }
        Ok(())
    }
}

/// Backends without a PostgreSQL ledger can supply their own unique ID.
impl From<u32> for SessionId {
    fn from(id: u32) -> Self {
        Self {
            id,
            reservation: None,
            _active: None,
        }
    }
}

impl std::fmt::LowerHex for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::LowerHex::fmt(&self.id, f)
    }
}

impl Drop for SessionId {
    fn drop(&mut self) {
        let Some((pool, token)) = self.reservation.take() else {
            return;
        };
        let id = self.id;
        // Shutdown/cancellation must not release somebody else's replacement.
        // A lost commit response or failed cleanup can retain a reservation;
        // this is safe, and stopped-owner reclamation handles normal shutdown.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = sqlx::query("DELETE FROM qbit_prism_session_reservations WHERE extranonce1=$1 AND reservation_token=$2")
                    .bind(i64::from(id)).bind(token).execute(&pool).await {
                    tracing::warn!(%error, "session reservation cleanup deferred");
                }
            });
        }
    }
}

impl Ledger {
    #[cfg(test)]
    pub(crate) fn offline_for_tests(pool: PgPool, instance_id: String) -> Self {
        Self {
            pool,
            instance_id,
            session_owner: std::sync::Arc::new(SessionOwner::new_for_tests()),
            metrics: None,
        }
    }

    /// Begin a ledger transaction, recording this ledger's pool acquisition.
    pub(super) async fn begin(&self) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
        begin(&self.pool, self.metrics.as_deref()).await
    }

    /// Take one advisory lock, recording this ledger's wait for it.
    pub(super) async fn lock(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        key: i64,
    ) -> Result<(), sqlx::Error> {
        lock(tx, key, self.metrics.as_deref()).await
    }

    /// Connect without native telemetry: nothing is recorded and behaviour is
    /// identical to [`Ledger::connect_with_metrics`] with `None`.
    pub async fn connect(
        url: &str,
        instance_id: String,
        max_connections: u32,
        initialize: bool,
    ) -> Result<Self> {
        Self::connect_with_metrics(url, instance_id, max_connections, initialize, None).await
    }

    /// Connect and, when a registry is supplied, record every advisory-lock
    /// wait and transaction pool acquisition this ledger performs. Passing
    /// `None` records nothing and leaves behaviour and results identical.
    pub async fn connect_with_metrics(
        url: &str,
        instance_id: String,
        max_connections: u32,
        initialize: bool,
        metrics: Option<std::sync::Arc<Metrics>>,
    ) -> Result<Self> {
        ensure!(!instance_id.is_empty(), "instance ID must not be empty");
        let timeout_setting = |name: &str, default: u64| -> Result<String> {
            let millis = std::env::var(name)
                .ok()
                .map(|value| value.parse::<u64>())
                .transpose()?
                .unwrap_or(default);
            ensure!(
                (1..=600_000).contains(&millis),
                "{name} must be between 1 and 600000 milliseconds"
            );
            Ok(millis.to_string())
        };
        let statement_timeout = timeout_setting("PRISM_DATABASE_STATEMENT_TIMEOUT_MS", 15_000)?;
        let lock_timeout = timeout_setting("PRISM_DATABASE_LOCK_TIMEOUT_MS", 5_000)?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(2))
            .acquire_timeout(std::time::Duration::from_secs(15))
            .after_connect(move |connection,_| {
                let statement_timeout = statement_timeout.clone();
                let lock_timeout = lock_timeout.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('statement_timeout',$1,false),set_config('lock_timeout',$2,false),set_config('synchronous_commit',CASE WHEN current_setting('synchronous_commit')='remote_apply' THEN 'remote_apply' ELSE 'on' END,false)")
                        .bind(statement_timeout).bind(lock_timeout).execute(&mut *connection).await?;
                    let durable:bool=sqlx::query_scalar("SELECT current_setting('fsync')='on' AND current_setting('full_page_writes')='on'").fetch_one(&mut *connection).await?;
                    if !durable {return Err(sqlx::Error::Protocol("PostgreSQL fsync and full_page_writes must be enabled for durable share acknowledgement".into()));}
                    Ok(())
                })
            })
            .connect(url)
            .await?;
        if initialize {
            let mut tx = begin(&pool, metrics.as_deref()).await?;
            migration::migrate_schema(&mut tx, &instance_id, metrics.as_deref()).await?;
            tx.commit().await?;
        }
        // The startup gate. Every start, with or without `initialize`, reads
        // the schema version and the declared capabilities before any
        // accounting statement: a newer binary never reaches the claim path
        // on a database it has not migrated, and an older binary is kept off
        // a format it must not touch by the capabilities a release declares,
        // not by the version number, so a rollout can replace one frontend
        // at a time.
        migration::require_schema_version(&pool).await?;
        migration::require_known_capabilities(&pool).await?;
        let ledger = Self {
            pool,
            instance_id,
            session_owner: std::sync::Arc::new(SessionOwner {
                token: Uuid::new_v4().to_string(),
                state: std::sync::Mutex::default(),
            }),
            metrics,
        };
        let source = migration::require_migration_source(&ledger.pool).await?;
        tracing::info!(
            state = %source.source_state,
            release = ?source.source_release,
            prior_schema_version = source.prior_schema_version,
            "PRISM database source"
        );
        let mut tx = ledger.begin().await?;
        writable(&mut tx).await?;
        tx.commit().await?;
        ledger
            .heartbeat(serde_json::json!({"state":"starting"}))
            .await?;
        Ok(ledger)
    }

    /// Every server in a cluster must agree on consensus, payout and signing
    /// configuration. The fingerprint excludes local ports and instance IDs.
    pub async fn configure(&self, fingerprint: &str) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let saved: Option<String> = sqlx::query_scalar(
            "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if let Some(saved) = saved {
            ensure!(
                saved == fingerprint,
                "cluster configuration fingerprint mismatch"
            );
        } else {
            sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1 WHERE singleton")
                .bind(fingerprint)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn heartbeat(&self, mut status: Value) -> Result<()> {
        // The stopped marker is proof about this process incarnation only.
        // Closing admission and checking pending/active guards happen under
        // one local mutex, before awaiting SQL; no new session can race it.
        if status.get("state").and_then(Value::as_str) == Some("stopped") {
            self.session_owner.stop()?;
        }
        status
            .as_object_mut()
            .context("heartbeat status must be an object")?
            .insert(
                "session_owner_token".into(),
                self.session_owner.token.clone().into(),
            );
        sqlx::query("INSERT INTO qbit_prism_instances(instance_id,status) VALUES($1,$2) ON CONFLICT(instance_id) DO UPDATE SET heartbeat_at=clock_timestamp(),status=EXCLUDED.status")
            .bind(&self.instance_id).bind(status).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn release_session_owner_reservations(&self) -> Result<()> {
        sqlx::query("DELETE FROM qbit_prism_session_reservations WHERE owner_token=$1")
            .bind(&self.session_owner.token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Reserve a four-byte extranonce across every frontend, including at wrap.
    /// The caller must retain the returned guard for the entire session.
    pub async fn new_session_id(&self) -> Result<SessionId> {
        // Count even an allocation still waiting for a connection/commit, so
        // shutdown cannot publish stopped ahead of an in-flight allocation.
        let active = self.session_owner.start()?;
        for _ in 0..SESSION_ALLOCATION_ATTEMPTS {
            // Each attempt has its own short transaction. The unique key,
            // rather than an extra global lock, arbitrates wrapped candidates.
            let mut tx = self.begin().await?;
            let id: i64 = sqlx::query_scalar("SELECT nextval('qbit_prism_session_sequence')")
                .fetch_one(&mut *tx)
                .await?;
            let id = u32::try_from(id)?;
            let token = Uuid::new_v4().to_string();
            let reserved = sqlx::query("INSERT INTO qbit_prism_session_reservations AS held (extranonce1,instance_id,owner_token,reservation_token) VALUES($1,$2,$3,$4) ON CONFLICT(extranonce1) DO UPDATE SET instance_id=EXCLUDED.instance_id,owner_token=EXCLUDED.owner_token,reservation_token=EXCLUDED.reservation_token,created_at=clock_timestamp() WHERE EXISTS (SELECT 1 FROM qbit_prism_instances owner WHERE owner.instance_id=held.instance_id AND owner.status->>'state'='stopped' AND owner.status->>'session_owner_token'=held.owner_token)")
                .bind(i64::from(id)).bind(&self.instance_id).bind(&self.session_owner.token).bind(&token)
                .execute(&mut *tx).await?.rows_affected();
            if reserved == 0 {
                tx.rollback().await?;
                continue;
            }
            let referenced: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM qbit_prism_jobs WHERE lower(payload->>'extranonce1')=$1 AND expires_at>clock_timestamp())")
                .bind(format!("{id:08x}")).fetch_one(&mut *tx).await?;
            if referenced {
                tx.rollback().await?;
                continue;
            }
            // Until now cancellation only rolls back an uncommitted attempt.
            // Arm cleanup before commit; a lost commit response can at worst
            // retain this token, never allow an unsafe reuse.
            let session = SessionId {
                id,
                reservation: Some((self.pool.clone(), token)),
                _active: Some(active),
            };
            tx.commit().await?;
            return Ok(session);
        }
        Err(SessionAllocationExhausted.into())
    }

    pub async fn payout_revision(&self) -> Result<i64> {
        Ok(sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'").fetch_one(&self.pool).await?)
    }
}

/// Which wait a [`WaitGuard`] is timing, and the label it records under.
#[derive(Clone, Copy)]
enum WaitKind {
    AdvisoryLock(LockKind),
    PoolAcquire,
}

/// Times one wait and records exactly one observation for it.
///
/// A wait that ends normally is recorded by [`WaitGuard::complete`], which also
/// disarms the guard. A wait whose future is dropped first — a cancelled share
/// append, a `tokio::time::timeout` that elapses — is recorded as a failure
/// with the time actually spent waiting, so cancellation stays distinct from
/// "no wait happened". The guard itself allocates nothing; the labels are
/// allocated inside the metrics API when it records. The metrics mutex is
/// taken only after the wait has ended, never across an `.await`.
struct WaitGuard<'a> {
    metrics: Option<&'a Metrics>,
    kind: WaitKind,
    started: std::time::Instant,
}

impl<'a> WaitGuard<'a> {
    /// Start the clock. Callers construct a guard only when a handle exists,
    /// so an unattached ledger reads no clock and records nothing.
    fn arm(metrics: &'a Metrics, kind: WaitKind) -> Self {
        Self {
            metrics: Some(metrics),
            kind,
            started: std::time::Instant::now(),
        }
    }

    /// Record the completed wait and disarm. Taking the handle out first makes
    /// the disarm unconditional, so `Drop` cannot record a second observation.
    fn complete(mut self, result: Outcome) {
        let elapsed = self.started.elapsed();
        if let Some(metrics) = self.metrics.take() {
            record(metrics, self.kind, result, elapsed);
        }
    }
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        if let Some(metrics) = self.metrics.take() {
            record(metrics, self.kind, Outcome::Failure, elapsed);
        }
    }
}

fn record(metrics: &Metrics, kind: WaitKind, result: Outcome, elapsed: std::time::Duration) {
    match kind {
        WaitKind::AdvisoryLock(lock) => metrics.observe_advisory_lock(lock, result, elapsed),
        WaitKind::PoolAcquire => metrics.observe_pool_acquire(result, elapsed),
    }
}

/// The `LockKind` each advisory lock key records under, or `None` for a key
/// that is deliberately not observed.
fn lock_kind(key: i64) -> Option<LockKind> {
    match key {
        MIGRATION_LOCK => Some(LockKind::Migration),
        ORDER_LOCK => Some(LockKind::Order),
        SETTLEMENT_LOCK => Some(LockKind::Settlement),
        // not observed: no LockKind value; see #328
        super::fanout::CPFP_FUNDING_LOCK => None,
        _ => {
            debug_assert!(false, "advisory lock key {key:#018x} has no LockKind");
            None
        }
    }
}

/// The error type is `sqlx::Error`, exactly what the raw statement returns, so
/// every call site's `?` converts as it did before this helper existed.
pub(super) async fn lock(
    tx: &mut Transaction<'_, Postgres>,
    key: i64,
    metrics: Option<&Metrics>,
) -> Result<(), sqlx::Error> {
    // Time the advisory lock statement and nothing else: the clock starts
    // immediately before the wait begins.
    let guard = match (metrics, lock_kind(key)) {
        (Some(metrics), Some(kind)) => Some(WaitGuard::arm(metrics, WaitKind::AdvisoryLock(kind))),
        _ => None,
    };
    let acquired = sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut **tx)
        .await;
    if let Some(guard) = guard {
        guard.complete(if acquired.is_ok() {
            Outcome::Success
        } else {
            Outcome::Failure
        });
    }
    acquired?;
    Ok(())
}

/// Begin a ledger transaction, timing the pool acquisition.
///
/// This is `Pool::begin` split in two around the timer (sqlx 0.8.6,
/// `pool/mod.rs`), so it is a drop-in replacement down to the error type. Only
/// `acquire` is timed: the `BEGIN` round trip stays outside the timer, so these
/// observations keep the same meaning as the metrics collector's own.
async fn begin(
    pool: &PgPool,
    metrics: Option<&Metrics>,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    let guard = metrics.map(|metrics| WaitGuard::arm(metrics, WaitKind::PoolAcquire));
    let acquired = pool.acquire().await;
    if let Some(guard) = guard {
        guard.complete(if acquired.is_ok() {
            Outcome::Success
        } else {
            Outcome::Failure
        });
    }
    Transaction::begin(acquired?, None).await
}

pub(super) async fn writable(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let row = sqlx::query("SELECT fatal_error,EXISTS(SELECT 1 FROM qbit_ledger_writer_lease WHERE lease_expires_at>clock_timestamp()) AS legacy_live FROM qbit_prism_cluster WHERE singleton").fetch_one(&mut **tx).await?;
    let fatal: Option<String> = row.try_get("fatal_error")?;
    if let Some(error) = fatal {
        bail!("cluster halted: {error}");
    }
    ensure!(
        !row.try_get::<bool, _>("legacy_live")?,
        "live legacy Python writer lease"
    );
    Ok(())
}

pub(super) async fn require_revision(
    tx: &mut Transaction<'_, Postgres>,
    expected: i64,
) -> Result<()> {
    let revision: i64 =
        sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(&mut **tx)
            .await?;
    ensure!(
        revision == expected,
        "payout revision changed while observing chain state"
    );
    Ok(())
}

#[cfg(test)]
mod lock_kind_tests {
    use super::*;

    /// Every key the ledger takes maps to its own label, and the one key with
    /// no label value stays unobserved rather than borrowing another's.
    #[test]
    fn every_advisory_lock_key_maps_to_its_own_label_or_to_none() {
        assert_eq!(lock_kind(MIGRATION_LOCK), Some(LockKind::Migration));
        assert_eq!(lock_kind(ORDER_LOCK), Some(LockKind::Order));
        assert_eq!(lock_kind(SETTLEMENT_LOCK), Some(LockKind::Settlement));
        assert_eq!(lock_kind(super::super::fanout::CPFP_FUNDING_LOCK), None);
    }
}

use super::*;

impl Ledger {
    pub async fn connect(
        url: &str,
        instance_id: String,
        max_connections: u32,
        initialize: bool,
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
            let mut tx = pool.begin().await?;
            lock(&mut tx, MIGRATION_LOCK).await?;
            sqlx::raw_sql("CREATE TABLE IF NOT EXISTS qbit_prism_schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())").execute(&mut *tx).await?;
            // Match PR319's membership approach: independently reserved
            // migrations must not hide a lower numbered, unapplied migration.
            let versions: Vec<i32> =
                sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations")
                    .fetch_all(&mut *tx)
                    .await?;
            if !versions.contains(&3) {
                // Existing native writers use this same lock order. Keep the
                // schema repair and cutover atomic with their accounting.
                lock(&mut tx, SETTLEMENT_LOCK).await?;
                lock(&mut tx, ORDER_LOCK).await?;
                let lease_exists: bool = sqlx::query_scalar(
                    "SELECT to_regclass('qbit_ledger_writer_lease') IS NOT NULL",
                )
                .fetch_one(&mut *tx)
                .await?;
                if lease_exists {
                    // The table lock also closes the race with a legacy process
                    // trying to reacquire its lease during the cutover.
                    sqlx::query("LOCK TABLE qbit_ledger_writer_lease IN ACCESS EXCLUSIVE MODE")
                        .execute(&mut *tx)
                        .await?;
                    let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_ledger_writer_lease WHERE lease_expires_at > clock_timestamp())").fetch_one(&mut *tx).await?;
                    ensure!(!live, "live legacy Python writer lease: stop the Python deployment and release or wait for its lease before Rust migration");
                }
                let outbox_exists: bool = sqlx::query_scalar(
                    "SELECT to_regclass('qbit_block_candidate_outbox') IS NOT NULL",
                )
                .fetch_one(&mut *tx)
                .await?;
                if outbox_exists {
                    let legacy_pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE state='pending' AND NOT(candidate ?& ARRAY['payout_revision','bundle','block_hash']))").fetch_one(&mut *tx).await?;
                    ensure!(!legacy_pending,"legacy Python block outbox is not drained; restart the legacy submitter and finish pending candidates before Rust migration");
                }
                let base_schema = migration::base_schema_transaction_body(include_str!(
                    "../../../qbit-prism/sql/001_share_ledger.sql"
                ))?;
                sqlx::raw_sql(&base_schema).execute(&mut *tx).await?;
                if !versions.contains(&2) {
                    sqlx::raw_sql(include_str!("../../migrations/002_multi_instance.sql"))
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
                        .execute(&mut *tx)
                        .await?;
                }
                sqlx::raw_sql(include_str!("../../migrations/003_2x_compatibility.sql"))
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(3)")
                    .execute(&mut *tx)
                    .await?;
            }
            if !versions.contains(&4) {
                sqlx::raw_sql(include_str!(
                    "../../migrations/004_cpfp_retired_funding.sql"
                ))
                .execute(&mut *tx)
                .await?;
                sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(4)")
                    .execute(&mut *tx)
                    .await?;
            }
            if !versions.contains(&5) {
                sqlx::raw_sql(include_str!("../../migrations/005_candidate_dispatch.sql"))
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(5)")
                    .execute(&mut *tx)
                    .await?;
            }
            if !versions.contains(&8) {
                sqlx::raw_sql(include_str!(
                    "../../migrations/008_prepared_window_reference.sql"
                ))
                .execute(&mut *tx)
                .await?;
                sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(8)")
                    .execute(&mut *tx)
                    .await?;
            }
            tx.commit().await?;
        }
        let ledger = Self { pool, instance_id };
        let mut tx = ledger.pool.begin().await?;
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
        let mut tx = self.pool.begin().await?;
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

    pub async fn heartbeat(&self, status: Value) -> Result<()> {
        sqlx::query("INSERT INTO qbit_prism_instances(instance_id,status) VALUES($1,$2) ON CONFLICT(instance_id) DO UPDATE SET heartbeat_at=clock_timestamp(),status=EXCLUDED.status")
            .bind(&self.instance_id).bind(status).execute(&self.pool).await?;
        Ok(())
    }

    /// Globally unique four-byte extranonce1; the sequence never cycles.
    pub async fn new_session_id(&self) -> Result<u32> {
        let id: i64 = sqlx::query_scalar("SELECT nextval('qbit_prism_session_sequence')")
            .fetch_one(&self.pool)
            .await?;
        Ok(u32::try_from(id)?)
    }

    pub async fn payout_revision(&self) -> Result<i64> {
        Ok(sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'").fetch_one(&self.pool).await?)
    }
}

pub(super) async fn lock(tx: &mut Transaction<'_, Postgres>, key: i64) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
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

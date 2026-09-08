//! Durable coordination for interchangeable PRISM servers.
//!
//! Proof-of-work verification is concurrent. Only assignment of the canonical
//! share order and capture of a job's accounting boundary are serialized. The
//! locks are transaction scoped, so disconnects and database failover release
//! ownership without an application lease election.
use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Utc};
use qbit_prism::{AcceptedShare, AuditBundle, CarryForwardBalance};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Postgres, Row, Transaction,
};
use uuid::Uuid;

mod blocks;
pub use blocks::{BlockObservation, FanoutClaim, PoolBlock};
mod audit;
pub use audit::{audit_canonical_bytes, materialize_audit_row};
mod difficulty;
mod fanout;
mod migration;
pub use difficulty::WorkerDifficulty;

const MIGRATION_LOCK: i64 = 0x505249534d000001;
const ORDER_LOCK: i64 = 0x505249534d000002;
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;

#[derive(Clone)]
pub struct Ledger {
    pub pool: PgPool,
    pub instance_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Candidate {
    pub block_hash: String,
    pub block_hex: String,
    pub job_id: String,
    pub payout_revision: i64,
    pub bundle: AuditBundle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coinbase_suffix_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred_share: Option<AcceptedShare>,
}

#[derive(Clone, Debug)]
pub struct CandidateClaim {
    pub candidate: Candidate,
    pub claim_token: String,
}

#[derive(Clone, Debug)]
pub struct AppendResult {
    pub share: AcceptedShare,
    pub inserted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub anchor_ms: i64,
    pub share_seq: u64,
    pub payout_revision: i64,
    pub shares: Vec<AcceptedShare>,
    pub prior_balances: Vec<CarryForwardBalance>,
}

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
            let version: Option<i32> =
                sqlx::query_scalar("SELECT max(version) FROM qbit_prism_schema_migrations")
                    .fetch_one(&mut *tx)
                    .await?;
            if version.unwrap_or(0) < 3 {
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
                    "../../qbit-prism/sql/001_share_ledger.sql"
                ))?;
                sqlx::raw_sql(&base_schema).execute(&mut *tx).await?;
                if version.unwrap_or(0) < 2 {
                    sqlx::raw_sql(include_str!("../migrations/002_multi_instance.sql"))
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
                        .execute(&mut *tx)
                        .await?;
                }
                sqlx::raw_sql(include_str!("../migrations/003_2x_compatibility.sql"))
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(3)")
                    .execute(&mut *tx)
                    .await?;
            }
            if version.unwrap_or(0) < 4 {
                sqlx::raw_sql(include_str!("../migrations/004_cpfp_retired_funding.sql"))
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(4)")
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

    /// Coordinate nodes by cumulative proof of work. A slower peer or an
    /// equal-work sibling cannot reverse another instance's accepted chain.
    pub async fn observe_chain_view(
        &self,
        tip: &str,
        height: u64,
        chainwork_hex: &str,
    ) -> Result<i64> {
        ensure!(
            tip.len() == 64 && tip.bytes().all(|c| c.is_ascii_hexdigit()),
            "invalid chain tip hash"
        );
        ensure!(
            !chainwork_hex.is_empty()
                && chainwork_hex.len() <= 64
                && chainwork_hex.bytes().all(|c| c.is_ascii_hexdigit()),
            "invalid cumulative chainwork"
        );
        let work = num_bigint::BigUint::parse_bytes(chainwork_hex.as_bytes(), 16)
            .context("invalid chainwork")?;
        ensure!(
            work > num_bigint::BigUint::from(0u8),
            "chainwork must be positive"
        );
        let work = work.to_str_radix(10);
        let height = i64::try_from(height)?;
        let tip = tip.to_ascii_lowercase();
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        let row=sqlx::query("SELECT payout_revision,best_chainwork=$1::text::numeric AS same_work,best_chainwork<$1::text::numeric AS more_work,best_tip_hash,best_tip_height FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .bind(&work).fetch_one(&mut *tx).await?;
        let same: bool = row.try_get("same_work")?;
        let greater: bool = row.try_get("more_work")?;
        ensure!(
            greater || same,
            "local node is behind the cluster's cumulative chainwork"
        );
        let mut revision: i64 = row.try_get("payout_revision")?;
        if same {
            ensure!(
                row.try_get::<Option<String>, _>("best_tip_hash")?
                    .as_deref()
                    == Some(&tip)
                    && row.try_get::<Option<i64>, _>("best_tip_height")? == Some(height),
                "local node follows a conflicting equal-work chain tip"
            );
        } else {
            revision=sqlx::query_scalar("UPDATE qbit_prism_cluster SET best_chainwork=$1::text::numeric,best_tip_hash=$2,best_tip_height=$3,payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton RETURNING payout_revision")
                .bind(work).bind(tip).bind(height).fetch_one(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(revision)
    }

    pub async fn append(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
    ) -> Result<AppendResult> {
        self.append_checked(share, candidate, None).await
    }

    pub async fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        expected_revision: i64,
    ) -> Result<AppendResult> {
        self.append_checked(share, candidate, Some(expected_revision))
            .await
    }

    async fn append_checked(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        expected_revision: Option<i64>,
    ) -> Result<AppendResult> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        if let Some(expected) = expected_revision {
            let revision:i64=sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL FOR SHARE").fetch_one(&mut *tx).await?;
            ensure!(
                revision == expected,
                "payout revision changed before share commit"
            );
        }
        let result = self.append_in(&mut tx, share).await?;
        if let Some(candidate) = candidate {
            ensure!(
                candidate.deferred_share.is_none(),
                "credited candidates cannot also contain a deferred share"
            );
            persist_candidate(&mut tx, &candidate, Some(&result.share.share_id)).await?;
        }
        tx.commit().await?;
        Ok(result)
    }

    async fn append_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut share: AcceptedShare,
    ) -> Result<AppendResult> {
        ensure!(
            share.share_difficulty > 0 && share.network_difficulty > 0,
            "share difficulty must be positive"
        );
        ensure!(
            share
                .credit_policy
                .as_deref()
                .is_none_or(|p| p == "stale-grace"),
            "invalid share credit policy"
        );
        let program = hex::decode(&share.p2mr_program_hex)?;
        ensure!(program.len() == 32, "P2MR program must be 32 bytes");
        share.p2mr_program_hex = hex::encode(program);
        let existing = sqlx::query(&format!("{SELECT_SHARE} WHERE share_id=$1"))
            .bind(&share.share_id)
            .fetch_optional(&mut **tx)
            .await?;
        if let Some(row) = existing {
            let previous = share_from_row(&row)?;
            share.share_seq = previous.share_seq;
            share.accepted_at_ms = previous.accepted_at_ms;
            ensure!(share == previous, "duplicate share_id payload mismatch");
            return Ok(AppendResult {
                share: previous,
                inserted: false,
            });
        }
        let header_hash = share_header_hash(&share.share_id);
        let duplicate: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_prism_share_hashes WHERE header_hash=$1)",
        )
        .bind(&header_hash)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            !duplicate,
            "duplicate-share: header already credited globally"
        );
        let accepted_at_ms: i64 = sqlx::query_scalar("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint) WHERE singleton RETURNING ledger_clock_ms").fetch_one(&mut **tx).await?;
        ensure!(
            share.job_issued_at_ms <= accepted_at_ms,
            "share references a job from the future"
        );
        let seq: i64 = sqlx::query_scalar("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) VALUES($1,$2,$3,decode($4,'hex'),$5::text::numeric,$6::text::numeric,$7,$8,to_timestamp($9::double precision/1000),$10,to_timestamp($11::double precision/1000),$12,true,$13,0) RETURNING share_seq")
            .bind(&share.share_id).bind(&share.miner_id).bind(&share.order_key).bind(&share.p2mr_program_hex)
            .bind(share.share_difficulty.to_string()).bind(share.network_difficulty.to_string()).bind(i64::try_from(share.template_height)?)
            .bind(&share.job_id).bind(share.job_issued_at_ms).bind(i64::from(share.ntime)).bind(accepted_at_ms).bind(&share.credit_policy).bind(&self.instance_id)
            .fetch_one(&mut **tx).await?;
        sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,$2)")
            .bind(header_hash)
            .bind(&share.share_id)
            .execute(&mut **tx)
            .await?;
        share.share_seq = u64::try_from(seq)?;
        share.accepted_at_ms = accepted_at_ms;
        Ok(AppendResult {
            share,
            inserted: true,
        })
    }

    /// Captures all three inputs under the same database boundary: ordered
    /// shares, prior balances and their revision. Timestamp barriers preserve
    /// the existing public audit format without relying on host clock sync.
    pub async fn snapshot(&self, network_difficulty: u128) -> Result<Snapshot> {
        let weight = network_difficulty
            .checked_mul(8)
            .context("window difficulty overflow")?;
        ensure!(weight > 0, "network difficulty must be positive");
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let row = sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint)+1 WHERE singleton RETURNING ledger_clock_ms-1 AS anchor_ms,payout_revision").fetch_one(&mut *tx).await?;
        let anchor_ms: i64 = row.try_get("anchor_ms")?;
        let payout_revision: i64 = row.try_get("payout_revision")?;
        let cutoff: i64 = sqlx::query_scalar(
            "SELECT COALESCE(max(share_seq),0) FROM qbit_share_ledger WHERE accepted",
        )
        .fetch_one(&mut *tx)
        .await?;
        let prior_balances = read_prior_balances(&mut tx).await?;
        tx.commit().await?;
        // Ledger rows are immutable and later commits receive a timestamp
        // strictly greater than this anchor. Release the ordering barrier
        // before scanning a potentially large payout window.
        let mut tx = self.pool.begin().await?;
        let mut shares = Vec::new();
        let mut remaining = weight;
        let mut cursor = cutoff.checked_add(1).context("share sequence exhausted")?;
        while remaining > 0 {
            let rows = sqlx::query(&format!("{SELECT_SHARE} WHERE accepted AND share_seq<$1 AND accepted_at<=to_timestamp($2::double precision/1000) AND job_issued_at<=to_timestamp($2::double precision/1000) ORDER BY share_seq DESC LIMIT 4096"))
                .bind(cursor).bind(anchor_ms).fetch_all(&mut *tx).await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let share = share_from_row(&row)?;
                cursor = i64::try_from(share.share_seq)?;
                remaining = remaining.saturating_sub(share.share_difficulty);
                shares.push(share);
                if remaining == 0 {
                    break;
                }
            }
        }
        shares.reverse();
        tx.commit().await?;
        Ok(Snapshot {
            anchor_ms,
            share_seq: u64::try_from(cutoff)?,
            payout_revision,
            shares,
            prior_balances,
        })
    }

    pub async fn save_job(
        &self,
        job_id: &str,
        payload: &Value,
        payout_revision: i64,
        parent_hash: &str,
        ttl_seconds: i64,
    ) -> Result<()> {
        ensure!(ttl_seconds > 0, "job TTL must be positive");
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            revision == payout_revision,
            "payout revision changed during job construction"
        );
        let inserted = sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at) VALUES($1,$2,$3,$4,$5,clock_timestamp()+$6*interval '1 second') ON CONFLICT DO NOTHING")
            .bind(job_id).bind(&self.instance_id).bind(parent_hash).bind(payout_revision).bind(payload).bind(ttl_seconds).execute(&mut *tx).await?.rows_affected();
        if inserted == 0 {
            let same: bool = sqlx::query_scalar("SELECT payload=$2 AND parent_hash=$3 AND payout_revision=$4 FROM qbit_prism_jobs WHERE job_id=$1").bind(job_id).bind(payload).bind(parent_hash).bind(payout_revision).fetch_one(&mut *tx).await?;
            ensure!(same, "immutable job ID conflict");
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn job(&self, job_id: &str) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT payload FROM qbit_prism_jobs WHERE job_id=$1 AND expires_at>clock_timestamp()",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn enqueue_candidate(&self, candidate: Candidate) -> Result<()> {
        self.enqueue_candidate_once(candidate).await.map(|_| ())
    }

    pub async fn enqueue_candidate_once(&self, candidate: Candidate) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash=$1)",
        )
        .bind(&candidate.block_hash)
        .fetch_one(&mut *tx)
        .await?;
        if exists {
            tx.commit().await?;
            return Ok(false);
        }
        let inserted = persist_candidate(&mut tx, &candidate, None).await?;
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn claim_candidate(&self, lease_seconds: i64) -> Result<Option<CandidateClaim>> {
        ensure!(lease_seconds > 0, "claim duration must be positive");
        let token = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        let row = sqlx::query("WITH next AS (SELECT block_hash FROM qbit_block_candidate_outbox WHERE state='pending' AND next_attempt_at<=clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()) ORDER BY created_at,block_hash FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE qbit_block_candidate_outbox o SET claim_token=$1,claim_instance_id=$2,claim_expires_at=clock_timestamp()+$3*interval '1 second',attempt_count=attempt_count+1,updated_at=clock_timestamp() FROM next WHERE o.block_hash=next.block_hash RETURNING candidate,candidate_sha256")
            .bind(&token).bind(&self.instance_id).bind(lease_seconds).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        row.map(|row| {
            let candidate: Candidate = serde_json::from_value(row.try_get("candidate")?)
                .context("invalid persisted candidate")?;
            let digest: String = row.try_get("candidate_sha256")?;
            ensure!(
                hex::encode(Sha256::digest(serde_json::to_vec(&candidate)?)) == digest,
                "persisted candidate digest mismatch"
            );
            Ok(CandidateClaim {
                candidate,
                claim_token: token,
            })
        })
        .transpose()
    }

    pub async fn retry_candidate(&self, claim: &CandidateClaim, error: &str) -> Result<()> {
        let result = sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,last_error=$3,next_attempt_at=clock_timestamp()+LEAST(60,attempt_count)*interval '1 second',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending'")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(error).execute(&self.pool).await?;
        ensure!(result.rows_affected() == 1, "candidate claim was lost");
        Ok(())
    }

    pub async fn candidate_revision_valid(&self, candidate: &Candidate) -> Result<bool> {
        Ok(candidate.payout_revision == self.payout_revision().await?)
    }

    pub async fn prune_expired_jobs(&self) -> Result<u64> {
        Ok(sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id IN (SELECT job_id FROM qbit_prism_jobs WHERE expires_at < clock_timestamp() ORDER BY expires_at LIMIT 4096)").execute(&self.pool).await?.rows_affected())
    }
}

async fn lock(tx: &mut Transaction<'_, Postgres>, key: i64) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn writable(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
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

async fn require_revision(tx: &mut Transaction<'_, Postgres>, expected: i64) -> Result<()> {
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

async fn persist_candidate(
    tx: &mut Transaction<'_, Postgres>,
    candidate: &Candidate,
    share_id: Option<&str>,
) -> Result<bool> {
    let header = hex::decode(&candidate.block_hex)?;
    ensure!(header.len() > 80, "candidate block is truncated");
    let mut hash = Sha256::digest(Sha256::digest(&header[..80])).to_vec();
    hash.reverse();
    ensure!(
        hex::encode(hash) == candidate.block_hash,
        "candidate header hash mismatch"
    );
    let payload = serde_json::to_value(candidate)?;
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(candidate)?));
    let inserted = sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,share_id,candidate,candidate_sha256) VALUES($1,$2,$3,$4) ON CONFLICT(block_hash) DO NOTHING")
        .bind(&candidate.block_hash).bind(share_id).bind(payload).bind(&digest).execute(&mut **tx).await?.rows_affected();
    if inserted == 0 {
        let same: bool = sqlx::query_scalar("SELECT candidate_sha256=$2 AND share_id IS NOT DISTINCT FROM $3 FROM qbit_block_candidate_outbox WHERE block_hash=$1").bind(&candidate.block_hash).bind(&digest).bind(share_id).fetch_one(&mut **tx).await?;
        ensure!(same, "candidate identity conflict");
    }
    if let Some(share) = &candidate.deferred_share {
        let payload = serde_json::to_value(share)?;
        let digest = hex::encode(Sha256::digest(serde_json::to_vec(share)?));
        sqlx::query("INSERT INTO qbit_prism_deferred_shares(block_hash,share,share_sha256) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(&candidate.block_hash).bind(&payload).bind(&digest).execute(&mut **tx).await?;
        let same:bool = sqlx::query_scalar("SELECT share=$2 AND share_sha256=$3 FROM qbit_prism_deferred_shares WHERE block_hash=$1")
            .bind(&candidate.block_hash).bind(payload).bind(digest).fetch_one(&mut **tx).await?;
        ensure!(same, "deferred share identity conflict");
    }
    Ok(inserted == 1)
}

async fn read_prior_balances(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<CarryForwardBalance>> {
    sqlx::query("SELECT miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,balance_sats::text AS balance FROM qbit_current_carry_forward_balances()")
        .fetch_all(&mut **tx).await?.into_iter().map(|row| Ok(CarryForwardBalance {
            recipient_id: row.try_get("miner_id")?, order_key: row.try_get("payout_order_key")?,
            p2mr_program_hex: row.try_get("program")?, balance_sats: row.try_get::<String,_>("balance")?.parse()?,
        })).collect()
}

fn share_header_hash(share_id: &str) -> String {
    if let Some(suffix) = share_id.get(share_id.len().saturating_sub(64)..) {
        if suffix.len() == 64 && suffix.bytes().all(|b| b.is_ascii_hexdigit()) {
            return suffix.to_ascii_lowercase();
        }
    }
    hex::encode(Sha256::digest(share_id.as_bytes()))
}

const SELECT_SHARE: &str = "SELECT share_seq,share_id,miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,share_difficulty::text AS difficulty,network_difficulty::text AS network_difficulty,template_height,job_id,job_issued_at,accepted_at,ntime,credit_policy FROM qbit_share_ledger";

fn share_from_row(row: &PgRow) -> Result<AcceptedShare> {
    Ok(AcceptedShare {
        share_seq: u64::try_from(row.try_get::<i64, _>("share_seq")?)?,
        share_id: row.try_get("share_id")?,
        miner_id: row.try_get("miner_id")?,
        order_key: row.try_get("payout_order_key")?,
        p2mr_program_hex: row.try_get("program")?,
        share_difficulty: row.try_get::<String, _>("difficulty")?.parse()?,
        network_difficulty: row.try_get::<String, _>("network_difficulty")?.parse()?,
        template_height: u64::try_from(row.try_get::<i64, _>("template_height")?)?,
        job_id: row.try_get("job_id")?,
        job_issued_at_ms: row
            .try_get::<DateTime<Utc>, _>("job_issued_at")?
            .timestamp_millis(),
        accepted_at_ms: row
            .try_get::<DateTime<Utc>, _>("accepted_at")?
            .timestamp_millis(),
        ntime: u32::try_from(row.try_get::<i64, _>("ntime")?)?,
        credit_policy: row.try_get("credit_policy")?,
    })
}

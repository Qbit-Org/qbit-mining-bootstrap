//! Single-execution and landing-interleaving regression coverage for the
//! native ledger (#272).
//!
//! Every test drives public `Ledger` operations and reads durable state back
//! through a direct connection. The two timeout tests additionally observe
//! the wire between one ledger pool and PostgreSQL through
//! `support/ledger_execution_proxy.rs`, so "the terminal write ran once" is a
//! count of the statement in `Execute`/`Query` frames across every
//! connection, not an inference from rows, and every server rejection in the
//! same window is accounted for, so a replay the server refused (at `Execute`
//! or at `Parse`) is seen even though no row or counted execution records it.
//! The statement under observation is identified at run time by a marker
//! NOTICE that a statement-level fixture trigger raises on the durable table,
//! never by its SQL text or its position in the transaction. Each test body
//! runs under [`Database::run`], which closes its pools, finishes its proxy
//! and drops its schema even when the body fails or panics.
//!
//! Faults are seeded inside each test's disposable schema: a real server
//! `statement_timeout`, armed for the rest of the transaction by the first
//! fixture-observed write and consumed by a row-level stall on the targeted
//! write, and a withheld acknowledgement (before and after `COMMIT`) at the
//! proxy. Production code is not touched.
use anyhow::{ensure, Context, Result};
use futures_util::FutureExt;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, verify_audit_bundle_with_ledger_public_key, AcceptedShare, AuditBundle,
    FoundBlock, PayoutPolicy,
};
use qbit_prism_server::ledger::{BlockObservation, Candidate, Ledger, Snapshot};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

#[path = "support/ledger_execution_proxy.rs"]
mod proxy;
use proxy::{
    target_executions, target_statement_count, Execution, ExecutionProxy, Fault, FaultPhase,
    Outcome, RejectedFrame, Rejection,
};

const OUTBOX: &str = "qbit_block_candidate_outbox";
const ATTEMPTS: &str = "qbit_ctv_fanout_broadcast_attempts";
/// Statement timeout the fixture arms for the targeted transaction, and the
/// stall that makes the targeted write exceed it. The stall is interrupted by
/// the timeout, so a test pays the timeout, not the stall.
const ARMED_TIMEOUT_MS: i32 = 500;
const STALL_SECONDS: f64 = 2.0;
const CALL_BUDGET: Duration = Duration::from_secs(30);

/// Fixture objects in the disposable schema. `prism_execution_observe` is a
/// statement-level trigger: it raises the marker NOTICE the proxy attaches to
/// the in-flight execution, and, while a stall budget remains, arms a short
/// `statement_timeout` for the rest of the transaction from any observed table
/// other than the stall target. `prism_execution_stall` is a row-level trigger
/// on the targeted row: it sleeps past that timeout for as many executions as
/// the budget allows. The budget is a sequence, so a stall consumed by a
/// transaction that then aborts stays consumed.
const FAULT_FIXTURE_SQL: &str = r#"
CREATE TABLE prism_execution_fault (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    target_table text NOT NULL,
    key_column text NOT NULL,
    key text NOT NULL,
    timeout_ms integer NOT NULL CHECK (timeout_ms > 0),
    stall_seconds double precision NOT NULL CHECK (stall_seconds > 0),
    budget bigint NOT NULL CHECK (budget >= 0)
);
CREATE SEQUENCE prism_execution_fault_hits;
CREATE FUNCTION prism_execution_observe() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    fault prism_execution_fault%ROWTYPE;
    consumed bigint;
BEGIN
    RAISE NOTICE 'prism-execution-marker % %', TG_TABLE_NAME, TG_OP;
    SELECT * INTO fault FROM prism_execution_fault;
    IF FOUND AND TG_TABLE_NAME <> fault.target_table THEN
        SELECT CASE WHEN is_called THEN last_value ELSE 0 END INTO consumed
            FROM prism_execution_fault_hits;
        IF consumed < fault.budget THEN
            PERFORM set_config('statement_timeout', fault.timeout_ms::text, true);
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE FUNCTION prism_execution_stall() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    fault prism_execution_fault%ROWTYPE;
BEGIN
    SELECT * INTO fault FROM prism_execution_fault;
    IF FOUND AND TG_TABLE_NAME = fault.target_table
        AND to_jsonb(NEW) ->> fault.key_column = fault.key THEN
        IF nextval('prism_execution_fault_hits') <= fault.budget THEN
            PERFORM pg_sleep(fault.stall_seconds);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER prism_execution_observe BEFORE INSERT OR UPDATE OR DELETE
    ON qbit_block_candidate_outbox FOR EACH STATEMENT EXECUTE FUNCTION prism_execution_observe();
CREATE TRIGGER prism_execution_observe BEFORE INSERT OR UPDATE OR DELETE
    ON qbit_pool_blocks FOR EACH STATEMENT EXECUTE FUNCTION prism_execution_observe();
CREATE TRIGGER prism_execution_observe BEFORE INSERT OR UPDATE OR DELETE
    ON qbit_prism_cluster FOR EACH STATEMENT EXECUTE FUNCTION prism_execution_observe();
CREATE TRIGGER prism_execution_observe BEFORE INSERT OR UPDATE OR DELETE
    ON qbit_ctv_fanout_artifacts FOR EACH STATEMENT EXECUTE FUNCTION prism_execution_observe();
CREATE TRIGGER prism_execution_observe BEFORE INSERT OR UPDATE OR DELETE
    ON qbit_ctv_fanout_broadcast_attempts FOR EACH STATEMENT EXECUTE FUNCTION prism_execution_observe();
CREATE TRIGGER prism_execution_stall BEFORE UPDATE
    ON qbit_block_candidate_outbox FOR EACH ROW EXECUTE FUNCTION prism_execution_stall();
CREATE TRIGGER prism_execution_stall BEFORE INSERT
    ON qbit_ctv_fanout_broadcast_attempts FOR EACH ROW EXECUTE FUNCTION prism_execution_stall();
"#;

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
    /// Every pool and proxy a test opened, closed by [`Self::run`] whatever
    /// the outcome of the test body.
    pools: Mutex<Vec<PgPool>>,
    proxies: Mutex<Vec<Arc<ExecutionProxy>>>,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let schema = format!("prism_exec_{}", Uuid::new_v4().simple());
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let admin = PgPool::connect(&raw).await?;
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        Ok(Some(Self {
            admin,
            schema,
            url: url.to_string(),
            pools: Mutex::default(),
            proxies: Mutex::default(),
        }))
    }

    /// Run a test body, then always close its pools, finish its proxies and
    /// drop the disposable schema, whether the body returned, failed or
    /// panicked. The body's own failure is what the test reports; a cleanup
    /// failure after it is printed, and fails the test only on its own.
    async fn run(&self, body: impl Future<Output = Result<()>>) -> Result<()> {
        let outcome = AssertUnwindSafe(body).catch_unwind().await;
        // The body and every ledger, transaction and claim it held have been
        // dropped here, so closing the pools cannot wait on a borrowed
        // connection.
        let cleanup = self.cleanup().await;
        match outcome {
            Ok(Ok(())) => cleanup,
            Ok(Err(error)) => {
                if let Err(cleanup) = cleanup {
                    eprintln!("cleanup after a failed test also failed: {cleanup:#}");
                }
                Err(error)
            }
            Err(panic) => {
                if let Err(cleanup) = cleanup {
                    eprintln!("cleanup after a panicked test also failed: {cleanup:#}");
                }
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Every step runs and is bounded; the first failure is returned.
    async fn cleanup(&self) -> Result<()> {
        let mut outcome = Ok(());
        let mut keep = |step: Result<()>| {
            if outcome.is_ok() {
                outcome = step;
            }
        };
        let pools = std::mem::take(&mut *self.pools.lock().expect("pool registry"));
        for pool in pools {
            keep(
                tokio::time::timeout(CALL_BUDGET, pool.close())
                    .await
                    .context("closing a test pool did not finish within its budget"),
            );
        }
        let proxies = std::mem::take(&mut *self.proxies.lock().expect("proxy registry"));
        for proxy in proxies {
            keep(
                tokio::time::timeout(CALL_BUDGET, proxy.finish())
                    .await
                    .context("finishing the proxy did not finish within its budget")
                    .and_then(|finished| finished),
            );
        }
        keep(
            tokio::time::timeout(
                CALL_BUDGET,
                sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema)).execute(&self.admin),
            )
            .await
            .context("dropping the disposable schema did not finish within its budget")
            .and_then(|dropped| dropped.map(|_| ()).map_err(Into::into)),
        );
        self.admin.close().await;
        outcome
    }

    fn register(&self, pool: &PgPool) {
        self.pools.lock().expect("pool registry").push(pool.clone());
    }

    /// A ledger on a direct connection that initializes the schema.
    async fn ledger(&self, id: &str) -> Result<Ledger> {
        let ledger = Ledger::connect(&self.url, id.to_owned(), 8, true).await?;
        self.register(&ledger.pool);
        Ok(ledger)
    }

    /// A direct pool inside the schema for seeding faults and reading state.
    async fn fixture(&self) -> Result<PgPool> {
        let pool = PgPool::connect(&self.url).await?;
        self.register(&pool);
        Ok(pool)
    }

    /// A ledger whose every socket goes through a fresh [`ExecutionProxy`].
    async fn proxied(&self, id: &str) -> Result<(Arc<ExecutionProxy>, Ledger)> {
        let raw = url::Url::parse(&self.url)?;
        let host = raw.host_str().context("database URL names a host")?;
        let port = raw.port().unwrap_or(5432);
        let upstream = tokio::net::lookup_host((host, port))
            .await?
            .next()
            .context("database host resolves")?;
        let proxy = Arc::new(ExecutionProxy::start(upstream).await?);
        self.proxies
            .lock()
            .expect("proxy registry")
            .push(proxy.clone());
        let ledger =
            Ledger::connect(&proxy.rewrite_url(&self.url)?, id.to_owned(), 4, false).await?;
        self.register(&ledger.pool);
        Ok((proxy, ledger))
    }

    async fn install_fault_fixture(&self, fixture: &PgPool) -> Result<()> {
        sqlx::raw_sql(FAULT_FIXTURE_SQL).execute(fixture).await?;
        Ok(())
    }

    /// Arm one stall on the row of `table` whose `key_column` is `key`.
    async fn arm_stall(
        &self,
        fixture: &PgPool,
        table: &str,
        key_column: &str,
        key: &str,
    ) -> Result<()> {
        sqlx::query("DELETE FROM prism_execution_fault")
            .execute(fixture)
            .await?;
        sqlx::query("INSERT INTO prism_execution_fault(target_table,key_column,key,timeout_ms,stall_seconds,budget) SELECT $1,$2,$3,$4,$5,COALESCE((SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM prism_execution_fault_hits),0)+1")
            .bind(table).bind(key_column).bind(key).bind(ARMED_TIMEOUT_MS).bind(STALL_SECONDS)
            .execute(fixture).await?;
        Ok(())
    }

    async fn disarm(&self, fixture: &PgPool) -> Result<()> {
        sqlx::query("DELETE FROM prism_execution_fault")
            .execute(fixture)
            .await?;
        Ok(())
    }
}

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{id:064x}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn ledger_public_key() -> String {
    keys().1.public_key_hex()
}

/// A signed, verifiable candidate over the snapshot, as the coordinator
/// enqueues after a network-target share.
fn candidate(snapshot: &Snapshot, nonce: u32) -> Result<Candidate> {
    let (coinbase_key, ledger_key) = keys();
    let bundle = build_audit_bundle(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        &coinbase_key,
        &ledger_key,
    )?;
    candidate_with_bundle(bundle, snapshot.payout_revision, nonce)
}

fn candidate_with_bundle(
    bundle: AuditBundle,
    payout_revision: i64,
    nonce: u32,
) -> Result<Candidate> {
    let report = verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_public_key())?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x20000000u32.to_le_bytes());
    block[4..36].fill(0x22);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
    block[72..76].copy_from_slice(&0x207fffffu32.to_le_bytes());
    block[76..80].copy_from_slice(&nonce.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&block)).to_vec();
    hash.reverse();
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    Ok(Candidate {
        block_hash: hex::encode(hash),
        block_hex: hex::encode(block),
        job_id: "job".into(),
        payout_revision,
        bundle,
        deferred_share: None,
        coinbase_suffix_hex: None,
    })
}

/// Land and confirm a CTV-settled block with one fanout per miner, then
/// reconcile it to maturity so its fanouts become claimable. Returns the
/// block hash.
async fn mature_fanouts(ledger: &Ledger, count: u8) -> Result<String> {
    for n in 1..=count {
        let mut accepted = share(u64::from(n));
        accepted.miner_id = format!("miner-{n}");
        accepted.order_key = accepted.miner_id.clone();
        accepted.p2mr_program_hex = format!("{n:02x}").repeat(32);
        ledger.append(accepted, None).await?;
    }
    let snapshot = ledger.snapshot(100).await?;
    let (coinbase_key, ledger_key) = keys();
    let bundle = qbit_prism::build_audit_bundle_with_ctv_settlement_options(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        u64::MAX,
        qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
        Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
        None,
        vec![],
        &coinbase_key,
        &ledger_key,
    )?;
    ensure!(
        bundle
            .ctv_fanout_manifest_set
            .as_ref()
            .map(|set| set.fanout_count)
            == Some(u32::from(count)),
        "fixture must produce one fanout per miner"
    );
    let block = candidate_with_bundle(bundle, snapshot.payout_revision, 31)?;
    let hash = block.block_hash.clone();
    ledger.enqueue_candidate(block).await?;
    let claim = ledger
        .claim_candidate(60)
        .await?
        .context("fanout parent claim")?;
    ledger
        .land_candidate(&claim, &ledger_key.public_key_hex())
        .await?;
    ledger.finish_candidate(&claim, true, None).await?;
    ledger
        .reconcile_blocks_at_revision(
            &[BlockObservation {
                block_hash: hash.clone(),
                active: true,
            }],
            1101,
            ledger.payout_revision().await?,
        )
        .await?;
    Ok(hash)
}

/// Durable outbox facts a disposition may or may not change.
#[derive(Clone, Debug, PartialEq, Eq)]
struct OutboxState {
    state: String,
    claim_token: Option<String>,
    claim_live: bool,
    attempt_count: i32,
    candidate_retained: bool,
    last_error: Option<String>,
}

async fn outbox(fixture: &PgPool, hash: &str) -> Result<OutboxState> {
    let (state, claim_token, claim_live, attempt_count, candidate_retained, last_error) =
        sqlx::query_as("SELECT state,claim_token,COALESCE(claim_expires_at>clock_timestamp(),false),attempt_count,candidate IS NOT NULL,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(hash)
            .fetch_one(fixture)
            .await?;
    Ok(OutboxState {
        state,
        claim_token,
        claim_live,
        attempt_count,
        candidate_retained,
        last_error,
    })
}

async fn outbox_row(fixture: &PgPool, hash: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(fixture)
    .await?)
}

async fn chain_state(fixture: &PgPool, hash: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
            .bind(hash)
            .fetch_optional(fixture)
            .await?,
    )
}

/// The shared revision read directly, whether or not the cluster is halted.
async fn stored_revision(fixture: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(fixture)
            .await?,
    )
}

/// Every durable row a landing publishes for one block.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Publication {
    blocks: i64,
    audits: i64,
    payouts: i64,
    carry: i64,
    fanout_sets: i64,
}

async fn publication(fixture: &PgPool, hash: &str) -> Result<Publication> {
    let (blocks, audits, payouts, carry, fanout_sets) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_pool_blocks WHERE block_hash=$1),(SELECT count(*) FROM qbit_pool_audit_bundles WHERE block_hash=$1),(SELECT count(*) FROM qbit_pool_payout_entries WHERE block_hash=$1),(SELECT count(*) FROM qbit_payout_carry_forward WHERE block_hash=$1),(SELECT count(*) FROM qbit_ctv_fanout_sets WHERE block_hash=$1)",
    )
    .bind(hash)
    .fetch_one(fixture)
    .await?;
    Ok(Publication {
        blocks,
        audits,
        payouts,
        carry,
        fanout_sets,
    })
}

/// Durable fanout facts a broadcast attempt may or may not change.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FanoutState {
    status: String,
    claim_token: Option<String>,
    claim_live: bool,
    attempt_count: i64,
    journal_rows: i64,
    next_attempt_scheduled: bool,
    last_attempt_status: Option<String>,
}

async fn fanout(fixture: &PgPool, txid: &str) -> Result<FanoutState> {
    // Scheduling is relative to the durable write, independent of test delays.
    let (status, claim_token, claim_live, attempt_count, journal_rows, next_attempt_scheduled, last_attempt_status) =
        sqlx::query_as("SELECT settlement_status,claim_token,COALESCE(claim_expires_at>clock_timestamp(),false),broadcast_attempt_count,(SELECT count(*) FROM qbit_ctv_fanout_broadcast_attempts WHERE fanout_txid=$1),COALESCE(next_broadcast_attempt_at>updated_at,false),last_broadcast_attempt_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
            .bind(txid)
            .fetch_one(fixture)
            .await?;
    Ok(FanoutState {
        status,
        claim_token,
        claim_live,
        attempt_count,
        journal_rows,
        next_attempt_scheduled,
        last_attempt_status,
    })
}

/// The attempt journal for one fanout, oldest first: (status, error).
async fn journal(fixture: &PgPool, txid: &str) -> Result<Vec<(String, Option<String>)>> {
    Ok(sqlx::query_as("SELECT attempt_status,error FROM qbit_ctv_fanout_broadcast_attempts WHERE fanout_txid=$1 ORDER BY attempt_seq")
        .bind(txid)
        .fetch_all(fixture)
        .await?)
}

fn sqlx_error(error: &anyhow::Error) -> Option<&sqlx::Error> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
}

/// The SQLSTATE the server reported, if the failure was a database error.
fn sqlstate(error: &anyhow::Error) -> Option<String> {
    sqlx_error(error)?
        .as_database_error()?
        .code()
        .map(|code| code.into_owned())
}

/// A socket-level failure: the client never received the server's answer.
fn transport_failure(error: &anyhow::Error) -> bool {
    matches!(sqlx_error(error), Some(sqlx::Error::Io(_)))
}

/// A public ledger call that must return within the call budget, so a
/// mutation that retries forever fails the test instead of hanging it.
async fn bounded<T>(call: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(CALL_BUDGET, call)
        .await
        .context("public ledger call did not return within its budget")?
}

/// Poll `probe` until it yields a value, within `budget`.
async fn wait_for<T, F, Fut>(budget: Duration, mut probe: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    tokio::time::timeout(budget, async {
        loop {
            if let Some(value) = probe().await? {
                return Ok::<_, anyhow::Error>(value);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("condition did not hold within its budget")?
}

/// The statements of the marked operation a window's executions carried;
/// see [`target_statement_count`].
fn count_target(executions: &[Execution], table: &str, op: &str) -> usize {
    target_statement_count(executions, table, op)
}

/// Every server rejection in a window must be the failure of one of the
/// `failed` executions, one each. A replay sent into an aborted transaction
/// is rejected, at its `Execute` or, under a statement text the connection
/// has not prepared, at its `Parse`, so it appears here even when no durable
/// row and no counted execution records it.
fn assert_rejected_only(rejections: &[Rejection], failed: &[u64]) {
    let rejected: Vec<RejectedFrame> = rejections
        .iter()
        .map(|rejection| rejection.frame.clone())
        .collect();
    let expected: Vec<RejectedFrame> = failed
        .iter()
        .copied()
        .map(RejectedFrame::Execution)
        .collect();
    assert_eq!(
        rejected, expected,
        "the server rejected work other than the targeted statement: {rejections:#?}"
    );
}

#[tokio::test]
async fn candidate_terminal_timeout_executes_once() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    db.run(candidate_terminal_timeout_body(&db)).await
}

async fn candidate_terminal_timeout_body(db: &Database) -> Result<()> {
    let direct = db.ledger("direct").await?;
    let fixture = db.fixture().await?;
    db.install_fault_fixture(&fixture).await?;
    let (proxy, ledger) = db.proxied("proxied").await?;
    let key = ledger_public_key();
    direct.append(share(1), None).await?;
    let snapshot = direct.snapshot(100).await?;
    for nonce in [101, 102, 103] {
        direct
            .enqueue_candidate(candidate(&snapshot, nonce)?)
            .await?;
    }
    // Three production-like candidates, each claimed and landed by the
    // observed ledger, so each terminal write confirms prepared rows.
    let mut claims = Vec::new();
    for _ in 0..3 {
        let claim = ledger
            .claim_candidate(120)
            .await?
            .context("candidate claim")?;
        ledger.land_candidate(&claim, &key).await?;
        claims.push(claim);
    }
    assert!(ledger.claim_candidate(120).await?.is_none());

    // A: the terminal write hits a real server statement timeout.
    let timed_out = &claims[0];
    let hash = timed_out.candidate.block_hash.clone();
    db.arm_stall(&fixture, OUTBOX, "block_hash", &hash).await?;
    let before = outbox(&fixture, &hash).await?;
    assert_eq!(before.state, "pending");
    assert!(before.claim_live);
    let revision = direct.payout_revision().await?;
    let mark = proxy.mark();
    let error = bounded(ledger.finish_candidate_at_revision(timed_out, true, None, revision))
        .await
        .err()
        .context("a timed-out terminal write reported success")?;
    let executions = proxy.executions_since(mark)?;
    let writes = target_executions(&executions, OUTBOX, "UPDATE");
    assert_eq!(
        count_target(&executions, OUTBOX, "UPDATE"),
        1,
        "the terminal write must be sent exactly once: {writes:#?}"
    );
    assert_eq!(
        sqlstate(&error).as_deref(),
        Some("57014"),
        "expected query_canceled from the server, got {error:#}"
    );
    assert!(
        format!("{error:#}").contains("statement timeout"),
        "expected the server's statement timeout, got {error:#}"
    );
    assert_eq!(writes[0].sqlstate(), Some("57014"));
    assert_rejected_only(&proxy.rejections_since(mark)?, &[writes[0].seq]);
    assert!(
        !executions.iter().any(Execution::is_commit),
        "a timed-out disposition must not commit"
    );
    assert_eq!(outbox(&fixture, &hash).await?, before);
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("prepared")
    );
    assert_eq!(stored_revision(&fixture).await?, revision);
    db.disarm(&fixture).await?;
    // Positive control: only a second, deliberate public invocation runs the
    // same statement again, and the counter sees both invocations.
    let control = proxy.mark();
    bounded(ledger.finish_candidate_at_revision(timed_out, true, None, revision)).await?;
    let executions = proxy.executions_since(control)?;
    let writes = target_executions(&executions, OUTBOX, "UPDATE");
    assert_eq!(
        count_target(&executions, OUTBOX, "UPDATE"),
        1,
        "{writes:#?}"
    );
    assert!(writes[0].delivered());
    assert!(executions.iter().any(Execution::is_commit));
    assert_rejected_only(&proxy.rejections_since(control)?, &[]);
    let both_calls = proxy.executions_since(mark)?;
    let both = target_executions(&both_calls, OUTBOX, "UPDATE");
    assert_eq!(both.len(), 2, "two invocations, two executions: {both:#?}");
    assert_eq!(count_target(&both_calls, OUTBOX, "UPDATE"), 2);
    assert_eq!(
        both[0].sql, both[1].sql,
        "both invocations ran the same statement"
    );
    let after = outbox(&fixture, &hash).await?;
    assert_eq!(after.state, "submitted");
    assert!(after.claim_token.is_none() && !after.candidate_retained);
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("confirmed")
    );
    assert_eq!(stored_revision(&fixture).await?, revision + 1);

    // B: the server executes the terminal write but its acknowledgement is
    // lost before COMMIT; the open transaction aborts with the socket.
    let lost_early = &claims[1];
    let hash = lost_early.candidate.block_hash.clone();
    let before = outbox(&fixture, &hash).await?;
    let revision = direct.payout_revision().await?;
    proxy.plan(Fault {
        table: OUTBOX.into(),
        op: "UPDATE".into(),
        phase: FaultPhase::AfterExecution,
    });
    let mark = proxy.mark();
    let error = bounded(ledger.finish_candidate_at_revision(lost_early, true, None, revision))
        .await
        .err()
        .context("a lost acknowledgement reported success")?;
    assert!(
        transport_failure(&error),
        "expected a transport failure, got {error:#}"
    );
    let fired = proxy.fired().context("the planned fault did not fire")?;
    let executions = proxy.executions_since(mark)?;
    let writes = target_executions(&executions, OUTBOX, "UPDATE");
    assert_eq!(
        count_target(&executions, OUTBOX, "UPDATE"),
        1,
        "{writes:#?}"
    );
    assert_eq!(writes[0].seq, fired);
    assert!(
        matches!(
            writes[0].outcome,
            Outcome::Completed {
                delivered: false,
                ..
            }
        ),
        "the server completed the write and the client never heard it: {:?}",
        writes[0].outcome
    );
    assert!(!executions.iter().any(Execution::is_commit));
    assert!(
        proxy.executions_since(fired)?.is_empty(),
        "the ledger sent more work after the lost acknowledgement"
    );
    assert_rejected_only(&proxy.rejections_since(mark)?, &[]);
    assert_eq!(outbox(&fixture, &hash).await?, before);
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("prepared")
    );
    assert_eq!(stored_revision(&fixture).await?, revision);
    // Explicit re-invocation over another socket is the only path to success.
    let severed = writes[0].connection;
    let control = proxy.mark();
    bounded(ledger.finish_candidate_at_revision(lost_early, true, None, revision)).await?;
    let executions = proxy.executions_since(control)?;
    let writes = target_executions(&executions, OUTBOX, "UPDATE");
    assert_eq!(
        count_target(&executions, OUTBOX, "UPDATE"),
        1,
        "{writes:#?}"
    );
    assert_rejected_only(&proxy.rejections_since(control)?, &[]);
    // Identity, not socket counts: the re-invocation ran on a connection
    // other than the one the fault closed, and nothing reused that one.
    assert!(
        executions
            .iter()
            .all(|execution| execution.connection != severed),
        "the severed connection carried more work: {executions:#?}"
    );
    assert!(executions.iter().any(Execution::is_commit));
    assert_eq!(
        count_target(&proxy.executions_since(mark)?, OUTBOX, "UPDATE"),
        2
    );
    let after = outbox(&fixture, &hash).await?;
    assert_eq!(after.state, "submitted");
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("confirmed")
    );
    assert_eq!(stored_revision(&fixture).await?, revision + 1);

    // C: the acknowledgement of COMMIT itself is lost. The disposition is
    // durable, the caller sees an error, and nothing replays it.
    let lost_late = &claims[2];
    let hash = lost_late.candidate.block_hash.clone();
    let revision = direct.payout_revision().await?;
    proxy.plan(Fault {
        table: OUTBOX.into(),
        op: "UPDATE".into(),
        phase: FaultPhase::AfterCommit,
    });
    let mark = proxy.mark();
    let error = bounded(ledger.finish_candidate_at_revision(lost_late, true, None, revision))
        .await
        .err()
        .context("a lost COMMIT acknowledgement reported success")?;
    assert!(
        transport_failure(&error),
        "expected a transport failure, got {error:#}"
    );
    let fired = proxy.fired().context("the planned fault did not fire")?;
    let executions = proxy.executions_since(mark)?;
    let writes = target_executions(&executions, OUTBOX, "UPDATE");
    assert_eq!(
        count_target(&executions, OUTBOX, "UPDATE"),
        1,
        "{writes:#?}"
    );
    assert!(
        writes[0].delivered(),
        "the write's own completion was delivered"
    );
    assert_rejected_only(&proxy.rejections_since(mark)?, &[]);
    let commit = executions
        .iter()
        .find(|execution| execution.is_commit())
        .context("the COMMIT was not observed")?;
    assert_eq!(commit.seq, fired);
    assert!(
        !commit.delivered(),
        "the COMMIT completion must be the withheld frame"
    );
    let severed_commit = commit.connection;
    assert!(
        proxy.executions_since(fired)?.is_empty(),
        "the ledger sent more work after the lost COMMIT acknowledgement"
    );
    let after = outbox(&fixture, &hash).await?;
    assert_eq!(after.state, "submitted", "the commit was durable");
    assert!(after.claim_token.is_none() && !after.candidate_retained);
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("confirmed")
    );
    assert_eq!(stored_revision(&fixture).await?, revision + 1);
    // The consumed claim cannot drive a replay, and no terminal write runs.
    let control = proxy.mark();
    assert!(
        bounded(ledger.finish_candidate_at_revision(lost_late, true, None, revision + 1))
            .await
            .is_err(),
        "a consumed claim finished a terminal row again"
    );
    // Counted from before the lost COMMIT, so the committed execution
    // identifies the statement and an unmarked replay of it still counts.
    assert_eq!(
        count_target(&proxy.executions_since(mark)?, OUTBOX, "UPDATE"),
        1,
        "the consumed claim replayed the terminal write"
    );
    assert_rejected_only(&proxy.rejections_since(control)?, &[]);
    assert!(
        ledger.claim_candidate(120).await?.is_none(),
        "a terminal row was claimable again"
    );
    // The calls after the lost COMMIT acknowledgement ran, and none of them
    // on either connection a fault closed.
    let later = proxy.executions_since(control)?;
    assert!(!later.is_empty(), "the later calls sent nothing");
    assert!(
        later
            .iter()
            .all(|execution| ![severed, severed_commit].contains(&execution.connection)),
        "a severed connection carried more work: {later:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn fanout_journal_timeout_executes_once() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    db.run(fanout_journal_timeout_body(&db)).await
}

async fn fanout_journal_timeout_body(db: &Database) -> Result<()> {
    let direct = db.ledger("direct").await?;
    let fixture = db.fixture().await?;
    db.install_fault_fixture(&fixture).await?;
    let (proxy, ledger) = db.proxied("proxied").await?;
    mature_fanouts(&direct, 3).await?;
    let mut claims = Vec::new();
    for _ in 0..3 {
        claims.push(ledger.claim_fanout(120).await?.context("fanout claim")?);
    }
    assert!(ledger.claim_fanout(120).await?.is_none());
    let submitted = || Some(json!({"accepted": true}));

    // A: the journal insert hits a real server statement timeout.
    let timed_out = &claims[0];
    let txid = timed_out.fanout_txid.clone();
    db.arm_stall(&fixture, ATTEMPTS, "fanout_txid", &txid)
        .await?;
    let before = fanout(&fixture, &txid).await?;
    assert_eq!((before.journal_rows, before.attempt_count), (0, 0));
    assert!(before.claim_live);
    let mark = proxy.mark();
    let error = bounded(ledger.finish_fanout(timed_out, "broadcast_submitted", submitted(), None))
        .await
        .err()
        .context("a timed-out journal write reported success")?;
    let executions = proxy.executions_since(mark)?;
    let inserts = target_executions(&executions, ATTEMPTS, "INSERT");
    assert_eq!(
        count_target(&executions, ATTEMPTS, "INSERT"),
        1,
        "the journal insert must be sent exactly once: {inserts:#?}"
    );
    assert_eq!(
        sqlstate(&error).as_deref(),
        Some("57014"),
        "expected query_canceled from the server, got {error:#}"
    );
    assert!(
        format!("{error:#}").contains("statement timeout"),
        "expected the server's statement timeout, got {error:#}"
    );
    assert_eq!(inserts[0].sqlstate(), Some("57014"));
    assert_rejected_only(&proxy.rejections_since(mark)?, &[inserts[0].seq]);
    assert!(!executions.iter().any(Execution::is_commit));
    assert_eq!(
        fanout(&fixture, &txid).await?,
        before,
        "a timed-out attempt changed durable state"
    );
    db.disarm(&fixture).await?;
    // Positive control: the second deliberate invocation is the second
    // execution the counter sees, and the first journal row.
    let control = proxy.mark();
    bounded(ledger.finish_fanout(timed_out, "broadcast_submitted", submitted(), None)).await?;
    let executions = proxy.executions_since(control)?;
    let inserts = target_executions(&executions, ATTEMPTS, "INSERT");
    assert_eq!(
        count_target(&executions, ATTEMPTS, "INSERT"),
        1,
        "{inserts:#?}"
    );
    assert!(inserts[0].delivered());
    assert!(executions.iter().any(Execution::is_commit));
    assert_rejected_only(&proxy.rejections_since(control)?, &[]);
    let both_calls = proxy.executions_since(mark)?;
    let both = target_executions(&both_calls, ATTEMPTS, "INSERT");
    assert_eq!(both.len(), 2, "{both:#?}");
    assert_eq!(count_target(&both_calls, ATTEMPTS, "INSERT"), 2);
    assert_eq!(both[0].sql, both[1].sql);
    let after = fanout(&fixture, &txid).await?;
    assert_eq!((after.journal_rows, after.attempt_count), (1, 1));
    assert_eq!(after.status, "broadcast_submitted");
    assert!(after.claim_token.is_none() && after.next_attempt_scheduled);

    // B: the journal insert executes but its acknowledgement is lost before
    // COMMIT: no row, claim intact, one explicit re-invocation succeeds.
    let lost_early = &claims[1];
    let txid = lost_early.fanout_txid.clone();
    let before = fanout(&fixture, &txid).await?;
    proxy.plan(Fault {
        table: ATTEMPTS.into(),
        op: "INSERT".into(),
        phase: FaultPhase::AfterExecution,
    });
    let mark = proxy.mark();
    let error = bounded(ledger.finish_fanout(lost_early, "broadcast_submitted", submitted(), None))
        .await
        .err()
        .context("a lost acknowledgement reported success")?;
    assert!(
        transport_failure(&error),
        "expected a transport failure, got {error:#}"
    );
    let fired = proxy.fired().context("the planned fault did not fire")?;
    let executions = proxy.executions_since(mark)?;
    let inserts = target_executions(&executions, ATTEMPTS, "INSERT");
    assert_eq!(
        count_target(&executions, ATTEMPTS, "INSERT"),
        1,
        "{inserts:#?}"
    );
    assert_eq!(inserts[0].seq, fired);
    assert!(!inserts[0].delivered() && inserts[0].completion().is_some());
    assert!(!executions.iter().any(Execution::is_commit));
    assert!(proxy.executions_since(fired)?.is_empty());
    assert_rejected_only(&proxy.rejections_since(mark)?, &[]);
    assert_eq!(fanout(&fixture, &txid).await?, before);
    let severed = inserts[0].connection;
    let control = proxy.mark();
    bounded(ledger.finish_fanout(lost_early, "broadcast_submitted", submitted(), None)).await?;
    let executions = proxy.executions_since(control)?;
    let inserts = target_executions(&executions, ATTEMPTS, "INSERT");
    assert_eq!(
        count_target(&executions, ATTEMPTS, "INSERT"),
        1,
        "{inserts:#?}"
    );
    assert_rejected_only(&proxy.rejections_since(control)?, &[]);
    // Identity, not socket counts: the re-invocation ran on a connection
    // other than the one the fault closed, and nothing reused that one.
    assert!(
        executions
            .iter()
            .all(|execution| execution.connection != severed),
        "the severed connection carried more work: {executions:#?}"
    );
    assert_eq!(
        count_target(&proxy.executions_since(mark)?, ATTEMPTS, "INSERT"),
        2
    );
    let after = fanout(&fixture, &txid).await?;
    assert_eq!((after.journal_rows, after.attempt_count), (1, 1));
    assert!(after.claim_token.is_none());

    // C: the COMMIT acknowledgement is lost: one durable journal row, the
    // claim consumed, an error reported, and no replay.
    let lost_late = &claims[2];
    let txid = lost_late.fanout_txid.clone();
    proxy.plan(Fault {
        table: ATTEMPTS.into(),
        op: "INSERT".into(),
        phase: FaultPhase::AfterCommit,
    });
    let mark = proxy.mark();
    let error = bounded(ledger.finish_fanout(lost_late, "broadcast_submitted", submitted(), None))
        .await
        .err()
        .context("a lost COMMIT acknowledgement reported success")?;
    assert!(
        transport_failure(&error),
        "expected a transport failure, got {error:#}"
    );
    let fired = proxy.fired().context("the planned fault did not fire")?;
    let executions = proxy.executions_since(mark)?;
    let inserts = target_executions(&executions, ATTEMPTS, "INSERT");
    assert_eq!(
        count_target(&executions, ATTEMPTS, "INSERT"),
        1,
        "{inserts:#?}"
    );
    assert!(inserts[0].delivered());
    let commit = executions
        .iter()
        .find(|execution| execution.is_commit())
        .context("the COMMIT was not observed")?;
    assert_eq!(commit.seq, fired);
    assert!(!commit.delivered());
    let severed_commit = commit.connection;
    assert!(proxy.executions_since(fired)?.is_empty());
    assert_rejected_only(&proxy.rejections_since(mark)?, &[]);
    let after = fanout(&fixture, &txid).await?;
    assert_eq!((after.journal_rows, after.attempt_count), (1, 1));
    assert_eq!(after.status, "broadcast_submitted");
    assert!(after.claim_token.is_none() && after.next_attempt_scheduled);
    assert_eq!(
        journal(&fixture, &txid).await?,
        vec![("submitted".to_owned(), None)],
        "one journal event for one authorized claim"
    );
    let control = proxy.mark();
    assert!(
        bounded(ledger.finish_fanout(lost_late, "failed", None, Some("late")))
            .await
            .is_err(),
        "a consumed fanout claim journaled again"
    );
    // Counted from before the lost COMMIT, so the committed execution
    // identifies the statement and an unmarked replay of it still counts.
    assert_eq!(
        count_target(&proxy.executions_since(mark)?, ATTEMPTS, "INSERT"),
        1,
        "the consumed claim replayed the journal insert"
    );
    assert_rejected_only(&proxy.rejections_since(control)?, &[]);
    assert_eq!(fanout(&fixture, &txid).await?, after);
    // The claim query considers every fanout. Freeze all completed attempts
    // after their durable-state checks, including the earlier controls whose
    // real backoffs may already have elapsed while this test was descheduled.
    let txids: Vec<_> = claims
        .iter()
        .map(|claim| claim.fanout_txid.clone())
        .collect();
    let frozen = sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at='infinity'::timestamptz WHERE fanout_txid=ANY($1)")
        .bind(&txids).execute(&fixture).await?.rows_affected();
    assert_eq!(frozen, claims.len() as u64);
    assert!(
        ledger.claim_fanout(120).await?.is_none(),
        "a fanout inside its backoff was claimable"
    );
    // The calls after the lost COMMIT acknowledgement ran, and none of them
    // on either connection a fault closed.
    let later = proxy.executions_since(control)?;
    assert!(!later.is_empty(), "the later calls sent nothing");
    assert!(
        later
            .iter()
            .all(|execution| ![severed, severed_commit].contains(&execution.connection)),
        "a severed connection carried more work: {later:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn candidate_backoff_requires_new_claim() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    db.run(candidate_backoff_body(&db)).await
}

async fn candidate_backoff_body(db: &Database) -> Result<()> {
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let fixture = db.fixture().await?;
    let key = ledger_public_key();
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 301)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block).await?;
    let claim = a.claim_candidate(60).await?.context("candidate claim")?;
    a.land_candidate(&claim, &key).await?;
    let revision = a.payout_revision().await?;
    // A terminal write that fails: the worker's expected revision is stale.
    assert!(a
        .finish_candidate_at_revision(&claim, true, None, revision + 1)
        .await
        .is_err());
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("prepared")
    );
    assert!(outbox(&fixture, &hash).await?.claim_live);

    // The intentional retry releases the claim and schedules the next
    // attempt; it does not run the failed terminal write.
    a.retry_candidate(&claim, "node unavailable").await?;
    // The schedule is compared with the retry's own write time, so it holds
    // however slowly the rest of the test runs.
    let scheduled: bool = sqlx::query_scalar(
        "SELECT next_attempt_at>updated_at FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(&hash)
    .fetch_one(&fixture)
    .await?;
    assert!(scheduled, "retry did not advance next_attempt_at");
    // Once the real backoff is verified, fixture-controlled eligibility keeps
    // scheduler delays from racing the one-second first-attempt deadline.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at='infinity'::timestamptz WHERE block_hash=$1")
        .bind(&hash).execute(&fixture).await?;
    let early = b.claim_candidate(60).await?;
    assert!(early.is_none(), "claimed before its next attempt was due");
    let released = outbox(&fixture, &hash).await?;
    assert_eq!(
        released,
        OutboxState {
            state: "pending".into(),
            claim_token: None,
            claim_live: false,
            attempt_count: 1,
            candidate_retained: true,
            last_error: Some("node unavailable".into()),
        }
    );
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("prepared")
    );
    assert_eq!(stored_revision(&fixture).await?, revision);

    // The released token is dead for every claim-fenced operation.
    let row = outbox_row(&fixture, &hash).await?;
    assert!(a.renew_candidate_claim(&claim, 60).await.is_err());
    assert!(a.retry_candidate(&claim, "again").await.is_err());
    assert!(a.land_candidate(&claim, &key).await.is_err());
    assert!(a
        .finish_candidate_at_revision(&claim, true, None, revision)
        .await
        .is_err());
    assert!(a
        .finish_candidate_at_revision(&claim, false, Some("late"), revision)
        .await
        .is_err());
    assert_eq!(
        outbox_row(&fixture, &hash).await?,
        row,
        "a dead token changed the row"
    );

    // After eligibility exactly one fresh token identifies the next attempt.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at='-infinity'::timestamptz WHERE block_hash=$1")
        .bind(&hash).execute(&fixture).await?;
    let fresh = b
        .claim_candidate(60)
        .await?
        .context("due candidate claim")?;
    assert_eq!(fresh.candidate.block_hash, hash);
    assert_ne!(fresh.claim_token, claim.claim_token);
    let claimed = outbox(&fixture, &hash).await?;
    assert_eq!(
        claimed.claim_token.as_deref(),
        Some(fresh.claim_token.as_str())
    );
    assert!(claimed.claim_live);
    assert_eq!(claimed.attempt_count, 2);
    assert!(
        a.claim_candidate(60).await?.is_none(),
        "two attempts were live at once"
    );
    assert!(a
        .finish_candidate_at_revision(&claim, true, None, revision)
        .await
        .is_err());
    b.finish_candidate_at_revision(&fresh, true, None, revision)
        .await?;
    assert_eq!(outbox(&fixture, &hash).await?.state, "submitted");
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("confirmed")
    );
    assert_eq!(stored_revision(&fixture).await?, revision + 1);
    Ok(())
}

#[tokio::test]
async fn broadcast_retry_requires_new_claim() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    db.run(broadcast_retry_body(&db)).await
}

async fn broadcast_retry_body(db: &Database) -> Result<()> {
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let fixture = db.fixture().await?;
    mature_fanouts(&a, 1).await?;
    let claim = a.claim_fanout(60).await?.context("fanout claim")?;
    let txid = claim.fanout_txid.clone();
    let before = fanout(&fixture, &txid).await?;
    assert_eq!((before.journal_rows, before.attempt_count), (0, 0));

    // One failed attempt: one journal event, the claim released, the next
    // attempt scheduled after the backoff.
    a.finish_fanout(&claim, "failed", None, Some("node unavailable"))
        .await?;
    let failed = fanout(&fixture, &txid).await?;
    assert_eq!(
        failed,
        FanoutState {
            status: "failed".into(),
            claim_token: None,
            claim_live: false,
            attempt_count: 1,
            journal_rows: 1,
            next_attempt_scheduled: true,
            last_attempt_status: Some("failed".into()),
        }
    );
    assert_eq!(
        journal(&fixture, &txid).await?,
        vec![("failed".to_owned(), Some("node unavailable".to_owned()))]
    );

    // The released token is rejected by every claim-fenced operation and
    // journals nothing.
    assert!(a
        .finish_fanout(&claim, "failed", None, Some("late"))
        .await
        .is_err());
    assert!(a.renew_fanout_claim(&claim, 60).await.is_err());
    assert!(a.record_fanout_scan(&claim, 1103, None).await.is_err());
    assert!(a
        .observe_fanout(&claim, "broadcastable", json!({"next_check_seconds": 5}))
        .await
        .is_err());
    assert_eq!(
        fanout(&fixture, &txid).await?,
        failed,
        "a dead token changed the artifact"
    );

    // The state checks above verify the real backoff before the fixture makes
    // eligibility independent of elapsed wall-clock time.
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at='infinity'::timestamptz WHERE fanout_txid=$1")
        .bind(&txid).execute(&fixture).await?;
    // Before the scheduled attempt nobody can claim it.
    assert!(
        b.claim_fanout(60).await?.is_none(),
        "claimed inside its backoff"
    );
    // Make the next attempt due without waiting for the backoff to elapse.
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at='-infinity'::timestamptz WHERE fanout_txid=$1")
        .bind(&txid).execute(&fixture).await?;
    let fresh = b
        .claim_fanout(60)
        .await?
        .context("due fanout not claimable")?;
    assert_eq!(fresh.fanout_txid, txid);
    assert_ne!(fresh.claim_token, claim.claim_token);
    assert_eq!(
        fresh.attempt_count, 1,
        "the fresh claim carries the attempt history"
    );
    assert!(
        a.claim_fanout(60).await?.is_none(),
        "two attempts were live at once"
    );
    assert!(a
        .finish_fanout(
            &claim,
            "broadcast_submitted",
            Some(json!({"accepted": true})),
            None
        )
        .await
        .is_err());

    // The additional legitimate attempt is a separate journal event.
    b.finish_fanout(
        &fresh,
        "broadcast_submitted",
        Some(json!({"accepted": true})),
        None,
    )
    .await?;
    let retried = fanout(&fixture, &txid).await?;
    assert_eq!(
        retried,
        FanoutState {
            status: "broadcast_submitted".into(),
            claim_token: None,
            claim_live: false,
            attempt_count: 2,
            journal_rows: 2,
            next_attempt_scheduled: true,
            last_attempt_status: Some("submitted".into()),
        }
    );
    assert_eq!(
        journal(&fixture, &txid).await?,
        vec![
            ("failed".to_owned(), Some("node unavailable".to_owned())),
            ("submitted".to_owned(), None),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn distinct_hashes_hold_independent_claims() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    db.run(distinct_hashes_body(&db)).await
}

async fn distinct_hashes_body(db: &Database) -> Result<()> {
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let fixture = db.fixture().await?;
    let key = ledger_public_key();
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    for nonce in [501, 502] {
        a.enqueue_candidate(candidate(&snapshot, nonce)?).await?;
    }
    let held = a.claim_candidate(120).await?.context("first claim")?;
    let progressing = b
        .claim_candidate(120)
        .await?
        .context("a second hash was not claimable while the first was held")?;
    assert_ne!(held.candidate.block_hash, progressing.candidate.block_hash);
    assert_ne!(held.claim_token, progressing.claim_token);
    let held_hash = held.candidate.block_hash.clone();
    let progressing_hash = progressing.candidate.block_hash.clone();

    // The held candidate's owner is mid-processing: its outbox row is locked
    // by an open transaction. The fixture takes the strongest row lock, so any
    // disposition path that touched another hash's row would block on it.
    let mut processing = fixture.begin().await?;
    sqlx::query(
        "SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE",
    )
    .bind(&held_hash)
    .fetch_one(&mut *processing)
    .await?;
    let held_before = outbox(&fixture, &held_hash).await?;
    let revision = b.payout_revision().await?;
    // The other hash lands and finishes while the sibling outbox row stays
    // locked. This proves row independence; candidate dispositions still
    // acquire the shared settlement and order locks.
    bounded(b.land_candidate(&progressing, &key)).await?;
    bounded(b.finish_candidate_at_revision(&progressing, true, None, revision)).await?;
    assert_eq!(
        outbox(&fixture, &progressing_hash).await?.state,
        "submitted"
    );
    assert_eq!(
        chain_state(&fixture, &progressing_hash).await?.as_deref(),
        Some("confirmed")
    );
    assert_eq!(stored_revision(&fixture).await?, revision + 1);
    assert_eq!(
        outbox(&fixture, &held_hash).await?,
        held_before,
        "the sibling's disposition touched the held claim"
    );
    assert!(held_before.claim_live);
    processing.commit().await?;

    // The held claim stays the only lease on its hash and stays usable by its
    // owner; the block it prepared is now superseded, so its owner disposes
    // of it under the current revision with the token it already holds.
    assert!(
        b.claim_candidate(120).await?.is_none(),
        "the held claim was reclaimable"
    );
    a.renew_candidate_claim(&held, 120).await?;
    assert!(
        a.land_candidate(&held, &key).await.is_err(),
        "a superseded candidate landed"
    );
    a.finish_candidate_at_revision(
        &held,
        false,
        Some("payout revision superseded"),
        revision + 1,
    )
    .await?;
    let held_after = outbox(&fixture, &held_hash).await?;
    assert_eq!(held_after.state, "abandoned");
    assert!(held_after.claim_token.is_none());
    assert_eq!(chain_state(&fixture, &held_hash).await?, None);
    assert_eq!(stored_revision(&fixture).await?, revision + 1);
    Ok(())
}

#[tokio::test]
async fn superseded_landing_reports_failure() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    db.run(superseded_landing_body(&db)).await
}

async fn superseded_landing_body(db: &Database) -> Result<()> {
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let fixture = db.fixture().await?;
    let key = ledger_public_key();
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 601)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block).await?;
    let claim = a.claim_candidate(120).await?.context("candidate claim")?;
    let issued = snapshot.payout_revision;
    // Another frontend proves a stronger chain view: the shared authority
    // moves on before this candidate lands.
    let current = b.observe_chain_view(&"33".repeat(32), 101, "200").await?;
    assert_eq!(current, issued + 1);
    let before = outbox_row(&fixture, &hash).await?;

    assert!(
        a.land_candidate(&claim, &key).await.is_err(),
        "a superseded landing reported success"
    );
    assert_eq!(
        publication(&fixture, &hash).await?,
        Publication::default(),
        "stale publication"
    );
    assert_eq!(
        outbox_row(&fixture, &hash).await?,
        before,
        "a failed landing changed the claim"
    );
    assert_eq!(stored_revision(&fixture).await?, current);

    // Confirming cannot invent the missing publication either.
    assert!(a
        .finish_candidate_at_revision(&claim, true, None, issued)
        .await
        .is_err());
    assert!(a.finish_candidate(&claim, true, None).await.is_err());
    assert_eq!(publication(&fixture, &hash).await?, Publication::default());
    assert_eq!(outbox_row(&fixture, &hash).await?, before);
    assert_eq!(stored_revision(&fixture).await?, current);

    // The only disposition left is an explicit abandonment under the current
    // revision, which publishes nothing and changes no shared state.
    a.finish_candidate_at_revision(&claim, false, Some("payout revision superseded"), current)
        .await?;
    let abandoned = outbox(&fixture, &hash).await?;
    assert_eq!(abandoned.state, "abandoned");
    assert!(abandoned.claim_token.is_none() && !abandoned.candidate_retained);
    assert_eq!(publication(&fixture, &hash).await?, Publication::default());
    assert_eq!(stored_revision(&fixture).await?, current);
    assert!(a.claim_candidate(120).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn restored_authority_requires_fresh_claim() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    db.run(restored_authority_body(&db)).await
}

async fn restored_authority_body(db: &Database) -> Result<()> {
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let fixture = db.fixture().await?;
    let key = ledger_public_key();
    a.append(share(1), None).await?;
    // A first block confirms and matures, so the cluster has a mature
    // checkpoint whose disconnection halts shared accounting.
    let first = a.snapshot(100).await?;
    let checkpoint = candidate(&first, 701)?;
    let checkpoint_hash = checkpoint.block_hash.clone();
    a.enqueue_candidate(checkpoint).await?;
    let checkpoint_claim = a.claim_candidate(120).await?.context("checkpoint claim")?;
    a.land_candidate(&checkpoint_claim, &key).await?;
    a.finish_candidate_at_revision(&checkpoint_claim, true, None, first.payout_revision)
        .await?;
    a.reconcile_blocks_at_revision(
        &[BlockObservation {
            block_hash: checkpoint_hash.clone(),
            active: true,
        }],
        1101,
        a.payout_revision().await?,
    )
    .await?;
    // The candidate under test is prepared under a claim that then holds a
    // short lease, as a worker about to submit does.
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 702)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block).await?;
    let claim = a.claim_candidate(120).await?.context("candidate claim")?;
    a.land_candidate(&claim, &key).await?;
    a.renew_candidate_claim(&claim, 1).await?;
    let revision = a.payout_revision().await?;

    // Authority is lost: a node reports the mature checkpoint disconnected.
    assert!(b
        .reconcile_blocks_at_revision(
            &[BlockObservation {
                block_hash: checkpoint_hash.clone(),
                active: false,
            }],
            1101,
            revision,
        )
        .await
        .is_err());
    let halted: Option<String> =
        sqlx::query_scalar("SELECT fatal_error FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(&fixture)
            .await?;
    assert!(
        halted.is_some(),
        "a mature disconnect did not halt the cluster"
    );
    assert!(
        a.payout_revision().await.is_err(),
        "a halted cluster still authorizes"
    );
    // While halted, the claim can neither finish nor renew, so it expires.
    assert!(a
        .finish_candidate_at_revision(&claim, true, None, revision)
        .await
        .is_err());
    assert!(a.renew_candidate_claim(&claim, 60).await.is_err());
    wait_for(Duration::from_secs(10), || async {
        let expired: bool = sqlx::query_scalar("SELECT claim_expires_at<=clock_timestamp() FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash).fetch_one(&fixture).await?;
        Ok(expired.then_some(()))
    })
    .await?;

    // The operator restores authority after investigating the checkpoint.
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL,updated_at=clock_timestamp() WHERE singleton")
        .execute(&fixture)
        .await?;
    assert_eq!(a.payout_revision().await?, revision);
    assert_eq!(
        chain_state(&fixture, &checkpoint_hash).await?.as_deref(),
        Some("confirmed")
    );

    // The expired lease stays invalid for every operation, and the prepared
    // block does not move.
    let before = outbox(&fixture, &hash).await?;
    assert_eq!(before.state, "pending");
    assert_eq!(
        before.claim_token.as_deref(),
        Some(claim.claim_token.as_str())
    );
    assert!(!before.claim_live);
    let row = outbox_row(&fixture, &hash).await?;
    assert!(a.renew_candidate_claim(&claim, 60).await.is_err());
    assert!(a.land_candidate(&claim, &key).await.is_err());
    assert!(a.finish_candidate(&claim, true, None).await.is_err());
    assert!(a
        .finish_candidate_at_revision(&claim, true, None, revision)
        .await
        .is_err());
    assert!(a.retry_candidate(&claim, "late").await.is_err());
    assert_eq!(
        outbox_row(&fixture, &hash).await?,
        row,
        "an expired lease changed the row"
    );
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("prepared")
    );
    assert_eq!(stored_revision(&fixture).await?, revision);

    // Only a newly acquired claim finishes it, and the old one stays dead
    // even then.
    let fresh = wait_for(Duration::from_secs(10), || b.claim_candidate(120)).await?;
    assert_eq!(fresh.candidate.block_hash, hash);
    assert_ne!(fresh.claim_token, claim.claim_token);
    assert_eq!(outbox(&fixture, &hash).await?.attempt_count, 2);
    assert!(a
        .finish_candidate_at_revision(&claim, true, None, revision)
        .await
        .is_err());
    b.finish_candidate_at_revision(&fresh, true, None, revision)
        .await?;
    assert_eq!(outbox(&fixture, &hash).await?.state, "submitted");
    assert_eq!(
        chain_state(&fixture, &hash).await?.as_deref(),
        Some("confirmed")
    );
    assert_eq!(stored_revision(&fixture).await?, revision + 1);
    Ok(())
}

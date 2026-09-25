//! Signing-key rotation: the guarded cluster fingerprint reset
//! (`qbit-prism-server signing-transition --confirm`).
//!
//! The outcome these tests exist to exclude is a reset that succeeds while a
//! frontend still holds a claim, an offer reservation or a fresh heartbeat,
//! and any write of `config_fingerprint = NULL` outside the transaction that
//! checked the outbox and the instances.
//!
//! ```text
//! test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server --test signing_transition
//! ```
//!
//! # A killed frontend never writes `stopped`
//!
//! `SIGKILL` runs no shutdown path, so a dead frontend's row keeps its last
//! health payload for ever (the trap `candidate_storm_restart` describes).
//! The command therefore accepts a row other than `stopped` only once its
//! heartbeat is older, by the database clock, than the freshness window
//! `self-check` uses: three `PRISM_HEALTH_REFRESH_SECONDS`, never less than
//! fifteen seconds. No test sleeps through that window. A killed frontend is
//! a health heartbeat written through the real `Ledger::heartbeat` and never
//! followed by `stopped`; ageing it is one `UPDATE` of `heartbeat_at`.
//!
//! # Fixtures
//!
//! Each fixture takes its own PostgreSQL database through
//! `support/ledger_database.rs`: the command holds the database-scoped
//! settlement and order advisory locks, so sharing a database would leak one
//! test's waits into another. The command's own connection is
//! `Ledger::connect_operator`, the one the CLI uses, so every row in
//! `qbit_prism_instances` belongs to a frontend the test registered.
//!
//! # No timing assertion
//!
//! The successful CLI run prints its wall clock with `--nocapture` for the
//! rotation rehearsal to compare against; nothing asserts on it. Every other
//! duration is a deadline that turns a hang into a failure.
use anyhow::{Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, verify_audit_bundle_with_ledger_public_key, AcceptedShare, FoundBlock,
    PayoutPolicy,
};
use qbit_prism_server::{
    config::Config,
    ledger::{
        Candidate, HeartbeatHealth, HeartbeatStatus, Ledger, OfferOutcome, SignerKeys, WindowRef,
    },
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "support/fake_qbitd.rs"]
mod fake_qbitd;
use fake_qbitd as fake;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

#[path = "support/cohort_fence.rs"]
mod cohort_fence;

/// The fake node's genesis, which the fingerprint binds.
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";
/// The default heartbeat cadence, whose freshness window is fifteen seconds.
const CADENCE: Duration = Duration::from_secs(2);
const OLD_SEEDS: (&str, &str) = ("42", "43");
const NEW_SEEDS: (&str, &str) = ("44", "45");

struct Fixture {
    database: FixtureDatabase,
    node: fake::FakeNode,
    /// The configuration the cluster is pinned to: the old keys.
    config: Config,
    a: Ledger,
    b: Ledger,
    /// The command's connection, as the CLI opens it.
    operator: Ledger,
}

impl Fixture {
    /// Two frontends configured with the old keys, both still `starting`.
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let database = FixtureDatabase::open(&raw, "prism_signing_").await?;
        let node = fake::FakeNode::open().await?;
        let config = config_with(&database.url, &node, OLD_SEEDS)?;
        let a = Ledger::connect(&database.url, "frontend-a".into(), 8, true).await?;
        let b = Ledger::connect(&database.url, "frontend-b".into(), 8, false).await?;
        for ledger in [&a, &b] {
            ledger
                .configure(&config.fingerprint(GENESIS)?, &signer_keys(OLD_SEEDS))
                .await?;
        }
        let operator = Ledger::connect_operator(&database.url, false).await?;
        Ok(Some(Self {
            database,
            node,
            config,
            a,
            b,
            operator,
        }))
    }

    async fn run(&self) -> Result<Value> {
        self.operator
            .transition_signing(&self.config, CADENCE)
            .await
    }

    /// The refusal, after asserting that it changed nothing.
    async fn refused(&self) -> Result<String> {
        let before = self.state().await?;
        let error = self
            .run()
            .await
            .err()
            .context("signing-transition reset the fingerprint")?;
        assert_eq!(self.state().await?, before, "a refusal changed state");
        Ok(format!("{error:#}"))
    }

    async fn state(&self) -> Result<Value> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('cluster',(SELECT to_jsonb(c) FROM qbit_prism_cluster c),'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY block_hash) FROM qbit_block_candidate_outbox o),'instances',(SELECT jsonb_agg(to_jsonb(i) ORDER BY instance_id) FROM qbit_prism_instances i),'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY transition_id) FROM qbit_prism_signing_transitions e))")
            .fetch_one(&self.operator.pool)
            .await?)
    }

    async fn fingerprint(&self) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&self.operator.pool)
                .await?,
        )
    }

    async fn journal(&self) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(e) FROM qbit_prism_signing_transitions e ORDER BY transition_id",
        )
        .fetch_all(&self.operator.pool)
        .await?)
    }

    /// A frontend that was serving and was then killed: its last write is a
    /// health heartbeat, and nothing ever writes `stopped` for it.
    async fn kill(&self, ledger: &Ledger) -> Result<()> {
        ledger
            .heartbeat(HeartbeatStatus::Health(HeartbeatHealth::new(
                true,
                Default::default(),
            )))
            .await
    }

    /// Move one heartbeat into the past by the database clock.
    async fn age(&self, instance_id: &str, seconds: f64) -> Result<()> {
        let rows = sqlx::query("UPDATE qbit_prism_instances SET heartbeat_at=clock_timestamp()-$2*interval '1 second' WHERE instance_id=$1")
            .bind(instance_id)
            .bind(seconds)
            .execute(&self.operator.pool)
            .await?
            .rows_affected();
        anyhow::ensure!(rows == 1, "{instance_id} has no heartbeat row");
        Ok(())
    }

    async fn stop_both(&self) -> Result<()> {
        self.a.heartbeat(HeartbeatStatus::Stopped).await?;
        self.b.heartbeat(HeartbeatStatus::Stopped).await
    }

    /// One candidate signed with the OLD keys, the invoking configuration's,
    /// enqueued by frontend-a and left `pending`.
    async fn enqueue(&self, nonce: u32) -> Result<String> {
        self.a.append(share(u64::from(nonce)), None).await?;
        let candidate = candidate(&self.a, nonce).await?;
        let hash = candidate.block_hash.clone();
        self.a.enqueue_candidate(candidate).await?;
        let keys: (String, String) = sqlx::query_as("SELECT candidate->'signer_keys'->>'manifest_key_hex',candidate->'signer_keys'->>'ledger_key_hex' FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_one(&self.operator.pool)
            .await?;
        let own = signer_keys(OLD_SEEDS);
        assert_eq!(
            keys,
            (own.manifest_key_hex, own.ledger_key_hex),
            "the stored candidate does not carry the invoking configuration's keys"
        );
        Ok(hash)
    }

    async fn outbox_state(&self, hash: &str) -> Result<String> {
        Ok(
            sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(hash)
                .fetch_one(&self.operator.pool)
                .await?,
        )
    }

    fn cli(&self, seeds: (&str, &str)) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
        command
            .env_clear()
            .kill_on_drop(true)
            .arg("signing-transition")
            .env("PRISM_DATABASE_URL", &self.database.url)
            .env("QBIT_RPC_URL", &self.node.url)
            .env("QBIT_CHAIN", "testnet")
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_MANIFEST_SIGNING_SEED_HEX", seeds.0.repeat(32))
            .env(
                "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
                seeds.1.repeat(32),
            )
            .env(
                "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
                key(seeds.1).public_key_hex(),
            )
            .env("PRISM_USERNAME_FALLBACK_ADDRESS", "signing-test-fallback")
            .env("PRISM_RUNTIME_WORKERS", "2");
        command
    }

    async fn close(self) -> Result<()> {
        for ledger in [self.a, self.b, self.operator] {
            ledger.pool.close().await;
        }
        self.database.close(Ok(())).await
    }
}

fn key(seed: &str) -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&seed.repeat(32)).unwrap()
}

fn signer_keys(seeds: (&str, &str)) -> SignerKeys {
    SignerKeys::of(&key(seeds.0), &key(seeds.1))
}

/// The configuration the CLI builds from [`Fixture::cli`]'s environment.
fn config_with(url: &str, node: &fake::FakeNode, seeds: (&str, &str)) -> Result<Config> {
    let mut config = fake::coordinator_config(url.to_owned(), node, "operator")?;
    config.manifest_seed = seeds.0.repeat(32);
    config.ledger_seed = seeds.1.repeat(32);
    config.ledger_public_key = key(seeds.1).public_key_hex();
    config.username_fallback = Some("signing-test-fallback".into());
    Ok(config)
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

/// A slim candidate on the ledger's current window, signed with the old
/// keys, with an 80-byte header whose double SHA-256 is its `block_hash`.
/// The builder `tests/ledger_postgres.rs` uses, reduced to what enqueueing,
/// claiming and offering need.
async fn candidate(ledger: &Ledger, nonce: u32) -> Result<Candidate> {
    let snapshot = ledger.snapshot(100).await?;
    let (coinbase_key, ledger_key) = (key(OLD_SEEDS.0), key(OLD_SEEDS.1));
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
    let report = verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_key.public_key_hex())?;
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
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "job".into(),
        payout_revision: snapshot.payout_revision,
        window: WindowRef::from_snapshot(&snapshot)?,
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: bundle.payout_policy.clone(),
        ctv: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&coinbase_key, &ledger_key),
        leased: false,
        coinbase_suffix_hex: bundle
            .coinbase_script_sig_suffix_hex
            .clone()
            .unwrap_or_else(|| "00".repeat(12)),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: Vec::new(),
    })
}

/// Wait until some backend is blocked on a lock `predicate` selects.
async fn wait_for_lock_wait(ledger: &Ledger, predicate: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE NOT granted AND {predicate})"
            ))
            .fetch_one(&ledger.pool)
            .await?;
            if waiting {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("nothing waited on a lock where {predicate}"))?
}

/// T1: a frontend that is alive refuses the reset, whatever it reports.
#[tokio::test]
async fn a_fresh_heartbeat_refuses_the_reset() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.b.heartbeat(HeartbeatStatus::Stopped).await?;
    // frontend-a is still `starting`, with the heartbeat connect wrote.
    let error = fixture.refused().await?;
    assert!(
        error.contains("frontend-a (starting, heartbeat")
            && error.contains("older than 15 seconds")
            && !error.contains("frontend-b"),
        "{error}"
    );
    // Serving, ready or not.
    for ready in [true, false] {
        fixture
            .a
            .heartbeat(HeartbeatStatus::Health(HeartbeatHealth::new(
                ready,
                Default::default(),
            )))
            .await?;
        let error = fixture.refused().await?;
        assert!(
            error.contains("frontend-a (running, heartbeat") && !error.contains("frontend-b"),
            "{error}"
        );
    }
    // A status this binary cannot read is unknown, never stopped; nor does a
    // legacy health payload become stopped by carrying that marker.
    for status in [
        json!({}),
        json!({"state":"draining"}),
        json!({"state":"drained"}),
        json!({"schema":"qbit.prism.audit-health.v1","ready":true,"state":"stopped"}),
    ] {
        sqlx::query("UPDATE qbit_prism_instances SET status=$1,heartbeat_at=clock_timestamp() WHERE instance_id='frontend-a'")
            .bind(&status)
            .execute(&fixture.operator.pool)
            .await?;
        let error = fixture.refused().await?;
        assert!(error.contains("frontend-a ("), "{status}: {error}");
    }
    // A heartbeat from the future has no measurable age: unknown is live.
    fixture.kill(&fixture.a).await?;
    fixture.age("frontend-a", -3600.0).await?;
    let error = fixture.refused().await?;
    assert!(
        error.contains("frontend-a (running, heartbeat age unknown)"),
        "{error}"
    );
    // Both frontends are named when both are live.
    fixture.kill(&fixture.a).await?;
    fixture.kill(&fixture.b).await?;
    let error = fixture.refused().await?;
    assert!(
        error.contains("frontend-a (running") && error.contains("frontend-b (running"),
        "{error}"
    );
    assert_eq!(
        fixture.fingerprint().await?,
        Some(fixture.config.fingerprint(GENESIS)?)
    );
    fixture.close().await
}

/// T2: SIGKILL never writes `stopped`. The killed frontend's row refuses
/// while its heartbeat is inside the freshness window and stops refusing
/// once it is outside, through the CLI and its real environment reader.
#[tokio::test]
async fn a_killed_frontend_refuses_until_its_heartbeat_is_stale() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.b.heartbeat(HeartbeatStatus::Stopped).await?;
    fixture.kill(&fixture.a).await?;

    // Immediately after the kill, and one second short of the window.
    for age in [0.0, 14.0] {
        fixture.age("frontend-a", age).await?;
        let output = fixture.cli(OLD_SEEDS).arg("--confirm").output().await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "reset at heartbeat age {age}");
        assert!(
            stderr.contains("frontend-a (running, heartbeat")
                && stderr.contains("older than 15 seconds"),
            "{stderr}"
        );
    }
    // The window follows the frontends' cadence: at twenty seconds it is a
    // minute, so a sixteen-second-old heartbeat is still a live frontend.
    fixture.age("frontend-a", 16.0).await?;
    let before = fixture.state().await?;
    let output = fixture
        .cli(OLD_SEEDS)
        .arg("--confirm")
        .env("PRISM_HEALTH_REFRESH_SECONDS", "20")
        .output()
        .await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && stderr.contains("older than 60 seconds"),
        "{stderr}"
    );
    // A cadence the shared reader rejects fails at the boundary.
    for cadence in ["0", "fast"] {
        let output = fixture
            .cli(OLD_SEEDS)
            .arg("--confirm")
            .env("PRISM_HEALTH_REFRESH_SECONDS", cadence)
            .output()
            .await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !output.status.success() && stderr.contains("PRISM_HEALTH_REFRESH_SECONDS"),
            "{cadence}: {stderr}"
        );
    }
    assert_eq!(fixture.state().await?, before);
    // Empty is unset, as it is for every other reader of this setting: the
    // default window, and a heartbeat sixteen seconds old no longer refuses.
    let output = fixture
        .cli(OLD_SEEDS)
        .arg("--confirm")
        .env("PRISM_HEALTH_REFRESH_SECONDS", "")
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut after = fixture.state().await?;
    let event = after["events"][0].take();
    assert_eq!(after["cluster"]["config_fingerprint"], Value::Null);
    assert_eq!(event["heartbeat_stale_after_seconds"], 15.0);
    let killed = &event["instances"][0];
    assert_eq!(killed["instance_id"], "frontend-a");
    // Still not `stopped`: the journal keeps the health payload it judged
    // stale and the age it measured.
    assert_eq!(killed["status"]["schema"], "qbit.prism.audit-health.v1");
    assert!(killed["heartbeat_age_seconds"].as_f64().unwrap() > 15.0);
    // Nothing else moved; `updated_at` is the reset's own timestamp.
    let mut expected = before;
    expected["cluster"]["config_fingerprint"] = Value::Null;
    for state in [&mut after, &mut expected] {
        state["cluster"]["updated_at"] = Value::Null;
        state["events"] = Value::Null;
    }
    assert_eq!(after, expected);
    fixture.close().await
}

/// A `starting` or unreadable row that went stale is a startup that died.
#[tokio::test]
async fn stale_rows_of_every_other_state_do_not_refuse() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    // frontend-a stays `starting`; frontend-b holds a status nobody can read.
    sqlx::query("UPDATE qbit_prism_instances SET status='{\"state\":\"draining\"}' WHERE instance_id='frontend-b'")
        .execute(&fixture.operator.pool)
        .await?;
    fixture.age("frontend-a", 15.5).await?;
    let error = fixture.refused().await?;
    assert!(
        error.contains("frontend-b (unrecognized status") && !error.contains("frontend-a"),
        "{error}"
    );
    fixture.age("frontend-b", 3600.0).await?;
    fixture.run().await?;
    assert_eq!(fixture.fingerprint().await?, None);
    fixture.close().await
}

/// T3: one `pending` candidate refuses.
#[tokio::test]
async fn a_pending_candidate_refuses_the_reset() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let hash = fixture.enqueue(2895).await?;
    fixture.stop_both().await?;
    assert_eq!(fixture.outbox_state(&hash).await?, "pending");
    let error = fixture.refused().await?;
    assert!(
        error.contains("1 unfinished block candidates (1 pending)") && error.contains(&hash),
        "{error}"
    );
    // A live frontend and an undrained outbox are reported together.
    fixture.kill(&fixture.b).await?;
    let error = fixture.refused().await?;
    assert!(
        error.contains("frontend-b (running") && error.contains("(1 pending)"),
        "{error}"
    );
    fixture.close().await
}

/// T3b: stricter than `configure`. Every row here carries the invoking
/// configuration's own keys, which `configure` would pin straight over.
#[tokio::test]
async fn offered_candidates_with_the_same_keys_refuse_the_reset() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let hash = fixture.enqueue(2896).await?;
    let claim = fixture
        .a
        .claim_candidate(60)
        .await?
        .context("no candidate to claim")?;
    fixture.a.reserve_offer(&claim).await?;
    fixture.stop_both().await?;
    assert_eq!(fixture.outbox_state(&hash).await?, "offer_reserved");
    let error = fixture.refused().await?;
    assert!(
        error.contains("(1 offer_reserved)") && error.contains(&hash),
        "{error}"
    );

    fixture
        .a
        .record_offer(&claim, 1_700_000_000_123, OfferOutcome::Accepted, None)
        .await?;
    assert_eq!(fixture.outbox_state(&hash).await?, "offered");
    let error = fixture.refused().await?;
    assert!(error.contains("(1 offered)"), "{error}");

    fixture
        .a
        .reconcile_candidate(&claim, "landing deferred")
        .await?;
    assert_eq!(fixture.outbox_state(&hash).await?, "reconciliation");
    let error = fixture.refused().await?;
    assert!(error.contains("(1 reconciliation)"), "{error}");

    // The same database with the fingerprint reset by hand: `configure`
    // accepts these keys, so the refusals above are the command's own rule.
    sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=NULL WHERE singleton")
        .execute(&fixture.operator.pool)
        .await?;
    fixture
        .b
        .configure(
            &fixture.config.fingerprint(GENESIS)?,
            &signer_keys(OLD_SEEDS),
        )
        .await?;
    fixture.close().await
}

/// T4: the rotation, end to end through the CLI.
#[tokio::test]
async fn the_reset_is_journaled_and_the_first_new_key_frontend_pins() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    // One frontend shut down, the other was killed an hour ago.
    fixture.a.heartbeat(HeartbeatStatus::Stopped).await?;
    fixture.kill(&fixture.b).await?;
    fixture.age("frontend-b", 3600.0).await?;
    let old = fixture.config.fingerprint(GENESIS)?;
    let revision = fixture.a.payout_revision().await?;

    // The new keys cannot perform the reset: the journal must record the
    // keys being retired.
    let output = fixture.cli(NEW_SEEDS).arg("--confirm").output().await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && stderr.contains("OLD key environment"),
        "{stderr}"
    );
    assert_eq!(fixture.fingerprint().await?, Some(old.clone()));
    assert!(fixture.journal().await?.is_empty());

    let started = Instant::now();
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        fixture.cli(OLD_SEEDS).arg("--confirm").output(),
    )
    .await??;
    let elapsed = started.elapsed();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    eprintln!("[signing-transition] successful CLI run, spawn to exit: {elapsed:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report: Value = serde_json::from_str(&stdout)?;
    let event = &report["signing_transition"];
    assert_eq!(report["config_fingerprint"], Value::Null);
    for step in [
        "first one to start pins the new fingerprint",
        "Keep the old public keys",
        "backfill-ctv or import-audits",
    ] {
        assert!(stdout.contains(step), "{stdout}");
    }
    // Public inputs only.
    for seed in [OLD_SEEDS.0, OLD_SEEDS.1] {
        assert!(!stdout.contains(&seed.repeat(32)), "a seed was printed");
    }

    assert_eq!(fixture.fingerprint().await?, None);
    assert_eq!(fixture.a.payout_revision().await?, revision);
    let journal = fixture.journal().await?;
    assert_eq!(journal, std::slice::from_ref(event));
    assert_eq!(event["transition_id"], 1);
    assert_eq!(event["previous_fingerprint"], old);
    assert_eq!(event["payout_revision"], revision);
    assert_eq!(
        event["previous_policy"],
        serde_json::to_value(policy_of(&fixture.config)?)?
    );
    assert_eq!(
        event["previous_policy"]["manifest_key"],
        key(OLD_SEEDS.0).public_key_hex()
    );
    assert_eq!(
        event["previous_policy"]["ledger_key"],
        key(OLD_SEEDS.1).public_key_hex()
    );
    let instances = event["instances"].as_array().unwrap();
    assert_eq!(instances.len(), 2);
    assert_eq!(instances[0]["status"]["state"], "stopped");
    assert_eq!(instances[1]["instance_id"], "frontend-b");
    assert!(instances[1]["heartbeat_age_seconds"].as_f64().unwrap() > 3599.0);

    // Idempotence: a second run reports the journal row and writes nothing.
    let output = fixture.cli(OLD_SEEDS).arg("--confirm").output().await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success()
            && stderr.contains("nothing to reset")
            && stderr.contains("\"transition_id\":1"),
        "{stderr}"
    );
    assert_eq!(fixture.journal().await?, journal);

    // The first new-key frontend pins the new fingerprint.
    let new_config = config_with(&fixture.database.url, &fixture.node, NEW_SEEDS)?;
    let new = new_config.fingerprint(GENESIS)?;
    assert_ne!(new, old);
    let c = Ledger::connect(&fixture.database.url, "frontend-c".into(), 8, false).await?;
    c.configure(&new, &signer_keys(NEW_SEEDS)).await?;
    assert_eq!(fixture.fingerprint().await?, Some(new.clone()));
    // An old-key frontend is now refused.
    let error = fixture
        .b
        .configure(&old, &signer_keys(OLD_SEEDS))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("cluster configuration fingerprint mismatch"),
        "{error}"
    );
    // And the old environment can no longer reset: the pinned fingerprint
    // belongs to the new configuration.
    c.heartbeat(HeartbeatStatus::Stopped).await?;
    let error = fixture.refused().await?;
    assert!(error.contains("OLD key environment"), "{error}");
    assert_eq!(fixture.fingerprint().await?, Some(new));
    assert_eq!(fixture.journal().await?, journal);

    for statement in [
        "UPDATE qbit_prism_signing_transitions SET previous_fingerprint='forged'",
        "DELETE FROM qbit_prism_signing_transitions",
        "TRUNCATE qbit_prism_signing_transitions",
    ] {
        assert!(sqlx::query(statement)
            .execute(&fixture.operator.pool)
            .await
            .is_err());
    }
    c.pool.close().await;
    fixture.close().await
}

/// The policy document the journal must hold, rebuilt from public inputs.
fn policy_of(config: &Config) -> Result<Value> {
    Ok(json!({
        "schema":2,"genesis":GENESIS,"ledger_key":config.ledger_public_key,
        "manifest_key":ManifestSigningKey::from_seed_hex(&config.manifest_seed)?.public_key_hex(),
        "username_fallback":config.username_fallback,
        "payout_policy":config.payout_policy,"ctv_enabled":config.ctv_enabled,"ctv_config":config.ctv_config,
        "ctv_direct_floor":config.ctv_direct_floor,"ctv_fee":config.ctv_fee,
        "window_multiplier":qbit_prism::PRISM_WINDOW_MULTIPLIER
    }))
}

/// The first configure after a reset pins, whoever runs it. An old-key
/// process that starts then undoes the rotation, safely: the new keys are
/// refused and the reset can be repeated.
#[tokio::test]
async fn an_old_key_start_after_the_reset_repins_and_the_reset_is_repeatable() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.stop_both().await?;
    fixture.run().await?;
    let old = fixture.config.fingerprint(GENESIS)?;
    fixture.b.configure(&old, &signer_keys(OLD_SEEDS)).await?;
    assert_eq!(fixture.fingerprint().await?, Some(old));
    let new_config = config_with(&fixture.database.url, &fixture.node, NEW_SEEDS)?;
    assert!(fixture
        .a
        .configure(&new_config.fingerprint(GENESIS)?, &signer_keys(NEW_SEEDS))
        .await
        .unwrap_err()
        .to_string()
        .contains("cluster configuration fingerprint mismatch"));
    let report = fixture.run().await?;
    assert_eq!(report["signing_transition"]["transition_id"], 2);
    assert_eq!(fixture.fingerprint().await?, None);
    assert_eq!(fixture.journal().await?.len(), 2);
    fixture.close().await
}

/// T5: the command serialises behind a holder of the cluster row and decides
/// on what that holder committed, never on what it read before waiting.
#[tokio::test]
async fn the_command_waits_for_the_cluster_row_and_decides_after_it() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.stop_both().await?;
    // The row lock `configure` holds, around a change of what is pinned.
    for commit in [true, false] {
        let mut holder = fixture.a.pool.begin().await?;
        sqlx::query("SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .execute(&mut *holder)
            .await?;
        sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint='pinned-by-the-holder' WHERE singleton")
            .execute(&mut *holder)
            .await?;
        let before = fixture.state().await?;
        let task = tokio::spawn({
            let operator = fixture.operator.clone();
            let config = fixture.config.clone();
            async move { operator.transition_signing(&config, CADENCE).await }
        });
        wait_for_lock_wait(&fixture.a, "locktype IN ('transactionid','tuple')").await?;
        assert!(!task.is_finished());
        assert_eq!(fixture.state().await?, before);
        if commit {
            // The holder's value is what the command must judge: refused,
            // and the holder's fingerprint is neither reset nor replaced.
            holder.commit().await?;
            let error = format!("{:#}", task.await?.unwrap_err());
            assert!(error.contains("OLD key environment"), "{error}");
            assert_eq!(
                fixture.fingerprint().await?.as_deref(),
                Some("pinned-by-the-holder")
            );
            assert!(fixture.journal().await?.is_empty());
            sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1 WHERE singleton")
                .bind(fixture.config.fingerprint(GENESIS)?)
                .execute(&fixture.operator.pool)
                .await?;
        } else {
            // The holder gave up: the command applies to what is committed.
            holder.rollback().await?;
            task.await??;
            assert_eq!(fixture.fingerprint().await?, None);
            assert_eq!(fixture.journal().await?.len(), 1);
        }
    }
    fixture.close().await
}

/// The 120-second ceiling does not bound a lock wait: the operator
/// connection's `lock_timeout` does, five seconds by default. A wait that
/// runs out changes nothing and tells the operator what still holds the lock.
#[tokio::test]
async fn a_lock_wait_ends_at_the_lock_timeout_and_changes_nothing() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.stop_both().await?;
    let before = fixture.state().await?;
    // What a frontend still running does: hold the cluster row.
    let mut holder = fixture.a.pool.begin().await?;
    sqlx::query("SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
        .execute(&mut *holder)
        .await?;
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        fixture
            .cli(OLD_SEEDS)
            .arg("--confirm")
            .env("PRISM_DATABASE_LOCK_TIMEOUT_MS", "200")
            .output(),
    )
    .await??;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success()
            && stderr.contains("lock timeout")
            && stderr.contains("PRISM_DATABASE_LOCK_TIMEOUT_MS")
            && stderr.contains("Confirm every frontend and tool is stopped"),
        "{stderr}"
    );
    holder.rollback().await?;
    assert_eq!(fixture.state().await?, before);
    // Nothing was left behind: the same command now succeeds.
    let output = fixture
        .cli(OLD_SEEDS)
        .arg("--confirm")
        .env("PRISM_DATABASE_LOCK_TIMEOUT_MS", "200")
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fixture.fingerprint().await?, None);
    fixture.close().await
}

/// T5: between the command's checks and its reset there is no gap. With the
/// command held at its journal write, after every check has passed, neither
/// a heartbeat nor a configure can land; both wait for its commit.
#[tokio::test]
async fn nothing_lands_between_the_checks_and_the_reset() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.a.heartbeat(HeartbeatStatus::Stopped).await?;
    fixture.kill(&fixture.b).await?;
    fixture.age("frontend-b", 3600.0).await?;
    let old = fixture.config.fingerprint(GENESIS)?;

    // Hold the command at its last statement before COMMIT.
    let mut gate = fixture.a.pool.begin().await?;
    sqlx::query("LOCK TABLE qbit_prism_signing_transitions IN EXCLUSIVE MODE")
        .execute(&mut *gate)
        .await?;
    let command = tokio::spawn({
        let operator = fixture.operator.clone();
        let config = fixture.config.clone();
        async move { operator.transition_signing(&config, CADENCE).await }
    });
    wait_for_lock_wait(
        &fixture.a,
        "relation='qbit_prism_signing_transitions'::regclass",
    )
    .await?;

    // The killed frontend turns out to be alive and heartbeats again.
    let before = fixture.state().await?;
    let heartbeat = tokio::spawn({
        let b = fixture.b.clone();
        async move {
            b.heartbeat(HeartbeatStatus::Health(HeartbeatHealth::new(
                true,
                Default::default(),
            )))
            .await
        }
    });
    wait_for_lock_wait(&fixture.a, "relation='qbit_prism_instances'::regclass").await?;
    // And an old-key configure arrives.
    let configure = tokio::spawn({
        let a = fixture.a.clone();
        let old = old.clone();
        async move { a.configure(&old, &signer_keys(OLD_SEEDS)).await }
    });
    wait_for_lock_wait(&fixture.a, "locktype IN ('transactionid','tuple')").await?;
    assert!(!command.is_finished() && !heartbeat.is_finished() && !configure.is_finished());
    // Uncommitted: the fingerprint is still pinned and nothing has moved.
    assert_eq!(fixture.state().await?, before);

    gate.commit().await?;
    let report = command.await??;
    heartbeat.await??;
    // The reset was decided on the stale row and committed before the new
    // heartbeat landed: never a reset under a fresh heartbeat.
    let event = &report["signing_transition"];
    let judged = &event["instances"][1];
    assert_eq!(judged["instance_id"], "frontend-b");
    assert!(judged["heartbeat_age_seconds"].as_f64().unwrap() > 3599.0);
    let ordered: bool = sqlx::query_scalar("SELECT e.activated_at < i.heartbeat_at FROM qbit_prism_signing_transitions e, qbit_prism_instances i WHERE i.instance_id='frontend-b'")
        .fetch_one(&fixture.operator.pool)
        .await?;
    assert!(ordered, "the heartbeat landed before the reset committed");
    // The configure ran after the commit, on the reset row: it is the first
    // configure after a reset, so it pins, exactly as one started a second
    // later would. It did not slip its value in under the command.
    configure.await??;
    assert_eq!(fixture.fingerprint().await?, Some(old));
    assert_eq!(fixture.journal().await?.len(), 1);
    fixture.close().await
}

/// T5: a failure after the reset statement leaves nothing behind.
#[tokio::test]
async fn a_failed_journal_write_rolls_the_reset_back() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.stop_both().await?;
    sqlx::raw_sql("CREATE FUNCTION reject_signing_transition() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected journal failure'; END $$; CREATE TRIGGER reject_signing_transition BEFORE INSERT ON qbit_prism_signing_transitions FOR EACH ROW EXECUTE FUNCTION reject_signing_transition();")
        .execute(&fixture.operator.pool)
        .await?;
    let error = fixture.refused().await?;
    assert!(error.contains("injected journal failure"), "{error}");
    assert_eq!(
        fixture.fingerprint().await?,
        Some(fixture.config.fingerprint(GENESIS)?)
    );
    // The locks went with the transaction: a frontend can still configure.
    fixture
        .a
        .configure(
            &fixture.config.fingerprint(GENESIS)?,
            &signer_keys(OLD_SEEDS),
        )
        .await?;
    sqlx::raw_sql("DROP TRIGGER reject_signing_transition ON qbit_prism_signing_transitions")
        .execute(&fixture.operator.pool)
        .await?;
    fixture.run().await?;
    assert_eq!(fixture.fingerprint().await?, None);
    fixture.close().await
}

/// A halt is cleared with the pinned fingerprint, so it is cleared first.
#[tokio::test]
async fn a_halted_cluster_refuses_the_reset() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.stop_both().await?;
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='unresolved reorg' WHERE singleton")
        .execute(&fixture.operator.pool)
        .await?;
    let error = fixture.refused().await?;
    assert!(
        error.contains("cluster halted: unresolved reorg")
            && error.contains("docs/prism-ledger-ops.md"),
        "{error}"
    );
    fixture.close().await
}

/// T6: without `--confirm` the command prints what it would check and do,
/// exits non-zero, and reads neither its configuration nor the database.
#[tokio::test]
async fn without_confirm_nothing_is_checked_or_changed() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.stop_both().await?;
    let before = fixture.state().await?;
    let mut bare = tokio::process::Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    bare.env_clear()
        .kill_on_drop(true)
        .arg("signing-transition");
    for mut command in [fixture.cli(OLD_SEEDS), bare] {
        let output = command.output().await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success());
        assert!(
            stdout.contains("qbit_prism_signing_transitions")
                && stdout.contains("config_fingerprint to NULL")
                && stdout.contains("offer_reserved"),
            "{stdout}"
        );
        assert!(stderr.contains("--confirm"), "{stderr}");
        assert_eq!(fixture.state().await?, before);
    }
    fixture.close().await
}

/// The reset of the pinned fingerprint is an authority write (#479): it
/// waits for a job cohort's `FOR KEY SHARE` fence on the cluster row.
#[tokio::test]
async fn the_signing_reset_waits_for_a_job_cohort_fence() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    fixture.stop_both().await?;
    let pool = sqlx::PgPool::connect(&fixture.database.url).await?;
    let operator = fixture.operator.clone();
    let config = fixture.config.clone();
    let waited =
        cohort_fence::waits_for_the_cohort_fence(&pool, "the signing transition", async move {
            operator.transition_signing(&config, CADENCE).await
        })
        .await;
    pool.close().await;
    waited??;
    assert_eq!(fixture.fingerprint().await?, None);
    fixture.close().await
}

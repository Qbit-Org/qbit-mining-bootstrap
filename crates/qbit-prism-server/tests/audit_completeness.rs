//! Shared-storage availability and the operator commands that enforce it.
use anyhow::{ensure, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, AuditBundle, FoundBlock, PayoutPolicy};
use qbit_prism_server::ledger::{audit_canonical_bytes, audit_completeness, Ledger};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{process::Output, time::Duration};
use tokio::{process::Command, time::timeout};
#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/pool_fee.rs"]
mod pool_fee;

fn bundle() -> Result<AuditBundle> {
    Ok(qbit_prism::build_audit_bundle(
        vec![AcceptedShare {
            share_seq: 1,
            share_id: "bootstrap-share".into(),
            miner_id: "miner".into(),
            order_key: "miner".into(),
            p2mr_program_hex: "11".repeat(32),
            share_difficulty: 1,
            network_difficulty: 100,
            template_height: 100,
            job_id: "bootstrap-job".into(),
            job_issued_at_ms: 0,
            accepted_at_ms: 0,
            ntime: 1,
            credit_policy: None,
        }],
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: 1,
        },
        vec![],
        PayoutPolicy::day_one_default(),
        &ManifestSigningKey::from_seed_hex(&"93".repeat(32))?,
        &ManifestSigningKey::from_seed_hex(&"94".repeat(32))?,
    )?)
}

fn ledger_key() -> String {
    ManifestSigningKey::from_seed_hex(&"94".repeat(32))
        .unwrap()
        .public_key_hex()
}

async fn assert_counts(
    pool: &PgPool,
    bodies: i64,
    canonical: i64,
    legacy: i64,
    native: i64,
) -> Result<()> {
    let report = audit_completeness(pool).await?;
    ensure!(report.missing_stored_bodies == bodies, "{report:?}");
    ensure!(report.missing_canonical_bytes == canonical, "{report:?}");
    // #734: a row lacking canonical bytes is either legacy or native.
    ensure!(report.pending_legacy_rows == legacy, "{report:?}");
    ensure!(report.incomplete_native_rows == native, "{report:?}");
    ensure!(legacy + native == canonical, "{report:?}");
    ensure!(
        report.require_complete().is_ok() == (bodies == 0 && canonical == 0),
        "completeness gate disagrees with its counts: {report:?}"
    );
    ensure!(
        report.require_native_complete().is_ok() == (native == 0),
        "native completeness gate disagrees with its counts: {report:?}"
    );
    Ok(())
}

#[tokio::test]
async fn counts_distinguish_legacy_imports_and_reconstructible_native_bodies() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    // Connection-local tables also permit a dangling snapshot reference,
    // which the deployed foreign key normally prevents, without weakening it.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&raw)
        .await?;
    let result = async {
        sqlx::raw_sql(
            "SET search_path = pg_temp;
             CREATE TEMP TABLE qbit_pool_audit_bundles (
                 block_hash text, audit_bundle jsonb, audit_bundle_sha256 text,
                 share_snapshot_sha256 text, canonical_audit_bytes bytea, body_uri text);
             CREATE TEMP TABLE qbit_prism_audit_snapshots (
                 snapshot_sha256 text PRIMARY KEY, first_share_seq bigint,
                 last_share_seq bigint, anchor_ms bigint, share_count bigint,
                 inline_shares jsonb, cut_seq_0 bigint, cut_seq_1 bigint);",
        )
        .execute(&pool)
        .await?;
        assert_counts(&pool, 0, 0, 0, 0).await?;
        let bundle = bundle()?;
        let bytes = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        let snapshot = hex::encode(Sha256::digest(serde_json::to_vec(&bundle.shares)?));
        let mut normalized = serde_json::to_value(&bundle)?;
        normalized.as_object_mut().unwrap().remove("shares");
        normalized["reward_manifest"] =
            serde_json::to_value(bundle.reward_manifest.clone().into_parts().0)?;
        sqlx::query("INSERT INTO qbit_prism_audit_snapshots VALUES($1,1,1,1,1,$2)")
            .bind(&snapshot)
            .bind(serde_json::to_value(&bundle.shares)?)
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles VALUES('native',$1,$2,$3,NULL,NULL)")
            .bind(&normalized).bind(&digest).bind(&snapshot).execute(&pool).await?;
        assert_counts(&pool, 0, 0, 0, 0).await?;
        ensure!(audit_canonical_bytes(&pool, "native").await? == Some(bytes.clone()));

        // A valid native snapshot alone cannot replace the metadata body.
        sqlx::query("INSERT INTO qbit_pool_audit_bundles VALUES('native-no-body',NULL,$1,$2,NULL,NULL)")
            .bind(&digest).bind(&snapshot).execute(&pool).await?;
        assert_counts(&pool, 1, 1, 0, 1).await?;
        // A body alone cannot reconstruct a native snapshot that is absent.
        sqlx::query("INSERT INTO qbit_pool_audit_bundles VALUES('dangling',$1,$2,'absent',NULL,NULL)")
            .bind(&normalized).bind(&digest).execute(&pool).await?;
        assert_counts(&pool, 1, 2, 0, 2).await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles VALUES('external',NULL,$1,NULL,NULL,'/unmounted/history.json')")
            .bind(&digest).execute(&pool).await?;
        assert_counts(&pool, 2, 3, 1, 2).await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles VALUES('inline',$1,$2,NULL,NULL,NULL)")
            .bind(serde_json::to_value(&bundle)?).bind(&digest).execute(&pool).await?;
        assert_counts(&pool, 2, 4, 2, 2).await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles VALUES('imported',NULL,$1,NULL,$2,NULL)")
            .bind(&digest).bind(&bytes).execute(&pool).await?;
        assert_counts(&pool, 2, 4, 2, 2).await?;
        ensure!(audit_canonical_bytes(&pool, "imported").await? == Some(bytes));
        for body in [Value::Null, json!([]), json!("not a body")] {
            sqlx::query("INSERT INTO qbit_pool_audit_bundles(audit_bundle) VALUES($1)")
                .bind(body).execute(&pool).await?;
        }
        assert_counts(&pool, 5, 7, 5, 2).await
    }
    .await;
    pool.close().await;
    result
}

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
    ledger: Ledger,
}

impl Database {
    async fn open(raw: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let schema = format!("audit_completeness_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger = Ledger::connect(url.as_str(), "audit-check-fixture".into(), 4, true).await?;
        Ok(Self {
            admin,
            schema,
            url: url.to_string(),
            ledger,
        })
    }

    async fn seed(
        &self,
        bundle: &AuditBundle,
        body: Option<Value>,
        uri: Option<&str>,
    ) -> Result<()> {
        let verified =
            qbit_prism::verify_audit_bundle_with_ledger_public_key(bundle, &ledger_key())?;
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256) VALUES($1,101,$2,$3,$4)")
            .bind("aa".repeat(32)).bind("bb".repeat(32)).bind(&verified.coinbase_txid)
            .bind("cc".repeat(32)).execute(&self.ledger.pool).await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,body_uri) VALUES($1,$2,$3,$4,$5)")
            .bind("aa".repeat(32)).bind(body)
            .bind(hex::encode(Sha256::digest(qbit_prism::canonical_audit_bundle_bytes(bundle)?)))
            .bind(&verified.coinbase_tx_hex).bind(uri).execute(&self.ledger.pool).await?;
        Ok(())
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

async fn command(db: &Database, args: &[&str], production: bool) -> Result<Output> {
    command_with(db, args, production, &[]).await
}

/// `command` with extra environment, applied last so it overrides.
async fn command_with(
    db: &Database,
    args: &[&str],
    production: bool,
    extra: &[(&str, &str)],
) -> Result<Output> {
    let secrets = tempfile::tempdir()?;
    let manifest = secrets.path().join("manifest");
    let ledger = secrets.path().join("ledger");
    std::fs::write(&manifest, "93".repeat(32))?;
    std::fs::write(&ledger, "94".repeat(32))?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command
        .args(args)
        .kill_on_drop(true)
        .env_clear()
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env("PRISM_INSTANCE_ID", "audit-completeness-cli")
        .env("PRISM_DATABASE_URL", &db.url)
        .env("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", ledger_key())
        .env("PRISM_MANIFEST_SIGNING_SEED_HEX_FILE", manifest)
        .env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX_FILE", ledger)
        .env("QBIT_CHAIN", if production { "mainnet" } else { "regtest" })
        .env("QBIT_RPC_URL", "http://127.0.0.1:0/")
        .env("QBIT_RPC_USER", "operator")
        .env("QBIT_RPC_PASSWORD", "test-only-password")
        .env("PRISM_RPC_TIMEOUT_SECONDS", "1")
        .env("QBIT_EXPECTED_GENESIS_HASH", "ab".repeat(32))
        .env("PRISM_STRATUM_STALE_GRACE_SECONDS", "0")
        .env("PRISM_STRATUM_SHARE_DIFF", "16")
        .env("PRISM_STRATUM_VARDIFF_MIN_DIFF", "1")
        .env("PRISM_STRATUM_VARDIFF_START_DIFF", "16")
        .env("PRISM_STRATUM_VARDIFF_MAX_DIFF", "1024")
        .envs(pool_fee::ZERO_BPS_POOL_FEE)
        .envs(extra.iter().copied());
    let output = timeout(Duration::from_secs(20), command.output()).await??;
    for stream in [&output.stdout, &output.stderr] {
        ensure!(!String::from_utf8_lossy(stream).contains("test-only-password"));
    }
    Ok(output)
}

/// #734: the operator's declaration that import-audits is under way.
const DECLARED: [(&str, &str); 1] = [("PRISM_SELF_CHECK_LEGACY_IMPORT_PENDING", "1")];

/// self-check's `audit_completeness` section.
fn audits(status: &str, bodies: i64, canonical: i64, legacy: i64, native: i64) -> Value {
    json!({
        "status": status,
        "missing_stored_bodies": bodies,
        "missing_canonical_bytes": canonical,
        "pending_legacy_rows": legacy,
        "incomplete_native_rows": native,
    })
}

/// Run self-check, which must fail, and return its report and stderr.
async fn failed_self_check(
    db: &Database,
    production: bool,
    extra: &[(&str, &str)],
) -> Result<(Value, String)> {
    let output = command_with(db, &["self-check"], production, extra).await?;
    let error = String::from_utf8_lossy(&output.stderr).into_owned();
    ensure!(!output.status.success(), "self-check must fail: {error}");
    let report: Value = serde_json::from_slice(&output.stdout)?;
    ensure!(report["ok"] == false, "{report}");
    Ok((report, error))
}

#[tokio::test]
async fn self_check_reports_missing_audits_before_rpc_and_enforces_production() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = Database::open(&raw).await?;
    let result = async {
        let bundle = bundle()?;
        db.seed(&bundle, None, Some("/unmounted/history.json"))
            .await?;
        for (body, canonical, bodies, missing) in [
            (None, None, 1, 1),
            (Some(serde_json::to_value(&bundle)?), None, 0, 1),
            (
                None,
                Some(qbit_prism::canonical_audit_bundle_bytes(&bundle)?),
                0,
                0,
            ),
        ] {
            sqlx::query(
                "UPDATE qbit_pool_audit_bundles SET audit_bundle=$1,canonical_audit_bytes=$2",
            )
            .bind(body)
            .bind(canonical)
            .execute(&db.ledger.pool)
            .await?;
            for (production, declared) in
                [(false, false), (false, true), (true, false), (true, true)]
            {
                let (report, error) =
                    failed_self_check(&db, production, if declared { &DECLARED } else { &[] })
                        .await?;
                // Only the legacy row is missing, so a declared import is
                // pending rather than incomplete.
                let status = match (missing, declared) {
                    (0, _) => "complete",
                    (_, true) => "legacy_import_pending",
                    _ => "incomplete",
                };
                ensure!(
                    report["audit_completeness"] == audits(status, bodies, missing, missing, 0),
                    "{report}"
                );
                // #734: the local checks run whatever the history holds; the
                // unavailable RPC is what fails them.
                ensure!(
                    error.contains("qbit RPC getblockhash transport failed"),
                    "{error}"
                );
                if production && status == "incomplete" {
                    ensure!(error.contains("audit history is incomplete"), "{error}");
                    ensure!(error.contains("run import-audits"), "{error}");
                    ensure!(
                        error.contains("set PRISM_SELF_CHECK_LEGACY_IMPORT_PENDING=1"),
                        "{error}"
                    );
                } else {
                    // Lab reports incompleteness but never fails on it.
                    ensure!(!error.contains("audit history is incomplete"), "{error}");
                }
                ensure!(
                    error.contains("legacy audit rows await import-audits, as")
                        == (status == "legacy_import_pending"),
                    "{error}"
                );
            }
        }
        // A declaration that does not parse fails before anything is read.
        let (report, error) = failed_self_check(
            &db,
            true,
            &[("PRISM_SELF_CHECK_LEGACY_IMPORT_PENDING", "pending")],
        )
        .await?;
        ensure!(report["audit_completeness"].is_null(), "{report}");
        ensure!(
            error.contains("PRISM_SELF_CHECK_LEGACY_IMPORT_PENDING must be a boolean"),
            "{error}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = db.close().await;
    result?;
    cleanup
}

/// #734: a declared legacy import excuses only legacy rows. A native row that
/// lacks its body or snapshot fails production's check either way, beside a
/// pending legacy row, after the local checks have run.
#[tokio::test]
async fn self_check_fails_on_an_incomplete_native_row_even_with_a_declared_import() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = Database::open(&raw).await?;
    let result = async {
        let bundle = bundle()?;
        db.seed(&bundle, None, Some("/unmounted/history.json"))
            .await?;
        let snapshot = hex::encode(Sha256::digest(serde_json::to_vec(&bundle.shares)?));
        sqlx::query("INSERT INTO qbit_prism_audit_snapshots(snapshot_sha256,first_share_seq,last_share_seq,anchor_ms,share_count,inline_shares) VALUES($1,1,1,1,1,$2)")
            .bind(&snapshot).bind(serde_json::to_value(&bundle.shares)?)
            .execute(&db.ledger.pool).await?;
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256) VALUES($1,102,$2,$3,$4)")
            .bind("dd".repeat(32)).bind("aa".repeat(32)).bind("ee".repeat(32))
            .bind("ff".repeat(32)).execute(&db.ledger.pool).await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle_sha256,coinbase_tx_hex,body_uri,share_snapshot_sha256) SELECT $1,$2,coinbase_tx_hex,'/unmounted/native.json',$3 FROM qbit_pool_audit_bundles")
            .bind("dd".repeat(32)).bind("12".repeat(32)).bind(&snapshot)
            .execute(&db.ledger.pool).await?;
        assert_counts(&db.ledger.pool, 2, 2, 1, 1).await?;
        for (production, declared) in [(false, false), (false, true), (true, false), (true, true)] {
            let (report, error) =
                failed_self_check(&db, production, if declared { &DECLARED } else { &[] }).await?;
            ensure!(
                report["audit_completeness"] == audits("incomplete", 2, 2, 1, 1),
                "{report}"
            );
            ensure!(
                error.contains("qbit RPC getblockhash transport failed"),
                "{error}"
            );
            ensure!(
                error.contains(
                    "1 native rows lack their metadata body or share snapshot, which \
                     import-audits does not repair"
                ) == production,
                "{error}"
            );
            ensure!(!error.contains("await import-audits"), "{error}");
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = db.close().await;
    result?;
    cleanup
}

/// #734: with a reachable node, production self-check fills in every local
/// check while a legacy import is pending. Declared, the import is reported
/// as pending and the first failure is the healthcheck of a frontend this
/// test does not run; undeclared, the same report fails on the audits.
#[tokio::test]
async fn self_check_runs_every_local_check_while_a_legacy_import_is_pending() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = Database::open(&raw).await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let result = async {
        let bundle = bundle()?;
        db.seed(&bundle, None, Some("/unmounted/history.json"))
            .await?;
        let genesis = "00".repeat(32);
        let node_env = [
            ("QBIT_PRODUCTION", "1"),
            ("QBIT_CHAIN", "testnet"),
            ("QBIT_RPC_URL", node.url.as_str()),
            ("QBIT_EXPECTED_GENESIS_HASH", genesis.as_str()),
            // Nothing listens on port 1, so the readiness probe fails.
            ("PRISM_AUDIT_PORT", "1"),
        ];
        for declared in [true, false] {
            let mut extra = node_env.to_vec();
            if declared {
                extra.extend(DECLARED);
            }
            let (report, error) = failed_self_check(&db, true, &extra).await?;
            let status = if declared {
                "legacy_import_pending"
            } else {
                "incomplete"
            };
            ensure!(
                report["audit_completeness"] == audits(status, 1, 1, 1, 0),
                "{report}"
            );
            for local in ["health", "carry_forward_integrity", "durability"] {
                ensure!(!report[local].is_null(), "{local} not reported: {report}");
            }
            // Every local check before the readiness probe passed; the probe
            // finds no frontend, and highdiff is not configured.
            ensure!(error.contains("127.0.0.1:1/healthz"), "{error}");
            ensure!(
                error.contains("audit history is incomplete") == !declared,
                "{error}"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = db.close().await;
    result?;
    cleanup
}

#[tokio::test]
async fn import_cli_reports_remaining_audits_on_success_and_failure() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = Database::open(&raw).await?;
    let result = async {
        let bundle = bundle()?;
        db.seed(&bundle, Some(serde_json::to_value(&bundle)?), None).await?;
        for expected in [1, 0] {
            let output = command(&db, &["import-audits"], false).await?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            ensure!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            ensure!(stdout.contains(&format!("Imported {expected} audit bodies")), "{stdout}");
            ensure!(stdout.contains("\"missing_stored_bodies\":0"), "{stdout}");
            ensure!(stdout.contains("\"missing_canonical_bytes\":0"), "{stdout}");
            assert_counts(&db.ledger.pool, 0, 0, 0, 0).await?;
        }
        // A failed repair still prints the unfinished-work counts and never
        // labels an unavailable filesystem body as durably imported.
        sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=NULL,canonical_audit_bytes=NULL,body_uri='/unmounted/history.json'")
            .execute(&db.ledger.pool).await?;
        let output = command(&db, &["import-audits"], false).await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        ensure!(!output.status.success());
        ensure!(stdout.contains("\"missing_stored_bodies\":1"), "{stdout}");
        ensure!(stdout.contains("\"missing_canonical_bytes\":1"), "{stdout}");

        // The importer skips native rows. Its zero-row result must still
        // fail when native metadata is missing, even in lab mode.
        let snapshot = hex::encode(Sha256::digest(serde_json::to_vec(&bundle.shares)?));
        sqlx::query("INSERT INTO qbit_prism_audit_snapshots(snapshot_sha256,first_share_seq,last_share_seq,anchor_ms,share_count,inline_shares) VALUES($1,1,1,1,1,$2)")
            .bind(&snapshot).bind(serde_json::to_value(&bundle.shares)?)
            .execute(&db.ledger.pool).await?;
        sqlx::query("UPDATE qbit_pool_audit_bundles SET share_snapshot_sha256=$1")
            .bind(snapshot).execute(&db.ledger.pool).await?;
        let output = command(&db, &["import-audits"], false).await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        ensure!(!output.status.success());
        ensure!(stdout.contains("Imported 0 audit bodies"), "{stdout}");
        ensure!(stdout.contains("\"missing_stored_bodies\":1"), "{stdout}");
        ensure!(stdout.contains("\"missing_canonical_bytes\":1"), "{stdout}");
        ensure!(stdout.contains("\"incomplete_native_rows\":1"), "{stdout}");
        ensure!(stderr.contains("audit history is incomplete"), "{stderr}");
        Ok::<_, anyhow::Error>(())
    }.await;
    let cleanup = db.close().await;
    result?;
    cleanup
}

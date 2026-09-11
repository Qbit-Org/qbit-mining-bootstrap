//! Harness pieces shared by the decision-D2 coordinator tests of issue #309,
//! `d2_bootstrap_tests` and `d2_below_target_tests`: the integration guard, a
//! throwaway schema, the coordinator configuration both run, and the payout
//! projection both compare against `qbit-prism`'s frozen money-path vectors.

use super::*;
use anyhow::anyhow;
use sqlx::PgPool;

/// Regtest-style compact bits. `codec::scaled_target_difficulty` reads them as
/// exactly 1_000_000, the network difficulty the below-target credit vectors
/// were exported against, and any header nonce passes the block target within
/// a few thousand tries.
pub(super) const TEMPLATE_BITS: &str = "207fffff";
pub(super) const EXTRANONCE1: &str = "00000000";
pub(super) const EXTRANONCE2_SIZE: usize = 8;

/// The fake node's template time. It must be the real clock: a fixed stamp
/// would age past `template_max_age` and turn every test into "template is
/// stale" once the wall clock overtakes it.
pub(super) fn unix_now() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs())
}

// ---------------------------------------------------------------------------
// Integration guard
// ---------------------------------------------------------------------------

/// Decides whether a D2 test runs, fails or skips.
///
/// | `PRISM_TEST_DATABASE_URL` | other variables | result |
/// | --- | --- | --- |
/// | set and non-empty | -- | run against that database |
/// | unset or empty | `PRISM_TEST_REQUIRE_INTEGRATION=1` | fail, naming the variable |
/// | unset or empty | `GITHUB_JOB=prism-native-postgres` | fail, naming the variable |
/// | unset or empty | -- | print a skip line and return |
///
/// The three variables play different roles. This repository sets
/// `PRISM_TEST_DATABASE_URL` itself, in the `prism-native-postgres` job of
/// `.github/workflows/ci.yml`. GitHub sets `GITHUB_JOB` to the running job's
/// id, so matching it on `prism-native-postgres` means a database outage in
/// that job surfaces as a failure instead of a silent pass, even though
/// nothing in the repository writes that variable. Nothing sets
/// `PRISM_TEST_REQUIRE_INTEGRATION` yet: it is an opt-in switch proposed by
/// #286 for a run that wants every integration test to be mandatory, honoured
/// here in advance so that adopting it needs no change to this file.
///
/// Keying on `CI` instead would be wrong: GitHub sets `CI=true` in every job,
/// including `rust-tests`, which builds and runs the whole workspace with no
/// database at all.
///
/// An empty or whitespace-only URL counts as unset. A non-empty but malformed
/// URL is deliberately not second-guessed here; it reaches `sqlx` and fails
/// the test with the connection error, which is the diagnostic an operator
/// needs.
///
/// This is the same guard as `tests/window_read_oracle.rs` `database_url`.
pub(super) fn database_url(test_name: &str) -> Result<Option<String>> {
    let configured = std::env::var("PRISM_TEST_DATABASE_URL").unwrap_or_default();
    let configured = configured.trim();
    if !configured.is_empty() {
        return Ok(Some(configured.to_owned()));
    }
    let required_by = if matches!(
        std::env::var("PRISM_TEST_REQUIRE_INTEGRATION").as_deref(),
        Ok("1")
    ) {
        Some("PRISM_TEST_REQUIRE_INTEGRATION=1")
    } else if matches!(
        std::env::var("GITHUB_JOB").as_deref(),
        Ok("prism-native-postgres")
    ) {
        Some("GITHUB_JOB=prism-native-postgres")
    } else {
        None
    };
    if let Some(signal) = required_by {
        anyhow::bail!(
            "{test_name} requires PostgreSQL: PRISM_TEST_DATABASE_URL is unset or empty while \
             {signal} demands the integration suite"
        );
    }
    eprintln!("skipping {test_name}: PRISM_TEST_DATABASE_URL is not set");
    Ok(None)
}

// ---------------------------------------------------------------------------
// Schema and configuration
// ---------------------------------------------------------------------------

/// A throwaway schema on the configured server, reached through a URL whose
/// `search_path` points at it.
pub(super) struct TestSchema {
    admin: PgPool,
    name: String,
    url: String,
}

impl TestSchema {
    pub(super) async fn create(raw: &str, prefix: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let name = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
        let url = match url::Url::parse(raw) {
            Ok(mut url) => {
                url.query_pairs_mut()
                    .append_pair("options", &format!("-csearch_path={name}"));
                url.to_string()
            }
            Err(error) => {
                admin.close().await;
                return Err(error.into());
            }
        };
        if let Err(error) = sqlx::query(&format!("CREATE SCHEMA {name}"))
            .execute(&admin)
            .await
        {
            admin.close().await;
            return Err(error.into());
        }
        Ok(Self { admin, name, url })
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }

    pub(super) async fn remove(self) -> Result<()> {
        let dropped = sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.name))
            .execute(&self.admin)
            .await;
        self.admin.close().await;
        dropped?;
        Ok(())
    }

    /// Remove the schema of a fixture that failed to open, and keep the setup
    /// error as the one the test reports.
    pub(super) async fn abandon(self, error: anyhow::Error) -> anyhow::Error {
        match self.remove().await {
            Ok(()) => error,
            Err(cleanup) => anyhow!("{error:#}\n(schema cleanup also failed: {cleanup:#})"),
        }
    }
}

/// Combine a test body's outcome with its cleanup. The test's own failure is
/// the one reported; a cleanup failure is reported on its own only when the
/// test passed, and is appended otherwise rather than replacing it.
pub(super) fn settle(outcome: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (outcome, cleanup) {
        (Ok(()), cleanup) => cleanup,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => Err(anyhow!(
            "{error:#}\n(fixture cleanup also failed: {cleanup:#})"
        )),
    }
}

/// The coordinator both D2 harnesses run: chain `testnet`, CTV off, the
/// day-one payout policy and no pool fee address, pointed at a throwaway
/// schema and a fake qbit node.
pub(super) fn test_config(
    database_url: &str,
    rpc_url: String,
    instance_id: &str,
    share_commit_timeout: Duration,
) -> Result<Config> {
    Ok(Config {
        database_url: database_url.to_owned(),
        instance_id: instance_id.into(),
        database_connections: 6,
        initialize_schema: true,
        chain: "testnet".into(),
        expected_genesis_hash: None,
        min_peers: 1,
        template_max_age: Duration::from_secs(120),
        rpc_url,
        rpc_user: "test".into(),
        rpc_password: "test".into(),
        rpc_timeout: Duration::from_secs(10),
        block_submit_timeout: Duration::from_secs(10),
        poll_interval: Duration::from_secs(1),
        blockwait: false,
        build_workers: 1,
        runtime_workers: 2,
        snapshot_interval: Duration::from_secs(60),
        health_timeout: Duration::from_secs(60),
        share_commit_timeout,
        extranonce2_size: EXTRANONCE2_SIZE,
        coinbase_tag: "/PRISM/".into(),
        manifest_seed: "11".repeat(32),
        ledger_seed: "22".repeat(32),
        ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex(),
        username_fallback: None,
        payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
        fee_address: None,
        ctv_enabled: false,
        ctv_config: qbit_prism::SettlementModeConfig::default(),
        ctv_direct_floor: 10_485_760,
        ctv_fee: None,
        ctv_fee_premium_bps: 12000,
        ctv_broadcast: false,
        ctv_broadcast_interval: Duration::from_secs(10),
        version_mask: codec::VERSION_ROLLING_MASK,
        audit_bind: "127.0.0.1".into(),
        audit_port: 0,
    })
}

// ---------------------------------------------------------------------------
// Projection and comparison
// ---------------------------------------------------------------------------

/// The payout consequence of one bundle, in exactly the shape
/// `crates/qbit-prism/tests/money_path_vectors.rs` `bundle_payout` records:
/// the counted window, the entitlements it produces and the payout policy
/// manifest applied to them.
pub(super) fn bundle_payout(bundle: &AuditBundle) -> Result<Value> {
    let reward = &bundle.reward_manifest;
    Ok(json!({
        "counted_window_weight": serde_json::to_value(reward.counted_window_weight)?,
        "counted_shares": reward
            .shares
            .iter()
            .map(|share| {
                Ok(json!({
                    "share_seq": share.share_seq,
                    "miner_id": share.miner_id,
                    "counted_difficulty": serde_json::to_value(share.counted_difficulty)?,
                }))
            })
            .collect::<Result<Vec<_>>>()?,
        "entitlements": serde_json::to_value(&reward.entitlements)?,
        "payout_policy_manifest": serde_json::to_value(&bundle.payout_policy_manifest)?,
    }))
}

/// Record every leaf where `actual` departs from `expected`.
fn diff(path: &str, expected: &Value, actual: &Value, mismatches: &mut Vec<String>) {
    match (expected, actual) {
        (Value::Object(want), Value::Object(got)) => {
            for key in want
                .keys()
                .chain(got.keys().filter(|key| !want.contains_key(*key)))
            {
                let child = format!("{path}.{key}");
                match (want.get(key), got.get(key)) {
                    (Some(want), Some(got)) => diff(&child, want, got, mismatches),
                    (Some(want), None) => {
                        mismatches.push(format!("{child}: expected {want}, got nothing"))
                    }
                    (None, Some(got)) => {
                        mismatches.push(format!("{child}: expected nothing, got {got}"))
                    }
                    (None, None) => unreachable!(),
                }
            }
        }
        (Value::Array(want), Value::Array(got)) if want.len() == got.len() => {
            for (index, (want, got)) in want.iter().zip(got).enumerate() {
                diff(&format!("{path}[{index}]"), want, got, mismatches);
            }
        }
        _ if expected == actual => {}
        _ => mismatches.push(format!("{path}: expected {expected}, got {actual}")),
    }
}

/// Every leaf path where `actual` departs from `expected`, empty when equal.
pub(super) fn mismatches(path: &str, expected: &Value, actual: &Value) -> Vec<String> {
    let mut found = Vec::new();
    diff(path, expected, actual, &mut found);
    found
}

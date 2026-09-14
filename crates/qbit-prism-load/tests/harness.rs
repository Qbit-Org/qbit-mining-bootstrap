//! Database-free unit coverage for the load harness.
//!
//! Every assertion here is checked against the production code it has to
//! agree with: the server's `codec`, its `capacity` validator and the refusal
//! text its runtime actually logs.

use anyhow::Result;
use num_bigint::BigUint;
use qbit_prism_load::{
    artifact::{self, ArtifactInputs, PhaseEvidence},
    classify::{self, BlockedKind, Rejection, RejectionClass},
    client, digest,
    frontend::{self, FrontendSpec, SharedEnvironment},
    node::{self, NodeState},
    profile, run, window,
};
use qbit_prism_server::{
    capacity::{validate_capacity_evidence, CONFIGURATION_KEYS, REQUIRED_PHASES, SUBJECT_KEYS},
    codec,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};

// --- header building and share identifiers -------------------------------

/// A minimal but structurally real non-witness coinbase, split at the
/// extranonce placeholder exactly where `codec::split_coinbase_extranonce`
/// would split it.
fn coinbase_halves(extranonce2_size: usize) -> (Vec<u8>, Vec<u8>) {
    let prefix = format!(
        "01000000\
         01{}ffffffff\
         {:02x}03650000",
        "00".repeat(32),
        4 + extranonce2_size + 4
    );
    let suffix = format!(
        "ffffffff\
         01\
         00f2052a01000000\
         22 5220 {}\
         00000000",
        "11".repeat(32)
    )
    .replace(' ', "");
    (hex::decode(prefix).unwrap(), hex::decode(suffix).unwrap())
}

fn sample_job() -> (codec::Job, String) {
    let extranonce2_size = 8usize;
    let (coinb1, coinb2) = coinbase_halves(extranonce2_size);
    let previousblockhash =
        "00000000000000000001aabbccddeeff00112233445566778899aabbccddeeff".to_owned();
    let mut prevhash_bytes = hex::decode(&previousblockhash).unwrap();
    prevhash_bytes.reverse();
    client::word_swap(&mut prevhash_bytes);
    let nbits = codec::parse_u32_hex(window::TEMPLATE_BITS).unwrap();
    let network_target = codec::target_from_compact(nbits).unwrap();
    let difficulty = 1.2207031e-8f64;
    let share_target = codec::difficulty_target(difficulty)
        .unwrap()
        .max(network_target.clone());
    let extranonce1 = "deadbeef".to_owned();
    let job = codec::Job {
        job_id: "load-job-1".into(),
        previousblockhash,
        prevhash: hex::encode(&prevhash_bytes),
        coinb1: hex::encode(&coinb1).into(),
        coinb2: hex::encode(&coinb2).into(),
        full_coinbase_prefix: hex::encode(&coinb1).into(),
        full_coinbase_suffix: hex::encode(&coinb2).into(),
        merkle_branch: Vec::new(),
        transactions: Arc::new(Vec::new()),
        version: 0x2000_0000,
        version_mask: codec::VERSION_ROLLING_MASK,
        nbits,
        ntime: 1_800_000_000,
        mintime: 1_799_999_999,
        share_difficulty: codec::target_difficulty(&share_target).unwrap(),
        network_target,
        share_target,
        extranonce1: extranonce1.clone(),
        extranonce2_size,
        clean_jobs: true,
        resume_expires_at: None,
        refresh_generation: 0,
        payout_revision: 0,
    };
    (job, extranonce1)
}

#[test]
fn client_headers_and_share_ids_match_the_server_codec() -> Result<()> {
    let (job, extranonce1) = sample_job();
    let extranonce1_bytes = hex::decode(&extranonce1)?;
    let username = "pload1abcdef01.s00007";
    for nonce in [0u32, 1, 7, 0x1234_5678, u32::MAX] {
        for extranonce2 in ["0000000000000001", "00000000cafebabe", "ffffffffffffffff"] {
            let extranonce2_bytes = hex::decode(extranonce2)?;
            let server = job.assemble_submission(
                extranonce2,
                &format!("{:08x}", job.ntime),
                &format!("{nonce:08x}"),
                None,
                job.version_mask,
            )?;
            let merkle = client::merkle_root(
                &hex::decode(job.coinb1.as_ref())?,
                &extranonce1_bytes,
                &extranonce2_bytes,
                &hex::decode(job.coinb2.as_ref())?,
                &[],
            );
            let header_prev = client::header_prev_from_wire(&job.prevhash)?;
            let header = client::assemble_header(
                job.version,
                &header_prev,
                &merkle,
                job.ntime,
                job.nbits,
                nonce,
            );
            assert_eq!(
                hex::encode(&header),
                server.header_hex,
                "client header must equal the codec's header"
            );
            assert_eq!(
                client::share_id(username, &header),
                format!("{username}:{}", server.block_hash_hex),
                "client share_id must equal the ledger's share_id"
            );
        }
    }
    Ok(())
}

#[test]
fn wire_prevhash_round_trips_to_the_tip_hash() -> Result<()> {
    let (job, _) = sample_job();
    assert_eq!(
        client::tip_from_wire_prevhash(&job.prevhash)?,
        job.previousblockhash
    );
    Ok(())
}

#[test]
fn little_endian_target_comparison_matches_bignum_comparison() -> Result<()> {
    let target = codec::difficulty_target(1.2207031e-8)?;
    let bytes = client::target_bytes_le(&target);
    for seed in 0u32..512 {
        let hash = codec::double_sha256(&seed.to_le_bytes());
        let expected = BigUint::from_bytes_le(&hash) <= target;
        assert_eq!(
            client::le_at_most(&hash, &bytes),
            expected,
            "byte comparison must agree with the codec's BigUint comparison"
        );
    }
    Ok(())
}

// --- digest canonicalisation ---------------------------------------------

#[test]
fn share_id_digest_is_sorted_deduplicated_and_newline_terminated() {
    let ids = ["b:2", "a:1", "b:2", "c:3"];
    let expected = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for id in ["a:1", "b:2", "c:3"] {
            hasher.update(id.as_bytes());
            hasher.update(b"\n");
        }
        hex::encode(hasher.finalize())
    };
    assert_eq!(digest::share_id_digest(ids), expected);
    // Input order must not matter.
    assert_eq!(
        digest::share_id_digest(["c:3", "b:2", "a:1"]),
        digest::share_id_digest(["a:1", "b:2", "c:3"])
    );
    // Byte order, not locale order: uppercase sorts before lowercase.
    assert_ne!(
        digest::share_id_digest(["B", "a"]),
        digest::share_id_digest(["a", "B\n"])
    );
    assert_eq!(
        digest::share_id_digest(Vec::<&str>::new()),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "the empty set digests the empty string"
    );
}

#[test]
fn reconciliation_separates_missing_from_unexpected() {
    let offered: std::collections::BTreeSet<String> =
        ["a", "b", "c"].into_iter().map(String::from).collect();
    let acknowledged: std::collections::BTreeSet<String> =
        ["a", "b"].into_iter().map(String::from).collect();
    let committed: std::collections::BTreeSet<String> =
        ["a", "c"].into_iter().map(String::from).collect();
    let reconciliation = digest::reconcile(offered, acknowledged, &committed);
    assert_eq!(
        reconciliation.missing.iter().collect::<Vec<_>>(),
        vec!["b"],
        "an acknowledged share with no row is missing"
    );
    assert_eq!(
        reconciliation.committed.iter().collect::<Vec<_>>(),
        vec!["a", "c"]
    );
    let attribution =
        digest::attribute_unexpected(&committed, &[("steady_state".to_owned(), &reconciliation)]);
    assert_eq!(attribution.by_phase.len(), 1);
    assert_eq!(attribution.by_phase[0].1, vec!["c".to_owned()]);
    assert!(attribution.outside_phases.is_empty());
}

#[test]
fn unexpected_rows_outside_every_phase_are_attributed_to_no_phase() {
    let offered: std::collections::BTreeSet<String> = ["a"].into_iter().map(String::from).collect();
    let acknowledged = offered.clone();
    let committed: std::collections::BTreeSet<String> =
        ["a", "z"].into_iter().map(String::from).collect();
    let reconciliation = digest::reconcile(offered, acknowledged, &committed);
    let attribution =
        digest::attribute_unexpected(&committed, &[("steady_state".to_owned(), &reconciliation)]);
    assert!(attribution.by_phase.is_empty());
    assert_eq!(attribution.outside_phases, vec!["z".to_owned()]);
}

// --- window arithmetic ---------------------------------------------------

#[test]
fn template_bits_give_the_documented_network_difficulty() -> Result<()> {
    let bits = codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    assert_eq!(window::scaled_network_difficulty(bits)?, 65_536_000_000);
    // The stock fake-node bits make every valid share a block, which is why
    // the harness does not use them.
    assert_eq!(window::scaled_network_difficulty(0x207f_ffff)?, 1_000_000);
    Ok(())
}

#[test]
fn solved_window_sizes_are_exact_and_never_the_lab_difficulty() -> Result<()> {
    let bits = codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    for requested in [1_000u64, 5_000, 20_000, 50_000, 100_000, 200_000, 400_000] {
        let solution = window::solve_window(bits, requested)?;
        assert_eq!(solution.computed_window, requested, "window must be exact");
        assert_eq!(solution.requested_window, requested);
        assert_ne!(
            solution.share_difficulty,
            window::FORBIDDEN_DIFFICULTY,
            "check-config and capacity-evidence both refuse exactly 1e-9"
        );
        assert!(solution.share_difficulty.is_finite() && solution.share_difficulty > 0.0);
        // The scaled difficulty the harness records must be the one the server
        // derives from the configured f64, through its own codec.
        let target = codec::difficulty_target(solution.share_difficulty)?
            .max(codec::target_from_compact(bits)?);
        assert_eq!(
            codec::scaled_target_difficulty(&target)?,
            solution.scaled_share_difficulty
        );
        assert_eq!(
            solution
                .window_weight
                .div_ceil(solution.scaled_share_difficulty),
            u128::from(requested)
        );
        // The configured difficulty must survive the string round trip the
        // frontend's environment puts it through.
        let rendered = format!("{}", solution.share_difficulty);
        assert_eq!(rendered.parse::<f64>()?, solution.share_difficulty);
        // A share must be easier than a block, or every share is a block.
        assert!(target > codec::target_from_compact(bits)?);
    }
    assert!(
        window::solve_window(bits, 0).is_err(),
        "a zero-share window is rejected at the boundary"
    );
    Ok(())
}

#[test]
fn the_documented_window_difficulties_land_where_the_contract_says() -> Result<()> {
    let bits = codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    let hundred_k = window::solve_window(bits, 100_000)?;
    let twenty_k = window::solve_window(bits, 20_000)?;
    assert!(
        (hundred_k.share_difficulty - 2.44e-9).abs() < 1e-11,
        "100k window difficulty was {}",
        hundred_k.share_difficulty
    );
    assert!(
        (twenty_k.share_difficulty - 1.22e-8).abs() < 1e-10,
        "20k window difficulty was {}",
        twenty_k.share_difficulty
    );
    assert!(
        (twenty_k.hashes_per_block - 131_072.0).abs() < 1.0,
        "a block should cost about 131k hashes, not {}",
        twenty_k.hashes_per_block
    );
    Ok(())
}

#[test]
fn the_seed_plan_fills_the_window_it_was_built_for() -> Result<()> {
    let bits = codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    let solution = window::solve_window(bits, 20_000)?;
    let plan = window::SeedPlan::new(
        20_000,
        solution.scaled_share_difficulty,
        solution.scaled_network_difficulty,
        window::DEFAULT_SEED_SHARE_BYTES,
    )?;
    assert_eq!(plan.window_length(), 20_000);
    assert_eq!(
        serde_json::to_vec(&plan.share(1))?.len(),
        window::DEFAULT_SEED_SHARE_BYTES,
        "seeded shares must be production sized"
    );
    assert_eq!(plan.share(7), plan.share(7), "shares must be deterministic");
    assert!(
        window::SeedPlan::share_id(3).starts_with(window::SEED_SHARE_ID_PREFIX),
        "seeded identifiers must never collide with a run's live prefix"
    );
    Ok(())
}

// --- fake node -----------------------------------------------------------

fn rpc(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "1.0", "id": 42, "method": method, "params": params})
}

fn block_hex(parent_display: &str, nonce: u32) -> String {
    let mut prev = hex::decode(parent_display).unwrap();
    prev.reverse();
    let header = client::assemble_header(
        0x2000_0000,
        &prev,
        &[7u8; 32],
        1_800_000_000,
        codec::parse_u32_hex(window::TEMPLATE_BITS).unwrap(),
        nonce,
    );
    format!("{}01{}", hex::encode(header), "00".repeat(64))
}

#[tokio::test]
async fn fake_node_answers_the_rpc_surface_the_frontends_call() -> Result<()> {
    let state = NodeState::new(window::TEMPLATE_BITS, "pload1");
    let info = state.handle(&rpc("getblockchaininfo", json!([]))).await;
    assert_eq!(info["id"], 42, "the id must be echoed exactly");
    assert!(info["error"].is_null());
    assert_eq!(info["result"]["chain"], "test");
    assert_eq!(info["result"]["initialblockdownload"], false);
    assert_eq!(info["result"]["blocks"], node::START_HEIGHT);
    assert_eq!(info["result"]["headers"], node::START_HEIGHT);
    let unknown = state.handle(&rpc("getblockcount", json!([]))).await;
    assert_eq!(unknown["error"]["code"], -32601);
    assert!(unknown.get("result").is_some(), "result is always present");
    let template = state.handle(&rpc("getblocktemplate", json!([{}]))).await;
    assert_eq!(template["result"]["bits"], window::TEMPLATE_BITS);
    assert_eq!(template["result"]["height"], node::START_HEIGHT + 1);
    assert_eq!(
        template["result"]["previousblockhash"],
        state.tip().0,
        "the template must build on the current tip"
    );
    let valid = state
        .handle(&rpc("validateaddress", json!(["pload1deadbeef"])))
        .await;
    assert_eq!(valid["result"]["isvalid"], true);
    let script = valid["result"]["scriptPubKey"].as_str().unwrap();
    assert!(
        script.starts_with("5220") && script.len() == 68,
        "P2MR only"
    );
    let invalid = state
        .handle(&rpc("validateaddress", json!(["somebody-else"])))
        .await;
    assert_eq!(invalid["result"]["isvalid"], false);
    Ok(())
}

#[tokio::test]
async fn fake_node_keeps_a_real_height_map_and_increasing_chainwork() -> Result<()> {
    let state = NodeState::new(window::TEMPLATE_BITS, "pload1");
    let genesis = state.handle(&rpc("getblockhash", json!([0]))).await;
    assert_eq!(genesis["result"], node::GENESIS);
    let (tip_before, height_before) = state.tip();
    let work_before = u128::from_str_radix(&state.chainwork_hex(), 16)?;
    // A height below the tip must answer with that height's own hash, not the
    // tip: returning the tip everywhere would later mark earlier pool blocks
    // inactive.
    let earlier = state.handle(&rpc("getblockhash", json!([50]))).await;
    assert_ne!(earlier["result"], Value::String(tip_before.clone()));
    let out_of_range = state
        .handle(&rpc("getblockhash", json!([height_before + 5])))
        .await;
    assert_eq!(out_of_range["error"]["code"], -8);

    let minted = state.mint_external_block();
    assert_eq!(minted.height, height_before + 1);
    let work_after = u128::from_str_radix(&state.chainwork_hex(), 16)?;
    assert!(
        work_after > work_before,
        "chainwork must increase strictly, or observe_chain_view refuses the tip"
    );
    let at_height = state
        .handle(&rpc("getblockhash", json!([height_before + 1])))
        .await;
    assert_eq!(at_height["result"], minted.hash);
    let still_earlier = state.handle(&rpc("getblockhash", json!([50]))).await;
    assert_eq!(
        still_earlier["result"], earlier["result"],
        "history must not move under a new tip"
    );
    let header = state
        .handle(&rpc("getblockheader", json!([minted.hash])))
        .await;
    assert_eq!(header["result"]["previousblockhash"], tip_before);
    Ok(())
}

#[tokio::test]
async fn fake_node_submitblock_checks_the_parent() -> Result<()> {
    let state = NodeState::new(window::TEMPLATE_BITS, "pload1");
    let (tip, height) = state.tip();
    let stale = state
        .handle(&rpc("submitblock", json!([block_hex(&"aa".repeat(32), 1)])))
        .await;
    assert_eq!(stale["result"], node::PARENT_MISMATCH);
    assert_eq!(
        state.tip(),
        (tip.clone(), height),
        "a rejection cannot move the tip"
    );

    let good = state
        .handle(&rpc("submitblock", json!([block_hex(&tip, 2)])))
        .await;
    assert!(good["result"].is_null(), "acceptance returns null");
    let (new_tip, new_height) = state.tip();
    assert_eq!(new_height, height + 1);
    assert_ne!(new_tip, tip);
    let submissions = state.submissions();
    assert_eq!(submissions.len(), 2);
    assert!(!submissions[0].accepted && submissions[0].rejection.is_some());
    assert!(submissions[1].accepted && submissions[1].rejection.is_none());
    assert_eq!(submissions[1].block_hash, new_tip);
    let best = state.handle(&rpc("getbestblockhash", json!([]))).await;
    assert_eq!(best["result"], new_tip);
    Ok(())
}

#[tokio::test]
async fn fake_node_waitfornewblock_wakes_on_a_tip_change() -> Result<()> {
    let state = Arc::new(NodeState::new(window::TEMPLATE_BITS, "pload1"));
    let waiter = {
        let state = state.clone();
        tokio::spawn(async move { state.handle(&rpc("waitfornewblock", json!([30_000]))).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let minted = state.mint_external_block();
    let answer = tokio::time::timeout(std::time::Duration::from_secs(5), waiter).await??;
    assert_eq!(answer["result"]["hash"], minted.hash);
    assert_eq!(answer["result"]["height"], minted.height);
    Ok(())
}

#[tokio::test]
async fn fake_node_waitfornewblock_returns_on_timeout_without_a_change() -> Result<()> {
    let state = NodeState::new(window::TEMPLATE_BITS, "pload1");
    let (tip, height) = state.tip();
    let answer = state.handle(&rpc("waitfornewblock", json!([50]))).await;
    assert_eq!(answer["result"]["hash"], tip);
    assert_eq!(answer["result"]["height"], height);
    Ok(())
}

// --- frontend environment -------------------------------------------------

fn sample_environment() -> BTreeMap<String, String> {
    let shared = SharedEnvironment {
        rpc_url: "http://127.0.0.1:1/".into(),
        rpc_user: "qbit".into(),
        rpc_password: "secret".into(),
        share_difficulty: "0.0000000122070312".into(),
        max_difficulty: "1024".into(),
        database_max_connections: 16,
        runtime_workers: 2,
        stratum_max_connections: 384,
        stratum_max_pending_initial_jobs: 128,
        share_commit_timeout_seconds: "15".into(),
        blockpoll_seconds: "2".into(),
        rust_log: "info".into(),
    };
    let spec = FrontendSpec {
        index: 0,
        instance_id: "load-fe-0".into(),
        stratum_port: 3340,
        audit_port: 3341,
        database_url: "postgresql://u@127.0.0.1:5432/postgres".into(),
    };
    frontend::frontend_environment(&shared, &spec)
}

#[test]
fn the_frontend_environment_carries_every_configuration_key() -> Result<()> {
    let env = sample_environment();
    let block = frontend::configuration_block(&env)?;
    assert_eq!(block.len(), CONFIGURATION_KEYS.len());
    for key in CONFIGURATION_KEYS {
        let value = block
            .get(*key)
            .unwrap_or_else(|| panic!("{key} is missing"));
        assert!(!value.trim().is_empty(), "{key} must never be empty");
    }
    // `config::value` takes a set-but-empty variable literally, so no key the
    // runtime reads that way may be exported empty.
    for (key, value) in &env {
        assert!(!value.is_empty(), "{key} was exported as an empty string");
    }
    assert_eq!(env["PRISM_STRATUM_VARDIFF"], "0");
    assert_eq!(
        env["PRISM_STRATUM_SHARE_DIFF"], env["PRISM_STRATUM_VARDIFF_MIN_DIFF"],
        "the floor must equal the share difficulty so no clamp can move the target"
    );
    assert_eq!(
        env["PRISM_STRATUM_SHARE_DIFF"],
        env["PRISM_STRATUM_VARDIFF_START_DIFF"]
    );
    assert_eq!(env["PRISM_ALLOW_TEST_SIGNING_SEEDS"], "1");
    assert_eq!(env["QBIT_PRODUCTION"], "0");
    assert_eq!(env["PRISM_POSTGRES_INIT_SCHEMA"], "1");
    assert_eq!(env["QBIT_CHAIN"], "testnet");
    assert_eq!(env["PRISM_MIN_PEERS"], "1");
    assert_eq!(env["RUST_LOG"], "info");
    assert_ne!(env["PRISM_AUDIT_PORT"], "0", "port 0 disables /metrics");
    for key in frontend::UNREAD_CONFIGURATION_KEYS {
        assert!(block.contains_key(*key), "{key} still has to be recorded");
    }
    assert_eq!(env["PRISM_SHARE_COMMIT_BATCH_SIZE"], "1");
    assert_eq!(env["PRISM_SHARE_COMMIT_LINGER_MILLISECONDS"], "0");
    assert_eq!(env["PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS"], "0");
    Ok(())
}

#[test]
fn a_missing_or_empty_configuration_key_is_refused() {
    let mut env = sample_environment();
    env.remove("PRISM_STRATUM_SHARE_DIFF");
    assert!(frontend::configuration_block(&env).is_err());
    let mut env = sample_environment();
    env.insert("PRISM_STRATUM_SEND_TIMEOUT_SECONDS".into(), "  ".into());
    assert!(frontend::configuration_block(&env).is_err());
}

#[test]
fn secrets_never_reach_a_report() {
    let env = sample_environment();
    let redacted = frontend::redacted(&env);
    assert_eq!(redacted["QBIT_RPC_PASSWORD"], "<redacted>");
    assert_eq!(redacted["QBIT_RPC_URL"], env["QBIT_RPC_URL"]);
}

/// `--database-url` in the usual `postgresql://user:password@host/db` form is
/// carried as `PRISM_DATABASE_URL`, and both `database-profile.json` and the
/// side report print the "redacted" environment, so the password has to go
/// -- from that variable and from any other URL-valued one.
#[test]
fn a_password_inside_any_url_valued_variable_never_reaches_a_report() {
    let password = "hunter2-Sup3r_Secret";
    let mut env = sample_environment();
    env.insert(
        "PRISM_DATABASE_URL".into(),
        format!("postgresql://alex:{password}@db.example:5432/qbit?sslmode=disable&application_name=load-fe-0"),
    );
    env.insert(
        "SOME_FUTURE_URL".into(),
        format!("https://svc:{password}@api.example/v1#frag"),
    );
    env.insert(
        "PRISM_DATABASE_URL_PARAM_FORM".into(),
        format!("postgresql://db.example/qbit?password={password}&sslmode=require"),
    );
    let redacted = frontend::redacted(&env);
    for (key, value) in &redacted {
        assert!(
            !value.contains(password),
            "{key} still carries the password: {value}"
        );
    }
    assert_eq!(
        redacted["PRISM_DATABASE_URL"],
        "postgresql://alex:<redacted>@db.example:5432/qbit?sslmode=disable&application_name=load-fe-0",
        "the user, host, port, database and non-secret parameters stay legible"
    );
    assert_eq!(
        redacted["SOME_FUTURE_URL"],
        "https://svc:<redacted>@api.example/v1#frag"
    );
    assert_eq!(
        redacted["PRISM_DATABASE_URL_PARAM_FORM"],
        "postgresql://db.example/qbit?password=<redacted>&sslmode=require"
    );
    // A URL without a password, and a value that is not a URL, are untouched.
    assert_eq!(
        frontend::redact_url_secrets("postgresql://u@127.0.0.1:5432/postgres"),
        "postgresql://u@127.0.0.1:5432/postgres"
    );
    assert_eq!(frontend::redact_url_secrets("info"), "info");
    assert_eq!(
        frontend::redact_url_secrets("0.0000000122070312"),
        "0.0000000122070312"
    );
    assert_eq!(
        redacted["QBIT_RPC_URL"], env["QBIT_RPC_URL"],
        "a URL with no userinfo is unchanged"
    );
}

/// A scratch directory under the system temp dir, removed on drop.
struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "prism-load-{tag}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A stand-in for `qbit-prism-server run`: a shell script that logs one line
/// to stderr and stays up until it is killed. It lets the process lifecycle
/// be exercised without a database or a real server.
fn stand_in_server(dir: &std::path::Path, stderr_line: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("stand-in-server");
    std::fs::write(
        &path,
        format!("#!/bin/sh\necho '{stderr_line}' >&2\nexec sleep 60\n"),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn wait_for_log(path: &std::path::Path, needle: &str, count: usize) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        if text.matches(needle).count() >= count {
            return text;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{} never showed {count} x {needle:?}; it holds {text:?}",
            path.display()
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Reusing an `--out` directory must not carry an earlier invocation's stderr
/// into this run's blocked-log classification: the first launch truncates.
/// A restart inside the invocation appends, because what the killed process
/// logged is this run's evidence.
#[tokio::test]
async fn a_stale_frontend_log_is_truncated_on_launch_and_kept_across_a_restart() -> Result<()> {
    let dir = ScratchDir::new("stale-log");
    let server = stand_in_server(dir.path(), "PRISM listening (stand-in)");
    let log_dir = dir.path().join("logs");
    std::fs::create_dir_all(&log_dir)?;
    let stale = "JSONB container ceiling reached in an earlier run\n";
    std::fs::write(log_dir.join("load-fe-7.stderr.log"), stale)?;
    std::fs::write(log_dir.join("load-fe-7.stdout.log"), "stale stdout\n")?;
    let spec = FrontendSpec {
        index: 7,
        instance_id: "load-fe-7".into(),
        stratum_port: 1,
        audit_port: 1,
        database_url: "postgresql://u@127.0.0.1:1/x".into(),
    };
    let mut child = frontend::Frontend::launch(server, spec, BTreeMap::new(), &log_dir)?;
    let text = wait_for_log(&child.stderr_path, "stand-in", 1);
    assert!(
        !text.contains("earlier run"),
        "the first launch must truncate the stale log, but it holds {text:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&child.stdout_path)?,
        "",
        "stdout is truncated too"
    );
    child.restart()?;
    let text = wait_for_log(&child.stderr_path, "stand-in", 2);
    assert_eq!(child.restarts, 1);
    assert!(
        !text.contains("earlier run"),
        "a restart appends to this invocation's log only"
    );
    child.kill();
    Ok(())
}

/// The build profile is read from the Cargo directory the binary sits in,
/// through symlinks. A binary at any other path has an unknown profile, and
/// an unknown profile needs the same override a debug build does: it cannot
/// be shown to be a release build, so letting it through silently would let a
/// debug build at a copied path produce a qualification artifact.
#[test]
fn an_unknown_build_profile_needs_the_debug_override() -> Result<()> {
    use qbit_prism_load::frontend::{build_profile, check_server_profile, BuildProfile};
    let dir = ScratchDir::new("profile");
    let release = dir.path().join("target").join("release");
    let debug = dir.path().join("target").join("debug");
    std::fs::create_dir_all(&release)?;
    std::fs::create_dir_all(&debug)?;
    let built = release.join("qbit-prism-server");
    std::fs::write(&built, b"binary")?;
    std::fs::write(debug.join("qbit-prism-server"), b"binary")?;
    let copied = dir.path().join("qbit-prism-server");
    std::fs::copy(&built, &copied)?;
    let linked = dir.path().join("server-link");
    std::os::unix::fs::symlink(&built, &linked)?;

    assert_eq!(build_profile(&built), BuildProfile::Release);
    assert_eq!(
        build_profile(&debug.join("qbit-prism-server")),
        BuildProfile::Debug
    );
    assert_eq!(
        build_profile(&linked),
        BuildProfile::Release,
        "a symlink into target/release is shown to be a release build"
    );
    assert_eq!(build_profile(&copied), BuildProfile::Unknown);

    check_server_profile(BuildProfile::Release, false, &built)?;
    check_server_profile(BuildProfile::Debug, true, &built)?;
    check_server_profile(BuildProfile::Unknown, true, &copied)?;
    let refused = format!(
        "{:#}",
        check_server_profile(BuildProfile::Debug, false, &built)
            .expect_err("a debug build needs the override")
    );
    assert!(refused.contains("--allow-debug-server"), "{refused}");
    let refused = format!(
        "{:#}",
        check_server_profile(BuildProfile::Unknown, false, &copied)
            .expect_err("an unknown profile needs the override too")
    );
    assert!(refused.contains("--allow-debug-server"), "{refused}");
    assert!(
        refused.contains("cannot be determined"),
        "the refusal says why: {refused}"
    );
    Ok(())
}

// --- a minimal Stratum server -------------------------------------------

/// The smallest Stratum server a session can complete a handshake with:
/// subscribe, configure, authorize, one job, and `true` to every submit.
/// `release_authorize` gates the authorize reply, which keeps a session
/// inside its handshake for as long as a test needs.
struct FakeStratum {
    address: String,
    release_authorize: tokio::sync::watch::Sender<bool>,
    submits: Arc<std::sync::atomic::AtomicUsize>,
    _task: tokio::task::JoinHandle<()>,
}

async fn fake_stratum(hold_authorize: bool) -> FakeStratum {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (release_authorize, release) = tokio::sync::watch::channel(!hold_authorize);
    let submits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = submits.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(serve_stratum(socket, release.clone(), counter.clone()));
        }
    });
    FakeStratum {
        address,
        release_authorize,
        submits,
        _task: task,
    }
}

async fn serve_stratum(
    socket: tokio::net::TcpStream,
    mut release: tokio::sync::watch::Receiver<bool>,
    submits: Arc<std::sync::atomic::AtomicUsize>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let (read, mut write) = socket.into_split();
    let mut lines = tokio::io::BufReader::new(read).lines();
    let (coinb1, coinb2) = coinbase_halves(8);
    // Network target diff 1 (one hash in 2^32 solves a block), so a share
    // search at difficulty 2^-26 finds a share in a few dozen hashes and
    // rarely discards one as an unscheduled block solution.
    let notify = json!({"id": null, "method": "mining.notify", "params": [
        "job-1",
        "00000000000000000001aabbccddeeff00112233445566778899aabbccddeeff",
        hex::encode(&coinb1), hex::encode(&coinb2), [],
        "20000000", "1d00ffff", "6b49d200", true
    ]});
    while let Ok(Some(line)) = lines.next_line().await {
        let request: Value = serde_json::from_str(&line).unwrap();
        let id = request["id"].clone();
        let reply = match request["method"].as_str() {
            Some("mining.subscribe") => json!({"id": id, "error": null, "result": [
                [["mining.set_difficulty", "1"], ["mining.notify", "1"]], "deadbeef", 8
            ]}),
            Some("mining.configure") => {
                json!({"id": id, "error": null, "result": {"version-rolling": false}})
            }
            Some("mining.authorize") => {
                while !*release.borrow() {
                    if release.changed().await.is_err() {
                        return;
                    }
                }
                json!({"id": id, "error": null, "result": true})
            }
            Some("mining.submit") => {
                submits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                json!({"id": id, "error": null, "result": true})
            }
            _ => continue,
        };
        let authorized = request["method"] == "mining.authorize";
        let mut bytes = serde_json::to_vec(&reply).unwrap();
        bytes.push(b'\n');
        if write.write_all(&bytes).await.is_err() {
            return;
        }
        if authorized {
            let mut bytes = serde_json::to_vec(&notify).unwrap();
            bytes.push(b'\n');
            if write.write_all(&bytes).await.is_err() {
                return;
            }
        }
    }
}

fn session_config(index: usize) -> client::SessionConfig {
    client::SessionConfig {
        index,
        username: format!("pload1test.s{index:05}"),
        password: "x".into(),
        // Diff 1 is 2^32 hashes per share; 2^-26 is about 64, so a share
        // search costs microseconds even in a debug build.
        share_difficulty: 1.0 / 67_108_864.0,
        version_rolling_mask: codec::VERSION_ROLLING_MASK,
        connect_timeout: std::time::Duration::from_secs(5),
        handshake_timeout: std::time::Duration::from_secs(20),
    }
}

/// A phase's rate and its artifact must describe the same offers. An offer
/// the scheduler places while the session cannot send it -- here, because
/// the session is still inside its handshake -- is counted as dispatched in
/// that phase, and has to be recorded under that phase even when the run has
/// moved on by the time the session gets to send it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queued_offer_is_recorded_under_the_phase_that_offered_it() -> Result<()> {
    let server = fake_stratum(true).await;
    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let shared = Arc::new(client::SessionShared {
        phase: std::sync::RwLock::new("steady_state".to_owned()),
        events,
        record_notifies: std::sync::atomic::AtomicBool::new(false),
    });
    let handle = client::spawn_session(
        session_config(0),
        0,
        server.address.clone(),
        shared.clone(),
        2,
    );
    let phase: Arc<str> = Arc::from("steady_state");
    assert!(handle.try_offer(2, &phase));
    assert!(handle.try_offer(2, &phase));
    assert!(
        !handle.try_offer(2, &phase),
        "the outstanding limit bounds queued and in-flight offers together"
    );
    assert_eq!(
        handle.outstanding.load(std::sync::atomic::Ordering::SeqCst),
        2
    );

    // The phase ends with both offers still queued; the next one begins.
    *shared.phase.write().unwrap() = "reconnect".to_owned();
    server.release_authorize.send(true)?;

    let mut records = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while records.len() < 2 {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the two submits are answered within the deadline")
            .expect("the session is still running");
        if let client::Event::Submit(record) = event {
            records.push(*record);
        }
    }
    assert_eq!(server.submits.load(std::sync::atomic::Ordering::SeqCst), 2);
    for record in &records {
        assert!(matches!(record.outcome, client::Outcome::Accepted));
        assert_eq!(
            record.phase, "steady_state",
            "the offer was dispatched in steady_state and belongs there, not in the phase \
             the run had reached when the session finally sent it"
        );
    }
    // The session reports a submit a few instructions before it releases the
    // slot, so give the counter a moment to settle.
    let settled = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while handle.outstanding.load(std::sync::atomic::Ordering::SeqCst) != 0
        && tokio::time::Instant::now() < settled
    {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        handle.outstanding.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "every slot is released exactly once"
    );
    let _ = handle.control.send(client::Control::Stop);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.task).await??;
    Ok(())
}

// --- server revision evidence --------------------------------------------

/// `coordinator_revision` is the checkout's HEAD; the binary has to be shown
/// to be what that checkout builds, or the artifact attributes measurements
/// to a commit that did not produce them. The evidence is Cargo's own: the
/// dep-info file beside the binary, every source it lists no newer than the
/// binary, and the server crate's root among those sources.
#[test]
fn the_server_revision_is_established_from_cargo_dep_info_or_not_at_all() -> Result<()> {
    use qbit_prism_load::provenance::{
        dep_info_path, parse_dep_info, server_revision_evidence, RevisionEvidence,
        SERVER_CRATE_ROOT, SERVER_MANIFEST,
    };
    use std::time::{Duration, SystemTime};

    let dir = ScratchDir::new("provenance");
    let root = dir.path().join("checkout");
    let sources = [
        root.join(SERVER_CRATE_ROOT),
        root.join("crates/qbit-prism-server/src/coordinator.rs"),
        root.join("crates/qbit-prism/src/lib.rs"),
    ];
    for source in &sources {
        std::fs::create_dir_all(source.parent().unwrap())?;
        std::fs::write(source, b"fn main() {}")?;
    }
    std::fs::write(root.join("Cargo.lock"), b"# lock")?;
    std::fs::write(root.join("Cargo.toml"), b"[workspace]")?;
    let manifests = [
        root.join(SERVER_MANIFEST),
        root.join("crates/qbit-prism/Cargo.toml"),
    ];
    for manifest in &manifests {
        std::fs::write(manifest, b"[package]")?;
    }
    let release = root.join("target/release");
    std::fs::create_dir_all(&release)?;
    let binary = release.join("qbit-prism-server");
    std::fs::write(&binary, b"ELF")?;
    let listed = sources
        .iter()
        .map(|source| source.display().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    std::fs::write(
        dep_info_path(&binary),
        format!("{}: {listed}\n", binary.display()),
    )?;
    let set_modified = |path: &std::path::Path, at: SystemTime| -> Result<()> {
        std::fs::File::options()
            .write(true)
            .open(path)?
            .set_modified(at)?;
        Ok(())
    };
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
    for source in &sources {
        set_modified(source, base)?;
    }
    set_modified(&root.join("Cargo.lock"), base)?;
    set_modified(&root.join("Cargo.toml"), base)?;
    for manifest in &manifests {
        set_modified(manifest, base)?;
    }
    set_modified(&binary, base + Duration::from_secs(60))?;

    // Every source is older than the binary and the server crate root is
    // listed: this binary is what the tree builds.
    match server_revision_evidence(&binary, &root) {
        RevisionEvidence::Established {
            sources_checked, ..
        } => assert_eq!(
            sources_checked,
            sources.len() + 2 + manifests.len(),
            "sources plus lock, workspace manifest and the two crate manifests"
        ),
        RevisionEvidence::Unestablished { reason } => panic!("should be established: {reason}"),
    }

    // A checkout that touched one server source after the build: the binary
    // is older than what the tree now says, and the revision would be wrong.
    set_modified(&sources[1], base + Duration::from_secs(120))?;
    let RevisionEvidence::Unestablished { reason } = server_revision_evidence(&binary, &root)
    else {
        panic!("a newer source must leave the revision unestablished");
    };
    assert!(reason.contains("coordinator.rs"), "{reason}");
    assert!(reason.contains("newer than the binary"), "{reason}");
    set_modified(&sources[1], base)?;

    // A dependency bump changes what the tree builds without touching any
    // listed source; the lock file is checked explicitly.
    set_modified(&root.join("Cargo.lock"), base + Duration::from_secs(120))?;
    let RevisionEvidence::Unestablished { reason } = server_revision_evidence(&binary, &root)
    else {
        panic!("a newer Cargo.lock must leave the revision unestablished");
    };
    assert!(reason.contains("Cargo.lock"), "{reason}");
    set_modified(&root.join("Cargo.lock"), base)?;

    // A binary built in another checkout lists that checkout's sources.
    let elsewhere = dir.path().join("elsewhere");
    let foreign_root = elsewhere.join(SERVER_CRATE_ROOT);
    std::fs::create_dir_all(foreign_root.parent().unwrap())?;
    std::fs::write(&foreign_root, b"fn main() {}")?;
    set_modified(&foreign_root, base)?;
    std::fs::write(
        dep_info_path(&binary),
        format!("{}: {}\n", binary.display(), foreign_root.display()),
    )?;
    let RevisionEvidence::Unestablished { reason } = server_revision_evidence(&binary, &root)
    else {
        panic!("a binary from another checkout must leave the revision unestablished");
    };
    assert!(reason.contains("different checkout"), "{reason}");

    // A copied or installed binary has no dep-info at all.
    let copied = dir.path().join("qbit-prism-server");
    std::fs::copy(&binary, &copied)?;
    let RevisionEvidence::Unestablished { reason } = server_revision_evidence(&copied, &root)
    else {
        panic!("a binary without dep-info must leave the revision unestablished");
    };
    assert!(reason.contains("no Cargo dep-info"), "{reason}");

    // The parser handles Cargo's escaped spaces and a rule with no sources.
    let parsed = parse_dep_info("/t/bin: /a/b.rs /c\\ d/e.rs\n")?;
    assert_eq!(
        parsed,
        vec![
            std::path::PathBuf::from("/a/b.rs"),
            std::path::PathBuf::from("/c d/e.rs")
        ]
    );
    assert!(parse_dep_info("/t/bin:\n")?.is_empty());
    assert!(parse_dep_info("\n").is_err());
    Ok(())
}

/// A package manifest can change what the tree builds without touching a
/// source or the lock file: enabling a feature of a dependency already in the
/// lock. Cargo's dep-info lists no manifests, so with only the sources, the
/// lock and the workspace manifest checked, a binary built before such a
/// change reads as Established and the artifact names a HEAD that did not
/// build it. The server's manifest and those of its path dependencies -- the
/// crates whose sources Cargo listed -- are inputs too.
#[test]
fn a_package_manifest_newer_than_the_binary_leaves_the_revision_unestablished() -> Result<()> {
    use qbit_prism_load::provenance::{
        crate_manifests, dep_info_path, server_revision_evidence, RevisionEvidence,
        SERVER_CRATE_ROOT, SERVER_MANIFEST,
    };
    use std::time::{Duration, SystemTime};

    let dir = ScratchDir::new("provenance-manifests");
    let root = dir.path().join("checkout");
    let sources = [
        root.join(SERVER_CRATE_ROOT),
        root.join("crates/qbit-prism/src/lib.rs"),
        root.join("crates/qbit-pool-builder/src/lib.rs"),
        root.join("crates/qbit-pool-builder/src/nested/deep.rs"),
    ];
    let server_manifest = root.join(SERVER_MANIFEST);
    let prism_manifest = root.join("crates/qbit-prism/Cargo.toml");
    let builder_manifest = root.join("crates/qbit-pool-builder/Cargo.toml");
    let files = [
        root.join("Cargo.lock"),
        root.join("Cargo.toml"),
        server_manifest.clone(),
        prism_manifest.clone(),
        builder_manifest.clone(),
    ];
    for path in sources.iter().chain(&files) {
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(path, b"#")?;
    }
    let release = root.join("target/release");
    std::fs::create_dir_all(&release)?;
    let binary = release.join("qbit-prism-server");
    std::fs::write(&binary, b"ELF")?;
    let listed = sources
        .iter()
        .map(|source| source.display().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    std::fs::write(
        dep_info_path(&binary),
        format!("{}: {listed}\n", binary.display()),
    )?;
    let set_modified = |path: &std::path::Path, at: SystemTime| -> Result<()> {
        std::fs::File::options()
            .write(true)
            .open(path)?
            .set_modified(at)?;
        Ok(())
    };
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
    for path in sources.iter().chain(&files) {
        set_modified(path, base)?;
    }
    set_modified(&binary, base + Duration::from_secs(60))?;

    // The path-dependency manifests are derived from the listed sources, one
    // per crate however many of its sources are listed, and never the
    // workspace manifest itself, which is an input in its own right.
    let derived = crate_manifests(
        &sources
            .iter()
            .map(|source| std::fs::canonicalize(source).unwrap())
            .collect::<Vec<_>>(),
        &std::fs::canonicalize(&root)?,
    );
    let expected: std::collections::BTreeSet<_> =
        [&server_manifest, &prism_manifest, &builder_manifest]
            .into_iter()
            .map(|manifest| std::fs::canonicalize(manifest).unwrap())
            .collect();
    assert_eq!(derived, expected);

    // Everything is older than the binary: established, with every manifest
    // among the inputs.
    match server_revision_evidence(&binary, &root) {
        RevisionEvidence::Established {
            sources_checked, ..
        } => assert_eq!(sources_checked, sources.len() + files.len()),
        RevisionEvidence::Unestablished { reason } => panic!("should be established: {reason}"),
    }

    // The server's own manifest changed after the build -- a dependency
    // feature enabled, no source and no lock entry touched. The binary is not
    // what the tree builds now.
    set_modified(&server_manifest, base + Duration::from_secs(120))?;
    let RevisionEvidence::Unestablished { reason } = server_revision_evidence(&binary, &root)
    else {
        panic!("a newer server manifest must leave the revision unestablished");
    };
    assert!(reason.contains("qbit-prism-server/Cargo.toml"), "{reason}");
    assert!(reason.contains("newer than the binary"), "{reason}");
    set_modified(&server_manifest, base)?;

    // The same for a path dependency's manifest.
    set_modified(&builder_manifest, base + Duration::from_secs(120))?;
    let RevisionEvidence::Unestablished { reason } = server_revision_evidence(&binary, &root)
    else {
        panic!("a newer path-dependency manifest must leave the revision unestablished");
    };
    assert!(reason.contains("qbit-pool-builder/Cargo.toml"), "{reason}");
    set_modified(&builder_manifest, base)?;

    // A checkout without the server manifest is not one the binary can be
    // tied to, whatever the dep-info lists.
    std::fs::remove_file(&server_manifest)?;
    let RevisionEvidence::Unestablished { reason } = server_revision_evidence(&binary, &root)
    else {
        panic!("a missing server manifest must leave the revision unestablished");
    };
    assert!(reason.contains("qbit-prism-server/Cargo.toml"), "{reason}");
    Ok(())
}

// --- artifact -------------------------------------------------------------

fn sample_inputs() -> ArtifactInputs {
    let configuration = frontend::configuration_block(&sample_environment()).unwrap();
    let subject: BTreeMap<String, String> = SUBJECT_KEYS
        .iter()
        .zip([
            "a".repeat(40),
            format!("sha256:{}", "b".repeat(64)),
            "16.10".to_owned(),
            "c".repeat(64),
        ])
        .map(|(key, value)| ((*key).to_owned(), value))
        .collect();
    let durability = ["fsync", "full_page_writes", "synchronous_commit"]
        .into_iter()
        .map(|key| (key.to_owned(), "on".to_owned()))
        .collect();
    let phases = REQUIRED_PHASES
        .iter()
        .map(|name| PhaseEvidence {
            name: (*name).to_owned(),
            duration_millis: 60_500,
            offered: 3_000,
            acknowledged: 3_000,
            committed: 3_000,
            rejected_valid: 0,
            missing: 0,
            unexpected: 0,
            acknowledged_digest: digest::share_id_digest([*name]),
            committed_digest: digest::share_id_digest([*name]),
            ack_p50_millis: 4.5,
            ack_p99_millis: 42.25,
            reconnect_events: (*name == "reconnect").then_some(12),
            database_delay_millis: (*name == "slow_database").then_some(10.0),
        })
        .collect();
    ArtifactInputs {
        artifact_kind: artifact::ARTIFACT_QUALIFICATION.to_owned(),
        run_id: uuid::Uuid::new_v4(),
        generated_at: chrono::Utc::now(),
        subject,
        durability,
        configuration,
        forecast_peak_shares_per_second: "20".to_owned(),
        ack_p99_limit_milliseconds: "1000".to_owned(),
        overall_ack_p50_millis: 4.5,
        overall_ack_p99_millis: 42.25,
        overall_acknowledged_digest: digest::share_id_digest(["overall"]),
        overall_committed_digest: digest::share_id_digest(["overall"]),
        phases,
    }
}

#[test]
fn the_artifact_builder_produces_evidence_the_validator_accepts() -> Result<()> {
    let inputs = sample_inputs();
    let document = artifact::build(&inputs)?;
    let options = artifact::validation_options(&inputs);
    validate_capacity_evidence(&document, &options)
        .map_err(|error| anyhow::anyhow!("{error:#}"))?;
    // The phase durations must sum to test_duration_seconds exactly.
    assert_eq!(document["test_duration_seconds"], "181.500");
    assert_eq!(document["schema"], qbit_prism_server::capacity::SCHEMA);
    assert_eq!(document["test_path"], artifact::TEST_PATH);
    assert!(document["generated_at"]
        .as_str()
        .is_some_and(|value| value.ends_with('Z') && value.as_bytes()[10] == b'T'));
    let verdict = artifact::verdict(&document, &options);
    assert!(verdict.valid, "{:?}", verdict.error_chain);
    Ok(())
}

/// An aborted run must not leave a qualification artifact behind. Crossing
/// the memory floor or losing a frontend only broke out of the phase loop,
/// and execution continued into artifact generation: an abort before the
/// three required phases made `artifact::build` error out before the side
/// report was written, and an abort during a long `slow_database` phase
/// emitted the partial phase as `completed: true`. The artifact step now
/// withholds the file on abort and removes a stale one at the same path.
#[test]
fn an_aborted_run_withholds_the_artifact_and_removes_a_stale_one() -> Result<()> {
    use qbit_prism_load::artifact::{write_or_withhold, Evidence};
    let dir = ScratchDir::new("withhold");
    let path = dir.path().join("capacity-evidence.json");
    std::fs::write(
        &path,
        b"{\"schema\": \"stale artifact from an earlier run\"}",
    )?;
    let inputs = sample_inputs();

    let withheld = write_or_withhold(
        &inputs,
        Some("MemAvailable fell to 512 MiB, below the 4096 MiB floor"),
        dir.path(),
        "qbit-prism-server",
    )?;
    let Evidence::Withheld {
        reason,
        stale_artifact_removed,
    } = withheld
    else {
        panic!("an aborted run must not write an artifact");
    };
    assert!(reason.contains("aborted"), "{reason}");
    assert!(reason.contains("MemAvailable"), "{reason}");
    assert!(
        stale_artifact_removed,
        "the earlier run's artifact is removed"
    );
    assert!(!path.exists(), "nothing self-validating survives the abort");

    // Aborting with nothing to remove says so, rather than claiming a removal.
    let Evidence::Withheld {
        stale_artifact_removed,
        ..
    } = write_or_withhold(&inputs, Some("load-fe-1 exited"), dir.path(), "srv")?
    else {
        panic!("still withheld");
    };
    assert!(!stale_artifact_removed);

    // Even inputs that would build a valid artifact are withheld on abort:
    // the same inputs, not aborted, are written and validate.
    let Evidence::Written {
        path: written,
        verdict,
        command,
        document,
    } = write_or_withhold(&inputs, None, dir.path(), "qbit-prism-server")?
    else {
        panic!("a completed run writes its artifact");
    };
    assert_eq!(written, path);
    assert!(path.exists());
    assert!(verdict.valid, "{:?}", verdict.error_chain);
    assert!(command.contains("capacity-evidence"));
    assert_eq!(document["artifact_kind"], "qualification");
    Ok(())
}

#[test]
fn removing_one_required_field_or_one_phase_invalidates_the_artifact() -> Result<()> {
    let inputs = sample_inputs();
    let options = artifact::validation_options(&inputs);
    for field in [
        "durability",
        "subject",
        "configuration",
        "run_id",
        "acknowledged_share_ids_sha256",
        "ack_p99_limit_milliseconds",
        "test_duration_seconds",
    ] {
        let mut document = artifact::build(&inputs)?;
        document
            .as_object_mut()
            .unwrap()
            .remove(field)
            .unwrap_or_else(|| panic!("{field} was not present"));
        let verdict = artifact::verdict(&document, &options);
        assert!(
            !verdict.valid,
            "removing {field} must invalidate the artifact"
        );
        assert!(
            verdict.error_chain.iter().any(|line| line.contains(field)),
            "the refusal must name {field}, got {:?}",
            verdict.error_chain
        );
    }
    for phase in REQUIRED_PHASES {
        let mut document = artifact::build(&inputs)?;
        document["phases"]
            .as_object_mut()
            .unwrap()
            .remove(*phase)
            .unwrap_or_else(|| panic!("{phase} was not present"));
        let verdict = artifact::verdict(&document, &options);
        assert!(!verdict.valid, "removing phase {phase} must invalidate");
        assert!(
            verdict.error_chain.iter().any(|line| line.contains(phase)),
            "the refusal must name {phase}, got {:?}",
            verdict.error_chain
        );
    }
    // A phase key inside a phase matters too.
    let mut document = artifact::build(&inputs)?;
    document["phases"]["reconnect"]
        .as_object_mut()
        .unwrap()
        .remove("reconnect_events");
    assert!(!artifact::verdict(&document, &options).valid);
    Ok(())
}

#[test]
fn the_validator_refuses_evidence_whose_expectations_do_not_match() -> Result<()> {
    let inputs = sample_inputs();
    let document = artifact::build(&inputs)?;
    let mut options = artifact::validation_options(&inputs);
    options.expected_forecast_peak_shares_per_second = Some("21".into());
    assert!(!artifact::verdict(&document, &options).valid);
    let mut options = artifact::validation_options(&inputs);
    let mut subject = inputs.subject.clone();
    subject.insert("postgres_server_version".into(), "15.1".into());
    options.expected_subject = Some(subject);
    assert!(!artifact::verdict(&document, &options).valid);
    Ok(())
}

#[test]
fn the_printed_cli_command_names_every_binding_the_self_check_used() {
    let inputs = sample_inputs();
    let command =
        artifact::cli_command(&inputs, "/tmp/capacity-evidence.json", "qbit-prism-server");
    for key in CONFIGURATION_KEYS {
        assert!(
            command.contains(&format!("--expect {key}=")),
            "{key} missing"
        );
    }
    for flag in [
        "--expect-coordinator-revision",
        "--expect-coordinator-image-digest",
        "--expect-postgres-server-version",
        "--expect-database-profile-sha256",
        "--forecast-peak-shares-per-second",
        "--ack-p99-limit-milliseconds",
    ] {
        assert!(command.contains(flag), "{flag} missing");
    }
    assert!(!command.contains("--allow-example-evidence-for-tests"));
}

#[test]
fn the_printed_cli_command_survives_a_shell() -> Result<()> {
    // `SHOW server_version` returns values like `16.15 (Ubuntu 16.15-…)`; an
    // unquoted one turns the printed command into a syntax error.
    let mut inputs = sample_inputs();
    inputs.subject.insert(
        "postgres_server_version".into(),
        "16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)".into(),
    );
    let command = artifact::cli_command(
        &inputs,
        "/tmp/out dir/capacity-evidence.json",
        "./qbit-prism-server",
    );
    assert!(
        command.contains("'16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)'"),
        "the version must be quoted: {command}"
    );
    assert!(command.contains("'/tmp/out dir/capacity-evidence.json'"));
    // A shell has to be able to parse it.
    let parsed = std::process::Command::new("bash")
        .arg("-n")
        .arg("-c")
        .arg(&command)
        .output()?;
    assert!(
        parsed.status.success(),
        "bash refused the printed command: {}\n{command}",
        String::from_utf8_lossy(&parsed.stderr)
    );
    assert_eq!(artifact::shell_quote("plain-value_1.2"), "plain-value_1.2");
    assert_eq!(artifact::shell_quote("it's"), "'it'\\''s'");
    Ok(())
}

// --- rejection and blocked-log classifiers --------------------------------

fn rejection(code: i64, reason: Option<&str>, message: &str) -> Rejection {
    Rejection {
        code,
        reason_id: reason.map(str::to_owned),
        message: message.to_owned(),
    }
}

#[test]
fn the_rejection_classifier_separates_harness_bugs_from_expected_races() {
    let cases = [
        (
            rejection(23, Some("low-difficulty"), "low difficulty share"),
            RejectionClass::HarnessBug,
        ),
        (
            rejection(22, Some("duplicate-share"), "duplicate share"),
            RejectionClass::HarnessBug,
        ),
        (
            rejection(20, Some("malformed-submit"), "malformed submit: bad"),
            RejectionClass::HarnessBug,
        ),
        (
            rejection(
                20,
                Some("invalid-extranonce"),
                "unexpected extranonce2 size",
            ),
            RejectionClass::HarnessBug,
        ),
        (
            rejection(
                20,
                Some("invalid-ntime-or-nonce"),
                "ntime and nonce must be 4-byte hex strings",
            ),
            RejectionClass::HarnessBug,
        ),
        (
            rejection(
                20,
                Some("unauthorized-worker"),
                "submit username does not match authorized username",
            ),
            RejectionClass::HarnessBug,
        ),
        (
            rejection(21, Some("stale-job"), classify::NEW_PAYOUT_WORK_PENDING),
            RejectionClass::Expected,
        ),
        (
            rejection(21, Some("stale-job"), classify::NEW_TIP_WORK_PENDING),
            RejectionClass::Expected,
        ),
        (
            rejection(21, Some("stale-job"), "stale job"),
            RejectionClass::Expected,
        ),
        (
            rejection(21, Some("unknown-job"), "stale job"),
            RejectionClass::Expected,
        ),
        (
            rejection(21, Some("pool-closed"), "no current work"),
            RejectionClass::Expected,
        ),
        (
            rejection(
                20,
                Some("backend-rpc-unavailable"),
                "current chain state is unavailable",
            ),
            RejectionClass::Backend,
        ),
        (
            rejection(
                20,
                Some("ledger-confirmation-failed"),
                "share was not confirmed by the database",
            ),
            RejectionClass::Backend,
        ),
        (
            rejection(20, Some("internal-error"), "difficulty overflow"),
            RejectionClass::Backend,
        ),
        (
            rejection(20, None, "too many connections for username"),
            RejectionClass::Expected,
        ),
        (
            rejection(20, Some("brand-new-reason"), "something else"),
            RejectionClass::Unknown,
        ),
        (
            rejection(20, None, "something nobody has seen"),
            RejectionClass::Unknown,
        ),
    ];
    for (rejection, expected) in cases {
        assert_eq!(
            classify::classify(&rejection),
            expected,
            "{:?} / {}",
            rejection.reason_id,
            rejection.message
        );
    }
    assert!(classify::is_rebuild_pending(&rejection(
        21,
        Some("stale-job"),
        classify::NEW_PAYOUT_WORK_PENDING
    )));
    assert!(!classify::is_rebuild_pending(&rejection(
        21,
        Some("stale-job"),
        "stale job"
    )));
}

#[test]
fn only_a_confirmation_failure_can_be_followed_by_a_commit() {
    // This is the one refusal the append can lose a race with: the commit
    // deadline answers the miner while PostgreSQL still commits the append.
    let failure = rejection(
        20,
        Some(classify::LEDGER_CONFIRMATION_FAILED),
        classify::NOT_CONFIRMED_BY_DATABASE,
    );
    assert!(classify::is_confirmation_failure(&failure));
    assert_eq!(classify::classify(&failure), RejectionClass::Backend);
    for other in [
        rejection(21, Some("stale-job"), classify::NEW_TIP_WORK_PENDING),
        rejection(23, Some("low-difficulty"), "low difficulty share"),
        rejection(
            20,
            Some("backend-rpc-unavailable"),
            "current chain state is unavailable",
        ),
        rejection(22, Some("duplicate-share"), "duplicate share"),
    ] {
        assert!(
            !classify::is_confirmation_failure(&other),
            "{:?} must not be treated as a confirmation failure",
            other.reason_id
        );
    }
}

#[test]
fn the_blocked_log_classifier_recognises_the_real_refusal_messages() {
    // The refusal the JSONB container ceiling produces, wrapped in the warning
    // `coordinator.rs` `refresh_loop` actually logs.
    let ceiling = "2026-09-11T10:00:00.123456Z  WARN qbit_prism_server::coordinator: template \
                   refresh deferred error=error returned from database: total size of jsonb \
                   object elements exceeds the maximum of 268435455 bytes";
    let found = classify::classify_log_line(ceiling).expect("ceiling refusal must be recognised");
    assert_eq!(found.kind, BlockedKind::JsonbCeiling);
    assert!(classify::is_hard_block(&found));

    let deferred = format!(
        "2026-09-11T10:00:00.123456Z  WARN qbit_prism_server::coordinator: {} error=tip changed \
         during job build",
        classify::REFRESH_DEFERRED
    );
    let found = classify::classify_log_line(&deferred).expect("a deferral must be recognised");
    assert_eq!(found.kind, BlockedKind::RefreshDeferred);
    assert!(
        !classify::is_hard_block(&found),
        "a transient deferral is not a blocked size"
    );

    for message in [
        classify::JOB_PREPARATION_DEFERRED,
        classify::JOB_PERSISTENCE_DEFERRED,
    ] {
        let line = format!("  WARN qbit_prism_server::coordinator: {message} error=whatever");
        assert_eq!(
            classify::classify_log_line(&line).map(|log| log.kind),
            Some(BlockedKind::JobDeferred)
        );
    }
    assert!(classify::classify_log_line(
        "2026-09-11T10:00:00Z  INFO qbit_prism_server::server: PRISM listening address=127.0.0.1:3340"
    )
    .is_none());
}

// --- small helpers --------------------------------------------------------

fn submit_record(
    phase: &str,
    outcome: qbit_prism_load::client::Outcome,
) -> qbit_prism_load::client::SubmitRecord {
    qbit_prism_load::client::SubmitRecord {
        share_id: format!("pload1abc.s00001:{}", "0".repeat(64)),
        session: 1,
        frontend: 0,
        phase: phase.to_owned(),
        job_id: "job-1".into(),
        sent: std::time::Instant::now(),
        responded: None,
        latency_millis: Some(16_000.0),
        outcome,
        scheduled_block: false,
        reoffer: false,
        header_hex: String::new(),
        extranonce2_hex: String::new(),
        ntime_hex: String::new(),
        nonce_hex: String::new(),
    }
}

#[test]
fn a_committed_share_is_a_divergence_only_when_a_confirmation_failure_explains_it() {
    use qbit_prism_load::client::Outcome;
    use qbit_prism_load::run::GapKind;
    let divergence = submit_record(
        "slow_database",
        Outcome::Rejected(rejection(
            20,
            Some(classify::LEDGER_CONFIRMATION_FAILED),
            classify::NOT_CONFIRMED_BY_DATABASE,
        )),
    );
    assert_eq!(
        run::classify_committed_gap(Some(&divergence)),
        GapKind::AckCommitDivergence
    );
    // Every other explanation, and no explanation at all, is a loss.
    for outcome in [
        Outcome::Accepted,
        Outcome::NoResponse {
            reason: "socket closed".into(),
        },
        Outcome::Rejected(rejection(
            20,
            Some("backend-rpc-unavailable"),
            "current chain state is unavailable",
        )),
        Outcome::Rejected(rejection(
            21,
            Some("stale-job"),
            classify::NEW_TIP_WORK_PENDING,
        )),
    ] {
        let record = submit_record("slow_database", outcome);
        assert_eq!(
            run::classify_committed_gap(Some(&record)),
            GapKind::DurabilityLoss,
            "{:?} must not be read as a divergence",
            record.outcome
        );
    }
    assert_eq!(
        run::classify_committed_gap(None),
        GapKind::DurabilityLoss,
        "a committed row the harness never offered is a loss, not a divergence"
    );
}

#[test]
fn an_unknown_outcome_commit_is_neither_a_divergence_nor_a_loss() {
    // #333 answers `ledger-outcome-unknown` when the COMMIT still has no reply
    // after the commit deadline and its grace window. The server is saying it
    // does not know, so a later commit is possible and is not a lost share.
    // Reading it as a durability loss would raise a false data-loss alarm.
    use qbit_prism_load::client::Outcome;
    use qbit_prism_load::run::GapKind;
    let unknown = rejection(
        20,
        Some(classify::LEDGER_OUTCOME_UNKNOWN),
        "share commit outcome is unknown",
    );
    assert!(classify::is_outcome_unknown(&unknown));
    assert!(
        !classify::is_confirmation_failure(&unknown),
        "an unknown outcome is not the same claim as a confirmation failure"
    );
    assert_eq!(
        classify::classify(&unknown),
        RejectionClass::Backend,
        "an unknown outcome is a backend result, not an unrecognised reason"
    );
    let record = submit_record("slow_database", Outcome::Rejected(unknown));
    assert_eq!(
        run::classify_committed_gap(Some(&record)),
        GapKind::UnknownOutcomeCommitted
    );
}

#[test]
fn only_entitled_races_are_kept_out_of_the_offered_set() {
    use qbit_prism_load::client::Outcome;
    let mut records = Vec::new();
    let mut push = |id: &str, outcome: Outcome| {
        let mut record = submit_record("steady_state", outcome);
        record.share_id = id.to_owned();
        records.push(record);
    };
    push("accepted", Outcome::Accepted);
    push(
        "expected",
        Outcome::Rejected(rejection(
            21,
            Some("stale-job"),
            classify::NEW_TIP_WORK_PENDING,
        )),
    );
    push(
        "backend",
        Outcome::Rejected(rejection(
            20,
            Some(classify::LEDGER_CONFIRMATION_FAILED),
            classify::NOT_CONFIRMED_BY_DATABASE,
        )),
    );
    push(
        "bug",
        Outcome::Rejected(rejection(
            23,
            Some("low-difficulty"),
            "low difficulty share",
        )),
    );
    push(
        "lost",
        Outcome::NoResponse {
            reason: "socket closed".into(),
        },
    );
    push("other_phase", Outcome::Accepted);
    records.last_mut().unwrap().phase = "reconnect".into();

    let (offered, acknowledged) = run::offered_and_acknowledged(&records, "steady_state");
    assert_eq!(
        offered.iter().map(String::as_str).collect::<Vec<_>>(),
        vec!["accepted", "backend", "bug", "lost"],
        "only the entitled race is excluded, and only this phase is counted"
    );
    assert_eq!(
        acknowledged.iter().map(String::as_str).collect::<Vec<_>>(),
        vec!["accepted"]
    );
    // A re-offer is never an offer of its own.
    let mut with_reoffer = records.clone();
    with_reoffer[0].reoffer = true;
    let (offered, _) = run::offered_and_acknowledged(&with_reoffer, "steady_state");
    assert!(!offered.contains("accepted"));
}

#[test]
fn the_two_reconciliation_gaps_have_distinct_exit_codes() {
    // A loss and a divergence are different failures, and neither may be
    // reported as a clean run.
    let codes = [
        run::EXIT_OK,
        run::EXIT_ERROR,
        run::EXIT_BLOCKED,
        run::EXIT_DURABILITY,
        run::EXIT_ACK_COMMIT_DIVERGENCE,
        run::EXIT_ABORTED,
        run::EXIT_HARNESS_BUG_REJECTIONS,
    ];
    let unique: std::collections::BTreeSet<i32> = codes.into_iter().collect();
    assert_eq!(unique.len(), codes.len(), "exit codes must be distinct");
    assert_ne!(run::EXIT_ACK_COMMIT_DIVERGENCE, run::EXIT_DURABILITY);
    assert_ne!(run::EXIT_ACK_COMMIT_DIVERGENCE, run::EXIT_OK);
}

/// The startup gate's precondition is that every session holds work. A
/// count of connection events cannot say that: one session that dropped and
/// reconnected produces two events and would stand in for another session
/// that never finished its handshake. The gate reads distinct sessions that
/// are connected right now.
#[test]
fn the_startup_gate_counts_sessions_holding_work_not_connection_events() {
    let mut collected = run::Collected::default();
    let connected = |session: usize| client::Event::Connected {
        session,
        frontend: 0,
    };
    // Session 0 connects, drops and reconnects while session 1 is still in
    // its handshake: two connection events, one session with work.
    collected.apply(connected(0));
    collected.apply(client::Event::Disconnected {
        session: 0,
        frontend: 0,
        reason: "end of stream".into(),
    });
    collected.apply(connected(0));
    assert_eq!(collected.connects, 2, "the event count is still reported");
    assert_eq!(
        collected.sessions_holding_work(),
        1,
        "two events from one session are one session"
    );
    assert!(
        collected.sessions_holding_work() < 2,
        "a two-session run is not ready yet"
    );
    // A session that is currently disconnected does not hold work.
    collected.apply(connected(1));
    assert_eq!(collected.sessions_holding_work(), 2);
    collected.apply(client::Event::Disconnected {
        session: 1,
        frontend: 0,
        reason: "socket closed".into(),
    });
    assert_eq!(collected.sessions_holding_work(), 1);
    collected.apply(connected(1));
    assert_eq!(
        collected.sessions_holding_work(),
        2,
        "now every session holds work"
    );
}

#[test]
fn the_harness_reads_its_own_postgres_binary_variable() {
    // The shared test-gate variables belong to the gate crate (#322); a second
    // reader of one would make that gate's manifest wrong.
    assert_eq!(
        qbit_prism_load::cluster::PG_BIN_DIR_VAR,
        "QBIT_PRISM_LOAD_PG_BIN_DIR"
    );
    for shared in [
        concat!("PRISM_TEST", "_PG_BIN_DIR"),
        concat!("PRISM_TEST", "_DATABASE_URL"),
        concat!("QBITD", "_BIN"),
        concat!("GITHUB", "_JOB"),
    ] {
        assert_ne!(qbit_prism_load::cluster::PG_BIN_DIR_VAR, shared);
    }
}

#[test]
fn a_foreign_order_lock_holder_is_not_billed_to_the_frontends() {
    use qbit_prism_load::measure::{is_own_row, split_lock_rows, LockRow, ORDER_LOCK};
    let row = |pid: i32, granted: bool, name: &str| LockRow {
        pid,
        objid: ORDER_LOCK.objid,
        granted,
        waitstart: None,
        application_name: name.to_owned(),
        activity_visible: true,
    };
    let rows = vec![
        // A frontend holding the lock is the normal case and belongs in no
        // foreign counter.
        row(1, true, "load-fe-0"),
        row(2, false, "load-fe-1"),
        // Anything else holding it stalls every frontend, which is exactly
        // what used to be invisible.
        row(3, true, "adversarial-foreign-holder"),
        row(4, false, "psql"),
    ];
    let frontends = vec!["load-fe-0".to_owned(), "load-fe-1".to_owned()];
    let split = split_lock_rows(&rows, &frontends);
    assert_eq!(split.own_holding.len(), 1);
    assert_eq!(split.own_waiting.len(), 1, "the waiter numbers stay ours");
    assert_eq!(split.foreign_holding.len(), 1);
    assert_eq!(
        split.foreign_holding[0].application_name,
        "adversarial-foreign-holder"
    );
    assert_eq!(split.foreign_waiting.len(), 1);
    assert_eq!(split.foreign_waiting[0].application_name, "psql");

    // With no attribution nothing can be called foreign: every ungranted row
    // counts as this run's, which is what the summary's attribution note says.
    let blind = split_lock_rows(&rows, &[]);
    assert!(blind.foreign_holding.is_empty());
    assert!(blind.foreign_waiting.is_empty());
    assert_eq!(blind.own_waiting.len(), 2);
    assert_eq!(blind.own_holding.len(), 2);
    assert!(is_own_row(&row(5, true, "anything"), &[]));
    assert!(!is_own_row(&row(5, true, "anything"), &frontends));

    // A lock row whose `pg_stat_activity` row this role cannot read is
    // foreign, even with no attribution at all: nothing about it can be shown
    // to belong to this run, and counting it as ours is the unsafe direction.
    let opaque = LockRow {
        activity_visible: false,
        application_name: String::new(),
        ..row(6, true, "")
    };
    assert!(!is_own_row(&opaque, &frontends));
    assert!(!is_own_row(&opaque, &[]));
    let blind_opaque = split_lock_rows(std::slice::from_ref(&opaque), &[]);
    assert_eq!(blind_opaque.foreign_holding.len(), 1);
    assert!(blind_opaque.own_holding.is_empty());
    assert_eq!(
        qbit_prism_load::measure::row_label(&opaque),
        qbit_prism_load::measure::UNREADABLE_ACTIVITY,
        "an unreadable row is named, not reported as an empty application_name"
    );
    assert_eq!(
        qbit_prism_load::measure::row_label(&row(7, false, "load-fe-0")),
        "load-fe-0"
    );
}

#[test]
fn the_two_sampled_locks_are_summarized_apart() {
    // The rebuild after a landing takes SETTLEMENT_LOCK first and ORDER_LOCK
    // second. One poll carries both, so a summary must read only its own lock:
    // mixing them would report a rebuild's queue as a share append's.
    use qbit_prism_load::measure::{
        split_lock_rows_for, LockRow, ORDER_LOCK, PRISM_LOCK_CLASSID, SAMPLED_LOCKS,
        SETTLEMENT_LOCK,
    };
    assert_eq!(ORDER_LOCK.key, "0x505249534d000002");
    assert_eq!(SETTLEMENT_LOCK.key, "0x505249534d000003");
    assert_ne!(ORDER_LOCK.objid, SETTLEMENT_LOCK.objid);
    assert_eq!(SAMPLED_LOCKS.len(), 2);
    // The predicate is the hex key split in half, not a decimal typed in.
    assert_eq!(PRISM_LOCK_CLASSID, 0x5052_4953);
    assert_eq!(
        (PRISM_LOCK_CLASSID << 32) | SETTLEMENT_LOCK.objid,
        0x5052_4953_4d00_0003
    );
    assert!(SETTLEMENT_LOCK.taken_by.contains("rebuild"));
    assert!(ORDER_LOCK.taken_by.contains("append"));

    let row = |pid: i32, objid: i64, granted: bool, name: &str| LockRow {
        pid,
        objid,
        granted,
        waitstart: None,
        application_name: name.to_owned(),
        activity_visible: true,
    };
    let rows = vec![
        row(1, ORDER_LOCK.objid, true, "load-fe-0"),
        row(2, ORDER_LOCK.objid, false, "load-fe-1"),
        row(3, SETTLEMENT_LOCK.objid, true, "load-fe-0"),
        row(4, SETTLEMENT_LOCK.objid, false, "load-fe-1"),
        row(5, SETTLEMENT_LOCK.objid, false, "load-fe-2"),
        row(6, SETTLEMENT_LOCK.objid, true, "adversarial-foreign-holder"),
    ];
    let frontends = vec![
        "load-fe-0".to_owned(),
        "load-fe-1".to_owned(),
        "load-fe-2".to_owned(),
    ];
    let order = split_lock_rows_for(&rows, ORDER_LOCK.objid, &frontends);
    assert_eq!(order.own_waiting.len(), 1, "one waiter on ORDER_LOCK");
    assert_eq!(order.own_holding.len(), 1);
    assert!(
        order.foreign_holding.is_empty(),
        "the foreign holder is on the other lock"
    );

    let settlement = split_lock_rows_for(&rows, SETTLEMENT_LOCK.objid, &frontends);
    assert_eq!(
        settlement.own_waiting.len(),
        2,
        "the rebuild's queue is its own number, not folded into ORDER_LOCK's"
    );
    assert_eq!(settlement.own_holding.len(), 1);
    assert_eq!(settlement.foreign_holding.len(), 1);
    assert_eq!(
        settlement.foreign_holding[0].application_name,
        "adversarial-foreign-holder"
    );

    // Every row belongs to exactly one block: nothing is counted twice and
    // nothing disappears between the two.
    let total = |split: &qbit_prism_load::measure::LockRowSplit<'_>| {
        split.own_waiting.len()
            + split.own_holding.len()
            + split.foreign_waiting.len()
            + split.foreign_holding.len()
    };
    assert_eq!(total(&order) + total(&settlement), rows.len());
}

#[test]
fn the_temporary_cluster_root_is_removed_even_when_start_up_fails() -> Result<()> {
    use qbit_prism_load::cluster::TempRoot;
    // The directory has to exist before the value that owns the cleanup can be
    // built, so the guard owns it from the first instant: a failure anywhere
    // between the two leaves nothing behind.
    let path = {
        let root = TempRoot::create(false)?;
        assert!(root.path().is_dir());
        std::fs::write(root.path().join("primary.log"), b"as if initdb had run")?;
        root.path().to_path_buf()
    };
    assert!(
        !path.exists(),
        "{} survived the guard, so a failed start-up would orphan it",
        path.display()
    );

    // `--keep-artifacts` means what it says.
    let kept = {
        let root = TempRoot::create(true)?;
        root.path().to_path_buf()
    };
    assert!(kept.is_dir(), "--keep-artifacts must keep the root");
    std::fs::remove_dir_all(&kept)?;
    Ok(())
}

#[test]
fn a_postgres_bin_directory_without_the_server_binaries_is_refused() -> Result<()> {
    use qbit_prism_load::cluster;
    let root = std::env::temp_dir().join(format!("prism-load-bindir-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    let missing = format!(
        "{:#}",
        cluster::verify_bin_dir(&root).expect_err("a directory that does not exist is refused")
    );
    assert!(missing.contains("--pg-bin-dir"), "{missing}");
    assert!(missing.contains("is not a directory"), "{missing}");

    std::fs::create_dir_all(&root)?;
    let empty = format!(
        "{:#}",
        cluster::verify_bin_dir(&root).expect_err("a directory with no binaries is refused")
    );
    assert!(empty.contains("initdb"), "{empty}");
    assert!(empty.contains("pg_basebackup"), "{empty}");

    for name in cluster::REQUIRED_BINARIES {
        std::fs::write(root.join(name), b"")?;
    }
    cluster::verify_bin_dir(&root).expect("all three binaries present");

    std::fs::remove_file(root.join("pg_ctl"))?;
    let partial = format!(
        "{:#}",
        cluster::verify_bin_dir(&root).expect_err("one missing binary is refused")
    );
    assert!(partial.contains("pg_ctl"), "{partial}");
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

#[test]
fn database_urls_are_rewritten_onto_the_delay_proxy() -> Result<()> {
    assert_eq!(
        run::rewrite_host(
            "postgresql://alex@127.0.0.1:5432/postgres",
            "127.0.0.1:9999"
        )?,
        "postgresql://alex@127.0.0.1:9999/postgres"
    );
    assert_eq!(
        run::rewrite_host(
            "postgres://u:p@db.example:5432/qbit?sslmode=disable",
            "127.0.0.1:1"
        )?,
        "postgres://u:p@127.0.0.1:1/qbit?sslmode=disable"
    );
    assert_eq!(
        run::rewrite_host("postgresql://127.0.0.1:5432/postgres", "127.0.0.1:2")?,
        "postgresql://127.0.0.1:2/postgres"
    );
    assert_eq!(
        run::with_application_name("postgresql://u@h:1/db", "load-fe-0"),
        "postgresql://u@h:1/db?application_name=load-fe-0"
    );
    assert_eq!(
        run::with_application_name("postgresql://u@h:1/db?sslmode=disable", "load-fe-1"),
        "postgresql://u@h:1/db?sslmode=disable&application_name=load-fe-1"
    );
    assert_eq!(run::host_port("postgresql://u@host:6000/db")?, "host:6000");
    assert_eq!(run::host_port("postgresql://u@host/db")?, "host:5432");
    assert!(run::host_port("not-a-url").is_err());
    Ok(())
}

/// `--database-url postgresql://user@postgres.example/db` is an ordinary URL,
/// and the SQLx connections before the proxy accept the name, so the proxy
/// has to as well: it resolves the host once at entry, tries every address
/// it names, and refuses a host that does not resolve with its name.
#[tokio::test]
async fn the_delay_proxy_accepts_a_hostname_upstream() -> Result<()> {
    use qbit_prism_load::proxy::{resolve_upstream, DelayProxy};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let echo = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 5];
        socket.read_exact(&mut buffer).await.unwrap();
        socket.write_all(&buffer).await.unwrap();
    });

    let upstream = format!("localhost:{port}");
    let resolved = resolve_upstream(&upstream).await?;
    assert!(
        resolved.iter().all(|address| address.port() == port),
        "every resolved address carries the port: {resolved:?}"
    );
    assert!(
        upstream.parse::<std::net::SocketAddr>().is_err(),
        "the name is exactly what a SocketAddr parse refuses"
    );
    let proxy = DelayProxy::open(&upstream).await?;
    assert_eq!(proxy.upstream, upstream);
    assert_eq!(proxy.upstream_resolved, resolved);
    let mut client = tokio::net::TcpStream::connect(proxy.url_host()).await?;
    client.write_all(b"hello").await?;
    let mut reply = [0u8; 5];
    client.read_exact(&mut reply).await?;
    assert_eq!(&reply, b"hello", "bytes reach the named upstream and back");
    echo.await?;

    let numeric = resolve_upstream("127.0.0.1:5432").await?;
    assert_eq!(numeric, vec!["127.0.0.1:5432".parse()?]);

    // A label that is not a valid host name is refused by the resolver
    // without a network query, so the test does not wait on search-domain
    // retries the way a plausible-looking unknown name would.
    let refused = match DelayProxy::open("-no-such-host-.invalid.:5432").await {
        Ok(_) => panic!("an unresolvable host is refused at entry"),
        Err(error) => format!("{error:#}"),
    };
    assert!(refused.contains("-no-such-host-.invalid."), "{refused}");
    Ok(())
}

#[test]
fn phase_durations_render_without_precision_loss() {
    assert_eq!(artifact::millis_as_seconds(60_000), "60.000");
    assert_eq!(artifact::millis_as_seconds(60_501), "60.501");
    assert_eq!(artifact::millis_as_seconds(1), "0.001");
}

#[test]
fn the_database_profile_digest_is_a_function_of_the_content_only() {
    let a = json!({"b": 1, "a": {"z": [1, 2], "y": "x"}});
    let b = json!({"a": {"y": "x", "z": [1, 2]}, "b": 1});
    assert_eq!(profile::canonical_json(&a), profile::canonical_json(&b));
    assert_eq!(
        profile::digest(&profile::canonical_json(&a)),
        profile::digest(&profile::canonical_json(&b))
    );
    assert_ne!(
        profile::digest(&profile::canonical_json(&a)),
        profile::digest(&profile::canonical_json(&json!({"b": 2})))
    );
}

#[test]
fn the_shipped_profile_file_hashes_to_the_digest_the_artifact_names() -> Result<()> {
    use sha2::{Digest, Sha256};
    // The one check a third party can make on an evidence bundle is
    // `sha256sum database-profile.json` against
    // `subject.database_profile_sha256`, so the bytes on disk have to be
    // exactly the bytes that were digested: no trailing newline, nothing else
    // appended (F1).
    let document = profile::build(
        json!({"settings": {"fsync": "on"}}),
        "16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)",
        json!({"declared": "async"}),
        json!({"kind": "in-harness tokio TCP proxy"}),
        json!({"cpus": 8}),
        json!([{"instance_id": "load-fe-0"}]),
    );
    let canonical = profile::canonical_json(&document);
    let digest = profile::digest(&canonical);
    let path = std::env::temp_dir().join(format!("prism-load-profile-{}.json", std::process::id()));
    profile::write_document(&path, &canonical)?;
    let bytes = std::fs::read(&path)?;
    std::fs::remove_file(&path)?;
    assert_eq!(
        hex::encode(Sha256::digest(&bytes)),
        digest,
        "sha256sum of the shipped file must equal subject.database_profile_sha256"
    );
    assert_ne!(
        bytes.last(),
        Some(&b'\n'),
        "a trailing newline would break every third-party verification"
    );
    // The file still parses as the document it describes.
    let reparsed: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(reparsed["schema"], json!(profile::SCHEMA));
    Ok(())
}

#[test]
fn latency_percentiles_report_unknown_rather_than_zero() {
    use qbit_prism_load::measure::{summarize, MILLISECONDS};
    let empty = summarize(Vec::new(), MILLISECONDS, "client monotonic");
    assert_eq!(empty.samples, 0);
    assert!(empty.p50.is_none() && empty.p99.is_none() && empty.max.is_none());
    assert!(empty.unavailable_reason.is_some());
    assert_eq!(
        empty.unit, MILLISECONDS,
        "an empty summary still names its unit"
    );
    let summary = summarize(
        (1..=100).map(f64::from).collect(),
        MILLISECONDS,
        "client monotonic",
    );
    assert_eq!(summary.p50, Some(50.0));
    assert_eq!(summary.p99, Some(99.0));
    assert_eq!(summary.max, Some(100.0));
    // A real zero is a measurement, not an absence.
    let zeros = summarize(vec![0.0, 0.0], MILLISECONDS, "client monotonic");
    assert_eq!(zeros.p50, Some(0.0));
    assert!(zeros.unavailable_reason.is_none());
}

#[test]
fn a_count_distribution_is_not_published_as_milliseconds() {
    // A consumer generic over the summary shape reads `unit`. Labelling a
    // count "milliseconds" renders 85 discarded shares as "85 ms", and a clock
    // string is not where such a consumer looks: a carried unit that is wrong
    // is worse than none (EP-OBSERVABILITY).
    use qbit_prism_load::measure::{summarize, COUNT, MILLISECONDS};
    assert_ne!(COUNT, MILLISECONDS);
    let counted = summarize(vec![3.0, 5.0, 85.0], COUNT, "one sample per landing");
    assert_eq!(counted.unit, COUNT);
    assert_eq!(counted.max, Some(85.0));
    assert_eq!(
        summarize(Vec::new(), COUNT, "one sample per landing").unit,
        COUNT,
        "an empty count summary is still a count"
    );
}

#[test]
fn server_histogram_scrapes_are_parsed_into_bucket_deltas() {
    let body = "\
# HELP qbit_prism_share_ack_seconds Complete mining.submit frame arrival to completed response write, by outcome.
# TYPE qbit_prism_share_ack_seconds histogram
qbit_prism_share_ack_seconds_bucket{result=\"accepted\",le=\"0.01\"} 5
qbit_prism_share_ack_seconds_bucket{result=\"accepted\",le=\"0.025\"} 9
qbit_prism_share_ack_seconds_sum{result=\"accepted\"} 0.5
qbit_prism_share_ack_seconds_count{result=\"accepted\"} 9
";
    let mut before = qbit_prism_load::measure::MetricsScrape {
        instance_id: "load-fe-0".into(),
        ok: true,
        ..Default::default()
    };
    qbit_prism_load::measure::parse_share_ack(body, &mut before);
    assert_eq!(before.ack_counts["accepted"], 9.0);
    assert_eq!(before.ack_buckets["accepted"]["0.01"], 5.0);
    let mut after = before.clone();
    after.ack_counts.insert("accepted".into(), 19.0);
    after
        .ack_buckets
        .get_mut("accepted")
        .unwrap()
        .insert("0.01".into(), 11.0);
    let delta = qbit_prism_load::measure::ack_delta(&before, &after);
    assert_eq!(delta.counts["accepted"], 10.0);
    assert_eq!(delta.bucket_deltas["accepted"]["0.01"], 6.0);
    assert!(delta.unavailable_reason.is_none());
    assert_eq!(
        delta.buckets_seconds,
        qbit_prism_server::metrics::BUCKETS.to_vec()
    );

    // A failed scrape must not look like zero traffic.
    let failed = qbit_prism_load::measure::MetricsScrape {
        instance_id: "load-fe-0".into(),
        ok: false,
        error: Some("connection refused".into()),
        ..Default::default()
    };
    let delta = qbit_prism_load::measure::ack_delta(&before, &failed);
    assert_eq!(
        delta.unavailable_reason.as_deref(),
        Some("connection refused")
    );
    assert!(delta.counts.is_empty());
}

/// A frontend restart between the two boundary scrapes resets its in-process
/// counters, so `after - before` was negative: the coordinator saw the
/// `reconnect` phase's `server_share_ack_seconds` for `load-fe-1` report
/// `accepted` counts of -1273 and -1238 with every bucket negative. A reset
/// must be visible as a reset -- not a negative, not a plausible small
/// number, and not zero -- and with a scrape on each side of the restart the
/// two per-process segments can be summed into the real number.
#[test]
fn a_counter_reset_between_boundary_scrapes_is_reported_not_subtracted() {
    use qbit_prism_load::measure::{
        ack_delta, ack_delta_across_restarts, parse_share_ack, AckSplit, MetricsScrape,
    };
    let scrape = |accepted: f64, bucket: f64| {
        let mut scrape = MetricsScrape {
            instance_id: "load-fe-1".into(),
            ok: true,
            ..Default::default()
        };
        parse_share_ack(
            &format!(
                "qbit_prism_share_ack_seconds_bucket{{result=\"accepted\",le=\"0.01\"}} {bucket}\n\
                 qbit_prism_share_ack_seconds_sum{{result=\"accepted\"}} {}\n\
                 qbit_prism_share_ack_seconds_count{{result=\"accepted\"}} {accepted}\n",
                accepted / 100.0
            ),
            &mut scrape,
        );
        scrape
    };
    // The old process had answered 1300 shares at the phase start; the new
    // one had answered 27 by the phase end.
    let before = scrape(1300.0, 900.0);
    let after = scrape(27.0, 20.0);

    // No restart recorded, counters went backwards: unknown, never -1273.
    let delta = ack_delta(&before, &after);
    assert!(delta.counts.is_empty(), "{:?}", delta.counts);
    assert!(delta.bucket_deltas.is_empty());
    let reason = delta.unavailable_reason.clone().expect("a reason is given");
    assert!(reason.contains("went from 1300 to 27"), "{reason}");
    assert!(reason.contains("without a recorded restart"), "{reason}");
    assert_eq!(delta.segments, 0);

    // A restart the run knows about but did not bracket with scrapes: the
    // delta is unknown and says why, and the reset count is visible.
    let delta = ack_delta_across_restarts(&before, &[], &after, 1);
    assert!(delta.counts.is_empty());
    assert_eq!(delta.counter_resets, 1);
    assert_eq!(delta.segments, 0);
    let reason = delta.unavailable_reason.clone().expect("a reason is given");
    assert!(reason.contains("restarted 1 time"), "{reason}");
    assert!(reason.contains("0 of those restarts"), "{reason}");

    // Scraped on each side of the restart: the old process reached 1500
    // before the kill, the new one started from 0, so the phase saw
    // (1500 - 1300) + (27 - 0) = 227 acknowledgements.
    let split = AckSplit {
        end_of_previous: scrape(1500.0, 1000.0),
        start_of_next: scrape(0.0, 0.0),
    };
    let delta = ack_delta_across_restarts(&before, &[&split], &after, 1);
    assert!(
        delta.unavailable_reason.is_none(),
        "{:?}",
        delta.unavailable_reason
    );
    assert_eq!(delta.counts["accepted"], 227.0);
    assert_eq!(delta.bucket_deltas["accepted"]["0.01"], 120.0);
    assert!((delta.sums["accepted"] - 2.27).abs() < 1e-9);
    assert_eq!(delta.counter_resets, 1);
    assert_eq!(delta.segments, 2);
    assert!(delta
        .note
        .as_deref()
        .unwrap_or_default()
        .contains("2 per-process segments"));

    // A failed scrape on one side of the restart is a failed delta, and the
    // other segment's number is not published as the phase's.
    let split = AckSplit {
        end_of_previous: scrape(1500.0, 1000.0),
        start_of_next: MetricsScrape {
            instance_id: "load-fe-1".into(),
            ok: false,
            error: Some("connection refused".into()),
            ..Default::default()
        },
    };
    let delta = ack_delta_across_restarts(&before, &[&split], &after, 1);
    assert!(delta.counts.is_empty());
    assert_eq!(delta.segments, 0);
    assert_eq!(
        delta.unavailable_reason.as_deref(),
        Some("connection refused")
    );

    // No restart, counters only ever grew: the plain delta as before.
    let delta = ack_delta(&before, &scrape(1400.0, 950.0));
    assert_eq!(delta.counts["accepted"], 100.0);
    assert_eq!(delta.counter_resets, 0);
    assert_eq!(delta.segments, 1);
    assert!(delta.note.is_none());
}

// --- the drained restart --------------------------------------------------

/// The smallest HTTP server the restart driver's readiness probe and metrics
/// scrapes can talk to: every request gets a 200 and a one-bucket histogram.
async fn stand_in_audit_port() -> (u16, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut request = vec![0u8; 4096];
                let _ = socket.read(&mut request).await;
                let body = "qbit_prism_share_ack_seconds_count{result=\"accepted\"} 5\n";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (port, task)
}

/// A session handle with no session task behind it, so a test can hold the
/// control receiver and set the outstanding counter directly.
fn detached_session(
    index: usize,
    frontend: usize,
    outstanding: usize,
) -> (
    client::SessionHandle,
    tokio::sync::mpsc::UnboundedReceiver<client::Control>,
) {
    let (handle, control_rx, _work_rx) = queued_session(index, frontend, outstanding, 1);
    (handle, control_rx)
}

/// A session handle with no task behind it, whose work queue stays open with
/// room for `queue` offers and is never consumed: an accepted offer stays
/// visible in `outstanding`, which is what a scheduler test needs.
fn queued_session(
    index: usize,
    frontend: usize,
    outstanding: usize,
    queue: usize,
) -> (
    client::SessionHandle,
    tokio::sync::mpsc::UnboundedReceiver<client::Control>,
    tokio::sync::mpsc::Receiver<client::Work>,
) {
    let (work, work_rx) = tokio::sync::mpsc::channel(queue);
    let (control, control_rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = client::SessionHandle {
        index,
        frontend: Arc::new(std::sync::atomic::AtomicUsize::new(frontend)),
        outstanding: Arc::new(std::sync::atomic::AtomicUsize::new(outstanding)),
        paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        work,
        control,
        task: tokio::spawn(async {}),
    };
    (handle, control_rx, work_rx)
}

fn stand_in_frontend(
    server: &std::path::Path,
    log_dir: &std::path::Path,
    index: usize,
    audit_port: u16,
) -> frontend::Frontend {
    frontend::Frontend::launch(
        server.to_path_buf(),
        FrontendSpec {
            index,
            instance_id: format!("load-fe-{index}"),
            stratum_port: 1,
            audit_port,
            database_url: "postgresql://u@127.0.0.1:1/x".into(),
        },
        BTreeMap::new(),
        log_dir,
    )
    .unwrap()
}

/// The reconnect phase measures the offered rate while one frontend is
/// unavailable, so the restart must not stall the scheduler that offers to
/// the others: every poll returns within a few milliseconds while the whole
/// restart -- drain, kill, relaunch, readiness -- takes far longer. The
/// restarted frontend's sessions are paused first and retargeted at the end;
/// the other frontend's sessions are never touched; and both sides of the
/// reset are scraped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_restart_never_stalls_the_scheduler() -> Result<()> {
    use qbit_prism_load::restart::RestartDriver;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};
    let dir = ScratchDir::new("restart");
    let server = stand_in_server(dir.path(), "PRISM listening (stand-in)");
    let (audit_port, _audit) = stand_in_audit_port().await;
    let mut frontends = vec![
        stand_in_frontend(&server, dir.path(), 0, audit_port),
        stand_in_frontend(&server, dir.path(), 1, audit_port),
    ];
    let first_pid = frontends[1].pid().expect("the stand-in is running");
    let (healthy, mut healthy_control) = detached_session(0, 0, 0);
    let (draining, mut draining_control) = detached_session(1, 1, 3);
    let sessions = vec![healthy, draining];
    // The outstanding submits settle 300 ms into the drain.
    let settle = sessions[1].outstanding.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        settle.store(0, Ordering::SeqCst);
    });

    let mut driver = RestartDriver::start(
        1,
        &sessions,
        Duration::from_secs(5),
        Duration::from_secs(20),
    );
    assert!(matches!(
        draining_control.try_recv(),
        Ok(client::Control::Pause)
    ));
    let started = Instant::now();
    let mut longest_poll = Duration::ZERO;
    let mut polls = 0usize;
    let record = loop {
        let poll_started = Instant::now();
        let progress = driver.poll(&sessions, &mut frontends, &[])?;
        longest_poll = longest_poll.max(poll_started.elapsed());
        polls += 1;
        if let Some(record) = progress {
            break record;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the restart did not complete"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    let total = started.elapsed();
    assert!(
        total >= Duration::from_millis(300),
        "the drain waited for the outstanding submits: {total:?}"
    );
    assert!(
        longest_poll < Duration::from_millis(100),
        "no single poll may stall the scheduler; the longest took {longest_poll:?} over \
         {polls} polls while the restart took {total:?}"
    );
    assert!(
        polls > 10,
        "the scheduler kept running during the restart: {polls} polls"
    );
    assert_eq!(record.index, 1);
    assert!(record.drain_seconds >= 0.3, "{}", record.drain_seconds);
    assert!(record.outage_seconds > 0.0);
    assert!(record.split.end_of_previous.ok && record.split.start_of_next.ok);
    assert_eq!(record.split.end_of_previous.ack_counts["accepted"], 5.0);
    assert_eq!(frontends[1].restarts, 1);
    assert_ne!(
        frontends[1].pid(),
        Some(first_pid),
        "a new process is running"
    );
    assert_eq!(frontends[0].restarts, 0);
    match draining_control.try_recv() {
        Ok(client::Control::Retarget {
            frontend: 1,
            reconnect: false,
            ..
        }) => {}
        other => panic!("the drained session is retargeted at the end: {other:?}"),
    }
    assert!(
        healthy_control.try_recv().is_err(),
        "the healthy frontend's sessions are never paused or retargeted"
    );
    for child in frontends.iter_mut() {
        child.kill();
    }
    Ok(())
}

/// Submits still outstanding after the drain limit -- reachable with the
/// default 15 s commit timeout and likely with a longer one or a contended
/// database -- mean the drained restart cannot be performed. Killing the
/// process anyway would convert the harness's own in-flight requests into
/// `NoResponse` records inside the phase. The driver reports the failure and
/// leaves the frontend running; the run aborts on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_whose_drain_never_completes_is_refused_not_forced() -> Result<()> {
    use qbit_prism_load::restart::RestartDriver;
    use std::time::Duration;
    let dir = ScratchDir::new("undrained");
    let server = stand_in_server(dir.path(), "PRISM listening (stand-in)");
    let (audit_port, _audit) = stand_in_audit_port().await;
    let mut frontends = vec![
        stand_in_frontend(&server, dir.path(), 0, audit_port),
        stand_in_frontend(&server, dir.path(), 1, audit_port),
    ];
    let pid = frontends[1].pid();
    let (stuck, _stuck_control) = detached_session(1, 1, 2);
    let sessions = vec![stuck];
    let mut driver = RestartDriver::start(
        1,
        &sessions,
        Duration::from_millis(300),
        Duration::from_secs(20),
    );
    let error = loop {
        match driver.poll(&sessions, &mut frontends, &[]) {
            Ok(None) => tokio::time::sleep(Duration::from_millis(5)).await,
            Ok(Some(record)) => panic!("an undrained frontend must not be restarted: {record:?}"),
            Err(error) => break format!("{error:#}"),
        }
    };
    assert!(error.contains("2 submits outstanding"), "{error}");
    assert!(error.contains("was not performed"), "{error}");
    assert_eq!(frontends[1].restarts, 0, "the process was left alone");
    assert_eq!(frontends[1].pid(), pid);
    assert!(frontends[1].exited().is_none(), "it is still running");
    for child in frontends.iter_mut() {
        child.kill();
    }
    Ok(())
}

/// The scheduler keeps offering during the restart, and a paused session
/// must be ineligible rather than a place for offers to pile up: an offer it
/// accepted would count as outstanding, hold the drain open until its
/// deadline, take the token from the frontends that are up, and go out as a
/// burst on resume. The flag the scheduler reads goes up when the pause is
/// decided and comes down with the retarget that ends the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_session_is_not_offered_work_until_it_is_retargeted() -> Result<()> {
    use qbit_prism_load::restart::RestartDriver;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};
    let dir = ScratchDir::new("paused-offers");
    let server = stand_in_server(dir.path(), "PRISM listening (stand-in)");
    let (audit_port, _audit) = stand_in_audit_port().await;
    let mut frontends = vec![
        stand_in_frontend(&server, dir.path(), 0, audit_port),
        stand_in_frontend(&server, dir.path(), 1, audit_port),
    ];
    let (healthy, _healthy_control, _healthy_queue) = queued_session(0, 0, 0, 8);
    let (draining, mut draining_control, _draining_queue) = queued_session(1, 1, 2, 8);
    let sessions = vec![healthy, draining];
    let phase: Arc<str> = Arc::from("reconnect");
    let limit = 8;
    // Two submits are in flight on the frontend about to be restarted; they
    // settle 200 ms in. Nothing consumes either session's work queue, so an
    // accepted offer stays visible in `outstanding`.
    let settle = sessions[1].outstanding.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        settle.fetch_sub(2, Ordering::SeqCst);
    });

    let mut driver = RestartDriver::start(
        1,
        &sessions,
        Duration::from_secs(5),
        Duration::from_secs(20),
    );
    // The scheduler's next offer already sees the pause, before the session
    // task has read its control channel.
    assert!(matches!(
        draining_control.try_recv(),
        Ok(client::Control::Pause)
    ));
    assert!(
        !sessions[1].try_offer(limit, &phase),
        "a paused session refuses the offer it has room for"
    );
    assert_eq!(sessions[1].outstanding.load(Ordering::SeqCst), 2);
    assert!(
        sessions[0].try_offer(limit, &phase),
        "the healthy frontend's session takes the token instead"
    );

    // Keep offering, as the reconnect phase's scheduler does, for the whole
    // restart. The drain can only complete because none of these land.
    let started = Instant::now();
    let mut refused = 0usize;
    let record = loop {
        assert!(
            !sessions[1].try_offer(limit, &phase),
            "no offer may land on the paused session while it is restarting"
        );
        refused += 1;
        if let Some(record) = driver.poll(&sessions, &mut frontends, &[])? {
            break record;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the restart did not complete"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    assert!(
        refused > 10,
        "the scheduler kept offering: {refused} offers"
    );
    assert_eq!(record.index, 1);
    assert!(
        record.drain_seconds >= 0.2 && record.drain_seconds < 4.0,
        "the drain waited for the in-flight submits and nothing else: {}",
        record.drain_seconds
    );
    match draining_control.try_recv() {
        Ok(client::Control::Retarget { frontend: 1, .. }) => {}
        other => panic!("the session is retargeted at the end: {other:?}"),
    }
    assert!(
        sessions[1].try_offer(limit, &phase),
        "the retarget makes the session eligible again"
    );
    assert_eq!(
        sessions[1].outstanding.load(Ordering::SeqCst),
        1,
        "only the offer made after the retarget was accepted"
    );
    for child in frontends.iter_mut() {
        child.kill();
    }
    Ok(())
}

/// The drain waits at least the configured commit timeout.
#[test]
fn the_drain_limit_covers_the_configured_commit_timeout() {
    use std::time::Duration;
    assert_eq!(
        run::drain_limit(15.0),
        Duration::from_secs(15) + run::DRAIN_MARGIN
    );
    assert!(run::drain_limit(60.0) >= Duration::from_secs(60));
    assert!(run::drain_limit(0.5) > Duration::from_millis(500));
}

#[test]
fn command_line_validation_rejects_impossible_runs() {
    use clap::Parser;
    let base = ["qbit-prism-load", "--frontends", "2", "--sessions", "8"];
    let args = qbit_prism_load::cli::Args::parse_from(base);
    args.validate().expect("the base arguments are valid");

    let mut bad = args.clone();
    bad.frontends = 3;
    assert!(
        bad.validate().is_err(),
        "only 1, 2 and 4 frontends are supported"
    );

    let mut bad = args.clone();
    bad.rate = f64::NAN;
    assert!(bad.validate().is_err(), "a non-finite rate is refused");

    let mut bad = args.clone();
    bad.rate = 0.0;
    assert!(bad.validate().is_err());

    let mut bad = args.clone();
    bad.slow_db_delay_ms = 5;
    assert!(
        bad.validate().is_err(),
        "the artifact phase needs at least 10 ms"
    );

    let mut bad = args.clone();
    bad.reconnect_target = 9;
    assert!(
        bad.validate().is_err(),
        "the artifact phase needs at least 10 events"
    );

    let mut bad = args.clone();
    bad.ack_p99_limit_ms = 20_000.0;
    assert!(
        bad.validate().is_err(),
        "the limit cannot exceed PRISM_SHARE_COMMIT_TIMEOUT_SECONDS x 1000"
    );

    let mut bad = args.clone();
    bad.db_max_connections = 2;
    assert!(bad.validate().is_err());

    // A sampling interval must be bounded above as well as below: one longer
    // than the phase measures nothing.
    for interval in [0u64, 1_001, 60_000] {
        let mut bad = args.clone();
        bad.lock_sample_interval_ms = interval;
        assert!(
            bad.validate().is_err(),
            "--lock-sample-interval-ms {interval} must be refused"
        );
    }
    for interval in [0u64, 10, 120_000] {
        let mut bad = args.clone();
        bad.process_sample_interval_ms = interval;
        assert!(
            bad.validate().is_err(),
            "--process-sample-interval-ms {interval} must be refused"
        );
    }

    let mut bad = args.clone();
    bad.steady_state_seconds = Some(30);
    assert!(
        bad.validate().is_err(),
        "an artifact phase shorter than 60 s is refused"
    );

    let mut bad = args.clone();
    bad.replication = "maybe".into();
    assert!(bad.validate().is_err());
}

#[test]
fn the_d1_plan_has_the_shape_decision_d1_asks_for() -> Result<()> {
    use clap::Parser;
    let args = qbit_prism_load::cli::Args::parse_from([
        "qbit-prism-load",
        "--plan",
        "d1",
        "--mid-flight-kill",
    ]);
    args.validate()?;
    let phases = qbit_prism_load::cli::phases(&args)?;
    let names: Vec<&str> = phases.iter().map(|phase| phase.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "warm_up",
            "steady_state",
            "burst",
            "reconnect",
            "slow_database",
            "mid_flight_kill"
        ]
    );
    let artifact_phases: Vec<&str> = phases
        .iter()
        .filter(|phase| phase.in_artifact)
        .map(|phase| phase.name.as_str())
        .collect();
    assert_eq!(artifact_phases, REQUIRED_PHASES.to_vec());
    let steady = phases.iter().find(|p| p.name == "steady_state").unwrap();
    assert_eq!((steady.seconds, steady.rate), (300, 500.0));
    let burst = phases.iter().find(|p| p.name == "burst").unwrap();
    assert_eq!((burst.seconds, burst.rate), (60, 2000.0));
    assert!(!burst.in_artifact, "the burst is side-report only");
    let slow = phases.iter().find(|p| p.name == "slow_database").unwrap();
    assert!(slow.seconds >= 60 && slow.database_delay_ms >= 10);
    let reconnect = phases.iter().find(|p| p.name == "reconnect").unwrap();
    assert!(reconnect.seconds >= 60 && reconnect.reconnects);
    Ok(())
}

#[test]
fn the_short_plan_runs_every_artifact_phase_for_a_minute() -> Result<()> {
    use clap::Parser;
    let args = qbit_prism_load::cli::Args::parse_from([
        "qbit-prism-load",
        "--plan",
        "short",
        "--rate",
        "50",
    ]);
    args.validate()?;
    let phases = qbit_prism_load::cli::phases(&args)?;
    for phase in phases.iter().filter(|phase| phase.in_artifact) {
        assert_eq!(phase.seconds, 60);
        assert_eq!(phase.rate, 50.0);
    }
    assert!(phases.iter().all(|phase| phase.name != "burst"));
    Ok(())
}

// --- the dense-cadence scenario (#271 criterion 6) -----------------------

#[test]
fn the_gap_pattern_repeats_cyclically_and_reserves_a_measurement_tail() -> Result<()> {
    use qbit_prism_load::cadence;
    let gaps = cadence::parse_gaps(cadence::DEFAULT_GAPS)?;
    assert_eq!(gaps, vec![9.0, 19.0, 9.0, 18.0, 20.0]);
    let offsets = cadence::landing_offsets(&gaps, 240.0);
    assert!(
        offsets.len() >= cadence::MIN_LANDINGS,
        "a 240 s phase must hold at least {} landings, held {}",
        cadence::MIN_LANDINGS,
        offsets.len()
    );
    assert_eq!(offsets[0], cadence::LEAD_IN_SECONDS);
    // The pattern repeats cyclically, so the differences are the gaps again.
    let deltas: Vec<f64> = offsets.windows(2).map(|pair| pair[1] - pair[0]).collect();
    for (index, delta) in deltas.iter().enumerate() {
        assert_eq!(*delta, gaps[index % gaps.len()], "gap {index}");
    }
    // #224's shape: 9 s single gaps, and 18-20 s pairs.
    assert!(deltas.contains(&9.0));
    assert!(deltas.iter().any(|delta| (18.0..=20.0).contains(delta)));
    let last = *offsets.last().expect("at least one landing");
    assert!(
        last + cadence::TAIL_SECONDS <= 240.0,
        "the last landing at {last} s leaves less than the {} s measurement tail",
        cadence::TAIL_SECONDS
    );
    Ok(())
}

#[test]
fn a_gap_pattern_that_cannot_be_measured_is_refused_at_entry() {
    use qbit_prism_load::cadence;
    let refusal = |text: &str| {
        format!(
            "{:#}",
            cadence::parse_gaps(text).expect_err("{text} should be refused")
        )
    };
    assert!(refusal("4").contains("floor"), "{}", refusal("4"));
    assert!(refusal("9,4,9").contains("floor"));
    assert!(refusal("9,,19").contains("empty"));
    assert!(refusal("").contains("empty"));
    assert!(refusal("nine").contains("not a number"));
    assert!(refusal("inf").contains("finite"));
    // The pattern and the phase length are checked against each other: a
    // phase too short for ten landings measures nothing.
    let short = format!(
        "{:#}",
        cadence::validate(cadence::DEFAULT_GAPS, 120).expect_err("120 s is too short")
    );
    assert!(short.contains("at least 10"), "{short}");
    assert!(short.contains("--cadence-seconds"), "{short}");
    cadence::validate(cadence::DEFAULT_GAPS, 240).expect("240 s holds ten landings");
    assert!(cadence::Cadence::parse("dense").expect("dense").is_dense());
    assert!(!cadence::Cadence::parse("none").expect("none").is_dense());
    assert!(cadence::Cadence::parse("fast").is_err());
}

#[test]
fn the_dense_cadence_phase_is_a_side_phase_after_slow_database() -> Result<()> {
    use clap::Parser;
    let plain = qbit_prism_load::cli::Args::parse_from(["qbit-prism-load"]);
    plain.validate()?;
    let before = qbit_prism_load::cli::phases(&plain)?;
    assert!(
        before
            .iter()
            .all(|phase| phase.name != qbit_prism_load::cadence::PHASE),
        "a run that did not ask for a cadence must not grow a phase"
    );

    let dense = qbit_prism_load::cli::Args::parse_from([
        "qbit-prism-load",
        "--cadence",
        "dense",
        "--rate",
        "50",
    ]);
    dense.validate()?;
    let phases = qbit_prism_load::cli::phases(&dense)?;
    let position = phases
        .iter()
        .position(|phase| phase.name == qbit_prism_load::cadence::PHASE)
        .expect("the dense phase is planned");
    let slow = phases
        .iter()
        .position(|phase| phase.name == "slow_database")
        .expect("slow_database is planned");
    assert!(position > slow, "the dense phase runs after slow_database");
    let phase = &phases[position];
    assert!(phase.dense_cadence);
    assert!(!phase.in_artifact, "it is a side phase");
    assert_eq!(phase.database_delay_ms, 0, "no proxy delay");
    assert_eq!(phase.seconds, 240);
    assert_eq!(phase.rate, 50.0, "it defaults to the steady-state rate");
    // The artifact's phases are untouched by the new phase (EP-COMPAT).
    let artifact_before: Vec<&String> = before
        .iter()
        .filter(|phase| phase.in_artifact)
        .map(|phase| &phase.name)
        .collect();
    let artifact_after: Vec<&String> = phases
        .iter()
        .filter(|phase| phase.in_artifact)
        .map(|phase| &phase.name)
        .collect();
    assert_eq!(artifact_before, artifact_after);

    let too_short = qbit_prism_load::cli::Args::parse_from([
        "qbit-prism-load",
        "--cadence",
        "dense",
        "--cadence-seconds",
        "120",
    ]);
    assert!(
        too_short.validate().is_err(),
        "a phase too short for ten landings is refused at entry"
    );
    Ok(())
}

// --- synthetic attribution -----------------------------------------------

const HASH_ZERO: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HASH_ONE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn at(base: std::time::Instant, millis: u64) -> std::time::Instant {
    base + std::time::Duration::from_millis(millis)
}

fn dense_submit(
    hash: &str,
    session: usize,
    frontend: usize,
    sent: std::time::Instant,
    responded: std::time::Instant,
    outcome: client::Outcome,
    scheduled_block: bool,
) -> client::SubmitRecord {
    client::SubmitRecord {
        share_id: format!("pload1abc.s{session:05}:{hash}"),
        session,
        frontend,
        phase: qbit_prism_load::cadence::PHASE.to_owned(),
        job_id: format!("job-{session}"),
        sent,
        responded: Some(responded),
        latency_millis: Some(responded.saturating_duration_since(sent).as_secs_f64() * 1000.0),
        outcome,
        scheduled_block,
        reoffer: false,
        header_hex: String::new(),
        extranonce2_hex: String::new(),
        ntime_hex: String::new(),
        nonce_hex: String::new(),
    }
}

fn pending(message: &str) -> client::Outcome {
    client::Outcome::Rejected(Rejection {
        code: 21,
        reason_id: Some("stale-job".into()),
        message: message.to_owned(),
    })
}

fn node_submission(hash: &str, height: u64, accepted: bool) -> node::SubmissionRecord {
    node::SubmissionRecord {
        block_hash: hash.to_owned(),
        parent: HASH_ZERO.to_owned(),
        height,
        accepted,
        rejection: (!accepted).then(|| node::PARENT_MISMATCH.to_owned()),
        received_at: chrono::Utc::now(),
        block_bytes: 494,
    }
}

fn pool_tip(hash: &str, height: u64, monotonic: std::time::Instant) -> node::TipChange {
    node::TipChange {
        hash: hash.to_owned(),
        height,
        origin: node::TipOrigin::Pool,
        monotonic,
        wall: chrono::Utc::now(),
    }
}

fn bump(
    revision: i64,
    previous: Option<i64>,
    monotonic: std::time::Instant,
) -> qbit_prism_load::cadence::RevisionSample {
    qbit_prism_load::cadence::RevisionSample {
        revision,
        server_timestamp: chrono::Utc::now(),
        monotonic,
        previous_revision: previous,
    }
}

fn health(index: usize) -> qbit_prism_load::cadence::FrontendHealth {
    qbit_prism_load::cadence::FrontendHealth {
        index,
        instance_id: format!("load-fe-{index}"),
        restarts_before: 0,
        restarts_after: 0,
        exited: None,
    }
}

#[test]
fn rejections_and_bumps_are_attributed_to_the_landing_they_follow() {
    use qbit_prism_load::cadence;
    let base = std::time::Instant::now();
    let landings = vec![
        cadence::Landing {
            index: 0,
            scheduled_offset_seconds: 5.0,
            requested_monotonic: at(base, 5_000),
            requested_wall: chrono::Utc::now(),
            session: 0,
            frontend: 0,
        },
        cadence::Landing {
            index: 1,
            scheduled_offset_seconds: 14.0,
            requested_monotonic: at(base, 14_000),
            requested_wall: chrono::Utc::now(),
            session: 1,
            frontend: 0,
        },
        cadence::Landing {
            index: 2,
            scheduled_offset_seconds: 23.0,
            requested_monotonic: at(base, 23_000),
            requested_wall: chrono::Utc::now(),
            session: 0,
            frontend: 0,
        },
    ];
    let submits = vec![
        // The two landings that landed.
        dense_submit(
            HASH_ZERO,
            0,
            0,
            at(base, 5_100),
            at(base, 5_200),
            client::Outcome::Accepted,
            true,
        ),
        dense_submit(
            HASH_ONE,
            1,
            0,
            at(base, 14_100),
            at(base, 14_200),
            client::Outcome::Accepted,
            true,
        ),
        // One rebuild-pending rejection before any landing: unattributed.
        dense_submit(
            &"1".repeat(64),
            2,
            0,
            at(base, 2_900),
            at(base, 3_000),
            pending(classify::NEW_TIP_WORK_PENDING),
            false,
        ),
        // Landing 0's window: two tip-pending, then one payout-pending.
        dense_submit(
            &"2".repeat(64),
            2,
            0,
            at(base, 7_100),
            at(base, 7_200),
            pending(classify::NEW_TIP_WORK_PENDING),
            false,
        ),
        dense_submit(
            &"3".repeat(64),
            3,
            0,
            at(base, 7_900),
            at(base, 8_000),
            pending(classify::NEW_TIP_WORK_PENDING),
            false,
        ),
        dense_submit(
            &"4".repeat(64),
            2,
            0,
            at(base, 8_900),
            at(base, 9_000),
            pending(classify::NEW_PAYOUT_WORK_PENDING),
            false,
        ),
        // Landing 1's window.
        dense_submit(
            &"5".repeat(64),
            3,
            0,
            at(base, 16_400),
            at(base, 16_500),
            pending(classify::NEW_TIP_WORK_PENDING),
            false,
        ),
    ];
    let node_submissions = vec![
        node_submission(HASH_ZERO, 104, true),
        node_submission(HASH_ONE, 105, true),
    ];
    let tip_changes = vec![
        pool_tip(HASH_ZERO, 104, at(base, 7_000)),
        pool_tip(HASH_ONE, 105, at(base, 16_000)),
    ];
    let revisions = cadence::RevisionSeries {
        interval_ms: 25,
        samples: 9_000,
        errors: 0,
        first_error: None,
        baseline: Some(bump(4, None, base)),
        changes: vec![
            // Before any landing: unattributed, cause unknown.
            bump(5, Some(4), at(base, 2_000)),
            // Landing 0 causes two: the chainwork bump at the rebuild, and
            // the candidate confirmation.
            bump(6, Some(5), at(base, 7_500)),
            bump(7, Some(6), at(base, 9_000)),
            // Landing 1 causes one.
            bump(8, Some(7), at(base, 17_000)),
        ],
    };
    let notifies = vec![
        client::NotifySighting {
            session: 2,
            frontend: 0,
            job_id: "job-a".into(),
            tip: HASH_ZERO.to_owned(),
            clean_jobs: true,
            at: at(base, 10_000),
        },
        client::NotifySighting {
            session: 3,
            frontend: 0,
            job_id: "job-b".into(),
            tip: HASH_ZERO.to_owned(),
            clean_jobs: true,
            at: at(base, 10_500),
        },
        client::NotifySighting {
            session: 2,
            frontend: 0,
            job_id: "job-c".into(),
            tip: HASH_ONE.to_owned(),
            clean_jobs: true,
            at: at(base, 18_000),
        },
    ];
    let tips = vec![
        client::TipSighting {
            session: 2,
            frontend: 0,
            tip: HASH_ZERO.to_owned(),
            at: at(base, 7_300),
        },
        client::TipSighting {
            session: 3,
            frontend: 0,
            tip: HASH_ZERO.to_owned(),
            at: at(base, 7_400),
        },
    ];
    let failures = vec![(
        0usize,
        "scheduled block: no block solution found under job job-0".to_owned(),
        at(base, 23_100),
    )];
    let frontends = vec![health(0)];
    let session_frontend = vec![0usize, 0, 0, 0];
    let gaps = vec![9.0, 19.0];
    let offsets = vec![5.0, 14.0, 23.0];
    // The unattributed rejection's share, and only it, is in PostgreSQL. A
    // census computed over the attributed subset would never look it up and
    // would report a clean shares_found_in_postgres: 0 -- which is the one
    // case the cross-check exists for.
    let committed: std::collections::BTreeSet<String> =
        [format!("pload1abc.s{:05}:{}", 2, "1".repeat(64))]
            .into_iter()
            .collect();
    let document = cadence::build(&cadence::ReportInputs {
        cadence: cadence::Cadence::Dense,
        gaps: &gaps,
        offsets: &offsets,
        phase_seconds: 240,
        phase_rate: 50.0,
        phase_started: base,
        phase_started_wall: chrono::Utc::now(),
        phase_ended: at(base, 240_000),
        phase_duration_millis: 240_000,
        landing_budget: 3,
        slots_over_budget: 0,
        landings: &landings,
        revisions: Some(&revisions),
        submits: &submits,
        notifies: &notifies,
        tips: &tips,
        node_submissions: &node_submissions,
        tip_changes: &tip_changes,
        session_frontend: &session_frontend,
        frontends: &frontends,
        failures: &failures,
        committed: &committed,
        aborted: None,
    });

    assert_eq!(document["ran"], json!(true));
    assert_eq!(document["landings"], json!(2), "two landings landed");
    assert_eq!(document["landing_attempts"], json!(3));
    assert_eq!(document["landing_outcomes"]["landed"], json!(2));
    assert_eq!(document["landing_outcomes"]["never_produced"], json!(1));
    // definitions.bump is "an observed change of payout_revision", so the
    // top-level key is every change the sampler saw; the split is one key
    // away. Publishing the attributed subset under the bare word made the
    // headline number mean something other than its own definition.
    assert_eq!(
        document["bumps"],
        json!(4),
        "every observed change of payout_revision"
    );
    assert_eq!(
        document["bumps"], document["revision_sampler"]["changes_observed"],
        "bumps is changes_observed, exactly as definitions.bump says"
    );
    assert_eq!(document["bump_attribution"]["observed"], json!(4));
    assert_eq!(
        document["bump_attribution"]["attributed"],
        json!(3),
        "three of the four followed a landing"
    );
    assert_eq!(
        document["bump_attribution"]["unattributed"],
        json!(1),
        "the bump before the first landing is unattributed, never dropped"
    );
    assert_eq!(
        document["bump_attribution"]["attributed"]
            .as_u64()
            .expect("attributed")
            + document["bump_attribution"]["unattributed"]
                .as_u64()
                .expect("unattributed"),
        document["bumps"].as_u64().expect("bumps"),
        "the split reconciles with the headline"
    );
    assert!(document["definitions"]["bump"]
        .as_str()
        .expect("the definition")
        .contains("The top-level bumps key is this count"));
    let bumps = document["bump_records"].as_array().expect("bump records");
    assert_eq!(bumps.len(), 4);
    assert_eq!(bumps[0]["attributed_to_landing"], Value::Null);
    assert!(bumps[0]["cause"]
        .as_str()
        .expect("cause")
        .contains("unknown"));
    assert_eq!(bumps[1]["attributed_to_landing"], json!(0));
    assert_eq!(bumps[1]["revision_delta"], json!(1));
    assert_eq!(bumps[2]["attributed_to_landing"], json!(0));
    assert_eq!(bumps[3]["attributed_to_landing"], json!(1));

    assert_eq!(
        document["rejection_attribution"]["rebuild_pending_rejections_in_phase"],
        json!(5)
    );
    assert_eq!(document["rejection_attribution"]["attributed"], json!(4));
    assert_eq!(document["rejection_attribution"]["unattributed"], json!(1));

    let landing = &document["landing_records"][0];
    assert_eq!(landing["outcome"], json!("landed"));
    assert_eq!(landing["block_hash"], json!(HASH_ZERO));
    assert_eq!(landing["node"]["accepted"], json!(true));
    assert_eq!(landing["bumps"], json!(2));
    assert_eq!(landing["bump_revisions"], json!([6, 7]));
    let table = &landing["frontends"][0];
    assert_eq!(table["frontend"], json!(0));
    assert_eq!(table["tip_pending_window"]["count"], json!(2));
    assert_eq!(
        table["tip_pending_window"]["first_millis_after_landing"],
        json!(200.0)
    );
    assert_eq!(
        table["tip_pending_window"]["duration_millis"],
        json!(800.0),
        "7.2 s to 8.0 s after the tip change"
    );
    assert_eq!(table["payout_pending_window"]["count"], json!(1));
    assert_eq!(
        table["payout_pending_window"]["duration_millis"],
        json!(0.0),
        "one rejection is a zero-length window, not a missing one"
    );
    assert_eq!(
        table["combined_rebuild_pending_window"]["count"],
        json!(3),
        "both messages, in one window"
    );
    assert_eq!(
        table["combined_rebuild_pending_window"]["duration_millis"],
        json!(1_800.0)
    );
    assert_eq!(
        table["reference_bump"]["revision"],
        json!(7),
        "the last bump of the landing is the revision a frontend must reach"
    );
    assert_eq!(
        table["rejected_before_new_revision_work"],
        json!(3),
        "all three rejections came before the first clean_jobs job"
    );
    assert_eq!(table["lost_valid_shares"], json!(3));
    assert_eq!(table["lost_valid_shares_found_in_postgres"], json!(0));
    assert_eq!(table["incomplete"], json!(false));
    assert_eq!(
        table["time_to_new_tip_work_millis"]["max"],
        json!(400.0),
        "measured from the node's tip stamp"
    );
    assert_eq!(
        table["time_to_new_revision_work_millis"]["max"],
        json!(1_500.0),
        "measured from the reference bump at 9.0 s"
    );
    assert!(table["new_revision_work_approximation"]
        .as_str()
        .expect("the approximation is labelled")
        .contains("clean_jobs"));

    let last = &document["landing_records"][2];
    assert_eq!(last["outcome"], json!("never_produced"));
    assert!(last["never_produced_error"]
        .as_str()
        .expect("the failure is reported, not hidden")
        .contains("no block solution"));
    assert_eq!(last["frontends"], json!([]));

    // Lost work is counted over every rebuild-pending rejection in the phase,
    // attributed or not: the fifth share is as much discarded miner work as
    // the other four.
    assert_eq!(document["lost_valid_work"]["shares"], json!(5));
    assert_eq!(
        document["lost_valid_work"]["shares_attributed"],
        json!(4),
        "the four a landing's span owns are still reported apart"
    );
    assert_eq!(
        document["lost_valid_work"]["shares_unattributed"],
        json!(1),
        "the rejection before the first landing stays in the census"
    );
    assert_eq!(
        document["lost_valid_work"]["shares_attributed"]
            .as_u64()
            .expect("attributed")
            + document["lost_valid_work"]["shares_unattributed"]
                .as_u64()
                .expect("unattributed"),
        document["lost_valid_work"]["shares"]
            .as_u64()
            .expect("shares"),
        "the split reconciles with the total"
    );
    assert_eq!(
        document["lost_valid_work"]["shares_attributed"],
        document["rejection_attribution"]["attributed"],
        "the attributed subset is exactly the rejections a span owns"
    );
    assert_eq!(
        document["lost_valid_work"]["shares_found_in_postgres"],
        json!(1),
        "the unattributed lost share is checked against PostgreSQL and found"
    );
    assert_eq!(
        document["lost_valid_work"]["shares_found_in_postgres_sample"][0],
        json!(format!("pload1abc.s{:05}:{}", 2, "1".repeat(64))),
        "the share is named, not just counted"
    );
    // Every distribution names its own unit. The five count distributions are
    // counts of shares, not milliseconds.
    for summary in [
        &document["summaries"]["overall"],
        &document["summaries"]["per_frontend"][0],
    ] {
        for key in [
            "tip_pending_rejections_per_landing",
            "payout_pending_rejections_per_landing",
            "combined_rebuild_pending_rejections_per_landing",
            "rejected_before_new_revision_work_per_landing",
            "lost_valid_shares_per_landing",
        ] {
            assert_eq!(summary[key]["unit"], json!("count"), "{key}");
            assert!(
                !summary[key]["clock"]
                    .as_str()
                    .expect("a clock")
                    .contains("not milliseconds"),
                "{key}: the clock names the clock, it does not apologise for the unit"
            );
        }
        for key in [
            "tip_pending_window_duration_millis",
            "payout_pending_window_duration_millis",
            "combined_rebuild_pending_window_duration_millis",
            "time_to_new_tip_work_max_millis",
            "time_to_new_revision_work_max_millis",
        ] {
            assert_eq!(summary[key]["unit"], json!("milliseconds"), "{key}");
        }
    }
    assert_eq!(
        document["landing_records"][0]["frontends"][0]["time_to_new_tip_work_millis"]["unit"],
        json!("milliseconds")
    );

    // The approximation label travels with the summarised numbers too: those
    // are the ones a reader quotes.
    for summary in [
        &document["summaries"]["overall"],
        &document["summaries"]["per_frontend"][0],
    ] {
        assert!(summary["time_to_new_revision_work_max_millis"]["max"].is_number());
        assert!(
            summary["new_revision_work_approximation"]
                .as_str()
                .expect("the summaries label the approximation")
                .contains("clean_jobs"),
            "summaries.* must carry the same label the landing tables carry"
        );
    }

    assert!(document["proposed_budget_for_issue_291"]["window_p99_millis"].is_number());
    assert!(document["definitions"]["combined_rebuild_pending_window"].is_string());
}

#[test]
fn a_landing_with_a_window_is_counted_as_having_one() {
    // A span -- and therefore a window -- is granted on the landing's own pool
    // tip change, whatever the outcome. Here the tip moved but the node's
    // submission record is missing, so the outcome is
    // accepted_without_node_submission and the `landed` tally is 0, while the
    // section really does carry a per-frontend window table and a summary.
    // Keying landings and windows_available on the tally let the two disagree.
    use qbit_prism_load::cadence;
    let base = std::time::Instant::now();
    let gaps = cadence::parse_gaps(cadence::DEFAULT_GAPS).expect("the default gaps parse");
    let offsets = cadence::landing_offsets(&gaps, 240.0);
    let landings = vec![cadence::Landing {
        index: 0,
        scheduled_offset_seconds: 5.0,
        requested_monotonic: at(base, 5_000),
        requested_wall: chrono::Utc::now(),
        session: 0,
        frontend: 0,
    }];
    let submits = vec![
        dense_submit(
            HASH_ZERO,
            0,
            0,
            at(base, 5_100),
            at(base, 5_200),
            client::Outcome::Accepted,
            true,
        ),
        dense_submit(
            &"7".repeat(64),
            0,
            0,
            at(base, 7_100),
            at(base, 7_200),
            pending(classify::NEW_TIP_WORK_PENDING),
            false,
        ),
    ];
    // The tip moved to the landing's block; the node kept no record of it.
    let tip_changes = vec![pool_tip(HASH_ZERO, 104, at(base, 7_000))];
    let revisions = cadence::RevisionSeries {
        interval_ms: 25,
        samples: 9_000,
        errors: 0,
        first_error: None,
        baseline: Some(bump(4, None, base)),
        changes: Vec::new(),
    };
    let frontends = vec![health(0)];
    let session_frontend = vec![0usize];
    let committed = std::collections::BTreeSet::new();
    let document = cadence::build(&cadence::ReportInputs {
        cadence: cadence::Cadence::Dense,
        gaps: &gaps,
        offsets: &offsets,
        phase_seconds: 240,
        phase_rate: 50.0,
        phase_started: base,
        phase_started_wall: chrono::Utc::now(),
        phase_ended: at(base, 240_000),
        phase_duration_millis: 240_000,
        landing_budget: 12,
        slots_over_budget: 0,
        landings: &landings,
        revisions: Some(&revisions),
        submits: &submits,
        notifies: &[],
        tips: &[],
        node_submissions: &[],
        tip_changes: &tip_changes,
        session_frontend: &session_frontend,
        frontends: &frontends,
        failures: &[],
        committed: &committed,
        aborted: None,
    });

    assert_eq!(
        document["landing_outcomes"]["accepted_without_node_submission"],
        json!(1)
    );
    assert_eq!(
        document["landing_outcomes"]["landed"],
        json!(0),
        "the outcome tally is unchanged and still reported"
    );
    // The window really is there, so the section says so.
    assert_eq!(
        document["landing_records"][0]["frontends"][0]["tip_pending_window"]["count"],
        json!(1)
    );
    assert_eq!(
        document["landings"],
        json!(1),
        "a landing with a window is counted as having one"
    );
    assert_eq!(document["windows_available"], json!(true));
    assert_eq!(
        document["reason"],
        Value::Null,
        "there is a window, so there is no no-landing reason beside it"
    );
    assert!(
        document["proposed_budget_for_issue_291"]["rejections_per_landing_per_frontend_p99"]
            .is_number(),
        "the budget is built from this landing's window, so the count must agree"
    );
    assert!(document["definitions"]["landings"]
        .as_str()
        .expect("the definition")
        .contains("attribution span"));
}

#[test]
fn a_sampler_that_failed_after_its_baseline_reports_itself_blind() {
    use qbit_prism_load::cadence::RevisionSeries;
    let base = std::time::Instant::now();
    let series = |samples: u64, errors: u64, changes: Vec<_>, baseline: bool| RevisionSeries {
        interval_ms: 25,
        samples,
        errors,
        first_error: (errors > 0).then(|| "pool timed out".to_owned()),
        baseline: baseline.then(|| bump(4, None, base)),
        changes,
    };

    // Never read anything: blind, as before.
    let never = series(0, 6_000, Vec::new(), false);
    assert!(never.blind());
    assert!(never
        .blind_reason()
        .expect("a reason")
        .contains("no reading"));
    assert_eq!(never.coverage(), Some(0.0));

    // Read a baseline, then failed for the rest of the phase. It observed no
    // change and also observed almost nothing; blind: false with
    // changes_observed: 0 is the misreading the flag exists to prevent.
    let died = series(1, 5_999, Vec::new(), true);
    assert!(died.blind(), "partial blindness is blindness");
    assert!(died
        .blind_reason()
        .expect("a reason")
        .contains("then failed"));
    let coverage = died.coverage().expect("coverage");
    assert!(coverage > 0.0 && coverage < 0.001, "{coverage}");

    // Errors beside observed changes are not blindness: the sampler did see
    // the revision move, and coverage says how much it watched.
    let lossy = series(3_000, 3_000, vec![bump(5, Some(4), at(base, 2_000))], true);
    assert!(!lossy.blind());
    assert_eq!(lossy.blind_reason(), None);
    assert_eq!(lossy.coverage(), Some(0.5));

    // A clean sampler.
    let clean = series(9_000, 0, vec![bump(5, Some(4), at(base, 2_000))], true);
    assert!(!clean.blind());
    assert_eq!(clean.coverage(), Some(1.0));

    // A sampler that never ticked has unknown coverage, which is not 0.
    let untouched = RevisionSeries::default();
    assert_eq!(untouched.coverage(), None);
    assert!(untouched.blind(), "no baseline is still blind");
}

#[test]
fn a_run_with_no_landing_reports_zero_landings_zero_bumps_and_no_window() {
    use qbit_prism_load::cadence;
    let base = std::time::Instant::now();
    let gaps = cadence::parse_gaps(cadence::DEFAULT_GAPS).expect("the default gaps parse");
    let offsets = cadence::landing_offsets(&gaps, 240.0);
    let revisions = cadence::RevisionSeries {
        interval_ms: 25,
        samples: 9_000,
        errors: 0,
        first_error: None,
        baseline: Some(bump(4, None, base)),
        changes: Vec::new(),
    };
    let frontends = vec![health(0), health(1)];
    let session_frontend = vec![0usize, 1];
    let committed = std::collections::BTreeSet::new();
    let inputs = cadence::ReportInputs {
        cadence: cadence::Cadence::Dense,
        gaps: &gaps,
        offsets: &offsets,
        phase_seconds: 240,
        phase_rate: 50.0,
        phase_started: base,
        phase_started_wall: chrono::Utc::now(),
        phase_ended: at(base, 240_000),
        phase_duration_millis: 240_000,
        landing_budget: 0,
        slots_over_budget: offsets.len(),
        landings: &[],
        revisions: Some(&revisions),
        submits: &[],
        notifies: &[],
        tips: &[],
        node_submissions: &[],
        tip_changes: &[],
        session_frontend: &session_frontend,
        frontends: &frontends,
        failures: &[],
        committed: &committed,
        aborted: None,
    };
    let document = cadence::build(&inputs);
    assert_eq!(document["ran"], json!(true), "the phase itself did run");
    assert_eq!(document["landings"], json!(0));
    assert_eq!(document["bumps"], json!(0));
    assert_eq!(document["windows_available"], json!(false));
    assert_eq!(document["landing_records"], json!([]));
    assert_eq!(document["landing_attempts"], json!(0));
    let reason = document["reason"].as_str().expect("a reason, not silence");
    assert!(reason.contains("--scheduled-blocks"), "{reason}");
    assert_eq!(
        document["summaries"]["overall"]["combined_rebuild_pending_window_duration_millis"]["p99"],
        Value::Null
    );
    assert_eq!(
        document["proposed_budget_for_issue_291"]["window_p99_millis"],
        Value::Null,
        "a budget is unknown, never zero, when nothing landed"
    );
    assert_eq!(
        document["proposed_budget_for_issue_291"]["recommended_soak_budget_millis"],
        Value::Null
    );
    assert_eq!(document["lost_valid_work"]["shares"], json!(0));

    // Every attempt failing is a different reason, and it names the tally.
    let landings = vec![cadence::Landing {
        index: 0,
        scheduled_offset_seconds: 5.0,
        requested_monotonic: at(base, 5_000),
        requested_wall: chrono::Utc::now(),
        session: 0,
        frontend: 0,
    }];
    let failed = cadence::build(&cadence::ReportInputs {
        landing_budget: 12,
        slots_over_budget: 0,
        landings: &landings,
        ..inputs
    });
    assert_eq!(failed["landings"], json!(0));
    assert_eq!(failed["bumps"], json!(0));
    let reason = failed["reason"].as_str().expect("a reason");
    assert!(reason.contains("never_produced=1"), "{reason}");

    // A run that never asked for the scenario says so.
    let none = cadence::build(&cadence::ReportInputs {
        cadence: cadence::Cadence::None,
        ..cadence::ReportInputs {
            landing_budget: 0,
            slots_over_budget: 0,
            landings: &[],
            ..inputs
        }
    });
    assert_eq!(none["ran"], json!(false));
    assert!(none["reason"]
        .as_str()
        .expect("a reason")
        .contains("--cadence dense"));

    // Nothing landed, but the revision moved anyway -- an operator touching
    // the cluster, a migration, a second harness. bumps is every observed
    // change, so it says 2 rather than hiding them behind an attribution the
    // word "bump" never promised.
    let moved = cadence::RevisionSeries {
        interval_ms: 25,
        samples: 9_000,
        errors: 0,
        first_error: None,
        baseline: Some(bump(4, None, base)),
        changes: vec![
            bump(5, Some(4), at(base, 30_000)),
            bump(6, Some(5), at(base, 60_000)),
        ],
    };
    let drifted = cadence::build(&cadence::ReportInputs {
        revisions: Some(&moved),
        ..inputs
    });
    assert_eq!(drifted["landings"], json!(0));
    assert_eq!(
        drifted["bumps"],
        json!(2),
        "a revision that moved with no landing is still two observed bumps"
    );
    assert_eq!(drifted["bump_attribution"]["attributed"], json!(0));
    assert_eq!(drifted["bump_attribution"]["unattributed"], json!(2));
    assert_eq!(
        drifted["bumps"],
        drifted["revision_sampler"]["changes_observed"]
    );
}

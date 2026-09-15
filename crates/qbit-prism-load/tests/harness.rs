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
    measure,
    node::{self, NodeState},
    profile, proxy, run, window,
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
    // The server retired these in #361, and the v3 validator refuses evidence
    // that names one. So they must be absent from both the frontend environment
    // and the configuration block -- not merely annotated as unread, which is
    // what v2 asked for (#288).
    for key in frontend::RETIRED_CONFIGURATION_KEYS {
        assert!(
            !block.contains_key(*key),
            "{key} is retired and must not reach the evidence"
        );
        assert!(
            !env.contains_key(*key),
            "{key} is retired and must not be set on a frontend"
        );
    }
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

/// SQLx reads a URL's query keys percent-decoded (`url.query_pairs()`), so
/// `pass%77ord=secret` sets the password. The redaction compared the encoded
/// spelling, so that value survived into both reports. The key is compared
/// as SQLx reads it and written back as it came.
#[test]
fn a_percent_encoded_password_key_is_redacted_as_sqlx_reads_it() {
    let password = "hunter2-Sup3r_Secret";
    for (url, expected) in [
        (
            format!("postgresql://db.example/qbit?pass%77ord={password}&sslmode=require"),
            "postgresql://db.example/qbit?pass%77ord=<redacted>&sslmode=require",
        ),
        (
            format!("postgresql://db.example/qbit?%70%61%73%73%77%6F%72%64={password}"),
            "postgresql://db.example/qbit?%70%61%73%73%77%6F%72%64=<redacted>",
        ),
        (
            format!("postgresql://alex:{password}@db.example/qbit?PASS%57ORD={password}#f"),
            "postgresql://alex:<redacted>@db.example/qbit?PASS%57ORD=<redacted>#f",
        ),
    ] {
        let redacted = frontend::redact_url_secrets(&url);
        assert!(
            !redacted.contains(password),
            "{url} still carries the password: {redacted}"
        );
        assert_eq!(redacted, expected);
    }
    // SQLx's decoder takes `+` for a space and keeps a malformed escape as
    // written, so neither of these is the password key, and neither value
    // is a secret the redaction may invent.
    assert_eq!(
        frontend::redact_url_secrets("postgresql://h/d?pass+word=x&pass%zzword=y"),
        "postgresql://h/d?pass+word=x&pass%zzword=y"
    );
    assert_eq!(frontend::percent_decode("pass%77ord"), "password");
    assert_eq!(frontend::percent_decode("a+b%2"), "a b%2");
}

/// The free-text sinks -- the failure report's error, the stderr print, the
/// `pg_settings` values in the profile -- redact whatever their sources
/// forgot to: every URL-shaped token, and every libpq `password=` value,
/// bare or quoted. Text with no secret is untouched, and redacting twice
/// changes nothing.
#[test]
fn free_text_sinks_redact_urls_and_libpq_passwords() {
    let password = "hunter2-Sup3r_Secret";
    let cases = [
        (
            format!("timing a round trip: connect: postgresql://alex:{password}@db:5432/q?a=1 x"),
            "timing a round trip: connect: postgresql://alex:<redacted>@db:5432/q?a=1 x",
        ),
        (
            format!("one postgresql://u:{password}@h/d\nand two https://s:{password}@api/v#f"),
            "one postgresql://u:<redacted>@h/d\nand two https://s:<redacted>@api/v#f",
        ),
        (
            format!("host=127.0.0.1 port=5432 user=rep password={password} application_name=s"),
            "host=127.0.0.1 port=5432 user=rep password=<redacted> application_name=s",
        ),
        (
            format!("host=h password='{password} with space' sslpassword='a\\'b' dbname=d"),
            "host=h password=<redacted> sslpassword=<redacted> dbname=d",
        ),
        (
            format!("postgresql://h/d?password={password}&sslmode=require"),
            "postgresql://h/d?password=<redacted>&sslmode=require",
        ),
    ];
    for (text, expected) in cases {
        let redacted = frontend::redact_secrets_in_text(&text);
        assert!(
            !redacted.contains(password),
            "{text:?} still carries the password: {redacted:?}"
        );
        assert_eq!(redacted, expected);
        assert_eq!(
            frontend::redact_secrets_in_text(&redacted),
            expected,
            "idempotent"
        );
    }
    for untouched in [
        "initialising the schema: connection refused",
        "fsync=on full_page_writes=on synchronous_commit=on",
        "test ! -f /archive/%f && cp %p /archive/%f",
        "",
        "a = b",
    ] {
        assert_eq!(frontend::redact_secrets_in_text(untouched), untouched);
    }
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

/// `--allow-debug-server` admits a `target/debug` binary, the dep-info beside
/// it still establishes the revision, and the artifact came out as
/// `qualification`: evidence the harness's own refusal text says measures
/// nothing. A non-release profile forces `example`, the way a dirty tree and
/// an unverified revision already do; the unknown profile is not a release
/// build either.
#[test]
fn a_debug_or_unknown_server_profile_forces_example_evidence() {
    use qbit_prism_load::frontend::BuildProfile;
    assert_eq!(
        run::artifact_kind(false, false, true, BuildProfile::Release),
        artifact::ARTIFACT_QUALIFICATION,
        "every premise established is qualification"
    );
    for profile in [BuildProfile::Debug, BuildProfile::Unknown] {
        assert_eq!(
            run::artifact_kind(false, false, true, profile),
            artifact::ARTIFACT_EXAMPLE,
            "a {profile:?} server with a clean tree and an established revision is not \
             qualification evidence"
        );
    }
    // The other overrides still force example on their own.
    assert_eq!(
        run::artifact_kind(true, false, true, BuildProfile::Release),
        artifact::ARTIFACT_EXAMPLE
    );
    assert_eq!(
        run::artifact_kind(false, true, true, BuildProfile::Release),
        artifact::ARTIFACT_EXAMPLE
    );
    assert_eq!(
        run::artifact_kind(false, false, false, BuildProfile::Release),
        artifact::ARTIFACT_EXAMPLE
    );
}

// --- a minimal Stratum server -------------------------------------------

/// The smallest Stratum server a session can complete a handshake with:
/// subscribe, configure, authorize, one job, and `true` to every submit.
/// `release_authorize` gates the authorize reply, which keeps a session
/// inside its handshake for as long as a test needs; `release_submits`
/// gates every submit reply the same way, which keeps a submit outstanding
/// for as long as a test needs.
struct FakeStratum {
    address: String,
    release_authorize: tokio::sync::watch::Sender<bool>,
    release_submits: tokio::sync::watch::Sender<bool>,
    submits: Arc<std::sync::atomic::AtomicUsize>,
    /// How many of the next accepted connections are closed at once, before
    /// a line is read: a frontend that is up but not yet serving.
    drop_connections: Arc<std::sync::atomic::AtomicUsize>,
    _task: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy, Default)]
struct StratumOptions {
    hold_authorize: bool,
    hold_submits: bool,
    /// A `mining.set_difficulty` to push after authorize, before the job.
    advertised_difficulty: Option<f64>,
}

async fn fake_stratum(hold_authorize: bool) -> FakeStratum {
    fake_stratum_with(StratumOptions {
        hold_authorize,
        ..Default::default()
    })
    .await
}

async fn fake_stratum_with(options: StratumOptions) -> FakeStratum {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (release_authorize, release) = tokio::sync::watch::channel(!options.hold_authorize);
    let (release_submits, submit_release) = tokio::sync::watch::channel(!options.hold_submits);
    let submits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = submits.clone();
    let drop_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let to_drop = drop_connections.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            if to_drop
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |left| left.checked_sub(1),
                )
                .is_ok()
            {
                drop(socket);
                continue;
            }
            tokio::spawn(serve_stratum(
                socket,
                release.clone(),
                submit_release.clone(),
                counter.clone(),
                options.advertised_difficulty,
            ));
        }
    });
    FakeStratum {
        address,
        release_authorize,
        release_submits,
        submits,
        drop_connections,
        _task: task,
    }
}

async fn serve_stratum(
    socket: tokio::net::TcpStream,
    mut release: tokio::sync::watch::Receiver<bool>,
    mut submit_release: tokio::sync::watch::Receiver<bool>,
    submits: Arc<std::sync::atomic::AtomicUsize>,
    advertised_difficulty: Option<f64>,
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
                while !*submit_release.borrow() {
                    if submit_release.changed().await.is_err() {
                        return;
                    }
                }
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
            if let Some(difficulty) = advertised_difficulty {
                let set = json!({"id": null, "method": "mining.set_difficulty",
                                 "params": [difficulty]});
                let mut bytes = serde_json::to_vec(&set).unwrap();
                bytes.push(b'\n');
                if write.write_all(&bytes).await.is_err() {
                    return;
                }
            }
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
        // What the run derives from the default 15 s commit timeout.
        quiesce_limit: run::drain_limit(15.0),
    }
}

/// Wait for the fake server to have `count` submits in hand.
async fn submits_received(server: &FakeStratum, count: usize) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while server.submits.load(std::sync::atomic::Ordering::SeqCst) < count {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the server received {} of {count} submits",
            server.submits.load(std::sync::atomic::Ordering::SeqCst)
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Drive one session with one submit held on the server, ask it to
/// reconnect, and return its record for that submit with how long the
/// session waited before producing it. `release_after` lets the server
/// answer part-way through the wait.
async fn quiesced_submit(
    quiesce_limit: std::time::Duration,
    release_after: Option<std::time::Duration>,
) -> Result<(client::SubmitRecord, std::time::Duration)> {
    let server = fake_stratum_with(StratumOptions {
        hold_submits: true,
        ..Default::default()
    })
    .await;
    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let shared = Arc::new(client::SessionShared {
        phase: std::sync::RwLock::new("reconnect".to_owned()),
        events,
        record_notifies: std::sync::atomic::AtomicBool::new(false),
    });
    let config = client::SessionConfig {
        quiesce_limit,
        ..session_config(0)
    };
    let handle = client::spawn_session(config, 0, server.address.clone(), shared.clone(), 1);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the session connects within the deadline")
            .expect("the session is still running");
        if matches!(event, client::Event::Connected { .. }) {
            break;
        }
    }
    let phase: Arc<str> = Arc::from("reconnect");
    assert!(handle.try_offer(1, &phase));
    submits_received(&server, 1).await;

    let asked = std::time::Instant::now();
    handle.control.send(client::Control::Reconnect {
        reason: "client-initiated".into(),
        phase: phase.clone(),
    })?;
    if let Some(after) = release_after {
        let release = server.release_submits.clone();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let _ = release.send(true);
        });
    }
    let record = loop {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the held submit is settled one way or the other within the deadline")
            .expect("the session is still running");
        if let client::Event::Submit(record) = event {
            break *record;
        }
    };
    let waited = asked.elapsed();
    let _ = server.release_submits.send(true);
    let _ = handle.control.send(client::Control::Stop);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.task).await??;
    Ok((record, waited))
}

/// `time_to_reconnect_milliseconds` is what the reconnect phase publishes,
/// and it measured only the attempt that succeeded: the start instant was
/// recreated on every pass through the retry loop, so a frontend unavailable
/// across several attempts reported its final handshake -- milliseconds --
/// for an outage of seconds. The instant is taken once, when the connection
/// goes, and every attempt's record measures from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconnect_across_several_failed_attempts_reports_the_whole_outage() -> Result<()> {
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    let server = fake_stratum(false).await;
    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let shared = Arc::new(client::SessionShared {
        phase: std::sync::RwLock::new("reconnect".to_owned()),
        events,
        record_notifies: std::sync::atomic::AtomicBool::new(false),
    });
    let handle = client::spawn_session(session_config(0), 0, server.address.clone(), shared, 1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the session connects within the deadline")
            .expect("the session is still running");
        if matches!(event, client::Event::Connected { .. }) {
            break;
        }
    }
    // The next three connections are accepted and closed before the
    // handshake, so the reconnect fails three times and backs off 250 ms
    // after each failure before the fourth attempt completes.
    let dropped = 3usize;
    server.drop_connections.store(dropped, Ordering::SeqCst);
    let asked = std::time::Instant::now();
    handle.control.send(client::Control::Reconnect {
        reason: "client-initiated".into(),
        phase: Arc::from("reconnect"),
    })?;
    let mut records = Vec::new();
    let completed = loop {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the reconnect completes within the deadline")
            .expect("the session is still running");
        if let client::Event::Reconnect(record) = event {
            let done = record.completed;
            records.push(record);
            if done {
                break records.last().cloned().expect("just pushed");
            }
        }
    };
    let outage = asked.elapsed();
    let _ = handle.control.send(client::Control::Stop);
    tokio::time::timeout(Duration::from_secs(5), handle.task).await??;

    let failed: Vec<&client::ReconnectRecord> =
        records.iter().filter(|record| !record.completed).collect();
    assert_eq!(failed.len(), dropped, "{records:?}");
    for record in &failed {
        assert_eq!(record.phase, "reconnect");
        assert_eq!(record.reason, "client-initiated");
        // Where the handshake fails depends on the platform: the read sees
        // end of stream, or the first write after the peer's close sees a
        // broken pipe. Either is a failed attempt with its reason.
        assert!(record.error.is_some(), "{record:?}");
    }
    // Three backoffs of 250 ms sit inside the outage, so a reported time
    // under that is the last handshake and not the outage.
    let floor = Duration::from_millis(250) * dropped as u32;
    assert!(
        completed.seconds >= floor.as_secs_f64(),
        "the completed reconnect reports {} s, less than the {floor:?} its own backoffs took",
        completed.seconds
    );
    assert!(
        completed.seconds <= outage.as_secs_f64(),
        "it cannot report more than the outage the test observed ({outage:?}): {}",
        completed.seconds
    );
    // Each failed attempt reports how long the session had been without a
    // connection, so the series is non-decreasing and ends below the total.
    let mut previous = 0.0f64;
    for record in &failed {
        assert!(record.seconds >= previous, "{records:?}");
        previous = record.seconds;
    }
    assert!(previous <= completed.seconds, "{records:?}");
    Ok(())
}

/// A re-offer or a scheduled block that reaches a session while it has no
/// connection cannot be held until there is one, and used to be dropped
/// without a trace: a re-offer never sent and one the server never answered
/// then read the same in the mid-flight census. Each is reported as the
/// failure it is, under its kind, so the offer accounting and the census
/// can say which it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn work_that_reaches_a_disconnected_session_is_reported_not_dropped() -> Result<()> {
    use std::time::Duration;
    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let shared = Arc::new(client::SessionShared {
        phase: std::sync::RwLock::new("mid_flight_kill".to_owned()),
        events,
        record_notifies: std::sync::atomic::AtomicBool::new(false),
    });
    // Nothing listens on port 1, so the session never holds a connection.
    let handle = client::spawn_session(session_config(3), 1, "127.0.0.1:1".into(), shared, 1);
    handle.control.send(client::Control::Reoffer {
        share_id: "pload1abc.s00003:lost".into(),
        job_id: "job-1".into(),
        extranonce2_hex: String::new(),
        ntime_hex: String::new(),
        nonce_hex: String::new(),
        header_hex: String::new(),
    })?;
    handle.control.send(client::Control::ScheduledBlock)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut failures = Vec::new();
    while failures.len() < 2 {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("both failures are reported within the deadline")
            .expect("the session is still running");
        match event {
            client::Event::Failure(failure) => failures.push(failure),
            client::Event::Reconnect(record) => assert!(!record.completed, "{record:?}"),
            other => panic!("unexpected event: {other:?}"),
        }
    }
    let _ = handle.control.send(client::Control::Stop);
    tokio::time::timeout(Duration::from_secs(5), handle.task).await??;
    let kinds: Vec<client::FailureKind> = failures.iter().map(|f| f.kind).collect();
    assert_eq!(
        kinds,
        vec![
            client::FailureKind::Reoffer,
            client::FailureKind::ScheduledBlock
        ]
    );
    for failure in &failures {
        assert_eq!(failure.session, 3);
        assert_eq!(failure.phase, "mid_flight_kill");
        assert!(!failure.recorded, "no submit record carries it");
        assert!(
            failure.error.contains("no connection"),
            "the failure says why: {}",
            failure.error
        );
    }
    assert!(
        failures[0].error.contains("pload1abc.s00003:lost"),
        "the re-offer names its share: {}",
        failures[0].error
    );
    assert_eq!(
        handle
            .outstanding
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "nothing was counted as outstanding"
    );
    Ok(())
}

/// A client-initiated reconnect waits for the session's outstanding submits
/// before it closes the socket, and the wait is the configured share-commit
/// timeout plus the drain margin, the deadline the phase boundaries and the
/// drained restart already use. It was a fixed 20 s: with
/// `--share-commit-timeout-seconds` above that, a submit the server was
/// still legitimately working on was recorded as no-response by the
/// harness's own close, and a later commit of it read as a durability loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconnect_waits_the_configured_commit_timeout_for_an_outstanding_submit() -> Result<()> {
    // The wait is the configured limit, not a constant: with a 1 s limit
    // the held submit is given up after about a second, where the fixed
    // deadline held the reconnect for 20 s.
    let (record, waited) = quiesced_submit(std::time::Duration::from_secs(1), None).await?;
    let client::Outcome::NoResponse { reason } = &record.outcome else {
        panic!("a submit still unanswered at the limit is no-response: {record:?}");
    };
    assert!(reason.contains("quiesce timed out"), "{reason}");
    assert!(
        reason.contains("1.0s") && reason.contains("share-commit timeout"),
        "the record says how long it was given and why: {reason}"
    );
    assert!(
        waited >= std::time::Duration::from_millis(900),
        "the whole limit is waited: {waited:?}"
    );
    assert!(
        waited < std::time::Duration::from_secs(10),
        "the limit is the configured one, not the fixed 20 s: {waited:?}"
    );

    // And a submit the server answers inside the configured limit is
    // recorded as answered, however long that took: nothing is manufactured
    // while the server is still allowed to be working.
    let (record, waited) = quiesced_submit(
        std::time::Duration::from_secs(8),
        Some(std::time::Duration::from_millis(2_500)),
    )
    .await?;
    assert!(
        matches!(record.outcome, client::Outcome::Accepted),
        "answered inside the limit: {record:?}"
    );
    assert!(
        waited >= std::time::Duration::from_millis(2_400),
        "the answer came after the hold: {waited:?}"
    );
    assert!(record
        .latency_millis
        .is_some_and(|millis| millis >= 2_400.0));
    Ok(())
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

/// A reconnect belongs to the phase that asked for it. One started near the
/// end of the `reconnect` phase and completed after `slow_database` began was
/// stamped with the phase current at completion, so it went missing from
/// `reconnect_events` and turned up under a phase that configured no
/// reconnect at all. The initiating phase travels with the operation, as it
/// does for a submit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconnect_is_attributed_to_the_phase_that_asked_for_it() -> Result<()> {
    let server = fake_stratum(false).await;
    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let shared = Arc::new(client::SessionShared {
        phase: std::sync::RwLock::new("reconnect".to_owned()),
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
    // The first connection completes.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the session connects within the deadline")
            .expect("the session is still running");
        if matches!(event, client::Event::Connected { .. }) {
            break;
        }
    }

    // The reconnect phase asks for a reconnect, and the new connection is
    // held inside its handshake while the run moves on to the next phase.
    server.release_authorize.send(false)?;
    let phase: Arc<str> = Arc::from("reconnect");
    handle.control.send(client::Control::Reconnect {
        reason: "client-initiated".into(),
        phase: phase.clone(),
    })?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    *shared.phase.write().unwrap() = "slow_database".to_owned();
    server.release_authorize.send(true)?;

    let record = loop {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the reconnect completes within the deadline")
            .expect("the session is still running");
        if let client::Event::Reconnect(record) = event {
            break record;
        }
    };
    assert!(record.completed, "{record:?}");
    assert_eq!(record.reason, "client-initiated");
    assert_eq!(
        record.phase, "reconnect",
        "the reconnect was asked for in the reconnect phase and belongs there, not in the \
         phase the run had reached when the handshake finally completed"
    );
    let _ = handle.control.send(client::Control::Stop);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.task).await??;
    Ok(())
}

/// The share difficulty is a premise of the whole measurement. A frontend
/// that advertised another value was recorded under
/// `client.difficulty_mismatches` and nothing read the list: with a lower
/// advertised value the client goes on mining the harder configured target,
/// its shares are still accepted, and the artifact validated while measuring
/// less work per share than the configuration it names. A non-empty list now
/// refuses qualification -- artifact withheld, exit 8 -- once every session
/// holds work and again after the load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_advertised_difficulty_other_than_the_configured_one_refuses_qualification() -> Result<()>
{
    use qbit_prism_load::artifact::{write_or_withhold, Evidence, Withhold};
    use qbit_prism_load::run::{
        difficulty_premise_contradiction, premise_block, withhold_decision, RunOutcome,
    };
    let replication = agreed_replication();

    // The observation: a session reports the disagreement with the values
    // on both sides, and still connects and holds work.
    let configured = session_config(0).share_difficulty;
    let server = fake_stratum_with(StratumOptions {
        advertised_difficulty: Some(configured / 2.0),
        ..Default::default()
    })
    .await;
    let (events, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    let shared = Arc::new(client::SessionShared {
        phase: std::sync::RwLock::new("setup".to_owned()),
        events,
        record_notifies: std::sync::atomic::AtomicBool::new(false),
    });
    let handle = client::spawn_session(
        session_config(0),
        0,
        server.address.clone(),
        shared.clone(),
        1,
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut mismatch = None;
    let mut connected = false;
    while !(mismatch.is_some() && connected) {
        let event = tokio::time::timeout_at(deadline, inbox.recv())
            .await
            .expect("the session reports the mismatch and connects within the deadline")
            .expect("the session is still running");
        match event {
            client::Event::DifficultyMismatch {
                session,
                advertised,
                configured: seen,
            } => {
                assert_eq!(session, 0);
                assert_eq!(seen, configured);
                mismatch = Some((session, advertised, seen));
            }
            client::Event::Connected { .. } => connected = true,
            _ => {}
        }
    }
    let mismatch = mismatch.expect("reported");
    assert!(
        (mismatch.1 - configured / 2.0).abs() <= f64::EPSILON,
        "the advertised value is reported as sent: {mismatch:?}"
    );
    let _ = handle.control.send(client::Control::Stop);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.task).await??;

    // The decision: agreement is nothing; one mismatch is a contradiction
    // that names the session and both values.
    assert_eq!(difficulty_premise_contradiction(&[]), None);
    let reason =
        difficulty_premise_contradiction(&[mismatch]).expect("a mismatch contradicts the premise");
    assert!(reason.contains("session 0"), "{reason}");
    assert!(reason.contains(&format!("{}", configured)), "{reason}");
    assert!(
        reason.contains(&format!("{}", configured / 2.0)),
        "{reason}"
    );
    let block = premise_block(Some(&reason), &[mismatch], &replication);
    assert_eq!(block["contradicted"], json!(true));
    assert_eq!(block["share_difficulty_agreed"], json!(false));
    assert_eq!(block["difficulty_mismatches"][0]["session"], json!(0));
    let agreed = premise_block(None, &[], &replication);
    assert_eq!(agreed["contradicted"], json!(false));
    assert!(agreed["error"].is_null());

    // The withholding: a contradicted premise withholds the artifact, ranks
    // below a hard block and above an abort, and exits 8.
    let withheld =
        withhold_decision(None, Some(&reason), Some("load-fe-1 exited")).expect("withheld");
    assert_eq!(withheld, Withhold::PremiseContradicted(reason.clone()));
    assert!(
        withheld.reason().contains("premise"),
        "{}",
        withheld.reason()
    );
    assert!(withheld.reason().contains(&reason), "{}", withheld.reason());
    assert!(matches!(
        withhold_decision(Some("jsonb exceeds the maximum of"), Some(&reason), None),
        Some(Withhold::Blocked(_))
    ));
    let outcome = RunOutcome {
        withhold: Some(&withheld),
        durability_findings: 0,
        harness_bug_rejections: 0,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: 0,
        no_response_commits_mid_run: 0,
    };
    assert_eq!(outcome.exit_code(), run::EXIT_PREMISE_CONTRADICTED);
    assert_ne!(run::EXIT_PREMISE_CONTRADICTED, run::EXIT_OK);
    let explanation = outcome
        .explanation(std::path::Path::new("r.json"))
        .expect("a refused run says so");
    assert!(explanation.contains("premise"), "{explanation}");
    assert!(explanation.contains("withheld"), "{explanation}");

    let dir = ScratchDir::new("premise");
    let path = dir.path().join("capacity-evidence.json");
    std::fs::write(&path, b"{\"schema\": \"stale artifact\"}")?;
    let Evidence::Withheld {
        reason: printed,
        stale_artifact_removed,
    } = write_or_withhold(&sample_inputs(), Some(&withheld), dir.path(), "srv")?
    else {
        panic!("a contradicted premise must not write an artifact");
    };
    assert!(printed.contains("session 0"), "{printed}");
    assert!(stale_artifact_removed);
    assert!(!path.exists());
    Ok(())
}

/// A replication premise that agrees with itself, for tests about the other
/// premise.
fn agreed_replication() -> run::ReplicationPremise {
    use qbit_prism_load::cluster::{ObservedReplication, Replication};
    run::ReplicationPremise {
        declared: Replication::Async,
        at_entry: ObservedReplication::Observed {
            mode: Replication::Async,
        },
        after_load: None,
    }
}

/// The run recorded the declared replication mode and the observed one and
/// did nothing when they disagreed: a run that declared an asynchronous
/// standby and observed none validated an artifact for conditions it never
/// established. Worse, `detect_replication` swallowed a read error into
/// `none`, a definite value for something it had not read. A disagreement
/// is now a contradicted premise -- artifact withheld, exit 8, through the
/// same block as the difficulty check -- and the unreadable case is its own
/// state, `unknown` with the reason, contradicted the same way rather than
/// guessed in either direction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replication_mode_other_than_the_declared_one_refuses_qualification() -> Result<()> {
    use qbit_prism_load::artifact::Withhold;
    use qbit_prism_load::cluster::{detect_replication, ObservedReplication, Replication};
    use qbit_prism_load::run::{
        premise_block, premise_contradiction, withhold_decision, ReplicationPremise, RunOutcome,
    };
    use std::time::Duration;

    // The unreadable case: a pool that cannot serve the query. It is closed
    // before it ever connects, so the failure is immediate and touches no
    // socket; this used to come back as `none`.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect_lazy("postgresql://nobody@127.0.0.1:1/nothing")?;
    pool.close().await;
    let unreadable = detect_replication(&pool).await;
    let ObservedReplication::Unknown {
        reason: unreadable_reason,
    } = &unreadable
    else {
        panic!("an unreadable pg_stat_replication is unknown, not a mode: {unreadable:?}");
    };
    assert!(
        unreadable_reason.contains("pg_stat_replication could not be read"),
        "the reason names what could not be read: {unreadable_reason}"
    );
    assert_eq!(unreadable.as_str(), "unknown");
    assert_eq!(unreadable.reason(), Some(unreadable_reason.as_str()));

    // Agreement is nothing.
    let agreed = agreed_replication();
    assert_eq!(agreed.contradiction(), None);
    assert!(agreed.agreed());
    let sync = ReplicationPremise {
        declared: Replication::Sync,
        at_entry: ObservedReplication::Observed {
            mode: Replication::Sync,
        },
        after_load: Some(ObservedReplication::Observed {
            mode: Replication::Sync,
        }),
    };
    assert_eq!(sync.contradiction(), None);

    // A declared standby that was not observed contradicts the premise, and
    // the reason names both modes and when.
    let missing = ReplicationPremise {
        declared: Replication::Async,
        at_entry: ObservedReplication::Observed {
            mode: Replication::None,
        },
        after_load: None,
    };
    let reason = missing
        .contradiction()
        .expect("a declared standby that is not there contradicts the premise");
    assert!(reason.contains("declared replication async"), "{reason}");
    assert!(reason.contains("observed none"), "{reason}");
    assert!(reason.contains("at entry"), "{reason}");

    // The check runs again after the load: a standby that vanished while
    // the load ran is the same contradiction seen late.
    let vanished = ReplicationPremise {
        after_load: Some(ObservedReplication::Observed {
            mode: Replication::None,
        }),
        ..agreed.clone()
    };
    let late = vanished.contradiction().expect("a standby that vanished");
    assert!(late.contains("after the load"), "{late}");
    assert!(!late.contains("at entry"), "{late}");

    // Unknown is contradicted too, with the reason, and is not read as
    // either mode.
    let unknown = ReplicationPremise {
        declared: Replication::Async,
        at_entry: unreadable.clone(),
        after_load: None,
    };
    let unknown_reason = unknown
        .contradiction()
        .expect("a mode that could not be observed contradicts the premise");
    assert!(
        unknown_reason.contains("could not observe"),
        "{unknown_reason}"
    );
    assert!(
        unknown_reason.contains(unreadable_reason.as_str()),
        "{unknown_reason}"
    );
    let none_declared = ReplicationPremise {
        declared: Replication::None,
        ..unknown.clone()
    };
    assert!(
        none_declared.contradiction().is_some(),
        "unknown is not none: declaring none does not make an unreadable view agree"
    );

    // The premise block carries the replication facts beside the
    // difficulty ones, and the two premises combine into one reason.
    let block = premise_block(Some(&unknown_reason), &[], &unknown);
    assert_eq!(block["contradicted"], json!(true));
    assert_eq!(block["share_difficulty_agreed"], json!(true));
    assert_eq!(block["replication_agreed"], json!(false));
    assert_eq!(block["replication"]["declared"], json!("async"));
    assert_eq!(block["replication"]["observed_at_entry"], json!("unknown"));
    assert_eq!(
        block["replication"]["observed_at_entry_reason"],
        json!(unreadable_reason)
    );
    assert_eq!(block["replication"]["checked_after_load"], json!(false));
    assert!(block["replication"]["observed_after_load"].is_null());
    assert_eq!(block["replication"]["agreed"], json!(false));
    let clean = premise_block(None, &[], &agreed);
    assert_eq!(clean["replication_agreed"], json!(true));
    assert_eq!(clean["replication"]["observed_at_entry"], json!("async"));
    assert!(clean["replication"]["observed_at_entry_reason"].is_null());
    assert_eq!(premise_contradiction(None, None), None);
    assert_eq!(
        premise_contradiction(None, Some("r".into())).as_deref(),
        Some("r")
    );
    let both = premise_contradiction(Some("d".into()), Some("r".into())).expect("both");
    assert!(both.contains('d') && both.contains('r'), "{both}");

    // The withholding: the same block, the same code.
    let withheld = withhold_decision(None, Some(&reason), None).expect("withheld");
    assert_eq!(withheld, Withhold::PremiseContradicted(reason.clone()));
    let outcome = RunOutcome {
        withhold: Some(&withheld),
        durability_findings: 0,
        harness_bug_rejections: 0,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: 0,
        no_response_commits_mid_run: 0,
    };
    assert_eq!(outcome.exit_code(), run::EXIT_PREMISE_CONTRADICTED);
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
    use qbit_prism_load::artifact::{write_or_withhold, Evidence, Withhold};
    let dir = ScratchDir::new("withhold");
    let path = dir.path().join("capacity-evidence.json");
    std::fs::write(
        &path,
        b"{\"schema\": \"stale artifact from an earlier run\"}",
    )?;
    let inputs = sample_inputs();

    let withheld = write_or_withhold(
        &inputs,
        Some(&Withhold::Aborted(
            "MemAvailable fell to 512 MiB, below the 4096 MiB floor".into(),
        )),
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
    } = write_or_withhold(
        &inputs,
        Some(&Withhold::Aborted("load-fe-1 exited".into())),
        dir.path(),
        "srv",
    )?
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

/// A hard refusal is a hard refusal whenever it is logged. The startup check
/// caught one at launch, but a JSONB-ceiling refusal logged later -- during
/// a scheduled-block rebuild, with ordinary shares still being accepted --
/// was only appended to `blocked.log_matches` by the final scan: the run
/// still built and validated the artifact, reported `blocked: false`, and
/// could exit 0. The late scan is now re-checked with `is_hard_block`, the
/// artifact is withheld with the line as its reason, and the run exits 3
/// ahead of every other outcome.
#[test]
fn a_hard_block_logged_after_startup_withholds_the_artifact_and_exits_blocked() -> Result<()> {
    use qbit_prism_load::artifact::{write_or_withhold, Evidence, Withhold};
    use qbit_prism_load::classify::{classify_log_line, BlockedKind};
    use qbit_prism_load::run::{hard_block_line, withhold_decision, RunOutcome};

    let ceiling = "2026-09-14T12:00:00Z WARN qbit_prism_server::coordinator: job persistence \
                   deferred error=total size of jsonb array elements exceeds the maximum of \
                   268435455 bytes";
    let deferral = "2026-09-14T12:00:01Z WARN qbit_prism_server::coordinator: template refresh \
                    deferred error=chain view unavailable";
    let ceiling_log = classify_log_line(ceiling).expect("the ceiling line is a refusal");
    let deferral_log = classify_log_line(deferral).expect("the deferral line is a refusal");
    assert_eq!(ceiling_log.kind, BlockedKind::JsonbCeiling);
    assert_eq!(deferral_log.kind, BlockedKind::RefreshDeferred);

    // A transient deferral alone is not a block; the ceiling is, wherever it
    // sits in the scan.
    assert_eq!(hard_block_line(std::slice::from_ref(&deferral_log)), None);
    assert_eq!(
        hard_block_line(&[deferral_log.clone(), ceiling_log.clone()]).as_deref(),
        Some(ceiling)
    );

    // The decision: a late hard block withholds, and outranks an abort.
    assert_eq!(withhold_decision(None, None, None), None);
    assert_eq!(
        withhold_decision(None, None, Some("load-fe-1 exited unexpectedly")),
        Some(Withhold::Aborted("load-fe-1 exited unexpectedly".into()))
    );
    let blocked = withhold_decision(Some(ceiling), None, Some("load-fe-1 exited unexpectedly"))
        .expect("a hard block withholds the artifact");
    assert_eq!(blocked, Withhold::Blocked(ceiling.to_owned()));
    assert!(blocked.reason().contains(ceiling), "{}", blocked.reason());
    assert!(blocked.reason().contains("blocked"), "{}", blocked.reason());

    // The exit code: 3, ahead of the abort and ahead of every finding a run
    // with a refused size may also carry.
    let clean = RunOutcome {
        withhold: None,
        durability_findings: 0,
        harness_bug_rejections: 0,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: 0,
        no_response_commits_mid_run: 0,
    };
    assert_eq!(clean.exit_code(), run::EXIT_OK);
    assert_eq!(clean.explanation(std::path::Path::new("r.json")), None);
    let late = RunOutcome {
        withhold: Some(&blocked),
        durability_findings: 1,
        harness_bug_rejections: 1,
        divergences: 1,
        unknown_outcome_commits: 1,
        no_response_commits: 1,
        no_response_commits_mid_run: 0,
    };
    assert_eq!(late.exit_code(), run::EXIT_BLOCKED);
    let explanation = late
        .explanation(std::path::Path::new("r.json"))
        .expect("a blocked run says so");
    assert!(explanation.starts_with("run blocked:"), "{explanation}");
    assert!(explanation.contains(ceiling), "{explanation}");
    let aborted = Withhold::Aborted("MemAvailable fell".into());
    assert_eq!(
        RunOutcome {
            withhold: Some(&aborted),
            ..late
        }
        .exit_code(),
        run::EXIT_ABORTED
    );
    assert_eq!(
        RunOutcome {
            withhold: None,
            ..late
        }
        .exit_code(),
        run::EXIT_DURABILITY,
        "with nothing withheld the findings decide, in their existing order"
    );
    assert_eq!(
        RunOutcome {
            withhold: None,
            durability_findings: 0,
            ..late
        }
        .exit_code(),
        run::EXIT_HARNESS_BUG_REJECTIONS
    );
    assert_eq!(
        RunOutcome {
            withhold: None,
            durability_findings: 0,
            harness_bug_rejections: 0,
            ..late
        }
        .exit_code(),
        run::EXIT_ACK_COMMIT_DIVERGENCE
    );

    // The artifact step: inputs that would build a valid artifact are
    // withheld under a late block exactly as under an abort, and a stale
    // artifact at the path goes with it.
    let dir = ScratchDir::new("late-block");
    let path = dir.path().join("capacity-evidence.json");
    std::fs::write(&path, b"{\"schema\": \"stale artifact\"}")?;
    let Evidence::Withheld {
        reason,
        stale_artifact_removed,
    } = write_or_withhold(&sample_inputs(), Some(&blocked), dir.path(), "srv")?
    else {
        panic!("a late hard block must not write an artifact");
    };
    assert!(reason.contains(ceiling), "{reason}");
    assert!(stale_artifact_removed);
    assert!(!path.exists(), "nothing self-validating survives the block");
    Ok(())
}

/// Every way out of a run writes a report that says how it ended, except an
/// error: that returned through the caller with a line on stderr, and the
/// directory the invocation had already claimed stayed empty, as if nothing
/// had run. A failed run now leaves a report naming the failure and nothing
/// that could be read as a measurement; a report the invocation already
/// wrote stands.
#[test]
fn a_run_that_fails_after_taking_its_directory_leaves_the_reason_in_it() -> Result<()> {
    use qbit_prism_load::report;
    let dir = ScratchDir::new("failed");
    let run_id = uuid::Uuid::new_v4();
    let removed = vec!["capacity-evidence.json".to_owned()];
    let path = report::write_failure(
        dir.path(),
        run_id,
        "initialising the schema: connection refused",
        &removed,
    )?
    .expect("the first report is written");
    assert_eq!(path, dir.path().join("load-harness-report.json"));
    let document: Value = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
    assert_eq!(document["schema"], json!(report::SCHEMA));
    assert_eq!(document["run_id"], json!(run_id.to_string()));
    assert_eq!(document["failed"]["failed"], json!(true));
    assert_eq!(
        document["failed"]["error"],
        json!("initialising the schema: connection refused")
    );
    assert_eq!(document["validator"]["artifact_written"], json!(false));
    assert!(document["validator"]["withheld_reason"]
        .as_str()
        .is_some_and(|reason| reason.contains("connection refused")));
    assert_eq!(document["stale_outputs_removed"], json!(removed));
    assert!(
        document.get("phases").is_none() && document.get("reconciliation").is_none(),
        "nothing in a failure report reads as a measurement"
    );
    assert!(
        !dir.path().join("capacity-evidence.json").exists(),
        "no artifact accompanies a failure"
    );

    // A report already written by this invocation is not overwritten by a
    // later failure: the failure is then not the whole story.
    let again = report::write_failure(dir.path(), run_id, "a later error", &[])?;
    assert_eq!(again, None);
    let unchanged: Value = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
    assert_eq!(unchanged, document);

    // The report is a sink: an error that names a URL, from a context the
    // source forgot to redact, is redacted here regardless.
    let dir = ScratchDir::new("failed-url");
    let path = report::write_failure(
        dir.path(),
        run_id,
        "timing a round trip through the delay proxy: connect for round-trip measurement: \
         postgresql://alex:hunter2@127.0.0.1:1/qbit: connection refused",
        &[],
    )?
    .expect("written");
    let text = std::fs::read_to_string(&path)?;
    assert!(!text.contains("hunter2"), "{text}");
    assert!(
        text.contains("postgresql://alex:<redacted>@127.0.0.1:1/qbit"),
        "{text}"
    );
    Ok(())
}

/// The abort path withholds the artifact and removes a stale one, but the
/// blocked path writes only a side report, and a run that fails before it
/// measures writes nothing. Reusing a default `--out` after a successful run
/// then left that run's self-validating artifact and profile beside a fresh
/// blocked report. The honest fix is one obligation at entry: an invocation
/// that takes the directory removes every earlier output first, so no exit
/// path can leave evidence it did not produce.
#[test]
fn an_invocation_removes_the_previous_run_s_outputs_when_it_takes_the_directory() -> Result<()> {
    use qbit_prism_load::report::{claim_out_dir, OUTPUTS};
    let dir = ScratchDir::new("claim-out");
    let out = dir.path().join("out");
    assert_eq!(
        OUTPUTS,
        [
            "capacity-evidence.json",
            "database-profile.json",
            "load-harness-report.json"
        ],
        "the three documents a run writes are the three an invocation clears"
    );

    // A fresh directory is created and nothing is reported removed.
    assert!(claim_out_dir(&out)?.is_empty());
    assert!(out.is_dir());

    // A previous run's outputs, plus a log the frontends manage themselves.
    for name in OUTPUTS {
        std::fs::write(out.join(name), b"{\"schema\": \"an earlier run\"}")?;
    }
    std::fs::create_dir_all(out.join("logs"))?;
    std::fs::write(out.join("logs/load-fe-0.stderr.log"), b"earlier log\n")?;

    let removed = claim_out_dir(&out)?;
    assert_eq!(
        removed, OUTPUTS,
        "every earlier output is removed and named"
    );
    for name in OUTPUTS {
        assert!(!out.join(name).exists(), "{name} survived");
    }
    assert!(
        out.join("logs/load-fe-0.stderr.log").exists(),
        "the logs are the frontends' to start empty, not this step's"
    );

    // A partial leftover -- the artifact alone, as an interrupted copy might
    // leave -- is removed and reported by name.
    std::fs::write(out.join("capacity-evidence.json"), b"{}")?;
    assert_eq!(claim_out_dir(&out)?, ["capacity-evidence.json"]);
    assert!(claim_out_dir(&out)?.is_empty());
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

/// The mid-flight kill's census took every no-response recorded since the
/// kill, whichever frontend's session produced it. A session on the other
/// frontend whose socket closed in the same few seconds was counted as one
/// of the kill's indeterminate shares and re-offered as such. The census is
/// now the killed frontend's own sessions, since the kill.
#[test]
fn the_mid_flight_census_is_the_killed_frontend_s_own_no_responses() {
    use qbit_prism_load::client::Outcome;
    use qbit_prism_load::run::indeterminate_after_kill;
    let no_response = |frontend: usize, share: &str| {
        let mut record = submit_record(
            "mid_flight_kill",
            Outcome::NoResponse {
                reason: "socket closed".into(),
            },
        );
        record.frontend = frontend;
        record.share_id = format!("pload1abc.s00001:{}", share.repeat(64));
        record
    };
    let mut accepted_after = submit_record("mid_flight_kill", Outcome::Accepted);
    accepted_after.frontend = 1;
    let mut reoffer = no_response(1, "4");
    reoffer.reoffer = true;
    let records = vec![
        // Before the kill: not in the window however it ended.
        no_response(1, "0"),
        // Since the kill: the killed frontend's victim, another frontend's
        // own closed socket, an answered submit, and a re-offer.
        no_response(1, "1"),
        no_response(0, "2"),
        accepted_after,
        reoffer,
    ];
    let census = indeterminate_after_kill(&records, 1, 1);
    assert_eq!(
        census
            .iter()
            .map(|record| record.share_id.as_str())
            .collect::<Vec<_>>(),
        vec![format!("pload1abc.s00001:{}", "1".repeat(64)).as_str()],
        "one victim: frontend 1's no-response since the kill, and nothing from frontend 0"
    );
    assert!(
        indeterminate_after_kill(&records, 10, 1).is_empty(),
        "a since past the end is an empty window, not a panic"
    );
}

/// In the mid-flight kill scenario a re-offered share that came back
/// `duplicate-share` while `in_postgres` was false was recorded and ignored:
/// the server believes it has a share the database does not, a possible
/// loss, dropped between two fields of the same JSON object. Each
/// indeterminate share now has an outcome of its own, the duplicate case is
/// reported on its own with its share ids, and the printed summary names
/// the possible losses. The exit code is unchanged: the mid-flight kill is
/// a deliberate side scenario in which indeterminate shares are legitimate.
#[test]
fn a_re_offer_the_server_called_a_duplicate_that_postgres_lacks_is_its_own_outcome() {
    use qbit_prism_load::artifact::Evidence;
    use qbit_prism_load::client::Outcome;
    use qbit_prism_load::run::{mid_flight_census, reoffer_outcome, summary_text, ReofferOutcome};
    use std::collections::BTreeSet;
    let duplicate = || {
        Outcome::Rejected(rejection(
            22,
            Some(classify::DUPLICATE_SHARE),
            "duplicate share",
        ))
    };
    let stale = || {
        Outcome::Rejected(rejection(
            21,
            Some("stale-job"),
            classify::NEW_TIP_WORK_PENDING,
        ))
    };
    let unanswered = || Outcome::NoResponse {
        reason: "socket closed".into(),
    };

    // The classification, over both answers and both database states.
    assert_eq!(
        reoffer_outcome(Some(&duplicate()), false),
        ReofferOutcome::DuplicateNotInPostgres
    );
    assert_eq!(
        reoffer_outcome(Some(&duplicate()), true),
        ReofferOutcome::CommittedBeforeKill
    );
    assert_eq!(
        reoffer_outcome(Some(&Outcome::Accepted), true),
        ReofferOutcome::ReofferAcceptedAndCommitted
    );
    assert_eq!(
        reoffer_outcome(Some(&Outcome::Accepted), false),
        ReofferOutcome::ReofferAcceptedNotInPostgres
    );
    assert_eq!(
        reoffer_outcome(Some(&stale()), false),
        ReofferOutcome::ReofferRejected
    );
    assert_eq!(
        reoffer_outcome(Some(&unanswered()), true),
        ReofferOutcome::ReofferUnanswered
    );
    assert_eq!(
        reoffer_outcome(None, false),
        ReofferOutcome::ReofferUnanswered
    );
    assert!(ReofferOutcome::DuplicateNotInPostgres.is_possible_loss());
    assert!(ReofferOutcome::ReofferAcceptedNotInPostgres.is_possible_loss());
    assert!(!ReofferOutcome::CommittedBeforeKill.is_possible_loss());
    assert!(!ReofferOutcome::ReofferUnanswered.is_possible_loss());

    // The census: four victims, re-offered. A's re-offer was a duplicate
    // and A is not in PostgreSQL; B's was a duplicate and B is; C's was
    // accepted and C is; D's was never answered.
    let share = |tag: &str| format!("pload1abc.s00001:{}", tag.repeat(64));
    let victim = |tag: &str| {
        let mut record = submit_record("mid_flight_kill", unanswered());
        record.share_id = share(tag);
        record
    };
    let reoffer = |tag: &str, outcome: Outcome| {
        let mut record = submit_record("mid_flight_kill", outcome);
        record.share_id = share(tag);
        record.reoffer = true;
        record
    };
    let indeterminate = vec![victim("a"), victim("b"), victim("c"), victim("d")];
    let submits = vec![
        victim("a"),
        victim("b"),
        victim("c"),
        victim("d"),
        reoffer("a", duplicate()),
        reoffer("b", duplicate()),
        reoffer("c", Outcome::Accepted),
    ];
    let committed: BTreeSet<String> = [share("b"), share("c")].into_iter().collect();
    let census = mid_flight_census(&indeterminate, &submits, &committed);
    assert_eq!(census["indeterminate_shares"], json!(4));
    let outcomes: Vec<&str> = census["shares"]
        .as_array()
        .expect("shares")
        .iter()
        .map(|entry| entry["outcome"].as_str().expect("an outcome per share"))
        .collect();
    assert_eq!(
        outcomes,
        vec![
            "duplicate-not-in-postgres",
            "committed-before-kill",
            "reoffer-accepted-and-committed",
            "reoffer-unanswered",
        ]
    );
    assert_eq!(census["shares"][0]["in_postgres"], json!(false));
    assert_eq!(
        census["shares"][0]["reoffer_answer"]["reason_id"],
        json!("duplicate-share")
    );
    assert!(census["shares"][3]["reoffer_answer"].is_null());
    assert_eq!(census["duplicate_not_in_postgres"]["count"], json!(1));
    assert_eq!(
        census["duplicate_not_in_postgres"]["share_ids"],
        json!([share("a")])
    );
    assert_eq!(census["possible_losses"]["count"], json!(1));
    assert_eq!(census["possible_losses"]["share_ids"], json!([share("a")]));
    assert!(
        census["outcomes"]
            .as_array()
            .expect("outcomes")
            .iter()
            .any(
                |entry| entry["outcome"] == json!("duplicate-not-in-postgres")
                    && entry["count"] == json!(1)
            ),
        "{}",
        census["outcomes"]
    );

    // Visible in the printed summary too, since the exit code does not
    // say so.
    let report = json!({"phases": [], "mid_flight_kill": census});
    let withheld = Evidence::Withheld {
        reason: "the run aborted: load-fe-1 exited".into(),
        stale_artifact_removed: false,
    };
    let text = summary_text(&report, &withheld, &[]);
    let line = text
        .lines()
        .find(|line| line.starts_with("mid-flight kill:"))
        .unwrap_or_else(|| panic!("the summary names the possible losses: {text}"));
    assert!(line.contains(&share("a")), "{line}");
    assert!(line.contains("duplicate-share"), "{line}");
    assert!(!line.contains(&share("b")), "{line}");
    // Nothing to say when there is nothing.
    let clean = mid_flight_census(&indeterminate[1..3], &submits, &committed);
    assert_eq!(clean["possible_losses"]["count"], json!(0));
    let report = json!({"phases": [], "mid_flight_kill": clean});
    assert!(!summary_text(&report, &withheld, &[]).contains("mid-flight kill:"));
}

/// One phase as `classify_gaps` takes it, with the kill's census.
fn driven(name: &str, kill_indeterminate: &[&str]) -> run::DrivenPhase {
    run::DrivenPhase {
        name: name.to_owned(),
        kill_indeterminate: kill_indeterminate
            .iter()
            .map(|id| (*id).to_owned())
            .collect(),
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
    // Every other explanation, and no explanation at all, is a loss. A
    // submit with no response is neither: it has its own kind, below.
    for outcome in [
        Outcome::Accepted,
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

/// A socket that closes after PostgreSQL commits a submit but before the
/// client reads the response leaves a `NoResponse` record; reconciliation
/// then finds the row, and the fallback called it a durability loss, exit 4.
/// Nothing was lost: the share is present, only its acknowledgement is, and
/// whether the server sent one cannot be known from this side. A false
/// data-loss alarm is a stop-and-ask for whoever reads the report, so the
/// transport-indeterminate case has its own kind, its own report bucket and
/// the divergence family's exit code -- never 0, because "we do not know" is
/// not "it was fine", and never 4, because it is not "it was lost".
#[test]
fn a_committed_share_whose_answer_was_lost_is_not_a_durability_loss() {
    use qbit_prism_load::client::Outcome;
    use qbit_prism_load::run::{classify_gaps, GapKind, RunOutcome};
    use std::collections::BTreeSet;
    let record = submit_record(
        "steady_state",
        Outcome::NoResponse {
            reason: "socket closed: end of stream".into(),
        },
    );
    assert_eq!(
        run::classify_committed_gap(Some(&record)),
        GapKind::NoResponseCommitted
    );
    // Through the whole classification: the share was offered in
    // steady_state, never acknowledged, and PostgreSQL holds it.
    let set =
        |ids: &[&str]| -> BTreeSet<String> { ids.iter().map(|id| (*id).to_owned()).collect() };
    let committed = set(&[record.share_id.as_str()]);
    let reconciliation = digest::reconcile(committed.clone(), set(&[]), &committed);
    let attribution =
        digest::attribute_unexpected(&committed, &[("steady_state".to_owned(), &reconciliation)]);
    let gaps = classify_gaps(
        &[driven("steady_state", &[])],
        &[("steady_state".to_owned(), reconciliation)],
        &attribution,
        std::slice::from_ref(&record),
        15.0,
    );
    assert_eq!(gaps.findings, json!([]), "not a loss");
    assert!(gaps.divergences.is_empty() && gaps.unknown_outcome_commits.is_empty());
    assert_eq!(gaps.no_response_commits.len(), 1, "{gaps:?}");
    let detail = &gaps.no_response_commits[0];
    assert_eq!(detail["share_id"], json!(record.share_id));
    assert_eq!(detail["phase"], json!("steady_state"));
    assert_eq!(
        detail["no_response_reason"],
        json!("socket closed: end of stream")
    );
    assert_eq!(detail["classification"], json!("transport-indeterminate"));
    // A peer close mid-run, not the measurement window ending underneath the
    // submit, so it counts toward the exit code.
    assert_eq!(detail["window_ended"], json!(false));
    let mid_run = gaps
        .no_response_commits
        .iter()
        .filter(|share| share["window_ended"] != json!(true))
        .count();
    let outcome = RunOutcome {
        withhold: None,
        durability_findings: 0,
        harness_bug_rejections: 0,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: gaps.no_response_commits.len(),
        no_response_commits_mid_run: mid_run,
    };
    assert_eq!(outcome.exit_code(), run::EXIT_ACK_COMMIT_DIVERGENCE);
    assert_ne!(
        outcome.exit_code(),
        run::EXIT_OK,
        "we do not know is not it was fine"
    );
    assert_ne!(
        outcome.exit_code(),
        run::EXIT_DURABILITY,
        "we do not know is not it was lost"
    );
    let explanation = outcome
        .explanation(std::path::Path::new("r.json"))
        .expect("a no-response commit is explained");
    assert!(explanation.contains("no response"), "{explanation}");
    assert!(explanation.contains("nothing lost"), "{explanation}");
    // The mid-flight kill's own indeterminate shares are its own business:
    // one in its census is reported there, under `mid_flight_kill`, and in
    // none of these buckets.
    let committed = set(&[record.share_id.as_str()]);
    let reconciliation = digest::reconcile(committed.clone(), set(&[]), &committed);
    let attribution = digest::attribute_unexpected(
        &committed,
        &[("mid_flight_kill".to_owned(), &reconciliation)],
    );
    let gaps = classify_gaps(
        &[driven("mid_flight_kill", &[record.share_id.as_str()])],
        &[("mid_flight_kill".to_owned(), reconciliation)],
        &attribution,
        std::slice::from_ref(&record),
        15.0,
    );
    assert_eq!(gaps.findings, json!([]));
    assert!(gaps.no_response_commits.is_empty());
}

/// With `--mid-flight-kill` the reconciliation analysis skipped the kill
/// phase entirely, not just the no-responses the kill produced. A share
/// that phase acknowledged in the ordinary way -- before the kill, on the
/// healthy frontend, or after the relaunch -- and that PostgreSQL then lost
/// sat in `reconciliation.missing`, reached no durability finding, and the
/// run could exit 0: a false negative on the one thing the harness exists
/// to catch. The exemption is now exactly the kill's census, the shares its
/// re-offers and its `mid_flight_kill` report account for. Everything else
/// in the phase is classified as it is in every other phase.
#[test]
fn a_kill_phase_does_not_hide_an_acknowledged_share_the_database_lost() {
    use qbit_prism_load::client::Outcome;
    use qbit_prism_load::run::{classify_gaps, RunOutcome};
    use std::collections::BTreeSet;
    let set =
        |ids: &[&str]| -> BTreeSet<String> { ids.iter().map(|id| (*id).to_owned()).collect() };
    let no_response = || Outcome::NoResponse {
        reason: "socket closed: end of stream".into(),
    };
    let with = |id: &str, frontend: usize, outcome: Outcome| client::SubmitRecord {
        share_id: id.to_owned(),
        frontend,
        ..submit_record("mid_flight_kill", outcome)
    };
    // `kept` and `lost` were acknowledged in the ordinary way; PostgreSQL
    // holds only `kept`. `killed` is the one submit the kill destroyed:
    // its answer never came, it is in the census, and PostgreSQL holds it.
    // `other` got no response on the healthy frontend, a socket closing for
    // its own reasons in the same phase, and PostgreSQL holds it too.
    // `refused` was rejected as a harness bug and is nonetheless committed,
    // which nothing explains.
    let records = vec![
        with("kept", 0, Outcome::Accepted),
        with("lost", 0, Outcome::Accepted),
        with("killed", 1, no_response()),
        with("other", 0, no_response()),
        with(
            "refused",
            0,
            Outcome::Rejected(rejection(
                23,
                Some("low-difficulty"),
                "low difficulty share",
            )),
        ),
    ];
    let committed = set(&["kept", "killed", "other", "refused"]);
    let (offered, acknowledged) = run::offered_and_acknowledged(&records, "mid_flight_kill");
    assert_eq!(
        offered,
        set(&["kept", "lost", "killed", "other", "refused"])
    );
    assert_eq!(acknowledged, set(&["kept", "lost"]));
    let reconciliation = digest::reconcile(offered, acknowledged, &committed);
    assert_eq!(reconciliation.missing, set(&["lost"]));
    let attribution = digest::attribute_unexpected(
        &committed,
        &[("mid_flight_kill".to_owned(), &reconciliation)],
    );
    let gaps = classify_gaps(
        &[driven("mid_flight_kill", &["killed"])],
        &[("mid_flight_kill".to_owned(), reconciliation)],
        &attribution,
        &records,
        15.0,
    );
    let findings = gaps
        .findings
        .as_array()
        .expect("durability_findings is a list");
    assert_eq!(findings.len(), 2, "{findings:?}");
    assert_eq!(findings[0]["phase"], json!("mid_flight_kill"));
    assert_eq!(
        findings[0]["kind"],
        json!("acknowledged share missing from PostgreSQL")
    );
    assert_eq!(findings[0]["count"], json!(1));
    assert_eq!(findings[0]["sample"], json!(["lost"]));
    assert_eq!(findings[1]["phase"], json!("mid_flight_kill"));
    assert_eq!(
        findings[1]["kind"],
        json!("committed share that was never acknowledged")
    );
    assert_eq!(findings[1]["sample"], json!(["refused"]));
    // The healthy frontend's own no-response is transport-indeterminate, as
    // it would be in any phase; the kill's is left to the kill's census.
    assert_eq!(
        gaps.no_response_commits.len(),
        1,
        "{:?}",
        gaps.no_response_commits
    );
    assert_eq!(gaps.no_response_commits[0]["share_id"], json!("other"));
    assert_eq!(gaps.no_response_commits[0]["window_ended"], json!(false));
    assert!(gaps.divergences.is_empty() && gaps.unknown_outcome_commits.is_empty());
    let outcome = RunOutcome {
        withhold: None,
        durability_findings: findings.len(),
        harness_bug_rejections: 1,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: 1,
        no_response_commits_mid_run: 1,
    };
    assert_eq!(
        outcome.exit_code(),
        run::EXIT_DURABILITY,
        "a loss in the kill phase weighs what a loss anywhere weighs"
    );

    // A kill that found nothing outstanding has an empty census and exempts
    // nothing: the phase is then an ordinary phase, and its `killed` row,
    // committed with no answer read, is a no-response commit like `other`.
    let (offered, acknowledged) = run::offered_and_acknowledged(&records, "mid_flight_kill");
    let reconciliation = digest::reconcile(offered, acknowledged, &committed);
    let attribution = digest::attribute_unexpected(
        &committed,
        &[("mid_flight_kill".to_owned(), &reconciliation)],
    );
    let gaps = classify_gaps(
        &[driven("mid_flight_kill", &[])],
        &[("mid_flight_kill".to_owned(), reconciliation)],
        &attribution,
        &records,
        15.0,
    );
    assert_eq!(
        gaps.no_response_commits
            .iter()
            .map(|share| share["share_id"].clone())
            .collect::<Vec<_>>(),
        vec![json!("killed"), json!("other")]
    );
    assert_eq!(gaps.findings.as_array().map(Vec::len), Some(2));
}

/// `reconciliation.unexpected_outside_phases` counted the run-prefixed rows
/// PostgreSQL holds that no phase claims, and nothing read the count: a run
/// holding such a row reconciled clean and exited 0. Every share the harness
/// offered has a submit record stamped with its phase, so the row is either
/// a commit the harness never saw offered or a persisted rejection it kept
/// out of the offered set as an entitled race. Both are durability
/// questions, so the rows are now a durability finding of their own kind,
/// with the sample, and weigh what an acknowledged share that went missing
/// weighs: exit 4.
#[test]
fn a_committed_row_that_no_phase_offered_is_a_durability_finding() {
    use qbit_prism_load::run::{
        classify_gaps, outside_phases_finding, RunOutcome, OUTSIDE_PHASES_KIND,
    };
    use std::collections::BTreeSet;
    let set =
        |ids: &[&str]| -> BTreeSet<String> { ids.iter().map(|id| (*id).to_owned()).collect() };
    // One phase offered and was acknowledged `a`; the database also holds
    // `z`, which no phase offered. A mid-flight kill phase ran too: its
    // census covers its own indeterminate rows, not rows outside every
    // phase.
    let committed = set(&["a", "z"]);
    let reconciliation = digest::reconcile(set(&["a"]), set(&["a"]), &committed);
    let phases = vec![
        driven("steady_state", &[]),
        driven("mid_flight_kill", &["k"]),
    ];
    let reconciliations = vec![("steady_state".to_owned(), reconciliation.clone())];
    let attribution =
        digest::attribute_unexpected(&committed, &[("steady_state".to_owned(), &reconciliation)]);
    assert_eq!(attribution.outside_phases, vec!["z".to_owned()]);
    let gaps = classify_gaps(&phases, &reconciliations, &attribution, &[], 15.0);
    let findings = gaps
        .findings
        .as_array()
        .expect("durability_findings is a list");
    assert_eq!(findings.len(), 1, "{findings:?}");
    assert_eq!(findings[0]["kind"], json!(OUTSIDE_PHASES_KIND));
    assert_eq!(findings[0]["count"], json!(1));
    assert_eq!(findings[0]["sample"], json!(["z"]));
    assert!(
        findings[0]["phase"].is_null(),
        "the row belongs to no phase, and the finding says so rather than naming one"
    );
    assert!(
        gaps.divergences.is_empty()
            && gaps.unknown_outcome_commits.is_empty()
            && gaps.no_response_commits.is_empty()
    );
    let outcome = RunOutcome {
        withhold: None,
        durability_findings: findings.len(),
        harness_bug_rejections: 0,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: 0,
        no_response_commits_mid_run: 0,
    };
    assert_eq!(outcome.exit_code(), run::EXIT_DURABILITY);

    // No such row, no finding: the fold adds nothing to a clean run.
    assert_eq!(outside_phases_finding(&[]), None);
    let clean = set(&["a"]);
    let reconciliation = digest::reconcile(set(&["a"]), set(&["a"]), &clean);
    let attribution =
        digest::attribute_unexpected(&clean, &[("steady_state".to_owned(), &reconciliation)]);
    let gaps = classify_gaps(
        &phases,
        &[("steady_state".to_owned(), reconciliation)],
        &attribution,
        &[],
        15.0,
    );
    assert_eq!(gaps.findings, json!([]));
}

/// A rejection whose `reason_id` the classifier does not recognise already
/// invalidated the artifact -- it is class `unknown`, so it stays in
/// `rejected_valid_shares` -- and the run said nothing about it: exit 0,
/// and the reader found it by reading the rejections JSON. An unrecognised
/// reason means the harness's model of the server is out of date, which is
/// the reader's problem to solve, so the printed summary names it and so
/// does the validator's refusal reason. The exit code is deliberately
/// unchanged: an unrecognised reason is not a harness bug (7) and not a
/// loss (4), and exit codes are a contract other tooling reads.
#[test]
fn an_unrecognised_rejection_reason_is_named_in_the_summary_and_the_refusal() {
    use qbit_prism_load::artifact::{Evidence, Verdict};
    use qbit_prism_load::client::Outcome;
    use qbit_prism_load::run::{
        name_unrecognised_reasons, summary_text, unrecognised_reasons_line,
        unrecognised_rejection_reasons, RunOutcome,
    };
    let novel = || rejection(29, Some("quantum-flux"), "share arrived from the future");
    assert_eq!(classify::classify(&novel()), RejectionClass::Unknown);
    let mut reoffered = submit_record("mid_flight_kill", Outcome::Rejected(novel()));
    reoffered.reoffer = true;
    let records = vec![
        submit_record("steady_state", Outcome::Rejected(novel())),
        submit_record("slow_database", Outcome::Rejected(novel())),
        // A re-offer's unrecognised reason says the same thing about the
        // harness's model of the server, so it counts.
        reoffered,
        submit_record(
            "steady_state",
            Outcome::Rejected(rejection(
                21,
                Some("stale-job"),
                classify::NEW_TIP_WORK_PENDING,
            )),
        ),
        submit_record(
            "steady_state",
            Outcome::Rejected(rejection(
                20,
                Some("backend-rpc-unavailable"),
                "current chain state is unavailable",
            )),
        ),
        submit_record("steady_state", Outcome::Accepted),
    ];
    let reasons = unrecognised_rejection_reasons(&records);
    assert_eq!(reasons.len(), 1, "{reasons:?}");
    assert_eq!(reasons[0].reason_id, "quantum-flux");
    assert_eq!(reasons[0].code, 29);
    assert_eq!(reasons[0].message, "share arrived from the future");
    assert_eq!(reasons[0].count, 3);
    assert!(unrecognised_rejection_reasons(&records[3..]).is_empty());
    assert_eq!(unrecognised_reasons_line(&[]), None);
    let line = unrecognised_reasons_line(&reasons).expect("a line for the reasons");
    assert!(line.contains("quantum-flux"), "{line}");
    assert!(line.starts_with("3 rejection(s)"), "{line}");
    assert!(line.contains("out of date"), "{line}");

    // The refusal reason: a written artifact the validator refused ends
    // its error chain with the line, so the printed INVALID verdict names
    // the reason.
    let written = |valid: bool| Evidence::Written {
        path: "capacity-evidence.json".into(),
        document: json!({}),
        verdict: Verdict {
            valid,
            summary: valid.then(|| "rate=1 shares/s".to_owned()),
            error_chain: if valid {
                Vec::new()
            } else {
                vec![
                    "phases.steady_state did not acknowledge every offered valid share: \
                      offered=4 acknowledged=1 rejected=3"
                        .to_owned(),
                ]
            },
        },
        command: "srv capacity-evidence capacity-evidence.json".into(),
    };
    let mut refused = written(false);
    name_unrecognised_reasons(&mut refused, &reasons);
    let Evidence::Written { verdict, .. } = &refused else {
        unreachable!()
    };
    assert_eq!(verdict.error_chain.len(), 2, "{:?}", verdict.error_chain);
    assert!(
        verdict.error_chain[0].contains("did not acknowledge"),
        "the validator's own reason stays first"
    );
    assert_eq!(verdict.error_chain[1], line);
    let report = json!({"phases": []});
    let text = summary_text(&report, &refused, &reasons);
    assert!(
        text.lines()
            .any(|l| l.starts_with("artifact verdict: INVALID:") && l.contains("quantum-flux")),
        "{text}"
    );
    assert!(
        text.lines()
            .any(|l| l.starts_with("unrecognised rejection reasons:") && l.contains("quantum-flux")),
        "{text}"
    );

    // A valid artifact stays valid -- its unrecognised reasons were in a
    // side phase -- and the summary still names them on their own line. So
    // does a withheld artifact's summary.
    let mut valid = written(true);
    name_unrecognised_reasons(&mut valid, &reasons);
    let Evidence::Written { verdict, .. } = &valid else {
        unreachable!()
    };
    assert!(verdict.valid && verdict.error_chain.is_empty());
    let text = summary_text(&report, &valid, &reasons);
    assert!(text.contains("artifact verdict: rate=1 shares/s"), "{text}");
    assert!(
        text.lines()
            .any(|l| l.starts_with("unrecognised rejection reasons:") && l.contains("quantum-flux")),
        "{text}"
    );
    let withheld = Evidence::Withheld {
        reason: "the run aborted: load-fe-1 exited".into(),
        stale_artifact_removed: false,
    };
    let text = summary_text(&report, &withheld, &reasons);
    assert!(text.contains("artifact withheld:"), "{text}");
    assert!(text.contains("quantum-flux"), "{text}");
    // Nothing to say when there is nothing.
    let mut clean = written(false);
    name_unrecognised_reasons(&mut clean, &[]);
    let Evidence::Written { verdict, .. } = &clean else {
        unreachable!()
    };
    assert_eq!(verdict.error_chain.len(), 1);
    assert!(!summary_text(&report, &clean, &[]).contains("unrecognised"));

    // The exit code is untouched: unrecognised is neither 7 nor 4.
    let outcome = RunOutcome {
        withhold: None,
        durability_findings: 0,
        harness_bug_rejections: 0,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: 0,
        no_response_commits_mid_run: 0,
    };
    assert_eq!(outcome.exit_code(), run::EXIT_OK);
}

/// Failed offers were collected under `client.failures` and never read, so a
/// phase's `dispatched` could exceed the submits it recorded with no account
/// of why: a silent discrepancy between two numbers in the same report. Each
/// failure now carries its phase and what the session was doing, and every
/// phase carries an `offer_accounting` that reconciles `dispatched` against
/// the submits recorded, the offers discarded and the offers that failed
/// before a submit line was written; what none of those explains is
/// `unaccounted`, reported rather than assumed away, and the printed summary
/// says so beside the phase's offered count whenever there is anything to
/// say.
#[test]
fn dispatched_offers_are_accounted_for_failures_included() {
    use qbit_prism_load::artifact::Evidence;
    use qbit_prism_load::client::{ClientFailure, Event, FailureKind, Outcome};
    use qbit_prism_load::run::{failures_by_kind, offer_accounting, summary_text, Collected};
    let now = std::time::Instant::now();
    let failure = |phase: &str, kind: FailureKind, recorded: bool, error: &str| {
        Event::Failure(ClientFailure {
            session: 1,
            phase: phase.to_owned(),
            kind,
            recorded,
            error: error.to_owned(),
            at: now,
        })
    };
    let submit = |record| Event::Submit(Box::new(record));
    let mut collected = Collected::default();
    // steady_state placed five offers. Two were submitted and answered; one's
    // write failed, which is a no-response submit record and a failure; one
    // was discarded when its session stopped; one found no solution.
    collected.apply(submit(submit_record("steady_state", Outcome::Accepted)));
    collected.apply(submit(submit_record(
        "steady_state",
        Outcome::Rejected(rejection(
            21,
            Some("stale-job"),
            classify::NEW_TIP_WORK_PENDING,
        )),
    )));
    collected.apply(submit(submit_record(
        "steady_state",
        Outcome::NoResponse {
            reason: "write failed: broken pipe".into(),
        },
    )));
    collected.apply(failure(
        "steady_state",
        FailureKind::Offer,
        true,
        "broken pipe",
    ));
    collected.apply(Event::DiscardedOffer {
        session: 1,
        phase: "steady_state".into(),
    });
    collected.apply(failure(
        "steady_state",
        FailureKind::Offer,
        false,
        "no share solution found under job job-1",
    ));
    // Not the scheduler's, so not in the account: a re-offer, a scheduled
    // block and its failure, a line the session could not handle. Another
    // phase's failures and discards are that phase's.
    let mut reoffer = submit_record("steady_state", Outcome::Accepted);
    reoffer.reoffer = true;
    let mut block = submit_record("steady_state", Outcome::Accepted);
    block.scheduled_block = true;
    collected.apply(submit(reoffer));
    collected.apply(submit(block));
    collected.apply(failure(
        "steady_state",
        FailureKind::ScheduledBlock,
        false,
        "scheduled block: no block solution found under job job-1",
    ));
    collected.apply(failure(
        "steady_state",
        FailureKind::Line,
        false,
        "unparseable line",
    ));
    collected.apply(failure(
        "slow_database",
        FailureKind::Offer,
        false,
        "no current job to mine",
    ));
    collected.apply(Event::DiscardedOffer {
        session: 2,
        phase: "slow_database".into(),
    });

    let account = offer_accounting("steady_state", 5, &collected);
    assert_eq!(account["dispatched"], json!(5));
    assert_eq!(account["submits_recorded"], json!(3));
    assert_eq!(account["offers_discarded"], json!(1));
    assert_eq!(account["offers_failed_before_a_submit"], json!(1));
    assert_eq!(
        account["offers_failed_at_the_write"],
        json!(1),
        "listed apart: it is already a submit record"
    );
    assert_eq!(account["unaccounted"], json!(0), "{account}");
    assert_eq!(account["client_failures"], json!(4));
    assert_eq!(
        account["client_failures_by_kind"],
        json!([
            {"kind": "offer", "count": 2},
            {"kind": "scheduled-block", "count": 1},
            {"kind": "line", "count": 1},
        ])
    );
    assert_eq!(
        account["client_failures_sample"].as_array().map(Vec::len),
        Some(4)
    );
    // A dispatched count nothing explains is reported, not assumed away.
    assert_eq!(
        offer_accounting("steady_state", 7, &collected)["unaccounted"],
        json!(2)
    );
    let other = offer_accounting("slow_database", 2, &collected);
    assert_eq!(other["submits_recorded"], json!(0));
    assert_eq!(other["offers_discarded"], json!(1));
    assert_eq!(other["offers_failed_before_a_submit"], json!(1));
    assert_eq!(other["unaccounted"], json!(0));
    let clean = offer_accounting("warm_up", 0, &collected);
    assert_eq!(clean["client_failures"], json!(0));
    assert_eq!(clean["unaccounted"], json!(0));

    // The run-wide count and kinds, for the client block.
    assert_eq!(collected.failures.len(), 5);
    assert_eq!(collected.discarded_offers, 2);
    assert_eq!(
        failures_by_kind(&collected.failures),
        json!([
            {"kind": "offer", "count": 3},
            {"kind": "scheduled-block", "count": 1},
            {"kind": "line", "count": 1},
        ])
    );

    // The printed summary says so where the offered count is, and only
    // when there is something to say.
    let report = json!({"phases": [
        {"name": "steady_state", "dispatched": 5, "offer_accounting": account},
        {"name": "warm_up", "dispatched": 0, "offer_accounting": clean},
    ]});
    let withheld = Evidence::Withheld {
        reason: "the run aborted: load-fe-1 exited".into(),
        stale_artifact_removed: false,
    };
    let text = summary_text(&report, &withheld, &[]);
    assert!(
        text.lines()
            .any(|line| line.contains("offer accounting for steady_state")
                && line.contains("submits_recorded=3")
                && line.contains("offers_failed_before_a_submit=1")
                && line.contains("client_failures=4")
                && line.contains("unaccounted=0")),
        "{text}"
    );
    assert!(!text.contains("offer accounting for warm_up"), "{text}");
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

/// A refusal is not an acknowledgement. The server answers one in
/// microseconds because it never reached PostgreSQL for it, so summarising
/// refusals with the acknowledgements pulled a phase's p50 to 0.7 ms while
/// every accepted share was taking seconds -- far enough for an artifact to
/// pass the ACK p99 limit its accepted shares were over. The ACK summary is
/// over accepted shares only, per phase and overall, and the refusals are
/// summarised apart so their timing is still visible.
#[test]
fn ack_latency_is_the_latency_of_acknowledgements_only() {
    use qbit_prism_load::client::Outcome;
    let mut records: Vec<client::SubmitRecord> = Vec::new();
    fn push(records: &mut Vec<client::SubmitRecord>, phase: &str, latency: f64, outcome: Outcome) {
        let mut record = submit_record(phase, outcome);
        record.share_id = format!("{phase}:{latency}");
        record.latency_millis = Some(latency);
        records.push(record);
    }
    // slow_database: two acknowledgements in the seconds, ninety-eight
    // refusals under a millisecond, as #324's reproduction produced.
    push(&mut records, "slow_database", 2_400.0, Outcome::Accepted);
    push(&mut records, "slow_database", 3_100.0, Outcome::Accepted);
    for index in 0..98 {
        push(
            &mut records,
            "slow_database",
            0.5 + index as f64 * 0.01,
            Outcome::Rejected(rejection(
                20,
                Some("backend-unavailable"),
                "current chain state is unavailable",
            )),
        );
    }
    // A submit that got no answer has no latency to report either way.
    push(
        &mut records,
        "slow_database",
        0.0,
        Outcome::NoResponse {
            reason: "socket closed".into(),
        },
    );
    records.last_mut().unwrap().latency_millis = None;
    // A re-offer's answer belongs to the kill scenario, not the phase.
    push(&mut records, "slow_database", 9_000.0, Outcome::Accepted);
    records.last_mut().unwrap().reoffer = true;
    // Another phase, fast and clean.
    push(&mut records, "steady_state", 4.0, Outcome::Accepted);
    push(&mut records, "steady_state", 6.0, Outcome::Accepted);
    push(
        &mut records,
        "steady_state",
        0.2,
        Outcome::Rejected(rejection(21, Some("stale-job"), "stale")),
    );

    let slow = run::ack_latency(&records, &["slow_database"]);
    assert_eq!(slow.samples, 2, "two acknowledgements, nothing else");
    assert_eq!(slow.p50, Some(2_400.0));
    assert_eq!(slow.p99, Some(3_100.0));
    assert_eq!(slow.max, Some(3_100.0));

    let overall = run::ack_latency(&records, &["steady_state", "slow_database"]);
    assert_eq!(overall.samples, 4);
    assert_eq!(overall.max, Some(3_100.0));
    assert!(
        overall.p99.expect("a p99") >= 2_400.0,
        "the overall p99 is an acknowledgement's latency, not a refusal's"
    );
    assert_eq!(run::ack_latency(&records, &["warm_up"]).samples, 0);

    let refused = run::rejection_latency(&records, "slow_database");
    assert_eq!(refused.samples, 98, "the refusals are summarised apart");
    assert!(refused.max.expect("a max") < 2.0);
    assert_eq!(run::rejection_latency(&records, "steady_state").samples, 1);
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

/// The lock sampler attributes rows by `application_name` only when every
/// frontend's was seen in `pg_stat_activity`; otherwise every waiter is
/// counted and the report says why. A `pg_stat_activity` that could not be
/// read used to arrive as an empty list -- what "no frontend carried its
/// name" looks like -- so the report blamed the driver for the sampler's own
/// blindness.
#[test]
fn an_unreadable_pg_stat_activity_is_named_as_the_reason_every_waiter_is_counted() {
    let frontends = vec!["load-fe-0".to_owned(), "load-fe-1".to_owned()];
    let (names, reason) = run::lock_attribution(&frontends, Ok(frontends.clone()));
    assert_eq!(names, frontends);
    assert!(reason.contains("seen in pg_stat_activity"), "{reason}");

    let (names, reason) = run::lock_attribution(
        &frontends,
        Ok(vec!["load-fe-0".to_owned(), "psql".to_owned()]),
    );
    assert!(names.is_empty());
    assert!(reason.contains("for 1 of 2 frontends"), "{reason}");
    assert!(
        reason.contains("every PRISM advisory-lock waiter"),
        "{reason}"
    );

    let (names, reason) = run::lock_attribution(
        &frontends,
        Err("permission denied for view pg_stat_activity".into()),
    );
    assert!(names.is_empty());
    assert!(
        reason.contains("could not be read") && reason.contains("permission denied"),
        "{reason}"
    );
    assert!(reason.contains("is unknown"), "{reason}");
    assert!(
        !reason.contains("for 0 of 2"),
        "an unreadable view is not a driver that carried no name: {reason}"
    );
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

/// `postgresql://user@[::1]/db` is a URL SQLx accepts, so the proxy has to
/// front it too. Testing the authority for a colon took the colons inside
/// the brackets for a port, returned `[::1]` with none, and the run failed
/// at the proxy's lookup after SQLx had already connected. The port is the
/// one outside the closing bracket, or the default when there is none, and
/// the URL is read with the parser that accepted it rather than a second
/// one that disagrees with it at the edges.
#[test]
fn a_bracketed_ipv6_authority_keeps_or_gains_its_port() -> Result<()> {
    // Bracketed, without and with a port; the port is the one after the
    // bracket, never a colon inside it.
    assert_eq!(run::host_port("postgresql://user@[::1]/db")?, "[::1]:5432");
    assert_eq!(
        run::host_port("postgresql://user:pw@[::1]:6000/db?sslmode=disable")?,
        "[::1]:6000"
    );
    assert_eq!(
        run::host_port("postgresql://[2001:db8::10]:5433/db")?,
        "[2001:db8::10]:5433"
    );
    // The hostname and IPv4 forms are unchanged.
    assert_eq!(run::host_port("postgresql://u@host/db")?, "host:5432");
    assert_eq!(run::host_port("postgresql://u@host:6000/db")?, "host:6000");
    assert_eq!(
        run::host_port("postgresql://u@10.0.0.5:5433/db")?,
        "10.0.0.5:5433"
    );
    // The libpq-style parameters SQLx honours are honoured here too, since
    // they name what SQLx actually dialled.
    assert_eq!(
        run::host_port("postgres:///db?host=db.internal&port=5433")?,
        "db.internal:5433"
    );
    // What the proxy cannot front is refused with the reason, not resolved. A
    // URL with no host is deliberately not asserted to a value: SQLx's default
    // is platform-dependent, a socket directory on macOS and a TCP host
    // elsewhere, so pinning one of them would pass on the machine it was written
    // on and fail on the next. What must hold everywhere is that a socket is
    // refused, whether it arrives as `socket` or as a path-shaped host.
    let socket = run::host_port("postgresql:///db?host=/var/run/postgresql").unwrap_err();
    assert!(format!("{socket:#}").contains("Unix socket"), "{socket:#}");
    let default_host = run::host_port("postgresql:///db");
    match default_host {
        Ok(value) => assert!(
            !value.starts_with('/'),
            "a frontable default must be a TCP host, got {value}"
        ),
        Err(error) => assert!(
            format!("{error:#}").contains("Unix socket"),
            "an unfrontable default must say why: {error:#}"
        ),
    }
    let junk = run::host_port("not-a-url").unwrap_err();
    assert!(
        format!("{junk:#}").contains("parsing the database URL"),
        "{junk:#}"
    );
    // The proxied URL composes with the rewrite the run applies afterwards.
    assert_eq!(
        run::rewrite_host("postgresql://user@[::1]/db", "127.0.0.1:2")?,
        "postgresql://user@127.0.0.1:2/db"
    );
    Ok(())
}

/// SQLx reads the libpq-style `host`, `hostaddr` and `port` parameters after
/// the authority and lets them win. A rewrite that replaced the authority and
/// kept the query string therefore left `postgresql:///db?host=db.internal`
/// pointing at `db.internal`: every frontend dialled the database directly,
/// the proxy carried nothing, and the `slow_database` phase reported a 10 ms
/// delay nothing had applied. The endpoint parameters are dropped, whatever
/// their spelling, and every other option is kept as written.
#[test]
fn endpoint_parameters_never_survive_the_rewrite() -> Result<()> {
    use sqlx::postgres::PgConnectOptions;
    // What SQLx would dial, read with SQLx's own parser: the only judge of
    // whether the rewrite reaches the proxy.
    let dialled = |url: &str| -> Result<(String, u16)> {
        let options: PgConnectOptions = url.parse()?;
        Ok((options.get_host().to_owned(), options.get_port()))
    };
    let proxy = "127.0.0.1:9999";

    // The three parameters, alone and together, with and without an authority.
    for url in [
        "postgresql:///db?host=db.internal&port=5433",
        "postgresql://db.internal:5433/db?host=db.internal&port=5433",
        "postgresql://u:p@127.0.0.1:5432/db?hostaddr=10.0.0.5",
        "postgresql://u@10.0.0.5/db?port=5433",
        "postgresql:///db?h%6Fst=db.internal&p%6Frt=5433",
    ] {
        let rewritten = run::rewrite_host(url, proxy)?;
        assert_eq!(
            dialled(&rewritten)?,
            ("127.0.0.1".to_owned(), 9999),
            "{url} rewrote to {rewritten}, which SQLx would not dial through the proxy"
        );
        for parameter in run::ENDPOINT_PARAMETERS {
            assert!(
                !rewritten.contains(&format!("{parameter}=")),
                "{rewritten} still carries {parameter}"
            );
        }
    }

    // Unrelated options survive in their order, and nothing is invented.
    assert_eq!(
        run::rewrite_host(
            "postgresql://u:p@db.internal:5433/db?sslmode=disable&host=db.internal&\
             application_name=x&port=5433&options=-c%20statement_timeout%3D5s",
            proxy
        )?,
        "postgresql://u:p@127.0.0.1:9999/db?sslmode=disable&application_name=x&\
         options=-c%20statement_timeout%3D5s"
    );
    assert_eq!(
        run::rewrite_host("postgresql:///db?host=db.internal", proxy)?,
        "postgresql://127.0.0.1:9999/db",
        "a query left empty is dropped rather than left as a bare `?`"
    );
    assert_eq!(
        run::rewrite_host("postgresql://u@h:1/db?hostname=x&porter=y", proxy)?,
        "postgresql://u@127.0.0.1:9999/db?hostname=x&porter=y",
        "only the exact names are endpoint parameters"
    );
    // An `@` inside the query is not user info.
    assert_eq!(
        run::rewrite_host("postgresql://h:1/db?options=-c%20a=b@c", proxy)?,
        "postgresql://127.0.0.1:9999/db?options=-c%20a=b@c"
    );
    // The rewrite composes with the parameter the run adds afterwards.
    assert_eq!(
        run::with_application_name(
            &run::rewrite_host("postgresql:///db?host=db.internal", proxy)?,
            "load-fe-0"
        ),
        "postgresql://127.0.0.1:9999/db?application_name=load-fe-0"
    );
    Ok(())
}

/// The rewrite is checked at run time as well: a round trip through the URL
/// the frontends were given, with a delay set, must cost at least twice the
/// delay, because each direction is held once and the proxy's sleep never
/// returns early. A trip that comes back sooner did not go through the proxy,
/// and the run refuses to attribute a delay to it.
/// The round-trip measurement's connection error is written into the failure
/// side report when the entry check cannot connect, and into a phase's
/// `database_delay_observation_error` when the per-phase check cannot. It
/// named the URL as written, password and all: a report meant to be attached
/// to an issue carried the credential the environment block had already been
/// redacted of. The error names the endpoint and never the password.
#[tokio::test]
async fn the_round_trip_measurement_error_never_carries_the_password() {
    let password = "hunter2-Sup3r_Secret";
    // Port 1 on the loopback interface refuses at once on every platform the
    // harness runs on: nothing listens there.
    let url = format!("postgresql://alex:{password}@127.0.0.1:1/qbit?sslmode=disable");
    let error = proxy::measure_select1_millis(&url, 1)
        .await
        .expect_err("nothing listens on port 1");
    let text = format!("{error:#}");
    assert!(
        !text.contains(password),
        "the error carries the password: {text}"
    );
    assert!(
        text.contains("postgresql://alex:<redacted>@127.0.0.1:1/qbit?sslmode=disable"),
        "the error still names the endpoint it could not reach: {text}"
    );
    // The query-parameter form of the password goes the same way.
    let url = format!("postgresql://127.0.0.1:1/qbit?password={password}");
    let error = proxy::measure_select1_millis(&url, 1)
        .await
        .expect_err("nothing listens on port 1");
    let text = format!("{error:#}");
    assert!(
        !text.contains(password),
        "the error carries the password: {text}"
    );
}

#[test]
fn a_round_trip_that_did_not_pay_the_delay_is_refused() {
    assert_eq!(run::delay_floor_millis(10), 20.0);
    assert_eq!(run::delay_floor_millis(0), 0.0);
    assert!(run::check_delay_observed(10, 20.0).is_ok());
    assert!(run::check_delay_observed(10, 23.7).is_ok());
    assert!(run::check_delay_observed(0, 0.08).is_ok());
    let refused = run::check_delay_observed(10, 0.31).unwrap_err();
    let text = format!("{refused:#}");
    assert!(text.contains("0.310 ms"), "{text}");
    assert!(text.contains("10 ms one-way delay"), "{text}");
    assert!(text.contains("20 ms floor"), "{text}");
    assert!(text.contains("not going through the delay proxy"), "{text}");
    assert!(run::check_delay_observed(10, 19.99).is_err());
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

/// The mid-flight kill was awaited inside the 1 ms scheduling loop: across
/// the wait for work, the kill, the relaunch, the readiness wait, a three
/// second settle, the re-offers and another three seconds no other frontend
/// was offered anything, and the next tick turned the gap into a burst plus
/// a shortfall -- the defect the drained restart had been fixed for twice,
/// in the one place it had not been. It is driven the same way now: every
/// poll returns within a few milliseconds while the whole kill takes
/// seconds; the killed frontend's sessions are retargeted once it answers
/// and then sent a re-offer for each no-response the kill produced; the
/// other frontend's sessions are never touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mid_flight_kill_never_stalls_the_scheduler() -> Result<()> {
    use qbit_prism_load::client::Outcome;
    use qbit_prism_load::kill::{KillDriver, RELAUNCH_DELAY, SETTLE};
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};
    let dir = ScratchDir::new("kill");
    let server = stand_in_server(dir.path(), "PRISM listening (stand-in)");
    let (audit_port, _audit) = stand_in_audit_port().await;
    let mut frontends = vec![
        stand_in_frontend(&server, dir.path(), 0, audit_port),
        stand_in_frontend(&server, dir.path(), 1, audit_port),
    ];
    let first_pid = frontends[1].pid().expect("the stand-in is running");
    let (healthy, mut healthy_control) = detached_session(0, 0, 1);
    let (victim, mut victim_control) = detached_session(1, 1, 2);
    let sessions = vec![healthy, victim];
    // One submit was already recorded before the kill began; it is not the
    // kill's, whatever its outcome.
    let collected = Arc::new(Mutex::new(run::Collected::default()));
    let earlier = client::SubmitRecord {
        session: 1,
        frontend: 1,
        ..submit_record(
            "mid_flight_kill",
            Outcome::NoResponse {
                reason: "before the kill".into(),
            },
        )
    };
    collected
        .lock()
        .unwrap()
        .apply(client::Event::Submit(Box::new(earlier)));

    let mut driver = KillDriver::start(1, Duration::from_secs(20), &collected);
    let started = Instant::now();
    let mut longest_poll = Duration::ZERO;
    let mut polls = 0usize;
    let mut reported = false;
    let record = loop {
        let poll_started = Instant::now();
        let progress = driver.poll(&sessions, &mut frontends, &[], &collected)?;
        longest_poll = longest_poll.max(poll_started.elapsed());
        polls += 1;
        if let Some(record) = progress {
            break record;
        }
        // Once the process is gone, the victim's session reports the
        // no-response the kill produced, as the real session's reader would
        // on end of stream; a session on the other frontend closing its
        // own socket in the same window is not the kill's.
        if !reported && frontends[1].restarts >= 1 {
            reported = true;
            let mut state = collected.lock().unwrap();
            let lost = client::SubmitRecord {
                share_id: "pload1abc.s00001:lost".into(),
                session: 1,
                frontend: 1,
                ..submit_record(
                    "mid_flight_kill",
                    Outcome::NoResponse {
                        reason: "socket closed: end of stream".into(),
                    },
                )
            };
            let other = client::SubmitRecord {
                share_id: "pload1abc.s00000:other".into(),
                session: 0,
                frontend: 0,
                ..submit_record(
                    "mid_flight_kill",
                    Outcome::NoResponse {
                        reason: "socket closed: end of stream".into(),
                    },
                )
            };
            state.apply(client::Event::Submit(Box::new(lost)));
            state.apply(client::Event::Submit(Box::new(other)));
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the kill did not complete"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    let total = started.elapsed();
    assert!(
        total >= RELAUNCH_DELAY + SETTLE,
        "the kill paused before the relaunch and settled after the retarget: {total:?}"
    );
    assert!(
        longest_poll < Duration::from_millis(100),
        "no single poll may stall the scheduler; the longest took {longest_poll:?} over \
         {polls} polls while the kill took {total:?}"
    );
    assert!(
        polls > 100,
        "the scheduler kept running during the kill: {polls} polls"
    );
    assert_eq!(record.index, 1);
    assert_eq!(
        record.outstanding_at_kill, 2,
        "the victim held work when it was killed"
    );
    assert_eq!(
        record
            .indeterminate
            .iter()
            .map(|r| r.share_id.as_str())
            .collect::<Vec<_>>(),
        vec!["pload1abc.s00001:lost"],
        "the census is the killed frontend's own no-responses since the kill"
    );
    assert_eq!(frontends[1].restarts, 1);
    assert_ne!(
        frontends[1].pid(),
        Some(first_pid),
        "a new process is running"
    );
    assert!(frontends[1].exited().is_none(), "and it is up");
    assert_eq!(frontends[0].restarts, 0);
    match victim_control.try_recv() {
        Ok(client::Control::Retarget {
            frontend: 1,
            reconnect: false,
            ..
        }) => {}
        other => panic!("the victim's session is retargeted once the relaunch answers: {other:?}"),
    }
    match victim_control.try_recv() {
        Ok(client::Control::Reoffer { share_id, .. }) => {
            assert_eq!(share_id, "pload1abc.s00001:lost");
        }
        other => panic!("then sent the re-offer for its lost share: {other:?}"),
    }
    assert!(
        victim_control.try_recv().is_err(),
        "nothing else is sent to it"
    );
    assert!(
        healthy_control.try_recv().is_err(),
        "the healthy frontend's sessions are never retargeted or re-offered"
    );
    assert_eq!(
        sessions[0].outstanding.load(Ordering::Relaxed),
        1,
        "the driver never touches a counter"
    );
    for child in frontends.iter_mut() {
        child.kill();
    }
    Ok(())
}

/// A kill whose relaunch never answers is reported as a failure the phase
/// aborts on, not waited for past its limit, and the scheduler is not
/// stalled while the limit runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kill_whose_relaunch_never_answers_is_reported_within_its_limit() -> Result<()> {
    use qbit_prism_load::kill::KillDriver;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};
    let dir = ScratchDir::new("kill-unready");
    let server = stand_in_server(dir.path(), "PRISM listening (stand-in)");
    // No audit port answers: the relaunch never becomes ready.
    let mut frontends = vec![
        stand_in_frontend(&server, dir.path(), 0, 1),
        stand_in_frontend(&server, dir.path(), 1, 1),
    ];
    let (victim, _victim_control) = detached_session(1, 1, 1);
    let sessions = vec![victim];
    let collected = Arc::new(Mutex::new(run::Collected::default()));
    let mut driver = KillDriver::start(1, Duration::from_millis(800), &collected);
    let started = Instant::now();
    let mut longest_poll = Duration::ZERO;
    let error = loop {
        let poll_started = Instant::now();
        match driver.poll(&sessions, &mut frontends, &[], &collected) {
            Ok(None) => {}
            Ok(Some(record)) => panic!("an unready relaunch must not complete: {record:?}"),
            Err(error) => break format!("{error:#}"),
        }
        longest_poll = longest_poll.max(poll_started.elapsed());
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the limit was not applied"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    assert!(error.contains("did not become ready"), "{error}");
    assert!(error.contains("mid-flight kill"), "{error}");
    assert!(
        longest_poll < Duration::from_millis(100),
        "the longest poll took {longest_poll:?}"
    );
    assert_eq!(frontends[1].restarts, 1, "the relaunch was attempted");
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
/// The proxy reads its delay per chunk, so a submit still in flight when a
/// phase boundary changes the delay finishes under the next phase's delay
/// while keeping the phase stamp it was offered with. The boundary therefore
/// waits for everything outstanding to settle before it touches the delay,
/// leaves the delay alone when nothing settles in time, and does nothing at
/// all when the delay is not changing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_phase_delay_is_applied_only_once_the_previous_phase_has_settled() -> Result<()> {
    use qbit_prism_load::proxy::DelayProxy;
    use qbit_prism_load::run::{apply_phase_delay, settle_outstanding, DelayChange};
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    // A numeric upstream needs no lookup, and nothing connects to it.
    let proxy = DelayProxy::open("127.0.0.1:1").await?;
    let (settled, _control_a) = detached_session(0, 0, 0);
    let (in_flight, _control_b) = detached_session(1, 0, 2);
    let sessions = [settled, in_flight];

    // Same delay: nothing to wait for, whatever is outstanding.
    assert_eq!(
        apply_phase_delay(&proxy, &sessions, 0, Duration::from_millis(50)).await,
        DelayChange::Unchanged
    );

    // A change waits. The delay is still the old one while submits are
    // outstanding, and goes on the instant they settle.
    let outstanding = sessions[1].outstanding.clone();
    let releaser = tokio::spawn({
        let outstanding = outstanding.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            outstanding.store(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            outstanding.store(0, Ordering::SeqCst);
        }
    });
    let started = std::time::Instant::now();
    let change = apply_phase_delay(&proxy, &sessions, 10, Duration::from_secs(10)).await;
    let waited = started.elapsed();
    releaser.await?;
    assert_eq!(change, DelayChange::Applied);
    assert_eq!(
        proxy.delay_millis(),
        10,
        "the delay is on once nothing is outstanding"
    );
    assert!(
        waited >= Duration::from_millis(400),
        "the boundary waited for the last submit to settle, not just the first: {waited:?}"
    );

    // Submits that never settle: the delay stays where it was, and the
    // caller learns how many are still out.
    outstanding.store(3, Ordering::SeqCst);
    let change = apply_phase_delay(&proxy, &sessions, 0, Duration::from_millis(200)).await;
    assert_eq!(change, DelayChange::Refused { outstanding: 3 });
    assert_eq!(
        proxy.delay_millis(),
        10,
        "a delay is never changed under submits offered under the old one"
    );
    assert_eq!(
        settle_outstanding(&sessions, Duration::from_millis(50)).await,
        3
    );
    outstanding.store(0, Ordering::SeqCst);
    assert_eq!(
        settle_outstanding(&sessions, Duration::from_millis(50)).await,
        0
    );
    Ok(())
}

/// `DRAIN_MARGIN` was 5 s and the server's `share_commit_grace` is also
/// 5 s, so every drain the harness derived gave up at exactly the moment
/// the server could still legitimately answer, with nothing left for
/// transit or scheduling. The margin is now strictly greater than the
/// grace. The server does not export the grace, so the harness restates it;
/// this test reads the server's source to keep the restatement honest.
#[test]
fn the_drain_margin_is_wider_than_the_server_s_commit_grace() -> Result<()> {
    use std::time::Duration;
    assert!(
        run::DRAIN_MARGIN > run::SERVER_SHARE_COMMIT_GRACE,
        "the margin ({:?}) must leave time past the grace ({:?}) for the answer to cross the \
         socket and be read",
        run::DRAIN_MARGIN,
        run::SERVER_SHARE_COMMIT_GRACE
    );
    assert!(
        run::DRAIN_MARGIN >= run::SERVER_SHARE_COMMIT_GRACE + Duration::from_secs(1),
        "at least a whole second past the grace"
    );
    assert_eq!(
        run::drain_limit(15.0),
        Duration::from_secs(15) + run::DRAIN_MARGIN,
        "every drain the harness derives carries the margin"
    );

    // The server's value, from its own source: `Config::from_env` sets it
    // as a literal, not from the environment.
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../qbit-prism-server/src/config.rs"
    ))?;
    let marker = "let share_commit_grace = Duration::from_secs(";
    let start = source
        .find(marker)
        .expect("the server sets share_commit_grace with Duration::from_secs in Config::from_env")
        + marker.len();
    let literal: String = source[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    let grace: u64 = literal
        .parse()
        .expect("share_commit_grace is set from a whole number of seconds");
    assert_eq!(
        Duration::from_secs(grace),
        run::SERVER_SHARE_COMMIT_GRACE,
        "the harness restates the server's share_commit_grace; update \
         SERVER_SHARE_COMMIT_GRACE and keep DRAIN_MARGIN ahead of it"
    );
    Ok(())
}

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
    let failures = vec![client::ClientFailure {
        session: 0,
        phase: "dense_cadence".to_owned(),
        kind: client::FailureKind::ScheduledBlock,
        recorded: false,
        error: "scheduled block: no block solution found under job job-0".to_owned(),
        at: at(base, 23_100),
    }];
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

/// A frontend that has not served the new revision by the time the next
/// landing's tip changes must be reported as such. The search for the first
/// `clean_jobs` job used to run to the end of the phase, so the *next*
/// landing's own clean_jobs notify was selected and reported as this
/// landing's new-revision work: time_to_new_revision_work was understated
/// and rejected_before_new_revision_work stopped at the wrong event. The
/// search now ends at the landing's span, and an empty result is its own
/// outcome with its own reason.
#[test]
fn new_revision_work_is_never_borrowed_from_the_next_landing() {
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
    ];
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
            HASH_ONE,
            1,
            0,
            at(base, 14_100),
            at(base, 14_200),
            client::Outcome::Accepted,
            true,
        ),
        // Landing 0's span: two payout-pending rejections after its bump,
        // and the frontend never serves the new revision before landing 1.
        dense_submit(
            &"2".repeat(64),
            2,
            0,
            at(base, 7_900),
            at(base, 8_000),
            pending(classify::NEW_PAYOUT_WORK_PENDING),
            false,
        ),
        dense_submit(
            &"3".repeat(64),
            2,
            0,
            at(base, 11_900),
            at(base, 12_000),
            pending(classify::NEW_PAYOUT_WORK_PENDING),
            false,
        ),
        // Landing 1's span: one rejection before its new-revision job.
        dense_submit(
            &"4".repeat(64),
            2,
            0,
            at(base, 16_600),
            at(base, 16_700),
            pending(classify::NEW_PAYOUT_WORK_PENDING),
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
            bump(5, Some(4), at(base, 7_500)),
            bump(6, Some(5), at(base, 16_500)),
        ],
    };
    // The only clean_jobs job on the frontend arrives after landing 1's tip
    // change: it is landing 1's work, 300 ms after landing 1's bump.
    let notifies = vec![client::NotifySighting {
        session: 2,
        frontend: 0,
        job_id: "job-c".into(),
        tip: HASH_ONE.to_owned(),
        clean_jobs: true,
        at: at(base, 16_800),
    }];
    let tips = Vec::new();
    let failures = Vec::new();
    let frontends = vec![health(0)];
    let session_frontend = vec![0usize, 0, 0];
    let gaps = vec![9.0];
    let offsets = vec![5.0, 14.0];
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
        landing_budget: 2,
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
    assert_eq!(document["landings"], json!(2));

    // Landing 0: its bump was attributed, but no job at the new revision was
    // seen inside its span. That is the outcome, with its reason; the next
    // landing's job at 16.8 s is not reported as 9.3 s of this landing's
    // rebuild, and the two rejections are not counted against it.
    let first = &document["landing_records"][0]["frontends"][0];
    assert_eq!(first["reference_bump"]["revision"], json!(5));
    assert_eq!(first["sessions_with_new_revision_work"], json!(0));
    assert_eq!(
        first["time_to_new_revision_work_millis"]["samples"],
        json!(0)
    );
    assert!(first["time_to_new_revision_work_millis"]["max"].is_null());
    assert_eq!(
        first["new_revision_work_unavailable_reason"],
        json!(cadence::NO_NEW_REVISION_WORK_IN_SPAN)
    );
    assert!(
        first["rejected_before_new_revision_work"].is_null(),
        "no new-revision work in the span, so nothing to count up to: {}",
        first["rejected_before_new_revision_work"]
    );
    assert_eq!(
        first["rejected_before_new_revision_work_unavailable_reason"],
        json!(cadence::NO_NEW_REVISION_WORK_IN_SPAN)
    );
    assert_eq!(
        first["payout_pending_window"]["count"],
        json!(2),
        "the window itself is still measured"
    );

    // Landing 1: its own job, 300 ms after its own bump, with one rejection
    // before it.
    let second = &document["landing_records"][1]["frontends"][0];
    assert_eq!(second["reference_bump"]["revision"], json!(6));
    assert_eq!(second["sessions_with_new_revision_work"], json!(1));
    assert_eq!(
        second["time_to_new_revision_work_millis"]["max"],
        json!(300.0)
    );
    assert!(second["new_revision_work_unavailable_reason"].is_null());
    assert_eq!(second["rejected_before_new_revision_work"], json!(1));

    // The summaries hold landing 1's figures only: nothing was invented for
    // landing 0.
    let overall = &document["summaries"]["overall"];
    assert_eq!(
        overall["time_to_new_revision_work_max_millis"]["samples"],
        json!(1)
    );
    assert_eq!(
        overall["time_to_new_revision_work_max_millis"]["max"],
        json!(300.0)
    );
    assert_eq!(
        overall["rejected_before_new_revision_work_per_landing"]["samples"],
        json!(1)
    );
    assert_eq!(
        overall["rejected_before_new_revision_work_per_landing"]["max"],
        json!(1.0)
    );
    assert!(
        document["definitions"]["time_to_new_revision_work"]
            .as_str()
            .expect("a definition")
            .contains("before the end of the landing's span"),
        "the definition says where the search stops"
    );
}

/// The new-tip search is bounded by the landing's span, as the new-revision
/// search beside it has been since H4. It was not: a session's first notify
/// for a landing's tip that arrived after the next landing's tip change --
/// a late job for a tip already replaced -- satisfied the search, and the
/// earlier landing reported a session with new-tip work at a time that was
/// really inside the next landing's span. A frontend with no such job inside
/// the span is now its own outcome, with its own reason.
#[test]
fn new_tip_work_is_never_borrowed_from_the_next_landing() {
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
    ];
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
            HASH_ONE,
            1,
            0,
            at(base, 14_100),
            at(base, 14_200),
            client::Outcome::Accepted,
            true,
        ),
    ];
    let node_submissions = vec![
        node_submission(HASH_ZERO, 104, true),
        node_submission(HASH_ONE, 105, true),
    ];
    // Landing 0's span is 7.0 s to 16.0 s; landing 1's runs from 16.0 s.
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
        changes: Vec::new(),
    };
    // Sessions 0..=2 are on frontend 0, session 3 on frontend 1.
    let sighting = |session: usize, frontend: usize, tip: &str, millis: u64| client::TipSighting {
        session,
        frontend,
        tip: tip.to_owned(),
        at: at(base, millis),
    };
    let tips = vec![
        // Frontend 0 served landing 0's tip to session 1 inside the span.
        sighting(1, 0, HASH_ZERO, 7_400),
        // Session 2's first job on landing 0's tip arrives after landing 1's
        // tip change: a late job for a replaced tip.
        sighting(2, 0, HASH_ZERO, 16_500),
        // Frontend 1 never served landing 0's tip inside the span; its only
        // job on it is the same kind of late arrival.
        sighting(3, 1, HASH_ZERO, 16_500),
        // Landing 1's own work, on both frontends.
        sighting(0, 0, HASH_ONE, 16_300),
        sighting(3, 1, HASH_ONE, 16_600),
    ];
    let notifies = Vec::new();
    let failures = Vec::new();
    let frontends = vec![health(0), health(1)];
    let session_frontend = vec![0usize, 0, 0, 1];
    let gaps = vec![9.0];
    let offsets = vec![5.0, 14.0];
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
        landing_budget: 2,
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
    assert_eq!(document["landings"], json!(2));

    // Landing 0, frontend 0: one session saw the tip inside the span, at
    // 400 ms. Session 2's late job is not a second one at 9.5 s.
    let first = &document["landing_records"][0]["frontends"][0];
    assert_eq!(first["frontend"], json!(0));
    assert_eq!(first["sessions_with_new_tip_work"], json!(1));
    assert_eq!(first["time_to_new_tip_work_millis"]["samples"], json!(1));
    assert_eq!(first["time_to_new_tip_work_millis"]["max"], json!(400.0));
    assert!(first["new_tip_work_unavailable_reason"].is_null());

    // Landing 0, frontend 1: nothing inside the span. That is the outcome,
    // with its reason -- not one session at 9.5 s borrowed from the next
    // landing's span.
    let other = &document["landing_records"][0]["frontends"][1];
    assert_eq!(other["frontend"], json!(1));
    assert_eq!(other["sessions_with_new_tip_work"], json!(0));
    assert_eq!(other["time_to_new_tip_work_millis"]["samples"], json!(0));
    assert!(other["time_to_new_tip_work_millis"]["max"].is_null());
    assert_eq!(
        other["new_tip_work_unavailable_reason"],
        json!(cadence::NO_NEW_TIP_WORK_IN_SPAN)
    );

    // Landing 1: its own work on both frontends, 300 ms and 600 ms.
    let second = &document["landing_records"][1]["frontends"];
    assert_eq!(second[0]["sessions_with_new_tip_work"], json!(1));
    assert_eq!(
        second[0]["time_to_new_tip_work_millis"]["max"],
        json!(300.0)
    );
    assert_eq!(second[1]["sessions_with_new_tip_work"], json!(1));
    assert_eq!(
        second[1]["time_to_new_tip_work_millis"]["max"],
        json!(600.0)
    );
    assert!(second[1]["new_tip_work_unavailable_reason"].is_null());

    // The summaries hold the three in-span figures and nothing borrowed:
    // three samples, worst 600 ms, not four with a worst of 9.5 s.
    let overall = &document["summaries"]["overall"]["time_to_new_tip_work_max_millis"];
    assert_eq!(overall["samples"], json!(3));
    assert_eq!(overall["max"], json!(600.0));
    let frontend_one = document["summaries"]["per_frontend"]
        .as_array()
        .expect("per-frontend summaries")
        .iter()
        .find(|summary| summary["frontend"] == json!(1))
        .expect("frontend 1 is summarised");
    assert_eq!(
        frontend_one["time_to_new_tip_work_max_millis"]["samples"],
        json!(1),
        "frontend 1 contributes landing 1's figure only"
    );
    assert!(
        document["definitions"]["time_to_new_tip_work"]
            .as_str()
            .expect("a definition")
            .contains("before the end of the landing's span"),
        "the definition says where the search stops"
    );
}

/// The run-level time-to-usable-work search had the same unbounded shape as
/// the dense section's new-tip search: the first sighting of a tip at or
/// after the tip's stamp, however late. A session whose first job on tip X
/// arrived after the node had already moved to tip Y was credited to X, at
/// a latency that ran into Y's reign, and X's all-sessions figure grew with
/// it. The search now stops at the next tip change on the node.
#[test]
fn usable_work_is_never_credited_after_the_tip_was_replaced() {
    let base = std::time::Instant::now();
    let changes = vec![
        pool_tip(HASH_ZERO, 104, at(base, 1_000)),
        pool_tip(HASH_ONE, 105, at(base, 11_000)),
    ];
    let mut collected = run::Collected::default();
    let sighting = |session: usize, tip: &str, millis: u64| client::TipSighting {
        session,
        frontend: 0,
        tip: tip.to_owned(),
        at: at(base, millis),
    };
    // Session 0 saw tip 104 at 1.3 s; session 1's first job on it arrived
    // at 11.5 s, after the node had moved to tip 105 at 11.0 s.
    collected.apply(client::Event::Tip(sighting(0, HASH_ZERO, 1_300)));
    collected.apply(client::Event::Tip(sighting(1, HASH_ZERO, 11_500)));
    collected.apply(client::Event::Tip(sighting(0, HASH_ONE, 11_200)));
    collected.apply(client::Event::Tip(sighting(1, HASH_ONE, 11_900)));

    let document = run::time_to_usable_work(&changes, &changes, &collected, 2);
    let first = &document["tips"][0];
    assert_eq!(first["tip"], json!(HASH_ZERO));
    assert_eq!(first["replaced_after_milliseconds"], json!(10_000.0));
    assert_eq!(
        first["sessions_with_work"],
        json!(1),
        "session 1's job on tip 104 arrived under tip 105 and is not usable work on 104"
    );
    assert_eq!(first["sessions_without_work_before_replacement"], json!(1));
    assert_eq!(first["latency_milliseconds"]["samples"], json!(1));
    assert_eq!(first["latency_milliseconds"]["max"], json!(300.0));
    assert_eq!(
        first["all_sessions_milliseconds"],
        json!(300.0),
        "the all-sessions figure is over the sessions that got work in the reign"
    );

    // Tip 105 was never replaced: both sessions, the late one at 900 ms.
    let second = &document["tips"][1];
    assert!(second["replaced_after_milliseconds"].is_null());
    assert_eq!(second["sessions_with_work"], json!(2));
    assert_eq!(second["sessions_without_work_before_replacement"], json!(0));
    assert_eq!(second["all_sessions_milliseconds"], json!(900.0));
    assert!(document["definition"]
        .as_str()
        .expect("a definition")
        .contains("before the node's next tip change"));
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

#[test]
fn a_phase_that_acknowledged_nothing_has_no_ack_latency_to_state() {
    // The artifact requires a p50 and a p99 for every phase it names. A phase
    // with no acknowledgements has neither, and the previous code wrote 0.000 --
    // a measurement that was never taken, inside the document whose whole point
    // is to be trustworthy. The run now withholds the artifact instead.
    let empty = measure::summarize(Vec::new(), measure::MILLISECONDS, "client monotonic");
    assert!(empty.p50.is_none() && empty.p99.is_none());
    assert!(
        run::has_no_ack_latency(&empty),
        "a phase with no acknowledgements states no latency"
    );

    let measured = measure::summarize(
        vec![1.0, 2.0, 3.0],
        measure::MILLISECONDS,
        "client monotonic",
    );
    assert!(measured.p50.is_some() && measured.p99.is_some());
    assert!(
        !run::has_no_ack_latency(&measured),
        "a phase with acknowledgements states its latency"
    );
}

#[test]
fn a_tail_the_measurement_window_cut_off_is_not_a_divergence() {
    use qbit_prism_load::run::RunOutcome;
    // A submit still outstanding when the drain expires had its window end
    // underneath it: the server is allowed to answer a moment later, and in
    // production the connection would still be there to carry the answer. A
    // socket the peer closed mid-run is the failure this harness exists to
    // catch. Conflating them would make exit 5 fire on nearly every run under
    // load -- measured: a slow_database ACK p99 of 25.3 s against a 25 s drain
    // -- and stop distinguishing a real divergence from where the run stopped.
    let window_ended = RunOutcome {
        withhold: None,
        durability_findings: 0,
        harness_bug_rejections: 0,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: 8,
        no_response_commits_mid_run: 0,
    };
    assert_eq!(window_ended.exit_code(), run::EXIT_OK);
    assert!(
        window_ended
            .explanation(std::path::Path::new("report.json"))
            .is_some_and(|line| line.contains("measurement window closed")),
        "a cut-off tail is still reported, it just is not a divergence"
    );

    let mid_run = RunOutcome {
        withhold: None,
        durability_findings: 0,
        harness_bug_rejections: 0,
        divergences: 0,
        unknown_outcome_commits: 0,
        no_response_commits: 8,
        no_response_commits_mid_run: 3,
    };
    assert_eq!(mid_run.exit_code(), run::EXIT_ACK_COMMIT_DIVERGENCE);
}

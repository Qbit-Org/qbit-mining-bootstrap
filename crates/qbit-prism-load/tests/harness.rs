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
fn latency_percentiles_report_unknown_rather_than_zero() {
    let empty = qbit_prism_load::measure::summarize(Vec::new(), "client monotonic");
    assert_eq!(empty.samples, 0);
    assert!(empty.p50.is_none() && empty.p99.is_none() && empty.max.is_none());
    assert!(empty.unavailable_reason.is_some());
    let summary =
        qbit_prism_load::measure::summarize((1..=100).map(f64::from).collect(), "client monotonic");
    assert_eq!(summary.p50, Some(50.0));
    assert_eq!(summary.p99, Some(99.0));
    assert_eq!(summary.max, Some(100.0));
    // A real zero is a measurement, not an absence.
    let zeros = qbit_prism_load::measure::summarize(vec![0.0, 0.0], "client monotonic");
    assert_eq!(zeros.p50, Some(0.0));
    assert!(zeros.unavailable_reason.is_none());
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

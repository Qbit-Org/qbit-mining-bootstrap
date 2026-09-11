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
    use qbit_prism_load::measure::{is_own_row, split_lock_rows, LockRow};
    let row = |pid: i32, granted: bool, name: &str| LockRow {
        pid,
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
    assert_eq!(
        document["bumps"],
        json!(3),
        "three bumps followed a landing"
    );
    assert_eq!(document["bump_attribution"]["attributed"], json!(3));
    assert_eq!(
        document["bump_attribution"]["unattributed"],
        json!(1),
        "the bump before the first landing is unattributed, never dropped"
    );
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

    assert_eq!(document["lost_valid_work"]["shares"], json!(4));
    assert_eq!(
        document["lost_valid_work"]["shares_found_in_postgres"],
        json!(0)
    );
    assert!(document["proposed_budget_for_issue_291"]["window_p99_millis"].is_number());
    assert!(document["definitions"]["combined_rebuild_pending_window"].is_string());
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
}

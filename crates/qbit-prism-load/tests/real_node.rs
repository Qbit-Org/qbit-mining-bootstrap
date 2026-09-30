//! Real-node mode (#547), and the proof that fake-node mode did not move.

use anyhow::{Context, Result};
use clap::Parser;
use qbit_prism_load::{
    cli::Args,
    client,
    frontend::{self, FrontendSpec},
    node::NodeState,
    preset, run, window,
};
use qbit_prism_server::codec;
use serde_json::{json, Value};
use std::path::PathBuf;

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

fn request(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "1.0", "id": 42, "method": method, "params": params})
}

fn block_hex(parent_display: &str, nonce: u32) -> String {
    let mut prev = hex::decode(parent_display).expect("parent hex");
    prev.reverse();
    let header = client::assemble_header(
        0x2000_0000,
        &prev,
        &[7u8; 32],
        1_800_000_000,
        codec::parse_u32_hex(window::TEMPLATE_BITS).expect("bits"),
        nonce,
    );
    format!("{}01{}", hex::encode(header), "00".repeat(64))
}

/// A template's clock fields are the wall clock; everything else the fake
/// node answers is a function of the requests alone.
fn normalise(mut answer: Value) -> Value {
    if let Some(template) = answer.get_mut("result").and_then(Value::as_object_mut) {
        if let (Some(cur), Some(min)) = (
            template.get("curtime").and_then(Value::as_i64),
            template.get("mintime").and_then(Value::as_i64),
        ) {
            assert_eq!(min, cur - 1, "mintime trails curtime by one second");
            template.insert("curtime".into(), json!("<now>"));
            template.insert("mintime".into(), json!("<now - 1>"));
        }
    }
    answer
}

/// Every answer the fake node gives to a fixed request corpus, its
/// submission log and its tip log, for the plain and the retargeting node.
async fn fake_node_transcript() -> Result<Value> {
    let mut nodes = Vec::new();
    for retarget in [false, true] {
        let state = NodeState::with_retarget(window::TEMPLATE_BITS, "pload1", retarget);
        let mut answers = Vec::new();
        let mut ask = |answer: Value| answers.push(normalise(answer));
        let (tip, height) = state.tip();
        for (method, params) in [
            ("getblockchaininfo", json!([])),
            ("getnetworkinfo", json!([])),
            ("getbestblockhash", json!([])),
            ("getblocktemplate", json!([{"rules": ["segwit"]}])),
            ("getblockhash", json!([0])),
            ("getblockhash", json!([50])),
            ("getblockhash", json!([height])),
            ("getblockhash", json!([height + 1])),
            ("getblockheader", json!([tip.clone()])),
            ("getblockheader", json!(["00".repeat(32)])),
            ("validateaddress", json!(["pload1deadbeef"])),
            ("validateaddress", json!(["somebody-else"])),
            ("submitblock", json!(["zz"])),
            ("submitblock", json!(["00".repeat(10)])),
            ("submitblock", json!([block_hex(&"aa".repeat(32), 1)])),
            ("submitblock", json!([block_hex(&tip, 2)])),
            ("waitfornewblock", json!([1])),
            ("estimatesmartfee", json!([2])),
            ("getmempoolinfo", json!([])),
            ("getblockcount", json!([])),
        ] {
            ask(state.handle(&request(method, params)).await);
        }
        let minted = state.mint_external_block();
        for (method, params) in [
            ("getblockchaininfo", json!([])),
            ("getblocktemplate", json!([{}])),
            ("getblockheader", json!([minted.hash.clone()])),
            ("getblockhash", json!([minted.height])),
        ] {
            ask(state.handle(&request(method, params)).await);
        }
        let submissions: Vec<Value> = state
            .submissions()
            .iter()
            .map(|record| {
                let mut value = serde_json::to_value(record).expect("submission");
                value.as_object_mut().expect("object").remove("received_at");
                value
            })
            .collect();
        let tips: Vec<Value> = state
            .tip_changes()
            .iter()
            .map(|change| {
                json!({
                    "hash": change.hash, "height": change.height, "origin": change.origin,
                    "next_template_bits": state.template_bits(change.height + 1),
                })
            })
            .collect();
        let mut counts: Vec<(String, u64)> = state.rpc_call_counts().into_iter().collect();
        counts.sort();
        nodes.push(json!({
            "retarget": retarget,
            "answers": answers,
            "submissions": submissions,
            "tip_changes": tips,
            "chainwork": state.chainwork_hex(),
            "rpc_call_counts": counts,
        }));
    }
    Ok(json!(nodes))
}

/// The environment a fake-node run launches its first frontend with, for
/// the pr-smoke preset's arguments.
fn fake_frontend_environment() -> Result<Value> {
    let argv: Vec<std::ffi::OsString> = vec![
        "qbit-prism-load".into(),
        "--preset".into(),
        preset::presets_dir().join("pr-smoke.json").into(),
    ];
    let (expanded, _) = preset::expand_command_line(argv)?;
    let args = Args::try_parse_from(expanded)?;
    let spec = FrontendSpec {
        index: 0,
        instance_id: "load-fe-0".into(),
        stratum_port: 3340,
        audit_port: 3341,
        database_url: "postgresql://u@127.0.0.1:5432/postgres".into(),
    };
    let shared = run::shared_environment(
        &args,
        "http://127.0.0.1:1/".into(),
        "load-0123abcd".into(),
        "0.0000000122070312".into(),
    );
    let environment = run::launch_environment(
        &shared,
        &spec,
        args.pool_fee_bps,
        &frontend::pool_fee_address("pload10123abcd"),
        args.node_mode()?,
    );
    Ok(serde_json::to_value(environment)?)
}

fn check_golden(name: &str, actual: &Value) -> Result<()> {
    let path = golden_dir().join(name);
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let expected: Value = serde_json::from_str(&text)?;
    if let Some(difference) = first_difference(actual, &expected, String::new()) {
        panic!("{name}: fake-node mode no longer matches what origin/3.x.x produced: {difference}");
    }
    Ok(())
}

/// The first place two documents differ, as a JSON pointer with both values.
fn first_difference(actual: &Value, expected: &Value, at: String) -> Option<String> {
    match (actual, expected) {
        (Value::Object(a), Value::Object(e)) => {
            let keys: std::collections::BTreeSet<&String> = a.keys().chain(e.keys()).collect();
            keys.into_iter().find_map(|key| {
                first_difference(
                    a.get(key).unwrap_or(&Value::Null),
                    e.get(key).unwrap_or(&Value::Null),
                    format!("{at}/{key}"),
                )
                .or_else(|| {
                    (a.contains_key(key) != e.contains_key(key))
                        .then(|| format!("{at}/{key}: present on one side only"))
                })
            })
        }
        (Value::Array(a), Value::Array(e)) if a.len() == e.len() => a
            .iter()
            .zip(e)
            .enumerate()
            .find_map(|(index, (a, e))| first_difference(a, e, format!("{at}/{index}"))),
        _ if actual == expected => None,
        _ => Some(format!("{at}: got {actual}, origin/3.x.x had {expected}")),
    }
}

#[tokio::test]
async fn fake_node_mode_is_unchanged() -> Result<()> {
    check_golden("fake_node_transcript.json", &fake_node_transcript().await?)?;
    check_golden(
        "fake_frontend_environment.json",
        &fake_frontend_environment()?,
    )?;
    Ok(())
}

/// `target/<profile>`, the directory this test binary's `deps` sits in.
fn profile_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe()?;
    let deps = exe.parent().context("test binary has no directory")?;
    Ok(deps
        .parent()
        .context("test binary's deps directory has no parent")?
        .to_owned())
}

/// Build the debug server beside this test binary, as `load_smoke` does: a
/// `qbit-prism-load` test cannot ask Cargo for another package's binary. Its
/// environment and package selection match this test binary's build, so the
/// server's library is not rebuilt; see `load_smoke`'s `build_server`.
fn build_server() -> Result<PathBuf> {
    let profile = profile_dir()?;
    let target = profile
        .parent()
        .context("profile directory has no parent")?;
    let mut command = std::process::Command::new(env!("CARGO"));
    for (name, _) in std::env::vars_os() {
        let name = name.to_string_lossy().into_owned();
        if [
            "CARGO_PKG_",
            "CARGO_MANIFEST_",
            "CARGO_CRATE_",
            "CARGO_BIN_",
            "CARGO_PRIMARY_PACKAGE",
            "CARGO_TARGET_TMPDIR",
        ]
        .iter()
        .any(|prefix| name.starts_with(prefix))
        {
            command.env_remove(name);
        }
    }
    let status = command
        .args([
            "build",
            "--locked",
            "-p",
            "qbit-prism-load",
            "-p",
            "qbit-prism-server",
            "--bin",
            "qbit-prism-server",
        ])
        .env("CARGO_TARGET_DIR", target)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .context("running cargo build for qbit-prism-server")?;
    anyhow::ensure!(status.success(), "cargo build qbit-prism-server: {status}");
    let server = profile.join("qbit-prism-server");
    anyhow::ensure!(server.is_file(), "{} was not built", server.display());
    Ok(server)
}

/// A scratch directory removed on drop.
struct TempDir {
    path: PathBuf,
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Each node's debug log after the ramp: the lines not stamped with the
/// ramp's mock clock (the ramp is ~13k blocks of them), the last 400 of each.
fn post_ramp_node_logs(logs: &std::path::Path) -> String {
    let mut text = String::new();
    for name in ["qbitd-a.debug.log", "qbitd-b.debug.log"] {
        let log = match std::fs::read(logs.join(name)) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(error) => format!("{} could not be read: {error}", logs.join(name).display()),
        };
        let lines: Vec<&str> = log
            .lines()
            .filter(|line| !line.contains("(mocktime:"))
            .collect();
        text.push_str(&format!("--- {name}, last lines after the ramp ---\n"));
        for line in &lines[lines.len().saturating_sub(400)..] {
            text.push_str(line);
            text.push('\n');
        }
    }
    text
}

/// The per-PR real-node smoke run (#547): the `real-node-smoke` preset
/// against a managed regtest qbitd pair ramped to the fake node's
/// difficulty. The harness has to exit 0 -- every share reconciled against
/// PostgreSQL, and every block above the ramp one of the peer's mints or the
/// pool's one accepted submission, at the ramped bits -- and the report has
/// to state the cadence band the run was in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_node_smoke_reconciles_against_postgres_and_the_chain() -> Result<()> {
    use qbit_prism_load::{gate, qbitd};
    use qbit_prism_test_gate as test_gate;
    let Some(values) = test_gate::inputs(
        test_gate::site!(),
        &[test_gate::Input::QbitdBin, test_gate::Input::PgBinDir],
    )?
    else {
        return Ok(());
    };
    let (qbitd_bin, pg_bin) = (values[0].clone(), values[1].clone());
    let started = std::time::Instant::now();
    let server = build_server()?;
    let built = started.elapsed();
    let out = TempDir {
        path: std::env::temp_dir().join(format!("prism-load-real-node-{}", uuid::Uuid::new_v4())),
    };
    std::fs::create_dir_all(&out.path)?;
    let argv: Vec<std::ffi::OsString> = vec![
        "qbit-prism-load".into(),
        "--preset".into(),
        preset::presets_dir().join("real-node-smoke.json").into(),
        "--qbitd-bin".into(),
        qbitd_bin.into(),
        "--server-bin".into(),
        server.into(),
        "--pg-bin-dir".into(),
        pg_bin.into(),
        "--out".into(),
        out.path.clone().into(),
        // A debug server from whatever tree the test runs in: a smoke run,
        // never evidence, and the tips plan writes no artifact.
        "--allow-debug-server".into(),
        "--allow-dirty-tree".into(),
        "--allow-unverified-server-revision".into(),
    ];
    let (expanded, loaded) = preset::expand_command_line(argv)?;
    let loaded = loaded.context("the real-node-smoke preset")?;
    let args = Args::try_parse_from(expanded)?;
    let exit = run::execute_with_preset(args, Some(loaded.clone())).await?;
    let ran = started.elapsed() - built;

    let text = std::fs::read_to_string(out.path.join("load-harness-report.json"))?;
    let report: Value = serde_json::from_str(&text)?;
    let checks = gate::evaluate(&report, Some(exit), &gate::Budgets::from(&loaded.gates));
    let table = gate::markdown("real-node smoke", &checks);
    let node = &report["node"];
    eprintln!(
        "{table}\nramp {}\nserver build {built:?}, harness run {ran:?}",
        node["ramp"]
    );
    if !gate::passed(&checks) {
        // The run directory goes with the test, and CI keeps only this
        // output: what the nodes said has to be printed to be seen.
        eprintln!(
            "chain reconciliation: {:#}\n{}",
            node["chain_reconciliation"],
            post_ramp_node_logs(&out.path.join("logs"))
        );
    }
    assert!(gate::passed(&checks), "{table}");
    assert_eq!(exit, run::EXIT_OK);

    assert_eq!(node["mode"], "qbitd");
    assert_eq!(node["template_bits"], qbitd::RAMP_BITS);
    assert_eq!(report["window"]["template_bits"], qbitd::RAMP_BITS);
    let ramp_height = node["ramp"]["height"].as_u64().context("ramp height")?;
    assert_eq!(
        ramp_height,
        qbitd::RAMP_HEIGHT
            + node["ramp"]["catch_up_blocks"]
                .as_u64()
                .context("catch-up")?
    );
    assert_eq!(report["artifact_kind"], "example");
    assert_eq!(
        report["frontend_environment"][0]["environment"]["QBIT_CHAIN"],
        "regtest"
    );
    let chain = &node["chain_reconciliation"];
    assert_eq!(chain["reconciled"], true, "{chain}");
    assert_eq!(
        chain["pool_blocks"], 1,
        "the preset lands one own block: {chain}"
    );
    assert!(
        chain["external_blocks"]
            .as_u64()
            .context("external blocks")?
            >= 3,
        "three tips minted on the peer: {chain}"
    );
    assert_eq!(chain["peer_on_same_tip"], true);
    assert_eq!(report["premise"]["contradicted"], false);
    let submissions = node["submissions"].as_array().context("submissions")?;
    assert_eq!(submissions.len(), 1, "{submissions:?}");
    assert_eq!(submissions[0]["accepted"], true);
    let landed = submissions[0]["height"]
        .as_u64()
        .context("landing height")?;
    assert!(
        landed > ramp_height && landed <= chain["tip_height"].as_u64().context("tip")?,
        "the landing's height comes from its parent on the watched chain: {landed}"
    );
    let templates = node["relay"]["template_bits"]
        .as_object()
        .context("template bits")?;
    assert_eq!(
        templates.keys().collect::<Vec<_>>(),
        vec![qbitd::RAMP_BITS],
        "every template at the ramped bits"
    );
    let pool_tips = node["tip_changes"]
        .as_array()
        .context("tip changes")?
        .iter()
        .filter(|tip| tip["origin"] == "pool")
        .count();
    assert_eq!(pool_tips, 1);
    let tips = report["time_to_usable_work"]["tips"]
        .as_array()
        .context("tips")?;
    assert_eq!(
        tips.len(),
        3,
        "the three warm-up tips, as the pool node saw them"
    );
    for tip in tips {
        assert_eq!(tip["sessions_with_work"], 50, "{tip}");
    }
    let band = &node["cadence_band"];
    assert_eq!(band["band_seconds"], json!([9.0, 600.0]));
    assert_eq!(
        band["phases"][0]["implied_own_block_interval_seconds"],
        125.0
    );
    assert_eq!(band["phases"][0]["in_band"], true);
    Ok(())
}

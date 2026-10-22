//! The checked-in seed corpora, derived from the stratum_protocol and
//! stratum_codec test vectors, and a replay of every seed through its target's
//! checks without libFuzzer. `PRISM_FUZZ_REGENERATE_SEEDS=1 cargo test`
//! rewrites `seeds/`; otherwise the test fails when the files drift from this
//! generator.
use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};
use qbit_prism_server::codec::{self, Job, VERSION_ROLLING_MASK};
use qbit_prism_server_fuzz::{lines, parsers, script};
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf};

type Corpus = BTreeMap<String, Vec<u8>>;

const LOGIN: &str = r#"{"id":1,"method":"mining.subscribe","params":["cgminer/4.10.0"]}
{"id":2,"method":"mining.authorize","params":["miner1.rig","x"]}"#;

fn session_seeds() -> Corpus {
    let submit =
        r#"{"id":10,"method":"mining.submit","params":["$user","$job","$en2","$ntime","$nonce"]}"#;
    let scripts: &[(&str, String)] = &[
        ("login_and_share", format!("{LOGIN}\n{submit}")),
        ("duplicate_share", format!("{LOGIN}\n{submit}\n#again")),
        (
            "version_rolling",
            format!(
                "{}\n{LOGIN}\n{}\n{}",
                r#"{"id":0,"method":"mining.configure","params":[["version-rolling","minimum-difficulty"],{"version-rolling.mask":"1fffe000","version-rolling.min-bit-count":2}]}"#,
                r#"{"id":11,"method":"mining.submit","params":["$user","$job","$en2","$ntime","$nonce","$vbits"]}"#,
                r#"{"id":12,"method":"mining.submit","params":["$user","$job","$en2","$ntime","$nonce","80000000"]}"#,
            ),
        ),
        (
            "configure_malformed_masks",
            [
                r#"{"id":1,"method":"mining.configure","params":[["version-rolling"],{"version-rolling.mask":7}]}"#,
                r#"{"id":2,"method":"mining.configure","params":[["version-rolling"],{"version-rolling.mask":"xyz"}]}"#,
                r#"{"id":3,"method":"mining.configure","params":[["version-rolling"],"not-an-object"]}"#,
                r#"{"id":4,"method":"mining.configure","params":[[1,null,"subscribe-extranonce"]]}"#,
            ]
            .join("\n"),
        ),
        (
            "suggest_high_difficulty",
            format!(
                "#cfg bits=1d00ffff\n{}\n{LOGIN}\n{submit}\n{}\n{submit}",
                r#"{"id":0,"method":"mining.suggest_difficulty","params":[1000]}"#,
                r#"{"id":3,"method":"mining.suggest_difficulty","params":["1e-300"]}"#,
            ),
        ),
        (
            "password_difficulty_options",
            [
                r#"{"id":1,"method":"mining.subscribe","params":[]}"#,
                r#"{"id":2,"method":"mining.authorize","params":["miner1","d=0.5,md=0.001,d=NaN,x"]}"#,
                r#"{"id":3,"method":"mining.authorize","params":["miner1","md=1e308,d=inf"]}"#,
                submit,
            ]
            .join("\n"),
        ),
        (
            "reauthorize_keeps_original_worker",
            format!(
                "{LOGIN}\n{}\n{}\n{submit}",
                r#"{"id":3,"method":"mining.authorize","params":["miner2.other","x"]}"#,
                r#"{"id":4,"method":"mining.submit","params":["miner2.other","$job1","$en2","$ntime","$nonce"]}"#,
            ),
        ),
        (
            "payout_revision_retires_same_parent_work",
            format!(
                "{LOGIN}\n#revision\n{}\n{submit}",
                r#"{"id":4,"method":"mining.submit","params":["$user","$job1","$en2","$ntime","$nonce"]}"#,
            ),
        ),
        (
            "tip_change_stale_grace",
            format!(
                "#cfg grace=3\n{LOGIN}\n#refresh\n{}\n{submit}",
                r#"{"id":4,"method":"mining.submit","params":["$user","$job1","$en2","$ntime","$nonce"]}"#,
            ),
        ),
        (
            "tip_change_without_grace",
            format!(
                "#cfg grace=0\n{LOGIN}\n#refresh\n{}",
                r#"{"id":4,"method":"mining.submit","params":["$user","$job1","$en2","$ntime","$nonce"]}"#,
            ),
        ),
        (
            "reconnect_resumes_issued_job",
            format!(
                "{LOGIN}\n#reconnect\n{LOGIN}\n{}",
                r#"{"id":4,"method":"mining.submit","params":["$user","$oldjob","$en2","$ntime","$nonce"]}"#,
            ),
        ),
        (
            "protocol_errors",
            [
                r#"{"id":1,"method":"mining.submit","params":["miner1","job-0","0000000000000000","6553f100","00000000"]}"#,
                r#"{"id":2,"method":"mining.unknown","params":[]}"#,
                r#"{"id":3,"params":[]}"#,
                r#"{"id":4,"method":"mining.subscribe","params":{"a":1}}"#,
                r#"{"id":5,"method":"mining.get_health","params":[1]}"#,
                r#"{"id":6,"method":"mining.extranonce.subscribe","params":[]}"#,
                LOGIN,
                r#"{"id":7,"method":"mining.submit","params":["$user","$job"]}"#,
                r#"{"id":8,"method":"mining.submit","params":["$user","$job",1,"$ntime","$nonce"]}"#,
                r#"{"id":9,"method":"mining.submit","params":["miner9","$job","$en2","$ntime","$nonce"]}"#,
                r#"{"id":10,"method":"mining.submit","params":["$user","$job","00","$ntime","$nonce"]}"#,
                r#"{"id":11,"method":"mining.submit","params":["$user","$job","$en2","655","$nonce"]}"#,
                r#"{"id":12,"method":"mining.submit","params":["$user","never-issued","$en2","$ntime","$nonce"]}"#,
                r#"{"id":13,"method":"mining.submit","params":["$user","$job","zzzzzzzzzzzzzzzz","$ntime","$nonce"]}"#,
                r#"{"id":14,"method":"mining.submit","params":["$user","$job","$en2","00000001","$nonce"]}"#,
                r#"{"id":15,"method":"mining.authorize","params":["not-a-miner","x"]}"#,
                r#"{"method":"mining.get_health"}"#,
                r#"[1,2,3]"#,
                "not json at all",
            ]
            .join("\n"),
        ),
        (
            "session_budgets",
            [
                "#cfg mb=2 ab=2 ub=1",
                "garbage",
                r#"{"id":1,"method":"mining.authorize","params":["nobody","x"]}"#,
                LOGIN,
                r#"{"id":3,"method":"mining.submit","params":["$user","gone-1","$en2","$ntime","$nonce"]}"#,
                r#"{"id":4,"method":"mining.submit","params":["$user","gone-2","$en2","$ntime","$nonce"]}"#,
                "#reconnect",
                "{",
                "}",
                "[]",
            ]
            .join("\n"),
        ),
        (
            "username_limit",
            format!(
                "#cfg user_limit=1\n{LOGIN}\n{}\n{}\n#reconnect\n{LOGIN}",
                r#"{"id":3,"method":"mining.authorize","params":["miner2","x"]}"#,
                r#"{"id":4,"method":"mining.authorize","params":["miner1.rig","x"]}"#,
            ),
        ),
        (
            "backend_failures",
            format!("#fail-build\n{LOGIN}\n{}\n#fail-submit\n{submit}\n{submit}", r#"{"id":3,"method":"mining.get_health","params":[]}"#),
        ),
        (
            "pipelined_requests",
            format!(
                "{LOGIN}\n#pipeline\n{}\n{}\n#again\n{}\n#refresh\n{}\n#pipeline\n{submit}",
                r#"{"id":20,"method":"mining.submit","params":["miner1.rig","$job","$en2","$ntime","$nonce"]}"#,
                r#"{"id":21,"method":"mining.authorize","params":["miner1.rig","d=1"]}"#,
                r#"{"id":22,"method":"mining.suggest_difficulty","params":[0.5]}"#,
                r#"{"id":23,"method":"mining.submit","params":["miner1.rig","$job","$en2","$ntime","$nonce"]}"#,
            ),
        ),
        (
            "short_retention_eviction",
            format!(
                "#cfg retain=0.001 jobs=2\n{LOGIN}\n#revision\n#revision\n#refresh\n#sleep\n{}\n{}",
                r#"{"id":4,"method":"mining.submit","params":["$user","$job3","$en2","$ntime","$nonce"]}"#,
                r#"{"id":5,"method":"mining.submit","params":["$user","$job2","$en2","$ntime","$nonce"]}"#,
            ),
        ),
        (
            "mainnet_difficulty_low_shares",
            format!("#cfg bits=1d00ffff start=1 vardiff=0\n{LOGIN}\n{submit}"),
        ),
        (
            "highdiff_floor",
            format!("#cfg min=500000 start=1024 bits=207fffff txs=4\n{LOGIN}\n{submit}"),
        ),
        (
            "oversize_then_reconnect",
            format!("#cfg max=256\n{LOGIN}\n{}\n#reconnect\n{LOGIN}", "x".repeat(300)),
        ),
        (
            "vardiff_retarget_keeps_old_targets",
            format!(
                "#cfg retarget=0.001 initial_min=0.001 initial_shares=1 bits=1d00ffff\n{LOGIN}\n{submit}\n#sleep\n{submit}\n#sleep\n{submit}\n{}\n{}",
                r#"{"id":5,"method":"mining.submit","params":["$user","$job1","$en2","$ntime","$nonce"]}"#,
                r#"{"id":6,"method":"mining.suggest_difficulty","params":[1e-9]}"#,
            ),
        ),
        (
            "configure_after_work_advertises_mask",
            format!(
                "{LOGIN}\n{}\n#refresh\n{}\n{}",
                r#"{"id":3,"method":"mining.configure","params":[["version-rolling"],{"version-rolling.mask":"00ffe000"}]}"#,
                r#"{"id":4,"method":"mining.configure","params":[["version-rolling"],{"version-rolling.mask":"1fffe000"}]}"#,
                r#"{"id":5,"method":"mining.submit","params":["$user","$job","$en2","$ntime","$nonce","$vbits"]}"#,
            ),
        ),
        (
            "reconnect_resumes_difficulty",
            format!(
                "#cfg bits=1d00ffff\n{}\n{LOGIN}\n{submit}\n#reconnect\n{LOGIN}\n{submit}",
                r#"{"id":0,"method":"mining.suggest_difficulty","params":[1e-7]}"#,
            ),
        ),
        ("zero_mask", format!("#cfg mask=0 en2=4\n{}\n{LOGIN}\n{submit}", r#"{"id":0,"method":"mining.configure","params":[["version-rolling"],{"version-rolling.mask":"ffffffff"}]}"#)),
    ];
    scripts
        .iter()
        .map(|(name, script)| {
            (
                format!("stratum_session/{name}"),
                script.clone().into_bytes(),
            )
        })
        .collect()
}

fn line_seeds() -> Corpus {
    // Header bytes: message bound index, malformed-frame budget, fragment seed.
    let seeds: Vec<(&str, [u8; 3], Vec<u8>)> = vec![
        ("login_one_write", [7, 0, 0], format!("{LOGIN}\n").into_bytes()),
        ("login_fragmented", [7, 0, 5], format!("{LOGIN}\n").into_bytes()),
        (
            "fragmented_json_across_writes",
            [0, 0, 1],
            b"{\"id\":42,\"method\":\"mining.subscribe\",\"params\":[]}\n".to_vec(),
        ),
        (
            "oversize_frame_closes",
            [0, 0, 3],
            [b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n".as_slice(), &[b'x'; 257]].concat(),
        ),
        (
            "exactly_at_bound",
            [0, 0, 0],
            [&[b' '; 255][..], b"\n", &[b' '; 256][..], b"\n"].concat(),
        ),
        (
            "malformed_budget",
            [7, 2, 9],
            b"garbage\n[]\n{\"id\":1}\n{\"id\":2,\"method\":7}\nmore\n".to_vec(),
        ),
        (
            "invalid_utf8_and_controls",
            [7, 0, 2],
            b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[\"\xff\xfe\"]}\n\r\n\n\x00\n{\"id\":\"\\ud800\"}\n".to_vec(),
        ),
        (
            "odd_ids",
            [7, 0, 4],
            [
                r#"{"id":1e400,"method":"mining.subscribe","params":[]}"#,
                r#"{"id":{"nested":[1,2]},"method":"mining.extranonce.subscribe"}"#,
                r#"{"id":-0.0,"method":"mining.get_health","params":[]}"#,
                r#"{"id":"\u0000","method":"mining.suggest_difficulty","params":["NaN"]}"#,
                r#"{"id":18446744073709551616,"method":"mining.suggest_difficulty","params":[1e-320]}"#,
                "",
            ]
            .join("\n")
            .into_bytes(),
        ),
        (
            "deep_nesting",
            [7, 0, 7],
            format!("{}{}\n", "[".repeat(200), "]".repeat(200)).into_bytes(),
        ),
        (
            "trailing_partial_frame",
            [7, 0, 6],
            b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n{\"id\":2,\"method\"".to_vec(),
        ),
        (
            "unknown_job_submit",
            [7, 0, 8],
            format!(
                "{LOGIN}\n{}\n",
                r#"{"id":3,"method":"mining.submit","params":["miner1.rig","job-0","0000000000000000","6553f100","00000000"]}"#
            )
            .into_bytes(),
        ),
    ];
    seeds
        .into_iter()
        .map(|(name, header, body)| {
            (
                format!("stratum_lines/{name}"),
                [header.as_slice(), &body].concat(),
            )
        })
        .collect()
}

/// The stratum_codec fixture's coinbase, job and a block assembled from it.
fn parser_seeds() -> Corpus {
    let template = json!({"height":101,"coinbasevalue":5_000_000_000u64,"previousblockhash":"0123456789abcdef".repeat(4),
        "version":0x20000000u32,"bits":"207fffff","curtime":1_700_000_000u32,"mintime":1_699_999_999u32,"transactions":[]});
    let manifest = build_manifest(CoinbaseBuildRequest {
        block_height: 101,
        coinbase_value_sats: 5_000_000_000,
        entitlements: vec![WeightedEntitlement {
            recipient_id: "miner".into(),
            order_key: "miner".into(),
            p2mr_program_hex: "ab".repeat(32),
            weight: 1,
        }],
        witness_nonce_hex: Some("00".repeat(32)),
        witness_merkle_leaves_hex: vec![],
        coinbase_script_sig_suffix_hex: Some(format!("505249534d12345678{}", "00".repeat(8))),
        pinned_first_output: None,
    })
    .unwrap();
    let coinbase = hex::decode(&manifest.coinbase_tx_hex).unwrap();
    let job = Job::from_manifest(
        "job".into(),
        &template,
        &manifest,
        "12345678",
        8,
        1e-9,
        0.0,
        true,
    )
    .unwrap();
    let block = (0u32..64)
        .map(|nonce| {
            job.assemble_submission(
                "0000000000000000",
                "6553f100",
                &format!("{nonce:08x}"),
                None,
                VERSION_ROLLING_MASK,
            )
            .unwrap()
        })
        .find(|s| s.block_pass)
        .map(|s| hex::decode(s.block_hex).unwrap())
        .unwrap();
    let stripped = codec::strip_witness_transaction(&coinbase).unwrap();
    let number = |bits: u32, a: f64, b: f64| {
        [
            bits.to_le_bytes().as_slice(),
            &[0; 4],
            &a.to_le_bytes(),
            &b.to_le_bytes(),
        ]
        .concat()
    };
    let mut malformed = coinbase.clone();
    malformed.splice(6..7, [253, 1, 0]);
    let seeds: Vec<(&str, Vec<u8>)> = vec![
        ("segwit_coinbase", coinbase.clone()),
        ("stripped_coinbase", stripped),
        ("noncanonical_compact_size", malformed),
        ("assembled_block", block),
        ("coinbase_split_placeholder", [&[16u8][..], &coinbase].concat()),
        ("compact_mainnet_difficulty_1", number(0x1d00ffff, 1.0, 2.0)),
        ("compact_regtest", number(0x207fffff, 1e-9, 500_000.0)),
        ("compact_negative", number(0x20800001, f64::MAX, f64::MIN_POSITIVE)),
        ("compact_overflow", number(0x2300ffff, f64::NAN, 5e-324)),
        ("hex_field", b"6553f100".to_vec()),
        ("template_json", serde_json::to_vec(&template).unwrap()),
        (
            "template_mask_and_transactions",
            serde_json::to_vec(&json!({"versionrollingmask":"0x0000e000","transactions":[{"data":hex::encode(&coinbase)}],
                "previousblockhash":"00".repeat(32),"bits":"1d00ffff","version":-1,"curtime":4294967296u64}))
            .unwrap(),
        ),
    ];
    seeds
        .into_iter()
        .map(|(name, bytes)| (format!("codec_parsers/{name}"), bytes))
        .collect()
}

fn corpus() -> Corpus {
    let mut all = session_seeds();
    all.extend(line_seeds());
    all.extend(parser_seeds());
    all
}

fn seeds_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("seeds")
}

#[test]
fn checked_in_seeds_match_the_generator() {
    let corpus = corpus();
    if std::env::var("PRISM_FUZZ_REGENERATE_SEEDS").as_deref() == Ok("1") {
        let _ = std::fs::remove_dir_all(seeds_dir());
        for (name, bytes) in &corpus {
            let path = seeds_dir().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
    }
    let mut on_disk = Corpus::new();
    for target in std::fs::read_dir(seeds_dir()).unwrap() {
        let target = target.unwrap();
        for seed in std::fs::read_dir(target.path()).unwrap() {
            let seed = seed.unwrap();
            on_disk.insert(
                format!(
                    "{}/{}",
                    target.file_name().to_string_lossy(),
                    seed.file_name().to_string_lossy()
                ),
                std::fs::read(seed.path()).unwrap(),
            );
        }
    }
    assert!(
        on_disk == corpus,
        "seeds/ drifted; regenerate with PRISM_FUZZ_REGENERATE_SEEDS=1"
    );
}

#[test]
fn every_seed_passes_its_targets_checks() {
    for (name, bytes) in corpus() {
        match name.split('/').next().unwrap() {
            "stratum_session" => drop(script::run(&bytes)),
            "stratum_lines" => lines::run(&bytes),
            "codec_parsers" => parsers::run(&bytes),
            other => panic!("unknown target {other}"),
        }
    }
}

/// The session seeds reach the states they are named for, so the corpus
/// starts deep in the state machine rather than at its front door.
#[test]
fn session_seeds_reach_their_states() {
    let expected: &[(&str, usize, &[&str])] = &[
        ("login_and_share", 1, &["1:ok", "2:ok", "10:ok"]),
        (
            "duplicate_share",
            1,
            &["1:ok", "2:ok", "10:ok", "10:duplicate-share"],
        ),
        (
            "version_rolling",
            1,
            &["0:ok", "1:ok", "2:ok", "11:ok", "12:malformed-submit"],
        ),
        (
            "suggest_high_difficulty",
            1,
            &["0:ok", "1:ok", "2:ok", "10:low-difficulty", "3:ok", "10:ok"],
        ),
        (
            "reauthorize_keeps_original_worker",
            2,
            &["1:ok", "2:ok", "3:ok", "4:ok", "10:ok"],
        ),
        (
            "payout_revision_retires_same_parent_work",
            1,
            &["1:ok", "2:ok", "4:stale-job", "10:ok"],
        ),
        (
            "tip_change_stale_grace",
            2,
            &["1:ok", "2:ok", "4:ok", "10:ok"],
        ),
        (
            "tip_change_without_grace",
            0,
            &["1:ok", "2:ok", "4:stale-job"],
        ),
        (
            "reconnect_resumes_issued_job",
            1,
            &["1:ok", "2:ok", "1:ok", "2:ok", "4:ok"],
        ),
        (
            "mainnet_difficulty_low_shares",
            0,
            &["1:ok", "2:ok", "10:low-difficulty"],
        ),
        (
            "configure_after_work_advertises_mask",
            1,
            &["1:ok", "2:ok", "3:ok", "4:ok", "5:ok"],
        ),
        (
            "reconnect_resumes_difficulty",
            2,
            &["0:ok", "1:ok", "2:ok", "10:ok", "1:ok", "2:ok", "10:ok"],
        ),
        (
            "backend_failures",
            1,
            &[
                "1:ok",
                "2:ok",
                "3:ok",
                "10:backend-rpc-unavailable",
                "10:ok",
            ],
        ),
        (
            "session_budgets",
            0,
            &[
                "1:unauthorized-worker",
                "1:ok",
                "2:ok",
                "3:unknown-job",
                "4:too many unknown job submissions",
            ],
        ),
        (
            "protocol_errors",
            0,
            &[
                "1:unauthorized-worker",
                "2:malformed-submit",
                "3:malformed-submit",
                "4:malformed-submit",
                "5:malformed-submit",
                "6:ok",
                "1:ok",
                "2:ok",
                "7:malformed-submit",
                "8:malformed-submit",
                "9:unauthorized-worker",
                "10:invalid-extranonce",
                "11:invalid-ntime-or-nonce",
                "12:unknown-job",
                "13:malformed-submit",
                "14:malformed-submit",
                "15:unauthorized-worker",
                "null:ok",
            ],
        ),
    ];
    let seeds = session_seeds();
    for (name, credits, outcomes) in expected {
        let reached = script::run(&seeds[&format!("stratum_session/{name}")]);
        assert_eq!(
            (reached.credits, reached.outcomes.as_slice()),
            (
                *credits,
                outcomes
                    .iter()
                    .map(|o| o.to_string())
                    .collect::<Vec<_>>()
                    .as_slice()
            ),
            "{name}"
        );
    }
}

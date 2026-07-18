use qbit_pool_builder::{ManifestSigningKey, SignedPayoutManifest};
use qbit_prism::{
    build_audit_bundle_with_coinbase_options, build_audit_bundle_with_ctv_settlement_options,
    profile_audit_build, AcceptedShare, AuditBundle, CarryForwardBalance, FanoutFeeRatePolicy,
    FoundBlock, PayoutPolicy, SettlementModeConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{self, BufReader, Write};
use std::time::Instant;
use std::{env, error::Error, fs, process};

const PHASE_METRICS_PREFIX: &str = "qbit-prism-build-phase-metrics ";

#[derive(Debug, Deserialize)]
struct BuildAuditBundleInput {
    #[serde(default)]
    shares: Vec<AcceptedShare>,
    #[serde(default)]
    compact_share_identities: Vec<CompactShareIdentity>,
    #[serde(default)]
    compact_shares: Vec<CompactAcceptedShare>,
    found_block: FoundBlock,
    #[serde(default)]
    prior_balances: Vec<CarryForwardBalance>,
    #[serde(default)]
    payout_policy: Option<PayoutPolicy>,
    #[serde(default)]
    coinbase_script_sig_suffix_hex: Option<String>,
    #[serde(default)]
    witness_merkle_leaves_hex: Vec<String>,
    #[serde(default)]
    ctv_settlement: Option<CtvSettlementInput>,
}

#[derive(Debug, Deserialize)]
struct CompactShareIdentity(String, String, String);

#[derive(Debug, Deserialize)]
struct CompactAcceptedShare(u64, String, usize, u128, i64, i64, Option<String>);

#[derive(Debug, Deserialize)]
struct CtvSettlementInput {
    direct_floor_sats: u64,
    config: SettlementModeConfig,
    #[serde(default)]
    fanout_fee_rate_policy: Option<FanoutFeeRatePolicy>,
}

#[derive(Serialize)]
struct JobBuildSummary<'a> {
    found_block: &'a FoundBlock,
    signed_coinbase_manifest: &'a SignedPayoutManifest,
}

#[derive(Serialize)]
struct BuildPhaseMetrics {
    input_deserialization_seconds: f64,
    phases_seconds: BTreeMap<&'static str, f64>,
    output_serialization_seconds: f64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("qbit-prism-build-audit-bundle: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut input_path: Option<String> = None;
    let mut signing_key_seed_hex: Option<String> = None;
    let mut ledger_signing_key_seed_hex: Option<String> = None;
    let mut canonical_output = false;
    let mut job_summary_output = false;
    let mut phase_metrics = false;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input_path = args.next(),
            "--signing-key-seed-hex" => signing_key_seed_hex = args.next(),
            "--ledger-signing-key-seed-hex" => ledger_signing_key_seed_hex = args.next(),
            "--canonical-output" => canonical_output = true,
            "--job-summary-output" => job_summary_output = true,
            "--phase-metrics" => phase_metrics = true,
            "-h" | "--help" => {
                print_usage();
                return Ok(());
            }
            _ => return Err(format!("unexpected argument: {arg}").into()),
        }
    }

    if canonical_output && job_summary_output {
        return Err("--canonical-output and --job-summary-output are mutually exclusive".into());
    }

    let input_started = Instant::now();
    let mut input: BuildAuditBundleInput = match input_path.as_deref() {
        Some("-") | None => serde_json::from_reader(io::stdin().lock())?,
        Some(path) => serde_json::from_reader(BufReader::new(fs::File::open(path)?))?,
    };
    let input_deserialization_seconds = input_started.elapsed().as_secs_f64();
    if !input.compact_shares.is_empty() {
        if !job_summary_output {
            return Err("compact shares are valid only with --job-summary-output".into());
        }
        if !input.shares.is_empty() {
            return Err("full and compact shares are mutually exclusive".into());
        }
        input.shares = input
            .compact_shares
            .drain(..)
            .map(
                |CompactAcceptedShare(
                    share_seq,
                    share_id,
                    identity_index,
                    share_difficulty,
                    job_issued_at_ms,
                    accepted_at_ms,
                    credit_policy,
                )| {
                    let CompactShareIdentity(miner_id, order_key, p2mr_program_hex) = input
                        .compact_share_identities
                        .get(identity_index)
                        .ok_or("compact share identity index is out of range")?;
                    Ok(AcceptedShare {
                        share_seq,
                        share_id,
                        miner_id: miner_id.clone(),
                        order_key: order_key.clone(),
                        p2mr_program_hex: p2mr_program_hex.clone(),
                        share_difficulty,
                        // These accepted-share fields are deliberately absent
                        // from the job-summary artifact: neither reward-window
                        // selection, payout derivation, nor its signed
                        // commitments consume them. Canonical audit builds
                        // continue to require and retain the full values.
                        network_difficulty: 1,
                        template_height: 0,
                        job_id: String::new(),
                        job_issued_at_ms,
                        accepted_at_ms,
                        ntime: 0,
                        credit_policy,
                    })
                },
            )
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    } else if !input.compact_share_identities.is_empty() {
        return Err("compact share identities were supplied without compact shares".into());
    }
    let signing_key_seed_hex = signing_key_seed_hex.ok_or("--signing-key-seed-hex is required")?;
    let ledger_signing_key_seed_hex =
        ledger_signing_key_seed_hex.ok_or("--ledger-signing-key-seed-hex is required")?;
    let signing_key = ManifestSigningKey::from_seed_hex(&signing_key_seed_hex)?;
    let ledger_signing_key = ManifestSigningKey::from_seed_hex(&ledger_signing_key_seed_hex)?;
    let payout_policy = input
        .payout_policy
        .unwrap_or_else(PayoutPolicy::day_one_default);
    let (bundle_result, phases_seconds) = profile_audit_build(|| {
        if let Some(ctv_settlement) = input.ctv_settlement {
            build_audit_bundle_with_ctv_settlement_options(
                input.shares,
                input.found_block,
                input.prior_balances,
                payout_policy,
                ctv_settlement.direct_floor_sats,
                ctv_settlement.config,
                ctv_settlement.fanout_fee_rate_policy,
                input.coinbase_script_sig_suffix_hex,
                input.witness_merkle_leaves_hex,
                &signing_key,
                &ledger_signing_key,
            )
        } else {
            build_audit_bundle_with_coinbase_options(
                input.shares,
                input.found_block,
                input.prior_balances,
                payout_policy,
                input.coinbase_script_sig_suffix_hex,
                input.witness_merkle_leaves_hex,
                &signing_key,
                &ledger_signing_key,
            )
        }
    });
    let bundle: AuditBundle = bundle_result?;

    let stdout = io::stdout();
    let mut output = stdout.lock();
    let output_started = Instant::now();
    if canonical_output {
        // serde_json::to_writer uses the same compact serializer as the
        // canonical to_vec helper, without allocating a second full body.
        serde_json::to_writer(&mut output, &bundle)?;
    } else if job_summary_output {
        serde_json::to_writer(
            &mut output,
            &JobBuildSummary {
                found_block: &bundle.found_block,
                signed_coinbase_manifest: &bundle.signed_coinbase_manifest,
            },
        )?;
    } else {
        serde_json::to_writer_pretty(&mut output, &bundle)?;
        writeln!(output)?;
    }
    output.flush()?;
    let output_serialization_seconds = output_started.elapsed().as_secs_f64();
    if phase_metrics {
        eprintln!(
            "{PHASE_METRICS_PREFIX}{}",
            serde_json::to_string(&BuildPhaseMetrics {
                input_deserialization_seconds,
                phases_seconds,
                output_serialization_seconds,
            })?
        );
    }
    Ok(())
}

fn print_usage() {
    println!(
        "usage: qbit-prism-build-audit-bundle --signing-key-seed-hex <64 hex chars> --ledger-signing-key-seed-hex <64 hex chars> [--input <bundle-input.json|-] [--canonical-output|--job-summary-output] [--phase-metrics]"
    );
}

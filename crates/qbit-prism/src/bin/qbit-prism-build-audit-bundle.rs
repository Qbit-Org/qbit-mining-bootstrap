use qbit_pool_builder::{ManifestSigningKey, SignedPayoutManifest};
use qbit_prism::{
    build_audit_bundle_with_coinbase_options, build_audit_bundle_with_ctv_settlement_options,
    profile_audit_build, AcceptedShare, AuditBundle, CarryForwardBalance, FanoutFeeRatePolicy,
    FoundBlock, PayoutPolicy, PayoutPolicyManifest, SettlementModeConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, Write};
use std::time::Instant;
use std::{env, error::Error, fs, process};

const PHASE_METRICS_PREFIX: &str = "qbit-prism-build-phase-metrics ";
/// Version of the --serve JSONL protocol announced in the startup handshake.
/// The coordinator refuses to speak to a daemon announcing a different
/// version and falls back to one-shot builds instead.
const SERVE_PROTOCOL_VERSION: u64 = 1;
/// Parsed share windows retained by the --serve daemon. Windows rotate with
/// payout/artifact generations, so two entries cover the current generation
/// plus the previous one still finishing in-flight builds.
const SERVE_WINDOW_CACHE_MAX_ENTRIES: usize = 2;

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

#[derive(Debug, Deserialize)]
struct ServeWindowKey {
    share_snapshot_sha256: String,
}

#[derive(Debug, Deserialize)]
struct ServeRequest {
    /// Identity of the parsed share window this build wants. Requests may
    /// omit the inline window once a prior request uploaded it.
    window_key: ServeWindowKey,
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

#[derive(Serialize)]
struct JobBuildSummary<'a> {
    found_block: &'a FoundBlock,
    signed_coinbase_manifest: &'a SignedPayoutManifest,
    payout_policy_manifest: &'a PayoutPolicyManifest,
}

#[derive(Serialize)]
struct BuildPhaseMetrics {
    input_deserialization_seconds: f64,
    phases_seconds: BTreeMap<&'static str, f64>,
    output_serialization_seconds: f64,
}

#[derive(Serialize)]
struct ServeWindowCacheStats {
    hit: bool,
    hits: u64,
    misses: u64,
    entries: usize,
}

#[derive(Serialize)]
struct ServeResponse<'a> {
    ok: bool,
    // Pre-encoded during the measured output window and embedded verbatim,
    // so the envelope write never re-serializes the summary.
    summary: &'a serde_json::value::RawValue,
    metrics: BuildPhaseMetrics,
    window_cache: ServeWindowCacheStats,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("qbit-prism-build-audit-bundle: {error}");
        process::exit(1);
    }
}

fn expand_compact_shares(
    identities: &[CompactShareIdentity],
    compact_shares: Vec<CompactAcceptedShare>,
) -> Result<Vec<AcceptedShare>, Box<dyn Error>> {
    compact_shares
        .into_iter()
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
                let CompactShareIdentity(miner_id, order_key, p2mr_program_hex) = identities
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
        .collect::<Result<Vec<_>, Box<dyn Error>>>()
}

#[allow(clippy::too_many_arguments)]
fn run_profiled_build(
    shares: Vec<AcceptedShare>,
    found_block: FoundBlock,
    prior_balances: Vec<CarryForwardBalance>,
    payout_policy: PayoutPolicy,
    ctv_settlement: Option<CtvSettlementInput>,
    coinbase_script_sig_suffix_hex: Option<String>,
    witness_merkle_leaves_hex: Vec<String>,
    signing_key: &ManifestSigningKey,
    ledger_signing_key: &ManifestSigningKey,
) -> (
    Result<AuditBundle, qbit_prism::PrismError>,
    BTreeMap<&'static str, f64>,
) {
    profile_audit_build(|| {
        if let Some(ctv_settlement) = ctv_settlement {
            build_audit_bundle_with_ctv_settlement_options(
                shares,
                found_block,
                prior_balances,
                payout_policy,
                ctv_settlement.direct_floor_sats,
                ctv_settlement.config,
                ctv_settlement.fanout_fee_rate_policy,
                coinbase_script_sig_suffix_hex,
                witness_merkle_leaves_hex,
                signing_key,
                ledger_signing_key,
            )
        } else {
            build_audit_bundle_with_coinbase_options(
                shares,
                found_block,
                prior_balances,
                payout_policy,
                coinbase_script_sig_suffix_hex,
                witness_merkle_leaves_hex,
                signing_key,
                ledger_signing_key,
            )
        }
    })
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut input_path: Option<String> = None;
    let mut signing_key_seed_hex: Option<String> = None;
    let mut ledger_signing_key_seed_hex: Option<String> = None;
    let mut canonical_output = false;
    let mut job_summary_output = false;
    let mut phase_metrics = false;
    let mut serve = false;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input_path = args.next(),
            "--signing-key-seed-hex" => signing_key_seed_hex = args.next(),
            "--ledger-signing-key-seed-hex" => ledger_signing_key_seed_hex = args.next(),
            "--canonical-output" => canonical_output = true,
            "--job-summary-output" => job_summary_output = true,
            "--phase-metrics" => phase_metrics = true,
            "--serve" => serve = true,
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

    let signing_key_seed_hex = signing_key_seed_hex.ok_or("--signing-key-seed-hex is required")?;
    let ledger_signing_key_seed_hex =
        ledger_signing_key_seed_hex.ok_or("--ledger-signing-key-seed-hex is required")?;
    let signing_key = ManifestSigningKey::from_seed_hex(&signing_key_seed_hex)?;
    let ledger_signing_key = ManifestSigningKey::from_seed_hex(&ledger_signing_key_seed_hex)?;

    if serve {
        if canonical_output || job_summary_output || phase_metrics || input_path.is_some() {
            return Err("--serve accepts only the signing key arguments".into());
        }
        return serve_requests(&signing_key, &ledger_signing_key);
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
        input.shares = expand_compact_shares(
            &input.compact_share_identities,
            std::mem::take(&mut input.compact_shares),
        )?;
    } else if !input.compact_share_identities.is_empty() {
        return Err("compact share identities were supplied without compact shares".into());
    }
    let payout_policy = input
        .payout_policy
        .unwrap_or_else(PayoutPolicy::day_one_default);
    let (bundle_result, phases_seconds) = run_profiled_build(
        input.shares,
        input.found_block,
        input.prior_balances,
        payout_policy,
        input.ctv_settlement,
        input.coinbase_script_sig_suffix_hex,
        input.witness_merkle_leaves_hex,
        &signing_key,
        &ledger_signing_key,
    );
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
                payout_policy_manifest: &bundle.payout_policy_manifest,
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

/// Long-lived JSONL build server over stdin/stdout.
///
/// One request per line; one response object per line. The parsed share
/// window is cached across requests keyed by the coordinator's
/// share_snapshot_sha256, bounded to the most recent generations, so repeat
/// builds skip re-parsing megabytes of unchanged window JSON. Every
/// response reports the same input_deserialization/output_serialization
/// timings the one-shot mode emits, measured for that request.
fn serve_requests(
    signing_key: &ManifestSigningKey,
    ledger_signing_key: &ManifestSigningKey,
) -> Result<(), Box<dyn Error>> {
    let stdout = io::stdout();
    {
        let mut out = stdout.lock();
        serde_json::to_writer(
            &mut out,
            &serde_json::json!({
                "event": "handshake",
                "tool": "qbit-prism-build-audit-bundle",
                "protocol": SERVE_PROTOCOL_VERSION,
            }),
        )?;
        writeln!(out)?;
        out.flush()?;
    }

    let mut window_cache: Vec<(String, Vec<AcceptedShare>)> = Vec::new();
    let mut cache_hits: u64 = 0;
    let mut cache_misses: u64 = 0;

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let input_started = Instant::now();
        let request: ServeRequest = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                respond_error(&stdout, &format!("malformed serve request: {error}"), false)?;
                continue;
            }
        };
        let window_sha = request.window_key.share_snapshot_sha256.clone();
        let uploaded_window = !request.compact_shares.is_empty();
        if !uploaded_window && !request.compact_share_identities.is_empty() {
            respond_error(
                &stdout,
                "compact share identities were supplied without compact shares",
                false,
            )?;
            continue;
        }
        // The window vector is loaned to the build rather than cloned: both
        // audit build entry points only borrow the shares for derivation and
        // move the vector unmodified into AuditBundle.shares, so it is
        // reclaimed from the finished bundle below. A failed build drops the
        // loaned window; the coordinator's needs_window bounce re-uploads it.
        let shares: Vec<AcceptedShare> = if uploaded_window {
            match expand_compact_shares(&request.compact_share_identities, request.compact_shares) {
                Ok(expanded) => {
                    cache_misses += 1;
                    window_cache.retain(|(key, _)| key != &window_sha);
                    expanded
                }
                Err(error) => {
                    respond_error(&stdout, &format!("invalid window upload: {error}"), false)?;
                    continue;
                }
            }
        } else if let Some(position) = window_cache
            .iter()
            .position(|(key, _)| key == &window_sha)
        {
            cache_hits += 1;
            window_cache.remove(position).1
        } else {
            // Not counted as a miss: the coordinator's follow-up upload of
            // this same window is the miss that gets counted.
            respond_error(
                &stdout,
                &format!("share window {window_sha} is not cached"),
                true,
            )?;
            continue;
        };
        // Derived from the same predicate that selected the cache branch, so
        // the per-response flag always agrees with the hit/miss counters.
        let cache_hit = !uploaded_window;
        let payout_policy = request
            .payout_policy
            .unwrap_or_else(PayoutPolicy::day_one_default);
        let input_deserialization_seconds = input_started.elapsed().as_secs_f64();
        let (bundle_result, phases_seconds) = run_profiled_build(
            shares,
            request.found_block,
            request.prior_balances,
            payout_policy,
            request.ctv_settlement,
            request.coinbase_script_sig_suffix_hex,
            request.witness_merkle_leaves_hex,
            signing_key,
            ledger_signing_key,
        );
        let bundle = match bundle_result {
            Ok(bundle) => bundle,
            Err(error) => {
                respond_error(&stdout, &format!("audit bundle build failed: {error}"), false)?;
                continue;
            }
        };
        // The summary is fully encoded inside the measured window, exactly
        // the serialization work the one-shot mode times for its stdout
        // body; the envelope only appends small fixed-size fields around the
        // pre-encoded bytes.
        let output_started = Instant::now();
        let summary_json = serde_json::to_string(&JobBuildSummary {
            found_block: &bundle.found_block,
            signed_coinbase_manifest: &bundle.signed_coinbase_manifest,
            payout_policy_manifest: &bundle.payout_policy_manifest,
        })?;
        let output_serialization_seconds = output_started.elapsed().as_secs_f64();
        let summary_raw = serde_json::value::RawValue::from_string(summary_json)?;
        // Reclaim the loaned window from the finished bundle before
        // reporting cache occupancy: no bytes were copied on the way in or
        // out, and the entry keeps most-recent position.
        window_cache.insert(0, (window_sha, bundle.shares));
        window_cache.truncate(SERVE_WINDOW_CACHE_MAX_ENTRIES);
        let response = ServeResponse {
            ok: true,
            summary: &summary_raw,
            metrics: BuildPhaseMetrics {
                input_deserialization_seconds,
                phases_seconds,
                output_serialization_seconds,
            },
            window_cache: ServeWindowCacheStats {
                hit: cache_hit,
                hits: cache_hits,
                misses: cache_misses,
                entries: window_cache.len(),
            },
        };
        let mut out = stdout.lock();
        serde_json::to_writer(&mut out, &response)?;
        writeln!(out)?;
        out.flush()?;
    }
    Ok(())
}

fn respond_error(
    stdout: &io::Stdout,
    error: &str,
    needs_window: bool,
) -> Result<(), Box<dyn Error>> {
    let mut out = stdout.lock();
    serde_json::to_writer(
        &mut out,
        &serde_json::json!({
            "ok": false,
            "error": error,
            "needs_window": needs_window,
        }),
    )?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

fn print_usage() {
    println!(
        "usage: qbit-prism-build-audit-bundle --signing-key-seed-hex <64 hex chars> --ledger-signing-key-seed-hex <64 hex chars> [--input <bundle-input.json|-] [--canonical-output|--job-summary-output] [--phase-metrics] [--serve]"
    );
}

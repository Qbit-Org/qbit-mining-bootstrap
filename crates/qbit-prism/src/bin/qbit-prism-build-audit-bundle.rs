use qbit_pool_builder::{ManifestSigningKey, SignedPayoutManifest};
use qbit_prism::window::{PayoutWindow, DEFAULT_WINDOW_PAGE_SIZE};
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
/// version and falls back to one-shot builds instead. Version 2 adds the
/// prepare_window request (payout-window fold, canonical digest, and
/// incremental advance daemon-side) and unifies its window state with the
/// build cache.
const SERVE_PROTOCOL_VERSION: u64 = 2;
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

/// One serve-mode request line, build and prepare_window shapes combined so
/// dispatch needs a single parse of megabyte-scale lines. `request` is absent
/// on build requests (the historical shape) and `"prepare_window"` on window
/// preparations; required fields are validated per shape after dispatch so a
/// malformed request answers with an error instead of killing the daemon.
#[derive(Debug, Deserialize)]
struct ServeRequest {
    #[serde(default)]
    request: Option<String>,
    /// Identity of the parsed share window this build wants. Requests may
    /// omit the inline window once a prior request uploaded or prepared it.
    #[serde(default)]
    window_key: Option<ServeWindowKey>,
    #[serde(default)]
    compact_share_identities: Vec<CompactShareIdentity>,
    #[serde(default)]
    compact_shares: Vec<CompactAcceptedShare>,
    #[serde(default)]
    found_block: Option<FoundBlock>,
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
    /// Coordinator append-invalidation epoch. Recorded against prepared
    /// window state and echoed back on advance for diagnostics; it never
    /// decides which window this daemon will serve. See `WindowState` for
    /// why the coordinator's own policy is the only one that can be right.
    #[serde(default)]
    append_invalidation_epoch: Option<u64>,
    // prepare_window fields.
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    records: Vec<AcceptedShare>,
    #[serde(default)]
    base_digest: Option<String>,
    #[serde(default)]
    anchor_job_issued_at_ms: Option<i64>,
    #[serde(default)]
    window_weight: Option<u128>,
    #[serde(default)]
    page_size: Option<usize>,
}

/// One unified window-cache entry: either a raw upload from the build path
/// (compact records, loaned to builds exactly as before) or a prepared
/// window that owns the fold state needed to advance incrementally. Prepared
/// windows are never loaned: builds clone their records so the advance
/// lineage survives every build outcome.
///
/// The recorded `epoch` is diagnostic only. This daemon never chooses a
/// window: every entry is addressed by the content digest the coordinator
/// asks for, builds are validated by the install fence and the job-bundle
/// epoch checks, and an `advance` base is the coordinator's own
/// digest-verified mirror -- which the coordinator keeps only while its own
/// (finer) append-invalidation policy says it may. Refusing an older-tagged
/// base could therefore never catch anything the coordinator had not
/// already decided; it could only turn the coordinator's deliberate retag,
/// the one that preserves an unaffected window across a late append, into a
/// full DB walk, fold and upload on every replay burst.
enum WindowState {
    Uploaded(Vec<AcceptedShare>),
    Prepared { window: PayoutWindow, epoch: u64 },
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

    let mut window_cache: Vec<(String, WindowState)> = Vec::new();
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
        // Sampled immediately after the JSON parse and before compact-window
        // expansion, exactly where one-shot mode samples the same metric, so
        // transport comparisons measure identical work.
        let input_deserialization_seconds = input_started.elapsed().as_secs_f64();
        // No epoch-driven eviction here: see `WindowState`. A window the
        // coordinator no longer considers valid is one it never asks for
        // again, and the entry it does ask for is one it has already
        // cleared under its own policy. Evicting on the tag would only
        // discard windows the coordinator still wants.
        match request.request.as_deref() {
            None => {}
            Some("prepare_window") => {
                serve_prepare_window(&stdout, request, &mut window_cache)?;
                continue;
            }
            Some(other) => {
                respond_error(
                    &stdout,
                    &format!("unsupported serve request type: {other}"),
                    false,
                )?;
                continue;
            }
        }
        let Some(window_key) = request.window_key else {
            respond_error(&stdout, "build request carries no window_key", false)?;
            continue;
        };
        let Some(found_block) = request.found_block else {
            respond_error(&stdout, "build request carries no found_block", false)?;
            continue;
        };
        let window_sha = window_key.share_snapshot_sha256;
        let uploaded_window = !request.compact_shares.is_empty();
        if !uploaded_window && !request.compact_share_identities.is_empty() {
            respond_error(
                &stdout,
                "compact share identities were supplied without compact shares",
                false,
            )?;
            continue;
        }
        // Uploaded windows are loaned to the build rather than cloned: both
        // audit build entry points only borrow the shares for derivation and
        // move the vector unmodified into AuditBundle.shares, so a loan is
        // reclaimed from the finished bundle below. Prepared windows are
        // never loaned -- the build gets a clone and the entry stays intact,
        // because the fold state must survive a failed build to keep
        // serving advances. A failed build drops a loaned upload; the
        // coordinator's needs_window bounce re-uploads it.
        let cached_position = window_cache.iter().position(|(key, _)| key == &window_sha);
        let prepared_hit = matches!(
            cached_position.map(|position| &window_cache[position].1),
            Some(WindowState::Prepared { .. })
        );
        let shares: Vec<AcceptedShare> = if uploaded_window {
            cache_misses += 1;
            if prepared_hit {
                // The prepared state is the same content-addressed window at
                // full fidelity; keep it (and its advance lineage) and serve
                // the build from it, ignoring the redundant upload bytes.
                let position = cached_position.expect("prepared_hit implies a position");
                match &window_cache[position].1 {
                    WindowState::Prepared { window, .. } => window.shares_for_build(),
                    WindowState::Uploaded(_) => unreachable!("prepared_hit checked the variant"),
                }
            } else {
                match expand_compact_shares(
                    &request.compact_share_identities,
                    request.compact_shares,
                ) {
                    Ok(expanded) => {
                        window_cache.retain(|(key, _)| key != &window_sha);
                        expanded
                    }
                    Err(error) => {
                        respond_error(&stdout, &format!("invalid window upload: {error}"), false)?;
                        continue;
                    }
                }
            }
        } else if let Some(position) = cached_position {
            cache_hits += 1;
            match &window_cache[position].1 {
                WindowState::Prepared { window, .. } => window.shares_for_build(),
                WindowState::Uploaded(_) => {
                    let (_, state) = window_cache.remove(position);
                    match state {
                        WindowState::Uploaded(shares) => shares,
                        WindowState::Prepared { .. } => unreachable!("variant checked above"),
                    }
                }
            }
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
        let (bundle_result, phases_seconds) = run_profiled_build(
            shares,
            found_block,
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
        // Reclaim a loaned upload from the finished bundle (no bytes were
        // copied on the way in or out); a prepared entry only moves to
        // most-recent position, its clone in bundle.shares is dropped.
        if let Some(position) = window_cache.iter().position(|(key, _)| key == &window_sha) {
            let entry = window_cache.remove(position);
            window_cache.insert(0, entry);
        } else {
            window_cache.insert(0, (window_sha, WindowState::Uploaded(bundle.shares)));
        }
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

/// Handle one prepare_window request against the unified window cache.
///
/// Success responses append a raw canonical-items section after the JSON
/// envelope line: the full items stream for `full` mode, or the appended
/// suffix for `advance` mode (whose envelope also carries the byte count to
/// drop from the front of the previous stream). The two distinguishable
/// non-success outcomes -- `needs_full` (state not held) and `fallback`
/// (an advance invariant failed) -- are ordinary control flow for the
/// coordinator, never daemon anomalies.
fn serve_prepare_window(
    stdout: &io::Stdout,
    request: ServeRequest,
    window_cache: &mut Vec<(String, WindowState)>,
) -> Result<(), Box<dyn Error>> {
    let Some(request_epoch) = request.append_invalidation_epoch else {
        respond_error(
            stdout,
            "prepare_window carries no append_invalidation_epoch",
            false,
        )?;
        return Ok(());
    };
    let Some(anchor_job_issued_at_ms) = request.anchor_job_issued_at_ms else {
        respond_error(
            stdout,
            "prepare_window carries no anchor_job_issued_at_ms",
            false,
        )?;
        return Ok(());
    };
    match request.mode.as_deref() {
        Some("full") => {
            let Some(window_weight) = request.window_weight else {
                respond_error(stdout, "prepare_window full carries no window_weight", false)?;
                return Ok(());
            };
            let page_size = request.page_size.unwrap_or(DEFAULT_WINDOW_PAGE_SIZE);
            let window = match PayoutWindow::from_full_snapshot(
                request.records,
                anchor_job_issued_at_ms,
                window_weight,
                page_size,
            ) {
                Ok(window) => window,
                Err(error) => {
                    // The snapshot itself violates the fold's invariants
                    // (duplicate share_seq/share_id, non-positive
                    // difficulty). The coordinator's in-process oracle
                    // raises the same rejection, so answer with a
                    // distinguishable outcome instead of a daemon anomaly.
                    let mut out = stdout.lock();
                    serde_json::to_writer(
                        &mut out,
                        &serde_json::json!({
                            "ok": false,
                            "request": "prepare_window",
                            "fold_invalid": true,
                            "error": error.to_string(),
                        }),
                    )?;
                    writeln!(out)?;
                    out.flush()?;
                    return Ok(());
                }
            };
            let digest = window.canonical_digest_hex();
            let items = window.canonical_items_bytes();
            let record_count = window.record_count();
            window_cache.retain(|(key, _)| key != &digest);
            window_cache.insert(
                0,
                (
                    digest.clone(),
                    WindowState::Prepared {
                        window,
                        epoch: request_epoch,
                    },
                ),
            );
            window_cache.truncate(SERVE_WINDOW_CACHE_MAX_ENTRIES);
            let mut out = stdout.lock();
            serde_json::to_writer(
                &mut out,
                &serde_json::json!({
                    "ok": true,
                    "request": "prepare_window",
                    "share_snapshot_sha256": digest,
                    "record_count": record_count,
                    "added_rows": 0,
                    "expired_rows": 0,
                    "touched_pages": 0,
                    "window_items_len": items.len(),
                }),
            )?;
            writeln!(out)?;
            out.write_all(&items)?;
            writeln!(out)?;
            out.flush()?;
        }
        Some("advance") => {
            let Some(base_digest) = request.base_digest else {
                respond_error(stdout, "prepare_window advance carries no base_digest", false)?;
                return Ok(());
            };
            let position = window_cache.iter().position(|(key, state)| {
                key == &base_digest && matches!(state, WindowState::Prepared { .. })
            });
            let Some(position) = position else {
                // Respawn or eviction dropped the base window; the
                // coordinator re-sends a full preparation.
                let mut out = stdout.lock();
                serde_json::to_writer(
                    &mut out,
                    &serde_json::json!({
                        "ok": false,
                        "request": "prepare_window",
                        "needs_full": true,
                        "error": format!("prepared window {base_digest} is not held"),
                    }),
                )?;
                writeln!(out)?;
                out.flush()?;
                return Ok(());
            };
            let (advance_result, base_epoch) = match &window_cache[position].1 {
                WindowState::Prepared { window, epoch } => (
                    window.advance(request.records, anchor_job_issued_at_ms),
                    *epoch,
                ),
                WindowState::Uploaded(_) => {
                    // Only prepared entries can match the predicate above,
                    // so this arm is dead -- answered rather than panicked
                    // because the position came from request-derived data.
                    respond_error(
                        stdout,
                        "prepare_window advance matched a non-prepared window",
                        false,
                    )?;
                    return Ok(());
                }
            };
            let (advanced, stats, byte_delta) = match advance_result {
                Ok(result) => result,
                Err(error) => {
                    // An advance invariant failed: the coordinator must take
                    // exactly its IncrementalWindowFallback path (clear the
                    // cache, full-rescan) rather than retiring the daemon.
                    let mut out = stdout.lock();
                    serde_json::to_writer(
                        &mut out,
                        &serde_json::json!({
                            "ok": false,
                            "request": "prepare_window",
                            "fallback": true,
                            "error": error.to_string(),
                        }),
                    )?;
                    writeln!(out)?;
                    out.flush()?;
                    return Ok(());
                }
            };
            let digest = advanced.canonical_digest_hex();
            let record_count = advanced.record_count();
            if digest == base_digest {
                // Anchor-only advance: replace the entry in place so the
                // stored anchor tracks the coordinator's, and promote it.
                let (key, _) = window_cache.remove(position);
                window_cache.insert(
                    0,
                    (
                        key,
                        WindowState::Prepared {
                            window: advanced,
                            epoch: request_epoch,
                        },
                    ),
                );
            } else {
                // The base entry stays for in-flight builds of the previous
                // generation; the bounded cache evicts it naturally.
                window_cache.retain(|(key, _)| key != &digest);
                window_cache.insert(
                    0,
                    (
                        digest.clone(),
                        WindowState::Prepared {
                            window: advanced,
                            epoch: request_epoch,
                        },
                    ),
                );
                window_cache.truncate(SERVE_WINDOW_CACHE_MAX_ENTRIES);
            }
            let mut out = stdout.lock();
            serde_json::to_writer(
                &mut out,
                &serde_json::json!({
                    "ok": true,
                    "request": "prepare_window",
                    "share_snapshot_sha256": digest,
                    "record_count": record_count,
                    "added_rows": stats.added_rows,
                    "expired_rows": stats.expired_rows,
                    "touched_pages": stats.touched_pages,
                    "retained_drop_bytes": byte_delta.retained_drop_bytes,
                    "appended_items_len": byte_delta.appended_items.len(),
                    // Diagnostics: which epoch the base was prepared under,
                    // next to the epoch this request carried. Never acted on.
                    "base_append_invalidation_epoch": base_epoch,
                    "append_invalidation_epoch": request_epoch,
                }),
            )?;
            writeln!(out)?;
            out.write_all(&byte_delta.appended_items)?;
            writeln!(out)?;
            out.flush()?;
        }
        Some(other) => {
            respond_error(
                stdout,
                &format!("unsupported prepare_window mode: {other}"),
                false,
            )?;
        }
        None => {
            respond_error(stdout, "prepare_window carries no mode", false)?;
        }
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

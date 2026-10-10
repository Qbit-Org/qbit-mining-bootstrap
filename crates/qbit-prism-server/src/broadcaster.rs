//! Recoverable, non-custodial CTV submission. A database claim coordinates
//! frontends; every attempt verifies the covenant and live parent maturity.
use crate::{
    carry_owner::{CarryOwnerSettings, PeerJournal},
    codec, config,
    coordinator::Coordinator,
    ledger::{FanoutClaim, FanoutSponsor, SubmissionHeld, SPONSOR_TAKEOVER_AFTER},
};
use anyhow::{bail, ensure, Context, Result};
use qbit_prism::{CpfpChildRequest, CtvFanoutManifest};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::watch;

/// The lease, in seconds, every fanout claim and renewal takes.
const LEASE: i64 = 120;

/// The holder's own deadline for one attempt (#654). Another frontend takes a
/// fanout over only once it has watched the claim's version go unrenewed for
/// the whole lease on its own monotonic clock, timed from a reply that comes
/// after the renewal's commit, so after the instant this holder sent it. The
/// claim's first renewal opens the attempt and every later one is sent
/// inside it, so an attempt that ends within the lease of its first renewal
/// ends before any takeover of any version it wrote. Its completion, after
/// this deadline, is fenced on the token and refused once a takeover has
/// replaced it.
const ATTEMPT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(90);
const _: () = assert!(ATTEMPT_DEADLINE.as_secs() < LEASE as u64);

pub async fn run(coordinator: Arc<Coordinator>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let mut tick = tokio::time::interval(coordinator.config.ctv_broadcast_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {_=shutdown.changed()=>break,_=tick.tick()=>{}}
        match run_pass(&coordinator, &shutdown).await {
            // #664: the health publisher logs the cluster's hold once; a pass
            // it refuses is expected for as long as the hold lasts.
            Err(error) if error.is::<SubmissionHeld>() => {
                tracing::debug!(%error, "CTV broadcaster pass held")
            }
            Err(error) => tracing::warn!(%error,"CTV broadcaster attempt deferred"),
            Ok(_) => {}
        }
    }
    Ok(())
}

pub async fn run_once(coordinator: &Coordinator) -> Result<usize> {
    let (_running, never) = watch::channel(false);
    run_pass(coordinator, &never).await
}

/// Resolves once `shutdown` is set. A closed channel that was never set
/// never resolves: `run_once` has no shutdown, and `run` ends at its own
/// `changed()` before starting another pass.
async fn stopping(shutdown: &watch::Receiver<bool>) {
    if shutdown.clone().wait_for(|stop| *stop).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Hand an abandoned attempt's claim back unrecorded, bounded so a database
/// that stops answering cannot stall the broadcaster. A failed release falls
/// back to the claim's expiry.
async fn hand_back(coordinator: &Coordinator, claim: &FanoutClaim, when: &str) {
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        coordinator.ledger.release_fanout_claim(claim),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(release)) => {
            tracing::warn!(%release,fanout=%claim.fanout_txid,"CTV claim release {when} failed; the claim waits for its expiry")
        }
        Err(_) => {
            tracing::warn!(fanout=%claim.fanout_txid,"CTV claim release {when} timed out; the claim waits for its expiry")
        }
    }
}

/// One pass over due fanouts that stops at `shutdown`: between fanouts, and
/// inside an attempt by abandoning it and handing its claim back (#573).
pub async fn run_pass(
    coordinator: &Coordinator,
    shutdown: &watch::Receiver<bool>,
) -> Result<usize> {
    // #291: with PRISM_BLOCK_SUBMIT_ENABLED off `run` starts no broadcaster
    // and `broadcast-ctv` refuses. Any other caller is refused here, before
    // the node is read or a fanout is claimed.
    coordinator
        .config
        .require_block_submission("the CTV broadcaster claimed no fanout")?;
    // #664: nor while the cluster holds block submission, whatever this
    // frontend's own switch says.
    coordinator.ledger.require_no_submission_hold().await?;
    // Tip observations recorded from here on can supersede this chain view.
    let pass_started = tokio::time::Instant::now();
    // A node behind its peers must leave their settlement claims available.
    let chain = crate::readiness::chain_info_with_metrics(
        &coordinator.rpc,
        &coordinator.config.chain,
        coordinator.config.min_peers,
        Some(&coordinator.metrics),
    )
    .await?;
    coordinator
        .ledger
        .observe_chain_view(
            chain["bestblockhash"]
                .as_str()
                .context("chain tip missing")?,
            chain["blocks"].as_u64().context("tip height missing")?,
            chain["chainwork"]
                .as_str()
                .context("cumulative chainwork missing")?,
        )
        .await?;
    let limit = config::number("PRISM_CTV_BROADCASTER_LIMIT", 100usize)?.min(1000);
    let pass_tip = chain["bestblockhash"]
        .as_str()
        .context("chain tip missing")?;
    let mut count = 0;
    for _ in 0..limit {
        // A native chunk is one claimed fanout. Never interrupt its durable
        // completion, but leave subsequent rows claimable by a later pass.
        // Compare hashes, not poll sequence numbers: same-tip polls must not
        // starve settlement. Also yield to a tip observed after this pass
        // began that differs from the pass tip, such as a replacement that
        // already published. An observation older than the pass never yields:
        // waiting for a blocked or absent refresh to catch up has no budget.
        // A replacement that keeps failing to publish holds settlement only
        // for its build budget; the pass-tip check ends with the pass.
        let tip = coordinator.observed_tip.read().await;
        let superseded = tip.superseded_since(pass_tip, pass_started);
        if tip.refresh_pending(coordinator.config.template_refresh_failure_exit) || superseded {
            coordinator.metrics.record_ctv_tip_refresh_yield();
            break;
        }
        drop(tip);
        if *shutdown.borrow() {
            break;
        }
        let Some(claim) = coordinator.ledger.claim_fanout(LEASE).await? else {
            break;
        };
        let started = std::time::Instant::now();
        let outcome = tokio::select! {
            biased;
            outcome = tokio::time::timeout(
                ATTEMPT_DEADLINE,
                process(coordinator, &claim),
            ) => outcome,
            () = stopping(shutdown) => {
                // #573: a shutdown abandons the attempt, as its deadline
                // would, and hands the claim back at once instead of making
                // another frontend wait for the lease. Nothing is recorded:
                // the next claim re-verifies the chain from scratch, as after
                // an expiry, and anything this attempt sent is already known
                // to the node.
                hand_back(coordinator, &claim, "at shutdown").await;
                break;
            }
        };
        let finished = match outcome {
            // #664: a hold set during the pass refused the send. Hand the claim
            // back unattempted, as at shutdown, and end the pass: the next
            // claim re-verifies the chain from scratch once the hold clears.
            Ok(Err(error)) if error.is::<SubmissionHeld>() => {
                hand_back(coordinator, &claim, "under the submission hold").await;
                return Err(error);
            }
            Ok(Ok((status, result))) => {
                let attempted = result.clone();
                let finished = coordinator
                    .ledger
                    .finish_fanout(&claim, status, Some(result), None)
                    .await;
                // #569: a completion refused after a good attempt, such as a
                // landing that moved the payout revision, must not hold the
                // claim for its whole lease. Handing it back is an early
                // expiry: the next claim re-verifies the chain from scratch,
                // and anything this attempt sent is already known to the
                // node. #573: it is due again behind the rows already due, so
                // a row refused on every attempt cannot hold up the others,
                // and a send is recorded as an attempt.
                if let Err(refused) = &finished {
                    if let Err(release) = coordinator
                        .ledger
                        .requeue_refused_fanout(
                            &claim,
                            &attempted,
                            &format!("completion not persisted: {refused:#}"),
                        )
                        .await
                    {
                        tracing::warn!(%release,fanout=%claim.fanout_txid,"CTV claim release deferred");
                    }
                }
                finished
            }
            result => {
                let error = match result {
                    Ok(Err(error)) => error,
                    _ => anyhow::anyhow!("CTV attempt deadline exceeded"),
                };
                if let Err(finish) = coordinator
                    .ledger
                    .finish_fanout(&claim, "failed", None, Some(&error.to_string()))
                    .await
                {
                    tracing::warn!(%finish,"CTV claim completion deferred");
                    // #573: the failed attempt and its backoff must not be
                    // lost with the completion, or the row is retried as soon
                    // as the claim expires.
                    if let Err(release) = coordinator
                        .ledger
                        .release_failed_fanout(&claim, &error.to_string())
                        .await
                    {
                        tracing::warn!(%release,fanout=%claim.fanout_txid,"CTV claim release deferred");
                    }
                }
                tracing::warn!(%error,fanout=%claim.fanout_txid,"CTV broadcast deferred");
                Ok(())
            }
        };
        // The chunk was attempted whether or not its completion persisted.
        count += 1;
        coordinator.metrics.observe_ctv_chunk(started.elapsed());
        finished?;
        tokio::task::yield_now().await;
    }
    Ok(count)
}

fn observation(status: &'static str, details: Value, seconds: u64) -> (&'static str, Value) {
    let mut value = details;
    value["check_only"] = json!(true);
    value["next_check_seconds"] = json!(seconds);
    (status, value)
}

fn confirmed(hash: &str, height: u64, tip: u64) -> Result<(&'static str, Value)> {
    let depth = tip
        .checked_sub(height)
        .context("confirmation above active tip")?
        + 1;
    Ok(observation(
        "confirmed",
        json!({"confirmation":{"block_hash":hash,"block_height":height,"confirmations":depth}}),
        if depth >= 1000 { 60 } else { 5 },
    ))
}

async fn process(coordinator: &Coordinator, claim: &FanoutClaim) -> Result<(&'static str, Value)> {
    coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
    let chain = crate::readiness::chain_info_with_metrics(
        &coordinator.rpc,
        &coordinator.config.chain,
        coordinator.config.min_peers,
        Some(&coordinator.metrics),
    )
    .await?;
    let revision = coordinator
        .ledger
        .observe_chain_view(
            chain["bestblockhash"]
                .as_str()
                .context("chain tip missing")?,
            chain["blocks"].as_u64().context("tip height missing")?,
            chain["chainwork"]
                .as_str()
                .context("cumulative chainwork missing")?,
        )
        .await?;
    let (status, mut result) = process_view(coordinator, claim, &chain, revision).await?;
    result["payout_revision"] = json!(revision);
    Ok((status, result))
}

async fn process_view(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    chain: &Value,
    revision: i64,
) -> Result<(&'static str, Value)> {
    let manifest: CtvFanoutManifest = serde_json::from_value(claim.manifest.clone())?;
    qbit_prism::verify_ctv_fanout_manifest_structure(&manifest)?;
    ensure!(
        manifest.fanout_txid == claim.fanout_txid,
        "fanout claim identity mismatch"
    );
    let rpc = &coordinator.rpc;
    let tip_hash = chain["bestblockhash"].clone();
    let block = rpc
        .call("getblockheader", json!([claim.block_hash]))
        .await?;
    ensure!(
        block["confirmations"].as_i64().unwrap_or(-1) > 0,
        "fanout coinbase is not active"
    );
    let height = block["height"]
        .as_u64()
        .context("coinbase block height missing")?;
    let tip = chain["blocks"].as_u64().context("tip height missing")?;
    let mature_height = height
        .checked_add(qbit_prism::QBIT_COINBASE_MATURITY_BLOCKS)
        .context("coinbase height overflow")?;
    ensure!(tip >= mature_height, "fanout coinbase is immature");
    maintain_funding_reservation(coordinator, claim).await?;
    if let (Some(hash), Some(height)) = (
        claim.progress["confirmed_block_hash"].as_str(),
        claim.progress["confirmed_block_height"].as_u64(),
    ) {
        ensure!(height <= tip, "node tip is behind tracked CTV confirmation");
        let active = rpc.call("getblockhash", json!([height])).await? == json!(hash);
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == tip_hash,
            "tip changed during CTV confirmation check"
        );
        if active {
            return confirmed(hash, height, tip);
        }
        if claim.progress["confirmed_depth"].as_u64().unwrap_or(0) >= 1000 {
            coordinator
                .ledger
                .halt_fanout_reorg(claim, revision)
                .await?;
            bail!("deep confirmed CTV checkpoint disconnected");
        }
    }
    if let Ok(tx) = rpc
        .call("getrawtransaction", json!([claim.fanout_txid, true]))
        .await
    {
        if tx["confirmations"].as_u64().unwrap_or(0) > 0 {
            let hash = tx["blockhash"]
                .as_str()
                .context("confirmed transaction lacks block hash")?;
            let header = rpc.call("getblockheader", json!([hash])).await?;
            let confirmed_height = header["height"]
                .as_u64()
                .context("confirmation height missing")?;
            ensure!(
                rpc.call("getblockhash", json!([confirmed_height])).await? == json!(hash),
                "fanout confirmation moved"
            );
            ensure!(
                rpc.call("getbestblockhash", json!([])).await? == tip_hash,
                "tip changed during CTV confirmation check"
            );
            return confirmed(hash, confirmed_height, tip);
        }
    }
    let coin = rpc
        .call(
            "gettxout",
            json!([
                manifest.parent_coinbase_txid,
                manifest.parent_coinbase_vout,
                false
            ]),
        )
        .await?;
    if coin.is_null() {
        return scan_spender(coordinator, claim, &manifest, mature_height, tip, &tip_hash).await;
    }
    ensure!(
        coin["scriptPubKey"]["hex"] == manifest.covenant_script_pubkey_hex,
        "live covenant script mismatch"
    );
    ensure!(
        amount_bits(&coin["value"])? == manifest.covenant_output_value_sats,
        "live covenant value mismatch"
    );
    if rpc
        .call("getmempoolentry", json!([manifest.fanout_txid]))
        .await
        .is_ok()
    {
        return Ok(observation(
            "broadcast_submitted",
            json!({"already_in_mempool":true}),
            5,
        ));
    }
    let fee = config::number(
        if config::optional("PRISM_CTV_BROADCASTER_FEE_BITS").is_some() {
            "PRISM_CTV_BROADCASTER_FEE_BITS"
        } else {
            "PRISM_CTV_BROADCASTER_FEE_SATS"
        },
        0u64,
    )?;
    let result = if manifest.precommitment.fanout_fee_sats > 0 {
        // Built-in-fee fanouts are anchorless and need no wallet sponsorship.
        coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
        coordinator.ledger.require_no_submission_hold().await?;
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == tip_hash,
            "tip changed before CTV submission"
        );
        rpc.call("sendrawtransaction", json!([manifest.fanout_tx_hex]))
            .await?
    } else {
        // Dual writer: each node funds a child from its own wallet and
        // reserves the coin in its own ledger, so two sponsors' children
        // would conflict and leave one coin locked. The node whose work found
        // the block sponsors; the other only watches until the fanout is
        // overdue and its finder silent (`Ledger::fanout_sponsor`).
        if !sponsors_fanout(coordinator, &claim.fanout_txid).await? {
            return Ok(observation(
                "broadcastable",
                json!({"sponsor":"finder_node"}),
                60,
            ));
        }
        ensure!(fee > 0, "zero-fee fanout requires CPFP fee sponsorship");
        let child = build_child(coordinator, claim, &manifest, fee).await?;
        coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
        coordinator.ledger.require_no_submission_hold().await?;
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == tip_hash,
            "tip changed before CTV package submission"
        );
        let result = rpc
            .call("submitpackage", json!([[manifest.fanout_tx_hex, child]]))
            .await?;
        ensure!(
            result["package_msg"] == "success",
            "CTV package rejected: {result}"
        );
        result
    };
    Ok(("broadcast_submitted", json!({"submit_result":result})))
}

async fn scan_spender(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    manifest: &CtvFanoutManifest,
    mature_height: u64,
    tip: u64,
    tip_hash: &Value,
) -> Result<(&'static str, Value)> {
    let rpc = &coordinator.rpc;
    let budget = config::number("PRISM_CTV_SPEND_SCAN_BLOCKS", 32u64)?;
    ensure!(
        (1..=256).contains(&budget),
        "PRISM_CTV_SPEND_SCAN_BLOCKS must be 1..256"
    );
    let mut next = claim.progress["scan_next_height"]
        .as_u64()
        .unwrap_or(mature_height)
        .max(mature_height);
    if let (Some(height), Some(hash)) = (
        claim.progress["scan_anchor_height"].as_u64(),
        claim.progress["scan_anchor_hash"].as_str(),
    ) {
        if height > tip || rpc.call("getblockhash", json!([height])).await? != json!(hash) {
            next = mature_height;
        }
    }
    let end = tip.min(next.saturating_add(budget - 1));
    for number in next..=end {
        coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
        let hash = rpc.call("getblockhash", json!([number])).await?;
        let block = rpc.call("getblock", json!([hash, 2])).await?;
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == *tip_hash,
            "tip changed during CTV spend scan"
        );
        for tx in block["tx"].as_array().into_iter().flatten() {
            if tx["vin"].as_array().into_iter().flatten().any(|input| {
                input["txid"] == manifest.parent_coinbase_txid
                    && input["vout"] == manifest.parent_coinbase_vout
            }) {
                ensure!(
                    tx["txid"] == manifest.fanout_txid,
                    "covenant spent by an unexpected transaction"
                );
                return confirmed(hash.as_str().context("block hash missing")?, number, tip);
            }
        }
        // Persist every completed page. Timeouts and process death do not
        // restart a years-old no-txindex search at its first block.
        coordinator
            .ledger
            .record_fanout_scan(
                claim,
                number + 1,
                Some((number, hash.as_str().context("block hash missing")?.into())),
            )
            .await?;
    }
    Ok(observation(
        "broadcastable",
        json!({"spend_scan_pending":true,"next_height":end.saturating_add(1)}),
        if next > tip { 10 } else { 1 },
    ))
}

/// Dual writer: the broadcaster's view of whether the peer's node, the
/// finder of the fanouts it may take over, is silent ([`finder_silent`]),
/// kept by the coordinator. One check runs at a time, and its verdict serves
/// every fanout for [`VERDICT_TTL`].
#[derive(Default)]
pub struct FinderLiveness {
    /// The peer's database: unopened, opened, or unusable (logged once).
    journal: std::sync::Mutex<Option<Option<Arc<PeerJournal>>>>,
    state: tokio::sync::Mutex<LivenessState>,
}

#[derive(Debug, Default)]
struct LivenessState {
    /// The last check: when, and its verdict.
    verdict: Option<(std::time::Instant, bool)>,
    /// The outage the last check found, if it found one.
    outage: Option<Outage>,
}

/// Checks that each found the peer's database out of reach, with no check
/// between them that reached it and no gap longer than [`OUTAGE_GAP`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Outage {
    since: std::time::Instant,
    failed_checks: u32,
}

impl FinderLiveness {
    fn journal(&self, settings: &CarryOwnerSettings) -> Option<Arc<PeerJournal>> {
        let mut journal = self
            .journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        journal
            .get_or_insert_with(|| {
                match PeerJournal::new(&settings.peer_urls, settings.peer_timeout) {
                    Ok(opened) => Some(Arc::new(opened)),
                    Err(error) => {
                        tracing::error!(
                            error = %format!("{error:#}"),
                            "CTV broadcaster: the peer's database URL is unusable, so no zero-fee fanout found on the peer's work is taken over"
                        );
                        None
                    }
                }
            })
            .clone()
    }

    /// For tests: read the peer's database through `journal`, forgetting
    /// every earlier check.
    #[doc(hidden)]
    pub async fn use_journal(&self, journal: PeerJournal) {
        *self
            .journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Some(Arc::new(journal)));
        *self.state.lock().await = LivenessState::default();
    }

    /// For tests: checks have found the peer's database out of reach since
    /// `since`, as often as an outage needs, the last one long enough ago
    /// for the next to run.
    #[doc(hidden)]
    pub async fn assume_unreachable_since(&self, since: std::time::Instant) {
        let mut state = self.state.lock().await;
        state.outage = Some(Outage {
            since,
            failed_checks: OUTAGE_CHECKS - 1,
        });
        state.verdict = Some((std::time::Instant::now() - VERDICT_TTL, false));
    }
}

/// How long one check's verdict serves.
const VERDICT_TTL: std::time::Duration = std::time::Duration::from_secs(30);
/// How long, and over how many checks, the peer's database must keep failing
/// before its node counts as silent: a blip, or two, only waits.
const OUT_OF_REACH_AFTER: std::time::Duration = std::time::Duration::from_secs(60);
const OUTAGE_CHECKS: u32 = 3;
/// The longest gap between two failed checks of one outage: an older
/// failure belongs to an outage that may have ended unobserved.
const OUTAGE_GAP: std::time::Duration = std::time::Duration::from_secs(120);

/// The outage a failed check at `now` belongs to: the one the previous
/// check, at `last_check`, found, if that was at most [`OUTAGE_GAP`] ago, or
/// a new one.
fn continue_outage(
    outage: Option<Outage>,
    last_check: Option<std::time::Instant>,
    now: std::time::Instant,
) -> Outage {
    match (outage, last_check) {
        (Some(outage), Some(last)) if now.saturating_duration_since(last) <= OUTAGE_GAP => Outage {
            failed_checks: outage.failed_checks + 1,
            ..outage
        },
        _ => Outage {
            since: now,
            failed_checks: 1,
        },
    }
}

impl Outage {
    /// Whether it has lasted long enough, over enough checks, for the peer's
    /// node to count as silent.
    fn long_enough(&self, now: std::time::Instant) -> bool {
        self.failed_checks >= OUTAGE_CHECKS
            && now.saturating_duration_since(self.since) >= OUT_OF_REACH_AFTER
    }
}

/// Dual writer: whether this node funds the zero-fee fanout `fanout_txid`'s
/// CPFP child: as the node its block was found on, to finish a package it
/// holds, or to take it over once it is overdue (the ledger's fence,
/// `Ledger::fanout_sponsor`) and its finder silent. Always on a single
/// writer.
pub async fn sponsors_fanout(coordinator: &Coordinator, fanout_txid: &str) -> Result<bool> {
    match coordinator.ledger.fanout_sponsor(fanout_txid).await? {
        Some(FanoutSponsor::Finder | FanoutSponsor::Held) => Ok(true),
        Some(FanoutSponsor::Overdue) => {
            let silent = finder_silent(coordinator).await?;
            if silent {
                tracing::warn!(
                    fanout_txid,
                    takeover_minutes = SPONSOR_TAKEOVER_AFTER.as_secs() / 60,
                    "CTV broadcaster: taking over CPFP sponsorship of a zero-fee fanout found on the peer's work: its block matured here, and the peer's node has published no work, for longer than the takeover delay"
                );
            }
            Ok(silent)
        }
        None => Ok(false),
    }
}

/// Whether the finder of an overdue fanout, the peer's node, is silent.
///
/// - **Its database answers** (through any of its URLs): silent if the
///   youngest newest work it shows of its own node, aged by its own clock, is
///   [`SPONSOR_TAKEOVER_AFTER`] old. A frontend that is alive publishes work
///   every few minutes (the reanchor and the template's maximum age).
/// - **It has failed [`OUTAGE_CHECKS`] checks in a row, over
///   [`OUT_OF_REACH_AFTER`] at least:** silent.
///   The overdue fence has already found the finder's work this node holds as
///   old as the takeover delay. A finder that is alive but cut off from this
///   node that long looks silent, and both nodes may then fund a child
///   (docs/prism-ledger-ops.md).
/// - **A shorter outage:** not silent yet.
async fn finder_silent(coordinator: &Coordinator) -> Result<bool> {
    let settings = match CarryOwnerSettings::from_config(&coordinator.config) {
        Ok(settings) => settings,
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "CTV broadcaster: no dual-writer settings, so the fanout is not taken over");
            return Ok(false);
        }
    };
    let liveness = &coordinator.finder_liveness;
    let Some(journal) = liveness.journal(&settings) else {
        return Ok(false);
    };
    let mut state = liveness.state.lock().await;
    if let Some((at, silent)) = state.verdict {
        if at.elapsed() < VERDICT_TTL {
            return Ok(silent);
        }
    }
    let peer = crate::carry_owner::peer_index(settings.node_index);
    let answer = journal.work_age(peer).await;
    let now = std::time::Instant::now();
    let silent = match answer {
        Some(age) => {
            state.outage = None;
            age.is_none_or(|age| age >= SPONSOR_TAKEOVER_AFTER)
        }
        None => {
            let outage = continue_outage(state.outage, state.verdict.map(|(at, _)| at), now);
            state.outage = Some(outage);
            outage.long_enough(now)
        }
    };
    state.verdict = Some((now, silent));
    Ok(silent)
}

async fn build_child(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    manifest: &CtvFanoutManifest,
    fee: u64,
) -> Result<String> {
    let anchor = manifest
        .precommitment
        .anchor_vout
        .context("fanout has no CPFP anchor")?;
    let mut package = coordinator.ledger.cpfp_package(&claim.fanout_txid).await?;
    for _ in 0..3 {
        if package.is_none() {
            package = select_cpfp_funding(coordinator, claim, fee).await?;
        }
        let current = package
            .as_ref()
            .context("sponsorship wallet has no unreserved suitable UTXO")?;
        if current["signed_child_hex"].is_string() {
            break;
        }
        match unsigned_funding_invalid_reason(coordinator, current, fee).await? {
            None => break,
            Some(reason) => {
                retire_unsigned_funding(coordinator, claim, current, reason).await?;
                package = None;
            }
        }
    }
    let package =
        package.context("sponsorship funding changed during selection; retry required")?;
    // A committed package can be relayed by any node. Requiring the original
    // wallet here would prevent recovery after mempool loss or node failover.
    if let Some(raw) = package["signed_child_hex"].as_str() {
        ensure!(
            package["wallet_lock_released"] != true,
            "unconfirmed CPFP funding reservation must be repaired before replay"
        );
        let stripped = codec::strip_witness_transaction(&hex::decode(raw)?)?;
        ensure!(
            package["child_txid"] == codec::hash_display(&codec::double_sha256(&stripped)),
            "persisted CPFP child txid mismatch"
        );
        return Ok(raw.into());
    }
    let wallet = coordinator.rpc.wallet(
        package["wallet_name"]
            .as_str()
            .context("reserved wallet missing")?,
    )?;
    let outpoint = json!({"txid":package["funding_txid"],"vout":package["funding_vout"]});
    // Persisting an existing lock is idempotent and also upgrades reservations
    // left by older processes that only held an in-memory wallet lock.
    coordinator
        .ledger
        .mark_cpfp_wallet_lock_pending(claim)
        .await?;
    coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
    ensure!(
        wallet
            .call("lockunspent", json!([false, [outpoint], true]))
            .await?
            == true,
        "failed to reserve funding wallet UTXO"
    );
    let address = wallet.call("getnewaddress", json!(["", "p2mr"])).await?;
    let info = wallet.call("getaddressinfo", json!([address])).await?;
    let child = qbit_prism::build_cpfp_child(&CpfpChildRequest {
        fanout_txid: manifest.fanout_txid.clone(),
        anchor_vout: anchor,
        funding_txid: package["funding_txid"]
            .as_str()
            .context("reserved funding txid missing")?
            .into(),
        funding_vout: package["funding_vout"]
            .as_u64()
            .context("reserved funding vout missing")?
            .try_into()?,
        funding_value_sats: package["funding_value_sats"]
            .as_u64()
            .context("reserved funding value missing")?,
        fee_sats: fee,
        change_script_pubkey_hex: info["scriptPubKey"]
            .as_str()
            .context("change script missing")?
            .into(),
    })?;
    coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
    let signed=wallet.call("signrawtransactionwithwallet",json!([child.unsigned_child_tx_hex,[{"txid":manifest.fanout_txid,"vout":anchor,"scriptPubKey":qbit_prism::P2A_ANCHOR_SCRIPT_PUBKEY_HEX,"amount":0}]])).await?;
    ensure!(
        signed["complete"] == true,
        "funding wallet did not complete signature"
    );
    let raw = signed["hex"]
        .as_str()
        .context("signed child missing")?
        .to_owned();
    let stripped = codec::strip_witness_transaction(&hex::decode(&raw)?)?;
    ensure!(
        stripped == codec::strip_witness_transaction(&hex::decode(&child.unsigned_child_tx_hex)?)?,
        "wallet changed the CPFP transaction"
    );
    if let Some(reason) = unsigned_funding_invalid_reason(coordinator, &package, fee).await? {
        retire_unsigned_funding(coordinator, claim, &package, reason).await?;
        bail!("CPFP funding changed before signed package persistence");
    }
    let txid = codec::hash_display(&codec::double_sha256(&stripped));
    coordinator
        .ledger
        .save_cpfp_package(claim, &raw, &txid)
        .await?;
    Ok(raw)
}

async fn select_cpfp_funding(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    fee: u64,
) -> Result<Option<Value>> {
    let wallet_name = config::optional("PRISM_CTV_BROADCASTER_WALLET")
        .context("CPFP requires PRISM_CTV_BROADCASTER_WALLET")?;
    let wallet = coordinator.rpc.wallet(&wallet_name)?;
    let utxos = wallet
        .call("listunspent", json!([1, 9_999_999, [], true]))
        .await?;
    let mut eligible = Vec::new();
    for utxo in utxos
        .as_array()
        .context("listunspent did not return an array")?
    {
        if utxo["spendable"] == true {
            let amount = amount_bits(&utxo["amount"])?;
            if amount > fee {
                eligible.push((amount, utxo));
            }
        }
    }
    eligible.sort_by_key(|(amount, _)| std::cmp::Reverse(*amount));
    for (amount, utxo) in eligible {
        let txid = utxo["txid"].as_str().context("funding txid missing")?;
        let vout = utxo["vout"]
            .as_u64()
            .context("funding vout missing")?
            .try_into()?;
        if coordinator
            .ledger
            .reserve_cpfp_funding(claim, &wallet_name, txid, vout, amount)
            .await?
        {
            return coordinator.ledger.cpfp_package(&claim.fanout_txid).await;
        }
    }
    Ok(None)
}

/// A failed RPC is uncertainty, not evidence that a reservation is unusable.
/// Signed packages never enter this replacement path.
async fn unsigned_funding_invalid_reason(
    coordinator: &Coordinator,
    package: &Value,
    fee: u64,
) -> Result<Option<&'static str>> {
    ensure!(
        package["signed_child_hex"].is_null(),
        "signed CPFP package is immutable"
    );
    let tip = coordinator.rpc.call("getbestblockhash", json!([])).await?;
    let coin = coordinator
        .rpc
        .call(
            "gettxout",
            json!([package["funding_txid"], package["funding_vout"], true]),
        )
        .await?;
    ensure!(
        coordinator.rpc.call("getbestblockhash", json!([])).await? == tip,
        "chain changed while validating unsigned CPFP funding"
    );
    if coin.is_null() {
        return Ok(Some(
            "funding output is spent, missing, or spent in the mempool",
        ));
    }
    let confirmations = coin["confirmations"]
        .as_u64()
        .context("funding confirmation count missing")?;
    if confirmations == 0
        || (coin["coinbase"] == true && confirmations < qbit_prism::QBIT_COINBASE_MATURITY_BLOCKS)
    {
        return Ok(Some("funding output is no longer confirmed and mature"));
    }
    let amount = amount_bits(&coin["value"])?;
    if Some(amount) != package["funding_value_sats"].as_u64() || amount <= fee {
        return Ok(Some("funding output value cannot fund the current fee"));
    }
    let wallet = coordinator.rpc.wallet(
        package["wallet_name"]
            .as_str()
            .context("reserved wallet missing")?,
    )?;
    // A wallet that cannot identify its original transaction may simply be
    // unavailable on this server. Preserve the reservation and retry there.
    let known = wallet
        .call("gettransaction", json!([package["funding_txid"]]))
        .await?;
    if known["confirmations"]
        .as_i64()
        .context("wallet funding confirmations missing")?
        <= 0
    {
        return Ok(Some("wallet funding transaction is no longer active"));
    }
    let available = wallet
        .call("listunspent", json!([1, 9_999_999, [], true]))
        .await?;
    if available
        .as_array()
        .context("wallet UTXO list missing")?
        .iter()
        .any(|coin| {
            coin["txid"] == package["funding_txid"]
                && coin["vout"] == package["funding_vout"]
                && coin["spendable"] == true
        })
    {
        return Ok(None);
    }
    let outpoint = json!({"txid":package["funding_txid"],"vout":package["funding_vout"]});
    let locked = wallet.call("listlockunspent", json!([])).await?;
    if locked
        .as_array()
        .context("wallet locked UTXO list missing")?
        .contains(&outpoint)
    {
        // listunspent excludes our existing lock. Confirm control of its
        // actual script rather than replacing valid crash-recovered funding.
        let address = coin["scriptPubKey"]["address"]
            .as_str()
            .context("locked funding address missing")?;
        let info = wallet.call("getaddressinfo", json!([address])).await?;
        if info["ismine"] == true && info["solvable"] == true {
            return Ok(None);
        }
    }
    Ok(Some("wallet funding output is no longer spendable"))
}

async fn retire_unsigned_funding(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    package: &Value,
    reason: &str,
) -> Result<()> {
    coordinator
        .ledger
        .retire_unsigned_cpfp_funding(
            claim,
            package["funding_txid"]
                .as_str()
                .context("reserved funding txid missing")?,
            package["funding_vout"]
                .as_u64()
                .context("reserved funding vout missing")?
                .try_into()?,
            reason,
        )
        .await
}

async fn require_retired_funding_wallet_control(
    rpc: &crate::rpc::Rpc,
    wallet: &crate::rpc::Rpc,
    txid: &str,
    vout: u32,
) -> Result<()> {
    let known = wallet.call("gettransaction", json!([txid])).await?;
    ensure!(
        known["txid"] == txid,
        "retired funding wallet did not identify its transaction"
    );
    // A transaction can pay two independently hosted wallets. Knowing
    // its txid is insufficient to discharge the other wallet's lock.
    let decoded = rpc
        .call(
            "decoderawtransaction",
            json!([known["hex"]
                .as_str()
                .context("retired funding transaction bytes missing")?]),
        )
        .await?;
    let output = decoded["vout"]
        .as_array()
        .context("retired funding outputs missing")?
        .iter()
        .find(|output| output["n"].as_u64() == Some(u64::from(vout)))
        .context("retired funding output missing")?;
    let address = output["scriptPubKey"]["address"]
        .as_str()
        .context("retired funding address missing")?;
    let info = wallet.call("getaddressinfo", json!([address])).await?;
    ensure!(
        info["ismine"] == true && info["solvable"] == true,
        "retired funding output is not controlled by this wallet"
    );
    Ok(())
}

async fn cleanup_retired_funding(coordinator: &Coordinator, claim: &FanoutClaim) -> Result<()> {
    for package in coordinator
        .ledger
        .retired_cpfp_funding(&claim.fanout_txid)
        .await?
    {
        let txid = package["funding_txid"]
            .as_str()
            .context("retired funding txid missing")?;
        let vout: u32 = package["funding_vout"]
            .as_u64()
            .context("retired funding vout missing")?
            .try_into()?;
        // Rotate the attempt before RPC so one unavailable wallet or Qbit's
        // unremovable spent lock cannot starve subsequent cleanup records.
        coordinator
            .ledger
            .record_retired_cpfp_wallet_cleanup(claim, txid, vout, false)
            .await?;
        let cleanup = async {
            let wallet = coordinator.rpc.wallet(
                package["wallet_name"]
                    .as_str()
                    .context("retired wallet missing")?,
            )?;
            require_retired_funding_wallet_control(&coordinator.rpc, &wallet, txid, vout).await?;
            let outpoint = json!({"txid":txid,"vout":vout});
            let locked = wallet.call("listlockunspent", json!([])).await?;
            if locked
                .as_array()
                .context("wallet locked UTXO list missing")?
                .contains(&outpoint)
            {
                coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
                ensure!(
                    wallet
                        .call("lockunspent", json!([true, [outpoint]]))
                        .await?
                        == true,
                    "retired funding UTXO unlock failed"
                );
            }
            coordinator
                .ledger
                .mark_retired_cpfp_wallet_unlocked(claim, txid, vout)
                .await
        }
        .await;
        if let Err(error) = cleanup {
            tracing::warn!(%error,fanout=%claim.fanout_txid,funding=%txid,"retired funding wallet cleanup deferred");
        }
    }
    Ok(())
}

async fn maintain_funding_reservation(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
) -> Result<()> {
    if !matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cleanup_retired_funding(coordinator, claim)
        )
        .await,
        Ok(Ok(()))
    ) {
        tracing::warn!(fanout=%claim.fanout_txid,"retired funding cleanup deferred");
    }
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        maintain_funding_reservation_inner(coordinator, claim),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(%error,fanout=%claim.fanout_txid,"sponsorship wallet reservation maintenance deferred")
        }
        Err(_) => {
            tracing::warn!(fanout=%claim.fanout_txid,"sponsorship wallet reservation maintenance deadline exceeded")
        }
    }
    Ok(())
}

async fn maintain_funding_reservation_inner(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
) -> Result<()> {
    let Some(package) = coordinator.ledger.cpfp_package(&claim.fanout_txid).await? else {
        return Ok(());
    };
    if package["signed_child_hex"].is_null() {
        return Ok(());
    }
    let wallet = coordinator.rpc.wallet(
        package["wallet_name"]
            .as_str()
            .context("reserved wallet missing")?,
    )?;
    let outpoint = json!({"txid":package["funding_txid"],"vout":package["funding_vout"]});
    // Excluding mempool spends is essential: an unconfirmed child may be
    // evicted and its exact durable bytes must remain valid for rebroadcast.
    let unspent = coordinator
        .rpc
        .call(
            "gettxout",
            json!([package["funding_txid"], package["funding_vout"], false]),
        )
        .await?;
    if !unspent.is_null() {
        // Qbit removes explicit locks when it learns a wallet spend. Its
        // non-abandoned/non-conflicted pending transaction then excludes the
        // input from coin selection, including after mempool eviction.
        let mut protected = false;
        if let Some(child_txid) = package["child_txid"].as_str() {
            if let Ok(child) = wallet.call("gettransaction", json!([child_txid])).await {
                let pending = child["txid"] == child_txid
                    && child["confirmations"].as_i64() == Some(0)
                    && child["walletconflicts"]
                        .as_array()
                        .is_some_and(Vec::is_empty)
                    && child["mempoolconflicts"]
                        .as_array()
                        .is_some_and(Vec::is_empty)
                    && child["details"].as_array().is_some_and(|details| {
                        !details.is_empty()
                            && details.iter().any(|detail| detail["abandoned"] == false)
                            && details.iter().all(|detail| detail["abandoned"] != true)
                    });
                if pending {
                    let available = wallet
                        .call("listunspent", json!([0, 9_999_999, [], true]))
                        .await?;
                    protected = available
                        .as_array()
                        .context("wallet UTXO list missing")?
                        .iter()
                        .all(|coin| {
                            coin["txid"] != package["funding_txid"]
                                || coin["vout"] != package["funding_vout"]
                        });
                }
            }
        }
        if !protected {
            coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
            ensure!(
                wallet
                    .call("lockunspent", json!([false, [outpoint], true]))
                    .await?
                    == true,
                "failed to retain persistent funding wallet lock"
            );
        }
        // Do not erase evidence of an older premature release until the
        // wallet has proved or successfully restored its protection.
        if package["wallet_lock_released"] == true {
            coordinator
                .ledger
                .mark_cpfp_wallet_lock_pending(claim)
                .await?;
        }
        return Ok(());
    }
    if package["wallet_lock_released"] == true {
        return Ok(());
    }
    // A missing UTXO alone could also mean its funding transaction was
    // disconnected. Require the signed child's actual active confirmation
    // before releasing the wallet lock. A walletless relay can safely defer
    // this cleanup until the sponsorship wallet becomes available again.
    let tip = coordinator.rpc.call("getbestblockhash", json!([])).await?;
    let child = wallet
        .call(
            "gettransaction",
            json!([package["child_txid"]
                .as_str()
                .context("signed CPFP child missing")?]),
        )
        .await?;
    if child["confirmations"].as_u64().unwrap_or(0) == 0 {
        return Ok(());
    }
    let block_hash = child["blockhash"]
        .as_str()
        .context("confirmed child block missing")?;
    let header = coordinator
        .rpc
        .call("getblockheader", json!([block_hash]))
        .await?;
    ensure!(
        header["confirmations"].as_u64().unwrap_or(0) > 0,
        "CPFP child confirmation is not active"
    );
    let height = header["height"]
        .as_u64()
        .context("CPFP child height missing")?;
    ensure!(
        coordinator
            .rpc
            .call("getblockhash", json!([height]))
            .await?
            == json!(block_hash)
            && coordinator.rpc.call("getbestblockhash", json!([])).await? == tip,
        "chain changed while checking CPFP child confirmation"
    );
    let locked = wallet.call("listlockunspent", json!([])).await?;
    if locked
        .as_array()
        .is_some_and(|rows| rows.contains(&outpoint))
    {
        // Qbit 1.0 can retain a restored lock for an already-known child, yet
        // reject per-output unlock once that child spends it. Keep cleanup
        // pending on that error; unlocking all coins would endanger other
        // reservations in the sponsorship wallet.
        coordinator.ledger.renew_fanout_claim(claim, LEASE).await?;
        ensure!(
            wallet
                .call("lockunspent", json!([true, [outpoint]]))
                .await?
                == true,
            "funding UTXO unlock failed"
        );
    }
    coordinator.ledger.mark_cpfp_wallet_unlocked(claim).await?;
    Ok(())
}

/// RPC monetary values are decimal amounts, never floating-point balances.
pub fn amount_bits(value: &Value) -> Result<u64> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => bail!("invalid monetary value"),
    };
    let (mantissa, exponent) = text
        .split_once(['e', 'E'])
        .map_or((text.as_str(), 0), |(m, e)| {
            (m, e.parse::<i32>().unwrap_or(i32::MIN))
        });
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    ensure!(
        whole
            .bytes()
            .chain(fraction.bytes())
            .all(|c| c.is_ascii_digit()),
        "invalid monetary decimal"
    );
    let digits = format!("{whole}{fraction}").parse::<u128>()?;
    let power = 8i32
        .checked_add(exponent)
        .and_then(|x| x.checked_sub(fraction.len() as i32))
        .context("monetary exponent overflow")?;
    ensure!(
        (-38..=38).contains(&power),
        "monetary exponent out of bounds"
    );
    let amount = if power >= 0 {
        digits
            .checked_mul(10u128.pow(power as u32))
            .context("monetary value overflow")?
    } else {
        let divisor = 10u128.pow((-power) as u32);
        ensure!(
            digits % divisor == 0,
            "monetary value has sub-bit precision"
        );
        digits / divisor
    };
    Ok(amount.try_into()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retired_cleanup_requires_control_of_the_reserved_output() {
        use axum::{extract::OriginalUri, Json, Router};
        async fn reply(OriginalUri(uri): OriginalUri, Json(request): Json<Value>) -> Json<Value> {
            let result = match request["method"].as_str().unwrap() {
                "gettransaction" => {
                    // Both wallets know this batched funding transaction, but
                    // they control different outputs in it.
                    assert_eq!(request["params"], json!(["funding"]));
                    json!({"txid":"funding","hex":"shared-transaction"})
                }
                "decoderawtransaction" => {
                    assert_eq!(request["params"], json!(["shared-transaction"]));
                    json!({"vout":[
                        {"n":0,"scriptPubKey":{"address":"recipient-a"}},
                        {"n":1,"scriptPubKey":{"address":"recipient-b"}}
                    ]})
                }
                "getaddressinfo" => {
                    let owned = (uri.path() == "/wallet/a"
                        && request["params"][0] == "recipient-a")
                        || (uri.path() == "/wallet/b" && request["params"][0] == "recipient-b");
                    // Being able to solve a public script alone is not control.
                    json!({"ismine":owned,"solvable":true})
                }
                _ => panic!("unexpected wallet mutation or cleanup before ownership proof"),
            };
            Json(json!({"id":request["id"],"error":null,"result":result}))
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc = crate::rpc::Rpc::new(
            format!("http://{}/", listener.local_addr().unwrap()),
            "test".into(),
            "test".into(),
            std::time::Duration::from_secs(2),
        )
        .unwrap();
        let server = tokio::spawn(async {
            axum::serve(listener, Router::new().fallback(reply))
                .await
                .unwrap()
        });
        let a = rpc.wallet("a").unwrap();
        let b = rpc.wallet("b").unwrap();
        require_retired_funding_wallet_control(&rpc, &a, "funding", 0)
            .await
            .unwrap();
        let error = require_retired_funding_wallet_control(&rpc, &b, "funding", 0)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not controlled by this wallet"));
        require_retired_funding_wallet_control(&rpc, &b, "funding", 1)
            .await
            .unwrap();
        assert!(
            require_retired_funding_wallet_control(&rpc, &a, "funding", 2)
                .await
                .is_err()
        );
        server.abort();
    }

    #[test]
    fn an_outage_needs_three_failed_checks_in_a_row_over_a_minute() {
        use std::time::{Duration, Instant};
        // Offsets back from a `now` five hours ahead never underflow.
        let now = Instant::now() + Duration::from_secs(5 * 3600);
        let ago = |seconds| now - Duration::from_secs(seconds);
        let first = continue_outage(None, None, now);
        assert_eq!(
            first,
            Outage {
                since: now,
                failed_checks: 1
            }
        );
        assert!(!first.long_enough(now));
        // Failed checks a minute apart continue it; two blips are not enough.
        let earlier = Outage {
            since: ago(60),
            failed_checks: 1,
        };
        let second = continue_outage(Some(earlier), Some(ago(60)), now);
        assert_eq!(second.failed_checks, 2);
        assert!(!second.long_enough(now));
        // A third, over a minute after the first: the peer's node is silent.
        let third = continue_outage(Some(second), Some(ago(30)), now + Duration::from_secs(30));
        assert!(third.failed_checks == 3 && third.long_enough(now + Duration::from_secs(30)));
        // Three quick failures within a minute are not enough either.
        let quick = Outage {
            since: ago(20),
            failed_checks: 3,
        };
        assert!(!quick.long_enough(now));
        // A failure long after the last check begins a new outage: the old
        // one may have ended unobserved.
        assert_eq!(
            continue_outage(
                Some(Outage {
                    since: ago(4 * 3600),
                    failed_checks: 9
                }),
                Some(ago(3 * 3600)),
                now
            ),
            Outage {
                since: now,
                failed_checks: 1
            }
        );
    }

    #[test]
    fn amount_is_exact() {
        assert_eq!(amount_bits(&json!("0.00000001")).unwrap(), 1);
        assert_eq!(amount_bits(&json!("1e-8")).unwrap(), 1);
        assert_eq!(
            amount_bits(&json!("21000000.00000001")).unwrap(),
            2_100_000_000_000_001
        );
        assert!(amount_bits(&json!("0.000000001")).is_err());
        assert!(amount_bits(&json!("-1")).is_err());
    }
}

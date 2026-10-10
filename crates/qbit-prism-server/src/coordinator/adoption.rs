//! Dual writer (S8): adopt a pool block whose finder died before its landing
//! rows reached this node (CONTRACT.md §5, S8; D-10, D-11).
//!
//! A node offers a found block only after the peer has ingested the block's
//! prepared record and its window's own shares, within a bound (D-19). If the
//! finder then dies with its disk, the block is on the active chain with no
//! landing rows anywhere, and its accruals would be lost. The survivor holds
//! the finder's prepared work, copied by peer sync, so it can rebuild the very
//! audit the block's coinbase commits to:
//!
//! 1. Each pass reads the coinbases of the recent active chain and keeps the
//!    pool blocks (`PoolRecognizer`) at least [`ADOPT_AFTER_CONFIRMATIONS`]
//!    deep that have no `qbit_pool_blocks` row and no unfinished candidate
//!    row here. That depth, about an hour, gives a living finder, and the sync
//!    of its rows, time to land the block the ordinary way first, so a short
//!    link cut adopts nothing.
//! 2. The prepared records on the block's parent whose template holds exactly
//!    the block's transactions are the work it may have been found on. Each
//!    is turned into a candidate with the block's own coinbase suffix, its
//!    audit is rebuilt from the as-issued window and balances, and the first
//!    whose audit commitment root is the block's coinbase witness reserved
//!    value lands, through [`Ledger::land_adopted_block`], which also requires
//!    the rebuilt coinbase to be the block's byte for byte.
//! 3. The rows land `prepared` with this node as their origin (D-10), and the
//!    reconciler confirms them from the chain like any other block. If the
//!    finder comes back and lands the block too, whole-block sync keeps one
//!    copy on each node (D-10). Which node found the block is still read from
//!    its coinbase ([`Ledger::found_here`]): only that node sponsors its
//!    fanouts and counts it as found, and no divergence is recorded here.
//!
//! Those rows are this node's own, so the loop starts only once the own-log
//! latch is set (D-8), as the submit loop does: a node restored from a backup
//! lands nothing before its own-log recovery has pulled back the landings it
//! lost and raised its sequences.
//!
//! While the peer's database is reachable and the peer sync is still pulling
//! landed blocks or prepared records it lags behind on (a heal or a
//! catch-up), a pass adopts and reports nothing: the rows it would miss are
//! in flight. A lag holds passes back only while its stream's pulls succeed,
//! and lets one pass through every [`ADOPTION_RETRY_INTERVAL`], so a sync that
//! is refused or never catches up cannot hide a block.
//!
//! A block with no adoptable record (an empty-window bootstrap block, a record
//! past retention, one built by another builder or keys, or a window or record
//! holding a peer row this node's sync refused as a conflict) is reported with
//! an ALERT and tried again every [`ADOPTION_RETRY_INTERVAL`]; nothing is
//! guessed. When it leaves the lookback unlanded, a last ALERT says so.
use super::*;
use crate::carry_owner::transfer::{
    first_link_break, read_blocks, ChainSource, PoolRecognizer, RpcChain, WalkedBlock,
};
use crate::ledger::{build_claim_parts, CandidateClaim, ClaimLifecycle, StoredCompactPrepared};
use std::collections::BTreeMap;

/// How deep a pool block must be before this node adopts it: about an hour
/// at the 60 s target spacing.
pub const ADOPT_AFTER_CONFIRMATIONS: u64 = 60;
/// The copied tables whose rows a block's adoption would replace or read: a
/// pass waits while the peer sync lags behind the peer on any of them.
const ADOPTION_INPUT_TABLES: [&str; 2] = ["qbit_pool_blocks", "qbit_prism_jobs"];

/// How recent a stream's last successful pull must be for its lag to hold a
/// pass back. A refused or failing sync keeps publishing its last lag, which
/// then says nothing about rows in flight. A slow but healthy pass, whose
/// streams run one after another with each read bounded by the peer sync's
/// statement timeout, stamps every stream well within it.
const SYNC_LAG_FRESHNESS: std::time::Duration = std::time::Duration::from_secs(300);

/// How far back a pass looks: about the 24 h a peer's prepared records are
/// retained (D-3), at the 60 s target spacing.
pub const ADOPT_LOOKBACK_BLOCKS: u64 = 1_440;
/// How long a block with no adoptable record waits before it is tried, and
/// reported, again.
pub const ADOPTION_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(600);

/// What the passes learned, kept between them so each block's coinbase is
/// read once.
#[derive(Debug)]
pub struct AdoptionState {
    /// The active chain as the passes read it, by height, within the
    /// lookback: always a linked chain, each block naming the one below as
    /// its parent. A block hash commits to every ancestor, so a pass stops
    /// walking down at the first height whose block is still active, and the
    /// links make every block below it that block's ancestry.
    chain: BTreeMap<u64, WalkedBlock>,
    /// Pool blocks with no landing rows here yet, by height: each pass looks
    /// at them again until they land, through their finder's rows or here.
    pending: BTreeMap<u64, String>,
    /// When adopting each pending block last failed.
    failed: BTreeMap<String, std::time::Instant>,
    retry_interval: std::time::Duration,
    /// Since when passes have been held back by a lagging peer sync, without
    /// a pass let through.
    held_back_since: Option<std::time::Instant>,
}

impl Default for AdoptionState {
    fn default() -> Self {
        Self::retrying_every(ADOPTION_RETRY_INTERVAL)
    }
}

impl AdoptionState {
    /// A state that tries a block with no adoptable record again after
    /// `retry_interval` ([`ADOPTION_RETRY_INTERVAL`] by default).
    pub fn retrying_every(retry_interval: std::time::Duration) -> Self {
        Self {
            chain: BTreeMap::new(),
            pending: BTreeMap::new(),
            failed: BTreeMap::new(),
            retry_interval,
            held_back_since: None,
        }
    }
}

/// Whether a block's coinbase scriptSig ends with a prepared record's
/// coinbase suffix: the issuing node's coinbase tag, then extranonce1 and
/// extranonce2 zeroed, as wide as that node's `PRISM_STRATUM_EXTRANONCE2_SIZE`,
/// which need not be this node's (the pair's fingerprint does not bind it).
/// The tag is printable ASCII, so the suffix's trailing zeros are exactly the
/// placeholder; the block holds the same tag right before its extranonces.
fn ends_with_suffix(script_sig: &[u8], suffix: &[u8]) -> std::result::Result<(), String> {
    let placeholder = suffix.iter().rev().take_while(|byte| **byte == 0).count();
    if !(5..=36).contains(&placeholder) {
        return Err(format!(
            "its coinbase suffix ends with {placeholder} zero bytes, not an extranonce placeholder of 5 to 36"
        ));
    }
    let tag_len = suffix.len() - placeholder;
    if script_sig.len() < suffix.len()
        || script_sig[script_sig.len() - suffix.len()..][..tag_len] != suffix[..tag_len]
    {
        return Err("the block's coinbase does not end with its coinbase suffix".into());
    }
    Ok(())
}

/// What a pass did with one pool block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Adoption {
    /// Landed from this prepared record.
    Landed {
        block_hash: String,
        prepared: String,
    },
    /// No prepared record here rebuilds the block's audit.
    NoRecord { block_hash: String, reason: String },
}

impl Coordinator {
    /// The pool's blocks as the chain shows them: the coinbase tag, or the
    /// pool-fee program the frontend resolved at start.
    fn pool_recognizer(&self) -> Result<PoolRecognizer> {
        PoolRecognizer::new(
            &self.config.coinbase_tag,
            self.config
                .payout_policy
                .pool_fee_policy
                .as_ref()
                .map(|fee| fee.p2mr_program_hex.as_str()),
        )
    }

    /// Whether the peer's database is reachable while the peer sync is still
    /// pulling landed blocks or prepared records it lags behind on: a lag
    /// counts only while its stream's pulls succeed ([`SYNC_LAG_FRESHNESS`]).
    fn peer_sync_lagging(&self) -> bool {
        let now = chrono::Utc::now();
        self.peer_sync.get().is_some_and(|status| {
            let status = status.borrow();
            status.peer_reachable
                && ADOPTION_INPUT_TABLES.iter().any(|table| {
                    status.per_table.get(*table).is_some_and(|table| {
                        table.lag_rows > 0
                            && table.last_success.is_some_and(|at| {
                                (now - at).to_std().unwrap_or_default() <= SYNC_LAG_FRESHNESS
                            })
                    })
                })
        })
    }

    /// One adoption pass over the recent active chain (module comment).
    pub async fn adoption_pass(&self, state: &mut AdoptionState) -> Result<Vec<Adoption>> {
        let chain = RpcChain(&self.rpc);
        let recognizer = self.pool_recognizer()?;
        let (tip_height, tip_hash) = chain.tip().await?;
        let first = tip_height.saturating_sub(ADOPT_LOOKBACK_BLOCKS);
        // A block that leaves the lookback unlanded is given up on, loudly.
        for (height, block_hash) in state.pending.range(..first) {
            if !self.ledger.block_rows_present(block_hash).await?.0 {
                tracing::error!(
                    block = %block_hash,
                    height,
                    "ALERT: giving up adopting a pool block that left the adoption lookback with no landing rows here; its payout data exists only where it was found"
                );
            }
        }
        state.chain.retain(|height, _| *height >= first);
        state.pending.retain(|height, _| *height >= first);
        let pending = &state.pending;
        state
            .failed
            .retain(|hash, _| pending.values().any(|pending| pending == hash));
        // The highest block read before that is still active; the links make
        // every block below it active too.
        let mut unchanged = None;
        for height in (first..=tip_height).rev() {
            if let Some(block) = state.chain.get(&height) {
                if block.hash == chain.block_hash(height).await? {
                    unchanged = Some(height);
                    break;
                }
            }
        }
        let from = unchanged.map_or(first, |height| height + 1);
        state.chain.retain(|height, _| *height < from);
        state.pending.retain(|height, _| *height < from);
        let read: BTreeMap<u64, WalkedBlock> = if from <= tip_height {
            read_blocks(&chain, from..=tip_height, &recognizer)
                .await?
                .into_iter()
                .collect()
        } else {
            BTreeMap::new()
        };
        // Keep the reads only if they link to the chain below them and end
        // at the tip read first: a read that spanned a reorganisation is
        // dropped whole, and the next pass reads that range again.
        let below = from.checked_sub(1).and_then(|height| {
            state
                .chain
                .get(&height)
                .map(|block| (height, block.clone()))
        });
        let mut linked: BTreeMap<u64, WalkedBlock> = below.into_iter().collect();
        linked.extend(read.clone());
        let top_is_tip = read.is_empty()
            || read
                .get(&tip_height)
                .is_some_and(|block| block.hash == tip_hash);
        if first_link_break(&linked, first).is_some() || !top_is_tip {
            tracing::debug!(
                from,
                tip_height,
                "adoption pass: the chain moved during the read; reading it again next pass"
            );
            return Ok(Vec::new());
        }
        for (height, block) in read {
            if block.pool {
                state.pending.insert(height, block.hash.clone());
            }
            state.chain.insert(height, block);
        }
        let mut adoptions = Vec::new();
        if self.peer_sync_lagging() {
            let since = *state
                .held_back_since
                .get_or_insert_with(std::time::Instant::now);
            if since.elapsed() < state.retry_interval {
                tracing::debug!(
                    "adoption pass: the peer is reachable and its blocks or prepared records are still being pulled; adopting nothing this pass"
                );
                return Ok(adoptions);
            }
        }
        // This pass goes on; a lag seen at the next one holds passes back
        // for another interval.
        state.held_back_since = None;
        let due: Vec<(u64, String)> = state
            .pending
            .range(..=tip_height.saturating_sub(ADOPT_AFTER_CONFIRMATIONS))
            .map(|(height, hash)| (*height, hash.clone()))
            .collect();
        for (height, block_hash) in due {
            if state
                .failed
                .get(&block_hash)
                .is_some_and(|at| at.elapsed() < state.retry_interval)
            {
                continue;
            }
            let (landed, enqueued) = self.ledger.block_rows_present(&block_hash).await?;
            if landed {
                state.pending.remove(&height);
                state.failed.remove(&block_hash);
                continue;
            }
            if enqueued {
                // This node's own unfinished candidate: its claim lands it.
                continue;
            }
            // One block that cannot be adopted (or not now) never stops the
            // pass: it is reported like a block with no record, and retried.
            let adoption = match self.adopt_block(&block_hash).await {
                Ok(adoption) => adoption,
                Err(error) => Adoption::NoRecord {
                    block_hash: block_hash.clone(),
                    reason: format!("adoption failed: {error:#}"),
                },
            };
            match &adoption {
                Adoption::Landed { prepared, .. } => {
                    tracing::warn!(
                        block = %block_hash,
                        height,
                        prepared = %prepared,
                        "adopted a pool block with no landing rows from its prepared record (S8); the reconciler confirms it from the chain"
                    );
                    state.pending.remove(&height);
                    state.failed.remove(&block_hash);
                }
                Adoption::NoRecord { reason, .. } => {
                    tracing::error!(
                        block = %block_hash,
                        height,
                        reason = %reason,
                        retry_seconds = state.retry_interval.as_secs(),
                        "ALERT: a pool block on the active chain has no landing rows here and no adoptable prepared record; its payout data exists only on the node that found it"
                    );
                    state
                        .failed
                        .insert(block_hash.clone(), std::time::Instant::now());
                }
            }
            adoptions.push(adoption);
        }
        Ok(adoptions)
    }

    /// Adopt one block from the first prepared record whose rebuilt audit
    /// is the one its coinbase commits to.
    pub async fn adopt_block(&self, block_hash: &str) -> Result<Adoption> {
        let no_record = |reason: String| Adoption::NoRecord {
            block_hash: block_hash.to_owned(),
            reason,
        };
        let raw = self.rpc.call("getblock", json!([block_hash, 0])).await?;
        let block_bytes = hex::decode(raw.as_str().context("getblock returned no block")?)?;
        let mut header_hash =
            codec::double_sha256(block_bytes.get(..80).context("truncated block")?);
        header_hash.reverse();
        ensure!(
            hex::encode(header_hash) == block_hash,
            "the node returned a block whose header is not {block_hash}"
        );
        let parent = header_parent(&block_bytes)?;
        let coinbase = codec::coinbase_from_block(&block_bytes)?;
        let root = hex::encode(crate::ledger::coinbase_witness_reserved_value(coinbase)?);
        let script_sig = codec::coinbase_script_sig(coinbase)?.to_vec();
        let leaves = codec::witness_merkle_leaves_from_block(&block_bytes)?;
        let records = self.ledger.compact_prepared_on_parent(&parent).await?;
        if records.is_empty() {
            return Ok(no_record(format!(
                "no prepared record on parent {parent} is held here"
            )));
        }
        let mut reasons = Vec::new();
        for (key, stored) in records {
            match self
                .adoption_candidate(block_hash, &block_bytes, &script_sig, &leaves, &stored)
                .await
            {
                Err(reason) => reasons.push(format!("{key}: {reason}")),
                Ok(candidate) => {
                    let candidate = candidate.with_parts_from(self, &key).await;
                    match candidate {
                        Err(error) => reasons.push(format!("{key}: rebuild failed: {error:#}")),
                        Ok(claim) => {
                            let rebuilt_root = claim
                                .parts
                                .as_ref()
                                .and_then(|parts| parts.body.audit_commitment_root_hex.clone());
                            if rebuilt_root.as_deref() != Some(root.as_str()) {
                                reasons.push(format!(
                                    "{key}: rebuilt commitment root {} is not the block's {root}",
                                    rebuilt_root.as_deref().unwrap_or("none")
                                ));
                                continue;
                            }
                            self.ledger
                                .land_adopted_block(&claim, &self.config.ledger_public_key)
                                .await?;
                            return Ok(Adoption::Landed {
                                block_hash: block_hash.to_owned(),
                                prepared: key,
                            });
                        }
                    }
                }
            }
        }
        Ok(no_record(reasons.join("; ")))
    }

    /// The candidate a block found on `stored`'s work would have been, or
    /// why it cannot be.
    async fn adoption_candidate(
        &self,
        block_hash: &str,
        block_bytes: &[u8],
        script_sig: &[u8],
        leaves: &[String],
        stored: &StoredCompactPrepared,
    ) -> std::result::Result<AdoptionCandidate, String> {
        let record = &stored.record;
        if record.window.shares.is_none() {
            return Err("an empty-window (bootstrap) record cannot be adopted from".into());
        }
        let template = &stored.template;
        let transactions = codec::transactions_from_template(template)
            .map_err(|error| format!("its template's transactions do not decode: {error:#}"))?;
        if codec::witness_merkle_leaves_hex(&transactions) != leaves {
            return Err("its template does not hold the block's transactions".into());
        }
        let suffix = hex::decode(&record.coinbase_suffix_hex)
            .map_err(|error| format!("its coinbase suffix is not hex: {error}"))?;
        ends_with_suffix(script_sig, &suffix)?;
        let network = codec::target_from_compact(
            codec::parse_u32_hex(
                template["bits"]
                    .as_str()
                    .ok_or("its template has no bits")?,
            )
            .map_err(|error| format!("its template bits are invalid: {error:#}"))?,
        )
        .and_then(|target| codec::scaled_target_difficulty(&target))
        .map_err(|error| format!("its template bits are invalid: {error:#}"))?;
        let candidate = Candidate {
            block_hash: block_hash.to_owned(),
            block_sha256: Candidate::block_digest_hex(block_bytes),
            job_id: String::new(),
            payout_revision: record.payout_revision,
            window: record.window,
            bootstrap_share: None,
            found_block: qbit_prism::FoundBlock {
                block_height: template["height"]
                    .as_u64()
                    .ok_or("its template has no height")?,
                coinbase_value_sats: template["coinbasevalue"]
                    .as_u64()
                    .ok_or("its template has no coinbase value")?,
                network_difficulty: network,
                anchor_job_issued_at_ms: record.window.anchor_ms,
            },
            payout_policy: record.payout_policy.clone(),
            ctv: record.ctv.clone(),
            audit_builder_version: record.audit_builder_version,
            signer_keys: record.signer_keys.clone(),
            leased: false,
            coinbase_suffix_hex: hex::encode(&script_sig[script_sig.len() - suffix.len()..]),
            deferred_share: None,
            block_bytes: block_bytes.to_vec(),
            as_issued_balances: Vec::new(),
        };
        if let Some(mismatch) = self
            .stored_inputs_mismatch(&candidate)
            .map_err(|error| format!("{error:#}"))?
        {
            return Err(mismatch);
        }
        Ok(AdoptionCandidate(candidate))
    }
}

/// A candidate rebuilt from a prepared record, before its audit parts.
struct AdoptionCandidate(Candidate);

impl AdoptionCandidate {
    /// Read the as-issued window and rebuild the audit parts.
    async fn with_parts_from(self, coordinator: &Coordinator, key: &str) -> Result<CandidateClaim> {
        let candidate = self.0;
        let window = coordinator
            .read_window(&candidate.window, BalanceSource::AsIssued)
            .await
            .with_context(|| format!("reading the window of prepared record {key}"))?;
        let permit = coordinator.build_slots.clone().acquire_owned().await?;
        let config = coordinator.config.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let Window {
                shares,
                prior_balances,
                ..
            } = window;
            let manifest_key = ManifestSigningKey::from_seed_hex(&config.manifest_seed)?;
            let ledger_key = ManifestSigningKey::from_seed_hex(&config.ledger_seed)?;
            let parts = build_claim_parts(
                &candidate,
                shares,
                prior_balances,
                &manifest_key,
                &ledger_key,
            )?;
            Ok(CandidateClaim {
                candidate,
                claim_token: String::new(),
                parts: Some(parts),
                lifecycle: ClaimLifecycle::default(),
            })
        })
        .await?
    }
}

/// How often the adoption pass runs.
pub const ADOPTION_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

impl Coordinator {
    /// Dual writer: run [`Coordinator::adoption_pass`] every
    /// [`ADOPTION_INTERVAL`] until shutdown, from the moment the own-log
    /// latch is set (module comment). A failed pass is logged and retried;
    /// nothing it read is trusted across a failure.
    pub async fn adoption_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        // The wait the server puts before its other own-row writers.
        if let Some(mut status) = self.peer_sync.get().cloned() {
            let caught_up = tokio::select! {
                biased;
                _ = shutdown.wait_for(|stop| *stop) => false,
                caught_up = status.wait_for(|status| status.own_log_caught_up) => caught_up.is_ok(),
            };
            if !caught_up {
                return;
            }
        }
        let mut state = AdoptionState::default();
        loop {
            if let Err(error) = self.adoption_pass(&mut state).await {
                tracing::warn!(error = %format!("{error:#}"), "adoption pass failed; retrying");
            }
            tokio::select! {
                _ = tokio::time::sleep(ADOPTION_INTERVAL) => {}
                _ = shutdown.changed() => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ends_with_suffix;

    /// A scriptSig: a height push, the tag, extranonce1 and `width` bytes of
    /// extranonce2.
    fn script_sig(tag: &[u8], width: usize) -> Vec<u8> {
        [
            &[0x03, 0x01, 0x02, 0x03][..],
            tag,
            &[0x80, 0, 0, 7],
            &vec![0xee; width],
        ]
        .concat()
    }

    /// A prepared record's suffix: the tag and a zeroed placeholder.
    fn suffix(tag: &[u8], width: usize) -> Vec<u8> {
        [tag, &vec![0; 4 + width][..]].concat()
    }

    #[test]
    fn a_block_ends_with_the_suffix_of_the_work_it_was_found_on_whatever_its_extranonce2_width() {
        for width in [1, 4, 8, 32] {
            assert_eq!(
                ends_with_suffix(&script_sig(b"/PRISM/", width), &suffix(b"/PRISM/", width)),
                Ok(()),
                "width {width}"
            );
        }
        // Another width or another tag is another layout.
        assert!(ends_with_suffix(&script_sig(b"/PRISM/", 4), &suffix(b"/PRISM/", 8)).is_err());
        assert!(ends_with_suffix(&script_sig(b"/PRISM/", 8), &suffix(b"/OTHER/", 8)).is_err());
        // No placeholder, or a scriptSig shorter than the suffix.
        assert!(ends_with_suffix(&script_sig(b"/PRISM/", 8), b"/PRISM/\0\0\0\0").is_err());
        assert!(ends_with_suffix(&[0, 0], &suffix(b"/PRISM/", 8)).is_err());
    }
}

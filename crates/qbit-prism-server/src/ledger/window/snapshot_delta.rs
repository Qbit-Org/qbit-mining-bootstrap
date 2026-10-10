//! Conservative delta acquisition; no retained input is publication authority.
use super::*;
pub use crate::metrics::WindowAcquisition;
use std::time::Duration;

/// Rows per delta or margin page: the full reader's page.
const DELTA_PAGE_ROWS: i64 = 4096;
/// Newly appended sequence slots one delta may span. Beyond this the retained
/// window is mostly obsolete and the bounded newest-first full scan reads no
/// more payload than the delta would, so it takes over. Sixty-four pages,
/// about half an hour of production shares at 133 a second.
///
/// **Memory at the bounds.** The retained window, the whole delta and the
/// whole margin are decoded and alive together until the merge, and with a
/// margin the exactly sized merged vector is allocated while all three still
/// exist (element storage is duplicated during the move; the string heap is
/// not). At about 600 B per decoded production share and 216 B of inline
/// element per row, a 400k window (240 MB steady) peaks at about 240 + 157
/// (delta at its bound) + 39 (margin at its bound) + 200 (assembly) ≈ 640 MB,
/// and a 500k window at about 720 MB; the full scan's own transient is its
/// vector doubling slack (≤ 216 B per row, 108 MB at 500k) plus one page.
/// Admission is a permit count, not bytes, so nothing but these two constants
/// bounds the transient.
pub(crate) const MAX_DELTA_SLOTS: u64 = 64 * 4096;
/// Older pages a retarget upward may pull in below the retained first row
/// before the full scan takes over. Sixteen pages, 65,536 rows: a per-block
/// retarget moves the crossing row by a fraction of a percent of the window
/// (about 3,200 rows at 400k for a 0.8% step; the measured margins were a
/// few thousand rows), so sixteen pages cover a long run of consecutive
/// upward steps while bounding the margin's share of the transient above to
/// 39 MB. A larger jump, as after a frontend was away for many blocks, takes
/// the full scan, which is what the base did for every retarget.
pub(crate) const MAX_MARGIN_PAGES: usize = 16;

/// Only moved out of the refresh loop's exclusively owned snapshot. Keep the
/// original cutoff and anchor: the last eligible row alone is not a cutoff.
pub(crate) struct RetainedShares {
    pub network: u128,
    pub anchor_ms: i64,
    pub cutoff: u64,
    /// The retained window's dual-writer cut (`window/cut.rs`); `None` in
    /// single-writer mode. A delta advances a window only within its mode.
    pub cut: Option<WindowCut>,
    pub shares: Vec<AcceptedShare>,
    pub leaf: Option<LeafWitness>,
}

/// Runtime-only acquisition evidence, never serialized or persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LeafWitness {
    tableoid: i64,
    inherits_xmin: String,
    timeline: String,
}

/// What one refresh snapshot cost and which path acquired it. Runtime-only:
/// logged and counted, never serialized, persisted or compared for reuse.
#[derive(Clone, Debug)]
pub struct AcquisitionReport {
    /// `Advanced` for the delta path; every other value names why the full
    /// newest-first scan ran instead.
    pub outcome: WindowAcquisition,
    /// Rows the retired window carried in, or zero without one.
    pub prior_rows: usize,
    /// Rows the delta path read above the retained cutoff.
    pub delta_rows: usize,
    /// Rows the delta path read below the retained first row for a heavier
    /// target.
    pub margin_rows: usize,
    /// Retained rows the delta path dropped from the old end.
    pub retired_rows: usize,
    /// Rows in the window that was captured, by either path.
    pub window_rows: usize,
    /// Pages of share payload the snapshot fetched: the delta and margin
    /// pages, plus, after a refusal, the full scan's own pages.
    pub pages: usize,
    /// Scaled network difficulty of the retired window, when one was offered.
    pub prior_network: Option<u128>,
    /// Anchor to captured snapshot, both transactions included.
    pub elapsed: Duration,
}

impl AcquisitionReport {
    pub(crate) fn full(outcome: WindowAcquisition) -> Self {
        Self {
            outcome,
            prior_rows: 0,
            delta_rows: 0,
            margin_rows: 0,
            retired_rows: 0,
            window_rows: 0,
            pages: 0,
            prior_network: None,
            elapsed: Duration::ZERO,
        }
    }

    pub fn advanced(&self) -> bool {
        self.outcome == WindowAcquisition::Advanced
    }
}

#[derive(Clone)]
pub(crate) struct SnapshotCapture {
    pub snapshot: Snapshot,
    pub leaf: Option<LeafWitness>,
    pub acquisition: AcquisitionReport,
    /// The writer timeline the window's rows were read on (#619).
    pub timeline: super::WriterTimeline,
    /// In dual-writer mode, the peer's share-stream high-water mark the
    /// snapshot read (`window/cut.rs`), which the refresh probe compares; `None`
    /// for a single writer. Runtime-only.
    pub peer_mark: Option<i64>,
}

impl std::ops::Deref for SnapshotCapture {
    type Target = Snapshot;
    fn deref(&self) -> &Snapshot {
        &self.snapshot
    }
}

impl RetainedShares {
    /// The cheap scalar refusals, decided before any database work.
    ///
    /// A changed network difficulty is not one of them: the retained rows are
    /// immutable members of the anchored ledger whatever the target, so a
    /// retarget only moves the crossing row, which `advance` re-derives.
    ///
    /// With a cut, the cutoff checks are made per node instead: the fresh cut
    /// must cover the retained one (a window whose mode changed, cut to none
    /// or none to cut, counts as regressed; a frontend's mode is fixed for
    /// its life, so that is a defect, not a path), and the slots the delta
    /// would read ([`cut_delta_slots`]) bound it as `MAX_DELTA_SLOTS` does.
    fn refusal(
        &self,
        anchor: i64,
        cutoff: u64,
        cut: Option<&WindowCut>,
    ) -> Option<WindowAcquisition> {
        if self.anchor_ms > anchor {
            return Some(WindowAcquisition::AnchorRegressed);
        }
        match (self.cut.as_ref(), cut) {
            (None, None) => {
                if self.cutoff > cutoff {
                    return Some(WindowAcquisition::CutoffRegressed);
                }
                if cutoff - self.cutoff > MAX_DELTA_SLOTS {
                    return Some(WindowAcquisition::DeltaTooLarge);
                }
            }
            (Some(retained), Some(cut)) => {
                if !cut.covers(retained) {
                    return Some(WindowAcquisition::CutoffRegressed);
                }
                let first = self.shares.first().map(|share| share.share_seq);
                if cut_delta_slots(retained, cut, first) > MAX_DELTA_SLOTS {
                    return Some(WindowAcquisition::DeltaTooLarge);
                }
            }
            _ => return Some(WindowAcquisition::CutoffRegressed),
        }
        let Some(first) = self.shares.first() else {
            return Some(WindowAcquisition::EmptyPrior);
        };
        if first.share_seq == 0 {
            return Some(WindowAcquisition::EmptyPrior);
        }
        let last = self.shares.last().unwrap().share_seq;
        match self.cut.as_ref() {
            // Native appends always make the accepted cutoff the last retained
            // row; only legacy or raw rows with a future timestamp at the top of
            // the ledger can leave the cutoff above the retained tail.
            None if last != self.cutoff => return Some(WindowAcquisition::TailMismatch),
            // Each entry is a row of its node the cut admits, so the higher
            // one is the retained window's newest row, as the cutoff is
            // without a cut.
            Some(retained) if retained.top() != Some(last) => {
                return Some(WindowAcquisition::TailMismatch)
            }
            _ => {}
        }
        if self.leaf.is_none() {
            return Some(WindowAcquisition::NoEvidence);
        }
        None
    }
}

/// The `share_seq` slots a dual-writer delta reads ([`advance_in_cut`]): for
/// each node, those above its retained entry, and at or above the retained
/// first row, up to its fresh entry. Each slot holds one row of either node,
/// so the union of the two ranges bounds the rows read, as the cutoff's
/// difference does without a cut; interleaved sequences count once.
fn cut_delta_slots(retained: &WindowCut, cut: &WindowCut, first: Option<u64>) -> u64 {
    let floor = first.map_or(0, |first| first.saturating_sub(1));
    let range = |node| {
        let high = cut.get(node).ok().flatten()?;
        let low = retained.get(node).ok().flatten().unwrap_or(0).max(floor);
        (high > low).then_some((low, high))
    };
    match (range(0), range(1)) {
        (None, None) => 0,
        (Some((low, high)), None) | (None, Some((low, high))) => high - low,
        (Some((low_0, high_0)), Some((low_1, high_1))) => {
            let overlap = high_0.min(high_1).saturating_sub(low_0.max(low_1));
            (high_0 - low_0) + (high_1 - low_1) - overlap
        }
    }
}

/// A live single leaf covers every integer between the endpoints. Checking
/// endpoints in different leaves cannot rule out an interior partition hole.
/// pg_inherits is authoritative, including the first phase of concurrent
/// detach; its tuple incarnation also detects detach/reattach during the read.
/// The insertion timeline distinguishes successive writers under the existing
/// single-standby D3 promotion/rejoin policy. It is shared across pooled SQL
/// sessions, but is not a globally unique identity for sibling physical copies.
/// D5 isolated recovery already stops frontends, discarding retained evidence.
///
/// A dual-writer window's `cut` joins the eligibility of both endpoints and
/// of every counted row (`window/cut.rs`); without one the statement is
/// 3.0's.
pub(super) async fn leaf_witness(
    tx: &mut Transaction<'_, Postgres>,
    first: i64,
    last: i64,
    anchor: i64,
    cut: Option<&WindowCut>,
    expected_count: Option<i64>,
) -> Result<Option<LeafWitness>> {
    // The cut's clause for each of the three rows the statement names.
    let within = |row: &str| super::cut::cut_clause(cut.map(|_| 5), Some(row));
    let row = sqlx::query(&format!(
        "SELECT a.tableoid::bigint AS tableoid,i.xmin::text AS inherits_xmin, \
         left(pg_walfile_name(pg_current_wal_lsn()),8) AS timeline \
         FROM qbit_share_ledger a \
         JOIN qbit_share_ledger b ON b.share_seq=$2 AND b.tableoid=a.tableoid \
         JOIN pg_inherits i ON i.inhrelid=a.tableoid \
           AND i.inhparent='qbit_share_ledger'::regclass AND NOT i.inhdetachpending \
         WHERE a.share_seq=$1 AND a.accepted AND b.accepted \
           AND a.accepted_at<=to_timestamp($3::double precision/1000) \
           AND a.job_issued_at<=to_timestamp($3::double precision/1000) \
           AND b.accepted_at<=to_timestamp($3::double precision/1000) \
           AND b.job_issued_at<=to_timestamp($3::double precision/1000){}{} \
           AND ($4::bigint IS NULL OR $4=(SELECT count(*) FROM qbit_share_ledger s \
               WHERE s.share_seq BETWEEN $1 AND $2 AND s.accepted \
                 AND s.accepted_at<=to_timestamp($3::double precision/1000) \
                 AND s.job_issued_at<=to_timestamp($3::double precision/1000){}))",
        within("a"),
        within("b"),
        within("s"),
    ))
    .bind(first)
    .bind(last)
    .bind(anchor)
    .bind(expected_count)
    .bind_cut(cut)?
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|row| {
        Ok(LeafWitness {
            tableoid: row.try_get("tableoid")?,
            inherits_xmin: row.try_get("inherits_xmin")?,
            timeline: row.try_get("timeline")?,
        })
    })
    .transpose()
}

/// The delta path's answer: the exact window for the fresh target, or why the
/// full scan must read it.
pub(crate) enum Advance {
    Advanced {
        shares: BlockingDrop<Vec<AcceptedShare>>,
        leaf: LeafWitness,
        report: AcquisitionReport,
    },
    Rejected(AcquisitionReport),
}

/// The retained window, the delta above it and the margin below it, between
/// blocking hand-offs. `remaining` is the target weight still unmet after the
/// newest-first walk so far; `delta_start` and `start` are the kept suffixes.
struct Merge {
    prior: Vec<AcceptedShare>,
    delta: Vec<AcceptedShare>,
    /// Newest first, as the margin pages arrive.
    margin: Vec<AcceptedShare>,
    delta_start: usize,
    start: usize,
    remaining: u128,
}

/// Release a retained vector under admission on a blocking thread and wait
/// for that release, exactly as the cancelled-build cleanup does.
async fn retire<T: Send + 'static>(owned: BlockingDrop<T>) -> Result<()> {
    owned.map_anyhow(|_| Ok(())).await?;
    Ok(())
}

/// Reuse the retired window's rows for the fresh anchor, cutoff and target
/// weight, or say why the full scan has to run.
///
/// The window for `weight` at `anchor` is the newest-first suffix of the
/// anchored eligible rows up to `cutoff` whose saturating weight fold first
/// reaches zero, the crossing row included: `Ledger::snapshot_with_admission`'s
/// full scan, restated. The retained rows are immutable members of that set,
/// so the fold is replayed over the delta read above the retained cutoff, then
/// the retained rows, then, for a heavier target, a bounded margin read below
/// the retained first row. The merged suffix is then proved to be exactly the
/// anchored set of its range by the count in the same leaf witness, which
/// cannot see rows older than the first row, which is why the fold itself
/// must have crossed: a window that ran out of history is refused and the
/// full scan decides whether it is partial.
///
/// **Isolation.** The transaction is READ COMMITTED, as the full scan's is:
/// each delta page, margin page and witness statement sees its own snapshot,
/// and nothing here relies on their being one snapshot. The anchored set is
/// frozen by construction instead: rows never change (the immutable-history
/// trigger), eligibility is `accepted_at <= anchor AND job_issued_at <=
/// anchor`, and the anchor was issued by an `UPDATE` of the cluster singleton
/// that every append also updates, so an append in flight when the anchor was
/// taken had already committed with an earlier clock and every later append
/// carries a clock strictly above the anchor. A row that appears between two
/// statements is therefore either above the cutoff and ineligible, or a
/// retroactive INSERT into an old slot or a row whose timestamps became
/// eligible, and the final count catches both. The partition and timeline
/// witness is re-read in that same final statement.
///
/// A dual-writer window (`cut` is `Some`) advances through
/// [`advance_in_cut`] instead, because a peer's rows are not appends.
// The parameter list is the single-writer one plus the cut.
#[allow(clippy::too_many_arguments)]
pub(super) async fn advance(
    tx: &mut Transaction<'_, Postgres>,
    prior: BlockingDrop<RetainedShares>,
    weight: u128,
    anchor: i64,
    cutoff: i64,
    cut: Option<WindowCut>,
    completion: &ReadAdmission,
) -> Result<Advance> {
    let cutoff_u64 = u64::try_from(cutoff)?;
    let mut report = AcquisitionReport::full(WindowAcquisition::Advanced);
    report.prior_rows = prior.shares.len();
    report.prior_network = Some(prior.network);
    if let Some(outcome) = prior.refusal(anchor, cutoff_u64, cut.as_ref()) {
        retire(prior).await?;
        return Ok(reject(report, outcome));
    }
    if let Some(cut) = cut {
        return advance_in_cut(tx, prior, weight, anchor, cut, completion, report).await;
    }
    // Keep ownership under admission across every SQL await below.
    let prior = completion.own(prior.into_inner());
    let first = i64::try_from(prior.shares[0].share_seq)?;
    let Some(witness) = leaf_witness(tx, first, cutoff, anchor, None, None).await? else {
        retire(prior).await?;
        return Ok(reject(report, WindowAcquisition::LeafChanged));
    };
    if prior.leaf.as_ref() != Some(&witness) {
        retire(prior).await?;
        return Ok(reject(report, WindowAcquisition::LeafChanged));
    }
    // The delta: every eligible row above the retained cutoff, in ascending
    // keyset pages. Planned per page with its bounds, as `read_range_owned`
    // is, so the partitioned ledger walks its key instead of sorting a leaf.
    let delta_page = format!(
        "{SELECT_SHARE} WHERE {} AND share_seq>$1 AND share_seq<=$2 \
         ORDER BY share_seq LIMIT {DELTA_PAGE_ROWS}",
        super::super::audit::anchored_eligibility_sql(3)
    );
    let mut delta = completion.own(Vec::<AcceptedShare>::new());
    let mut cursor = i64::try_from(prior.cutoff)?;
    while cursor < cutoff {
        let rows = sqlx::query(&delta_page)
            .persistent(false)
            .bind(cursor)
            .bind(cutoff)
            .bind(anchor)
            .fetch_all(&mut **tx)
            .await?;
        if rows.is_empty() {
            break;
        }
        report.pages += 1;
        delta = delta
            .map_anyhow(move |mut delta| {
                delta.reserve(rows.len());
                for row in &rows {
                    delta.push(share_from_row(row)?);
                }
                Ok(delta)
            })
            .await?;
        cursor = i64::try_from(
            delta
                .last()
                .context("decoded delta page is empty")?
                .share_seq,
        )?;
    }
    report.delta_rows = delta.len();
    // Replay the full reader's newest-first saturating fold over the delta,
    // then the retained rows. Saturating subtraction matches it, including
    // zero weights, a partial crossing row, and sums that overflow.
    let merge = completion
        .own((prior, delta))
        .map_anyhow(move |(prior, delta)| {
            let prior = prior.into_inner();
            let delta = delta.into_inner();
            let mut remaining = weight;
            let mut delta_start = 0;
            for (index, share) in delta.iter().enumerate().rev() {
                remaining = remaining.saturating_sub(share.share_difficulty);
                if remaining == 0 {
                    delta_start = index;
                    break;
                }
            }
            let mut start = prior.shares.len();
            if remaining > 0 {
                for (index, share) in prior.shares.iter().enumerate().rev() {
                    remaining = remaining.saturating_sub(share.share_difficulty);
                    start = index;
                    if remaining == 0 {
                        break;
                    }
                }
            }
            Ok(Merge {
                prior: prior.shares,
                delta,
                margin: Vec::new(),
                delta_start,
                start,
                remaining,
            })
        })
        .await?;
    // A heavier target than the retained rows reach continues the same walk
    // below the retained first row, newest first, bounded in pages.
    let retired = merge.start;
    let merge = match walk_margin(tx, merge, first, anchor, None, retired, &mut report).await? {
        Ok(merge) => merge,
        Err(outcome) => return Ok(reject(report, outcome)),
    };
    // Assemble oldest first: margin (reversed), the kept retained suffix, the
    // kept delta suffix. Without a margin the retained vector is trimmed in
    // place and appended to, so obsolete rows never inflate its capacity and
    // only the retained window and the delta coexist; a large retirement
    // also releases the old capacity. With a margin one exactly sized vector
    // is built instead.
    let merged = merge
        .map_anyhow(move |merge| {
            let Merge {
                mut prior,
                mut delta,
                margin,
                delta_start,
                start,
                ..
            } = merge;
            let shares = if margin.is_empty() {
                prior.drain(..start);
                prior.extend(delta.drain(delta_start..));
                if prior.capacity() > prior.len().saturating_mul(2).max(4) {
                    prior.shrink_to_fit();
                }
                prior
            } else {
                let mut shares = Vec::with_capacity(
                    margin.len() + (prior.len() - start) + (delta.len() - delta_start),
                );
                shares.extend(margin.into_iter().rev());
                shares.extend(prior.drain(start..));
                shares.extend(delta.drain(delta_start..));
                shares
            };
            // The merged window must be one strictly ascending sequence and
            // must cross exactly at its first row: the full fold reaches
            // zero and the fold without the first row does not. Landing
            // re-derives the same boundary (`ledger/audit.rs`,
            // `oldest_boundary`), so a window failing either check here
            // would be refused there; it is refused here first.
            let holds = window_invariant(&shares, weight);
            Ok((shares, holds))
        })
        .await?;
    if !merged.1 {
        tracing::error!(
            first = merged.0.first().map(|share| share.share_seq),
            last = merged.0.last().map(|share| share.share_seq),
            rows = merged.0.len(),
            "delta window failed its ordering or crossing invariant; taking the full scan"
        );
        retire(merged).await?;
        return Ok(reject(report, WindowAcquisition::Invariant));
    }
    let merged = merged.map_anyhow(|(shares, _)| Ok(shares)).await?;
    report.window_rows = merged.len();
    prove_merged(tx, merged, cutoff, anchor, None, witness, report).await
}

fn reject(mut report: AcquisitionReport, outcome: WindowAcquisition) -> Advance {
    report.outcome = outcome;
    Advance::Rejected(report)
}

/// The newest-first walk's state while the margin below the retained first
/// row is read: what [`walk_margin`] extends, for [`advance`]'s merge and
/// [`advance_in_cut`]'s alike.
trait MarginWalk: Send + 'static {
    /// The target weight the walk has still to meet.
    fn remaining(&self) -> u128;
    /// The rows read below the retained first row, newest first.
    fn margin(&self) -> &[AcceptedShare];
    /// Take the next older row: its weight comes off the target, saturating
    /// as the full reader's fold does.
    fn take(&mut self, share: AcceptedShare);
}

impl MarginWalk for Merge {
    fn remaining(&self) -> u128 {
        self.remaining
    }
    fn margin(&self) -> &[AcceptedShare] {
        &self.margin
    }
    fn take(&mut self, share: AcceptedShare) {
        self.remaining = self.remaining.saturating_sub(share.share_difficulty);
        self.margin.push(share);
    }
}

impl MarginWalk for CutMerge {
    fn remaining(&self) -> u128 {
        self.remaining
    }
    fn margin(&self) -> &[AcceptedShare] {
        &self.margin
    }
    fn take(&mut self, share: AcceptedShare) {
        self.remaining = self.remaining.saturating_sub(share.share_difficulty);
        self.margin.push(share);
    }
}

/// Continue the walk below the retained first row, `first`, newest first,
/// in bounded pages under the fresh window's predicate (and its cut, for a
/// dual-writer window), until the target is met or history runs out. The
/// margin's rows, pages and the merge's `retired` rows go in the report.
/// `Err` is the refusal: more than [`MAX_MARGIN_PAGES`], or out of history
/// before the target ([`WindowAcquisition::Partial`]), where the count proof
/// could not tell the partial window from one missing older rows, so the
/// full scan decides.
async fn walk_margin<M: MarginWalk>(
    tx: &mut Transaction<'_, Postgres>,
    mut merge: BlockingDrop<M>,
    first: i64,
    anchor: i64,
    cut: Option<&WindowCut>,
    retired: usize,
    report: &mut AcquisitionReport,
) -> Result<Result<BlockingDrop<M>, WindowAcquisition>> {
    let margin_page = format!(
        "{SELECT_SHARE} WHERE {} AND share_seq<$1 ORDER BY share_seq DESC LIMIT {DELTA_PAGE_ROWS}",
        super::cut::window_eligibility_sql(2, cut.map(|_| 3))
    );
    let mut margin_pages = 0;
    let mut cursor = first;
    while merge.remaining() > 0 {
        if margin_pages == MAX_MARGIN_PAGES {
            retire(merge).await?;
            return Ok(Err(WindowAcquisition::MarginTooLarge));
        }
        let rows = sqlx::query(&margin_page)
            .persistent(false)
            .bind(cursor)
            .bind(anchor)
            .bind_cut(cut)?
            .fetch_all(&mut **tx)
            .await?;
        if rows.is_empty() {
            break;
        }
        margin_pages += 1;
        report.pages += 1;
        merge = merge
            .map_anyhow(move |mut merge| {
                for row in &rows {
                    merge.take(share_from_row(row)?);
                    if merge.remaining() == 0 {
                        break;
                    }
                }
                Ok(merge)
            })
            .await?;
        cursor = i64::try_from(
            merge
                .margin()
                .last()
                .context("decoded margin page is empty")?
                .share_seq,
        )?;
    }
    report.margin_rows = merge.margin().len();
    report.retired_rows = retired;
    if merge.remaining() > 0 {
        retire(merge).await?;
        return Ok(Err(WindowAcquisition::Partial));
    }
    Ok(Ok(merge))
}

/// The proof both advances end with, over the merged window's range up to
/// `last` (the cutoff without a cut, the window's last row with one), under
/// the fresh anchor and cut.
///
/// The merged range's witness is re-read first without the count, so a
/// margin that crossed into another leaf, or an incarnation or timeline
/// that changed while the pages were read, is named as such; then the
/// same statement with the count checks complete eligible membership.
/// Within the same writer timeline, immutability makes the retained rows
/// a subset of the anchored set and the delta and margin rows were just
/// read from it; equal cardinality proves equality, even with sequence
/// gaps, retroactive INSERTs and newly eligible timestamps. This is
/// intentionally an O(window) metadata scan, not a claim of O(delta)
/// database work.
async fn prove_merged(
    tx: &mut Transaction<'_, Postgres>,
    merged: BlockingDrop<Vec<AcceptedShare>>,
    last: i64,
    anchor: i64,
    cut: Option<&WindowCut>,
    witness: LeafWitness,
    report: AcquisitionReport,
) -> Result<Advance> {
    let first = i64::try_from(merged.first().context("merged window is empty")?.share_seq)?;
    if leaf_witness(tx, first, last, anchor, cut, None)
        .await?
        .as_ref()
        != Some(&witness)
    {
        retire(merged).await?;
        return Ok(reject(report, WindowAcquisition::WitnessChanged));
    }
    let count = i64::try_from(merged.len())?;
    if leaf_witness(tx, first, last, anchor, cut, Some(count))
        .await?
        .as_ref()
        != Some(&witness)
    {
        retire(merged).await?;
        return Ok(reject(report, WindowAcquisition::CountMismatch));
    }
    Ok(Advance::Advanced {
        shares: merged,
        leaf: witness,
        report,
    })
}

/// [`advance`] for a dual-writer window, whose fresh `cut` covers the
/// retained one (the refusal checked).
///
/// The rows the fresh window may add are the rows inside the fresh cut and
/// outside the retained one: per node, `share_seq` in `(retained[n], cut[n]]`.
/// This node's are appends, but the peer's can sit anywhere below the
/// retained window's top, in its middle or under its first row, wherever the
/// peer's sequence stood when it allocated them. So the delta is read per node
/// as bounded ascending ranges, at or above the retained first row only, and
/// merged into the retained rows in `share_seq` order ([`merge_cut_delta`]),
/// where the newest-first fold runs as in [`advance`]. Rows under the retained
/// first row, old or new, are reached only by the margin, which reads the
/// fresh window's whole predicate there, so none is read twice.
///
/// Every row the retained window held stays eligible: rows never change, the
/// anchor only rises and the cut only grows. The proof is [`advance`]'s: the
/// merged range's leaf witness, then its count of rows eligible under the
/// fresh anchor and cut, which must equal the merged window's length. A
/// retained row that would no longer qualify, a delta row read twice, or one
/// missed, all fail it, and the full scan runs.
async fn advance_in_cut(
    tx: &mut Transaction<'_, Postgres>,
    prior: BlockingDrop<RetainedShares>,
    weight: u128,
    anchor: i64,
    cut: WindowCut,
    completion: &ReadAdmission,
    mut report: AcquisitionReport,
) -> Result<Advance> {
    let retained_cut = prior
        .cut
        .context("a retained dual-writer window has no cut")?;
    // Keep ownership under admission across every SQL await below.
    let prior = completion.own(prior.into_inner());
    let first = i64::try_from(prior.shares[0].share_seq)?;
    let last = i64::try_from(prior.shares[prior.shares.len() - 1].share_seq)?;
    let Some(witness) = leaf_witness(tx, first, last, anchor, Some(&cut), None).await? else {
        retire(prior).await?;
        return Ok(reject(report, WindowAcquisition::LeafChanged));
    };
    if prior.leaf.as_ref() != Some(&witness) {
        retire(prior).await?;
        return Ok(reject(report, WindowAcquisition::LeafChanged));
    }
    // The delta, one node at a time, in ascending keyset pages bounded on
    // both sides, each planned with its bounds as `read_range_owned` is.
    let delta_page = format!(
        "{SELECT_SHARE} WHERE {} AND origin_node=$6 AND share_seq>$1 AND share_seq<=$2 \
         ORDER BY share_seq LIMIT {DELTA_PAGE_ROWS}",
        super::cut::window_eligibility_sql(3, Some(4))
    );
    let mut delta = completion.own(Vec::<AcceptedShare>::new());
    for node in 0..WindowCut::NODES {
        let Some(high) = cut.get(node)? else {
            continue;
        };
        let high = i64::try_from(high)?;
        let low = i64::try_from(retained_cut.get(node)?.unwrap_or(0))?;
        let mut cursor = low.max(first - 1);
        while cursor < high {
            let rows = sqlx::query(&delta_page)
                .persistent(false)
                .bind(cursor)
                .bind(high)
                .bind(anchor)
                .bind_cut(Some(&cut))?
                .bind(i16::from(node))
                .fetch_all(&mut **tx)
                .await?;
            if rows.is_empty() {
                break;
            }
            report.pages += 1;
            delta = delta
                .map_anyhow(move |mut delta| {
                    delta.reserve(rows.len());
                    for row in &rows {
                        delta.push(share_from_row(row)?);
                    }
                    Ok(delta)
                })
                .await?;
            cursor = i64::try_from(
                delta
                    .last()
                    .context("decoded delta page is empty")?
                    .share_seq,
            )?;
        }
    }
    report.delta_rows = delta.len();
    let merge = completion
        .own((prior, delta))
        .map_anyhow(move |(prior, delta)| {
            merge_cut_delta(prior.into_inner().shares, delta.into_inner(), weight)
        })
        .await?;
    // A heavier target than the merged rows reach continues the walk below the
    // retained first row, newest first and bounded in pages, under the fresh
    // predicate: old rows and late peer rows alike.
    let retired = merge.retired;
    let merge =
        match walk_margin(tx, merge, first, anchor, Some(&cut), retired, &mut report).await? {
            Ok(merge) => merge,
            Err(outcome) => return Ok(reject(report, outcome)),
        };
    let merged = merge
        .map_anyhow(move |merge| {
            let shares = merge.assemble();
            let holds = window_invariant(&shares, weight);
            Ok((shares, holds))
        })
        .await?;
    if !merged.1 {
        tracing::error!(
            first = merged.0.first().map(|share| share.share_seq),
            last = merged.0.last().map(|share| share.share_seq),
            rows = merged.0.len(),
            "dual-writer delta window failed its ordering or crossing invariant; taking the full scan"
        );
        retire(merged).await?;
        return Ok(reject(report, WindowAcquisition::Invariant));
    }
    let merged = merged.map_anyhow(|(shares, _)| Ok(shares)).await?;
    report.window_rows = merged.len();
    let last = i64::try_from(merged.last().context("merged window is empty")?.share_seq)?;
    prove_merged(tx, merged, last, anchor, Some(&cut), witness, report).await
}

/// The cut path's state between blocking hand-offs: the merged rows the
/// newest-first fold kept (ascending, from the oldest kept row up), the margin
/// read below them (newest first), and the target weight still unmet.
struct CutMerge {
    kept: Vec<AcceptedShare>,
    margin: Vec<AcceptedShare>,
    remaining: u128,
    /// Merged rows the fold dropped from the old end.
    retired: usize,
}

impl CutMerge {
    /// Oldest first: the margin, reversed, then the kept rows.
    fn assemble(self) -> Vec<AcceptedShare> {
        let CutMerge {
            mut kept, margin, ..
        } = self;
        if margin.is_empty() {
            return kept;
        }
        let mut shares = Vec::with_capacity(margin.len() + kept.len());
        shares.extend(margin.into_iter().rev());
        shares.append(&mut kept);
        shares
    }
}

/// Merge the retained window and the delta read at or above its first row,
/// both ascending and disjoint, in `share_seq` order, and replay the full
/// reader's newest-first saturating fold over the result: the kept suffix
/// starts at the row where the fold reached zero, or at the first merged row
/// when it did not, where the margin continues it. A delta that only appends
/// is the common case, the building node's own shares, and is appended in
/// place, as [`advance`] does.
fn merge_cut_delta(
    mut prior: Vec<AcceptedShare>,
    mut delta: Vec<AcceptedShare>,
    weight: u128,
) -> Result<CutMerge> {
    // Each node's run is ascending; the runs were read one after the other.
    delta.sort_unstable_by_key(|share| share.share_seq);
    let appends = match (prior.last(), delta.first()) {
        (Some(last), Some(first)) => first.share_seq > last.share_seq,
        _ => true,
    };
    let merged = if appends {
        prior.append(&mut delta);
        prior
    } else {
        let mut merged = Vec::with_capacity(prior.len() + delta.len());
        let (mut old, mut new) = (prior.into_iter().peekable(), delta.into_iter().peekable());
        loop {
            let take_old = match (old.peek(), new.peek()) {
                (Some(retained), Some(late)) => {
                    ensure!(
                        retained.share_seq != late.share_seq,
                        "a delta row repeats retained share_seq {}",
                        late.share_seq
                    );
                    retained.share_seq < late.share_seq
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            merged.push(if take_old { old.next() } else { new.next() }.expect("peeked"));
        }
        merged
    };
    let mut remaining = weight;
    let mut start = merged.len();
    for (index, share) in merged.iter().enumerate().rev() {
        remaining = remaining.saturating_sub(share.share_difficulty);
        start = index;
        if remaining == 0 {
            break;
        }
    }
    let mut kept = merged;
    kept.drain(..start);
    if kept.capacity() > kept.len().saturating_mul(2).max(4) {
        kept.shrink_to_fit();
    }
    Ok(CutMerge {
        kept,
        margin: Vec::new(),
        remaining,
        retired: start,
    })
}

/// The merged window is one strictly ascending sequence that crosses exactly
/// at its first row: the full fold reaches zero and the fold without the
/// first row does not. Landing re-derives the same boundary
/// (`ledger/audit.rs`, `oldest_boundary`).
fn window_invariant(shares: &[AcceptedShare], weight: u128) -> bool {
    let ascending = shares
        .windows(2)
        .all(|pair| pair[0].share_seq < pair[1].share_seq);
    let without_first = shares.iter().skip(1).fold(weight, |left, share| {
        left.saturating_sub(share.share_difficulty)
    });
    let crossing = shares.first().is_some_and(|first| {
        without_first > 0 && without_first.saturating_sub(first.share_difficulty) == 0
    });
    ascending && crossing
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod cut_merge_tests;

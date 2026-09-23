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
    fn refusal(&self, anchor: i64, cutoff: u64) -> Option<WindowAcquisition> {
        if self.anchor_ms > anchor {
            return Some(WindowAcquisition::AnchorRegressed);
        }
        if self.cutoff > cutoff {
            return Some(WindowAcquisition::CutoffRegressed);
        }
        if cutoff - self.cutoff > MAX_DELTA_SLOTS {
            return Some(WindowAcquisition::DeltaTooLarge);
        }
        let Some(first) = self.shares.first() else {
            return Some(WindowAcquisition::EmptyPrior);
        };
        if first.share_seq == 0 {
            return Some(WindowAcquisition::EmptyPrior);
        }
        // Native appends always make the accepted cutoff the last retained
        // row; only legacy or raw rows with a future timestamp at the top of
        // the ledger can leave the cutoff above the retained tail.
        if self.shares.last().unwrap().share_seq != self.cutoff {
            return Some(WindowAcquisition::TailMismatch);
        }
        if self.leaf.is_none() {
            return Some(WindowAcquisition::NoEvidence);
        }
        None
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
pub(super) async fn leaf_witness(
    tx: &mut Transaction<'_, Postgres>,
    first: i64,
    last: i64,
    anchor: i64,
    expected_count: Option<i64>,
) -> Result<Option<LeafWitness>> {
    let row = sqlx::query(
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
           AND b.job_issued_at<=to_timestamp($3::double precision/1000) \
           AND ($4::bigint IS NULL OR $4=(SELECT count(*) FROM qbit_share_ledger s \
               WHERE s.share_seq BETWEEN $1 AND $2 AND s.accepted \
                 AND s.accepted_at<=to_timestamp($3::double precision/1000) \
                 AND s.job_issued_at<=to_timestamp($3::double precision/1000)))",
    )
    .bind(first)
    .bind(last)
    .bind(anchor)
    .bind(expected_count)
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
pub(super) async fn advance(
    tx: &mut Transaction<'_, Postgres>,
    prior: BlockingDrop<RetainedShares>,
    weight: u128,
    anchor: i64,
    cutoff: i64,
    completion: &ReadAdmission,
) -> Result<Advance> {
    let cutoff_u64 = u64::try_from(cutoff)?;
    let mut report = AcquisitionReport::full(WindowAcquisition::Advanced);
    report.prior_rows = prior.shares.len();
    report.prior_network = Some(prior.network);
    let reject = |mut report: AcquisitionReport, outcome: WindowAcquisition| {
        report.outcome = outcome;
        Advance::Rejected(report)
    };
    if let Some(outcome) = prior.refusal(anchor, cutoff_u64) {
        retire(prior).await?;
        return Ok(reject(report, outcome));
    }
    // Keep ownership under admission across every SQL await below.
    let prior = completion.own(prior.into_inner());
    let first = i64::try_from(prior.shares[0].share_seq)?;
    let Some(witness) = leaf_witness(tx, first, cutoff, anchor, None).await? else {
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
    let mut merge = completion
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
    let margin_page = format!(
        "{SELECT_SHARE} WHERE {} AND share_seq<$1 ORDER BY share_seq DESC LIMIT {DELTA_PAGE_ROWS}",
        super::super::audit::anchored_eligibility_sql(2)
    );
    let mut margin_pages = 0;
    let mut cursor = first;
    while merge.remaining > 0 {
        if margin_pages == MAX_MARGIN_PAGES {
            retire(merge).await?;
            return Ok(reject(report, WindowAcquisition::MarginTooLarge));
        }
        let rows = sqlx::query(&margin_page)
            .persistent(false)
            .bind(cursor)
            .bind(anchor)
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
                    let share = share_from_row(row)?;
                    merge.remaining = merge.remaining.saturating_sub(share.share_difficulty);
                    merge.margin.push(share);
                    if merge.remaining == 0 {
                        break;
                    }
                }
                Ok(merge)
            })
            .await?;
        cursor = i64::try_from(
            merge
                .margin
                .last()
                .context("decoded margin page is empty")?
                .share_seq,
        )?;
    }
    report.margin_rows = merge.margin.len();
    report.retired_rows = merge.start;
    if merge.remaining > 0 {
        // Out of history before the target: the count proof below could not
        // tell this partial window from one missing older rows, so the full
        // scan decides.
        retire(merge).await?;
        return Ok(reject(report, WindowAcquisition::Partial));
    }
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
            let ascending = shares
                .windows(2)
                .all(|pair| pair[0].share_seq < pair[1].share_seq);
            let without_first = shares.iter().skip(1).fold(weight, |left, share| {
                left.saturating_sub(share.share_difficulty)
            });
            let crossing = shares.first().is_some_and(|first| {
                without_first > 0 && without_first.saturating_sub(first.share_difficulty) == 0
            });
            Ok((shares, ascending && crossing))
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
    // The merged range's witness is re-read first without the count, so a
    // margin that crossed into another leaf, or an incarnation or timeline
    // that changed while the pages were read, is named as such; then the
    // same statement with the count checks complete eligible membership.
    // Within the same writer timeline, immutability makes the retained rows
    // a subset of the anchored set and the delta and margin rows were just
    // read from it; equal cardinality proves equality, even with sequence
    // gaps, retroactive INSERTs and newly eligible timestamps. This is
    // intentionally an O(window) metadata scan, not a claim of O(delta)
    // database work.
    let first = i64::try_from(
        merged
            .first()
            .context("full retained suffix disappeared")?
            .share_seq,
    )?;
    if leaf_witness(tx, first, cutoff, anchor, None)
        .await?
        .as_ref()
        != Some(&witness)
    {
        retire(merged).await?;
        return Ok(reject(report, WindowAcquisition::WitnessChanged));
    }
    let count = i64::try_from(merged.len())?;
    if leaf_witness(tx, first, cutoff, anchor, Some(count))
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

#[cfg(test)]
mod tests;

//! Measurements the dual-writer scenarios report: how long a share takes to
//! reach the peer, and which acknowledged shares a dead node had not yet
//! handed to its survivor (its unsynced tail).

use crate::{frontend::Node, load::RunClock, load::ShareRecord};
use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::PgPool;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::JoinHandle;

/// How often the lag sampler reads both databases; the resolution of every
/// lag it reports.
pub const LAG_POLL: Duration = Duration::from_millis(100);
/// A sample still unseen on the peer after this is reported as unresolved.
const LAG_GIVE_UP_MS: u64 = 30_000;

/// Samples, while it runs, the newest accepted share committed on one
/// database and the time it first appears on the other.
pub struct LagSampler {
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<(Vec<LagSample>, u64)>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LagSample {
    pub share_id: String,
    pub seen_on_origin_ms: u64,
    pub seen_on_peer_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LagReport {
    pub samples: usize,
    pub resolved: usize,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub max_ms: Option<u64>,
    /// The sampler's poll interval: no lag below it can be told apart.
    pub resolution_ms: u64,
    /// Reads of either database that failed; each costs a sample or delays
    /// one's resolution.
    pub failed_polls: u64,
}

impl LagSampler {
    /// Start sampling shares committed on `origin` until they appear on
    /// `peer`.
    pub fn start(origin: PgPool, peer: PgPool, clock: RunClock) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let stop = stop.clone();
            async move {
                let mut samples: Vec<LagSample> = Vec::new();
                let mut failed_polls = 0;
                let mut last: Option<String> = None;
                while !stop.load(Ordering::SeqCst) {
                    let newest = sqlx::query_scalar::<_, String>(
                        "SELECT share_id FROM qbit_share_ledger WHERE accepted \
                         ORDER BY accepted_at DESC, share_seq DESC LIMIT 1",
                    )
                    .fetch_optional(&origin)
                    .await;
                    let now = clock.now_ms();
                    match newest {
                        Ok(Some(newest)) if last.as_ref() != Some(&newest) => {
                            samples.push(LagSample {
                                share_id: newest.clone(),
                                seen_on_origin_ms: now,
                                seen_on_peer_ms: None,
                            });
                            last = Some(newest);
                        }
                        Ok(_) => {}
                        Err(_) => failed_polls += 1,
                    }
                    let pending: Vec<String> = samples
                        .iter()
                        .filter(|s| {
                            s.seen_on_peer_ms.is_none()
                                && now.saturating_sub(s.seen_on_origin_ms) < LAG_GIVE_UP_MS
                        })
                        .map(|s| s.share_id.clone())
                        .collect();
                    if !pending.is_empty() {
                        match sqlx::query_scalar::<_, String>(
                            "SELECT share_id FROM qbit_prism_share_hashes WHERE share_id = ANY($1)",
                        )
                        .bind(&pending)
                        .fetch_all(&peer)
                        .await
                        {
                            Ok(seen) => {
                                let seen: BTreeSet<String> = seen.into_iter().collect();
                                let at = clock.now_ms();
                                for sample in samples.iter_mut() {
                                    if sample.seen_on_peer_ms.is_none()
                                        && seen.contains(&sample.share_id)
                                    {
                                        sample.seen_on_peer_ms = Some(at);
                                    }
                                }
                            }
                            Err(_) => failed_polls += 1,
                        }
                    }
                    tokio::time::sleep(LAG_POLL).await;
                }
                (samples, failed_polls)
            }
        });
        Self {
            stop,
            task: Some(task),
        }
    }

    /// Stop sampling and summarise.
    pub async fn stop(mut self) -> Result<(LagReport, Vec<LagSample>)> {
        self.stop.store(true, Ordering::SeqCst);
        let task = self
            .task
            .take()
            .context("the sampler was already stopped")?;
        let (samples, failed_polls) = task.await?;
        let mut report = lag_report(&samples);
        report.failed_polls = failed_polls;
        Ok((report, samples))
    }
}

impl Drop for LagSampler {
    /// A scenario that fails part-way drops its samplers: stop polling.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub fn lag_report(samples: &[LagSample]) -> LagReport {
    let mut lags: Vec<u64> = samples
        .iter()
        .filter_map(|s| Some(s.seen_on_peer_ms? - s.seen_on_origin_ms))
        .collect();
    lags.sort_unstable();
    let percentile = |p: f64| -> Option<u64> {
        if lags.is_empty() {
            return None;
        }
        let rank = ((p * lags.len() as f64).ceil() as usize).clamp(1, lags.len());
        Some(lags[rank - 1])
    };
    LagReport {
        samples: samples.len(),
        resolved: lags.len(),
        p50_ms: percentile(0.50),
        p95_ms: percentile(0.95),
        max_ms: lags.last().copied(),
        resolution_ms: LAG_POLL.as_millis() as u64,
        failed_polls: 0,
    }
}

/// CONTRACT.md D-5: within a sync cycle shares are applied before
/// landings, so a peer's block is never seen on a node before every share
/// of its window is. While it runs, the sampler polls one node for peer
/// blocks it has not seen yet and, in the same statement (one snapshot),
/// counts each one's window shares there against the recorded count.
///
/// It samples: an out-of-order landing shorter than its poll interval can
/// go unseen, and a failed poll widens that blind spot (it is counted and
/// reported, never ignored). A block whose audit bundle arrives after its
/// landing row is counted once its window record does; since shares only
/// ever accumulate, a short count then still proves a violation, while a
/// full one cannot prove the order and is reported as unresolved.
pub struct LandingOrderSampler {
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<LandingOrderReport>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LandingOrder {
    pub block: String,
    /// When the sampler first saw the block's landing row.
    pub seen_ms: u64,
    /// When its window was counted: `seen_ms` when the window's record was
    /// there with the block, later when it arrived after it, never when it
    /// did not arrive while the sampler ran.
    pub counted_ms: Option<u64>,
    pub window_shares: Option<i64>,
    pub present: Option<i64>,
    /// A bootstrap window, held inline: no ledger rows to wait for.
    pub inline: bool,
}

/// What a sample shows about D-5.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LandingVerdict {
    /// Counted with the block's first sighting, every share present.
    InOrder,
    /// Counted short: the block was there before its window's shares.
    Violated,
    /// Counted complete only after the block's first sighting, inline, or
    /// never counted: the order is not shown either way.
    Unresolved,
}

impl LandingOrder {
    pub fn verdict(&self) -> LandingVerdict {
        match (self.window_shares, self.present) {
            _ if self.inline => LandingVerdict::Unresolved,
            (Some(window), Some(present)) if present < window => LandingVerdict::Violated,
            (Some(_), Some(_)) if self.counted_ms == Some(self.seen_ms) => LandingVerdict::InOrder,
            _ => LandingVerdict::Unresolved,
        }
    }
}

/// Everything one sampler saw.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LandingOrderReport {
    pub samples: Vec<LandingOrder>,
    pub polls: u64,
    pub failed_polls: u64,
    /// The first errors, in order.
    pub errors: Vec<String>,
    /// The poll interval: an out-of-order landing shorter than this can go
    /// unseen.
    pub resolution_ms: u64,
}

/// How many poll errors a report keeps.
const KEPT_ERRORS: usize = 10;

/// One poll's row: the block, its window's recorded count, the window
/// shares present now (both NULL until the window's record is there), and
/// whether the window is inline.
type LandingRow = (String, Option<i64>, Option<i64>, Option<bool>);

/// The poll: each peer block not yet settled, with its window counted in
/// the same statement. `cut` adds D-13's per-node cut, which a window
/// without one (both columns NULL) skips.
fn landing_order_sql(cut: bool) -> String {
    let cut = if cut {
        "AND ((s.cut_seq_0 IS NULL AND s.cut_seq_1 IS NULL) \
              OR (l.origin_node = 0 AND l.share_seq <= s.cut_seq_0) \
              OR (l.origin_node = 1 AND l.share_seq <= s.cut_seq_1))"
    } else {
        ""
    };
    format!(
        "SELECT b.block_hash, s.share_count::bigint, \
                CASE WHEN s.snapshot_sha256 IS NULL OR s.inline_shares IS NOT NULL THEN NULL \
                     ELSE (SELECT count(*) FROM qbit_share_ledger l \
                            WHERE l.accepted \
                              AND l.share_seq BETWEEN s.first_share_seq AND s.last_share_seq \
                              AND l.accepted_at <= to_timestamp(s.anchor_ms::double precision / 1000) \
                              AND l.job_issued_at <= to_timestamp(s.anchor_ms::double precision / 1000) \
                              {cut}) END, \
                s.inline_shares IS NOT NULL \
         FROM qbit_pool_blocks b \
         LEFT JOIN qbit_pool_audit_bundles a ON a.block_hash = b.block_hash \
         LEFT JOIN qbit_prism_audit_snapshots s ON s.snapshot_sha256 = a.share_snapshot_sha256 \
         WHERE b.origin_node = $1 AND NOT (b.block_hash = ANY($2))"
    )
}

impl LandingOrderSampler {
    /// Watch `pool` (one node's database) for blocks of `peer` origin that
    /// arrive after it starts.
    pub fn start(pool: PgPool, peer: Node, clock: RunClock) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let stop = stop.clone();
            async move {
                let mut report = LandingOrderReport {
                    resolution_ms: LAG_POLL.as_millis() as u64,
                    ..Default::default()
                };
                let mut sql: Option<String> = None;
                // Blocks there before the first poll, and blocks counted:
                // neither is looked at again. Blocks seen but not yet
                // counted stay out of it, so their window is counted the
                // moment it arrives.
                let mut settled: Vec<String> = Vec::new();
                let mut baseline = false;
                let mut index: BTreeMap<String, usize> = BTreeMap::new();
                while !stop.load(Ordering::SeqCst) {
                    report.polls += 1;
                    let rows = async {
                        if sql.is_none() {
                            let shape = crate::invariants::window_shape(&pool).await?;
                            sql = Some(landing_order_sql(
                                shape == crate::invariants::WindowShape::Cut,
                            ));
                        }
                        let rows: Vec<LandingRow> =
                            sqlx::query_as(sql.as_deref().unwrap_or_default())
                                .bind(peer.index() as i16)
                                .bind(&settled)
                                .fetch_all(&pool)
                                .await?;
                        anyhow::Ok(rows)
                    }
                    .await;
                    let now = clock.now_ms();
                    match rows {
                        Err(error) => {
                            report.failed_polls += 1;
                            if report.errors.len() < KEPT_ERRORS {
                                report.errors.push(format!("at {now} ms: {error:#}"));
                            }
                        }
                        Ok(rows) if !baseline => {
                            baseline = true;
                            settled.extend(rows.into_iter().map(|(block, ..)| block));
                        }
                        Ok(rows) => {
                            for (block, window, present, inline) in rows {
                                let at = *index.entry(block.clone()).or_insert_with(|| {
                                    report.samples.push(LandingOrder {
                                        block: block.clone(),
                                        seen_ms: now,
                                        counted_ms: None,
                                        window_shares: None,
                                        present: None,
                                        inline: false,
                                    });
                                    report.samples.len() - 1
                                });
                                let sample = &mut report.samples[at];
                                if inline == Some(true) {
                                    sample.inline = true;
                                    settled.push(block);
                                } else if let (Some(window), Some(present)) = (window, present) {
                                    sample.counted_ms = Some(now);
                                    sample.window_shares = Some(window);
                                    sample.present = Some(present);
                                    settled.push(block);
                                }
                            }
                        }
                    }
                    tokio::time::sleep(LAG_POLL).await;
                }
                report
            }
        });
        Self {
            stop,
            task: Some(task),
        }
    }

    pub async fn stop(mut self) -> Result<LandingOrderReport> {
        self.stop.store(true, Ordering::SeqCst);
        let task = self
            .task
            .take()
            .context("the sampler was already stopped")?;
        Ok(task.await?)
    }
}

impl Drop for LandingOrderSampler {
    /// A scenario that fails part-way drops its samplers: stop polling.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Every header hash a database credits now.
pub async fn credited_headers(pool: &PgPool) -> Result<BTreeSet<String>> {
    Ok(
        sqlx::query_scalar::<_, String>("SELECT header_hash FROM qbit_prism_share_hashes")
            .fetch_all(pool)
            .await?
            .into_iter()
            .collect(),
    )
}

/// A dead node's unsynced tail: the shares it acknowledged up to the
/// moment it went (`fault_at_ms`) that its survivor did not hold then.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TailReport {
    pub node: Option<Node>,
    pub fault_at_ms: u64,
    pub count: usize,
    /// When the first and the last of them were answered.
    pub first_answered_ms: Option<u64>,
    pub last_answered_ms: Option<u64>,
    pub share_ids: Vec<String>,
}

/// The tail of `dead`, given what the survivor held right after the fault.
/// A share counts as the dead node's when the job it was mined on was the
/// dead node's.
pub fn tail(
    records: &[ShareRecord],
    dead: Node,
    fault_at_ms: u64,
    survivor_held: &BTreeSet<String>,
) -> TailReport {
    let mut shares: Vec<&ShareRecord> = records
        .iter()
        .filter(|record| {
            record.accepted()
                && record.issuer == Some(dead)
                && record.answered_ms.is_some_and(|at| at <= fault_at_ms)
                && !survivor_held.contains(&record.header_hash().to_ascii_lowercase())
        })
        .collect();
    shares.sort_by_key(|record| record.answered_ms);
    TailReport {
        node: Some(dead),
        fault_at_ms,
        count: shares.len(),
        first_answered_ms: shares.first().and_then(|r| r.answered_ms),
        last_answered_ms: shares.last().and_then(|r| r.answered_ms),
        share_ids: shares.iter().map(|r| r.share_id.clone()).collect(),
    }
}

/// `excused_missing` for the checker: each tail share, with why.
pub fn excuse(tail: &TailReport, why: &str) -> BTreeMap<String, String> {
    tail.share_ids
        .iter()
        .map(|id| (id.clone(), why.to_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acked(id: &str, issuer: Node, answered: u64) -> ShareRecord {
        ShareRecord {
            share_id: format!("u.w:{id}"),
            session: 0,
            account: "a".into(),
            job_id: String::new(),
            issuer: Some(issuer),
            outcome: "accepted".into(),
            reason: None,
            sent_ms: answered - 1,
            answered_ms: Some(answered),
            scheduled_block: false,
        }
    }

    #[test]
    fn the_tail_is_the_dead_nodes_acked_shares_the_survivor_lacked() {
        let records = vec![
            acked("aa", Node::A, 100),
            acked("bb", Node::A, 200),
            acked("cc", Node::A, 300),
            acked("dd", Node::B, 250),
            acked("ee", Node::A, 900),
        ];
        let held: BTreeSet<String> = ["aa".to_owned()].into();
        let tail = tail(&records, Node::A, 500, &held);
        assert_eq!(tail.count, 2);
        assert_eq!(tail.share_ids, vec!["u.w:bb", "u.w:cc"]);
        assert_eq!(
            (tail.first_answered_ms, tail.last_answered_ms),
            (Some(200), Some(300))
        );
        assert_eq!(excuse(&tail, "why").len(), 2);
    }

    fn landing(seen: u64, counted: Option<u64>, window: i64, present: i64) -> LandingOrder {
        LandingOrder {
            block: "b".into(),
            seen_ms: seen,
            counted_ms: counted,
            window_shares: counted.map(|_| window),
            present: counted.map(|_| present),
            inline: false,
        }
    }

    #[test]
    fn a_landing_shows_the_order_only_when_counted_as_it_arrived() {
        assert_eq!(
            landing(100, Some(100), 50, 50).verdict(),
            LandingVerdict::InOrder
        );
        assert_eq!(
            landing(100, Some(100), 50, 49).verdict(),
            LandingVerdict::Violated
        );
        // Counted after it arrived: a short count still proves the block came
        // first (shares only accumulate); a full one proves nothing.
        assert_eq!(
            landing(100, Some(400), 50, 49).verdict(),
            LandingVerdict::Violated
        );
        assert_eq!(
            landing(100, Some(400), 50, 50).verdict(),
            LandingVerdict::Unresolved
        );
        assert_eq!(
            landing(100, None, 0, 0).verdict(),
            LandingVerdict::Unresolved
        );
        let mut inline = landing(100, Some(100), 50, 50);
        inline.inline = true;
        assert_eq!(inline.verdict(), LandingVerdict::Unresolved);
    }

    #[test]
    fn the_landing_poll_applies_the_cut_only_where_the_schema_has_one() {
        assert!(landing_order_sql(true).contains("s.cut_seq_0"));
        assert!(!landing_order_sql(false).contains("cut_seq"));
        for cut in [true, false] {
            let sql = landing_order_sql(cut);
            assert!(sql.contains("LEFT JOIN qbit_pool_audit_bundles"));
            assert!(sql.contains("b.origin_node = $1"));
        }
    }

    #[test]
    fn lag_percentiles_ignore_unresolved_samples() {
        let samples = vec![
            LagSample {
                share_id: "1".into(),
                seen_on_origin_ms: 0,
                seen_on_peer_ms: Some(100),
            },
            LagSample {
                share_id: "2".into(),
                seen_on_origin_ms: 0,
                seen_on_peer_ms: Some(300),
            },
            LagSample {
                share_id: "3".into(),
                seen_on_origin_ms: 0,
                seen_on_peer_ms: None,
            },
        ];
        let report = lag_report(&samples);
        assert_eq!((report.samples, report.resolved), (3, 2));
        assert_eq!(report.p50_ms, Some(100));
        assert_eq!(report.max_ms, Some(300));
    }
}

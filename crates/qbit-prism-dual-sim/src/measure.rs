//! Measurements the dual-writer scenarios report: how long a share takes to
//! reach the peer, whether a peer's block ever arrives before its window's
//! shares (D-5), and which acknowledged shares a dead node had not yet
//! handed to its survivor (its unsynced tail).

use crate::{frontend::Node, load::RunClock, load::ShareRecord};
use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::PgPool;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::JoinHandle;

/// How often the samplers read the databases: the resolution of what they
/// report.
pub const LAG_POLL: Duration = Duration::from_millis(100);
/// A sample still unseen on the peer after this is reported as unresolved.
const LAG_GIVE_UP_MS: u64 = 30_000;
/// How many poll errors a report keeps.
const KEPT_ERRORS: usize = 10;

/// A polling task that runs until stopped and returns what it gathered. A
/// scenario that fails part-way drops it, which stops it too.
struct Poller<T> {
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<T>>,
}

impl<T: Send + 'static> Poller<T> {
    /// Run `body`, which checks the stop flag it is given between polls.
    fn spawn<F>(body: impl FnOnce(Arc<AtomicBool>) -> F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(body(stop.clone()));
        Self {
            stop,
            task: Some(task),
        }
    }

    /// Stop polling and take what it gathered.
    async fn finish(mut self) -> Result<T> {
        self.stop.store(true, Ordering::SeqCst);
        let task = self
            .task
            .take()
            .context("the poller was already finished")?;
        Ok(task.await?)
    }
}

impl<T> Drop for Poller<T> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Samples, while it runs, the newest accepted share committed on one
/// database and the time it first appears on the other.
pub struct LagSampler {
    poller: Poller<(Vec<LagSample>, u64)>,
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
        let poller = Poller::spawn(|stop| async move {
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
        });
        Self { poller }
    }

    /// Stop sampling and summarise.
    pub async fn stop(self) -> Result<(LagReport, Vec<LagSample>)> {
        let (samples, failed_polls) = self.poller.finish().await?;
        let mut report = lag_report(&samples);
        report.failed_polls = failed_polls;
        Ok((report, samples))
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
/// of its window is. And D-10: a peer block is applied with all its child
/// rows at once, its audit bundle and window snapshot among them.
///
/// While it runs, the sampler polls one node for peer blocks it has not seen
/// yet and, in the same statement (one snapshot), counts each one's window
/// shares there against the window's recorded count. It samples: an
/// out-of-order landing shorter than its poll interval can go unseen. A
/// block first seen after a failed poll may have arrived during it, so it
/// cannot show the order unless counted short; failed polls are counted and
/// listed.
pub struct LandingOrderSampler {
    poller: Poller<LandingOrderReport>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LandingOrder {
    pub block: String,
    /// When the sampler first saw the block's landing row.
    pub seen_ms: u64,
    /// The poll before that one failed: the block may have arrived earlier.
    pub after_failed_poll: bool,
    /// The block's audit bundle or window snapshot was missing when the
    /// block was first seen (D-10).
    pub torn: bool,
    /// When its window was counted: at `seen_ms` unless torn, and never
    /// when its window record did not arrive while the sampler ran.
    pub counted_ms: Option<u64>,
    pub window_shares: Option<i64>,
    pub present: Option<i64>,
    /// A bootstrap window, held inline: no ledger rows to wait for.
    pub inline: bool,
}

/// What one sample shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LandingVerdict {
    /// Seen with every share of its window already there.
    InOrder,
    /// Counted short: the block was there before its window's shares (D-5).
    /// Shares only accumulate, so a short count at any time proves it.
    Violated,
    /// Seen without its audit bundle or window snapshot (D-10).
    Torn,
    /// An inline window, or complete but first seen after a failed poll:
    /// the order is not shown either way.
    Unresolved,
}

impl LandingOrder {
    pub fn verdict(&self) -> LandingVerdict {
        match (self.window_shares, self.present) {
            (Some(window), Some(present)) if !self.inline && present < window => {
                LandingVerdict::Violated
            }
            _ if self.torn => LandingVerdict::Torn,
            _ if self.inline || self.after_failed_poll => LandingVerdict::Unresolved,
            (Some(_), Some(_)) => LandingVerdict::InOrder,
            _ => LandingVerdict::Unresolved,
        }
    }
}

/// Everything one landing-order sampler saw.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LandingOrderReport {
    pub samples: Vec<LandingOrder>,
    /// Peer blocks already there when the sampler started: not sampled.
    pub baseline: usize,
    pub polls: u64,
    pub failed_polls: u64,
    /// The first errors, in order.
    pub errors: Vec<String>,
    /// The poll interval: an out-of-order landing shorter than this can go
    /// unseen.
    pub resolution_ms: u64,
}

impl LandingOrderReport {
    pub fn count(&self, verdict: LandingVerdict) -> usize {
        self.samples
            .iter()
            .filter(|sample| sample.verdict() == verdict)
            .count()
    }
}

/// One poll's row: the block, whether its window record (audit bundle and
/// snapshot) is there, the window's recorded count, the window shares
/// present now, and whether the window is inline.
type LandingRow = (String, bool, Option<i64>, Option<i64>, bool);

/// The poll: each peer block not yet settled, with its window counted in the
/// same statement by D-13's eligibility (`invariants::eligibility`), with
/// the cut where the schema has one.
fn landing_order_sql(cut: bool) -> String {
    let eligible = crate::invariants::eligibility(
        "s.anchor_ms",
        cut.then_some(("s.cut_seq_0", "s.cut_seq_1")),
    );
    format!(
        "SELECT b.block_hash, s.snapshot_sha256 IS NOT NULL, s.share_count::bigint, \
                CASE WHEN s.snapshot_sha256 IS NULL OR s.inline_shares IS NOT NULL THEN NULL \
                     ELSE (SELECT count(*) FROM qbit_share_ledger l \
                            WHERE l.share_seq BETWEEN s.first_share_seq AND s.last_share_seq \
                              AND {eligible}) END, \
                s.inline_shares IS NOT NULL \
         FROM qbit_pool_blocks b \
         LEFT JOIN qbit_pool_audit_bundles a ON a.block_hash = b.block_hash \
         LEFT JOIN qbit_prism_audit_snapshots s ON s.snapshot_sha256 = a.share_snapshot_sha256 \
         WHERE b.origin_node = $1 AND NOT (b.block_hash = ANY($2))"
    )
}

async fn poll_landings(
    pool: &PgPool,
    sql: &str,
    peer: Node,
    settled: &[String],
) -> Result<Vec<LandingRow>> {
    Ok(sqlx::query_as(sql)
        .bind(peer.index() as i16)
        .bind(settled)
        .fetch_all(pool)
        .await?)
}

impl LandingOrderSampler {
    /// Watch `pool` (one node's database) for blocks of `peer` origin. The
    /// first poll, which takes the blocks already there as the baseline,
    /// runs before this returns: a sampler that cannot watch fails its
    /// scenario, and every block that lands afterwards is sampled.
    pub async fn start(pool: PgPool, peer: Node, clock: RunClock) -> Result<Self> {
        let shape = crate::invariants::window_shape(&pool).await?;
        let sql = landing_order_sql(shape == crate::invariants::WindowShape::Cut);
        let mut settled: Vec<String> = poll_landings(&pool, &sql, peer, &[])
            .await
            .context("the landing-order sampler's first poll")?
            .into_iter()
            .map(|(block, ..)| block)
            .collect();
        let poller = Poller::spawn(move |stop| async move {
            let mut report = LandingOrderReport {
                baseline: settled.len(),
                resolution_ms: LAG_POLL.as_millis() as u64,
                ..Default::default()
            };
            let mut index: BTreeMap<String, usize> = BTreeMap::new();
            let mut previous_ok = true;
            while !stop.load(Ordering::SeqCst) {
                tokio::time::sleep(LAG_POLL).await;
                report.polls += 1;
                let rows = poll_landings(&pool, &sql, peer, &settled).await;
                let now = clock.now_ms();
                let rows = match rows {
                    Ok(rows) => rows,
                    Err(error) => {
                        report.failed_polls += 1;
                        if report.errors.len() < KEPT_ERRORS {
                            report.errors.push(format!("at {now} ms: {error:#}"));
                        }
                        previous_ok = false;
                        continue;
                    }
                };
                for (block, recorded, window, present, inline) in rows {
                    let at = *index.entry(block.clone()).or_insert_with(|| {
                        report.samples.push(LandingOrder {
                            block: block.clone(),
                            seen_ms: now,
                            after_failed_poll: !previous_ok,
                            torn: !recorded,
                            counted_ms: None,
                            window_shares: None,
                            present: None,
                            inline: false,
                        });
                        report.samples.len() - 1
                    });
                    // A torn block is polled again until its window record
                    // arrives, and counted then.
                    if recorded {
                        let sample = &mut report.samples[at];
                        sample.counted_ms = Some(now);
                        sample.window_shares = window;
                        sample.present = present;
                        sample.inline = inline;
                        settled.push(block);
                    }
                }
                previous_ok = true;
            }
            report
        });
        Ok(Self { poller })
    }

    pub async fn stop(self) -> Result<LandingOrderReport> {
        self.poller.finish().await
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

    /// A block seen at 100 ms and counted `window`/`present` then.
    fn landing(window: i64, present: i64) -> LandingOrder {
        LandingOrder {
            block: "b".into(),
            seen_ms: 100,
            after_failed_poll: false,
            torn: false,
            counted_ms: Some(100),
            window_shares: Some(window),
            present: Some(present),
            inline: false,
        }
    }

    #[test]
    fn a_landing_shows_the_order_only_when_seen_whole_on_a_poll_after_a_good_one() {
        assert_eq!(landing(50, 50).verdict(), LandingVerdict::InOrder);
        assert_eq!(landing(50, 49).verdict(), LandingVerdict::Violated);
        // Seen without its window record: a D-10 breach, counted later. A
        // short count then still proves the block came first (shares only
        // accumulate).
        let torn = LandingOrder {
            torn: true,
            counted_ms: Some(400),
            ..landing(50, 50)
        };
        assert_eq!(torn.verdict(), LandingVerdict::Torn);
        let torn_short = LandingOrder {
            present: Some(10),
            ..torn.clone()
        };
        assert_eq!(torn_short.verdict(), LandingVerdict::Violated);
        let never_counted = LandingOrder {
            counted_ms: None,
            window_shares: None,
            present: None,
            ..torn
        };
        assert_eq!(never_counted.verdict(), LandingVerdict::Torn);
        // After a failed poll the block may have arrived earlier: complete
        // proves nothing, short still proves the violation.
        let late = LandingOrder {
            after_failed_poll: true,
            ..landing(50, 50)
        };
        assert_eq!(late.verdict(), LandingVerdict::Unresolved);
        let late_short = LandingOrder {
            present: Some(49),
            ..late
        };
        assert_eq!(late_short.verdict(), LandingVerdict::Violated);
        let inline = LandingOrder {
            inline: true,
            window_shares: Some(5),
            present: None,
            ..landing(5, 5)
        };
        assert_eq!(inline.verdict(), LandingVerdict::Unresolved);
    }

    #[test]
    fn the_landing_poll_applies_the_cut_only_where_the_schema_has_one() {
        assert!(landing_order_sql(true).contains("s.cut_seq_0"));
        let anchored = landing_order_sql(false);
        assert!(!anchored.contains("cut_seq"));
        assert!(
            !anchored.contains("l.origin_node"),
            "a 3.0 schema has no origin_node on the ledger"
        );
        for cut in [true, false] {
            let sql = landing_order_sql(cut);
            assert!(sql.contains("LEFT JOIN qbit_pool_audit_bundles"));
            assert!(sql.contains("b.origin_node = $1"));
            assert!(sql.contains(&crate::invariants::eligibility(
                "s.anchor_ms",
                cut.then_some(("s.cut_seq_0", "s.cut_seq_1"))
            )));
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

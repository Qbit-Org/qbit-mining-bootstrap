//! Measurements the dual-writer scenarios report: how long a share takes to
//! reach the peer, and which acknowledged shares a dead node had not yet
//! handed to its survivor (its unsynced tail).

use crate::{frontend::Node, load::RunClock, load::ShareRecord};
use anyhow::Result;
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
    task: JoinHandle<Result<Vec<LagSample>>>,
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
                let mut last: Option<String> = None;
                while !stop.load(Ordering::SeqCst) {
                    let newest: Option<String> = sqlx::query_scalar(
                        "SELECT share_id FROM qbit_share_ledger WHERE accepted \
                         ORDER BY accepted_at DESC, share_seq DESC LIMIT 1",
                    )
                    .fetch_optional(&origin)
                    .await
                    .unwrap_or(None);
                    let now = clock.now_ms();
                    if let Some(newest) = newest {
                        if last.as_ref() != Some(&newest) {
                            samples.push(LagSample {
                                share_id: newest.clone(),
                                seen_on_origin_ms: now,
                                seen_on_peer_ms: None,
                            });
                            last = Some(newest);
                        }
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
                        let seen: Vec<String> = sqlx::query_scalar(
                            "SELECT share_id FROM qbit_prism_share_hashes WHERE share_id = ANY($1)",
                        )
                        .bind(&pending)
                        .fetch_all(&peer)
                        .await
                        .unwrap_or_default();
                        let seen: BTreeSet<String> = seen.into_iter().collect();
                        let at = clock.now_ms();
                        for sample in samples.iter_mut() {
                            if sample.seen_on_peer_ms.is_none() && seen.contains(&sample.share_id) {
                                sample.seen_on_peer_ms = Some(at);
                            }
                        }
                    }
                    tokio::time::sleep(LAG_POLL).await;
                }
                Ok(samples)
            }
        });
        Self { stop, task }
    }

    /// Stop sampling, give pending samples a last look, and summarise.
    pub async fn stop(self) -> Result<(LagReport, Vec<LagSample>)> {
        self.stop.store(true, Ordering::SeqCst);
        let samples = self.task.await??;
        Ok((lag_report(&samples), samples))
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

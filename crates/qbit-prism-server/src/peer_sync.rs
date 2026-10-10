//! The 3.1 peer sync: with `PRISM_DUAL_WRITER` on, each node pulls the rows
//! its peer originated in the copied tables, over `PRISM_PEER_DATABASE_URL`
//! (or the fallback), and inserts them by identity. It never follows a WAL
//! position, and it never copies derived or local state: balance summaries,
//! claims and leases, sessions, the cluster singleton, rollups, divergences,
//! the payout revision, the candidate outbox and deferred shares, broadcast
//! attempts and CPFP packages, the node identity and the sync's own state
//! (CONTRACT §1, D-2, D-9).
//!
//! [`PeerSyncStatus`] is what the sync publishes, through a
//! `tokio::sync::watch` channel, for health and readiness (D-8, D-9).
use serde::Serialize;
use std::collections::BTreeMap;
use tokio::sync::watch;

mod engine;
mod offer_wait;
pub use engine::{PassReport, PeerSync, Refusal};
pub use offer_wait::{AdoptionNeeds, PeerIngest, PeerIngestWait};

/// Every table the peer sync copies, in the order a pull inserts them
/// (parents before the rows that reference them). Each has an
/// `origin_node smallint NOT NULL DEFAULT 0` (migration 027), which
/// `node-identity set` sets to the node's index.
pub const COPIED_TABLES: &[&str] = &[
    "qbit_share_ledger",
    "qbit_prism_share_hashes",
    "qbit_pool_blocks",
    "qbit_prism_audit_snapshots",
    "qbit_pool_audit_bundles",
    "qbit_pool_payout_entries",
    "qbit_payout_carry_forward",
    "qbit_ctv_fanout_sets",
    "qbit_ctv_fanout_artifacts",
    "qbit_prism_templates",
    "qbit_prism_balance_snapshots",
    "qbit_prism_jobs",
    "qbit_prism_node_roles",
];

/// The sync's view of the peer and of this node's own log.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct PeerSyncStatus {
    /// The last pull reached the peer's database on one of its paths.
    /// Reported, never a readiness input.
    pub peer_reachable: bool,
    /// The startup lineage latch (CONTRACT D-8), the one readiness input.
    /// False at start, and again on a detected local rollback, until this node
    /// has verified that it holds every row it originated that the peer
    /// holds, pulling back any it lacks; then true. Losing the peer later
    /// never clears it. Starting with the peer unreachable, it is true unless
    /// the database shows evidence of a rollback (D-17: a new system
    /// identifier or WAL timeline since the last verification, or no
    /// verification recorded); with evidence it stays false and alerts.
    pub own_log_caught_up: bool,
    /// Per copied table, by name. Reported, never a readiness input.
    pub per_table: BTreeMap<String, TableSyncStatus>,
}

/// How far this node's copy of one table is behind the peer's rows.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct TableSyncStatus {
    /// Peer-originated rows the last pull saw on the peer and this node
    /// does not hold yet.
    pub lag_rows: u64,
    /// Seconds since this table was last fully caught up with the peer.
    pub lag_seconds: f64,
    /// When a pull of this table last completed.
    pub last_success: Option<chrono::DateTime<chrono::Utc>>,
}

/// The sync task's end of the status channel. Public so that a test of what
/// reads the status can publish one.
#[derive(Debug)]
pub struct PeerSyncPublisher {
    status: watch::Sender<PeerSyncStatus>,
}

impl PeerSyncPublisher {
    /// A publisher and the receiver the Coordinator reads the status from.
    /// Until the sync publishes, the receiver holds the default: the peer
    /// unreached and the own log not caught up.
    pub fn new() -> (Self, watch::Receiver<PeerSyncStatus>) {
        let (status, receiver) = watch::channel(PeerSyncStatus::default());
        (Self { status }, receiver)
    }

    pub fn update(&self, change: impl FnOnce(&mut PeerSyncStatus)) {
        self.status.send_modify(change);
    }

    /// Another receiver of the same status.
    pub fn subscribe(&self) -> watch::Receiver<PeerSyncStatus> {
        self.status.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_receiver_before_any_publication_reports_nothing_caught_up() {
        let (_publisher, status) = PeerSyncPublisher::new();
        assert!(!status.borrow().own_log_caught_up);
        assert_eq!(*status.borrow(), PeerSyncStatus::default());
        assert_eq!(
            serde_json::to_value(&*status.borrow()).unwrap(),
            serde_json::json!({"peer_reachable": false, "own_log_caught_up": false, "per_table": {}})
        );
    }

    #[test]
    fn a_published_change_reaches_every_receiver() {
        let (publisher, status) = PeerSyncPublisher::new();
        let later = publisher.subscribe();
        publisher.update(|status| {
            status.peer_reachable = true;
            status.own_log_caught_up = true;
            status.per_table.insert(
                "qbit_share_ledger".into(),
                TableSyncStatus {
                    lag_rows: 3,
                    lag_seconds: 0.5,
                    last_success: None,
                },
            );
        });
        assert!(status.has_changed().unwrap());
        assert!(later.borrow().own_log_caught_up);
        assert_eq!(
            serde_json::to_value(&*status.borrow()).unwrap()["per_table"]["qbit_share_ledger"],
            serde_json::json!({"lag_rows": 3, "lag_seconds": 0.5, "last_success": null})
        );
    }
}

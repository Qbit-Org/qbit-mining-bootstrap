//! Dual-writer readiness (3.1, CONTRACT.md §3): the `/healthz` `dual_writer`
//! object, and what keeps a node from serving whatever its work. A node
//! serves only while its own share log is caught up from the peer (the peer
//! sync's startup lineage latch, decision D-8) and its writes go to its own
//! writable PostgreSQL. A broken link to the peer never withdraws it: the
//! peer's reachability and the sync lag are reported, never decided on.
use super::admission::Withdrawal;
use crate::{metrics::WriterPathLabel, node_identity::NodeIdentity, peer_sync::PeerSyncStatus};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// How long one probe of this node's database may take before it counts as
/// unanswered.
pub const WRITER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// How long the database may go unanswered before the node withdraws at
/// once, rather than after the readiness grace: long enough to ride out one
/// slow probe, short enough that a dead local PostgreSQL moves its miners to
/// the peer within seconds.
pub const WRITER_UNANSWERED_WITHDRAWAL: Duration = Duration::from_secs(4);
/// A health read within this long of the last probe reuses it, so Stratum
/// health probes cannot multiply database reads.
const WRITER_PROBE_REUSE: Duration = Duration::from_secs(1);

/// Where this frontend's writes go, as its database last answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterPath {
    /// A writable primary that is this node's own database: its
    /// `qbit_prism_node_identity` row names `PRISM_NODE_INDEX` (D-9).
    Local,
    /// A database whose identity row names the peer node.
    Remote,
    /// A database with no identity row, never personalised for dual mode.
    Unidentified,
    /// A standby, or a database whose sessions are read-only.
    ReadOnly,
    /// The probe failed or timed out.
    Unanswered,
}

impl WriterPath {
    pub fn label(self) -> WriterPathLabel {
        match self {
            Self::Local => WriterPathLabel::Local,
            Self::Remote => WriterPathLabel::Remote,
            Self::Unidentified => WriterPathLabel::Unidentified,
            Self::ReadOnly => WriterPathLabel::ReadOnly,
            Self::Unanswered => WriterPathLabel::Unanswered,
        }
    }
}

/// The last probe, and since when the database has gone unanswered.
#[derive(Clone, Copy, Debug, Default)]
struct WriterProbe {
    path: Option<WriterPath>,
    probed_at: Option<Instant>,
    unanswered_since: Option<Instant>,
}

impl WriterProbe {
    fn record(&mut self, at: Instant, path: WriterPath) {
        self.unanswered_since = match path {
            WriterPath::Unanswered => Some(self.unanswered_since.unwrap_or(at)),
            _ => None,
        };
        self.path = Some(path);
        self.probed_at = Some(at);
    }

    fn due(&self, now: Instant) -> bool {
        self.probed_at
            .is_none_or(|at| now.saturating_duration_since(at) >= WRITER_PROBE_REUSE)
    }

    /// A definitive answer that this is not the node's writable database
    /// withdraws at once; no answer does after a short streak.
    fn withdrawal(&self, now: Instant) -> Option<Withdrawal> {
        match self.path? {
            WriterPath::Local => None,
            WriterPath::Remote | WriterPath::Unidentified | WriterPath::ReadOnly => {
                Some(Withdrawal::WriterNotLocal)
            }
            WriterPath::Unanswered => self
                .unanswered_since
                .filter(|since| {
                    now.saturating_duration_since(*since) >= WRITER_UNANSWERED_WITHDRAWAL
                })
                .map(|_| Withdrawal::WriterNotLocal),
        }
    }
}

/// One evaluation, for `/healthz`, admission and the metrics.
#[derive(Clone, Debug)]
pub struct DualWriterReport {
    pub identity: NodeIdentity,
    pub own_log_caught_up: bool,
    pub writer_path: Option<WriterPath>,
    /// The fault that withdraws the node at once, if any.
    pub withdrawal: Option<Withdrawal>,
    /// The `dual_writer` object of `/healthz`.
    pub value: Value,
}

impl DualWriterReport {
    /// Whether the node may serve as far as dual-writer state goes.
    pub fn serving(&self) -> bool {
        self.own_log_caught_up && self.writer_path == Some(WriterPath::Local)
    }

    /// The health `status` that names why the node does not serve.
    pub fn status(&self) -> Option<&'static str> {
        if !self.own_log_caught_up {
            Some(Withdrawal::OwnLogBehind.as_str())
        } else if self.writer_path != Some(WriterPath::Local) {
            Some(Withdrawal::WriterNotLocal.as_str())
        } else {
            None
        }
    }
}

/// A dual-writer frontend's readiness inputs, besides the peer sync's status
/// (`Coordinator::peer_sync`, attached where the sync starts).
#[derive(Debug)]
pub struct DualWriterReadiness {
    identity: NodeIdentity,
    writer: tokio::sync::Mutex<WriterProbe>,
}

impl DualWriterReadiness {
    pub fn new(identity: NodeIdentity) -> Self {
        Self {
            identity,
            writer: tokio::sync::Mutex::new(WriterProbe::default()),
        }
    }

    /// Probe the writer if the last probe is not recent, then report.
    /// `peer_sync` is the sync's status channel; until it is attached the own
    /// log reads as not caught up, so the node does not serve.
    pub async fn report(
        &self,
        pool: &PgPool,
        peer_sync: Option<&watch::Receiver<PeerSyncStatus>>,
    ) -> DualWriterReport {
        let writer = {
            let mut probe = self.writer.lock().await;
            if probe.due(Instant::now()) {
                let path = probe_writer(pool, self.identity).await;
                probe.record(Instant::now(), path);
            }
            *probe
        };
        let peer = peer_sync
            .map(|status| status.borrow().clone())
            .unwrap_or_default();
        self.assemble(writer, peer, Instant::now())
    }

    fn assemble(
        &self,
        writer: WriterProbe,
        peer: PeerSyncStatus,
        now: Instant,
    ) -> DualWriterReport {
        let own_log_caught_up = peer.own_log_caught_up;
        let withdrawal = if own_log_caught_up {
            writer.withdrawal(now)
        } else {
            Some(Withdrawal::OwnLogBehind)
        };
        let value = json!({
            "node_index": self.identity.node,
            "carry_owner": self.identity.carry_owner,
            "own_log_caught_up": own_log_caught_up,
            "peer_sync": peer,
            "writer_path": writer.path.map(|path| path.label().as_str()),
        });
        DualWriterReport {
            identity: self.identity,
            own_log_caught_up,
            writer_path: writer.path,
            withdrawal,
            value,
        }
    }
}

/// Ask this frontend's own database whether it is this node's and can take
/// its writes. Any error or a timeout is no answer.
pub(crate) async fn probe_writer(pool: &PgPool, identity: NodeIdentity) -> WriterPath {
    let probe = sqlx::query_as::<_, (bool, bool, Option<i16>)>(
        "SELECT pg_is_in_recovery(), current_setting('transaction_read_only') = 'on', \
         (SELECT node_index FROM qbit_prism_node_identity WHERE singleton)",
    )
    .fetch_one(pool);
    match tokio::time::timeout(WRITER_PROBE_TIMEOUT, probe).await {
        Ok(Ok((in_recovery, read_only, recorded))) => {
            classify(in_recovery, read_only, recorded, identity)
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "dual-writer writer probe failed");
            WriterPath::Unanswered
        }
        Err(_) => {
            tracing::warn!(
                timeout_ms = WRITER_PROBE_TIMEOUT.as_millis() as u64,
                "dual-writer writer probe timed out"
            );
            WriterPath::Unanswered
        }
    }
}

/// The database's identity decides first, since a wrong one is a
/// configuration fault whatever its role; then whether it can write.
fn classify(
    in_recovery: bool,
    read_only: bool,
    recorded: Option<i16>,
    identity: NodeIdentity,
) -> WriterPath {
    match recorded {
        None => WriterPath::Unidentified,
        Some(index) if index != identity.node.index() => WriterPath::Remote,
        Some(_) if in_recovery || read_only => WriterPath::ReadOnly,
        Some(_) => WriterPath::Local,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{node_identity::NodeIndex, peer_sync::TableSyncStatus};

    const IDENTITY: NodeIdentity = NodeIdentity {
        node: NodeIndex::B,
        carry_owner: false,
    };

    fn probe(path: WriterPath, at: Instant) -> WriterProbe {
        let mut probe = WriterProbe::default();
        probe.record(at, path);
        probe
    }

    fn caught_up() -> PeerSyncStatus {
        PeerSyncStatus {
            peer_reachable: false,
            own_log_caught_up: true,
            per_table: Default::default(),
        }
    }

    #[test]
    fn a_node_serves_only_with_its_own_log_caught_up_and_a_local_writer() {
        let readiness = DualWriterReadiness::new(IDENTITY);
        let now = Instant::now();
        let report = readiness.assemble(probe(WriterPath::Local, now), caught_up(), now);
        assert!(report.serving());
        assert_eq!(report.status(), None);
        assert_eq!(report.withdrawal, None);
        // The peer being unreachable is reported, never a reason to withdraw.
        assert_eq!(report.value["peer_sync"]["peer_reachable"], false);

        let behind = readiness.assemble(
            probe(WriterPath::Local, now),
            PeerSyncStatus::default(),
            now,
        );
        assert!(!behind.serving());
        assert_eq!(behind.status(), Some("own-log-behind"));
        assert_eq!(behind.withdrawal, Some(Withdrawal::OwnLogBehind));

        for path in [
            WriterPath::Remote,
            WriterPath::Unidentified,
            WriterPath::ReadOnly,
        ] {
            let report = readiness.assemble(probe(path, now), caught_up(), now);
            assert!(!report.serving(), "{path:?}");
            assert_eq!(report.status(), Some("writer-not-local"), "{path:?}");
            assert_eq!(
                report.withdrawal,
                Some(Withdrawal::WriterNotLocal),
                "{path:?}"
            );
        }
        let unprobed = readiness.assemble(WriterProbe::default(), caught_up(), now);
        assert!(!unprobed.serving());
        assert_eq!(unprobed.status(), Some("writer-not-local"));
        assert_eq!(unprobed.withdrawal, None, "no probe yet is not a fault");
        assert_eq!(unprobed.value["writer_path"], Value::Null);
    }

    #[test]
    fn an_unanswered_writer_withdraws_only_after_its_streak() {
        let readiness = DualWriterReadiness::new(IDENTITY);
        let start = Instant::now();
        let mut writer = probe(WriterPath::Unanswered, start);
        let early = readiness.assemble(writer, caught_up(), start + Duration::from_secs(2));
        assert!(!early.serving());
        assert_eq!(early.withdrawal, None, "one slow probe is ridden out");
        writer.record(start + Duration::from_secs(2), WriterPath::Unanswered);
        let late = readiness.assemble(writer, caught_up(), start + WRITER_UNANSWERED_WITHDRAWAL);
        assert_eq!(
            late.withdrawal,
            Some(Withdrawal::WriterNotLocal),
            "the streak counts from the first unanswered probe"
        );
        writer.record(start + Duration::from_secs(5), WriterPath::Local);
        let answered = readiness.assemble(writer, caught_up(), start + Duration::from_secs(9));
        assert!(answered.serving());
        writer.record(start + Duration::from_secs(10), WriterPath::Unanswered);
        let again = readiness.assemble(writer, caught_up(), start + Duration::from_secs(11));
        assert_eq!(again.withdrawal, None, "an answer resets the streak");
    }

    #[test]
    fn the_health_object_carries_the_contract_fields() {
        let readiness = DualWriterReadiness::new(IDENTITY);
        let now = Instant::now();
        let mut status = caught_up();
        status.per_table.insert(
            "qbit_share_ledger".into(),
            TableSyncStatus {
                lag_rows: 7,
                lag_seconds: 0.25,
                last_success: None,
            },
        );
        let report = readiness.assemble(probe(WriterPath::Local, now), status, now);
        assert_eq!(
            report.value,
            json!({
                "node_index": 1,
                "carry_owner": false,
                "own_log_caught_up": true,
                "peer_sync": {
                    "peer_reachable": false,
                    "own_log_caught_up": true,
                    "per_table": {"qbit_share_ledger": {"lag_rows": 7, "lag_seconds": 0.25, "last_success": null}},
                },
                "writer_path": "local",
            })
        );
    }

    #[test]
    fn only_this_nodes_writable_database_is_a_local_writer() {
        let own = Some(IDENTITY.node.index());
        let peer = Some(IDENTITY.node.peer().index());
        assert_eq!(classify(false, false, own, IDENTITY), WriterPath::Local);
        assert_eq!(classify(true, false, own, IDENTITY), WriterPath::ReadOnly);
        assert_eq!(classify(false, true, own, IDENTITY), WriterPath::ReadOnly);
        assert_eq!(classify(true, true, own, IDENTITY), WriterPath::ReadOnly);
        // A wrong or missing identity is reported whatever the role.
        for (in_recovery, read_only) in [(false, false), (true, false), (false, true)] {
            assert_eq!(
                classify(in_recovery, read_only, peer, IDENTITY),
                WriterPath::Remote
            );
            assert_eq!(
                classify(in_recovery, read_only, None, IDENTITY),
                WriterPath::Unidentified
            );
        }
    }

    #[test]
    fn a_recent_probe_is_reused() {
        let now = Instant::now();
        assert!(WriterProbe::default().due(now));
        let probe = probe(WriterPath::Local, now);
        assert!(!probe.due(now + WRITER_PROBE_REUSE / 2));
        assert!(probe.due(now + WRITER_PROBE_REUSE));
    }
}

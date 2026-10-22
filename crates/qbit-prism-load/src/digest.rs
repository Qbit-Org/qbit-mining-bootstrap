//! The canonical share-identifier digest and exact reconciliation.
//!
//! The consumer (`capacity.rs`) only compares the two digests for equality and
//! cannot tell two encodings apart, so the definition lives here and is
//! reprinted in the side report.

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// SHA-256, lowercase hex, over the de-duplicated share identifiers sorted
/// bytewise, each followed by a single `\n`.
///
/// Bytewise sorting is Rust's `String`/`[u8]` ordering and PostgreSQL's
/// `ORDER BY share_id COLLATE "C"`; the database's default collation may
/// disagree, so the harness always sorts in process.
pub const DIGEST_DEFINITION: &str = "sha256(lowercase hex) over the de-duplicated set of share \
identifiers sorted by UTF-8 byte order, each identifier followed by one 0x0a byte";

pub fn share_id_digest<'a>(ids: impl IntoIterator<Item = &'a str>) -> String {
    let sorted: BTreeSet<&str> = ids.into_iter().collect();
    let mut hasher = Sha256::new();
    for id in sorted {
        hasher.update(id.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

/// The exact reconciliation of one phase, or of the whole run.
#[derive(Clone, Debug, Default)]
pub struct Reconciliation {
    /// Offered set O: shares the harness believes were valid when offered.
    pub offered: BTreeSet<String>,
    /// Acknowledged set A.
    pub acknowledged: BTreeSet<String>,
    /// Committed set C: accepted ledger rows written by this run's frontends
    /// under this run's username prefix, restricted to O.
    pub committed: BTreeSet<String>,
    /// A minus the database.
    pub missing: BTreeSet<String>,
    /// Run-prefixed database rows that are in no phase's A.
    pub unexpected: BTreeSet<String>,
}

impl Reconciliation {
    pub fn acknowledged_digest(&self) -> String {
        share_id_digest(self.acknowledged.iter().map(String::as_str))
    }
    pub fn committed_digest(&self) -> String {
        share_id_digest(self.committed.iter().map(String::as_str))
    }
    pub fn is_exact(&self) -> bool {
        self.missing.is_empty() && self.unexpected.is_empty()
    }
}

/// Build one phase's reconciliation from the offered and acknowledged sets and
/// the run's committed rows.
///
/// `missing` is `A \ DB` over the whole run's committed rows, so a share the
/// database attributed to a neighbouring phase is not reported as missing.
pub fn reconcile(
    offered: BTreeSet<String>,
    acknowledged: BTreeSet<String>,
    all_committed: &BTreeSet<String>,
) -> Reconciliation {
    let committed = offered
        .iter()
        .filter(|id| all_committed.contains(*id))
        .cloned()
        .collect::<BTreeSet<_>>();
    let missing = acknowledged
        .iter()
        .filter(|id| !all_committed.contains(*id))
        .cloned()
        .collect::<BTreeSet<_>>();
    Reconciliation {
        offered,
        acknowledged,
        committed,
        missing,
        unexpected: BTreeSet::new(),
    }
}

/// Attribute every run-prefixed database row that no phase acknowledged.
/// Rows offered inside a phase are attributed to it; the rest fall to
/// "outside phases".
#[derive(Clone, Debug, Default)]
pub struct UnexpectedAttribution {
    pub by_phase: Vec<(String, Vec<String>)>,
    pub outside_phases: Vec<String>,
}

pub fn attribute_unexpected(
    all_committed: &BTreeSet<String>,
    phases: &[(String, &Reconciliation)],
) -> UnexpectedAttribution {
    let acknowledged_anywhere: BTreeSet<&String> = phases
        .iter()
        .flat_map(|(_, phase)| phase.acknowledged.iter())
        .collect();
    let mut attribution = UnexpectedAttribution::default();
    let mut claimed: BTreeSet<&String> = BTreeSet::new();
    for (name, phase) in phases {
        let rows: Vec<String> = all_committed
            .iter()
            .filter(|id| !acknowledged_anywhere.contains(*id) && phase.offered.contains(*id))
            .cloned()
            .collect();
        for id in &rows {
            if let Some(found) = all_committed.get(id) {
                claimed.insert(found);
            }
        }
        if !rows.is_empty() {
            attribution.by_phase.push((name.clone(), rows));
        }
    }
    attribution.outside_phases = all_committed
        .iter()
        .filter(|id| !acknowledged_anywhere.contains(*id) && !claimed.contains(*id))
        .cloned()
        .collect();
    attribution
}

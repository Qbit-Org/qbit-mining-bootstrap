//! Classification of Stratum rejections and of frontend log lines that say a
//! size is refused.
//!
//! The wire forms come from `crates/qbit-prism-server/src/stratum.rs` and
//! `crates/qbit-prism-server/src/coordinator.rs`; the classifier keys on the
//! `(code, reason_id, message)` triple exactly as those files emit it.

use serde::{Deserialize, Serialize};

/// The server's answer to a submit whose share it already holds. Expected
/// for a mid-flight re-offer whose original had committed before the kill,
/// and a harness bug anywhere else.
pub const DUPLICATE_SHARE: &str = "duplicate-share";

/// Payout-revision and tip-rebuild rejections. Both share `reason_id`
/// `stale-job` and code 21 with the retention cases, so only the message text
/// separates them (`coordinator.rs`, `submit`).
pub const NEW_PAYOUT_WORK_PENDING: &str = "new payout work is pending";
pub const NEW_TIP_WORK_PENDING: &str = "new tip work is pending";

/// What a rejection means for the run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionClass {
    /// Transient and expected after a tip change or a landed block. The share
    /// was never persisted, so it does not enter reconciliation.
    Expected,
    /// A defect in the harness: it offered work the server had every right to
    /// refuse. The run must exit non-zero.
    HarnessBug,
    /// The backend could not answer. Neither expected nor a harness defect;
    /// recorded and surfaced, never silently folded into either bucket.
    Backend,
    /// A rejection this classifier does not recognise. Treated as unknown, not
    /// as success (EP-OBSERVABILITY).
    Unknown,
}

impl RejectionClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Expected => "expected",
            Self::HarnessBug => "harness-bug",
            Self::Backend => "backend",
            Self::Unknown => "unknown",
        }
    }
}

/// One rejection as the client saw it.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Rejection {
    pub code: i64,
    pub reason_id: Option<String>,
    pub message: String,
}

/// Classify a rejection by `(code, reason_id, message)`.
///
/// The harness-bug classes are the ones that can only happen if the client
/// built a bad submit: `low-difficulty`, `malformed-submit`, `duplicate-share`,
/// every `invalid-*`, and `unauthorized-worker`.
pub fn classify(rejection: &Rejection) -> RejectionClass {
    let reason = rejection.reason_id.as_deref().unwrap_or("");
    if reason.starts_with("invalid-") {
        return RejectionClass::HarnessBug;
    }
    match reason {
        "low-difficulty" | "malformed-submit" | DUPLICATE_SHARE | "unauthorized-worker" => {
            RejectionClass::HarnessBug
        }
        "stale-job" => {
            // Every `stale-job` is a race the server is entitled to lose: the
            // tip moved, the payout revision moved, or the job aged past
            // retention. This includes a typed pre-COMMIT refusal proving
            // the original publication lease expired or was replaced.
            // None of them persist a share.
            RejectionClass::Expected
        }
        "unknown-job" | "pool-closed" => RejectionClass::Expected,
        // Older producers used ledger-confirmation-failed for every closed
        // gate. Neither that reason nor its generic gate-closed message
        // proves stale work: deadline, lock and readiness failures stay here.
        "backend-rpc-unavailable"
        | "ledger-confirmation-failed"
        | "ledger-outcome-unknown"
        | "internal-error" => RejectionClass::Backend,
        "" => {
            // `too many connections for username` carries code 20 and no
            // reason_id (`stratum.rs`, authorize). So does the session's
            // unknown-job budget refusal (`stratum.rs`, submit): a miss on a
            // job ID the session no longer holds costs a ledger lookup, and a
            // session that spends its budget is refused before the lookup.
            if rejection.message.contains("too many connections")
                || is_unknown_job_budget(rejection)
            {
                RejectionClass::Expected
            } else {
                RejectionClass::Unknown
            }
        }
        _ => RejectionClass::Unknown,
    }
}

/// The refusal `stratum.rs` answers, with code 20 and no reason_id, once a
/// session has spent its unknown-job budget
/// (`ConnectionRefusalReason::UnknownJobBudget`). It is a rate limit on
/// lookups, not a verdict on the work: the job may or may not have been
/// retired, so no landing owns it.
pub const UNKNOWN_JOB_BUDGET: &str = "too many unknown job submissions";

pub fn is_unknown_job_budget(rejection: &Rejection) -> bool {
    rejection.code == 20 && rejection.reason_id.is_none() && rejection.message == UNKNOWN_JOB_BUDGET
}

/// A definite failure to record credit on current producers, including a
/// deadline that won before COMMIT, or a gate with no proven stale cause.
///
/// Historical producers could answer this while COMMIT was still in flight.
/// Keep the reconciliation predicate for those reports; current uncertain
/// COMMIT outcomes use `ledger-outcome-unknown` instead.
pub const LEDGER_CONFIRMATION_FAILED: &str = "ledger-confirmation-failed";
pub const NOT_CONFIRMED_BY_DATABASE: &str = "share was not confirmed by the database";

pub fn is_duplicate_share(rejection: &Rejection) -> bool {
    rejection.reason_id.as_deref() == Some(DUPLICATE_SHARE)
}

pub fn is_confirmation_failure(rejection: &Rejection) -> bool {
    rejection.reason_id.as_deref() == Some(LEDGER_CONFIRMATION_FAILED)
}

/// The rejection #333 introduces for a COMMIT that still has no reply once the
/// server has waited past `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` and its grace
/// window (Qbit-Org/qbit-mining-bootstrap#324).
///
/// It is deliberately non-committal: the server is saying it does not know
/// whether the append landed, not that it did not. So, like
/// `ledger-confirmation-failed`, it can be followed by the share appearing in
/// PostgreSQL, and the harness must not read that as a durability loss.
pub const LEDGER_OUTCOME_UNKNOWN: &str = "ledger-outcome-unknown";

pub fn is_outcome_unknown(rejection: &Rejection) -> bool {
    rejection.reason_id.as_deref() == Some(LEDGER_OUTCOME_UNKNOWN)
}

/// True for the two payout/tip rebuild messages the contract calls out as
/// expected after tips and blocks.
pub fn is_rebuild_pending(rejection: &Rejection) -> bool {
    rejection.reason_id.as_deref() == Some("stale-job")
        && (rejection.message == NEW_PAYOUT_WORK_PENDING
            || rejection.message == NEW_TIP_WORK_PENDING)
}

/// The one message the current server attaches to a `stale-job` or
/// `unknown-job` refusal of retired work: `Coordinator::stale_job` in
/// `coordinator/miner_submit.rs` (a stale parent or a superseded payout
/// revision, whose internal cause goes only to
/// `qbit_prism_stale_job_rejections_total{cause}`), the lease checks there and
/// in `coordinator/tip_observation.rs`, and the job lookup and resume expiry in
/// `stratum.rs` (`unknown-job` for a job ID the session no longer holds).
pub const STALE_JOB: &str = "stale job";
/// The `stale-job` refusal for a job whose CTV fee fell below the relay floor
/// (`coordinator/miner_submit.rs`). A fee change, not a landing, retired it.
pub const FEE_BELOW_RELAY_FLOOR: &str = "job CTV fee is below the current relay floor";

/// What a rejection is to the dense-cadence landing whose span it falls in
/// (Qbit-Org/qbit-mining-bootstrap#480).
///
/// A landing owns the refusals of work it retired: a share proven on a job
/// whose parent or payout revision the landing replaced. The wire says so by
/// `reason_id` together with a message the harness knows, so a message it
/// does not know is [`LandingCost::Unrecognised`], never silently owned or
/// silently dropped (EP-OBSERVABILITY).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LandingCost {
    /// `stale-job` with `new tip work is pending` (older producers).
    TipPending,
    /// `stale-job` with `new payout work is pending` (older producers).
    PayoutPending,
    /// `stale-job` with `stale job`: the current server's answer for a share
    /// on a retired parent or payout revision.
    StaleJob,
    /// `unknown-job` with `stale job`: the job ID had already left the
    /// session's job list, which the rebuild's `clean_jobs` notify retires.
    UnknownJob,
    /// Recognised, and not the landing's: a backend refusal
    /// (`backend-rpc-unavailable`, `ledger-confirmation-failed`,
    /// `ledger-outcome-unknown`, `internal-error`), a fee-floor `stale-job`,
    /// `pool-closed`, the reason-less unknown-job budget refusal
    /// ([`UNKNOWN_JOB_BUDGET`]), or a harness-bug class. Reported beside the landing's
    /// cost, never inside it.
    NotOwned,
    /// A `stale-job` or `unknown-job` message the harness does not know, or a
    /// reason the classifier does not know: whether the landing owns it is
    /// unknown.
    Unrecognised,
}

impl LandingCost {
    /// True when the landing whose span holds the rejection owns it.
    pub fn owned(self) -> bool {
        matches!(
            self,
            Self::TipPending | Self::PayoutPending | Self::StaleJob | Self::UnknownJob
        )
    }
}

/// Classify a rejection for dense-cadence attribution.
pub fn landing_cost(rejection: &Rejection) -> LandingCost {
    match (rejection.reason_id.as_deref(), rejection.message.as_str()) {
        (Some("stale-job"), NEW_TIP_WORK_PENDING) => LandingCost::TipPending,
        (Some("stale-job"), NEW_PAYOUT_WORK_PENDING) => LandingCost::PayoutPending,
        (Some("stale-job"), STALE_JOB) => LandingCost::StaleJob,
        (Some("stale-job"), FEE_BELOW_RELAY_FLOOR) => LandingCost::NotOwned,
        (Some("unknown-job"), STALE_JOB) => LandingCost::UnknownJob,
        (Some("stale-job" | "unknown-job"), _) => LandingCost::Unrecognised,
        _ => match classify(rejection) {
            RejectionClass::Unknown => LandingCost::Unrecognised,
            RejectionClass::Expected | RejectionClass::HarnessBug | RejectionClass::Backend => {
                LandingCost::NotOwned
            }
        },
    }
}

/// A frontend log line that says this window size cannot be served.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockedLog {
    /// Which refusal this is.
    pub kind: BlockedKind,
    /// The whole log line, verbatim.
    pub line: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockedKind {
    /// PostgreSQL refused a JSONB container over its 268,435,455-byte ceiling
    /// (issue #273). The error text names neither the table nor the column.
    JsonbCeiling,
    /// The refresh loop could not publish work for some other reason.
    RefreshDeferred,
    /// Prepared-job persistence or preparation was deferred.
    JobDeferred,
}

/// The refusal messages the native runtime actually logs. Sources:
/// `coordinator.rs` `refresh_loop` logs `template refresh deferred`,
/// `deliver_job` logs `job preparation deferred`, and `save_job` failures log
/// `job persistence deferred`. The JSONB ceiling arrives inside the `error=`
/// field of one of those, worded by PostgreSQL.
pub const REFRESH_DEFERRED: &str = "template refresh deferred";
pub const JOB_PREPARATION_DEFERRED: &str = "job preparation deferred";
pub const JOB_PERSISTENCE_DEFERRED: &str = "job persistence deferred";

/// Classify one frontend log line. `None` when the line is not a refusal.
///
/// The JSONB ceiling test mirrors the #264 gate's own `ceiling_rejection`:
/// the text mentions `jsonb` and `exceeds the maximum of`.
pub fn classify_log_line(line: &str) -> Option<BlockedLog> {
    let lowered = line.to_ascii_lowercase();
    let ceiling = lowered.contains("jsonb") && lowered.contains("exceeds the maximum of");
    if ceiling {
        return Some(BlockedLog {
            kind: BlockedKind::JsonbCeiling,
            line: line.to_owned(),
        });
    }
    if line.contains(REFRESH_DEFERRED) {
        return Some(BlockedLog {
            kind: BlockedKind::RefreshDeferred,
            line: line.to_owned(),
        });
    }
    if line.contains(JOB_PREPARATION_DEFERRED) || line.contains(JOB_PERSISTENCE_DEFERRED) {
        return Some(BlockedLog {
            kind: BlockedKind::JobDeferred,
            line: line.to_owned(),
        });
    }
    None
}

/// A refusal that means the size itself is blocked, as opposed to a transient
/// deferral that the next refresh clears.
pub fn is_hard_block(log: &BlockedLog) -> bool {
    log.kind == BlockedKind::JsonbCeiling
}

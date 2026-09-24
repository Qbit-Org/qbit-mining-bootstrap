//! Durable coordination for interchangeable PRISM servers.
//!
//! Proof-of-work verification is concurrent. Only assignment of the canonical
//! share order and capture of a job's accounting boundary are serialized. The
//! locks are transaction scoped, so disconnects and database failover release
//! ownership without an application lease election.
use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Utc};
use qbit_prism::{AcceptedShare, AuditBundle, CarryForwardBalance};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Postgres, Row, Transaction,
};
use uuid::Uuid;

mod append_admission;
mod blocks;
pub use blocks::{BlockObservation, FanoutClaim, PoolBlock};
/// Share ledger retention: the operator commands behind `share-archive`.
pub mod archive;
mod audit;
pub use audit::{
    audit_canonical_bytes, audit_canonical_bytes_admitted, audit_completeness,
    decode_canonical_audit_body, materialize_audit_row, AuditCompleteness, AuditReader,
};
mod candidates;
use candidates::prepare_candidate_observed;
pub use candidates::{
    adoption_evidence, authenticate_landed_audit, build_claim_parts,
    coinbase_witness_reserved_value, header_bits_hex, Candidate, CandidateClaim, CandidateCtv,
    CandidateState, ClaimLifecycle, ClaimParts, LandedAudit, OfferOutcome, OfferRecord,
    RecoveryClaim, RecoveryReader, RecoveryRow, SignerKeys, ADOPTED_OFFER_REPLY_PREFIX,
    LANDING_FAILED_REASON_PREFIX, ORPHANED_STATE, SIDE_CHAIN_REPLIES,
};
mod connect;
use connect::{require_revision, writable};
pub use connect::{SessionAllocationExhausted, SessionId};
mod difficulty;
mod fanout;
mod fatal_state;
mod instances;
mod policy_transition;
mod signing_transition;
pub(crate) use instances::{live_instances, unavailable_live_instances, LiveInstancesReport};
pub use instances::{HeartbeatHealth, HeartbeatStatus};
mod jobs;
pub use jobs::{
    BlobPruneCursor, BlobPruneResult, CompactBatchAttempt, CompactDependency, CompactIssuedJob,
    CompactPrepared, CompactRepair, IssuedJobSave, PreparedAuditHashes, PreparedDependency,
    PreparedTemplate, StoredCompactPrepared,
};
mod migration;
pub use migration::{
    schema_version_list, MigrationSource, SourceState, SourceStateRule, NOT_VALID_EXEMPT,
    REQUIRED_SCHEMA_VERSIONS, SOURCE_STATES,
};
mod window;
pub use difficulty::WorkerDifficulty;
pub(crate) use window::blocking_drop::{BlockingDrop, ReadAdmission};
pub use window::CommitGateClosed;
pub use window::{
    probe_share_rows, put_balance_snapshot, read_range_paged, AppendResult, BalanceSource,
    ChainObservationState, ChainTransition, PayoutState, ShareRange, Snapshot, Window, WindowError,
    WindowRef,
};
use window::{read_prior_balances, share_from_row};
pub(crate) use window::{
    AcquisitionReport, ChainObservationBehind, ChainObservationRetry, LeafWitness, RefreshProbe,
    RetainedShares, SnapshotCapture,
};

const MIGRATION_LOCK: i64 = 0x505249534d000001;
const ORDER_LOCK: i64 = 0x505249534d000002;
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
/// The class of the two-key session-level advisory lock the online
/// migration runner takes on its own connection, outside any transaction
/// (`migration::apply_online_migration`), so two starting frontends never
/// build the same index twice. The second key is the ledger's schema: the
/// migration's DDL creates there, and a database hosting several ledger
/// schemas builds each on its own.
const ONLINE_DDL_LOCK_CLASS: i32 = 0x5052_4953;
const SELECT_SHARE: &str = "SELECT share_seq,share_id,miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,share_difficulty::text AS difficulty,network_difficulty::text AS network_difficulty,template_height,job_id,job_issued_at,accepted_at,ntime,credit_policy FROM qbit_share_ledger";

#[derive(Clone)]
pub struct Ledger {
    pub pool: PgPool,
    pub instance_id: String,
    session_owner: std::sync::Arc<connect::SessionOwner>,
    /// Native wait telemetry, when the process has a registry to record into.
    /// Without a handle nothing is recorded and behaviour is identical, so
    /// tests keep using [`Ledger::connect`] and the database-only commands
    /// [`Ledger::connect_tool`] without one.
    metrics: Option<std::sync::Arc<crate::metrics::Metrics>>,
    /// The cluster fingerprint [`Ledger::configure`] pinned or verified, read
    /// back through [`Ledger::config_fingerprint`]. Shared across clones, so
    /// every handle to one frontend's ledger sees the same pinned value: the
    /// writer fence re-reads `qbit_prism_cluster.config_fingerprint` `FOR
    /// SHARE` in its own transaction and compares it against this.
    config_fingerprint: std::sync::Arc<std::sync::OnceLock<String>>,
    #[cfg(test)]
    pub(crate) compact_decode_hook: std::sync::Arc<std::sync::Mutex<Option<CompactDecodeHook>>>,
    #[cfg(test)]
    pub(crate) snapshot_decode_hook: std::sync::Arc<std::sync::Mutex<Option<SnapshotDecodeHook>>>,
}

#[cfg(test)]
type CompactDecodeHook = std::sync::Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type SnapshotDecodeHook = std::sync::Arc<dyn Fn(&'static str) + Send + Sync>;

#[cfg(test)]
#[path = "../tests/support/ledger_execution_proxy.rs"]
mod execution_proxy;

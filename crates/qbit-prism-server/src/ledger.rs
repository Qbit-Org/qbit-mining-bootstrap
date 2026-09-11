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
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Postgres, Row, Transaction,
};
use uuid::Uuid;

mod blocks;
pub use blocks::{BlockObservation, FanoutClaim, PoolBlock};
mod audit;
pub use audit::{audit_canonical_bytes, materialize_audit_row};
mod candidates;
use candidates::persist_candidate;
pub use candidates::{Candidate, CandidateClaim};
mod connect;
use connect::{lock, require_revision, writable};
mod difficulty;
mod fanout;
mod jobs;
pub use jobs::{IssuedJobSave, PreparedDependency};
mod migration;
mod window;
pub use difficulty::WorkerDifficulty;
use window::{read_prior_balances, share_from_row};
pub use window::{AppendResult, Snapshot};

const MIGRATION_LOCK: i64 = 0x505249534d000001;
const ORDER_LOCK: i64 = 0x505249534d000002;
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
const SELECT_SHARE: &str = "SELECT share_seq,share_id,miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,share_difficulty::text AS difficulty,network_difficulty::text AS network_difficulty,template_height,job_id,job_issued_at,accepted_at,ntime,credit_policy FROM qbit_share_ledger";

#[derive(Clone)]
pub struct Ledger {
    pub pool: PgPool,
    pub instance_id: String,
}

use crate::{
    codec,
    config::Config,
    ledger::{
        authenticate_landed_audit, build_claim_parts, header_bits_hex, BalanceSource,
        BlockObservation, Candidate, CandidateClaim, CandidateCtv, CandidateState, ClaimParts,
        HeartbeatHealth, Ledger, OfferOutcome, SignerKeys, Snapshot, Window, WindowError,
        WindowRef, ORPHANED_STATE,
    },
    rpc::Rpc,
    stratum::{MiningBackend, MiningJob, StaleGrace, StratumError, Worker},
};
use anyhow::{ensure, Context, Result};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use qbit_pool_builder::ManifestSigningKey;
#[cfg(test)]
use qbit_prism::AuditBundle;
use qbit_prism::{AcceptedShare, FanoutFeeRatePolicy, FoundBlock, PayoutPolicy};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{watch, Mutex, Notify, RwLock, Semaphore};

mod bundle_build;
mod chain_observation;
mod compact_resume;
mod compact_runtime;
mod issued_batcher;
mod miner_submit;
mod prepared_storage;
mod publication_authority;
mod refresh_window;
mod submit_ledger;
// The reviewed authority facade retains legacy entrypoints exercised by
// compatibility fixtures; activation uses the opaque issuance-proof API.
mod tip_observation;
mod work_ledger;
pub use compact_runtime::{PreparedBundle, PreparedSnapshot};
pub use tip_observation::{IssuanceAuthority, TipState};

pub struct JobContext {
    pub prepared: Arc<Prepared>,
    pub worker: Worker,
    pub bundle: Arc<PreparedBundle>,
    /// The synthetic share a per-worker bootstrap build fabricated for an
    /// empty window, verbatim. It exists only here and in the bundle, and a
    /// candidate found on this job stores it inline: its fields come from the
    /// worker and the template the issuing frontend saw, which a claiming
    /// frontend cannot re-derive.
    pub bootstrap_share: Option<AcceptedShare>,
    /// Original admission for Coordinator-built/recovered work. Internal
    /// contexts not built by Coordinator start their proof at persistence.
    pub issuance_authority: Option<Arc<IssuanceAuthority>>,
}

/// The builder inputs a job was built with, other than the window, captured
/// where `build_bundle` reads them and carried beside the bundle. A candidate
/// stores exactly these, so a rebuild reads no local configuration for any
/// field that reaches the signed bundle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleInputs {
    pub payout_policy: PayoutPolicy,
    /// `Some` exactly when the CTV builder was used; its presence selects
    /// the builder at a rebuild in place of `config.ctv_enabled`.
    /// The key is required: explicit null means no CTV; omission is corruption.
    #[serde(deserialize_with = "Option::deserialize")]
    pub ctv: Option<CandidateCtv>,
    /// The public keys of the seeds the bundle was signed with.
    pub signer_keys: SignerKeys,
    pub audit_builder_version: u16,
}

impl BundleInputs {
    fn capture(config: &Config, fee: Option<FanoutFeeRatePolicy>) -> Result<Self> {
        Ok(Self {
            payout_policy: config.payout_policy.clone(),
            ctv: config.ctv_enabled.then_some(CandidateCtv {
                direct_floor_sats: config.ctv_direct_floor,
                settlement_config: config.ctv_config,
                fanout_fee_policy: fee,
            }),
            signer_keys: local_signer_keys(config)?,
            audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        })
    }

    /// Check the policy and actual signer identities recorded in the bundle.
    /// Resume also compares the persisted inputs, since a bundle alone cannot
    /// prove its CTV configuration or builder version.
    #[cfg(test)]
    fn describes(&self, bundle: &AuditBundle) -> bool {
        bundle.payout_policy == self.payout_policy
            && bundle
                .signed_coinbase_manifest
                .signature
                .public_key_hex
                .eq_ignore_ascii_case(&self.signer_keys.manifest_key_hex)
            && bundle
                .ledger_window_attestation
                .signature
                .public_key_hex
                .eq_ignore_ascii_case(&self.signer_keys.ledger_key_hex)
    }
}

/// The public keys of this frontend's signing seeds.
fn local_signer_keys(config: &Config) -> Result<SignerKeys> {
    Ok(SignerKeys::of(
        &ManifestSigningKey::from_seed_hex(&config.manifest_seed)?,
        &ManifestSigningKey::from_seed_hex(&config.ledger_seed)?,
    ))
}

pub struct Prepared {
    reservation: Arc<compact_runtime::PreparedReservation>,
    repair: Arc<Mutex<()>>,
    #[cfg(test)]
    repair_probe: std::sync::Mutex<Option<Arc<prepared_storage::RepairProbe>>>,
    pub template: Value,
    pub snapshot: Arc<PreparedSnapshot>,
    /// The reference for `snapshot`'s window, computed once per non-cached
    /// refresh and carried with the work instead of being re-derived. Submit
    /// clones it into the candidate, so a found block never re-digests the
    /// window on the share path.
    pub window: WindowRef,
    /// What `bundle` was built with, other than the window. A per-worker
    /// bootstrap build uses the same inputs, and a candidate copies them.
    /// This nonoptional view copies the original build's or validated stored
    /// inputs; callers never need to handle the legacy storage-only absence.
    pub inputs: BundleInputs,
    pub bundle: Option<Arc<PreparedBundle>>,
    pub base_wire: Option<codec::Job>,
    pub storage_key: String,
    pub fee: Option<FanoutFeeRatePolicy>,
    pub fingerprint: String,
    pub generation: u64,
    pub created: Instant,
    pub parent_of_tip: String,
}

#[cfg(test)]
#[derive(Serialize, Deserialize)]
struct StoredPrepared {
    template: Value,
    snapshot: Arc<Snapshot>,
    bundle: Option<Arc<AuditBundle>>,
    /// Absent only in legacy inline rows, which cannot prove compatibility.
    /// A present value must decode completely; null is not a legacy record.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "StoredPrepared::deserialize_inputs"
    )]
    inputs: Option<BundleInputs>,
    fee: Option<FanoutFeeRatePolicy>,
    fingerprint: String,
    generation: u64,
    parent_of_tip: String,
    coinbase_suffix: String,
}

#[cfg(test)]
impl StoredPrepared {
    /// Legacy rows cannot prove their CTV inputs or builder version. A resumed
    /// candidate (including bootstrap work) must use the exact issued inputs,
    /// and any stored bundle must agree with their policy and signer keys.
    #[cfg(test)]
    fn issued_inputs(&self, config: &Config) -> Result<Option<&BundleInputs>> {
        let Some(inputs) = self.inputs.as_ref() else {
            return Ok(None);
        };
        if inputs != &BundleInputs::capture(config, self.fee)?
            || self
                .bundle
                .as_ref()
                .is_some_and(|bundle| !inputs.describes(bundle))
        {
            return Ok(None);
        }
        Ok(Some(inputs))
    }

    fn deserialize_inputs<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Option<BundleInputs>, D::Error> {
        BundleInputs::deserialize(deserializer).map(Some)
    }
}

#[derive(Serialize, Deserialize)]
struct StoredJob {
    prepared_key: String,
    worker: Worker,
    extranonce1: String,
    extranonce2_size: usize,
    share_target_hex: String,
    share_difficulty: f64,
    version_mask: u32,
    expires_at_ms: i64,
}

#[derive(Default)]
struct RefreshState {
    observation: chain_observation::ChainObservation,
    cached_window: Option<prepared_storage::compact::CompactOwner<refresh_window::CachedWindow>>,
}

pub struct Coordinator {
    pub config: Arc<Config>,
    pub ledger: Arc<Ledger>,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub rpc: Rpc,
    pub prepared: Arc<RwLock<Option<Arc<Prepared>>>>,
    pub refresh: watch::Sender<u64>,
    pub wake: Notify,
    pub accepted: AtomicU64,
    pub rejected: AtomicU64,
    pub blocks: AtomicU64,
    readiness: Arc<RwLock<ReadinessState>>,
    pub observed_tip: Arc<RwLock<TipState>>,
    submit_ledger: Arc<dyn submit_ledger::SubmitLedger>,
    work_ledger: Arc<dyn work_ledger::WorkLedger>,
    issued_batcher: issued_batcher::IssuedBatcher,
    pub last_error: RwLock<Option<String>>,
    /// The builder admission permits, `PRISM_JOB_BUILD_EXECUTOR_WORKERS` of
    /// them. Public so a test can saturate build capacity and prove the offer
    /// never waits for it.
    pub build_slots: Arc<Semaphore>,
    /// Bounds how many `Ledger::read_window` calls hold a pool connection at
    /// once. `build_slots` alone does not: `PRISM_JOB_BUILD_EXECUTOR_WORKERS`
    /// may exceed `PRISM_DATABASE_MAX_CONNECTIONS`, and the pool is shared with
    /// share appends and the candidate-lease heartbeat, which must not wait out
    /// the 15 s acquire timeout behind a multi-page read.
    window_reads: Arc<Semaphore>,
    // Both states survive cancelled refreshes under the same serialization:
    // retiring cached inputs must not reset a consumed node transition.
    refresh_lock: Mutex<RefreshState>,
    resume_flights: compact_resume::ResumeFlights,
    identities: Mutex<HashMap<String, (Worker, Instant)>>,
    chain_cache: Mutex<Option<ChainCache>>,
    /// The ledger sessions' effective `statement_timeout`, `None` when
    /// disabled. A COMMIT that ran this long may be a cancelled synchronous
    /// replication wait that committed only locally.
    statement_timeout: Option<Duration>,
    /// A test seam between the offer reservation and the token fence that
    /// precedes the `submitblock` call, so a test can take the row away in
    /// exactly the window the fence guards.
    #[cfg(test)]
    offer_probe: std::sync::Mutex<Option<Arc<OfferProbe>>>,
}

/// See `Coordinator::offer_probe`.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct OfferProbe {
    pub(crate) entered: Notify,
    pub(crate) release: Notify,
}

/// The wall clock as UNIX milliseconds. Every proof-to-first-offer sample is
/// the difference of two such readings, one taken by the enqueuing frontend
/// and one by the offering frontend; neither is the ledger clock, and the
/// two hosts' clocks may disagree, which the observer treats as skew.
pub(crate) fn unix_ms_now() -> Result<i64> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("wall clock is before the UNIX epoch")?;
    i64::try_from(elapsed.as_millis()).context("wall clock overflow")
}

/// Classify the one `submitblock` reply. `null` is acceptance on some chain,
/// a string is the node's rejection reason, and anything else, a transport
/// failure or a timeout included, leaves delivery unknown: the block may or
/// may not have reached the node, and the row is reconciled against the
/// chain rather than offered again.
fn classify_offer(result: &Result<Value>) -> (OfferOutcome, Option<String>) {
    match result {
        Ok(Value::Null) => (OfferOutcome::Accepted, None),
        Ok(Value::String(reason)) => (OfferOutcome::Rejected, Some(reason.clone())),
        Ok(other) => (
            OfferOutcome::Unknown,
            Some(format!("unexpected submitblock reply {other}")),
        ),
        Err(error) => (OfferOutcome::Unknown, Some(format!("{error:#}"))),
    }
}

fn describe_offer(outcome: OfferOutcome, reply: Option<&str>) -> String {
    match (outcome, reply) {
        (OfferOutcome::Accepted, _) => "node accepted the offer".into(),
        (OfferOutcome::Rejected, reply) => {
            format!("node rejected the offer: {}", reply.unwrap_or("no reason"))
        }
        (OfferOutcome::Unknown, reply) => format!(
            "offer outcome unknown: {}",
            reply.unwrap_or("no reply was recorded")
        ),
    }
}

#[derive(Default)]
struct ReadinessState {
    last_poll: Option<Instant>,
    generation: u64,
    ctv_fee_floor: Option<u64>,
}

struct ChainCache {
    tip: String,
    height: u64,
    observations: HashMap<String, bool>,
}

/// One coherent observation of a candidate against the chain, see
/// [`Coordinator::observe_candidate`].
struct CandidateObservation {
    /// The candidate's own block is the active block at its height.
    active: bool,
    /// The payout revision the ledger recorded for this tip view; the fence
    /// every settlement written from this observation revalidates.
    revision: i64,
    tip: String,
    tip_height: u64,
    /// The active block at the candidate's height, `None` while the tip is
    /// below it.
    at_height: Option<String>,
}

#[derive(Clone, Copy)]
struct CandidateLease {
    seconds: i64,
    interval: Duration,
    timeout: Duration,
    /// The one deadline around a claim's window read plus its rebuild. The
    /// steps after it keep their own deadlines; the lease is not one, its
    /// heartbeat renews it for as long as processing runs.
    rebuild_deadline: Duration,
}

const CANDIDATE_LEASE: CandidateLease = CandidateLease {
    seconds: 120,
    interval: Duration::from_secs(30),
    timeout: Duration::from_secs(5),
    rebuild_deadline: Duration::from_secs(60),
};

/// Why a claim's window read or rebuild did not produce parts. Every variant
/// is recoverable and none abandons a candidate: after the offer the attempt
/// settles in `reconciliation` with the reason and is retried without any
/// RPC; before the offer (an active block found by the pre-offer probe) the
/// row is rescheduled through `retry_candidate`.
#[derive(Debug)]
enum RebuildFailure {
    /// Fail this attempt with a reason; a later attempt may succeed.
    Retry(String),
    /// Under a `Current` read, the current balances are not the reference's,
    /// so the current set is not the one the block's coinbase commits to.
    PriorBalancesChanged,
    /// Under an `AsIssued` read, no snapshot row holds the reference's set.
    /// The caller may fall back to a `Current` read, which proves the digest
    /// itself before it returns any balances.
    BalanceSnapshotMissing,
}

/// Map a window read error to the claim's action. A database error is not
/// mapped: it propagates to `submit_loop`, whose retry releases the claim.
fn classify_window_error(error: WindowError) -> Result<RebuildFailure> {
    Ok(match error {
        WindowError::PriorBalancesChanged { .. } => RebuildFailure::PriorBalancesChanged,
        WindowError::BalanceSnapshotMissing { .. } => RebuildFailure::BalanceSnapshotMissing,
        WindowError::Incomplete { expected, got } => RebuildFailure::Retry(format!(
            "window range incomplete: expected {expected} shares, read {got}; rows pruned or missing, or a different predicate (#268 owns recovery)"
        )),
        WindowError::SnapshotDigestMismatch { expected, actual } => {
            RebuildFailure::Retry(format!(
                "window snapshot digest mismatch: reference {} read {}; corruption or a reference built from different bytes",
                hex::encode(expected),
                hex::encode(actual)
            ))
        }
        WindowError::Decode(error) => {
            RebuildFailure::Retry(format!("window decode error: {error:#}; corruption"))
        }
        // A cancelled or panicked blocking hand-off found nothing wrong with
        // any row or digest: retryable, with its own alert, and never the
        // corruption or abandon path.
        WindowError::TaskFailed(error) => RebuildFailure::Retry(format!(
            "window read blocking task cancelled or failed: {error}; retrying, not corruption"
        )),
        WindowError::Database(error) => return Err(error.into()),
    })
}

/// Release a rebuilt window off the runtime. Dropping a 400,000-share window
/// on a runtime worker would stall candidate-lease heartbeats and share
/// processing, so every path, early returns included, hands it to a blocking
/// thread instead.
struct OffRuntime<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> OffRuntime<T> {
    fn new(value: T) -> Self {
        Self(Some(value))
    }
}

impl<T: Send + 'static> std::ops::Deref for OffRuntime<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.as_ref().expect("owned until dropped")
    }
}

impl<T: Send + 'static> Drop for OffRuntime<T> {
    fn drop(&mut self) {
        if let Some(value) = self.0.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    runtime.spawn_blocking(move || drop(value));
                }
                Err(_) => drop(value),
            }
        }
    }
}

/// How many `Ledger::read_window` calls may hold a pool connection at once:
/// `clamp(database_max_connections - 2, 1, build_workers)`.
///
/// Two connections are always left over, so a share append and the
/// candidate-lease heartbeat never wait out the pool's 15 s acquire timeout
/// behind multi-page window reads; a failed heartbeat drops recoverable claim
/// work. The ceiling is `build_workers`, because a caller holds its
/// `build_slots` permit across the read, so no more reads can be in flight
/// than there are build slots. The floor is 1: one read at a time still makes
/// progress, and `PRISM_DATABASE_MAX_CONNECTIONS` can be as low as 4 while the
/// pool itself is opened with at least 2 connections.
fn window_read_permits(database_connections: u32, build_workers: usize) -> usize {
    usize::try_from(database_connections)
        .unwrap_or(usize::MAX)
        .saturating_sub(2)
        .clamp(1, build_workers.max(1))
}

fn protocol_error(reason: &'static str, message: &str) -> StratumError {
    let code = match reason {
        "stale-job" | "unknown-job" | "pool-closed" => 21,
        "duplicate-share" => 22,
        "low-difficulty" => 23,
        _ => 20,
    };
    StratumError::new(code, message, reason)
}

fn template_parent_height(candidate_height: u64) -> Result<u64> {
    candidate_height
        .checked_sub(1)
        .context("candidate block height must be positive")
}

/// Convert qbit/kweight to bits/kweight, rounding a positive sub-bit remainder
/// upward without introducing binary floating-point error into the fee policy.
fn fee_estimate_bits(value: &Value) -> Result<u64> {
    let text = match value {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => anyhow::bail!("invalid CTV market fee estimate"),
    };
    ensure!(
        text.len() <= 4096,
        "CTV fee estimate exceeds decimal length limit"
    );
    // Validate number strings with the same decimal grammar as JSON numbers.
    // arbitrary_precision preserves their exact coefficient and exponent.
    let decimal = text
        .parse::<serde_json::Number>()
        .context("invalid CTV market fee decimal")?
        .to_string();
    ensure!(
        !decimal.starts_with('-'),
        "CTV market fee estimate must be positive"
    );
    let (mantissa, exponent) = match decimal.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (
            mantissa,
            exponent
                .parse::<i64>()
                .context("CTV fee exponent overflow")?,
        ),
        None => (decimal.as_str(), 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    let significant = digits.trim_start_matches('0');
    ensure!(
        !significant.is_empty(),
        "CTV market fee estimate must be positive"
    );
    let power = exponent
        .checked_add(8)
        .and_then(|power| power.checked_sub(fraction.len() as i64))
        .context("CTV fee exponent overflow")?;
    let coefficient = significant.parse::<BigUint>()?;
    let amount = if power >= 0 {
        ensure!(
            power <= 19 && significant.len() as i64 + power <= 20,
            "CTV fee rate overflow"
        );
        coefficient * BigUint::from(10u8).pow(power as u32)
    } else {
        let places = power.unsigned_abs();
        if places >= significant.len() as u64 {
            // Any positive amount strictly below one bit rounds up to one.
            // This also avoids allocating enormous powers for tiny exponents.
            return Ok(1);
        }
        let divisor = BigUint::from(10u8).pow(places as u32);
        (coefficient + &divisor - BigUint::from(1u8)) / divisor
    };
    amount.to_u64().context("CTV fee rate overflow")
}

#[derive(Debug)]
pub(crate) struct ValidatedFeePolicy {
    policy: FanoutFeeRatePolicy,
    floor: u64,
}

pub(crate) async fn validated_ctv_fee_policy(
    rpc: &Rpc,
    configured: Option<FanoutFeeRatePolicy>,
    premium_bps: u64,
) -> Result<ValidatedFeePolicy> {
    let policy = if let Some(policy) = configured {
        policy
    } else {
        let estimate = rpc.call("estimatesmartfee", json!([2])).await?;
        let bits = fee_estimate_bits(&estimate["feerate"]).context("CTV fee estimate unavailable; configure PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT")?;
        FanoutFeeRatePolicy::new(bits, premium_bps)
    };
    let mempool = rpc.call("getmempoolinfo", json!([])).await?;
    ensure!(mempool.is_object(), "getmempoolinfo returned non-object");
    let mut required_rate = None;
    for name in ["minrelaytxfee", "mempoolminfee"] {
        if let Some(value) = mempool.get(name).filter(|value| !value.is_null()) {
            let floor = fee_estimate_bits(value)
                .with_context(|| format!("invalid getmempoolinfo.{name} fee floor"))?;
            required_rate = Some(required_rate.map_or(floor, |current: u64| current.max(floor)));
        }
    }
    let required_rate = required_rate.context("getmempoolinfo did not report a relay fee floor")?;
    validate_fee_floor(policy, required_rate)?;
    Ok(ValidatedFeePolicy {
        policy,
        floor: required_rate,
    })
}

fn validate_fee_floor(policy: FanoutFeeRatePolicy, required_rate: u64) -> Result<()> {
    ensure!(
        policy.market_fee_rate_sats_per_1000_weight >= required_rate,
        "PRISM CTV fanout fee rate is below the connected node relay floor: configured={} required={required_rate} bits/1000 weight",
        policy.market_fee_rate_sats_per_1000_weight
    );
    // The configured premium is a multiplier, and can be less than 1x. Check
    // its exact effective rate as well, without rounding a discounted rate up.
    ensure!(
        u128::from(policy.market_fee_rate_sats_per_1000_weight) * u128::from(policy.premium_bps)
            >= u128::from(required_rate) * 10_000,
        "PRISM CTV fanout fee premium reduces the effective fee below the connected node relay floor"
    );
    Ok(())
}

impl Coordinator {
    /// Start a `run` frontend's coordinator: validate the node and the
    /// configuration, register the frontend with a `starting` heartbeat and
    /// pin or verify the cluster fingerprint.
    pub async fn new(config: Config, metrics: Arc<crate::metrics::Metrics>) -> Result<Arc<Self>> {
        Self::connect(config, metrics, true).await
    }

    /// Start a one-shot command's coordinator (`self-check`, `broadcast-ctv`).
    /// Every gate of [`Coordinator::new`] still applies: the node's genesis
    /// and chain, the schema and capability checks, the halt guard and the
    /// cluster fingerprint. The difference is [`Ledger::connect_tool`] in
    /// place of the registering connection: the command writes no heartbeat,
    /// so its exit leaves no frontend row behind and a live frontend sharing
    /// its instance ID is not touched. Claims it takes remain fenced by their
    /// own tokens.
    pub async fn new_tool(
        config: Config,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Result<Arc<Self>> {
        Self::connect(config, metrics, false).await
    }

    async fn connect(
        mut config: Config,
        metrics: Arc<crate::metrics::Metrics>,
        register_frontend: bool,
    ) -> Result<Arc<Self>> {
        let rpc = Rpc::new(
            config.rpc_url.clone(),
            config.rpc_user.clone(),
            config.rpc_password.clone(),
            config.rpc_timeout,
        )?;
        if let Some(address) = &config.fee_address {
            let validation = rpc.call("validateaddress", json!([address])).await?;
            let script = validation["scriptPubKey"]
                .as_str()
                .context("pool fee address has no script")?;
            ensure!(
                validation["isvalid"] == true
                    && script.starts_with("5220")
                    && hex::decode(script)?.len() == 34,
                "pool fee address must be P2MR"
            );
            config
                .payout_policy
                .pool_fee_policy
                .as_mut()
                .context("missing fee policy")?
                .p2mr_program_hex = script[4..].into();
        }
        let genesis = rpc.call("getblockhash", json!([0])).await?;
        config.verify_genesis(genesis.as_str().context("qbit genesis hash missing")?)?;
        let info = rpc.call("getblockchaininfo", json!([])).await?;
        let configured_chain = match config.chain.as_str() {
            "mainnet" => "main",
            "testnet" => "test",
            other => other,
        };
        let actual_chain = info["chain"]
            .as_str()
            .context("qbit chain identity missing")?;
        ensure!(
            actual_chain == configured_chain
                || (configured_chain == "testnet3" && actual_chain == "test"),
            "configured QBIT_CHAIN differs from connected node"
        );
        let ledger = Arc::new(if register_frontend {
            Ledger::connect_with_metrics(
                &config.database_url,
                config.instance_id.clone(),
                config.database_connections,
                config.initialize_schema,
                Some(metrics.clone()),
            )
            .await?
        } else {
            Ledger::connect_tool(
                &config.database_url,
                config.instance_id.clone(),
                config.database_connections,
                config.initialize_schema,
                Some(metrics.clone()),
            )
            .await?
        });
        if !config.initialize_schema {
            let ready: bool = sqlx::query_scalar(
                "SELECT count(*)=2 FROM qbit_prism_schema_migrations WHERE version IN (7,9)",
            )
            .fetch_one(&ledger.pool)
            .await
            .context("schema migrations table missing; initialize the Prism schema")?;
            ensure!(
                ready,
                "Prism schema migrations 007 and 009 are required for mining startup"
            );
        }
        // Keep a frontend's initial heartbeat non-quiescent if configuration
        // fails. Another live incarnation may share this instance ID, so this
        // rejected startup cannot safely publish `stopped` for the shared row.
        // A one-shot command wrote no heartbeat and has nothing to retract.
        ledger
            .configure(
                &config.fingerprint(genesis.as_str().context("invalid genesis hash")?)?,
                &local_signer_keys(&config)?,
            )
            .await?;
        // Read through the ledger pool, so this is the value its sessions run
        // with. PostgreSQL reports it in milliseconds; zero disables it.
        let statement_timeout: i64 = sqlx::query_scalar(
            "SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'",
        )
        .fetch_one(&ledger.pool)
        .await
        .context("reading the ledger sessions' statement_timeout")?;
        let statement_timeout = u64::try_from(statement_timeout)
            .ok()
            .filter(|millis| *millis > 0)
            .map(Duration::from_millis);
        let (refresh, _) = watch::channel(0);
        Ok(Arc::new(Self {
            metrics,
            build_slots: Arc::new(Semaphore::new(config.build_workers)),
            resume_flights: compact_resume::ResumeFlights::new(config.build_workers),
            window_reads: Arc::new(Semaphore::new(window_read_permits(
                config.database_connections,
                config.build_workers,
            ))),
            config: Arc::new(config),
            submit_ledger: ledger.clone(),
            work_ledger: ledger.clone(),
            issued_batcher: issued_batcher::IssuedBatcher::new(ledger.clone()),
            ledger,
            rpc,
            refresh,
            prepared: Arc::new(RwLock::new(None)),
            wake: Notify::new(),
            accepted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            blocks: AtomicU64::new(0),
            readiness: Arc::new(RwLock::new(ReadinessState::default())),
            observed_tip: Arc::new(RwLock::new(TipState::default())),
            last_error: RwLock::new(None),
            refresh_lock: Mutex::new(RefreshState::default()),
            identities: Mutex::new(HashMap::new()),
            chain_cache: Mutex::new(None),
            statement_timeout,
            #[cfg(test)]
            offer_probe: Default::default(),
        }))
    }

    async fn fee_policy(&self) -> Result<Option<FanoutFeeRatePolicy>> {
        if !self.config.ctv_enabled {
            return Ok(None);
        }
        match validated_ctv_fee_policy(
            &self.rpc,
            self.config.ctv_fee,
            self.config.ctv_fee_premium_bps,
        )
        .await
        {
            Ok(validated) => {
                // Admission uses the latest observed floor even while a new
                // bundle is being built. Replaced jobs retain their own fee.
                self.readiness.write().await.ctv_fee_floor = Some(validated.floor);
                Ok(Some(validated.policy))
            }
            Err(error) => {
                self.invalidate_readiness().await;
                Err(error)
            }
        }
    }

    async fn ensure_job_fee_current(&self, fee: Option<FanoutFeeRatePolicy>) -> Result<()> {
        if self.config.ctv_enabled {
            let floor = self
                .readiness
                .read()
                .await
                .ctv_fee_floor
                .context("live CTV relay floor is unavailable")?;
            validate_fee_floor(fee.context("job CTV fee policy missing")?, floor)?;
        }
        Ok(())
    }

    async fn ready_chain_info(&self) -> Result<Value> {
        self.observe_chain_info(false).await
    }

    async fn observe_chain_info(&self, from_refresh: bool) -> Result<Value> {
        let sequence = self.observed_tip.write().await.reserve();
        let result =
            crate::readiness::chain_info(&self.rpc, &self.config.chain, self.config.min_peers)
                .await
                .and_then(|info| {
                    let hash = tip_observation::tip_hash(&info["bestblockhash"])
                        .context("qbit did not report a valid tip hash")?
                        .to_owned();
                    Ok((info, hash))
                });
        match result {
            Ok((info, hash)) => {
                self.observed_tip
                    .write()
                    .await
                    .observe(&hash, sequence, from_refresh);
                Ok(info)
            }
            Err(error) => {
                // An observed unsafe node state closes admission immediately;
                // a previous successful poll must not keep work trusted.
                self.invalidate_readiness().await;
                Err(error)
            }
        }
    }

    async fn invalidate_readiness(&self) {
        let mut state = self.readiness.write().await;
        state.last_poll = None;
        state.generation = state
            .generation
            .checked_add(1)
            .expect("readiness generation exhausted");
    }

    async fn ensure_template_fresh(&self, template: &Value) -> Result<()> {
        let result =
            crate::readiness::validate_template_age(template, self.config.template_max_age);
        if result.is_err() {
            self.invalidate_readiness().await;
        }
        result
    }

    async fn ready_tip(&self, tip: &str) -> Result<Value> {
        let info = self.ready_chain_info().await?;
        ensure!(
            info["bestblockhash"].as_str() == Some(tip),
            "tip changed during node readiness proof"
        );
        Ok(info)
    }

    /// Reconcile against one coherent tip. Every frontend observes prepared
    /// parents before building a descendant with its carry-forward snapshot.
    pub async fn reconcile(&self, tip: &str, tip_height: u64, revision: i64) -> Result<()> {
        let blocks = self.work_ledger.pool_blocks().await?;
        let mut cache = self.chain_cache.lock().await;
        let extends = if let Some(previous) = cache.as_ref() {
            tip_height >= previous.height
                && (tip == previous.tip
                    || self
                        .rpc
                        .call("getblockhash", json!([previous.height]))
                        .await?
                        .as_str()
                        == Some(&previous.tip))
        } else {
            false
        };
        let mut observations = Vec::with_capacity(blocks.len());
        for block in blocks {
            ensure!(
                block.maturity_state != "mature" || block.height <= tip_height,
                "node is behind a mature accounting checkpoint"
            );
            let known = cache
                .as_ref()
                .filter(|previous| extends && block.height <= previous.height)
                .and_then(|previous| previous.observations.get(&block.block_hash))
                .copied();
            let active = if block.height > tip_height {
                false
            } else {
                match known {
                    Some(active) => active,
                    None => {
                        self.rpc
                            .call("getblockhash", json!([block.height]))
                            .await?
                            .as_str()
                            == Some(&block.block_hash)
                    }
                }
            };
            observations.push(BlockObservation {
                block_hash: block.block_hash,
                active,
            });
        }
        ensure!(
            self.rpc.call("getbestblockhash", json!([])).await?.as_str() == Some(tip),
            "tip changed during reconciliation"
        );
        self.ready_tip(tip).await?;
        // Reconciliation and settlement both count the block's durable first
        // confirmation, regardless of the outbox state at that moment.
        let first_confirmations = self
            .work_ledger
            .reconcile(&observations, tip_height, revision)
            .await?;
        self.blocks
            .fetch_add(first_confirmations, Ordering::Relaxed);
        *cache = Some(ChainCache {
            tip: tip.into(),
            height: tip_height,
            observations: observations
                .into_iter()
                .map(|row| (row.block_hash, row.active))
                .collect(),
        });
        Ok(())
    }

    pub async fn refresh_once(&self) -> Result<()> {
        let mut refresh = self.refresh_lock.lock().await;
        let RefreshState {
            observation,
            cached_window,
        } = &mut *refresh;
        // Concurrent candidate observations can revoke trust while this
        // refresh waits for RPC or database work. Their later failure must
        // survive an older successful proof completing afterwards.
        let proof = self.begin_compact_build().await;
        let readiness_generation = proof.readiness_epoch();
        // Capture before node I/O, so a delayed equal-work observation cannot
        // overwrite a replacement accepted while its proof was in flight.
        let chain_observation = self.work_ledger.chain_observation_state().await?;
        let info = self.observe_chain_info(true).await?;
        let chainwork = info["chainwork"]
            .as_str()
            .context("node chainwork missing")?;
        let rules = if self.config.chain == "signet" {
            json!(["segwit", "signet"])
        } else {
            json!(["segwit"])
        };
        let mut template = self
            .rpc
            .call("getblocktemplate", json!([{"rules":rules}]))
            .await?;
        self.ensure_template_fresh(&template).await?;
        let selected_mask = codec::version_mask_from_template(&template, self.config.version_mask)?;
        template["versionrollingmask"] = json!(format!("{selected_mask:08x}"));
        let parent = template["previousblockhash"]
            .as_str()
            .context("template parent missing")?;
        let height = template["height"]
            .as_u64()
            .context("template height missing")?;
        ensure!(height > 0, "invalid template height");
        ensure!(
            info["bestblockhash"].as_str() == Some(parent)
                && info["blocks"].as_u64() == Some(height - 1),
            "tip changed between chain observation and template"
        );
        ensure!(
            self.rpc.call("getbestblockhash", json!([])).await?.as_str() == Some(parent),
            "template tip is stale"
        );
        self.cache_tip_parent(parent).await?;
        let observed_revision = observation
            .observe(
                &*self.work_ledger,
                parent,
                height - 1,
                chainwork,
                &chain_observation,
            )
            .await?;
        self.reconcile(parent, height - 1, observed_revision)
            .await?;
        let bits =
            codec::parse_u32_hex(template["bits"].as_str().context("template bits missing")?)?;
        let network = codec::scaled_target_difficulty(&codec::target_from_compact(bits)?)?;
        let mut stable = template.clone();
        for field in ["curtime", "mintime", "longpollid", "noncerange", "mutable"] {
            stable
                .as_object_mut()
                .context("template object missing")?
                .remove(field);
        }
        let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(&stable)?));
        let probe = self
            .work_ledger
            .refresh_probe(crate::ledger::ReadAdmission::default())
            .await?;
        let state = probe.payout_state;
        let share_seq = probe.accepted_share_seq;
        // Relay floors can change without changing the template or ledger.
        // Validate them on every refresh, including the cached-work path.
        let fee = self.fee_policy().await?;
        if let Some(current) = self.prepared.read().await.as_ref() {
            // A new share invalidates build inputs, but does not itself replace
            // usable published work. Preserve the existing same-template
            // cadence; the next template/economic change or original reanchor
            // reads the latest shares. Empty-to-first-share remains immediate.
            if cached_window.as_ref().is_some_and(|window| {
                window.reference == current.window
                    && window.within_reanchor_interval(self.config.snapshot_interval)
            }) && current.fee == fee
                && current.fingerprint == fingerprint
                && current.snapshot.payout_revision == state.payout_revision
                && current.window.prior_balances_digest == state.prior_balances_digest
                && (current.bundle.is_some() || current.snapshot.share_seq == share_seq)
                && crate::readiness::validate_template_age(
                    &current.template,
                    self.config.template_max_age,
                )
                .is_ok()
            {
                self.ready_tip(parent).await?;
                self.ensure_template_fresh(&template).await?;
                ensure!(
                    self.work_ledger.payout_state().await? == state,
                    "payout state changed during work reuse"
                );
                let mut readiness = self.readiness.write().await;
                ensure!(
                    readiness.generation == readiness_generation,
                    "node readiness changed during work reuse"
                );
                self.observed_tip
                    .write()
                    .await
                    .refresh_publication(parent)?;
                readiness.last_poll = Some(Instant::now());
                return Ok(());
            }
        }
        // Keep the original publication proof captured before node observation.
        let inputs = BundleInputs::capture(&self.config, fee)?;
        let retention =
            crate::config::number("PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS", 30.0f64)?.max(
                crate::config::number("PRISM_STRATUM_STALE_GRACE_SECONDS", 3.0f64)?,
            );
        ensure!(
            retention.is_finite() && retention > 0.0,
            "invalid job retention interval"
        );
        let shared_ttl = (retention
            + self.config.snapshot_interval.as_secs_f64()
            + self.config.health_timeout.as_secs_f64()
            + 60.0)
            .ceil();
        ensure!(
            shared_ttl < i64::MAX as f64 / 1000.0,
            "prepared TTL overflow"
        );
        let original_expires_at_ms = self
            .work_ledger
            .now_ms()
            .await?
            .checked_add(
                (shared_ttl as i64)
                    .checked_mul(1000)
                    .context("prepared TTL overflow")?,
            )
            .context("prepared expiry overflow")?;
        let permit = Arc::new(self.build_slots.clone().acquire_owned().await?);
        // Admission can wait across new shares, settlement, or reanchor expiry.
        // Select valid inputs after that wait, at the same boundary where a
        // fresh snapshot would be read. Later shares belong to the next window;
        // the selected WindowRef remains immutable through build/publication.
        let reuse_window = if let Some(window) = cached_window.as_ref() {
            let probe = self
                .work_ledger
                .refresh_probe(crate::ledger::ReadAdmission::shared(permit.clone()))
                .await?;
            window.reusable(
                network,
                probe.accepted_share_seq,
                probe.payout_state,
                self.config.snapshot_interval,
            )
        } else {
            false
        };
        if !reuse_window {
            // Retire cache ownership under admission before reading its
            // replacement. Any active blocking build keeps its own admission.
            let retired = cached_window.take();
            let cleanup = prepared_storage::compact::CompactOwner::new((retired, permit.clone()));
            cleanup
                .spawn_blocking(|(retired, permit)| {
                    let _admission = permit;
                    drop(retired.map(|window| window.into_inner()));
                })
                .await?;
            *cached_window = Some(self.capture_refresh_window(network, permit.clone()).await?);
        }
        // Cached inputs are not publication authority. Keep exactly one owner
        // in the serialized refresh loop even if a later build/save is cancelled.
        // This lets build admission end after actual build cleanup, so existing
        // lease holders can resume while a replacement reservation waits.
        let window = Arc::clone(cached_window.as_ref().expect("captured refresh window"));
        let admitted = prepared_storage::compact::CompactOwner::new((window, permit));
        let equivalent = self.prepared.read().await.as_ref().is_some_and(|current| {
            current.fingerprint == fingerprint
                && current.snapshot.share_seq == admitted.0.snapshot.share_seq
                && current.snapshot.payout_revision == admitted.0.snapshot.payout_revision
                && current.window.prior_balances_digest
                    == admitted.0.reference.prior_balances_digest
                && current.fee == fee
        });
        let generation = self
            .refresh
            .borrow()
            .checked_add(u64::from(!equivalent))
            .context("prepared generation overflow")?;
        let parent_of_tip = self.cache_tip_parent(parent).await?;
        let storage_key = format!(
            "prepared:{}:{}",
            self.config.instance_id,
            uuid::Uuid::new_v4().simple()
        );
        let (window, permit) = admitted.into_inner();
        let source = prepared_storage::compact::RefreshBuild {
            proof,
            key: storage_key,
            template,
            window,
            inputs,
            fee,
            fingerprint,
            generation,
            parent_of_tip,
            suffix: format!(
                "{}{}",
                hex::encode(&self.config.coinbase_tag),
                "00".repeat(4 + self.config.extranonce2_size)
            ),
            original_expires_at_ms,
        };
        let captured = self
            .capture_refresh(prepared_storage::compact::CompactOwner::new((
                source, permit,
            )))
            .await?;
        let reserved = self.reserve_fresh_compact(&captured).await?;
        self.lock_compact_publication(reserved).await?.publish()?;
        Ok(())
    }

    /// Read a referenced window under a `window_reads` permit.
    ///
    /// The one entry point callers use. The design record's order is: the
    /// caller takes its own `build_slots` permit **first** and holds it across
    /// this read and the rebuild that follows; the `window_reads` permit is
    /// taken after it and released as soon as the read returns, before the
    /// rebuild, so at least two pool connections always stay free. Waiting for
    /// it counts against the caller's deadline.
    ///
    /// The permit is owned and handed to `Ledger::read_window_with_permit`,
    /// which holds it across the whole read, including the off-runtime cleanup
    /// a cancellation leaves behind, and releases it the moment the read
    /// returns. `Ledger::read_window` is awaited there on the runtime, never
    /// wrapped in `spawn_blocking`: it owns its own blocking hand-offs, one per
    /// page, and an outer blocking wrapper would starve them.
    ///
    /// The claim rebuild calls this under its 60 s deadline and the
    /// `build_slots` permit it already holds.
    async fn read_window(
        &self,
        window: &WindowRef,
        balances: BalanceSource,
    ) -> Result<Window, WindowError> {
        // Nothing closes this semaphore; a closed one could only mean the
        // process is shutting down, which callers treat as transient exactly
        // as they treat a closed pool.
        let permit = self
            .window_reads
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| WindowError::Database(sqlx::Error::PoolClosed))?;
        self.ledger
            .read_window_with_permit(window, balances, permit)
            .await
    }

    /// Build a job's bundle from `inputs`, never from configuration: the only
    /// configuration read here is the signing seeds, the signer, whose public
    /// keys `inputs.signer_keys` already names. With `bootstrap` set, the
    /// window is the one synthetic share fabricated from that worker, which
    /// is returned beside the bundle so the job can carry it verbatim.
    #[cfg(test)]
    async fn build_bundle(
        &self,
        snapshot: Arc<Snapshot>,
        template: Value,
        bootstrap: Option<Worker>,
        suffix: String,
        inputs: BundleInputs,
    ) -> Result<(AuditBundle, Option<AcceptedShare>)> {
        let permit = self.build_slots.clone().acquire_owned().await?;
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let (body, bootstrap_share) =
                bundle_build::build_body(&config, &snapshot, &template, bootstrap, suffix, inputs)?;
            // This owning compatibility adapter is used by legacy fixtures.
            // Compact runtime callers keep the borrowed parts through hashing.
            let shares = bootstrap_share
                .as_ref()
                .map_or_else(|| snapshot.shares.clone(), |share| vec![share.clone()]);
            Ok((body.into_bundle(shares), bootstrap_share))
        })
        .await?
    }

    pub async fn refresh_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(self.config.poll_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_hint_prune = Instant::now();
        loop {
            tokio::select! { _=tick.tick()=>{},_=self.wake.notified()=>{},_=shutdown.changed()=>break }
            // One definite accounting-only refusal may retry immediately with
            // a fresh proof. The allowance belongs to this external trigger:
            // another refusal must return to tick/wake cadence, not replenish
            // it. refresh_once retains the original witness epoch itself.
            for attempt in 0..2 {
                if attempt > 0 && shutdown.has_changed().unwrap_or(true) {
                    return;
                }
                match self.refresh_once().await {
                    Ok(()) => {
                        *self.last_error.write().await = None;
                        break;
                    }
                    Err(error) => {
                        let retry = error.is::<crate::ledger::ChainObservationRetry>();
                        tracing::warn!(%error,"template refresh deferred");
                        *self.last_error.write().await = Some(error.to_string());
                        if !retry {
                            break;
                        }
                    }
                }
            }
            if last_hint_prune.elapsed() >= Duration::from_secs(300) {
                last_hint_prune = Instant::now();
                if let Ok(ttl) =
                    crate::config::number("PRISM_STRATUM_VARDIFF_RESUME_TTL_SECONDS", 900u64)
                {
                    if ttl > 0 {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(2),
                            self.ledger.prune_worker_difficulties(ttl, 1024),
                        )
                        .await;
                    }
                }
            }
        }
    }

    pub async fn blockwait_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                break;
            }
            let notification = tokio::select! {
                biased;
                _=shutdown.changed()=>break,
                result=self.rpc.call_timeout("waitfornewblock",json!([5000]),Some(Duration::from_secs(7)))=>result,
            };
            // A notification is a wake hint, not a sequenced chain proof. Even
            // errors wake normal polling; an unavailable long-poll method must
            // not terminate this critical task or declare the node unsafe.
            self.wake.notify_one();
            let verification = async {
                let value = notification?;
                let hash = tip_observation::tip_hash(&value["hash"])
                    .context("qbit block notification has no valid tip hash")?;
                let changed = self.observed_tip.read().await.as_deref() != Some(hash);
                if changed {
                    // Reserve order when this fresh request starts, after the
                    // wait completes. A later poll can still supersede it.
                    // No refresh lock: a pending build must not delay fencing.
                    self.observe_chain_info(true).await?;
                }
                Ok::<_, anyhow::Error>(())
            };
            let result = tokio::select! {
                biased;
                _=shutdown.changed()=>break,
                result=verification=>result,
            };
            if let Err(error) = result {
                tracing::debug!(%error, "block notification deferred to polling");
                tokio::select! {
                    biased;
                    _=shutdown.changed()=>break,
                    _=tokio::time::sleep(self.config.poll_interval)=>{},
                }
            }
        }
    }

    /// One coherent read-only observation of a candidate's block against the
    /// chain: the tip before and after is the same hash, the node is ready
    /// on it, and the payout revision is the one the ledger recorded for
    /// that tip view. `at_height` is the active block at the candidate's
    /// height, `None` while the tip is below it; the block is `active`
    /// exactly when that is the candidate's own hash. Any failure leaves the
    /// candidate unobserved: the caller settles nothing on an error.
    async fn observe_candidate(&self, claim: &CandidateClaim) -> Result<CandidateObservation> {
        let height = claim.candidate.found_block.block_height;
        let info = self.ready_chain_info().await?;
        let tip_height = info["blocks"].as_u64().context("invalid tip height")?;
        let tip = info["bestblockhash"]
            .as_str()
            .context("invalid tip hash")?
            .to_owned();
        let revision = self
            .ledger
            .observe_chain_view(
                &tip,
                tip_height,
                info["chainwork"]
                    .as_str()
                    .context("node chainwork missing")?,
            )
            .await?;
        // ready_chain_info already published this sequenced observation.
        let at_height = if tip_height >= height {
            Some(
                tip_observation::tip_hash(&self.rpc.call("getblockhash", json!([height])).await?)
                    .context("qbit did not report a valid block hash at the candidate's height")?
                    .to_owned(),
            )
        } else {
            None
        };
        ensure!(
            self.rpc.call("getbestblockhash", json!([])).await?.as_str() == Some(&tip),
            "tip changed while observing candidate"
        );
        self.ready_tip(&tip).await?;
        let active = at_height.as_deref() == Some(claim.candidate.block_hash.as_str());
        Ok(CandidateObservation {
            active,
            revision,
            tip,
            tip_height,
            at_height,
        })
    }

    pub async fn process_candidate(&self, claim: &CandidateClaim) -> Result<()> {
        self.process_candidate_with_lease(claim, CANDIDATE_LEASE)
            .await
    }

    async fn renew_candidate(&self, claim: &CandidateClaim, lease: CandidateLease) -> Result<()> {
        tokio::time::timeout(
            lease.timeout,
            self.ledger.renew_candidate_claim(claim, lease.seconds),
        )
        .await
        .context("candidate lease renewal deadline exceeded")?
        .context("candidate lease renewal failed")
    }

    async fn process_candidate_with_lease(
        &self,
        claim: &CandidateClaim,
        lease: CandidateLease,
    ) -> Result<()> {
        // Establish ownership before even waiting for build capacity. Keep
        // renewal and processing independently polled: processing may hold a
        // database row lock while a renewal waits for that same transaction.
        let initially_renewed = tokio::time::Instant::now();
        self.renew_candidate(claim, lease).await?;
        let live_token_query = format!(
            "SELECT CASE WHEN state IN {} AND claim_token=$2 AND claim_expires_at>clock_timestamp() THEN floor(extract(epoch FROM claim_expires_at-clock_timestamp())*1000)::bigint END FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            CandidateState::UNFINISHED_SQL
        );
        let heartbeat = async {
            let mut valid_until = initially_renewed + Duration::from_secs(lease.seconds as u64);
            let mut delay = lease.interval;
            loop {
                tokio::time::sleep(
                    delay.min(valid_until.saturating_duration_since(tokio::time::Instant::now())),
                )
                .await;
                let started = tokio::time::Instant::now();
                ensure!(
                    started < valid_until,
                    "candidate lease expired before renewal"
                );
                let bounded = CandidateLease {
                    timeout: lease.timeout.min(valid_until - started),
                    ..lease
                };
                match self.renew_candidate(claim, bounded).await {
                    Ok(()) => {
                        valid_until = started + Duration::from_secs(lease.seconds as u64);
                        delay = lease.interval;
                    }
                    Err(error) => {
                        let contention = error
                            .downcast_ref::<tokio::time::error::Elapsed>()
                            .is_some()
                            || error
                                .downcast_ref::<sqlx::Error>()
                                .and_then(sqlx::Error::as_database_error)
                                .is_some_and(|database| {
                                    database.code().as_deref() == Some("55P03")
                                });
                        if !contention {
                            return Err(error);
                        }
                        // A brief terminal UPDATE can conflict with renewal.
                        // Continue only after a read proves the exact token is
                        // still live, and never run beyond that database expiry.
                        let observed = tokio::time::Instant::now();
                        let budget = lease
                            .timeout
                            .min(valid_until.saturating_duration_since(observed));
                        let remaining = tokio::time::timeout(
                            budget,
                            sqlx::query_scalar::<_, Option<i64>>(&live_token_query)
                                .bind(&claim.candidate.block_hash)
                                .bind(&claim.claim_token)
                                .fetch_optional(&self.ledger.pool),
                        )
                        .await;
                        let Ok(Ok(Some(Some(remaining)))) = remaining else {
                            return Err(error);
                        };
                        if remaining <= 0 {
                            return Err(error);
                        }
                        let remaining = Duration::from_millis(remaining as u64);
                        valid_until = observed + remaining;
                        delay = lease
                            .interval
                            .min((remaining / 2).max(Duration::from_millis(1)));
                    }
                }
            }
        };
        // Neither future is spawned. Completion, cancellation and lease loss
        // all drop the other future; no orphan task can keep a lease alive.
        let mut work = Box::pin(self.process_candidate_inner(claim, lease));
        tokio::select! {
            biased;
            result = &mut work => result,
            failure = heartbeat => {
                drop(work);
                // Finishing can commit its terminal state just before the
                // processing future receives COMMIT's reply. The canceled
                // work needs no retry when durable completion already won.
                // Every state that is not unfinished is terminal. A proven
                // orphan that committed here was not counted by
                // `record_candidate_orphaned`: that counter reports completions
                // this process observed, not every committed settlement.
                let terminal = tokio::time::timeout(lease.timeout, sqlx::query_scalar::<_, bool>(
                    &format!("SELECT state NOT IN {} FROM qbit_block_candidate_outbox WHERE block_hash=$1", CandidateState::UNFINISHED_SQL)
                ).bind(&claim.candidate.block_hash).fetch_optional(&self.ledger.pool)).await;
                if matches!(terminal, Ok(Ok(Some(true)))) { Ok(()) } else { failure }
            },
        }
    }

    /// Whether the block's audit has already landed and, if so, whether the
    /// landed row is the audit of this block: authenticated against the
    /// candidate's block bytes, never read back as a body. `None` when no
    /// audit has landed; `Some(Err(reason))` when one has and it is not this
    /// block's, which is a refusal for the caller to settle, never a rebuild
    /// over the existing evidence. Only database errors propagate.
    async fn landed_audit_authenticated(
        &self,
        claim: &CandidateClaim,
    ) -> Result<Option<Result<(), String>>> {
        let candidate = &claim.candidate;
        let Some(landed) = self.ledger.landed_audit(&candidate.block_hash).await? else {
            return Ok(None);
        };
        if let Err(error) = authenticate_landed_audit(candidate, &landed) {
            return Ok(Some(Err(format!(
                "landed audit for block {} does not authenticate against the candidate's block: {error:#}",
                candidate.block_hash
            ))));
        }
        if landed.found_block_bits.is_none() {
            self.ledger
                .record_landed_audit_bits(
                    &candidate.block_hash,
                    &header_bits_hex(&candidate.block_bytes)?,
                )
                .await?;
        }
        Ok(Some(Ok(())))
    }

    /// The stored builder version and signer keys must be this binary's, or
    /// the reference is never rebuilt here: a rebuild under another builder or
    /// other keys would produce bytes the coinbase does not commit to.
    fn stored_inputs_mismatch(&self, candidate: &Candidate) -> Result<Option<String>> {
        if candidate.audit_builder_version != qbit_prism::AUDIT_BUILDER_VERSION {
            return Ok(Some(format!(
                "candidate was built by audit builder version {} and this binary is version {}; not rebuilding, drain it with a matching frontend",
                candidate.audit_builder_version,
                qbit_prism::AUDIT_BUILDER_VERSION
            )));
        }
        let local = local_signer_keys(&self.config)?;
        if !candidate
            .signer_keys
            .manifest_key_hex
            .eq_ignore_ascii_case(&local.manifest_key_hex)
            || !candidate
                .signer_keys
                .ledger_key_hex
                .eq_ignore_ascii_case(&local.ledger_key_hex)
        {
            return Ok(Some(format!(
                "candidate was signed with keys manifest={} ledger={} and this frontend holds manifest={} ledger={}; not rebuilding, drain it with a frontend that holds those keys",
                candidate.signer_keys.manifest_key_hex,
                candidate.signer_keys.ledger_key_hex,
                local.manifest_key_hex,
                local.ledger_key_hex
            )));
        }
        Ok(None)
    }

    /// Read the candidate's window and rebuild its audit parts, under the
    /// `build_slots` permit and the one rebuild deadline.
    ///
    /// The `build_slots` permit is taken first and moves into the blocking
    /// build, so it is released only when that build finishes. The
    /// `window_reads` permit is taken after it, inside the deadline, and is
    /// held for the read alone. `Ledger::read_window` is awaited on the
    /// runtime; only the builder runs in `spawn_blocking`, and it is the
    /// borrowing builder called directly, never `build_bundle`.
    async fn rebuild_claim_parts(
        &self,
        claim: &CandidateClaim,
        balances: BalanceSource,
        lease: CandidateLease,
    ) -> Result<Result<ClaimParts, RebuildFailure>> {
        let permit = self.build_slots.clone().acquire_owned().await?;
        let candidate = claim.candidate.clone();
        let config = self.config.clone();
        let rebuild = async {
            let window = match self.read_window(&candidate.window, balances).await {
                Ok(window) => window,
                Err(error) => return classify_window_error(error).map(Err),
            };
            // No await between here and the hand-off: the window moves into
            // the blocking build, which owns it and drops it there.
            let build =
                tokio_util::task::AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let Window {
                        shares,
                        prior_balances,
                        ..
                    } = window;
                    let manifest_key = ManifestSigningKey::from_seed_hex(&config.manifest_seed)?;
                    let ledger_key = ManifestSigningKey::from_seed_hex(&config.ledger_seed)?;
                    build_claim_parts(
                        &candidate,
                        shares,
                        prior_balances,
                        &manifest_key,
                        &ledger_key,
                    )
                }));
            Ok(Ok(build.await??))
        };
        match tokio::time::timeout(lease.rebuild_deadline, rebuild).await {
            Ok(outcome) => outcome,
            Err(_) => Ok(Err(RebuildFailure::Retry(format!(
                "window read and audit rebuild exceeded the {} s deadline",
                lease.rebuild_deadline.as_secs()
            )))),
        }
    }

    /// The reason a rebuild failure gives the row.
    fn rebuild_reason(candidate: &Candidate, failure: RebuildFailure) -> String {
        match failure {
            RebuildFailure::Retry(reason) => reason,
            RebuildFailure::PriorBalancesChanged => format!(
                "as-issued balance snapshot {} is missing and the current balances are no longer the reference's; the audit the block commits to cannot be rebuilt here",
                hex::encode(candidate.window.prior_balances_digest)
            ),
            RebuildFailure::BalanceSnapshotMissing => format!(
                "as-issued balance snapshot {} is missing; pruned or never written",
                hex::encode(candidate.window.prior_balances_digest)
            ),
        }
    }

    /// Land the audit the block commits to, or say why that could not be
    /// done. `Ok(Ok(()))` means the audit is durable: an earlier attempt
    /// landed it and the row authenticates against the block, or it was
    /// rebuilt now from the as-issued balances and landed at the revision
    /// observed immediately before the landing transaction. The builder
    /// admission, the rebuild, the signature and coinbase verification and
    /// the landing's own fences all run here, after the offer. Every refusal
    /// is returned as its reason so the caller settles the row; only node and
    /// database errors propagate.
    ///
    /// The rebuild reads `AsIssued`: the snapshot every enqueue writes for
    /// its reference. A row enqueued without one (a bare caller) falls back
    /// to a `Current` read, which proves the current set still hashes to the
    /// reference before returning it, so it is the as-issued set exactly
    /// when it succeeds; if the balances have moved the audit cannot be
    /// rebuilt anywhere and the reason says so. The revision is observed
    /// after the rebuild, so however long the wait for build capacity was,
    /// a block that confirmed meanwhile does not fail the fence.
    async fn land_offered(
        &self,
        claim: &CandidateClaim,
        lease: CandidateLease,
    ) -> Result<Result<(), String>> {
        let candidate = &claim.candidate;
        match self.landed_audit_authenticated(claim).await? {
            Some(Ok(())) => return Ok(Ok(())),
            Some(Err(reason)) => return Ok(Err(reason)),
            None => {}
        }
        if let Some(reason) = self.stored_inputs_mismatch(candidate)? {
            return Ok(Err(reason));
        }
        let parts = match self
            .rebuild_claim_parts(claim, BalanceSource::AsIssued, lease)
            .await?
        {
            Ok(parts) => parts,
            Err(RebuildFailure::BalanceSnapshotMissing) => {
                match self
                    .rebuild_claim_parts(claim, BalanceSource::Current, lease)
                    .await?
                {
                    Ok(parts) => parts,
                    Err(failure) => return Ok(Err(Self::rebuild_reason(candidate, failure))),
                }
            }
            Err(failure) => return Ok(Err(Self::rebuild_reason(candidate, failure))),
        };
        // The rebuilt window is released off the runtime once the landing
        // returns, whichever way it went.
        let rebuilt = OffRuntime::new(claim.clone().with_parts(parts));
        let revision = self.observe_candidate(claim).await?.revision;
        match self
            .ledger
            .land_candidate_at_revision(&rebuilt, &self.config.ledger_public_key, revision)
            .await
        {
            Ok(_) => Ok(Ok(())),
            Err(error) => Ok(Err(format!("{error:#}"))),
        }
    }

    async fn process_candidate_inner(
        &self,
        claim: &CandidateClaim,
        lease: CandidateLease,
    ) -> Result<()> {
        match claim.lifecycle.state {
            CandidateState::Pending => self.offer_candidate(claim, lease).await,
            // A reservation this or another frontend took and never recorded
            // an outcome for: the call may or may not have been made. Never
            // offered again; reconciled against the chain as unknown.
            CandidateState::OfferReserved => {
                let reserved_by = claim
                    .lifecycle
                    .offer
                    .reserved_by
                    .as_deref()
                    .unwrap_or("an unknown instance");
                let reply = format!(
                    "offer reservation recovered from {reserved_by}: the submitblock call may or may not have been made and its outcome was never recorded; delivery unknown"
                );
                self.settle_offered_candidate(claim, lease, OfferOutcome::Unknown, Some(reply))
                    .await
            }
            CandidateState::Offered | CandidateState::Reconciliation => {
                let outcome = claim
                    .lifecycle
                    .offer
                    .outcome
                    .context("offered candidate row records no offer outcome")?;
                let reply = claim.lifecycle.offer.reply.clone();
                self.settle_offered_candidate(claim, lease, outcome, reply)
                    .await
            }
        }
    }

    /// The offer phase of a pending candidate: the minimum before the one
    /// `submitblock` call. The proof was validated in Stratum and the row was
    /// authenticated at claim. An ordinary candidate passes the cached
    /// staleness screen; a leased candidate skips it (#350: its lease covered
    /// the work, and its block is offered before any supersession check).
    /// Then the durable reservation is taken, the strictly-live token is
    /// fenced immediately before the bounded RPC, and the node's answer is
    /// recorded. Builder admission, the rebuild, verification, landing and
    /// confirmation all follow in the post-offer phase.
    async fn offer_candidate(&self, claim: &CandidateClaim, lease: CandidateLease) -> Result<()> {
        let candidate = &claim.candidate;
        let parent = header_parent(&candidate.block_bytes)?;
        if !candidate.leased
            && (self.observed_tip.read().await.as_deref() != Some(parent.as_str())
                || self.ledger.payout_revision().await? != candidate.payout_revision)
        {
            // Backlogged work commonly becomes stale before it is offered.
            // Cached hints only trigger this authoritative probe. A block
            // that is already active needs no offer, only its audit and its
            // confirmation; a superseded one is the one proven pre-offer
            // rejection that may abandon.
            let CandidateObservation {
                active,
                revision,
                tip,
                ..
            } = self.observe_candidate(claim).await?;
            if active {
                return self.adopt_active_candidate(claim, lease, &tip).await;
            }
            if revision != candidate.payout_revision || tip != parent {
                self.ledger
                    .finish_candidate_at_revision(
                        claim,
                        false,
                        Some("payout revision or parent superseded"),
                        revision,
                    )
                    .await?;
                return Ok(());
            }
        }
        // The request is prepared here, before the reservation and the
        // fence, so nothing but the fence and the send itself sits between
        // the strictly-live token check and the node. A wall clock the
        // offer boundary cannot be read from is refused now, while the row
        // is still pending, rather than after the reservation.
        let params = json!([hex::encode(&candidate.block_bytes)]);
        unix_ms_now().context("the first-offer boundary needs the wall clock")?;
        // The durable reservation: once it commits, no claim on any
        // frontend, this one included after a crash, offers the block again.
        self.ledger.reserve_offer(claim).await?;
        #[cfg(test)]
        self.offer_probe().await;
        // Renewal failure cancels the attempt even between periodic ticks.
        // Recheck the strictly-live token at the external mutation boundary.
        self.renew_candidate(claim, lease).await?;
        let (result, offered_at_ms) = self.submit_block(params).await?;
        // The one sample, emitted once the one call has returned, from the
        // start time captured immediately before it: a crash during the call
        // loses the sample (the approved unknown-timing exception), and no
        // sample is ever emitted for a call that never began.
        self.observe_first_offer(claim, offered_at_ms);
        let (outcome, reply) = classify_offer(&result);
        self.ledger
            .record_offer(claim, offered_at_ms, outcome, reply.as_deref())
            .await?;
        self.settle_offered_candidate(claim, lease, outcome, reply)
            .await
    }

    /// A pending block the chain already holds, found by the pre-offer
    /// probe: a frontend offered it and lost the outcome before this
    /// lifecycle existed, or reconciliation confirmed it. It needs no offer.
    /// Before anything lands it is adopted, durably, into the
    /// no-resubmission lifecycle with the node's evidence: a crash after this
    /// point recovers it as a reconciliation row that never offers, a later
    /// reorg cannot abandon it, and its landing runs under the post-offer
    /// rules, as-issued. The original offer's time is unknown and stays so.
    async fn adopt_active_candidate(
        &self,
        claim: &CandidateClaim,
        lease: CandidateLease,
        tip: &str,
    ) -> Result<()> {
        let evidence = format!(
            "node reports block {} active at height {} with tip {tip}",
            claim.candidate.block_hash, claim.candidate.found_block.block_height
        );
        let reason = format!(
            "adopted before any recorded offer: {evidence}; the original offer time is unknown, never offered again"
        );
        tracing::warn!(block = %claim.candidate.block_hash, %reason, "adopting an active pending block");
        self.ledger
            .adopt_active_candidate(claim, &evidence, &reason)
            .await?;
        self.settle_offered_candidate(claim, lease, OfferOutcome::Unknown, Some(evidence))
            .await
    }

    /// The post-offer phase, for the attempt that offered and for every
    /// recovery of an offered, reserved or adopted row: observe the chain,
    /// land the audit the block commits to at the observed revision, observe
    /// again, and finish the row as submitted once the block is proven active
    /// now. Nothing here offers, and nothing here abandons: a block that is
    /// not active, a landing that was refused, and a node or database error
    /// after the offer all settle the row in reconciliation with the reason
    /// and keep every piece of evidence, to be retried with read-only chain
    /// observations and never another `submitblock`. Only
    /// when even that settlement cannot be written (the database is
    /// unavailable, or the claim was lost) does the error propagate, and the
    /// row keeps its reservation or offer record for a later recovery.
    async fn settle_offered_candidate(
        &self,
        claim: &CandidateClaim,
        lease: CandidateLease,
        outcome: OfferOutcome,
        reply: Option<String>,
    ) -> Result<()> {
        let block = &claim.candidate.block_hash;
        let offer = describe_offer(outcome, reply.as_deref());
        let error = match self.settle_offered_inner(claim, lease, &offer).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        let reason = format!("post-offer processing failed ({offer}): {error:#}");
        match self.ledger.reconcile_candidate(claim, &reason).await {
            Ok(()) => {
                tracing::error!(
                    %block,
                    %reason,
                    "ALERT: offered candidate could not be processed; kept for reconciliation, never offered again"
                );
                Ok(())
            }
            Err(settlement) => Err(error.context(format!(
                "and the row could not be settled in reconciliation either ({settlement:#}); it keeps its offer record for recovery"
            ))),
        }
    }

    async fn settle_offered_inner(
        &self,
        claim: &CandidateClaim,
        lease: CandidateLease,
        offer: &str,
    ) -> Result<()> {
        let block = &claim.candidate.block_hash;
        if let Err(reason) = self.land_offered(claim, lease).await? {
            let reason = format!("landing failed after the offer ({offer}): {reason}");
            tracing::error!(
                %block,
                %reason,
                "ALERT: offered candidate could not land; kept for reconciliation, never offered again"
            );
            return self.ledger.reconcile_candidate(claim, &reason).await;
        }
        // A null reply can still describe a known side-chain block, and the
        // landing may have taken long: only active-chain evidence observed
        // now, at a revision proven now, advances the shared payout state.
        let observation = self.observe_candidate(claim).await?;
        if observation.active {
            let first_confirmation = self
                .ledger
                .finish_candidate_counted_at_revision(claim, true, None, observation.revision)
                .await?;
            self.blocks
                .fetch_add(u64::from(first_confirmation), Ordering::Relaxed);
            self.wake.notify_one();
            return Ok(());
        }
        // The proven orphan (#415): the audit above is durable, and this one
        // coherent observation shows a DIFFERENT block active at the
        // candidate's height with the configured confirmations. The row is
        // settled terminal at the observed revision, with the evidence as its
        // reason; a reorg reconciler that confirmed the block meanwhile has
        // bumped that revision, and the settlement then fails and falls to
        // the ordinary reconciliation retry below. An observation that
        // failed never reaches here, so a failed observation never settles.
        // The log line and the counter follow the committed settlement only;
        // a refused one reports nothing terminal.
        if let Some(reason) = self.orphan_evidence(claim, &observation) {
            self.ledger
                .orphan_candidate_at_revision(claim, &reason, observation.revision)
                .await?;
            self.metrics.record_candidate_orphaned();
            tracing::warn!(%block, %reason, "offered candidate settled as a proven orphan");
            return Ok(());
        }
        let reason = format!(
            "block is not on the active chain after the offer ({offer}); kept for reconciliation, never offered again"
        );
        tracing::warn!(%block, %reason, "offered candidate awaits chain reconciliation");
        self.ledger.reconcile_candidate(claim, &reason).await
    }

    /// The evidence that settles an offered candidate as a proven orphan, or
    /// `None` while the chain has not proven it: the candidate's own block
    /// is active, no block is active at its height yet (the tip is below
    /// it), or the competitor has fewer than
    /// `PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS` confirmations. Confirmations
    /// are counted from the tip the observation proved coherent
    /// (`tip_height - height + 1` for a block at `height`), so the verdict
    /// and the revision it is written at come from the same observation.
    fn orphan_evidence(
        &self,
        claim: &CandidateClaim,
        observation: &CandidateObservation,
    ) -> Option<String> {
        let height = claim.candidate.found_block.block_height;
        // The candidate's own hash at its height is exactly
        // `observation.active`. The tip height is the node's report: the
        // subtraction stays checked rather than trusting that `at_height` is
        // only ever read at or below it, and the count saturates.
        let competitor = observation.at_height.as_deref()?;
        if competitor == claim.candidate.block_hash {
            return None;
        }
        let confirmations = observation
            .tip_height
            .checked_sub(height)?
            .saturating_add(1);
        let required = self.config.candidate_orphan_confirmations;
        if confirmations < required {
            return None;
        }
        Some(format!(
            "proven orphan: block {competitor} is active at height {height} with {confirmations} confirmations (tip {} at height {}, {required} required) and block {} is not on the active chain; settled terminal with its landed audit kept for a reorg back, never offered again",
            observation.tip, observation.tip_height, claim.candidate.block_hash
        ))
    }

    /// The one proof-to-first-offer sample for a block, emitted by the
    /// attempt that holds the reservation once its `submitblock` call has
    /// returned, from the start time captured immediately before the call,
    /// and never anywhere else: a recovered reservation does not reach this
    /// point, so no second frontend can observe the same offer. Provenance:
    /// `proof_observed_at_ms` is the enqueuing frontend's wall clock and
    /// `offered_at_ms` this frontend's, so the interval spans two hosts' wall
    /// clocks and a negative one is skew, dropped with a warning rather than
    /// clamped to zero. A row without a proof time, written before 011 or by
    /// a bare enqueue, yields no sample. The durable `offered_at_ms` the
    /// outcome commit records is the marker of the sample this process took;
    /// a crash during the call or before that commit loses the sample with
    /// the process, and the recovery records none, which is the approved
    /// at-most-once exception.
    fn observe_first_offer(&self, claim: &CandidateClaim, offered_at_ms: i64) {
        let block = &claim.candidate.block_hash;
        let Some(proof_ms) = claim.lifecycle.proof_observed_at_ms else {
            tracing::info!(
                %block,
                "first offer: the proof observation time is unknown for this row; no latency sample"
            );
            return;
        };
        match u64::try_from(offered_at_ms.saturating_sub(proof_ms)) {
            Ok(millis) => self
                .metrics
                .observe_first_offer(Duration::from_millis(millis)),
            Err(_) => tracing::warn!(
                %block,
                proof_observed_at_ms = proof_ms,
                offered_at_ms,
                "first offer: the offering frontend's wall clock is behind the enqueuing frontend's; skew, no latency sample"
            ),
        }
    }

    #[cfg(test)]
    async fn offer_probe(&self) {
        let probe = self.offer_probe.lock().unwrap().clone();
        if let Some(probe) = probe {
            probe.entered.notify_one();
            probe.release.notified().await;
        }
    }

    /// The one bounded `submitblock` call of a prepared request. The wall
    /// clock is read immediately before the request is sent, after the token
    /// fence and after the request was prepared: the actual call boundary,
    /// which is what `offered_at_ms` records and where the sample starts.
    async fn submit_block(&self, params: Value) -> Result<(Result<Value>, i64)> {
        let offered_at_ms = unix_ms_now()?;
        let result = self
            .rpc
            .call_timeout(
                "submitblock",
                params,
                Some(self.config.block_submit_timeout),
            )
            .await;
        Ok((result, offered_at_ms))
    }

    pub async fn submit_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {_=shutdown.changed()=>break,_=tick.tick()=>{}}
            match self.ledger.claim_candidate(CANDIDATE_LEASE.seconds).await {
                Ok(Some(claim)) => {
                    let result = tokio::select! {
                        _ = shutdown.changed() => break,
                        result = self.process_candidate(&claim) => result,
                    };
                    if let Err(error) = result {
                        tracing::warn!(%error,block=%claim.candidate.block_hash,"candidate remains recoverable");
                        if let Err(retry) = self
                            .ledger
                            .retry_candidate(&claim, &error.to_string())
                            .await
                        {
                            tracing::error!(%retry,"candidate retry persistence failed");
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error,"candidate polling failed"),
            }
        }
    }

    pub async fn health(&self) -> Value {
        let (poll_age, fee_floor) = {
            let readiness = self.readiness.read().await;
            (
                readiness.last_poll.map(|time| time.elapsed().as_secs_f64()),
                readiness.ctv_fee_floor,
            )
        };
        let prepared = self.prepared.read().await.clone();
        let observed = self.observed_tip.read().await.as_deref().map(str::to_owned);
        let revision = self.ledger.payout_revision().await.ok();
        let ready = prepared.as_ref().is_some_and(|work| {
            work.template["previousblockhash"].as_str() == observed.as_deref()
                && Some(work.snapshot.payout_revision) == revision
                && (!self.config.ctv_enabled
                    || work
                        .fee
                        .zip(fee_floor)
                        .is_some_and(|(fee, floor)| validate_fee_floor(fee, floor).is_ok()))
        }) && poll_age
            .is_some_and(|age| age < self.config.health_timeout.as_secs_f64());
        let Value::Object(fields) = json!({"ok":ready,"status":if ready {"ok"} else {"unavailable"},"backend":"postgres","instance_id":self.config.instance_id,"runtime_workers":self.config.runtime_workers,"tip_poll_age_seconds":poll_age,"accepted_share_count":self.accepted.load(Ordering::Relaxed),"found_block_count":self.blocks.load(Ordering::Relaxed),"template_generation":prepared.as_ref().map(|p|p.generation),"template_age_seconds":prepared.as_ref().map(|p|p.created.elapsed().as_secs_f64()),"observed_tip":observed,"payout_state_generation":prepared.as_ref().map(|p|p.snapshot.payout_revision)})
        else {
            unreachable!("coordinator health fields are an object");
        };
        HeartbeatHealth::new(ready, fields).into_value()
    }
}

fn header_parent(block: &[u8]) -> Result<String> {
    let bytes = block.get(4..36).context("truncated block header")?;
    Ok(hex::encode(bytes.iter().rev().copied().collect::<Vec<_>>()))
}

impl MiningBackend for Coordinator {
    async fn observed_tip_hint(&self) -> Option<crate::stratum::RetentionTip> {
        self.observed_tip.read().await.retention_hint()
    }
    type Context = JobContext;

    async fn health_ready(&self) -> bool {
        self.health().await["ready"] == true
    }

    async fn worker_difficulty(
        &self,
        listener: &str,
        worker: &Worker,
        ttl_seconds: u64,
    ) -> Result<Option<(f64, Duration)>> {
        let hint = self
            .ledger
            .worker_difficulty(listener, &worker.username, ttl_seconds)
            .await?;
        Ok(hint.map(|hint| (hint.difficulty, Duration::from_millis(hint.age_ms))))
    }

    async fn remember_worker_difficulty(
        &self,
        listener: &str,
        worker: &Worker,
        difficulty: f64,
        share_id: Option<&str>,
        downward_only: bool,
    ) -> Result<()> {
        if downward_only {
            self.ledger
                .lower_worker_difficulty(listener, &worker.username, difficulty)
                .await?;
        } else if let Some(share_id) = share_id {
            ensure!(
                share_id.starts_with(&format!("{}:", worker.username)),
                "difficulty evidence belongs to another worker"
            );
            if let Some(evidence) = self.ledger.share_accepted_at_ms(share_id).await? {
                self.ledger
                    .record_worker_difficulty(listener, &worker.username, difficulty, evidence)
                    .await?;
            }
        }
        Ok(())
    }

    async fn new_session_id(&self) -> Result<crate::ledger::SessionId, StratumError> {
        self.ledger.new_session_id().await.map_err(|error| {
            if error.is::<crate::ledger::SessionAllocationExhausted>() {
                protocol_error("session-allocation-exhausted", &error.to_string())
            } else {
                protocol_error("backend-rpc-unavailable", "database unavailable")
            }
        })
    }

    async fn authorize(&self, username: &str) -> Result<Worker, StratumError> {
        if username.is_empty() || username.len() > 512 || username.chars().any(char::is_control) {
            return Err(protocol_error("unauthorized-worker", "invalid username"));
        }
        if let Some((worker, at)) = self.identities.lock().await.get(username) {
            if at.elapsed() < Duration::from_secs(3600) {
                return Ok(worker.clone());
            }
        }
        let (address, worker_name) = username
            .split_once('.')
            .map_or((username, None), |(address, worker)| {
                (address, Some(worker.to_string()))
            });
        let resolve = |address: String| async move {
            let validation = self
                .rpc
                .call("validateaddress", json!([address]))
                .await
                .map_err(|_| StratumError::backend("payout address validation unavailable"))?;
            match validation["isvalid"].as_bool() {
                Some(false) => return Ok(None),
                Some(true) => {}
                None => return Err(StratumError::backend("invalid address validation response")),
            }
            let script = validation["scriptPubKey"]
                .as_str()
                .ok_or_else(|| StratumError::backend("address validation has no payout script"))?;
            let script = hex::decode(script).map_err(|_| {
                StratumError::backend("address validation has an invalid payout script")
            })?;
            if script.len() == 34 && script.starts_with(&[0x52, 0x20]) {
                return Ok(Some((address, hex::encode(&script[2..]))));
            }
            // Preserve fallback for complete standard address scripts that
            // Prism cannot pay, but not malformed or unrecognized RPC output.
            let unsupported = (script.len() == 25
                && script.starts_with(&[0x76, 0xa9, 0x14])
                && script.ends_with(&[0x88, 0xac]))
                || (script.len() == 23
                    && script.starts_with(&[0xa9, 0x14])
                    && script.ends_with(&[0x87]))
                || (script.len() == 22 && script.starts_with(&[0x00, 0x14]))
                || (script.len() == 34 && script.starts_with(&[0x00, 0x20]))
                || ((4..=42).contains(&script.len())
                    && (0x51..=0x60).contains(&script[0])
                    && usize::from(script[1]) == script.len() - 2);
            if unsupported {
                Ok(None)
            } else {
                Err(StratumError::backend(
                    "address validation has an unrecognized payout script",
                ))
            }
        };
        let (payout_address, p2mr_program_hex) = match resolve(address.into()).await? {
            Some(identity) => identity,
            // Only a definitive invalid/unsupported address uses alias fallback.
            // RPC failures must never redirect or cache a miner's payout identity.
            None => match &self.config.username_fallback {
                Some(fallback) => resolve(fallback.clone()).await?.ok_or_else(|| {
                    protocol_error("unauthorized-worker", "invalid P2MR payout address")
                })?,
                None => {
                    return Err(protocol_error(
                        "unauthorized-worker",
                        "invalid P2MR payout address",
                    ))
                }
            },
        };
        let worker = Worker {
            username: username.into(),
            payout_address,
            worker_name,
            p2mr_program_hex,
        };
        let mut cache = self.identities.lock().await;
        if cache.len() >= 4096 {
            cache.retain(|_, (_, at)| at.elapsed() < Duration::from_secs(300));
            if cache.len() >= 4096 {
                cache.clear();
            }
        }
        cache.insert(username.into(), (worker.clone(), Instant::now()));
        Ok(worker)
    }

    async fn build_job(
        &self,
        worker: &Worker,
        extranonce1: &str,
        difficulty: f64,
        minimum_difficulty: f64,
    ) -> Result<MiningJob<JobContext>, StratumError> {
        let build = async {
            let initial = self.authority_view().await;
            let readiness_epoch = initial.readiness.generation;
            let published_tip = initial.tip.publication_stamp();
            let prepared = initial
                .prepared
                .as_ref()
                .cloned()
                .context("no current template")?;
            drop(initial);
            let mut issuance_authority = self
                .begin_issuance_authority(
                    tip_observation::PreparedIdentity::of(&prepared),
                    readiness_epoch,
                    None,
                )
                .await?
                .context("payout snapshot stale")?;
            ensure!(
                self.observed_tip.read().await.publication_stamp() == published_tip,
                "work publication changed during work admission"
            );
            self.ensure_job_fee_current(prepared.fee).await?;
            let (base, bundle, bootstrap_share) = self
                .materialize_wire(
                    prepared.clone(),
                    worker.clone(),
                    self.config.extranonce2_size,
                )
                .await?;
            let id = format!(
                "{}-{}",
                self.config.instance_id,
                uuid::Uuid::new_v4().simple()
            );
            let mut wire = base.reassign(id, extranonce1, difficulty, minimum_difficulty)?;
            wire.refresh_generation = prepared.generation;
            wire.payout_revision = prepared.snapshot.payout_revision;
            self.revalidate_issuance_authority(&mut issuance_authority, None)
                .await?
                .context("payout snapshot stale")?;
            Ok::<_, anyhow::Error>(MiningJob {
                wire,
                context: Arc::new(JobContext {
                    prepared,
                    worker: worker.clone(),
                    bundle,
                    bootstrap_share,
                    issuance_authority: Some(Arc::new(issuance_authority)),
                }),
            })
        };
        build.await.map_err(|error| {
            tracing::warn!(%error,"job preparation deferred");
            protocol_error("pool-closed", "current work temporarily unavailable")
        })
    }

    async fn persist_issued_job(
        &self,
        worker: &Worker,
        job: &MiningJob<JobContext>,
        version_mask: u32,
        ttl: Duration,
    ) -> Result<(), StratumError> {
        let save = self.save_issued_record(worker, job, version_mask, ttl);
        save.await.map_err(|error| {
            tracing::warn!(%error,"job persistence deferred");
            protocol_error("backend-rpc-unavailable", "job persistence unavailable")
        })
    }

    async fn resume_job(
        &self,
        worker: &Worker,
        job_id: &str,
    ) -> Result<Option<MiningJob<JobContext>>, StratumError> {
        let resume = async {
            let initial = self.authority_view().await;
            let readiness_epoch = initial.readiness.generation;
            let published_tip = initial.tip.publication_stamp();
            let published_payout = initial.prepared.as_ref().map(|prepared| {
                (
                    prepared.template["previousblockhash"].clone(),
                    prepared.snapshot.payout_revision,
                )
            });
            drop(initial);
            if job_id.len() > 256 || job_id.starts_with("prepared:") {
                return Ok(None);
            }
            let Some(payload) = self.work_ledger.job(job_id).await? else {
                return Ok(None);
            };
            let stored: StoredJob = serde_json::from_value(payload)?;
            ensure!(
                stored.extranonce2_size > 0 && stored.extranonce2_size <= 32,
                "invalid stored extranonce2 size"
            );
            if stored.worker.username != worker.username
                || stored.worker.payout_address != worker.payout_address
                || stored.worker.p2mr_program_hex != worker.p2mr_program_hex
            {
                return Ok(None);
            }
            prepared_storage::compact::prepared_dependency_key(&stored.prepared_key)?;
            let clock_started = tokio::time::Instant::now();
            let deadline = publication_authority::AbsoluteDeadline::from_database(
                self.work_ledger.now_ms().await?,
                clock_started,
                stored.expires_at_ms,
            )?;
            if !deadline.live() {
                return Ok(None);
            }
            // This is the same absolute child deadline translated before
            // any coalescer, permit or reconstruction wait. An expired waiter
            // releases only its own shared handle; siblings keep their proof.
            let recovery = async {
                let flight = self
                    .resume_flights
                    .join(self, &stored.prepared_key, stored.extranonce2_size)
                    .await;
                let Some(metadata) = flight.metadata.clone().await? else {
                    return Ok(None);
                };
                let (published_parent, published_revision) =
                    published_payout.as_ref().context("no current template")?;
                let identity = tip_observation::PreparedIdentity::from_compact(
                    &stored.prepared_key,
                    &metadata.record,
                );
                let Some(mut issuance_authority) = self
                    .begin_issuance_authority(identity, readiness_epoch, Some(stored.expires_at_ms))
                    .await?
                else {
                    return Ok(None);
                };
                // Preserve ordinary resume's publication compatibility. A
                // current database revision alone does not prove this frontend
                // has published that payout. Exact identity remains the
                // separate, stricter requirement for a replacement lease.
                if published_parent.as_str() != Some(metadata.record.parent_hash.as_str())
                    || *published_revision != metadata.record.payout_revision
                {
                    return Ok(None);
                }
                // Coalescer/metadata waits precede the identity-specific
                // proof. They cannot borrow a publication that superseded the
                // operation's original admission while those waits ran.
                if self.observed_tip.read().await.publication_stamp() != published_tip {
                    return Ok(None);
                }
                if !deadline.live() {
                    return Ok(None);
                }
                self.ensure_job_fee_current(metadata.record.fee).await?;
                let prepared = flight
                    .reconstruction(
                        self,
                        stored.prepared_key.clone(),
                        metadata,
                        stored.extranonce2_size,
                    )
                    .await?;
                if !deadline.live()
                    || self
                        .revalidate_issuance_authority(
                            &mut issuance_authority,
                            Some(stored.expires_at_ms),
                        )
                        .await?
                        .is_none()
                {
                    return Ok(None);
                }
                let target =
                    num_bigint::BigUint::parse_bytes(stored.share_target_hex.as_bytes(), 16)
                        .context("invalid stored share target")?;
                ensure!(
                    target.bits() > 0 && target.bits() <= 256,
                    "stored target out of range"
                );
                ensure!(
                    stored.share_difficulty.is_finite() && stored.share_difficulty > 0.0,
                    "invalid stored difficulty"
                );
                ensure!(
                    stored.extranonce1.len() == 8 && hex::decode(&stored.extranonce1)?.len() == 4,
                    "invalid stored extranonce1"
                );
                let (base, bundle, bootstrap_share) = self
                    .materialize_wire(
                        prepared.clone(),
                        stored.worker.clone(),
                        stored.extranonce2_size,
                    )
                    .await?;
                let mut wire = base.reassign(
                    job_id.into(),
                    &stored.extranonce1,
                    stored.share_difficulty,
                    0.0,
                )?;
                wire.clean_jobs = false;
                wire.share_target = target;
                wire.share_difficulty = stored.share_difficulty;
                wire.version_mask = stored.version_mask;
                wire.refresh_generation = prepared.generation;
                wire.payout_revision = prepared.snapshot.payout_revision;
                if !deadline.live()
                    || self
                        .revalidate_issuance_authority(
                            &mut issuance_authority,
                            Some(stored.expires_at_ms),
                        )
                        .await?
                        .is_none()
                {
                    return Ok(None);
                }
                if !deadline.live() {
                    return Ok(None);
                }
                wire.resume_expires_at = Some(
                    issuance_authority
                        .deadline()
                        .map_or(deadline.instant(), |original| {
                            original.min(deadline.instant())
                        }),
                );
                Ok(Some(MiningJob {
                    wire,
                    context: Arc::new(JobContext {
                        prepared,
                        worker: stored.worker,
                        bundle,
                        bootstrap_share,
                        issuance_authority: Some(Arc::new(issuance_authority)),
                    }),
                }))
            };
            match tokio::time::timeout_at(deadline.instant().into(), recovery).await {
                Ok(result) => result,
                Err(_) => Ok(None), // the original issued lease expired
            }
        };
        resume.await.map_err(|error| {
            tracing::warn!(%error,"job resume unavailable");
            protocol_error("backend-rpc-unavailable", "job resume unavailable")
        })
    }

    async fn submit(
        &self,
        worker: &Worker,
        job: &MiningJob<JobContext>,
        submission: codec::Submission,
        stale_grace: StaleGrace,
    ) -> Result<(), StratumError> {
        self.submit_share(worker, job, submission, stale_grace)
            .await
    }
}

#[cfg(test)]
mod fee_estimate_tests {
    use super::*;

    fn assert_number_and_string(decimal: &str, expected: u64) {
        let number: Value = serde_json::from_str(decimal).unwrap();
        assert_eq!(fee_estimate_bits(&number).unwrap(), expected, "{decimal}");
        assert_eq!(
            fee_estimate_bits(&Value::String(decimal.into())).unwrap(),
            expected,
            "string {decimal}"
        );
    }

    #[test]
    fn exact_fee_estimate_does_not_round_an_integer_bit_up() {
        assert_number_and_string("0.00001", 1000);
        assert_number_and_string("0.00000001", 1);
        assert_number_and_string("0.000010000000000000000001", 1001);
        assert_number_and_string("0.0000100000000000000000000001", 1001);
        assert_number_and_string("184467440737.09551615", u64::MAX);
        assert_number_and_string("184467440737.095516150", u64::MAX);
    }

    #[test]
    fn exact_fee_estimate_handles_scientific_and_sub_bit_values() {
        assert_number_and_string("1e-5", 1000);
        assert_number_and_string("1E-5", 1000);
        assert_number_and_string("1e+0", 100_000_000);
        assert_number_and_string("1.000000001e-5", 1001);
        assert_number_and_string("1e-9", 1);
        assert_number_and_string("9.999999999e-9", 1);
        assert_number_and_string("1e-1000000", 1);
    }

    #[test]
    fn exact_fee_estimate_rejects_overflow_including_fractional_ceiling() {
        for decimal in [
            "184467440737.0955161501",
            "184467440737.09551616",
            "1e1000000",
            "1e9999999999999999999999999999999999999",
        ] {
            let number: Value = serde_json::from_str(decimal).unwrap();
            assert!(fee_estimate_bits(&number).is_err(), "{decimal}");
            assert!(
                fee_estimate_bits(&Value::String(decimal.into())).is_err(),
                "string {decimal}"
            );
        }
    }

    #[test]
    fn exact_fee_estimate_rejects_zero_negative_and_malformed_values() {
        for value in [
            Value::Null,
            json!(true),
            json!([]),
            json!({}),
            json!(0),
            json!(-1),
        ] {
            assert!(fee_estimate_bits(&value).is_err(), "{value}");
        }
        for decimal in [
            "",
            "0",
            "0.000000000",
            "0e1000000",
            "-0",
            "-0.00001",
            "NaN",
            "Infinity",
            "+1",
            ".1",
            "1.",
            "01",
            "1e",
            "1e+",
            "1e2e3",
            r#"{"$serde_json::private::Number":"0.00001"}"#,
        ] {
            assert!(
                fee_estimate_bits(&Value::String(decimal.into())).is_err(),
                "{decimal}"
            );
        }
        assert!(fee_estimate_bits(&Value::String("1".repeat(4097))).is_err());
    }
}

#[cfg(test)]
mod fee_policy_tests;

#[cfg(test)]
mod candidate_lease_tests;

#[cfg(test)]
pub(crate) mod miner_tests;

#[cfg(test)]
mod compact_decode_tests;

#[cfg(test)]
mod d2_below_target_tests;

#[cfg(test)]
mod commit_reconcile_tests;

#[cfg(test)]
mod d2_bootstrap_tests;

#[cfg(test)]
mod d2_test_support;
#[cfg(test)]
mod test_serial;

#[cfg(test)]
mod window_ref_tests;

#[cfg(test)]
mod window_switch_tests;

#[cfg(test)]
mod window_incident_tests;

#[cfg(test)]
mod storm_evidence_tests;

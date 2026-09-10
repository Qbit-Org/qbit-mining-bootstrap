use crate::{
    codec,
    config::Config,
    ledger::{BlockObservation, Candidate, CandidateClaim, Ledger, Snapshot},
    rpc::Rpc,
    stratum::{MiningBackend, MiningJob, StratumError, Worker},
};
use anyhow::{ensure, Context, Result};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, AuditBundle, FanoutFeeRatePolicy, FoundBlock};
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

pub struct JobContext {
    pub prepared: Arc<Prepared>,
    pub worker: Worker,
    pub bundle: Arc<AuditBundle>,
}

pub struct Prepared {
    pub template: Value,
    pub snapshot: Arc<Snapshot>,
    pub bundle: Option<Arc<AuditBundle>>,
    pub base_wire: Option<codec::Job>,
    pub storage_key: String,
    pub fee: Option<FanoutFeeRatePolicy>,
    pub fingerprint: String,
    pub generation: u64,
    pub created: Instant,
    pub parent_of_tip: String,
}

#[derive(Serialize, Deserialize)]
struct StoredPrepared {
    template: Value,
    snapshot: Arc<Snapshot>,
    bundle: Option<Arc<AuditBundle>>,
    fee: Option<FanoutFeeRatePolicy>,
    fingerprint: String,
    generation: u64,
    parent_of_tip: String,
    coinbase_suffix: String,
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

pub struct Coordinator {
    pub config: Arc<Config>,
    pub ledger: Arc<Ledger>,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub rpc: Rpc,
    pub prepared: RwLock<Option<Arc<Prepared>>>,
    pub refresh: watch::Sender<u64>,
    pub wake: Notify,
    pub accepted: AtomicU64,
    pub rejected: AtomicU64,
    pub blocks: AtomicU64,
    readiness: RwLock<ReadinessState>,
    pub observed_tip: RwLock<Option<String>>,
    pub last_error: RwLock<Option<String>>,
    build_slots: Arc<Semaphore>,
    refresh_lock: Mutex<()>,
    identities: Mutex<HashMap<String, (Worker, Instant)>>,
    chain_cache: Mutex<Option<ChainCache>>,
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

#[derive(Clone, Copy)]
struct CandidateLease {
    seconds: i64,
    interval: Duration,
    timeout: Duration,
}

const CANDIDATE_LEASE: CandidateLease = CandidateLease {
    seconds: 120,
    interval: Duration::from_secs(30),
    timeout: Duration::from_secs(5),
};

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
struct ValidatedFeePolicy {
    policy: FanoutFeeRatePolicy,
    floor: u64,
}

async fn validated_ctv_fee_policy(
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
    pub async fn new(mut config: Config) -> Result<Arc<Self>> {
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
        let ledger = Arc::new(
            Ledger::connect(
                &config.database_url,
                config.instance_id.clone(),
                config.database_connections,
                config.initialize_schema,
            )
            .await?,
        );
        ledger
            .configure(&config.fingerprint(genesis.as_str().context("invalid genesis hash")?)?)
            .await?;
        let (refresh, _) = watch::channel(0);
        Ok(Arc::new(Self {
            metrics: Arc::new(crate::metrics::Metrics::default()),
            build_slots: Arc::new(Semaphore::new(config.build_workers)),
            config: Arc::new(config),
            ledger,
            rpc,
            refresh,
            prepared: RwLock::new(None),
            wake: Notify::new(),
            accepted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            blocks: AtomicU64::new(0),
            readiness: RwLock::new(ReadinessState::default()),
            observed_tip: RwLock::new(None),
            last_error: RwLock::new(None),
            refresh_lock: Mutex::new(()),
            identities: Mutex::new(HashMap::new()),
            chain_cache: Mutex::new(None),
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
        let result =
            crate::readiness::chain_info(&self.rpc, &self.config.chain, self.config.min_peers)
                .await;
        match result {
            Ok(info) => {
                *self.observed_tip.write().await =
                    info["bestblockhash"].as_str().map(str::to_owned);
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
        let blocks = self.ledger.pool_blocks_for_reconcile().await?;
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
        self.ledger
            .reconcile_blocks_at_revision(&observations, tip_height, revision)
            .await?;
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
        let _flight = self.refresh_lock.lock().await;
        // Concurrent candidate observations can revoke trust while this
        // refresh waits for RPC or database work. Their later failure must
        // survive an older successful proof completing afterwards.
        let readiness_generation = self.readiness.read().await.generation;
        let info = self.ready_chain_info().await?;
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
        *self.observed_tip.write().await = Some(parent.into());
        let observed_revision = self
            .ledger
            .observe_chain_view(parent, height - 1, chainwork)
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
        let revision = self.ledger.payout_revision().await?;
        // Relay floors can change without changing the template or ledger.
        // Validate them on every refresh, including the cached-work path.
        let fee = self.fee_policy().await?;
        if let Some(current) = self.prepared.read().await.as_ref() {
            if current.bundle.is_some()
                && current.fee == fee
                && current.fingerprint == fingerprint
                && current.snapshot.payout_revision == revision
                && current.created.elapsed() < self.config.snapshot_interval
                && crate::readiness::validate_template_age(
                    &current.template,
                    self.config.template_max_age,
                )
                .is_ok()
            {
                self.ready_tip(parent).await?;
                self.ensure_template_fresh(&template).await?;
                ensure!(
                    self.ledger.payout_revision().await? == current.snapshot.payout_revision,
                    "payout revision changed during work reuse"
                );
                let mut readiness = self.readiness.write().await;
                ensure!(
                    readiness.generation == readiness_generation,
                    "node readiness changed during work reuse"
                );
                readiness.last_poll = Some(Instant::now());
                return Ok(());
            }
        }
        let snapshot = Arc::new(self.ledger.snapshot(network).await?);
        let bundle = if snapshot.shares.is_empty() {
            None
        } else {
            Some(Arc::new(
                self.build_bundle(
                    snapshot.clone(),
                    template.clone(),
                    None,
                    format!(
                        "{}{}",
                        hex::encode(&self.config.coinbase_tag),
                        "00".repeat(4 + self.config.extranonce2_size)
                    ),
                    fee,
                )
                .await?,
            ))
        };
        let base_wire = if let Some(bundle) = &bundle {
            let template = template.clone();
            let bundle = bundle.clone();
            let extranonce2_size = self.config.extranonce2_size;
            Some(
                tokio::task::spawn_blocking(move || {
                    codec::Job::from_manifest(
                        "shared".into(),
                        &template,
                        &bundle.signed_coinbase_manifest.manifest,
                        "00000000",
                        extranonce2_size,
                        1.0,
                        0.0,
                        true,
                    )
                })
                .await??,
            )
        } else {
            None
        };
        ensure!(
            self.ledger
                .observe_chain_view(parent, height - 1, chainwork)
                .await?
                == snapshot.payout_revision,
            "payout revision changed during job build"
        );
        ensure!(
            self.rpc.call("getbestblockhash", json!([])).await?.as_str() == Some(parent),
            "tip changed during job build"
        );
        // Timer reanchors may refresh ntime without changing payable work.
        // Keep semantic coverage stable for that case; new accepted shares,
        // payout state, fee policy or template content require fresh delivery.
        let equivalent = self.prepared.read().await.as_ref().is_some_and(|current| {
            current.fingerprint == fingerprint
                && current.snapshot.share_seq == snapshot.share_seq
                && current.snapshot.payout_revision == snapshot.payout_revision
                && current.fee == fee
        });
        let generation = *self.refresh.borrow() + u64::from(!equivalent);
        let tip_header = self.rpc.call("getblockheader", json!([parent])).await?;
        let parent_of_tip = tip_header["previousblockhash"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let storage_key = format!(
            "prepared:{}:{}",
            self.config.instance_id,
            uuid::Uuid::new_v4().simple()
        );
        let stored = StoredPrepared {
            template: template.clone(),
            snapshot: snapshot.clone(),
            bundle: bundle.clone(),
            fee,
            fingerprint: fingerprint.clone(),
            generation,
            parent_of_tip: parent_of_tip.clone(),
            coinbase_suffix: format!(
                "{}{}",
                hex::encode(&self.config.coinbase_tag),
                "00".repeat(4 + self.config.extranonce2_size)
            ),
        };
        let payload = tokio::task::spawn_blocking(move || serde_json::to_value(stored)).await??;
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
            .ceil() as i64;
        self.ledger
            .save_job(
                &storage_key,
                &payload,
                snapshot.payout_revision,
                parent,
                shared_ttl,
            )
            .await?;
        // Persisting a large bundle can outlive the original node observation.
        // Recheck immediately before making this prepared work available.
        let published_info = self.ready_tip(parent).await?;
        self.ensure_template_fresh(&template).await?;
        ensure!(
            self.ledger
                .observe_chain_view(
                    parent,
                    height - 1,
                    published_info["chainwork"]
                        .as_str()
                        .context("node chainwork missing")?,
                )
                .await?
                == snapshot.payout_revision,
            "payout revision changed before job publication"
        );
        let mut prepared = self.prepared.write().await;
        let mut readiness = self.readiness.write().await;
        ensure!(
            readiness.generation == readiness_generation,
            "node readiness changed before job publication"
        );
        *prepared = Some(Arc::new(Prepared {
            template,
            snapshot,
            bundle,
            base_wire,
            storage_key,
            fee,
            fingerprint,
            generation,
            created: Instant::now(),
            parent_of_tip,
        }));
        readiness.last_poll = Some(Instant::now());
        self.refresh.send_replace(generation);
        Ok(())
    }

    async fn build_bundle(
        &self,
        snapshot: Arc<Snapshot>,
        template: Value,
        bootstrap: Option<Worker>,
        suffix: String,
        fee: Option<FanoutFeeRatePolicy>,
    ) -> Result<AuditBundle> {
        let permit = self.build_slots.clone().acquire_owned().await?;
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let network = codec::scaled_target_difficulty(&codec::target_from_compact(
                codec::parse_u32_hex(template["bits"].as_str().context("missing bits")?)?,
            )?)?;
            let found = FoundBlock {
                block_height: template["height"].as_u64().context("missing height")?,
                coinbase_value_sats: template["coinbasevalue"]
                    .as_u64()
                    .context("missing coinbase value")?,
                network_difficulty: network,
                anchor_job_issued_at_ms: snapshot.anchor_ms,
            };
            let shares = if let Some(worker) = bootstrap {
                vec![AcceptedShare {
                    share_seq: 1,
                    share_id: "bootstrap-share".into(),
                    miner_id: worker.payout_address.clone(),
                    order_key: worker.payout_address,
                    p2mr_program_hex: worker.p2mr_program_hex,
                    share_difficulty: network,
                    network_difficulty: network,
                    template_height: template_parent_height(found.block_height)?,
                    job_id: "bootstrap-job".into(),
                    job_issued_at_ms: snapshot.anchor_ms,
                    accepted_at_ms: snapshot.anchor_ms,
                    ntime: template["curtime"]
                        .as_u64()
                        .context("missing time")?
                        .try_into()?,
                    credit_policy: None,
                }]
            } else {
                snapshot.shares.clone()
            };
            let witnesses =
                codec::witness_merkle_leaves_hex(&codec::transactions_from_template(&template)?);
            let manifest_key = ManifestSigningKey::from_seed_hex(&config.manifest_seed)?;
            let ledger_key = ManifestSigningKey::from_seed_hex(&config.ledger_seed)?;
            // Prior-only recipients remain in the payout universe, including
            // during bootstrap after an empty reward window.
            if config.ctv_enabled {
                Ok(qbit_prism::build_audit_bundle_with_ctv_settlement_options(
                    shares,
                    found,
                    snapshot.prior_balances.clone(),
                    config.payout_policy.clone(),
                    config.ctv_direct_floor,
                    config.ctv_config,
                    fee,
                    Some(suffix),
                    witnesses,
                    &manifest_key,
                    &ledger_key,
                )?)
            } else {
                Ok(qbit_prism::build_audit_bundle_with_coinbase_options(
                    shares,
                    found,
                    snapshot.prior_balances.clone(),
                    config.payout_policy.clone(),
                    Some(suffix),
                    witnesses,
                    &manifest_key,
                    &ledger_key,
                )?)
            }
        })
        .await?
    }

    pub async fn refresh_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(self.config.poll_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_hint_prune = Instant::now();
        loop {
            tokio::select! { _=tick.tick()=>{},_=self.wake.notified()=>{},_=shutdown.changed()=>break }
            match self.refresh_once().await {
                Ok(()) => {
                    *self.last_error.write().await = None;
                }
                Err(error) => {
                    tracing::warn!(%error,"template refresh deferred");
                    *self.last_error.write().await = Some(error.to_string());
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
            tokio::select! {
                _=shutdown.changed()=>break,
                result=self.rpc.call_timeout("waitfornewblock",json!([5000]),Some(Duration::from_secs(7)))=> {
                    match result {Ok(value)=>{if let Some(hash)=value["hash"].as_str() {*self.observed_tip.write().await=Some(hash.into());}self.wake.notify_one();},Err(_)=>tokio::time::sleep(self.config.poll_interval).await}
                }
            }
        }
    }

    async fn observe_candidate(&self, claim: &CandidateClaim) -> Result<(bool, i64, String)> {
        let height = claim.candidate.bundle.found_block.block_height;
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
        *self.observed_tip.write().await = Some(tip.clone());
        let active = tip_height >= height
            && self
                .rpc
                .call("getblockhash", json!([height]))
                .await?
                .as_str()
                == Some(&claim.candidate.block_hash);
        ensure!(
            self.rpc.call("getbestblockhash", json!([])).await?.as_str() == Some(&tip),
            "tip changed while observing candidate"
        );
        self.ready_tip(&tip).await?;
        Ok((active, revision, tip))
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
                        let remaining = tokio::time::timeout(budget,sqlx::query_scalar::<_,Option<i64>>(
                            "SELECT CASE WHEN state='pending' AND claim_token=$2 AND claim_expires_at>clock_timestamp() THEN floor(extract(epoch FROM claim_expires_at-clock_timestamp())*1000)::bigint END FROM qbit_block_candidate_outbox WHERE block_hash=$1"
                        ).bind(&claim.candidate.block_hash).bind(&claim.claim_token).fetch_optional(&self.ledger.pool)).await;
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
                let terminal = tokio::time::timeout(lease.timeout, sqlx::query_scalar::<_, bool>(
                    "SELECT state IN ('submitted','abandoned') FROM qbit_block_candidate_outbox WHERE block_hash=$1"
                ).bind(&claim.candidate.block_hash).fetch_optional(&self.ledger.pool)).await;
                if matches!(terminal, Ok(Ok(Some(true)))) { Ok(()) } else { failure }
            },
        }
    }

    async fn process_candidate_inner(
        &self,
        claim: &CandidateClaim,
        lease: CandidateLease,
    ) -> Result<()> {
        let parent = header_parent(&claim.candidate.block_hex)?;
        if self.observed_tip.read().await.as_deref() != Some(parent.as_str())
            || self.ledger.payout_revision().await? != claim.candidate.payout_revision
        {
            // Backlogged work commonly becomes stale before reaching scarce
            // build capacity. Cached hints only trigger this authoritative
            // probe; an already-active block still needs its audit recovered.
            let (active, revision, tip) = self.observe_candidate(claim).await?;
            if !active && (revision != claim.candidate.payout_revision || tip != parent) {
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
        let finalized;
        let claim = if let Some(suffix) = &claim.candidate.coinbase_suffix_hex {
            let bundle = &claim.candidate.bundle;
            let config = self.config.clone();
            let source = bundle.clone();
            let suffix = suffix.clone();
            let permit = self.build_slots.clone().acquire_owned().await?;
            let rebuilt = tokio_util::task::AbortOnDropHandle::new(tokio::task::spawn_blocking(
                move || -> Result<AuditBundle> {
                    let _permit = permit;
                    let manifest_key = ManifestSigningKey::from_seed_hex(&config.manifest_seed)?;
                    let ledger_key = ManifestSigningKey::from_seed_hex(&config.ledger_seed)?;
                    if config.ctv_enabled {
                        Ok(qbit_prism::build_audit_bundle_with_ctv_settlement_options(
                            source.shares,
                            source.found_block,
                            source.prior_balances,
                            source.payout_policy,
                            config.ctv_direct_floor,
                            config.ctv_config,
                            source.ctv_fanout_fee_policy,
                            Some(suffix),
                            source.witness_merkle_leaves_hex,
                            &manifest_key,
                            &ledger_key,
                        )?)
                    } else {
                        Ok(qbit_prism::build_audit_bundle_with_coinbase_options(
                            source.shares,
                            source.found_block,
                            source.prior_balances,
                            source.payout_policy,
                            Some(suffix),
                            source.witness_merkle_leaves_hex,
                            &manifest_key,
                            &ledger_key,
                        )?)
                    }
                },
            ))
            .await??;
            let mut updated = claim.clone();
            updated.candidate.bundle = rebuilt;
            finalized = updated;
            &finalized
        } else {
            claim
        };
        let (active, revision, tip) = self.observe_candidate(claim).await?;
        if active {
            self.ledger
                .land_candidate_at_revision(claim, &self.config.ledger_public_key, revision)
                .await?;
            self.ledger
                .finish_candidate_at_revision(claim, true, None, revision)
                .await?;
            self.wake.notify_one();
            return Ok(());
        }
        let parent = header_parent(&claim.candidate.block_hex)?;
        if revision != claim.candidate.payout_revision || tip != parent {
            // Another claim may have sent this block before expiring. Landed
            // inactive records and deferred credit survive terminal outbox
            // disposition, so a late acceptance remains reconcilable.
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
        // This verified prepared record precedes the external RPC, so a
        // crash after node acceptance is recoverable by any cluster member.
        self.ledger
            .land_candidate(claim, &self.config.ledger_public_key)
            .await?;
        let (active, revision, tip) = self.observe_candidate(claim).await?;
        if active || revision != claim.candidate.payout_revision || tip != parent {
            self.ledger
                .finish_candidate_at_revision(
                    claim,
                    active,
                    (!active).then_some("parent changed before submission"),
                    revision,
                )
                .await?;
            return Ok(());
        }
        // Renewal failure cancels the attempt even between periodic ticks.
        // Recheck the strictly-live token at the external mutation boundary.
        self.renew_candidate(claim, lease).await?;
        let result = self
            .rpc
            .call_timeout(
                "submitblock",
                json!([claim.candidate.block_hex]),
                Some(self.config.block_submit_timeout),
            )
            .await?;
        // A null response can still describe a known side-chain block; use
        // active-chain evidence before advancing the shared payout state.
        let (active, revision, _) = self.observe_candidate(claim).await?;
        if active {
            self.ledger
                .finish_candidate_at_revision(claim, true, None, revision)
                .await?;
            self.blocks.fetch_add(1, Ordering::Relaxed);
            self.wake.notify_one();
        } else if result.is_string() {
            self.ledger
                .finish_candidate_at_revision(claim, false, result.as_str(), revision)
                .await?;
        } else {
            anyhow::bail!("block submission outcome unresolved");
        }
        Ok(())
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
        let observed = self.observed_tip.read().await.clone();
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
        json!({"schema":"qbit.prism.audit-health.v1","ok":ready,"ready":ready,"status":if ready {"ok"} else {"unavailable"},"backend":"postgres","instance_id":self.config.instance_id,"runtime_workers":self.config.runtime_workers,"tip_poll_age_seconds":poll_age,"accepted_share_count":self.accepted.load(Ordering::Relaxed),"found_block_count":self.blocks.load(Ordering::Relaxed),"template_generation":prepared.as_ref().map(|p|p.generation),"template_age_seconds":prepared.as_ref().map(|p|p.created.elapsed().as_secs_f64()),"observed_tip":observed,"payout_state_generation":prepared.as_ref().map(|p|p.snapshot.payout_revision)})
    }
}

fn header_parent(block_hex: &str) -> Result<String> {
    let bytes = hex::decode(block_hex.get(8..72).context("truncated block header")?)?;
    Ok(hex::encode(bytes.into_iter().rev().collect::<Vec<_>>()))
}

impl MiningBackend for Coordinator {
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

    async fn new_session_id(&self) -> Result<u32, StratumError> {
        self.ledger
            .new_session_id()
            .await
            .map_err(|_| protocol_error("backend-rpc-unavailable", "database unavailable"))
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
            let prepared = self
                .prepared
                .read()
                .await
                .clone()
                .context("no current template")?;
            ensure!(
                self.observed_tip.read().await.as_deref()
                    == prepared.template["previousblockhash"].as_str(),
                "new tip work is pending"
            );
            ensure!(
                self.readiness
                    .read()
                    .await
                    .last_poll
                    .is_some_and(|at| at.elapsed() < self.config.health_timeout),
                "tip polling stale"
            );
            self.ensure_job_fee_current(prepared.fee).await?;
            ensure!(
                self.ledger.payout_revision().await? == prepared.snapshot.payout_revision,
                "payout snapshot stale"
            );
            let bundle = if let Some(bundle) = &prepared.bundle {
                bundle.clone()
            } else {
                Arc::new(
                    self.build_bundle(
                        prepared.snapshot.clone(),
                        prepared.template.clone(),
                        Some(worker.clone()),
                        format!(
                            "{}{}",
                            hex::encode(&self.config.coinbase_tag),
                            "00".repeat(4 + self.config.extranonce2_size)
                        ),
                        prepared.fee,
                    )
                    .await?,
                )
            };
            let id = format!(
                "{}-{}",
                self.config.instance_id,
                uuid::Uuid::new_v4().simple()
            );
            let mut wire = if let Some(base) = &prepared.base_wire {
                base.reassign(id, extranonce1, difficulty, minimum_difficulty)?
            } else {
                let template = prepared.template.clone();
                let bundle = bundle.clone();
                let extra = extranonce1.to_string();
                let extranonce2_size = self.config.extranonce2_size;
                tokio::task::spawn_blocking(move || {
                    let base = codec::Job::from_manifest(
                        "collection".into(),
                        &template,
                        &bundle.signed_coinbase_manifest.manifest,
                        "00000000",
                        extranonce2_size,
                        difficulty,
                        minimum_difficulty,
                        true,
                    )?;
                    base.reassign(id, &extra, difficulty, minimum_difficulty)
                })
                .await??
            };
            wire.refresh_generation = prepared.generation;
            wire.payout_revision = prepared.snapshot.payout_revision;
            Ok::<_, anyhow::Error>(MiningJob {
                wire,
                context: Arc::new(JobContext {
                    prepared,
                    worker: worker.clone(),
                    bundle,
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
        let save = async {
            let now_ms: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&self.ledger.pool)
            .await?;
            let ttl_seconds = ttl.as_secs_f64().ceil() as i64;
            let record = StoredJob {
                prepared_key: job.context.prepared.storage_key.clone(),
                worker: worker.clone(),
                extranonce1: job.wire.extranonce1.clone(),
                extranonce2_size: job.wire.extranonce2_size,
                share_target_hex: job.wire.share_target.to_str_radix(16),
                share_difficulty: job.wire.share_difficulty,
                version_mask,
                expires_at_ms: now_ms
                    .checked_add(ttl_seconds.checked_mul(1000).context("job TTL overflow")?)
                    .context("job expiry overflow")?,
            };
            self.ledger
                .save_job(
                    &job.wire.job_id,
                    &serde_json::to_value(record)?,
                    job.context.prepared.snapshot.payout_revision,
                    &job.wire.previousblockhash,
                    ttl_seconds,
                )
                .await
        };
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
            if job_id.len() > 256 || job_id.starts_with("prepared:") {
                return Ok(None);
            }
            let Some(payload) = self.ledger.job(job_id).await? else {
                return Ok(None);
            };
            let stored: StoredJob = serde_json::from_value(payload)?;
            if stored.worker.username != worker.username
                || stored.worker.payout_address != worker.payout_address
                || stored.worker.p2mr_program_hex != worker.p2mr_program_hex
            {
                return Ok(None);
            }
            let Some(payload) = self.ledger.job(&stored.prepared_key).await? else {
                return Ok(None);
            };
            let prepared: StoredPrepared =
                tokio::task::spawn_blocking(move || serde_json::from_value(payload)).await??;
            let current = self
                .prepared
                .read()
                .await
                .clone()
                .context("no current template")?;
            ensure!(
                self.observed_tip.read().await.as_deref()
                    == current.template["previousblockhash"].as_str(),
                "new tip work is pending"
            );
            let revision = self.ledger.payout_revision().await?;
            if current.template["previousblockhash"] != prepared.template["previousblockhash"]
                || current.snapshot.payout_revision != revision
                || prepared.snapshot.payout_revision != revision
            {
                return Ok(None);
            }
            ensure!(
                self.readiness
                    .read()
                    .await
                    .last_poll
                    .is_some_and(|at| at.elapsed() < self.config.health_timeout),
                "tip polling stale"
            );
            self.ensure_job_fee_current(prepared.fee).await?;
            let bundle = match prepared.bundle.as_ref() {
                Some(bundle) => bundle.clone(),
                None => Arc::new(
                    self.build_bundle(
                        prepared.snapshot.clone(),
                        prepared.template.clone(),
                        Some(stored.worker.clone()),
                        prepared.coinbase_suffix.clone(),
                        prepared.fee,
                    )
                    .await?,
                ),
            };
            let template = prepared.template.clone();
            let wire_bundle = bundle.clone();
            let id = job_id.to_string();
            let extra = stored.extranonce1.clone();
            let n2 = stored.extranonce2_size;
            let target = num_bigint::BigUint::parse_bytes(stored.share_target_hex.as_bytes(), 16)
                .context("invalid stored share target")?;
            ensure!(
                target.bits() > 0 && target.bits() <= 256,
                "stored target out of range"
            );
            ensure!(
                stored.share_difficulty.is_finite() && stored.share_difficulty > 0.0,
                "invalid stored difficulty"
            );
            let mut wire = tokio::task::spawn_blocking(move || {
                let mut wire = codec::Job::from_manifest(
                    id,
                    &template,
                    &wire_bundle.signed_coinbase_manifest.manifest,
                    "00000000",
                    n2,
                    1.0,
                    0.0,
                    false,
                )?;
                ensure!(
                    extra.len() == 8 && hex::decode(&extra)?.len() == 4,
                    "invalid stored extranonce1"
                );
                wire.extranonce1 = extra;
                Ok::<_, anyhow::Error>(wire)
            })
            .await??;
            wire.share_target = target;
            wire.share_difficulty = stored.share_difficulty;
            wire.version_mask = stored.version_mask;
            wire.refresh_generation = prepared.generation;
            wire.payout_revision = prepared.snapshot.payout_revision;
            let now_ms: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&self.ledger.pool)
            .await?;
            if now_ms >= stored.expires_at_ms {
                return Ok(None);
            }
            wire.resume_expires_at = Some(
                Instant::now() + Duration::from_millis((stored.expires_at_ms - now_ms) as u64),
            );
            // The absolute DB expiry is translated once to monotonic time;
            // moving the session between hosts never extends its work lease.
            let prepared = Arc::new(Prepared {
                template: prepared.template,
                snapshot: prepared.snapshot,
                bundle: Some(bundle.clone()),
                base_wire: None,
                storage_key: stored.prepared_key,
                fee: prepared.fee,
                fingerprint: prepared.fingerprint,
                generation: prepared.generation,
                parent_of_tip: prepared.parent_of_tip,
                created: Instant::now(),
            });
            Ok(Some(MiningJob {
                wire,
                context: Arc::new(JobContext {
                    prepared,
                    worker: stored.worker,
                    bundle,
                }),
            }))
        };
        resume.await.map_err(|error| {
            tracing::warn!(%error,"job resume unavailable");
            protocol_error("backend-rpc-unavailable", "job resume unavailable")
        })
    }

    async fn submit(
        &self,
        _worker: &Worker,
        job: &MiningJob<JobContext>,
        submission: codec::Submission,
        stale_grace_eligible: bool,
    ) -> Result<(), StratumError> {
        if !self
            .readiness
            .read()
            .await
            .last_poll
            .is_some_and(|at| at.elapsed() < self.config.health_timeout)
        {
            return Err(protocol_error(
                "backend-rpc-unavailable",
                "current chain state is unavailable",
            ));
        }
        let current = self
            .prepared
            .read()
            .await
            .clone()
            .ok_or_else(|| protocol_error("pool-closed", "no current work"))?;
        if self.observed_tip.read().await.as_deref()
            != current.template["previousblockhash"].as_str()
        {
            return Err(protocol_error("stale-job", "new tip work is pending"));
        }
        let context = &job.context;
        self.ensure_job_fee_current(context.prepared.fee)
            .await
            .map_err(|_| {
                protocol_error("stale-job", "job CTV fee is below the current relay floor")
            })?;
        let revision = self.ledger.payout_revision().await.map_err(|_| {
            protocol_error(
                "backend-rpc-unavailable",
                "current payout state is unavailable",
            )
        })?;
        if current.snapshot.payout_revision != revision {
            return Err(protocol_error("stale-job", "new payout work is pending"));
        }
        let parent_stale =
            current.template["previousblockhash"] != context.prepared.template["previousblockhash"];
        let stale = parent_stale || context.prepared.snapshot.payout_revision != revision;
        if stale
            && !(parent_stale
                && stale_grace_eligible
                && current.parent_of_tip == job.wire.previousblockhash)
        {
            return Err(protocol_error("stale-job", "stale job"));
        }
        if !submission.share_pass && !(submission.block_pass && !stale) {
            return Err(protocol_error("low-difficulty", "low difficulty share"));
        }
        let network = context.bundle.found_block.network_difficulty;
        let difficulty = if submission.share_pass {
            codec::scaled_target_difficulty(&job.wire.share_target)
                .map_err(|_| protocol_error("internal-error", "difficulty overflow"))?
        } else {
            network
        };
        let share = AcceptedShare {
            share_seq: 0,
            share_id: format!("{}:{}", context.worker.username, submission.block_hash_hex),
            miner_id: context.worker.payout_address.clone(),
            order_key: context.worker.payout_address.clone(),
            p2mr_program_hex: context.worker.p2mr_program_hex.clone(),
            share_difficulty: difficulty,
            network_difficulty: network,
            template_height: template_parent_height(context.bundle.found_block.block_height)
                .map_err(|_| protocol_error("internal-error", "invalid candidate block height"))?,
            job_id: job.wire.job_id.clone(),
            job_issued_at_ms: context.prepared.snapshot.anchor_ms,
            accepted_at_ms: 0,
            ntime: submission.ntime,
            credit_policy: stale.then(|| "stale-grace".into()),
        };
        let save = async {
            let candidate = if submission.block_pass && !stale {
                let original = context
                    .bundle
                    .coinbase_script_sig_suffix_hex
                    .as_ref()
                    .context("job coinbase suffix missing")?;
                let placeholder_length = (4 + job.wire.extranonce2_size) * 2;
                let prefix = original
                    .get(
                        ..original
                            .len()
                            .checked_sub(placeholder_length)
                            .context("job coinbase suffix too short")?,
                    )
                    .context("invalid job suffix")?;
                let suffix = format!(
                    "{prefix}{}{}",
                    job.wire.extranonce1, submission.extranonce2_hex
                );
                Some(Candidate {
                    block_hash: submission.block_hash_hex.clone(),
                    block_hex: submission.block_hex,
                    job_id: job.wire.job_id.clone(),
                    payout_revision: context.prepared.snapshot.payout_revision,
                    bundle: (*context.bundle).clone(),
                    coinbase_suffix_hex: Some(suffix),
                    deferred_share: (!submission.share_pass).then(|| share.clone()),
                })
            } else {
                None
            };
            if submission.share_pass {
                let result = self
                    .ledger
                    .append_at_revision(share, candidate, revision)
                    .await?;
                Ok::<bool, anyhow::Error>(result.inserted)
            } else {
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1)",
                )
                .bind(&share.share_id)
                .fetch_one(&self.ledger.pool)
                .await?;
                if exists {
                    return Ok(false);
                }
                if !self
                    .ledger
                    .enqueue_candidate_once(candidate.context("missing candidate")?)
                    .await?
                {
                    return Ok(false);
                }
                // A block below the advertised share target earns only proven
                // network work, and only after active-chain confirmation.
                loop {
                    // Observe credit and disposition in one MVCC snapshot so
                    // finalization cannot fall between two separate reads.
                    let (credited,state): (bool,Option<String>) = sqlx::query_as(
                        "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1), (SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$2)",
                    )
                    .bind(&share.share_id)
                    .bind(&submission.block_hash_hex)
                    .fetch_one(&self.ledger.pool)
                    .await?;
                    if credited {
                        break Ok(true);
                    }
                    ensure!(
                        state.as_deref() == Some("pending"),
                        "block-only proof was not accepted on the active chain"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        let save = tokio::time::timeout(self.config.share_commit_timeout, save)
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("share confirmation deadline exceeded")));
        match save {
            Ok(true) => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
                self.metrics.record_share_accepted(stale);
                if current.bundle.is_none() {
                    self.wake.notify_one();
                }
                Ok(())
            }
            Ok(false) => Err(protocol_error("duplicate-share", "duplicate share")),
            Err(error) => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                if error.to_string().contains("duplicate-share")
                    || error.to_string().contains("duplicate share_id")
                {
                    return Err(protocol_error("duplicate-share", "duplicate share"));
                }
                tracing::warn!(%error,"share persistence failed");
                Err(protocol_error(
                    "ledger-confirmation-failed",
                    "share was not confirmed by the database",
                ))
            }
        }
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

use anyhow::{bail, ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    CoinbaseOutputPolicy, FanoutFeeRatePolicy, PayoutPolicy, PoolFeePolicy, SettlementModeConfig,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{env, time::Duration};

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub instance_id: String,
    pub database_connections: u32,
    pub initialize_schema: bool,
    pub chain: String,
    pub expected_genesis_hash: Option<String>,
    pub min_peers: u64,
    pub template_max_age: Duration,
    pub submit_tip_max_age: Duration,
    pub template_refresh_failure_exit: Duration,
    pub rpc_url: String,
    pub rpc_user: String,
    pub rpc_password: String,
    pub rpc_timeout: Duration,
    pub block_submit_timeout: Duration,
    pub poll_interval: Duration,
    pub blockwait: bool,
    pub build_workers: usize,
    pub runtime_workers: usize,
    pub snapshot_interval: Duration,
    pub health_timeout: Duration,
    pub share_commit_timeout: Duration,
    pub extranonce2_size: usize,
    pub coinbase_tag: String,
    pub manifest_seed: String,
    pub ledger_seed: String,
    pub ledger_public_key: String,
    pub username_fallback: Option<String>,
    pub payout_policy: PayoutPolicy,
    pub fee_address: Option<String>,
    pub ctv_enabled: bool,
    pub ctv_config: SettlementModeConfig,
    pub ctv_direct_floor: u64,
    pub ctv_fee: Option<FanoutFeeRatePolicy>,
    pub ctv_fee_premium_bps: u64,
    pub ctv_broadcast: bool,
    pub ctv_broadcast_interval: Duration,
    pub version_mask: u32,
    pub audit_bind: String,
    pub audit_port: u16,
}

pub fn value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}
pub fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|s| !s.trim().is_empty())
}
pub(crate) fn authority_host(host: &str) -> String {
    if host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{host}]")
    } else {
        host.into()
    }
}
/// Keep the independent public reader and mining coordinator on the same
/// node unless the operator explicitly overrides their process environment.
pub(crate) fn rpc_connection_from_env() -> (String, String, String) {
    let endpoint = optional("QBIT_RPC_URL").unwrap_or_else(|| {
        format!(
            "http://{}:{}/",
            authority_host(&value("QBIT_RPC_HOST", "127.0.0.1")),
            value("QBIT_RPC_PORT", "18452")
        )
    });
    (
        endpoint,
        value("QBIT_RPC_USER", "qbit"),
        value("QBIT_RPC_PASSWORD", "change-this"),
    )
}
pub fn flag(name: &str, default: bool) -> Result<bool> {
    match optional(name)
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None => Ok(default),
        Some("1" | "true" | "yes" | "on") => Ok(true),
        Some("0" | "false" | "no" | "off") => Ok(false),
        _ => bail!("{name} must be a boolean"),
    }
}
pub fn number<T: std::str::FromStr>(name: &str, default: T) -> Result<T> {
    match optional(name) {
        Some(s) => s.parse().map_err(|_| anyhow::anyhow!("invalid {name}")),
        None => Ok(default),
    }
}
fn positive(name: &str, default: u64) -> Result<u64> {
    let n = number(name, default)?;
    ensure!(n > 0, "{name} must be positive");
    Ok(n)
}
fn genesis_pin(chain: &str, pin: Option<String>) -> Result<Option<String>> {
    ensure!(
        !matches!(chain, "main" | "mainnet") || pin.is_some(),
        "QBIT_EXPECTED_GENESIS_HASH is required on mainnet"
    );
    pin.map(|pin| {
        ensure!(
            pin.len() == 64 && pin.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "QBIT_EXPECTED_GENESIS_HASH must be exactly 64 hexadecimal characters"
        );
        Ok(pin.to_ascii_lowercase())
    })
    .transpose()
}
fn bounded_usize(name: &str, default: usize, minimum: usize, maximum: usize) -> Result<usize> {
    let value = number::<u64>(
        name,
        u64::try_from(default).context("default budget exceeds uint64")?,
    )?;
    let value =
        usize::try_from(value).with_context(|| format!("{name} exceeds platform capacity"))?;
    ensure!(
        (minimum..=maximum).contains(&value),
        "{name} must be {minimum}..{maximum}"
    );
    Ok(value)
}
fn alias(primary: &str, legacy: &str, default: u64) -> Result<u64> {
    if optional(primary).is_some() {
        positive(primary, default)
    } else {
        positive(legacy, default)
    }
}
fn seconds(name: &str, default: f64) -> Result<Duration> {
    let n = number(name, default)?;
    ensure!(
        n.is_finite() && n > 0.0 && n <= 86400.0,
        "{name} must be between 0 and 86400 seconds"
    );
    Ok(Duration::from_secs_f64(n))
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let requested_chain = value("QBIT_CHAIN", "regtest").trim().to_ascii_lowercase();
        let chain = match requested_chain.as_str() {
            "main" | "mainnet" => "mainnet",
            "test" | "testnet" | "testnet3" => "testnet",
            "testnet4" => "testnet4",
            "signet" => "signet",
            "regtest" => "regtest",
            _ => bail!("QBIT_CHAIN must name mainnet, testnet, testnet4, signet, or regtest"),
        }
        .to_string();
        let production = matches!(chain.as_str(), "main" | "mainnet")
            || flag("QBIT_PRODUCTION", false)?
            || flag("QBIT_TOOLS_PRODUCTION", false)?;
        if production {
            ensure!(
                chain != "regtest",
                "production mode rejects regtest QBIT_CHAIN"
            );
            for name in [
                "PRISM_ALLOW_MEMORY_LEDGER",
                "PRISM_ALLOW_TEST_SIGNING_SEEDS",
                "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY",
                "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN",
            ] {
                ensure!(!flag(name, false)?, "production rejects {name}");
            }
            for name in [
                "PRISM_STRATUM_SHARE_DIFF",
                "PRISM_STRATUM_VARDIFF_MIN_DIFF",
                "PRISM_STRATUM_VARDIFF_START_DIFF",
                "PRISM_STRATUM_VARDIFF_MAX_DIFF",
            ] {
                let n: f64 = optional(name)
                    .with_context(|| format!("production requires {name}"))?
                    .parse()
                    .with_context(|| format!("invalid {name}"))?;
                ensure!(
                    n.is_finite() && n > 0.0 && n != 1e-9,
                    "production requires a non-lab {name}"
                );
            }
            ensure!(
                number("PRISM_STRATUM_MAX_CONNECTIONS", 384u64)? > 0,
                "production requires a connection limit"
            );
            ensure!(
                number("PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS", 30f64)? > 0.0,
                "production requires initial-job timeout"
            );
        }
        let expected_genesis_hash = genesis_pin(&chain, optional("QBIT_EXPECTED_GENESIS_HASH"))?;
        let min_peers = positive("PRISM_MIN_PEERS", 1)?;
        let submit_tip_max_age_seconds = number("PRISM_SUBMIT_TIP_MAX_AGE_SECONDS", 10.0f64)?;
        let submit_tip_max_age = Duration::try_from_secs_f64(submit_tip_max_age_seconds)
            .context("PRISM_SUBMIT_TIP_MAX_AGE_SECONDS must be finite and nonnegative")?;
        let template_refresh_failure_exit = Duration::try_from_secs_f64(number(
            "PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS",
            120.0f64,
        )?)
        .context("PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS must be finite and nonnegative")?;
        ensure!(
            !production || !template_refresh_failure_exit.is_zero(),
            "production mode requires a positive PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS"
        );
        let template_max_age_seconds = number("PRISM_TEMPLATE_MAX_AGE_SECONDS", 120u64)?;
        ensure!(
            template_max_age_seconds <= 86400,
            "PRISM_TEMPLATE_MAX_AGE_SECONDS must be 0..86400"
        );
        ensure!(
            !flag("PRISM_ALLOW_MEMORY_LEDGER", false)?,
            "PRISM_ALLOW_MEMORY_LEDGER is retired; Rust PRISM requires PostgreSQL"
        );
        ensure!(!flag("PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN",false)? && optional("PRISM_LEDGER_WRITER_SESSION_TOKEN").is_none(),
            "fixed ledger writer sessions are retired; unset PRISM_LEDGER_WRITER_SESSION_TOKEN and PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN");
        let database_url = optional("PRISM_DATABASE_URL").context(
            "PRISM_DATABASE_URL is required (Rust PRISM uses PostgreSQL for every instance)",
        )?;
        let parsed_database =
            url::Url::parse(&database_url).context("invalid PRISM_DATABASE_URL")?;
        ensure!(
            matches!(parsed_database.scheme(), "postgres" | "postgresql"),
            "PRISM_DATABASE_URL must use postgres or postgresql"
        );
        let allow_test = flag("PRISM_ALLOW_TEST_SIGNING_SEEDS", false)? && !production;
        let seed = |name: &str, byte: &str| -> Result<String> {
            let s = optional(name)
                .or_else(|| allow_test.then(|| byte.repeat(32)))
                .with_context(|| format!("{name} is required"))?;
            ManifestSigningKey::from_seed_hex(&s).with_context(|| format!("invalid {name}"))?;
            if !allow_test {
                ensure!(
                    !["11", "22", "42", "43"]
                        .iter()
                        .any(|byte| s.eq_ignore_ascii_case(&byte.repeat(32))),
                    "test signing seeds require PRISM_ALLOW_TEST_SIGNING_SEEDS=1"
                );
            }
            Ok(s)
        };
        let manifest_seed = seed("PRISM_MANIFEST_SIGNING_SEED_HEX", "11")?;
        let ledger_seed = seed("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", "22")?;
        let manifest_key = ManifestSigningKey::from_seed_hex(&manifest_seed)?;
        let ledger_key = ManifestSigningKey::from_seed_hex(&ledger_seed)?;
        ensure!(
            manifest_key.public_key_hex() != ledger_key.public_key_hex(),
            "manifest and ledger signing keys must differ"
        );
        let ledger_public_key = optional("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX")
            .or_else(|| allow_test.then(|| ledger_key.public_key_hex()))
            .context("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX is required")?
            .to_lowercase();
        ensure!(
            ledger_public_key == ledger_key.public_key_hex(),
            "trusted ledger public key does not match signing seed"
        );
        let coinbase_tag = value("PRISM_COINBASE_TAG", "/PRISM/");
        ensure!(
            coinbase_tag.len() <= 40 && coinbase_tag.bytes().all(|b| (0x20..=0x7e).contains(&b)),
            "PRISM_COINBASE_TAG must be at most 40 printable ASCII bytes"
        );
        let extranonce2_size = bounded_usize("PRISM_STRATUM_EXTRANONCE2_SIZE", 8, 1, 32)?;
        let mut payout_policy = PayoutPolicy::day_one_default();
        payout_policy.p2mr_spend_input_bytes =
            positive("PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES", 3680)?;
        payout_policy.target_feerate_sats_per_byte = alias(
            "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE",
            "PRISM_PAYOUT_TARGET_FEERATE_SATS_PER_BYTE",
            1,
        )?;
        payout_policy.safety_multiplier = positive("PRISM_PAYOUT_SAFETY_MULTIPLIER", 4)?;
        if optional("PRISM_PAYOUT_MIN_OUTPUT_BITS")
            .or_else(|| optional("PRISM_PAYOUT_MIN_OUTPUT_SATS"))
            .is_some()
        {
            payout_policy.min_output_sats = Some(alias(
                "PRISM_PAYOUT_MIN_OUTPUT_BITS",
                "PRISM_PAYOUT_MIN_OUTPUT_SATS",
                14720,
            )?);
        }
        let fee_address = optional("PRISM_POOL_FEE_ADDRESS");
        let fee_program = optional("PRISM_POOL_FEE_P2MR_PROGRAM_HEX");
        if flag("PRISM_POOL_FEE_ENABLED", false)? {
            ensure!(
                fee_address.is_some() != fee_program.is_some(),
                "configure exactly one pool fee address or P2MR program"
            );
            let fee_bps = number("PRISM_POOL_FEE_BPS", 0u16)?;
            ensure!(fee_bps <= 10_000, "pool fee cannot exceed 10000 bps");
            let recipient_id = fee_address
                .clone()
                .or_else(|| optional("PRISM_POOL_FEE_RECIPIENT_ID"))
                .context("pool fee recipient ID is required")?;
            let p2mr_program_hex = fee_program.unwrap_or_default();
            if !p2mr_program_hex.is_empty() {
                ensure!(
                    hex::decode(&p2mr_program_hex)?.len() == 32,
                    "pool fee program must be 32 bytes"
                );
            }
            payout_policy.pool_fee_policy = Some(PoolFeePolicy {
                fee_bps,
                order_key: optional("PRISM_POOL_FEE_ORDER_KEY")
                    .unwrap_or_else(|| recipient_id.clone()),
                recipient_id,
                p2mr_program_hex,
            });
        } else {
            ensure!(
                fee_address.is_none()
                    && fee_program.is_none()
                    && optional("PRISM_POOL_FEE_BPS").is_none(),
                "pool fee configuration requires PRISM_POOL_FEE_ENABLED=1"
            );
        }
        payout_policy.coinbase_output_policy =
            match value("PRISM_COINBASE_OUTPUT_POLICY", "canonical").as_str() {
                "canonical" => CoinbaseOutputPolicy::Canonical,
                "pool-fee-first" if payout_policy.pool_fee_policy.is_some() => {
                    CoinbaseOutputPolicy::PoolFeeFirst
                }
                _ => bail!("invalid PRISM_COINBASE_OUTPUT_POLICY or missing pool fee"),
            };
        payout_policy.min_output_sats()?;
        let ctv_enabled = flag("PRISM_CTV_SETTLEMENT_ENABLED", false)?;
        let ctv_fee_premium_bps = positive("PRISM_CTV_FANOUT_FEE_PREMIUM_BPS", 12000)?;
        let ctv_fee = if optional("PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT")
            .or_else(|| optional("PRISM_CTV_FANOUT_FEE_MARKET_RATE_SATS_PER_1000_WEIGHT"))
            .is_some()
        {
            Some(FanoutFeeRatePolicy::new(
                alias(
                    "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                    "PRISM_CTV_FANOUT_FEE_MARKET_RATE_SATS_PER_1000_WEIGHT",
                    1000,
                )?,
                ctv_fee_premium_bps,
            ))
        } else {
            None
        };
        ensure!(
            !(ctv_enabled && matches!(chain.as_str(), "main" | "mainnet") && ctv_fee.is_none()),
            "mainnet CTV requires an explicit market fee rate"
        );
        let (rpc_url, rpc_user, rpc_password) = rpc_connection_from_env();
        if production {
            ensure!(
                !database_url.contains("change-this")
                    && rpc_password != "change-this"
                    && !rpc_password.is_empty(),
                "production requires non-default database and RPC credentials"
            );
        }
        let runtime_workers = bounded_usize(
            "PRISM_RUNTIME_WORKERS",
            std::thread::available_parallelism().map_or(2, usize::from),
            1,
            1024,
        )?;
        let database_connections = u32::try_from(bounded_usize(
            "PRISM_DATABASE_MAX_CONNECTIONS",
            16,
            4,
            1024,
        )?)
        .context("database connection budget exceeds uint32")?;
        let build_workers = bounded_usize(
            "PRISM_JOB_BUILD_EXECUTOR_WORKERS",
            runtime_workers.min(4),
            1,
            runtime_workers + 8,
        )?;
        let ctv_config = SettlementModeConfig {
            max_coinbase_settlement_outputs: bounded_usize(
                "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS",
                16,
                1,
                qbit_prism::MAX_COINBASE_SETTLEMENT_OUTPUTS,
            )?,
            max_direct_coinbase_outputs: bounded_usize(
                "PRISM_MAX_DIRECT_COINBASE_OUTPUTS",
                12,
                0,
                qbit_prism::MAX_DIRECT_COINBASE_OUTPUTS,
            )?,
            max_fanout_recipients_per_transaction: bounded_usize(
                "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION",
                1000,
                1,
                qbit_prism::MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION,
            )?,
            reserved_coinbase_outputs: bounded_usize(
                "PRISM_RESERVED_COINBASE_OUTPUTS",
                0,
                0,
                qbit_prism::MAX_COINBASE_SETTLEMENT_OUTPUTS,
            )?,
        };
        ensure!(
            ctv_config.reserved_coinbase_outputs < ctv_config.max_coinbase_settlement_outputs,
            "reserved coinbase outputs leave no settlement capacity"
        );
        let instance_id =
            optional("PRISM_INSTANCE_ID").unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        ensure!(instance_id.len()<=128 && !instance_id.chars().any(char::is_control) && !instance_id.starts_with("prepared:"),"PRISM_INSTANCE_ID must be at most 128 characters, without controls or the reserved prepared: prefix");
        let parsed_rpc = url::Url::parse(&rpc_url)
            .context("invalid QBIT_RPC_URL or QBIT_RPC_HOST/QBIT_RPC_PORT")?;
        ensure!(
            matches!(parsed_rpc.scheme(), "http" | "https") && parsed_rpc.host().is_some(),
            "qbit RPC URL must use http or https and include a host"
        );
        let version_mask = u32::from_str_radix(
            value("PRISM_VERSION_ROLLING_MASK", "1fffe000").trim_start_matches("0x"),
            16,
        )
        .context("invalid PRISM_VERSION_ROLLING_MASK")?;
        Ok(Self {
            database_url,
            instance_id,
            database_connections,
            initialize_schema: flag("PRISM_POSTGRES_INIT_SCHEMA", false)?,
            chain: chain.clone(),
            expected_genesis_hash,
            min_peers,
            template_max_age: Duration::from_secs(template_max_age_seconds),
            submit_tip_max_age,
            template_refresh_failure_exit,
            rpc_url,
            rpc_user,
            rpc_password,
            rpc_timeout: seconds("PRISM_RPC_TIMEOUT_SECONDS", 15.0)?,
            block_submit_timeout: seconds("PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS", 1.0)?,
            poll_interval: seconds("PRISM_BLOCKPOLL_SECONDS", 2.0)?,
            blockwait: flag("PRISM_BLOCKWAIT_ENABLED", true)?,
            runtime_workers,
            build_workers,
            snapshot_interval: seconds("PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS", 60.0)?,
            health_timeout: seconds("PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS", 15.0)?,
            share_commit_timeout: seconds("PRISM_SHARE_COMMIT_TIMEOUT_SECONDS", 15.0)?,
            extranonce2_size,
            coinbase_tag,
            manifest_seed,
            ledger_seed,
            ledger_public_key,
            username_fallback: optional("PRISM_USERNAME_FALLBACK_ADDRESS").or_else(|| {
                matches!(
                    chain.as_str(),
                    "testnet" | "testnet3" | "testnet4" | "signet"
                )
                .then(|| "tq1zlsq9dpxz8mennhdpr9nf9s0f2tjtq6gxs9m84k6xglhkfp92q2zszzu4m3".into())
            }),
            payout_policy,
            fee_address,
            ctv_enabled,
            ctv_fee,
            ctv_fee_premium_bps,
            ctv_config,
            ctv_direct_floor: alias(
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_SATS",
                10_485_760,
            )?,
            ctv_broadcast: flag("PRISM_CTV_BROADCASTER_ENABLED", false)?,
            ctv_broadcast_interval: seconds("PRISM_CTV_BROADCASTER_POLL_SECONDS", 10.0)?,
            version_mask,
            audit_bind: value("PRISM_AUDIT_BIND", "127.0.0.1"),
            audit_port: number("PRISM_AUDIT_PORT", 3341u16)?,
        })
    }

    pub(crate) fn verify_genesis(&self, actual: &str) -> Result<()> {
        if let Some(expected) = genesis_pin(&self.chain, self.expected_genesis_hash.clone())? {
            ensure!(
                actual.eq_ignore_ascii_case(&expected),
                "qbit genesis hash differs from QBIT_EXPECTED_GENESIS_HASH"
            );
        }
        Ok(())
    }

    /// Every node in a cluster must construct the same payouts and attestations.
    /// Credentials and local resource limits are intentionally absent.
    pub fn fingerprint(&self, genesis: &str) -> Result<String> {
        let mut policy = json!({
            "schema":2,"genesis":genesis,"ledger_key":self.ledger_public_key,
            "manifest_key":ManifestSigningKey::from_seed_hex(&self.manifest_seed)?.public_key_hex(),
            "username_fallback":self.username_fallback,
            "payout_policy":self.payout_policy,"ctv_enabled":self.ctv_enabled,"ctv_config":self.ctv_config,
            "ctv_direct_floor":self.ctv_direct_floor,"ctv_fee":self.ctv_fee,
            "window_multiplier":qbit_prism::PRISM_WINDOW_MULTIPLIER
        });
        // Explicit rates already bind their premium in ctv_fee. Bind the
        // separate automatic policy only when it can affect a constructed payout.
        if self.ctv_enabled && self.ctv_fee.is_none() {
            policy["ctv_auto_fee_premium_bps"] = json!(self.ctv_fee_premium_bps);
        }
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&policy)?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn automatic_ctv_config() -> Config {
        Config {
            database_url: "postgresql://localhost/prism".into(),
            instance_id: "frontend-a".into(),
            database_connections: 4,
            initialize_schema: false,
            chain: "regtest".into(),
            expected_genesis_hash: None,
            min_peers: 1,
            template_max_age: Duration::from_secs(120),
            submit_tip_max_age: Duration::from_secs(10),
            template_refresh_failure_exit: Duration::from_secs(120),
            rpc_url: "http://127.0.0.1:18452/".into(),
            rpc_user: "operator".into(),
            rpc_password: "test-only".into(),
            rpc_timeout: Duration::from_secs(15),
            block_submit_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_secs(2),
            blockwait: true,
            build_workers: 2,
            runtime_workers: 2,
            snapshot_interval: Duration::from_secs(60),
            health_timeout: Duration::from_secs(15),
            share_commit_timeout: Duration::from_secs(15),
            extranonce2_size: 8,
            coinbase_tag: "/PRISM/".into(),
            manifest_seed: "11".repeat(32),
            ledger_seed: "22".repeat(32),
            ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))
                .unwrap()
                .public_key_hex(),
            username_fallback: None,
            payout_policy: PayoutPolicy::day_one_default(),
            fee_address: None,
            ctv_enabled: true,
            ctv_config: SettlementModeConfig::default(),
            ctv_direct_floor: 10_485_760,
            ctv_fee: None,
            ctv_fee_premium_bps: 12000,
            ctv_broadcast: false,
            ctv_broadcast_interval: Duration::from_secs(10),
            version_mask: 0x1fffe000,
            audit_bind: "127.0.0.1".into(),
            audit_port: 3341,
        }
    }

    #[test]
    fn automatic_ctv_fee_premium_binds_the_shared_configuration() {
        let first = automatic_ctv_config();
        let mut second = first.clone();
        second.instance_id = "frontend-b".into();
        second.database_connections = 16;
        second.runtime_workers = 4;
        second.rpc_password = "different-local-credential".into();
        assert_eq!(
            first.fingerprint("genesis").unwrap(),
            second.fingerprint("genesis").unwrap(),
            "local resources and credentials must not split the pool"
        );
        second.ctv_fee_premium_bps = 15000;
        assert_ne!(
            first.fingerprint("genesis").unwrap(),
            second.fingerprint("genesis").unwrap(),
            "automatic fee premiums change immutable payouts"
        );
    }

    #[test]
    fn mainnet_genesis_pin_is_required_and_matches_the_node() {
        let mut config = automatic_ctv_config();
        config.chain = "mainnet".into();
        let actual = "ab".repeat(32);
        assert!(config
            .verify_genesis(&actual)
            .unwrap_err()
            .to_string()
            .contains("required on mainnet"));
        config.expected_genesis_hash = Some("ab".repeat(31));
        assert!(config.verify_genesis(&actual).is_err());
        config.expected_genesis_hash = Some("AB".repeat(32));
        config.verify_genesis(&actual).unwrap();
        assert!(config
            .verify_genesis(&"cd".repeat(32))
            .unwrap_err()
            .to_string()
            .contains("differs"));
        config.chain = "regtest".into();
        assert!(
            config.verify_genesis(&"cd".repeat(32)).is_err(),
            "optional pins still protect non-mainnet chains"
        );
        config.expected_genesis_hash = None;
        config.verify_genesis(&actual).unwrap();
    }

    #[test]
    fn explicit_ctv_policy_and_disabled_ctv_keep_their_effective_fingerprint() {
        let mut first = automatic_ctv_config();
        first.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
        let mut second = first.clone();
        second.ctv_fee_premium_bps = 15000;
        assert_eq!(
            first.fingerprint("genesis").unwrap(),
            second.fingerprint("genesis").unwrap(),
            "explicit fee policy already contains the effective premium"
        );
        second.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 15000));
        assert_ne!(
            first.fingerprint("genesis").unwrap(),
            second.fingerprint("genesis").unwrap()
        );
        first.ctv_enabled = false;
        first.ctv_fee = None;
        second = first.clone();
        second.ctv_fee_premium_bps = 15000;
        assert_eq!(
            first.fingerprint("genesis").unwrap(),
            second.fingerprint("genesis").unwrap(),
            "unused automatic fee policy must not change a non-CTV cluster"
        );
    }

    #[test]
    fn effective_username_fallback_is_shared_policy_even_without_ctv() {
        let mut first = automatic_ctv_config();
        first.ctv_enabled = false;
        first.username_fallback =
            Some("tq1zlsq9dpxz8mennhdpr9nf9s0f2tjtq6gxs9m84k6xglhkfp92q2zszzu4m3".into());
        let mut second = first.clone();
        second.username_fallback = None;
        assert_ne!(
            first.fingerprint("genesis").unwrap(),
            second.fingerprint("genesis").unwrap(),
            "rejecting an alias and redirecting it to a fallback are different policies"
        );
        second.username_fallback = Some("another-payout-address".into());
        assert_ne!(
            first.fingerprint("genesis").unwrap(),
            second.fingerprint("genesis").unwrap(),
            "an alias must not select its payout recipient by frontend"
        );
    }
}

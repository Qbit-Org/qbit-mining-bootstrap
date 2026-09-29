//! #535: every server refuses to run, check or self-check its configuration
//! without a pool fee, in either settlement mode. Tests that launch one run
//! this 0-bps fee, which pays nothing and adds no output until dust below
//! the payout floor must be swept.

pub const ZERO_BPS_POOL_FEE: [(&str, &str); 4] = [
    ("PRISM_POOL_FEE_ENABLED", "1"),
    ("PRISM_POOL_FEE_BPS", "0"),
    ("PRISM_POOL_FEE_RECIPIENT_ID", "pool-fee"),
    (
        "PRISM_POOL_FEE_P2MR_PROGRAM_HEX",
        "fefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe",
    ),
];

/// `ZERO_BPS_POOL_FEE` as the payout policy the server reads from it, for a
/// fixture that pins the cluster's configuration in process.
#[allow(dead_code)]
pub fn zero_bps_policy() -> qbit_prism::PoolFeePolicy {
    qbit_prism::PoolFeePolicy {
        fee_bps: 0,
        recipient_id: ZERO_BPS_POOL_FEE[2].1.into(),
        order_key: ZERO_BPS_POOL_FEE[2].1.into(),
        p2mr_program_hex: ZERO_BPS_POOL_FEE[3].1.into(),
    }
}

/// The 0-bps fee, unless `names` configure a pool fee of their own: an
/// address beside the default program, or a disabled fee beside its rate,
/// would be refused as a configuration error rather than tested.
#[allow(dead_code)]
pub fn default_pool_fee<'a>(
    mut names: impl Iterator<Item = &'a str>,
) -> &'static [(&'static str, &'static str)] {
    if names.any(|name| name.starts_with("PRISM_POOL_FEE_")) {
        &[]
    } else {
        &ZERO_BPS_POOL_FEE
    }
}

use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle_with_coinbase_options, build_audit_bundle_with_ctv_settlement_options,
    AcceptedShare, AuditBundle, CarryForwardBalance, FanoutFeeRatePolicy, FoundBlock, PayoutPolicy,
    SettlementModeConfig,
};
use serde::Deserialize;
use std::io::{self, Read};
use std::{env, error::Error, fs, process};

#[derive(Debug, Deserialize)]
struct BuildAuditBundleInput {
    shares: Vec<AcceptedShare>,
    found_block: FoundBlock,
    #[serde(default)]
    prior_balances: Vec<CarryForwardBalance>,
    #[serde(default)]
    payout_policy: Option<PayoutPolicy>,
    #[serde(default)]
    coinbase_script_sig_suffix_hex: Option<String>,
    #[serde(default)]
    witness_merkle_leaves_hex: Vec<String>,
    #[serde(default)]
    ctv_settlement: Option<CtvSettlementInput>,
}

#[derive(Debug, Deserialize)]
struct CtvSettlementInput {
    direct_floor_sats: u64,
    config: SettlementModeConfig,
    #[serde(default)]
    fanout_fee_rate_policy: Option<FanoutFeeRatePolicy>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("qbit-prism-build-audit-bundle: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut input_path: Option<String> = None;
    let mut signing_key_seed_hex: Option<String> = None;
    let mut ledger_signing_key_seed_hex: Option<String> = None;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input_path = args.next(),
            "--signing-key-seed-hex" => signing_key_seed_hex = args.next(),
            "--ledger-signing-key-seed-hex" => ledger_signing_key_seed_hex = args.next(),
            "-h" | "--help" => {
                print_usage();
                return Ok(());
            }
            _ => return Err(format!("unexpected argument: {arg}").into()),
        }
    }

    let input_json = match input_path.as_deref() {
        Some("-") | None => {
            let mut buffer = String::new();
            io::stdin().read_to_string(&mut buffer)?;
            buffer
        }
        Some(path) => fs::read_to_string(path)?,
    };
    let input: BuildAuditBundleInput = serde_json::from_str(&input_json)?;
    let signing_key_seed_hex = signing_key_seed_hex.ok_or("--signing-key-seed-hex is required")?;
    let ledger_signing_key_seed_hex =
        ledger_signing_key_seed_hex.ok_or("--ledger-signing-key-seed-hex is required")?;
    let signing_key = ManifestSigningKey::from_seed_hex(&signing_key_seed_hex)?;
    let ledger_signing_key = ManifestSigningKey::from_seed_hex(&ledger_signing_key_seed_hex)?;
    let payout_policy = input
        .payout_policy
        .unwrap_or_else(PayoutPolicy::day_one_default);
    let bundle: AuditBundle = if let Some(ctv_settlement) = input.ctv_settlement {
        build_audit_bundle_with_ctv_settlement_options(
            input.shares,
            input.found_block,
            input.prior_balances,
            payout_policy,
            ctv_settlement.direct_floor_sats,
            ctv_settlement.config,
            ctv_settlement.fanout_fee_rate_policy,
            input.coinbase_script_sig_suffix_hex,
            input.witness_merkle_leaves_hex,
            &signing_key,
            &ledger_signing_key,
        )?
    } else {
        build_audit_bundle_with_coinbase_options(
            input.shares,
            input.found_block,
            input.prior_balances,
            payout_policy,
            input.coinbase_script_sig_suffix_hex,
            input.witness_merkle_leaves_hex,
            &signing_key,
            &ledger_signing_key,
        )?
    };

    println!("{}", serde_json::to_string_pretty(&bundle)?);
    Ok(())
}

fn print_usage() {
    println!(
        "usage: qbit-prism-build-audit-bundle --signing-key-seed-hex <64 hex chars> --ledger-signing-key-seed-hex <64 hex chars> [--input <bundle-input.json|-]"
    );
}

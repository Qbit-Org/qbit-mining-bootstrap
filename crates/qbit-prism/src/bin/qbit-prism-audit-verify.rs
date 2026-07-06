use qbit_prism::{
    load_audit_bundle_from_path, verify_audit_bundle_against_coinbase_tx_hex,
    verify_audit_bundle_against_coinbase_tx_hex_and_expected_coinbase_value,
};
use std::{env, error::Error, process};

fn main() {
    if let Err(error) = run() {
        eprintln!("qbit-prism-audit-verify: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut bundle_path = None;
    let mut coinbase_tx_hex = None;
    let mut expected_coinbase_value_sats = None;
    let mut ledger_writer_public_key_hex = None;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                return Ok(());
            }
            "--coinbase-tx-hex" => {
                coinbase_tx_hex = Some(
                    args.next()
                        .ok_or("--coinbase-tx-hex requires a hex argument")?,
                );
            }
            "--expected-coinbase-value-sats" => {
                expected_coinbase_value_sats = Some(
                    args.next()
                        .ok_or("--expected-coinbase-value-sats requires a sats argument")?
                        .parse::<u64>()?,
                );
            }
            "--ledger-writer-public-key-hex" => {
                ledger_writer_public_key_hex = Some(
                    args.next()
                        .ok_or("--ledger-writer-public-key-hex requires a hex argument")?,
                );
            }
            _ if bundle_path.is_none() => bundle_path = Some(arg),
            _ => return Err(format!("unexpected argument: {arg}").into()),
        }
    }

    let bundle_path = bundle_path.ok_or("missing audit bundle JSON path")?;
    let coinbase_tx_hex = coinbase_tx_hex.ok_or("--coinbase-tx-hex is required")?;
    let ledger_writer_public_key_hex =
        ledger_writer_public_key_hex.ok_or("--ledger-writer-public-key-hex is required")?;
    let bundle = load_audit_bundle_from_path(bundle_path)?;
    let report = if let Some(expected_coinbase_value_sats) = expected_coinbase_value_sats {
        verify_audit_bundle_against_coinbase_tx_hex_and_expected_coinbase_value(
            &bundle,
            &coinbase_tx_hex,
            &ledger_writer_public_key_hex,
            expected_coinbase_value_sats,
        )?
    } else {
        verify_audit_bundle_against_coinbase_tx_hex(
            &bundle,
            &coinbase_tx_hex,
            &ledger_writer_public_key_hex,
        )?
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn print_usage() {
    println!(
        "usage: qbit-prism-audit-verify <audit-bundle.json> --coinbase-tx-hex <raw-tx-hex> --ledger-writer-public-key-hex <64 hex chars> [--expected-coinbase-value-sats <sats>]"
    );
}

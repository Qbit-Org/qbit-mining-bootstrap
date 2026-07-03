use qbit_prism::{
    build_maturity_entries, current_carry_forward_balances, reverse_disconnected_blocks,
    CarryForwardBalance, PayoutMaturityState, PayoutPolicyManifest,
};
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use std::{env, error::Error, fs, process};

#[derive(Debug, Deserialize)]
struct ReorgVerificationInput {
    disconnected_block_hash: String,
    disconnected_block_height: u64,
    disconnected_payout_policy_manifest: PayoutPolicyManifest,
    replacement_block_hash: String,
    replacement_block_height: u64,
    replacement_payout_policy_manifest: PayoutPolicyManifest,
    #[serde(default)]
    active_tip_height: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ReorgVerificationReport {
    schema: String,
    disconnected_block_hash: String,
    replacement_block_hash: String,
    disconnected_entry_count: usize,
    reversed_entry_count: usize,
    replacement_entry_count: usize,
    carry_forward_balances: Vec<CarryForwardBalance>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("qbit-prism-reorg-verify: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut input_path: Option<String> = None;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input_path = args.next(),
            "-h" | "--help" => {
                print_usage();
                return Ok(());
            }
            _ if input_path.is_none() => input_path = Some(arg),
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
    let input: ReorgVerificationInput = serde_json::from_str(&input_json)?;
    let active_tip_height = input
        .active_tip_height
        .unwrap_or(input.replacement_block_height);
    let disconnected_entries = build_maturity_entries(
        &input.disconnected_block_hash,
        input.disconnected_block_height,
        &input.disconnected_payout_policy_manifest,
    );
    let reversed_entries = reverse_disconnected_blocks(
        &disconnected_entries,
        std::slice::from_ref(&input.disconnected_block_hash),
        active_tip_height,
    )?;
    let reversed_count = reversed_entries
        .iter()
        .filter(|entry| {
            entry.block_hash == input.disconnected_block_hash
                && entry.state == PayoutMaturityState::Reversed
        })
        .count();
    if reversed_count != disconnected_entries.len() {
        return Err(format!(
            "expected all {} disconnected entries to reverse, got {reversed_count}",
            disconnected_entries.len()
        )
        .into());
    }

    let replacement_entries = build_maturity_entries(
        &input.replacement_block_hash,
        input.replacement_block_height,
        &input.replacement_payout_policy_manifest,
    );
    let mut combined_entries = reversed_entries.clone();
    combined_entries.extend(replacement_entries.clone());
    let combined_balances = current_carry_forward_balances(&combined_entries);
    let replacement_balances = current_carry_forward_balances(&replacement_entries);
    if combined_balances != replacement_balances {
        return Err("carry-forward balances did not reconverge to replacement branch".into());
    }

    let report = ReorgVerificationReport {
        schema: "qbit.prism.reorg-verification-report.v1".to_string(),
        disconnected_block_hash: input.disconnected_block_hash,
        replacement_block_hash: input.replacement_block_hash,
        disconnected_entry_count: disconnected_entries.len(),
        reversed_entry_count: reversed_count,
        replacement_entry_count: replacement_entries.len(),
        carry_forward_balances: combined_balances,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn print_usage() {
    println!("usage: qbit-prism-reorg-verify [--input] <reorg-input.json|->");
}

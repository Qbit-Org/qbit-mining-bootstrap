use std::env;
use std::fs;
use std::io::{self, Read};

use qbit_pool_builder::{build_signed_manifest, CoinbaseBuildRequest, ManifestSigningKey};

fn main() {
    if let Err(error) = run() {
        eprintln!("qbit-pool-builder: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut input_path: Option<String> = None;
    let mut signing_key_seed_hex: Option<String> = None;
    let mut print_public_key_hex = false;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input_path = args.next(),
            "--signing-key-seed-hex" => signing_key_seed_hex = args.next(),
            "--print-public-key-hex" => print_public_key_hex = true,
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    let seed = signing_key_seed_hex.ok_or("--signing-key-seed-hex is required")?;
    let signing_key = ManifestSigningKey::from_seed_hex(&seed)?;
    if print_public_key_hex {
        println!("{}", signing_key.public_key_hex());
        return Ok(());
    }

    let request_json = match input_path.as_deref() {
        Some("-") | None => {
            let mut buffer = String::new();
            io::stdin().read_to_string(&mut buffer)?;
            buffer
        }
        Some(path) => fs::read_to_string(path)?,
    };
    let request: CoinbaseBuildRequest = serde_json::from_str(&request_json)?;

    let manifest = build_signed_manifest(request, &signing_key)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

fn print_help() {
    println!(
        "Usage: qbit-pool-builder --signing-key-seed-hex <64 hex chars> [--input request.json]\n\
         Reads a CoinbaseBuildRequest JSON document and writes a signed payout manifest JSON document.\n\
         Use --print-public-key-hex to print the Ed25519 public key for a seed without reading input."
    );
}

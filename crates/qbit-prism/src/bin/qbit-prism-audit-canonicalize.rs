use qbit_prism::{canonical_audit_bundle_bytes, AuditBundle};
use std::io::{self, Read, Write};
use std::{env, error::Error, fs, process};

fn main() {
    if let Err(error) = run() {
        eprintln!("qbit-prism-audit-canonicalize: {error}");
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
    let bundle: AuditBundle = serde_json::from_str(&input_json)?;
    let canonical = canonical_audit_bundle_bytes(&bundle)?;
    io::stdout().write_all(&canonical)?;
    Ok(())
}

fn print_usage() {
    println!("usage: qbit-prism-audit-canonicalize [--input <audit-bundle.json|->]");
}

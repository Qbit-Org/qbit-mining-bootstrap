use qbit_prism::{
    canonical_audit_bundle_bytes, load_audit_bundle_from_path, parse_audit_bundle_json,
};
use std::io::{self, Read, Write};
use std::{env, error::Error, process};

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

    let bundle = match input_path.as_deref() {
        Some("-") | None => {
            let mut buffer = String::new();
            io::stdin().read_to_string(&mut buffer)?;
            parse_audit_bundle_json(&buffer, None)?
        }
        Some(path) => load_audit_bundle_from_path(path)?,
    };
    let canonical = canonical_audit_bundle_bytes(&bundle)?;
    io::stdout().write_all(&canonical)?;
    Ok(())
}

fn print_usage() {
    println!("usage: qbit-prism-audit-canonicalize [--input <audit-bundle.json|->]");
}

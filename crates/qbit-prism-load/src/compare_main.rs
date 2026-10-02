//! `qbit-prism-load-compare`: summarize an A/B series of two builds on one
//! preset (#511) and print the comparison as Markdown.
//!
//! Exit 0 when the candidate meets #473's D1 rule in every phase the preset
//! gates, 1 when it does not, 2 when the inputs cannot be read.

use clap::Parser;
use qbit_prism_load::{compare, gate, preset::Preset};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "qbit-prism-load-compare",
    about = "Compare two builds' qbit-prism-load runs of one preset",
    version
)]
struct CompareArgs {
    /// The series' `manifest.json`, as scripts/prism_load_ab.py writes it.
    #[arg(long, required_unless_present = "validate_preset")]
    manifest: Option<PathBuf>,
    /// Only check that `--preset` loads and that this harness can derive
    /// its settings and phase plan, then exit (0 valid, 2 not): the driver
    /// runs this before it builds or runs anything.
    #[arg(long)]
    validate_preset: bool,
    /// The preset the series ran; every run is held to its gates, its
    /// `gates.phases` choose the gated D1 rows, and its SHA-256 must be the
    /// manifest's.
    #[arg(long)]
    preset: PathBuf,
}

fn main() {
    let args = CompareArgs::parse();
    match run(&args) {
        Ok(true) => std::process::exit(0),
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("qbit-prism-load-compare: error: {error:#}");
            std::process::exit(2);
        }
    }
}

fn run(args: &CompareArgs) -> anyhow::Result<bool> {
    use anyhow::ensure;
    let preset = Preset::load(&args.preset)?;
    if args.validate_preset {
        compare::expected_settings(&preset.args)?;
        let planned = compare::expected_phases(&preset.args)?;
        // The comparison needs a gated D1 phase the preset runs.
        let gated: Vec<&str> = compare::D1_PHASES
            .iter()
            .copied()
            .filter(|phase| planned.iter().any(|plan| plan.name == *phase))
            .filter(|phase| {
                preset
                    .gates
                    .phases
                    .as_ref()
                    .is_none_or(|names| names.iter().any(|n| n == phase))
            })
            .collect();
        anyhow::ensure!(
            !gated.is_empty(),
            "preset {} gates none of the D1 phases ({}) it runs; the A/B comparison has nothing \
             to hold the candidate to",
            preset.name,
            compare::D1_PHASES.join(", ")
        );
        println!("preset {} is valid", preset.name);
        return Ok(true);
    }
    let manifest_path = args.manifest.as_ref().expect("clap requires it");
    let manifest = compare::read_manifest(manifest_path)?;
    ensure!(
        manifest.preset.name == preset.name && manifest.preset.sha256 == preset.sha256,
        "the series ran preset {} ({}), not {} ({})",
        manifest.preset.name,
        manifest.preset.sha256,
        preset.name,
        preset.sha256
    );
    let base_dir = manifest_path
        .parent()
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
    let runs = compare::load_runs(&manifest, &base_dir)?;
    let comparison = compare::compare(
        &manifest,
        &runs,
        &gate::Budgets::from(&preset.gates),
        &preset.args,
    )?;
    print!("{}", comparison.markdown);
    Ok(comparison.passed)
}

//! `qbit-prism-load-compare`: summarize an A/B series of two builds on one
//! preset (#511) and print the comparison as Markdown.
//!
//! Exit 0 when the candidate meets #473's D1 rule in every phase the preset
//! gates, 1 when it does not, 2 when the inputs cannot be read.
//!
//! `--collate <plan.json>` instead renders one build's L3 suite (#550): every
//! preset of the plan in #473's document tables, from the run jobs'
//! artifacts under `--runs-dir`. Exit 0 when every planned run reached its
//! gate and passed it, 1 when not, 2 when the inputs cannot be read.

use clap::Parser;
use qbit_prism_load::{collate, compare, gate, preset::Preset};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "qbit-prism-load-compare",
    about = "Compare two builds' qbit-prism-load runs of one preset",
    version
)]
struct CompareArgs {
    /// The series' `manifest.json`, as scripts/prism_load_ab.py writes it.
    #[arg(long, required_unless_present_any = ["validate_preset", "collate"])]
    manifest: Option<PathBuf>,
    /// Only check that `--preset` loads and that this harness can derive
    /// its settings and phase plan, then exit (0 valid, 2 not): the driver
    /// runs this before it builds or runs anything.
    #[arg(long)]
    validate_preset: bool,
    /// The preset the series ran; every run is held to its gates, its
    /// `gates.phases` choose the gated D1 rows, and its SHA-256 must be the
    /// manifest's.
    #[arg(long, required_unless_present = "collate")]
    preset: Option<PathBuf>,
    /// Collate an L3 suite's runs (#550): the plan job's `plan.json`
    /// (`qbit.prism.l3-plan.v1`).
    #[arg(long, conflicts_with_all = ["manifest", "validate_preset", "preset"])]
    collate: Option<PathBuf>,
    /// The directory the run jobs' `prism-l3-run-<id>` artifacts were
    /// downloaded into, one directory each.
    #[arg(long, requires = "collate")]
    runs_dir: Option<PathBuf>,
    /// The checked-in presets at the plan's commit.
    #[arg(long, requires = "collate")]
    presets_dir: Option<PathBuf>,
    /// Where to write the verdict (`qbit.prism.l3-verdict.v1`) the tag's
    /// promotion reads.
    #[arg(long, requires = "collate")]
    verdict_out: Option<PathBuf>,
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

fn collate(args: &CompareArgs, plan_path: &std::path::Path) -> anyhow::Result<bool> {
    use anyhow::Context;
    let plan = collate::read_plan(plan_path)?;
    let runs_dir = args
        .runs_dir
        .as_ref()
        .context("--collate needs --runs-dir")?;
    let presets_dir = args
        .presets_dir
        .as_ref()
        .context("--collate needs --presets-dir")?;
    let presets = qbit_prism_load::preset::load_all(presets_dir)?
        .into_iter()
        .map(|preset| (preset.name.clone(), preset))
        .collect();
    let collation = collate::collate(&plan, runs_dir, &presets)?;
    if let Some(path) = &args.verdict_out {
        std::fs::write(
            path,
            serde_json::to_string_pretty(&collation.verdict)? + "\n",
        )
        .with_context(|| format!("writing {}", path.display()))?;
    }
    print!("{}", collation.markdown);
    Ok(collation.passed)
}

fn run(args: &CompareArgs) -> anyhow::Result<bool> {
    use anyhow::ensure;
    if let Some(plan) = &args.collate {
        return collate(args, plan);
    }
    let preset = Preset::load(args.preset.as_ref().expect("clap requires it"))?;
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

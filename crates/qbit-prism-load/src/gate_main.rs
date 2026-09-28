//! `qbit-prism-load-gate`: hold one harness run to its preset's gates and
//! print the verdict as a Markdown table (#521).
//!
//! Exit 0 when every gate passes, 1 when one fails, 2 when the inputs cannot
//! be read.

use clap::Parser;
use qbit_prism_load::{gate, preset::Preset};
use std::{io::Write, path::PathBuf};

#[derive(Parser, Debug)]
#[command(
    name = "qbit-prism-load-gate",
    about = "Gate a qbit-prism-load run",
    version
)]
struct GateArgs {
    /// The run's `load-harness-report.json`.
    #[arg(long)]
    report: PathBuf,
    /// The preset the run used; its `gates` block is the default budget.
    #[arg(long)]
    preset: PathBuf,
    /// The harness's exit code. Omitted, the exit-code check fails.
    #[arg(long)]
    exit_code: Option<i32>,
    /// Override the preset's `max_shortfall`.
    #[arg(long)]
    max_shortfall: Option<u64>,
    /// Override the preset's `tip_last_notify_p99_budget_ms`.
    #[arg(long)]
    tip_last_notify_p99_budget_ms: Option<f64>,
    /// Append the Markdown verdict to this file (a job summary).
    #[arg(long)]
    summary: Option<PathBuf>,
}

fn main() {
    let args = GateArgs::parse();
    match run(&args) {
        Ok(true) => std::process::exit(0),
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("qbit-prism-load-gate: error: {error:#}");
            std::process::exit(2);
        }
    }
}

fn run(args: &GateArgs) -> anyhow::Result<bool> {
    use anyhow::{ensure, Context};
    let preset = Preset::load(&args.preset)?;
    let mut budgets = gate::Budgets::from(&preset.gates);
    if let Some(value) = args.max_shortfall {
        budgets.max_shortfall = value;
    }
    if let Some(value) = args.tip_last_notify_p99_budget_ms {
        ensure!(
            value.is_finite() && value > 0.0,
            "--tip-last-notify-p99-budget-ms must be finite and positive"
        );
        budgets.tip_last_notify_p99_ms = Some(value);
    }
    let text = std::fs::read_to_string(&args.report)
        .with_context(|| format!("reading {}", args.report.display()))?;
    let report: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parsing {}", args.report.display()))?;
    let checks = gate::evaluate(&report, args.exit_code, &budgets);
    let mut table = gate::markdown(&format!("qbit-prism-load `{}`", preset.name), &checks);
    if budgets.d1_verdict_table {
        table.push_str(&format!(
            "\n#### D1 verdict in #473's format\n\n{}",
            gate::d1_table(&report, args.exit_code, &preset.name)
        ));
    }
    print!("{table}");
    if let Some(path) = &args.summary {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening {}", path.display()))?;
        writeln!(file, "{table}")?;
    }
    Ok(gate::passed(&checks))
}

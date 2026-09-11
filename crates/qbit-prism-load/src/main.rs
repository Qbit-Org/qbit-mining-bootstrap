//! `qbit-prism-load`: drive real Stratum sockets against real PRISM frontends
//! and produce a capacity-evidence artifact plus a side report.

use anyhow::Result;
use clap::Parser;
use qbit_prism_load::{cli::Args, run};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("prism-load")
        .build()?;
    // The harness owns child processes and two PostgreSQL clusters. A signal
    // must unwind through the same teardown as a normal exit, so it cancels
    // the run rather than killing the process where it stands.
    let code = runtime.block_on(async move {
        tokio::select! {
            result = run::execute(args) => result,
            reason = signal() => {
                eprintln!("qbit-prism-load: {reason}; tearing down");
                Ok(run::EXIT_ABORTED)
            }
        }
    })?;
    std::process::exit(code);
}

async fn signal() -> String {
    use tokio::signal::unix::{signal, SignalKind};
    let mut interrupt = match signal(SignalKind::interrupt()) {
        Ok(handle) => handle,
        Err(_) => return "signal handling unavailable".into(),
    };
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(handle) => handle,
        Err(_) => return "signal handling unavailable".into(),
    };
    tokio::select! {
        _ = interrupt.recv() => "interrupted".into(),
        _ = terminate.recv() => "terminated".into(),
    }
}

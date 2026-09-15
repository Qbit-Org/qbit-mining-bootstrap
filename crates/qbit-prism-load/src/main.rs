//! `qbit-prism-load`: drive real Stratum sockets against real PRISM frontends
//! and produce a capacity-evidence artifact plus a side report.

use clap::Parser;
use qbit_prism_load::{cli::Args, frontend, run};

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("prism-load")
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => fail(error.into()),
    };
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
    });
    match code {
        Ok(code) => std::process::exit(code),
        Err(error) => fail(error),
    }
}

/// Print the error and exit with the documented code for a harness error.
///
/// Returning the error from `main` printed it verbatim and exited 1, while
/// the README promises 2 for "the harness failed with an error": `EXIT_ERROR`
/// was defined and never produced. The text goes out through the same
/// redaction the failure report applies, because stderr is a sink like any
/// other and the error chain can name a URL (EP-OBSERVABILITY).
fn fail(error: anyhow::Error) -> ! {
    eprintln!(
        "qbit-prism-load: error: {}",
        frontend::redact_secrets_in_text(&format!("{error:#}"))
    );
    std::process::exit(run::EXIT_ERROR);
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

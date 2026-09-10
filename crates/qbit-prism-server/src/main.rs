fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let workers = qbit_prism_server::config::number(
        "PRISM_RUNTIME_WORKERS",
        std::thread::available_parallelism().map_or(2, usize::from),
    )?;
    anyhow::ensure!(
        workers > 0 && workers <= 1024,
        "PRISM_RUNTIME_WORKERS must be 1..1024"
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(workers + 8)
        .thread_name("prism")
        .enable_all()
        .build()?;
    runtime.block_on(qbit_prism_server::tools::run())
}

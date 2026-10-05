//! External-target mode against the in-repo frontend (#291).
//!
//! Two debug `qbit-prism-server` frontends on a managed PostgreSQL 16 and
//! the fake node, behind a round-robin TCP balancer of the test's own, as
//! the operator's load balancer sits in front of the deployed pair. Two
//! external processes drive it as two client machines would, one asking for
//! its own share difficulty with `d=`. One frontend is SIGKILLed mid-run and
//! relaunched. Then what the clients recorded is held to what PostgreSQL
//! holds: every acknowledged share is committed, and every committed share
//! of theirs was either acknowledged or lost its answer with the frontend.
//!
//! The server binary is built as `load_smoke` builds it, into this test's
//! own target directory; see that test's `build_server`.

use anyhow::{ensure, Context, Result};
use clap::Parser;
use qbit_prism_load::{
    cli::{Args, NodeMode},
    cluster::{ManagedPostgres, Replication},
    external::{self, ExternalArgs, Shutdown},
    frontend::{self, Frontend, FrontendSpec},
    node::FakeNode,
    run, window,
};
use qbit_prism_test_gate as test_gate;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// `target/<profile>`, the directory this test binary's `deps` sits in.
fn profile_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe()?;
    let deps = exe.parent().context("test binary has no directory")?;
    Ok(deps
        .parent()
        .context("test binary's deps directory has no parent")?
        .to_owned())
}

/// Build the debug server beside this test binary, exactly as `load_smoke`
/// does, so neither build invalidates the other's dependencies.
fn build_server() -> Result<PathBuf> {
    let profile = profile_dir()?;
    let target = profile
        .parent()
        .context("profile directory has no parent")?;
    let mut command = std::process::Command::new(env!("CARGO"));
    for (name, _) in std::env::vars_os() {
        let name = name.to_string_lossy().into_owned();
        if [
            "CARGO_PKG_",
            "CARGO_MANIFEST_",
            "CARGO_CRATE_",
            "CARGO_BIN_",
            "CARGO_PRIMARY_PACKAGE",
            "CARGO_TARGET_TMPDIR",
        ]
        .iter()
        .any(|prefix| name.starts_with(prefix))
        {
            command.env_remove(name);
        }
    }
    let status = command
        .args([
            "build",
            "--locked",
            "-p",
            "qbit-prism-load",
            "-p",
            "qbit-prism-server",
            "--bin",
            "qbit-prism-server",
        ])
        .env("CARGO_TARGET_DIR", target)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .context("running cargo build for qbit-prism-server")?;
    ensure!(status.success(), "cargo build qbit-prism-server: {status}");
    let server = profile.join("qbit-prism-server");
    ensure!(server.is_file(), "{} was not built", server.display());
    Ok(server)
}

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "prism-external-frontend-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

/// A TCP load balancer: each connection goes to the next backend in turn
/// that accepts it, so a dead frontend's sessions land on the live one when
/// they reconnect, as the operator's balancer ejects a backend.
struct Balancer {
    address: String,
    connections: Arc<AtomicUsize>,
    _task: tokio::task::JoinHandle<()>,
}

impl Balancer {
    async fn start(backends: Vec<String>) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = connections.clone();
        let task = tokio::spawn(async move {
            let next = Arc::new(AtomicUsize::new(0));
            loop {
                let Ok((mut client, _)) = listener.accept().await else {
                    return;
                };
                counted.fetch_add(1, Ordering::SeqCst);
                let backends = backends.clone();
                let next = next.clone();
                tokio::spawn(async move {
                    let first = next.fetch_add(1, Ordering::SeqCst);
                    for offset in 0..backends.len() {
                        let backend = &backends[(first + offset) % backends.len()];
                        if let Ok(mut upstream) = tokio::net::TcpStream::connect(backend).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                            return;
                        }
                    }
                });
            }
        });
        Ok(Self {
            address,
            connections,
            _task: task,
        })
    }
}

const ADDRESS: &str = "pload1external";
/// The frontends' share difficulty: about 512 hashes a share.
const FRONTEND_DIFFICULTY: f64 = 1.0 / 8_388_608.0;
/// What the second client asks for with `d=`: four times the frontends'.
const REQUESTED_DIFFICULTY: f64 = 1.0 / 2_097_152.0;

fn client_args(target: &str, out: &Path, log: &Path, label: &str, extra: &[&str]) -> ExternalArgs {
    let out = out.display().to_string();
    let log = log.display().to_string();
    let mut argv = vec![
        "qbit-prism-load external",
        "--target",
        target,
        external::GUARD_FLAG,
        "--address",
        ADDRESS,
        "--label",
        label,
        "--worker-prefix",
        label,
        "--sessions",
        "8",
        "--rate",
        "20",
        "--duration-seconds",
        "20",
        "--work-timeout-seconds",
        "120",
        "--drain-seconds",
        "25",
        "--progress-seconds",
        "5",
        "--out",
        &out,
        "--share-log",
        &log,
    ];
    argv.extend_from_slice(extra);
    ExternalArgs::try_parse_from(argv).expect("client arguments")
}

/// Wait for a frontend to serve, or fail with the end of its log: the
/// scratch directory that holds the log goes with the test.
async fn ready(frontend: &mut Frontend) -> Result<()> {
    if let Err(error) = frontend.wait_ready(Duration::from_secs(120)).await {
        let log = frontend.read_stderr();
        let tail: Vec<&str> = log.lines().rev().take(30).collect();
        anyhow::bail!(
            "{error:#}\n{}",
            tail.into_iter().rev().collect::<Vec<_>>().join("\n")
        );
    }
    Ok(())
}

/// Every line of a share log.
fn share_log(path: &Path) -> Result<Vec<Value>> {
    std::fs::read_to_string(path)?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_load_through_a_balancer_reaches_the_in_repo_frontends_and_reconciles(
) -> Result<()> {
    let Some(pg_bin) = test_gate::pg_bin_dir(test_gate::site!())? else {
        return Ok(());
    };
    let server = build_server()?;
    let scratch = Scratch::new()?;
    let log_dir = scratch.path("logs");
    std::fs::create_dir_all(&log_dir)?;
    let cluster = ManagedPostgres::start(PathBuf::from(pg_bin), Replication::None, 96, false)
        .await
        .context("starting the managed cluster")?;
    let node = FakeNode::open(window::TEMPLATE_BITS, "pload1").await?;
    // The harness's own frontend environment for two frontends, at the
    // share difficulty above with vardiff off: the settings a target needs
    // for low-difficulty load (README, External-target mode).
    let args = Args::try_parse_from(["qbit-prism-load", "--frontends", "2", "--sessions", "16"])?;
    let shared = run::shared_environment(
        &args,
        node.url.clone(),
        "external".into(),
        format!("{FRONTEND_DIFFICULTY}"),
    );
    let mut frontends: Vec<Frontend> = Vec::new();
    for index in 0..2 {
        let instance_id = format!("load-fe-{index}");
        let spec = FrontendSpec {
            index,
            database_url: run::with_application_name(&cluster.primary_url, &instance_id),
            instance_id,
            stratum_port: free_port()?,
            audit_port: free_port()?,
        };
        let environment = run::launch_environment(
            &shared,
            &spec,
            0,
            &frontend::pool_fee_address(ADDRESS),
            false,
            NodeMode::Fake,
        );
        // One at a time: concurrent first-boot migrations wait on each other.
        let mut child = Frontend::launch(server.clone(), spec, environment, &log_dir)?;
        ready(&mut child).await?;
        frontends.push(child);
    }
    let balancer = Balancer::start(
        frontends
            .iter()
            .map(|frontend| frontend.stratum_address())
            .collect(),
    )
    .await?;

    let (out_a, log_a) = (scratch.path("vm-a.json"), scratch.path("vm-a.jsonl"));
    let (out_b, log_b) = (scratch.path("vm-b.json"), scratch.path("vm-b.jsonl"));
    let requested = format!("{REQUESTED_DIFFICULTY}");
    let first = client_args(&balancer.address, &out_a, &log_a, "vm-a", &[]);
    let second = client_args(
        &balancer.address,
        &out_b,
        &log_b,
        "vm-b",
        &["--difficulty", &requested],
    );
    let first = tokio::spawn(async move { external::run(&first, &Shutdown::never()).await });
    let second = tokio::spawn(async move { external::run(&second, &Shutdown::never()).await });

    // Once the load is committing, kill one frontend outright, hold it down,
    // and bring it back.
    let side = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&cluster.primary_url)
        .await?;
    let committed = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM qbit_share_ledger WHERE accepted AND share_id LIKE $1",
        )
        .bind(format!("{ADDRESS}.%"))
        .fetch_one(&side)
        .await
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while committed().await? < 60 {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the external load did not commit 60 shares within 120 s; see {}",
            log_dir.display()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    frontends[1].kill();
    tokio::time::sleep(Duration::from_secs(3)).await;
    frontends[1].restart()?;
    ready(&mut frontends[1]).await?;

    let first = first.await??;
    let second = second.await??;
    assert_eq!(first.exit_code, run::EXIT_OK);
    assert_eq!(second.exit_code, run::EXIT_OK);
    for (outcome, difficulty) in [
        (&first, FRONTEND_DIFFICULTY),
        (&second, REQUESTED_DIFFICULTY),
    ] {
        let summary = &outcome.document["summary"];
        let process = &outcome.document["processes"][0];
        assert_eq!(process["ended"], json!("completed"), "{summary:#}");
        assert_eq!(process["sessions_holding_work_at_start"], json!(8));
        assert!(
            summary["shares"]["accepted"].as_u64().unwrap() >= 100,
            "{summary:#}"
        );
        // The frontend's verdict on every share: none mined wrong.
        let classes = &summary["rejections"]["by_class"];
        for class in ["harness-bug", "unknown", "unclassified"] {
            assert!(classes.get(class).is_none(), "{summary:#}");
        }
        // Mined at what the frontend advertised: its own difficulty, or the
        // one asked for with d=, which it honours with vardiff off.
        assert_eq!(summary["difficulty"]["advertised_min"], json!(difficulty));
        assert_eq!(summary["difficulty"]["advertised_max"], json!(difficulty));
        assert_eq!(summary["offers"]["unaccounted"], json!(0), "{summary:#}");
        assert_eq!(
            summary["ack_latency"]["samples"],
            summary["shares"]["accepted"]
        );
    }
    let merged = external::merge_files(&[out_a.clone(), out_b.clone()])?;
    let summary = &merged["summary"];
    // The killed frontend's sessions lost their connections and came back
    // through the balancer.
    let disconnects = summary["reconnects"]["disconnects"].as_u64().unwrap();
    assert!(disconnects >= 1, "{summary:#}");
    assert_eq!(
        summary["reconnects"]["completed"],
        json!(disconnects),
        "{summary:#}"
    );
    assert!(balancer.connections.load(Ordering::SeqCst) as u64 >= 16 + disconnects);
    assert_eq!(summary["sessions"], json!(16));
    assert_eq!(
        summary["shares"]["accepted"].as_u64().unwrap(),
        first.document["summary"]["shares"]["accepted"]
            .as_u64()
            .unwrap()
            + second.document["summary"]["shares"]["accepted"]
                .as_u64()
                .unwrap()
    );

    // Reconcile the clients' ids against the ledger, as the failover drill
    // does against the promoted primary.
    let mut acknowledged = BTreeSet::new();
    let mut unanswered = BTreeSet::new();
    for path in [&log_a, &log_b] {
        for line in share_log(path)? {
            let id = line["share_id"].as_str().unwrap().to_owned();
            match line["outcome"].as_str().unwrap() {
                "accepted" => assert!(acknowledged.insert(id), "an id acknowledged twice"),
                "no-response" => {
                    unanswered.insert(id);
                }
                _ => {}
            }
        }
    }
    assert_eq!(
        acknowledged.len() as u64,
        summary["shares"]["accepted"].as_u64().unwrap()
    );
    let in_postgres: BTreeSet<String> = sqlx::query_scalar::<_, String>(
        "SELECT share_id FROM qbit_share_ledger WHERE accepted AND share_id LIKE $1",
    )
    .bind(format!("{ADDRESS}.%"))
    .fetch_all(&side)
    .await?
    .into_iter()
    .collect();
    let missing: Vec<&String> = acknowledged.difference(&in_postgres).collect();
    assert!(
        missing.is_empty(),
        "acknowledged but not committed: {missing:?}"
    );
    let unexplained: Vec<&String> = in_postgres
        .iter()
        .filter(|id| !acknowledged.contains(*id) && !unanswered.contains(*id))
        .collect();
    assert!(
        unexplained.is_empty(),
        "committed but neither acknowledged nor unanswered: {unexplained:?}"
    );

    side.close().await;
    // The frontends, then the cluster, stop as they drop.
    Ok(())
}

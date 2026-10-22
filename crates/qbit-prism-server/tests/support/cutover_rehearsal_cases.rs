//! #575 item 1: the cutover rehearsal on every PR, and its operator mode.
//!
//! The per-PR case writes a mainnet-shaped 2.x.x ledger at PR size, dumps it
//! with `pg_dump` and rehearses that dump through the same path
//! `make prism-cutover-rehearsal` runs on an operator's snapshot. The two
//! `#[ignore]` cases are the operator mode itself and the generator of a
//! mainnet-shaped dump at any scale; both are driven by the make targets and
//! documented in `docs/prism-rust-migration.md`.
use super::cutover_rehearsal::{self as rehearsal, seed};
use super::recovery;
use anyhow::{bail, ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism_test_gate as gate;
use serde_json::Value;
use std::path::{Path, PathBuf};

/// The synthetic chain's name; the rehearsal node serves the same hashes.
const CHAIN_TAG: &str = "575";
/// The height of the first seeded block.
const CHAIN_BASE: u64 = 100_000;

fn keys() -> Result<(ManifestSigningKey, ManifestSigningKey)> {
    Ok((
        ManifestSigningKey::from_seed_hex(&rehearsal::COINBASE_SEED.repeat(32))?,
        ManifestSigningKey::from_seed_hex(&rehearsal::LEDGER_SEED.repeat(32))?,
    ))
}

/// Writes `shape` into `source` and dumps it to `dump`, returning the
/// generator's summary.
async fn seed_and_dump(
    source: &recovery::Database,
    shape: seed::MainnetShape,
    artifacts: &Path,
    pg_bin: &Path,
    dump: &Path,
) -> Result<Value> {
    let cutover_ms = chrono::Utc::now().timestamp_millis();
    let plan = seed::MainnetPlan::new(shape, cutover_ms)?;
    let (chain, _) = plan.synthetic_chain(CHAIN_TAG, CHAIN_BASE);
    let (coinbase_key, ledger_key) = keys()?;
    let (_, summary) = plan
        .write(&source.pool, artifacts, &chain, &coinbase_key, &ledger_key)
        .await?;
    // The database URL without the search_path option the pool added, every
    // other connection parameter kept; a password goes through the
    // environment rather than pg_dump's arguments.
    let mut url = url::Url::parse(&source.url)?;
    let kept: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(name, _)| name != "options")
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect();
    url.set_query(None);
    if !kept.is_empty() {
        url.query_pairs_mut().extend_pairs(kept);
    }
    let password = url.password().map(str::to_owned);
    url.set_password(None)
        .map_err(|_| anyhow::anyhow!("the database URL cannot hold a password"))?;
    let mut command = tokio::process::Command::new(pg_bin.join("pg_dump"));
    if let Some(password) = password {
        command.env(
            "PGPASSWORD",
            percent_encoding::percent_decode_str(&password)
                .decode_utf8()?
                .into_owned(),
        );
    }
    let output = command
        .args([
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            "--schema",
        ])
        .arg(&source.schema)
        .arg("--file")
        .arg(dump)
        .arg("--dbname")
        .arg(url.as_str())
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "pg_dump failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(summary)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mainnet_shaped_2x_dump_rehearses_the_cutover_and_keeps_every_balance() -> Result<()> {
    let Some(inputs) = gate::inputs(
        gate::site!(),
        &[gate::Input::DatabaseUrl, gate::Input::PgBinDir],
    )?
    else {
        return Ok(());
    };
    let pg_bin = PathBuf::from(&inputs[1]);
    let source = recovery::Database::open(&inputs[0]).await?;
    let result = async {
        let work = tempfile::tempdir()?;
        let artifacts = tempfile::tempdir()?;
        let dump = work.path().join("source.dump");
        let summary = seed_and_dump(
            &source,
            seed::MainnetShape::pull_request(),
            artifacts.path(),
            &pg_bin,
            &dump,
        )
        .await?;
        // The PR ledger still has the mainnet shape: every identity mined,
        // a dominant whale on most days, every block state, pending payouts.
        ensure!(
            summary["identities_with_shares"] == seed::MAINNET_IDENTITIES,
            "{summary}"
        );
        ensure!(
            summary["top_identity_work_share_per_day"]["days_at_or_above_0_82"]
                .as_u64()
                .is_some_and(|days| days >= 30),
            "{summary}"
        );
        for state in ["mature", "immature", "inactive", "reversed", "rejected"] {
            ensure!(
                summary["blocks"][state]
                    .as_u64()
                    .is_some_and(|count| count > 0),
                "no {state} block: {summary}"
            );
        }
        // The restore in the private cluster must reproduce this exactly.
        let source_evidence = recovery::evidence(&source, &pg_bin).await?;
        let mut report = rehearsal::Report::new(summary);
        rehearsal::rehearse_dump(
            &rehearsal::DumpRehearsal {
                dump,
                schema: None,
                options: rehearsal::Options {
                    pg_bin: pg_bin.clone(),
                    audit_root: artifacts.path().to_owned(),
                    ledger_public_key: keys()?.1.public_key_hex(),
                    node: rehearsal::NodeChoice::Ledger {
                        tag: CHAIN_TAG.into(),
                    },
                    start_frontend: true,
                    extra_env: Vec::new(),
                    source_evidence: Some(source_evidence),
                },
                workdir: work.path().to_owned(),
            },
            &mut report,
        )
        .await?;
        eprintln!("{}", report.render());
        ensure!(
            report.pass,
            "the cutover rehearsal failed:\n{}",
            report.render()
        );
        // The online steps take milliseconds at this size and may fall
        // between two lock samples; the transaction and the commands cannot.
        for step in [
            "migrate: transaction (001, 002-020)",
            "migrate (total)",
            "import-audits",
            "first Stratum job",
        ] {
            ensure!(
                report.steps.iter().any(|reported| reported.name == step),
                "the report has no {step} step"
            );
        }
        ensure!(
            report.rows["pending_payout_addresses"]
                .as_u64()
                .is_some_and(|count| count > 0),
            "no pending payouts to carry across"
        );
        // Both window readers page 4,096 rows at a time: the window must span
        // more than one page for the comparison to cover the paging.
        ensure!(
            report.rows["window_shares"]
                .as_u64()
                .is_some_and(|shares| shares > 4_096),
            "the payout window is too small to span a page: {}",
            report.rows["window_shares"]
        );
        anyhow::Ok(())
    }
    .await;
    source.close().await?;
    result
}

/// An optional operator setting; set but empty is refused, as the server
/// refuses an empty setting.
fn setting(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => bail!("{name} is set but empty"),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("{name}: {error}"),
    }
}

fn required(name: &str) -> Result<String> {
    setting(name)?.with_context(|| format!("set {name}"))
}

/// An operator's reviewed environment file: `NAME=value` lines, `#`
/// comments, an optional `export` and optional matching quotes. Values are
/// never printed.
fn env_file(path: &Path) -> Result<Vec<(String, String)>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading PRISM_REHEARSAL_ENV_FILE {}", path.display()))?;
    let mut env = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (name, value) = line
            .split_once('=')
            .with_context(|| format!("{}:{}: not NAME=value", path.display(), number + 1))?;
        ensure!(
            !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "{}:{}: {name:?} is not a variable name",
            path.display(),
            number + 1
        );
        let value = value.trim();
        let value = ['"', '\'']
            .into_iter()
            .find_map(|quote| {
                value
                    .strip_prefix(quote)
                    .and_then(|inner| inner.strip_suffix(quote))
            })
            .unwrap_or(value);
        env.push((name.to_owned(), value.to_owned()));
    }
    ensure!(
        !env.is_empty(),
        "PRISM_REHEARSAL_ENV_FILE {} sets nothing",
        path.display()
    );
    Ok(env)
}

/// `make prism-cutover-rehearsal DUMP=<pg_dump> LEDGER_KEY=<hex>`: rehearses
/// the cutover on an operator's snapshot and prints the pass/fail report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "operator mode: run through `make prism-cutover-rehearsal`"]
async fn operator_dump_rehearsal() -> Result<()> {
    let pg_bin =
        PathBuf::from(gate::required_inputs(gate::site!(), &[gate::Input::PgBinDir])?.remove(0));
    let dump = PathBuf::from(required("PRISM_REHEARSAL_DUMP")?);
    ensure!(
        dump.exists(),
        "PRISM_REHEARSAL_DUMP {} does not exist",
        dump.display()
    );
    let ledger_public_key = required("PRISM_REHEARSAL_LEDGER_PUBLIC_KEY_HEX")?;
    ensure!(
        ledger_public_key.len() == 64 && ledger_public_key.bytes().all(|b| b.is_ascii_hexdigit()),
        "PRISM_REHEARSAL_LEDGER_PUBLIC_KEY_HEX must be 64 hex digits"
    );
    let empty_root = tempfile::tempdir()?;
    let audit_root = match setting("PRISM_REHEARSAL_AUDIT_ROOT")? {
        Some(root) => {
            let root = PathBuf::from(root);
            ensure!(
                root.is_dir(),
                "PRISM_REHEARSAL_AUDIT_ROOT {} is not a directory",
                root.display()
            );
            root
        }
        None => empty_root.path().to_owned(),
    };
    let node = match setting("PRISM_REHEARSAL_ENV_FILE")? {
        Some(path) => rehearsal::NodeChoice::Operator {
            env: env_file(Path::new(&path))?,
        },
        None => rehearsal::NodeChoice::Ledger {
            tag: "operator".into(),
        },
    };
    // The statement timeout the operator's migrate will run with.
    let mut extra_env = Vec::new();
    if let Some(millis) = setting("PRISM_REHEARSAL_STATEMENT_TIMEOUT_MS")? {
        ensure!(
            millis.parse::<u64>().is_ok_and(|millis| (1..=600_000).contains(&millis)),
            "PRISM_REHEARSAL_STATEMENT_TIMEOUT_MS must be 1 to 600000 milliseconds, as the server requires"
        );
        extra_env.push(("PRISM_DATABASE_STATEMENT_TIMEOUT_MS".to_owned(), millis));
    }
    let workdir = match setting("PRISM_REHEARSAL_WORKDIR")? {
        Some(dir) => PathBuf::from(dir),
        None => std::env::temp_dir(),
    };
    let mut report = rehearsal::Report::new(serde_json::json!({
        "dump": dump.file_name().map(|name| name.to_string_lossy().into_owned()),
        "dump_bytes": std::fs::metadata(&dump)?.len(),
        "configuration": match &node {
            rehearsal::NodeChoice::Operator { .. } => "the operator's environment",
            _ => "lab, with the rehearsal node",
        },
    }));
    let result = rehearsal::rehearse_dump(
        &rehearsal::DumpRehearsal {
            dump,
            schema: setting("PRISM_REHEARSAL_SCHEMA")?,
            options: rehearsal::Options {
                pg_bin,
                audit_root,
                ledger_public_key,
                node,
                start_frontend: true,
                extra_env,
                source_evidence: None,
            },
            workdir,
        },
        &mut report,
    )
    .await;
    if let Err(error) = &result {
        report.checks.push(rehearsal::Check {
            name: "rehearsal ran to the end".into(),
            pass: false,
            detail: format!("{error:#}"),
        });
        report.finish();
    }
    println!("{}", report.render());
    if let Some(path) = setting("PRISM_REHEARSAL_REPORT")? {
        std::fs::write(&path, serde_json::to_vec_pretty(&report)?)?;
        println!("report: {path}");
    }
    ensure!(report.pass, "PRISM cutover rehearsal FAILED");
    Ok(())
}

/// `make prism-cutover-rehearsal-dump OUT=<file> SCALE=1 DENSITY=0.0625`:
/// writes a mainnet-shaped 2.x.x ledger in a private cluster and dumps it,
/// with its audit bodies beside it, for the operator mode and for sizing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "generator: run through `make prism-cutover-rehearsal-dump`"]
async fn generate_mainnet_shaped_dump() -> Result<()> {
    let pg_bin =
        PathBuf::from(gate::required_inputs(gate::site!(), &[gate::Input::PgBinDir])?.remove(0));
    let out = PathBuf::from(required("PRISM_REHEARSAL_GENERATE_DUMP")?);
    let scale: u32 = setting("PRISM_REHEARSAL_SCALE")?
        .map_or(Ok(1), |value| value.parse())
        .context("PRISM_REHEARSAL_SCALE must be a whole number")?;
    let density: f64 = setting("PRISM_REHEARSAL_DENSITY")?
        .map_or(Ok(1.0 / 16.0), |value| value.parse())
        .context("PRISM_REHEARSAL_DENSITY must be a number")?;
    let shape = seed::MainnetShape::mainnet(scale, density)?;
    let audits = out.with_extension("audits");
    std::fs::create_dir_all(&audits)?;
    let workdir = out.parent().unwrap_or(Path::new(".")).to_owned();
    let cluster = rehearsal::PrivateCluster::start(&pg_bin, &workdir).await?;
    let source = recovery::Database::open(&cluster.url("postgres")).await?;
    let started = std::time::Instant::now();
    let summary = seed_and_dump(&source, shape, &audits, &pg_bin, &out).await;
    source.close().await?;
    let mut summary = summary?;
    summary["generate_seconds"] = serde_json::json!(started.elapsed().as_secs());
    summary["audit_root"] = serde_json::json!(audits);
    summary["ledger_public_key_hex"] = serde_json::json!(keys()?.1.public_key_hex());
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

/// #639: the frontend's three ports stay bound by the rehearsal from their
/// pick until the frontend spawns, so none can be picked twice or taken by
/// another socket while the operator commands run. The competing bind stands
/// in for whatever took the audit port on CI; with the listeners dropped at
/// the pick, it succeeds.
#[test]
fn frontend_ports_stay_held_until_the_frontend_spawns() -> Result<()> {
    let mut ports = rehearsal::Ports::reserve()?;
    let picked = [ports.stratum, ports.highdiff, ports.api];
    ensure!(
        picked
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            == 3,
        "a port was picked twice: {ports}"
    );
    ensure!(
        ports.held() == 3,
        "{} of 3 ports held: {ports}",
        ports.held()
    );
    for port in picked {
        ensure!(
            std::net::TcpListener::bind(("127.0.0.1", port)).is_err(),
            "another socket bound 127.0.0.1:{port} before the frontend spawned ({ports})"
        );
    }
    // A released port goes back to the host, where a concurrent test or
    // process can bind it at once; so the release is checked on the
    // reservation, not by binding the port again.
    ports.release();
    ensure!(ports.held() == 0, "a port is still held after release");
    Ok(())
}

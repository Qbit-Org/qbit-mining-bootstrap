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
    let output = tokio::process::Command::new(pg_bin.join("pg_dump"))
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
        .arg(source.url.split('?').next().unwrap_or_default())
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
        for step in [
            "migrate: transaction (001, 002-020)",
            "migrate: 013 concurrent indexes",
            "migrate: 017 validate",
            "migrate: 017 swap",
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
    let node = match setting("PRISM_REHEARSAL_NODE_RPC")? {
        Some(url) => {
            url::Url::parse(&url).context("PRISM_REHEARSAL_NODE_RPC is not a URL")?;
            rehearsal::NodeChoice::External {
                url,
                user: required("PRISM_REHEARSAL_NODE_USER")?,
                password: required("PRISM_REHEARSAL_NODE_PASSWORD")?,
                chain: setting("PRISM_REHEARSAL_NODE_CHAIN")?.unwrap_or_else(|| "main".into()),
            }
        }
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
        "node": match &node { rehearsal::NodeChoice::Ledger { .. } => "rehearsal node", _ => "operator node" },
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
    let cluster = rehearsal::PrivateCluster::start(&pg_bin, &workdir)?;
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

//! The dual-writer scenario matrix (CONTRACT.md §5), one opt-in test per
//! scenario and variant.
//!
//! Each test needs PostgreSQL 16's server binaries (`PRISM_TEST_PG_BIN_DIR`)
//! and a `qbitd` (`QBITD_BIN`), read through the shared gate. They are
//! `#[ignore]`d: the PR fast lane (`test/prism-dual-writer-pr-tests.txt`,
//! the `dual-writer-e2e` job of `.github/workflows/ci.yml`) and the nightly
//! matrix (`test/prism-dual-writer-nightly-tests.txt`, the `dual-writer-e2e`
//! job of `.github/workflows/prism-load-nightly.yml`) select them by name
//! with `--ignored --exact`, one at a time.
//!
//! The server and the audit verifier are built once per run into this
//! workspace's target directory, as `qbit-prism-load`'s tests build the
//! server. Each scenario writes `report.json`, `report.md`, `invariants.json`,
//! `shares.jsonl` and every process's log under
//! `target/dual-sim-reports/<scenario>/`; CI uploads that directory.

use anyhow::{bail, Context, Result};
use qbit_prism_dual_sim::frontend::Node;
use qbit_prism_dual_sim::{
    premigrate::{self, Plan},
    report::ScenarioReport,
    scenarios::{self, Death, DeathAtFind, Scenario, Unreadable},
    sim::{self, Inputs},
};
use qbit_prism_test_gate as gate;
use std::{
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

/// One scenario at a time per process: each runs a whole pair.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `target/<profile>`, the directory this test binary's `deps` sits in.
fn profile_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe()?;
    let deps = exe.parent().context("test binary has no directory")?;
    Ok(deps
        .parent()
        .context("test binary's deps directory has no parent")?
        .to_owned())
}

/// The server and verifier, built once per process.
fn binaries() -> Result<(PathBuf, PathBuf)> {
    static BUILT: OnceLock<Mutex<Option<(PathBuf, PathBuf)>>> = OnceLock::new();
    let slot = BUILT.get_or_init(|| Mutex::new(None));
    let mut built = slot
        .lock()
        .map_err(|_| anyhow::anyhow!("build lock poisoned"))?;
    if let Some(paths) = built.as_ref() {
        return Ok(paths.clone());
    }
    let paths = sim::build_binaries(env!("CARGO_MANIFEST_DIR").as_ref(), &profile_dir()?)?;
    *built = Some(paths.clone());
    Ok(paths)
}

fn inputs(values: Vec<String>) -> Result<Inputs> {
    let [pg_bin, qbitd] = <[String; 2]>::try_from(values)
        .map_err(|_| anyhow::anyhow!("the gate returned the wrong number of inputs"))?;
    let (server, verifier_dir) = binaries()?;
    let profile = profile_dir()?;
    Ok(Inputs {
        pg_bin: pg_bin.into(),
        qbitd: qbitd.into(),
        server,
        verifier_dir,
        // Data directories go under the temporary directory (a short path,
        // a real disk on CI's runners), reports under the target directory.
        work_root: std::env::temp_dir().join("dual-sim"),
        report_root: profile
            .parent()
            .context("profile directory has no parent")?
            .join("dual-sim-reports"),
        keep: false,
        rc6_server: None,
    })
}

async fn run(scenario: Scenario, gated: Vec<String>) -> Result<()> {
    run_with(scenario, inputs(gated)?).await
}

async fn run_with(scenario: Scenario, inputs: Inputs) -> Result<()> {
    let _serial = SERIAL.lock().await;
    let report_root = inputs.report_root.clone();
    let report = scenarios::run(scenario, inputs).await?;
    verdict(&report, &report_root.join(scenario.id()))
}

fn verdict(report: &ScenarioReport, dir: &std::path::Path) -> Result<()> {
    eprintln!("{}", report.markdown());
    if report.passed {
        return Ok(());
    }
    let failed: Vec<String> = report
        .expectations
        .iter()
        .filter(|expectation| !expectation.passed)
        .map(|expectation| format!("{}: {}", expectation.name, expectation.detail))
        .collect();
    bail!(
        "{} failed ({} expectations): {}; report and logs in {}",
        report.scenario,
        failed.len(),
        failed.join(" | "),
        dir.display()
    )
}

macro_rules! gated {
    () => {
        gate::required_inputs(
            gate::site!(),
            &[gate::Input::PgBinDir, gate::Input::QbitdBin],
        )?
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the fast lane and the nightly matrix"]
async fn checker_control_two_unsynced_single_writers_fail_landing_and_share_presence_checks(
) -> Result<()> {
    let gated = gated!();
    run(Scenario::CheckerControl, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the fast lane and the nightly matrix"]
async fn s10_single_writer_pair_keeps_every_share_and_pays_exactly_through_a_frontend_kill(
) -> Result<()> {
    let gated = gated!();
    run(Scenario::S10SingleWriter, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the fast lane and the nightly matrix"]
async fn s01_steady_state_copies_a_shares_and_blocks_to_b_within_the_sync_interval() -> Result<()> {
    let gated = gated!();
    run(Scenario::S01SteadyState, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the fast lane and the nightly matrix"]
async fn s02_a_frontend_killed_miners_move_to_b_which_mines_carry_free_and_a_pays_down_on_return(
) -> Result<()> {
    let gated = gated!();
    run(Scenario::S02ADies(Death::Kill9), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s02_a_postgres_killed_miners_move_to_b_and_a_recovers_its_tail_on_return() -> Result<()> {
    let gated = gated!();
    run(Scenario::S02ADies(Death::PostgresKill), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s02_a_frozen_miners_move_to_b_and_a_resumes_without_double_pay() -> Result<()> {
    let gated = gated!();
    run(Scenario::S02ADies(Death::Freeze), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s02_a_frozen_again_and_again_mid_pull_never_stalls_b() -> Result<()> {
    let gated = gated!();
    run(Scenario::S02FrozenDuringPulls, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s02_a_puller_dying_mid_read_leaves_b_no_snapshot_and_flat_appends() -> Result<()> {
    let gated = gated!();
    run(Scenario::S02PullerDiesMidRead, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s02_a_network_dropped_miners_move_to_b_and_a_merges_on_heal() -> Result<()> {
    let gated = gated!();
    run(Scenario::S02ADies(Death::NetworkDrop), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s03_b_dies_a_is_unaffected_and_b_catches_up_on_return() -> Result<()> {
    let gated = gated!();
    run(Scenario::S03BDies, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the fast lane and the nightly matrix"]
async fn s04_link_cut_with_both_alive_costs_no_mining_and_sync_catches_up_on_heal() -> Result<()> {
    let gated = gated!();
    run(Scenario::S04LinkCut, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s04_a_transient_failure_applying_bs_block_is_retried_until_it_lands() -> Result<()> {
    let gated = gated!();
    run(Scenario::S04TransientApply, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the fast lane and the nightly matrix"]
async fn s05_both_nodes_writing_at_once_keep_every_invariant_and_b_stays_carry_free() -> Result<()>
{
    let gated = gated!();
    run(Scenario::S05BothWrite, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s06_a_restored_from_an_old_backup_recovers_its_own_rows_before_it_is_ready() -> Result<()>
{
    let gated = gated!();
    run(Scenario::S06Restore(Node::A), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s06_b_restored_from_an_old_backup_recovers_its_own_rows_before_it_is_ready() -> Result<()>
{
    let gated = gated!();
    run(Scenario::S06Restore(Node::B), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s06_a_restarts_and_serves_while_b_answers_but_its_ledger_is_locked() -> Result<()> {
    let gated = gated!();
    run(Scenario::S06PeerUnreadable(Unreadable::Locked), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s06_a_restarts_and_serves_while_b_answers_but_refuses_its_reads() -> Result<()> {
    let gated = gated!();
    run(Scenario::S06PeerUnreadable(Unreadable::Refused), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s07_b_disk_replaced_is_rebuilt_from_its_peer_and_loses_only_its_unsynced_tail(
) -> Result<()> {
    let gated = gated!();
    run(Scenario::S07DiskReplaced(Node::B), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s07_a_disk_replaced_is_rebuilt_from_its_peer_and_loses_only_its_unsynced_tail(
) -> Result<()> {
    let gated = gated!();
    run(Scenario::S07DiskReplaced(Node::A), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s08_a_block_lost_with_its_dying_node_is_never_paid() -> Result<()> {
    let gated = gated!();
    run(Scenario::S08BlockAtDeath(DeathAtFind::Lost), gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s08_a_block_accepted_as_its_node_dies_is_adopted_by_the_survivor_and_landed_once(
) -> Result<()> {
    let gated = gated!();
    run(
        Scenario::S08BlockAtDeath(DeathAtFind::Accepted { wait: true }),
        gated,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s08_without_the_peer_ingest_wait_a_block_accepted_as_its_node_dies_is_landed_once(
) -> Result<()> {
    let gated = gated!();
    run(
        Scenario::S08BlockAtDeath(DeathAtFind::Accepted { wait: false }),
        gated,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s09_a_3_0_ledger_cut_over_to_dual_mode_keeps_its_balances_and_audits() -> Result<()> {
    let gated = gated!();
    run(Scenario::S09Migration, gated).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the nightly matrix"]
async fn s11_carry_owner_transfer_is_refused_while_an_own_block_is_unknown_then_succeeds(
) -> Result<()> {
    let gated = gated!();
    run(Scenario::S11CarryOwnerTransfer, gated).await
}

/// The gate's inputs for the scenarios that also run 3.0.0-rc.6's server:
/// the usual two, then rc.6's server in [`Inputs::rc6_server`].
fn rc6_inputs(values: Vec<String>) -> Result<Inputs> {
    let [pg_bin, qbitd, rc6] = <[String; 3]>::try_from(values)
        .map_err(|_| anyhow::anyhow!("the gate returned the wrong number of inputs"))?;
    let mut inputs = inputs(vec![pg_bin, qbitd])?;
    inputs.rc6_server = Some(rc6.into());
    Ok(inputs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the fast lane and the nightly matrix"]
async fn r01_rc6_pair_serves_lands_and_restarts_while_3_1_migrates_its_database_to_31() -> Result<()>
{
    let gated = gate::required_inputs(
        gate::site!(),
        &[
            gate::Input::PgBinDir,
            gate::Input::QbitdBin,
            gate::Input::Rc6ServerBin,
        ],
    )?;
    run_with(Scenario::R01Rc6Premigrate, rc6_inputs(gated)?).await
}

/// [`premigrate::run`] with `plan`, its outcome written to
/// `<report root>/<name>/outcome.json`.
async fn premigrate_outcome(
    name: &str,
    plan: Plan,
    values: Vec<String>,
) -> Result<premigrate::Outcome> {
    let _serial = SERIAL.lock().await;
    let [pg_bin, rc6] = <[String; 2]>::try_from(values)
        .map_err(|_| anyhow::anyhow!("the gate returned the wrong number of inputs"))?;
    let (server, _) = binaries()?;
    let profile = profile_dir()?;
    let report = profile
        .parent()
        .context("profile directory has no parent")?
        .join("dual-sim-reports")
        .join(name);
    std::fs::create_dir_all(&report)?;
    let work = std::env::temp_dir().join("dual-sim").join(name);
    let outcome = premigrate::run(pg_bin.as_ref(), rc6.as_ref(), &server, &work, plan).await;
    // The cluster's statement log and the migrate's output, pass or fail.
    for file in ["postgres.log", "migrate-3.1.log"] {
        let _ = std::fs::copy(work.join(file), report.join(file));
    }
    let outcome = outcome?;
    std::fs::write(
        report.join("outcome.json"),
        serde_json::to_string_pretty(&outcome)?,
    )?;
    let _ = std::fs::remove_dir_all(&work);
    Ok(outcome)
}

/// Hold an outcome to the pre-migrate's guarantees: 27, 28 and 31 recorded
/// and the index valid; the ledger's partitions (p0 and its TOAST table
/// included) never rewritten, frozen, vacuumed or analyzed; no maintenance
/// statement; p0's index built CONCURRENTLY only; every append through it
/// succeeded within the frontends' 5 s lock timeout.
fn check_premigrate(outcome: &premigrate::Outcome) -> Result<()> {
    let mut failed = Vec::new();
    if !outcome.migrate_success
        || ![27, 28, 31].iter().all(|v| outcome.versions.contains(v))
        || !outcome.index_valid
    {
        failed.push(format!(
            "migrate success {}, recorded {:?}, index valid {}: {}",
            outcome.migrate_success, outcome.versions, outcome.index_valid, outcome.migrate_stdout
        ));
    }
    if outcome.storage_before != outcome.storage_after {
        failed.push(format!(
            "storage changed: before {:?}, after {:?}",
            outcome.storage_before, outcome.storage_after
        ));
    }
    if outcome.maintenance_before != outcome.maintenance_after {
        failed.push(format!(
            "maintenance counters changed: before {:?}, after {:?}",
            outcome.maintenance_before, outcome.maintenance_after
        ));
    }
    if !outcome.maintenance_statements.is_empty() {
        failed.push(format!(
            "maintenance statements: {:?}",
            outcome.maintenance_statements
        ));
    }
    if outcome.p0_index_builds.is_empty()
        || !outcome
            .p0_index_builds
            .iter()
            .all(|(concurrent, _)| *concurrent)
    {
        failed.push(format!("p0's index builds: {:?}", outcome.p0_index_builds));
    }
    if !outcome.appends.errors.is_empty() || outcome.appends.longest_ms >= 5_000 {
        failed.push(format!(
            "appends: {} errors {:?}, longest {} ms",
            outcome.appends.errors.len(),
            outcome.appends.errors.iter().take(3).collect::<Vec<_>>(),
            outcome.appends.longest_ms
        ));
    }
    eprintln!(
        "premigrate: {} rows in p0 ({} heap bytes); migrate {} ms; ledger lock hold {:?} ms, \
         transaction {:?} ms; p0 build {:?} ms, parent {:?} ms, attaches {:?} ms; \
         index {} bytes; temp files {} bytes; {} appends, longest {} ms",
        outcome.p0_rows,
        outcome.p0_heap_bytes,
        outcome.migrate_ms,
        outcome.ledger_lock_hold_ms,
        outcome.transaction_ms,
        outcome.p0_build_ms,
        outcome.parent_index_ms,
        outcome.attach_ms,
        outcome.p0_leaf_bytes,
        outcome.temp_file_bytes,
        outcome.appends.count,
        outcome.appends.longest_ms
    );
    if failed.is_empty() {
        Ok(())
    } else {
        bail!(
            "the pre-migrate broke its guarantees: {}",
            failed.join(" | ")
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dual-writer E2E: run explicitly by the fast lane and the nightly matrix"]
async fn r01_the_premigrate_never_rewrites_freezes_vacuums_or_analyzes_the_ledgers_data_partition(
) -> Result<()> {
    let gated = gate::required_inputs(
        gate::site!(),
        &[gate::Input::PgBinDir, gate::Input::Rc6ServerBin],
    )?;
    let plan = Plan {
        rows: 300_000,
        batch: 100_000,
        audit_snapshots: 690,
        maintenance_work_mem: "64MB".into(),
        maintenance_workers: 2,
        append_every_ms: 50,
    };
    let outcome = premigrate_outcome("r01-premigrate-p0", plan, gated).await?;
    check_premigrate(&outcome)
}

/// A setting of the by-hand measurement below, or its default.
fn measure_setting<T: std::str::FromStr>(name: &str, default: T) -> Result<T> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} is not valid: {value}")),
        Err(_) => Ok(default),
    }
}

/// The same run at scale, by hand: `PRISM_PREMIGRATE_ROWS` rows (default
/// 15,000,000, below p0's 2^24 bound), with `PRISM_PREMIGRATE_MWM`
/// (default 2GB) and `PRISM_PREMIGRATE_WORKERS` (default 4) as
/// `maintenance_work_mem` and `max_parallel_maintenance_workers`. It checks
/// the same guarantees and writes the measurement to
/// `target/<profile>/../dual-sim-reports/r01-premigrate-scale/outcome.json`.
/// No lane runs it: it takes a few minutes and tens of GB of disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: run by hand, see its comment"]
async fn r01_measure_the_premigrate_on_a_ledger_of_millions_of_rows() -> Result<()> {
    let gated = gate::required_inputs(
        gate::site!(),
        &[gate::Input::PgBinDir, gate::Input::Rc6ServerBin],
    )?;
    let plan = Plan {
        rows: measure_setting("PRISM_PREMIGRATE_ROWS", 15_000_000)?,
        batch: 1_000_000,
        audit_snapshots: 690,
        maintenance_work_mem: measure_setting("PRISM_PREMIGRATE_MWM", "2GB".to_owned())?,
        maintenance_workers: measure_setting("PRISM_PREMIGRATE_WORKERS", 4)?,
        append_every_ms: 100,
    };
    let outcome = premigrate_outcome("r01-premigrate-scale", plan, gated).await?;
    check_premigrate(&outcome)
}

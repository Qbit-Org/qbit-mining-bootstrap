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
    })
}

async fn run(scenario: Scenario, gated: Vec<String>) -> Result<()> {
    let _serial = SERIAL.lock().await;
    let inputs = inputs(gated)?;
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

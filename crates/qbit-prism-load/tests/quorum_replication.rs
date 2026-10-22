//! Replication detection against a real PostgreSQL 16 primary and standby.
//!
//! Gated on `PRISM_TEST_PG_BIN_DIR` through the shared integration gate: a
//! missing input skips with the gate's one line, or fails when the
//! `prism-native-postgres` job or `PRISM_TEST_REQUIRE_INTEGRATION=1` demands
//! the suite. The clusters are the harness's own managed ones, so the
//! quorum topology is brought up by the same code that runs `FIRST 1`.
//!
//! The managed cluster puts its Unix socket under the process temp directory.
//! On macOS the default one is deep enough to exceed PostgreSQL's 103-byte
//! socket path limit, so run this locally with `TMPDIR=/tmp`; the Linux CI
//! runner's `/tmp` is short enough as it is.

use anyhow::{Context, Result};
use qbit_prism_load::cluster::{
    self, detect_replication, ManagedPostgres, ObservedReplication, Replication, SynchronousMethod,
    QUORUM_SYNCHRONOUS_NAMES, STANDBY_NAME, SYNCHRONOUS_NAMES,
};
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::path::PathBuf;

/// What the primary reports for the one standby, once the synchronous
/// setting has applied: the setting itself and every `sync_state` row.
async fn observed(
    managed: &ManagedPostgres,
) -> Result<(String, Vec<Option<String>>, ObservedReplication)> {
    let pool = PgPool::connect(&managed.primary_url)
        .await
        .context("connect to the managed primary")?;
    let names: String = sqlx::query_scalar("SELECT current_setting('synchronous_standby_names')")
        .fetch_one(&pool)
        .await?;
    let states: Vec<Option<String>> =
        sqlx::query_scalar("SELECT sync_state FROM pg_stat_replication WHERE application_name=$1")
            .bind(STANDBY_NAME)
            .fetch_all(&pool)
            .await?;
    let detected = detect_replication(&pool).await;
    pool.close().await;
    Ok((names, states, detected))
}

/// An external cluster with `synchronous_standby_names = 'ANY 1 (...)'`
/// reports its standby as `quorum`, which `detect_replication` used to read
/// as `async`. Against a real PostgreSQL 16 primary and streaming standby the
/// quorum topology is observed as `sync`, alongside the managed `FIRST 1`
/// path, which still reports `sync` and is still the default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_any_1_quorum_standby_on_postgres_16_is_observed_as_sync_like_first_1() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let bin = PathBuf::from(bin);
    cluster::verify_bin_dir(&bin)?;

    // The external topology: `ANY 1`, the standby reported as `quorum`.
    let mut quorum = ManagedPostgres::start_with_method(
        bin.clone(),
        Replication::Sync,
        20,
        false,
        SynchronousMethod::Any,
    )
    .await
    .context("start the ANY 1 primary and standby")?;
    let (names, states, detected) = observed(&quorum).await?;
    quorum.stop();
    assert_eq!(names, QUORUM_SYNCHRONOUS_NAMES);
    assert_eq!(names, "ANY 1 (prism_standby_1)");
    assert_eq!(
        states,
        vec![Some("quorum".to_owned())],
        "PostgreSQL 16 reports an ANY-set candidate as quorum, not sync"
    );
    assert_eq!(
        detected,
        ObservedReplication::Observed {
            mode: Replication::Sync
        },
        "a quorum standby is synchronous"
    );

    // The managed default: `FIRST 1`, the standby reported as `sync`.
    let mut first = ManagedPostgres::start(bin, Replication::Sync, 20, false)
        .await
        .context("start the FIRST 1 primary and standby")?;
    assert_eq!(first.synchronous_method, SynchronousMethod::First);
    let (names, states, detected) = observed(&first).await?;
    first.stop();
    assert_eq!(names, SYNCHRONOUS_NAMES);
    assert_eq!(names, "FIRST 1 (prism_standby_1)");
    assert_eq!(states, vec![Some("sync".to_owned())]);
    assert_eq!(
        detected,
        ObservedReplication::Observed {
            mode: Replication::Sync
        }
    );
    Ok(())
}

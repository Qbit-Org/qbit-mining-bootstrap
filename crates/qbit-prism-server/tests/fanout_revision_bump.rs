//! A payout revision that moves between a job's build and its persistence is
//! caught by the shared clock+revision read: the persistence is refused and
//! no job row for the stale work is written, against a real coordinator and
//! PostgreSQL.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{coordinator::Coordinator, metrics::Metrics, stratum::MiningBackend};
use qbit_prism_test_gate as gate;
use std::{sync::Arc, time::Duration};

#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;

#[tokio::test]
async fn a_revision_bump_between_build_and_persist_refuses_the_stale_job() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "revision_bump_").await?;
    let result = async {
        let node = fake_qbitd::FakeNode::open().await?;
        let config = fake_qbitd::coordinator_config(db.url.clone(), &node, "revision-bump")?;
        let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
        coordinator.refresh_once().await?;
        let worker = MiningBackend::authorize(&*coordinator, "bump.worker").await?;
        let admin = sqlx::PgPool::connect(&db.url).await?;

        // Control: the same flow with an unchanged revision persists a row.
        let job = MiningBackend::build_job(&*coordinator, &worker, "00000001", 1e-12, 0.0).await?;
        MiningBackend::persist_issued_job(&*coordinator, &worker, &job, 0, Duration::from_secs(30))
            .await
            .context("control persistence")?;
        let stored: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(&job.wire.job_id)
                .fetch_one(&admin)
                .await?;
        ensure!(stored == 1, "control job was not persisted");

        // The revision moves after this build and before its persistence.
        let job = MiningBackend::build_job(&*coordinator, &worker, "00000002", 1e-12, 0.0).await?;
        let before: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&admin)
                .await?;
        ensure!(
            job.wire.payout_revision == before,
            "job carries revision {} not {before}",
            job.wire.payout_revision
        );
        sqlx::query(
            "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
        )
        .execute(&admin)
        .await?;
        let refused = MiningBackend::persist_issued_job(
            &*coordinator,
            &worker,
            &job,
            0,
            Duration::from_secs(30),
        )
        .await;
        ensure!(
            refused.is_err(),
            "persistence after a revision bump must be refused"
        );
        let stored: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(&job.wire.job_id)
                .fetch_one(&admin)
                .await?;
        ensure!(
            stored == 0,
            "a stale job row was written after the revision moved"
        );
        admin.close().await;
        Ok(())
    }
    .await;
    db.close(result).await
}

//! The cluster singleton row's lock matrix after #479: a job-persistence
//! fence held `FOR KEY SHARE` does not block the share append's non-key
//! `ledger_clock_ms` `UPDATE`, and still blocks every authority writer's
//! `SELECT … FOR UPDATE` (`lock_cluster_authority`) until it commits.
//! PRISM_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55483/postgres cargo test -p qbit-prism-server --test cluster_row_fences
use anyhow::{ensure, Context, Result};
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as gate;
use sqlx::{Connection, PgConnection, PgPool};
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const FENCE: &str =
    "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR KEY SHARE";
const CLOCK: &str = "UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint) WHERE singleton RETURNING ledger_clock_ms";
const AUTHORITY: &str = "SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE";

async fn waiting_on_a_lock(admin: &PgPool, application_name: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')",
    )
    .bind(application_name)
    .fetch_one(admin)
    .await?)
}

#[tokio::test]
async fn a_key_share_fence_lets_the_share_clock_through_and_still_blocks_authority_writers(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_fence_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let result = async {
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger = Ledger::connect(url.as_str(), "fence-test".into(), 4, true).await?;
        ledger.pool.close().await;
        let connect = |name: &str| {
            let mut url = url.clone();
            url.query_pairs_mut().append_pair("application_name", name);
            async move { PgConnection::connect(url.as_str()).await.context("connect") }
        };
        let mut cohort = connect("fence-cohort").await?;
        let mut append = connect("fence-append").await?;
        let mut authority = connect("fence-authority").await?;
        // The cohort holds its fence, as a batched issued-job save does until commit.
        let mut cohort_tx = cohort.begin().await?;
        let _fingerprint: Option<String> =
            sqlx::query_scalar(FENCE).fetch_one(&mut *cohort_tx).await?;
        // The share append's clock UPDATE does not wait for it.
        let mut append_tx = append.begin().await?;
        let clock: i64 = timeout(
            Duration::from_secs(5),
            sqlx::query_scalar(CLOCK).fetch_one(&mut *append_tx),
        )
        .await
        .context("the share append's clock UPDATE waited behind a KEY SHARE fence")??;
        ensure!(clock > 0, "the clock UPDATE returned nothing");
        append_tx.commit().await?;
        // An authority writer's exclusive lock does wait for the fence…
        let authority_task = tokio::spawn(async move {
            let mut tx = authority.begin().await?;
            sqlx::query(AUTHORITY).fetch_one(&mut *tx).await?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok::<_, anyhow::Error>(())
        });
        let mut saw_wait = false;
        for _ in 0..100 {
            if waiting_on_a_lock(&admin, "fence-authority").await? {
                saw_wait = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(
            saw_wait,
            "the authority writer did not wait behind the KEY SHARE fence"
        );
        ensure!(
            !authority_task.is_finished(),
            "the authority write committed while the fence was held"
        );
        // …and proceeds once the cohort commits.
        cohort_tx.commit().await?;
        timeout(Duration::from_secs(10), authority_task)
            .await
            .context("the authority write never proceeded after the fence was released")???;
        let revision: i64 =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut cohort)
                .await?;
        ensure!(revision >= 1, "the authority write did not land");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

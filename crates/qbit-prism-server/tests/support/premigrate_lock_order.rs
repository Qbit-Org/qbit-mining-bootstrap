//! #789: 3.1's `migrate` against a database rc.6 frontends still serve, as
//! the pre-migrate runs it. 027 alters a dozen tables in the migration
//! transaction. Applied as written, it held each table it had altered while
//! it waited for the next, so a frontend transaction that held the next one
//! and then read one already altered closed a lock cycle with it, which
//! PostgreSQL broke only after its 1 s `deadlock_timeout` by aborting one
//! side, while every other reader of the held table waited, past the 500 ms
//! lock timeout of rc.6's metrics collector. The migrate now locks all of
//! 027's and 028's tables at once, waiting at most 100 ms, and otherwise
//! rolls back and tries again (`ledger/migration/table_locks.rs`).
use super::*;
use qbit_prism_server::ledger::MigrateOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{sleep, timeout, Instant};

/// The lock timeout of rc.6's metrics collector, which in #789's second run
/// cancelled its read of `qbit_pool_blocks` behind 027.
const COLLECTOR_LOCK_TIMEOUT: Duration = Duration::from_millis(500);

async fn versions(pool: &PgPool) -> Result<Vec<i32>> {
    Ok(
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await?,
    )
}

/// #789's second run, held still. rc.6's CTV broadcaster has read
/// `qbit_ctv_fanout_artifacts` in a transaction that reads
/// `qbit_pool_blocks` next when 3.1's migrate starts on the database at 26.
/// Once the migrate holds, or is acquiring, `qbit_pool_blocks`, the
/// broadcaster reads it under the collector's 500 ms lock timeout. Applied
/// as written, 027 held `qbit_pool_blocks` while it waited for the
/// broadcaster's table, so that read waited until its timeout cancelled it
/// (and without one, until PostgreSQL aborted one side as a deadlock). Now
/// it is answered within the timeout, the migrate keeps giving up while the
/// broadcaster's transaction stays open, and commits 27 and 28 once it has
/// ended.
#[tokio::test]
async fn migration_027_backs_off_instead_of_closing_a_lock_cycle_with_a_serving_frontend(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    super::two_x::undo_028(&a.pool).await?;
    super::two_x::undo_027(&a.pool).await?;
    let before = versions(&a.pool).await?;
    ensure!(
        before.last() == Some(&26),
        "not at rc.6's schema: {before:?}"
    );

    // The migrate and the broadcaster run side by side in this task, so that
    // the migrate rolls back each attempt as soon as it gives up.
    let finished = AtomicBool::new(false);
    let migrate = async {
        let migrated = Ledger::connect_migrate(&db.url, MigrateOptions::default()).await;
        finished.store(true, Ordering::SeqCst);
        migrated
    };
    let broadcaster = async {
        let mut frontend = PgConnection::connect(&db.url).await?;
        sqlx::query("SELECT set_config('lock_timeout',$1,false)")
            .bind(COLLECTOR_LOCK_TIMEOUT.as_millis().to_string())
            .execute(&mut frontend)
            .await?;
        let mut broadcaster = frontend.begin().await?;
        sqlx::query("SELECT count(*) FROM qbit_ctv_fanout_artifacts")
            .execute(&mut *broadcaster)
            .await?;
        timeout(Duration::from_secs(60), async {
            while !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='relation' \
                 AND relation='qbit_pool_blocks'::regclass AND mode='AccessExclusiveLock')",
            )
            .fetch_one(&a.pool)
            .await?
            {
                ensure!(
                    !finished.load(Ordering::SeqCst),
                    "the migrate ended before it locked qbit_pool_blocks"
                );
                sleep(Duration::from_millis(2)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("the migrate never held or requested qbit_pool_blocks")??;

        let asked = Instant::now();
        let read = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_pool_blocks")
            .fetch_one(&mut *broadcaster)
            .await;
        let waited = asked.elapsed();
        read.context("the broadcaster's read of qbit_pool_blocks failed behind the migrate")?;
        ensure!(
            waited < COLLECTOR_LOCK_TIMEOUT,
            "the broadcaster's read of qbit_pool_blocks waited {waited:?} behind the migrate"
        );
        // While the broadcaster's transaction stays open, every attempt
        // gives up.
        sleep(Duration::from_millis(500)).await;
        ensure!(
            !finished.load(Ordering::SeqCst),
            "the migrate finished while the broadcaster held qbit_ctv_fanout_artifacts"
        );
        let during = versions(&a.pool).await?;
        ensure!(
            !during.contains(&27) && !during.contains(&28),
            "27 or 28 recorded while the broadcaster held its table: {during:?}"
        );
        broadcaster.commit().await?;
        frontend.close().await?;
        Ok::<_, anyhow::Error>(())
    };
    let (migrated, broadcast) = timeout(Duration::from_secs(180), async {
        tokio::join!(migrate, broadcaster)
    })
    .await
    .context("the migrate did not finish once the broadcaster's transaction ended")?;
    broadcast?;
    let migrated = migrated?;
    let after = versions(&a.pool).await?;
    ensure!(
        [27, 28, 31, 32]
            .iter()
            .all(|version| after.contains(version)),
        "the migrate finished without 27, 28, 31 and 32: {after:?}"
    );
    db.close(vec![a, migrated]).await
}

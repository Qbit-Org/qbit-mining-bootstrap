//! The offline epoch upgrade preserves accounting, refuses undeclared metadata
//! and never resets a durable counter on restart or concurrent migration.
use anyhow::{ensure, Result};
use qbit_prism_server::ledger::{
    ChainTransition, HeartbeatStatus, Ledger, REQUIRED_SCHEMA_VERSIONS,
};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sqlx::PgPool;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;

async fn pre_epoch(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql("ALTER TABLE qbit_prism_cluster DROP COLUMN chain_epoch; DELETE FROM qbit_prism_schema_capabilities WHERE capability='chain_observation_epoch'; DELETE FROM qbit_prism_schema_migrations WHERE version=18")
        .execute(pool).await?;
    Ok(())
}

async fn unrecorded(pool: &PgPool) -> Result<()> {
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_migrations WHERE version=18) OR EXISTS(SELECT 1 FROM qbit_prism_schema_capabilities WHERE capability='chain_observation_epoch')")
        .fetch_one(pool).await?;
    ensure!(!recorded, "failed migration recorded epoch metadata");
    Ok(())
}

#[tokio::test]
async fn epoch_upgrade_requires_shutdown_preserves_checkpoint_and_survives_concurrent_restart(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "epoch_upgrade_").await?;
    let old = Ledger::connect(&db.url, "old-writer".into(), 4, true).await?;
    let result = async {
        old.observe_chain_view(&"ab".repeat(32), 100, "04").await?;
        pre_epoch(&old.pool).await?;
        let before: Value =
            sqlx::query_scalar("SELECT to_jsonb(c) FROM qbit_prism_cluster c WHERE singleton")
                .fetch_one(&old.pool)
                .await?;
        // A stale heartbeat still belongs to a writer, even with no candidate.
        sqlx::query(
            "UPDATE qbit_prism_instances SET heartbeat_at=clock_timestamp()-interval '1 day'",
        )
        .execute(&old.pool)
        .await?;
        let error = Ledger::connect(&db.url, "new-writer".into(), 4, true)
            .await
            .err()
            .expect("running old writer accepted");
        ensure!(
            error
                .to_string()
                .contains("migration 018 requires every earlier instance"),
            "{error:#}"
        );
        unrecorded(&old.pool).await?;
        old.heartbeat(HeartbeatStatus::Stopped).await?;
        let (a, b) = tokio::try_join!(
            Ledger::connect(&db.url, "epoch-a".into(), 4, true),
            Ledger::connect(&db.url, "epoch-b".into(), 4, true)
        )?;
        let upgraded = async {
            let after: Value = sqlx::query_scalar(
                "SELECT to_jsonb(c)-'chain_epoch' FROM qbit_prism_cluster c WHERE singleton",
            )
            .fetch_one(&a.pool)
            .await?;
            ensure!(
                before == after,
                "epoch migration changed existing cluster state"
            );
            let state = a.chain_observation_state().await?;
            ensure!(
                state.chain_epoch == 0
                    && state.best_tip_hash.as_deref() == Some("ab".repeat(32).as_str())
            );
            let witness = ChainTransition {
                predecessor: "ab".repeat(32),
                origin_chain_epoch: 0,
            };
            let next = a
                .observe_chain_transition(&witness, &"cd".repeat(32), 100, "04", &state)
                .await?;
            ensure!(next == state.payout_revision + 1);
            ensure!(b.chain_observation_state().await?.chain_epoch == 1);
            let restarted = Ledger::connect(&db.url, "epoch-restart".into(), 4, true).await?;
            let observed = restarted.chain_observation_state().await?;
            restarted.pool.close().await;
            ensure!(observed.chain_epoch == 1, "restart reset the epoch");
            let versions: Vec<i32> = sqlx::query_scalar(
                "SELECT version FROM qbit_prism_schema_migrations ORDER BY version",
            )
            .fetch_all(&a.pool)
            .await?;
            ensure!(versions == REQUIRED_SCHEMA_VERSIONS);
            Ok(())
        }
        .await;
        a.pool.close().await;
        b.pool.close().await;
        upgraded
    }
    .await;
    old.pool.close().await;
    db.close(result).await
}

#[tokio::test]
async fn epoch_upgrade_collision_and_rollback_leave_no_partial_metadata() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "epoch_collision_").await?;
    let old = Ledger::connect(&db.url, "old-writer".into(), 4, true).await?;
    let result = async {
        old.heartbeat(HeartbeatStatus::Stopped).await?;
        pre_epoch(&old.pool).await?;
        sqlx::query("ALTER TABLE qbit_prism_cluster ADD COLUMN chain_epoch text DEFAULT 'foreign'").execute(&old.pool).await?;
        ensure!(Ledger::connect(&db.url, "collision".into(), 4, true).await.is_err());
        unrecorded(&old.pool).await?;
        let foreign: String = sqlx::query_scalar("SELECT chain_epoch::text FROM qbit_prism_cluster WHERE singleton").fetch_one(&old.pool).await?;
        ensure!(foreign == "foreign");
        sqlx::query("ALTER TABLE qbit_prism_cluster DROP COLUMN chain_epoch").execute(&old.pool).await?;
        // A real transaction rolls back the new column and capability together.
        let mut tx = old.pool.begin().await?;
        sqlx::raw_sql(include_str!("../migrations/018_chain_observation_epoch.sql")).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(18)").execute(&mut *tx).await?;
        tx.rollback().await?;
        unrecorded(&old.pool).await?;
        let present: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='qbit_prism_cluster' AND column_name='chain_epoch')").fetch_one(&old.pool).await?;
        ensure!(!present);
        let migrated = Ledger::connect(&db.url, "after-rollback".into(), 4, true).await?;
        let epoch = migrated.chain_observation_state().await?.chain_epoch;
        migrated.pool.close().await;
        ensure!(epoch == 0);
        Ok(())
    }.await;
    old.pool.close().await;
    db.close(result).await
}

#[tokio::test]
async fn epoch_updates_are_atomic_and_accounting_does_not_advance_them() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "epoch_atomic_").await?;
    let ledger = Ledger::connect(&db.url, "epoch-atomic".into(), 4, true).await?;
    let result = async {
        ensure!(ledger.chain_observation_state().await?.chain_epoch == 0);
        ledger
            .observe_chain_view(&"ab".repeat(32), 100, "04")
            .await?;
        let initial = ledger.chain_observation_state().await?;
        ensure!(initial.chain_epoch == 1);
        ledger
            .observe_chain_view(&"ab".repeat(32), 100, "04")
            .await?;
        ensure!(ledger.chain_observation_state().await?.chain_epoch == 1);
        sqlx::query(
            "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+3 WHERE singleton",
        )
        .execute(&ledger.pool)
        .await?;
        ensure!(ledger.chain_observation_state().await?.chain_epoch == 1);
        ensure!(ledger
            .observe_chain_view(&"cd".repeat(32), 99, "03")
            .await
            .is_err());
        ensure!(ledger.chain_observation_state().await?.chain_epoch == 1);
        ledger
            .observe_chain_view(&"cd".repeat(32), 99, "05")
            .await?;
        let advanced = ledger.chain_observation_state().await?;
        ensure!(
            advanced.chain_epoch == 2 && advanced.payout_revision == initial.payout_revision + 4
        );
        // bigint overflow must roll back the coupled tip/revision update.
        sqlx::query("UPDATE qbit_prism_cluster SET chain_epoch=$1 WHERE singleton")
            .bind(i64::MAX)
            .execute(&ledger.pool)
            .await?;
        ensure!(ledger
            .observe_chain_view(&"ef".repeat(32), 101, "06")
            .await
            .is_err());
        let refused = ledger.chain_observation_state().await?;
        ensure!(
            refused.chain_epoch == i64::MAX
                && refused.payout_revision == advanced.payout_revision
                && refused.best_tip_hash == advanced.best_tip_hash
        );
        ensure!(
            sqlx::query("UPDATE qbit_prism_cluster SET chain_epoch=-1 WHERE singleton")
                .execute(&ledger.pool)
                .await
                .is_err()
        );
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test]
async fn cancelled_epoch_migrator_rolls_back_before_commit() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "epoch_cancel_").await?;
    let old = Ledger::connect(&db.url, "old-writer".into(), 4, true).await?;
    let result = tokio::task::LocalSet::new().run_until(async {
        old.heartbeat(HeartbeatStatus::Stopped).await?;
        pre_epoch(&old.pool).await?;
        let mut blocker = old.pool.begin().await?;
        sqlx::query("LOCK TABLE qbit_prism_cluster IN ACCESS SHARE MODE").execute(&mut *blocker).await?;
        let url = db.url.clone();
        let mut pending = tokio::task::spawn_local(async move { Ledger::connect(&url, "cancel-migration".into(), 4, true).await });
        let observed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%ADD COLUMN chain_epoch bigint%')").fetch_one(&old.pool).await?;
                if waiting { return Ok::<_, anyhow::Error>(()); }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await;
        pending.abort();
        let cancelled = (&mut pending).await;
        blocker.rollback().await?;
        observed??;
        ensure!(cancelled.is_err_and(|error| error.is_cancelled()));
        unrecorded(&old.pool).await?;
        let migrated = Ledger::connect(&db.url, "after-cancel".into(), 4, true).await?;
        ensure!(migrated.chain_observation_state().await?.chain_epoch == 0);
        migrated.pool.close().await;
        Ok(())
    }).await;
    old.pool.close().await;
    db.close(result).await
}

#[tokio::test]
async fn epoch_metadata_refuses_missing_declarations_and_undeclared_collisions() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "epoch_metadata_").await?;
    let ledger = Ledger::connect(&db.url, "epoch-metadata".into(), 4, true).await?;
    let result = async {
        for (damage, restore) in [
            ("DELETE FROM qbit_prism_schema_capabilities WHERE capability='chain_observation_epoch'", "INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('chain_observation_epoch',1)"),
            ("UPDATE qbit_prism_schema_capabilities SET capability_value=2 WHERE capability='chain_observation_epoch'", "UPDATE qbit_prism_schema_capabilities SET capability_value=1 WHERE capability='chain_observation_epoch'"),
            ("ALTER TABLE qbit_prism_cluster RENAME COLUMN chain_epoch TO saved_chain_epoch", "ALTER TABLE qbit_prism_cluster RENAME COLUMN saved_chain_epoch TO chain_epoch"),
        ] {
            sqlx::raw_sql(damage).execute(&ledger.pool).await?;
            for initialize in [false,true] {
                let error = Ledger::connect(&db.url, "damaged-epoch".into(), 4, initialize).await.err().expect("startup accepted damaged epoch metadata");
                ensure!(format!("{error:#}").contains("chain_epoch") || format!("{error:#}").contains("chain_observation_epoch"), "{error:#}");
            }
            sqlx::raw_sql(restore).execute(&ledger.pool).await?;
        }
        ledger.heartbeat(HeartbeatStatus::Stopped).await?;
        pre_epoch(&ledger.pool).await?;
        // A declaration without its recorded migration is not overwritten.
        sqlx::query("INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('chain_observation_epoch',1)").execute(&ledger.pool).await?;
        ensure!(Ledger::connect(&db.url, "undeclared-epoch".into(), 4, true).await.is_err());
        let recorded: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_migrations WHERE version=18)").fetch_one(&ledger.pool).await?;
        let present: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='qbit_prism_cluster' AND column_name='chain_epoch')").fetch_one(&ledger.pool).await?;
        let capability: i32 = sqlx::query_scalar("SELECT capability_value FROM qbit_prism_schema_capabilities WHERE capability='chain_observation_epoch'").fetch_one(&ledger.pool).await?;
        ensure!(!recorded && !present && capability == 1, "collision left partial schema or changed the preexisting capability");
        Ok(())
    }.await;
    ledger.pool.close().await;
    db.close(result).await
}

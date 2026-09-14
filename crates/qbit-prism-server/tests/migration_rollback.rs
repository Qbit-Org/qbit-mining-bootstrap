use anyhow::{ensure, Result};
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;

#[path = "support/recovery.rs"]
mod recovery;

#[tokio::test]
async fn legacy_ordinal_revert_refuses_native_schema_without_removing_columns() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_revert_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let ledger = Ledger::connect(url.as_str(), "rollback-guard-test".into(), 2, true).await?;
    let result = async {
        let mut conn = ledger.pool.acquire().await?;
        let error = sqlx::raw_sql(include_str!(
            "../../qbit-prism/sql/001_share_ledger_revert_audit_publication_sequence.sql"
        ))
        .execute(&mut *conn)
        .await
        .expect_err("legacy destructive revert must refuse native databases");
        ensure!(error
            .to_string()
            .contains("cannot run against native Prism"));
        let message = error.to_string();
        ensure!(message.contains("one-way migration"));
        ensure!(message.contains("docs/prism-rust-migration.md#recovery-and-rollback"));
        ensure!(
            message.contains("never discard acknowledged shares without accounting reconciliation")
        );
        sqlx::query("ROLLBACK").execute(&mut *conn).await?;
        // Preparing this read proves both the retained publication ordinal and
        // the native reorg column survive the rejected destructive operation.
        sqlx::query(
            "SELECT audit_publication_sequence, inactive_since FROM qbit_pool_blocks LIMIT 1",
        )
        .fetch_optional(&mut *conn)
        .await?;
        let versions: i32 =
            sqlx::query_scalar("SELECT max(version) FROM qbit_prism_schema_migrations")
                .fetch_one(&mut *conn)
                .await?;
        ensure!(versions >= 3);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

#[tokio::test]
async fn frozen_2x_cli_import_restores_all_body_layouts_and_serves_exact_artifacts() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = recovery::Database::open(&raw).await?;
    let result = async {
        let artifacts_dir = tempfile::tempdir()?;
        let artifacts = recovery::seed_legacy(&db.pool, artifacts_dir.path()).await?;
        let before = recovery::accounting_state(&db.pool).await?;
        let ledger = Ledger::connect_operator(&db.url, true).await?;
        ledger.pool.close().await;
        ensure!(recovery::import_cli(&db, artifacts_dir.path())
            .await?
            .contains("Imported 3 audit bodies"));
        // Every original external file is now unavailable, including the
        // sidecar-only body's gzip. Public reads must use shared PostgreSQL.
        artifacts_dir.close()?;
        recovery::assert_artifacts(&db.pool, &artifacts).await?;
        ensure!(recovery::accounting_state(&db.pool).await? == before);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}

#[tokio::test]
async fn frozen_2x_backup_restore_reconciles_before_ack_and_exposes_post_ack_loss() -> Result<()> {
    let Some(inputs) = gate::inputs(
        gate::site!(),
        &[gate::Input::DatabaseUrl, gate::Input::PgBinDir],
    )?
    else {
        return Ok(());
    };
    let raw = &inputs[0];
    let pg_bin = std::path::Path::new(&inputs[1]);
    let source = recovery::Database::open(raw).await?;
    let restored = recovery::Database::open(raw).await?;
    let result = async {
        let artifacts_dir = tempfile::tempdir()?;
        let artifacts = recovery::seed_legacy(&source.pool, artifacts_dir.path()).await?;
        let before = recovery::accounting_state(&source.pool).await?;
        let source_evidence = recovery::evidence(&source, pg_bin).await?;
        ensure!(
            recovery::evidence_with_bytea(&source, pg_bin, "escape").await? == source_evidence,
            "connection bytea defaults changed the recovery fingerprints"
        );
        ensure!(source_evidence["accepted_shares"] == 3);
        ensure!(source_evidence["last_share_seq"] == 8);
        ensure!(source_evidence["records"]["audits"]["count"] == 3);
        ensure!(source_evidence["carry_forward_integrity"]["mismatch_count"] == 0);
        // Independent 2.x chain calculation over the three fixed seed rows.
        // Comparing two exports alone would miss a consistently wrong head.
        ensure!(
            source_evidence["audit_head_sha256"]
                == "848c67eafac5e3545df6d79ba68994a2fe1d98641ea28f131147e09c464a05f8"
        );
        let archive = recovery::backup(&source, pg_bin).await?;
        let ledger = Ledger::connect_operator(&source.url, true).await?;
        ensure!(recovery::import_cli(&source, artifacts_dir.path())
            .await?
            .contains("Imported 3 audit bodies"));
        recovery::assert_artifacts(&source.pool, &artifacts).await?;
        ensure!(recovery::accounting_state(&source.pool).await? == before);
        ensure!(recovery::evidence(&source, pg_bin).await? == source_evidence);
        recovery::restore(&archive, &source, &restored, pg_bin).await?;
        ensure!(recovery::accounting_state(&restored.pool).await? == before);
        ensure!(recovery::evidence(&restored, pg_bin).await? == source_evidence);
        let native_tables_absent: bool =
            sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_migrations') IS NULL")
                .fetch_one(&restored.pool)
                .await?;
        ensure!(
            native_tables_absent,
            "restore must be the isolated pre-migration database"
        );

        // append() returns only after the durable COMMIT: this is the storage
        // acknowledgement boundary used by Stratum, not a fabricated SQL row.
        let mut share = recovery::share(9);
        share.share_seq = 0;
        share.job_issued_at_ms = 1;
        let acknowledged = ledger.append(share, None).await?;
        ensure!(acknowledged.inserted);
        ensure!(
            acknowledged.share.share_seq > 40,
            "migration lost the source sequence high-water mark"
        );
        let after = recovery::evidence(&source, pg_bin).await?;
        ensure!(after["accepted_shares"] == 4);
        ensure!(
            after["records"]["shares"]["sha256"] != source_evidence["records"]["shares"]["sha256"]
        );
        ensure!(after["audit_head_sha256"] == source_evidence["audit_head_sha256"]);
        ensure!(after["records"]["audits"] == source_evidence["records"]["audits"]);
        ensure!(recovery::evidence(&restored, pg_bin).await? == source_evidence);
        let lost_on_restore: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
                .bind(&acknowledged.share.share_id)
                .fetch_one(&restored.pool)
                .await?;
        ensure!(
            lost_on_restore == 0,
            "older restore unexpectedly contains the post-cutover ACK"
        );
        ledger.pool.close().await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    source.close().await?;
    restored.close().await?;
    result
}

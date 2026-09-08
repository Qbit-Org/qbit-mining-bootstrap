use anyhow::{ensure, Result};
use qbit_prism_server::ledger::Ledger;
use sqlx::PgPool;

#[tokio::test]
async fn legacy_ordinal_revert_refuses_native_schema_without_removing_columns() -> Result<()> {
    let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
        eprintln!("set PRISM_TEST_DATABASE_URL for the native rollback guard test");
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

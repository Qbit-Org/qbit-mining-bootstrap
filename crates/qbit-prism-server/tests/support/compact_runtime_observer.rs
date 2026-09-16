//! Measurements around real operations on an isolated PostgreSQL primary.
//! AFTER ROW notices record writes without writing an observer table or WAL.
//! Install before the bracket; discovery and measurement queries stay outside it.
use super::jsonb_inventory::Inventory;
use super::proxy::{ExecutionProxy, JsonbWrites, JSONB_WRITE_PREFIX};
use anyhow::{ensure, Context, Result};
use sqlx::PgPool;
use std::collections::BTreeMap;

fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub struct JsonbProbe {
    _installed: (),
}

impl JsonbProbe {
    /// Discover every JSONB column, including tables added after the baseline.
    /// All instrumentation lives in the disposable schema and commits together.
    pub async fn install(pool: &PgPool, schema: &str) -> Result<Self> {
        let inventory = Inventory::discover(pool, schema).await?;
        let mut tables: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (table, column) in inventory.columns {
            tables.entry(table).or_default().push(column);
        }
        let schema = identifier(schema);
        let mut tx = pool.begin().await?;
        for (index, (table, columns)) in tables.into_iter().enumerate() {
            let function = format!("{schema}.compact_runtime_jsonb_{index}");
            let values = columns
                .into_iter()
                .map(|column| {
                    format!(
                        "{},pg_column_size(NEW.{}::text::jsonb)",
                        literal(&column),
                        identifier(&column)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $probe$ \
                 BEGIN RAISE NOTICE '{JSONB_WRITE_PREFIX}%', \
                 json_build_object('table',TG_TABLE_NAME,'operation',TG_OP,\
                 'values',jsonb_strip_nulls(jsonb_build_object({values})))::text; \
                 RETURN NULL; END $probe$; \
                 CREATE TRIGGER compact_runtime_jsonb AFTER INSERT OR UPDATE \
                 ON {schema}.{} FOR EACH ROW EXECUTE FUNCTION {function}()",
                identifier(&table),
            );
            sqlx::raw_sql(&sql).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(Self { _installed: () })
    }

    /// Successful zero means no observed row write. A failed/incomplete proxy
    /// or database operation is an error, never an empty healthy measurement.
    /// Row writes include unchanged values and repeated writes in one transaction;
    /// they are not a claim that each value was newly serialized to TOAST or that
    /// a later transaction committed. Record operation outcome separately.
    pub fn measure(&self, proxy: &ExecutionProxy, mark: u64) -> Result<JsonbWrites> {
        ensure!(
            proxy.rejections_since(mark)?.is_empty(),
            "database rejected an observed operation"
        );
        let executions = proxy.executions_since(mark)?;
        let mut total = JsonbWrites::default();
        for execution in executions {
            ensure!(
                execution.complete_response(),
                "operation completion is unavailable"
            );
            for measured in execution.jsonb_writes.values() {
                total.rows = total
                    .rows
                    .checked_add(measured.rows)
                    .context("row count overflow")?;
                total.values = total
                    .values
                    .checked_add(measured.values)
                    .context("value count overflow")?;
                total.max_uncompressed_bytes = total
                    .max_uncompressed_bytes
                    .max(measured.max_uncompressed_bytes);
            }
        }
        Ok(total)
    }
}

/// Insert LSN, not the asynchronously advanced flush/write position.
pub async fn insert_lsn(pool: &PgPool) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
            .fetch_one(pool)
            .await?,
    )
}

/// Both boundaries were captured already; this query adds no work to the bracket.
pub async fn wal_bytes(pool: &PgPool, before: &str, after: &str) -> Result<u64> {
    let bytes: i64 = sqlx::query_scalar("SELECT pg_wal_lsn_diff($2::pg_lsn,$1::pg_lsn)::bigint")
        .bind(before)
        .bind(after)
        .fetch_one(pool)
        .await?;
    u64::try_from(bytes).context("WAL insert position moved backwards")
}

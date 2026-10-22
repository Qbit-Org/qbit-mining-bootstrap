//! Migration 017, the share ledger partition conversion, applied outside
//! the migration transaction on a ledger that has rows (#144).
//!
//! The conversion attaches the release table as the first partition of a
//! new partitioned parent instead of copying it: with a validated
//! `CHECK (share_seq < bound)` on the release table, `ATTACH PARTITION`
//! proves the bound from the constraint and adopts every existing index,
//! so the swap is catalog work, milliseconds whatever the table holds. The
//! validation is the only pass over the data, and it runs under SHARE
//! UPDATE EXCLUSIVE, which blocks neither appends nor reads. The three
//! steps are the SQL functions migration 016 installs; this runner orders
//! them on a dedicated connection, each in its own transaction, because the
//! validation can take hours on a large ledger and the migration
//! transaction's statement timeout and lock hold would not survive it, and
//! because the swap's ACCESS EXCLUSIVE lock must be taken with a short
//! lock timeout and retried rather than queued behind a long read while
//! every append queues behind it. Where the ledger is empty (a fresh
//! deployment, an empty 2.x.x source), `migrate_schema` applies the
//! migration file inside its transaction instead, under the cutover locks
//! that exclude writers; the file runs the same functions.
//!
//! The runner is resumable from what the database holds: a plain table
//! without the bound is prepared; a pending or validated bound is bounded
//! again, further out, when the sequence has come within one partition of
//! it (and validated again); a pending bound is validated; a validated
//! bound is swapped; a converted ledger is recorded. Every
//! name the swap creates is checked before the first step, so a refusal
//! changes nothing. Until 17 is recorded every start refuses the database,
//! as for every other required migration.
use super::online::{acquire_runner_lock, recorded};
use super::*;
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};

/// The conversion migration 017 declares. Nothing is derived from the
/// scratch apply beyond the proof that the file converts the table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartitionMigration {
    pub(super) version: i32,
}

/// How long the prepare and swap steps keep retrying for the table lock
/// before giving up; each attempt waits `LOCK_ATTEMPT` at most, so a long
/// payout-window read delays the step without stalling the appends queued
/// behind an ACCESS EXCLUSIVE request.
const LOCK_RETRY_BUDGET: Duration = Duration::from_secs(600);
const LOCK_ATTEMPT_MS: &str = "2000";

/// Confirm, from the scratch apply, that the migration file converts the
/// ledger: the parent has `qbit_share_ledger_p0` as a partition afterwards
/// and had none before.
pub(super) fn derive(
    version: i32,
    before: &SchemaFingerprint,
    after: &SchemaFingerprint,
) -> Result<PartitionMigration> {
    let plain = before
        .tables
        .get("qbit_share_ledger")
        .with_context(|| format!("migration {version}: qbit_share_ledger is missing before it"))?;
    ensure!(
        plain.children.is_empty(),
        "migration {version}: qbit_share_ledger already has partitions before it"
    );
    let parent = after
        .tables
        .get("qbit_share_ledger")
        .with_context(|| format!("migration {version}: qbit_share_ledger is missing after it"))?;
    ensure!(
        parent
            .children
            .iter()
            .any(|child| child == "qbit_share_ledger_p0"),
        "migration {version}: qbit_share_ledger_p0 is not a partition of qbit_share_ledger after it"
    );
    Ok(PartitionMigration { version })
}

/// What the ledger looks like to the runner, read afresh before each step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// The release table, no bound yet.
    Plain,
    /// The bound is on the table, enforced on new rows, not yet validated.
    Pending,
    /// The bound is validated: the swap can attach without a scan.
    Validated,
    /// The parent is partitioned and the catalog records the conversion.
    Converted,
}

async fn stage(connection: &mut PgConnection) -> Result<Stage> {
    let row = sqlx::query(
        "SELECT c.relkind::text AS kind,k.convalidated AS validated,(SELECT converted_at IS NOT NULL FROM qbit_prism_share_partitioning WHERE singleton) AS converted FROM pg_class c LEFT JOIN pg_constraint k ON k.conrelid=c.oid AND k.conname='qbit_share_ledger_p0_bound' WHERE c.oid=to_regclass('qbit_share_ledger')",
    )
    .fetch_optional(&mut *connection)
    .await?
    .context("qbit_share_ledger does not exist")?;
    let kind: String = row.try_get("kind")?;
    let validated: Option<bool> = row.try_get("validated")?;
    let converted: Option<bool> = row.try_get("converted")?;
    match (kind.as_str(), validated, converted) {
        ("p", _, Some(true)) => Ok(Stage::Converted),
        ("p", _, _) => bail!(
            "qbit_share_ledger is a partitioned table but qbit_prism_share_partitioning does not record a conversion; the migrator did not partition it. Check what did, then restore the full backup or move the table aside and migrate again"
        ),
        ("r", None, _) => Ok(Stage::Plain),
        ("r", Some(false), _) => Ok(Stage::Pending),
        ("r", Some(true), _) => Ok(Stage::Validated),
        (kind, ..) => bail!(
            "qbit_share_ledger is a relation of kind {kind}, not a table; the migrator cannot partition it"
        ),
    }
}

/// Every name the swap and the first lead partitions take, refused before
/// any step when held by any relation: a relation under a partition's name
/// would be refused by the swap after the bound was prepared, and the
/// promise that a refusal changed nothing must hold for the whole run.
async fn refuse_reserved_names(connection: &mut PgConnection, version: i32) -> Result<()> {
    let held: Vec<(String, String)> = sqlx::query_as(
        "SELECT c.relname::text,c.relkind::text FROM pg_class c WHERE c.relnamespace=current_schema()::regnamespace AND (c.relname ~ '^qbit_share_ledger_p[0-9]+(_.*)?$') ORDER BY 1",
    )
    .fetch_all(&mut *connection)
    .await?;
    ensure!(
        held.is_empty(),
        "refusing to apply migration {version}: {} already hold names the share ledger partitions take. The migration is not recorded and nothing was changed by it. Check what they hold, then rename or move them aside and migrate again",
        held.iter()
            .map(|(name, kind)| format!("{} {name}", super::online::relation_kind(kind)))
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(())
}

/// Run one statement that needs a table lock, retrying on `lock_timeout`
/// within the budget. The session's `lock_timeout` is set for the
/// statement and reset afterwards.
async fn with_lock_retries(
    connection: &mut PgConnection,
    version: i32,
    what: &str,
    statement: &str,
) -> Result<()> {
    sqlx::query("SELECT set_config('lock_timeout',$1,false)")
        .bind(LOCK_ATTEMPT_MS)
        .execute(&mut *connection)
        .await?;
    let started = Instant::now();
    let outcome = loop {
        match sqlx::raw_sql(statement).execute(&mut *connection).await {
            Ok(_) => break Ok(()),
            Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => {
                if started.elapsed() >= LOCK_RETRY_BUDGET {
                    break Err(anyhow::anyhow!(
                        "migration {version}: {what} could not take its lock on qbit_share_ledger within {} s; a transaction has held the table throughout (a long read, an open writer). Let it finish, or stop the frontends, and migrate again",
                        LOCK_RETRY_BUDGET.as_secs()
                    ));
                }
                tracing::warn!(
                    version,
                    step = what,
                    waited_s = started.elapsed().as_secs(),
                    "table lock not available; retrying so queued appends are not held behind the request"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => break Err(error.into()),
        }
    };
    sqlx::query("SELECT set_config('lock_timeout','0',false)")
        .execute(&mut *connection)
        .await?;
    outcome
}

/// Apply the conversion to the source and record it. Idempotent: each run
/// resumes from the stage it finds, and a version another instance recorded
/// meanwhile is not applied again.
pub(super) async fn apply(
    connection: &mut PgConnection,
    migration: &PartitionMigration,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    let version = migration.version;
    sqlx::query(
        "SELECT set_config('statement_timeout','0',false),set_config('lock_timeout','0',false)",
    )
    .execute(&mut *connection)
    .await?;
    acquire_runner_lock(connection).await?;
    if recorded(connection, version).await? {
        tracing::info!(version, "online migration already recorded");
        return Ok(());
    }
    let mut current = stage(connection).await?;
    if current == Stage::Plain {
        refuse_reserved_names(connection, version).await?;
    }
    loop {
        match current {
            Stage::Plain => {
                with_lock_retries(
                    connection,
                    version,
                    "preparing the partition bound",
                    "SELECT qbit_prism_share_ledger_convert_prepare()",
                )
                .await?;
                tracing::info!(version, "share ledger bound prepared; validating it next");
            }
            Stage::Pending => {
                // The bound may have lost its headroom while this run was
                // away; prepare again first, which bounds further out in
                // that case and otherwise keeps the pending constraint.
                with_lock_retries(
                    connection,
                    version,
                    "checking the partition bound",
                    "SELECT qbit_prism_share_ledger_convert_prepare()",
                )
                .await?;
                let started = Instant::now();
                tracing::info!(
                    version,
                    "validating the share ledger bound: one scan of the table per pending constraint, appends and reads continue"
                );
                let validated: Vec<String> =
                    sqlx::query_scalar("SELECT qbit_prism_share_ledger_convert_validate()")
                        .fetch_one(&mut *connection)
                        .await
                        .with_context(|| format!("validating the share ledger bound for migration {version}; migrate again to resume"))?;
                tracing::info!(
                    version,
                    constraints = validated.join(", "),
                    elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "share ledger constraints validated"
                );
            }
            Stage::Validated => {
                // A validated bound can still have lost its headroom while
                // this run was away, and the swap refuses a bound the
                // sequence has reached. Prepare again first: it keeps a bound
                // with headroom and otherwise replaces it with a pending one
                // further out, which the next round validates.
                with_lock_retries(
                    connection,
                    version,
                    "checking the partition bound",
                    "SELECT qbit_prism_share_ledger_convert_prepare()",
                )
                .await?;
                if stage(connection).await? != Stage::Validated {
                    tracing::warn!(
                        version,
                        "the validated bound lost its headroom while this run was away; bounded again further out, validating again"
                    );
                    current = Stage::Pending;
                    continue;
                }
                let started = Instant::now();
                with_lock_retries(
                    connection,
                    version,
                    "swapping the share ledger",
                    "SELECT qbit_prism_share_ledger_convert_swap()",
                )
                .await?;
                tracing::info!(
                    version,
                    elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "share ledger converted: the release table is partition qbit_share_ledger_p0"
                );
            }
            Stage::Converted => break,
        }
        current = stage(connection).await?;
    }
    let created: i32 = sqlx::query_scalar("SELECT qbit_prism_share_partition_ensure()")
        .fetch_one(&mut *connection)
        .await?;
    if created > 0 {
        tracing::info!(version, created, "share ledger partitions created ahead");
    }
    let mut tx = connection.begin().await?;
    lock(&mut tx, MIGRATION_LOCK, metrics).await?;
    ensure!(
        stage(&mut tx).await? == Stage::Converted,
        "refusing to record migration {version}: qbit_share_ledger is no longer the converted table this run left; migrate again"
    );
    sqlx::query(
        "INSERT INTO qbit_prism_schema_migrations(version) VALUES($1) ON CONFLICT (version) DO NOTHING",
    )
    .bind(version)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::info!(version, "online migration recorded");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(children: &[&str]) -> TableDefinition {
        TableDefinition {
            persistence: "p".into(),
            row_security: false,
            force_row_security: false,
            parents: Vec::new(),
            children: children.iter().map(|child| (*child).to_owned()).collect(),
            columns: BTreeMap::new(),
        }
    }

    #[test]
    fn derivation_requires_the_release_table_to_become_the_first_partition() {
        let mut before = SchemaFingerprint::default();
        before.tables.insert("qbit_share_ledger".into(), table(&[]));
        let mut after = SchemaFingerprint::default();
        after.tables.insert(
            "qbit_share_ledger".into(),
            table(&["qbit_share_ledger_p0", "qbit_share_ledger_p1"]),
        );
        assert_eq!(
            derive(17, &before, &after).unwrap(),
            PartitionMigration { version: 17 }
        );
        let error = derive(17, &before, &before).unwrap_err().to_string();
        assert!(error.contains("is not a partition"), "{error}");
        let error = derive(17, &after, &after).unwrap_err().to_string();
        assert!(error.contains("already has partitions"), "{error}");
        let error = derive(17, &SchemaFingerprint::default(), &after)
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing before"), "{error}");
    }
}

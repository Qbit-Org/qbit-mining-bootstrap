//! Migration 015 (#415), the terminal `orphaned` disposition, applied by the
//! runner to a database an earlier build migrated through 014, against a
//! real PostgreSQL: the legitimate upgrade on plain and dual (#258 body)
//! outboxes, the refusals of foreign lifecycle CHECKs and of tampered or
//! missing 011 rules, the shutdown proof it requires, and its bounded wait
//! for the outbox lock. Every refusal must leave the database exactly as it
//! was: no version, no capability, the same constraints and rows, and no
//! probe object.
//!
//! Each case owns a database, because the runner's migration lock is an
//! advisory lock and PostgreSQL scopes those to a database, and the lock case
//! holds the runner inside that lock for the whole lock_timeout.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test orphan_disposition_migration.
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_prism_server::ledger::{HeartbeatStatus, Ledger, REQUIRED_SCHEMA_VERSIONS};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// The frozen 2.x.x v2.0.2 release files (see tests/fixtures/schema_2x): a
/// dual-format outbox is built from these, never from the live files.
const FROZEN_2X_001: &str = include_str!("fixtures/schema_2x/001_share_ledger.sql");
const FROZEN_2X_002: &str = include_str!("fixtures/schema_2x/002_candidate_bodies.sql");
/// 011's rules are read out of its own file, so the pre-015 outbox carries
/// exactly the text 011 wrote.
const MIGRATION_011: &str = include_str!("../migrations/011_offer_before_landing.sql");

const STATE_RULE: &str = "qbit_block_candidate_outbox_lifecycle_state_check";
const PAYLOAD_RULE: &str = "qbit_block_candidate_outbox_lifecycle_payload_check";
const OFFER_RULE: &str = "qbit_block_candidate_outbox_offer_check";

/// An operator CHECK that references neither `state` nor `completed_at`: 015
/// keeps it byte for byte, validation state included, and never names it.
const KEPT_OPERATOR_CHECK: (&str, &str) = (
    "operator_updated_after_created",
    "CHECK (updated_at >= created_at) NOT VALID",
);

/// The outbox a pre-015 database has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Storage {
    /// A fresh native database: no body column.
    Plain,
    /// A #258 2.x.x source the runner migrated: `storage_version` and
    /// `body_id`, and 011's dual-format payload rule.
    Dual,
}

const STORAGES: [Storage; 2] = [Storage::Plain, Storage::Dual];

struct Database {
    fixture: FixtureDatabase,
    /// No statement or lock timeouts, so a test can hold a lock as long as it
    /// needs to.
    pool: PgPool,
    url: String,
    ledgers: Mutex<Vec<PgPool>>,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let fixture = FixtureDatabase::open(&raw, "prism_orphan_migration_").await?;
        let pool = match PgPoolOptions::new()
            .max_connections(4)
            .connect(&fixture.url)
            .await
        {
            Ok(pool) => pool,
            Err(error) => return Err(fixture.abandon(error.into()).await),
        };
        Ok(Some(Self {
            url: fixture.url.clone(),
            fixture,
            pool,
            ledgers: Mutex::new(Vec::new()),
        }))
    }

    async fn ledger(&self, id: &str) -> Result<Ledger> {
        let ledger = Ledger::connect(&self.url, id.to_owned(), 4, true).await?;
        self.ledgers.lock().unwrap().push(ledger.pool.clone());
        Ok(ledger)
    }

    async fn close(self, result: Result<()>) -> Result<()> {
        for pool in self.ledgers.into_inner().unwrap() {
            pool.close().await;
        }
        self.pool.close().await;
        self.fixture.close(result).await
    }

    /// A database an earlier build migrated through 014: every migration
    /// applied by the runner, then 015 undone exactly (011's three rules
    /// back under their names, no capability, no version row). The earlier
    /// build's instance is still registered as starting; the caller decides
    /// whether it shut down.
    async fn install_pre_015(&self, storage: Storage) -> Result<Ledger> {
        if storage == Storage::Dual {
            sqlx::raw_sql(FROZEN_2X_001).execute(&self.pool).await?;
            sqlx::raw_sql(FROZEN_2X_002).execute(&self.pool).await?;
        }
        let earlier = self.ledger("pre-015").await?;
        ensure!(self.versions().await? == REQUIRED_SCHEMA_VERSIONS);
        ensure!(
            self.has_body_column().await? == (storage == Storage::Dual),
            "{storage:?}: the outbox body column does not match the source"
        );
        let payload = match storage {
            Storage::Plain => rule_011("lifecycle_payload_plain")?,
            Storage::Dual => rule_011("lifecycle_payload_dual")?,
        };
        sqlx::raw_sql(&format!(
            "ALTER TABLE qbit_block_candidate_outbox \
                 DROP CONSTRAINT {STATE_RULE}, DROP CONSTRAINT {PAYLOAD_RULE}, DROP CONSTRAINT {OFFER_RULE}, \
                 ADD CONSTRAINT {STATE_RULE} CHECK ({}), \
                 ADD CONSTRAINT {PAYLOAD_RULE} CHECK ({payload}), \
                 ADD CONSTRAINT {OFFER_RULE} CHECK ({}); \
             DELETE FROM qbit_prism_schema_capabilities WHERE capability='candidate_orphan_disposition'; \
             DELETE FROM qbit_prism_schema_migrations WHERE version=15",
            rule_011("lifecycle_state")?,
            rule_011("offer_rule")?,
        ))
        .execute(&self.pool)
        .await?;
        ensure!(!self.versions().await?.contains(&15));
        Ok(earlier)
    }

    /// Offer-lifecycle rows an earlier build left, one per offer shape, and
    /// a terminal row that kept its offer record.
    async fn seed_offer_rows(&self) -> Result<()> {
        // (tag, state, completed, offered_at, outcome, reply, last_error)
        let rows = [
            (
                "a1",
                "reconciliation",
                false,
                true,
                Some("accepted"),
                None,
                Some("landing failed after acceptance"),
            ),
            (
                "a2",
                "reconciliation",
                false,
                false,
                Some("unknown"),
                None,
                Some("delivery unknown"),
            ),
            (
                "a3",
                "offered",
                false,
                true,
                Some("rejected"),
                Some("duplicate"),
                None,
            ),
            ("a4", "offer_reserved", false, false, None, None, None),
            ("a5", "submitted", true, true, Some("accepted"), None, None),
        ];
        for (tag, state, completed, offered_at, outcome, reply, last_error) in rows {
            let unfinished = !completed;
            sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,state,completed_at,offer_reserved_at,offer_reserved_by,offered_at_ms,offer_outcome,offer_reply,last_error,proof_observed_at_ms) VALUES($1,CASE WHEN $2 THEN '{}'::jsonb END,$3,CASE WHEN $2 THEN '\\x00'::bytea END,CASE WHEN $2 THEN 1::bigint END,CASE WHEN $2 THEN repeat('00',32) END,$4,CASE WHEN $5 THEN clock_timestamp() END,clock_timestamp(),'pre-015',CASE WHEN $6 THEN 1700000000456::bigint END,$7,$8,$9,1700000000123)")
                .bind(tag.repeat(32))
                .bind(unfinished)
                .bind("11".repeat(32))
                .bind(state)
                .bind(completed)
                .bind(offered_at)
                .bind(outcome)
                .bind(reply)
                .bind(last_error)
                .execute(&self.pool)
                .await
                .with_context(|| format!("seeding the {state} row {tag}"))?;
        }
        Ok(())
    }

    async fn versions(&self) -> Result<Vec<i32>> {
        Ok(
            sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn has_body_column(&self) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='qbit_block_candidate_outbox' AND column_name='body_id')")
            .fetch_one(&self.pool).await?)
    }

    async fn orphan_capability(&self) -> Result<Option<i32>> {
        Ok(sqlx::query_scalar(
            "SELECT capability_value FROM qbit_prism_schema_capabilities WHERE capability='candidate_orphan_disposition'",
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Every CHECK on the outbox with its declaration, validation state
    /// included, by name.
    async fn checks(&self) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as(
            "SELECT conname::text,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='qbit_block_candidate_outbox'::regclass AND contype='c' ORDER BY 1",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn rows(&self) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o ORDER BY block_hash",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// Everything a refusal must leave as it was.
    async fn state(&self) -> Result<Snapshot> {
        let probes: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM pg_class WHERE relname LIKE '%015_probe%') + (SELECT count(*) FROM pg_constraint WHERE conname LIKE '%probe%')",
        )
        .fetch_one(&self.pool)
        .await?;
        ensure!(probes == 0, "a 015 probe object was left behind");
        Ok(Snapshot {
            versions: self.versions().await?,
            capability: self.orphan_capability().await?,
            checks: self.checks().await?,
            rows: self.rows().await?,
        })
    }
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    versions: Vec<i32>,
    capability: Option<i32>,
    checks: Vec<(String, String)>,
    rows: Vec<Value>,
}

async fn run(
    body: impl for<'a> FnOnce(&'a Database) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = body(&db).await;
    db.close(result).await
}

/// One of the constants 011 declares, verbatim: the text between
/// `<name> constant text := $def$` and the closing `$def$;`.
fn rule_011(name: &str) -> Result<&'static str> {
    let opening = format!("    {name} constant text := $def$");
    let start = MIGRATION_011
        .find(&opening)
        .with_context(|| format!("011 declares no {name}"))?
        + opening.len();
    let length = MIGRATION_011[start..]
        .find("$def$;")
        .with_context(|| format!("011's {name} is not closed"))?;
    Ok(&MIGRATION_011[start..start + length])
}

/// The error's whole chain, which is where the database's message is.
fn chain(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

/// 015 upgrades a stopped deployment on either outbox: 011's three rules are
/// replaced under their names by 015's, which admit `orphaned`; an unrelated
/// operator CHECK survives byte for byte; every existing row, offer record
/// included, is unchanged; the capability is declared and no probe object is
/// left. On the migrated outbox an orphaned row is terminal with its payload
/// cleared (the body column included) and keeps its reservation, outcome,
/// call time, reply and reason, while an orphan that kept its payload, lost
/// its reason, or claims a node answer without a call time is refused.
#[tokio::test]
async fn migration_015_upgrades_a_stopped_deployment_on_plain_and_dual_outboxes() -> Result<()> {
    for storage in STORAGES {
        run(move |db| {
            Box::pin(async move {
                let earlier = db.install_pre_015(storage).await?;
                db.seed_offer_rows().await?;
                sqlx::raw_sql(&format!(
                    "ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT {} {}",
                    KEPT_OPERATOR_CHECK.0, KEPT_OPERATOR_CHECK.1
                ))
                .execute(&db.pool)
                .await?;
                earlier.heartbeat(HeartbeatStatus::Stopped).await?;
                let before = db.state().await?;
                ensure!(before.capability.is_none());

                let _upgraded = db.ledger("post-015").await?;
                let after = db.state().await?;
                ensure!(after.versions == REQUIRED_SCHEMA_VERSIONS, "{storage:?}: {:?}", after.versions);
                ensure!(after.capability == Some(1), "{storage:?}: the capability was not declared");
                ensure!(after.rows == before.rows, "{storage:?}: 015 rewrote a row");
                ensure!(
                    after.checks.iter().map(|(name, _)| name).eq(before.checks.iter().map(|(name, _)| name)),
                    "{storage:?}: 015 changed the set of CHECKs: {:?}",
                    after.checks
                );
                for (name, definition) in &after.checks {
                    let previous = &before.checks.iter().find(|(known, _)| known == name).context("check")?.1;
                    if [STATE_RULE, PAYLOAD_RULE, OFFER_RULE].contains(&name.as_str()) {
                        ensure!(definition.contains("orphaned") && !previous.contains("orphaned"), "{storage:?}: {name} was not replaced: {definition}");
                    } else {
                        ensure!(definition == previous, "{storage:?}: 015 changed {name}: {previous} became {definition}");
                    }
                }
                let payload = &after.checks.iter().find(|(name, _)| name == PAYLOAD_RULE).context("payload rule")?.1;
                ensure!(payload.contains("body_id") == (storage == Storage::Dual), "{storage:?}: {payload}");
                let kept = after.checks.iter().find(|(name, _)| name == KEPT_OPERATOR_CHECK.0).context("operator check")?;
                ensure!(kept.1.ends_with("NOT VALID"), "{storage:?}: 015 validated {kept:?}");

                // The disposition's row shapes on this outbox, from the
                // reconciliation rows the earlier build left.
                let orphan = |hash: &'static str, clear: bool, extra: &'static str| {
                    let payload = if clear {
                        "candidate=NULL,block_bytes=NULL,window_anchor_ms=NULL,window_prior_balances_sha256=NULL,window_first_share_seq=NULL,window_last_share_seq=NULL,window_share_count=NULL,window_snapshot_sha256=NULL,"
                    } else {
                        ""
                    };
                    let statement = format!("UPDATE qbit_block_candidate_outbox SET {payload}state='orphaned',completed_at=clock_timestamp(),last_error='proven orphan: another block is active at this height'{extra} WHERE block_hash=$1");
                    let pool = db.pool.clone();
                    async move {
                        let mut tx = pool.begin().await?;
                        let result = sqlx::query(&statement).bind(hash.repeat(32)).execute(&mut *tx).await;
                        tx.rollback().await?;
                        Ok::<_, anyhow::Error>(result.is_ok())
                    }
                };
                ensure!(orphan("a1", true, "").await?, "{storage:?}: a cleared orphan with its accepted offer was refused");
                ensure!(orphan("a2", true, "").await?, "{storage:?}: a cleared orphan of a lost reservation was refused");
                ensure!(!orphan("a1", false, "").await?, "{storage:?}: an orphan that kept its payload was accepted");
                ensure!(!orphan("a1", true, ",last_error=' '").await?, "{storage:?}: an orphan without a reason was accepted");
                ensure!(!orphan("a2", true, ",offer_outcome='accepted'").await?, "{storage:?}: an orphan claiming an answer without a call time was accepted");
                if storage == Storage::Dual {
                    // A #258 body row for the orphan to point at: the body
                    // is payload too, and a terminal orphan carries none.
                    sqlx::raw_sql("INSERT INTO qbit_block_candidate_body(body_id,storage_version,block_hash,candidate_sha256,byte_count,chunk_count,chunk_bytes,share_count,shares_offset,shares_end,staging_writer_id,staging_writer_epoch,staging_session_token) VALUES(repeat('b',32),2,repeat('a1',32),repeat('11',32),0,0,1,0,0,0,'pre-015',0,'pre-015')")
                        .execute(&db.pool)
                        .await?;
                    ensure!(!orphan("a1", true, ",body_id=repeat('b',32)").await?, "{storage:?}: an orphan that kept a body was accepted");
                }
                // Settle one for real and read it back: the offer record is kept.
                let before_row = db.rows().await?.into_iter().find(|row| row["block_hash"] == "a1".repeat(32)).context("a1")?;
                sqlx::query("UPDATE qbit_block_candidate_outbox SET candidate=NULL,block_bytes=NULL,window_anchor_ms=NULL,window_prior_balances_sha256=NULL,state='orphaned',completed_at=clock_timestamp(),last_error='proven orphan' WHERE block_hash=$1")
                    .bind("a1".repeat(32)).execute(&db.pool).await?;
                let row = db.rows().await?.into_iter().find(|row| row["block_hash"] == "a1".repeat(32)).context("a1")?;
                for column in ["offer_reserved_at", "offer_reserved_by", "offered_at_ms", "offer_outcome", "offer_reply", "proof_observed_at_ms"] {
                    ensure!(row[column] == before_row[column], "{storage:?}: {column} changed: {row}");
                }
                Ok(())
            })
        })
        .await
        .with_context(|| format!("storage {storage:?}"))?;
    }
    Ok(())
}

/// 015 refuses, by name and all at once, a CHECK it does not know that
/// references `state` or `completed_at` (it cannot tell whether one admits a
/// terminal orphan), one of 011's rules carrying another definition, and a
/// missing 011 rule. Each refusal changes nothing, never names the unrelated
/// operator CHECK, and leaves no probe object; after the operator's remedy
/// the same binary applies 015.
#[tokio::test]
async fn migration_015_refuses_foreign_lifecycle_checks_and_tampered_or_missing_rules_changing_nothing(
) -> Result<()> {
    for storage in STORAGES {
        for case in ["foreign", "tampered", "missing"] {
            run(move |db| {
                Box::pin(async move {
                    let earlier = db.install_pre_015(storage).await?;
                    db.seed_offer_rows().await?;
                    earlier.heartbeat(HeartbeatStatus::Stopped).await?;
                    let mut setup = vec![format!(
                        "ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT {} {}",
                        KEPT_OPERATOR_CHECK.0, KEPT_OPERATOR_CHECK.1
                    )];
                    let (named, detail, remedy): (Vec<&str>, &str, Vec<String>) = match case {
                        "foreign" => {
                            setup.push("ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT operator_known_states CHECK (state IN ('pending','offer_reserved','offered','reconciliation','submitted','abandoned'))".into());
                            setup.push("ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT operator_completed_without_document CHECK (completed_at IS NULL OR candidate IS NULL)".into());
                            (
                                vec!["operator_known_states", "operator_completed_without_document"],
                                "reference its lifecycle columns state or completed_at",
                                vec![
                                    "ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT operator_known_states".into(),
                                    "ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT operator_completed_without_document".into(),
                                ],
                            )
                        }
                        "tampered" => {
                            setup.push(format!(
                                "ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT {OFFER_RULE}, ADD CONSTRAINT {OFFER_RULE} CHECK (({}) AND attempt_count >= 0)",
                                rule_011("offer_rule")?
                            ));
                            (
                                vec![OFFER_RULE],
                                "carries a definition 011 did not write",
                                vec![format!(
                                    "ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT {OFFER_RULE}, ADD CONSTRAINT {OFFER_RULE} CHECK ({})",
                                    rule_011("offer_rule")?
                                )],
                            )
                        }
                        _ => {
                            setup.push(format!("ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT {STATE_RULE}"));
                            (
                                vec![STATE_RULE],
                                "is missing the lifecycle CHECK constraint(s) 011 created",
                                vec![format!(
                                    "ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT {STATE_RULE} CHECK ({})",
                                    rule_011("lifecycle_state")?
                                )],
                            )
                        }
                    };
                    for statement in &setup {
                        sqlx::raw_sql(statement).execute(&db.pool).await?;
                    }
                    let before = db.state().await?;

                    let error = db.ledger("refused-015").await.err().context("015 was applied")?;
                    let text = chain(&error);
                    ensure!(text.contains("migration 015"), "{text}");
                    for name in &named {
                        ensure!(text.contains(name), "the refusal did not name {name}: {text}");
                    }
                    ensure!(text.contains(detail), "{text}");
                    ensure!(text.contains("nothing was changed"), "{text}");
                    ensure!(!text.contains(KEPT_OPERATOR_CHECK.0), "the refusal blamed the unrelated CHECK: {text}");
                    ensure!(db.state().await? == before, "the refusal changed the database");
                    ensure!(before.capability.is_none() && !before.versions.contains(&15));

                    for statement in &remedy {
                        sqlx::raw_sql(statement).execute(&db.pool).await?;
                    }
                    let _applied = db.ledger("applied-015").await?;
                    let after = db.state().await?;
                    ensure!(after.versions == REQUIRED_SCHEMA_VERSIONS && after.capability == Some(1));
                    ensure!(after.rows == before.rows, "015 rewrote a row");
                    let kept = before.checks.iter().find(|check| check.0 == KEPT_OPERATOR_CHECK.0).context("operator check")?;
                    ensure!(after.checks.contains(kept), "015 changed {kept:?}: {:?}", after.checks);
                    Ok(())
                })
            })
            .await
            .with_context(|| format!("storage {storage:?}, case {case}"))?;
        }
    }
    Ok(())
}

/// The capability 015 declares is read at connect only and evicts no
/// running frontend, so 015 requires the same shutdown proof as 011 and 012:
/// an earlier instance that has not reported `drained` or `stopped` refuses
/// the migration before any of its DDL, whatever its heartbeat age, and the
/// refusal changes nothing. Once every instance reports shutdown, it applies.
#[tokio::test]
async fn migration_015_refuses_running_earlier_instances_before_touching_the_outbox() -> Result<()>
{
    for storage in STORAGES {
        run(move |db| {
            Box::pin(async move {
                let earlier = db.install_pre_015(storage).await?;
                db.seed_offer_rows().await?;
                sqlx::query(r#"INSERT INTO qbit_prism_instances(instance_id,heartbeat_at,status) VALUES('idle-pre-015',clock_timestamp()-interval '1 day','{"schema":"qbit.prism.audit-health.v1","ready":true}'::jsonb)"#)
                    .execute(&db.pool).await?;
                let before = db.state().await?;

                let error = db.ledger("refused-015").await.err().context("015 ran beside earlier instances")?;
                let text = chain(&error);
                ensure!(text.contains("migration 015 requires every earlier instance to report drained or stopped"), "{text}");
                ensure!(text.contains("offending instances: idle-pre-015; pre-015."), "{text}");
                ensure!(db.state().await? == before, "the refusal changed the database");

                // One instance that has not reported shutdown is enough to refuse.
                earlier.heartbeat(HeartbeatStatus::Stopped).await?;
                let error = db.ledger("refused-015-again").await.err().context("015 ran beside an idle instance")?;
                let text = chain(&error);
                ensure!(text.contains("offending instances: idle-pre-015."), "{text}");
                ensure!(db.state().await? == before, "the second refusal changed the database");

                sqlx::query(r#"UPDATE qbit_prism_instances SET status='{"state":"drained"}'::jsonb WHERE instance_id='idle-pre-015'"#)
                    .execute(&db.pool).await?;
                let _applied = db.ledger("applied-015").await?;
                let after = db.state().await?;
                ensure!(after.versions == REQUIRED_SCHEMA_VERSIONS && after.capability == Some(1));
                ensure!(after.rows == before.rows);
                Ok(())
            })
        })
        .await
        .with_context(|| format!("storage {storage:?}"))?;
    }
    Ok(())
}

/// A session still holding the outbox (here an uncommitted writer's lock)
/// fails 015 within the runner's lock_timeout instead of stalling the
/// migration, which holds the migration lock meanwhile; the failure changes
/// nothing, and once the session ends the same binary applies 015.
#[tokio::test]
async fn migration_015_bounds_its_wait_for_the_outbox_lock() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let earlier = db.install_pre_015(Storage::Plain).await?;
            db.seed_offer_rows().await?;
            earlier.heartbeat(HeartbeatStatus::Stopped).await?;
            let before = db.state().await?;

            let mut writer = db.pool.begin().await?;
            sqlx::query("LOCK TABLE qbit_block_candidate_outbox IN ROW EXCLUSIVE MODE")
                .execute(&mut *writer)
                .await?;
            let started = Instant::now();
            // The runner's default lock_timeout is 5 s; the outer bound only
            // turns a hang into a failure.
            let outcome = tokio::time::timeout(Duration::from_secs(60), db.ledger("blocked-015"))
                .await
                .context("015 did not give up waiting for the outbox lock")?;
            let elapsed = started.elapsed();
            writer.rollback().await?;
            let error = outcome
                .err()
                .context("015 was applied while a session held the outbox")?;
            let text = chain(&error);
            ensure!(text.contains("lock timeout"), "{text}");
            ensure!(
                elapsed < Duration::from_secs(30),
                "the lock wait took {elapsed:?}"
            );
            ensure!(
                db.state().await? == before,
                "the lock timeout changed the database"
            );

            let _applied = db.ledger("applied-015").await?;
            let after = db.state().await?;
            ensure!(after.versions == REQUIRED_SCHEMA_VERSIONS && after.capability == Some(1));
            Ok(())
        })
    })
    .await
}

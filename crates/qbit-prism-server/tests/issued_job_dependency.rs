//! Explicit PostgreSQL acceptance for immutable issued/prepared job lifetimes.
//! PRISM_TEST_DATABASE_URL=<disposable> cargo test -p qbit-prism-server
//! --test issued_job_dependency -- --ignored --test-threads=2
use anyhow::{ensure, Context, Result};
use qbit_prism_server::ledger::{IssuedJobSave, Ledger, PreparedDependency};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const PREPARED: &str = "prepared:original";
const ISSUED: &str = "issued:original";

struct Database {
    admin: PgPool,
    ledger: Ledger,
    schema: String,
    parent: String,
    revision: i64,
    prepared: Value,
}
impl Database {
    async fn open() -> Result<Self> {
        let raw = gate::required_database_url(gate::site!())?;
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_dependency_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger = Ledger::connect(url.as_str(), "dependency-test".into(), 4, true).await?;
        let parent = "11".repeat(32);
        let revision = ledger.observe_chain_view(&parent, 100, "01").await?;
        let prepared = json!({"snapshot":{"payout_revision":revision,"anchor_ms":1234},
            "template":{"previousblockhash":parent},"coinbase_suffix":"original-entropy",
            "bundle":{"issued_economics":[1,2,3]}});
        ledger
            .save_job(PREPARED, &prepared, revision, &parent, 60)
            .await?;
        Ok(Self {
            admin,
            ledger,
            schema,
            parent,
            revision,
            prepared,
        })
    }
    fn dependency(&self) -> PreparedDependency<'_> {
        PreparedDependency {
            key: PREPARED,
            original_revision: self.revision,
            parent: &self.parent,
        }
    }
    async fn deadline(&self) -> Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint + 30000",
        )
        .fetch_one(&self.ledger.pool)
        .await?)
    }
    async fn row(&self, id: &str) -> Result<Option<Value>> {
        Ok(
            sqlx::query_scalar("SELECT to_jsonb(j) FROM qbit_prism_jobs j WHERE job_id=$1")
                .bind(id)
                .fetch_optional(&self.ledger.pool)
                .await?,
        )
    }
    async fn age_prepared(&self) -> Result<()> {
        sqlx::query("UPDATE qbit_prism_jobs SET expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1")
            .bind(PREPARED).execute(&self.ledger.pool).await?;
        Ok(())
    }
    async fn save(
        &self,
        payload: &Value,
        revision: i64,
        expiry: i64,
        repair: Option<&Value>,
    ) -> Result<IssuedJobSave> {
        self.ledger
            .save_issued_job(
                ISSUED,
                payload,
                revision,
                &self.parent,
                expiry,
                self.dependency(),
                repair,
            )
            .await
    }
    async fn close(self, result: Result<()>) -> Result<()> {
        self.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        result
    }
}
fn issued(expiry: i64) -> Value {
    json!({"prepared_key":PREPARED,"expires_at_ms":expiry,
        "worker":{"username":"original.worker"},"extranonce1":"01020304"})
}

#[tokio::test]
#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run this integration target explicitly"]
async fn issued_save_extends_only_dependency_lifetime_through_the_child_deadline() -> Result<()> {
    let db = Database::open().await?;
    let result = async {
        db.age_prepared().await?;
        let before = db.row(PREPARED).await?.context("prepared row missing")?;
        let expiry = db.deadline().await?;
        let child = issued(expiry);
        ensure!(db.save(&child, db.revision, expiry, None).await? == IssuedJobSave::Saved);
        ensure!(db.ledger.job(PREPARED).await? == Some(db.prepared.clone()));
        ensure!(db.ledger.job(ISSUED).await? == Some(child));
        let mut after = db.row(PREPARED).await?.context("dependency disappeared")?;
        after["expires_at"] = before["expires_at"].clone();
        ensure!(after == before, "renewal changed original payload or metadata");
        let covers: bool = sqlx::query_scalar("SELECT p.expires_at>=j.expires_at AND j.expires_at=to_timestamp($3::double precision/1000) FROM qbit_prism_jobs p,qbit_prism_jobs j WHERE p.job_id=$1 AND j.job_id=$2")
            .bind(PREPARED).bind(ISSUED).bind(expiry).fetch_one(&db.ledger.pool).await?;
        ensure!(covers, "dependency must outlive the immutable issued deadline");
        ensure!(db.ledger.prune_expired_jobs().await? == 0);
        Ok(())
    }.await;
    db.close(result).await
}

#[tokio::test]
#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run this integration target explicitly"]
async fn pruned_dependency_returns_missing_without_committing_a_dangling_child() -> Result<()> {
    let db = Database::open().await?;
    let result = async {
        db.age_prepared().await?;
        ensure!(db.ledger.prune_expired_jobs().await? == 1);
        let expiry = db.deadline().await?;
        ensure!(
            db.save(&issued(expiry), db.revision, expiry, None).await?
                == IssuedJobSave::PreparedMissing
        );
        ensure!(db.row(PREPARED).await?.is_none());
        ensure!(
            db.row(ISSUED).await?.is_none(),
            "missing dependency committed a child"
        );
        Ok(())
    }
    .await;
    db.close(result).await
}

#[tokio::test]
#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run this integration target explicitly"]
async fn repair_preserves_original_revision_and_payload_under_a_new_current_revision() -> Result<()>
{
    let db = Database::open().await?;
    let result = async {
        db.age_prepared().await?;
        ensure!(db.ledger.prune_expired_jobs().await? == 1);
        let current = db
            .ledger
            .observe_chain_view(&"22".repeat(32), 101, "02")
            .await?;
        ensure!(current > db.revision);
        let expiry = db.deadline().await?;
        ensure!(
            db.save(&issued(expiry), current, expiry, Some(&db.prepared))
                .await?
                == IssuedJobSave::Saved
        );
        let original = db.row(PREPARED).await?.context("repair missing")?;
        ensure!(original["payload"] == db.prepared);
        ensure!(original["payout_revision"] == db.revision && original["parent_hash"] == db.parent);
        let child = db.row(ISSUED).await?.context("issued row missing")?;
        ensure!(child["payout_revision"] == current && child["parent_hash"] == db.parent);
        ensure!(db.ledger.job(PREPARED).await? == Some(db.prepared.clone()));
        ensure!(db.ledger.prune_expired_jobs().await? == 0);
        Ok(())
    }
    .await;
    db.close(result).await
}

#[tokio::test]
#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run this integration target explicitly"]
async fn wrong_current_revision_rejects_before_renewal_or_repair() -> Result<()> {
    let db = Database::open().await?;
    let result = async {
        db.age_prepared().await?;
        let before = db.row(PREPARED).await?;
        db.ledger
            .observe_chain_view(&"22".repeat(32), 101, "02")
            .await?;
        let expiry = db.deadline().await?;
        ensure!(db
            .save(&issued(expiry), db.revision, expiry, None)
            .await
            .is_err());
        ensure!(db.row(PREPARED).await? == before && db.row(ISSUED).await?.is_none());
        ensure!(db.ledger.prune_expired_jobs().await? == 1);
        ensure!(db
            .save(&issued(expiry), db.revision, expiry, Some(&db.prepared))
            .await
            .is_err());
        ensure!(db.row(PREPARED).await?.is_none() && db.row(ISSUED).await?.is_none());
        Ok(())
    }
    .await;
    db.close(result).await
}

#[tokio::test]
#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run this integration target explicitly"]
async fn exact_retry_is_immutable_and_conflicting_child_rolls_back_parent_extension() -> Result<()>
{
    let db = Database::open().await?;
    let result = async {
        let expiry = db.deadline().await?;
        let child = issued(expiry);
        ensure!(db.save(&child, db.revision, expiry, None).await? == IssuedJobSave::Saved);
        let before_parent = db.row(PREPARED).await?;
        let before_child = db.row(ISSUED).await?;
        ensure!(db.save(&child, db.revision, expiry, None).await? == IssuedJobSave::Saved);
        ensure!(db.row(PREPARED).await? == before_parent && db.row(ISSUED).await? == before_child);
        let mut conflicting = child;
        conflicting["extranonce1"] = json!("ffffffff");
        ensure!(db
            .save(&conflicting, db.revision, expiry, None)
            .await
            .is_err());
        // A later caller deadline would first need to renew the dependency.
        // The subsequent immutable-child collision must roll that renewal back.
        ensure!(db
            .save(
                &issued(expiry + 120_000),
                db.revision,
                expiry + 120_000,
                None
            )
            .await
            .is_err());
        ensure!(db.row(PREPARED).await? == before_parent && db.row(ISSUED).await? == before_child);
        Ok(())
    }
    .await;
    db.close(result).await
}

#[tokio::test]
#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run this integration target explicitly"]
async fn conflicting_repair_and_invalid_dependency_boundaries_leave_no_child() -> Result<()> {
    let db = Database::open().await?;
    let result = async {
        let expiry = db.deadline().await?;
        let before = db.row(PREPARED).await?;
        let mut other = db.prepared.clone();
        other["coinbase_suffix"] = json!("different-original-entropy");
        ensure!(db
            .save(&issued(expiry), db.revision, expiry, Some(&other))
            .await
            .is_err());
        for payload in [
            json!({}),
            json!({"prepared_key":"wrong","expires_at_ms":expiry}),
            json!({"prepared_key":PREPARED,"expires_at_ms":expiry + 1}),
        ] {
            ensure!(db.save(&payload, db.revision, expiry, None).await.is_err());
        }
        for deadline in [0, -1, i64::MAX] {
            ensure!(db
                .save(&issued(deadline), db.revision, deadline, None)
                .await
                .is_err());
        }
        for dependency in [
            PreparedDependency {
                key: "",
                ..db.dependency()
            },
            PreparedDependency {
                key: ISSUED,
                ..db.dependency()
            },
            PreparedDependency {
                original_revision: db.revision + 1,
                ..db.dependency()
            },
            PreparedDependency {
                parent: "wrong-parent",
                ..db.dependency()
            },
        ] {
            let payload = json!({"prepared_key":dependency.key,"expires_at_ms":expiry});
            ensure!(db
                .ledger
                .save_issued_job(
                    ISSUED,
                    &payload,
                    db.revision,
                    &db.parent,
                    expiry,
                    dependency,
                    None
                )
                .await
                .is_err());
        }
        other = db.prepared.clone();
        other["snapshot"]["payout_revision"] = json!(db.revision + 1);
        ensure!(db
            .save(&issued(expiry), db.revision, expiry, Some(&other))
            .await
            .is_err());
        ensure!(db.row(PREPARED).await? == before && db.row(ISSUED).await?.is_none());
        Ok(())
    }
    .await;
    db.close(result).await
}

struct Running<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Running<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn blocked_query(admin: &PgPool, blocker: i32, prefix: &str) -> Result<i32> {
    timeout(Duration::from_secs(3), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE $2 ORDER BY pid LIMIT 1")
                .bind(blocker).bind(format!("{prefix}%")).fetch_optional(admin).await?;
            if let Some(pid) = pid { return Ok::<_, anyhow::Error>(pid); }
            sleep(Duration::from_millis(10)).await;
        }
    }).await.context("expected PostgreSQL lock wait did not occur")?
}

#[tokio::test]
#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run this integration target explicitly"]
async fn gc_rechecks_expiry_after_waiting_for_atomic_dependency_renewal() -> Result<()> {
    let db = Database::open().await?;
    let result = async {
        db.age_prepared().await?;
        let expiry = db.deadline().await?;
        // Pause the actual public save at its child INSERT, after dependency
        // renewal but before commit. The trigger is local to this test schema.
        const GATE: i64 = 0x5052495300de01;
        sqlx::raw_sql(&format!("CREATE FUNCTION pause_issued_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({GATE}); RETURN NEW; END $$; CREATE TRIGGER pause_issued BEFORE INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id = '{ISSUED}') EXECUTE FUNCTION pause_issued_insert()"))
            .execute(&db.ledger.pool).await?;
        let mut hold = db.admin.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(GATE).execute(&mut *hold).await?;
        let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *hold).await?;
        let ledger = db.ledger.clone();
        let parent = db.parent.clone();
        let revision = db.revision;
        let mut saving = Running(tokio::spawn(async move {
            ledger.save_issued_job(ISSUED, &issued(expiry), revision, &parent, expiry,
                PreparedDependency { key: PREPARED, original_revision: revision, parent: &parent }, None).await
        }));
        let saver = blocked_query(&db.admin, blocker, "INSERT INTO qbit_prism_jobs").await?;
        ensure!(db.ledger.job(PREPARED).await?.is_none() && db.row(ISSUED).await?.is_none(), "uncommitted renewal/child leaked");
        let ledger = db.ledger.clone();
        let mut collecting = Running(tokio::spawn(async move { ledger.prune_expired_jobs().await }));
        let collector = blocked_query(&db.admin, saver, "DELETE FROM qbit_prism_jobs").await?;
        eprintln!("observed lock chain: save {saver} waits on gate {blocker}; GC {collector} waits on save");
        hold.commit().await?;
        let saved = timeout(Duration::from_secs(5), &mut saving.0).await???;
        let removed = timeout(Duration::from_secs(5), &mut collecting.0).await???;
        eprintln!("after release: save={saved:?}, GC removed={removed}");
        ensure!(saved == IssuedJobSave::Saved);
        ensure!(removed == 0, "GC deleted a dependency renewed while its DELETE waited");
        ensure!(db.ledger.job(PREPARED).await? == Some(db.prepared.clone()));
        ensure!(db.ledger.job(ISSUED).await? == Some(issued(expiry)));
        Ok(())
    }.await;
    db.close(result).await
}

//! The record of 2 while frontends serve, against a real PostgreSQL. An
//! attempt whose COMMIT committed, but whose reply was then lost or cut
//! short, has finished the backfill: the run reports it done, and never
//! retries into the cursor that commit dropped (Codex on #746). Faults are
//! keyed by schema, so the other tests in this binary never meet them.
use super::faults;
use super::*;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
    ledger: crate::ledger::Ledger,
}

impl Database {
    /// A native ledger in a schema of its own, every migration applied.
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_record_two_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger =
            crate::ledger::Ledger::connect(url.as_str(), "record-two".into(), 2, true).await?;
        Ok(Some(Self {
            admin,
            schema,
            url: url.into(),
            ledger,
        }))
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

/// The migrated ledger as a backfill that permits serving leaves it once
/// every batch has run: 2 unrecorded, the cursor at the end of the legacy
/// ledger, here empty, and the fence at 2, beside 017's conversion bound.
async fn pending_at_its_end(connection: &mut PgConnection) -> Result<()> {
    let mut tx = connection.begin().await?;
    sqlx::query("DELETE FROM qbit_prism_schema_migrations WHERE version=$1")
        .bind(VERSION)
        .execute(&mut *tx)
        .await?;
    create_cursor(&mut tx).await?;
    sqlx::query(
        "INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES($1,$2)",
    )
    .bind(PENDING_CAPABILITY)
    .bind(FENCE_SERVING)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE qbit_prism_share_partitioning SET conversion_bound=COALESCE(conversion_bound,1) WHERE singleton",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// `backfill-share-hashes` whose first record attempt commits and then
/// fails with a statement timeout, as an attempt whose COMMIT reply was cut
/// short would. The run reads what the attempt left, finds 2 recorded and
/// the cursor gone, and reports the backfill done. Without that read it
/// would try again, and fail on the dropped cursor.
#[tokio::test]
async fn a_record_attempt_that_committed_before_it_failed_finishes_the_backfill() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        let mut connection = PgConnection::connect(&db.url).await?;
        pending_at_its_end(&mut connection).await?;
        faults::lose_record_commit_reply(&db.schema);
        let throttle = Throttle::default().with_record_attempts(3, Duration::from_millis(100))?;
        let finished = finish(&mut connection, &throttle, None).await;
        let fired = !faults::armed(&db.schema);
        connection.close().await?;
        let finished = finished?;
        ensure!(fired, "the injected failure never fired");
        ensure!(
            finished.mapped == 0 && finished.range == Some((0, 0)) && !finished.already_complete(),
            "{finished:?}"
        );
        // What the attempt committed: 2 recorded, the cursor and the fence
        // gone.
        let mut check = PgConnection::connect(&db.url).await?;
        let recorded_2 = recorded(&mut check, VERSION).await?;
        let cursor = cursor_relation(&mut check).await?;
        let declared = fence(&mut check).await?;
        check.close().await?;
        ensure!(
            recorded_2 && cursor.is_none() && declared.is_none(),
            "2 recorded: {recorded_2}, cursor: {cursor:?}, fence: {declared:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = db.close().await;
    match (result, closed) {
        (Ok(()), closed) => closed,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(closed)) => {
            Err(error.context(format!("schema cleanup also failed: {closed:#}")))
        }
    }
}

//! #668: a CTV fanout claim poll reads the fanouts due now and the ones
//! still being watched, never the settled history behind them.
//!
//! Settled fanouts accumulate with every found block. Before migration 023
//! the lane's selection matched them on its status and schedule predicates
//! and dropped them only after reading them, a sequential scan of the whole
//! table on every claim. This plans and executes the two statements every
//! claim poll runs, the survey (`Ledger::fanout_claim_survey_sql`) and the
//! lane (`Ledger::fanout_lane_sql`), on real PostgreSQL, over a synthetic
//! history of deep-confirmed fanouts beside the fanouts still being watched.
//! It runs them prepared, as sqlx runs them, through their custom plans and
//! the generic plan PostgreSQL settles on after five executions.
//!
//! Plans report buffer accesses, not physical I/O, and no latency is
//! asserted. What is asserted is the shape: no sequential scan of the fanout
//! table, and the fanout rows visited bounded by the fanouts due now (the
//! lane, through its own index) or by the watched fanouts (the survey, which
//! reads the scheduled ones), whatever the settled history holds.
//!
//! Migration 023 builds the lane's index online, as 013 builds its own:
//! the last test upgrades a ledger at 22 while a fanout write is open.
use super::*;
use qbit_prism_server::ledger::REQUIRED_SCHEMA_VERSIONS;
use serde_json::Value;
use tokio::time::{sleep, timeout};

/// Deep-confirmed fanouts no claim takes again. The plans do not depend on
/// the count once a sequential scan costs more than the indexes, well below
/// this; mainnet adds about 65,000 to 130,000 a year at #521's rate.
const SETTLED: i64 = 20_000;
/// Confirmed fanouts under 1,000 deep, still checked every few seconds,
/// none of them due while the test runs.
const WATCHED: i64 = 500;
/// The fanout rows the lane may visit: its two due fanouts and the
/// checkpoint, each read as a candidate and then by primary key, the claimed
/// one once more by its update, with room for the planner's choice of
/// lookups. Independent of `SETTLED` and `WATCHED`.
const LANE_VISITS: f64 = 16.0;

/// Every plan node, depth first, init plans and CTEs included.
fn nodes<'a>(node: &'a Value, all: &mut Vec<&'a Value>) {
    all.push(node);
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            nodes(child, all);
        }
    }
}

/// Add the settled history and the watched fanouts to the block
/// `prepare_mature_cpfp_fanouts` landed, as copies of its first fanout, then
/// refresh the statistics the planner reads, as autovacuum would. Settled
/// fanouts were last checked a day ago, so their schedules are long past;
/// the newest of them, the checkpoint reconciliation keeps watching, and the
/// watched fanouts are checked again later, so none of those is due.
async fn settle_history(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,anchor_vout,covenant_output_value_sats,fanout_output_sum_sats,settlement_status,confirmed_block_hash,confirmed_block_height,confirmed_depth,next_broadcast_attempt_at,broadcast_attempt_count,updated_at) \
         SELECT md5('settled'||g)||md5('history'||g),block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,1+g,$1+$2+2,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,anchor_vout,covenant_output_value_sats,fanout_output_sum_sats, \
                'confirmed',repeat('cd',32),2000+g, \
                CASE WHEN g<=$1 THEN 1000+g ELSE 1+g%999 END, \
                CASE WHEN g<$1 THEN clock_timestamp()-interval '1 day' ELSE clock_timestamp()+interval '1 hour' END, \
                1,clock_timestamp()-interval '1 minute' \
         FROM qbit_ctv_fanout_artifacts, generate_series(1,$1+$2) AS s(g) WHERE chunk_index=0",
    )
    .bind(SETTLED)
    .bind(WATCHED)
    .execute(pool)
    .await?;
    sqlx::raw_sql("VACUUM ANALYZE qbit_ctv_fanout_artifacts")
        .execute(pool)
        .await?;
    Ok(())
}

/// Plan and run one execution of a prepared statement of the claim poll:
/// it must never scan the fanout table, and must visit at most `bound`
/// fanout rows. Returns the plan.
async fn explain(conn: &mut PgConnection, execute: &str, bound: f64) -> Result<Value> {
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) {execute}"
    ))
    .fetch_one(&mut *conn)
    .await?;
    let mut all = Vec::new();
    nodes(&plan[0]["Plan"], &mut all);
    let scans: Vec<_> = all
        .iter()
        .filter(|node| node["Relation Name"] == "qbit_ctv_fanout_artifacts")
        .collect();
    ensure!(
        !scans.iter().any(|node| node["Node Type"] == "Seq Scan"),
        "{execute} scanned the fanout table: {plan}"
    );
    // EXPLAIN reports rows per loop: a lookup loops once per row it serves.
    // A lossy bitmap's heap rows count too, as rows its recheck removed.
    let visited: f64 = scans
        .iter()
        .map(|node| {
            (node["Actual Rows"].as_f64().unwrap_or(0.0)
                + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0)
                + node["Rows Removed by Index Recheck"]
                    .as_f64()
                    .unwrap_or(0.0))
                * node["Actual Loops"].as_f64().unwrap_or(1.0)
        })
        .sum();
    ensure!(
        visited <= bound,
        "{execute} visited {visited} fanout rows, at most {bound} allowed, of {SETTLED} settled and {WATCHED} watched: {plan}"
    );
    Ok(plan)
}

/// Whether a plan reads `index`.
fn reads(plan: &Value, index: &str) -> bool {
    let mut all = Vec::new();
    nodes(&plan[0]["Plan"], &mut all);
    all.iter().any(|node| node["Index Name"] == index)
}

#[tokio::test]
async fn a_fanout_claim_poll_never_reads_the_settled_history() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    // Two mature fanouts, never attempted and so due now.
    prepare_mature_cpfp_fanouts(&a, 2).await?;
    settle_history(&a.pool).await?;
    let mut conn = a.pool.acquire().await?;
    sqlx::raw_sql("BEGIN").execute(&mut *conn).await?;
    sqlx::raw_sql(&format!(
        "PREPARE survey AS {}; PREPARE lane(text,text,bigint) AS {}",
        Ledger::fanout_claim_survey_sql(),
        Ledger::fanout_lane_sql()
    ))
    .execute(&mut *conn)
    .await?;
    // Five custom plans, then the generic one, which the later executions
    // keep. The survey reads the scheduled fanouts, the watched ones, to
    // find any the clock stepped back over; it finds none.
    for _ in 1..=7 {
        let plan = explain(&mut conn, "EXECUTE survey", (WATCHED + 16) as f64).await?;
        ensure!(
            plan[0]["Plan"]["Actual Rows"].as_f64() == Some(1.0),
            "the survey returned other than one row: {plan}"
        );
    }
    // The lane claims the two due fanouts first, then nothing is due. It
    // reads them through its own index, and the watched fanouts not at all.
    let mut claimed = 0.0;
    for execution in 1..=7 {
        let plan = explain(
            &mut conn,
            &format!("EXECUTE lane('plan-{execution}','plan',60)"),
            LANE_VISITS,
        )
        .await?;
        ensure!(
            reads(&plan, "qbit_ctv_fanout_artifacts_lane_idx"),
            "execution {execution} of the lane did not read its index: {plan}"
        );
        claimed += plan[0]["Plan"]["Actual Rows"].as_f64().unwrap_or(0.0);
    }
    ensure!(
        claimed == 2.0,
        "the lane claimed {claimed} of two due fanouts"
    );
    sqlx::raw_sql("ROLLBACK; DEALLOCATE survey; DEALLOCATE lane")
        .execute(&mut *conn)
        .await?;
    drop(conn);
    // Through the ledger: the due fanout is claimed, never a settled one.
    let claim = a
        .claim_fanout(60)
        .await?
        .context("no due fanout was claimed")?;
    ensure!(
        claim.progress["status"] == "broadcastable",
        "the lane claimed settled history: {}",
        claim.progress
    );
    db.close(vec![a]).await
}

/// The fanout writes the test makes while 023 builds: one per chunk, each
/// a single-row update in its caller's transaction.
async fn touch_fanout<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    chunk: i32,
) -> Result<()> {
    let touched = sqlx::query(
        "UPDATE qbit_ctv_fanout_artifacts SET updated_at=clock_timestamp() WHERE chunk_index=$1",
    )
    .bind(chunk)
    .execute(executor)
    .await?
    .rows_affected();
    ensure!(touched == 1, "touched {touched} fanouts of chunk {chunk}");
    Ok(())
}

/// 023 on a ledger at 22 is applied online, after the migration
/// transaction, with `CREATE INDEX CONCURRENTLY`: its build waits for a
/// fanout write already open, and the fanout writes that arrive meanwhile,
/// a found block's landing among them, land at once. A plain `CREATE INDEX`
/// would queue behind the open write, and every later fanout write behind
/// it. Then the index is valid, 23 is recorded and the lane reads it.
#[tokio::test]
async fn migration_023_builds_the_lane_index_without_blocking_fanout_writes() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    prepare_mature_cpfp_fanouts(&a, 2).await?;
    sqlx::raw_sql(
        "DELETE FROM qbit_prism_schema_migrations WHERE version=23; DROP INDEX qbit_ctv_fanout_artifacts_lane_idx",
    )
    .execute(&a.pool)
    .await?;
    let mut writer = a.pool.begin().await?;
    touch_fanout(&mut *writer, 0).await?;
    let mut migrate = Box::pin(db.ledger("online"));
    // The relation OID scopes this to this test's own schema.
    let waiting = timeout(Duration::from_secs(60), async {
        while !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.query LIKE 'CREATE INDEX CONCURRENTLY%' AND a.wait_event_type='Lock' AND EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='relation' AND l.relation='qbit_ctv_fanout_artifacts'::regclass AND l.granted))",
        )
        .fetch_one(&a.pool)
        .await?
        {
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    });
    tokio::select! {
        result = &mut migrate => match result {
            Ok(_) => bail!("023 was applied while a fanout write was open"),
            Err(error) => {
                return Err(error.context("023 failed before its build waited for the open fanout write"))
            }
        },
        result = waiting => result.context("023's build never waited for the open fanout write")??,
    }
    timeout(Duration::from_secs(5), async {
        let mut tx = a.pool.begin().await?;
        touch_fanout(&mut *tx, 1).await?;
        tx.commit().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("a fanout write blocked behind 023's build")??;
    ensure!(
        futures_util::poll!(&mut migrate).is_pending(),
        "023 finished before the open fanout write committed"
    );
    writer.commit().await?;
    let online = timeout(Duration::from_secs(60), migrate).await??;
    let valid: bool = sqlx::query_scalar(
        "SELECT indisvalid FROM pg_index WHERE indexrelid='qbit_ctv_fanout_artifacts_lane_idx'::regclass",
    )
    .fetch_one(&a.pool)
    .await?;
    ensure!(valid, "023's index is not valid");
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&a.pool)
            .await?;
    ensure!(
        versions == REQUIRED_SCHEMA_VERSIONS,
        "recorded {versions:?}"
    );
    let claim = online
        .claim_fanout(60)
        .await?
        .context("no due fanout was claimed after 023")?;
    ensure!(
        claim.progress["status"] == "broadcastable",
        "{}",
        claim.progress
    );
    db.close(vec![a, online]).await
}

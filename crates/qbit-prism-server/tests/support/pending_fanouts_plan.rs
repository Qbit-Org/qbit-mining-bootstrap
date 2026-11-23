//! #688: the pending CTV fanouts the two pending feeds list
//! (`/public/v1/fanouts/pending`, `/audit/fanouts/pending`) are found
//! through `qbit_ctv_fanout_artifacts_status_idx`, never by reading the
//! settled fanouts behind them.
//!
//! The feeds' statement excluded the settled statuses with
//! `NOT IN ('confirmed','reorged')`, which is no index condition: every
//! response cache miss read every fanout. A whole-table scan of this shape
//! cost 63 to 67 ms at 100,000 settled fanouts and about 400 ms at 1,000,000
//! in #668's measurements of the claim lane; this statement was not timed.
//! It now names the pending statuses. These tests pin that list to every
//! status the schema accepts, prove the plan over settled fanouts spread
//! over many blocks, and compare the output, page by page, with the
//! `NOT IN` statement's on blocks holding every status in every schedule.
//!
//! As in `fanout_lane_plan`, plans report buffer accesses, not physical I/O,
//! and no latency is asserted: what is asserted is that the fanout table is
//! never scanned and the fanout rows visited are bounded by the fanouts in
//! a pending status, however many are settled. The parent block join is not
//! bounded; it predates #688 and the block table is a tenth of the size.
use super::fanout_lane_plan::{nodes, reads};
use super::*;
use serde_json::Value;
use std::collections::BTreeSet;

/// The statement both feeds run.
const PENDING_SQL: &str = include_str!("../../src/api/queries/pending_fanouts.sql");

/// The feeds' statement before #688, verbatim: the output `PENDING_SQL`
/// must keep.
const NOT_IN_SQL: &str = "WITH eligible AS (SELECT a.fanout_txid,a.chunk_index,b.block_height FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE b.chain_state='confirmed' AND a.settlement_status NOT IN ('confirmed','reorged') AND (a.next_broadcast_attempt_at IS NULL OR a.next_broadcast_attempt_at <= clock_timestamp())), page AS (SELECT * FROM eligible ORDER BY block_height,chunk_index,fanout_txid LIMIT $1 OFFSET $2) SELECT jsonb_build_object('total_count',(SELECT count(*) FROM eligible),'rows',COALESCE((SELECT jsonb_agg(qbit_fanout_status(fanout_txid) ORDER BY block_height,chunk_index,fanout_txid) FROM page),'[]'::jsonb))";

/// The settlement statuses neither feed lists: a fanout confirmed on chain,
/// or one whose parent block left the chain.
const SETTLED: [&str; 2] = ["confirmed", "reorged"];

/// The settled history of the plan test: blocks of `CHUNKS` fanouts each.
/// The plan does not depend on the count once a sequential scan costs more
/// than the index, well below this. Every 40th block left the chain and its
/// fanouts are `reorged`; the rest are deep-confirmed.
const SETTLED_BLOCKS: i64 = 2_000;
const CHUNKS: i64 = 10;
/// The recent blocks above it, every fanout in a pending status: one
/// immature, two mature, and one still a candidate, which no feed lists.
const PENDING_BLOCKS: i64 = 4;
/// The fanout rows a page may visit: each fanout in a pending status, with
/// room for the planner's choice of lookups. Independent of
/// `SETTLED_BLOCKS`.
const PENDING_VISITS: f64 = (PENDING_BLOCKS * CHUNKS + 16) as f64;

/// The quoted values in `sql`, a list of literals: every other `'` opens one.
fn quoted(sql: &str) -> Vec<String> {
    sql.split('\'')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

/// The quoted values of the SQL list `head (...)` in `sql`.
fn quoted_list(sql: &str, head: &str) -> Vec<String> {
    let (_, rest) = sql
        .split_once(head)
        .unwrap_or_else(|| panic!("no {head:?}"));
    let (list, _) = rest.split_once(')').expect("a closing parenthesis");
    quoted(list)
}

/// That the pending statuses `PENDING_SQL` lists and `SETTLED` split
/// `accepted`, the statuses `from` accepts, each status on exactly one side.
fn ensure_split(accepted: &BTreeSet<String>, from: &str) -> Result<()> {
    let pending = quoted_list(PENDING_SQL, "a.settlement_status IN");
    let listed: BTreeSet<String> = pending.iter().cloned().collect();
    ensure!(
        listed.len() == pending.len(),
        "pending_fanouts.sql lists a status twice: {pending:?}"
    );
    ensure!(
        SETTLED.iter().all(|status| !listed.contains(*status)),
        "pending_fanouts.sql lists a settled status: {pending:?}"
    );
    let split: BTreeSet<String> = listed
        .into_iter()
        .chain(SETTLED.iter().map(|status| status.to_string()))
        .collect();
    ensure!(
        &split == accepted,
        "{from} accepts the settlement statuses {accepted:?}, but pending_fanouts.sql lists \
         {pending:?} and {SETTLED:?} are settled: a status on neither side drops out of both \
         pending feeds without a word, so list it there or name it settled here"
    );
    Ok(())
}

/// The feeds list every status the schema declares but the settled ones.
/// A status a later change adds fails here, and against the migrated
/// schema in `the_pending_feeds_keep_the_not_in_output`, until it is listed
/// or named settled.
#[test]
fn the_pending_feeds_list_every_status_but_the_settled_ones() -> Result<()> {
    let schema = include_str!("../../../qbit-prism/sql/001_share_ledger.sql");
    let (_, column) = schema
        .split_once("settlement_status text NOT NULL")
        .expect("001 declares settlement_status");
    let accepted: BTreeSet<String> = quoted_list(column, "settlement_status IN")
        .into_iter()
        .collect();
    ensure!(
        SETTLED.iter().all(|status| accepted.contains(*status)),
        "the CHECK was misread: {accepted:?}"
    );
    ensure_split(&accepted, "001_share_ledger.sql")
}

/// Add blocks `first..first+count`, each with a fanout set of `chunks`
/// fanouts. `height`, `chain_state` and `maturity_state` are SQL
/// expressions of the block number `n`; `status` and `schedule`, the
/// settlement status and `next_broadcast_attempt_at`, of `n` and the chunk
/// `c`.
async fn blocks(
    pool: &PgPool,
    (first, count, chunks): (i64, i64, i64),
    height: &str,
    chain_state: &str,
    maturity_state: &str,
    status: &str,
    schedule: &str,
) -> Result<()> {
    let last = first + count - 1;
    sqlx::raw_sql(&format!(
        "INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state,matured_at) \
         SELECT md5('block'||n)||md5('hash'||n),{height},repeat('0',64),md5('coinbase'||n)||md5('txid'||n),repeat('f',64),m.chain_state,m.maturity_state, \
                CASE WHEN m.maturity_state='mature' THEN clock_timestamp()-interval '1 day' END \
         FROM generate_series({first},{last}) n, LATERAL (SELECT ({chain_state})::text AS chain_state,({maturity_state})::text AS maturity_state) m; \
         INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats) \
         SELECT md5('block'||n)||md5('hash'||n),'{{}}','{{}}',repeat('e',64),'ctv_fanout',md5('coinbase'||n)||md5('txid'||n),'00',{chunks},100*{chunks},100*{chunks} \
         FROM generate_series({first},{last}) n; \
         INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,anchor_vout,covenant_output_value_sats,fanout_output_sum_sats,settlement_status,confirmed_depth,next_broadcast_attempt_at) \
         SELECT md5('fanout'||n||'.'||c)||md5('txid'||n||'.'||c),md5('block'||n)||md5('hash'||n),repeat('e',64),'{{}}','{{}}',repeat('d',64),repeat('d',64),repeat('c',64),repeat('b',64), \
                c,{chunks},md5('coinbase'||n)||md5('txid'||n),0,'00','00',1,100,100,s.status,CASE WHEN s.status='confirmed' THEN 1000+n ELSE 0 END,s.schedule \
         FROM generate_series({first},{last}) n, generate_series(0,{chunks}-1) c, \
              LATERAL (SELECT ({status})::text AS status,({schedule})::timestamptz AS schedule) s"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// The fanout rows a plan visited, whether it scanned the fanout table, and
/// whether any scan of it ran other than once. A lossy bitmap's heap
/// rows count too, as rows its recheck removed. EXPLAIN reports rows per
/// loop, rounded, so the count is exact only for a scan that runs once: a
/// lookup repeated per block could round its rows away.
fn fanout_visits(plan: &Value) -> (f64, bool, bool) {
    let mut all = Vec::new();
    nodes(&plan[0]["Plan"], &mut all);
    let scans: Vec<_> = all
        .iter()
        .filter(|node| node["Relation Name"] == "qbit_ctv_fanout_artifacts")
        .collect();
    let visited = scans
        .iter()
        .map(|node| {
            node["Actual Rows"].as_f64().unwrap_or(0.0)
                + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0)
                + node["Rows Removed by Index Recheck"]
                    .as_f64()
                    .unwrap_or(0.0)
        })
        .sum();
    (
        visited,
        scans.iter().any(|node| node["Node Type"] == "Seq Scan"),
        scans
            .iter()
            .any(|node| node["Actual Loops"].as_f64() != Some(1.0)),
    )
}

/// A page of either pending feed reads the fanouts in a pending status
/// through the status index, once, and none of the settled fanouts,
/// prepared as sqlx runs it, in custom plans and in the generic plan
/// PostgreSQL may settle on after five executions.
#[tokio::test]
async fn a_pending_fanouts_page_never_reads_the_settled_history() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    // 19,500 deep-confirmed fanouts, last checked a day ago, and 500
    // reorged ones under the blocks that left the chain.
    blocks(
        &a.pool,
        (1, SETTLED_BLOCKS, CHUNKS),
        "1000+n",
        "CASE WHEN n%40=0 THEN 'inactive' ELSE 'confirmed' END",
        "'mature'",
        "CASE WHEN n%40=0 THEN 'reorged' ELSE 'confirmed' END",
        "clock_timestamp()-interval '1 day'",
    )
    .await?;
    // Above them, an immature block's fanouts awaiting maturity, two mature
    // blocks' fanouts in every broadcast state, and a candidate block's.
    // Chunks 1, 5 and 9 wait out a retry; the rest are due.
    let first = SETTLED_BLOCKS + 1;
    let mature = format!("n IN ({},{})", first + 1, first + 2);
    blocks(
        &a.pool,
        (first, PENDING_BLOCKS, CHUNKS),
        "1000+n",
        &format!("CASE WHEN n={} THEN 'prepared' ELSE 'confirmed' END", first + 3),
        &format!("CASE WHEN {mature} THEN 'mature' ELSE 'immature' END"),
        &format!(
            "CASE WHEN {mature} THEN (ARRAY['broadcastable','broadcast_submitted','failed'])[1+c%3] ELSE 'awaiting_maturity' END"
        ),
        "CASE c%4 WHEN 0 THEN NULL WHEN 1 THEN clock_timestamp()+interval '1 hour' ELSE clock_timestamp()-interval '1 minute' END",
    )
    .await?;
    sqlx::raw_sql(
        "VACUUM ANALYZE qbit_ctv_fanout_artifacts, qbit_pool_blocks, qbit_ctv_fanout_sets",
    )
    .execute(&a.pool)
    .await?;
    // Three confirmed blocks, seven due chunks each.
    let due = 21i64;
    let mut conn = a.pool.acquire().await?;
    sqlx::raw_sql(&format!("PREPARE pending(bigint,bigint) AS {PENDING_SQL}"))
        .execute(&mut *conn)
        .await?;
    // The public feed's pages and the audit feed's one page of up to 1,000.
    let pages = [
        (50, 0),
        (5, 0),
        (5, 10),
        (5, 20),
        (1000, 0),
        (5, 5),
        (50, 0),
    ];
    // Each plan PostgreSQL may run the statement under, forced in turn,
    // whichever its cost comparison would pick.
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode={mode}"))
            .execute(&mut *conn)
            .await?;
        for (limit, offset) in pages {
            let page = format!("{mode} page {limit},{offset}");
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) EXECUTE pending({limit},{offset})"
            ))
            .fetch_one(&mut *conn)
            .await?;
            let (visited, scanned, repeated) = fanout_visits(&plan);
            ensure!(!scanned, "{page} scanned the fanout table: {plan}");
            ensure!(
                !repeated,
                "{page} ran a fanout table scan other than once: {plan}"
            );
            ensure!(
                reads(&plan, "qbit_ctv_fanout_artifacts_status_idx"),
                "{page} did not read the status index: {plan}"
            );
            ensure!(
                visited <= PENDING_VISITS,
                "{page} visited {visited} fanout rows, at most {PENDING_VISITS} allowed, of {} settled: {plan}",
                SETTLED_BLOCKS * CHUNKS
            );
            let output: Value = sqlx::query_scalar(&format!("EXECUTE pending({limit},{offset})"))
                .fetch_one(&mut *conn)
                .await?;
            ensure!(
                output["total_count"] == due
                    && output["rows"].as_array().map(Vec::len)
                        == Some((due - offset).clamp(0, limit) as usize),
                "{page} returned the wrong page: {output}"
            );
        }
    }
    let (custom, generic): (i64, i64) = sqlx::query_as(
        "SELECT custom_plans,generic_plans FROM pg_prepared_statements WHERE name='pending'",
    )
    .fetch_one(&mut *conn)
    .await?;
    ensure!(
        custom > 0 && generic > 0,
        "{custom} custom and {generic} generic plans ran"
    );
    sqlx::raw_sql("DEALLOCATE pending; RESET plan_cache_mode")
        .execute(&mut *conn)
        .await?;
    drop(conn);
    db.close(vec![a]).await
}

/// The statuses the migrated schema's CHECK on `settlement_status` accepts.
async fn accepted_statuses(pool: &PgPool) -> Result<BTreeSet<String>> {
    let checks: Vec<String> = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
         WHERE conrelid='qbit_ctv_fanout_artifacts'::regclass AND contype='c' \
           AND pg_get_constraintdef(oid) LIKE '%settlement_status%'",
    )
    .fetch_all(pool)
    .await?;
    let [check] = checks.as_slice() else {
        bail!("expected one CHECK on settlement_status, found {checks:?}");
    };
    // `CHECK ((settlement_status = ANY (ARRAY['a'::text, 'b'::text])))`.
    Ok(quoted(check).into_iter().collect())
}

/// The statement lists the same fanouts as the `NOT IN` statement did, in
/// the same order, with the same count, page for page, compared as
/// serialized JSON, from which the feeds build their responses: under confirmed blocks, immature and mature,
/// two of them at one height, and under every other chain state, every
/// status the migrated schema accepts, never attempted, due and waiting
/// out a retry.
#[tokio::test]
async fn the_pending_feeds_keep_the_not_in_output() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let accepted = accepted_statuses(&a.pool).await?;
    ensure_split(&accepted, "the migrated schema")?;
    let statuses: Vec<String> = accepted.into_iter().collect();
    let schedules = [
        "NULL",
        "clock_timestamp()-interval '1 day'",
        "clock_timestamp()+interval '1 day'",
    ];
    // Each block holds every status in every schedule, one per chunk, in an
    // order that follows neither: chunk c is combination c*stride mod
    // chunks, a stride prime to the chunk count stepping through them all.
    let kinds = statuses.len() as i64;
    let chunks = kinds * schedules.len() as i64;
    let prime_to = |a: i64, b: i64| (2..=a.min(b)).all(|d| a % d != 0 || b % d != 0);
    let stride = (5..)
        .find(|stride| prime_to(*stride, chunks))
        .expect("a stride");
    let combination = format!("((c*{stride})%{chunks})");
    let status = format!(
        "(ARRAY['{}'])[1+{combination}%{kinds}]",
        statuses.join("','")
    );
    let schedule = format!(
        "(ARRAY[{}]::timestamptz[])[1+{combination}/{kinds}]",
        schedules.join(",")
    );
    let shapes = [
        ("100", "'confirmed'", "'immature'"),
        ("101", "'confirmed'", "'mature'"),
        ("101", "'confirmed'", "'mature'"),
        ("102", "'confirmed'", "'mature'"),
        ("100", "'prepared'", "'immature'"),
        ("101", "'inactive'", "'mature'"),
        ("102", "'rejected'", "'immature'"),
        ("103", "'reversed'", "'immature'"),
    ];
    for (n, (height, chain_state, maturity_state)) in (1..).zip(shapes) {
        blocks(
            &a.pool,
            (n, 1, chunks),
            height,
            chain_state,
            maturity_state,
            &status,
            &schedule,
        )
        .await?;
    }
    let complete: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT block_hash FROM qbit_ctv_fanout_artifacts GROUP BY block_hash \
         HAVING count(DISTINCT (settlement_status, CASE WHEN next_broadcast_attempt_at IS NULL THEN 0 \
                WHEN next_broadcast_attempt_at<=clock_timestamp() THEN 1 ELSE 2 END))=$1) complete",
    )
    .bind(chunks)
    .fetch_one(&a.pool)
    .await?;
    ensure!(
        complete == shapes.len() as i64,
        "{complete} of {} blocks hold every status in every schedule",
        shapes.len()
    );
    // Four confirmed blocks, each pending status never attempted and due.
    let pending = statuses.len() - SETTLED.len();
    let due = 4 * pending as i64 * 2;
    let mut conn = a.pool.acquire().await?;
    let mut listed = Vec::new();
    for limit in [1i64, 3, 7, due, due + 1, 100, 1000] {
        // Every page, and the empty one past the end.
        for page in 1..=(due + limit - 1) / limit + 1 {
            let offset = (page - 1) * limit;
            let mut outputs = Vec::new();
            for sql in [NOT_IN_SQL, PENDING_SQL] {
                let output: Value = sqlx::query_scalar(sql)
                    .bind(limit)
                    .bind(offset)
                    .fetch_one(&mut *conn)
                    .await?;
                outputs.push(output);
            }
            let [old, output] = <[Value; 2]>::try_from(outputs).expect("two outputs");
            let (old_bytes, new_bytes) = (
                serde_json::to_string(&old)?,
                serde_json::to_string(&output)?,
            );
            ensure!(
                old_bytes == new_bytes,
                "limit {limit} page {page}: NOT IN gave {old_bytes}, the status list {new_bytes}"
            );
            ensure!(
                output["total_count"] == due,
                "limit {limit} page {page}: {output}"
            );
            if limit == 7 {
                listed.extend(output["rows"].as_array().cloned().unwrap_or_default());
            }
        }
    }
    drop(conn);
    // The pages together list every due pending fanout of a confirmed block
    // once, in block height, chunk and txid order.
    ensure!(
        listed.len() as i64 == due,
        "the pages listed {} fanouts",
        listed.len()
    );
    let keys: Vec<_> = listed
        .iter()
        .map(|row| {
            (
                row["block_height"].as_i64(),
                row["chunk_index"].as_i64(),
                row["fanout_txid"].as_str().map(str::to_owned),
            )
        })
        .collect();
    ensure!(
        keys.windows(2).all(|pair| pair[0] < pair[1]),
        "the pages are out of order: {keys:?}"
    );
    let mut kinds_listed = BTreeSet::new();
    for row in &listed {
        let status = row["settlement_status"].as_str().unwrap_or("");
        ensure!(
            row["chain_state"] == "confirmed" && !SETTLED.contains(&status),
            "a page listed {row}"
        );
        kinds_listed.insert((
            status.to_owned(),
            row["next_broadcast_attempt_at"].is_null(),
        ));
    }
    ensure!(
        kinds_listed.len() == pending * 2,
        "the pages miss a pending status or schedule: {kinds_listed:?}"
    );
    db.close(vec![a]).await
}

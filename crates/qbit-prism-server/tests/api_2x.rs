//! 2.x dashboard behavior against real PostgreSQL and a controlled node RPC.
use anyhow::{ensure, Result};
use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    routing::post,
    Json, Router,
};
use qbit_prism_server::{
    api::{
        self,
        public_service::{self, ServiceConfig},
        ApiConfig, ApiState,
    },
    ledger::Ledger,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio_util::task::AbortOnDropHandle;
use tower::ServiceExt;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

struct Fixture {
    database: FixtureDatabase,
    pool: PgPool,
    rpc: String,
    node: AbortOnDropHandle<()>,
    network: Arc<Mutex<Value>>,
}
impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let network = Arc::new(Mutex::new(json!("1234567890123.125")));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc = format!("http://{}/", listener.local_addr()?);
        // Advisory migration locks are database-wide, so schemas alone do
        // not isolate concurrent fixture setup.
        let database = FixtureDatabase::open(&raw, "api2_").await?;
        let ledger = match Ledger::connect(&database.url, "api2-test".into(), 4, true).await {
            Ok(ledger) => ledger,
            Err(error) => return Err(database.abandon(error).await),
        };
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(network.clone());
        let node = AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }));
        Ok(Some(Self {
            database,
            pool: ledger.pool.clone(),
            rpc,
            node,
            network,
        }))
    }
    fn state(&self, cache: bool) -> ApiState {
        ApiState::new(
            self.pool.clone(),
            ApiConfig {
                rpc_url: self.rpc.clone(),
                cache_enabled: cache,
                read_timeout: Duration::from_millis(250),
                ..Default::default()
            },
            std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
        )
    }
    async fn close(self, result: Result<()>) -> Result<()> {
        self.node.abort();
        self.pool.close().await;
        self.database.close(result).await
    }
    async fn block(&self, n: u8, height: i64, state: &str, at: i64) -> Result<String> {
        let hash = format!("{n:02x}").repeat(32);
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,found_at,maturity_state,disconnected_at) VALUES($1,$2,repeat('0',64),repeat('1',64),repeat('2',64),$3,to_timestamp($4),CASE WHEN $3='reversed' THEN 'reversed' ELSE 'immature' END,CASE WHEN $3='reversed' THEN to_timestamp($4+10) ELSE NULL END)")
            .bind(&hash).bind(height).bind(state).bind(at as f64).execute(&self.pool).await?;
        Ok(hash)
    }
    async fn share(&self, id: &str, miner: &str, at: i64, difficulty: i64) -> Result<i64> {
        Ok(sqlx::query_scalar("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) VALUES($1,$2,$2,decode(repeat('1',64),'hex'),$3::bigint,1000000,100,'job',to_timestamp($4),0,to_timestamp($4),'test',1) RETURNING share_seq")
            .bind(id).bind(miner).bind(difficulty).bind(at as f64).fetch_one(&self.pool).await?)
    }
}
async fn node_reply(
    State(network): State<Arc<Mutex<Value>>>,
    Json(input): Json<Value>,
) -> Json<Value> {
    let result = match input["method"].as_str().unwrap_or("") {
        "getblockchaininfo" => {
            json!({"chain":"regtest","blocks":100,"bestblockhash":"ab".repeat(32),"initialblockdownload":false})
        }
        "getblocktemplate" => json!({"bits":"207fffff","coinbasevalue":5000000000u64}),
        "getnetworkinfo" => json!({"connections":2}),
        "getnetworkhashps" => {
            assert_eq!(input["params"], json!([120, -1, "permissionless"]));
            network.lock().await.clone()
        }
        _ => Value::Null,
    };
    Json(json!({"id":input["id"],"result":result,"error":null}))
}
async fn raw_get(app: &Router, path: &str) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 10_000_000).await.unwrap();
    (status, headers, bytes.to_vec())
}
async fn get(app: &Router, path: &str) -> (StatusCode, HeaderMap, Value) {
    let (status, headers, body) = raw_get(app, path).await;
    (status, headers, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn block_views_markers_and_network_estimate_keep_2x_contracts() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result=async {
        let at=chrono::Utc::now().timestamp().div_euclid(300)*300-600;
        let mut confirmed=Vec::new();
        for n in 1..=4 {confirmed.push(f.block(n,100+n as i64,"confirmed",at).await?);}
        f.block(5,105,"prepared",at).await?;f.block(6,106,"rejected",at).await?;f.block(7,107,"inactive",at).await?;
        let disconnected=f.block(8,108,"confirmed",at).await?;
        sqlx::query("UPDATE qbit_pool_blocks SET chain_state='inactive',inactive_since=to_timestamp($2) WHERE block_hash=$1").bind(&disconnected).bind((at+15) as f64).execute(&f.pool).await?;
        f.block(9,109,"reversed",at).await?;
        let app=api::router(f.state(false));
        let(status,_,active)=get(&app,"/public/v1/blocks").await;ensure!(status==StatusCode::OK,"{active}");
        ensure!(active["schema"]=="prism.dashboard.blocks.v1"&&active["pagination"]["total_count"]==4);
        ensure!(active["rows"].as_array().unwrap().iter().all(|row|row.get("chain_state").is_none()));
        let(status,_,all)=get(&app,"/public/v1/blocks?chain_state=all&limit=3&page=2").await;ensure!(status==StatusCode::OK,"{all}");
        ensure!(all["schema"]=="prism.dashboard.blocks.v2"&&all["pagination"]["total_count"]==9&&all["pagination"]["total_pages"]==3);
        let(_,_,reversed)=get(&app,"/public/v1/blocks?chain_state=reversed").await;
        ensure!(reversed["pagination"]["total_count"]==2,"{reversed}");
        ensure!(reversed["rows"].as_array().unwrap().iter().all(|row|row["chain_state"]=="reversed"&&row["disconnected_at"].is_string()));
        let(status,_,markers)=get(&app,"/public/v1/block-markers?range=1w&bucket=5m").await;ensure!(status==StatusCode::OK,"{markers}");
        ensure!(markers["schema"]=="prism.dashboard.block-markers.v1"&&markers["bucket_seconds"]==300&&markers["total_blocks"]==4);
        ensure!(markers["points"].as_array().unwrap().len()==1&&markers["points"][0]["block_count"]==4&&markers["points"][0]["truncated"]==true);
        ensure!(markers["points"][0]["blocks"].as_array().unwrap().len()==3&&markers["points"][0]["blocks"][0]["hash"]==confirmed[3]);
        let(status,_,summary)=get(&app,"/public/v1/pool-summary").await;ensure!(status==StatusCode::OK,"{summary}");
        ensure!(summary["network"]["hashrate_ths"]=="1.234567890123125");
        ensure!(summary["pool"]["blocks_found_total"]==4&&summary["pool"]["blocks_reversed_total"]==2&&summary["pool"]["blocks_inactive_total"]==1);
        *f.network.lock().await=json!("invalid");
        let(status,_,summary)=get(&app,"/public/v1/pool-summary").await;ensure!(status==StatusCode::OK&&summary["network"]["hashrate_ths"].is_null());
        sqlx::query("UPDATE qbit_pool_blocks SET chain_state='confirmed',inactive_since=NULL WHERE block_hash=$1").bind(&disconnected).execute(&f.pool).await?;
        let(_,_,reversed)=get(&app,"/public/v1/blocks?chain_state=reversed").await;ensure!(reversed["pagination"]["total_count"]==1);
        Ok::<_,anyhow::Error>(())
    }.await;
    f.close(result).await
}

#[tokio::test]
async fn chart_rollups_match_raw_boundaries_tail_and_missing_progress() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result=async {
        let epoch=chrono::Utc::now().timestamp();let lower=epoch-7200;
        f.share("before","alice",lower-1,1000000).await?;
        f.share("partial","alice",lower+1,2000000).await?;
        f.share("full","bob",epoch-4000,3000000).await?;
        f.share("current","alice",epoch-1,4000000).await?;
        let watermark=f.share("future","alice",epoch+60,5000000).await?;
        let query=|sql:&'static str,subject:Option<&'static str>|sqlx::query_scalar::<_,Value>(sql).bind(3600i64).bind(Some(7200i64)).bind(Some(epoch as f64)).bind(subject);
        let raw_sql=include_str!("../src/api/queries/dashboard_hashrate_series.sql");
        let rollup_sql=include_str!("../src/api/queries/dashboard_hashrate_rollups.sql");
        for subject in [None,Some("alice")] {ensure!(query(raw_sql,subject).fetch_one(&f.pool).await?==query(rollup_sql,subject).fetch_one(&f.pool).await?,"empty-progress fallback differs");}
        let progress=qbit_prism_server::rollups::advance(&f.pool,100).await?;
        ensure!(progress.advanced&&progress.scanned==5&&progress.last_share_seq==watermark);
        f.share("tail","alice",epoch-3900,6000000).await?;
        for subject in [None,Some("alice")] {let raw=query(raw_sql,subject).fetch_one(&f.pool).await?;let rolled=query(rollup_sql,subject).fetch_one(&f.pool).await?;ensure!(raw==rolled,"rollup boundary/tail mismatch: {raw} vs {rolled}");}
        let app=api::router(f.state(false));
        let(status,_,v1)=get(&app,"/public/v1/hashrate-series?range=1w&bucket=5m").await;ensure!(status==StatusCode::OK,"{v1}");
        let(status,_,v2)=get(&app,"/public/v1/hashrate-series?range=1w&bucket=5m&view=both").await;ensure!(status==StatusCode::OK,"{v2}");
        ensure!(v2["schema"]=="prism.dashboard.hashrate-series.v2"&&v2["smoothing"]==json!({"method":"trailing","window_seconds":1800}));
        for (a,b) in v1["points"].as_array().unwrap().iter().zip(v2["points"].as_array().unwrap()) {ensure!(a["timestamp"]==b["timestamp"]&&a["hashrate_ths"]==b["smoothed_hashrate_ths"]);}
        sqlx::query("DROP TABLE qbit_hashrate_rollup_pool,qbit_hashrate_rollup_miner,qbit_hashrate_rollup_progress").execute(&f.pool).await?;
        let(status,_,fallback)=get(&app,"/public/v1/hashrate-series?range=1w&bucket=5m&view=both").await;ensure!(status==StatusCode::OK&&fallback["points"]==v2["points"],"pre-schema fallback differs: {fallback}");
        Ok::<_,anyhow::Error>(())
    }.await;
    f.close(result).await
}
/// Compares the rollup series with the raw ledger series for every bucket
/// and range pair the API allows, with and without smoothing context and an
/// anchor, for the pool, a miner and an unknown miner. Returns the cases run.
async fn rollups_match_raw(pool: &PgPool, epoch: i64, state: &str) -> Result<usize> {
    let raw_sql = include_str!("../src/api/queries/dashboard_hashrate_series.sql");
    let rollup_sql = include_str!("../src/api/queries/dashboard_hashrate_rollups.sql");
    let mut cases = 0;
    for (bucket, range) in [
        (300i64, Some(7 * 86400i64)),
        (3600, Some(7 * 86400)),
        (86400, Some(7 * 86400)),
        (3600, Some(30 * 86400)),
        (86400, Some(30 * 86400)),
        (86400, Some(180 * 86400)),
        (86400, None),
    ] {
        for context in [0i64, 7200] {
            if range.is_none() && context > 0 {
                continue;
            }
            for anchor in [None, Some(epoch as f64)] {
                for subject in [None, Some("alice"), Some("nobody")] {
                    let run = |sql: &'static str| {
                        sqlx::query_scalar::<_, Value>(sql)
                            .bind(bucket)
                            .bind(range.map(|range| range + context))
                            .bind(anchor)
                            .bind(subject)
                            .fetch_one(pool)
                    };
                    let (mut raw, mut rolled) = (run(raw_sql).await?, run(rollup_sql).await?);
                    // A bucket or range edge can pass between the two reads.
                    if raw != rolled {
                        (raw, rolled) = (run(raw_sql).await?, run(rollup_sql).await?);
                    }
                    ensure!(
                        raw == rolled,
                        "{state}: bucket {bucket} range {range:?}+{context} anchor {anchor:?} subject {subject:?}: raw {raw} vs rollups {rolled}"
                    );
                    cases += 1;
                }
            }
        }
    }
    Ok(cases)
}
#[tokio::test]
async fn chart_rollups_match_raw_for_every_range_bucket_and_watermark() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // Shares on each side of every bucket, range and smoothing edge the
        // chart reads, a spread over 200 days, and one in the future.
        let epoch = chrono::Utc::now().timestamp();
        let mut at = Vec::new();
        for bucket in [300i64, 3600, 86400] {
            let edge = epoch.div_euclid(bucket) * bucket;
            at.extend([edge - 1, edge, edge + 1]);
        }
        for range in [7 * 86400i64, 30 * 86400, 180 * 86400] {
            for context in [0i64, 7200] {
                let edge = epoch - range - context;
                at.extend([edge - 1, edge, edge + 1]);
            }
        }
        at.extend((0..200i64).map(|n| epoch - n * 86400 - n * n * 7));
        at.push(epoch + 60);
        for (n, t) in at.iter().enumerate() {
            f.share(
                &format!("edge-{n}"),
                ["alice", "bob", "carol"][n % 3],
                *t,
                1_000_000 + n as i64,
            )
            .await?;
        }
        let cases = rollups_match_raw(&f.pool, epoch, "no progress").await?;
        ensure!(cases == 78, "{cases} cases");
        let progress = qbit_prism_server::rollups::advance(&f.pool, 100_000).await?;
        ensure!(
            progress.advanced && progress.scanned == at.len() as i64,
            "{progress:?}"
        );
        rollups_match_raw(&f.pool, epoch, "current watermark").await?;
        // Shares past the watermark, one dated into an already rolled bucket.
        for (n, t) in [epoch - 30, epoch - 5 * 86400, epoch - 6 * 86400]
            .iter()
            .enumerate()
        {
            f.share(&format!("tail-{n}"), "alice", *t, 7_000_000)
                .await?;
        }
        rollups_match_raw(&f.pool, epoch, "lagging watermark").await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.close(result).await
}
/// Ledger shares a plan node produced or dropped (by its filter, or on a
/// lossy bitmap's recheck), over every loop. Row counts are per-loop
/// averages, fractional from PostgreSQL 18.
fn ledger_tuples(node: &Value) -> f64 {
    let own = if node["Relation Name"]
        .as_str()
        .is_some_and(|name| name.starts_with("qbit_share_ledger"))
    {
        let count = |key: &str| node[key].as_f64().unwrap_or(0.0);
        (count("Actual Rows")
            + count("Rows Removed by Filter")
            + count("Rows Removed by Index Recheck"))
            * node["Actual Loops"].as_f64().unwrap_or(1.0)
    } else {
        0.0
    };
    own + node["Plans"]
        .as_array()
        .map_or(0.0, |plans| plans.iter().map(ledger_tuples).sum())
}
/// Ledger scans that bound no column from both sides, outside a Limit's
/// single probe: at production scale each walks an index from one end, or the
/// whole partition, even where a small fixture's run read nothing.
fn one_sided_ledger_scans(node: &Value, under_limit: bool, found: &mut Vec<String>) {
    let ledger = |key: &str| {
        node[key]
            .as_str()
            .is_some_and(|name| name.starts_with("qbit_share_ledger"))
    };
    let kind = node["Node Type"].as_str().unwrap_or("");
    if !under_limit && (ledger("Relation Name") || ledger("Index Name")) {
        let cond = node["Index Cond"].as_str().unwrap_or("");
        let tokens: Vec<&str> = cond
            .split(|c: char| c.is_whitespace() || c == '(' || c == ')')
            .filter(|token| !token.is_empty())
            .collect();
        let bounded = |ops: &[&str]| -> std::collections::HashSet<&str> {
            tokens
                .windows(2)
                .filter(|pair| ops.contains(&pair[1]))
                .map(|pair| pair[0])
                .collect()
        };
        let two_sided = !bounded(&[">", ">="]).is_disjoint(&bounded(&["<", "<="]));
        let examined = node["Actual Rows"].as_f64().unwrap_or(0.0)
            + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0);
        let walks = match kind {
            "Seq Scan" => examined > 0.0,
            "Index Scan" | "Index Only Scan" | "Bitmap Index Scan" => !two_sided,
            _ => false,
        };
        if walks {
            found.push(format!(
                "{kind} on {}: {}",
                node["Index Name"]
                    .as_str()
                    .or(node["Relation Name"].as_str())
                    .unwrap_or("?"),
                if cond.is_empty() {
                    "no index condition"
                } else {
                    cond
                }
            ));
        }
    }
    for plan in node["Plans"].as_array().into_iter().flatten() {
        one_sided_ledger_scans(plan, under_limit || kind == "Limit", found);
    }
}
#[tokio::test]
async fn chart_rollups_read_only_the_partial_buckets_under_any_plan() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result=async {
        // 20,000 rolled-up shares over 60 days, half of them one miner's; a
        // chart's two partial buckets hold a few dozen. A boundary or tail
        // whose bounds the planner can't use walks a whole index instead:
        // 17-27 s on a 66M-share ledger. Which index it walks depends on the
        // plan mode and the cost settings, so the test tries both modes under
        // PostgreSQL's default random_page_cost and under 1.1, the SSD value
        // a production planner chose the walk under.
        let epoch=chrono::Utc::now().timestamp();
        sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) SELECT 'plan-'||g,m,m,decode(repeat('1',64),'hex'),1000000,1000000,100,'job',to_timestamp($1-(20001-g)*259),0,to_timestamp($1-(20001-g)*259),'test',1 FROM generate_series(1,20000) g, LATERAL (SELECT CASE WHEN g%2=0 THEN 'whale' ELSE 'miner-'||g%50 END AS m) miner").bind(epoch as f64).execute(&f.pool).await?;
        let progress=qbit_prism_server::rollups::advance(&f.pool,100_000).await?;
        ensure!(progress.scanned==20_000,"{progress:?}");
        sqlx::raw_sql("VACUUM (ANALYZE) qbit_share_ledger").execute(&f.pool).await?;
        let sql=include_str!("../src/api/queries/dashboard_hashrate_rollups.sql");
        // Its own connection, never returned to the pool, so the statement and
        // plan mode set below reach no other query; the fixture's DROP
        // DATABASE ... WITH (FORCE) ends it, whatever the outcome.
        let mut conn=f.pool.acquire().await?.detach();
        sqlx::raw_sql(&format!("PREPARE chart(bigint,bigint,double precision,text) AS {sql}")).execute(&mut conn).await?;
        // The default pool request and a 7-day one, both without an anchor; a
        // 5-minute one, whose smoothing context widens the range and binds
        // the anchor; and the default request for a small miner and for the
        // miner with half the shares.
        let requests=["3600,2592000,NULL,NULL".to_owned(),"3600,604800,NULL,NULL".to_owned(),format!("300,606600,{epoch},NULL"),"3600,2592000,NULL,'miner-7'".to_owned(),"3600,2592000,NULL,'whale'".to_owned()];
        let mut walks=Vec::new();
        // First with the rollups current, so the tail is empty; then with 300
        // shares past the watermark, half the big miner's, which the tail must
        // read and nothing else: its cost follows the rollup lag.
        for (phase,lag) in [("current watermark",0.0),("300 shares past the watermark",300.0)] {
            if lag>0.0 {
                sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) SELECT 'lag-'||g,m,m,decode(repeat('1',64),'hex'),1000000,1000000,100,'job',to_timestamp($1-g*120),0,to_timestamp($1-g*120),'test',1 FROM generate_series(1,300) g, LATERAL (SELECT CASE WHEN g%2=0 THEN 'whale' ELSE 'miner-7' END AS m) miner").bind(epoch as f64).execute(&f.pool).await?;
            }
            for cost in ["4","1.1"] {
                for mode in ["force_custom_plan","force_generic_plan"] {
                    sqlx::raw_sql(&format!("SET random_page_cost={cost}; SET plan_cache_mode={mode}")).execute(&mut conn).await?;
                    for request in &requests {
                        let plan:Value=sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE chart({request})")).fetch_one(&mut conn).await?;
                        let read=ledger_tuples(&plan[0]["Plan"]).round();
                        if read>1_000.0+lag {walks.push(format!("{phase}, random_page_cost {cost}, {mode}, chart({request}): read {read} ledger shares"));}
                        let mut scans=Vec::new();
                        one_sided_ledger_scans(&plan[0]["Plan"],false,&mut scans);
                        walks.extend(scans.into_iter().map(|scan|format!("{phase}, random_page_cost {cost}, {mode}, chart({request}): {scan}")));
                    }
                }
            }
        }
        ensure!(walks.is_empty(),"the chart plans walk the ledger:\n{}",walks.join("\n"));
        Ok::<_,anyhow::Error>(())
    }.await;
    f.close(result).await
}

/// The dashboard queries as 3.1.1 shipped them, before #857.
mod dashboard_3_1_1 {
    pub(super) const MINER_SHARE_SUMMARY: &str =
        include_str!("fixtures/dashboard_queries_3_1_1/dashboard_miner_share_summary.sql");
    pub(super) const REWARD_LEADERBOARD: &str =
        include_str!("fixtures/dashboard_queries_3_1_1/dashboard_reward_leaderboard.sql");
    pub(super) const POOL_SNAPSHOT: &str =
        include_str!("fixtures/dashboard_queries_3_1_1/dashboard_pool_snapshot.sql");
    pub(super) const BLOCKS: &str =
        include_str!("fixtures/dashboard_queries_3_1_1/dashboard_blocks.sql");
}
const MINER_SHARE_SUMMARY: &str =
    include_str!("../src/api/queries/dashboard_miner_share_summary.sql");
const MINER_REWARD_SHARE: &str =
    include_str!("../src/api/queries/dashboard_miner_reward_share.sql");
const POOL_SNAPSHOT: &str = include_str!("../src/api/queries/dashboard_pool_snapshot.sql");
const POOL_HASHRATE: &str = include_str!("../src/api/queries/dashboard_pool_hashrate.sql");
const POOL_HASHRATE_ROLLUPS: &str =
    include_str!("../src/api/queries/dashboard_pool_hashrate_rollups.sql");
const BLOCKS: &str = include_str!("../src/api/queries/dashboard_blocks.sql");

/// Compares the 3.1.1 answer `read` returns with the new one.
async fn same_answer<F, Fut>(what: &str, read: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<(Value, Value)>>,
{
    let (old, new) = read().await?;
    ensure!(old == new, "{what}: 3.1.1 {old} vs now {new}");
    Ok(())
}

/// A pool on `pool`'s database whose sessions read `clock_timestamp()` as
/// `at`, from `qbit_test_clock`, which their `search_path` puts before
/// `pg_catalog`. The 3.1.1 statements and their rewrites read only that
/// clock, so on this pool both answer at one instant, every run: a share
/// placed on a window or bucket edge relative to `at` is on the edge they
/// read, and none can cross it between the two statements. Its sessions
/// keep the ledger pool's statement, lock and checkout timeouts.
async fn pinned_clock_pool(pool: &PgPool, at: i64) -> Result<PgPool> {
    sqlx::raw_sql(
        "CREATE SCHEMA qbit_test_clock;\
         CREATE TABLE qbit_test_clock.pinned(at timestamptz NOT NULL);\
         CREATE FUNCTION qbit_test_clock.clock_timestamp() RETURNS timestamptz \
         LANGUAGE sql VOLATILE AS 'SELECT at FROM qbit_test_clock.pinned'",
    )
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO qbit_test_clock.pinned VALUES(to_timestamp($1))")
        .bind(at as f64)
        .execute(pool)
        .await?;
    let (schemas, statement_timeout, lock_timeout): (String, String, String) = sqlx::query_as(
        "SELECT current_setting('search_path'),current_setting('statement_timeout'),current_setting('lock_timeout')",
    )
    .fetch_one(pool)
    .await?;
    let path = format!("qbit_test_clock, pg_catalog, {schemas}");
    let pinned = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(15))
        .after_connect(move |connection, _| {
            let settings = (path.clone(), statement_timeout.clone(), lock_timeout.clone());
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path',$1,false),set_config('statement_timeout',$2,false),set_config('lock_timeout',$3,false)")
                    .bind(settings.0)
                    .bind(settings.1)
                    .bind(settings.2)
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(pool.connect_options().as_ref().clone())
        .await?;
    let read: f64 = sqlx::query_scalar("SELECT extract(epoch FROM clock_timestamp())::float8")
        .fetch_one(&pinned)
        .await?;
    ensure!(read == at as f64, "the pinned clock reads {read}, not {at}");
    Ok(pinned)
}

/// Moves `pinned_clock_pool`'s clock to `at`, in seconds since the epoch.
async fn pin_clock(pool: &PgPool, at: f64) -> Result<()> {
    sqlx::query("UPDATE qbit_test_clock.pinned SET at=to_timestamp($1)")
        .bind(at)
        .execute(pool)
        .await?;
    Ok(())
}

/// Whether `dashboard_pool_hashrate_rollups.sql` reads the rollups at `at`:
/// its readiness test, the share at or below the watermark inside the 24
/// hours.
async fn rollups_ready_at(pool: &PgPool, at: f64) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE((SELECT ledger.accepted_at >= to_timestamp($1) - interval '24 hours' \
         FROM qbit_share_ledger ledger \
         WHERE ledger.share_seq <= (SELECT last_share_seq FROM qbit_hashrate_rollup_progress WHERE singleton) \
         ORDER BY ledger.share_seq DESC LIMIT 1), false)",
    )
    .bind(at)
    .fetch_one(pool)
    .await?)
}

/// [`dashboard_queries_match_3_1_1`] with `pinned`'s clock at each of
/// `instants`, then back at the first.
async fn dashboard_queries_match_3_1_1_at(
    pinned: &PgPool,
    instants: &[f64],
    phase: &str,
) -> Result<()> {
    for at in instants {
        pin_clock(pinned, *at).await?;
        dashboard_queries_match_3_1_1(pinned, &format!("{phase} at {at}")).await?;
    }
    pin_clock(pinned, instants[0]).await
}

/// Every #857 rewrite answers what its 3.1.1 query answers on the same data.
async fn dashboard_queries_match_3_1_1(pool: &PgPool, phase: &str) -> Result<usize> {
    let mut cases = 0;
    for miner in ["alice", "bob", "carol", "eve", "nobody"] {
        same_answer(&format!("{phase}: miner share summary {miner}"), || async {
            let mut old: Value = sqlx::query_scalar(dashboard_3_1_1::MINER_SHARE_SUMMARY)
                .bind(miner)
                .fetch_one(pool)
                .await?;
            // The pool's 3-hour sum nothing read is what #857 dropped.
            ensure!(old
                .as_object_mut()
                .unwrap()
                .remove("pool_h3_difficulty")
                .is_some());
            let new: Value = sqlx::query_scalar(MINER_SHARE_SUMMARY)
                .bind(miner)
                .fetch_one(pool)
                .await?;
            Ok((old, new))
        })
        .await?;
        cases += 1;
        for difficulty in ["1000000", "250000", "1000000000"] {
            same_answer(
                &format!("{phase}: reward share {miner} at difficulty {difficulty}"),
                || async {
                    let board: Value = sqlx::query_scalar(dashboard_3_1_1::REWARD_LEADERBOARD)
                        .bind(difficulty)
                        .bind(None::<String>)
                        .bind(miner)
                        .bind(1i64)
                        .bind(0i64)
                        .fetch_one(pool)
                        .await?;
                    let row = &board["rows"][0];
                    let old = json!({"counted_window_weight":board["counted_window_weight"],"counted_share_difficulty":row["counted_share_difficulty"],"share_percent":row["share_percent"]});
                    let new: Value = sqlx::query_scalar(MINER_REWARD_SHARE)
                        .bind(difficulty)
                        .bind(miner)
                        .fetch_one(pool)
                        .await?;
                    Ok((old, new))
                },
            )
            .await?;
            cases += 1;
        }
    }
    for difficulty in ["1000000", "1000000000"] {
        for hashrate_sql in [POOL_HASHRATE, POOL_HASHRATE_ROLLUPS] {
            let sums = if hashrate_sql == POOL_HASHRATE {
                "raw"
            } else {
                "rollup"
            };
            same_answer(
                &format!("{phase}: pool snapshot with {sums} sums at difficulty {difficulty}"),
                || async {
                    let old: Value = sqlx::query_scalar(dashboard_3_1_1::POOL_SNAPSHOT)
                        .bind(difficulty)
                        .fetch_one(pool)
                        .await?;
                    let mut new: Value = sqlx::query_scalar(POOL_SNAPSHOT)
                        .bind(difficulty)
                        .fetch_one(pool)
                        .await?;
                    let sums: Value = sqlx::query_scalar(hashrate_sql).fetch_one(pool).await?;
                    for (key, value) in sums.as_object().unwrap() {
                        ensure!(new.get(key).is_none(), "{key} is read twice");
                        new[key] = value.clone();
                    }
                    Ok((old, new))
                },
            )
            .await?;
            cases += 1;
        }
    }
    for filter in ["active", "all", "reversed"] {
        for (limit, offset) in [(100i64, 0i64), (2, 0), (2, 2), (2, 4), (3, 6), (1, 100)] {
            let read = |sql: &'static str| {
                sqlx::query_scalar::<_, Value>(sql)
                    .bind(limit)
                    .bind(offset)
                    .bind(filter)
                    .fetch_one(pool)
            };
            let (old, new) = (read(dashboard_3_1_1::BLOCKS).await?, read(BLOCKS).await?);
            ensure!(
                old == new,
                "{phase}: blocks {filter} limit {limit} offset {offset}: 3.1.1 {old} vs now {new}"
            );
            cases += 1;
        }
    }
    Ok(cases)
}

#[tokio::test]
async fn dashboard_queries_answer_what_their_3_1_1_queries_answer() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // Every comparison reads one clock, pinned at `epoch`, so the shares
        // placed below sit on the exact window and bucket edges the
        // statements read, on every run. A fixed instant, mid-bucket: each
        // window's first bucket is partial, so every slice below is where
        // its comment says, and a statement that read the real clock, weeks
        // later, would miss every share and fail.
        let epoch: i64 = 1_789_999_950;
        ensure!(epoch.rem_euclid(300) == 150);
        let pinned = pinned_clock_pool(&f.pool, epoch).await?;
        let cases = dashboard_queries_match_3_1_1(&pinned, "empty database").await?;
        ensure!(cases == 42, "{cases} cases");
        // Shares on each side of every window and 5-minute bucket edge the
        // pool summary and the miner page read, others spread over 30 hours,
        // one in the future; carol has a single share, nobody none.
        let mut at = Vec::new();
        let current = epoch.div_euclid(300) * 300;
        at.extend([current - 1, current, current + 1]);
        for span in [60i64, 300, 600, 3600, 10800, 86400] {
            let edge = epoch - span;
            let bucket = edge.div_euclid(300) * 300;
            at.extend([edge - 1, edge, edge + 1, bucket - 1, bucket, bucket + 300]);
        }
        at.extend((0..120i64).map(|n| epoch - n * 800 - n * n));
        at.push(epoch + 60);
        for (n, t) in at.iter().enumerate() {
            f.share(&format!("edge-{n}"), ["alice", "bob"][n % 2], *t, 100_000 + n as i64 * 7)
                .await?;
        }
        f.share("carol-1", "carol", epoch - 1200, 333_333).await?;
        // Miners that tell the 3-hour participants from the 24-hour ones:
        // eve only older than 3 hours (in a whole bucket, and in the 3-hour
        // window's first, partial, bucket just before it starts; a share
        // past the watermark follows below); frank and grace each only in a
        // partial slice, the current bucket and the 3-hour window's first
        // bucket, so the miner rollups alone never count them.
        f.share("eve-1", "eve", epoch - 5 * 3600, 444_444).await?;
        f.share("eve-2", "eve", epoch - 10800 - 30, 444_445).await?;
        f.share("frank-1", "frank", current.max(epoch - 20), 555_555)
            .await?;
        f.share("grace-1", "grace", epoch - 10800 + 20, 666_666)
            .await?;
        // henry only in the 24-hour window's first, partial, bucket. henry-2,
        // the last share before the rollups advance, is the watermark's
        // share: inside the 24 hours at every instant compared below.
        f.share("henry-1", "henry", epoch - 86400 + 20, 777_777)
            .await?;
        f.share(
            "henry-2",
            "henry",
            (epoch - 86400 + 299).div_euclid(300) * 300 - 1,
            777_778,
        )
        .await?;
        // Blocks in every chain state, a reversed one and a disconnected one
        // whose publication makes it public as reversed, two at one height.
        let found = epoch - 3600;
        for (n, height, state) in [
            (1u8, 101i64, "confirmed"),
            (2, 102, "confirmed"),
            (3, 103, "reversed"),
            (4, 104, "confirmed"),
            (5, 105, "prepared"),
            (6, 106, "rejected"),
            (7, 107, "inactive"),
            (8, 108, "inactive"),
            (9, 109, "confirmed"),
            (10, 109, "confirmed"),
        ] {
            f.block(n, height, state, found + n as i64).await?;
        }
        sqlx::query("UPDATE qbit_pool_blocks SET audit_publication_sequence=(SELECT COALESCE(max(audit_publication_sequence),0)+1 FROM qbit_pool_blocks),inactive_since=to_timestamp($2) WHERE block_hash=$1")
            .bind(format!("{:02x}", 8u8).repeat(32))
            .bind((found + 100) as f64)
            .execute(&f.pool)
            .await?;
        // The phases the rollups can serve are compared at the pinned
        // instant and four more: bucket-aligned (every window's leading slice
        // empty), a second into the bucket, its last second, and a
        // fractional one.
        let instants = [0.0, -150.0, -149.0, 149.0, 0.5].map(|offset| epoch as f64 + offset);
        ensure!(!rollups_ready_at(&f.pool, instants[0]).await?);
        dashboard_queries_match_3_1_1_at(&pinned, &instants, "no rollup progress").await?;
        let progress = qbit_prism_server::rollups::advance(&f.pool, 100_000).await?;
        ensure!(progress.advanced, "{progress:?}");
        for at in instants {
            ensure!(rollups_ready_at(&f.pool, at).await?, "the rollups are not read at {at}");
        }
        dashboard_queries_match_3_1_1_at(&pinned, &instants, "current watermark").await?;
        // Shares past the watermark, in the current bucket, in each window's
        // partial first bucket and in an already rolled one, on each side of
        // every window's start, at the pinned instant and after it, a miner
        // the rollups have never seen, eve again older than 3 hours, and
        // ivan and judy only on either side of the 3-hour start, all placed
        // by the same pinned clock.
        for (n, t) in [
            epoch - 20,
            epoch - 3590,
            epoch - 10790,
            epoch - 86390,
            epoch - 4900,
            epoch - 86401,
            epoch - 86400,
            epoch - 10801,
            epoch - 10800,
            epoch - 3601,
            epoch - 3600,
            epoch,
            epoch + 30,
        ]
        .iter()
        .enumerate()
        {
            f.share(&format!("tail-{n}"), ["alice", "dave"][n % 2], *t, 7_000_000 + n as i64)
                .await?;
        }
        f.share("eve-3", "eve", epoch - 4 * 3600, 444_446).await?;
        f.share("ivan-1", "ivan", epoch - 10800, 888_888).await?;
        f.share("judy-1", "judy", epoch - 10801, 999_999).await?;
        dashboard_queries_match_3_1_1_at(&pinned, &instants, "lagging watermark").await?;
        // A watermark a day behind, its share older than the 24 hours (and,
        // here, the rollups ahead of it, which the sums must then not read).
        sqlx::query("UPDATE qbit_hashrate_rollup_progress SET last_share_seq=(SELECT share_seq FROM qbit_share_ledger WHERE accepted_at<to_timestamp($1)-interval '25 hours' ORDER BY share_seq DESC LIMIT 1)")
            .bind(epoch as f64)
            .execute(&f.pool)
            .await?;
        ensure!(!rollups_ready_at(&f.pool, instants[0]).await?);
        dashboard_queries_match_3_1_1(&pinned, "watermark a day behind").await?;
        pinned.close().await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.close(result).await
}

/// Every plan node, depth first, init plans and CTEs included.
fn plan_nodes<'a>(node: &'a Value, all: &mut Vec<&'a Value>) {
    all.push(node);
    for plan in node["Plans"].as_array().into_iter().flatten() {
        plan_nodes(plan, all);
    }
}

/// #857: an `active` blocks page is an ordered read of
/// qbit_pool_blocks_public_recent_idx that stops at its page, under custom
/// and generic plans and PostgreSQL's default and SSD page costs, where
/// 3.1.1's CASE filter scanned every block.
#[tokio::test]
async fn an_active_blocks_page_reads_the_public_recent_index_under_any_plan() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // 5,000 blocks, one in fifty reversed, one in a hundred disconnected.
        let epoch = chrono::Utc::now().timestamp();
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,found_at,maturity_state,disconnected_at) SELECT lpad(to_hex(g),64,'0'),g,repeat('0',64),repeat('1',64),repeat('2',64),s,to_timestamp($1-(5001-g)*600),CASE WHEN s='reversed' THEN 'reversed' ELSE 'immature' END,CASE WHEN s='reversed' THEN to_timestamp($1) END FROM generate_series(1,5000) g, LATERAL (SELECT CASE WHEN g%50=0 THEN 'reversed' WHEN g%100=1 THEN 'inactive' ELSE 'confirmed' END AS s) state")
            .bind(epoch as f64)
            .execute(&f.pool)
            .await?;
        sqlx::raw_sql("VACUUM (ANALYZE) qbit_pool_blocks")
            .execute(&f.pool)
            .await?;
        // Its own connection, never returned to the pool, so the settings
        // below reach no other query.
        let mut conn = f.pool.acquire().await?.detach();
        sqlx::raw_sql(&format!(
            "PREPARE blocks(bigint,bigint,text) AS {}",
            BLOCKS.trim_end().trim_end_matches(';')
        ))
        .execute(&mut conn)
        .await?;
        let mut problems = Vec::new();
        for cost in ["4", "1.1"] {
            for mode in ["force_custom_plan", "force_generic_plan"] {
                sqlx::raw_sql(&format!(
                    "SET random_page_cost={cost}; SET plan_cache_mode={mode}"
                ))
                .execute(&mut conn)
                .await?;
                for (limit, offset) in [(15i64, 0i64), (15, 300), (100, 0)] {
                    let plan: Value = sqlx::query_scalar(&format!(
                        "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE blocks({limit},{offset},'active')"
                    ))
                    .fetch_one(&mut conn)
                    .await?;
                    let mut all = Vec::new();
                    plan_nodes(&plan[0]["Plan"], &mut all);
                    let index = all.iter().find(|node| {
                        node["Index Name"] == "qbit_pool_blocks_public_recent_idx"
                    });
                    let case = format!("random_page_cost {cost}, {mode}, page {limit}+{offset}");
                    match index {
                        None => problems.push(format!("{case}: no read of the index: {plan}")),
                        Some(node) => {
                            let read = (node["Actual Rows"].as_f64().unwrap_or(0.0)
                                + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
                                * node["Actual Loops"].as_f64().unwrap_or(1.0);
                            // The page, and the disconnected blocks among it.
                            if read > (limit + offset) as f64 * 1.1 + 2.0 {
                                problems.push(format!("{case}: the index read {read} blocks"));
                            }
                        }
                    }
                }
            }
        }
        ensure!(problems.is_empty(), "{}", problems.join("\n"));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.close(result).await
}

/// #857: the pool summary's 24-hour sums read the 5-minute rollups and only
/// the raw shares of their partial buckets and the rollup lag, under
/// PostgreSQL's default and SSD page costs, where 3.1.1 read every share of
/// the last 24 hours; and with a watermark a day behind, or none, that
/// 24-hour range.
#[tokio::test]
async fn pool_hashrate_sums_read_only_the_partial_buckets_and_the_lag() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // 20,000 rolled-up shares over 25 hours, one every 4.5 s, from 50
        // miners; each window's partial buckets hold a few dozen.
        let epoch = chrono::Utc::now().timestamp();
        sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) SELECT 'sum-'||g,'miner-'||g%50,'miner-'||g%50,decode(repeat('1',64),'hex'),1000000,1000000,100,'job',to_timestamp($1-(20001-g)*4.5),0,to_timestamp($1-(20001-g)*4.5),'test',1 FROM generate_series(1,20000) g")
            .bind(epoch as f64)
            .execute(&f.pool)
            .await?;
        let progress = qbit_prism_server::rollups::advance(&f.pool, 100_000).await?;
        ensure!(progress.scanned == 20_000, "{progress:?}");
        sqlx::raw_sql("VACUUM (ANALYZE) qbit_share_ledger")
            .execute(&f.pool)
            .await?;
        let mut conn = f.pool.acquire().await?.detach();
        let mut walks = Vec::new();
        let mut raw_read = 0.0;
        for (phase, lag) in [("current watermark", 0.0), ("300 shares past the watermark", 300.0)] {
            if lag > 0.0 {
                sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) SELECT 'lag-'||g,'late-'||g%7,'late-'||g%7,decode(repeat('1',64),'hex'),1000000,1000000,100,'job',to_timestamp($1-g*240),0,to_timestamp($1-g*240),'test',1 FROM generate_series(1,300) g")
                    .bind(epoch as f64)
                    .execute(&f.pool)
                    .await?;
            }
            for cost in ["4", "1.1"] {
                sqlx::raw_sql(&format!("SET random_page_cost={cost}"))
                    .execute(&mut conn)
                    .await?;
                let plan: Value = sqlx::query_scalar(&format!(
                    "EXPLAIN (ANALYZE, FORMAT JSON) {POOL_HASHRATE_ROLLUPS}"
                ))
                .fetch_one(&mut conn)
                .await?;
                let read = ledger_tuples(&plan[0]["Plan"]).round();
                if read > 1_000.0 + lag {
                    walks.push(format!("{phase}, random_page_cost {cost}: read {read} ledger shares"));
                }
                let mut scans = Vec::new();
                one_sided_ledger_scans(&plan[0]["Plan"], false, &mut scans);
                walks.extend(
                    scans
                        .into_iter()
                        .map(|scan| format!("{phase}, random_page_cost {cost}: {scan}")),
                );
                let raw: Value = sqlx::query_scalar(&format!(
                    "EXPLAIN (ANALYZE, FORMAT JSON) {POOL_HASHRATE}"
                ))
                .fetch_one(&mut conn)
                .await?;
                raw_read = ledger_tuples(&raw[0]["Plan"]).round();
            }
        }
        ensure!(
            raw_read > 19_000.0,
            "the raw scan this replaces read only {raw_read} shares"
        );
        // With a watermark whose share is older than the 24 hours, or none
        // (rollups disabled, stalled or never run), the sums read the 24
        // hours' range, as the raw scan does, not every share past the
        // watermark.
        for (phase, reset) in [
            ("a watermark a day behind", "UPDATE qbit_hashrate_rollup_progress SET last_share_seq=1"),
            ("no rollup progress", "DELETE FROM qbit_hashrate_rollup_progress"),
        ] {
        sqlx::query(reset).execute(&f.pool).await?;
        for cost in ["4", "1.1"] {
            sqlx::raw_sql(&format!("SET random_page_cost={cost}"))
                .execute(&mut conn)
                .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, FORMAT JSON) {POOL_HASHRATE_ROLLUPS}"
            ))
            .fetch_one(&mut conn)
            .await?;
            let read = ledger_tuples(&plan[0]["Plan"]).round();
            if read > raw_read + 300.0 {
                walks.push(format!(
                    "{phase}, random_page_cost {cost}: read {read} ledger shares, the raw scan {raw_read}"
                ));
            }
            let mut scans = Vec::new();
            one_sided_ledger_scans(&plan[0]["Plan"], false, &mut scans);
            walks.extend(scans.into_iter().map(|scan| {
                format!("{phase}, random_page_cost {cost}: {scan}")
            }));
        }
        }
        ensure!(walks.is_empty(), "the pool sums walk the ledger:\n{}", walks.join("\n"));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.close(result).await
}

#[tokio::test]
async fn public_service_is_read_only_and_bounds_http_database_work() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result=async {
        let at=chrono::Utc::now().timestamp()-10;f.block(1,101,"confirmed",at).await?;
        let(app,service)=public_service::router(f.state(false),ServiceConfig::default());
        ensure!(get(&app,"/audit/latest").await.0==StatusCode::NOT_FOUND);
        ensure!(get(&app,"/owed").await.0==StatusCode::NOT_FOUND);
        ensure!(get(&app,"/healthz").await.0==StatusCode::SERVICE_UNAVAILABLE);
        ensure!(get(&app,"/public/v1/mining-configuration").await.0==StatusCode::OK);
        service.probe_once().await;
        let(status,_,health)=get(&app,"/healthz").await;ensure!(status==StatusCode::OK&&health["schema"]=="qbit.prism.public-read-health.v1");
        sqlx::query("ALTER TABLE qbit_pool_blocks RENAME COLUMN inactive_since TO absent_native_column").execute(&f.pool).await?;
        service.probe_once().await;
        let(status,_,health)=get(&app,"/healthz").await;
        ensure!(status==StatusCode::SERVICE_UNAVAILABLE&&health["error"]=="native public read schema is incomplete","{health}");
        let(status,_,error)=get(&app,"/public/v1/blocks").await;
        ensure!(status==StatusCode::SERVICE_UNAVAILABLE&&error["error"]["code"]=="upstream_unavailable","schema mismatch should refuse before executing the read model: {error}");
        let(schema_replica_app,schema_replica)=public_service::router(f.state(false),ServiceConfig{replica_required:true,..Default::default()});
        schema_replica.probe_once().await;
        let(status,_,health)=get(&schema_replica_app,"/healthz").await;
        ensure!(status==StatusCode::SERVICE_UNAVAILABLE&&health["replica"]["schema_ready"]==false,"required replica mode must also validate the native schema: {health}");
        sqlx::query("ALTER TABLE qbit_pool_blocks RENAME COLUMN absent_native_column TO inactive_since").execute(&f.pool).await?;
        service.probe_once().await;
        ensure!(get(&app,"/healthz").await.0==StatusCode::OK,"schema migration should restore readiness");
        let(status,headers,_)=get(&app,"/public/v1/blocks").await;ensure!(status==StatusCode::OK&&headers["x-prism-staleness-budget-seconds"]=="15");
        let mut lock=f.pool.begin().await?;sqlx::query("LOCK TABLE qbit_pool_blocks IN ACCESS EXCLUSIVE MODE").execute(&mut*lock).await?;
        let started=Instant::now();let(status,headers,error)=get(&app,"/public/v1/blocks").await;
        ensure!(status==StatusCode::SERVICE_UNAVAILABLE&&error["error"]["code"]=="read_timeout","{status}: {error}");
        ensure!(started.elapsed()<Duration::from_secs(2)&&headers["cache-control"]=="no-store");
        tokio::time::sleep(Duration::from_millis(75)).await;
        let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='prism-public-read' AND state='active' AND wait_event_type='Lock' AND query LIKE '%qbit_pool_blocks%'").fetch_one(&f.database.admin).await?;
        ensure!(waiting==0,"timed-out public SQL remained active on PostgreSQL");lock.rollback().await?;
        ensure!(get(&app,"/public/v1/blocks").await.0==StatusCode::OK);
        let(replica_app,replica)=public_service::router(f.state(false),ServiceConfig{replica_required:true,..Default::default()});
        replica.probe_once().await;
        ensure!(get(&replica_app,"/healthz").await.0==StatusCode::SERVICE_UNAVAILABLE);
        let(status,headers,error)=get(&replica_app,"/public/v1/blocks").await;ensure!(status==StatusCode::SERVICE_UNAVAILABLE&&error["error"]["code"]=="upstream_unavailable"&&headers["cache-control"]=="no-store");
        ensure!(get(&replica_app,"/public/v1/mining-configuration").await.0==StatusCode::OK);
        ensure!(get(&replica_app,"/public/v1/miners/alice/unknown").await.0==StatusCode::NOT_FOUND);
        let(status,_,metrics)=raw_get(&app,"/metrics").await;
        let metrics=String::from_utf8(metrics)?;
        ensure!(status==StatusCode::OK&&metrics.contains("qbit_prism_public_requests_total")&&metrics.contains("qbit_prism_public_responses_total{status=\"503\"}")&&metrics.contains("qbit_prism_public_cache_total"));
        Ok::<_,anyhow::Error>(())
    }.await;
    f.close(result).await
}

#[tokio::test]
async fn content_addressed_bytes_and_legacy_fallback_headers_are_exact() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result=async {
        let block=f.block(1,101,"confirmed",chrono::Utc::now().timestamp()-10).await?;
        let canonical=b"{\"z\":3,\"a\":1,\"schema\":\"historical\"}";
        let hash=hex::encode(Sha256::digest(canonical));
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,canonical_audit_bytes,coinbase_tx_hex) VALUES($1,$2,$3,$4,'00')")
            .bind(&block).bind(json!({"z":3,"a":1,"schema":"historical"})).bind(&hash).bind(canonical.as_slice()).execute(&f.pool).await?;
        let app=api::router(f.state(false));let path=format!("/public/v1/artifacts/{hash}");
        let(status,headers,body)=raw_get(&app,&path).await;ensure!(status==StatusCode::OK&&body==canonical&&hex::encode(Sha256::digest(&body))==hash);
        ensure!(headers["etag"]==format!("\"{hash}\""));
        sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=NULL WHERE block_hash=$1").bind(&block).execute(&f.pool).await?;
        let(status,headers,body)=raw_get(&app,&path).await;ensure!(status==StatusCode::OK&&headers["cache-control"]=="no-store"&&headers["x-prism-artifact-canonical-state"]=="missing");
        ensure!(serde_json::from_slice::<Value>(&body)?==json!({"z":3,"a":1,"schema":"historical"}));
        sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes='{}'::bytea WHERE block_hash=$1").bind(&block).execute(&f.pool).await?;
        ensure!(get(&app,&path).await.0==StatusCode::INTERNAL_SERVER_ERROR);
        Ok::<_,anyhow::Error>(())
    }.await;
    f.close(result).await
}

/// The read model's events on the test's thread, as JSON lines.
#[derive(Clone, Default)]
struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl LogCapture {
    /// The events logged since the last drain, as `(level, fields)`.
    fn drain(&self) -> Result<Vec<(String, Value)>> {
        let bytes = std::mem::take(&mut *self.0.lock().unwrap());
        String::from_utf8(bytes)?
            .lines()
            .map(|line| {
                let event: Value = serde_json::from_str(line)?;
                Ok((
                    event["level"].as_str().unwrap_or_default().to_owned(),
                    event["fields"].clone(),
                ))
            })
            .collect()
    }
}

/// #735: a legacy artifact whose body is not at its recorded location yet
/// answers a 404 that no cache keeps and no 5xx count sees; a body that is
/// there but cannot be read or proven still answers 500, logged with its
/// block, artifact and location. The block's audit view keeps answering 404
/// with its own messages, and the settlement-artifacts link check, which
/// serves no artifact, logs nothing above debug.
#[tokio::test]
async fn legacy_artifact_without_its_body_answers_uncached_404_and_faults_stay_500() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    // `#[tokio::test]` polls every task on this thread, so a thread default
    // sees every event the requests below log, and no other test's.
    let logs = LogCapture::default();
    let writer = logs.clone();
    let _logging = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_ansi(false)
            .with_env_filter("qbit_prism_server::api::read_models=debug")
            .with_writer(move || writer.clone())
            .finish(),
    );
    let result=async {
        let at=chrono::Utc::now().timestamp()-10;
        let(app,service)=public_service::router(f.state(true),ServiceConfig::default());
        service.probe_once().await;
        // Uncached, so every settlement-artifacts request runs its link check.
        let audit=api::router(f.state(false));
        let immutable=|step:&str,status:StatusCode,headers:&HeaderMap,hash:&str|->Result<()>{
            ensure!(status==StatusCode::OK&&headers["etag"]==format!("\"{hash}\""),"{step}: {status}: {headers:?}");
            ensure!(headers["cache-control"]=="public, max-age=0, must-revalidate","{step}: {headers:?}");
            for header in ["cdn-cache-control","vercel-cdn-cache-control"] {
                ensure!(headers[header]=="public, max-age=86400, stale-while-revalidate=86400, immutable","{step}: {header}: {headers:?}");
            }
            ensure!(headers["x-prism-staleness-budget-seconds"]=="unbounded"&&!headers.contains_key("x-prism-artifact-canonical-state"),"{step}: {headers:?}");
            Ok(())
        };
        let absent=|step:&str,(status,headers,body):(StatusCode,HeaderMap,Value)|->Result<()>{
            ensure!(status==StatusCode::NOT_FOUND&&body==json!({"schema":"prism.dashboard.error.v1","error":{"code":"not_found","message":"audit bundle body is not retrievable","request_id":null}}),"{step}: {status}: {body}");
            ensure!(headers["cache-control"]=="no-store","{step}: {headers:?}");
            for header in ["cdn-cache-control","vercel-cdn-cache-control","etag"] {
                ensure!(!headers.contains_key(header),"{step}: a 404 carries {header}: {headers:?}");
            }
            ensure!(!headers.values().any(|value|value.to_str().is_ok_and(|value|value.contains("immutable"))),"{step}: {headers:?}");
            Ok(())
        };
        let fault=|step:&str,(status,headers,body):(StatusCode,HeaderMap,Value)|->Result<()>{
            ensure!(status==StatusCode::INTERNAL_SERVER_ERROR&&body["error"]["code"]=="internal_error","{step}: {status}: {body}");
            ensure!(headers["cache-control"]=="no-store"&&!headers.contains_key("cdn-cache-control"),"{step}: {headers:?}");
            Ok(())
        };
        // A stored artifact keeps today's immutable answer.
        let stored=f.block(1,101,"confirmed",at).await?;
        let canonical=b"{\"z\":3,\"a\":1,\"schema\":\"historical\"}";
        let stored_hash=hex::encode(Sha256::digest(canonical));
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,canonical_audit_bytes,coinbase_tx_hex) VALUES($1,$2,$3,$4,'00')")
            .bind(&stored).bind(json!({"z":3,"a":1,"schema":"historical"})).bind(&stored_hash).bind(canonical.as_slice()).execute(&f.pool).await?;
        let(status,headers,body)=raw_get(&app,&format!("/public/v1/artifacts/{stored_hash}")).await;
        ensure!(body==canonical,"stored artifact: body");immutable("stored artifact",status,&headers,&stored_hash)?;
        // Corrupt stored canonical bytes are a server fault.
        let corrupt=f.block(3,103,"confirmed",at).await?;
        let corrupt_hash=hex::encode(Sha256::digest(b"never stored"));
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,canonical_audit_bytes,coinbase_tx_hex) VALUES($1,'{}',$2,'{}'::bytea,'00')")
            .bind(&corrupt).bind(&corrupt_hash).execute(&f.pool).await?;
        fault("corrupt stored canonical bytes",get(&app,&format!("/public/v1/artifacts/{corrupt_hash}")).await)?;
        ensure!(logs.drain()?.is_empty(),"stored rows: the legacy body reader logged");
        // A legacy row that `import-audits` has not stored, whose body is not
        // on this host yet. Its block settles through a CTV fanout, so its
        // settlement artifacts check the body before linking it.
        let legacy=f.block(2,102,"confirmed",at).await?;
        let body=b"{\"schema\":\"historical\",\"legacy\":true}";
        let hash=hex::encode(Sha256::digest(body));
        let dir=tempfile::tempdir()?;
        let file=dir.path().join("legacy-audit.json");
        let uri=format!("file://{}",file.display());
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle_sha256,body_uri,coinbase_tx_hex) VALUES($1,$2,$3,'00')")
            .bind(&legacy).bind(&hash).bind(&uri).execute(&f.pool).await?;
        let set_hash=hex::encode(Sha256::digest(b"{\"z\":2,\"a\":1}"));
        let manifest_hash=hex::encode(Sha256::digest(b"{\"z\":1,\"a\":2}"));
        sqlx::query("INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats) VALUES($1,'{\"z\":2,\"a\":1}','{\"z\":2,\"a\":1}',$2,'ctv_fanout',$3,'00',1,100,100)")
            .bind(&legacy).bind(&set_hash).bind("5".repeat(64)).execute(&f.pool).await?;
        sqlx::query("INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,anchor_vout,covenant_output_value_sats,fanout_output_sum_sats,settlement_status) VALUES($1,$2,$3,'{\"z\":1,\"a\":2}','{\"z\":1,\"a\":2}',$4,$4,$4,$4,0,1,$5,0,'00','00',1,100,100,'broadcastable')")
            .bind("8".repeat(64)).bind(&legacy).bind(&set_hash).bind(&manifest_hash).bind("5".repeat(64)).execute(&f.pool).await?;
        let path=format!("/public/v1/artifacts/{hash}");
        let view=format!("/audit/blocks/{legacy}/bundle");
        let settlement=format!("/public/v1/blocks/{legacy}/settlement-artifacts");
        let audit_view=|step:&'static str,message:&'static str|{let audit=audit.clone();let view=view.clone();async move{
            let(status,_,body)=get(&audit,&view).await;
            ensure!(status==StatusCode::NOT_FOUND&&body["error"]==message,"{step}: audit view: {status}: {body}");
            Ok::<_,anyhow::Error>(())
        }};
        let unlinked=|step:&'static str|{let audit=audit.clone();let settlement=settlement.clone();async move{
            let(status,_,body)=get(&audit,&settlement).await;
            ensure!(status==StatusCode::OK&&body["artifact_links"].as_array().is_some_and(|links|links.iter().all(|link|link["kind"]!="audit_bundle")),"{step}: settlement artifacts: {status}: {body}");
            Ok::<_,anyhow::Error>(())
        }};
        // Every event for the legacy row names its block, artifact and body.
        let logged=|step:&str,expected:&[(&str,&str,Option<&str>)]|->Result<()>{
            let events=logs.drain()?;
            let seen:Vec<_>=events.iter().map(|(level,fields)|(level.clone(),fields["message"].as_str().unwrap_or_default().to_owned(),fields["reason"].as_str().map(str::to_owned))).collect();
            let wanted:Vec<_>=expected.iter().map(|(level,message,reason)|(level.to_string(),message.to_string(),reason.map(str::to_owned))).collect();
            ensure!(seen==wanted,"{step}: logged {events:?}");
            for(_,fields) in &events {
                ensure!(fields["block_hash"]==legacy.as_str()&&fields["artifact_sha256"]==hash.as_str()&&fields["body_uri"]==uri.as_str(),"{step}: {fields}");
            }
            Ok(())
        };
        const ABSENT:&str="legacy audit body is not at its recorded location";
        const FAULT:&str="legacy audit body cannot be served";
        // Asked twice: the first 404 is not kept by the origin cache. The
        // audit view logs nothing, as before.
        absent("absent body",get(&app,&path).await)?;absent("absent body, asked again",get(&app,&path).await)?;
        audit_view("absent body","audit bundle body is not retrievable").await?;
        logged("absent body",&[("DEBUG",ABSENT,None),("DEBUG",ABSENT,None)])?;
        unlinked("absent body").await?;
        logged("absent body, link check",&[("DEBUG",ABSENT,None)])?;
        // A body-ref envelope whose share segment is not there is absent too.
        std::fs::write(&file,serde_json::to_vec(&json!({"schema":"qbit.prism.audit-body-ref.v1","audit_bundle_sha256":hash,"share_count":1,"bundle_without_shares":{},"share_parts":[{"kind":"segment","first_share_seq":1,"last_share_seq":1,"share_count":1,"sha256":"0".repeat(64),"body_uri":"segment.json"}]}))?)?;
        absent("absent segment",get(&app,&path).await)?;
        audit_view("absent segment","audit bundle body is not retrievable").await?;
        logged("absent segment",&[("DEBUG",ABSENT,None)])?;
        // A segment in a store this server does not read is a fault, not an
        // absence: no wait makes it readable here.
        std::fs::write(&file,serde_json::to_vec(&json!({"schema":"qbit.prism.audit-body-ref.v1","audit_bundle_sha256":hash,"share_count":1,"bundle_without_shares":{},"share_parts":[{"kind":"segment","first_share_seq":1,"last_share_seq":1,"share_count":1,"sha256":"0".repeat(64),"body_uri":"s3://audits/segment.json"}]}))?)?;
        fault("segment in another store",get(&app,&path).await)?;
        audit_view("segment in another store","audit bundle body is not retrievable").await?;
        logged("segment in another store",&[("WARN",FAULT,Some("audit bundle body is not retrievable"))])?;
        std::fs::write(&file,serde_json::to_vec(&json!({"schema":"qbit.prism.audit-body-ref.v1","audit_bundle_sha256":hash,"share_count":1,"bundle_without_shares":{},"share_parts":[{"kind":"segment","first_share_seq":1,"last_share_seq":1,"share_count":1,"sha256":"0".repeat(64),"body_uri":"segment.json"}]}))?)?;
        // A segment that is there but does not prove is a fault.
        std::fs::write(dir.path().join("segment.json"),b"{}")?;
        fault("unproven segment",get(&app,&path).await)?;
        audit_view("unproven segment","audit bundle body is not retrievable").await?;
        logged("unproven segment",&[("WARN",FAULT,Some("audit bundle body is not retrievable"))])?;
        // So is an envelope that proves itself but is another audit's body.
        let other=qbit_prism::build_audit_bundle(
            vec![qbit_prism::AcceptedShare{share_seq:1,share_id:"share".into(),miner_id:"miner".into(),order_key:"miner".into(),p2mr_program_hex:"11".repeat(32),share_difficulty:1,network_difficulty:100,template_height:100,job_id:"job".into(),job_issued_at_ms:0,accepted_at_ms:0,ntime:1,credit_policy:None}],
            qbit_prism::FoundBlock{block_height:102,coinbase_value_sats:500_000_000,network_difficulty:100,anchor_job_issued_at_ms:1},
            vec![],qbit_prism::PayoutPolicy::day_one_default(),
            &qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"93".repeat(32))?,
            &qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"94".repeat(32))?,
        )?;
        let own=hex::encode(Sha256::digest(qbit_prism::canonical_audit_bundle_bytes(&other)?));
        let mut without=serde_json::to_value(&other)?;
        let shares=without.as_object_mut().and_then(|object|object.remove("shares")).ok_or_else(||anyhow::anyhow!("an audit bundle has shares"))?;
        ensure!(own!=hash);
        std::fs::write(&file,serde_json::to_vec(&json!({"schema":"qbit.prism.audit-body-ref.v1","audit_bundle_sha256":own,"share_count":1,"bundle_without_shares":without,"share_parts":[{"kind":"inline","first_share_seq":1,"last_share_seq":1,"share_count":1,"shares":shares}]}))?)?;
        fault("another audit's envelope",get(&app,&path).await)?;
        audit_view("another audit's envelope","audit bundle body hash mismatch").await?;
        logged("another audit's envelope",&[("WARN",FAULT,Some("audit bundle body hash mismatch"))])?;
        // A share segment recorded as a `file://` URI is read from its path.
        let segmented=f.block(4,104,"confirmed",at).await?;
        let segment=serde_json::to_vec(&json!({"schema":"qbit.prism.audit-share-segment.v1","first_share_seq":1,"last_share_seq":1,"share_count":1,"shares":shares}))?;
        let segment_path=dir.path().join("file-uri-segment.json");
        std::fs::write(&segment_path,&segment)?;
        let envelope=dir.path().join("segmented-audit.json");
        std::fs::write(&envelope,serde_json::to_vec(&json!({"schema":"qbit.prism.audit-body-ref.v1","audit_bundle_sha256":own,"share_count":1,"bundle_without_shares":without,"share_parts":[{"kind":"segment","first_share_seq":1,"last_share_seq":1,"share_count":1,"sha256":hex::encode(Sha256::digest(&segment)),"body_uri":format!("file://{}",segment_path.display())}]}))?)?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle_sha256,body_uri,coinbase_tx_hex) VALUES($1,$2,$3,'00')")
            .bind(&segmented).bind(&own).bind(format!("file://{}",envelope.display())).execute(&f.pool).await?;
        let(status,headers,served)=raw_get(&app,&format!("/public/v1/artifacts/{own}")).await;
        ensure!(status==StatusCode::OK&&serde_json::from_slice::<Value>(&served)?==serde_json::to_value(&other)?&&headers["x-prism-artifact-canonical-state"]=="missing","file:// segment: {status}: {headers:?}");
        ensure!(logs.drain()?.is_empty(),"file:// segment: the legacy body reader logged");
        // So are a body that is not JSON, one that is not the advertised
        // body, and one that cannot be read. The link check serves none of
        // these 500s, so it logs them at debug only.
        std::fs::write(&file,b"not json")?;
        fault("not JSON",get(&app,&path).await)?;
        audit_view("not JSON","audit bundle body is not valid JSON").await?;
        logged("not JSON",&[("WARN",FAULT,Some("audit bundle body is not valid JSON"))])?;
        unlinked("not JSON").await?;
        logged("not JSON, link check",&[("DEBUG",FAULT,Some("audit bundle body is not valid JSON"))])?;
        std::fs::write(&file,b"{\"schema\":\"historical\",\"legacy\":false}")?;
        fault("another body",get(&app,&path).await)?;
        audit_view("another body","audit bundle body hash mismatch").await?;
        logged("another body",&[("WARN",FAULT,Some("audit bundle body hash mismatch"))])?;
        std::fs::remove_file(&file)?;std::fs::create_dir(&file)?;
        fault("unreadable body",get(&app,&path).await)?;
        audit_view("unreadable body","audit bundle body is not retrievable").await?;
        logged("unreadable body",&[("WARN",FAULT,Some("audit bundle body is not retrievable"))])?;
        std::fs::remove_dir(&file)?;
        // So is a body recorded under a scheme this server does not read: no
        // wait makes it retrievable here.
        let elsewhere="s3://audits/legacy-audit.json";
        sqlx::query("UPDATE qbit_pool_audit_bundles SET body_uri=$2 WHERE block_hash=$1").bind(&legacy).bind(elsewhere).execute(&f.pool).await?;
        fault("another scheme",get(&app,&path).await)?;
        audit_view("another scheme","audit bundle body is not retrievable").await?;
        let events=logs.drain()?;
        ensure!(events.len()==1&&events[0].0=="WARN"&&events[0].1["message"]==FAULT&&events[0].1["error"]=="unsupported body URI scheme"&&events[0].1["body_uri"]==elsewhere,"another scheme: logged {events:?}");
        sqlx::query("UPDATE qbit_pool_audit_bundles SET body_uri=$2 WHERE block_hash=$1").bind(&legacy).bind(&uri).execute(&f.pool).await?;
        // Once the body arrives, the same address serves it at once.
        std::fs::write(&file,body)?;
        let(status,headers,served)=raw_get(&app,&path).await;
        ensure!(status==StatusCode::OK&&serde_json::from_slice::<Value>(&served)?==serde_json::from_slice::<Value>(body)?&&headers["cache-control"]=="no-store"&&headers["x-prism-artifact-canonical-state"]=="missing","body arrived: {status}: {headers:?}");
        // And once `import-audits` stores it, as immutably as any artifact.
        sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2 WHERE block_hash=$1").bind(&legacy).bind(body.as_slice()).execute(&f.pool).await?;
        let(status,headers,served)=raw_get(&app,&path).await;
        ensure!(served==body,"body imported: body");immutable("body imported",status,&headers,&hash)?;
        logged("body served",&[])?;
        // Only the faults count toward the 5xx alert.
        let(_,_,metrics)=raw_get(&app,"/metrics").await;
        let metrics=String::from_utf8(metrics)?;
        ensure!(metrics.contains("qbit_prism_public_responses_total{status=\"404\"} 3\n")&&metrics.contains("qbit_prism_public_responses_total{status=\"500\"} 8\n"),"metrics: {metrics}");
        Ok::<_,anyhow::Error>(())
    }.await;
    f.close(result).await
}

/// #735: `/public/v1/artifacts/<sha>` finds an audit row by its address and
/// then reads it by block: its stored canonical bytes, or its legacy body.
/// A row re-imported between the two reads answers another address, so
/// neither its bytes nor its body is served under the old one, which answers
/// the unknown-artifact 404 instead of another audit or a digest-mismatch 500.
#[tokio::test]
async fn an_audit_reimported_between_lookup_and_read_is_not_served_under_its_old_address(
) -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result=async {
        let at=chrono::Utc::now().timestamp()-10;
        let dir=tempfile::tempdir()?;
        // Uncached, with a read deadline the interleavings cannot reach.
        let state=ApiState::new(f.pool.clone(),ApiConfig{rpc_url:f.rpc.clone(),cache_enabled:false,read_timeout:Duration::from_secs(30),..Default::default()},Arc::new(qbit_prism_server::metrics::Metrics::default()));
        let app=api::router(state.clone());
        let request=|address:&str|{let app=app.clone();let path=format!("/public/v1/artifacts/{address}");tokio::spawn(async move{get(&app,&path).await})};
        // Waits until a backend other than the lock holders waits on a lock.
        let blocked=|step:&'static str,holders:Vec<i32>|{let pool=f.pool.clone();async move{
            tokio::time::timeout(Duration::from_secs(10),async{
                loop {
                    let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>ALL($1)").bind(&holders).fetch_one(&pool).await?;
                    if waiting>0 {return Ok::<_,anyhow::Error>(());}
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.map_err(|_|anyhow::anyhow!("{step}: no read waited on the held lock"))?
        }};
        let unknown=|step:&str,(status,headers,error):(StatusCode,HeaderMap,Value)|->Result<()>{
            ensure!(status==StatusCode::NOT_FOUND&&error["error"]["code"]=="not_found"&&error["error"]["message"]=="unknown public PRISM artifact"&&headers["cache-control"]=="no-store"&&!headers.contains_key("cdn-cache-control"),"{step}: {status}: {headers:?}: {error}");
            Ok(())
        };
        let insert="INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,canonical_audit_bytes,body_uri,coinbase_tx_hex) VALUES($1,$2,$3,$4,$5,'00')";

        // A legacy body: the lookup reads only the audit table and the body
        // read joins the blocks table, so holding that table stops the request
        // between them.
        let block=f.block(1,101,"confirmed",at).await?;
        let old=hex::encode(Sha256::digest(b"the body first advertised"));
        let body=b"{\"schema\":\"historical\",\"reimported\":true}";
        let new=hex::encode(Sha256::digest(body));
        let file=dir.path().join("legacy-audit.json");
        std::fs::write(&file,body)?;
        sqlx::query(insert).bind(&block).bind(None::<Value>).bind(&old).bind(None::<Vec<u8>>).bind(format!("file://{}",file.display())).execute(&f.pool).await?;
        let mut lock=f.pool.begin().await?;
        let holder:i32=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut*lock).await?;
        sqlx::query("LOCK TABLE qbit_pool_blocks IN ACCESS EXCLUSIVE MODE").execute(&mut*lock).await?;
        let reply=request(&old);
        blocked("legacy body",vec![holder]).await?;
        sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle_sha256=$2 WHERE block_hash=$1").bind(&block).bind(&new).execute(&mut*lock).await?;
        lock.commit().await?;
        unknown("legacy body re-imported",reply.await?)?;
        let(status,_,served)=get(&app,&format!("/public/v1/artifacts/{new}")).await;
        ensure!(status==StatusCode::OK&&served==serde_json::from_slice::<Value>(body)?,"legacy body, new address: {status}: {served}");

        // Stored canonical bytes: the request waits for a rebuild permit
        // after the lookup, once it holds its admission, and the bytes are
        // read by block after it. Served under the old address, the new bytes
        // would be an immutable response every cache keeps.
        let block=f.block(2,102,"confirmed",at).await?;
        let first=b"{\"schema\":\"historical\",\"first\":true}";
        let reimported=b"{\"schema\":\"historical\",\"first\":false}";
        let (old,new)=(hex::encode(Sha256::digest(first)),hex::encode(Sha256::digest(reimported)));
        sqlx::query(insert).bind(&block).bind(json!({})).bind(&old).bind(first.as_slice()).bind(None::<String>).execute(&f.pool).await?;
        let rebuilds=state.audit_rebuild_limit();
        let held=rebuilds.clone().acquire_many_owned(rebuilds.available_permits() as u32).await?;
        let admission=state.audit_artifact_admission();
        let free=admission.available_permits();
        let reply=request(&old);
        tokio::time::timeout(Duration::from_secs(10),async{while admission.available_permits()==free {tokio::time::sleep(Duration::from_millis(5)).await;}})
            .await.map_err(|_|anyhow::anyhow!("stored bytes: the request was never admitted"))?;
        sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle_sha256=$2,canonical_audit_bytes=$3 WHERE block_hash=$1").bind(&block).bind(&new).bind(reimported.as_slice()).execute(&f.pool).await?;
        drop(held);
        unknown("stored bytes re-imported",reply.await?)?;
        let(status,_,served)=raw_get(&app,&format!("/public/v1/artifacts/{new}")).await;
        ensure!(status==StatusCode::OK&&served==reimported,"stored bytes, new address: {status}");

        // Bytes stored between the canonical read and the bundle read, then a
        // re-import before the bundle read fetches them: the first read joins
        // the blocks table, the second reads only the audit table, which a
        // second session takes as soon as the first read lets it go.
        let block=f.block(3,103,"confirmed",at).await?;
        let first=b"{\"schema\":\"historical\",\"third\":true}";
        let reimported=b"{\"schema\":\"historical\",\"third\":false}";
        let (old,new)=(hex::encode(Sha256::digest(first)),hex::encode(Sha256::digest(reimported)));
        sqlx::query(insert).bind(&block).bind(None::<Value>).bind(&old).bind(None::<Vec<u8>>).bind(format!("file://{}",dir.path().join("not-stored-yet.json").display())).execute(&f.pool).await?;
        let mut blocks=f.pool.begin().await?;
        let blocks_holder:i32=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut*blocks).await?;
        sqlx::query("LOCK TABLE qbit_pool_blocks IN ACCESS EXCLUSIVE MODE").execute(&mut*blocks).await?;
        let reply=request(&old);
        blocked("bundle read",vec![blocks_holder]).await?;
        sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2 WHERE block_hash=$1").bind(&block).bind(first.as_slice()).execute(&mut*blocks).await?;
        let (pid,audit_holder)=tokio::sync::oneshot::channel();
        let audits=tokio::spawn({let pool=f.pool.clone();async move{
            let mut audits=pool.begin().await?;
            let _=pid.send(sqlx::query_scalar::<_,i32>("SELECT pg_backend_pid()").fetch_one(&mut*audits).await?);
            sqlx::query("LOCK TABLE qbit_pool_audit_bundles IN ACCESS EXCLUSIVE MODE").execute(&mut*audits).await?;
            Ok::<_,anyhow::Error>(audits)
        }});
        let audit_holder=audit_holder.await?;
        tokio::time::timeout(Duration::from_secs(10),async{
            loop {
                let queued:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')").bind(audit_holder).fetch_one(&f.pool).await?;
                if queued {return Ok::<_,anyhow::Error>(());}
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.map_err(|_|anyhow::anyhow!("bundle read: the audit table lock never queued"))??;
        blocks.commit().await?;
        let mut audits=audits.await??;
        blocked("stored bytes read",vec![audit_holder]).await?;
        sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle_sha256=$2,canonical_audit_bytes=$3 WHERE block_hash=$1").bind(&block).bind(&new).bind(reimported.as_slice()).execute(&mut*audits).await?;
        audits.commit().await?;
        unknown("re-imported between the bundle reads",reply.await?)?;
        Ok::<_,anyhow::Error>(())
    }.await;
    f.close(result).await
}

/// Holding a fixture's migration key must not block another fixture's setup.
#[tokio::test]
async fn fixture_migrations_do_not_share_advisory_locks() -> Result<()> {
    // The ledger's private migration key, as in ledger_postgres's isolation test.
    const MIGRATION_LOCK: i64 = 0x505249534d000001;
    let Some(first) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let mut held = first.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(MIGRATION_LOCK)
            .execute(&mut *held)
            .await?;
        let second = Fixture::open()
            .await?
            .expect("integration gate already passed");
        second.close(Ok(())).await?;
        held.rollback().await?;
        Ok(())
    }
    .await;
    first.close(result).await
}

/// A node that counts the methods it is asked and holds `getblockchaininfo`
/// until it is opened (#858).
struct CountingNode {
    calls: std::sync::Mutex<std::collections::BTreeMap<String, usize>>,
    open: tokio::sync::watch::Sender<bool>,
}
impl CountingNode {
    fn calls(&self, method: &str) -> usize {
        self.calls.lock().unwrap().get(method).copied().unwrap_or(0)
    }
}
async fn counting_node_reply(
    State(node): State<Arc<CountingNode>>,
    Json(input): Json<Value>,
) -> Json<Value> {
    let method = input["method"].as_str().unwrap_or("").to_owned();
    *node
        .calls
        .lock()
        .unwrap()
        .entry(method.clone())
        .or_default() += 1;
    let result = match method.as_str() {
        "getblockchaininfo" => {
            let _ = node.open.subscribe().wait_for(|open| *open).await;
            json!({"chain":"regtest","blocks":100,"bestblockhash":"ab".repeat(32),"initialblockdownload":false})
        }
        "getblocktemplate" => json!({"bits":"207fffff","coinbasevalue":5000000000u64}),
        "getnetworkinfo" => json!({"connections":2}),
        "getnetworkhashps" => json!("1234567890123.125"),
        _ => Value::Null,
    };
    Json(json!({"id":input["id"],"result":result,"error":null}))
}

/// #858: cache misses of the pool summary, the reward leaderboard and miner
/// pages that arrive together wait for one set of node calls, and serve the
/// network data an uncached request computes on the same node.
#[tokio::test]
async fn concurrent_pool_leaderboard_and_miner_misses_ask_the_node_for_one_template() -> Result<()>
{
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let at = chrono::Utc::now().timestamp() - 60;
        for (n, miner) in ["alice", "bob", "alice"].into_iter().enumerate() {
            f.share(&format!("share-{n}"), miner, at + n as i64, 1000).await?;
        }
        let node = Arc::new(CountingNode {
            calls: Default::default(),
            open: tokio::sync::watch::channel(false).0,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc_url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(counting_node_reply))
            .with_state(node.clone());
        let _server = AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }));
        // Every route's lifetime at 60 s, so the remembered answer lives its
        // longest, 15 s (the leaderboard's staleness budget), however slowly
        // the routes' own reads run.
        let lifetime = api::CacheLifetime {
            ttl: 60,
            stale_while_revalidate: 0,
        };
        let state = |cache_enabled| {
            ApiState::new(
                f.pool.clone(),
                ApiConfig {
                    rpc_url: rpc_url.clone(),
                    cache_enabled,
                    cache_debug_headers: true,
                    cache_lifetimes: api::CacheLifetimes {
                        default: lifetime,
                        aggregate: lifetime,
                        ..Default::default()
                    },
                    read_timeout: Duration::from_secs(30),
                    ..Default::default()
                },
                Arc::new(qbit_prism_server::metrics::Metrics::default()),
            )
        };
        let app = api::router(state(true));
        let mut paths = vec![
            "/public/v1/pool-summary".to_owned(),
            "/public/v1/leaderboard?window=reward".to_owned(),
            "/public/v1/leaderboard?window=reward&recipient_id=alice".to_owned(),
        ];
        paths.extend(["alice", "bob", "carol", "dave"].map(|m| format!("/public/v1/miners/{m}")));
        let requests: Vec<_> = paths
            .iter()
            .cloned()
            .map(|path| {
                let app = app.clone();
                tokio::spawn(async move { get(&app, &path).await })
            })
            .collect();
        tokio::time::timeout(Duration::from_secs(10), async {
            while node.calls("getblockchaininfo") == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        ensure!(
            requests.iter().all(|request| !request.is_finished()),
            "every miss waits for the node calls in flight"
        );
        ensure!(node.calls("getblockchaininfo") == 1);
        node.open.send_replace(true);
        let mut bodies = Vec::new();
        for (path, request) in paths.iter().zip(requests) {
            let (status, _, body) = request.await?;
            ensure!(status == StatusCode::OK, "{path}: {body}");
            bodies.push(body);
        }
        for method in ["getblockchaininfo", "getblocktemplate", "getnetworkinfo"] {
            ensure!(node.calls(method) == 1, "{method}: {}", node.calls(method));
        }
        // A later miss reads the remembered answer.
        let (status, headers, body) = get(&app, "/public/v1/miners/erin").await;
        ensure!(
            status == StatusCode::OK && headers["x-prism-public-cache"] == "MISS",
            "{headers:?}: {body}"
        );
        ensure!(node.calls("getblocktemplate") == 1);
        // The remembered answer is the one the node gives an uncached request.
        let (status, _, uncached) = get(&api::router(state(false)), "/public/v1/pool-summary").await;
        ensure!(status == StatusCode::OK, "{uncached}");
        ensure!(node.calls("getblocktemplate") == 2);
        ensure!(bodies[0]["network"] == uncached["network"], "{} != {}", bodies[0]["network"], uncached["network"]);
        ensure!(
            uncached["network"]
                == json!({"name":"regtest","height":100,"tip_hash":"ab".repeat(32),"bits":"207fffff","network_difficulty":"1000000","initial_block_download":false,"peers":2,"hashrate_ths":"1.234567890123125"}),
            "{}",
            uncached["network"]
        );
        ensure!(bodies[1]["window"]["network_difficulty"] == "1000000", "{}", bodies[1]);
        ensure!(bodies[3]["recipient_id"] == "alice" && bodies[3]["shares"]["accepted_3h"] == 2, "{}", bodies[3]);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.close(result).await
}

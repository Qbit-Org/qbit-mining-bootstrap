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
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tower::ServiceExt;

struct Fixture {
    admin: PgPool,
    pool: PgPool,
    schema: String,
    rpc: String,
    node: tokio::task::JoinHandle<()>,
    network: Arc<Mutex<Value>>,
}
impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("api2_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger = Ledger::connect(url.as_str(), "api2-test".into(), 4, true).await?;
        let network = Arc::new(Mutex::new(json!("1234567890123.125")));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(network.clone());
        let node = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Ok(Some(Self {
            admin,
            pool: ledger.pool.clone(),
            schema,
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
        )
    }
    async fn close(self) -> Result<()> {
        self.node.abort();
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
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
    f.close().await?;
    result
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
    f.close().await?;
    result
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
        let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='prism-public-read' AND state='active' AND wait_event_type='Lock' AND query LIKE '%qbit_pool_blocks%'").fetch_one(&f.admin).await?;
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
    f.close().await?;
    result
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
    f.close().await?;
    result
}

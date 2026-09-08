use anyhow::{ensure, Result};
use qbit_prism_server::{ledger::Ledger, rollups};
use sqlx::PgPool;
use std::time::{Duration, Instant};

struct Database {
    admin: PgPool,
    schema: String,
    ledger: Ledger,
}
impl Database {
    async fn open() -> Result<Option<Self>> {
        let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
            eprintln!("set PRISM_TEST_DATABASE_URL for real rollup transaction tests");
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_rollup_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"))
            .append_pair("application_name", &schema);
        let ledger = Ledger::connect(url.as_str(), "rollup-test".into(), 8, true).await?;
        Ok(Some(Self {
            admin,
            schema,
            ledger,
        }))
    }

    async fn insert(&self, id: i64, accepted: bool, epoch: i64) -> Result<()> {
        // Use a near-maximum source value so a summed bucket also proves the
        // unconstrained aggregate columns don't truncate or overflow numeric78.
        sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,reject_reason,writer_id,writer_epoch) VALUES($1,$2,$2,decode(repeat('11',32),'hex'),$3::text::numeric,1,100,'fixture',to_timestamp(1),1,to_timestamp($4::double precision),$5,CASE WHEN $5 THEN NULL ELSE 'low-difficulty' END,'rollup-test',0)")
            .bind(format!("share:{id}"))
            .bind(format!("miner{}",id % 3))
            .bind("9".repeat(78)).bind(epoch).bind(accepted)
            .execute(&self.ledger.pool).await?;
        Ok(())
    }

    async fn assert_exact(&self) -> Result<()> {
        for miner in [false, true] {
            let table = if miner {
                "qbit_hashrate_rollup_miner"
            } else {
                "qbit_hashrate_rollup_pool"
            };
            let key = if miner { ", miner_id" } else { "" };
            let sql = format!("WITH raw AS (SELECT grain_seconds, floor(extract(epoch FROM accepted_at)/grain_seconds)::bigint*grain_seconds AS bucket_epoch{key}, count(*) AS n, sum(share_difficulty) AS d FROM qbit_share_ledger CROSS JOIN (VALUES(300),(3600),(86400)) grains(grain_seconds) WHERE accepted GROUP BY grain_seconds,bucket_epoch{key}) SELECT count(*) FROM raw FULL JOIN {table} r USING(grain_seconds,bucket_epoch{key}) WHERE raw.n IS DISTINCT FROM r.accepted_share_count OR raw.d IS DISTINCT FROM r.accepted_share_difficulty");
            let wrong: i64 = sqlx::query_scalar(&sql)
                .fetch_one(&self.ledger.pool)
                .await?;
            ensure!(wrong == 0, "{table} differs from the exact raw ledger");
        }
        let complete: bool = sqlx::query_scalar("SELECT (SELECT last_share_seq FROM qbit_hashrate_rollup_progress) = (SELECT max(share_seq) FROM qbit_share_ledger)")
            .fetch_one(&self.ledger.pool).await?;
        ensure!(complete, "rejected rows did not advance the watermark");
        Ok(())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_frontends_fold_each_share_once_including_late_clocked_work() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        for id in 1..=40 {
            db.insert(id,id%5!=0,1_800_000_000-(id%7)*86401).await?;
        }
        sqlx::query("INSERT INTO qbit_hashrate_rollup_progress VALUES(true,0,clock_timestamp())")
            .execute(&db.ledger.pool).await?;
        let mut hold = db.ledger.pool.begin().await?;
        sqlx::query("SELECT last_share_seq FROM qbit_hashrate_rollup_progress FOR UPDATE")
            .fetch_one(&mut *hold).await?;
        let one = tokio::spawn({let pool=db.ledger.pool.clone();async move {rollups::advance(&pool,7).await}});
        let two = tokio::spawn({let pool=db.ledger.pool.clone();async move {rollups::advance(&pool,7).await}});
        let deadline = Instant::now()+Duration::from_secs(5);
        loop {
            let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'WITH progress AS%'")
                .bind(&db.schema).fetch_one(&db.admin).await?;
            if waiting==2 {break;}
            ensure!(Instant::now()<deadline,"rollup contenders did not reach the same watermark");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        hold.commit().await?;
        let (one,two)=(one.await??,two.await??);
        ensure!(one.advanced != two.advanced,"both competing snapshots advanced");
        ensure!(one.scanned==7 && two.scanned==7,"fixture did not overlap the same batch");
        loop {
            let (one,two)=tokio::try_join!(rollups::advance(&db.ledger.pool,7),rollups::advance(&db.ledger.pool,9))?;
            if (one.advanced && one.scanned==0)||(two.advanced && two.scanned==0) {break;}
        }
        db.assert_exact().await?;
        // A newly committed share can belong to an old bucket. Watermark
        // ordering must accumulate it even though its clock moved backwards.
        db.insert(41,true,1_700_000_000).await?;
        db.insert(42,false,1_800_000_001).await?;
        rollups::advance(&db.ledger.pool,7).await?;
        db.assert_exact().await?;
        for _ in 0..3 {ensure!(rollups::advance(&db.ledger.pool,7).await?.scanned==0);}
        db.assert_exact().await?;
        Ok::<_,anyhow::Error>(())
    }.await;
    db.close().await?;
    result
}

#[tokio::test]
async fn failed_bucket_write_rolls_back_the_watermark_and_every_other_grain() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = async {
        db.insert(1,true,1_800_000_000).await?;
        sqlx::raw_sql("CREATE FUNCTION fail_rollup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected miner rollup failure'; END $$; CREATE TRIGGER fail_rollup BEFORE INSERT ON qbit_hashrate_rollup_miner FOR EACH ROW EXECUTE FUNCTION fail_rollup();")
            .execute(&db.ledger.pool).await?;
        ensure!(rollups::advance(&db.ledger.pool,10).await.is_err());
        for table in ["qbit_hashrate_rollup_progress","qbit_hashrate_rollup_pool","qbit_hashrate_rollup_miner"] {
            let count:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&db.ledger.pool).await?;
            ensure!(count==0,"{table} survived a failed atomic batch");
        }
        sqlx::query("DROP TRIGGER fail_rollup ON qbit_hashrate_rollup_miner")
            .execute(&db.ledger.pool).await?;
        ensure!(rollups::advance(&db.ledger.pool,10).await?.advanced);
        db.assert_exact().await?;
        Ok::<_,anyhow::Error>(())
    }.await;
    db.close().await?;
    result
}

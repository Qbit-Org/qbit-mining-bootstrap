//! Every production writer of the cluster row's authority (`payout_revision`,
//! `config_fingerprint`, `fatal_error`), driven through its own API against a
//! held job-cohort fence (#479; see `support/cohort_fence.rs`). The signing
//! transition's case is in `signing_transition.rs` and the orphan settlement's
//! in `offer_lifecycle.rs`, next to their fixtures.
use super::cohort_fence::waits_for_the_cohort_fence;
use super::*;
use qbit_prism_server::{config::Config, ledger::HeartbeatStatus};

use super::fake_qbitd as fake;

async fn cluster(pool: &PgPool) -> Result<(i64, Option<String>, Option<String>)> {
    Ok(sqlx::query_as(
        "SELECT payout_revision,config_fingerprint,fatal_error FROM qbit_prism_cluster WHERE singleton",
    )
    .fetch_one(pool)
    .await?)
}

/// A frontend configured with `config`'s fingerprint, and that config.
async fn configured(db: &Database) -> Result<(Ledger, fake::FakeNode, Config)> {
    let ledger = db.ledger("frontend-a").await?;
    let node = fake::FakeNode::open().await?;
    let mut config = fake::coordinator_config(db.url.clone(), &node, "operator")?;
    config.manifest_seed = "42".repeat(32);
    config.ledger_seed = "43".repeat(32);
    config.ledger_public_key = keys().1.public_key_hex();
    config.username_fallback = Some("authority-writers-fallback".into());
    ledger
        .configure(
            &config.fingerprint(&"00".repeat(32))?,
            &SignerKeys::of(&keys().0, &keys().1),
        )
        .await?;
    Ok((ledger, node, config))
}

#[tokio::test]
async fn configure_and_tip_observation_wait_for_a_job_cohort_fence() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    let pool = PgPool::connect(&db.url).await?;
    let result = async {
        let pinning = ledger.clone();
        waits_for_the_cohort_fence(&pool, "configure's first pin", async move {
            pinning
                .configure(&"ab".repeat(32), &SignerKeys::of(&keys().0, &keys().1))
                .await
        })
        .await??;
        ensure!(
            cluster(&pool).await?.1 == Some("ab".repeat(32)),
            "configure did not pin"
        );
        let before = cluster(&pool).await?.0;
        let observing = ledger.clone();
        waits_for_the_cohort_fence(&pool, "a tip observation", async move {
            observing
                .observe_chain_view(&"cd".repeat(32), 100, "04")
                .await
        })
        .await??;
        ensure!(
            cluster(&pool).await?.0 > before,
            "the new tip did not bump the revision"
        );
        Ok(())
    }
    .await;
    pool.close().await;
    result?;
    db.close(vec![ledger]).await
}

/// `bump_revision` through the settlement and the chain reconciler, and the
/// reconciler's halt on a disconnected mature block.
#[tokio::test]
async fn settlement_and_reconcile_bumps_and_the_mature_block_halt_wait_for_a_job_cohort_fence(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let pool = PgPool::connect(&db.url).await?;
    let result = async {
        a.append(share(1), None).await?;
        let snapshot = a.snapshot(100).await?;
        let block = candidate(&snapshot, 7)?;
        let hash = block.block_hash.clone();
        a.enqueue_candidate(block.candidate.clone()).await?;
        let claim = block.claim(a.claim_candidate(60).await?.context("claim")?);
        a.land_candidate(&claim, &keys().1.public_key_hex()).await?;
        let before = cluster(&pool).await?.0;
        let settling = a.clone();
        waits_for_the_cohort_fence(&pool, "the landing's first confirmation", async move {
            settling.finish_candidate(&claim, true, None).await
        })
        .await??;
        ensure!(cluster(&pool).await?.0 == before + 1, "finish did not bump");
        let observe = |active: bool, height: u64| {
            let ledger = a.clone();
            let observation = [BlockObservation {
                block_hash: hash.clone(),
                active,
            }];
            async move { ledger.reconcile_blocks(&observation, height).await }
        };
        for (what, active, height) in [
            ("a reconcile that deactivates the block", false, 101),
            ("a reconcile that confirms and matures it", true, 1101),
        ] {
            let before = cluster(&pool).await?.0;
            waits_for_the_cohort_fence(&pool, what, observe(active, height)).await??;
            ensure!(cluster(&pool).await?.0 > before, "{what} did not bump");
        }
        let halted = waits_for_the_cohort_fence(
            &pool,
            "the mature-block disconnect halt",
            observe(false, 1101),
        )
        .await?;
        ensure!(
            format!("{:#}", halted.err().context("the halt returned success")?)
                .contains("mature pool block disconnected"),
            "the reconcile failed for another reason"
        );
        ensure!(cluster(&pool).await?.2.is_some(), "the halt did not commit");
        Ok(())
    }
    .await;
    pool.close().await;
    result?;
    db.close(vec![a]).await
}

#[tokio::test]
async fn the_deep_fanout_halt_waits_for_a_job_cohort_fence() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    let pool = PgPool::connect(&db.url).await?;
    let result = async {
        let parent = "ab".repeat(32);
        let txid = "ef".repeat(32);
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state,matured_at) VALUES($1,10,'parent','coinbase','manifest','confirmed','mature',clock_timestamp())")
            .bind(&parent).execute(&pool).await?;
        sqlx::query("INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats) VALUES($1,'{}','{}','set','ctv_fanout','coinbase','00',1,1,1)")
            .bind(&parent).execute(&pool).await?;
        sqlx::query("INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,covenant_output_value_sats,fanout_output_sum_sats,settlement_status,confirmed_depth,confirmed_block_hash,confirmed_block_height) VALUES($1,$2,'set','{}','{}','manifest','precommit','ctv','00',0,1,'coinbase',0,'00','00',1,1,'confirmed',1000,$3,20)")
            .bind(&txid).bind(&parent).bind("cd".repeat(32)).execute(&pool).await?;
        let claim = ledger.claim_fanout(60).await?.context("fanout claim")?;
        let revision = ledger.payout_revision().await?;
        let halting = ledger.clone();
        waits_for_the_cohort_fence(&pool, "the deep fanout halt", async move {
            halting.halt_fanout_reorg(&claim, revision).await
        })
        .await??;
        ensure!(
            cluster(&pool)
                .await?
                .2
                .is_some_and(|error| error.starts_with("deep confirmed CTV fanout disconnected")),
            "the halt did not land"
        );
        Ok(())
    }
    .await;
    pool.close().await;
    result?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn a_policy_transition_waits_for_a_job_cohort_fence() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, _node, config) = configured(&db).await?;
    let pool = PgPool::connect(&db.url).await?;
    let result = async {
        ledger.heartbeat(HeartbeatStatus::Stopped).await?;
        let mut target = config.clone();
        target.payout_policy.pool_fee_policy = Some(qbit_prism::PoolFeePolicy {
            fee_bps: 200,
            recipient_id: "fee".into(),
            order_key: "fee".into(),
            p2mr_program_hex: "11".repeat(32),
        });
        let before = cluster(&pool).await?;
        let transitioning = ledger.clone();
        let (current, next) = (config.clone(), target.clone());
        waits_for_the_cohort_fence(&pool, "a policy transition", async move {
            transitioning.transition_policy(&current, &next).await
        })
        .await??;
        let after = cluster(&pool).await?;
        ensure!(
            after.0 > before.0 && after.1 == Some(target.fingerprint(&"00".repeat(32))?),
            "the transition did not land: {before:?} -> {after:?}"
        );
        Ok(())
    }
    .await;
    pool.close().await;
    result?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn a_fatal_state_clear_waits_for_a_job_cohort_fence() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, _node, config) = configured(&db).await?;
    let pool = PgPool::connect(&db.url).await?;
    let result = async {
        // Set the halt as the production writers do: the row lock first.
        let mut halt = pool.begin().await?;
        sqlx::query("SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .execute(&mut *halt)
            .await?;
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='test halt' WHERE singleton")
            .execute(&mut *halt)
            .await?;
        halt.commit().await?;
        ledger.heartbeat(HeartbeatStatus::Stopped).await?;
        let clearing = ledger.clone();
        waits_for_the_cohort_fence(&pool, "fatal-state clear", async move {
            clearing.clear_fatal_state(&config, "investigated").await
        })
        .await??;
        ensure!(cluster(&pool).await?.2.is_none(), "the clear did not land");
        Ok(())
    }
    .await;
    pool.close().await;
    result?;
    db.close(vec![ledger]).await
}

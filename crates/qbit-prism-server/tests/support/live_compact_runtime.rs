//! A real wallet/mempool template, first blob insertion, compact A/B recovery,
//! and node-accepted block. Fixture loading and validation stay outside WAL.
use super::*;
use qbit_prism_server::{coordinator::Coordinator, metrics::Metrics, stratum::MiningBackend};
use std::sync::Arc;

#[path = "prepared_work_assertions.rs"]
#[allow(dead_code)]
mod assertions;
#[path = "fake_qbitd.rs"]
#[allow(dead_code)]
mod configuration;
#[path = "jsonb_inventory.rs"]
#[allow(dead_code)]
mod jsonb_inventory;
#[path = "compact_runtime_observer.rs"]
mod observer;
#[path = "ledger_execution_proxy.rs"]
mod proxy;
#[path = "wal_primary.rs"]
mod wal_primary;
#[path = "window_fixture.rs"]
#[allow(dead_code)]
mod window_fixture;

async fn large_wallet_template(f: &Fixture) -> Result<Value> {
    // Coinbase maturity is 1000 on this chain. These 200 independent inputs
    // create four standard transactions, comfortably below block weight limits.
    f.rpc("generatetoaddress", json!([1200, f.address])).await?;
    let unspent = f
        .rpc("listunspent", json!([1000, 9_999_999, [], true]))
        .await?;
    let funding: Vec<_> = unspent
        .as_array()
        .context("wallet UTXOs missing")?
        .iter()
        .filter(|row| row["spendable"] == true)
        .take(200)
        .collect();
    ensure!(
        funding.len() == 200,
        "insufficient mature independent wallet inputs"
    );
    for inputs in funding.chunks(50) {
        let amount = inputs
            .iter()
            .try_fold(0u64, |sum, row| -> Result<u64> {
                sum.checked_add(qbit_prism_server::broadcaster::amount_bits(&row["amount"])?)
                    .context("funding amount overflow")
            })?
            .checked_sub(1_000_000)
            .context("insufficient fee funding")?;
        let outputs = serde_json::Map::from_iter([(
            f.address.clone(),
            serde_json::from_str::<Value>(&format!(
                "{}.{:08}",
                amount / 100_000_000,
                amount % 100_000_000
            ))?,
        )]);
        let outpoints: Vec<_> = inputs
            .iter()
            .map(|row| json!({"txid":row["txid"], "vout":row["vout"]}))
            .collect();
        let unsigned = f
            .rpc("createrawtransaction", json!([outpoints, outputs]))
            .await?;
        let signed = f
            .rpc("signrawtransactionwithwallet", json!([unsigned]))
            .await?;
        ensure!(
            signed["complete"] == true,
            "wallet did not sign every input"
        );
        f.rpc("sendrawtransaction", json!([signed["hex"]])).await?;
    }
    let template = f
        .rpc("getblocktemplate", json!([{"rules":["segwit"]}]))
        .await?;
    let transactions = template["transactions"]
        .as_array()
        .context("template transactions missing")?;
    ensure!(
        transactions.len() == 4,
        "real template did not include every signed transaction"
    );
    let bytes = serde_json::to_vec(&template)?.len();
    ensure!(
        bytes > 1_000_000,
        "real template must exceed the former JSONB bound: {bytes}"
    );
    eprintln!(
        "large real-node template: transactions={}, serialized_bytes={bytes}",
        transactions.len()
    );
    Ok(template)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_large_template_first_insertion_resumes_and_mines_with_bounded_jsonb_and_wal(
) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let primary = wal_primary::Primary::start(bin).await?;
    let Some(fixture) = Fixture::open_on_database(false, false, Some(&primary.url)).await? else {
        return Ok(());
    };
    let mut frontends = Vec::new();
    let mut observed = None;
    let result = async {
        let (version, fsync): (String, String) = sqlx::query_as(
            "SELECT current_setting('server_version_num'),current_setting('fsync')")
            .fetch_one(&fixture.pool).await?;
        ensure!(version.parse::<u32>()? / 10_000 == 16 && fsync == "on", "qualification requires durable PostgreSQL 16");
        large_wallet_template(&fixture).await?;
        let url = url::Url::parse(&fixture.database_url)?;
        let upstream = tokio::net::lookup_host((url.host_str().context("database host")?, url.port().unwrap_or(5432)))
            .await?.next().context("database address missing")?;
        let proxy = proxy::ExecutionProxy::start(upstream).await?;
        let database_url = proxy.rewrite_url(&fixture.database_url)?;
        observed = Some(proxy);
        let proxy = observed.as_ref().unwrap();
        for instance in ["large-template-a", "large-template-b"] {
            let mut config = configuration::coordinator_config_at(database_url.clone(),
                format!("http://127.0.0.1:{}/", fixture.rpc_port), instance)?;
            config.chain = "regtest".into();
            config.rpc_user = "prismtest".into();
            config.rpc_password = "prismtest".into();
            config.min_peers = 0;
            config.submit_tip_max_age = Duration::from_secs(120);
            config.health_timeout = Duration::from_secs(120);
            frontends.push(Coordinator::new(config, Arc::new(Metrics::default())).await?);
        }
        let (a, b) = (&frontends[0], &frontends[1]);
        let plan = window_fixture::WindowPlan::new(5_000)?;
        plan.load(&fixture.pool, "real-template-window").await?;
        plan.verify_round_trip(&fixture.pool, &[1, 2500, 5000]).await?;
        let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_templates")
            .fetch_one(&fixture.pool).await?;
        ensure!(existing == 0, "first insertion proof must start without any template blob");
        let probe = observer::JsonbProbe::install(&fixture.pool, &fixture.schema).await?;
        sqlx::query("CHECKPOINT").execute(&fixture.pool).await?;
        let mark = proxy.mark();
        let before = observer::insert_lsn(&fixture.pool).await?;
        let started = Instant::now();
        a.refresh_once().await?;
        let elapsed = started.elapsed();
        let after = observer::insert_lsn(&fixture.pool).await?;
        let wal = observer::wal_bytes(&fixture.pool, &before, &after).await?;
        let measured = probe.measure(proxy, mark)?;
        eprintln!("real-node refresh observation: max_jsonb={}, insert_wal_bytes={wal}, refresh_seconds={:.3}", measured.max_uncompressed_bytes, elapsed.as_secs_f64());
        let prepared = a.prepared.read().await.clone().context("refresh did not publish")?;
        assertions::assert_refresh_measurements(5000, prepared.window.shares.context("window missing")?.share_count,
            Some(measured.max_uncompressed_bytes), Some(wal))?;
        let stored = a.ledger.compact_prepared(&prepared.storage_key).await?.context("typed original missing")?;
        let template_bytes = serde_json::to_vec(&stored.template)?.len();
        ensure!(template_bytes > 1_000_000, "refresh did not capture the real large template");
        let blob_bytes: i64 = sqlx::query_scalar("SELECT octet_length(template_bytes)::bigint FROM qbit_prism_templates WHERE template_sha256=$1")
            .bind(&stored.record.template_sha256).fetch_one(&fixture.pool).await?;
        ensure!(blob_bytes == template_bytes as i64, "first template bytes changed in storage");
        let worker = a.authorize(&format!("{}.large", fixture.address)).await?;
        let issued = a.build_job(&worker, "12345678", 1e-12, 0.0).await?;
        a.persist_issued_job(&worker, &issued, 0, Duration::from_secs(60)).await?;
        b.refresh_once().await?;
        let resumed = tokio::time::timeout(Duration::from_secs(25), b.resume_job(&worker, &issued.wire.job_id))
            .await??.context("cross-frontend reconstruction missed")?;
        ensure!(resumed.wire.coinb1 == issued.wire.coinb1 && resumed.wire.coinb2 == issued.wire.coinb2
            && resumed.wire.transactions == issued.wire.transactions
            && resumed.wire.merkle_branch == issued.wire.merkle_branch, "reconstructed large template work changed");
        let mut solved = None;
        for nonce in 0..10_000u32 {
            let proof = resumed.wire.assemble_submission(&"00".repeat(resumed.wire.extranonce2_size),
                &format!("{:08x}", resumed.wire.ntime), &format!("{nonce:08x}"), None, 0)?;
            if proof.block_pass { solved = Some(proof); break; }
        }
        let solved = solved.context("bounded regtest proof search exhausted")?;
        ensure!(fixture.rpc("submitblock", json!([solved.block_hex])).await?.is_null(),
            "node rejected the reconstructed real template block");
        ensure!(fixture.rpc("getbestblockhash", json!([])).await? == solved.block_hash_hex,
            "node did not activate the reconstructed block");
        let hashes = stored.record.audit_hashes.context("original audit hashes missing")?;
        eprintln!("real large-template compact refresh: template_bytes={template_bytes}, shares=5000, max_jsonb={}, insert_wal_bytes={wal}, refresh_seconds={:.3}, audit_sha256={}, manifest_sha256={}, accepted_block={}",
            measured.max_uncompressed_bytes, elapsed.as_secs_f64(), hashes.audit_bundle_sha256,
            hashes.coinbase_manifest_sha256, solved.block_hash_hex);
        Ok::<_, anyhow::Error>(())
    }.await;
    for frontend in frontends {
        frontend.ledger.pool.close().await;
    }
    let proxy_result = match observed {
        Some(proxy) => proxy.finish().await,
        None => Ok(()),
    };
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    let primary = primary.close().await;
    result.and(proxy_result).and(cleanup).and(primary)
}

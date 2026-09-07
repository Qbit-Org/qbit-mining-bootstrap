//! Legacy zero-fee fanouts still need a wallet-funded P2A package. Exercise
//! recovery after the original broadcaster died with a reserved wallet UTXO.
use super::*;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy, SettlementModeConfig};
use qbit_prism_server::{
    codec,
    ledger::{BlockObservation, Candidate, Ledger},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_cpfp_recovers_reserved_funding_after_owner_crash() -> Result<()> {
    let Some(mut fixture) = Fixture::open(true).await? else {
        return Ok(());
    };
    let result=async {
        for server in &mut fixture.servers {server.stop();}
        let ledger=Ledger::connect(&fixture.database_url,"crashed-broadcaster".into(),4,false).await?;
        let template=fixture.rpc("getblocktemplate",json!([{"rules":["segwit"]}])).await?;
        let height=template["height"].as_u64().context("template height missing")?;
        let ntime=template["curtime"].as_u64().context("template time missing")?;
        let target=codec::target_from_compact(codec::parse_u32_hex(template["bits"].as_str().context("template bits missing")?)?)?;
        let network=codec::scaled_target_difficulty(&target)?;
        let validation=fixture.rpc("validateaddress",json!([fixture.address])).await?;
        let script=validation["scriptPubKey"].as_str().context("payout script missing")?;
        ledger.append(AcceptedShare {
            share_seq:0,share_id:format!("cpfp:{}","12".repeat(32)),miner_id:fixture.address.clone(),order_key:fixture.address.clone(),
            p2mr_program_hex:script[4..].into(),share_difficulty:network,network_difficulty:network,template_height:height,
            job_id:"legacy-cpfp".into(),job_issued_at_ms:1,accepted_at_ms:0,ntime:ntime.try_into()?,credit_policy:None,
        },None).await?;
        let snapshot=ledger.snapshot(network).await?;
        let manifest_key=ManifestSigningKey::from_seed_hex(&"11".repeat(32))?;
        let ledger_key=ManifestSigningKey::from_seed_hex(&"22".repeat(32))?;
        let bundle=qbit_prism::build_audit_bundle_with_ctv_settlement_options(
            snapshot.shares.clone(),FoundBlock {block_height:height,coinbase_value_sats:template["coinbasevalue"].as_u64().context("subsidy missing")?,network_difficulty:network,anchor_job_issued_at_ms:snapshot.anchor_ms},
            snapshot.prior_balances.clone(),PayoutPolicy::day_one_default(),u64::MAX,SettlementModeConfig::default(),None,
            Some("00".repeat(12)),codec::witness_merkle_leaves_hex(&codec::transactions_from_template(&template)?),&manifest_key,&ledger_key,
        )?;
        let fanout=bundle.ctv_fanout_manifest_set.as_ref().context("missing zero-fee fanout")?.manifests.first().context("missing fanout chunk")?;
        ensure!(fanout.precommitment.fanout_fee_sats==0 && fanout.precommitment.anchor_vout.is_some(),"fixture did not build a legacy anchored zero-fee fanout");
        let fanout_txid=fanout.fanout_txid.clone();
        let job=codec::Job::from_manifest("legacy-cpfp".into(),&template,&bundle.signed_coinbase_manifest.manifest,"00000000",8,1e-9,0.0,true)?;
        let mut solved=None;
        for nonce in 0u32..10_000 {
            let submission=job.assemble_submission(&"00".repeat(8),&format!("{ntime:08x}"),&format!("{nonce:08x}"),None,0)?;
            if submission.block_pass {solved=Some(submission);break;}
        }
        let solved=solved.context("regtest proof search exhausted")?;
        let block_hash=solved.block_hash_hex.clone();
        ledger.enqueue_candidate(Candidate {block_hash:block_hash.clone(),block_hex:solved.block_hex.clone(),job_id:"legacy-cpfp".into(),payout_revision:snapshot.payout_revision,bundle,coinbase_suffix_hex:None,deferred_share:None}).await?;
        let candidate=ledger.claim_candidate(60).await?.context("candidate claim missing")?;
        ledger.land_candidate(&candidate,&ledger_key.public_key_hex()).await?;
        ensure!(fixture.rpc("submitblock",json!([solved.block_hex])).await?.is_null(),"legacy CTV coinbase rejected");
        ledger.finish_candidate_at_revision(&candidate,true,None,snapshot.payout_revision).await?;
        fixture.rpc("generatetoaddress",json!([1000,fixture.address])).await?;
        ledger.reconcile_blocks_at_revision(&[BlockObservation {block_hash,active:true}],height+1000,ledger.payout_revision().await?).await?;
        let abandoned=ledger.claim_fanout(60).await?.context("mature fanout claim missing")?;
        let unspent=fixture.rpc("listunspent",json!([1,9_999_999,[],true])).await?;
        let funding=unspent.as_array().context("wallet UTXO list missing")?.iter().find(|row|row["spendable"]==true).context("mature sponsorship funding missing")?;
        let funding_txid=funding["txid"].as_str().context("funding txid missing")?;
        let funding_vout:u32=funding["vout"].as_u64().context("funding vout missing")?.try_into()?;
        let amount=qbit_prism_server::broadcaster::amount_bits(&funding["amount"])?;
        ensure!(ledger.reserve_cpfp_funding(&abandoned,"prism",funding_txid,funding_vout,amount).await?,"funding reservation failed");
        let outpoint=json!({"txid":funding_txid,"vout":funding_vout});
        ensure!(fixture.rpc("lockunspent",json!([false,[outpoint]])).await?==true,"simulated owner's wallet lock failed");
        // The owner dies after locking the wallet but before signing. Its
        // successor must find this same reservation and complete the package.
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE fanout_txid=$1").bind(&fanout_txid).execute(&fixture.pool).await?;
        for index in 0..2 {fixture.servers[index]=fixture.start_server_with_sponsorship(index,Some(100_000))?;}
        until("recovered CPFP package in mempool",40,||async {
            let package=ledger.cpfp_package(&fanout_txid).await?;
            let Some(package)=package else{return Ok(false)};
            let Some(child)=package["child_txid"].as_str() else{return Ok(false)};
            let mempool=fixture.rpc("getrawmempool",json!([])).await?;
            Ok(mempool.as_array().is_some_and(|rows|rows.contains(&json!(fanout_txid))&&rows.contains(&json!(child))))
        }).await?;
        let package=ledger.cpfp_package(&fanout_txid).await?.context("signed package not durable")?;
        ensure!(package["funding_txid"]==funding_txid && package["funding_vout"]==funding_vout,"recovery changed funding outpoint");
        until("spent wallet reservation released",10,||async {Ok(ledger.cpfp_package(&fanout_txid).await?.is_some_and(|p|p["wallet_lock_released"]==true))}).await?;
        let locks=fixture.rpc("listlockunspent",json!([])).await?;
        ensure!(!locks.as_array().is_some_and(|rows|rows.contains(&outpoint)),"recovered wallet lock was orphaned");
        ensure!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM qbit_prism_cpfp_packages").fetch_one(&fixture.pool).await?==1,"concurrent broadcasters duplicated the package");
        // Lose the entire node mempool, then resume on a node with no wallet.
        // The durable signed child must be sufficient for idempotent replay.
        for server in &mut fixture.servers {server.stop();}
        fixture.rpc("stop",json!([])).await?;
        until("qbit chain state flushed on shutdown",20,||std::future::ready(fixture.node.child.try_wait().map(|status|status.is_some()).map_err(Into::into))).await?;
        let mut node=Command::new(std::env::var("QBITD_BIN")?);
        node.args(["-regtest","-server=1","-listen=0","-dnsseed=0","-discover=0","-fallbackfee=0.00001","-rpcuser=prismtest","-rpcpassword=prismtest","-txindex=0","-persistmempool=0","-disablewallet=1"])
            .arg(format!("-datadir={}",fixture.directory.path().display())).arg(format!("-rpcport={}",fixture.rpc_port)).arg(format!("-port={}",free_port()?));
        fixture.node=Process::spawn(&mut node,fixture.directory.path().join("qbit-restarted.log"))?;
        until("qbit restarted without sponsorship wallet",30,||async {Ok(fixture.rpc("getblockcount",json!([])).await?==json!(height+1000))}).await?;
        ensure!(fixture.rpc("getrawmempool",json!([])).await?.as_array().is_some_and(Vec::is_empty),"node restart retained the package mempool");
        for index in 0..2 {fixture.servers[index]=fixture.start_server_with_sponsorship(index,Some(100_000))?;}
        until("durable package replayed without original wallet",35,||async {
            let mempool=fixture.rpc("getrawmempool",json!([])).await?;
            Ok(mempool.as_array().is_some_and(|rows|rows.contains(&json!(fanout_txid))&&rows.contains(&package["child_txid"])))
        }).await?;
        ensure!(ledger.cpfp_package(&fanout_txid).await?.context("package disappeared")?["signed_child_hex"]==package["signed_child_hex"],"recovery rewrote the signed package");
        fixture.rpc("generatetoaddress",json!([1,fixture.address])).await?;
        until("recovered CPFP fanout confirmed",40,||async {Ok(sqlx::query_scalar::<_,String>("SELECT settlement_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&fanout_txid).fetch_one(&fixture.pool).await?=="confirmed")}).await?;
        fixture.integrity().await?;
        eprintln!("live CPFP regtest: expired owner wallet reservation recovered by two broadcasters; one durable child replayed after node mempool loss without its original wallet, then confirmed; wallet lock released after spend");
        ledger.pool.close().await;
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

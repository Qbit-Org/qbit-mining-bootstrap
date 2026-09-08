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
    cpfp_recovery_case(RecoveryCase::Locked).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_cpfp_abandoned_child_repair_retains_unremovable_spent_lock() -> Result<()> {
    cpfp_recovery_case(RecoveryCase::Abandoned).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_cpfp_replaces_unsigned_funding_spent_after_reservation_before_wallet_lock(
) -> Result<()> {
    cpfp_recovery_case(RecoveryCase::SpentBeforeLock).await
}

#[derive(Clone, Copy)]
enum RecoveryCase {
    Locked,
    Abandoned,
    SpentBeforeLock,
}

async fn cpfp_recovery_case(case: RecoveryCase) -> Result<()> {
    let repair_abandoned = matches!(case, RecoveryCase::Abandoned);
    let replace_unsigned = matches!(case, RecoveryCase::SpentBeforeLock);
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
            p2mr_program_hex:script[4..].into(),share_difficulty:network,network_difficulty:network,template_height:height.checked_sub(1).context("template parent height missing")?,
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
        if replace_unsigned {
            // The process dies after committing the reservation, before its
            // external wallet lock. A wallet user can still spend this input.
            let reserved=ledger.cpfp_package(&fanout_txid).await?.context("reservation missing")?;
            ensure!(reserved["signed_child_hex"].is_null(),"fixture already signed a child");
            spend_reserved_funding(&fixture,&outpoint,amount).await?;
            ensure!(fixture.rpc("gettxout",json!([funding_txid,funding_vout,false])).await?.is_null(),"reserved input was not spent on the active chain");
        } else {
            ensure!(fixture.rpc("lockunspent",json!([false,[outpoint],true])).await?==true,"simulated owner's wallet lock failed");
        }
        // The owner dies before signing. Two successors must recover either
        // the still-protected reservation or replacement spendable funding.
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
        if replace_unsigned {
            ensure!(package["funding_txid"]!=funding_txid || package["funding_vout"]!=funding_vout,"takeover kept the confirmed-spent unsigned input");
            let archived:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_prism_cpfp_retired_funding WHERE fanout_txid=$1 AND funding_txid=$2 AND funding_vout=$3)")
                .bind(&fanout_txid).bind(funding_txid).bind(i64::from(funding_vout)).fetch_one(&fixture.pool).await?;
            ensure!(archived,"invalid reservation lost cleanup history");
            ensure!(ledger.reserve_cpfp_funding(&abandoned,"prism",funding_txid,funding_vout,amount).await.is_err(),"expired owner replaced successor funding");
            ensure!(ledger.save_cpfp_package(&abandoned,"00",&"ff".repeat(32)).await.is_err(),"expired owner overwrote signed package");
            let replacement_txid=package["funding_txid"].as_str().context("replacement txid missing")?;
            let replacement_vout:u32=package["funding_vout"].as_u64().context("replacement vout missing")?.try_into()?;
            ensure_funding_excluded(&fixture,replacement_txid,replacement_vout).await?;
            ensure!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM qbit_prism_cpfp_packages").fetch_one(&fixture.pool).await?==1,"takeover duplicated the active signed package");
            fixture.rpc("generatetoaddress",json!([1,fixture.address])).await?;
            until("replacement-funded CPFP fanout confirmed",40,||async {Ok(sqlx::query_scalar::<_,String>("SELECT settlement_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&fanout_txid).fetch_one(&fixture.pool).await?=="confirmed")}).await?;
            ensure!(fixture.rpc("gettransaction",json!([package["child_txid"]])).await?["confirmations"].as_u64().unwrap_or(0)>0,"replacement child did not confirm");
            until("retired spent reservation cleanup acknowledged",15,||async {
                Ok(sqlx::query_scalar::<_,bool>("SELECT wallet_lock_released FROM qbit_prism_cpfp_retired_funding WHERE fanout_txid=$1 AND funding_txid=$2 AND funding_vout=$3")
                    .bind(&fanout_txid).bind(funding_txid).bind(i64::from(funding_vout)).fetch_one(&fixture.pool).await?)
            }).await?;
            ensure!(ledger.cpfp_package(&fanout_txid).await?.context("signed package disappeared")?["signed_child_hex"]==package["signed_child_hex"],"confirmation rewrote signed replacement bytes");
            fixture.integrity().await?;
            eprintln!("live CPFP regtest: database-only unsigned reservation became confirmed-spent; two broadcasters archived it, fenced the expired owner, funded one immutable replacement package and confirmed the payout");
            ledger.pool.close().await;
            return Ok(());
        }
        ensure!(package["funding_txid"]==funding_txid && package["funding_vout"]==funding_vout,"recovery changed protected funding outpoint");
        ensure!(package["wallet_lock_released"]==false,"mempool acceptance released unconfirmed funding reservation");
        ensure_funding_excluded(&fixture,funding_txid,funding_vout).await?;
        ensure!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM qbit_prism_cpfp_packages").fetch_one(&fixture.pool).await?==1,"concurrent broadcasters duplicated the package");
        // Evict the entire package through a node restart. The pending wallet
        // spend must keep funding unavailable independently of the mempool.
        stop_broadcasters(&mut fixture,&fanout_txid).await?;
        restart_node(&mut fixture,true,height+1000).await?;
        ensure_funding_excluded(&fixture,funding_txid,funding_vout).await?;
        // Upgrade recovery also repairs a reservation prematurely released by
        // an older broadcaster. The stored signed transaction stays immutable.
        if repair_abandoned {
            fixture.rpc("abandontransaction",json!([package["child_txid"]])).await?;
            let available=fixture.rpc("listunspent",json!([1,9_999_999,[],true])).await?;
            ensure!(available.as_array().is_some_and(|rows|rows.iter().any(|coin|coin["txid"]==funding_txid&&coin["vout"]==funding_vout)),"abandoned child did not expose funding for persistent-lock repair");
        }
        sqlx::query("UPDATE qbit_prism_cpfp_packages SET wallet_lock_released=true WHERE fanout_txid=$1").bind(&fanout_txid).execute(&fixture.pool).await?;
        restart_broadcasters(&mut fixture,&fanout_txid).await?;
        wait_for_package(&fixture,&fanout_txid,&package["child_txid"]).await?;
        until("unconfirmed wallet reservation restored",15,||async {
            let pending=ledger.cpfp_package(&fanout_txid).await?.is_some_and(|p|p["wallet_lock_released"]==false);
            let available=fixture.rpc("listunspent",json!([1,9_999_999,[],true])).await?;
            Ok(pending&&available.as_array().is_some_and(|rows|rows.iter().all(|coin|coin["txid"]!=funding_txid||coin["vout"]!=funding_vout)))
        }).await?;
        ensure!(ledger.cpfp_package(&fanout_txid).await?.context("package disappeared")?["signed_child_hex"]==package["signed_child_hex"],"eviction recovery rewrote the signed package");
        // A different node can relay the exact package without its original
        // wallet; cleanup remains pending until that wallet is available.
        stop_broadcasters(&mut fixture,&fanout_txid).await?;
        restart_node(&mut fixture,false,height+1000).await?;
        restart_broadcasters(&mut fixture,&fanout_txid).await?;
        wait_for_package(&fixture,&fanout_txid,&package["child_txid"]).await?;
        ensure!(ledger.cpfp_package(&fanout_txid).await?.context("package disappeared")?["wallet_lock_released"]==false,"walletless replay discarded cleanup responsibility");
        fixture.rpc("generatetoaddress",json!([1,fixture.address])).await?;
        until("recovered CPFP fanout confirmed",40,||async {Ok(sqlx::query_scalar::<_,String>("SELECT settlement_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&fanout_txid).fetch_one(&fixture.pool).await?=="confirmed")}).await?;
        stop_broadcasters(&mut fixture,&fanout_txid).await?;
        restart_node(&mut fixture,true,height+1001).await?;
        let child=fixture.rpc("gettransaction",json!([package["child_txid"]])).await?;
        ensure!(child["confirmations"].as_u64().unwrap_or(0)>0,"sponsorship child was not actually confirmed");
        let before:chrono::DateTime<chrono::Utc>=sqlx::query_scalar("SELECT updated_at FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&fanout_txid).fetch_one(&fixture.pool).await?;
        restart_broadcasters(&mut fixture,&fanout_txid).await?;
        if repair_abandoned {
            until("confirmed repaired child cleanup attempted",15,||async {
                Ok(sqlx::query_scalar::<_,bool>("SELECT updated_at>$2 AND claim_token IS NULL FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1").bind(&fanout_txid).bind(before).fetch_one(&fixture.pool).await?)
            }).await?;
            ensure!(ledger.cpfp_package(&fanout_txid).await?.context("package disappeared")?["wallet_lock_released"]==false,"failed per-output cleanup was reported as released");
            let locks=fixture.rpc("listlockunspent",json!([])).await?;
            ensure!(locks.as_array().is_some_and(|rows|rows.contains(&outpoint)),"expected Qbit's retained repaired lock on a spent output");
            ensure!(fixture.rpc("gettxout",json!([funding_txid,funding_vout,false])).await?.is_null(),"retained lock was not a harmless confirmed-spent outpoint");
            ensure!(ledger.cpfp_package(&fanout_txid).await?.context("package disappeared")?["signed_child_hex"]==package["signed_child_hex"],"abandoned-child repair changed signed bytes");
        } else {
            until("confirmed child releases funding wallet reservation",15,||async {
                let released=ledger.cpfp_package(&fanout_txid).await?.is_some_and(|p|p["wallet_lock_released"]==true);
                let locks=fixture.rpc("listlockunspent",json!([])).await?;
                Ok(released&&!locks.as_array().is_some_and(|rows|rows.contains(&outpoint)))
            }).await?;
        }
        fixture.integrity().await?;
        eprintln!("live CPFP regtest: two broadcasters recovered funding through eviction and wallet restart, repaired released reservation, replayed exact signed child without its wallet and confirmed it; abandoned_repair={repair_abandoned}, cleanup_deferred={repair_abandoned}");
        ledger.pool.close().await;
        Ok::<_,anyhow::Error>(())
    }.await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

async fn spend_reserved_funding(fixture: &Fixture, outpoint: &Value, amount: u64) -> Result<()> {
    let output = amount
        .checked_sub(100_000)
        .context("funding cannot cover fixture spend fee")?;
    let mut outputs = serde_json::Map::new();
    outputs.insert(
        fixture.address.clone(),
        serde_json::from_str(&format!(
            "{}.{:08}",
            output / 100_000_000,
            output % 100_000_000
        ))?,
    );
    let unsigned = fixture
        .rpc("createrawtransaction", json!([[outpoint], outputs]))
        .await?;
    let signed = fixture
        .rpc("signrawtransactionwithwallet", json!([unsigned]))
        .await?;
    ensure!(
        signed["complete"] == true,
        "wallet did not sign the competing funding spend"
    );
    fixture
        .rpc("sendrawtransaction", json!([signed["hex"]]))
        .await?;
    fixture
        .rpc("generatetoaddress", json!([1, fixture.address]))
        .await?;
    Ok(())
}

async fn stop_broadcasters(fixture: &mut Fixture, fanout: &str) -> Result<()> {
    // Reserve a quiet interval before killing processes, so this test does not
    // depend on waiting out a 120-second claim lease at every node restart.
    until("broadcaster attempt quiescent",20,||async {
        Ok(sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=clock_timestamp()+interval '1 hour' WHERE fanout_txid=$1 AND claim_token IS NULL")
            .bind(fanout).execute(&fixture.pool).await?.rows_affected()==1)
    }).await?;
    for server in &mut fixture.servers {
        server.stop();
    }
    Ok(())
}
async fn restart_broadcasters(fixture: &mut Fixture, fanout: &str) -> Result<()> {
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=clock_timestamp() WHERE fanout_txid=$1").bind(fanout).execute(&fixture.pool).await?;
    for index in 0..2 {
        fixture.servers[index] = fixture.start_server_with_sponsorship(index, Some(100_000))?;
    }
    Ok(())
}
async fn wait_for_package(fixture: &Fixture, fanout: &str, child: &Value) -> Result<()> {
    until("durable package replayed after eviction", 35, || async {
        let mempool = fixture.rpc("getrawmempool", json!([])).await?;
        Ok(mempool
            .as_array()
            .is_some_and(|rows| rows.contains(&json!(fanout)) && rows.contains(child)))
    })
    .await
}
async fn restart_node(fixture: &mut Fixture, wallet: bool, height: u64) -> Result<()> {
    fixture.rpc("stop", json!([])).await?;
    until("qbit chain state flushed on shutdown", 20, || {
        std::future::ready(
            fixture
                .node
                .child
                .try_wait()
                .map(|status| status.is_some())
                .map_err(Into::into),
        )
    })
    .await?;
    let mut node = Command::new(std::env::var("QBITD_BIN")?);
    node.args([
        "-regtest",
        "-server=1",
        "-listen=0",
        "-dnsseed=0",
        "-discover=0",
        "-fallbackfee=0.00001",
        "-rpcuser=prismtest",
        "-rpcpassword=prismtest",
        "-txindex=0",
        "-persistmempool=0",
    ])
    .arg(if wallet {
        "-wallet=prism"
    } else {
        "-disablewallet=1"
    })
    .arg(format!("-datadir={}", fixture.directory.path().display()))
    .arg(format!("-rpcport={}", fixture.rpc_port))
    .arg(format!("-port={}", free_port()?));
    fixture.node = Process::spawn(
        &mut node,
        fixture.directory.path().join("qbit-restarted.log"),
    )?;
    until("qbit restarted after package eviction", 30, || async {
        Ok(fixture.rpc("getblockcount", json!([])).await? == json!(height))
    })
    .await?;
    ensure!(
        fixture
            .rpc("getrawmempool", json!([]))
            .await?
            .as_array()
            .is_some_and(Vec::is_empty),
        "node restart retained the package mempool"
    );
    Ok(())
}

async fn ensure_funding_excluded(fixture: &Fixture, txid: &str, vout: u32) -> Result<()> {
    let available = fixture
        .rpc("listunspent", json!([0, 9_999_999, [], true]))
        .await?;
    ensure!(
        available.as_array().is_some_and(|rows| rows
            .iter()
            .all(|coin| coin["txid"] != txid || coin["vout"] != vout)),
        "wallet coin selection could reuse unconfirmed CPFP funding"
    );
    Ok(())
}

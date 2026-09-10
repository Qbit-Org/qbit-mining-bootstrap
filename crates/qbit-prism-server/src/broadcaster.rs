//! Recoverable, non-custodial CTV submission. A database claim coordinates
//! frontends; every attempt verifies the covenant and live parent maturity.
use crate::{codec, config, coordinator::Coordinator, ledger::FanoutClaim};
use anyhow::{bail, ensure, Context, Result};
use qbit_prism::{CpfpChildRequest, CtvFanoutManifest};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::watch;

pub async fn run(coordinator: Arc<Coordinator>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let mut tick = tokio::time::interval(coordinator.config.ctv_broadcast_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {_=shutdown.changed()=>break,_=tick.tick()=>{}}
        if let Err(error) = run_once(&coordinator).await {
            tracing::warn!(%error,"CTV broadcaster attempt deferred");
        }
    }
    Ok(())
}

pub async fn run_once(coordinator: &Coordinator) -> Result<usize> {
    // A node behind its peers must leave their settlement claims available.
    let chain = crate::readiness::chain_info(
        &coordinator.rpc,
        &coordinator.config.chain,
        coordinator.config.min_peers,
    )
    .await?;
    coordinator
        .ledger
        .observe_chain_view(
            chain["bestblockhash"]
                .as_str()
                .context("chain tip missing")?,
            chain["blocks"].as_u64().context("tip height missing")?,
            chain["chainwork"]
                .as_str()
                .context("cumulative chainwork missing")?,
        )
        .await?;
    let limit = config::number("PRISM_CTV_BROADCASTER_LIMIT", 100usize)?.min(1000);
    let mut count = 0;
    for _ in 0..limit {
        let Some(claim) = coordinator.ledger.claim_fanout(120).await? else {
            break;
        };
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(90),
            process(coordinator, &claim),
        )
        .await;
        match outcome {
            Ok(Ok((status, result))) => {
                coordinator
                    .ledger
                    .finish_fanout(&claim, status, Some(result), None)
                    .await?
            }
            result => {
                let error = match result {
                    Ok(Err(error)) => error,
                    _ => anyhow::anyhow!("CTV attempt deadline exceeded"),
                };
                if let Err(finish) = coordinator
                    .ledger
                    .finish_fanout(&claim, "failed", None, Some(&error.to_string()))
                    .await
                {
                    tracing::warn!(%finish,"CTV claim completion deferred");
                }
                tracing::warn!(%error,fanout=%claim.fanout_txid,"CTV broadcast deferred");
            }
        }
        count += 1;
        tokio::task::yield_now().await;
    }
    Ok(count)
}

fn observation(status: &'static str, details: Value, seconds: u64) -> (&'static str, Value) {
    let mut value = details;
    value["check_only"] = json!(true);
    value["next_check_seconds"] = json!(seconds);
    (status, value)
}

fn confirmed(hash: &str, height: u64, tip: u64) -> Result<(&'static str, Value)> {
    let depth = tip
        .checked_sub(height)
        .context("confirmation above active tip")?
        + 1;
    Ok(observation(
        "confirmed",
        json!({"confirmation":{"block_hash":hash,"block_height":height,"confirmations":depth}}),
        if depth >= 1000 { 60 } else { 5 },
    ))
}

async fn process(coordinator: &Coordinator, claim: &FanoutClaim) -> Result<(&'static str, Value)> {
    coordinator.ledger.renew_fanout_claim(claim, 120).await?;
    let chain = crate::readiness::chain_info(
        &coordinator.rpc,
        &coordinator.config.chain,
        coordinator.config.min_peers,
    )
    .await?;
    let revision = coordinator
        .ledger
        .observe_chain_view(
            chain["bestblockhash"]
                .as_str()
                .context("chain tip missing")?,
            chain["blocks"].as_u64().context("tip height missing")?,
            chain["chainwork"]
                .as_str()
                .context("cumulative chainwork missing")?,
        )
        .await?;
    let (status, mut result) = process_view(coordinator, claim, &chain, revision).await?;
    result["payout_revision"] = json!(revision);
    Ok((status, result))
}

async fn process_view(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    chain: &Value,
    revision: i64,
) -> Result<(&'static str, Value)> {
    let manifest: CtvFanoutManifest = serde_json::from_value(claim.manifest.clone())?;
    qbit_prism::verify_ctv_fanout_manifest_structure(&manifest)?;
    ensure!(
        manifest.fanout_txid == claim.fanout_txid,
        "fanout claim identity mismatch"
    );
    let rpc = &coordinator.rpc;
    let tip_hash = chain["bestblockhash"].clone();
    let block = rpc
        .call("getblockheader", json!([claim.block_hash]))
        .await?;
    ensure!(
        block["confirmations"].as_i64().unwrap_or(-1) > 0,
        "fanout coinbase is not active"
    );
    let height = block["height"]
        .as_u64()
        .context("coinbase block height missing")?;
    let tip = chain["blocks"].as_u64().context("tip height missing")?;
    let mature_height = height
        .checked_add(qbit_prism::QBIT_COINBASE_MATURITY_BLOCKS)
        .context("coinbase height overflow")?;
    ensure!(tip >= mature_height, "fanout coinbase is immature");
    maintain_funding_reservation(coordinator, claim).await?;
    if let (Some(hash), Some(height)) = (
        claim.progress["confirmed_block_hash"].as_str(),
        claim.progress["confirmed_block_height"].as_u64(),
    ) {
        ensure!(height <= tip, "node tip is behind tracked CTV confirmation");
        let active = rpc.call("getblockhash", json!([height])).await? == json!(hash);
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == tip_hash,
            "tip changed during CTV confirmation check"
        );
        if active {
            return confirmed(hash, height, tip);
        }
        if claim.progress["confirmed_depth"].as_u64().unwrap_or(0) >= 1000 {
            coordinator
                .ledger
                .halt_fanout_reorg(claim, revision)
                .await?;
            bail!("deep confirmed CTV checkpoint disconnected");
        }
    }
    if let Ok(tx) = rpc
        .call("getrawtransaction", json!([claim.fanout_txid, true]))
        .await
    {
        if tx["confirmations"].as_u64().unwrap_or(0) > 0 {
            let hash = tx["blockhash"]
                .as_str()
                .context("confirmed transaction lacks block hash")?;
            let header = rpc.call("getblockheader", json!([hash])).await?;
            let confirmed_height = header["height"]
                .as_u64()
                .context("confirmation height missing")?;
            ensure!(
                rpc.call("getblockhash", json!([confirmed_height])).await? == json!(hash),
                "fanout confirmation moved"
            );
            ensure!(
                rpc.call("getbestblockhash", json!([])).await? == tip_hash,
                "tip changed during CTV confirmation check"
            );
            return confirmed(hash, confirmed_height, tip);
        }
    }
    let coin = rpc
        .call(
            "gettxout",
            json!([
                manifest.parent_coinbase_txid,
                manifest.parent_coinbase_vout,
                false
            ]),
        )
        .await?;
    if coin.is_null() {
        return scan_spender(coordinator, claim, &manifest, mature_height, tip, &tip_hash).await;
    }
    ensure!(
        coin["scriptPubKey"]["hex"] == manifest.covenant_script_pubkey_hex,
        "live covenant script mismatch"
    );
    ensure!(
        amount_bits(&coin["value"])? == manifest.covenant_output_value_sats,
        "live covenant value mismatch"
    );
    if rpc
        .call("getmempoolentry", json!([manifest.fanout_txid]))
        .await
        .is_ok()
    {
        return Ok(observation(
            "broadcast_submitted",
            json!({"already_in_mempool":true}),
            5,
        ));
    }
    let fee = config::number(
        if config::optional("PRISM_CTV_BROADCASTER_FEE_BITS").is_some() {
            "PRISM_CTV_BROADCASTER_FEE_BITS"
        } else {
            "PRISM_CTV_BROADCASTER_FEE_SATS"
        },
        0u64,
    )?;
    let result = if manifest.precommitment.fanout_fee_sats > 0 {
        // Built-in-fee fanouts are anchorless and need no wallet sponsorship.
        coordinator.ledger.renew_fanout_claim(claim, 120).await?;
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == tip_hash,
            "tip changed before CTV submission"
        );
        rpc.call("sendrawtransaction", json!([manifest.fanout_tx_hex]))
            .await?
    } else {
        ensure!(fee > 0, "zero-fee fanout requires CPFP fee sponsorship");
        let child = build_child(coordinator, claim, &manifest, fee).await?;
        coordinator.ledger.renew_fanout_claim(claim, 120).await?;
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == tip_hash,
            "tip changed before CTV package submission"
        );
        let result = rpc
            .call("submitpackage", json!([[manifest.fanout_tx_hex, child]]))
            .await?;
        ensure!(
            result["package_msg"] == "success",
            "CTV package rejected: {result}"
        );
        result
    };
    Ok(("broadcast_submitted", json!({"submit_result":result})))
}

async fn scan_spender(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    manifest: &CtvFanoutManifest,
    mature_height: u64,
    tip: u64,
    tip_hash: &Value,
) -> Result<(&'static str, Value)> {
    let rpc = &coordinator.rpc;
    let budget = config::number("PRISM_CTV_SPEND_SCAN_BLOCKS", 32u64)?;
    ensure!(
        (1..=256).contains(&budget),
        "PRISM_CTV_SPEND_SCAN_BLOCKS must be 1..256"
    );
    let mut next = claim.progress["scan_next_height"]
        .as_u64()
        .unwrap_or(mature_height)
        .max(mature_height);
    if let (Some(height), Some(hash)) = (
        claim.progress["scan_anchor_height"].as_u64(),
        claim.progress["scan_anchor_hash"].as_str(),
    ) {
        if height > tip || rpc.call("getblockhash", json!([height])).await? != json!(hash) {
            next = mature_height;
        }
    }
    let end = tip.min(next.saturating_add(budget - 1));
    for number in next..=end {
        coordinator.ledger.renew_fanout_claim(claim, 120).await?;
        let hash = rpc.call("getblockhash", json!([number])).await?;
        let block = rpc.call("getblock", json!([hash, 2])).await?;
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == *tip_hash,
            "tip changed during CTV spend scan"
        );
        for tx in block["tx"].as_array().into_iter().flatten() {
            if tx["vin"].as_array().into_iter().flatten().any(|input| {
                input["txid"] == manifest.parent_coinbase_txid
                    && input["vout"] == manifest.parent_coinbase_vout
            }) {
                ensure!(
                    tx["txid"] == manifest.fanout_txid,
                    "covenant spent by an unexpected transaction"
                );
                return confirmed(hash.as_str().context("block hash missing")?, number, tip);
            }
        }
        // Persist every completed page. Timeouts and process death do not
        // restart a years-old no-txindex search at its first block.
        coordinator
            .ledger
            .record_fanout_scan(
                claim,
                number + 1,
                Some((number, hash.as_str().context("block hash missing")?.into())),
            )
            .await?;
    }
    Ok(observation(
        "broadcastable",
        json!({"spend_scan_pending":true,"next_height":end.saturating_add(1)}),
        if next > tip { 10 } else { 1 },
    ))
}

async fn build_child(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    manifest: &CtvFanoutManifest,
    fee: u64,
) -> Result<String> {
    let anchor = manifest
        .precommitment
        .anchor_vout
        .context("fanout has no CPFP anchor")?;
    let mut package = coordinator.ledger.cpfp_package(&claim.fanout_txid).await?;
    for _ in 0..3 {
        if package.is_none() {
            package = select_cpfp_funding(coordinator, claim, fee).await?;
        }
        let current = package
            .as_ref()
            .context("sponsorship wallet has no unreserved suitable UTXO")?;
        if current["signed_child_hex"].is_string() {
            break;
        }
        match unsigned_funding_invalid_reason(coordinator, current, fee).await? {
            None => break,
            Some(reason) => {
                retire_unsigned_funding(coordinator, claim, current, reason).await?;
                package = None;
            }
        }
    }
    let package =
        package.context("sponsorship funding changed during selection; retry required")?;
    // A committed package can be relayed by any node. Requiring the original
    // wallet here would prevent recovery after mempool loss or node failover.
    if let Some(raw) = package["signed_child_hex"].as_str() {
        ensure!(
            package["wallet_lock_released"] != true,
            "unconfirmed CPFP funding reservation must be repaired before replay"
        );
        let stripped = codec::strip_witness_transaction(&hex::decode(raw)?)?;
        ensure!(
            package["child_txid"] == codec::hash_display(&codec::double_sha256(&stripped)),
            "persisted CPFP child txid mismatch"
        );
        return Ok(raw.into());
    }
    let wallet = coordinator.rpc.wallet(
        package["wallet_name"]
            .as_str()
            .context("reserved wallet missing")?,
    )?;
    let outpoint = json!({"txid":package["funding_txid"],"vout":package["funding_vout"]});
    // Persisting an existing lock is idempotent and also upgrades reservations
    // left by older processes that only held an in-memory wallet lock.
    coordinator
        .ledger
        .mark_cpfp_wallet_lock_pending(claim)
        .await?;
    coordinator.ledger.renew_fanout_claim(claim, 120).await?;
    ensure!(
        wallet
            .call("lockunspent", json!([false, [outpoint], true]))
            .await?
            == true,
        "failed to reserve funding wallet UTXO"
    );
    let address = wallet.call("getnewaddress", json!(["", "p2mr"])).await?;
    let info = wallet.call("getaddressinfo", json!([address])).await?;
    let child = qbit_prism::build_cpfp_child(&CpfpChildRequest {
        fanout_txid: manifest.fanout_txid.clone(),
        anchor_vout: anchor,
        funding_txid: package["funding_txid"]
            .as_str()
            .context("reserved funding txid missing")?
            .into(),
        funding_vout: package["funding_vout"]
            .as_u64()
            .context("reserved funding vout missing")?
            .try_into()?,
        funding_value_sats: package["funding_value_sats"]
            .as_u64()
            .context("reserved funding value missing")?,
        fee_sats: fee,
        change_script_pubkey_hex: info["scriptPubKey"]
            .as_str()
            .context("change script missing")?
            .into(),
    })?;
    coordinator.ledger.renew_fanout_claim(claim, 120).await?;
    let signed=wallet.call("signrawtransactionwithwallet",json!([child.unsigned_child_tx_hex,[{"txid":manifest.fanout_txid,"vout":anchor,"scriptPubKey":qbit_prism::P2A_ANCHOR_SCRIPT_PUBKEY_HEX,"amount":0}]])).await?;
    ensure!(
        signed["complete"] == true,
        "funding wallet did not complete signature"
    );
    let raw = signed["hex"]
        .as_str()
        .context("signed child missing")?
        .to_owned();
    let stripped = codec::strip_witness_transaction(&hex::decode(&raw)?)?;
    ensure!(
        stripped == codec::strip_witness_transaction(&hex::decode(&child.unsigned_child_tx_hex)?)?,
        "wallet changed the CPFP transaction"
    );
    if let Some(reason) = unsigned_funding_invalid_reason(coordinator, &package, fee).await? {
        retire_unsigned_funding(coordinator, claim, &package, reason).await?;
        bail!("CPFP funding changed before signed package persistence");
    }
    let txid = codec::hash_display(&codec::double_sha256(&stripped));
    coordinator
        .ledger
        .save_cpfp_package(claim, &raw, &txid)
        .await?;
    Ok(raw)
}

async fn select_cpfp_funding(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    fee: u64,
) -> Result<Option<Value>> {
    let wallet_name = config::optional("PRISM_CTV_BROADCASTER_WALLET")
        .context("CPFP requires PRISM_CTV_BROADCASTER_WALLET")?;
    let wallet = coordinator.rpc.wallet(&wallet_name)?;
    let utxos = wallet
        .call("listunspent", json!([1, 9_999_999, [], true]))
        .await?;
    let mut eligible = Vec::new();
    for utxo in utxos
        .as_array()
        .context("listunspent did not return an array")?
    {
        if utxo["spendable"] == true {
            let amount = amount_bits(&utxo["amount"])?;
            if amount > fee {
                eligible.push((amount, utxo));
            }
        }
    }
    eligible.sort_by_key(|(amount, _)| std::cmp::Reverse(*amount));
    for (amount, utxo) in eligible {
        let txid = utxo["txid"].as_str().context("funding txid missing")?;
        let vout = utxo["vout"]
            .as_u64()
            .context("funding vout missing")?
            .try_into()?;
        if coordinator
            .ledger
            .reserve_cpfp_funding(claim, &wallet_name, txid, vout, amount)
            .await?
        {
            return coordinator.ledger.cpfp_package(&claim.fanout_txid).await;
        }
    }
    Ok(None)
}

/// A failed RPC is uncertainty, not evidence that a reservation is unusable.
/// Signed packages never enter this replacement path.
async fn unsigned_funding_invalid_reason(
    coordinator: &Coordinator,
    package: &Value,
    fee: u64,
) -> Result<Option<&'static str>> {
    ensure!(
        package["signed_child_hex"].is_null(),
        "signed CPFP package is immutable"
    );
    let tip = coordinator.rpc.call("getbestblockhash", json!([])).await?;
    let coin = coordinator
        .rpc
        .call(
            "gettxout",
            json!([package["funding_txid"], package["funding_vout"], true]),
        )
        .await?;
    ensure!(
        coordinator.rpc.call("getbestblockhash", json!([])).await? == tip,
        "chain changed while validating unsigned CPFP funding"
    );
    if coin.is_null() {
        return Ok(Some(
            "funding output is spent, missing, or spent in the mempool",
        ));
    }
    let confirmations = coin["confirmations"]
        .as_u64()
        .context("funding confirmation count missing")?;
    if confirmations == 0
        || (coin["coinbase"] == true && confirmations < qbit_prism::QBIT_COINBASE_MATURITY_BLOCKS)
    {
        return Ok(Some("funding output is no longer confirmed and mature"));
    }
    let amount = amount_bits(&coin["value"])?;
    if Some(amount) != package["funding_value_sats"].as_u64() || amount <= fee {
        return Ok(Some("funding output value cannot fund the current fee"));
    }
    let wallet = coordinator.rpc.wallet(
        package["wallet_name"]
            .as_str()
            .context("reserved wallet missing")?,
    )?;
    // A wallet that cannot identify its original transaction may simply be
    // unavailable on this server. Preserve the reservation and retry there.
    let known = wallet
        .call("gettransaction", json!([package["funding_txid"]]))
        .await?;
    if known["confirmations"]
        .as_i64()
        .context("wallet funding confirmations missing")?
        <= 0
    {
        return Ok(Some("wallet funding transaction is no longer active"));
    }
    let available = wallet
        .call("listunspent", json!([1, 9_999_999, [], true]))
        .await?;
    if available
        .as_array()
        .context("wallet UTXO list missing")?
        .iter()
        .any(|coin| {
            coin["txid"] == package["funding_txid"]
                && coin["vout"] == package["funding_vout"]
                && coin["spendable"] == true
        })
    {
        return Ok(None);
    }
    let outpoint = json!({"txid":package["funding_txid"],"vout":package["funding_vout"]});
    let locked = wallet.call("listlockunspent", json!([])).await?;
    if locked
        .as_array()
        .context("wallet locked UTXO list missing")?
        .contains(&outpoint)
    {
        // listunspent excludes our existing lock. Confirm control of its
        // actual script rather than replacing valid crash-recovered funding.
        let address = coin["scriptPubKey"]["address"]
            .as_str()
            .context("locked funding address missing")?;
        let info = wallet.call("getaddressinfo", json!([address])).await?;
        if info["ismine"] == true && info["solvable"] == true {
            return Ok(None);
        }
    }
    Ok(Some("wallet funding output is no longer spendable"))
}

async fn retire_unsigned_funding(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
    package: &Value,
    reason: &str,
) -> Result<()> {
    coordinator
        .ledger
        .retire_unsigned_cpfp_funding(
            claim,
            package["funding_txid"]
                .as_str()
                .context("reserved funding txid missing")?,
            package["funding_vout"]
                .as_u64()
                .context("reserved funding vout missing")?
                .try_into()?,
            reason,
        )
        .await
}

async fn require_retired_funding_wallet_control(
    rpc: &crate::rpc::Rpc,
    wallet: &crate::rpc::Rpc,
    txid: &str,
    vout: u32,
) -> Result<()> {
    let known = wallet.call("gettransaction", json!([txid])).await?;
    ensure!(
        known["txid"] == txid,
        "retired funding wallet did not identify its transaction"
    );
    // A transaction can pay two independently hosted wallets. Knowing
    // its txid is insufficient to discharge the other wallet's lock.
    let decoded = rpc
        .call(
            "decoderawtransaction",
            json!([known["hex"]
                .as_str()
                .context("retired funding transaction bytes missing")?]),
        )
        .await?;
    let output = decoded["vout"]
        .as_array()
        .context("retired funding outputs missing")?
        .iter()
        .find(|output| output["n"].as_u64() == Some(u64::from(vout)))
        .context("retired funding output missing")?;
    let address = output["scriptPubKey"]["address"]
        .as_str()
        .context("retired funding address missing")?;
    let info = wallet.call("getaddressinfo", json!([address])).await?;
    ensure!(
        info["ismine"] == true && info["solvable"] == true,
        "retired funding output is not controlled by this wallet"
    );
    Ok(())
}

async fn cleanup_retired_funding(coordinator: &Coordinator, claim: &FanoutClaim) -> Result<()> {
    for package in coordinator
        .ledger
        .retired_cpfp_funding(&claim.fanout_txid)
        .await?
    {
        let txid = package["funding_txid"]
            .as_str()
            .context("retired funding txid missing")?;
        let vout: u32 = package["funding_vout"]
            .as_u64()
            .context("retired funding vout missing")?
            .try_into()?;
        // Rotate the attempt before RPC so one unavailable wallet or Qbit's
        // unremovable spent lock cannot starve subsequent cleanup records.
        coordinator
            .ledger
            .record_retired_cpfp_wallet_cleanup(claim, txid, vout, false)
            .await?;
        let cleanup = async {
            let wallet = coordinator.rpc.wallet(
                package["wallet_name"]
                    .as_str()
                    .context("retired wallet missing")?,
            )?;
            require_retired_funding_wallet_control(&coordinator.rpc, &wallet, txid, vout).await?;
            let outpoint = json!({"txid":txid,"vout":vout});
            let locked = wallet.call("listlockunspent", json!([])).await?;
            if locked
                .as_array()
                .context("wallet locked UTXO list missing")?
                .contains(&outpoint)
            {
                coordinator.ledger.renew_fanout_claim(claim, 120).await?;
                ensure!(
                    wallet
                        .call("lockunspent", json!([true, [outpoint]]))
                        .await?
                        == true,
                    "retired funding UTXO unlock failed"
                );
            }
            coordinator
                .ledger
                .mark_retired_cpfp_wallet_unlocked(claim, txid, vout)
                .await
        }
        .await;
        if let Err(error) = cleanup {
            tracing::warn!(%error,fanout=%claim.fanout_txid,funding=%txid,"retired funding wallet cleanup deferred");
        }
    }
    Ok(())
}

async fn maintain_funding_reservation(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
) -> Result<()> {
    if !matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cleanup_retired_funding(coordinator, claim)
        )
        .await,
        Ok(Ok(()))
    ) {
        tracing::warn!(fanout=%claim.fanout_txid,"retired funding cleanup deferred");
    }
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        maintain_funding_reservation_inner(coordinator, claim),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(%error,fanout=%claim.fanout_txid,"sponsorship wallet reservation maintenance deferred")
        }
        Err(_) => {
            tracing::warn!(fanout=%claim.fanout_txid,"sponsorship wallet reservation maintenance deadline exceeded")
        }
    }
    Ok(())
}

async fn maintain_funding_reservation_inner(
    coordinator: &Coordinator,
    claim: &FanoutClaim,
) -> Result<()> {
    let Some(package) = coordinator.ledger.cpfp_package(&claim.fanout_txid).await? else {
        return Ok(());
    };
    if package["signed_child_hex"].is_null() {
        return Ok(());
    }
    let wallet = coordinator.rpc.wallet(
        package["wallet_name"]
            .as_str()
            .context("reserved wallet missing")?,
    )?;
    let outpoint = json!({"txid":package["funding_txid"],"vout":package["funding_vout"]});
    // Excluding mempool spends is essential: an unconfirmed child may be
    // evicted and its exact durable bytes must remain valid for rebroadcast.
    let unspent = coordinator
        .rpc
        .call(
            "gettxout",
            json!([package["funding_txid"], package["funding_vout"], false]),
        )
        .await?;
    if !unspent.is_null() {
        // Qbit removes explicit locks when it learns a wallet spend. Its
        // non-abandoned/non-conflicted pending transaction then excludes the
        // input from coin selection, including after mempool eviction.
        let mut protected = false;
        if let Some(child_txid) = package["child_txid"].as_str() {
            if let Ok(child) = wallet.call("gettransaction", json!([child_txid])).await {
                let pending = child["txid"] == child_txid
                    && child["confirmations"].as_i64() == Some(0)
                    && child["walletconflicts"]
                        .as_array()
                        .is_some_and(Vec::is_empty)
                    && child["mempoolconflicts"]
                        .as_array()
                        .is_some_and(Vec::is_empty)
                    && child["details"].as_array().is_some_and(|details| {
                        !details.is_empty()
                            && details.iter().any(|detail| detail["abandoned"] == false)
                            && details.iter().all(|detail| detail["abandoned"] != true)
                    });
                if pending {
                    let available = wallet
                        .call("listunspent", json!([0, 9_999_999, [], true]))
                        .await?;
                    protected = available
                        .as_array()
                        .context("wallet UTXO list missing")?
                        .iter()
                        .all(|coin| {
                            coin["txid"] != package["funding_txid"]
                                || coin["vout"] != package["funding_vout"]
                        });
                }
            }
        }
        if !protected {
            coordinator.ledger.renew_fanout_claim(claim, 120).await?;
            ensure!(
                wallet
                    .call("lockunspent", json!([false, [outpoint], true]))
                    .await?
                    == true,
                "failed to retain persistent funding wallet lock"
            );
        }
        // Do not erase evidence of an older premature release until the
        // wallet has proved or successfully restored its protection.
        if package["wallet_lock_released"] == true {
            coordinator
                .ledger
                .mark_cpfp_wallet_lock_pending(claim)
                .await?;
        }
        return Ok(());
    }
    if package["wallet_lock_released"] == true {
        return Ok(());
    }
    // A missing UTXO alone could also mean its funding transaction was
    // disconnected. Require the signed child's actual active confirmation
    // before releasing the wallet lock. A walletless relay can safely defer
    // this cleanup until the sponsorship wallet becomes available again.
    let tip = coordinator.rpc.call("getbestblockhash", json!([])).await?;
    let child = wallet
        .call(
            "gettransaction",
            json!([package["child_txid"]
                .as_str()
                .context("signed CPFP child missing")?]),
        )
        .await?;
    if child["confirmations"].as_u64().unwrap_or(0) == 0 {
        return Ok(());
    }
    let block_hash = child["blockhash"]
        .as_str()
        .context("confirmed child block missing")?;
    let header = coordinator
        .rpc
        .call("getblockheader", json!([block_hash]))
        .await?;
    ensure!(
        header["confirmations"].as_u64().unwrap_or(0) > 0,
        "CPFP child confirmation is not active"
    );
    let height = header["height"]
        .as_u64()
        .context("CPFP child height missing")?;
    ensure!(
        coordinator
            .rpc
            .call("getblockhash", json!([height]))
            .await?
            == json!(block_hash)
            && coordinator.rpc.call("getbestblockhash", json!([])).await? == tip,
        "chain changed while checking CPFP child confirmation"
    );
    let locked = wallet.call("listlockunspent", json!([])).await?;
    if locked
        .as_array()
        .is_some_and(|rows| rows.contains(&outpoint))
    {
        // Qbit 1.0 can retain a restored lock for an already-known child, yet
        // reject per-output unlock once that child spends it. Keep cleanup
        // pending on that error; unlocking all coins would endanger other
        // reservations in the sponsorship wallet.
        coordinator.ledger.renew_fanout_claim(claim, 120).await?;
        ensure!(
            wallet
                .call("lockunspent", json!([true, [outpoint]]))
                .await?
                == true,
            "funding UTXO unlock failed"
        );
    }
    coordinator.ledger.mark_cpfp_wallet_unlocked(claim).await?;
    Ok(())
}

/// RPC monetary values are decimal amounts, never floating-point balances.
pub fn amount_bits(value: &Value) -> Result<u64> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => bail!("invalid monetary value"),
    };
    let (mantissa, exponent) = text
        .split_once(['e', 'E'])
        .map_or((text.as_str(), 0), |(m, e)| {
            (m, e.parse::<i32>().unwrap_or(i32::MIN))
        });
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    ensure!(
        whole
            .bytes()
            .chain(fraction.bytes())
            .all(|c| c.is_ascii_digit()),
        "invalid monetary decimal"
    );
    let digits = format!("{whole}{fraction}").parse::<u128>()?;
    let power = 8i32
        .checked_add(exponent)
        .and_then(|x| x.checked_sub(fraction.len() as i32))
        .context("monetary exponent overflow")?;
    ensure!(
        (-38..=38).contains(&power),
        "monetary exponent out of bounds"
    );
    let amount = if power >= 0 {
        digits
            .checked_mul(10u128.pow(power as u32))
            .context("monetary value overflow")?
    } else {
        let divisor = 10u128.pow((-power) as u32);
        ensure!(
            digits % divisor == 0,
            "monetary value has sub-bit precision"
        );
        digits / divisor
    };
    Ok(amount.try_into()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retired_cleanup_requires_control_of_the_reserved_output() {
        use axum::{extract::OriginalUri, Json, Router};
        async fn reply(OriginalUri(uri): OriginalUri, Json(request): Json<Value>) -> Json<Value> {
            let result = match request["method"].as_str().unwrap() {
                "gettransaction" => {
                    // Both wallets know this batched funding transaction, but
                    // they control different outputs in it.
                    assert_eq!(request["params"], json!(["funding"]));
                    json!({"txid":"funding","hex":"shared-transaction"})
                }
                "decoderawtransaction" => {
                    assert_eq!(request["params"], json!(["shared-transaction"]));
                    json!({"vout":[
                        {"n":0,"scriptPubKey":{"address":"recipient-a"}},
                        {"n":1,"scriptPubKey":{"address":"recipient-b"}}
                    ]})
                }
                "getaddressinfo" => {
                    let owned = (uri.path() == "/wallet/a"
                        && request["params"][0] == "recipient-a")
                        || (uri.path() == "/wallet/b" && request["params"][0] == "recipient-b");
                    // Being able to solve a public script alone is not control.
                    json!({"ismine":owned,"solvable":true})
                }
                _ => panic!("unexpected wallet mutation or cleanup before ownership proof"),
            };
            Json(json!({"id":request["id"],"error":null,"result":result}))
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc = crate::rpc::Rpc::new(
            format!("http://{}/", listener.local_addr().unwrap()),
            "test".into(),
            "test".into(),
            std::time::Duration::from_secs(2),
        )
        .unwrap();
        let server = tokio::spawn(async {
            axum::serve(listener, Router::new().fallback(reply))
                .await
                .unwrap()
        });
        let a = rpc.wallet("a").unwrap();
        let b = rpc.wallet("b").unwrap();
        require_retired_funding_wallet_control(&rpc, &a, "funding", 0)
            .await
            .unwrap();
        let error = require_retired_funding_wallet_control(&rpc, &b, "funding", 0)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not controlled by this wallet"));
        require_retired_funding_wallet_control(&rpc, &b, "funding", 1)
            .await
            .unwrap();
        assert!(
            require_retired_funding_wallet_control(&rpc, &a, "funding", 2)
                .await
                .is_err()
        );
        server.abort();
    }

    #[test]
    fn amount_is_exact() {
        assert_eq!(amount_bits(&json!("0.00000001")).unwrap(), 1);
        assert_eq!(amount_bits(&json!("1e-8")).unwrap(), 1);
        assert_eq!(
            amount_bits(&json!("21000000.00000001")).unwrap(),
            2_100_000_000_000_001
        );
        assert!(amount_bits(&json!("0.000000001")).is_err());
        assert!(amount_bits(&json!("-1")).is_err());
    }
}

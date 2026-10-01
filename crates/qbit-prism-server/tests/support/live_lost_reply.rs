//! #474 slice C: the node accepted the operation, but its reply was lost.
//!
//! Both servers reach a real qbitd through [`RpcReplyHold`]. In each node
//! case the proxy lets one call run on the node (`submitblock`,
//! `sendrawtransaction` for a fee-bearing CTV fanout, or `submitpackage` for a
//! legacy zero-fee fanout and its CPFP child), confirms the node holds the
//! side effect, and withholds the reply. A [`JournalGate`] holds the owning
//! frontend's next journal update for that row, so however long the kill
//! takes, the frontend cannot record the outcome. The test `SIGKILL`s the
//! owner and, with the gate still closed, terminates every database backend
//! it had (a statement it wrote before dying must not commit afterwards),
//! confirms the row carries no outcome, and hands the dead owner's
//! lease to the survivor. The survivor must reconcile against the chain and
//! finish: the original block or transaction, with its original bytes, and
//! no fabricated rejection, duplicate credit or second payout. The killed
//! frontend is then restarted and must not redo anything.
//!
//! The miner case is the Stratum analogue: a [`StratumRelay`] withholds the
//! server's acknowledgement of a share, which the server sends only after
//! the share's ledger outcome is durable (`coordinator/miner_submit.rs`),
//! checks the credit is committed, and closes the miner's connection. The
//! miner's outcome for that submission stays unknown; its retry of the
//! identical share on a new connection, to either frontend, is refused as a
//! duplicate, and the ledger holds exactly one credit.
//!
//! A dead owner's claim lease is expired by [`expire_dead_lease`] rather than
//! waited out (120 s for candidates and fanouts): only after the owner was
//! reaped and its backends terminated, and only while that owner still holds
//! it, so the survivor takes over through the production claim predicate.
use super::fault_seams::{
    end_frontend_backends, expire_dead_lease, frontend_index, kill_frontend, start_frontend,
    HeldReply, JournalGate, RpcReplyHold, StratumRelay,
};
use super::share_client::{
    reason_id, start_share_only_servers, Answer, Proof, ShareClient, StratumSession, ANSWER,
};
use super::*;
use qbit_prism_server::{
    codec::{double_sha256, hash_display, strip_witness_transaction},
    ledger::Ledger,
};

/// A found block whose `submitblock` reply is lost and whose owning
/// frontend dies before it records the offer outcome. The survivor settles
/// the unrecorded reservation as delivery unknown, finds the block on the
/// chain and lands it once: one `submitblock`, the node's bytes, one credit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_submitblock_reply_owner_killed_survivor_lands_the_block_once() -> Result<()> {
    let Some(mut fixture) = Fixture::open_with_servers(false, false).await? else {
        return Ok(());
    };
    let proxy = RpcReplyHold::start(fixture.rpc_port).await?;
    let result = lost_block_reply(&mut fixture, &proxy).await;
    finish(fixture, result).await
}

/// A mature fee-bearing CTV fanout whose `sendrawtransaction` the node
/// accepted, with the reply lost and the owning broadcaster killed before
/// it journals the attempt. The survivor finds it in the mempool and
/// confirms the one fanout with its signed bytes: no second payout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_sendrawtransaction_reply_owner_killed_survivor_confirms_the_fanout_once() -> Result<()>
{
    let Some(mut fixture) = Fixture::open_with_servers(true, false).await? else {
        return Ok(());
    };
    let proxy = RpcReplyHold::start(fixture.rpc_port).await?;
    let result = lost_fanout_reply(&mut fixture, &proxy).await;
    finish(fixture, result).await
}

/// A legacy zero-fee fanout whose CPFP package the node accepted through
/// `submitpackage`, with the reply lost and the owning broadcaster killed
/// after it reserved, locked and signed its wallet funding. The survivor
/// keeps the one immutable package and its funding reservation pending
/// until the child confirms, then releases the wallet lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_submitpackage_reply_owner_killed_survivor_confirms_the_cpfp_package_once(
) -> Result<()> {
    // Started directly on the node, so the schema exists for the in-process
    // builder, and stopped before any work: the proxied servers replace them.
    let Some(mut fixture) = Fixture::open(true).await? else {
        return Ok(());
    };
    for server in &mut fixture.servers {
        server.stop();
    }
    let proxy = RpcReplyHold::start(fixture.rpc_port).await?;
    let result = lost_package_reply(&mut fixture, &proxy).await;
    finish(fixture, result).await
}

/// A share credited durably whose acknowledgement never reaches the miner:
/// the connection closes between the server's answer and its delivery. The
/// retry of the identical share, on either frontend, is a duplicate, and the
/// ledger holds exactly one credit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn share_credited_but_ack_lost_retry_is_deduplicated_exactly() -> Result<()> {
    let Some(mut fixture) = Fixture::open_with_servers(false, false).await? else {
        return Ok(());
    };
    let result = lost_share_ack(&mut fixture).await;
    finish(fixture, result).await
}

async fn finish(fixture: Fixture, result: Result<()>) -> Result<()> {
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

async fn start_frontends(
    fixture: &mut Fixture,
    proxy: &RpcReplyHold,
    fee: Option<u64>,
) -> Result<()> {
    for index in 0..2 {
        let process = start_frontend(fixture, index, proxy.port, fee).await?;
        match fixture.servers.get_mut(index) {
            Some(stopped) => *stopped = process,
            None => fixture.servers.push(process),
        }
    }
    Ok(())
}

/// Kill `owner` and every database backend it had, with `gate` closed, and
/// open the gate. Returns how many of its updates the gate held and how
/// many backends were terminated.
async fn kill_owner_behind_gate(
    fixture: &mut Fixture,
    gate: JournalGate,
    owner: usize,
) -> Result<(usize, usize)> {
    kill_frontend(fixture, owner)?;
    // Only then open the gate: an update held there, or still unread in a
    // backend's socket, rolls back with its backend.
    let held = gate.waiting(fixture).await?.len();
    let terminated = end_frontend_backends(fixture, owner).await?;
    gate.remove(fixture).await?;
    Ok((held, terminated))
}

/// The block hash of a `submitblock` call's parameters.
fn submitted_block(params: &Value) -> Result<String> {
    let block = hex::decode(params[0].as_str().context("submitblock without a block")?)?;
    Ok(hash_display(&double_sha256(
        block.get(..80).context("block shorter than its header")?,
    )))
}

/// The txid of a raw transaction.
fn txid(raw: &str) -> Result<String> {
    Ok(hash_display(&double_sha256(&strip_witness_transaction(
        &hex::decode(raw)?,
    )?)))
}

async fn wait_candidate(fixture: &Fixture, block: &str, state: &str, seconds: u64) -> Result<()> {
    until(&format!("candidate {block} {state}"), seconds, || async {
        let current: Option<String> =
            sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(block)
                .fetch_optional(&fixture.pool)
                .await?;
        Ok(current.as_deref() == Some(state))
    })
    .await
}

/// Mine one block on server `index`'s current work and wait for it to land.
async fn mine_block(fixture: &Fixture, index: usize, label: &str) -> Result<String> {
    let tip = fixture.rpc("getbestblockhash", json!([])).await?;
    let tip = tip.as_str().context("tip missing")?;
    let mut client = ShareClient::connect(
        fixture.stratum[index],
        &format!("{}.{label}", fixture.address),
    )
    .await?;
    client.work_on(tip, Duration::from_secs(30)).await?;
    let submitted = client.submit(Proof::Block).await?;
    ensure!(
        submitted.answer.accepted(),
        "block {} on server {index}: {}",
        submitted.hash,
        submitted.answer
    );
    wait_candidate(fixture, &submitted.hash, "submitted", 60).await?;
    Ok(submitted.hash)
}

/// The accepted credits and credited headers for one header hash.
async fn credits(fixture: &Fixture, hash: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_share_ledger WHERE accepted AND share_id LIKE '%:'||$1),(SELECT count(*) FROM qbit_prism_share_hashes WHERE header_hash=$1)",
    )
    .bind(hash)
    .fetch_one(&fixture.pool)
    .await?)
}

async fn lost_block_reply(fixture: &mut Fixture, proxy: &RpcReplyHold) -> Result<()> {
    start_frontends(fixture, proxy, None).await?;
    // Holds the recording of the offer's outcome from the start: under the
    // 1 s `submitblock` deadline an owner would otherwise record `unknown`
    // on its own before the kill.
    let mut gate = JournalGate::install(
        fixture,
        "hold_offer_outcome",
        "qbit_block_candidate_outbox",
        "OLD.state='offer_reserved' AND NEW.state IS DISTINCT FROM 'offer_reserved'",
    )
    .await?;
    gate.close(fixture).await?;
    let held = proxy.arm("submitblock");
    let tip = fixture.rpc("getbestblockhash", json!([])).await?;
    let mut client = ShareClient::connect(
        fixture.stratum[0],
        &format!("{}.lost-reply", fixture.address),
    )
    .await?;
    client
        .work_on(
            tip.as_str().context("tip missing")?,
            Duration::from_secs(30),
        )
        .await?;
    // The miner's answer is collected at the end: it may be lost with the
    // owner, or wait on the landing, but it must never be a rejection.
    let miner = tokio::spawn(async move { client.submit(Proof::Block).await });
    let held = tokio::time::timeout(Duration::from_secs(60), held)
        .await
        .context("no submitblock reached the proxy")??;
    let block = submitted_block(&held.params)?;
    ensure!(
        held.reply["error"].is_null() && held.reply["result"].is_null(),
        "the node did not accept {block}: {}",
        held.reply
    );
    let header = fixture.rpc("getblockheader", json!([block])).await?;
    ensure!(
        header["confirmations"].as_i64().unwrap_or(0) > 0,
        "the accepted block {block} is not on the node's active chain"
    );
    // The row carries its block bytes until it finishes.
    let (owner, stored): (String, Vec<u8>) = sqlx::query_as(
        "SELECT claim_instance_id,block_bytes FROM qbit_block_candidate_outbox WHERE block_hash=$1 AND state='offer_reserved' AND offer_outcome IS NULL",
    )
    .bind(&block)
    .fetch_one(&fixture.pool)
    .await
    .context("the held block's row is not an unrecorded reservation")?;
    let owner_index = frontend_index(&owner)?;
    let terminated = kill_owner_behind_gate(fixture, gate, owner_index).await?;
    let _ = held.release.send(());
    let (state, outcome, instance): (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT state,offer_outcome,claim_instance_id FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(&block)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        state == "offer_reserved" && outcome.is_none() && instance.as_deref() == Some(&owner),
        "the killed owner journaled its offer: {state}, {outcome:?}, {instance:?}"
    );
    expire_dead_lease(
        fixture,
        "qbit_block_candidate_outbox",
        "block_hash",
        &block,
        &owner,
    )
    .await?;
    wait_candidate(fixture, &block, "submitted", 90).await?;
    let submitted = miner.await??;
    ensure!(submitted.hash == block, "the miner submitted another block");
    let answer_ok = match &submitted.answer {
        Answer::Accepted | Answer::Lost(_) => true,
        answer => answer.reason_id() == Some("ledger-outcome-unknown"),
    };
    ensure!(
        answer_ok,
        "the miner was answered {} for a block the node accepted",
        submitted.answer
    );

    // The killed frontend comes back and mines a block of its own.
    fixture.servers[owner_index] = start_frontend(fixture, owner_index, proxy.port, None).await?;
    let after = mine_block(fixture, owner_index, "after-restart").await?;
    fixture.quiesce().await?;

    let offers: Vec<String> = proxy
        .calls("submitblock")
        .iter()
        .map(|call| submitted_block(&call.params))
        .collect::<Result<_>>()?;
    ensure!(
        offers == [block.clone(), after.clone()],
        "each block must be offered exactly once: {offers:?}"
    );
    let rows: Vec<(String, String, Option<String>, bool)> = sqlx::query_as(
        "SELECT block_hash,state,offer_outcome,claim_token IS NULL FROM qbit_block_candidate_outbox ORDER BY created_at",
    )
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        rows == vec![
            (
                block.clone(),
                "submitted".into(),
                Some("unknown".into()),
                true
            ),
            (
                after.clone(),
                "submitted".into(),
                Some("accepted".into()),
                true
            ),
        ],
        "unexpected candidate outcomes: {rows:?}"
    );
    let chain: Vec<(String, String)> =
        sqlx::query_as("SELECT block_hash,chain_state FROM qbit_pool_blocks ORDER BY block_height")
            .fetch_all(&fixture.pool)
            .await?;
    ensure!(
        chain
            == vec![
                (block.clone(), "confirmed".into()),
                (after.clone(), "confirmed".into())
            ],
        "pool blocks disagree with the chain: {chain:?}"
    );
    // The bytes the frontend journaled before the offer are the node's.
    let raw = fixture.rpc("getblock", json!([block, 0])).await?;
    ensure!(
        hex::decode(raw.as_str().context("raw block missing")?)? == stored
            && held.params[0].as_str().map(hex::decode).transpose()? == Some(stored.clone()),
        "the journaled, offered and chain block bytes differ"
    );
    // The survivor's audit verifies against the coinbase the node holds.
    let survivor = 1 - owner_index;
    let body: Value = fixture
        .client
        .get(format!(
            "http://127.0.0.1:{}/audit/blocks/{block}/bundle",
            fixture.api[survivor]
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let bundle: AuditBundle = serde_json::from_value(body["audit_bundle"].clone())?;
    let verbose = fixture.rpc("getblock", json!([block, 2])).await?;
    let coinbase = fixture
        .rpc(
            "getrawtransaction",
            json!([verbose["tx"][0]["txid"], false, block]),
        )
        .await?;
    let key = ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex();
    verify_audit_bundle_against_coinbase_tx_hex(
        &bundle,
        coinbase.as_str().context("node coinbase missing")?,
        &key,
    )?;
    for hash in [&block, &after] {
        let (credited, headers) = credits(fixture, hash).await?;
        ensure!(
            (credited, headers) == (1, 1),
            "block {hash} credited {credited} time(s) under {headers} header(s)"
        );
    }
    fixture.integrity().await?;
    eprintln!(
        "lost submitblock reply: {block} accepted by the node, owner {owner} killed before journaling ({} update(s) held by the gate, {} backend(s) terminated); survivor live-{survivor} landed it as delivery unknown; one offer, node bytes, one credit, audit verified; miner answer {}; restarted {owner} then landed {after}",
        terminated.0,
        terminated.1,
        submitted.answer
    );
    Ok(())
}

/// The first CTV fanout of a block mined on server 0, once it is recorded:
/// its txid, signed bytes and block height.
async fn mined_fanout(fixture: &Fixture) -> Result<(String, String, i64)> {
    let block = mine_block(fixture, 0, "fanout").await?;
    let query = "SELECT a.fanout_txid,a.manifest->>'fanout_tx_hex',b.block_height FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE a.block_hash=$1 AND b.chain_state='confirmed'";
    until("the block's CTV fanout recorded", 60, || async {
        Ok(sqlx::query(query)
            .bind(&block)
            .fetch_optional(&fixture.pool)
            .await?
            .is_some())
    })
    .await?;
    Ok(sqlx::query_as(query)
        .bind(&block)
        .fetch_one(&fixture.pool)
        .await?)
}

/// The fanout row's journal: status, attempts, and claim owner.
async fn fanout_journal(fixture: &Fixture, fanout: &str) -> Result<(String, i64, Option<String>)> {
    Ok(sqlx::query_as(
        "SELECT settlement_status,broadcast_attempt_count,claim_instance_id FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1",
    )
    .bind(fanout)
    .fetch_one(&fixture.pool)
    .await?)
}

/// Install the gate that holds a fanout's completion: any update that ends
/// or replaces a claim (a renewal keeps the token and passes).
async fn fanout_gate(fixture: &Fixture) -> Result<JournalGate> {
    JournalGate::install(
        fixture,
        "hold_fanout_completion",
        "qbit_ctv_fanout_artifacts",
        "OLD.claim_token IS NOT NULL AND NEW.claim_token IS DISTINCT FROM OLD.claim_token",
    )
    .await
}

/// With a broadcaster's call held after the node accepted it, behind the
/// closed `gate`: kill its owner, check the attempt stayed unjournaled, and
/// hand its lease to the survivor. Returns the owner's instance id and index.
async fn kill_fanout_owner(
    fixture: &mut Fixture,
    gate: JournalGate,
    fanout: &str,
) -> Result<(String, usize)> {
    // The gate was closed before the call: the attempt cannot be journaled,
    // so this is the row as the owner left it before sending.
    let (status, attempts, owner) = fanout_journal(fixture, fanout).await?;
    let owner = owner.context("the held fanout has no claim owner")?;
    ensure!(
        !matches!(status.as_str(), "broadcast_submitted" | "confirmed"),
        "the owner journaled its held send before the kill: {status}"
    );
    let owner_index = frontend_index(&owner)?;
    let terminated = kill_owner_behind_gate(fixture, gate, owner_index).await?;
    let journal = fanout_journal(fixture, fanout).await?;
    ensure!(
        journal == (status, attempts, Some(owner.clone())),
        "the killed owner journaled its attempt: {journal:?}"
    );
    expire_dead_lease(
        fixture,
        "qbit_ctv_fanout_artifacts",
        "fanout_txid",
        fanout,
        &owner,
    )
    .await?;
    eprintln!(
        "killed fanout owner {owner}; {} update(s) held by the gate, {} backend(s) terminated",
        terminated.0, terminated.1
    );
    Ok((owner, owner_index))
}

/// Wait for the armed call's withheld reply while `gate` stays closed, so
/// the owner can never journal the call first, however late this task runs.
/// An attempt that ends without sending would wait on the gate and hold its
/// claim: it is let through, and the gate closes again at once. Returns the
/// held reply and how many such attempts passed.
async fn held_behind_gate(
    fixture: &Fixture,
    gate: &mut JournalGate,
    mut held: tokio::sync::oneshot::Receiver<HeldReply>,
    call: &str,
) -> Result<(HeldReply, usize)> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut passed = 0;
    loop {
        tokio::select! {
            biased;
            reply = &mut held => {
                return Ok((reply.with_context(|| format!("the proxy dropped the held {call}"))?, passed));
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        ensure!(
            Instant::now() < deadline,
            "no {call} reached the proxy ({passed} unsent attempt(s) let through the gate)"
        );
        if !gate.waiting(fixture).await?.is_empty() {
            // The proxy hands over the held reply before it withholds the
            // node's answer, so an update that follows a send always finds
            // the reply already here: take it instead of opening the gate.
            // Only an attempt that never sent gets through.
            match held.try_recv() {
                Ok(reply) => return Ok((reply, passed)),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    bail!("the proxy dropped the held {call}")
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
            // Closing again waits for the released update's transaction.
            gate.open().await?;
            gate.close(fixture).await?;
            passed += 1;
        }
    }
}

async fn wait_fanout(fixture: &Fixture, fanout: &str, status: &str) -> Result<()> {
    until(&format!("fanout {fanout} {status} and unclaimed"), 60, || async {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT settlement_status=$2 AND claim_token IS NULL FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1",
        )
        .bind(fanout)
        .bind(status)
        .fetch_one(&fixture.pool)
        .await?)
    })
    .await
}

async fn in_mempool(fixture: &Fixture, txid: &str) -> Result<bool> {
    Ok(fixture
        .rpc("getrawmempool", json!([]))
        .await?
        .as_array()
        .is_some_and(|rows| rows.contains(&json!(txid))))
}

/// The fanout confirmed on chain with exactly `raw` as its bytes, spending
/// its covenant output, and nothing left in the mempool to pay again.
async fn confirmed_once(fixture: &Fixture, fanout: &str, raw: &str) -> Result<()> {
    let (block, height): (String, i64) = sqlx::query_as(
        "SELECT confirmed_block_hash,confirmed_block_height FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1",
    )
    .bind(fanout)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        fixture.rpc("getblockhash", json!([height])).await? == json!(block),
        "the recorded confirmation is not on the active chain"
    );
    let onchain = fixture
        .rpc("getrawtransaction", json!([fanout, false, block]))
        .await?;
    ensure!(
        onchain.as_str() == Some(raw),
        "the confirmed fanout's bytes differ from the signed manifest"
    );
    let manifest: Value =
        sqlx::query_scalar("SELECT manifest FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
            .bind(fanout)
            .fetch_one(&fixture.pool)
            .await?;
    ensure!(
        fixture
            .rpc(
                "gettxout",
                json!([
                    manifest["parent_coinbase_txid"],
                    manifest["parent_coinbase_vout"],
                    true
                ])
            )
            .await?
            .is_null(),
        "the covenant output is still unspent"
    );
    ensure!(
        fixture
            .rpc("getrawmempool", json!([]))
            .await?
            .as_array()
            .is_some_and(Vec::is_empty),
        "the mempool still holds a payout transaction"
    );
    Ok(())
}

async fn lost_fanout_reply(fixture: &mut Fixture, proxy: &RpcReplyHold) -> Result<()> {
    start_frontends(fixture, proxy, None).await?;
    // Installed open once the servers made the schema; nothing is claimable
    // before the coinbase matures.
    let mut gate = fanout_gate(fixture).await?;
    let (fanout, raw, height) = mined_fanout(fixture).await?;
    ensure!(
        txid(&raw)? == fanout,
        "the manifest's bytes are not its txid"
    );
    // Closed before the coinbase matures: nothing is claimable until then.
    gate.close(fixture).await?;
    let held = proxy.arm("sendrawtransaction");
    let tip = fixture
        .rpc("getblockcount", json!([]))
        .await?
        .as_i64()
        .context("tip missing")?;
    fixture
        .rpc(
            "generatetoaddress",
            json!([height + 1000 - tip, fixture.address]),
        )
        .await?;
    let (held, passed) = held_behind_gate(fixture, &mut gate, held, "sendrawtransaction").await?;
    ensure!(
        held.params[0].as_str() == Some(raw.as_str()) && held.reply["result"] == json!(fanout),
        "the node did not accept the fanout's signed bytes: {}",
        held.reply
    );
    ensure!(
        in_mempool(fixture, &fanout).await?,
        "the accepted fanout is not in the mempool"
    );
    let (owner, owner_index) = kill_fanout_owner(fixture, gate, &fanout).await?;
    let _ = held.release.send(());
    // The survivor finds the fanout in the mempool and records it.
    wait_fanout(fixture, &fanout, "broadcast_submitted").await?;
    fixture.servers[owner_index] = start_frontend(fixture, owner_index, proxy.port, None).await?;
    fixture
        .rpc("generatetoaddress", json!([1, fixture.address]))
        .await?;
    wait_fanout(fixture, &fanout, "confirmed").await?;
    fixture.quiesce().await?;

    confirmed_once(fixture, &fanout, &raw).await?;
    let sends = proxy.calls("sendrawtransaction");
    ensure!(
        !sends.is_empty()
            && sends
                .iter()
                .all(|call| call.params[0].as_str() == Some(raw.as_str())),
        "a broadcast carried other bytes than the signed fanout"
    );
    let fanouts: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_artifacts")
        .fetch_one(&fixture.pool)
        .await?;
    ensure!(fanouts == 1, "{fanouts} fanouts recorded for one block");
    fixture.integrity().await?;
    let (_, attempts, _) = fanout_journal(fixture, &fanout).await?;
    eprintln!(
        "lost sendrawtransaction reply: fanout {fanout} accepted by the node, owner {owner} killed before journaling; survivor recorded it from the mempool and it confirmed with its signed bytes; {} identical broadcast(s), {attempts} journaled attempt(s), {passed} unsent attempt(s) let through the gate first",
        sends.len()
    );
    Ok(())
}

async fn lost_package_reply(fixture: &mut Fixture, proxy: &RpcReplyHold) -> Result<()> {
    let ledger = Ledger::connect(&fixture.database_url, "legacy-builder".into(), 4, false).await?;
    let result = async {
        let (fanout, _) = cpfp_tests::land_legacy_zero_fee_fanout(fixture, &ledger).await?;
        let raw: String = sqlx::query_scalar(
            "SELECT manifest->>'fanout_tx_hex' FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1",
        )
        .bind(&fanout)
        .fetch_one(&fixture.pool)
        .await?;
        // Closed before any broadcaster runs.
        let mut gate = fanout_gate(fixture).await?;
        gate.close(fixture).await?;
        let held = proxy.arm("submitpackage");
        start_frontends(fixture, proxy, Some(100_000)).await?;
        let (held, passed) = held_behind_gate(fixture, &mut gate, held, "submitpackage").await?;
        let child_raw = held.params[0][1]
            .as_str()
            .context("package without a child")?
            .to_owned();
        let child = txid(&child_raw)?;
        ensure!(
            held.params[0][0].as_str() == Some(raw.as_str())
                && held.reply["result"]["package_msg"] == "success",
            "the node did not accept the package: {}",
            held.reply
        );
        for tx in [&fanout, &child] {
            ensure!(in_mempool(fixture, tx).await?, "the accepted package's {tx} is not in the mempool");
        }
        // The owner saved its signed child before the call.
        let package: (Option<String>, String, i32, bool) = sqlx::query_as(
            "SELECT signed_child_hex,funding_txid,funding_vout,wallet_lock_released FROM qbit_prism_cpfp_packages WHERE fanout_txid=$1",
        )
        .bind(&fanout)
        .fetch_one(&fixture.pool)
        .await?;
        ensure!(
            package.0.as_deref() == Some(child_raw.as_str()) && !package.3,
            "the durable package is not the one submitted: {package:?}"
        );
        let (funding_txid, funding_vout) = (package.1.clone(), package.2);
        let (owner, owner_index) = kill_fanout_owner(fixture, gate, &fanout).await?;
        let _ = held.release.send(());
        wait_fanout(fixture, &fanout, "broadcast_submitted").await?;
        // Unconfirmed: the funding stays reserved and locked, its cleanup
        // explicitly pending, whoever owns the row now.
        let released: bool = sqlx::query_scalar(
            "SELECT wallet_lock_released FROM qbit_prism_cpfp_packages WHERE fanout_txid=$1",
        )
        .bind(&fanout)
        .fetch_one(&fixture.pool)
        .await?;
        ensure!(!released, "the takeover released an unconfirmed funding reservation");
        // The reservation is consumed by the durable child alone: qbitd's
        // wallet drops its lock once a mempool transaction spends the coin,
        // so the database flag above is the pending cleanup record, and the
        // funding outpoint must be spent by exactly that child.
        let outpoint = json!({"txid":funding_txid,"vout":funding_vout});
        ensure!(
            fixture
                .rpc("gettxout", json!([funding_txid, funding_vout, true]))
                .await?
                .is_null(),
            "the reserved funding is unspent in the mempool"
        );
        let decoded = fixture
            .rpc("decoderawtransaction", json!([child_raw]))
            .await?;
        ensure!(
            decoded["vin"].as_array().is_some_and(|inputs| inputs
                .iter()
                .any(|input| input["txid"] == funding_txid && input["vout"] == funding_vout)),
            "the durable child does not spend the reserved funding"
        );
        fixture.servers[owner_index] =
            start_frontend(fixture, owner_index, proxy.port, Some(100_000)).await?;
        fixture
            .rpc("generatetoaddress", json!([1, fixture.address]))
            .await?;
        wait_fanout(fixture, &fanout, "confirmed").await?;
        until("the confirmed child releases its funding lock", 30, || async {
            let released: bool = sqlx::query_scalar(
                "SELECT wallet_lock_released FROM qbit_prism_cpfp_packages WHERE fanout_txid=$1",
            )
            .bind(&fanout)
            .fetch_one(&fixture.pool)
            .await?;
            let locks = fixture.rpc("listlockunspent", json!([])).await?;
            Ok(released && !locks.as_array().is_some_and(|rows| rows.contains(&outpoint)))
        })
        .await?;
        fixture.quiesce().await?;

        confirmed_once(fixture, &fanout, &raw).await?;
        let confirmed = fixture.rpc("gettransaction", json!([child])).await?;
        ensure!(
            confirmed["confirmations"].as_u64().unwrap_or(0) > 0,
            "the CPFP child did not confirm"
        );
        let packages: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT fanout_txid,signed_child_hex FROM qbit_prism_cpfp_packages",
        )
        .fetch_all(&fixture.pool)
        .await?;
        ensure!(
            packages == vec![(fanout.clone(), Some(child_raw.clone()))],
            "the package was duplicated or rewritten: {packages:?}"
        );
        let submits = proxy.calls("submitpackage");
        ensure!(
            submits
                .iter()
                .all(|call| call.params[0] == json!([raw, child_raw])),
            "a package submission carried other bytes"
        );
        let retired: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_cpfp_retired_funding")
            .fetch_one(&fixture.pool)
            .await?;
        ensure!(retired == 0, "the takeover retired the signed package's funding");
        fixture.integrity().await?;
        eprintln!(
            "lost submitpackage reply: package {fanout}+{child} accepted by the node, owner {owner} killed before journaling; survivor recorded it from the mempool, kept funding {funding_txid}:{funding_vout} reserved until the child confirmed, then released it; {} identical submission(s), {passed} unsent attempt(s) let through the gate first",
            submits.len()
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    ledger.pool.close().await;
    result
}

async fn lost_share_ack(fixture: &mut Fixture) -> Result<()> {
    start_share_only_servers(fixture, &[(0, Vec::new()), (1, Vec::new())]).await?;
    let relay = StratumRelay::start(fixture.stratum[0]).await?;
    let username = format!("{}.lost-ack", fixture.address);
    let mut session = StratumSession::open(relay.port).await?;
    session
        .request(&json!({"id":1,"method":"mining.subscribe","params":["prism-lost-ack"]}))
        .await?;
    let authorized = session
        .request(&json!({"id":2,"method":"mining.authorize","params":[username,"x"]}))
        .await?;
    ensure!(authorized["result"] == true, "authorize: {authorized}");
    let (params, submitted) = session
        .solve_latest(&username, Some(Proof::Share), false)
        .await?;
    let held = relay.arm(json!(10));
    session
        .send(&json!({"id":10,"method":"mining.submit","params":params}))
        .await?;
    let held = tokio::time::timeout(ANSWER, held)
        .await
        .context("the server never answered the share")??;
    ensure!(
        held.answer["result"] == true && held.answer["error"].is_null(),
        "the server did not accept the share: {}",
        held.answer
    );
    // The answer follows the ledger outcome: the credit is already durable.
    ensure!(
        credits(fixture, &submitted.hash).await? == (1, 1),
        "the acknowledged share is not durably credited"
    );
    let _ = held.sever.send(());
    let first = match session.answer(&json!(10), Duration::from_secs(10)).await {
        Ok(message) => bail!("the miner received the withheld answer: {message}"),
        Err(error) => Answer::Lost(format!("{error:#}")),
    };

    // The miner retries the identical submission on a new connection, to
    // the same frontend and to the other one. Resuming the job right after
    // the credit can race the frontend's republication of its work, which
    // answers `unknown-job` and must be re-queried (`stratum.rs`, "a miss is
    // not proof the ID is bogus"): a miner that retries is answered again,
    // up to a few times, and nothing but that miss may precede the answer.
    let mut retries = Vec::new();
    for index in [0, 1] {
        let mut retry = StratumSession::open(fixture.stratum[index]).await?;
        retry
            .request(&json!({"id":1,"method":"mining.subscribe","params":["prism-lost-ack"]}))
            .await?;
        let authorized = retry
            .request(&json!({"id":2,"method":"mining.authorize","params":[username,"x"]}))
            .await?;
        ensure!(authorized["result"] == true, "authorize: {authorized}");
        let mut misses = 0;
        let answer = loop {
            let answer = retry
                .request(&json!({"id":11+misses,"method":"mining.submit","params":params}))
                .await?;
            if reason_id(&answer) != Some("unknown-job") || misses == 5 {
                break answer;
            }
            misses += 1;
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        ensure!(
            answer["result"] != true && reason_id(&answer) == Some("duplicate-share"),
            "the retry on server {index} was not refused as a duplicate after {misses} transient miss(es): {answer}"
        );
        retries.push((index, misses));
    }
    ensure!(
        credits(fixture, &submitted.hash).await? == (1, 1),
        "the retried share was credited again"
    );
    let rows: Vec<(String, bool)> = sqlx::query_as(
        "SELECT share_id,accepted FROM qbit_share_ledger WHERE share_id LIKE $1||':%'",
    )
    .bind(&username)
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        rows == vec![(submitted.share_id.clone(), true)],
        "the miner's ledger rows are not exactly the one credit: {rows:?}"
    );
    fixture.integrity().await?;
    eprintln!(
        "lost share ACK: {} credited durably, the server's acceptance withheld and the connection closed; the miner's own outcome stays {first}; its identical retry was refused as a duplicate on both frontends (transient unknown-job misses first: {:?}); one ledger row, one credited header",
        submitted.share_id,
        retries
            .iter()
            .map(|(index, misses)| format!("server {index}: {misses}"))
            .collect::<Vec<_>>()
    );
    Ok(())
}

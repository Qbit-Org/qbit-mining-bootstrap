//! The operator recovery command (#418): `candidates recover`, its plan and
//! its `--apply`.
//!
//! Every test runs the real binary against a scripted fake node that records
//! every RPC method it is asked and answers only the read-only chain queries
//! a recovery needs, so "never calls `submitblock`" is asserted against a
//! real socket the child could have reached, and a call there would have
//! been refused as an unknown method rather than accepted. No test registers
//! an instance: the only `qbit_prism_instances` row is the ledger the test
//! itself opened.
use super::candidates_cli::{
    assert_no_new_instances, code, seed, stderr, stdout, whole_row, Row, Shape,
};
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use serde_json::Value;
use sqlx::{Column, Executor};
use std::{
    collections::{BTreeMap, HashMap},
    process::Output,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::process::Command;

#[path = "ledger_execution_proxy.rs"]
mod execution_proxy;

/// The instance ID every `--apply` run below names itself with, as the
/// runbook recommends, so a claim it holds is legible in `candidates list`.
const RECOVERY_INSTANCE: &str = "operator-recovery-test";

/// The chain the scripted node reports: which hash the active chain holds at
/// each height, and which headers it knows, side-chain headers included.
#[derive(Default)]
struct Chain {
    active: BTreeMap<u64, String>,
    headers: HashMap<String, (u64, String)>,
    header_error: Option<Value>,
    methods: Vec<String>,
}

impl Chain {
    /// Put `hash` on the active chain at `height`, its header naming `parent`.
    fn extend(&mut self, height: u64, hash: &str, parent: &str) {
        self.active.insert(height, hash.to_owned());
        self.headers
            .insert(hash.to_owned(), (height, parent.to_owned()));
    }

    /// A header the node knows whose height the active chain fills with
    /// another block: a stale block.
    fn side(&mut self, height: u64, hash: &str, parent: &str) {
        self.headers
            .insert(hash.to_owned(), (height, parent.to_owned()));
    }

    fn tip(&self) -> (u64, String) {
        self.active
            .iter()
            .next_back()
            .map(|(height, hash)| (*height, hash.clone()))
            .expect("the genesis is always on the chain")
    }
}

/// The scripted node. `submitblock` has no arm: a call is recorded and
/// answered with the unknown-method error.
struct ScriptedNode {
    url: String,
    chain: Arc<Mutex<Chain>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ScriptedNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ScriptedNode {
    async fn open() -> Result<Self> {
        let mut chain = Chain::default();
        chain.active.insert(0, "00".repeat(32));
        let chain = Arc::new(Mutex::new(chain));
        let app = Router::new()
            .route("/", post(answer))
            .with_state(chain.clone());
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", socket.local_addr()?);
        let task = tokio::spawn(async move {
            let _ = axum::serve(socket, app).await;
        });
        Ok(Self { url, chain, task })
    }

    fn script(&self, edit: impl FnOnce(&mut Chain)) {
        edit(&mut self.chain.lock().unwrap());
    }

    fn methods(&self) -> Vec<String> {
        self.chain.lock().unwrap().methods.clone()
    }

    fn forget_methods(&self) {
        self.chain.lock().unwrap().methods.clear();
    }

    /// Not one `submitblock`, whatever else was asked.
    fn assert_never_offered(&self) {
        let methods = self.methods();
        assert!(
            !methods.iter().any(|method| method == "submitblock"),
            "recover offered a block: {methods:?}"
        );
    }

    /// The plan reads the chain and nothing else: no template, no address,
    /// no genesis check, no offer.
    fn assert_only_read_the_chain(&self) {
        let methods = self.methods();
        assert!(
            !methods.is_empty()
                && methods
                    .iter()
                    .all(|method| method == "getblockheader" || method == "getblockhash"),
            "the plan asked the node more than the chain: {methods:?}"
        );
    }
}

async fn answer(State(chain): State<Arc<Mutex<Chain>>>, Json(request): Json<Value>) -> Json<Value> {
    let method = request["method"].as_str().unwrap_or("<unnamed>").to_owned();
    let (result, error) = {
        let mut chain = chain.lock().unwrap();
        chain.methods.push(method.clone());
        let (tip_height, tip_hash) = chain.tip();
        match method.as_str() {
            "getblockchaininfo" => (
                json!({
                    "chain":"test","initialblockdownload":false,
                    "blocks":tip_height,"headers":tip_height,
                    "bestblockhash":tip_hash,"chainwork":format!("{:016x}", tip_height + 1)
                }),
                Value::Null,
            ),
            "getnetworkinfo" => (json!({"connections": 2}), Value::Null),
            "getbestblockhash" => (json!(tip_hash), Value::Null),
            "getblockhash" => match request["params"][0]
                .as_u64()
                .and_then(|height| chain.active.get(&height))
            {
                Some(hash) => (json!(hash), Value::Null),
                None => (
                    Value::Null,
                    json!({"code":-8,"message":"Block height out of range"}),
                ),
            },
            "getblockheader" if chain.header_error.is_some() => {
                (Value::Null, chain.header_error.clone().unwrap())
            }
            "getblockheader" => match request["params"][0]
                .as_str()
                .and_then(|hash| chain.headers.get(hash).map(|header| (hash, header)))
            {
                Some((hash, (height, parent))) => (
                    json!({"hash":hash,"height":height,"previousblockhash":parent,"confirmations":1}),
                    Value::Null,
                ),
                None => (Value::Null, json!({"code":-5,"message":"Block not found"})),
            },
            _ => (
                Value::Null,
                json!({"code":-32601,"message":"unexpected RPC"}),
            ),
        }
    };
    Json(json!({"id":request["id"],"result":result,"error":error}))
}

/// The real binary against the scripted node. Every `PRISM_`/`QBIT_`
/// variable is stripped first. A plan gets the database URL and the node
/// URL and nothing else: no chain, no seed, no instance. `--apply` gets what
/// a frontend gets, with the seeds the fixtures sign with (so the rebuild is
/// this frontend's) and the named instance the runbook recommends.
async fn recover(db: &Database, node: &ScriptedNode, args: &[&str]) -> Result<Output> {
    recover_at(&db.url, node, args).await
}

async fn recover_at(database_url: &str, node: &ScriptedNode, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    for (key, _) in
        std::env::vars().filter(|(key, _)| key.starts_with("PRISM_") || key.starts_with("QBIT_"))
    {
        command.env_remove(key);
    }
    command
        .args(["candidates", "recover"])
        .args(args)
        .kill_on_drop(true)
        .env("PRISM_DATABASE_URL", database_url)
        .env("QBIT_RPC_URL", &node.url)
        .env("PRISM_RUNTIME_WORKERS", "2");
    if args.contains(&"--apply") {
        command
            .env("QBIT_CHAIN", "testnet")
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_MANIFEST_SIGNING_SEED_HEX", "42".repeat(32))
            .env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", "43".repeat(32))
            .env("PRISM_USERNAME_FALLBACK_ADDRESS", "recovery-test-fallback")
            .env("PRISM_INSTANCE_ID", RECOVERY_INSTANCE);
    }
    Ok(tokio::time::timeout(Duration::from_secs(90), command.output()).await??)
}

/// `--block-hash` for each hash, in the given order.
fn allowlist<'a>(hashes: &[&'a str]) -> Vec<&'a str> {
    hashes
        .iter()
        .flat_map(|hash| ["--block-hash", hash])
        .collect()
}

/// The plan's table rows on stdout, as (hash, height, state, action).
fn planned(output: &Output) -> Vec<(String, String, String, String)> {
    stdout(output)
        .lines()
        .skip(1)
        .filter(|line| !line.starts_with("plan:"))
        .map(|line| {
            let cells: Vec<&str> = line.split_whitespace().collect();
            (
                cells[0].to_owned(),
                cells[1].to_owned(),
                cells[2].to_owned(),
                cells[cells.len() - 1].to_owned(),
            )
        })
        .collect()
}

/// Every outbox row and every pool block, for a byte-identical comparison.
async fn everything(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object('outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY o.block_hash) FROM qbit_block_candidate_outbox o),'blocks',(SELECT jsonb_agg(to_jsonb(b) ORDER BY b.block_hash) FROM qbit_pool_blocks b),'audits',(SELECT jsonb_agg(a.block_hash ORDER BY a.block_hash) FROM qbit_pool_audit_bundles a))",
    )
    .fetch_one(pool)
    .await?)
}

async fn confirmed_block(pool: &PgPool, hash: &str, height: i64) -> Result<()> {
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state) VALUES($1,$2,$3,'coinbase','manifest','confirmed','immature')")
        .bind(hash).bind(height).bind("22".repeat(32)).execute(pool).await?;
    Ok(())
}

/// A candidate the coordinator's rebuild reproduces byte for byte: built
/// with the coinbase suffix the candidate stores and the keys the `--apply`
/// runs sign with, and carrying its as-issued balances so the post-offer
/// landing reads the snapshot however the balances have moved by then.
fn rebuildable(snapshot: &Snapshot, height: u64, nonce: u32) -> Result<TestCandidate> {
    let (coinbase_key, ledger_key) = keys();
    let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: height,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        Some("00".repeat(12)),
        vec![],
        &coinbase_key,
        &ledger_key,
    )?;
    let mut block = candidate_with_bundle(
        bundle,
        WindowRef::from_snapshot(snapshot)?,
        snapshot.payout_revision,
        None,
        nonce,
    )?;
    block.as_issued_balances = snapshot.prior_balances.clone();
    Ok(block)
}

/// The parent every fixture block's header names (`candidate_with_bundle`
/// fills the header's parent with `0x22`).
fn fixture_parent() -> String {
    "22".repeat(32)
}

#[tokio::test]
async fn recover_plan_orders_parent_before_child_without_claiming_or_loading_payloads() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = ScriptedNode::open().await?;
    // Three pending rows, a chain of parents, listed out of height order,
    // and with the middle one held by a live claim and the last one parked.
    let mut first = Row::new("11", "pending");
    first.height = Some(101);
    let mut middle = Row::new("22", "pending");
    middle.height = Some(102);
    middle.state = "reconciliation";
    middle.claim = Some(("frontend-b".to_owned(), 3600.0));
    let mut last = Row::new("33", "pending");
    last.height = Some(103);
    last.parked = true;
    last.last_error = Some("window read and audit rebuild exceeded the 60 s deadline".to_owned());
    for row in [&first, &middle, &last] {
        seed(&ledger.pool, row).await?;
    }
    node.script(|chain| {
        chain.extend(101, &first.hash, &"aa".repeat(32));
        chain.extend(102, &middle.hash, &first.hash);
        chain.extend(103, &last.hash, &middle.hash);
    });
    let before = everything(&ledger.pool).await?;
    let dispatches: i64 =
        sqlx::query_scalar("SELECT last_value FROM qbit_prism_candidate_dispatch_sequence")
            .fetch_one(&ledger.pool)
            .await?;

    // Proof one that the plan cannot write: it runs while the cluster is
    // halted, where every ledger write is refused.
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='test halt' WHERE singleton")
        .execute(&ledger.pool)
        .await?;
    let plan = recover(
        &db,
        &node,
        &allowlist(&[&last.hash, &first.hash, &middle.hash]),
    )
    .await?;
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL WHERE singleton")
        .execute(&ledger.pool)
        .await?;
    assert_eq!(code(&plan), 0, "{}", stderr(&plan));
    let printed = stdout(&plan);
    let rows = planned(&plan);
    assert_eq!(
        rows.iter()
            .map(|(hash, height, state, action)| {
                (
                    hash.as_str(),
                    height.as_str(),
                    state.as_str(),
                    action.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (first.hash.as_str(), "101", "pending", "recover"),
            (middle.hash.as_str(), "102", "reconciliation", "recover"),
            (last.hash.as_str(), "103", "pending", "recover"),
        ],
        "height order, parent before child: {printed}"
    );
    let held = printed
        .lines()
        .find(|line| line.starts_with(&middle.hash))
        .unwrap_or_default();
    assert!(
        held.contains("frontend-b until "),
        "a live claim is reported, not refused, by the plan: {printed}"
    );
    assert!(
        printed
            .ends_with("plan: 3 to recover, 0 already complete; rerun with --apply to land them\n"),
        "{printed}"
    );

    // Proof two: nothing moved, neither a claim nor the retry state nor a
    // dispatch slot.
    assert_eq!(everything(&ledger.pool).await?, before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT last_value FROM qbit_prism_candidate_dispatch_sequence"
        )
        .fetch_one(&ledger.pool)
        .await?,
        dispatches,
        "the plan must consume no dispatch slot"
    );

    // Proof three: neither payload column is a selected column of the
    // statement the plan runs, read from the projection rather than the SQL.
    let described = (&ledger.pool)
        .describe(&qbit_prism_server::ledger::RecoveryReader::sql())
        .await?;
    let columns: Vec<&str> = described
        .columns()
        .iter()
        .map(|column| column.name())
        .collect();
    assert!(
        !columns.contains(&"candidate") && !columns.contains(&"block_bytes"),
        "the plan must not transfer a payload: {columns:?}"
    );
    assert!(columns.contains(&"stored_height"));

    node.assert_only_read_the_chain();
    node.assert_never_offered();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn recover_plan_fails_closed_on_missing_terminal_inactive_legacy_and_unlisted_parents(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = ScriptedNode::open().await?;
    let parent = fixture_parent();
    // One good row, so every refusal below is proven to refuse the whole
    // plan and not only its own hash.
    let good = Row::new("11", "pending");
    seed(&ledger.pool, &good).await?;
    node.script(|chain| chain.extend(101, &good.hash, &parent));

    let abandoned = Row::new("22", "abandoned");
    let mut unaudited = Row::new("33", "submitted");
    unaudited.height = Some(102);
    let mut unlanded = Row::new("34", "submitted");
    unlanded.height = Some(103);
    let mut stale = Row::new("44", "pending");
    stale.height = Some(104);
    let mut unknown = Row::new("55", "pending");
    unknown.height = Some(105);
    let mut orphaned = Row::new("66", "pending");
    orphaned.height = Some(107);
    let mut unlisted_parent = Row::new("77", "offered");
    unlisted_parent.height = Some(106);
    let mut misheight = Row::new("88", "pending");
    misheight.height = Some(150);
    let mut unreadable = Row::new("89", "pending");
    unreadable.height = None;
    let mut chunked = Row::new("99", "pending");
    chunked.storage_version = 2;
    chunked.height = Some(109);
    let mut legacy = Row::new("aa", "pending");
    legacy.shape = Shape::Legacy;
    legacy.height = Some(110);
    for row in [
        &abandoned,
        &unaudited,
        &unlanded,
        &stale,
        &unknown,
        &orphaned,
        &unlisted_parent,
        &misheight,
        &unreadable,
        &chunked,
        &legacy,
    ] {
        seed(&ledger.pool, row).await?;
    }
    confirmed_block(&ledger.pool, &unaudited.hash, 102).await?;
    node.script(|chain| {
        chain.extend(102, &unaudited.hash, &parent);
        chain.extend(103, &unlanded.hash, &parent);
        // The node knows the stale header, but its active chain holds
        // another block at that height.
        chain.extend(104, &"04".repeat(32), &parent);
        chain.side(104, &stale.hash, &parent);
        chain.extend(106, &unlisted_parent.hash, &parent);
        chain.extend(107, &orphaned.hash, &unlisted_parent.hash);
        chain.extend(101, &misheight.hash, &parent);
        chain.extend(108, &unreadable.hash, &parent);
        chain.extend(109, &chunked.hash, &parent);
        chain.extend(110, &legacy.hash, &parent);
    });
    // The chain of `misheight` at 101 displaced `good`; put it back on a
    // height of its own.
    node.script(|chain| {
        chain.extend(101, &good.hash, &parent);
        chain.extend(111, &misheight.hash, &parent);
    });
    let before = everything(&ledger.pool).await?;
    let missing = "ee".repeat(32);

    for (listed, expected_code, expected) in [
        (vec![good.hash.as_str(), missing.as_str()], 2, format!("no candidate row for {missing}")),
        (
            vec![&good.hash, &abandoned.hash],
            4,
            format!("candidate {} is already abandoned; its evidence was released and it cannot be recovered", abandoned.hash),
        ),
        (
            vec![&good.hash, &unaudited.hash],
            4,
            format!("candidate {} is submitted but its accounting is not proven complete (no qbit_pool_audit_bundles row)", unaudited.hash),
        ),
        (
            vec![&good.hash, &unlanded.hash],
            4,
            format!("candidate {} is submitted but its accounting is not proven complete (no qbit_pool_blocks row, no qbit_pool_audit_bundles row)", unlanded.hash),
        ),
        (
            vec![&good.hash, &stale.hash],
            9,
            format!("candidate {} is not on the active chain (the node's active chain holds {} at height 104); nothing to recover. recover never offers a block", stale.hash, "04".repeat(32)),
        ),
        (
            vec![&good.hash, &unknown.hash],
            9,
            format!("candidate {} is not on the active chain (the node has no such block: Block not found)", unknown.hash),
        ),
        (
            vec![&good.hash, &orphaned.hash],
            10,
            format!("candidate {} has an unfinished parent {} (offered) that is not in the allowlist; add --block-hash {} so it lands first", orphaned.hash, unlisted_parent.hash, unlisted_parent.hash),
        ),
        (
            vec![&good.hash, &misheight.hash],
            10,
            format!("candidate {} is stored at height 150 but the node holds it at height 111", misheight.hash),
        ),
        (
            vec![&good.hash, &unreadable.hash],
            10,
            format!("candidate {} has no readable stored height", unreadable.hash),
        ),
        (
            vec![&good.hash, &chunked.hash],
            7,
            format!("candidate {} has unsupported storage_version 2; evidence preserved", chunked.hash),
        ),
        (
            vec![&good.hash, &legacy.hash],
            8,
            format!("candidate {} holds a pre-migration 2.x.x document at storage_version 1; evidence preserved", legacy.hash),
        ),
    ] {
        let refused = recover(&db, &node, &allowlist(&listed)).await?;
        assert_eq!(code(&refused), expected_code, "{listed:?}: {}", stderr(&refused));
        assert!(
            stderr(&refused).contains(&expected),
            "{listed:?}: expected {expected:?} in {}",
            stderr(&refused)
        );
        // The good row was planned and printed, and yet nothing was
        // applied: the whole selection failed.
        assert!(stdout(&refused).contains(&good.hash), "{}", stdout(&refused));
        assert!(!stdout(&refused).contains("plan:"), "{}", stdout(&refused));
    }
    // The parent rule is satisfied by listing the parent, whatever the order.
    let ordered = recover(
        &db,
        &node,
        &allowlist(&[&orphaned.hash, &unlisted_parent.hash]),
    )
    .await?;
    assert_eq!(code(&ordered), 0, "{}", stderr(&ordered));
    assert_eq!(
        planned(&ordered)
            .iter()
            .map(|(hash, ..)| hash.as_str())
            .collect::<Vec<_>>(),
        vec![unlisted_parent.hash.as_str(), orphaned.hash.as_str()]
    );

    // Every problem is reported, and the first one's status is the exit
    // status, in allowlist order.
    let several = recover(
        &db,
        &node,
        &allowlist(&[&abandoned.hash, &missing, &stale.hash]),
    )
    .await?;
    assert_eq!(code(&several), 4, "{}", stderr(&several));
    let message = stderr(&several);
    assert!(message.contains("is already abandoned"), "{message}");
    assert!(message.contains("no candidate row for"), "{message}");
    assert!(message.contains("is not on the active chain"), "{message}");

    // A node failure is not proof that an accepted block is missing, in
    // either a plan or --apply. Preserve the RPC diagnostic and every row.
    for error in [
        json!({"code": -28, "message": "Loading block index"}),
        json!({"code": -32603, "message": "Internal error"}),
        json!({"message": "No error code"}),
        json!({"code": "-5", "message": "Invalid error code"}),
    ] {
        node.script(|chain| chain.header_error = Some(error.clone()));
        for apply in [false, true] {
            let mut args = allowlist(&[&good.hash]);
            if apply {
                args.push("--apply");
            }
            let failed = recover(&db, &node, &args).await?;
            assert_eq!(code(&failed), 1, "{}", stderr(&failed));
            let message = stderr(&failed);
            assert!(message.contains("qbit RPC getblockheader"), "{message}");
            assert!(
                message.contains(error["message"].as_str().unwrap()),
                "{message}"
            );
            assert!(!message.contains("nothing to recover"), "{message}");
            assert!(!message.contains("may be abandoned"), "{message}");
        }
    }
    node.script(|chain| chain.header_error = None);

    // Malformed, duplicate and out-of-range allowlists are refused at the
    // entry boundary, before any connection: the node is not asked.
    node.forget_methods();
    let too_many: Vec<String> = (0..33).map(|n| format!("{n:064x}")).collect();
    let too_many: Vec<&str> = too_many.iter().map(String::as_str).collect();
    for (args, expected) in [
        (
            allowlist(&[&good.hash, &good.hash]),
            "listed more than once",
        ),
        (allowlist(&too_many), "at most 32 times"),
        (allowlist(&["abc"]), "--block-hash must be"),
        (allowlist(&[&"AB".repeat(32)]), "--block-hash must be"),
        (vec![], "--block-hash"),
        (
            vec!["--block-hash", &good.hash, "--timeout-seconds", "0"],
            "timeout-seconds",
        ),
        (
            vec!["--block-hash", &good.hash, "--timeout-seconds", "3601"],
            "timeout-seconds",
        ),
    ] {
        let output = recover(&db, &node, &args).await?;
        assert!(!output.status.success(), "{args:?} was accepted");
        assert!(
            stderr(&output).contains(expected),
            "{args:?}: {}",
            stderr(&output)
        );
    }
    assert!(
        node.methods().is_empty(),
        "a usage error asked the node: {:?}",
        node.methods()
    );

    assert_eq!(
        everything(&ledger.pool).await?,
        before,
        "a refused plan must write nothing"
    );
    node.assert_never_offered();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn recover_apply_lands_the_listed_blocks_and_verifies_completed_ones_idempotently(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = ScriptedNode::open().await?;
    ledger.append(share(1), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    // A pending block the coordinator never reached, a block whose landing
    // the lane gave up on, and a third unfinished block that is not listed.
    let pending = rebuildable(&snapshot, 101, 418)?;
    let stuck = rebuildable(&snapshot, 102, 419)?;
    let unlisted = rebuildable(&snapshot, 103, 420)?;
    for block in [&pending, &stuck, &unlisted] {
        ledger.enqueue_candidate(block.candidate.clone()).await?;
    }
    sqlx::query("UPDATE qbit_block_candidate_outbox SET state='reconciliation',offer_reserved_at=clock_timestamp(),offer_reserved_by='frontend-b',offer_outcome='unknown',attempt_count=7,last_error='post-offer processing failed (offer outcome unknown): window read and audit rebuild exceeded the 60 s deadline',next_attempt_at=clock_timestamp()+interval '1 hour' WHERE block_hash=$1")
        .bind(&stuck.block_hash).execute(&ledger.pool).await?;
    let parent = fixture_parent();
    node.script(|chain| {
        chain.extend(101, &pending.block_hash, &parent);
        chain.extend(102, &stuck.block_hash, &parent);
        chain.extend(103, &unlisted.block_hash, &parent);
    });
    let unlisted_before = whole_row(&ledger.pool, &unlisted.block_hash).await?;
    let listed = allowlist(&[&stuck.block_hash, &pending.block_hash]);

    // The plan, first: both to recover, in height order.
    let plan = recover(&db, &node, &listed).await?;
    assert_eq!(code(&plan), 0, "{}", stderr(&plan));
    assert_eq!(
        planned(&plan)
            .iter()
            .map(|(hash, _, state, action)| (hash.as_str(), state.as_str(), action.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (pending.block_hash.as_str(), "pending", "recover"),
            (stuck.block_hash.as_str(), "reconciliation", "recover"),
        ]
    );
    node.assert_only_read_the_chain();
    node.forget_methods();

    let mut apply = listed.clone();
    apply.push("--apply");
    let applied = recover(&db, &node, &apply).await?;
    assert_eq!(code(&applied), 0, "{}", stderr(&applied));
    let printed = stdout(&applied);
    assert!(
        printed.ends_with(&format!(
            "recovering {p} at height 101 from pending\nrecovered {p} at height 101\nrecovering {s} at height 102 from reconciliation\nrecovered {s} at height 102\nrecovered 2, verified 0 already complete\n",
            p = pending.block_hash,
            s = stuck.block_hash
        )),
        "{printed}"
    );
    node.assert_never_offered();
    assert!(
        node.methods()
            .iter()
            .any(|method| method == "getblockheader"),
        "the apply verified the header against the node: {:?}",
        node.methods()
    );

    for (block, adopted) in [(&pending, true), (&stuck, false)] {
        let row = whole_row(&ledger.pool, &block.block_hash).await?;
        assert_eq!(row["state"], "submitted", "{row}");
        assert_eq!(
            row["candidate"],
            Value::Null,
            "the document is released on a finished row"
        );
        assert_eq!(row["claim_token"], Value::Null);
        assert_eq!(row["last_error"], Value::Null);
        // The offer record is the evidence of how the row got here: the
        // pending block was adopted by this recovery with the node's
        // evidence, never offered; the stuck one keeps its own record.
        assert_eq!(row["offer_outcome"], "unknown", "{row}");
        if adopted {
            assert_eq!(row["offer_reserved_by"], RECOVERY_INSTANCE, "{row}");
            assert!(
                row["offer_reply"]
                    .as_str()
                    .is_some_and(|reply| reply.contains("node reports block")
                        && reply.contains("active at height 101")),
                "{row}"
            );
        } else {
            assert_eq!(row["offer_reserved_by"], "frontend-b", "{row}");
        }
        assert_eq!(
            row["offered_at_ms"],
            Value::Null,
            "no offer time is ever fabricated"
        );
        let block_row: (String, i64) = sqlx::query_as(
            "SELECT chain_state,block_height FROM qbit_pool_blocks WHERE block_hash=$1",
        )
        .bind(&block.block_hash)
        .fetch_one(&ledger.pool)
        .await?;
        assert_eq!(
            block_row,
            (
                "confirmed".to_owned(),
                block.found_block.block_height as i64
            )
        );
        assert!(ledger.audit_bundle(&block.block_hash).await?.is_some());
        let payouts: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_pool_payout_entries WHERE block_hash=$1")
                .bind(&block.block_hash)
                .fetch_one(&ledger.pool)
                .await?;
        assert!(payouts > 0, "the landing wrote the block's payout entries");
    }
    // Exactly the listed hashes: the third block is untouched.
    assert_eq!(
        whole_row(&ledger.pool, &unlisted.block_hash).await?,
        unlisted_before
    );
    assert_no_new_instances(&ledger.pool).await?;

    // The same allowlist again: verified and skipped, nothing written.
    let before = everything(&ledger.pool).await?;
    node.forget_methods();
    let again = recover(&db, &node, &apply).await?;
    assert_eq!(code(&again), 0, "{}", stderr(&again));
    let printed = stdout(&again);
    assert!(
        printed.ends_with(&format!(
            "verified {p} at height 101: already complete\nverified {s} at height 102: already complete\nrecovered 0, verified 2 already complete\n",
            p = pending.block_hash,
            s = stuck.block_hash
        )),
        "{printed}"
    );
    assert!(printed.contains("complete"), "{printed}");
    assert_eq!(everything(&ledger.pool).await?, before);
    let replanned = recover(&db, &node, &listed).await?;
    assert_eq!(code(&replanned), 0, "{}", stderr(&replanned));
    assert!(
        stdout(&replanned)
            .ends_with("plan: nothing to recover; every listed block is already complete\n"),
        "{}",
        stdout(&replanned)
    );
    node.assert_never_offered();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn recover_apply_refuses_a_live_foreign_claim_and_leaves_the_row_untouched() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = ScriptedNode::open().await?;
    ledger.append(share(1), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let block = rebuildable(&snapshot, 101, 422)?;
    let hash = block.block_hash.clone();
    ledger.enqueue_candidate(block.candidate.clone()).await?;
    // Another frontend holds the row for the next hour.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_token='token-frontend-b',claim_instance_id='frontend-b',claim_expires_at=clock_timestamp()+interval '1 hour',attempt_count=1 WHERE block_hash=$1")
        .bind(&hash).execute(&ledger.pool).await?;
    node.script(|chain| chain.extend(101, &hash, &fixture_parent()));
    let before = whole_row(&ledger.pool, &hash).await?;
    let mut apply = allowlist(&[&hash]);
    apply.push("--apply");

    let refused = recover(&db, &node, &apply).await?;
    assert_eq!(code(&refused), 5, "{}", stderr(&refused));
    let message = stderr(&refused);
    assert!(
        message.contains(&format!("candidate {hash} is held by frontend-b until ")),
        "{message}"
    );
    assert!(
        message.contains("retry after the claim expires"),
        "{message}"
    );
    // The plan ran and named the holder; the apply stopped at the claim.
    assert!(
        stdout(&refused).contains("frontend-b until "),
        "{}",
        stdout(&refused)
    );
    assert!(stdout(&refused).contains(&format!("recovering {hash} at height 101")));
    assert_eq!(whole_row(&ledger.pool, &hash).await?, before);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_hash=$1)"
        )
        .bind(&hash)
        .fetch_one(&ledger.pool)
        .await?,
        "nothing landed under a foreign claim"
    );

    // An expired claim is not a live claim: the row is claimed and finished.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1")
        .bind(&hash).execute(&ledger.pool).await?;
    let done = recover(&db, &node, &apply).await?;
    assert_eq!(code(&done), 0, "{}", stderr(&done));
    let after = whole_row(&ledger.pool, &hash).await?;
    assert_eq!(after["state"], "submitted", "{after}");
    assert_eq!(after["offer_reserved_by"], RECOVERY_INSTANCE);

    // A row this binary cannot authenticate (the seeded block bytes are not
    // the block) is claimed, released with the reason, and neither parked
    // nor abandoned: the operator named it, the operator decides. Its
    // document names this frontend's keys, as a row the startup gate lets a
    // frontend pin its fingerprint beside must.
    let mut forged = Row::new("11", "pending");
    forged.height = Some(102);
    seed(&ledger.pool, &forged).await?;
    let (coinbase_key, ledger_key) = keys();
    sqlx::query("UPDATE qbit_block_candidate_outbox SET candidate=jsonb_set(candidate,'{signer_keys}',$2::jsonb) WHERE block_hash=$1")
        .bind(&forged.hash)
        .bind(serde_json::to_value(SignerKeys::of(&coinbase_key, &ledger_key))?)
        .execute(&ledger.pool)
        .await?;
    node.script(|chain| chain.extend(102, &forged.hash, &fixture_parent()));
    let before = whole_row(&ledger.pool, &forged.hash).await?;
    let mut apply = allowlist(&[&forged.hash]);
    apply.push("--apply");
    let invalid = recover(&db, &node, &apply).await?;
    assert_eq!(code(&invalid), 12, "{}", stderr(&invalid));
    assert!(
        stderr(&invalid).contains(&format!("recovery of {} stopped: ", forged.hash)),
        "{}",
        stderr(&invalid)
    );
    assert!(stderr(&invalid).contains("the candidate was left recoverable"));
    let after = whole_row(&ledger.pool, &forged.hash).await?;
    assert_eq!(after["state"], "pending");
    assert_eq!(after["claim_token"], Value::Null, "the claim was released");
    assert_eq!(after["claim_instance_id"], Value::Null);
    assert_eq!(
        after["attempt_count"],
        json!(before["attempt_count"].as_i64().unwrap() + 1),
        "the attempt was counted, as every claim is"
    );
    assert!(
        after["last_error"]
            .as_str()
            .is_some_and(|reason| reason.contains("could not authenticate the row")),
        "{after}"
    );
    assert_eq!(
        after["next_attempt_at"], before["next_attempt_at"],
        "the schedule is untouched"
    );
    assert_eq!(
        after["candidate"], before["candidate"],
        "the evidence is untouched"
    );
    assert_eq!(after["block_bytes"], before["block_bytes"]);

    node.assert_never_offered();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

/// COMMIT is durable but its reply has not reached the claimant: the
/// deadline must release that attempt's token without touching a new owner.
#[tokio::test]
async fn recover_apply_releases_a_committed_claim_when_its_reply_times_out() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = ScriptedNode::open().await?;
    ledger.append(share(1), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    sqlx::raw_sql(
        "CREATE FUNCTION mark_recovery_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_block_candidate_outbox UPDATE'; RETURN NEW; END $$; CREATE TRIGGER mark_recovery_claim AFTER UPDATE OF claim_token ON qbit_block_candidate_outbox FOR EACH ROW EXECUTE FUNCTION mark_recovery_claim();",
    )
    .execute(&ledger.pool)
    .await?;
    let raw = url::Url::parse(&db.url)?;
    let upstream = tokio::net::lookup_host((
        raw.host_str().context("database URL names a host")?,
        raw.port().unwrap_or(5432),
    ))
    .await?
    .next()
    .context("database host resolves")?;
    let proxy = execution_proxy::ExecutionProxy::start(upstream).await?;
    let proxied_url = proxy.rewrite_url(&db.url)?;

    for (nonce, replace_claim) in [(422, false), (423, true)] {
        let block = rebuildable(&snapshot, 101, nonce)?;
        let hash = block.block_hash.clone();
        ledger.enqueue_candidate(block.candidate.clone()).await?;
        node.script(|chain| chain.extend(101, &hash, &fixture_parent()));
        let before = whole_row(&ledger.pool, &hash).await?;
        let pause = proxy.pause_after_commit("qbit_block_candidate_outbox", "UPDATE")?;
        let mut args = allowlist(&[&hash]);
        args.extend(["--apply", "--timeout-seconds", "4"]);
        let run = recover_at(&proxied_url, &node, &args);
        tokio::pin!(run);
        tokio::time::timeout(Duration::from_secs(15), async {
            tokio::select! {
                _ = pause.entered() => Ok(()),
                output = &mut run => bail!("recovery ended before its committed claim was paused: {}", stderr(&output?)),
            }
        })
        .await
        .context("the claim COMMIT did not reach the reply barrier")??;
        // Read directly, bypassing the proxy, to prove the claim really
        // committed before the command's deadline fires.
        let claimed = whole_row(&ledger.pool, &hash).await?;
        assert!(claimed["claim_token"].is_string(), "{claimed}");
        assert_eq!(claimed["claim_instance_id"], RECOVERY_INSTANCE);
        assert_eq!(
            claimed["attempt_count"],
            before["attempt_count"].as_i64().unwrap() + 1
        );
        let replacement = if replace_claim {
            sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_token=$2,claim_instance_id='replacement-owner',claim_expires_at=clock_timestamp()+interval '120 seconds' WHERE block_hash=$1")
                .bind(&hash).bind(Uuid::new_v4().to_string()).execute(&ledger.pool).await?;
            Some(whole_row(&ledger.pool, &hash).await?)
        } else {
            None
        };
        let expired = tokio::time::timeout(Duration::from_secs(20), &mut run).await??;
        assert_eq!(code(&expired), 11, "{}", stderr(&expired));
        assert!(
            stderr(&expired).contains("exceeded (claiming)"),
            "{}",
            stderr(&expired)
        );
        assert!(
            stderr(&expired).contains("is no longer claimed by this attempt"),
            "{}",
            stderr(&expired)
        );
        let after = whole_row(&ledger.pool, &hash).await?;
        if let Some(replacement) = replacement {
            assert_eq!(after, replacement, "cleanup changed another owner's row");
        } else {
            for field in ["claim_token", "claim_instance_id", "claim_expires_at"] {
                assert_eq!(after[field], Value::Null, "{field}: {after}");
            }
            for field in ["state", "candidate", "block_bytes", "next_attempt_at"] {
                assert_eq!(after[field], before[field], "cleanup changed {field}");
            }
            assert!(after["last_error"]
                .as_str()
                .unwrap()
                .contains("the recovery deadline expired"));
        }
        pause.release();
        if !replace_claim {
            // No wait for the old 120-second lease: the same row can be
            // recovered immediately after the timed-out command exits.
            let mut resumed = allowlist(&[&hash]);
            resumed.push("--apply");
            let done = recover(&db, &node, &resumed).await?;
            assert_eq!(code(&done), 0, "{}", stderr(&done));
        }
    }
    proxy.finish().await?;
    node.assert_never_offered();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

/// Pause the real landing inside the database, after its block insert, past
/// the operator's deadline.
#[tokio::test]
async fn recover_apply_enforces_the_deadline_and_leaves_the_candidate_recoverable() -> Result<()> {
    const LANDING_GATE: i64 = 0x41800001;
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = ScriptedNode::open().await?;
    ledger.append(share(1), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let block = rebuildable(&snapshot, 101, 421)?;
    let hash = block.block_hash.clone();
    ledger.enqueue_candidate(block.candidate.clone()).await?;
    node.script(|chain| chain.extend(101, &hash, &fixture_parent()));
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION pause_candidate_landing() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({LANDING_GATE}); RETURN NEW; END $$; CREATE TRIGGER pause_candidate_landing AFTER INSERT ON qbit_pool_blocks FOR EACH ROW EXECUTE FUNCTION pause_candidate_landing();"
    ))
    .execute(&ledger.pool)
    .await?;
    let mut gate = ledger.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(LANDING_GATE)
        .execute(&mut *gate)
        .await?;

    let mut apply = allowlist(&[&hash]);
    apply.extend(["--apply", "--timeout-seconds", "4"]);
    let started = std::time::Instant::now();
    let expired = recover(&db, &node, &apply).await?;
    assert_eq!(code(&expired), 11, "{}", stderr(&expired));
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the deadline and its bounded cleanup took {:?}",
        started.elapsed()
    );
    assert!(
        stderr(&expired).contains(&format!(
            "recovery deadline of 4 seconds exceeded (landing); candidate {hash} was left recoverable and its claim released"
        )),
        "{}",
        stderr(&expired)
    );
    // The claim was released before the paused transaction could finish:
    // its row lock is compatible with a landing in flight.
    let row = whole_row(&ledger.pool, &hash).await?;
    assert_eq!(row["claim_token"], Value::Null, "{row}");
    assert_eq!(row["claim_instance_id"], Value::Null);
    assert!(
        row["last_error"]
            .as_str()
            .is_some_and(|reason| reason.contains("the recovery deadline expired")),
        "{row}"
    );
    // Adopted before the landing began: never offered again by anyone, and
    // still unfinished.
    assert_eq!(row["state"], "reconciliation", "{row}");
    assert_eq!(row["offer_reserved_by"], RECOVERY_INSTANCE);
    assert!(row["candidate"].is_object(), "the evidence is kept");

    // Let the paused landing go: its process is gone, so it rolls back, and
    // no block was committed.
    gate.commit().await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let landing_alive: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state<>'idle' AND query LIKE 'INSERT INTO qbit_pool_blocks%')",
            )
            .fetch_one(&ledger.pool)
            .await?;
            if !landing_alive {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("the paused landing did not end")??;
    let landed: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_hash=$1)")
            .bind(&hash)
            .fetch_one(&ledger.pool)
            .await?;
    assert!(
        !landed,
        "the landing the deadline cut short must not commit"
    );
    sqlx::query("DROP TRIGGER pause_candidate_landing ON qbit_pool_blocks")
        .execute(&ledger.pool)
        .await?;

    // The same allowlist, with an adequate deadline, resumes and finishes.
    let mut resumed = allowlist(&[&hash]);
    resumed.push("--apply");
    let done = recover(&db, &node, &resumed).await?;
    assert_eq!(code(&done), 0, "{}", stderr(&done));
    assert!(
        stdout(&done).contains(&format!(
            "recovering {hash} at height 101 from reconciliation"
        )),
        "{}",
        stdout(&done)
    );
    let row = whole_row(&ledger.pool, &hash).await?;
    assert_eq!(row["state"], "submitted", "{row}");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1"
        )
        .bind(&hash)
        .fetch_one(&ledger.pool)
        .await?,
        "confirmed"
    );
    node.assert_never_offered();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

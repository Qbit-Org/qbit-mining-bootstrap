//! The operator candidate commands (#268): the `candidates list` inventory and
//! the one `candidates abandon` an operator may perform.
//!
//! Every test here runs the real binary against a counting proxy in front of
//! the fake node, and asserts that the process made **no** RPC call at all —
//! `submitblock` included — and registered no instance. Neither command is
//! given a chain, a signing seed or a fallback address, so a command that
//! loaded `Config` or started a listener could not have succeeded.
use super::*;
use qbit_prism_server::ledger::{HeartbeatHealth, HeartbeatStatus};
use serde_json::Value;
use sqlx::{Column, Executor};
use std::{
    process::Output,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::process::Command;

use super::fake_qbitd as fake;

/// The fake node behind a recorder. "Zero `submitblock` calls" is asserted
/// against a real socket the child process could have reached, not against
/// the absence of a call site.
struct CountingNode {
    url: String,
    methods: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
    _upstream: fake::FakeNode,
}

impl Drop for CountingNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CountingNode {
    async fn open() -> Result<Self> {
        use axum::{extract::State, routing::post, Json, Router};
        let upstream = fake::FakeNode::open().await?;
        let methods: Arc<Mutex<Vec<String>>> = Arc::default();
        let state = (upstream.url.clone(), methods.clone());
        let app = Router::new()
            .route(
                "/",
                post(
                    |State((upstream, methods)): State<(String, Arc<Mutex<Vec<String>>>)>,
                     Json(request): Json<Value>| async move {
                        methods
                            .lock()
                            .unwrap()
                            .push(request["method"].as_str().unwrap_or("<unnamed>").to_owned());
                        Json(
                            reqwest::Client::new()
                                .post(&upstream)
                                .json(&request)
                                .send()
                                .await
                                .unwrap()
                                .json::<Value>()
                                .await
                                .unwrap(),
                        )
                    },
                ),
            )
            .with_state(state);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", socket.local_addr()?);
        let task = tokio::spawn(async move {
            let _ = axum::serve(socket, app).await;
        });
        Ok(Self {
            url,
            methods,
            task,
            _upstream: upstream,
        })
    }

    /// No RPC of any kind, so in particular no `submitblock`.
    fn assert_never_reached(&self) {
        assert_eq!(
            self.methods.lock().unwrap().clone(),
            Vec::<String>::new(),
            "the candidate commands must never call the node"
        );
    }
}

/// The real binary with the database URL, a node URL and nothing else. Every
/// `PRISM_`/`QBIT_` variable is stripped first: the child has no chain, no
/// signing seeds and no fallback address.
async fn cli(db: &Database, node: &CountingNode, args: &[&str]) -> Result<Output> {
    cli_with_env(db, node, args, &[]).await
}

/// `cli` with extra environment, for a configured `PRISM_INSTANCE_ID`.
/// `extra` is applied last, so it overrides.
async fn cli_with_env(
    db: &Database,
    node: &CountingNode,
    args: &[&str],
    extra: &[(&str, &str)],
) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    for (key, _) in
        std::env::vars().filter(|(key, _)| key.starts_with("PRISM_") || key.starts_with("QBIT_"))
    {
        command.env_remove(key);
    }
    command
        .args(args)
        .kill_on_drop(true)
        .env("PRISM_DATABASE_URL", &db.url)
        .env("QBIT_RPC_URL", &node.url)
        .env("PRISM_RUNTIME_WORKERS", "2");
    for (key, value) in extra {
        command.env(key, value);
    }
    Ok(tokio::time::timeout(Duration::from_secs(20), command.output()).await??)
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The exit status decision 4 assigns to this outcome.
fn code(output: &Output) -> i32 {
    output.status.code().expect("the command was signalled")
}

/// Which candidate document a seeded row carries. `storage_version = 1`
/// spans all three: the native claim lane parks a pre-migration 2.x.x
/// document at version 1 rather than rewriting it, which is exactly why
/// `abandon` cannot read the version alone (#425).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// What this release writes since 007: the `window` reference beside
    /// `payout_revision` and `block_hash`, with the block out in
    /// `block_bytes`. The field names are `ledger::Candidate`'s.
    NativeWindow,
    /// What native 3.x.x wrote before 007: the same identity fields with the
    /// audit bundle still inline. Migration 007 refuses a pending row in this
    /// shape, so it reaches `abandon` only on a database that never migrated;
    /// the migrator counts it as native, and so must this guard.
    NativeBundle,
    /// The 2.x.x Python writer's `qbit.prism.block-candidate-intent.v1`
    /// document (`lab/prism/block_candidates.py` at v2.0.2). It names the
    /// block `block_hash_hex`, carries no `payout_revision`, and has neither
    /// a `bundle` nor a `window`: the shape the migrator refuses as undrained
    /// legacy work. Only ever seeded `pending`, which is where the claim lane
    /// parks one; 011 requires the four 007 payload columns on an offered row,
    /// and a 2.x.x writer never wrote them.
    Legacy,
}

/// One seeded outbox row. The defaults are an ordinary retrying `pending`
/// row; each test changes only the fields its case is about.
#[derive(Clone)]
struct Row {
    hash: String,
    state: &'static str,
    height: Option<i64>,
    shape: Shape,
    storage_version: i32,
    attempt_count: i32,
    last_error: Option<String>,
    parked: bool,
    due_in_seconds: f64,
    claim: Option<(String, f64)>,
    proof_observed_at_ms: Option<i64>,
}

impl Row {
    fn new(byte: &str, state: &'static str) -> Self {
        Self {
            hash: byte.repeat(32),
            state,
            height: Some(101),
            shape: Shape::NativeWindow,
            storage_version: 1,
            attempt_count: 0,
            last_error: None,
            parked: false,
            due_in_seconds: 30.0,
            claim: None,
            proof_observed_at_ms: Some(1_800_000_000_000),
        }
    }
}

/// The `candidate` document a seeded row carries, in the shape its writer
/// really produced. `abandon` reads the document itself — the identity fields
/// beside a `bundle` or a `window` are what tell a row this release wrote
/// from a parked legacy one — so a fixture that carried only `found_block`
/// could not tell the guard's two cases apart.
fn candidate_document(row: &Row) -> Value {
    // `found_block` is `qbit_prism::FoundBlock`; the height is the one field
    // `candidates list` projects, and `None` is the unreadable-height case.
    let found_block = match row.height {
        Some(height) => json!({
            "block_height": height,
            "coinbase_value_sats": 5_000_000_000i64,
            "network_difficulty": 1_048_576i64,
            "anchor_job_issued_at_ms": 1_800_000_000_000i64,
        }),
        // A document an unknown writer left behind: no readable height.
        None => json!({"coinbase_value_sats": 5_000_000_000i64}),
    };
    match row.shape {
        Shape::NativeWindow | Shape::NativeBundle => {
            let mut document = json!({
                "block_hash": row.hash,
                "block_sha256": "cc".repeat(32),
                "job_id": "job",
                "payout_revision": 7,
                "found_block": found_block,
                "payout_policy": {
                    "p2mr_spend_input_bytes": 68,
                    "target_feerate_sats_per_byte": 2,
                    "safety_multiplier": 2,
                },
                "audit_builder_version": 1,
                "signer_keys": {
                    "manifest_key_hex": "02".to_owned() + &"11".repeat(32),
                    "ledger_key_hex": "03".to_owned() + &"22".repeat(32),
                },
                "leased": false,
                "coinbase_suffix_hex": "00".repeat(12),
            });
            match row.shape {
                // The window columns this row is seeded with, restated in the
                // document the way an enqueue writes them: an empty window
                // carries no share range, so `shares` is null here and the
                // four share columns stay NULL on the row.
                Shape::NativeWindow => {
                    document["window"] = json!({
                        "anchor_ms": 1_800_000_000_000i64,
                        "prior_balances_digest": "aa".repeat(32),
                        "shares": null,
                    });
                }
                _ => document["bundle"] = json!({"schema": "qbit.prism.audit-bundle.v1"}),
            }
            document
        }
        // Verbatim key set of `block_candidate_intent` at v2.0.2 (#258),
        // trimmed to the fixed-metadata fields: no share list is needed to
        // prove the shape, and none of the omitted keys is an identity field.
        Shape::Legacy => json!({
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": row.hash,
            "block_hex": "00".repeat(80),
            "coinbase_tx_hex": "01".repeat(32),
            "parent_hash": "dd".repeat(32),
            "expected_height": row.height,
            "template": {
                "previousblockhash": "dd".repeat(32),
                "height": row.height,
                "coinbasevalue": 5_000_000_000i64,
            },
            "shares_json": [],
            "prior_balances": [],
            "found_block": found_block,
            "witness_merkle_leaves_hex": [],
            "extranonce1_hex": "deadbeef",
            "extranonce2_hex": "00000000",
            "username": "qbit1legacyminer",
            "pending_share": {"job_id": "job", "share_id": "share-1"},
            "credit_share_on_accept": true,
            "collection_only": false,
        }),
    }
}

/// Seed `row` exactly as migration 011's lifecycle, payload and offer rules
/// require for its state, so all four unfinished states and both terminal
/// states can be held at once without driving six claims.
async fn seed(pool: &PgPool, row: &Row) -> Result<()> {
    let terminal = matches!(row.state, "submitted" | "abandoned");
    let offered = matches!(row.state, "offer_reserved" | "offered" | "reconciliation");
    // `block_bytes` and the six window columns arrived with 007. A parked
    // 2.x.x row and a pre-007 native row both predate them and keep their
    // block inside their own document, so seeding either with those columns
    // would not be the row an operator meets. 011 requires all four on an
    // offered row, which is why only `pending` rows take the older shapes.
    let window_payload = !terminal && row.shape == Shape::NativeWindow;
    let candidate = (!terminal).then(|| candidate_document(row));
    let last_error = match (row.state, &row.last_error) {
        // 011 requires a nonblank reason on a reconciliation row.
        ("reconciliation", None) => Some("node answer was lost in transport".to_owned()),
        (_, other) => other.clone(),
    };
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox(block_hash,candidate_sha256,candidate,block_bytes,state,storage_version,attempt_count,last_error,next_attempt_at,completed_at,window_anchor_ms,window_prior_balances_sha256,offer_reserved_at,offer_reserved_by,offered_at_ms,offer_outcome,proof_observed_at_ms,claim_token,claim_instance_id,claim_expires_at) \
         VALUES($1,$1,$2,$3,$4,$5,$6,$7,\
         CASE WHEN $8 THEN 'infinity'::timestamptz ELSE clock_timestamp()+make_interval(secs=>$9) END,\
         CASE WHEN $4 IN ('submitted','abandoned') THEN clock_timestamp() END,$10,$11,\
         CASE WHEN $12::text IS NOT NULL THEN clock_timestamp() END,$12,$13,$14,$15,\
         CASE WHEN $16::text IS NOT NULL THEN 'token-'||$16 END,$16,\
         CASE WHEN $16::text IS NOT NULL THEN clock_timestamp()+make_interval(secs=>$17) END)")
        .bind(&row.hash)
        .bind(candidate)
        .bind(window_payload.then(|| vec![0u8; 80]))
        .bind(row.state)
        .bind(row.storage_version)
        .bind(row.attempt_count)
        .bind(last_error)
        .bind(row.parked)
        .bind(row.due_in_seconds)
        .bind(window_payload.then_some(1_800_000_000_000i64))
        .bind(window_payload.then(|| "aa".repeat(32)))
        .bind(offered.then(|| "frontend-b".to_owned()))
        .bind((row.state == "offered").then_some(1_800_000_001_000i64))
        .bind(match row.state {
            "offered" => Some("accepted"),
            "reconciliation" => Some("unknown"),
            _ => None,
        })
        .bind(row.proof_observed_at_ms)
        .bind(row.claim.as_ref().map(|(instance, _)| instance.clone()))
        .bind(row.claim.as_ref().map_or(0.0, |(_, seconds)| *seconds))
        .execute(pool)
        .await?;
    Ok(())
}

/// The whole row, for a byte-identical comparison across a refusal.
async fn whole_row(pool: &PgPool, hash: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?)
}

async fn landed_block(pool: &PgPool, hash: &str) -> Result<()> {
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state) VALUES($1,101,'parent','coinbase','manifest','prepared','immature')")
        .bind(hash).execute(pool).await?;
    Ok(())
}

/// The one document `--json` prints, checked for its versioned schema field.
fn listed(output: &Output) -> Vec<Value> {
    let document: Value = serde_json::from_slice(&output.stdout).expect("candidates list --json");
    assert_eq!(document["schema"], "qbit.prism.candidates.list.v1");
    document["candidates"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

fn row_of<'a>(listed: &'a [Value], hash: &str) -> &'a Value {
    listed
        .iter()
        .find(|row| row["block_hash"] == hash)
        .unwrap_or_else(|| panic!("{hash} is missing from the inventory"))
}

/// Operator tools never register a frontend, so the only instance row is the
/// ledger this test opened.
async fn assert_no_new_instances(pool: &PgPool) -> Result<()> {
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_instances")
            .fetch_one(pool)
            .await?,
        1,
        "operator tools must not create heartbeats"
    );
    Ok(())
}

#[tokio::test]
async fn list_shows_every_unfinished_state_without_claiming_or_loading_payloads() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    let mut pending = Row::new("11", "pending");
    pending.attempt_count = 4;
    pending.last_error = Some("node RPC transport failed".to_owned());
    let mut reserved = Row::new("22", "offer_reserved");
    reserved.height = Some(102);
    reserved.claim = Some(("frontend-b".to_owned(), 3600.0));
    let mut offered = Row::new("33", "offered");
    offered.height = Some(103);
    // A claim whose owner died: the row is workable again, and `list` must
    // say `expired` rather than drop the holder.
    offered.claim = Some(("frontend-c".to_owned(), -60.0));
    let mut reconciliation = Row::new("44", "reconciliation");
    reconciliation.height = Some(104);
    reconciliation.attempt_count = 9;
    for row in [&pending, &reserved, &offered, &reconciliation] {
        seed(&ledger.pool, row).await?;
    }
    // Terminal rows are not unfinished work and never appear.
    seed(&ledger.pool, &Row::new("55", "submitted")).await?;
    seed(&ledger.pool, &Row::new("66", "abandoned")).await?;
    let before: Value = sqlx::query_scalar(
        "SELECT jsonb_agg(to_jsonb(o) ORDER BY o.block_hash) FROM qbit_block_candidate_outbox o",
    )
    .fetch_one(&ledger.pool)
    .await?;
    let dispatches: i64 =
        sqlx::query_scalar("SELECT last_value FROM qbit_prism_candidate_dispatch_sequence")
            .fetch_one(&ledger.pool)
            .await?;

    // Proof one that no write path is involved: every ledger write refuses
    // while the cluster is halted, and the inventory is exactly what an
    // operator needs then.
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='test halt' WHERE singleton")
        .execute(&ledger.pool)
        .await?;
    let text = cli(&db, &node, &["candidates", "list"]).await?;
    assert!(text.status.success(), "{}", stderr(&text));
    let json = cli(&db, &node, &["candidates", "list", "--json"]).await?;
    assert!(json.status.success(), "{}", stderr(&json));
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL WHERE singleton")
        .execute(&ledger.pool)
        .await?;

    let printed = stdout(&text);
    let rows = listed(&json);
    assert_eq!(rows.len(), 4, "{printed}");
    for row in [&pending, &reserved, &offered, &reconciliation] {
        let listed = row_of(&rows, &row.hash);
        assert_eq!(listed["state"], row.state);
        assert_eq!(listed["block_height"], json!(row.height));
        assert_eq!(listed["attempt_count"], json!(row.attempt_count));
        // The full hash is printed so it can be pasted into `abandon`.
        assert!(printed.contains(&row.hash), "{printed}");
        assert!(printed.contains(row.state), "{printed}");
    }
    assert!(printed.contains("node RPC transport failed"), "{printed}");
    assert_eq!(
        row_of(&rows, &pending.hash)["last_error"],
        "node RPC transport failed"
    );
    assert_eq!(
        row_of(&rows, &reserved.hash)["claim_instance_id"],
        "frontend-b"
    );
    assert_eq!(row_of(&rows, &reserved.hash)["claim_live"], true);
    assert!(row_of(&rows, &reserved.hash)["claim_expires_at"].is_string());
    assert!(
        printed.contains("frontend-b until "),
        "a live claim names its holder and expiry: {printed}"
    );
    assert_eq!(
        row_of(&rows, &offered.hash)["claim_instance_id"],
        "frontend-c"
    );
    assert_eq!(row_of(&rows, &offered.hash)["claim_live"], false);
    assert!(
        printed.contains("expired"),
        "a stale claim is `expired`, never absent: {printed}"
    );
    assert_eq!(
        row_of(&rows, &reconciliation.hash)["last_error"],
        "node answer was lost in transport"
    );

    // Proof two: nothing about the claim or the retry state moved.
    assert_eq!(
        sqlx::query_scalar::<_, Value>(
            "SELECT jsonb_agg(to_jsonb(o) ORDER BY o.block_hash) FROM qbit_block_candidate_outbox o"
        )
        .fetch_one(&ledger.pool)
        .await?,
        before,
        "list must leave every row byte-identical"
    );
    for row in [&pending, &reserved, &offered, &reconciliation] {
        let after = whole_row(&ledger.pool, &row.hash).await?;
        for field in [
            "claim_token",
            "claim_instance_id",
            "claim_expires_at",
            "attempt_count",
        ] {
            assert_eq!(
                after[field],
                before
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|seeded| seeded["block_hash"] == row.hash.as_str())
                    .unwrap()[field],
                "list changed {field}"
            );
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT last_value FROM qbit_prism_candidate_dispatch_sequence"
        )
        .fetch_one(&ledger.pool)
        .await?,
        dispatches,
        "list must consume no dispatch slot"
    );

    // Proof three: neither payload column is a selected column of the
    // statement the command runs. PostgreSQL names the result columns, so
    // this reads the projection rather than the SQL text.
    let described = (&ledger.pool)
        .describe(&qbit_prism_server::ledger::Ledger::candidate_list_sql())
        .await?;
    let columns: Vec<&str> = described
        .columns()
        .iter()
        .map(|column| column.name())
        .collect();
    assert!(
        !columns.contains(&"candidate") && !columns.contains(&"block_bytes"),
        "list must not transfer a payload: {columns:?}"
    );
    assert!(columns.contains(&"block_height"));

    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn list_separates_parked_rows_from_retrying_rows_and_shows_unknown_storage_versions(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    let retrying = Row::new("11", "pending");
    let mut parked = Row::new("22", "pending");
    parked.parked = true;
    parked.attempt_count = 2;
    parked.last_error = Some("block digest did not authenticate".to_owned());
    // The claim lane parks an unknown storage version with a reason naming
    // it; it is unfinished work and must be visible.
    let mut unknown_version = Row::new("33", "pending");
    unknown_version.parked = true;
    unknown_version.storage_version = 3;
    unknown_version.height = None;
    unknown_version.last_error = Some(
        "candidate storage_version 3 is not supported by this server; only version 1 JSONB candidates are".to_owned(),
    );
    // A row an older binary wrote: no proof observation and no offer record.
    let mut older_binary = Row::new("44", "pending");
    older_binary.proof_observed_at_ms = None;
    older_binary.due_in_seconds = 5.0;
    for row in [&retrying, &parked, &unknown_version, &older_binary] {
        seed(&ledger.pool, row).await?;
    }

    let text = cli(&db, &node, &["candidates", "list"]).await?;
    assert!(text.status.success(), "{}", stderr(&text));
    let printed = stdout(&text);
    let rows = listed(&cli(&db, &node, &["candidates", "list", "--json"]).await?);

    assert_eq!(row_of(&rows, &retrying.hash)["parked"], false);
    assert!(row_of(&rows, &retrying.hash)["next_attempt_at"].is_string());
    for row in [&parked, &unknown_version] {
        assert_eq!(row_of(&rows, &row.hash)["parked"], true);
        assert_eq!(
            row_of(&rows, &row.hash)["next_attempt_at"],
            Value::Null,
            "a parked row has no due time to report"
        );
        assert_eq!(
            row_of(&rows, &row.hash)["last_error"],
            json!(row.last_error)
        );
    }
    assert_eq!(
        printed.matches("parked").count(),
        2,
        "exactly the two quarantined rows read as parked: {printed}"
    );
    assert_eq!(row_of(&rows, &unknown_version.hash)["storage_version"], 3);
    assert!(
        printed.contains("storage_version 3 is not supported"),
        "{printed}"
    );

    // Unknown is never zero and never blank, in either mode.
    assert_eq!(
        row_of(&rows, &unknown_version.hash)["block_height"],
        Value::Null
    );
    assert_eq!(
        row_of(&rows, &older_binary.hash)["proof_observed_at_ms"],
        Value::Null
    );
    for field in [
        "offer_outcome",
        "offer_reply",
        "offered_at_ms",
        "offer_reserved_by",
    ] {
        assert_eq!(row_of(&rows, &older_binary.hash)[field], Value::Null);
    }
    assert_eq!(row_of(&rows, &older_binary.hash)["claim_live"], false);

    // Oldest due first, and parked rows last: the claim lane's own ordering.
    let order: Vec<&str> = rows
        .iter()
        .map(|row| row["block_hash"].as_str().unwrap())
        .collect();
    assert_eq!(
        order,
        vec![
            older_binary.hash.as_str(),
            retrying.hash.as_str(),
            parked.hash.as_str(),
            unknown_version.hash.as_str()
        ],
        "{printed}"
    );

    // An empty inventory is this command's success case.
    sqlx::query("DELETE FROM qbit_block_candidate_outbox")
        .execute(&ledger.pool)
        .await?;
    let empty = cli(&db, &node, &["candidates", "list"]).await?;
    assert!(empty.status.success(), "{}", stderr(&empty));
    assert_eq!(stdout(&empty).trim(), "no unfinished candidates");
    let empty_json = cli(&db, &node, &["candidates", "list", "--json"]).await?;
    assert!(empty_json.status.success());
    assert!(listed(&empty_json).is_empty());

    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn abandon_terminalizes_a_pending_row_exactly_as_supersession_does() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    // Two identical rows: one the operator abandons, one the epoch
    // supersession abandons with its own statement.
    let mut operator = Row::new("11", "pending");
    operator.attempt_count = 3;
    operator.parked = true;
    operator.last_error = Some("block digest did not authenticate".to_owned());
    let mut superseded = operator.clone();
    superseded.hash = "22".repeat(32);
    seed(&ledger.pool, &operator).await?;
    seed(&ledger.pool, &superseded).await?;
    let before = whole_row(&ledger.pool, &operator.hash).await?;
    let reason = "INC-311: block proven superseded before any offer";

    // The two fences the ordinary ledger write path gives `abandon` for free.
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='test halt' WHERE singleton")
        .execute(&ledger.pool)
        .await?;
    let halted = cli(
        &db,
        &node,
        &[
            "candidates",
            "abandon",
            "--block-hash",
            &operator.hash,
            "--reason",
            reason,
        ],
    )
    .await?;
    assert_eq!(code(&halted), 1);
    assert!(
        stderr(&halted).contains("cluster halted"),
        "{}",
        stderr(&halted)
    );
    assert_eq!(whole_row(&ledger.pool, &operator.hash).await?, before);
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL WHERE singleton")
        .execute(&ledger.pool)
        .await?;

    sqlx::raw_sql(
        "ALTER TABLE qbit_ledger_writer_lease DISABLE TRIGGER qbit_prism_no_legacy_writer",
    )
    .execute(&ledger.pool)
    .await?;
    sqlx::query("INSERT INTO qbit_ledger_writer_lease(singleton,writer_id,writer_epoch,writer_session_token,lease_expires_at) VALUES(true,'python',1,'session',clock_timestamp()+interval '1 hour')")
        .execute(&ledger.pool).await?;
    let legacy = cli(
        &db,
        &node,
        &[
            "candidates",
            "abandon",
            "--block-hash",
            &operator.hash,
            "--reason",
            reason,
        ],
    )
    .await?;
    assert_eq!(code(&legacy), 1);
    assert!(
        stderr(&legacy).contains("live legacy Python writer lease"),
        "{}",
        stderr(&legacy)
    );
    assert_eq!(whole_row(&ledger.pool, &operator.hash).await?, before);
    sqlx::raw_sql("DELETE FROM qbit_ledger_writer_lease; ALTER TABLE qbit_ledger_writer_lease ENABLE TRIGGER qbit_prism_no_legacy_writer")
        .execute(&ledger.pool)
        .await?;

    let done = cli(
        &db,
        &node,
        &[
            "candidates",
            "abandon",
            "--block-hash",
            &operator.hash,
            "--reason",
            reason,
        ],
    )
    .await?;
    assert_eq!(code(&done), 0, "{}", stderr(&done));
    assert_eq!(
        stdout(&done).trim(),
        format!("abandoned {}: {reason}", operator.hash)
    );

    let after = whole_row(&ledger.pool, &operator.hash).await?;
    assert_eq!(after["state"], "abandoned");
    assert_eq!(after["last_error"], reason);
    assert!(after["completed_at"].is_string());
    for released in [
        "candidate",
        "block_bytes",
        "window_anchor_ms",
        "window_prior_balances_sha256",
        "window_first_share_seq",
        "window_last_share_seq",
        "window_share_count",
        "window_snapshot_sha256",
        "claim_token",
        "claim_instance_id",
        "claim_expires_at",
    ] {
        assert_eq!(after[released], Value::Null, "{released} was not released");
    }
    // The parked marker is evidence, and the supersession path keeps it too.
    assert_eq!(after["next_attempt_at"], before["next_attempt_at"]);

    // The supersession statement, verbatim from ledger/policy_transition.rs.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET state='abandoned',candidate=NULL,block_bytes=NULL,window_anchor_ms=NULL,window_prior_balances_sha256=NULL,window_first_share_seq=NULL,window_last_share_seq=NULL,window_share_count=NULL,window_snapshot_sha256=NULL,completed_at=clock_timestamp(),updated_at=clock_timestamp(),last_error='epoch-superseded',claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL WHERE state='pending'")
        .execute(&ledger.pool).await?;
    let supersession = whole_row(&ledger.pool, &superseded.hash).await?;
    let comparable = |row: &Value| {
        let mut row = row.clone();
        for varying in [
            "block_hash",
            "candidate_sha256",
            "last_error",
            "completed_at",
            "updated_at",
            "created_at",
        ] {
            row[varying] = Value::Null;
        }
        row
    };
    assert_eq!(
        comparable(&after),
        comparable(&supersession),
        "the operator abandon must leave the same row shape as the supersession path"
    );

    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn abandon_refuses_every_offered_state_and_leaves_the_row_byte_identical() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    for (byte, state) in [
        ("11", "offer_reserved"),
        ("22", "offered"),
        ("33", "reconciliation"),
    ] {
        let row = Row::new(byte, state);
        seed(&ledger.pool, &row).await?;
        let before = whole_row(&ledger.pool, &row.hash).await?;
        let refused = cli(
            &db,
            &node,
            &[
                "candidates",
                "abandon",
                "--block-hash",
                &row.hash,
                "--reason",
                "INC-311: operator believes the block is superseded",
            ],
        )
        .await?;
        assert_eq!(code(&refused), 3, "{}", stderr(&refused));
        let message = stderr(&refused);
        assert!(
            message.contains(&format!("is in state {state}")),
            "{message}"
        );
        assert!(
            message.contains("it was offered to the node and is never abandoned"),
            "{message}"
        );
        assert!(
            message.contains("Its block may already have been submitted"),
            "{message}"
        );
        assert_eq!(
            whole_row(&ledger.pool, &row.hash).await?,
            before,
            "{state} must be left byte-identical"
        );
    }
    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

/// Pause the real landing after require_claim and its block insert, then expire
/// the claim while that insert is still invisible to the operator connection.
#[tokio::test]
async fn abandon_waits_for_inflight_landing_after_claim_expiry() -> Result<()> {
    const LANDING_GATE: i64 = 0x42500001;
    const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    ledger.append(share(1), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 425)?;
    let hash = block.block_hash.clone();
    ledger.enqueue_candidate(block.candidate.clone()).await?;
    let claim = block.claim(ledger.claim_candidate(60).await?.unwrap());

    sqlx::raw_sql(&format!(
        "CREATE FUNCTION pause_candidate_landing() RETURNS trigger LANGUAGE plpgsql AS $$          BEGIN PERFORM pg_advisory_xact_lock({LANDING_GATE}); RETURN NEW; END $$;          CREATE TRIGGER pause_candidate_landing AFTER INSERT ON qbit_pool_blocks          FOR EACH ROW EXECUTE FUNCTION pause_candidate_landing();"
    )).execute(&ledger.pool).await?;
    let mut gate = ledger.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(LANDING_GATE)
        .execute(&mut *gate)
        .await?;
    let landing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn({
        let ledger = ledger.clone();
        async move {
            ledger
                .land_candidate(&claim, &keys().1.public_key_hex())
                .await
        }
    }));
    let waiting_for = |key: i64| {
        let pool = &ledger.pool;
        async move {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let waiting: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid::bigint=($1>>32) AND objid::bigint=($1&4294967295) AND objsubid=1)"
                    ).bind(key).fetch_one(pool).await?;
                    if waiting { return anyhow::Ok(()); }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.context("transaction did not wait for the expected lock")?
        }
    };
    waiting_for(LANDING_GATE).await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1")
        .bind(&hash).execute(&ledger.pool).await?;
    let before = whole_row(&ledger.pool, &hash).await?;
    let visible: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_hash=$1)")
            .bind(&hash)
            .fetch_one(&ledger.pool)
            .await?;
    assert!(!visible, "the landing must still be uncommitted");

    let args = [
        "candidates",
        "abandon",
        "--block-hash",
        &hash,
        "--reason",
        "operator sweep",
    ];
    let refused = {
        let abandon = cli(&db, &node, &args);
        tokio::pin!(abandon);
        tokio::select! {
            result = &mut abandon => bail!("abandon finished before landing committed: {:?}", result?),
            queued = waiting_for(SETTLEMENT_LOCK) => queued?,
        }
        assert_eq!(whole_row(&ledger.pool, &hash).await?, before);
        gate.commit().await?;
        tokio::time::timeout(Duration::from_secs(10), landing).await???;
        abandon.await?
    };
    assert_eq!(code(&refused), 6, "{}", stderr(&refused));
    assert!(stderr(&refused).contains("already in qbit_pool_blocks"));
    assert_eq!(whole_row(&ledger.pool, &hash).await?, before);
    assert!(ledger.audit_bundle(&hash).await?.is_some());
    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn abandon_refuses_unsupported_storage_versions_without_changing_evidence() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    for (byte, version, parked) in [
        ("11", 2, true),
        ("22", 3, true),
        ("33", 2, false),
        ("44", 3, false),
    ] {
        let mut row = Row::new(byte, "pending");
        row.storage_version = version;
        row.parked = parked;
        row.attempt_count = 2;
        row.last_error = Some(format!(
            "candidate storage_version {version} is not supported"
        ));
        // Even an expired claim must not allow an unsupported row to be deleted.
        row.claim = Some(("frontend-b".to_owned(), -60.0));
        seed(&ledger.pool, &row).await?;
        let before = whole_row(&ledger.pool, &row.hash).await?;
        let refused = cli(
            &db,
            &node,
            &[
                "candidates",
                "abandon",
                "--block-hash",
                &row.hash,
                "--reason",
                "operator sweep",
            ],
        )
        .await?;
        assert_eq!(code(&refused), 7, "{}", stderr(&refused));
        let message = stderr(&refused);
        assert!(
            message.contains(&format!("unsupported storage_version {version}")),
            "{message}"
        );
        assert!(message.contains("evidence preserved"), "{message}");
        assert_eq!(whole_row(&ledger.pool, &row.hash).await?, before);
    }
    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

/// #425. `storage_version = 1` is not a synonym for "native": the claim lane
/// parks a pre-migration 2.x.x document at that version rather than rewriting
/// it, and the migrator's drain check still owes that block to the pinned
/// 2.x.x image. With the version predicate alone, this command NULLed the
/// document and terminalized the row, destroying the only copy of evidence no
/// native release can rebuild. The guard is the migrator's own shape test, so
/// both native shapes in the same database stay abandonable.
#[tokio::test]
async fn abandon_refuses_a_parked_legacy_document_and_leaves_its_evidence_whole() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    // Parked and retrying, with and without a dead claim: every way the
    // version guard alone would have let an operator through.
    for (byte, parked, claim) in [
        ("11", true, None),
        ("22", false, None),
        // An expired claim is not a live claim, so code 5 cannot be what
        // saves this row; only the shape can.
        ("33", true, Some(("frontend-b".to_owned(), -60.0))),
    ] {
        let mut row = Row::new(byte, "pending");
        row.shape = Shape::Legacy;
        row.parked = parked;
        row.claim = claim;
        row.attempt_count = 2;
        row.last_error = Some(
            "candidate document is not a native candidate; drain with the 2.x.x image".to_owned(),
        );
        seed(&ledger.pool, &row).await?;
        let before = whole_row(&ledger.pool, &row.hash).await?;
        assert_eq!(before["storage_version"], 1, "the case under test is v1");
        assert!(
            before["candidate"]["block_hash_hex"].is_string(),
            "the fixture must be the 2.x.x document: {before}"
        );
        let refused = cli(
            &db,
            &node,
            &[
                "candidates",
                "abandon",
                "--block-hash",
                &row.hash,
                "--reason",
                "operator sweep",
            ],
        )
        .await?;
        assert_eq!(code(&refused), 8, "{}", stderr(&refused));
        let message = stderr(&refused);
        assert!(
            message.contains("pre-migration 2.x.x document"),
            "{message}"
        );
        assert!(message.contains("evidence preserved"), "{message}");
        assert!(message.contains("pinned 2.x.x"), "{message}");
        // Not "still pending": the whole row, column for column, including
        // the document, `updated_at` and `completed_at`.
        assert_eq!(whole_row(&ledger.pool, &row.hash).await?, before);
    }
    // The same guard on the rows it exists to keep abandonable: the 007
    // window reference this release writes, and the inline bundle native
    // 3.x.x wrote before it.
    for (byte, shape) in [("44", Shape::NativeWindow), ("55", Shape::NativeBundle)] {
        let mut native = Row::new(byte, "pending");
        native.shape = shape;
        seed(&ledger.pool, &native).await?;
        let abandoned = cli(
            &db,
            &node,
            &[
                "candidates",
                "abandon",
                "--block-hash",
                &native.hash,
                "--reason",
                "INC-425: superseded before any offer",
            ],
        )
        .await?;
        assert_eq!(code(&abandoned), 0, "{}", stderr(&abandoned));
        let after = whole_row(&ledger.pool, &native.hash).await?;
        assert_eq!(after["state"], "abandoned", "{shape:?} was not abandoned");
        assert_eq!(after["candidate"], Value::Null);
    }
    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn abandon_refuses_a_live_foreign_claim_and_succeeds_once_it_expires() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    let mut held = Row::new("11", "pending");
    held.claim = Some(("frontend-b".to_owned(), 3600.0));
    seed(&ledger.pool, &held).await?;
    let before = whole_row(&ledger.pool, &held.hash).await?;
    let reason = "INC-311: superseded before any offer";
    let args = [
        "candidates",
        "abandon",
        "--block-hash",
        &held.hash,
        "--reason",
        reason,
    ];

    let refused = cli(&db, &node, &args).await?;
    assert_eq!(code(&refused), 5, "{}", stderr(&refused));
    let message = stderr(&refused);
    assert!(
        message.contains("is held by frontend-b until "),
        "{message}"
    );
    assert!(
        message.contains("retry after the claim expires"),
        "{message}"
    );
    assert_eq!(whole_row(&ledger.pool, &held.hash).await?, before);

    // An expired claim is not a live claim: the row is abandonable again.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1")
        .bind(&held.hash).execute(&ledger.pool).await?;
    let done = cli(&db, &node, &args).await?;
    assert_eq!(code(&done), 0, "{}", stderr(&done));
    let after = whole_row(&ledger.pool, &held.hash).await?;
    assert_eq!(after["state"], "abandoned");
    assert_eq!(after["last_error"], reason);
    assert_eq!(after["claim_instance_id"], Value::Null);
    assert_eq!(after["claim_token"], Value::Null);

    // The state predicate is the version guard: a second abandon completing
    // after the first cannot overwrite the reason the first recorded, and
    // says so rather than reporting a success or a lost row.
    let again = cli(
        &db,
        &node,
        &[
            "candidates",
            "abandon",
            "--block-hash",
            &held.hash,
            "--reason",
            "INC-311: a second operator, later, with another reason",
        ],
    )
    .await?;
    assert_eq!(code(&again), 4, "{}", stderr(&again));
    assert!(
        stderr(&again).contains("is already abandoned; nothing to do"),
        "{}",
        stderr(&again)
    );
    assert_eq!(whole_row(&ledger.pool, &held.hash).await?, after);

    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn abandon_refuses_a_terminal_row_and_a_pending_row_whose_block_landed() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    let reason = "INC-311: operator sweep";
    let abandon = |hash: String| {
        let db = &db;
        let node = &node;
        async move {
            cli(
                db,
                node,
                &[
                    "candidates",
                    "abandon",
                    "--block-hash",
                    &hash,
                    "--reason",
                    reason,
                ],
            )
            .await
        }
    };

    // Already terminal: legible as "nothing to do", and distinct from a row
    // that was never there, so a repeated abandon is not a lost row.
    for (byte, state) in [("11", "submitted"), ("22", "abandoned")] {
        let row = Row::new(byte, state);
        seed(&ledger.pool, &row).await?;
        let before = whole_row(&ledger.pool, &row.hash).await?;
        let refused = abandon(row.hash.clone()).await?;
        assert_eq!(code(&refused), 4, "{}", stderr(&refused));
        assert!(
            stderr(&refused).contains(&format!("is already {state}; nothing to do")),
            "{}",
            stderr(&refused)
        );
        assert_eq!(
            whole_row(&ledger.pool, &row.hash).await?,
            before,
            "a terminal row must never be re-applied"
        );
    }

    // Pending, but its accounting has landed.
    let landed = Row::new("33", "pending");
    seed(&ledger.pool, &landed).await?;
    landed_block(&ledger.pool, &landed.hash).await?;
    let before = whole_row(&ledger.pool, &landed.hash).await?;
    let refused = abandon(landed.hash.clone()).await?;
    assert_eq!(code(&refused), 6, "{}", stderr(&refused));
    let message = stderr(&refused);
    assert!(
        message.contains("is already in qbit_pool_blocks"),
        "{message}"
    );
    assert!(
        message.contains("abandoning would discard landed accounting"),
        "{message}"
    );
    assert_eq!(whole_row(&ledger.pool, &landed.hash).await?, before);

    // No such row at all.
    let missing = "ee".repeat(32);
    let absent = abandon(missing.clone()).await?;
    assert_eq!(code(&absent), 2, "{}", stderr(&absent));
    assert!(
        stderr(&absent).contains(&format!("no candidate row for {missing}")),
        "{}",
        stderr(&absent)
    );

    // Malformed, missing and out-of-range inputs are refused at the entry
    // boundary, in the formats the row itself uses.
    for (args, expected) in [
        (
            vec!["candidates", "abandon", "--reason", reason],
            "--block-hash",
        ),
        (
            vec![
                "candidates",
                "abandon",
                "--block-hash",
                "abc",
                "--reason",
                reason,
            ],
            "--block-hash",
        ),
        (
            vec![
                "candidates",
                "abandon",
                "--block-hash",
                "AB".repeat(32).leak(),
                "--reason",
                reason,
            ],
            "--block-hash",
        ),
        (
            vec![
                "candidates",
                "abandon",
                "--block-hash",
                "ab".repeat(32).leak(),
                "--reason",
                "   ",
            ],
            "--reason",
        ),
        (
            vec![
                "candidates",
                "abandon",
                "--block-hash",
                "ab".repeat(32).leak(),
            ],
            "--reason",
        ),
        (vec!["candidates", "list", "--limit", "0"], "limit"),
        (vec!["candidates", "list", "--limit", "10001"], "limit"),
    ] {
        let output = cli(&db, &node, &args).await?;
        assert!(!output.status.success(), "{args:?} was accepted");
        assert!(
            stderr(&output).contains(expected),
            "{args:?}: {}",
            stderr(&output)
        );
    }

    node.assert_never_reached();
    assert_no_new_instances(&ledger.pool).await?;
    db.close(vec![ledger]).await
}

/// `abandon` writes an ordinary ledger row, so it is a one-shot tool and owes
/// the same four properties #412 asserts for the other four: no heartbeat on
/// success or failure, a live frontend's row untouched when the tool runs
/// under that frontend's instance ID, the halt guard preserved, and nothing
/// left behind for `fatal-state clear` to refuse.
#[tokio::test]
async fn abandon_registers_no_heartbeat_and_keeps_the_halt_guard() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("frontend-a").await?;
    let node = CountingNode::open().await?;
    let instances = |pool: PgPool| async move {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_instances")
            .fetch_one(&pool)
            .await
    };
    let frontend_row = |pool: PgPool| async move {
        sqlx::query_scalar::<_, Value>(
            "SELECT to_jsonb(i) FROM qbit_prism_instances i WHERE instance_id='frontend-a'",
        )
        .fetch_one(&pool)
        .await
    };
    let reason = "INC-381: operator abandon ran while frontend-a was live";
    let run = |hash: String, extra: Vec<(&'static str, &'static str)>| {
        let db = &db;
        let node = &node;
        async move {
            cli_with_env(
                db,
                node,
                &[
                    "candidates",
                    "abandon",
                    "--block-hash",
                    &hash,
                    "--reason",
                    reason,
                ],
                &extra,
            )
            .await
        }
    };

    // 1. A generated instance ID leaves no row, on the success path and on a
    // refusal: `cli` sets no PRISM_INSTANCE_ID, so each run generates one.
    let generated = Row::new("11", "pending");
    seed(&ledger.pool, &generated).await?;
    let done = run(generated.hash.clone(), vec![]).await?;
    assert_eq!(code(&done), 0, "{}", stderr(&done));
    assert_eq!(
        instances(ledger.pool.clone()).await?,
        1,
        "abandon registered"
    );
    let refused = run("ee".repeat(32), vec![]).await?;
    assert_eq!(code(&refused), 2, "{}", stderr(&refused));
    assert_eq!(
        instances(ledger.pool.clone()).await?,
        1,
        "a failing abandon registered"
    );

    // 2. A live frontend's row — its status with the session-owner token, its
    // heartbeat and start times — is untouched when abandon shares its ID.
    ledger
        .heartbeat(HeartbeatStatus::Health(HeartbeatHealth::new(
            true,
            Default::default(),
        )))
        .await?;
    let live_row = frontend_row(ledger.pool.clone()).await?;
    assert_eq!(live_row["status"]["ready"], true, "{live_row}");
    assert!(
        live_row["status"]["session_owner_token"].is_string(),
        "{live_row}"
    );
    let shared = Row::new("22", "pending");
    seed(&ledger.pool, &shared).await?;
    let under_frontend = run(
        shared.hash.clone(),
        vec![("PRISM_INSTANCE_ID", "frontend-a")],
    )
    .await?;
    assert_eq!(code(&under_frontend), 0, "{}", stderr(&under_frontend));
    assert_eq!(
        frontend_row(ledger.pool.clone()).await?,
        live_row,
        "abandon touched the live frontend row"
    );
    assert_eq!(instances(ledger.pool.clone()).await?, 1);

    // 3. The halt guard holds, and a halted run leaves no row behind.
    let halted_row = Row::new("33", "pending");
    seed(&ledger.pool, &halted_row).await?;
    let before = whole_row(&ledger.pool, &halted_row.hash).await?;
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='test halt' WHERE singleton")
        .execute(&ledger.pool)
        .await?;
    let halted = run(halted_row.hash.clone(), vec![]).await?;
    assert!(!halted.status.success(), "{}", stdout(&halted));
    assert!(
        stderr(&halted).contains("cluster halted"),
        "{}",
        stderr(&halted)
    );
    assert_eq!(whole_row(&ledger.pool, &halted_row.hash).await?, before);
    assert_eq!(frontend_row(ledger.pool.clone()).await?, live_row);
    assert_eq!(instances(ledger.pool.clone()).await?, 1);

    // 4. Nothing is left for `fatal-state clear` to refuse. Its instance gate
    // requires every stored row to be `stopped` or `drained`; once the one
    // real frontend stops, that is the whole table, because no abandon run —
    // generated ID, shared ID, or halted — added a row of its own.
    ledger.heartbeat(HeartbeatStatus::Stopped).await?;
    let unfinished: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(i) FROM qbit_prism_instances i WHERE coalesce(i.status->>'state','') NOT IN ('stopped','drained') ORDER BY i.instance_id")
        .fetch_all(&ledger.pool).await?;
    assert!(unfinished.is_empty(), "{unfinished:?}");
    assert_eq!(instances(ledger.pool.clone()).await?, 1);

    node.assert_never_reached();
    db.close(vec![ledger]).await
}

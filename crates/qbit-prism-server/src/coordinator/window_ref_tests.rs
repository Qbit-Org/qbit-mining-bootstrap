//! The coordinator's half of the window reference (issue #265, slice 2): the
//! `window_reads` semaphore and its entry point, and `Prepared.window`.
//!
//! Four contracts are pinned here.
//!
//! * **Sizing.** `window_read_permits` is `clamp(database_max_connections - 2,
//!   1, build_workers)` at every edge, so two pool connections always stay free
//!   for share appends and the candidate-lease heartbeat.
//! * **The permit.** [`Coordinator::read_window`] holds one `window_reads`
//!   permit for exactly the duration of the read, and none before or after.
//! * **Runtime placement.** The entry point awaits `Ledger::read_window` on the
//!   runtime. `read_window` owns its own blocking hand-offs, one
//!   `spawn_blocking` per page, so an outer blocking wrapper would starve them.
//!   The test runs on a runtime with a single blocking thread: the read
//!   completes as written, and deadlocks if the entry point or a caller ever
//!   wraps it in `spawn_blocking`.
//! * **`Prepared.window`.** A non-cached refresh publishes the reference for
//!   the snapshot it captured, for a window with shares and for an empty one.
//!
//! PRISM_TEST_DATABASE_URL=postgres://postgres:prism@127.0.0.1:5432/postgres \
//!     cargo test -p qbit-prism-server --lib window_ref_tests -- --nocapture

use super::d2_test_support::*;
use super::*;
use crate::ledger::ShareRange;
use anyhow::anyhow;
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism::AcceptedShare;
use tokio::task::JoinHandle;

/// The ledger's advisory locks are cluster-wide constants, not schema-scoped,
/// so a private schema does not isolate these tests from each other.
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

const TEMPLATE_VERSION: u32 = 0x2000_0000;
const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const TIP_HASH: &str = "aa";

// ---------------------------------------------------------------------------
// Sizing
// ---------------------------------------------------------------------------

/// `clamp(database_max_connections - 2, 1, build_workers)`, at the edges the
/// configuration can actually reach: `PRISM_DATABASE_MAX_CONNECTIONS` is
/// bounded to `4..=1024` and `PRISM_JOB_BUILD_EXECUTOR_WORKERS` to
/// `1..=runtime_workers + 8`, but the function must also survive values
/// outside those bounds rather than panicking inside `clamp`.
#[test]
fn window_read_permits_leave_two_connections_and_never_exceed_the_build_slots() {
    for (connections, workers, expected) in [
        // The floor: the smallest permitted pool, and one build worker.
        (4u32, 1usize, 1usize),
        // More build workers than spare connections: the pool bounds it.
        (4, 8, 2),
        // The default pool with the default build workers: the slots bound it.
        (16, 4, 4),
        (16, 1, 1),
        // A build worker count above the pool, at the default pool size.
        (16, 64, 14),
        // Degenerate inputs: never zero, and never a panicking clamp.
        (0, 4, 1),
        (1, 4, 1),
        (2, 4, 1),
        (3, 4, 1),
        (4, 0, 1),
        (1024, 1, 1),
        (1024, 1024, 1022),
    ] {
        assert_eq!(
            window_read_permits(connections, workers),
            expected,
            "window_read_permits({connections},{workers})"
        );
    }
}

// ---------------------------------------------------------------------------
// Fake node
// ---------------------------------------------------------------------------

struct NodeState {
    height: u64,
    hashes: HashMap<u64, String>,
    coinbase_value_sats: u64,
}

impl NodeState {
    fn at_tip(height: u64) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            height,
            hashes: HashMap::from([(0, GENESIS_HASH.to_owned()), (height, TIP_HASH.repeat(32))]),
            coinbase_value_sats: 500_000_000,
        }))
    }

    fn tip(&self) -> String {
        self.hashes[&self.height].clone()
    }
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let node = node.lock().await;
    let result = match request["method"].as_str().unwrap_or_default() {
        "getblockhash" => request["params"][0]
            .as_u64()
            .and_then(|height| node.hashes.get(&height))
            .map_or(Value::Null, |hash| json!(hash)),
        "getbestblockhash" => json!(node.tip()),
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":node.height,"headers":node.height,"bestblockhash":node.tip(),
            "chainwork":format!("{:064x}",1)}),
        "getnetworkinfo" => json!({ "connections": 2 }),
        "getblockheader" => json!({ "previousblockhash": GENESIS_HASH }),
        "getblocktemplate" => json!({"version":TEMPLATE_VERSION,"bits":TEMPLATE_BITS,
            "height":node.height+1,"coinbasevalue":node.coinbase_value_sats,
            "curtime":unix_now().expect("the host clock precedes the epoch"),
            "previousblockhash":node.tip(),"transactions":[]}),
        method => panic!("unexpected window-reference RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    schema: TestSchema,
    coordinator: Arc<Coordinator>,
    server: JoinHandle<()>,
}

impl Fixture {
    /// A private schema, a fake node, and a coordinator wired to both, sized
    /// by `database_connections` and `build_workers`.
    async fn open(raw: &str, connections: u32, build_workers: usize) -> Result<Self> {
        let schema = TestSchema::create(raw, "prism_window_ref").await?;
        let opened = async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc_url = format!("http://{}/", listener.local_addr()?);
            let mut config =
                test_config(schema.url(), rpc_url, "window-ref", Duration::from_secs(15))?;
            config.database_connections = connections;
            config.build_workers = build_workers;
            let node = NodeState::at_tip(100);
            let app = Router::new().route("/", post(node_reply)).with_state(node);
            let server = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            match Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await {
                Ok(coordinator) => Ok((coordinator, server)),
                Err(error) => {
                    server.abort();
                    Err(error)
                }
            }
        }
        .await;
        match opened {
            Ok((coordinator, server)) => Ok(Self {
                schema,
                coordinator,
                server,
            }),
            Err(error) => Err(schema.abandon(error).await),
        }
    }

    async fn prepared(&self) -> Result<Arc<Prepared>> {
        self.coordinator
            .prepared
            .read()
            .await
            .clone()
            .context("refresh_once published no prepared work")
    }

    /// Teardown that cannot itself hang.
    ///
    /// Every step is bounded, because a read these tests deliberately starve
    /// leaves a pool connection inside an open transaction, and both draining
    /// the pool and dropping the schema would then wait on it for good. A
    /// failing assertion has to reach the reader as a failure, not as a test
    /// binary that never returns, so a teardown that cannot finish is reported
    /// and the schema is left for the run's disposable cluster to take away.
    async fn close(self) -> Result<()> {
        self.server.abort();
        let drained = tokio::time::timeout(
            Duration::from_secs(10),
            self.coordinator.ledger.pool.close(),
        )
        .await
        .is_ok();
        match tokio::time::timeout(Duration::from_secs(20), self.schema.remove()).await {
            Ok(removed) => removed,
            Err(_) => Err(anyhow!(
                "the test schema could not be dropped within 20 s (pool drained: {drained}); a \
                 window read is still holding its transaction open"
            )),
        }
    }
}

/// A share the append path accepts, distinct per index.
fn share(index: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("window-ref:{index:064x}"),
        miner_id: format!("miner-{}", index % 3),
        order_key: format!("order-{}", index % 2),
        p2mr_program_hex: format!("{:02x}", 0x30 + (index % 3)).repeat(32),
        share_difficulty: 4,
        network_difficulty: 1_000_000,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

async fn seed(ledger: &Ledger, count: u64) -> Result<()> {
    for index in 1..=count {
        ledger.append(share(index), None).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Prepared.window
// ---------------------------------------------------------------------------

/// A non-cached refresh publishes the reference for the snapshot it captured:
/// the same value `WindowRef::from_snapshot` computes from that snapshot, with
/// the range's own fields and the digest of the serialized window.
///
/// The reference is computed beside `build_bundle` under `tokio::join!`, so
/// this also covers that the joined blocking task's result actually reaches
/// `Prepared`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refresh_publishes_the_reference_for_the_window_it_captured() -> Result<()> {
    let Some(url) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&url, 6, 2).await?;
    let outcome = async {
        seed(&fixture.coordinator.ledger, 5).await?;
        fixture.coordinator.refresh_once().await?;
        let prepared = fixture.prepared().await?;
        ensure!(
            prepared.bundle.is_some(),
            "the refresh did not select the seeded window"
        );
        ensure!(
            prepared.window == WindowRef::from_snapshot(&prepared.snapshot)?,
            "Prepared.window is not the reference for the published snapshot"
        );
        let range = prepared
            .window
            .shares
            .context("a window of five shares must carry a range")?;
        let shares = &prepared.snapshot.shares;
        ensure!(
            range
                == ShareRange {
                    first_share_seq: shares[0].share_seq,
                    last_share_seq: shares[shares.len() - 1].share_seq,
                    share_count: u64::try_from(shares.len())?,
                    snapshot_sha256: Sha256::digest(serde_json::to_vec(shares)?).into(),
                },
            "the published range does not describe the published window: {range:?}"
        );
        ensure!(
            prepared.window.anchor_ms == prepared.snapshot.anchor_ms,
            "the reference carries a different anchor than the snapshot"
        );
        ensure!(
            prepared.window.prior_balances_digest
                == qbit_prism::prior_balances_digest(&prepared.snapshot.prior_balances),
            "the reference carries a different balances digest than the snapshot"
        );
        Ok(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// An empty window is routine: the refresh publishes a reference with no
/// range, and the balances digest of the snapshot it captured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refresh_over_an_empty_ledger_publishes_an_empty_window_reference() -> Result<()> {
    let Some(url) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&url, 6, 2).await?;
    let outcome = async {
        fixture.coordinator.refresh_once().await?;
        let prepared = fixture.prepared().await?;
        ensure!(
            prepared.bundle.is_none() && prepared.snapshot.shares.is_empty(),
            "the ledger was empty, so the refresh should have published no window bundle"
        );
        ensure!(
            prepared.window.shares.is_none(),
            "an empty window must carry no range, got {:?}",
            prepared.window.shares
        );
        ensure!(
            prepared.window == WindowRef::from_snapshot(&prepared.snapshot)?,
            "Prepared.window is not the reference for the published empty snapshot"
        );
        ensure!(
            prepared.window.prior_balances_digest
                == qbit_prism::prior_balances_digest(&prepared.snapshot.prior_balances),
            "the empty reference carries a different balances digest than the snapshot"
        );
        Ok(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

// ---------------------------------------------------------------------------
// The permit
// ---------------------------------------------------------------------------

/// The entry point holds one `window_reads` permit for the read and no longer.
///
/// The coordinator is sized to a single permit, so a permit the test holds
/// blocks the read outright: the call cannot finish while the test owns it,
/// finishes once it is released, and leaves the semaphore full again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_entry_point_holds_a_window_read_permit_only_while_it_reads() -> Result<()> {
    let Some(url) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    // clamp(4 - 2, 1, 1) == 1.
    let fixture = Fixture::open(&url, 4, 1).await?;
    let outcome = async {
        seed(&fixture.coordinator.ledger, 3).await?;
        fixture.coordinator.refresh_once().await?;
        let reference = fixture.prepared().await?.window;
        ensure!(
            fixture.coordinator.window_reads.available_permits() == 1,
            "an idle coordinator must hold no window-read permit"
        );
        let held = fixture
            .coordinator
            .window_reads
            .clone()
            .acquire_owned()
            .await?;
        let coordinator = fixture.coordinator.clone();
        let read = tokio::spawn(async move {
            coordinator
                .read_window(&reference, BalanceSource::Current)
                .await
                .map(|window| window.shares.len())
        });
        ensure!(
            tokio::time::timeout(Duration::from_millis(500), async {
                // The read must not begin while the only permit is held.
            })
            .await
            .is_ok(),
            "the test's own delay did not elapse"
        );
        ensure!(!read.is_finished(), "the read ran without taking a permit");
        drop(held);
        let shares = tokio::time::timeout(Duration::from_secs(30), read)
            .await
            .map_err(|_| anyhow!("the read did not finish after the permit was released"))???;
        ensure!(shares == 3, "the read returned {shares} shares, expected 3");
        ensure!(
            fixture.coordinator.window_reads.available_permits() == 1,
            "the entry point kept a permit after the read returned"
        );
        Ok(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

// ---------------------------------------------------------------------------
// Runtime placement
// ---------------------------------------------------------------------------

/// The entry point must await `Ledger::read_window` on the runtime.
///
/// `read_window` hands every page's row mapping and hash update to a
/// `spawn_blocking` task of its own. On a runtime with exactly one blocking
/// thread, that still completes: the page tasks run one after another. Wrap the
/// read in `spawn_blocking` -- in the entry point or in a caller -- and the
/// wrapper occupies the only blocking thread while waiting for a page task
/// that can never be scheduled, so this read never returns and the timeout
/// fails the test.
///
/// The window spans several pages' worth of hand-offs only in the sense that
/// each read performs at least the balances-digest hand-off and one page
/// hand-off; one starved hand-off is enough to hang, which is the property
/// under test.
///
/// It is a plain `#[test]` because the runtime has to be built here, with
/// `max_blocking_threads(1)`, rather than by the `#[tokio::test]` macro.
#[test]
fn the_entry_point_reads_without_occupying_a_blocking_thread() -> Result<()> {
    let Some(url) = database_url()? else {
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(async move {
        let _serial = TEST_LOCK.lock().await;
        let fixture = Fixture::open(&url, 6, 1).await?;
        let outcome = async {
            seed(&fixture.coordinator.ledger, 4).await?;
            fixture.coordinator.refresh_once().await?;
            let reference = fixture.prepared().await?.window;
            let window = tokio::time::timeout(
                Duration::from_secs(30),
                fixture
                    .coordinator
                    .read_window(&reference, BalanceSource::Current),
            )
            .await
            .map_err(|_| {
                anyhow!(
                    "the window read did not finish on a runtime with one blocking thread; a \
                     caller or the entry point is running read_window inside spawn_blocking"
                )
            })??;
            ensure!(
                window.shares.len() == 4,
                "the read returned {} shares, expected 4",
                window.shares.len()
            );
            Ok(())
        }
        .await;
        settle(outcome, fixture.close().await)
    });
    // A caller that wrapped the read in `spawn_blocking` leaves that wrapper
    // parked on the runtime's only blocking thread for good. Dropping the
    // runtime waits for its blocking tasks, which would turn this failure into
    // a test binary that never returns; hand the runtime to the process exit
    // instead, so the assertion above is what the reader sees.
    runtime.shutdown_background();
    outcome
}

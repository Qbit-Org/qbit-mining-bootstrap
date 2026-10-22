//! #598: a job build that a later publication overtakes.
//!
//! A frontend builds and persists one job per session. When publications
//! arrive faster than one fan-out, every build used to be superseded before
//! it finished and was thrown away, so no session got work. Fresh work now
//! survives a later publication on the same published tip that kept its
//! parent, payout revision and fee (a reanchor, new shares, a template
//! change), and is delivered exactly as the jobs other sessions already hold
//! from its publication. A tip change, including a return to an earlier tip
//! (A -> B -> A), a payout revision change or a fee change still discards it.
//!
//! Each case runs a real `Coordinator` against a private PostgreSQL schema
//! and a fake node, and holds `build_job` at `build_job_probe`, after the job
//! is built and before its admission is revalidated, while the case
//! publishes.
//!
//! PRISM_TEST_DATABASE_URL=postgres://postgres:prism@127.0.0.1:5432/postgres \
//!     cargo test -p qbit-prism-server --lib same_tip_republish_tests

use super::d2_test_support::{
    database_url, settle, test_config, unix_now, TestSchema, EXTRANONCE1, TEMPLATE_BITS,
};
use super::test_serial::TEST_LOCK;
use super::*;
use anyhow::{anyhow, bail};
use axum::{extract::State, routing::post, Json, Router};
use std::sync::Mutex as StdMutex;
use tokio::task::JoinHandle;
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument::WithSubscriber;

const HEIGHT: u64 = 100;

fn block(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

/// A chain of distinct hashes, a fee estimate and a template the cases edit.
struct Node {
    tip: String,
    height: u64,
    chainwork: u64,
    parents: HashMap<String, String>,
    hashes: HashMap<u64, String>,
    coinbase_value_sats: u64,
    /// `estimatesmartfee`'s rate, in coins per 1,000 virtual bytes.
    feerate: &'static str,
}

impl Node {
    fn advance(&mut self, hash: String) {
        self.parents.insert(hash.clone(), self.tip.clone());
        self.height += 1;
        self.chainwork += 1;
        self.hashes.insert(self.height, hash.clone());
        self.tip = hash;
    }
}

async fn node_reply(
    State(node): State<Arc<StdMutex<Node>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let node = node.lock().unwrap();
    let result = match request["method"].as_str().unwrap() {
        "getblockhash" => request["params"][0]
            .as_u64()
            .and_then(|height| node.hashes.get(&height))
            .map_or(Value::Null, |hash| json!(hash)),
        "getbestblockhash" => json!(node.tip),
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":node.height,"headers":node.height,"bestblockhash":node.tip,
            "chainwork":format!("{:064x}",node.chainwork)}),
        "getnetworkinfo" => json!({"connections":2}),
        "getblockheader" => json!({"previousblockhash":node
            .parents
            .get(request["params"][0].as_str().unwrap_or_default())}),
        "getblocktemplate" => json!({"version":0x2000_0000u32,"bits":TEMPLATE_BITS,
            "height":node.height+1,"coinbasevalue":node.coinbase_value_sats,
            "curtime":unix_now().expect("the host clock precedes the epoch"),
            "previousblockhash":node.tip,"transactions":[]}),
        "getmempoolinfo" => json!({"minrelaytxfee":0.00001,"mempoolminfee":0.00001}),
        "estimatesmartfee" => json!({"feerate":node.feerate.parse::<Value>().unwrap(),"blocks":2}),
        method => panic!("unexpected RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

struct Fixture {
    schema: TestSchema,
    coordinator: Arc<Coordinator>,
    node: Arc<StdMutex<Node>>,
    server: JoinHandle<()>,
    worker: Worker,
}

impl Fixture {
    /// CTV is on with the fee read from the node, so a fee change republishes;
    /// a zero snapshot interval makes every refresh a reanchor.
    async fn open(raw: &str) -> Result<Self> {
        let schema = TestSchema::create(raw, "prism_same_tip").await?;
        let mut hashes = HashMap::from([(0, block(0))]);
        hashes.insert(HEIGHT, block(0xa0));
        let node = Arc::new(StdMutex::new(Node {
            tip: block(0xa0),
            height: HEIGHT,
            chainwork: 1,
            parents: HashMap::from([(block(0xa0), block(0))]),
            hashes,
            coinbase_value_sats: 500_000_000,
            feerate: "0.0001",
        }));
        let opened = async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc_url = format!("http://{}/", listener.local_addr()?);
            let mut config =
                test_config(schema.url(), rpc_url, "same-tip", Duration::from_secs(15))?;
            config.ctv_enabled = true;
            config.snapshot_interval = Duration::ZERO;
            let app = Router::new()
                .route("/", post(node_reply))
                .with_state(node.clone());
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
        let (coordinator, server) = match opened {
            Ok(opened) => opened,
            Err(error) => return Err(schema.abandon(error).await),
        };
        let worker = Worker {
            username: "miner-a.rig".into(),
            payout_address: "miner-a".into(),
            worker_name: Some("rig".into()),
            p2mr_program_hex: "ab".repeat(32),
        };
        Ok(Self {
            schema,
            coordinator,
            node,
            server,
            worker,
        })
    }

    /// A share from the fixture's worker, or from a second miner, which
    /// changes how the window splits the coinbase.
    async fn append_share(&self, sequence: u64, second_miner: bool) -> Result<()> {
        let other = Worker {
            username: "miner-b.rig".into(),
            payout_address: "miner-b".into(),
            worker_name: Some("rig".into()),
            p2mr_program_hex: "cd".repeat(32),
        };
        let worker = if second_miner { &other } else { &self.worker };
        self.coordinator
            .ledger
            .append(
                AcceptedShare {
                    share_seq: 0,
                    share_id: format!("same-tip:{sequence:064x}"),
                    miner_id: worker.username.clone(),
                    order_key: worker.username.clone(),
                    p2mr_program_hex: worker.p2mr_program_hex.clone(),
                    share_difficulty: 1,
                    network_difficulty: 100,
                    template_height: HEIGHT + 1,
                    job_id: "same-tip-seed".into(),
                    job_issued_at_ms: 1,
                    accepted_at_ms: 0,
                    ntime: 1_800_000_000,
                    credit_policy: None,
                },
                None,
            )
            .await?;
        Ok(())
    }

    async fn prepared(&self) -> Result<Arc<Prepared>> {
        self.coordinator
            .prepared
            .read()
            .await
            .clone()
            .context("no published work")
    }

    /// Publish once more, and prove a new publication was installed.
    async fn republish(&self) -> Result<Arc<Prepared>> {
        let before = self.prepared().await?;
        self.coordinator.refresh_once().await?;
        let after = self.prepared().await?;
        ensure!(
            after.storage_key != before.storage_key,
            "the refresh did not republish"
        );
        Ok(after)
    }

    async fn issue(&self) -> Result<MiningJob<JobContext>> {
        let job = self
            .coordinator
            .build_job(&self.worker, EXTRANONCE1, 1.0, 0.0)
            .await
            .map_err(|error| anyhow!("build: {error:?}"))?;
        self.persist(&job).await?;
        Ok(job)
    }

    async fn persist(&self, job: &MiningJob<JobContext>) -> Result<()> {
        self.coordinator
            .persist_issued_job(&self.worker, job, 0, Duration::from_secs(30))
            .await
            .map_err(|error| anyhow!("persist: {error:?}"))?;
        let stored: Option<i64> =
            sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_jobs WHERE job_id=$1")
                .bind(&job.wire.job_id)
                .fetch_optional(&self.coordinator.ledger.pool)
                .await?;
        ensure!(
            stored == Some(job.wire.payout_revision),
            "the issued job was not stored under its own payout revision: {stored:?}"
        );
        Ok(())
    }

    /// Build a job, hold it after it is built, run `publish`, then let the
    /// build revalidate. The build's own warnings are returned beside it.
    async fn overtaken<F: std::future::Future<Output = Result<()>>>(
        &self,
        publish: F,
    ) -> Result<(Result<MiningJob<JobContext>, StratumError>, String)> {
        let probe = Arc::new(OfferProbe::default());
        *self.coordinator.build_job_probe.lock().unwrap() = Some(probe.clone());
        let log = super::miner_tests::SharedLog::default();
        let build = AbortOnDropHandle::new(tokio::spawn({
            let coordinator = self.coordinator.clone();
            let worker = self.worker.clone();
            async move { coordinator.build_job(&worker, EXTRANONCE1, 1.0, 0.0).await }
                .with_subscriber(log.dispatch())
        }));
        let entered = tokio::time::timeout(Duration::from_secs(10), probe.entered.notified()).await;
        *self.coordinator.build_job_probe.lock().unwrap() = None;
        entered.context("the build never reached the probe")?;
        let published = publish.await;
        probe.release.notify_one();
        let built = tokio::time::timeout(Duration::from_secs(10), build).await??;
        published?;
        Ok((built, log.text()))
    }

    async fn close(self) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        self.schema.remove().await
    }
}

/// Everything a job pays and commits to, other than its ID and target.
fn payout_outputs(job: &codec::Job) -> Value {
    json!({
        "previousblockhash": job.previousblockhash,
        "coinb1": &*job.coinb1,
        "coinb2": &*job.coinb2,
        "full_coinbase_prefix": &*job.full_coinbase_prefix,
        "full_coinbase_suffix": &*job.full_coinbase_suffix,
        "merkle_branch": job.merkle_branch.iter().map(hex::encode).collect::<Vec<_>>(),
        "transactions": job.transactions.iter().map(hex::encode).collect::<Vec<_>>(),
        "version": job.version,
        "nbits": job.nbits,
        "payout_revision": job.payout_revision,
        "refresh_generation": job.refresh_generation,
    })
}

/// The overtaken job is the publication it was built on, byte for byte what
/// a session that got work from that publication already holds, and saved.
async fn assert_delivered_as_live(
    fixture: &Fixture,
    case: &str,
    overtaken: MiningJob<JobContext>,
    live: &MiningJob<JobContext>,
    replacement: &Prepared,
) -> Result<()> {
    ensure!(
        Arc::ptr_eq(&overtaken.context.prepared, &live.context.prepared)
            && Arc::ptr_eq(&overtaken.context.bundle, &live.context.bundle),
        "{case}: the overtaken job does not carry its own publication"
    );
    ensure!(
        payout_outputs(&overtaken.wire) == payout_outputs(&live.wire),
        "{case}: the overtaken job differs from the live job of its publication"
    );
    ensure!(
        replacement.template["previousblockhash"]
            == live.context.prepared.template["previousblockhash"]
            && replacement.snapshot.payout_revision == live.wire.payout_revision,
        "{case}: the republication changed the parent or the payout revision"
    );
    fixture.persist(&overtaken).await
}

fn assert_discarded(
    case: &str,
    built: Result<MiningJob<JobContext>, StratumError>,
    log: &str,
) -> Result<()> {
    let Err(error) = built else {
        bail!("{case}: the overtaken build was delivered");
    };
    let response = error.response(json!(1));
    ensure!(
        response["error"][2]["reason_id"] == "pool-closed",
        "{case}: unexpected refusal {response}"
    );
    ensure!(
        log.contains("job preparation deferred") && log.contains("payout snapshot stale"),
        "{case}: the build was refused for another reason: {log}"
    );
    Ok(())
}

/// A reanchor, new shares and a template change each republish on the same
/// tip, revision and fee while a build is held. The build is delivered and
/// saved, and it is byte-identical to the job the same worker already holds
/// from that publication, while the republication itself pays differently
/// for the shares and template cases. A job built before a republication is
/// also still saved after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_overtaken_by_a_same_tip_republication_is_delivered_as_its_publication(
) -> Result<()> {
    let Some(url) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&url).await?;
    let result = async {
        fixture.append_share(1, false).await?;
        fixture.coordinator.refresh_once().await?;
        ensure!(
            fixture.prepared().await?.bundle.is_some(),
            "the seeded window did not publish shared work"
        );
        for (index, case) in ["reanchor", "shares", "template"].into_iter().enumerate() {
            let live = fixture.issue().await?;
            let (built, log) = fixture
                .overtaken(async {
                    match case {
                        "shares" => fixture.append_share(10 + index as u64, true).await?,
                        "template" => fixture.node.lock().unwrap().coinbase_value_sats -= 1_000,
                        _ => {}
                    }
                    fixture.republish().await.map(drop)
                })
                .await?;
            let overtaken = built.map_err(|error| anyhow!("{case}: {error:?}\n{log}"))?;
            let replacement = fixture.prepared().await?;
            if case != "reanchor" {
                let fresh = fixture.issue().await?;
                ensure!(
                    fresh.wire.coinb1 != live.wire.coinb1 || fresh.wire.coinb2 != live.wire.coinb2,
                    "{case}: the republication pays the same outputs, so this case proves nothing"
                );
            }
            assert_delivered_as_live(&fixture, case, overtaken, &live, &replacement).await?;
        }
        // Persistence after the build: work built before a republication is
        // saved after it.
        let built = fixture
            .coordinator
            .build_job(&fixture.worker, EXTRANONCE1, 1.0, 0.0)
            .await
            .map_err(|error| anyhow!("build: {error:?}"))?;
        fixture.republish().await?;
        fixture
            .persist(&built)
            .await
            .context("persist after a reanchor")
    }
    .await;
    settle(result, fixture.close().await)
}

/// A new tip, and a return to the build's own tip through another
/// (A -> B -> A), each discard a held build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_overtaken_by_a_tip_change_is_discarded() -> Result<()> {
    let Some(url) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&url).await?;
    let result = async {
        fixture.append_share(1, false).await?;
        fixture.coordinator.refresh_once().await?;
        let (built, log) = fixture
            .overtaken(async {
                let tip = fixture.prepared().await?.template["previousblockhash"].clone();
                let mut observed = fixture.coordinator.observed_tip.write().await;
                for hash in [block(0xb0), tip.as_str().context("no parent")?.to_owned()] {
                    let sequence = observed.reserve();
                    observed.observe(&hash, sequence, true);
                    observed.publish(&hash)?;
                }
                Ok(())
            })
            .await?;
        assert_discarded("A -> B -> A", built, &log)?;
        let (built, log) = fixture
            .overtaken(async {
                fixture.node.lock().unwrap().advance(block(0xc0));
                let replacement = fixture.republish().await?;
                ensure!(replacement.template["previousblockhash"] == block(0xc0));
                Ok(())
            })
            .await?;
        assert_discarded("new tip", built, &log)
    }
    .await;
    settle(result, fixture.close().await)
}

/// A payout revision change, with or without a republication, and a fee
/// change on the same tip each discard a held build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_overtaken_by_a_revision_or_fee_change_is_discarded() -> Result<()> {
    let Some(url) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&url).await?;
    let result = async {
        fixture.append_share(1, false).await?;
        fixture.coordinator.refresh_once().await?;
        for (case, republish) in [("revision", false), ("revision republished", true)] {
            let (built, log) = fixture
                .overtaken(async {
                    sqlx::query(
                        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
                    )
                    .execute(&fixture.coordinator.ledger.pool)
                    .await?;
                    if republish {
                        fixture.republish().await?;
                    }
                    Ok(())
                })
                .await?;
            assert_discarded(case, built, &log)?;
            fixture.coordinator.refresh_once().await?;
        }
        let before = fixture.prepared().await?;
        let (built, log) = fixture
            .overtaken(async {
                fixture.node.lock().unwrap().feerate = "0.0002";
                let replacement = fixture.republish().await?;
                ensure!(
                    replacement.fee != before.fee
                        && replacement.snapshot.payout_revision == before.snapshot.payout_revision,
                    "the fee republication changed something else or nothing"
                );
                // The job's own fee still clears the live relay floor, so
                // only the fee comparison can refuse it.
                fixture
                    .coordinator
                    .ensure_job_fee_current(before.fee)
                    .await
            })
            .await?;
        assert_discarded("fee", built, &log)
    }
    .await;
    settle(result, fixture.close().await)
}

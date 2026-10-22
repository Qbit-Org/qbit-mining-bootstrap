//! #521 scenario 4: a qbitd outage around `submitblock`, with a real node.
//!
//! Both servers reach the node through an HTTP JSON-RPC proxy owned by the
//! test. The proxy forwards every call unchanged, records every `submitblock`
//! it sees, and can hold one: just before the request is forwarded (the node
//! has not seen the block), or just after the node's reply was read (the node
//! accepted the block, the offering server has not yet heard so). While the
//! call is held, all proxy traffic is paused, the test stops (`SIGSTOP`) or
//! kills (`SIGKILL`) the node, and only then is the call released. Blocks are
//! mined one at a time by a Stratum client in the test, so each fault has
//! exactly one block in flight.
//!
//! A request the proxy has read was written by the server, so a kill before
//! the proxy forwards it is, for the server, a call that may have reached the
//! node. The kill-before-submit case therefore faults earlier (#522): a
//! database trigger holds the block's offer reservation while the node is
//! killed and the proxy closes its port, so the one `submitblock` finds the
//! connection refused before any byte is written. The port stays closed until
//! the restarted node has finished its warmup, so the servers see only refused
//! connections, never a warmup reply.
//!
//! The warmup case (#526) faults the same way, then restarts the node held in
//! its warmup and reopens the port: the retry reaches the real node, which
//! answers `RPC_IN_WARMUP` (-28) without running the call. The row must go
//! back to `pending` again, and the block lands once the node is warm. The
//! hold is deterministic: the node's fee-estimates file is a FIFO, which qbitd
//! opens during startup after its RPC server is up and before its warmup ends,
//! and that open blocks until the test opens the other end.
//!
//! Each case asserts: both servers report the node unavailable on /healthz
//! and /metrics and recover; every block is offered exactly once; the held block lands
//! (the node resumed with it, reindexed it, or, when the offer was never
//! sent, received it once it was back) or reconciles to a proven orphan (the
//! node lost the accepted block in the crash); no row is left in
//! `offer_reserved` or `offered`; and a new block found after recovery lands.
use super::*;
use num_bigint::BigUint;
use qbit_prism_server::codec::{
    difficulty_target, double_sha256, hash_display, parse_u32_hex, target_from_compact,
};
use qbit_prism_server::rpc::RPC_IN_WARMUP;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    io::AsyncReadExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{oneshot, watch},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_stopped_before_submitblock_lands_the_block_once_after_resume() -> Result<()> {
    outage_case(Fault::Stop, Hold::Request, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_stopped_after_accepting_lands_the_block_once_after_resume() -> Result<()> {
    outage_case(Fault::Stop, Hold::Reply, false).await
}

/// #522: the node dies after the block passed Stratum admission and before
/// its `submitblock`, which finds the port refusing connections. Nothing
/// reached the node, so the row returns to `pending` and retries while the
/// port refuses, and the restarted node, on the same tip, receives the block
/// exactly once and it lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_killed_before_submitblock_lands_the_block_once_after_restart() -> Result<()> {
    outage_case(Fault::Kill, Hold::Reservation, false).await
}

/// #526: as above, but the retry reaches the restarted node while it is
/// still warming up. The node answers -28 without running the call, the row
/// returns to `pending` again, and once the node is warm the block lands:
/// exactly one `submitblock` the node ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_restarted_into_warmup_before_submitblock_lands_the_block_once() -> Result<()> {
    outage_case(Fault::Kill, Hold::Warmup, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_killed_after_accepting_reconciles_the_lost_block_once() -> Result<()> {
    outage_case(Fault::Kill, Hold::Reply, false).await
}

/// The crash variant whose restart runs `-reindex`, which recovers the
/// accepted block from the block files. Opt-in (`--ignored`): no PR job runs
/// it; it is meant for the nightly job #521 adds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in #521 variant for the nightly job: run with --ignored"]
async fn node_killed_after_accepting_and_reindexed_lands_the_block_once() -> Result<()> {
    outage_case(Fault::Kill, Hold::Reply, true).await
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Fault {
    /// `SIGSTOP`, later `SIGCONT`: the node keeps its sockets and state.
    Stop,
    /// `SIGKILL`, later a restart on the same data directory and RPC port.
    Kill,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Hold {
    /// Before the `submitblock` request is forwarded to the node.
    Request,
    /// After the node's reply to `submitblock` was read, before it is
    /// relayed to the offering server.
    Reply,
    /// Before the `submitblock` is sent: the held block's offer reservation
    /// waits on a database trigger, and the proxy refuses connections from
    /// the fault until the node is back.
    Reservation,
    /// As `Reservation`, but the port reopens while the restarted node is
    /// held in its warmup, so a retry is answered -28 (#526).
    Warmup,
}

async fn outage_case(fault: Fault, hold: Hold, reindex: bool) -> Result<()> {
    let Some(mut fixture) = Fixture::open_with_servers(false, false).await? else {
        return Ok(());
    };
    let result = outage_case_on(
        &mut fixture,
        fault,
        hold,
        reindex,
        async |_| Ok(()),
        async |_| Ok(()),
    )
    .await;
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

/// One case on a fixture opened without servers, which the caller cleans
/// up: `ready` once both servers report healthy, before the first block,
/// and `after` while the servers still reach the node through the proxy.
/// #553 runs the cases under session load.
pub(super) async fn outage_case_on<R, F>(
    fixture: &mut Fixture,
    fault: Fault,
    hold: Hold,
    reindex: bool,
    ready: R,
    after: F,
) -> Result<()>
where
    R: AsyncFnOnce(&mut Fixture) -> Result<()>,
    F: AsyncFnOnce(&mut Fixture) -> Result<()>,
{
    let proxy = RpcProxy::start(fixture.rpc_port).await?;
    let mut result = outage_steps(fixture, &proxy, fault, hold, reindex, ready).await;
    if result.is_ok() {
        result = after(fixture).await;
    }
    if result.is_err() {
        eprintln!("proxy saw submitblock for {:?}", proxy.submitted());
        eprintln!("{}", fixture.diagnostics());
    }
    proxy.stop();
    result
}

async fn outage_steps<R>(
    fixture: &mut Fixture,
    proxy: &RpcProxy,
    fault: Fault,
    hold: Hold,
    reindex: bool,
    ready: R,
) -> Result<()>
where
    R: AsyncFnOnce(&mut Fixture) -> Result<()>,
{
    // The servers are the only clients that go through the proxy; the test's
    // own calls go to the node directly.
    let node_port = fixture.rpc_port;
    fixture.rpc_port = proxy.port;
    let started = (0..2).try_for_each(|index| {
        let process = fixture.start_server(index)?;
        fixture.servers.push(process);
        Ok::<_, anyhow::Error>(())
    });
    fixture.rpc_port = node_port;
    started?;
    wait_health(fixture, true, 30).await?;
    ready(fixture).await?;

    // A block before the fault: the proxy is transparent.
    let genesis = fixture.rpc("getbestblockhash", json!([])).await?;
    let mut client = BlockClient::current(fixture, 0, &genesis).await?;
    let before = client.mine().await?;
    wait_state(fixture, &before, "submitted", 30).await?;
    if fault == Fault::Kill {
        // A crash loses whatever the node has not flushed. Flush now so the
        // kill loses at most the held block, never the chain before it.
        fixture.rpc("gettxoutsetinfo", json!([])).await?;
    }

    // The held block, on current work that builds on the first one. It
    // ends with one outcome: a request the node read and answered after its
    // resume, the node's acceptance, or (never sent while the port refused)
    // the acceptance of its one offer after the restart.
    let pid = libc::pid_t::try_from(fixture.node.child.lock().id())?;
    let mut client = BlockClient::current(fixture, 0, &json!(before)).await?;
    let outcome = match hold {
        Hold::Request => "unknown",
        Hold::Reply | Hold::Reservation | Hold::Warmup => "accepted",
    };
    let held = match hold {
        Hold::Reservation | Hold::Warmup => {
            unsent_offer(fixture, proxy, &mut client, fault, pid, &before).await?
        }
        Hold::Request | Hold::Reply => {
            held_offer(fixture, proxy, &mut client, fault, pid, hold, outcome).await?
        }
    };
    wait_health(fixture, false, 60).await?;
    for index in 0..2 {
        let label = format!("server {index} metrics showing the node unavailable");
        until(&label, 30, || async {
            let during = metrics(fixture, index).await?;
            Ok(sample(&during, "qbit_prism_health_state") == Some(0.0)
                && sample(&during, "qbit_prism_node_observation_age_seconds")
                    .is_some_and(|age| age >= 5.0))
        })
        .await?;
    }
    // The accepting frontend's alarm for an accepted block that has not
    // landed; nothing was accepted while the request was held.
    ensure!(
        (unlanded(fixture).await? > 0.0) == (hold == Hold::Reply),
        "accepted-unlanded signal disagrees with the node's answer"
    );

    match fault {
        Fault::Stop => signal(pid, libc::SIGCONT)?,
        Fault::Kill if hold == Hold::Warmup => {
            restart_node_in_warmup(fixture, proxy, &held).await?
        }
        Fault::Kill => restart_node(fixture, reindex).await?,
    }
    if hold == Hold::Reservation {
        // The restarted node is out of its warmup: its port accepts again.
        proxy.reopen().await?;
    }
    wait_health(fixture, true, 60).await?;
    for index in 0..2 {
        let label = format!("server {index} metrics showing the node recovered");
        until(&label, 30, || async {
            let recovered = metrics(fixture, index).await?;
            Ok(sample(&recovered, "qbit_prism_health_state") == Some(1.0)
                && sample(&recovered, "qbit_prism_node_observation_age_seconds")
                    .is_some_and(|age| (0.0..5.0).contains(&age)))
        })
        .await?;
    }

    // The node resumed with the held request in its socket, accepted the
    // block and kept it (only a reindex recovers it after a crash), or was
    // never sent it and receives its one offer now: the block lands.
    // Otherwise the node lost the accepted block in the crash, and the row
    // reconciles to a proven orphan once a competing block has the orphan
    // confirmations; an offered row is never offered again.
    let lands = fault == Fault::Stop || reindex || matches!(hold, Hold::Reservation | Hold::Warmup);
    if lands {
        wait_state(fixture, &held, "submitted", 90).await?;
        let header = fixture.rpc("getblockheader", json!([held])).await?;
        ensure!(
            header["confirmations"].as_i64().unwrap_or(0) > 0,
            "the landed block is not on the node's active chain"
        );
    } else {
        ensure!(
            fixture.rpc("getblockheader", json!([held])).await.is_err(),
            "the node holds a block the case expects it never received or lost"
        );
        until("the held row released into reconciliation", 60, || async {
            Ok(sqlx::query_scalar::<_, bool>(
                "SELECT state='reconciliation' AND claim_token IS NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&held)
            .fetch_one(&fixture.pool)
            .await?)
        })
        .await?;
        fixture
            .rpc("generatetoaddress", json!([6, fixture.address]))
            .await?;
        wait_state(fixture, &held, "orphaned", 90).await?;
    }

    // Job delivery resumes: a new connection to the other server receives
    // work on the node's current tip, and its block lands.
    let tip = fixture.rpc("getbestblockhash", json!([])).await?;
    let mut client = BlockClient::current(fixture, 1, &tip).await?;
    let after = client.mine().await?;
    wait_state(fixture, &after, "submitted", 60).await?;

    fixture.quiesce().await?;
    let stuck: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state IN ('pending','offer_reserved','offered')",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(stuck == 0, "{stuck} candidate row(s) stuck before landing");
    // A call the node answered from its warmup never ran (#526): only the
    // warmup case sees one, and only for the held block.
    let submitted = proxy.submitted();
    let warmup = proxy.warmup_answered();
    ensure!(
        if hold == Hold::Warmup {
            !warmup.is_empty() && warmup.iter().all(|block| *block == held)
        } else {
            warmup.is_empty()
        },
        "unexpected submitblock calls answered in warmup: {warmup:?}"
    );
    let mut offers = HashMap::<&str, usize>::new();
    for block in &submitted {
        *offers.entry(block.as_str()).or_default() += 1;
    }
    for block in &warmup {
        *offers
            .get_mut(block.as_str())
            .context("a warmup answer without its call")? -= 1;
    }
    let expected = [before.as_str(), held.as_str(), after.as_str()];
    ensure!(
        offers.len() == expected.len()
            && expected.iter().all(|block| offers.get(block) == Some(&1)),
        "each block must be run by the node exactly once; submitblock calls: {submitted:?}, answered in warmup: {warmup:?}"
    );
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT block_hash,state,offer_outcome FROM qbit_block_candidate_outbox ORDER BY created_at",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let held_state = if lands { "submitted" } else { "orphaned" };
    ensure!(
        rows == vec![
            (before.clone(), "submitted".into(), Some("accepted".into())),
            (held.clone(), held_state.into(), Some(outcome.into())),
            (after.clone(), "submitted".into(), Some("accepted".into())),
        ],
        "unexpected candidate outcomes: {rows:?}"
    );
    let chain: Vec<(String, String)> =
        sqlx::query_as("SELECT block_hash,chain_state FROM qbit_pool_blocks ORDER BY block_height")
            .fetch_all(&fixture.pool)
            .await?;
    for block in [&before, &after] {
        ensure!(
            chain.contains(&(block.clone(), "confirmed".into())),
            "block {block} is not confirmed in the pool: {chain:?}"
        );
    }
    ensure!(
        lands == chain.contains(&(held.clone(), "confirmed".into())),
        "held block credit disagrees with the node: {chain:?}"
    );
    // A peer's orphan settlement reaches the accepting frontend through its
    // 10-second database collector.
    until("accepted-unlanded alarm cleared", 30, || async {
        Ok(unlanded(fixture).await? == 0.0)
    })
    .await?;
    fixture.integrity().await?;
    eprintln!(
        "live node outage ({fault:?}, held {hold:?}, reindex {reindex}): held block {held} ended {held_state} after {} submitblock call(s) in all, {} answered in warmup",
        submitted.len(),
        warmup.len()
    );
    Ok(())
}

/// The fault itself: `SIGSTOP` the node, or kill it.
fn inject(fixture: &mut Fixture, fault: Fault, pid: libc::pid_t) -> Result<()> {
    match fault {
        Fault::Stop => signal(pid, libc::SIGSTOP),
        Fault::Kill => {
            fixture.node.stop();
            Ok(())
        }
    }
}

/// Mine the held block with its `submitblock` held in the proxy (`hold`),
/// inject the fault, release the call, and wait for its recorded outcome.
async fn held_offer(
    fixture: &mut Fixture,
    proxy: &RpcProxy,
    client: &mut BlockClient,
    fault: Fault,
    pid: libc::pid_t,
    hold: Hold,
    outcome: &str,
) -> Result<String> {
    let hit = proxy.arm(hold);
    let held = client.mine().await?;
    let hit = tokio::time::timeout(Duration::from_secs(30), hit)
        .await
        .context("no submitblock reached the proxy")??;
    ensure!(
        hit.block == held,
        "held submitblock was for {}, not the mined block {held}",
        hit.block
    );
    inject(fixture, fault, pid)?;
    let _ = hit.release.send(());
    until("the held offer's outcome recorded", 30, || async {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&held)
        .fetch_one(&fixture.pool)
        .await?
        .as_deref()
            == Some(outcome))
    })
    .await?;
    Ok(held)
}

/// Mine the held block with its offer reservation held in the database,
/// inject the fault, close the proxy's port, and release the reservation:
/// the one `submitblock` finds the connection refused (#522). The row must
/// return to `pending` unsent and keep retrying, with nothing reaching the
/// node, while the port refuses.
async fn unsent_offer(
    fixture: &mut Fixture,
    proxy: &RpcProxy,
    client: &mut BlockClient,
    fault: Fault,
    pid: libc::pid_t,
    before: &str,
) -> Result<String> {
    let gate = ReservationGate::close(fixture).await?;
    let held = client.mine().await?;
    gate.wait_for_reservation(fixture).await?;
    inject(fixture, fault, pid)?;
    proxy.refuse().await?;
    gate.open(fixture).await?;
    wait_unsent(fixture, &held, "connection refused").await?;
    until("a second unsent offer while the port refuses", 30, || async {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT attempt_count>=2 AND state='pending' AND offer_outcome IS NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&held)
        .fetch_one(&fixture.pool)
        .await?)
    })
    .await?;
    ensure!(
        proxy.submitted() == [before],
        "a submitblock reached the node while its port refused: {:?}",
        proxy.submitted()
    );
    Ok(held)
}

fn signal(pid: libc::pid_t, signal: libc::c_int) -> Result<()> {
    // SAFETY: kill(2) takes no pointers; `pid` is the fixture's own child.
    ensure!(
        unsafe { libc::kill(pid, signal) } == 0,
        "signal {signal} to qbitd failed: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}

/// Restart the killed node on its data directory and RPC port, as the CPFP
/// cases restart theirs.
async fn restart_node(fixture: &mut Fixture, reindex: bool) -> Result<()> {
    spawn_node(fixture, reindex)?;
    until("qbit restarted after the crash", 60, || async {
        Ok(fixture.rpc("getblockcount", json!([])).await?.is_u64())
    })
    .await
}

fn spawn_node(fixture: &mut Fixture, reindex: bool) -> Result<()> {
    let mut node = Command::new(&fixture.qbitd);
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
        "-wallet=prism",
    ])
    .arg(format!("-datadir={}", fixture.directory.path().display()))
    .arg(format!("-rpcport={}", fixture.rpc_port))
    .arg(format!("-port={}", free_port()?));
    if reindex {
        node.arg("-reindex");
    }
    fixture.node = Process::spawn(
        &mut node,
        fixture.directory.path().join("qbit-restarted.log"),
    )?;
    Ok(())
}

/// Restart the killed node held in its warmup and reopen the proxy's port
/// (#526). qbitd opens its fee-estimates file in startup step 6, after its
/// RPC server is up (step 4a) and before its warmup ends (step 13); as a
/// FIFO, that open blocks until the test opens the other end, and until then
/// the node answers every call -28. The held block's retry reaches the node
/// and is answered from the warmup; the row must be back in `pending` with
/// that answer as its reason, and only then is the node released.
async fn restart_node_in_warmup(fixture: &mut Fixture, proxy: &RpcProxy, held: &str) -> Result<()> {
    let fifo = fixture
        .directory
        .path()
        .join("regtest")
        .join("fee_estimates.dat");
    match std::fs::remove_file(&fifo) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        _ => {}
    }
    let path = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes())?;
    // SAFETY: `path` is a valid NUL-terminated string for the call's duration.
    ensure!(
        unsafe { libc::mkfifo(path.as_ptr(), 0o600) } == 0,
        "mkfifo {}: {}",
        fifo.display(),
        std::io::Error::last_os_error()
    );
    spawn_node(fixture, false)?;
    until(
        "the restarted qbit answering from its warmup",
        60,
        || async { Ok(warmup_code(fixture).await == Some(RPC_IN_WARMUP)) },
    )
    .await?;
    proxy.reopen().await?;
    until("the held block's retry answered in warmup", 60, || async {
        Ok(proxy.warmup_answered().iter().any(|block| block == held))
    })
    .await?;
    wait_unsent(fixture, held, "\"code\":-28").await?;
    ensure!(
        warmup_code(fixture).await == Some(RPC_IN_WARMUP),
        "the node left its warmup while it was held"
    );
    // Release the startup: an empty fee-estimates file is read, rejected and
    // ignored. A non-blocking open for writing fails (ENXIO) until the node
    // is waiting in its open for reading, so a node that never gets there
    // fails the wait instead of hanging the test. The node still reads the
    // file's time after its open returns, so the FIFO goes only once the
    // warmup has ended; the node's later flush then writes a plain file.
    until(
        "the held node opening its fee-estimates file",
        30,
        || async {
            use std::os::unix::fs::OpenOptionsExt;
            Ok(std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&fifo)
                .is_ok())
        },
    )
    .await?;
    until("qbit out of its warmup", 60, || async {
        Ok(fixture.rpc("getblockcount", json!([])).await?.is_u64())
    })
    .await?;
    std::fs::remove_file(&fifo)?;
    Ok(())
}

/// The JSON-RPC error code of a direct `getblockcount`, `None` when it
/// succeeded or the node did not answer.
async fn warmup_code(fixture: &Fixture) -> Option<i64> {
    let reply: Value = fixture
        .client
        .post(format!("http://127.0.0.1:{}/", fixture.rpc_port))
        .basic_auth("prismtest", Some("prismtest"))
        .json(&json!({"jsonrpc":"1.0","id":"live-test","method":"getblockcount","params":[]}))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    reply["error"]["code"].as_i64()
}

/// Holds every offer reservation on the fixture's outbox: a trigger waits on
/// an advisory lock the test holds on its own connection (#522).
struct ReservationGate {
    connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    pid: i32,
}

impl ReservationGate {
    async fn close(fixture: &Fixture) -> Result<Self> {
        sqlx::raw_sql(&format!(
            "CREATE FUNCTION {schema}.hold_offer_reservation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtext(TG_TABLE_SCHEMA)::bigint); RETURN NEW; END $$; CREATE TRIGGER hold_offer_reservation BEFORE UPDATE OF state ON {schema}.qbit_block_candidate_outbox FOR EACH ROW WHEN (OLD.state='pending' AND NEW.state='offer_reserved') EXECUTE FUNCTION {schema}.hold_offer_reservation();",
            schema = fixture.schema
        ))
        .execute(&fixture.pool)
        .await?;
        let mut connection = fixture.pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock(hashtext($1)::bigint)")
            .bind(&fixture.schema)
            .execute(&mut *connection)
            .await?;
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await?;
        Ok(Self { connection, pid })
    }

    /// Until an offer reservation waits on the gate.
    async fn wait_for_reservation(&self, fixture: &Fixture) -> Result<()> {
        until("an offer reservation waiting on the gate", 30, || async {
            Ok(sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(self.pid)
            .fetch_one(&fixture.admin)
            .await?)
        })
        .await
    }

    /// Release the waiting reservation and remove the trigger.
    async fn open(mut self, fixture: &Fixture) -> Result<()> {
        sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
            .bind(&fixture.schema)
            .execute(&mut *self.connection)
            .await?;
        drop(self.connection);
        sqlx::raw_sql(&format!(
            "DROP TRIGGER hold_offer_reservation ON {schema}.qbit_block_candidate_outbox; DROP FUNCTION {schema}.hold_offer_reservation();",
            schema = fixture.schema
        ))
        .execute(&fixture.pool)
        .await?;
        Ok(())
    }
}

/// Until the held block's offer is recorded as not sent: back in `pending`
/// with no outcome and `answer` (the refusal, or the warmup reply) in its
/// reason. A recorded outcome fails at once: the offer would never be
/// offered again.
async fn wait_unsent(fixture: &Fixture, block: &str, answer: &str) -> Result<()> {
    let started = Instant::now();
    loop {
        let (state, outcome, error): (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT state,offer_outcome,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(block)
        .fetch_one(&fixture.pool)
        .await?;
        ensure!(
            outcome.is_none(),
            "the offer not sent ({answer}) was recorded with outcome {outcome:?} ({state}): {error:?}"
        );
        if state == "pending"
            && error
                .as_deref()
                .is_some_and(|error| error.starts_with("offer not sent") && error.contains(answer))
        {
            return Ok(());
        }
        ensure!(
            started.elapsed() < Duration::from_secs(30),
            "the offer not sent ({answer}) was not returned to pending: {state}, {error:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_health(fixture: &Fixture, ready: bool, seconds: u64) -> Result<()> {
    for index in 0..2 {
        let label = format!(
            "server {index} health {}",
            if ready { "ready" } else { "unavailable" }
        );
        until(&label, seconds, || async {
            let response = fixture
                .client
                .get(format!("http://127.0.0.1:{}/healthz", fixture.api[index]))
                .send()
                .await?;
            Ok(response.status().is_success() == ready)
        })
        .await?;
    }
    Ok(())
}

async fn metrics(fixture: &Fixture, index: usize) -> Result<String> {
    Ok(fixture
        .client
        .get(format!("http://127.0.0.1:{}/metrics", fixture.api[index]))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

/// A sample of an unlabelled gauge.
fn sample(body: &str, name: &str) -> Option<f64> {
    body.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
}

/// The oldest accepted-but-unlanded block's age across both servers.
async fn unlanded(fixture: &Fixture) -> Result<f64> {
    let mut oldest = 0.0f64;
    for index in 0..2 {
        let body = metrics(fixture, index).await?;
        oldest = oldest.max(
            sample(&body, "qbit_prism_accepted_block_unlanded_seconds")
                .context("accepted-unlanded gauge missing")?,
        );
    }
    Ok(oldest)
}

async fn wait_state(fixture: &Fixture, block: &str, state: &str, seconds: u64) -> Result<()> {
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

/// A Stratum client that mines one block per call on the current job.
struct BlockClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    username: String,
    extranonce1: String,
    extranonce2_size: usize,
    difficulty: Option<f64>,
    notify: Value,
    next_id: u64,
}

impl BlockClient {
    /// A new connection to `index` once that server is ready on `tip`, so
    /// its first job carries the current tip and payout revision.
    async fn current(fixture: &Fixture, index: usize, tip: &Value) -> Result<Self> {
        until(&format!("server {index} ready on {tip}"), 30, || async {
            let health: Value = fixture
                .client
                .get(format!("http://127.0.0.1:{}/healthz", fixture.api[index]))
                .send()
                .await?
                .json()
                .await?;
            Ok(health["ok"] == true && &health["observed_tip"] == tip)
        })
        .await?;
        let mut client = Self::open(fixture, index).await?;
        client
            .wait_for_parent(tip.as_str().context("tip hash missing")?)
            .await?;
        Ok(client)
    }

    async fn open(fixture: &Fixture, index: usize) -> Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", fixture.stratum[index])).await?;
        let (read, writer) = stream.into_split();
        let mut client = Self {
            reader: BufReader::new(read),
            writer,
            username: format!("{}.outage-{index}", fixture.address),
            extranonce1: String::new(),
            extranonce2_size: 0,
            difficulty: None,
            notify: Value::Null,
            next_id: 10,
        };
        client
            .send(json!({"id":1,"method":"mining.subscribe","params":["outage-regtest"]}))
            .await?;
        let subscribed = client.response(1).await?;
        client.extranonce1 = subscribed["result"][1]
            .as_str()
            .context("extranonce1 missing")?
            .into();
        client.extranonce2_size = subscribed["result"][2]
            .as_u64()
            .context("extranonce2 size missing")?
            .try_into()?;
        client
            .send(json!({"id":2,"method":"mining.authorize","params":[client.username,"x"]}))
            .await?;
        let authorized = client.response(2).await?;
        ensure!(authorized["result"] == true, "authorize: {authorized}");
        client.work().await?;
        Ok(client)
    }

    async fn send(&mut self, payload: Value) -> Result<()> {
        self.writer
            .write_all(format!("{payload}\n").as_bytes())
            .await?;
        Ok(())
    }

    async fn read(&mut self) -> Result<Value> {
        let mut line = String::new();
        let read = tokio::time::timeout(Duration::from_secs(20), self.reader.read_line(&mut line))
            .await
            .context("no Stratum message within 20 s")??;
        ensure!(read > 0, "Stratum connection closed");
        let message: Value = serde_json::from_str(&line)?;
        if message["method"] == "mining.set_difficulty" {
            self.difficulty = message["params"][0].as_f64();
        }
        if message["method"] == "mining.notify" {
            self.notify = message.clone();
        }
        Ok(message)
    }

    async fn response(&mut self, id: u64) -> Result<Value> {
        loop {
            let message = self.read().await?;
            if message["id"] == id {
                return Ok(message);
            }
        }
    }

    /// Until both a job and a share difficulty have arrived.
    async fn work(&mut self) -> Result<()> {
        while self.notify.is_null() || self.difficulty.is_none() {
            self.read().await?;
        }
        Ok(())
    }

    /// Until the current job builds on `parent` (display order).
    async fn wait_for_parent(&mut self, parent: &str) -> Result<()> {
        let mut wire = hex::decode(parent)?;
        wire.reverse();
        for word in wire.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let expected = hex::encode(wire);
        while self.notify["params"][1].as_str() != Some(expected.as_str()) {
            self.read().await?;
        }
        Ok(())
    }

    /// Solve the current job for a block, submit it, and return its hash.
    /// Nothing is awaited after the submission: a held reply must be
    /// released inside the server's 1-second `submitblock` deadline.
    async fn mine(&mut self) -> Result<String> {
        self.work().await?;
        let params = self.notify["params"]
            .as_array()
            .context("notify missing")?
            .clone();
        let field = |index: usize| params[index].as_str().context("invalid notify field");
        self.next_id += 1;
        let extranonce2 = format!(
            "{:0width$x}",
            self.next_id,
            width = self.extranonce2_size * 2
        );
        let coinbase = hex::decode(format!(
            "{}{}{extranonce2}{}",
            field(2)?,
            self.extranonce1,
            field(3)?
        ))?;
        let mut merkle = double_sha256(&coinbase);
        for sibling in params[4].as_array().context("merkle branch missing")? {
            let sibling = hex::decode(sibling.as_str().context("invalid sibling")?)?;
            merkle = double_sha256(&[merkle.as_slice(), sibling.as_slice()].concat());
        }
        let mut previous = hex::decode(field(1)?)?;
        for word in previous.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let bits = parse_u32_hex(field(6)?)?;
        let target = target_from_compact(bits)?.min(difficulty_target(
            self.difficulty.context("share difficulty missing")?,
        )?);
        let ntime = parse_u32_hex(field(7)?)?;
        let version = parse_u32_hex(field(5)?)?;
        for nonce in 0..1_000_000u32 {
            let header = [
                version.to_le_bytes().as_slice(),
                previous.as_slice(),
                merkle.as_slice(),
                ntime.to_le_bytes().as_slice(),
                bits.to_le_bytes().as_slice(),
                nonce.to_le_bytes().as_slice(),
            ]
            .concat();
            let hash = double_sha256(&header);
            if BigUint::from_bytes_le(&hash) <= target {
                let request = json!({"id":self.next_id,"method":"mining.submit","params":[self.username,field(0)?,extranonce2,format!("{ntime:08x}"),format!("{nonce:08x}")]});
                self.send(request).await?;
                return Ok(hash_display(&hash));
            }
        }
        bail!("no regtest block in the nonce budget")
    }
}

/// One held `submitblock`: its block hash, and the release the test sends
/// once the fault is in place.
struct Hit {
    block: String,
    release: oneshot::Sender<()>,
}

struct ProxyState {
    upstream: u16,
    submitted: Mutex<Vec<String>>,
    /// Blocks whose `submitblock` the node answered -28 from its warmup:
    /// calls it never ran (#526).
    warmup_answered: Mutex<Vec<String>>,
    trap: Mutex<Option<(Hold, oneshot::Sender<Hit>)>>,
    /// Closed while a held call waits for its release: every connection
    /// pauses before its next forward, so nothing reaches the node or the
    /// servers between the hold and the fault.
    open: watch::Sender<bool>,
    /// Every relayed connection, so a refusal can close them all.
    connections: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// A transparent HTTP/1.1 JSON-RPC proxy in front of the node.
struct RpcProxy {
    port: u16,
    state: Arc<ProxyState>,
    /// The accept loop, `None` while the proxy refuses connections.
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RpcProxy {
    async fn start(upstream: u16) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let state = Arc::new(ProxyState {
            upstream,
            submitted: Mutex::new(Vec::new()),
            warmup_answered: Mutex::new(Vec::new()),
            trap: Mutex::new(None),
            open: watch::Sender::new(true),
            connections: Mutex::new(Vec::new()),
        });
        let proxy = Self {
            port,
            state,
            task: Mutex::new(None),
        };
        proxy.serve(listener);
        Ok(proxy)
    }

    fn serve(&self, listener: tokio::net::TcpListener) {
        let shared = self.state.clone();
        *self.task.lock().unwrap() = Some(tokio::spawn(async move {
            while let Ok((downstream, _)) = listener.accept().await {
                let state = shared.clone();
                let connection = tokio::spawn(async move {
                    // A failed connection is what the server sees as a
                    // transport failure; nothing else to report.
                    let _ = relay(state, downstream).await;
                });
                shared.connections.lock().unwrap().push(connection);
            }
        }));
    }

    /// Close the listener and every relayed connection, as a dead node's
    /// port is closed, and prove that a new connection is refused.
    async fn refuse(&self) -> Result<()> {
        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        let connections = std::mem::take(&mut *self.state.connections.lock().unwrap());
        for connection in connections {
            connection.abort();
            let _ = connection.await;
        }
        let refused = TcpStream::connect(("127.0.0.1", self.port)).await;
        ensure!(
            refused
                .as_ref()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionRefused),
            "the proxy port still accepts connections: {refused:?}"
        );
        Ok(())
    }

    /// Accept connections on the same port again.
    async fn reopen(&self) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", self.port)).await?;
        self.serve(listener);
        Ok(())
    }

    fn stop(&self) {
        if let Some(task) = self.task.lock().unwrap().take() {
            task.abort();
        }
        for connection in self.state.connections.lock().unwrap().drain(..) {
            connection.abort();
        }
    }

    fn arm(&self, hold: Hold) -> oneshot::Receiver<Hit> {
        let (sender, receiver) = oneshot::channel();
        *self.state.trap.lock().unwrap() = Some((hold, sender));
        receiver
    }

    fn submitted(&self) -> Vec<String> {
        self.state.submitted.lock().unwrap().clone()
    }

    fn warmup_answered(&self) -> Vec<String> {
        self.state.warmup_answered.lock().unwrap().clone()
    }
}

/// Relays one client connection, one request and reply at a time, as
/// HTTP/1.1 without pipelining does.
async fn relay(state: Arc<ProxyState>, downstream: TcpStream) -> Result<()> {
    let upstream = TcpStream::connect(("127.0.0.1", state.upstream)).await?;
    let (down_read, mut down_write) = downstream.into_split();
    let (up_read, mut up_write) = upstream.into_split();
    let mut down_read = BufReader::new(down_read);
    let mut up_read = BufReader::new(up_read);
    let mut open = state.open.subscribe();
    loop {
        // A node connection that closes while idle (qbitd's idle timeout,
        // or a kill) closes the server's connection too, as a direct
        // connection would, so the server never reuses a dead one.
        let next = tokio::select! {
            next = read_http(&mut down_read) => next?,
            _ = up_read.fill_buf() => return Ok(()),
        };
        let Some((request, body)) = next else {
            return Ok(());
        };
        let submitted = submitted_block(&body);
        let trap = submitted.as_ref().and_then(|block| {
            state.submitted.lock().unwrap().push(block.clone());
            let trap = state.trap.lock().unwrap().take();
            trap.map(|(hold, sender)| (block, hold, sender))
        });
        let (before, after) = match trap {
            Some((block, Hold::Request, sender)) => (Some((block.clone(), sender)), None),
            Some((block, Hold::Reply, sender)) => (None, Some((block.clone(), sender))),
            // Never armed: a reservation is held in the database.
            Some((_, Hold::Reservation | Hold::Warmup, _)) | None => (None, None),
        };
        if let Some((block, sender)) = before {
            hold_here(&state, block, sender).await;
        }
        open.wait_for(|open| *open).await?;
        up_write.write_all(&request).await?;
        let Some((reply, reply_body)) = read_http(&mut up_read).await? else {
            return Ok(());
        };
        if let Some(block) = &submitted {
            let answer: Value = serde_json::from_slice(&reply_body).unwrap_or_default();
            if answer["error"]["code"] == RPC_IN_WARMUP {
                state.warmup_answered.lock().unwrap().push(block.clone());
            }
        }
        if let Some((block, sender)) = after {
            hold_here(&state, block, sender).await;
        }
        open.wait_for(|open| *open).await?;
        down_write.write_all(&reply).await?;
    }
}

async fn hold_here(state: &ProxyState, block: String, sender: oneshot::Sender<Hit>) {
    state.open.send_replace(false);
    let (release, released) = oneshot::channel();
    if sender.send(Hit { block, release }).is_ok() {
        let _ = released.await;
    }
    state.open.send_replace(true);
}

/// The block hash of a `submitblock` request body, if it is one.
fn submitted_block(body: &[u8]) -> Option<String> {
    let request: Value = serde_json::from_slice(body).ok()?;
    if request["method"] != "submitblock" {
        return None;
    }
    let block = hex::decode(request["params"][0].as_str()?).ok()?;
    Some(hash_display(&double_sha256(block.get(..80)?)))
}

/// One HTTP/1.1 message with a `Content-Length` body: the raw bytes and the
/// body. `None` at a clean end of stream. qbitd's JSON-RPC server and the
/// servers' client both frame every message by `Content-Length`; anything
/// else fails the connection loudly rather than being misread.
async fn read_http<R>(reader: &mut BufReader<R>) -> Result<Option<(Vec<u8>, Vec<u8>)>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut raw = Vec::new();
    let mut length = None;
    loop {
        let start = raw.len();
        if reader.read_until(b'\n', &mut raw).await? == 0 {
            ensure!(raw.is_empty(), "connection closed inside an HTTP header");
            return Ok(None);
        }
        let line = std::str::from_utf8(&raw[start..])?.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = Some(value.trim().parse::<usize>()?);
            }
        }
    }
    let mut body = vec![0; length.context("HTTP message without Content-Length")?];
    reader.read_exact(&mut body).await?;
    raw.extend_from_slice(&body);
    Ok(Some((raw, body)))
}

//! #522 on a real PostgreSQL: a `submitblock` whose connection was never
//! established returns its reservation to `pending` and the block is offered
//! once the node is reachable again, exactly once, as does one whose connect
//! outlasted its share of the deadline (#762); a failure after the request was
//! written, the deadline included, stays unknown and is never offered again;
//! and a crash between the refused connect and the row update recovers as
//! delivery unknown. The offering frontend reaches the fixture's node through
//! a small TCP relay the test controls, so "refused" is a closed port and a
//! reset is a real RST.
use super::*;
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[path = "offer_warmup_tests.rs"]
mod offer_warmup_tests;

/// What the relay does with a `submitblock` request.
#[derive(Clone, Copy, PartialEq)]
enum Submit {
    Forward,
    /// Forward it, read the node's reply, and reset the client's connection
    /// instead of relaying the reply: the node acted on the request and the
    /// offering frontend cannot know.
    ResetAfterReply,
    /// Forward it, read the node's reply, and hold the client's connection
    /// without relaying the reply: the offering frontend's deadline passes
    /// after the node acted on the request (#762).
    HoldReply,
    /// Answer it as qbitd does while it is still warming up after a restart,
    /// without forwarding it: the node never sees the block (#526).
    Warmup,
    /// Forward it, relay the node's reply, and then switch to `Warming`: the
    /// node restarted into its warmup right after it ran the call (#526).
    WarmupAfterReply,
    /// Answer every call, of any method, as a node still warming up (#526).
    Warming,
}

struct RelayState {
    upstream: SocketAddr,
    submit: std::sync::Mutex<Submit>,
    connections: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

/// A TCP relay in front of the fixture's node that can refuse connections
/// outright: [`Relay::refuse`] closes the listener and every relayed
/// connection, so a new connection gets a TCP reset, as a stopped node's
/// port does.
struct Relay {
    address: SocketAddr,
    state: Arc<RelayState>,
    listener: Option<JoinHandle<()>>,
}

impl Relay {
    async fn start(upstream: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let state = Arc::new(RelayState {
            upstream,
            submit: std::sync::Mutex::new(Submit::Forward),
            connections: std::sync::Mutex::new(Vec::new()),
        });
        let mut relay = Self {
            address,
            state,
            listener: None,
        };
        relay.serve(listener);
        Ok(relay)
    }

    fn url(&self) -> String {
        format!("http://{}/", self.address)
    }

    fn serve(&mut self, listener: TcpListener) {
        let state = self.state.clone();
        self.listener = Some(tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let connection = tokio::spawn(relay(state.clone(), client));
                state.connections.lock().unwrap().push(connection);
            }
        }));
    }

    fn submit(&self, submit: Submit) {
        *self.state.submit.lock().unwrap() = submit;
    }

    /// The node finished its warmup: calls are forwarded again.
    fn warm(&self) {
        self.submit(Submit::Forward);
    }

    async fn refuse(&mut self) -> Result<()> {
        if let Some(listener) = self.listener.take() {
            listener.abort();
            let _ = listener.await;
        }
        let connections = std::mem::take(&mut *self.state.connections.lock().unwrap());
        for connection in connections {
            connection.abort();
            let _ = connection.await;
        }
        let refused = TcpStream::connect(self.address).await;
        ensure!(
            refused
                .as_ref()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionRefused),
            "the relay port still accepts connections: {refused:?}"
        );
        // Give the offering frontend's pooled connections time to see their
        // end of stream, as they would after a node stopped.
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn reopen(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.address).await?;
        self.serve(listener);
        Ok(())
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
        for connection in self.state.connections.lock().unwrap().iter() {
            connection.abort();
        }
    }
}

/// One HTTP/1.1 message with a `Content-Length` body: the raw bytes and the
/// body, or `None` at a clean end of stream.
async fn read_http<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut raw = Vec::new();
    let mut length = 0;
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
                length = value.trim().parse()?;
            }
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await?;
    raw.extend_from_slice(&body);
    Ok(Some((raw, body)))
}

async fn relay(state: Arc<RelayState>, client: TcpStream) {
    let Ok(upstream) = TcpStream::connect(state.upstream).await else {
        return;
    };
    let mut client = BufReader::new(client);
    let mut upstream = BufReader::new(upstream);
    while let Ok(Some((request, body))) = read_http(&mut client).await {
        let call = serde_json::from_slice::<Value>(&body).unwrap_or_default();
        let submit = call["method"] == "submitblock";
        let mode = *state.submit.lock().unwrap();
        if mode == Submit::Warming || (submit && mode == Submit::Warmup) {
            if client
                .get_mut()
                .write_all(&warmup_reply(&call["id"]))
                .await
                .is_err()
            {
                return;
            }
            continue;
        }
        if upstream.get_mut().write_all(&request).await.is_err() {
            return;
        }
        let Ok(Some((reply, _))) = read_http(&mut upstream).await else {
            return;
        };
        if submit && mode == Submit::WarmupAfterReply {
            *state.submit.lock().unwrap() = Submit::Warming;
        }
        if submit && mode == Submit::ResetAfterReply {
            let client = client.into_inner();
            let _ = client.set_zero_linger();
            return;
        }
        if submit && mode == Submit::HoldReply {
            return std::future::pending().await;
        }
        if client.get_mut().write_all(&reply).await.is_err() {
            return;
        }
    }
}

/// A node address whose connects an `https` client can never finish: the
/// listener accepts and holds each connection without answering, so the TLS
/// handshake, which is part of the connect, never completes (#762).
async fn handshake_stall() -> Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let held = tokio::spawn(async move {
        let mut connections = Vec::new();
        while let Ok((connection, _)) = listener.accept().await {
            connections.push(connection);
        }
    });
    Ok((address, held))
}

/// qbitd's answer to a JSON-RPC 1.0 call while it is still warming up, as
/// its HTTP server writes it: status 500 and the `RPC_IN_WARMUP` error.
fn warmup_reply(id: &Value) -> Vec<u8> {
    let body = format!(
        "{}\n",
        json!({"result": null, "error": {"code": crate::rpc::RPC_IN_WARMUP, "message": "Loading banlist…"}, "id": id})
    );
    format!(
        "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

impl Fixture {
    /// A third frontend on the fixture's database that reaches the node
    /// through `relay`.
    async fn relayed_frontend(&self, relay: &Relay, instance_id: &str) -> Result<Arc<Coordinator>> {
        let frontend = Coordinator::new(
            Config {
                rpc_url: relay.url(),
                instance_id: instance_id.into(),
                ..(*self.coordinator.config).clone()
            },
            Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        *frontend.observed_tip.write().await = TipState::baseline("aa".repeat(32));
        Ok(frontend)
    }

    fn node_address(&self) -> Result<SocketAddr> {
        let url = url::Url::parse(&self.coordinator.config.rpc_url)?;
        Ok(SocketAddr::new(
            url.host_str().context("node host")?.parse()?,
            url.port().context("node port")?,
        ))
    }

    async fn backed_off(&self) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT next_attempt_at>clock_timestamp() FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&self.claim.candidate.block_hash)
        .fetch_one(&self.coordinator.ledger.pool)
        .await?)
    }

    async fn unknown_outcomes(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM qbit_block_candidate_outbox WHERE offer_outcome='unknown'",
        )
        .fetch_one(&self.coordinator.ledger.pool)
        .await?)
    }
}

/// The node's port refuses the connection: nothing reached the node, so the
/// row returns to `pending` with the attempt recorded, no outcome, no call
/// time and no first-offer sample, and backs off. Once the node is reachable
/// the next claim offers the block, and it lands with exactly one
/// `submitblock` in all. A classifier that took the refusal for an unknown
/// outcome would reconcile the row and never offer the block: lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_submitblock_returns_the_row_to_pending_and_lands_the_block_once_when_the_node_is_back(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let mut relay = Relay::start(fixture.node_address()?).await?;
    let frontend = fixture.relayed_frontend(&relay, "candidate-unsent").await?;
    let result = async {
        relay.refuse().await?;
        frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        let row = fixture.row().await?;
        let reason = row.last_error.clone().unwrap_or_default();
        ensure!(
            row.state == "pending"
                && row.token.is_none()
                && row.outcome.is_none()
                && row.offered_at_ms.is_none()
                && row.reserved_by.is_none()
                && row.evidence,
            "a refused offer did not return the row to pending with its offer columns empty: state {}, outcome {:?}, reason {reason}",
            row.state,
            row.outcome
        );
        ensure!(
            reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX)
                && reason.contains("connection refused")
                && reason.contains("candidate-unsent"),
            "the not-sent attempt is not recorded: {reason}"
        );
        ensure!(fixture.backed_off().await?, "the unsent row did not back off");
        ensure!(fixture.submissions().await == 0);
        ensure!(fixture.unknown_outcomes().await? == 0, "a not-sent attempt was counted as an unknown outcome");
        ensure!(
            first_offer_samples(&frontend.metrics) == 0,
            "an attempt that never reached the node recorded a first-offer sample"
        );
        // While the port still refuses, the next attempt is not sent either.
        fixture.expire().await?;
        let again = frontend
            .ledger
            .claim_candidate(10)
            .await?
            .context("the unsent row was not claimable")?;
        ensure!(again.lifecycle.state == CandidateState::Pending);
        frontend
            .process_candidate_with_lease(&again, CANDIDATE_LEASE)
            .await?;
        ensure!(fixture.state().await? == "pending" && fixture.submissions().await == 0);
        // The node is reachable again.
        relay.reopen().await?;
        fixture.expire().await?;
        let offer = frontend
            .ledger
            .claim_candidate(10)
            .await?
            .context("the unsent row was not claimable after the node returned")?;
        frontend
            .process_candidate_with_lease(&offer, CANDIDATE_LEASE)
            .await?;
        let row = fixture.row().await?;
        ensure!(
            row.state == "submitted"
                && row.outcome.as_deref() == Some("accepted")
                && row.offered_at_ms.is_some()
                && row.reserved_by.as_deref() == Some("candidate-unsent"),
            "the re-offered block did not land: state {}, outcome {:?}",
            row.state,
            row.outcome
        );
        ensure!(fixture.landed().await?);
        ensure!(fixture.submissions().await == 1, "the block was offered more than once");
        ensure!(first_offer_samples(&frontend.metrics) == 1);
        fixture.expire().await?;
        ensure!(fixture.successor.claim_candidate(10).await?.is_none());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(relay);
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}

/// The request was written and the node accepted the block, but the reply
/// was lost to a reset: delivery is unknown, the row never returns to
/// `pending`, and no frontend offers the block again. A classifier that took
/// the reset for "not sent" would offer the block a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_after_the_submitblock_was_written_stays_unknown_and_is_never_offered_again(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let relay = Relay::start(fixture.node_address()?).await?;
    let frontend = fixture.relayed_frontend(&relay, "candidate-reset").await?;
    let result = async {
        relay.submit(Submit::ResetAfterReply);
        frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        relay.submit(Submit::Forward);
        let row = fixture.row().await?;
        ensure!(
            row.state != "pending" && row.outcome.as_deref() == Some("unknown") && row.offered_at_ms.is_some(),
            "a reset after the write was not recorded as an unknown outcome: state {}, outcome {:?}, reason {:?}",
            row.state,
            row.outcome,
            row.last_error
        );
        ensure!(fixture.submissions().await == 1);
        // Every later claim, on any frontend, observes and never offers.
        for _ in 0..2 {
            fixture.expire().await?;
            if let Some(claim) = fixture.successor.claim_candidate(10).await? {
                ensure!(claim.lifecycle.state != CandidateState::Pending);
                fixture
                    .successor_coordinator
                    .process_candidate_with_lease(&claim, CANDIDATE_LEASE)
                    .await?;
            }
        }
        ensure!(
            fixture.submissions().await == 1,
            "the block was offered again after a reset that followed the write"
        );
        ensure!(fixture.state().await? == "submitted");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(relay);
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}

/// #762: the frontend has no connection to its node, and the one it opens
/// cannot finish within the `submitblock` deadline: here the node never
/// completes the TLS handshake, as a lost SYN, a full accept queue or a slow
/// name lookup would hold a plain connect. The call stops waiting for it at
/// half the deadline, so the request was never written: the row returns to
/// `pending`, with that reason, as after a refused connection, and the next
/// claim lands the block with one `submitblock`. Before #762 the deadline fired during the connect, the
/// offer was recorded as unknown, and the block was never offered again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_submitblock_whose_connect_outlasts_the_submit_deadline_returns_the_row_to_pending_and_lands_the_block_once(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let (stall, held) = handshake_stall().await?;
    let config = (*fixture.coordinator.config).clone();
    let mut frontend = Coordinator::new(
        Config {
            instance_id: "candidate-slow-connect".into(),
            ..config.clone()
        },
        Arc::new(crate::metrics::Metrics::default()),
    )
    .await?;
    *frontend.observed_tip.write().await = TipState::baseline("aa".repeat(32));
    // Started against the node, the frontend now reaches it only through a
    // connect that never finishes.
    Arc::get_mut(&mut frontend)
        .context("the new frontend is already shared")?
        .rpc = Rpc::new(
        format!("https://{stall}/"),
        config.rpc_user,
        config.rpc_password,
        config.rpc_timeout,
    )?;
    let result = async {
        frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        let row = fixture.row().await?;
        let reason = row.last_error.clone().unwrap_or_default();
        ensure!(
            row.state == "pending"
                && row.token.is_none()
                && row.outcome.is_none()
                && row.offered_at_ms.is_none()
                && row.reserved_by.is_none()
                && row.evidence,
            "a connect cut short by the deadline did not return the row to pending with its offer columns empty: state {}, outcome {:?}, reason {reason}",
            row.state,
            row.outcome
        );
        ensure!(
            reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX)
                && reason.contains("no connection within 500ms, its share of the call's 1s deadline")
                && reason.contains("candidate-slow-connect"),
            "the not-sent attempt is not recorded: {reason}"
        );
        ensure!(fixture.backed_off().await?, "the unsent row did not back off");
        ensure!(fixture.submissions().await == 0);
        ensure!(fixture.unknown_outcomes().await? == 0, "a connect cut short by the deadline was counted as an unknown outcome");
        ensure!(
            first_offer_samples(&frontend.metrics) == 0,
            "an attempt that never reached the node recorded a first-offer sample"
        );
        // The next claim, on a frontend that connects, offers the block.
        fixture.expire().await?;
        let offer = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("the unsent row was not claimable")?;
        ensure!(offer.lifecycle.state == CandidateState::Pending);
        fixture
            .successor_coordinator
            .process_candidate_with_lease(&offer, CANDIDATE_LEASE)
            .await?;
        let row = fixture.row().await?;
        ensure!(
            row.state == "submitted"
                && row.outcome.as_deref() == Some("accepted")
                && row.reserved_by.as_deref() == Some("candidate-successor"),
            "the re-offered block did not land: state {}, outcome {:?}",
            row.state,
            row.outcome
        );
        ensure!(fixture.landed().await?);
        ensure!(fixture.submissions().await == 1, "the block was offered more than once");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    held.abort();
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}

/// #762's other side: the `submitblock` was written and the node accepted the
/// block, but no reply came back before the deadline. The request may have
/// run, so delivery is unknown, the row never returns to `pending`, and no
/// frontend offers the block again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_submitblock_deadline_that_passes_after_the_request_was_written_stays_unknown_and_is_never_offered_again(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let relay = Relay::start(fixture.node_address()?).await?;
    let frontend = fixture.relayed_frontend(&relay, "candidate-held").await?;
    let result = async {
        relay.submit(Submit::HoldReply);
        frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        relay.submit(Submit::Forward);
        let row = fixture.row().await?;
        let reason = row.last_error.clone().unwrap_or_default();
        ensure!(
            row.state != "pending"
                && row.outcome.as_deref() == Some("unknown")
                && row.offered_at_ms.is_some()
                && !reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX),
            "a deadline after the write was not recorded as an unknown outcome: state {}, outcome {:?}, reason {reason}",
            row.state,
            row.outcome
        );
        ensure!(fixture.submissions().await == 1);
        // Every later claim, on any frontend, observes and never offers.
        for _ in 0..2 {
            fixture.expire().await?;
            if let Some(claim) = fixture.successor.claim_candidate(10).await? {
                ensure!(claim.lifecycle.state != CandidateState::Pending);
                fixture
                    .successor_coordinator
                    .process_candidate_with_lease(&claim, CANDIDATE_LEASE)
                    .await?;
            }
        }
        ensure!(
            fixture.submissions().await == 1,
            "the block was offered again after a deadline that followed the write"
        );
        ensure!(fixture.state().await? == "submitted");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(relay);
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}

/// The connection was refused, and the frontend died before the row update
/// committed (here the update itself fails). The reservation stays with no
/// recorded call, and nothing durable proves that the call was not made, so
/// recovery treats it as delivery unknown and never offers it, even though
/// the node is reachable again and would accept it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_the_refused_connect_and_the_row_update_recovers_as_delivery_unknown(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let mut relay = Relay::start(fixture.node_address()?).await?;
    let frontend = fixture
        .relayed_frontend(&relay, "candidate-crashed")
        .await?;
    let result = async {
        sqlx::raw_sql("CREATE FUNCTION fail_unsent_release() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'the frontend died before the unsent release committed'; END $$; CREATE TRIGGER fail_unsent_release BEFORE UPDATE OF state ON qbit_block_candidate_outbox FOR EACH ROW WHEN (OLD.state='offer_reserved' AND NEW.state='pending') EXECUTE FUNCTION fail_unsent_release();")
            .execute(&fixture.coordinator.ledger.pool).await?;
        relay.refuse().await?;
        let failed = frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await;
        ensure!(failed.is_err(), "the release could not commit, yet processing succeeded");
        let row = fixture.row().await?;
        ensure!(
            row.state == "offer_reserved" && row.outcome.is_none() && row.offered_at_ms.is_none(),
            "the failed release changed the reservation: state {}, outcome {:?}",
            row.state,
            row.outcome
        );
        sqlx::raw_sql("DROP TRIGGER fail_unsent_release ON qbit_block_candidate_outbox; DROP FUNCTION fail_unsent_release();")
            .execute(&fixture.coordinator.ledger.pool).await?;
        relay.reopen().await?;
        for _ in 0..2 {
            fixture.expire().await?;
            let recovered = fixture
                .successor
                .claim_candidate(10)
                .await?
                .context("the reservation was not recoverable")?;
            ensure!(recovered.lifecycle.state != CandidateState::Pending);
            fixture
                .successor_coordinator
                .process_candidate_with_lease(&recovered, CANDIDATE_LEASE)
                .await?;
        }
        let row = fixture.row().await?;
        ensure!(
            row.state == "reconciliation" && row.outcome.as_deref() == Some("unknown"),
            "the recovered reservation was not settled as delivery unknown: state {}, outcome {:?}",
            row.state,
            row.outcome
        );
        ensure!(
            fixture.submissions().await == 0,
            "a recovered reservation without a recorded call was offered"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(relay);
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}

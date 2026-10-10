//! A TCP relay the fault injector can cut: every link of the simulated
//! network that a fault may break goes through one (the peer-sync DSNs, the
//! balancer's routes and health checks, and in the 3.0 topology the writer
//! endpoint and the standby's replication link).
//!
//! A link is in one of three states:
//!
//! - **Open:** bytes flow.
//! - **Reset:** every connection is closed with a TCP reset and every new one
//!   is accepted and reset at once, as a host that is gone or rebooted
//!   answers. Each side learns of the cut on its next read or write.
//! - **Blackholed:** nothing is forwarded and nothing is closed. A new
//!   connection is accepted and held without an upstream. This is how a
//!   dropped VLAN or a dead switch looks: each side learns only through its
//!   own timeouts or keepalives (the #761 §1.1 lock-wedge class). Healing
//!   resumes every held connection where it stopped, as TCP retransmission
//!   does once a path returns, unless an endpoint gave up meanwhile.
//! - **Discard:** every byte either side sends is read and thrown away, and
//!   no close is passed on, even when one side closes. A new connection is
//!   held as when blackholed. This is a dead host as its peer's database
//!   server sees it: its replies drain and it never hears a close, so only
//!   its own session timeouts end the session. (Blackholed, a server still
//!   sending a result would block on a full window instead.) When the link
//!   leaves Discard, for any state, every connection that lived through it
//!   is reset on both sides, as the dead host's restarted kernel would.
//!
//! Adapted from the `Relay` of
//! `crates/qbit-prism-server/tests/support/live_pg_failover.rs`, which
//! fences and re-routes a writer endpoint.

use anyhow::{Context, Result};
use serde::Serialize;
use std::sync::{
    atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::watch,
    task::JoinHandle,
};

/// What a link does with the bytes offered to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkState {
    Open,
    Reset,
    Blackholed,
    Discard,
}

/// Counts for the scenario report.
#[derive(Default, Debug)]
struct Counters {
    accepted: AtomicU64,
    reset: AtomicU64,
    upstream_failures: AtomicU64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RelayStats {
    pub name: String,
    pub state: LinkState,
    pub accepted: u64,
    pub reset: u64,
    pub upstream_failures: u64,
}

pub struct Relay {
    name: String,
    port: u16,
    target: Arc<AtomicU16>,
    state: watch::Sender<LinkState>,
    counters: Arc<Counters>,
    task: JoinHandle<()>,
}

impl Relay {
    /// A relay on a fresh loopback port, open, in front of `target`.
    pub async fn open(name: &str, target: u16) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .with_context(|| format!("binding relay {name}"))?;
        let port = listener.local_addr()?.port();
        let target = Arc::new(AtomicU16::new(target));
        let (state, _) = watch::channel(LinkState::Open);
        let counters = Arc::new(Counters::default());
        let task = tokio::spawn(accept_loop(
            listener,
            target.clone(),
            state.subscribe(),
            counters.clone(),
        ));
        Ok(Self {
            name: name.to_owned(),
            port,
            target,
            state,
            counters,
            task,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The port clients connect to.
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn state(&self) -> LinkState {
        *self.state.borrow()
    }

    pub fn set(&self, state: LinkState) {
        self.state.send_replace(state);
    }

    /// Point new connections at another port (a restarted child's, say).
    pub fn retarget(&self, target: u16) {
        self.target.store(target, Ordering::SeqCst);
    }

    pub fn stats(&self) -> RelayStats {
        RelayStats {
            name: self.name.clone(),
            state: self.state(),
            accepted: self.counters.accepted.load(Ordering::Relaxed),
            reset: self.counters.reset.load(Ordering::Relaxed),
            upstream_failures: self.counters.upstream_failures.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.state.send_replace(LinkState::Reset);
        self.task.abort();
    }
}

async fn accept_loop(
    listener: TcpListener,
    target: Arc<AtomicU16>,
    state: watch::Receiver<LinkState>,
    counters: Arc<Counters>,
) {
    while let Ok((client, _)) = listener.accept().await {
        counters.accepted.fetch_add(1, Ordering::Relaxed);
        let _ = client.set_nodelay(true);
        let state = state.clone();
        let target = target.clone();
        let counters = counters.clone();
        tokio::spawn(async move {
            connection(client, target, state, counters).await;
        });
    }
}

/// Wait until the link's state satisfies `until`, and return it; `None`
/// when the relay is gone.
async fn wait_state(
    state: &mut watch::Receiver<LinkState>,
    until: impl Fn(LinkState) -> bool,
) -> Option<LinkState> {
    loop {
        let now = *state.borrow_and_update();
        if until(now) {
            return Some(now);
        }
        if state.changed().await.is_err() {
            return None;
        }
    }
}

/// Wait until the link is open. `false` when it is reset (or the relay is
/// gone) instead; a blackhole or a discarding link holds a new connection.
async fn opened(state: &mut watch::Receiver<LinkState>) -> bool {
    wait_state(state, |s| matches!(s, LinkState::Open | LinkState::Reset)).await
        == Some(LinkState::Open)
}

/// Close with a reset rather than a FIN: `SO_LINGER` zero, which makes the
/// close abortive and so never blocks.
pub fn reset(stream: TcpStream) {
    abort_on_close(&stream);
    drop(stream);
}

/// Make the next close of `stream` a reset.
pub fn abort_on_close(stream: &TcpStream) {
    let _ = socket2::SockRef::from(stream).set_linger(Some(std::time::Duration::ZERO));
}

async fn connection(
    client: TcpStream,
    target: Arc<AtomicU16>,
    mut state: watch::Receiver<LinkState>,
    counters: Arc<Counters>,
) {
    // A connection accepted while blackholed is held with no upstream.
    if !opened(&mut state).await {
        counters.reset.fetch_add(1, Ordering::Relaxed);
        reset(client);
        return;
    }
    let upstream = match TcpStream::connect(("127.0.0.1", target.load(Ordering::SeqCst))).await {
        Ok(upstream) => upstream,
        Err(_) => {
            counters.upstream_failures.fetch_add(1, Ordering::Relaxed);
            reset(client);
            return;
        }
    };
    let _ = upstream.set_nodelay(true);
    let (client_read, client_write) = client.into_split();
    let (upstream_read, upstream_write) = upstream.into_split();
    // Set once the connection lives through Discard, in either direction.
    let discarded = Arc::new(AtomicBool::new(false));
    let outbound = tokio::spawn(pump(
        client_read,
        upstream_write,
        state.clone(),
        discarded.clone(),
    ));
    let inbound = tokio::spawn(pump(
        upstream_read,
        client_write,
        state.clone(),
        discarded.clone(),
    ));
    // Each pump hands its halves back when it ends, so a reset can abort
    // both sides instead of letting a half-closed connection linger.
    let (outbound, inbound) = (outbound.await, inbound.await);
    // A reset ends every connection with a reset. So does leaving Discard for
    // every connection that lived through it, whether or not it lost bytes:
    // the host Discard modelled as dead does not know the connection, and its
    // restarted kernel answers the next segment with a reset (and any bytes
    // dropped left a hole a resumed stream could not survive).
    let reset_seen =
        matches!(*state.borrow(), LinkState::Reset) || discarded.load(Ordering::SeqCst);
    if let (Ok((client_read, upstream_write)), Ok((upstream_read, client_write))) =
        (outbound, inbound)
    {
        if reset_seen {
            counters.reset.fetch_add(1, Ordering::Relaxed);
            if let Ok(client) = client_read.reunite(client_write) {
                reset(client);
            }
            if let Ok(upstream) = upstream_read.reunite(upstream_write) {
                reset(upstream);
            }
        }
    }
}

/// What a link in motion does with bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flow {
    Forward,
    Drop,
}

/// Wait until bytes move: forwarded when the link is open, dropped when it
/// discards; `None` when it is reset (or the relay is gone). A blackhole
/// waits.
async fn flowing(state: &mut watch::Receiver<LinkState>) -> Option<Flow> {
    match wait_state(state, |s| s != LinkState::Blackholed).await? {
        LinkState::Open => Some(Flow::Forward),
        LinkState::Discard => Some(Flow::Drop),
        LinkState::Reset | LinkState::Blackholed => None,
    }
}

/// What became of a chunk a pump forwarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Forwarded {
    /// All of it reached the receiver.
    Written,
    /// The link discards: the rest of it was dropped.
    Dropped,
    /// The link was reset, or the receiver is gone while the link is open.
    Ended,
}

/// Write `chunk` to `to` a piece at a time, re-checking the link between
/// pieces: a blackhole holds what is left until the link moves again, a
/// discarding link drops it, a reset ends it. `write` is cancel-safe (a
/// cancelled write wrote nothing), so a state change can interrupt a write
/// stuck on a receiver that stopped reading.
async fn forward(
    to: &mut OwnedWriteHalf,
    chunk: &[u8],
    state: &mut watch::Receiver<LinkState>,
) -> Forwarded {
    let mut sent = 0;
    while sent < chunk.len() {
        match flowing(state).await {
            None => return Forwarded::Ended,
            Some(Flow::Drop) => return Forwarded::Dropped,
            Some(Flow::Forward) => {}
        }
        tokio::select! {
            written = to.write(&chunk[sent..]) => match written {
                Ok(count) if count > 0 => sent += count,
                // The receiver is gone: a discarding link drains on.
                _ if *state.borrow() == LinkState::Discard => return Forwarded::Dropped,
                _ => return Forwarded::Ended,
            },
            changed = state.changed() => {
                if changed.is_err() {
                    return Forwarded::Ended;
                }
            }
        }
    }
    Forwarded::Written
}

/// Copy `from` to `to` while the link is open. Blackholed, it stops reading,
/// so the sender's window fills as it would against a dead path, and it
/// holds what it has not yet written until the link moves again.
/// Discarding, it reads and drops everything, including the rest of a chunk
/// it was writing, and passes on no close: it holds until the link leaves
/// Discard and then ends, for the connection to be reset. It ends on EOF, an
/// error, or a reset, and hands both halves back.
async fn pump(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    mut state: watch::Receiver<LinkState>,
    discarded: Arc<AtomicBool>,
) -> (OwnedReadHalf, OwnedWriteHalf) {
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let Some(flow) = flowing(&mut state).await else {
            return (from, to);
        };
        if flow == Flow::Drop {
            discarded.store(true, Ordering::SeqCst);
        } else if discarded.load(Ordering::SeqCst) {
            // Out of Discard: the connection ends, to be reset.
            return (from, to);
        }
        let read = tokio::select! {
            read = from.read(&mut buffer) => read,
            // Re-check the state: a blackhole stops the read here.
            changed = state.changed() => {
                if changed.is_err() {
                    return (from, to);
                }
                continue;
            }
        };
        let count = match read {
            Ok(0) | Err(_) => {
                if discarded.load(Ordering::SeqCst) || *state.borrow() == LinkState::Discard {
                    // Pass on no close: hold until the link leaves Discard.
                    discarded.store(true, Ordering::SeqCst);
                    wait_state(&mut state, |s| s != LinkState::Discard).await;
                    return (from, to);
                }
                let _ = to.shutdown().await;
                return (from, to);
            }
            Ok(count) => count,
        };
        match forward(&mut to, &buffer[..count], &mut state).await {
            Forwarded::Written => {}
            Forwarded::Dropped => discarded.store(true, Ordering::SeqCst),
            Forwarded::Ended => return (from, to),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    async fn echo_server() -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buffer = [0u8; 1024];
                    while let Ok(count) = stream.read(&mut buffer).await {
                        if count == 0 || stream.write_all(&buffer[..count]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        Ok(port)
    }

    async fn round_trip(stream: &mut TcpStream, text: &[u8]) -> Result<Vec<u8>> {
        stream.write_all(text).await?;
        let mut buffer = vec![0u8; text.len()];
        stream.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    #[tokio::test]
    async fn an_open_link_forwards_and_a_reset_closes_old_and_new_connections() -> Result<()> {
        let relay = Relay::open("test", echo_server().await?).await?;
        let mut stream = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        assert_eq!(round_trip(&mut stream, b"ping").await?, b"ping");
        relay.set(LinkState::Reset);
        let mut buffer = [0u8; 4];
        let read = timeout(Duration::from_secs(5), stream.read(&mut buffer)).await?;
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "an existing connection sees the cut: {read:?}"
        );
        let mut fresh = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        let read = timeout(Duration::from_secs(5), fresh.read(&mut buffer)).await?;
        assert!(matches!(read, Ok(0) | Err(_)), "a new connection is reset");
        relay.set(LinkState::Open);
        let mut healed = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        assert_eq!(round_trip(&mut healed, b"pong").await?, b"pong");
        assert!(relay.stats().reset >= 2);
        Ok(())
    }

    #[tokio::test]
    async fn a_blackhole_holds_bytes_silently_and_healing_delivers_them() -> Result<()> {
        let relay = Relay::open("test", echo_server().await?).await?;
        let mut stream = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        assert_eq!(round_trip(&mut stream, b"one").await?, b"one");
        relay.set(LinkState::Blackholed);
        stream.write_all(b"two").await?;
        let mut buffer = [0u8; 3];
        assert!(
            timeout(Duration::from_millis(500), stream.read_exact(&mut buffer))
                .await
                .is_err(),
            "nothing crosses a blackhole, and nothing is closed"
        );
        // A connection made during the blackhole is accepted and held.
        let mut held = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        held.write_all(b"three").await?;
        relay.set(LinkState::Open);
        timeout(Duration::from_secs(5), stream.read_exact(&mut buffer)).await??;
        assert_eq!(&buffer, b"two");
        let mut five = [0u8; 5];
        timeout(Duration::from_secs(5), held.read_exact(&mut five)).await??;
        assert_eq!(&five, b"three");
        Ok(())
    }

    #[tokio::test]
    async fn a_discarding_link_drains_both_ways_holds_closes_and_resets_on_heal() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay = Relay::open("test", listener.local_addr()?.port()).await?;
        let mut client = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        let (mut server, _) = timeout(Duration::from_secs(5), listener.accept()).await??;
        relay.set(LinkState::Discard);
        client.write_all(b"lost").await?;
        drop(client);
        // A reply far larger than any window drains instead of blocking, as
        // a blackhole would make it.
        let reply = vec![7u8; 4 * 1024 * 1024];
        timeout(Duration::from_secs(10), server.write_all(&reply)).await??;
        let mut buffer = [0u8; 4];
        assert!(
            timeout(Duration::from_millis(500), server.read(&mut buffer))
                .await
                .is_err(),
            "neither the client's bytes nor its close reach the server"
        );
        // Leaving Discard resets what lived through it: its stream has a hole.
        relay.set(LinkState::Open);
        let read = timeout(Duration::from_secs(5), server.read(&mut buffer)).await?;
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "a connection that lived through Discard is reset"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_write_stuck_on_a_stalled_client_is_dropped_when_the_link_discards() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay = Relay::open("test", listener.local_addr()?.port()).await?;
        // The client never reads, so with the link open the relay's write to
        // it blocks once the windows fill.
        let _client = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        let (server, _) = timeout(Duration::from_secs(5), listener.accept()).await??;
        let (_server_read, mut server_write) = server.into_split();
        // Far more than the four socket buffers on the way can hold, even
        // fully autotuned (about 20 MiB on loopback).
        let flood =
            tokio::spawn(async move { server_write.write_all(&vec![1u8; 64 * 1024 * 1024]).await });
        tokio::time::sleep(Duration::from_millis(500)).await;
        if flood.is_finished() {
            // Socket buffers this large can hold the whole flood: the host
            // cannot show a stuck write, so there is nothing to unstick.
            return Ok(());
        }
        relay.set(LinkState::Discard);
        timeout(Duration::from_secs(10), flood).await???;
        Ok(())
    }
}

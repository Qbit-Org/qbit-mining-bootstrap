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
//!   leaves Discard, for any state, every established connection that moved
//!   or awaited bytes while it discarded is reset on both sides, as the dead
//!   host's restarted kernel would. A connection accepted while discarding is
//!   held as when blackholed and connects only once the link opens, and a
//!   Discard too brief for a connection to notice leaves it alone.
//!
//! Adapted from the `Relay` of
//! `crates/qbit-prism-server/tests/support/live_pg_failover.rs`, which
//! fences and re-routes a writer endpoint.

use anyhow::{Context, Result};
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
        Arc,
    },
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
    /// The highest one-way latency the link carried during the run, in
    /// milliseconds (a scenario lifts it before the report is taken).
    pub latency_ms_max: u64,
    pub accepted: u64,
    pub reset: u64,
    pub upstream_failures: u64,
}

pub struct Relay {
    name: String,
    port: u16,
    target: Arc<AtomicU16>,
    state: watch::Sender<LinkState>,
    /// One-way latency, in milliseconds, added to each burst forwarded.
    latency_ms: Arc<AtomicU64>,
    latency_ms_max: AtomicU64,
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
        let latency_ms = Arc::new(AtomicU64::new(0));
        let counters = Arc::new(Counters::default());
        let task = tokio::spawn(accept_loop(
            listener,
            target.clone(),
            state.subscribe(),
            latency_ms.clone(),
            counters.clone(),
        ));
        Ok(Self {
            name: name.to_owned(),
            port,
            target,
            state,
            latency_ms,
            latency_ms_max: AtomicU64::new(0),
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

    /// Delay every byte either side sends by `latency` before it is
    /// forwarded, on connections old and new: a path between two sites. The
    /// pumps are delay lines, so a round trip costs twice the latency and a
    /// stream keeps its rate (up to 1 MiB in flight each way). Nothing
    /// being discarded waits.
    pub fn set_latency(&self, latency: std::time::Duration) {
        let ms = latency.as_millis() as u64;
        self.latency_ms.store(ms, Ordering::SeqCst);
        self.latency_ms_max.fetch_max(ms, Ordering::SeqCst);
    }

    /// Point new connections at another port (a restarted child's, say).
    pub fn retarget(&self, target: u16) {
        self.target.store(target, Ordering::SeqCst);
    }

    pub fn stats(&self) -> RelayStats {
        RelayStats {
            name: self.name.clone(),
            state: self.state(),
            latency_ms_max: self.latency_ms_max.load(Ordering::Relaxed),
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
    latency_ms: Arc<AtomicU64>,
    counters: Arc<Counters>,
) {
    while let Ok((client, _)) = listener.accept().await {
        counters.accepted.fetch_add(1, Ordering::Relaxed);
        let _ = client.set_nodelay(true);
        let state = state.clone();
        let target = target.clone();
        let latency_ms = latency_ms.clone();
        let counters = counters.clone();
        tokio::spawn(async move {
            connection(client, target, state, latency_ms, counters).await;
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
    latency_ms: Arc<AtomicU64>,
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
        latency_ms.clone(),
        discarded.clone(),
    ));
    let inbound = tokio::spawn(pump(
        upstream_read,
        client_write,
        state.clone(),
        latency_ms.clone(),
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

/// How much a pump may hold in its delay line before it stops reading: a
/// receiver slower than its sender pushes back, as it would over TCP.
const DELAY_LINE_BYTES: usize = 1 << 20;

/// Copy `from` to `to` as a delay line. Each chunk is stamped when it is
/// read and written once the link's latency has passed since then, so every
/// byte is delayed by the same latency and a stream keeps its rate.
///
/// - **Open:** reads, and writes what is due.
/// - **Blackholed:** neither reads nor writes. What it holds waits for the
///   link, as unacknowledged segments wait for a path, and keeps its stamp.
/// - **Discard:** reads and drops everything, what it held included, and
///   passes on no close. It holds until the link leaves Discard, then ends,
///   for the connection to be reset.
/// - **Reset:** ends at once.
///
/// It ends on EOF (once what it holds is written), an error, or a reset, and
/// hands both halves back. Writes are partial and cancel-safe, so a state
/// change interrupts one stuck on a receiver that stopped reading.
async fn pump(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    mut state: watch::Receiver<LinkState>,
    latency_ms: Arc<AtomicU64>,
    discarded: Arc<AtomicBool>,
) -> (OwnedReadHalf, OwnedWriteHalf) {
    let mut buffer = vec![0u8; 16 * 1024];
    let mut line: VecDeque<(tokio::time::Instant, Vec<u8>)> = VecDeque::new();
    // Bytes the line holds, and how much of its head is already written.
    let (mut held, mut written) = (0usize, 0usize);
    let mut eof = false;
    loop {
        let link = *state.borrow_and_update();
        match link {
            LinkState::Reset => return (from, to),
            LinkState::Discard => {
                discarded.store(true, Ordering::SeqCst);
                line.clear();
                (held, written) = (0, 0);
                if eof {
                    // Pass on no close: hold until the link leaves Discard.
                    wait_state(&mut state, |s| s != LinkState::Discard).await;
                    return (from, to);
                }
            }
            // Out of Discard: the connection ends, to be reset.
            _ if discarded.load(Ordering::SeqCst) => return (from, to),
            LinkState::Open | LinkState::Blackholed => {}
        }
        if eof && line.is_empty() {
            let _ = to.shutdown().await;
            return (from, to);
        }
        let reading = !eof && link != LinkState::Blackholed && held < DELAY_LINE_BYTES;
        let latency = std::time::Duration::from_millis(latency_ms.load(Ordering::SeqCst));
        let head = match link {
            LinkState::Open => line
                .front()
                .map(|(stamp, chunk)| (*stamp + latency, &chunk[written..])),
            _ => None,
        };
        let writing = head.is_some();
        tokio::select! {
            read = from.read(&mut buffer), if reading => match read {
                Ok(0) | Err(_) => eof = true,
                Ok(count) => {
                    if link != LinkState::Discard {
                        line.push_back((tokio::time::Instant::now(), buffer[..count].to_vec()));
                        held += count;
                    }
                }
            },
            wrote = write_due(&mut to, head), if writing => match wrote {
                Ok(count) if count > 0 => {
                    written += count;
                    if line.front().is_some_and(|(_, chunk)| written == chunk.len()) {
                        if let Some((_, chunk)) = line.pop_front() {
                            held -= chunk.len();
                        }
                        written = 0;
                    }
                }
                // The receiver is gone while the link is open.
                _ => return (from, to),
            },
            changed = state.changed() => {
                if changed.is_err() {
                    return (from, to);
                }
            }
        }
    }
}

/// Wait until the head of a delay line is due, then write a piece of it.
/// Cancel-safe, as `write` is: cancelled, it wrote nothing.
async fn write_due(
    to: &mut OwnedWriteHalf,
    head: Option<(tokio::time::Instant, &[u8])>,
) -> std::io::Result<usize> {
    let Some((due, chunk)) = head else {
        return std::future::pending().await;
    };
    tokio::time::sleep_until(due).await;
    to.write(chunk).await
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
    async fn a_stream_through_a_slow_link_keeps_its_rate() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay = Relay::open("test", listener.local_addr()?.port()).await?;
        relay.set_latency(Duration::from_millis(150));
        let mut client = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        let (mut server, _) = timeout(Duration::from_secs(5), listener.accept()).await??;
        // 100 pieces of 8 KiB, 5 ms apart: half a second of streaming. A link
        // that paid its latency per piece would take 15 s.
        let sender = tokio::spawn(async move {
            for _ in 0..100 {
                server.write_all(&[3u8; 8 * 1024]).await?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            anyhow::Ok(server)
        });
        let started = std::time::Instant::now();
        let mut received = vec![0u8; 100 * 8 * 1024];
        timeout(Duration::from_secs(10), client.read_exact(&mut received)).await??;
        let took = started.elapsed();
        let _server = sender.await??;
        assert!(
            took >= Duration::from_millis(150) && took < Duration::from_secs(3),
            "one latency, then the stream's own pace: {took:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn latency_delays_each_way_and_can_be_lifted() -> Result<()> {
        let relay = Relay::open("test", echo_server().await?).await?;
        let mut stream = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        relay.set_latency(Duration::from_millis(150));
        let started = std::time::Instant::now();
        assert_eq!(round_trip(&mut stream, b"slow").await?, b"slow");
        assert!(
            started.elapsed() >= Duration::from_millis(300),
            "both ways wait: {:?}",
            started.elapsed()
        );
        // A burst pays the latency once, not once a chunk: 4 MiB echoed in
        // 16 KiB chunks would take minutes if every chunk waited.
        let bulk = vec![9u8; 4 * 1024 * 1024];
        let started = std::time::Instant::now();
        let (mut read, mut write) = stream.split();
        let (sent, echoed) = tokio::join!(write.write_all(&bulk), async {
            let mut back = vec![0u8; bulk.len()];
            read.read_exact(&mut back).await.map(|_| back)
        });
        sent?;
        assert_eq!(echoed?, bulk);
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "bulk transfer is not capped: {:?}",
            started.elapsed()
        );
        relay.set_latency(Duration::ZERO);
        let started = std::time::Instant::now();
        assert_eq!(round_trip(&mut stream, b"fast").await?, b"fast");
        assert!(started.elapsed() < Duration::from_millis(300));
        assert_eq!(relay.stats().latency_ms_max, 150);
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
        // The client does not read, so with the link open the relay's write
        // to it blocks once the windows fill.
        let mut client = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        let (server, _) = timeout(Duration::from_secs(5), listener.accept()).await??;
        let (_server_read, mut server_write) = server.into_split();
        // Far more than the four socket buffers on the way can hold, even
        // fully autotuned (about 20 MiB on loopback).
        let flood =
            tokio::spawn(async move { server_write.write_all(&vec![1u8; 64 * 1024 * 1024]).await });
        tokio::time::sleep(Duration::from_millis(500)).await;
        if flood.is_finished() {
            // Either the host's socket buffers hold the whole flood, and there
            // is no stuck write to unstick, or the open link lost bytes. Only
            // the first may pass: an open link delivers every byte.
            let (mut received, mut buffer) = (0usize, vec![0u8; 1 << 20]);
            while let Ok(Ok(count @ 1..)) =
                timeout(Duration::from_secs(2), client.read(&mut buffer)).await
            {
                received += count;
            }
            assert_eq!(
                received,
                64 * 1024 * 1024,
                "an open link delivers every byte"
            );
            return Ok(());
        }
        relay.set(LinkState::Discard);
        timeout(Duration::from_secs(10), flood).await???;
        Ok(())
    }
}

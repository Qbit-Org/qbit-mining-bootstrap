//! A TCP relay the fault injector can cut: every link of the simulated
//! network that a fault may break goes through one (the peer-sync DSNs, the
//! balancer's routes and health checks, and in the 3.0 topology the writer
//! endpoint and the standby's replication link).
//!
//! A link is in one of four states:
//!
//! - **Open:** bytes flow, and so does each side's close, an orderly one as
//!   a FIN and an abortive one (or a socket error) as a reset, which ends
//!   the connection both ways. So does a write a side refuses (it reset, or
//!   is gone), at once: whichever direction meets a reset first, the other
//!   side is reset too.
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
    /// One-way latency, in milliseconds, added to every byte forwarded; a
    /// change wakes every pump, which re-dates what it holds.
    latency_ms: watch::Sender<u64>,
    latency_ms_max: AtomicU64,
    counters: Arc<Counters>,
    task: JoinHandle<()>,
}

impl Relay {
    /// A relay on a fresh loopback port, open, in front of `target`.
    pub async fn open(name: &str, target: u16) -> Result<Self> {
        let listener = TcpListener::from_std(crate::postgres::fresh_listener()?)
            .with_context(|| format!("binding relay {name}"))?;
        let port = listener.local_addr()?.port();
        let target = Arc::new(AtomicU16::new(target));
        let (state, _) = watch::channel(LinkState::Open);
        let (latency_ms, _) = watch::channel(0u64);
        let counters = Arc::new(Counters::default());
        let task = tokio::spawn(accept_loop(
            listener,
            target.clone(),
            state.subscribe(),
            latency_ms.subscribe(),
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
        self.latency_ms
            .send_if_modified(|current| std::mem::replace(current, ms) != ms);
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
    latency_ms: watch::Receiver<u64>,
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
    latency_ms: watch::Receiver<u64>,
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
    // Sent once either side's reset has crossed: both directions end.
    let aborted = Arc::new(watch::channel(false).0);
    let outbound = tokio::spawn(pump(
        client_read,
        upstream_write,
        state.clone(),
        latency_ms.clone(),
        discarded.clone(),
        aborted.clone(),
    ));
    let inbound = tokio::spawn(pump(
        upstream_read,
        client_write,
        state.clone(),
        latency_ms.clone(),
        discarded.clone(),
        aborted.clone(),
    ));
    // Each pump hands its halves back when it ends, so a reset can abort
    // both sides instead of letting a half-closed connection linger.
    let (outbound, inbound) = (outbound.await, inbound.await);
    // A reset ends every connection with a reset. So does leaving Discard for
    // every connection that lived through it, whether or not it lost bytes:
    // the host Discard modelled as dead does not know the connection, and its
    // restarted kernel answers the next segment with a reset (and any bytes
    // dropped left a hole a resumed stream could not survive). And so does a
    // side's own reset, once it has crossed.
    let reset_seen = matches!(*state.borrow(), LinkState::Reset)
        || discarded.load(Ordering::SeqCst)
        || *aborted.borrow();
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

/// How much a pump may hold in its delay line before it stops reading, with
/// latency on the link: a receiver slower than its sender pushes back, as it
/// would over TCP. Without latency it holds one read, as a plain relay
/// would, so a stopped receiver pushes back at once.
const DELAY_LINE_BYTES: usize = 1 << 20;
/// How many entries the line may hold, whatever their size. Reads less than
/// [`COALESCE`] apart share an entry, so this binds only on a receiver that
/// stopped reading, or a latency of seconds.
const DELAY_LINE_ENTRIES: usize = 4096;
const COALESCE: std::time::Duration = std::time::Duration::from_millis(1);
const CHUNK: usize = 16 * 1024;

/// What a delay line holds, in the order the sender sent it.
enum Sent {
    Bytes(Vec<u8>),
    /// An orderly close (a FIN).
    Close,
    /// An abortive one: a reset, or a socket error that ends the connection
    /// as one.
    Reset,
}

/// Copy `from` to `to` as a delay line. Each chunk, and the sender's close,
/// is stamped when it is read and passed on once the link's latency has
/// passed since then, so every byte and the close wait the same latency and
/// a stream keeps its rate. A close passes on as the sender closed: a FIN,
/// or a reset that ends both directions (`aborted`). A write `to` refuses
/// ends both directions at once: its reset may have been used up by that
/// write, so the pump reading that socket would see only an end of file, or,
/// with its line full, nothing at all.
///
/// - **Open:** reads, and passes on what is due.
/// - **Blackholed:** neither reads nor passes anything on, a close included.
///   What it holds waits for the link, as unacknowledged segments wait for a
///   path, and keeps its stamp.
/// - **Discard:** reads and drops everything, what it held included, and
///   passes on no close. It holds until the link leaves Discard, then ends,
///   for the connection to be reset.
/// - **Reset:** ends at once.
///
/// A change of state, or of latency, is acted on before anything else is
/// read or written; a new latency re-dates what the line holds, except a
/// chunk already partly written. Writes are partial and cancel-safe, so a
/// change also interrupts one stuck on a receiver that stopped reading. It
/// hands both halves back when it ends.
async fn pump(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    mut state: watch::Receiver<LinkState>,
    mut latency_ms: watch::Receiver<u64>,
    discarded: Arc<AtomicBool>,
    aborted: Arc<watch::Sender<bool>>,
) -> (OwnedReadHalf, OwnedWriteHalf) {
    let mut ended = aborted.subscribe();
    let mut buffer = vec![0u8; CHUNK];
    // Each entry: when it was read, and what.
    let mut line: VecDeque<(tokio::time::Instant, Sent)> = VecDeque::new();
    // Bytes the line holds, and how much of its head is already written.
    let (mut held, mut written) = (0usize, 0usize);
    let mut eof = false;
    loop {
        let link = *state.borrow_and_update();
        let latency = std::time::Duration::from_millis(*latency_ms.borrow_and_update());
        // The other direction passed on a reset: the connection is gone.
        if *ended.borrow_and_update() {
            return (from, to);
        }
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
        let room = if latency.is_zero() {
            line.is_empty()
        } else {
            held < DELAY_LINE_BYTES && line.len() < DELAY_LINE_ENTRIES
        };
        let reading = !eof && link != LinkState::Blackholed && room;
        let head = match link {
            LinkState::Open => line.front().map(|(stamp, sent)| {
                // A chunk partly written is due: its first bytes are out.
                let due = if written > 0 {
                    *stamp
                } else {
                    *stamp + latency
                };
                let bytes = match sent {
                    Sent::Bytes(bytes) => Some(&bytes[written..]),
                    Sent::Close | Sent::Reset => None,
                };
                (due, bytes)
            }),
            _ => None,
        };
        let writing = head.is_some();
        tokio::select! {
            // A change of state or latency first: nothing moves on a stale one.
            biased;
            changed = state.changed() => {
                if changed.is_err() {
                    return (from, to);
                }
            }
            changed = latency_ms.changed() => {
                if changed.is_err() {
                    return (from, to);
                }
            }
            // The other direction's reset: acted on at the top.
            _ = ended.changed() => {}
            wrote = write_due(&mut to, head), if writing => match wrote {
                // The sender's close, now due: pass it on as it came, and end.
                Ok(None) => {
                    if matches!(line.front(), Some((_, Sent::Reset))) {
                        aborted.send_replace(true);
                    } else {
                        let _ = to.shutdown().await;
                    }
                    return (from, to);
                }
                Ok(Some(count)) if count > 0 => {
                    written += count;
                    let done = matches!(
                        line.front(),
                        Some((_, Sent::Bytes(bytes))) if written == bytes.len()
                    );
                    if done {
                        if let Some((_, Sent::Bytes(bytes))) = line.pop_front() {
                            held -= bytes.len();
                        }
                        written = 0;
                    }
                }
                // The receiver is gone. A link that has just turned to Discard
                // drains on, as a dead client's server must, and the
                // connection is reset when the link leaves Discard, however
                // soon; otherwise the connection ends with a reset.
                _ if *state.borrow() == LinkState::Discard => {
                    discarded.store(true, Ordering::SeqCst);
                }
                _ => {
                    aborted.send_replace(true);
                    return (from, to);
                }
            },
            read = from.read(&mut buffer), if reading => {
                let now = tokio::time::Instant::now();
                match read {
                    Ok(count @ 1..) if link != LinkState::Discard => {
                        held += count;
                        let bytes = &buffer[..count];
                        match line.back_mut() {
                            // Reads close together share an entry, so a stream
                            // of small ones does not fill the line by count.
                            Some((stamp, Sent::Bytes(tail)))
                                if now.saturating_duration_since(*stamp) < COALESCE =>
                            {
                                tail.extend_from_slice(bytes);
                            }
                            _ => line.push_back((now, Sent::Bytes(bytes.to_vec()))),
                        }
                    }
                    Ok(1..) => {}
                    end => {
                        eof = true;
                        if link != LinkState::Discard {
                            let close = if end.is_ok() { Sent::Close } else { Sent::Reset };
                            line.push_back((now, close));
                        }
                    }
                }
            }
        }
    }
}

/// Wait until the head of a delay line is due, then write a piece of it:
/// `Some(count)` written, or `None` for the sender's close, which the caller
/// passes on. Cancel-safe, as `write` is: cancelled, it wrote nothing.
async fn write_due(
    to: &mut OwnedWriteHalf,
    head: Option<(tokio::time::Instant, Option<&[u8]>)>,
) -> std::io::Result<Option<usize>> {
    let Some((due, chunk)) = head else {
        return std::future::pending().await;
    };
    tokio::time::sleep_until(due).await;
    match chunk {
        Some(bytes) => to.write(bytes).await.map(Some),
        None => Ok(None),
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
        let mut first = [0u8; 1];
        timeout(Duration::from_secs(10), client.read_exact(&mut first)).await??;
        let first_at = started.elapsed();
        let mut received = vec![0u8; 100 * 8 * 1024 - 1];
        timeout(Duration::from_secs(10), client.read_exact(&mut received)).await??;
        let took = started.elapsed();
        let _server = sender.await??;
        assert!(
            first_at >= Duration::from_millis(145),
            "the first byte waits the latency: {first_at:?}"
        );
        assert!(
            took < Duration::from_secs(3),
            "then the stream keeps its own pace: {took:?}"
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

    /// A client through `relay`, and the server end of its connection.
    async fn connected(relay: &Relay, listener: &TcpListener) -> Result<(TcpStream, TcpStream)> {
        let client = TcpStream::connect(("127.0.0.1", relay.port())).await?;
        let (server, _) = timeout(Duration::from_secs(5), listener.accept()).await??;
        Ok((client, server))
    }

    #[tokio::test]
    async fn a_close_waits_the_latency_and_a_reset_crosses_as_a_reset() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay = Relay::open("test", listener.local_addr()?.port()).await?;
        relay.set_latency(Duration::from_millis(150));
        let mut buffer = [0u8; 4];

        let (mut client, mut server) = connected(&relay, &listener).await?;
        let started = std::time::Instant::now();
        client.shutdown().await?;
        let read = timeout(Duration::from_secs(5), server.read(&mut buffer)).await?;
        assert!(
            matches!(read, Ok(0)),
            "an orderly close arrives as one: {read:?}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(145),
            "the close waits the latency: {:?}",
            started.elapsed()
        );

        let (client, mut server) = connected(&relay, &listener).await?;
        let started = std::time::Instant::now();
        reset(client);
        let read = timeout(Duration::from_secs(5), server.read(&mut buffer)).await?;
        assert!(
            matches!(&read, Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset),
            "an abortive close arrives as a reset: {read:?}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(145),
            "the reset waits the latency: {:?}",
            started.elapsed()
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_reset_met_on_a_write_resets_the_other_side_even_with_the_other_way_stuck(
    ) -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay = Relay::open("test", listener.local_addr()?.port()).await?;
        relay.set_latency(Duration::from_millis(10));
        let (client, server) = connected(&relay, &listener).await?;
        // The server reads nothing, so the relay's line toward it fills and
        // it stops reading the client; the client reads nothing either, so
        // the relay's write toward it is pending when it resets.
        let (_server_read, mut server_write) = server.into_split();
        abort_on_close(&client);
        let (client_read, mut client_write) = client.into_split();
        let upload = tokio::spawn(async move {
            let _ = client_write.write_all(&vec![1u8; 64 << 20]).await;
        });
        let flood = tokio::spawn(async move {
            let chunk = vec![2u8; 64 << 10];
            while server_write.write_all(&chunk).await.is_ok() {}
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!flood.is_finished(), "both ways are stuck before the reset");
        upload.abort();
        let _ = upload.await;
        drop(client_read);
        // Only the write toward the client meets its reset; the server must
        // be reset too, or its writes block for good.
        timeout(Duration::from_secs(10), flood).await??;
        assert_eq!(relay.stats().reset, 1, "the connection ends in a reset");
        Ok(())
    }

    #[tokio::test]
    async fn a_close_held_in_the_line_never_crosses_a_blackhole() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay = Relay::open("test", listener.local_addr()?.port()).await?;
        relay.set_latency(Duration::from_millis(300));
        let (mut client, mut server) = connected(&relay, &listener).await?;
        client.write_all(b"last").await?;
        client.shutdown().await?;
        // The bytes and the close are in the line, not yet due, when the
        // link goes dark.
        tokio::time::sleep(Duration::from_millis(50)).await;
        relay.set(LinkState::Blackholed);
        let mut buffer = [0u8; 4];
        assert!(
            timeout(Duration::from_millis(600), server.read(&mut buffer))
                .await
                .is_err(),
            "neither bytes nor a close cross a blackhole"
        );
        relay.set(LinkState::Open);
        timeout(Duration::from_secs(5), server.read_exact(&mut buffer)).await??;
        assert_eq!(&buffer, b"last");
        let read = timeout(Duration::from_secs(5), server.read(&mut buffer)).await?;
        assert!(matches!(read, Ok(0)), "then the close: {read:?}");
        Ok(())
    }

    #[tokio::test]
    async fn a_new_latency_re_dates_what_the_line_holds() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay = Relay::open("test", listener.local_addr()?.port()).await?;
        relay.set_latency(Duration::from_secs(5));
        let (mut client, mut server) = connected(&relay, &listener).await?;
        let started = std::time::Instant::now();
        client.write_all(b"held").await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        relay.set_latency(Duration::ZERO);
        let mut buffer = [0u8; 4];
        timeout(Duration::from_secs(10), server.read_exact(&mut buffer)).await??;
        assert_eq!(&buffer, b"held");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "lifting the latency releases what waited: {:?}",
            started.elapsed()
        );
        assert_eq!(relay.stats().latency_ms_max, 5000);
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

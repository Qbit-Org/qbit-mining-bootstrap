//! Stratum client sessions.
//!
//! One TCP connection per session. The header recipe reproduces
//! `crates/qbit-prism-server/src/codec.rs` `Job::assemble_submission` exactly,
//! using the server's own `codec` helpers where they are public, so a share
//! identifier computed here is the identifier the ledger stores.

use crate::classify::Rejection;
use anyhow::{bail, ensure, Context, Result};
use num_bigint::BigUint;
use qbit_prism_server::codec;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::mpsc,
};

/// How many past jobs a session keeps, so a late response can still be
/// attributed to the job it was built on (EP-STATE).
const JOB_HISTORY: usize = 8;
/// Nonces tried under one extranonce2 before the client rolls extranonce2.
const NONCE_SPAN: u32 = 1 << 22;

/// Reverse each 4-byte word in place. `mining.notify` sends the previous block
/// hash with its words swapped (`codec.rs`, `Job::from_manifest`); the
/// transform is its own inverse.
pub fn word_swap(bytes: &mut [u8]) {
    // `as_chunks_mut` over a constant width: a trailing partial word is left
    // alone, which is what the server's own transform does for a 32-byte hash.
    for word in bytes.as_chunks_mut::<4>().0 {
        word.reverse();
    }
}

/// Header bytes 4..36 from the `prevhash` field of `mining.notify`.
pub fn header_prev_from_wire(prevhash_hex: &str) -> Result<Vec<u8>> {
    let mut bytes = hex::decode(prevhash_hex).context("notify prevhash is not hex")?;
    ensure!(bytes.len() == 32, "notify prevhash must be 32 bytes");
    word_swap(&mut bytes);
    Ok(bytes)
}

/// The tip hash in display order, as `getbestblockhash` returns it.
pub fn tip_from_wire_prevhash(prevhash_hex: &str) -> Result<String> {
    let mut bytes = header_prev_from_wire(prevhash_hex)?;
    bytes.reverse();
    Ok(hex::encode(bytes))
}

/// `sha256d(coinb1 ‖ extranonce1 ‖ extranonce2 ‖ coinb2)` folded through the
/// branch, exactly as the server folds it.
pub fn merkle_root(
    coinb1: &[u8],
    extranonce1: &[u8],
    extranonce2: &[u8],
    coinb2: &[u8],
    branch: &[[u8; 32]],
) -> [u8; 32] {
    let preimage = [coinb1, extranonce1, extranonce2, coinb2].concat();
    let mut merkle = codec::double_sha256(&preimage);
    for sibling in branch {
        merkle = codec::double_sha256(&[merkle.as_slice(), sibling.as_slice()].concat());
    }
    merkle
}

/// The 80-byte block header.
pub fn assemble_header(
    version: u32,
    header_prev: &[u8],
    merkle: &[u8; 32],
    ntime: u32,
    nbits: u32,
    nonce: u32,
) -> Vec<u8> {
    [
        version.to_le_bytes().as_slice(),
        header_prev,
        merkle.as_slice(),
        ntime.to_le_bytes().as_slice(),
        nbits.to_le_bytes().as_slice(),
        nonce.to_le_bytes().as_slice(),
    ]
    .concat()
}

/// `share_id` as `coordinator.rs` builds it: the authorized username, a colon,
/// and the display-order double SHA-256 of the header.
pub fn share_id(username: &str, header: &[u8]) -> String {
    format!(
        "{username}:{}",
        codec::hash_display(&codec::double_sha256(header))
    )
}

/// A 256-bit target as little-endian bytes, so proof-of-work comparison needs
/// no bignum allocation per hash.
pub fn target_bytes_le(target: &BigUint) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    for (slot, byte) in bytes.iter_mut().zip(target.to_bytes_le()) {
        *slot = byte;
    }
    bytes
}

/// `a <= b` for two little-endian 256-bit integers.
pub fn le_at_most(a: &[u8; 32], b: &[u8; 32]) -> bool {
    for index in (0..32).rev() {
        match a[index].cmp(&b[index]) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => {}
        }
    }
    true
}

/// One job as the client holds it.
#[derive(Clone, Debug)]
pub struct JobState {
    pub job_id: String,
    pub tip: String,
    pub header_prev: Vec<u8>,
    pub coinb1: Vec<u8>,
    pub coinb2: Vec<u8>,
    pub branch: Vec<[u8; 32]>,
    pub version: u32,
    pub nbits: u32,
    pub ntime: u32,
    pub clean_jobs: bool,
    pub received: Instant,
    pub share_target: [u8; 32],
    pub network_target: [u8; 32],
}

/// What the server answered, or did not.
/// The `NoResponse` reason recorded when the run itself stopped waiting,
/// rather than the peer closing the socket mid-run.
///
/// The distinction decides an exit code. A submit still outstanding when the
/// drain expires is one whose measurement window ended underneath it: the
/// server is allowed to answer a moment later, and in production the
/// connection would still be there to carry the answer. A socket the peer
/// closed mid-run is the failure this harness exists to catch.
pub const RUN_ENDED: &str = "run ended";

#[derive(Clone, Debug)]
pub enum Outcome {
    Accepted,
    Rejected(Rejection),
    /// The socket closed or the run ended before an answer arrived. Never
    /// folded into either of the other two (EP-ERRORS).
    NoResponse {
        reason: String,
    },
}

impl Outcome {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected(_) => "rejected",
            Self::NoResponse { .. } => "no-response",
        }
    }
}

/// One offered share.
#[derive(Clone, Debug)]
pub struct SubmitRecord {
    pub share_id: String,
    pub session: usize,
    pub frontend: usize,
    pub phase: String,
    pub job_id: String,
    pub sent: Instant,
    pub responded: Option<Instant>,
    pub latency_millis: Option<f64>,
    pub outcome: Outcome,
    pub scheduled_block: bool,
    /// A deliberate re-offer of an indeterminate share, with the same header.
    pub reoffer: bool,
    /// The value of the run's kill fence (`SessionShared::kill_fence`) when
    /// the outcome this record reports was observed -- the answer arriving,
    /// the socket ending, the deadline expiring -- read where and when that
    /// happened. It is the record's identity with respect to the mid-flight
    /// kill: a record whose fence is below the value the kill stamped
    /// reports something that happened before the kill, however long it then
    /// took to be turned into a record and reach the collector, and neither
    /// that lag nor delivery lag can move it into the kill's census
    /// (EP-STATE).
    pub fence: u64,
    pub header_hex: String,
    pub extranonce2_hex: String,
    pub ntime_hex: String,
    pub nonce_hex: String,
}

/// A completed or attempted reconnect.
#[derive(Clone, Debug)]
pub struct ReconnectRecord {
    pub session: usize,
    pub frontend: usize,
    /// The phase that asked for the reconnect, not the phase it completed
    /// in.
    pub phase: String,
    pub reason: String,
    pub completed: bool,
    pub error: Option<String>,
    /// Measured from the moment the previous connection went -- the socket
    /// closing, or the deliberate close after a reconnect was asked for and
    /// the session had quiesced -- to the end of this attempt. For a
    /// completed reconnect that is the whole outage, every failed attempt
    /// and backoff included; for a failed attempt it is how long the
    /// session had been without a connection when the attempt failed. It
    /// used to restart on every attempt, so a frontend unavailable across
    /// several of them reported its final handshake as the time to
    /// reconnect (EP-OBSERVABILITY).
    pub seconds: f64,
}

/// The first time one session saw work built on a tip.
#[derive(Clone, Debug)]
pub struct TipSighting {
    pub session: usize,
    pub frontend: usize,
    pub tip: String,
    pub at: Instant,
}

/// One `mining.notify` a session received.
///
/// Recorded only while the run asks for it, because in the dense-cadence
/// phase the arrival of a job *is* the measurement, and in every other phase
/// the record would be a few thousand rows nothing reads.
#[derive(Clone, Debug)]
pub struct NotifySighting {
    pub session: usize,
    pub frontend: usize,
    pub job_id: String,
    pub tip: String,
    pub clean_jobs: bool,
    pub at: Instant,
}

/// Everything a session reports back.
#[derive(Debug)]
pub enum Event {
    /// Acknowledge only after the collector has applied this session's prior events.
    CensusBarrier(mpsc::UnboundedSender<()>),
    Submit(Box<SubmitRecord>),
    Reconnect(ReconnectRecord),
    /// The first time this session saw work built on a tip.
    Tip(TipSighting),
    /// One `mining.notify`, while `SessionShared::record_notifies` is set.
    Notify(NotifySighting),
    /// A share-passing nonce that also solved a block and was therefore never
    /// submitted.
    DiscardedBlockSolution {
        session: usize,
    },
    /// An offer the scheduler placed that this session never sent, because it
    /// was paused or stopped first. Counted, with the phase whose scheduler
    /// placed it, so `dispatched` and the submits recorded can be reconciled
    /// against each other.
    DiscardedOffer {
        session: usize,
        phase: String,
    },
    /// A `set_difficulty` whose value disagrees with the difficulty the
    /// harness configured.
    DifficultyMismatch {
        session: usize,
        advertised: f64,
        configured: f64,
    },
    Connected {
        session: usize,
        frontend: usize,
    },
    Disconnected {
        session: usize,
        frontend: usize,
        reason: String,
    },
    Failure(ClientFailure),
}

/// What a session was doing when it failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailureKind {
    /// An offer the scheduler placed and counted as dispatched that failed:
    /// no job to mine, no solution found, or the submit line could not be
    /// written.
    Offer,
    /// A scheduled block that could not be built or sent.
    ScheduledBlock,
    /// A mid-flight re-offer that could not be sent.
    Reoffer,
    /// A line from the server the session could not handle.
    Line,
}

/// One client failure, as the collector keeps it.
///
/// The failures were collected and never read, so `dispatched` and the
/// submits a phase recorded could disagree with no account of why. Each
/// carries the phase it happened in and what the session was doing, so the
/// phase's offer accounting can say (EP-OBSERVABILITY).
#[derive(Clone, Debug)]
pub struct ClientFailure {
    pub session: usize,
    /// The phase whose scheduler placed the offer, for an offer; the phase
    /// the run was in, for the rest.
    pub phase: String,
    pub kind: FailureKind,
    /// Whether a submit record already carries the failed submit. A write
    /// failure is recorded as a no-response submit before it is reported
    /// here, so the offer accounting must not count it twice.
    pub recorded: bool,
    pub error: String,
    /// When the failure happened, so a scheduled-block failure can be
    /// attributed to the landing that asked for it (EP-ERRORS).
    pub at: Instant,
}

/// Why an offer produced nothing to acknowledge, and whether a submit record
/// already carries it.
pub struct OfferFailure {
    pub error: anyhow::Error,
    pub recorded: bool,
}

impl OfferFailure {
    fn unrecorded(error: anyhow::Error) -> Self {
        Self {
            error,
            recorded: false,
        }
    }
}

/// Work and control messages a session accepts.
#[derive(Clone, Debug)]
pub enum Work {
    /// One share to find and submit, stamped with the phase whose scheduler
    /// offered it. The stamp travels with the item because a session may
    /// send it after the phase boundary -- while reconnecting, or paused --
    /// and the phase that counted it as dispatched is the phase it belongs
    /// to (EP-STATE).
    Submit { phase: Arc<str> },
}

#[derive(Clone, Debug)]
pub enum Control {
    /// Forward a barrier through the event queue after prior session work.
    CensusBarrier(mpsc::UnboundedSender<()>),
    /// Quiesce outstanding submits, close, reconnect and re-authorize. The
    /// record it produces is stamped with `phase`, the phase that asked for
    /// it: a reconnect started near the end of the `reconnect` phase and
    /// completed after the next one began is still that phase's reconnect,
    /// the same way a submit belongs to the phase that offered it (EP-STATE).
    Reconnect {
        reason: String,
        phase: Arc<str>,
    },
    /// Stop submitting but stay connected until retargeted. The sender sets
    /// `SessionHandle::paused` first, so the scheduler stops offering at once.
    Pause,
    /// Point at a frontend and resume: the only message that lifts a pause.
    Retarget {
        frontend: usize,
        address: String,
        reconnect: bool,
    },
    /// Find a network-target solution on the current job and submit it.
    ScheduledBlock,
    /// Re-offer an indeterminate share with exactly the header it carried.
    Reoffer {
        share_id: String,
        job_id: String,
        extranonce2_hex: String,
        ntime_hex: String,
        nonce_hex: String,
        header_hex: String,
    },
    Stop,
}

/// Immutable per-session configuration.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub index: usize,
    pub username: String,
    pub password: String,
    pub share_difficulty: f64,
    pub version_rolling_mask: u32,
    pub connect_timeout: Duration,
    pub handshake_timeout: Duration,
    /// How long a deliberate close -- a client-initiated reconnect, a
    /// retarget that asks for a fresh connection -- waits for the session's
    /// outstanding submits to settle before the socket goes. The run derives
    /// it from the configured share-commit timeout plus the drain margin
    /// (`run::drain_limit`), the one deadline the phase boundaries and the
    /// drained restart already wait: a submit the server is still allowed to
    /// be working on is never turned into a `NoResponse` by the harness's
    /// own close (EP-ERRORS).
    pub quiesce_limit: Duration,
}

/// Shared, live run state a session reads.
pub struct SessionShared {
    pub phase: std::sync::RwLock<String>,
    pub events: mpsc::UnboundedSender<Event>,
    /// Set for the phases that measure job arrival. Off everywhere else, so
    /// no existing run pays for a record it does not report.
    pub record_notifies: AtomicBool,
    /// The run's mid-flight kill fence: bumped once by
    /// [`crate::kill::KillDriver`] immediately before it SIGKILLs a frontend,
    /// and read by every session as it builds a [`SubmitRecord`]. One counter
    /// shared by the run, every session and the driver, so "before the kill"
    /// and "after the kill" are the same fact for all of them.
    pub kill_fence: Arc<AtomicU64>,
}

impl SessionShared {
    pub fn phase(&self) -> String {
        self.phase.read().expect("phase lock").clone()
    }

    /// The fence to stamp on a record being built now. `SeqCst` on both this
    /// load and the driver's bump, so the two orderings agree: a record built
    /// before the bump cannot read the bumped value, and one built after it
    /// cannot read the earlier one.
    pub fn fence(&self) -> u64 {
        self.kill_fence.load(Ordering::SeqCst)
    }

    pub fn recording_notifies(&self) -> bool {
        self.record_notifies.load(Ordering::Relaxed)
    }
}

/// Handle the run keeps for each session.
pub struct SessionHandle {
    pub index: usize,
    pub frontend: Arc<AtomicUsize>,
    pub outstanding: Arc<AtomicUsize>,
    /// Whether the session is paused: told to stop submitting and stay
    /// connected until it is retargeted. The session task reads it to decide
    /// whether it may send, and the scheduler reads it to decide whether to
    /// offer, so both decisions see the one state (EP-STATE). Whoever sends
    /// `Control::Pause` sets it before sending, so the scheduler stops
    /// offering the instant the pause is decided rather than when the task
    /// gets round to the message; the task sets it again on receipt, which is
    /// harmless, and clears it on `Control::Retarget`.
    pub paused: Arc<AtomicBool>,
    pub work: mpsc::Sender<Work>,
    pub control: mpsc::UnboundedSender<Control>,
    pub task: tokio::task::JoinHandle<()>,
}

impl SessionHandle {
    /// Offer one share to this session if it is under its outstanding limit
    /// and not paused. `phase` is the phase making the offer; the record the
    /// session eventually reports carries it whatever the phase is by then.
    ///
    /// A paused session is ineligible, not merely slow: an offer it accepted
    /// would sit in its queue, counted as outstanding, until it is retargeted.
    /// That takes the token away from the frontends that are up, holds the
    /// drain that waits for `outstanding` to reach zero open until its
    /// deadline, and then sends the queue as one burst on resume; the
    /// reconnect phase would measure that burst instead of steady traffic
    /// while one frontend is away.
    ///
    /// This must stay synchronous, and the counter must stay private to the run
    /// task. The session task can receive and finish the work between the
    /// `try_send` and the `fetch_add` below, and its `fetch_sub` then takes the
    /// counter through `usize::MAX`. That is harmless only because the wrapping
    /// add and sub commute, so the settled value is exact, and because nothing
    /// can read the counter in between: the only reader is this function and the
    /// quiesce, restart and teardown checks, all on the one run task, and there
    /// is no await point here for a cancellation to land in.
    ///
    /// Making this `async`, or reading `outstanding` from a spawned task, breaks
    /// that and lets a session sit permanently over its limit.
    pub fn try_offer(&self, limit: usize, phase: &Arc<str>) -> bool {
        if self.paused.load(Ordering::Relaxed) || self.outstanding.load(Ordering::Relaxed) >= limit
        {
            return false;
        }
        if self
            .work
            .try_send(Work::Submit {
                phase: phase.clone(),
            })
            .is_ok()
        {
            self.outstanding.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
}

struct Pending {
    share_id: String,
    job_id: String,
    sent: Instant,
    scheduled_block: bool,
    reoffer: bool,
    header_hex: String,
    extranonce2_hex: String,
    ntime_hex: String,
    nonce_hex: String,
    frontend: usize,
    phase: String,
}

enum Incoming {
    Line(String),
    /// The socket ended. `fence` is [`SessionShared::kill_fence`] as the
    /// reader read it at the instant the end was observed, and it travels
    /// with the reason all the way to the records the closure fails.
    ///
    /// The value cannot be read where the records are built. The reader
    /// queues this the instant the socket ends, while `outstanding` is still
    /// non-zero because nothing has failed the pending submits yet, so a
    /// `KillDriver` polling just then still sees work in flight, pauses the
    /// session and bumps the fence. The session's `select!` is `biased` on
    /// control, so it takes that pause first and only reaches this value on
    /// the next pass -- by which time `SessionShared::fence` reads the
    /// kill's value, and a disconnect that happened before the kill, for its
    /// own unrelated reason, would be stamped as the kill's. The census
    /// would then re-offer it and `run::classify_gaps` would exempt it from
    /// the ordinary mid-run no-response check, so a share that committed
    /// could evade the acknowledgement-loss check entirely.
    ///
    /// So the identity is captured when the event happens, not when the
    /// record is convenient to build (EP-STATE, EP-OBSERVABILITY).
    Closed {
        reason: String,
        fence: u64,
    },
}

struct Connection {
    writer: tokio::net::tcp::OwnedWriteHalf,
    lines: mpsc::Receiver<Incoming>,
    reader_task: tokio::task::JoinHandle<()>,
    extranonce1: Vec<u8>,
    extranonce2_size: usize,
    next_id: u64,
    jobs: VecDeque<JobState>,
    pending: HashMap<u64, Pending>,
    last_tip: Option<String>,
    extranonce2_counter: u64,
}

impl Connection {
    fn drop_reader(&mut self) {
        self.reader_task.abort();
    }
}

/// Spawn one session task.
#[allow(clippy::too_many_arguments)]
pub fn spawn_session(
    config: SessionConfig,
    frontend: usize,
    address: String,
    shared: Arc<SessionShared>,
    max_outstanding: usize,
) -> SessionHandle {
    let (work_tx, work_rx) = mpsc::channel(max_outstanding.max(1));
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let outstanding = Arc::new(AtomicUsize::new(0));
    let paused = Arc::new(AtomicBool::new(false));
    let frontend_slot = Arc::new(AtomicUsize::new(frontend));
    let task = tokio::spawn(run_session(
        config.clone(),
        address,
        frontend_slot.clone(),
        shared.clone(),
        work_rx,
        control_rx,
        outstanding.clone(),
        paused.clone(),
        max_outstanding,
    ));
    SessionHandle {
        index: config.index,
        frontend: frontend_slot,
        outstanding,
        paused,
        work: work_tx,
        control: control_tx,
        task,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    config: SessionConfig,
    mut address: String,
    frontend: Arc<AtomicUsize>,
    shared: Arc<SessionShared>,
    mut work: mpsc::Receiver<Work>,
    mut control: mpsc::UnboundedReceiver<Control>,
    outstanding: Arc<AtomicUsize>,
    paused: Arc<AtomicBool>,
    max_outstanding: usize,
) {
    let mut stopping = false;
    let mut connection: Option<Connection> = None;
    let mut reconnect_reason = String::from("initial");
    // The phase the reconnect in progress belongs to, stamped when it was
    // decided -- the phase that sent `Control::Reconnect`, or the run's
    // phase when the socket closed or a retarget asked for a fresh
    // connection. Every attempt's record carries it, however many phases
    // pass before the reconnect completes; reading the run's phase at
    // completion labelled a late reconnect with the next phase, and the
    // phase that configured it lost the event (EP-STATE).
    let mut reconnect_phase = String::new();
    // When the connection now being sought was lost or given up. Every
    // attempt's record measures from here, so the outage is what is
    // reported, not the attempt that finally ended it.
    let mut reconnect_started = Instant::now();
    while !stopping {
        if connection.is_none() {
            match connect(&config, &address, &shared, &frontend).await {
                Ok(fresh) => {
                    let _ = shared.events.send(Event::Connected {
                        session: config.index,
                        frontend: frontend.load(Ordering::Relaxed),
                    });
                    if reconnect_reason != "initial" {
                        let _ = shared.events.send(Event::Reconnect(ReconnectRecord {
                            session: config.index,
                            frontend: frontend.load(Ordering::Relaxed),
                            phase: reconnect_phase.clone(),
                            reason: reconnect_reason.clone(),
                            completed: true,
                            error: None,
                            seconds: reconnect_started.elapsed().as_secs_f64(),
                        }));
                    }
                    connection = Some(fresh);
                }
                Err(error) => {
                    // The initial connection has no phase to belong to: the
                    // run stamps it "setup" until the first phase starts.
                    if reconnect_reason == "initial" {
                        reconnect_phase = shared.phase();
                    }
                    let _ = shared.events.send(Event::Reconnect(ReconnectRecord {
                        session: config.index,
                        frontend: frontend.load(Ordering::Relaxed),
                        phase: reconnect_phase.clone(),
                        reason: reconnect_reason.clone(),
                        completed: false,
                        error: Some(format!("{error:#}")),
                        seconds: reconnect_started.elapsed().as_secs_f64(),
                    }));
                    // Wait for a control message or a short backoff, so a
                    // frontend that is still restarting is not hammered.
                    tokio::select! {
                        message = control.recv() => {
                            match message {
                                Some(Control::Stop) | None => break,
                                Some(Control::Retarget { frontend: index, address: next, .. }) => {
                                    frontend.store(index, Ordering::Relaxed);
                                    address = next;
                                    paused.store(false, Ordering::Relaxed);
                                }
                                Some(Control::Pause) => {
                                    paused.store(true, Ordering::Relaxed);
                                    drain_work(&mut work, &outstanding, &shared, config.index);
                                }
                                Some(Control::CensusBarrier(ack)) => {
                                    let _ = shared.events.send(Event::CensusBarrier(ack));
                                }
                                // Already reconnecting: the request is being
                                // honoured, under the phase that was stamped
                                // when the connection went.
                                Some(Control::Reconnect { .. }) => {}
                                // Work that needs a connection cannot be held
                                // until there is one -- the block would be
                                // built on a stale job, the re-offer's answer
                                // would arrive after the census -- and used
                                // to be dropped here without a trace, so a
                                // re-offer that was never sent and one the
                                // server never answered read the same
                                // (EP-OBSERVABILITY). Each is reported as
                                // the failure it is.
                                Some(Control::ScheduledBlock) => {
                                    let _ = shared.events.send(Event::Failure(ClientFailure {
                                        session: config.index,
                                        phase: shared.phase(),
                                        kind: FailureKind::ScheduledBlock,
                                        recorded: false,
                                        error: "scheduled block: the session had no connection \
                                                (reconnecting)"
                                            .into(),
                                        at: Instant::now(),
                                    }));
                                }
                                Some(Control::Reoffer { share_id, .. }) => {
                                    let _ = shared.events.send(Event::Failure(ClientFailure {
                                        session: config.index,
                                        phase: shared.phase(),
                                        kind: FailureKind::Reoffer,
                                        recorded: false,
                                        error: format!(
                                            "re-offer of {share_id}: the session had no \
                                             connection (reconnecting), so it was never sent"
                                        ),
                                        at: Instant::now(),
                                    }));
                                }
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                    continue;
                }
            }
        }
        let active = connection.as_mut().expect("connection present");
        let can_submit = !paused.load(Ordering::Relaxed) && active.pending.len() < max_outstanding;
        tokio::select! {
            biased;
            message = control.recv() => {
                match message {
                    Some(Control::CensusBarrier(ack)) => {
                        let _ = shared.events.send(Event::CensusBarrier(ack));
                    }
                    None | Some(Control::Stop) => {
                        stopping = true;
                        drain_work(&mut work, &outstanding, &shared, config.index);
                    }
                    Some(Control::Pause) => {
                        paused.store(true, Ordering::Relaxed);
                        // Offers queued before the sender set the flag will
                        // now never send and must release their slot, or the
                        // run's drain would wait for work that is not coming.
                        drain_work(&mut work, &outstanding, &shared, config.index);
                    }
                    Some(Control::Retarget { frontend: index, address: next, reconnect }) => {
                        frontend.store(index, Ordering::Relaxed);
                        address = next;
                        paused.store(false, Ordering::Relaxed);
                        if reconnect {
                            reconnect_phase = shared.phase();
                            quiesce(active, &shared, &config, &frontend, &outstanding).await;
                            active.drop_reader();
                            connection = None;
                            reconnect_started = Instant::now();
                            reconnect_reason = "retarget".into();
                        }
                    }
                    Some(Control::Reconnect { reason, phase }) => {
                        reconnect_phase = phase.to_string();
                        // A pause outlives a reconnect that lands during it:
                        // the pause was decided for the frontend being
                        // restarted, and only the retarget that ends the
                        // restart lifts it. Lifting it here would let the
                        // scheduler offer to a session whose frontend is down.
                        quiesce(active, &shared, &config, &frontend, &outstanding).await;
                        active.drop_reader();
                        connection = None;
                        reconnect_started = Instant::now();
                        reconnect_reason = reason;
                    }
                    Some(Control::ScheduledBlock) => {
                        // A scheduled block occupies an outstanding slot like
                        // any other submit: its response goes through the same
                        // path, which releases the slot exactly once.
                        outstanding.fetch_add(1, Ordering::Relaxed);
                        let phase = shared.phase();
                        if let Err(failure) = offer(active, &config, &shared, &frontend, true, phase.clone()).await {
                            outstanding.fetch_sub(1, Ordering::Relaxed);
                            let _ = shared.events.send(Event::Failure(ClientFailure {
                                session: config.index,
                                phase,
                                kind: FailureKind::ScheduledBlock,
                                recorded: failure.recorded,
                                error: format!("scheduled block: {:#}", failure.error),
                                at: Instant::now(),
                            }));
                        }
                    }
                    Some(Control::Reoffer {
                        share_id,
                        job_id,
                        extranonce2_hex,
                        ntime_hex,
                        nonce_hex,
                        header_hex,
                    }) => {
                        outstanding.fetch_add(1, Ordering::Relaxed);
                        if let Err(error) = reoffer(
                            active, &config, &shared, &frontend, share_id, job_id,
                            extranonce2_hex, ntime_hex, nonce_hex, header_hex,
                        )
                        .await
                        {
                            outstanding.fetch_sub(1, Ordering::Relaxed);
                            let _ = shared.events.send(Event::Failure(ClientFailure {
                                session: config.index,
                                phase: shared.phase(),
                                kind: FailureKind::Reoffer,
                                recorded: false,
                                error: format!("re-offer: {error:#}"),
                                at: Instant::now(),
                            }));
                        }
                    }
                }
            }
            incoming = active.lines.recv() => {
                match incoming {
                    Some(Incoming::Line(line)) => {
                        if let Err(error) = handle_line(active, &line, &config, &shared, &frontend, &outstanding) {
                            let _ = shared.events.send(Event::Failure(ClientFailure {
                                session: config.index,
                                phase: shared.phase(),
                                kind: FailureKind::Line,
                                recorded: false,
                                error: format!("{error:#}"),
                                at: Instant::now(),
                            }));
                        }
                    }
                    other => {
                        // The closure carries the fence it was observed
                        // under; a reader that simply stopped has no earlier
                        // observation to carry, so this is its moment.
                        let (reason, fence) = match other {
                            Some(Incoming::Closed { reason, fence }) => {
                                (reason_or_default(reason), fence)
                            }
                            _ => ("reader stopped".to_owned(), shared.fence()),
                        };
                        reconnect_phase = shared.phase();
                        fail_pending(active, &reason, fence, &shared, &config, &outstanding);
                        let _ = shared.events.send(Event::Disconnected {
                            session: config.index,
                            frontend: frontend.load(Ordering::Relaxed),
                            reason: reason.clone(),
                        });
                        active.drop_reader();
                        connection = None;
                        reconnect_started = Instant::now();
                        reconnect_reason = format!("socket closed: {reason}");
                    }
                }
            }
            offered = work.recv(), if can_submit => {
                match offered {
                    None => { stopping = true; }
                    Some(Work::Submit { phase }) => {
                        let phase = phase.to_string();
                        if let Err(failure) = offer(active, &config, &shared, &frontend, false, phase.clone()).await {
                            outstanding.fetch_sub(1, Ordering::Relaxed);
                            let _ = shared.events.send(Event::Failure(ClientFailure {
                                session: config.index,
                                phase,
                                kind: FailureKind::Offer,
                                recorded: failure.recorded,
                                error: format!("{:#}", failure.error),
                                at: Instant::now(),
                            }));
                        }
                    }
                }
            }
        }
    }
    if let Some(mut active) = connection {
        // The run stopping is the cause, and it is happening now.
        fail_pending(
            &mut active,
            RUN_ENDED,
            shared.fence(),
            &shared,
            &config,
            &outstanding,
        );
        active.drop_reader();
    }
}

/// Discard offers this session has accepted but not yet sent, releasing their
/// outstanding slots so the scheduler's accounting stays exact.
fn drain_work(
    work: &mut mpsc::Receiver<Work>,
    outstanding: &Arc<AtomicUsize>,
    shared: &Arc<SessionShared>,
    session: usize,
) {
    while let Ok(Work::Submit { phase }) = work.try_recv() {
        outstanding.fetch_sub(1, Ordering::Relaxed);
        let _ = shared.events.send(Event::DiscardedOffer {
            session,
            phase: phase.to_string(),
        });
    }
}

/// How often the quiesce re-checks its deadline while no line arrives. A
/// poll interval, not a wait on anything the server governs.
const QUIESCE_POLL: Duration = Duration::from_millis(500);

/// Wait for every outstanding submit to settle before a deliberate close, so a
/// planned reconnect never manufactures indeterminate shares.
///
/// The wait is `config.quiesce_limit`: the configured share-commit timeout
/// plus the drain margin. A fixed 20 s here was reachable with
/// `--share-commit-timeout-seconds` above 20, and then a submit the server
/// was still legitimately working on was recorded as `NoResponse` by the
/// harness's own close, and a later commit of it read as a durability loss
/// that the configured deadline had never permitted (EP-ERRORS).
async fn quiesce(
    connection: &mut Connection,
    shared: &Arc<SessionShared>,
    config: &SessionConfig,
    frontend: &Arc<AtomicUsize>,
    outstanding: &Arc<AtomicUsize>,
) {
    let deadline = Instant::now() + config.quiesce_limit;
    while !connection.pending.is_empty() && Instant::now() < deadline {
        match tokio::time::timeout(QUIESCE_POLL, connection.lines.recv()).await {
            Ok(Some(Incoming::Line(line))) => {
                let _ = handle_line(connection, &line, config, shared, frontend, outstanding);
            }
            Ok(other) => {
                let (reason, fence) = match other {
                    Some(Incoming::Closed { reason, fence }) => (reason_or_default(reason), fence),
                    _ => ("reader stopped".to_owned(), shared.fence()),
                };
                fail_pending(connection, &reason, fence, shared, config, outstanding);
                return;
            }
            Err(_) => {}
        }
    }
    if !connection.pending.is_empty() {
        let reason = format!(
            "quiesce timed out after {:.1?}, the configured share-commit timeout plus its \
             margin, with the submit still unanswered",
            config.quiesce_limit
        );
        // The deadline expiring *is* the cause, and it is happening now.
        fail_pending(
            connection,
            &reason,
            shared.fence(),
            shared,
            config,
            outstanding,
        );
    }
}

fn reason_or_default(reason: String) -> String {
    if reason.is_empty() {
        "socket closed".to_owned()
    } else {
        reason
    }
}

/// Fail every submit still outstanding on `connection`, as `reason`.
///
/// `fence` is the run's kill fence as it stood **when the cause these records
/// report was observed** -- the instant the reader saw the socket end, the
/// instant the quiesce deadline expired, the instant the run stopped waiting.
/// It is a parameter rather than a `shared.fence()` read here because those
/// two moments are not always the same one, and the gap between them is
/// exactly where a mid-flight kill can land (see `Incoming::Closed`). Every
/// caller states which moment its records belong to.
fn fail_pending(
    connection: &mut Connection,
    reason: &str,
    fence: u64,
    shared: &Arc<SessionShared>,
    config: &SessionConfig,
    outstanding: &Arc<AtomicUsize>,
) {
    for (_, pending) in connection.pending.drain() {
        let _ = shared.events.send(Event::Submit(Box::new(SubmitRecord {
            share_id: pending.share_id,
            session: config.index,
            frontend: pending.frontend,
            phase: pending.phase,
            job_id: pending.job_id,
            sent: pending.sent,
            responded: None,
            latency_millis: None,
            outcome: Outcome::NoResponse {
                reason: reason.to_owned(),
            },
            scheduled_block: pending.scheduled_block,
            reoffer: pending.reoffer,
            fence,
            header_hex: pending.header_hex,
            extranonce2_hex: pending.extranonce2_hex,
            ntime_hex: pending.ntime_hex,
            nonce_hex: pending.nonce_hex,
        })));
        outstanding.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn connect(
    config: &SessionConfig,
    address: &str,
    shared: &Arc<SessionShared>,
    frontend: &Arc<AtomicUsize>,
) -> Result<Connection> {
    let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(address))
        .await
        .context("connect timed out")??;
    stream.set_nodelay(true)?;
    let (read_half, writer) = stream.into_split();
    let (tx, rx) = mpsc::channel(256);
    // The reader's own handle on the run's fence, so the moment the socket
    // ends is the moment the fence is read. See `Incoming::Closed`.
    let kill_fence = shared.kill_fence.clone();
    let reader_task = tokio::spawn(async move {
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    // Read before the send, not after: the send can await.
                    let fence = kill_fence.load(Ordering::SeqCst);
                    let _ = tx
                        .send(Incoming::Closed {
                            reason: "end of stream".into(),
                            fence,
                        })
                        .await;
                    break;
                }
                Ok(_) => {
                    if tx
                        .send(Incoming::Line(line.trim().to_owned()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(error) => {
                    let fence = kill_fence.load(Ordering::SeqCst);
                    let _ = tx
                        .send(Incoming::Closed {
                            reason: error.to_string(),
                            fence,
                        })
                        .await;
                    break;
                }
            }
        }
    });
    let mut connection = Connection {
        writer,
        lines: rx,
        reader_task,
        extranonce1: Vec::new(),
        extranonce2_size: 8,
        next_id: 1,
        jobs: VecDeque::new(),
        pending: HashMap::new(),
        last_tip: None,
        extranonce2_counter: u64::from(config.index as u32) << 32,
    };
    let subscribe = connection.next_id;
    connection.next_id += 1;
    write_line(
        &mut connection.writer,
        &json!({"id": subscribe, "method": "mining.subscribe", "params": ["qbit-prism-load/1"]}),
    )
    .await?;
    let response = await_response(&mut connection, subscribe, config, shared, frontend).await?;
    let result = response
        .get("result")
        .and_then(Value::as_array)
        .context("mining.subscribe did not return a result array")?;
    let extranonce1 = result
        .get(1)
        .and_then(Value::as_str)
        .context("mining.subscribe returned no extranonce1")?;
    connection.extranonce1 = hex::decode(extranonce1).context("extranonce1 is not hex")?;
    connection.extranonce2_size = result
        .get(2)
        .and_then(Value::as_u64)
        .context("mining.subscribe returned no extranonce2 size")?
        as usize;
    ensure!(
        connection.extranonce2_size > 0 && connection.extranonce2_size <= 32,
        "unusable extranonce2 size {}",
        connection.extranonce2_size
    );

    let configure = connection.next_id;
    connection.next_id += 1;
    write_line(
        &mut connection.writer,
        &json!({"id": configure, "method": "mining.configure", "params": [
            ["version-rolling"],
            {"version-rolling.mask": format!("{:08x}", config.version_rolling_mask)}
        ]}),
    )
    .await?;
    await_response(&mut connection, configure, config, shared, frontend).await?;

    let authorize = connection.next_id;
    connection.next_id += 1;
    write_line(
        &mut connection.writer,
        &json!({"id": authorize, "method": "mining.authorize",
                "params": [config.username, config.password]}),
    )
    .await?;
    let response = await_response(&mut connection, authorize, config, shared, frontend).await?;
    ensure!(
        response.get("result") == Some(&Value::Bool(true)),
        "mining.authorize was refused: {response}"
    );

    // A session is only usable once it holds work.
    let deadline = Instant::now() + config.handshake_timeout;
    while connection.jobs.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), "no job arrived after authorize");
        match tokio::time::timeout(remaining, connection.lines.recv()).await {
            Ok(Some(Incoming::Line(line))) => {
                consume(
                    &mut connection,
                    &line,
                    config,
                    shared,
                    &Arc::new(AtomicUsize::new(0)),
                    frontend,
                )?;
            }
            Ok(Some(Incoming::Closed { reason, .. })) => {
                bail!("socket closed during handshake: {reason}")
            }
            Ok(None) => bail!("reader stopped during handshake"),
            Err(_) => bail!("no job arrived after authorize"),
        }
    }
    Ok(connection)
}

async fn await_response(
    connection: &mut Connection,
    id: u64,
    config: &SessionConfig,
    shared: &Arc<SessionShared>,
    frontend: &Arc<AtomicUsize>,
) -> Result<Value> {
    let deadline = Instant::now() + config.handshake_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), "no response to request {id}");
        match tokio::time::timeout(remaining, connection.lines.recv()).await {
            Ok(Some(Incoming::Line(line))) => {
                let value: Value = serde_json::from_str(&line)
                    .with_context(|| format!("unparsable Stratum line {line:?}"))?;
                if value.get("id").and_then(Value::as_u64) == Some(id) {
                    if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
                        bail!("request {id} failed: {error}");
                    }
                    return Ok(value);
                }
                consume(
                    connection,
                    &line,
                    config,
                    shared,
                    &Arc::new(AtomicUsize::new(0)),
                    frontend,
                )?;
            }
            Ok(Some(Incoming::Closed { reason, .. })) => bail!("socket closed: {reason}"),
            Ok(None) => bail!("reader stopped"),
            Err(_) => bail!("no response to request {id}"),
        }
    }
}

fn handle_line(
    connection: &mut Connection,
    line: &str,
    config: &SessionConfig,
    shared: &Arc<SessionShared>,
    frontend: &Arc<AtomicUsize>,
    outstanding: &Arc<AtomicUsize>,
) -> Result<()> {
    consume(connection, line, config, shared, outstanding, frontend)
}

/// Dispatch one inbound line: a response to a submit, or a server push.
fn consume(
    connection: &mut Connection,
    line: &str,
    config: &SessionConfig,
    shared: &Arc<SessionShared>,
    outstanding: &Arc<AtomicUsize>,
    frontend: &Arc<AtomicUsize>,
) -> Result<()> {
    if line.is_empty() {
        return Ok(());
    }
    let value: Value =
        serde_json::from_str(line).with_context(|| format!("unparsable Stratum line {line:?}"))?;
    if let Some(id) = value.get("id").and_then(Value::as_u64) {
        if let Some(pending) = connection.pending.remove(&id) {
            let responded = Instant::now();
            let outcome = if value.get("result") == Some(&Value::Bool(true)) {
                Outcome::Accepted
            } else {
                Outcome::Rejected(parse_rejection(&value))
            };
            let _ = shared.events.send(Event::Submit(Box::new(SubmitRecord {
                share_id: pending.share_id,
                session: config.index,
                frontend: pending.frontend,
                phase: pending.phase,
                job_id: pending.job_id,
                sent: pending.sent,
                responded: Some(responded),
                latency_millis: Some(
                    responded
                        .saturating_duration_since(pending.sent)
                        .as_secs_f64()
                        * 1000.0,
                ),
                outcome,
                scheduled_block: pending.scheduled_block,
                reoffer: pending.reoffer,
                fence: shared.fence(),
                header_hex: pending.header_hex,
                extranonce2_hex: pending.extranonce2_hex,
                ntime_hex: pending.ntime_hex,
                nonce_hex: pending.nonce_hex,
            })));
            outstanding.fetch_sub(1, Ordering::Relaxed);
            return Ok(());
        }
    }
    match value.get("method").and_then(Value::as_str) {
        Some("mining.notify") => note_job(connection, &value, config, shared, frontend)?,
        Some("mining.set_difficulty") => {
            if let Some(advertised) = value["params"][0].as_f64() {
                // The harness pins the difficulty, so a disagreement here means
                // the frontend is not running the configuration the artifact
                // will claim (EP-CONFIG).
                let configured = config.share_difficulty;
                if (advertised - configured).abs() > configured.abs() * 1e-9 {
                    let _ = shared.events.send(Event::DifficultyMismatch {
                        session: config.index,
                        advertised,
                        configured,
                    });
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_rejection(value: &Value) -> Rejection {
    let error = &value["error"];
    Rejection {
        code: error[0].as_i64().unwrap_or(-1),
        reason_id: error[2]["reason_id"].as_str().map(str::to_owned),
        message: error[1].as_str().unwrap_or_default().to_owned(),
    }
}

fn note_job(
    connection: &mut Connection,
    value: &Value,
    config: &SessionConfig,
    shared: &Arc<SessionShared>,
    frontend: &Arc<AtomicUsize>,
) -> Result<()> {
    let params = value["params"]
        .as_array()
        .context("mining.notify has no parameters")?;
    let job_id = params[0].as_str().context("notify job id")?.to_owned();
    let prevhash = params[1].as_str().context("notify prevhash")?;
    let header_prev = header_prev_from_wire(prevhash)?;
    let tip = {
        let mut bytes = header_prev.clone();
        bytes.reverse();
        hex::encode(bytes)
    };
    let coinb1 = hex::decode(params[2].as_str().context("notify coinb1")?)?;
    let coinb2 = hex::decode(params[3].as_str().context("notify coinb2")?)?;
    let branch = params[4]
        .as_array()
        .context("notify merkle branch")?
        .iter()
        .map(|entry| {
            let bytes = hex::decode(entry.as_str().context("merkle branch entry")?)?;
            let array: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("merkle branch entry is not 32 bytes"))?;
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?;
    let version = codec::parse_u32_hex(params[5].as_str().context("notify version")?)?;
    let nbits = codec::parse_u32_hex(params[6].as_str().context("notify nbits")?)?;
    let ntime = codec::parse_u32_hex(params[7].as_str().context("notify ntime")?)?;
    let clean_jobs = params[8].as_bool().unwrap_or(false);
    let network_target = codec::target_from_compact(nbits)?;
    // The share target is derived from the difficulty the harness configured,
    // through the server's own `difficulty_target`, so the client's acceptance
    // test is bit-for-bit the server's.
    let share_target =
        codec::difficulty_target(config.share_difficulty)?.max(network_target.clone());
    let job = JobState {
        job_id,
        tip: tip.clone(),
        header_prev,
        coinb1,
        coinb2,
        branch,
        version,
        nbits,
        ntime,
        clean_jobs,
        received: Instant::now(),
        share_target: target_bytes_le(&share_target),
        network_target: target_bytes_le(&network_target),
    };
    let job_id = job.job_id.clone();
    let seen = job.received;
    if clean_jobs {
        connection.jobs.clear();
    }
    connection.jobs.push_back(job);
    while connection.jobs.len() > JOB_HISTORY {
        connection.jobs.pop_front();
    }
    let frontend = frontend.load(Ordering::Relaxed);
    if shared.recording_notifies() {
        let _ = shared.events.send(Event::Notify(NotifySighting {
            session: config.index,
            frontend,
            job_id,
            tip: tip.clone(),
            clean_jobs,
            at: seen,
        }));
    }
    if connection.last_tip.as_deref() != Some(tip.as_str()) {
        connection.last_tip = Some(tip.clone());
        let _ = shared.events.send(Event::Tip(TipSighting {
            session: config.index,
            frontend,
            tip,
            at: seen,
        }));
    }
    Ok(())
}

/// Search the newest job and send one submit, recorded under `phase`: the
/// phase that offered it, not whatever phase the run is in when it is sent.
async fn offer(
    connection: &mut Connection,
    config: &SessionConfig,
    shared: &Arc<SessionShared>,
    frontend: &Arc<AtomicUsize>,
    scheduled_block: bool,
    phase: String,
) -> std::result::Result<(), OfferFailure> {
    let job = connection
        .jobs
        .back()
        .cloned()
        .context("no current job to mine")
        .map_err(OfferFailure::unrecorded)?;
    connection.extranonce2_counter = connection.extranonce2_counter.wrapping_add(1);
    let mut extranonce2 = vec![0u8; connection.extranonce2_size];
    let counter = connection.extranonce2_counter.to_be_bytes();
    let width = connection.extranonce2_size.min(counter.len());
    extranonce2[connection.extranonce2_size - width..]
        .copy_from_slice(&counter[counter.len() - width..]);
    let extranonce1 = connection.extranonce1.clone();
    let session = config.index;
    let events = shared.events.clone();
    let found = if scheduled_block {
        let job = job.clone();
        let extranonce2 = extranonce2.clone();
        tokio::task::spawn_blocking(move || search(&job, &extranonce1, &extranonce2, true, None))
            .await
            .map_err(|error| OfferFailure::unrecorded(error.into()))?
    } else {
        search(
            &job,
            &extranonce1,
            &extranonce2,
            false,
            Some((session, events)),
        )
    };
    let Some((nonce, header)) = found else {
        return Err(OfferFailure::unrecorded(anyhow::anyhow!(
            "no {} solution found under job {}",
            if scheduled_block { "block" } else { "share" },
            job.job_id
        )));
    };
    let extranonce2_hex = hex::encode(&extranonce2);
    let ntime_hex = format!("{:08x}", job.ntime);
    let nonce_hex = format!("{nonce:08x}");
    let id = connection.next_id;
    connection.next_id += 1;
    let share = share_id(&config.username, &header);
    let request = json!({"id": id, "method": "mining.submit", "params": [
        config.username.clone(), job.job_id.clone(), extranonce2_hex.clone(),
        ntime_hex.clone(), nonce_hex.clone()
    ]});
    let sent = Instant::now();
    connection.pending.insert(
        id,
        Pending {
            share_id: share,
            job_id: job.job_id.clone(),
            sent,
            scheduled_block,
            reoffer: false,
            header_hex: hex::encode(&header),
            extranonce2_hex,
            ntime_hex,
            nonce_hex,
            frontend: frontend.load(Ordering::Relaxed),
            phase,
        },
    );
    if let Err(error) = write_line(&mut connection.writer, &request).await {
        if let Some(pending) = connection.pending.remove(&id) {
            let _ = shared.events.send(Event::Submit(Box::new(SubmitRecord {
                share_id: pending.share_id,
                session: config.index,
                frontend: pending.frontend,
                phase: pending.phase,
                job_id: pending.job_id,
                sent: pending.sent,
                responded: None,
                latency_millis: None,
                outcome: Outcome::NoResponse {
                    reason: format!("write failed: {error}"),
                },
                scheduled_block: pending.scheduled_block,
                reoffer: pending.reoffer,
                fence: shared.fence(),
                header_hex: pending.header_hex,
                extranonce2_hex: pending.extranonce2_hex,
                ntime_hex: pending.ntime_hex,
                nonce_hex: pending.nonce_hex,
            })));
        }
        return Err(OfferFailure {
            error,
            recorded: true,
        });
    }
    Ok(())
}

/// Find a nonce. Share searches step over any solution that also meets the
/// network target: an unscheduled block is never submitted, only counted.
fn search(
    job: &JobState,
    extranonce1: &[u8],
    extranonce2: &[u8],
    want_block: bool,
    discards: Option<(usize, tokio::sync::mpsc::UnboundedSender<Event>)>,
) -> Option<(u32, Vec<u8>)> {
    let merkle = merkle_root(
        &job.coinb1,
        extranonce1,
        extranonce2,
        &job.coinb2,
        &job.branch,
    );
    let mut header = assemble_header(
        job.version,
        &job.header_prev,
        &merkle,
        job.ntime,
        job.nbits,
        0,
    );
    let span = if want_block { u32::MAX } else { NONCE_SPAN };
    for nonce in 0..span {
        header[76..80].copy_from_slice(&nonce.to_le_bytes());
        let hash = codec::double_sha256(&header);
        if want_block {
            if le_at_most(&hash, &job.network_target) {
                return Some((nonce, header));
            }
            continue;
        }
        if !le_at_most(&hash, &job.share_target) {
            continue;
        }
        if le_at_most(&hash, &job.network_target) {
            if let Some((session, events)) = &discards {
                let _ = events.send(Event::DiscardedBlockSolution { session: *session });
            }
            continue;
        }
        return Some((nonce, header));
    }
    None
}

async fn write_line(writer: &mut tokio::net::tcp::OwnedWriteHalf, value: &Value) -> Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await?;
    Ok(())
}

/// Send a submit that reproduces an earlier one byte for byte, so the server
/// can say what it did with the share whose answer was lost.
#[allow(clippy::too_many_arguments)]
async fn reoffer(
    connection: &mut Connection,
    config: &SessionConfig,
    shared: &Arc<SessionShared>,
    frontend: &Arc<AtomicUsize>,
    share: String,
    job_id: String,
    extranonce2_hex: String,
    ntime_hex: String,
    nonce_hex: String,
    header_hex: String,
) -> Result<()> {
    let id = connection.next_id;
    connection.next_id += 1;
    let request = json!({"id": id, "method": "mining.submit", "params": [
        config.username.clone(), job_id.clone(), extranonce2_hex.clone(),
        ntime_hex.clone(), nonce_hex.clone()
    ]});
    connection.pending.insert(
        id,
        Pending {
            share_id: share,
            job_id,
            sent: Instant::now(),
            scheduled_block: false,
            reoffer: true,
            header_hex,
            extranonce2_hex,
            ntime_hex,
            nonce_hex,
            frontend: frontend.load(Ordering::Relaxed),
            phase: shared.phase(),
        },
    );
    if let Err(error) = write_line(&mut connection.writer, &request).await {
        connection.pending.remove(&id);
        return Err(error);
    }
    Ok(())
}

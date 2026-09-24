//! Builder-only ordered parallel serialization: the same bytes, produced by
//! several threads in index order.
//!
//! Nothing here defines a canonical byte layout. Each array element and each
//! struct field value is written by `serde_json`'s own serializer (the same
//! escaping and the same `arbitrary_precision` integer digits as the whole
//! value would get), and the only framing added by hand is the `[`, `,`, `]`
//! and `{`, `,`, `"name":`, `}` that `serde_json` writes around them. Verifier
//! paths never call into this module: they serialize whole values serially.
use serde::ser::{Impossible, SerializeStruct, Serializer};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Condvar, Mutex, OnceLock};
use std::thread::{Scope, ScopedJoinHandle};

/// How much of a build may run on worker threads.
///
/// [`Parallelism::serial`] keeps every byte on the calling thread through the
/// unmodified `serde_json::to_writer` path; anything wider splits share arrays
/// into `chunk_len` element chunks that `workers` threads serialize while the
/// caller consumes them in index order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Parallelism {
    workers: usize,
    chunk_len: usize,
}

impl Parallelism {
    const MAX_WORKERS: usize = 8;
    const DEFAULT_CHUNK_LEN: usize = 4096;

    /// One thread, whole-value `serde_json` serialization.
    pub const fn serial() -> Self {
        Self {
            workers: 1,
            chunk_len: usize::MAX,
        }
    }

    /// `workers` threads over `chunk_len` element chunks; both are raised to
    /// at least one.
    pub fn new(workers: usize, chunk_len: usize) -> Self {
        Self {
            workers: workers.max(1),
            chunk_len: chunk_len.max(1),
        }
    }

    /// This process's builder parallelism: [`Parallelism::serial`] when the
    /// builder pool is disabled ([`configure_builder_threads`] with `0`),
    /// otherwise a per-stage window of up to eight chunks in flight (at most
    /// the pool's thread count) over 4096-element chunks. The window bounds
    /// look-ahead memory per stage; the threads are the pool's (see
    /// [`builder_threads`]), shared by every stage in the process.
    pub fn detect() -> Self {
        let threads = builder_threads();
        if threads == 0 {
            Self::serial()
        } else {
            Self::new(threads.min(Self::MAX_WORKERS), Self::DEFAULT_CHUNK_LEN)
        }
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    pub fn chunk_len(&self) -> usize {
        self.chunk_len
    }

    pub fn is_serial(&self) -> bool {
        self.workers <= 1
    }
}

impl Default for Parallelism {
    fn default() -> Self {
        Self::serial()
    }
}

/// One job on the builder pool: a borrowed closure whose lifetime was erased
/// by [`ordered_chunks`], which never returns (or unwinds) before the job
/// has finished.
type Job = Box<dyn FnOnce() + Send + 'static>;

/// The builder pool's thread count for this process, fixed by the operator
/// before the pool starts. `0` disables the pool: every builder runs serially.
static CONFIGURED_THREADS: OnceLock<usize> = OnceLock::new();

/// The pool itself, started on first use.
static POOL: OnceLock<&'static Pool> = OnceLock::new();

thread_local! {
    /// Set on every pool thread: a builder that runs *on* the pool must not
    /// wait for the pool, so [`ordered_chunks`] runs inline there.
    static ON_POOL_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// [`configure_builder_threads`] was called after the count was fixed, by an
/// earlier call or by the pool starting, with a different value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuilderThreadsFixed {
    /// The count in force.
    pub threads: usize,
}

impl std::fmt::Display for BuilderThreadsFixed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "builder threads already fixed at {} for this process",
            self.threads
        )
    }
}

impl std::error::Error for BuilderThreadsFixed {}

/// Fix the builder pool's thread count for this process before the pool
/// starts: `0` disables the pool and every builder runs serially
/// ([`Parallelism::detect`] returns [`Parallelism::serial`]). Idempotent for
/// the same value; a different value after the count is fixed is an error.
pub fn configure_builder_threads(threads: usize) -> Result<(), BuilderThreadsFixed> {
    if let Some(pool) = POOL.get() {
        let started = pool.started();
        return if started == threads {
            Ok(())
        } else {
            Err(BuilderThreadsFixed { threads: started })
        };
    }
    match CONFIGURED_THREADS.set(threads) {
        Ok(()) => Ok(()),
        Err(_) => {
            let fixed = *CONFIGURED_THREADS.get().expect("set");
            if fixed == threads {
                Ok(())
            } else {
                Err(BuilderThreadsFixed { threads: fixed })
            }
        }
    }
}

/// The builder pool's thread count: the configured value, else three
/// quarters of the host's cores clamped to `1..=4`.
///
/// Four is where the measured refresh stops getting faster: every stage's
/// consumer (the SHA-256 states and the fold's merge) is the bound, and
/// four producers keep it fed; more threads only spread the counted-share
/// strings and chunk buffers over more allocator arenas (at 400k shares,
/// 16 threads retained ~130 MiB more per frontend than four for the same
/// refresh time; two threads left the tee's producers as the bottleneck).
///
/// What that costs on a host: the pool's threads plus, per refresh, its two
/// lane threads (Tokio's blocking pool) and its two digest threads, so up to
/// 8 CPU-bound threads per frontend process at the default; every frontend
/// on the host has its own pool, so two frontends can run 16. The pool only
/// works during the refresh's build, which ends before the fan-out.
/// Operators size it with `PRISM_REFRESH_BUILD_THREADS` (up to 64).
pub fn builder_threads() -> usize {
    if let Some(pool) = POOL.get() {
        return pool.started();
    }
    CONFIGURED_THREADS.get().copied().unwrap_or_else(|| {
        (std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1)
            * 3
            / 4)
        .clamp(1, 4)
    })
}

/// The builder's worker threads: one process-wide pool, started on first
/// use and never stopped, so every refresh's chunk work lands on the same
/// threads. That keeps their allocator arenas stable across refreshes;
/// threads spawned per stage would each take a fresh arena and leave the
/// counted-share strings and chunk buffers fragmented across dozens of them.
struct Pool {
    queue: Mutex<VecDeque<Job>>,
    signal: Condvar,
    /// Threads that actually started; a spawn failure leaves fewer, and
    /// none at all makes every caller run serially.
    started: AtomicUsize,
}

impl Pool {
    /// The pool, started with [`builder_threads`] threads on first use;
    /// `None` when the operator disabled it.
    fn global() -> Option<&'static Pool> {
        if builder_threads() == 0 {
            return None;
        }
        Some(POOL.get_or_init(|| {
            let pool: &'static Pool = Box::leak(Box::new(Pool {
                queue: Mutex::new(VecDeque::new()),
                signal: Condvar::new(),
                started: AtomicUsize::new(0),
            }));
            for index in 0..builder_threads() {
                // A failed spawn is an ordinary resource error: keep the
                // threads that did start; with none, callers stay serial.
                if std::thread::Builder::new()
                    .name(format!("prism-build-{index}"))
                    .spawn(move || pool.run())
                    .is_ok()
                {
                    pool.started.fetch_add(1, Ordering::SeqCst);
                }
            }
            pool
        }))
    }

    fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    fn run(&self) {
        ON_POOL_THREAD.with(|flag| flag.set(true));
        loop {
            let job = {
                let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    if let Some(job) = queue.pop_front() {
                        break job;
                    }
                    queue = self.signal.wait(queue).unwrap_or_else(|e| e.into_inner());
                }
            };
            // Jobs catch their own panics; one that unwound here would take
            // a pool thread with it.
            job();
        }
    }

    fn submit(&self, job: Job) {
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(job);
        self.signal.notify_one();
    }
}

struct State<C, E> {
    ready: BTreeMap<usize, C>,
    failed: Option<E>,
    panicked: Option<Box<dyn std::any::Any + Send>>,
    outstanding: usize,
}

impl<C, E> State<C, E> {
    fn stopping(&self) -> bool {
        self.failed.is_some() || self.panicked.is_some()
    }
}

/// Waits for every submitted job to finish, also while unwinding: no job may
/// outlive the borrowed closures and state it was given.
struct Drain<'a, C, E> {
    state: &'a Mutex<State<C, E>>,
    signal: &'a Condvar,
}

impl<C, E> Drop for Drain<'_, C, E> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        while state.outstanding > 0 {
            state = self.signal.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// Produce chunks `0..count` on the builder pool and consume each one, in
/// index order, on the calling thread. At most `2 * workers` chunks are
/// submitted ahead of the consumer, so memory stays bounded by that many
/// chunks. The first error, from either side, stops further submission and
/// is returned once the jobs already running have finished; a panic in
/// `produce` is re-raised here, likewise only after every job has finished,
/// and a panic in `consume` unwinds only after every job has finished.
///
/// Contract for `produce`: it must never block on anything the consumer
/// feeds (the consumer runs on the calling thread and waits for chunks in
/// order) and it must not depend on the pool, because it may itself be run
/// on a pool thread. A caller that is already on a pool thread, a disabled
/// pool, or a pool whose threads failed to start all run this function
/// serially on the calling thread, so nested use cannot deadlock the pool.
pub(crate) fn ordered_chunks<C, E>(
    count: usize,
    workers: usize,
    produce: impl Fn(usize) -> Result<C, E> + Sync,
    mut consume: impl FnMut(usize, C) -> Result<(), E>,
) -> Result<(), E>
where
    C: Send,
    E: Send,
{
    if count == 0 {
        return Ok(());
    }
    let pool = match Pool::global() {
        Some(pool) if pool.started() > 0 && !ON_POOL_THREAD.with(Cell::get) => pool,
        _ => {
            #[cfg(test)]
            SERIAL_FALLBACKS.fetch_add(1, Ordering::SeqCst);
            for index in 0..count {
                consume(index, produce(index)?)?;
            }
            return Ok(());
        }
    };
    let max_inflight = workers.clamp(1, count) * 2;
    let state = Mutex::new(State {
        ready: BTreeMap::new(),
        failed: None,
        panicked: None,
        outstanding: 0,
    });
    let signal = Condvar::new();
    let drain = Drain {
        state: &state,
        signal: &signal,
    };
    let produce: &(dyn Fn(usize) -> Result<C, E> + Sync) = &produce;
    let (state_ref, signal_ref) = (&state, &signal);
    let mut next = 0;
    for index in 0..count {
        {
            let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
            while next < count && next - index < max_inflight && !guard.stopping() {
                guard.outstanding += 1;
                let submitted = next;
                next += 1;
                let job: Box<dyn FnOnce() + Send + '_> = Box::new(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        produce(submitted)
                    }));
                    let mut guard = state_ref.lock().unwrap_or_else(|e| e.into_inner());
                    match outcome {
                        Ok(Ok(chunk)) => {
                            guard.ready.insert(submitted, chunk);
                        }
                        Ok(Err(error)) => {
                            guard.failed.get_or_insert(error);
                        }
                        Err(panic) => {
                            guard.panicked.get_or_insert(panic);
                        }
                    }
                    guard.outstanding -= 1;
                    signal_ref.notify_all();
                });
                // SAFETY: the job borrows `produce`, `state` and `signal`,
                // which outlive every exit path of this function: `drain`,
                // declared after them, waits for `outstanding` to reach zero
                // before it is dropped, on return and on unwind alike, and
                // jobs never block, so that wait ends.
                let job: Job = unsafe { std::mem::transmute(job) };
                pool.submit(job);
            }
        }
        let chunk = {
            let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if let Some(chunk) = guard.ready.remove(&index) {
                    break Some(chunk);
                }
                if guard.stopping() {
                    break None;
                }
                guard = signal.wait(guard).unwrap_or_else(|e| e.into_inner());
            }
        };
        let Some(chunk) = chunk else {
            break;
        };
        if let Err(error) = consume(index, chunk) {
            state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .failed
                .get_or_insert(error);
            break;
        }
    }
    drop(drain);
    let mut state = state.into_inner().unwrap_or_else(|e| e.into_inner());
    if let Some(panic) = state.panicked.take() {
        std::panic::resume_unwind(panic);
    }
    match state.failed.take() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Times [`ordered_chunks`] ran serially because the pool was unavailable
/// or the caller was already on a pool thread.
#[cfg(test)]
static SERIAL_FALLBACKS: AtomicUsize = AtomicUsize::new(0);

/// Reusable chunk buffers. A pipeline allocates at most its in-flight count
/// of them, each growing once to its largest chunk, instead of one fresh
/// allocation per chunk. That bound holds as long as consumers hand every
/// buffer back with [`BufferPool::give`] (the hasher thread of
/// [`spawn_hasher`] does); a buffer that is dropped instead is simply
/// allocated anew by the next producer.
pub(crate) struct BufferPool(Mutex<Vec<Vec<u8>>>);

impl BufferPool {
    pub(crate) fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    pub(crate) fn take(&self) -> Vec<u8> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop()
            .unwrap_or_default()
    }

    pub(crate) fn give(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(buffer);
    }
}

/// A SHA-256 state advanced on its own thread over the owned buffers sent
/// to it, in send order; each buffer goes back to `pool` once hashed. Drop
/// the sender, then join for the state.
pub(crate) fn spawn_hasher<'scope, 'env: 'scope>(
    scope: &'scope Scope<'scope, 'env>,
    pool: &'env BufferPool,
    depth: usize,
) -> (SyncSender<Vec<u8>>, ScopedJoinHandle<'scope, Sha256>) {
    let (send, receive) = sync_channel::<Vec<u8>>(depth);
    let handle = scope.spawn(move || {
        let mut hasher = Sha256::new();
        for buffer in receive {
            hasher.update(&buffer);
            pool.give(buffer);
        }
        hasher
    });
    (send, handle)
}

/// The error a pipeline reports when a helper thread it feeds has gone away.
pub(crate) fn stopped(what: &str) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::other(format!("{what} thread stopped")))
}

/// The bytes of `serde_json::to_writer(items)` as owned frames handed to
/// `consume` in order: `[`, each chunk (`,`-prefixed after the first), `]`.
/// Every element is still written by `serde_json::to_writer`. `consume` owns
/// each frame and returns it to `pool` when done, so the producers reuse it.
/// A slice no longer than one chunk arrives as a single frame.
pub(crate) fn write_array_frames<T>(
    items: &[T],
    parallelism: Parallelism,
    pool: &BufferPool,
    mut consume: impl FnMut(Vec<u8>) -> Result<(), serde_json::Error>,
) -> Result<(), serde_json::Error>
where
    T: Serialize + Sync,
{
    let chunk_len = parallelism.chunk_len.min(items.len()).max(1);
    if parallelism.is_serial() || items.len() <= chunk_len {
        let mut frame = pool.take();
        serde_json::to_writer(&mut frame, items)?;
        return consume(frame);
    }
    let chunks = items.len().div_ceil(chunk_len);
    let mut open = pool.take();
    open.push(b'[');
    consume(open)?;
    ordered_chunks(
        chunks,
        parallelism.workers,
        |index| {
            let chunk = &items[index * chunk_len..((index + 1) * chunk_len).min(items.len())];
            let mut buffer = pool.take();
            for (position, item) in chunk.iter().enumerate() {
                if position > 0 || index > 0 {
                    buffer.push(b',');
                }
                serde_json::to_writer(&mut buffer, item)?;
            }
            Ok::<_, serde_json::Error>(buffer)
        },
        |_, buffer| consume(buffer),
    )?;
    let mut close = pool.take();
    close.push(b']');
    consume(close)
}

/// Write exactly the bytes of `serde_json::to_writer(writer, items)`.
///
/// Serial parallelism, or a slice no longer than one chunk, is that call.
/// Otherwise the frames of [`write_array_frames`] reach `writer` in order.
pub(crate) fn write_array<T, W>(
    writer: &mut W,
    items: &[T],
    parallelism: Parallelism,
) -> Result<(), serde_json::Error>
where
    T: Serialize + Sync,
    W: Write,
{
    if parallelism.is_serial() || items.len() <= parallelism.chunk_len {
        return serde_json::to_writer(writer, items);
    }
    let pool = BufferPool::new();
    write_array_frames(items, parallelism, &pool, |frame| {
        writer.write_all(&frame).map_err(serde_json::Error::io)?;
        pool.give(frame);
        Ok(())
    })
}

/// A serializer for one struct value whose bytes are exactly `serde_json`'s
/// compact struct output, with a single named field's value bytes supplied
/// by `splice` instead of by the field's own `Serialize`.
///
/// The struct's field order, names and `skip_serializing_if` decisions stay
/// with its derived `Serialize` implementation: this only writes the framing
/// (`{`, `,`, `"name":`, `}`) and hands every other field value, and every
/// field name, to `serde_json::to_writer` on the same writer. A value that
/// is not a struct is refused; nested values never reach this serializer.
pub(crate) struct StructSplice<'w, W, F> {
    writer: &'w mut W,
    field: &'static str,
    splice: Option<F>,
    opening: &'static [u8],
}

impl<'w, W, F> StructSplice<'w, W, F>
where
    W: Write,
    F: FnOnce(&mut W) -> Result<(), serde_json::Error>,
{
    /// A complete `{...}` object.
    pub(crate) fn object(writer: &'w mut W, field: &'static str, splice: F) -> Self {
        Self {
            writer,
            field,
            splice: Some(splice),
            opening: b"{",
        }
    }

    /// The remaining fields of an object whose opening brace and earlier
    /// fields were already written: `,"name":...}` instead of `{"name":...}`,
    /// exactly what `audit_hash::ContinueObject` produces.
    pub(crate) fn continuing(writer: &'w mut W, field: &'static str, splice: F) -> Self {
        Self {
            writer,
            field,
            splice: Some(splice),
            opening: b",",
        }
    }
}

fn unsupported(what: &str) -> serde_json::Error {
    serde::ser::Error::custom(format!(
        "StructSplice serializes one struct; {what} is not a struct"
    ))
}

macro_rules! refuse {
    ($($method:ident($($argument:ident: $type:ty),*) -> $what:literal;)*) => {
        $(fn $method(self, $($argument: $type),*) -> Result<(), serde_json::Error> {
            $(let _ = $argument;)*
            Err(unsupported($what))
        })*
    };
}

impl<'w, W, F> Serializer for StructSplice<'w, W, F>
where
    W: Write,
    F: FnOnce(&mut W) -> Result<(), serde_json::Error>,
{
    type Ok = ();
    type Error = serde_json::Error;
    type SerializeSeq = Impossible<(), serde_json::Error>;
    type SerializeTuple = Impossible<(), serde_json::Error>;
    type SerializeTupleStruct = Impossible<(), serde_json::Error>;
    type SerializeTupleVariant = Impossible<(), serde_json::Error>;
    type SerializeMap = Impossible<(), serde_json::Error>;
    type SerializeStruct = SpliceFields<'w, W, F>;
    type SerializeStructVariant = Impossible<(), serde_json::Error>;

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.writer
            .write_all(self.opening)
            .map_err(serde_json::Error::io)?;
        Ok(SpliceFields {
            writer: self.writer,
            field: self.field,
            splice: self.splice,
            first: true,
        })
    }

    refuse! {
        serialize_bool(value: bool) -> "bool";
        serialize_i8(value: i8) -> "i8";
        serialize_i16(value: i16) -> "i16";
        serialize_i32(value: i32) -> "i32";
        serialize_i64(value: i64) -> "i64";
        serialize_u8(value: u8) -> "u8";
        serialize_u16(value: u16) -> "u16";
        serialize_u32(value: u32) -> "u32";
        serialize_u64(value: u64) -> "u64";
        serialize_f32(value: f32) -> "f32";
        serialize_f64(value: f64) -> "f64";
        serialize_char(value: char) -> "char";
        serialize_str(value: &str) -> "str";
        serialize_bytes(value: &[u8]) -> "bytes";
        serialize_none() -> "none";
        serialize_unit() -> "unit";
        serialize_unit_struct(name: &'static str) -> "unit struct";
        serialize_unit_variant(name: &'static str, index: u32, variant: &'static str) -> "unit variant";
    }

    fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<(), Self::Error> {
        Err(unsupported("some"))
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Err(unsupported("newtype struct"))
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Err(unsupported("newtype variant"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(unsupported("seq"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(unsupported("tuple"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(unsupported("tuple struct"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(unsupported("tuple variant"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(unsupported("map"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(unsupported("struct variant"))
    }
}

pub(crate) struct SpliceFields<'w, W, F> {
    writer: &'w mut W,
    field: &'static str,
    splice: Option<F>,
    first: bool,
}

impl<W, F> SerializeStruct for SpliceFields<'_, W, F>
where
    W: Write,
    F: FnOnce(&mut W) -> Result<(), serde_json::Error>,
{
    type Ok = ();
    type Error = serde_json::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        if !self.first {
            self.writer.write_all(b",").map_err(serde_json::Error::io)?;
        }
        self.first = false;
        serde_json::to_writer(&mut *self.writer, key)?;
        self.writer.write_all(b":").map_err(serde_json::Error::io)?;
        if key == self.field {
            let splice = self
                .splice
                .take()
                .ok_or_else(|| serde::ser::Error::custom("spliced field serialized twice"))?;
            splice(&mut *self.writer)
        } else {
            serde_json::to_writer(&mut *self.writer, value)
        }
    }

    fn skip_field(&mut self, _key: &'static str) -> Result<(), Self::Error> {
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(b"}").map_err(serde_json::Error::io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Serialize)]
    struct Row {
        seq: u64,
        text: String,
        wide: u128,
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    }

    fn rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|index| Row {
                seq: index as u64,
                text: format!("r\"{index}\\\u{1F600}\n\u{0}"),
                wide: if index % 3 == 0 {
                    u128::MAX - index as u128
                } else {
                    index as u128
                },
                note: (index % 2 == 0).then(|| "credit".into()),
            })
            .collect()
    }

    #[test]
    fn write_array_matches_serde_json_for_every_chunking() {
        for count in [0usize, 1, 2, 3, 7, 8, 9, 64, 65, 200] {
            let items = rows(count);
            let expected = serde_json::to_vec(&items).unwrap();
            for (workers, chunk_len) in [(1, 1), (2, 1), (3, 2), (4, 7), (8, 8), (5, 9), (2, 1000)]
            {
                let mut actual = Vec::new();
                write_array(&mut actual, &items, Parallelism::new(workers, chunk_len)).unwrap();
                assert_eq!(
                    actual, expected,
                    "count {count} workers {workers} chunk {chunk_len}"
                );
            }
            let mut serial = Vec::new();
            write_array(&mut serial, &items, Parallelism::serial()).unwrap();
            assert_eq!(serial, expected);
        }
    }

    #[test]
    fn ordered_chunks_consumes_in_order_with_bounded_lookahead() {
        let peak = AtomicUsize::new(0);
        let inflight = AtomicUsize::new(0);
        let mut seen = Vec::new();
        ordered_chunks(
            50,
            3,
            |index| {
                let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_micros((index % 5) as u64 * 50));
                Ok::<_, ()>(index * 10)
            },
            |index, value| {
                inflight.fetch_sub(1, Ordering::SeqCst);
                seen.push((index, value));
                std::thread::sleep(std::time::Duration::from_micros(100));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen, (0..50).map(|i| (i, i * 10)).collect::<Vec<_>>());
        assert!(
            peak.load(Ordering::SeqCst) <= 6,
            "peak {}",
            peak.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn ordered_chunks_reports_the_first_producer_error_and_stops() {
        let produced = AtomicUsize::new(0);
        let mut consumed = 0;
        let error = ordered_chunks(
            1000,
            4,
            |index| {
                produced.fetch_add(1, Ordering::SeqCst);
                if index == 5 {
                    Err(format!("chunk {index} failed"))
                } else {
                    Ok(index)
                }
            },
            |_, _| {
                consumed += 1;
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error, "chunk 5 failed");
        assert!(consumed <= 5);
        assert!(produced.load(Ordering::SeqCst) < 1000);
    }

    #[test]
    fn nested_ordered_chunks_runs_serially_on_a_pool_thread_and_completes() {
        let before = SERIAL_FALLBACKS.load(Ordering::SeqCst);
        let mut outer = Vec::new();
        ordered_chunks(
            40,
            4,
            |index| {
                // A producer that itself uses the ordered pipeline: it must
                // not park the pool thread waiting for the pool.
                let mut inner = Vec::new();
                ordered_chunks(
                    5,
                    4,
                    |j| Ok::<_, ()>(j * 10),
                    |_, v| {
                        inner.push(v);
                        Ok(())
                    },
                )?;
                Ok::<_, ()>((index, inner))
            },
            |_, value| {
                outer.push(value);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(outer.len(), 40);
        for (index, (seen, inner)) in outer.iter().enumerate() {
            assert_eq!(*seen, index);
            assert_eq!(*inner, vec![0, 10, 20, 30, 40]);
        }
        assert!(SERIAL_FALLBACKS.load(Ordering::SeqCst) >= before + 40);
    }

    #[test]
    fn consumer_panic_unwinds_only_after_every_submitted_job_finished() {
        for round in 0..20 {
            let started = AtomicUsize::new(0);
            let finished = AtomicUsize::new(0);
            // A borrowed canary the jobs read; it must be intact whenever a
            // job runs, i.e. no job may run after this frame is gone.
            let canary = vec![0xA5u8; 4096];
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ordered_chunks(
                    64,
                    8,
                    |index| {
                        started.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_micros(150));
                        assert!(canary.iter().all(|byte| *byte == 0xA5));
                        finished.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, ()>(index)
                    },
                    |index, _| {
                        if index == round % 3 {
                            panic!("injected consumer panic");
                        }
                        Ok(())
                    },
                )
            }));
            assert_eq!(
                result.unwrap_err().downcast_ref::<&str>().copied(),
                Some("injected consumer panic")
            );
            assert_eq!(
                started.load(Ordering::SeqCst),
                finished.load(Ordering::SeqCst),
                "round {round}: a job was still running after the unwind"
            );
            let settled = finished.load(Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(5));
            assert_eq!(finished.load(Ordering::SeqCst), settled);
        }
        let mut seen = Vec::new();
        ordered_chunks(
            16,
            4,
            |i| Ok::<_, ()>(i * 3),
            |i, v| {
                seen.push((i, v));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen, (0..16).map(|i| (i, i * 3)).collect::<Vec<_>>());
    }

    #[test]
    fn builder_threads_are_fixed_once_the_pool_runs() {
        ordered_chunks(3, 2, Ok::<_, ()>, |_, _| Ok(())).unwrap();
        let running = builder_threads();
        assert!(running >= 1);
        assert_eq!(configure_builder_threads(running), Ok(()));
        assert_eq!(
            configure_builder_threads(running + 1),
            Err(BuilderThreadsFixed { threads: running })
        );
        // The detected parallelism follows the running pool: one worker per
        // pool thread (at most eight in flight), which is the serial path on
        // a host whose pool has a single thread.
        let detected = Parallelism::detect();
        assert_eq!(detected.workers(), running.min(Parallelism::MAX_WORKERS));
        assert_eq!(detected.is_serial(), running == 1);
    }

    #[test]
    fn ordered_chunks_reraises_a_producer_panic_after_draining() {
        let finished = AtomicUsize::new(0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ordered_chunks(
                40,
                4,
                |index| {
                    if index == 2 {
                        panic!("injected producer panic");
                    }
                    std::thread::sleep(std::time::Duration::from_micros(200));
                    finished.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>(index)
                },
                |_, _| Ok(()),
            )
        }));
        let payload = result.unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&str>().copied(),
            Some("injected producer panic")
        );
        // Every job submitted before the panic ran to completion before the
        // panic reached this thread; none is still running now.
        let settled = finished.load(Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(finished.load(Ordering::SeqCst), settled);
        assert!(settled < 40);
    }

    #[test]
    fn ordered_chunks_reports_a_consumer_error_and_stops() {
        let produced = AtomicUsize::new(0);
        let error = ordered_chunks(
            1000,
            4,
            |index| {
                produced.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(index)
            },
            |index, _| {
                if index == 3 {
                    Err("sink closed".to_string())
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert_eq!(error, "sink closed");
        assert!(produced.load(Ordering::SeqCst) < 1000);
    }

    #[test]
    fn frames_through_a_hasher_thread_give_the_serial_digest_with_a_bounded_pool() {
        let items = rows(300);
        let expected = Sha256::digest(serde_json::to_vec(&items).unwrap());
        let pool = BufferPool::new();
        let actual = std::thread::scope(|scope| {
            let (send, hasher) = spawn_hasher(scope, &pool, 2);
            write_array_frames(&items, Parallelism::new(3, 7), &pool, |frame| {
                send.send(frame).map_err(|_| stopped("hasher"))
            })
            .unwrap();
            drop(send);
            hasher.join().unwrap().finalize()
        });
        assert_eq!(actual, expected);
        let pooled = pool.0.lock().unwrap().len();
        assert!(pooled <= 3 * 2 + 2 + 2, "pooled buffers {pooled}");
    }

    #[test]
    fn write_array_propagates_writer_failures() {
        struct Failed;
        impl Write for Failed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("injected writer failure"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let error = write_array(&mut Failed, &rows(20), Parallelism::new(2, 3)).unwrap_err();
        assert!(error.is_io());
    }

    #[derive(Serialize)]
    struct Outer {
        first: u128,
        #[serde(skip_serializing_if = "Option::is_none")]
        missing: Option<u8>,
        rows: Vec<Row>,
        last: String,
    }

    #[test]
    fn struct_splice_matches_serde_json_and_continues_objects() {
        let outer = Outer {
            first: u128::MAX,
            missing: None,
            rows: rows(30),
            last: "tail \"quoted\"".into(),
        };
        let expected = serde_json::to_vec(&outer).unwrap();
        let mut actual = Vec::new();
        outer
            .serialize(StructSplice::object(&mut actual, "rows", |writer| {
                write_array(writer, &outer.rows, Parallelism::new(3, 4))
            }))
            .unwrap();
        assert_eq!(actual, expected);
        let mut continued = Vec::new();
        outer
            .serialize(StructSplice::continuing(&mut continued, "rows", |writer| {
                write_array(writer, &outer.rows, Parallelism::new(3, 4))
            }))
            .unwrap();
        assert_eq!(continued[0], b',');
        assert_eq!(&continued[1..], &expected[1..]);
    }

    #[test]
    fn struct_splice_refuses_non_struct_values() {
        let mut out = Vec::new();
        let error = 7u8
            .serialize(StructSplice::object(&mut out, "rows", |_| Ok(())))
            .unwrap_err();
        assert!(error.to_string().contains("not a struct"));
        let error = vec![1u8]
            .serialize(StructSplice::object(&mut out, "rows", |_| Ok(())))
            .unwrap_err();
        assert!(error.to_string().contains("not a struct"));
    }
}

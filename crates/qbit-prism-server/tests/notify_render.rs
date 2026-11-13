//! `mining.notify` rendered once per published work (#861): the bytes on the
//! wire are exactly what each session serialised for itself before, and what
//! that saves for a fan-out.
use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};
use qbit_prism_server::codec::{Job, NotifyBodies};
use serde_json::json;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
    time::{Duration, Instant},
};

thread_local! {
    /// Allocations and bytes allocated on this thread while counting.
    static COUNTING: Cell<Option<(u64, u64)>> = const { Cell::new(None) };
}

struct CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn count(bytes: usize) {
    // `try_with`: the allocator also runs while thread-locals are torn down.
    let _ = COUNTING.try_with(|counting| {
        if let Some((calls, total)) = counting.get() {
            counting.set(Some((calls + 1, total + bytes as u64)));
        }
    });
}

/// What `action` allocated on this thread: calls, and bytes requested.
fn allocations(action: impl FnOnce()) -> (u64, u64) {
    COUNTING.with(|counting| counting.set(Some((0, 0))));
    action();
    COUNTING.with(|counting| counting.take().unwrap())
}

/// The line a session wrote before #861: its own `notify()` value,
/// serialised, and the newline `write_json` appends.
fn reference_line(job: &Job) -> Vec<u8> {
    let mut line = serde_json::to_vec(&job.notify()).unwrap();
    line.push(b'\n');
    line
}

/// Shared work as a publication holds it: a coinbase paying `recipients`
/// addresses and a merkle branch `depth` levels deep.
fn published_work(recipients: usize, depth: usize) -> Job {
    let entitlements = (0..recipients)
        .map(|index| WeightedEntitlement {
            recipient_id: format!("recipient-{index:05}"),
            order_key: format!("recipient-{index:05}"),
            p2mr_program_hex: format!("{:064x}", index + 1),
            weight: 1 + index as u128 % 7,
        })
        .collect();
    let manifest = build_manifest(CoinbaseBuildRequest {
        block_height: 900_001,
        coinbase_value_sats: 312_500_000,
        entitlements,
        witness_nonce_hex: None,
        witness_merkle_leaves_hex: vec![],
        coinbase_script_sig_suffix_hex: Some(format!("505249534d{}", "00".repeat(12))),
        pinned_first_output: None,
    })
    .unwrap();
    let template = json!({"version":0x20000000u32,"bits":"17034219","curtime":1_800_000_000u32,
        "previousblockhash":"00000000000000000001a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f7",
        "transactions":[]});
    let mut job = Job::from_manifest(
        "shared".into(),
        &template,
        &manifest,
        "00000000",
        8,
        1.0,
        0.0,
        true,
    )
    .unwrap();
    job.merkle_branch = (0..depth)
        .map(|level| qbit_prism_server::codec::double_sha256(&[level as u8]))
        .collect();
    job
}

/// One session's job of that work, as `Coordinator::build_job` stamps it.
fn session_job(work: &Job, job_id: String, session: u32, clean_jobs: bool) -> Job {
    let mut job = work
        .reassign(job_id, &format!("{session:08x}"), 1024.0, 0.0)
        .unwrap();
    job.clean_jobs = clean_jobs;
    job
}

/// A job ID as the coordinator issues one: `PRISM_INSTANCE_ID` (up to 128
/// characters, any but controls) and a UUID.
fn issued_id(instance: &str, session: u32) -> String {
    format!(
        "{instance}-{:032x}",
        u128::from(session) * 0x9e37_79b9_7f4a_7c15
    )
}

#[test]
fn every_session_gets_exactly_the_bytes_it_serialised_for_itself() {
    let longest = "p".repeat(128);
    // Characters a PRISM_INSTANCE_ID may hold (no controls) that JSON
    // escapes or encodes in more than one byte, padded to its 128 bytes.
    let mut awkward = String::from("q\"uote\\back/slash é 漢 🦀 \u{2028}\u{fffd}");
    awkward.push_str(&"x".repeat(128 - awkward.len()));
    let instances = ["frontend-a", longest.as_str(), awkward.as_str()];
    for (recipients, depth) in [(1, 0), (130, 0), (130, 12), (2_600, 12), (2_600, 0)] {
        let work = published_work(recipients, depth);
        assert_eq!(work.merkle_branch.len(), depth);
        let bodies = NotifyBodies::default();
        for (session, instance) in instances.iter().enumerate() {
            for clean_jobs in [true, false] {
                let id = issued_id(instance, session as u32);
                // An announced ID is at most 161 bytes; longer ones, up to
                // the 256 bytes `resume_job` accepts, render the same way.
                for job_id in [id.clone(), format!("{id}{}", "r".repeat(256 - id.len()))] {
                    let job = session_job(&work, job_id, session as u32, clean_jobs);
                    assert_eq!(
                        String::from_utf8(bodies.line(&job)).unwrap(),
                        String::from_utf8(reference_line(&job)).unwrap(),
                        "{recipients} recipients, depth {depth}, clean {clean_jobs}"
                    );
                }
            }
        }
    }
}

/// The line for `job`, checked against the old path, and how many
/// allocations producing it took.
fn line_allocations(bodies: &NotifyBodies, job: &Job) -> u64 {
    let mut line = Vec::new();
    let (calls, _) = allocations(|| line = bodies.line(job));
    assert_eq!(line, reference_line(job));
    calls
}

#[test]
fn a_body_belongs_to_the_work_it_was_rendered_from() {
    let bodies = NotifyBodies::default();
    let work = published_work(130, 3);
    let job = session_job(&work, issued_id("frontend-a", 1), 1, true);
    // The first session of a work renders its body; every other session of
    // that work reuses it, so its line is its one allocation.
    assert!(line_allocations(&bodies, &job) > 1);
    let sibling = session_job(&work, issued_id("frontend-a", 2), 2, false);
    assert_eq!(
        line_allocations(&bodies, &sibling),
        1,
        "the body was rendered again"
    );
    // Equal coinbase text in another allocation is other work, and so is a
    // change to any field the body holds: each renders its own body, never
    // the kept one with this job's ID spliced in.
    let mut rebuilt = job.clone();
    rebuilt.coinb2 = job.coinb2.to_string().into();
    let mut variants = vec![rebuilt];
    for change in 0..6 {
        let mut other = job.clone();
        match change {
            0 => other.coinb1 = format!("{}00", job.coinb1).into(),
            1 => other.coinb2 = format!("{}00", job.coinb2).into(),
            2 => other.prevhash = "11".repeat(32),
            3 => other.merkle_branch.push([7; 32]),
            4 => other.version ^= 0x2000,
            5 => other.nbits ^= 1,
            _ => unreachable!(),
        }
        variants.push(other);
    }
    let mut other = job.clone();
    other.ntime += 1;
    variants.push(other);
    for other in &variants {
        assert!(line_allocations(&bodies, other) > 1, "{other:?}");
        // Kept now, beside this job's: each of them is reused.
        assert_eq!(line_allocations(&bodies, other), 1, "{other:?}");
        assert_eq!(line_allocations(&bodies, &job), 1);
    }
    // More works than are kept, then the first again: rendered anew.
    let works: Vec<_> = (0..12)
        .map(|depth| session_job(&published_work(1, depth), issued_id("f", 2), 2, false))
        .collect();
    for job in &works {
        line_allocations(&bodies, job);
    }
    assert!(line_allocations(&bodies, &job) > 1);
}

/// Run `fan_out` `reps` times; the fastest and median wall time, and what one
/// fan-out allocated.
fn measure(reps: usize, mut fan_out: impl FnMut()) -> (Duration, Duration, (u64, u64)) {
    let allocated = allocations(&mut fan_out);
    let mut times: Vec<_> = (0..reps)
        .map(|_| {
            let started = Instant::now();
            fan_out();
            started.elapsed()
        })
        .collect();
    times.sort();
    (times[0], times[reps / 2], allocated)
}

/// Before/after for one new tip's fan-out to 2,000 sessions: build and
/// serialise each session's `mining.notify` (#861). Single-threaded: under a
/// second with the optimisation flags in tests/perf/notify_render_fan_out.md,
/// about 25 s without. Run with
/// `cargo test -p qbit-prism-server --test notify_render -- --ignored --nocapture`.
#[test]
#[ignore = "measurement; prints a table"]
fn fan_out_cost_before_and_after() {
    const SESSIONS: u32 = 2_000;
    println!("| coinbase | message | before: fastest / median | after: fastest / median | before: allocations, bytes | after: allocations, bytes |");
    println!("| --- | --- | --- | --- | --- | --- |");
    for recipients in [130, 2_600] {
        let work = published_work(recipients, 12);
        let jobs: Vec<_> = (0..SESSIONS)
            .map(|session| {
                session_job(
                    &work,
                    issued_id("frontend-a", session),
                    session,
                    session % 2 == 0,
                )
            })
            .collect();
        let reps = if recipients > 1_000 { 3 } else { 9 };
        let before = measure(reps, || {
            for job in &jobs {
                black_box(reference_line(black_box(job)));
            }
        });
        // A fresh cache per fan-out: the first session renders the body.
        let after = measure(reps, || {
            let bodies = NotifyBodies::default();
            for job in &jobs {
                black_box(bodies.line(black_box(job)));
            }
        });
        println!(
            "| {recipients} outputs | {} bytes | {:.1} / {:.1} ms | {:.1} / {:.1} ms | {}, {:.1} MB | {}, {:.1} MB |",
            reference_line(&jobs[0]).len(),
            before.0.as_secs_f64() * 1e3,
            before.1.as_secs_f64() * 1e3,
            after.0.as_secs_f64() * 1e3,
            after.1.as_secs_f64() * 1e3,
            before.2 .0,
            before.2 .1 as f64 / 1e6,
            after.2 .0,
            after.2 .1 as f64 / 1e6,
        );
    }
}

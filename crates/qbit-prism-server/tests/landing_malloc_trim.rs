//! #600: a landing frontend returns the landing's freed heap to the kernel.
//!
//! Landing a block rebuilds its payout window and audit, a burst of many small
//! allocations that glibc keeps in its arenas once they are freed, so without
//! a trim the landing frontend's resident floor ratchets up by most of each
//! burst. This lands blocks over a production-shaped window through the real
//! offer path and holds the resident floor after each landing's trim well
//! below the landings' resident burst.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{coordinator::Coordinator, metrics::Metrics, stratum::MiningBackend};
use qbit_prism_test_gate as gate;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{run, window_fixture, Fixture, DIFFICULTY};

const TRIMS: &str = "qbit_prism_landing_malloc_trim_seconds_count";
const RELEASED: &str = "qbit_prism_landing_malloc_trim_released_bytes_total";
const RESIDENT: &str = "qbit_prism_landing_malloc_trim_resident_bytes";
/// Window shares: enough that a landing's burst (about 70 MiB in a debug
/// build) stands well clear of the fixture's own noise, few enough that a
/// debug landing takes about 4 s.
const WINDOW_SHARES: u64 = 20_000;
const LANDINGS: u32 = 3;

/// Measured on a debug build (#600), the whole test binary on 2 and on 18
/// cores: the resident floor after each landing above the pre-landing floor,
/// as a fraction of the burst (the resident peak above that floor, 80-120
/// MiB over the three landings):
///
/// | after landing | 1 | 2 | 3 |
/// |---|---|---|---|
/// | trim on (12 runs) | 0.03-0.11 | 0.2-0.33 | 0.24-0.46 |
/// | trim off, no `malloc_trim` call, or no follow-up (10 runs) | 0.44-0.58 | 0.71-0.84 | 0.82-1.0 |
///
/// With the trim the floor keeps only the frontend's live caches; without it
/// glibc keeps most of each burst and the floor climbs to the peak. The bound
/// sits midway between the worst landing of each. Trimming before the window
/// is released is caught by `OffRuntime`'s own test, not here: that trim
/// still returns the rest of the landing's garbage.
const FLOOR_BURST_FRACTION: f64 = 0.65;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_landing_trim_returns_the_landing_frontend_to_its_resident_floor() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            window_fixture::WindowPlan::new(WINDOW_SHARES)?
                .load(f.pool(), "trim-fixture")
                .await?;
            f.a.refresh_once().await?;
            // The floor before any landing, with glibc's free pages returned.
            qbit_prism_server::memory::trim();
            let baseline = resident_bytes()?;
            let peak = PeakSampler::start(baseline);
            let mut floors = Vec::new();
            for landing in 1..=LANDINGS {
                land(f, &f.a).await?;
                let trimmed = wait_trims(&f.a.metrics, f64::from(landing)).await;
                floors.push((resident_bytes()?, trimmed));
            }
            let peak = peak.stop();
            let burst = peak.saturating_sub(baseline);
            let mib = |bytes: u64| (bytes as f64 / 1_048_576.).round();
            let report = format!(
                "baseline {} MiB, peak {} MiB, floor after each landing (MiB, trimmed): {:?}",
                mib(baseline),
                mib(peak),
                floors
                    .iter()
                    .map(|(floor, trimmed)| (mib(*floor), *trimmed))
                    .collect::<Vec<_>>()
            );
            eprintln!("{report}");
            ensure!(
                burst > 32 << 20,
                "the landings made no resident burst: {report}"
            );
            for (floor, _) in &floors {
                ensure!(
                    floor.saturating_sub(baseline) as f64 <= FLOOR_BURST_FRACTION * burst as f64,
                    "the landing frontend's resident floor kept more than {FLOOR_BURST_FRACTION} of the landing burst: {report}"
                );
            }
            // One trim per landing, each recorded.
            ensure!(
                floors.iter().all(|(_, trimmed)| *trimmed),
                "a landing was not trimmed: {report}"
            );
            ensure!(
                sample(&f.a.metrics, TRIMS) == f64::from(LANDINGS),
                "{report}"
            );
            ensure!(sample(&f.a.metrics, RELEASED) > 0., "{report}");
            ensure!(sample(&f.a.metrics, RESIDENT) > 0., "{report}");
            // The other frontend landed nothing and trimmed nothing.
            ensure!(sample(&f.b.metrics, TRIMS) == 0., "{report}");
            ensure!(sample(&f.b.metrics, RESIDENT) == -1., "{report}");
            Ok(())
        })
    })
    .await
}

/// Land one block through `frontend`'s offer path.
async fn land(f: &Fixture, frontend: &Arc<Coordinator>) -> Result<()> {
    f.node.accept_blocks();
    frontend.refresh_once().await?;
    let worker = frontend.authorize("trim.rig").await?;
    let job = frontend
        .build_job(&worker, "1a2b3c4d", DIFFICULTY, 0.)
        .await?;
    frontend
        .persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
        .await?;
    for nonce in 0..10_000u32 {
        let proof = job.wire.assemble_submission(
            &"00".repeat(job.wire.extranonce2_size),
            &format!("{:08x}", job.wire.ntime),
            &format!("{nonce:08x}"),
            None,
            0,
        )?;
        if proof.block_pass {
            frontend.submit(&worker, &job, proof, false.into()).await?;
            let claim = frontend
                .ledger
                .claim_candidate(60)
                .await?
                .context("candidate missing")?;
            frontend.process_candidate(&claim).await?;
            let state: String = sqlx::query_scalar(
                "SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&claim.candidate.block_hash)
            .fetch_one(f.pool())
            .await?;
            ensure!(state == "submitted", "offer did not land: {state}");
            return Ok(());
        }
    }
    anyhow::bail!("no block proof in bounded fixture search")
}

/// Wait for the `count`th post-landing trim; false when it never comes, as
/// with the trim turned off.
async fn wait_trims(metrics: &Metrics, count: f64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if sample(metrics, TRIMS) >= count {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// An unlabelled sample, zero while the family has none.
fn sample(metrics: &Metrics, name: &str) -> f64 {
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .map_or(0., |value| value.parse().unwrap_or(f64::NAN))
}

/// The process's resident set, as the frontend's process collector reads it.
fn resident_bytes() -> Result<u64> {
    qbit_prism_server::metrics::collectors::resident_bytes(std::path::Path::new("/proc/self"))
}

/// The highest resident set seen every 5 ms between `start` and `stop`: the
/// landings' burst, without resetting the kernel's high-water mark.
struct PeakSampler {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<u64>,
}

impl PeakSampler {
    fn start(floor: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut peak = floor;
            while !stopped.load(Ordering::Relaxed) {
                peak = peak.max(resident_bytes().unwrap_or(0));
                std::thread::sleep(Duration::from_millis(5));
            }
            peak
        });
        Self { stop, thread }
    }

    fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().expect("peak sampler panicked")
    }
}

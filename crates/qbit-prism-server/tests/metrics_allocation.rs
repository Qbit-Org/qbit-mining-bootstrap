//! Scoped allocation counts cover only event hooks, excluding construction,
//! rendering, and test/thread setup. Run with `cargo test --release --locked
//! -p qbit-prism-server --test metrics_allocation` as well as the debug suite.
use qbit_prism_server::metrics::{AckResult, LockKind, Metrics, Outcome, RejectReason};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
    sync::{Arc, Barrier},
    time::Duration,
};

struct CountingAllocator;
thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}
fn count_allocation() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
        }
    });
}
// SAFETY: All allocation/deallocation operations are forwarded unchanged to
// System. The thread-local counter neither allocates nor shares mutable state.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn allocations(action: impl FnOnce()) -> usize {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATIONS.with(|count| count.set(None));
        }
    }
    ALLOCATIONS.with(|count| {
        assert!(count.get().is_none(), "allocation scopes must not nest");
        count.set(Some(0));
    });
    let _reset = Reset;
    action();
    ALLOCATIONS.with(|count| count.get().unwrap())
}

fn observe_all(metrics: &Metrics, elapsed: Duration) {
    for result in AckResult::ALL {
        metrics.observe_share_ack(*result, elapsed);
    }
    for result in Outcome::ALL {
        metrics.observe_pool_acquire(*result, elapsed);
        for lock in LockKind::ALL {
            metrics.observe_advisory_lock(*lock, *result, elapsed);
        }
    }
    metrics.observe_first_offer(elapsed);
}

fn increment_all(metrics: &Metrics) {
    for reason in RejectReason::ALL {
        metrics.record_rejection(*reason);
    }
    metrics.record_grace_credit();
}

#[test]
fn every_closed_event_key_allocates_nothing_on_first_and_repeated_calls() {
    assert!(
        allocations(|| {
            black_box(vec![0_u8; black_box(32)]);
        }) > 0
    );
    let metrics = Metrics::default();
    // macOS initializes std::sync::Mutex storage lazily. Exercise locking and
    // rendering during setup without observing any event or activating any key.
    black_box(metrics.render());
    for iterations in [1, 10_000] {
        let count = allocations(|| {
            for _ in 0..iterations {
                observe_all(black_box(&metrics), black_box(Duration::from_millis(125)));
                increment_all(black_box(&metrics));
            }
        });
        assert_eq!(count, 0, "event allocations in {iterations} rounds");
    }
}

fn sample(body: &str, key: &str) -> f64 {
    let prefix = format!("{key} ");
    let mut values = body.lines().filter_map(|line| line.strip_prefix(&prefix));
    let value = values.next().unwrap_or_else(|| panic!("missing {key}"));
    assert!(values.next().is_none(), "duplicate {key}");
    value.parse().unwrap()
}

#[test]
fn concurrent_events_preserve_every_count_and_sum_without_allocating() {
    let metrics = Arc::new(Metrics::default());
    black_box(metrics.render());
    let barrier = Arc::new(Barrier::new(5));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let metrics = metrics.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                allocations(|| {
                    for _ in 0..512 {
                        observe_all(&metrics, Duration::from_millis(125));
                        increment_all(&metrics);
                    }
                })
            })
        })
        .collect();
    barrier.wait();
    // Rendering clones the registry under the same mutex as an observation.
    // Every visible histogram must remain internally consistent during writes.
    for _ in 0..32 {
        let body = metrics.render();
        for line in body.lines().filter(|line| line.contains("_count")) {
            let (key, count) = line.rsplit_once(' ').unwrap();
            let count: f64 = count.parse().unwrap();
            assert_eq!(sample(&body, &key.replace("_count", "_sum")), count * 0.125);
        }
    }
    for thread in threads {
        assert_eq!(thread.join().unwrap(), 0);
    }
    let body = metrics.render();
    let histogram_count = body.lines().filter(|line| line.contains("_count")).count();
    assert_eq!(histogram_count, 11);
    for line in body.lines().filter(|line| line.contains("_count")) {
        let (key, count) = line.rsplit_once(' ').unwrap();
        assert_eq!(count, "2048");
        assert_eq!(sample(&body, &key.replace("_count", "_sum")), 256.);
    }
    for reason in RejectReason::ALL {
        assert_eq!(
            sample(
                &body,
                &format!(
                    "qbit_prism_rejections_total{{reason_id=\"{}\"}}",
                    reason.as_str()
                )
            ),
            2048.
        );
    }
    for (name, count) in [
        ("stale", 4096.),
        ("duplicate", 2048.),
        ("low_difficulty", 2048.),
        ("grace_credited", 2048.),
    ] {
        assert_eq!(
            sample(&body, &format!("qbit_prism_{name}_shares_total")),
            count
        );
    }
}

#[test]
fn event_exposition_matches_the_pre_allocation_change_byte_for_byte() {
    let metrics = Metrics::default();
    for millis in [0, 5, 10, 25, 125, 5000, 5001, 15000, 35000] {
        observe_all(&metrics, Duration::from_millis(millis));
    }
    increment_all(&metrics);
    let body = metrics.render();
    // Captured with the registry/events/labels and initialization from 2914a629,
    // and refreshed at e13051cf, which added the `ledger-outcome-unknown` reason.
    // This pins HELP, TYPE, ordering, escaping, cumulative buckets, count and sum,
    // so adding a value to a closed label set has to be recorded here too.
    let expected = include_str!("fixtures/metric_events.prom");
    let families: Vec<_> = expected
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .map(|line| line.split_once(' ').unwrap().0)
        .collect();
    let mut actual = String::new();
    let mut include = false;
    for line in body.lines() {
        if let Some(help) = line.strip_prefix("# HELP ") {
            include = families.contains(&help.split_once(' ').unwrap().0);
        }
        if include {
            actual.push_str(line);
            actual.push('\n');
        }
    }
    assert_eq!(actual, expected);
}

#[test]
fn reserving_lazy_histograms_does_not_invent_observations() {
    let metrics = Metrics::default();
    let initial = metrics.render();
    for name in [
        "block_submit_seconds",
        "database_advisory_lock_wait_seconds",
    ] {
        assert!(initial.contains(&format!("# TYPE qbit_prism_{name} histogram\n")));
        assert!(!initial
            .lines()
            .any(|line| line.starts_with(&format!("qbit_prism_{name}"))));
    }
    metrics.observe_advisory_lock(LockKind::Order, Outcome::Failure, Duration::ZERO);
    let body = metrics.render();
    assert_eq!(sample(&body, "qbit_prism_database_advisory_lock_wait_seconds_count{lock=\"order\",result=\"failure\"}"), 1.);
    assert_eq!(
        sample(
            &body,
            "qbit_prism_database_advisory_lock_wait_seconds_sum{lock=\"order\",result=\"failure\"}"
        ),
        0.
    );
    assert!(!body
        .lines()
        .any(|line| line.starts_with("qbit_prism_block_submit_seconds")));
    let lock_samples: Vec<_> = body
        .lines()
        .filter(|line| line.starts_with("qbit_prism_database_advisory_lock_wait_seconds"))
        .collect();
    assert_eq!(lock_samples.len(), 14);
    assert!(lock_samples
        .iter()
        .all(|line| line.contains("lock=\"order\",result=\"failure\"")));
}

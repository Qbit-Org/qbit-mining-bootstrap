//! Returning a block landing's freed heap to the operating system (#600).
//!
//! Landing a block rebuilds its payout window and audit: at a 400,000-share
//! window about 1.4 GB of short-lived allocations, millions of them small.
//! glibc keeps what they free in the arenas of the threads that did the work
//! and does not give it back, so a landing frontend's resident set ratchets to
//! a plateau of about 3.3 GB while its live heap returns to about 250 MiB after
//! every landing. `malloc_trim(0)` returns those free pages to the kernel.
//!
//! The frontend that landed trims once the rebuilt window is released, on the
//! blocking thread that released it: never on a runtime worker, and never
//! while it holds a lock or a database transaction. The trim still locks
//! each glibc arena in turn, so a thread allocating from an arena while it is
//! trimmed waits for it; `qbit_prism_landing_malloc_trim_seconds` bounds that
//! wait. Only glibc has
//! `malloc_trim`; on any other target the trim is a no-op that records
//! nothing. `PRISM_LANDING_MALLOC_TRIM_ENABLED=0` turns it off.
use crate::metrics::Metrics;
use anyhow::Result;
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

/// Whether this build can trim: glibc on Linux, the target the image and the
/// release binaries are built for.
pub const SUPPORTED: bool = cfg!(all(target_os = "linux", target_env = "gnu"));

/// `PRISM_LANDING_MALLOC_TRIM_ENABLED`: trim after each landing, default on.
pub fn landing_trim_from_env() -> Result<bool> {
    crate::config::flag("PRISM_LANDING_MALLOC_TRIM_ENABLED", true)
}

/// One `malloc_trim(0)`: how long it took, and the process's resident set
/// read just before and just after it (`None` when procfs could not be read).
#[derive(Clone, Copy, Debug)]
pub struct Trim {
    pub elapsed: Duration,
    pub resident_before: Option<u64>,
    pub resident_after: Option<u64>,
}

impl Trim {
    /// Resident bytes the trim returned, never negative. Other threads keep
    /// allocating while it runs, so this can understate the release.
    pub fn released(&self) -> Option<u64> {
        Some(self.resident_before?.saturating_sub(self.resident_after?))
    }
}

/// Return every free page glibc holds in any arena to the kernel. Blocking:
/// it locks each arena in turn. `None`, and no call, where glibc is absent.
pub fn trim() -> Option<Trim> {
    if !SUPPORTED {
        return None;
    }
    let resident_bytes =
        || crate::metrics::collectors::resident_bytes(Path::new("/proc/self")).ok();
    let resident_before = resident_bytes();
    let started = Instant::now();
    release();
    let elapsed = started.elapsed();
    Some(Trim {
        elapsed,
        resident_before,
        resident_after: resident_bytes(),
    })
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn release() {
    // SAFETY: malloc_trim takes no pointer and may be called from any thread;
    // its return value only says whether any memory was released.
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn release() {}

/// A frontend's post-landing trim, on unless the setting turned it off. A
/// frontend lands one candidate at a time, so its trims rarely overlap; when
/// one does, glibc takes the arenas in turn and the second finds little left.
pub struct LandingTrim {
    enabled: AtomicBool,
}

impl Default for LandingTrim {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(true),
        }
    }
}

impl LandingTrim {
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Trim once a landing has released its rebuilt window, recording each
    /// trim in `metrics`. Blocking: call it from a blocking thread, after the
    /// release, holding nothing.
    pub fn after_landing(&self, metrics: &Metrics) {
        if !self.enabled() {
            return;
        }
        if let Some(trim) = trim() {
            metrics.observe_landing_trim(&trim);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trims(metrics: &Metrics) -> f64 {
        metrics
            .render()
            .lines()
            .find_map(|line| line.strip_prefix("qbit_prism_landing_malloc_trim_seconds_count "))
            .map_or(0., |count| count.parse().unwrap())
    }

    #[test]
    fn only_glibc_linux_builds_can_trim() {
        assert_eq!(
            SUPPORTED,
            cfg!(target_os = "linux") && cfg!(target_env = "gnu")
        );
        // The unsupported build makes no call and reports no trim; the
        // supported one always reports what it did.
        assert_eq!(trim().is_some(), SUPPORTED);
    }

    #[test]
    fn a_landing_trims_on_glibc_when_enabled_and_records_nothing_otherwise() {
        let metrics = Metrics::default();
        let trimmer = LandingTrim::default();
        assert!(trimmer.enabled(), "the trim defaults to on");
        trimmer.after_landing(&metrics);
        assert_eq!(trims(&metrics), f64::from(u8::from(SUPPORTED)));
        trimmer.set_enabled(false);
        trimmer.after_landing(&metrics);
        assert_eq!(trims(&metrics), f64::from(u8::from(SUPPORTED)));
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn a_glibc_trim_reads_the_resident_set_around_the_call() {
        let trim = trim().expect("glibc trims");
        let before = trim.resident_before.expect("procfs resident set");
        let after = trim.resident_after.expect("procfs resident set");
        assert!(before > 0 && after > 0);
        assert_eq!(trim.released(), Some(before.saturating_sub(after)));
    }
}

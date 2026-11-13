//! Host contention over a run (#842): the `/proc/stat` and PSI parsers, the
//! window summaries, the tip annotation, the lock sampler's longest gap and
//! the gate's rows. Pure unit tests over synthetic readings; no database.

use qbit_prism_load::{
    contention::{self, CpuTimes, HostReading, PressureRead, PressureTotals},
    gate, measure,
    node::{TipChange, TipOrigin},
    run,
};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const STAT: &str = "cpu  100 5 50 1000 20 3 2 10 7 0\n\
                    cpu0 50 2 25 500 10 1 1 5 3 0\n\
                    intr 12345\n";

#[test]
fn proc_stat_reads_the_aggregate_line_and_leaves_absent_fields_unknown() {
    // guest and guest_nice are already inside user and nice, so the total
    // stops at steal.
    assert_eq!(
        contention::parse_proc_stat(STAT),
        Ok(CpuTimes {
            total: 100 + 5 + 50 + 1000 + 20 + 3 + 2 + 10,
            iowait: Some(20),
            steal: Some(10),
        })
    );
    // A kernel whose line stops before steal: steal unknown, not 0.
    assert_eq!(
        contention::parse_proc_stat("cpu 1 2 3 4 5 6 7\n"),
        Ok(CpuTimes {
            total: 28,
            iowait: Some(5),
            steal: None,
        })
    );
    assert_eq!(
        contention::parse_proc_stat("cpu 1 2 3 4\n"),
        Ok(CpuTimes {
            total: 10,
            iowait: None,
            steal: None,
        })
    );
    // Only the aggregate line counts; a per-CPU line is not it.
    assert!(contention::parse_proc_stat("cpu0 1 2 3 4\n")
        .unwrap_err()
        .contains("no aggregate cpu line"));
    assert!(contention::parse_proc_stat("cpu 1 2 x 4\n")
        .unwrap_err()
        .contains("non-numeric"));
    assert!(contention::parse_proc_stat("cpu 1 2 3\n")
        .unwrap_err()
        .contains("3 fields"));
}

#[test]
fn pressure_files_read_their_totals_and_a_missing_full_line_stays_unknown() {
    let io = "some avg10=1.00 avg60=0.50 avg300=0.10 total=123456\n\
              full avg10=0.50 avg60=0.20 avg300=0.05 total=65432\n";
    assert_eq!(
        contention::parse_pressure(io),
        Ok(PressureTotals {
            some_us: 123_456,
            full_us: Some(65_432),
        })
    );
    // CPU pressure before 5.13 has no full line.
    assert_eq!(
        contention::parse_pressure("some avg10=0.00 avg60=0.00 avg300=0.00 total=9\n"),
        Ok(PressureTotals {
            some_us: 9,
            full_us: None,
        })
    );
    assert!(contention::parse_pressure("full total=1\n")
        .unwrap_err()
        .contains("no some line"));
    assert!(contention::parse_pressure("some avg10=0.00 total=x\n")
        .unwrap_err()
        .contains("not a number"));
    assert!(contention::parse_pressure("some avg10=0.00\n")
        .unwrap_err()
        .contains("no total= field"));
}

/// A host without `/proc/pressure` (no PSI) reads its CPU counters and
/// reports each pressure source unknown with the read error.
#[test]
fn a_host_without_pressure_files_reads_unknown_pressure_not_zero() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("stat"), STAT).unwrap();
    let reading = contention::read_host_at(root.path());
    assert_eq!(reading.cpu.as_ref().unwrap().steal, Some(10));
    for pressure in [
        &reading.cpu_pressure,
        &reading.io_pressure,
        &reading.memory_pressure,
    ] {
        assert!(pressure.as_ref().unwrap_err().contains("pressure/"));
    }
    let block = contention::run_block(std::slice::from_ref(&reading), Duration::from_secs(1));
    assert_eq!(block["sources"]["proc_stat"], json!("read at start"));
    assert!(block["sources"]["pressure_io"]
        .as_str()
        .unwrap()
        .contains("pressure/io"));
    assert_eq!(block["steal_counter_at_start_ticks"].as_u64(), Some(10));

    let mut later = reading.clone();
    later.at = reading.at + Duration::from_secs(1);
    later.cpu = Ok(CpuTimes {
        total: 1190 + 800,
        iowait: Some(24),
        steal: Some(10),
    });
    let readings = [reading.clone(), later];
    let summary = contention::summarize(&readings, reading.at, reading.at);
    assert_eq!(summary.intervals, 1);
    assert_eq!(summary.steal.percent, Some(0.0), "a real zero stays zero");
    for share in [&summary.cpu_some, &summary.io_some, &summary.memory_full] {
        assert_eq!(share.percent, None);
        assert!(share
            .unavailable_reason
            .as_deref()
            .unwrap()
            .contains("pressure/"));
    }
    let json = serde_json::to_value(&summary).unwrap();
    assert!(json["io_some"]["percent"].is_null(), "{json}");
}

fn reading(
    base: Instant,
    millis: u64,
    total: u64,
    steal: Option<u64>,
    io_some: u64,
    io_full: Option<u64>,
) -> HostReading {
    let at = base + Duration::from_millis(millis);
    let pressure = |some_us, full_us| {
        Ok(PressureRead {
            before: at,
            after: at,
            totals: PressureTotals { some_us, full_us },
        })
    };
    HostReading {
        at,
        cpu: Ok(CpuTimes {
            total,
            iowait: Some(total / 100),
            steal,
        }),
        cpu_pressure: pressure(io_some * 2, None),
        io_pressure: pressure(io_some, io_full),
        memory_pressure: pressure(0, Some(0)),
    }
}

#[test]
fn a_window_reports_its_share_over_every_interval_and_its_worst_interval() {
    let base = Instant::now();
    // Three reads a second apart: 800 ticks of CPU time per interval, the
    // first with 80 of them stolen, the second with none; io stalled 100 ms
    // and then 300 ms of each second.
    let readings = [
        reading(base, 0, 10_000, Some(500), 1_000_000, Some(400_000)),
        reading(base, 1000, 10_800, Some(580), 1_100_000, Some(450_000)),
        reading(base, 2000, 11_600, Some(580), 1_400_000, Some(450_000)),
    ];
    let whole = contention::summarize(
        &readings,
        base + Duration::from_millis(200),
        base + Duration::from_millis(1500),
    );
    assert_eq!(whole.intervals, 2);
    assert_eq!(whole.covered_from_milliseconds, Some(-200.0));
    assert_eq!(whole.covered_to_milliseconds, Some(1800.0));
    assert_eq!(whole.steal.percent, Some(5.0));
    assert_eq!(whole.steal.max_interval_percent, Some(10.0));
    assert_eq!(whole.steal.measured_intervals, 2);
    assert!(whole.steal.unavailable_reason.is_none());
    let io = whole.io_some.percent.unwrap();
    assert!((io - 20.0).abs() < 1e-9, "{io}");
    assert!((whole.io_some.max_interval_percent.unwrap() - 30.0).abs() < 1e-9);
    assert!((whole.cpu_some.percent.unwrap() - 40.0).abs() < 1e-9);
    assert_eq!(whole.memory_some.percent, Some(0.0));

    // A window inside the second interval sees only it.
    let late = contention::summarize(
        &readings,
        base + Duration::from_millis(1200),
        base + Duration::from_millis(1300),
    );
    assert_eq!(late.intervals, 1);
    assert_eq!(late.steal.percent, Some(0.0));

    // A window no two reads span is unknown throughout, not quiet.
    let after = contention::summarize(
        &readings,
        base + Duration::from_millis(2500),
        base + Duration::from_millis(3000),
    );
    assert_eq!(after.intervals, 0);
    assert_eq!(after.covered_from_milliseconds, None);
    for share in [&after.steal, &after.iowait, &after.io_full] {
        assert_eq!(share.percent, None);
        assert_eq!(
            share.unavailable_reason.as_deref(),
            Some("no two reads span the window")
        );
    }
}

/// Codex on #854: a sampler preempted between reading `/proc/stat` and a
/// pressure file must not borrow the CPU reads' interval for the file. Here
/// the second read's `/proc/stat` came at 1 s but its io pressure only at
/// 6 s, after a 4 s stall; a window at 2-5 s lies inside the pressure
/// interval 0-6 s, which carries the stall, not the quiet one 6-7 s that the
/// CPU interval 1-7 s would have picked.
#[test]
fn each_pressure_interval_is_placed_by_its_own_reads() {
    let base = Instant::now();
    let at = |ms: u64| base + Duration::from_millis(ms);
    let read = |cpu_ms: u64, total: u64, io_ms: u64, io_some: u64| HostReading {
        at: at(cpu_ms),
        cpu: Ok(CpuTimes {
            total,
            iowait: Some(0),
            steal: Some(0),
        }),
        cpu_pressure: Err("no PSI here".into()),
        io_pressure: Ok(PressureRead {
            before: at(io_ms),
            after: at(io_ms),
            totals: PressureTotals {
                some_us: io_some,
                full_us: Some(0),
            },
        }),
        memory_pressure: Err("no PSI here".into()),
    };
    let readings = [
        read(0, 0, 0, 0),
        read(1000, 800, 6000, 4_000_000),
        read(7000, 5600, 7000, 4_000_000),
    ];
    let summary = contention::summarize(&readings, at(2000), at(5000));
    // The /proc/stat reads place one interval, 1-7 s.
    assert_eq!(summary.intervals, 1);
    assert_eq!(summary.steal.intervals, 1);
    // The io pressure's own interval over the window is 0-6 s: 4 s stalled
    // of 6.
    let io = &summary.io_some;
    assert_eq!(io.intervals, 1);
    assert!((io.percent.unwrap() - 400.0 / 6.0).abs() < 1e-9, "{io:?}");
    assert_eq!(io.covered_from_milliseconds, Some(-2000.0));
    assert_eq!(io.covered_to_milliseconds, Some(4000.0));
    // An unreadable file is placed by the /proc/stat reads and unknown.
    assert_eq!(summary.cpu_some.intervals, 1);
    assert_eq!(summary.cpu_some.percent, None);
    assert_eq!(
        summary.cpu_some.unavailable_reason.as_deref(),
        Some("no PSI here")
    );
}

/// Sixth Man on #854: the sampler can lose the CPU between reading a pressure
/// file and stamping the read. Here the first read of the io file was stamped
/// 900 ms after it began, and the host was stalled for the whole second up to
/// the next read. The time between the two stamps, 100 ms, would make that
/// 1000%. The two totals are at most the time from before the first read to
/// after the second apart, which is what the share divides by: 100%. The
/// third read was stamped a second late and its interval holds one stalled
/// second, so it reads 50% of the two seconds it may cover, not more.
#[test]
fn a_sampler_preempted_around_a_pressure_read_never_raises_its_share() {
    let base = Instant::now();
    let at = |ms: u64| base + Duration::from_millis(ms);
    let read = |cpu_ms: u64, io: (u64, u64), io_some: u64| HostReading {
        at: at(cpu_ms),
        cpu: Err("no /proc/stat here".into()),
        cpu_pressure: Err("no PSI here".into()),
        io_pressure: Ok(PressureRead {
            before: at(io.0),
            after: at(io.1),
            totals: PressureTotals {
                some_us: io_some,
                full_us: Some(io_some),
            },
        }),
        memory_pressure: Err("no PSI here".into()),
    };
    let readings = [
        read(0, (0, 900), 0),
        read(1000, (1000, 1000), 1_000_000),
        read(2000, (2000, 3000), 2_000_000),
    ];
    let first = contention::summarize(&readings[..2], at(0), at(1000));
    for share in [&first.io_some, &first.io_full] {
        assert_eq!(share.intervals, 1);
        assert_eq!(share.percent, Some(100.0), "{share:?}");
        assert_eq!(share.max_interval_percent, Some(100.0), "{share:?}");
        assert_eq!(share.covered_from_milliseconds, Some(0.0));
        assert_eq!(share.covered_to_milliseconds, Some(1000.0));
    }
    let second = contention::summarize(&readings[1..], at(1000), at(3000));
    let io = &second.io_some;
    assert_eq!(io.intervals, 1);
    assert_eq!(io.percent, Some(50.0), "{io:?}");
    assert_eq!(io.covered_from_milliseconds, Some(0.0));
    assert_eq!(io.covered_to_milliseconds, Some(2000.0));
}

/// A pressure file's read is stamped on both sides, so the two instants
/// bound the moment the kernel's totals were taken.
#[test]
fn a_pressure_read_is_stamped_before_and_after_the_file_is_read() {
    let during = std::sync::Mutex::new(None);
    let pause = || std::thread::sleep(Duration::from_millis(2));
    let reading = contention::read_host_with(|relative| {
        if relative != "pressure/io" {
            return Err(format!("{relative}: not here"));
        }
        pause();
        *during.lock().unwrap() = Some(Instant::now());
        pause();
        Ok("some avg10=0.00 avg60=0.00 avg300=0.00 total=5\n\
            full avg10=0.00 avg60=0.00 avg300=0.00 total=3\n"
            .to_owned())
    });
    let during = during.lock().unwrap().expect("the io file was read");
    let io = reading.io_pressure.expect("the io file parses");
    assert!(io.before < during && during < io.after, "{io:?}");
    assert_eq!(
        io.totals,
        PressureTotals {
            some_us: 5,
            full_us: Some(3)
        }
    );
    assert!(reading.cpu.unwrap_err().contains("stat: not here"));
    assert!(reading
        .cpu_pressure
        .unwrap_err()
        .contains("pressure/cpu: not here"));
}

#[test]
fn a_missing_field_or_a_counter_that_went_backwards_is_unknown_for_its_intervals() {
    let base = Instant::now();
    // No steal field and no io full line: both unknown, iowait measured.
    let readings = [
        reading(base, 0, 10_000, None, 0, None),
        reading(base, 1000, 10_800, None, 0, None),
    ];
    let summary = contention::summarize(&readings, base, base + Duration::from_secs(1));
    assert_eq!(summary.steal.percent, None);
    assert!(summary
        .steal
        .unavailable_reason
        .as_deref()
        .unwrap()
        .contains("no steal field"));
    assert_eq!(summary.io_full.percent, None);
    assert!(summary
        .io_full
        .unavailable_reason
        .as_deref()
        .unwrap()
        .contains("no full line"));
    assert!(summary.iowait.percent.is_some());

    // The steal counter going backwards in the second interval leaves that
    // interval out, and the figure says over how many it was measured.
    let readings = [
        reading(base, 0, 10_000, Some(500), 0, Some(0)),
        reading(base, 1000, 10_800, Some(540), 0, Some(0)),
        reading(base, 2000, 11_600, Some(10), 0, Some(0)),
    ];
    let summary = contention::summarize(&readings, base, base + Duration::from_secs(2));
    assert_eq!(summary.steal.percent, Some(5.0));
    assert_eq!(summary.steal.measured_intervals, 1);
    let reason = summary.steal.unavailable_reason.unwrap();
    assert!(
        reason == "measured over 1 of 2 intervals: the steal counter went backwards",
        "{reason}"
    );
}

fn change(hash: &str, monotonic: Instant) -> TipChange {
    TipChange {
        hash: hash.into(),
        height: 101,
        origin: TipOrigin::External,
        monotonic,
        wall: chrono::Utc::now(),
    }
}

#[test]
fn each_tip_gets_the_host_over_its_delivery_and_the_phase_it_fell_in() {
    let base = Instant::now();
    let readings: Vec<HostReading> = (0..6)
        .map(|i| {
            // Steal only in the fourth second.
            let steal = if i >= 4 { 400 } else { 0 };
            reading(base, i * 1000, 10_000 + i * 800, Some(steal), 0, Some(0))
        })
        .collect();
    let stamp = base + Duration::from_millis(2500);
    let mut tips = vec![
        json!({"tip": "aa", "all_sessions_milliseconds": 1200.0}),
        json!({"tip": "bb", "all_sessions_milliseconds": null,
               "latency_milliseconds": {"max": 300.0}}),
        json!({"tip": "unrecorded", "all_sessions_milliseconds": 100.0}),
        json!({"tip": "cc", "all_sessions_milliseconds": null,
               "latency_milliseconds": {"max": 100.0}, "replaced_after_milliseconds": 3000.0}),
    ];
    let phases = vec![("warm_up".to_owned(), base, base + Duration::from_secs(10))];
    run::annotate_tip_contention(
        Some(&mut tips),
        &[
            change("aa", stamp),
            change("bb", base + Duration::from_millis(500)),
            change("cc", base + Duration::from_millis(500)),
        ],
        &readings,
        &phases,
    );
    let aa = &tips[0]["host_contention"];
    // 2.5 s to 3.7 s: the third and fourth intervals, the fourth stolen.
    assert_eq!(aa["intervals"].as_u64(), Some(2), "{aa}");
    assert_eq!(aa["steal"]["max_interval_percent"].as_f64(), Some(50.0));
    assert_eq!(aa["steal"]["percent"].as_f64(), Some(25.0));
    assert_eq!(aa["phase"], json!("warm_up"));
    assert_eq!(
        aa["window"],
        json!("from the node's tip stamp to the slowest session's first usable job")
    );
    let bb = &tips[1]["host_contention"];
    assert!(bb["window"].as_str().unwrap().contains("never served"));
    assert_eq!(bb["steal"]["percent"].as_f64(), Some(0.0));
    // A tip the node did not record gets no block over a guessed window.
    assert!(tips[2].get("host_contention").is_none());
    // A tip some session never got runs to its replacement, so the stall
    // that starved them (the fourth second) is inside its window.
    let cc = &tips[3]["host_contention"];
    assert_eq!(cc["intervals"].as_u64(), Some(4), "{cc}");
    assert_eq!(cc["steal"]["max_interval_percent"].as_f64(), Some(50.0));
    assert!(cc["window"]
        .as_str()
        .unwrap()
        .starts_with("from the node's tip stamp to its replacement"));
    // No list (a run without churn) is fine.
    run::annotate_tip_contention(None, &[], &readings, &phases);
}

#[test]
fn the_lock_sampler_reports_its_longest_gap_and_when_it_ended() {
    let base = Instant::now();
    let server = chrono::Utc::now();
    let at = |ms: u64| base + Duration::from_millis(ms);
    let samples: Vec<measure::LockSample> = [0u64, 11, 36, 47]
        .iter()
        .enumerate()
        .map(|(i, ms)| measure::LockSample {
            monotonic: at(*ms),
            server_time: server + chrono::Duration::milliseconds(i as i64),
            rows: Vec::new(),
            query_millis: 0.5,
        })
        .collect();
    assert_eq!(
        measure::longest_sample_gap(&samples, at(0), at(47)),
        Some((25.0, server + chrono::Duration::milliseconds(2)))
    );
    // A gap that began before the window and ended inside it is the
    // window's: a stall straddling a phase's start is not lost.
    assert_eq!(
        measure::longest_sample_gap(&samples, at(30), at(40)).map(|(gap, _)| gap),
        Some(25.0)
    );
    assert_eq!(
        measure::longest_sample_gap(&samples, at(40), at(100)).map(|(gap, _)| gap),
        Some(11.0)
    );
    // No two polls around the window: unknown, not 0.
    assert_eq!(measure::longest_sample_gap(&samples, at(50), at(60)), None);
    assert_eq!(
        measure::longest_sample_gap(&samples[..1], at(0), at(60)),
        None
    );
    // The pure window summary leaves it to the sampler, which sees the polls
    // outside the window too; serialised, it is null rather than 0.
    let summary = measure::summarize_lock_window(
        measure::ORDER_LOCK,
        &samples,
        None,
        &["load-fe-0".to_owned()],
        "application_name",
        Duration::from_millis(10),
    );
    assert_eq!(summary.longest_sample_gap_milliseconds, None);
    let json = serde_json::to_value(&summary).unwrap();
    assert!(json["longest_sample_gap_milliseconds"].is_null());
}

fn block(steal: Option<f64>, max: Option<f64>) -> Value {
    let share = |percent: Option<f64>, max: Option<f64>| match percent {
        Some(p) => json!({"percent": p, "max_interval_percent": max, "measured_intervals": 3,
                          "unavailable_reason": null}),
        None => json!({"percent": null, "max_interval_percent": null, "measured_intervals": 0,
                       "unavailable_reason": "/proc/pressure/cpu: No such file or directory"}),
    };
    json!({
        "intervals": 3,
        "covered_from_milliseconds": -400.0,
        "covered_to_milliseconds": 2600.0,
        "steal": share(steal, max),
        "iowait": share(Some(0.5), Some(1.0)),
        "cpu_some": share(None, None),
        "io_some": share(Some(2.0), Some(4.0)),
        "io_full": share(Some(1.0), Some(2.0)),
        "memory_some": share(Some(0.0), Some(0.0)),
        "memory_full": share(Some(0.0), Some(0.0)),
        "phase": "warm_up",
    })
}

fn budgets() -> gate::Budgets {
    gate::Budgets {
        phases: None,
        max_shortfall: 0,
        max_rejected_valid_shares: None,
        max_unanswered_submits: None,
        tip_last_notify_p99_ms: Some(2500.0),
        tip_last_notify_median_ms: None,
        d1_verdict_table: false,
        churn_tip_last_notify_p99_ms: None,
        churn_tip_last_notify_median_ms: None,
        new_session_first_job_p99_ms: None,
    }
}

fn report(slowest: f64) -> Value {
    json!({
        "phases": [{"name": "warm_up", "shortfall": 0,
                    "host_contention": block(Some(0.2), Some(0.4)),
                    "order_lock": {"longest_sample_gap_milliseconds": 812.0,
                                   "longest_sample_gap_ended_at": "2026-11-03T21:40:02Z"}}],
        "time_to_usable_work": {"tips": [
            {"tip": "aa", "height": 101, "all_sessions_milliseconds": slowest,
             "host_contention": block(Some(31.0), Some(62.5))},
            {"tip": "bb", "height": 102, "all_sessions_milliseconds": 900.0,
             "host_contention": block(Some(0.0), Some(0.0))},
        ]},
    })
}

#[test]
fn a_contention_line_says_unknown_for_what_the_host_did_not_expose() {
    let line = gate::host_contention_line(&block(Some(31.0), Some(62.5))).unwrap();
    assert_eq!(
        line,
        "steal 31.0% (max 62.5%), iowait 0.5% (max 1.0%); PSI cpu some unknown, io some 2.0% \
         (max 4.0%), io full 1.0% (max 2.0%), memory some 0.0% (max 0.0%), memory full 0.0% \
         (max 0.0%); 3 intervals, reads from -400 to +2600 ms"
    );
    // A report written before the block existed has no line.
    assert_eq!(gate::host_contention_line(&Value::Null), None);
}

#[test]
fn a_failed_tip_row_says_what_the_host_did_over_the_slowest_tip_and_its_phase() {
    let row = |checks: &[gate::Check]| {
        checks
            .iter()
            .find(|c| c.name == "slowest tip: the host over its delivery, and over its phase")
            .cloned()
    };
    let checks = gate::evaluate(&report(4546.0), Some(0), &budgets());
    let failed = checks
        .iter()
        .find(|c| c.name == "tip to last session's notify, p99 over tips")
        .unwrap();
    assert_eq!(failed.pass, Some(false));
    let info = row(&checks).expect("the info row on a failed tip row");
    assert_eq!(info.pass, None);
    for needle in [
        "tip at height 101 (4546 ms): steal 31.0% (max 62.5%)",
        ". Phase warm_up: steal 0.2% (max 0.4%)",
        ". Lock sampler's longest gap in warm_up: 812 ms, ended 2026-11-03T21:40:02Z",
    ] {
        assert!(
            info.observed.contains(needle),
            "{needle}: {}",
            info.observed
        );
    }
    // A passing tip row gets no such row.
    let checks = gate::evaluate(&report(1200.0), Some(0), &budgets());
    assert!(row(&checks).is_none());
    // Nor does a failed one from a report without the blocks.
    let mut old = report(4546.0);
    for tip in old["time_to_usable_work"]["tips"].as_array_mut().unwrap() {
        tip.as_object_mut().unwrap().remove("host_contention");
    }
    assert!(row(&gate::evaluate(&old, Some(0), &budgets())).is_none());
    // An unserved tip is the one the row describes.
    let mut unserved = report(1200.0);
    unserved["time_to_usable_work"]["tips"][1]["all_sessions_milliseconds"] = Value::Null;
    let info = row(&gate::evaluate(&unserved, Some(0), &budgets())).unwrap();
    assert!(
        info.observed.starts_with("tip at height 102 (unserved)"),
        "{}",
        info.observed
    );
    // The timeline prints every tip's line.
    let timeline = gate::tip_timeline(&report(4546.0));
    assert_eq!(
        timeline.matches("  host over its delivery: steal").count(),
        2,
        "{timeline}"
    );
    // A steal counter still at 0 since boot puts its caveat beside a 0%.
    let caveat = "The steal counter was 0 since boot";
    let mut unreported = report(4546.0);
    unreported["host_contention"] = json!({"steal_counter_at_start_ticks": 0});
    let info = row(&gate::evaluate(&unreported, Some(0), &budgets())).unwrap();
    assert!(info.observed.contains(caveat), "{}", info.observed);
    assert!(gate::tip_timeline(&unreported).contains(caveat));
    let mut reported = report(4546.0);
    reported["host_contention"] = json!({"steal_counter_at_start_ticks": 12});
    let info = row(&gate::evaluate(&reported, Some(0), &budgets())).unwrap();
    assert!(!info.observed.contains(caveat), "{}", info.observed);
    assert!(!gate::tip_timeline(&report(4546.0)).contains(caveat));
}

/// The sampler's last read is taken when it finishes, so the run's final
/// interval has a read after it however short the teardown was.
#[tokio::test]
async fn finishing_the_sampler_takes_one_last_read() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("stat"), STAT).unwrap();
    let sampler = contention::HostSampler::start(root.path().to_owned(), Duration::from_secs(60));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let periodic = sampler.readings().len();
    assert_eq!(periodic, 1, "the first periodic read is taken at once");
    let before = Instant::now();
    sampler.finish();
    let readings = sampler.readings();
    assert_eq!(readings.len(), periodic + 1);
    assert!(readings.last().unwrap().at >= before);
}

#[test]
fn a_failed_churn_tip_row_gets_the_same_row() {
    let mut report = report(900.0);
    report["churn"] = json!({
        "ran": true,
        "tip_delivery": {"tips": [
            {"tip": "cc", "height": 108, "last_served_milliseconds": 3826.0,
             "host_contention": block(Some(12.0), Some(40.0))},
            {"tip": "dd", "height": 109, "last_served_milliseconds": 683.0,
             "host_contention": block(Some(0.0), Some(0.0))},
        ]},
        "time_to_first_job": {"new_sessions": {"p99": 20.0, "samples": 10, "max": 30.0}},
        "realised": {},
    });
    let budgets = gate::Budgets {
        churn_tip_last_notify_p99_ms: Some(3000.0),
        ..budgets()
    };
    let checks = gate::evaluate(&report, Some(0), &budgets);
    let info = checks
        .iter()
        .find(|c| c.name == "churn: slowest tip: the host over its delivery, and over its phase")
        .unwrap_or_else(|| panic!("{}", gate::markdown("x", &checks)));
    assert!(
        info.observed
            .starts_with("tip at height 108 (3826 ms): steal 12.0% (max 40.0%)"),
        "{}",
        info.observed
    );
    // Within budget, no row.
    let relaxed = gate::Budgets {
        churn_tip_last_notify_p99_ms: Some(6000.0),
        ..budgets
    };
    assert!(!gate::evaluate(&report, Some(0), &relaxed)
        .iter()
        .any(|c| c.name.starts_with("churn: slowest tip")));
}

/// The parser against this host's own `/proc/stat`, so the synthetic lines
/// above match the real format. PSI is not asserted: a CI container or a
/// kernel without it reads unknown, which the tests above cover.
#[cfg(target_os = "linux")]
#[test]
fn this_hosts_proc_stat_parses_with_a_steal_field() {
    let reading = contention::read_host_at(std::path::Path::new("/proc"));
    let cpu = reading.cpu.expect("/proc/stat parses");
    assert!(cpu.total > 0);
    assert!(cpu.steal.is_some(), "Linux has had steal since 2.6.11");
}

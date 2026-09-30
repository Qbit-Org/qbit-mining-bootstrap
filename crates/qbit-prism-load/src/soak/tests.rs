use super::*;
use crate::gate::Check;
use chrono::Utc;
use std::collections::BTreeMap;

fn gates() -> Gates {
    Gates {
        warmup_minutes: 10.0,
        min_gated_samples: 5,
        rss_slope_mib_per_hour_max: 16.0,
        rss_trend_window_minutes: 20.0,
        min_trend_windows: 3,
        rss_warmup_peak_multiple_max: Some(2.0),
        rss_expected_failure: None,
        fd_slope_per_hour_max: 2.0,
        pool_connections_max: 20,
        pool_connections_drift_max: 2.0,
        wal_bytes_max: 1 << 30,
        min_rollovers: 1,
        min_archive_cycles: 1,
        one_lifetime: true,
    }
}

/// One sample a minute for `minutes`, RSS growing by `leak_mib_per_hour`,
/// the share sequence crossing one bound and one partition dropped.
fn series(minutes: usize, leak_mib_per_hour: f64) -> Vec<Sample> {
    (0..minutes)
        .map(|minute| {
            let hours = minute as f64 / 60.0;
            let rss = (200.0 + leak_mib_per_hour * hours) * 1048576.0;
            let dropped = minute > minutes / 2;
            Sample {
                schema: SAMPLE_SCHEMA.into(),
                at: Utc::now(),
                elapsed_seconds: minute as f64 * 60.0,
                phase: Some("c01.x.steady_state".into()),
                steady: true,
                processes: vec![ProcessPoint {
                    instance: "load-fe-0".into(),
                    pid: Some(42),
                    rss_bytes: Some(rss as u64),
                    open_fds: Some(300 + (minute % 3) as u64),
                    threads: Some(8),
                    ..ProcessPoint::default()
                }],
                database: DatabasePoint {
                    connections: Some(
                        [("load-fe-0".to_owned(), 10 + (minute % 2) as u64)]
                            .into_iter()
                            .collect(),
                    ),
                    idle_in_transaction_over_60s: Some(0),
                    wal_bytes: Some(64 << 20),
                    next_share_seq: Some(1000 + 100 * minute as i64),
                    partitions: vec![
                        PartitionPoint {
                            name: "qbit_share_ledger_p0".into(),
                            state: if dropped { "dropped" } else { "attached" }.into(),
                            lower_seq: None,
                            upper_seq: 2000,
                            bytes: (!dropped).then_some(1 << 20),
                        },
                        PartitionPoint {
                            name: "qbit_share_ledger_p1".into(),
                            state: "attached".into(),
                            lower_seq: Some(2000),
                            upper_seq: 1 << 40,
                            bytes: Some(1 << 20),
                        },
                    ],
                    payout_divergences: Some(0),
                    ..DatabasePoint::default()
                },
                latency: None,
                ledger: None,
            }
        })
        .collect()
}

fn failed(checks: &[Check]) -> Vec<String> {
    checks
        .iter()
        .filter(|check| check.pass == Some(false))
        .map(|check| format!("{}: {}", check.name, check.observed))
        .collect()
}

#[test]
fn slope_is_least_squares_per_unit() {
    assert_eq!(slope(&[(0.0, 1.0), (1.0, 3.0), (2.0, 5.0)]), Some(2.0));
    assert_eq!(slope(&[(1.0, 1.0), (1.0, 2.0)]), None);
    assert_eq!(slope(&[(1.0, 1.0)]), None);
}

#[test]
fn window_peaks_take_each_whole_window_and_drop_the_tail() {
    let points: Vec<(f64, f64)> = (0..10).map(|i| (i as f64, i as f64 % 3.0)).collect();
    assert_eq!(
        window_peaks(&points, 3.0),
        vec![(1.5, 2.0), (4.5, 2.0), (7.5, 2.0)]
    );
    assert!(window_peaks(&[], 3.0).is_empty());
}

#[test]
fn landing_steps_pass_the_envelope_and_a_leak_under_them_does_not() {
    // A 700 MiB step for 12 of every 20 minutes, as a 400k window's
    // block landing makes, over a flat 500 MiB.
    let landing = |minute: usize| if minute % 20 < 12 { 700.0 } else { 0.0 };
    let mut samples = series(240, 0.0);
    for (minute, sample) in samples.iter_mut().enumerate() {
        sample.processes[0].rss_bytes = Some(((500.0 + landing(minute)) * 1048576.0) as u64);
    }
    assert!(failed(&evaluate(&samples, &gates())).is_empty());
    // The same landings over a 64 MiB/h leak.
    for (minute, sample) in samples.iter_mut().enumerate() {
        let leak = 64.0 * minute as f64 / 60.0;
        sample.processes[0].rss_bytes = Some(((500.0 + landing(minute) + leak) * 1048576.0) as u64);
    }
    let failures = failed(&evaluate(&samples, &gates()));
    assert!(
        failures
            .iter()
            .any(|line| line.starts_with("resident memory slope")),
        "{failures:?}"
    );
}

#[test]
fn a_flat_soak_passes_every_gate() {
    let checks = evaluate(&series(120, 0.0), &gates());
    assert!(failed(&checks).is_empty(), "{:?}", failed(&checks));
}

#[test]
fn a_leak_fails_the_resident_memory_gate_and_nothing_else() {
    let checks = evaluate(&series(120, 64.0), &gates());
    let failed = failed(&checks);
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert!(failed[0].starts_with("resident memory slope"), "{failed:?}");
    assert!(failed[0].contains("+64.00 MiB/h"), "{failed:?}");
    // The documented 2x bound is the coarser of the two: a leak that
    // doubles the warm-up peak fails it too.
    let failed = super::tests::failed(&evaluate(&series(120, 400.0), &gates()));
    assert!(
        failed
            .iter()
            .any(|line| line.starts_with("resident memory against the warm-up peak")),
        "{failed:?}"
    );
}

#[test]
fn the_warm_up_is_not_fitted() {
    let mut samples = series(120, 0.0);
    // Growth confined to the warm-up (the first 10 minutes) is allowed,
    // within the documented 2x of its peak.
    for sample in samples.iter_mut().take(10) {
        sample.processes[0].rss_bytes = Some(120 << 20);
    }
    assert!(failed(&evaluate(&samples, &gates())).is_empty());
}

#[test]
fn unknown_readings_fail_rather_than_pass() {
    let mut samples = series(120, 0.0);
    for sample in &mut samples {
        sample.processes[0].rss_bytes = None;
        sample.database.wal_bytes = None;
        sample.database.idle_in_transaction_over_60s = None;
    }
    let failures = failed(&evaluate(&samples, &gates()));
    for name in ["resident memory", "WAL size", "idle in transaction"] {
        assert!(
            failures
                .iter()
                .any(|line| line.starts_with(name) && line.contains("unknown")),
            "{name}: {failures:?}"
        );
    }
    assert!(failed(&evaluate(&[], &gates()))[0].contains("no sample"));
}

#[test]
fn drift_restarts_rollovers_and_retention_are_each_gated() {
    let mut samples = series(120, 0.0);
    let len = samples.len();
    for sample in samples.iter_mut().skip(len * 3 / 4) {
        if let Some(clients) = sample.database.connections.as_mut() {
            clients.insert("load-fe-0".into(), 19);
        }
    }
    samples[60].processes[0].pid = Some(43);
    for sample in &mut samples {
        sample.database.next_share_seq = Some(1000);
        sample.database.partitions[0].state = "attached".into();
    }
    samples[70].database.wal_bytes = Some(2 << 30);
    samples[80].database.idle_in_transaction_over_60s = Some(1);
    let failed = failed(&evaluate(&samples, &gates()));
    for name in [
        "database connection drift",
        "one server lifetime",
        "share partition rollovers",
        "share partitions archived and dropped",
        "WAL size",
        "idle in transaction",
    ] {
        assert!(
            failed.iter().any(|line| line.starts_with(name)),
            "{name}: {failed:?}"
        );
    }
}

#[test]
fn a_deployment_ledger_must_hold_every_acknowledged_share() {
    let mut samples = series(120, 0.0);
    let point = |acked, committed| LedgerPoint {
        acknowledged_since_start: Some(acked),
        acknowledged_gaps: 0,
        committed_since_start: Some(committed),
        committed_through: None,
        tolerance_rows: Some(3),
        tolerance_seconds: 4.0,
    };
    samples.last_mut().unwrap().ledger = Some(point(1000, 1000));
    assert!(failed(&evaluate(&samples, &gates())).is_empty());
    // Processes scraped a few seconds after the earliest may be ahead by the
    // rows of that spread, and no more.
    samples.last_mut().unwrap().ledger = Some(point(1000, 997));
    assert!(failed(&evaluate(&samples, &gates())).is_empty());
    samples.last_mut().unwrap().ledger = Some(point(1000, 996));
    let failed = failed(&evaluate(&samples, &gates()));
    assert!(
        failed[0].starts_with("acknowledged shares in the ledger"),
        "{failed:?}"
    );
}

#[test]
fn a_process_gone_dark_at_the_end_fails_its_trends() {
    let mut samples = series(120, 0.0);
    // Its last hour of samples name it but read nothing, as the deployment
    // sampler records a process whose series vanished.
    for sample in samples.iter_mut().skip(60) {
        sample.processes[0].rss_bytes = None;
        sample.processes[0].open_fds = None;
    }
    let failures = failed(&evaluate(&samples, &gates()));
    for name in ["resident memory slope", "open file descriptors slope"] {
        assert!(
            failures
                .iter()
                .any(|line| line.starts_with(name) && line.contains("latest sample")),
            "{name}: {failures:?}"
        );
    }
}

#[test]
fn an_unreadable_divergence_table_fails_rather_than_passes() {
    let mut samples = series(120, 0.0);
    samples[119].database.payout_divergences = None;
    let failures = failed(&evaluate(&samples, &gates()));
    assert!(
        failures[0].starts_with("payout divergences recorded during the soak")
            && failures[0].contains("unknown in 1 of 120"),
        "{failures:?}"
    );
    for sample in &mut samples {
        sample.database.payout_divergences = None;
    }
    assert!(!failed(&evaluate(&samples, &gates())).is_empty());
}

#[test]
fn no_client_is_a_real_zero_and_an_unread_activity_view_is_unknown() {
    let mut samples = series(120, 0.0);
    for sample in &mut samples {
        sample.database.connections = Some(BTreeMap::new());
    }
    assert!(failed(&evaluate(&samples, &gates())).is_empty());
    samples[30].database.connections = None;
    let failures = failed(&evaluate(&samples, &gates()));
    assert!(
        failures[0].starts_with("database connections: unknown: 1 of 120"),
        "{failures:?}"
    );
}

#[test]
fn a_restart_warms_up_again_instead_of_failing_or_fitting_the_cold_start() {
    let mut gates = gates();
    gates.one_lifetime = false;
    let mut samples = series(240, 0.0);
    // Restarted at 120 minutes, it starts cold and grows for its own
    // warm-up (10 minutes) before settling flat.
    for (minute, sample) in samples.iter_mut().enumerate().skip(120) {
        let cold = (minute - 120).min(10) as f64 * 40.0;
        sample.processes[0].pid = Some(43);
        sample.processes[0].rss_bytes = Some(((100.0 + cold) * 1048576.0) as u64);
    }
    let failures = failed(&evaluate(&samples, &gates));
    assert!(failures.is_empty(), "{failures:?}");
    // The same restart with a leak in the new lifetime still fails.
    for (minute, sample) in samples.iter_mut().enumerate().skip(130) {
        let leak = 64.0 * (minute - 130) as f64 / 60.0;
        sample.processes[0].rss_bytes = Some(((500.0 + leak) * 1048576.0) as u64);
    }
    let failures = failed(&evaluate(&samples, &gates));
    assert!(
        failures
            .iter()
            .any(|line| line.starts_with("resident memory slope")),
        "{failures:?}"
    );
}

#[test]
fn a_new_payout_divergence_fails_the_soak() {
    let mut samples = series(120, 0.0);
    samples.last_mut().unwrap().database.payout_divergences = Some(1);
    let failed = failed(&evaluate(&samples, &gates()));
    assert!(failed[0].starts_with("payout divergences"), "{failed:?}");
}

#[test]
fn samples_round_trip_through_the_jsonl_file() {
    let dir = std::env::temp_dir().join(format!("soak-samples-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(SAMPLES_FILE);
    let samples = series(3, 0.0);
    for sample in &samples {
        append_line(&path, sample).unwrap();
    }
    assert_eq!(read_samples(&path).unwrap(), samples);
    std::fs::write(&path, "{\"schema\":\"other\"}\n").unwrap();
    assert!(read_samples(&path).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn the_checked_in_testnet4_gates_parse_and_validate() {
    let gates: Gates =
        serde_json::from_str(include_str!("../../soak-gates/testnet4.json")).unwrap();
    gates.validate().unwrap();
    assert!(
        !gates.one_lifetime,
        "a deployment may be restarted by its operator"
    );
    assert_eq!(gates.rss_expected_failure, None);
}

#[test]
fn an_expected_resident_memory_failure_passes_until_it_looks_fixed() {
    let mut known = gates();
    known.rss_expected_failure = Some("#600".into());
    // A leak fails the slope row alone; with the issue named it is reported,
    // not gated, and every other gate still holds.
    let checks = evaluate(&series(120, 64.0), &known);
    assert!(failed(&checks).is_empty(), "{:?}", failed(&checks));
    let expected: Vec<&Check> = checks
        .iter()
        .filter(|c| c.observed.starts_with("expected failure (#600): "))
        .collect();
    assert_eq!(expected.len(), 1, "{checks:?}");
    assert!(expected[0].name.starts_with("resident memory slope"));
    assert_eq!(expected[0].pass, None);
    // The warm-up ratio row passes beside it, as fe-1's did in #600's run:
    // the group has failed, so that is not "looks fixed".
    assert!(checks
        .iter()
        .any(|c| c.name.starts_with("resident memory against") && c.pass == Some(true)));
    // #600's shape: one frontend leaks, a second stays flat and passes both
    // of its rows. The group has still failed as expected.
    let mut samples = series(120, 64.0);
    for sample in &mut samples {
        let mut flat = sample.processes[0].clone();
        flat.instance = "load-fe-1".into();
        flat.pid = Some(43);
        flat.rss_bytes = Some(200 << 20);
        sample.processes.push(flat);
        if let Some(clients) = sample.database.connections.as_mut() {
            clients.insert("load-fe-1".into(), 10);
        }
    }
    let checks = evaluate(&samples, &known);
    assert!(failed(&checks).is_empty(), "{:?}", failed(&checks));
    assert_eq!(
        checks
            .iter()
            .filter(|c| c.name.starts_with("resident memory") && c.name.ends_with("load-fe-1"))
            .filter(|c| c.pass == Some(true))
            .count(),
        2,
        "{checks:?}"
    );
    // Flat memory: the issue looks fixed, and that fails the soak.
    let failures = failed(&evaluate(&series(120, 0.0), &known));
    assert_eq!(
        failures,
        vec!["resident memory expected failure, #600: #600 looks fixed: every resident-memory row passed"]
    );
    // Other gates stay hard: a descriptor leak still fails.
    let mut samples = series(120, 64.0);
    for (minute, sample) in samples.iter_mut().enumerate() {
        sample.processes[0].open_fds = Some(300 + minute as u64);
    }
    let failures = failed(&evaluate(&samples, &known));
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(failures[0].starts_with("open file descriptors slope"));
}

#[test]
fn an_expected_resident_memory_failure_is_no_excuse_for_an_unknown_reading() {
    let mut known = gates();
    known.rss_expected_failure = Some("#600".into());
    let mut samples = series(120, 0.0);
    for sample in &mut samples {
        sample.processes[0].rss_bytes = None;
    }
    let failures = failed(&evaluate(&samples, &known));
    assert!(
        failures
            .iter()
            .all(|line| line.starts_with("resident memory") && line.contains("unknown")),
        "{failures:?}"
    );
    assert!(!failures.is_empty());
    assert!(!failures.iter().any(|line| line.contains("looks fixed")));
}

#[test]
fn rss_expected_failure_must_name_an_issue() {
    let mut known = gates();
    for bad in ["600", "#", "#60a", "see #600"] {
        known.rss_expected_failure = Some(bad.into());
        assert!(known.validate().is_err(), "{bad}");
    }
    known.rss_expected_failure = Some("#600".into());
    known.validate().unwrap();
}

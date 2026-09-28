//! The connection-churn model (#521): parsing, the seeded plan, and the
//! realised-churn and tip-delivery measurements over synthetic timelines.
//! Pure unit tests; the end-to-end churn run is the `rental-churn` preset.

use clap::Parser;
use qbit_prism_load::{
    churn::{self, BurstSizes, ChurnSpec, Tail},
    cli::{self, Args},
    client::{ConnectionClosed, ConnectionOpened, TipSighting},
    gate,
    realism::Rng,
};
use serde_json::json;
use std::time::{Duration, Instant};

fn spec() -> ChurnSpec {
    ChurnSpec {
        seconds: 300,
        tips: 8,
        bursts: BurstSizes::parse("100,500,2000").unwrap(),
        burst_window_seconds: 10.0,
        burst_interval_seconds: 90.0,
        lifetime: Tail::parse("pareto:xm=30,alpha=1.2,max=600", "lifetime").unwrap(),
        rental_hashrate: 20.0,
        storms: churn::parse_storms("0.1,0.25,0.5").unwrap(),
        storm_interval_seconds: 90.0,
        storm_reconnect_seconds: 5.0,
        seed: 1,
    }
}

#[test]
fn churn_parameters_are_parsed_and_refused_at_the_boundary() {
    for text in [
        "",
        "pareto",
        "pareto:xm=30,alpha=1.2",
        "pareto:xm=0,alpha=1.2,max=10",
        "pareto:xm=30,alpha=0,max=100",
        "pareto:xm=30,alpha=1.2,max=10",
        "pareto:xm=inf,alpha=1.2,max=inf",
        "pareto:xm=30,alpha=1.2,max=100,xm=3",
        "weibull:k=1",
    ] {
        assert!(Tail::parse(text, "x").is_err(), "accepted {text:?}");
    }
    for text in [
        "",
        "0",
        "100,-1",
        "1.5",
        "a,b",
        "pareto:xm=1,alpha=1,max=5;count=0",
    ] {
        assert!(BurstSizes::parse(text).is_err(), "accepted {text:?}");
    }
    for text in ["0", "1.5", "-0.1", "NaN", "0.1,,0.2"] {
        assert!(churn::parse_storms(text).is_err(), "accepted {text:?}");
    }
    assert_eq!(BurstSizes::parse("none").unwrap(), BurstSizes::None);
    assert_eq!(churn::parse_storms("none").unwrap(), Vec::<f64>::new());
    assert_eq!(
        BurstSizes::parse("pareto:xm=100,alpha=1.1,max=2000;count=4")
            .unwrap()
            .render(),
        "pareto:xm=100,alpha=1.1,max=2000;count=4"
    );
}

#[test]
fn every_churn_flag_defaults_to_off_and_needs_the_phase() {
    let args = Args::parse_from(["qbit-prism-load"]);
    args.validate().unwrap();
    assert!(!args.churn_spec().unwrap().is_on());
    assert_eq!(args.peak_sessions(), args.sessions);
    assert!(cli::phases(&args)
        .unwrap()
        .iter()
        .all(|phase| phase.name != churn::PHASE));
    let refused = |extra: &[&str], needle: &str| {
        let mut argv = vec!["qbit-prism-load"];
        argv.extend_from_slice(extra);
        let error = Args::try_parse_from(argv)
            .map_err(anyhow::Error::from)
            .and_then(|args| args.validate())
            .unwrap_err();
        assert!(
            format!("{error:#}").contains(needle),
            "{extra:?}: {error:#}"
        );
    };
    refused(&["--rental-bursts", "100"], "need --churn-seconds");
    refused(&["--reconnect-storms", "0.5"], "need --churn-seconds");
    refused(&["--churn-tips", "3"], "need --churn-seconds");
    refused(&["--churn-seconds", "10"], "30..7200");
    refused(
        &["--churn-seconds", "60", "--rental-hashrate", "0"],
        "--rental-hashrate",
    );
    refused(
        &["--churn-seconds", "60", "--storm-reconnect-seconds", "NaN"],
        "--storm-reconnect-seconds",
    );
    refused(
        &["--churn-seconds", "60", "--churn-rate=-1"],
        "--churn-rate",
    );
}

#[test]
fn the_churn_phase_follows_the_plan_and_sizes_the_connection_cap() {
    let args = Args::parse_from([
        "qbit-prism-load",
        "--churn-seconds",
        "300",
        "--rental-bursts",
        "100,500,2000",
        "--sessions",
        "400",
        "--frontends",
        "2",
    ]);
    args.validate().unwrap();
    let names: Vec<String> = cli::phases(&args)
        .unwrap()
        .into_iter()
        .map(|phase| phase.name)
        .collect();
    assert_eq!(
        names,
        [
            "warm_up",
            "steady_state",
            "reconnect",
            "slow_database",
            "churn"
        ]
    );
    assert_eq!(args.peak_sessions(), 3000);
    let limits = args.stratum_limits();
    assert_eq!(limits.sessions_per_frontend, 1500);
    assert_eq!(limits.max_connections, 3064);
    let tips = Args::parse_from(["qbit-prism-load", "--plan", "tips", "--churn-seconds", "60"]);
    tips.validate().unwrap();
    let names: Vec<String> = cli::phases(&tips)
        .unwrap()
        .into_iter()
        .map(|phase| phase.name)
        .collect();
    assert_eq!(names, ["warm_up", "churn"]);
}

#[test]
fn the_plan_is_seeded_and_places_bursts_lifetimes_storms_and_tips() {
    let plan = spec().plan();
    assert_eq!(plan, spec().plan(), "same seed, same plan");
    assert_ne!(plan, ChurnSpec { seed: 2, ..spec() }.plan());
    assert_eq!(plan.rentals.len(), 2600);
    for (burst, start, size) in [(0, 5.0, 100), (1, 95.0, 500), (2, 185.0, 2000)] {
        let members: Vec<_> = plan.rentals.iter().filter(|r| r.burst == burst).collect();
        assert_eq!(members.len(), size);
        assert!(members
            .iter()
            .all(|r| r.arrive_at >= start && r.arrive_at < start + 10.0));
        for rental in members {
            assert!(rental.lifetime_seconds >= 30.0 && rental.lifetime_seconds <= 600.0);
            match rental.depart_at {
                Some(depart) => assert!(
                    (depart - rental.arrive_at - rental.lifetime_seconds).abs() < 1e-9
                        && depart < 300.0
                ),
                None => assert!(rental.arrive_at + rental.lifetime_seconds >= 300.0),
            }
        }
    }
    // Heavy-tailed: most leave inside the phase, a tail outlives it.
    let departing = plan
        .rentals
        .iter()
        .filter(|r| r.depart_at.is_some())
        .count();
    assert!(departing > 1000 && departing < 2600, "{departing}");
    assert!(plan.peak_rentals() <= 2600 && plan.peak_rentals() >= 2000);
    let storms: Vec<(f64, f64)> = plan.storms.iter().map(|s| (s.at, s.fraction)).collect();
    assert_eq!(storms, [(60.0, 0.1), (150.0, 0.25), (240.0, 0.5)]);
    assert_eq!(plan.tips.len(), 8);
    assert!(plan.tips.windows(2).all(|pair| pair[0] < pair[1]));
    // A burst that would start after the phase is not planned.
    let short = ChurnSpec {
        seconds: 60,
        ..spec()
    }
    .plan();
    assert_eq!(short.rentals.len(), 100);
    assert!(short.storms.is_empty());
    // Drawn sizes are reproducible and capped.
    let drawn = BurstSizes::parse("pareto:xm=100,alpha=1.1,max=2000;count=6").unwrap();
    let sizes = drawn.sizes(&mut Rng::new(1, "churn-burst-sizes"));
    assert_eq!(sizes, drawn.sizes(&mut Rng::new(1, "churn-burst-sizes")));
    assert!(sizes.iter().all(|size| (100..=2000).contains(size)));
    assert_eq!(drawn.upper_bound(), 12_000);
}

fn opened(session: usize, started: Instant, ready: Instant, cause: &str) -> ConnectionOpened {
    ConnectionOpened {
        session,
        frontend: 0,
        started,
        ready,
        cause: cause.into(),
    }
}

fn closed(session: usize, at: Instant, cause: &str) -> ConnectionClosed {
    ConnectionClosed {
        session,
        at,
        cause: cause.into(),
    }
}

fn sighting(session: usize, tip: &str, at: Instant) -> TipSighting {
    TipSighting {
        session,
        frontend: 0,
        tip: tip.into(),
        at,
    }
}

/// Tip delivery counts only the sessions connected at the tip: one served,
/// one that left before it was served (excluded), one that arrived after
/// (not counted), and, in the second case, one that stayed and was never
/// served (the failure the gate holds).
#[test]
fn tip_delivery_is_over_the_sessions_connected_at_the_tip() {
    let t = Instant::now();
    let at = |ms: u64| t + Duration::from_millis(ms);
    let tip = qbit_prism_load::node::TipChange {
        hash: "aa".into(),
        height: 101,
        origin: qbit_prism_load::node::TipOrigin::External,
        monotonic: at(1000),
        wall: chrono::Utc::now(),
    };
    let next = qbit_prism_load::node::TipChange {
        monotonic: at(9000),
        hash: "bb".into(),
        ..tip.clone()
    };
    let opened_list = vec![
        opened(0, at(0), at(10), "initial"),
        opened(1, at(0), at(20), "initial"),
        opened(2, at(2000), at(2300), "initial"),
        opened(3, at(0), at(30), "initial"),
    ];
    let closed_list = vec![closed(1, at(1500), "churn close: rental departed")];
    let sightings = vec![
        sighting(0, "aa", at(1250)),
        sighting(2, "aa", at(2300)),
        sighting(3, "aa", at(1900)),
    ];
    let changes = [tip.clone(), next];
    let delivery = churn::tip_delivery_over(
        std::slice::from_ref(&tip),
        &changes,
        &opened_list,
        &closed_list,
        &sightings,
        at(10_000),
    );
    let row = &delivery["tips"][0];
    assert_eq!(row["connected_at_tip"], 3, "{row}");
    assert_eq!(row["served"], 2, "{row}");
    assert_eq!(row["left_before_served"], 1, "{row}");
    assert_eq!(row["unserved"], 0, "{row}");
    assert_eq!(row["last_served_milliseconds"], 900.0, "{row}");
    // Session 3 never served while connected: the tip's figure is null.
    let unserved = churn::tip_delivery_over(
        std::slice::from_ref(&tip),
        &changes,
        &opened_list,
        &closed_list,
        &sightings[..2],
        at(10_000),
    );
    let row = &unserved["tips"][0];
    assert_eq!(row["unserved"], 1, "{row}");
    assert!(row["last_served_milliseconds"].is_null(), "{row}");
}

#[test]
fn the_gate_holds_churn_tips_and_first_jobs_and_fails_what_it_cannot_measure() {
    let report = |last: serde_json::Value, p99: serde_json::Value| {
        json!({
            "aborted": null, "durability_findings": [],
            "phases": [{"name": "churn", "shortfall": 0, "rejected_valid_shares": 0,
                        "reconciliation": {"missing": 0, "unexpected": 0}}],
            "rejections": {"no_response_by_phase": {}, "by_phase_reason_and_message": []},
            "time_to_usable_work": {"tips": [{"all_sessions_milliseconds": 100.0}]},
            "churn": {"ran": true,
                "tip_delivery": {"tips": [
                    {"last_served_milliseconds": 800.0, "unserved": 0, "connected_at_tip": 900},
                    {"last_served_milliseconds": last, "unserved": 2, "connected_at_tip": 2400}]},
                "time_to_first_job": {"new_sessions": {"p99": p99, "samples": 2600, "max": 4000.0,
                    "unavailable_reason": null}},
                "realised": {"connects_per_second_max": 400, "concurrent_sessions_min": 380,
                    "concurrent_sessions_max": 2900, "storms": [{}, {}, {}],
                    "rentals_spawned": 2600, "rentals_departed": 1800}},
        })
    };
    let budgets = gate::Budgets {
        phases: None,
        max_shortfall: 0,
        max_rejected_valid_shares: None,
        max_unanswered_submits: None,
        tip_last_notify_p99_ms: None,
        d1_verdict_table: false,
        churn_tip_last_notify_p99_ms: Some(3000.0),
        new_session_first_job_p99_ms: Some(10_000.0),
    };
    let clean = gate::evaluate(&report(json!(1200.0), json!(2500.0)), Some(0), &budgets);
    assert!(gate::passed(&clean), "{}", gate::markdown("x", &clean));
    for (last, p99) in [
        (json!(null), json!(2500.0)),
        (json!(3500.0), json!(2500.0)),
        (json!(1200.0), json!(12_000.0)),
        (json!(1200.0), json!(null)),
    ] {
        let checks = gate::evaluate(&report(last.clone(), p99.clone()), Some(0), &budgets);
        assert!(
            !gate::passed(&checks),
            "{last} {p99}: {}",
            gate::markdown("x", &checks)
        );
    }
    let mut none = report(json!(1200.0), json!(2500.0));
    none["churn"] = json!({"ran": false, "reason": "the run did not ask for --churn-seconds"});
    assert!(!gate::passed(&gate::evaluate(&none, Some(0), &budgets)));
}

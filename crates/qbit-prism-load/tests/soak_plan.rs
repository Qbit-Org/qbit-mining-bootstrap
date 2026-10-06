//! The checked-in soak presets plan what they say (#575 item 2), without a
//! cluster: which workloads loop, over which server, for how long, and that
//! nothing in a soak restarts a frontend.

use anyhow::{Context, Result};
use clap::Parser;
use qbit_prism_load::{cli::Args, preset, soak, soak_driver};

fn load(name: &str) -> Result<(preset::Preset, Args)> {
    let loaded = preset::Preset::load(&preset::presets_dir().join(format!("{name}.json")))?;
    let mut argv = vec!["qbit-prism-load".to_owned()];
    argv.extend(loaded.argv()?);
    let args = Args::try_parse_from(&argv)?;
    Ok((loaded, args))
}

#[test]
fn the_weekly_soak_loops_three_presets_for_twelve_cycles_in_one_lifetime() -> Result<()> {
    let (loaded, args) = load("soak-weekly")?;
    assert_eq!(loaded.schedule, preset::Schedule::Weekly);
    let plan = soak_driver::plan(&args, &loaded)?;
    // 600 s of mainnet shape, 360 s of rental churn, 570 s of tip delivery:
    // their own phases less the degraded-database one.
    assert_eq!(plan.cycle_seconds, 1530);
    assert_eq!(plan.cycles, 13);
    let seconds: u64 = plan.phases.iter().map(|phase| phase.plan.seconds).sum();
    assert_eq!(seconds, 13 * 1530, "5.5 h of scheduled load");
    assert!(seconds / 60 <= plan.spec.minutes);
    assert!(u64::from(loaded.timeout_minutes) < 360);
    for phase in &plan.phases {
        assert!(!phase.plan.restart_frontend, "{}", phase.plan.name);
        assert!(!phase.plan.mid_flight_kill, "{}", phase.plan.name);
        assert!(!phase.plan.in_artifact, "{}", phase.plan.name);
        assert_ne!(phase.plan.kind, "slow_database", "{}", phase.plan.name);
        // The server-level flags are the soak preset's, whatever the looped
        // preset's own: one lifetime has one configuration.
        assert_eq!(phase.args.frontends, args.frontends);
        assert_eq!(phase.args.sessions, args.sessions);
        assert_eq!(phase.args.window_shares, args.window_shares);
        assert_eq!(phase.args.pool_fee_bps, args.pool_fee_bps);
        assert_eq!(phase.args.template_bits()?, args.template_bits()?);
    }
    let names: Vec<&str> = plan.phases.iter().map(|p| p.plan.name.as_str()).collect();
    assert_eq!(
        &names[..9],
        [
            "c01.mainnet-shape-130-addresses.warm_up",
            "c01.mainnet-shape-130-addresses.steady_state",
            "c01.mainnet-shape-130-addresses.reconnect",
            "c01.rental-churn-bursts-and-storms.warm_up",
            "c01.rental-churn-bursts-and-storms.churn",
            "c01.tip-delivery-2000-miners-400k-2fe-retarget.warm_up",
            "c01.tip-delivery-2000-miners-400k-2fe-retarget.steady_state",
            "c01.tip-delivery-2000-miners-400k-2fe-retarget.reconnect",
            "c02.mainnet-shape-130-addresses.warm_up",
        ]
    );
    // Each segment runs its own preset's workload.
    let tip = plan
        .phases
        .iter()
        .find(|p| p.plan.name == "c01.tip-delivery-2000-miners-400k-2fe-retarget.warm_up")
        .context("tip-delivery warm-up")?;
    assert_eq!(tip.args.external_tips, 8);
    assert_eq!(tip.plan.rate, 133.0);
    assert_eq!(tip.args.sessions, 400, "the soak's population, not 2,000");
    let churn = plan
        .phases
        .iter()
        .find(|p| p.plan.kind == "churn")
        .context("churn")?;
    assert_eq!(churn.args.rental_bursts, "100,500,2000");
    // The blocks land where each looped preset's own plan puts them (the
    // phase kind, not the soak's phase name): mainnet shape's two per cycle
    // in steady_state, none from the tips-plan churn or the tip delivery.
    let mut landings = 0;
    for phase in &plan.phases {
        if qbit_prism_load::run::holds_scheduled_blocks(&phase.args, &phase.plan)? {
            landings += phase.args.scheduled_blocks;
        }
    }
    assert_eq!(landings, 26);
    // The rollovers and the retention the gates ask for fit in the soak.
    let spec = &plan.spec;
    assert!(spec.rollover_minutes.len() as u64 > spec.gates.min_rollovers);
    assert!(spec
        .rollover_minutes
        .iter()
        .all(|m| *m < spec.gates.warmup_minutes * 3.0));
    Ok(())
}

#[test]
fn every_soak_preset_plans_and_the_short_ones_fit_their_timeouts() -> Result<()> {
    for name in ["soak-weekly", "soak-short", "soak-smoke"] {
        let (loaded, args) = load(name)?;
        let plan = soak_driver::plan(&args, &loaded)?;
        let seconds: u64 = plan.phases.iter().map(|phase| phase.plan.seconds).sum();
        assert!(
            seconds / 60 + 10 < u64::from(loaded.timeout_minutes),
            "{name}: {seconds} s of load in {} min",
            loaded.timeout_minutes
        );
        assert_eq!(args.plan()?, qbit_prism_load::cli::Plan::Soak);
        // A churn budget over a soak that loops no churn would fail as
        // unmeasured on every run.
        if !plan.phases.iter().any(|phase| phase.plan.kind == "churn") {
            assert_eq!(
                loaded.gates.churn_tip_last_notify_p99_budget_ms, None,
                "{name}"
            );
            assert_eq!(
                loaded.gates.new_session_first_job_p99_budget_ms, None,
                "{name}"
            );
        }
    }
    let (_, args) = load("soak-smoke")?;
    assert_eq!(args.window_shares, 1000);
    Ok(())
}

/// #600's resident-memory ratchet is fixed (#627): the weekly soak passed
/// every resident-memory row, so no soak expects a resident-memory failure
/// and every soak gates resident memory for real.
#[test]
fn no_soak_expects_a_resident_memory_failure() -> Result<()> {
    for (name, issue) in [
        ("soak-weekly", None),
        ("soak-short", None),
        ("soak-smoke", None),
    ] {
        let (loaded, _) = load(name)?;
        let spec = loaded.soak.as_ref().context("no soak block")?;
        assert_eq!(spec.gates.rss_expected_failure.as_deref(), issue, "{name}");
    }
    Ok(())
}

#[test]
fn a_soak_block_and_plan_soak_go_together() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("soak-plan-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    let text = std::fs::read_to_string(preset::presets_dir().join("soak-smoke.json"))?;
    let mut value: serde_json::Value = serde_json::from_str(&text)?;
    value["name"] = "x".into();
    // A soak block without --plan soak.
    value["args"]["--plan"] = "tips".into();
    std::fs::write(dir.join("x.json"), value.to_string())?;
    assert!(preset::Preset::load(&dir.join("x.json")).is_err());
    // --plan soak without a soak block.
    value["args"]["--plan"] = "soak".into();
    value.as_object_mut().context("object")?.remove("soak");
    std::fs::write(dir.join("x.json"), value.to_string())?;
    assert!(preset::Preset::load(&dir.join("x.json")).is_err());
    // An unknown key in the soak block.
    let mut value: serde_json::Value = serde_json::from_str(&text)?;
    value["name"] = "x".into();
    value["soak"]["surprise"] = 1.into();
    std::fs::write(dir.join("x.json"), value.to_string())?;
    assert!(preset::Preset::load(&dir.join("x.json")).is_err());
    std::fs::remove_dir_all(&dir)?;
    // --plan soak without a preset has no phases of its own.
    let args = Args::try_parse_from(["qbit-prism-load", "--plan", "soak"])?;
    assert!(qbit_prism_load::cli::phases(&args).is_err());
    // The looped presets pin the fake node, so a real-node soak is refused
    // by name rather than run on segments that disagree with it.
    let error = Args::try_parse_from([
        "qbit-prism-load",
        "--plan",
        "soak",
        "--node",
        "qbitd",
        "--qbitd-bin",
        "/bin/true",
    ])?
    .validate()
    .expect_err("--node qbitd --plan soak");
    assert!(
        format!("{error:#}").contains("does not run --plan soak"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn a_looped_preset_that_kills_a_frontend_or_outgrows_the_listeners_is_refused() -> Result<()> {
    let (loaded, args) = load("soak-smoke")?;
    let dir = std::env::temp_dir().join(format!("soak-refuse-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    let smoke = std::fs::read_to_string(preset::presets_dir().join("pr-smoke.json"))?;
    let write = |edit: &dyn Fn(&mut serde_json::Value)| -> Result<preset::Preset> {
        let mut value: serde_json::Value = serde_json::from_str(&smoke)?;
        edit(&mut value);
        std::fs::write(dir.join("pr-smoke.json"), value.to_string())?;
        let mut soak_value: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
            preset::presets_dir().join("soak-smoke.json"),
        )?)?;
        soak_value["name"] = "soak-smoke".into();
        std::fs::write(dir.join("soak-smoke.json"), soak_value.to_string())?;
        preset::Preset::load(&dir.join("soak-smoke.json"))
    };
    let killer = write(&|value| {
        value["args"]["--plan"] = "short".into();
        value["args"]["--mid-flight-kill"] = true.into();
    })?;
    let error = soak_driver::plan(&args, &killer).unwrap_err();
    assert!(
        format!("{error:#}").contains("mid-flight-kill"),
        "{error:#}"
    );
    let crowd = write(&|value| value["args"]["--rental-bursts"] = "20,40,5000".into())?;
    let error = soak_driver::plan(&args, &crowd).unwrap_err();
    assert!(
        format!("{error:#}").contains("sessions at once"),
        "{error:#}"
    );
    // The checked-in pair plans.
    soak_driver::plan(&args, &loaded)?;
    std::fs::remove_dir_all(&dir)?;
    let _ = soak::WORKLOAD_FLAGS;
    Ok(())
}

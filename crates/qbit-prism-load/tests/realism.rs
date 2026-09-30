//! Population realism, checked-in presets and the run gate (#521). Pure unit
//! tests: nothing here needs PostgreSQL or a server.

use anyhow::Result;
use clap::Parser;
use qbit_prism_load::{
    cli::{self, Args},
    gate, preset,
    realism::{
        self, apportion, Arrival, Population, PopulationSpec, Rng, SessionDifficulty, WeightDist,
    },
    run, window,
};
use qbit_prism_server::codec;
use serde_json::{json, Value};

fn spec(recipients: Option<usize>, weights: &str, sessions: usize) -> PopulationSpec {
    PopulationSpec {
        recipients,
        weights: WeightDist::parse(weights).unwrap(),
        difficulty: SessionDifficulty::Fixed,
        hashrate_sigma: 0.0,
        sessions,
        seed: 1,
    }
}

// --- generator -----------------------------------------------------------

#[test]
fn every_stream_is_reproducible_from_its_seed_and_independent_of_the_others() {
    let draw = |seed, name: &str| {
        let mut rng = Rng::new(seed, name);
        (0..8).map(|_| rng.next_u64()).collect::<Vec<_>>()
    };
    assert_eq!(draw(7, "window"), draw(7, "window"));
    assert_ne!(draw(7, "window"), draw(8, "window"));
    assert_ne!(draw(7, "window"), draw(7, "offers"));
    let mut rng = Rng::new(0, "");
    for _ in 0..10_000 {
        let value = rng.next_f64();
        assert!((0.0..1.0).contains(&value), "{value}");
    }
    let mut rng = Rng::new(3, "lognormal");
    let n = 200_000;
    let draws: Vec<f64> = (0..n).map(|_| rng.lognormal_mean_one(1.2)).collect();
    let mean = draws.iter().sum::<f64>() / n as f64;
    let sd = (draws.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64).sqrt();
    assert!((mean - 1.0).abs() < 0.03, "mean {mean}");
    assert!((sd / mean - 1.2).abs() < 0.15, "cv {}", sd / mean);
    assert_eq!(Rng::new(1, "x").lognormal_mean_one(0.0), 1.0);
}

// --- distributions ---------------------------------------------------------

#[test]
fn weight_distributions_sum_to_one_heaviest_first_with_the_asked_shape() -> Result<()> {
    let mut rng = Rng::new(1, "w");
    let uniform = WeightDist::parse("uniform")?.weights(4, &mut rng);
    assert_eq!(uniform, vec![0.25; 4]);
    let zipf = WeightDist::parse("zipf:1")?.weights(3, &mut rng);
    assert!((zipf[0] / zipf[1] - 2.0).abs() < 1e-12);
    assert!((zipf[0] / zipf[2] - 3.0).abs() < 1e-12);
    let whale = WeightDist::parse("whale:0.85+zipf:1.1")?.weights(130, &mut rng);
    assert!((whale[0] - 0.85).abs() < 1e-12);
    let pareto = WeightDist::parse("pareto:1.5")?.weights(50, &mut rng);
    for weights in [&uniform, &zipf, &whale, &pareto] {
        assert!((weights.iter().sum::<f64>() - 1.0).abs() < 1e-9);
        assert!(weights.windows(2).all(|pair| pair[0] >= pair[1]));
    }
    assert_eq!(
        WeightDist::parse("whale:0.85+zipf:1.1")?.render(),
        "whale:0.85+zipf:1.1"
    );
    assert_eq!(
        WeightDist::parse("whale:0.5+uniform")?.weights(1, &mut rng),
        vec![1.0]
    );
    Ok(())
}

#[test]
fn malformed_empty_nonfinite_and_out_of_range_distributions_are_refused() {
    for text in [
        "",
        "zipf",
        "zipf:",
        "zipf:x",
        "zipf:0",
        "zipf:-1",
        "zipf:inf",
        "zipf:NaN",
        "zipf:11",
        "pareto:0",
        "pareto:inf",
        "whale:0.85",
        "whale:1+zipf:1",
        "whale:0+zipf:1",
        "whale:0.5+whale:0.5+zipf:1",
        "lognormal:1",
    ] {
        assert!(WeightDist::parse(text).is_err(), "accepted {text:?}");
    }
    for text in [
        "",
        "vardiff",
        "vardiff:0.5",
        "vardiff:inf",
        "vardiff:1e7",
        "hashrate",
    ] {
        assert!(SessionDifficulty::parse(text).is_err(), "accepted {text:?}");
    }
    for text in [
        "",
        "bursty",
        "bursty:cv1=1",
        "bursty:cv1=1,cv60=1",
        "bursty:cv1=1,cv60=1,max=0.5",
        "bursty:cv1=-1,cv60=1,max=2",
        "bursty:cv1=NaN,cv60=1,max=2",
        "bursty:cv1=1,cv60=1,max=2,cv1=1",
        "bursty:cv1=1,cv60=1,max=2,burst=1",
        "poisson",
    ] {
        assert!(Arrival::parse(text).is_err(), "accepted {text:?}");
    }
    assert_eq!(
        Arrival::parse("bursty:max=8,cv60=0.9,cv1=0.6")
            .unwrap()
            .render(),
        "bursty:cv1=0.6,cv60=0.9,max=8"
    );
}

#[test]
fn apportionment_is_exact_and_gives_every_address_a_session_when_it_can() {
    assert_eq!(apportion(10, &[0.5, 0.3, 0.2], false), vec![5, 3, 2]);
    assert_eq!(apportion(10, &[0.85, 0.1, 0.05], true), vec![7, 2, 1]);
    assert_eq!(apportion(2, &[0.85, 0.1, 0.05], false), vec![2, 0, 0]);
    let weights = WeightDist::parse("whale:0.85+zipf:1.1")
        .unwrap()
        .weights(130, &mut Rng::new(1, "w"));
    for total in [130, 131, 400, 2000, 8000] {
        let seats = apportion(total, &weights, true);
        assert_eq!(seats.iter().sum::<usize>(), total);
        assert!(seats.iter().all(|seats| *seats >= 1));
    }
}

// --- population ------------------------------------------------------------

/// A run that names no realism flag gets exactly the sessions every run
/// before them had: one address, `<address>.sNNNNN`, the configured
/// difficulty, the password `x`, round-robin offers and the legacy share-id
/// prefix and window.
#[test]
fn the_default_population_is_the_legacy_one_byte_for_byte() -> Result<()> {
    let args = Args::parse_from(["qbit-prism-load", "--sessions", "7"]);
    args.validate()?;
    let population = Population::build(&args.population_spec()?, "pload1deadbeef")?;
    assert!(population.legacy);
    assert_eq!(population.addresses, vec!["pload1deadbeef".to_owned()]);
    for (index, session) in population.sessions.iter().enumerate() {
        assert_eq!(session.username, format!("pload1deadbeef.s{index:05}"));
        assert_eq!(session.difficulty_multiplier, 1.0);
        assert_eq!(
            run::session_difficulty(1.22e-8, session.difficulty_multiplier),
            (1.22e-8, "x".to_owned())
        );
    }
    assert!(population.uniform_offers());
    assert_eq!(population.share_prefix("pload1deadbeef"), "pload1deadbeef.");
    assert_eq!(population.window_assignments(100), None);
    assert!(!run::OfferPicker::new(Some(&population), 7, "steady_state").is_weighted());
    let clock = args.arrival()?.clock(args.seed, "steady_state", 60);
    assert!(clock.is_smooth());
    for seconds in [0.0, 0.001, 1.5, 59.999, 60.0, 61.2] {
        assert_eq!(clock.elapsed(seconds), seconds);
    }
    // The legacy seeded share is the #264 fixture's.
    let bits = codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    let solution = window::solve_window(bits, 20_000)?;
    let plan = window::SeedPlan::new(
        20_000,
        solution.scaled_share_difficulty,
        solution.scaled_network_difficulty,
        window::DEFAULT_SEED_SHARE_BYTES,
    )?;
    let share = plan.share(6);
    assert_eq!(share.share_id, "seed-1:é000000000006");
    assert!(share.miner_id.starts_with("m1x"));
    assert_eq!(share.order_key, "k");
    assert_eq!(share.p2mr_program_hex, "ab".repeat(32));
    assert!(plan.recipients().is_none());
    Ok(())
}

#[test]
fn named_recipients_skew_sessions_window_shares_and_difficulty_as_asked() -> Result<()> {
    let mut spec = spec(Some(130), "whale:0.85+zipf:1.1", 400);
    spec.difficulty = SessionDifficulty::Vardiff { max_ratio: 1000.0 };
    spec.hashrate_sigma = 1.0;
    let population = Population::build(&spec, "pload1deadbeef")?;
    assert!(!population.legacy);
    assert_eq!(population.addresses.len(), 130);
    assert_eq!(population.addresses[0], "pload1deadbeefr00000");
    assert_eq!(population.addresses[129], "pload1deadbeefr00129");
    assert_eq!(population.sessions.len(), 400);
    assert_eq!(population.share_prefix("pload1deadbeef"), "pload1deadbeefr");
    let mut per = vec![0usize; 130];
    for session in &population.sessions {
        per[session.recipient] += 1;
        assert!(session
            .username
            .starts_with(&format!("{}.s", population.addresses[session.recipient])));
        assert!((1.0..=1000.0).contains(&session.difficulty_multiplier));
        assert!(
            (session.offer_weight - session.hashrate / session.difficulty_multiplier).abs() < 1e-12
        );
    }
    assert!(per.iter().all(|count| *count >= 1));
    assert!(per[0] > 200, "the whale holds most sessions: {}", per[0]);
    let mean = population.sessions.iter().map(|s| s.hashrate).sum::<f64>() / 400.0;
    assert!((mean - 1.0).abs() < 1e-9);
    let spread = population.max_difficulty_multiplier();
    // The average offered share's multiplier, by share rate.
    let rate: f64 = population.sessions.iter().map(|s| s.offer_weight).sum();
    let mean = population
        .sessions
        .iter()
        .map(|s| s.offer_weight * s.difficulty_multiplier)
        .sum::<f64>()
        / rate;
    assert!((population.mean_offered_multiplier() - mean).abs() < 1e-9 * mean);
    assert!(population.mean_offered_multiplier() > 1.0);
    assert_eq!(
        Population::build(
            &PopulationSpec {
                difficulty: SessionDifficulty::Fixed,
                ..spec.clone()
            },
            "p"
        )?
        .mean_offered_multiplier(),
        1.0
    );
    assert!(
        spread > 100.0,
        "three orders of magnitude were asked for: {spread}"
    );
    // Vardiff gives every unclamped session one share rate; only the
    // clamped ones offer faster.
    let clamped = population
        .sessions
        .iter()
        .any(|s| s.difficulty_multiplier == 1000.0);
    assert_eq!(population.uniform_offers(), !clamped);
    let fixed = Population::build(
        &PopulationSpec {
            difficulty: SessionDifficulty::Fixed,
            ..spec.clone()
        },
        "pload1deadbeef",
    )?;
    assert!(
        !fixed.uniform_offers(),
        "fixed difficulty offers by hashrate"
    );
    assert!(run::OfferPicker::new(Some(&fixed), 400, "steady_state").is_weighted());
    assert!(!run::OfferPicker::new(Some(&fixed), 399, "steady_state").is_weighted());
    // The window follows the weights and is reproducible.
    let assignments = population.window_assignments(400_000).expect("recipients");
    assert_eq!(
        Some(assignments.clone()),
        population.window_assignments(400_000)
    );
    let whale = assignments.iter().filter(|r| **r == 0).count() as f64 / 400_000.0;
    assert!(
        (whale - 0.85).abs() < 0.01,
        "whale share of the window {whale}"
    );
    // Same seed, same population; another seed, another jitter.
    assert_eq!(
        Population::build(&spec, "pload1deadbeef")?.sessions,
        population.sessions
    );
    let other = Population::build(&PopulationSpec { seed: 2, ..spec }, "pload1deadbeef")?;
    assert_ne!(other.sessions, population.sessions);
    Ok(())
}

#[test]
fn recipient_windows_carry_the_live_addresses_programs_and_production_size() -> Result<()> {
    let bits = codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    let solution = window::solve_window(bits, 20_000)?;
    let population = Population::build(&spec(Some(20), "zipf:1.1", 100), "pload1deadbeef")?;
    let assignments = population.window_assignments(22_000).expect("recipients");
    let recipients = window::SeedRecipients::new(population.addresses.clone(), assignments);
    let counts = recipients.counts();
    assert_eq!(counts.iter().sum::<u64>(), 22_000);
    assert!(counts.iter().all(|count| *count > 0));
    let plan = window::SeedPlan::with_recipients(
        22_000,
        solution.scaled_share_difficulty,
        solution.scaled_network_difficulty,
        window::DEFAULT_SEED_SHARE_BYTES,
        Some(recipients),
    )?;
    // Share 1 is the one the padding is sized on, as in the legacy seed;
    // a later share's sequence number is a few digits longer.
    assert_eq!(
        serde_json::to_vec(&plan.share(1))?.len(),
        window::DEFAULT_SEED_SHARE_BYTES
    );
    for index in [1, 2, 999, 22_000] {
        let share = plan.share(index);
        assert!(serde_json::to_vec(&share)?.len() <= window::DEFAULT_SEED_SHARE_BYTES + 4);
        assert!(share.share_id.starts_with(window::SEED_SHARE_ID_PREFIX));
        assert!(share.share_id.ends_with(&format!("é{index:012}")));
        assert!(population.addresses.contains(&share.miner_id));
        assert_eq!(share.order_key, share.miner_id);
        // The program the server derives from the fake node's scriptPubKey.
        let script = qbit_prism_load::node::NodeState::payout_script_hex(&share.miner_id);
        assert_eq!(format!("5220{}", share.p2mr_program_hex), script);
    }
    assert!(window::SeedPlan::with_recipients(
        10,
        solution.scaled_share_difficulty,
        solution.scaled_network_difficulty,
        window::DEFAULT_SEED_SHARE_BYTES,
        Some(window::SeedRecipients::new(vec!["a".into()], vec![0; 9])),
    )
    .is_err());
    Ok(())
}

#[test]
fn a_session_difficulty_the_run_cannot_serve_is_refused_before_launch() -> Result<()> {
    let bits = codec::parse_u32_hex(window::TEMPLATE_BITS)?;
    let twenty_k = window::solve_window(bits, 20_000)?;
    run::check_session_difficulties(&twenty_k, 1.0, 1.0)?;
    run::check_session_difficulties(&twenty_k, 300.0, 1.0)?;
    // A 20k window's share is 1/2,500 of a block, so 1,000 times it would
    // find a block every two or three shares.
    let error = run::check_session_difficulties(&twenty_k, 1000.0, 1.0).unwrap_err();
    assert!(format!("{error:#}").contains("eighth of the network difficulty"));
    let four_hundred_k = window::solve_window(bits, 400_000)?;
    run::check_session_difficulties(&four_hundred_k, 1000.0, 1.0)?;
    // Relative to the average share, not the configured floor: a ratio of
    // 1,000 whose average share is 10 times the floor is a 100 times spread
    // above the average, which a 20k window can hold.
    run::check_session_difficulties(&twenty_k, 1000.0, 10.0)?;
    assert_eq!(
        run::live_base_difficulty(&twenty_k, 1.0),
        twenty_k.share_difficulty
    );
    assert_eq!(
        run::live_base_difficulty(&twenty_k, 4.0),
        twenty_k.share_difficulty / 4.0
    );
    // A mean multiplier that takes the configured difficulty below 2^-32
    // is refused, naming how much harder the network must be.
    let error = run::check_session_difficulties(&four_hundred_k, 1000.0, 316.0).unwrap_err();
    assert!(
        format!("{error:#}").contains("--template-bits"),
        "{error:#}"
    );
    let harder = window::solve_window(codec::parse_u32_hex("1d3fffff")?, 400_000)?;
    run::check_session_difficulties(&harder, 1000.0, 316.0)?;
    let (difficulty, password) = run::session_difficulty(twenty_k.share_difficulty, 250.0);
    assert_eq!(password, format!("x,d={difficulty}"));
    assert_eq!(
        password.split_once("d=").unwrap().1.parse::<f64>()?,
        difficulty
    );
    Ok(())
}

#[test]
fn bursty_arrival_is_seeded_capped_and_near_its_mean() -> Result<()> {
    let arrival = Arrival::parse("bursty:cv1=0.6,cv60=0.9,max=8")?;
    let a = arrival.clock(9, "steady_state", 3600);
    let b = arrival.clock(9, "steady_state", 3600);
    let c = arrival.clock(9, "warm_up", 3600);
    assert_eq!(a.elapsed(1234.5), b.elapsed(1234.5));
    assert_ne!(a.elapsed(1234.5), c.elapsed(1234.5));
    let per_second: Vec<f64> = (0..3600)
        .map(|s| a.elapsed(s as f64 + 1.0) - a.elapsed(s as f64))
        .collect();
    assert!(per_second.iter().all(|m| *m >= 0.0 && *m <= 8.0));
    let mean = per_second.iter().sum::<f64>() / 3600.0;
    assert!((0.8..1.1).contains(&mean), "mean multiplier {mean}");
    // Monotone and piecewise linear inside a second.
    assert!(a.elapsed(10.25) >= a.elapsed(10.0) && a.elapsed(10.25) <= a.elapsed(11.0));
    let counts: Vec<u64> = per_second.iter().map(|m| (m * 100.0) as u64).collect();
    let cv1 = realism::windowed_cv(&counts, 1).unwrap();
    assert!(cv1 > 0.8, "a burstier-than-Poisson 1 s CV {cv1}");
    assert!(realism::windowed_cv(&counts, 60).unwrap() > 0.3);
    assert_eq!(realism::windowed_cv(&[1, 2, 3], 60), None);
    assert_eq!(realism::windowed_cv(&[0, 0, 0], 1), None);
    Ok(())
}

#[test]
fn concentration_reports_unknown_as_null_and_measures_skew() {
    assert!(realism::concentration(&[])["top1_share"].is_null());
    assert!(realism::concentration(&[0.0, 0.0])["gini"].is_null());
    let even = realism::concentration(&[1.0; 10]);
    assert!((even["gini"].as_f64().unwrap()).abs() < 1e-12);
    assert!((even["top1_share"].as_f64().unwrap() - 0.1).abs() < 1e-12);
    let skewed = realism::concentration(&[97.0, 1.0, 1.0, 1.0]);
    assert!((skewed["top1_share"].as_f64().unwrap() - 0.97).abs() < 1e-12);
    assert!(skewed["gini"].as_f64().unwrap() > 0.7);
}

// --- flags -----------------------------------------------------------------

#[test]
fn realism_flags_are_validated_at_entry() {
    let parse = |extra: &[&str]| {
        let mut argv = vec!["qbit-prism-load"];
        argv.extend_from_slice(extra);
        Args::try_parse_from(argv)
            .map_err(anyhow::Error::from)
            .and_then(|args| args.validate())
    };
    assert!(parse(&[]).is_ok());
    assert!(parse(&["--recipients", "50", "--recipient-weights", "zipf:1.1"]).is_ok());
    let refused = |extra: &[&str], needle: &str| {
        let error = format!("{:#}", parse(extra).unwrap_err());
        assert!(error.contains(needle), "{extra:?}: {error}");
    };
    refused(&["--recipient-weights", "zipf:1.1"], "needs --recipients");
    refused(&["--recipients", "0"], "--recipients must be");
    refused(&["--recipients", "100000"], "--recipients must be");
    refused(
        &["--session-hashrate-sigma", "NaN"],
        "--session-hashrate-sigma",
    );
    refused(
        &["--session-hashrate-sigma", "6"],
        "--session-hashrate-sigma",
    );
    refused(&["--session-difficulty", "vardiff:0"], "vardiff ratio");
    refused(&["--arrival", "bursty:cv1=1"], "all required");
    refused(&["--plan", "tips", "--warmup-seconds", "0"], "--plan tips");
    refused(&["--plan", "tips", "--external-tips", "0"], "--plan tips");
    refused(&["--plan", "tips", "--mid-flight-kill"], "--plan tips");
    refused(&["--template-bits", "1e7ffff"], "8 hex digits");
    refused(&["--template-bits", "zz7fffff"], "8 hex digits");
    refused(&["--template-bits", "207fffff"], "at least the default");
    refused(&["--template-bits", "1c7fffff"], "at most 1,024");
    assert!(parse(&["--template-bits", "1d3fffff"]).is_ok());
}

#[test]
fn the_tips_plan_is_warm_up_only() -> Result<()> {
    let args = Args::parse_from([
        "qbit-prism-load",
        "--plan",
        "tips",
        "--warmup-seconds",
        "20",
    ]);
    args.validate()?;
    let phases = cli::phases(&args)?;
    assert_eq!(phases.len(), 1);
    assert_eq!(phases[0].name, "warm_up");
    assert!(!phases[0].in_artifact);
    assert_eq!(cli::Plan::parse("tips")?.as_str(), "tips");
    Ok(())
}

// --- presets ---------------------------------------------------------------

#[test]
fn every_checked_in_preset_pins_every_result_flag_and_validates() -> Result<()> {
    let presets = preset::load_all(&preset::presets_dir())?;
    let names: Vec<&str> = presets.iter().map(|p| p.name.as_str()).collect();
    for required in [
        "throughput-20k-window-1fe",
        "throughput-20k-window-1fe-500-addresses",
        "tip-delivery-2000-miners-400k-2fe-retarget",
        "tip-delivery-2000-miners-400k-2fe-retarget-500-addresses",
        "mainnet-shape-130-addresses",
        "mainnet-shape-650-addresses",
        "mainnet-shape-2600-addresses",
        "pr-smoke",
        "rental-churn-bursts-and-storms",
        "short-plan-20k-window-1fe",
        "throughput-200k-window-1fe-async",
        "throughput-400k-window-1fe-async",
        "throughput-400k-window-2fe-async",
        "throughput-400k-window-4fe-async",
        "throughput-400k-window-2fe-sync",
        "throughput-500k-window-1fe-async",
        "throughput-500k-window-2fe-async",
        "throughput-500k-window-4fe-async",
    ] {
        assert!(
            names.contains(&required),
            "{required} is missing: {names:?}"
        );
    }
    let flags = preset::result_flags();
    for flag in [
        "--recipients",
        "--recipient-weights",
        "--seed",
        "--arrival",
        "--plan",
    ] {
        assert!(flags.contains(&flag.to_owned()), "{flag}");
    }
    for loaded in &presets {
        loaded.check_complete()?;
        assert_eq!(loaded.args.len(), flags.len(), "{}", loaded.name);
        let mut argv = vec!["qbit-prism-load".to_owned()];
        argv.extend(loaded.argv()?);
        let args = Args::try_parse_from(&argv)?;
        args.validate()
            .map_err(|error| error.context(format!("preset {}", loaded.name)))?;
        // The population and every phase it asks for can be generated, and
        // every session's difficulty is one the frontends can serve.
        let population = Population::build(&args.population_spec()?, "pload1deadbeef")?;
        run::check_session_difficulties(
            &window::solve_window(args.template_bits()?, args.window_shares)?,
            population.max_difficulty_multiplier(),
            population.mean_offered_multiplier(),
        )
        .map_err(|error| error.context(format!("preset {}", loaded.name)))?;
        assert!(loaded.runner.starts_with("blacksmith-"), "{}", loaded.name);
        // A gated phase is one the preset's plan drives; a soak's phases are
        // its looped presets', planned from its soak block (#575).
        let driven: Vec<String> = match &loaded.soak {
            Some(_) => qbit_prism_load::soak_driver::plan(&args, loaded)?
                .phases
                .into_iter()
                .map(|p| p.plan.name)
                .collect(),
            None => cli::phases(&args)?.into_iter().map(|p| p.name).collect(),
        };
        for phase in loaded.gates.phases.iter().flatten() {
            assert!(
                driven.contains(phase),
                "{}: {phase} is not driven",
                loaded.name
            );
        }
    }
    Ok(())
}

/// The realism presets run mainnet's pool fee, and every legacy preset a
/// 0-bps one, which reproduces its fee-off measurement (#535); the nightly
/// schedule runs on 8 vCPU.
#[test]
fn realism_presets_run_mainnets_fee_and_legacy_ones_do_not() -> Result<()> {
    for loaded in preset::load_all(&preset::presets_dir())? {
        let mut argv = vec!["qbit-prism-load".to_owned()];
        argv.extend(loaded.argv()?);
        let args = Args::try_parse_from(&argv)?;
        let realism = [
            "mainnet-shape-130-addresses",
            "mainnet-shape-650-addresses",
            "mainnet-shape-2600-addresses",
            "rental-churn-bursts-and-storms",
            "soak-weekly",
            "soak-short",
        ]
        .contains(&loaded.name.as_str());
        assert_eq!(
            args.pool_fee_bps,
            if realism { 200 } else { 0 },
            "{}",
            loaded.name
        );
        if loaded.schedule == preset::Schedule::Nightly {
            assert_eq!(
                loaded.runner, "blacksmith-8vcpu-ubuntu-2404",
                "{}",
                loaded.name
            );
        }
    }
    let args = Args::parse_from(["qbit-prism-load"]);
    assert_eq!(args.pool_fee_bps, 0);
    assert!(
        Args::parse_from(["qbit-prism-load", "--pool-fee-bps", "10001"])
            .validate()
            .is_err()
    );
    // #535: every frontend runs a fee, the legacy presets' at 0 bps.
    let address = qbit_prism_load::frontend::pool_fee_address("pload1deadbeef");
    for (bps, expected) in [(0, "0"), (200, "200")] {
        let mut environment = std::collections::BTreeMap::new();
        qbit_prism_load::frontend::apply_pool_fee(&mut environment, bps, &address);
        assert_eq!(environment["PRISM_POOL_FEE_ENABLED"], "1");
        assert_eq!(environment["PRISM_POOL_FEE_BPS"], expected);
        assert_eq!(environment["PRISM_POOL_FEE_ADDRESS"], "pload1deadbeeffee");
    }
    Ok(())
}

/// #473's cells are its exact argument lists at a1937054, which ran before
/// #497 moved the initial-job admission default, with #473's pass rule.
#[test]
fn the_473_cells_pin_its_arguments_admission_and_rule() -> Result<()> {
    for (name, window, frontends, replication, admission, schedule) in [
        (
            "throughput-200k-window-1fe-async",
            200_000,
            1,
            "async",
            2016,
            preset::Schedule::Manual,
        ),
        (
            "throughput-400k-window-1fe-async",
            400_000,
            1,
            "async",
            2016,
            preset::Schedule::Nightly,
        ),
        (
            "throughput-400k-window-2fe-async",
            400_000,
            2,
            "async",
            1016,
            preset::Schedule::Manual,
        ),
        (
            "throughput-400k-window-4fe-async",
            400_000,
            4,
            "async",
            516,
            preset::Schedule::Manual,
        ),
        (
            "throughput-400k-window-2fe-sync",
            400_000,
            2,
            "sync",
            1016,
            preset::Schedule::Manual,
        ),
        (
            "throughput-500k-window-1fe-async",
            500_000,
            1,
            "async",
            2016,
            preset::Schedule::Manual,
        ),
        (
            "throughput-500k-window-2fe-async",
            500_000,
            2,
            "async",
            1016,
            preset::Schedule::Manual,
        ),
        (
            "throughput-500k-window-4fe-async",
            500_000,
            4,
            "async",
            516,
            preset::Schedule::Manual,
        ),
    ] {
        let loaded = preset::Preset::load(&preset::presets_dir().join(format!("{name}.json")))?;
        let mut argv = vec!["qbit-prism-load".to_owned()];
        argv.extend(loaded.argv()?);
        let args = Args::try_parse_from(&argv)?;
        assert_eq!(args.window_shares, window, "{name}");
        assert_eq!(args.frontends, frontends, "{name}");
        assert_eq!(args.replication, replication, "{name}");
        assert_eq!(args.sessions, 2000, "{name}");
        assert_eq!(args.plan, "d1", "{name}");
        assert_eq!(args.forecast_peak_shares_per_second, 2000.0, "{name}");
        assert_eq!(args.ack_p99_limit_ms, 1000.0, "{name}");
        assert_eq!(args.min_mem_available_mib, 6144, "{name}");
        let limits = args.stratum_limits();
        assert_eq!(limits.max_pending_initial_jobs, admission, "{name}");
        assert_eq!(
            limits.pre_flag_max_pending_initial_jobs(),
            admission,
            "{name}"
        );
        assert_eq!(
            args.recipients, None,
            "{name}: one payout address, as #473 ran"
        );
        assert_eq!(args.pool_fee_bps, 0, "{name}: no fee earned, as #473 ran");
        assert!(!args.retarget_bits, "{name}");
        assert_eq!(loaded.schedule, schedule, "{name}");
        let gates = &loaded.gates;
        assert_eq!(
            gates.phases.as_deref(),
            Some(&["steady_state".to_owned()][..])
        );
        assert_eq!(
            (
                gates.max_shortfall,
                gates.max_rejected_valid_shares,
                gates.max_unanswered_submits
            ),
            (0, Some(0), Some(0)),
            "{name}: #473's rule"
        );
        assert!(gates.d1_verdict_table);
    }
    Ok(())
}

/// The test that fails if a preset omits a result flag: remove any one flag
/// from a real preset, or add a flag the harness grew, and the check refuses.
#[test]
fn a_preset_that_omits_a_flag_or_leaves_one_to_a_default_is_refused() -> Result<()> {
    let base =
        preset::Preset::load(&preset::presets_dir().join("mainnet-shape-130-addresses.json"))?;
    for flag in preset::result_flags() {
        let mut missing = base.clone();
        missing.args.remove(&flag);
        let error = format!("{:#}", missing.check_complete().unwrap_err());
        assert!(error.contains(&flag), "{flag}: {error}");
    }
    let mut defaulted = base.clone();
    defaulted
        .args
        .insert("--steady-state-rate".into(), Value::Null);
    assert!(defaulted.check_complete().is_err());
    let mut operational = base.clone();
    operational.args.insert("--out".into(), json!("x"));
    assert!(operational.check_complete().is_err());
    let mut unknown = base.clone();
    unknown.args.insert("--no-such-flag".into(), json!(1));
    assert!(unknown.check_complete().is_err());
    let mut d1 =
        preset::Preset::load(&preset::presets_dir().join("throughput-20k-window-1fe.json"))?;
    d1.args.insert("--burst-seconds".into(), Value::Null);
    assert!(
        d1.check_complete().is_err(),
        "a D1 burst left to the plan default"
    );
    Ok(())
}

#[test]
fn a_preset_flag_cannot_be_given_again_on_the_command_line() -> Result<()> {
    let path = preset::presets_dir().join("pr-smoke.json");
    let (argv, loaded) = preset::expand_command_line(
        [
            "qbit-prism-load",
            "--preset",
            path.to_str().unwrap(),
            "--out",
            "x",
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
    )?;
    assert_eq!(loaded.expect("preset").name, "pr-smoke");
    let args = Args::try_parse_from(&argv)?;
    assert_eq!(args.sessions, 100);
    assert_eq!(args.recipients, Some(20));
    // Any flag the preset pins is refused on the command line, whatever the
    // preset's value: one it sets, one it pins off, one it leaves null.
    for extra in [
        vec!["--sessions", "5"],
        vec!["--mid-flight-kill"],
        vec!["--burst-seconds", "60"],
        vec!["--database-url=postgresql://u@h/db"],
    ] {
        let mut words = vec!["qbit-prism-load", "--preset", path.to_str().unwrap()];
        words.extend(extra.iter().copied());
        let error =
            preset::expand_command_line(words.into_iter().map(Into::into).collect()).unwrap_err();
        assert!(
            format!("{error:#}").contains("is pinned by preset pr-smoke"),
            "{extra:?}: {error:#}"
        );
    }
    let (unchanged, none) =
        preset::expand_command_line(vec!["qbit-prism-load".into(), "--sessions".into()])?;
    assert_eq!(unchanged.len(), 2);
    assert!(none.is_none());
    assert!(
        preset::expand_command_line(vec!["qbit-prism-load".into(), "--preset".into()]).is_err()
    );
    Ok(())
}

// --- gate ------------------------------------------------------------------

fn passing_report() -> Value {
    json!({
        "aborted": null,
        "durability_findings": [],
        "phases": [
            {"name": "warm_up", "shortfall": 0,
             "reconciliation": {"missing": 0, "unexpected": 0},
             "arrival": {"offered_per_second_cv_1s": 1.2, "offered_per_second_cv_60s": null,
                         "offered_max_1s": 90}},
        ],
        "rejections": {"no_response_by_phase": {},
            "by_phase_reason_and_message": [
                {"phase": "warm_up", "class": "expected", "count": 4}]},
        "time_to_usable_work": {"tips": [
            {"all_sessions_milliseconds": 900.0},
            {"all_sessions_milliseconds": 1100.0},
            {"all_sessions_milliseconds": 1000.0},
        ]},
        "topology": {"sessions": 100},
        "population": {"recipients": 20,
            "live_accepted_work_per_recipient_concentration": {"top1_share": 0.6, "top10_share": 0.95},
            "session_difficulty_spread_orders_of_magnitude": 2.0},
    })
}

#[test]
fn the_gate_passes_a_clean_run_and_fails_each_way_a_run_can_fall_short() {
    let budgets = gate::Budgets {
        phases: None,
        max_shortfall: 0,
        max_rejected_valid_shares: None,
        max_unanswered_submits: None,
        tip_last_notify_p99_ms: Some(5000.0),
        d1_verdict_table: false,
        churn_tip_last_notify_p99_ms: None,
        new_session_first_job_p99_ms: None,
    };
    let clean = gate::evaluate(&passing_report(), Some(0), &budgets);
    assert!(gate::passed(&clean), "{}", gate::markdown("clean", &clean));
    let table = gate::markdown("clean", &clean);
    assert!(
        table.contains("PASS") && table.contains("1100 ms over 3 tips"),
        "{table}"
    );

    let fails = |report: Value, exit: Option<i32>, budgets: &gate::Budgets, needle: &str| {
        let checks = gate::evaluate(&report, exit, budgets);
        assert!(!gate::passed(&checks), "{needle} passed");
        let table = gate::markdown("x", &checks);
        let failed: Vec<&gate::Check> = checks.iter().filter(|c| c.pass == Some(false)).collect();
        assert!(
            failed.iter().any(|c| c.name.contains(needle)),
            "{needle}: {table}"
        );
    };
    fails(passing_report(), Some(5), &budgets, "exit code");
    fails(passing_report(), None, &budgets, "exit code");
    let mut aborted = passing_report();
    aborted["aborted"] = json!("MemAvailable fell");
    fails(aborted, Some(0), &budgets, "run completed");
    let mut lost = passing_report();
    lost["durability_findings"] = json!([{"kind": "acknowledged share missing"}]);
    fails(lost, Some(0), &budgets, "durability");
    let mut missing = passing_report();
    missing["phases"][0]["reconciliation"]["missing"] = json!(1);
    fails(missing, Some(0), &budgets, "reconciliation");
    // A commit whose answer the drain window cut off is explained, and the
    // harness exits 0 for it: reported, not failed.
    let mut explained = passing_report();
    explained["phases"][0]["reconciliation"]["unexpected"] = json!(1);
    explained["no_response_commits"] = json!({"count": 1});
    let checks = gate::evaluate(&explained, Some(0), &budgets);
    assert!(gate::passed(&checks), "{}", gate::markdown("x", &checks));
    assert!(gate::markdown("x", &checks).contains("| 1 (1 / 0 / 0) |"));
    let mut short = passing_report();
    short["phases"][0]["shortfall"] = json!(3);
    fails(short, Some(0), &budgets, "shortfall");
    let mut unserved = passing_report();
    unserved["time_to_usable_work"]["tips"][1] = json!({"all_sessions_milliseconds": null,
        "all_sessions_unavailable_reason": "4 of 100 sessions got no usable work"});
    fails(unserved, Some(0), &budgets, "tip to last");
    let mut none = passing_report();
    none["time_to_usable_work"]["tips"] = json!([]);
    fails(none, Some(0), &budgets, "tip to last");
    let tight = gate::Budgets {
        tip_last_notify_p99_ms: Some(1000.0),
        ..budgets.clone()
    };
    fails(passing_report(), Some(0), &tight, "tip to last");
    let mut unreported = passing_report();
    unreported["phases"][0]
        .as_object_mut()
        .unwrap()
        .remove("shortfall");
    fails(unreported, Some(0), &budgets, "shortfall");
}

#[test]
fn nearest_rank_percentiles_are_observed_values() {
    assert_eq!(gate::nearest_rank(&[], 0.99), None);
    assert_eq!(gate::nearest_rank(&[3.0], 0.99), Some(3.0));
    let hundred: Vec<f64> = (1..=100).map(f64::from).collect();
    assert_eq!(gate::nearest_rank(&hundred, 0.99), Some(99.0));
    assert_eq!(gate::nearest_rank(&hundred, 0.5), Some(50.0));
    assert_eq!(gate::nearest_rank(&[5.0, 1.0, 9.0], 0.99), Some(9.0));
}

#[test]
fn the_473_rule_gates_only_the_named_phases_and_each_of_its_three_figures() {
    let mut report = passing_report();
    report["phases"] = json!([
        {"name": "steady_state", "shortfall": 0, "rejected_valid_shares": 0,
         "reconciliation": {"missing": 0, "unexpected": 0}},
        {"name": "burst", "shortfall": 26_490, "rejected_valid_shares": 3,
         "reconciliation": {"missing": 0, "unexpected": 0}},
    ]);
    let rule = gate::Budgets {
        phases: Some(vec!["steady_state".into()]),
        max_shortfall: 0,
        max_rejected_valid_shares: Some(0),
        max_unanswered_submits: Some(0),
        tip_last_notify_p99_ms: None,
        d1_verdict_table: true,
        churn_tip_last_notify_p99_ms: None,
        new_session_first_job_p99_ms: None,
    };
    let checks = gate::evaluate(&report, Some(0), &rule);
    assert!(gate::passed(&checks), "{}", gate::markdown("x", &checks));
    assert!(checks
        .iter()
        .any(|c| c.name.starts_with("burst:") && c.pass.is_none()));
    // Each of the three figures fails the gated phase on its own.
    for (key, value) in [("shortfall", json!(1)), ("rejected_valid_shares", json!(1))] {
        let mut failing = report.clone();
        failing["phases"][0][key] = value;
        assert!(
            !gate::passed(&gate::evaluate(&failing, Some(0), &rule)),
            "{key}"
        );
    }
    let mut unanswered = report.clone();
    unanswered["rejections"]["no_response_by_phase"] = json!({"steady_state": 2});
    assert!(!gate::passed(&gate::evaluate(&unanswered, Some(0), &rule)));
    // A null gives no pass.
    let mut unreported = report.clone();
    unreported["phases"][0]
        .as_object_mut()
        .unwrap()
        .remove("rejected_valid_shares");
    assert!(!gate::passed(&gate::evaluate(&unreported, Some(0), &rule)));
    // A gated phase the run did not drive fails rather than passing vacuously.
    let absent = gate::Budgets {
        phases: Some(vec!["slow_database".into()]),
        ..rule.clone()
    };
    assert!(!gate::passed(&gate::evaluate(&report, Some(0), &absent)));
}

#[test]
fn the_d1_table_has_473s_columns_precision_and_verdict_words() {
    assert_eq!(gate::number(499.997, Some(500.0)), "499.997");
    assert_eq!(gate::number(499.4, Some(500.0)), "499.4");
    assert_eq!(gate::number(500.0, Some(500.0)), "500");
    assert_eq!(gate::number(1558.5, None), "1,558.5");
    assert_eq!(gate::number(26_490.0, None), "26,490");
    assert_eq!(gate::number(1_234_567.0, None), "1,234,567");
    assert_eq!(gate::number(20.54, None), "20.5");
    let report = json!({
        "topology": {"frontends": 1, "plan": "d1"},
        "window": {"requested_window_shares": 400_000},
        "database": {"replication": {"declared": "async"}},
        "validator": {"ack_p99_limit_used_milliseconds": 1000.0},
        "dense_cadence": {"ran": false},
        "rejections": {"no_response_by_phase": {},
            "by_phase_reason_and_message": [
                {"phase": "steady_state", "class": "expected", "count": 193},
                {"phase": "steady_state", "class": "backend", "count": 1}]},
        "phases": [
            {"name": "steady_state", "target_rate_shares_per_second": 500.0,
             "offered_rate_shares_per_second": 499.996_666, "achieved_rate_shares_per_second": 499.4,
             "shortfall": 0, "rejected_valid_shares": 1, "scheduled_blocks": 0,
             "client_ack_latency": {"p99": 18.5},
             "order_lock": {"max_waiters": 15, "mean_waiters": 0.0812}},
            {"name": "burst", "target_rate_shares_per_second": 2000.0,
             "offered_rate_shares_per_second": 1558.5, "achieved_rate_shares_per_second": 1558.5,
             "shortfall": 26_490, "rejected_valid_shares": 0, "scheduled_blocks": 0,
             "client_ack_latency": {"p99": 1886.0},
             "order_lock": {"max_waiters": 15, "mean_waiters": 14.41}},
        ],
    });
    let table = gate::d1_table(&report, Some(0), "throughput-400k-window-1fe-async");
    assert!(
        table.contains(&format!("| {} |", gate::D1_COLUMNS.join(" | "))),
        "{table}"
    );
    assert!(table.contains(
        "| 400k | 1 | async | d1 | 1 | 500 | 499.997 | 499.4 | 0 | 1 | 0 | 193 | **not met** (0 of 1): \
         valid shares refused in 1 of 1 repeats, at most 1 per run | 18.5 | 1 of 1 (limit 1,000 ms) \
         | 15 / 0.08 | – |"
    ), "{table}");
    assert!(table.contains(
        "| 400k | 1 | async | d1 | 1 | 2,000 | 1,558.5 | 1,558.5 | 26,490 | 0 | 0 | 0 | **not met** \
         (0 of 1): shortfall in 1 of 1 repeats, at most 26,490 per run | 1,886 | 0 of 1 (limit \
         1,000 ms) | 15 / 14.41 | – |"
    ), "{table}");
    for row in table.lines().filter(|line| line.starts_with("| 400k")) {
        assert_eq!(
            row.matches(" | ").count() + 1,
            gate::D1_COLUMNS.len(),
            "{row}"
        );
    }
    let refused = gate::d1_table(&report, Some(5), "throughput-400k-window-1fe-async");
    assert!(
        refused.contains("no verdict: no run in the medians"),
        "{refused}"
    );
    assert!(
        refused.contains("throughput-400k-window-1fe-async (exit 5)"),
        "{refused}"
    );
    for row in refused.lines().filter(|line| line.starts_with("| 400k")) {
        assert_eq!(
            row.matches(" | ").count() + 1,
            gate::D1_COLUMNS.len(),
            "{row}"
        );
    }
}

//! The `faults` phase's verdict (#554): each fault's windows, the shares
//! offered in them, its evidence and its checks, computed after the run from
//! the same records the reconciliation reads.

use super::frontend::SUBMIT_RPC_TIMEOUT;
use super::*;

/// One pass line of a fault.
#[derive(Clone, Debug, Serialize)]
pub struct Check {
    pub name: String,
    pub pass: bool,
    pub detail: String,
}

fn check(name: &str, pass: bool, detail: impl Into<String>) -> Check {
    Check {
        name: name.into(),
        pass,
        detail: detail.into(),
    }
}

/// What the verdict reads beside the driver's records.
pub struct EvalInputs<'a> {
    pub submits: &'a [SubmitRecord],
    pub collected: &'a Collected,
    /// The share ids PostgreSQL holds, for a kill's census.
    pub committed: &'a std::collections::BTreeSet<String>,
    /// A real node's `MintPurpose::Fault` tips as its pool node saw them,
    /// in the order asked; empty on the fake node, whose mint is known at
    /// once.
    pub fault_mints: &'a [TipChange],
    pub sessions: usize,
    /// The phase's offers per second, and its first second.
    pub per_second: &'a crate::run::PerSecond,
    pub phase_started: Instant,
    pub ack_p99_limit_ms: f64,
    pub read_tier: Option<&'a [read_tier::ReadSample]>,
    /// The public process reads the standby, which the exhaustion of the
    /// primary's slots does not touch.
    pub read_tier_on_replica: bool,
    pub instance_ids: &'a [String],
}

/// How the shares offered in a window fared.
#[derive(Clone, Debug, Default, Serialize)]
pub struct WindowShares {
    pub offered: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub no_response: usize,
    pub rejection_reasons: BTreeMap<String, usize>,
    pub ack_milliseconds: measure::LatencySummary,
    pub shortfall: u64,
    pub tokens: u64,
}

/// `PRISM_SUBMIT_TIP_MAX_AGE_SECONDS`'s default, which the harness leaves
/// as it is: how long a frontend accepts shares on work whose refresh has
/// stalled.
pub const SUBMIT_TIP_MAX_AGE: Duration = Duration::from_secs(10);

/// Send-to-answer latency of every answered submit sent in the window,
/// accepted or refused.
fn answer_latency(inputs: &EvalInputs<'_>, from: Instant, to: Instant) -> measure::LatencySummary {
    measure::summarize(
        inputs
            .submits
            .iter()
            .filter(|record| {
                record.phase == PHASE && !record.reoffer && record.sent >= from && record.sent < to
            })
            .filter(|record| !matches!(record.outcome, Outcome::NoResponse { .. }))
            .filter_map(|record| record.latency_millis)
            .collect(),
        measure::MILLISECONDS,
        "client monotonic, submit sent to answer",
    )
}

fn window_shares(inputs: &EvalInputs<'_>, from: Instant, to: Instant) -> WindowShares {
    let mut window = WindowShares::default();
    let mut latencies = Vec::new();
    for record in inputs.submits.iter().filter(|record| {
        record.phase == PHASE && !record.reoffer && record.sent >= from && record.sent < to
    }) {
        window.offered += 1;
        match &record.outcome {
            Outcome::Accepted => {
                window.accepted += 1;
                if let Some(latency) = record.latency_millis {
                    latencies.push(latency);
                }
            }
            Outcome::Rejected(rejection) => {
                window.rejected += 1;
                let reason = rejection
                    .reason_id
                    .clone()
                    .unwrap_or_else(|| rejection.message.clone());
                *window.rejection_reasons.entry(reason).or_default() += 1;
            }
            Outcome::NoResponse { .. } => window.no_response += 1,
        }
    }
    window.ack_milliseconds = measure::summarize(
        latencies,
        measure::MILLISECONDS,
        "client monotonic, submit sent to answer",
    );
    let second =
        |at: Instant| at.saturating_duration_since(inputs.phase_started).as_secs() as usize;
    for index in second(from)..second(to) {
        let tokens = inputs.per_second.tokens.get(index).copied().unwrap_or(0);
        let dispatched = inputs
            .per_second
            .dispatched
            .get(index)
            .copied()
            .unwrap_or(0);
        window.tokens += tokens;
        window.shortfall += tokens.saturating_sub(dispatched);
    }
    window
}

impl FaultDriver {
    /// Every fault's row and the phase's verdict, for the side report.
    pub fn evaluate(&self, inputs: &EvalInputs<'_>) -> Value {
        let origin = self.started;
        let at = |instant: Option<Instant>| {
            instant.map(|at| at.saturating_duration_since(origin).as_secs_f64())
        };
        let mut rows = Vec::new();
        let mut all_pass = !self.runs.is_empty();
        let mut outages: Vec<(String, Instant, Instant)> = Vec::new();
        let mut public_exclusions: Vec<(Instant, Instant)> = Vec::new();
        for run in &self.runs {
            let end = run.recovery_end.unwrap_or_else(Instant::now);
            match &run.action {
                Action::SigtermDrain(drain) => {
                    if let (Some(index), Some(from)) = (drain.signalled, drain.signalled_at) {
                        outages.push((
                            inputs.instance_ids[index].clone(),
                            from,
                            drain.ready_at.unwrap_or(end),
                        ));
                    }
                }
                Action::RollingRestart(rolling) => {
                    for (index, from, to) in rolling.outages(end) {
                        outages.push((inputs.instance_ids[index].clone(), from, to));
                    }
                }
                Action::FrontendSigkill(kill) => {
                    if let (Some(index), Some(from)) = (kill.killed_index, kill.kill_started_at) {
                        outages.push((
                            inputs.instance_ids[index].clone(),
                            from,
                            kill.relaunched_at.unwrap_or(end),
                        ));
                    }
                }
                Action::PoolExhaustion { .. } if !inputs.read_tier_on_replica => {
                    // A full primary denies a public reader on it too: its
                    // answers then are not the read tier's independence. One
                    // on the standby is held to them throughout.
                    if let (Some(from), Some(to)) = (run.inject_start, run.removed_at) {
                        public_exclusions.push((from, to + Duration::from_secs(5)));
                    }
                }
                Action::WalDiskFull(_) | Action::Failover(_) => {
                    // A public reader that requires a replica refuses, by
                    // design, while its standby has no stream to prove it
                    // current (the primary down, the link cut) or has been
                    // promoted; and one on the primary is refused with it.
                    // Excused from the fault's start until it answers again,
                    // for at most PUBLIC_RETURN after the fault's removal;
                    // the fault's row says how long that took.
                    if let (Some(from), Some(to)) = (run.inject_start, run.removed_at) {
                        let back = public_back(inputs.read_tier, to);
                        public_exclusions.push((from, back.unwrap_or(to + PUBLIC_RETURN)));
                    }
                }
                Action::CandidateBacklog(backlog) => {
                    for (index, from, to) in backlog.outage_windows(end) {
                        outages.push((inputs.instance_ids[index].clone(), from, to));
                    }
                }
                _ => {}
            }
        }
        for run in &self.runs {
            let (row, pass) = self.evaluate_run(run, inputs, &outages, &public_exclusions);
            all_pass &= pass;
            rows.push(row);
        }
        let submits_per_hash = {
            let mut counts: BTreeMap<String, usize> = BTreeMap::new();
            for submit in self.tools.relay.submits() {
                if submit.forwarded_at.is_some() {
                    *counts.entry(submit.block_hash).or_default() += 1;
                }
            }
            counts
        };
        let offered_twice: Vec<&String> = submits_per_hash
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(hash, _)| hash)
            .collect();
        all_pass &= offered_twice.is_empty();
        let read_tier = inputs.read_tier.map(|samples| {
            let end = self
                .runs
                .iter()
                .filter_map(|run| run.recovery_end)
                .max()
                .unwrap_or_else(Instant::now);
            read_tier::summarize(samples, origin, end, &outages, &public_exclusions)
        });
        if let Some(verdict) = &read_tier {
            all_pass &= verdict["pass"] == true;
        }
        json!({
            "phase": PHASE,
            "plan": self.plan.report(),
            "faults": rows,
            "blocks_offered_through_the_relay": submits_per_hash.len(),
            "blocks_offered_more_than_once": offered_twice,
            "read_tier_over_the_phase": read_tier,
            "planned_outages": outages.iter().map(|(instance, from, to)| json!({
                "frontend": instance,
                "from_seconds": at(Some(*from)),
                "to_seconds": at(Some(*to)),
            })).collect::<Vec<_>>(),
            "passed": all_pass,
            "note": "Each fault's checks are its pass criteria from #554's plan; a check whose \
                     figure could not be measured fails with the reason. The run's own contract \
                     still applies on top: every acknowledged share in PostgreSQL, and no \
                     committed share without an answer mid-run.",
        })
    }

    fn evaluate_run(
        &self,
        run: &FaultRun,
        inputs: &EvalInputs<'_>,
        outages: &[(String, Instant, Instant)],
        public_exclusions: &[(Instant, Instant)],
    ) -> (Value, bool) {
        let origin = self.started;
        let at = |instant: Option<Instant>| {
            instant.map(|at| at.saturating_duration_since(origin).as_secs_f64())
        };
        let mut checks: Vec<Check> = Vec::new();
        let mut evidence = json!({});
        let finished = run.recovery_end.is_some();
        checks.push(check(
            "completed",
            finished && run.problems.is_empty(),
            if run.problems.is_empty() {
                if finished {
                    "injected, removed and recovered within the phase".to_owned()
                } else {
                    "did not finish".to_owned()
                }
            } else {
                run.problems.join("; ")
            },
        ));
        let (Some(baseline_start), Some(inject_start), Some(removed), Some(end)) = (
            run.baseline_start,
            run.inject_start,
            run.removed_at,
            run.recovery_end,
        ) else {
            let row = json!({
                "ordinal": run.ordinal,
                "fault": run.kind.name(),
                "checks": checks,
                "pass": false,
            });
            return (row, false);
        };
        let baseline = window_shares(inputs, baseline_start, inject_start);
        let during = window_shares(inputs, inject_start, removed);
        let tail_start = removed + (end.saturating_duration_since(removed)) / 2;
        let tail = window_shares(inputs, tail_start, end);
        // Recovery: the second half of the recovery window offers the full
        // rate again, with acknowledgements back near the baseline's.
        let tail_limit = inputs
            .ack_p99_limit_ms
            .max(baseline.ack_milliseconds.p99.unwrap_or(0.0) * 2.0);
        let recovered = match tail.ack_milliseconds.p99 {
            Some(p99) => tail.shortfall == 0 && tail.accepted > 0 && p99 <= tail_limit,
            None => false,
        };
        checks.push(check(
            "recovered",
            recovered,
            format!(
                "second half of the recovery window: {} accepted, shortfall {}, ACK p99 {:?} ms \
                 against max(--ack-p99-limit-ms, 2 x baseline p99) = {tail_limit:.1} ms",
                tail.accepted, tail.shortfall, tail.ack_milliseconds.p99
            ),
        ));
        match &run.action {
            Action::SlowDatabase {
                observed_ms, error, ..
            } => {
                let floor = crate::run::delay_floor_millis(self.tools.slow_db_delay_ms);
                checks.push(check(
                    "delay on the frontends' path",
                    observed_ms.is_some_and(|median| median >= floor),
                    format!(
                        "SELECT 1 through the frontends' URL took {observed_ms:?} ms against a \
                         floor of {floor:.1} ms{}",
                        error
                            .as_deref()
                            .map(|e| format!(" ({e})"))
                            .unwrap_or_default()
                    ),
                ));
                evidence = json!({
                    "delay_one_way_milliseconds": self.tools.slow_db_delay_ms,
                    "observed_select1_median_milliseconds": observed_ms,
                });
            }
            Action::SettlementLock {
                minted, tip, stall, ..
            } => {
                let injected = run.injected_at.unwrap_or(inject_start);
                let before_tip = window_shares(inputs, injected, minted.unwrap_or(removed));
                // Shares take ORDER_LOCK, never SETTLEMENT_LOCK, so none may
                // wait on the holder: every answer in the hold, accepted or
                // refused, comes back as fast as the run's ACK limit. The
                // whole hold, not only up to the mint: a refresh of the
                // minted tip that held ORDER_LOCK while it queued for the
                // holder would stall the shares only after it.
                let answers = answer_latency(inputs, injected, removed);
                checks.push(check(
                    "no share waited on the lock holder",
                    answers
                        .p99
                        .is_some_and(|p99| p99 <= inputs.ack_p99_limit_ms),
                    format!(
                        "answer p99 {:?} ms, max {:?} ms over {} answers in the hold, against \
                         --ack-p99-limit-ms {}",
                        answers.p99, answers.max, answers.samples, inputs.ack_p99_limit_ms
                    ),
                ));
                // Until the refresh has been stalled for the submit tip's
                // maximum age, retained work is still provably current and
                // its shares are accepted. Past it the server refuses the
                // shares it cannot prove current, as designed (#525); those
                // refusals are counted in the shares block, not failed.
                let fresh_until = (injected + SUBMIT_TIP_MAX_AGE).min(minted.unwrap_or(removed));
                let fresh = window_shares(inputs, injected, fresh_until);
                let ratio =
                    (fresh.offered > 0).then(|| fresh.accepted as f64 / fresh.offered as f64);
                checks.push(check(
                    "shares on retained work accepted while the stall is within the tip's max age",
                    ratio.is_some_and(|ratio| ratio >= 0.9),
                    format!(
                        "{} of {} offered in the first {:.1} s of the hold were accepted (refusals: \
                         {:?})",
                        fresh.accepted,
                        fresh.offered,
                        fresh_until.saturating_duration_since(injected).as_secs_f64(),
                        fresh.rejection_reasons
                    ),
                ));
                // The tip minted halfway through the hold: a fake node's
                // mint is known at once; a real node's is the first of the
                // fault's own mints the pool node saw after the request, never
                // just the next tip change, which a keepalive or a found
                // block can be.
                let tip = tip.clone().or_else(|| {
                    minted.and_then(|minted| {
                        inputs
                            .fault_mints
                            .iter()
                            .find(|change| change.monotonic >= minted)
                            .cloned()
                    })
                });
                // A tip that reached the pool node only after the release
                // was never gated by the lock: no session could have had
                // work on it during the hold, so a zero would pass vacuously.
                let arrived_in_hold = tip.as_ref().is_some_and(|tip| tip.monotonic < removed);
                let (served_during_hold, all_served_after_release) = match &tip {
                    Some(tip) if arrived_in_hold => {
                        let mut first: BTreeMap<usize, Instant> = BTreeMap::new();
                        for sighting in &inputs.collected.tips {
                            if sighting.tip == tip.hash && sighting.session < inputs.sessions {
                                let slot = first.entry(sighting.session).or_insert(sighting.at);
                                if sighting.at < *slot {
                                    *slot = sighting.at;
                                }
                            }
                        }
                        let during = first.values().filter(|at| **at < removed).count();
                        let all = (first.len() == inputs.sessions)
                            .then(|| first.values().max().copied())
                            .flatten()
                            .map(|last| last.saturating_duration_since(removed).as_secs_f64());
                        (Some(during), all)
                    }
                    _ => (None, None),
                };
                checks.push(check(
                    "the tip's jobs waited for the release",
                    served_during_hold == Some(0),
                    match (served_during_hold, &tip) {
                        (Some(count), _) => format!(
                            "{count} sessions got work on the tip minted during the hold before \
                             the lock was released"
                        ),
                        (None, Some(_)) => "the tip minted during the hold reached the pool node \
                                            only after the release, so the lock's effect on job \
                                            issuance was not observed"
                            .into(),
                        (None, None) => "no tip was minted during the hold, so the lock's effect \
                                         on job issuance was not observed"
                            .into(),
                    },
                ));
                checks.push(check(
                    "job issuance back within 10 s of the release",
                    all_served_after_release.is_some_and(|seconds| seconds <= 10.0),
                    match all_served_after_release {
                        Some(seconds) => format!(
                            "every session had work on the tip {seconds:.2} s after the release"
                        ),
                        None => "not every session got work on the tip".into(),
                    },
                ));
                let stall_samples = stall
                    .as_ref()
                    .map(StallSampler::samples)
                    .unwrap_or_default();
                let peak_during: Option<f64> = stall_samples
                    .iter()
                    .filter(|(_, at, _)| *at >= injected && *at < removed)
                    .filter_map(|(_, _, value)| *value)
                    .fold(None, |peak, value| {
                        Some(peak.map_or(value, |p: f64| p.max(value)))
                    });
                let after_recovery: Vec<(String, Option<f64>)> = inputs
                    .instance_ids
                    .iter()
                    .map(|instance| {
                        let last = stall_samples
                            .iter()
                            .rfind(|(name, at, _)| name == instance && *at >= removed)
                            .and_then(|(_, _, value)| *value);
                        (instance.clone(), last)
                    })
                    .collect();
                evidence = json!({
                    "hold_seconds": removed.saturating_duration_since(injected).as_secs_f64(),
                    "tip_minted_after_seconds": at(*minted),
                    "tip": tip.as_ref().map(|tip| &tip.hash),
                    "ack_milliseconds_during_hold": during.ack_milliseconds,
                    "ack_milliseconds_before_the_tip": before_tip.ack_milliseconds,
                    "shares_before_the_tip": before_tip,
                    "work_refresh_stalled_seconds_peak_during_hold": peak_during,
                    "work_refresh_stalled_seconds_last_after_release": after_recovery,
                    "work_refresh_stalled_note": "Sampled from each frontend's /metrics every \
                        second; recorded as evidence of how the stall is reported, not gated.",
                });
            }
            Action::PoolExhaustion {
                outcome,
                acquires_before,
                acquires_after,
                ..
            } => {
                let outcome = outcome.clone().unwrap_or_default();
                checks.push(check(
                    "every connection slot taken",
                    outcome.saturated_at.is_some(),
                    outcome
                        .refusal
                        .clone()
                        .unwrap_or_else(|| "PostgreSQL never refused a connection".into()),
                ));
                checks.push(check(
                    "a frontend had to reconnect into the full server",
                    !outcome.terminated.is_empty(),
                    format!(
                        "{} idle frontend backends terminated",
                        outcome.terminated.len()
                    ),
                ));
                let delta =
                    |before: &Option<Spawned<Vec<(String, String)>>>,
                     after: &Option<Spawned<Vec<(String, String)>>>| {
                        let read = |spawned: &Option<Spawned<Vec<(String, String)>>>| {
                            spawned
                                .as_ref()
                                .and_then(|s| s.value.clone())
                                .unwrap_or_default()
                        };
                        let (before, after) = (read(before), read(after));
                        before
                            .iter()
                            .map(|(instance, text)| {
                                let later = after
                                    .iter()
                                    .find(|(name, _)| name == instance)
                                    .map(|(_, text)| text.as_str())
                                    .unwrap_or_default();
                                let count = |text: &str, outcome: &str| {
                                    metric_sum(
                                        text,
                                        "database_pool_acquire_seconds_count",
                                        Some(&format!("outcome=\"{outcome}\"")),
                                    )
                                };
                                let outcomes: Vec<Value> = ACQUIRE_OUTCOMES
                                    .iter()
                                    .map(|outcome| {
                                        let delta =
                                            match (count(text, outcome), count(later, outcome)) {
                                                (Some(b), Some(a)) => Some(a - b),
                                                (None, Some(a)) => Some(a),
                                                _ => None,
                                            };
                                        json!({"outcome": outcome, "delta": delta})
                                    })
                                    .collect();
                                json!({
                                    "frontend": instance,
                                    "acquire_outcomes_delta": outcomes,
                                })
                            })
                            .collect::<Vec<_>>()
                    };
                evidence = json!({
                    "exhaustion": outcome.report(origin),
                    "frontend_pool_acquires": delta(acquires_before, acquires_after),
                    "ack_milliseconds_during": during.ack_milliseconds,
                });
            }
            Action::SigtermDrain(drain) => {
                checks.push(check(
                    "a found block's offer was in flight at the SIGTERM",
                    drain.seen.is_some(),
                    match &drain.seen {
                        Some(seen) => format!("{} held at the relay", seen.block_hash),
                        None => "no submitblock reached the relay".into(),
                    },
                ));
                let exit_seconds = match (drain.signalled_at, drain.exited_at) {
                    (Some(from), Some(to)) => {
                        Some(to.saturating_duration_since(from).as_secs_f64())
                    }
                    _ => None,
                };
                checks.push(check(
                    "exited 0 within the shutdown bound",
                    drain.exit_success == Some(true)
                        && !drain.forced_kill
                        && exit_seconds.is_some_and(|seconds| seconds <= 35.0),
                    format!(
                        "status {:?} {exit_seconds:?} s after SIGTERM (bound 35 s)",
                        drain.exit_status
                    ),
                ));
                checks.push(check(
                    "the shutdown waited for the offer",
                    drain.waits_logged == Some(true) && drain.gave_up_logged == Some(false),
                    format!(
                        "logged {SHUTDOWN:?}: {:?}; logged the budget ALERT: {:?}",
                        drain.waits_logged,
                        drain.gave_up_logged,
                        SHUTDOWN = frontend::SHUTDOWN_WAITS_LINE
                    ),
                ));
                let sent = drain.seen.as_ref().map(|seen| {
                    self.tools
                        .relay
                        .submits()
                        .iter()
                        .filter(|submit| {
                            submit.block_hash == seen.block_hash && submit.forwarded_at.is_some()
                        })
                        .count()
                });
                checks.push(check(
                    "the block reached the node exactly once",
                    sent == Some(1),
                    format!("forwarded {sent:?} times"),
                ));
                checks.push(check(
                    "the node's answer was recorded before the process exited",
                    drain
                        .row_after_exit
                        .as_ref()
                        .is_some_and(|row| row.offer_outcome.as_deref() == Some("accepted")),
                    format!(
                        "candidate row after the exit: {:?}{}",
                        drain.row_after_exit,
                        drain
                            .row_error
                            .as_deref()
                            .map(|e| format!(" ({e})"))
                            .unwrap_or_default()
                    ),
                ));
                // The settle the next fault waited for (#686): without it a
                // drain whose block never landed would pass, and the next
                // fault would measure the landing's revision bump instead.
                checks.push(check(
                    "the block landed and every frontend served work at its revision",
                    drain.current_at.is_some(),
                    drain.settle_detail(),
                ));
                evidence = drain.evidence(origin);
            }
            Action::RollingRestart(rolling) => {
                let turns = &rolling.turns;
                let clean = turns.len() >= 2
                    && turns.iter().all(|turn| {
                        turn.exit_success == Some(true)
                            && !turn.forced_kill
                            && turn.exited_at.is_some_and(|exited| {
                                exited.saturating_duration_since(turn.signalled_at)
                                    <= Duration::from_secs(35)
                            })
                    });
                checks.push(check(
                    "every frontend exited 0 within the shutdown bound",
                    clean,
                    format!("{} turns", turns.len()),
                ));
                // Each turn's moved sessions were working on the other
                // frontend again within 30 s of the SIGTERM.
                let mut late = Vec::new();
                for turn in turns {
                    for session in &turn.sessions_moved {
                        let back = inputs.collected.opened.iter().any(|opened| {
                            opened.session == *session
                                && opened.frontend == turn.moved_to
                                && opened.ready > turn.signalled_at
                                && opened.ready.saturating_duration_since(turn.signalled_at)
                                    <= Duration::from_secs(30)
                        });
                        if !back {
                            late.push(*session);
                        }
                    }
                }
                checks.push(check(
                    "moved sessions had work on the serving frontend within 30 s",
                    late.is_empty(),
                    format!("{} sessions did not", late.len()),
                ));
                evidence = rolling.evidence(origin);
            }
            Action::FrontendSigkill(kill) => {
                // Held: killed inside its own submitblock deadline, so the
                // answer really died with it rather than timing out first.
                let kill_after_send = kill
                    .seen
                    .as_ref()
                    .zip(kill.kill_started_at)
                    .map(|(seen, killed)| killed.saturating_duration_since(seen.at));
                // Accepted, not merely answered: a rejection or an RPC error
                // is an answer too.
                let accepted = kill.seen.as_ref().is_some_and(|seen| {
                    self.tools.relay.submits().iter().any(|submit| {
                        submit.block_hash == seen.block_hash && submit.node_accepted()
                    })
                });
                checks.push(check(
                    "the killed frontend held a found block the node had accepted",
                    kill.node_answered_at.is_some()
                        && accepted
                        && kill_after_send.is_some_and(|after| after < SUBMIT_RPC_TIMEOUT),
                    format!(
                        "offer seen: {}; node answered before the kill: {}; accepted: {accepted}; \
                         killed {:?} ms after the call, against the frontend's {} ms submitblock \
                         deadline",
                        kill.seen.is_some(),
                        kill.node_answered_at.is_some(),
                        kill_after_send.map(|after| after.as_millis()),
                        SUBMIT_RPC_TIMEOUT.as_millis()
                    ),
                ));
                let sent = kill.seen.as_ref().map(|seen| {
                    self.tools
                        .relay
                        .submits()
                        .iter()
                        .filter(|submit| {
                            submit.block_hash == seen.block_hash && submit.forwarded_at.is_some()
                        })
                        .count()
                });
                checks.push(check(
                    "the block reached the node exactly once",
                    sent == Some(1),
                    format!("forwarded {sent:?} times"),
                ));
                checks.push(check(
                    "the block landed without a second offer",
                    kill.landed.is_some(),
                    match &kill.landed {
                        Some((_, row)) => format!("landed {row:?}"),
                        None => format!(
                            "not landed within the {} s lease wait; last row {:?}",
                            self.tools.lease_wait_seconds, kill.last_row
                        ),
                    },
                ));
                evidence = kill.evidence(origin, self.tools.lease_wait_seconds);
                // The kill's census, as the mid-flight kill reports its own:
                // each re-offer's answer against PostgreSQL, possible losses
                // named (reported, not gated, by the same rule).
                evidence["census"] = crate::run::mid_flight_census(
                    &kill.indeterminate,
                    inputs.submits,
                    inputs.committed,
                );
            }
            Action::ReconnectStorm(storm) => {
                let departed = storm.departed_at.unwrap_or(inject_start);
                let mut late = Vec::new();
                let mut delays = Vec::new();
                for (position, session) in storm.stormed.iter().enumerate() {
                    let returned = departed + storm.returns[position];
                    let ready = inputs
                        .collected
                        .opened
                        .iter()
                        .filter(|opened| opened.session == *session && opened.ready > departed)
                        .map(|opened| opened.ready)
                        .min();
                    match ready {
                        Some(ready) => {
                            let seconds = ready.saturating_duration_since(returned).as_secs_f64();
                            delays.push(seconds * 1000.0);
                            if ready.saturating_duration_since(returned)
                                > frontend::STORM_WORK_BUDGET
                            {
                                late.push(*session);
                            }
                        }
                        None => late.push(*session),
                    }
                }
                checks.push(check(
                    "every stormed session had work again within its budget",
                    !storm.stormed.is_empty() && late.is_empty(),
                    format!(
                        "{} of {} stormed sessions late or never back (budget {:?} after return)",
                        late.len(),
                        storm.stormed.len(),
                        frontend::STORM_WORK_BUDGET
                    ),
                ));
                evidence = json!({
                    "sessions_stormed": storm.stormed.len(),
                    "return_to_work_milliseconds": measure::summarize(
                        delays,
                        measure::MILLISECONDS,
                        "harness monotonic, the session's return to its first job",
                    ),
                });
            }
            Action::Failover(failover) => {
                let (new_checks, row_evidence) =
                    self.evaluate_failover(failover, baseline_start, inputs);
                checks.extend(new_checks);
                evidence = row_evidence;
            }
            Action::WalDiskFull(full) => {
                let (new_checks, row_evidence) =
                    evaluate_wal_disk_full(full, baseline_start, end, inputs, origin);
                checks.extend(new_checks);
                evidence = row_evidence;
            }
            Action::CandidateBacklog(backlog) => {
                let (new_checks, row_evidence) = self.evaluate_backlog(backlog, origin);
                checks.extend(new_checks);
                evidence = row_evidence;
            }
        }
        let read_tier = inputs.read_tier.map(|samples| {
            read_tier::summarize(samples, inject_start, end, outages, public_exclusions)
        });
        if let Some(verdict) = &read_tier {
            checks.push(check(
                "read tier answered throughout",
                verdict["pass"] == true,
                format!(
                    "public API {}, frontend /metrics {}",
                    verdict["public_api"]["pass"], verdict["frontend_metrics"]["pass"]
                ),
            ));
        }
        if matches!(run.action, Action::WalDiskFull(_) | Action::Failover(_)) {
            if let Some(samples) = inputs.read_tier {
                let back = public_back(Some(samples), removed);
                checks.push(check(
                    "the public API answered again within 30 s of the fault's removal",
                    back.is_some(),
                    match back {
                        Some(at) => format!(
                            "first 2xx {:.2} s after the removal (it refuses, by design, while its \
                             replica cannot show it is current)",
                            at.saturating_duration_since(removed).as_secs_f64()
                        ),
                        None => format!("no 2xx within {PUBLIC_RETURN:?} of the removal"),
                    },
                ));
            }
        }
        let pass = checks.iter().all(|check| check.pass);
        let row = json!({
            "ordinal": run.ordinal,
            "fault": run.kind.name(),
            "gap_seconds": run.gap_seconds,
            "baseline_start_seconds": at(run.baseline_start),
            "inject_start_seconds": at(run.inject_start),
            "in_effect_seconds": at(run.injected_at),
            "removed_seconds": at(run.removed_at),
            "recovery_end_seconds": at(run.recovery_end),
            "shares": {
                "baseline": baseline,
                "during": during,
                "recovery_second_half": tail,
            },
            "evidence": evidence,
            "read_tier": read_tier,
            "checks": checks,
            "pass": pass,
        });
        (row, pass)
    }
}

/// How soon after a database fault's removal the public API must answer
/// again: its standby's stream reconnects within PostgreSQL's 5 s
/// `wal_retrieve_retry_interval`, and its readiness probe follows.
pub const PUBLIC_RETURN: Duration = Duration::from_secs(30);

/// The first 2xx public answer at or after `after`, within
/// [`PUBLIC_RETURN`].
fn public_back(samples: Option<&[read_tier::ReadSample]>, after: Instant) -> Option<Instant> {
    samples?
        .iter()
        .filter(|sample| {
            sample.target == "public-api"
                && sample.path != "/metrics"
                && sample.ok()
                && sample.at >= after
                && sample.at <= after + PUBLIC_RETURN
        })
        .map(|sample| sample.at)
        .min()
}

/// Each frontend's first accepted share answered after `after`, and whether
/// it came within `bound` of `since`.
fn served_again(
    inputs: &EvalInputs<'_>,
    frontends: usize,
    after: Instant,
    since: Instant,
    bound: Duration,
) -> (bool, Vec<Value>) {
    let mut all = frontends > 0;
    let mut rows = Vec::new();
    for index in 0..frontends {
        let first = inputs
            .submits
            .iter()
            .filter(|record| {
                record.frontend == index
                    && matches!(record.outcome, Outcome::Accepted)
                    && record.responded.is_some_and(|at| at > after)
            })
            .filter_map(|record| record.responded)
            .min();
        let seconds = first.map(|at| at.saturating_duration_since(since).as_secs_f64());
        let within = first.is_some_and(|at| at.saturating_duration_since(since) <= bound);
        all &= within;
        rows.push(json!({
            "frontend": inputs.instance_ids.get(index),
            "first_accepted_share_after_seconds": seconds,
        }));
    }
    (all, rows)
}

/// A database or landing fault's own steps all ran: a step it could not
/// carry out is a failed check, not a note in its evidence.
fn injector_steps(problems: &[String]) -> Check {
    check(
        "the injector carried out every step",
        problems.is_empty(),
        if problems.is_empty() {
            "every step ran".to_owned()
        } else {
            problems.join("; ")
        },
    )
}

/// Whether every frontend kept its process: the same pid before and after,
/// and each known.
fn same_processes(before: &[Option<u32>], after: &[Option<u32>]) -> bool {
    !before.is_empty() && before == after && before.iter().all(Option::is_some)
}

impl FaultDriver {
    fn evaluate_failover(
        &self,
        failover: &failover::Failover,
        from: Instant,
        inputs: &EvalInputs<'_>,
    ) -> (Vec<Check>, Value) {
        use failover::Mode;
        let mut checks = Vec::new();
        let origin = self.started;
        let acknowledged = failover.acknowledged(inputs.submits, from);
        let lost = failover.lost(inputs.submits, inputs.committed, from);
        checks.push(check(
            "the standby was promoted and the writer endpoint moved to it",
            failover.promoted_at.is_some() && failover.moved_at.is_some(),
            format!(
                "promoted: {}; endpoint moved: {}",
                failover.promoted_at.is_some(),
                failover.moved_at.is_some()
            ),
        ));
        let (served, first_shares) = match (failover.promoted_at, failover.moved_at) {
            (Some(promoted), Some(moved)) => served_again(
                inputs,
                inputs.instance_ids.len(),
                moved,
                promoted,
                failover::SERVE_BOUND,
            ),
            _ => (false, Vec::new()),
        };
        let kept = same_processes(&failover.pids_before, &failover.pids_after);
        checks.push(check(
            "every frontend served on the new primary within 30 s of the promotion, without a \
             restart",
            served && kept,
            format!(
                "first accepted share per frontend after the promotion: {first_shares:?}; same \
                 processes: {kept}"
            ),
        ));
        checks.push(check(
            "a new standby streams from the new primary",
            failover.rebuilt_at.is_some(),
            match failover.rebuilt_at {
                Some(at) => format!(
                    "streaming {:.1} s after the promotion",
                    failover.promoted_at.map_or(0.0, |promoted| at
                        .saturating_duration_since(promoted)
                        .as_secs_f64())
                ),
                None => "no standby was rebuilt".into(),
            },
        ));
        let before_barrier: Vec<&str> = lost
            .iter()
            .filter(|record| {
                failover
                    .barrier_at
                    .zip(record.responded)
                    .is_none_or(|(barrier, answered)| answered <= barrier)
            })
            .map(|record| record.share_id.as_str())
            .collect();
        match failover.mode {
            Mode::Fenced => {
                checks.push(check(
                    "the fenced switch lost no acknowledged share",
                    lost.is_empty() && failover.gap_bytes.is_some_and(|gap| gap <= 0),
                    format!(
                        "{} of {} acknowledged shares missing on the new primary; the standby \
                         was {:?} WAL bytes behind the fenced primary at promotion",
                        lost.len(),
                        acknowledged.len(),
                        failover.gap_bytes
                    ),
                ));
            }
            Mode::Async | Mode::Block => {
                checks.push(check(
                    "no share acknowledged before the barrier was lost",
                    failover.barrier_at.is_some() && before_barrier.is_empty(),
                    format!(
                        "{} lost shares were acknowledged before the standby was shown to hold \
                         the primary's flushed WAL: {:?}",
                        before_barrier.len(),
                        before_barrier.iter().take(20).collect::<Vec<_>>()
                    ),
                ));
                let outside: Vec<&str> = lost
                    .iter()
                    .filter(|record| !failover.in_gap(record))
                    .map(|record| record.share_id.as_str())
                    .collect();
                checks.push(check(
                    "every lost share lies after the standby's last received position",
                    outside.is_empty(),
                    format!(
                        "{} of {} lost shares were acknowledged after the barrier and absent \
                         from the standby when it was promoted (each listed in the evidence); \
                         outside the gap: {:?}",
                        lost.len() - outside.len(),
                        lost.len(),
                        outside.iter().take(20).collect::<Vec<_>>()
                    ),
                ));
                let dropped: Vec<&String> = failover
                    .frozen_present
                    .iter()
                    .filter(|share| !inputs.committed.contains(*share))
                    .collect();
                checks.push(check(
                    "promotion kept every share the standby held",
                    failover.frozen_read && dropped.is_empty(),
                    if failover.frozen_read {
                        format!(
                            "{} of the {} acknowledged shares the standby held before promotion \
                             are missing from the new primary",
                            dropped.len(),
                            failover.frozen_present.len()
                        )
                    } else {
                        "the standby could not be read before promotion, so what it held is \
                         unknown and no loss is shown to lie in the gap"
                            .into()
                    },
                ));
            }
        }
        if failover.mode == Mode::Async {
            checks.push(check(
                "the replication cut left an unreplicated interval",
                failover.gap_bytes.is_some_and(|gap| gap > 0) && !lost.is_empty(),
                format!(
                    "{:?} WAL bytes the standby never received; {} acknowledged shares lost with \
                     them",
                    failover.gap_bytes,
                    lost.len()
                ),
            ));
        }
        if failover.mode == Mode::Block {
            checks.push(check(
                "a found block's submitblock was held when replication was cut",
                failover.seen.is_some(),
                match &failover.seen {
                    Some(seen) => format!("{} held at the relay", seen.block_hash),
                    None => "no submitblock reached the relay".into(),
                },
            ));
            checks.push(check(
                "its reservation was on the promoted primary",
                matches!(&failover.row_at_promotion, Some(Some(_))),
                format!(
                    "row on the new primary at the promotion: {:?} (#529's standby wait keeps \
                     the reservation on the standby before the call)",
                    failover.row_at_promotion
                ),
            ));
            let sent = failover.seen.as_ref().map(|seen| {
                self.tools
                    .relay
                    .submits()
                    .iter()
                    .filter(|submit| {
                        submit.block_hash == seen.block_hash && submit.forwarded_at.is_some()
                    })
                    .count()
            });
            checks.push(check(
                "the block reached the node exactly once",
                sent == Some(1),
                format!("forwarded {sent:?} times"),
            ));
            let landed_after = failover
                .landed
                .as_ref()
                .zip(failover.promoted_at)
                .map(|((landed, _), promoted)| landed.saturating_duration_since(promoted));
            let outcome = failover
                .landed
                .as_ref()
                .and_then(|(_, row)| row.offer_outcome.clone());
            checks.push(check(
                "the block landed within 30 s of the promotion, its outcome unknown or accepted",
                landed_after.is_some_and(|after| after <= failover::SERVE_BOUND)
                    && matches!(outcome.as_deref(), Some("unknown") | Some("accepted")),
                format!(
                    "landed {:?} s after the promotion with outcome {outcome:?}; last row {:?}",
                    landed_after.map(|after| after.as_secs_f64()),
                    failover.last_row
                ),
            ));
        }
        checks.push(injector_steps(&failover.problems));
        let evidence = failover.evidence(origin, &lost, acknowledged.len());
        (checks, evidence)
    }

    fn evaluate_backlog(
        &self,
        backlog: &backlog::CandidateBacklog,
        origin: Instant,
    ) -> (Vec<Check>, Value) {
        let mut checks = Vec::new();
        let unfinished_before = backlog
            .before
            .values()
            .filter(|state| !backlog::terminal(state))
            .count();
        checks.push(check(
            "a backlog of found blocks was pending when the frontends stopped",
            unfinished_before >= self.tools.backlog,
            format!(
                "{unfinished_before} unfinished candidates of {} refused blocks (target {})",
                backlog.before.len(),
                self.tools.backlog
            ),
        ));
        let clean = !backlog.exits.is_empty()
            && backlog.exits.iter().all(|exit| {
                exit.success == Some(true)
                    && !exit.forced_kill
                    && exit.after_sigterm_seconds.is_some_and(|s| s <= 35.0)
            });
        checks.push(check(
            "every frontend exited 0 within the shutdown bound",
            clean,
            format!("{} exits", backlog.exits.len()),
        ));
        let not_terminal: Vec<(&String, Option<&String>)> = backlog
            .before
            .keys()
            .map(|hash| (hash, backlog.after.get(hash)))
            .filter(|(_, state)| !state.is_some_and(|state| backlog::terminal(state)))
            .collect();
        checks.push(check(
            "every backlog row reached a terminal state after the heal",
            backlog.settled_at.is_some() && not_terminal.is_empty(),
            format!("not terminal: {not_terminal:?}"),
        ));
        let submits = self.tools.relay.submits();
        let twice: Vec<&String> = backlog
            .before
            .keys()
            .filter(|hash| {
                submits
                    .iter()
                    .filter(|submit| &submit.block_hash == *hash && submit.forwarded_at.is_some())
                    .count()
                    > 1
            })
            .collect();
        checks.push(check(
            "no backlog block reached the node twice",
            twice.is_empty(),
            format!("forwarded more than once: {twice:?}"),
        ));
        // Settled is not enough: rows abandoned at the relaunch would settle
        // too. The backlog's blocks share a parent, so one of them must
        // land, through one offer.
        let landed: Vec<&String> = backlog
            .before
            .keys()
            .filter(|hash| backlog.after.get(*hash).map(String::as_str) == Some("submitted"))
            .collect();
        let landed_once = landed.iter().all(|hash| {
            submits
                .iter()
                .filter(|submit| &submit.block_hash == *hash && submit.forwarded_at.is_some())
                .count()
                == 1
        });
        checks.push(check(
            "a backlog block landed after the heal, offered once",
            !landed.is_empty() && landed_once,
            format!(
                "{} backlog rows submitted (each forwarded once: {landed_once}); the rest: {:?}",
                landed.len(),
                backlog
                    .after
                    .iter()
                    .filter(|(_, state)| state.as_str() != "submitted")
                    .collect::<Vec<_>>()
            ),
        ));
        checks.push(check(
            "every row counted before the restart was settled after it",
            !backlog.before.is_empty()
                && backlog
                    .before
                    .keys()
                    .all(|hash| backlog.after.contains_key(hash)),
            format!(
                "{} rows before the restart, {} of them read after the heal",
                backlog.before.len(),
                backlog
                    .before
                    .keys()
                    .filter(|hash| backlog.after.contains_key(*hash))
                    .count()
            ),
        ));
        checks.push(injector_steps(&backlog.problems));
        (checks, backlog.evidence(origin))
    }
}

fn evaluate_wal_disk_full(
    full: &disk::WalDiskFull,
    from: Instant,
    end: Instant,
    inputs: &EvalInputs<'_>,
    origin: Instant,
) -> (Vec<Check>, Value) {
    let mut checks = Vec::new();
    checks.push(check(
        "PostgreSQL stopped on the full WAL volume",
        full.down_at.is_some() && full.ballast_bytes.is_some(),
        format!(
            "ballast {:?} bytes; down {:?} s after the fill",
            full.ballast_bytes,
            full.filled_at
                .zip(full.down_at)
                .map(|(filled, down)| down.saturating_duration_since(filled).as_secs_f64())
        ),
    ));
    let acknowledged: Vec<&SubmitRecord> = inputs
        .submits
        .iter()
        .filter(|record| {
            !record.reoffer
                && matches!(record.outcome, Outcome::Accepted)
                && record.responded.is_some_and(|at| at >= from && at < end)
        })
        .collect();
    let lost: Vec<&str> = acknowledged
        .iter()
        .filter(|record| !inputs.committed.contains(&record.share_id))
        .map(|record| record.share_id.as_str())
        .collect();
    checks.push(check(
        "no acknowledged share was lost",
        lost.is_empty(),
        format!(
            "{} of {} shares acknowledged around the fault missing from PostgreSQL: {:?}",
            lost.len(),
            acknowledged.len(),
            lost.iter().take(20).collect::<Vec<_>>()
        ),
    ));
    // PostgreSQL's own record of when it was down; an answer within the
    // grace of a PANIC may be for a commit made just before it.
    let intervals = &full.down_intervals;
    // An answer read inside an interval for a share PostgreSQL holds is a
    // commit made before the PANIC whose answer arrived late; one PostgreSQL
    // does not hold was acknowledged with nothing durable behind it.
    let acked_down: Vec<&str> = acknowledged
        .iter()
        .filter(|record| !inputs.committed.contains(&record.share_id))
        .filter(|record| {
            record.responded.is_some_and(|at| {
                intervals
                    .iter()
                    .any(|(from, to)| at >= *from + disk::ANSWER_GRACE && at < *to)
            })
        })
        .map(|record| record.share_id.as_str())
        .collect();
    let down_seconds: f64 = intervals
        .iter()
        .map(|(from, to)| to.saturating_duration_since(*from).as_secs_f64())
        .sum();
    checks.push(check(
        "no share was acknowledged while PostgreSQL was down",
        !intervals.is_empty() && acked_down.is_empty(),
        format!(
            "{} shares acknowledged, and absent from PostgreSQL, inside the {} intervals \
             ({down_seconds:.1} s) PostgreSQL's log shows it down, from {} ms after each PANIC: \
             {:?}",
            acked_down.len(),
            intervals.len(),
            disk::ANSWER_GRACE.as_millis(),
            acked_down.iter().take(20).collect::<Vec<_>>()
        ),
    ));
    let down_window = full.down_at.zip(full.up_at);
    let (served, first_shares) = match full.up_at {
        Some(up) => served_again(
            inputs,
            inputs.instance_ids.len(),
            up,
            up,
            failover::SERVE_BOUND,
        ),
        None => (false, Vec::new()),
    };
    let kept = same_processes(&full.pids_before, &full.pids_after);
    checks.push(check(
        "every frontend accepted shares again within 30 s, without a restart",
        served && kept,
        format!(
            "first accepted share per frontend after PostgreSQL returned: {first_shares:?}; same \
             processes: {kept}"
        ),
    ));
    let samples = full.collector_samples();
    // The collector runs every 10 s, so its gauge can lag the outage by a
    // cycle: read from the fill to one cycle after PostgreSQL's return.
    let shown: Vec<(String, bool)> = inputs
        .instance_ids
        .iter()
        .map(|instance| {
            let seen = down_window.is_some_and(|(down, up)| {
                samples.iter().any(|(name, at, value)| {
                    name == instance
                        && *at >= down
                        && *at < up + disk::COLLECTOR_LAG
                        && *value == Some(0.0)
                })
            });
            (instance.clone(), seen)
        })
        .collect();
    checks.push(check(
        "every frontend's /metrics showed the database collector unavailable (#575's paging \
         condition)",
        !shown.is_empty() && shown.iter().all(|(_, seen)| *seen),
        format!(
            "qbit_prism_collector_available{{collector=\"database\"}} == 0 during the outage: \
             {shown:?}"
        ),
    ));
    checks.push(injector_steps(&full.problems));
    let mut evidence = full.evidence(origin);
    evidence["acknowledged_while_down"] = json!(acked_down.len());
    evidence["collector_unavailable_shown"] = json!(shown
        .iter()
        .map(|(instance, seen)| json!({"frontend": instance, "shown": seen}))
        .collect::<Vec<_>>());
    evidence["first_accepted_after_return"] = json!(first_shares);
    (checks, evidence)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(at: Instant, target: &str, status: Option<u16>) -> read_tier::ReadSample {
        read_tier::ReadSample {
            target: target.into(),
            path: "/public/v1/blocks".into(),
            at,
            millis: 1.0,
            status,
            error: None,
        }
    }

    #[test]
    fn the_public_api_is_back_at_its_first_2xx_within_the_bound() {
        let removed = Instant::now();
        let seconds = |s: u64| removed + Duration::from_secs(s);
        let samples = vec![
            sample(seconds(1), "public-api", Some(503)),
            sample(seconds(2), "load-fe-0", Some(200)),
            sample(seconds(3), "public-api", Some(200)),
            sample(seconds(4), "public-api", Some(200)),
        ];
        assert_eq!(public_back(Some(&samples), removed), Some(seconds(3)));
        let late = vec![sample(
            removed + PUBLIC_RETURN + Duration::from_secs(1),
            "public-api",
            Some(200),
        )];
        assert_eq!(public_back(Some(&late), removed), None);
        assert_eq!(public_back(None, removed), None);
    }

    #[test]
    fn a_frontend_that_changed_process_did_not_serve_without_a_restart() {
        assert!(same_processes(&[Some(1), Some(2)], &[Some(1), Some(2)]));
        assert!(!same_processes(&[Some(1), Some(2)], &[Some(1), Some(3)]));
        assert!(!same_processes(&[Some(1), None], &[Some(1), None]));
        assert!(!same_processes(&[], &[]));
    }
}

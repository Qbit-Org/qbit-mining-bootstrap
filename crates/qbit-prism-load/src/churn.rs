//! The connection-churn model (#521): rentals and reconnect storms.
//!
//! Production does not hold a fixed set of sessions. Hash rental arrives as
//! a burst of sessions that connect within seconds, mines for a
//! heavy-tailed lifetime and leaves by dropping its sockets, and a rental
//! failing over between pools drops and reconnects a large fraction of its
//! sessions at once. Mainnet does not record connection velocity (there is
//! no session table and connects are not logged), so the shape is a
//! parameter sweep: every parameter is a flag, every flag defaults to off,
//! and a preset pins them all.
//!
//! The model runs in its own side phase, `churn`, after `slow_database` and
//! with no proxy delay, so the artifact phases are unchanged and the phase
//! measures delivery under churn rather than a delayed database. Inside it:
//!
//! - **Rental bursts.** Burst `i` connects `sizes[i]` rental sessions, each at
//!   a seeded uniform offset within the burst window after the burst's start.
//!   Rental sessions mine for one address (the heaviest recipient, or the
//!   run's address) at `hashrate` times a mean base session.
//! - **Lifetimes.** Each rental session stays for a seeded draw from the
//!   lifetime distribution, then closes its socket abruptly: no quiesce, its
//!   outstanding submits left to the server. A lifetime past the phase's end
//!   is cut by the phase's end, which quiesces as every phase end does.
//! - **Reconnect storms.** Storm `j` drops a seeded `fractions[j]` of the
//!   sessions then connected, base and rental, abruptly, and each reconnects
//!   after a seeded uniform delay up to the storm's reconnect window.
//! - **Tips.** `tips` external tips are minted evenly through the phase, so
//!   delivery is measured while sessions come and go.
//!
//! EP-VALIDATION: every parameter is parsed and range-checked at entry; the
//! plan the run drives is generated once, from the seed, before the phase.

use crate::realism::Rng;
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};

/// The side phase's name.
pub const PHASE: &str = "churn";

/// The most rental sessions one run may plan, over every burst.
pub const MAX_RENTAL_SESSIONS: usize = 50_000;

fn number(text: &str, what: &str) -> Result<f64> {
    let value: f64 = text
        .trim()
        .parse()
        .with_context(|| format!("{what} {text:?} is not a number"))?;
    ensure!(value.is_finite(), "{what} must be finite, not {text:?}");
    Ok(value)
}

/// `key=value,...` with exactly `keys`, each once.
fn pairs(text: &str, keys: &[&str], what: &str) -> Result<Vec<f64>> {
    let mut values: Vec<Option<f64>> = vec![None; keys.len()];
    for pair in text.split(',') {
        let (key, value) = pair
            .split_once('=')
            .with_context(|| format!("{what} {text:?}: {pair:?} is not key=value"))?;
        let index = keys
            .iter()
            .position(|k| *k == key.trim())
            .with_context(|| format!("{what} {text:?}: unknown key {:?}", key.trim()))?;
        ensure!(
            values[index].is_none(),
            "{what} {text:?}: {} is given twice",
            keys[index]
        );
        values[index] = Some(number(value, keys[index])?);
    }
    values
        .into_iter()
        .zip(keys)
        .map(|(value, key)| value.with_context(|| format!("{what} {text:?}: {key} is required")))
        .collect()
}

/// A heavy-tailed positive distribution, in seconds or sessions.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Tail {
    /// Pareto with scale `xm` and shape `alpha`, capped at `max`.
    Pareto { xm: f64, alpha: f64, max: f64 },
    /// Lognormal with median `median` and log-sigma `sigma`, capped at `max`.
    Lognormal { median: f64, sigma: f64, max: f64 },
}

impl Tail {
    pub fn parse(text: &str, what: &str) -> Result<Self> {
        let text = text.trim();
        let tail = if let Some(rest) = text.strip_prefix("pareto:") {
            let v = pairs(rest, &["xm", "alpha", "max"], what)?;
            Self::Pareto {
                xm: v[0],
                alpha: v[1],
                max: v[2],
            }
        } else if let Some(rest) = text.strip_prefix("lognormal:") {
            let v = pairs(rest, &["median", "sigma", "max"], what)?;
            Self::Lognormal {
                median: v[0],
                sigma: v[1],
                max: v[2],
            }
        } else {
            bail!(
                "unknown {what} {text:?}; use pareto:xm=<x>,alpha=<a>,max=<m> or \
                 lognormal:median=<x>,sigma=<s>,max=<m>"
            );
        };
        let (scale, shape, max) = match tail {
            Self::Pareto { xm, alpha, max } => (xm, alpha, max),
            Self::Lognormal { median, sigma, max } => (median, sigma, max),
        };
        ensure!(scale > 0.0, "{what}: the scale must be positive");
        ensure!(
            shape > 0.0 && shape <= 20.0,
            "{what}: the shape must be above 0 and at most 20"
        );
        ensure!(max >= scale, "{what}: max must be at least the scale");
        Ok(tail)
    }

    pub fn render(&self) -> String {
        match self {
            Self::Pareto { xm, alpha, max } => format!("pareto:xm={xm},alpha={alpha},max={max}"),
            Self::Lognormal { median, sigma, max } => {
                format!("lognormal:median={median},sigma={sigma},max={max}")
            }
        }
    }

    pub fn draw(&self, rng: &mut Rng) -> f64 {
        match *self {
            Self::Pareto { xm, alpha, max } => {
                (xm * (1.0 - rng.next_f64()).powf(-1.0 / alpha)).min(max)
            }
            Self::Lognormal { median, sigma, max } => {
                (median * (sigma * rng.normal()).exp()).min(max)
            }
        }
    }
}

/// Burst sizes: an explicit sweep, or `count` seeded draws.
#[derive(Clone, Debug, PartialEq)]
pub enum BurstSizes {
    None,
    List(Vec<usize>),
    Drawn { tail: Tail, count: usize },
}

impl BurstSizes {
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if text == "none" {
            return Ok(Self::None);
        }
        if let Some((tail, count)) = text.split_once(";count=") {
            let tail = Tail::parse(tail, "--rental-bursts")?;
            let count = count
                .trim()
                .parse::<usize>()
                .context("--rental-bursts count is not a whole number")?;
            ensure!(
                (1..=1000).contains(&count),
                "--rental-bursts count must be 1..1000"
            );
            return Ok(Self::Drawn { tail, count });
        }
        let sizes = text
            .split(',')
            .map(|size| {
                size.trim()
                    .parse::<usize>()
                    .with_context(|| format!("--rental-bursts size {size:?} is not a whole number"))
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            sizes.iter().all(|size| *size > 0),
            "--rental-bursts sizes must be positive"
        );
        Ok(Self::List(sizes))
    }

    pub fn render(&self) -> String {
        match self {
            Self::None => "none".into(),
            Self::List(sizes) => sizes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
            Self::Drawn { tail, count } => format!("{};count={count}", tail.render()),
        }
    }

    pub fn sizes(&self, rng: &mut Rng) -> Vec<usize> {
        match self {
            Self::None => Vec::new(),
            Self::List(sizes) => sizes.clone(),
            Self::Drawn { tail, count } => (0..*count)
                .map(|_| tail.draw(rng).round().max(1.0) as usize)
                .collect(),
        }
    }

    /// The most sessions the sizes can add, for the connection cap.
    pub fn upper_bound(&self) -> usize {
        match self {
            Self::None => 0,
            Self::List(sizes) => sizes.iter().sum(),
            Self::Drawn { tail, count } => {
                let max = match tail {
                    Tail::Pareto { max, .. } | Tail::Lognormal { max, .. } => *max,
                };
                max.round() as usize * count
            }
        }
    }
}

/// Reconnect-storm fractions, in order.
pub fn parse_storms(text: &str) -> Result<Vec<f64>> {
    let text = text.trim();
    if text == "none" {
        return Ok(Vec::new());
    }
    text.split(',')
        .map(|fraction| {
            let fraction = number(fraction, "a --reconnect-storms fraction")?;
            ensure!(
                fraction > 0.0 && fraction <= 1.0,
                "a --reconnect-storms fraction must be above 0 and at most 1, not {fraction}"
            );
            Ok(fraction)
        })
        .collect()
}

/// Every churn flag, parsed and checked.
#[derive(Clone, Debug, PartialEq)]
pub struct ChurnSpec {
    pub seconds: u64,
    pub tips: usize,
    pub bursts: BurstSizes,
    pub burst_window_seconds: f64,
    pub burst_interval_seconds: f64,
    pub lifetime: Tail,
    pub rental_hashrate: f64,
    pub storms: Vec<f64>,
    pub storm_interval_seconds: f64,
    pub storm_reconnect_seconds: f64,
    pub seed: u64,
}

impl ChurnSpec {
    pub fn is_on(&self) -> bool {
        self.seconds > 0
    }

    /// The plan the phase drives: every rental's arrival and departure, and
    /// every storm, on the phase's clock.
    pub fn plan(&self) -> ChurnPlan {
        if !self.is_on() {
            return ChurnPlan::default();
        }
        let mut sizes_rng = Rng::new(self.seed, "churn-burst-sizes");
        let mut arrivals_rng = Rng::new(self.seed, "churn-arrivals");
        let mut lifetimes_rng = Rng::new(self.seed, "churn-lifetimes");
        let phase = self.seconds as f64;
        let mut rentals = Vec::new();
        for (burst, size) in self.bursts.sizes(&mut sizes_rng).into_iter().enumerate() {
            let start = 5.0 + burst as f64 * self.burst_interval_seconds;
            if start >= phase {
                break;
            }
            for _ in 0..size {
                let arrive = start + arrivals_rng.next_f64() * self.burst_window_seconds;
                let lifetime = self.lifetime.draw(&mut lifetimes_rng);
                rentals.push(RentalPlan {
                    burst,
                    arrive_at: arrive.min(phase),
                    depart_at: (arrive + lifetime < phase).then_some(arrive + lifetime),
                    lifetime_seconds: lifetime,
                });
            }
        }
        let storms = self
            .storms
            .iter()
            .enumerate()
            .map(|(index, fraction)| StormPlan {
                index,
                at: 5.0
                    + self.burst_window_seconds
                    + index as f64 * self.storm_interval_seconds
                    + self.storm_interval_seconds / 2.0,
                fraction: *fraction,
            })
            .filter(|storm| storm.at < phase)
            .collect();
        let tips = (0..self.tips)
            .map(|index| phase * (index as f64 + 1.0) / (self.tips as f64 + 1.0))
            .collect();
        ChurnPlan {
            rentals,
            storms,
            tips,
        }
    }
}

/// One rental session's schedule.
#[derive(Clone, Debug, PartialEq)]
pub struct RentalPlan {
    pub burst: usize,
    /// Seconds into the phase.
    pub arrive_at: f64,
    /// Seconds into the phase; `None` when the lifetime outlasts the phase.
    pub depart_at: Option<f64>,
    pub lifetime_seconds: f64,
}

/// One reconnect storm.
#[derive(Clone, Debug, PartialEq)]
pub struct StormPlan {
    pub index: usize,
    pub at: f64,
    pub fraction: f64,
}

/// What the churn phase will drive, generated before it starts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChurnPlan {
    pub rentals: Vec<RentalPlan>,
    pub storms: Vec<StormPlan>,
    /// Tip offsets, seconds into the phase.
    pub tips: Vec<f64>,
}

impl ChurnPlan {
    /// The most rental sessions connected at once under this plan.
    pub fn peak_rentals(&self) -> usize {
        let mut edges: Vec<(f64, i64)> = Vec::new();
        for rental in &self.rentals {
            edges.push((rental.arrive_at, 1));
            if let Some(depart) = rental.depart_at {
                edges.push((depart, -1));
            }
        }
        edges.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let (mut now, mut peak) = (0i64, 0i64);
        for (_, delta) in edges {
            now += delta;
            peak = peak.max(now);
        }
        peak as usize
    }

    pub fn summary(&self) -> Value {
        json!({
            "rental_sessions": self.rentals.len(),
            "bursts": self.rentals.iter().map(|r| r.burst).max().map_or(0, |b| b + 1),
            "rentals_departing_in_phase": self.rentals.iter().filter(|r| r.depart_at.is_some()).count(),
            "peak_rentals_planned": self.peak_rentals(),
            "storms": self.storms.iter().map(|s| json!({"at_seconds": s.at, "fraction": s.fraction})).collect::<Vec<_>>(),
            "tips_at_seconds": self.tips,
        })
    }
}

// --- the driver -------------------------------------------------------------

use crate::client::{self, Control, SessionConfig, SessionHandle, SessionShared};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A rental session's difficulty multiplier: its hashrate over the slowest
/// base session's under vardiff, capped at the ratio; 1 under fixed.
pub fn rental_difficulty_multiplier(
    population: &crate::realism::Population,
    rental_hashrate: f64,
) -> f64 {
    match population.difficulty {
        crate::realism::SessionDifficulty::Fixed => 1.0,
        crate::realism::SessionDifficulty::Vardiff { max_ratio } => {
            let slowest = population
                .sessions
                .iter()
                .map(|s| s.hashrate)
                .fold(f64::INFINITY, f64::min);
            (rental_hashrate / slowest).clamp(1.0, max_ratio)
        }
    }
}

/// What the rental sessions are, beside the plan of when they come and go.
pub struct RentalTemplate {
    /// The address rentals mine for: the heaviest recipient.
    pub address: String,
    pub share_difficulty: f64,
    pub password: String,
    pub difficulty_multiplier: f64,
    /// Relative share rate, on the base sessions' scale.
    pub offer_weight: f64,
    /// A base session's configuration, of which a rental's is a copy with its
    /// own index, username, password and difficulty.
    pub config: SessionConfig,
}

/// One storm as driven.
#[derive(Clone, Debug, serde::Serialize)]
pub struct StormRecord {
    pub index: usize,
    pub at_seconds: f64,
    pub fraction: f64,
    pub connected: usize,
    pub dropped: usize,
}

/// Drives one churn phase's plan from the scheduler loop, never awaited.
pub struct ChurnDriver {
    plan: ChurnPlan,
    spec: ChurnSpec,
    template: RentalTemplate,
    frontends: Vec<String>,
    shared: Arc<SessionShared>,
    max_outstanding: usize,
    first_index: usize,
    base_offer_weight: f64,
    /// Rental index into `plan.rentals` by arrival order.
    arrivals: Vec<usize>,
    /// `(depart_at, rental index)` in departure order.
    departures: Vec<(f64, usize)>,
    next_arrival: usize,
    next_departure: usize,
    next_storm: usize,
    next_tip: usize,
    storm_rng: Rng,
    offer_rng: Rng,
    rental_cursor: AtomicUsize,
    /// Every rental session spawned, by rental index.
    pub rentals: Vec<Option<SessionHandle>>,
    departed: Vec<bool>,
    /// Session indices of the rentals spawned and not departed.
    active: Vec<usize>,
    pub tips: Vec<crate::node::TipChange>,
    pub storms: Vec<StormRecord>,
    pub spawned_at: Vec<Option<Instant>>,
    pub departed_at: Vec<Option<Instant>>,
}

impl ChurnDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        spec: ChurnSpec,
        template: RentalTemplate,
        frontends: Vec<String>,
        shared: Arc<SessionShared>,
        max_outstanding: usize,
        first_index: usize,
        base_offer_weight: f64,
    ) -> Self {
        let plan = spec.plan();
        let mut arrivals: Vec<usize> = (0..plan.rentals.len()).collect();
        arrivals.sort_by(|a, b| {
            plan.rentals[*a]
                .arrive_at
                .total_cmp(&plan.rentals[*b].arrive_at)
                .then(a.cmp(b))
        });
        let mut departures: Vec<(f64, usize)> = plan
            .rentals
            .iter()
            .enumerate()
            .filter_map(|(index, rental)| rental.depart_at.map(|at| (at, index)))
            .collect();
        departures.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let count = plan.rentals.len();
        Self {
            storm_rng: Rng::new(spec.seed, "churn-storms"),
            offer_rng: Rng::new(spec.seed, "churn-offers"),
            plan,
            spec,
            template,
            frontends,
            shared,
            max_outstanding,
            first_index,
            base_offer_weight,
            arrivals,
            departures,
            next_arrival: 0,
            next_departure: 0,
            next_storm: 0,
            next_tip: 0,
            rental_cursor: AtomicUsize::new(0),
            rentals: (0..count).map(|_| None).collect(),
            departed: vec![false; count],
            active: Vec::new(),
            tips: Vec::new(),
            storms: Vec::new(),
            spawned_at: vec![None; count],
            departed_at: vec![None; count],
        }
    }

    pub fn plan(&self) -> &ChurnPlan {
        &self.plan
    }

    pub fn template(&self) -> &RentalTemplate {
        &self.template
    }

    /// The session index of rental `k`.
    pub fn session_index(&self, rental: usize) -> usize {
        self.first_index + rental
    }

    /// Everything due by `seconds` into the phase: arrivals, departures,
    /// storms and tips, in that order.
    pub fn tick(&mut self, seconds: f64, base: &[SessionHandle], node: &crate::node::NodeState) {
        while self.next_arrival < self.arrivals.len()
            && self.plan.rentals[self.arrivals[self.next_arrival]].arrive_at <= seconds
        {
            let rental = self.arrivals[self.next_arrival];
            self.next_arrival += 1;
            let index = self.session_index(rental);
            let frontend = rental % self.frontends.len();
            let config = SessionConfig {
                index,
                username: format!("{}.rent{rental:05}", self.template.address),
                password: self.template.password.clone(),
                share_difficulty: self.template.share_difficulty,
                ..self.template.config.clone()
            };
            self.rentals[rental] = Some(client::spawn_session(
                config,
                frontend,
                self.frontends[frontend].clone(),
                self.shared.clone(),
                self.max_outstanding,
            ));
            self.spawned_at[rental] = Some(Instant::now());
            self.active.push(rental);
        }
        while self.next_departure < self.departures.len()
            && self.departures[self.next_departure].0 <= seconds
        {
            let rental = self.departures[self.next_departure].1;
            self.next_departure += 1;
            self.depart(rental);
        }
        while self.next_storm < self.plan.storms.len()
            && self.plan.storms[self.next_storm].at <= seconds
        {
            let storm = self.plan.storms[self.next_storm].clone();
            self.next_storm += 1;
            self.storm(&storm, base);
        }
        while self.next_tip < self.plan.tips.len() && self.plan.tips[self.next_tip] <= seconds {
            self.next_tip += 1;
            self.tips.push(node.mint_external_block());
        }
    }

    fn depart(&mut self, rental: usize) {
        if self.departed[rental] {
            return;
        }
        if let Some(handle) = &self.rentals[rental] {
            // Ineligible for offers from this instant, as a rental that has
            // gone is; the session records what it abandons.
            handle.paused.store(true, Ordering::Relaxed);
            let _ = handle.control.send(Control::Depart {
                reason: "rental departed".into(),
                reconnect_after: None,
            });
        }
        self.departed[rental] = true;
        self.departed_at[rental] = Some(Instant::now());
        self.active.retain(|active| *active != rental);
    }

    /// Drop a seeded `fraction` of the sessions connected now, each to
    /// reconnect after a seeded delay.
    fn storm(&mut self, storm: &StormPlan, base: &[SessionHandle]) {
        let mut candidates: Vec<&SessionHandle> = base
            .iter()
            .filter(|session| !session.paused.load(Ordering::Relaxed))
            .collect();
        candidates.extend(
            self.active
                .iter()
                .filter_map(|rental| self.rentals[*rental].as_ref()),
        );
        let connected = candidates.len();
        let dropped = ((storm.fraction * connected as f64).round() as usize).min(connected);
        // A partial Fisher-Yates over the candidates: the first `dropped` are
        // the storm's.
        for index in 0..dropped {
            let pick = index + (self.storm_rng.next_u64() as usize) % (connected - index);
            candidates.swap(index, pick);
            let delay = self.storm_rng.next_f64() * self.spec.storm_reconnect_seconds;
            let _ = candidates[index].control.send(Control::Depart {
                reason: format!("reconnect storm {}", storm.index),
                reconnect_after: Some(Duration::from_secs_f64(delay)),
            });
        }
        self.storms.push(StormRecord {
            index: storm.index,
            at_seconds: storm.at,
            fraction: storm.fraction,
            connected,
            dropped,
        });
    }

    /// Place one offer across the base sessions and the rentals connected
    /// now, by their share rates: the rentals as a pool whose weight is
    /// their count times a rental's offer weight. Either pool falls back to
    /// the other when it cannot take the offer.
    pub fn offer(
        &mut self,
        base: &[SessionHandle],
        base_offer: &mut dyn FnMut(&[SessionHandle]) -> bool,
        limit: usize,
        phase: &Arc<str>,
    ) -> bool {
        let rental_weight = self.active.len() as f64 * self.template.offer_weight;
        let total = rental_weight + self.base_offer_weight;
        let rentals_first = total > 0.0 && self.offer_rng.next_f64() * total < rental_weight;
        if rentals_first && self.offer_rental(limit, phase) {
            return true;
        }
        if base_offer(base) {
            return true;
        }
        !rentals_first && self.offer_rental(limit, phase)
    }

    fn offer_rental(&self, limit: usize, phase: &Arc<str>) -> bool {
        let count = self.active.len();
        for _ in 0..count {
            let slot = self.rental_cursor.fetch_add(1, Ordering::Relaxed) % count;
            if let Some(handle) = &self.rentals[self.active[slot]] {
                if handle.try_offer(limit, phase) {
                    return true;
                }
            }
        }
        false
    }

    /// End the phase: every rental still connected quiesces as any session
    /// does at a phase end, within `limit`, and stops. Returns how many
    /// submits were still outstanding at the limit.
    pub async fn finish(&mut self, limit: Duration) -> usize {
        for handle in self.rentals.iter().flatten() {
            handle.paused.store(true, Ordering::Relaxed);
            let _ = handle.control.send(Control::Pause);
        }
        let deadline = Instant::now() + limit;
        let outstanding = |rentals: &[Option<SessionHandle>]| -> usize {
            rentals
                .iter()
                .flatten()
                .map(|handle| handle.outstanding.load(Ordering::Relaxed))
                .sum()
        };
        let mut left = outstanding(&self.rentals);
        while left > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
            left = outstanding(&self.rentals);
        }
        for handle in self.rentals.iter().flatten() {
            let _ = handle.control.send(Control::Stop);
        }
        for handle in self.rentals.iter_mut() {
            if let Some(handle) = handle.take() {
                let _ = tokio::time::timeout(Duration::from_secs(15), handle.task).await;
            }
        }
        self.active.clear();
        left
    }
}

// --- the report -------------------------------------------------------------

use crate::client::{ConnectionClosed, ConnectionOpened, TipSighting};
use std::collections::BTreeMap;

/// What the report reads, gathered once the phase has run.
pub struct ReportInputs<'a> {
    pub spec: &'a ChurnSpec,
    pub driver: &'a ChurnDriver,
    pub phase_started: Instant,
    pub phase_ended: Instant,
    pub opened: &'a [ConnectionOpened],
    pub closed: &'a [ConnectionClosed],
    pub sightings: &'a [TipSighting],
    pub all_tip_changes: &'a [crate::node::TipChange],
    pub reconnects: &'a [client::ReconnectRecord],
    pub rentals_undrained: usize,
}

/// Each session's connection intervals, `(open, close)`, a connection still
/// open at the end closing at `None`.
fn intervals(
    opened: &[ConnectionOpened],
    closed: &[ConnectionClosed],
) -> BTreeMap<usize, Vec<(Instant, Option<Instant>)>> {
    let mut events: BTreeMap<usize, Vec<(Instant, bool)>> = BTreeMap::new();
    for open in opened {
        events
            .entry(open.session)
            .or_default()
            .push((open.ready, true));
    }
    for close in closed {
        events
            .entry(close.session)
            .or_default()
            .push((close.at, false));
    }
    events
        .into_iter()
        .map(|(session, mut list)| {
            list.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            let mut spans = Vec::new();
            let mut open: Option<Instant> = None;
            for (at, is_open) in list {
                match (is_open, open) {
                    (true, None) => open = Some(at),
                    (false, Some(start)) => {
                        spans.push((start, Some(at)));
                        open = None;
                    }
                    // A second open without a close, or a close without an
                    // open (a failed attempt's), carries nothing to add.
                    _ => {}
                }
            }
            if let Some(start) = open {
                spans.push((start, None));
            }
            (session, spans)
        })
        .collect()
}

fn connected_at(spans: &[(Instant, Option<Instant>)], at: Instant) -> bool {
    spans
        .iter()
        .any(|(open, close)| *open <= at && close.is_none_or(|close| at < close))
}

fn closed_between(spans: &[(Instant, Option<Instant>)], from: Instant, until: Instant) -> bool {
    spans
        .iter()
        .any(|(_, close)| close.is_some_and(|close| from <= close && close < until))
}

fn millis(from: Instant, to: Instant) -> f64 {
    to.saturating_duration_since(from).as_secs_f64() * 1000.0
}

/// Tip delivery under churn, per tip, over the sessions connected when the
/// tip changed (#521): `served` got work on it while it was the tip;
/// `left_before_served` closed their connection first (a departure or a
/// storm, excluded from the figure because the miner left); `unserved` stayed
/// connected and never got it, which the gate fails. `last_served_milliseconds`
/// is the slowest served session, and `null` while any session is unserved.
pub fn tip_delivery(inputs: &ReportInputs<'_>) -> Value {
    tip_delivery_over(
        &inputs.driver.tips,
        inputs.all_tip_changes,
        inputs.opened,
        inputs.closed,
        inputs.sightings,
        inputs.phase_ended,
    )
}

/// [`tip_delivery`] over explicit inputs: the tips to measure, every tip
/// change (a tip's reign ends at the next), the connection timeline, the
/// sightings, and the phase's end (the reign of a tip never replaced).
pub fn tip_delivery_over(
    tips: &[crate::node::TipChange],
    all_tip_changes: &[crate::node::TipChange],
    opened: &[ConnectionOpened],
    closed: &[ConnectionClosed],
    sightings: &[TipSighting],
    phase_ended: Instant,
) -> Value {
    let spans = intervals(opened, closed);
    let tips: Vec<Value> = tips
        .iter()
        .map(|tip| {
            let t0 = tip.monotonic;
            let replaced = all_tip_changes
                .iter()
                .filter(|change| change.monotonic > t0)
                .map(|change| change.monotonic)
                .min()
                .unwrap_or(phase_ended.max(t0));
            let mut first: BTreeMap<usize, Instant> = BTreeMap::new();
            for sighting in sightings {
                if sighting.tip == tip.hash && sighting.at >= t0 && sighting.at < replaced {
                    let slot = first.entry(sighting.session).or_insert(sighting.at);
                    if sighting.at < *slot {
                        *slot = sighting.at;
                    }
                }
            }
            let (mut served, mut left, mut unserved) = (Vec::new(), 0usize, Vec::new());
            for (session, session_spans) in &spans {
                if !connected_at(session_spans, t0) {
                    continue;
                }
                match first.get(session) {
                    Some(at) if !closed_between(session_spans, t0, *at) => {
                        served.push(millis(t0, *at));
                    }
                    Some(_) => left += 1,
                    None if closed_between(session_spans, t0, replaced) => left += 1,
                    None => unserved.push(*session),
                }
            }
            let last = served.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            json!({
                "tip": tip.hash,
                "height": tip.height,
                "replaced_after_milliseconds": millis(t0, replaced),
                "connected_at_tip": served.len() + left + unserved.len(),
                "served": served.len(),
                "left_before_served": left,
                "unserved": unserved.len(),
                "unserved_sample": unserved.iter().take(10).collect::<Vec<_>>(),
                "last_served_milliseconds": (unserved.is_empty() && !served.is_empty()).then_some(last),
                "latency_milliseconds": crate::measure::summarize(
                    served,
                    crate::measure::MILLISECONDS,
                    "client monotonic against the node's tip stamp",
                ),
            })
        })
        .collect();
    json!({
        "definition": "for each tip minted in the churn phase, the sessions with an open \
                       connection at the node's tip stamp: served got usable work on the tip \
                       while it was the tip, on the connection they had; left_before_served \
                       closed that connection first (a rental departure or a storm) and are not \
                       in the figure; unserved stayed connected and never got it. \
                       last_served_milliseconds is the slowest served session, null while any \
                       session is unserved.",
        "tips": tips,
    })
}

/// Everything the churn phase generated and drove, and what it measured.
pub fn report(inputs: &ReportInputs<'_>) -> Value {
    let spec = inputs.spec;
    let driver = inputs.driver;
    let start = inputs.phase_started;
    let end = inputs.phase_ended;
    let seconds = end.saturating_duration_since(start).as_secs().max(1) as usize;
    let in_phase = |at: Instant| at >= start && at <= end;
    let second_of = |at: Instant| at.saturating_duration_since(start).as_secs() as usize;

    let mut connects_per_second = vec![0u64; seconds + 1];
    for open in inputs.opened.iter().filter(|open| in_phase(open.ready)) {
        connects_per_second[second_of(open.ready).min(seconds)] += 1;
    }
    let spans = intervals(inputs.opened, inputs.closed);
    let concurrent: Vec<usize> = (0..=seconds)
        .map(|second| {
            let at = start + Duration::from_secs(second as u64);
            spans
                .values()
                .filter(|session_spans| connected_at(session_spans, at))
                .count()
        })
        .collect();
    let first_rental = driver.first_index;
    let is_rental = |session: usize| session >= first_rental;
    let first_job = |filter: &dyn Fn(&ConnectionOpened) -> bool| {
        crate::measure::summarize(
            inputs
                .opened
                .iter()
                .filter(|open| in_phase(open.started) && filter(open))
                .map(|open| millis(open.started, open.ready))
                .collect(),
            crate::measure::MILLISECONDS,
            "client monotonic, connection attempt to first job",
        )
    };
    let rental_arrivals = first_job(&|open| is_rental(open.session) && open.cause == "initial");
    let storm_reconnects = first_job(&|open| open.cause.starts_with(client::CHURN_CLOSED));
    let new_sessions = first_job(&|open| {
        (is_rental(open.session) && open.cause == "initial")
            || open.cause.starts_with(client::CHURN_CLOSED)
    });
    let departures = driver.departed_at.iter().flatten().count();
    let abrupt_closes = inputs
        .closed
        .iter()
        .filter(|close| in_phase(close.at) && close.cause.starts_with(client::CHURN_CLOSED))
        .count();
    let failed_attempts = inputs
        .reconnects
        .iter()
        .filter(|record| record.phase == PHASE && !record.completed)
        .count();
    let reconnects_completed = inputs
        .reconnects
        .iter()
        .filter(|record| record.phase == PHASE && record.completed)
        .count();
    let mean = |series: &[u64]| series.iter().sum::<u64>() as f64 / series.len().max(1) as f64;
    json!({
        "ran": true,
        "parameters": {
            "seconds": spec.seconds,
            "tips": spec.tips,
            "rental_bursts": spec.bursts.render(),
            "rental_burst_window_seconds": spec.burst_window_seconds,
            "rental_burst_interval_seconds": spec.burst_interval_seconds,
            "rental_lifetime": spec.lifetime.render(),
            "rental_hashrate": spec.rental_hashrate,
            "reconnect_storms": spec.storms,
            "storm_interval_seconds": spec.storm_interval_seconds,
            "storm_reconnect_seconds": spec.storm_reconnect_seconds,
            "seed": spec.seed,
        },
        "rental": {
            "address": driver.template.address,
            "share_difficulty": driver.template.share_difficulty,
            "difficulty_multiplier": driver.template.difficulty_multiplier,
            "offer_weight": driver.template.offer_weight,
        },
        "plan": driver.plan.summary(),
        "realised": {
            "rentals_spawned": driver.spawned_at.iter().flatten().count(),
            "rentals_departed": departures,
            "abrupt_closes": abrupt_closes,
            "storms": driver.storms,
            "reconnects_completed": reconnects_completed,
            "connection_attempts_failed": failed_attempts,
            "connects_per_second": connects_per_second,
            "connects_per_second_max": connects_per_second.iter().max(),
            "connects_per_second_mean": mean(&connects_per_second),
            "concurrent_sessions_per_second": concurrent,
            "concurrent_sessions_min": concurrent.iter().min(),
            "concurrent_sessions_max": concurrent.iter().max(),
            "rentals_still_outstanding_at_phase_end": inputs.rentals_undrained,
        },
        "time_to_first_job": {
            "new_sessions": new_sessions,
            "rental_arrivals": rental_arrivals,
            "storm_reconnects": storm_reconnects,
            "definition": "from the connection attempt's start to the session holding its \
                           first job (subscribe, configure, authorize and the first notify), \
                           for connections started in the phase; new_sessions is rental \
                           arrivals and storm reconnects together. A storm's reconnect delay \
                           is the scenario's, and is not in the figure.",
        },
        "tip_delivery": tip_delivery(inputs),
    })
}

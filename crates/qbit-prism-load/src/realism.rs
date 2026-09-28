//! Population realism (#521): distinct payout addresses with skewed weights,
//! per-session hashrate and difficulty, and bursty share arrival.
//!
//! Every option defaults to the shape every run before it had: one payout
//! address for every live session, five round-robin recipients in the seeded
//! window, one share difficulty, a round-robin scheduler and a smooth token
//! bucket. A run that asks for none of them takes the old code paths, not an
//! equivalent new one, so its sessions, its window and its schedule are
//! byte-for-byte what they were.
//!
//! Every random draw comes from its own [`Rng`] stream, keyed by `--seed` and
//! the stream's name, so adding a draw to one stream never moves another and
//! a run is reproducible from its seed. The generator is SplitMix64, kept
//! here rather than taken from a crate so a dependency upgrade cannot change
//! what a checked-in preset generates.
//!
//! EP-VALIDATION: every distribution is parsed and range-checked here, at the
//! entry boundary, and the parsed value is what the run uses and reports.

use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};

/// The highest recipient count the fixed-width address suffix can name.
pub const MAX_RECIPIENTS: usize = 99_999;
/// A per-session difficulty spread wider than this is not a Stratum pool.
pub const MAX_DIFFICULTY_RATIO: f64 = 1_000_000.0;
/// A coefficient of variation above this is a different process, not a
/// burstier one.
pub const MAX_ARRIVAL_CV: f64 = 10.0;
/// Seconds per slow arrival segment: the 60 s window the slow coefficient of
/// variation describes.
pub const SLOW_SEGMENT_SECONDS: u64 = 60;

/// SplitMix64: one independent stream per purpose.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    /// The stream `name` under `seed`. The name is folded in with FNV-1a, so
    /// two streams under one seed are unrelated.
    pub fn new(seed: u64, name: &str) -> Self {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in name.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        let mut rng = Self(seed ^ hash);
        // One step so a zero seed and an empty name still start mixed.
        rng.next_u64();
        rng
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`, 53 bits.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Standard normal, by Box-Muller.
    pub fn normal(&mut self) -> f64 {
        let u1 = 1.0 - self.next_f64();
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }

    /// Lognormal with mean 1 and coefficient of variation `cv`; exactly 1
    /// when `cv` is 0.
    pub fn lognormal_mean_one(&mut self, cv: f64) -> f64 {
        if cv == 0.0 {
            return 1.0;
        }
        let sigma2 = (1.0 + cv * cv).ln();
        (-sigma2 / 2.0 + sigma2.sqrt() * self.normal()).exp()
    }
}

fn finite_number(text: &str, what: &str) -> Result<f64> {
    let value: f64 = text
        .trim()
        .parse()
        .with_context(|| format!("{what} {text:?} is not a number"))?;
    ensure!(value.is_finite(), "{what} must be finite, not {text:?}");
    Ok(value)
}

/// How work is spread over the payout addresses (`--recipient-weights`).
#[derive(Clone, Debug, PartialEq)]
pub enum WeightDist {
    /// Every address carries the same weight.
    Uniform,
    /// Rank `k` (from 1) weighs `1 / k^s`.
    Zipf { s: f64 },
    /// Weights drawn from a Pareto(`alpha`) with scale 1.
    Pareto { alpha: f64 },
    /// One address carries `fraction` of the work and the rest is spread by
    /// `tail`, which is never itself a whale.
    Whale {
        fraction: f64,
        tail: Box<WeightDist>,
    },
}

impl WeightDist {
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if let Some(rest) = text.strip_prefix("whale:") {
            let (fraction, tail) = rest.split_once('+').with_context(|| {
                format!("--recipient-weights {text:?}: write whale:<fraction>+<tail>")
            })?;
            let fraction = finite_number(fraction, "the whale fraction")?;
            ensure!(
                fraction > 0.0 && fraction < 1.0,
                "the whale fraction must be above 0 and below 1, not {fraction}"
            );
            let tail = Self::parse(tail)?;
            ensure!(
                !matches!(tail, Self::Whale { .. }),
                "--recipient-weights {text:?}: a whale's tail cannot be another whale"
            );
            return Ok(Self::Whale {
                fraction,
                tail: Box::new(tail),
            });
        }
        if text == "uniform" {
            return Ok(Self::Uniform);
        }
        if let Some(s) = text.strip_prefix("zipf:") {
            let s = finite_number(s, "the Zipf exponent")?;
            ensure!(
                s > 0.0 && s <= 10.0,
                "the Zipf exponent must be above 0 and at most 10, not {s}"
            );
            return Ok(Self::Zipf { s });
        }
        if let Some(alpha) = text.strip_prefix("pareto:") {
            let alpha = finite_number(alpha, "the Pareto shape")?;
            ensure!(
                alpha > 0.0 && alpha <= 100.0,
                "the Pareto shape must be above 0 and at most 100, not {alpha}"
            );
            return Ok(Self::Pareto { alpha });
        }
        bail!(
            "unknown --recipient-weights {text:?}; use uniform, zipf:<s>, pareto:<alpha> or \
             whale:<fraction>+<one of those>"
        )
    }

    /// The canonical spelling, as the report prints it.
    pub fn render(&self) -> String {
        match self {
            Self::Uniform => "uniform".into(),
            Self::Zipf { s } => format!("zipf:{s}"),
            Self::Pareto { alpha } => format!("pareto:{alpha}"),
            Self::Whale { fraction, tail } => format!("whale:{fraction}+{}", tail.render()),
        }
    }

    pub fn is_uniform(&self) -> bool {
        matches!(self, Self::Uniform)
    }

    /// `n` weights summing to 1, heaviest first.
    pub fn weights(&self, n: usize, rng: &mut Rng) -> Vec<f64> {
        if n == 0 {
            return Vec::new();
        }
        let mut raw: Vec<f64> = match self {
            Self::Uniform => vec![1.0; n],
            Self::Zipf { s } => (1..=n).map(|rank| (rank as f64).powf(-s)).collect(),
            Self::Pareto { alpha } => {
                let mut draws: Vec<f64> = (0..n)
                    .map(|_| (1.0 - rng.next_f64()).powf(-1.0 / alpha))
                    .collect();
                draws.sort_by(|a, b| b.total_cmp(a));
                draws
            }
            Self::Whale { fraction, tail } => {
                if n == 1 {
                    vec![1.0]
                } else {
                    let mut weights = vec![*fraction];
                    weights.extend(
                        tail.weights(n - 1, rng)
                            .into_iter()
                            .map(|weight| weight * (1.0 - fraction)),
                    );
                    weights
                }
            }
        };
        let total: f64 = raw.iter().sum();
        for weight in &mut raw {
            *weight /= total;
        }
        raw
    }
}

/// How each session's share difficulty is set (`--session-difficulty`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SessionDifficulty {
    /// Every session mines the configured share difficulty.
    Fixed,
    /// As a converged vardiff would: difficulty in proportion to the
    /// session's hashrate, the slowest session at the configured difficulty
    /// and none above `max_ratio` times it. Each session asks for its value
    /// with `d=<difficulty>` in the Stratum password, which the server honours
    /// with vardiff off.
    Vardiff { max_ratio: f64 },
}

impl SessionDifficulty {
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if text == "fixed" {
            return Ok(Self::Fixed);
        }
        if let Some(ratio) = text.strip_prefix("vardiff:") {
            let max_ratio = finite_number(ratio, "the vardiff ratio")?;
            ensure!(
                (1.0..=MAX_DIFFICULTY_RATIO).contains(&max_ratio),
                "the vardiff ratio must be 1..{MAX_DIFFICULTY_RATIO}, not {max_ratio}"
            );
            return Ok(Self::Vardiff { max_ratio });
        }
        bail!("unknown --session-difficulty {text:?}; use fixed or vardiff:<max ratio>")
    }

    pub fn render(&self) -> String {
        match self {
            Self::Fixed => "fixed".into(),
            Self::Vardiff { max_ratio } => format!("vardiff:{max_ratio}"),
        }
    }
}

/// How offered shares arrive over time (`--arrival`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Arrival {
    /// The open-loop token bucket at the phase's constant rate.
    Smooth,
    /// The phase's rate times a mean-1 multiplier that changes every second:
    /// the product of a per-60-s lognormal factor with coefficient of
    /// variation `cv60` and a per-second one with `cv1`, capped at `max`
    /// times the rate. The cap is the peak a preset names; it lowers the
    /// realised mean slightly, and the report states the realised figures.
    Bursty { cv1: f64, cv60: f64, max: f64 },
}

impl Arrival {
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if text == "smooth" {
            return Ok(Self::Smooth);
        }
        let Some(rest) = text.strip_prefix("bursty:") else {
            bail!("unknown --arrival {text:?}; use smooth or bursty:cv1=<x>,cv60=<y>,max=<m>");
        };
        let (mut cv1, mut cv60, mut max) = (None, None, None);
        for pair in rest.split(',') {
            let (key, value) = pair
                .split_once('=')
                .with_context(|| format!("--arrival {text:?}: {pair:?} is not key=value"))?;
            let slot = match key.trim() {
                "cv1" => &mut cv1,
                "cv60" => &mut cv60,
                "max" => &mut max,
                other => bail!("--arrival {text:?}: unknown key {other:?}"),
            };
            ensure!(slot.is_none(), "--arrival {text:?}: {key} is given twice");
            *slot = Some(finite_number(value, key)?);
        }
        let (Some(cv1), Some(cv60), Some(max)) = (cv1, cv60, max) else {
            bail!("--arrival {text:?}: cv1, cv60 and max are all required");
        };
        for (name, cv) in [("cv1", cv1), ("cv60", cv60)] {
            ensure!(
                (0.0..=MAX_ARRIVAL_CV).contains(&cv),
                "--arrival {name} must be 0..{MAX_ARRIVAL_CV}, not {cv}"
            );
        }
        ensure!(
            (1.0..=1000.0).contains(&max),
            "--arrival max must be 1..1000 times the rate, not {max}"
        );
        Ok(Self::Bursty { cv1, cv60, max })
    }

    pub fn render(&self) -> String {
        match self {
            Self::Smooth => "smooth".into(),
            Self::Bursty { cv1, cv60, max } => format!("bursty:cv1={cv1},cv60={cv60},max={max}"),
        }
    }

    /// The offered-share clock for one phase: how many rate-seconds have
    /// elapsed after `seconds` of wall time. [`Arrival::Smooth`] is exactly
    /// the wall time, so the token bucket is the one every run before this
    /// option used.
    pub fn clock(&self, seed: u64, phase: &str, phase_seconds: u64) -> ArrivalClock {
        match *self {
            Self::Smooth => ArrivalClock {
                multipliers: None,
                prefix: Vec::new(),
            },
            Self::Bursty { cv1, cv60, max } => {
                let mut slow = Rng::new(seed, &format!("arrival-60s:{phase}"));
                let mut fast = Rng::new(seed, &format!("arrival-1s:{phase}"));
                let seconds = phase_seconds as usize + 1;
                let mut multipliers = Vec::with_capacity(seconds);
                let mut slow_factor = 1.0;
                for second in 0..seconds {
                    if (second as u64).is_multiple_of(SLOW_SEGMENT_SECONDS) {
                        slow_factor = slow.lognormal_mean_one(cv60);
                    }
                    let factor = slow_factor * fast.lognormal_mean_one(cv1);
                    multipliers.push(factor.min(max));
                }
                let mut prefix = Vec::with_capacity(seconds + 1);
                prefix.push(0.0);
                for multiplier in &multipliers {
                    prefix.push(prefix.last().copied().unwrap_or(0.0) + multiplier);
                }
                ArrivalClock {
                    multipliers: Some(multipliers),
                    prefix,
                }
            }
        }
    }
}

/// See [`Arrival::clock`].
#[derive(Clone, Debug)]
pub struct ArrivalClock {
    multipliers: Option<Vec<f64>>,
    prefix: Vec<f64>,
}

impl ArrivalClock {
    /// Rate-seconds elapsed after `seconds` of wall time.
    pub fn elapsed(&self, seconds: f64) -> f64 {
        let Some(multipliers) = &self.multipliers else {
            return seconds;
        };
        let whole = (seconds.floor() as usize).min(multipliers.len().saturating_sub(1));
        let partial = seconds - whole as f64;
        self.prefix[whole] + partial * multipliers[whole]
    }

    pub fn is_smooth(&self) -> bool {
        self.multipliers.is_none()
    }
}

/// One live session's place in the population.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionProfile {
    /// Index into [`Population::addresses`].
    pub recipient: usize,
    pub username: String,
    /// Relative hashrate; the population's mean is 1.
    pub hashrate: f64,
    /// The session's share difficulty over the configured one.
    pub difficulty_multiplier: f64,
    /// Relative share rate: hashrate over difficulty.
    pub offer_weight: f64,
}

/// The live sessions and payout addresses of a run.
#[derive(Clone, Debug)]
pub struct Population {
    /// `--recipients` was omitted: one address for every session and the
    /// five-recipient seeded window every earlier run used.
    pub legacy: bool,
    pub addresses: Vec<String>,
    /// The generated work weight of each address, summing to 1.
    pub weights: Vec<f64>,
    pub sessions: Vec<SessionProfile>,
    pub weight_dist: WeightDist,
    pub difficulty: SessionDifficulty,
    pub hashrate_sigma: f64,
    pub seed: u64,
}

/// The inputs [`Population::build`] reads, from the command line.
#[derive(Clone, Debug)]
pub struct PopulationSpec {
    pub recipients: Option<usize>,
    pub weights: WeightDist,
    pub difficulty: SessionDifficulty,
    pub hashrate_sigma: f64,
    pub sessions: usize,
    pub seed: u64,
}

impl Population {
    /// `payout_address` is the run's address; with `--recipients N` the
    /// addresses are it plus a fixed-width `rNNNNN` suffix, so every one
    /// validates at the fake node and shares one share-id prefix.
    pub fn build(spec: &PopulationSpec, payout_address: &str) -> Result<Self> {
        ensure!(spec.sessions > 0, "the population needs a session");
        let mut weight_rng = Rng::new(spec.seed, "recipient-weights");
        let (legacy, addresses, weights) = match spec.recipients {
            None => (true, vec![payout_address.to_owned()], vec![1.0]),
            Some(count) => {
                ensure!(
                    (1..=MAX_RECIPIENTS).contains(&count),
                    "--recipients must be 1..{MAX_RECIPIENTS}"
                );
                (
                    false,
                    (0..count)
                        .map(|index| recipient_address(payout_address, index))
                        .collect(),
                    spec.weights.weights(count, &mut weight_rng),
                )
            }
        };
        let counts = apportion(spec.sessions, &weights, spec.sessions >= weights.len());
        let mut jitter = Rng::new(spec.seed, "session-hashrate");
        let mut sessions = Vec::with_capacity(spec.sessions);
        for (recipient, count) in counts.iter().enumerate() {
            for _ in 0..*count {
                let index = sessions.len();
                let username = format!("{}.s{index:05}", addresses[recipient]);
                let base = weights[recipient] / *count as f64;
                let factor = if spec.hashrate_sigma == 0.0 {
                    1.0
                } else {
                    let sigma = spec.hashrate_sigma;
                    (sigma * jitter.normal() - sigma * sigma / 2.0).exp()
                };
                sessions.push(SessionProfile {
                    recipient,
                    username,
                    hashrate: base * factor,
                    difficulty_multiplier: 1.0,
                    offer_weight: 0.0,
                });
            }
        }
        let mean = sessions.iter().map(|s| s.hashrate).sum::<f64>() / sessions.len() as f64;
        for session in &mut sessions {
            session.hashrate /= mean;
        }
        let slowest = sessions
            .iter()
            .map(|s| s.hashrate)
            .fold(f64::INFINITY, f64::min);
        for session in &mut sessions {
            session.difficulty_multiplier = match spec.difficulty {
                SessionDifficulty::Fixed => 1.0,
                SessionDifficulty::Vardiff { max_ratio } => {
                    (session.hashrate / slowest).clamp(1.0, max_ratio)
                }
            };
            session.offer_weight = session.hashrate / session.difficulty_multiplier;
        }
        Ok(Self {
            legacy,
            addresses,
            weights,
            sessions,
            weight_dist: spec.weights.clone(),
            difficulty: spec.difficulty,
            hashrate_sigma: spec.hashrate_sigma,
            seed: spec.seed,
        })
    }

    /// Whether the scheduler can keep its round-robin: every session offered
    /// at the same rate, to a part in a billion. That is every run that
    /// asked for no skew, and every vardiff run whose ratio clamps nobody:
    /// a converged vardiff gives every session the same share rate, so the
    /// skew is in the work each share carries, not in how often it comes.
    pub fn uniform_offers(&self) -> bool {
        let first = self.sessions[0].offer_weight;
        self.sessions
            .iter()
            .all(|s| (s.offer_weight - first).abs() <= first.abs() * 1e-9)
    }

    /// The difficulty multiplier of the average offered share: each
    /// session's multiplier weighted by its share rate. Exactly 1 when every
    /// session mines the configured difficulty.
    pub fn mean_offered_multiplier(&self) -> f64 {
        if self.sessions.iter().all(|s| s.difficulty_multiplier == 1.0) {
            return 1.0;
        }
        let rate: f64 = self.sessions.iter().map(|s| s.offer_weight).sum();
        self.sessions
            .iter()
            .map(|s| s.offer_weight * s.difficulty_multiplier)
            .sum::<f64>()
            / rate
    }

    pub fn max_difficulty_multiplier(&self) -> f64 {
        self.sessions
            .iter()
            .map(|s| s.difficulty_multiplier)
            .fold(1.0, f64::max)
    }

    /// The share-id prefix every live share of the run starts with.
    pub fn share_prefix(&self, payout_address: &str) -> String {
        if self.legacy {
            format!("{payout_address}.")
        } else {
            format!("{payout_address}r")
        }
    }

    /// The seeded window's recipient for each share, 1-based index `i` at
    /// position `i - 1`, drawn from the address weights so the recipients
    /// interleave as they would in a real ledger. `None` for a legacy run,
    /// whose window keeps its five round-robin recipients.
    pub fn window_assignments(&self, rows: u64) -> Option<Vec<u32>> {
        if self.legacy {
            return None;
        }
        let table = AliasTable::new(&self.weights);
        let mut rng = Rng::new(self.seed, "window-recipients");
        Some((0..rows).map(|_| table.sample(&mut rng) as u32).collect())
    }
}

/// `payout_address` plus the recipient's fixed-width suffix.
pub fn recipient_address(payout_address: &str, index: usize) -> String {
    format!("{payout_address}r{index:05}")
}

/// Largest-remainder apportionment of `total` seats by `weights`, with one
/// seat each first when `min_one`. Deterministic: ties go to the lower index.
pub fn apportion(total: usize, weights: &[f64], min_one: bool) -> Vec<usize> {
    let n = weights.len();
    let mut seats = vec![usize::from(min_one); n];
    let remaining = total.saturating_sub(if min_one { n } else { 0 });
    let sum: f64 = weights.iter().sum();
    let quotas: Vec<f64> = weights
        .iter()
        .map(|weight| weight / sum * remaining as f64)
        .collect();
    let mut given = 0;
    for (seat, quota) in seats.iter_mut().zip(&quotas) {
        let whole = quota.floor() as usize;
        *seat += whole;
        given += whole;
    }
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|a, b| {
        let fa = quotas[*a] - quotas[*a].floor();
        let fb = quotas[*b] - quotas[*b].floor();
        fb.total_cmp(&fa).then(a.cmp(b))
    });
    for index in order.into_iter().take(remaining.saturating_sub(given)) {
        seats[index] += 1;
    }
    seats
}

/// Vose's alias table: O(1) weighted draws.
#[derive(Clone, Debug)]
pub struct AliasTable {
    probability: Vec<f64>,
    alias: Vec<usize>,
}

impl AliasTable {
    pub fn new(weights: &[f64]) -> Self {
        let n = weights.len();
        let sum: f64 = weights.iter().sum();
        let mut scaled: Vec<f64> = weights.iter().map(|w| w / sum * n as f64).collect();
        let mut probability = vec![0.0; n];
        let mut alias = vec![0; n];
        let (mut small, mut large): (Vec<usize>, Vec<usize>) =
            (0..n).partition(|index| scaled[*index] < 1.0);
        while let (Some(s), Some(l)) = (small.pop(), large.pop()) {
            probability[s] = scaled[s];
            alias[s] = l;
            scaled[l] = scaled[l] + scaled[s] - 1.0;
            if scaled[l] < 1.0 {
                small.push(l);
            } else {
                large.push(l);
            }
        }
        for index in large.into_iter().chain(small) {
            probability[index] = 1.0;
        }
        Self { probability, alias }
    }

    pub fn sample(&self, rng: &mut Rng) -> usize {
        let n = self.probability.len();
        let column = ((rng.next_f64() * n as f64) as usize).min(n - 1);
        if rng.next_f64() < self.probability[column] {
            column
        } else {
            self.alias[column]
        }
    }
}

/// Top-1 and top-10 shares, the Gini coefficient and the count of a
/// non-negative sample, heaviest first. Empty or all-zero is `null`, never
/// a concentration of 0.
pub fn concentration(values: &[f64]) -> Value {
    let total: f64 = values.iter().sum();
    if values.is_empty() || total <= 0.0 {
        return json!({
            "count": values.len(),
            "total": total,
            "top1_share": null,
            "top10_share": null,
            "gini": null,
            "unavailable_reason": "nothing was recorded",
        });
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| b.total_cmp(a));
    let top = |k: usize| sorted.iter().take(k).sum::<f64>() / total;
    let n = sorted.len() as f64;
    // Gini over the ascending order: sum((2i - n - 1) x_i) / (n sum x).
    let gini = sorted
        .iter()
        .rev()
        .enumerate()
        .map(|(i, x)| (2.0 * (i as f64 + 1.0) - n - 1.0) * x)
        .sum::<f64>()
        / (n * total);
    json!({
        "count": values.len(),
        "nonzero": values.iter().filter(|value| **value > 0.0).count(),
        "total": total,
        "top1_share": top(1),
        "top10_share": top(10),
        "gini": gini,
    })
}

/// Coefficient of variation of per-window counts: the whole seconds of the
/// series grouped `window` at a time, a trailing partial window dropped.
/// `null` when fewer than two windows fit or the mean is 0.
pub fn windowed_cv(per_second: &[u64], window: usize) -> Option<f64> {
    let sums: Vec<f64> = per_second
        .chunks_exact(window.max(1))
        .map(|chunk| chunk.iter().sum::<u64>() as f64)
        .collect();
    if sums.len() < 2 {
        return None;
    }
    let mean = sums.iter().sum::<f64>() / sums.len() as f64;
    if mean <= 0.0 {
        return None;
    }
    let variance = sums.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / sums.len() as f64;
    Some(variance.sqrt() / mean)
}

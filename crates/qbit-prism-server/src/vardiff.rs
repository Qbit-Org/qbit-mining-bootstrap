//! Difficulty changes use accepted work, including shares from retained jobs.
use anyhow::{ensure, Result};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct VardiffConfig {
    pub enabled: bool,
    pub target_seconds: f64,
    pub minimum: f64,
    pub maximum: f64,
    pub retarget_seconds: f64,
    pub max_step_up: f64,
    pub max_step_down: f64,
    pub ewma_alpha: f64,
    pub tolerance: f64,
}

impl Default for VardiffConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            target_seconds: 15.0,
            minimum: 0.000000001,
            maximum: 1024.0,
            retarget_seconds: 90.0,
            max_step_up: 4.0,
            max_step_down: 4.0,
            ewma_alpha: 0.4,
            tolerance: 0.25,
        }
    }
}

impl VardiffConfig {
    pub fn validate(&self) -> Result<()> {
        for value in [
            self.target_seconds,
            self.minimum,
            self.maximum,
            self.retarget_seconds,
            self.max_step_up,
            self.max_step_down,
            self.ewma_alpha,
        ] {
            ensure!(
                value.is_finite() && value > 0.0,
                "vardiff values must be finite and positive"
            );
        }
        ensure!(
            self.minimum <= self.maximum
                && self.max_step_up >= 1.0
                && self.max_step_down >= 1.0
                && self.ewma_alpha <= 1.0,
            "invalid vardiff bounds"
        );
        ensure!(
            self.tolerance.is_finite() && self.tolerance >= 0.0,
            "invalid vardiff tolerance"
        );
        ensure!(
            Duration::try_from_secs_f64(self.retarget_seconds).is_ok(),
            "vardiff retarget duration is too large"
        );
        Ok(())
    }

    pub fn next_difficulty(
        &self,
        current: f64,
        accepted_work: f64,
        elapsed: f64,
        previous_estimate: Option<f64>,
    ) -> (f64, Option<f64>) {
        let current = current.clamp(self.minimum, self.maximum);
        let estimate = if accepted_work > 0.0 {
            let observed = accepted_work * self.target_seconds / elapsed.max(0.001);
            Some(
                previous_estimate
                    .map_or(observed, |old| {
                        self.ewma_alpha * observed + (1.0 - self.ewma_alpha) * old
                    })
                    .clamp(self.minimum, self.maximum),
            )
        } else {
            None
        };
        let desired = estimate.unwrap_or(current / self.max_step_down);
        (
            desired
                .clamp(current / self.max_step_down, current * self.max_step_up)
                .clamp(self.minimum, self.maximum),
            estimate.or(previous_estimate),
        )
    }
}

pub struct Vardiff {
    pub config: VardiffConfig,
    started: Instant,
    accepted_work: f64,
    estimate: Option<f64>,
}

impl Vardiff {
    pub fn new(config: VardiffConfig) -> Self {
        Self {
            config,
            started: Instant::now(),
            accepted_work: 0.0,
            estimate: None,
        }
    }
    pub fn accepted(&mut self, work: f64) {
        self.accepted_work += work;
    }
    pub fn reset(&mut self) {
        self.started = Instant::now();
        self.accepted_work = 0.0;
        self.estimate = None;
    }
    pub fn retarget(&mut self, current: f64) -> Option<f64> {
        if !self.config.enabled
            || self.started.elapsed() < Duration::from_secs_f64(self.config.retarget_seconds)
        {
            return None;
        }
        let (next, estimate) = self.config.next_difficulty(
            current,
            self.accepted_work,
            self.started.elapsed().as_secs_f64(),
            self.estimate,
        );
        self.estimate = estimate;
        self.accepted_work = 0.0;
        self.started = Instant::now();
        if next != current && (next - current).abs() / current >= self.config.tolerance {
            Some(next)
        } else {
            None
        }
    }
}

/// Conventional miner passwords are options, not authentication secrets.
pub fn password_difficulties(password: &str) -> (Option<f64>, Option<f64>) {
    let (mut requested, mut minimum) = (None, None);
    for token in password.split(',') {
        let Some((key, value)) = token.trim().split_once('=') else {
            continue;
        };
        let Ok(value) = value.trim().parse::<f64>() else {
            continue;
        };
        if !value.is_finite() || value <= 0.0 {
            continue;
        }
        match key.trim().to_ascii_lowercase().as_str() {
            "d" => requested = Some(value),
            "md" => minimum = Some(value),
            _ => {}
        }
    }
    (requested, minimum)
}

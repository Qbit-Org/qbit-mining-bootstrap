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
    pub initial_enabled: bool,
    pub initial_max_step_up: f64,
    pub initial_min_shares: u64,
    pub initial_min_step_up: f64,
    pub initial_min_seconds: f64,
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
            initial_enabled: true,
            initial_max_step_up: 64.0,
            initial_min_shares: 8,
            initial_min_step_up: 4.0,
            initial_min_seconds: 1.0,
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
            self.initial_max_step_up,
            self.initial_min_step_up,
            self.initial_min_seconds,
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
            self.initial_max_step_up >= self.max_step_up
                && self.initial_min_step_up >= 1.0
                && self.initial_min_shares > 0,
            "invalid initial vardiff convergence bounds"
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

#[derive(Clone)]
pub struct Vardiff {
    pub config: VardiffConfig,
    started: Instant,
    accepted_work: f64,
    estimate: Option<f64>,
    accepted_shares: u64,
    initial_pending: bool,
    initial_evaluated: bool,
    pub proposed_initial: bool,
    pub proposed_share_backed: bool,
}

impl Vardiff {
    pub fn new(config: VardiffConfig) -> Self {
        Self {
            config,
            started: Instant::now(),
            accepted_work: 0.0,
            estimate: None,
            accepted_shares: 0,
            initial_pending: true,
            initial_evaluated: false,
            proposed_initial: false,
            proposed_share_backed: false,
        }
    }
    pub fn accepted(&mut self, work: f64) {
        self.accepted_work += work;
        self.accepted_shares += 1;
    }
    pub fn reset(&mut self) {
        self.started = Instant::now();
        self.accepted_work = 0.0;
        self.estimate = None;
        self.accepted_shares = 0;
    }
    pub fn retarget(&mut self, current: f64) -> Option<f64> {
        if !self.config.enabled {
            return None;
        }
        let elapsed = self.started.elapsed().as_secs_f64();
        let evaluate_initial = self.initial_pending
            && !self.initial_evaluated
            && self.config.initial_enabled
            && self.accepted_shares >= self.config.initial_min_shares
            && elapsed >= self.config.initial_min_seconds;
        let initial = evaluate_initial
            && self.accepted_work * self.config.target_seconds / elapsed
                >= current * self.config.initial_min_step_up;
        if evaluate_initial {
            self.initial_evaluated = true;
        }
        if elapsed < self.config.retarget_seconds && !initial {
            return None;
        }
        let mut policy = self.config.clone();
        if initial {
            policy.max_step_up = policy.initial_max_step_up;
        }
        let (next, estimate) =
            policy.next_difficulty(current, self.accepted_work, elapsed, self.estimate);
        self.estimate = estimate;
        self.proposed_initial = initial;
        self.proposed_share_backed = self.accepted_shares > 0;
        self.accepted_work = 0.0;
        self.accepted_shares = 0;
        self.started = Instant::now();
        if next != current && (next - current).abs() / current >= self.config.tolerance {
            Some(next)
        } else {
            None
        }
    }

    pub fn delivered_retarget(&mut self) {
        self.initial_pending = false;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_arrival_uses_one_bounded_evidence_gate_and_normal_steps_after_delivery() {
        let config = VardiffConfig {
            minimum: 1.0,
            maximum: 100_000.0,
            ..Default::default()
        };
        let mut vardiff = Vardiff::new(config);
        vardiff.started = Instant::now() - Duration::from_secs(1);
        for _ in 0..7 {
            vardiff.accepted(1.0);
        }
        assert!(
            vardiff.retarget(1.0).is_none(),
            "seven shares cannot trigger the early gate"
        );
        vardiff.accepted(1.0);
        let uncommitted = vardiff.clone();
        assert_eq!(vardiff.retarget(1.0), Some(64.0));
        assert!(vardiff.proposed_initial);
        // A failed paired delivery restores this evidence and its one-shot
        // permission. A successful paired send consumes the permission.
        vardiff = uncommitted;
        assert_eq!(vardiff.retarget(1.0), Some(64.0));
        vardiff.delivered_retarget();
        vardiff.started = Instant::now() - Duration::from_secs(2);
        for _ in 0..8 {
            vardiff.accepted(64.0);
        }
        assert!(vardiff.retarget(64.0).is_none());
        vardiff.started = Instant::now() - Duration::from_secs(91);
        for _ in 0..100 {
            vardiff.accepted(64.0);
        }
        assert!(vardiff.retarget(64.0).unwrap() <= 256.0);
        assert!(!vardiff.proposed_initial);
    }

    #[test]
    fn normal_arrivals_do_not_repeatedly_test_the_initial_noise_gate() {
        let config = VardiffConfig {
            minimum: 1.0,
            maximum: 1000.0,
            ..Default::default()
        };
        let mut vardiff = Vardiff::new(config);
        vardiff.started = Instant::now() - Duration::from_secs(60);
        for _ in 0..8 {
            vardiff.accepted(1.0);
        }
        assert!(vardiff.retarget(1.0).is_none());
        for _ in 0..100 {
            vardiff.accepted(1.0);
        }
        assert!(
            vardiff.retarget(1.0).is_none(),
            "one evaluated noisy window was tested repeatedly"
        );
    }
}

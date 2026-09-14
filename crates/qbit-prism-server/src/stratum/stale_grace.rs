//! Per-connection delivery anchors; refresh observation alone starts no timer.
use super::*;

fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// Published work provenance for local retention, independent of RPC reuse age.
/// This hint never authorizes credit; Coordinator::submit checks the chain.
#[derive(Clone, Debug)]
pub struct RetentionTip {
    pub hash: String,
    pub parent: Option<String>,
    pub transitioned: bool,
}

impl RetentionTip {
    pub(super) fn keeps_previous(&self, parent: &str, grace: &StaleGrace) -> bool {
        self.transitioned
            && self.hash != parent
            && grace.eligible_for(&self.hash)
            && self
                .parent
                .as_ref()
                .is_none_or(|previous| previous == parent)
    }
}

#[derive(Clone, Debug)]
pub struct StaleGrace {
    interval: Duration,
    delivered: Option<(String, Instant)>,
    checked_at: Instant,
}

impl StaleGrace {
    pub fn for_connection(interval: Duration, delivered: Option<(String, Instant)>) -> Self {
        Self {
            interval,
            delivered,
            checked_at: now(),
        }
    }

    pub fn eligible_for(&self, current_tip: &str) -> bool {
        !self.interval.is_zero()
            && self.delivered.as_ref().is_none_or(|(tip, at)| {
                tip != current_tip
                    || self.checked_at.saturating_duration_since(*at) <= self.interval
            })
    }
}

impl From<bool> for StaleGrace {
    fn from(eligible: bool) -> Self {
        Self::for_connection(Duration::from_secs(if eligible { 3 } else { 0 }), None)
    }
}

impl<C> Session<C> {
    pub(super) fn stale_grace(&self, config: &StratumConfig) -> StaleGrace {
        StaleGrace::for_connection(
            Duration::from_secs_f64(config.stale_grace_seconds),
            self.tip_work_delivered.clone(),
        )
    }

    pub(super) fn note_delivery(&mut self, tip: &str) {
        // Preserve first delivery on same-tip refresh; a real tip flip,
        // including A -> B -> A, reanchors this connection's grace.
        if self
            .tip_work_delivered
            .as_ref()
            .is_none_or(|(old, _)| old != tip)
        {
            self.tip_work_delivered = Some((tip.into(), now()));
        }
    }

    pub(super) fn prune_jobs(
        &mut self,
        config: &StratumConfig,
        observed_tip: Option<&RetentionTip>,
    ) {
        let now = now();
        let grace = self.stale_grace(config);
        self.jobs.retain(|issued| {
            if issued
                .job
                .wire
                .resume_expires_at
                .is_some_and(|expires| now >= expires)
            {
                return false;
            }
            // A retired same-tip job may have almost exhausted its retention
            // when the chain flips. Preserve it until THIS connection gets
            // replacement work and its newly anchored grace expires. The
            // coordinator still proves exactly-one-parent eligibility.
            if observed_tip
                .is_some_and(|tip| tip.keeps_previous(&issued.job.wire.previousblockhash, &grace))
            {
                return true;
            }
            issued.retired_at.is_none_or(|when| {
                now.saturating_duration_since(when).as_secs_f64()
                    <= config.job_retention_seconds.max(config.stale_grace_seconds)
            })
        });
        self.retained.prune(config, observed_tip, &grace);
    }
}

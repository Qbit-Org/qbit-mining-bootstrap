//! Same-connection graveyard: original work and identity survive active eviction.
use super::*;

pub(super) struct RetainedJobs<C> {
    jobs: HashMap<String, IssuedJob<C>>,
    order: VecDeque<String>,
}

impl<C> Default for RetainedJobs<C> {
    fn default() -> Self {
        Self {
            jobs: HashMap::new(),
            order: VecDeque::new(),
        }
    }
}

impl<C> RetainedJobs<C> {
    pub(super) fn get(&self, id: &str) -> Option<&IssuedJob<C>> {
        self.jobs.get(id)
    }

    pub(super) fn permit(&self, username: &str) -> Option<Arc<OwnedSemaphorePermit>> {
        self.jobs
            .values()
            .find(|job| job.worker.username == username)
            .and_then(|job| job.authorization_permit.clone())
    }

    pub(super) fn replace_payout(&mut self, replacement: &Job) {
        self.jobs.retain(|_, prior| {
            prior.job.wire.previousblockhash != replacement.previousblockhash
                || prior.job.wire.payout_revision == replacement.payout_revision
        });
        self.order.retain(|id| self.jobs.contains_key(id));
    }

    fn bury(&mut self, mut job: IssuedJob<C>, config: &StratumConfig, published_tip: Option<&str>) {
        let id = job.job.wire.job_id.clone();
        self.order.retain(|old| old != &id);
        // Legacy TTL starts at burial, not the earlier same-tip retirement.
        job.retired_at = Some(tokio::time::Instant::now().into_std());
        self.jobs.insert(id.clone(), job);
        self.order.push_back(id);
        // Capacity follows publication, even before this connection receives
        // replacement work. Delivery state owns grace timing, not this class.
        let is_same_tip = |job: &IssuedJob<C>| {
            published_tip.is_none_or(|tip| job.job.wire.previousblockhash == tip)
        };
        let mut same_tip = self.jobs.values().filter(|job| is_same_tip(job)).count();
        self.order.retain(|id| {
            if same_tip > config.max_jobs_per_connection
                && self.jobs.get(id).is_some_and(&is_same_tip)
            {
                self.jobs.remove(id);
                same_tip -= 1;
                false
            } else {
                true
            }
        });
        // At a flip the previous parent can own both the active set and its
        // same-tip graveyard (2N). The new parent's graveyard adds at most N.
        // A hard 3N bound also covers a temporarily unknown predecessor.
        while self.order.len() > config.max_jobs_per_connection.saturating_mul(3) {
            if let Some(id) = self.order.pop_front() {
                self.jobs.remove(&id);
            }
        }
    }

    pub(super) fn prune(
        &mut self,
        config: &StratumConfig,
        tip: Option<&RetentionTip>,
        grace: &StaleGrace,
    ) {
        let now = tokio::time::Instant::now().into_std();
        self.jobs.retain(|_, issued| {
            if issued
                .job
                .wire
                .resume_expires_at
                .is_some_and(|expiry| now >= expiry)
            {
                return false;
            }
            if let Some(tip) = tip.filter(|tip| tip.hash != issued.job.wire.previousblockhash) {
                return tip.keeps_previous(&issued.job.wire.previousblockhash, grace);
            }
            issued.retired_at.is_some_and(|buried| {
                now.saturating_duration_since(buried).as_secs_f64() <= config.job_retention_seconds
            })
        });
        self.order.retain(|id| self.jobs.contains_key(id));
    }
}

impl<C> Session<C> {
    pub(super) fn make_job_room(&mut self, config: &StratumConfig, tip: Option<&RetentionTip>) {
        while self.jobs.len() >= config.max_jobs_per_connection {
            if let Some(job) = self.jobs.pop_front() {
                self.retained
                    .bury(job, config, tip.map(|tip| tip.hash.as_str()));
            }
        }
    }
}

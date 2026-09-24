//! Same-connection graveyard: original work and identity survive active eviction.
use super::*;
use crate::codec::JobKind;

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

/// #478: `replacement` supersedes `prior` when it builds on the same parent at
/// another payout revision.
fn supersedes(replacement: &Job, prior: &Job) -> bool {
    prior.previousblockhash == replacement.previousblockhash
        && prior.payout_revision != replacement.payout_revision
}

/// #478 block capture (coordinator decision): superseded work stays
/// submittable for a BLOCK only. It releases its username reservation at once,
/// exactly as discarding it did before, and its kind makes `submit_share`
/// refuse it every credit path.
fn retire_to_block_only<C>(issued: &mut IssuedJob<C>) {
    issued.authorization_permit = None;
    issued.job.wire.kind = JobKind::BlockOnly;
}

impl<C> RetainedJobs<C> {
    pub(super) fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub(super) fn get(&self, id: &str) -> Option<&IssuedJob<C>> {
        self.jobs.get(id)
    }

    /// The reservation of the newest retained job of `username` that still
    /// holds one. Deterministic: burial order, not map iteration order, and a
    /// job without a reservation (block-only work, or a disabled limit) never
    /// hides one that has it.
    pub(super) fn permit(&self, username: &str) -> Option<Arc<OwnedSemaphorePermit>> {
        self.order
            .iter()
            .rev()
            .filter_map(|id| self.jobs.get(id))
            .filter(|job| job.worker.username == username)
            .find_map(|job| job.authorization_permit.clone())
    }

    /// Retire every retained job `replacement` supersedes to block-only, then
    /// hold block-only same-tip work to its cap.
    fn retire_superseded(
        &mut self,
        replacement: &Job,
        config: &StratumConfig,
        published_tip: Option<&str>,
    ) {
        for job in self.jobs.values_mut() {
            if supersedes(replacement, &job.job.wire) {
                retire_to_block_only(job);
            }
        }
        self.cap(JobKind::BlockOnly, config, published_tip);
    }

    /// Bury `job`, then bound its kind's same-tip graveyard.
    fn bury(&mut self, mut job: IssuedJob<C>, config: &StratumConfig, published_tip: Option<&str>) {
        let id = job.job.wire.job_id.clone();
        let kind = job.job.wire.kind;
        self.order.retain(|old| old != &id);
        // Legacy TTL starts at burial, not the earlier same-tip retirement.
        job.retired_at = Some(tokio::time::Instant::now().into_std());
        self.jobs.insert(id.clone(), job);
        self.order.push_back(id);
        self.cap(kind, config, published_tip);
    }

    /// At most N same-tip graveyard jobs of `kind`, evicting the oldest.
    /// Credit work and block-only work are capped separately, so evicting
    /// block-only work never touches credit work a reauthorization may still
    /// reuse, and the reverse. Capacity follows publication, even before this
    /// connection receives replacement work. Delivery state owns grace timing,
    /// not this class.
    fn cap(&mut self, kind: JobKind, config: &StratumConfig, published_tip: Option<&str>) {
        let capped = |job: &IssuedJob<C>| {
            job.job.wire.kind == kind
                && published_tip.is_none_or(|tip| job.job.wire.previousblockhash == tip)
        };
        let mut same_tip = self.jobs.values().filter(|job| capped(job)).count();
        self.order.retain(|id| {
            if same_tip > config.max_jobs_per_connection && self.jobs.get(id).is_some_and(&capped) {
                self.jobs.remove(id);
                same_tip -= 1;
                false
            } else {
                true
            }
        });
        // At a flip the previous parent can own both the active set and its
        // same-tip graveyard (2N). The new parent's graveyard adds at most N.
        // A hard 3N bound also covers a temporarily unknown predecessor and
        // same-tip block-only work.
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
                // Block-only work can be captured only on the active parent,
                // so it has nothing left to offer once the parent moves.
                return issued.job.wire.kind == JobKind::Credit
                    && tip.keeps_previous(&issued.job.wire.previousblockhash, grace);
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

    /// #478 block capture: a same-parent payout replacement no longer discards
    /// the work it supersedes. That work, live or already in the graveyard, is
    /// retired to block-only, so a block found on it can still be offered while
    /// its parent is the active tip. It holds no username reservation and
    /// `submit_share` refuses it every credit path. The graveyard caps
    /// block-only same-tip work at N per session; the retention TTL and the
    /// next parent change end it.
    pub(super) fn bury_superseded_same_parent(
        &mut self,
        replacement: &Job,
        config: &StratumConfig,
        tip: Option<&RetentionTip>,
    ) {
        let published_tip = tip.map(|tip| tip.hash.as_str());
        // Retire the graveyard first, so every cap below counts every
        // superseded job: at most N block-only same-tip jobs, never 2N.
        self.retained
            .retire_superseded(replacement, config, published_tip);
        let mut kept = VecDeque::with_capacity(self.jobs.len());
        while let Some(mut prior) = self.jobs.pop_front() {
            if supersedes(replacement, &prior.job.wire) {
                retire_to_block_only(&mut prior);
                self.retained.bury(prior, config, published_tip);
            } else {
                kept.push_back(prior);
            }
        }
        self.jobs = kept;
    }
}

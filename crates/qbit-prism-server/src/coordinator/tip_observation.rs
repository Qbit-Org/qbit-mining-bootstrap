//! Detected tips fence candidates immediately; published tips own miner credit.
use super::publication_authority::{AbsoluteDeadline, AuthorityView};
use super::*;
use tokio::time::Instant as MonotonicInstant;

pub(super) fn tip_hash(value: &Value) -> Option<&str> {
    value
        .as_str()
        .filter(|hash| hash.len() == 64 && hex::decode(hash).is_ok())
}

pub(super) struct SubmitAdmission {
    pub current: Arc<Prepared>,
    pub tip: TipView,
    pub lease: Option<PublishedLease>,
}

/// The coherent revision authenticated with the original balance digest. It
/// is a transaction fence, never a replacement for the job's issued revision.
pub(super) struct PublishedLease {
    identity: PreparedIdentity,
    published_tip: Option<(String, u64)>,
    readiness_epoch: u64,
    current_revision: i64,
    deadline: MonotonicInstant,
}

/// A missing observation cannot by itself prove that the original work expired.
pub(super) enum LeaseSelection {
    Selected(Arc<Prepared>, TipView),
    Stale,
    Unavailable,
}

pub(super) struct WorkAuthority {
    pub revision: i64,
    pub lease: Option<PublishedLease>,
    deadline: Option<AbsoluteDeadline>,
}

/// Original creation/recovery admission carried through later issue delivery.
/// Fields stay opaque: callers can retain the proof, never manufacture one.
#[derive(Clone)]
pub struct IssuanceAuthority {
    identity: Arc<PreparedIdentity>,
    readiness_epoch: u64,
    published_tip: Option<(String, u64)>,
    lease: Option<Arc<PublishedLease>>,
    deadline: Option<AbsoluteDeadline>,
    expires_at_ms: Option<i64>,
}

impl IssuanceAuthority {
    pub(super) fn absolute_expiry(&self) -> Option<i64> {
        self.expires_at_ms
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        self.deadline.map(|deadline| deadline.instant())
    }
}

impl PublishedLease {
    pub(super) fn revision_for(&self, prepared: &Prepared) -> Option<i64> {
        self.identity
            .matches(prepared)
            .then_some(self.current_revision)
    }

    pub(super) fn select(
        &self,
        view: &AuthorityView<'_>,
        config: &Config,
    ) -> Result<Option<(Arc<Prepared>, TipView)>> {
        // Preserve the existing admission/revalidation contract. The commit
        // fence additionally needs the cause of a refused selection.
        Ok(match self.select_with_cause(view, config)? {
            LeaseSelection::Selected(current, tip) => Some((current, tip)),
            LeaseSelection::Stale | LeaseSelection::Unavailable => None,
        })
    }

    pub(super) fn select_with_cause(
        &self,
        view: &AuthorityView<'_>,
        config: &Config,
    ) -> Result<LeaseSelection> {
        ensure!(
            view.readiness.generation == self.readiness_epoch && view.readiness.last_poll.is_some(),
            "node readiness changed during replacement lease admission"
        );
        let Some(current) = view.prepared.as_ref().filter(|p| self.identity.matches(p)) else {
            return Ok(LeaseSelection::Stale);
        };
        if view.tip.publication_stamp() != self.published_tip {
            return Ok(LeaseSelection::Stale);
        }
        let tip = view.tip.authority(
            config.submit_tip_max_age,
            config.template_refresh_failure_exit,
        );
        if tip.is_none() {
            // A non-refresh probe can observe a return to the published parent
            // without renewing its cached observation. No new publication or
            // original lease expiry is proven by that loss of ordinary proof.
            // An expired original bound while still diverged is positive stale
            // evidence even when the cached observation is also unavailable.
            return Ok(
                if view.tip.as_deref() != self.identity.parent.as_deref()
                    && MonotonicInstant::now() > self.deadline
                {
                    LeaseSelection::Stale
                } else {
                    LeaseSelection::Unavailable
                },
            );
        }
        let tip = tip.filter(|tip| {
            Some(tip.hash.as_str()) == self.identity.parent.as_deref()
                && ((tip.share_lease && MonotonicInstant::now() <= self.deadline)
                    || (view.tip.as_deref() == self.identity.parent.as_deref()
                        && self.current_revision == self.identity.revision))
        });
        if tip.as_ref().is_some_and(|tip| !tip.share_lease) {
            // Returning to the published parent grants only ordinary authority:
            // the original current revision and a fresh poll are still required.
            ensure!(
                view.readiness
                    .last_poll
                    .is_some_and(|poll| poll.elapsed() < config.health_timeout),
                "tip polling stale"
            );
        }
        Ok(match tip {
            Some(tip) => LeaseSelection::Selected(current.clone(), tip),
            None => LeaseSelection::Stale,
        })
    }
}

/// Immutable publication identity, including reconstructed copies of the same
/// dependency. A same-parent/revision payout alone cannot borrow its lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PreparedIdentity {
    key: String,
    generation: u64,
    fingerprint: String,
    window: WindowRef,
    revision: i64,
    parent: Option<String>,
}

impl PreparedIdentity {
    pub(super) fn matches(&self, prepared: &Prepared) -> bool {
        self.key == prepared.storage_key
            && self.generation == prepared.generation
            && self.fingerprint == prepared.fingerprint
            && self.window == prepared.window
            && self.revision == prepared.snapshot.payout_revision
            && self.parent.as_deref() == prepared.template["previousblockhash"].as_str()
    }
    #[cfg(test)]
    pub(super) fn from_stored(key: &str, prepared: &StoredPrepared, window: WindowRef) -> Self {
        Self {
            key: key.into(),
            generation: prepared.generation,
            fingerprint: prepared.fingerprint.clone(),
            window,
            revision: prepared.snapshot.payout_revision,
            parent: prepared.template["previousblockhash"]
                .as_str()
                .map(str::to_owned),
        }
    }

    pub(super) fn of(prepared: &Prepared) -> Self {
        Self {
            key: prepared.storage_key.clone(),
            generation: prepared.generation,
            fingerprint: prepared.fingerprint.clone(),
            window: prepared.window,
            revision: prepared.snapshot.payout_revision,
            parent: prepared.template["previousblockhash"]
                .as_str()
                .map(str::to_owned),
        }
    }

    /// Construct original resume authority without retaining a Prepared,
    /// Snapshot or AuditBundle. Expiry stays a separate fixed input.
    pub(super) fn from_compact(key: &str, record: &crate::ledger::CompactPrepared) -> Self {
        Self {
            key: key.into(),
            generation: record.generation,
            fingerprint: record.fingerprint.clone(),
            window: record.window,
            revision: record.payout_revision,
            parent: Some(record.parent_hash.clone()),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct TipView {
    pub hash: String,
    pub parent: Option<String>,
    pub transitioned: bool,
    /// Published authority is retained during a detected replacement build.
    pub share_lease: bool,
    observed_at: MonotonicInstant,
    sequence: u64,
}

#[derive(Default, Debug)]
pub struct TipState {
    current: Option<TipView>,
    published: Option<TipView>,
    divergence_started: Option<MonotonicInstant>,
    requested: u64,
    // Observation ordering changes on every poll; this generation changes
    // only when a prepared publication is installed or replaced.
    publication_generation: u64,
}

impl TipState {
    pub(super) fn publication_stamp(&self) -> Option<(String, u64)> {
        self.published
            .as_ref()
            .map(|tip| (tip.hash.clone(), self.publication_generation))
    }

    /// Freeze the selected lease interval before waiting on its economic
    /// proof. A later return/redeparture may grant a new admission a new
    /// interval, but cannot renew an operation already waiting to persist.
    fn lease_deadline(
        &self,
        max_age: Duration,
        build_budget: Duration,
    ) -> Result<MonotonicInstant> {
        let ordinary = self
            .published
            .as_ref()
            .context("published tip missing")?
            .observed_at
            .checked_add(max_age)
            .context("tip authority deadline overflow")?;
        let replacement = self
            .divergence_started
            .filter(|_| !build_budget.is_zero())
            .map(|at| {
                at.checked_add(build_budget)
                    .context("replacement lease deadline overflow")
            })
            .transpose()?;
        Ok(replacement.map_or(ordinary, |at| ordinary.max(at)))
    }

    /// A startup baseline fences work but cannot open stale grace.
    pub fn baseline(hash: String) -> Self {
        let mut state = Self::default();
        let sequence = state.reserve();
        state.observe(&hash, sequence, true);
        state.publish(&hash).expect("baseline observation exists");
        state
    }

    pub fn as_deref(&self) -> Option<&str> {
        self.current.as_ref().map(|tip| tip.hash.as_str())
    }

    /// Background settlement yields while the detected tip awaits publication,
    /// for at most the replacement-build budget from the first departure. Failed
    /// refreshes and newer tips never renew it, so a publication outage cannot
    /// strand settlement; a zero budget disables the yield like the lease.
    pub(crate) fn refresh_pending(&self, build_budget: Duration) -> bool {
        let unpublished = self.current.as_ref().is_some_and(|current| {
            self.published
                .as_ref()
                .is_none_or(|published| published.hash != current.hash)
        });
        unpublished
            && !build_budget.is_zero()
            && self
                .divergence_started
                .is_some_and(|at| at.elapsed() <= build_budget)
    }

    /// Whether a tip recorded after `since` differs from `hash`. Only evidence
    /// newer than a pass's own chain view can supersede that view. An older
    /// observation is stale, not a newer tip: yielding to it would hold
    /// settlement until a blocked or absent refresh caught up, with no budget.
    pub(crate) fn superseded_since(&self, hash: &str, since: MonotonicInstant) -> bool {
        self.current
            .as_ref()
            .is_some_and(|tip| tip.hash != hash && tip.observed_at > since)
    }

    pub(super) fn reserve(&mut self) -> u64 {
        self.requested = self
            .requested
            .checked_add(1)
            .expect("tip sequence exhausted");
        self.requested
    }

    pub(super) fn observe(&mut self, hash: &str, sequence: u64, from_refresh: bool) {
        if self
            .current
            .as_ref()
            .is_some_and(|tip| tip.sequence > sequence)
        {
            return;
        }
        let now = MonotonicInstant::now();
        if let Some(current) = self.current.as_mut().filter(|tip| tip.hash == hash) {
            current.observed_at = now;
            current.sequence = sequence;
        } else {
            self.current = Some(TipView {
                hash: hash.into(),
                parent: None,
                transitioned: false,
                share_lease: false,
                observed_at: now,
                sequence,
            });
        }
        if from_refresh {
            match self.published.as_mut() {
                Some(published) if published.hash == hash => {
                    published.observed_at = now;
                    self.divergence_started = None;
                }
                // A -> B -> C and retries never renew the first departure; an
                // unpublished startup tip anchors its budget the same way.
                _ => {
                    self.divergence_started.get_or_insert(now);
                }
            }
        }
    }

    /// Called while holding the prepared-publication lock, only after work
    /// construction, persistence and the final chain/revision checks succeed.
    pub(super) fn publish(&mut self, hash: &str) -> Result<()> {
        let generation = self
            .publication_generation
            .checked_add(1)
            .context("prepared publication generation exhausted")?;
        self.update_published_tip(hash)?;
        self.publication_generation = generation;
        Ok(())
    }

    /// A cached refresh revalidates the same prepared work. Update its tip
    /// freshness without revoking proofs captured before the poll.
    pub(super) fn refresh_publication(&mut self, hash: &str) -> Result<()> {
        ensure!(
            self.published.as_ref().is_some_and(|tip| tip.hash == hash),
            "cached work publication changed"
        );
        self.update_published_tip(hash)
    }

    fn update_published_tip(&mut self, hash: &str) -> Result<()> {
        let mut current = self
            .current
            .clone()
            .filter(|tip| tip.hash == hash)
            .context("tip observation superseded before work publication")?;
        current.transitioned = self
            .published
            .as_ref()
            .is_some_and(|old| old.hash != hash || old.transitioned);
        current.observed_at = MonotonicInstant::now();
        self.published = Some(current);
        self.divergence_started = None;
        Ok(())
    }

    pub(super) fn retention_hint(&self) -> Option<crate::stratum::RetentionTip> {
        self.published
            .as_ref()
            .map(|tip| crate::stratum::RetentionTip {
                hash: tip.hash.clone(),
                parent: tip.parent.clone(),
                transitioned: tip.transitioned,
            })
    }

    pub(super) fn authority(&self, max_age: Duration, build_budget: Duration) -> Option<TipView> {
        let published = self.published.as_ref()?;
        if max_age.is_zero() {
            return None;
        }
        let diverged = self
            .current
            .as_ref()
            .is_some_and(|tip| tip.hash != published.hash);
        let lease = diverged
            && !build_budget.is_zero()
            && self
                .divergence_started
                .is_some_and(|at| at.elapsed() <= build_budget);
        if published.observed_at.elapsed() > max_age && !lease {
            return None;
        }
        let mut selected = published.clone();
        selected.share_lease = diverged;
        Some(selected)
    }

    #[cfg(test)]
    pub(super) fn divergence_for_test(&self) -> Option<MonotonicInstant> {
        self.divergence_started
    }

    #[cfg(test)]
    pub(super) fn expire_lease_for_test(&mut self, age: Duration) {
        self.age_for_test(age);
        self.divergence_started = Some(MonotonicInstant::now() - age);
    }

    #[cfg(test)]
    pub(super) fn age_for_test(&mut self, age: Duration) {
        if let Some(tip) = self.published.as_mut() {
            tip.observed_at -= age;
        }
    }
}

impl Coordinator {
    pub(super) async fn begin_issuance_authority(
        &self,
        identity: PreparedIdentity,
        readiness_epoch: u64,
        expires_at_ms: Option<i64>,
    ) -> Result<Option<IssuanceAuthority>> {
        let view = self.authority_view().await;
        ensure!(
            view.readiness.generation == readiness_epoch,
            "node readiness changed during work admission"
        );
        let published_tip = view.tip.publication_stamp();
        drop(view);
        let mut proof = IssuanceAuthority {
            identity: Arc::new(identity),
            readiness_epoch,
            published_tip,
            lease: None,
            deadline: None,
            expires_at_ms,
        };
        Ok(self
            .revalidate_issuance_authority(&mut proof, expires_at_ms)
            .await?
            .map(|_| proof))
    }

    /// Preserve the first selected lease and deadline across every build,
    /// reconstruction, persistence and repair wait; never recapture its epoch.
    pub(super) async fn revalidate_issuance_authority(
        &self,
        proof: &mut IssuanceAuthority,
        expires_at_ms: Option<i64>,
    ) -> Result<Option<i64>> {
        let expires_at_ms = match (proof.expires_at_ms, expires_at_ms) {
            (Some(original), Some(next)) => Some(original.min(next)),
            (original, next) => original.or(next),
        };
        let Some(current) = self
            .work_authority_in_epoch(&proof.identity, expires_at_ms, proof.readiness_epoch)
            .await?
        else {
            return Ok(None);
        };
        let view = self.authority_view().await;
        ensure!(
            view.readiness.generation == proof.readiness_epoch,
            "node readiness changed during work admission"
        );
        if view.tip.publication_stamp() != proof.published_tip {
            return Ok(None);
        }
        if let Some(lease) = &proof.lease {
            if lease.select(&view, &self.config)?.is_none() {
                return Ok(None);
            }
        } else if let Some(lease) = current.lease {
            if lease.select(&view, &self.config)?.is_none() {
                return Ok(None);
            }
            proof.lease = Some(Arc::new(lease));
        }
        proof.deadline = match (proof.deadline, current.deadline) {
            (Some(original), Some(next)) => Some(original.min(next)),
            (original, next) => original.or(next),
        };
        proof.expires_at_ms = expires_at_ms;
        if proof.deadline.is_some_and(|deadline| !deadline.live()) {
            return Ok(None);
        }
        Ok(Some(current.revision))
    }

    /// Current publication may issue/recover miner work during its bounded
    /// replacement lease. Persistence still locks the *current* DB revision;
    /// as-issued payloads retain their original economic snapshot.
    #[cfg(test)]
    pub(super) async fn issued_work_revision(&self, prepared: &Prepared) -> Result<Option<i64>> {
        self.work_authority_revision(&PreparedIdentity::of(prepared), None)
            .await
    }

    /// A slim original authority view plus an optional absolute issued expiry.
    /// The caller owns its original deadline; no stage creates a new budget.
    #[cfg(test)]
    pub(super) async fn work_authority_revision(
        &self,
        identity: &PreparedIdentity,
        expires_at_ms: Option<i64>,
    ) -> Result<Option<i64>> {
        let readiness_epoch = self.readiness.read().await.generation;
        self.work_authority_revision_in_epoch(identity, expires_at_ms, readiness_epoch)
            .await
    }

    #[cfg(test)]
    pub(super) async fn work_authority_revision_in_epoch(
        &self,
        identity: &PreparedIdentity,
        expires_at_ms: Option<i64>,
        readiness_epoch: u64,
    ) -> Result<Option<i64>> {
        Ok(self
            .work_authority_in_epoch(identity, expires_at_ms, readiness_epoch)
            .await?
            .map(|authority| authority.revision))
    }

    pub(super) async fn work_authority_in_epoch(
        &self,
        identity: &PreparedIdentity,
        expires_at_ms: Option<i64>,
        readiness_epoch: u64,
    ) -> Result<Option<WorkAuthority>> {
        let clock = if let Some(expires) = expires_at_ms {
            let requested_at = MonotonicInstant::now();
            let now = self.work_ledger.now_ms().await?;
            let clock = AbsoluteDeadline::from_database(now, requested_at, expires)?;
            if !clock.live() {
                return Ok(None);
            }
            Some(clock)
        } else {
            None
        };
        // Preserve the existing database-first failure priority, even when a
        // later coherent lease proof will supply the transaction revision.
        let revision = self.work_ledger.payout_revision().await?;
        // Match publication lock order: prepared -> observed tip. A lease
        // belongs to the published payout, never an older same-parent payout.
        let view = self.authority_view().await;
        let published_work = &view.prepared;
        let readiness = &view.readiness;
        let selected = &view.tip;
        ensure!(
            readiness.generation == readiness_epoch,
            "node readiness changed during work admission"
        );
        let parent = identity.parent.as_deref().context("job parent missing")?;
        let authority = selected.authority(
            self.config.submit_tip_max_age,
            self.config.template_refresh_failure_exit,
        );
        let leased = authority
            .as_ref()
            .is_some_and(|tip| tip.hash == parent && tip.share_lease);
        let last_poll = readiness.last_poll.context("tip polling unavailable")?;
        if leased
            && !published_work
                .as_deref()
                .is_some_and(|p| identity.matches(p))
        {
            return Ok(None);
        }
        if selected.as_deref() != Some(parent) && !leased {
            // Preserve the unavailable-current-publication contract. A miss
            // describes retired requested work only when the current work
            // still has ordinary tip authority or a replacement lease.
            let current_parent = published_work
                .as_ref()
                .and_then(|p| p.template["previousblockhash"].as_str());
            let current_leased = authority
                .as_ref()
                .is_some_and(|tip| tip.share_lease && Some(tip.hash.as_str()) == current_parent);
            ensure!(
                current_parent.is_some()
                    && (current_parent == selected.as_deref() || current_leased),
                "new tip work is pending"
            );
            ensure!(
                last_poll.elapsed() < self.config.health_timeout || current_leased,
                "tip polling stale"
            );
            return Ok(None);
        }
        ensure!(
            last_poll.elapsed() < self.config.health_timeout || leased,
            "tip polling stale"
        );
        let published_tip = selected.publication_stamp();
        let deadline = leased
            .then(|| {
                selected.lease_deadline(
                    self.config.submit_tip_max_age,
                    self.config.template_refresh_failure_exit,
                )
            })
            .transpose()?;
        drop(view);
        let admitted = if let Some(deadline) = deadline {
            self.prove_published_lease(identity.clone(), published_tip, readiness_epoch, deadline)
                .await?
                .map(|(_, _, lease)| WorkAuthority {
                    revision: lease.current_revision,
                    lease: Some(lease),
                    deadline: clock,
                })
        } else {
            (identity.revision == revision).then_some(WorkAuthority {
                revision,
                lease: None,
                deadline: clock,
            })
        };
        if clock.is_some_and(|clock| !clock.live()) {
            return Ok(None);
        }
        // A known superseded payout is an unknown/retired resumable job,
        // not a failed database lookup. Keep that outcome distinct.
        Ok(admitted)
    }

    async fn prove_published_lease(
        &self,
        identity: PreparedIdentity,
        published_tip: Option<(String, u64)>,
        readiness_epoch: u64,
        deadline: MonotonicInstant,
    ) -> Result<Option<(Arc<Prepared>, TipView, PublishedLease)>> {
        // This coherent proof reads/hashes the current balance set per lease
        // admission. Caching just the publication key would miss mid-lease
        // balance changes; revisit only with a transactionally versioned digest
        // producer if recipient-count cost makes this bounded path too costly.
        let state = self.work_ledger.payout_state().await?;
        let lease = PublishedLease {
            identity,
            published_tip,
            readiness_epoch,
            current_revision: state.payout_revision,
            deadline,
        };
        let selected = self.select_validated_lease(&lease).await?;
        if selected.as_ref().is_some_and(|(_, tip)| tip.share_lease)
            && state.prior_balances_digest != lease.identity.window.prior_balances_digest
        {
            return Ok(None);
        }
        Ok(selected.map(|(current, tip)| (current, tip, lease)))
    }

    async fn select_validated_lease(
        &self,
        lease: &PublishedLease,
    ) -> Result<Option<(Arc<Prepared>, TipView)>> {
        lease.select(&self.authority_view().await, &self.config)
    }

    pub(super) async fn revalidate_published_lease(&self, lease: &PublishedLease) -> Result<bool> {
        Ok(self.select_validated_lease(lease).await?.is_some())
    }

    pub(super) async fn submit_admission(&self) -> Result<SubmitAdmission, StratumError> {
        // Match publication order. A tip's lease belongs to the payout selected
        // with it, never a prepared snapshot captured before an awaited lookup.
        let lease_candidate = {
            let view = self.tip_publication_view().await;
            let current = view
                .prepared
                .clone()
                .ok_or_else(|| protocol_error("pool-closed", "no current work"))?;
            let state = &view.tip;
            if let Some(tip) = state.authority(
                self.config.submit_tip_max_age,
                self.config.template_refresh_failure_exit,
            ) {
                if !tip.share_lease {
                    return Ok(SubmitAdmission {
                        current,
                        tip,
                        lease: None,
                    });
                }
                let deadline = state
                    .lease_deadline(
                        self.config.submit_tip_max_age,
                        self.config.template_refresh_failure_exit,
                    )
                    .map_err(|_| {
                        protocol_error(
                            "backend-rpc-unavailable",
                            "current chain state is unavailable",
                        )
                    })?;
                Some((
                    PreparedIdentity::of(&current),
                    state.publication_stamp(),
                    deadline,
                ))
            } else {
                None
            }
        };
        if let Some((identity, published_tip, deadline)) = lease_candidate {
            let readiness_epoch = self.readiness.read().await.generation;
            let (current, tip, lease) = self
                .prove_published_lease(identity, published_tip, readiness_epoch, deadline)
                .await
                .map_err(|_| {
                    protocol_error(
                        "backend-rpc-unavailable",
                        "current payout state is unavailable",
                    )
                })?
                .ok_or_else(|| protocol_error("stale-job", "stale job"))?;
            return Ok(SubmitAdmission {
                current,
                tip,
                lease: Some(lease),
            });
        }
        let result = self
            .rpc
            .call("getbestblockhash", json!([]))
            .await
            .map_err(|_| {
                protocol_error(
                    "backend-rpc-unavailable",
                    "current chain state is unavailable",
                )
            })?;
        let hash = tip_hash(&result).ok_or_else(|| {
            protocol_error(
                "backend-rpc-unavailable",
                "current chain state is unavailable",
            )
        })?;
        // Work may have published during the RPC. Select that complete pair
        // now, but only a publication matching the answer supplies provenance.
        // RPC itself cannot publish a transition or restore a disabled lease.
        let view = self.tip_publication_view().await;
        let current = view
            .prepared
            .clone()
            .ok_or_else(|| protocol_error("pool-closed", "no current work"))?;
        let state = &view.tip;
        let mut tip = state
            .published
            .clone()
            .filter(|tip| tip.hash == hash)
            .unwrap_or_else(|| TipView {
                hash: hash.into(),
                parent: None,
                transitioned: false,
                share_lease: false,
                observed_at: MonotonicInstant::now(),
                sequence: 0,
            });
        tip.share_lease = false;
        Ok(SubmitAdmission {
            current,
            tip,
            lease: None,
        })
    }

    #[cfg(test)]
    pub(super) async fn submit_tip_view(&self) -> Result<TipView, StratumError> {
        Ok(self.submit_admission().await?.tip)
    }

    pub(super) async fn tip_parent(&self, selected: &TipView) -> Result<String> {
        if let Some(parent) = &selected.parent {
            return Ok(parent.clone());
        }
        let header = self
            .rpc
            .call("getblockheader", json!([selected.hash]))
            .await?;
        let parent = match header["previousblockhash"].as_str() {
            Some(parent) if parent.len() == 64 && hex::decode(parent).is_ok() => parent.to_owned(),
            None if header["height"] == 0 => String::new(),
            _ => anyhow::bail!("tip predecessor is unavailable"),
        };
        // A slow lookup may answer its caller, never overwrite a newer view.
        let mut state = self.observed_tip.write().await;
        let TipState {
            current, published, ..
        } = &mut *state;
        for tip in [current, published] {
            if let Some(current) = tip
                .as_mut()
                .filter(|tip| tip.hash == selected.hash && tip.sequence == selected.sequence)
            {
                current.parent = Some(parent.clone());
            }
        }
        Ok(parent)
    }

    pub(super) async fn cache_tip_parent(&self, hash: &str) -> Result<String> {
        let selected = self
            .observed_tip
            .read()
            .await
            .current
            .clone()
            .filter(|tip| tip.hash == hash)
            .context("tip observation superseded")?;
        self.tip_parent(&selected).await
    }
}

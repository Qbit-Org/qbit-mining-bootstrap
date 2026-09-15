//! Detected tips fence candidates immediately; published tips own miner credit.
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
}

impl PublishedLease {
    pub(super) fn revision_for(&self, identity: &PreparedIdentity) -> Option<i64> {
        (self.identity == *identity).then_some(self.current_revision)
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

    /// Activation can construct the same slim authority view without retaining
    /// a Prepared, Snapshot or AuditBundle. Expiry stays a separate fixed input.
    #[allow(dead_code)]
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
}

impl TipState {
    pub(super) fn publication_stamp(&self) -> Option<(String, u64)> {
        self.published
            .as_ref()
            .map(|tip| (tip.hash.clone(), tip.sequence))
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
            if let Some(published) = self.published.as_mut() {
                if published.hash == hash {
                    published.observed_at = now;
                    self.divergence_started = None;
                } else {
                    // A -> B -> C and retries never renew the first departure.
                    self.divergence_started.get_or_insert(now);
                }
            }
        }
    }

    /// Called while holding the prepared-publication lock, only after work
    /// construction, persistence and the final chain/revision checks succeed.
    pub(super) fn publish(&mut self, hash: &str) -> Result<()> {
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
    /// Current publication may issue/recover miner work during its bounded
    /// replacement lease. Persistence still locks the *current* DB revision;
    /// as-issued payloads retain their original economic snapshot.
    pub(super) async fn issued_work_revision(&self, prepared: &Prepared) -> Result<Option<i64>> {
        self.work_authority_revision(&PreparedIdentity::of(prepared), None)
            .await
    }

    /// A slim original authority view plus an optional absolute issued expiry.
    /// The caller owns its original deadline; no stage creates a new budget.
    pub(super) async fn work_authority_revision(
        &self,
        identity: &PreparedIdentity,
        expires_at_ms: Option<i64>,
    ) -> Result<Option<i64>> {
        let readiness_epoch = self.readiness.read().await.generation;
        self.work_authority_revision_in_epoch(identity, expires_at_ms, readiness_epoch)
            .await
    }

    pub(super) async fn work_authority_revision_in_epoch(
        &self,
        identity: &PreparedIdentity,
        expires_at_ms: Option<i64>,
        readiness_epoch: u64,
    ) -> Result<Option<i64>> {
        let clock = if let Some(expires) = expires_at_ms {
            let requested_at = MonotonicInstant::now();
            let now = self.work_ledger.now_ms().await?;
            if now >= expires {
                return Ok(None);
            }
            Some((now, requested_at, expires))
        } else {
            None
        };
        let revision = self.work_ledger.payout_revision().await?;
        // Match publication lock order: prepared -> observed tip. A lease
        // belongs to the published payout, never an older same-parent payout.
        let published_work = self.prepared.read().await;
        let readiness = self.readiness.read().await;
        let selected = self.observed_tip.read().await;
        ensure!(
            readiness.generation == readiness_epoch,
            "node readiness changed during work admission"
        );
        let parent = identity.parent.as_deref().context("job parent missing")?;
        let leased = selected
            .authority(
                self.config.submit_tip_max_age,
                self.config.template_refresh_failure_exit,
            )
            .is_some_and(|tip| tip.hash == parent && tip.share_lease);
        let last_poll = readiness.last_poll.context("tip polling unavailable")?;
        if leased
            && !published_work
                .as_deref()
                .is_some_and(|p| PreparedIdentity::of(p) == *identity)
        {
            return Ok(None);
        }
        if selected.as_deref() != Some(parent) && !leased {
            // A known retired parent is a miss. It must not turn Stratum's
            // retained-job fallback into an apparent backend outage.
            return Ok(None);
        }
        ensure!(
            last_poll.elapsed() < self.config.health_timeout || leased,
            "tip polling stale"
        );
        let published_tip = selected.publication_stamp();
        drop(selected);
        drop(readiness);
        drop(published_work);
        let admitted_revision = if leased {
            self.prove_published_lease(identity.clone(), published_tip, readiness_epoch)
                .await?
                .map(|(_, _, lease)| lease.current_revision)
        } else {
            (identity.revision == revision).then_some(revision)
        };
        if let Some((now, requested_at, expires)) = clock {
            let elapsed_ms = i64::try_from(requested_at.elapsed().as_millis())
                .context("issued expiry clock overflow")?;
            if now.checked_add(elapsed_ms).is_none_or(|now| now >= expires) {
                return Ok(None);
            }
        }
        // A known superseded payout is an unknown/retired resumable job,
        // not a failed database lookup. Keep that outcome distinct.
        Ok(admitted_revision)
    }

    async fn prove_published_lease(
        &self,
        identity: PreparedIdentity,
        published_tip: Option<(String, u64)>,
        readiness_epoch: u64,
    ) -> Result<Option<(Arc<Prepared>, TipView, PublishedLease)>> {
        let state = self.work_ledger.payout_state().await?;
        let lease = PublishedLease {
            identity,
            published_tip,
            readiness_epoch,
            current_revision: state.payout_revision,
        };
        let selected = self.select_validated_lease(&lease).await?;
        if state.prior_balances_digest != lease.identity.window.prior_balances_digest {
            return Ok(None);
        }
        Ok(selected.map(|(current, tip)| (current, tip, lease)))
    }

    async fn select_validated_lease(
        &self,
        lease: &PublishedLease,
    ) -> Result<Option<(Arc<Prepared>, TipView)>> {
        let prepared = self.prepared.read().await;
        let readiness = self.readiness.read().await;
        let selected = self.observed_tip.read().await;
        ensure!(
            readiness.generation == lease.readiness_epoch && readiness.last_poll.is_some(),
            "node readiness changed during replacement lease admission"
        );
        let Some(current) = prepared
            .as_ref()
            .filter(|p| PreparedIdentity::of(p) == lease.identity)
        else {
            return Ok(None);
        };
        if selected.publication_stamp() != lease.published_tip {
            return Ok(None);
        }
        let tip = selected
            .authority(
                self.config.submit_tip_max_age,
                self.config.template_refresh_failure_exit,
            )
            .filter(|tip| {
                tip.share_lease && Some(tip.hash.as_str()) == lease.identity.parent.as_deref()
            });
        Ok(tip.map(|tip| (current.clone(), tip)))
    }

    pub(super) async fn revalidate_published_lease(&self, lease: &PublishedLease) -> Result<bool> {
        Ok(self.select_validated_lease(lease).await?.is_some())
    }

    pub(super) async fn submit_admission(&self) -> Result<SubmitAdmission, StratumError> {
        // Match publication order. A tip's lease belongs to the payout selected
        // with it, never a prepared snapshot captured before an awaited lookup.
        // Readiness is checked separately; never take it after observed_tip.
        let lease_candidate = {
            let prepared = self.prepared.read().await;
            let current = prepared
                .clone()
                .ok_or_else(|| protocol_error("pool-closed", "no current work"))?;
            let state = self.observed_tip.read().await;
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
                Some((PreparedIdentity::of(&current), state.publication_stamp()))
            } else {
                None
            }
        };
        if let Some((identity, published_tip)) = lease_candidate {
            let readiness_epoch = self.readiness.read().await.generation;
            let (current, tip, lease) = self
                .prove_published_lease(identity, published_tip, readiness_epoch)
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
        let prepared = self.prepared.read().await;
        let current = prepared
            .clone()
            .ok_or_else(|| protocol_error("pool-closed", "no current work"))?;
        let state = self.observed_tip.read().await;
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

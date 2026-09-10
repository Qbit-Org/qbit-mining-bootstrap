//! Detected tips fence candidates immediately; published tips own miner credit.
use super::*;
use tokio::time::Instant as MonotonicInstant;

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
        let revision = self.work_ledger.payout_revision().await?;
        // Match publication lock order: prepared -> observed tip. A lease
        // belongs to the published payout, never an older same-parent payout.
        let published_work = self.prepared.read().await;
        let selected = self.observed_tip.read().await;
        let parent = prepared.template["previousblockhash"]
            .as_str()
            .context("job parent missing")?;
        let leased = selected
            .authority(
                self.config.submit_tip_max_age,
                self.config.template_refresh_failure_exit,
            )
            .is_some_and(|tip| tip.hash == parent && tip.share_lease)
            && published_work.as_ref().is_some_and(|current| {
                current.snapshot.payout_revision == prepared.snapshot.payout_revision
                    && current.template["previousblockhash"].as_str() == Some(parent)
            });
        ensure!(
            selected.as_deref() == Some(parent) || leased,
            "new tip work is pending"
        );
        drop(selected);
        drop(published_work);
        let last_poll = self
            .readiness
            .read()
            .await
            .last_poll
            .context("tip polling unavailable")?;
        ensure!(
            last_poll.elapsed() < self.config.health_timeout || leased,
            "tip polling stale"
        );
        // A known superseded payout is an unknown/retired resumable job,
        // not a failed database lookup. Keep that outcome distinct.
        Ok((prepared.snapshot.payout_revision == revision || leased).then_some(revision))
    }

    pub(super) async fn submit_tip_view(&self) -> Result<TipView, StratumError> {
        // Hash, age, predecessor and publication provenance are selected under
        // ONE lock. Later I/O cannot tear the selected point-in-time view.
        let published = {
            let state = self.observed_tip.read().await;
            if let Some(tip) = state.authority(
                self.config.submit_tip_max_age,
                self.config.template_refresh_failure_exit,
            ) {
                return Ok(tip);
            }
            state.published.clone()
        };
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
        let hash = result
            .as_str()
            .filter(|hash| hash.len() == 64 && hex::decode(hash).is_ok())
            .ok_or_else(|| {
                protocol_error(
                    "backend-rpc-unavailable",
                    "current chain state is unavailable",
                )
            })?;
        // Submit RPC cannot publish authority or open refresh-anchored grace.
        Ok(published
            .filter(|tip| tip.hash == hash)
            .unwrap_or_else(|| TipView {
                hash: hash.into(),
                parent: None,
                transitioned: false,
                share_lease: false,
                observed_at: MonotonicInstant::now(),
                sequence: 0,
            }))
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

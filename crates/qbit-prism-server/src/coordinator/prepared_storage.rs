//! Retain the exact prepared dependency when publishing compact issued work.
use super::publication_authority::AbsoluteDeadline;
use super::tip_observation::PreparedIdentity;
use super::*;
use crate::ledger::{CompactRepair, IssuedJobSave};

pub(super) mod compact;

/// One issue operation retains its original identity, epoch, bytes and expiry
/// across dependency repair and every storage wait. A lease selected by any
/// attempt stays bound to that exact publication through final delivery.
struct IssuedPersistence<'a> {
    job: &'a MiningJob<JobContext>,
    payload: Value,
    expires_at_ms: i64,
    deadline: AbsoluteDeadline,
    authority: IssuanceAuthority,
}

impl Coordinator {
    pub(super) async fn save_issued_record(
        &self,
        worker: &Worker,
        job: &MiningJob<JobContext>,
        version_mask: u32,
        ttl: Duration,
    ) -> Result<()> {
        let readiness_epoch = self.readiness.read().await.generation;
        let requested_at = tokio::time::Instant::now();
        let now_ms = self.work_ledger.now_ms().await?;
        let seconds = ttl
            .as_secs()
            .checked_add(u64::from(ttl.subsec_nanos() != 0))
            .and_then(|seconds| i64::try_from(seconds).ok())
            .context("job TTL overflow")?;
        ensure!(seconds > 0, "job TTL must be positive");
        let expires_at_ms = now_ms
            .checked_add(seconds.checked_mul(1000).context("job TTL overflow")?)
            .context("job expiry overflow")?;
        let authority = if let Some(original) = &job.context.issuance_authority {
            (**original).clone()
        } else {
            self.begin_issuance_authority(
                PreparedIdentity::of(&job.context.prepared),
                readiness_epoch,
                None,
            )
            .await?
            .context("payout snapshot stale")?
        };
        let expires_at_ms = authority
            .absolute_expiry()
            .map_or(expires_at_ms, |original| original.min(expires_at_ms));
        let record = StoredJob {
            prepared_key: job.context.prepared.storage_key.clone(),
            worker: worker.clone(),
            extranonce1: job.wire.extranonce1.clone(),
            extranonce2_size: job.wire.extranonce2_size,
            share_target_hex: job.wire.share_target.to_str_radix(16),
            share_difficulty: job.wire.share_difficulty,
            version_mask,
            expires_at_ms,
        };
        let mut issued = IssuedPersistence {
            job,
            payload: serde_json::to_value(record)?,
            expires_at_ms,
            deadline: AbsoluteDeadline::from_database(now_ms, requested_at, expires_at_ms)?,
            authority,
        };
        if self.save_with_dependency(&mut issued, None).await? == IssuedJobSave::Saved {
            return Ok(());
        }
        let prepared = &job.context.prepared;
        // Followers retry compact persistence after the first repair completes.
        // The guard also lives through actual blocking work if its waiter dies.
        let repair = prepared.repair.clone().lock_owned().await;
        if self.save_with_dependency(&mut issued, None).await? == IssuedJobSave::Saved {
            return Ok(());
        }
        let permit = self.build_slots.clone().acquire_owned().await?;
        let original = prepared.reservation.clone();
        #[cfg(test)]
        let probe = prepared.repair_probe.lock().unwrap().clone();
        let admitted = compact::CompactOwner::new((original, repair, permit));
        let encoded = admitted
            .spawn_blocking(move |(source, repair, permit)| {
                // Bind admission first so later locals drop before it on error.
                let admission = permit;
                let repair_guard = repair;
                let original = source;
                #[cfg(test)]
                if let Some(probe) = probe {
                    probe.block();
                }
                let encoded = original.encode_repair()?;
                Ok::<_, anyhow::Error>(compact::CompactOwner::new((
                    encoded,
                    repair_guard,
                    admission,
                )))
            })
            .await??;
        let (encoded, repair_guard, admission) = encoded.into_inner();
        let serialized = compact::CompactOwner::new((encoded, repair_guard));
        drop(admission);
        // Both admission and the transaction's current revision are checked
        // again after waiting. The original child deadline/payload stay fixed.
        ensure!(
            self.save_with_dependency(&mut issued, Some(&serialized.0))
                .await?
                == IssuedJobSave::Saved,
            "prepared dependency repair did not save issued work"
        );
        Ok(())
    }

    async fn save_with_dependency(
        &self,
        issued: &mut IssuedPersistence<'_>,
        repair: Option<&CompactRepair>,
    ) -> Result<IssuedJobSave> {
        let revision = self.revalidate_issued(issued).await?;
        let prepared = &issued.job.context.prepared;
        let saved = if repair.is_none() {
            self.issued_batcher
                .save(
                    &issued.job.wire.job_id,
                    &issued.payload,
                    revision,
                    &issued.job.wire.previousblockhash,
                    issued.expires_at_ms,
                    prepared.reservation.dependency(&prepared.storage_key),
                    issued.deadline.instant().into(),
                )
                .await?
        } else {
            self.work_ledger
                .save_issued_job_compact(
                    &issued.job.wire.job_id,
                    &issued.payload,
                    revision,
                    &issued.job.wire.previousblockhash,
                    issued.expires_at_ms,
                    prepared.reservation.dependency(&prepared.storage_key),
                    repair,
                )
                .await?
        };
        if saved == IssuedJobSave::Saved {
            // A transaction may leave an immutable row after revocation, but
            // that row must never be delivered with the old admission proof.
            self.revalidate_issued(issued).await?;
        }
        Ok(saved)
    }

    async fn revalidate_issued(&self, issued: &mut IssuedPersistence<'_>) -> Result<i64> {
        let revision = self
            .revalidate_issuance_authority(&mut issued.authority, Some(issued.expires_at_ms))
            .await?
            .context("payout snapshot stale")?;
        // No later database clock read or repair retry renews this deadline.
        ensure!(issued.deadline.live(), "issued job deadline elapsed");
        Ok(revision)
    }
}

/// Controls the real cold serializer's I/O boundary in ungated cancellation tests.
#[cfg(test)]
#[derive(Default)]
pub(super) struct RepairProbe {
    pub entered: Notify,
    pub calls: AtomicU64,
    released: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
impl RepairProbe {
    pub(super) fn block(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }

    pub fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

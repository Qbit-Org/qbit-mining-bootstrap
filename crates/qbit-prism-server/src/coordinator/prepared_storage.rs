//! Retain the exact prepared dependency when publishing compact issued work.
use super::*;
use crate::ledger::{IssuedJobSave, PreparedDependency};

impl Coordinator {
    pub(super) async fn save_issued_record(
        &self,
        worker: &Worker,
        job: &MiningJob<JobContext>,
        version_mask: u32,
        ttl: Duration,
    ) -> Result<()> {
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
        let payload = serde_json::to_value(record)?;
        if self
            .save_with_dependency(job, &payload, expires_at_ms, None)
            .await?
            == IssuedJobSave::Saved
        {
            return Ok(());
        }
        let prepared = &job.context.prepared;
        // Followers retry compact persistence after the first repair completes.
        // The guard also lives through actual blocking work if its waiter dies.
        let repair = prepared.repair.clone().lock_owned().await;
        if self
            .save_with_dependency(job, &payload, expires_at_ms, None)
            .await?
            == IssuedJobSave::Saved
        {
            return Ok(());
        }
        let permit = self.build_slots.clone().acquire_owned().await?;
        let original = prepared.stored.clone();
        #[cfg(test)]
        let probe = prepared.repair_probe.lock().unwrap().clone();
        let (serialized, _repair) = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            #[cfg(test)]
            if let Some(probe) = probe {
                probe.block();
            }
            (serde_json::to_value(original), repair)
        })
        .await?;
        let serialized = serialized?;
        // Both admission and the transaction's current revision are checked
        // again after waiting. The original child deadline/payload stay fixed.
        ensure!(
            self.save_with_dependency(job, &payload, expires_at_ms, Some(&serialized))
                .await?
                == IssuedJobSave::Saved,
            "prepared dependency repair did not save issued work"
        );
        Ok(())
    }

    async fn save_with_dependency(
        &self,
        job: &MiningJob<JobContext>,
        payload: &Value,
        expires_at_ms: i64,
        repair: Option<&Value>,
    ) -> Result<IssuedJobSave> {
        let prepared = &job.context.prepared;
        let revision = self
            .issued_work_revision(prepared)
            .await?
            .context("payout snapshot stale")?;
        self.work_ledger
            .save_issued_job(
                &job.wire.job_id,
                payload,
                revision,
                &job.wire.previousblockhash,
                expires_at_ms,
                PreparedDependency {
                    key: &prepared.storage_key,
                    original_revision: prepared.stored.snapshot.payout_revision,
                    parent: prepared.stored.template["previousblockhash"]
                        .as_str()
                        .context("prepared parent missing")?,
                },
                repair,
            )
            .await
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
    fn block(&self) {
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

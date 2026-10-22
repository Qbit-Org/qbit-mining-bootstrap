//! A `MiningBackend` with no database or node. It builds real jobs from real
//! payout manifests, remembers every one it issued, and credits a share by
//! the coordinator's gate: ordinary (not block-only) work, on the current tip
//! or inside stale grace, meeting the share target, not a duplicate.
use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};
use qbit_prism_server::{
    codec::{Job, JobKind, Submission},
    ledger::SessionId,
    stratum::{MiningBackend, MiningJob, RetentionTip, StaleGrace, StratumError, Worker},
};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// A job as the backend built it, before the session touched it.
#[derive(Clone)]
pub struct Issued {
    pub job: Job,
    pub worker: Worker,
    /// The version mask the session persisted it with; `None` until persisted.
    pub mask: Option<u32>,
}

/// One share the backend credited.
#[derive(Clone, Debug)]
pub struct Credit {
    pub job_id: String,
    pub worker: String,
    pub submission: Submission,
}

pub struct State {
    pub extranonce2_size: usize,
    pub bits: u32,
    pub transactions: usize,
    pub generation: u64,
    pub payout_revision: i64,
    pub sessions: u32,
    pub builds: u64,
    pub issued: HashMap<String, Issued>,
    stored: HashMap<String, (MiningJob<()>, Worker, u32, Instant)>,
    pub credits: Vec<Credit>,
    seen: HashSet<String>,
    /// Retained vardiff evidence per (listener, username), as the ledger's
    /// worker-difficulty rows hold it.
    hints: HashMap<(String, String), (f64, Instant)>,
    /// Injected failures still to deliver, and how many ever were.
    pub fail_builds: u32,
    pub fail_submits: u32,
    pub injected: u32,
}

pub struct FuzzBackend {
    pub state: Mutex<State>,
}

pub fn tip(generation: u64) -> String {
    format!("{:064x}", generation.wrapping_add(0x5157_4954))
}

/// A pre-segwit transaction for the template, so work has a merkle branch.
fn template_transaction(tag: u8) -> Vec<u8> {
    let mut tx = vec![1, 0, 0, 0, 1];
    tx.extend([tag; 32]);
    tx.extend(0u32.to_le_bytes());
    tx.extend([1, 0x51]);
    tx.extend(u32::MAX.to_le_bytes());
    tx.push(1);
    tx.extend(1_000u64.to_le_bytes());
    tx.extend([1, 0x51]);
    tx.extend(0u32.to_le_bytes());
    tx
}

impl FuzzBackend {
    pub fn new(extranonce2_size: usize, bits: u32, transactions: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                extranonce2_size,
                bits,
                transactions,
                generation: 0,
                payout_revision: 0,
                sessions: 0,
                builds: 0,
                issued: HashMap::new(),
                stored: HashMap::new(),
                credits: Vec::new(),
                seen: HashSet::new(),
                hints: HashMap::new(),
                fail_builds: 0,
                fail_submits: 0,
                injected: 0,
            }),
        })
    }

    pub fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The single username rule: miners are `miner` plus a short suffix.
    fn valid_username(username: &str) -> bool {
        username.starts_with("miner")
            && username.len() <= 64
            && username
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    }
}

impl MiningBackend for FuzzBackend {
    type Context = ();

    async fn observed_tip_hint(&self) -> Option<RetentionTip> {
        let generation = self.state().generation;
        Some(RetentionTip {
            hash: tip(generation),
            parent: generation.checked_sub(1).map(tip),
            transitioned: generation > 0,
        })
    }

    async fn worker_difficulty(
        &self,
        listener: &str,
        worker: &Worker,
        ttl_seconds: u64,
    ) -> anyhow::Result<Option<(f64, Duration)>> {
        Ok(self
            .state()
            .hints
            .get(&(listener.to_owned(), worker.username.clone()))
            .filter(|(_, at)| at.elapsed() <= Duration::from_secs(ttl_seconds))
            .map(|(difficulty, at)| (*difficulty, at.elapsed())))
    }

    async fn remember_worker_difficulty(
        &self,
        listener: &str,
        worker: &Worker,
        difficulty: f64,
        share_id: Option<&str>,
        downward_only: bool,
    ) -> anyhow::Result<()> {
        if !(difficulty.is_finite() && difficulty > 0.0) {
            crate::violation(format!("vardiff evidence of difficulty {difficulty}"));
        }
        let mut state = self.state();
        let key = (listener.to_owned(), worker.username.clone());
        if downward_only {
            if let Some((old, _)) = state.hints.get_mut(&key) {
                *old = old.min(difficulty);
            }
        } else if share_id.is_some() {
            state.hints.insert(key, (difficulty, Instant::now()));
        }
        Ok(())
    }

    async fn new_session_id(&self) -> Result<SessionId, StratumError> {
        let mut state = self.state();
        state.sessions += 1;
        Ok(state.sessions.into())
    }

    async fn authorize(&self, username: &str) -> Result<Worker, StratumError> {
        if !Self::valid_username(username) {
            return Err(StratumError::new(
                20,
                "invalid payout",
                "unauthorized-worker",
            ));
        }
        Ok(Worker {
            username: username.into(),
            payout_address: username.split('.').next().unwrap_or(username).into(),
            worker_name: username.split_once('.').map(|(_, w)| w.into()),
            p2mr_program_hex: "ab".repeat(32),
        })
    }

    async fn build_job(
        &self,
        worker: &Worker,
        extranonce1: &str,
        difficulty: f64,
        minimum_difficulty: f64,
    ) -> Result<MiningJob<()>, StratumError> {
        // The session hands the backend only a difficulty it could have
        // advertised: a NaN, infinite or non-positive one is a session bug
        // even though the real builder would refuse it.
        if !(difficulty.is_finite() && difficulty > 0.0) {
            crate::violation(format!("build_job was asked for difficulty {difficulty}"));
        }
        if !(minimum_difficulty.is_finite() && minimum_difficulty >= 0.0) {
            crate::violation(format!(
                "build_job was asked for difficulty floor {minimum_difficulty}"
            ));
        }
        let mut state = self.state();
        if state.fail_builds > 0 {
            state.fail_builds -= 1;
            return Err(StratumError::backend("injected builder failure"));
        }
        let transactions: Vec<Vec<u8>> = (0..state.transactions)
            .map(|i| template_transaction(i as u8))
            .collect();
        let template = json!({
            "version": 0x20000000u32,
            "bits": format!("{:08x}", state.bits),
            "curtime": 1_700_000_000u32,
            "mintime": 1_699_999_000u32,
            "previousblockhash": tip(state.generation),
            "transactions": transactions.iter().map(|tx| json!({"data": hex::encode(tx)})).collect::<Vec<_>>(),
        });
        let manifest = build_manifest(CoinbaseBuildRequest {
            block_height: 1 + state.generation,
            coinbase_value_sats: 5_000_000_000,
            entitlements: vec![WeightedEntitlement {
                recipient_id: worker.payout_address.clone(),
                order_key: worker.payout_address.clone(),
                p2mr_program_hex: worker.p2mr_program_hex.clone(),
                weight: 1,
            }],
            witness_nonce_hex: Some("00".repeat(32)),
            witness_merkle_leaves_hex: qbit_prism_server::codec::witness_merkle_leaves_hex(
                &transactions,
            ),
            coinbase_script_sig_suffix_hex: Some(format!(
                "{extranonce1}{}",
                "00".repeat(state.extranonce2_size)
            )),
            pinned_first_output: None,
        })
        .map_err(|e| StratumError::internal(format!("manifest: {e}")))?;
        let id = format!("job-{:x}", state.builds);
        state.builds += 1;
        let mut job = Job::from_manifest(
            id.clone(),
            &template,
            &manifest,
            extranonce1,
            state.extranonce2_size,
            difficulty,
            minimum_difficulty,
            true,
        )
        .unwrap_or_else(|e| crate::violation(format!("a valid template failed to build: {e:#}")));
        job.refresh_generation = state.generation;
        job.payout_revision = state.payout_revision;
        state.issued.insert(
            id,
            Issued {
                job: job.clone(),
                worker: worker.clone(),
                mask: None,
            },
        );
        Ok(MiningJob {
            wire: job,
            context: Arc::new(()),
        })
    }

    async fn persist_issued_job(
        &self,
        worker: &Worker,
        job: &MiningJob<()>,
        version_mask: u32,
        ttl: Duration,
    ) -> Result<(), StratumError> {
        let mut state = self.state();
        match state.issued.get_mut(&job.wire.job_id) {
            Some(issued) => issued.mask = Some(version_mask),
            None => crate::violation(format!(
                "the session persisted job {} the backend never built",
                job.wire.job_id
            )),
        }
        state.stored.insert(
            job.wire.job_id.clone(),
            (
                job.clone(),
                worker.clone(),
                version_mask,
                Instant::now() + ttl,
            ),
        );
        Ok(())
    }

    async fn resume_job(
        &self,
        worker: &Worker,
        job_id: &str,
    ) -> Result<Option<MiningJob<()>>, StratumError> {
        let state = self.state();
        let Some((job, original, mask, expires)) = state.stored.get(job_id) else {
            return Ok(None);
        };
        if worker.username != original.username
            || Instant::now() >= *expires
            || job.wire.previousblockhash != tip(state.generation)
            || job.wire.payout_revision != state.payout_revision
        {
            return Ok(None);
        }
        let mut job = job.clone();
        job.wire.version_mask = *mask;
        job.wire.resume_expires_at = Some(*expires);
        Ok(Some(job))
    }

    async fn submit(
        &self,
        worker: &Worker,
        job: &MiningJob<()>,
        submission: Submission,
        grace: StaleGrace,
    ) -> Result<(), StratumError> {
        let mut state = self.state();
        let Some(issued) = state.issued.get(&job.wire.job_id) else {
            crate::violation(format!(
                "a share reached the backend for job {} it never built",
                job.wire.job_id
            ));
        };
        // The session may mark work block-only and restore a resumed job's
        // mask and expiry; it must never change what the work commits to.
        let built = &issued.job;
        if built.coinb1 != job.wire.coinb1
            || built.coinb2 != job.wire.coinb2
            || built.extranonce1 != job.wire.extranonce1
            || built.merkle_branch != job.wire.merkle_branch
            || built.share_target != job.wire.share_target
            || built.previousblockhash != job.wire.previousblockhash
        {
            crate::violation(format!(
                "job {} changed after it was built",
                job.wire.job_id
            ));
        }
        if issued.worker.username != worker.username {
            crate::violation(format!(
                "job {} built for {} was submitted as {}",
                job.wire.job_id, issued.worker.username, worker.username
            ));
        }
        if state.fail_submits > 0 {
            state.fail_submits -= 1;
            return Err(StratumError::backend("injected ledger outage"));
        }
        if job.wire.kind == JobKind::BlockOnly {
            return Err(StratumError::new(21, "stale job", "stale-job"));
        }
        let current = tip(state.generation);
        if job.wire.previousblockhash != current && !grace.eligible_for(&current) {
            return Err(StratumError::new(21, "stale job", "stale-job"));
        }
        if !submission.share_pass {
            return Err(StratumError::new(
                23,
                "low difficulty share",
                "low-difficulty",
            ));
        }
        if !state.seen.insert(submission.block_hash_hex.clone()) {
            return Err(StratumError::new(22, "duplicate share", "duplicate-share"));
        }
        state.credits.push(Credit {
            job_id: job.wire.job_id.clone(),
            worker: worker.username.clone(),
            submission,
        });
        Ok(())
    }
}

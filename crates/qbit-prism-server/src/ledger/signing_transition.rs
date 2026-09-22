//! Offline signing-key rotation: the guarded cluster fingerprint reset.
//! There is deliberately no force/live mode, and this is the only statement
//! that writes `config_fingerprint = NULL`.
use super::policy_transition::resolved_policy;
use super::*;
use crate::config::Config;
use serde_json::json;
use std::time::Duration;

/// How many unfinished candidates a refusal names; the counts are exact.
const UNFINISHED_NAMED: i64 = 10;

/// What the operator does after a reset, printed with its result.
const NEXT_STEPS: [&str; 5] = [
    "The cluster fingerprint is now unset. Keep every old-key frontend, supervisor restart and one-shot tool stopped: the first configure of ANY key environment pins the fingerprint, including the old one.",
    "Start the new-key frontends. The first one to start pins the new fingerprint; every other frontend must then use exactly that configuration.",
    "Keep the old public keys (previous_policy.ledger_key and previous_policy.manifest_key in qbit_prism_signing_transitions): bundles signed before this rotation verify only with them.",
    "Do not run backfill-ctv or import-audits with the new key against audits signed before this rotation: both abort on the first old-key row.",
    "If the cluster halts before a new fingerprint is pinned, follow the halted-while-unset recovery in docs/prism-ledger-ops.md (Signing-key rotation).",
];

impl Ledger {
    /// Reset the pinned cluster fingerprint so frontends with new signing
    /// keys can pin theirs. `current` is the configuration the cluster runs
    /// today, old keys included: its policy document is what the journal
    /// keeps. `health_refresh_interval` is the frontends' heartbeat cadence;
    /// the freshness window is derived from it exactly as `self-check` does.
    pub async fn transition_signing(
        &self,
        current: &Config,
        health_refresh_interval: Duration,
    ) -> Result<Value> {
        // 120 seconds is the ceiling for the whole command: the node calls,
        // the transaction and its commit. It does not bound a lock wait. The
        // operator connection's `lock_timeout`
        // (`PRISM_DATABASE_LOCK_TIMEOUT_MS`, five seconds by default) ends a
        // wait for the advisory locks, the instance table or the cluster
        // row, and its statement timeout
        // (`PRISM_DATABASE_STATEMENT_TIMEOUT_MS`, fifteen seconds by default)
        // ends any one statement; both abort before COMMIT and roll back.
        // Cancellation before COMMIT rolls back too; once COMMIT has been
        // sent, a lost response is resolved from the journal, and a retry
        // then refuses with that row instead of writing another.
        tokio::time::timeout(
            Duration::from_secs(120),
            self.transition_signing_in(current, health_refresh_interval),
        )
        .await
        .context("signing transition exceeded 120 seconds; inspect qbit_prism_signing_transitions before retrying")?
        .map_err(|error| {
            if lock_timed_out(&error) {
                error.context("a lock wait exceeded PRISM_DATABASE_LOCK_TIMEOUT_MS and nothing was changed: something still holds the cluster row, the instance table or the settlement and order locks. Confirm every frontend and tool is stopped, then rerun; do not raise the timeout blindly")
            } else {
                error
            }
        })
    }

    async fn transition_signing_in(
        &self,
        current: &Config,
        health_refresh_interval: Duration,
    ) -> Result<Value> {
        let stale_after = crate::api::health_stale_after(health_refresh_interval).as_secs_f64();
        let (old, genesis) = resolved_policy(current, false).await?;
        let previous_policy = old.policy_document(&genesis)?;
        let previous = old.fingerprint(&genesis)?;

        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        // Exclude registrations and heartbeats racing the instance scan: a
        // frontend starting now writes its row only after this commits, and
        // its configure then waits on the cluster row below.
        sqlx::query(
            "LOCK TABLE qbit_prism_instances, qbit_ledger_writer_lease IN SHARE ROW EXCLUSIVE MODE",
        )
        .execute(&mut *tx)
        .await?;
        // A halt is cleared with the pinned fingerprint (`fatal-state clear`
        // compares it), so it must be cleared before that fingerprint goes.
        writable(&mut tx).await.context(
            "signing-transition requires a writable cluster; clear a fatal state with the current keys first (docs/prism-ledger-ops.md, Signing-key rotation)",
        )?;
        // The row lock `Ledger::configure` takes and every writer fence reads
        // `FOR SHARE`. Everything below is decided and written under it.
        let (saved, revision): (Option<String>, i64) = sqlx::query_as(
            "SELECT config_fingerprint,payout_revision FROM qbit_prism_cluster WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let Some(saved) = saved else {
            let last: Option<Value> = sqlx::query_scalar(
                "SELECT to_jsonb(t) FROM qbit_prism_signing_transitions t ORDER BY transition_id DESC LIMIT 1",
            )
            .fetch_optional(&mut *tx)
            .await?;
            match last {
                Some(last) => bail!(
                    "nothing to reset: the cluster fingerprint is already unset; last signing transition: {last}"
                ),
                None => bail!(
                    "nothing to reset: the cluster fingerprint is already unset and qbit_prism_signing_transitions is empty (a database no frontend has configured)"
                ),
            }
        };
        ensure!(
            saved == previous,
            "the cluster fingerprint at payout revision {revision} is not this configuration's; run signing-transition with the OLD key environment the cluster is pinned to, so the journal records the keys being retired"
        );

        let mut refusals = Vec::new();
        let instances: Vec<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(i) || jsonb_build_object('heartbeat_age_seconds', extract(epoch FROM (clock_timestamp() - i.heartbeat_at))) FROM qbit_prism_instances i ORDER BY instance_id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let live: Vec<String> = instances
            .iter()
            .filter_map(|instance| liveness(instance, stale_after))
            .collect();
        if !live.is_empty() {
            refusals.push(format!(
                "every frontend must be stopped, or its heartbeat older than {stale_after} seconds by the database clock; offending instances: {}",
                live.join(", ")
            ));
        }
        // Stricter than `configure`, which only refuses other keys: any
        // unfinished candidate is a claim, an offer reservation or an offer a
        // frontend may still be settling.
        let unfinished: Vec<(String, i64)> = sqlx::query_as(&format!(
            "SELECT state,count(*) FROM qbit_block_candidate_outbox WHERE state IN {} GROUP BY state ORDER BY state",
            CandidateState::UNFINISHED_SQL
        ))
        .fetch_all(&mut *tx)
        .await?;
        if !unfinished.is_empty() {
            let named: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT block_hash FROM qbit_block_candidate_outbox WHERE state IN {} ORDER BY block_hash LIMIT $1",
                CandidateState::UNFINISHED_SQL
            ))
            .bind(UNFINISHED_NAMED)
            .fetch_all(&mut *tx)
            .await?;
            let total: i64 = unfinished.iter().map(|(_, count)| count).sum();
            let states: Vec<String> = unfinished
                .iter()
                .map(|(state, count)| format!("{count} {state}"))
                .collect();
            refusals.push(format!(
                "the candidate outbox must be drained; {total} unfinished block candidates ({}), first {}: {}",
                states.join(", "),
                named.len(),
                named.join(", ")
            ));
        }
        ensure!(
            refusals.is_empty(),
            "signing-transition refused, nothing was changed: {}",
            refusals.join("; ")
        );

        let reset = sqlx::query(
            "UPDATE qbit_prism_cluster SET config_fingerprint=NULL,updated_at=clock_timestamp() WHERE singleton AND config_fingerprint=$1",
        )
        .bind(&previous)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        ensure!(
            reset == 1,
            "the cluster fingerprint changed under its row lock; nothing was changed"
        );
        let event: Value = sqlx::query_scalar(
            "INSERT INTO qbit_prism_signing_transitions(previous_fingerprint,previous_policy,instances,payout_revision,heartbeat_stale_after_seconds) VALUES($1,$2,$3,$4,$5) RETURNING to_jsonb(qbit_prism_signing_transitions)",
        )
        .bind(&previous)
        .bind(previous_policy)
        .bind(json!(instances))
        .bind(revision)
        .bind(stale_after)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await.context("signing transition commit failed; outcome may be unknown; inspect qbit_prism_signing_transitions before retrying")?;
        Ok(json!({
            "signing_transition": event,
            "config_fingerprint": Value::Null,
            "next_steps": NEXT_STEPS,
        }))
    }
}

/// PostgreSQL's `lock_not_available`: "canceling statement due to lock
/// timeout", anywhere in the chain.
fn lock_timed_out(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::Database(database)) if database.code().as_deref() == Some("55P03")
        )
    })
}

/// `Some(description)` when the instance may still be running. Only an
/// explicit `stopped` marker or a heartbeat measurably older than the window
/// proves otherwise: SIGKILL never writes `stopped`, so a stale row of any
/// other state is a dead frontend, while an unreadable or future-dated age is
/// unknown and counts as live.
fn liveness(instance: &Value, stale_after: f64) -> Option<String> {
    let status = serde_json::from_value::<HeartbeatStatus>(instance["status"].clone());
    if matches!(status, Ok(HeartbeatStatus::Stopped)) {
        return None;
    }
    let id = instance["instance_id"].as_str().unwrap_or("<unknown>");
    let state = match status {
        Ok(HeartbeatStatus::Starting) => "starting",
        Ok(_) => "running",
        Err(_) => "unrecognized status",
    };
    match instance["heartbeat_age_seconds"].as_f64() {
        Some(age) if age > stale_after => None,
        Some(age) if age >= 0.0 => Some(format!("{id} ({state}, heartbeat {age:.3} seconds old)")),
        _ => Some(format!("{id} ({state}, heartbeat age unknown)")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(status: Value, age: Value) -> Value {
        json!({"instance_id":"frontend-a","status":status,"heartbeat_age_seconds":age})
    }

    fn health() -> Value {
        json!({"schema":"qbit.prism.audit-health.v1","ready":true})
    }

    #[test]
    fn only_a_stopped_marker_or_a_measurably_stale_heartbeat_is_quiescent() {
        // Stopped is proof whatever its age, even an unreadable one.
        for age in [json!(0.0), json!(-1.0), Value::Null] {
            assert_eq!(
                liveness(&instance(json!({"state":"stopped"}), age), 15.0),
                None
            );
        }
        for status in [
            health(),
            json!({"state":"starting"}),
            json!({}),
            Value::Null,
        ] {
            // The boundary is inclusive, as it is for self-check.
            for age in [json!(0.0), json!(15.0)] {
                let live = liveness(&instance(status.clone(), age), 15.0).unwrap();
                assert!(
                    live.contains("frontend-a") && live.contains("seconds old"),
                    "{live}"
                );
            }
            assert_eq!(
                liveness(&instance(status.clone(), json!(15.001)), 15.0),
                None
            );
            // Future-dated, missing and non-numeric ages are unknown, not stale.
            for age in [json!(-0.001), Value::Null, json!("16")] {
                let live = liveness(&instance(status.clone(), age), 15.0).unwrap();
                assert!(live.contains("heartbeat age unknown"), "{live}");
            }
        }
        // A legacy health payload carrying a stopped marker is still health.
        let mut mixed = health();
        mixed["state"] = json!("stopped");
        assert!(liveness(&instance(mixed, json!(1.0)), 15.0)
            .unwrap()
            .contains("running"));
    }
}

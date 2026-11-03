//! `/audit/carry-forward-integrity` and `/audit/ledger-integrity` (#737).
//!
//! `qbit_carry_forward_integrity_report()` took about a minute at production
//! size before migration 026, and is estimated at half that since: in either
//! case longer than the 15 s every ledger session runs with, and as long a
//! snapshot held on the primary (#738). So a frontend runs it on demand, one
//! run at a time, in a read-only transaction under a statement timeout of its
//! own, as `self-check` does. Every request that arrives during a run waits
//! for that run and is answered with its outcome. A report is then served for
//! [`REUSE`] after its run ends, with when it was computed, and a failure for
//! as long as its run took, at most [`REUSE`]; only the first request after
//! that starts another run.
//!
//! On demand rather than on a timer: a frontend nobody asks runs no report,
//! and one that is polled, however often, runs at most one per run time plus
//! [`REUSE`].

use super::{ApiError, ApiResult};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

/// One run's bound, from its first connection checkout to its last
/// statement: self-check's allowance, about ten times the report's estimated
/// production time since migration 026.
pub(super) const RUN_TIMEOUT: Duration = Duration::from_secs(300);

/// How long past its deadline a run waits for the server to cancel its last
/// statement, which ends by the deadline in the server, before giving up on
/// it: only a server or a network that does not answer takes this long.
const CANCEL_GRACE: Duration = Duration::from_secs(1);

/// How long a report is served after its run ends. No request in that time
/// starts a run, so a poller keeps the report's snapshot open less than a
/// tenth of the time at the estimated 29 s, and about a sixth at a minute
/// (#738).
pub(super) const REUSE: Duration = Duration::from_secs(300);

/// The integrity routes' runs: the one in flight, if any, and the newest
/// outcome.
pub(super) struct IntegrityReports {
    timeout: Duration,
    reuse: Duration,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// How many runs have started; each run's number is its count.
    started: u64,
    /// The newest run's outcome, by run number.
    latest: Option<Arc<Run>>,
    running: Option<Running>,
}

/// What a request does: serve an outcome still reused, or wait for a run's.
enum Next {
    Reuse(Arc<Run>),
    Wait(watch::Receiver<Option<Arc<Run>>>),
}

/// The run in flight, and where the requests waiting for it hear its outcome.
struct Running {
    number: u64,
    outcome: watch::Receiver<Option<Arc<Run>>>,
}

/// One finished run.
struct Run {
    number: u64,
    /// When the run started, by the wall clock. Its figures were read from
    /// then until it ended.
    computed_at: DateTime<Utc>,
    started: Instant,
    ended: Instant,
    /// The report, or why the run produced none.
    outcome: Result<Value, String>,
}

impl Run {
    /// How long after the run ends its outcome is served: `reuse` for a
    /// report. A failure is served no longer than its run took, so a brief one,
    /// a moment without a free connection say, is retried soon, while one that
    /// keeps running into the timeout still leaves the database about half
    /// the time.
    fn reused_for(&self, reuse: Duration) -> Duration {
        match self.outcome {
            Ok(_) => reuse,
            Err(_) => reuse.min(self.ended - self.started),
        }
    }

    /// The route's answer: the report with when it was computed, its age now
    /// and how long the run took, or a 503 saying why there is none.
    fn response(&self, reuse: Duration) -> ApiResult<Value> {
        let now = Instant::now();
        let computed_at = self
            .computed_at
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        let age = now.saturating_duration_since(self.started);
        match &self.outcome {
            Ok(report) => {
                let mut report = report.clone();
                report["computed_at"] = json!(computed_at);
                report["age_seconds"] = json!(age.as_secs_f64());
                report["run_seconds"] = json!((self.ended - self.started).as_secs_f64());
                Ok(report)
            }
            Err(reason) => {
                let next = self
                    .reused_for(reuse)
                    .saturating_sub(now.saturating_duration_since(self.ended));
                Err(ApiError::integrity_unavailable(format!(
                    "{reason}; that run started at {computed_at}, {:.0} s ago, and no report is current; the first request in {:.0} s starts another run",
                    age.as_secs_f64(),
                    next.as_secs_f64()
                )))
            }
        }
    }
}

impl IntegrityReports {
    pub(super) fn new(timeout: Duration, reuse: Duration) -> Self {
        Self {
            timeout,
            reuse,
            state: Mutex::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The route's answer from the run in flight, the newest outcome while it
    /// is reused, or else a new run.
    pub(super) async fn answer_request(self: &Arc<Self>, pool: &PgPool) -> ApiResult<Value> {
        let pool = pool.clone();
        self.answer_request_with(move |deadline| async move { run_report(&pool, deadline).await })
            .await
    }

    /// [`IntegrityReports::answer_request`], with `compute` producing a new run's
    /// report by its deadline. It is called only when a run starts.
    async fn answer_request_with<R, F>(self: &Arc<Self>, compute: R) -> ApiResult<Value>
    where
        R: FnOnce(Instant) -> F,
        F: Future<Output = anyhow::Result<Value>> + Send + 'static,
    {
        let next = {
            let mut state = self.lock();
            if let Some(running) = &state.running {
                Next::Wait(running.outcome.clone())
            } else if let Some(run) = state
                .latest
                .clone()
                .filter(|run| run.ended.elapsed() < run.reused_for(self.reuse))
            {
                Next::Reuse(run)
            } else {
                Next::Wait(self.start(&mut state, compute))
            }
        };
        let mut outcome = match next {
            Next::Reuse(run) => return run.response(self.reuse),
            Next::Wait(outcome) => outcome,
        };
        // A run whose task died without an outcome leaves the channel
        // closed and empty.
        let run = match outcome.wait_for(Option::is_some).await {
            Ok(run) => (*run).clone(),
            Err(_) => None,
        };
        match run {
            Some(run) => run.response(self.reuse),
            None => Err(ApiError::integrity_unavailable(
                "the carry-forward integrity report run ended without an outcome; see the frontend's log",
            )),
        }
    }

    /// Start the next run on a task of its own: a request that stops waiting
    /// leaves it running, bounded by its deadline, for the next request.
    fn start<R, F>(
        self: &Arc<Self>,
        state: &mut State,
        compute: R,
    ) -> watch::Receiver<Option<Arc<Run>>>
    where
        R: FnOnce(Instant) -> F,
        F: Future<Output = anyhow::Result<Value>> + Send + 'static,
    {
        let computed_at = Utc::now();
        let started = Instant::now();
        let deadline = started + self.timeout;
        let timeout = self.timeout;
        // Before the run is recorded: should `compute` itself panic, nothing
        // is left in flight.
        let running = compute(deadline);
        state.started += 1;
        let number = state.started;
        let (sender, outcome) = watch::channel(None);
        state.running = Some(Running {
            number,
            outcome: outcome.clone(),
        });
        let finishing = Finishing {
            reports: Arc::clone(self),
            number,
        };
        tokio::spawn(async move {
            let outcome = match tokio::time::timeout_at(deadline + CANCEL_GRACE, running).await {
                Ok(Ok(report)) => Ok(report),
                Ok(Err(error)) => Err(failure(&error, started.elapsed(), timeout)),
                Err(_) => Err(format!(
                    "the carry-forward integrity report did not finish within its {} s",
                    timeout.as_secs()
                )),
            };
            let run = Arc::new(Run {
                number,
                computed_at,
                started,
                ended: Instant::now(),
                outcome,
            });
            let run_seconds = (run.ended - started).as_secs_f64();
            match &run.outcome {
                Ok(_) => tracing::info!(run_seconds, "carry-forward integrity report computed"),
                Err(reason) => {
                    tracing::warn!(run_seconds, %reason, "carry-forward integrity report failed")
                }
            }
            finishing.reports.publish(Arc::clone(&run));
            sender.send_replace(Some(run));
            drop(finishing);
        });
        outcome
    }

    /// Record a finished run, unless a newer run's outcome is already
    /// recorded, and end it as the run in flight.
    fn publish(&self, run: Arc<Run>) {
        let mut state = self.lock();
        if state
            .latest
            .as_ref()
            .is_none_or(|latest| latest.number < run.number)
        {
            state.latest = Some(Arc::clone(&run));
        }
        state.end(run.number);
    }
}

impl State {
    fn end(&mut self, number: u64) {
        if self
            .running
            .as_ref()
            .is_some_and(|running| running.number == number)
        {
            self.running = None;
        }
    }
}

/// Ends run `number` as the run in flight however its task ends, a panic or
/// an abort included, so a run that dies keeps no later request waiting on it
/// and none from starting another.
struct Finishing {
    reports: Arc<IntegrityReports>,
    number: u64,
}

impl Drop for Finishing {
    fn drop(&mut self) {
        self.reports.lock().end(self.number);
    }
}

/// Why a run failed `after` its start, for its answer. Only a cancellation's
/// own message is passed on; any other error is logged instead.
fn failure(error: &anyhow::Error, after: Duration, timeout: Duration) -> String {
    let database = error.downcast_ref::<sqlx::Error>();
    let (what, note) = match database.and_then(sqlx::Error::as_database_error) {
        // A statement timeout's, or an operator's cancel request.
        Some(cancelled) if cancelled.code().as_deref() == Some("57014") => {
            (format!("was cancelled ({})", cancelled.message()), "")
        }
        _ if matches!(database, Some(sqlx::Error::PoolTimedOut)) => {
            ("found no free ledger connection".to_owned(), "")
        }
        _ => {
            tracing::warn!(error = %format!("{error:#}"), "carry-forward integrity report error");
            ("failed".to_owned(), " (see the frontend's log)")
        }
    };
    format!(
        "the carry-forward integrity report {what} after {:.1} s of its {} s{note}",
        after.as_secs_f64(),
        timeout.as_secs()
    )
}

/// The report and its `payout_divergence` line, as the routes have always
/// served them. The line is read first, so the report is the run's last
/// statement, and its own statement timeout, the time left to `deadline`,
/// ends it in the server by then too.
async fn run_report(pool: &PgPool, deadline: Instant) -> anyhow::Result<Value> {
    // #478: debt from divergent landings is exact accounting, so the
    // report's mismatch count never shows it; this line does.
    let divergence = crate::ledger::payout_divergence_line(pool).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await?;
    let left = deadline.saturating_duration_since(Instant::now());
    let mut report = crate::ledger::integrity_report_bounded(&mut tx, left).await?;
    tx.rollback().await?;
    anyhow::ensure!(
        report.is_object(),
        "the carry-forward integrity report is not a JSON object"
    );
    report["payout_divergence"] = divergence;
    report["ledger_backend"] = json!("postgres-native");
    Ok(report)
}

#[cfg(test)]
mod tests;

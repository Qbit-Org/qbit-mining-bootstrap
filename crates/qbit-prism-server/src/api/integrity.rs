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
//! On demand rather than on a timer: a frontend nobody asks runs no report.
//! One that is polled, however often, starts a run at most once per run time
//! plus [`REUSE`] after a report, and after a failure at most once per twice
//! the failed run's time: a run that keeps failing holds its connection at
//! most about half the time. A stopping frontend abandons its run in flight
//! and starts none, so a stop never waits on a report.

use super::{ApiError, ApiResult};
use anyhow::Context;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};
use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

/// One run's bound, from its first connection checkout to its last
/// statement: self-check's allowance, about ten times the report's estimated
/// production time since migration 026.
pub(super) const RUN_TIMEOUT: Duration = Duration::from_secs(300);

/// How long past its deadline a run waits before giving up on it: for the
/// server to cancel its last statement, which ends by the deadline in the
/// server, and for the report's last round trip, putting back the session's
/// statement timeout. Only a server or a network that does not answer takes
/// this long.
const CANCEL_GRACE: Duration = Duration::from_secs(5);

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
    /// The frontend's stop signal: `true` once it is stopping.
    stop: watch::Receiver<bool>,
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

/// What a request does: serve an outcome still reused, wait for the run in
/// flight, start one (`T`, its task) and wait for it, or refuse to start one
/// on a stopping frontend.
enum Next<T> {
    Reuse(Arc<Run>),
    Wait(watch::Receiver<Option<Arc<Run>>>),
    Start(watch::Receiver<Option<Arc<Run>>>, T),
    Stopping,
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
    /// The run's start and end. Their difference, the response's
    /// `run_seconds`, bounds the report's own time from above: it includes
    /// the connection checkout and the divergence line.
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
    /// and how long the run took, or a 503 saying why there is none and when
    /// another run may start, which a `stopping` frontend never does.
    fn response(&self, reuse: Duration, stopping: bool) -> ApiResult<Value> {
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
                let next = if stopping {
                    "the frontend is stopping and starts no other run".to_owned()
                } else {
                    let next = self
                        .reused_for(reuse)
                        .saturating_sub(now.saturating_duration_since(self.ended));
                    format!(
                        "the first request in {:.0} s starts another run",
                        next.as_secs_f64()
                    )
                };
                Err(ApiError::integrity_unavailable(format!(
                    "{reason}; that run started at {computed_at}, {:.0} s ago, and no report is current; {next}",
                    age.as_secs_f64()
                )))
            }
        }
    }
}

impl IntegrityReports {
    /// Runs bounded by `timeout`, a report reused for `reuse`, and no stop
    /// signal.
    pub(super) fn new(timeout: Duration, reuse: Duration) -> Self {
        Self {
            timeout,
            reuse,
            // A signal whose sender is gone never stops (see `stopping`).
            stop: watch::channel(false).1,
            state: Mutex::default(),
        }
    }

    /// No run yet, with another run bound and reuse window.
    pub(super) fn with_timing(&self, timeout: Duration, reuse: Duration) -> Self {
        Self {
            stop: self.stop.clone(),
            ..Self::new(timeout, reuse)
        }
    }

    /// No run yet, stopping when `stop` turns `true`.
    pub(super) fn stopping_on(&self, stop: watch::Receiver<bool>) -> Self {
        Self {
            stop,
            ..Self::new(self.timeout, self.reuse)
        }
    }

    fn stopped(&self) -> bool {
        *self.stop.borrow()
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
            } else if self.stopped() {
                Next::Stopping
            } else {
                let (outcome, task) = self.start(&mut state, compute);
                Next::Start(outcome, task)
            }
        };
        let mut outcome =
            match next {
                Next::Reuse(run) => return run.response(self.reuse, self.stopped()),
                Next::Stopping => return Err(ApiError::integrity_unavailable(
                    "the frontend is stopping, so it starts no carry-forward integrity report run",
                )),
                Next::Wait(outcome) => outcome,
                // Spawned once the state's lock is released: a runtime that is
                // shutting down drops the task at once, and its `Finishing` takes
                // that lock.
                Next::Start(outcome, task) => {
                    tokio::spawn(task);
                    outcome
                }
            };
        // A run whose task died without an outcome leaves the channel
        // closed and empty.
        let run = match outcome.wait_for(Option::is_some).await {
            Ok(run) => (*run).clone(),
            Err(_) => None,
        };
        match run {
            Some(run) => run.response(self.reuse, self.stopped()),
            None => Err(ApiError::integrity_unavailable(
                "the carry-forward integrity report run ended without an outcome; see the frontend's log",
            )),
        }
    }

    /// Record the next run and return its task, for the caller to spawn: a
    /// request that stops waiting leaves the run going, bounded by its
    /// deadline and the frontend's stop, for the next request.
    fn start<R, F>(
        self: &Arc<Self>,
        state: &mut State,
        compute: R,
    ) -> (
        watch::Receiver<Option<Arc<Run>>>,
        impl Future<Output = ()> + Send + 'static,
    )
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
        let stop = self.stop.clone();
        let task = async move {
            // Either way the run's future is dropped, and with it its
            // connection, which closes at once (see `run_report`).
            let outcome = tokio::select! {
                // A run that has finished keeps its outcome, stop or not.
                biased;
                outcome = tokio::time::timeout_at(deadline + CANCEL_GRACE, running) => match outcome {
                    Ok(Ok(report)) => Ok(report),
                    Ok(Err(error)) => Err(failure(&error, started.elapsed(), timeout)),
                    Err(_) => Err(format!(
                        "the carry-forward integrity report run did not finish within its {} s",
                        timeout.as_secs()
                    )),
                },
                () = stopping(stop) => {
                    Err("the frontend stopped during the carry-forward integrity report run".to_owned())
                }
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
        };
        (outcome, task)
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

/// Resolves once `stop` turns `true`; never, once its sender is gone without
/// having stopped.
async fn stopping(mut stop: watch::Receiver<bool>) {
    if stop.wait_for(|stopped| *stopped).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// The part of a run that failed, named in its answer.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// The connection checkout and the read-only transaction's setup.
    Connection,
    Divergence,
    Report,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Connection => "its connection",
            Self::Divergence => "the payout divergence line",
            Self::Report => "the report",
        })
    }
}

/// Why a run failed `after` its start, for its answer: the step that failed
/// and how. Only a cancellation's own message is passed on; any other error
/// is logged instead.
fn failure(error: &anyhow::Error, after: Duration, timeout: Duration) -> String {
    let database = error.downcast_ref::<sqlx::Error>();
    let what = match database.and_then(sqlx::Error::as_database_error) {
        // A statement timeout's, or an operator's cancel request.
        Some(cancelled) if cancelled.code().as_deref() == Some("57014") => {
            format!("cancelled ({})", cancelled.message())
        }
        _ if matches!(database, Some(sqlx::Error::PoolTimedOut)) => {
            "no free ledger connection".to_owned()
        }
        _ => {
            tracing::warn!(error = %format!("{error:#}"), "carry-forward integrity report error");
            "an error (see the frontend's log)".to_owned()
        }
    };
    let at = error
        .downcast_ref::<Step>()
        .map_or_else(String::new, |step| format!(" at {step}"));
    format!(
        "the carry-forward integrity report run failed{at}: {what} after {:.1} s of its {} s",
        after.as_secs_f64(),
        timeout.as_secs()
    )
}

/// The report and its `payout_divergence` line, as the routes have always
/// served them.
///
/// The run's connection is closed when the run drops it, at its end or
/// mid-statement, instead of going back to the pool. Dropped mid-statement,
/// by a stopping frontend or the deadline's backstop, a connection returned
/// to the pool would wait there for the statement, which the server runs on
/// to its timeout. Closed, it frees its place in the pool at once, and
/// PostgreSQL, told to check the client every second, ends the statement
/// within about a second: a stop never waits on a report and leaves none
/// running.
async fn run_report(pool: &PgPool, deadline: Instant) -> anyhow::Result<Value> {
    let mut connection = pool.acquire().await.context(Step::Connection)?;
    connection.close_on_drop();
    read_report(&mut connection, deadline).await
}

/// [`run_report`] on `connection`, in one read-only transaction. The line is
/// read first, under the session's statement timeout as it always was, so the
/// report is the run's last statement, and its own statement timeout, the time
/// then left to `deadline`, ends it in the server by then too.
async fn read_report(connection: &mut PgConnection, deadline: Instant) -> anyhow::Result<Value> {
    // While a statement runs, the server checks every second that the client
    // is still there, so an abandoned report ends within that long. Best
    // effort, for this connection's life only, which ends with the run: a
    // server that cannot check (before PostgreSQL 14, or on a platform
    // without the check) refuses the setting, and an abandoned report then
    // runs on to its statement timeout.
    let _ = sqlx::query("SELECT set_config('client_connection_check_interval','1000',false)")
        .execute(&mut *connection)
        .await;
    let mut tx = connection.begin().await.context(Step::Connection)?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .context(Step::Connection)?;
    // #478: debt from divergent landings is exact accounting, so the
    // report's mismatch count never shows it; this line does.
    let divergence = crate::ledger::payout_divergence_line(&mut *tx)
        .await
        .context(Step::Divergence)?;
    let left = deadline.saturating_duration_since(Instant::now());
    let mut report = crate::ledger::integrity_report_bounded(&mut tx, left)
        .await
        .context(Step::Report)?;
    // No ROLLBACK to wait for: the transaction ends with the connection,
    // which closes when the run drops it.
    if !report.is_object() {
        return Err(
            anyhow::anyhow!("the carry-forward integrity report is not a JSON object")
                .context(Step::Report),
        );
    }
    report["payout_divergence"] = divergence;
    report["ledger_backend"] = json!("postgres-native");
    Ok(report)
}

#[cfg(test)]
mod tests;

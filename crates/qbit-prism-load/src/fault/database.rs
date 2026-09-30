//! The database faults that run outside the frontends' path (#554): a long
//! `SETTLEMENT_LOCK` holder, and connection-slot exhaustion.
//!
//! Both are driven by spawned tasks that the fault driver polls, never
//! awaits, so the scheduler keeps offering load while they run (EP-STATE).
//! Each connects straight to PostgreSQL, not through the delay proxy, with
//! an `application_name` of its own so the lock sampler and the report can
//! tell it from a frontend.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};

/// `SETTLEMENT_LOCK` in `crates/qbit-prism-server/src/ledger.rs`, the key
/// `pg_advisory_xact_lock` takes. `measure::SETTLEMENT_LOCK_OBJID` is its
/// low half; the unit test below holds the two together.
pub const SETTLEMENT_LOCK_KEY: i64 = 0x5052_4953_4d00_0003;

/// The `application_name` of the lock holder's connection.
pub const LOCK_HOLDER_NAME: &str = "load-fault-settlement-lock";
/// The `application_name` of every connection the exhaustion opens.
pub const EXHAUSTION_NAME: &str = "load-fault-exhaustion";

/// An outside transaction holding an advisory lock until it is released.
pub struct LockHolder {
    acquired: Option<oneshot::Receiver<Instant>>,
    acquired_at: Option<Instant>,
    release: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<Instant>>>,
    released_at: Option<Instant>,
    error: Option<String>,
}

impl LockHolder {
    /// Connect to `url` and take `key` inside a transaction. The lock is
    /// queued behind whoever holds it now, as a server writer's would be.
    pub fn start(url: String, key: i64) -> Self {
        let (acquired_tx, acquired) = oneshot::channel();
        let (release, release_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let url = crate::run::with_application_name(&url, LOCK_HOLDER_NAME);
            let mut connection = PgConnection::connect(&url)
                .await
                .context("connecting the lock holder")?;
            sqlx::query("SET statement_timeout = 0")
                .execute(&mut connection)
                .await?;
            sqlx::query("SET lock_timeout = 0")
                .execute(&mut connection)
                .await?;
            sqlx::query("BEGIN").execute(&mut connection).await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(key)
                .execute(&mut connection)
                .await
                .context("taking the advisory lock")?;
            let _ = acquired_tx.send(Instant::now());
            // A dropped sender releases too: the lock never outlives the
            // driver that asked for it.
            let _ = release_rx.await;
            // Stamped as the ROLLBACK is sent: until then the lock is
            // certainly held, so work on the tip before this is a violation
            // and none after it can be one.
            let released = Instant::now();
            sqlx::query("ROLLBACK").execute(&mut connection).await?;
            let _ = connection.close().await;
            Ok(released)
        });
        Self {
            acquired: Some(acquired),
            acquired_at: None,
            release: Some(release),
            task: Some(task),
            released_at: None,
            error: None,
        }
    }

    /// When the lock was taken, once it has been. An error once the task
    /// failed before taking it.
    pub fn poll_acquired(&mut self) -> Result<Option<Instant>> {
        if let Some(at) = self.acquired_at {
            return Ok(Some(at));
        }
        if let Some(receiver) = self.acquired.as_mut() {
            match receiver.try_recv() {
                Ok(at) => {
                    self.acquired_at = Some(at);
                    self.acquired = None;
                    return Ok(Some(at));
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => self.acquired = None,
            }
        }
        if self.acquired.is_none() {
            // The sender drops as the task returns, a moment before the task
            // reads as finished: wait for that, so its error is the reason.
            if self.poll_released()?.is_none() {
                return Ok(None);
            }
            anyhow::bail!(
                "the lock holder ended before it took the lock: {}",
                self.error.as_deref().unwrap_or("no reason given")
            );
        }
        Ok(None)
    }

    pub fn release(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }

    /// When the transaction ended, once it has. The task's own error is kept
    /// for the report.
    pub fn poll_released(&mut self) -> Result<Option<Instant>> {
        if self.released_at.is_some() {
            return Ok(self.released_at);
        }
        let Some(task) = self.task.as_ref() else {
            return Ok(None);
        };
        if !task.is_finished() {
            return Ok(None);
        }
        let result = super::finished(self.task.as_mut().expect("checked above"));
        if result.is_some() {
            self.task = None;
        }
        match result {
            Some(Ok(Ok(at))) => {
                self.released_at = Some(at);
                Ok(Some(at))
            }
            Some(Ok(Err(error))) => {
                self.error = Some(format!("{error:#}"));
                // Ended, with its lock gone either way: the connection
                // closed and PostgreSQL ended the transaction.
                self.released_at = Some(Instant::now());
                Ok(self.released_at)
            }
            Some(Err(error)) => {
                self.error = Some(format!("lock holder task: {error}"));
                self.released_at = Some(Instant::now());
                Ok(self.released_at)
            }
            None => Ok(None),
        }
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

impl Drop for LockHolder {
    fn drop(&mut self) {
        self.release();
    }
}

/// What connection-slot exhaustion did.
#[derive(Clone, Debug, Default)]
pub struct Exhaustion {
    /// The most connections the exhaustion held at once.
    pub held_peak: usize,
    /// When PostgreSQL first refused a connection for want of a slot.
    pub saturated_at: Option<Instant>,
    /// That refusal's SQLSTATE and message.
    pub refusal: Option<String>,
    /// The frontend backends terminated, by `application_name`.
    pub terminated: Vec<String>,
    /// Slots the frontends freed that the exhaustion then took.
    pub slots_taken_after_saturation: usize,
    pub error: Option<String>,
}

impl Exhaustion {
    pub fn report(&self, origin: Instant) -> Value {
        json!({
            "held_peak": self.held_peak,
            "saturated_after_seconds": self.saturated_at
                .map(|at| at.saturating_duration_since(origin).as_secs_f64()),
            "refusal": self.refusal,
            "frontend_backends_terminated": self.terminated,
            "slots_taken_after_saturation": self.slots_taken_after_saturation,
            "error": self.error,
        })
    }
}

/// Take every free connection slot on the server, then terminate the
/// frontends' idle backends and keep taking the slots they free, until
/// stopped. A frontend that needs a new connection meanwhile is refused, as
/// it would be by a server at `max_connections`.
pub struct Exhauster {
    stop: watch::Sender<bool>,
    progress: watch::Receiver<Exhaustion>,
    task: Option<JoinHandle<Exhaustion>>,
    outcome: Option<Exhaustion>,
}

/// How often a freed slot is looked for once the server is saturated.
const SOAK_INTERVAL: Duration = Duration::from_millis(20);
/// A bound on the connections one exhaustion may hold, so a server with no
/// limit (or a much larger one than the cluster the harness manages) cannot
/// make it open connections forever.
pub const EXHAUSTION_CEILING: usize = 2_000;

impl Exhauster {
    pub fn start(url: String, frontend_prefix: &'static str) -> Self {
        let (stop, mut stop_rx) = watch::channel(false);
        let (progress_tx, progress) = watch::channel(Exhaustion::default());
        let task = tokio::spawn(async move {
            let url = crate::run::with_application_name(&url, EXHAUSTION_NAME);
            let mut held: Vec<PgConnection> = Vec::new();
            let mut outcome = Exhaustion::default();
            let publish = |outcome: &Exhaustion| {
                let _ = progress_tx.send(outcome.clone());
            };
            // Fill every slot.
            while outcome.saturated_at.is_none() && !*stop_rx.borrow() {
                if held.len() >= EXHAUSTION_CEILING {
                    outcome.error = Some(format!(
                        "the server accepted {EXHAUSTION_CEILING} connections without refusing \
                         one; not an exhaustible server"
                    ));
                    break;
                }
                match PgConnection::connect(&url).await {
                    Ok(connection) => {
                        held.push(connection);
                        outcome.held_peak = outcome.held_peak.max(held.len());
                    }
                    Err(error) if is_slot_refusal(&error) => {
                        outcome.saturated_at = Some(Instant::now());
                        outcome.refusal = Some(describe(&error));
                    }
                    Err(error) => {
                        outcome.error = Some(format!("opening a connection: {error}"));
                        break;
                    }
                }
            }
            publish(&outcome);
            // Terminate the frontends' idle backends through a slot already
            // held, so their pools have to reconnect into a full server.
            if outcome.saturated_at.is_some() {
                if let Some(connection) = held.first_mut() {
                    let terminated: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar(
                        "SELECT application_name FROM pg_stat_activity \
                         WHERE application_name LIKE $1 AND state = 'idle' \
                         AND pid <> pg_backend_pid() AND pg_terminate_backend(pid)",
                    )
                    .bind(format!("{frontend_prefix}%"))
                    .fetch_all(&mut *connection)
                    .await;
                    match terminated {
                        Ok(names) => outcome.terminated = names,
                        Err(error) => {
                            outcome.error = Some(format!("terminating frontend backends: {error}"))
                        }
                    }
                }
                publish(&outcome);
                // Keep every slot the terminations and the frontends free.
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        _ = tokio::time::sleep(SOAK_INTERVAL) => {}
                    }
                    if *stop_rx.borrow() {
                        break;
                    }
                    while held.len() < EXHAUSTION_CEILING {
                        match PgConnection::connect(&url).await {
                            Ok(connection) => {
                                held.push(connection);
                                outcome.held_peak = outcome.held_peak.max(held.len());
                                outcome.slots_taken_after_saturation += 1;
                            }
                            Err(_) => break,
                        }
                    }
                    publish(&outcome);
                }
            }
            for connection in held {
                let _ = connection.close().await;
            }
            outcome
        });
        Self {
            stop,
            progress,
            task: Some(task),
            outcome: None,
        }
    }

    /// The exhaustion as it stands.
    pub fn progress(&self) -> Exhaustion {
        self.outcome
            .clone()
            .unwrap_or_else(|| self.progress.borrow().clone())
    }

    pub fn stop(&self) {
        self.stop.send_replace(true);
    }

    /// The final outcome once every held connection is closed.
    pub fn poll_finished(&mut self) -> Option<Exhaustion> {
        if let Some(outcome) = &self.outcome {
            return Some(outcome.clone());
        }
        let task = self.task.as_ref()?;
        if !task.is_finished() {
            return None;
        }
        let outcome = match super::finished(self.task.as_mut().expect("checked above")) {
            Some(Ok(outcome)) => outcome,
            Some(Err(error)) => Exhaustion {
                error: Some(format!("exhaustion task: {error}")),
                ..self.progress.borrow().clone()
            },
            None => return None,
        };
        self.task = None;
        self.outcome = Some(outcome.clone());
        Some(outcome)
    }
}

impl Drop for Exhauster {
    fn drop(&mut self) {
        self.stop();
    }
}

/// SQLSTATE 53300, `too_many_connections`: the server is at
/// `max_connections`, or only reserved slots are left.
fn is_slot_refusal(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.code().as_deref() == Some("53300"))
}

fn describe(error: &sqlx::Error) -> String {
    match error {
        sqlx::Error::Database(database) => format!(
            "{}: {}",
            database.code().unwrap_or_default(),
            database.message()
        ),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_settlement_key_is_the_one_the_lock_sampler_watches() {
        assert_eq!(
            SETTLEMENT_LOCK_KEY >> 32,
            crate::measure::PRISM_LOCK_CLASSID
        );
        assert_eq!(
            SETTLEMENT_LOCK_KEY & 0xffff_ffff,
            crate::measure::SETTLEMENT_LOCK_OBJID
        );
        assert_eq!(
            format!("{SETTLEMENT_LOCK_KEY:#x}"),
            crate::measure::SETTLEMENT_LOCK.key
        );
    }
}

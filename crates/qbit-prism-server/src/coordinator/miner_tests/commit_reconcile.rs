//! Issue #324 without a database: the share-commit deadline reconciles an
//! in-flight COMMIT instead of dropping it. `MemoryLedger` honours the commit
//! gate exactly as the production hook does; real short durations drive the
//! deadlines.
use super::*;
use crate::metrics::{Metrics, RejectReason};
use miner_submit::{classify_share_append, enqueue_failed_before_commit, SaveOutcome};
use submit_ledger::GateState;
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tracing::instrument::WithSubscriber;

const MS: fn(u64) -> Duration = Duration::from_millis;

async fn fixture(tune: impl FnOnce(&mut Config), statement_timeout: Option<Duration>) -> Fixture {
    let fixture = Fixture::build(Duration::from_secs(10), tune, statement_timeout).await;
    fixture.observe(1, true).await;
    fixture
}

/// A current-tip proof. `candidate` keeps its block pass, so the append also
/// carries the found block.
fn proof(fixture: &Fixture, candidate: bool) -> (MiningJob<JobContext>, codec::Submission) {
    let job = fixture.job(1, 0, "original.worker");
    let mut proof = fixture.proof(&job, 0);
    proof.block_pass = candidate;
    (job, proof)
}

fn submit(
    fixture: &Fixture,
    (job, proof): (MiningJob<JobContext>, codec::Submission),
) -> (JoinHandle<Result<(), StratumError>>, SharedLog) {
    let log = SharedLog::default();
    let coordinator = fixture.coordinator.clone();
    let submitted = tokio::spawn(
        async move {
            coordinator
                .submit(&job.context.worker, &job, proof, false.into())
                .await
        }
        .with_subscriber(log.dispatch()),
    );
    (submitted, log)
}

fn late_confirmed(fixture: &Fixture) -> u64 {
    fixture
        .coordinator
        .metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix("qbit_prism_late_confirmed_shares_total "))
        .map_or(0, |value| value.trim().parse::<f64>().unwrap() as u64)
}

async fn eventually(what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = TokioInstant::now() + Duration::from_secs(2);
    while !probe() {
        assert!(
            TokioInstant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(MS(10)).await;
    }
}

fn records(fixture: &Fixture) -> usize {
    fixture.store.records.lock().unwrap().len()
}

#[tokio::test]
async fn commit_reconcile_commit_confirmed_within_grace_is_accepted_late() {
    let fixture = fixture(
        |config| {
            config.share_commit_timeout = MS(300);
            config.share_commit_grace = MS(700);
        },
        None,
    )
    .await;
    let commit = Arc::new(Gate::default());
    *fixture.store.commit_gate.lock().unwrap() = Some(commit.clone());
    let started = TokioInstant::now();
    let (submitted, _log) = submit(&fixture, proof(&fixture, false));
    commit.entered.notified().await;
    tokio::time::sleep_until(started + MS(450)).await;
    assert!(
        !submitted.is_finished(),
        "an in-flight COMMIT was answered at the share deadline"
    );
    commit.release.notify_one();
    submitted
        .await
        .unwrap()
        .expect("a COMMIT confirmed within the grace period is accepted");
    assert!(started.elapsed() >= MS(450));
    assert_eq!(records(&fixture), 1);
    assert_eq!(late_confirmed(&fixture), 1);
    assert_eq!(fixture.coordinator.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn commit_reconcile_confirmation_before_the_deadline_is_not_counted_late() {
    // The counter is for confirmations that land after the share deadline. A
    // commit confirmed well inside it is an ordinary accept, however late this
    // task is polled afterwards.
    let fixture = fixture(
        |config| {
            config.share_commit_timeout = MS(600);
            config.share_commit_grace = MS(400);
        },
        None,
    )
    .await;
    let commit = Arc::new(Gate::default());
    *fixture.store.commit_gate.lock().unwrap() = Some(commit.clone());
    let started = TokioInstant::now();
    let (submitted, _log) = submit(&fixture, proof(&fixture, false));
    commit.entered.notified().await;
    tokio::time::sleep_until(started + MS(150)).await;
    commit.release.notify_one();
    submitted
        .await
        .unwrap()
        .expect("a COMMIT confirmed inside the deadline is accepted");
    assert!(
        started.elapsed() < MS(600),
        "the confirmation was not on time"
    );
    assert_eq!(records(&fixture), 1);
    assert_eq!(late_confirmed(&fixture), 0);
    assert_eq!(fixture.coordinator.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn commit_reconcile_append_before_commit_is_refused_and_aborted_at_the_deadline() {
    let fixture = fixture(|config| config.share_commit_timeout = MS(300), None).await;
    let append = Arc::new(Gate::default());
    *fixture.store.append_gate.lock().unwrap() = Some(append.clone());
    let started = TokioInstant::now();
    let (submitted, _log) = submit(&fixture, proof(&fixture, false));
    append.entered.notified().await;
    assert_error(
        submitted.await.unwrap().unwrap_err(),
        "ledger-confirmation-failed",
        "share was not confirmed by the database",
    );
    let answered = started.elapsed();
    assert!(
        answered >= MS(300) && answered < MS(1000),
        "a refused append was answered after {answered:?}, not at the deadline"
    );
    eventually("the refused append to be aborted", || {
        fixture.store.cancelled.load(Ordering::SeqCst) == 1
    })
    .await;
    // Releasing the pre-COMMIT hold can no longer produce a record.
    append.release.notify_one();
    tokio::time::sleep(MS(100)).await;
    assert_eq!(records(&fixture), 0);
    assert_eq!(late_confirmed(&fixture), 0);
}

#[tokio::test]
async fn commit_reconcile_commit_still_in_flight_after_grace_is_unknown() {
    let fixture = fixture(
        |config| {
            config.share_commit_timeout = MS(200);
            config.share_commit_grace = MS(300);
        },
        None,
    )
    .await;
    let commit = Arc::new(Gate::default());
    *fixture.store.commit_gate.lock().unwrap() = Some(commit.clone());
    let started = TokioInstant::now();
    let (job, proof) = proof(&fixture, false);
    let share_id = format!("original.worker:{}", proof.block_hash_hex);
    let (submitted, log) = submit(&fixture, (job, proof));
    commit.entered.notified().await;
    assert_error(
        submitted.await.unwrap().unwrap_err(),
        "ledger-outcome-unknown",
        "share outcome is not yet known",
    );
    assert!(started.elapsed() >= MS(500));
    let text = log.text();
    assert!(text.contains("commit-in-flight"), "{text}");
    assert!(text.contains(&share_id), "{text}");
    // The in-flight COMMIT is never cancelled, and it still lands.
    assert_eq!(fixture.store.cancelled.load(Ordering::SeqCst), 0);
    commit.release.notify_one();
    eventually("the in-flight COMMIT to land", || records(&fixture) == 1).await;
    assert_eq!(fixture.coordinator.accepted.load(Ordering::SeqCst), 0);
    assert_eq!(late_confirmed(&fixture), 0);
}

#[tokio::test]
async fn commit_reconcile_indeterminate_commit_error_is_unknown() {
    for (failure, recorded) in [(FailCommit::NotRecorded, 0), (FailCommit::Recorded, 1)] {
        let fixture = fixture(|_| {}, None).await;
        *fixture.store.fail_commit.lock().unwrap() = Some(failure);
        let started = TokioInstant::now();
        let (submitted, log) = submit(&fixture, proof(&fixture, false));
        assert_error(
            submitted.await.unwrap().unwrap_err(),
            "ledger-outcome-unknown",
            "share outcome is not yet known",
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(log.text().contains("commit-error"), "{}", log.text());
        assert_eq!(records(&fixture), recorded);
    }
}

#[tokio::test]
async fn commit_reconcile_candidate_bearing_append_is_never_refused() {
    // Held before COMMIT past the share deadline, then released before the
    // block-only bound: accepted late, found block kept.
    let fixture_ = fixture(
        |config| {
            config.share_commit_timeout = MS(200);
            config.block_only_ack_timeout = MS(1000);
        },
        None,
    )
    .await;
    let append = Arc::new(Gate::default());
    *fixture_.store.append_gate.lock().unwrap() = Some(append.clone());
    let started = TokioInstant::now();
    let (submitted, _log) = submit(&fixture_, proof(&fixture_, true));
    append.entered.notified().await;
    tokio::time::sleep_until(started + MS(350)).await;
    assert!(
        !submitted.is_finished(),
        "a found block was refused at the share deadline"
    );
    append.release.notify_one();
    submitted
        .await
        .unwrap()
        .expect("the candidate-bearing share is accepted");
    assert_eq!(late_confirmed(&fixture_), 1);
    {
        let records = fixture_.store.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].1.is_some(), "the found block was discarded");
    }

    // Held past the block-only bound: unknown, and the append runs on.
    let fixture = fixture(
        |config| {
            config.share_commit_timeout = MS(200);
            config.block_only_ack_timeout = MS(400);
        },
        None,
    )
    .await;
    let append = Arc::new(Gate::default());
    *fixture.store.append_gate.lock().unwrap() = Some(append.clone());
    let started = TokioInstant::now();
    let (submitted, log) = submit(&fixture, proof(&fixture, true));
    append.entered.notified().await;
    assert_error(
        submitted.await.unwrap().unwrap_err(),
        "ledger-outcome-unknown",
        "share outcome is not yet known",
    );
    assert!(started.elapsed() >= MS(400));
    assert!(log.text().contains("candidate-pending"), "{}", log.text());
    append.release.notify_one();
    eventually("the candidate-bearing append to land", || {
        records(&fixture) == 1
    })
    .await;
    assert!(fixture.store.records.lock().unwrap()[0].1.is_some());
    assert_eq!(fixture.store.cancelled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn commit_reconcile_commit_as_long_as_statement_timeout_is_unknown() {
    let fixture = fixture(|_| {}, Some(MS(200))).await;
    let commit = Arc::new(Gate::default());
    *fixture.store.commit_gate.lock().unwrap() = Some(commit.clone());
    let started = TokioInstant::now();
    let (submitted, log) = submit(&fixture, proof(&fixture, false));
    commit.entered.notified().await;
    tokio::time::sleep(MS(300)).await;
    commit.release.notify_one();
    assert_error(
        submitted.await.unwrap().unwrap_err(),
        "ledger-outcome-unknown",
        "share outcome is not yet known",
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "answered after the share deadline"
    );
    let text = log.text();
    assert!(text.contains("sync-rep-guard"), "{text}");
    assert!(text.contains("possible sync-rep cancellation"), "{text}");
    assert_eq!(records(&fixture), 1);
    assert_eq!(fixture.coordinator.accepted.load(Ordering::SeqCst), 0);
}

// Classifier. A severity-ERROR `PgDatabaseError` cannot be built outside
// sqlx, so the definite COMMIT failure is proved against PostgreSQL, as are
// FATAL replies (`commit_reconcile_tests`).

#[derive(Debug)]
struct ForeignDatabaseError;

impl std::fmt::Display for ForeignDatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a database error from another driver")
    }
}

impl std::error::Error for ForeignDatabaseError {}

impl sqlx::error::DatabaseError for ForeignDatabaseError {
    fn message(&self) -> &str {
        "a database error from another driver"
    }
    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> sqlx::error::ErrorKind {
        sqlx::error::ErrorKind::Other
    }
}

fn errors() -> Vec<(&'static str, anyhow::Error)> {
    vec![
        (
            "io",
            sqlx::Error::Io(std::io::ErrorKind::UnexpectedEof.into()).into(),
        ),
        (
            "protocol",
            sqlx::Error::Protocol("unexpected message".into()).into(),
        ),
        (
            "foreign-database",
            sqlx::Error::Database(Box::new(ForeignDatabaseError)).into(),
        ),
        ("non-sqlx", anyhow::anyhow!("worker crashed")),
        (
            "wrapped-io",
            anyhow::Error::from(sqlx::Error::Io(std::io::ErrorKind::BrokenPipe.into()))
                .context("committing"),
        ),
    ]
}

async fn join_error() -> tokio::task::JoinError {
    let task = tokio::spawn(std::future::pending::<Result<bool>>());
    task.abort();
    task.await.unwrap_err()
}

#[tokio::test]
async fn commit_reconcile_errors_before_committing_are_definite() {
    for state in [GateState::Open, GateState::Closed] {
        for (name, error) in errors().into_iter().chain([(
            "gate-closed",
            anyhow::Error::from(crate::ledger::CommitGateClosed),
        )]) {
            let outcome = classify_share_append(Ok(Err(error)), state, None, None);
            assert!(
                matches!(outcome, SaveOutcome::Failed(_)),
                "{state:?} {name}: {outcome:?}"
            );
        }
        let outcome = classify_share_append(Err(join_error().await), state, None, None);
        assert!(matches!(outcome, SaveOutcome::Failed(_)), "{outcome:?}");
        let outcome = classify_share_append(Ok(Ok(true)), state, None, Some(MS(1)));
        assert!(matches!(outcome, SaveOutcome::Accepted), "{outcome:?}");
        let outcome = classify_share_append(Ok(Ok(false)), state, None, None);
        assert!(matches!(outcome, SaveOutcome::Duplicate), "{outcome:?}");
    }
}

#[tokio::test]
async fn commit_reconcile_errors_after_committing_are_unknown() {
    let committing = GateState::Committing;
    for (name, error) in errors() {
        let outcome = classify_share_append(Ok(Err(error)), committing, Some(MS(1)), None);
        assert!(
            matches!(
                outcome,
                SaveOutcome::Unknown {
                    phase: "commit-error",
                    ..
                }
            ),
            "{name}: {outcome:?}"
        );
    }
    let outcome = classify_share_append(Err(join_error().await), committing, Some(MS(1)), None);
    assert!(
        matches!(
            outcome,
            SaveOutcome::Unknown {
                phase: "commit-error",
                ..
            }
        ),
        "{outcome:?}"
    );
    let outcome = classify_share_append(Ok(Ok(false)), committing, Some(MS(1)), None);
    assert!(matches!(outcome, SaveOutcome::Duplicate), "{outcome:?}");
}

#[test]
fn commit_reconcile_sync_rep_guard_answers_long_commits_unknown() {
    let committing = GateState::Committing;
    for (elapsed, limit, accepted) in [
        (Some(Duration::from_secs(60)), None, true),
        (Some(MS(199)), Some(MS(200)), true),
        (Some(MS(200)), Some(MS(200)), false),
        (Some(MS(900)), Some(MS(200)), false),
        (None, Some(MS(200)), false),
    ] {
        let outcome = classify_share_append(Ok(Ok(true)), committing, elapsed, limit);
        if accepted {
            assert!(
                matches!(outcome, SaveOutcome::Accepted),
                "{elapsed:?}: {outcome:?}"
            );
        } else {
            assert!(
                matches!(
                    outcome,
                    SaveOutcome::Unknown {
                        phase: "sync-rep-guard",
                        ..
                    }
                ),
                "{elapsed:?}: {outcome:?}"
            );
        }
    }
}

#[test]
fn commit_reconcile_block_only_enqueue_errors_are_definite_only_before_commit() {
    for (name, definite) in [
        ("io", false),
        ("protocol", false),
        ("foreign-database", false),
        ("non-sqlx", true),
        ("wrapped-io", false),
    ] {
        let error = errors()
            .into_iter()
            .find(|(candidate, _)| *candidate == name)
            .unwrap()
            .1;
        assert_eq!(enqueue_failed_before_commit(&error), definite, "{name}");
    }
    for error in [sqlx::Error::PoolTimedOut, sqlx::Error::PoolClosed] {
        assert!(enqueue_failed_before_commit(&error.into()));
    }
}

#[test]
fn commit_reconcile_unknown_reason_is_its_own_closed_label() {
    assert_eq!(
        RejectReason::from_reason_id(Some("ledger-outcome-unknown")),
        RejectReason::LedgerOutcomeUnknown
    );
    let error = protocol_error("ledger-outcome-unknown", "share outcome is not yet known");
    let response = error.response(json!(7));
    assert_eq!(response["error"][0], 20);
    assert_eq!(response["error"][2]["reason_id"], "ledger-outcome-unknown");
    let metrics = Metrics::default();
    metrics.record_rejection(RejectReason::from_reason_id(error.reason_id.as_deref()));
    let rendered = metrics.render();
    assert!(
        rendered.contains("qbit_prism_rejections_total{reason_id=\"ledger-outcome-unknown\"} 1"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("reason_id=\"internal-error\"} 1"),
        "{rendered}"
    );
}

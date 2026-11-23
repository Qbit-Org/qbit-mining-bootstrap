//! `/audit/latest`'s runs (#859), with runs injected in place of the ledger.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use tokio::sync::oneshot;

fn runs(timeout_ms: u64, reuse_ms: u64) -> Arc<LatestEvidence> {
    Arc::new(LatestEvidence::new(
        Duration::from_millis(timeout_ms),
        Duration::from_millis(reuse_ms),
    ))
}

fn evidence(block: &str) -> Arc<Evidence> {
    Arc::new(Evidence {
        key: (block.repeat(64), "bb".repeat(32), Some("cc".repeat(32))),
        fields: json!({"job_share_count": 3}),
    })
}

type Answer = std::future::Ready<ApiResult<(Value, Arc<Evidence>)>>;

/// A run that counts itself in `runs`, records the evidence it was handed in
/// `seen`, and answers `answer` about block `block` at once.
fn counted(
    runs: &Arc<AtomicUsize>,
    seen: &Arc<std::sync::Mutex<Vec<Option<String>>>>,
    block: &'static str,
    answer: Value,
) -> impl FnOnce(Option<Arc<Evidence>>) -> Answer {
    let (runs, seen) = (runs.clone(), seen.clone());
    move |previous| {
        runs.fetch_add(1, SeqCst);
        seen.lock()
            .unwrap()
            .push(previous.map(|evidence| evidence.key.0.clone()));
        std::future::ready(Ok((answer, evidence(block))))
    }
}

async fn panicking() -> ApiResult<(Value, Arc<Evidence>)> {
    panic!("latest evidence run panicked")
}

/// A run no request may start.
fn unstarted(_: Option<Arc<Evidence>>) -> Answer {
    panic!("a request started a run while another one's answer was current")
}

async fn until(done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition reached");
}

fn running(evidence: &LatestEvidence) -> bool {
    evidence.lock().running.is_some()
}

fn computed_at(answer: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(answer["computed_at"].as_str().expect("dated"))
        .expect("RFC 3339")
        .with_timezone(&Utc)
}

/// Requests that arrive while a run is in flight start none of their own:
/// they wait for that run and get its answer, with when it was computed.
#[tokio::test]
async fn concurrent_requests_share_one_run() {
    let evidence = runs(60_000, 60_000);
    let (release, released) = oneshot::channel::<()>();
    let first = tokio::spawn({
        let evidence = evidence.clone();
        async move {
            evidence
                .answer_with(move |_| async move {
                    released.await.expect("released");
                    Ok((json!({"accepted_share_count":7}), self::evidence("aa")))
                })
                .await
        }
    });
    until(|| running(&evidence)).await;
    let waiting: Vec<_> = (0..7)
        .map(|_| {
            let evidence = evidence.clone();
            tokio::spawn(async move { evidence.answer_with(unstarted).await })
        })
        .collect();
    tokio::task::yield_now().await;
    release.send(()).unwrap();
    let answer = first.await.unwrap().unwrap();
    assert_eq!(answer["accepted_share_count"], 7, "{answer}");
    for request in waiting {
        let other = request.await.unwrap().unwrap();
        assert_eq!(other["accepted_share_count"], 7);
        assert_eq!(other["computed_at"], answer["computed_at"]);
    }
    assert!(!running(&evidence));
}

/// An answer carries when its run started, its age and its run time, and is
/// served until its window ends; the next run is handed its evidence.
#[tokio::test]
async fn an_answer_is_dated_and_served_until_its_window_ends() {
    let evidence = runs(60_000, 300);
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::default();
    let before = Utc::now();
    let first = evidence
        .answer_with(counted(
            &count,
            &seen,
            "aa",
            json!({"accepted_share_count":1}),
        ))
        .await
        .unwrap();
    let at = computed_at(&first);
    assert!(at >= before - chrono::Duration::milliseconds(1) && at <= Utc::now());
    assert!(first["age_seconds"].as_f64().unwrap() >= 0.0);
    assert!(first["run_seconds"].as_f64().unwrap() >= 0.0);
    let reused = evidence.answer_with(unstarted).await.unwrap();
    assert_eq!(reused["accepted_share_count"], 1);
    assert_eq!(computed_at(&reused), at);
    tokio::time::sleep(Duration::from_millis(350)).await;
    let fresh = evidence
        .answer_with(counted(
            &count,
            &seen,
            "dd",
            json!({"accepted_share_count":2}),
        ))
        .await
        .unwrap();
    assert_eq!(fresh["accepted_share_count"], 2);
    assert!(computed_at(&fresh) > at);
    assert_eq!(count.load(SeqCst), 2);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![None, Some("aa".repeat(64))],
        "the second run is handed the first run's evidence"
    );
}

/// A failed run answers its own waiters with its own error and is not served
/// again: the next request starts a run, handed the newest answer's evidence.
#[tokio::test]
async fn a_failure_is_not_remembered() {
    let evidence = runs(60_000, 60_000);
    let error = evidence
        .answer_with(|_| async {
            Err::<(Value, Arc<Evidence>), _>(ApiError::missing(
                "no PRISM evidence has been produced",
            ))
        })
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::NOT_FOUND);
    assert_eq!(error.message, "no PRISM evidence has been produced");
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::default();
    let answer = evidence
        .answer_with(counted(
            &count,
            &seen,
            "aa",
            json!({"accepted_share_count":1}),
        ))
        .await
        .unwrap();
    assert_eq!(answer["accepted_share_count"], 1);
    assert_eq!(count.load(SeqCst), 1);
    assert_eq!(*seen.lock().unwrap(), vec![None]);
}

/// A run that finishes after a newer one never replaces the newer answer.
#[tokio::test]
async fn an_earlier_run_never_replaces_a_later_answer() {
    let evidence = runs(60_000, 60_000);
    let finished = |number: u64, answer: Value| {
        let now = tokio::time::Instant::now();
        Arc::new(EvidenceRun {
            number,
            computed_at: Utc::now(),
            started: now,
            ended: now,
            outcome: Ok(EvidenceAnswer {
                answer,
                evidence: self::evidence("aa"),
            }),
        })
    };
    evidence.publish(finished(2, json!({"run":2})));
    evidence.publish(finished(1, json!({"run":1})));
    assert_eq!(evidence.answer_with(unstarted).await.unwrap()["run"], 2);
}

/// Drops `released` in its `Drop`, so a test sees when a run's future went.
struct Held(Arc<AtomicBool>);
impl Drop for Held {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
    }
}

/// A run past its timeout is dropped and fails as a timeout, leaving nothing
/// in flight: the next request starts a run.
#[tokio::test]
async fn a_run_past_its_timeout_fails_and_leaves_nothing_in_flight() {
    let evidence = runs(100, 60_000);
    let dropped = Arc::new(AtomicBool::new(false));
    let held = Held(dropped.clone());
    let error = evidence
        .answer_with(move |_| async move {
            let _held = held;
            std::future::pending::<ApiResult<(Value, Arc<Evidence>)>>().await
        })
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.code, "read_timeout");
    assert!(
        error.message.contains("did not finish within"),
        "{}",
        error.message
    );
    until(|| dropped.load(SeqCst) && !running(&evidence)).await;
    let count = Arc::new(AtomicUsize::new(0));
    let answer = evidence
        .answer_with(counted(&count, &Arc::default(), "aa", json!({"ok":true})))
        .await
        .unwrap();
    assert_eq!(answer["ok"], true);
    assert_eq!(count.load(SeqCst), 1);
}

/// A request that stops waiting, as when its client disconnects, leaves the
/// run going: its answer is published and serves the next request.
#[tokio::test]
async fn a_request_that_stops_waiting_leaves_its_run_to_finish() {
    let evidence = runs(60_000, 60_000);
    let (release, released) = oneshot::channel::<()>();
    let request = tokio::spawn({
        let evidence = evidence.clone();
        async move {
            evidence
                .answer_with(move |_| async move {
                    released.await.expect("released");
                    Ok((json!({"accepted_share_count":5}), self::evidence("aa")))
                })
                .await
        }
    });
    until(|| running(&evidence)).await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(running(&evidence));
    release.send(()).unwrap();
    until(|| !running(&evidence)).await;
    let answer = evidence.answer_with(unstarted).await.unwrap();
    assert_eq!(answer["accepted_share_count"], 5);
}

/// A run whose task panics fails its waiters and leaves nothing in flight:
/// the next request starts a run.
#[tokio::test]
async fn a_run_that_panics_leaves_nothing_in_flight() {
    let evidence = runs(60_000, 60_000);
    let error = evidence.answer_with(|_| panicking()).await.unwrap_err();
    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!running(&evidence));
    let count = Arc::new(AtomicUsize::new(0));
    evidence
        .answer_with(counted(&count, &Arc::default(), "aa", json!({})))
        .await
        .unwrap();
    assert_eq!(count.load(SeqCst), 1);
}

/// A stopping frontend abandons its run in flight, answers its waiters at
/// once, and starts no run after.
#[tokio::test]
async fn a_stop_abandons_the_run_in_flight_and_starts_none() {
    let (stop, stopping) = watch::channel(false);
    let evidence = Arc::new(runs(60_000, 0).stopping_on(stopping));
    let dropped = Arc::new(AtomicBool::new(false));
    let held = Held(dropped.clone());
    let request = tokio::spawn({
        let evidence = evidence.clone();
        async move {
            evidence
                .answer_with(move |_| async move {
                    let _held = held;
                    std::future::pending::<ApiResult<(Value, Arc<Evidence>)>>().await
                })
                .await
        }
    });
    until(|| running(&evidence)).await;
    stop.send_replace(true);
    let error = request.await.unwrap().unwrap_err();
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(error.message.contains("stopped"), "{}", error.message);
    assert!(dropped.load(SeqCst));
    assert!(!running(&evidence));
    let refused = evidence.answer_with(unstarted).await.unwrap_err();
    assert!(refused.message.contains("stopping"), "{}", refused.message);
}

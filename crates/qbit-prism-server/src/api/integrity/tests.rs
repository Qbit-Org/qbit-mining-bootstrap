use super::*;
use axum::http::StatusCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use tokio::sync::oneshot;

fn reports(timeout: u64, reuse: u64) -> Arc<IntegrityReports> {
    Arc::new(IntegrityReports::new(
        Duration::from_secs(timeout),
        Duration::from_secs(reuse),
    ))
}

/// A run that counts itself in `runs` and reports `report` at once.
fn counted(
    runs: &Arc<AtomicUsize>,
    report: Value,
) -> impl FnOnce(Instant) -> std::future::Ready<anyhow::Result<Value>> {
    let runs = runs.clone();
    move |_| {
        runs.fetch_add(1, SeqCst);
        std::future::ready(Ok(report))
    }
}

async fn panicking() -> anyhow::Result<Value> {
    panic!("report panicked")
}

/// A run no request may start.
fn unstarted(_: Instant) -> std::future::Ready<anyhow::Result<Value>> {
    panic!("a request started a run while another one's outcome was current")
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

fn running(reports: &IntegrityReports) -> bool {
    reports.lock().running.is_some()
}

/// Run `number`, which took 10 s and has just ended.
fn finished(number: u64, outcome: Result<Value, String>) -> Arc<Run> {
    let now = Instant::now();
    Arc::new(Run {
        number,
        computed_at: Utc::now(),
        started: now - Duration::from_secs(10),
        ended: now,
        outcome,
    })
}

/// Requests that arrive while a run is in flight start none of their own:
/// they wait for that run and get its outcome, with when it was computed.
#[tokio::test]
async fn concurrent_requests_share_one_run() {
    let reports = reports(60, 60);
    let runs = Arc::new(AtomicUsize::new(0));
    let (release, released) = oneshot::channel::<()>();
    let first = {
        let runs = runs.clone();
        reports.answer_request_with(move |_| {
            runs.fetch_add(1, SeqCst);
            async move {
                released.await?;
                Ok(json!({"mismatch_count":0,"current_drift_count":0}))
            }
        })
    };
    let releasing = async {
        // Each request has been polled once, and is waiting, before this.
        tokio::task::yield_now().await;
        release.send(()).unwrap();
    };
    // Polled in order: the first request starts the run.
    let (first, second, third, ()) = futures_util::future::join4(
        first,
        reports.answer_request_with(counted(&runs, json!({}))),
        reports.answer_request_with(counted(&runs, json!({}))),
        releasing,
    )
    .await;
    assert_eq!(runs.load(SeqCst), 1);
    let (first, second, third) = (first.unwrap(), second.unwrap(), third.unwrap());
    assert_eq!(first["mismatch_count"], 0, "{first}");
    assert!(first["computed_at"].is_string(), "{first}");
    assert!(first["age_seconds"].is_f64(), "{first}");
    assert!(first["run_seconds"].is_f64(), "{first}");
    assert_eq!(first["computed_at"], second["computed_at"]);
    assert_eq!(first["computed_at"], third["computed_at"]);
    assert_eq!(second["mismatch_count"], 0, "{second}");
    assert!(!running(&reports));
}

/// A run's outcome is served, with its age, until the reuse window after it
/// ends; the first request after that starts the next run.
#[tokio::test(start_paused = true)]
async fn an_outcome_is_reused_until_its_window_ends() {
    let reports = reports(300, 60);
    let runs = Arc::new(AtomicUsize::new(0));
    let first = reports
        .answer_request_with(counted(&runs, json!({"mismatch_count":0})))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(59)).await;
    let reused = reports.answer_request_with(unstarted).await.unwrap();
    assert_eq!(reused["computed_at"], first["computed_at"]);
    assert_eq!(reused["age_seconds"], 59.0, "{reused}");
    assert_eq!(runs.load(SeqCst), 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    let next = reports
        .answer_request_with(counted(&runs, json!({"mismatch_count":1})))
        .await
        .unwrap();
    assert_eq!(runs.load(SeqCst), 2);
    assert_eq!(next["mismatch_count"], 1, "{next}");
    assert_eq!(next["age_seconds"], 0.0, "{next}");
}

/// Sets its flag when dropped: a run's future, and the connection it holds.
struct Held(Arc<AtomicBool>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
    }
}

/// A run that outlasts its timeout answers as unavailable, never as a report,
/// and is dropped, and with it whatever connection it held. Its failure is
/// served as such for the reuse window, and the first request after that
/// starts a run that can succeed: nothing is left in flight.
#[tokio::test(start_paused = true)]
async fn a_run_past_its_timeout_fails_and_leaves_nothing_in_flight() {
    let reports = reports(300, 60);
    let released = Arc::new(AtomicBool::new(false));
    let held = Held(released.clone());
    let started = Instant::now();
    let error = reports
        .answer_request_with(move |_| async move {
            let _held = held;
            std::future::pending::<anyhow::Result<Value>>().await
        })
        .await
        .unwrap_err();
    // The bound and the grace the server's own cancellation has.
    assert!(
        (Duration::from_secs(305)..Duration::from_secs(306)).contains(&started.elapsed()),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        error
            .message
            .starts_with("the carry-forward integrity report run did not finish within its 300 s; that run started at ")
            && error.message.contains("no report is current; the first request in 60 s starts another run"),
        "{}",
        error.message
    );
    assert!(
        released.load(SeqCst),
        "the run's resources were not released"
    );
    assert!(!running(&reports));
    let again = reports.answer_request_with(unstarted).await.unwrap_err();
    assert_eq!(again.code, "integrity_report_unavailable");
    tokio::time::advance(Duration::from_secs(61)).await;
    let runs = Arc::new(AtomicUsize::new(0));
    let report = reports
        .answer_request_with(counted(&runs, json!({"mismatch_count":0})))
        .await
        .unwrap();
    assert_eq!(report["mismatch_count"], 0);
    assert_eq!(runs.load(SeqCst), 1);
}

/// A failed run's answer says why, and is never a report. A failure is
/// served no longer than its run took: one 20 s run is served for 20 s,
/// and the first request after that starts a run.
#[tokio::test(start_paused = true)]
async fn a_failed_run_answers_why_for_as_long_as_it_ran() {
    let reports = reports(300, 60);
    let error = reports
        .answer_request_with(|_| async {
            tokio::time::sleep(Duration::from_secs(20)).await;
            Err(anyhow::anyhow!("connection reset"))
        })
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        error.message.starts_with(
            "the carry-forward integrity report run failed: an error (see the frontend's log) after 20.0 s of its 300 s; "
        ) && error.message.ends_with("; the first request in 20 s starts another run"),
        "{}",
        error.message
    );
    tokio::time::advance(Duration::from_secs(19)).await;
    let again = reports.answer_request_with(unstarted).await.unwrap_err();
    assert!(
        again.message.ends_with("in 1 s starts another run"),
        "{}",
        again.message
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    let runs = Arc::new(AtomicUsize::new(0));
    let report = reports
        .answer_request_with(counted(&runs, json!({"mismatch_count":0})))
        .await
        .unwrap();
    assert_eq!(report["mismatch_count"], 0);
    assert_eq!(runs.load(SeqCst), 1);
}

/// EP-STATE: an earlier run's outcome that is published after a later run's
/// leaves the later one served, good or failed; a later outcome replaces an
/// earlier one, a failure replacing a report included.
#[tokio::test]
async fn an_earlier_run_never_replaces_a_later_outcome() {
    let reports = reports(300, 60);
    reports.publish(finished(2, Ok(json!({"run":2}))));
    reports.publish(finished(1, Err("run 1 failed".into())));
    assert_eq!(
        reports.answer_request_with(unstarted).await.unwrap()["run"],
        2
    );
    reports.publish(finished(3, Err("run 3 failed".into())));
    reports.publish(finished(2, Ok(json!({"run":2}))));
    let error = reports.answer_request_with(unstarted).await.unwrap_err();
    assert!(
        error.message.starts_with("run 3 failed; "),
        "{}",
        error.message
    );
    reports.publish(finished(4, Ok(json!({"run":4}))));
    assert_eq!(
        reports.answer_request_with(unstarted).await.unwrap()["run"],
        4
    );
}

/// A request that stops waiting, as when its client disconnects, leaves the
/// run going: its outcome is published and serves the next request.
#[tokio::test]
async fn a_request_that_stops_waiting_leaves_its_run_to_finish() {
    let reports = reports(60, 60);
    let (release, released) = oneshot::channel::<()>();
    let request = tokio::spawn({
        let reports = reports.clone();
        async move {
            reports
                .answer_request_with(move |_| async move {
                    released.await?;
                    Ok(json!({"mismatch_count":0}))
                })
                .await
        }
    });
    until(|| running(&reports)).await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(running(&reports));
    release.send(()).unwrap();
    until(|| !running(&reports)).await;
    let report = reports.answer_request_with(unstarted).await.unwrap();
    assert_eq!(report["mismatch_count"], 0, "{report}");
}

/// A run whose task panics answers its waiters as unavailable and leaves
/// nothing in flight: the next request starts a run.
#[tokio::test]
async fn a_run_that_panics_leaves_nothing_in_flight() {
    let reports = reports(60, 60);
    let error = reports
        .answer_request_with(|_| panicking())
        .await
        .unwrap_err();
    assert_eq!(error.code, "integrity_report_unavailable");
    assert!(
        error.message.contains("ended without an outcome"),
        "{}",
        error.message
    );
    assert!(!running(&reports));
    let runs = Arc::new(AtomicUsize::new(0));
    let report = reports
        .answer_request_with(counted(&runs, json!({"mismatch_count":0})))
        .await
        .unwrap();
    assert_eq!(report["mismatch_count"], 0);
    assert_eq!(runs.load(SeqCst), 1);
}

/// A stopping frontend abandons its run in flight: the run's future, and so
/// its connection, is dropped at once, the waiting request answers that the
/// frontend stopped, and nothing is left in flight. It starts no run after.
#[tokio::test]
async fn a_stop_abandons_the_run_in_flight_and_starts_none() {
    let (stop, stopping) = watch::channel(false);
    let reports = Arc::new(reports(60, 60).stopping_on(stopping));
    let released = Arc::new(AtomicBool::new(false));
    let held = Held(released.clone());
    let request = tokio::spawn({
        let reports = reports.clone();
        async move {
            reports
                .answer_request_with(move |_| async move {
                    let _held = held;
                    std::future::pending::<anyhow::Result<Value>>().await
                })
                .await
        }
    });
    until(|| running(&reports)).await;
    stop.send_replace(true);
    let error = tokio::time::timeout(Duration::from_secs(10), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, "integrity_report_unavailable");
    assert!(
        error
            .message
            .starts_with("the frontend stopped during the carry-forward integrity report run; ")
            && error
                .message
                .ends_with("; the frontend is stopping and starts no other run"),
        "{}",
        error.message
    );
    assert!(
        released.load(SeqCst),
        "the run's resources were not released"
    );
    assert!(!running(&reports));
    // The abandoned run's failure, while it is reused, and after that the
    // refusal to start one: either way no run starts.
    let refused = reports.answer_request_with(unstarted).await.unwrap_err();
    assert_eq!(refused.code, "integrity_report_unavailable");
    // Longer than the abandoned run took, so its failure is no longer reused.
    let run = reports.lock().latest.clone().unwrap();
    tokio::time::sleep(run.ended - run.started + Duration::from_millis(10)).await;
    let refused = reports.answer_request_with(unstarted).await.unwrap_err();
    assert!(
        refused.message.starts_with("the frontend is stopping"),
        "{}",
        refused.message
    );
}

/// The failure names the step of the run that failed.
#[test]
fn a_failure_names_its_step() {
    let failed = |step| {
        failure(
            &anyhow::Error::from(sqlx::Error::PoolTimedOut).context(step),
            Duration::from_millis(15_000),
            Duration::from_secs(300),
        )
    };
    assert_eq!(
        failed(Step::Divergence),
        "the carry-forward integrity report run failed at the payout divergence line: no free ledger connection after 15.0 s of its 300 s"
    );
    assert_eq!(
        failed(Step::Connection),
        "the carry-forward integrity report run failed at its connection: no free ledger connection after 15.0 s of its 300 s"
    );
}

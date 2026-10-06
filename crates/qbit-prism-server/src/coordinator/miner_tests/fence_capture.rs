//! #657 without a database: a share admitted at payout revision 0 whose
//! append meets revision 1 at the fence, as when a settlement commits between
//! the submit check and the append's read under `ORDER_LOCK`. `MemoryLedger`
//! models the production fence: a plain share is refused before any write,
//! and a block-bearing one captures its block with the share deferred. The
//! real transaction is pinned against PostgreSQL in `commit_reconcile_tests`.
use super::stale_causes::{assert_stale_wire, stale_causes};
use super::*;
use tokio::task::JoinHandle;
use tracing::instrument::WithSubscriber;

const MS: fn(u64) -> Duration = Duration::from_millis;

/// Tip 1 at revision 0, with share answers bounded at 300 ms plus 300 ms of
/// grace for a COMMIT in flight, capture on (100 bps, the default).
async fn fixture() -> Fixture {
    fixture_with(|_| {}).await
}

async fn fixture_with(tune: impl FnOnce(&mut Config)) -> Fixture {
    let fixture = Fixture::build(
        Duration::from_secs(10),
        |config| {
            config.share_commit_timeout = MS(300);
            config.share_commit_grace = MS(300);
            tune(config);
        },
        None,
    )
    .await;
    fixture.observe(1, true).await;
    fixture
}

type Submitted = JoinHandle<Result<(), StratumError>>;

/// Submit a current-tip share-pass proof issued at revision 0: block-bearing
/// when `block`. Returns the submission, its log and its block hash.
fn submit(fixture: &Fixture, block: bool) -> (Submitted, SharedLog, String) {
    let job = fixture.job(1, 0, "original.worker");
    let mut proof = fixture.proof(&job, 0);
    proof.block_pass = block;
    let block_hash = proof.block_hash_hex.clone();
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
    (submitted, log, block_hash)
}

/// The append paused at `gate`, before its fence, while a settlement moves
/// the payout revision to 1.
async fn move_revision_at_the_fence(fixture: &Fixture, gate: &Gate) {
    gate.entered.notified().await;
    fixture.store.revision.store(1, Ordering::SeqCst);
    gate.release.notify_one();
}

async fn eventually(what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !probe() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(MS(10)).await;
    }
}

/// The issue's reproduction. On the base the fence refused the append, the
/// candidate rolled back with it, and the found block was never offered.
#[tokio::test]
async fn a_block_whose_revision_moves_before_its_append_is_captured_not_dropped() {
    let fixture = fixture().await;
    let gate = Arc::new(Gate::default());
    *fixture.store.append_gate.lock().unwrap() = Some(gate.clone());
    let (submitted, log, block_hash) = submit(&fixture, true);
    move_revision_at_the_fence(&fixture, &gate).await;
    // The answer follows the captured block, which nothing lands here.
    assert_error(
        submitted.await.unwrap().unwrap_err(),
        "ledger-outcome-unknown",
        "share outcome is not yet known",
    );
    assert!(
        fixture.store.records.lock().unwrap().is_empty(),
        "the share was credited at the superseded revision"
    );
    let captures = fixture.store.captures.lock().unwrap();
    assert_eq!(captures.len(), 1, "the found block was not captured");
    let (share, candidate, fence) = &captures[0];
    assert_eq!(candidate.block_hash, block_hash);
    assert_eq!(share.share_id, format!("original.worker:{block_hash}"));
    assert_eq!(candidate.payout_revision, 0, "a capture lands as issued");
    assert_eq!(*fence, 1);
    assert_eq!(fixture.coordinator.accepted.load(Ordering::SeqCst), 0);
    assert!(fixture
        .coordinator
        .metrics
        .render()
        .contains("qbit_prism_block_proof_ack_capped_total{path=\"share\"} 1\n"));
    let text = log.text();
    assert!(
        text.contains("share outcome unknown") && text.contains(&block_hash),
        "{text}"
    );
}

/// The fence's refusal is the submit check's own answer for superseded work:
/// `stale-job` with cause `payout_revision`, counted once (#675). It passed
/// the submit check at revision 0, so the cause can only be the fence's. It
/// is not a database failure: no `ledger-confirmation-failed`, which feeds
/// the share-append failure warning, and no persistence WARN.
fn assert_refused_by_the_fence(fixture: &Fixture, error: StratumError, log: &SharedLog) {
    assert_stale_wire(error, "stale job");
    assert_eq!(stale_causes(&fixture.coordinator.metrics), [0., 0., 0., 1.]);
    let text = log.text();
    assert!(!text.contains("share persistence failed"), "{text}");
}

/// With capture off (`PRISM_CAPTURE_OVERPAY_CEILING_BPS=0`) the fence refuses
/// a block-bearing share as it always did, just as the submit check refuses
/// one whose revision had already moved, and with the same answer: nothing
/// is captured. The found block is lost, so a WARN names it.
#[tokio::test]
async fn a_block_whose_revision_moves_with_capture_off_is_refused() {
    let fixture = fixture_with(|config| config.capture_overpay_ceiling_bps = 0).await;
    let gate = Arc::new(Gate::default());
    *fixture.store.append_gate.lock().unwrap() = Some(gate.clone());
    let (submitted, log, block_hash) = submit(&fixture, true);
    move_revision_at_the_fence(&fixture, &gate).await;
    assert_refused_by_the_fence(&fixture, submitted.await.unwrap().unwrap_err(), &log);
    assert!(fixture.store.records.lock().unwrap().is_empty());
    assert!(fixture.store.captures.lock().unwrap().is_empty());
    let text = log.text();
    assert!(
        text.contains("found block refused at the payout-revision fence with capture off")
            && text.contains(&block_hash)
            && text.contains("payout revision changed before share commit: admitted at 0, now 1"),
        "{text}"
    );
}

/// A share without a block keeps the fence's refusal, answered as the submit
/// check answers superseded work, and the fence captures nothing for it. Like
/// the submit check's refusal it is counted, not logged.
#[tokio::test]
async fn a_plain_share_whose_revision_moves_before_its_append_is_still_refused() {
    let fixture = fixture().await;
    let gate = Arc::new(Gate::default());
    *fixture.store.append_gate.lock().unwrap() = Some(gate.clone());
    let (submitted, log, _) = submit(&fixture, false);
    move_revision_at_the_fence(&fixture, &gate).await;
    assert_refused_by_the_fence(&fixture, submitted.await.unwrap().unwrap_err(), &log);
    assert!(fixture.store.records.lock().unwrap().is_empty());
    assert!(fixture.store.captures.lock().unwrap().is_empty());
    assert_eq!(fixture.coordinator.accepted.load(Ordering::SeqCst), 0);
    let text = log.text();
    assert!(!text.contains("payout revision changed"), "{text}");
}

/// A capture whose COMMIT is still in flight at the acknowledgement deadline
/// is answered unknown and followed, never cancelled: the capture is the
/// append's own work, so it commits whether or not anyone still waits.
#[tokio::test]
async fn a_capture_still_committing_at_the_deadline_runs_on_and_commits() {
    let fixture = fixture().await;
    let append = Arc::new(Gate::default());
    let commit = Arc::new(Gate::default());
    *fixture.store.append_gate.lock().unwrap() = Some(append.clone());
    *fixture.store.commit_gate.lock().unwrap() = Some(commit.clone());
    let (submitted, log, block_hash) = submit(&fixture, true);
    move_revision_at_the_fence(&fixture, &append).await;
    commit.entered.notified().await;
    assert_error(
        submitted.await.unwrap().unwrap_err(),
        "ledger-outcome-unknown",
        "share outcome is not yet known",
    );
    let text = log.text();
    assert!(
        text.contains("commit-in-flight") && text.contains(&block_hash),
        "{text}"
    );
    assert!(fixture.store.captures.lock().unwrap().is_empty());
    assert_eq!(fixture.store.cancelled.load(Ordering::SeqCst), 0);
    commit.release.notify_one();
    eventually("the in-flight capture to commit", || {
        fixture.store.captures.lock().unwrap().len() == 1
    })
    .await;
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

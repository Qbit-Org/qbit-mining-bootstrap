//! #478: work a same-parent payout replacement retired is block-only. The
//! fence sits in `submit_share` before any grace branch, so each credit path
//! refuses it: stale grace, parent grace, ordinary current-revision credit and
//! (pinned against PostgreSQL in `tests/b478_stale_revision_block.rs`) the
//! deferred share of its captured block. Each case has a credit-kind control.
use super::stale_causes::{assert_stale_wire, stale_causes};
use super::*;
use crate::codec::JobKind;

fn block_only(mut job: MiningJob<JobContext>) -> MiningJob<JobContext> {
    job.wire.kind = JobKind::BlockOnly;
    job
}

/// Stale grace: after a flip whose tip builds on the job's parent, credit work
/// is grace-credited; block-only work is refused before the grace branch.
#[tokio::test]
async fn block_only_work_earns_no_stale_grace_after_a_flip() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let metrics = fixture.coordinator.metrics.clone();
    let credit = fixture.job(1, 0, "original.worker");
    let retired = block_only(fixture.job(1, 0, "original.worker"));
    fixture.observe(1, true).await;
    fixture.observe(2, true).await;
    for grace in [true, false] {
        assert_stale_wire(
            fixture.submit(&retired, grace).await.unwrap_err(),
            "stale job",
        );
    }
    assert_eq!(stale_causes(&metrics), [0., 0., 2., 0.]);
    assert!(fixture.store.records.lock().unwrap().is_empty());
    assert!(metrics
        .render()
        .contains("\nqbit_prism_grace_credited_shares_total 0\n"));
    // Control: the same proof on credit work is grace-credited.
    fixture.submit(&credit, true).await.unwrap();
    let records = fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0.credit_policy.as_deref(), Some("stale-grace"));
}

/// Parent grace: a flip past the job's parent is refused either way, and the
/// block-only refusal never reads the tip parent the grace branch would.
#[tokio::test]
async fn block_only_work_is_refused_parent_grace_without_reading_the_tip_parent() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let metrics = fixture.coordinator.metrics.clone();
    fixture.observe(1, true).await;
    fixture.observe(3, false).await;
    let retired = block_only(fixture.job(1, 0, "original.worker"));
    let header_reads = |fixture: &Fixture| {
        fixture
            .node
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|method| *method == "getblockheader")
            .count()
    };
    let before = header_reads(&fixture);
    assert_stale_wire(
        fixture.submit(&retired, true).await.unwrap_err(),
        "stale job",
    );
    assert_eq!(
        header_reads(&fixture),
        before,
        "the block-only fence precedes the grace branch's parent read"
    );
    // Control: credit work takes the grace branch, reads the parent and is
    // refused there.
    let credit = fixture.job(1, 0, "original.worker");
    assert_stale_wire(
        fixture.submit(&credit, true).await.unwrap_err(),
        "stale job",
    );
    assert_eq!(header_reads(&fixture), before + 1);
    assert_eq!(stale_causes(&metrics), [0., 0., 2., 0.]);
    assert!(fixture.store.records.lock().unwrap().is_empty());
}

/// Ordinary credit: a plain share on the current parent at the current
/// revision credits credit work, but never block-only work.
#[tokio::test]
async fn block_only_work_earns_no_share_credit_at_the_current_revision() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    let metrics = fixture.coordinator.metrics.clone();
    fixture.observe(1, true).await;
    let credit = fixture.job(1, 0, "original.worker");
    let retired = block_only(credit.clone());
    let mut share_only = fixture.proof(&credit, 0);
    share_only.block_pass = false;
    for grace in [false, true] {
        assert_stale_wire(
            fixture
                .coordinator
                .submit(
                    &retired.context.worker,
                    &retired,
                    share_only.clone(),
                    grace.into(),
                )
                .await
                .unwrap_err(),
            "stale job",
        );
    }
    assert_eq!(stale_causes(&metrics), [0., 0., 0., 2.]);
    assert!(fixture.store.records.lock().unwrap().is_empty());
    // Control: the same proof on credit work is credited normally.
    fixture
        .coordinator
        .submit(&credit.context.worker, &credit, share_only, false.into())
        .await
        .unwrap();
    let records = fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0.credit_policy, None);
}

/// A block on block-only work takes the block-only path even when its revision
/// is current, so its share is never credited through the share append. (The
/// fixture's ledger is offline, so that path fails before any write.)
#[tokio::test]
async fn block_only_work_block_never_takes_the_credited_share_path() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.observe(1, true).await;
    let credit = fixture.job(1, 0, "original.worker");
    let retired = block_only(credit.clone());
    // Control first: the same block on credit work is credited with its
    // candidate through the share append.
    fixture.submit(&credit, false).await.unwrap();
    {
        let records = fixture.store.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].1.is_some());
    }
    assert_error(
        fixture.submit(&retired, false).await.unwrap_err(),
        "ledger-confirmation-failed",
        "share was not confirmed by the database",
    );
    assert_eq!(
        fixture.store.records.lock().unwrap().len(),
        1,
        "block-only work never reaches the share append"
    );
}

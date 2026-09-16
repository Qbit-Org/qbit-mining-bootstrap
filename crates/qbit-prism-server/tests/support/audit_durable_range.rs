//! Durable window boundary contracts, sharing the audit fixture in the parent.

use super::*;
use acquire_metrics::counts;
use qbit_prism_server::metrics::Metrics;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// The durable-range proof
// ---------------------------------------------------------------------------

/// Shares in the proof's window: two full pages of `VERIFY_PAGE_ROWS` (4096,
/// `ledger/audit.rs`) and a partial third, so the page arithmetic runs at a
/// full boundary and at the final short page. The smallest divisor of the
/// fixture's window weight above two pages.
pub(super) const PROOF_WINDOW_SHARES: u64 = 10_000;
/// `VERIFY_PAGE_ROWS`, the page the proof reads; the crate does not export it.
const PROOF_PAGE_ROWS: usize = 4096;
const _: () = assert!(PROOF_WINDOW_SHARES as usize > 2 * PROOF_PAGE_ROWS);
/// What the proof fails a landing with. Asserted by name: a landing refused
/// for any other reason, such as the audit signature check that runs first,
/// is not the proof at work.
const DIFFERS_FROM_HISTORY: &str = "audit share snapshot differs from canonical database history";

// Return failures through the fixture's Result path so db.close still runs.
fn check_counts(metrics: &Metrics, expected: (f64, f64)) -> Result<()> {
    let actual = counts(metrics);
    ensure!(
        actual == expected,
        "pool checkout counts: expected {expected:?}, got {actual:?}"
    );
    Ok(())
}

/// A signed candidate over `window` for `snapshot`'s anchor, balances and
/// revision. The bundle is built and signed over exactly this window, so it
/// is internally consistent whatever the window holds: the audit signature
/// check accepts it, and only the ledger can disagree with it.
pub(super) fn signed_candidate(
    window: Vec<AcceptedShare>,
    snapshot: &Snapshot,
    plan: &WindowPlan,
    nonce: u32,
) -> Result<TestCandidate> {
    let (coinbase_key, ledger_key) = keys();
    let bundle = build_audit_bundle(
        window,
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 5_000_000_000,
            network_difficulty: plan.window_network_difficulty(),
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        &coinbase_key,
        &ledger_key,
    )?;
    let reference = window_ref_for(&bundle.shares, snapshot)?;
    candidate_with_bundle(bundle, reference, snapshot.payout_revision, nonce)
}

/// Enqueue `candidate` and claim it back through the outbox.
pub(super) async fn claim_enqueued(
    ledger: &Ledger,
    candidate: TestCandidate,
) -> Result<CandidateClaim> {
    let hash = candidate.candidate.block_hash.clone();
    ledger
        .enqueue_candidate(candidate.candidate.clone())
        .await?;
    let claim = candidate.claim(
        ledger
            .claim_candidate(60)
            .await?
            .context("no pending candidate to claim")?,
    );
    ensure!(
        claim.candidate.block_hash == hash,
        "claimed another candidate than the one just enqueued"
    );
    Ok(claim)
}

/// Whether a landing wrote the block or its audit row.
async fn wrote_block_or_audit_row(pool: &PgPool, hash: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_hash=$1) \
         OR EXISTS(SELECT 1 FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?)
}

/// The durable-range proof (`verify_durable_range` in `ledger/audit.rs`)
/// runs before the settlement lock and compares the ledger's anchored range
/// with the candidate window one page at a time. The share ledger is
/// immutable by trigger, so
/// every disagreement here is built on the claim side: each window is signed
/// over as it is, the audit signature check that precedes the proof accepts
/// it, and the proof is what refuses it, by name. The window is three pages:
///
/// - one field of the first share of the second page differs, so the second
///   page is compared at the right positions, not only the first page;
/// - one field of the last share of the final partial page differs, so the
///   short page is compared too;
/// - the window has a gap in the final page, so the durable page outruns the
///   window slice and the bounded slice refuses it;
/// - the window reaches past its anchor with a share the ledger accepted
///   after it, so every page matches, the loop runs out of durable rows, and
///   the final count refuses it.
///
/// A refused landing writes nothing. The unaltered window then lands and
/// serves identical bytes, so the same fixture proves the loop accepts a
/// correct multi-page window and the refusals are not trivially green.
#[tokio::test]
async fn durable_range_proof_refuses_a_window_that_differs_from_ledger_history_on_any_page(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("durable-range").await?;
    let plan = WindowPlan::new(PROOF_WINDOW_SHARES)?;
    plan.load(&ledger.pool, "durable-range").await?;
    let snapshot = ledger.snapshot(plan.window_network_difficulty()).await?;
    ensure!(
        snapshot.shares.len() == usize::try_from(PROOF_WINDOW_SHARES)?,
        "snapshot window is {} shares, expected exactly {PROOF_WINDOW_SHARES}",
        snapshot.shares.len()
    );
    let window = &snapshot.shares;
    // The ledger accepts one more share after the anchor. `append` stamps it
    // past the anchor and returns it as stored, so a window that claims it
    // disagrees with the ledger about nothing but the anchor.
    let beyond = ledger
        .append(plan.share(PROOF_WINDOW_SHARES + 1), None)
        .await?
        .share;
    ensure!(
        beyond.share_seq == PROOF_WINDOW_SHARES + 1 && beyond.accepted_at_ms > snapshot.anchor_ms,
        "the share appended after the snapshot is not past its anchor: {beyond:?}"
    );

    // `ntime` is not part of the counted share, so an altered `ntime` leaves
    // the reward manifest, the signatures and the coinbase unchanged: the
    // full equality over every field is the only thing that can notice it.
    let altered = |index: usize| {
        let mut window = window.clone();
        window[index].ntime += 1;
        window
    };
    let cases = [
        (
            "one field of the first share of the second page",
            altered(PROOF_PAGE_ROWS),
        ),
        (
            "one field of the last share of the final page",
            altered(window.len() - 1),
        ),
        ("a gap in the final page", {
            let mut window = window.clone();
            window.remove(2 * PROOF_PAGE_ROWS + 1);
            window
        }),
        ("a share accepted after the anchor", {
            let mut window = window.clone();
            window.push(beyond);
            window
        }),
    ];
    for (nonce, (what, window)) in (2674u32..).zip(cases) {
        let candidate = signed_candidate(window, &snapshot, &plan, nonce)?;
        let claim = claim_enqueued(&ledger, candidate).await?;
        let error = ledger
            .land_candidate(&claim, &ledger_public_key())
            .await
            .err()
            .with_context(|| {
                format!("{what}: the landing accepted a window the ledger does not hold")
            })?;
        let text = format!("{error:#}");
        ensure!(
            text.contains(DIFFERS_FROM_HISTORY),
            "{what}: refused for another reason than the durable-range proof: {text}"
        );
        ensure!(
            !wrote_block_or_audit_row(&ledger.pool, &claim.candidate.block_hash).await?,
            "{what}: a refused landing wrote the block or its audit row"
        );
        ledger.finish_candidate(&claim, false, Some(what)).await?;
    }

    // Control: the same three pages, unaltered, land and serve.
    let candidate = signed_candidate(window.clone(), &snapshot, &plan, 2680)?;
    let canonical = canonical_audit_bundle_bytes(&candidate.bundle)?;
    let claim = claim_enqueued(&ledger, candidate).await?;
    ledger
        .land_candidate(&claim, &ledger_public_key())
        .await
        .context("the unaltered multi-page window was refused")?;
    let hash = &claim.candidate.block_hash;
    let row = stored_row(&ledger.pool, hash).await?;
    ensure!(
        row.native && !row.top_shares && !row.manifest_shares,
        "landed row is not a normalized native row: {row:?}"
    );
    let served = audit_canonical_bytes(&ledger.pool, hash).await?;
    ensure!(
        served.as_deref() == Some(canonical.as_slice()),
        "served canonical bytes differ from the landed candidate's"
    );
    db.close(vec![ledger]).await
}

/// Rebuild both the signed bundle and its reference from the truncated
/// selection. Agreement with that reference must not certify its boundaries.
async fn refuses_truncated_window(newest: bool) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let metrics = Arc::new(Metrics::default());
    let ledger = acquire_metrics::ledger(&db, &metrics).await?;
    let result = async {
        let plan = WindowPlan::new(PROOF_WINDOW_SHARES)?;
        plan.load(&ledger.pool, "truncated-window").await?;
        let snapshot = ledger.snapshot(plan.window_network_difficulty()).await?;
        ensure!(snapshot.shares.len() == PROOF_WINDOW_SHARES as usize);
        // Later appends must not change the independently checked boundary.
        ledger
            .append(plan.share(PROOF_WINDOW_SHARES + 1), None)
            .await?;
        let mut truncated = snapshot.shares.clone();
        let expected = if newest {
            truncated.pop();
            "audit share snapshot omits newest canonical share"
        } else {
            truncated.remove(0);
            "audit share snapshot omits oldest canonical shares"
        };
        ensure!(truncated.len() > 2 * PROOF_PAGE_ROWS);
        let candidate = signed_candidate(truncated, &snapshot, &plan, 3560)?;
        let claim = claim_enqueued(&ledger, candidate).await?;
        let before = counts(&metrics);
        let error = ledger
            .land_candidate(&claim, &ledger_public_key())
            .await
            .err()
            .context("landing accepted a self-consistent truncated window")?;
        ensure!(
            format!("{error:#}").contains(expected),
            "wrong refusal: {error:#}"
        );
        // Probe + three pages + newest; partial history also checks oldest.
        // A proof refusal follows successful checkouts and never starts BEGIN.
        let acquired = if newest { 5. } else { 6. };
        check_counts(&metrics, (before.0 + acquired, before.1))?;
        ensure!(!wrote_block_or_audit_row(&ledger.pool, &claim.candidate.block_hash).await?);
        let snapshots: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_audit_snapshots")
            .fetch_one(&ledger.pool)
            .await?;
        ensure!(snapshots == 0, "refused landing persisted a snapshot");
        ledger
            .finish_candidate(&claim, false, Some(expected))
            .await?;

        let candidate = signed_candidate(snapshot.shares.clone(), &snapshot, &plan, 3561)?;
        let canonical = canonical_audit_bundle_bytes(&candidate.bundle)?;
        let claim = claim_enqueued(&ledger, candidate).await?;
        let before = counts(&metrics);
        let report = ledger.land_candidate(&claim, &ledger_public_key()).await?;
        // The full crossing window skips oldest: probe + three pages + newest + BEGIN.
        check_counts(&metrics, (before.0 + 6., before.1))?;
        ensure!(report.audit_bundle_sha256_hex == sha256_hex(&canonical));
        let served = audit_canonical_bytes(&ledger.pool, &claim.candidate.block_hash).await?;
        ensure!(served.as_deref() == Some(canonical.as_slice()));
        Ok(())
    }
    .await;
    let cleanup = db.close(vec![ledger]).await;
    result.and(cleanup)
}

#[tokio::test]
async fn durable_range_proof_refuses_newest_truncation_of_a_multi_page_window() -> Result<()> {
    refuses_truncated_window(true).await
}

#[tokio::test]
async fn durable_range_proof_refuses_oldest_truncation_of_a_multi_page_window() -> Result<()> {
    refuses_truncated_window(false).await
}

async fn lands_identically(
    ledger: &Ledger,
    candidate: TestCandidate,
    checkout_counts: Option<(&Metrics, f64)>,
) -> Result<()> {
    let canonical = canonical_audit_bundle_bytes(&candidate.bundle)?;
    let claim = claim_enqueued(ledger, candidate).await?;
    let before = checkout_counts.map(|(metrics, _)| counts(metrics));
    let report = ledger.land_candidate(&claim, &ledger_public_key()).await?;
    if let Some(((metrics, acquired), before)) = checkout_counts.zip(before) {
        check_counts(metrics, (before.0 + acquired, before.1))?;
    }
    ensure!(report.audit_bundle_sha256_hex == sha256_hex(&canonical));
    let served = audit_canonical_bytes(&ledger.pool, &claim.candidate.block_hash).await?;
    ensure!(served.as_deref() == Some(canonical.as_slice()));
    ledger.finish_candidate(&claim, true, None).await?;
    Ok(())
}

#[tokio::test]
async fn durable_range_proof_retains_the_crossing_share_and_refuses_an_extra_prefix() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("crossing-window").await?;
    let result = async {
        let plan = WindowPlan::new(PROOF_WINDOW_SHARES)?;
        plan.load(&ledger.pool, "crossing-window").await?;
        let mut newest = plan.share(PROOF_WINDOW_SHARES + 1);
        newest.share_difficulty = 1_201;
        ledger.append(newest, None).await?;
        let snapshot = ledger.snapshot(plan.window_network_difficulty()).await?;
        ensure!(snapshot.shares.len() == PROOF_WINDOW_SHARES as usize);
        ensure!(snapshot.shares[0].share_seq == 2);
        let weight: u128 = snapshot.shares.iter().map(|s| s.share_difficulty).sum();
        ensure!(weight == window_fixture::WINDOW_WEIGHT + 401);
        let mut extra = vec![plan.share(1)];
        extra.extend(snapshot.shares.clone());
        let candidate = signed_candidate(extra, &snapshot, &plan, 3562)?;
        let claim = claim_enqueued(&ledger, candidate).await?;
        let error = ledger
            .land_candidate(&claim, &ledger_public_key())
            .await
            .err()
            .context("landing accepted an extra oldest share")?;
        ensure!(
            format!("{error:#}")
                .contains("audit share snapshot extends past canonical oldest share"),
            "wrong refusal: {error:#}"
        );
        ensure!(!wrote_block_or_audit_row(&ledger.pool, &claim.candidate.block_hash).await?);
        ledger
            .finish_candidate(&claim, false, Some("extra oldest share"))
            .await?;
        lands_identically(
            &ledger,
            signed_candidate(snapshot.shares.clone(), &snapshot, &plan, 3563)?,
            None,
        )
        .await
    }
    .await;
    let cleanup = db.close(vec![ledger]).await;
    result.and(cleanup)
}

#[tokio::test]
async fn durable_range_proof_accepts_partial_history_and_ignores_ineligible_endpoints() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let metrics = Arc::new(Metrics::default());
    let ledger = acquire_metrics::ledger(&db, &metrics).await?;
    let result = async {
        // Rejected rows precede and follow the real range; the gap also
        // proves boundaries are about eligibility, not sequence adjacency.
        sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,reject_reason,writer_id,writer_epoch)
            SELECT i,'rejected-'||i,'miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(1),1,to_timestamp(1),false,'stale-job','partial-window',0 FROM unnest(ARRAY[1,3,5,9]) AS g(i)")
            .execute(&ledger.pool).await?;
        sqlx::query("SELECT setval(pg_get_serial_sequence('qbit_share_ledger','share_seq'),5)")
            .execute(&ledger.pool).await?;
        let plan = WindowPlan::new(PROOF_WINDOW_SHARES)?;
        ledger.append(plan.share(6), None).await?;
        let snapshot = ledger.snapshot(plan.window_network_difficulty()).await?;
        ensure!(snapshot.shares.len() == 1);
        // A future job on an otherwise eligible accepted row is excluded by
        // the same predicate on both sides of the range.
        sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch)
            SELECT i,'future-job-'||i,'miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(($1+1)::double precision/1000),1,to_timestamp(1),true,'partial-window',0 FROM unnest(ARRAY[2,7]) AS g(i)")
            .bind(snapshot.anchor_ms).execute(&ledger.pool).await?;
        sqlx::query("SELECT setval(pg_get_serial_sequence('qbit_share_ledger','share_seq'),7)")
            .execute(&ledger.pool).await?;
        ledger.append(plan.share(8), None).await?;
        // Probe + one page + newest + oldest + BEGIN, using a single pool slot.
        lands_identically(&ledger, signed_candidate(snapshot.shares.clone(), &snapshot, &plan, 3564)?, Some((&metrics, 5.))).await
    }.await;
    let cleanup = db.close(vec![ledger]).await;
    result.and(cleanup)
}

#[tokio::test]
async fn durable_range_proof_checks_bootstrap_against_the_anchored_ledger() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let metrics = Arc::new(Metrics::default());
    let ledger = acquire_metrics::ledger(&db, &metrics).await?;
    let result = async {
        let plan = WindowPlan::new(PROOF_WINDOW_SHARES)?;
        let empty = ledger.snapshot(plan.window_network_difficulty()).await?;
        ledger.append(plan.share(1), None).await?;
        let bootstrap = |snapshot: &Snapshot, nonce| -> Result<TestCandidate> {
            let mut synthetic = plan.share(1);
            synthetic.share_id = "bootstrap-share".into();
            synthetic.job_id = "bootstrap-job".into();
            synthetic.share_difficulty = plan.window_network_difficulty();
            synthetic.network_difficulty = plan.window_network_difficulty();
            synthetic.job_issued_at_ms = snapshot.anchor_ms;
            synthetic.accepted_at_ms = snapshot.anchor_ms;
            let mut candidate = signed_candidate(vec![synthetic.clone()], snapshot, &plan, nonce)?;
            candidate.candidate.window.shares = None;
            candidate.candidate.bootstrap_share = Some(synthetic);
            Ok(candidate)
        };
        // The first share arrived after issuance of this empty window.
        // Probe + bootstrap EXISTS + BEGIN; no range pages or boundary scans.
        lands_identically(&ledger, bootstrap(&empty, 3565)?, Some((&metrics, 3.))).await?;
        let nonempty = ledger.snapshot(plan.window_network_difficulty()).await?;
        let claim = claim_enqueued(&ledger, bootstrap(&nonempty, 3566)?).await?;
        // Fail the bootstrap query after its checkout, preserving SQLSTATE and
        // returning the sole pool slot before retrying the boundary refusal.
        sqlx::query("ALTER TABLE qbit_share_ledger RENAME COLUMN accepted TO acquire_test_hidden")
            .execute(&ledger.pool)
            .await?;
        let before = counts(&metrics);
        let error = ledger
            .land_candidate(&claim, &ledger_public_key())
            .await
            .err()
            .context("bootstrap query accepted a missing eligibility column")?;
        ensure!(
            error
                .downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|error| error.code())
                .as_deref()
                == Some("42703"),
            "wrong bootstrap SQL error: {error:#}"
        );
        check_counts(&metrics, (before.0 + 2., before.1))?;
        sqlx::query("ALTER TABLE qbit_share_ledger RENAME COLUMN acquire_test_hidden TO accepted")
            .execute(&ledger.pool)
            .await?;
        let before = counts(&metrics);
        let error = ledger
            .land_candidate(&claim, &ledger_public_key())
            .await
            .err()
            .context("landing accepted bootstrap over nonempty history")?;
        ensure!(
            format!("{error:#}").contains("bootstrap audit window omits canonical shares"),
            "wrong refusal: {error:#}"
        );
        check_counts(&metrics, (before.0 + 2., before.1))?;
        ensure!(!wrote_block_or_audit_row(&ledger.pool, &claim.candidate.block_hash).await?);
        Ok(())
    }
    .await;
    let cleanup = db.close(vec![ledger]).await;
    result.and(cleanup)
}

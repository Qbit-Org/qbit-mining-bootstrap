//! Helpers around the existing real Coordinator fixture; no refresh simulation.
use super::runtime::{self, Fixture};
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_prism_server::{coordinator::Prepared, stratum::MiningBackend};
use serde_json::json;
use std::sync::Arc;

/// Explicitly selected ignored probes must never pass by skipping their DB.
/// The reused fixture checks the same input again and may record a duplicate
/// executed manifest line, which the shared gate contract allows.
pub async fn run(
    site: qbit_prism_test_gate::Site,
    body: impl for<'a> FnOnce(&'a Fixture) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    qbit_prism_test_gate::required_database_url(site)?;
    runtime::run(site, body).await
}

pub async fn published(f: &Fixture) -> Result<Arc<Prepared>> {
    f.a.prepared.read().await.clone().context("no publication")
}

/// Change real template transaction bytes while keeping parent, reward, bits,
/// and time identical. This syntactically valid legacy transaction spends a
/// synthetic outpoint: these tests make no node-acceptance claim.
pub fn churn(f: &Fixture, previous: &Prepared, nonce: u32) {
    let mut template = previous.template.clone();
    let transaction = format!(
        "0200000001{}0000000000ffffffff0101000000000000000151{}",
        "12".repeat(32),
        hex::encode(nonce.to_le_bytes())
    );
    template["transactions"] = json!([{"data": transaction}]);
    f.node.set_template(Some(template));
}

pub async fn append_from_other_frontend(f: &Fixture) -> Result<()> {
    let source = published(f).await?;
    let window =
        f.b.ledger
            .read_window(
                &source.window,
                qbit_prism_server::ledger::BalanceSource::AsIssued,
            )
            .await?;
    let mut share = window
        .shares
        .last()
        .cloned()
        .context("seed share missing")?;
    share.share_id = "b274-new-committed-share".into();
    let result = f.b.ledger.append(share, None).await?;
    ensure!(result.inserted, "new-share stimulus was deduplicated");
    ensure!(
        result.share.share_seq == runtime::SHARES + 1,
        "new-share stimulus did not advance the committed watermark to 17"
    );
    Ok(())
}

/// Measure actual returned share rows and snapshot anchor-barrier executions.
/// A metadata/watermark query is not a full window read; failures are errors.
pub fn reads(f: &Fixture, mark: u64, phase: &str) -> Result<(u64, usize)> {
    let rows = f.returned_share_rows(mark)?;
    let snapshots = f
        .proxy
        .executions_since(mark)?
        .iter()
        .filter(|execution| {
            execution
                .sql
                .contains("RETURNING ledger_clock_ms-1 AS anchor_ms,payout_revision")
        })
        .count();
    eprintln!("b274 {phase}: returned_share_rows={rows}, snapshot_anchor_executions={snapshots}");
    Ok((rows, snapshots))
}

pub fn changed_template(before: &Prepared, after: &Prepared) -> Result<()> {
    ensure!(
        before.fingerprint != after.fingerprint,
        "fingerprint did not change"
    );
    ensure!(
        before.storage_key != after.storage_key,
        "new template was not reserved"
    );
    ensure!(
        after.generation > before.generation,
        "new template was not announced"
    );
    let before_wire = before.base_wire.as_ref().context("original wire missing")?;
    let after_wire = after
        .base_wire
        .as_ref()
        .context("replacement wire missing")?;
    ensure!(
        before_wire.transactions != after_wire.transactions,
        "transaction bytes did not change"
    );
    ensure!(
        before_wire.merkle_branch != after_wire.merkle_branch,
        "merkle branch did not change"
    );
    ensure!(
        before_wire.coinb1 != after_wire.coinb1 || before_wire.coinb2 != after_wire.coinb2,
        "coinbase did not change with transaction commitment"
    );
    Ok(())
}

/// Reuse PR397's original-input/native-byte oracle, including AsIssued balances
/// and the canonical audit and manifest functions. Never define audit bytes here.
pub async fn native_artifacts(f: &Fixture) -> Result<()> {
    let worker = f.a.authorize("alice.b274").await?;
    let job = f.issue(&worker, std::time::Duration::from_secs(30)).await?;
    runtime::assertions::compact_storage(f, &job).await
}

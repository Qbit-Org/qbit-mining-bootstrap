//! The one-statement clock+revision read matches the two reads it replaces,
//! against a real PostgreSQL.
use super::work_ledger::WorkLedger;
use crate::ledger::Ledger;
use anyhow::{ensure, Result};
use qbit_prism_test_gate as gate;

#[tokio::test]
async fn clocked_read_matches_the_separate_clock_and_revision_reads() -> Result<()> {
    let Some(url) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let ledger = Ledger::connect(&url, "clocked-revision-test".into(), 2, true).await?;
    let before = WorkLedger::now_ms(&ledger).await?;
    let read = WorkLedger::clocked_payout_revision(&ledger).await?;
    let after = WorkLedger::now_ms(&ledger).await?;
    let revision = WorkLedger::payout_revision(&ledger).await?;
    ensure!(
        before <= read.now_ms && read.now_ms <= after,
        "clock {} outside [{before}, {after}]",
        read.now_ms
    );
    ensure!(
        read.payout_revision == revision,
        "revision {} != {revision}",
        read.payout_revision
    );
    ledger.pool.close().await;
    Ok(())
}

//! Assertions for real refresh/resume measurements, not a measurement source.
//! Callers must isolate the refresh and pass observed values; missing values
//! deliberately fail qualification instead of becoming zero.

use anyhow::{ensure, Context, Result};
use qbit_prism_server::ledger::WindowRef;
use serde::Deserialize;
use serde_json::Value;

pub const REGRESSION_SHARES: u64 = 400_000;
pub const HEADROOM_SHARES: u64 = 500_000;
pub const JSONB_LIMIT_BYTES: u64 = 1_000_000;
pub const WAL_LIMIT_BYTES: u64 = 5_000_000;

/// Inspect the entire prepared payload, including nested objects and arrays.
/// Only root `window.shares` may name shares: it must be the existing WindowRef
/// metadata shape (null or ShareRange), never materialized share rows. This is
/// explicitly narrower than #273's literal no-`shares`-key acceptance wording.
pub fn assert_no_materialized_shares(payload: &Value) -> Result<()> {
    inspect_prepared_payload(payload, true)
}

fn inspect_prepared_payload(payload: &Value, at_root: bool) -> Result<()> {
    match payload {
        Value::Object(fields) => {
            ensure!(
                !fields.contains_key("shares"),
                "stored payload contains shares key"
            );
            for (key, value) in fields {
                if at_root && key == "window" {
                    assert_window_metadata(value)?;
                } else {
                    inspect_prepared_payload(value, false)?;
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                inspect_prepared_payload(value, false)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn assert_window_metadata(value: &Value) -> Result<()> {
    // Reuse the actual serializer's integer and lowercase digest rules. Its
    // deserializer ignores unknown fields and defaults missing Option fields;
    // exact round-trip equality rejects both, including hidden share arrays.
    let window = WindowRef::deserialize(value).context("invalid window metadata")?;
    ensure!(
        serde_json::to_value(window)? == *value,
        "window metadata has missing, extra or noncanonical fields"
    );
    if let Some(range) = window.shares {
        // Match the private ShareRange::bounds / CompactPrepared::validate
        // constraints without changing their persisted representation.
        ensure!(
            range.first_share_seq >= 1
                && range.last_share_seq >= range.first_share_seq
                && i64::try_from(range.last_share_seq).is_ok()
                && range.share_count >= 1
                && range.share_count <= range.last_share_seq - range.first_share_seq + 1,
            "invalid window range metadata"
        );
    }
    Ok(())
}

/// `max_jsonb_bytes` is the maximum observed uncompressed refresh write,
/// not pg_column_size of a possibly compressed/TOASTed stored row.
/// `wal_bytes` is the server-wide insert-LSN delta bracketing just refresh.
/// This qualifies a non-cached refresh that writes logged prepared work.
/// Cached no-write reuse must be measured and checked separately.
pub fn assert_refresh_measurements(
    expected_shares: u64,
    published_shares: u64,
    max_jsonb_bytes: Option<u64>,
    wal_bytes: Option<u64>,
) -> Result<()> {
    ensure!(expected_shares > 0, "qualification window must be nonempty");
    ensure!(
        published_shares == expected_shares,
        "published window count differs"
    );
    let jsonb = max_jsonb_bytes.context("refresh JSONB measurement unavailable")?;
    // A non-cached refresh must persist a prepared record.
    ensure!(jsonb > 0, "no refresh JSONB write was observed");
    ensure!(
        jsonb < JSONB_LIMIT_BYTES,
        "refresh JSONB value is not under 1 MB"
    );
    let wal = wal_bytes.context("refresh WAL measurement unavailable")?;
    ensure!(wal > 0, "no WAL was observed for the logged refresh write");
    ensure!(wal < WAL_LIMIT_BYTES, "refresh WAL is not under 5 MB");
    Ok(())
}

/// Compare canonical bytes produced by the real issuing and resuming jobs.
/// Exact equality preserves both their hashes and signed economics; a caller
/// must not substitute a digest of WindowRef or the legacy window encoding.
pub fn assert_canonical_equality(issued: &[u8], resumed: &[u8]) -> Result<()> {
    ensure!(!issued.is_empty(), "issued canonical audit is absent");
    ensure!(
        issued == resumed,
        "resumed canonical audit differs from issued audit"
    );
    Ok(())
}

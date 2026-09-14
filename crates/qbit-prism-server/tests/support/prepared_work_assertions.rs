//! Assertions for real refresh/resume measurements, not a measurement source.
//! Callers must isolate the refresh and pass observed values; missing values
//! deliberately fail qualification instead of becoming zero.

use anyhow::{ensure, Context, Result};
use serde_json::Value;

pub const REGRESSION_SHARES: u64 = 400_000;
pub const HEADROOM_SHARES: u64 = 500_000;
pub const JSONB_LIMIT_BYTES: u64 = 1_000_000;
pub const WAL_LIMIT_BYTES: u64 = 5_000_000;

/// Inspect the entire prepared payload, including nested objects and arrays.
/// A WindowRef's optional `shares` range is metadata, so the compact storage
/// adapter must supply the serialized job payload whose contract forbids that
/// key, not an independently serialized WindowRef.
pub fn assert_no_shares_key(payload: &Value) -> Result<()> {
    match payload {
        Value::Object(fields) => {
            ensure!(
                !fields.contains_key("shares"),
                "stored payload contains shares key"
            );
            for value in fields.values() {
                assert_no_shares_key(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_no_shares_key(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// `max_jsonb_bytes` is the maximum observed uncompressed refresh write,
/// not pg_column_size of a possibly compressed/TOASTed stored row.
/// `wal_bytes` is the server-wide insert-LSN delta bracketing just refresh.
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
    // A successful nonempty refresh must persist a prepared record.
    ensure!(jsonb > 0, "no refresh JSONB write was observed");
    ensure!(
        jsonb < JSONB_LIMIT_BYTES,
        "refresh JSONB value is not under 1 MB"
    );
    let wal = wal_bytes.context("refresh WAL measurement unavailable")?;
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

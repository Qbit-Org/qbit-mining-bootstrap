//! A source backstop for the cluster-row authority contract (#479).
//!
//! Job persistence fences authority with `FOR KEY SHARE` on the cluster row,
//! which a bare non-key `UPDATE` of an authority column passes. Every
//! production `UPDATE qbit_prism_cluster SET` that assigns `payout_revision`,
//! `config_fingerprint` or `fatal_error` must therefore sit in a function that
//! takes the row `FOR UPDATE` first. The behavioural proof drives each writer
//! against a held fence (`tests/support/cohort_fence.rs`); this scan catches a
//! new writer before anyone writes its test.
use std::path::{Path, PathBuf};

const STATEMENT: &str = "UPDATE qbit_prism_cluster SET";
const AUTHORITY: [&str; 3] = ["payout_revision", "config_fingerprint", "fatal_error"];
const LOCKS: [&str; 2] = [
    "lock_cluster_authority(",
    "FROM qbit_prism_cluster WHERE singleton FOR UPDATE",
];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("source directory") {
        let path = entry.expect("source entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

/// The production part of a source file: test files are skipped whole, and
/// an inline `#[cfg(test)] mod … {` ends the scanned text.
fn production(path: &Path, text: &str) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    if name.contains("test") {
        return None;
    }
    let mut kept = String::new();
    let mut lines = text.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim() == "#[cfg(test)]"
            && lines
                .peek()
                .is_some_and(|next| next.trim_start().starts_with("mod ") && next.contains('{'))
        {
            break;
        }
        kept.push_str(line);
        kept.push('\n');
    }
    Some(kept)
}

/// Whether the `SET` list starting at `set` assigns an authority column.
fn assigns_authority(set: &str) -> bool {
    let end = [" WHERE", "\""]
        .iter()
        .filter_map(|stop| set.find(stop))
        .min()
        .unwrap_or(set.len());
    set[..end].split(',').any(|assignment| {
        assignment
            .split('=')
            .next()
            .is_some_and(|column| AUTHORITY.contains(&column.trim()))
    })
}

/// The text of the function enclosing byte `at`: from its `fn` line on.
fn enclosing_function(text: &str, at: usize) -> &str {
    let before = &text[..at];
    let mut start = 0;
    let mut offset = 0;
    for line in before.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if (trimmed.starts_with("fn ")
            || trimmed.starts_with("async fn ")
            || trimmed.starts_with("pub"))
            && trimmed.contains("fn ")
        {
            start = offset;
        }
        offset += line.len();
    }
    &text[start..at]
}

#[test]
fn authority_writers_lock_the_cluster_row_first() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let mut writers = Vec::new();
    let mut bare = Vec::new();
    for path in files {
        let text = std::fs::read_to_string(&path).expect("source file");
        let Some(text) = production(&path, &text) else {
            continue;
        };
        for (at, _) in text.match_indices(STATEMENT) {
            if !assigns_authority(&text[at + STATEMENT.len()..]) {
                continue;
            }
            let line = text[..at].lines().count();
            let site = format!(
                "{}:{line}",
                path.strip_prefix(&root).unwrap_or(&path).display()
            );
            let function = enclosing_function(&text, at);
            if LOCKS.iter().any(|lock| function.contains(lock)) {
                writers.push(site);
            } else {
                bare.push(site);
            }
        }
    }
    assert!(
        bare.is_empty(),
        "a bare UPDATE of a cluster authority column passes a job cohort's FOR KEY SHARE fence; take the row FOR UPDATE first (lock_cluster_authority): {bare:?}"
    );
    // Eight writers exist at #479; fewer means the scan stopped seeing them.
    assert!(
        writers.len() >= 8,
        "the scan found only {} authority writers: {writers:?}",
        writers.len()
    );
}

#[test]
fn the_scan_tells_authority_assignments_from_the_clock() {
    assert!(assigns_authority(
        "payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton"
    ));
    assert!(assigns_authority(
        " best_chainwork=$1,fatal_error=$2 WHERE singleton"
    ));
    assert!(!assigns_authority(
        "ledger_clock_ms=GREATEST(ledger_clock_ms,1) WHERE singleton RETURNING ledger_clock_ms"
    ));
    assert!(!assigns_authority("updated_at=clock_timestamp()\")"));
}

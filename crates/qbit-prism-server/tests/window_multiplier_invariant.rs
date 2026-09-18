//! The payout window's difficulty multiplier has one value,
//! `qbit_prism::PRISM_WINDOW_MULTIPLIER`, but several spellings. SQL bakes it
//! in as a literal in the dashboard query files and in the shipped schema, and
//! a handful of Rust sites embed it in inline SQL or render it into a read
//! model. This target enumerates those literal spellings and pins every one of
//! them to the constant, so changing the multiplier cannot leave a dashboard,
//! the audit window view or the archive horizon scaling by the old value.
//!
//! A multiplier site is the expression that scales a network difficulty into a
//! requested window weight, or the reported multiplier itself. The scanner
//! matches five idioms, each kept narrow so ordinary `* 8` arithmetic is not
//! swept in:
//!
//! * `difficulty-text-bind` — `$N::text::numeric * M`, the shape that passes
//!   the scaled difficulty into `qbit_prism_window(...)` from a query file or
//!   from inline SQL in Rust.
//! * `sql-difficulty-scale` — `[audit_]network_difficulty[::numeric] *
//!   M::numeric`, the requested window weight computed from a column. The
//!   trailing `::numeric` is required, so the bind form above cannot match
//!   twice.
//! * `sql-window-multiplier-column` — `M::numeric AS window_multiplier`, the
//!   value the audit window view reports.
//! * `json-window-multiplier` — `"window_multiplier": M` in a rendered read
//!   model. A site that derives the field from the constant spells no literal
//!   and is correctly invisible here.
//! * `rust-difficulty-scale` — `* Mu8` on a line that mentions a difficulty,
//!   the `BigUint` scaling behind a read model's `requested_window_weight`.
//!
//! `*_revert_*.sql` rollback scripts are excluded: they undo a migration
//! rather than describe the deployed schema. So is
//! `tests/fixtures/schema_2x/001_share_ledger.sql`, a frozen copy of the 2.x
//! schema kept for the migration tests — it must keep whatever multiplier 2.x
//! shipped, and it lives outside the scanned roots for that reason.

use std::path::{Path, PathBuf};

/// One multiplier literal: where it is, which idiom matched, and what it says.
struct Site {
    path: String,
    line: usize,
    idiom: &'static str,
    value: u128,
}

/// Every file and the number of multiplier literals it is expected to spell.
/// A new spelling, or a site the scanner stops recognising, has to show up
/// here — either derive it from `PRISM_WINDOW_MULTIPLIER` instead, or add it.
const EXPECTED_SITES: &[(&str, usize)] = &[
    ("crates/qbit-prism-server/src/api/public.rs", 2),
    (
        "crates/qbit-prism-server/src/api/queries/dashboard_blocks.sql",
        1,
    ),
    (
        "crates/qbit-prism-server/src/api/queries/dashboard_pool_snapshot.sql",
        1,
    ),
    (
        "crates/qbit-prism-server/src/api/queries/dashboard_reward_leaderboard.sql",
        1,
    ),
    ("crates/qbit-prism-server/src/api/read_models.rs", 2),
    ("crates/qbit-prism-server/src/ledger/archive.rs", 2),
    ("crates/qbit-prism/sql/001_share_ledger.sql", 3),
];

/// Reads the decimal integer at the front of `text`, with what follows it.
fn leading_integer(text: &str) -> Option<(u128, &str)> {
    let end = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    Some((text[..end].parse().ok()?, &text[end..]))
}

/// Reads the decimal integer at the end of `text`.
fn trailing_integer(text: &str) -> Option<u128> {
    let start = text
        .char_indices()
        .rev()
        .take_while(|(_, c)| c.is_ascii_digit())
        .last()
        .map(|(index, _)| index)?;
    text[start..].parse().ok()
}

/// Steps over horizontal space, a single `*`, and the space after it.
fn after_times(text: &str) -> Option<&str> {
    let rest = text.trim_start_matches([' ', '\t']).strip_prefix('*')?;
    Some(rest.trim_start_matches([' ', '\t']))
}

fn scan_line(path: &str, number: usize, line: &str, rust: bool, sites: &mut Vec<Site>) {
    let mut push = |idiom, value| {
        sites.push(Site {
            path: path.to_owned(),
            line: number,
            idiom,
            value,
        })
    };

    for (offset, marker) in line.match_indices("::text::numeric") {
        let bind = line[..offset].trim_end_matches(|c: char| c.is_ascii_digit());
        if bind.len() == offset || !bind.ends_with('$') {
            continue;
        }
        if let Some((value, _)) =
            after_times(&line[offset + marker.len()..]).and_then(leading_integer)
        {
            push("difficulty-text-bind", value);
        }
    }

    for (offset, marker) in line.match_indices("network_difficulty") {
        let rest = &line[offset + marker.len()..];
        let rest = rest.strip_prefix("::numeric").unwrap_or(rest);
        let Some((value, tail)) = after_times(rest).and_then(leading_integer) else {
            continue;
        };
        if tail.starts_with("::numeric") {
            push("sql-difficulty-scale", value);
        }
    }

    for (offset, _) in line.match_indices("window_multiplier") {
        let head = line[..offset].trim_end();
        let Some(head) = head
            .strip_suffix("AS")
            .or_else(|| head.strip_suffix("as"))
            .map(str::trim_end)
            .and_then(|head| head.strip_suffix("::numeric"))
        else {
            continue;
        };
        if let Some(value) = trailing_integer(head) {
            push("sql-window-multiplier-column", value);
        }
    }

    for (offset, marker) in line.match_indices("\"window_multiplier\"") {
        let rest = line[offset + marker.len()..].trim_start_matches([' ', '\t']);
        let Some(rest) = rest.strip_prefix(':') else {
            continue;
        };
        if let Some((value, _)) = leading_integer(rest.trim_start_matches([' ', '\t'])) {
            push("json-window-multiplier", value);
        }
    }

    // Rust arithmetic only counts on a line that names a difficulty, which
    // keeps unrelated `* 8` byte and second arithmetic out of the scan.
    if rust && line.contains("difficulty") {
        for (offset, _) in line.match_indices('*') {
            let rest = line[offset + 1..].trim_start_matches([' ', '\t']);
            let Some((value, tail)) = leading_integer(rest) else {
                continue;
            };
            let Some(tail) = tail.strip_prefix("u8") else {
                continue;
            };
            if !tail.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
                push("rust-difficulty-scale", value);
            }
        }
    }
}

/// The server's Rust and SQL sources plus the shipped schema, relative to the
/// workspace root so a failure names a path a reader can open.
fn scanned_sources() -> Vec<(String, PathBuf)> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root is two levels above this crate");
    let mut sources = Vec::new();
    let mut pending = vec![
        workspace.join("crates/qbit-prism-server/src"),
        workspace.join("crates/qbit-prism/sql"),
    ];
    while let Some(directory) = pending.pop() {
        let entries = std::fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("reading {}: {error}", directory.display()));
        for entry in entries {
            let path = entry.expect("a readable directory entry").path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if !(name.ends_with(".rs") || (name.ends_with(".sql") && !name.contains("_revert_"))) {
                continue;
            }
            let relative = path
                .strip_prefix(&workspace)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            sources.push((relative, path.clone()));
        }
    }
    sources.sort();
    sources
}

#[test]
fn every_window_multiplier_literal_matches_the_constant() {
    let mut sites = Vec::new();
    for (relative, path) in scanned_sources() {
        let rust = relative.ends_with(".rs");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("reading {relative}: {error}"));
        for (index, line) in text.lines().enumerate() {
            scan_line(&relative, index + 1, line, rust, &mut sites);
        }
    }
    assert!(
        !sites.is_empty(),
        "the multiplier scanner matched nothing; its idioms have gone stale, \
         see this file's header comment"
    );

    // Report every disagreeing site at once: a multiplier change that missed
    // several spellings should not be fixed one failure at a time.
    let wrong: Vec<String> = sites
        .iter()
        .filter(|site| site.value != qbit_prism::PRISM_WINDOW_MULTIPLIER)
        .map(|site| format!("{}:{}: found {}", site.path, site.line, site.value))
        .collect();
    assert!(
        wrong.is_empty(),
        "every window multiplier literal must equal \
         qbit_prism::PRISM_WINDOW_MULTIPLIER ({}), but {} {}:\n{}",
        qbit_prism::PRISM_WINDOW_MULTIPLIER,
        wrong.len(),
        if wrong.len() == 1 {
            "site disagrees"
        } else {
            "sites disagree"
        },
        wrong.join("\n")
    );

    let mut found: Vec<(&str, usize)> = Vec::new();
    for site in &sites {
        match found.iter_mut().find(|(path, _)| *path == site.path) {
            Some((_, count)) => *count += 1,
            None => found.push((&site.path, 1)),
        }
    }
    found.sort();
    assert_eq!(
        found,
        EXPECTED_SITES.to_vec(),
        "the set of multiplier spellings changed. A new site must either derive \
         its value from qbit_prism::PRISM_WINDOW_MULTIPLIER — the bound-parameter \
         or constant form the rest of the server already uses — or be added to \
         EXPECTED_SITES in this file. A site that vanished means the scanner no \
         longer recognises its idiom; see this file's header comment. \
         Idioms matched: {:?}",
        sites
            .iter()
            .map(|site| format!("{}:{}: {}", site.path, site.line, site.idiom))
            .collect::<Vec<_>>()
    );
}

//! Tying the server binary to the source revision the artifact names.
//!
//! `subject.coordinator_revision` is `git rev-parse HEAD` in the harness's
//! checkout, while the server under test is whatever `--server-bin` points
//! at. A binary left over from an earlier checkout would let the tree be
//! clean and the recorded revision current while the bytes that ran were
//! built from something else, and the artifact would then attribute its
//! measurements to a commit that did not produce them. The binary digest does
//! not help: it identifies the bytes, not the source.
//!
//! The server embeds no build revision, so the revision is established the
//! way Cargo itself decides whether a binary is fresh: from the dep-info file
//! Cargo writes beside it, which lists every workspace source the binary was
//! compiled from. If every listed source belongs to this checkout and none is
//! newer than the binary -- and neither are the lock file and the workspace
//! manifest, which Cargo's list omits -- then the binary is what this tree
//! builds, and with a clean tree that is HEAD. Otherwise the revision cannot
//! be established and the harness says so instead of guessing.

use anyhow::{ensure, Context, Result};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

/// The source file that proves the dep-info belongs to the server crate in
/// this checkout and not to a binary built elsewhere.
pub const SERVER_CRATE_ROOT: &str = "crates/qbit-prism-server/src/main.rs";

/// Inputs Cargo's dep-info does not list but which change what the tree
/// builds.
pub const WORKSPACE_INPUTS: &[&str] = &["Cargo.lock", "Cargo.toml"];

/// Whether the binary can be tied to the checkout's HEAD, and how.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RevisionEvidence {
    /// Every source Cargo recorded for the binary is in this checkout and no
    /// newer than the binary, so the binary is what this tree builds.
    Established {
        dep_info: String,
        sources_checked: usize,
        built_at: String,
    },
    /// The binary cannot be tied to this checkout's HEAD.
    Unestablished { reason: String },
}

impl RevisionEvidence {
    pub fn is_established(&self) -> bool {
        matches!(self, Self::Established { .. })
    }
}

/// Cargo's dep-info file for a binary: the same path with a `.d` extension.
pub fn dep_info_path(binary: &Path) -> PathBuf {
    binary.with_extension("d")
}

/// The prerequisites of the first rule in a Makefile-style dep-info file.
/// Cargo writes one rule, `binary: source source ...`, with spaces inside a
/// path escaped as `\ `.
pub fn parse_dep_info(text: &str) -> Result<Vec<PathBuf>> {
    let rule = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .context("the dep-info file is empty")?;
    let (_, prerequisites) = rule
        .split_once(": ")
        .or_else(|| rule.strip_suffix(':').map(|target| (target, "")))
        .context("the dep-info file has no `target: sources` rule")?;
    let mut sources = Vec::new();
    let mut current = String::new();
    let mut chars = prerequisites.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' if chars.peek() == Some(&' ') => {
                current.push(' ');
                chars.next();
            }
            ' ' => {
                if !current.is_empty() {
                    sources.push(PathBuf::from(std::mem::take(&mut current)));
                }
            }
            other => current.push(other),
        }
    }
    if !current.is_empty() {
        sources.push(PathBuf::from(current));
    }
    Ok(sources)
}

fn modified(path: &Path) -> Result<SystemTime> {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .with_context(|| format!("reading the modification time of {}", path.display()))
}

/// Establish whether `binary` is what `repo_root`'s tree builds.
pub fn server_revision_evidence(binary: &Path, repo_root: &Path) -> RevisionEvidence {
    match check(binary, repo_root) {
        Ok(evidence) => evidence,
        Err(error) => RevisionEvidence::Unestablished {
            reason: format!("{error:#}"),
        },
    }
}

fn check(binary: &Path, repo_root: &Path) -> Result<RevisionEvidence> {
    let binary =
        std::fs::canonicalize(binary).with_context(|| format!("resolving {}", binary.display()))?;
    let repo_root = std::fs::canonicalize(repo_root)
        .with_context(|| format!("resolving {}", repo_root.display()))?;
    let built_at = modified(&binary)?;
    let dep_info = dep_info_path(&binary);
    let text = std::fs::read_to_string(&dep_info).with_context(|| {
        format!(
            "{} has no Cargo dep-info file beside it ({}), so nothing records which sources \
             it was built from; a copied or installed binary cannot be tied to a revision",
            binary.display(),
            dep_info.display()
        )
    })?;
    let listed =
        parse_dep_info(&text).with_context(|| format!("parsing {}", dep_info.display()))?;
    ensure!(
        !listed.is_empty(),
        "{} lists no sources",
        dep_info.display()
    );
    // Cargo writes the paths it was invoked with, which are not canonical: on
    // macOS a checkout under /tmp or /var is really /private/tmp or /private/var,
    // and a symlinked home or work directory does the same on any platform.
    // `repo_root` above is canonical, so comparing the two verbatim would refuse
    // a binary that was in fact built from this checkout. A path that cannot be
    // resolved is kept as written: it is about to be reported as missing, and the
    // message should name what the file actually said.
    let sources: Vec<PathBuf> = listed
        .into_iter()
        .map(|source| std::fs::canonicalize(&source).unwrap_or(source))
        .collect();
    let crate_root = repo_root.join(SERVER_CRATE_ROOT);
    ensure!(
        sources.contains(&crate_root),
        "{} does not list {}, so the binary was built from a different checkout than the one \
         whose HEAD the artifact would name",
        dep_info.display(),
        crate_root.display()
    );
    let inputs: Vec<PathBuf> = sources
        .iter()
        .cloned()
        .chain(WORKSPACE_INPUTS.iter().map(|input| repo_root.join(input)))
        .collect();
    let mut checked = 0usize;
    for source in &inputs {
        let modified = modified(source).with_context(|| {
            format!(
                "{} was an input to the binary but is not in the tree any more",
                source.display()
            )
        })?;
        ensure!(
            modified <= built_at,
            "{} is newer than the binary, so the binary was not built from the tree as it is \
             now; rebuild it",
            source.display()
        );
        checked += 1;
    }
    Ok(RevisionEvidence::Established {
        dep_info: dep_info.display().to_string(),
        sources_checked: checked,
        built_at: chrono::DateTime::<chrono::Utc>::from(built_at).to_rfc3339(),
    })
}

//! Host programs the live scenarios drive: finding one and running it to
//! completion (#575). Self-contained (std and `anyhow`), so any test binary
//! can include it with `#[path]`.
use anyhow::{ensure, Context, Result};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

/// A program on `PATH`, or in the system directories `mkfs` lives in.
pub(crate) fn program(name: &str) -> Result<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .chain(["/usr/sbin", "/sbin"].map(PathBuf::from))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .with_context(|| format!("{name} not found on PATH"))
}

pub(crate) fn run(program: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new(program).args(args).output()?;
    ensure!(
        output.status.success(),
        "{} {args:?} failed: {} {}",
        program.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

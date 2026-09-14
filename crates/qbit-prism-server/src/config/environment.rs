//! Names supported by the native process, including conditional settings.
//! The shared inventory also drives the CI retired-setting documentation guard.
use super::*;
use std::collections::BTreeSet;

pub fn check_environment() -> Result<()> {
    let known: BTreeSet<&str> = include_str!("native-settings.txt")
        .lines()
        .filter(|line| line.starts_with("PRISM_"))
        .collect();
    let unread: BTreeSet<String> = env::vars_os()
        .map(|(name, _)| name.to_string_lossy().into_owned())
        .filter(|name| name.starts_with("PRISM_") && !known.contains(name.as_str()))
        .collect();
    if !unread.is_empty() {
        let diagnostic = format!(
            "set but unread by native PRISM: {}",
            unread.into_iter().collect::<Vec<_>>().join(", ")
        );
        if production_mode()? {
            bail!("{diagnostic}");
        }
        eprintln!("warning: {diagnostic}");
    }
    Ok(())
}

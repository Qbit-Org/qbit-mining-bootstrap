use super::*;
use std::{ffi::OsString, io::Read, path::Path};

/// Called only from tools::prepare, before the executable creates any threads.
/// Restore the original environment even when target validation fails.
pub(crate) fn transition_configs(path: &Path) -> Result<(Config, Config)> {
    let current = Config::from_env().context("invalid current policy configuration")?;
    let file =
        std::fs::File::open(path).map_err(|_| anyhow::anyhow!("cannot open --to env file"))?;
    let mut bytes = Vec::new();
    file.take(1_048_577)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow::anyhow!("cannot read --to env file"))?;
    ensure!(bytes.len() <= 1_048_576, "--to env file exceeds 1 MiB");
    // dotenv errors contain source lines, which can contain signing seeds.
    let entries = dotenvy::from_read_iter(bytes.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| anyhow::anyhow!("invalid --to env file syntax"))?;
    struct Restore(Vec<(String, Option<OsString>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (key, value) in self.0.iter().rev() {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }
    let mut restore = Restore(Vec::new());
    for (key, value) in entries {
        // A stack env file may contain unrelated services' settings. Only
        // PRISM and qbit settings reach this configuration reader.
        if key.starts_with("PRISM_") || key.starts_with("QBIT_") {
            ensure!(!value.contains('\0'), "invalid --to env value");
            restore.0.push((key.clone(), env::var_os(&key)));
            env::set_var(key, value);
        }
    }
    let target = Config::from_env().context("invalid target policy configuration")?;
    ensure!(
        current.database_url == target.database_url,
        "policy-transition cannot change PRISM_DATABASE_URL"
    );
    Ok((current, target))
}

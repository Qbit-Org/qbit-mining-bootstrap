//! Configuration for commands that access stored data without signing work.
use super::*;

pub struct DatabaseConfig {
    pub database_url: String,
    pub instance_id: String,
    pub database_connections: u32,
}

impl DatabaseConfig {
    pub fn from_env() -> Result<Self> {
        let database_url = optional("PRISM_DATABASE_URL").context(
            "PRISM_DATABASE_URL is required (Rust PRISM uses PostgreSQL for every instance)",
        )?;
        let parsed = url::Url::parse(&database_url).context("invalid PRISM_DATABASE_URL")?;
        ensure!(
            matches!(parsed.scheme(), "postgres" | "postgresql"),
            "PRISM_DATABASE_URL must use postgres or postgresql"
        );
        ensure!(
            !production_mode()? || !database_url.contains("change-this"),
            "production requires non-default database credentials"
        );
        let instance_id =
            optional("PRISM_INSTANCE_ID").unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        ensure!(
            instance_id.len() <= 128
                && !instance_id.chars().any(char::is_control)
                && !instance_id.starts_with("prepared:"),
            "PRISM_INSTANCE_ID must be at most 128 characters, without controls or the reserved prepared: prefix"
        );
        Ok(Self {
            database_url,
            instance_id,
            database_connections: bounded_usize("PRISM_DATABASE_MAX_CONNECTIONS", 16, 4, 1024)?
                as u32,
        })
    }

    /// Verification of imported artifacts needs a public trust pin, never a seed.
    pub fn ledger_public_key() -> Result<String> {
        let name = "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX";
        let key = optional(name).with_context(|| format!("{name} is required"))?;
        ensure!(
            key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "{name} must be exactly 64 hexadecimal characters"
        );
        Ok(key.to_ascii_lowercase())
    }
}

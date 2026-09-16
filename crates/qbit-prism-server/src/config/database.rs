//! Configuration for commands that access stored data without signing work.
use super::*;
use sqlx::{postgres::PgConnectOptions, ConnectOptions};

fn non_default_credentials(value: &str) -> Result<()> {
    ensure!(
        !value.contains("change-this"),
        "production requires non-default database credentials"
    );
    Ok(())
}

/// Parse the public process's effective reader DSN using SQLx's own credential
/// precedence (query/URI password, PGPASSWORD, then its native password file).
/// This is configuration validation only: PostgreSQL decides which auth method
/// is required, and connection/authentication failures remain readiness errors.
pub fn public_database_options_from_env() -> Result<PgConnectOptions> {
    let database = optional("PRISM_DATABASE_URL")
        .context("PRISM_DATABASE_URL is required by the public service")?;
    let url = url::Url::parse(&database)
        .map_err(|_| anyhow::anyhow!("invalid public PRISM_DATABASE_URL"))?;
    ensure!(
        matches!(url.scheme(), "postgres" | "postgresql"),
        "public PRISM_DATABASE_URL must use postgres or postgresql"
    );
    // SQLx logs unknown query parameters with their values. Parse synchronously
    // under a silent subscriber, and never retain a value-bearing parser error.
    let options =
        tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
            PgConnectOptions::from_url(&url)
        })
        .map_err(|_| anyhow::anyhow!("invalid public PRISM_DATABASE_URL connection options"))?;
    if production_mode()? {
        // Preserve DatabaseConfig's existing marker policy. Extend it to the
        // effective password so percent encoding and a separate reader carrier
        // cannot accidentally retain the shipped default in production.
        non_default_credentials(&database)?;
        // SQLx exposes the resolved password only through ConnectOptions' URL
        // conversion. Normalize non-password fields on a clone: SQLx's URL
        // builder interpolates decoded usernames/hosts without escaping them.
        // The original options, including socket/TLS/auth settings, are returned.
        let credential_url = options
            .clone()
            .username("reader")
            .socket("/prism-config-validation")
            .to_url_lossy();
        if let Some(password) = credential_url.password() {
            non_default_credentials(
                &percent_encoding::percent_decode_str(password).decode_utf8_lossy(),
            )?;
        }
    }
    Ok(options)
}

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
        if production_mode()? {
            non_default_credentials(&database_url)?;
        }
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

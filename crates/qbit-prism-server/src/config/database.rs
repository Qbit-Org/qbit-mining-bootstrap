//! Configuration for commands that access stored data without signing work.
use super::*;
use sqlx::{
    postgres::{PgConnectOptions, PgSslMode},
    ConnectOptions,
};

/// The connect's refusal of `sslmode=verify-ca` without a root certificate
/// (vendor/sqlx-core's `src/net/tls/tls_rustls.rs`, #818).
const VERIFY_CA_WITHOUT_SSLROOTCERT: &str =
    "sslmode=verify-ca requires sslrootcert: verify-ca does not check the server's host name, \
     so it trusts only the root certificates in sslrootcert, never the public roots; set \
     sslrootcert, or use sslmode=verify-full";

fn non_default_credentials(value: &str) -> Result<()> {
    ensure!(
        !value.contains("change-this"),
        "production requires non-default database credentials"
    );
    Ok(())
}

/// `url` as the PostgreSQL client parses it, with the `PG*` environment
/// defaults it applies (`PGSSLMODE`, `PGSSLROOTCERT`, ...). SQLx logs unknown
/// query parameters with their values, so it parses under a silent
/// subscriber, and its error, which can quote a value, is dropped.
pub(super) fn client_options(url: &url::Url) -> Option<PgConnectOptions> {
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        PgConnectOptions::from_url(url)
    })
    .ok()
}

/// `options` through SQLx's own URL conversion, the only view of its
/// resolved password and `sslrootcert`. SQLx's URL builder interpolates
/// decoded usernames and hosts without escaping them, so a clone's are
/// replaced first.
fn resolved_url(options: &PgConnectOptions) -> url::Url {
    options
        .clone()
        .username("reader")
        .socket("/prism-config-validation")
        .to_url_lossy()
}

/// Refuses `sslmode=verify-ca` without a root certificate, by `name` and
/// with the connect's message, since every connect would fail (#825). The
/// mode and certificate are the connect's: the URL's `sslmode` and
/// `sslrootcert` over `PGSSLMODE` and `PGSSLROOTCERT`. An empty
/// `sslrootcert` names no file, so it counts as none.
pub(super) fn verify_ca_has_root_cert(name: &str, options: &PgConnectOptions) -> Result<()> {
    if !matches!(options.get_ssl_mode(), PgSslMode::VerifyCa) {
        return Ok(());
    }
    // SQLx shows a certificate file as `file: <path>`, inline PEM as itself
    // (`CertificateInput`'s Display, in vendor/sqlx-core's src/net/tls/mod.rs).
    let root_cert = resolved_url(options)
        .query_pairs()
        .find_map(|(key, value)| (key == "sslrootcert").then(|| value.into_owned()));
    match root_cert.as_deref() {
        None | Some("file: ") => bail!("{name}: {VERIFY_CA_WITHOUT_SSLROOTCERT}"),
        Some(_) => Ok(()),
    }
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
    let options =
        client_options(&url).context("invalid public PRISM_DATABASE_URL connection options")?;
    verify_ca_has_root_cert("public PRISM_DATABASE_URL", &options)?;
    if production_mode()? {
        // Preserve DatabaseConfig's existing marker policy. Extend it to the
        // effective password so percent encoding and a separate reader carrier
        // cannot accidentally retain the shipped default in production.
        non_default_credentials(&database)?;
        // The original options, including socket/TLS/auth settings, are returned.
        if let Some(password) = resolved_url(&options).password() {
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
        let database_url = Self::url_from_env()?;
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

    /// `PRISM_DATABASE_URL` alone, checked as [`Self::from_env`] checks it,
    /// for an operator command that reads nothing else: a frontend-only
    /// setting left invalid must not stop it (#664).
    pub fn url_from_env() -> Result<String> {
        let database_url = optional("PRISM_DATABASE_URL").context(
            "PRISM_DATABASE_URL is required (Rust PRISM uses PostgreSQL for every instance)",
        )?;
        let parsed = url::Url::parse(&database_url).context("invalid PRISM_DATABASE_URL")?;
        ensure!(
            matches!(parsed.scheme(), "postgres" | "postgresql"),
            "PRISM_DATABASE_URL must use postgres or postgresql"
        );
        // The ledger connects with these options, so a URL it cannot use
        // fails here, by name, never at the first connect.
        let options = client_options(&parsed)
            .context("PRISM_DATABASE_URL is not a connection URL the PostgreSQL client accepts")?;
        verify_ca_has_root_cert("PRISM_DATABASE_URL", &options)?;
        if production_mode()? {
            non_default_credentials(&database_url)?;
        }
        Ok(database_url)
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

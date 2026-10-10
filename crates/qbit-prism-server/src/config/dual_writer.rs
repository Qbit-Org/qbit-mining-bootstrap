//! The 3.1 dual-writer settings: two nodes, each with its own writable
//! PostgreSQL, each pulling the rows the other originated. Off by default.
//! While `PRISM_DUAL_WRITER` is off no other setting here is read, so a
//! single-writer frontend behaves exactly as 3.0 does, whatever the others
//! hold. Validation errors name the setting and never echo a value: the
//! peer DSNs carry the sync role's credentials.
use super::*;
use crate::node_identity::{NodeIdentity, NodeIndex};

/// `PRISM_PEER_SYNC_INTERVAL_MS` when unset: how long the peer sync waits
/// between pulls that found nothing new.
pub const DEFAULT_PEER_SYNC_INTERVAL_MS: u64 = 250;
const PEER_SYNC_INTERVAL_MS: std::ops::RangeInclusive<u64> = 10..=60_000;
/// `PRISM_PEER_SYNC_BATCH_ROWS` when unset: the most rows of one stream one
/// pull reads from the peer and inserts in one local transaction.
pub const DEFAULT_PEER_SYNC_BATCH_ROWS: u32 = 5000;
const PEER_SYNC_BATCH_ROWS: std::ops::RangeInclusive<u32> = 1..=100_000;

/// This node's dual-writer configuration: which node it is, whether it pays
/// down carried balances, and how it reaches the peer's database.
#[derive(Clone, PartialEq, Eq)]
pub struct DualWriterConfig {
    pub identity: NodeIdentity,
    /// `PRISM_PEER_DATABASE_URL`: the peer's PostgreSQL, as its read-only
    /// sync role. A credential: never logged.
    pub peer_database_url: String,
    /// `PRISM_PEER_DATABASE_URL_FALLBACK`: a second network path to the same
    /// database, tried when the first fails.
    pub peer_database_url_fallback: Option<String>,
    pub peer_sync_interval: Duration,
    pub peer_sync_batch_rows: u32,
}

impl std::fmt::Debug for DualWriterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualWriterConfig")
            .field("identity", &self.identity)
            .field("peer_database_url", &"<redacted>")
            .field(
                "peer_database_url_fallback",
                &self
                    .peer_database_url_fallback
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("peer_sync_interval", &self.peer_sync_interval)
            .field("peer_sync_batch_rows", &self.peer_sync_batch_rows)
            .finish()
    }
}

impl DualWriterConfig {
    /// `None` while `PRISM_DUAL_WRITER` is off (the default). `database_url`
    /// is this node's own `PRISM_DATABASE_URL`, which neither peer DSN may
    /// name.
    pub fn from_env(database_url: &str) -> Result<Option<Self>> {
        if !flag("PRISM_DUAL_WRITER", false)? {
            return Ok(None);
        }
        let production = production_mode()?;
        let node = node_index(optional("PRISM_NODE_INDEX"))?;
        let carry_owner = carry_owner(optional("PRISM_CARRY_OWNER"))?;
        let peer_database_url = peer_url(
            "PRISM_PEER_DATABASE_URL",
            optional("PRISM_PEER_DATABASE_URL"),
            database_url,
            production,
        )?
        .context(
            "PRISM_PEER_DATABASE_URL is required when PRISM_DUAL_WRITER is on: the peer's \
             PostgreSQL, as its read-only sync role",
        )?;
        let peer_database_url_fallback = peer_url(
            "PRISM_PEER_DATABASE_URL_FALLBACK",
            optional("PRISM_PEER_DATABASE_URL_FALLBACK"),
            database_url,
            production,
        )?;
        ensure!(
            peer_database_url_fallback.as_deref() != Some(peer_database_url.as_str()),
            "PRISM_PEER_DATABASE_URL_FALLBACK repeats PRISM_PEER_DATABASE_URL; it must name a \
             second network path to the peer, or be unset"
        );
        let interval_ms = number("PRISM_PEER_SYNC_INTERVAL_MS", DEFAULT_PEER_SYNC_INTERVAL_MS)
            .ok()
            .filter(|ms| PEER_SYNC_INTERVAL_MS.contains(ms))
            .with_context(|| {
                format!(
                    "PRISM_PEER_SYNC_INTERVAL_MS must be {}..{} milliseconds",
                    PEER_SYNC_INTERVAL_MS.start(),
                    PEER_SYNC_INTERVAL_MS.end()
                )
            })?;
        let batch_rows = number("PRISM_PEER_SYNC_BATCH_ROWS", DEFAULT_PEER_SYNC_BATCH_ROWS)
            .ok()
            .filter(|rows| PEER_SYNC_BATCH_ROWS.contains(rows))
            .with_context(|| {
                format!(
                    "PRISM_PEER_SYNC_BATCH_ROWS must be {}..{} rows",
                    PEER_SYNC_BATCH_ROWS.start(),
                    PEER_SYNC_BATCH_ROWS.end()
                )
            })?;
        Ok(Some(Self {
            identity: NodeIdentity { node, carry_owner },
            peer_database_url,
            peer_database_url_fallback,
            peer_sync_interval: Duration::from_millis(interval_ms),
            peer_sync_batch_rows: batch_rows,
        }))
    }

    /// The peer DSNs in the order the sync tries them.
    pub fn peer_database_urls(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.peer_database_url.as_str())
            .chain(self.peer_database_url_fallback.as_deref())
    }
}

/// `PRISM_DUAL_WRITER_DOWNGRADE` (boolean, default off; CONTRACT D-12): lets
/// a single-writer frontend start on a database that has run as a
/// dual-writer node. Only the deliberate rollback to one writer sets it.
pub fn dual_writer_downgrade() -> Result<bool> {
    flag("PRISM_DUAL_WRITER_DOWNGRADE", false)
}

fn node_index(raw: Option<String>) -> Result<NodeIndex> {
    let raw = raw.context(
        "PRISM_NODE_INDEX is required when PRISM_DUAL_WRITER is on: 0 on node A, 1 on node B",
    )?;
    raw.trim()
        .parse::<i64>()
        .ok()
        .and_then(NodeIndex::from_index)
        .context("PRISM_NODE_INDEX must be 0 (node A) or 1 (node B)")
}

fn carry_owner(raw: Option<String>) -> Result<bool> {
    match raw.as_deref().map(|raw| raw.trim().to_ascii_lowercase()) {
        None => bail!(
            "PRISM_CARRY_OWNER is required when PRISM_DUAL_WRITER is on: 1 on the one node that \
             pays down carried balances (normally A), 0 on the other"
        ),
        Some(value) => match value.as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => bail!("PRISM_CARRY_OWNER must be a boolean"),
        },
    }
}

/// A peer DSN: a `postgres` URL with a host, never this node's own
/// database, and in production never the shipped placeholder credential.
fn peer_url(
    name: &str,
    raw: Option<String>,
    database_url: &str,
    production: bool,
) -> Result<Option<String>> {
    let Some(raw) = raw else { return Ok(None) };
    let raw = raw.trim().to_owned();
    let parsed = url::Url::parse(&raw).map_err(|_| anyhow::anyhow!("invalid {name}"))?;
    ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql"),
        "{name} must use postgres or postgresql"
    );
    ensure!(
        parsed.host_str().is_some_and(|host| !host.is_empty()),
        "{name} must name the peer's host"
    );
    ensure!(
        !same_database(&parsed, database_url),
        "{name} names this node's own database (PRISM_DATABASE_URL); it must name the peer's"
    );
    if production {
        ensure!(
            !raw.contains("change-this"),
            "production requires non-default credentials in {name}"
        );
    }
    Ok(Some(raw))
}

/// Whether `peer` and `own` name the same host, port and database,
/// whatever the credentials and options.
fn same_database(peer: &url::Url, own: &str) -> bool {
    let Ok(own) = url::Url::parse(own) else {
        return false;
    };
    let name = |url: &url::Url| url.path().trim_start_matches('/').to_owned();
    peer.host_str().map(str::to_ascii_lowercase) == own.host_str().map(str::to_ascii_lowercase)
        && peer.port_or_known_default().unwrap_or(5432)
            == own.port_or_known_default().unwrap_or(5432)
        && name(peer) == name(&own)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWN: &str = "postgresql://prism:secret@db-a:5432/prism";
    const PEER: &str = "postgresql://prism_peer_sync:secret@db-b:5432/prism";

    #[test]
    fn node_index_is_required_and_only_zero_or_one() {
        assert_eq!(node_index(Some("0".into())).unwrap(), NodeIndex::A);
        assert_eq!(node_index(Some(" 1 ".into())).unwrap(), NodeIndex::B);
        let missing = node_index(None).unwrap_err().to_string();
        assert!(
            missing.contains("PRISM_NODE_INDEX is required"),
            "{missing}"
        );
        for bad in ["2", "-1", "a", "B", "0.5", ""] {
            let error = node_index(Some(bad.into())).unwrap_err().to_string();
            assert!(
                error.contains("PRISM_NODE_INDEX must be 0 (node A) or 1 (node B)"),
                "{bad:?}: {error}"
            );
        }
    }

    #[test]
    fn carry_owner_is_a_required_boolean() {
        for yes in ["1", "true", "YES", "on"] {
            assert!(carry_owner(Some(yes.into())).unwrap(), "{yes}");
        }
        for no in ["0", "false", "No", "off"] {
            assert!(!carry_owner(Some(no.into())).unwrap(), "{no}");
        }
        let missing = carry_owner(None).unwrap_err().to_string();
        assert!(
            missing.contains("PRISM_CARRY_OWNER is required"),
            "{missing}"
        );
        let bad = carry_owner(Some("owner".into())).unwrap_err().to_string();
        assert!(bad.contains("PRISM_CARRY_OWNER must be a boolean"), "{bad}");
    }

    #[test]
    fn a_peer_url_names_another_database_and_never_echoes_its_value() {
        let name = "PRISM_PEER_DATABASE_URL";
        assert_eq!(peer_url(name, None, OWN, false).unwrap(), None);
        assert_eq!(
            peer_url(name, Some(PEER.into()), OWN, false).unwrap(),
            Some(PEER.to_owned())
        );
        for (bad, why) in [
            ("not a url secret", "invalid PRISM_PEER_DATABASE_URL"),
            (
                "mysql://user:secret@db-b/prism",
                "must use postgres or postgresql",
            ),
            (
                "postgresql:///prism?password=secret",
                "must name the peer's host",
            ),
            (
                "postgres://other:secret@DB-A/prism?sslmode=require",
                "names this node's own database",
            ),
        ] {
            let error = peer_url(name, Some(bad.into()), OWN, false)
                .unwrap_err()
                .to_string();
            assert!(error.contains(why), "{bad:?}: {error}");
            assert!(error.contains(name), "{bad:?}: {error}");
            assert!(!error.contains("secret"), "a value leaked: {error}");
        }
        // Same host, another database or port: a second cluster on the host.
        for other in [
            "postgresql://u:secret@db-a:5432/prism_b",
            "postgresql://u:secret@db-a:5433/prism",
        ] {
            assert!(peer_url(name, Some(other.into()), OWN, false).is_ok());
        }
        let placeholder = "postgresql://sync:change-this@db-b/prism";
        assert!(peer_url(name, Some(placeholder.into()), OWN, false).is_ok());
        let error = peer_url(name, Some(placeholder.into()), OWN, true)
            .unwrap_err()
            .to_string();
        assert!(error.contains("non-default credentials"), "{error}");
        assert!(!error.contains("change-this"), "{error}");
    }

    #[test]
    fn debug_output_redacts_both_peer_urls() {
        let config = DualWriterConfig {
            identity: NodeIdentity {
                node: NodeIndex::B,
                carry_owner: false,
            },
            peer_database_url: PEER.into(),
            peer_database_url_fallback: Some(PEER.replace("db-b", "db-b-tailnet")),
            peer_sync_interval: Duration::from_millis(250),
            peer_sync_batch_rows: 5000,
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert_eq!(
            config.peer_database_urls().collect::<Vec<_>>(),
            [PEER.to_owned(), PEER.replace("db-b", "db-b-tailnet")]
        );
    }
}

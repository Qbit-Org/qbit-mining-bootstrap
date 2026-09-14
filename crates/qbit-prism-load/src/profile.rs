//! `database-profile.json`: the document whose digest the artifact's
//! `subject.database_profile_sha256` names.
//!
//! The repository defines no schema for it, so the harness writes one and
//! ships it beside the artifact. It is canonical JSON — object keys sorted,
//! no insignificant whitespace — so the digest is a function of the content
//! and not of the serializer.

use anyhow::Result;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

pub const SCHEMA: &str = "qbit.prism.database-profile.v1";

/// Sorted-key, compact JSON. `serde_json::Map` is already a `BTreeMap` in this
/// workspace, but the digest must not depend on that staying true.
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String((*key).clone()).to_string());
                out.push(':');
                write_canonical(&map[*key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

pub fn digest(canonical: &str) -> String {
    hex::encode(Sha256::digest(canonical.as_bytes()))
}

/// Write exactly the bytes the digest was taken over.
///
/// Nothing may be appended, not even a trailing newline. The field exists so a
/// third party can run `sha256sum database-profile.json` against
/// `subject.database_profile_sha256`, and a mismatched digest in an evidence
/// bundle honestly reads as a corrupted or tampered bundle. Digesting the
/// file's bytes instead would make the digest a function of the serializer,
/// which this module exists to avoid.
pub fn write_document(path: &std::path::Path, canonical: &str) -> Result<()> {
    use anyhow::Context;
    std::fs::write(path, canonical.as_bytes())
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// `SHOW ALL`, as a name to setting map plus the units the server reports.
///
/// Every value goes through `redact_secrets_in_text`. A setting is free text
/// PostgreSQL does not mask for a superuser, and some carry credentials:
/// `primary_conninfo` on a promoted standby is a libpq string with
/// `password=`, and `archive_command`, `restore_command` or
/// `ssl_passphrase_command` can embed a URL. The profile is shipped beside
/// the artifact for a third party to verify, so it must not be the file
/// that carries the database password (EP-OBSERVABILITY). A value with no
/// secret in it is written as PostgreSQL reports it.
pub async fn show_all(pool: &PgPool) -> Result<Value> {
    let rows = sqlx::query("SELECT name, setting, COALESCE(unit,'') AS unit FROM pg_settings")
        .fetch_all(pool)
        .await?;
    let mut settings = Map::new();
    for row in rows {
        let name: String = row.try_get("name")?;
        let setting: String = row.try_get("setting")?;
        let unit: String = row.try_get("unit")?;
        let setting = crate::frontend::redact_secrets_in_text(&setting);
        settings.insert(name, json!({"setting": setting, "unit": unit}));
    }
    Ok(Value::Object(settings))
}

/// Build the profile document.
#[allow(clippy::too_many_arguments)]
pub fn build(
    settings: Value,
    server_version: &str,
    replication: Value,
    proxy: Value,
    host: Value,
    frontends: Value,
) -> Value {
    json!({
        "schema": SCHEMA,
        "postgres": {
            "server_version": server_version,
            "settings": settings,
        },
        "replication": replication,
        "connection_path": proxy,
        "host": host,
        "frontends": frontends,
    })
}

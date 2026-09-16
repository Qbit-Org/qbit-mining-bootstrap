use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy};
use qbit_prism_server::api::{self, ApiConfig, ApiState};
use serde_json::Value;
use sqlx::{Connection, PgConnection, PgPool};
use std::{io::Write, path::Path, process::Stdio, sync::Arc};
use tokio::process::Command;
use tower::ServiceExt;

pub struct Database {
    pub admin: PgPool,
    pub pool: PgPool,
    pub schema: String,
    pub url: String,
}

impl Database {
    pub async fn open(raw: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let schema = format!("prism_recovery_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let pool = PgPool::connect(url.as_str()).await?;
        Ok(Self {
            admin,
            pool,
            schema,
            url: url.into(),
        })
    }

    pub async fn close(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

pub fn ledger_key() -> ManifestSigningKey {
    ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap()
}

pub fn share(seq: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: seq,
        share_id: format!("recovery:{seq:064x}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "legacy-job".into(),
        job_issued_at_ms: 1_800_000_000_000,
        accepted_at_ms: 1_800_000_001_000 + seq as i64,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

pub struct Artifact {
    pub block_hash: String,
    pub digest: String,
    pub canonical: Vec<u8>,
}

/// Genuine frozen 2.x DDL, with ordered shares and all three historical body
/// layouts: canonical-sidecar-only, external body, and inline body. The latter
/// two deliberately have no canonical sidecar for the importer to rely on.
pub async fn seed_legacy(pool: &PgPool, root: &Path) -> Result<Vec<Artifact>> {
    sqlx::raw_sql(include_str!("../fixtures/schema_2x/001_share_ledger.sql"))
        .execute(pool)
        .await?;
    sqlx::raw_sql(include_str!(
        "../fixtures/schema_2x/002_candidate_bodies.sql"
    ))
    .execute(pool)
    .await?;
    let shares = vec![share(1), share(5), share(8)];
    for share in &shares {
        sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,accepted_at,ntime,writer_id,writer_epoch) VALUES($1,$2,'miner','miner',decode(repeat('11',32),'hex'),1,100,100,'legacy-job',to_timestamp($3::double precision/1000),to_timestamp($4::double precision/1000),1800000000,'python',7)")
            .bind(share.share_seq as i64).bind(&share.share_id)
            .bind(share.job_issued_at_ms).bind(share.accepted_at_ms).execute(pool).await?;
    }
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',40)")
        .execute(pool)
        .await?;
    let coinbase_key = ManifestSigningKey::from_seed_hex(&"42".repeat(32))?;
    let mut artifacts = Vec::new();
    for index in 0..3 {
        let bundle = qbit_prism::build_audit_bundle(
            shares.clone(),
            FoundBlock {
                block_height: 101 + index,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: 1_800_000_002_000,
            },
            vec![],
            PayoutPolicy::day_one_default(),
            &coinbase_key,
            &ledger_key(),
        )?;
        let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
            &bundle,
            &ledger_key().public_key_hex(),
        )?;
        let canonical = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
        let hash = format!("{:02x}", 80 + index).repeat(32);
        let body_path = root.join(format!("legacy-audit-{hash}.json"));
        let inline = if index == 2 {
            Some(serde_json::to_value(&bundle)?)
        } else {
            None
        };
        if index == 0 {
            let path = root.join(format!(
                "prism-audit-bundle-canonical-{hash}-{}.json.gz",
                report.audit_bundle_sha256_hex
            ));
            let mut encoder = flate2::GzBuilder::new()
                .mtime(0)
                .write(std::fs::File::create(path)?, flate2::Compression::best());
            encoder.write_all(&canonical)?;
            encoder.finish()?;
            ensure!(!body_path.exists(), "sidecar-only row must have no body");
        } else if index == 1 {
            std::fs::write(&body_path, serde_json::to_vec(&bundle)?)?;
        }
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,$2,repeat('00',32),$3,$4,'confirmed')")
            .bind(&hash).bind((101 + index) as i64).bind(&report.coinbase_txid)
            .bind(&report.coinbase_manifest_sha256_hex).execute(pool).await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,body_uri) VALUES($1,$2,$3,$4,$5)")
            .bind(&hash).bind(inline).bind(&report.audit_bundle_sha256_hex).bind(&report.coinbase_tx_hex)
            .bind((index != 2).then(|| body_path.to_string_lossy().into_owned())).execute(pool).await?;
        sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,$2,$3,$3,decode(repeat('11',32),'hex'),1000,0,1000,0,1000,'accrued')")
            .bind((101 + index) as i64).bind(&hash).bind(format!("miner-{index}"))
            .execute(pool).await?;
        artifacts.push(Artifact {
            block_hash: hash,
            digest: report.audit_bundle_sha256_hex,
            canonical,
        });
    }
    Ok(artifacts)
}

/// Stable accounting facts only: native migration metadata and the summary's
/// repair timestamp are intentionally outside the recovery comparison.
pub async fn accounting_state(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(\
          'shares',(SELECT jsonb_agg(to_jsonb(s) ORDER BY share_seq) FROM qbit_share_ledger s),\
          'audits',(SELECT jsonb_agg(jsonb_build_array(block_hash,audit_bundle_sha256) ORDER BY block_hash) FROM qbit_pool_audit_bundles),\
          'ordinals',(SELECT jsonb_agg(jsonb_build_array(block_hash,audit_publication_sequence) ORDER BY audit_publication_sequence) FROM qbit_pool_blocks),\
          'carry',(SELECT jsonb_agg(to_jsonb(c) ORDER BY carry_forward_seq) FROM qbit_payout_carry_forward c),\
          'current',(SELECT jsonb_agg(to_jsonb(c)-'updated_at' ORDER BY miner_id,payout_order_key,p2mr_program) FROM qbit_payout_carry_forward_current c),\
          'drift',(SELECT count(*) FROM qbit_carry_forward_current_drift()))",
    )
    .fetch_one(pool)
    .await?)
}

pub async fn import_cli(db: &Database, root: &Path) -> Result<String> {
    let output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"))
        .args(["import-audits", "--root"])
        .arg(root)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("PRISM_DATABASE_URL", &db.url)
        .env("PRISM_DATABASE_MAX_CONNECTIONS", "4")
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env(
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
            ledger_key().public_key_hex(),
        )
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "import-audits failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

pub async fn assert_artifacts(pool: &PgPool, artifacts: &[Artifact]) -> Result<()> {
    let app = api::router(ApiState::new(
        pool.clone(),
        ApiConfig {
            cache_enabled: false,
            ..Default::default()
        },
        Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ));
    for artifact in artifacts {
        let uri = format!("/public/v1/artifacts/{}", artifact.digest);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(&uri)
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        ensure!(response.status() == axum::http::StatusCode::OK);
        ensure!(response
            .headers()
            .get("x-prism-artifact-canonical-state")
            .is_none());
        let etag = format!("\"{}\"", artifact.digest);
        ensure!(response.headers().get("etag").context("missing ETag")? == etag.as_str());
        ensure!(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await?
                .as_ref()
                == artifact.canonical
        );
        let stored: Vec<u8> = sqlx::query_scalar(
            "SELECT canonical_audit_bytes FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        )
        .bind(&artifact.block_hash)
        .fetch_one(pool)
        .await?;
        ensure!(stored == artifact.canonical);
        let conditional = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(&uri)
                    .header("if-none-match", &etag)
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        ensure!(conditional.status() == axum::http::StatusCode::NOT_MODIFIED);
        ensure!(axum::body::to_bytes(conditional.into_body(), usize::MAX)
            .await?
            .is_empty());
    }
    Ok(())
}

/// Exercise PostgreSQL's real archive writer/reader. INSERT output permits
/// executing the restored SQL through SQLx; only a random test schema name is
/// remapped, so the original database never receives restore statements.
pub async fn backup(db: &Database, pg_bin: &Path) -> Result<Vec<u8>> {
    let output = postgres_command(db, pg_bin, "pg_dump")?
        .args([
            "--format=custom",
            "--inserts",
            "--no-owner",
            "--no-privileges",
            "--schema",
        ])
        .arg(&db.schema)
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "pg_dump failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(
        output.stdout.starts_with(b"PGDMP"),
        "not a PostgreSQL archive"
    );
    Ok(output.stdout)
}

// libpq expands a connection URI supplied as dbname, but not one inherited
// through PGDATABASE. Keep passwords out of the process argument list.
fn postgres_command(db: &Database, pg_bin: &Path, program: &str) -> Result<Command> {
    let mut url = url::Url::parse(&db.url)?;
    let mut password = url
        .password()
        .map(|value| {
            percent_encoding::percent_decode_str(value)
                .decode_utf8()
                .map(String::from)
        })
        .transpose()?;
    url.set_password(None)
        .map_err(|_| anyhow::anyhow!("invalid PostgreSQL connection URL"))?;
    let options: Vec<(String, String)> = url
        .query_pairs()
        .filter_map(|(key, value)| {
            if key == "password" {
                password = Some(value.into_owned());
                None
            } else {
                Some((key.into_owned(), value.into_owned()))
            }
        })
        .collect();
    url.query_pairs_mut().clear().extend_pairs(options);
    let mut command = Command::new(pg_bin.join(program));
    command.arg("--dbname").arg(url.as_str());
    if let Some(password) = password {
        command.env("PGPASSWORD", password);
    }
    Ok(command)
}

pub async fn restore(
    archive: &[u8],
    source: &Database,
    target: &Database,
    pg_bin: &Path,
) -> Result<()> {
    let mut file = tempfile::tempfile()?;
    file.write_all(archive)?;
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))?;
    let output = Command::new(pg_bin.join("pg_restore"))
        .args([
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            "--file=-",
        ])
        .stdin(Stdio::from(file))
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "pg_restore failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Current PostgreSQL patch releases add psql-only safety metacommands.
    // No psql input is evaluated here; retain every actual SQL statement.
    let sql = String::from_utf8(output.stdout)?
        .lines()
        .filter(|line| !line.starts_with("\\restrict ") && !line.starts_with("\\unrestrict "))
        .collect::<Vec<_>>()
        .join("\n")
        .replace(&format!("CREATE SCHEMA {};", source.schema), "")
        .replace(&source.schema, &target.schema);
    // The SQL opens with session SET statements (search_path, lock_timeout,
    // statement_timeout, ...) that survive COMMIT, so it runs on a session of
    // its own that is closed afterwards, never on one that would carry those
    // settings back into `target.pool`. Only the URL, which pins search_path
    // to the fixture schema, is shared with the pool.
    let mut connection = PgConnection::connect(&target.url).await?;
    let restored = async {
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&mut connection)
            .await?;
        ensure!(
            schema == target.schema,
            "restore session resolves {schema:?}, not the target schema {:?}",
            target.schema
        );
        let mut transaction = connection.begin().await?;
        sqlx::raw_sql(&sql).execute(&mut *transaction).await?;
        transaction.commit().await?;
        anyhow::Ok(())
    }
    .await;
    // The session is closed on both paths; `restore_outcome` pins which
    // error is reported when the close fails as well.
    let closed = connection.close().await.map_err(anyhow::Error::from);
    restore_outcome(restored, closed)
}

/// Combines the restore result with the result of closing its session.
///
/// The restore error wins: a failed close must not hide why the restore itself
/// failed. A close failure is reported on its own only when the restore
/// succeeded, and is appended to the restore error's message when both fail
/// so it is not silently lost.
fn restore_outcome(restored: Result<()>, closed: Result<()>) -> Result<()> {
    match (restored, closed) {
        (Ok(()), closed) => closed,
        (Err(restore), Ok(())) => Err(restore),
        (Err(restore), Err(close)) => {
            let message =
                format!("{restore:#}; closing the restore session also failed: {close:#}");
            Err(restore.context(message))
        }
    }
}

#[test]
fn restore_outcome_is_ok_when_restore_and_close_succeed() {
    assert!(restore_outcome(Ok(()), Ok(())).is_ok());
}

#[test]
fn restore_outcome_reports_the_restore_error_when_close_succeeds() {
    let error = restore_outcome(Err(anyhow::anyhow!("restore boom")), Ok(()))
        .expect_err("a failed restore must fail the outcome");
    assert_eq!(error.to_string(), "restore boom");
}

#[test]
fn restore_outcome_reports_the_close_error_when_restore_succeeds() {
    let error = restore_outcome(Ok(()), Err(anyhow::anyhow!("close boom")))
        .expect_err("a failed close must fail the outcome");
    assert_eq!(error.to_string(), "close boom");
}

#[test]
fn restore_outcome_prefers_the_restore_error_when_both_fail() {
    let error = restore_outcome(
        Err(anyhow::anyhow!("restore boom")),
        Err(anyhow::anyhow!("close boom")),
    )
    .expect_err("two failures must fail the outcome");
    let message = error.to_string();
    assert!(
        message.starts_with("restore boom"),
        "the restore error must lead the message: {message:?}"
    );
    assert!(
        message.contains("close boom"),
        "the close failure must not be lost from the message: {message:?}"
    );
    assert_eq!(
        error.root_cause().to_string(),
        "restore boom",
        "the root cause must be the restore error, not the close error"
    );
}

pub async fn evidence(db: &Database, pg_bin: &Path) -> Result<Value> {
    evidence_with_bytea(db, pg_bin, "hex").await
}

pub async fn evidence_with_bytea(db: &Database, pg_bin: &Path, format: &str) -> Result<Value> {
    use std::io::{Seek, SeekFrom};
    ensure!(matches!(format, "hex" | "escape"));
    let mut input = tempfile::tempfile()?;
    writeln!(input, "SET search_path TO {};", db.schema)?;
    writeln!(input, "SET bytea_output TO '{format}';")?;
    input.write_all(include_bytes!(
        "../../../../scripts/prism-recovery-evidence.sql"
    ))?;
    input.seek(SeekFrom::Start(0))?;
    let output = postgres_command(db, pg_bin, "psql")?
        .args(["-XqAt", "-v", "ON_ERROR_STOP=1"])
        .stdin(Stdio::from(input))
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "recovery evidence SQL failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut records = tempfile::NamedTempFile::new()?;
    records.write_all(&output.stdout)?;
    records.flush()?;
    let output = Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/prism-recovery-evidence.py"
        ))
        .arg(records.path())
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "recovery evidence summary failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

use anyhow::{ensure, Result};
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;

#[path = "support/recovery.rs"]
mod recovery;

#[tokio::test]
async fn legacy_ordinal_revert_refuses_native_schema_without_removing_columns() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_revert_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let ledger = Ledger::connect(url.as_str(), "rollback-guard-test".into(), 2, true).await?;
    let result = async {
        let mut conn = ledger.pool.acquire().await?;
        let error = sqlx::raw_sql(include_str!(
            "../../qbit-prism/sql/001_share_ledger_revert_audit_publication_sequence.sql"
        ))
        .execute(&mut *conn)
        .await
        .expect_err("legacy destructive revert must refuse native databases");
        ensure!(error
            .to_string()
            .contains("cannot run against native Prism"));
        let message = error.to_string();
        ensure!(message.contains("one-way migration"));
        ensure!(message.contains("docs/prism-rust-migration.md#recovery-and-rollback"));
        ensure!(
            message.contains("never discard acknowledged shares without accounting reconciliation")
        );
        sqlx::query("ROLLBACK").execute(&mut *conn).await?;
        // Preparing this read proves both the retained publication ordinal and
        // the native reorg column survive the rejected destructive operation.
        sqlx::query(
            "SELECT audit_publication_sequence, inactive_since FROM qbit_pool_blocks LIMIT 1",
        )
        .fetch_optional(&mut *conn)
        .await?;
        let versions: i32 =
            sqlx::query_scalar("SELECT max(version) FROM qbit_prism_schema_migrations")
                .fetch_one(&mut *conn)
                .await?;
        ensure!(versions >= 3);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

#[tokio::test]
async fn frozen_2x_cli_import_restores_all_body_layouts_and_serves_exact_artifacts() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = recovery::Database::open(&raw).await?;
    let result = async {
        let artifacts_dir = tempfile::tempdir()?;
        let artifacts = recovery::seed_legacy(&db.pool, artifacts_dir.path()).await?;
        let before = recovery::accounting_state(&db.pool).await?;
        let ledger = Ledger::connect_operator(&db.url, true).await?;
        ledger.pool.close().await;
        ensure!(recovery::import_cli(&db, artifacts_dir.path())
            .await?
            .contains("Imported 3 audit bodies"));
        // Every original external file is now unavailable, including the
        // sidecar-only body's gzip. Public reads must use shared PostgreSQL.
        artifacts_dir.close()?;
        recovery::assert_artifacts(&db.pool, &artifacts).await?;
        ensure!(recovery::accounting_state(&db.pool).await? == before);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    db.close().await?;
    result
}

#[tokio::test]
async fn frozen_2x_backup_restore_reconciles_before_ack_and_exposes_post_ack_loss() -> Result<()> {
    let Some(inputs) = gate::inputs(
        gate::site!(),
        &[gate::Input::DatabaseUrl, gate::Input::PgBinDir],
    )?
    else {
        return Ok(());
    };
    let raw = &inputs[0];
    let pg_bin = std::path::Path::new(&inputs[1]);
    let source = recovery::Database::open(raw).await?;
    let restored = recovery::Database::open(raw).await?;
    let result = async {
        let artifacts_dir = tempfile::tempdir()?;
        let artifacts = recovery::seed_legacy(&source.pool, artifacts_dir.path()).await?;
        // Broadcast attempts already exist in frozen 2.x. Preserve a nonempty
        // history through backup/restore and migration, alongside dependencies
        // for the native-only obligations exercised below.
        sqlx::raw_sql(
            r#"
            INSERT INTO qbit_ctv_fanout_sets(
                block_hash,manifest_set_json,manifest_set,manifest_set_sha256,
                settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,
                fanout_count,fanout_output_sum_sats,covenant_output_value_sats)
            VALUES(repeat('50',32),'{}','{}',repeat('11',32),'ctv_fanout',
                repeat('22',32),'00',1,1000,1000);
            INSERT INTO qbit_ctv_fanout_artifacts(
                fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,
                manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,
                chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,
                fanout_tx_template_hex,fanout_tx_hex,covenant_output_value_sats,fanout_output_sum_sats)
            VALUES(repeat('33',32),repeat('50',32),repeat('11',32),'{}','{}',
                repeat('44',32),repeat('55',32),repeat('66',32),'00',0,1,
                repeat('22',32),0,'00','00',1000,1000);
            INSERT INTO qbit_ctv_fanout_broadcast_attempts(fanout_txid,attempt_status)
            VALUES(repeat('33',32),'planned');
            INSERT INTO qbit_block_candidate_outbox(block_hash,candidate_sha256,state,completed_at)
            VALUES(repeat('77',32),repeat('88',32),'abandoned',clock_timestamp());
            "#,
        )
        .execute(&source.pool)
        .await?;
        let before = recovery::accounting_state(&source.pool).await?;
        let source_evidence = recovery::evidence(&source, pg_bin).await?;
        ensure!(
            recovery::evidence_with_bytea(&source, pg_bin, "escape").await? == source_evidence,
            "connection bytea defaults changed the recovery fingerprints"
        );
        ensure!(source_evidence["accepted_shares"] == 3);
        ensure!(source_evidence["last_share_seq"] == 8);
        ensure!(source_evidence["records"]["audits"]["count"] == 3);
        ensure!(source_evidence["records"]["ctv_broadcast_attempts"]["count"] == 1);
        for kind in [
            "audit_bodies", "audit_snapshots",
            "ctv_checkpoints", "cpfp_packages", "cpfp_retired_funding", "deferred_shares",
            "fatal_state", "fatal_state_events",
        ] {
            ensure!(source_evidence["records"][kind]["count"] == 0);
        }
        ensure!(source_evidence["carry_forward_integrity"]["mismatch_count"] == 0);
        // Independent 2.x chain calculation over the three fixed seed rows.
        // Comparing two exports alone would miss a consistently wrong head.
        ensure!(
            source_evidence["audit_head_sha256"]
                == "848c67eafac5e3545df6d79ba68994a2fe1d98641ea28f131147e09c464a05f8"
        );
        let archive = recovery::backup(&source, pg_bin).await?;
        let ledger = Ledger::connect_operator(&source.url, true).await?;
        ensure!(recovery::evidence(&source, pg_bin).await? == source_evidence);
        ensure!(recovery::import_cli(&source, artifacts_dir.path())
            .await?
            .contains("Imported 3 audit bodies"));
        recovery::assert_artifacts(&source.pool, &artifacts).await?;
        ensure!(recovery::accounting_state(&source.pool).await? == before);
        ensure!(recovery::evidence(&source, pg_bin).await? == source_evidence);
        assert_canonical_audit_fingerprints(&source, pg_bin, &artifacts, artifacts_dir.path()).await?;
        recovery::restore(&archive, &source, &restored, pg_bin).await?;
        ensure!(recovery::accounting_state(&restored.pool).await? == before);
        ensure!(recovery::evidence(&restored, pg_bin).await? == source_evidence);
        let native_tables_absent: bool =
            sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_migrations') IS NULL")
                .fetch_one(&restored.pool)
                .await?;
        ensure!(
            native_tables_absent,
            "restore must be the isolated pre-migration database"
        );

        // append() returns only after the durable COMMIT: this is the storage
        // acknowledgement boundary used by Stratum, not a fabricated SQL row.
        let mut share = recovery::share(9);
        share.share_seq = 0;
        share.job_issued_at_ms = 1;
        let acknowledged = ledger.append(share, None).await?;
        ensure!(acknowledged.inserted);
        ensure!(
            acknowledged.share.share_seq > 40,
            "migration lost the source sequence high-water mark"
        );
        let after = recovery::evidence(&source, pg_bin).await?;
        ensure!(after["accepted_shares"] == 4);
        ensure!(
            after["records"]["shares"]["sha256"] != source_evidence["records"]["shares"]["sha256"]
        );
        ensure!(after["audit_head_sha256"] == source_evidence["audit_head_sha256"]);
        ensure!(after["records"]["audits"] == source_evidence["records"]["audits"]);
        ensure!(recovery::evidence(&restored, pg_bin).await? == source_evidence);
        let lost_on_restore: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
                .bind(&acknowledged.share.share_id)
                .fetch_one(&restored.pool)
                .await?;
        ensure!(
            lost_on_restore == 0,
            "older restore unexpectedly contains the post-cutover ACK"
        );

        // Each obligation must independently change evidence even when shares,
        // candidates and the exported CTV artifact state remain unchanged.
        let mut prior = after;
        for (kind, insert, update) in [
            (
                "cpfp_packages",
                "INSERT INTO qbit_prism_cpfp_packages(fanout_txid,funding_txid,funding_vout,funding_value_sats,wallet_name) VALUES(repeat('33',32),repeat('99',32),0,1000,'recovery-test')",
                "UPDATE qbit_prism_cpfp_packages SET signed_child_hex='deadbeef',child_txid=repeat('aa',32)",
            ),
            (
                "cpfp_retired_funding",
                "INSERT INTO qbit_prism_cpfp_retired_funding(funding_txid,funding_vout,fanout_txid,funding_value_sats,wallet_name,retirement_reason) VALUES(repeat('bb',32),1,repeat('33',32),1000,'recovery-test','spent')",
                "UPDATE qbit_prism_cpfp_retired_funding SET wallet_lock_released=true",
            ),
            (
                "ctv_broadcast_attempts",
                "INSERT INTO qbit_ctv_fanout_broadcast_attempts(fanout_txid,attempt_status) VALUES(repeat('33',32),'submitted')",
                "UPDATE qbit_ctv_fanout_broadcast_attempts SET submit_result='{\"accepted\":true}' WHERE attempt_status='submitted'",
            ),
            (
                "deferred_shares",
                "INSERT INTO qbit_prism_deferred_shares(block_hash,share,share_sha256) VALUES(repeat('77',32),'{\"miner_id\":\"alice\"}',repeat('cc',32))",
                "UPDATE qbit_prism_deferred_shares SET share='{\"miner_id\":\"bob\"}',share_sha256=repeat('dd',32)",
            ),
        ] {
            for (sql, added) in [(insert, 1), (update, 0)] {
                sqlx::query(sql).execute(&source.pool).await?;
                let current = recovery::evidence(&source, pg_bin).await?;
                ensure!(current["records"][kind]["count"].as_u64()
                    == prior["records"][kind]["count"].as_u64().map(|n| n + added));
                ensure!(current["records"][kind]["sha256"] != prior["records"][kind]["sha256"],
                    "{kind} obligation change was invisible to recovery evidence");
                let mut unchanged = current.clone();
                unchanged["records"][kind] = prior["records"][kind].clone();
                ensure!(unchanged == prior, "unrelated accounting changed with {kind}");
                prior = current;
            }
        }
        ensure!(recovery::evidence(&source, pg_bin).await? == prior);
        ensure!(recovery::evidence(&restored, pg_bin).await? == source_evidence);

        // Checkpoints can change while settlement remains confirmed. Each
        // field alone must create evidence, and resetting it must remove it.
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='confirmed' WHERE fanout_txid=repeat('33',32)")
            .execute(&source.pool).await?;
        prior = recovery::evidence(&source, pg_bin).await?;
        let before_checkpoints = prior.clone();
        ensure!(prior["records"]["ctv_checkpoints"]["count"] == 0);
        for (column, first, next, default) in [
            ("confirmed_block_hash", "repeat('aa',32)", "repeat('bb',32)", "NULL"),
            ("confirmed_block_height", "100", "101", "NULL"),
            ("confirmed_depth", "999", "1000", "0"),
            ("spend_scan_next_height", "0", "1", "NULL"),
            ("spend_scan_anchor_height", "0", "1", "NULL"),
            ("spend_scan_anchor_hash", "repeat('cc',32)", "repeat('dd',32)", "NULL"),
        ] {
            for value in [first, next, default] {
                sqlx::query(&format!(
                    "UPDATE qbit_ctv_fanout_artifacts SET {column}={value} WHERE fanout_txid=repeat('33',32)"
                )).execute(&source.pool).await?;
                let current = recovery::evidence(&source, pg_bin).await?;
                ensure!(current["records"]["ctv_checkpoints"]["count"].as_u64()
                    == Some(u64::from(value != default)));
                ensure!(current["records"]["ctv_checkpoints"]["sha256"]
                    != prior["records"]["ctv_checkpoints"]["sha256"],
                    "CTV checkpoint change was invisible to recovery evidence: {column}={value}");
                let mut unchanged = current.clone();
                unchanged["records"]["ctv_checkpoints"] = prior["records"]["ctv_checkpoints"].clone();
                ensure!(unchanged == prior, "unrelated accounting changed with {column}={value}");
                prior = current;
            }
            ensure!(prior == before_checkpoints);
        }
        // Leave a populated checkpoint for the native backup roundtrip below.
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET confirmed_block_hash=repeat('aa',32),confirmed_block_height=100,confirmed_depth=1000,spend_scan_next_height=1101,spend_scan_anchor_height=1100,spend_scan_anchor_hash=repeat('bb',32) WHERE fanout_txid=repeat('33',32)")
            .execute(&source.pool).await?;
        prior = recovery::evidence(&source, pg_bin).await?;
        ensure!(prior["records"]["ctv_checkpoints"]["count"] == 1);
        let mut unchanged = prior.clone();
        unchanged["records"]["ctv_checkpoints"] = before_checkpoints["records"]["ctv_checkpoints"].clone();
        ensure!(unchanged == before_checkpoints);
        // Acquiring or renewing ownership must not alter durable evidence.
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_token='recovery-claim',claim_instance_id='recovery-owner',claim_expires_at=clock_timestamp()+interval '1 minute' WHERE fanout_txid=repeat('33',32)")
            .execute(&source.pool).await?;
        ensure!(recovery::evidence(&source, pg_bin).await? == prior);

        // A halt can be the only durable change. Routine cluster activity must
        // not count, but every halt and its immutable recovery decision must.
        sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp()")
            .execute(&source.pool).await?;
        ensure!(recovery::evidence(&source, pg_bin).await? == prior);
        let before_halt = prior.clone();
        for mutation in [
            "fatal_error='deep confirmed CTV fanout disconnected: test; manual reconciliation required'",
            "fatal_error_set_at=NULL",
            "fatal_error_set_at='2026-09-14T21:00:00Z'",
            "fatal_error='mature pool block disconnected: test; manual reconciliation required'",
        ] {
            sqlx::query(&format!("UPDATE qbit_prism_cluster SET {mutation}"))
                .execute(&source.pool).await?;
            let current = recovery::evidence(&source, pg_bin).await?;
            ensure!(current["records"]["fatal_state"]["count"] == 1);
            ensure!(current["records"]["fatal_state"]["sha256"]
                != prior["records"]["fatal_state"]["sha256"],
                "fatal state change was invisible to recovery evidence: {mutation}");
            let mut unchanged = current.clone();
            unchanged["records"]["fatal_state"] = prior["records"]["fatal_state"].clone();
            ensure!(unchanged == prior, "unrelated accounting changed with fatal state");
            prior = current;
        }
        // Model the atomic clear-and-audit write, preserving the original halt
        // payload while leaving the cluster in its default nonfatal state.
        let mut tx = source.pool.begin().await?;
        sqlx::query("INSERT INTO qbit_prism_fatal_state_events(reason,fatal_error,fatal_error_set_at,instances,reconciliation) SELECT 'reconciled test halt',fatal_error,fatal_error_set_at,'[]','{\"blocks_checked\":3}' FROM qbit_prism_cluster")
            .execute(&mut *tx).await?;
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL")
            .execute(&mut *tx).await?;
        tx.commit().await?;
        let cleared = recovery::evidence(&source, pg_bin).await?;
        ensure!(cleared["records"]["fatal_state"] == before_halt["records"]["fatal_state"]);
        ensure!(cleared["records"]["fatal_state_events"]["count"] == 1);
        ensure!(cleared["records"]["fatal_state_events"]["sha256"]
            != before_halt["records"]["fatal_state_events"]["sha256"]);
        let mut unchanged = cleared.clone();
        unchanged["records"]["fatal_state_events"] = before_halt["records"]["fatal_state_events"].clone();
        ensure!(unchanged == before_halt, "recovery history must independently distinguish a cleared halt");

        assert_native_audit_payload_fingerprints(&source, pg_bin, &artifacts[0]).await?;
        assert_candidate_payload_fingerprints(raw, pg_bin, &artifacts[0]).await?;
        assert_share_hash_fingerprints(raw, pg_bin).await?;

        // Restore a native database containing both an active halt and a prior
        // clear event; their exact evidence must survive the backup roundtrip.
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='recurring halt'")
            .execute(&source.pool).await?;
        let halted = recovery::evidence(&source, pg_bin).await?;
        let native_restore = recovery::Database::open(raw).await?;
        let restored_result = async {
            let archive = recovery::backup(&source, pg_bin).await?;
            recovery::restore(&archive, &source, &native_restore, pg_bin).await?;
            ensure!(recovery::evidence(&native_restore, pg_bin).await? == halted);
            // pg_restore SQL changes its session's search_path. Verify reads
            // through a fresh pool using the restored schema's connection URL.
            let read_pool = PgPool::connect(&native_restore.url).await?;
            let artifacts_result = async {
                for artifact in &artifacts {
                    ensure!(qbit_prism_server::ledger::audit_canonical_bytes(
                        &read_pool, &artifact.block_hash).await? == Some(artifact.canonical.clone()));
                }
                Ok::<_, anyhow::Error>(())
            }.await;
            read_pool.close().await;
            artifacts_result?;
            Ok::<_, anyhow::Error>(())
        }.await;
        native_restore.close().await?;
        restored_result?;

        // Corrupt payload fields without changing the artifact's identity or
        // settlement status. Both native and frozen 2.x exports must detect
        // each change independently, even though the row count is unchanged.
        for (db, schema) in [(&source, "native"), (&restored, "frozen 2.x")] {
            let mut prior = recovery::evidence(db, pg_bin).await?;
            for mutation in [
                "manifest_set_json='{} '",
                "manifest_set='{\"corrupted\":true}'",
                "settlement_mode='hybrid_coinbase_ctv_fanout'",
                "parent_coinbase_tx_hex='01'",
                "fanout_output_sum_sats=999",
                "covenant_output_value_sats=1001",
            ] {
                sqlx::query(&format!(
                    "UPDATE {}.qbit_ctv_fanout_sets SET {mutation} WHERE block_hash=repeat('50',32)",
                    db.schema
                ))
                .execute(&db.pool)
                .await?;
                let current = recovery::evidence(db, pg_bin).await?;
                ensure!(current["records"]["ctv_sets"]["count"] == 1);
                ensure!(
                    current["records"]["ctv_sets"]["sha256"]
                        != prior["records"]["ctv_sets"]["sha256"],
                    "{schema} CTV manifest-set payload change was invisible to recovery evidence: {mutation}"
                );
                let mut unchanged = current.clone();
                unchanged["records"]["ctv_sets"] = prior["records"]["ctv_sets"].clone();
                ensure!(unchanged == prior, "unrelated accounting changed with {mutation}");
                prior = current;
            }
            for mutation in [
                "manifest_json='{} '",
                "manifest='{\"corrupted\":true}'",
                "manifest_sha256=repeat('aa',32)",
                "precommitment_sha256=repeat('bb',32)",
                "ctv_hash=repeat('cc',32)",
                "commitment_witness_leaf_hex='01'",
                "chunk_count=2",
                "chunk_index=1",
                "parent_coinbase_txid=repeat('dd',32)",
                "parent_coinbase_vout=1",
                "fanout_tx_template_hex='02'",
                "fanout_tx_hex='03'",
                "anchor_vout=0",
                "covenant_output_value_sats=1001",
                "fanout_output_sum_sats=999",
            ] {
                sqlx::query(&format!(
                    "UPDATE {}.qbit_ctv_fanout_artifacts SET {mutation} WHERE fanout_txid=repeat('33',32)",
                    db.schema
                ))
                .execute(&db.pool)
                .await?;
                let current = recovery::evidence(db, pg_bin).await?;
                ensure!(current["records"]["ctv_artifacts"]["count"] == 1);
                ensure!(
                    current["records"]["ctv_artifacts"]["sha256"]
                        != prior["records"]["ctv_artifacts"]["sha256"],
                    "{schema} CTV payload change was invisible to recovery evidence: {mutation}"
                );
                let mut unchanged = current.clone();
                unchanged["records"]["ctv_artifacts"] = prior["records"]["ctv_artifacts"].clone();
                ensure!(unchanged == prior, "unrelated accounting changed with {mutation}");
                prior = current;
            }
        }
        ledger.pool.close().await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    source.close().await?;
    restored.close().await?;
    result
}

async fn assert_canonical_audit_fingerprints(
    source: &recovery::Database,
    pg_bin: &std::path::Path,
    artifacts: &[recovery::Artifact],
    artifacts_dir: &std::path::Path,
) -> Result<()> {
    use qbit_prism_server::ledger::{audit_canonical_bytes, audit_completeness};

    let baseline = recovery::evidence(source, pg_bin).await?;
    let accounting = recovery::accounting_state(&source.pool).await?;
    for artifact in artifacts {
        // Even valid JSON with only extra whitespace violates the published
        // byte digest. Empty and non-UTF-8 bytea must also be fingerprinted.
        let mut changed = artifact.canonical.clone();
        changed.push(b' ');
        for bytes in [changed, Vec::new(), vec![0xff, 0x00, 0x80]] {
            sqlx::query(
                "UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2 WHERE block_hash=$1",
            )
            .bind(&artifact.block_hash)
            .bind(bytes)
            .execute(&source.pool)
            .await?;
            audit_completeness(&source.pool).await?.require_complete()?;
            ensure!(recovery::import_cli(source, artifacts_dir)
                .await?
                .contains("Imported 0 audit bodies"));
            let error = audit_canonical_bytes(&source.pool, &artifact.block_hash)
                .await
                .expect_err("corrupt canonical bytes must fail artifact authentication");
            ensure!(error
                .to_string()
                .contains("stored canonical audit bytes have a digest mismatch"));
            ensure!(recovery::accounting_state(&source.pool).await? == accounting);
            let current = recovery::evidence(source, pg_bin).await?;
            ensure!(
                current["records"]["audits"]["count"] == baseline["records"]["audits"]["count"]
            );
            ensure!(
                current["records"]["audits"]["sha256"] != baseline["records"]["audits"]["sha256"],
                "corrupted stored canonical audit bytes were invisible to recovery evidence"
            );
            ensure!(recovery::evidence_with_bytea(source, pg_bin, "escape").await? == current);
            let mut unchanged = current;
            unchanged["records"]["audits"] = baseline["records"]["audits"].clone();
            ensure!(unchanged == baseline, "unrelated recovery evidence changed");
            sqlx::query(
                "UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2 WHERE block_hash=$1",
            )
            .bind(&artifact.block_hash)
            .bind(&artifact.canonical)
            .execute(&source.pool)
            .await?;
            ensure!(recovery::evidence(source, pg_bin).await? == baseline);
            ensure!(
                audit_canonical_bytes(&source.pool, &artifact.block_hash).await?
                    == Some(artifact.canonical.clone())
            );
        }
    }
    Ok(())
}

async fn assert_share_hash_fingerprints(raw: &str, pg_bin: &std::path::Path) -> Result<()> {
    use sha2::{Digest, Sha256};

    let source = recovery::Database::open(raw).await?;
    let restored = recovery::Database::open(raw).await?;
    let result = async {
        let artifacts_dir = tempfile::tempdir()?;
        recovery::seed_legacy(&source.pool, artifacts_dir.path()).await?;
        // Migration keeps the first accepted identity for a hex suffix,
        // ignoring case, rejected shares and legacy non-hex identifiers.
        sqlx::raw_sql(r#"
            INSERT INTO qbit_share_ledger(
                share_seq,share_id,miner_id,payout_order_key,p2mr_program,
                share_difficulty,network_difficulty,template_height,job_id,
                job_issued_at,ntime,accepted_at,accepted,reject_reason,writer_id,writer_epoch)
            SELECT v.seq,v.id,s.miner_id,s.payout_order_key,s.p2mr_program,
                s.share_difficulty,s.network_difficulty,s.template_height,s.job_id,
                s.job_issued_at,s.ntime,s.accepted_at,v.accepted,
                CASE WHEN v.accepted THEN NULL ELSE 'duplicate-share' END,s.writer_id,s.writer_epoch
            FROM qbit_share_ledger s CROSS JOIN (VALUES
                (2,'rejected-first:'||repeat('AB',32),false),
                (3,'first:'||repeat('AB',32),true),
                (4,'later:'||repeat('ab',32),true),
                (6,'invalid-header',true),
                (7,'rejected-only:'||repeat('cd',32),false)
            ) v(seq,id,accepted) WHERE s.share_seq=1;
        "#).execute(&source.pool).await?;
        let baseline = recovery::evidence(&source, pg_bin).await?;
        let ledger = Ledger::connect_operator(&source.url, true).await?;
        let result = async {
            recovery::import_cli(&source, artifacts_dir.path()).await?;
            ensure!(recovery::evidence(&source, pg_bin).await? == baseline);
            let first: String = sqlx::query_scalar("SELECT share_id FROM qbit_prism_share_hashes WHERE header_hash=repeat('ab',32)")
                .fetch_one(&source.pool).await?;
            ensure!(first == format!("first:{}", "AB".repeat(32)));
            for (mutation, count) in [
                ("DELETE FROM qbit_prism_share_hashes WHERE header_hash=repeat('ab',32)", 3),
                ("UPDATE qbit_prism_share_hashes SET header_hash=repeat('ef',32) WHERE header_hash=repeat('ab',32)", 4),
                ("UPDATE qbit_prism_share_hashes SET share_id='later:'||repeat('ab',32) WHERE header_hash=repeat('ab',32)", 4),
            ] {
                sqlx::query(mutation).execute(&source.pool).await?;
                let current = recovery::evidence(&source, pg_bin).await?;
                ensure!(current != baseline, "share-hash replay protection change was invisible: {mutation}");
                ensure!(current["records"]["share_hashes"]["count"] == count);
                ensure!(current["records"]["share_hashes"]["sha256"] != baseline["records"]["share_hashes"]["sha256"]);
                let mut unchanged = current;
                unchanged["records"]["share_hashes"] = baseline["records"]["share_hashes"].clone();
                ensure!(unchanged == baseline, "unrelated evidence changed: {mutation}");
                sqlx::query("DELETE FROM qbit_prism_share_hashes WHERE header_hash IN (repeat('ab',32),repeat('ef',32))")
                    .execute(&source.pool).await?;
                sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES(repeat('ab',32),$1)")
                    .bind(&first).execute(&source.pool).await?;
                ensure!(recovery::evidence(&source, pg_bin).await? == baseline);
            }
            ensure!(baseline["records"]["share_hashes"]["count"] == 4);
            // Native identifiers without a hex suffix use a SHA-256 fallback,
            // unlike migration's legacy backfill. Export the actual mapping.
            let mut share = recovery::share(9);
            share.share_seq = 0;
            share.share_id = "native-nonhex-id".into();
            share.job_issued_at_ms = 1;
            let expected = hex::encode(Sha256::digest(share.share_id.as_bytes()));
            ensure!(ledger.append(share, None).await?.inserted);
            let actual: String = sqlx::query_scalar("SELECT header_hash FROM qbit_prism_share_hashes WHERE share_id='native-nonhex-id'")
                .fetch_one(&source.pool).await?;
            ensure!(actual == expected);
            let native = recovery::evidence(&source, pg_bin).await?;
            ensure!(native["records"]["share_hashes"]["count"] == 5);
            let archive = recovery::backup(&source, pg_bin).await?;
            recovery::restore(&archive, &source, &restored, pg_bin).await?;
            ensure!(recovery::evidence(&restored, pg_bin).await? == native);
            // Losing the whole native table must fail closed, never fall back
            // to synthesizing the protection rows from the accepted ledger.
            sqlx::query("ALTER TABLE qbit_prism_share_hashes RENAME TO missing_share_hashes")
                .execute(&source.pool).await?;
            let missing = recovery::evidence(&source, pg_bin).await
                .expect_err("missing native replay table must fail export");
            ensure!(missing.to_string().contains("qbit_prism_share_hashes"));
            sqlx::query("ALTER TABLE missing_share_hashes RENAME TO qbit_prism_share_hashes")
                .execute(&source.pool).await?;
            ensure!(recovery::evidence(&source, pg_bin).await? == native);
            Ok::<_, anyhow::Error>(())
        }.await;
        ledger.pool.close().await;
        result
    }.await;
    source.close().await?;
    restored.close().await?;
    result
}

async fn assert_candidate_payload_fingerprints(
    raw: &str,
    pg_bin: &std::path::Path,
    artifact: &recovery::Artifact,
) -> Result<()> {
    use qbit_prism_server::ledger::Candidate;
    use sha2::{Digest, Sha256};

    let source = recovery::Database::open(raw).await?;
    let restored = recovery::Database::open(raw).await?;
    let result = async {
        // Pre-258 rows have no storage_version column. Migration adds version
        // 1 without changing the evidence for an already-drained candidate.
        sqlx::raw_sql(include_str!("fixtures/schema_2x/001_share_ledger.sql"))
            .execute(&source.pool).await?;
        sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate_sha256,state,completed_at) VALUES(repeat('ef',32),repeat('ab',32),'abandoned',clock_timestamp())")
            .execute(&source.pool).await?;
        let legacy = recovery::evidence(&source, pg_bin).await?;
        let ledger = Ledger::connect_operator(&source.url, true).await?;
        let result = async {
            ensure!(recovery::evidence(&source, pg_bin).await? == legacy);
            let block = [0_u8; 81];
            let mut hash = Sha256::digest(Sha256::digest(&block[..80])).to_vec();
            hash.reverse();
            let candidate = Candidate {
                block_hash: hex::encode(hash),
                block_hex: hex::encode(block),
                job_id: "recovery-candidate".into(),
                payout_revision: 0,
                bundle: serde_json::from_slice(&artifact.canonical)?,
                coinbase_suffix_hex: None,
                deferred_share: None,
            };
            let body = serde_json::to_value(&candidate)?;
            let digest = hex::encode(Sha256::digest(serde_json::to_vec(&candidate)?));
            ledger.enqueue_candidate(candidate.clone()).await?;
            let baseline = recovery::evidence(&source, pg_bin).await?;
            ensure!(baseline["pending_candidates"] == 1);
            let claim = ledger.claim_candidate(60).await?.expect("pending candidate");
            ensure!(serde_json::to_value(claim.candidate)? == body);
            ensure!(recovery::evidence(&source, pg_bin).await? == baseline,
                "candidate claim ownership changed recovery evidence");
            sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL WHERE block_hash=$1")
                .bind(&candidate.block_hash).execute(&source.pool).await?;

            // Keep the declared digest, identity and pending state fixed.
            // JSON null is permitted by the schema but cannot deserialize.
            for mutation in [
                "candidate=jsonb_set(candidate,'{block_hex}','\"deadbeef\"')",
                "candidate=jsonb_set(candidate,'{bundle,found_block,network_difficulty}','101')",
                "candidate='null'::jsonb",
                "storage_version=3",
            ] {
                sqlx::query(&format!("UPDATE qbit_block_candidate_outbox SET {mutation} WHERE block_hash=$1"))
                    .bind(&candidate.block_hash).execute(&source.pool).await?;
                let current = recovery::evidence(&source, pg_bin).await?;
                ensure!(current["records"]["candidates"]["count"] == baseline["records"]["candidates"]["count"]);
                ensure!(current["records"]["candidates"]["sha256"] != baseline["records"]["candidates"]["sha256"],
                    "candidate recovery payload change was invisible: {mutation}");
                let declared: String = sqlx::query_scalar("SELECT candidate_sha256 FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                    .bind(&candidate.block_hash).fetch_one(&source.pool).await?;
                ensure!(declared == digest);
                let mut unchanged = current;
                unchanged["records"]["candidates"] = baseline["records"]["candidates"].clone();
                ensure!(unchanged == baseline, "unrelated evidence changed: {mutation}");
                sqlx::query("UPDATE qbit_block_candidate_outbox SET candidate=$2,storage_version=1 WHERE block_hash=$1")
                    .bind(&candidate.block_hash).bind(&body).execute(&source.pool).await?;
                ensure!(recovery::evidence(&source, pg_bin).await? == baseline);
            }
            let archive = recovery::backup(&source, pg_bin).await?;
            recovery::restore(&archive, &source, &restored, pg_bin).await?;
            ensure!(recovery::evidence(&restored, pg_bin).await? == baseline);
            let restored_ledger = Ledger::connect_operator(&restored.url, false).await?;
            let replayed = restored_ledger.claim_candidate(60).await;
            restored_ledger.pool.close().await;
            ensure!(serde_json::to_value(replayed?.expect("restored pending candidate").candidate)? == body);
            Ok::<_, anyhow::Error>(())
        }.await;
        ledger.pool.close().await;
        result
    }.await;
    source.close().await?;
    restored.close().await?;
    result
}

async fn assert_native_audit_payload_fingerprints(
    source: &recovery::Database,
    pg_bin: &std::path::Path,
    artifact: &recovery::Artifact,
) -> Result<()> {
    use qbit_prism_server::ledger::{audit_canonical_bytes, audit_completeness};
    use sha2::{Digest, Sha256};

    let bundle: qbit_prism::AuditBundle = serde_json::from_slice(&artifact.canonical)?;
    let mut body = serde_json::to_value(&bundle)?;
    body.as_object_mut().unwrap().remove("shares");
    body["reward_manifest"] = serde_json::to_value(bundle.reward_manifest.clone().into_parts().0)?;

    // Two valid ranges let the reference change without violating the foreign
    // key or changing snapshot identities/payloads.
    let mut snapshots = Vec::new();
    for shares in [
        &bundle.shares[..],
        &bundle.shares[..bundle.shares.len() - 1],
    ] {
        let digest = hex::encode(Sha256::digest(serde_json::to_vec(shares)?));
        sqlx::query("INSERT INTO qbit_prism_audit_snapshots(snapshot_sha256,first_share_seq,last_share_seq,anchor_ms,share_count) VALUES($1,$2,$3,$4,$5)")
            .bind(&digest)
            .bind(shares.first().unwrap().share_seq as i64)
            .bind(shares.last().unwrap().share_seq as i64)
            .bind(bundle.found_block.anchor_job_issued_at_ms)
            .bind(shares.len() as i64)
            .execute(&source.pool).await?;
        snapshots.push(digest);
    }
    // Turn one authenticated fixture into the native compact layout after
    // the frozen/imported equality assertions have passed.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2,share_snapshot_sha256=$3,canonical_audit_bytes=NULL WHERE block_hash=$1")
        .bind(&artifact.block_hash).bind(&body).bind(&snapshots[0])
        .execute(&source.pool).await?;
    ensure!(
        audit_canonical_bytes(&source.pool, &artifact.block_hash).await?
            == Some(artifact.canonical.clone())
    );
    audit_completeness(&source.pool).await?.require_complete()?;
    let baseline = recovery::evidence(source, pg_bin).await?;
    ensure!(baseline["records"]["audit_bodies"]["count"] == 1);
    ensure!(baseline["records"]["audit_snapshots"]["count"] == 2);

    let changed_reference = format!("share_snapshot_sha256='{}'", snapshots[1]);
    for (kind, mutation) in [
        (
            "audit_bodies",
            "audit_bundle=jsonb_set(audit_bundle,'{found_block,network_difficulty}','101')",
        ),
        (
            "audit_bodies",
            "audit_bundle=jsonb_set(audit_bundle,'{reward_manifest,included_share_count}','2')",
        ),
        ("audit_bodies", changed_reference.as_str()),
        ("audit_snapshots", "first_share_seq=2"),
        ("audit_snapshots", "last_share_seq=7"),
        ("audit_snapshots", "anchor_ms=anchor_ms+1"),
        ("audit_snapshots", "share_count=share_count+1"),
        ("audit_snapshots", "inline_shares='[]'::jsonb"),
    ] {
        let (table, key, identity) = if kind == "audit_bodies" {
            (
                "qbit_pool_audit_bundles",
                "block_hash",
                &artifact.block_hash,
            )
        } else {
            (
                "qbit_prism_audit_snapshots",
                "snapshot_sha256",
                &snapshots[0],
            )
        };
        sqlx::query(&format!("UPDATE {table} SET {mutation} WHERE {key}=$1"))
            .bind(identity)
            .execute(&source.pool)
            .await?;

        // Availability checks still pass; evidence must detect changed payloads
        // independently of completeness and the unchanged declared digests.
        audit_completeness(&source.pool).await?.require_complete()?;
        let current = recovery::evidence(source, pg_bin).await?;
        ensure!(current["records"][kind]["count"] == baseline["records"][kind]["count"]);
        ensure!(
            current["records"][kind]["sha256"] != baseline["records"][kind]["sha256"],
            "native audit payload change was invisible: {mutation}"
        );
        let mut unchanged = current;
        unchanged["records"][kind] = baseline["records"][kind].clone();
        ensure!(
            unchanged == baseline,
            "unrelated evidence changed: {mutation}"
        );

        sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2,share_snapshot_sha256=$3 WHERE block_hash=$1")
            .bind(&artifact.block_hash).bind(&body).bind(&snapshots[0])
            .execute(&source.pool).await?;
        sqlx::query("UPDATE qbit_prism_audit_snapshots SET first_share_seq=$2,last_share_seq=$3,anchor_ms=$4,share_count=$5,inline_shares=NULL WHERE snapshot_sha256=$1")
            .bind(&snapshots[0])
            .bind(bundle.shares.first().unwrap().share_seq as i64)
            .bind(bundle.shares.last().unwrap().share_seq as i64)
            .bind(bundle.found_block.anchor_job_issued_at_ms)
            .bind(bundle.shares.len() as i64)
            .execute(&source.pool).await?;
        ensure!(recovery::evidence(source, pg_bin).await? == baseline);
    }
    // Preserve a valid inline snapshot through the native backup as well.
    sqlx::query("UPDATE qbit_prism_audit_snapshots SET inline_shares=$2 WHERE snapshot_sha256=$1")
        .bind(&snapshots[0])
        .bind(serde_json::to_value(&bundle.shares)?)
        .execute(&source.pool)
        .await?;
    let inline = recovery::evidence(source, pg_bin).await?;
    ensure!(
        inline["records"]["audit_snapshots"]["sha256"]
            != baseline["records"]["audit_snapshots"]["sha256"]
    );
    ensure!(
        audit_canonical_bytes(&source.pool, &artifact.block_hash).await?
            == Some(artifact.canonical.clone())
    );
    Ok(())
}

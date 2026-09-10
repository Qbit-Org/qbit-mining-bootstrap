//! Real PostgreSQL transaction, failover and accounting tests.
//! PRISM_TEST_DATABASE_URL=postgres://ubuntu@127.0.0.1:55483/postgres cargo test -p qbit-prism-server --test ledger_postgres
use anyhow::{Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, verify_audit_bundle_with_ledger_public_key, AcceptedShare, FoundBlock,
    PayoutPolicy,
};
use qbit_prism_server::ledger::{BlockObservation, Candidate, Ledger, Snapshot};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

#[path = "support/ledger_2x.rs"]
mod two_x;

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
}
impl Database {
    async fn open() -> Result<Option<Self>> {
        let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
            eprintln!("skipping PostgreSQL integration test; set PRISM_TEST_DATABASE_URL");
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        Ok(Some(Self {
            admin,
            schema,
            url: url.to_string(),
        }))
    }
    async fn ledger(&self, id: &str) -> Result<Ledger> {
        Ledger::connect(&self.url, id.to_owned(), 8, true).await
    }
    async fn close(self, ledgers: Vec<Ledger>) -> Result<()> {
        for ledger in ledgers {
            ledger.pool.close().await;
        }
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{id:064x}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn candidate(snapshot: &Snapshot, nonce: u32) -> Result<Candidate> {
    let (coinbase_key, ledger_key) = keys();
    let bundle = build_audit_bundle(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        &coinbase_key,
        &ledger_key,
    )?;
    candidate_with_bundle(bundle, snapshot.payout_revision, nonce)
}

fn candidate_with_bundle(
    bundle: qbit_prism::AuditBundle,
    payout_revision: i64,
    nonce: u32,
) -> Result<Candidate> {
    let (_, ledger_key) = keys();
    let report = verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_key.public_key_hex())?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x20000000u32.to_le_bytes());
    block[4..36].fill(0x22);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
    block[72..76].copy_from_slice(&0x207fffffu32.to_le_bytes());
    block[76..80].copy_from_slice(&nonce.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&block)).to_vec();
    hash.reverse();
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    Ok(Candidate {
        block_hash: hex::encode(hash),
        block_hex: hex::encode(block),
        job_id: "job".into(),
        payout_revision,
        bundle,
        deferred_share: None,
        coinbase_suffix_hex: None,
    })
}

#[tokio::test]
async fn candidate_renewal_requires_a_live_pending_token() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let block = candidate(&a.snapshot(100).await?, 401)?;
    a.enqueue_candidate(block).await?;
    let owner = a.claim_candidate(1).await?.context("candidate missing")?;
    a.renew_candidate_claim(&owner, 60).await?;
    let (remaining, attempts): (bool, i32) = sqlx::query_as("SELECT claim_expires_at>clock_timestamp()+interval '50 seconds',attempt_count FROM qbit_block_candidate_outbox")
        .fetch_one(&a.pool).await?;
    assert!(remaining);
    assert_eq!(attempts, 1, "renewal counted as another processing attempt");
    assert!(b.claim_candidate(60).await?.is_none());
    assert!(a.renew_candidate_claim(&owner, 0).await.is_err());
    assert!(a.renew_candidate_claim(&owner, 601).await.is_err());
    let mut stranger = owner.clone();
    stranger.claim_token = Uuid::new_v4().to_string();
    assert!(b.renew_candidate_claim(&stranger, 60).await.is_err());
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second'")
        .execute(&a.pool).await?;
    assert!(
        a.renew_candidate_claim(&owner, 60).await.is_err(),
        "expired claim revived"
    );
    let before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o")
            .fetch_one(&a.pool)
            .await?;
    assert!(
        a.retry_candidate(&owner, "expired worker").await.is_err(),
        "expired owner delayed recovery"
    );
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o")
            .fetch_one(&a.pool)
            .await?;
    assert_eq!(
        before, after,
        "expired retry cleared ownership or changed its due time"
    );
    // Failed transactions can release their row locks asynchronously in SQLx.
    let recovered = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(claim) = b.claim_candidate(60).await? {
                return Ok::<_, anyhow::Error>(claim);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert_ne!(owner.claim_token, recovered.claim_token);
    assert!(
        a.renew_candidate_claim(&owner, 60).await.is_err(),
        "former owner renewed takeover token"
    );
    b.renew_candidate_claim(&recovered, 60).await?;
    b.finish_candidate(&recovered, false, Some("test completed"))
        .await?;
    assert!(
        b.renew_candidate_claim(&recovered, 60).await.is_err(),
        "terminal claim revived"
    );
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn candidate_mutations_reject_a_token_expired_while_waiting_for_its_row() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    for (index, operation) in ["renew", "retry", "land", "finish"].into_iter().enumerate() {
        let block = candidate(&snapshot, 402 + index as u32)?;
        let hash = block.block_hash.clone();
        a.enqueue_candidate(block).await?;
        let claim = a.claim_candidate(60).await?.unwrap();
        assert_eq!(claim.candidate.block_hash, hash);
        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()+interval '400 milliseconds' WHERE block_hash=$1")
            .bind(&hash).execute(&a.pool).await?;
        let before: serde_json::Value = sqlx::query_scalar(
            "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o WHERE block_hash=$1",
        )
        .bind(&hash)
        .fetch_one(&a.pool)
        .await?;
        let mut blocker = a.pool.begin().await?;
        sqlx::query(
            "SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE",
        )
        .bind(&hash)
        .fetch_one(&mut *blocker)
        .await?;
        let acting = b.clone();
        let public_key = keys().1.public_key_hex();
        let attempt = tokio::spawn(async move {
            match operation {
                "renew" => acting.renew_candidate_claim(&claim, 60).await,
                "retry" => acting.retry_candidate(&claim, "test retry").await,
                "land" => acting.land_candidate(&claim, &public_key).await.map(|_| ()),
                "finish" => acting.finish_candidate(&claim, false, None).await,
                _ => unreachable!(),
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        assert!(
            !attempt.is_finished(),
            "{operation} did not wait for its row lock"
        );
        blocker.commit().await?;
        assert!(
            attempt.await?.is_err(),
            "{operation} accepted a token expired during the lock wait"
        );
        let after: serde_json::Value = sqlx::query_scalar(
            "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o WHERE block_hash=$1",
        )
        .bind(&hash)
        .fetch_one(&a.pool)
        .await?;
        assert_eq!(
            before, after,
            "{operation} changed expired ownership or disposition"
        );
    }
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn candidate_processing_lock_allows_renewal_and_blocks_expired_takeover() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    a.enqueue_candidate(candidate(&a.snapshot(100).await?, 403)?)
        .await?;
    let owner = a.claim_candidate(60).await?.unwrap();
    let mut processing = a.pool.begin().await?;
    // This is the lock held by land/finish while accounting is persisted.
    sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox FOR KEY SHARE")
        .fetch_one(&mut *processing)
        .await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        a.renew_candidate_claim(&owner, 60),
    )
    .await
    .context("processing lock blocked its own renewal")??;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second'")
        .execute(&a.pool).await?;
    assert!(
        b.claim_candidate(60).await?.is_none(),
        "takeover bypassed the processing transaction"
    );
    processing.commit().await?;
    let recovered = b
        .claim_candidate(60)
        .await?
        .context("claim unavailable after processing lock released")?;
    assert_ne!(owner.claim_token, recovered.claim_token);
    assert!(a.renew_candidate_claim(&owner, 60).await.is_err());
    b.finish_candidate(&recovered, false, None).await?;
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn candidate_dispatch_prioritizes_fresh_work_and_services_oldest_due_fairly() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    for _ in 0..5 {
        assert!(a.claim_candidate(60).await?.is_none());
    }
    let consumed: bool =
        sqlx::query_scalar("SELECT is_called FROM qbit_prism_candidate_dispatch_sequence")
            .fetch_one(&a.pool)
            .await?;
    assert!(!consumed, "plainly empty polling consumed dispatch slots");
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let recovery = candidate(&snapshot, 410)?;
    let old_unattempted = candidate(&snapshot, 411)?;
    a.enqueue_candidate(recovery.clone()).await?;
    a.enqueue_candidate(old_unattempted.clone()).await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET attempt_count=3,created_at=clock_timestamp()-interval '1 hour',next_attempt_at=clock_timestamp()-interval '1 hour' WHERE block_hash=$1")
        .bind(&recovery.block_hash).execute(&a.pool).await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET created_at=clock_timestamp()-interval '2 hours',next_attempt_at=clock_timestamp()-interval '2 hours' WHERE block_hash=$1")
        .bind(&old_unattempted.block_hash).execute(&a.pool).await?;
    for slot in 1..=7 {
        let fresh = candidate(&snapshot, 420 + slot)?;
        a.enqueue_candidate(fresh.clone()).await?;
        let ledger = if slot % 2 == 0 { &a } else { &b };
        let claim = ledger.claim_candidate(60).await?.unwrap();
        assert_eq!(
            claim.candidate.block_hash, fresh.block_hash,
            "old backlog delayed slot {slot}"
        );
        ledger.finish_candidate(&claim, false, None).await?;
    }
    let latest = candidate(&snapshot, 428)?;
    a.enqueue_candidate(latest.clone()).await?;
    let fair = b.claim_candidate(60).await?.unwrap();
    assert_eq!(
        fair.candidate.block_hash, old_unattempted.block_hash,
        "old never-attempted work starved"
    );
    b.finish_candidate(&fair, false, None).await?;
    let fresh = a.claim_candidate(60).await?.unwrap();
    assert_eq!(fresh.candidate.block_hash, latest.block_hash);
    a.finish_candidate(&fresh, false, None).await?;
    for slot in 10..=15 {
        let fresh = candidate(&snapshot, 420 + slot)?;
        a.enqueue_candidate(fresh.clone()).await?;
        let claim = a.claim_candidate(60).await?.unwrap();
        assert_eq!(claim.candidate.block_hash, fresh.block_hash);
        a.finish_candidate(&claim, false, None).await?;
    }
    a.enqueue_candidate(candidate(&snapshot, 436)?).await?;
    let fair = b.claim_candidate(60).await?.unwrap();
    assert_eq!(
        fair.candidate.block_hash, recovery.block_hash,
        "due recovery starved under new work"
    );
    b.finish_candidate(&fair, false, None).await?;
    db.close(vec![a, b]).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_instances_have_one_commit_order_and_stable_snapshots() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let mut tasks = Vec::new();
    for index in 1..=96 {
        let ledger = if index % 2 == 0 { a.clone() } else { b.clone() };
        tasks.push(tokio::spawn(async move {
            ledger.append(share(index), None).await
        }));
    }
    let mut snapshots = Vec::new();
    for _ in 0..4 {
        snapshots.push(a.snapshot(100).await?);
    }
    let mut accepted = Vec::new();
    for task in tasks {
        accepted.push(task.await??.share);
    }
    accepted.sort_by_key(|s| s.share_seq);
    assert_eq!(accepted.len(), 96);
    assert!(accepted
        .windows(2)
        .all(|s| s[0].share_seq < s[1].share_seq && s[0].accepted_at_ms <= s[1].accepted_at_ms));
    for snapshot in snapshots {
        let expected: Vec<_> = accepted
            .iter()
            .filter(|s| s.share_seq <= snapshot.share_seq)
            .cloned()
            .collect();
        assert_eq!(snapshot.shares, expected);
        assert!(accepted
            .iter()
            .filter(|s| s.share_seq > snapshot.share_seq)
            .all(|s| s.accepted_at_ms > snapshot.anchor_ms));
    }
    let limited = a.snapshot(1).await?;
    assert_eq!(limited.shares.len(), 8);
    let ids = futures_util::future::try_join_all((0..32).map(|_| b.new_session_id())).await?;
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(ids.len(), unique.len());
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn global_duplicates_idempotence_and_config_fencing() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.configure("same").await?;
    b.configure("same").await?;
    assert!(b.configure("different").await.is_err());
    let inserted = a.append(share(1), None).await?;
    assert!(inserted.inserted);
    let replay = b.append(share(1), None).await?;
    assert!(!replay.inserted);
    assert_eq!(inserted.share, replay.share);
    let mut another = share(1);
    another.share_id = format!("different-worker:{:064x}", 1);
    assert!(b
        .append(another, None)
        .await
        .unwrap_err()
        .to_string()
        .contains("duplicate-share"));
    let mut mismatch = share(1);
    mismatch.share_difficulty = 2;
    assert!(b.append(mismatch, None).await.is_err());
    assert_eq!(a.snapshot(100).await?.shares.len(), 1);
    assert!(
        sqlx::query("UPDATE qbit_share_ledger SET share_difficulty=2")
            .execute(&a.pool)
            .await
            .is_err()
    );
    assert!(sqlx::query("INSERT INTO qbit_ledger_writer_lease(singleton,writer_id,writer_epoch,writer_session_token,lease_expires_at) VALUES(true,'python',1,'session',clock_timestamp()+interval '1 hour')").execute(&a.pool).await.is_err());
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn shared_chainwork_rejects_lagging_nodes_and_fences_acknowledgements() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let first = a.observe_chain_view(&"11".repeat(32), 100, "100").await?;
    assert_eq!(
        b.observe_chain_view(&"11".repeat(32), 100, "0100").await?,
        first
    );
    assert!(b
        .observe_chain_view(&"22".repeat(32), 99, "ff")
        .await
        .is_err());
    assert!(b
        .observe_chain_view(&"22".repeat(32), 100, "100")
        .await
        .is_err());
    assert!(b
        .observe_chain_view(&"11".repeat(32), 101, "100")
        .await
        .is_err());
    a.append_at_revision(share(1), None, first).await?;
    let next = b.observe_chain_view(&"33".repeat(32), 101, "102").await?;
    assert_eq!(next, first + 1);
    assert!(
        a.append_at_revision(share(2), None, first).await.is_err(),
        "slower node acknowledged work after peer chain advancement"
    );
    assert!(
        a.reconcile_blocks_at_revision(&[], 101, first)
            .await
            .is_err(),
        "stale chain observations passed revision fencing"
    );
    b.append_at_revision(share(2), None, next).await?;
    // Greater cumulative work, rather than block height, chooses the view.
    let max = b
        .observe_chain_view(&"44".repeat(32), 90, &"ff".repeat(32))
        .await?;
    assert_eq!(max, next + 1);
    assert!(a
        .observe_chain_view(&"33".repeat(32), 101, "102")
        .await
        .is_err());
    assert_eq!(a.snapshot(100).await?.shares.len(), 2);
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn active_candidate_can_land_at_proven_new_chain_revision() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 987)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block).await?;
    let claim = a.claim_candidate(60).await?.unwrap();
    let revision = a.observe_chain_view(&hash, 101, "1234").await?;
    assert!(a
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await
        .is_err());
    assert!(a
        .land_candidate_at_revision(&claim, &keys().1.public_key_hex(), snapshot.payout_revision)
        .await
        .is_err());
    a.land_candidate_at_revision(&claim, &keys().1.public_key_hex(), revision)
        .await?;
    a.finish_candidate_at_revision(&claim, true, None, revision)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1"
        )
        .bind(hash)
        .fetch_one(&a.pool)
        .await?,
        "confirmed"
    );
    db.close(vec![a]).await
}

#[tokio::test]
async fn candidate_outbox_is_atomic_and_claims_recover_after_owner_loss() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 1)?;
    let mut invalid = block.clone();
    invalid.block_hash = "00".repeat(32);
    assert!(a.append(share(2), Some(invalid)).await.is_err());
    assert_eq!(a.snapshot(100).await?.shares.len(), 1);
    a.append(share(2), Some(block.clone())).await?;
    assert!(
        !b.enqueue_candidate_once(block.clone()).await?,
        "block already credited through the ordinary share path was enqueued again"
    );
    let owner = a
        .claim_candidate(60)
        .await?
        .context("candidate not claimed")?;
    assert!(b.claim_candidate(60).await?.is_none());
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second'").execute(&a.pool).await?;
    let recovered = b
        .claim_candidate(60)
        .await?
        .context("candidate not recovered")?;
    assert_ne!(owner.claim_token, recovered.claim_token);
    assert!(a.finish_candidate(&owner, false, None).await.is_err());
    b.finish_candidate(&recovered, false, Some("test rejection"))
        .await?;
    let state: String = sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox")
        .fetch_one(&a.pool)
        .await?;
    assert_eq!(state, "abandoned");
    assert!(a.claim_candidate(60).await?.is_none());
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn durable_sessions_upgrade_async_commit_and_keep_remote_apply() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let initialized = db.ledger("initialize").await?;
    let mut ledgers = vec![initialized];
    for (requested, expected) in [
        ("off", "on"),
        ("local", "on"),
        ("remote_apply", "remote_apply"),
    ] {
        let mut url = url::Url::parse(&db.url)?;
        url.set_query(None);
        url.query_pairs_mut().append_pair(
            "options",
            &format!(
                "-csearch_path={} -csynchronous_commit={requested}",
                db.schema
            ),
        );
        let ledger =
            Ledger::connect(url.as_str(), format!("durable-{requested}"), 2, false).await?;
        let mode: String = sqlx::query_scalar("SHOW synchronous_commit")
            .fetch_one(&ledger.pool)
            .await?;
        assert_eq!(mode, expected);
        ledgers.push(ledger);
    }
    db.close(ledgers).await
}

#[tokio::test]
async fn simultaneous_block_only_enqueue_has_one_winner() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 99)?;
    let (first, second) = tokio::join!(
        a.enqueue_candidate_once(block.clone()),
        b.enqueue_candidate_once(block)
    );
    assert_ne!(
        first?, second?,
        "both duplicate network proofs won deferred acknowledgement"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_block_candidate_outbox")
            .fetch_one(&a.pool)
            .await?,
        1
    );
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn verified_landing_reconstructs_audit_and_reorgs_are_revision_fenced() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 7)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.clone()).await?;
    let claim = a.claim_candidate(60).await?.unwrap();
    assert!(a
        .land_candidate(&claim, &keys().0.public_key_hex())
        .await
        .is_err());
    a.land_candidate(&claim, &keys().1.public_key_hex()).await?;
    a.land_candidate(&claim, &keys().1.public_key_hex()).await?;
    let compact:bool=sqlx::query_scalar("SELECT NOT(audit_bundle ? 'shares') AND share_snapshot_sha256 IS NOT NULL FROM qbit_pool_audit_bundles").fetch_one(&a.pool).await?;
    assert!(compact);
    let hydrated = a.audit_bundle(&hash).await?.unwrap();
    assert_eq!(hydrated, serde_json::to_value(&block.bundle)?);
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle_sha256=$1 WHERE block_hash=$2")
        .bind("00".repeat(32))
        .bind(&hash)
        .execute(&a.pool)
        .await?;
    assert!(
        a.audit_bundle(&hash).await.is_err(),
        "hydrated native audit ignored the declared full body hash"
    );
    let digest = hex::encode(Sha256::digest(qbit_prism::canonical_audit_bundle_bytes(
        &block.bundle,
    )?));
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle_sha256=$1 WHERE block_hash=$2")
        .bind(digest)
        .bind(&hash)
        .execute(&a.pool)
        .await?;
    a.finish_candidate(&claim, true, None).await?;
    let first_revision = a.payout_revision().await?;
    assert_eq!(first_revision, snapshot.payout_revision + 1);
    let inactive = [BlockObservation {
        block_hash: hash.clone(),
        active: false,
    }];
    assert!(b
        .reconcile_blocks_at_revision(&inactive, 101, snapshot.payout_revision)
        .await
        .is_err());
    b.reconcile_blocks_at_revision(&inactive, 101, first_revision)
        .await?;
    assert_eq!(
        a.pool_blocks_for_reconcile().await?[0].chain_state,
        "inactive"
    );
    let active = [BlockObservation {
        block_hash: hash.clone(),
        active: true,
    }];
    b.reconcile_blocks_at_revision(&active, 1101, b.payout_revision().await?)
        .await?;
    assert_eq!(
        a.pool_blocks_for_reconcile().await?[0].maturity_state,
        "mature"
    );
    assert!(a
        .reconcile_blocks_at_revision(&inactive, 1101, a.payout_revision().await?)
        .await
        .is_err());
    assert!(a.append(share(3), None).await.is_err());
    assert!(a.snapshot(100).await.is_err());
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn legacy_audit_import_validates_envelope_hash_and_pinned_key() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 31)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.clone()).await?;
    let claim = a.claim_candidate(60).await?.unwrap();
    let report = a.land_candidate(&claim, &keys().1.public_key_hex()).await?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("legacy-audit.json");
    let mut body = serde_json::to_value(&block.bundle)?;
    let shares = body.as_object_mut().unwrap().remove("shares").unwrap();
    let envelope = json!({"schema":"qbit.prism.audit-body-ref.v1","audit_bundle_sha256":report.audit_bundle_sha256_hex,"share_count":1,"bundle_without_shares":body,"share_parts":[{"kind":"inline","first_share_seq":block.bundle.shares[0].share_seq,"last_share_seq":block.bundle.shares[0].share_seq,"share_count":1,"shares":shares}]});
    std::fs::write(&path, serde_json::to_vec(&envelope)?)?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=NULL,share_snapshot_sha256=NULL,body_uri=$2 WHERE block_hash=$1").bind(&hash).bind(path.to_str().unwrap()).execute(&a.pool).await?;
    assert!(a
        .import_legacy_audits(Some(directory.path()), &keys().0.public_key_hex())
        .await
        .is_err());
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle_sha256=$2 WHERE block_hash=$1")
        .bind(&hash)
        .bind("00".repeat(32))
        .execute(&a.pool)
        .await?;
    assert!(a
        .import_legacy_audits(Some(directory.path()), &keys().1.public_key_hex())
        .await
        .is_err());
    assert!(a.audit_bundle(&hash).await?.is_none());
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle_sha256=$2 WHERE block_hash=$1")
        .bind(&hash)
        .bind(&report.audit_bundle_sha256_hex)
        .execute(&a.pool)
        .await?;
    assert_eq!(
        a.import_legacy_audits(Some(directory.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    assert_eq!(
        a.import_legacy_audits(Some(directory.path()), &keys().1.public_key_hex())
            .await?,
        0
    );
    assert_eq!(
        a.audit_bundle(&hash).await?.unwrap(),
        serde_json::to_value(&block.bundle)?
    );
    assert!(path.exists());
    db.close(vec![a]).await
}

#[tokio::test]
async fn deferred_share_survives_confirmation_crash_and_is_credited_once() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let mut block = candidate(&snapshot, 9)?;
    block.deferred_share = Some(share(2));
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block).await?;
    let claim = a.claim_candidate(60).await?.unwrap();
    a.land_candidate(&claim, &keys().1.public_key_hex()).await?;
    assert_eq!(a.snapshot(100).await?.shares.len(), 1);
    b.reconcile_blocks_at_revision(
        &[BlockObservation {
            block_hash: hash,
            active: true,
        }],
        101,
        snapshot.payout_revision,
    )
    .await?;
    a.finish_candidate(&claim, true, None).await?;
    assert_eq!(a.snapshot(100).await?.shares.len(), 2);
    let stats: serde_json::Value =
        sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(&a.pool)
            .await?;
    assert_eq!(stats["current_drift_count"], json!(0));
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn migration_refuses_an_active_legacy_writer() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    sqlx::raw_sql(include_str!("../../qbit-prism/sql/001_share_ledger.sql"))
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO qbit_ledger_writer_lease(singleton,writer_id,writer_epoch,writer_session_token,lease_expires_at) VALUES(true,'python',1,'session',clock_timestamp()+interval '1 hour')").execute(&pool).await?;
    let result = db.ledger("a").await;
    assert!(result.is_err());
    assert!(result
        .err()
        .unwrap()
        .to_string()
        .contains("live legacy Python"));
    sqlx::query("UPDATE qbit_ledger_writer_lease SET lease_expires_at=clock_timestamp()-interval '1 second'").execute(&pool).await?;
    let a = db.ledger("a").await?;
    pool.close().await;
    db.close(vec![a]).await
}

#[tokio::test]
async fn migration_refuses_undrained_legacy_block_candidates() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    sqlx::raw_sql(include_str!("../../qbit-prism/sql/001_share_ledger.sql"))
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256) VALUES($1,$2,$3)")
        .bind("77".repeat(32)).bind(json!({"block_hex":"legacy-python-payload"})).bind("88".repeat(32)).execute(&pool).await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted undrained legacy candidate")?;
    assert!(
        error.to_string().contains("outbox is not drained"),
        "{error}"
    );
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state='pending'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(pending, 1, "failed migration changed legacy state");
    sqlx::query("UPDATE qbit_block_candidate_outbox SET state='submitted',candidate=NULL,completed_at=clock_timestamp()").execute(&pool).await?;
    let a = db.ledger("a").await?;
    pool.close().await;
    db.close(vec![a]).await
}

#[tokio::test]
async fn reconciliation_watches_only_recent_blocks_and_one_mature_checkpoint() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    for height in 1i64..=12 {
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state,matured_at) VALUES($1,$2,'parent','coinbase','manifest','confirmed',CASE WHEN $2<=10 THEN 'mature' ELSE 'immature' END,CASE WHEN $2<=10 THEN clock_timestamp() ELSE NULL END)")
            .bind(format!("{height:064x}")).bind(height).execute(&a.pool).await?;
    }
    let watched = a.pool_blocks_for_reconcile().await?;
    assert_eq!(
        watched.iter().map(|block| block.height).collect::<Vec<_>>(),
        vec![10, 11, 12]
    );
    db.close(vec![a]).await
}

#[tokio::test]
async fn late_acceptance_after_abandoned_claim_recovers_deferred_credit() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let mut block = candidate(&snapshot, 43)?;
    block.deferred_share = Some(share(2));
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block).await?;
    let expired = a.claim_candidate(60).await?.unwrap();
    a.land_candidate(&expired, &keys().1.public_key_hex())
        .await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second'").execute(&a.pool).await?;
    let recovered = b.claim_candidate(60).await?.unwrap();
    b.finish_candidate_at_revision(
        &recovered,
        false,
        Some("parent changed"),
        snapshot.payout_revision,
    )
    .await?;
    assert_eq!(
        a.pool_blocks_for_reconcile().await?[0].chain_state,
        "inactive"
    );
    let cleared: bool = sqlx::query_scalar(
        "SELECT candidate IS NULL AND state='abandoned' FROM qbit_block_candidate_outbox",
    )
    .fetch_one(&a.pool)
    .await?;
    assert!(cleared);
    assert!(a
        .finish_candidate_at_revision(&expired, true, None, snapshot.payout_revision)
        .await
        .is_err());
    // The expired process's submitblock can still finish after its replacement
    // abandoned the attempt. Active-chain reconciliation must recover it.
    let active = [BlockObservation {
        block_hash: hash.clone(),
        active: true,
    }];
    b.reconcile_blocks_at_revision(&active, 101, b.payout_revision().await?)
        .await?;
    b.reconcile_blocks_at_revision(&active, 101, b.payout_revision().await?)
        .await?;
    assert_eq!(a.snapshot(100).await?.shares.len(), 2);
    assert_eq!(
        a.pool_blocks_for_reconcile().await?[0].chain_state,
        "confirmed"
    );
    assert!(a.audit_bundle(&hash).await?.is_some());
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn stale_candidate_active_proof_cannot_overwrite_a_newer_reorg() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 47)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block).await?;
    let claim = a.claim_candidate(60).await?.unwrap();
    a.land_candidate(&claim, &keys().1.public_key_hex()).await?;
    b.reconcile_blocks_at_revision(
        &[BlockObservation {
            block_hash: hash.clone(),
            active: true,
        }],
        101,
        snapshot.payout_revision,
    )
    .await?;
    let active_proof_revision = a.payout_revision().await?;
    b.reconcile_blocks_at_revision(
        &[BlockObservation {
            block_hash: hash,
            active: false,
        }],
        101,
        active_proof_revision,
    )
    .await?;
    assert!(a
        .finish_candidate_at_revision(&claim, true, None, active_proof_revision)
        .await
        .is_err());
    assert_eq!(
        a.pool_blocks_for_reconcile().await?[0].chain_state,
        "inactive"
    );
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn ctv_artifacts_wait_for_maturity_and_claims_are_fenced() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let mut second = share(2);
    second.miner_id = "other".into();
    second.order_key = "other".into();
    second.p2mr_program_hex = "22".repeat(32);
    a.append(second, None).await?;
    let snapshot = a.snapshot(100).await?;
    let (coinbase_key, ledger_key) = keys();
    let bundle = qbit_prism::build_audit_bundle_with_ctv_settlement_options(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        vec![],
        PayoutPolicy::day_one_default(),
        u64::MAX,
        qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
        Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
        None,
        vec![],
        &coinbase_key,
        &ledger_key,
    )?;
    assert_eq!(
        bundle
            .ctv_fanout_manifest_set
            .as_ref()
            .unwrap()
            .fanout_count,
        2
    );
    let block = candidate_with_bundle(bundle, snapshot.payout_revision, 11)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block).await?;
    let claim = a.claim_candidate(60).await?.unwrap();
    a.land_candidate(&claim, &ledger_key.public_key_hex())
        .await?;
    a.finish_candidate(&claim, true, None).await?;
    assert!(a.claim_fanout(60).await?.is_none());
    assert_eq!(a.backfill_ctv(&ledger_key.public_key_hex()).await?, 0);
    // Rebuild one missing chunk without duplicating the set or the other.
    sqlx::query("DELETE FROM qbit_ctv_fanout_artifacts WHERE chunk_index=1")
        .execute(&a.pool)
        .await?;
    assert_eq!(a.backfill_ctv(&ledger_key.public_key_hex()).await?, 1);
    assert_eq!(a.backfill_ctv(&ledger_key.public_key_hex()).await?, 0);
    a.reconcile_blocks_at_revision(
        &[BlockObservation {
            block_hash: hash,
            active: true,
        }],
        1101,
        a.payout_revision().await?,
    )
    .await?;
    let first = a.claim_fanout(60).await?.unwrap();
    let second = b.claim_fanout(60).await?.unwrap();
    assert_ne!(first.fanout_txid, second.fanout_txid);
    assert!(a.claim_fanout(60).await?.is_none());
    a.record_fanout_scan(&first, 1103, Some((1102, "44".repeat(32))))
        .await?;
    assert!(
        a.reserve_cpfp_funding(&first, "wallet", "55".repeat(32).as_str(), 0, 100_000)
            .await?
    );
    assert!(
        !b.reserve_cpfp_funding(&second, "wallet", "55".repeat(32).as_str(), 0, 100_000)
            .await?,
        "instances reserved the same funding outpoint"
    );
    a.save_cpfp_package(&first, "aabb", &"66".repeat(32))
        .await?;
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE fanout_txid=$1").bind(&first.fanout_txid).execute(&a.pool).await?;
    assert!(
        a.renew_fanout_claim(&first, 60).await.is_err(),
        "expired owner revived its lease"
    );
    // SQLx queues rollback when the rejected renewal drops its transaction.
    // SKIP LOCKED may briefly skip that row until the rollback releases it.
    let recovered = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(claim) = b.claim_fanout(60).await? {
                return Ok::<_, anyhow::Error>(claim);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .context("expired fanout was not reclaimed after renewal rollback")??;
    assert_eq!(first.fanout_txid, recovered.fanout_txid);
    assert_eq!(recovered.progress["scan_next_height"], json!(1103));
    assert_eq!(
        recovered.progress["scan_anchor_hash"],
        json!("44".repeat(32))
    );
    assert_eq!(
        b.cpfp_package(&recovered.fanout_txid).await?.unwrap()["signed_child_hex"],
        json!("aabb")
    );
    assert!(
        a.save_cpfp_package(&first, "aabb", &"66".repeat(32))
            .await
            .is_err(),
        "expired owner changed the package"
    );
    b.save_cpfp_package(&recovered, "aabb", &"66".repeat(32))
        .await?;
    assert!(
        b.save_cpfp_package(&recovered, "ccdd", &"77".repeat(32))
            .await
            .is_err(),
        "persisted signed package was mutable"
    );
    assert!(a
        .finish_fanout(&first, "confirmed", Some(json!({"accepted":true})), None)
        .await
        .is_err());
    b.finish_fanout(
        &recovered,
        "broadcast_submitted",
        Some(json!({"accepted":true})),
        None,
    )
    .await?;
    b.finish_fanout(&second, "failed", None, Some("temporary RPC outage"))
        .await?;
    let attempts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_broadcast_attempts")
            .fetch_one(&a.pool)
            .await?;
    assert_eq!(attempts, 2);
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=NULL")
        .execute(&a.pool)
        .await?;
    let one = a.claim_fanout(60).await?.unwrap();
    let two = b.claim_fanout(60).await?.unwrap();
    a.observe_fanout(&one,"confirmed",json!({"confirmation":{"block_hash":"aa".repeat(32),"block_height":1102,"confirmations":1001}})).await?;
    b.observe_fanout(&two,"confirmed",json!({"confirmation":{"block_hash":"bb".repeat(32),"block_height":1103,"confirmations":1000}})).await?;
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=NULL")
        .execute(&a.pool)
        .await?;
    let checkpoint = a.claim_fanout(60).await?.unwrap();
    assert_eq!(
        checkpoint.fanout_txid, two.fanout_txid,
        "reconciliation missed latest mature fanout checkpoint"
    );
    assert!(
        b.claim_fanout(60).await?.is_none(),
        "all deep fanout history was polled"
    );
    let prior_revision = a.payout_revision().await?;
    a.observe_chain_view(&"cc".repeat(32), 2102, "100").await?;
    assert!(
        a.halt_fanout_reorg(&checkpoint, prior_revision)
            .await
            .is_err(),
        "stale node proof halted a stronger shared chain view"
    );
    assert!(a.observe_fanout(&checkpoint,"confirmed",json!({"payout_revision":prior_revision,"confirmation":{"block_hash":"bb".repeat(32),"block_height":1103,"confirmations":1000}})).await.is_err(),"stale fanout observation changed shared settlement");
    assert!(
        a.payout_revision().await.is_ok(),
        "stale mature fanout observation halted the cluster"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_ctv_fanout_broadcast_attempts")
            .fetch_one(&a.pool)
            .await?,
        2,
        "read observations bloated attempt history"
    );
    a.finish_fanout(
        &checkpoint,
        "failed",
        None,
        Some("node temporarily unavailable"),
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT settlement_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1"
        )
        .bind(&checkpoint.fanout_txid)
        .fetch_one(&a.pool)
        .await?,
        "confirmed",
        "transient node error discarded a proven global confirmation"
    );
    let stats: serde_json::Value =
        sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(&a.pool)
            .await?;
    assert_eq!(stats["current_drift_count"], json!(0));
    db.close(vec![a, b]).await
}

async fn prepare_mature_cpfp_fanouts(ledger: &Ledger, count: u8) -> Result<()> {
    for n in 1..=count {
        let mut accepted = share(u64::from(n));
        accepted.miner_id = format!("miner-{n}");
        accepted.order_key = accepted.miner_id.clone();
        accepted.p2mr_program_hex = format!("{n:02x}").repeat(32);
        ledger.append(accepted, None).await?;
    }
    let snapshot = ledger.snapshot(100).await?;
    let (coinbase_key, ledger_key) = keys();
    let bundle = qbit_prism::build_audit_bundle_with_ctv_settlement_options(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        vec![],
        PayoutPolicy::day_one_default(),
        u64::MAX,
        qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
        Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
        None,
        vec![],
        &coinbase_key,
        &ledger_key,
    )?;
    assert_eq!(
        bundle
            .ctv_fanout_manifest_set
            .as_ref()
            .unwrap()
            .fanout_count,
        u32::from(count)
    );
    let block = candidate_with_bundle(bundle, snapshot.payout_revision, 31)?;
    let hash = block.block_hash.clone();
    ledger.enqueue_candidate(block).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    ledger
        .land_candidate(&claim, &ledger_key.public_key_hex())
        .await?;
    ledger.finish_candidate(&claim, true, None).await?;
    ledger
        .reconcile_blocks_at_revision(
            &[BlockObservation {
                block_hash: hash,
                active: true,
            }],
            1101,
            ledger.payout_revision().await?,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn cpfp_retirement_requires_current_claim_and_preserves_signed_packages() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("cpfp-a").await?;
    let b = db.ledger("cpfp-b").await?;
    prepare_mature_cpfp_fanouts(&a, 2).await?;
    let signed = a.claim_fanout(60).await?.unwrap();
    let old = b.claim_fanout(60).await?.unwrap();
    let signed_funding = "55".repeat(32);
    let unsigned_funding = "66".repeat(32);
    assert!(
        a.reserve_cpfp_funding(&signed, "signing-wallet", &signed_funding, 0, 100_000)
            .await?
    );
    a.save_cpfp_package(&signed, "aabb", &"77".repeat(32))
        .await?;
    let signed_before = a.cpfp_package(&signed.fanout_txid).await?.unwrap();
    assert!(
        a.retire_unsigned_cpfp_funding(&signed, &signed_funding, 0, "spent")
            .await
            .is_err(),
        "signed package retirement was accepted"
    );
    assert_eq!(
        a.cpfp_package(&signed.fanout_txid).await?.unwrap(),
        signed_before
    );
    assert!(a
        .retired_cpfp_funding(&signed.fanout_txid)
        .await?
        .is_empty());
    assert!(
        b.reserve_cpfp_funding(&old, "original-wallet", &unsigned_funding, 2, 200_000)
            .await?
    );
    let unsigned_before = b.cpfp_package(&old.fanout_txid).await?.unwrap();
    assert!(b
        .retire_unsigned_cpfp_funding(&old, &unsigned_funding, 3, "wrong outpoint")
        .await
        .is_err());
    assert_eq!(
        b.cpfp_package(&old.fanout_txid).await?.unwrap(),
        unsigned_before
    );
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE fanout_txid=$1").bind(&old.fanout_txid).execute(&a.pool).await?;
    assert!(
        b.retire_unsigned_cpfp_funding(&old, &unsigned_funding, 2, "spent")
            .await
            .is_err(),
        "expired claim retired funding"
    );
    let current = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(claim) = a.claim_fanout(60).await? {
                break Ok::<_, anyhow::Error>(claim);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert_eq!(current.fanout_txid, old.fanout_txid);
    a.retire_unsigned_cpfp_funding(
        &current,
        &unsigned_funding,
        2,
        "funding spent before signing",
    )
    .await?;
    assert!(a.cpfp_package(&current.fanout_txid).await?.is_none());
    let pending = a.retired_cpfp_funding(&current.fanout_txid).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["funding_txid"], unsigned_funding);
    assert_eq!(pending[0]["funding_vout"], 2);
    assert_eq!(pending[0]["wallet_name"], "original-wallet");
    assert_eq!(pending[0]["funding_value_sats"], 200_000);
    assert_eq!(
        pending[0]["retirement_reason"],
        "funding spent before signing"
    );
    assert_eq!(pending[0]["wallet_lock_released"], false);
    assert!(
        b.mark_retired_cpfp_wallet_unlocked(&old, &unsigned_funding, 2)
            .await
            .is_err(),
        "stale claim acknowledged cleanup"
    );
    let replacement = "88".repeat(32);
    assert!(
        a.reserve_cpfp_funding(&current, "replacement-wallet", &replacement, 1, 300_000)
            .await?
    );
    a.mark_retired_cpfp_wallet_unlocked(&current, &unsigned_funding, 2)
        .await?;
    assert!(a
        .retired_cpfp_funding(&current.fanout_txid)
        .await?
        .is_empty());
    let archived:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(r) FROM qbit_prism_cpfp_retired_funding r WHERE funding_txid=$1 AND funding_vout=2").bind(&unsigned_funding).fetch_one(&a.pool).await?;
    assert_eq!(archived["wallet_lock_released"], true);
    assert_eq!(archived["wallet_name"], "original-wallet");
    assert_eq!(
        archived["retirement_reason"],
        "funding spent before signing"
    );
    let active = a.cpfp_package(&current.fanout_txid).await?.unwrap();
    assert_eq!(active["funding_txid"], replacement);
    assert_eq!(
        active["wallet_lock_released"], false,
        "retired cleanup changed the active package"
    );
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn concurrent_cpfp_allocations_exclude_active_and_retired_outpoints() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("allocator-a").await?;
    let b = db.ledger("allocator-b").await?;
    prepare_mature_cpfp_fanouts(&a, 2).await?;
    let first = a.claim_fanout(60).await?.unwrap();
    let second = b.claim_fanout(60).await?.unwrap();
    let retired = "91".repeat(32);
    assert!(
        a.reserve_cpfp_funding(&first, "wallet", &retired, 0, 100_000)
            .await?
    );
    assert!(
        !b.reserve_cpfp_funding(&second, "wallet", &retired, 0, 100_000)
            .await?
    );
    let (retirement, allocation) = tokio::join!(
        a.retire_unsigned_cpfp_funding(&first, &retired, 0, "stale external spend"),
        b.reserve_cpfp_funding(&second, "wallet", &retired, 0, 100_000),
    );
    retirement?;
    assert!(
        !allocation?,
        "allocation slipped between retirement and archived exclusion"
    );
    assert!(a.cpfp_package(&first.fanout_txid).await?.is_none());
    assert!(b.cpfp_package(&second.fanout_txid).await?.is_none());
    a.mark_retired_cpfp_wallet_unlocked(&first, &retired, 0)
        .await?;
    let (again_first, again_second) = tokio::join!(
        a.reserve_cpfp_funding(&first, "wallet", &retired, 0, 100_000),
        b.reserve_cpfp_funding(&second, "wallet", &retired, 0, 100_000),
    );
    assert!(
        !again_first? && !again_second?,
        "cleaned retired funding became allocatable again"
    );
    let fresh = "92".repeat(32);
    let (first_won, second_won) = tokio::join!(
        a.reserve_cpfp_funding(&first, "wallet", &fresh, 1, 200_000),
        b.reserve_cpfp_funding(&second, "wallet", &fresh, 1, 200_000),
    );
    let first_won = first_won?;
    let second_won = second_won?;
    assert_ne!(
        first_won, second_won,
        "concurrent frontends must allocate the fresh outpoint once"
    );
    let (loser, claim) = if first_won {
        (&b, &second)
    } else {
        (&a, &first)
    };
    assert!(
        loser
            .reserve_cpfp_funding(claim, "wallet", &"93".repeat(32), 2, 300_000)
            .await?
    );
    let active: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_cpfp_packages")
        .fetch_one(&a.pool)
        .await?;
    let archived:i64=sqlx::query_scalar("SELECT count(*) FROM qbit_prism_cpfp_retired_funding WHERE funding_txid=$1 AND wallet_lock_released").bind(&retired).fetch_one(&a.pool).await?;
    assert_eq!(active, 2);
    assert_eq!(archived, 1, "replacement lost durable cleanup history");
    db.close(vec![a, b]).await
}

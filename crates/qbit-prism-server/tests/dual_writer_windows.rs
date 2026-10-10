//! Dual-writer windows through the production landing path (PRISM 3.1).
//!
//! A frontend in dual-writer mode builds work on windows with a per-node cut.
//! These tests enqueue and land blocks found on such work while the peer's
//! rows keep arriving late, with `share_seq` inside the window's range and
//! above it, stamped before its anchor: the case a timestamp-only window
//! cannot survive. The landing's durable-range proof, its in-lock count, the
//! #619 holding probe at enqueue and the audit reconstruction afterwards must
//! all read the window the coinbase paid.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=... \
//!   cargo test --locked -p qbit-prism-server --test dual_writer_windows
//! ```

use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle_body_with_coinbase_options_parallel, canonical_audit_bundle_bytes,
    verify_audit_bundle_with_ledger_public_key, AcceptedShare, AuditBundle, FoundBlock,
    Parallelism, PayoutPolicy, WindowCut,
};
use qbit_prism_server::ledger::{
    audit_canonical_bytes, Candidate, CandidateClaim, Ledger, SignerKeys, Snapshot, WindowRef,
};
use qbit_prism_server::node_identity::{NodeIdentity, NodeIndex};
use qbit_prism_test_gate as gate;
use sha2::{Digest, Sha256};

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// A fresh database and a node-0 ledger taking window cuts on it.
async fn open() -> Result<Option<(FixtureDatabase, Ledger)>> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(None);
    };
    let fixture = FixtureDatabase::open(&raw, "prism_dual_writer_").await?;
    let ledger = match Ledger::connect(&fixture.url, "dual-writer-a".into(), 8, true).await {
        Ok(ledger) => ledger,
        Err(error) => return Err(fixture.abandon(error).await),
    };
    let identity = NodeIdentity {
        node: NodeIndex::A,
        carry_owner: true,
    };
    let ready = async {
        ledger.set_dual_writer_identity(identity)?;
        // Migration 031's (origin_node, share_seq) index, which a dual-writer
        // snapshot requires, where this branch's migrations do not create it
        // yet.
        let indexed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] \
             JOIN pg_attribute b ON b.attrelid=i.indrelid AND b.attnum=i.indkey[1] \
             WHERE i.indrelid='qbit_share_ledger'::regclass AND i.indisvalid AND a.attname='origin_node' AND b.attname='share_seq')",
        )
        .fetch_one(&ledger.pool)
        .await?;
        if !indexed {
            sqlx::query("CREATE INDEX qbit_share_ledger_origin_seq_until_031 ON qbit_share_ledger (origin_node, share_seq)")
                .execute(&ledger.pool)
                .await?;
        }
        anyhow::Ok(())
    }
    .await;
    if let Err(error) = ready {
        ledger.pool.close().await;
        return Err(fixture.abandon(error).await);
    }
    Ok(Some((fixture, ledger)))
}

async fn close(fixture: FixtureDatabase, ledger: Ledger, result: Result<()>) -> Result<()> {
    ledger.pool.close().await;
    fixture.close(result).await
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn ledger_public_key() -> String {
    keys().1.public_key_hex()
}

/// A stamp well before any anchor a snapshot takes now.
const PAST_MS: i64 = 1_700_000_000_000;

/// Rows of `node` at these `share_seq` values, difficulty 1, stamped in the
/// past, as an append or the peer sync leaves them.
async fn rows(ledger: &Ledger, node: i16, seqs: impl IntoIterator<Item = i64>) -> Result<()> {
    for seq in seqs {
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch,origin_node)
             VALUES($1,'n'||$2::text||':'||lpad(to_hex($1),64,'0'),'miner-'||$2::text,'miner',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(($3-5)::double precision/1000),1800000000,to_timestamp($3::double precision/1000),true,'fixture',0,$2)",
        )
        .bind(seq)
        .bind(node)
        .bind(PAST_MS + seq)
        .execute(&ledger.pool)
        .await?;
    }
    Ok(())
}

/// The peer sync's high-water mark for node `peer`'s shares.
async fn mark(ledger: &Ledger, peer: i16, through: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_prism_peer_sync_cursors(stream,peer_node,scanned_through,ingested_through) VALUES('shares',$1,$2,$2)
         ON CONFLICT(stream) DO UPDATE SET scanned_through=EXCLUDED.scanned_through,ingested_through=EXCLUDED.ingested_through",
    )
    .bind(peer)
    .bind(through)
    .execute(&ledger.pool)
    .await?;
    Ok(())
}

/// A candidate and the bundle it was built from, as `audit_body_normalization`
/// keeps them; the bundle carries the snapshot's cut in its reward manifest.
struct TestCandidate {
    candidate: Candidate,
    bundle: AuditBundle,
}

fn candidate(
    snapshot: &Snapshot,
    network: u128,
    bootstrap: Option<AcceptedShare>,
    nonce: u32,
) -> Result<TestCandidate> {
    let (coinbase_key, ledger_key) = keys();
    let shares = match &bootstrap {
        Some(share) => vec![share.clone()],
        None => snapshot.shares.clone(),
    };
    let body = build_audit_bundle_body_with_coinbase_options_parallel(
        &shares,
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: network,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        // A bootstrap window's one share is synthetic: its bundle has no cut.
        if bootstrap.is_some() {
            None
        } else {
            snapshot.cut
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        Some("00".repeat(12)),
        Vec::new(),
        &coinbase_key,
        &ledger_key,
        Parallelism::serial(),
    )?;
    let bundle = body.into_bundle(shares);
    let report = verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_public_key())?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x2000_0000u32.to_le_bytes());
    block[4..36].fill(0x22);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
    block[72..76].copy_from_slice(&0x207f_ffffu32.to_le_bytes());
    block[76..80].copy_from_slice(&nonce.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&block)).to_vec();
    hash.reverse();
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    let candidate = Candidate {
        block_hash: hex::encode(hash),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "dual-writer-job".into(),
        payout_revision: snapshot.payout_revision,
        window: WindowRef::from_snapshot(snapshot)?,
        bootstrap_share: bootstrap,
        found_block: bundle.found_block.clone(),
        payout_policy: bundle.payout_policy.clone(),
        ctv: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&coinbase_key, &ledger_key),
        leased: false,
        coinbase_suffix_hex: "00".repeat(12),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: Vec::new(),
    };
    Ok(TestCandidate { candidate, bundle })
}

async fn enqueue_and_claim(ledger: &Ledger, block: &TestCandidate) -> Result<CandidateClaim> {
    ledger.enqueue_candidate(block.candidate.clone()).await?;
    let claim = ledger
        .claim_candidate(60)
        .await?
        .context("no pending candidate to claim")?;
    ensure!(
        claim.candidate.block_hash == block.candidate.block_hash,
        "claimed another candidate"
    );
    Ok(claim.with_bundle(block.bundle.clone()))
}

/// The landed block's audit, read back through reconstruction, is the bundle
/// its coinbase committed to, byte for byte.
async fn audit_reads_back(ledger: &Ledger, block: &TestCandidate) -> Result<()> {
    let canonical = canonical_audit_bundle_bytes(&block.bundle)?;
    let served = audit_canonical_bytes(&ledger.pool, &block.candidate.block_hash)
        .await?
        .context("landed block has no audit")?;
    ensure!(served == canonical, "reconstructed audit bytes differ");
    let body = ledger
        .audit_bundle(&block.candidate.block_hash)
        .await?
        .context("landed block has no audit body")?;
    ensure!(
        body["reward_manifest"]["cut"] == serde_json::to_value(block.bundle.reward_manifest.cut)?,
        "reconstructed audit lost its cut"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_block_lands_after_late_peer_rows_arrive_inside_its_window() -> Result<()> {
    let Some((fixture, ledger)) = open().await? else {
        return Ok(());
    };
    let result = async {
        // Node 0 is ahead; node 1's sequence trails it.
        rows(&ledger, 0, (1..=40).map(|i| 2 * i)).await?;
        rows(&ledger, 1, [1, 7, 15]).await?;
        mark(&ledger, 1, 15).await?;
        let network = 4;
        let snapshot = ledger.snapshot(network).await?;
        let cut = snapshot.cut.context("dual-writer snapshot has no cut")?;
        assert_eq!(cut, WindowCut::new(Some(80), Some(15))?);
        let first = snapshot.shares[0].share_seq as i64;
        let block = candidate(&snapshot, network, None, 1)?;
        assert_eq!(block.bundle.reward_manifest.cut, Some(cut));
        // Late rows before the enqueue: the #619 holding probe still holds.
        rows(&ledger, 1, [first + 1, first + 3]).await?;
        mark(&ledger, 1, first + 3).await?;
        let claim = enqueue_and_claim(&ledger, &block).await?;
        // More late rows between the claim and the landing: inside the range,
        // under its first row and above its top.
        rows(&ledger, 1, [first + 7, 79, 81]).await?;
        mark(&ledger, 1, 81).await?;
        ledger.land_candidate(&claim, &ledger_public_key()).await?;
        audit_reads_back(&ledger, &block).await?;
        // The next window holds them.
        let next = ledger.snapshot(network).await?;
        assert_eq!(next.cut, Some(WindowCut::new(Some(80), Some(81))?));
        assert!(next.shares.iter().any(|share| share.share_seq == 81));
        Ok(())
    }
    .await;
    close(fixture, ledger, result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dual_writer_bootstrap_block_lands_after_peer_rows_arrive() -> Result<()> {
    let Some((fixture, ledger)) = open().await? else {
        return Ok(());
    };
    let result = async {
        // A pool's first block on an empty ledger: the window is empty and its
        // cut admits nothing.
        let network = 100;
        let snapshot = ledger.snapshot(network).await?;
        assert!(snapshot.shares.is_empty());
        assert_eq!(snapshot.cut, Some(WindowCut::default()));
        let bootstrap = AcceptedShare {
            share_seq: 1,
            share_id: "bootstrap-share".into(),
            miner_id: "miner".into(),
            order_key: "miner".into(),
            p2mr_program_hex: "11".repeat(32),
            share_difficulty: network,
            network_difficulty: network,
            template_height: 100,
            job_id: "bootstrap-job".into(),
            job_issued_at_ms: snapshot.anchor_ms,
            accepted_at_ms: snapshot.anchor_ms,
            ntime: 1_800_000_000,
            credit_policy: None,
        };
        let block = candidate(&snapshot, network, Some(bootstrap), 2)?;
        assert_eq!(block.bundle.reward_manifest.cut, None);
        let claim = enqueue_and_claim(&ledger, &block).await?;
        // The peer's first rows reach this node, stamped before the anchor.
        // A timestamp-only proof would now find the bootstrap window
        // incomplete; the cut says it saw none of them.
        rows(&ledger, 1, [1, 3, 5]).await?;
        mark(&ledger, 1, 5).await?;
        ledger.land_candidate(&claim, &ledger_public_key()).await?;
        audit_reads_back(&ledger, &block).await?;
        Ok(())
    }
    .await;
    close(fixture, ledger, result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_whose_cut_disagrees_with_its_bundle_is_never_landed() -> Result<()> {
    let Some((fixture, ledger)) = open().await? else {
        return Ok(());
    };
    let result = async {
        rows(&ledger, 0, (1..=20).map(|i| 2 * i)).await?;
        rows(&ledger, 1, [3, 9]).await?;
        mark(&ledger, 1, 9).await?;
        let network = 2;
        let snapshot = ledger.snapshot(network).await?;
        // A bundle built without the window's cut pays the same shares, but
        // commits to a different manifest than the reference names.
        let mut uncut = snapshot.clone();
        uncut.cut = None;
        let mut block = candidate(&uncut, network, None, 3)?;
        block.candidate.window = WindowRef::from_snapshot(&snapshot)?;
        let claim = enqueue_and_claim(&ledger, &block).await?;
        let error = ledger
            .land_candidate(&claim, &ledger_public_key())
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("window cut"), "{error:#}");
        Ok(())
    }
    .await;
    close(fixture, ledger, result).await
}

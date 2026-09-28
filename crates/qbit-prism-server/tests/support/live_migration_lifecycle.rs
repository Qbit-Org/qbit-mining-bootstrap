//! #487 S20 (#545): the 2.x.x-to-native migration lifecycle on a real node.
//!
//! A 2.x.x ledger, built from the frozen `schema_2x` release SQL and scaled
//! up from the three rows `migration_rollback` seeds, goes through the whole
//! of `docs/prism-rust-migration.md#recovery-and-rollback`, the one-way
//! migration with isolated-restore reconciliation that #287 settled:
//!
//! 1. the drained source's evidence and a `pg_dump` backup (step 2);
//! 2. the backup restored into an isolated database, reconciling exactly
//!    with the source before any native acknowledgement (step 3);
//! 3. `migrate` and `import-audits` on the source, reconciling exactly again
//!    and serving every historical artifact byte for byte (steps 4 and 5);
//! 4. both native servers started on the migrated ledger, mining on the real
//!    node until their own blocks land, with the history untouched and
//!    continued: sequences, publication order and carry integrity (step 7's
//!    `self-check` included);
//! 5. the post-ACK boundary (step 6): the runbook's tail query names exactly
//!    the native shares, the isolated restore still equals the source and
//!    holds none of them, and the legacy revert refuses the native schema.
//!
//! The 2.x.x history lives on the regtest chain the servers mine: every
//! legacy pool block is a real block at its height, mature under the tip, so
//! the native observer checks the latest of them against the node as it
//! would in production, where a mature block missing from the chain is a
//! fatal state. #291's rehearsal stays the authority on production-sized
//! data; this is the repeatable version, run weekly.
use super::*;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy};
use rand::{rngs::StdRng, Rng, SeedableRng};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Write, path::Path};

#[path = "recovery.rs"]
#[allow(dead_code)]
mod recovery;

/// The legacy history: distinct payout recipients, ledger rows (a few of
/// them rejected, as 2.x.x recorded rejects in the same table) and found
/// blocks, each with its own signed audit bundle and carry-forward rows.
const LEGACY_RECIPIENTS: usize = 250;
const LEGACY_SHARES: usize = 60_000;
const LEGACY_BLOCKS: usize = 36;
/// Every this-many ledger rows is a 2.x.x reject.
const REJECT_EVERY: usize = 97;
/// Legacy pool blocks sit this many heights apart on the regtest chain.
const LEGACY_SPACING: u64 = 5;
/// The regtest coinbase maturity: the tip is mined this far past the last
/// legacy block so all of them are mature, as a drained 2.x.x pool's are.
const MATURITY: u64 = 100;
/// The 2.x.x network difficulty the legacy shares and blocks record.
const LEGACY_NETWORK_DIFFICULTY: u128 = 100;
/// A candidate balance at or above this pays on chain; below it accrues.
const LEGACY_FLOOR_SATS: u64 = 40_000;
/// Sats credited per unit of legacy share difficulty in a block.
const SATS_PER_DIFFICULTY: u64 = 500;
/// The high-water mark 2.x.x's share sequence ends at, above the last row:
/// allocations that rolled back still advance a PostgreSQL sequence.
const SEQUENCE_HEADROOM: i64 = 11;
/// Native pool blocks each server must land before the post-ACK checks.
const NATIVE_BLOCKS: i64 = 3;
/// The live servers' signing keys (`PRISM_ALLOW_TEST_SIGNING_SEEDS`). The
/// legacy history is signed with the same keys, as a cutover keeps them.
const COINBASE_SEED: &str = "11";
const LEDGER_SEED: &str = "22";

/// The 2.x.x columns of `qbit_share_ledger`, which native migrations extend:
/// the legacy prefix is compared on exactly these, before and after.
const LEGACY_SHARE_COLUMNS: &str = "share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,reject_reason,writer_id,writer_epoch,credit_policy";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "weekly: a scaled 2.x.x ledger through migration, mining and rollback; run with --ignored"]
async fn weekly_2x_ledger_migrates_mines_reconciles_and_restores_in_isolation() -> Result<()> {
    let Some(mut inputs) = gate::inputs(
        gate::site!(),
        &[
            gate::Input::QbitdBin,
            gate::Input::DatabaseUrl,
            gate::Input::PgBinDir,
        ],
    )?
    else {
        return Ok(());
    };
    let pg_bin = PathBuf::from(inputs.remove(2));
    let database = inputs.remove(1);
    let qbitd = inputs.remove(0);
    let mut fixture = Fixture::open_with_inputs(qbitd, &database, false, false).await?;
    let restored = recovery::Database::open(&database).await;
    let result = match &restored {
        Ok(restored) => run(&mut fixture, restored, &pg_bin).await,
        Err(error) => Err(anyhow::anyhow!("isolated restore database: {error:#}")),
    };
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    let restored = match restored {
        Ok(restored) => restored.close().await,
        Err(_) => Ok(()),
    };
    result.and(cleanup).and(restored)
}

/// The phases' wall times, printed as the rehearsal records them.
#[derive(Default)]
struct Timings(Vec<(&'static str, Duration)>);
impl Timings {
    async fn time<T>(&mut self, phase: &'static str, work: impl Future<Output = T>) -> T {
        let started = Instant::now();
        let value = work.await;
        self.0.push((phase, started.elapsed()));
        value
    }
}

async fn run(fixture: &mut Fixture, restored: &recovery::Database, pg_bin: &Path) -> Result<()> {
    let mut timings = Timings::default();
    let artifacts_dir = tempfile::tempdir()?;
    let legacy = timings
        .time(
            "seed 2.x.x",
            seed_legacy_history(fixture, artifacts_dir.path()),
        )
        .await?;
    let source = recovery::Database {
        admin: fixture.admin.clone(),
        pool: fixture.pool.clone(),
        schema: fixture.schema.clone(),
        url: fixture.database_url.clone(),
    };

    // Step 2: the drained source baseline and its backup.
    let source_evidence = timings
        .time("source evidence", recovery::evidence(&source, pg_bin))
        .await?;
    ensure!(source_evidence["unfinished_candidates"] == 0);
    ensure!(source_evidence["carry_forward_integrity"]["mismatch_count"] == 0);
    ensure!(source_evidence["accepted_shares"] == legacy.accepted_shares);
    ensure!(source_evidence["last_share_seq"] == legacy.last_share_seq);
    ensure!(source_evidence["records"]["audits"]["count"] == LEGACY_BLOCKS);
    let legacy_rows = legacy_snapshot(&fixture.pool, &legacy).await?;
    let archive = timings
        .time("pg_dump", recovery::backup(&source, pg_bin))
        .await?;

    // Step 3: the pre-ACK recovery path, an isolated restore of the backup.
    timings
        .time(
            "isolated restore",
            recovery::restore(&archive, &source, restored, pg_bin),
        )
        .await?;
    ensure!(
        recovery::evidence(restored, pg_bin).await? == source_evidence,
        "the isolated restore does not reconcile with the drained source"
    );
    ensure!(
        native_tables_absent(&restored.pool).await?,
        "the isolated restore must be the pre-migration database"
    );

    // Steps 4 and 5: forward migration and the audit import, reconciled
    // before any native traffic, and canonical reads of every artifact.
    let migrated = timings.time("migrate", fixture.tool(&["migrate"])).await?;
    ensure!(
        migrated.contains("database source:") && !migrated.contains("unrecorded"),
        "migrate did not record the 2.x.x source: {migrated}"
    );
    let imported = timings
        .time(
            "import-audits",
            fixture.tool(&[
                "import-audits",
                "--root",
                &artifacts_dir.path().to_string_lossy(),
            ]),
        )
        .await?;
    ensure!(
        imported.contains(&format!("Imported {LEGACY_BLOCKS} audit bodies"))
            && imported.contains("\"missing_stored_bodies\":0")
            && imported.contains("\"missing_canonical_bytes\":0"),
        "import-audits left history incomplete: {imported}"
    );
    ensure!(
        recovery::evidence(&source, pg_bin).await? == source_evidence,
        "migration and import changed the reconciled history"
    );
    // Every original external file is now unavailable: public reads must
    // come from PostgreSQL.
    artifacts_dir.close()?;
    recovery::assert_artifacts(&fixture.pool, &legacy.artifacts).await?;
    ensure!(legacy_snapshot(&fixture.pool, &legacy).await? == legacy_rows);

    // The native servers on the migrated ledger, mining on the real node.
    let mining = Instant::now();
    for index in 0..2 {
        let process = fixture.start_server(index)?;
        fixture.servers.push(process);
    }
    for index in 0..2 {
        until(
            &format!("PRISM HTTP readiness of server {index}"),
            60,
            || async {
                Ok(fixture
                    .client
                    .get(format!("http://127.0.0.1:{}/healthz", fixture.api[index]))
                    .send()
                    .await?
                    .status()
                    .is_success())
            },
        )
        .await?;
    }
    fixture.start_miner(0)?;
    fixture.start_miner(1)?;
    until("native pool blocks from both servers", 90, || async {
        let landed: Vec<i64> = sqlx::query_scalar(
            "SELECT count(DISTINCT b.block_hash) FROM qbit_pool_blocks b JOIN qbit_block_candidate_outbox o USING(block_hash) JOIN qbit_share_ledger s USING(share_id) WHERE b.chain_state='confirmed' AND s.writer_id=ANY($1) GROUP BY s.writer_id",
        )
        .bind(["live-0", "live-1"])
        .fetch_all(&fixture.pool)
        .await?;
        Ok(landed.len() == 2 && landed.iter().all(|count| *count >= NATIVE_BLOCKS))
    })
    .await?;
    fixture.quiesce().await?;
    timings.0.push(("native mining", mining.elapsed()));
    fixture.integrity().await?;
    // The carry chain crosses the cutover: native blocks pay 2.x.x
    // recipients on top of the balances 2.x.x left them, and the integrity
    // report above replays that chain across both eras.
    let continued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_payout_carry_forward WHERE carry_forward_seq>$1 AND miner_id LIKE 'legacy-%' AND prior_balance_sats<>0",
    )
    .bind(legacy.last_carry_seq)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        continued > 0,
        "no native carry row continued a 2.x.x balance"
    );

    // The history is untouched and continued.
    ensure!(
        legacy_snapshot(&fixture.pool, &legacy).await? == legacy_rows,
        "native mining changed a 2.x.x row"
    );
    let native = native_blocks(fixture, &legacy).await?;
    ensure!(!native.is_empty(), "no native pool block confirmed");
    let legacy_last_ordinal = legacy.last_publication_ordinal;
    for (hash, ordinal) in &native {
        ensure!(
            ordinal.is_some_and(|ordinal| ordinal > legacy_last_ordinal),
            "native block {hash} did not continue the 2.x.x publication order: {ordinal:?} after {legacy_last_ordinal}"
        );
        verify_native_audit(fixture, hash).await?;
    }
    let self_check = timings
        .time("self-check", fixture.tool(&["self-check"]))
        .await?;
    let self_check: Value = serde_json::from_str(&self_check)?;
    ensure!(
        self_check["audit_completeness"]
            == json!({"missing_stored_bodies": 0, "missing_canonical_bytes": 0}),
        "self-check reports incomplete history: {self_check}"
    );

    // Step 6: the post-ACK boundary, from the runbook's own tail query.
    let tail: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT share_seq, share_id, writer_id FROM qbit_share_ledger WHERE accepted AND share_seq > $1 ORDER BY share_seq",
    )
    .bind(legacy.last_share_seq)
    .fetch_all(&fixture.pool)
    .await?;
    let native_accepted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_share_ledger WHERE accepted AND writer_id=ANY($1)",
    )
    .bind(["live-0", "live-1"])
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        !tail.is_empty() && tail.len() as i64 == native_accepted,
        "the post-ACK tail is not exactly the native shares: {} rows, {native_accepted} native",
        tail.len()
    );
    ensure!(
        tail.iter().all(
            |(seq, _, writer)| *seq > legacy.last_share_seq + SEQUENCE_HEADROOM
                && (writer == "live-0" || writer == "live-1")
        ),
        "a native share reused the 2.x.x sequence range or has another writer"
    );
    let current_evidence = recovery::evidence(&source, pg_bin).await?;
    ensure!(
        current_evidence["accepted_shares"] == legacy.accepted_shares + native_accepted,
        "evidence does not count the native tail"
    );
    ensure!(current_evidence["carry_forward_integrity"]["mismatch_count"] == 0);
    ensure!(current_evidence["records"]["shares"] != source_evidence["records"]["shares"]);

    // The rollback after the ACK: the isolated restore is still exactly the
    // source, and every native record is what restoring it would discard.
    ensure!(
        recovery::evidence(restored, pg_bin).await? == source_evidence,
        "the isolated restore drifted while the native servers ran"
    );
    let tail_ids: Vec<&str> = tail.iter().map(|(_, id, _)| id.as_str()).collect();
    let survived: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=ANY($1)")
            .bind(&tail_ids)
            .fetch_one(&restored.pool)
            .await?;
    ensure!(survived == 0, "the older restore holds post-cutover shares");
    let native_hashes: Vec<&str> = native.iter().map(|(hash, _)| hash.as_str()).collect();
    let restored_blocks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash=ANY($1)")
            .bind(&native_hashes)
            .fetch_one(&restored.pool)
            .await?;
    ensure!(
        restored_blocks == 0,
        "the older restore holds native blocks"
    );
    assert_legacy_revert_refuses(fixture).await?;

    let phases: Vec<String> = timings
        .0
        .iter()
        .map(|(phase, took)| format!("{phase} {:.1}s", took.as_secs_f64()))
        .collect();
    eprintln!(
        "live migration lifecycle: {} legacy shares ({} accepted) and {LEGACY_BLOCKS} blocks for {LEGACY_RECIPIENTS} recipients reconciled through migrate and import; {} native shares and {} native blocks after cutover are exactly the records an isolated restore discards; {}",
        LEGACY_SHARES,
        legacy.accepted_shares,
        tail.len(),
        native.len(),
        phases.join(", ")
    );
    Ok(())
}

/// What the seed wrote that later phases check against.
struct Legacy {
    accepted_shares: i64,
    last_share_seq: i64,
    last_publication_ordinal: i64,
    last_carry_seq: i64,
    hashes: Vec<String>,
    artifacts: Vec<recovery::Artifact>,
}

/// Mines the 2.x.x-era chain, then writes a drained 2.x.x ledger over it:
/// the frozen release DDL, ordered shares from a skewed recipient set with
/// rejects among them, a signed audit bundle per found block in the three
/// historical body layouts, chained carry-forward rows, and terminal
/// candidate rows.
async fn seed_legacy_history(fixture: &Fixture, root: &Path) -> Result<Legacy> {
    let tip = fixture
        .rpc("getblockcount", json!([]))
        .await?
        .as_u64()
        .context("block count missing")?;
    let first = tip + 1;
    let last = first + (LEGACY_BLOCKS as u64 - 1) * LEGACY_SPACING;
    let mined = last - tip + MATURITY;
    fixture
        .rpc("generatetoaddress", json!([mined, fixture.address]))
        .await?;
    let mut blocks = Vec::with_capacity(LEGACY_BLOCKS);
    for index in 0..LEGACY_BLOCKS as u64 {
        let height = first + index * LEGACY_SPACING;
        let hash = fixture
            .rpc("getblockhash", json!([height]))
            .await?
            .as_str()
            .context("block hash missing")?
            .to_owned();
        let header = fixture.rpc("getblockheader", json!([hash])).await?;
        let parent = header["previousblockhash"]
            .as_str()
            .context("parent hash missing")?
            .to_owned();
        blocks.push((height, hash, parent));
    }

    sqlx::raw_sql(include_str!("../fixtures/schema_2x/001_share_ledger.sql"))
        .execute(&fixture.pool)
        .await?;
    sqlx::raw_sql(include_str!(
        "../fixtures/schema_2x/002_candidate_bodies.sql"
    ))
    .execute(&fixture.pool)
    .await?;

    let recipients: Vec<(String, String)> = (0..LEGACY_RECIPIENTS)
        .map(|index| {
            let program = hex::encode(Sha256::digest(format!("s20-legacy-recipient-{index}")));
            (format!("legacy-{index:03}"), program)
        })
        .collect();
    // Zipf-like weights: a few recipients hold most of the work.
    let weights: Vec<f64> = (1..=LEGACY_RECIPIENTS)
        .map(|rank| 1.0 / rank as f64)
        .collect();
    let total_weight: f64 = weights.iter().sum();
    let mut rng = StdRng::seed_from_u64(0x0545_0020);
    let now_ms = chrono::Utc::now().timestamp_millis();
    // Three days of history, ending an hour before the cutover.
    let start_ms = now_ms - 3 * 86_400_000;
    let step_ms = (3 * 86_400_000 - 3_600_000) / LEGACY_SHARES as i64;
    // The first `LEGACY_SHARES % LEGACY_BLOCKS` blocks take one more row, so
    // every configured row is written.
    let per_block = LEGACY_SHARES / LEGACY_BLOCKS;
    let longer_blocks = LEGACY_SHARES % LEGACY_BLOCKS;

    let coinbase_key = ManifestSigningKey::from_seed_hex(&COINBASE_SEED.repeat(32))?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&LEDGER_SEED.repeat(32))?;
    let mut balances: BTreeMap<usize, u64> = BTreeMap::new();
    let mut artifacts = Vec::with_capacity(LEGACY_BLOCKS);
    let mut accepted_shares = 0i64;
    let mut seq = 0u64;
    for (block_index, (height, hash, parent)) in blocks.iter().enumerate() {
        let block_shares = per_block + usize::from(block_index < longer_blocks);
        let mut window: Vec<AcceptedShare> = Vec::with_capacity(block_shares);
        let mut credited: BTreeMap<usize, u64> = BTreeMap::new();
        let mut transaction = fixture.pool.begin().await?;
        for _ in 0..block_shares {
            seq += 1;
            let mut pick = rng.gen::<f64>() * total_weight;
            let recipient = weights
                .iter()
                .position(|weight| {
                    pick -= weight;
                    pick <= 0.0
                })
                .unwrap_or(LEGACY_RECIPIENTS - 1);
            let (miner, program) = &recipients[recipient];
            let difficulty: u128 = rng.gen_range(1..=4);
            let accepted_at_ms = start_ms + seq as i64 * step_ms;
            let share = AcceptedShare {
                share_seq: seq,
                share_id: format!("legacy:{seq:064x}"),
                miner_id: miner.clone(),
                order_key: miner.clone(),
                p2mr_program_hex: program.clone(),
                share_difficulty: difficulty,
                network_difficulty: LEGACY_NETWORK_DIFFICULTY,
                template_height: *height,
                job_id: format!("legacy-job-{block_index}"),
                job_issued_at_ms: accepted_at_ms - 1_000,
                accepted_at_ms,
                ntime: u32::try_from(accepted_at_ms / 1_000)?,
                credit_policy: None,
            };
            let rejected = (seq as usize).is_multiple_of(REJECT_EVERY);
            sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,accepted_at,ntime,accepted,reject_reason,writer_id,writer_epoch) VALUES($1,$2,$3,$3,decode($4,'hex'),$5::numeric,$6::numeric,$7,$8,to_timestamp($9::double precision/1000),to_timestamp($10::double precision/1000),$11,$12,$13,'python',7)")
                .bind(seq as i64)
                .bind(&share.share_id)
                .bind(miner)
                .bind(program)
                .bind(difficulty.to_string())
                .bind(LEGACY_NETWORK_DIFFICULTY.to_string())
                .bind(*height as i64)
                .bind(&share.job_id)
                .bind(share.job_issued_at_ms)
                .bind(share.accepted_at_ms)
                .bind(i64::from(share.ntime))
                .bind(!rejected)
                .bind(rejected.then_some("stale-job"))
                .execute(&mut *transaction)
                .await?;
            if !rejected {
                accepted_shares += 1;
                *credited.entry(recipient).or_default() += difficulty as u64 * SATS_PER_DIFFICULTY;
                window.push(share);
            }
        }
        let found_share = window
            .last()
            .context("a legacy block without shares")?
            .clone();
        let bundle = qbit_prism::build_audit_bundle(
            window,
            FoundBlock {
                block_height: *height,
                coinbase_value_sats: 5_000_000_000,
                network_difficulty: LEGACY_NETWORK_DIFFICULTY,
                anchor_job_issued_at_ms: found_share.job_issued_at_ms,
            },
            vec![],
            PayoutPolicy::day_one_default(),
            &coinbase_key,
            &ledger_key,
        )?;
        let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
            &bundle,
            &ledger_key.public_key_hex(),
        )?;
        let canonical = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
        // The three historical body layouts, in turn: a canonical sidecar
        // only, an external body file, and an inline body.
        let body_path = root.join(format!("legacy-audit-{hash}.json"));
        let (inline, body_uri) = match block_index % 3 {
            0 => {
                let path = root.join(format!(
                    "prism-audit-bundle-canonical-{hash}-{}.json.gz",
                    report.audit_bundle_sha256_hex
                ));
                let mut encoder = flate2::GzBuilder::new()
                    .mtime(0)
                    .write(File::create(path)?, flate2::Compression::best());
                encoder.write_all(&canonical)?;
                encoder.finish()?;
                (None, Some(body_path.to_string_lossy().into_owned()))
            }
            1 => {
                std::fs::write(&body_path, serde_json::to_vec(&bundle)?)?;
                (None, Some(body_path.to_string_lossy().into_owned()))
            }
            _ => (Some(serde_json::to_value(&bundle)?), None),
        };
        let found_at_ms = found_share.accepted_at_ms;
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,found_at,chain_state,maturity_state,matured_at) VALUES($1,$2,$3,$4,$5,to_timestamp($6::double precision/1000),'confirmed','mature',clock_timestamp())")
            .bind(hash)
            .bind(*height as i64)
            .bind(parent)
            .bind(&report.coinbase_txid)
            .bind(&report.coinbase_manifest_sha256_hex)
            .bind(found_at_ms)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,body_uri) VALUES($1,$2,$3,$4,$5)")
            .bind(hash)
            .bind(inline)
            .bind(&report.audit_bundle_sha256_hex)
            .bind(&report.coinbase_tx_hex)
            .bind(body_uri)
            .execute(&mut *transaction)
            .await?;
        // Each credited recipient's carry chain: the prior balance is the
        // last carry, and a candidate at the floor pays out whole.
        for (recipient, gross) in credited {
            let (miner, program) = &recipients[recipient];
            let prior = balances.get(&recipient).copied().unwrap_or(0);
            let candidate = prior + gross;
            let onchain = if candidate >= LEGACY_FLOOR_SATS {
                candidate
            } else {
                0
            };
            let carry = candidate - onchain;
            balances.insert(recipient, carry);
            sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state) VALUES($1,$2,$3,$3,decode($4,'hex'),$5,$6::numeric,$7::numeric,$8,$9::numeric,$10,'mature')")
                .bind(*height as i64)
                .bind(hash)
                .bind(miner)
                .bind(program)
                .bind(i64::try_from(gross)?)
                .bind(prior.to_string())
                .bind(candidate.to_string())
                .bind(i64::try_from(onchain)?)
                .bind(carry.to_string())
                .bind(if onchain > 0 { "onchain" } else { "accrued" })
                .execute(&mut *transaction)
                .await?;
        }
        // The found block's candidate, drained by the 2.x.x submitter.
        sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,share_id,candidate,candidate_sha256,state,attempt_count,completed_at) VALUES($1,$2,NULL,$3,'submitted',1,clock_timestamp())")
            .bind(hash)
            .bind(&found_share.share_id)
            .bind(hex::encode(Sha256::digest(&canonical)))
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        artifacts.push(recovery::Artifact {
            block_hash: hash.clone(),
            digest: report.audit_bundle_sha256_hex,
            canonical,
        });
    }
    // One candidate 2.x.x abandoned, whose block never reached the chain.
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,last_error,completed_at) VALUES($1,NULL,$2,'abandoned','stale before submission',clock_timestamp())")
        .bind(hex::encode(Sha256::digest("s20-abandoned-candidate")))
        .bind("88".repeat(32))
        .execute(&fixture.pool)
        .await?;
    ensure!(
        seq as usize == LEGACY_SHARES,
        "seeded {seq} ledger rows, not {LEGACY_SHARES}"
    );
    let last_share_seq = seq as i64;
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
        .bind(last_share_seq + SEQUENCE_HEADROOM)
        .execute(&fixture.pool)
        .await?;
    let last_publication_ordinal: i64 =
        sqlx::query_scalar("SELECT max(audit_publication_sequence) FROM qbit_pool_blocks")
            .fetch_one(&fixture.pool)
            .await?;
    let last_carry_seq: i64 =
        sqlx::query_scalar("SELECT max(carry_forward_seq) FROM qbit_payout_carry_forward")
            .fetch_one(&fixture.pool)
            .await?;
    Ok(Legacy {
        accepted_shares,
        last_carry_seq,
        last_share_seq,
        last_publication_ordinal,
        hashes: blocks.into_iter().map(|(_, hash, _)| hash).collect(),
        artifacts,
    })
}

/// Every 2.x.x row a migration or native writer could touch, on the 2.x.x
/// columns only: the share prefix, the blocks with their publication order
/// and audit identity, and the carry chain.
async fn legacy_snapshot(pool: &PgPool, legacy: &Legacy) -> Result<Value> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT jsonb_build_object(\
          'shares',(SELECT jsonb_agg(to_jsonb(s) ORDER BY share_seq) FROM (SELECT {LEGACY_SHARE_COLUMNS} FROM qbit_share_ledger WHERE share_seq<=$1) s),\
          'blocks',(SELECT jsonb_agg(jsonb_build_array(b.block_hash,b.block_height,b.parent_hash,b.coinbase_txid,b.payout_manifest_sha256,b.chain_state,b.maturity_state,b.audit_publication_sequence,a.audit_bundle_sha256) ORDER BY b.block_height) FROM qbit_pool_blocks b JOIN qbit_pool_audit_bundles a USING(block_hash) WHERE b.block_hash=ANY($2)),\
          'carry',(SELECT jsonb_agg(jsonb_build_array(carry_forward_seq,block_height,block_hash,miner_id,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state) ORDER BY carry_forward_seq) FROM qbit_payout_carry_forward WHERE carry_forward_seq<=$3))"
    ))
    .bind(legacy.last_share_seq)
    .bind(&legacy.hashes)
    .bind(legacy.last_carry_seq)
    .fetch_one(pool)
    .await?)
}

/// The confirmed pool blocks the native servers found, with their
/// publication ordinals.
async fn native_blocks(fixture: &Fixture, legacy: &Legacy) -> Result<Vec<(String, Option<i64>)>> {
    Ok(sqlx::query_as(
        "SELECT block_hash, audit_publication_sequence FROM qbit_pool_blocks WHERE chain_state='confirmed' AND NOT block_hash=ANY($1) ORDER BY block_height",
    )
    .bind(&legacy.hashes)
    .fetch_all(&fixture.pool)
    .await?)
}

/// A native block's audit bundle, served by the public API, verifies against
/// the coinbase the node actually has.
async fn verify_native_audit(fixture: &Fixture, hash: &str) -> Result<()> {
    let body: Value = fixture
        .client
        .get(format!(
            "http://127.0.0.1:{}/audit/blocks/{hash}/bundle",
            fixture.api[1]
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let bundle: AuditBundle = serde_json::from_value(body["audit_bundle"].clone())?;
    let block = fixture.rpc("getblock", json!([hash, 2])).await?;
    let coinbase = fixture
        .rpc(
            "getrawtransaction",
            json!([block["tx"][0]["txid"], false, hash]),
        )
        .await?;
    let key = ManifestSigningKey::from_seed_hex(&LEDGER_SEED.repeat(32))?.public_key_hex();
    verify_audit_bundle_against_coinbase_tx_hex(
        &bundle,
        coinbase.as_str().context("node coinbase missing")?,
        &key,
    )?;
    Ok(())
}

async fn native_tables_absent(pool: &PgPool) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_migrations') IS NULL")
            .fetch_one(pool)
            .await?,
    )
}

/// 2.x.x's destructive publication-ordinal revert refuses the native schema
/// and names the one-way procedure, leaving the schema as it was.
async fn assert_legacy_revert_refuses(fixture: &Fixture) -> Result<()> {
    let mut connection = fixture.pool.acquire().await?;
    let error = sqlx::raw_sql(include_str!(
        "../../../qbit-prism/sql/001_share_ledger_revert_audit_publication_sequence.sql"
    ))
    .execute(&mut *connection)
    .await
    .err()
    .context("the legacy revert ran against the native schema")?;
    sqlx::query("ROLLBACK").execute(&mut *connection).await?;
    let message = error.to_string();
    ensure!(
        message.contains("one-way migration")
            && message.contains("docs/prism-rust-migration.md#recovery-and-rollback"),
        "the legacy revert refused without naming the procedure: {message}"
    );
    Ok(())
}

impl Fixture {
    /// Runs an operator subcommand of the server binary with server 0's
    /// settings, as the runbook runs it beside the frontends, and returns
    /// its standard output. The migrated ledger's writer key is the one the
    /// history was signed with.
    async fn tool(&self, args: &[&str]) -> Result<String> {
        let ledger_public_key =
            ManifestSigningKey::from_seed_hex(&LEDGER_SEED.repeat(32))?.public_key_hex();
        let mut command = self.server_command(
            0,
            None,
            &[("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", ledger_public_key)],
        );
        command.args(args).stdin(Stdio::null());
        let output = tokio::time::timeout(
            Duration::from_secs(300),
            tokio::process::Command::from(command).output(),
        )
        .await
        .with_context(|| format!("qbit-prism-server {args:?} did not finish in 300 s"))??;
        let stdout = String::from_utf8(output.stdout)?;
        ensure!(
            output.status.success(),
            "qbit-prism-server {args:?} failed with {}: {stdout}{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(stdout)
    }
}

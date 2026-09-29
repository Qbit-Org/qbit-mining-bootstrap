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
//! 3. `migrate` and `import-audits` rehearsed on a second isolated restore
//!    (step 4), then run on the source for the cutover, each reconciling
//!    exactly with the source and serving every historical artifact byte for
//!    byte (step 5);
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
use std::path::Path;

#[path = "recovery.rs"]
#[allow(dead_code)]
mod recovery;

// #575: the mainnet-shaped seed and the measured cutover rehearsal.
#[path = "cutover_rehearsal.rs"]
#[allow(dead_code)]
mod rehearsal;
use rehearsal::seed;

/// #545's legacy history (`seed::seed_s20`): distinct payout recipients,
/// ledger rows and found blocks.
const LEGACY_RECIPIENTS: usize = seed::S20_RECIPIENTS;
const LEGACY_SHARES: usize = seed::S20_SHARES;
const LEGACY_BLOCKS: usize = seed::S20_BLOCKS;
/// Legacy pool blocks sit this many heights apart on the regtest chain.
const LEGACY_SPACING: u64 = 5;
/// The regtest coinbase maturity: the tip is mined this far past the last
/// legacy block so all of them are mature, as a drained 2.x.x pool's are.
const MATURITY: u64 = 100;
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
    // Two isolated restores of the backup: one stays the pre-migration
    // database for the rollback, the other rehearses the migration.
    let restored = recovery::Database::open(&database).await;
    let rehearsal = recovery::Database::open(&database).await;
    let result = match (&restored, &rehearsal) {
        (Ok(restored), Ok(rehearsal)) => run(&mut fixture, restored, rehearsal, &pg_bin).await,
        (Err(error), _) | (_, Err(error)) => {
            Err(anyhow::anyhow!("isolated restore database: {error:#}"))
        }
    };
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    let mut closed = Ok(());
    for database in [restored, rehearsal].into_iter().flatten() {
        closed = closed.and(database.close().await);
    }
    result.and(cleanup).and(closed)
}

/// #575: the share density of the weekly mainnet-shaped ledger. At 1/16 it
/// holds about 4.1M of mainnet's 65.5M shares, each carrying 16 times a
/// share's difficulty; every block, address and payout row is mainnet's.
const WEEKLY_DENSITY: f64 = 1.0 / 16.0;
/// The statement timeout the weekly cutover's `migrate` runs with, the
/// highest the server accepts. At the default 15 s, 002's share-hash
/// backfill, one statement inside the migration transaction, times out on
/// this ledger (#582); drop this once #582 lets `migrate` run on defaults.
const WEEKLY_STATEMENT_TIMEOUT_MS: &str = "600000";
/// From about this many ledger rows, 002's backfill outlasts the default
/// 15 s statement timeout (#582: 26-38 s at 1.03M rows on a 22-core host).
const KNOWN_582_ROWS: u64 = 1_000_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "weekly: #575's mainnet-shaped 2.x.x ledger through the measured cutover, mining and rollback; run with --ignored"]
async fn weekly_mainnet_shaped_2x_ledger_cuts_over_measured_mines_and_restores_in_isolation(
) -> Result<()> {
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
    // A smaller density for a host without the weekly runner's disk.
    let density = match std::env::var("PRISM_REHEARSAL_WEEKLY_DENSITY") {
        Ok(value) => value
            .parse::<f64>()
            .context("PRISM_REHEARSAL_WEEKLY_DENSITY must be a number")?,
        Err(_) => WEEKLY_DENSITY,
    };
    let shape = seed::MainnetShape::mainnet(1, density)?;
    let mut fixture = Fixture::open_with_inputs(qbitd, &database, false, false).await?;
    let result = run_mainnet(&mut fixture, shape, &database, &pg_bin).await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
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

async fn run(
    fixture: &mut Fixture,
    restored: &recovery::Database,
    rehearsal: &recovery::Database,
    pg_bin: &Path,
) -> Result<()> {
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

    // Step 4: the forward migration and the audit import rehearsed on a
    // second isolated restore of the same backup, reconciled exactly, with
    // canonical reads of every artifact (step 5).
    timings
        .time(
            "rehearsal restore",
            recovery::restore(&archive, &source, rehearsal, pg_bin),
        )
        .await?;
    let artifacts_root = artifacts_dir.path().to_string_lossy().into_owned();
    timings
        .time(
            "rehearsal migrate and import",
            migrate_and_import(fixture, &rehearsal.url, &artifacts_root),
        )
        .await?;
    ensure!(
        recovery::evidence(rehearsal, pg_bin).await? == source_evidence,
        "the rehearsed migration and import changed the reconciled history"
    );
    recovery::assert_artifacts(&rehearsal.pool, &legacy.artifacts).await?;

    // The cutover: the same commands on the source, reconciled again before
    // any native traffic.
    timings
        .time(
            "migrate and import",
            migrate_and_import(fixture, &fixture.database_url, &artifacts_root),
        )
        .await?;
    ensure!(
        recovery::evidence(&source, pg_bin).await? == source_evidence,
        "migration and import changed the reconciled history"
    );
    // Every original external file is now unavailable: public reads must
    // come from PostgreSQL.
    artifacts_dir.close()?;
    recovery::assert_artifacts(&fixture.pool, &legacy.artifacts).await?;
    ensure!(legacy_snapshot(&fixture.pool, &legacy).await? == legacy_rows);

    let (tail, native) = mine_and_reconcile(
        fixture,
        restored,
        pg_bin,
        &legacy,
        &legacy_rows,
        &source_evidence,
        &mut timings,
        true,
        None,
    )
    .await?;

    let phases: Vec<String> = timings
        .0
        .iter()
        .map(|(phase, took)| format!("{phase} {:.1}s", took.as_secs_f64()))
        .collect();
    eprintln!(
        "live migration lifecycle: {} legacy shares ({} accepted) and {LEGACY_BLOCKS} blocks for {LEGACY_RECIPIENTS} recipients reconciled through migrate and import; {} native shares and {} native blocks after cutover are exactly the records an isolated restore discards; {}",
        LEGACY_SHARES,
        legacy.accepted_shares,
        tail,
        native,
        phases.join(", ")
    );
    Ok(())
}

/// The native servers on the migrated ledger, mining on the real node until
/// each lands its own blocks, then the history checks and the post-ACK
/// boundary against the isolated restore. Returns the native tail's share
/// and block counts.
#[allow(clippy::too_many_arguments)]
async fn mine_and_reconcile(
    fixture: &mut Fixture,
    restored: &recovery::Database,
    pg_bin: &Path,
    legacy: &Legacy,
    legacy_rows: &Value,
    source_evidence: &Value,
    timings: &mut Timings,
    expect_continued_carry: bool,
    api_balances: Option<&rehearsal::Balances>,
) -> Result<(usize, usize)> {
    let source = recovery::Database {
        admin: fixture.admin.clone(),
        pool: fixture.pool.clone(),
        schema: fixture.schema.clone(),
        url: fixture.database_url.clone(),
    };
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
    // Before any native share: every 2.x.x address's balances, as each
    // frontend's public API serves them, equal what 2.x.x computed.
    if let Some(before) = api_balances {
        for port in fixture.api {
            let served = rehearsal::api_balances(&fixture.client, port, before).await?;
            ensure!(
                &served == before,
                "the migrated API serves other balances than 2.x.x computed ({} addresses)",
                before.len()
            );
        }
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
    if expect_continued_carry {
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
    }

    // The history is untouched and continued.
    ensure!(
        &legacy_snapshot(&fixture.pool, legacy).await? == legacy_rows,
        "native mining changed a 2.x.x row"
    );
    let native = native_blocks(fixture, legacy).await?;
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
        .time(
            "self-check",
            fixture.tool(&fixture.database_url, &["self-check"]),
        )
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
            |(seq, _, writer)| *seq > legacy.last_share_seq + legacy.sequence_headroom
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
        recovery::evidence(restored, pg_bin).await? == *source_evidence,
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

    Ok((tail.len(), native.len()))
}

/// #575's weekly rehearsal: a mainnet-shaped 2.x.x ledger over a real
/// regtest chain, backed up and restored in isolation, cut over with the
/// measured `check-config`, `migrate` and `import-audits` and every
/// invariant of `rehearsal::rehearse`, then mined natively and reconciled
/// exactly as #545's scenario is.
async fn run_mainnet(
    fixture: &mut Fixture,
    shape: seed::MainnetShape,
    database: &str,
    pg_bin: &Path,
) -> Result<()> {
    let mut timings = Timings::default();
    let artifacts_dir = tempfile::tempdir()?;
    let work = tempfile::tempdir()?;
    let plan = seed::MainnetPlan::new(shape, chrono::Utc::now().timestamp_millis())?;
    let chain = timings
        .time("mine the 2.x.x chain", mine_plan_chain(fixture, &plan))
        .await?;
    let coinbase_key = ManifestSigningKey::from_seed_hex(&COINBASE_SEED.repeat(32))?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&LEDGER_SEED.repeat(32))?;
    let (seeded, summary) = timings
        .time(
            "seed 2.x.x",
            plan.write(
                &fixture.pool,
                artifacts_dir.path(),
                &chain,
                &coinbase_key,
                &ledger_key,
            ),
        )
        .await?;
    let mut legacy = Legacy::from(seeded);
    legacy.digest_rows = true;
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
    ensure!(source_evidence["accepted_shares"] == legacy.accepted_shares);
    ensure!(source_evidence["last_share_seq"] == legacy.last_share_seq);
    let legacy_rows = legacy_snapshot(&fixture.pool, &legacy).await?;
    let before_balances = rehearsal::balances(&fixture.pool).await?;
    let dump = work.path().join("source.dump");
    timings
        .time(
            "pg_dump",
            pg_dump_schema(database, &fixture.schema, pg_bin, &dump),
        )
        .await?;

    // Step 3: the pre-ACK recovery path, an isolated restore of the backup
    // into a database of its own.
    let (restored, restored_name) = timings
        .time(
            "isolated restore",
            restore_database(fixture, database, &dump, pg_bin),
        )
        .await?;
    let result = async {
        ensure!(
            recovery::evidence(&restored, pg_bin).await? == source_evidence,
            "the isolated restore does not reconcile with the drained source"
        );
        ensure!(
            native_tables_absent(&restored.pool).await?,
            "the isolated restore must be the pre-migration database"
        );

        // #582: 002's share-hash backfill is one statement inside the
        // migration transaction, so `migrate` at the default statement
        // timeout refuses a ledger of this size, and changes nothing. When
        // #582 is fixed this fails: drop it and WEEKLY_STATEMENT_TIMEOUT_MS.
        let started_582 = Instant::now();
        if summary["rows"].as_u64().unwrap_or(0) >= KNOWN_582_ROWS {
            let refused = fixture
                .tool(&fixture.database_url, &["migrate"])
                .await
                .err()
                .context("migrate at the default statement timeout succeeded: #582 looks fixed, so remove this check and WEEKLY_STATEMENT_TIMEOUT_MS")?;
            let refused = format!("{refused:#}");
            ensure!(
                refused.contains("canceling statement due to statement timeout"),
                "migrate at the default statement timeout failed for another reason than #582: {refused}"
            );
            ensure!(
                recovery::evidence(&source, pg_bin).await? == source_evidence
                    && native_tables_absent(&fixture.pool).await?,
                "the refused migrate changed the source"
            );
            timings.0.push(("#582 refusal at the default timeout", started_582.elapsed()));
        }

        // Steps 4-5 on the source itself, measured: check-config, migrate,
        // import-audits, then evidence, sums, balances and the window.
        let mut report = rehearsal::Report::new(summary);
        let target = rehearsal::Target {
            url: fixture.database_url.clone(),
            schema: fixture.schema.clone(),
            admin: fixture.admin.clone(),
            pool: fixture.pool.clone(),
        };
        let started = Instant::now();
        rehearsal::rehearse(
            &target,
            database,
            &rehearsal::Options {
                pg_bin: pg_bin.to_owned(),
                audit_root: artifacts_dir.path().to_owned(),
                ledger_public_key: ledger_key.public_key_hex(),
                node: rehearsal::NodeChoice::External {
                    url: format!("http://127.0.0.1:{}/", fixture.rpc_port),
                    user: "prismtest".into(),
                    password: "prismtest".into(),
                    chain: "regtest".into(),
                },
                start_frontend: false,
                extra_env: vec![(
                    "PRISM_DATABASE_STATEMENT_TIMEOUT_MS".into(),
                    WEEKLY_STATEMENT_TIMEOUT_MS.into(),
                )],
            },
            &mut report,
        )
        .await?;
        timings.0.push(("measured cutover", started.elapsed()));
        eprintln!("{}", report.render());
        eprintln!("{}", serde_json::to_string(&report)?);
        ensure!(report.pass, "the measured cutover failed");
        // A sample of the historical artifacts, served from PostgreSQL
        // once the original files are gone.
        artifacts_dir.close()?;
        let sample: Vec<recovery::Artifact> = legacy
            .artifacts
            .iter()
            .step_by((legacy.artifacts.len() / 40).max(1))
            .chain(legacy.artifacts.last())
            .map(|artifact| recovery::Artifact {
                block_hash: artifact.block_hash.clone(),
                digest: artifact.digest.clone(),
                canonical: artifact.canonical.clone(),
            })
            .collect();
        recovery::assert_artifacts(&fixture.pool, &sample).await?;
        ensure!(legacy_snapshot(&fixture.pool, &legacy).await? == legacy_rows);

        let (tail, native) = mine_and_reconcile(
            fixture,
            &restored,
            pg_bin,
            &legacy,
            &legacy_rows,
            &source_evidence,
            &mut timings,
            false,
            Some(&before_balances),
        )
        .await?;
        let phases: Vec<String> = timings
            .0
            .iter()
            .map(|(phase, took)| format!("{phase} {:.1}s", took.as_secs_f64()))
            .collect();
        eprintln!(
            "live mainnet-shaped cutover: {} legacy rows ({} accepted) and {} blocks for {} addresses reconciled through the measured cutover; {tail} native shares and {native} native blocks after cutover are exactly the records an isolated restore discards; {}",
            legacy.last_share_seq,
            legacy.accepted_shares,
            legacy.hashes.len(),
            before_balances.len(),
            phases.join(", ")
        );
        anyhow::Ok(())
    }
    .await;
    restored.pool.close().await;
    restored.admin.close().await;
    let dropped = sqlx::query(&format!("DROP DATABASE {restored_name} WITH (FORCE)"))
        .execute(&fixture.admin)
        .await;
    result.and(dropped.map(|_| ()).map_err(Into::into))
}

/// Mines the regtest chain the plan's blocks sit on, from genesis: every
/// height at the time the plan gives it (`setmocktime`), so each legacy block
/// carries its found time and the tip ends where history does. qbitd refuses
/// a block more than ten minutes ahead of its clock, and a burst of 14,000
/// blocks at the current time would run past that; the fixture's first block
/// is invalidated so history can start in the past.
async fn mine_plan_chain(
    fixture: &Fixture,
    plan: &seed::MainnetPlan,
) -> Result<Vec<seed::ChainBlock>> {
    let height = fixture
        .rpc("getblockcount", json!([]))
        .await?
        .as_u64()
        .context("block count missing")?;
    if height > 0 {
        let first = fixture.rpc("getblockhash", json!([1])).await?;
        fixture.rpc("invalidateblock", json!([first])).await?;
    }
    let times = plan.chain_times();
    let mined = async {
        for time in &times {
            fixture.rpc("setmocktime", json!([time])).await?;
            fixture
                .rpc("generatetoaddress", json!([1, fixture.address]))
                .await?;
        }
        anyhow::Ok(())
    }
    .await;
    // The node's own clock again, whatever happened.
    fixture.rpc("setmocktime", json!([0])).await?;
    mined?;
    // Every height from genesis, so each block's parent is at hand.
    let mut hashes = Vec::with_capacity(times.len() + 1);
    for height in 0..=times.len() as u64 {
        hashes.push(
            fixture
                .rpc("getblockhash", json!([height]))
                .await?
                .as_str()
                .context("block hash missing")?
                .to_owned(),
        );
    }
    let (offsets, _) = plan.chain_offsets();
    Ok(offsets
        .into_iter()
        .map(|offset| {
            let height = 1 + offset;
            seed::ChainBlock {
                height,
                hash: hashes[height as usize].clone(),
                parent: hashes[height as usize - 1].clone(),
            }
        })
        .collect())
}

/// `pg_dump` of one schema in the custom format, as an operator backs up.
async fn pg_dump_schema(database: &str, schema: &str, pg_bin: &Path, file: &Path) -> Result<()> {
    let output = tokio::process::Command::new(pg_bin.join("pg_dump"))
        .args([
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            "--schema",
        ])
        .arg(schema)
        .arg("--file")
        .arg(file)
        .arg("--dbname")
        .arg(database)
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "pg_dump failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Restores `dump` into a new database on the same server, schema name and
/// all, with `pg_restore`: the scale #545's statement-by-statement restore
/// does not reach.
async fn restore_database(
    fixture: &Fixture,
    database: &str,
    dump: &Path,
    pg_bin: &Path,
) -> Result<(recovery::Database, String)> {
    let name = format!("prism_rollback_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&fixture.admin)
        .await?;
    let mut url = url::Url::parse(database)?;
    url.set_path(&format!("/{name}"));
    let output = tokio::process::Command::new(pg_bin.join("pg_restore"))
        .args([
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
            "--dbname",
        ])
        .arg(url.as_str())
        .arg(dump)
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "pg_restore failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let admin = PgPool::connect(url.as_str()).await?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={}", fixture.schema));
    Ok((
        recovery::Database {
            admin,
            pool: PgPool::connect(url.as_str()).await?,
            schema: fixture.schema.clone(),
            url: url.into(),
        },
        name,
    ))
}

/// The legacy rows #545's snapshot compares, as per-chunk digests, so a
/// ledger of millions of rows compares without holding it in memory.
async fn legacy_digest(pool: &PgPool, legacy: &Legacy) -> Result<Value> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT jsonb_build_object(\
          'shares',(SELECT jsonb_agg(h ORDER BY c) FROM (SELECT share_seq/100000 AS c,md5(string_agg(md5(ROW({LEGACY_SHARE_COLUMNS})::text),'' ORDER BY share_seq)) AS h FROM qbit_share_ledger WHERE share_seq<=$1 GROUP BY 1) s),\
          'blocks',(SELECT md5(string_agg(md5(ROW(b.block_hash,b.block_height,b.parent_hash,b.coinbase_txid,b.payout_manifest_sha256,b.chain_state,b.maturity_state,b.audit_publication_sequence,a.audit_bundle_sha256)::text),'' ORDER BY b.block_hash)) FROM qbit_pool_blocks b JOIN qbit_pool_audit_bundles a USING(block_hash) WHERE b.block_hash=ANY($2)),\
          'carry',(SELECT jsonb_agg(h ORDER BY c) FROM (SELECT carry_forward_seq/100000 AS c,md5(string_agg(md5(ROW(carry_forward_seq,block_height,block_hash,miner_id,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state)::text),'' ORDER BY carry_forward_seq)) AS h FROM qbit_payout_carry_forward WHERE carry_forward_seq<=$3 GROUP BY 1) q))"
    ))
    .bind(legacy.last_share_seq)
    .bind(&legacy.hashes)
    .bind(legacy.last_carry_seq)
    .fetch_one(pool)
    .await?)
}

/// `migrate` then `import-audits` against `database_url`, as the runbook
/// runs them, requiring the recorded 2.x.x source and complete history.
async fn migrate_and_import(fixture: &Fixture, database_url: &str, root: &str) -> Result<()> {
    let migrated = fixture.tool(database_url, &["migrate"]).await?;
    ensure!(
        migrated.contains("database source:") && !migrated.contains("unrecorded"),
        "migrate did not record the 2.x.x source: {migrated}"
    );
    let imported = fixture
        .tool(database_url, &["import-audits", "--root", root])
        .await?;
    ensure!(
        imported.contains(&format!("Imported {LEGACY_BLOCKS} audit bodies"))
            && imported.contains("\"missing_stored_bodies\":0")
            && imported.contains("\"missing_canonical_bytes\":0"),
        "import-audits left history incomplete: {imported}"
    );
    Ok(())
}

/// What the seed wrote that later phases check against.
struct Legacy {
    accepted_shares: i64,
    last_share_seq: i64,
    sequence_headroom: i64,
    last_publication_ordinal: i64,
    last_carry_seq: i64,
    hashes: Vec<String>,
    artifacts: Vec<recovery::Artifact>,
    /// Compare the legacy rows by digest rather than as one JSON document,
    /// for a ledger too large to hold in memory.
    digest_rows: bool,
}

impl From<seed::Seeded> for Legacy {
    fn from(seeded: seed::Seeded) -> Self {
        Self {
            accepted_shares: seeded.accepted_shares,
            last_share_seq: seeded.last_share_seq,
            sequence_headroom: seeded.sequence_headroom,
            last_publication_ordinal: seeded.last_publication_ordinal,
            last_carry_seq: seeded.last_carry_seq,
            hashes: seeded.hashes,
            artifacts: seeded
                .artifacts
                .into_iter()
                .map(|artifact| recovery::Artifact {
                    block_hash: artifact.block_hash,
                    digest: artifact.digest,
                    canonical: artifact.canonical,
                })
                .collect(),
            digest_rows: false,
        }
    }
}

/// Mines the 2.x.x-era chain, then writes #545's drained 2.x.x ledger over
/// it (`seed::seed_s20`): the frozen release DDL, ordered shares from a
/// skewed recipient set with rejects among them, a signed audit bundle per
/// found block in the three historical body layouts, chained carry-forward
/// rows, and terminal candidate rows.
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
        blocks.push(chain_block(fixture, height).await?);
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let coinbase_key = ManifestSigningKey::from_seed_hex(&COINBASE_SEED.repeat(32))?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&LEDGER_SEED.repeat(32))?;
    Ok(seed::seed_s20(
        &fixture.pool,
        root,
        &blocks,
        now_ms,
        &coinbase_key,
        &ledger_key,
    )
    .await?
    .into())
}

/// The block the node has at `height`, with its parent.
async fn chain_block(fixture: &Fixture, height: u64) -> Result<seed::ChainBlock> {
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
    Ok(seed::ChainBlock {
        height,
        hash,
        parent,
    })
}

/// Every 2.x.x row a migration or native writer could touch, on the 2.x.x
/// columns only: the share prefix, the blocks with their publication order
/// and audit identity, and the carry chain.
async fn legacy_snapshot(pool: &PgPool, legacy: &Legacy) -> Result<Value> {
    if legacy.digest_rows {
        return legacy_digest(pool, legacy).await;
    }
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
    /// settings against `database_url`, as the runbook runs it beside the
    /// frontends, and returns its standard output. The migrated ledger's
    /// writer key is the one the history was signed with.
    async fn tool(&self, database_url: &str, args: &[&str]) -> Result<String> {
        let ledger_public_key =
            ManifestSigningKey::from_seed_hex(&LEDGER_SEED.repeat(32))?.public_key_hex();
        let mut command = self.server_command(
            0,
            None,
            &[
                ("PRISM_DATABASE_URL", database_url.to_owned()),
                ("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", ledger_public_key),
            ],
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

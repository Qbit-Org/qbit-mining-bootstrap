//! Dual-writer windows on PostgreSQL: both nodes' rows in one ledger.
//!
//! Each case writes node 0's rows (even `share_seq`) and node 1's (odd) into
//! one ledger, as the peer sync leaves them, sets the peer's high-water mark
//! as the sync would, and checks the window a snapshot takes, every later
//! proof of it, and its reproduction on the other node's database, against
//! a model: the newest-first walk over the rows the cut admits.
use super::super::super::audit::{
    persist_audit_snapshot, verify_durable_range, AuditSnapshotWrite,
};
use super::super::snapshot_delta::{advance, Advance, RetainedShares, SnapshotCapture};
use super::*;
use super::{ORIGIN_INDEX_SQL, OWN_CUT_SQL, PEER_CUT_SQL};
use crate::metrics::WindowAcquisition;
use crate::node_identity::{NodeIdentity, NodeIndex};
use futures_util::future::LocalBoxFuture;
use qbit_prism_test_gate as gate;

/// One fresh database for `node`, its ledger taking window cuts as that node.
async fn open(
    raw: &str,
    node: i16,
) -> Result<(crate::ledger_test_database::FixtureDatabase, Ledger)> {
    let fixture =
        crate::ledger_test_database::FixtureDatabase::open(raw, "dual_writer_windows_").await?;
    let ledger = match Ledger::connect(&fixture.url, format!("dual-writer-{node}"), 4, true).await {
        Ok(ledger) => ledger,
        Err(error) => return Err(fixture.abandon(error).await),
    };
    let identity = NodeIdentity {
        node: NodeIndex::from_index(node.into()).context("node index")?,
        carry_owner: node == 0,
    };
    let ready = async {
        ledger.set_dual_writer_identity(identity)?;
        origin_index(&ledger).await
    }
    .await;
    if let Err(error) = ready {
        ledger.pool.close().await;
        return Err(fixture.abandon(error).await);
    }
    Ok((fixture, ledger))
}

/// Migration 031's `(origin_node, share_seq)` index, which a dual-writer
/// snapshot requires, where this branch's migrations do not create it yet.
async fn origin_index(ledger: &Ledger) -> Result<()> {
    let indexed: bool = sqlx::query_scalar(ORIGIN_INDEX_SQL)
        .fetch_one(&ledger.pool)
        .await?;
    if !indexed {
        sqlx::query("CREATE INDEX qbit_share_ledger_origin_seq_until_031 ON qbit_share_ledger (origin_node, share_seq)")
            .execute(&ledger.pool)
            .await?;
    }
    Ok(())
}

async fn run(
    site: gate::Site,
    body: impl for<'a> FnOnce(&'a Ledger) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(raw) = gate::database_url(site)? else {
        return Ok(());
    };
    let (fixture, ledger) = open(&raw, 0).await?;
    let result = body(&ledger).await;
    ledger.pool.close().await;
    fixture.close(result).await
}

/// A ledger row as the sync or an append leaves it: its node, explicit
/// `share_seq`, difficulty and stamps (milliseconds).
#[derive(Clone, Copy, Debug)]
struct Row {
    node: i16,
    seq: i64,
    difficulty: u128,
    accepted_ms: i64,
}

/// A stamp well before any anchor a snapshot takes now.
const PAST_MS: i64 = 1_700_000_000_000;

fn row(node: i16, seq: i64, difficulty: u128) -> Row {
    Row {
        node,
        seq,
        difficulty,
        accepted_ms: PAST_MS + seq,
    }
}

async fn insert(ledger: &Ledger, rows: &[Row]) -> Result<()> {
    for row in rows {
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch,origin_node)
             VALUES($1,'n'||$2::text||':'||$1::text,'miner-'||($1%3)::text,'miner',decode(repeat('11',32),'hex'),$3::text::numeric,1,1,'job',to_timestamp(($4-5)::double precision/1000),1,to_timestamp($4::double precision/1000),true,'fixture',0,$2)",
        )
        .bind(row.seq)
        .bind(row.node)
        .bind(row.difficulty.to_string())
        .bind(row.accepted_ms)
        .execute(&ledger.pool)
        .await?;
    }
    Ok(())
}

/// Where the peer sync stands for `peer`'s shares: every peer row at or
/// below `through` is here.
async fn mark(ledger: &Ledger, peer: i16, through: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_prism_peer_sync_cursors(stream,peer_node,scanned_through,ingested_through) VALUES('shares',$1,$2,$2)
         ON CONFLICT(stream) DO UPDATE SET peer_node=EXCLUDED.peer_node,scanned_through=EXCLUDED.scanned_through,ingested_through=EXCLUDED.ingested_through",
    )
    .bind(peer)
    .bind(through)
    .execute(&ledger.pool)
    .await?;
    Ok(())
}

async fn capture(ledger: &Ledger, network: u128) -> Result<SnapshotCapture> {
    Ok(ledger
        .snapshot_with_admission(network, ReadAdmission::default(), None)
        .await?
        .into_inner())
}

/// The model: the rows the cut admits at the anchor, walked newest first to
/// the crossing row, ascending.
fn model(rows: &[Row], anchor: i64, cut: &WindowCut, weight: u128) -> Vec<u64> {
    let mut eligible: Vec<&Row> = rows
        .iter()
        .filter(|row| row.accepted_ms <= anchor && cut.admits(row.node as u8, row.seq as u64))
        .collect();
    eligible.sort_by_key(|row| std::cmp::Reverse(row.seq));
    let mut remaining = weight;
    let mut window = Vec::new();
    for row in eligible {
        if remaining == 0 {
            break;
        }
        remaining = remaining.saturating_sub(row.difficulty);
        window.push(row.seq as u64);
    }
    window.reverse();
    window
}

fn seqs(shares: &[AcceptedShare]) -> Vec<u64> {
    shares.iter().map(|share| share.share_seq).collect()
}

/// What a landing writes and proves for a captured window.
fn audit_write(capture: &SnapshotCapture, network: u128) -> AuditSnapshotWrite {
    let shares = &capture.snapshot.shares;
    AuditSnapshotWrite {
        digest: hex::encode(Sha256::digest(serde_json::to_vec(shares).unwrap())),
        first_share_seq: shares[0].share_seq as i64,
        last_share_seq: shares[shares.len() - 1].share_seq as i64,
        anchor_ms: capture.anchor_ms,
        network_difficulty: network,
        share_count: shares.len() as i64,
        inline: None,
        cut: capture.cut,
        shares: std::sync::Arc::new(shares.clone()),
    }
}

/// Every proof of `window` a landing, a rebuild, the #619 probe and the
/// in-lock count make, on `ledger`'s database.
async fn prove(ledger: &Ledger, capture: &SnapshotCapture, network: u128) -> Result<()> {
    let reference = WindowRef::from_snapshot(&capture.snapshot)?;
    let read = ledger
        .read_window(&reference, BalanceSource::Current)
        .await?;
    ensure!(read.shares == capture.snapshot.shares, "re-read differs");
    let write = audit_write(capture, network);
    verify_durable_range(&ledger.pool, &write, None).await?;
    let mut tx = ledger.begin().await?;
    persist_audit_snapshot(&mut tx, &write).await?;
    let holding = probe_window_holding(&mut tx, &reference).await?;
    ensure!(holding == WindowHolding::Held, "holding {holding:?}");
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_cuts_each_node_at_its_newest_row_the_window_may_hold() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            // Nothing yet: a cut that admits nothing, and an empty window.
            let empty = capture(ledger, 1).await?;
            assert_eq!(empty.cut, Some(WindowCut::default()));
            assert!(empty.shares.is_empty());
            // Own rows only, no peer mark yet: the peer's rows are not admitted
            // even when some are present.
            let mut rows: Vec<Row> = (1..=10).map(|i| row(0, 2 * i, 1)).collect();
            rows.push(row(1, 7, 1));
            insert(ledger, &rows).await?;
            let own_only = capture(ledger, 1).await?;
            assert_eq!(own_only.cut, Some(WindowCut::new(Some(20), None)?));
            assert!(seqs(&own_only.shares).iter().all(|seq| seq % 2 == 0));
            // The mark admits the peer's rows at or below it, and nothing above.
            let late = [row(1, 9, 1), row(1, 21, 1)];
            insert(ledger, &late).await?;
            rows.extend(late);
            mark(ledger, 1, 9).await?;
            let both = capture(ledger, 2).await?;
            let cut = WindowCut::new(Some(20), Some(9))?;
            assert_eq!(both.cut, Some(cut));
            assert_eq!(seqs(&both.shares), model(&rows, both.anchor_ms, &cut, 16));
            assert!(seqs(&both.shares).contains(&9) && !seqs(&both.shares).contains(&21));
            // The cutoff stays the accepted maximum over both nodes: what the
            // refresh probe reads, and at least every row of the window.
            assert_eq!(both.share_seq, 21);
            prove(ledger, &both, 2).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_peer_rows_never_change_a_window_already_built() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            // Node 0 is ahead; node 1's rows arrive late, with share_seq inside
            // node 0's range, then above it.
            let mut rows: Vec<Row> = (1..=30).map(|i| row(0, 2 * i, 1)).collect();
            rows.extend([1, 5, 13].map(|seq| row(1, seq, 1)));
            insert(ledger, &rows).await?;
            mark(ledger, 1, 13).await?;
            let network = 3;
            let built = capture(ledger, network).await?;
            let first = built.shares[0].share_seq;
            assert_eq!(built.cut, Some(WindowCut::new(Some(60), Some(13))?));
            // Late rows: inside the window's range, under its first row, and
            // above its top, all stamped well before the anchor.
            let late: Vec<Row> = [first as i64 + 1, first as i64 - 1, 41, 59, 61]
                .into_iter()
                .filter(|seq| seq % 2 == 1 && *seq > 13)
                .map(|seq| row(1, seq, 1))
                .collect();
            assert!(late
                .iter()
                .any(|row| (row.seq as u64) > first && row.seq < 60));
            insert(ledger, &late).await?;
            mark(ledger, 1, 61).await?;
            // Every proof of the built window still holds.
            prove(ledger, &built, network).await?;
            // Without its cut the same anchor would admit the late rows: the cut
            // is what keeps the window fixed.
            let mut uncut = audit_write(&built, network);
            uncut.cut = None;
            let error = verify_durable_range(&ledger.pool, &uncut, None)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("differs from canonical database history")
                    || error.contains("omits newest canonical share"),
                "{error}"
            );
            // The late rows join the next window.
            let next = capture(ledger, network).await?;
            assert_eq!(next.cut, Some(WindowCut::new(Some(60), Some(61))?));
            rows.extend(late);
            assert_eq!(
                seqs(&next.shares),
                model(&rows, next.anchor_ms, &next.cut.unwrap(), network * 8)
            );
            prove(ledger, &next, network).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_clock_running_ahead_only_delays_its_newest_rows() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            let now: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&ledger.pool)
            .await?;
            let mut rows: Vec<Row> = (1..=10).map(|i| row(0, 2 * i, 1)).collect();
            rows.extend((0..5).map(|i| row(1, 2 * i + 1, 1)));
            // Node 1's two newest rows were stamped by a clock an hour ahead.
            let ahead = [
                Row {
                    accepted_ms: now + 3_600_000,
                    ..row(1, 11, 1)
                },
                Row {
                    accepted_ms: now + 3_600_001,
                    ..row(1, 13, 1)
                },
            ];
            rows.extend(ahead);
            insert(ledger, &rows).await?;
            mark(ledger, 1, 13).await?;
            let built = capture(ledger, 4).await?;
            // The peer's entry stops below its first row stamped after the anchor,
            // so the anchor rule never removes a row the cut admits.
            assert_eq!(built.cut, Some(WindowCut::new(Some(20), Some(9))?));
            assert!(built
                .shares
                .iter()
                .all(|share| share.accepted_at_ms <= built.anchor_ms
                    && share.job_issued_at_ms <= built.anchor_ms));
            prove(ledger, &built, 4).await?;
            // The verifier binaries' fold agrees with the window, cut included.
            let found = qbit_prism::FoundBlock {
                block_height: 10,
                coinbase_value_sats: 100_000_000,
                network_difficulty: 4,
                anchor_job_issued_at_ms: built.anchor_ms,
            };
            let manifest_key =
                qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"42".repeat(32))?;
            let ledger_key =
                qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"43".repeat(32))?;
            let body = qbit_prism::build_audit_bundle_body_with_coinbase_options_parallel(
                &built.shares,
                found,
                built.cut,
                Vec::new(),
                qbit_prism::PayoutPolicy::day_one_default(),
                None,
                Vec::new(),
                &manifest_key,
                &ledger_key,
                qbit_prism::Parallelism::serial(),
            )?;
            qbit_prism::verify_audit_parts(&body, &built.shares, &ledger_key.public_key_hex())?;
            // Once this node's clock passes their stamps they join, and the
            // window built before stays provable.
            sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=$1 WHERE singleton")
                .bind(now + 3_600_002)
                .execute(&ledger.pool)
                .await?;
            let later = capture(ledger, 4).await?;
            assert_eq!(later.cut, Some(WindowCut::new(Some(20), Some(13))?));
            assert!(seqs(&later.shares).contains(&13));
            prove(ledger, &built, 4).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_window_reproduces_on_the_other_nodes_database() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let (fixture_a, a) = open(&raw, 0).await?;
    let (fixture_b, b) = match open(&raw, 1).await {
        Ok(opened) => opened,
        Err(error) => {
            a.pool.close().await;
            return Err(fixture_a.abandon(error).await);
        }
    };
    let result = async {
        // A's rows and B's interleave; each database holds its own rows and
        // the peer's up to that peer's mark.
        let a_rows: Vec<Row> = (1..=25)
            .map(|i| row(0, 2 * i, 1 + (i % 3) as u128))
            .collect();
        let b_rows: Vec<Row> = (0..20)
            .map(|i| row(1, 2 * i + 1, 1 + (i % 2) as u128))
            .collect();
        insert(&a, &a_rows).await?;
        insert(&a, &b_rows[..12]).await?;
        mark(&a, 1, b_rows[11].seq).await?;
        insert(&b, &b_rows).await?;
        insert(&b, &a_rows[..20]).await?;
        mark(&b, 0, a_rows[19].seq).await?;
        // A builds the window; B holds A's rows only to A's 20th so far.
        let network = 5;
        let built = capture(&a, network).await?;
        let cut = built.cut.context("A's window has no cut")?;
        assert_eq!(cut, WindowCut::new(Some(50), Some(b_rows[11].seq as u64))?);
        let reference = WindowRef::from_snapshot(&built.snapshot)?;
        let window_on_b = b.read_window(&reference, BalanceSource::Current).await;
        if seqs(&built.shares)
            .iter()
            .any(|seq| *seq > a_rows[19].seq as u64)
        {
            assert!(window_on_b.is_err(), "B reproduced rows it does not hold");
        }
        // Once B has A's rows up to A's entry, the window reproduces there
        // byte for byte, B's own later rows notwithstanding.
        insert(&b, &a_rows[20..]).await?;
        mark(&b, 0, 50).await?;
        prove(&b, &built, network).await?;
        let mut all = a_rows.clone();
        all.extend(&b_rows);
        assert_eq!(
            seqs(&built.shares),
            model(&all, built.anchor_ms, &cut, network * 8)
        );
        // And B's own window, built now, reproduces on A once A holds B's rows.
        let b_window = capture(&b, network).await?;
        insert(&a, &b_rows[12..]).await?;
        mark(&a, 1, b_rows[19].seq).await?;
        prove(&a, &b_window, network).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    a.pool.close().await;
    b.pool.close().await;
    let closed = fixture_a.close(result).await;
    fixture_b.close(closed).await
}

/// The two cut reads probe the `(origin_node, share_seq)` index and never walk
/// the other node's rows: on a ledger where node 1's rows all lie under a run
/// of node 0's, as on a node that idled while its peer served, neither plan
/// filters rows out of a primary-key walk, with custom or generic plans.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cut_reads_probe_the_origin_index_and_never_walk_the_other_nodes_run() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            let mut rows: Vec<Row> = (0..200).map(|i| row(1, 4 * i + 1, 1)).collect();
            rows.extend((1..=2_000).map(|i| row(0, 2 * i, 1)));
            insert(ledger, &rows).await?;
            ensure!(
                sqlx::query_scalar::<_, bool>(ORIGIN_INDEX_SQL)
                    .fetch_one(&ledger.pool)
                    .await?,
                "no (origin_node, share_seq) index"
            );
            sqlx::query("ANALYZE qbit_share_ledger")
                .execute(&ledger.pool)
                .await?;
            for mode in ["force_custom_plan", "force_generic_plan"] {
                let mut tx = ledger.begin().await?;
                sqlx::query(&format!("SET LOCAL plan_cache_mode = {mode}"))
                    .execute(&mut *tx)
                    .await?;
                for (label, sql, peer) in [
                    ("own cut", OWN_CUT_SQL, false),
                    ("peer cut", PEER_CUT_SQL, true),
                ] {
                    let statement =
                        format!("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {sql}");
                    let mut explain = sqlx::query_scalar::<_, String>(&statement).bind(1i16);
                    if peer {
                        explain = explain.bind(i64::MAX).bind(PAST_MS + 86_400_000_000);
                    }
                    let plan = explain.fetch_all(&mut *tx).await?.join("\n");
                    ensure!(
                        plan.contains("origin")
                            && !plan.contains("pkey")
                            && !plan.contains("Rows Removed by Filter"),
                        "{label} under {mode} walks rows:\n{plan}"
                    );
                }
                tx.rollback().await?;
            }
            // And they still answer: node 1's newest row is 797, under node 0's run.
            let own: Option<i64> = sqlx::query_scalar(OWN_CUT_SQL)
                .bind(1i16)
                .fetch_one(&ledger.pool)
                .await?;
            assert_eq!(own, Some(797));
            Ok(())
        })
    })
    .await
}

/// A dual-writer snapshot refuses, before it takes any lock, on a ledger
/// without the `(origin_node, share_seq)` index its cut reads probe: it would
/// otherwise scan the ledger inside `ORDER_LOCK`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dual_writer_snapshot_refuses_without_the_origin_index() -> Result<()> {
    run(gate::site!(), |ledger| Box::pin(async move {
        insert(ledger, &[row(0, 2, 1), row(1, 3, 1)]).await?;
        let indexes: Vec<String> = sqlx::query_scalar(
            "SELECT i.indexrelid::regclass::text FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] \
             JOIN pg_attribute b ON b.attrelid=i.indrelid AND b.attnum=i.indkey[1] \
             WHERE i.indrelid='qbit_share_ledger'::regclass AND a.attname='origin_node' AND b.attname='share_seq'",
        )
        .fetch_all(&ledger.pool)
        .await?;
        ensure!(!indexes.is_empty(), "the fixture has no origin index to drop");
        for index in indexes {
            sqlx::query(&format!("DROP INDEX {index}"))
                .execute(&ledger.pool)
                .await?;
        }
        let Err(error) = capture(ledger, 1).await else {
            bail!("a dual-writer snapshot was taken without the origin index");
        };
        assert!(
            format!("{error:#}").contains("needs a valid (origin_node, share_seq) index"),
            "{error:#}"
        );
        // Nothing was taken: no lock is left held and the clock did not move.
        let held: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_locks WHERE locktype='advisory'")
            .fetch_one(&ledger.pool)
            .await?;
        assert_eq!(held, 0);
        Ok(())
    })).await
}

/// The refresh probe reads the peer's mark in dual-writer mode, and a snapshot
/// records the mark its cut was taken at, so a window is rebuilt once peer
/// rows arrive below this node's cutoff; a single writer reads neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_refresh_probe_and_the_snapshot_report_the_peer_mark() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            let probe = ledger.refresh_probe(ReadAdmission::default()).await?;
            assert_eq!(probe.peer_mark, None);
            insert(
                ledger,
                &(1..=10).map(|i| row(0, 2 * i, 1)).collect::<Vec<_>>(),
            )
            .await?;
            insert(ledger, &[row(1, 3, 1)]).await?;
            mark(ledger, 1, 3).await?;
            let captured = capture(ledger, 1).await?;
            assert_eq!(captured.peer_mark, Some(3));
            let probe = ledger.refresh_probe(ReadAdmission::default()).await?;
            assert_eq!((probe.peer_mark, probe.accepted_share_seq), (Some(3), 20));
            // A late peer row under the cutoff moves the mark, not the cutoff.
            insert(ledger, &[row(1, 7, 1)]).await?;
            mark(ledger, 1, 7).await?;
            let probe = ledger.refresh_probe(ReadAdmission::default()).await?;
            assert_eq!((probe.peer_mark, probe.accepted_share_seq), (Some(7), 20));
            Ok(())
        })
    })
    .await
}

/// Retained window, advanced by the delta path, against a full scan at the
/// same anchor, as the single-writer differential does.
async fn differential(
    ledger: &Ledger,
    prior: RetainedShares,
    network: u128,
) -> Result<(SnapshotCapture, WindowAcquisition)> {
    let full = capture(ledger, network).await?;
    let completion = ReadAdmission::default();
    let mut tx = ledger.begin().await?;
    let result = advance(
        &mut tx,
        completion.own(prior),
        network * 8,
        full.anchor_ms,
        i64::try_from(full.share_seq)?,
        full.cut,
        &completion,
    )
    .await?;
    tx.commit().await?;
    let outcome = match result {
        Advance::Advanced {
            shares,
            leaf,
            report,
        } => {
            assert_eq!(Some(&leaf), full.leaf.as_ref());
            assert_eq!(shares.into_inner(), full.snapshot.shares);
            report.outcome
        }
        Advance::Rejected(report) => report.outcome,
    };
    Ok((full, outcome))
}

fn retained(capture: SnapshotCapture, network: u128) -> RetainedShares {
    let SnapshotCapture { snapshot, leaf, .. } = capture;
    RetainedShares {
        network,
        leaf,
        anchor_ms: snapshot.anchor_ms,
        cutoff: snapshot.share_seq,
        cut: snapshot.cut,
        shares: snapshot.shares,
    }
}

/// Seeded walks: node 0 appends, node 1's rows arrive late with share_seq
/// anywhere below node 0's top (inside the retained window and under it) or
/// above it, the mark moves, and the target retargets. Every crossing window
/// the full scan takes, the delta path takes too, row for row; late rows are
/// merged, not a reason to rescan.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_incremental_advance_merges_late_peer_rows_exactly() -> Result<()> {
    use rand::{Rng, SeedableRng};
    for seed in [3u64, 31, 2026] {
        run(gate::site!(), |ledger| {
            Box::pin(async move {
                let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
                let mut own_next = 2i64;
                let mut peer_next = 1i64;
                let mut rows = Vec::new();
                for _ in 0..40 {
                    rows.push(row(0, own_next, rng.gen_range(1..=4)));
                    own_next += 2 * rng.gen_range(1..=2);
                }
                insert(ledger, &rows).await?;
                let mut network: u128 = 4;
                let mut prior = capture(ledger, network).await?;
                let (mut advanced, mut merged_late) = (0, 0);
                for _ in 0..30 {
                    let top = prior
                        .shares
                        .last()
                        .map_or(0, |share| share.share_seq as i64);
                    let mut fresh = Vec::new();
                    for _ in 0..rng.gen_range(0..=4) {
                        fresh.push(row(0, own_next, rng.gen_range(1..=4)));
                        own_next += 2;
                    }
                    // The peer's sequence trails node 0's: its rows land below
                    // the retained top, until it catches up.
                    for _ in 0..rng.gen_range(0..=5) {
                        peer_next = peer_next.max(1) + 2 * rng.gen_range(1..=3);
                        fresh.push(row(1, peer_next, rng.gen_range(1..=4)));
                    }
                    let late = fresh.iter().any(|row| row.node == 1 && row.seq < top);
                    insert(ledger, &fresh).await?;
                    rows.extend(fresh);
                    if rng.gen_range(0..4) != 0 {
                        mark(ledger, 1, peer_next).await?;
                    }
                    let previous = network;
                    network = match rng.gen_range(0..4) {
                        0 => (network / 2).max(1),
                        1 => network + 1,
                        _ => network,
                    };
                    let (next, outcome) =
                        differential(ledger, retained(prior, previous), network).await?;
                    assert_eq!(
                        seqs(&next.shares),
                        model(&rows, next.anchor_ms, &next.cut.unwrap(), network * 8),
                        "seed {seed}"
                    );
                    if outcome == WindowAcquisition::Advanced {
                        advanced += 1;
                        merged_late += usize::from(late);
                    } else {
                        assert!(
                            matches!(
                                outcome,
                                WindowAcquisition::Partial | WindowAcquisition::MarginTooLarge
                            ),
                            "seed {seed}: {outcome:?}"
                        );
                    }
                    prior = next;
                }
                assert!(advanced > 10, "seed {seed}: only {advanced} advanced");
                assert!(merged_late > 0, "seed {seed}: no late row was merged");
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_window_cuts_a_ledger_holding_peer_rows_keeps_the_3_0_window() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let fixture =
        crate::ledger_test_database::FixtureDatabase::open(&raw, "dual_writer_windows_").await?;
    let ledger = match Ledger::connect(&fixture.url, "single-writer".into(), 4, true).await {
        Ok(ledger) => ledger,
        Err(error) => return Err(fixture.abandon(error).await),
    };
    let result = async {
        let mut rows: Vec<Row> = (1..=10).map(|i| row(0, 2 * i, 1)).collect();
        rows.extend((0..10).map(|i| row(1, 2 * i + 1, 1)));
        insert(&ledger, &rows).await?;
        let snapshot = capture(&ledger, 2).await?;
        // No cut: every accepted row stamped by the anchor is eligible, as in
        // 3.0, whatever its node.
        assert_eq!(snapshot.cut, None);
        assert_eq!(snapshot.peer_mark, None);
        assert_eq!(
            ledger
                .refresh_probe(ReadAdmission::default())
                .await?
                .peer_mark,
            None
        );
        assert_eq!(seqs(&snapshot.shares), (5..=20).collect::<Vec<u64>>());
        let reference = WindowRef::from_snapshot(&snapshot.snapshot)?;
        assert_eq!(reference.cut, None);
        assert!(!serde_json::to_string(&reference)?.contains("cut"));
        prove(&ledger, &snapshot, 2).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    ledger.pool.close().await;
    fixture.close(result).await
}

/// What a dual-writer refresh costs at production window sizes: the full
/// scan, the delta path for an append-only refresh and for one that merges
/// late peer rows inside the window, and this node's cut read on a node that
/// has written nothing for a long run of the peer's rows, with and without
/// the `(origin_node, share_seq)` index (migration 031). Prints one line per
/// figure; asserts only that every acquisition is exact.
///
/// ```text
/// PRISM_TEST_DATABASE_URL=... PRISM_DUAL_WINDOW_MEASURE_ROWS=400000 \
///   cargo test --locked -p qbit-prism-server --lib -- --ignored --nocapture \
///   ledger::window::cut::dual_writer_tests::measure_dual_writer_refresh
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement harness: run on demand with PRISM_DUAL_WINDOW_MEASURE_ROWS"]
async fn measure_dual_writer_refresh() -> Result<()> {
    let raw = gate::required_database_url(gate::site!())?;
    let rows: i64 = std::env::var("PRISM_DUAL_WINDOW_MEASURE_ROWS")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(200_000);
    let (fixture, ledger) = open(&raw, 0).await?;
    let result = async {
        // Node 0 on every even share_seq. Node 1 on every fourth (1 mod 4), but
        // only through the lower half: its sequence trails node 0's, so its
        // later rows arrive above its mark and below node 0's top, inside the
        // window. Difficulty 1, so the window is the newest `rows` rows.
        let started = std::time::Instant::now();
        let span = 2 * rows;
        let peer_mark = span / 2 - (span / 2) % 4 + 1;
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch,origin_node)
             SELECT i,'m:'||i::text,'miner-'||(i%7)::text,'miner',decode(repeat('11',32),'hex'),1,1,1,'job',to_timestamp(1),1,to_timestamp(2),true,'fixture',0,(i%2)::smallint
             FROM generate_series(1,$1::bigint) i WHERE i%2=0 OR (i%4=1 AND i<=$2)",
        )
        .bind(span)
        .bind(peer_mark)
        .execute(&ledger.pool)
        .await?;
        let top: i64 = sqlx::query_scalar("SELECT max(share_seq) FROM qbit_share_ledger")
            .fetch_one(&ledger.pool)
            .await?;
        mark(&ledger, 1, peer_mark).await?;
        sqlx::query("ANALYZE qbit_share_ledger").execute(&ledger.pool).await?;
        println!("measure: loaded {} rows in {:?}", rows, started.elapsed());
        let network = u128::try_from(rows / 8).unwrap();
        let timed = |label: &'static str, started: std::time::Instant| {
            println!("measure: {label}: {:?}", started.elapsed())
        };
        let started = std::time::Instant::now();
        let full = capture(&ledger, network).await?;
        timed("full scan, empty refresh", started);
        assert_eq!(full.cut, Some(WindowCut::new(Some(top as u64), Some(peer_mark as u64))?));
        // An append-only refresh: 1,000 new own rows above the top.
        let next_own = top + 2 - top % 2;
        let appended: Vec<Row> = (0..1_000).map(|i| row(0, next_own + 2 * i, 1)).collect();
        insert(&ledger, &appended).await?;
        let started = std::time::Instant::now();
        let (after_append, outcome) = differential(&ledger, retained(full, network), network).await?;
        timed("full scan plus delta, append-only refresh", started);
        assert_eq!(outcome, WindowAcquisition::Advanced);
        // A refresh that merges 1,000 late peer rows, continuing node 1's
        // sequence above its mark and inside the window, with 1,000 own
        // appends.
        let late: Vec<Row> = (1..=1_000).map(|i| row(1, peer_mark + 4 * i, 1)).collect();
        assert!(late[0].seq as u64 > after_append.shares[0].share_seq);
        let own_top = next_own + 2 * 999;
        let more: Vec<Row> = (1..=1_000).map(|i| row(0, own_top + 2 * i, 1)).collect();
        insert(&ledger, &late).await?;
        insert(&ledger, &more).await?;
        mark(&ledger, 1, peer_mark + 4_000).await?;
        let started = std::time::Instant::now();
        let full_only = capture(&ledger, network).await?;
        timed("full scan, refresh with late rows", started);
        let completion = ReadAdmission::default();
        let mut tx = ledger.begin().await?;
        let started = std::time::Instant::now();
        let result = advance(
            &mut tx,
            completion.own(retained(after_append, network)),
            network * 8,
            full_only.anchor_ms,
            i64::try_from(full_only.share_seq)?,
            full_only.cut,
            &completion,
        )
        .await?;
        timed("delta alone, merging late rows", started);
        tx.commit().await?;
        match result {
            Advance::Advanced { shares, report, .. } => {
                println!(
                    "measure: merged {} delta rows into {} retained rows, {} pages",
                    report.delta_rows, report.prior_rows, report.pages
                );
                assert_eq!(shares.into_inner(), full_only.snapshot.shares);
            }
            Advance::Rejected(report) => bail!("the merge fell back: {report:?}"),
        }
        // This node's cut read on a node that has written nothing since the
        // peer's run began: node 1's view of this ledger once node 0 has
        // appended `rows` more rows, all above node 1's newest.
        let base: i64 = sqlx::query_scalar("SELECT max(share_seq) FROM qbit_share_ledger")
            .fetch_one(&ledger.pool)
            .await?;
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch,origin_node)
             SELECT i,'run:'||i::text,'miner','miner',decode(repeat('11',32),'hex'),1,1,1,'job',to_timestamp(1),1,to_timestamp(2),true,'fixture',0,0
             FROM generate_series($1::bigint,$2::bigint,2) i",
        )
        .bind(base + 2 - base % 2)
        .bind(base + 2 * rows)
        .execute(&ledger.pool)
        .await?;
        sqlx::query("ANALYZE qbit_share_ledger").execute(&ledger.pool).await?;
        for (label, index) in [("own cut on an idle node, no index", false), ("own cut on an idle node, (origin_node, share_seq) index", true)] {
            if index {
                sqlx::query("CREATE INDEX measure_origin_seq ON qbit_share_ledger (origin_node, share_seq)")
                    .execute(&ledger.pool)
                    .await?;
                sqlx::query("ANALYZE qbit_share_ledger").execute(&ledger.pool).await?;
            }
            let started = std::time::Instant::now();
            for _ in 0..10 {
                let _: Option<i64> = sqlx::query_scalar(OWN_CUT_SQL)
                    .bind(1i16)
                    .fetch_one(&ledger.pool)
                    .await?;
            }
            println!("measure: {label}: {:?} per read", started.elapsed() / 10);
            let mut tx = ledger.begin().await?;
            sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan").execute(&mut *tx).await?;
            let plan = format!("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {OWN_CUT_SQL}");
            let lines: Vec<String> = sqlx::query_scalar(&plan).bind(1i16).fetch_all(&mut *tx).await?;
            println!("measure: own cut plan ({label}):\n{}", lines.join("\n"));
            if index {
                assert!(
                    !lines.iter().any(|line| line.contains("Rows Removed by Filter")),
                    "the own cut walked rows:\n{}",
                    lines.join("\n")
                );
            }
            let plan = format!("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {}", super::PEER_CUT_SQL);
            let lines: Vec<String> = sqlx::query_scalar(&plan)
                .bind(1i16)
                .bind(i64::MAX)
                .bind(PAST_MS + 86_400_000_000)
                .fetch_all(&mut *tx)
                .await?;
            println!("measure: peer cut plan ({label}):\n{}", lines.join("\n"));
            tx.rollback().await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    ledger.pool.close().await;
    fixture.close(result).await
}

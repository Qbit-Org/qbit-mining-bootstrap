//! The ledger clock derived from append-only state (#738).
//!
//! A share append stamps `accepted_at` with the greatest of the stored
//! `ledger_clock_ms`, the database's clock and the newest own share's stamp,
//! and writes nothing to the cluster row; the snapshot anchor takes the same
//! greatest plus one and stores it; the peer sync's own-log recovery only
//! raises the stored clock. These tests drive the database's clock "back" by
//! pinning the stored clock ahead of it and winding it back again, which is
//! what a backward step of the database's clock looks like to the derivation:
//! the newest share stamped ahead of both the stored clock and `now`.
use anyhow::{ensure, Context, Result};
use qbit_prism::AcceptedShare;
use qbit_prism_server::{
    ledger::Ledger,
    node_identity::{NodeIdentity, NodeIndex},
};
use qbit_prism_test_gate as gate;
use sqlx::PgPool;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

const ORDER_LOCK: i64 = 0x505249534d000002;
/// Far beyond every fixture's total difficulty, so the weight never cuts a
/// window here and the anchor alone decides membership.
const NETWORK_DIFFICULTY: u128 = 1_000_000;
const HOUR_MS: i64 = 3_600_000;

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("clock:{id:064x}"),
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

struct Fixture {
    db: FixtureDatabase,
    ledger: Ledger,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let db = FixtureDatabase::open(&raw, "prism_clock_").await?;
        match Ledger::connect(&db.url, "clock-a".into(), 4, true).await {
            Ok(ledger) => Ok(Some(Self { db, ledger })),
            Err(error) => Err(db.abandon(error).await),
        }
    }

    fn pool(&self) -> &PgPool {
        &self.ledger.pool
    }

    async fn close(self, result: Result<()>) -> Result<()> {
        self.ledger.pool.close().await;
        self.db.close(result).await
    }
}

async fn now_ms(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(pool)
            .await?,
    )
}

async fn stored_clock(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT ledger_clock_ms FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(pool)
            .await?,
    )
}

/// Overwrites the stored clock, as only a test may: production writers only
/// raise it.
async fn set_stored_clock(pool: &PgPool, ms: i64) -> Result<()> {
    sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=$1 WHERE singleton")
        .bind(ms)
        .execute(pool)
        .await?;
    Ok(())
}

/// Every share's `(share_seq, accepted_at)` in `share_seq` order, the stamp
/// in UNIX ms.
async fn stamps(pool: &PgPool) -> Result<Vec<(i64, i64)>> {
    Ok(sqlx::query_as(
        "SELECT share_seq,floor(extract(epoch FROM accepted_at)*1000)::bigint \
         FROM qbit_share_ledger ORDER BY share_seq",
    )
    .fetch_all(pool)
    .await?)
}

/// Stamps rise (never fall) with `share_seq`.
fn ensure_monotonic(stamps: &[(i64, i64)]) -> Result<()> {
    for pair in stamps.windows(2) {
        ensure!(
            pair[0].1 <= pair[1].1,
            "share {} is stamped {} below share {}'s {}: {stamps:?}",
            pair[1].0,
            pair[1].1,
            pair[0].0,
            pair[0].1
        );
    }
    Ok(())
}

fn credited(snapshot: &qbit_prism_server::ledger::Snapshot) -> Vec<String> {
    snapshot
        .shares
        .iter()
        .map(|share| share.share_id.clone())
        .collect()
}

/// A share appended after an anchor is stamped after it, and a share
/// appended before is in the anchor's window, whether the stamps come from
/// the database's clock or from a stored clock ahead of it.
#[tokio::test]
async fn a_share_appended_after_an_anchor_is_stamped_after_it() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        for (round, ahead) in [(0_u64, false), (1, true)] {
            if ahead {
                set_stored_clock(f.pool(), now_ms(f.pool()).await? + HOUR_MS).await?;
            }
            let before = f.ledger.append(share(10 * round + 1), None).await?.share;
            let snapshot = f.ledger.snapshot(NETWORK_DIFFICULTY).await?;
            ensure!(
                snapshot.anchor_ms >= before.accepted_at_ms,
                "round {round}: anchor {} is below the share appended before it, stamped {}",
                snapshot.anchor_ms,
                before.accepted_at_ms
            );
            if ahead {
                ensure!(
                    snapshot.anchor_ms == before.accepted_at_ms,
                    "the anchor {} is not the stored clock's {}",
                    snapshot.anchor_ms,
                    before.accepted_at_ms
                );
            }
            ensure!(credited(&snapshot).contains(&before.share_id));
            ensure!(stored_clock(f.pool()).await? == snapshot.anchor_ms + 1);
            let after = f.ledger.append(share(10 * round + 2), None).await?.share;
            ensure!(
                after.accepted_at_ms > snapshot.anchor_ms,
                "round {round}: a share appended after anchor {} is stamped {}",
                snapshot.anchor_ms,
                after.accepted_at_ms
            );
            if ahead {
                ensure!(after.accepted_at_ms == snapshot.anchor_ms + 1);
            }
        }
        ensure_monotonic(&stamps(f.pool()).await?)
    }
    .await;
    f.close(result).await
}

/// The issue's clock skew: the database's clock (and the stored clock) behind
/// the newest share. The next share is stamped with the newest share's stamp,
/// never below it; a share whose job was issued at that stamp is admitted, as
/// the stored clock admitted it before; and the anchor covers both shares, so
/// neither drops out of a window (#738's anchor correction).
#[tokio::test]
async fn appends_and_anchors_follow_the_newest_share_when_the_clock_runs_behind_it() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let ahead = now_ms(f.pool()).await? + HOUR_MS;
        set_stored_clock(f.pool(), ahead).await?;
        let first = f.ledger.append(share(1), None).await?.share;
        ensure!(first.accepted_at_ms == ahead, "{first:?}");
        // The stored clock no longer covers the share: the clock is an hour
        // behind the newest stamp.
        set_stored_clock(f.pool(), 0).await?;
        let mut issued_then = share(2);
        issued_then.job_issued_at_ms = ahead;
        let second = f.ledger.append(issued_then, None).await?.share;
        ensure!(
            second.accepted_at_ms == ahead,
            "with the clock behind the newest share the next was stamped {}, not {ahead}",
            second.accepted_at_ms
        );
        let mut later_job = share(3);
        later_job.job_issued_at_ms = ahead + 1;
        let error = f
            .ledger
            .append(later_job, None)
            .await
            .err()
            .context("a share from a job issued after every clock was appended")?;
        ensure!(
            format!("{error:#}").contains("share references a job from the future"),
            "{error:#}"
        );
        ensure!(
            stored_clock(f.pool()).await? == 0,
            "an append wrote the stored clock"
        );
        let snapshot = f.ledger.snapshot(NETWORK_DIFFICULTY).await?;
        ensure!(
            snapshot.anchor_ms == ahead,
            "the anchor {} does not cover the newest share {ahead}",
            snapshot.anchor_ms
        );
        let ids = credited(&snapshot);
        ensure!(
            ids.contains(&first.share_id) && ids.contains(&second.share_id),
            "a committed share dropped out of the window: {ids:?}"
        );
        let third = f.ledger.append(share(4), None).await?.share;
        ensure!(third.accepted_at_ms == ahead + 1, "{third:?}");
        ensure_monotonic(&stamps(f.pool()).await?)
    }
    .await;
    f.close(result).await
}

/// The newest share's stamp is read back to the millisecond it was stamped
/// with, which its stored conversion misses by a microsecond either way past
/// about 2^43 ms (`window_read_oracle`): 8796093022998 is stored low and
/// 8796093022999 high. A row stamped with a fraction of a millisecond, as a
/// legacy writer could, is followed by the next millisecond, so a later share
/// and the anchor are never below it.
#[tokio::test]
async fn the_newest_stamp_is_read_back_to_the_millisecond_it_was_stamped_with() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let mut id = 0;
        for ms in [8_796_093_022_998_i64, 8_796_093_022_999, 8_796_093_023_000] {
            set_stored_clock(f.pool(), ms).await?;
            id += 1;
            ensure!(f.ledger.append(share(id), None).await?.share.accepted_at_ms == ms);
            set_stored_clock(f.pool(), 0).await?;
            id += 1;
            let next = f.ledger.append(share(id), None).await?.share;
            ensure!(
                next.accepted_at_ms == ms,
                "the share after one stamped {ms} was stamped {}",
                next.accepted_at_ms
            );
            set_stored_clock(f.pool(), 0).await?;
            let snapshot = f.ledger.snapshot(NETWORK_DIFFICULTY).await?;
            ensure!(
                snapshot.anchor_ms == ms,
                "anchored {} after {ms}",
                snapshot.anchor_ms
            );
            ensure!(snapshot.shares.len() == usize::try_from(id)?);
            set_stored_clock(f.pool(), 0).await?;
        }
        let legacy_us = (8_796_093_023_000_i64 + 10) * 1_000 + 400;
        sqlx::query(
            "INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,\
             share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,\
             accepted_at,writer_id,writer_epoch) \
             VALUES('legacy','miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',\
             to_timestamp(0.001),1,'epoch'::timestamptz+$1*interval '1 microsecond','legacy',0)",
        )
        .bind(legacy_us)
        .execute(f.pool())
        .await?;
        id += 1;
        let after = f.ledger.append(share(id), None).await?.share;
        ensure!(
            after.accepted_at_ms == 8_796_093_023_011,
            "the share after a row stamped {legacy_us} us was stamped {} ms",
            after.accepted_at_ms
        );
        let snapshot = f.ledger.snapshot(NETWORK_DIFFICULTY).await?;
        ensure!(snapshot
            .shares
            .iter()
            .any(|share| share.share_id == "legacy"));
        ensure_monotonic(&stamps(f.pool()).await?)
    }
    .await;
    f.close(result).await
}

/// The newest own share is looked for between the share probe's bounds,
/// then, when none is there, in every partition: after the sequence has
/// jumped three partitions past it (as a peer's rows move a dual-writer
/// node's sequence while it idles), the next share is still stamped no lower
/// than it, and the one after finds that share in the lead.
#[tokio::test]
async fn the_newest_own_share_below_the_probe_floor_still_bounds_the_next_stamp() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let ahead = now_ms(f.pool()).await? + HOUR_MS;
        set_stored_clock(f.pool(), ahead).await?;
        let first = f.ledger.append(share(1), None).await?.share;
        ensure!(first.accepted_at_ms == ahead);
        set_stored_clock(f.pool(), 0).await?;
        let width: i64 = sqlx::query_scalar(
            "SELECT partition_rows FROM qbit_prism_share_partitioning WHERE singleton",
        )
        .fetch_one(f.pool())
        .await?;
        sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
            .bind(3 * width + 9)
            .execute(f.pool())
            .await?;
        qbit_prism_server::partitions::ensure(f.pool()).await?;
        let floor: i64 = sqlx::query_scalar("SELECT qbit_prism_share_probe_floor()")
            .fetch_one(f.pool())
            .await?;
        ensure!(
            floor > i64::try_from(first.share_seq)?,
            "the probe floor {floor} did not pass share {}",
            first.share_seq
        );
        for id in [2, 3] {
            let next = f.ledger.append(share(id), None).await?.share;
            ensure!(
                next.accepted_at_ms == ahead,
                "share {id} was stamped {} below the newest own share's {ahead}",
                next.accepted_at_ms
            );
        }
        ensure_monotonic(&stamps(f.pool()).await?)
    }
    .await;
    f.close(result).await
}

/// Two frontends on one database, appending in turn while the stored clock is
/// pinned ahead and wound back between them: every stamp is at least the
/// one before it in `share_seq` order, and a window anchored afterwards holds
/// every share.
#[tokio::test]
async fn two_frontends_stamp_monotonically_in_share_seq_order() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let other = Ledger::connect(&f.db.url, "clock-b".into(), 4, true).await?;
        let inner = async {
            let base = now_ms(f.pool()).await?;
            // Each step: who appends, and the stored clock it finds.
            let steps = [
                (0, None),
                (1, Some(base + HOUR_MS)),
                (0, Some(0)),
                (1, None),
                (0, Some(base + 2 * HOUR_MS)),
                (1, Some(base + HOUR_MS)),
                (0, Some(0)),
                (1, Some(0)),
            ];
            let mut stamped = Vec::new();
            for (index, (frontend, clock)) in steps.into_iter().enumerate() {
                if let Some(clock) = clock {
                    set_stored_clock(f.pool(), clock).await?;
                }
                let ledger = if frontend == 0 { &f.ledger } else { &other };
                let appended = ledger.append(share(index as u64), None).await?;
                ensure!(appended.inserted);
                stamped.push(appended.share.accepted_at_ms);
            }
            ensure!(
                stamped.last() == Some(&(base + 2 * HOUR_MS)),
                "the newest stamp {stamped:?} lost the clock's high-water mark"
            );
            let rows = stamps(f.pool()).await?;
            ensure!(rows.len() == steps.len());
            ensure_monotonic(&rows)?;
            let snapshot = other.snapshot(NETWORK_DIFFICULTY).await?;
            ensure!(
                snapshot.shares.len() == steps.len(),
                "the window anchored at {} holds {} of {} shares",
                snapshot.anchor_ms,
                snapshot.shares.len(),
                steps.len()
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        other.pool.close().await;
        inner
    }
    .await;
    f.close(result).await
}

/// The peer sync's own-log recovery raises the stored clock to the newest
/// own stamp the peer held (`Ledger::raise_own_sequences`): every later
/// stamp respects that floor, and a floor below the newest share lowers no
/// stamp.
#[tokio::test]
async fn stamps_respect_the_peer_sync_clock_floor_and_never_fall_below_the_newest_share(
) -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        f.ledger
            .set_node_identity(NodeIndex::A, "ledger-clock-test")
            .await?;
        let first = f.ledger.append(share(1), None).await?.share;
        let floor = first.accepted_at_ms + HOUR_MS;
        f.ledger
            .raise_own_sequences(NodeIndex::A, None, None, Some(floor))
            .await?;
        ensure!(stored_clock(f.pool()).await? == floor);
        let second = f.ledger.append(share(2), None).await?.share;
        ensure!(
            second.accepted_at_ms == floor,
            "the share after the floor {floor} was stamped {}",
            second.accepted_at_ms
        );
        // A lower floor changes nothing, and no stamp falls under the newest
        // share's even once the stored clock is wound below it.
        f.ledger
            .raise_own_sequences(NodeIndex::A, None, None, Some(first.accepted_at_ms))
            .await?;
        ensure!(stored_clock(f.pool()).await? == floor, "the floor fell");
        set_stored_clock(f.pool(), 0).await?;
        let third = f.ledger.append(share(3), None).await?.share;
        ensure!(
            third.accepted_at_ms == floor,
            "the share after the floor was wound back was stamped {}",
            third.accepted_at_ms
        );
        ensure_monotonic(&stamps(f.pool()).await?)
    }
    .await;
    f.close(result).await
}

/// Inserts a row as the peer sync does for a peer's share: its node, its
/// sequence and its own stamp, verbatim.
async fn insert_peer_row(pool: &PgPool, origin: i16, seq: i64, accepted_ms: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,\
         share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,\
         writer_id,writer_epoch,origin_node) \
         VALUES($1,'peer:'||$1,'miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',\
         to_timestamp(0.001),1,to_timestamp($2::double precision/1000),'peer',0,$3)",
    )
    .bind(seq)
    .bind(accepted_ms)
    .bind(origin)
    .execute(pool)
    .await?;
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1,true)")
        .bind(seq)
        .execute(pool)
        .await?;
    Ok(())
}

/// Only this node's own shares carry its clock: a peer's row above the
/// newest own share, stamped by the peer's clock, neither lowers the next
/// own stamp (stamped behind it) nor raises it (stamped ahead), and a
/// dual-writer frontend reads its own node's rows. 3.0 stamped exactly so.
#[tokio::test]
async fn a_peer_row_stamped_by_the_peers_clock_moves_no_own_stamp() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        f.ledger.set_dual_writer_identity(NodeIdentity {
            node: NodeIndex::A,
            carry_owner: true,
        })?;
        let ahead = now_ms(f.pool()).await? + HOUR_MS;
        set_stored_clock(f.pool(), ahead).await?;
        let own = f.ledger.append(share(1), None).await?.share;
        ensure!(own.accepted_at_ms == ahead);
        set_stored_clock(f.pool(), 0).await?;
        let seq = i64::try_from(own.share_seq)?;
        insert_peer_row(f.pool(), 1, seq + 1, 1_000).await?;
        let next = f.ledger.append(share(2), None).await?.share;
        ensure!(
            next.accepted_at_ms == ahead,
            "a peer row stamped behind the newest own share lowered the next stamp to {}",
            next.accepted_at_ms
        );
        let next_seq = i64::try_from(next.share_seq)?;
        insert_peer_row(f.pool(), 1, next_seq + 1, ahead + 10 * HOUR_MS).await?;
        let last = f.ledger.append(share(3), None).await?.share;
        ensure!(
            last.accepted_at_ms == ahead,
            "a peer row stamped ahead raised the next own stamp to {}",
            last.accepted_at_ms
        );
        Ok(())
    }
    .await;
    f.close(result).await
}

/// Before #738 every share append stamped and stored the clock in its write
/// statement, verbatim from 3.1.0 (`ledger/window.rs`). An older frontend
/// still runs it while a newer one serves the same database in a rolling
/// upgrade.
const OLD_APPEND_WRITE_SQL: &str = "WITH clock AS (UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint) WHERE singleton RETURNING ledger_clock_ms),\
     appended AS (INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) \
        SELECT $1,$2,$3,decode($4,'hex'),$5::text::numeric,$6::text::numeric,$7,$8,to_timestamp($9::double precision/1000),$10,to_timestamp(clock.ledger_clock_ms::double precision/1000),$11,true,$12,0 \
        FROM clock WHERE $9<=clock.ledger_clock_ms RETURNING share_seq),\
     hashed AS (INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT $13,$1 FROM appended) \
     SELECT clock.ledger_clock_ms AS accepted_at_ms,appended.share_seq FROM clock LEFT JOIN appended ON true";

/// A new share appended as a 3.1.0 frontend does, under `ORDER_LOCK`: its
/// reads before the write find nothing for a new share and change nothing,
/// so the write is all that differs. Returns the stamp.
async fn old_frontend_append(pool: &PgPool, id: u64) -> Result<i64> {
    let share = share(id);
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(ORDER_LOCK)
        .execute(&mut *tx)
        .await?;
    let (accepted_at_ms, seq): (i64, Option<i64>) = sqlx::query_as(OLD_APPEND_WRITE_SQL)
        .bind(&share.share_id)
        .bind(&share.miner_id)
        .bind(&share.order_key)
        .bind(&share.p2mr_program_hex)
        .bind(share.share_difficulty.to_string())
        .bind(share.network_difficulty.to_string())
        .bind(i64::try_from(share.template_height)?)
        .bind(&share.job_id)
        .bind(share.job_issued_at_ms)
        .bind(i64::from(share.ntime))
        .bind(&share.credit_policy)
        .bind("clock-old")
        .bind(format!("{id:064x}"))
        .fetch_one(&mut *tx)
        .await?;
    seq.context("the old write inserted nothing")?;
    tx.commit().await?;
    Ok(accepted_at_ms)
}

/// EP-COMPAT: a 3.1.0 frontend, which stores the clock on every append, and
/// this one, which derives it, interleave on one ledger with the stored clock
/// ahead of the database's and moving: stamps rise with `share_seq` and
/// every window holds every share. Each direction holds by construction: the
/// old write stores its stamp, which the derivation reads; the derivation's
/// stamp is the stored clock or the database's clock or an earlier own stamp
/// the stored clock already covers, which the old write reads in turn. The
/// one interleaving this cannot cover is a backward step of the database's
/// own clock between a new append stamped by that clock and an old one: the
/// old write cannot see the new share's stamp. That needs the step and both
/// versions serving at once.
#[tokio::test]
async fn an_older_frontend_storing_the_clock_and_this_one_interleave_monotonically() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let base = now_ms(f.pool()).await?;
        let mut expected = Vec::new();
        for (index, (old, clock)) in [
            (true, None),
            (false, None),
            (true, Some(base + HOUR_MS)),
            (false, None),
            (false, None),
            (true, None),
            (false, Some(base + 2 * HOUR_MS)),
            (true, None),
            (false, None),
        ]
        .into_iter()
        .enumerate()
        {
            if let Some(clock) = clock {
                // Raised only, as the anchor and the peer sync raise it.
                sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,$1) WHERE singleton")
                    .bind(clock)
                    .execute(f.pool())
                    .await?;
            }
            let stamp = if old {
                old_frontend_append(f.pool(), index as u64).await?
            } else {
                f.ledger.append(share(index as u64), None).await?.share.accepted_at_ms
            };
            expected.push(stamp);
            if index == 4 {
                let snapshot = f.ledger.snapshot(NETWORK_DIFFICULTY).await?;
                ensure!(snapshot.shares.len() == index + 1, "{snapshot:?}");
            }
        }
        let rows = stamps(f.pool()).await?;
        ensure!(
            rows.iter().map(|(_, stamp)| *stamp).collect::<Vec<_>>() == expected,
            "{rows:?} != {expected:?}"
        );
        ensure_monotonic(&rows)?;
        let snapshot = f.ledger.snapshot(NETWORK_DIFFICULTY).await?;
        ensure!(snapshot.shares.len() == rows.len());
        ensure!(snapshot.anchor_ms >= base + 2 * HOUR_MS);
        Ok(())
    }
    .await;
    f.close(result).await
}

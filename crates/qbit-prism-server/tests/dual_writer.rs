//! 3.1 dual writer, node identity and peer sync (D1), on local PostgreSQL:
//! each node is its own fixture database.
use anyhow::{ensure, Result};
use qbit_prism_server::{
    ledger::{IdentityCheck, Ledger},
    node_identity::NodeIndex,
    peer_sync::COPIED_TABLES,
};
use qbit_prism_test_gate as gate;
use sqlx::PgPool;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;

/// A column's type, nullability and default, as the catalog states them.
async fn column(
    pool: &PgPool,
    table: &str,
    column: &str,
) -> Result<Option<(String, bool, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid) \
         FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum \
         WHERE a.attrelid=to_regclass($1) AND a.attname=$2 AND NOT a.attisdropped",
    )
    .bind(table)
    .bind(column)
    .fetch_optional(pool)
    .await?)
}

/// Migration 027 gives every copied table `origin_node smallint NOT NULL
/// DEFAULT 0`, the share ledger's partitions included, gives the two
/// non-share streams' root tables a `sync_seq` drawn by
/// `qbit_prism_next_sync_seq()`, and creates the carry-owner journal, the
/// node identity, the lineage, cursor and conflict tables empty. The safe
/// peer mark is NULL before any pull, and the journal is append-only.
#[tokio::test]
async fn migration_027_adds_origin_node_to_every_copied_table() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "dual_writer_027_").await?;
    let ledger = Ledger::connect(&db.url, "node-a".into(), 4, true).await?;
    let result = async {
        let pool = &ledger.pool;
        for table in COPIED_TABLES {
            let found = column(pool, table, "origin_node").await?;
            ensure!(
                found == Some(("smallint".into(), true, Some("0".into()))),
                "{table}.origin_node is {found:?}"
            );
        }
        let leaves: Vec<String> = sqlx::query_scalar(
            "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent='qbit_share_ledger'::regclass ORDER BY 1",
        )
        .fetch_all(pool)
        .await?;
        ensure!(!leaves.is_empty(), "the share ledger has no partitions");
        for leaf in &leaves {
            let found = column(pool, leaf, "origin_node").await?;
            ensure!(
                found
                    .as_ref()
                    .is_some_and(|(kind, not_null, _)| kind == "smallint" && *not_null),
                "partition {leaf}.origin_node is {found:?}"
            );
        }
        for table in ["qbit_pool_blocks", "qbit_prism_jobs"] {
            let found = column(pool, table, "sync_seq").await?;
            ensure!(
                found
                    == Some((
                        "bigint".into(),
                        false,
                        Some("qbit_prism_next_sync_seq()".into())
                    )),
                "{table}.sync_seq is {found:?}"
            );
        }
        for table in [
            "qbit_prism_node_roles",
            "qbit_prism_node_identity",
            "qbit_prism_node_lineage",
            "qbit_prism_peer_sync_cursors",
            "qbit_prism_peer_sync_conflicts",
        ] {
            let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(pool)
                .await?;
            ensure!(
                rows == 0,
                "{table} holds {rows} rows before any dual-writer step"
            );
        }
        let mark: Option<i64> = sqlx::query_scalar("SELECT qbit_prism_peer_share_mark()")
            .fetch_one(pool)
            .await?;
        ensure!(mark.is_none(), "the safe peer mark is {mark:?} before any pull");
        // The sync number is taken after the inserting transaction's xid, and
        // grows with each insert.
        let (first, second, xid_held): (i64, i64, bool) = sqlx::query_as(
            "SELECT qbit_prism_next_sync_seq(),qbit_prism_next_sync_seq(),pg_current_xact_id_if_assigned() IS NOT NULL",
        )
        .fetch_one(pool)
        .await?;
        ensure!(second > first && xid_held, "{first} {second} {xid_held}");
        // The carry-owner journal takes new epochs and refuses every change.
        sqlx::query("INSERT INTO qbit_prism_node_roles(origin_node,epoch,carry_owner,action,recorded_by) VALUES(0,0,true,'seed','test')")
            .execute(pool)
            .await?;
        for statement in [
            "UPDATE qbit_prism_node_roles SET carry_owner=false",
            "DELETE FROM qbit_prism_node_roles",
            "TRUNCATE qbit_prism_node_roles",
        ] {
            let error = sqlx::query(statement)
                .execute(pool)
                .await
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default();
            ensure!(error.contains("append-only journal"), "{statement}: {error}");
        }
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    db.close(result).await
}

fn share(id: u64) -> qbit_prism::AcceptedShare {
    qbit_prism::AcceptedShare {
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

/// Appends `ids` and returns each row's `(share_seq, origin_node)` and its
/// header mapping's `origin_node`.
async fn append(ledger: &Ledger, ids: std::ops::Range<u64>) -> Result<Vec<(i64, i16, i16)>> {
    let mut rows = Vec::new();
    for id in ids {
        let appended = ledger.append(share(id), None).await?;
        ensure!(appended.inserted, "share {id} was not appended");
        rows.push(
            sqlx::query_as(
                "SELECT s.share_seq,s.origin_node,h.origin_node FROM qbit_share_ledger s \
                 JOIN qbit_prism_share_hashes h ON h.share_id=s.share_id WHERE s.share_id=$1",
            )
            .bind(format!("worker:{id:064x}"))
            .fetch_one(&ledger.pool)
            .await?,
        );
    }
    Ok(rows)
}

/// A sequence's step, bounds and whether it cycles.
async fn sequence(pool: &PgPool, name: &str) -> Result<(i64, i64, i64, bool)> {
    Ok(sqlx::query_as(
        "SELECT seqincrement,seqmin,seqmax,seqcycle FROM pg_sequence WHERE seqrelid=$1::regclass",
    )
    .bind(name)
    .fetch_one(pool)
    .await?)
}

async fn origin_default(pool: &PgPool, table: &str) -> Result<Option<String>> {
    Ok(column(pool, table, "origin_node")
        .await?
        .and_then(|(_, _, default)| default))
}

/// Three fixture databases with a ledger each, closed whatever the test does.
async fn three_ledgers(
    raw: &str,
    test: impl AsyncFnOnce(&Ledger, &Ledger, &Ledger) -> Result<()>,
) -> Result<()> {
    let a_db = ledger_database::FixtureDatabase::open(raw, "dual_writer_a_").await?;
    let b_db = match ledger_database::FixtureDatabase::open(raw, "dual_writer_b_").await {
        Ok(db) => db,
        Err(error) => return a_db.close(Err(error)).await,
    };
    let c_db = match ledger_database::FixtureDatabase::open(raw, "dual_writer_c_").await {
        Ok(db) => db,
        Err(error) => {
            let result = b_db.close(Err(error)).await;
            return a_db.close(result).await;
        }
    };
    let result = async {
        let a = Ledger::connect(&a_db.url, "node-a".into(), 4, true).await?;
        let b = Ledger::connect(&b_db.url, "node-b".into(), 4, true).await?;
        let c = Ledger::connect(&c_db.url, "single".into(), 4, true).await?;
        let result = test(&a, &b, &c).await;
        for ledger in [a, b, c] {
            ledger.pool.close().await;
        }
        result
    }
    .await;
    let result = c_db.close(result).await;
    let result = b_db.close(result).await;
    a_db.close(result).await
}

/// `node-identity set` makes one database node A's and the other node B's:
/// rows each writes carry its index, its key sequences step by 2 with its
/// parity above everything it held, and its sessions come from its half of
/// the extranonce1 space. A third database nobody personalised keeps 3.0's
/// contiguous sequence, full session range and node-0 defaults. Setting the
/// same node again changes nothing and restores what was lost; setting the
/// other node is refused. A dual-writer frontend's check reports each case.
#[tokio::test]
async fn node_identity_set_gives_each_node_its_origin_parity_and_extranonce_half() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    three_ledgers(&raw, async |a, b, single| {
        // Before 3.1 every writer is node 0 with one contiguous sequence.
        for ledger in [a, b, single] {
            ensure!(append(ledger, 1..4).await? == [(1, 0, 0), (2, 0, 0), (3, 0, 0)]);
        }
        ensure!(a.check_node_identity(NodeIndex::A).await? == IdentityCheck::Unidentified);
        let set_a = a.set_node_identity(NodeIndex::A, "test").await?;
        let set_b = b.set_node_identity(NodeIndex::B, "test").await?;
        ensure!(
            set_a.node == NodeIndex::A && set_a.recorded_by == "test",
            "{set_a:?}"
        );
        ensure!(set_b.node == NodeIndex::B, "{set_b:?}");
        ensure!(a.recorded_node_identity().await? == Some(set_a.clone()));
        ensure!(single.recorded_node_identity().await?.is_none());
        let lineage_a = a.node_lineage().await?.expect("set records the lineage");
        let lineage_b = b.node_lineage().await?.expect("set records the lineage");
        ensure!(
            (lineage_a.share_seq_floor, lineage_b.share_seq_floor) == (4, 3),
            "{lineage_a:?} {lineage_b:?}"
        );
        ensure!(lineage_a.verified.is_none(), "no own-log check has run");
        // Each node's rows carry its index, with its parity, above the floor.
        ensure!(append(a, 10..13).await? == [(6, 0, 0), (8, 0, 0), (10, 0, 0)]);
        ensure!(append(b, 20..23).await? == [(5, 1, 1), (7, 1, 1), (9, 1, 1)]);
        ensure!(append(single, 30..33).await? == [(4, 0, 0), (5, 0, 0), (6, 0, 0)]);
        for table in COPIED_TABLES {
            for (ledger, expected) in [(a, "0"), (b, "1"), (single, "0")] {
                ensure!(
                    origin_default(&ledger.pool, table).await?.as_deref() == Some(expected),
                    "{} {table}",
                    ledger.instance_id
                );
            }
        }
        for (pool, step) in [(&a.pool, 2), (&b.pool, 2), (&single.pool, 1)] {
            for name in [
                "qbit_share_ledger_share_seq_seq",
                "qbit_pool_payout_entries_payout_entry_seq_seq",
                "qbit_payout_carry_forward_carry_forward_seq_seq",
            ] {
                ensure!(
                    sequence(pool, name).await?.0 == step,
                    "{name} steps by {step}"
                );
            }
        }
        // Sessions come from each node's half; the single writer's span both.
        let session = "qbit_prism_session_sequence";
        ensure!(sequence(&a.pool, session).await? == (1, 1, 0x7fff_ffff, true));
        ensure!(sequence(&b.pool, session).await? == (1, 0x8000_0000, 0xffff_ffff, true));
        ensure!(sequence(&single.pool, session).await? == (1, 1, 0xffff_ffff, true));
        let session_a = a.new_session_id().await?;
        let session_b = b.new_session_id().await?;
        ensure!(NodeIndex::A
            .extranonce1_range()
            .contains(&session_a.value()));
        ensure!(NodeIndex::B
            .extranonce1_range()
            .contains(&session_b.value()));
        drop((session_a, session_b));
        // What a dual-writer frontend's start finds.
        ensure!(a.check_node_identity(NodeIndex::A).await? == IdentityCheck::Ready(set_a.clone()));
        ensure!(
            a.check_node_identity(NodeIndex::B).await? == IdentityCheck::OtherNode(set_a.clone())
        );
        ensure!(single.check_node_identity(NodeIndex::A).await? == IdentityCheck::Unidentified);
        // Setting the same node again changes nothing.
        let last = "SELECT last_value FROM qbit_share_ledger_share_seq_seq";
        let before: i64 = sqlx::query_scalar(last).fetch_one(&a.pool).await?;
        ensure!(a.set_node_identity(NodeIndex::A, "again").await? == set_a);
        ensure!(
            sqlx::query_scalar::<_, i64>(last)
                .fetch_one(&a.pool)
                .await?
                == before
        );
        // A lost default and step are reported at start, and restored by set.
        sqlx::raw_sql(
            "ALTER TABLE qbit_pool_blocks ALTER COLUMN origin_node SET DEFAULT 0; \
             ALTER SEQUENCE qbit_pool_payout_entries_payout_entry_seq_seq INCREMENT BY 1",
        )
        .execute(&b.pool)
        .await?;
        let drifted = b.check_node_identity(NodeIndex::B).await?;
        ensure!(
            drifted
                == IdentityCheck::Drifted(
                    set_b.clone(),
                    vec![
                        "qbit_pool_blocks.origin_node default".into(),
                        "qbit_pool_payout_entries.payout_entry_seq parity".into()
                    ]
                ),
            "{drifted:?}"
        );
        ensure!(b.set_node_identity(NodeIndex::B, "repair").await? == set_b);
        ensure!(
            origin_default(&b.pool, "qbit_pool_blocks")
                .await?
                .as_deref()
                == Some("1")
        );
        let payout_sequence = "qbit_pool_payout_entries_payout_entry_seq_seq";
        ensure!(sequence(&b.pool, payout_sequence).await?.0 == 2);
        ensure!(b.check_node_identity(NodeIndex::B).await? == IdentityCheck::Ready(set_b));
        // Setting the other node is refused, naming both.
        let error = a
            .set_node_identity(NodeIndex::B, "test")
            .await
            .err()
            .map(|error| format!("{error:#}"))
            .unwrap_or_default();
        ensure!(
            error.contains("already dual-writer node A") && error.contains("make it node B"),
            "{error}"
        );
        ensure!(a.recorded_node_identity().await? == Some(set_a));
        Ok(())
    })
    .await
}

/// CONTRACT D-12: a single writer starts on a database whose carry-owner
/// journal is empty, as 3.0 did, and refuses one that has run as a
/// dual-writer node unless PRISM_DUAL_WRITER_DOWNGRADE says so.
#[tokio::test]
async fn a_single_writer_refuses_a_dual_writer_database_without_the_downgrade_flag() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "dual_writer_downgrade_").await?;
    let ledger = Ledger::connect(&db.url, "single".into(), 4, true).await?;
    let result = async {
        ledger.refuse_single_writer_on_dual_ledger(false).await?;
        sqlx::query("INSERT INTO qbit_prism_node_roles(origin_node,epoch,carry_owner,action,recorded_by) VALUES(0,0,true,'seed','test')")
            .execute(&ledger.pool)
            .await?;
        let error = ledger
            .refuse_single_writer_on_dual_ledger(false)
            .await
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        ensure!(
            error.contains("has run as a dual-writer node")
                && error.contains("PRISM_DUAL_WRITER_DOWNGRADE=1"),
            "{error}"
        );
        ledger.refuse_single_writer_on_dual_ledger(true).await?;
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    db.close(result).await
}

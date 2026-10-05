//! Migration 016's solver attribution for a 2.x.x source's blocks (#672).
//!
//! The migrator fills `qbit_pool_blocks.solver_*` for the blocks the pool
//! found before 016 in statements of at most 128 blocks, inside the
//! migration transaction, instead of one UPDATE over every block. The test
//! holds the result to the single statement's lookup on more blocks than
//! two statements cover, with the cases that lookup decides: the latest
//! accepted share wins, then the highest `share_seq`; case is folded;
//! rejected shares, bare 64-digit IDs and blocks without a share leave the
//! columns NULL.
use super::*;
use qbit_prism_server::ledger::REQUIRED_SCHEMA_VERSIONS;

/// Blocks the seed writes: more than two of the migrator's statements.
const BLOCKS: i64 = 300;

/// 300 blocks with hashes spread over the key space, each in one of six
/// cases by `i % 6`: one solving share; an earlier and a later one, the
/// later in upper case; two at the same instant, the higher `share_seq`
/// winning; a rejected share only; no share; a bare 64-digit share ID.
async fn seed(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256) \
         SELECT md5(i::text)||md5((i+1)::text),i,'parent','coinbase-'||i,'manifest' FROM generate_series(1,$1::bigint) i",
    )
    .bind(BLOCKS)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,reject_reason,writer_id,writer_epoch) \
         SELECT s.share_id,s.miner,s.miner,decode(repeat('11',32),'hex'),s.difficulty,100,100,'job',to_timestamp(1),1,s.at,s.accepted,CASE WHEN s.accepted THEN NULL ELSE 'duplicate-share' END,'solver-test',0 \
         FROM generate_series(1,$1::bigint) i, LATERAL (SELECT md5(i::text)||md5((i+1)::text) AS h) b, \
         LATERAL (VALUES \
             (i%6=0,'w'||i%3||'.rig:'||b.h,'miner-'||i%3,7,to_timestamp(1000+i),true), \
             (i%6=1,'early.rig:'||b.h,'early',3,to_timestamp(1000+i),true), \
             (i%6=1,'late.rig:'||upper(b.h),'late',5,to_timestamp(2000+i),true), \
             (i%6=2,'first.rig:'||b.h,'first',2,to_timestamp(1000+i),true), \
             (i%6=2,'second.rig:'||b.h,'second',4,to_timestamp(1000+i),true), \
             (i%6=3,'rejected.rig:'||b.h,'rejected',6,to_timestamp(1000+i),false), \
             (i%6=5,b.h,'bare',8,to_timestamp(1000+i),true) \
         ) AS s(wanted,share_id,miner,difficulty,at,accepted) WHERE s.wanted ORDER BY i,s.at,s.share_id",
    )
    .bind(BLOCKS)
    .execute(pool)
    .await?;
    Ok(())
}

type Solver = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// What 016's single statement attributed to each block: the latest
/// accepted share whose ID ends in the block's hash.
async fn expected(pool: &PgPool) -> Result<Vec<Solver>> {
    Ok(sqlx::query_as(
        "SELECT found.block_hash,share.miner_id,share.share_id,share.share_difficulty::text,share.network_difficulty::text FROM qbit_pool_blocks found \
         LEFT JOIN LATERAL (SELECT share.miner_id,share.share_id,share.share_difficulty,share.network_difficulty FROM qbit_share_ledger share \
             WHERE share.accepted AND length(share.share_id)>=65 AND lower(right(share.share_id,64))=found.block_hash \
             ORDER BY share.accepted_at DESC,share.share_seq DESC LIMIT 1) share ON true \
         ORDER BY found.block_hash",
    )
    .fetch_all(pool)
    .await?)
}

async fn attributed(pool: &PgPool) -> Result<Vec<Solver>> {
    Ok(sqlx::query_as(
        "SELECT block_hash,solver_miner_id,solver_share_id,solver_share_difficulty::text,solver_network_difficulty::text FROM qbit_pool_blocks ORDER BY block_hash",
    )
    .fetch_all(pool)
    .await?)
}

#[tokio::test]
async fn migration_016_attributes_a_2x_sources_blocks_as_its_single_statement_did() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed(&pool).await?;
    let expected = expected(&pool).await?;
    assert_eq!(expected.len(), BLOCKS as usize);
    // Every case of the lookup is there: half the blocks have a solver.
    let solved = expected.iter().filter(|block| block.2.is_some()).count();
    assert_eq!(solved, BLOCKS as usize / 2);
    assert!(expected.iter().any(|block| block
        .2
        .as_deref()
        .is_some_and(|id| id.starts_with("late.rig:"))));
    assert!(expected.iter().any(|block| block
        .2
        .as_deref()
        .is_some_and(|id| id.starts_with("second.rig:"))));
    assert!(!expected
        .iter()
        .any(|block| block
            .2
            .as_deref()
            .is_some_and(|id| id.starts_with("early.rig:")
                || id.starts_with("first.rig:")
                || id.starts_with("rejected.rig:")
                || id.len() == 64)));
    let ledger = db.ledger("cutover").await?;
    assert_eq!(attributed(&pool).await?, expected);
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&pool)
            .await?;
    assert_eq!(versions, REQUIRED_SCHEMA_VERSIONS);
    db.close(vec![ledger]).await
}

/// A native ledger an earlier build migrated before 016 existed applies 016
/// on the native path, with the same attribution: the ledger is put back to
/// before 017 and 016, given the same blocks and shares, and migrated again.
#[tokio::test]
async fn migration_016_attributes_a_native_ledgers_blocks_when_016_is_missing() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let earlier = db.ledger("earlier-build").await?;
    super::share_partitions::undo_017(&pool).await?;
    super::share_partitions::undo_016(&pool).await?;
    seed(&pool).await?;
    let expected = expected(&pool).await?;
    assert_eq!(
        expected.iter().filter(|block| block.2.is_some()).count(),
        BLOCKS as usize / 2
    );
    let migrated = db.ledger("this-build").await?;
    assert_eq!(attributed(&pool).await?, expected);
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&pool)
            .await?;
    assert_eq!(versions, REQUIRED_SCHEMA_VERSIONS);
    db.close(vec![earlier, migrated]).await
}

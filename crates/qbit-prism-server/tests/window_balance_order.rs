//! The current-balance read keeps its SQL order (djh58's ruling on #327).
//!
//! `Ledger::snapshot` and `Ledger::read_window(BalanceSource::Current)` both
//! return the carried balances in the order
//! `qbit_current_carry_forward_balances()` yields them; only a clone is sorted,
//! for the digest. That order is the canonical `(order_key, recipient_id,
//! p2mr_program_hex)` key, compared under the database's collation, so on a C
//! collation it already equals the byte-wise canonical sort and a sort added in
//! the decoder would change nothing observable. Here the carry table's text
//! columns use an ICU collation, as a database created with a natural-language
//! default would, so the SQL order and the byte-wise sort disagree, and the
//! test sees which one the readers return.
//!
//! `window_reference::as_issued_snapshot_preserves_persisted_balance_order`
//! covers the as-issued path.

use anyhow::{bail, ensure, Context, Result};
use qbit_prism::{AcceptedShare, CarryForwardBalance};
use qbit_prism_server::ledger::{BalanceSource, Ledger, WindowRef};
use qbit_prism_test_gate as gate;
use sqlx::PgPool;

/// ICU collations PostgreSQL 16 ships when built with ICU, which orders
/// `a-order` before `B-order`, where a byte-wise sort puts `B` first.
const COLLATIONS: &[&str] = &["und-x-icu", "en-x-icu", "en_US.utf8", "en_US"];

fn canonical(mut balances: Vec<CarryForwardBalance>) -> Vec<CarryForwardBalance> {
    balances.sort_by(|a, b| {
        a.order_key
            .cmp(&b.order_key)
            .then_with(|| a.recipient_id.cmp(&b.recipient_id))
            .then_with(|| a.p2mr_program_hex.cmp(&b.p2mr_program_hex))
    });
    balances
}

fn order_keys(balances: &[CarryForwardBalance]) -> Vec<&str> {
    balances
        .iter()
        .map(|balance| balance.order_key.as_str())
        .collect()
}

async fn body(pool: &PgPool, ledger: &Ledger) -> Result<()> {
    let mut collation = None;
    for name in COLLATIONS {
        let orders: Option<bool> = sqlx::query_scalar(&format!(
            "SELECT 'a-order' < ('B-order' COLLATE \"{name}\") FROM pg_collation WHERE collname=$1 LIMIT 1"
        ))
        .bind(name)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        if orders == Some(true) {
            collation = Some(*name);
            break;
        }
    }
    let Some(collation) = collation else {
        bail!(
            "none of the collations {COLLATIONS:?} exists on this server with a non-byte-wise order; \
             the test needs one to make the SQL order differ from the canonical sort"
        );
    };
    sqlx::raw_sql(&format!(
        "ALTER TABLE qbit_payout_carry_forward_current \
           ALTER COLUMN miner_id TYPE text COLLATE \"{collation}\", \
           ALTER COLUMN payout_order_key TYPE text COLLATE \"{collation}\"; \
         INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) \
           VALUES(repeat('aa',32),100,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed'); \
         INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) \
           VALUES(100,repeat('aa',32),'miner-b','B-order',decode(repeat('22',32),'hex'),500,0,500,0,500,'accrued'), \
                 (100,repeat('aa',32),'miner-a','a-order',decode(repeat('11',32),'hex'),1000,0,1000,0,1000,'accrued');"
    ))
    .execute(pool)
    .await?;
    ledger
        .append(
            AcceptedShare {
                share_seq: 0,
                share_id: "balance-order:share".into(),
                miner_id: "miner-a".into(),
                order_key: "a-order".into(),
                p2mr_program_hex: "11".repeat(32),
                share_difficulty: 800,
                network_difficulty: 100,
                template_height: 100,
                job_id: "job".into(),
                job_issued_at_ms: 1,
                accepted_at_ms: 0,
                ntime: 1_800_000_000,
                credit_policy: None,
            },
            None,
        )
        .await?;

    let sql: Vec<String> = sqlx::query_scalar(
        "SELECT payout_order_key::text FROM qbit_current_carry_forward_balances()",
    )
    .fetch_all(pool)
    .await?;
    ensure!(
        sql == ["a-order", "B-order"],
        "under {collation} the SQL order is {sql:?}, not [a-order, B-order]; the precondition does not hold"
    );
    let snapshot = ledger.snapshot(100).await?;
    ensure!(
        order_keys(&canonical(snapshot.prior_balances.clone())) == ["B-order", "a-order"],
        "the byte-wise canonical sort agrees with the SQL order, so this case proves nothing"
    );
    ensure!(
        order_keys(&snapshot.prior_balances) == sql,
        "Ledger::snapshot returned the balances as {:?}, not in their SQL order {sql:?}",
        order_keys(&snapshot.prior_balances)
    );
    let window = WindowRef::from_snapshot(&snapshot)?;
    let read = ledger
        .read_window(&window, BalanceSource::Current)
        .await
        .map_err(|error| anyhow::anyhow!("the current-balance window read failed: {error}"))?;
    ensure!(
        read.prior_balances == snapshot.prior_balances,
        "read_window(Current) returned the balances as {:?}, not in their SQL order {sql:?}",
        order_keys(&read.prior_balances)
    );
    Ok(())
}

#[tokio::test]
async fn current_balance_reads_keep_the_sql_order_under_a_non_bytewise_collation() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_balance_order_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let outcome = async {
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger = Ledger::connect(url.as_str(), "balance-order".into(), 4, true)
            .await
            .context("the ledger did not connect")?;
        let result = body(&ledger.pool.clone(), &ledger).await;
        ledger.pool.close().await;
        result
    }
    .await;
    let dropped = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    match (outcome, dropped) {
        (Ok(()), dropped) => dropped.map(|_| ()).map_err(Into::into),
        (Err(error), _) => Err(error),
    }
}

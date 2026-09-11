//! Real database evidence for the B-owned admission observation, not a runtime
//! lease or A265 candidate test. Existing as-issued reconstruction stays intact.
use super::*;

fn balance(sats: i128) -> CarryForwardBalance {
    CarryForwardBalance {
        recipient_id: "prior".into(),
        order_key: "prior".into(),
        p2mr_program_hex: "22".repeat(32),
        balance_sats: sats,
    }
}

async fn seed(pool: &PgPool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('prior','prior',decode(repeat('22',32),'hex'),12345,1)")
        .execute(pool).await?;
    Ok(())
}

async fn wait_for_balances(db: &Database) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT miner_id,payout_order_key,%' AND pid IN (SELECT pid FROM pg_locks WHERE relation=$1::regclass AND NOT granted))")
                .bind(format!("{}.qbit_payout_carry_forward_current", db.schema))
                .fetch_one(&db.admin).await?;
            if blocked {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("payout reader never reached the balance read")?
}

#[tokio::test]
async fn revision_only_change_and_balance_change_are_distinct_without_reading_a_window(
) -> Result<()> {
    run(|db| Box::pin(async move {
        let ledger = db.ledger().await?;
        // Admission must not require share history, even for a nonempty job,
        // and must not confuse today's digest with the as-issued blob's digest.
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger RENAME TO hidden_history; ALTER TABLE qbit_prism_balance_snapshots RENAME TO hidden_snapshots")
            .execute(&db.pool).await?;
        seed(&db.pool).await?;
        let issued = ledger.payout_state().await?;
        ensure!(issued.prior_balances_digest == qbit_prism::prior_balances_digest(&[balance(12345)]),
            "current balance digest differs from the reference format");
        sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1")
            .execute(&db.pool).await?;
        let same_balances = ledger.payout_state().await?;
        ensure!(same_balances.payout_revision == issued.payout_revision + 1
            && same_balances.prior_balances_digest == issued.prior_balances_digest,
            "a revision change alone invalidated the balance identity");
        let mut change = db.pool.begin().await?;
        sqlx::raw_sql("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1; UPDATE qbit_payout_carry_forward_current SET balance_sats=67890")
            .execute(&mut *change).await?;
        change.commit().await?;
        let changed = ledger.payout_state().await?;
        ensure!(changed.payout_revision == same_balances.payout_revision + 1
            && changed.prior_balances_digest == qbit_prism::prior_balances_digest(&[balance(67890)])
            && changed.prior_balances_digest != issued.prior_balances_digest,
            "changed balances retained the old eligibility digest");
        Ok(())
    })).await
}

#[tokio::test]
async fn revision_and_balance_digest_cannot_mix_across_a_blocked_read() -> Result<()> {
    run(|db| Box::pin(async move {
        let ledger = db.ledger().await?;
        seed(&db.pool).await?;
        let original = ledger.payout_state().await?;
        let gate_key: i64 = sqlx::query_scalar("SELECT oid::bigint FROM pg_namespace WHERE nspname=$1")
            .bind(&db.schema).fetch_one(&db.pool).await?;
        // Delay the *first* SELECT after it captures the revision. Blocking
        // the balance SELECT instead could preserve that statement's snapshot
        // under READ COMMITTED too, and would not prove transaction coherence.
        sqlx::raw_sql(&format!(
            "ALTER TABLE qbit_prism_cluster RENAME TO payout_state_source;
             CREATE FUNCTION delayed_payout_revision(revision bigint) RETURNS bigint
             LANGUAGE plpgsql VOLATILE AS 'BEGIN PERFORM pg_advisory_xact_lock({gate_key}); RETURN revision; END;';
             CREATE VIEW qbit_prism_cluster AS SELECT singleton,fatal_error,
                 delayed_payout_revision(payout_revision) AS payout_revision FROM payout_state_source"
        )).execute(&db.pool).await?;
        let mut change = db.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(gate_key).execute(&mut *change).await?;
        let source = ledger.clone();
        let reader = tokio::spawn(async move { source.payout_state().await });
        timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=0 AND objid::bigint=$1)")
                    .bind(gate_key).fetch_one(&db.admin).await?;
                if blocked { return Ok::<_, anyhow::Error>(()); }
                sleep(Duration::from_millis(10)).await;
            }
        }).await.context("revision query did not reach the advisory gate")??;
        // The revision argument has been evaluated from the old row; the
        // balance statement has not started. READ COMMITTED would mix them.
        sqlx::raw_sql("UPDATE payout_state_source SET payout_revision=payout_revision+1; UPDATE qbit_payout_carry_forward_current SET balance_sats=67890")
            .execute(&mut *change).await?;
        change.commit().await?;
        let during = timeout(Duration::from_secs(5), reader).await???;
        ensure!(during == original, "payout eligibility mixed two MVCC snapshots");
        let after = ledger.payout_state().await?;
        ensure!(after.payout_revision == original.payout_revision + 1
            && after.prior_balances_digest == qbit_prism::prior_balances_digest(&[balance(67890)]),
            "a new read did not see the committed pair");
        Ok(())
    })).await
}

#[tokio::test]
async fn unavailable_read_only_and_corrupt_balances_remain_errors() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let mut ledger = db.ledger().await?;
            // One connection makes the session setting and its later cleanup exact.
            ledger.pool = db.pool.clone();
            let empty = ledger.payout_state().await?;
            ensure!(
                empty.prior_balances_digest == qbit_prism::prior_balances_digest(&[]),
                "valid zero state is not the empty balance digest"
            );
            sqlx::query("SET default_transaction_read_only=on")
                .execute(&db.pool)
                .await?;
            let read_only = ledger.payout_state().await;
            sqlx::query("SET default_transaction_read_only=off")
                .execute(&db.pool)
                .await?;
            ensure!(
                matches!(
                    read_only,
                    Err(WindowError::Database(sqlx::Error::RowNotFound))
                ),
                "read-only configuration was overridden or treated as a valid state"
            );
            sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='test failure'")
                .execute(&db.pool)
                .await?;
            ensure!(
                matches!(
                    ledger.payout_state().await,
                    Err(WindowError::Database(sqlx::Error::RowNotFound))
                ),
                "fatal state was treated as a valid admission observation"
            );
            sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL")
                .execute(&db.pool)
                .await?;
            seed(&db.pool).await?;
            sqlx::query(
                "UPDATE qbit_payout_carry_forward_current SET balance_sats=power(10::numeric,50)",
            )
            .execute(&db.pool)
            .await?;
            ensure!(
                matches!(ledger.payout_state().await, Err(WindowError::Decode(_))),
                "balance overflow was hidden as ineligible work"
            );
            sqlx::query("ALTER TABLE qbit_payout_carry_forward_current RENAME TO hidden_current")
                .execute(&db.pool)
                .await?;
            ensure!(
                matches!(ledger.payout_state().await, Err(WindowError::Database(_))),
                "balance read failure was hidden as ineligible work"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn timeout_and_cancelled_balance_read_release_the_connection() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let mut ledger = db.ledger().await?;
            ledger.pool = db.pool.clone();
            seed(&db.pool).await?;
            for cancel in [false, true] {
                sqlx::query(if cancel {
                    "SET statement_timeout='5s'"
                } else {
                    "SET statement_timeout='80ms'"
                })
                .execute(&db.pool)
                .await?;
                let mut lock = db.admin.begin().await?;
                sqlx::query(&format!(
                    "LOCK TABLE {}.qbit_payout_carry_forward_current IN ACCESS EXCLUSIVE MODE",
                    db.schema
                ))
                .execute(&mut *lock)
                .await?;
                if cancel {
                    let source = ledger.clone();
                    let reader = tokio::spawn(async move { source.payout_state().await });
                    wait_for_balances(db).await?;
                    reader.abort();
                    ensure!(
                        reader.await.unwrap_err().is_cancelled(),
                        "read was not cancelled"
                    );
                } else {
                    let error = ledger.payout_state().await.unwrap_err();
                    ensure!(
                        matches!(error, WindowError::Database(sqlx::Error::Database(ref error))
                    if error.code().as_deref() == Some("57014")),
                        "statement timeout lost its SQLSTATE: {error}"
                    );
                }
                lock.rollback().await?;
                let recovered = timeout(Duration::from_secs(2), ledger.payout_state()).await??;
                ensure!(
                    recovered.prior_balances_digest
                        == qbit_prism::prior_balances_digest(&[balance(12345)]),
                    "payout reader did not recover its connection"
                );
            }
            Ok(())
        })
    })
    .await
}

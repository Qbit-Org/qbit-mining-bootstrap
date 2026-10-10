//! #664: the cluster-wide block submission hold (migration 023). While it is
//! set no frontend claims a block candidate, reserves a block's offer, or
//! claims or sends a CTV fanout, whatever its own `PRISM_BLOCK_SUBMIT_ENABLED`
//! says, so a restored rehearsal ledger can be held for every frontend that
//! connects to it. `qbit-prism-server submission-hold` sets, clears and shows
//! it, and journals every set and clear.
use super::connect::SERVING;
use super::fatal_state::require_operator_reason;
use super::*;
use std::time::Duration;

/// Why, when and by whom the cluster's block submission was held.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionHold {
    pub reason: String,
    /// The database clock when the hold was set, RFC 3339.
    pub set_at: String,
    /// The database session user that set it.
    pub set_by: String,
}

/// A claim, an offer reservation or a fanout send refused because the cluster
/// holds block submission (#664).
#[derive(Clone, Debug, thiserror::Error)]
#[error("the cluster holds block submission, set by {} at {}: {}; no frontend claims a candidate, offers a block or sends a fanout until `qbit-prism-server submission-hold clear`", .0.set_by, .0.set_at, .0.reason)]
pub struct SubmissionHeld(pub SubmissionHold);

/// The hold's columns as every reader selects them, from the hold row
/// aliased `h`.
pub(super) const HOLD_COLUMNS: &str =
    "h.reason AS hold_reason,to_jsonb(h.set_at)#>>'{}' AS hold_set_at,h.set_by AS hold_set_by";

impl SubmissionHold {
    /// The hold a row selected with [`HOLD_COLUMNS`] carries, if any.
    pub(super) fn from_row(row: &PgRow) -> Result<Option<Self>> {
        let Some(reason) = row.try_get::<Option<String>, _>("hold_reason")? else {
            return Ok(None);
        };
        Ok(Some(Self {
            reason,
            set_at: row
                .try_get::<Option<String>, _>("hold_set_at")?
                .context("a held cluster records when its hold was set")?,
            set_by: row
                .try_get::<Option<String>, _>("hold_set_by")?
                .context("a held cluster records who set its hold")?,
        }))
    }
}

/// What `submission-hold clear` found and did.
#[derive(Clone, Debug, Serialize)]
pub struct SubmissionHoldCleared {
    /// The hold it cleared, or `None` when nothing held submission.
    pub cleared: Option<SubmissionHold>,
    /// Candidates still `pending`, which frontends with submission enabled
    /// offer to their nodes once nothing holds them.
    pub pending_candidates: i64,
}

/// Take the hold row's exclusive lock, which waits for every offer
/// reservation reading it `FOR SHARE` and makes a later one wait for this
/// transaction, and read the hold it carries.
async fn lock_submission_hold(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<SubmissionHold>> {
    let row = sqlx::query(&format!(
        "SELECT {HOLD_COLUMNS} FROM qbit_prism_submission_hold h WHERE h.singleton FOR UPDATE"
    ))
    .fetch_one(&mut **tx)
    .await?;
    SubmissionHold::from_row(&row)
}

/// Candidates a frontend would claim and offer once nothing holds them. Read
/// after the hold row's lock, so it counts every row committed before it.
async fn pending_candidates(tx: &mut Transaction<'_, Postgres>) -> Result<i64> {
    Ok(
        sqlx::query_scalar(
            "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state='pending'",
        )
        .fetch_one(&mut **tx)
        .await?,
    )
}

async fn record_event(
    tx: &mut Transaction<'_, Postgres>,
    action: &str,
    reason: &str,
    pending_candidates: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_prism_submission_hold_events(action,reason,pending_candidates) VALUES($1,$2,$3)",
    )
    .bind(action)
    .bind(reason)
    .bind(pending_candidates)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Ledger {
    /// The cluster's block submission hold, if one is set.
    pub async fn submission_hold(&self) -> Result<Option<SubmissionHold>> {
        let row = sqlx::query(&format!(
            "SELECT {HOLD_COLUMNS} FROM qbit_prism_submission_hold h WHERE h.singleton"
        ))
        .fetch_one(&mut *self.acquire().await?)
        .await?;
        SubmissionHold::from_row(&row)
    }

    /// Refuse with [`SubmissionHeld`] while the cluster holds submission: the
    /// CTV broadcaster's check before a pass and before each send.
    pub async fn require_no_submission_hold(&self) -> Result<()> {
        match self.submission_hold().await? {
            Some(hold) => Err(SubmissionHeld(hold).into()),
            None => Ok(()),
        }
    }

    /// What a health publication reads, in one statement: the payout
    /// revision, `None` whenever [`Ledger::payout_revision`] refuses it, and
    /// the cluster's block submission hold.
    pub async fn health_reads(&self) -> Result<(Option<i64>, Option<SubmissionHold>)> {
        health_reads_with(&mut *self.acquire().await?).await
    }

    /// Hold block submission cluster-wide, or keep a hold already set as it
    /// is. Returns the hold in force and whether this call set it; only a
    /// new hold is journaled. Allowed on a halted cluster: a hold only ever
    /// stops submission.
    pub async fn set_submission_hold(&self, reason: &str) -> Result<(SubmissionHold, bool)> {
        require_operator_reason(reason)?;
        let mut tx = self.begin().await?;
        if let Some(existing) = lock_submission_hold(&mut tx).await? {
            tx.rollback().await?;
            return Ok((existing, false));
        }
        let pending = pending_candidates(&mut tx).await?;
        let row = sqlx::query(&format!(
            "UPDATE qbit_prism_submission_hold h SET reason=$1,set_at=clock_timestamp(),set_by=session_user \
             WHERE h.singleton RETURNING {HOLD_COLUMNS}"
        ))
        .bind(reason)
        .fetch_one(&mut *tx)
        .await?;
        let hold = SubmissionHold::from_row(&row)?.context("the hold was not recorded")?;
        record_event(&mut tx, "set", reason, pending).await?;
        tx.commit().await?;
        Ok((hold, true))
    }

    /// Clear the cluster's block submission hold, journaling `reason`. While
    /// any candidate is still `pending` this is refused unless
    /// `offer_pending` says to let frontends with submission enabled offer
    /// them: on a rehearsal ledger those are the blocks the hold kept from
    /// the network.
    pub async fn clear_submission_hold(
        &self,
        reason: &str,
        offer_pending: bool,
    ) -> Result<SubmissionHoldCleared> {
        require_operator_reason(reason)?;
        let mut tx = self.begin().await?;
        let held = lock_submission_hold(&mut tx).await?;
        let pending_candidates = pending_candidates(&mut tx).await?;
        let Some(hold) = held else {
            tx.rollback().await?;
            return Ok(SubmissionHoldCleared {
                cleared: None,
                pending_candidates,
            });
        };
        ensure!(
            offer_pending || pending_candidates == 0,
            "refusing to clear the block submission hold while candidates are pending ({pending_candidates}): every frontend with submission enabled would offer them to its node. On a rehearsal ledger, discard the database or abandon each one with `qbit-prism-server candidates abandon`; to offer them, pass --offer-pending-candidates. Nothing was changed"
        );
        sqlx::query(
            "UPDATE qbit_prism_submission_hold SET reason=NULL,set_at=NULL,set_by=NULL WHERE singleton",
        )
        .execute(&mut *tx)
        .await?;
        record_event(&mut tx, "clear", reason, pending_candidates).await?;
        tx.commit().await?;
        Ok(SubmissionHoldCleared {
            cleared: Some(hold),
            pending_candidates,
        })
    }

    /// `submission-hold show`: a read-only report that needs only the
    /// database URL and also reads a ledger from before migration 023, which
    /// cannot hold submission. Bounded as a whole, connection included, so a
    /// database that stops answering fails the command rather than hangs it.
    pub async fn inspect_submission_hold(url: &str) -> Result<Value> {
        use sqlx::Connection;
        tokio::time::timeout(Duration::from_secs(15), async {
            // One connection attempt, not a pool that retries: self-check
            // samples this beside the heartbeats and must fail fast when
            // they do.
            let mut connection = sqlx::PgConnection::connect(url).await?;
            let state = read_submission_hold_report(&mut connection).await;
            let _ = connection.close().await;
            let mut state = state?;
            state["schema"] = json!("qbit.prism.submission-hold.v1");
            Ok(state)
        })
        .await
        .context("reading the block submission hold took more than 15 seconds")?
    }
}

async fn read_submission_hold_report(connection: &mut sqlx::PgConnection) -> Result<Value> {
    sqlx::query("SELECT set_config('default_transaction_read_only','on',false),set_config('statement_timeout','15s',false),set_config('lock_timeout','5s',false)")
        .execute(&mut *connection)
        .await?;
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state='pending'",
    )
    .fetch_one(&mut *connection)
    .await?;
    let supported: bool =
        sqlx::query_scalar("SELECT to_regclass('qbit_prism_submission_hold') IS NOT NULL")
            .fetch_one(&mut *connection)
            .await?;
    let mut state = if supported {
        sqlx::query_scalar::<_, Value>(
            "SELECT jsonb_build_object('held', h.reason IS NOT NULL, 'reason', h.reason, \
             'set_at', h.set_at, 'set_by', h.set_by, 'last_event', \
             (SELECT to_jsonb(e) FROM qbit_prism_submission_hold_events e ORDER BY event_id DESC LIMIT 1)) \
             FROM qbit_prism_submission_hold h WHERE h.singleton",
        )
        .fetch_one(&mut *connection)
        .await?
    } else {
        json!({"held": false, "reason": null, "set_at": null, "set_by": null, "last_event": null})
    };
    state["schema_supports_hold"] = json!(supported);
    state["pending_candidates"] = json!(pending);
    Ok(state)
}

/// [`Ledger::health_reads`]'s statement on `connection`: a dual-writer
/// frontend runs it on its own health pool (3.1).
pub(crate) async fn health_reads_with(
    connection: &mut sqlx::PgConnection,
) -> Result<(Option<i64>, Option<SubmissionHold>)> {
    let row = sqlx::query(&format!(
        "SELECT CASE WHEN {SERVING} THEN c.payout_revision END AS payout_revision,{HOLD_COLUMNS} \
         FROM qbit_prism_cluster c CROSS JOIN qbit_prism_submission_hold h WHERE c.singleton AND h.singleton"
    ))
    .fetch_one(&mut *connection)
    .await?;
    Ok((
        row.try_get("payout_revision")?,
        SubmissionHold::from_row(&row)?,
    ))
}

use super::claim_observer::{decode_survey, ClaimVersion};
use super::*;

// Allocation and retirement span two tables; this short lock makes their
// combined outpoint exclusion atomic across independently claimed fanouts.
pub(super) const CPFP_FUNDING_LOCK: i64 = 0x505249534d000006;

// How a handed-back claim is rescheduled (#573): due at once, but ordered
// behind every row already due.
const REQUEUE_BEHIND_DUE: &str = "next_broadcast_attempt_at=clock_timestamp()";

/// The assignments that end a fanout's claim. Every statement that clears
/// `claim_token` clears the lease 022 records with it (#654), so the
/// version and lease a survey reads always describe the current claim.
pub(super) const CLEAR_FANOUT_CLAIM_SQL: &str = "claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,claim_lease_seconds=NULL,claim_renewals=0";

/// The fanouts a claim may take, `a` joined with its parent block `b`: a
/// mature, confirmed parent, and a settlement still to broadcast or a
/// confirmation still to watch, which is one under 1,000 deep or the newest
/// deep one (the checkpoint reconciliation keeps watching). Its schedule and
/// its claim are decided apart. The status list leads, so the broadcast
/// index serves it.
pub(super) const FANOUT_CLAIMABLE_SQL: &str = "b.chain_state='confirmed' AND b.maturity_state='mature' AND a.settlement_status IN ('broadcastable','broadcast_submitted','failed','confirmed') AND (a.settlement_status<>'confirmed' OR a.confirmed_depth<1000 OR a.fanout_txid=(SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed' AND confirmed_depth>=1000 ORDER BY confirmed_block_height DESC,fanout_txid DESC LIMIT 1))";

/// Whether an unclaimed fanout's schedule was written before the database
/// clock stepped back (#654), as `STEPPED_SQL` decides it for a candidate
/// retry (#581). Every statement that schedules `next_broadcast_attempt_at`
/// writes `updated_at` from the same clock, so a row whose last write is
/// later than the clock now proves the step, and its attempt is due at once
/// instead of waiting out the step. A backward step therefore delays an
/// attempt or a confirmation check by less than its own delay, never by the
/// step; a forward step only makes it due early. A row held at `infinity` is
/// never made due.
const FANOUT_STEPPED_SQL: &str =
    "(a.next_broadcast_attempt_at<>'infinity'::timestamptz AND a.updated_at>clock_timestamp())";

/// Whether a claimed fanout may be taken over once its lease is over
/// (#654): whatever its schedule, unless it is held at `infinity`. The lane
/// claims only due rows and nothing rewrites a claimed row's schedule, so
/// its `next_broadcast_attempt_at` is the one it was due at when claimed, on
/// the clock of that moment: after a backward step it no longer reads due,
/// though the claim's holder died.
const FANOUT_TAKEOVER_SQL: &str =
    "(a.next_broadcast_attempt_at IS NULL OR a.next_broadcast_attempt_at<>'infinity'::timestamptz)";

/// The predicate of `qbit_ctv_fanout_artifacts_lane_idx` (migration 024,
/// #668), which a statement must repeat exactly for the planner to read the
/// index: every fanout still to broadcast or check, and every confirmed one
/// under 1,000 deep. Apart from the newest deep fanout, the checkpoint
/// reconciliation keeps watching, these are all the fanouts a claim may
/// take; the settled history is the rest.
const FANOUT_LANE_INDEXED_SQL: &str = "(settlement_status IN ('broadcastable','broadcast_submitted','failed') OR (settlement_status='confirmed' AND confirmed_depth<1000))";

/// The newest deep fanout, the checkpoint reconciliation keeps watching,
/// through `qbit_prism_fanout_checkpoint_idx`: the one fanout a claim may
/// take outside the lane's index. `FANOUT_CLAIMABLE_SQL` admits the same
/// one, which a unit test checks.
const FANOUT_CHECKPOINT_SQL: &str = "SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed' AND confirmed_depth>=1000 ORDER BY confirmed_block_height DESC,fanout_txid DESC LIMIT 1";

/// What a claim returns, the lane's and a takeover's alike.
pub(super) const FANOUT_CLAIMED_COLUMNS: &str = "a.fanout_txid,a.block_hash,a.manifest,a.broadcast_attempt_count,jsonb_build_object('status',a.settlement_status,'confirmed_block_hash',a.confirmed_block_hash,'confirmed_block_height',a.confirmed_block_height,'confirmed_depth',a.confirmed_depth,'scan_next_height',a.spend_scan_next_height,'scan_anchor_height',a.spend_scan_anchor_height,'scan_anchor_hash',a.spend_scan_anchor_hash) AS progress";

/// The retry schedule a recorded attempt sets, from the attempt count before
/// it (every `SET` expression reads the old row).
pub(super) const ATTEMPT_BACKOFF: &str = "next_broadcast_attempt_at=clock_timestamp()+LEAST(3600,10*(broadcast_attempt_count+1))*interval '1 second',broadcast_retry_backoff_seconds=LEAST(3600,10*(broadcast_attempt_count+1))";

/// The counter and `last_*` columns every recorded attempt updates, reading
/// its attempt status, result and error from the given parameters. Shared by
/// `finish_fanout` and the hand-backs so the attempt accounting cannot drift.
pub(super) fn attempt_columns(status: &str, result: &str, error: &str) -> String {
    format!("broadcast_attempt_count=broadcast_attempt_count+1,broadcast_attempt_detail_count=LEAST(32,broadcast_attempt_detail_count+1),first_broadcast_attempt_at=COALESCE(first_broadcast_attempt_at,clock_timestamp()),last_broadcast_attempt_at=clock_timestamp(),last_broadcast_attempt_status={status},last_broadcast_submit_result={result},last_broadcast_error={error},broadcast_attempt_status_counts=jsonb_set(broadcast_attempt_status_counts,ARRAY[{status}],to_jsonb(COALESCE((broadcast_attempt_status_counts->>{status})::bigint,0)+1))")
}

/// Append an attempt to the fanout's history, keeping its newest 32.
pub(super) async fn record_attempt_history(
    tx: &mut Transaction<'_, Postgres>,
    fanout_txid: &str,
    attempt_status: &str,
    result: Option<&Value>,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query("INSERT INTO qbit_ctv_fanout_broadcast_attempts(fanout_txid,attempt_status,submit_result,error) VALUES($1,$2,$3,$4)")
        .bind(fanout_txid).bind(attempt_status).bind(result).bind(error).execute(&mut **tx).await?;
    sqlx::query("DELETE FROM qbit_ctv_fanout_broadcast_attempts WHERE fanout_txid=$1 AND attempt_seq NOT IN (SELECT attempt_seq FROM qbit_ctv_fanout_broadcast_attempts WHERE fanout_txid=$1 ORDER BY attempt_seq DESC LIMIT 32)").bind(fanout_txid).execute(&mut **tx).await?;
    Ok(())
}

/// A check-only observation (a confirmation, a mempool hit, a scan page)
/// records no attempt; every other attempt result is a send.
pub(super) fn is_check_only(result: &Value) -> bool {
    result["check_only"] == true
}

/// An attempt a hand-back records because its own completion did not persist.
struct LostAttempt<'a> {
    status: &'static str,
    result: Option<&'a Value>,
    error: &'a str,
}

/// Dual writer: how long a zero-fee fanout found on the peer's work waits for
/// the finder's node before this node may sponsor it: since its block matured
/// here, and since the newest work of the finder's this node holds (a synced
/// prepared record). The broadcaster also reads the finder's own database
/// live (`broadcaster::finder_silent`): a finder that is alive publishes work
/// every few minutes, so its fanouts are not taken over while it can be read.
pub const SPONSOR_TAKEOVER_AFTER: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Why this node may fund a zero-fee fanout's CPFP child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanoutSponsor {
    /// The fanout's block was found on this node's work, or this is a single
    /// writer.
    Finder,
    /// Dual writer: found on the peer's work, and this node holds a CPFP
    /// package for it, from a takeover it started and finishes.
    Held,
    /// Dual writer: found on the peer's work; its block matured here, and
    /// the finder's newest work this node holds was published, at least
    /// [`SPONSOR_TAKEOVER_AFTER`] ago. The broadcaster takes it over if the
    /// finder's own database, when it can be read, agrees.
    Overdue,
}

/// The newest work of node `$1` this database holds: a prepared record, by
/// the time it records. One definition for the ledger's overdue fence and
/// for the age the broadcaster reads live from the peer's database.
macro_rules! newest_work_sql {
    () => {
        "SELECT created_at FROM qbit_prism_jobs WHERE origin_node=$1 AND sync_seq IS NOT NULL AND job_id LIKE 'prepared:%' ORDER BY sync_seq DESC LIMIT 1"
    };
}

/// A fanout's finder (by its origin and its block's coinbase), whether this
/// node holds a CPFP package for it, and whether it is overdue: its block
/// matured here, and the newest work of node `$1` (the peer) this node holds
/// was published, at least `$2` seconds ago. The fanout is `$3`.
const FANOUT_SPONSOR_SQL: &str = concat!(
    "SELECT artifact.origin_node,fanout_set.parent_coinbase_tx_hex,",
    "EXISTS(SELECT 1 FROM qbit_prism_cpfp_packages package WHERE package.fanout_txid=artifact.fanout_txid) AS held,",
    "COALESCE(block.matured_at<=clock_timestamp()-make_interval(secs=>$2),false) ",
    "AND NOT EXISTS(SELECT 1 FROM (",
    newest_work_sql!(),
    ") newest WHERE newest.created_at>clock_timestamp()-make_interval(secs=>$2)) AS overdue ",
    "FROM qbit_ctv_fanout_artifacts artifact JOIN qbit_ctv_fanout_sets fanout_set ON fanout_set.block_hash=artifact.block_hash ",
    "LEFT JOIN qbit_pool_blocks block ON block.block_hash=artifact.block_hash WHERE artifact.fanout_txid=$3"
);

/// How long ago node `$1` published its newest work held in this database
/// (`newest_work_sql!`), by this database's clock, in seconds; no row when it
/// holds none. Run on the peer's own database, it ages the peer's work by the
/// peer's own clock.
pub const WORK_AGE_SQL: &str = concat!(
    "SELECT EXTRACT(EPOCH FROM clock_timestamp()-created_at)::float8 FROM (",
    newest_work_sql!(),
    ") newest"
);

impl Ledger {
    /// Whether, and why, this node may sponsor (CPFP) the zero-fee fanout
    /// `fanout_txid` (`None`: the finder's node does). A single writer always
    /// does. In dual-writer mode the node whose work the block was found on
    /// does ([`Ledger::found_here`]), whichever node landed it, so the two
    /// nodes do not fund conflicting children; the other node only once the
    /// fanout is overdue, or to finish a package it holds.
    pub async fn fanout_sponsor(&self, fanout_txid: &str) -> Result<Option<FanoutSponsor>> {
        let Some(own) = self.own_node() else {
            return Ok(Some(FanoutSponsor::Finder));
        };
        let (origin, coinbase, held, overdue): (i16, String, bool, bool) =
            sqlx::query_as(FANOUT_SPONSOR_SQL)
                .bind(1 - own)
                .bind(SPONSOR_TAKEOVER_AFTER.as_secs_f64())
                .bind(fanout_txid)
                .fetch_one(&mut *self.acquire().await?)
                .await?;
        let coinbase = hex::decode(coinbase).ok();
        Ok(if self.found_here(coinbase.as_deref(), origin == own) {
            Some(FanoutSponsor::Finder)
        } else if held {
            Some(FanoutSponsor::Held)
        } else if overdue {
            Some(FanoutSponsor::Overdue)
        } else {
            None
        })
    }

    /// How long ago node `origin` published its newest work this database
    /// holds ([`WORK_AGE_SQL`]); `None` when it holds none.
    pub async fn work_age(&self, origin: i16) -> Result<Option<std::time::Duration>> {
        let seconds: Option<f64> = sqlx::query_scalar(WORK_AGE_SQL)
            .bind(origin)
            .fetch_optional(&mut *self.acquire().await?)
            .await?;
        Ok(seconds.map(|seconds| std::time::Duration::from_secs_f64(seconds.max(0.))))
    }

    /// Renew only a token that still holds the fanout. A worker another
    /// instance took over must not revive itself before wallet or network
    /// mutations. Fenced on the token alone (#654): whether the lease is
    /// still live is the holder's own monotonic deadline (the broadcaster's
    /// attempt deadline), never the database clock, and a takeover replaces
    /// the token. The renewal count makes this a new version, which every
    /// observer times afresh.
    pub async fn renew_fanout_claim(&self, claim: &FanoutClaim, seconds: i64) -> Result<()> {
        ensure!(
            (1..=600).contains(&seconds),
            "invalid fanout lease duration"
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_expires_at=clock_timestamp()+$3*interval '1 second',claim_lease_seconds=$3::integer,claim_renewals=claim_renewals+1 WHERE fanout_txid=$1 AND claim_token=$2")
            .bind(&claim.fanout_txid).bind(&claim.claim_token).bind(seconds).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Hand back a claim whose completion failed to persist, as its expiry
    /// would, without recording an attempt (#569). Only the holder's token
    /// matches: a completion that did commit, or a claim another instance
    /// already took over, is left alone. Returns whether a claim was released.
    ///
    /// The row is due again as its expiry would leave it. A claim takes
    /// only a due row and nothing rewrites a claimed row's schedule, so the
    /// schedule is kept unless a backward database clock step left it ahead
    /// of the clock (#654): this write's `updated_at` would then hide the step
    /// from every claim poll, and the row would wait out the step.
    pub async fn release_fanout_claim(&self, claim: &FanoutClaim) -> Result<bool> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let released = sqlx::query(&format!("UPDATE qbit_ctv_fanout_artifacts SET {CLEAR_FANOUT_CLAIM_SQL},next_broadcast_attempt_at=CASE WHEN next_broadcast_attempt_at IS NULL OR next_broadcast_attempt_at='infinity'::timestamptz THEN next_broadcast_attempt_at ELSE LEAST(next_broadcast_attempt_at,clock_timestamp()) END,updated_at=clock_timestamp() WHERE fanout_txid=$1 AND claim_token=$2"))
            .bind(&claim.fanout_txid).bind(&claim.claim_token).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(released == 1)
    }

    /// Hand back a claim whose successful attempt's completion was refused
    /// (such as by a moved payout revision) or otherwise did not persist
    /// (#573). Like [`Self::release_fanout_claim`], but the row is due again
    /// at once *behind* every row already due, so a row refused on every
    /// attempt cannot hold up the rows after it. An attempt that sent the
    /// fanout (a `result` that is not a check-only observation) is recorded
    /// as `submitted` with `error`, so the attempt history and counters see
    /// it; the settlement status and chain evidence the completion would have
    /// written are left alone, and the retry backoff is not extended. Fenced
    /// by the holder's token; returns whether a claim was released.
    pub async fn requeue_refused_fanout(
        &self,
        claim: &FanoutClaim,
        result: &Value,
        error: &str,
    ) -> Result<bool> {
        let sent = (!is_check_only(result)).then_some(LostAttempt {
            status: "submitted",
            result: Some(result),
            error,
        });
        self.hand_back_fanout(claim, sent, REQUEUE_BEHIND_DUE).await
    }

    /// The fallback when a failed attempt's `finish_fanout(…, "failed")` did
    /// not persist (#573): record the failed attempt and hand the claim back
    /// with the backoff that completion would have set, rather than hold it
    /// until expiry and then retry at once. It needs neither the settlement
    /// lock nor an active parent, and leaves the settlement status alone.
    /// Fenced by the holder's token; returns whether a claim was released.
    pub async fn release_failed_fanout(&self, claim: &FanoutClaim, error: &str) -> Result<bool> {
        let failed = LostAttempt {
            status: "failed",
            result: None,
            error,
        };
        self.hand_back_fanout(claim, Some(failed), ATTEMPT_BACKOFF)
            .await
    }

    async fn hand_back_fanout(
        &self,
        claim: &FanoutClaim,
        attempt: Option<LostAttempt<'_>>,
        schedule: &'static str,
    ) -> Result<bool> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let released = if let Some(attempt) = &attempt {
            sqlx::query(&format!("UPDATE qbit_ctv_fanout_artifacts SET {},{schedule},{CLEAR_FANOUT_CLAIM_SQL},updated_at=clock_timestamp() WHERE fanout_txid=$1 AND claim_token=$2", attempt_columns("$3", "$4", "$5")))
                .bind(&claim.fanout_txid).bind(&claim.claim_token).bind(attempt.status).bind(attempt.result).bind(attempt.error).execute(&mut *tx).await?.rows_affected()
        } else {
            sqlx::query(&format!("UPDATE qbit_ctv_fanout_artifacts SET {schedule},{CLEAR_FANOUT_CLAIM_SQL},updated_at=clock_timestamp() WHERE fanout_txid=$1 AND claim_token=$2"))
                .bind(&claim.fanout_txid).bind(&claim.claim_token).execute(&mut *tx).await?.rows_affected()
        };
        // A stale token records nothing: a completion that did commit already
        // recorded this attempt, and another holder owns the row's history.
        if let (1, Some(attempt)) = (released, &attempt) {
            record_attempt_history(
                &mut tx,
                &claim.fanout_txid,
                attempt.status,
                attempt.result,
                Some(attempt.error),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(released == 1)
    }

    pub async fn record_fanout_scan(
        &self,
        claim: &FanoutClaim,
        next: u64,
        anchor: Option<(u64, String)>,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        let (height, hash) = anchor.map_or((None, None), |(h, hash)| (Some(h), Some(hash)));
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET spend_scan_next_height=$3,spend_scan_anchor_height=$4,spend_scan_anchor_hash=$5 WHERE fanout_txid=$1 AND claim_token=$2")
            .bind(&claim.fanout_txid).bind(&claim.claim_token).bind(i64::try_from(next)?).bind(height.map(i64::try_from).transpose()?).bind(hash).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn observe_fanout(
        &self,
        claim: &FanoutClaim,
        status: &str,
        result: Value,
    ) -> Result<()> {
        ensure!(
            ["confirmed", "broadcast_submitted", "broadcastable"].contains(&status),
            "invalid fanout observation"
        );
        let delay = result["next_check_seconds"]
            .as_i64()
            .unwrap_or(10)
            .clamp(1, 3600);
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        apply_progress(&mut tx, claim, status, Some(&result)).await?;
        sqlx::query(&format!("UPDATE qbit_ctv_fanout_artifacts SET settlement_status=$3,next_broadcast_attempt_at=clock_timestamp()+$4*interval '1 second',{CLEAR_FANOUT_CLAIM_SQL},updated_at=clock_timestamp() WHERE fanout_txid=$1 AND claim_token=$2"))
            .bind(&claim.fanout_txid).bind(&claim.claim_token).bind(status).bind(delay).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn halt_fanout_reorg(
        &self,
        claim: &FanoutClaim,
        expected_revision: i64,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        require_fanout(&mut tx, claim).await?;
        // This path always writes: lock the row first, so the revision
        // validated is the locked row's.
        super::connect::lock_cluster_authority(&mut tx).await?;
        require_revision(&mut tx, expected_revision).await?;
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=$1,updated_at=clock_timestamp() WHERE singleton")
            .bind(format!("deep confirmed CTV fanout disconnected: {}; manual reconciliation required; after investigation run qbit-prism-server fatal-state clear --reason <text>",claim.fanout_txid)).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn cpfp_package(&self, fanout_txid: &str) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(p) FROM qbit_prism_cpfp_packages p WHERE fanout_txid=$1",
        )
        .bind(fanout_txid)
        .fetch_optional(&mut *self.acquire().await?)
        .await?)
    }

    pub async fn reserve_cpfp_funding(
        &self,
        claim: &FanoutClaim,
        wallet: &str,
        txid: &str,
        vout: u32,
        value: u64,
    ) -> Result<bool> {
        // Dual writer: a fanout found on the peer's work is funded from this
        // wallet only once it is overdue, by the block's maturity and the
        // finder's newest work held here; this refuses anything earlier. The
        // broadcaster also reads the finder's own database first.
        ensure!(
            self.fanout_sponsor(&claim.fanout_txid).await?.is_some(),
            "fanout {} was found on the peer's work, whose node sponsors it until it is overdue for a takeover",
            claim.fanout_txid
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        self.lock(&mut tx, CPFP_FUNDING_LOCK).await?;
        let inserted=sqlx::query("INSERT INTO qbit_prism_cpfp_packages(fanout_txid,funding_txid,funding_vout,funding_value_sats,wallet_name) SELECT $1,$2,$3,$4,$5 WHERE NOT EXISTS(SELECT 1 FROM qbit_prism_cpfp_retired_funding WHERE funding_txid=$2 AND funding_vout=$3) ON CONFLICT DO NOTHING")
            .bind(&claim.fanout_txid).bind(txid).bind(i32::try_from(vout)?).bind(i64::try_from(value)?).bind(wallet).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(inserted == 1)
    }

    pub async fn retire_unsigned_cpfp_funding(
        &self,
        claim: &FanoutClaim,
        txid: &str,
        vout: u32,
        reason: &str,
    ) -> Result<()> {
        ensure!(
            !reason.is_empty() && reason.len() <= 1024,
            "invalid funding retirement reason"
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        self.lock(&mut tx, CPFP_FUNDING_LOCK).await?;
        // Never drop even a supposedly released lock's cleanup record without
        // checking the wallet: the previous owner could have crashed at an RPC.
        let moved = sqlx::query("WITH retired AS (DELETE FROM qbit_prism_cpfp_packages WHERE fanout_txid=$1 AND funding_txid=$2 AND funding_vout=$3 AND signed_child_hex IS NULL AND child_txid IS NULL RETURNING *) INSERT INTO qbit_prism_cpfp_retired_funding(fanout_txid,funding_txid,funding_vout,funding_value_sats,wallet_name,retirement_reason) SELECT fanout_txid,funding_txid,funding_vout,funding_value_sats,wallet_name,$4 FROM retired")
            .bind(&claim.fanout_txid).bind(txid).bind(i32::try_from(vout)?).bind(reason).execute(&mut *tx).await?.rows_affected();
        ensure!(
            moved == 1,
            "unsigned CPFP reservation changed or signed package is immutable"
        );
        tx.commit().await?;
        Ok(())
    }

    pub async fn retired_cpfp_funding(&self, fanout_txid: &str) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT to_jsonb(r) FROM qbit_prism_cpfp_retired_funding r WHERE fanout_txid=$1 AND NOT wallet_lock_released ORDER BY updated_at,funding_txid,funding_vout LIMIT 16")
            .bind(fanout_txid).fetch_all(&mut *self.acquire().await?).await?)
    }

    pub async fn record_retired_cpfp_wallet_cleanup(
        &self,
        claim: &FanoutClaim,
        txid: &str,
        vout: u32,
        unlocked: bool,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        let updated = sqlx::query("UPDATE qbit_prism_cpfp_retired_funding SET wallet_lock_released=$4,updated_at=clock_timestamp() WHERE fanout_txid=$1 AND funding_txid=$2 AND funding_vout=$3 AND NOT wallet_lock_released")
            .bind(&claim.fanout_txid).bind(txid).bind(i32::try_from(vout)?).bind(unlocked).execute(&mut *tx).await?.rows_affected();
        ensure!(updated == 1, "retired CPFP cleanup reservation changed");
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_retired_cpfp_wallet_unlocked(
        &self,
        claim: &FanoutClaim,
        txid: &str,
        vout: u32,
    ) -> Result<()> {
        self.record_retired_cpfp_wallet_cleanup(claim, txid, vout, true)
            .await
    }

    pub async fn save_cpfp_package(
        &self,
        claim: &FanoutClaim,
        signed_child_hex: &str,
        child_txid: &str,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        let updated=sqlx::query("UPDATE qbit_prism_cpfp_packages SET signed_child_hex=$2,child_txid=$3,updated_at=clock_timestamp() WHERE fanout_txid=$1 AND (signed_child_hex IS NULL OR (signed_child_hex=$2 AND child_txid=$3))")
            .bind(&claim.fanout_txid).bind(signed_child_hex).bind(child_txid).execute(&mut *tx).await?.rows_affected();
        ensure!(
            updated == 1,
            "CPFP package missing or immutable package conflict"
        );
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_cpfp_wallet_unlocked(&self, claim: &FanoutClaim) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        sqlx::query("UPDATE qbit_prism_cpfp_packages SET wallet_lock_released=true,updated_at=clock_timestamp() WHERE fanout_txid=$1").bind(&claim.fanout_txid).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_cpfp_wallet_lock_pending(&self, claim: &FanoutClaim) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        sqlx::query("UPDATE qbit_prism_cpfp_packages SET wallet_lock_released=false,updated_at=clock_timestamp() WHERE fanout_txid=$1").bind(&claim.fanout_txid).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// The holder's fence: lock the fanout and require its token and a mature,
/// active parent. Fenced on the token alone (#654), never on
/// `claim_expires_at`, which a database clock step moves: the holder's lease
/// is its own monotonic deadline, and a takeover replaces the token.
pub(super) async fn require_fanout(
    tx: &mut Transaction<'_, Postgres>,
    claim: &FanoutClaim,
) -> Result<()> {
    let valid:Option<bool>=sqlx::query_scalar("SELECT a.claim_token=$2 AND b.chain_state='confirmed' AND b.maturity_state='mature' FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE a.fanout_txid=$1 FOR UPDATE OF a")
        .bind(&claim.fanout_txid).bind(&claim.claim_token).fetch_optional(&mut **tx).await?;
    ensure!(
        valid == Some(true),
        "fanout claim lost or parent no longer mature and active"
    );
    Ok(())
}

pub(super) async fn apply_progress(
    tx: &mut Transaction<'_, Postgres>,
    claim: &FanoutClaim,
    status: &str,
    result: Option<&Value>,
) -> Result<()> {
    if let Some(expected) = result.and_then(|value| value["payout_revision"].as_i64()) {
        require_revision(tx, expected).await?;
    }
    if status == "confirmed" {
        let result = result.context("confirmed fanout lacks chain evidence")?;
        let confirmation = &result["confirmation"];
        let hash = confirmation["block_hash"]
            .as_str()
            .context("confirmed fanout lacks block hash")?;
        let height = confirmation["block_height"]
            .as_i64()
            .context("confirmed fanout lacks block height")?;
        let depth = confirmation["confirmations"]
            .as_i64()
            .context("confirmed fanout lacks depth")?;
        ensure!(
            height >= 0 && depth > 0,
            "invalid fanout confirmation evidence"
        );
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET confirmed_block_hash=$3,confirmed_block_height=$4,confirmed_depth=$5 WHERE fanout_txid=$1 AND claim_token=$2")
            .bind(&claim.fanout_txid).bind(&claim.claim_token).bind(hash).bind(height).bind(depth).execute(&mut **tx).await?;
    } else if status != "failed" {
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET confirmed_block_hash=NULL,confirmed_block_height=NULL,confirmed_depth=0 WHERE fanout_txid=$1 AND claim_token=$2")
            .bind(&claim.fanout_txid).bind(&claim.claim_token).execute(&mut **tx).await?;
    }
    if let Some(delay) = result.and_then(|r| r["next_check_seconds"].as_i64()) {
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=clock_timestamp()+$2*interval '1 second' WHERE fanout_txid=$1")
            .bind(&claim.fanout_txid).bind(delay.clamp(1,3600)).execute(&mut **tx).await?;
    }
    Ok(())
}

impl Ledger {
    /// The fanout claim lane (#654, #668): claim the next unclaimed, due
    /// fanout `FANOUT_CLAIMABLE_SQL` admits, for `$3` seconds under token
    /// `$1` and instance `$2`, in the lane's order (every settlement before
    /// any confirmation check, then the oldest schedule, never-attempted
    /// first, then block height and chunk), returning
    /// `FANOUT_CLAIMED_COLUMNS`.
    ///
    /// It reads only the fanouts due now, never the settled history or the
    /// fanouts still waiting for their next check (#668). Its candidates are
    /// gathered once into an array: from the lane's index, as two ranges of
    /// it, those never attempted and those scheduled at or before
    /// `statement_timestamp()`, and the checkpoint (`FANOUT_CHECKPOINT_SQL`).
    /// The bound must stay `statement_timestamp()`: it is stable, so the
    /// index serves it as a range, where the volatile `clock_timestamp()`
    /// would only filter every entry. A fanout falling due later in the same
    /// statement waits for the next poll. The selection looks the candidates
    /// up by primary key and applies `FANOUT_CLAIMABLE_SQL` and the lane's
    /// own `clock_timestamp()` schedule test in full. That test is what
    /// decides the checkpoint, which no candidate bound covers, and it is
    /// checked again on a row another transaction changed before this one
    /// locked it. `FOR UPDATE SKIP LOCKED` leaves a fanout another
    /// transaction holds to a later poll.
    pub fn fanout_lane_sql() -> String {
        format!(
            "WITH next AS (SELECT a.fanout_txid FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) \
             WHERE a.fanout_txid=ANY(ARRAY(SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE {FANOUT_LANE_INDEXED_SQL} AND next_broadcast_attempt_at IS NULL \
             UNION ALL SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE {FANOUT_LANE_INDEXED_SQL} AND next_broadcast_attempt_at<=statement_timestamp() \
             UNION ALL ({FANOUT_CHECKPOINT_SQL}))) \
             AND {FANOUT_CLAIMABLE_SQL} \
             AND (a.next_broadcast_attempt_at IS NULL OR a.next_broadcast_attempt_at<=clock_timestamp()) AND a.claim_token IS NULL \
             ORDER BY (a.settlement_status='confirmed'),a.next_broadcast_attempt_at NULLS FIRST,b.block_height,a.chunk_index FOR UPDATE OF a SKIP LOCKED LIMIT 1) \
             UPDATE qbit_ctv_fanout_artifacts a SET claim_token=$1,claim_instance_id=$2,claim_expires_at=clock_timestamp()+$3*interval '1 second',claim_lease_seconds=$3::integer,claim_renewals=0 \
             FROM next WHERE a.fanout_txid=next.fanout_txid RETURNING {FANOUT_CLAIMED_COLUMNS}"
        )
    }

    /// The statement every fanout claim poll opens with (#654), in one round
    /// trip, as [`Ledger::claim_survey_sql`] is for candidates: it reschedules,
    /// as due now, every unclaimed fanout a claim may take whose schedule
    /// `FANOUT_STEPPED_SQL` shows the clock stepped back over, so the lane
    /// keeps comparing `next_broadcast_attempt_at` with the clock as it always
    /// has; and it reads every claimed fanout's version for this frontend's
    /// observer, with whether `FANOUT_CLAIMABLE_SQL` and `FANOUT_TAKEOVER_SQL`
    /// let it be taken over. Rows another transaction holds are skipped and
    /// rescheduled by a later poll. `statement_timestamp()` is stable, so its
    /// bound lets the broadcast index serve the reschedule as a range over
    /// rows still waiting. Always one row: a `NULL` txid when nothing is
    /// claimed.
    pub fn fanout_claim_survey_sql() -> String {
        format!(
            "WITH stepped AS (SELECT a.fanout_txid FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE a.claim_token IS NULL AND {FANOUT_CLAIMABLE_SQL} AND a.next_broadcast_attempt_at>statement_timestamp() AND a.next_broadcast_attempt_at>clock_timestamp() AND {FANOUT_STEPPED_SQL} FOR UPDATE OF a SKIP LOCKED), \
             made_due AS (UPDATE qbit_ctv_fanout_artifacts a SET next_broadcast_attempt_at=clock_timestamp(),updated_at=clock_timestamp() FROM stepped WHERE a.fanout_txid=stepped.fanout_txid RETURNING a.fanout_txid) \
             SELECT (SELECT count(*) FROM made_due) AS made_due,a.fanout_txid,a.claim_token,a.claim_renewals,a.claim_lease_seconds,a.claim_instance_id,COALESCE({FANOUT_CLAIMABLE_SQL} AND {FANOUT_TAKEOVER_SQL},false) AS due \
             FROM (SELECT 1) AS one LEFT JOIN (qbit_ctv_fanout_artifacts a LEFT JOIN qbit_pool_blocks b USING(block_hash)) ON a.claim_token IS NOT NULL \
             ORDER BY a.fanout_txid"
        )
    }

    /// Run [`Ledger::fanout_claim_survey_sql`] in `tx` and hand its claims to
    /// this process's fanout observer, timed from the instant the reply
    /// arrived.
    pub(super) async fn survey_fanout_claims(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<Vec<ClaimVersion>> {
        let rows = sqlx::query(&Self::fanout_claim_survey_sql())
            .fetch_all(&mut **tx)
            .await?;
        let replied = tokio::time::Instant::now();
        let (made_due, claims) = decode_survey(&rows, "fanout_txid")?;
        if made_due > 0 {
            tracing::warn!(
                rows = made_due,
                "CTV fanout attempts were last scheduled later than the database clock reads now, so the clock stepped back; they are due now"
            );
        }
        self.fanout_claim_observer.survey(&claims, replied);
        Ok(claims)
    }

    /// Take over the first fanout claim this process has watched go
    /// unrenewed for its whole lease (#654), by compare and set on the
    /// version it timed, which any renewal, release or other takeover since
    /// has changed. `FOR UPDATE SKIP LOCKED` conflicts with the holder's
    /// `FOR UPDATE` while one of its fenced writes is open, as the lane's
    /// selection does, so a completion is never taken over mid-commit; the
    /// next poll tries again.
    pub(super) async fn take_over_fanout(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        claims: &[ClaimVersion],
        token: &str,
        lease_seconds: i64,
    ) -> Result<Option<PgRow>> {
        let query = format!("WITH next AS (SELECT a.fanout_txid FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE a.fanout_txid=$4 AND a.claim_token=$5 AND a.claim_renewals=$6 AND {FANOUT_CLAIMABLE_SQL} AND {FANOUT_TAKEOVER_SQL} FOR UPDATE OF a SKIP LOCKED) UPDATE qbit_ctv_fanout_artifacts a SET claim_token=$1,claim_instance_id=$2,claim_expires_at=clock_timestamp()+$3*interval '1 second',claim_lease_seconds=$3::integer,claim_renewals=0 FROM next WHERE a.fanout_txid=next.fanout_txid RETURNING {FANOUT_CLAIMED_COLUMNS}");
        for claim in self.fanout_claim_observer.takeable(claims) {
            let row = sqlx::query(&query)
                .bind(token)
                .bind(&self.instance_id)
                .bind(lease_seconds)
                .bind(&claim.key)
                .bind(&claim.token)
                .bind(claim.renewals)
                .fetch_optional(&mut **tx)
                .await?;
            if row.is_some() {
                tracing::warn!(
                    fanout = %claim.key,
                    holder = claim.instance_id.as_deref().unwrap_or("unknown"),
                    lease_seconds = claim.lease().as_secs(),
                    "took over a CTV fanout claim this frontend watched go unrenewed for its whole lease"
                );
                return Ok(row);
            }
        }
        Ok(None)
    }
}

/// Revoke CTV fanout claims (#654), as [`super::revoke_candidate_claims`]
/// revokes candidate claims: end the lease of `fanout_txid`'s claim, or of
/// every claim with `None`, as if its holder had stopped renewing it a whole
/// lease ago. `claim_lease_seconds` becomes 0, so the next frontend to read
/// the claim takes it over at once, by compare and set on the version it
/// read, and `claim_expires_at` moves into the past so the database clock's
/// estimate agrees. The token stays: the holder may still renew or write
/// until a takeover replaces it. Only claimed fanouts are touched, and no
/// schedule: a takeover does not wait for one, and an unclaimed or settled
/// fanout keeps its own, `infinity` holds included.
///
/// The one hook the lease tests use where waiting out a real lease is not
/// deterministic. An operator may run the same statement only once the
/// holder is known to be gone.
pub async fn revoke_fanout_claims<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    fanout_txid: Option<&str>,
) -> Result<u64> {
    Ok(sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_lease_seconds=0,claim_expires_at=LEAST(claim_expires_at,clock_timestamp()-interval '1 second') WHERE claim_token IS NOT NULL AND ($1::text IS NULL OR fanout_txid=$1)")
        .bind(fanout_txid)
        .execute(executor)
        .await?
        .rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SQL without its whitespace, to compare texts written apart.
    fn compact(sql: &str) -> String {
        sql.chars().filter(|c| !c.is_whitespace()).collect()
    }

    /// The lane's candidates repeat the predicate of 024's index, or the
    /// planner cannot read the index and every claim reads the settled
    /// history again (#668). `ledger_postgres::fanout_lane_plan` proves the
    /// plan on PostgreSQL, behind the integration gate; this does not need
    /// a database.
    #[test]
    fn the_lane_repeats_its_index_predicate() {
        let statement = include_str!("../../migrations/024_fanout_lane_index.sql")
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let (_, predicate) = statement
            .split_once("WHERE")
            .expect("024's index is partial");
        assert_eq!(
            format!("({})", compact(predicate).trim_end_matches(';')),
            compact(FANOUT_LANE_INDEXED_SQL)
        );
        assert_eq!(
            Ledger::fanout_lane_sql()
                .matches(FANOUT_LANE_INDEXED_SQL)
                .count(),
            2
        );
    }

    /// The checkpoint the lane gathers is the one `FANOUT_CLAIMABLE_SQL`
    /// admits, or reconciliation would stop watching it.
    #[test]
    fn the_lane_gathers_the_checkpoint_a_claim_admits() {
        assert!(FANOUT_CLAIMABLE_SQL.contains(&format!("a.fanout_txid=({FANOUT_CHECKPOINT_SQL})")));
        assert!(Ledger::fanout_lane_sql().contains(&format!("UNION ALL ({FANOUT_CHECKPOINT_SQL})")));
    }
}

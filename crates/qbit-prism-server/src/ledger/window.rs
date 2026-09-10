use super::*;

#[derive(Clone, Debug)]
pub struct AppendResult {
    pub share: AcceptedShare,
    pub inserted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub anchor_ms: i64,
    pub share_seq: u64,
    pub payout_revision: i64,
    pub shares: Vec<AcceptedShare>,
    pub prior_balances: Vec<CarryForwardBalance>,
}

impl Ledger {
    /// Coordinate nodes by cumulative proof of work. A slower peer or an
    /// equal-work sibling cannot reverse another instance's accepted chain.
    pub async fn observe_chain_view(
        &self,
        tip: &str,
        height: u64,
        chainwork_hex: &str,
    ) -> Result<i64> {
        ensure!(
            tip.len() == 64 && tip.bytes().all(|c| c.is_ascii_hexdigit()),
            "invalid chain tip hash"
        );
        ensure!(
            !chainwork_hex.is_empty()
                && chainwork_hex.len() <= 64
                && chainwork_hex.bytes().all(|c| c.is_ascii_hexdigit()),
            "invalid cumulative chainwork"
        );
        let work = num_bigint::BigUint::parse_bytes(chainwork_hex.as_bytes(), 16)
            .context("invalid chainwork")?;
        ensure!(
            work > num_bigint::BigUint::from(0u8),
            "chainwork must be positive"
        );
        let work = work.to_str_radix(10);
        let height = i64::try_from(height)?;
        let tip = tip.to_ascii_lowercase();
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        let row=sqlx::query("SELECT payout_revision,best_chainwork=$1::text::numeric AS same_work,best_chainwork<$1::text::numeric AS more_work,best_tip_hash,best_tip_height FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .bind(&work).fetch_one(&mut *tx).await?;
        let same: bool = row.try_get("same_work")?;
        let greater: bool = row.try_get("more_work")?;
        ensure!(
            greater || same,
            "local node is behind the cluster's cumulative chainwork"
        );
        let mut revision: i64 = row.try_get("payout_revision")?;
        if same {
            ensure!(
                row.try_get::<Option<String>, _>("best_tip_hash")?
                    .as_deref()
                    == Some(&tip)
                    && row.try_get::<Option<i64>, _>("best_tip_height")? == Some(height),
                "local node follows a conflicting equal-work chain tip"
            );
        } else {
            revision=sqlx::query_scalar("UPDATE qbit_prism_cluster SET best_chainwork=$1::text::numeric,best_tip_hash=$2,best_tip_height=$3,payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton RETURNING payout_revision")
                .bind(work).bind(tip).bind(height).fetch_one(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(revision)
    }

    pub async fn append(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
    ) -> Result<AppendResult> {
        self.append_checked(share, candidate, None).await
    }

    pub async fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        expected_revision: i64,
    ) -> Result<AppendResult> {
        self.append_checked(share, candidate, Some(expected_revision))
            .await
    }

    async fn append_checked(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        expected_revision: Option<i64>,
    ) -> Result<AppendResult> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        if let Some(expected) = expected_revision {
            let revision:i64=sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL FOR SHARE").fetch_one(&mut *tx).await?;
            ensure!(
                revision == expected,
                "payout revision changed before share commit"
            );
        }
        let result = self.append_in(&mut tx, share).await?;
        if let Some(candidate) = candidate {
            ensure!(
                candidate.deferred_share.is_none(),
                "credited candidates cannot also contain a deferred share"
            );
            persist_candidate(&mut tx, &candidate, Some(&result.share.share_id)).await?;
        }
        tx.commit().await?;
        Ok(result)
    }

    pub(super) async fn append_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut share: AcceptedShare,
    ) -> Result<AppendResult> {
        ensure!(
            share.share_difficulty > 0 && share.network_difficulty > 0,
            "share difficulty must be positive"
        );
        ensure!(
            share
                .credit_policy
                .as_deref()
                .is_none_or(|p| p == "stale-grace"),
            "invalid share credit policy"
        );
        let program = hex::decode(&share.p2mr_program_hex)?;
        ensure!(program.len() == 32, "P2MR program must be 32 bytes");
        share.p2mr_program_hex = hex::encode(program);
        let existing = sqlx::query(&format!("{SELECT_SHARE} WHERE share_id=$1"))
            .bind(&share.share_id)
            .fetch_optional(&mut **tx)
            .await?;
        if let Some(row) = existing {
            let previous = share_from_row(&row)?;
            share.share_seq = previous.share_seq;
            share.accepted_at_ms = previous.accepted_at_ms;
            ensure!(share == previous, "duplicate share_id payload mismatch");
            return Ok(AppendResult {
                share: previous,
                inserted: false,
            });
        }
        let header_hash = share_header_hash(&share.share_id);
        let duplicate: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_prism_share_hashes WHERE header_hash=$1)",
        )
        .bind(&header_hash)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            !duplicate,
            "duplicate-share: header already credited globally"
        );
        let accepted_at_ms: i64 = sqlx::query_scalar("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint) WHERE singleton RETURNING ledger_clock_ms").fetch_one(&mut **tx).await?;
        ensure!(
            share.job_issued_at_ms <= accepted_at_ms,
            "share references a job from the future"
        );
        let seq: i64 = sqlx::query_scalar("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) VALUES($1,$2,$3,decode($4,'hex'),$5::text::numeric,$6::text::numeric,$7,$8,to_timestamp($9::double precision/1000),$10,to_timestamp($11::double precision/1000),$12,true,$13,0) RETURNING share_seq")
            .bind(&share.share_id).bind(&share.miner_id).bind(&share.order_key).bind(&share.p2mr_program_hex)
            .bind(share.share_difficulty.to_string()).bind(share.network_difficulty.to_string()).bind(i64::try_from(share.template_height)?)
            .bind(&share.job_id).bind(share.job_issued_at_ms).bind(i64::from(share.ntime)).bind(accepted_at_ms).bind(&share.credit_policy).bind(&self.instance_id)
            .fetch_one(&mut **tx).await?;
        sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,$2)")
            .bind(header_hash)
            .bind(&share.share_id)
            .execute(&mut **tx)
            .await?;
        share.share_seq = u64::try_from(seq)?;
        share.accepted_at_ms = accepted_at_ms;
        Ok(AppendResult {
            share,
            inserted: true,
        })
    }

    /// Captures all three inputs under the same database boundary: ordered
    /// shares, prior balances and their revision. Timestamp barriers preserve
    /// the existing public audit format without relying on host clock sync.
    pub async fn snapshot(&self, network_difficulty: u128) -> Result<Snapshot> {
        let weight = network_difficulty
            .checked_mul(8)
            .context("window difficulty overflow")?;
        ensure!(weight > 0, "network difficulty must be positive");
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let row = sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint)+1 WHERE singleton RETURNING ledger_clock_ms-1 AS anchor_ms,payout_revision").fetch_one(&mut *tx).await?;
        let anchor_ms: i64 = row.try_get("anchor_ms")?;
        let payout_revision: i64 = row.try_get("payout_revision")?;
        let cutoff: i64 = sqlx::query_scalar(
            "SELECT COALESCE(max(share_seq),0) FROM qbit_share_ledger WHERE accepted",
        )
        .fetch_one(&mut *tx)
        .await?;
        let prior_balances = read_prior_balances(&mut tx).await?;
        tx.commit().await?;
        // Ledger rows are immutable and later commits receive a timestamp
        // strictly greater than this anchor. Release the ordering barrier
        // before scanning a potentially large payout window.
        let mut tx = self.pool.begin().await?;
        let mut shares = Vec::new();
        let mut remaining = weight;
        let mut cursor = cutoff.checked_add(1).context("share sequence exhausted")?;
        while remaining > 0 {
            let rows = sqlx::query(&format!("{SELECT_SHARE} WHERE accepted AND share_seq<$1 AND accepted_at<=to_timestamp($2::double precision/1000) AND job_issued_at<=to_timestamp($2::double precision/1000) ORDER BY share_seq DESC LIMIT 4096"))
                .bind(cursor).bind(anchor_ms).fetch_all(&mut *tx).await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let share = share_from_row(&row)?;
                cursor = i64::try_from(share.share_seq)?;
                remaining = remaining.saturating_sub(share.share_difficulty);
                shares.push(share);
                if remaining == 0 {
                    break;
                }
            }
        }
        shares.reverse();
        tx.commit().await?;
        Ok(Snapshot {
            anchor_ms,
            share_seq: u64::try_from(cutoff)?,
            payout_revision,
            shares,
            prior_balances,
        })
    }
}

pub(super) async fn read_prior_balances(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<CarryForwardBalance>> {
    sqlx::query("SELECT miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,balance_sats::text AS balance FROM qbit_current_carry_forward_balances()")
        .fetch_all(&mut **tx).await?.into_iter().map(|row| Ok(CarryForwardBalance {
            recipient_id: row.try_get("miner_id")?, order_key: row.try_get("payout_order_key")?,
            p2mr_program_hex: row.try_get("program")?, balance_sats: row.try_get::<String,_>("balance")?.parse()?,
        })).collect()
}

fn share_header_hash(share_id: &str) -> String {
    if let Some(suffix) = share_id.get(share_id.len().saturating_sub(64)..) {
        if suffix.len() == 64 && suffix.bytes().all(|b| b.is_ascii_hexdigit()) {
            return suffix.to_ascii_lowercase();
        }
    }
    hex::encode(Sha256::digest(share_id.as_bytes()))
}

pub(super) fn share_from_row(row: &PgRow) -> Result<AcceptedShare> {
    Ok(AcceptedShare {
        share_seq: u64::try_from(row.try_get::<i64, _>("share_seq")?)?,
        share_id: row.try_get("share_id")?,
        miner_id: row.try_get("miner_id")?,
        order_key: row.try_get("payout_order_key")?,
        p2mr_program_hex: row.try_get("program")?,
        share_difficulty: row.try_get::<String, _>("difficulty")?.parse()?,
        network_difficulty: row.try_get::<String, _>("network_difficulty")?.parse()?,
        template_height: u64::try_from(row.try_get::<i64, _>("template_height")?)?,
        job_id: row.try_get("job_id")?,
        job_issued_at_ms: row
            .try_get::<DateTime<Utc>, _>("job_issued_at")?
            .timestamp_millis(),
        accepted_at_ms: row
            .try_get::<DateTime<Utc>, _>("accepted_at")?
            .timestamp_millis(),
        ntime: u32::try_from(row.try_get::<i64, _>("ntime")?)?,
        credit_policy: row.try_get("credit_policy")?,
    })
}

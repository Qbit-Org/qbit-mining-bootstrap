use super::*;

impl Ledger {
    /// Renew only a still-live token. An expired worker must not revive itself
    /// before wallet or network mutations after another instance takes over.
    pub async fn renew_fanout_claim(&self, claim: &FanoutClaim, seconds: i64) -> Result<()> {
        ensure!(
            (1..=600).contains(&seconds),
            "invalid fanout lease duration"
        );
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_expires_at=clock_timestamp()+$3*interval '1 second' WHERE fanout_txid=$1 AND claim_token=$2")
            .bind(&claim.fanout_txid).bind(&claim.claim_token).bind(seconds).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_fanout_scan(
        &self,
        claim: &FanoutClaim,
        next: u64,
        anchor: Option<(u64, String)>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
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
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        apply_progress(&mut tx, claim, status, Some(&result)).await?;
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status=$3,next_broadcast_attempt_at=clock_timestamp()+$4*interval '1 second',claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE fanout_txid=$1 AND claim_token=$2")
            .bind(&claim.fanout_txid).bind(&claim.claim_token).bind(status).bind(delay).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn halt_fanout_reorg(
        &self,
        claim: &FanoutClaim,
        expected_revision: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, SETTLEMENT_LOCK).await?;
        require_fanout(&mut tx, claim).await?;
        require_revision(&mut tx, expected_revision).await?;
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=$1,updated_at=clock_timestamp() WHERE singleton")
            .bind(format!("deep confirmed CTV fanout disconnected: {}; manual reconciliation required",claim.fanout_txid)).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn cpfp_package(&self, fanout_txid: &str) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(p) FROM qbit_prism_cpfp_packages p WHERE fanout_txid=$1",
        )
        .bind(fanout_txid)
        .fetch_optional(&self.pool)
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
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        let inserted=sqlx::query("INSERT INTO qbit_prism_cpfp_packages(fanout_txid,funding_txid,funding_vout,funding_value_sats,wallet_name) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
            .bind(&claim.fanout_txid).bind(txid).bind(i32::try_from(vout)?).bind(i64::try_from(value)?).bind(wallet).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(inserted == 1)
    }

    pub async fn save_cpfp_package(
        &self,
        claim: &FanoutClaim,
        signed_child_hex: &str,
        child_txid: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
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
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        sqlx::query("UPDATE qbit_prism_cpfp_packages SET wallet_lock_released=true,updated_at=clock_timestamp() WHERE fanout_txid=$1").bind(&claim.fanout_txid).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_cpfp_wallet_lock_pending(&self, claim: &FanoutClaim) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        require_fanout(&mut tx, claim).await?;
        sqlx::query("UPDATE qbit_prism_cpfp_packages SET wallet_lock_released=false,updated_at=clock_timestamp() WHERE fanout_txid=$1").bind(&claim.fanout_txid).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub(super) async fn require_fanout(
    tx: &mut Transaction<'_, Postgres>,
    claim: &FanoutClaim,
) -> Result<()> {
    let valid:Option<bool>=sqlx::query_scalar("SELECT a.claim_token=$2 AND a.claim_expires_at>clock_timestamp() AND b.chain_state='confirmed' AND b.maturity_state='mature' FROM qbit_ctv_fanout_artifacts a JOIN qbit_pool_blocks b USING(block_hash) WHERE a.fanout_txid=$1 FOR UPDATE OF a")
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

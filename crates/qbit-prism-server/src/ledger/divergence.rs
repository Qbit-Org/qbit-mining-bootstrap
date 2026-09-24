//! #478 block capture: the offer-time overpay ceiling and the
//! confirmation-time divergence record (migration 020).
//!
//! A block whose work was issued before a landing it does not include pays,
//! on chain, the prior balances its coinbase was built with. When those have
//! been paid down by the time its rows count, an account is paid twice
//! against one balance and the additive balance carries the difference as
//! debt. Per account `m`, with `issued(m)` the as-issued prior, `p(m)` the
//! canonical balance when the block's rows start to count (its
//! confirmation), and `g(m)`, `o(m)` its gross and on-chain amounts, that
//! debt is `max(0, -(p + g - o)) - max(0, -p)` when positive.
//!
//! The payout policy never pays more than the candidate balance
//! (`o <= max(0, issued + g)`, so `o - g <= max(0, issued)`). The debt a
//! block creates is therefore at most `max(0, issued(m))` per account for any
//! `p`: at most `max(0, (o - g) - p)` when `p >= 0`, and at most
//! `max(p, o - g) <= max(0, issued)` when `p < 0`. Summed, a block's realized
//! debt is at most its positive as-issued float, `F = sum over m of
//! max(0, issued(m))`, whatever confirms before or after it. That is the
//! bound the offer holds to the ceiling: it needs only the block's own
//! immutable snapshot.
use super::*;
use qbit_prism::{PayoutPolicyAccount, PayoutPolicyAccountType};
use std::collections::{BTreeMap, HashMap};

/// What an offer reservation decided for a pending block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OfferReservation {
    /// Reserved for its one offer. `bound` is present exactly when the
    /// block's payout revision was superseded and the bound was checked.
    Reserved { bound: Option<OverpayBound> },
    /// Not reserved: the bound exceeds the ceiling, or capture is off. The
    /// row is still pending; the caller abandons it.
    Refused(OverpayBound),
}

/// The overpay bound a reservation held to the ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverpayBound {
    /// The block's positive as-issued float; 0 when capture is off, which
    /// computes none.
    pub bound_sats: i128,
    pub ceiling_sats: i128,
    pub ceiling_bps: u16,
    /// The payout revision the reservation observed.
    pub observed_revision: i64,
}

/// `bps` basis points of a coinbase value, rounded down.
pub fn overpay_ceiling_sats(coinbase_value_sats: u64, bps: u16) -> i128 {
    i128::from(coinbase_value_sats) * i128::from(bps) / 10_000
}

fn program_key(hex: &str) -> String {
    hex.to_ascii_lowercase()
}

fn balance_map(balances: &[CarryForwardBalance]) -> HashMap<String, i128> {
    let mut map = HashMap::new();
    for balance in balances {
        *map.entry(program_key(&balance.p2mr_program_hex))
            .or_default() += balance.balance_sats;
    }
    map
}

/// `F = sum over m of max(0, issued(m))`: the most debt a block built on
/// `issued` can create, in every order of confirmations (module comment).
pub fn overpay_bound(issued: &[CarryForwardBalance]) -> i128 {
    balance_map(issued)
        .into_values()
        .map(|balance| balance.max(0))
        .sum()
}

/// One account a divergent landing overpaid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountOverpay {
    pub p2mr_program_hex: String,
    pub miner_id: String,
    pub issued_prior_sats: i128,
    pub current_prior_sats: i128,
    pub gross_sats: i128,
    pub onchain_sats: i128,
    pub overpay_sats: i128,
    pub debt_after_sats: i128,
}

/// What a block's rows do to the carry-forward debt when they start to count,
/// from the canonical balances at that moment and the accounts its coinbase
/// commits to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LandingDivergence {
    /// Accounts whose as-issued prior differs from the current balance.
    pub divergent_accounts: usize,
    /// The debt this landing creates: per account, the increase in
    /// `max(0, -balance)`.
    pub overpay_sats: i128,
    /// What the accounts it overpaid owe after it.
    pub overpaid_debt_after_sats: i128,
    /// What every account owes after it.
    pub pool_debt_after_sats: i128,
    /// The accounts it overpaid, by program.
    pub overpaid: Vec<AccountOverpay>,
}

/// One program's rows in a block: its as-issued prior, gross and on-chain.
#[derive(Clone, Debug)]
pub struct CarryRow {
    pub p2mr_program_hex: String,
    pub miner_id: String,
    pub issued_prior_sats: i128,
    pub gross_sats: i128,
    pub onchain_sats: i128,
}

/// [`rows_divergence`] of a landing's miner accounts.
pub fn landing_divergence(
    accounts: &[PayoutPolicyAccount],
    current: &[CarryForwardBalance],
) -> LandingDivergence {
    rows_divergence(
        accounts
            .iter()
            .filter(|account| account.account_type == PayoutPolicyAccountType::Miner)
            .map(|account| CarryRow {
                p2mr_program_hex: account.p2mr_program_hex.clone(),
                miner_id: account.recipient_id.clone(),
                issued_prior_sats: account.prior_balance_sats,
                gross_sats: i128::from(account.gross_amount_sats),
                onchain_sats: i128::from(account.onchain_amount_sats),
            }),
        current,
    )
}

/// The debt a block's rows create when they start to count on `current`.
pub fn rows_divergence(
    block_rows: impl IntoIterator<Item = CarryRow>,
    current: &[CarryForwardBalance],
) -> LandingDivergence {
    struct Row {
        miner_id: String,
        issued: i128,
        gross: i128,
        onchain: i128,
    }
    let current = balance_map(current);
    let mut rows: BTreeMap<String, Row> = BTreeMap::new();
    for row in block_rows {
        let entry = rows
            .entry(program_key(&row.p2mr_program_hex))
            .or_insert_with(|| Row {
                miner_id: row.miner_id.clone(),
                issued: 0,
                gross: 0,
                onchain: 0,
            });
        entry.issued += row.issued_prior_sats;
        entry.gross += row.gross_sats;
        entry.onchain += row.onchain_sats;
    }
    let mut divergence = LandingDivergence::default();
    for (program, row) in &rows {
        let before = current.get(program).copied().unwrap_or(0);
        let after = before + row.gross - row.onchain;
        let debt_before = (-before).max(0);
        let debt_after = (-after).max(0);
        let overpay = (debt_after - debt_before).max(0);
        divergence.divergent_accounts += usize::from(row.issued != before);
        divergence.pool_debt_after_sats += debt_after;
        if overpay > 0 {
            divergence.overpay_sats += overpay;
            divergence.overpaid_debt_after_sats += debt_after;
            divergence.overpaid.push(AccountOverpay {
                p2mr_program_hex: program.clone(),
                miner_id: row.miner_id.clone(),
                issued_prior_sats: row.issued,
                current_prior_sats: before,
                gross_sats: row.gross,
                onchain_sats: row.onchain,
                overpay_sats: overpay,
                debt_after_sats: debt_after,
            });
        }
    }
    divergence.pool_debt_after_sats += current
        .iter()
        .filter(|(program, _)| !rows.contains_key(*program))
        .map(|(_, balance)| (-balance).max(0))
        .sum::<i128>();
    divergence
}

impl Ledger {
    /// The durable reservation before the one `submitblock` call: the
    /// pending row this live claim holds becomes `offer_reserved`, recording
    /// which instance took it and when (database clock). The row is the
    /// unique reservation per block hash; once it commits, no claim on any
    /// frontend, this one included after a crash, will offer the block
    /// again.
    pub async fn reserve_offer(&self, claim: &CandidateClaim) -> Result<()> {
        match self.reserve_offer_within(claim, None).await? {
            OfferReservation::Reserved { .. } => Ok(()),
            OfferReservation::Refused(bound) => {
                bail!("an unbounded reservation refused its offer: {bound:?}")
            }
        }
    }

    /// [`Ledger::reserve_offer`], holding a block whose payout revision was
    /// superseded to `ceiling_bps` of its coinbase value (#478).
    ///
    /// The revision is first read `FOR SHARE` in the reservation's own
    /// transaction. Every revision bump updates that row, so no bump can
    /// commit between that read and the reservation's commit: a block still
    /// current here is reserved current. (Share appends also update the row,
    /// so they wait at most for this short transaction.)
    ///
    /// A superseded block's bound is its positive as-issued float, which
    /// needs only its own immutable snapshot, read and parsed after the row
    /// lock is released. The decision and the reservation commit together.
    /// Over the ceiling nothing is reserved and the decision alone is
    /// recorded; with a ceiling of 0 no bound is computed and the refusal is
    /// recorded as `disabled`. Any error, including a missing snapshot,
    /// reserves nothing and decides nothing. A leased candidate, or `None`,
    /// is reserved unconditionally.
    pub async fn reserve_offer_within(
        &self,
        claim: &CandidateClaim,
        ceiling_bps: Option<u16>,
    ) -> Result<OfferReservation> {
        let candidate = &claim.candidate;
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let Some(bps) = ceiling_bps.filter(|_| !candidate.leased) else {
            return self.reserve_in(tx, claim, None).await;
        };
        let observed_revision: i64 = sqlx::query_scalar(
            "SELECT payout_revision FROM qbit_prism_cluster WHERE singleton FOR SHARE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if observed_revision == candidate.payout_revision {
            return self.reserve_in(tx, claim, None).await;
        }
        let ceiling_sats = overpay_ceiling_sats(candidate.found_block.coinbase_value_sats, bps);
        if bps == 0 {
            record_offer_decision(
                &mut tx,
                candidate,
                observed_revision,
                None,
                ceiling_sats,
                "disabled",
            )
            .await?;
            tx.commit().await?;
            return Ok(OfferReservation::Refused(OverpayBound {
                bound_sats: 0,
                ceiling_sats,
                ceiling_bps: bps,
                observed_revision,
            }));
        }
        // Release the row lock before the snapshot is read and parsed.
        tx.rollback().await?;
        let issued = self.as_issued_snapshot(candidate).await?;
        let bound_sats = tokio::task::spawn_blocking(move || overpay_bound(&issued)).await?;
        let bound = OverpayBound {
            bound_sats,
            ceiling_sats,
            ceiling_bps: bps,
            observed_revision,
        };
        let offered = bound.bound_sats <= bound.ceiling_sats;
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        record_offer_decision(
            &mut tx,
            candidate,
            observed_revision,
            Some(bound_sats),
            ceiling_sats,
            if offered { "offered" } else { "abandoned" },
        )
        .await?;
        if !offered {
            tx.commit().await?;
            return Ok(OfferReservation::Refused(bound));
        }
        self.reserve_in(tx, claim, Some(bound)).await
    }

    /// Count the committed divergent confirmations and set the debt gauge
    /// from the balance-derived debt, once the transaction committed.
    pub(super) fn record_debt_metrics<'a>(
        &self,
        divergences: impl Iterator<Item = &'a LandingDivergence>,
        debt: Option<u64>,
    ) {
        let Some(metrics) = self.metrics.as_deref() else {
            return;
        };
        for divergence in divergences.filter(|divergence| divergence.divergent_accounts > 0) {
            metrics.record_divergent_landing(
                u64::try_from(divergence.overpay_sats).unwrap_or(u64::MAX),
            );
        }
        if let Some(debt) = debt {
            metrics.record_carry_forward_debt(debt);
        }
    }

    /// Record that capture is off for a pending block whose payout revision
    /// was superseded, before the caller abandons it (#478).
    pub async fn record_capture_disabled(
        &self,
        candidate: &Candidate,
        observed_revision: i64,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        record_offer_decision(&mut tx, candidate, observed_revision, None, 0, "disabled").await?;
        tx.commit().await?;
        Ok(())
    }

    /// The candidate's as-issued balances, parsed off the runtime.
    async fn as_issued_snapshot(&self, candidate: &Candidate) -> Result<Vec<CarryForwardBalance>> {
        let digest = hex::encode(candidate.window.prior_balances_digest);
        let bytes: Vec<u8> = sqlx::query_scalar(
            "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
        )
        .bind(&digest)
        .fetch_optional(&mut *self.acquire().await?)
        .await?
        .with_context(|| {
            format!(
                "the as-issued balance snapshot {digest} is missing, so the overpay bound is unknown"
            )
        })?;
        tokio::task::spawn_blocking(move || Ok(serde_json::from_slice(&bytes)?)).await?
    }

    /// Take the reservation in `tx` and commit.
    async fn reserve_in(
        &self,
        mut tx: Transaction<'static, Postgres>,
        claim: &CandidateClaim,
        bound: Option<OverpayBound>,
    ) -> Result<OfferReservation> {
        let candidate = &claim.candidate;
        sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR NO KEY UPDATE")
            .bind(&candidate.block_hash).fetch_optional(&mut *tx).await?;
        let reserved = sqlx::query("UPDATE qbit_block_candidate_outbox SET state='offer_reserved',offer_reserved_at=clock_timestamp(),offer_reserved_by=$3,updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending' AND claim_expires_at>clock_timestamp()")
            .bind(&candidate.block_hash).bind(&claim.claim_token).bind(&self.instance_id).execute(&mut *tx).await?.rows_affected();
        ensure!(
            reserved == 1,
            "candidate claim was lost or expired before the offer reservation"
        );
        tx.commit().await?;
        Ok(OfferReservation::Reserved { bound })
    }
}

async fn record_offer_decision(
    tx: &mut Transaction<'_, Postgres>,
    candidate: &Candidate,
    observed_revision: i64,
    bound_sats: Option<i128>,
    ceiling_sats: i128,
    decision: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO qbit_prism_payout_divergences(block_hash,block_height,candidate_payout_revision,coinbase_value_sats,as_issued_prior_balances_sha256,offer_observed_payout_revision,overpay_bound_sats,overpay_ceiling_sats,offer_decision,offer_decided_at) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,$8::text::numeric,$9,clock_timestamp()) ON CONFLICT(block_hash) DO UPDATE SET offer_observed_payout_revision=EXCLUDED.offer_observed_payout_revision,overpay_bound_sats=EXCLUDED.overpay_bound_sats,overpay_ceiling_sats=EXCLUDED.overpay_ceiling_sats,offer_decision=EXCLUDED.offer_decision,offer_decided_at=EXCLUDED.offer_decided_at")
        .bind(&candidate.block_hash)
        .bind(i64::try_from(candidate.found_block.block_height)?)
        .bind(candidate.payout_revision)
        .bind(i64::try_from(candidate.found_block.coinbase_value_sats)?)
        .bind(hex::encode(candidate.window.prior_balances_digest))
        .bind(observed_revision)
        .bind(bound_sats.map(|sats| sats.to_string()))
        .bind(ceiling_sats.to_string())
        .bind(decision)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Program, miner, as-issued prior (text), gross, on-chain, block height, and
/// whether the block already has a divergence record.
type CarryRecordRow = (String, String, String, i64, i64, i64, bool);

/// Record, in the transaction that is about to make a landed block's carry
/// rows count (its confirmation, or a reactivation), the debt those rows
/// create against the canonical balances at that moment. A divergent
/// confirmation writes or replaces its record and its overpaid accounts; a
/// re-confirmation that no longer diverges clears them. Returns the
/// divergence, for the caller's metrics after its commit, or `None` for a
/// block with no carry rows.
pub(super) async fn record_confirmation(
    tx: &mut Transaction<'_, Postgres>,
    block_hash: &str,
    candidate: Option<&Candidate>,
) -> Result<Option<LandingDivergence>> {
    // One read: the block's carry rows while they do not count yet, its
    // height, and whether it already has a record.
    let rows: Vec<CarryRecordRow> = sqlx::query_as(
        "SELECT encode(ledger.p2mr_program,'hex'),ledger.miner_id,ledger.prior_balance_sats::text,ledger.gross_amount_sats,ledger.onchain_amount_sats,block.block_height,EXISTS(SELECT 1 FROM qbit_prism_payout_divergences divergence WHERE divergence.block_hash=$1) FROM qbit_payout_carry_forward ledger JOIN qbit_pool_blocks block ON block.block_hash=ledger.block_hash WHERE ledger.block_hash=$1 AND ledger.maturity_state<>'reversed' AND block.chain_state IN ('prepared','inactive')",
    )
    .bind(block_hash)
    .fetch_all(&mut **tx)
    .await?;
    let Some(&(_, _, _, _, _, height, exists)) = rows.first() else {
        return Ok(None);
    };
    let current = read_prior_balances(tx).await?;
    let (divergence, digest) = tokio::task::spawn_blocking(move || {
        let rows = rows
            .into_iter()
            .map(|(program, miner_id, issued, gross, onchain, _, _)| {
                Ok(CarryRow {
                    p2mr_program_hex: program,
                    miner_id,
                    issued_prior_sats: issued.parse()?,
                    gross_sats: i128::from(gross),
                    onchain_sats: i128::from(onchain),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        anyhow::Ok((
            rows_divergence(rows, &current),
            qbit_prism::prior_balances_digest(&current),
        ))
    })
    .await??;
    if divergence.divergent_accounts == 0 && !exists {
        return Ok(Some(divergence));
    }
    sqlx::query("INSERT INTO qbit_prism_payout_divergences(block_hash,block_height,candidate_payout_revision,coinbase_value_sats,as_issued_prior_balances_sha256,confirmed_prior_balances_sha256,confirmed_at,divergent_accounts,overpay_sats,overpaid_debt_after_sats,pool_debt_after_sats) VALUES($1,$2,$3,$4,$5,$6,clock_timestamp(),$7,$8::text::numeric,$9::text::numeric,$10::text::numeric) ON CONFLICT(block_hash) DO UPDATE SET candidate_payout_revision=COALESCE(qbit_prism_payout_divergences.candidate_payout_revision,EXCLUDED.candidate_payout_revision),coinbase_value_sats=COALESCE(qbit_prism_payout_divergences.coinbase_value_sats,EXCLUDED.coinbase_value_sats),as_issued_prior_balances_sha256=COALESCE(qbit_prism_payout_divergences.as_issued_prior_balances_sha256,EXCLUDED.as_issued_prior_balances_sha256),confirmed_prior_balances_sha256=EXCLUDED.confirmed_prior_balances_sha256,confirmed_at=EXCLUDED.confirmed_at,divergent_accounts=EXCLUDED.divergent_accounts,overpay_sats=EXCLUDED.overpay_sats,overpaid_debt_after_sats=EXCLUDED.overpaid_debt_after_sats,pool_debt_after_sats=EXCLUDED.pool_debt_after_sats")
        .bind(block_hash)
        .bind(height)
        .bind(candidate.map(|candidate| candidate.payout_revision))
        .bind(candidate.map(|candidate| i64::try_from(candidate.found_block.coinbase_value_sats)).transpose()?)
        .bind(candidate.map(|candidate| hex::encode(candidate.window.prior_balances_digest)))
        .bind(hex::encode(digest))
        .bind(i32::try_from(divergence.divergent_accounts)?)
        .bind(divergence.overpay_sats.to_string())
        .bind(divergence.overpaid_debt_after_sats.to_string())
        .bind(divergence.pool_debt_after_sats.to_string())
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM qbit_prism_payout_divergence_accounts WHERE block_hash=$1")
        .bind(block_hash)
        .execute(&mut **tx)
        .await?;
    for account in &divergence.overpaid {
        sqlx::query("INSERT INTO qbit_prism_payout_divergence_accounts(block_hash,p2mr_program,miner_id,issued_prior_sats,current_prior_sats,gross_sats,onchain_sats,overpay_sats,debt_after_sats) VALUES($1,decode($2,'hex'),$3,$4::text::numeric,$5::text::numeric,$6::text::numeric,$7::text::numeric,$8::text::numeric,$9::text::numeric)")
            .bind(block_hash)
            .bind(&account.p2mr_program_hex)
            .bind(&account.miner_id)
            .bind(account.issued_prior_sats.to_string())
            .bind(account.current_prior_sats.to_string())
            .bind(account.gross_sats.to_string())
            .bind(account.onchain_sats.to_string())
            .bind(account.overpay_sats.to_string())
            .bind(account.debt_after_sats.to_string())
            .execute(&mut **tx)
            .await?;
    }
    Ok(Some(divergence))
}

/// Every account's debt, from the canonical balances in `tx`.
pub(super) async fn pool_debt(tx: &mut Transaction<'_, Postgres>) -> Result<u64> {
    let debt: String = sqlx::query_scalar(
        "SELECT COALESCE(-sum(balance_sats),0)::text FROM qbit_current_carry_forward_balances() WHERE balance_sats<0",
    )
    .fetch_one(&mut **tx)
    .await?;
    Ok(debt.parse()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn balance(program: &str, sats: i128) -> CarryForwardBalance {
        CarryForwardBalance {
            recipient_id: format!("r-{program}"),
            order_key: program.into(),
            p2mr_program_hex: program.into(),
            balance_sats: sats,
        }
    }

    fn account(program: &str, prior: i128, gross: u64, onchain: u64) -> PayoutPolicyAccount {
        PayoutPolicyAccount {
            account_type: PayoutPolicyAccountType::Miner,
            recipient_id: format!("r-{program}"),
            order_key: program.into(),
            p2mr_program_hex: program.into(),
            gross_amount_sats: gross,
            prior_balance_sats: prior,
            candidate_balance_sats: prior + i128::from(gross),
            onchain_amount_sats: onchain,
            settlement_fee_sats: 0,
            carry_forward_balance_sats: prior + i128::from(gross) - i128::from(onchain),
            action: qbit_prism::PayoutPolicyAction::Onchain,
        }
    }

    /// The review's sequence P, C1, C2 with a 5,000-sat carry on `aa` and a
    /// 0.8124 paydown: each confirmation's realized debt is within the
    /// block's positive as-issued float.
    #[test]
    fn the_bound_covers_each_realized_debt_in_a_one_stale_chain() {
        let after_p = [balance("aa", 938)];
        let c1_issued = [balance("aa", 5_000)];
        let c1 = landing_divergence(&[account("aa", 5_000, 1_000, 5_062)], &after_p);
        assert_eq!(c1.overpay_sats, 3_124);
        assert!(c1.overpay_sats <= overpay_bound(&c1_issued));
        let after_c1 = [balance("aa", -3_124)];
        let c2_issued = [balance("aa", 938)];
        let c2 = landing_divergence(&[account("aa", 938, 1_000, 1_762)], &after_c1);
        assert_eq!(c2.overpay_sats, 762);
        assert_eq!(c2.pool_debt_after_sats, 3_886);
        assert!(c2.overpay_sats <= overpay_bound(&c2_issued));
    }

    /// The bound is the positive float, per program: negative priors and
    /// other programs' surpluses never offset it.
    #[test]
    fn the_bound_is_the_positive_as_issued_float() {
        let issued = [
            balance("aa", 10_000),
            balance("AA", 500),
            balance("bb", -7_000),
            balance("cc", 0),
        ];
        assert_eq!(overpay_bound(&issued), 10_500);
        assert_eq!(overpay_bound(&[]), 0);
    }

    /// A landing on current balances creates no debt and diverges nowhere;
    /// pool fee accounts carry no balance.
    #[test]
    fn a_current_landing_records_no_overpay() {
        let mut fee = account("ff", 0, 0, 10);
        fee.account_type = PayoutPolicyAccountType::PoolFee;
        let divergence = landing_divergence(
            &[account("aa", 938, 1_000, 1_762), fee],
            &[balance("aa", 938), balance("bb", -5)],
        );
        assert_eq!(divergence.divergent_accounts, 0);
        assert_eq!(divergence.overpay_sats, 0);
        assert_eq!(divergence.pool_debt_after_sats, 5);
        assert!(divergence.overpaid.is_empty());
    }

    #[test]
    fn the_ceiling_is_basis_points_of_the_coinbase_rounded_down() {
        assert_eq!(overpay_ceiling_sats(5_000_000_000, 100), 50_000_000);
        assert_eq!(overpay_ceiling_sats(9_999, 1), 0);
        assert_eq!(overpay_ceiling_sats(5_000_000_000, 0), 0);
    }
}

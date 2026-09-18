//! Conservative delta acquisition; no retained input is publication authority.
use super::*;

/// Only moved out of the refresh loop's exclusively owned snapshot. Keep the
/// original cutoff and anchor: the last eligible row alone is not a cutoff.
pub(crate) struct RetainedShares {
    pub network: u128,
    pub anchor_ms: i64,
    pub cutoff: u64,
    pub shares: Vec<AcceptedShare>,
    pub leaf: Option<LeafWitness>,
}

/// Runtime-only acquisition evidence, never serialized or persisted.
pub(crate) type LeafWitness = (i64, String);

#[derive(Clone)]
pub(crate) struct SnapshotCapture {
    pub snapshot: Snapshot,
    pub leaf: Option<LeafWitness>,
}

impl std::ops::Deref for SnapshotCapture {
    type Target = Snapshot;
    fn deref(&self) -> &Snapshot {
        &self.snapshot
    }
}

impl RetainedShares {
    fn eligible(&self, network: u128, weight: u128, anchor: i64, cutoff: u64) -> bool {
        if self.network != network || self.anchor_ms > anchor || self.cutoff > cutoff {
            return false;
        }
        // Limit speculative work to one existing SQL page. Larger deltas take
        // the bounded newest-first full scan instead of retaining obsolete rows.
        if cutoff - self.cutoff > 4096 {
            return false;
        }
        let Some(first) = self.shares.first() else {
            return false;
        };
        if first.share_seq == 0 || self.shares.last().unwrap().share_seq != self.cutoff {
            return false;
        }
        if self.leaf.is_none() {
            return false;
        }
        let mut remaining = weight;
        for share in &self.shares {
            remaining = remaining.saturating_sub(share.share_difficulty);
        }
        remaining == 0
    }
}

/// A live single leaf covers every integer between the endpoints. Checking
/// endpoints in different leaves cannot rule out an interior partition hole.
/// pg_inherits is authoritative, including the first phase of concurrent
/// detach; its tuple incarnation also detects detach/reattach during the read.
pub(super) async fn leaf_witness(
    tx: &mut Transaction<'_, Postgres>,
    first: i64,
    last: i64,
    anchor: i64,
    expected_count: Option<i64>,
) -> Result<Option<LeafWitness>> {
    Ok(sqlx::query_as(
        "SELECT a.tableoid::bigint,i.xmin::text FROM qbit_share_ledger a \
         JOIN qbit_share_ledger b ON b.share_seq=$2 AND b.tableoid=a.tableoid \
         JOIN pg_inherits i ON i.inhrelid=a.tableoid \
           AND i.inhparent='qbit_share_ledger'::regclass AND NOT i.inhdetachpending \
         WHERE a.share_seq=$1 AND a.accepted AND b.accepted \
           AND a.accepted_at<=to_timestamp($3::double precision/1000) \
           AND a.job_issued_at<=to_timestamp($3::double precision/1000) \
           AND b.accepted_at<=to_timestamp($3::double precision/1000) \
           AND b.job_issued_at<=to_timestamp($3::double precision/1000) \
           AND ($4::bigint IS NULL OR $4=(SELECT count(*) FROM qbit_share_ledger s \
               WHERE s.share_seq BETWEEN $1 AND $2 AND s.accepted \
                 AND s.accepted_at<=to_timestamp($3::double precision/1000) \
                 AND s.job_issued_at<=to_timestamp($3::double precision/1000)))",
    )
    .bind(first)
    .bind(last)
    .bind(anchor)
    .bind(expected_count)
    .fetch_optional(&mut **tx)
    .await?)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn advance(
    tx: &mut Transaction<'_, Postgres>,
    prior: BlockingDrop<RetainedShares>,
    network: u128,
    weight: u128,
    anchor: i64,
    cutoff: i64,
    completion: &ReadAdmission,
) -> Result<Option<(BlockingDrop<Vec<AcceptedShare>>, LeafWitness)>> {
    let cutoff_u64 = u64::try_from(cutoff)?;
    let checked = prior
        .map_anyhow(move |prior| {
            Ok(prior
                .eligible(network, weight, anchor, cutoff_u64)
                .then_some(prior))
        })
        .await?;
    // Keep ownership under admission even before the first SQL await.
    let Some(prior) = checked.into_inner() else {
        return Ok(None);
    };
    let prior = completion.own(prior);
    let first = i64::try_from(prior.shares[0].share_seq)?;
    let Some(witness) = leaf_witness(tx, first, cutoff, anchor, None).await? else {
        prior.map_anyhow(|_| Ok(())).await?;
        return Ok(None);
    };
    if prior.leaf.as_ref() != Some(&witness) {
        prior.map_anyhow(|_| Ok(())).await?;
        return Ok(None);
    }
    let delta_len = usize::try_from(cutoff_u64 - prior.cutoff)?;
    let rows = if delta_len == 0 {
        Vec::new()
    } else {
        sqlx::query(&format!(
            "{SELECT_SHARE} WHERE {} AND share_seq>$1 AND share_seq<=$2 \
             ORDER BY share_seq LIMIT 4096",
            super::super::audit::anchored_eligibility_sql(3)
        ))
        .bind(i64::try_from(prior.cutoff)?)
        .bind(cutoff)
        .bind(anchor)
        .fetch_all(&mut **tx)
        .await?
    };
    let rows = completion.own(rows);
    let merged = rows
        .map_anyhow(move |rows| {
            let mut prior = prior.into_inner();
            let mut delta = Vec::with_capacity(rows.len());
            for row in rows {
                let share = share_from_row(&row)?;
                delta.push(share);
            }
            // Saturating subtraction matches the full reader, including zero
            // weights, a partial crossing row, and weights whose sum overflows.
            let mut remaining = weight;
            let mut delta_start = 0;
            for (index, share) in delta.iter().enumerate().rev() {
                remaining = remaining.saturating_sub(share.share_difficulty);
                if remaining == 0 {
                    delta_start = index;
                    break;
                }
            }
            let mut start = prior.shares.len();
            if remaining > 0 {
                for (index, share) in prior.shares.iter().enumerate().rev() {
                    remaining = remaining.saturating_sub(share.share_difficulty);
                    start = index;
                    if remaining == 0 {
                        break;
                    }
                }
            }
            // Trim before appending, so obsolete rows never inflate the vector's
            // capacity. Only the retained window and one decoded SQL page coexist.
            prior.shares.drain(..start);
            prior.shares.extend(delta.drain(delta_start..));
            // A high-difficulty delta can retire almost the entire old window.
            // Do not retain that old vector capacity across later refreshes.
            if prior.shares.capacity() > prior.shares.len().saturating_mul(2).max(4) {
                prior.shares.shrink_to_fit();
            }
            Ok(prior.shares)
        })
        .await?;
    // Same statement/snapshot checks both the live incarnation and complete
    // eligible membership after trimming. Immutability makes our retained rows
    // a subset; equal cardinality proves equality, even with sequence gaps or
    // retroactive INSERTs and newly eligible timestamps. This is intentionally
    // an O(window) metadata scan, not a claim of O(delta) database work.
    let first = i64::try_from(
        merged
            .first()
            .context("full retained suffix disappeared")?
            .share_seq,
    )?;
    let count = i64::try_from(merged.len())?;
    if leaf_witness(tx, first, cutoff, anchor, Some(count))
        .await?
        .as_ref()
        != Some(&witness)
    {
        merged.map_anyhow(|_| Ok(())).await?;
        return Ok(None);
    }
    Ok(Some((merged, witness)))
}

#[cfg(test)]
mod tests;

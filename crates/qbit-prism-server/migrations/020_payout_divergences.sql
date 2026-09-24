-- #478 block capture: evidence for every payout divergence. A block found on
-- work whose payout revision was superseded on the still-current tip is
-- offered only when its maximum possible overpay (the positive priors of its
-- own as-issued balances) is within PRISM_CAPTURE_OVERPAY_CEILING_BPS of its
-- coinbase value; the offer records that decision here, in the transaction
-- that reserved (or refused) the offer. Debt is realized when a landed
-- block's carry rows start to count, at its confirmation: every
-- confirmation whose as-issued prior balances differ from the canonical
-- balances at that moment records the debt it created here, in the
-- confirming transaction, with one row per account it overpaid. Additive: a
-- binary that does not know these tables never reads or writes them.
CREATE TABLE qbit_prism_payout_divergences (
    block_hash text PRIMARY KEY CHECK (block_hash ~ '^[0-9a-f]{64}$'),
    block_height bigint NOT NULL CHECK (block_height >= 0),
    -- Known from the candidate when the offer or the settlement recorded the
    -- row; NULL when only the chain reconciler saw the block.
    candidate_payout_revision bigint,
    coinbase_value_sats bigint CHECK (coinbase_value_sats >= 0),
    as_issued_prior_balances_sha256 text
        CHECK (as_issued_prior_balances_sha256 ~ '^[0-9a-f]{64}$'),
    -- The offer decision, when the offer made one: the revision observed,
    -- the bound (the as-issued positive priors) and the ceiling it was held
    -- to. 'disabled' is a refusal because capture is off (a ceiling of 0);
    -- it computes no bound.
    offer_observed_payout_revision bigint,
    overpay_bound_sats numeric CHECK (overpay_bound_sats >= 0),
    overpay_ceiling_sats numeric CHECK (overpay_ceiling_sats >= 0),
    offer_decision text CHECK (offer_decision IN ('offered', 'abandoned', 'disabled')),
    offer_decided_at timestamptz,
    -- The confirmation, when the block's rows started to count divergent:
    -- the digest of the canonical balances just before, the divergent
    -- accounts, the debt it created (per account, the increase in
    -- max(0, -balance)), what the accounts it overpaid owe after it, and
    -- what every account owes after it. A re-confirmation after a reorg
    -- replaces these.
    confirmed_prior_balances_sha256 text
        CHECK (confirmed_prior_balances_sha256 ~ '^[0-9a-f]{64}$'),
    confirmed_at timestamptz,
    divergent_accounts integer CHECK (divergent_accounts >= 0),
    overpay_sats numeric CHECK (overpay_sats >= 0),
    overpaid_debt_after_sats numeric CHECK (overpaid_debt_after_sats >= 0),
    pool_debt_after_sats numeric CHECK (pool_debt_after_sats >= 0),
    CHECK ((offer_decision IS NULL) = (offer_decided_at IS NULL)),
    CHECK ((offer_decision IS NULL) = (overpay_ceiling_sats IS NULL)),
    CHECK ((offer_decision IS NULL) = (offer_observed_payout_revision IS NULL)),
    CHECK ((offer_decision IN ('offered', 'abandoned')) = (overpay_bound_sats IS NOT NULL)),
    CHECK ((confirmed_at IS NULL) = (overpay_sats IS NULL)),
    CHECK ((confirmed_at IS NULL) = (confirmed_prior_balances_sha256 IS NULL)),
    CHECK ((confirmed_at IS NULL) = (divergent_accounts IS NULL)),
    CHECK ((confirmed_at IS NULL) = (overpaid_debt_after_sats IS NULL)),
    CHECK ((confirmed_at IS NULL) = (pool_debt_after_sats IS NULL)),
    CHECK (offer_decision IS NOT NULL OR confirmed_at IS NOT NULL)
);

CREATE TABLE qbit_prism_payout_divergence_accounts (
    block_hash text NOT NULL REFERENCES qbit_prism_payout_divergences(block_hash),
    p2mr_program bytea NOT NULL,
    miner_id text NOT NULL,
    issued_prior_sats numeric NOT NULL,
    current_prior_sats numeric NOT NULL,
    gross_sats numeric NOT NULL CHECK (gross_sats >= 0),
    onchain_sats numeric NOT NULL CHECK (onchain_sats >= 0),
    overpay_sats numeric NOT NULL CHECK (overpay_sats > 0),
    debt_after_sats numeric NOT NULL CHECK (debt_after_sats >= overpay_sats),
    PRIMARY KEY (block_hash, p2mr_program)
);

-- The integrity report's divergence line. A confirmation counts while its
-- block counts for balances (confirmed and not reversed); `overpay_sats` is
-- the debt those confirmations created, cumulatively. The debt fields are
-- read from the current canonical balances, whatever created the debt, and
-- fall below the created total as indebted miners earn gross again.
CREATE FUNCTION qbit_prism_payout_divergence_report()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    WITH effective AS (
        SELECT divergence.overpay_sats
        FROM qbit_prism_payout_divergences divergence
        JOIN qbit_pool_blocks block ON block.block_hash = divergence.block_hash
        WHERE divergence.confirmed_at IS NOT NULL
          AND divergence.divergent_accounts > 0
          AND block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
    ),
    debt AS (
        SELECT balance_sats
        FROM qbit_current_carry_forward_balances()
        WHERE balance_sats < 0
    )
    SELECT jsonb_build_object(
        'schema', 'qbit.prism.payout-divergence.v1',
        'divergent_landings', (SELECT count(*) FROM effective),
        'overpay_sats', (SELECT COALESCE(sum(overpay_sats), 0)::text FROM effective),
        'offers_abandoned_by_ceiling', (
            SELECT count(*) FROM qbit_prism_payout_divergences WHERE offer_decision = 'abandoned'
        ),
        'offers_refused_capture_off', (
            SELECT count(*) FROM qbit_prism_payout_divergences WHERE offer_decision = 'disabled'
        ),
        'debtor_count', (SELECT count(*) FROM debt),
        'debt_sats', (SELECT COALESCE(-sum(balance_sats), 0)::text FROM debt),
        'largest_debt_sats', (SELECT COALESCE(-min(balance_sats), 0)::text FROM debt)
    );
$$;

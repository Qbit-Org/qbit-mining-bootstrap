-- An unsigned reservation may become unusable before the wallet locks it.
-- Preserve its cleanup responsibility when selecting replacement funding;
-- even a spent or disconnected output can retain a persistent wallet lock.
CREATE TABLE IF NOT EXISTS qbit_prism_cpfp_retired_funding (
    funding_txid text NOT NULL,
    funding_vout integer NOT NULL CHECK (funding_vout >= 0),
    fanout_txid text NOT NULL REFERENCES qbit_ctv_fanout_artifacts(fanout_txid),
    funding_value_sats bigint NOT NULL CHECK (funding_value_sats > 0),
    wallet_name text NOT NULL,
    wallet_lock_released boolean NOT NULL DEFAULT false,
    retirement_reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (funding_txid, funding_vout)
);

CREATE INDEX IF NOT EXISTS qbit_prism_cpfp_retired_cleanup_idx
    ON qbit_prism_cpfp_retired_funding(fanout_txid, updated_at)
    WHERE NOT wallet_lock_released;

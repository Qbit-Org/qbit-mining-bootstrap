-- Rollup tables are optional: charts retain the raw-ledger fallback. All
-- remaining public read models require both the legacy and native schema.
SELECT NOT EXISTS (
    SELECT 1 FROM unnest(ARRAY[
        'qbit_share_ledger','qbit_pool_blocks','qbit_pool_audit_bundles',
        'qbit_pool_payout_entries','qbit_payout_carry_forward',
        'qbit_ctv_fanout_artifacts','qbit_ctv_fanout_sets',
        'qbit_prism_audit_snapshots'
    ]) AS required(name) WHERE to_regclass(name) IS NULL
) AND NOT EXISTS (
    SELECT 1 FROM (VALUES
        ('qbit_pool_blocks','audit_publication_sequence'),
        ('qbit_pool_blocks','inactive_since'),
        ('qbit_pool_audit_bundles','share_snapshot_sha256'),
        ('qbit_pool_audit_bundles','canonical_audit_bytes')
    ) AS required(relation,column_name)
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid=to_regclass(required.relation)
          AND attname=required.column_name AND NOT attisdropped
    )
);

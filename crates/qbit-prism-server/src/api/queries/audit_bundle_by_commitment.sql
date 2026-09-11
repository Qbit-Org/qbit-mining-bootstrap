-- Parameterized shared-database read model.

SELECT COALESCE(
    (
        SELECT json_build_object(
            'block_hash', bundle.block_hash,
            'block_height', block.block_height,
            'payout_manifest_sha256', block.payout_manifest_sha256,
            'audit_commitment_leaf_hex', $1,
            'audit_bundle_sha256', bundle.audit_bundle_sha256,
            'coinbase_tx_hex', bundle.coinbase_tx_hex,
            -- Imported canonical bytes supersede any inline copy; load only
            -- the representation the reader will serve.
            'audit_bundle', CASE
                WHEN bundle.share_snapshot_sha256 IS NULL
                 AND bundle.canonical_audit_bytes IS NOT NULL THEN NULL
                ELSE bundle.audit_bundle
            END,
            'body_uri', bundle.body_uri,
            'share_snapshot_sha256', bundle.share_snapshot_sha256,
            -- Read by the handler to skip the bytes query; never served.
            'has_canonical_audit_bytes', bundle.canonical_audit_bytes IS NOT NULL
        )
        FROM qbit_pool_audit_bundles bundle
        JOIN qbit_pool_blocks block ON block.block_hash = bundle.block_hash
        WHERE bundle.audit_commitment_leaves_hex ? $1
           OR bundle.witness_merkle_leaves_hex ? $1
           OR bundle.audit_bundle->'audit_commitment_leaves_hex' ? $1
           OR bundle.audit_bundle->'witness_merkle_leaves_hex' ? $1
        ORDER BY block.block_height DESC, bundle.created_at DESC, bundle.block_hash
        LIMIT 1
    ),
    'null'::json
);

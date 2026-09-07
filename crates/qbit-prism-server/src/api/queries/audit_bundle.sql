-- Parameterized shared-database read model.

SELECT COALESCE(
    (
        SELECT json_build_object(
            'block_hash', bundle.block_hash,
            'block_height', block.block_height,
            'payout_manifest_sha256', block.payout_manifest_sha256,
            'audit_bundle_sha256', bundle.audit_bundle_sha256,
            'coinbase_tx_hex', bundle.coinbase_tx_hex,
            'audit_bundle', bundle.audit_bundle,
            'body_uri', bundle.body_uri,
            'share_snapshot_sha256', bundle.share_snapshot_sha256
        )
        FROM qbit_pool_audit_bundles bundle
        JOIN qbit_pool_blocks block
          ON block.block_hash = bundle.block_hash
        WHERE bundle.block_hash = $1
    ),
    'null'::json
);

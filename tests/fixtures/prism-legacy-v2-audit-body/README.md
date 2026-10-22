# A 2.x audit-bundle.v2 body (#731)

These three files are a v2 audit body and its two share-segment slots, as 2.x.x
publishes them. Two `segment_range` parts cover shares 1–4 and 5–6. The
window proof's `share_parts_digest_hex` is 2.x's: sha256 over the parts in
insertion order. 3.x recomputes that digest with sorted keys and refuses the
body with "window proof share_parts_digest_hex mismatch" (#731), which is how
`import-audits` refuses such a body when its canonical sidecar is missing.

`tests/test_prism_legacy_parts_digest_check.py` reads them:

- `scripts/prism_legacy_parts_digest_check.py` must call the body
  `2x-order-only`, and its `digest_2x` must reproduce the stored digest;
- the real `qbit-prism-audit-canonicalize` must refuse the body, and accept it
  once the digest is recomputed by the checker's `digest_3x`, with canonical
  bytes whose sha256 is the body's `audit_bundle_sha256`;
- `scripts/prism_legacy_range_sidecars_sharded.sh` must turn copies of it into
  proven canonical sidecars.

## How it was made

The generator needs 2.x.x's `lab/` tree, so it isn't in this repository.

1. Build the bundle with this repository's `qbit-prism-build-audit-bundle
   --canonical-output`.
   - Input: `crates/qbit-prism/fixtures/power-law-accrual.prism-fixture.json`'s
     `found_block` and `shares`, with the prior balance `audit_cli.rs` passes
     (`miner-whale`, order key `01`, program `11`×32, 4,800 sats).
   - Signing seeds: `42`×32, and `43`×32 for the ledger key.
   - The canonical bundle is 10,999 bytes, with sha256
     `65b11e1b7e2025472fad2e4cd6b555eaba5eab2a4903e17179ba792d58780a4b`.
2. Publish it for block `00`×26 + `c8`×6 with 2.x.x's
   `AuditArtifactStore.prepare_external_audit_body`, at 2.x.x `50e208ca`, with
   `share_segment_size=4`.
   - 2.x wrote the body, both slots and its own canonical sidecar. The
     sidecar's decompressed bytes are the canonical bundle.
3. Make the body relocatable.
   - 2.x named each slot by its absolute path under the temporary store root.
     Each part's `body_uri` was rewritten to the production prefix,
     `/var/lib/qbit-mining-pool/prism/audit/<slot name>`.
   - `share_parts_digest_hex` was recomputed with 2.x's own
     `AuditArtifactStore.share_parts_digest_hex`.
   - The body was re-encoded with 2.x's `AuditArtifactStore.storage_json_bytes`.
     That's the encoder 2.x writes bodies with; re-encoding the unmodified body
     reproduces 2.x's bytes exactly.

Two runs produced identical files. 2.x's v2 writer only emits `segment_range`
parts. Inline parts occur only in `audit-body-ref.v1` bodies, which carry no
parts digest, so this fixture has none.

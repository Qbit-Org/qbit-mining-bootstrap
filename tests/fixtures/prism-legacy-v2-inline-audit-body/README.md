# A 2.x audit-bundle.v2 body with an inline part and non-ASCII workers (#731)

This file is a v2 audit body as 2.x.x's `AuditArtifactStore` encodes it. Its
window proof holds one `inline` part with all six shares, two of whose workers
are non-ASCII: `miner-mid-a.Bjørn-rig` (share 2) and `miner-small-b.rig-🦀`
(share 5, above the BMP). The body needs no slot.

2.x hashed the window proof's `share_parts_digest_hex` and encoded the body
with Python's `json.dumps`, which writes non-ASCII text as `\uXXXX` escapes
and a character above the BMP as a surrogate pair. 3.x recomputes the digest
with the same escaping since #731's fix.

`crates/qbit-prism/src/audit_body_ref.rs`'s `share_parts_digest_tests` read
it: 3.x must reproduce the stored parts digest, canonicalize the body to bytes
whose sha256 is its `audit_bundle_sha256`, and refuse the body once an inline
share's worker name changes.

## How it was made

The generator needs 2.x.x's `lab/` tree, so it isn't in this repository.

1. Build the bundle with this repository's `qbit-prism-build-audit-bundle
   --canonical-output`.
   - Input: `crates/qbit-prism/fixtures/power-law-accrual.prism-fixture.json`'s
     `found_block` and `shares`, with `.Bjørn-rig` appended to share 2's
     `miner_id` and `.rig-🦀` to share 5's, and the prior balance
     `audit_cli.rs` passes (`miner-whale`, order key `01`, program `11`×32,
     4,800 sats).
   - Signing seeds: `42`×32, and `43`×32 for the ledger key.
   - The canonical bundle is 11,101 bytes, with sha256
     `c328186054819ca745a8dfbd3bc2ab6e9e1a73d88adc0417356781c28b06cbd5`.
2. Encode the v2 body for block `00`×26 + `c9`×6 with 2.x.x's
   `AuditArtifactStore` at 2.x.x `50e208ca`, `share_segment_size=8`.
   - 2.x's v2 writer cuts only `segment_range` parts, and its own v2 reader
     refuses inline ones, so no 2.x v2 body in production has one. The store's
     `audit_share_range_parts` was therefore replaced by its
     `audit_share_parts`, the body-ref part iterator, which inlines a partial
     slot: the window, shares 1–6 of slot 1–8, became one inline part.
   - Everything else is 2.x's: `audit_bundle_v2` built the body and its window
     proof, `share_parts_digest_hex` hashed the parts, and the body's bytes are
     `_storage_json_chunks` of it, which `storage_json_bytes` of the parsed
     body reproduces exactly.

Two runs produced identical files.

# PRISM Ledger Operations Contract

This contract is the public operating model for the qbit PRISM share ledger.
PRISM means Payouts, Rewards, and Integrity Settlement Manifest.
It is intentionally narrower than a full production deployment guide: it
defines the invariants the implementation and regtest evidence rely on, and it
names the hardening layers that can be added without changing reward semantics.

## Canonical Write Path

Accepted shares must enter exactly one ordered log: `qbit_share_ledger`.
Stratum frontends may scale horizontally, but they do not insert independent
share sequences. The supported topology is:

1. Stratum frontend validates miner identity, share shape, job id, and target.
2. Frontend submits the accepted share to a durable queue or equivalent
   at-least-once delivery boundary.
3. One logical ledger writer owns the Postgres writer lease and inserts shares
   into `qbit_share_ledger`.
4. PRISM's TIDES-style reward windows, audit exports, payout policy, and reorg
   reversal read from that same canonical log.

The direct regtest coordinator runs the frontend and writer in one process, but
the database contract is already the same: every accepted share receives one
monotonic `share_seq`, and all reward windows are derived from that ordering.

## Writer Lease and Replay

The writer lease is stored in `qbit_ledger_writer_lease`. A writer is identified
by `(writer_id, writer_epoch, writer_session_token)`. A process may refresh only
the exact session token it acquired; another process with the same writer id and
epoch is still fenced until the existing lease expires. During startup, a
replacement process with the same writer id and epoch waits and retries until
that predecessor lease expires, then acquires a fresh session token. A different
writer id or epoch is treated as a conflicting active writer and fails fast.

`share_id` is globally unique. Queue replay is therefore idempotent: replaying a
share that was already committed must fail without consuming a new sequence
number. After failover, the replacement writer resumes at the next database
sequence value and stale writers are rejected before insert.

Phase 1 is single-writer, not active-active. High availability is achieved by
frontends buffering accepted shares durably and a standby writer taking the
lease after expiry. Active-active insertion would create ambiguous ordering and
is outside the accepted PRISM contract.

Production deployments must use the Postgres-backed ledger. The in-memory ledger
exists only for local/regtest proof runs and requires an explicit
`PRISM_ALLOW_MEMORY_LEDGER=1` opt-in.

## Reward Query Semantics

`qbit_prism_window(anchor_job_issued_at, window_weight)` returns accepted shares
in descending `share_seq` order until the requested weight is filled. This is
the TIDES-style reward-window primitive inside PRISM. The oldest included share
is partially counted when needed. Eligibility requires both:

- `job_issued_at <= anchor_job_issued_at`
- `accepted_at <= anchor_job_issued_at`

That second condition freezes the block view and prevents an old-job share that
arrives after the found-block anchor from entering the published payout split.

`qbit_audit_share_window(anchor_job_issued_at, network_difficulty)` is the
public audit wrapper. It fixes the TIDES-style window multiplier at 8x network
difficulty and returns the counted difficulty for every included share.

`qbit_shares_since_template_height(min_template_height)` supports operational
replay and frontend recovery. It returns accepted shares at or above the
template height in ascending `share_seq` order and excludes rejected shares.

## Payout and Reorg State

Accepted pool blocks are persisted in `qbit_pool_blocks`, with payout rows in
`qbit_pool_payout_entries`, carried balances in `qbit_payout_carry_forward`, and
audit bundles in `qbit_pool_audit_bundles`. The direct coordinator verifies and
persists the deterministic candidate rows before calling `submitblock`; if qbitd
rejects that candidate before it becomes the active tip, the coordinator marks
the block `chain_state='rejected'` and reverses the pre-persisted immature
payout rows.

Immature disconnects are first quarantined as `chain_state='inactive'`, which
removes their carry-forward balances from current owed totals without mutating
historical shares or payout rows. If the block returns to the active chain, the
coordinator reactivates it. The terminal reversal path is the fenced ledger
wrapper `PsqlShareLedger.reverse_immature_block(...)`, backed by
`qbit_reverse_immature_pool_block(...)`; it marks the block, payout entries, and
carried balances as reversed. Mature rows, and rows already height-mature at the
supplied active tip, must not be reversed by that path. qbit coinbase maturity
is 1000 blocks, so operators must not mark pool payouts mature before
`block_height + 1000`.

## CTV Fanout Artifact Repair

Schema initialization is also the idempotent repair path for deployed PRISM
databases. In particular, it drops the old `NOT NULL` constraint from
`qbit_ctv_fanout_artifacts.anchor_vout` so fee-bearing, anchorless CTV fanouts
can persist with `anchor_vout = NULL`.

If a block was mined while the old constraint was still present, backfill the
missing fanout artifact rows from the persisted audit bundle or a local
`prism-live-audit-bundle-*.json` file:

```bash
PRISM_DATABASE_URL='postgres://...' \
python3 -m lab.prism.backfill_ctv_fanouts --db-block-height 21883
```

The repair tool also accepts `--db-block-hash <hash>` or local JSON paths. For a
local candidate/final audit bundle whose filename does not include the block
hash, pass `--path-block-hash <hash>`. The tool runs schema init by default
before backfilling and then calls the same fenced `persist_ctv_fanout_manifest_set`
path as the coordinator. Stop the active coordinator or otherwise ensure the
repair process can acquire the ledger writer lease before running a backfill.

## Compaction and Archive Contract

The ledger is append-only for reward correctness. Compaction is allowed only as
an archive-first operation, and only after proving it cannot change any future
window, audit, maturity, or reorg answer.

Before deleting any hot rows, an operator must:

1. Export the candidate `share_seq` prefix to durable archive storage.
2. Record row count, first and last sequence, and a cryptographic hash of the
   exported rows in canonical order.
3. Prove no unresolved pool block, audit bundle, immature payout, or future 8x
   PRISM reward window can reference the candidate rows.
4. Re-run representative `qbit_prism_window`,
   `qbit_audit_share_window`, and `qbit_shares_since_template_height` queries
   before and after the dry run and verify identical results.

No public harness currently deletes ledger rows. Until an archive proof exists,
production deployments should retain the full canonical share log.

For operator disk planning and permanent-vs-ephemeral data categories, see
[`docs/prism-storage-sizing.md`](prism-storage-sizing.md).

## Throughput Evidence

`make test-prism-postgres-throughput` is an opt-in capacity harness. It creates
a temporary Postgres container, bulk-inserts synthetic accepted shares through
the canonical schema, records observed shares/sec, and stores
`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` output for the audit-window query.

Useful environment variables:

- `QBIT_PRISM_THROUGHPUT_SHARES`: number of synthetic rows to insert.
- `QBIT_PRISM_MIN_SHARES_PER_SEC`: optional failing threshold.
- `QBIT_PRISM_THROUGHPUT_REPORT`: JSON report path.

The throughput harness measures schema/query capacity. It is separate from the
lab `psql` adapter, which favors zero Python dependencies and shells out per
operation for portability in regtest.

It does not simulate live Stratum miner swarms, reconnect storms, malformed
client messages, or stale-share bursts across job changes. Those remain separate
operator-load concerns if production needs coverage beyond ledger/query
capacity.

## Operator Readiness

`make prism-self-check` is the PRISM operator readiness probe. It resolves the
same Compose environment as `make up-prism-pool`, then emits PASS/WARN/FAIL
rows for:

- qbit RPC reachability, chain identity, IBD state, and peer count.
- PRISM coordinator `/healthz` from inside the coordinator container.
- miner-facing Stratum TCP reachability.
- Postgres readiness for the canonical ledger.
- PRISM signing/key environment and forbidden production bypass flags.
- audit/archive path writability.
- basic mining configuration such as share difficulty, vardiff bounds, pool
  fee configuration, and minimum ready miners.

The command is safe to run repeatedly. It exits non-zero when any FAIL row is
present. Use `python3 scripts/prism-self-check.py --skip-live` to validate only
static configuration before the profile is running.

For operator runs, `make up-prism-pool` requires Postgres plus
`PRISM_MANIFEST_SIGNING_SEED_HEX`,
`PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX`, and the trusted
`PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX`. The two seed values are 32-byte hex
Ed25519 signing seeds. The ledger public key is the verifying key derived from
the ledger attestation seed and must be distributed through an operator-trusted
channel, not copied from the bundle being verified. Keep
`PRISM_ALLOW_MEMORY_LEDGER`, `PRISM_ALLOW_TEST_SIGNING_SEEDS`,
`PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY`, and
`PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN` disabled outside local tests.

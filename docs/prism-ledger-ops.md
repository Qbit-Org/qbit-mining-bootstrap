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
2. Frontend submits the accepted share to the bounded group-commit writer and
   waits for Postgres to commit it.
3. One logical ledger writer owns the Postgres writer lease and inserts shares
   into `qbit_share_ledger`.
4. PRISM's TIDES-style reward windows, audit exports, payout policy, and reorg
   reversal read from that same canonical log.

The coordinator runs the frontend and writer in one process. Every accepted
share receives one monotonic `share_seq`, and all reward windows are derived
from that ordering. Stratum success is sent only after the transaction commits;
worker counters and vardiff accounting advance at the same boundary. A full
queue, commit error, or commit timeout returns no success. Miners may retry the
same submission safely.

The writer batches up to `PRISM_SHARE_COMMIT_BATCH_SIZE` shares for at most
`PRISM_SHARE_COMMIT_LINGER_MILLISECONDS` before committing. The defaults are 64
shares and 5 ms. `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` bounds queue admission.
Once admitted, a client waits for a definite commit outcome; the watchdog
restarts a wedged writer instead of returning an ambiguous timeout. Tune the linger against measured ACK
latency, but do not weaken Postgres durability settings to reduce it. A batch
flushes immediately when it contains a block candidate so the normal linger
does not consume that candidate's tip-race budget.

## Writer Lease and Replay

The writer lease is stored in `qbit_ledger_writer_lease`. A writer is identified
by `(writer_id, writer_epoch, writer_session_token)`. A process may refresh only
the exact session token it acquired; another process with the same writer id and
epoch is still fenced until the existing lease expires. During startup, a
replacement process with the same writer id and epoch waits and retries until
that predecessor lease expires, then acquires a fresh session token. A different
writer id or epoch is treated as a conflicting active writer and fails fast.

`share_id` is globally unique. Replaying an exact share payload is idempotent
and returns its original sequence without inserting another row. Reusing the ID
with any different payout, difficulty, job, timestamp, nonce, or credit policy
fails the complete batch. After failover, the replacement writer resumes at the
next database sequence value and stale writers are rejected before insert.

The ledger is single-writer, not active-active. A replacement writer takes the
lease after expiry. Active-active insertion would create ambiguous ordering and
is outside the accepted PRISM contract.

## Block Candidate Outbox

A block-worthy share transaction also inserts an immutable intent into
`qbit_block_candidate_outbox`. The intent contains the complete block, template
context, reward inputs, and extranonce fields required to finish audit and
submission. The share and intent become visible atomically before Stratum
success.

The in-memory candidate queue is only a bounded wakeup path. Queue saturation
coalesces wakeups; it cannot delete an outbox row. Before opening Stratum
listeners and whenever the queue drains, the coordinator replays pending rows.
Successful submissions become `submitted`; candidates that definitively lose
their tip race or fail validation become `abandoned`. If the process exits
after `submitblock` but before finalizing the row, restart recognizes the
candidate as the active tip and completes the idempotent confirmation path.
Transient RPC, audit, and ledger outcomes remain pending and retry with an
exponential delay starting at 250 milliseconds and capped at 30 seconds. They
do not increment terminal abandonment counters. Replay carries the database
row's block hash separately from candidate JSON, so malformed payloads can be
quarantined by their authoritative outbox key instead of replaying forever.

When a network-valid hash is below a listener's advertised share target, the
coordinator first stores a candidate-only intent, submits it synchronously, and
links share credit only if the block lands. This closes the submit-to-credit
crash window without crediting a below-target hash that loses its tip race.
Terminal outbox rows retain the intent digest but clear the large block/template
body, bounding permanent outbox storage while preserving exact-replay checks.

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

Accepted rows may carry a nullable `credit_policy`. Normal shares leave it
empty; `stale-grace` marks a prior-tip share credited by the coordinator's short
stale-grace policy. Reward-window queries still count these rows because they
are accepted shares, while audits can distinguish them from normal current-tip
shares. Audit bundles containing a credited row use
`qbit.prism.audit-bundle.v1.1`; external auditors must upgrade before operators
enable stale-grace crediting.

Deployments that run with `PRISM_POSTGRES_INIT_SCHEMA=0` must apply
`crates/qbit-prism/sql/001_share_ledger.sql` before starting upgraded
coordinators. Otherwise share inserts will fail because the `credit_policy`
column and updated window function signatures are missing.

`qbit_shares_since_template_height(min_template_height)` supports operational
replay and frontend recovery. It returns accepted shares at or above the
template height in ascending `share_seq` order and excludes rejected shares.

## Payout and Reorg State

Accepted pool blocks are persisted in `qbit_pool_blocks`, with payout rows in
`qbit_pool_payout_entries`, carried balances in `qbit_payout_carry_forward`, and
audit bundles in `qbit_pool_audit_bundles`. The direct coordinator durably
persists the compact candidate intent before calling `submitblock`, then builds,
verifies, and persists the full audit and payout state after the block becomes
active. A definitive pre-acceptance rejection terminalizes the outbox row and
creates no prepared payout state.

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

The same schema init path also adds bounded broadcast-attempt summaries to
`qbit_ctv_fanout_artifacts`. Operators can tune retained detail rows with
`PRISM_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT`; the summary columns retain total
attempt count, latest package/result/error context, and per-status counts after
old detail rows are no longer retained.

If a block was mined while the old constraint was still present, backfill the
missing fanout artifact rows from the persisted audit bundle, a local
`prism-live-audit-bundle-*.json` envelope, or a local audit body file:

```bash
PRISM_DATABASE_URL='postgres://...' \
python3 -m lab.prism.backfill_ctv_fanouts --db-block-height 21883
```

The repair tool also accepts `--db-block-hash <hash>` or local JSON paths. Local
paths may be full v1 bundles, live envelopes, legacy compact audit body refs,
or `qbit.prism.audit-bundle.v2` proof bodies; the tool follows envelope
`body_uri` pointers and reads `bundle_without_shares` from compact bodies
because CTV backfill does not need share rows. For a local candidate/final
audit bundle whose filename does not include the block hash, pass
`--path-block-hash <hash>`. The tool runs schema init by default before
backfilling and then calls the same fenced
`persist_ctv_fanout_manifest_set` path as the coordinator. Stop the active
coordinator or otherwise ensure the repair process can acquire the ledger
writer lease before running a backfill.

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

Production capacity qualification and its versioned evidence contract are
documented in [PRISM production capacity qualification](prism-capacity-readiness.md).

`make prism-self-check` is the PRISM operator readiness probe. It resolves the
same Compose environment as `make up-prism-pool`, then emits PASS/WARN/FAIL
rows for:

- qbit RPC reachability, chain identity, IBD state, and peer count.
- the configured genesis hash against `getblockhash 0` when
  `QBIT_EXPECTED_GENESIS_HASH` is set.
- PRISM coordinator `/healthz` from inside the coordinator container.
- miner-facing Stratum TCP reachability.
- Postgres readiness for the canonical ledger.
- PRISM signing/key environment and forbidden production bypass flags.
- audit/archive path writability.
- basic mining configuration such as share difficulty, vardiff bounds, pool
  fee configuration, CTV fanout fee sourcing, and minimum ready miners.

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

Mainnet configuration is always treated as production configuration, even when
the separate production toggle is omitted. It must select the chain explicitly
with `QBIT_CHAIN_FLAG=-chain=main` and pin the final release genesis hash in
`QBIT_EXPECTED_GENESIS_HASH`. The live readiness probe normalizes the configured
`mainnet` name to the `main` name returned by qbit RPC, then verifies height zero
against the pin.

Production builds using the git source provider must set `QBIT_GIT_COMMIT` to a
full 40-character object ID. The environment doctor verifies that the resolved
checkout is at that exact commit instead of trusting a mutable branch or tag.
Production also requires `PRISM_STRATUM_STALE_GRACE_SECONDS=0`; stale-credit
grace should be enabled only after the deployed verifier and accounting release
have an explicit compatibility proof for it.

The parent-chain selector is checked independently: `BITCOIN_CHAIN` and
`BITCOIN_CHAIN_FLAG` must be an exact pair, including `mainnet` with
`-chain=main`. When a production configuration selects a non-regtest parent for
AuxPoW, both `QBIT_MINER_ADDRESS` and `BITCOIN_MINER_ADDRESS` must be explicit;
automatic wallet-derived payout addresses are rejected.

Mainnet CTV settlement requires an operator-reviewed positive
`PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT`. On non-mainnet networks,
if it is omitted, the live readiness probe requires `estimatesmartfee` to return
a positive rate before the pool is considered ready. A new chain, or a chain
producing only empty blocks, does not have the confirmed transaction history
needed for empirical fee estimation. A wallet fallback fee does not populate
that history and is not a substitute for the explicit CTV fanout rate.

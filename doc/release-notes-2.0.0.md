# qbit-mining-bootstrap 2.0.0 Release Notes

Release date: pending

This is the first 2.x release. Changes temporarily labelled 2.0.1 and 2.0.2
during development are included here; those versions were not tagged or
published as GitHub releases.

## Highlights

- Separates the PRISM public dashboard API from the mining coordinator into
  the `prism-public-api` service, with a Postgres hot standby for public reads,
  bounded stale responses during outages, and independent readiness checks.
- Completes the PRISM coordinator decomposition into owners for share ingest,
  payout state, job delivery, block landing, audit artifacts, vardiff, and
  observability, backed by deterministic concurrency and landing harnesses.
- Hardens accepted-block accounting under dense block cadence: definitive node
  acceptances have a dedicated accounting lane, obsolete candidates collapse
  before replay, stuck accepted-parent transitions are retried in-process,
  and accepted-preview publication and cleanup backlogs are bounded.
- Bounds payout-window work and memory across database decoding, serialization,
  builder transport, and Rust paging. An opt-in Rust daemon pipeline handles
  window folding, canonical digests, and incremental advances, with Python
  fallback and a differential parity gate against the real daemon.
- Preserves converged worker difficulty across reconnects and, with Postgres,
  coordinator restarts; accelerates initial vardiff convergence and restores
  the configured prior-tip share grace window on mainnet.
- Adds public block markers, reorg visibility, network hashrate, dual raw and
  smoothed credited-hashrate series, and incremental historical rollups.
- Persists canonical audit-bundle bytes and durable audit publication order,
  improves writer-lease behavior under contention, and adds heap, allocator,
  component-cardinality, and lease-monitor diagnostics.
- Bounds candidate persistence, startup replay, and canonical audit finalization
  with chunked storage and file-backed views; adds offline recovery of
  accepted pending blocks.
- Resolves CKPool version masks after node readiness and refuses dynamic-mode
  startup when no live block template can be obtained.
- Moves the Python service images and CI to Python 3.14 and splits CI into
  independent lint, Python, Postgres, and Rust checks.

## Mainnet Compatibility

This release continues to pair with qbit `v1.0.0` at
`7ebcddb622d6e639041f005a189b048ec2a221fe`. The checked-in qbit source pin,
Bitcoin Core 30.2 release pins, and AuxPoW chain IDs are unchanged:

- Mainnet: `47`
- Public testnet4: `31430`

The qbit mainnet genesis hash remains:

`0000000000004d60aa5d46013991d0a0e2995d89ee98e53068ae196d763e79f2`

The 2.0.0 version identifies this bootstrap and pool software release. The
public API remains under `/public/v1`; individual opt-in response schemas
have their own version tags.

## Upgrade Notes

### Public API service and replica cutover

- Route `/public/v1` traffic to `prism-public-api`, on port `3342` by default.
  The coordinator's audit HTTP listener now returns 404 for these routes.
  Keep each process's `/healthz` and `/metrics`, and the coordinator's audit
  and operator endpoints, private.
- The shipped Compose profile adds `prism-postgres-replica` and reads through
  `PRISM_PUBLIC_DATABASE_URL`. Compose defaults
  `PRISM_PUBLIC_REPLICA_MODE=require`; provision the standby before cutover.
  Leave the public DSN empty to derive the built-in standby URL from the
  configured Postgres credentials, or set an explicit URL for a separate
  standby. The coordinator keeps its primary database DSN. Direct launches
  outside Compose default replica enforcement to `off`.
- The public psql backend also uses the public DSN. If a wrapper or custom
  connection arguments are needed, set `PRISM_PUBLIC_PSQL_COMMAND` separately;
  the coordinator's `PRISM_POSTGRES_PSQL_COMMAND` is not inherited by the
  public service.
- Production needs an explicit `PRISM_POSTGRES_REPLICA_DATA_SOURCE` directory
  and read-only access to the shared audit files from the public API service.
  Release-provenance checks require the replica path to be absolute and
  distinct from other enabled state sources before contacting Docker.
  Use a dedicated replication role and monitor the physical replication slot
  and retained WAL. The public standby is a read tier, not an automated
  failover target. Follow
  [the replica runbook](../docs/prism-postgres-replica.md) for provisioning,
  the non-default database-user caveat, lag monitoring, and slot retirement.
- Replica freshness enforcement bounds replication-stream heartbeat age,
  with a default limit of 60 seconds; replay lag is reported separately.
  Outage responses may serve eligible cached data only within the documented
  staleness bounds, after which the service refuses the read with 503.

### Ledger migration and audit history

- Back up the ledger and stop the old coordinator before applying
  `crates/qbit-prism/sql/001_share_ledger.sql`, followed by
  `crates/qbit-prism/sql/002_candidate_bodies.sql`. Both are required, even
  when configured to write legacy candidate bodies. The coordinator and
  offline recovery command refuse a database without the second migration.
  The first migration runs atomically, repairs partially seeded carry-forward
  summaries, adds durable worker-difficulty state, and assigns
  `audit_publication_sequence` to historical confirmed and inactive blocks.
- The migration takes exclusive table locks and builds a unique index
  non-concurrently. Schedule a maintenance window that allows interruption
  of reads as well as writes. With `PRISM_POSTGRES_INIT_SCHEMA=1`, the new
  coordinator applies the schema before opening listeners. For explicit
  application, follow the fail-fast transaction requirements in
  [the ledger operations guide](../docs/prism-ledger-ops.md) and apply the
  candidate-body migration next, as described in
  [the candidate storage guide](../docs/prism-candidate-storage.md).
- New audit bundles are stored and served as their exact canonical bytes.
  Historical bundles retain the legacy reconstructed response until verified
  backfill publishes the canonical artifact. Run
  `python3 -m lab.prism.backfill_audit_bundle_canonical --dry-run` to inspect
  history; stop the coordinator and release its writer lease before publish
  mode with `--no-init-schema`. Review failure rows as well as checkpoints.
  See [settlement artifacts](../docs/public-dashboard-api/README.md#settlement-artifacts)
  for the complete backfill and replica-visibility procedure.
- Use the documented ledger rollback procedure if reverting the software.
  The targeted audit-publication revert SQL is for a coordinated schema
  rollback with all new-version writers stopped. Candidate-format rollback
  must also follow the compatibility restrictions below.

### Candidate storage and offline recovery

- `PRISM_CANDIDATE_STORAGE_VERSION` defaults to `2`. Candidate bodies use
  immutable 256 KiB chunks and bounded replay headers; existing version 1
  JSONB candidates remain readable. Candidate identity, timestamp normalization,
  atomic share/outbox publication, and writer-session fencing are preserved.
- Canonical audit finalization uses replayable file-backed views and streamed
  publication. Replay membership uses an exact PostgreSQL subset check through
  bounded COPY writes. Spools, temporary tables, indexes, and candidate body
  references have explicit retirement paths.
- `PRISM_BLOCK_REPLAY_PAGE_SIZE` accepts 1–1,024 descriptors. Smaller pages
  increase database/RPC round trips; a row-count limit alone does not bound
  the bytes in one candidate. Candidate spool reservations default to 4 GiB
  per ledger instance; an empty `PRISM_CANDIDATE_SPOOL_DIR` selects the system
  temporary directory. Provision and monitor that filesystem separately.
- Use `python3 -m lab.prism.recover_pending_blocks --help` for the offline
  accepted-pending recovery command. Follow
  [the ledger operations guide](../docs/prism-ledger-ops.md) for stopping the
  writer, acquiring its lease, and reviewing recovery results.
- Qualify a compatible reader before enabling new-format writes. Keep the
  additive schema during rollback. Once a new-format pending candidate exists,
  an older reader that cannot hydrate it is unsafe. Use a compatible reader
  with `PRISM_CANDIDATE_STORAGE_VERSION=1` to stop new-format writes, or a
  separately verified drain/conversion procedure. Setting `1` does not remove
  the migration requirement. Follow
  [the candidate storage guide](../docs/prism-candidate-storage.md) for the
  complete schema, capacity, rollout, and rollback procedure.

### Configuration and runtime behavior

- No existing assigned `.env.example` key was removed, renamed, or given a
  different value since 1.1.0. There are new service, replica, vardiff,
  database-session, and diagnostic settings; deployment behavior still
  changes as described above. Review your deployment environment alongside
  the new example and [mainnet runbook](../docs/mainnet-deployment.md).
- Mainnet no longer forces `PRISM_STRATUM_STALE_GRACE_SECONDS=0`. The sample
  and code default is 3 seconds: qualifying prior-tip shares are credited
  during this window but never submitted as block candidates. An explicit
  zero in an existing deployment remains zero until the operator changes it.
- Difficulty resume is enabled by default, retains up to 8,192 worker entries,
  and has a 900-second TTL. It is keyed by listener and exact Stratum username;
  give heterogeneous rigs distinct worker names. Postgres-backed deployments
  can preload recent converged values after restart. Resume is bounded
  assistance, not a guarantee that every active worker survives an abrupt
  exit with a retained difficulty.
- `PRISM_WINDOW_PIPELINE_RUST` defaults to `0`. When enabled with the daemon
  transport, unsupported integer ranges and daemon anomalies fall back to the
  Python pipeline for that materialization. Qualify a deployment before
  changing this switch; the real-daemon parity gate is part of CI.
- CKPool dynamic version-mask resolution runs after node readiness, retries
  transient template failures, and aborts startup if no template is available.
  A successful template without the field retains the configured fallback;
  an explicit zero mask remains zero. `CKPOOL_VERSION_MASK_MODE=static` is the
  explicit option for authorized prelaunch without template availability.
- Rebuild service images for Python 3.14. The PRISM image sets
  `MALLOC_ARENA_MAX=2`; allocator telemetry defaults on, while heap census,
  tracemalloc, and malloc-trim controls default off. The optional
  `PRISM_PYTHON_SWITCH_INTERVAL_SECONDS` override remains unset by default.
  Follow the capacity and allocator runbooks before tuning these controls.
- Update consumers of removed `qbit_prism_shares_per_second`
  telemetry to rates derived from the monotonic share counters. Review
  coordinator lock, accepted-preview, lease-monitor, and memory metrics with
  the [overload alert specification](../docs/prism-overload-alerts.md).
  That document is a specification; this repository does not install live
  alert rules.
- Dashboard clients can opt into dual-rate series with `view=both`, reorg
  block history with `chain_state=all` or `reversed`, and the new
  `/public/v1/block-markers` endpoint. Unsupported range/bucket combinations
  now return 400; use the allowed vocabulary in the
  [public API contract](../docs/public-dashboard-api/README.md).

## Changes Since v1.1.0

This release includes the `2.x.x` changes after `v1.1.0` through
`504846c` (#258), along with release-promotion fixes in #246.

### Coordinator ownership and concurrency validation

- Decompose coordinator core owners, audit artifacts, block candidate
  submission, share submission, vardiff, block finalization, and observability;
  shard tests by domain and document the completed refactor and review stack.
- Add deterministic writer-lease and block-landing concurrency harnesses
  (#139, #148), concurrency instruments and classification fixes (#170),
  candidate-storm characterization (#192), and a per-row storm oracle (#194).
- Apply the integrated PRISM core, infrastructure, and vardiff stability
  review fixes (#141).

### Block landing, accounting, and writer leases

- Preserve the audit envelope when two found blocks land within one
  confirmation/publication window (#136), distinguish confirmation replay
  from fresh confirmation (#146), and validate fresh confirmations against
  the current-balance summary (#210).
- Bound startup candidate enumeration and sweep stranded prepared blocks
  (#142); add fenced bulk abandonment (#195), collapse superseded candidates
  before replay (#196), and bound collapsed-candidate cleanup retries (#222).
- Give definitive node acceptances their own accounting lane, skip provably
  stale candidates at dequeue, and measure acceptance-to-preview latency
  (the three follow-up commits referencing #181). Bound preview publication
  latency during dense accepted-tip cadence (#230).
- Re-drive stuck accepted-parent payout transitions in-process (#205), classify
  their preview backpressure as coordination-blocked (#190), and probe each
  ancestor height once when selecting a transition (#193).
- Keep replay reads out of the writer convoy (#213), scope append invalidation
  per anchor (#151), and route the landing tail through the named submit port
  (#172).
- Bound writer-lease acquisition and guard Postgres sessions (#137), enforce
  read-only sessions and name lease races (#168), tolerate heartbeat
  verification tail latency (#214), and attribute lease-monitor stalls with
  explicit lateness responses (#233).
- Cap escalated landing deadlines below the watchdog tolerance (#135).
- Allow one-candidate startup replay pages without truncating enumeration
  (#256), and add offline recovery for accepted pending blocks (#259).
- Scope pending-fanout broadcast attempts to their own fanout (#292).

### Payout windows, audit artifacts, and memory

- Make schema applies atomic and repair a partially seeded carry-forward
  summary (#155).
- Serve content-addressed artifacts as their exact canonical bytes (#154)
  and persist byte-exact canonical audit bundles with verified historical
  backfill tooling (#173).
- Stop serializing a full block for every accepted share (#162), add a
  differential window parity oracle (#165), move folding, digests, and
  incremental advance into the persistent builder (#166), and gate parity
  against the real Rust daemon (#178).
- Hold payout snapshot weight within a band to avoid per-block rescans
  (#206); bound full rescans and retain daemon preparation after re-centering
  (#235).
- Replace builder pipe polling with readiness waits (#237), bound share
  serialization and lazy parsing across all paths (#239), bound database
  decoding (#238), and bound Rust window paging memory (#241).
- Release completed executor work and callback captures, avoiding traceback
  cycles that retain finished job builds and delivery requests (#250).
- Release failed public-cache and address-validation requests (#252), and
  clear finished cancellation frames while keeping each job-build waiter's
  traceback separate (#253), without waiting for cyclic garbage collection.
- Bound candidate encoding, persistence, replay, payout membership checks,
  and canonical audit finalization; add the candidate-body migration, spool
  ownership, and crash/restart recovery gates (#258).

### Stratum, vardiff, and delivery health

- Persist per-session vardiff difficulty across reconnects (#138), improve
  initial difficulty convergence and credited-hashrate visibility (#204),
  and stop overriding mainnet stale-share grace to zero (#225).
- Remove the first-job admission lock convoy (#171) and evaluate delivery
  health by semantic work coverage (#221).
- Resolve CKPool version masks after node readiness, preserve static mode and
  explicit zero masks, and validate retry settings before wallet setup (#257).

### Public dashboard API

- Document the public/operator endpoint split (#147), extract the public read
  process (#152), and serve its database reads from a hot standby (#157).
- Serve bounded stale public reads during database outages (#167), enforce
  hashrate-series statement deadlines and bucket vocabulary with in-process
  stale-while-revalidate (#218), and serve historical series from incremental
  rollups (#219).
- Add reorg visibility and network hashrate (#217), dual-rate credited-hashrate
  series (#204), and found-block markers (#220).

### Observability, runtime, and CI

- Replace the shares-per-second gauge and add a coordinator-lock contention
  denominator (#161); keep telemetry useful under overload (#191).
- Add always-on heap and component-cardinality telemetry (#232), bounded heap
  census and allocator controls, and a documented resident-memory soak bound
  (#234).
- Release sampled thread-frame references after each stall-probe sample,
  including sampling caps and formatting errors, so completed workers' locals
  are not retained until cyclic garbage collection (#248).
- Make the interpreter switch interval tunable (#179), move service images
  and CI to Python 3.14 (#201), and shard lint, compile, Python, Postgres, and
  Rust CI checks while preserving the aggregate required check name (#243).
- Run push CI only for long-lived branches; PR branches are tested through
  their pull-request merge result (#295).

## Operator Verification

- Run `make doctor` with the deployment environment before starting the stack.
- After migration and startup, run `make prism-self-check`, inspect coordinator
  and public API readiness separately, and verify public requests reach the
  new service and its intended database.
- Verify replica health, WAL retention, canonical artifact downloads, accepted
  share accounting, and worker difficulty after reconnects before widening
  traffic. Keep production images digest-qualified.
- Additional validation targets include `make test-prism-postgres-seed-guard`
  and `make test-prism-public-read-replica`, plus
  `make test-prism-postgres-candidate-storage` and
  `make test-prism-postgres-native-ledger` for both candidate storage formats.
  Capacity qualification and memory soak procedures are in
  [the capacity readiness guide](../docs/prism-capacity-readiness.md).
- Candidate codec/storage experiments and actual mined-block crash/restart
  gates are separate evidence. ARM64 experiments do not establish production
  x86_64 performance or prove the cause of the exits reported in #255.
  PostgreSQL and exceptional helpers still need capacity proportional to their
  inputs within the documented limits. Coordinate production acceptance with
  #254: at least two continuous hours in one process epoch, the specified risky
  workloads, stable ownership after drain, and unchanged raw lease-failure
  counters. This release does not raise lease thresholds, suppress alerts, or
  change mining and payout rules.

Historical release notes remain in `doc/`.

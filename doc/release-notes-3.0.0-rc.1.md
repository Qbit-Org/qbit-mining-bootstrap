# qbit-mining-bootstrap 3.0.0-rc.1 Release Notes

Release date: 2026-10-06 (release candidate; tag `v3.0.0-rc.1`, published as a
GitHub pre-release).

This is the first release candidate of 3.0.0, the native Rust PRISM line. It
is the tree that #291's go/no-go evaluates on the PRISM production pair: two
frontends, each with its own `qbitd`, a PostgreSQL primary with one
asynchronous standby, and a public-read replica. It is not a production
release. Until #291 records a go, the production line stays 2.x.x (2.0.2).

A candidate runs in pre-production with block submission and broadcasting off,
so it offers the network no found block and no transaction. A rehearsal uses
both holds, as the rehearsal procedure requires:

- hold the ledger itself with `qbit-prism-server submission-hold set --reason
  ...` (#664, migration 023) before any frontend starts on it, so that it
  holds every frontend that connects;
- and start every frontend with `PRISM_BLOCK_SUBMIT_ENABLED=0` (#661), which
  also keeps the CTV fanout broadcaster from starting. A frontend started
  without it, on a ledger with no hold, offers every `pending` row it finds.

See [the block submission kill switch](../docs/prism-configuration.md#block-submission-kill-switch)
and [the rehearsal procedure](../docs/prism-ledger-ops.md#block-submission-kill-switch-for-rehearsals).
Never leave either in place on a production frontend.

## Highlights

- Replaces the 2.x.x Python coordinator with `qbit-prism-server`, a native
  Rust executable that runs Stratum, job refresh, block submission, CTV
  broadcasting and HTTP; no Python runtime or builder subprocess remains in
  the mining path. Operator work moves to subcommands, among them
  `migrate`, `import-audits`, `self-check`, `candidates`, `fatal-state`,
  `policy-transition`, `signing-transition`, `share-archive` and
  `submission-hold` (#244, #360, #401, #419, #439, #464, #682).
- Several frontends can serve miners at once against one PostgreSQL writer
  endpoint. Every instance coordinates through PostgreSQL, and only the
  canonical share order and each job's accounting boundary are serialized: a
  short transaction lock gives accepted shares one global `share_seq` order,
  database constraints deduplicate proofs across instances, extranonces come
  from a database sequence, expiring claims coordinate block candidates and
  CTV broadcasts, and a database fingerprint refuses conflicting
  configuration. A reconnecting miner can resume unexpired work on another
  frontend (#244, #319, #397).
- Payout vectors exported from 2.x.x replay identically except for three
  differences approved under decision D2: bootstrap work, which pays the
  solver, is used only while there are no historical shares (2.x.x also used
  it below its default three-miner gate); a proof that meets the network
  target but not its share target is credited at network difficulty once its
  block confirms on the active chain (2.x.x credited its share difficulty at
  node acceptance); and a carry-only balance at or above the payout floor
  can be paid in a bootstrap block. The 2.x.x per-worker credited-difficulty
  overrides are not supported (#269, #302, #310, #316).
- `qbit_share_ledger` is partitioned by `RANGE (share_seq)` (migrations 016
  and 017): `migrate` makes the existing table the first partition behind a
  validated bound, copying no rows and rebuilding no index, and every
  frontend keeps empty lead partitions attached. Migration 013 trims the
  ledger's secondary indexes, built concurrently on a populated ledger
  (#144, #153, #369, #419).
- `share-archive` is the share retention path: it plans, seals, archives,
  verifies, detaches, drops and restores whole partitions with the frontends
  running, never deletes a share, and first seals each dependent audit's
  canonical bytes, so blocks keep their published digests. A miner's
  `last_share_at` reads `null` once its last share is archived, and
  `/audit/share-window` returns no rows for an archived anchor. Stored native
  audit bodies no longer grow with the payout window (#144, #267, #353,
  #419).
- Migration 002's share-hash backfill runs after the migration transaction,
  in short batches with a durable cursor, so `migrate` needs no statement
  timeout above the default 15 s and resumes where it stopped. No frontend
  starts until the backfill finishes, and meanwhile the
  `share_hash_backfill_pending` capability fences earlier native builds off
  (#582, #663, #669, #681). Migration 016 attributes block solvers in
  bounded statements for the same reason (#672, #680).
- Found blocks are offered to the node before their audit lands, under one
  durable reservation per block, so no frontend offers a block twice, even
  after a crash. Unknown outcomes are reconciled from read-only chain
  observations, a proven lost tip race settles `orphaned`, and a
  `submitblock` that provably never ran (no connection, or `RPC_IN_WARMUP`)
  is offered again (#391, #415, #424, #522, #526, #527, #566).
- A block found on work whose payout revision was superseded on the current
  tip is captured and offered within a per-frontend overpay ceiling, with any
  overpay recorded (migration 020) and carried as debt. Candidate and CTV
  fanout claim leases are timed on the taker's monotonic clock, so a
  database clock step no longer hands a live claim to another frontend
  (migrations 021 and 022; #478, #504, #581, #646, #654, #657, #667, #674).
- `PRISM_BLOCK_SUBMIT_ENABLED=0` holds one frontend for rehearsals: found
  blocks are enqueued but stay `pending`, `submitblock` is never called and
  the CTV broadcaster does not start. `submission-hold set` holds every
  frontend from the ledger, whatever its own setting (migration 023),
  journaled with a reason, and `PrismBlockSubmissionHeld` pages when a
  frontend holds submission (#291, #661, #664, #666, #676, #682).
- CTV fanouts are broadcast by several frontends through database claims.
  The claim lane reads only due fanouts through a partial index (migration
  024), claims are handed back at shutdown, and a pass yields to its
  frontend's pending template refresh for at most the replacement-build
  budget. Every settlement mode now requires a pool fee (#433, #525, #534,
  #535, #568, #573, #576, #668, #679).
- The HA reference (decision D3) is one primary plus one dedicated
  asynchronous failover standby, separate from the public-read replica, with
  a two-frontend overlay, `compose.prism-ha.yaml`. Failover can lose
  acknowledged shares in the replication gap. An optional bounded wait lets
  the standby flush a found block's rows before its offer, and, given
  synchronized database clocks, blocks on work from a lost gap are refused
  after a promotion. Promotion, fencing and the TCP load balancer are
  operator-supplied, the last against a documented `/healthz` probe contract
  (#281, #304, #331, #449, #529, #570, #619, #645).
- Stratum keeps the 2.x.x miner contract, including username syntax, BIP310
  version rolling, vardiff, the high-difficulty listener and the one-parent
  stale grace (3 s by default). A per-source connection cap and per-session
  budgets for malformed frames, unknown job IDs and authorize attempts are
  new and off by default. A session opening during a same-tip rebuild storm
  no longer waits for the whole fan-out, overtaken same-tip builds are still
  delivered, and submits are answered while a session's rebuild is in flight.
  A plain share the payout-revision fence refuses is answered `stale-job`,
  not `ledger-confirmation-failed` (#244, #313, #468, #471, #598, #604, #616,
  #621, #625, #642, #675, #698).
- Template refresh keeps its incremental path across retargets, builds on a
  per-process worker pool (`PRISM_REFRESH_BUILD_THREADS`), and renews
  readiness from a guarded tip poll, so a slow rebuild no longer refuses
  current work as stale. After each block it lands, a frontend returns the
  freed heap to the kernel (`PRISM_LANDING_MALLOC_TRIM_ENABLED`, default 1)
  (#275, #501, #502, #600, #622, #627, #643).
- Coordinator metrics come from one native registry, with a generated
  inventory covering both roles, and `/metrics` answers 200 with freshness
  headers. The repository specifies the native alert rules and a review-only
  diff for the deployed rules that maps each retired Python alert. The
  native rules cover a stalled work refresh, a PostgreSQL outage and held
  block submission (#277, #278, #279, #299, #308, #320, #534, #646, #676).
- The new `qbit-prism-load` harness drives real Stratum sessions against
  real frontends, PostgreSQL with a streaming standby and a fake or real
  regtest node, and reconciles the ledger exactly with what clients were
  acknowledged. Presets model recipient weights, bursty arrival and rental
  churn; a `faults` phase injects faults under load; `external` mode drives
  deployed frontends through their load balancer; and #511's release
  benchmark adds an A/B driver and a D1 comparison summarizer (#271, #342,
  #521, #536, #539, #547, #554, #564, #590, #623, #635, #662).
- CI runs PRISM end to end. Required CI fails unless every gated test, live
  regtest included, runs without skipping. A nightly workflow runs load
  presets, real-node scenarios and Stratum fuzzing, and the `run-load` label
  runs its preset set on a pull request; weekly jobs run the 2.x.x migration
  lifecycle and a measured mainnet-shaped cutover on a real node, a 5.5 h
  soak, and the shipped images under Compose with the HA overlay. The L3
  production-window matrix runs on a version-bump pull request, the
  `release-candidate` label and `v*` tags, and a reduced matrix weekly. None
  of these lanes is a required check (#322, #487, #544, #545, #549, #550,
  #553, #565, #575, #587, #588, #594, #596, #597, #608).

## Upgrading from 2.0.x

Follow the [migration guide](../docs/prism-rust-migration.md). Native
migration is one-way, with no down-migrations (decision D5, #287, #366):

> Before the first native share is acknowledged, restore the complete
> pre-migration database and artifact backup and restart the pinned old image
> in isolation from the migrated database. After the first native share is
> acknowledged, restoring an older database loses those accepted records.
> Any rollback that discards acknowledged history requires an explicit
> accounting reconciliation and recovery decision; it is not an ordinary
> image rollback. Keep every native frontend stopped while performing an
> isolated restore/recovery operation against its replacement database.

- Drain pending candidates with a v2.0.1 or later image (v2.0.2 for a v2.0.2
  database), then stop every 2.x.x process; `migrate` takes v2.0.0 to v2.0.2
  sources and refuses an undrained one. Run `check-config`, `migrate` and
  `import-audits`, whose two missing counts must be 0, after rehearsing on
  an isolated restore.
- On a populated ledger the outage includes 002's share-hash backfill (35 to
  over 80 minutes for 65.5M shares) and the online steps of 013, 017 and 024,
  which can take hours. Earlier native builds are refused while the backfill is
  pending; never record migration 2 by hand (#582, #669).
- The 102 retired 2.x.x settings are listed in
  [`retired-settings.txt`](../crates/qbit-prism-server/src/config/retired-settings.txt).
  [`check-config`](../docs/prism-configuration.md) and `run` name any still
  set, and production mode refuses them (#361, #462).
- Production reads signing seeds from `_FILE` mounts, and every settlement
  mode needs a pool fee with one recipient, 0 bps allowed (#535).
- Block totals, chart markers and the default `chain_state=active` view of
  `/public/v1/blocks` count confirmed blocks only. Native block intents are
  persisted before node acceptance and stay out of them until confirmed;
  `chain_state=all` shows every candidate with its state.
- This candidate requires every native schema migration through 024.
  `migrate` applies them, and every start refuses a database missing one.
- It is tested against qbit 1.0.0, the release
  `.github/scripts/install-prism-qbit.sh` pins.
- The coordinator listener on 3341 now also serves `/public/v1` (2.0.x
  answered 404); keep it private and send public traffic to `public-api` on
  3342, which needs no audit mount. Runbooks:
  [ledger operations](../docs/prism-ledger-ops.md),
  [HA](../docs/prism-ha-reference-architecture.md),
  [alerts](../docs/prism-alert-migration.md) and
  [go-live checks](../docs/mainnet-deployment.md#go-live-checks).

## Verification

A candidate is verified by the full suite (#557):

- **CI** (`ci.yml`), on the version-bump pull request, and dispatched by hand
  on the tag, since `ci.yml` has no tag trigger: lint, compile and Compose
  validation; the Python and Rust unit tests; the PostgreSQL contract shards
  and the proof that every gated test executed; the dependency audit; and the
  Docker and real-`qbitd` image builds.
- **The L3 production-window matrix** (`prism-load-l3.yml`, suite `l3-full`:
  every #473 cell, three repeats, on 32 vCPU runners). It runs by itself on
  the version-bump pull request. The `v*` tag promotes that run when the
  tagged commit's tree is the one the run measured, and runs the suite again
  otherwise (#550, #608).
- **The load harness** (`prism-load-nightly.yml`), dispatched by hand on the
  version-bump pull request and on the tag, with `preset=all live=true
  weekly=true fuzz=true images=true bridging=true`:
  - every nightly and manual preset, including #473's 400k and 500k window
    cells at 1, 2 and 4 frontends, the 200k one-frontend cell and the
    mainnet shapes;
  - the bridging pair (#552);
  - the live regtest nightly and weekly scenario lists (#521, #553, #575);
  - the Stratum fuzz targets;
  - the shipped images under the Compose `prism` profile with the HA
    overlay (L6, #544).
- **The 5.5 h soak and the long fault set** (`preset=weekly`: `soak-weekly`
  and `faults-long-real-node`). For this candidate the soak runs once, at
  3.x.x `c95febb3`. The only runtime change between that commit and this
  candidate is #698 (#675). `faults-long-real-node` runs there and again on
  the version-bump pull request. Their verdicts are in the evidence bundle.

The evidence bundle on the `v3.0.0-rc.1` GitHub pre-release lists:
- every run, with the commit and tree it tested;
- each job's verdict and its artifacts;
- any failure, recorded as a named exception with its issue.

#291 links to the bundle. Job and gate verdicts are correctness evidence for
#291. The measured rates are not D1 verdicts (#487 decision 5).

## Known issues

Open on `3.x.x` when this candidate was cut:

- #695: at the 20x growth shape (8,000 sessions at a 1,000/s mean), share
  appends queue on the global order lock and reach 420 to 675 shares/s, short
  of the shape's rate. Today's mainnet shape (130 addresses) and the 5x shape
  (650) kept up with their offered rates, with zero shortfall; their headroom
  was not measured. This is a capacity ceiling for D1 (#477), not a
  correctness defect: no acknowledged share was lost and reconciliation was
  exact.
- #600: a 400k-window frontend's resident set ratchets up with block
  landings, from allocator retention. #627 returns the freed heap after each
  landing. Making jemalloc the allocator (#628) and judging the soak's memory
  on per-cycle floors (#629) are open, and the weekly soak still records #600
  as an expected failure.
- #602: block landing and settlement hold the order lock, which can delay
  share acknowledgements around a pool block.
- #622: new-tip delivery missed a 30 s bound with about 3,000 sessions at a
  1 s reanchor. #643 fixed the mechanism found; the issue stays open until
  that scenario is re-run. Production reanchors every 60 s.
- #630: a payout-revision race during the post-offer landing raises
  "offered candidate could not be processed" about twice per 30 minutes
  under load. The ledger runbook says when those ALERT lines are benign and
  when to act on them (#696).
- #670: `broadcast-ctv` cannot take over a dead frontend's fanout claim
  within one pass.
- #688, #689 and #690: the public pending-fanouts query, the CTV claim lane
  in a backlog, and fatal-state clearing each do work that grows with the
  number of fanouts.
- #582 (the share-hash backfill on a production-sized ledger) and #604
  (first jobs queued behind rebuild storms) are fixed in code (#663, #680,
  #681; #625) and stay open for #291's go/no-go sweep.
- #701: in `faults-long-real-node` at `c95febb3`, one public-API request
  failed with a connection error during the block failover. It was the run's
  only read-tier failure, and it fell outside the window in which the public
  API refuses by design. #291 requires the public API to keep answering
  through the failover drill.

Before a go, every acceptance criterion of #291 must be met. When this
candidate was cut, the open ones included:
- the production-sized migration rehearsal with its timings;
- the D4 key-rotation rehearsal and the D5 isolated-restore rehearsal;
- the failover drill and the load-balancer exercise under mining load, with
  `prism-public-api` answering `/public/v1` throughout;
- the two-hour soak against its criteria (#556);
- D1 throughput on the rehearsal host (#477).

# qbit-mining-bootstrap 3.1.2 Release Notes

Release date: not yet set. The tag will be `v3.1.2`, cut from the `3.x.x`
branch.

3.1.2 is a patch release of [3.1.1](release-notes-3.1.1.md): fixes, bounds
and measurements, with no migration and no new setting. One default changes:
`mining.authorize` is now bounded per session (#823). These notes describe
every change merged into `3.x.x` after the `v3.1.1` tag up to `af448068`;
what is still in review is [listed at the end](#still-in-review-not-yet-described).
The dual writer stays off by default: with `PRISM_DUAL_WRITER` unset, a
frontend runs as one writer, as in 3.1.1.

The upgrade from 3.1.1 is a binary swap, with three things to do around it:
- **Two lines an existing `.env` keeps.** An `.env` copied from 3.1.1's
  `.env.example` sets `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL=0`,
  which keeps the new authorize budget off, and pins Qbit Core v1.0.0
  ([Settings](#settings)).
- **Two new warning rules.** Re-vendor them only once every frontend runs
  3.1.2, and roll them back before the binaries
  ([Alert rules](#alert-rules)).
- **Work paid to the pool-fee address is a 3.1.2-only input** (#822). A
  3.1.1 frontend, dual-writer node or verifier refuses what 3.1.2 builds
  from it, so finish a rolling upgrade promptly
  ([Upgrading](#upgrading)).

Everything in the [3.1.1 notes](release-notes-3.1.1.md) applies, with the
changes below.

## Changes since 3.1.1

### Stratum: authorization and work

- **#828 (#823):** `mining.authorize` is bounded by default.
  - `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL` defaults to 32
    attempts per session per `PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS`
    (60 s by default), in the binary, `compose.yaml` and `.env.example`. It
    was 0, which turns the budget off, and 0 still does. A session's 33rd
    attempt in an interval is answered with error 20 `too many authorization
    attempts`, then the session is closed and counted in
    `qbit_prism_stratum_connection_refusals_total{reason="authorize_budget"}`.
  - A resolved payout identity is cached by its address (the text before the
    first `.`, exactly as typed) instead of by the whole username, so a new
    worker suffix on a known address costs no node `validateaddress` call.
    Ledger `miner_id`s are unchanged.
  - Sessions naming the same new address share one validation, and a
    frontend runs at most four at once. A session that has waited 5 s in
    all, behind another session's validation of its address and then for
    one of the four, is refused with `backend-rpc-unavailable` (`payout
    address validation is busy; retry`) and keeps its connection. Share
    submission, `getblocktemplate` and `submitblock` never wait on this.
  - When the cache (4,096 addresses, an hour each) is full, entries past
    their hour go first, then the addresses the node refused, which resolved
    to `PRISM_USERNAME_FALLBACK_ADDRESS`, and only then the others.
  - `PRISM_STRATUM_MAX_CONNECTIONS_PER_IP` stays 0 by default: this
    repository reads no PROXY header, so behind a load balancer every miner
    arrives from the balancer's addresses
    ([configuration](../docs/prism-configuration.md#stratum-per-source-cap-and-per-session-budgets)).
- **#827 (#822):** work on the pool fee's own P2MR program no longer fails
  template builds.
  - The fee program is published in every audit bundle
    (`payout_policy_manifest.pool_fee.p2mr_program_hex`), so any miner can
    authorize with it, as can an operator mining to its own fee address.
    Before, a share paying it made every work build fail with `pool fee
    account duplicates a miner payout account` until the share left the
    window, so work refresh stayed failed that long.
  - Now that work's gross is paid in the pool fee output and counted in
    `earned_pool_fee_sats`; no miner account is opened for the program. A
    carry the program accrued before it became the fee recipient is held
    unchanged. When such work leaves the other miners too little to clear
    the payout floor, the remainder is swept into the fee output and they
    accrue it, as with any dust sweep.
  - A window without such work builds byte-identical bundles, and
    `AUDIT_BUILDER_VERSION` stays 1. A window with it builds only on 3.1.2:
    [Upgrading](#upgrading) says what earlier frontends, dual-writer nodes
    and verifiers do with it.
- **#863 (#856):** a session no longer waits for its vardiff resume hint to
  be written.
  - After an accepted share's acknowledgement, and after a retarget, a
    session awaited the hint's write on the ledger pool, for up to 500 ms,
    before it read its next request. A slow write held up the miner's next
    `mining.submit`, and the share-acknowledgement histogram never saw it.
  - Now the session queues the hint and carries on. Each Stratum listener
    has one queue, at most 4,096 workers with one entry each, and one writer
    that writes one hint at a time, each as before and under the same
    500 ms bound. So hint writes hold about one ledger connection per
    listener instead of one per waiting session; a write that passes its
    bound keeps its connection until PostgreSQL ends the statement.
  - A worker's queued hints are combined into what their separate writes
    would have left. A decrease that would start its write more than 500 ms
    after the retarget that offered it is not written: the worker's row
    keeps its higher difficulty until a share at the lower one records it.
  - `qbit_prism_stratum_difficulty_hints_total{outcome}` counts every hint
    once: `written`, `failed`, `timed_out`, `aged_out` (that decrease),
    `dropped` (queue full), `superseded` (combined), `abandoned` (its
    listener stopped) or `writer_panicked`. A writer that panics is logged
    as an error and not restarted: that listener writes no more hints until
    the frontend restarts, and keeps serving.
- **#864 (#861):** the shared part of `mining.notify` is rendered once per
  published work instead of once per session; each session adds only its
  job ID and `clean_jobs`. The bytes on the wire are unchanged. A frontend
  keeps the eight most recently used rendered works, about 3.6 MB at a
  2,600-output coinbase, so its resident floor after a landing
  (`qbit_prism_landing_malloc_trim_resident_bytes`) can read up to that much
  higher.

### Blocks and offers

- **#834 (#816, item 3):** a `submitblock` that the HTTP client handed back
  unwritten is not sent, and its block is offered again. When the node or a
  proxy closed the parked spare connection after its check and before the
  request was written, the call failed as a plain `transport failed`, and
  the block was recorded with an unknown outcome and never offered again,
  although it never left the frontend. It now fails as `transport failed
  before the request was sent: the connection closed before the request was
  written (…)`, the reservation returns to `pending`, and the next offer
  goes out on a new connection. A close that can't be shown to precede the
  write stays unknown.

### Work refresh: the payout-revision listener

- **#834 (#816, item 1):** the payout-revision listener (#752) reports its
  health. `qbit_prism_payout_revision_listener_up` reads 1 while its
  `LISTEN` holds, and 0 before the first one, while it reconnects and once
  it ends. `qbit_prism_payout_revision_announcements_heard_total` counts the
  notifications it heard. A `LISTEN` through a transaction-pooling proxy
  holds and hears nothing, so the gauge reads 1: compare the counter across
  a node's frontends.
- **#851 (#843):** a new warning, `PrismPayoutRevisionListenerDown`, fires
  when that gauge has read 0 for 10 minutes on a frontend that had an
  authorized miner in the last 15 minutes ([Alert rules](#alert-rules),
  [runbook](../docs/prism-ledger-ops.md#a-payout-revision-listener-that-stays-down-843)).

### Share appends: where the time goes

- **#850 (#829, #844):** `qbit_prism_share_append_stage_seconds{stage,outcome}`
  times four stages of a share's path to its append: `submit_checks` (the
  checks before the append starts), `admission` (the wait for one of the
  `PRISM_DATABASE_MAX_CONNECTIONS - 1` append slots), `locked_call` (the
  one-call append under `ORDER_LOCK`, on PostgreSQL's clock) and `commit`.
  Each stage is recorded after it ends, outside `ORDER_LOCK`, and the share
  path's results are unchanged. A new warning,
  `PrismShareAppendAdmissionWaitHigh`, fires when a frontend's admission-wait
  p99 over five minutes is above 1 s, from at least 100 admissions, for
  10 minutes ([Alert rules](#alert-rules),
  [runbook](../docs/prism-ledger-ops.md#shares-queued-for-an-append-slot-844)).

### Database URLs and timeouts

- **#836 (#825, items 1 and 2):** startup refuses `sslmode=verify-ca`
  without a root certificate. Since 3.1.1 (#818) every such connect fails;
  now the setting is refused first, by name, with the connect's message
  `sslmode=verify-ca requires sslrootcert: …`. That covers
  `PRISM_DATABASE_URL` (the frontend, `check-config` and the database
  commands that check it as the server does), the public reader's
  `PRISM_DATABASE_URL`, `PRISM_PEER_DATABASE_URL` and
  `PRISM_PEER_DATABASE_URL_FALLBACK`. The mode and the certificate are
  resolved as the connect resolves them: the URL's `sslmode` and
  `sslrootcert` over `PGSSLMODE` and `PGSSLROOTCERT`, and an empty
  `sslrootcert` counts as none. `PRISM_DATABASE_URL` is now also parsed at
  startup with the PostgreSQL client's own parser, as the peer URLs and the
  public reader's already were, so a URL the client can't use
  (`sslmode=sometimes`, say) is refused by name instead of at the first
  connect. `make doctor` relays the public reader's refusal. CI now checks
  that `vendor/sqlx-core` is upstream sqlx-core 0.8.6 plus the patches
  recorded in `vendor/sqlx-core.patches/`, byte for byte
  ([database TLS](../docs/prism-configuration.md#database-tls-and-verify-ca)).
- **#852 (#824):** a statement timeout is told from a cancel by its SQLSTATE
  and its elapsed time, not by PostgreSQL's message, which the server
  translates under `lc_messages`. On a server whose messages were not
  English, 3.1.1's `migrate`, and a start with
  `PRISM_POSTGRES_INIT_SCHEMA=1`, stopped at the first brief lock contention
  in 027 and 028's table-lock attempts instead of trying again; the
  share-hash backfill stopped where it would have halved a batch or retried
  its record of 2, and `fatal-state clear` no longer advised raising
  `--timeout-seconds`. The table-lock requests now wait at most
  80 ms for any one table and 100 ms in all, so a request blocked on one
  table ends with 55P03 (`lock_not_available`), and the retry warning logs
  its `sqlstate=`. A 57014 that arrives before nine tenths of its
  statement's timeout is a cancel request, an operator's, and stops the run
  as before
  ([the migration guide](../docs/prism-rust-migration.md#supported-2xx-source-schemas)).

### Audits, operator routes and CTV

- **#833 (#737, #690):** the `/audit/carry-forward-integrity` and
  `/audit/ledger-integrity` routes run one report at a time and serve it
  dated.
  - Before, each call ran the report on the writer pool under
    `PRISM_DATABASE_STATEMENT_TIMEOUT_MS` (15 s by default), which the
    report outlasts at production size: every call held a writer connection
    for 15 s and answered 503 `read_timeout`, and concurrent calls each did
    the same.
  - Now each frontend runs one report at a time, in a read-only transaction
    under a statement timeout of its own (the time left to the run's 300 s
    deadline), on a connection that is closed when the run ends. Requests
    that arrive during a run wait for it and share its outcome. A report is
    served for 5 minutes after its run ends; a failed run answers 503
    `integrity_report_unavailable` for as long as it ran, at most 5 minutes
    ([API behaviour](#api-behaviour)). A stopping frontend abandons its run
    and starts none.
  - `fatal-state clear` asks the node `getblockhash` once per distinct
    height, where it asked once per pool block and once per deep-confirmed
    fanout, one after another, inside its recovery transaction.
- **#865 (#859):** `/audit/latest` is computed by one run at a time per
  frontend, bounded at 60 s, and its answer is served for 10 s, with the
  same three added fields. Before, every call rebuilt it on the share
  writer's pool, decoding the newest block's whole audit bundle, with no
  route timeout and no limit on concurrent calls. Now a frontend decodes
  each new piece of evidence once. A failure is not served again: the
  request that waited for it gets the 404 `no PRISM evidence has been
  produced` as before, or 503 `read_timeout` when the run hit its 60 s.
  [Ledger operations](../docs/prism-ledger-ops.md#blocks-balances-and-reorgs)
  describes what runs past 60 s look like.
- **#846 (#739):** `backfill-ctv` accepts a CTV set that a 2.x ledger stored
  under its sorted-key digest, and compares every column a verified set
  determines, as 2.x's own repair did. Before, it refused every such set
  with `existing CTV manifest set conflicts with verified audit`, while it
  compared too little of a 3.x row: a tampered `manifest_set_json`,
  `parent_coinbase_tx_hex`, amount or artifact column passed. A refusal now
  names the block, the fanout and the differing columns, and rolls back
  everything that block wrote; it still stops `backfill-ctv` at that block.
  A row whose digest is not the hash of its stored text, as 2.x's repair
  tool could write from an operator-supplied digest, is now refused.
  Stored rows are never rewritten.
- **#848 (#814):** the operator docs catch up with 3.1.1 (the cluster row,
  the chunked prepared apply, the one-call share append), and
  `scripts/prism_legacy_parts_digest_check.py --with-731-fix` models 3.1.1's
  import, reporting `ok-in-3x` for the 2.x v2 bodies it accepts. Its default
  mode is unchanged. `PrismAcceptedRevisionWorkTrackingUnknown`'s
  description says an idle backup's unknown wait also ends when the block
  matures.

### The public API

- **#862 (#858):** the pool summary, the reward leaderboard and the miner
  pages share one remembered answer to their node calls
  (`getblockchaininfo`, `getblocktemplate`, `getnetworkinfo`). Before, each
  cache miss of each route made its own, so template requests grew with the
  number of distinct miner pages viewed, on the node that also serves the
  mining path. One set of calls runs at a time, and the misses that arrive
  meanwhile wait for it. A failed or degraded answer is not remembered.
  A set still running at `PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS` fails
  with `read_timeout` and is dropped. Response bodies are unchanged; their
  network fields can be older than `Age` says
  ([API behaviour](#api-behaviour)).
- **#867 (#857):** the miner page, the pool summary and the blocks page read
  less on each cache miss. The miner page drops a whole-pool sum nothing
  read and computes its reward share without ranking every miner. The pool
  summary's hashrate sums and 3-hour participants come from the 5-minute
  rollups plus the raw shares they don't cover yet, as the hashrate charts
  do, and from the raw 24 hours when the rollups lag a day or are absent.
  An `active` blocks page reads `qbit_pool_blocks_public_recent_idx`. The
  answers are the same on the same data, with one exception: on a network
  whose payout window is under a day of shares, after
  `share-archive detach --retention-days 0` detaches shares younger than
  24 hours, `h24` (in the extreme `h3`, `h1` and the 3-hour participants)
  keeps counting them through the rollups, as the charts already do, where
  3.1.1 dropped them.
- **#849 (#688):** `/public/v1/fanouts/pending` and `/audit/fanouts/pending`
  read the fanout status index instead of every fanout, settled ones
  included. The responses are unchanged, byte for byte.

### Qbit Core v1.0.1

- **#876:** the bootstrap builds, downloads and tests against Qbit Core
  v1.0.1 (`0439a7e5b37fb3f7e4ddfb735b68961445a882c8`) instead of v1.0.0
  (`7ebcddb6`). `config/upstream.env`, `config/upstream.env.example` and
  `.env.example` carry the new pin, and CI on `3.x.x` installs and builds
  v1.0.1 only. An existing `.env` keeps v1.0.0 ([Settings](#settings)).
  #876 records why no test expectation changed.

### Tests, CI and the release process only

- **Load harness and simulators:** #835 and the harness half of #850 (#829,
  where accepted acknowledgements' time went, split at `ORDER_LOCK`, with
  the WAL syncs and checkpoints behind it), #854 (#842, the host's steal
  and pressure at each tip, and median tip gates on five nightly presets),
  #847 (#695, the weekly reduced L3 measures `mainnet-shape-2600-addresses`
  report-only), #874 (the collate tests after #847 and #854), #845 (#817,
  the side report carries every gate) and #853 (#841, the dual-writer
  simulator waits out a candidate claim nobody renews).
- **CI:** #839 (a pull request whose tree already passed skips the repeated
  work; the `run-full-ci` label runs everything), #845 (the lint job runs
  both generators' `--check`), #866 (the PostgreSQL service image is pinned
  to 16.15, the version of the runner's own PostgreSQL tools) and #836's
  vendored sqlx-core check.
- **Release process and repository rules:** #831, #838 and #875 (`main` is
  a parent of `3.x.x`, the rulesets, and the `signed-landing` check a pull
  request into `3.x.x` must pass) and #832 (#825, item 3: the
  release guide's notes rewrite fails closed).

None of these changes the server; #853 only exposes server constants to the
simulator.

No dependency changes: no manifest and neither lockfile changed since
`v3.1.1`. The vendored sqlx-core source is unchanged; #836 adds its
`vendor/sqlx-core.patches/` and rewrites its README.

## Operator-facing changes

### Migrations

None. `git diff --stat v3.1.1` over `crates/qbit-prism-server/migrations`
is empty, so 3.1.2 runs on the database 3.1.1 left, at 032, and a 3.1.1
binary runs on it after a rollback.

### Settings

- **Changed default:** `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL`,
  0 to 32 (#823). An explicit value wins over the default, and 3.1.1's
  `.env.example` shipped `=0`: an `.env` copied from it keeps the budget
  off. Delete the line, or set it to 32. A deployment that sets no value
  gets 32 from the binary, or from `compose.yaml`'s `:-32`. A Stratum proxy
  that sends `mining.authorize` for more than 32 downstream workers over one
  upstream connection within an interval needs a higher value, or 0.
- **Qbit Core pin:** an `.env` (or `DEPLOY_ENV_FILE`) is read after
  `config/upstream.env` by `scripts/prepare-qbit-source.sh` and
  `scripts/check-env.sh`, so one copied from an earlier `.env.example` keeps
  `QBIT_GIT_REF=v1.0.0` and `QBIT_GIT_COMMIT=7ebcddb6…`, and `make doctor`
  reports that checkout verified, since it matches its own `.env`. To build
  v1.0.1, set:

  ```dotenv
  QBIT_GIT_REF=v1.0.1
  QBIT_GIT_COMMIT=0439a7e5b37fb3f7e4ddfb735b68961445a882c8
  ```

  The bootstrap stages and builds the node from that source
  (`scripts/prepare-qbit-source.sh`, which `make` runs), independently of
  the PRISM binaries. A deployment that passes both values in its
  environment is unaffected by the new default.
- **Added, removed or renamed:** none. So, apart from the two lines above,
  the environment is the same for upgrade and rollback.
- **Refused at startup now:** `sslmode=verify-ca` without a root
  certificate on `PRISM_DATABASE_URL`, the public reader's URL and both
  peer URLs, and a `PRISM_DATABASE_URL` the PostgreSQL client can't parse
  (#836). Both already failed at connect in 3.1.1, so only the point of
  failure moves. A few operator commands read the URLs directly and still
  fail only at connect, with the same message: `fatal-state show`,
  `submission-hold show`, `candidates list`, `recover`'s planning and
  `node-identity`'s peer URLs.

### Database connections

No change to the budget the [3.1.1 notes](release-notes-3.1.1.md#database-connections)
set: each frontend uses up to `PRISM_DATABASE_MAX_CONNECTIONS` + 1, and 2
more in dual-writer mode. What moved inside it: the integrity routes hold at
most one run pool connection per frontend, closed when its run ends (#833),
and resume-hint writes hold about one per Stratum listener, more only while
writes outlast their 500 ms bound (#856).

### Metrics

- **Added:**
  - `qbit_prism_payout_revision_listener_up` and
    `qbit_prism_payout_revision_announcements_heard_total` (#816);
  - `qbit_prism_share_append_stage_seconds{stage,outcome}` (#829);
  - `qbit_prism_stratum_difficulty_hints_total{outcome}` (#856).
- **Now counts by default:**
  `qbit_prism_stratum_connection_refusals_total{reason="authorize_budget"}`,
  since the budget is on (#823).
- **Changed meaning, renamed or removed:** none
  ([metrics](../docs/prism-native-metrics.md)).

### Alert rules

Two warnings are added to `docs/prism-native-alert-rules.json`. Neither has a
page label, and both answer no data and evaluation errors with OK:

| Rule | UID | Fires when |
| --- | --- | --- |
| PrismPayoutRevisionListenerDown | `qbit-prism-revision-listener-down` | a frontend's `qbit_prism_payout_revision_listener_up` has read 0 for 10 minutes while it had an authorized miner in the last 15 minutes (#843) |
| PrismShareAppendAdmissionWaitHigh | `qbit-prism-share-append-admission-wait` | a frontend's append-slot wait p99 over five minutes is above 1 s, from at least 100 admissions, for 10 minutes (#844) |

No rule's UID, expression or gate changed, and none was removed.
`PrismAcceptedRevisionWorkTrackingUnknown`'s description changed (#848).
Re-vendor the rules (for qbit-tools, its next re-vendor of the native rules)
in this order:
- **After the last 3.1.1 frontend is gone.** The listener rule counts a
  scraped frontend that does not export the gauge as down, and 3.1.1 does
  not export it. Deployed while a 3.1.1 frontend serves miners, it warns on
  that frontend after 10 minutes. The admission rule stays quiet on a
  frontend without the stage histogram, so it can go first.
- **Unlike 3.1.1's rules,** these do not survive a binary rollback
  unchanged: see [Rolling back](#rolling-back).

Both are in [the alert migration](../docs/prism-alert-migration.md#review-and-apply-the-draft).

### API behaviour

- **`/audit/carry-forward-integrity` and `/audit/ledger-integrity`** (#833):
  a failed, timed-out or abandoned run answers 503
  `integrity_report_unavailable`, where it answered `read_timeout`; its
  message names the step that failed, when that run started and when the
  next may start. A report is reused for 5 minutes after its run ends, so it
  can be up to 5 minutes plus its run time old, and it adds three fields:
  `computed_at` (when its run started), `age_seconds` (its age when served)
  and `run_seconds` (how long the run took, an upper bound on the report's
  own time). A fresh report on demand is `self-check`'s.
- **`/audit/latest`** (#859): reused for 10 s after its run ends, so its
  counts can trail the ledger by 10 s plus the run's time; a run is bounded
  at 60 s and then answers 503 `read_timeout`. It adds the same three
  fields. Every key it had keeps its name and value.
- **No 304 from these three routes.** Each answer carries `age_seconds`,
  which changes from one request to the next, and the `ETag` is the body's
  hash, so a request with `If-None-Match` gets the full body.
- **Public network fields** (#858): on the pool summary, the reward
  leaderboard and the miner pages, the `network` object (all but
  `network.hashrate_ths`, read by every response) and what its
  `network_difficulty` sizes can be older than `Age` says: by up to the
  shortest cache lifetime of those routes, 5 s by default and never more
  than 15 s, counted from when its node calls started, or, for a response
  that waited for a set of calls already running, by however long that set
  had run. Not at all when the public cache is off or any of their TTLs is
  0 ([the public API](../docs/public-dashboard-api/README.md)).
- **The pending fanouts feeds** (#688) answer as before, byte for byte.

## Upgrading

- **From 3.1.1:** replace the binaries.
  1. **Decide the two `.env` lines** ([Settings](#settings)): delete
     `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL=0` or set it to 32
     (higher behind a Stratum proxy), and move the Qbit Core pin to v1.0.1
     when you rebuild the node.
  2. **Replace the binary** on every frontend and tool, and on the
     `public-api` service. There is no `migrate` step: 3.1.2 adds no
     migration and runs on 3.1.1's database as it is.
  3. **Re-vendor the alert rules** once the last 3.1.1 frontend is gone
     ([Alert rules](#alert-rules)).

  **A rolling upgrade is supported as a single writer.** 3.1.1 and 3.1.2
  frontends can serve one database together, so they can be replaced one at
  a time. Until the last 3.1.1 frontend is gone:
  - only 3.1.2 frontends apply the authorize budget, share an address's
    validation, offer again a `submitblock` handed back unwritten (#816),
    hand resume hints to a writer (#856) and serve the integrity routes and
    `/audit/latest` as above;
  - only 3.1.2 frontends export the listener gauge, so deploy its rule
    afterwards ([Alert rules](#alert-rules));
  - **work paid to the pool-fee address (#822) is built and landed only by
    3.1.2.** While a share paying the fee program is in the window:
    - a 3.1.1 frontend still fails every work build, as in 3.1.1; a 3.1.2
      one builds the window;
    - a block found on that work is offered as usual, since `submitblock`
      doesn't wait for a rebuild. If a 3.1.1 frontend claims it for its
      landing, as after the finder's frontend dies, the rebuild fails: it
      logs `ALERT: offered candidate could not be processed; kept for
      reconciliation, never offered again`, with a reason that starts
      `post-offer processing failed (…)` and ends `pool fee account
      duplicates a miner payout account`, and the row waits in
      `reconciliation`, retried `min(3600, 10 × attempt_count)` s apart.
      Nothing is offered twice and no wrong bytes are written. Once a 3.1.2
      frontend claims the row, it lands. If other pool blocks confirm in the
      meantime, its confirmation can record a payout divergence and debt, as
      for a captured block
      ([#478](../docs/prism-ledger-ops.md#the-overpay-a-captured-block-can-cause));
    - 3.1.1's `qbit-prism-audit-verify` and `backfill-ctv` refuse such a
      block's bundle. Verify bundles with a 3.1.2 or later verifier, and
      tell third-party verifiers so.

    It takes a miner using the fee address while both releases serve, so
    finish the rollout promptly, and until then expect that ALERT for such
    a block ([ledger operations](../docs/prism-ledger-ops.md#work-paid-to-the-pool-fee-address-during-a-311-to-312-upgrade-822)).
    `AUDIT_BUILDER_VERSION` stays 1 on purpose: a new version would make
    3.1.1 refuse every candidate 3.1.2 builds.

  **In dual-writer mode,** replace each node's frontends as above, and a
  node's frontends together, since they claim from one candidate outbox.
  3.1.2 changes no table and no row format the peer sync copies. A 3.1.1 node that has to adopt its peer's block from the
  peer's prepared record (a block found by a dying node, S8) cannot rebuild
  one whose window holds fee-program work: it logs `ALERT: a pool block on
  the active chain has no landing rows here and no adoptable prepared
  record` with the same refusal as its reason, and tries again.
- **From 3.1.0:** as in the [3.1.1 notes](release-notes-3.1.1.md#upgrading),
  with 3.1.2 in 3.1.1's place: 3.1.2's `migrate` applies 032 while the
  3.1.0 frontends serve. Then the `.env` lines and the alert rules as above;
  the rules 3.1.1 changed and the two added here are re-vendored together,
  after the last 3.1.0 frontend is gone. A 3.1.0 frontend is refused the
  fee-program window as a 3.1.1 one is.
- **From 3.0.0 or 3.0.0-rc.6:** as in the 3.1.1 notes, with 3.1.2 in
  3.1.1's place. The pre-migrate's table-lock requests now wait at most
  80 ms for any one table and 100 ms in all, and on a server whose
  `lc_messages` is not English they retry contention instead of stopping
  (#824). Its refusal after ten minutes now reads `refusing to wait any
  longer for the table locks of …: … attempts over … s each waited up to
  100 ms for …, 80 ms for any one, and gave up`.
- **From 2.0.x, or from a 3.0.0 candidate before rc.6:** as the 3.1.0 notes
  say, with 3.1.2 in 3.1.0's place.

## Rolling back

- **To 3.1.1:** replace the binaries with 3.1.1's, one at a time if need
  be. Nothing in the database needs undoing.
  - **Roll the alert rules back first,** or at least remove
    `PrismPayoutRevisionListenerDown`. Otherwise each 3.1.1 frontend that
    serves miners warns after 10 minutes, because 3.1.1 doesn't export the
    gauge. `PrismShareAppendAdmissionWaitHigh` can stay: without the stage
    histogram it is quiet.
  - **Settings:** 3.1.1 reads the same authorize setting and honours the
    value it is given. Only its default differs: 0 in 3.1.1's binary and
    `compose.yaml`, so set the value explicitly if you roll the checkout
    back too. The Qbit Core pin is a separate decision: CI tested 3.1.1
    against v1.0.0 and tests 3.1.2 against v1.0.1.
  - **#822 returns.** While a share paying the fee program is in the
    window, every 3.1.1 frontend fails its work builds. A block whose
    window holds such work and that has not landed waits in
    `reconciliation` with the ALERT above until a 3.1.2 frontend lands it,
    and 3.1.1's `qbit-prism-audit-verify` and `backfill-ctv` refuse the
    bundles of those that landed.
  - The other fixes above go with the binaries, and the issues they fixed
    return.
- **To 3.1.0, 3.0.0 or 3.0.0-rc.6:** as in the
  [3.1.1 notes](release-notes-3.1.1.md#rolling-back), with the alert rules
  rolled back first as above.

## Verification

Each fix's tests are in its pull request. For this release, CI runs on the
version-bump and promotion pull requests, and `prism-load-l3.yml` runs the
L3 production-window matrix (`l3-full`) on the version bump; the tag
promotes the run whose tree it matches (#608). No ruleset makes CI a
required check: `signed-landing` is the only required check on a version
line, so these runs are the release coordinator's to read. Their results
are recorded here when the release is cut.

## Known issues

As in the [3.1.1 notes](release-notes-3.1.1.md#known-issues), with these
changes.

**Fixed in 3.1.2:**
- #822 by #827, and #823 by #828;
- #737's open part, the integrity routes and #690, by #833, so 3.1.1's
  "don't poll them" no longer applies;
- #739 by #846, so `backfill-ctv` need no longer be skipped for a 2.x
  ledger;
- #688 by #849;
- #816's items 1 and 3 by #834, and #843 by #851;
- #825's items 1 to 3 by #836 and #832;
- #824 by #852, except the case below;
- #829's missing measurements by #835 and #850, and #844 by #850;
- #856 by #863, #857 by #867, #858 by #862, #861 by #864, and
  `/audit/latest` from #859 by #865;
- #814 by #848.

3.1.2's tests and CI also fix #817 (#845), #841 (#853) and #842's gates
(#854).

**Still open:**
- **After an unclean loss of the database primary,** when the fence leaves
  the frontends' established connections to it silent (a power-off, or a
  rule that drops packets without a reset), a frontend's ledger connection
  that had a request in flight has no TCP keepalive or user timeout to end
  it. It waits for the kernel's retransmissions to give up, which with
  Linux's default `net.ipv4.tcp_retries2` the kernel documents as about
  15 minutes, and while it waits it holds its pool slot and, for a share
  append, its append slot.
  So a frontend can refuse shares with `ledger-confirmation-failed` after a
  failover that otherwise succeeded. As a precaution, the failover runbook
  now restarts both frontends after moving the writer endpoint, unless the
  move resets every established connection
  ([step 5](../docs/prism-ha-reference-architecture.md#promotion-fencing-and-the-stable-writer-endpoint)).
- **#824's remaining case:** a share append that finds no partition for its
  `share_seq` attaches the partition lead and tries once more only when
  PostgreSQL's message is the English `no partition of relation`; on a
  server whose `lc_messages` is not English it fails as any other ledger
  error. Partition maintenance keeps the lead attached ahead of the
  sequence.
- **#829:** what slows the `ORDER_LOCK` section in the load runs that queue
  share acknowledgements is not yet attributed. 3.1.2 adds the measurements
  and the warning above.
- **#859's other routes:** `/owed` and `/miners/{id}/status` still read the
  share writer's pool on every call.
- **#816's item 2:** a payout-revision wake that reaches a frontend whose
  node still lags the tip logs a spurious `template refresh deferred` WARN
  until the node catches up.
- **#825's items 4 and 5:** the spare `submitblock` connection is never
  reused over TLS 1.3, which costs only a fresh connect, and parking it
  scans the process's file descriptors on a runtime thread.
- **#695:** the 20x growth shape still misses its capacity budgets; the
  weekly reduced L3 now measures it report-only (#847).
- #779, #712, #741, the dual writer's known limits, and #602, #622, #670,
  #689 and #704, as the 3.1.1 notes describe them.

## Still in review, not yet described

These are not merged at `af448068`, so nothing above describes them. The
release coordinator replaces this list before the release:
- #868 (#860, issued-job saves in overlapping batch transactions);
- #880 (#878, the dependency audit and chrono's RUSTSEC-2026-0543);
- the pull request for #872 (a server-side bound on difficulty-hint
  statements);
- the pull request for #869 (`PrismPayoutRevisionListenerDown`'s smoothing
  against `PrismMetricsEndpointDown`'s dwell);
- six more this wave opens: the found-block re-offer, the found-block append
  retry, the public cache key, `accept()` and `ntime`, the release guard,
  and these notes with their runbook changes.

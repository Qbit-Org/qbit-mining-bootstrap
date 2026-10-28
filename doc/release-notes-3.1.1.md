# qbit-mining-bootstrap 3.1.1 Release Notes

Release date: not yet set. The tag will be `v3.1.1`, cut from the `3.x.x`
branch.

3.1.1 is a patch release of [3.1.0](release-notes-3.1.0.md): fixes only, with
no new feature. Its one new setting is for a `self-check` run, not a frontend
(#808). It contains every change merged into `3.x.x` after the `v3.1.0` tag.
The dual writer stays off by default: with `PRISM_DUAL_WRITER` unset, a
frontend runs as one writer, as in 3.1.0.

Unlike the upgrade from 3.1.0-rc.1 to 3.1.0, this one is more than a binary
swap:
- **Every start requires migration 032.** It adds two functions and changes
  no table. 3.1.1's `migrate` applies it while the 3.1.0 frontends serve, and
  they keep serving and restarting beside it ([Upgrading](#upgrading)).
- **Each frontend holds one more database connection,** for its
  payout-revision listener (#795). Budget it before the upgrade
  ([Database connections](#database-connections)).
- **Five published alert rule UIDs changed** (#793), and two warnings are
  gated differently (#800). Re-provision the rules as
  [Alert rules](#alert-rules) says.

Everything in the [3.1.0 notes](release-notes-3.1.0.md) applies, with the
changes below.

## Changes since 3.1.0

### Share acceptance and the order lock

- **#788 (#716):** a share admitted under a tip-change lease keeps its credit
  when its commit gate meets a writer on one of the chain-authority locks.
  The append rolls back, waits for the authority outside `ORDER_LOCK`, and
  runs again, logging `share commit gate waited out contention for its
  authority; appending again` at INFO. Before, the share was refused and
  never credited. Contention that outlasts the acknowledgement deadline is
  still answered `current chain state is unavailable`, now logged
  `closure="authority-contended"`; `closure="authority-unavailable"` now means
  a readiness or tip-observation loss only
  ([commit-gate refusals](../docs/prism-rejections.md#commit-gate-refusals)).
  This is the refusal behind #779's L3 failures.
- **#801 (#738):** a share append no longer writes the `qbit_prism_cluster`
  row. Each share is stamped with the greatest of the stored ledger clock,
  the database's clock and the stamp of the node's newest own share
  (`qbit_prism_ledger_clock`, migration 032). A snapshot held on
  the primary no longer leaves a dead row version per share, which
  saturated `ORDER_LOCK` in about 20 s at 400 shares/s. The snapshot anchor
  and the peer sync's own-log floor are the only writers of the stored clock.
- **#802 (#711):** a share append without a block does its in-lock work in
  one server-side call, `qbit_prism_append_share` (migration 032). `ORDER_LOCK`
  is held for that call and the `COMMIT`, where it was held for the lock
  statement, three statements and the `COMMIT`. A refusal is raised before
  any write and mapped back to the miner answer the statements gave. The
  append's `ORDER_LOCK` wait and hold metrics now come from the server's own
  measures. An append abandoned while queued on `ORDER_LOCK` can now leave a
  gap in `share_seq`, as a rollback always could; window proofs tolerate
  gaps.

### Blocks, offers and settlement

- **#797 (#762):** a `submitblock` whose new connection isn't ready within
  half its deadline (`PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS`, 1 s by default,
  and never more than 4.9 s) fails as not sent: its block returns to pending
  and is offered again. Before, it was recorded as an unknown offer and never
  offered again, although it never left the frontend. `last_error` names the
  cut, as in `no connection within 500ms, its share of the call's 1s
  deadline`. The cut connect runs on and serves the next call, on Linux only
  and only while the kernel reports it open and unread. A call that took a
  pooled connection meanwhile keeps that request's outcome.
- **#798 (#630):** a post-offer write (the landing, the finish, or an orphan
  settlement) that loses a payout-revision race is observed again and
  retried in place, at most 3 times, none starting 60 s or more after the
  first lost race. Before, every lost race logged `ALERT: offered candidate
  could not be processed` and went to reconciliation, about once every 3
  landings on the 400k two-frontend soak. A spent budget, and every other
  failure, keep the ALERT and reconciliation. `candidates recover` retries
  its finish too. The new counter
  `qbit_prism_post_offer_revision_races_total` counts both outcomes, and
  [ledger operations](../docs/prism-ledger-ops.md#a-lost-revision-race-after-the-offer-630)
  has the rewritten runbook.
- **#786 (main's #781):** each disposition poll under the block-proof
  acknowledgement bound runs in its own task, so the bound no longer drops a
  pooled connection partway through a checkout (3.1.0's known issue). #786
  merged the released `main` (#783) into `3.x.x`, which also brought
  `docs/releasing.md` and the released 3.0.0 notes.

### Work refresh

- **#795 (#752):** when one frontend moves the payout revision without
  moving the tip, as a pool block's confirmation does, every other frontend's
  refresh wakes through PostgreSQL `LISTEN`/`NOTIFY` and republishes at once.
  Before, they kept the superseded work, and refused shares on it as
  `stale-job`, until their next `PRISM_BLOCKPOLL_SECONDS` tick, up to about
  2.6 s after each pool block. The revision fence is unchanged, and the tick
  stays as the fallback. Each frontend's listener holds its own database
  connection ([Database connections](#database-connections)).

### Migration runner

- **#791 (#789):** 3.1's `migrate` can no longer deadlock with frontends that
  serve the database while 027 and 028 run, as rc.6 frontends do during the
  pre-migrate from 3.0. It locks every table 027 and 028 alter in one
  statement that waits at most 100 ms, and on a timeout or deadlock rolls the
  whole migration transaction back and tries again, backing off from 100 ms
  to 2 s, for up to ten minutes. 027's and 028's SQL is unchanged. Past ten
  minutes it stops, records nothing, and names up to five holding
  transactions. Every connect that migrates takes the migration history's
  lock the same way, so a start with `PRISM_POSTGRES_INIT_SCHEMA=1` that
  can't take it now retries for up to ten minutes, warning on the first
  attempt and every tenth, where it failed after
  `PRISM_DATABASE_LOCK_TIMEOUT_MS` (5 s)
  ([the migration guide](../docs/prism-rust-migration.md#supported-2xx-source-schemas)).

### Database TLS: the vendored sqlx-core

- **#794 (#771):** sqlx-core 0.8.6 is vendored in `vendor/sqlx-core` with
  launchbadge/sqlx#4251, so a large statement over TLS can no longer lose its
  wakeup and stall the ledger connection, and any advisory lock it holds,
  until a client deadline. Both workspaces patch sqlx-core to that
  directory, and `lab/prism/Dockerfile` copies it, so a build from the tag
  needs `vendor/`. CI checks that both lockfiles build the vendored crate
  and that every sqlx-core RustSec advisory is reviewed.
- **#807 (#713):** `sslmode=verify-ca` accepts a CA-signed certificate issued
  for another host name, as libpq's `verify-ca` does: it checks the chain,
  not the name. Before, it refused one, as `verify-full` does. `verify-full`
  still checks the name. This backports launchbadge/sqlx#3861 into the same
  vendored crate.

### Dual writer

- **#799 (#785):** the peer sync's `apply_prepared` takes the blob-GC fence,
  in chunks of at most 1,000 jobs and 2 MiB each, so the blob collector can't
  delete a template or balance blob a pulled job reuses. Before, the job
  could commit without it, and a block the peer found on that job couldn't
  be adopted on this node.

### Audits, self-check and the public API

- **#808 (#734):** production `self-check` runs and reports every local check
  before its audit-completeness verdict, so it can gate a deployment while
  `import-audits` runs. Before, it stopped at audit completeness and skipped
  them. Its `audit_completeness` now reports a `status` (`complete`,
  `legacy_import_pending` or `incomplete`), and `pending_legacy_rows` and
  `incomplete_native_rows` beside the existing counts. With
  `PRISM_SELF_CHECK_LEGACY_IMPORT_PENDING=1`, set for that run only, legacy
  rows awaiting import are reported as pending instead of failing; a native
  row missing its bytes, or an unreadable count, still fails.
  `make prism-self-check` passes the setting into the container only when it
  is set. `scripts/prism_legacy_import_admission_gate.sh` stays, for earlier
  builds
  ([the migration guide](../docs/prism-rust-migration.md#drain-and-migrate)).
- **#796 (#735):** `/public/v1/artifacts/<sha>` answers a `no-store` 404,
  not 500, when an audit's legacy body, or a share segment it names, isn't on
  the host yet, as before `import-audits` has stored it. A body that is there
  but can't be read or verified still answers 500, logged at warn. A row
  re-imported under another address between the lookup and the read answers
  404 instead of serving the other audit's body. `file://` share segment URIs
  now resolve in the API, `import-audits` and the audit CLIs.
- **#806 (#733):** a legacy `segment_range` or `segment_prefix` part verifies
  when its slot has a `share_seq` gap outside the range the part references.
- **#809 (#709, #731):** legacy (1.x and 2.x) audit bodies verify without
  sidecars when their digests were computed as the Python writers computed
  them: range and prefix digests over Python's ASCII-escaped JSON (#709), and
  2.x audit-bundle.v2 parts digests in 2.x's key order (#731). A parts digest
  in sorted-key order, which no writer produced, is now refused.
  `import-audits`, the canonicalizer and the public API all verify through
  the fixed parser. The parts-digest checker keeps its statuses, so with this
  release its `2x-order-only` rows are ones `import-audits` accepts, and the
  sidecar tool reports them `ok-in-3x` without writing a sidecar
  ([#731 in the migration guide](../docs/prism-rust-migration.md#2x-v2-bodies-whose-window-proof-parts-digest-3x-recomputes-differently-731)).

### Alerting

- **#793 (#757):** five native alert rule UIDs are shortened to Grafana's
  40-character limit ([Alert rules](#alert-rules)). CI rejects any UID that
  is too long, has other characters or is duplicated.
- **#800 (#758):** `PrismAcceptedRevisionWorkPending` and
  `PrismAcceptedRevisionWorkTrackingUnknown` count only on a frontend that
  had an authorized miner in the last 15 minutes, so an idle backup frontend
  no longer warns after every pool block. On the server, a frontend no
  longer keeps a block's wait unknown until restart when another settlement
  confirmed the block first (a peer's, or its own attempt whose reply was
  lost): its own later settlement proves a covering revision, and its next
  write at or above it ends the wait without a sample.

### Tests and CI only

- #775 (R1's pre-migrate checks), #792 (#729, the restart test's statement
  count), #804 (the block-only poll acquire tests) and #805 (#790, the
  real-node smoke's tip-to-notify gate). None changes the server.

No dependency version changes, and no package is added to or removed from
either lockfile: sqlx-core 0.8.6 now builds from `vendor/sqlx-core`. Two
manifest changes touch only crates already in the graph:
- the server's tests name `rustls` and `sqlx-core` as dev-dependencies, both
  already in the graph through sqlx, with `sqlx-core` held at `=0.8.6`, the
  vendored copy (#794 added them; #807's verify-ca test uses them too);
- the server's `hyper-util` dependency enables its `client-legacy` feature
  (#797).

## Operator-facing changes

### Migrations

- **New: 032** (`crates/qbit-prism-server/migrations/032_share_append_call.sql`,
  #711, #738). It creates two functions, `qbit_prism_ledger_clock` and
  `qbit_prism_append_share`, and alters no table. It declares no capability
  and needs no shutdown proof. It is applied in the migration transaction,
  with no up-front table lock: `CREATE FUNCTION` takes no lock a serving
  frontend waits for
  ([migration 032](../docs/prism-rust-migration.md#migration-032-the-share-append-in-one-server-side-call)).
  Every 3.1.1 frontend, tool and operator command but `migrate` refuses a
  database without it.
- **No released migration changed.** `git diff --stat v3.1.0` over
  `crates/qbit-prism-server/migrations` shows only the new 032, so sqlx's
  checksums of 001 to 031 are unchanged. #791 changes the runner around 027
  and 028, not their SQL.

### Settings

- **Added, for `self-check` only:** `PRISM_SELF_CHECK_LEGACY_IMPORT_PENDING`
  (#808). Set it for a `self-check` run while `import-audits` is storing
  legacy audits, and never in a frontend's environment: there it would hide
  an import that stopped, and a 3.1.0 `run` or `check-config` refuses a
  `PRISM_*` name it doesn't know in production mode.
- **Removed or renamed:** none, and no default changes.
- **Bounding something more:**
  - `PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS` also bounds `submitblock`'s
    connect, at half the deadline and never more than 4.9 s (#797).
  - `PRISM_DATABASE_LOCK_TIMEOUT_MS` no longer bounds a migrating start's
    wait for the migration history's lock, which retries for up to ten
    minutes (#791).

### Database connections

Each running frontend opens one `LISTEN` connection for its payout-revision
listener (#795), on its own one-connection pool outside the run pool and its
`PRISM_DATABASE_MAX_CONNECTIONS`. Its session is named
`qbit-prism-revision-wake` in `pg_stat_activity`. So each frontend uses up to
`PRISM_DATABASE_MAX_CONNECTIONS` + 1 connections, + 2 more in dual-writer
mode for its health pool. Raise the database's `max_connections`, or lower
`PRISM_DATABASE_MAX_CONNECTIONS`, by one per frontend before the upgrade, and
keep the reserve for migrations, monitoring, backup and failover
administration
([storage sizing](../docs/prism-storage-sizing.md#local-and-external-database-deployments)).
`LISTEN` needs a session: if `PRISM_DATABASE_URL` goes through a
transaction-pooling proxy, the listener doesn't work and the frontend falls
back to its tick
([pool acquisition](../docs/prism-pool-acquire-metrics.md#source-census-and-exclusions)).

### Metrics

- **Added:** `qbit_prism_post_offer_revision_races_total`
  (`phase=landing,settlement`, `outcome=retried,spent`, #798). No rule fires
  on it.
- **Changed meaning:** for a share append without a block,
  `qbit_prism_database_advisory_lock_wait_seconds{lock="order"}` and
  `qbit_prism_database_order_lock_hold_seconds{holder="append"}` record the
  server's own measures from `qbit_prism_append_share`, without the lock
  statement's round trip (#802). `qbit_prism_rejections_total` counts
  commit-gate contention only when it outlasts the acknowledgement deadline
  (#788).
- **Renamed or removed:** none
  ([metrics](../docs/prism-native-metrics.md)).

### Alert rules

- **#793 (#757)** shortens five UIDs in `docs/prism-native-alert-rules.json`.
  Titles, labels and expressions are unchanged:

  | Rule | Former UID | UID |
  | --- | --- | --- |
  | PrismPoolResubmissionRejectRatioHigh | `qbit-prism-pool-resubmission-reject-ratio` | `qbit-prism-pool-resubmit-reject-ratio` |
  | PrismBlockCandidateLandingFailedCritical | `qbit-prism-candidate-landing-failed-critical` | `qbit-prism-candidate-landing-failed-crit` |
  | PrismBlockOfferStandbyUnconfirmed | `qbit-prism-block-offer-standby-unconfirmed` | `qbit-prism-offer-standby-unconfirmed` |
  | PrismDatabaseAdvisoryLockFailuresCritical | `qbit-prism-database-advisory-lock-failures-critical` | `qbit-prism-advisory-lock-failures-crit` |
  | PrismAcceptedRevisionWorkPendingCritical | `qbit-prism-revision-work-pending-critical` | `qbit-prism-revision-work-pending-crit` |

  Grafana refused the former UIDs, so it never stored a rule under one, and
  there is nothing to delete for them. A deployment that renamed them at
  render time to exactly these UIDs, as qbit-tools' five aliases do, keeps
  its rules and alert state and can drop the renames. One that used other
  names must delete those UIDs when it adopts these: Grafana treats a new
  UID as a new rule, so state history and any silence matching
  `__alert_rule_uid__` start over. Routing and label-based silences are
  unaffected.
- **#800 (#758)** gates `PrismAcceptedRevisionWorkPending` and
  `PrismAcceptedRevisionWorkTrackingUnknown` on
  `max_over_time(qbit_prism_authorized_clients[15m]) > 0`. A deployment that
  gates `PrismAcceptedRevisionWorkPending` itself must turn its own gate off
  when it adopts these rules. For qbit-tools, that is the idle-backup gate
  switch and its render guard, which stops the next re-vendor until they are
  removed. This drops its carry-owner backstop, and an absent miner count
  goes quiet instead of warning.

Both are in
[the alert migration](../docs/prism-alert-migration.md#review-and-apply-the-draft).
The rules read only metrics 3.1.0 exports too, so they need no change if the
binaries roll back.

### API behaviour

`/public/v1/artifacts/<sha>` answers a `no-store` 404 for an audit whose
legacy body or share segment isn't on the host yet, where it answered 500
(#796, #735). The OpenAPI document declares the 404's `Cache-Control:
no-store` and its messages. The API verifies legacy bodies through the
parser #809 fixed, so on a host that holds the 2.x bodies it also stops
answering 500 for the 2.x audit-bundle.v2 bodies of #731.

## Upgrading

- **From 3.1.0:** migrate first, then replace the binaries.
  1. **Migrate while 3.1.0 serves.** Run 3.1.1's `qbit-prism-server migrate`
     once, as a one-off command with a frontend's database settings. It
     applies 032 alone, in one short transaction, and alters no table. The
     3.1.0 frontends keep serving, and restart, throughout: 032 declares no
     capability, so a 3.1.0 binary accepts the database and logs `database
     has schema migrations this server does not know; a later release
     applied them`, naming 32. CI's required `prism-dual-writer` lane runs
     the same check against 3.0.0-rc.6: its R1 scenario runs this release's
     `migrate` beside serving rc.6 frontends and restarts rc.6 at 32.
  2. **Budget the listener's connection** before the first 3.1.1 frontend
     starts ([Database connections](#database-connections)).
  3. **Replace the binary** on every frontend and tool, and on the
     `public-api` service. A 3.1.1 binary refuses the database until 32 is
     recorded. 3.1.1 adds no frontend setting, so the environments stay as
     they are.
  4. **Re-provision the alert rules** ([Alert rules](#alert-rules)).

  **A rolling upgrade is supported as a single writer.** On one database,
  3.1.0 and 3.1.1 frontends can serve together, so they can be replaced one
  at a time. A 3.1.0 frontend never calls 032's functions: it appends with
  its own statements under the same `ORDER_LOCK` and stores each stamp in
  the cluster row, which 3.1.1 reads as its clock's floor, so both keep
  `share_seq` order with rising stamps (#801, #802). The one exception is a
  backward step of the database host's clock while both releases append,
  such as an NTP step or a promotion to a host whose clock lags: a 3.1.0
  frontend can then stamp a share below one 3.1.1 stamped just before. So
  replace every frontend of a database before a promotion to a host whose
  clock may lag
  ([migration 032](../docs/prism-rust-migration.md#migration-032-the-share-append-in-one-server-side-call)).
  Until the last 3.1.0 frontend is gone:
  - the 3.1.0 frontends still write the cluster row on every append (#738);
  - a revision bump by a 3.1.0 frontend sends no notification, and a 3.1.0
    frontend doesn't listen, so either waits for its tick as before (#795);
  - a 3.1.0 frontend's lost post-offer revision race still raises the ALERT
    and goes to reconciliation (#798);
  - only 3.1.1's `public-api` answers the 404s above, only 3.1.1's
    `import-audits` verifies the legacy bodies #806 and #809 fixed without
    sidecars, and only 3.1.1's `self-check` runs its local checks during an
    import (#808).

  **In dual-writer mode,** migrate and replace each node's database and
  frontends as above. 032 adds no table or column, so it changes nothing the
  peer sync copies or compares, and each node's listener hears only its own
  database's bumps. Nothing refuses a 3.1.0 node beside a 3.1.1 peer, but
  no test runs that pair.

  Don't overlap the migrate with heavy disk jobs on the database host, as
  for 3.1.0.
- **From 3.0.0 or 3.0.0-rc.6:** as in the
  [3.1.0 notes](release-notes-3.1.0.md#upgrading), with 3.1.1 in 3.1.0's
  place. Its pre-migrate applies 027, 028 and 032 in one short transaction,
  then builds 031's index online, while 3.0 serves. #791 doesn't change the
  steps: it makes the pre-migrate deadlock-free. 3.1.0's pre-migrate could
  deadlock with a serving rc.6 frontend, and PostgreSQL then aborted one
  side after its 1 s `deadlock_timeout` (#789). 3.1.1's waits at most
  100 ms for each attempt's table locks and tries again. What you may see:
  - the share ledger and `qbit_prism_share_hashes` are held from the start
    of 027 to the commit, not only from their own `ALTER`; in two R1 runs
    027 and 028 committed 372 and 558 ms after `migrate` started, and no
    share waited over 39 ms for its answer;
  - while another transaction holds one of the tables for long, each retry
    queues the frontends behind its request for up to 100 ms, so they see
    repeated short queues rather than one long one or a deadlock;
  - if the locks can't be taken for ten minutes, `migrate` stops with
    `refusing to wait any longer for the table locks of migration 27, 28`,
    changes nothing, and names the holding transactions: end them, or stop
    the frontends, and migrate again.

  3.0.0 and rc.6 append with the same statement as 3.1.0, so while 3.0 and
  3.1.1 frontends serve one database, the clock caveat above applies to them
  too. Budget the listener's connection and re-provision the alert rules, as
  from 3.1.0.
- **From 2.0.x, or from a 3.0.0 candidate before rc.6:** as the 3.1.0 notes
  say, with 3.1.1 in 3.1.0's place. Its `migrate` applies 032 with the rest.

## Rolling back

- **To 3.1.0:** replace the binaries with 3.1.0's, one at a time if need be.
  Nothing needs undoing: 032 stays applied, its functions unused, and 3.1.0
  starts on the database with the warning above. 3.1.0 refuses a newer
  database only through a capability it doesn't know, and 032 declares
  none. 3.1.1 adds no frontend setting, so no environment changes; a 3.1.0
  `self-check` doesn't read `PRISM_SELF_CHECK_LEGACY_IMPORT_PENDING`, and
  stops at audit completeness again while an import runs. A 3.1.0 frontend
  reads only the stored clock and the database's clock, so the rolling
  upgrade's clock caveat applies to its first appends after the rollback
  too. The server fixes above go with the binaries, and the issues they
  fixed return.
- **To 3.0.0 or 3.0.0-rc.6:** as in the 3.1.0 notes'
  [rollback](release-notes-3.1.0.md#upgrading). 3.0 accepts the database at
  32, as at 31, with the same warning. **Never start a 3.0.0 or 3.0.0-rc.6
  binary on a personalised database.**

## Verification

Each fix's tests are in its pull request. For this release:
- CI runs on the version-bump pull request, including the required
  `prism-dual-writer` lane and its R1 pre-migrate scenario, and is
  dispatched on the tag;
- the L3 production-window matrix runs on the version-bump pull request,
  which the tag promotes on a tree match (#608). It is the first `l3-full` on
  a tree with #788 ([#779](#known-issues));
- `cargo deny --locked check` passes, and CI also checks that every
  sqlx-core RustSec advisory is reviewed (#794).

## Known issues

As in the [3.1.0 notes](release-notes-3.1.0.md#known-issues), with these
changes.

**Fixed in 3.1.1:**
- #716 by #788, and with it #724's last intermittent answer;
- #771 by #794, and #713 by #807;
- #762 by #797, and #630 by #798;
- #757 by #793, and #758 by #800;
- #752 by #795;
- #738 by #801, and #711 by #802;
- #709 and #731 by #809;
- #734 by #808;
- the block-proof acknowledgement bound's dropped pooled connection, by
  main's #781 through #786.

3.1.1 also fixes #733 (#806), #735 (#796), #785 (#799) and #789 (#791).

**Still open:**
- **#779, the L3 matrix on 3.0.0's tree:** the
  `throughput-400k-window-2fe-async-3-blocks` preset failed its gate in four
  of six repeats, each with one valid share refused, #716's commit-gate
  class. #788 removes that refusal, but no `l3-full` has yet run on a tree
  with it. #779 also records a steady-state shortfall in one repeat of
  `throughput-400k-window-2fe-sync`, not yet traced to the code or the
  runner.
- **#739:** `backfill-ctv` refuses every legacy CTV set. The cutover skips
  it.
- **#737's open part:** fatal-state clear asks the node one call at a time
  (#690), and the `/audit/carry-forward-integrity` and
  `/audit/ledger-integrity` routes still run under
  `PRISM_DATABASE_STATEMENT_TIMEOUT_MS`, which the report can outlast at
  production size: don't poll them.
- **#712:** the source export is still on the cutover's critical path.
- **#741:** CI runs the admission gate's real-schema tests as a superuser.
  Tooling only.
- The dual writer's known limits, as in the
  [3.1.0-rc.1 notes](release-notes-3.1.0-rc.1.md#known-issues).
- The issues still open from the 3.0.0 candidates, as their notes describe
  them: #695, #602, #622, #670, #688 to #690 and #704.

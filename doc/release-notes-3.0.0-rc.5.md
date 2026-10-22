# qbit-mining-bootstrap 3.0.0-rc.5 Release Notes

Release date: 2026-10-08 (release candidate; tag `v3.0.0-rc.5`, published as a
GitHub pre-release).

This is the fifth release candidate of 3.0.0: 3.0.0-rc.4 plus #737's fixes to
`fatal-state clear` and the carry-forward integrity report (#742, and #744's
migration 026), and two ways to shorten the cutover's outage: serving while
migration 002's share-hash backfill finishes (#743 and #746) and offline index
builds (#745). It also carries the cutover tooling merged after rc.4's tag
(#728, #732, #736 and #740) and a test-only fix (#747). It is the tree that
#291's go/no-go evaluates for production on the PRISM pair. It is not a
production release. Until #291 records a go, the production line stays 2.x.x
(2.0.2).

Everything in the [3.0.0-rc.1](release-notes-3.0.0-rc.1.md),
[3.0.0-rc.2](release-notes-3.0.0-rc.2.md),
[3.0.0-rc.3](release-notes-3.0.0-rc.3.md) and
[3.0.0-rc.4](release-notes-3.0.0-rc.4.md) notes applies, with the changes
below. Unlike rc.4, an upgrade from the previous candidate runs
`qbit-prism-server migrate` first ([Upgrading](#upgrading)).

## Changes since 3.0.0-rc.4

- #737, by #742: `fatal-state clear` accepts a tip that only grew during its
  run, under a bound the operator sets, and the integrity report runs under a
  statement timeout of its own.
  - **Before:** the clear refused whenever the node's tip moved during its
    run, a plain extension included. At production size a clear takes about a
    minute, nearly all of it the integrity report, and tips arrive about once
    a minute, so a drill cleared about one attempt in three. Its bound was a
    fixed 120 s, and the report ran under the session's statement timeout,
    15 s by default, so the clear and `self-check` both needed
    `PRISM_DATABASE_STATEMENT_TIMEOUT_MS=120000`.
  - **F5, a tip that only grew:** the clear walks the node's new best block's
    headers back to the captured height, where it must find the captured tip.
    When the tip has moved by the end of its block and fanout checks, it first
    asks the node each of their questions again. It still refuses, clearing
    nothing, a reorganization, a moved tip while a pool block row lies above
    the captured height, and a tip more than 1,000 blocks up. Each of these
    refusals ends `retry fatal-state clear`. The event's `reconciliation` adds
    `final_tip_hash` and `tip_extended`.
  - **F2, `fatal-state clear --timeout-seconds`** (10 to 3600, default 120)
    replaces the fixed bound, which the recovery must meet before its commit.
    The closing tip check must end 5 s before the bound, so the `UPDATE` and
    the event `INSERT` keep their 5 s. The commit runs outside the bound and
    waits for its outcome, which under `synchronous_commit=remote_apply`
    includes a standby's replay. A clear that runs out of its bound has
    therefore always committed nothing, and the halt stays.
  - **F1, the report's own statement timeout:** in the clear, the time left in
    the bound less 5 s and one node RPC timeout (`PRISM_RPC_TIMEOUT_SECONDS`),
    so PostgreSQL cancels a report that runs out of time before the bound
    ends; in `self-check`, 300 s, in a read-only transaction. Neither command
    needs `PRISM_DATABASE_STATEMENT_TIMEOUT_MS` raised.
- #737, by #744: migration 026 makes `qbit_carry_forward_integrity_report()`
  compute each of its two expensive sets once (F4). It counted and listed each
  separately, so it ran the validator and the drift check twice.
  - At production size the report is estimated at about 29 s, down from about
    57 s. The estimate is the sum of the two checks' EXPLAIN ANALYZE times on a
    production-sized ledger, 20.29 s and 9.14 s, not a timed 026 run. Locally,
    on 400,000 carry rows, it took 1.21 s against 001's 2.30 s.
  - 026 keeps 001's subqueries word for word and evaluates them in 001's
    order, so the report is evaluated as 001's is. Like 025, it changes no
    stored row and declares no capability. The recovery evidence export,
    `scripts/prism-recovery-evidence.sql`, requires it, as startup does.
  - `self-check`, `fatal-state clear` and the import admission gate all wait
    for the report. The gate's header comment still says it takes about a
    minute: the script is pinned by its sha256, so the comment waits for the
    gate's next change.
- #582's cutover window, by #743 and #746:
  `qbit-prism-server migrate --defer-share-hashes` lets frontends serve before
  migration 002's share-hash backfill has finished, and
  `qbit-prism-server backfill-share-hashes` finishes it after go-live.
  - **Before:** no frontend started until the backfill had mapped every legacy
    share's header, which on a production-sized ledger is most of `migrate`'s
    time. So it sat inside the cutover's outage.
  - **The deferred run** maps only the recent range: every accepted legacy
    share within 1,000 template heights, the coinbase maturity, of the highest
    one. It maps them before 013 drops the index their bounds read. It then
    raises the backfill's fence to `share_hash_backfill_pending = 2`, which
    permits serving with migration 2 unrecorded, and each start logs a warning
    naming the backfill's cursor.
  - **Measured on the pair:** in the first round of #291's rehearsal for this
    candidate, the `qbit-prism-server migrate --defer-share-hashes` command,
    on a copy of the production ledger, took 5 min 06 s end to end. It ran on
    3.x.x `539d6c91`, before #745, so 013's and 024's indexes were built
    concurrently.
  - **Why serving is safe:** a native share is credited only on a job whose
    parent is the current tip or, within stale grace, the tip's parent, and
    every copy of a legacy header has the same parent. So a credited native
    share can repeat only a legacy header from the recent range, unless the
    chain reorganizes more than 1,000 blocks below the legacy tip, and the
    append's existing header check refuses that repeat as it would with the
    whole mapping.
  - **`qbit-prism-server backfill-share-hashes [--max-batch 5000]
    [--statement-timeout-ms 2000] [--duty-cycle 0.5]`** maps the rest while
    frontends serve, and records 2. Each batch maps at most `--max-batch`
    `share_seq` values in one statement, in a transaction of its own. A
    statement that outlasts `--statement-timeout-ms`, at most 5,000, is
    cancelled and tried again at half its size, so no statement holds a
    snapshot on the serving primary longer than that (#738). After a batch
    that took `t`, the run rests `t * (1 - d) / d` with no transaction open,
    `d` being `--duty-cycle`: as long again at the default 0.5. Stopped, it
    resumes at its cursor. Plain `migrate` at fence 2 runs the same backfill
    at the defaults, with or without `--offline-indexes`.
  - **Before either records 2,** it checks every native share, under the same
    throttle, and refuses any that repeats the header of an accepted legacy
    share, naming it, and leaves the cursor, the fence and the missing record
    as they are. A failure of the argument above is then a loud stop, not a
    silent double credit.
  - **The record of 2** is one short transaction, which waits at most 2 s for
    the migration lock or the cursor table's lock and keeps to the throttle's
    statement timeout. An attempt that waits longer is rolled back and tried
    again, backing off from 2 s to 30 s, 20 times, about ten minutes in all.
    Then the run stops, naming what held it, with 2 unrecorded, and a rerun
    goes straight to the check and the record. After any failed attempt the
    run first reads whether 2 is recorded, so an attempt that committed but
    lost its reply ends the run as a success.
  - **A rerun is safe:** on a database whose backfill has finished,
    `backfill-share-hashes` maps nothing and succeeds with
    `"already_complete": true`.
  - **At fence 2,** `self-check` reports a `share_hash_backfill` field with a
    warning, never as a failure: `"state": "pending"` with the cursor, or
    `"state": "unknown"` with the reason when it cannot read the backfill. The
    field is absent once the read succeeds with nothing pending. The recovery
    evidence export exports the cursor, as one `share_hashes_deferred` record,
    in place of the partial mapping, and the summary marks `share_hashes`
    deferred. A complete export's summary is unchanged.
  - **While the backfill is pending,** `share-archive restore` refuses at any
    fence, and `detach` and `drop` refuse every share-ledger partition,
    whatever its bounds: the double-credit check reads the native shares
    through the attached ledger, so none may leave it. Each refusal names
    `backfill-share-hashes`, or plain `migrate`, to finish the backfill
    first. `plan`, `seal`, `archive` and `verify` are unaffected.
  - Without `--defer-share-hashes`, `migrate` keeps rc.4's behaviour: the
    whole backfill, then 2 recorded, before anything serves.
- #745: `qbit-prism-server migrate --offline-indexes [--index-build-workers N]
  [--index-build-memory SIZE]` builds 013's and 024's indexes with a plain
  parallel `CREATE INDEX` when nothing else runs against the database.
  - **Before:** on a populated ledger, `migrate` built them with
    `CREATE INDEX CONCURRENTLY`, which reads the share ledger twice and waits
    out every older snapshot, even in the cutover with every frontend stopped.
  - **The offline run** takes each of the two migrations in one transaction.
    Before any DDL it refuses an instance that has not reported drained or
    stopped and a live legacy writer lease. It then locks the tables
    `ACCESS EXCLUSIVE` with a 5 s lock timeout, builds, drops, and records the
    version as it commits. A transaction still open on a table ends the run at
    the lock timeout with nothing changed, and an interrupted migration rolls
    back whole.
  - `N` defaults to 4, at most 1024. `SIZE` is a `maintenance_work_mem` as
    PostgreSQL writes it, such as `2GB`; without it the run takes 2GB, or the
    server's setting when that is higher. A build's sort uses about that much
    in all, divided among its leader and workers.
  - **With `--defer-share-hashes`,** as the cutover's migrate step runs them,
    the recent range is mapped first, before 013 drops the index that serves
    it, and 013 and 024 then build offline with the backfill pending. A gated
    test runs both flags in one run on a populated 2.x.x source, then starts a
    frontend that serves.
  - The plan's estimate is about 3.7 min of outage down to 1.5 to 2 min,
    unmeasured here; the rehearsal's measurement is in the cutover's go/no-go
    record.
  - The flag is `migrate`'s alone. Without it nothing changes, and a frontend
    started with `PRISM_POSTGRES_INIT_SCHEMA=1` always builds concurrently.
- Cutover tooling, and a test fix, merged after rc.4's tag, with no runtime
  change:
  - #728 (#712): `scripts/prism-recovery-evidence.py --jobs N` summarizes the
    recovery evidence in parallel, byte for byte as the serial summarizer
    does. With 8 jobs it ran about 6.5 times as fast on a 3.25 GB export,
    which puts the 45-minute summary of a 65.9M-share restore at an estimated
    7 minutes.
  - #732 (#731): 3.x refuses the window-proof parts digest of every 2.x
    audit-bundle.v2 body, which 2.x hashed in insertion order.
    `scripts/prism_legacy_parts_digest_check.py` lists the rows
    `import-audits` would refuse for it, 9,531 on the cutover drill's restored
    tree, and `scripts/prism_legacy_range_sidecars_sharded.sh` writes their
    canonical sidecars in parallel shards.
  - #736 (#734): `scripts/prism_legacy_import_admission_gate.sh` is a go-live
    gate for frontends that admit traffic while `import-audits` still runs. It
    checks every fact `self-check` checks except two: legacy audit
    completeness, which it reports as pending, and, since #746, the pending
    share-hash backfill, which it doesn't check. Track the backfill with
    `self-check`'s `share_hash_backfill` field or the cursor.
  - #740 (#738): the gate runs its carry-forward integrity check on the hot
    standby `PRISM_INTEGRITY_REPORT_DATABASE_URL` names, or skips it with
    `PRISM_GATE_INTEGRITY=skip`. It refuses a standby that is not in recovery,
    replicates another database or timeline, has `hot_standby_feedback` on,
    serves a writer that waits for its apply, or is stale. Every query runs
    under a 15 s statement timeout and a 5 s lock timeout; the report gets
    120 s on the writer and 600 s on a standby.
  - #747 (#724), test-only: the async-promotion live drill opens its gap
    session on the work the frontend has published, not on the prepared row
    it saves first. Required CI had failed that way three times.

## Upgrading

- **From 2.0.x:** as in the rc.4 notes, with these changes.
  - This candidate requires every native schema migration through 026.
  - **The cutover's migrate step,** with every frontend and tool stopped, is
    `qbit-prism-server migrate --defer-share-hashes --offline-indexes`. It
    maps the recent range and permits serving, builds 013's and 024's indexes
    offline with the backfill pending, and returns saying that 2 is deferred.
    Start frontends only once it has returned.
  - **Its evidence:** the migrated summary marks `share_hashes` deferred.
    Before go-live, compare every other record, with
    `jq 'del(.records.share_hashes)'` on both summaries. Check the share
    hashes once 2 is recorded, with the
    [migration guide's query](../docs/prism-rust-migration.md#recovery-evidence-while-the-backfill-is-pending),
    on a standby with replay paused, never on the primary. Summarize with
    `--jobs` (#728).
  - On a source whose `share_seq` sequence lags behind its rows, the deferred
    run moves the sequence up to the backfill's frozen end and logs that at
    INFO, so a before-and-after comparison of `pg_sequences.last_value`
    differs there by design. A healthy source, a promoted physical copy
    included, is left untouched.
  - Before `import-audits`, list the 2.x v2 bodies it would refuse and write
    their canonical sidecars (#732). To admit traffic while it runs, gate each
    frontend on `scripts/prism_legacy_import_admission_gate.sh` (#736, #740).
    The gate doesn't check the pending share-hash backfill: `self-check`'s
    `share_hash_backfill` field or the cursor shows it.
  - **After go-live, finish the backfill** with
    `qbit-prism-server backfill-share-hashes`, once frontends serve and
    `self-check` passes. Run that `self-check` only while the share rate is
    low, below about 200 shares/s: its integrity report, an estimated 29 s
    since 026, holds one snapshot on the primary (#738). Above that, rely on
    the admission gate, with its report on a hot standby
    (`PRISM_INTEGRITY_REPORT_DATABASE_URL`). Then, for the backfill:
    - Run it at a time of ordinary load. Its defaults suit a primary taking
      up to a few hundred shares a second.
    - Run it where it survives a disconnect, with `RUST_LOG=info` for its
      progress lines. At the defaults it is estimated at three to four hours
      for 65.9M shares, unmeasured beside live load.
    - It keeps to #738's snapshot rule by itself: every statement it runs is
      short and in a transaction of its own. Its writes can still slow share
      appends, so watch the primary's order-lock hold
      (`qbit_prism_database_order_lock_hold_seconds{holder="append"}`) and
      share acknowledgement times while it runs. If they climb, stop it
      (Ctrl-C) and run it again later, or with a lower `--duty-cycle`: it
      resumes where it stopped.
    - Start no `migrate` while it runs: the `migrate` would wait for it.
    - Until it has recorded 2, `self-check` warns, and
      `share-archive restore`, `detach` and `drop` refuse.
    - It ends by printing `"recorded": true`. Running it again afterwards is
      safe.

    See
    [the runbook](../docs/prism-ledger-ops.md#finish-a-deferred-share-hash-backfill-after-go-live)
    and
    [serving with the backfill pending](../docs/prism-rust-migration.md#serving-with-the-backfill-pending-fence-2).
- **From 3.0.0-rc.4: run `qbit-prism-server migrate` before starting any rc.5
  frontend.** It applies 026 in the migration transaction, with no capability
  and no shutdown proof, so rc.4 frontends need not be stopped first: they
  accept 026 with a warning and read the same report.
  - **A binary-only upgrade, as rc.4's was, no longer starts.** At the native
    default, `PRISM_POSTGRES_INIT_SCHEMA=0`, an rc.5 frontend refuses the
    database with `database schema is missing migration(s) 26; …`, naming
    `qbit-prism-server migrate` as the remedy. One started with
    `PRISM_POSTGRES_INIT_SCHEMA=1`, compose's default, applies 026 itself.
  - **The evidence export:** this candidate's
    `scripts/prism-recovery-evidence.sql` requires 026, so it refuses an rc.4
    database that `migrate` hasn't reached yet (`missing required native
    migrations {26} (found …)`). Export such a database with rc.4's script,
    or run `migrate` first.
  - A database that rc.4 served has recorded 2, so `--defer-share-hashes` and
    `backfill-share-hashes` have nothing to do there.
- **Rolling back to rc.4:**
  - **After an upgrade from rc.4, or once 2 is recorded,** an rc.4 frontend
    starts on the database and accepts 026 with a warning. First take
    `--timeout-seconds` off any `fatal-state clear` command line, since rc.4
    doesn't know the flag. rc.4's `self-check` and clear need the statement
    timeout override again, 026 or not (below).
  - **While the fence is 2,** from `migrate --defer-share-hashes` until 2 is
    recorded, no rc.4 binary can start or migrate on the database:
    - an rc.4 start meets the backfill's cursor first, and refuses with
      ``database is not ready: migration 2's share-hash backfill has not
      finished (#582). … Run `qbit-prism-server migrate` to resume it from
      there; …``. **Don't follow that advice with rc.4:** rc.4's `migrate`
      refuses fence 2 too;
    - rc.4's `migrate`, and an rc.4 start with `PRISM_POSTGRES_INIT_SCHEMA=1`,
      refuse with `refusing to migrate a native database at schema migrations
      … before any DDL: database declares share_hash_backfill_pending = 2, but
      this server understands share_hash_backfill_pending 1 to 1 only: …`.
  - **So a rollback to rc.4 from fence 2 has two ways only:** finish the
    backfill with this candidate's `backfill-share-hashes` first, after which
    rc.4 starts as above, or begin again from the pre-migrate backup or
    physical copy of the source, migrated with rc.4, whose outage includes
    the whole backfill. Once frontends have acknowledged a native share, that
    restore loses those shares: it is the D5 boundary, with its accounting
    reconciliation, not an image rollback
    ([the recovery contract](release-notes-3.0.0.md#migration-and-recovery-d5)).
- **Operating rules still in force:**
  - **#737's statement timeout for rc.4's tools.** rc.4's `self-check` and
    `fatal-state clear` run the integrity report under
    `PRISM_DATABASE_STATEMENT_TIMEOUT_MS`, 15 s by default. That is too short
    at production size even with 026, whose report is estimated at about
    29 s. So whenever they are rc.4's, before this candidate's `migrate` has
    applied 026 or after a rollback to rc.4, run them with
    `PRISM_DATABASE_STATEMENT_TIMEOUT_MS=120000`, on that process only and
    never in a frontend's environment. This candidate's run the report under
    their own timeouts.
  - **#738's snapshot rule.** Every share append updates the cluster row inside
    the order lock, and a snapshot held on the primary keeps that row's dead
    versions, so the lock's hold grows while the snapshot lasts. At about
    400 shares/s the order lock saturates after about 18 to 20 s of it. While
    miners are live:
    - hold no snapshot or transaction on the primary for more than about 5 s
      at peak, or about 60 s below 200 shares/s;
    - never set `hot_standby_feedback=on`, and keep heavy write I/O off the
      WAL device;
    - run long reads on a standby with replay paused: the evidence export,
      and, above about 200 shares/s, the integrity report, which holds one
      snapshot for its whole run. The admission gate does that through
      `PRISM_INTEGRITY_REPORT_DATABASE_URL`; otherwise run `self-check` while
      the share rate is low.

    `backfill-share-hashes` keeps to this rule by itself: its statement
    timeout is at most 5 s. The go-live step above says when to run it.
  - **`fatal-state clear --timeout-seconds <10 to 3600>`,** 120 by default,
    sets the clear's bound. The clear refuses with `raise --timeout-seconds`
    when less than 6 s plus one RPC timeout (21 s by default) of the bound is
    left as the report would start. When it exceeds its bound (`fatal-state
    recovery exceeded <n> seconds before its commit`), or its closing tip
    check cannot end 5 s before the bound, nothing was committed and the halt
    stays: rerun, with a larger bound if it ran out of time. Only a connection
    lost during the commit leaves the outcome unknown; then check
    `fatal-state show` and the event table before rerunning. Its three chain
    refusals, `the chain reorganized during fatal-state recovery`, `the tip
    moved and a pool block lies above the captured height` and `the node's
    tip moved <n> blocks during fatal-state recovery`, only ask for a rerun
    once the tip has settled. See
    [fatal-state recovery](../docs/prism-ledger-ops.md#fatal-state-recovery).

## Verification

The runtime changes between rc.4 and this candidate are in `migrate`, the
start gate and operator commands:
- #742, merged as `539d6c91`: `fatal-state clear` and `self-check`'s report,
  in `ledger/fatal_state.rs` and `tools.rs`;
- #743, merged as `d838cfd7`: the deferred backfill, fence 2 and the start
  gate in `ledger/migration.rs` and `ledger/migration/share_hashes.rs`, the
  online runner in `ledger/migration/online.rs`, `ledger/connect.rs`, the
  archive refusals in `ledger/archive.rs`, and `tools.rs`;
- #744, merged as `1a4e353c`: migration 026, its SQL and its registration in
  `ledger/migration.rs`, which adds 26 to the versions every start requires;
- #745, merged as `2d103c32`: the offline index builds in
  `ledger/migration/online.rs`, their options through `ledger/connect.rs` and
  `ledger/migration.rs`, and `tools.rs`;
- #746, merged as `1ede828f`: the throttled finish and the record of 2 in
  `ledger/migration/share_hashes.rs`, `ledger/migration.rs` and
  `ledger/migration/online.rs`, and `backfill-share-hashes` and
  `self-check`'s field in `tools.rs`.

The operator's evidence scripts change with them: #744 makes
`scripts/prism-recovery-evidence.sql` require 026, and #746 makes it,
`scripts/prism-recovery-evidence.py` and
`scripts/prism-recovery-evidence-parallel.py` export and summarize a pending
backfill's cursor as `share_hashes_deferred`.

None of them touches the share append (`ledger/window.rs`), the order lock,
refresh or settlement. So the pair evidence rc.3's notes cite still applies to
the serving path: the D1 peak-second gate, the 750/s ceiling and the failover
drill under load, run 1, all measured on #719's build. What is new on the pair
is the cutover itself, and a backfill running on the primary beside live
load.

The weekly measured mainnet-shaped cutover still runs plain `migrate`, which
now also applies 026 (#744). None of #742 to #746 adds the new flags or
`backfill-share-hashes` to it. So in CI the new paths are proved by their
gated PostgreSQL tests:
- `--defer-share-hashes` and `--offline-indexes` by `share_hash_backfill`'s
  and `index_trim`'s, one of which runs both flags in one run, as the cutover
  does;
- `backfill-share-hashes` by `share_hash_backfill`'s: the throttle, plain
  `migrate` at the default throttle, the record's attempts, the double-credit
  check and `self-check`'s field. The lib's `share_hashes::postgres_tests`
  cover a record whose commit reply was lost and a cursor dropped while it
  is read, and `migration_rollback`'s covers the export at fence 2, with the
  parallel export byte for byte the serial one. `config_cli` and
  `self_check_cli` pin the command line and the reports, and the Python tests
  cover the export scripts;
- the clear by `fatal_state`'s, and 026 by `offer_lifecycle`'s equivalence of
  its report with 001's.

On the pair, #291's rehearsal of this candidate is the proof. Its first round
measured the `migrate --defer-share-hashes` command, on `539d6c91`, at
5 min 06 s end to end, with 013's and 024's indexes built concurrently
(above). The rehearsal's full measurements are recorded in the
cutover's go/no-go record: the recent range, the offline index builds,
`migrate` with both flags, an hour of throttled backfill beside live load,
and a `fatal-state clear` with F5 and 026.

Otherwise the evidence is the same set as rc.4's (#557):
- CI on the version-bump pull request, and dispatched on the tag;
- the L3 production-window matrix on the version-bump pull request, which
  the tag promotes on a tree match (#608);
- the targeted nightly batch: `short-plan-fake-node`, `short-plan-real-node`,
  `soak-smoke`, `faults-short-real-node` and `faults-failover-fake-node`;
- the live regtest weekly scenarios.

rc.1's full nightly set and 5.5 h soak carry over. The evidence bundle on the
`v3.0.0-rc.5` GitHub pre-release lists every run, and #291 links to it.

## Known issues

As in the rc.4 notes, with these changes:

- **Fixed since rc.4:** #737's F1, F2, F4 and F5, by #742 and #744.
- **Closed since rc.4:** #582. Its fix has been in every candidate since rc.1,
  and `migrate` has completed on a production-sized copy at the default
  statement timeout. What remained, the backfill's time inside the outage, is
  what `--defer-share-hashes` takes out.
- **Open since rc.4, not fixed in this candidate:**
  - **#737 (open part):** F3, the clear still asks the node once per pool
    block and once per deep-confirmed fanout, one call at a time (#690). And
    the `/audit/carry-forward-integrity` and `/audit/ledger-integrity` routes
    still run the report under `PRISM_DATABASE_STATEMENT_TIMEOUT_MS`. Since
    026 the report is estimated at about 29 s at production size, so they
    would still time out at the default 15 s: don't poll them.
  - **#738:** every share append updates the `qbit_prism_cluster` row, so a
    long snapshot on the primary slows the append and, at high share rates,
    saturates the order lock. The operating rule above applies. The fix, which
    derives each append's clock from append-only state, comes in a later
    candidate (planned for rc.6).
  - **#739:** `backfill-ctv` refuses every legacy CTV set: 2.x stored each
    set's digest over sorted keys, and 3.x checks it in struct order. The
    cutover skips `backfill-ctv`. A census of a migrated production ledger
    found no set for it to insert, and the frontends' maturity sweep already
    rewrites mature blocks' artifacts. Proposed for rc.6.
  - **#741:** CI runs the admission gate's real-schema tests as the
    database's superuser, not as the frontend's role, which owns the schema
    without being a superuser. A one-off run as such a role passed. Tooling
    only.
  - **#731 and #734, after the cutover:** 3.x still refuses 2.x's v2
    window-proof parts digest, so those bodies import through canonical
    sidecars (#732). And production `self-check` still stops at legacy audit
    completeness while `import-audits` runs, which the admission gate covers
    (#736).
- **Still open from rc.4:**
  - #716's open part and #711 are as the rc.4 notes describe them. #716's
    open part is a share admitted under a tip-change lease and refused, never
    credited, when its commit gate finds the publication authority busy.
  - **#724:** #747 fixes one way the async-promotion live drill failed in CI:
    a gap session that was issued the work published before the gap. Its
    other intermittent answers, `stale-job` and `backend-rpc-unavailable`,
    stay open.
  - **#712:** the parallel export (#721) and the parallel summarizer (#728)
    shorten the evidence step. Taking the source export off the critical path
    is still open, and both exports still sit inside the cutover outage.

# qbit-mining-bootstrap 3.0.0-rc.6 Release Notes

Release date: 2026-10-08 (release candidate; tag `v3.0.0-rc.6`, published as
a GitHub pre-release).

This is the sixth release candidate of 3.0.0: 3.0.0-rc.5 plus #754's fix for
a 3.x regression in which the pool hashrate series timed out on large
ledgers, and #750's fix for #748, a finished share-hash backfill reported as
failed when its record's connection dropped. It also carries a test-only fix
for #724 (#753). It is the tree that #291's go/no-go evaluates for production
on the PRISM pair. It is not a production release. Until #291 records a go,
the production line stays 2.x.x (2.0.2).

Everything in the [3.0.0-rc.1](release-notes-3.0.0-rc.1.md),
[3.0.0-rc.2](release-notes-3.0.0-rc.2.md),
[3.0.0-rc.3](release-notes-3.0.0-rc.3.md),
[3.0.0-rc.4](release-notes-3.0.0-rc.4.md) and
[3.0.0-rc.5](release-notes-3.0.0-rc.5.md) notes applies, with the changes
below. Unlike rc.5, an upgrade from the previous candidate replaces the
binary only: this candidate adds no migration ([Upgrading](#upgrading)).

## Changes since 3.0.0-rc.5

- #754 fixes a 3.x regression where the pool hashrate series,
  `/public/v1/hashrate-series`, timed out on large ledgers. It bounds both
  of the series' ledger reads at both ends, so that, once the rollups have
  run, no plan walks the ledger for them. Every candidate from rc.1 to rc.5
  has the regression: 2.x built the same read with bounded predicates.
  - **Before:** the series takes most buckets from the rollup tables and
    re-reads two kinds of rows from the share ledger: the boundary, which is
    the current bucket and the leading partial one, and the tail, the shares
    past the rollups' watermark. Neither read gave the planner an index
    range it could use. The boundary bounded `accepted_at` with an `OR`,
    plus a `NULL` guard on the range's start. The tail bounded `share_seq`
    only from below, by the watermark, with the same guard on time and an
    `OR` on the miner.
  - **On a production-sized ledger,** 66M shares with the rollups current,
    PostgreSQL walked whole indexes. rc.5's default pool request took 19 to
    40 s. With only the boundary fixed, as #754 first had it, the generic
    plan's tail still walked `qbit_share_ledger_p0_accepted_recent_idx`, 66M
    entries for no row, in 17 to 27 s. And a custom plan for a miner walked
    the miner's whole history. So pool hashrate-series requests answered
    504, past the public API's read deadline, and big miners' charts were
    slow. PRISM itself answers a read that outlasts
    `PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS` with 503 `read_timeout`.
  - **Now,** in `dashboard_hashrate_rollups.sql`, the boundary is two
    disjoint slices, each bounded by `accepted_at` at both ends: a range
    scan on `qbit_share_ledger_accepted_recent_idx`, or on the miner index
    for a miner. `GREATEST` and `LEAST` drop a missing range start or first
    full bucket, and without a range start the leading slice is empty. The
    tail takes its shares from a subquery that bounds `share_seq` on both
    sides, by the watermark and by the ledger's `max(share_seq)`, one
    backward probe per partition. `OFFSET 0` fences it, so the time and
    miner tests only filter its stream, and it reads only the shares past
    the watermark: none while the rollups are current, the lag while they
    catch up.
  - **Measured** on a 6M-share copy of a 2.x ledger migrated to the native
    schema, with a 600,000-share miner added: every request ran in 1 to
    17 ms, under generic, custom and auto plans, at `random_page_cost` 4
    and 1.1, for the pool, a small miner and the big miner, with and
    without an anchor. On that copy, at `random_page_cost` 1.1, rc.5's
    default request had taken 1,957 ms under the generic plan and 346 ms
    under a custom one. On a production-sized ledger, 66M shares, the fixed
    query took 9 to 23 ms for the pool and 2 to 15 ms for miners in all 20
    runs, across generic, custom (with and without an anchor) and auto
    plans. rc.5's query took 15.8 to 25.2 s on the same requests, and the
    outputs were identical in 7 of 7.
  - **The output,** compared in 312 cases, matched rc.5's: every allowed
    range and bucket pair, the pool, two miners and an unknown miner, a
    smoothing context of 0 or 2 h, with and without an anchor, and the
    watermark current, 200,000 shares behind or without a progress row. The
    exceptions were unanchored cases whose range start slid while the slower
    old query ran, which differed by one share, in the leading bucket only.
  - **By design, the tail's cost follows the rollups' lag.** With no
    progress row, before the rollups first run, it reads every share, as
    rc.5's did. Alarm on the lag,
    `qbit_prism_hashrate_rollup_watermark_lag_seconds`.
  - It is a read-only change to the public read path: no migration, no
    schema change and no write. It applies wherever `/public/v1` is served,
    on the `public-api` service and on each frontend's coordinator listener.
    The raw fallback, `dashboard_hashrate_series.sql`, read only when the
    rollup tables are absent, is unchanged; both the 2.x and the 3.x
    schemas have them.
- #748, by #750: `backfill-share-hashes`, and plain `migrate` at fence 2, no
  longer report a finished backfill as failed when the record of 2
  committed but its connection was lost, as long as a fresh connection can
  read the database in time (below).
  - **Before:** after a failed attempt to record 2 while frontends serve,
    the run read whether the attempt had recorded 2 all the same, on the
    attempt's own connection. When that connection had dropped, and its
    `COMMIT`'s reply with it, the read failed too, so a run whose `COMMIT`
    had recorded 2 stopped and asked to be run again. That was safe, since
    the rerun finds 2 recorded and says so, but it reported a finished
    backfill as failed. The rc.5 notes' "an attempt that committed but lost
    its reply ends the run as a success" held only while the attempt's
    connection survived.
  - **A lost connection** is one on which the attempt, or the read after it,
    failed with a transport, TLS or protocol error, one the server ended
    (SQLSTATE class 08, 57P01 to 57P05 or 25P03: a termination, a crash or
    restart, a dropped database, an idle timeout), or one on which the
    attempt got no reply within its own deadline. The read then runs on a
    fresh connection from the same URL, search path and TLS, under the
    record's 2 s lock timeout and the throttle's statement timeout. While the
    server turns it away, it connects again after 250 ms, doubling to 2 s,
    for up to 14 s at the defaults and 17 s at the 5 s cap. With 2 recorded
    and the cursor gone, the run succeeds. If the fresh read fails too, the
    run stops, naming each failure: on a dead network, for one, it gives up
    at that deadline and stops as rc.5's run did.
  - **Each attempt's own deadline** is its ten statements' statement and
    lock timeouts plus 5 s: 45 s at the defaults, 75 s at the cap. The
    server's own timeouts end a healthy attempt well within it, so an
    attempt still unanswered is on a half-open connection, a network
    partition say, whose `COMMIT` would otherwise wait out TCP's
    retransmissions for some 15 minutes. It is abandoned, and its connection
    taken as lost.
  - **A lost connection that shows 2 unrecorded stops the run,** rather than
    a retry, since the runners' lock, a session lock on that connection,
    went with it: `refusing to try recording migration 2 again: an attempt
    lost its connection, and on a fresh one 2 was not recorded when
    checked. …`, or `… an attempt failed, and the connection was lost while
    checking whether it had recorded 2 (…); on a fresh one 2 was not
    recorded when checked. …`. A `COMMIT` still in flight there, waiting for
    a synchronous standby say, can land moments later. Run
    `backfill-share-hashes` again: it takes the lock, then finds 2 recorded
    and says so, or goes straight to the double-credit check and the record.
  - The batches, the throttle, the double-credit check, and the number of
    record attempts and their backoff are rc.5's. So are the record of 2
    before anything serves, at fence 1, and `migrate --defer-share-hashes`,
    which leaves 2 unrecorded.
- #753 (#724), test-only: the async-promotion live drill's new-tip shares
  wait for work at the payout revision the final block's confirmation set.
  - The drill's last check sends a share from each surviving session on
    work for the tip the final block moved to, and requires it accepted and
    credited once. It took the first job built on the new tip as that work.
    The block moves the payout revision twice on that tip, though: when a
    frontend first records the tip, and when the block's landing confirms
    it. Work published between the two is superseded once the confirmation
    commits, and its frontend answers a share on it `stale-job` until it
    republishes (#752). Required CI had failed that way twice.
  - Each session now waits for a job on work at the ledger's payout
    revision, passing over jobs on superseded work or without their records.
    The first job on superseded work the drill meets gets one share, which
    must be refused, `stale-job` or, for a job retired past its retention,
    `unknown-job`, and leave no ledger row: the original symptom, kept as a
    check. One 30 s deadline bounds every step of the phase, share
    submissions and ledger reads included.

## Upgrading

- **From 2.0.x, or from 3.0.0-rc.4:** as in the
  [rc.5 notes](release-notes-3.0.0-rc.5.md#upgrading), with this candidate
  in rc.5's place. That covers `migrate` for 026, which every start of this
  candidate requires, as rc.5's does; the cutover's
  `migrate --defer-share-hashes --offline-indexes`, and finishing the
  backfill after go-live with `backfill-share-hashes`; rolling back to rc.4
  at fence 2; and the operating rules still in force: #737's statement
  timeout for rc.4's tools, #738's snapshot rule and
  `fatal-state clear --timeout-seconds`. From an earlier candidate, the
  upgrade sections of each later candidate's notes apply in turn.
- **From 3.0.0-rc.5: no migration; replace the binary on every frontend and
  tool, and on the `public-api` service.** This candidate requires the same
  native schema migrations as rc.5, 2 to 26, declares the same capabilities
  and understands the same share-hash fence values, 1 and 2. It adds no
  setting and changes no evidence or gate script.
  - **A backfill that rc.5 deferred,** at `share_hash_backfill_pending = 2`,
    is served and finished as before: this candidate's frontends start
    beside it, each logging the cursor, and its `backfill-share-hashes`
    resumes at the cursor. An rc.5 run still going can finish, or be stopped
    (Ctrl-C) and run again with this candidate.
- **A cutover that pinned rc.5 and hasn't gone live** can take rc.6 before
  or after its `migrate` step. A database that rc.5's `migrate` migrated,
  with or without `--defer-share-hashes` and `--offline-indexes`, is served
  by rc.6 unchanged: there is no migration for it, no new capability, and
  the same settings give the same configuration fingerprint. So start
  rc.6's frontends on it with no second `migrate`, and finish the backfill
  after go-live with rc.6's `backfill-share-hashes`, as the rc.5 notes
  describe. rc.6 runs the cutover's
  `migrate --defer-share-hashes --offline-indexes` as rc.5 does, since #750
  changes only the record of 2, which that run defers. The evidence and
  admission-gate scripts are rc.5's, byte for byte.
- **Rolling back to rc.5:** replace the binary, at fence 2 too; nothing else
  changes. rc.5 serves the same database, and its `backfill-share-hashes`
  finishes a pending backfill. With it, #748 returns: after a lost
  connection, its run can stop and ask to be run again when its `COMMIT`
  had recorded 2, and the rerun then succeeds with
  `"already_complete": true`; and an attempt on a half-open connection can
  again wait out TCP's retransmissions, some 15 minutes. The pool hashrate
  series times out again on a large ledger.

## Verification

The runtime changes between rc.5 and this candidate are in the public read
path and in the record of 2 at fence 2:
- #754, merged as `da02b1cc`: the pool hashrate series' two ledger reads, its
  boundary and its tail, in `api/queries/dashboard_hashrate_rollups.sql`;
- #750, merged as `97bcd7e3`: the read after a failed record of 2 and each
  attempt's deadline, in `ledger/migration/share_hashes.rs`, with its two
  callers in `ledger/migration.rs` (`backfill-share-hashes`) and
  `ledger/migration/online.rs` (plain `migrate`). It also updates the
  migration guide's record paragraph and refusal table, and the ledger
  runbook.

#753, merged as `c8efdaef`, changes only the live
drill's support code, `tests/support/live_pg_async_live_miners.rs` and
`tests/support/live_share_client.rs`.

None of them touches a migration, a capability or a setting, the share
append (`ledger/window.rs`), the order lock, refresh or settlement, and
#754 changes one read-only public query. So the pair evidence rc.3's notes
cite still applies to the serving path: the D1 peak-second gate, the 750/s
ceiling and the failover drill under load, run 1, all measured on #719's
build. For #291's rehearsal of rc.5: this candidate runs the cutover's
`migrate` as rc.5 does, and of the backfill's finish only the read after a
failed record of 2 and each attempt's deadline are new.

In CI, the changes are proved by their gated tests:
- #754 by `api_2x`'s: the rollup series must equal the raw ledger series in
  78 cases, every range and bucket pair, context, anchor and subject,
  before the rollups run, at a current watermark and with shares past it.
  And on 20,000 rolled-up shares, half of them one miner's, five requests,
  the pool's, a small miner's and the big miner's, anchored or not, run
  under both plan modes and at `random_page_cost` 4 and 1.1, with the
  rollups current and again after 300 shares land past the watermark. Each
  plan may read at most 1,000 ledger shares, plus the lag in the second
  run, counting filtered and recheck-dropped rows; must bound every ledger
  index scan outside a `Limit` probe on one column from both sides; and
  must run no sequential scan over ledger rows. It fails against rc.5's
  query and against the boundary-only fix;
- #750 by the lib's `share_hashes::postgres_tests`, against a real
  PostgreSQL, most through the crate's execution proxy: a `COMMIT` that
  completes before its connection drops, fresh connects refused and then
  told 57P03, a reply held past the attempt's deadline, a drop before the
  `COMMIT`, a connection lost while checking a failed attempt, and a reply
  held before any `COMMIT`, whose lost connection is dropped, never closed.
  Unit tests pin which errors read as a lost connection, and the deadlines;
- #753 by the drill itself, in the live regtest suite that required CI
  runs.

Otherwise the evidence is the same set as rc.5's (#557):
- CI on the version-bump pull request, and dispatched on the tag;
- the L3 production-window matrix on the version-bump pull request, which
  the tag promotes on a tree match (#608);
- the targeted nightly batch: `short-plan-fake-node`, `short-plan-real-node`,
  `soak-smoke`, `faults-short-real-node` and `faults-failover-fake-node`;
- the live regtest weekly scenarios.

rc.1's full nightly set and 5.5 h soak carry over. The evidence bundle on the
`v3.0.0-rc.6` GitHub pre-release lists every run, and #291 links to it.

## Known issues

As in the rc.5 notes, with these changes:

- **Fixed since rc.5:**
  - **#748, by #750:** rc.5's `backfill-share-hashes`, and plain `migrate`
    at fence 2, could report a finished backfill as failed when the
    record's connection dropped (above).
  - **#724, on the test side:** #753 fixes the async-promotion live drill's
    new-tip shares, as #747 fixed its drill race in rc.5. The product side
    of the new-tip `stale-job` is #752, below.
- **Open since rc.5, not fixed in this candidate:**
  - **#752, after the cutover:** the product behaviour behind #724's
    `stale-job`, a burst of `stale-job` rejections after each pool block.
    The block moves the payout revision twice on its new tip: when a
    frontend first records the tip, and when the landing confirms the
    block. Work a frontend publishes on the new tip between the two is
    superseded once the confirmation commits, and the frontend refuses
    shares on it, and on anything else it still publishes, as `stale-job`
    until it republishes on the confirmed revision: the landing frontend at
    once, the others at their next refresh tick (`PRISM_BLOCKPOLL_SECONDS`,
    2 s by default). So miners see slightly more `stale-job` rejections
    around the pool's own blocks than on 2.x, which invalidates in-flight
    work once per pool block, most of them on the frontends that didn't land
    the block. Nothing is credited wrongly and no acknowledged share is
    lost. `qbit_prism_stale_job_rejections_total{cause="payout_revision"}`
    around each pool block shows them.
- **Still open from rc.5:**
  - **#738, after the cutover:** as the rc.5 notes describe it, under the
    same operating rule. Its fix, which derives each append's clock from
    append-only state, is now planned after the cutover, as the first
    capacity change after go-live, not for rc.6 as rc.5's notes said. At
    production's current share rates it would change nothing measurable,
    and shipping it before go-live would reopen the throughput
    qualification, since it changes the money path's time semantics on the
    hot path. The issue lists what would bring it forward, the share rate
    and the order lock's load among them, and what to watch until then.
  - **#739:** as the rc.5 notes describe it. It was proposed for rc.6 and is
    not in this candidate. The cutover still skips `backfill-ctv`.
  - **#724:** its other intermittent answer, `backend-rpc-unavailable` to a
    lost acknowledged share's replay after the promotion, #716's commit-gate
    class, stays open, and with it the issue.
  - #737's open part, #741, #731 and #734, #716's open part, #711 and #712
    are as the rc.5 notes describe them.

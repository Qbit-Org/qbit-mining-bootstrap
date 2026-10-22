# qbit-mining-bootstrap 3.0.0 Release Notes

Release date: 2026-10-22 (tag `v3.0.0`, cut from the `3.0.x` branch).

3.0.0 is 3.0.0-rc.6 plus #770's fix for #765, in which `import-audits` held
`SETTLEMENT_LOCK` while it sent and stored each legacy audit body, so a large
body stalled settlement and template refresh on the frontends serving beside
it, and #760's fix for #759, in which the node RPC client reused an idle
connection past qbitd's own idle close. Those two pull requests were prepared
as 3.0.0-rc.7 (#772), which was never tagged. `3.0.x` branches from `3.x.x`
at #770's merge, `4c134913`. `3.x.x` continues as 3.1 from 3.1.0-rc.1, which
adds the dual writer and migrations 027, 028 and 031; 3.0.0 has none of them.
#291 records the cutover qualification and the go/no-go for the production
rollout.

Everything in the [3.0.0-rc.1](release-notes-3.0.0-rc.1.md),
[3.0.0-rc.2](release-notes-3.0.0-rc.2.md),
[3.0.0-rc.3](release-notes-3.0.0-rc.3.md),
[3.0.0-rc.4](release-notes-3.0.0-rc.4.md),
[3.0.0-rc.5](release-notes-3.0.0-rc.5.md) and
[3.0.0-rc.6](release-notes-3.0.0-rc.6.md) notes applies, with the changes
below; see also the
[development-line status](release-notes-3.x.x-development-line.md). Like
rc.6, an upgrade from the previous candidate replaces the binary only: 3.0.0
adds no migration ([Upgrading](#upgrading)).

## Changes since 3.0.0-rc.6

- #770 (#765): `import-audits` writes each legacy audit before it takes
  `SETTLEMENT_LOCK`, holds the lock only to check the write guard and
  commit, and bounds every phase of a row's transaction.
  - **Before:** for each row, rc.6 took `SETTLEMENT_LOCK`, checked the write
    guard, and only then encoded the row's UPDATE and sent it: the canonical
    body as `bytea`, beside the audit's commitment and witness Merkle leaves
    as JSON. The lock was held while the client encoded and sent the
    statement and while the server stored it. Nothing bounded the send: the
    session's `statement_timeout` (`PRISM_DATABASE_STATEMENT_TIMEOUT_MS`,
    15 s by default) starts only once the whole statement has arrived. In
    #765, a legacy audit with a ~112 MB canonical body held the lock for
    about 90 s, until it was killed, while the server waited for the rest
    of the statement (`idle in transaction`, waiting on `ClientRead`).
    Every other `SETTLEMENT_LOCK` user waited behind it: the frontends'
    advisory-lock waits timed out, template refresh was deferred, and
    settlement would have waited too. `docker stop` didn't stop it either:
    in a container the command runs as PID 1, which ignores a SIGTERM it
    has no handler for.
  - **Now, per row:** the body is loaded, verified and canonicalized, and all
    nine UPDATE parameters are encoded, on one blocking thread before any
    transaction opens. The UPDATE is then sent and run on the row's own
    connection, holding only that row's lock. Only then does the importer
    poll for `SETTLEMENT_LOCK` with `pg_try_advisory_xact_lock`, every 25 ms
    and never queued, check the write guard again under it, and commit.
    Every other `SETTLEMENT_LOCK` holder still sees a row committed before
    it took the lock, or not at all. The write guard also runs before the
    write, so a halted cluster or a live legacy writer is refused before
    anything is sent.
  - **A lock holder that needs the row goes first.** A poll that misses the
    lock also walks the lock holder's blockers (`pg_blocking_pids`, then
    theirs in turn). If the holder waits on this row, directly or through
    another session, the row is rolled back so the holder can proceed, and
    the command fails naming the row. Other sessions waiting on the row,
    such as a second import, just wait.
  - **Bounds:** two new settings, read by `import-audits` only.
    - `PRISM_IMPORT_AUDITS_WRITE_TIMEOUT_MS` (60000; 1000 to 600000) is the
      write's transaction-local `idle_in_transaction_session_timeout`,
      `statement_timeout` and `lock_timeout`. So the server ends a session
      that stops sending mid-statement, and cancels an UPDATE that runs, or
      waits for the row, that long.
    - `PRISM_IMPORT_AUDITS_LOCK_TIMEOUT_MS` (5000; 100 to 600000) bounds the
      wait for `SETTLEMENT_LOCK`. Once the lock is held, it is also the
      transaction's idle and statement timeout, so a client or statement
      that stalls under the lock loses its session, and the lock, within
      that long.
    - The row's connection also sets `client_connection_check_interval` to
      1 s where the server supports it (PostgreSQL 14 or later, on Linux).
      An abandoned row is then rolled back within about a second, even while
      the server is storing it.
  - **A row that exceeds a bound** is rolled back, and the command exits 1
    with an error naming the row, `importing legacy audit <block hash>`, and
    its cause, such as `SETTLEMENT_LOCK stayed busy for 2000 ms; the row is
    rolled back and nothing was stored`. Rows imported before it stay
    imported, and a re-run resumes from it.
  - **SIGTERM or SIGINT stops the command.** It stops before the next row,
    or rolls back a row that has not been granted the lock, and exits 1 with
    `import-audits stopped on request after importing <n> audit bodies; …`.
    A row that holds the lock finishes its commit first, and a stop that
    lands once the last row is in ends as a finished import. A second signal
    exits at once.
  - **Each row it writes prints one line on stderr,** `imported`, or
    `unchanged` for a row that changed after it was read, for example
    `import-audits: block_hash=<hash> imported
    parameter_bytes=block_hash:64,…,canonical_audit_bytes:<n>
    total_parameter_bytes=<n> prepare_ms=… write_ms=… lock_wait_ms=…
    lock_held_ms=… total_ms=…`. stdout keeps the completeness counts and
    `Imported <n> audit bodies`.
  - **Measured** in a release build, over TLS through a TCP proxy, on a
    synthetic legacy row with a 112.2 MB canonical body and a 37.5 MB
    witness-leaves array. rc.6's order held `SETTLEMENT_LOCK` for 3.145 s:
    0.10 s encoding the statement, 3.01 s sending it and the server storing
    it, about 0.15 s of that the send, and 28 ms for the guard and the
    commit. 3.0.0 holds it for 13 ms. With the client's send
    slowed to 16 MiB/s, the hold was 14.9 s against 16 ms. A session taking
    the lock every 20 ms waited at most 3.13 s and 14.9 s beside rc.6's
    order, and 3 ms and 17 ms beside 3.0.0's.
  - **Why #765's send took so long is not proven:** it did not reproduce
    here. The likely cause is #771, below. 3.0.0's `import-audits`
    doesn't depend on it: whatever slows or stops the send now happens
    before the lock and within the write timeout. The client also polls the
    statement every 100 ms, which un-sticks #771's lost wakeup if it occurs.
  - It changes `import-audits` only: the command, its transaction and two
    setting names. There is no migration or schema change, and nothing the
    frontends or the `public-api` service run changes.
- #760 (#759): every node RPC client keeps an idle connection for at most
  20 s.
  - **Before:** the frontends' qbitd RPC client kept an idle kept-alive
    connection for 60 s, and the public API's node client for reqwest's
    default 90 s, while qbitd closes one after its `-rpcservertimeout`, 30 s
    by default. A request written into a connection just as the node closes
    it is never read, and fails after the connection existed: the plain
    `transport failed` error, which nothing retries. On `submitblock` that
    is an unknown outcome, and the block is never offered again. One
    deployment, with `PRISM_CTV_BROADCASTER_POLL_SECONDS=30` (the default is
    10), logged the error from the CTV broadcaster, whose passes ran in step
    with the 2 s refresh, on about one pass in eight to fifteen.
  - **Now** every node client is built by `node_client_builder()`: the
    frontends', the public API's and the load harness's. It keeps an idle
    connection for at most 20 s (`POOL_IDLE_TIMEOUT`), so the client drops
    it before the node can. A compile-time assert keeps the bound below
    qbitd's 30 s default. Relay calls, `submitblock` included, share the
    bounded pool as before.
  - **Operator rule:** keep qbitd's `-rpcservertimeout`, and the idle
    timeout of any proxy between a PRISM process and its node, above 20 s.
    qbitd's default, 30 s, is.
- TLS dependencies, for the RustSec advisories `cargo deny` reports against
  rc.6's lockfile: rustls 0.23.45 to 0.23.46 (RUSTSEC-2026-0346, -0347,
  -0348 and -0349), rustls-webpki 0.103.15 to 0.103.16 (RUSTSEC-2026-0382)
  and webpki-roots 1.0.9 to 1.0.10 (RUSTSEC-2026-0381), in the root and fuzz
  lockfiles. They are patch releases within the versions rc.6 already
  resolved, with no other dependency change. The serving binary's
  PostgreSQL (sqlx) and HTTPS (reqwest) clients use them.

## Upgrading

- **From 2.0.x, or from 3.0.0-rc.4:** as in the
  [rc.5 notes](release-notes-3.0.0-rc.5.md#upgrading), with 3.0.0
  in rc.5's place. That covers `migrate` for 026, which every start of
  3.0.0 requires, as rc.5's does; the cutover's
  `migrate --defer-share-hashes --offline-indexes`, and finishing the
  backfill after go-live with `backfill-share-hashes`; rolling back to rc.4
  at fence 2; and the operating rules still in force: #737's statement
  timeout for rc.4's tools, #738's snapshot rule and
  `fatal-state clear --timeout-seconds`. From an earlier candidate, the
  upgrade sections of each later candidate's notes apply in turn.
- **From 3.0.0-rc.5:** replace the binary, as the
  [rc.6 notes](release-notes-3.0.0-rc.6.md#upgrading) describe for rc.6;
  everything below about rc.6 applies too.
- **From 3.0.0-rc.6: no migration; replace the binary on every frontend and
  tool, and on the `public-api` service.** 3.0.0 requires the same
  native schema migrations as rc.6, 2 to 26, declares the same capabilities
  and understands the same share-hash fence values, 1 and 2. The same
  settings give its frontends the same configuration fingerprint, so they
  can replace rc.6's one at a time. It changes no evidence or admission-gate
  script.
  - **The two new settings are accepted by 3.0.0 binaries only.** rc.6's
    `run` and `check-config` refuse a `PRISM_*` name they don't know in
    production mode (`QBIT_CHAIN` main or mainnet, `QBIT_PRODUCTION` or
    `QBIT_TOOLS_PRODUCTION`), and warn about it otherwise. So pass them only
    to a 3.0.0 `import-audits` run, never through an environment that an
    rc.6 frontend reads.
  - **#760's node rule:** check that qbitd's `-rpcservertimeout`, and the
    idle timeout of any proxy between a PRISM process (a frontend, a tool or
    the `public-api` service) and its node, are above 20 s.
  - **`import-audits` beside serving frontends** no longer needs the guard
    #765 describes, one that kills it with SIGKILL once a settlement waiter
    has waited a few seconds. Its hold is now the guard read and the commit,
    and `docker stop` stops it.
- **Rolling back to rc.6:** first remove the two settings from every
  environment an rc.6 binary will read, then replace the binary; nothing
  else changes. With rc.6, #765 returns: run its `import-audits` beside
  serving frontends only under the guard #765 describes. #759 returns too:
  the node clients keep an idle connection for 60 s again, 90 s on the
  public API.

## Verification

The runtime changes between rc.6 and 3.0.0 are in `import-audits`, in how
long the node clients keep an idle connection, and in the TLS crates' patch
releases above:
- #770, merged as `4c134913`: `import-audits` only, in
  `ledger/migration.rs` (`Ledger::import_legacy_audits_with` and its per-row
  transaction) and `tools.rs` (the command, its two settings, its stop
  signal and its stderr line), with the two names in
  `config/native-settings.txt` and a re-export in `ledger.rs`. It adds
  `tests/import_audits_settlement_lock.rs` and its entries in
  `test/prism-gated-tests.txt`, and updates the server's README, the ledger
  runbook and the pool-acquire census.
- #760, merged as `12f1ae8e`: the node client's idle bound in `rpc.rs`, used
  by the public API's node client (`api.rs`) and the load harness's, with
  the node requirement in the migration guide.

Neither touches a migration, a capability, the share append
(`ledger/window.rs`), the order lock or settlement. #760 changes only how
long each node client keeps an idle connection. So the serving evidence
rc.3's notes cite still applies: the D1 peak-second gate, the 750/s ceiling
and the failover drill under load, run 1, all measured on #719's build.

In CI, the changes are proved by their tests:
- #770 by `import_audits_settlement_lock`'s 13 gated tests. They put the
  importer's connections behind a relay that slows or stalls what the client
  sends, which leaves the server in #765's state, and check:
  - that a session taking `SETTLEMENT_LOCK` every 20 ms beside the import of
    a 107 MB body with a 37.5 MB witness-leaves array waits less than 3 s,
    while the throttled write takes longer than that (rc.6's order fails
    it);
  - that a stall mid-send and a stall under the lock each end within their
    bound, and that a busy lock fails the row within its bound;
  - that a lock holder waiting on the row goes first, directly or behind
    another session;
  - that a stop mid-send, mid-execution, before the grant, between rows and
    after the last row each behaves as above;
  - and, on the real command, its SIGTERM exit and its busy-lock output.
- #760 by `rpc_idle_close`: the real client against a loopback node that
  drops a request arriving after its idle limit, as the race does. With a
  bound below the node's, the next call opens a new connection and is
  answered; with a bound past it, the call fails with the production error.

Otherwise the evidence is the same set as rc.6's (#557):
- CI on the version-bump pull request, and dispatched on the tag;
- the L3 production-window matrix (`l3-full`) on the `v3.0.0` tag push. The
  version-bump pull request targets `3.0.x`, where the matrix doesn't run
  on pull requests, so the tag runs it rather than promoting a pull
  request's run (#608);
- the targeted nightly batch: `short-plan-fake-node`, `short-plan-real-node`,
  `soak-smoke`, `faults-short-real-node` and `faults-failover-fake-node`;
- the live regtest weekly scenarios.

rc.1's full nightly set and 5.5 h soak carry over. The evidence bundle on the
`v3.0.0` GitHub release lists every run, and #291 links to it.

## Known issues

As in the rc.6 notes, with these changes:

- **Fixed since rc.6:**
  - **#765, by #770:** rc.6's `import-audits` held `SETTLEMENT_LOCK` while
    it sent and stored each body, with nothing bounding the send, and ignored
    `docker stop` (above).
  - **#759, by #760:** the node clients kept an idle connection past qbitd's
    idle close, so a call could fail in the close (above).
- **Open since rc.6, not fixed in 3.0.0:**
  - **#771:** sqlx 0.8.6's rustls stream can lose the wakeup of a large write.
    A large statement sent over TLS can then stall, with the server waiting
    for the rest of it, until something closes the connection. It is the
    likely cause of #765's hold. 3.0.0's `import-audits` works
    around it (above), but other large statements from the serving binary
    don't. The fix is upstream, launchbadge/sqlx#4251, which no 0.8.x
    release carries; #771 tracks backporting it.
  - **#762:** a `submitblock` whose deadline,
    `PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS` (1 s by default), passes while
    its connection is still opening is recorded as an unknown offer, not an
    unsent one, so that block is never offered again. It needs no free
    pooled connection when the call starts and a connect slower than the
    deadline, which the 2 s refresh calls make unlikely. #760 leaves relay
    calls on the bounded pool for this reason.
  - **#757:** five alert-rule UIDs in `docs/prism-native-alert-rules.json`
    exceed Grafana's 40-character limit, so provisioning the rule set as
    published stops Grafana from starting. Until it's fixed, map the five
    to short forms when rendering the rules; #757 lists them.
  - **#758:** the two accepted-block revision-work warnings,
    `PrismAcceptedRevisionWorkPending` and
    `PrismAcceptedRevisionWorkTrackingUnknown`, fire after every pool block
    on a frontend that has no miners and doesn't commit the settlements, a
    backup frontend say, and the second latches until that frontend
    restarts. Until it's fixed, silence the two on idle frontends.
- **Still open from rc.6,** as the rc.6 notes describe them: #752, #738,
  #739, #724's other answer, #737's open part, #741, #731, #734, #716's open
  part, #711 and #712.

## Migration and recovery (D5)

Native schema migration is one-way. No down-migrations are provided. Prefer
forward repair or a compatible native image retaining the latest durable state.
Both [migration](../docs/prism-rust-migration.md#recovery-and-rollback) and
[ledger operations](../docs/prism-ledger-ops.md#one-way-migration-and-isolated-restore-reconciliation)
runbooks contain the numbered reconciliation procedure and exact queries.

> Before the first native share is acknowledged, restore the complete
> pre-migration database and artifact backup and restart the pinned old image
> in isolation from the migrated database. After the first native share is
> acknowledged, restoring an older database loses those accepted records.
> Any rollback that discards acknowledged history requires an explicit
> accounting reconciliation and recovery decision; it is not an ordinary
> image rollback. Keep every native frontend stopped while performing an
> isolated restore/recovery operation against its replacement database.

Native `import-audits` is the canonical backfill for recoverable legacy bodies,
including those without a canonical sidecar. It verifies the original audit
digest, trusted ledger key and recorded coinbase. Rehearse it on an isolated
source restore before migrating the source. Production `self-check` fails when
audits lack shared stored bodies or canonical availability; native immutable
snapshots provide canonical reconstruction without a duplicate full byte copy.

The one-shot commands `self-check`, `import-audits`, `backfill-ctv` and
`broadcast-ctv` no longer register a frontend heartbeat (#381). Earlier 3.x.x
builds left a `starting` row under a generated instance ID after every
invocation, which `fatal-state clear` then refused, and overwrote the live
frontend's status when the ID was configured. Rows those builds left behind are
not removed automatically; the fatal-state runbook's investigation still applies
to them.

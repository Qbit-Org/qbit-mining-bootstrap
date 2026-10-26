# qbit-mining-bootstrap 3.1.0 Release Notes

Release date: 2026-10-26 (tag `v3.1.0`, cut from the `3.x.x` branch).

3.1.0 is the production release of the `3.x.x` line. It is 3.1.0-rc.1 plus
two dependency updates for RustSec advisories, and it changes no runtime code,
setting or migration since rc.1. 3.1.0-rc.1 has run the mainnet pair in
dual-writer mode since 2026-10-20.

Against [3.0.0](release-notes-3.0.0.md), which was cut from the `3.0.x`
branch, 3.1.0 adds the 3.1 dual writer and three schema migrations, 027, 028 and 031:
- **The dual writer is off by default.** With `PRISM_DUAL_WRITER` unset, a
  frontend serves as 3.0's does, once the migrations below are applied.
  Turned on, it lets two nodes each write their own PostgreSQL and serve
  miners at once.
- **Every start requires migrations 027, 028 and 031,** whether or not the
  dual writer is turned on. 031 builds its index online, so `migrate` can run
  while 3.0.0 or 3.0.0-rc.6 serves ([Upgrading](#upgrading)).

3.1.0 also contains everything 3.0.0 adds to 3.0.0-rc.6: #770's fix for #765
(`import-audits` writes each legacy audit before it takes `SETTLEMENT_LOCK`),
#760's fix for #759 (node RPC clients keep an idle connection for at most
20 s), and the rustls, rustls-webpki and webpki-roots updates.

Everything in the [3.1.0-rc.1 notes](release-notes-3.1.0-rc.1.md) applies,
with the changes below. They describe the dual writer, its
[new settings](release-notes-3.1.0-rc.1.md#new-settings), the
[migrations](release-notes-3.1.0-rc.1.md#migrations), the
[rollbacks](release-notes-3.1.0-rc.1.md#rolling-back) and the dual writer's
[known limits](release-notes-3.1.0-rc.1.md#known-issues). The 3.0.0
release candidates' notes, from
[3.0.0-rc.1](release-notes-3.0.0-rc.1.md) to
[3.0.0-rc.6](release-notes-3.0.0-rc.6.md), apply as the rc.1 notes say.

## Changes since 3.1.0-rc.1

- #777: the TLS crates move to their first fixed patch releases, with
  `cargo update --precise`:
  - rustls-webpki 0.103.15 → 0.103.16 (RUSTSEC-2026-0382), in the root and
    fuzz lockfiles;
  - webpki-roots 1.0.9 → 1.0.10 (RUSTSEC-2026-0381), in the root and fuzz
    lockfiles;
  - rustls 0.23.45 → 0.23.46 (RUSTSEC-2026-0346, RUSTSEC-2026-0347,
    RUSTSEC-2026-0348 and RUSTSEC-2026-0349), in the fuzz lockfile only; the
    root lockfile already had 0.23.46 in rc.1.
- tokio 1.53.1 → 1.53.3 (RUSTSEC-2026-0386: tokio's safe `AsyncFd`
  constructors violate I/O safety), in the root and fuzz lockfiles, as
  `main` took it in #781. Nothing in the workspace uses `AsyncFd`. Only
  tokio's version and checksum change.
- The version is 3.1.0.

No other dependency changes.

## Upgrading

- **From 3.1.0-rc.1:** replace the binary on every frontend and tool, and on
  the `public-api` service. 3.1.0 adds no migration and no setting, so there
  is nothing to migrate or reconfigure: a `migrate` finds every migration
  recorded and changes nothing. Nothing refuses a mix of rc.1 and 3.1.0
  frontends on one database or across the pair, so they can be replaced one
  at a time. To roll back, replace the binary with rc.1's.
- **From 3.0.0 or 3.0.0-rc.6:** first the pre-migrate, then the binary, then,
  if the pair is to run two writers, the dual-writer cutover.
  1. **Pre-migrate while 3.0 serves.** Run 3.1.0's
     `qbit-prism-server migrate` once, as a one-off command with a
     frontend's database settings, while the 3.0 frontends keep serving. It
     applies 027 and 028 in one short transaction, then builds 031's index
     online, as
     [migration 031](../docs/prism-rust-migration.md#migration-031-the-share-ledgers-origin-index-applied-online)
     in the migration guide describes. A 3.0 frontend serves, and restarts,
     at every step: the three migrations declare no capability, so it accepts
     the database and logs that a later release applied migrations it does
     not know. #773's tests prove this for 3.0.0-rc.6 frontends under load
     (R1), and show the build never rewrites the share ledger's data
     partition (p0); see the rc.1 notes'
     [upgrade from rc.6](release-notes-3.1.0-rc.1.md#upgrading). 3.0.0
     changes neither rc.6's migrations nor its start checks, so the same
     holds for it.
  2. **Then replace the binary** on every frontend and tool, and on the
     `public-api` service, as in the rc.1 notes' steps 2 and 3. A 3.1.0
     frontend or tool refuses the database until 31 is recorded. Pass the
     new settings only to 3.1.0 binaries: a 3.0 `run` or `check-config`
     refuses a `PRISM_*` name it doesn't know in production mode.
     From 3.0.0-rc.6, also follow the rc.1 notes' steps 4 and 5: check
     #760's node rule, and drop #765's `import-audits` guard. From 3.0.0,
     its own upgrade already covered both.
  3. **The dual-writer cutover** is optional; with `PRISM_DUAL_WRITER` off,
     3.1.0 runs as one writer. Each database is personalised with
     `node-identity set --index N` before its first dual-writer start
     ([dual-writer node identity](../docs/prism-ledger-ops.md#dual-writer-node-identity-027)),
     and each grants the peer's pulls the `prism_peer_sync` role with exactly
     the grants in [the peer sync](../docs/prism-ledger-ops.md#the-peer-sync).
     The frontends get the
     [dual-writer settings](../docs/prism-configuration.md#dual-writer-31),
     the carry owner is chosen as in
     [the carry owner and peer settlement](../docs/prism-ledger-ops.md#dual-writer-carry-owner-and-peer-settlement-31),
     and the balancer routes on `/readyz`
     ([dual-writer readiness](../docs/prism-dual-writer-readiness.md#the-balancer)).
     A node rebuilt from a physical copy of its peer is re-personalised
     with `node-identity repersonalise --index N`
     ([rebuilding a node](../docs/prism-ledger-ops.md#rebuilding-a-node-from-its-peer)).
     The cutover itself is the pair's runbook, as the rc.1 notes'
     [upgrading section](release-notes-3.1.0-rc.1.md#upgrading) says.

  Don't overlap the pre-migrate with heavy disk jobs on the database host:
  fsync starvation can push the serving frontends past their 5 s lock
  timeouts.
- **From 2.0.x, or from a 3.0.0 candidate before rc.6:** as in the
  [3.0.0-rc.6 notes](release-notes-3.0.0-rc.6.md#upgrading), with 3.1.0 in
  rc.6's place: its `migrate` applies 027, 028 and 031 with the rest, and the
  cutover's `migrate --offline-indexes` builds 031 offline too. Check
  #760's node rule (the rc.1 notes' step 4) as well. An
  `import-audits` beside serving frontends needs no #765 guard with 3.1.0
  (the rc.1 notes' step 5).

Rolling back from 3.1.0 is as the rc.1 notes'
[Rolling back](release-notes-3.1.0-rc.1.md#rolling-back) describes, with
3.1.0 in rc.1's place. A rollback to 3.0.0 follows its rollback to
3.0.0-rc.6: only from a database that was never personalised, after
removing the dual-writer and readiness settings (3.0.0 knows the two
`PRISM_IMPORT_AUDITS_*` settings). Unlike a rollback to rc.6, #765 and #759
don't return with 3.0.0. **Never start a 3.0.0 or 3.0.0-rc.6 binary on a
personalised database.**

## Verification

Since rc.1, only the lockfiles, the version and docs have changed, so the
rc.1 notes' [verification](release-notes-3.1.0-rc.1.md#verification) applies. For this
release:
- `cargo deny --locked check` passes; on 3.1.0-rc.1's tree with #777 it
  fails on RUSTSEC-2026-0386;
- CI runs on the version-bump pull request, including the required
  `prism-dual-writer` lane, and is dispatched on the tag;
- the L3 production-window matrix runs on the version-bump pull request,
  which the tag promotes on a tree match (#608);
- otherwise the evidence set is rc.1's (#557), and the evidence bundle on
  the `v3.1.0` GitHub release lists every run.

## Known issues

As in the [3.1.0-rc.1 notes](release-notes-3.1.0-rc.1.md#known-issues): no
issue listed there has been fixed since. That covers #771, #762, #757 and
#758, the dual writer's known limits, and the issues still open from rc.6:
#752, #738, #739, #724's other answer, #737's open part, #741, #731, #734,
#716's open part, #711 and #712. In addition:

- **#716 in the L3 matrix (#779):** on 3.0.0's tree, the
  `throughput-400k-window-2fe-async-3-blocks` preset failed its gate in
  four of six repeats, each with one valid share refused, #716's
  commit-gate class. 3.1.0 doesn't fix #716, so the preset can fail the
  same way. #779 also records a steady-state shortfall in one repeat of
  `throughput-400k-window-2fe-sync`, not yet traced to the code or the
  runner.
- **The block-proof acknowledgement bound can drop a pooled connection.**
  A block-bearing submission's disposition poll runs under that bound. When
  the bound expires while the poll is still checking out its connection
  (during the pool's ping of an idle connection, or a new connection's
  setup), the checkout is cancelled partway and the connection is dropped
  with a reply unread. The pool loses that connection, and PostgreSQL logs a
  client reset. The submission's answer is unaffected. `main` fixes it in
  #781 by running each poll in its own task; 3.1.0 doesn't carry that
  change.

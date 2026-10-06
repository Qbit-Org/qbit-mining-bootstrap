# qbit-mining-bootstrap 3.0.0-rc.4 Release Notes

Release date: 2026-10-06 (release candidate; tag `v3.0.0-rc.4`, published as a
GitHub pre-release).

This is the fourth release candidate of 3.0.0: 3.0.0-rc.3 plus #723's
configurable Stratum listen backlog. It is the tree that #291's go/no-go
evaluates for production on the PRISM pair. It is not a production release.
Until #291 records a go, the production line stays 2.x.x (2.0.2).

Everything in the [3.0.0-rc.1](release-notes-3.0.0-rc.1.md),
[3.0.0-rc.2](release-notes-3.0.0-rc.2.md) and
[3.0.0-rc.3](release-notes-3.0.0-rc.3.md) notes applies, with the changes
below.

## Changes since 3.0.0-rc.3

- #723: both Stratum listeners (the primary and the high-difficulty one)
  listen with a backlog of `PRISM_STRATUM_LISTEN_BACKLOG`, default 4096.
  - **Before:** every native listener inherited mio's fixed backlog of 128,
    a silent drop from 2.x.x's 1024. On the pair, N2's C5 run opened 400
    Stratum sessions at once through the load balancer. The accept queue
    overflowed (`TcpExtListenOverflows` 175), the load balancer marked frontend
    A down for about 4 s, and 158 of the 400 sessions spilled to the backup
    frontend and stayed there.
  - **The kernel caps the backlog at `net.core.somaxconn`** in the frontend's
    network namespace. The `PRISM listening` log line now records both
    values, and a warning names the cap when somaxconn is the lower.
  - **The audit and operator HTTP listener and the public read service** now
    listen with a fixed 1024.
  - **`PRISM_STRATUM_LISTEN_BACKLOG` is a supported setting again.** It was
    2.x.x's setting and left the retired list; `check-config` validates it
    (1 to 2147483647).

## Upgrading

- **From 2.0.x:** as in the rc.3 notes. A 2.x.x `PRISM_STRATUM_LISTEN_BACKLOG`
  is honored again, where rc.1 to rc.3 refused it as retired. Unset, it is
  4096.
- **From 3.0.0-rc.3:** no migration; replace the binary on every frontend.
  Keep `net.core.somaxconn` in each frontend's network namespace at least as
  high as the backlog.

## Verification

The only runtime change between rc.3 and this candidate is #723's listener
binding and its setting:
- `listen.rs` (new), `server.rs`, `stratum.rs`, `lib.rs` and
  `api/public_service.rs`;
- the setting's move from `config/retired-settings.txt` to
  `config/native-settings.txt`.

It doesn't touch the order lock, the append, refresh or settlement, so rc.3's
pair evidence (D1, C4 and C10) applies to this candidate. Its own evidence is
the same set as rc.3's (#557):
- CI on the version-bump pull request, and dispatched on the tag;
- the L3 production-window matrix on the version-bump pull request, which
  the tag promotes on a tree match (#608);
- the targeted nightly batch: `short-plan-fake-node`, `short-plan-real-node`,
  `soak-smoke`, `faults-short-real-node` and `faults-failover-fake-node`;
- the live regtest weekly scenarios.

rc.1's full nightly set and 5.5 h soak carry over. The evidence bundle on the
`v3.0.0-rc.4` GitHub pre-release lists every run, and #291 links to it.

## Known issues

As in the rc.3 notes:
- **#716 (open part):** at a tip change under load, a share admitted under a
  tip-change lease can be refused when its commit gate finds the publication
  authority busy. It is never credited, nothing is written and no ACKed
  share is lost. Since #718 it is answered `backend-rpc-unavailable`
  (code 20). It was seen:
  - 4 times in about 149k shares in N2's rc.1 D1 sustained run on the pair
    (500/s for 300 s, 7 tips);
  - once in about 121k in the peak run on #719's build;
  - once in about 150k in rc.2's L3 (3-blocks r2).

  The fix, a credit-preserving retry (#716 fix B), comes after the
  candidates.
- **#711:** improved by fix B (#719). The issue stays open for fix A, and the
  pair's ceiling is still flagged under 788 shares/s.
- **#712:** the parallel export (#721) shortens the evidence export. Taking
  the source export off the critical path, and a parallel summarizer, are
  still open. Both exports still sit inside the cutover outage.
- **#724:** a live regtest test is intermittent in CI.

# qbit-mining-bootstrap 3.0.0-rc.3 Release Notes

Release date: 2026-10-06 (release candidate; tag `v3.0.0-rc.3`, published as a
GitHub pre-release).

This is the third release candidate of 3.0.0: 3.0.0-rc.2 plus #719's shorter
order-lock hold for share appends (#711) and #721's parallel recovery-evidence
export (#712). It is the tree that #291's go/no-go evaluates for production
on the PRISM pair. It is not a production release. Until #291 records a go,
the production line stays 2.x.x (2.0.2).

Everything in the [3.0.0-rc.1](release-notes-3.0.0-rc.1.md) and
[3.0.0-rc.2](release-notes-3.0.0-rc.2.md) notes applies, with the changes
below.

## Changes since 3.0.0-rc.2

- #711, fix B by #719: a share append holds the global ORDER_LOCK for 3
  statements plus COMMIT, down from 9 client round trips. There is no
  migration. N2 measured it on the pair, on a build whose runtime is identical
  to #719's head `82c80bb1`:
  - the D1 peak-second gate now passes, with a window p99 of 898 ms against
    1,000 ms (rc.1: 1,479 ms on the same shape);
  - reconciliation is exact;
  - the mean order-lock hold is 1.154 ms, against 1.473 ms;
  - the ceiling is 750 shares/s in mode (b), flagged as 5% under 788;
  - C4 run 1 passes, with zero acknowledged shares lost.
- #712, by #721: `scripts/prism-recovery-evidence-parallel.py` exports the
  recovery evidence in parallel, byte-identical to the serial export: 24 s
  against 115 s with 8 jobs on a 6M-share copy. It is a script; no runtime
  change.

## Upgrading

- **From 2.0.x:** as in the rc.2 notes. The schema is still migrations 2 to 25.
- **From 3.0.0-rc.2:** no migration; replace the binary on every frontend.

## Verification

The only runtime change between rc.2 and this candidate is #719's share append:
- `ledger/window.rs`;
- `check_writable` and `WRITABLE_COLUMNS` made visible from `ledger/connect.rs`;
- the import in `ledger.rs`.

The evidence is the same set as rc.2's (#557):
- CI on the version-bump pull request, and dispatched on the tag;
- the L3 production-window matrix on the version-bump pull request, which the
  tag promotes on a tree match (#608);
- the targeted nightly batch (`short-plan-fake-node`, `short-plan-real-node`,
  `soak-smoke`, `faults-short-real-node`, `faults-failover-fake-node`);
- the live regtest weekly scenarios.

rc.1's full nightly set and 5.5 h soak carry over. The evidence bundle on the
`v3.0.0-rc.3` GitHub pre-release lists every run, and #291 links to it.

## Known issues

As in the rc.2 notes, with these changes:

- **#716 (open part):** at a tip change under load, a share admitted under a
  tip-change lease can be refused when its commit gate finds the publication
  authority busy. It is never credited, nothing is written and no ACKed
  share is lost. Since #718 it is answered `backend-rpc-unavailable`
  (code 20). Seen once in rc.2's L3 (3-blocks r2: 1 of about 150k at 500/s)
  and about once per 150k on the pair. The fix, a credit-preserving retry
  (#716 fix B), comes after the candidates.
- **Addressed since rc.2:**
  - #711, by fix B in #719. The pair's measured ceiling is above;
  - #712, by #721's parallel export.

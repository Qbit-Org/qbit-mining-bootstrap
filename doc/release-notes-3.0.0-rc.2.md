# qbit-mining-bootstrap 3.0.0-rc.2 Release Notes

Release date: 2026-10-06 (release candidate; tag `v3.0.0-rc.2`, published as a
GitHub pre-release).

This is the second release candidate of 3.0.0: 3.0.0-rc.1 plus migration 025
and the operator tooling for legacy audit bodies. It is the tree that #291's
go/no-go evaluates for production on the PRISM pair. It is not a production
release. Until #291 records a go, the production line stays 2.x.x (2.0.2).

Everything in the [3.0.0-rc.1 notes](release-notes-3.0.0-rc.1.md) applies
(the highlights, the upgrade contract, the two submission holds for a
rehearsal, and how a candidate is verified), with the changes below.

## Changes since 3.0.0-rc.1

- #708: migration 025 checks legacy carry-forward chains per payout program,
  as 2.x keeps the balances, not per case-sensitive label. A miner who
  re-authorized in uppercase split one program into two labels on union's
  ledger: 5,506 false mismatches under the old rule, 0 under the new over
  2,753,849 rows. `self-check` and `fatal-state clear` refused it (#710).
- Legacy audit bodies whose share-segment range digests were written with
  Python's escaped JSON (non-ASCII worker names; 339 on union, heights
  53313-57440) import through canonical sidecars from
  `scripts/prism_legacy_range_sidecars.py` (#715). Run it before
  `import-audits`. #709 makes 3.x accept the escaped form after GA.
- #705: `scripts/prism-recovery-evidence.sql` streams with `FETCH_COUNT`;
  the runbooks pass `-v FETCH_COUNT=10000` (#706).
- The load harness and CI, with no runtime change:
  - the fault phase's read tier lets its scrapes in flight finish before it
    stops `public-api` (#701, #702);
  - the weekly reduced L3 runs #555's session points and #521's mainnet
    shapes (#614);
  - the weekly soak's resident-memory gate is real again, now that #627
    fixed #600 (#714).

## Upgrading

- **From 2.0.x:** as in the rc.1 notes, except that this candidate requires
  every native schema migration through 025.
- **From 3.0.0-rc.1:** run `migrate`. 025 changes no stored row, so it is a
  plain in-transaction migration, with no capability and no shutdown proof.
  An rc.1 frontend accepts the unknown migration with a warning, keeps
  starting and reads the corrected report. Because 011 defines the validator
  that 025 replaces, `migrate` runs 025 again whenever it runs 011.

## Verification

The only runtime change between rc.1 and this candidate is migration 025:
its SQL, and its registration in `ledger/migration.rs`. So its evidence is
lean (#557):
- CI on the version-bump pull request, and dispatched on the tag;
- the L3 production-window matrix on the version-bump pull request, which
  the tag promotes on a tree match (#608);
- a targeted nightly batch: `short-plan-fake-node`, `short-plan-real-node`,
  `soak-smoke`, `faults-short-real-node` and `faults-failover-fake-node`;
- the live regtest weekly scenarios, including the 2.x.x migration lifecycle
  and the measured mainnet-shaped cutover.

rc.1's full nightly set and its 5.5 h soak carry over. The evidence bundle on
the `v3.0.0-rc.2` GitHub pre-release lists every run, and #291 links to it.

## Known issues

As in the rc.1 notes, with these changes:

- **Fixed since rc.1:**
  - #708, by 025;
  - #701, by #702;
  - #600, by #627. rc.1's soak passed every resident-memory row.
- **Still open:** #695, #602, #622, #630, #670 and #688 to #690.
- **Open for #291's go/no-go sweep:** #582 and #604, which are fixed in code.
- **New since rc.1:** #709. Until it lands, legacy bodies with escaped range
  digests need the canonical sidecars above.

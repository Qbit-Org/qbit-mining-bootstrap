# qbit-mining-bootstrap 3.0.0-rc.2 Release Notes

Release date: 2026-10-06 (release candidate; tag `v3.0.0-rc.2`, published as a
GitHub pre-release).

This is the second release candidate of 3.0.0: 3.0.0-rc.1 plus migration 025,
the commit-gate answer fix for #716 and the operator tooling for legacy audit
bodies. It is the tree that #291's
go/no-go evaluates for production on the PRISM pair. It is not a production
release. Until #291 records a go, the production line stays 2.x.x (2.0.2).

Everything in the [3.0.0-rc.1 notes](release-notes-3.0.0-rc.1.md) applies
(the highlights, the upgrade contract, the two submission holds for a
rehearsal, and how a candidate is verified), with the changes below.

## Changes since 3.0.0-rc.1

- #708, fixed by #710: migration 025 checks legacy carry-forward chains per
  payout program, as 2.x keeps the balances, not per case-sensitive label. A
  miner who re-authorized in uppercase split one program into two labels on
  union's ledger: 5,506 false mismatches under the old rule, 0 under the new
  over 2,753,849 rows. Before 025, `self-check` and `fatal-state clear`
  refused that ledger. #710 also changes
  `scripts/prism-recovery-evidence.py`: the summarizer clears the old rule's
  per-label findings, so a 2.x source and its migrated copy summarize the
  same. Use this candidate's summarizer on both sides of a cutover.
- #716, fix A by #718: a share refused at the commit gate because chain-state
  authority was briefly unavailable at a tip change now answers
  `backend-rpc-unavailable` ("current chain state is unavailable"), or
  `backend-database-unavailable` when the ledger database is what failed,
  instead of `ledger-confirmation-failed`. So `PrismShareAppendFailures` no
  longer fires for it. Such a share is still never credited; the
  credit-preserving retry is #716's follow-up.
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
  - the weekly soak's resident-memory gate is real again (#714). #627's fix
    for #600 was already in rc.1, whose soak passed every resident-memory
    row.

## Upgrading

- **From 2.0.x:** as in the rc.1 notes, with three additions.
  - This candidate requires every native schema migration through 025.
  - Run `scripts/prism_legacy_range_sidecars.py` before `import-audits`, so
    bodies with escaped range digests import and its two missing counts
    reach 0.
  - Export the recovery evidence with `FETCH_COUNT`, as the runbooks do.
    Expect the export to take a long time on a production-sized ledger
    (#712).
- **From 3.0.0-rc.1:** run `migrate` before starting any rc.2 frontend. An
  rc.2 frontend refuses a database that lacks 025.
  - 025 replaces a validator function and changes no stored row, so it is a
    plain in-transaction migration, with no capability and no shutdown
    proof.
  - An rc.1 frontend accepts the new migration with a warning, keeps
    starting and reads the corrected report.
  - Because 011 defines the validator that 025 replaces, `migrate` runs 025
    again whenever it runs 011.

## Verification

The runtime changes between rc.1 and this candidate are:
- migration 025: its SQL, and its registration in `ledger/migration.rs`,
  which adds 25 to the versions every start requires;
- #718's commit-gate answer in `coordinator/miner_submit.rs`.

So the evidence is lean (#557):
- CI on the version-bump pull request, and dispatched on the tag;
- the L3 production-window matrix on the version-bump pull request, which
  the tag promotes on a tree match (#608);
- a targeted nightly batch: `short-plan-fake-node`, `short-plan-real-node`,
  `soak-smoke`, `faults-short-real-node` and `faults-failover-fake-node`;
- the live regtest weekly scenarios, including the 2.x.x migration lifecycle
  and the measured mainnet-shaped cutover.

rc.1's full nightly set and its 5.5 h soak carry over:
- **The soak** passed every gate. Its job was marked failed only by the
  preset's "#600 looks fixed" tripwire, which #714 has since removed.
- **`faults-long-real-node`** is not re-run for this candidate. Its rc.1
  runs failed only on the harness artifact #701, which #702 fixes, with every
  functional fault check passing.

The evidence bundle on the `v3.0.0-rc.2` GitHub pre-release lists every run,
and #291 links to it.

## Known issues

As in the rc.1 notes, with these changes:

- **Fixed since rc.1:**
  - #708, by 025;
  - #701, by #702;
  - #716's reject reason and alert, by #718. The credit-preserving retry stays
    open in #716.
  - #600 is closed: rc.1 already had #627's fix.
- **Open since rc.1, not fixed in this candidate:**
  - #711: a share append holds the order lock across about nine client round
    trips, capping the cluster at about 890 shares/s locally and about 300/s
    from the remote frontend on the pair. See #695.
  - #712: the recovery-evidence export took 85 minutes at 65.9M shares, and
    both exports sit inside the cutover outage.
  - #713: `sslmode=verify-ca` still checks the host name, so it behaves like
    `verify-full`.
  - #704: the fenced failover-mid-landing drill test can time out when both
    miners find a block during the held offer (a CI flake).
  - #709: until it lands, legacy bodies with escaped range digests need the
    canonical sidecars above.
- **Still open from rc.1:** #695, #602, #622, #630, #670 and #688 to #690.
- **Open for #291's go/no-go sweep:** #582 and #604, which are fixed in code.

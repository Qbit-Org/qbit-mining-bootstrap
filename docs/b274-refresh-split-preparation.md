# Refresh split preparation for #274

This is executable test preparation, not final #274 acceptance or a production
optimization. Its frozen starting point is PR #397 commit
`1b0d409344c1b99f5f4bd04970890b9b034a9eeb`. The
[2026-09-10 scope update](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/274)
overrides the issue's older incremental-window acceptance text: refresh templates
when transactions change, and rebuild the payout window on new shares, revision
change, or reanchor. Incremental `PayoutWindow` adoption requires separate measured
justification from #264; it is absent here.

## Frozen runtime lifecycle

1. `Coordinator::refresh_once` takes the existing refresh lock and captures
   `begin_compact_build` before RPC/database awaits. It observes the chain,
   validates the template and tip, observes/reconciles accounting, and derives
   network difficulty and template fingerprint. The fingerprint excludes
   `curtime`, `mintime`, `longpollid`, `noncerange`, and `mutable`; transaction
   bytes remain included.
2. The fast path reuses the entire `Prepared` when bundle, fee, fingerprint,
   payout revision, `Prepared.created` age, and template freshness permit it.
   It rechecks tip, revision and readiness before refreshing publication. It
   does not check the accepted-share watermark.
3. Otherwise it acquires build admission and always calls
   `WorkLedger::snapshot(network)`. `Ledger::snapshot` captures anchor, revision,
   highest accepted sequence, and balances under the existing ordering barrier,
   then reads eligible share pages after releasing that barrier.
4. `capture_refresh` moves the snapshot and admission into its blocking owner,
   canonicalizes original balances, creates `WindowRef`, invokes the borrowed
   native builder, derives native audit/manifest hashes and wire work, and
   produces the compact reservation. It drops accepted/count arrays before
   releasing admission. `Prepared` keeps metadata, original immutable inputs,
   and the reference, not the share window.
5. `reserve_fresh_compact` and `lock_compact_publication` reprove tip, original
   publication/readiness identity, payout revision and balance digest after
   awaits. The final publication guard installs the coupled prepared identity,
   tip publication, readiness clock and generation synchronously. Preserve this
   path for both kinds of refresh.

## Minimal proposed production seam (not implemented)

Separate the reusable payout-window input identity and its monotonic anchor
clock from each new template's `Prepared.created`. Put selection immediately
before the current unconditional snapshot acquisition, with the selected inputs
flowing into the original native `capture_refresh` build and existing publication
proof. A transaction-only refresh must create new template/wire/artifact work
from the same original window identity; it must not renew the window clock.

Before either whole-work or window reuse, read an authoritative committed
accepted-share watermark from the ledger, including shares from other frontends.
An in-memory local accepted counter is insufficient. Preserve a coherent anchor,
watermark, revision, prior-balance digest and original builder inputs, with fresh
version/tip checks after waits. A watermark check is metadata I/O, not a window
scan. Reanchor still uses the existing full snapshot; no delta query, batching,
lock-order change or miner protocol change is proposed.

The owner of this seam is an integration decision after PR397's refresh and
cancellation changes settle. A reference alone cannot satisfy `build_body` or
the canonical audit hash, both of which consume accepted rows. If retaining one
frontend snapshot is chosen, explicitly bound that memory, own cancellation and
off-runtime destruction, and keep it out of `Prepared` and persisted payloads.
Do not implement a nominal reference cache that silently rereads all rows on
every template build. Do not substitute another canonical digest for the native
audit bytes. Bootstrap, difficulty/economic input changes, balance freshness,
failed-build cache installation and cancellation need integration review beyond
the narrow triggers covered here.

## Executable scaffolding

`tests/b274_refresh_split.rs` imports the existing
`tests/support/compact_runtime_e2e` fixture unchanged. It runs actual public
Coordinator methods, PostgreSQL, real native bundle generation and compact
storage. RPC uses the existing deterministic fake node. Transaction changes use
syntactically valid legacy transactions with synthetic outpoints; this is not a
node acceptance or miner-wire interoperability test.

The fixture enforces PostgreSQL 16 with `fsync`, `full_page_writes` and
`synchronous_commit` on, creates UUID schemas and ephemeral RPC/proxy ports, and
cleans up its pools/proxy/schema. Only 16 shares are seeded. No load, scale,
global WAL or memory benchmark runs here.

The existing SQL execution proxy counts completed actual share-row responses.
The additional anchor counter matches the frozen snapshot-barrier statement;
update that classifier if the statement changes. Metadata queries and endpoint
probes do not count as full window reads. Unknown/rejected responses fail the
observation. The existing native artifact oracle reads `AsIssued` balances and
uses `canonical_audit_bundle_bytes_from_parts` and `canonical_manifest_bytes`;
this preparation does not redefine either serialization contract.

| Test group | What it exercises | Frozen expectation |
| --- | --- | --- |
| `baseline_397_` | Three transaction changes, native artifact and wire changes, same shares/revision | Passing characterization: 16 share rows and one snapshot per refresh |
| `control_unchanged_` | Unchanged fast reuse preserves the same publication | Pass: zero share rows/snapshots |
| `control_revision_` | Revision bump with identical template and shares | Pass: one full snapshot and new revision/anchor |
| `control_reanchor_` | Real elapsed two-second reanchor interval, unchanged other inputs | Pass: one full snapshot and new anchor |
| `control_tip_` | Tip changes while a completed reservation COMMIT reply is held | Pass: stale refresh errors and old coupled publication remains |
| `pending_transaction_` | Template/wire/artifact changes while retaining original window | Expected failure: frozen code rereads all 16 shares |
| `pending_new_share_` | Other frontend commits a new share with unchanged template/revision | Expected failure: frozen fast path retains watermark 16 instead of 17 |
| `pending_template_churn_` | Churn at one/two seconds, then original four-second reanchor deadline | Expected failure: frozen template publication renews the reanchor clock |

All tests are explicitly `#[ignore]` during preparation, with reasons. Default
CI compiles them and does not execute them. They are not listed in
`test/prism-gated-tests.txt` and are not evidence that a required integration gate
passed. Selecting a probe without a database fails through
`required_database_url`, even if the require switch is absent. The reused fixture
then checks the input again; duplicate `executed` entries for the same identity
are allowed by the gate contract. Once the production change is integrated, remove the frozen baseline,
promote applicable regression/control tests, remove their ignores, and add their
exact identities to that manifest in the same change.

## Reproduction

Use a disposable PostgreSQL 16 cluster. The repository runner starts a private
cluster on a random loopback port when its database URL is unset. It defaults to
durability on; the runtime fixture verifies those settings. The following uses
two Cargo build jobs and two test/runtime threads. Preserve separate logs and
manifests for controls and expected failures.

```sh
mkdir -p tmp/b274-evidence
CARGO_BUILD_JOBS=2 cargo test --locked -p qbit-prism-server \
  --test b274_refresh_split --no-run

env -u PRISM_TEST_DATABASE_URL CARGO_BUILD_JOBS=2 \
  PRISM_TEST_PG_BIN_DIR="$(pg_config --bindir)" \
  PRISM_TEST_GATE_MANIFEST="$PWD/tmp/b274-evidence/controls.manifest" \
  test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server \
  --test b274_refresh_split control_ -- --ignored --nocapture --test-threads=2
```

Repeat the scoped command with `baseline_397_` and its own manifest for the
characterization. Select each `pending_` test explicitly with `--exact` and its
own manifest/log to reproduce expected failures; do not wrap it in an assertion
that turns the failure into a passing test. A manifest `executed` line proves
the input gate allowed execution, not that assertions passed: record the actual
test exit status beside it. No full required-gate or 400k-share performance
claim follows from these runs.

## Remaining integration gates

### Observed preparation results (2026-09-15)

At the frozen production source plus this preparation, the final explicitly
selected manual run executed all eight database scenarios: **five passed and
three failed as expected**, with Cargo exit status **101**. All eight had
`executed` gate-manifest entries. This run was not a passing integration gate.

- Each of the three baseline transaction refreshes returned 16 share rows and
  executed one snapshot anchor barrier; native artifact reconstruction passed.
- Unchanged refresh returned zero share rows and executed zero snapshot barriers.
- Revision and elapsed reanchor each returned 16 share rows and executed one
  snapshot barrier. Revision native artifact reconstruction passed.
- The held-COMMIT tip change left the old publication installed and returned the
  intended tip-change error after reading 16 rows for the unpublished build.
- The pending new-share test confirmed an inserted share at sequence 17, then
  observed zero refresh share reads and a still-published sequence 16.
- The pending transaction test returned 16 rows and one snapshot barrier where
  it requires zero of each; its fresh native artifact check passed before that
  assertion failed.
- The pending lifetime test observed `(rows, snapshots)` of `(16, 1)` and
  `(16, 1)` for transaction changes at one and two seconds, then `(0, 0)` at the
  original four-second reanchor deadline. The required sequence is `(0, 0)`,
  `(0, 0)`, `(16, 1)`.

Compilation, workspace formatting, and the integration environment-read lint
passed. The default target run passed three inherited fixture checks and
explicitly ignored these eight scenarios. An explicitly selected control with
the database URL and require switch both unset failed at the required-input
gate with exit status 101, as intended. The five baseline/control scenarios also passed in a separate scoped
run with exit status zero and five `executed` manifest entries. These are
functional 16-share observations, not timing, allocation, memory, scale, WAL,
real-node acceptance, or full required-suite evidence.

### Before an implementation PR

Settle the production ownership seam against the updated parent; implement the
split without changing original WindowRef/balances/audit contracts; turn the
three expected-red regressions green; cover the additional inputs and failure
ownership above; promote tests with the required manifest; run scoped and
required checks; then complete the review lanes and GitHub review loop before
publishing/merging an implementation PR. This preparation does not close #274.

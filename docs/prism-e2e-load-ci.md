# PRISM end-to-end and load CI lanes (#487)

This page covers each CI lane that runs PRISM end to end or under load. For
each lane it says what the lane proves and what it does not. It also covers
how to dispatch a run, how to read its summary, where the evidence is kept and
how to reproduce a run locally. #487 plans the lanes L0 to L6. This page is
updated as each lane lands (#546). A lane that has not landed has a short
section marked **not yet running**, which names the issue that owns it.

The checked list of scenarios is `test/e2e-scenarios.toml`. For each scenario
it gives the lane that runs it, the issue that owns it, its pass criteria and
whether it runs today. `scripts/check_e2e_scenarios.py` holds that list to
what the workflows run. The manifest names lanes by trigger (`pr`, `nightly`,
`dispatch`, `weekly`, `L6`) rather than by #487's numbers:

| #487 lane | Where it runs today | Manifest lane | Status |
|---|---|---|---|
| L0 existing CI | `ci.yml`, every PR and every push to `main`, `1.x.x`, `2.x.x`, `3.x.x` | `pr` | running, required |
| L1 E2E smoke | `ci.yml`'s `prism-native-postgres` shards | `pr` | running, required |
| L2 nightly load | `prism-load-nightly.yml`: `run`, `live-nightly`, `stratum-fuzz` | `nightly`, `dispatch` | running; repeats, trend and regression rule not yet (#549, #551, #542) |
| L3 production-window matrix | #473's cells as manual presets, by dispatch only | `dispatch` | **not yet running** (#550) |
| L4 real-node scenarios | `live-nightly` variants and the Sunday `live-weekly` job | `nightly` (and weekly, below) | running; the 2,000-wallet case **not yet** (#604, #622) |
| L5 soak and chaos | the Saturday soak (#575) is not L5 | `weekly` | **not yet running** (#556) |
| L6 shipped images | `prism-load-nightly.yml`'s `shipped-images` job | `L6` | running |

A GitHub schedule runs only from the default branch's copy of a workflow, and
only a workflow whose file is on that branch can be dispatched. So each change
to a lane lands on `3.x.x` first and is then mirrored to `main`. The schedules
build and test `3.x.x`. None of the lanes in `prism-load-nightly.yml` is a
required check.

## L0: existing CI

**Runs:** `ci.yml` on every PR, as its merge result, and on pushes to the
long-lived branches (`main`, `1.x.x`, `2.x.x`, `3.x.x`); a push to a feature
branch with no PR runs nothing. The jobs are lint and compile, Python
unit tests in four shards (including `tests/test_check_e2e_scenarios.py`),
`cargo test`, and the four required `prism-native-postgres` shards against a
`postgres:16` service and the pinned regtest `qbitd`. The
`prism-integration-proof` job uses `scripts/check_gate_manifest.py` to show
that every id in `test/prism-gated-tests.txt` ran and did not skip
([prism-integration-test-gate.md](prism-integration-test-gate.md)).

**Proves:** the unit, component and contract tests, and the `live_regtest`
baseline on a real node: two frontends mining, failover, audit and a reorg;
CTV maturity; CPFP recovery. No gated test can skip silently.

**Does not prove:** anything about rate, latency or memory at scale. The
runners have 2 vCPU and the builds are debug.

## L1: end-to-end smoke

**Runs:** gated tests inside the same required shards (#487 decision 2):

- `qbit-prism-load::load_smoke` runs the `pr-smoke` preset: a debug frontend
  with 100 sessions over 20 addresses, 3 tips and 30 s of churn.
- `qbit-prism-load::real_node` runs `real-node-smoke` against a ramped real
  regtest node.
- `live_regtest::weighted_recipients_tests::real_weighted_recipients_pay_exact_pplns_outputs_through_fanout_spend`
  is #524's wallets, mined through to spent outputs.

**Proves:** the harness still drives the server end to end on every change.
Every session gets work on every tip. The ledger reconciles exactly with what
the clients were acknowledged, against both the fake node and a real node. The
weighted payouts are paid and spent on chain.

**Does not prove:** throughput or latency budgets. The build is debug, the
runner has 2 vCPU, and the gates are about correctness, not speed.

## L2: nightly load

**Runs:** `prism-load-nightly.yml` at 02:17 UTC every night. It also runs on
dispatch, and on a PR when a maintainer applies the `run-load` label (#549);
the label runs against the PR's merge result, on `pull_request`. It has these
parts:

- `run`: every preset whose `schedule` is `nightly`
  (`scripts/prism_load_matrix.py nightly`). Each preset runs in its own job on
  the runner it names (8 vCPU today), from release binaries that one `build`
  job made at a commit pinned by the `plan` job. The presets are the D1 20k
  fixture with and without 500 addresses, #473's 400k one-frontend cell,
  #275's tip delivery, the `mainnet-shape-130/650-addresses` shapes, rental
  churn, and the bridging pair `short-plan-{fake,real}-node`.
- `live-nightly`: the opt-in `#[ignore]` real-node variants listed in
  `test/prism-nightly-gated-tests.txt`. They are the qbitd `-reindex` crash,
  the 130-payee weighted recipients, the dense-cadence soak, a full PostgreSQL
  disk, wall-clock jumps, the soak smoke and #553's short set of chain events
  under 2,000 Stratum sessions (L4, below). The job uses the same gate
  manifest check to show each one ran.
- `stratum-fuzz`: the cargo-fuzz Stratum targets for 20 minutes, gated on no
  crash ([prism-stratum-fuzzing.md](prism-stratum-fuzzing.md)).
- `guard` and `report`: a night on which `3.x.x` has not moved is cancelled,
  never passed. A failed scheduled run opens or comments on the one
  `prism-load-nightly-failure` issue.

**Proves:** each preset completes, reconciles exactly, loses no acknowledged
share and has no durability finding. It has zero shortfall in each gated
phase, and meets any tip-delivery budget or #473 D1 rule the preset sets. All
of this is on one named runner class, on one commit.

**Does not prove:**

- **A regression.** Each preset runs once, and there is no trailing baseline
  or noise floor yet. A rate that drifts while its gates hold goes unnoticed.
  The repeats and the runner class are #549 and #542; the trend and the
  regression rule are #551.
- **A D1 verdict** (#487 decision 5). The rates hold only on the runner class
  they ran on, and CI runners are not the rehearsal host (#477).

### Bridging lane (#552)

**Not yet running; owner #552.** The two halves of the pair run as separate
matrix jobs on separate VMs, so their difference includes the VM-to-VM
spread. #552 runs both halves on one runner and reports what the real node
adds.

### Trend, regression rule and evidence promotion (#551)

**Not yet running; owner #551.** The plan is one JSON row per run on an orphan
`ci-evidence` branch, a regression rule against the trailing same-class
baseline, and raw bundles promoted to release assets. See
[Where evidence lives](#where-evidence-lives).

### Runner classes and noise floor (#541, #542)

`.github/workflows/prism-load-runner-probe.yml` (#541) is dispatch-only. It
measures the Blacksmith classes: `pg_test_fsync` where the clusters live, the
build, seeding and run times, peak host memory, shortfall, and the
tip-to-last-notify p99. It runs `short-plan-20k-window-1fe` and
`throughput-20k-window-1fe` on each class, and its collate job writes one
table (artifact `prism-probe-table`).

**Not yet running; owner #542:** at least 10 repeats per class for the
run-to-run and VM-to-VM spread, and the runner class chosen for each lane.

## L3: production-window matrix

**Not yet running; owner #550.** Today #473's other cells
(`throughput-{200k,400k,500k}-window-*`, 16 vCPU) are `manual` presets, and
only a dispatch runs them (manifest lane `dispatch`). #550 makes them a
matrix. The full matrix runs on the version-bump PR, the `v*` tag and the
`release-candidate` label; a reduced matrix runs weekly. Its correctness rows
count toward #291. Its rates are not D1 verdicts.

## L4: real-node scenarios

**Runs today:**

- the nightly `live-nightly` variants above;
- the Sunday 03:43 UTC `live-weekly` job, which runs
  `test/prism-weekly-gated-tests.txt` in release mode: #545's 2.x.x migration
  lifecycle, #575's measured cutover of a mainnet-shaped 2.x.x ledger and
  #553's long set of chain events under 2,000 sessions.

#553 runs #521's fixtures unchanged under a load of 2,000 share-only Stratum
sessions (`tests/support/live_session_load.rs`), which adds its own checks:
every acknowledged share durable, only refusals a correct miner can earn,
and every session given work on each lasting tip within 15 s, with #481's
time to usable work reported per frontend. Nightly, at 100 shares/s:
scenarios 1 and 2, scenario 4's crash case, two frontends on two nodes that
disagree about the tip, a node that loses its peer, 5 minutes of external
tips, and #598's guard (2,000 sessions on one frontend at a 1 s reanchor, in
debug). Weekly, at 400 shares/s: scenario 4's other four cases, scenario 5's
20-minute soak and 45 minutes of external tips.

**Proves:** each listed scenario's own assertions on a real regtest node and
PostgreSQL 16, and that every listed id executed.

**Does not prove:** #521 scenario 7 at 2,000 wallets, which fails on #604 in
debug and #622 in release and runs only by hand until both are fixed. Fault
injection under load is #554.

## L5: soak and chaos

**Not yet running; owner #556:** the two-hour soak with randomised faults,
judged against #291's criteria.

L5 is not the Saturday 05:41 UTC `soak-weekly` job (#575, manifest lane
`weekly`). That job runs one server lifetime of about 5.5 h with no faults. It
is gated on RSS and descriptor trends, connection drift, bounded WAL, zero
lost shares and exact reconciliation. [prism-soak.md](prism-soak.md) has its
gates.

## L6: shipped images

**Runs:** the `shipped-images` job (#544) on the Sunday schedule, on dispatch
(`images: true`), and on a PR into `3.x.x` or a `3.x.x` push that changes a
path in `WATCHED_PATHS` in `scripts/prism_shipped_image_lane.py`. The job
builds the images with `docker compose build` and brings up `compose.yaml` +
`compose.prism-ha.yaml` under the `prism` profile. cpuminer-opt and
`qbit-prism-miner` then mine against it.

**Proves:** the checks in the driver's `CHECKS`:

- both frontends are ready;
- #281 criterion 1 holds across the frontends;
- each client finds a block and its audit bundle verifies;
- the ledger reconciles exactly;
- the public API lists every found block.

**Does not prove:** anything about capacity. The load is a few miners.

## Dispatching a run

Dispatch runs the default branch's copy of the workflow. `ref` picks the
commit that is built and run:

```sh
# The nightly set against a branch (the live variants are on by default).
gh workflow run prism-load-nightly.yml -f preset=nightly -f ref=my-branch

# Named presets only, without the live variants.
gh workflow run prism-load-nightly.yml \
  -f preset=short-plan-fake-node,short-plan-real-node -f ref=3.x.x -f live=false
```

The inputs are:

- `preset`: comma-separated names, `nightly`, `all` (nightly plus manual) or
  `weekly` (the soak);
- `ref`;
- `tip_last_notify_p99_budget_ms` and `max_shortfall`: empty keeps each
  preset's own budget;
- `live`, `weekly`, `fuzz` and `images`: add those jobs.

A one-off configuration is a checked-in preset on a branch, dispatched with
that branch as `ref`. The harness refuses a flag the preset pins if it is given
again, so a result always matches a reviewed file.

To run the nightly set against a PR, apply the `run-load` label. A later push
needs the label applied again. The runner probe is dispatched the same way
(`gh workflow run prism-load-runner-probe.yml -f classes=8,16`).

## Reading a job summary and `gate.md`

A preset job's summary starts with `### <preset> on <runner>` and the commit.
Next comes the gate's verdict, `### <title>: PASS` or `FAIL`, followed by a
table:

| Column | Meaning |
|---|---|
| Check | Every run gets the exit code, lost acknowledged shares, durability findings and a shortfall row per gated phase. A row is added for each gate the preset sets (see the harness README's gate table) |
| Observed | what the side report says, in the unit shown |
| Budget | the preset's budget, or the dispatch override |
| Result | `pass`; `**FAIL**`; or `info` for a row reported but not gated (ungated phases, the D1 burst) |

A gated figure the report could not measure fails its check and gives the
reason. An ungated one reads `unmeasured: <reason>`. Neither is ever shown as
a pass or as zero. The run's exit codes are:

- `harness-exit-code`: 0 means the run completed and reconciled. 2 to 8 each
  have one meaning in the
  [exit-code table](../crates/qbit-prism-load/README.md#exit-codes): 4 is a
  durability loss, 6 an abort, 8 a contradicted premise.
- `gate-exit-code`: 0 means pass and 1 means fail. Anything else (2, or no
  file) means the gate could not reach a verdict. The nightly does not count
  that commit as tested and runs it again the next night.

D1 presets add #473's verdict table. The folded sections hold `host.json`
(runner label, CPU, memory, kernel, and the mount options and write cache of
the cluster's disk) and `pg_test_fsync`. Compare two runs only when both of
these match.

For more detail, the side report `load-harness-report.json` has every phase's
`client_ack_latency`, `shortfall`, `processes` (per-frontend CPU and RSS) and
`reconciliation`. It also has `time_to_usable_work.tips[]` and, for a
real-node run, the `node` block. The harness README's
[Honest values](../crates/qbit-prism-load/README.md#honest-values) section
defines each field.

## Where evidence lives

| Where | What | Kept | Status |
|---|---|---|---|
| Actions artifact `prism-load-<preset>` | the run directory: `load-harness-report.json`, `gate.md`, both exit codes, `host.json`, `pg_test_fsync.txt`, `logs/`, and the soak's samples and report for a soak | 90 days | running |
| `prism-live-nightly`, `prism-live-weekly`, `prism-stratum-fuzz`, `prism-shipped-images` | gate manifests and test logs; fuzz logs and crashing inputs; `l6-report.json` and the Compose logs | 30 days | running |
| `prism-load-tested-commit` | the commit the last nightly carried to a verdict (the guard reads it) | 90 days | running |
| `ci-evidence` branch | one JSON line per run, one file per lane per month | permanent | **not yet; owner #551** |
| Release assets | bundles that must outlive 90 days: every full-suite job, flagged L2 runs, soaks, and any run cited in an issue or doc | permanent | **not yet; owner #551** (the bundle for a release candidate is #557) |

A run cited in [prism-throughput-measurements.md](prism-throughput-measurements.md)
or in an issue should link its run and artifact until #551 can promote it.

## Reproducing a run locally

`.github/scripts/prism-load-run.sh` is exactly what a `run` job executes:

```sh
cargo build --locked --release -p qbit-prism-server -p qbit-prism-load
# Only a real-node preset (--node qbitd) needs this. It exports nothing when
# run like this, so set QBITD_BIN yourself.
bash .github/scripts/install-prism-qbit.sh "$HOME/prism-qbit"
export QBITD_BIN="$HOME/prism-qbit/qbit-1.0.0/bin/qbitd"   # the path it prints
PG_BIN_DIR=/usr/lib/postgresql/16/bin RUNNER_LABEL=local \
  .github/scripts/prism-load-run.sh short-plan-fake-node load-out
```

The script needs PostgreSQL 16 server binaries (`PG_BIN_DIR`). The harness
builds and removes its own cluster under `PRISM_LOAD_TMPDIR`, which defaults
to `/tmp/pload`. Extra arguments go to the harness, and only flags the preset
does not pin are accepted, for example `--allow-dirty-tree` for a modified
checkout. The output directory then has the same files as the CI artifact.
Compare against CI only on a host of the same class; `host.json` says what
yours is.

To reproduce the other jobs:

- **A gated or live test.** An id in the gated lists is
  `<package>::<binary>::<test path>`. Split it the way the workflow does, and
  pass only the test path to the filter:
  `PRISM_TEST_DATABASE_URL=… PRISM_TEST_PG_BIN_DIR=… PRISM_TEST_REQUIRE_INTEGRATION=1 cargo test -p <package> --test <binary> -- --ignored --exact <test path>`.
  Leave out `--ignored` for an id from `test/prism-gated-tests.txt`. Check
  that the output says `1 passed`: a filter that matches nothing also exits
  0.
- **L6.** Run
  `python3 scripts/prism_shipped_image_lane.py prepare|build|run --work DIR [--out OUT]`,
  in that order. The script's docstring lists what it needs.
- **The soak.** See [prism-soak.md](prism-soak.md#running-one).

## Keeping this page current

When a lane lands, the PR that lands it replaces the lane's **not yet
running** section with what it runs, what it proves and what it does not. The
same PR updates the table at the top and the manifest's `[lanes]` entry.

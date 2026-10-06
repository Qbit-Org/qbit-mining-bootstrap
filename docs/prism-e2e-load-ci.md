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
| L2 nightly load | `prism-load-nightly.yml`: `run`, `bridging`, `live-nightly`, `stratum-fuzz`, `evidence` | `nightly`, `dispatch` | running; trend rows and a report-only regression rule with provisional thresholds (#551); repeats not yet (#549) |
| L3 production-window matrix | `prism-load-l3.yml`: `l3-full` on the version bump, the `release-candidate` label, a `v*` tag and dispatch; `l3-reduced` weekly | `L3` (and `dispatch` for the presets) | running on pull requests and tags; dispatch and the weekly run need the file on `main` |
| L4 real-node scenarios | `live-nightly` variants and the Sunday `live-weekly` job | `nightly` (and weekly, below) | running; the 2,000-wallet case **not yet** (#604, #622) |
| L5 soak and chaos | the Saturday soak (#575) is not L5 | `weekly` | **not yet running** (#556) |
| L6 shipped images | `prism-load-nightly.yml`'s `shipped-images` job | `L6` | running |

A GitHub schedule runs only from the default branch's copy of a workflow, and
only a workflow whose file is on that branch can be dispatched. So each change
to a lane lands on `3.x.x` first and is then mirrored to `main`. The schedules
build and test `3.x.x`. None of the lanes in `prism-load-nightly.yml` or
`prism-load-l3.yml` is a required check.

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
- `qbit-prism-load::ctv_settlement` runs the harness with `--ctv-settlement`
  (#548): 400 addresses overflow the direct-output cap, so the landed block
  pays 12 directly and the rest through one CTV fanout chunk, counted in the
  side report's `settlement` block.
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
- `bridging`: the bridging pair again, both on one runner, with the
  real-minus-fake row ([below](#bridging-lane-552)).
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
- `evidence` (scheduled runs only) and `evidence-preview` (a `run-load` PR or
  a dispatch, read-only): the trend rows, the regression rule and evidence
  promotion ([below](#trend-regression-rule-and-evidence-promotion-551)).

**Proves:** each preset completes, reconciles exactly, loses no acknowledged
share and has no durability finding. It has zero shortfall in each gated
phase, and meets any tip-delivery budget or #473 D1 rule the preset sets. All
of this is on one named runner class, on one commit.

**Does not prove:**

- **A regression, as a gate.** The regression rule (#551) compares each
  number with its trailing baseline and files one tracking issue, but it is
  report-only and its thresholds are provisional until #542's measured
  variance is checked in. Each preset still runs once a night; the repeats
  are #549.
- **A D1 verdict** (#487 decision 5). The rates hold only on the runner class
  they ran on, and CI runners are not the rehearsal host (#477).

### Bridging lane (#552)

**Runs:** the `bridging` job, on the nightly schedule, on the `run-load`
label, and on dispatch with `bridging: true`. It runs `short-plan-fake-node`
and `short-plan-real-node` one after the other on one 8 vCPU runner, through
`prism-load-run.sh`. The order alternates with the run number, and the row
records it. The two presets differ only in `--node`: `--plan short`, 2
frontends, 100 sessions, a 20k window at 50 shares/s and 3 external tips.
The matrix still runs each half on its own VM as well.
`scripts/prism_load_bridge.py` then writes `bridge-row.json` (schema
`qbit.prism.load-bridge.v1`) and a table in the job summary. Each row is real
minus fake:

- `client_ack_latency` p50 and p99, per phase;
- time to usable work: the slowest session's time on each external tip, p50
  and max over the tips;
- peak RSS (`VmHWM`) of each frontend and of the frontends summed;
- the pool node's `submitblock` and `getblocktemplate` latency through the
  harness's relay, and the pool qbitd's peak RSS. These exist only on the
  real node. The fake side and the difference read `unknown`, never 0.

A figure either side could not measure is `unknown`, and a footnote gives the
reason. If either side failed its harness or its gate, the row is marked
**Not trusted** and says why, and the job fails once the row and the artifact
`prism-load-bridging` are written.

**Proves:** what the real node costs on this runner class, at this commit,
for this plan, without the VM-to-VM spread that separate jobs include.

**Does not prove:** a D1 verdict or a D1 rate (#487 decisions 1 and 5).
The plan runs at 50 shares/s. Each run is one sample, so a single night's
difference carries the run-to-run noise, which #542 measures.

### Trend, regression rule and evidence promotion (#551)

**Runs:** the `evidence` job, on every scheduled run of
`prism-load-nightly.yml` (the nightly, the Sunday weekly run and the Saturday
soak). It is the only job in the workflow with `contents: write` (with
`issues: write` and `actions: read`). A `run-load` PR and a dispatch get
`evidence-preview` instead, with a read-only token. It applies the same rule
to its own rows against the recorded trend and writes only to the job
summary. Both jobs call `.github/actions/prism-load-evidence`, which the L3
tag-push job will call too once #550's tag hook lands (after #608).

**Trend rows.** Each preset job builds its row on every event with
`scripts/prism_load_trend.py row` and uploads it as the artifact
`prism-load-trend-<preset>` (7 days). Only `evidence` records rows. A row is
one JSON object, schema `qbit.prism.load-trend-row.v1`:

| Field | What |
|---|---|
| `kind` | `preset` (a preset run), `missing` (a planned preset whose job, at its latest attempt, left no row), `job` (a non-preset job's outcome: `bridging` as L2, `live-nightly` and `live-weekly` as L4, `stratum-fuzz` as `fuzz`, `shipped-images` as L6), or `promotion` (an asset promoted after its row was written) |
| `run_id`, `run_attempt`, `run_url`, `event`, `ref`, `commit`, `recorded_at` | the run, the commit its job checked out, and when the row was built (UTC) |
| `lane`, `preset`, `preset_sha256` | L2 for the nightly presets, `weekly` for the Saturday soak (#575, not L5); the preset file's hash, so a changed preset starts a new series |
| `runner_class`, `host` | the runner label and `host.json`'s CPU, memory, kernel, filesystem, write cache and FUA |
| `fsync` | `pg_test_fsync`'s fdatasync ops/s and µs/op, and its band |
| `outcome` | the harness and gate exit codes, `pass`, `fail` or `no verdict`, and the build provenance |
| `headline` | per phase: achieved rate, client ACK p50/p99 (ms, client clock), server ACK mean and bucketed p99 (ms, receipt to response write, including the share append), ORDER_LOCK max and mean waiters, peak frontend RSS (MiB), shortfall, refused valid shares, missing shares; per run: tip-to-last-notify p99 (ms) and durability findings |
| `artifact`, `promotion`, `asset_url` | the run's artifact (name, ID, URL), and its release asset once promoted |

A number the run did not measure is `null`, never 0. A row of another schema
is skipped by every reader, not guessed at.

**The branch.** `ci-evidence` is an orphan branch holding
`trend/<lane>/YYYY-MM.jsonl` (the month of `recorded_at`) and a README. It is
append-only: rows are added, never edited, and a push is never forced. The
first write creates it. `scripts/prism_load_trend.py append` refuses any row
whose event is not `schedule`, or a `push` of a `refs/tags/v*` tag. The
action refuses to write from any other event too. Each attempt fetches the
tip and skips rows already there. The identity is kind, run, attempt and
preset; for a job row it is job, attempt, outcome and commit (each job exports the attempt it ran in). It then commits on top
and pushes, and retries a rejected or unconfirmed push from a fresh fetch, up
to 5 attempts in 300 s. So the nightly and the soak writing at once lose no
row, and a re-run never duplicates one.

**Promotion.** Bundles that must outlive the 90-day artifacts become release
assets:

- the Saturday soak's (`weekly`) bundle, and any L5 run's, always;
- an L2 run's bundle when it is flagged, meaning the rule found a regressed
  number or the gate failed;
- a tag's full-suite bundles (L3), all of them, once the tag hook lands.

Monthly bundles go to the rolling `ci-evidence/YYYY-MM` pre-release, which
is created when missing and never marked latest. A tag's bundles go to that
tag's release. An asset is the run's artifact, fetched by its exact ID and
tarred as `<lane>-<preset>-run<id>-attempt<n>.tar.gz`, so `--clobber` only
ever replaces the same bundle on a retry. The row records the asset URL read
back from the release. A promotion that fails is recorded as `failed` in the
row, and the job fails. A re-run that later promotes it adds a `promotion`
row. To keep a run cited in an issue or a doc, a maintainer runs
`scripts/prism_load_trend.py cite --artifact-id N --lane L2 --preset NAME
--repository Qbit-Org/qbit-mining-bootstrap [--append]`.

**The regression rule** (`scripts/prism_load_regress.py`) is **report-only**.
Its thresholds live in `test/prism-load-regression.toml`, which is marked
`provisional`.

- **Series.** A series is one lane (L2 today), preset and preset sha256,
  runner class and fsync band (`fast` under 1,000 µs/op, `medium` under
  4,000, `slow` above). Only runs whose harness exited 0 count, each at its
  latest attempt.
- **Limit.** A number regresses when it is worse than the median of the last
  14 such runs (at least 5) by more than
  `max(k × spread, min_relative_change × |median|, min_absolute_change)`,
  with `k = 3` and per-number floors.
- **Spread.** The spread is #542's measured coefficient of variation over
  every run (`all.cv` in `scripts/prism_load_probe.py variance`'s document,
  schema `qbit.prism.runner-probe-variance.v1`). It is read from the group
  for the run's own #542 fsync band, or else the class's all-band group, when
  that document is checked in as `test/prism-load-variance.json`. Otherwise
  it is the baseline's scaled MAD, which is provisional.
- **Where #542 does not apply.** #542 measures the rate, the client ACK
  p50/p99 and the tip-to-last-notify p99, so the server ACK, lock waiters
  and RSS stay provisional. A variance document of another schema or version
  is refused by name. When #542's VM-to-VM spread is over 2× its run-to-run
  spread, the summary points at #511's same-VM A/B instead.
- **Commit range.** A regressed number is reported with the range from the
  last good run's commit to the first bad run's. Flagged runs stay out of
  later baselines, so a lasting regression keeps its range. After 14 flagged
  runs in a row, the new level is taken as accepted.
- **Unknowns.** An unmeasured number, a run with no fsync cost, a short
  series, a harness exit other than 0 and a planned preset with no row each
  read **unknown** or **not evaluated**, with the reason, never as within.
- **Where it reports.** The verdict goes to the job summary and the artifact
  `prism-load-regression-verdict` (90 days). On a regression, `evidence`
  opens or updates the one `prism-load-regression` issue. Its body is the
  latest flagged run's table, with the commit range and both runs' numbers
  side by side, and each flagged run adds a comment. It never pages and
  never blocks a merge.

**Proves:** every scheduled run since #551 has trend rows, and PR and
dispatch runs record nothing. It also shows whether a number moved beyond
the (provisional) noise allowance of its own series, and between which
commits.

**Does not prove:** a D1 verdict (decision 5), or a regression gate. The rule
never fails a job. Its thresholds stay provisional until #542's measured
document is checked in and `provisional` is set to false.

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

**Runs:** `prism-load-l3.yml` (#550). The suites are checked in at
`crates/qbit-prism-load/presets/suites.toml`, separate from each preset's
`schedule`, so a preset can be nightly and in L3 at once:

| Suite | Presets | Repeats | Runner | When |
|---|---|---|---|---|
| `l3-full` | #447's matrix as #473 ran it: 200k fe1, 400k fe1/2/4 async, 400k fe2 sync, `throughput-400k-window-2fe-async-3-blocks`, `dense-cadence-400k-window-{1,2}fe-async`, 500k fe1/2/4 | 3 (33 jobs) | 32 vCPU | the version-bump PR, `release-candidate`, a `v*` tag, dispatch |
| `l3-reduced` | 400k at 1, 2 and 4 frontends, async | 1 | 32 vCPU | Monday 06:13 UTC on `3.x.x`, dispatch |

The runner is the 32 vCPU class until #542 picks one per lane. The triggers:

- **The version bump:** a PR into `3.x.x` or `main` that changes `VERSION`,
  from this repository, on every push. The `changes` job checks the PR's file
  list. A trigger-level `paths` filter would also swallow the label event.
- **`release-candidate`:** a maintainer applying the label to a PR into
  `3.x.x` or `main`, including a fork's. As with `run-load`, only the
  `labeled` event starts it.
- **A `v*` tag**, on either line. The tag first looks for the newest finished
  same-repository PR run of `l3-full` whose verdict is complete for the tagged
  commit's tree (`git rev-parse HEAD^{tree}`, recorded by the plan job). If it
  finds one, it promotes it: it carries that run's tables and verdict, links
  the run, and fails if that run failed. Otherwise it runs the full suite
  (`scripts/prism_l3_promote.py`). A lookup that fails reruns the suite. A
  tag reads the workflow file at the tagged commit, so a tag cut before this
  file reached its line starts nothing.
- **Dispatch:** `suite` and `ref`, or `tag_dry_run`, which acts as a tag push
  on `ref` without pushing a tag. Like every dispatch, it needs the file on
  `main`.
- **Weekly:** `l3-reduced` on `3.x.x`. Like every schedule it runs from
  `main`'s copy of the file.

The workflow follows L2's pattern. The plan job pins one commit. One job
builds the release binaries once. Each preset repeat runs in its own job,
through `prism-load-run.sh` and held to its preset's gates, with at most eight
jobs at once. Then the `collate` job runs `qbit-prism-load-compare --collate`
(#511's summarizer) and writes #473's document tables to the job summary.
They have one table per D1 phase, with a row per preset: its sessions,
window, frontends, replication and plan, then #511's cells (n, target,
achieved, shortfall, refused valid shares, unanswered submits, ACK p50 and
p99 against the limit, `ORDER_LOCK` waiters, the D1 verdict). A table of each
preset's own gates follows. The job also writes a verdict:

- `complete`: every planned run reached its gate;
- `passed`: every planned run passed it.

A planned run whose job left no artifact, whose report names another commit,
or which ran a preset file other than the checked-in one is listed and fails
the suite. It is never dropped. A failed build lists every run as never run
and writes no verdict. Concurrency is per PR and label (a new push cancels
the older push's run), per tag, and one group for the weekly run. The
workflow is never a required check. Cost: `l3-full` is about 660 runner-minutes
(about $42) a release candidate, and `l3-reduced` about 60 runner-minutes a week.

**Proves:** on one commit and one runner class, #447's production-window
cells, including the found-block path at 400k and dense cadence, complete,
reconcile exactly, lose no acknowledged share and hold their gates, with
three repeats for each cell. Its correctness rows count toward #291. A tag
has L3 evidence for exactly its tree.

**Does not prove:**

- **A D1 verdict** (#487 decision 5). The D1 column applies #473's rule on
  the CI runner class, not on the rehearsal host.
- **A regression.** Nothing compares against a baseline. The A/B release
  benchmark on the reference host (#511, [prism-release-benchmark.md](prism-release-benchmark.md))
  and #551's trend do that.
- **#473's flush and build controls** and the 20k dense attempt (exit 6):
  they are not presets.

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

**Does not prove:** #521 scenario 7 at 2,000 wallets, which fails on #621 in
debug and #622 in release and runs only by hand until both are fixed.

**Faults under load (#554)** run in the load harness's `faults` phase
(`crates/qbit-prism-load/README.md`, "Faults under load"), one fault at a
time while the sessions keep mining, each held to its criteria in the
manifest:

- per PR, `faults-pr-smoke` (`qbit-prism-load::faults`): a SIGTERM drain with
  an offer in flight and a `SETTLEMENT_LOCK` holder, on the fake node;
- nightly, `faults-short-real-node`: every fault but the full WAL volume, on
  the real node with 500 sessions: the slow database, pool exhaustion, the
  lock holder, the frontend SIGKILL, the drain, the rolling restart, the
  reconnect storm, the restart over a candidate backlog, the primary lost
  with its async standby promoted, the fenced switch and a found block across
  a failover;
- weekly, on the Saturday selection beside the soak, `faults-long-real-node`:
  every fault, the full WAL volume included, with 2,000 sessions on a 16 vCPU
  runner;
- on dispatch, `faults-failover-fake-node`: the five database and landing
  faults on the fake node.

They prove D3's loss policy under load (only acknowledged shares in the
replication gap are lost, each listed), that the frontends serve the promoted
primary within 30 s without a restart, that a block mid-landing lands once
across a failover, and that a full WAL volume acknowledges nothing it cannot
keep. They do not prove a real network partition: the relays and endpoints
stand in for one.

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

**Reading a failure:** an error outside the checks leaves the checks after it
`not reached`. The job summary and `l6-report.json` (`failed_phase`) name the
phase it stopped in: `node`, `stack`, `resume`, `load`, `quiesce`, `ledger` or
`blocks`. A qbitd RPC that times out names its method. In the `node` phase,
qbitd's `createwallet` takes about 25-30 s on the runner, which is mostly the
create-time P2MR keys. If the call outlasts its 30 s client timeout, the lane
waits for `listwallets` to show the wallet instead of failing (#637).
`createwallet` plus that wait is bounded at 300 s. The report's `wallet`
records which way the wallet arrived and how long it took.

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
- `live`, `weekly`, `fuzz`, `images` and `bridging`: add those jobs.

A one-off configuration is a checked-in preset on a branch, dispatched with
that branch as `ref`. The harness refuses a flag the preset pins if it is given
again, so a result always matches a reviewed file.

To run the nightly set against a PR, apply the `run-load` label. A later push
needs the label applied again. The runner probe is dispatched the same way
(`gh workflow run prism-load-runner-probe.yml -f classes=8,16`).

The production-window matrix is dispatched from its own workflow, once the
file is on `main`:

```sh
# The full suite on a ref.
gh workflow run prism-load-l3.yml -f suite=l3-full -f ref=my-branch

# What a v* tag on this ref would do: promote a matching PR run, or run l3-full.
gh workflow run prism-load-l3.yml -f ref=v3.0.0-rc1 -f tag_dry_run=true
```

To run `l3-full` on a PR, apply `release-candidate`. A PR that changes
`VERSION` runs it on every push.

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
| `prism-load-bridging` | both runs' directories (`fake/`, `real/`) and `bridge-row.json` | 90 days | running |
| `prism-live-nightly`, `prism-live-weekly`, `prism-stratum-fuzz`, `prism-shipped-images` | gate manifests and test logs; fuzz logs and crashing inputs; `l6-report.json` and the Compose logs | 30 days | running |
| `prism-load-tested-commit` | the commit the last nightly carried to a verdict (the guard reads it) | 90 days | running |
| `prism-l3-run-<preset>-r<repeat>` | one L3 run's directory, as `prism-load-<preset>` | 90 days | running |
| `prism-l3-plan`, `prism-l3-collated` | the plan (commit, tree, matrix) and a tag's promotion decision; the collated tables, `verdict.json` and, on a promoted tag, `promotion.json` | 90 days | running |
| `prism-l3-verdict-<tree>` | the verdict a tag's promotion looks up by tree | 90 days | running |
| `prism-load-trend-<preset>` | the preset run's trend row (every event) | 7 days | running |
| `prism-load-regression-verdict` | the regression rule's verdict JSON and summary | 90 days | running |
| `ci-evidence` branch | `trend/<lane>/YYYY-MM.jsonl`: one JSON line per preset run, job outcome, missing preset or later promotion, from scheduled runs (and `v*` tag runs once L3's tag hook lands) | permanent | running (#551) |
| Release assets | the rolling `ci-evidence/YYYY-MM` pre-release: soaks, flagged L2 runs and cited runs; a tag's release: its full-suite jobs | permanent | running for the nightly workflow (#551); the tag's release follows L3's tag hook, and the bundle for a release candidate is #557 |

A run cited in [prism-throughput-measurements.md](prism-throughput-measurements.md)
or in an issue can be promoted with `scripts/prism_load_trend.py cite`
([above](#trend-regression-rule-and-evidence-promotion-551)); link the asset.

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

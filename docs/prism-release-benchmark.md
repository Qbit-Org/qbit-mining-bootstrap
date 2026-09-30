# PRISM release benchmark

Before a release is tagged, its build is compared with the previous
release's on the reference host, on the release preset pair. Each preset is
run as interleaved A/B repeats and held to #473's D1 rule (#511, S4 of
#487). This page names the reference host, the presets and the command. It
also says how to read the result.

## The command

```sh
python3 scripts/prism_load_ab.py \
  --base <previous release> --candidate <release candidate> \
  --preset throughput-20k-window-1fe --out ab-throughput-20k-window-1fe
```

That one command reproduces #479 STEP 1's steady-state comparison of two
commits with `--base 5d0042f6 --candidate <commit>` (three interleaved
repeats by default).

The driver, `scripts/prism_load_ab.py`:

1. **Builds each ref separately.** Each gets a release build in its own
   clean detached worktree under `<out>/builds/`. Every run uses that
   build's own harness and server, because the harness refuses another
   build's server.
2. **Takes the host's benchmark lock.** It holds `/tmp/qbit-prism-bench.lock`
   from the first build to the comparison, so two series never share the
   host. A second invocation on the same `--out` is refused
   outright, whatever lock file it names. After each run the driver, a child subreaper, stops
   anything the run left below it, including the servers and PostgreSQL that
   leave the harness's process group with `setsid()`, and it does the same
   for an interrupted build before releasing the locks. A host where the
   driver cannot become a subreaper is refused, dry runs included.
3. **Records `pg_test_fsync`.** It runs `-s 5` on the filesystem the harness
   builds its clusters on, once before the series and once after.
4. **Alternates the order.** Odd repeats run the base first and even
   repeats the candidate first.
5. **Gates each run on load.** Before each run it waits a 60 s cooldown and
   then until the 1-minute load average is under 3. It gives up after an
   hour and exits 3; it never runs on a loaded host.
6. **Samples the host.** During each run it records the load average and
   `MemAvailable` every 10 s. It also snapshots the process table at the
   start and end of each run.
7. **Enforces a ceiling.** A run still going after 2,400 s is killed.
8. **Summarizes.** It calls `qbit-prism-load-compare` and writes the result
   to `<out>/comparison.md`. The exit status is the comparison's: 0 pass,
   1 fail, 2 bad inputs.

`--dry-run` builds both refs, prints each build's resolved command line and
runs nothing. It takes the lock too, since its builds load the host.

`--resume` continues an interrupted series from `<out>/manifest.json`. It
must run on the same host (hostname, vCPUs, memory and kernel) with every
setting unchanged except `--resume`, `--skip-build`, `--dry-run` and
`--compare-bin`. An interrupted run's harness, server and cluster are
stopped before the driver exits, on Ctrl-C, SIGTERM or SIGHUP alike. The summarizer refuses a series in which
either build is short of its repeats.

`python3 scripts/prism_load_ab.py --help` lists the
settings.

### Every flag comes from the preset

The preset is read from the checkout the driver runs in. It pins every flag
the result depends on, so both builds run the same workload whatever their
harness defaults (see #497, which moved `--stratum-max-pending-initial-jobs`
from 2,016 to 128). Both refusals below happen before anything runs:

- **A build's harness has an unpinned result flag.** The pair is refused.
- **A build's harness predates a flag the preset pins.** For example,
  `5d0042f6` has no `--stratum-max-pending-initial-jobs`. The driver leaves
  the flag off only when `crates/qbit-prism-load/legacy-flags.json` says
  that build ran that value anyway. For this flag the old value was
  `ceil(sessions / frontends) + 16`, at least 128, so `throughput-20k-window-1fe`'s 2,016
  qualifies. Any other value refuses the pair.

`tests/legacy_flags.rs` holds the table to the harness's own defaults.
`tests/test_prism_load_ab.py` checks that every D1 preset resolves against
`5d0042f6`'s flag list.

## The release preset pair

| Preset | What it measures | Runner class |
|---|---|---|
| `throughput-20k-window-1fe` | #271's D1 plan over the 20k window, 2,000 sessions on one frontend, async standby. This is #479 STEP 1's configuration. | 8 vCPU |
| `throughput-400k-window-1fe-async` | The same plan over #473's 400k production window. | 8 vCPU |

These are the `d1-20k` and `d1-400k` presets #511 asks for. #559 renamed
them; the old names still resolve through `presets/aliases.txt` for one
release.

`throughput-500k-window-1fe-async` is the 500k cell, and
`tip-delivery-2000-miners-400k-2fe-retarget` is #275's tip-delivery
benchmark. Run them with the same command when a release touches the window
or tip paths. The tip-delivery preset pins workload flags that `5d0042f6`'s
harness cannot express, so the driver refuses it against that base.

Both presets gate `steady_state` only. The 2,000 shares/s `burst` is reported
beside it, not gated: #473 found no configuration that meets it, and on the
reference host the disk caps it for every build.

## Reading the comparison

`comparison.md` has three parts:

- **Host facts** and both flush-cost readings.
- **A run table** in run order, with the load average before and during
  each run.
- **One table per D1 phase**, with a row per build.

A build **meets** the D1 rule in a phase only when every repeat passes all
of these:

- the harness exited 0;
- `shortfall == 0` and no valid share was refused;
- no submit went unanswered;
- the client's ACK p99 stayed within the run's validator limit.

A run killed at the ceiling, or one that exited non-zero, is kept out of
the medians and fails its build's verdict. A figure a report does not carry
shows as "n/a" and fails the rule; it is never counted as zero. A report
naming a revision other than its build's is refused rather than pooled.

Every run is also held to the preset's own gates, as
`qbit-prism-load-gate` holds a nightly run: reconciliation, durability, and
any tip-delivery or churn budget the preset sets. Both builds must report the
same target rate and, within 5%, the same phase length in every phase, and
every counted run must report every phase any run reported and the
preset's pinned frontends, sessions, submits in flight per session, plan,
window size, replication and ACK p99 limit, with every frontend launched
with the pinned server settings (runtime workers, database connections,
commit timeout, block poll, initial-job admission, pool fee), and with the arrival,
population, template, pool-fee, churn and node settings the report states reading
as this harness renders the preset's (a build whose harness predates one of
those flags is exempt from it, since the legacy table already proved it ran
the pinned value); a build that reads a pinned rate differently
fails the comparison rather than passing on less load. The same holds for
the rest of the planned workload, even when every run agrees: each phase's
database delay and frontend restarts (the reconnect phase's drained restart
with two or more frontends, the mid-flight kill's relaunch), the scheduled
blocks (under the dense cadence, the pinned landing budget and the landings
it buys), a connection for every pinned session, the completed reconnects,
the memory floor, and the samplers' pinned intervals on every launched
frontend.

**The verdict is PASS** when the candidate meets the rule in every gated
phase, every candidate run passes the preset's gates, and the targets
agree. When the base meets it and the candidate does not, the comparison
calls it a **regression**. The medians' differences are printed under each
phase.

## Reference host

Rates depend heavily on what a durable commit costs. The share append
commits inside `ORDER_LOCK`, so the flush cost is serialized per share:

- #473's laptop flushed without forcing the drive cache (about 34 µs per
  commit) and found no 500 shares/s gap between builds.
- #479 then found that gap on a Linux host with real flushes.

A comparison is only evidence on a host whose flushes are real.

The reference host is **`ref-linux-479`**, the Linux host #479 STEP 1 ran
on:

| Fact | Value |
|---|---|
| Machine | shared Linux development VM, 22 vCPU, 41 GiB RAM, one SATA SSD shared with other work |
| PostgreSQL | 16, managed by the harness: a fresh primary and async standby per run, `fsync`, `synchronous_commit` and `full_page_writes` on, `wal_sync_method = fdatasync` |
| **Flush cost** | `pg_test_fsync -s 5` on the clusters' filesystem, one 8 kB write: **fdatasync 3,467 ops/s = 288 µs**, open_datasync 3,937 ops/s = 254 µs, fsync 2,123 ops/s = 471 µs |
| Load gate | 1-minute load under 3, nothing else holding the benchmark lock |
| What the builds did there | `5d0042f6` sustained 500 shares/s in `steady_state` (shortfall 0 / 1,213 / 0); `8fc94faf` placed 420.8–475.1. `burst` was disk-capped at 470–601 shares/s for `5d0042f6` |

The 288 µs is compiled into the summarizer (`REFERENCE_FDATASYNC_USECS` in
`crates/qbit-prism-load/src/compare.rs`). Every comparison says whether both
of its `pg_test_fsync` readings fall within 2× of it. A reading outside that
band is printed in bold, and that series' rates are not comparable with the
reference's. #479 did not record the VM's hostname; every series records its
own in `manifest.json` (`host.hostname`).

A replacement reference host needs three things:

- the same class of machine;
- its `pg_test_fsync` fdatasync figure updated in the table above and in
  `REFERENCE_FDATASYNC_USECS`;
- an A/A series (the same commit as base and candidate) to show its spread.

## Release checklist line

> Run `scripts/prism_load_ab.py` with `throughput-20k-window-1fe` and `throughput-400k-window-1fe-async`,
> previous release as base and the release candidate as candidate, on the
> reference host, and attach both `comparison.md` files to the release PR.
> Do not tag on a FAIL without a written disposition.

For the first 3.x.x release the previous release (2.x.x) has no Rust server.
The base is `5d0042f6`, #271's measured build, as it was for #479.
[`mainnet-deployment.md`](mainnet-deployment.md#release-inputs) carries this
line.

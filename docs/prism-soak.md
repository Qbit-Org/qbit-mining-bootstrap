# Long soak (#575 item 2)

Before #575 the longest automated PRISM run was about twelve minutes. This
page covers the three pieces that hold a server to hours and days instead:

1. **`--plan soak`** in the load harness (`qbit-prism-load`): one server
   lifetime on regtest, looping checked-in presets' workloads for hours, with
   forced share-ledger partition rollovers and the operator's retention cycle
   running under the load.
2. **The weekly soak**, the `soak-weekly` preset, run by
   `.github/workflows/prism-load-nightly.yml` once a week for about 5.5 h.
3. **`qbit-prism-soak-report`**, the same gates applied hourly to a live
   deployment (testnet4) through Prometheus and a read-only database URL.

All three write the same per-sample record and are judged by the same code
(`crates/qbit-prism-load/src/soak.rs`), so a regtest soak and a multi-day
testnet4 soak mean the same thing by "passed".

## What a soak is held to

Each sample records, per server process, resident memory, open file
descriptors and threads; for the database, connections per client,
connections idle in a transaction for over a minute, `pg_wal` size, the next
`share_seq`, every share-ledger partition with its state and size, the
twenty largest other relations, and the payout divergence rows (#478); and
the share ACK latency since the previous sample. A value that could not be
read is `null` with the reason beside it, never zero, and a gate whose inputs
are unknown fails (EP-OBSERVABILITY).

| Gate | Holds |
|---|---|
| resident memory slope | the peak of each whole `rss_trend_window_minutes` window after the warm-up, per process; the least-squares slope over at least `min_trend_windows` peaks, at most `rss_slope_mib_per_hour_max` |
| resident memory against the warm-up peak | the bound of [prism-capacity-readiness.md](prism-capacity-readiness.md#the-resident-set-bound): after the warm-up, at most `rss_warmup_peak_multiple_max` (2.0) times the warm-up's peak |
| open file descriptors slope | the same fit, at most `fd_slope_per_hour_max` |
| database connections | per client, never above `pool_connections_max` |
| database connection drift | mean of the last quarter of post-warm-up samples minus the first quarter's, at most `pool_connections_drift_max` |
| idle in transaction over 60 s | zero at every sample |
| WAL size | `pg_wal` never above `wal_bytes_max` |
| share partition rollovers | partition bounds the share sequence crossed, at least `min_rollovers` |
| share partitions archived and dropped | partitions that left the ledger through `share-archive`, at least `min_archive_cycles` |
| one server lifetime | one PID per process for the whole soak (`one_lifetime`) |
| payout divergences | no new row during the soak |
| acknowledged shares in the ledger | deployments only; see below |

On regtest the harness's own gate still applies on top: exit 0, which is an
exact reconciliation of every offered, acknowledged and committed share, no
durability finding, no shortfall in any phase, and the tip-delivery budgets.

**A known resident-memory failure.** `rss_expected_failure` names an issue,
or is `null`. While it names `<issue>`, the resident-memory rows of every
process are an expected failure as a group. A measured failure is reported as
`expected failure (<issue>)` and does not fail the soak. A soak in which
every resident-memory row passed fails with `<issue> looks fixed`, so the key
goes back to `null` and the gate is real again. It is the group, not each row,
because one frontend's warm-up ratio can pass while its slope, or the other
frontend's rows, fail. An unknown reading still fails, and no other gate is
affected.

Every checked-in soak now sets `null`. `soak-weekly` named #600 until #627:
its first run found a 400k-window frontend's resident set ratcheting up with
block landings to about 3.3 GB. With #627's post-landing `malloc_trim`, the
5.5 h run of 2026-10-06 (at `c95febb3`) passed every resident-memory row:

| Frontend | Peak vs warm-up peak | Window-peak slope |
|---|---|---|
| fe-0 | 1.17x | +8.65 MiB/h |
| fe-1 | 1.13x | -15.75 MiB/h |

Memory and descriptors are fitted only over *steady* samples, those taken
outside rental churn, so the population being compared is the same from
sample to sample; the connection gates use every sample.

Resident memory is fitted over window peaks, not over every sample, because
a block landing on a 400k window is a step, not a trend: in the first
`soak-short` runs each landing's audit build lifted a frontend's resident
set by about 800 MiB (520 to 1,300 MiB) for ten minutes or more, and a second
one to 2.2 GiB before it fell back to 0.9 GiB. A least-squares line through
those samples reads thousands of MiB/h on a healthy server. A window at
least as long as the landing cadence holds a landing in every peak alike,
so landings shift the envelope and a leak tilts it. Descriptors have no such
steps and are fitted over every steady sample.

The server counts its own descriptors (`qbit_prism_process_open_fds`, beside
`qbit_prism_process_resident_memory_bytes`): it makes itself non-dumpable at
start, which closes `/proc/<pid>/fd` to every other process of its user, so
the gauge is the only way to read the count without privileges.

## `--plan soak`

A soak preset is an ordinary preset with `"--plan": "soak"` and a `soak`
block. Its own flags are the server, cluster and population of the one
lifetime: frontends, sessions, window, pool fee, template bits, database and
runtime sizes. The `soak.presets` it names contribute only their workload
flags (`WORKLOAD_FLAGS` in `soak.rs`: plan, rates, arrival, tips, blocks,
churn, storms), and each is run with the phases its own run would drive,
with three changes: no phase restarts or kills a frontend (the reconnect
phase keeps its client reconnects; a looped preset with `--mid-flight-kill`
is refused); the deliberately degraded `slow_database` phase is left out,
since its backlog is its own measurement and, looped, it has to drain before
the next cycle starts, which it can outlast (a first `soak-short` run aborted
there, 16 submits still outstanding at the 25 s drain limit); and every
phase is named `c<cycle>.<preset>.<phase>`. Whole
cycles are planned while they fit in `soak.minutes`. A looped workload that
could connect more sessions than the soak preset's listeners are sized for is
refused before anything starts.

| `soak` key | Meaning |
|---|---|
| `minutes` | scheduled load; whole cycles only |
| `presets` | the looped presets, in order |
| `sample_seconds` | seconds between samples |
| `rollover_minutes` | when to roll the share ledger into its next partition |
| `rollover_margin_rows` | rows left below the bound by a rollover |
| `archive_every_minutes` | minutes between retention passes |
| `archive_window_multiple`, `archive_retention_days` | `share-archive --window-multiple` and `--retention-days` |
| `partition_ensure_interval_seconds` | `PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS` for every frontend |
| `gates` | the table above |

**Rollover.** A partition is 2^24 rows, days of a soak's load. At each
`rollover_minutes` the harness advances the share sequence to
`rollover_margin_rows` below the bound of the partition the sequence is in
(`setval`, which only ever raises it here, as the partition tests do); the
live load then crosses the bound itself into the lead partition, and the
frontends' maintenance attaches a new lead within
`partition_ensure_interval_seconds`. `share_seq` already has gaps, since a
rolled-back append's value is never reused, and every reader walks it in
order rather than by count.

**Retention.** Every `archive_every_minutes` the harness runs the operator's
own commands against the live cluster, as
[the retention procedure](prism-share-ledger-partitioning.md#retention-procedure)
describes: `share-archive seal`, `archive` and `verify` for every partition
the sequence has passed, then `plan`, and `detach` and `drop` for each
partition it reports eligible. A command that fails, or runs past ten
minutes and is killed, is an event with its output (`soak-events.jsonl`) and
fails the soak. At the end every partition that left the ledger is read back
for the run's reconciliation, beside the live ledger: a detached one as it
stands, a dropped one restored from its archive with `share-archive
restore`, not attached. An acknowledged share has to be in PostgreSQL or in
an archive that restores row for row.

The run writes `soak-samples.jsonl`, `soak-events.jsonl` and, from the
gate, `soak-report.md` (the verdict and a table of the samples) beside
`load-harness-report.json`, whose `soak` section lists the rollovers, the
retention steps and errors, the dropped and restored partitions.

### The presets

| Preset | Schedule | Load | Rollovers / archived | Purpose |
|---|---|---|---|---|
| `soak-weekly` | weekly | 13 cycles of 25.5 min (mainnet-shape-130-addresses, rental-churn-bursts-and-storms, tip-delivery-2000-miners-400k-2fe-retarget) = 5.5 h | 4 forced, gated >= 3 / >= 2 | the weekly CI soak |
| `soak-short` | manual | 2 cycles of tip-delivery-2000-miners-400k-2fe-retarget = 19 min, no block landing | 1 forced, gated >= 1; the 400k-window partition sealed, archived and verified under load, not dropped | a quick soak, and the run the leak mutant must fail |
| `soak-smoke` | manual | 4 cycles of pr-smoke = 3.6 min | 2 / 2 | the nightly opt-in gated test `soak_smoke`, proving the machinery end to end |

`soak-weekly` runs the mainnet-shape-130-addresses cluster: two frontends,
400 sessions over 130 addresses, a 400k window, retargeting tips and a
200 bps fee. The tip-delivery segment therefore runs its 133 shares/s and
eight tips 45 s apart over those 400 sessions, not the 2,000 of its own
preset: one lifetime has one population.

Its bounds: resident memory's envelope (the peak of each 25.5 min cycle)
at most 16 MiB/h after a one-hour warm-up and within 2x the warm-up peak,
descriptors at most 2/h, at most 24 connections
per frontend (16 in the pool, the rest the server's single-connection side
pools) drifting by at most 2, `pg_wal` at most 2 GiB (the managed cluster's
`max_wal_size` is PostgreSQL's 1 GB default, and the async standby's slot
can hold more while it catches up). A leak of 1 KiB per accepted share at the
soak's average of about 73 shares/s is about 128 MiB/h per frontend, eight
times the slope bound. The first 5.5 h run (2026-10-06, above) stayed inside
these bounds, but its +8.65 MiB/h slope leaves less than a 2x margin on one
sample. Read a later slope failure against #628 and #629 (allocator, and
per-cycle floors instead of landing-transient peaks) before calling it a
regression.

`soak-short` cannot be that sensitive. In its first twenty minutes a healthy
server's resident set still grows, as its caches fill, by 0.6 to 0.9 GiB/h
(measured on this preset's workload), so its envelope bound is 1.5 GiB/h and
it catches a gross leak only: the test-only mutant below keeps 16 KiB per
accepted share, about 3.7 GiB/h per frontend at soak-short's rate.

`soak-smoke`'s memory and descriptor bounds are deliberately loose: four
minutes of a debug server is not a trend. It proves the sampling, rollover,
retention, restore and gating end to end, every night, through the opt-in
gated test `soak_smoke` (four minutes was too much to add to every PR);
`soak-short` and `soak-weekly` hold the trends. Every PR runs the gated test
`soak_sampling` instead: in a few seconds against the PR shard's database it
samples a fresh PRISM schema and the test process, forces a rollover, and
holds the samples to the soak gates.

### Running one

```sh
cargo build --locked --release -p qbit-prism-server -p qbit-prism-load
bash .github/scripts/prism-load-run.sh soak-short out/soak-short
```

The gate's verdict, including the soak table, is `out/soak-short/gate.md`;
the exit status is the gate's.

**Proving the gate bites.** `qbit-prism-server` has a test-only feature,
`soak-leak-mutant`, that keeps 16 KiB per accepted share for the life of the
process. No build or image of this repository enables it. Build it into a
separate target directory and run `soak-short` against it; the
resident-memory slope gate fails:

```sh
CARGO_TARGET_DIR=target-mutant cargo build --locked --release \
  -p qbit-prism-server --features qbit-prism-server/soak-leak-mutant
target/release/qbit-prism-load --preset crates/qbit-prism-load/presets/soak-short.json \
  --server-bin target-mutant/release/qbit-prism-server \
  --allow-unverified-server-revision --out out/soak-leak
target/release/qbit-prism-load-gate --report out/soak-leak/load-harness-report.json \
  --preset crates/qbit-prism-load/presets/soak-short.json --exit-code 0
```

## The weekly CI job

`prism-load-nightly.yml` has a third schedule, Saturday 05:41 UTC, that
selects `weekly` (`scripts/prism_load_matrix.py weekly`), which is
`soak-weekly`. It reuses the workflow's release build, runs the preset with
`prism-load-run.sh` on an 8 vCPU runner and uploads the run directory. The
preset's timeout is 355 min, under the 6 h job limit. The soak has its own
concurrency group, so it never holds the nightly queue, and the live regtest
variants do not run on its schedule. Dispatch it by hand with
`preset: soak-weekly`.

The nightly's skip-unchanged guard (#549) does not apply to the soak: it runs
every Saturday whether or not `3.x.x` moved. A soak measures drift over one
server lifetime and its bounds are still being calibrated, so another lifetime
on the same commit is a second sample, not a repeat; and the commit the guard
compares with is the one the nightly presets tested, not the soak. A failed
soak opens or comments on the `prism-load-nightly-failure` issue like any
scheduled run.

**Cost.** About 5 h 40 min of one 8 vCPU runner a week plus the shared
release build, roughly 350 runner-minutes, about $5.60 a week at $0.016 per
8 vCPU minute, or about $24 a month.

## A multi-day testnet4 soak

The regtest soak cannot see real miners, a real node or days of time.
`qbit-prism-soak-report` applies the same gates to a live deployment, hourly,
from outside it: it needs only a Prometheus that scrapes the PRISM servers'
`/metrics` and a database URL. Nothing about the deployment is in the
repository; the endpoints come from flags or the environment.

### Before you start

- **Prometheus** scrapes every PRISM server of the deployment. Each server's
  series carry a label that names the process (`instance` by default,
  `--instance-label` otherwise); `--selector` narrows the metrics to the
  deployment's servers, e.g. `--selector 'job="prism"'`.
- **A read-only database role** on the PRISM database. `pg_monitor` lets it
  read `pg_wal`'s size and `pg_read_all_stats` the other sessions' states;
  without them the WAL and idle-in-transaction gates read unknown and fail,
  by design. The ledger is read through its parent table, so partitions
  attached after the grant need no grant of their own.

  ```sql
  CREATE ROLE prism_soak LOGIN PASSWORD '...';
  GRANT CONNECT ON DATABASE <db> TO prism_soak;
  GRANT USAGE ON SCHEMA public TO prism_soak;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO prism_soak;
  GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO prism_soak;
  GRANT pg_monitor, pg_read_all_stats TO prism_soak;
  ```

- **One lifetime, one configuration.** Deploy the release under test, record
  its image ID and the deploy environment (secrets redacted), and do not
  restart or reconfigure it during the soak. A restart is detected (the
  acknowledged-share counter resets) and restarts the trends from the new
  process; the checked-in testnet4 gates do not fail on it
  (`one_lifetime: false`), so note every intentional restart in the soak log.
- **Ordinary load throughout**: the testnet4 miners that normally mine, with
  the same population for the whole soak. The
  [24 h resident-set runbook](prism-capacity-readiness.md#testnet-24-h-soak-runbook-deferred-to-the-operator)
  in the capacity readiness record is the stricter, shorter procedure for the
  memory bound alone; a multi-day soak here does not replace it.

### Start it

Build the reporter from the release's commit, on any host that can reach
Prometheus and the database:

```sh
cargo build --locked --release -p qbit-prism-load --bin qbit-prism-soak-report
export PRISM_SOAK_PROMETHEUS_URL=<prometheus base URL>
export PRISM_SOAK_DATABASE_URL=<read-only database URL>
mkdir -p soak-testnet4-$(date -u +%Y%m%d)
target/release/qbit-prism-soak-report \
  --out soak-testnet4-$(date -u +%Y%m%d) --selector 'job="prism"' --once
```

The first `--once` sample checks the plumbing: every row of
`soak-report.md` should read a value, not `unknown`. Then either leave it
running in the foreground (it samples every `--interval-seconds`, 3600 by
default, until stopped):

```sh
nohup target/release/qbit-prism-soak-report --out soak-testnet4-<date> \
  --selector 'job="prism"' >> soak-testnet4-<date>/reporter.log 2>&1 &
```

or run `--once` hourly from cron or a systemd timer with the same `--out`;
each run resumes from the samples file, so the soak's history is the file's.
`--once` exits 0 when every gate passes, 1 when one fails and 2 when its
inputs cannot be read, so a timer's failure state is the verdict.

### Read it

`soak-report.md` is rewritten after every sample: the verdict table over
every sample so far, then up to 48 evenly spaced samples. The checked-in
gates, `crates/qbit-prism-load/soak-gates/testnet4.json` (`--gates` for
another file), use a one-hour warm-up like the resident-set bound, need at
least twelve post-warm-up samples before a trend can pass, and hold resident
memory's envelope (the peak of each 4 h window, at least four of them, so
the trend can pass from about 17 h in) to 4 MiB/h and 2x the warm-up peak, descriptors to 0.5/h, connections
to 40 per client with a drift of at most 2, and `pg_wal` to 4 GiB. They ask
for no rollover or archive, since testnet4's share rate does not fill a
2^24-row partition in days; a soak that runs the retention procedure can
raise `min_rollovers` and `min_archive_cycles` in its own gates file.

Connections are attributed to a client by `application_name` when it has
one, else by role and address. The servers do not set an application name,
so on a deployment one client is one server host's role; raise
`pool_connections_max` when several servers share a host and a role.

**Acknowledged shares in the ledger.** The reporter cannot see the miners'
side, so it compares the servers' own count of acknowledged shares
(`qbit_prism_accepted_shares_total`, summed over processes, across restarts)
with the accepted rows the ledger holds for the same span. The first
sample's readings are the baseline. Each later sample adds the counters'
growth since the previous reading to the acknowledged side, and the ledger's
accepted rows between the two readings' scrape times (Prometheus's
`timestamp()`, the earliest among the processes) to the committed side,
counted while those rows are still live, so retention cannot take them out
of the count. Processes scraped a little after the earliest may be ahead by
the rows of that spread, so the check is `committed + spread >=
acknowledged`. A counter that reset since its last reading
(`resets()`) is a restart: the new process's count is all new, and what the
old one acknowledged after its last reading cannot be counted; the report
counts such restarts. That can hide a loss inside the uncounted stretch; it
cannot invent one. A sample that could not read the counters or the ledger
advances neither side, and the next one covers the stretch. The regtest
soak's reconciliation, which knows every share each client was told was
accepted, is the exact version.

A process whose series stops appearing stays in the samples with every
figure unknown, and its trends fail on the first sample that cannot read
it: a process gone dark is not judged on what it did before.

When the soak ends, attach `soak-report.md`, `soak-samples.jsonl`, the image
ID and the deploy environment to the release's tracking issue.

# `qbit-prism-load`

A Stratum-to-PostgreSQL load harness for PRISM, and the only thing that
produces a `qbit-prism-capacity-evidence/v3` artifact (#303).

It launches real `qbit-prism-server run` child processes, drives them over real
Stratum sockets with real proof of work, and reports what the cluster did:
client ACK latency, PRISM advisory-lock waits, per-frontend CPU and RSS, reconnect
behaviour, time to usable work after a new tip, and rejections by reason.

Nothing in the harness changes production code, adds a metric, or bypasses a
validation. The server verifies every share for real.

## What it measures, and why

Decision D1 (#260) sets the targets: 2,000 shares/s for a minute, 500 shares/s
for five minutes, 2,000 sessions, a 400,000-share payout window and a 500,000
headroom run. Decision D3 sets the topology: one primary and one asynchronous
standby, with synchronous mode a primary-side flip the harness can make and
measure.

## Prerequisites

- **Release builds.** A debug-profile server is not a capacity measurement; the
  harness refuses one unless `--allow-debug-server` is given, and records the
  build profile of both binaries either way. The profile is read from the
  Cargo directory the binary sits in, after following symlinks, so a copied
  or installed binary has an unknown profile, and an unknown profile needs
  the same override: it cannot be shown to be a release build. The override
  admits the run and nothing more: like `--allow-dirty-tree` and
  `--allow-unverified-server-revision` it forces `artifact_kind: "example"`,
  because only a server shown to be a release build can produce
  qualification evidence.

  ```sh
  cargo build --locked --release -p qbit-prism-server -p qbit-prism-load
  ```

- **PostgreSQL 16 server binaries** (`initdb`, `pg_ctl`, `pg_basebackup`) in
  `--pg-bin-dir`, or `QBIT_PRISM_LOAD_PG_BIN_DIR`, or `pg_config --bindir`. On
  Debian and Ubuntu that is `/usr/lib/postgresql/16/bin`. No container runtime
  is needed. The directory is resolved and checked before the run creates
  anything, so a wrong path names the flag and the missing binary instead of
  failing later inside `initdb`. A `--database-url` run is not asked for
  them.

- **A clean tree.** `subject.coordinator_revision` names the commit that was
  built. With modified tracked files the harness refuses to run unless
  `--allow-dirty-tree` is given, and that flag forces `artifact_kind:
  "example"` and marks the side report `dirty: true`.

- **A server binary built from that tree.** The revision comes from the
  harness's checkout, so the binary has to be shown to be what that checkout
  builds. The server embeds no revision; the harness reads the Cargo dep-info
  file beside the binary (`qbit-prism-server.d`), which lists every workspace
  source it was compiled from, and requires that every listed source -- plus
  the manifests Cargo's list omits: `Cargo.lock`, the workspace `Cargo.toml`,
  `crates/qbit-prism-server/Cargo.toml` and the `Cargo.toml` of every crate
  a listed source belongs to, which is the server's path dependencies derived
  from Cargo's own record -- belongs to this checkout and is no newer than
  the binary. That is the test Cargo itself applies before it decides not to
  rebuild, and the manifests are in it because a package manifest can change
  what is built (a dependency feature, say) without touching a source or the
  lock file. A binary with no dep-info beside it (copied, installed) or with
  a newer source or manifest is refused with the reason, unless
  `--allow-unverified-server-revision` is given, and that flag forces
  `artifact_kind: "example"`. The side report records the outcome under
  `versions.server_revision_evidence`.

- **File descriptors.** The harness raises its own soft `RLIMIT_NOFILE` to what
  the session count needs and the child frontends inherit it.

## Running

```sh
target/release/qbit-prism-load \
  --server-bin target/release/qbit-prism-server \
  --pg-bin-dir /usr/lib/postgresql/16/bin \
  --frontends 2 --sessions 200 --window-shares 20000 \
  --replication async --plan short --rate 50 \
  --out load-out
```

The D1 plan is `--plan d1`. Every phase length and rate is overridable.

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--server-bin` | the `qbit-prism-server` beside this binary | Frontend executable |
| `--allow-debug-server` | off | Run a server whose build profile is debug, or cannot be determined from its location, anyway; forces `artifact_kind: example` |
| `--allow-dirty-tree` | off | Run with modified tracked files; forces `artifact_kind: example` |
| `--allow-unverified-server-revision` | off | Run a server binary that cannot be tied to this checkout's HEAD (no Cargo dep-info beside it, or a source or manifest newer than it); forces `artifact_kind: example` |
| `--example-artifact` | off | Emit `artifact_kind: example` from a clean tree |
| `--pg-bin-dir` | `QBIT_PRISM_LOAD_PG_BIN_DIR`, then `pg_config --bindir` | PostgreSQL server binaries. The harness keeps its own variable rather than reading one of the shared test-gate variables, which belong to the gate crate (#322) |
| `--database-url` | none | Use an existing database; no standby is managed, and the replication mode is detected, never assumed: it must be the one `--replication` declares, at entry and again after the load, or the run exits 8. The host may be a name: the delay proxy resolves it once at entry and records the addresses in the side report's `delay_proxy` block. The libpq-style `host`, `hostaddr` and `port` parameters are honoured for the proxy's upstream and dropped from the URL the frontends receive, because SQLx applies them over the authority and they would otherwise route every frontend around the proxy; every other option is kept |
| `--replication` | `async` | `async`, `sync` or `none`: the replication mode the run declares. A managed cluster is built to it; an external database is checked against it |
| `--frontends` | 1 | 1, 2 or 4 |
| `--sessions` | 100 | Stratum sessions, round-robin across the frontends |
| `--window-shares` | 20000 | Shares pre-seeded into the payout window |
| `--seed-share-bytes` | 581 | Serialized size of one seeded share |
| `--plan` | `short` | `d1` or `short` |
| `--rate` | 50 | Offered shares per second for the `short` plan |
| `--max-outstanding-per-session` | 1 | The server answers one request per session at a time |
| `--warmup-seconds` | 30 | Warm-up before the first artifact phase; not in the artifact |
| `--steady-state-seconds`, `--steady-state-rate` | plan | Override the `steady_state` phase |
| `--burst-seconds`, `--burst-rate` | plan | Override the burst phase (side report only) |
| `--reconnect-seconds` | 60 | `reconnect` phase length |
| `--slow-database-seconds` | 60 | `slow_database` phase length |
| `--reconnect-target` | 12 | Completed reconnects to drive; the artifact needs at least 10 |
| `--slow-db-delay-ms` | 10 | One-way per-chunk proxy delay; the artifact phase needs at least 10 |
| `--mid-flight-kill` | off | SIGKILL a frontend with submits outstanding, in a side phase. That phase runs under the same proxy delay as `slow_database` and the kill waits for the target frontend to actually hold work, because with no delay an acknowledgement takes a few milliseconds and the scenario would quietly not happen. The report carries `submits_outstanding_at_kill`, and zero there means it did not exercise. Only the shares the kill made indeterminate are exempt from reconciliation; an acknowledged share this phase loses is a durability finding (exit 4) as in every other phase. The kill, the relaunch, the readiness wait and the re-offers are driven from the scheduler loop without stalling it, as the `reconnect` phase's restart is, so the other frontends keep receiving their scheduled load throughout; a relaunch that exits or never answers `/healthz` within `--work-timeout` aborts the run (exit 6) |
| `--scheduled-blocks` | 0 | Own blocks to find and submit during `steady_state`. Each one bumps the payout revision, so expect a burst of rebuild-pending rejections on every frontend afterwards. With `--cadence dense` this is instead the dense phase's landing budget, and `steady_state` schedules none |
| `--cadence` | `none` | `none`, or `dense` for the dense-cadence side phase (#271 criterion 6) |
| `--cadence-seconds` | 240 | Length of the `dense_cadence` phase |
| `--cadence-rate` | the steady-state rate | Offered shares per second during `dense_cadence` |
| `--cadence-gaps` | `9,19,9,18,20` | Seconds between own-block landings, repeated cyclically. Every gap must be finite and at least 5 s, and the pattern must place at least 10 landings in the phase, or the run is refused at entry |
| `--external-tips` | 3 | Tips minted during warm-up, for time to usable work. They need a warm-up phase: with `--warmup-seconds 0` none is minted and the time-to-usable-work section is empty rather than zero |
| `--work-timeout` | 120 | Seconds to wait for frontends to serve work |
| `--forecast-peak-shares-per-second` | 2000 | D1's forecast; the validator's gate is twice this |
| `--ack-p99-limit-ms` | 1000 | Must be at most `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` × 1000 |
| `--db-max-connections` | 16 | `PRISM_DATABASE_MAX_CONNECTIONS` per frontend |
| `--runtime-workers` | 2 | `PRISM_RUNTIME_WORKERS` per frontend |
| `--blockpoll-seconds` | 2 | `PRISM_BLOCKPOLL_SECONDS` per frontend |
| `--share-commit-timeout-seconds` | 15 | `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` per frontend |
| `--lock-sample-interval-ms` | 10 | PRISM advisory-lock sampling cadence, 1..1000 ms. One poll covers both sampled locks |
| `--process-sample-interval-ms` | 1000 | CPU and RSS sampling cadence, 50..60000 ms |
| `--min-mem-available-mib` | 4096 | Stop the run if `MemAvailable` falls below this |
| `--out` | `load-out` | Output directory |
| `--keep-artifacts` | off | Keep cluster data directories and logs |

### The D1 plan

1. **warm-up**, not in the artifact. External tips are minted here.
2. **`steady_state`**, 500 shares/s for 300 s.
3. **`burst`**, 2,000 shares/s for 60 s. Side report only.
4. **`reconnect`**, at least 60 s with at least 10 completed reconnects. With
   two or more frontends, one of them is restarted a third of the way in:
   its sessions are paused, their outstanding submits are allowed to settle
   for up to the share-commit timeout plus the 10 s drain margin, the
   process is killed and relaunched, and the sessions are pointed back at it
   once `/healthz`
   answers. The restart is driven from the scheduler loop without stalling
   it, so the other frontends keep receiving their scheduled load during the
   outage, which is what the phase measures. A paused session is ineligible
   for offers until it is retargeted: the scheduler's tokens go to the
   frontends that are up, nothing queues behind the pause to count as
   outstanding or to go out as a burst on resume, and a client-initiated
   reconnect that lands on a paused session does not lift the pause. If the
   submits never settle the frontend is not killed and the run aborts
   (exit 6) rather than turning the harness's own in-flight submits into
   lost acknowledgements. A restart still in flight when the phase's
   deadline arrives is completed after it, with nothing scheduled
   meanwhile: the phase's `duration_seconds`, its `ended_at`, its lock and
   process windows and the denominator of its achieved and offered rates
   cover exactly the scheduling window, and the wait for the relaunch is
   boundary time, like the settle before the next phase's delay is applied.
   The restart itself is still this phase's, under `drained_restarts` and
   `frontend_restarts`. A client-initiated reconnect quiesces the same
   way before it closes its socket: the session waits for its outstanding
   submits to settle for up to the share-commit timeout plus that same 10 s
   margin, so a planned close never turns a submit the server is still
   allowed to be working on into a `no-response` record that a later commit
   would make read as a durability loss. A submit still unanswered after
   that is recorded as `no-response` with the wait it was given. A
   reconnect is attributed to the phase that asked
   for it: one started near the end of this phase and completed after the
   next began is still counted in `reconnect_events`, not under the phase it
   happened to finish in, the same way a submit belongs to the phase that
   offered it.
5. **`slow_database`**, at least 60 s at a delay of at least 10 ms.
6. **`dense_cadence`**, only with `--cadence dense`. Side report only.
7. **`mid_flight_kill`**, only with `--mid-flight-kill`. Side report only.
   A third of the way in, one frontend is killed while its sessions hold
   work. Its sessions are paused before the kill so new offers cannot hide
   the old pending work. After relaunch and `/healthz`, the harness waits
   for their outstanding counts to reach zero and for the collector to
   acknowledge each session's queued records. Only then are the sessions
   resumed and every share whose answer the kill destroyed re-offered with
   exactly the header it carried. The census has one shared deadline of the
   share-commit timeout plus the 10 s drain margin; incomplete accounting
   aborts the run (exit 6) and withholds the artifact. Like the restart above,
   it is polled from the scheduler loop and never awaited, so the healthy
   frontends' traffic is unaffected by the outage.

The artifact's phases are exactly `steady_state`, `reconnect` and
`slow_database`. Adding `--cadence dense` does not change any of them.

## The dense-cadence scenario

`--cadence dense` adds a side phase, `dense_cadence`, that lands own blocks on
the shape #224 recorded — an accepted-candidate minimum interarrival of about
9.14 s, 35 accepted blocks in the trailing hour, and repeated pairs about
18–20 s apart — and reports, for every payout-revision bump, how long each
frontend rejects shares with `new payout work is pending` and how many shares
it rejects before it serves work at the new revision. That number is the budget
#291's soak should hold itself to.

It runs after `slow_database`, with **no proxy delay**: the measurement is the
frontends' rebuild latency, and a delayed database would drown it out. It is
not part of the capacity-evidence artifact.

### The schedule

Landings follow `--cadence-gaps`, repeated cyclically. The first is at 5 s into
the phase, and a landing is only placed if 15 s still fit after it, so the last
landing's windows are measured inside the phase rather than truncated by its
end. At the default gaps a 240 s phase holds 15 landings; a phase shorter than
150 s cannot hold ten and is refused at entry, and the refusal says which
length would do.

Each landing uses the same `Control::ScheduledBlock` path a
`--scheduled-blocks` run uses: the harness asks one session to search its
current job for a network-target solution (about 131,000 hashes) and submit it,
the frontend appends the share together with a candidate, and the candidate
worker calls `submitblock`. Landings rotate across sessions, so no single
frontend has an early view of its own block. A landing the session could not
produce, one the frontend rejected, and one the node rejected each get their own
entry and are counted; none is dropped.

### Where the bumps come from

`payout_revision` lives in the singleton `qbit_prism_cluster` row, which is
what `Ledger::payout_revision()` reads. The harness samples

```sql
SELECT payout_revision, clock_timestamp() FROM qbit_prism_cluster WHERE singleton
```

every 25 ms on its side pool — outside the frontends' path and outside the
delay proxy — and records every change with the server's `clock_timestamp()`
and its own monotonic clock. That is the authoritative list of bumps; nothing
is inferred from a rejection.

One landing can cause more than one bump, and the report says how many rather
than assuming. Two bumps inside one 25 ms interval appear as a single change
with `revision_delta` above 1, which is reported as such.

A bump is attributed to the landing whose pool tip change it follows and which
the next landing has not yet replaced. Anything else is `unattributed`, with
its cause recorded as unknown.

The top-level `bumps` key is every observed change — the same number as
`revision_sampler.changes_observed`, which is what the section's own
`definitions.bump` defines a bump to be. `bump_attribution` carries the split
(`observed`, `attributed`, `unattributed`), and the per-landing `bumps` field
counts only that landing's. So a run in which nothing landed but the revision
moved anyway reports the changes it saw rather than a zero.

### The two windows are reported apart

`coordinator.rs` checks the observed tip before it checks the payout revision,
so a landing produces `new tip work is pending` first, and
`new payout work is pending` only when the revision moves again after that
frontend has rebuilt. The report therefore carries three windows per landing
and frontend:

- the **tip-pending window**: first and last `new tip work is pending`, the
  count, and last minus first;
- the **payout-pending window**: the same for `new payout work is pending`;
- the **combined rebuild-pending window**: first to last of either, with both
  counts summed. This is the one #291 should budget against.

A rejection is stamped when the client read the response line, which is the
only instant the harness observed directly.

### Time to new-tip work and time to new-revision work

**Time to new-tip work** is the run's usual definition, restricted to one
frontend's sessions: the first `mining.notify` whose prevhash resolves to the
landing's tip, measured from the fake node's tip stamp. The search stops at
the span's end, as the new-revision search below does. A notify for the tip
that arrives after the next landing's tip change is a late job for a tip
that has already been replaced; counting it reported new-tip work inside a
span that had none, at a time that was really the next landing's. A frontend
with no such job inside the span reports `sessions_with_new_tip_work: 0`,
the time `null`, and `new_tip_work_unavailable_reason` saying so. The
whole-phase view over every session is in the same section, through the
harness's own `time_to_usable_work`.

**Time to new-revision work is an approximation, and is labelled as one.**
`mining.notify` carries no payout revision, so the first notify with
`clean_jobs=true` at or after the bump *and before the end of the landing's
span* stands in for the first job built at the new revision. `clean_jobs` is
set when the parent *or* the payout revision differs from the session's last
job (`stratum.rs`, `deliver_job`), so inside a landing's window — after the new
tip has already been served — it is the rebuild at the new revision. The bump
it is measured from is the last bump attributed to the landing: the revision a
frontend has to reach before it stops answering `new payout work is pending`.

The search stops at the span's end. The next landing's own `clean_jobs` notify
is that landing's work, and a frontend that had not served the new revision by
then is reported as such: `sessions_with_new_revision_work` is 0, the time is
`null`, and `new_revision_work_unavailable_reason` says no job at the new
revision was seen inside the span. Borrowing the later job would have
understated the time and stopped the count below at the wrong event.

`rejected_before_new_revision_work` counts the rebuild-pending rejections a
frontend returned between the landing and the earliest new-revision work on any
of its sessions inside the span. It is `null`, with the same reason, when there
was no bump or no such job inside the span.

The label travels with the numbers: every object that carries a new-revision
figure carries `new_revision_work_approximation` beside it — the per-landing,
per-frontend tables, `summaries.overall` and each `summaries.per_frontend[]`.
The summaries are the numbers people quote, and their keys
(`time_to_new_revision_work_max_millis`,
`rejected_before_new_revision_work_per_landing`) are not the keys `definitions`
is indexed on, so a reader looking either one up there would find nothing.

### Lost valid work

Every rebuild-pending rejection is a share the client had already proven
against the share target, so each one is miner work the pool discarded. None of
them is persisted, so none is a durability finding — and the report checks that
against this run's committed share identifiers rather than asserting it
(`shares_found_in_postgres`, which must be 0).

The census covers **every** rebuild-pending rejection in the phase, not only
the ones a landing's span owns. `lost_valid_work` publishes the split —
`shares`, `shares_attributed`, `shares_unattributed` — and
`shares_found_in_postgres` is checked over all of them. A landing whose pool tip
change is missing leaves its rejections unattributed, and that is exactly the
case the cross-check exists for: counting only the attributed subset would read
as a clean pass in the one situation that would make it fail. The per-landing,
per-frontend `lost_valid_shares` tables are unchanged and still count a span's
rejections, so they sum to `shares_attributed`.

### Where the rebuild queues

The rebuild a landing triggers takes `SETTLEMENT_LOCK` first and `ORDER_LOCK`
second, so the database-side queueing this phase is read for is split across
two locks. Both are sampled and both are reported, in the phase's own
`phases[]` entry, as `order_lock` and `settlement_lock`. The dense section's
`definitions.advisory_locks_sampled` says which locks are sampled, which server
path takes each and which are not sampled, so the two blocks are not left to be
found by name.

### Honest output

- No landing — `--scheduled-blocks 0`, a pattern that places none, or every
  scheduled block failing — reports `landings: 0` with the reason and no
  windows. No percentile is invented; the budget proposal is `null`. `bumps` is
  whatever the sampler actually observed, all of it `unattributed`: if the
  revision moved with nothing landing, the report says so.
- A sampler that could not read the revision reports its error count and its
  first error, and marks itself `blind`, so an empty bump list is never read as
  "no bumps happened". `blind` covers partial blindness too: a sampler whose
  first read succeeded and whose every later read failed observed no change
  *and* observed almost nothing, so it is blind and says which of the two cases
  it is in `blind_reason`. Beside `samples` and `errors` it reports `coverage`
  — `samples / (samples + errors)`, `null` rather than 0 when it never ticked —
  so a sampler that watched 3 % of the phase is visible as such whatever
  `blind` says. Errors beside observed changes are not blindness: the sampler
  did see the revision move.
- A frontend that restarted or died during the phase is reported, and its
  windows are marked `incomplete` with the reason. The last landing's window is
  marked `span_truncated_at_phase_end`.
- Every rebuild-pending rejection and every bump belongs to exactly one landing
  or to `unattributed`; the counts are reconciled in
  `rejection_attribution` and `bump_attribution`.
- `landings` and `windows_available` count the landings that were granted an
  attribution span, which is exactly the ones that have a window: a span is
  granted on the landing's own pool tip change, whatever its outcome. That is
  usually the same as `landing_outcomes.landed` and differs when a landing's
  tip moved but the node kept no submission record
  (`accepted_without_node_submission`). The outcome tally is reported beside
  them, so "a window exists" and "a window is reported as existing" cannot
  disagree.

### Running it

```sh
target/release/qbit-prism-load \
  --server-bin target/release/qbit-prism-server \
  --pg-bin-dir /usr/lib/postgresql/16/bin \
  --frontends 2 --sessions 100 --window-shares 20000 \
  --replication async --plan short --rate 50 \
  --cadence dense --scheduled-blocks 12 \
  --out load-out
```

## Exit codes

| Code | Meaning |
|---|---|
| 0 | The run completed and reconciled exactly |
| 2 | The harness failed with an error: before it could measure anything, or, rarely, while reconciling or writing its outputs after the load. The error is printed on stderr, redacted. Once the invocation has taken `--out`, the side report is written with `failed.error` naming the failure and nothing that could be read as a measurement; a failure after the phases does not recover their numbers. No artifact is written |
| 3 | Blocked: no frontend served work, or a frontend log showed a hard refusal of the size -- at startup, or at any later point in the run. A refusal logged after the startup check (a scheduled-block rebuild hitting the JSONB ceiling, say, while ordinary shares kept flowing) is re-checked once the load has stopped and every frontend has been stopped and reaped, so a rebuild still running when the sessions drained cannot log a refusal after the check has read the log: the artifact is withheld, `blocked.blocked` is `true` with the line under `blocked.error`, and the side report carries every number the run produced. A run blocked at startup writes only the side report. Either way an earlier run's artifact and profile were already removed when the invocation took `--out` |
| 4 | A durability loss: an acknowledged share is missing from PostgreSQL, a committed share was never acknowledged and nothing explains it, or PostgreSQL holds a run-prefixed row that no phase offered |
| 5 | An ACK/commit divergence: PostgreSQL holds a share whose acknowledgement never reached the client, for a reason inside the run. The server refused it with `ledger-confirmation-failed` or with `ledger-outcome-unknown` (#324), or the socket closed mid-run before its answer was read. Nothing was lost in any of the three. A submit still outstanding when the drain expires does **not** exit 5: see below |
| 6 | The run was aborted: the memory floor was crossed, a frontend exited, the `reconnect` phase's drained restart could not be performed because the frontend's sessions still had submits outstanding after the share-commit timeout plus the 10 s drain margin, the `mid_flight_kill` phase's relaunched frontend exited or did not answer `/healthz` within `--work-timeout`, its session accounting or collector barriers did not finish within the share-commit timeout plus the 10 s drain margin, a phase boundary could not change the proxy delay because the previous phase's submits were still outstanding after that same limit, or a delayed phase's round trip through the proxied URL did not pay the delay. No `capacity-evidence.json` is written (and an earlier run's was already removed when the invocation took `--out`), so an aborted run can never leave a self-validating artifact behind; the side report is still written, with `aborted` set, the cut-short phase marked `completed: false`, and `validator.artifact_written: false` with the reason |
| 7 | Rejections classified as harness bugs |
| 8 | A premise of the measurement was contradicted. Either a frontend advertised, in `mining.set_difficulty`, a share difficulty other than the one the harness configured in `PRISM_STRATUM_SHARE_DIFF` -- the client mines the configured target either way, so with a lower advertised value its shares are still accepted and an artifact would validate while measuring a different amount of work per share than the configuration names; checked once every session holds work, before any phase, and again after the load stops -- or the replication mode observed in `pg_stat_replication` is not the one `--replication` declares, or could not be observed at all; checked at entry, before a frontend is launched, and again after the load stops. The artifact is withheld and the side report's `premise` block carries every difficulty mismatch with its session, advertised and configured values, and the declared and observed replication modes with the reason when one could not be observed |

## Outputs

`--out` receives four things. Each invocation takes the directory for itself
first: any `capacity-evidence.json`, `database-profile.json` or
`load-harness-report.json` an earlier run left there is removed before
anything else happens, and the side report lists what was removed under
`stale_outputs_removed`. So however this invocation ends -- blocked before a
frontend served work, aborted mid-run, failed with an error, or complete --
the directory holds only this invocation's outputs, and never an earlier
run's self-validating artifact beside this run's blocked or aborted report.
A run that fails with an error after taking the directory leaves a side
report naming the failure under `failed.error` rather than an empty
directory.

### 1. `capacity-evidence.json`

Schema `qbit-prism-capacity-evidence/v3`, exactly the shape
`crates/qbit-prism-server/src/capacity.rs` validates: `TOP_KEYS`,
`SUBJECT_KEYS`, `durability`, the `CONFIGURATION_KEYS` read out of the exact
environment the frontends were launched with, and each phase's extra keys. The
key list is taken from that module rather than copied, so a server that adds or
retires one moves this artifact with it.

`durability` is read back from PostgreSQL on a connection carrying the ledger's
own session settings. If any of `fsync`, `full_page_writes` or
`synchronous_commit` is not `on`, the harness aborts before the load.

### 2. `database-profile.json`

Schema `qbit.prism.database-profile.v1`: canonical JSON with sorted keys,
carrying `pg_settings`, the replication mode and rows, the proxy configuration,
host facts and any cgroup limits. Its SHA-256 is the artifact's
`subject.database_profile_sha256`, over the file's exact bytes: the document is
written with no trailing newline, so `sha256sum database-profile.json` is the
value the artifact names and a third party can verify the bundle with one
command. The repository defines no schema for this document; the harness writes
one and ships it beside the artifact.

### 3. `load-harness-report.json`

Schema `qbit.prism.load-harness.v1`. Everything the artifact cannot carry: the
host, versions and build profiles, the redacted frontend environment, window
sizes requested, computed and read back, per-phase measurements (a submit
belongs to the phase whose scheduler offered it, even when the session sent
it after that phase's boundary), the
reconciliation definition and results, rejections by `(code, reason_id,
message)` per phase and frontend, reconnect statistics, time to usable work,
the mid-flight-kill census, blocked-run records, the honest-value notes, the
validator verdict and the exact `capacity-evidence` command line.

The `dense_cadence` key is added by `--cadence dense` and holds the gap
pattern, the landing list, the bump list on both clocks, the per-landing and
per-frontend window tables, the run summaries and a `definitions` block stating
exactly how each window and time is measured and on which clock. It is
additive: every other key keeps its shape, and the artifact is untouched.

Each phase's `server_share_ack_seconds` is the frontend's own
`qbit_prism_share_ack_seconds` histogram over the phase, one entry per
frontend. A frontend restarted during the phase resets its counters, so the
entry says how: `counter_resets` is the number of times its process was
replaced, `segments` the number of per-process segments summed into `counts`,
and `drained_restarts` on the phase carries the timings and whether each side
of the restart was scraped. A reset no scrape bracketed (a mid-flight kill)
leaves `counts` empty with `unavailable_reason` set: unknown, which is not
zero. A counter that went backwards between two scrapes with no recorded
restart is reported the same way rather than as a negative number.

The frontend environment is printed redacted, here and in
`database-profile.json`: the RPC password and the signing seeds are replaced
outright, and any URL-valued variable loses the password in its userinfo and
the value of any `password` query parameter, the parameter's key compared
percent-decoded as SQLx reads it. So a `--database-url` of
`postgresql://user:secret@host/db` is recorded as
`postgresql://user:<redacted>@host/db`, and the same rule covers every other
URL the harness records. The free-text sinks redact as well, whatever their
sources did: a failure report's `failed.error` and the error the harness
prints on stderr have every URL-shaped token and every libpq `password=`
value redacted, and so does every `pg_settings` value in
`database-profile.json`, because `primary_conninfo` on a promoted standby
carries a password and a superuser sees it unmasked.

### 4. Logs

`logs/load-fe-<i>.stdout.log` and `logs/load-fe-<i>.stderr.log` per frontend,
plus the fake node's submission log inside the side report. Each invocation
starts these files empty: reusing an `--out` directory does not carry an
earlier run's refusal into this run's blocked-log classification. A frontend
restarted within the run appends, so what the killed process logged stays.

## Validating the artifact

The harness validates its own artifact in process, with
`ValidationOptions` carrying the exact configuration, subject, forecast and
limit the run used, and records the verdict. It also prints the equivalent CLI:

```
qbit-prism-server capacity-evidence load-out/capacity-evidence.json \
  --expect PRISM_STRATUM_SHARE_DIFF=… \
  …one --expect per configuration key, 16 in all… \
  --expect-coordinator-revision … \
  --expect-coordinator-image-digest … \
  --expect-postgres-server-version … \
  --expect-database-profile-sha256 … \
  --forecast-peak-shares-per-second … \
  --ack-p99-limit-milliseconds …
```

### Expect the 2× gate to fail at a realistic forecast

Every accepted share runs about eight statements while holding the global
`ORDER_LOCK` advisory lock, so shares are serialized cluster-wide at the
database. Under a 10 ms one-way delay each of those round trips pays the delay
twice, and the `slow_database` phase sustains only a low rate. The validator
requires every phase to sustain at least twice the forecast peak, so at D1's
forecast of 2,000 shares/s that phase's gate fails.

**That is a result, not a harness bug.** The artifact is emitted anyway, the
run exits 0 if it completed and reconciled, and the verdict is reported. To
demonstrate a validating artifact, run again with a forecast no higher than
half the slowest phase's rate; the side report prints that number as
`validator.suggested_forecast_for_a_valid_artifact`.

Two other gates bind in the same phase, and the side report prints what each
of them would need:

- **The ACK p99 limit.** Under delay the phase's client ACK p99 runs into
  seconds, well past the 1,000 ms default. `--ack-p99-limit-ms` can be raised
  as far as `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` × 1000 and no further, because
  the consumer refuses anything above it;
  `validator.suggested_ack_p99_limit_milliseconds` is the rounded-up value the
  run would need, capped at that ceiling. When the observed p99 is above the
  ceiling, no limit admits the phase, and that too is a result.
- **`offered == acknowledged`.** Under delay the frontends' readiness poll goes
  stale behind the blocked refresh loop and submits are refused with `current
  chain state is unavailable`. Those refusals are counted, so the phase reports
  more offered than acknowledged.

Because the forecast and the limit are recorded in the artifact and checked
against the expectations, changing either means running again, not
re-validating the same file.

## Honest values

The side report repeats all of this under `honest_value_notes`.

- **`subject.coordinator_image_digest` is a binary digest.** There is no OCI
  image: it is `sha256:` followed by the SHA-256 of the `qbit-prism-server`
  executable's bytes. A validator run with `--expect-coordinator-image-digest`
  set to a real image digest will correctly reject it. Never fabricate a value
  that looks like an image digest.
- **Retired configuration keys are left out, not explained** (#361). Three keys
  are retired: <!-- retired-setting: PRISM_SHARE_COMMIT_BATCH_SIZE -->
  <!-- retired-setting: PRISM_SHARE_COMMIT_LINGER_MILLISECONDS -->
  <!-- retired-setting: PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS -->
  the batch size, the commit linger and the vardiff idle sweep. The native
  server does not read them, and the `v3` validator refuses evidence that names
  one as not having measured the native binary. So the harness neither sets them
  on a frontend nor records them, and lists them under
  `retired_configuration_keys` in the side report. Under `v2` it carried them with the values the frontends
  really used and annotated them as unread (#288); the server has since answered
  that question, so leaving them out is now the honest answer rather than the
  lossy one.
- **A tail the measurement window cut off is reported, not counted as a
  divergence.** A submit still outstanding when the teardown drain expires had
  its window end underneath it: the server is still allowed to answer, and in
  production the connection would still be there to carry the answer. Such a
  share, if PostgreSQL then holds it, is in `no_response_commits` with
  `window_ended: true`, and the run says so on stderr, but it does not by itself
  make the run exit 5. Only a no-response caused *inside* the run does.
  Measured reason: under 2000 sessions against a delayed database the
  `slow_database` ACK p99 was 25.3 s against a 25 s drain limit, so a tail is
  near-certain. Exiting 5 for it would have fired on nearly every run and left
  the code unable to distinguish a real divergence from where the run stopped.
- **An ACK/commit divergence is counted, never smoothed over.** A share
  PostgreSQL holds after the server refused it is in `ack_commit_divergence`,
  in `unexpected_committed_share_ids` and in `rejected_valid_shares`, and it
  exits 5. The server-side bug is #324. It is reported apart from a durability loss because only a loss
  means credited work disappeared.
- **An unknown outcome is kept apart from both.** #333 answers
  `ledger-outcome-unknown` when the COMMIT still has no reply after the commit
  deadline and its grace window: the server is saying it does not know whether
  the append landed. A share PostgreSQL then holds is in
  `unknown_outcome_commits` rather than in `ack_commit_divergence` or in
  `durability_findings`, and it exits 5 as well.
- **A lost response is not a lost share.** A socket that closes after
  PostgreSQL commits a submit but before the client reads the answer leaves a
  `no-response` record, and reconciliation then finds the row. The share is
  present; only its acknowledgement is missing, and whether the server ever
  sent one cannot be known from this side. That is transport-indeterminate,
  and it is in `no_response_commits` with the reason the socket gave, not in
  `durability_findings`: it used to be read as a durability loss, a false
  data-loss alarm that is a stop-and-ask for whoever reads the report. It
  exits 5, because "we do not know" is neither "it was lost" (4) nor "it was
  fine" (0). Four separate buckets, then, because they are four different
  claims: the server said no and was wrong, the server said it did not know,
  the client never heard, and nothing explains the row at all. Only the last
  is a durability bug.
- **`offered_valid_shares`** counts shares the harness believed valid when it
  offered them: every acknowledged share, plus every rejection that is not a
  race the server was entitled to lose, plus every submit that received no
  response. Only the transient rejections are excluded — `stale-job` after a
  tip change or a payout-revision bump, an unknown or retired job, a closed
  pool — and they are reported in full under `rejections`. None of those
  persists a share, so none can affect reconciliation.
- **A backend refusal is counted, not hidden.** `current chain state is
  unavailable` and `share was not confirmed by the database` are capacity
  results rather than harness defects, so they do not make the run exit
  non-zero, but they stay in `offered_valid_shares` and in
  `rejected_valid_shares`. The artifact is then honestly invalid for that
  phase. Only the harness-bug classes — `low-difficulty`, `malformed-submit`,
  `duplicate-share`, every `invalid-*` and `unauthorized-worker` — exit 7.
- **An unrecognised rejection reason is loud.** A rejection whose
  `reason_id` the classifier does not recognise is class `unknown`: it stays
  in `rejected_valid_shares`, so the artifact is already invalid, and it
  means the harness's model of the server's rejections is out of date, which
  is the reader's problem to solve. The summary the run prints names every
  such reason with its code, message and count on its own line, the
  validator's refusal reason ends with the same line when the artifact was
  written and refused, and `validator.unrecognised_rejection_reasons` in the
  side report lists them. The exit code is unchanged: an unrecognised reason
  is not a harness bug (7) and not a loss (4), and exit codes are a contract
  other tooling reads.
- **Every dispatched offer is accounted for.** A phase's `dispatched` is what
  its scheduler placed, and each placed offer ends as a submit record, a
  discarded offer (the session was paused or stopped before it sent) or an
  offer that failed before a submit line was written (no job to mine, no
  solution found). Client failures used to be collected under
  `client.failures` and read by nothing, so `dispatched` and the submits a
  phase recorded could disagree with no account of why. Each failure now
  carries its phase and what the session was doing (`offer`,
  `scheduled-block`, `reoffer` or `line`), each phase carries an
  `offer_accounting` that reconciles `dispatched` against those three
  buckets, and what none of them explains is `unaccounted`, reported rather
  than assumed away. An offer whose write failed is already a no-response
  submit record and is listed apart so it is not counted twice. The printed
  summary adds an `offer accounting` line under a phase whenever it had a
  client failure or an unaccounted offer.
- **ACK latency is client-measured, over acknowledgements only**: from
  writing the submit line to reading the response line that accepted it, on
  the client's monotonic clock, per phase and overall. A refusal is not an
  acknowledgement -- the server answers one in microseconds because it never
  reached PostgreSQL for it -- so refusals are kept out of `ack_p50_millis`
  and `ack_p99_millis`, where they once pulled a phase's p50 to 0.7 ms while
  its accepted shares were taking seconds, and are summarised apart in each
  phase's `client_rejection_latency`. A submit that got no response has no
  latency in either. The server's own
  `qbit_prism_share_ack_seconds` histogram measures a narrower, server-side
  boundary (complete frame receipt to completed response write) with 10/25/50/
  100 ms buckets; it is reported separately as per-phase bucket deltas.
- **`database_delay_milliseconds` is the one-way per-chunk proxy delay.** A
  round trip pays it twice. The configured delay and the measured added
  round-trip time are both recorded under `delay_proxy`.
- **A delay is reported only after it was seen to be paid.** Before the run,
  with the `slow_database` delay set, and again before every phase with that
  phase's delay set, a `SELECT 1` round trip is timed through the proxied URL
  exactly as the frontends received it. Each direction is held once and the
  proxy's sleep never returns early, so a proxied trip costs at least twice
  the delay; one that comes back sooner did not go through the proxy. The
  entry check refuses to start, and a phase that fails it aborts the run
  (exit 6, artifact withheld) rather than reporting a delay nothing applied.
  Every phase's observation is in its `phases[]` entry as
  `database_delay_observed_select1_median_milliseconds`, with the floor beside
  it, and the entry check's under `delay_proxy`.
- **A phase's numbers were produced under the delay it reports.** The proxy
  reads its delay per chunk, so the delay is changed at a phase boundary only
  once every submit offered under the previous delay has been answered:
  nothing is offered while that settles, the wait is recorded in the next
  phase's entry as `previous_phase_settled_before_delay_change_seconds`
  (`null` when the delay did not change). If they have not settled after
  the share-commit timeout plus the 10 s drain margin the delay is left
  alone and the run aborts (exit 6) rather than let a `reconnect` submit
  pay the slow-database
  delay or a `slow_database` submit finish without it. The same limit holds
  at teardown, where the last phase's delay stays on until its submits have
  settled: the sessions are paused and given the share-commit timeout plus
  the 10 s drain margin, the report's `drain` block records the limit, how
  long it took and what was still outstanding, and only a submit the
  server's own deadline had already passed is then recorded as
  `no-response` in its phase.
- **The drain margin is wider than the server's commit grace.** The server
  goes on waiting `share_commit_grace` (5 s, set in
  `crates/qbit-prism-server/src/config.rs` and not an environment variable)
  past the share-commit timeout for a COMMIT reply before it answers
  `ledger-outcome-unknown`, so a submit can legitimately be answered up to
  the timeout plus that grace after it was sent. Every drain the harness
  derives waits the timeout plus a 10 s margin: strictly more than the
  grace, with the difference left for the answer to cross the socket and be
  read. The margin used to equal the grace, so every drain gave up at
  exactly the moment the server was still allowed to answer. The harness
  restates the server's value as `SERVER_SHARE_COMMIT_GRACE` rather than
  importing it, because the server does not export it, and a test reads the
  server's source to keep the two the same.
- **Two advisory locks are sampled, and reported apart.** Each phase carries an
  `order_lock` block and a `settlement_lock` block, same shape, same own/foreign
  split, from the same polls. `ORDER_LOCK` (`0x505249534d000002`) is what a
  share append takes: `Window::append_checked` takes it and no other.
  `SETTLEMENT_LOCK` (`0x505249534d000003`) is what the rebuild after a landing
  queues on first — `observe_chain_view`, the job build and candidate
  confirmation or abandonment all take it *before* `ORDER_LOCK` — so in the
  dense-cadence phase a large share of the frontends' ungranted advisory-lock
  rows are on it, and a harness watching `ORDER_LOCK` alone reported part of
  the queueing with no way for a reader to tell which part. Each block names
  the path that takes its lock in `taken_by`. `MIGRATION_LOCK` and
  `CPFP_FUNDING_LOCK` are not sampled: neither is on the append or rebuild
  path. Because one poll carries both, the two blocks share a `samples` count
  and a sampler cost, and cover exactly the same instants.
- **A PRISM advisory lock is database-wide.** Anything else in the same database
  taking the same advisory lock would distort every number in that block, so each
  frontend connects with `application_name=load-fe-<i>` and the sampler
  attributes rows by it. Whether the driver really carried the name is
  verified against `pg_stat_activity` rather than assumed: when it did not, the
  summary says so and counts every waiter in the database instead. Rows that
  are not this run's frontends are reported separately, and the holder is
  reported apart from the waiter because they are different events: a foreign
  *holder* blocks every frontend, and the frontends then queue up as waiters
  that really are this run's own, so a holder-blind sampler would bill the
  whole stall to them. The fields are `foreign_waiter_samples`,
  `foreign_application_names` and `foreign_waiter_seconds_estimate` for
  waiters, `foreign_holder_samples`, `foreign_holder_application_names` and
  `foreign_holder_seconds_estimate` for holders, and one
  `foreign_contention_observed` covering both — `null` rather than `false`
  when the sampler could not attribute rows at all. A frontend holding the
  lock is the normal case and is in none of them.
- **Advisory-lock waits are sampled**, not instrumented. The metric family
  `qbit_prism_database_advisory_lock_wait_seconds` exists but has no producer,
  so the harness samples every `ORDER_LOCK` and `SETTLEMENT_LOCK` row in
  `pg_locks`, left-joined to `pg_stat_activity`, every 10 ms, tags each row with
  its `objid` and reports a waiter-count summary per lock over the ungranted
  rows belonging to this run. The two locks are summarized apart: they are
  different queues taken by different server paths, and mixing them would
  report a rebuild's queue as a share append's. The join is on the left so a lock row
  whose backend the sampler's role cannot read still appears, and such a row
  counts as foreign — it cannot be shown to be one of this run's frontends, and
  that is the safe direction. Every poll carries the server's
  `clock_timestamp()`, including a poll that finds no lock at all, so a sample
  never mixes the harness's clock with the server's. Beside the counts it
  reports an episode count as "at least N", and a
  Riemann waiter-seconds estimate that is a lower bound: waits shorter than the
  sampling interval can be missed entirely. The sampler's own query cost is
  reported beside the numbers. When `pg_stat_statements` is loaded, the
  advisory-lock statement's `calls` and `total_exec_time` are recorded too, and
  the three PRISM locks share one normalized query text.
- **Every distribution names its own unit.** A percentile summary carries
  `unit` and `clock`. The five count distributions in the dense section —
  `tip_pending_rejections_per_landing`,
  `payout_pending_rejections_per_landing`,
  `combined_rebuild_pending_rejections_per_landing`,
  `rejected_before_new_revision_work_per_landing` and
  `lost_valid_shares_per_landing` — carry `"unit": "count"`, because they count
  shares. A consumer generic over the summary shape reads `unit`, so labelling
  a count "milliseconds" renders 85 discarded shares as "85 ms"; a carried unit
  that is wrong is worse than none. Their `clock` says what the sample is (one
  per landing and frontend) and which clock the rejections it counts were
  stamped on.
- **Unknown is never zero.** A measurement that could not be taken is `null`
  with a reason. A peak RSS from Linux's `VmHWM` is labelled a kernel peak; on
  macOS it is a sampled maximum, and on other platforms it is `null`. A
  per-phase replication observation whose view could not be read carries
  `error` with `synchronous_standby_names: null` and no rows, rather than
  the empty name and empty row list a primary with no standby shows; a
  `pg_stat_activity` the sampler could not read at startup is named as the
  reason every waiter is counted, rather than reading as "no frontend
  carried its name"; and a re-offer or scheduled block that reached a
  session while it had no connection is a `client.failures` entry of its
  kind, rather than a silent drop that read the same as an unanswered one.
- **Time to reconnect is the outage, not the last handshake.** The
  `reconnects` block's `time_to_reconnect_milliseconds` runs from the moment a
  session's connection went -- the socket closing, or the deliberate close
  after a client-initiated reconnect quiesced -- to the job that completes the
  reconnect, with every failed attempt and backoff in between. It used to
  restart on each attempt, so a frontend unavailable across several attempts
  reported a multi-second outage as the milliseconds its final handshake took.
  A failed attempt's `seconds` is how long the session had been without a
  connection when that attempt failed.
- **Time to usable work stops at the next tip.** `time_to_usable_work`
  credits a session with work on a tip only for a `mining.notify` that
  arrived while that tip was the node's tip; each entry records
  `replaced_after_milliseconds`. A notify for the tip after the node had
  moved on is a late job for a replaced tip, and counting it credited the
  tip with a session whose work really arrived under the next one. The
  dense section's per-frontend `time_to_new_tip_work` stops at the
  landing's span for the same reason.
- **The share difficulty is a premise, and a frontend that disagrees with it
  ends the run.** Every session checks each `mining.set_difficulty` against
  the configured share difficulty. A disagreement is not a finding to note
  beside the numbers: the window arithmetic, each share's weight and the
  artifact's rate all assume the configured target, and a frontend that
  advertised another value was measured at a different amount of work per
  share. The run refuses qualification -- the artifact is withheld, the
  `premise` block says which sessions saw what, and the exit code is 8 --
  once every session holds work, before a phase runs, and again after the
  load in case the value moved mid-run.
- **The replication mode is a premise too, and unknown is not `none`.** The
  run declares a mode in `--replication` and observes one in
  `pg_stat_replication`, at entry before a frontend is launched and again
  after the load. A run that declares an asynchronous standby and observes
  none is not measuring what it says, so a disagreement refuses
  qualification the same way: artifact withheld, `premise.replication`
  saying what was declared and what was observed when, exit 8. A view the
  role cannot read, or whose rows hide `sync_state`, is observed as
  `unknown` with the reason rather than as `none` in one direction or
  `async` in the other, and it is a contradicted premise as well: a run
  that cannot tell whether its standby exists has not established the
  conditions it claims. `database.replication` in the side report carries
  the same observations.

## Reconciliation

For each phase the harness takes the offered set O, the acknowledged set A, and
the committed set C — `qbit_share_ledger` rows that are `accepted`, whose
`writer_id` is one of the run's frontends, whose `share_id` carries the run's
username prefix, and which are in O.

- **`missing`** is A minus the database.
- **`unexpected`** is every run-prefixed database row that is in no phase's A,
  attributed to the phase in which it was offered, or to "outside phases".
  A row outside every phase is one the harness never saw offered, or a
  persisted rejection it kept out of the offered set as an entitled race;
  nothing explains it either way, so beside its count under
  `reconciliation.unexpected_outside_phases` it is a durability finding of
  its own kind, `committed share that no phase offered`, with the sample.
- **The digest** is SHA-256 in lowercase hex over the de-duplicated share
  identifiers sorted by UTF-8 byte order, each followed by one `0x0a` byte. The
  top level uses the union of the artifact phases. Byte order is Rust's and
  PostgreSQL's `COLLATE "C"`; the database's default collation may disagree, so
  the harness always sorts in process.

A gap between what was acknowledged and what PostgreSQL holds is one of two
different failures, and the harness never reports them as one thing.

- **A durability loss** is an acknowledged share the database does not hold, a
  committed share that was never acknowledged and that nothing explains, or a
  run-prefixed row that no phase offered at all. The run exits 4 with the
  evidence in the side report, and it is a stop-and-ask. A committed share
  whose submit got no response is not one of these: the acknowledgement was
  lost in transit, not the share, and it is reported under
  `no_response_commits` (exit 5) rather than raise a false alarm.
- **An ACK/commit divergence** is a share the database holds that the server
  had already refused with `ledger-confirmation-failed` / `share was not
  confirmed by the database`. Nothing was lost: the share is credited in the
  payout window, but the miner was told it was not confirmed. The mechanism is
  in `crates/qbit-prism-server/src/coordinator.rs`, which wraps the append in
  `tokio::time::timeout(share_commit_timeout, save)`; when that deadline fires
  the sqlx future is dropped mid-`COMMIT`, and PostgreSQL may still commit it.
  The server-side bug is filed as
  [#324](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/324).
  Those shares appear in `ack_commit_divergence` with their phase, frontend,
  session, send-to-response time and the commit deadline they crossed; they are
  counted in `unexpected_committed_share_ids` and in `rejected_valid_shares`,
  so the artifact cannot hide them; and the run exits 5.

Only the `mid_flight_kill` phase can legitimately produce an indeterminate
share. Those are re-offered with the same header, and each carries an
`outcome` in the side report's `mid_flight_kill.shares`, from the server's
answer to the re-offer and whether PostgreSQL holds the share:
`reoffer-accepted-and-committed`, `reoffer-accepted-not-in-postgres`,
`committed-before-kill` (the re-offer was a `duplicate-share` and the row is
there: only the acknowledgement was lost), `duplicate-not-in-postgres` (the
re-offer was a `duplicate-share` and the row is not there: the server
believes it has a share the database does not), `reoffer-rejected` and
`reoffer-unanswered`. The two in which the server and the database disagree
are `possible_losses`, and the duplicate case is also reported on its own
under `duplicate_not_in_postgres` with its share ids; it used to be recorded
and ignored. The printed summary names the possible losses. An accepted
re-offer is a new acknowledgement: if its row is missing, it also appears
in `durability_findings` and the run exits 4. A duplicate or unanswered
re-offer does not create an acknowledgement and keeps its existing census
classification without changing the exit code.

That census is the whole of what the kill exempts from the classification
above. The `mid_flight_kill` phase used to be skipped by it entirely, so a
share the phase acknowledged in the ordinary way -- before the kill, on the
healthy frontend, or after the relaunch -- that PostgreSQL then lost sat in
that phase's `reconciliation.missing` and reached no finding, and the run could
exit 0. Now every gap in the phase is classified as it is in every other
phase: an acknowledged share missing from PostgreSQL, or a committed row that
nothing explains, is in `durability_findings` under `mid_flight_kill` and
exits 4; a committed row whose submit got no response that the kill did not
cause -- a socket on the healthy frontend closing for its own reasons -- is in
`no_response_commits` and can exit 5. Only a committed row whose submit is one
of the kill's indeterminate shares is left to the census, where its re-offer
and its PostgreSQL outcome already are. A kill that found nothing outstanding
has an empty census and exempts nothing.

## Measurement hygiene

- Build release. Debug builds change every number.
- Run on a quiet host. The harness records the lowest `MemAvailable` it saw and
  stops the run if it falls below `--min-mem-available-mib`.
- Repeat and interleave runs. Take at least three of each configuration and
  interleave the frontend counts rather than running all of one and then all of
  another, so a drifting host shows up as spread rather than as a trend.
- Never mix hosts in one table. Every row carries its host; two hosts belong in
  two tables.
- Record the whole side report, not a number out of it. The forecast, the
  window size, the delay and the session count all move the result.
- The harness's own client cost is small — about 50 double-SHA-256 attempts per
  share at a 20,000-share window — but it shares the host with the frontends
  and PostgreSQL. Record its CPU too if the host is small.

## Blocked sizes

Some sizes are refused today. A refusal is a result, never something to work
around: the harness records the run as blocked with the error text from the
log, writes the side report and exits 3. That holds whenever the refusal is
logged. At startup no phase runs. Later in the run -- a candidate refused at
landing while ordinary shares keep being accepted -- the phases complete and
their numbers go into the side report, but the artifact is withheld and the
run still exits 3: the numbers describe a size the server did not serve in
full, and nothing downstream is guaranteed to notice a refused candidate.

| Size | Refused by | Unblocked by |
|---|---|---|
| Payout windows of 200,000 shares and above | PostgreSQL's JSONB container ceiling of 268,435,455 bytes, hit when the prepared job is persisted | #273 |
| Found-block candidates at a 400,000-share window | the audit bundle written at landing | #265 |
| Operator gates for large windows | — | #236 |

The blocked-log classifier keys on the refusal text PostgreSQL produces
(`jsonb` … `exceeds the maximum of`) inside the warnings the runtime logs
(`template refresh deferred`, `job preparation deferred`, `job persistence
deferred`).

## Tests

`cargo test --locked -p qbit-prism-load` is database-free and covers header
building and share-identifier derivation against the server's own `codec`,
digest canonicalisation, the window arithmetic, the fake node's chainwork,
height map, `submitblock` parent check and `waitfornewblock` wake-up, that the
frontend environment carries all 16 configuration keys, that the artifact
builder's output passes `validate_capacity_evidence` and fails once one
required field or one phase is removed, the rejection classifier, the
blocked-log classifier against the real refusal message, the dense-cadence gap
generator and its entry validation, the attribution of synthetic rejections and
bumps to landings (including the unattributed ones), and the no-landing
report.

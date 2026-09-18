# PRISM throughput measurements (#271, #447)

This document records throughput matrices measured by the `qbit-prism-load`
harness. The harness drives real `qbit-prism-server` frontends over real
Stratum sockets, with real proof of work, against a PostgreSQL primary and
standby that it manages. None of these numbers sets a CI floor. None is a
capacity claim for other hardware.

It has two parts, and **no table mixes them**:

- **The #271 matrix** on base `5d0042f6`, on a shared Linux VM: the 20k and
  100k windows. It is everything from
  [What the matrix established](#what-the-matrix-established) to
  [What was not measured](#what-was-not-measured), and within it every number
  comes from that one host and that one build.
- **[The production-window matrix (#447)](#production-window-matrix-on-a1937054-447)**
  on base `a1937054`, on a laptop: the 200k, 400k and 500k windows that #271
  could not run, found blocks under load and dense cadence. It has its own
  host table, method notes and tables.

Several statements in the #271 part were true of its base and are no longer
true of `3.x.x`: the three large windows run, found blocks land at 400k, and
dense cadence has been measured at D1 at the production window. Each carries
a pointer to the #447 part rather than being rewritten, because it is still
an accurate record of what that base did.

This document does not change `docs/prism-capacity-readiness.md`, which #303
owns. The raw side reports, artifacts, frontend logs and host samples are kept
outside the repository, and this document cites them by run id.

## What the matrix established

- **No configuration reaches 500 shares/s.**
  - At the 20k window, every configuration's median `steady_state` rate fell
    between 238 and 304 shares/s against the 500 shares/s target. Individual
    runs ranged from 235 to 320 shares/s across three repeats.
  - The 2,000 shares/s burst achieved no more.
  - **Adding frontends does not add throughput.** Summed over all frontends,
    CPU stayed at or below 0.65 cores in every phase.
  - **`ORDER_LOCK` waiters saturate every pool.** The waiter queue peaked at
    the frontends' total database connections minus the holder: 15, 31 and 63
    at 1, 2 and 4 frontends. It averaged 85–94 % of that in `steady_state`.
    The share append takes that one advisory lock, so appends serialise on it.
- **Synchronous replication costs 12–18 % against asynchronous, consistently.**
  - Median `steady_state` was 290, 304 and 288 shares/s async against 238, 251
    and 254 shares/s sync, at 1, 2 and 4 frontends. That is 18 %, 17 % and
    12 %.
  - Every sync run was slower than every async run at the same frontend count.
- **200k, 400k and 500k are all refused by the JSONB ceiling (#273)** at
  startup, with the verbatim refusal in [Blocked sizes](#blocked-sizes). On
  `a1937054` all three run; see
  [The blocked sizes now run](#the-blocked-sizes-now-run).
  - **At 400k**, #265 (found-block candidates) would refuse next, but it cannot
    be reached. See [The 400k result](#the-400k-result), which is what #271
    stays open for.
- **100k at 4 frontends does not fit on this host.**
  - The async attempt stopped itself at the harness memory floor, at
    `MemAvailable` 6,092 MiB against the 6,144 MiB floor, 64 s into
    `steady_state`.
    That run is the load-bearing evidence for this conclusion.
  - The sync attempt was stopped from outside by the coordinator's memory guard
    about 3 minutes in, so it shows only that free memory fell past an external
    threshold, not that the harness declined to continue. It is reported for
    completeness rather than relied on.
  - At 1 frontend, 100k fits and the ceiling holds at five times the window:
    285 shares/s async and 238 shares/s sync.
- **ACK latency now counts acknowledgements only.**
  - The median `slow_database` ACK p50 is 12.2–15.3 s on this base. On
    `2e94136c` the same column read 0.4 ms, because refusal response times were
    blended into acknowledgement latency.
  - Those earlier figures are kept under `base-2e94136` for comparison. The
    harness fix is why they moved; see
    [Comparison with the earlier harness bases](#comparison-with-the-earlier-harness-bases).
- **Repeats.** The 20k tables have three repeats per configuration. The 100k
  tables have two, which cannot identify an outlier.
- **Other results.**
  - Every run's capacity evidence is refused, as expected.
  - No completed run on this base recorded an ACK/commit divergence (#324), an
    unknown-outcome commit or a durability finding.
  - The replication premise agreed at entry and after the load in every run.
- **The dense-cadence block was dropped.** At D1 the harness correctly refuses
  to start the dense phase; see [Dense cadence](#dense-cadence). #447
  attempted it again; see [Dense cadence at D1](#dense-cadence-at-d1).

## Host

Every table in this document comes from this one host. No table mixes hosts.

| Fact | Value |
|---|---|
| Host | alexdevbox2, a shared development VM |
| CPU | Intel Core Processor (Haswell, no TSX), 8 vCPU, x86_64 |
| RAM | 22.9 GiB (`MemTotal` 24,026,912 KiB), no swap, no cgroup CPU or memory limit |
| Disk | 200 GiB QEMU virtual disk, ext4 root; the managed clusters live on the same disk |
| OS | Ubuntu 24.04.4 LTS, Linux 6.8.0-106-generic |
| PostgreSQL | 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1), managed by the harness: one primary and one standby per run |
| Durability | `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`, read back from the primary for every run |
| Build | release profile for both `qbit-prism-server` and `qbit-prism-load`, built from `5d0042f629f3271ef6743ec9d859cb0c35018f31` with Rust 1.98.1 stable |
| Frontend settings | harness defaults: `PRISM_RUNTIME_WORKERS=2`, `PRISM_DATABASE_MAX_CONNECTIONS=16`, `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS=15`, `PRISM_BLOCKPOLL_SECONDS=2` |

**The host was shared.** Other users' agent sessions, their `git` processes
and two unrelated PostgreSQL containers ran on it throughout. For each run the
harness driver records three things: the 1-minute load average and
`MemAvailable` just before the run (after a 60 s cooldown), the same two every
10 s during the run, and a process snapshot at each end. The load average and
lowest `MemAvailable` are reported beside each run's numbers. Before a run, the
load average was 0.8–2.8 across the whole matrix. The load average during a
run includes the run's own PostgreSQL, frontends and client.

## Method

- **Plan.** D1 (`--plan d1`) runs these phases in order:
  - a 30 s `warm_up`, which is not in the artifact;
  - `steady_state` at 500 shares/s for 300 s;
  - `burst` at 2,000 shares/s for 60 s;
  - `reconnect` at 500 shares/s for 60 s, with one drained frontend restart;
  - `slow_database` at 500 shares/s for 60 s, behind a 10 ms one-way proxy
    delay.
- **Load shape.** 2,000 Stratum sessions, round-robin across the frontends,
  each with one outstanding submit at a time.
- **Validator inputs.** Forecast peak 2,000 shares/s; ACK p99 limit 1,000 ms.
- **Memory floor.** `--min-mem-available-mib 6144`, above the external
  memory guard's threshold of 25 % of `MemTotal` (5,866 MiB).
- **Order and repeats.**
  - Runs were strictly one at a time, with a 60 s cooldown between them.
  - **20k: three repeats per configuration**, interleaved. One pass runs async
    at 1, 2 and 4 frontends, then sync at 1, 2 and 4, and the next pass repeats
    that order.
  - **100k: two repeats per configuration**, at 1 and 4 frontends only, by
    coordinator decision.
- **Cells.** Each cell shows the median of the repeats, with the minimum and
  maximum in parentheses.
- **Two repeats cannot identify an outlier.** A two-run cell's "median" is the
  mean of the two runs, and its range is just those two runs. It carries less
  confidence than a three-run cell. At 20k, the `steady_state` rate's spread
  across three repeats reached 14.4–14.5 % of the median at 1 and 4
  frontends, async.
- **Units.**
  - Rates are shares per second.
  - ACK latency is measured on the client, from writing the submit to reading
    its response, in milliseconds on the client's monotonic clock.
  - On this base, ACK latency covers **accepted shares only**. Refusal latency
    is reported separately.
- **Phase abbreviations.** In the tables, *steady* is `steady_state`, *reconn*
  is `reconnect` and *slowdb* is `slow_database`.
- **Achieved and offered.** "Achieved" is acknowledged shares per second.
  "Offered" is what the client actually sent. Sessions wait for their previous
  response, so offered falls below the target when responses are slow.
- **Advisory-lock waits are sampled.** The harness polls `pg_locks` every
  10 ms, so waiter-seconds is a lower bound. The `pg_stat_statements` columns
  give the calls and total execution time of the one normalized advisory-lock
  statement during the phase. That execution time includes time spent waiting
  for the lock.
- **Frontend CPU** is the mean number of cores over the phase. *Sum* adds all
  frontends together; *max* is the busiest single frontend.
- **Memory.** **RSS** is the maximum sampled resident set. **VmHWM** is the
  kernel's peak.
- **Teardown tails.** A submit still outstanding when the run's 25 s drain
  expires, and which PostgreSQL then held, is reported as a no-response commit
  with `window_ended: true`. This is not a divergence, and the run still exits
  0. Tails are listed per run below.

## Replication premise probes

Two short probes ran before the matrix to check the replication premise
against a real managed cluster. Both used 2 frontends, 200 sessions, the short
plan at 50 shares/s, and the 20k window.

| Run id | Exit | Declared | Observed at entry | Observed after load | Agreed | Share difficulty agreed |
|---|---|---|---|---|---|---|
| w11-premise-fe2-async-short50 | 0 | async | async | async | true | true |
| w11-premise-fe2-sync-short50 | 0 | sync | sync | sync | true | true |

In both probes, every one of these was 0: no-response commits, ACK/commit
divergences, unknown-outcome commits, durability findings, submits outstanding
at stop, harness-bug rejections, and unaccounted offers in any phase.

## 20k window: D1 matrix (three repeats)

All 18 runs exited 0. Every artifact is refused; see
[Capacity-evidence verdicts](#capacity-evidence-verdicts).

### Rate and ACK latency

| fe | repl | n | steady achieved/offered (target) /s | steady ACK p50 / p99 / max ms | burst achieved/offered (target) /s | burst ACK p50 / p99 / max ms | reconn achieved/offered (target) /s | reconn ACK p50 / p99 / max ms | slowdb achieved/offered (target) /s | slowdb ACK p50 / p99 / max ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | async | 3 | 290 (278–320) / 350 (344–366) (500) | 6,186 (5,679–6,351) / 8,638 (8,497–10,794) / 12,935 (12,456–15,649) | 278 (274–300) / 598 (552–610) (2000) | 6,422 (5,983–6,552) / 11,000 (10,217–12,056) / 15,857 (15,567–18,902) | 301 (289–303) / 364 (342–368) (500) | 6,100 (5,804–6,410) / 9,234 (7,498–9,836) / 12,572 (7,862–13,578) | 3.9 (2.3–4.1) / 322 (316–322) (500) | 12,619 (11,931–12,802) / 30,364 (30,306–30,485) / 30,506 (30,396–30,519) |
| 2 | async | 3 | 304 (299–313) / 384 (370–385) (500) | 6,089 (5,763–6,190) / 8,462 (7,559–9,123) / 15,463 (13,500–16,614) | 267 (232–308) / 762 (687–960) (2000) | 6,670 (5,878–7,344) / 9,767 (9,479–12,920) / 18,133 (15,564–20,134) | 283 (264–339) / 372 (343–383) (500) | 6,001 (5,462–6,104) / 7,581 (6,419–10,832) / 15,925 (8,942–15,951) | 2.8 (2.6–3.9) / 413 (407–416) (500) | 14,705 (14,595–14,716) / 32,323 (29,989–33,613) / 32,746 (29,995–33,646) |
| 4 | async | 3 | 288 (266–307) / 410 (379–412) (500) | 6,359 (5,987–6,762) / 7,930 (7,562–9,909) / 15,365 (14,176–19,114) | 251 (244–295) / 1,283 (1,018–1,603) (2000) | 7,483 (6,390–7,552) / 9,450 (8,971–10,878) / 20,011 (19,365–21,943) | 271 (253–302) / 416 (412–428) (500) | 6,350 (5,692–6,599) / 7,230 (7,077–9,155) / 10,674 (9,594–13,569) | 5.9 (5.8–6.2) / 414 (412–418) (500) | 15,346 (14,152–16,512) / 29,014 (28,204–34,085) / 29,848 (29,304–34,658) |
| 1 | sync | 3 | 238 (235–238) / 325 (320–327) (500) | 7,321 (7,251–7,441) / 11,675 (11,524–11,840) / 18,166 (17,557–19,077) | 231 (227–243) / 711 (566–860) (2000) | 7,910 (7,607–7,970) / 13,538 (13,237–14,494) / 21,370 (20,040–21,437) | 249 (239–262) / 333 (289–357) (500) | 7,275 (7,202–7,391) / 8,207 (8,152–8,286) / 8,414 (8,319–8,865) | 4 (1.7–4.2) / 320 (310–322) (500) | 12,204 (9,188–12,727) / 30,334 (19,019–30,397) / 30,386 (29,935–30,407) |
| 2 | sync | 3 | 251 (248–256) / 352 (349–361) (500) | 7,217 (7,075–7,222) / 10,768 (10,306–11,477) / 16,944 (16,869–17,402) | 225 (210–238) / 810 (779–864) (2000) | 7,740 (7,430–7,839) / 12,999 (11,216–13,980) / 20,713 (19,475–21,194) | 261 (256–262) / 350 (348–382) (500) | 6,900 (5,567–7,060) / 8,114 (7,833–8,492) / 8,540 (7,945–9,009) | 3.9 (3.8–4.5) / 433 (396–440) (500) | 13,931 (13,493–14,296) / 34,143 (30,221–34,234) / 34,279 (32,038–35,252) |
| 4 | sync | 3 | 254 (248–264) / 382 (378–393) (500) | 7,240 (6,810–7,283) / 10,790 (10,270–10,926) / 20,286 (18,911–20,851) | 224 (218–253) / 1,270 (876–1,274) (2000) | 7,565 (7,542–7,994) / 13,115 (10,448–13,997) / 20,959 (19,470–21,341) | 245 (239–272) / 411 (400–442) (500) | 7,430 (6,553–7,502) / 9,306 (9,042–10,434) / 12,416 (11,171–17,749) | 5.1 (4.8–5.3) / 421 (407–442) (500) | 14,644 (14,517–16,404) / 29,148 (28,942–29,359) / 29,696 (29,625–30,034) |

### Refusal latency

A refusal's latency runs from writing the submit to reading the refusal. The
count of refusals is in the last parentheses.

| fe | repl | steady refusal p50 / p99 ms (refusals) | burst refusal p50 / p99 ms (refusals) | reconn refusal p50 / p99 ms (refusals) | slowdb refusal p50 / p99 ms (refusals) |
|---|---|---|---|---|---|
| 1 | async | 0.5 (0.5–0.5) / 9,191 (8,288–10,859) (18,149 (13,592–19,660)) | 0.5 (0.4–0.5) / 95 (6–484) (19,205 (15,111–20,144)) | 0.5 (0.4–0.5) / 500 (290–500) (3,978 (2,329–4,466)) | 0.5 (0.5–0.5) / 29,716 (29,643–29,741) (19,105 (18,721–19,186)) |
| 2 | async | 0.5 (0.5–0.5) / 8,248 (8,080–9,950) (24,352 (16,808–25,340)) | 0.5 (0.4–0.5) / 338 (154–417) (31,782 (22,773–41,533)) | 0.5 (0.5–0.5) / 378 (344–483) (4,697 (2,646–5,385)) | 0.5 (0.4–0.5) / 29,754 (29,496–29,812) (23,881 (23,609–24,183)) |
| 4 | async | 0.5 (0.5–0.6) / 7,753 (6,192–9,963) (34,002 (31,267–36,658)) | 0.5 (0.5–0.5) / 216 (101–323) (61,910 (43,422–81,547)) | 0.5 (0.5–0.5) / 454 (413–470) (8,739 (7,499–9,506)) | 0.5 (0.5–0.5) / 28,284 (26,028–29,031) (24,214 (24,104–24,419)) |
| 1 | sync | 0.5 (0.5–0.5) / 12,953 (10,222–13,062) (26,826 (24,822–26,983)) | 0.5 (0.5–0.5) / 441 (416–479) (28,834 (19,416–37,990)) | 0.5 (0.4–0.5) / 500 (500–500) (4,291 (2,372–7,072)) | 0.5 (0.5–0.5) / 29,665 (29,249–35,612) (18,927 (17,392–19,099)) |
| 2 | sync | 0.5 (0.5–0.5) / 8,754 (8,099–9,603) (30,307 (30,304–31,484)) | 0.5 (0.5–0.5) / 315 (149–405) (35,091 (32,452–39,242)) | 0.4 (0.4–0.5) / 470 (182–492) (5,614 (5,268–7,224)) | 0.5 (0.5–0.5) / 30,011 (29,874–42,087) (24,777 (22,995–25,751)) |
| 4 | sync | 0.5 (0.5–0.5) / 7,786 (7,375–9,838) (40,349 (34,140–41,688)) | 0.5 (0.5–0.5) / 105 (100–413) (60,969 (39,077–63,358)) | 0.5 (0.5–0.5) / 466 (356–471) (9,942 (7,667–12,179)) | 0.5 (0.5–0.5) / 25,199 (22,889–25,914) (24,964 (24,130–26,174)) |

### Rejections by `(code, reason_id, message)`

Counts per phase. No harness-bug rejection class appeared in any run.

| fe | repl | phase | `20` `backend-rpc-unavailable` "current chain state is unavailable" | `20` `backend-rpc-unavailable` "current payout state is unavailable" | `20` `ledger-confirmation-failed` "share was not confirmed by the database" | `21` `stale-job` "stale job" |
|---|---|---|---|---|---|---|
| 1 | async | steady | 16,149 (11,592–17,659) | 0 (0–0) | 0 (0–0) | 2,000 (2,000–2,001) |
| 1 | async | burst | 19,205 (15,111–20,144) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 1 | async | reconn | 3,978 (2,329–4,466) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 1 | async | slowdb | 15,204 (14,828–15,935) | 1,600 (568–2,520) | 2,293 (1,381–2,683) | 0 (0–0) |
| 2 | async | steady | 22,352 (14,808–23,340) | 0 (0–0) | 0 (0–0) | 2,000 (2,000–2,000) |
| 2 | async | burst | 31,782 (22,773–41,533) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 2 | async | reconn | 4,697 (2,646–5,385) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 2 | async | slowdb | 21,611 (21,299–21,754) | 7 (5–15) | 2,112 (1,993–2,877) | 0 (0–0) |
| 4 | async | steady | 31,878 (29,267–34,426) | 0 (0–0) | 0 (0–0) | 2,124 (2,000–2,232) |
| 4 | async | burst | 61,910 (43,422–81,547) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 4 | async | reconn | 8,739 (7,499–9,506) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 4 | async | slowdb | 21,011 (20,800–21,208) | 74 (54–102) | 3,312 (2,842–3,334) | 0 (0–0) |
| 1 | sync | steady | 24,826 (22,131–24,983) | 0 (0–0) | 0 (0–0) | 2,000 (2,000–2,691) |
| 1 | sync | burst | 28,834 (19,416–37,990) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 1 | sync | reconn | 4,291 (2,372–7,072) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 1 | sync | slowdb | 15,203 (15,041–15,463) | 1,806 (235–2,403) | 1,694 (1,493–2,080) | 0 (0–0) |
| 2 | sync | steady | 28,305 (28,229–29,483) | 0 (0–0) | 0 (0–0) | 2,002 (2,001–2,075) |
| 2 | sync | burst | 35,091 (32,452–39,242) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 2 | sync | reconn | 5,614 (5,268–7,224) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 2 | sync | slowdb | 22,867 (20,119–23,805) | 14 (11–153) | 2,731 (958–2,865) | 0 (0–0) |
| 4 | sync | steady | 37,944 (32,140–39,353) | 0 (0–0) | 0 (0–0) | 2,335 (2,000–2,405) |
| 4 | sync | burst | 60,969 (39,077–63,358) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 4 | sync | reconn | 9,942 (7,667–12,179) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 4 | sync | slowdb | 22,063 (21,733–23,314) | 98 (10–257) | 2,644 (2,387–2,762) | 0 (0–0) |

### ORDER_LOCK, SETTLEMENT_LOCK and `pg_stat_statements` lock time

| fe | repl | steady ORDER_LOCK max / mean waiters · waiter-s ≥ | steady SETTLEMENT_LOCK max / mean · waiter-s ≥ | steady lock stmt calls / total exec ms | burst ORDER_LOCK max / mean · waiter-s ≥ | burst SETTLEMENT_LOCK max / mean · waiter-s ≥ | burst lock stmt calls / total exec ms | reconn ORDER_LOCK max / mean · waiter-s ≥ | reconn SETTLEMENT_LOCK max / mean · waiter-s ≥ | reconn lock stmt calls / total exec ms | slowdb ORDER_LOCK max / mean · waiter-s ≥ | slowdb SETTLEMENT_LOCK max / mean · waiter-s ≥ | slowdb lock stmt calls / total exec ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | async | 15 (15–15) / 13.05 (12.9–13.12) · 3,912 (3,864–3,932) | 15 (15–15) / 0.93 (0.92–1.09) · 279 (276–328) | 95,540 (91,994–104,284) / 4,208,315 (4,207,615–4,224,209) | 15 (15–15) / 12.56 (12.54–12.82) · 752 (752–768) | 15 (15–15) / 1.25 (1.13–1.33) · 74.8 (67.4–79.8) | 18,208 (17,828–19,945) / 835,980 (831,176–839,664) | 15 (15–15) / 12.71 (12.67–14.29) · 762 (759–857) | 15 (0–15) / 1.13 (0–1.16) · 67.6 (0–68.5) | 19,357 (18,522–20,076) / 835,521 (831,370–861,671) | 15 (15–15) / 9.41 (7.44–10.89) · 564 (447–650) | 0 (0–7) / 0 (0–0.06) · 0 (0–3.4) | 1,429 (954–2,500) / 563,953 (446,703–658,015) |
| 2 | async | 31 (31–31) / 28.19 (27.68–28.34) · 8,449 (8,292–8,498) | 31 (31–31) / 1.46 (1.43–2.01) · 440 (430–606) | 99,342 (97,721–102,244) / 8,922,586 (8,918,861–8,957,885) | 31 (31–31) / 27.04 (26.05–27.61) · 1,620 (1,558–1,655) | 31 (15–31) / 2.35 (1.59–3.17) · 142 (95–192) | 18,370 (16,097–20,437) / 1,762,222 (1,762,028–1,771,431) | 31 (31–31) / 27.41 (24.33–28.1) · 1,642 (1,448–1,683) | 16 (16–31) / 1.49 (1.32–4.28) · 89 (79–264) | 18,912 (18,836–22,436) / 1,747,394 (1,726,697–1,776,260) | 31 (31–31) / 17.86 (17.54–18.26) · 1,075 (1,053–1,097) | 1 (1–1) / 0.08 (0.06–0.08) · 4.9 (3.3–5) | 1,874 (1,665–2,421) / 1,044,616 (1,022,458–1,071,911) |
| 4 | async | 63 (63–63) / 58.98 (56.43–59.22) · 17,681 (16,914–17,754) | 32 (32–63) / 1.92 (1.87–4.11) · 579 (567–1,234) | 94,969 (88,053–101,606) / 18,325,162 (18,207,141–18,382,415) | 63 (63–63) / 57.88 (57.08–59.11) · 3,470 (3,422–3,542) | 32 (16–39) / 2.42 (1.66–2.86) · 145 (101–171) | 17,099 (16,634–18,701) / 3,655,179 (3,620,239–3,675,014) | 63 (63–63) / 59.09 (58.04–59.84) · 3,541 (3,482–3,587) | 17 (16–32) / 1.3 (0.88–2.01) · 78 (52–120) | 18,340 (17,262–20,745) / 3,660,660 (3,641,823–3,673,196) | 63 (63–63) / 43.63 (40.56–47.39) · 2,622 (2,438–2,846) | 3 (3–16) / 0.85 (0.56–1.57) · 50.9 (33.7–94.1) | 1,611 (1,130–1,689) / 1,841,510 (1,699,275–2,001,131) |
| 1 | sync | 15 (15–15) / 12.77 (12.75–12.79) · 3,827 (3,821–3,833) | 15 (15–15) / 1.19 (1.19–1.2) · 358 (357–359) | 79,490 (79,355–80,731) / 4,234,493 (4,228,749–4,241,389) | 15 (15–15) / 12.38 (12.23–13.71) · 742 (731–822) | 15 (15–15) / 1.49 (0.17–1.59) · 88.5 (9.8–96.4) | 14,922 (13,900–15,261) / 841,107 (837,476–842,681) | 15 (15–15) / 14.27 (12.95–14.27) · 856 (776–856) | 1 (0–15) / 0 (0–1.33) · 0 (0–80) | 16,279 (15,722–16,686) / 867,636 (867,267–868,674) | 15 (15–15) / 9.62 (4.09–10.74) · 577 (247–640) | 3 (0–6) / 0.01 (0–0.04) · 0.8 (0–2.5) | 1,534 (1,014–1,548) / 580,148 (247,282–645,757) |
| 2 | sync | 31 (31–31) / 27.39 (27.3–27.64) · 8,214 (8,179–8,286) | 31 (31–31) / 2.28 (2.05–2.28) · 684 (618–689) | 83,968 (82,651–85,085) / 8,954,420 (8,926,213–8,961,739) | 31 (31–31) / 25.71 (25.62–26.65) · 1,538 (1,534–1,597) | 31 (31–31) / 3.3 (2.68–3.56) · 198 (161–213) | 15,719 (14,614–15,741) / 1,760,972 (1,752,960–1,773,282) | 31 (31–31) / 28.63 (27.66–28.74) · 1,715 (1,658–1,722) | 16 (15–17) / 0.84 (0.83–1.5) · 50.1 (49.4–89.2) | 17,483 (17,071–17,677) / 1,784,937 (1,766,441–1,787,743) | 31 (31–31) / 17.87 (9.84–18.69) · 1,074 (592–1,122) | 6 (1–16) / 0.26 (0.1–3.7) · 15 (6–226) | 2,004 (1,204–2,036) / 1,047,327 (780,684–1,115,924) |
| 4 | sync | 63 (63–63) / 56.82 (56.4–56.83) · 17,039 (16,914–17,042) | 63 (48–63) / 4.02 (3.53–4.29) · 1,207 (1,059–1,286) | 84,492 (83,895–87,578) / 18,293,714 (18,183,425–18,345,445) | 63 (63–63) / 53.34 (51.87–54.69) · 3,191 (3,105–3,277) | 63 (32–63) / 7.12 (4.83–7.37) · 432 (290–444) | 15,340 (15,148–16,101) / 3,604,921 (3,575,778–3,665,108) | 63 (63–63) / 58.91 (53.91–59.85) · 3,532 (3,230–3,588) | 18 (17–48) / 1.45 (1.02–5.08) · 87 (61–305) | 15,893 (15,737–18,598) / 3,646,332 (3,568,878–3,686,966) | 63 (63–63) / 41.48 (36.92–43.28) · 2,496 (2,228–2,604) | 10 (3–18) / 1.2 (0.76–1.79) · 72 (45–106) | 1,131 (1,050–2,035) / 2,014,481 (1,631,188–2,032,333) |

### Frontend CPU, memory and host load

| fe | repl | steady CPU cores (sum / max fe) | burst CPU cores (sum / max fe) | reconn CPU cores (sum / max fe) | slowdb CPU cores (sum / max fe) | RSS max MiB (max fe) | VmHWM MiB (max fe) | lowest MemAvailable MiB | load1 before run | load1 during mean / max |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | async | 0.37 (0.37–0.39) / 0.37 (0.37–0.39) | 0.42 (0.42–0.42) / 0.42 (0.42–0.42) | 0.38 (0.36–0.39) / 0.38 (0.36–0.39) | 0.12 (0.12–0.13) / 0.12 (0.12–0.13) | 694 (633–731) | 694 (633–731) | 17,789 (16,813–18,050) | 1.42 (1.23–1.73) | 3.37 (2.77–3.67) / 4.55 (4.53–4.75) |
| 2 | async | 0.42 (0.42–0.43) / 0.21 (0.21–0.22) | 0.49 (0.47–0.55) / 0.26 (0.24–0.29) | 0.45 (0.43–0.45) / 0.23 (0.22–0.23) | 0.19 (0.17–0.19) / 0.11 (0.09–0.11) | 655 (617–657) | 655 (617–708) | 16,273 (15,464–16,907) | 0.96 (0.96–1.67) | 3.87 (3–3.92) / 5.49 (4.24–5.97) |
| 4 | async | 0.48 (0.45–0.49) / 0.12 (0.12–0.13) | 0.65 (0.57–0.71) / 0.2 (0.16–0.2) | 0.48 (0.44–0.49) / 0.13 (0.12–0.13) | 0.2 (0.2–0.2) / 0.07 (0.07–0.08) | 689 (647–693) | 689 (647–693) | 13,683 (12,942–14,295) | 1.93 (0.81–2.39) | 3.65 (3.28–4.44) / 5.14 (4.8–6.31) |
| 1 | sync | 0.33 (0.32–0.33) / 0.33 (0.32–0.33) | 0.4 (0.38–0.43) / 0.4 (0.38–0.43) | 0.31 (0.31–0.32) / 0.31 (0.31–0.32) | 0.12 (0.12–0.15) / 0.12 (0.12–0.15) | 638 (550–704) | 660 (550–755) | 17,313 (16,536–17,728) | 1.98 (1.4–2.75) | 3.12 (2.9–3.52) / 4.42 (4.16–4.92) |
| 2 | sync | 0.37 (0.36–0.38) / 0.19 (0.18–0.19) | 0.46 (0.46–0.46) / 0.24 (0.23–0.24) | 0.38 (0.37–0.4) / 0.2 (0.19–0.21) | 0.19 (0.18–0.2) / 0.11 (0.1–0.12) | 662 (639–713) | 662 (639–713) | 16,310 (16,188–16,668) | 1.68 (1.35–2.6) | 3.47 (3.34–3.75) / 5.14 (4.73–5.29) |
| 4 | sync | 0.42 (0.42–0.43) / 0.11 (0.11–0.12) | 0.6 (0.53–0.61) / 0.16 (0.15–0.17) | 0.44 (0.41–0.45) / 0.12 (0.11–0.12) | 0.2 (0.18–0.21) / 0.06 (0.06–0.07) | 628 (624–657) | 628 (624–657) | 13,721 (12,982–14,399) | 1.58 (1.05–1.7) | 3.49 (2.93–3.53) / 4.66 (3.83–4.84) |

### Reconnects, time to usable work and outcomes

The fake node mints three external tips 6 s apart, at heights 101, 102 and
103, early in each run. The time-to-usable-work cell counts only notifies that
arrive while their tip is still the tip.

- **Tips 101 and 102.** No session received usable work before the tip was
  replaced, in any run: 0 of 2,000 sessions, so no latency exists to report.
- **Tip 103, which is never replaced.** The cell gives the time until all
  2,000 sessions had work.

| fe | repl | reconnect phase: completed / failed · reconnect p50 / max ms | tip 103: all 2,000 sessions with work, ms | ACK/commit divergences (#324) | unknown-outcome commits | durability findings | teardown tail (window-ended no-response commits) | harness-bug rejections | exit codes |
|---|---|---|---|---|---|---|---|---|---|
| 1 | async | 13 (13–13) / 0 (0–0) · 12,642 (12,318–14,439) / 20,815 (20,422–20,917) | 42,384 (35,235–48,100) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 4, 4 | 0, 0, 0 | 0, 0, 0 |
| 2 | async | 1,012 (1,012–1,013) / 772 (383–819) · 4,322 (3,954–6,626) / 16,145 (13,629–16,639) | 45,562 (39,632–45,952) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 1, 7 | 0, 0, 0 | 0, 0, 0 |
| 4 | async | 513 (512–513) / 351 (233–461) · 3,657 (3,447–3,767) / 20,979 (16,213–25,233) | 59,908 (40,505–61,786) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 17, 12, 11 | 0, 0, 0 | 0, 0, 0 |
| 1 | sync | 13 (13–13) / 0 (0–0) · 16,115 (15,430–16,650) / 25,777 (24,380–26,068) | 53,878 (49,593–65,362) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 5, 4 | 0, 0, 0 | 0, 0, 0 |
| 2 | sync | 1,012 (1,011–1,013) / 593 (275–760) · 4,231 (4,160–4,561) / 22,028 (16,802–25,100) | 55,188 (48,676–57,626) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 8, 3 | 0, 0, 0 | 0, 0, 0 |
| 4 | sync | 513 (512–513) / 424 (307–498) · 3,763 (3,703–3,923) / 22,711 (19,051–28,382) | 65,256 (49,357–66,652) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 |

Every `reconnect` phase lasted 60.0–60.001 s. At 2 and 4 frontends each
includes one drained frontend restart; at 1 frontend the phase recorded no
restart. So a restart still running at the phase deadline did not inflate the
rate denominator on this host.

### Per run

| run id | harness run tag | started (UTC) | exit | steady achieved /s · p99 ms | burst achieved /s · p99 ms | reconn achieved /s · p99 ms | slowdb achieved /s · p99 ms | lowest MemAvailable MiB | load1 before | load1 during mean |
|---|---|---|---|---|---|---|---|---|---|---|
| w11-20k-fe1-async-r1 | 99cc308d | 2026-09-15T13:38:22 | 0 | 320.4 · 8497.0 | 299.9 · 10217.0 | 301.4 · 9234.5 | 2.3 · 30485.4 | 18050 | 1.42 | 2.77 |
| w11-20k-fe2-async-r1 | dcd15dc1 | 2026-09-15T13:48:16 | 0 | 313.4 · 8462.3 | 307.5 · 9479.4 | 338.6 · 6418.7 | 2.6 · 29988.6 | 16907 | 0.96 | 3.0 |
| w11-20k-fe4-async-r1 | 62c943a5 | 2026-09-15T13:58:39 | 0 | 307.4 · 7561.6 | 294.8 · 8970.8 | 302.5 · 7076.7 | 5.9 · 28203.7 | 13683 | 0.81 | 3.28 |
| w11-20k-fe1-sync-r1 | c090b02a | 2026-09-15T14:09:05 | 0 | 237.5 · 11840.0 | 227.2 · 13537.9 | 249.1 · 8207.0 | 1.7 · 19019.2 | 16536 | 2.75 | 3.52 |
| w11-20k-fe2-sync-r1 | aad3e400 | 2026-09-15T14:19:28 | 0 | 250.7 · 10767.7 | 238.2 · 12998.8 | 260.7 · 7833.4 | 3.9 · 34143.0 | 16188 | 2.6 | 3.47 |
| w11-20k-fe4-sync-r1 | cdce683d | 2026-09-15T14:29:54 | 0 | 247.8 · 10789.7 | 253.3 · 10447.8 | 239.1 · 9042.4 | 5.1 · 29147.5 | 12982 | 1.05 | 3.49 |
| w11-20k-fe1-async-r2 | bb0a5d65 | 2026-09-15T14:40:20 | 0 | 290.0 · 8638.3 | 277.7 · 11000.5 | 289.1 · 9835.7 | 4.1 · 30306.0 | 16813 | 1.23 | 3.37 |
| w11-20k-fe2-async-r2 | 04d10be0 | 2026-09-15T14:50:41 | 0 | 303.7 · 9122.7 | 232.4 · 12920.0 | 264.5 · 10832.5 | 2.8 · 33613.2 | 15464 | 0.96 | 3.87 |
| w11-20k-fe4-async-r2 | 897e8d7a | 2026-09-15T15:01:06 | 0 | 265.6 · 9908.9 | 243.8 · 10878.3 | 253.3 · 9154.9 | 6.2 · 34084.7 | 12942 | 1.93 | 4.44 |
| w11-20k-fe1-sync-r2 | 6416bac2 | 2026-09-15T15:11:37 | 0 | 237.8 · 11523.8 | 242.6 · 14494.5 | 239.1 · 8151.7 | 4.2 · 30396.8 | 17313 | 1.98 | 2.9 |
| w11-20k-fe2-sync-r2 | 8680e498 | 2026-09-15T15:21:59 | 0 | 256.3 · 10305.7 | 224.8 · 11215.6 | 261.8 · 8491.8 | 4.5 · 30220.6 | 16310 | 1.35 | 3.34 |
| w11-20k-fe4-sync-r2 | 44935228 | 2026-09-15T15:32:22 | 0 | 253.7 · 10926.3 | 218.4 · 13115.1 | 245.3 · 9305.7 | 5.3 · 29358.6 | 14399 | 1.7 | 3.53 |
| w11-20k-fe1-async-r3 | f7a684b4 | 2026-09-15T15:42:50 | 0 | 278.5 · 10794.0 | 274.0 · 12055.6 | 302.7 · 7498.1 | 3.9 · 30363.6 | 17789 | 1.73 | 3.67 |
| w11-20k-fe2-async-r3 | b6779a8b | 2026-09-15T15:53:15 | 0 | 299.2 · 7558.8 | 267.2 · 9767.3 | 282.7 · 7581.3 | 3.9 · 32323.3 | 16273 | 1.67 | 3.92 |
| w11-20k-fe4-async-r3 | 9808f2fe | 2026-09-15T16:03:40 | 0 | 288.3 · 7929.6 | 251.0 · 9450.4 | 270.8 · 7229.5 | 5.8 · 29013.5 | 14295 | 2.39 | 3.65 |
| w11-20k-fe1-sync-r3 | ddcb51f9 | 2026-09-15T16:14:08 | 0 | 235.0 · 11674.8 | 230.6 · 13237.0 | 261.6 · 8286.1 | 4.0 · 30334.5 | 17728 | 1.4 | 3.12 |
| w11-20k-fe2-sync-r3 | 5a000353 | 2026-09-15T16:24:39 | 0 | 248.2 · 11477.3 | 210.4 · 13980.1 | 256.3 · 8113.7 | 3.8 · 34234.4 | 16668 | 1.68 | 3.75 |
| w11-20k-fe4-sync-r3 | 0a8bc48a | 2026-09-15T16:35:00 | 0 | 263.7 · 10270.5 | 224.2 · 13996.8 | 272.5 · 10434.0 | 4.8 · 28942.1 | 13721 | 1.58 | 2.93 |

### Calibration probes

These probes use 2 frontends, async replication, 2,000 sessions, the 20k
window, and the short plan: `warm_up` 30 s, `steady_state` 60 s, `reconnect`
60 s and `slow_database` 60 s. Both exited 0. They find the rate this host
sustains without shortfall. That rate is the one the dense short run uses.

| run id | exit | steady achieved/target /s · ACK p50 / p99 ms | reconn achieved/target /s · ACK p99 ms | slowdb achieved/target /s · ACK p99 ms | teardown tail | lowest MemAvailable MiB | load1 before |
|---|---|---|---|---|---|---|---|
| w11-cal-fe2-async-short-r200 | 0 | 200 / 200 · 4.1 / 5,485 | 200 / 200 · 2,141 | 4.05 / 200 · 29,625 | 0 | 17,319 | 1.11 |
| w11-cal-fe2-async-short-r100 | 0 | 100 / 100 · 4.4 / 4,859 | 100 / 100 · 3,427 | 3.92 / 100 · 28,664 | 3 | 16,997 | 0.97 |

At 200 shares/s the ACK median is about 4 ms, but p99 is still about 5 s.
Rate is therefore not the only source of the multi-second tail.

## 100k window: D1 matrix (two repeats)

**Every cell in this section is two runs, or one.** Two repeats cannot tell an
outlier from the configuration's real behaviour. The medians below are the
mean of two runs, and their ranges are only those two runs. The block runs 1
and 4 frontends only; the frontend dimension is settled at 20k.

### 1 frontend

All four runs exited 0.

| fe | repl | n | steady achieved/offered (target) /s | steady ACK p50 / p99 / max ms | burst achieved/offered (target) /s | burst ACK p50 / p99 / max ms | reconn achieved/offered (target) /s | reconn ACK p50 / p99 / max ms | slowdb achieved/offered (target) /s | slowdb ACK p50 / p99 / max ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | async | 2 | 285 (278–293) / 366 (360–373) (500) | 5,946 (5,792–6,101) / 8,560 (8,416–8,704) / 16,464 (15,732–17,195) | 331 (325–336) / 428 (410–446) (2000) | 6,076 (6,045–6,108) / 8,820 (6,729–10,912) / 12,694 (7,492–17,896) | 265 (258–272) / 405 (398–412) (500) | 5,895 (5,724–6,066) / 10,012 (9,563–10,460) / 16,021 (15,090–16,952) | 2.15 (2.1–2.2) / 408 (408–408) (500) | 11,772 (11,529–12,016) / 30,404 (30,362–30,446) / 30,475 (30,462–30,489) |
| 1 | sync | 2 | 238 (233–243) / 354 (351–357) (500) | 6,904 (6,796–7,012) / 11,546 (11,398–11,695) / 21,175 (20,712–21,639) | 252 (240–265) / 608 (504–712) (2000) | 7,364 (7,186–7,542) / 8,367 (8,275–8,458) / 12,585 (8,576–16,595) | 222 (220–224) / 373 (362–384) (500) | 7,030 (6,758–7,303) / 11,936 (10,842–13,030) / 16,519 (14,978–18,059) | 2.2 (2.2–2.2) / 386 (368–405) (500) | 12,048 (12,001–12,094) / 30,203 (30,015–30,391) / 30,259 (30,016–30,503) |

| fe | repl | steady refusal p50 / p99 ms (refusals) | burst refusal p50 / p99 ms (refusals) | reconn refusal p50 / p99 ms (refusals) | slowdb refusal p50 / p99 ms (refusals) |
|---|---|---|---|---|---|
| 1 | async | 0.5 (0.5–0.5) / 9,520 (9,429–9,610) (24,347 (24,075–24,619)) | 0.45 (0.4–0.5) / 428 (414–441) (5,830 (4,430–7,231)) | 0.5 (0.5–0.5) / 500 (500–500) (8,420 (7,564–9,275)) | 0.5 (0.5–0.5) / 29,042 (28,996–29,088) (24,352 (24,319–24,385)) |
| 1 | sync | 0.5 (0.5–0.5) / 10,639 (10,457–10,821) (34,606 (33,985–35,227)) | 0.5 (0.5–0.5) / 172 (2–342) (21,349 (14,369–28,329)) | 0.45 (0.4–0.5) / 431 (361–501) (9,050 (8,535–9,565)) | 0.5 (0.5–0.5) / 29,355 (29,166–29,544) (23,040 (21,919–24,160)) |

| fe | repl | phase | `20` `backend-rpc-unavailable` "current chain state is unavailable" | `20` `backend-rpc-unavailable` "current payout state is unavailable" | `20` `ledger-confirmation-failed` "share was not confirmed by the database" | `21` `stale-job` "stale job" |
|---|---|---|---|---|---|---|
| 1 | async | steady | 22,090 (21,830–22,351) | 0 (0–0) | 0 (0–0) | 2,256 (2,245–2,268) |
| 1 | async | burst | 5,830 (4,430–7,231) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 1 | async | reconn | 8,420 (7,564–9,275) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 1 | async | slowdb | 22,402 (22,371–22,433) | 186 (123–249) | 1,764 (1,703–1,825) | 0 (0–0) |
| 1 | sync | steady | 31,860 (30,814–32,907) | 0 (0–0) | 462 (0–925) | 2,283 (2,246–2,320) |
| 1 | sync | burst | 21,349 (14,369–28,329) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 1 | sync | reconn | 9,050 (8,535–9,565) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 1 | sync | slowdb | 20,891 (19,570–22,212) | 394 (307–482) | 1,754 (1,641–1,867) | 0 (0–0) |

| fe | repl | steady ORDER_LOCK max / mean waiters · waiter-s ≥ | steady SETTLEMENT_LOCK max / mean · waiter-s ≥ | steady lock stmt calls / total exec ms | burst ORDER_LOCK max / mean · waiter-s ≥ | burst lock stmt calls / total exec ms | reconn ORDER_LOCK max / mean · waiter-s ≥ | reconn lock stmt calls / total exec ms | slowdb ORDER_LOCK max / mean · waiter-s ≥ | slowdb lock stmt calls / total exec ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | async | 15 (15–15) / 12.51 (12.43–12.59) · 3,743 (3,720–3,767) | 15 (15–15) / 0.82 (0.74–0.9) · 245 (220–269) | 93,876 (91,229–96,523) / 4,002,732 (4,001,888–4,003,575) | 15 (15–15) / 13.665 (13.04–14.29) · 819 (782–857) | 19,658 (19,657–19,658) / 860,499 (859,203–861,795) | 15 (15–15) / 11.49 (11.32–11.66) · 686 (676–696) | 17,896 (17,456–18,335) / 765,458 (757,805–773,112) | 15 (15–15) / 6.08 (6.02–6.14) · 366 (363–370) | 1,606 (1,495–1,717) / 365,958 (362,107–369,810) |
| 1 | sync | 15 (15–15) / 12.13 (11.97–12.29) · 3,629 (3,583–3,676) | 15 (15–15) / 1.11 (1.01–1.21) · 333 (304–361) | 79,740 (78,116–81,365) / 4,009,606 (3,992,401–4,026,810) | 15 (15–15) / 13.9 (13.52–14.28) · 834 (811–857) | 16,097 (15,644–16,550) / 866,260 (865,305–867,216) | 15 (15–15) / 11.045 (10.94–11.15) · 661 (655–666) | 14,698 (13,961–15,435) / 757,729 (756,733–758,725) | 15 (15–15) / 7.005 (6.52–7.49) · 422 (393–451) | 1,584 (1,526–1,642) / 421,802 (392,756–450,848) |

| fe | repl | steady CPU cores | burst CPU cores | reconn CPU cores | slowdb CPU cores | RSS max MiB | VmHWM MiB | lowest MemAvailable MiB | load1 before run | load1 during mean / max |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | async | 0.43 (0.43–0.43) | 0.39 (0.39–0.39) | 0.435 (0.43–0.44) | 0.2 (0.2–0.2) | 2,741 (2,520–2,962) | 2,794 (2,619–2,969) | 13,240 (12,566–13,913) | 1.16 (0.9–1.42) | 3.14 (2.58–3.7) / 4.11 (3.03–5.19) |
| 1 | sync | 0.385 (0.38–0.39) | 0.385 (0.36–0.41) | 0.39 (0.39–0.39) | 0.21 (0.2–0.22) | 2,640 (2,506–2,775) | 2,770 (2,637–2,903) | 13,253 (12,636–13,870) | 1.35 (1.24–1.46) | 2.705 (2.62–2.79) / 3.63 (3.36–3.9) |

| fe | repl | reconnect phase: completed / failed · reconnect p50 / max ms | tip 103: all 2,000 sessions with work, ms | ACK/commit divergences (#324) | unknown-outcome commits | durability findings | teardown tail | harness-bug rejections | exit codes |
|---|---|---|---|---|---|---|---|---|---|
| 1 | async | 13 (13–13) / 0 (0–0) · 12,340 (11,440–13,239) / 21,815 (14,972–28,658) | 57,227; 60,727 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 0, 0 |
| 1 | sync | 13 (13–13) / 0 (0–0) · 11,766 (11,575–11,956) / 18,643 (18,068–19,219) | 71,326; 66,854 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 0, 0 |

As at 20k, no session had work at tips 101 or 102 before they were replaced.

| run id | harness run tag | started (UTC) | exit | steady achieved /s · p99 ms | burst achieved /s · p99 ms | reconn achieved /s · p99 ms | slowdb achieved /s · p99 ms | lowest MemAvailable MiB | load1 before | load1 during mean |
|---|---|---|---|---|---|---|---|---|---|---|
| w11-100k-fe1-async-r1 | 7547cde1 | 2026-09-15T17:19:59 | 0 | 293.1 · 8416.5 | 336.0 · 6728.6 | 272.0 · 9563.3 | 2.2 · 30445.7 | 12566 | 0.9 | 3.7 |
| w11-100k-fe1-sync-r1 | 53fc52cd | 2026-09-15T17:34:19 | 0 | 243.3 · 11397.7 | 264.8 · 8274.8 | 224.3 · 10842.0 | 2.2 · 30015.0 | 12636 | 1.24 | 2.79 |
| w11-100k-fe1-async-r2 | 4e3210b4 | 2026-09-15T17:44:30 | 0 | 277.6 · 8704.1 | 325.1 · 10912.2 | 257.9 · 10459.8 | 2.1 · 30362.0 | 13913 | 1.42 | 2.58 |
| w11-100k-fe1-sync-r2 | 68a24d41 | 2026-09-15T17:54:41 | 0 | 233.3 · 11694.6 | 240.1 · 8458.5 | 219.9 · 13029.8 | 2.2 · 30391.0 | 13870 | 1.46 | 2.62 |

### 4 frontends: did not fit on this host

`w11-100k-fe4-async-r1` (harness run tag `a9f991a8`) exited 6. The harness
stopped the run itself 64 s into `steady_state` with the message `MemAvailable
fell to 6092 MiB, below the 6144 MiB floor`, and withheld the artifact.

- **Memory.** The four frontends' VmHWM reached 2,390, 1,682, 1,687 and
  2,410 MiB, about 8.2 GiB together, and was still rising.
- **Host.** `MemAvailable` fell from 19,939 MiB just before the run to
  5,865 MiB at the last 10 s sample.
- **Load.** The 1-minute load average was 1.45 before the run.
- **Rates before the stop.** `warm_up` achieved 354 shares/s at ACK p99
  6,462 ms. The partial `steady_state` achieved 266 shares/s at ACK p99
  9,385 ms.

`w11-100k-fe4-sync-r1` exited 6 after 177 s, with no side report.

- **How it ended.** The coordinator's external memory guard sent it SIGTERM.
  That guard's log, kept on the coordinator's machine rather than this host,
  records the decision and the reading it acted on:

  ```text
  2026-09-15T18:22:05Z avail=6360MB total=23463MB pct=27 min_pct=26 running=4
  2026-09-15T18:22:07Z avail=5686MB total=23463MB pct=24 min_pct=24 running=4
  2026-09-15T18:22:07Z BELOW FLOOR: SIGTERM qbit-prism-load on alexdevbox2
  ```

  The harness logged `qbit-prism-load: terminated; tearing down` at 18:22:08.

- **This is weaker evidence than its async sibling, and the difference
  matters.** The async attempt stopped *itself* at the harness's own floor, so
  it demonstrates the harness declining to measure what it cannot measure. This
  run was stopped from outside, so it demonstrates only that free memory fell
  past an external threshold. The conclusion -- 100k at 4 frontends does not fit
  on this host -- rests on the async run.

- **Why the external guard won.** The harness floor is 6,144 MiB and the guard's
  is 25 % of 23,463 MiB, about 5,866 MiB: a gap of roughly 280 MiB. Memory fell
  from 11,658 MiB to 5,686 MiB in under a minute, and at that gradient 280 MiB
  is a few seconds. The two thresholds are too close together for this
  workload, and the harness should have been given more room to stop itself
  first. That is a fact about how these measurements were set up, not about
  PRISM.
- **Memory.** Before the run, `MemAvailable` was 19,871 MiB and the 1-minute
  load average 0.51. In the last minute, 10 s samples went from 11,658 MiB
  through 9,652, 8,819, 7,911 and 7,925 to 7,303 MiB; the signal arrived
  before the next sample.

No further 4-frontend repeat was attempted. A configuration that does not fit
needs no second demonstration.

## Dense cadence

**The dense-cadence block was dropped.** It is the input to #291's soak, not
a #271 gate, and it was not worth more host time. It was not retried at D1 or
at the calibrated sustained rate. The one attempt produced a finding: **at D1
on this host, the `dense_cadence` phase cannot start.** The harness refuses to
publish numbers under a delay the phase does not declare, and that refusal is
correct behaviour.

`w11-dense20k-fe1-async-r1` (D1 with `--cadence dense --scheduled-blocks 15`,
1 frontend, async, 20k window) exited 6. It completed every D1 phase, then the
harness aborted before the dense phase with this reason:

```text
1201 submit(s) offered before the dense_cadence phase were still outstanding 25.0 s later, so its 0 ms delay could not be applied without them finishing under it
```

The dense phase runs at 0 ms database delay and follows `slow_database`,
which runs at 10 ms. The harness changes the proxy delay only once every submit
offered under the previous delay has settled. Otherwise those submits would
finish under the new delay while keeping the old phase's stamp. If they have
not settled within the 25 s phase-boundary limit, the harness aborts the run.

`slow_database` left about 1,200 submits outstanding, and they did not drain
inside that limit. Its accepted-share ACK p99 on this host was 28–34 s in 17
of the 18 runs at 20k, and 19 s in the other.

The same condition appears at the end of the ordinary runs. In 15 of the 20
runs at 20k (including the calibration probes), submits were still
outstanding when the 25 s stop drain expired, up to 1,116 of them. The earlier
dense runs on `2e94136c` completed only because that harness did not yet have
this check.

Everything else in the aborted run was clean:

- the premise agreed;
- there was no divergence, no durability finding and no teardown tail;
- the D1 phases matched the 20k matrix (steady 319 shares/s at p99 8,256 ms).

The dense runs at 2 and 4 frontends, the repeats, and the short-plan run at
the calibrated 200 shares/s were not run.

## Blocked sizes

All three used 1 frontend, async, D1 and 2,000 sessions, with one attempt
each. Each exited 3 at startup: `load-fe-0 did not become ready within 120s
(listening=true)`. The only refusal kind the harness found in the frontend log
was the JSONB ceiling. The side report is the only output; there is no
artifact.

| Size | Run id | Exit | Refusal text (frontend log, verbatim) | Matches | Lowest MemAvailable MiB | Unblocked by |
|---|---|---|---|---|---|---|
| 200k | w11-200k-fe1-async-r1 | 3 | `template refresh deferred error=error returned from database: total size of jsonb object elements exceeds the maximum of 268435455 bytes` | 8 | 13,124 | #273 |
| 400k | w11-400k-fe1-async-r1 | 3 | `template refresh deferred error=error returned from database: total size of jsonb object elements exceeds the maximum of 268435455 bytes` | 3 | 9,517 | #273; then #265 for found-block candidates |
| 500k | w11-500k-fe1-async-r1 | 3 | `template refresh deferred error=error returned from database: total size of jsonb array elements exceeds the maximum of 268435455 bytes` | 1 | 6,575 | #273; #265 refuses candidates from 400k |

At 500k the refusal names `jsonb array elements`; at 200k and 400k it names
`jsonb object elements`. At 500k, seeding took the host to 6,575 MiB
`MemAvailable`, which is 431 MiB above the harness floor and about 700 MiB
above the external memory guard.

## The 400k result

**#271 stays open for this result.** (It has since closed, and #447 made the
re-run this section asks for. The run got past startup, and the #265 refusal
expected below did not occur; see
[Found blocks at 400k](#found-blocks-at-400k).)

- **Run.** `w11-400k-fe1-async-r1`: 1 frontend, async, D1, 2,000 sessions,
  `--window-shares 400000`. It exited 3 (blocked) after 156 s.
- **Startup.** The frontend never became ready: `load-fe-0 did not become
  ready within 120s (listening=true)`.
- **Refusal.** The frontend log shows the refusal three times, verbatim:

  ```text
  template refresh deferred error=error returned from database: total size of jsonb object elements exceeds the maximum of 268435455 bytes
  ```

- **Harness classification.** It recorded `blocked.blocked: true`, found only
  the `jsonb_ceiling` refusal kind, and wrote a startup-only side report and
  no artifact.
- **Memory.** The lowest `MemAvailable` sampled was 9,517 MiB. The load
  average was 0.98 before the run.

Two issues stand between this size and a measurement:

1. **#273, the JSONB ceiling.** Persisting the prepared job exceeds
   PostgreSQL's 268,435,455-byte JSONB container limit. The template refresh
   is deferred, so no frontend ever serves work.
2. **#265, found-block candidates at 400k.** The audit bundle written at
   landing is refused at this window. **That refusal was not observed here,
   and cannot be while #273 stands.** No work is served, so no share or
   candidate is produced and nothing reaches the landing path. This run is
   therefore evidence of the #273 block only; it is not yet evidence about
   #265.

Once #273 lands, the same command should be re-run. It is expected to get
past startup and reach the #265 refusal. If it does not, the plan changes.

## Capacity-evidence verdicts

The validator used forecast peak 2,000 shares/s and ACK p99 limit 1,000 ms.
Every completed run's artifact was refused with the same first error, `capacity
run did not acknowledge every offered valid share`, which is the expected
verdict. The next table shows why beyond that first error: the slowest artifact
phase is always `slow_database`, at 2–6 shares/s, with ACK p99 of 19–34 s.

| Window | fe | repl | runs | valid | slowest artifact phase /s (min–max) | worst artifact ACK p99 ms (min–max) | suggested forecast (min–max) | suggested ACK p99 limit ms |
|---|---|---|---|---|---|---|---|---|
| 20k | 1 | async | 3 | 0 | 2.3–4.1 | 30,306–30,485 | 1.14–2.03 | 15,000 |
| 20k | 2 | async | 3 | 0 | 2.6–3.9 | 29,989–33,613 | 1.32–1.93 | 15,000 |
| 20k | 4 | async | 3 | 0 | 5.8–6.2 | 28,204–34,085 | 2.90–3.11 | 15,000 |
| 20k | 1 | sync | 3 | 0 | 1.7–4.2 | 19,019–30,397 | 0.83–2.09 | 15,000 |
| 20k | 2 | sync | 3 | 0 | 3.8–4.5 | 30,221–34,234 | 1.90–2.25 | 15,000 |
| 20k | 4 | sync | 3 | 0 | 4.8–5.3 | 28,942–29,359 | 2.38–2.67 | 15,000 |
| 100k | 1 | async | 2 | 0 | 2.1–2.2 | 30,362–30,446 | 1.03–1.09 | 15,000 |
| 100k | 1 | sync | 2 | 0 | 2.2–2.2 | 30,015–30,391 | 1.08–1.09 | 15,000 |

Refusals make up 23–46 % of offered valid shares per run. They are mostly
`backend-rpc-unavailable` "current chain state is unavailable"; see the
rejection tables. The 1,000 ms ACK p99 limit is exceeded in every phase of
every run, including `steady_state`, where p99 is 7.6–11.8 s. The runs on the
aborted, blocked and did-not-fit paths withheld their artifacts, so they carry
no verdict.

## Comparison with the earlier harness bases

Earlier runs of the same matrix on this host are kept under their own bases.
They are evidence that the harness fixes changed what they were meant to
change. They are **not** a like-for-like comparison of the server, because the
server also changed between those bases:

- from `2e94136c` to this base, 64 files under `crates/qbit-prism-server/src`
  changed;
- from `840449b` to this base, 52 files changed.

| Measure (20k, D1) | `2e94136c` (18 runs) | `840449b` (5 runs, r1 only) | `5d0042f6` (18 runs) |
|---|---|---|---|
| Harness ACK latency | accepted and refused shares blended | accepted only (H3) | accepted only |
| Proxy delay changed only after the previous phase settled (H2) | no | yes | yes |
| slow_database ACK p50 ms, median per configuration | 0.4 in every configuration | 11,219–20,980 | 12,204–15,346 |
| burst ACK p50 ms, median per configuration | 0.5–5,118 | 5,695–6,946 | 6,422–7,910 |
| reconnect ACK p99 ms, median per configuration | 18,553–29,753 | 6,083–13,930 | 7,230–9,306 |
| slow_database achieved /s, median per configuration | 2.9–31.9 | 3.0–5.1 | 2.8–5.9 |
| steady_state achieved /s, median per configuration | 286–346 | 286–340 | 238–304 |
| runs exiting 5 | 12 of 18 | 0 of 5 | 0 of 18 |

What the table shows:

- **ACK p50 under refusals.** On `2e94136c` the harness counted refusal
  response times as acknowledgement latency.
  - Refusals return in about 0.5 ms and outnumber acknowledgements in
    `slow_database`, so that phase's ACK p50 read 0.4 ms in every
    configuration.
  - The burst ACK p50 read about 1 ms in five of six configurations.
  - Once the p50 counts acknowledgements only (H3), `slow_database` is 12–15 s
    and burst is 6–8 s, which is what the accepted shares actually waited.

  The change is the harness fix, not a change in the server's behaviour. The
  blended figures stay under `base-2e94136` for this comparison.
- **Reconnect and slow_database.** The high reconnect ACK p99 and the higher
  `slow_database` rates on `2e94136c` are consistent with submits from the
  previous phase finishing under the next phase's stamp. That is what H2
  prevents, and both measures moved to the levels seen on `840449b` and here.
- **Exit 5.** The 12 exit-5 runs on `2e94136c` counted ACK/commit divergences
  under the harness of that time. On this base a no-response commit at
  teardown is classified as a window-ended tail rather than a divergence. No
  completed run on this base recorded a divergence of either kind.
- **Steady-state rate.** It is lower on this base than on either earlier base,
  with sync lower by 40–60 shares/s. The load average during runs was also
  higher: a mean of 2.8–3.9 here against 1.6–3.0 on `840449b`. Because both
  the server and the host's background load differ, this document does not
  attribute the drop to either one.

The `840449b` set has one run for each of five configurations; its cells are
ranges over those single runs, not medians.

## Metric gaps (#278, #279)

The server's native registry now exports `qbit_prism_share_ack_seconds`,
`qbit_prism_rejections_total`, the advisory-lock-wait and pool-acquire
histograms, and runtime lag and RSS gauges. The harness still had to measure
the following from outside, or could not resolve them from the server's own
series:

1. **The cause of "current chain state is unavailable" is not identifiable.**
   Three different submit-admission checks return the same `(code 20,
   backend-rpc-unavailable, "current chain state is unavailable")`:
   - no chain poll has completed;
   - the last poll is older than the health timeout, without a share lease;
   - the readiness generation changed during admission.

   Neither the refusal nor `qbit_prism_rejections_total{reason_id}` says which
   check fired. At this load it is the largest refusal class, about 4k–62k
   per phase at 20k, so the capacity result's main refusal cannot be attributed
   from metrics. (#278, reject reasons.)
2. **The ACK histogram does not resolve the latencies measured here.** The
   `qbit_prism_share_ack_seconds` buckets above 1 s are 2.5, 5, 10 and 30 s.
   Accepted-share ACKs at this load sit between 5 and 10 s, and
   `slow_database` sits at about 30 s. Server-side p99 is therefore only known
   to lie between two buckets. The client-measured p99 in this document is the
   only precise figure. (#278, share-ACK latency.)
3. **There is no per-tip time-to-usable-work series.** The server exposes
   gauges such as `qbit_prism_stratum_oldest_pending_initial_job_seconds` and
   `qbit_prism_stratum_current_tip_coverage_gap_seconds`. It has no
   distribution of how long sessions waited for work on each tip. The harness
   measured that on the client: 35–71 s for all 2,000 sessions, and zero
   sessions served within a tip's 6 s life at tips 101 and 102. A gauge's
   instantaneous oldest value cannot reproduce those two facts. (#278, time
   to usable work.)
4. **Reconnect timing is client-side only.** Time to reconnect was measured
   from the client's close across failed attempts. No server series covers it.
   (#278.)
5. **Advisory-lock attribution needed `pg_locks` sampling.**
   `qbit_prism_database_advisory_lock_wait_seconds{lock}` gives each wait's
   duration by lock. The waiter queue depth (15, 31 and 63 here, equal to the
   pool size) came from 10 ms `pg_locks` sampling. `pg_stat_statements` cannot
   separate the locks because MIGRATION, ORDER and SETTLEMENT share one
   normalized statement text. (#278, database pool and lock waits.)
6. **Alert inventory (#279).** None of the conditions this matrix hit have an
   alert this document could cite: a sustained ACK p99 far above the 1,000 ms
   limit, a refusal ratio of 23–46 %, or `MemAvailable` falling to the floor
   at 100k with 4 frontends. Whether the deployed rules migrated under #279
   cover them should be checked against the #278 registry inventory.

## Reproducing a run

Build both binaries in release mode from the base commit. Then run the harness
with PostgreSQL 16 server binaries on the host. `<pg-bin>` is the directory
holding `initdb` and `postgres`, and `<out>` is any empty directory.

```sh
cargo build --locked --release -p qbit-prism-load -p qbit-prism-server
```

Every run uses the same base command. `<args>` is the run's argument list from
the table below.

```sh
target/release/qbit-prism-load \
  --server-bin target/release/qbit-prism-server \
  --pg-bin-dir <pg-bin> \
  <args> \
  --out <out>
```

| Run ids | `<args>` |
|---|---|
| `w11-premise-fe2-{async,sync}-short50` | `--frontends 2 --sessions 200 --window-shares 20000 --replication {async,sync} --plan short --rate 50 --forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000 --min-mem-available-mib 6144` |
| `w11-20k-fe{1,2,4}-{async,sync}-r{1,2,3}` | `--frontends {1,2,4} --sessions 2000 --window-shares 20000 --replication {async,sync} --plan d1 --forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000 --min-mem-available-mib 6144` |
| `w11-cal-fe2-async-short-r{200,100}` | `--frontends 2 --sessions 2000 --window-shares 20000 --replication async --plan short --rate {200,100} --forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000 --min-mem-available-mib 6144` |
| `w11-100k-fe1-{async,sync}-r{1,2}`, `w11-100k-fe4-{async,sync}-r1` | `--frontends {1,4} --sessions 2000 --window-shares 100000 --replication {async,sync} --plan d1 --forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000 --min-mem-available-mib 6144` |
| `w11-dense20k-fe1-async-r1` | `--frontends 1 --sessions 2000 --window-shares 20000 --replication async --plan d1 --cadence dense --scheduled-blocks 15 --forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000 --min-mem-available-mib 6144` |
| `w11-{200,400,500}k-fe1-async-r1` | `--frontends 1 --sessions 2000 --window-shares {200000,400000,500000} --replication async --plan d1 --forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000 --min-mem-available-mib 6144` |

Runs were driven one at a time, detached from any interactive session, with a
60 s cooldown and a whole-run ceiling of 2,400 s (1,500 s for the blocked
sizes). The harness exit code is the run's result:

| Exit | Meaning |
|---|---|
| 0 | completed |
| 3 | blocked |
| 4 | durability loss |
| 5 | ACK/commit divergence |
| 6 | aborted |
| 7 | harness-bug rejections |
| 8 | contradicted premise |

## What was not measured

This list is the #271 matrix's. #447 has since measured dense cadence at D1
and the three blocked sizes; its own list is
[What was not measured (#447)](#what-was-not-measured-447).

- **Dense cadence.** The block was dropped after its first run showed the
  dense phase cannot start at D1 on this host.
- **100k at 4 frontends.** The configuration does not fit on this host, so
  no repeat was attempted.
- **100k at 2 frontends, and a third 100k repeat.** Both were cut from the
  plan; the 20k block settles the frontend dimension.

## Production-window matrix on `a1937054` (#447)

This part records the runs #271 could not make: the 200k, 400k and 500k
windows, the frontend dimension at 400k, one synchronous comparison, found
blocks under load, and dense cadence. It is a different host and a different
build from everything above, so it has its own host table, method notes and
tables. **No table in this part contains a run from the #271 host, and no
table above contains a run from this one.**

### What the production-window matrix established

- **200k, 400k and 500k all run.** Each exited 0 at one frontend with #271's
  argument lists unchanged, so #273 holds; see
  [The blocked sizes now run](#the-blocked-sizes-now-run).
- **Every ordinary run reconciled exactly.** All 22 runs of the rate matrix
  exited 0, with no ACK/commit divergence, no unknown-outcome commit, no
  durability finding, no acknowledgement lost inside a run and no harness-bug
  rejection. One run has a teardown tail, as defined under [Method](#method):
  `d1-400k-fe4-async-r3` (7, in `slow_database`). Those submits were still
  outstanding when the 25 s stop drain expired, PostgreSQL holds them, and the
  harness reports them with `window_ended: true` and exits 0. The runs outside
  the medians are the two flush-control repeats that exited 5 and the 20k
  dense attempt that exited 6; see [Runs outside the
  medians](#runs-outside-the-medians).
- **The host is a laptop, and it is not the #291 rehearsal host,** which had
  not been named. Its commits do not force the drive cache, and that decides
  the next two results; see [What this host changes](#what-this-host-changes).
- **500 shares/s for five minutes: the rate was sustained in all 22 repeats,
  with a median ACK p99 of 13–33 ms.**
  - No token of the target went unplaced in any repeat, at any window, at 1,
    2 or 4 frontends, async or sync.
  - By the strict rule four of the eight configurations read "met". The
    other four each miss it on one to three shares of about 150,000, refused
    at the commit gate when a tip's work arrived; see
    [D1 verdict](#d1-verdict).
- **With a forced flush the same configuration sustains about 117 shares/s.**
  The [flush control](#the-flush-control) changes one PostgreSQL setting and
  nothing else. It misses 500 shares/s by a factor of four, in three repeats.
  The result above is therefore what this build does when a commit costs
  tens of microseconds, and it is not a prediction for the rehearsal host.
  What one WAL flush costs there decides it, and `pg_test_fsync` measures
  that in two minutes.
- **2,000 shares/s for one minute: not met by any configuration.**
  - The median `burst` rate is 1,468–1,623 shares/s.
  - **Adding frontends adds `ORDER_LOCK` waiters, not throughput:** 14.4, 30.3
    and 62.4 waiters on average at 1, 2 and 4 frontends, with all frontends
    together using at most 0.48 cores. This is #271's finding at the
    production window.
  - The window size does not move it: 1,558, 1,623 and 1,550 shares/s at
    200k, 400k and 500k. The build control below adds 1,621 at 20k.
- **#271's 238–304 shares/s was its host, and its build has the higher
  ceiling here.** The [build control](#the-build-control) runs #271's 20k
  argument list on this host on both builds, three interleaved repeats each.
  - Both sustain 500 shares/s, so the build is not what took #271's figure to
    500.
  - In `burst` #271's build acknowledged 1,979–1,994 shares/s with
    `ORDER_LOCK` unsaturated, and the current build 1,559–1,649 with it
    saturated: about a fifth lower. The current build is more than ten times
    better on ACK p99 at 500 shares/s, 17–32 ms against 418–446 ms.
  - "Build" is the harness and the server together; both cross runs were
    refused, so which of the two moved is not known.
- **Synchronous replication shows no cost here,** and that says little. At
  400k and 2 frontends, sync sustained 500 shares/s and its `burst` range
  overlaps its async pair's. The standby shares the primary's disk and
  neither forces a flush.
- **Found blocks land at 400k under load.** Three scheduled blocks were
  accepted, no refusal was logged, and `candidates list` showed no unfinished
  row afterwards. The #265 refusal #271 expected did not occur; see
  [Found blocks at 400k](#found-blocks-at-400k).
- **Dense cadence ran at D1 at the production window,** in three runs of
  three. While own blocks landed 9 to 20 s apart the pool acknowledged
  399–415 of the 500 shares/s offered, and every session had work for the new
  tip about 3 s after each landing. 44 of 45 scheduled blocks landed; one,
  found 9 s after the previous landing, was refused as a stale job. The
  harness's own per-landing figures are empty on this build, because they key
  on a rejection message the server no longer sends; see
  [Dense cadence at D1](#dense-cadence-at-d1).
- **Time to usable work after a tip splits in two,** 3–8 s or 20–37 s
  depending on the configuration, and the cause was not investigated; see
  [Time to usable work after a tip](#time-to-usable-work-after-a-tip).
- **Memory is not a constraint on this build.** A frontend held at most
  1,869 MiB at a 500k window, at 4 frontends during `warm_up`, and about
  1,075 MiB at 1 frontend. The host never had less than 60,959 MiB
  reclaimable.
- **The harness under-reported frontend CPU about 42 times on Apple
  silicon.** It is fixed in the change that adds this part, and the CPU
  figures here are converted; see
  [Harness observations](#harness-observations-447).

### Host (#447)

Every table in this part comes from this one host. No table mixes it with
the #271 host above.

| Fact | Value |
|---|---|
| Host | Anatolies-MacBook-Pro, a developer's laptop in interactive use. **It is not the #291 rehearsal host**, which had not been named when these runs were made; see [What this host changes](#what-this-host-changes) |
| CPU | Apple M5 Max, 18 cores (6 "Super" and 12 "Performance"), arm64. The core types are asymmetric and macOS decides placement |
| RAM | 128 GiB (`hw.memsize` 137,438,953,472), no swap in use, no memory or CPU limit |
| Disk | Apple SSD AP2048Z, 2 TB, APFS; the managed clusters live on the same disk |
| OS | macOS 26.6.2 (25G83), Darwin 25.6.0 |
| PostgreSQL | 16.15 (Homebrew), managed by the harness: one primary and one standby per run |
| Durability | `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`, read back from the primary for every run. **`wal_sync_method=open_datasync`, the macOS default, which does not force the drive's write cache** |
| Build | release profile for both `qbit-prism-server` and `qbit-prism-load`, built from `a1937054ac50765c82222de5c7ac27edfd4925ac` with Rust 1.98.1 stable, in a clean detached worktree that nothing else touched |
| Frontend settings | harness defaults, unchanged from #271: `PRISM_RUNTIME_WORKERS=2`, `PRISM_DATABASE_MAX_CONNECTIONS=16`, `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS=15`, `PRISM_BLOCKPOLL_SECONDS=2` |
| Power | AC power, **except for about 35 minutes on battery** on 2026-09-18, from about 17:11 to between 17:34 and 17:48 UTC. `d1-400k-fe2-async-blocks3-r1` and `d1-dense20k-fe1-async-r1` ran wholly on battery; `d1-dense400k-fe1-async-r1` started on battery and ended on AC; `d1-400k-fe2-sync-r3` ended on battery, and the battery still read its held 80 % a minute after the run, which puts the switch in the run's last minutes, after `steady_state` and `burst` had ended. `pmset` reports `powermode 0` for both sources, so Low Power Mode was off. The power source and `pmset -g therm` were recorded before and after every D1 run (not the premise probe, which predates the recording), and no run recorded a thermal or performance warning |

**The host was shared.** It is a workstation. The window server, a browser, a
chat client, other coding-agent sessions and two unrelated PostgreSQL
instances ran throughout, outside this measurement's control. The driver
records the 1-minute load average and a memory sample just before each run
(after a 60 s cooldown) and every 10 s during it, and a process snapshot at
each end, as #271's driver did. Both are reported beside each run. Before a
run the 1-minute load average was 1.56–8.15 across the matrix, on 18
cores; #271's was 0.8–2.8 on 8.

### What this host changes

The difference from the #291 rehearsal host is not only size. These facts
about this host decide how far its numbers travel, and the first one decides
the verdict.

- **Commits do not force the drive cache here, and the 500 shares/s result
  depends on that.**
  - PostgreSQL on macOS defaults to `wal_sync_method=open_datasync`. On macOS
    neither `O_DSYNC` nor `fsync()` forces the drive's write cache; only
    `fsync_writethrough` (`F_FULLFSYNC`) does. The harness sets `fsync=on` and
    reads it back, but it does not set the sync method.
  - `pg_test_fsync` on this disk, with nothing else running: `open_datasync`
    28,992 ops/s (34 µs per 8 kB write) against `fsync_writethrough`
    284 ops/s (3,517 µs). That is a factor of about 100.
  - The share append commits while it holds `ORDER_LOCK`, so the cost of a
    commit is inside the serialised section.
  - [The flush control](#the-flush-control) measures the consequence: with a
    forced flush the same configuration sustains about 117 shares/s, not 500.
  - So a rate in this part is what this build does **when a commit costs tens
    of microseconds**. It is not a prediction for a host whose commits wait
    for a flush. What the rehearsal host's commits cost is the first thing to
    measure there, and `pg_test_fsync` measures it in two minutes.
- **The harness memory floor does not operate on macOS.**
  `--min-mem-available-mib` reads `MemAvailable` from `/proc/meminfo`. That
  file does not exist here, and the harness then skips the check without
  saying so (`run.rs`, the 1 s memory check). The flag was still passed,
  unchanged, so the argument lists match #271's. The driver carried a guard
  instead: every 10 s it summed free, inactive and speculative pages from
  `vm_stat`, and it would have sent SIGTERM below 16 GiB. It never fired. That
  sum is an approximation and is **not** `MemAvailable`; macOS publishes no
  equivalent, so it is not comparable with the Linux figures above.
- **Frontend CPU was under-reported about 42 times, and is converted here.**
  The harness read `proc_pid_rusage` CPU times as nanoseconds. On Apple
  silicon they are Mach ticks of 125/3 ns. A probe that burned 0.2500 s of
  CPU read 0.0060 s through the harness's arithmetic and 0.2500 s after
  scaling. The harness is fixed in the change that adds this part, with a
  test that fails against the old arithmetic. The runs recorded here were
  made with the unfixed build, so every CPU figure in this part is the
  reported value multiplied by 125/3, and the tables say so.
- **Peak RSS is a sampled maximum, not a kernel peak.** Linux's `VmHWM` has no
  macOS equivalent, and the harness labels the figure accordingly. A spike
  between two 1 s samples is not in it.
- **`pg_stat_statements` was not loaded,** so the lock-statement columns are
  `n/a`. The harness looks for `pg_stat_statements.so` and macOS ships a
  `.dylib`.
- **The side report's host block is mostly `null`** (`cpu_model`, `kernel`,
  `memory_total_kib`), because the harness reads those from `/proc`. The host
  table above was taken from `sysctl`, `system_profiler` and `sw_vers`.

Apart from the CPU units, none of these is wrong output: the harness targets
Linux, and on macOS it reports what it cannot measure as unknown rather than
as zero.

### Method (#447)

The method is #271's, with these differences.

- **Run ids** carry the prefix `d1-` instead of `w11-`. The argument lists of
  the three blocked sizes are unchanged from
  `w11-{200,400,500}k-fe1-async-r1`.
- **Cluster root.** The harness builds its cluster under `$TMPDIR`. With the
  macOS default, PostgreSQL refuses to start: `Unix-domain socket path ... is
  too long (maximum 103 bytes)`, and the harness exits 2. The driver
  therefore set `TMPDIR=/tmp/d1t`. Nothing else in the environment was
  changed.
- **Whole-run ceiling** 3,600 s. No run reached it.
- **Order and repeats.**
  - Runs were strictly one at a time, with a 60 s cooldown between them.
  - The three blocked sizes ran first, on 2026-09-17. They are also the first
    repeat of the 1-frontend async cells at 400k and 500k.
  - Everything else ran on 2026-09-18, interleaved as #271's was. At 400k one
    pass runs async at 1, 2 and 4 frontends and then sync at 2, and the next
    pass repeats that order; at 500k a pass is async at 1, 2 and 4.
  - **Three repeats per configuration** in the rate matrix and the flush
    control. 200k, the found-block run and the 20k dense attempt are one run
    each; dense cadence at 400k is two runs at 1 frontend and one at 2.
  - Synchronous replication was measured at 2 frontends, the topology #291
    describes.
- **D1 verdict.** D1 (#260) sets rates only, so "met" is defined from the
  harness's own accounting rather than from a rate comparison; the definition
  is printed above the [verdict tables](#d1-verdict).

### D1 verdict

D1 (#260) sets rates only: "2,000/s sustained for one minute, 500/s for five
minutes". It sets no acknowledgement fraction and no latency, so ACK p99 is
shown beside each verdict, against the limit the run's validator used, and is
**not** part of it. The two phases are judged separately.

**Definition.** A repeat **meets** a phase when all three hold for that phase:
`shortfall == 0` (the open-loop scheduler placed every token of the target
rate; a token is a shortfall when no session could take it because each was
still waiting for its previous answer, run.rs:2059-2068);
`rejected_valid_shares == 0` (the server refused no share the harness holds
valid, that is, no rejection outside the races the server is entitled to lose,
run.rs:2534-2548); and no submit of the phase went unanswered
(`rejections.no_response_by_phase`). A configuration is **met** only when
EVERY repeat in its medians meets it, and the count says how many did. A null
in any of the three gives no verdict, never a pass.

**Why not `achieved >= target`.** The scheduler generates `floor(elapsed *
rate)` tokens and stops at the deadline, so it places 149,999 tokens in a 300
s phase at 500/s: a phase that acknowledged every one reads 499.997 shares/s.
Achieved also excludes the entitled races, which are listed in their own
column. Rates that would round to their target are printed to three decimals
so none reads as the target.

#### The 500 shares/s phase (`steady_state`, 300 s)

| window | fe | repl | plan | n | target /s | offered /s | achieved /s | shortfall tokens | rejected valid shares, per run | unanswered submits, per run | entitled-race rejections, per run | verdict | ACK p99 ms | ACK p99 within the validator limit | ORDER_LOCK max / mean waiters | same-configuration runs outside the medians |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | 500 | 499.997 | 499.997 | 0 | 0 | 0 | 0 | **met** (1 of 1) | 20.5 | 1 of 1 (limit 1,000 ms) | 15 / 0.07 | – |
| 400k | 1 | async | d1 | 3 | 500 | 499.997 (499.995–499.997) | 499.4 (499.3–499.4) | 0 (0–0) | 0, 1, 0 | 0, 0, 0 | 193, 199, 171 | **not met** (2 of 3): valid shares refused in 1 of 3 repeats, at most 1 per run | 18.5 (8.6–25.1) | 3 of 3 (limit 1,000 ms) | 15 (15–15) / 0.08 (0.07–0.09) | – |
| 400k | 2 | async | d1 | 3 | 500 | 499.995 (499.995–499.997) | 499.3 (499.3–499.3) | 0 (0–0) | 1, 1, 1 | 0, 0, 0 | 201, 205, 203 | **not met** (0 of 3): valid shares refused in 3 of 3 repeats, at most 1 per run | 23.2 (11.4–37.5) | 3 of 3 (limit 1,000 ms) | 31 (31–31) / 0.16 (0.12–0.24) | – |
| 400k | 4 | async | d1 | 3 | 500 | 499.995 (499.995–499.997) | 499.995 (499.995–499.997) | 0 (0–0) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | **met** (3 of 3) | 15 (13.2–16.1) | 3 of 3 (limit 1,000 ms) | 63 (62–63) / 0.22 (0.21–0.23) | – |
| 400k | 2 | sync | d1 | 3 | 500 | 499.995 (499.995–499.997) | 499.995 (499.995–499.997) | 0 (0–0) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | **met** (3 of 3) | 33 (9–2,560) | 2 of 3 (limit 1,000 ms) | 31 (30–31) / 0.21 (0.15–1.39) | – |
| 400k | 2 | async | d1 +3 blocks | 1 | 500 | 499.995 | 482.5 | 0 | 12 | 0 | 5251 | **not met** (0 of 1): valid shares refused in 1 of 1 repeats, at most 12 per run | 51.1 | 1 of 1 (limit 1,000 ms) | 31 / 0.17 | – |
| 400k | 1 | async | d1 +dense(15) | 2 | 500 | 498.828 (497.66–499.997) | 498.2 (497–499.3) | 351 (0–701) | 9, 2 | 0, 0 | 187, 201 | **not met** (0 of 2): shortfall in 1 of 2 repeats, at most 701 per run; valid shares refused in 2 of 2 repeats, at most 9 per run | 1,963 (22–3,903) | 1 of 2 (limit 1,000 ms) | 15 (15–15) / 0.64 (0.1–1.17) | – |
| 400k | 2 | async | d1 +dense(15) | 1 | 500 | 499.995 | 499.3 | 0 | 1 | 0 | 196 | **not met** (0 of 1): valid shares refused in 1 of 1 repeats, at most 1 per run | 19.8 | 1 of 1 (limit 1,000 ms) | 31 / 0.17 | – |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | 500 | 250.1 | 117.3 | 74,981 | 38170 | 0 | 1671 | **not met** (0 of 1): shortfall in 1 of 1 repeats, at most 74,981 per run; valid shares refused in 1 of 1 repeats, at most 38,170 per run | 38,296 | 0 of 1 (limit 1,000 ms) | 15 / 14.11 | d1-400k-fe1-async-wt-r1 (exit 5), d1-400k-fe1-async-wt-r3 (exit 5) |
| 500k | 1 | async | d1 | 3 | 500 | 499.997 (499.995–499.997) | 499.4 (499.2–499.4) | 0 (0–0) | 0, 1, 1 | 0, 0, 0 | 193, 189, 247 | **not met** (1 of 3): valid shares refused in 2 of 3 repeats, at most 1 per run | 13.3 (13.2–26.7) | 3 of 3 (limit 1,000 ms) | 15 (15–15) / 0.08 (0.07–0.09) | – |
| 500k | 2 | async | d1 | 3 | 500 | 499.997 (499.995–499.997) | 499.3 (499.3–499.3) | 0 (0–0) | 2, 3, 3 | 0, 0, 0 | 198, 212, 203 | **not met** (0 of 3): valid shares refused in 3 of 3 repeats, at most 3 per run | 17.4 (8.1–28.1) | 3 of 3 (limit 1,000 ms) | 31 (31–31) / 0.15 (0.09–0.19) | – |
| 500k | 4 | async | d1 | 3 | 500 | 499.997 (499.995–499.997) | 499.997 (499.995–499.997) | 0 (0–0) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | **met** (3 of 3) | 13.3 (13–22.5) | 3 of 3 (limit 1,000 ms) | 63 (63–63) / 0.18 (0.16–0.26) | – |
| 20k | 1 | async | d1 | 0 | n/a | n/a | n/a | n/a | n/a | n/a | n/a | no verdict: no run in the medians | n/a | n/a | n/a | d1-dense20k-fe1-async-r1 (exit 6) |

#### The 2,000 shares/s phase (`burst`, 60 s)

| window | fe | repl | plan | n | target /s | offered /s | achieved /s | shortfall tokens | rejected valid shares, per run | unanswered submits, per run | entitled-race rejections, per run | verdict | ACK p99 ms | ACK p99 within the validator limit | ORDER_LOCK max / mean waiters | same-configuration runs outside the medians |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | 2000 | 1,558.5 | 1,558.5 | 26,490 | 0 | 0 | 0 | **not met** (0 of 1): shortfall in 1 of 1 repeats, at most 26,490 per run | 1,886 | 0 of 1 (limit 1,000 ms) | 15 / 14.41 | – |
| 400k | 1 | async | d1 | 3 | 2000 | 1,623 (1,503.1–1,755.6) | 1,623 (1,503.1–1,755.6) | 22,622 (14,663–29,816) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | **not met** (0 of 3): shortfall in 3 of 3 repeats, at most 29,816 per run | 4,026 (3,357–4,397) | 0 of 3 (limit 1,000 ms) | 15 (15–15) / 14.36 (14.36–14.38) | – |
| 400k | 2 | async | d1 | 3 | 2000 | 1,510.2 (1,468.2–1,521.7) | 1,490.2 (1,445.6–1,521.7) | 29,384 (28,698–31,908) | 1203, 1356, 0 | 0, 0, 0 | 0, 0, 0 | **not met** (0 of 3): shortfall in 3 of 3 repeats, at most 31,908 per run; valid shares refused in 2 of 3 repeats, at most 1,356 per run | 2,608 (2,464–3,099) | 0 of 3 (limit 1,000 ms) | 31 (31–31) / 30.33 (30.31–30.35) | – |
| 400k | 4 | async | d1 | 3 | 2000 | 1,504.2 (1,423–1,662.7) | 1,504.2 (1,423–1,662.7) | 29,746 (20,237–34,621) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | **not met** (0 of 3): shortfall in 3 of 3 repeats, at most 34,621 per run | 1,882 (1,634–1,972) | 0 of 3 (limit 1,000 ms) | 63 (63–63) / 62.38 (62.32–62.39) | – |
| 400k | 2 | sync | d1 | 3 | 2000 | 1,504.7 (1,402.8–1,616.5) | 1,504.7 (1,402.8–1,616.5) | 29,714 (23,008–35,830) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | **not met** (0 of 3): shortfall in 3 of 3 repeats, at most 35,830 per run | 1,812 (1,667–1,849) | 0 of 3 (limit 1,000 ms) | 31 (31–31) / 30.47 (30.44–30.47) | – |
| 400k | 2 | async | d1 +3 blocks | 1 | 2000 | 1,600 | 1,600 | 23,998 | 0 | 0 | 0 | **not met** (0 of 1): shortfall in 1 of 1 repeats, at most 23,998 per run | 1,688 | 0 of 1 (limit 1,000 ms) | 31 / 30.43 | – |
| 400k | 1 | async | d1 +dense(15) | 2 | 2000 | 1,491.5 (1,455.9–1,527) | 1,490.8 (1,454.6–1,527) | 30,510 (28,377–32,643) | 83, 0 | 0, 0 | 0, 0 | **not met** (0 of 2): shortfall in 2 of 2 repeats, at most 32,643 per run; valid shares refused in 1 of 2 repeats, at most 83 per run | 4,786 (4,378–5,194) | 0 of 2 (limit 1,000 ms) | 15 (15–15) / 14.35 (14.34–14.36) | – |
| 400k | 2 | async | d1 +dense(15) | 1 | 2000 | 1,492.7 | 1,466.6 | 30,439 | 1561 | 0 | 0 | **not met** (0 of 1): shortfall in 1 of 1 repeats, at most 30,439 per run; valid shares refused in 1 of 1 repeats, at most 1,561 per run | 2,255 | 0 of 1 (limit 1,000 ms) | 31 / 30.32 | – |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | 2000 | 762.2 | 107.2 | 74,266 | 39297 | 0 | 2 | **not met** (0 of 1): shortfall in 1 of 1 repeats, at most 74,266 per run; valid shares refused in 1 of 1 repeats, at most 39,297 per run | 39,017 | 0 of 1 (limit 1,000 ms) | 15 / 13.68 | d1-400k-fe1-async-wt-r1 (exit 5), d1-400k-fe1-async-wt-r3 (exit 5) |
| 500k | 1 | async | d1 | 3 | 2000 | 1,550 (1,481.4–1,561.4) | 1,550 (1,481.4–1,561.4) | 26,996 (26,313–31,114) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | **not met** (0 of 3): shortfall in 3 of 3 repeats, at most 31,114 per run | 4,744 (3,828–4,870) | 0 of 3 (limit 1,000 ms) | 15 (15–15) / 14.38 (14.37–14.38) | – |
| 500k | 2 | async | d1 | 3 | 2000 | 1,521.7 (1,521.6–1,547.8) | 1,504.3 (1,496.5–1,522.9) | 28,695 (27,130–28,700) | 1516, 1041, 1496 | 0, 0, 0 | 0, 0, 0 | **not met** (0 of 3): shortfall in 3 of 3 repeats, at most 28,700 per run; valid shares refused in 3 of 3 repeats, at most 1,516 per run | 2,289 (2,179–3,083) | 0 of 3 (limit 1,000 ms) | 31 (31–31) / 30.33 (30.32–30.33) | – |
| 500k | 4 | async | d1 | 3 | 2000 | 1,468 (1,401.4–1,514.5) | 1,468 (1,401.4–1,514.5) | 31,921 (29,131–35,917) | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | **not met** (0 of 3): shortfall in 3 of 3 repeats, at most 35,917 per run | 1,879 (1,854–1,978) | 0 of 3 (limit 1,000 ms) | 63 (63–63) / 62.35 (62.34–62.39) | – |
| 20k | 1 | async | d1 | 0 | n/a | n/a | n/a | n/a | n/a | n/a | n/a | no verdict: no run in the medians | n/a | n/a | n/a | d1-dense20k-fe1-async-r1 (exit 6) |

**Which rows are the rate matrix.** The eight rows whose plan column reads
`d1` alone, 22 repeats in all, are the rate matrix, and the reading below is
about them. The tables also hold the other D1-plan runs, each marked in its
plan column and discussed in its own section: the found-block run
(`+3 blocks`), dense cadence (`+dense(15)`) and the flush control
(`+wal_sync_method=fsync_writethrough`). Landing blocks and forcing a flush
both cost the rate phases something, and their rows say how much. The `20k`
row with no run in its medians is the dense attempt that aborted.

**Reading the 500 shares/s table.**

- **The rate was sustained in every repeat of every ordinary configuration.**
  `shortfall` is 0 in all 22 repeats: every token of the 500 shares/s target
  was placed, at 200k, 400k and 500k, at 1, 2 and 4 frontends, async and
  sync. No submit went unanswered in the phase. ACK p99 was 8–38 ms in 21 of
  the 22; one synchronous repeat reached 2,560 ms and is the only one above
  the 1,000 ms limit.
- **The configurations that read "not met" miss it on one to three shares.**
  The strict rule needs every valid share acknowledged. Nine repeats, all at
  1 or 2 frontends async, refused between one and three shares of about
  150,000, 14 shares in all. Every one is `ledger-confirmation-failed`: 12
  "share was not
  committed because its commit gate closed" and 2 "share was not confirmed by
  the database".
  - **They come from one event per run,** the arrival of work for the last
    external tip. The fake node mints tips 6, 12 and 18 s into the 30 s
    `warm_up`. When work for the last one reaches the sessions, the jobs they
    hold go stale at once: 171–247 `stale-job` rejections, which are races
    the server is entitled to lose, and at most three shares caught at the
    commit gate as the epoch changes.
  - **Where that event lands decides the verdict.** At 4 frontends, and at 2
    frontends under synchronous replication, all 2,000 sessions had work for
    the last tip 3.3–7.8 s after it was minted, inside `warm_up`, so
    `steady_state` sees neither the stale jobs nor the refused share and
    reads "met". At 1 and 2 frontends async it took 19.9–36.7 s, which is 8
    to 25 s into `steady_state`. The rows differ in when a tip's work
    arrived, not in the rate they sustained.
  - **Why the arrival time splits in two was not investigated,** and it is a
    result of its own; see
    [Time to usable work after a tip](#time-to-usable-work-after-a-tip).
  - **The rule was not relaxed to absorb this.** It was fixed before these
    runs, and a refusal count matters in general: a refusal returns in half a
    millisecond, so a run that refused a quarter of its shares, as #271's
    did, can still show no shortfall. Instead each "not met" carries its
    reason and its size.
  - **What it leaves open** is whether a share refused at the commit gate
    during a tip change is a race the server is entitled to lose, as
    `stale-job` is, or a refusal of valid work. The harness classes it as the
    second, by its `reason_id`. That is a question for the owners of #429's
    gate and of the harness's classifier.

**Reading the 2,000 shares/s table.**

- **No configuration meets it.** The median `burst` rate is between 1,468 and
  1,623 shares/s. Between a fifth and a quarter of the target's tokens could
  not be placed, and the median ACK p99 is 1.8–4.7 s.
- **Adding frontends adds waiters, not throughput.** At 400k `ORDER_LOCK`
  averaged 14.4, 30.3 and 62.4 waiters at 1, 2 and 4 frontends, which is each
  topology's database connections minus the holder. The rate did not rise
  with them: 1,623, 1,490 and 1,504 shares/s. This is #271's finding, now at
  the production window and at more than five times its rate.
- **The frontends are idle while it happens.** All frontends together used at
  most 0.48 cores in `burst`, in any run.
- **The window size does not move the ceiling.** 200k, 400k and 500k at 1
  frontend gave 1,558, 1,623 and 1,550 shares/s.
- **At 2 frontends the saturated phase also refuses work.** In five of the
  six async repeats at 400k and 500k, `burst` refused 1,041–1,516 shares with
  `backend-rpc-unavailable`, "current chain state is unavailable", 1.1–1.7 %
  of what it offered. It is the class that made up 23–46 % of offers in
  #271's runs. Here it appears in no other D1 rate phase of any ordinary run.

### Time to usable work after a tip

The fake node mints three external tips during `warm_up`, 6 s apart, at
heights 101, 102 and 103. Tips 101 and 102 are each replaced after 6 s; tip
103 is never replaced. A tip counts as served in a run only when all 2,000
sessions had work for it while it was still the tip. Each cell gives the
number of repeats that served the tip and, for those, the time until the last
session had work: median, with minimum and maximum.

| window | fe | repl | n | tip 101 (6 s life): repeats that served it | tip 102 (6 s life): repeats that served it | tip 103 (never replaced): repeats that served it |
|---|---|---|---|---|---|---|
| 200k | 1 | async | 1 | 1 of 1: 5.4 s | 1 of 1: 5.5 s | 1 of 1: 5.6 s |
| 400k | 1 | async | 3 | 0 of 3 | 0 of 3 | 3 of 3: 21.6 (19.9–22.0) s |
| 400k | 2 | async | 3 | 0 of 3 | 0 of 3 | 3 of 3: 35.1 (33.7–36.7) s |
| 400k | 4 | async | 3 | 0 of 3 | 2 of 3: 4.2 (4.0–4.4) s | 3 of 3: 3.8 (3.7–7.8) s |
| 400k | 2 | sync | 3 | 3 of 3: 3.4 (3.3–3.4) s | 3 of 3: 3.3 (3.3–3.4) s | 3 of 3: 3.3 (3.3–3.3) s |
| 500k | 1 | async | 3 | 0 of 3 | 0 of 3 | 3 of 3: 23.8 (22.8–25.9) s |
| 500k | 2 | async | 3 | 0 of 3 | 0 of 3 | 3 of 3: 27.5 (26.7–27.8) s |
| 500k | 4 | async | 3 | 2 of 3: 4.9 (4.7–5.1) s | 3 of 3: 4.4 (4.4–4.9) s | 3 of 3: 4.5 (4.4–5.0) s |

- **The time splits in two, and the split follows the configuration, not the
  repeat.** At 4 frontends, and at 2 frontends under synchronous replication,
  work reaches every session 3.3–7.8 s after a tip. At 1 and 2 frontends
  async the last tip takes 19.9–36.7 s, and tips 101 and 102 are replaced
  before any session has work for them, in every repeat.
- **It is not drift.** The configurations were interleaved within each pass,
  and each one kept its own value across its three repeats, which were made
  30 to 40 minutes apart and in two cases a day apart.
- **It is not the window alone.** The same 1-frontend async configuration
  that takes about 22 s here served every own-block landing of the
  [dense-cadence phase](#dense-cadence-at-d1) in 3.0–3.7 s, at the same
  window and rate. The slow case is specific to external tips arriving in
  the first seconds of load.
- **The cause was not investigated.** The frontend log line `template refresh
  deferred error=tip observation superseded` appears only in runs where a tip
  was replaced before it was served. (One 500k 4-frontend repeat left a tip
  unserved without logging it.) That restates the symptom and does not
  explain it. In particular nothing
  here explains why synchronous replication, with the same 2 frontends and
  the same offered rate, serves every tip in 3.3 s while its async pair
  takes about ten times as long. #271 saw the slow case only: 35–71 s at
  20k, and no tip served within its 6 s life.
- **Why it matters here.** It decides which phase the tip-change rejections
  fall in, and so which rows of the [D1 verdict](#d1-verdict) read "met".

### The blocked sizes now run

All three sizes that #271 recorded as refused now run. The argument lists are
unchanged from `w11-{200,400,500}k-fe1-async-r1`: 1 frontend, async, D1,
2,000 sessions.

| Size | Run id | Exit | JSONB refusals in the frontend log | steady achieved /s · ACK p99 ms | burst achieved /s · ACK p99 ms | frontend RSS max MiB (sampled) | lowest reclaimable-approx MiB |
|---|---|---|---|---|---|---|---|
| 200k | d1-200k-fe1-async-r1 | 0 | 0 | 499.997 · 20.5 | 1,558 · 1,886 | 650 | 62,338 |
| 400k | d1-400k-fe1-async-r1 | 0 | 0 | 499.4 · 18.5 | 1,623 · 4,026 | 928 | 62,537 |
| 500k | d1-500k-fe1-async-r1 | 0 | 0 | 499.4 · 13.2 | 1,561 · 4,744 | 1,075 | 62,600 |

- **Exit 0 at every size, so #273 holds.** Each frontend became ready and
  served work, no frontend log holds the JSONB refusal, and
  `blocked.blocked` is `false` in all three side reports. On #271's base all
  three exited 3 at startup.
- **Every run reconciled exactly:** no ACK/commit divergence, no
  unknown-outcome commit, no durability finding, and no committed share whose
  acknowledgement was lost. (At 200k, 17 `slow_database` submits were still
  unanswered when the stop drain expired; PostgreSQL holds none of them.)
- **Memory is no longer the constraint it was.** One frontend holds about
  1 GiB at a 500k window. On #271's base one frontend held 2.7 GiB at 100k,
  and seeding 500k left that host 431 MiB above its floor. The two hosts and
  builds differ, so this shows that the window fits here, not by how much
  #273 reduced it.
- **These three runs are also the first repeat of the 1-frontend cells** at
  400k and 500k in the tables below.

### The flush control

This control is beyond the steps #447 lists. It exists because the host's
commits do not force the drive cache, and the verdict above cannot be read
without knowing what that is worth.

- **What differs.** The control is the 1-frontend async 400k configuration,
  argument list unchanged. The one difference is a line in the cluster's
  `postgresql.conf`: `wal_sync_method = fsync_writethrough`. It was added by
  pointing `--pg-bin-dir` at a directory of links to the real PostgreSQL
  binaries in which `initdb` is wrapped to append that line. The standby is
  made by `pg_basebackup`, which copies the primary's configuration with its
  data directory. The harness was not changed.
- **It reached the runtime.** Each run's `database-profile.json` records
  `wal_sync_method: fsync_writethrough`, and during the first run the live
  primary reported the same value with source `configuration file`.
- **Same host, same build, same day** as its ordinary pair,
  `d1-400k-fe1-async-r{1,2,3}`.

| run id | exit | steady achieved /s (target 500) | steady shortfall tokens | steady ACK p50 / p99 ms | burst achieved /s (target 2000) | reconn achieved /s (target 500) | steady ORDER_LOCK mean waiters | committed shares whose answer was lost | divergences / unknown-outcome / durability findings |
|---|---|---|---|---|---|---|---|---|---|
| d1-400k-fe1-async-wt-r1 | 5 | 112.6 | 72,359 | 10,194 / 40,408 | 114.2 | 134.9 | 14.07 | 1 | 0 / 0 / 0 |
| d1-400k-fe1-async-wt-r2 | 0 | 117.3 | 74,981 | 10,323 / 38,296 | 107.2 | 122.5 | 14.11 | 0 | 0 / 0 / 0 |
| d1-400k-fe1-async-wt-r3 | 5 | 117.9 | 75,649 | 10,338 / 37,519 | 111.0 | 133.2 | 14.11 | 2 | 0 / 0 / 0 |

- **With a forced flush, the 500 shares/s phase is missed by a factor of
  about four.** `steady_state` acknowledged 112.6, 117.3 and 117.9
  shares/s, against a median of 499.4 for its pair. About half of the
  target's tokens could not be placed at all, and accepted shares waited a
  median of about 10 s for their ACK.
- **The 2,000 shares/s phase achieves no more**, because the ceiling is the
  same serialised commit: `ORDER_LOCK` averaged 14 waiters of a possible 15
  in `steady_state` already, where the pair averaged 0.08.
- **So the ordinary runs' 500 shares/s result is a property of cheap commits,
  not only of this build.** `pg_test_fsync` puts a forced flush on this disk
  at
  3.5 ms against 34 µs without one. A commit that waits for one sits inside
  `ORDER_LOCK`, and nothing a frontend does runs in parallel with it.
- **What this does and does not say about other hardware.**
  - It is not a prediction for the rehearsal host. An Apple laptop SSD takes
    milliseconds to honour `F_FULLFSYNC`; a server device with power-loss
    protection can take tens of microseconds, and a network volume can take
    more than either.
  - It does say which number decides the outcome there: what one WAL flush
    costs. `pg_test_fsync` on the rehearsal host's WAL volume gives it in two
    minutes, before any harness run.
  - #271's host, a Linux VM on a virtual disk, sustained 238–304 shares/s.
    That sits between this host's two figures, which is where a commit cost
    between the two would put it. The builds differ as well, so this is
    consistent with the explanation and does not prove it.
- **Two of the three runs exit 5, and nothing was lost in either.** In
  `reconnect`, a client-initiated reconnect waits up to 25 s for its
  outstanding submit before it closes the socket. The `reconnect` ACK p99 was
  56, 37 and 33 s in the three runs, all above that limit, so the wait
  expired for one submit in the first run and two in the third, and
  PostgreSQL holds all three shares. The harness classes each as
  transport-indeterminate: the share is present and only its acknowledgement
  is missing. None of the three runs has an ACK/commit divergence, an
  unknown-outcome commit or a durability finding. Exit-5 runs are kept out of
  the medians, so the control's row in the tables is the second run alone;
  the table above gives all three.

### The build control

This control is also beyond the steps #447 lists. #271 measured 238–304
shares/s and this part measures 500, and between the two both the host and
the build changed. #271 declined to attribute its own base-to-base
differences for the same reason. This control changes one of the two: it runs
#271's 20k argument list, `w11-20k-fe1-async`, unchanged, on this host, on
both builds.

- **Both builds were built the same way:** release profile, Rust 1.98.1, each
  in its own clean detached worktree, `5d0042f6` being the base of the #271
  matrix above.
- **The runs alternate:** current build, then #271's, three times, on the
  same afternoon and on AC power.
- **"Build" means the harness and the server together.** Two cross runs were
  attempted to separate them, and neither could run; see below.

This is the one table in this part that holds more than one build, as #271's
[comparison with its earlier
bases](#comparison-with-the-earlier-harness-bases)
does, and every row says which binaries it ran.

| run id | binaries | exit | steady achieved /s (target 500) | steady shortfall tokens | steady ACK p50 / p99 ms | burst achieved /s (target 2000) | burst shortfall tokens | burst ACK p50 / p99 ms | burst ORDER_LOCK max / mean waiters | last external tip: all 2,000 sessions with work, s |
|---|---|---|---|---|---|---|---|---|---|---|
| d1-20k-fe1-async-r1 | `a1937054` harness and server (this matrix) | 0 | 499.997 | 0 | 1.6 / 28 | 1,558.5 | 26,487 | 1,261 / 3,871 | 15 / 14.35 | 0.7 |
| d1-20k-fe1-async-r2 | `a1937054` harness and server (this matrix) | 0 | 499.997 | 0 | 1.6 / 17 | 1,649.3 | 21,042 | 1,216 / 3,339 | 15 / 14.34 | 0.7 |
| d1-20k-fe1-async-r3 | `a1937054` harness and server (this matrix) | 0 | 499.997 | 0 | 1.1 / 32 | 1,621.2 | 22,729 | 1,237 / 1,861 | 15 / 14.41 | 0.7 |
| d1-20k-fe1-async-build5d0042f6-r1 | `5d0042f6` harness and server (#271's build) | 0 | 499.997 | 0 | 1.4 / 431 | 1,978.9 | 1,266 | 154 / 1,652 | 15 / 9.58 | 1.3 |
| d1-20k-fe1-async-build5d0042f6-r2 | `5d0042f6` harness and server (#271's build) | 8 | 499.997 | 0 | 1.4 / 418 | 1,994.2 | 348 | 1 / 987 | 15 / 5.04 | 1.3 |
| d1-20k-fe1-async-build5d0042f6-r3 | `5d0042f6` harness and server (#271's build) | 0 | 499.997 | 0 | 1.5 / 446 | 1,992.5 | 448 | 80 / 1,167 | 15 / 10.28 | 1.4 |

- **#271's 238–304 shares/s was its host.** Its own build sustains 500
  shares/s here in all three runs, with no shortfall and no refused share. At
  20k the two builds do not differ at all on the 500 shares/s phase's rate.
- **The current build's 2,000 shares/s ceiling is about a fifth lower than
  #271's build's, on the same host.**
  - #271's build acknowledged 1,979–1,994 shares/s in `burst` and placed
    98.9–99.7 % of the target, with `ORDER_LOCK` averaging 5–10 waiters of a
    possible 15: the lock was not saturated.
  - The current build acknowledged 1,559–1,649 with the lock saturated at
    14.3–14.4.
  - The two ranges are about 330 shares/s apart and neither set's spread
    exceeds 100, so three interleaved repeats are enough to call them
    different.
  - By the strict rule #271's build does not meet the phase either: 348–1,266
    of its 120,000 tokens went unplaced.
- **The current build is more than ten times better on latency at 500
  shares/s:** `steady_state` ACK p99 of 17–32 ms against 418–446 ms.
- **It also serves a new tip sooner at this window:** 0.7–0.9 s for every
  session against 1.3–1.5 s. #271's host took 35–71 s on the same build, so
  that
  figure, too, was its host.
- **Whether the server or the harness moved could not be measured.** Both
  cross runs were attempted and both were refused at startup within three
  seconds, each correctly:
  - the `a1937054` harness driving the `5d0042f6` server: the harness had
    migrated the database to its own schema, and the older server refused it,
    `database declares capability candidate_offer_lifecycle = 1, which this
    server does not understand: a newer PRISM release wrote this database`;
  - the `5d0042f6` harness driving the `a1937054` server: `migration 011
    requires every earlier instance to report drained or stopped; offending
    instances: load-seed`.

  So "build" above stays the harness and the server together. The harness
  drives the load, and a client that offered more slowly would lower the
  ceiling without any change in the server; this control cannot rule that
  out.
- **What this does not show.** It is one window, 20k, at one frontend. The
  production windows were not run on #271's build, which could not run them.
  The cause of the lower ceiling was not looked for; 50 commits separate
  the two builds. Whether a fifth of the peak rate is a fair price
  for the latency is a judgement for the people deciding D1, and this
  section only puts the two numbers side by side.
- **One of #271's build's runs exited 8**
  (`d1-20k-fe1-async-build5d0042f6-r2`).
  Its `slow_database` phase acknowledged no share at all in its minute, and
  that harness then withholds the artifact because the phase has no ACK
  latency to state. The phase serves about 3 shares/s on either build, so an
  empty minute is within its reach. The run's `steady_state` and `burst`
  completed in the ordinary way and are in the table.

### Found blocks at 400k

`d1-400k-fe2-async-blocks3-r1` is the ordinary 2-frontend async 400k
configuration with `--scheduled-blocks 3`, so three own blocks are found and
submitted during `steady_state`. It ran with `--keep-artifacts`, so the run's
own database could be inspected afterwards.

- **All three blocks landed.** The phase reports `scheduled_blocks: 3`. The
  node accepted all three submissions, at heights 104, 105 and 106, and
  recorded three pool tip changes. The run exited 0, `blocked.blocked` is
  `false`, and it reconciled exactly.
- **The refusal #271 expected did not happen.** #271 predicted that once #273
  landed, this size would get past startup and reach #265's refusal of the
  audit bundle at landing. It reached the landing path three times and no
  refusal was logged. This is the first time #265's candidate path and #267's
  landing have been exercised under load at the production window.
  - The frontend logs hold one candidate-related line: `candidate polling
    failed error=pool timed out while waiting for an open connection`, once,
    on `load-fe-1`. It falls in the last seconds of `slow_database`, almost
    three minutes after the phase with the landings ended, so it is the
    delayed database starving the pool and not a landing that failed.
- **No unfinished candidate was left behind.** After the run, the kept primary
  was restarted and `qbit-prism-server candidates list --limit 10000` (#268)
  was run against it. It printed `no unfinished candidates`; the JSON form
  holds 0 rows and `truncated: false`.
- **The database agrees, so the empty list is not an empty database.** The
  run's `qbit_block_candidate_outbox` holds exactly three rows. All three
  are `state = submitted` with `offer_outcome = accepted`, one attempt each
  and no `last_error`, and each completed 5.8–6.4 s after it was created,
  inside `steady_state`.
- **What three landings cost.** Each landing bumps the payout revision, and
  every session's job goes stale at once.
  - `steady_state` acknowledged 482.5 shares/s against about 499.3 in the
    same configuration without landings. The scheduler still placed the whole
    target (`shortfall` 0), so the difference is rejected shares, not offers
    that could not be made.
  - 5,251 of those rejections are races the server is entitled to lose: 4,901
    `stale-job` and 350 `unknown-job`.
  - 12 are refusals of shares the harness holds valid, all
    `ledger-confirmation-failed`: 6 "share was not committed because its
    commit gate closed" and 6 "share was not confirmed by the database".
    PostgreSQL holds none of the 12, so none is a divergence.
  - ACK p99 rose to 51 ms, from a median of 23 ms.

One run cannot show spread. It shows that the path works at this window and
what a landing costs in rejected work; the dense-cadence phase is the
measurement of that cost per landing.

Per run: own blocks the scheduler asked for (`phases[].scheduled_blocks`),
what the fake node recorded for `submitblock` (`node.submissions`), the pool
tips that resulted (`node.tip_changes` with origin `pool`), client failures of
kind `scheduled-block`, and frontend log refusals (`blocked.log_matches`).
Per-run values; nothing here is a median.

| run id | status | blocks scheduled (by phase) | node submitblock: calls / accepted / rejected | node rejection reasons | pool tip changes | client scheduled-block failures | frontend log refusals (kind × lines) |
|---|---|---|---|---|---|---|---|
| d1-400k-fe2-async-blocks3-r1 | ok | steady: 3 | 3 / 3 / 0 | – | 3 | 0 | job_deferred × 3000; refresh_deferred × 4 |

### Dense cadence at D1

#271 dropped this block after one attempt showed the phase could not start
at D1 on its host. #447 asked for it to be attempted again at D1, and measured
at a reduced rate if it still could not start.

**It started at D1 at the production window, three times out of three.**

| run id | fe | landed | payout-revision bumps | achieved / offered /s (target 500) | shortfall tokens | entitled-race rejections | refused valid shares | rejections per landing | all 2,000 sessions with new-tip work, s after a landing: median (min–max) | ACK p50 / p99 ms | rejections the harness attributes to a landing |
|---|---|---|---|---|---|---|---|---|---|---|---|
| d1-dense400k-fe1-async-r1 | 1 | 14 of 15 | 28 | 415.2 / 500.0 | 0 | 19,262 | 1,101 | 1,454 | 3.12 (2.98–3.71) | 1.1 / 381 | 0 |
| d1-dense400k-fe1-async-r2 | 1 | 15 of 15 | 30 | 409.7 / 500.0 | 0 | 20,596 | 1,079 | 1,445 | 3.13 (3.02–3.47) | 1.1 / 392 | 0 |
| d1-dense400k-fe2-async-r1 | 2 | 15 of 15 | 30 | 399.4 / 500.0 | 0 | 23,016 | 1,139 | 1,610 | 3.19 (2.96–3.51) | 1.2 / 353 | 0 |

All three are D1 runs with `--cadence dense --scheduled-blocks 15`, async,
2,000 sessions, 400k window: the phase is 240 s at 500 shares/s, with own
blocks landing 9, 19, 9, 18 and 20 s apart. Nothing here is a median across
runs.

- **A landing costs about three seconds of work.** After every one of the
  44 landings, all 2,000 sessions had work for the new tip 3.0–3.7 s later,
  including across the 9 s gaps, and no landing's tip was replaced before it
  was served. At 500 shares/s that is about 1,550 shares mined on a job the
  landing made stale, and the phase rejected 1,445–1,610 per landing.
- **So the pool acknowledges 399–415 of every 500 shares/s offered while
  blocks land at this cadence**, with the whole target placed (`shortfall`
  0). About 95 % of the rejections are races the server is entitled to lose,
  `stale-job` and `unknown-job`.
- **About 1,100 per run are refusals of valid work.** Nearly all are
  `backend-rpc-unavailable`, "current chain state is unavailable"; 27–44 per
  run are `ledger-confirmation-failed`. PostgreSQL holds none of them.
- **One scheduled block of 45 never reached the node.** In the first run the
  block found 9 s after the previous landing was answered `unknown-job`,
  "stale job", 14 ms after it was sent: the job it was mined on had already
  been retired. The harness counts it as a rejected landing, correctly. What
  it means for a real block found seconds after another is a question for
  the owners of the found-block path; this document only records that it
  happened once in 45.
- **The first of the three ran partly on battery** (see [Host](#host-447)).
  Its repeat on AC power gives the same figures.

**The harness's own dense-cadence figures are empty on this build, and that
is a harness gap, not a result.** The section attributes to a landing only
rejections whose message is `new payout work is pending` or `new tip work is
pending`. At this base the server no longer sends the first at all, and the
second did not occur. So the section below reports 0 rebuild-pending
rejections, no window, and a `null` budget for #291, in runs that rejected
more than 20,000 shares around their landings. The table above is taken from
the same reports' phase entry, rejection tally and time-to-new-tip-work
block, which do not depend on that attribution. Until the harness attributes
`stale-job` and `unknown-job` rejections to landings, its proposed #291
budget should not be read as "no rejected work".

**At 20k the phase still cannot start, by a small margin.**
`d1-dense20k-fe1-async-r1` repeats #271's attempt with the argument list
unchanged, and exited 6 with the same reason:

```text
20 submit(s) offered before the dense_cadence phase were still outstanding 25.0 s later, so its 0 ms delay could not be applied without them finishing under it
```

#271's host left 1,201 outstanding. Every D1 phase before the abort
completed: `steady_state` placed its whole target and refused nothing, and
`burst` reached 1,643 shares/s. The condition is
the same one: `slow_database` is offered 500 shares/s and serves about 3, so
some submits are still waiting when the 25 s boundary limit expires. On this
host the three 400k attempts started and the 20k attempt missed by 20
submits. The margin is thin, and the phase starting should not be relied on.

Per run; nothing here is a median, because a percentile of per-landing windows
cannot be pooled across runs from the summaries alone. Windows are
milliseconds on the harness's monotonic clock; the per-landing distributions
are counts of shares, one sample per landing and frontend
(cadence.rs:678-696). `landings` counts landings that were given a window
(`dense_cadence.landings`), `landed` those the node accepted
(`landing_outcomes.landed`).

| run id | status | ran | landing budget / attempts / landings with a window | outcomes: landed / rejected / never produced / accepted without node submission / no response | schedule slots over budget | bumps: observed / attributed / unattributed | rebuild-pending rejections: in phase / attributed / unattributed | revision sampler coverage | no-landing reason |
|---|---|---|---|---|---|---|---|---|---|
| d1-dense20k-fe1-async-r1 | NOT COMPLETED (exit 6); not in medians | false | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| d1-dense400k-fe1-async-r1 | ok | true (phase completed: true) | 15 / 15 / 14 | 14 / 1 / 0 / 0 / 0 | 0 | 28 / 28 / 0 | 0 / 0 / 0 | 1 / blind: false | – |
| d1-dense400k-fe1-async-r2 | ok | true (phase completed: true) | 15 / 15 / 15 | 15 / 0 / 0 / 0 / 0 | 0 | 30 / 30 / 0 | 0 / 0 / 0 | 1 / blind: false | – |
| d1-dense400k-fe2-async-r1 | ok | true (phase completed: true) | 15 / 15 / 15 | 15 / 0 / 0 / 0 / 0 | 0 | 30 / 30 / 0 | 0 / 0 / 0 | 1 / blind: false | – |

Summaries over every landing and frontend (`dense_cadence.summaries.overall`):

| run id | status | combined rebuild-pending window ms: p50 / p99 / max (samples) | combined rebuild-pending rejections per landing per frontend: p50 / p99 / max (samples) | rejected before new-revision work per landing per frontend: p50 / p99 / max (samples) | lost valid shares: total / found in PostgreSQL | #291 budget ms: window p99 / recommended |
|---|---|---|---|---|---|---|
| d1-dense400k-fe1-async-r1 | ok | n/a / n/a / n/a (0) | 0 / 0 / 0 (14) | 0 / 0 / 0 (14) | 0 / 0 | n/a / n/a |
| d1-dense400k-fe1-async-r2 | ok | n/a / n/a / n/a (0) | 0 / 0 / 0 (15) | 0 / 0 / 0 (15) | 0 / 0 | n/a / n/a |
| d1-dense400k-fe2-async-r1 | ok | n/a / n/a / n/a (0) | 0 / 0 / 0 (30) | 0 / 0 / 0 (27) | 0 / 0 | n/a / n/a |

"New-revision work" is an approximation the harness labels as such: the first
`clean_jobs` notify at or after the bump stands in for the first job at the
new revision (cadence.rs:1221-1228).

Why a cell is `n/a`, or what qualifies it:

- dense phase did not run: "the run ended before the dense_cadence phase could
  run, so there is no schedule, no landing and no window to report"; aborted:
  "20 submit(s) offered before the dense_cadence phase were still outstanding
  25.0 s later, so its 0 ms delay could not be applied without them finishing
  under it" — d1-dense20k-fe1-async-r1
- `summaries.overall.combined_rebuild_pending_window_duration_millis`: no
  samples were recorded — d1-dense400k-fe1-async-r1;
  d1-dense400k-fe1-async-r2; d1-dense400k-fe2-async-r1

### Measurements

Every table below is generated from the side reports by one script and none
is typed by hand. The flush control shares these tables with its ordinary
pair, in its own rows: its plan column reads
`d1 +wal_sync_method=fsync_writethrough`, and no median mixes the two. The
build control's runs, on either build, are not in these tables; they are
under [The build control](#the-build-control). Source line references in this
part
are to `a1937054`.

**Configuration groups.**

Grouping uses the report's own fields: `window.requested_window_shares`,
`topology.frontends`, `premise.replication.declared`, `topology.plan`, and, so
that only true repeats share a row, `topology.sessions`, each phase's
`target_rate_shares_per_second`, `dense_cadence.cadence`, the scheduled-block
count, `mid_flight_kill.ran`, the primary's effective `wal_sync_method`
(`database-profile.json`), the commit the binaries were built from and whether
the server binary was that build's own (`driver.json`). A run that differs in
any of these is never a repeat of another, and its plan column says how it
differs. Runs are in `driver.json` `started_at_utc` order.

| window | fe | repl | plan | sessions | target /s steady / burst / reconn / slowdb | scheduled blocks | n in medians | runs in medians (run order) | completed with a finding (same group) | not completed (same window/fe/repl/plan) |
|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 2000 | 500 / 2000 / 500 / 500 | 0 | 1 | d1-200k-fe1-async-r1 | – | – |
| 400k | 1 | async | d1 | 2000 | 500 / 2000 / 500 / 500 | 0 | 3 | d1-400k-fe1-async-r1, d1-400k-fe1-async-r2, d1-400k-fe1-async-r3 | – | – |
| 400k | 2 | async | d1 | 2000 | 500 / 2000 / 500 / 500 | 0 | 3 | d1-400k-fe2-async-r1, d1-400k-fe2-async-r2, d1-400k-fe2-async-r3 | – | – |
| 400k | 4 | async | d1 | 2000 | 500 / 2000 / 500 / 500 | 0 | 3 | d1-400k-fe4-async-r1, d1-400k-fe4-async-r2, d1-400k-fe4-async-r3 | – | – |
| 400k | 2 | sync | d1 | 2000 | 500 / 2000 / 500 / 500 | 0 | 3 | d1-400k-fe2-sync-r1, d1-400k-fe2-sync-r2, d1-400k-fe2-sync-r3 | – | – |
| 400k | 2 | async | d1 +3 blocks | 2000 | 500 / 2000 / 500 / 500 | 3 | 1 | d1-400k-fe2-async-blocks3-r1 | – | – |
| 400k | 1 | async | d1 +dense(15) | 2000 | 500 / 2000 / 500 / 500 | 15 | 2 | d1-dense400k-fe1-async-r1, d1-dense400k-fe1-async-r2 | – | – |
| 400k | 2 | async | d1 +dense(15) | 2000 | 500 / 2000 / 500 / 500 | 15 | 1 | d1-dense400k-fe2-async-r1 | – | – |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 2000 | 500 / 2000 / 500 / 500 | 0 | 1 | d1-400k-fe1-async-wt-r2 | d1-400k-fe1-async-wt-r1, d1-400k-fe1-async-wt-r3 | – |
| 500k | 1 | async | d1 | 2000 | 500 / 2000 / 500 / 500 | 0 | 3 | d1-500k-fe1-async-r1, d1-500k-fe1-async-r2, d1-500k-fe1-async-r3 | – | – |
| 500k | 2 | async | d1 | 2000 | 500 / 2000 / 500 / 500 | 0 | 3 | d1-500k-fe2-async-r1, d1-500k-fe2-async-r2, d1-500k-fe2-async-r3 | – | – |
| 500k | 4 | async | d1 | 2000 | 500 / 2000 / 500 / 500 | 0 | 3 | d1-500k-fe4-async-r1, d1-500k-fe4-async-r2, d1-500k-fe4-async-r3 | – | – |
| 20k | 1 | async | d1 | n/a | n/a | n/a | 0 | – | – | d1-dense20k-fe1-async-r1 |

#### Rate and ACK latency

| window | fe | repl | plan | n | steady achieved/offered (target) /s | steady ACK p50 / p99 / max ms | burst achieved/offered (target) /s | burst ACK p50 / p99 / max ms | reconn achieved/offered (target) /s | reconn ACK p50 / p99 / max ms | slowdb achieved/offered (target) /s | slowdb ACK p50 / p99 / max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | 499.997 / 499.997 (500) | 1.1 / 20.5 / 630 | 1,558 / 1,558 (2000) | 1,289 / 1,886 / 2,241 | 499.983 / 499.983 (500) | 1.1 / 128 / 610 | 3.2 / 329 (500) | 10,015 / 30,348 / 30,429 |
| 400k | 1 | async | d1 | 3 | 499 (499–499) / 499.997 (499.995–499.997) (500) | 1.1 (1.1–1.7) / 18.5 (8.6–25.1) / 574 (546–636) | 1,623 (1,503–1,756) / 1,623 (1,503–1,756) (2000) | 1,228 (1,145–1,318) / 4,026 (3,357–4,397) / 10,033 (9,626–11,348) | 499.983 (499.975–499.983) / 499.983 (499.975–499.983) (500) | 1.2 (1.1–1.7) / 780 (705–1,015) / 1,189 (1,132–1,390) | 1.7 (1.7–1.7) / 417 (412–422) (500) | 10,413 (10,216–10,483) / 30,010 (29,993–30,514) / 30,024 (30,020–30,516) |
| 400k | 2 | async | d1 | 3 | 499 (499–499) / 499.995 (499.995–499.997) (500) | 1.1 (1.1–1.1) / 23.2 (11.4–37.5) / 640 (621–726) | 1,490 (1,446–1,522) / 1,510 (1,468–1,522) (2000) | 1,340 (1,324–1,368) / 2,608 (2,464–3,099) / 5,882 (5,828–6,596) | 499.983 (499.975–499.983) / 499.983 (499.975–499.983) (500) | 1.1 (1.1–1.1) / 870 (845–940) / 1,275 (1,261–1,334) | 2.4 (2.4–2.6) / 423 (422–430) (500) | 14,825 (14,596–16,225) / 29,710 (29,651–29,861) / 29,816 (29,774–29,863) |
| 400k | 4 | async | d1 | 3 | 499.995 (499.995–499.997) / 499.995 (499.995–499.997) (500) | 1.1 (1.1–1.2) / 15 (13.2–16.1) / 566 (473–606) | 1,504 (1,423–1,663) / 1,504 (1,423–1,663) (2000) | 1,346 (1,213–1,411) / 1,882 (1,634–1,972) / 2,016 (1,784–4,093) | 499.975 (499.967–499.983) / 499.983 (499.975–499.983) (500) | 1.2 (1.1–1.7) / 1,309 (1,102–1,381) / 1,674 (1,503–1,771) | 4.4 (3.6–4.4) / 439 (423–443) (500) | 16,148 (15,393–18,004) / 41,191 (28,282–41,425) / 41,791 (28,918–42,701) |
| 400k | 2 | sync | d1 | 3 | 499.995 (499.995–499.997) / 499.995 (499.995–499.997) (500) | 1.2 (1.2–1.3) / 33 (9–2,560) / 603 (505–2,660) | 1,505 (1,403–1,617) / 1,505 (1,403–1,617) (2000) | 1,342 (1,253–1,432) / 1,812 (1,667–1,849) / 1,991 (1,790–2,079) | 499.983 (499.983–499.983) / 499.983 (499.983–499.983) (500) | 1.2 (1.2–1.2) / 1,265 (966–1,355) / 1,620 (1,381–1,692) | 2.4 (2.2–2.5) / 420 (418–440) (500) | 14,485 (13,379–14,996) / 29,708 (29,369–29,751) / 29,848 (29,414–29,925) |
| 400k | 2 | async | d1 +3 blocks | 1 | 482 / 499.995 (500) | 1.2 / 51.1 / 779 | 1,600 / 1,600 (2000) | 1,247 / 1,688 / 1,946 | 499.983 / 499.983 (500) | 1.1 / 828 / 1,246 | 3.8 / 428 (500) | 13,698 / 29,334 / 29,725 |
| 400k | 1 | async | d1 +dense(15) | 2 | 498 (497–499) / 498.828 (497.66–499.997) (500) | 1.1 (1.1–1.1) / 1,963 (22–3,903) / 2,966 (532–5,401) | 1,491 (1,455–1,527) / 1,491 (1,456–1,527) (2000) | 1,318 (1,287–1,348) / 4,786 (4,378–5,194) / 11,300 (10,871–11,729) | 499.983 (499.983–499.983) / 499.983 (499.983–499.983) (500) | 1.1 (1.1–1.2) / 887 (775–999) / 1,373 (1,192–1,554) | 1.7 (1.7–1.7) / 422 (421–422) (500) | 10,342 (10,226–10,459) / 30,133 (30,019–30,247) / 30,133 (30,019–30,247) |
| 400k | 2 | async | d1 +dense(15) | 1 | 499 / 499.995 (500) | 1.1 / 19.8 / 641 | 1,467 / 1,493 (2000) | 1,368 / 2,255 / 5,706 | 499.983 / 499.983 (500) | 1.1 / 1,008 / 1,350 | 2.5 / 423 (500) | 15,072 / 29,621 / 29,927 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | 117 / 250 (500) | 10,323 / 38,296 / 60,775 | 107 / 762 (2000) | 7,330 / 39,017 / 66,057 | 122 / 309 (500) | 8,134 / 36,512 / 38,573 | 1.8 / 406 (500) | 11,532 / 30,386 / 30,390 |
| 500k | 1 | async | d1 | 3 | 499 (499–499) / 499.997 (499.995–499.997) (500) | 1.2 (1.1–1.2) / 13.3 (13.2–26.7) / 606 (598–711) | 1,550 (1,481–1,561) / 1,550 (1,481–1,561) (2000) | 1,253 (1,251–1,324) / 4,744 (3,828–4,870) / 12,042 (11,052–12,149) | 499.975 (499.975–499.983) / 499.975 (499.975–499.983) (500) | 1.1 (1.1–1.4) / 865 (766–887) / 1,245 (1,184–1,258) | 1.8 (1.6–1.8) / 416 (416–417) (500) | 10,859 (10,040–11,039) / 30,029 (30,023–30,285) / 30,040 (30,029–30,293) |
| 500k | 2 | async | d1 | 3 | 499 (499–499) / 499.997 (499.995–499.997) (500) | 1.1 (1.1–1.1) / 17.4 (8.1–28.1) / 586 (515–612) | 1,504 (1,496–1,523) / 1,522 (1,522–1,548) (2000) | 1,320 (1,302–1,323) / 2,289 (2,179–3,083) / 6,167 (6,140–7,119) | 499.975 (499.975–499.983) / 499.975 (499.975–499.983) (500) | 1.1 (1.1–1.1) / 885 (867–904) / 1,291 (1,265–1,302) | 2.3 (2.1–2.3) / 425 (423–425) (500) | 14,019 (13,030–14,253) / 29,834 (29,569–29,926) / 29,927 (29,896–29,994) |
| 500k | 4 | async | d1 | 3 | 499.997 (499.995–499.997) / 499.997 (499.995–499.997) (500) | 1.1 (1.1–1.2) / 13.3 (13–22.5) / 616 (513–646) | 1,468 (1,401–1,514) / 1,468 (1,401–1,514) (2000) | 1,388 (1,332–1,423) / 1,879 (1,854–1,978) / 2,175 (1,964–4,257) | 499.983 (499.983–499.983) / 499.983 (499.983–499.983) (500) | 1.1 (1.1–1.2) / 1,513 (1,449–1,815) / 1,852 (1,823–2,124) | 2.4 (2.3–4.5) / 440 (431–444) (500) | 14,785 (14,498–17,890) / 27,345 (26,762–40,747) / 27,995 (26,924–40,831) |

#### Refusal latency

A refusal's latency runs from writing the submit to reading the refusal. The
count of refusals (`client_rejection_latency.samples`) is in the last
parentheses.

| window | fe | repl | plan | n | steady refusal p50 / p99 ms (refusals) | burst refusal p50 / p99 ms (refusals) | reconn refusal p50 / p99 ms (refusals) | slowdb refusal p50 / p99 ms (refusals) |
|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | n/a / n/a (0) | n/a / n/a (0) | n/a / n/a (0) | 0.2 / 29,898 (19,524) |
| 400k | 1 | async | d1 | 3 | 168 (166–190) / 408 (363–433) (193 (171–200)) | n/a / n/a (0 (0–0)) | n/a / n/a (0 (0–0)) | 0.2 (0.2–0.2) / 28,452 (28,400–28,905) (24,945 (24,594–25,237)) |
| 400k | 2 | async | d1 | 3 | 200 (186–203) / 453 (449–456) (204 (202–206)) | 0.1 (0.1–0.1) [n=2] / 0.2 (0.2–0.2) [n=2] (1,203 (0–1,356)) | n/a / n/a (0 (0–0)) | 0.2 (0.2–0.2) / 29,123 (28,846–29,137) (25,258 (25,145–25,635)) |
| 400k | 4 | async | d1 | 3 | n/a / n/a (0 (0–0)) | n/a / n/a (0 (0–0)) | n/a / n/a (0 (0–0)) | 0.2 (0.2–0.2) / 23,334 (22,863–34,044) (25,943 (25,193–26,026)) |
| 400k | 2 | sync | d1 | 3 | n/a / n/a (0 (0–0)) | n/a / n/a (0 (0–0)) | n/a / n/a (0 (0–0)) | 0.2 (0.2–0.2) / 29,023 (28,925–29,870) (25,076 (24,957–26,028)) |
| 400k | 2 | async | d1 +3 blocks | 1 | 0.3 / 293 (5,263) | n/a / n/a (0) | n/a / n/a (0) | 0.2 / 29,660 (25,431) |
| 400k | 1 | async | d1 +dense(15) | 2 | 194 (194–195) / 1,763 (463–3,063) (200 (196–203)) | 0.1 [n=1] / 0.2 [n=1] (41.5 (0–83)) | n/a / n/a (0 (0–0)) | 0.2 (0.2–0.2) / 28,589 (28,524–28,654) (25,192 (25,151–25,232)) |
| 400k | 2 | async | d1 +dense(15) | 1 | 202 / 441 (197) | 0.1 / 0.3 (1,561) | n/a / n/a (0) | 0.2 / 29,249 (25,222) |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | 0.2 / 60,470 (39,841) | 0.2 / 55,357 (39,299) | 0.2 / 19,773 (11,191) | 0.2 / 28,547 (24,265) |
| 500k | 1 | async | d1 | 3 | 198 (192–229) / 459 (428–559) (193 (190–248)) | n/a / n/a (0 (0–0)) | n/a / n/a (0 (0–0)) | 0.2 (0.2–0.2) / 28,757 (28,752–28,868) (24,889 (24,879–24,924)) |
| 500k | 2 | async | d1 | 3 | 200 (188–201) / 453 (433–475) (206 (200–215)) | 0.1 (0.1–0.1) / 0.3 (0.2–3.7) (1,496 (1,041–1,516)) | n/a / n/a (0 (0–0)) | 0.2 (0.2–0.2) / 28,882 (28,868–29,495) (25,336 (25,251–25,349)) |
| 500k | 4 | async | d1 | 3 | n/a / n/a (0 (0–0)) | n/a / n/a (0 (0–0)) | n/a / n/a (0 (0–0)) | 0.2 (0.2–0.2) / 23,088 (22,219–23,482) (25,784 (25,714–26,476)) |

Why a cell is `n/a`, or what qualifies it:

- `client_rejection_latency`: no samples were recorded — 26 runs, including
  d1-200k-fe1-async-r1 (steady, burst, reconn); d1-400k-fe1-async-r1 (burst,
  reconn); d1-400k-fe1-async-r2 (burst, reconn)

#### Rejections by `(code, reason_id, message)`

Counts per phase, over the classes found in the data. No harness-bug rejection
class appeared in any run.
A class with no row in a completed phase's tally is a counted 0: the tally is
built from every submit record and lists a class only where it occurred
(run.rs:2665-2698).

| window | fe | repl | plan | n | phase | `20` `backend-rpc-unavailable` "current chain state is unavailable" | `20` `backend-rpc-unavailable` "current payout state is unavailable" | `20` `ledger-confirmation-failed` "share was not committed because its commit gate closed" | `20` `ledger-confirmation-failed` "share was not confirmed by the database" | `21` `stale-job` "stale job" | `21` `unknown-job` "stale job" |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | steady | 0 | 0 | 0 | 0 | 0 | 0 |
| 200k | 1 | async | d1 | 1 | burst | 0 | 0 | 0 | 0 | 0 | 0 |
| 200k | 1 | async | d1 | 1 | reconn | 0 | 0 | 0 | 0 | 0 | 0 |
| 200k | 1 | async | d1 | 1 | slowdb | 15,599 | 548 | 0 | 3,377 | 0 | 0 |
| 200k | 1 | async | d1 | 1 | warm_up | 0 | 0 | 3 | 1 | 0 | 0 |
| 400k | 1 | async | d1 | 3 | steady | 0 (0–0) | 0 (0–0) | 0 (0–1) | 0 (0–0) | 193 (171–199) | 0 (0–0) |
| 400k | 1 | async | d1 | 3 | burst | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 1 | async | d1 | 3 | reconn | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 1 | async | d1 | 3 | slowdb | 22,979 (22,628–23,277) | 272 (219–340) | 0 (0–0) | 1,688 (1,626–1,747) | 0 (0–0) | 0 (0–0) |
| 400k | 1 | async | d1 | 3 | warm_up | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–2) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | async | d1 | 3 | steady | 0 (0–0) | 0 (0–0) | 1 (1–1) | 0 (0–0) | 203 (201–205) | 0 (0–0) |
| 400k | 2 | async | d1 | 3 | burst | 1,203 (0–1,356) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | async | d1 | 3 | reconn | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | async | d1 | 3 | slowdb | 23,167 (22,735–23,302) | 3 (0–4) | 0 (0–0) | 1,974 (1,956–2,897) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | async | d1 | 3 | warm_up | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–1) | 0 (0–0) | 0 (0–0) |
| 400k | 4 | async | d1 | 3 | steady | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 4 | async | d1 | 3 | burst | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 4 | async | d1 | 3 | reconn | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 4 | async | d1 | 3 | slowdb | 23,955 (22,762–24,032) | 20 (11–23) | 0 (0–0) | 1,983 (1,968–2,408) | 0 (0–0) | 0 (0–0) |
| 400k | 4 | async | d1 | 3 | warm_up | 0 (0–0) | 0 (0–0) | 3 (3–7) | 125 (118–268) | 200 (143–221) | 0 (0–0) |
| 400k | 2 | sync | d1 | 3 | steady | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | sync | d1 | 3 | burst | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | sync | d1 | 3 | reconn | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | sync | d1 | 3 | slowdb | 23,116 (22,990–24,048) | 5 (4–73) | 0 (0–0) | 1,956 (1,907–1,962) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | sync | d1 | 3 | warm_up | 0 (0–0) | 0 (0–0) | 1 (1–4) | 1 (1–1) | 0 (0–1) | 0 (0–0) |
| 400k | 2 | async | d1 +3 blocks | 1 | steady | 0 | 0 | 6 | 6 | 4,901 | 350 |
| 400k | 2 | async | d1 +3 blocks | 1 | burst | 0 | 0 | 0 | 0 | 0 | 0 |
| 400k | 2 | async | d1 +3 blocks | 1 | reconn | 0 | 0 | 0 | 0 | 0 | 0 |
| 400k | 2 | async | d1 +3 blocks | 1 | slowdb | 21,557 | 12 | 0 | 3,862 | 0 | 0 |
| 400k | 1 | async | d1 +dense(15) | 2 | steady | 0 (0–0) | 0 (0–0) | 1 (0–2) | 4.5 (0–9) | 194 (187–201) | 0 (0–0) |
| 400k | 1 | async | d1 +dense(15) | 2 | burst | 41.5 (0–83) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 1 | async | d1 +dense(15) | 2 | reconn | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 400k | 1 | async | d1 +dense(15) | 2 | slowdb | 23,227 (23,187–23,267) | 268 (264–272) | 0 (0–0) | 1,697 (1,692–1,701) | 0 (0–0) | 0 (0–0) |
| 400k | 1 | async | d1 +dense(15) | 2 | dense_cadence | 1,055 (1,035–1,074) | 0 (0–0) | 8 (7–9) | 27.5 (20–35) | 17,030 (16,461–17,599) | 2,899 (2,801–2,997) |
| 400k | 1 | async | d1 +dense(15) | 2 | warm_up | 0 (0–0) | 0 (0–0) | 0 (0–0) | 1.5 (0–3) | 0 (0–0) | 0 (0–0) |
| 400k | 2 | async | d1 +dense(15) | 1 | steady | 0 | 0 | 1 | 0 | 196 | 0 |
| 400k | 2 | async | d1 +dense(15) | 1 | burst | 1,561 | 0 | 0 | 0 | 0 | 0 |
| 400k | 2 | async | d1 +dense(15) | 1 | reconn | 0 | 0 | 0 | 0 | 0 | 0 |
| 400k | 2 | async | d1 +dense(15) | 1 | slowdb | 22,813 | 5 | 0 | 2,404 | 0 | 0 |
| 400k | 2 | async | d1 +dense(15) | 1 | dense_cadence | 1,097 | 0 | 19 | 23 | 21,504 | 1,512 |
| 400k | 2 | async | d1 +dense(15) | 1 | warm_up | 0 | 0 | 0 | 2 | 0 | 0 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | steady | 37,008 | 7 | 845 | 310 | 1,671 | 0 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | burst | 39,297 | 0 | 0 | 0 | 2 | 0 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | reconn | 11,190 | 0 | 0 | 0 | 1 | 0 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | slowdb | 22,306 | 299 | 0 | 1,660 | 0 | 0 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | warm_up | 0 | 0 | 0 | 1,419 | 0 | 0 |
| 500k | 1 | async | d1 | 3 | steady | 0 (0–0) | 0 (0–0) | 1 (0–1) | 0 (0–0) | 193 (189–247) | 0 (0–0) |
| 500k | 1 | async | d1 | 3 | burst | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 500k | 1 | async | d1 | 3 | reconn | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 500k | 1 | async | d1 | 3 | slowdb | 22,920 (22,919–22,964) | 234 (210–245) | 0 (0–0) | 1,726 (1,724–1,750) | 0 (0–0) | 0 (0–0) |
| 500k | 1 | async | d1 | 3 | warm_up | 0 (0–0) | 0 (0–0) | 0 (0–0) | 1 (0–1) | 0 (0–0) | 0 (0–0) |
| 500k | 2 | async | d1 | 3 | steady | 0 (0–0) | 0 (0–0) | 2 (2–2) | 1 (0–1) | 203 (198–212) | 0 (0–0) |
| 500k | 2 | async | d1 | 3 | burst | 1,496 (1,041–1,516) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 500k | 2 | async | d1 | 3 | reconn | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 500k | 2 | async | d1 | 3 | slowdb | 23,375 (23,284–23,396) | 20 (14–72) | 0 (0–0) | 1,933 (1,889–1,953) | 0 (0–0) | 0 (0–0) |
| 500k | 2 | async | d1 | 3 | warm_up | 0 (0–0) | 0 (0–0) | 0 (0–0) | 1 (0–1) | 0 (0–0) | 0 (0–0) |
| 500k | 4 | async | d1 | 3 | steady | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 500k | 4 | async | d1 | 3 | burst | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 500k | 4 | async | d1 | 3 | reconn | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) | 0 (0–0) |
| 500k | 4 | async | d1 | 3 | slowdb | 23,799 (23,740–24,500) | 6 (5–8) | 0 (0–0) | 1,969 (1,968–1,979) | 0 (0–0) | 0 (0–0) |
| 500k | 4 | async | d1 | 3 | warm_up | 0 (0–0) | 0 (0–0) | 7 (4–11) | 2 (2–3) | 6 (3–51) | 0 (0–0) |

#### ORDER_LOCK, SETTLEMENT_LOCK and `pg_stat_statements` lock time

Waiters are sampled from `pg_locks` every 10 ms, so waiter-seconds is a lower
bound (`waiter_seconds_estimate`). The statement columns are `calls` and
`total_exec_time` (ms) of the one normalized advisory-lock statement, which
the MIGRATION, ORDER and SETTLEMENT locks share.

| window | fe | repl | plan | n | steady ORDER_LOCK max / mean waiters · waiter-s ≥ | steady SETTLEMENT_LOCK max / mean · waiter-s ≥ | steady lock stmt calls / total exec ms | burst ORDER_LOCK max / mean · waiter-s ≥ | burst SETTLEMENT_LOCK max / mean · waiter-s ≥ | burst lock stmt calls / total exec ms | reconn ORDER_LOCK max / mean · waiter-s ≥ | reconn SETTLEMENT_LOCK max / mean · waiter-s ≥ | reconn lock stmt calls / total exec ms | slowdb ORDER_LOCK max / mean · waiter-s ≥ | slowdb SETTLEMENT_LOCK max / mean · waiter-s ≥ | slowdb lock stmt calls / total exec ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | 15 / 0.07 · 20.4 | 1 / 0.00074 · 0.2 | n/a / n/a | 15 / 14.41 · 865 | 1 / 0.00037 · 0.022 | n/a / n/a | 15 / 0.53 · 31 | 1 / 0.00037 · 0.022 | n/a / n/a | 15 / 8.43 · 506 | 1 / 0.04 · 2.7 | n/a / n/a |
| 400k | 1 | async | d1 | 3 | 15 (15–15) / 0.08 (0.07–0.09) · 23.5 (23.1–29.2) | 1 (1–1) / 0.00078 (0.00075–0.00086) · 0.2 (0.2–0.3) | n/a / n/a | 15 (15–15) / 14.36 (14.36–14.38) · 862 (862–863) | 1 (1–1) / 0.00037 (0.00018–0.00055) · 0.022 (0.011–0.033) | n/a / n/a | 15 (15–15) / 0.51 (0.49–0.63) · 29.7 (28.9–37.2) | 1 (0–1) / 0.00056 (0–0.0011) · 0.033 (0–0.1) | n/a / n/a | 15 (15–15) / 6.14 (6.14–6.35) · 369 (368–381) | 1 (1–1) / 0.05 (0.05–0.05) · 3 (2.9–3) | n/a / n/a |
| 400k | 2 | async | d1 | 3 | 31 (31–31) / 0.16 (0.12–0.24) · 49.8 (34.9–69.8) | 3 (1–4) / 0.0024 (0.0021–0.0026) · 0.7 (0.6–0.8) | n/a / n/a | 31 (31–31) / 30.33 (30.31–30.35) · 1,820 (1,818–1,821) | 1 (1–2) / 0.0037 (0.0022–0.0039) · 0.2 (0.1–0.2) | n/a / n/a | 31 (31–31) / 1.21 (1.21–1.23) · 71.4 (71.4–72.6) | 2 (1–2) / 0.003 (0.0024–0.0039) · 0.2 (0.1–0.2) | n/a / n/a | 31 (31–31) / 17.81 (17.14–17.94) · 1,070 (1,031–1,078) | 2 (2–3) / 0.2 (0.14–0.3) · 12.2 (8.1–18) | n/a / n/a |
| 400k | 4 | async | d1 | 3 | 63 (62–63) / 0.22 (0.21–0.23) · 64.4 (62.6–70.3) | 5 (4–5) / 0.01 (0.01–0.01) · 2.6 (2.3–2.7) | n/a / n/a | 63 (63–63) / 62.38 (62.32–62.39) · 3,743 (3,739–3,744) | 2 (2–3) / 0.01 (0.01–0.02) · 0.7 (0.6–1.3) | n/a / n/a | 63 (63–63) / 3.1 (2.61–3.1) · 183 (153–183) | 7 (5–8) / 0.02 (0.01–0.04) · 1.2 (0.8–2.4) | n/a / n/a | 63 (63–63) / 33.83 (29.12–36.4) · 2,042 (1,748–2,187) | 7 (6–7) / 1.81 (1.54–1.84) · 108 (92–110) | n/a / n/a |
| 400k | 2 | sync | d1 | 3 | 31 (30–31) / 0.21 (0.15–1.39) · 63 (44–421) | 2 (2–2) / 0.0028 (0.0022–0.0033) · 0.8 (0.6–1) | n/a / n/a | 31 (31–31) / 30.47 (30.44–30.47) · 1,828 (1,826–1,828) | 1 (1–1) / 0.0013 (0.00092–0.0017) · 0.1 (0.1–0.1) | n/a / n/a | 31 (31–31) / 1.52 (1.15–1.7) · 90 (68–101) | 2 (2–2) / 0.0028 (0.0024–0.0039) · 0.2 (0.1–0.2) | n/a / n/a | 31 (31–31) / 17.71 (16.54–17.98) · 1,061 (996–1,081) | 2 (1–3) / 0.23 (0.08–0.26) · 14 (4.8–15.6) | n/a / n/a |
| 400k | 2 | async | d1 +3 blocks | 1 | 31 / 0.17 · 51.4 | 2 / 0.0025 · 0.7 | n/a / n/a | 31 / 30.43 · 1,826 | 1 / 0.0017 · 0.1 | n/a / n/a | 31 / 1.18 · 69.8 | 2 / 0.0022 · 0.1 | n/a / n/a | 31 / 22.18 · 1,331 | 2 / 0.12 · 7.4 | n/a / n/a |
| 400k | 1 | async | d1 +dense(15) | 2 | 15 (15–15) / 0.64 (0.1–1.17) · 202 (33–371) | 1.5 (1–2) / 0.0011 (0.00097–0.0012) · 0.3 (0.3–0.3) | n/a / n/a | 15 (15–15) / 14.35 (14.34–14.36) · 861 (860–862) | 1 (1–1) / 0.00064 (0.00055–0.00074) · 0.038 (0.033–0.044) | n/a / n/a | 15 (15–15) / 0.58 (0.53–0.62) · 34.2 (31.6–36.8) | 1.5 (1–2) / 0.0011 (0.0011–0.0011) · 0.1 (0.1–0.1) | n/a / n/a | 15 (15–15) / 6.15 (6.14–6.16) · 369 (368–370) | 1 (1–1) / 0.05 (0.05–0.05) · 2.9 (2.8–2.9) | n/a / n/a |
| 400k | 2 | async | d1 +dense(15) | 1 | 31 / 0.17 · 51.4 | 2 / 0.0024 · 0.7 | n/a / n/a | 31 / 30.32 · 1,819 | 2 / 0.0028 · 0.2 | n/a / n/a | 31 / 1.35 · 80.2 | 3 / 0.01 · 0.3 | n/a / n/a | 31 / 17.67 · 1,061 | 2 / 0.26 · 15.4 | n/a / n/a |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | 15 / 14.11 · 4,234 | 1 / 0.0012 · 0.4 | n/a / n/a | 15 / 13.68 · 821 | 1 / 0.0045 · 0.3 | n/a / n/a | 15 / 13.57 · 815 | 1 / 0.00057 · 0.033 | n/a / n/a | 15 / 6.5 · 391 | 1 / 0.05 · 2.8 | n/a / n/a |
| 500k | 1 | async | d1 | 3 | 15 (15–15) / 0.08 (0.07–0.09) · 24.6 (21–27.3) | 2 (1–2) / 0.00071 (0.00063–0.00074) · 0.2 (0.2–0.2) | n/a / n/a | 15 (15–15) / 14.38 (14.37–14.38) · 863 (862–863) | 1 (1–1) / 0.00037 (0.00018–0.00037) · 0.022 (0.011–0.022) | n/a / n/a | 15 (15–15) / 0.53 (0.52–0.56) · 31.5 (30.5–33) | 1 (1–1) / 0.00074 (0.00019–0.00093) · 0.044 (0.01–0.1) | n/a / n/a | 15 (15–15) / 6.15 (6.08–6.16) · 369 (365–371) | 1 (1–1) / 0.05 (0.05–0.05) · 2.8 (2.7–2.9) | n/a / n/a |
| 500k | 2 | async | d1 | 3 | 31 (31–31) / 0.15 (0.09–0.19) · 46.6 (27.5–62.2) | 3 (2–3) / 0.0025 (0.0021–0.0039) · 0.7 (0.6–1.2) | n/a / n/a | 31 (31–31) / 30.33 (30.32–30.33) · 1,820 (1,819–1,820) | 2 (1–2) / 0.0031 (0.0029–0.0033) · 0.2 (0.2–0.2) | n/a / n/a | 31 (31–31) / 1.15 (1.13–1.22) · 69.1 (66.8–72.8) | 1 (1–1) / 0.0024 (0.002–0.003) · 0.2 (0.1–0.2) | n/a / n/a | 31 (31–31) / 16.61 (16.11–16.92) · 998 (969–1,018) | 3 (2–3) / 0.24 (0.22–0.28) · 14.4 (13.3–16.4) | n/a / n/a |
| 500k | 4 | async | d1 | 3 | 63 (63–63) / 0.18 (0.16–0.26) · 56.6 (49.9–83.5) | 4 (4–5) / 0.01 (0.01–0.01) · 2.5 (2.2–2.6) | n/a / n/a | 63 (63–63) / 62.35 (62.34–62.39) · 3,741 (3,740–3,744) | 2 (2–3) / 0.02 (0.01–0.02) · 1 (0.7–1.1) | n/a / n/a | 63 (63–63) / 3.69 (3.17–4.84) · 218 (187–287) | 4 (3–5) / 0.02 (0.01–0.02) · 0.9 (0.6–0.9) | n/a / n/a | 63 (63–63) / 29.74 (28.26–33.1) · 1,790 (1,706–1,987) | 7 (7–7) / 1.37 (1.19–1.84) · 82 (72–110) | n/a / n/a |

Why a cell is `n/a`, or what qualifies it:

- `advisory_lock_statement` is null: `database.pg_stat_statements` =
  'unavailable' — 27 runs, including d1-200k-fe1-async-r1 (steady, burst,
  reconn, slowdb); d1-400k-fe1-async-r1 (steady, burst, reconn, slowdb);
  d1-400k-fe1-async-r2 (steady, burst, reconn, slowdb)

#### Frontend CPU, memory and host load

CPU is the mean number of cores over the phase (`processes[].cpu_cores_mean`):
*sum* adds all frontends, *max fe* is the busiest one. RSS is the largest
`rss_kib_max` (a 1 s sampled resident set) over every phase and frontend, in
MiB.

**CPU columns are converted.** macOS CPU unit defect in the harness:
`processes[].cpu_seconds` and `cpu_cores_mean` come from `proc_pid_rusage`
`ri_user_time + ri_system_time` divided by 1e9 as if nanoseconds
(measure.rs:267-271). On Apple Silicon those fields are Mach absolute-time
ticks (`mach_timebase_info` 125/3 on the measurement host), so the reported
cores are understated by 125/3 = 41.67x. Values from Apple-silicon reports
built from `a1937054` (before the harness fix) are multiplied by 125/3 here;
the reports themselves are unchanged, and their as-reported per-frontend
`cpu_cores_mean` spans 0.0003–0.0194 cores.

| window | fe | repl | plan | n | steady CPU cores, reported × 125/3 (sum / max fe) | burst CPU cores, reported × 125/3 (sum / max fe) | reconn CPU cores, reported × 125/3 (sum / max fe) | slowdb CPU cores, reported × 125/3 (sum / max fe) | RSS max MiB (max fe; sampled maximum) | lowest reclaimable-approx MiB (driver vm_stat: free + inactive + speculative pages; not MemAvailable) | load1 before run | load1 during mean / max |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | 0.18 / 0.18 | 0.36 / 0.36 | 0.18 / 0.18 | 0.05 / 0.05 | 650 | 62,338 | 7.53 | 4.98 / 8.38 |
| 400k | 1 | async | d1 | 3 | 0.21 (0.2–0.21) / 0.21 (0.2–0.21) | 0.39 (0.39–0.4) / 0.39 (0.39–0.4) | 0.2 (0.2–0.21) / 0.2 (0.2–0.21) | 0.06 (0.05–0.07) / 0.06 (0.05–0.07) | 928 (885–928) | 62,812 (62,537–66,918) | 3.53 (1.56–6.95) | 3.98 (2.76–4.57) / 4.76 (3.83–6.64) |
| 400k | 2 | async | d1 | 3 | 0.27 (0.27–0.28) / 0.14 (0.13–0.14) | 0.46 (0.46–0.46) / 0.23 (0.23–0.23) | 0.31 (0.3–0.31) / 0.17 (0.17–0.17) | 0.07 (0.07–0.08) / 0.04 (0.04–0.04) | 846 (843–888) | 62,495 (61,580–64,049) | 4.13 (2.91–4.77) | 5.03 (4.31–5.06) / 7.2 (7.04–7.86) |
| 400k | 4 | async | d1 | 3 | 0.36 (0.36–0.37) / 0.09 (0.09–0.09) | 0.41 (0.4–0.43) / 0.11 (0.1–0.13) | 0.39 (0.38–0.41) / 0.12 (0.11–0.12) | 0.09 (0.08–0.1) / 0.03 (0.02–0.03) | 875 (874–890) | 62,354 (60,959–63,017) | 4.27 (3.18–4.32) | 4.87 (4.57–5.61) / 7.03 (6.27–8.31) |
| 400k | 2 | sync | d1 | 3 | 0.25 (0.25–0.26) / 0.13 (0.13–0.13) | 0.33 (0.33–0.34) / 0.17 (0.16–0.17) | 0.3 (0.29–0.31) / 0.17 (0.16–0.17) | 0.07 (0.07–0.08) / 0.04 (0.04–0.04) | 1,176 (1,160–1,296) | 63,642 (63,343–65,361) | 5.62 (2.62–6.57) | 3.92 (3.12–3.92) / 5.34 (4.42–6.1) |
| 400k | 2 | async | d1 +3 blocks | 1 | 0.44 / 0.25 | 0.44 / 0.22 | 0.27 / 0.14 | 0.06 / 0.03 | 3,315 | 61,940 | 3.82 | 4.53 / 6.72 |
| 400k | 1 | async | d1 +dense(15) | 2 | 0.21 (0.2–0.22) / 0.21 (0.2–0.22) | 0.39 (0.38–0.39) / 0.39 (0.38–0.39) | 0.2 (0.2–0.21) / 0.2 (0.2–0.21) | 0.06 (0.06–0.06) / 0.06 (0.06–0.06) | 3,824 (3,504–4,143) | 64,293 (61,789–66,796) | 3.57 (2.8–4.34) | 5.08 (4.25–5.9) / 7.5 (6.25–8.74) |
| 400k | 2 | async | d1 +dense(15) | 1 | 0.28 / 0.14 | 0.46 / 0.23 | 0.3 / 0.17 | 0.07 / 0.05 | 3,442 | 65,862 | 3.97 | 4.95 / 6.56 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | 0.13 / 0.13 | 0.16 / 0.16 | 0.15 / 0.15 | 0.07 / 0.07 | 883 | 65,832 | 4.26 | 4.31 / 5.64 |
| 500k | 1 | async | d1 | 3 | 0.22 (0.22–0.22) / 0.22 (0.22–0.22) | 0.36 (0.36–0.37) / 0.36 (0.36–0.37) | 0.22 (0.22–0.22) / 0.22 (0.22–0.22) | 0.06 (0.06–0.06) / 0.06 (0.06–0.06) | 1,075 (1,072–1,079) | 64,763 (62,600–67,311) | 4.68 (4.13–5.01) | 4.49 (4.49–4.91) / 6.35 (6.22–6.89) |
| 500k | 2 | async | d1 | 3 | 0.29 (0.29–0.3) / 0.15 (0.15–0.15) | 0.48 (0.47–0.48) / 0.24 (0.24–0.24) | 0.28 (0.27–0.29) / 0.14 (0.14–0.15) | 0.07 (0.07–0.08) / 0.04 (0.04–0.05) | 1,037 (1,036–1,156) | 64,014 (63,704–68,106) | 3.97 (2.54–3.98) | 5.32 (5.22–6.03) / 7.58 (7.22–8.65) |
| 500k | 4 | async | d1 | 3 | 0.41 (0.41–0.43) / 0.1 (0.1–0.11) | 0.39 (0.39–0.42) / 0.1 (0.1–0.11) | 0.46 (0.41–0.48) / 0.14 (0.12–0.16) | 0.1 (0.09–0.12) / 0.03 (0.02–0.03) | 1,868 (1,864–1,869) | 63,840 (61,964–64,919) | 4.7 (3.23–7.92) | 5.48 (4.52–5.95) / 8.49 (6–9.57) |

Why a cell is `n/a`, or what qualifies it:

- No kernel peak-RSS (`VmHWM`) column: `processes[].peak_rss_kib` is null in
  every report here (`peak_rss_source`: sampled max; measure.rs:278-304). The
  RSS column is the only resident-set figure these reports carry
- No `MemAvailable` column: `phases[].min_mem_available_kib` is null in every
  report here (the harness reads /proc/meminfo, measure.rs:1192-1202, which
  macOS lacks). The reclaimable-approx column is the driver's own
  approximation and is not comparable with the Linux `MemAvailable` figures in
  earlier tables

#### Reconnects, time to usable work and outcomes

The time-to-usable-work cell is `all_sessions_milliseconds` of the last
external tip (the one no later external tip replaces), and only when every
session had work on it. The outcome columns list per-run values in run order;
they cover the runs in the medians and then, starred, the runs of the same
group that completed with a finding (those are in no median).

| window | fe | repl | plan | n | reconnect phase: completed / failed · reconnect p50 / max ms | tip 103: all 2,000 sessions with work, ms | earlier tips: sessions with usable work before replacement | ACK/commit divergences (#324) | unknown-outcome commits | durability findings | teardown tail (window-ended no-response commits) | mid-run no-response commits | harness-bug rejections | submits outstanding when the stop drain expired | exit codes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | 13 / 0 · 5.2 / 7.8 | 5,582 | 101: 2,000 of 2,000; 102: 2,000 of 2,000 | 0 | 0 | 0 | 0 | 0 | 0 | 17 | 0 |
| 400k | 1 | async | d1 | 3 | 13 (13–13) / 0 (0–0) · 5.8 (5.8–6.5) / 7.2 (7–7.8) | 21,557 (19,895–21,958) | 101: 0 (0–0) of 2,000; 102: 0 (0–0) of 2,000 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 |
| 400k | 2 | async | d1 | 3 | 1,013 (1,013–1,013) / 1,000 (997–1,000) · 2,792 (2,764–2,867) / 2,849 (2,817–2,929) | 35,061 (33,713–36,684) | 101: 0 (0–0) of 2,000; 102: 0 (0–0) of 2,000 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 |
| 400k | 4 | async | d1 | 3 | 513 (513–513) / 495 (491–500) · 2,705 (2,646–2,784) / 2,739 (2,680–2,820) | 3,832 (3,704–7,809) | 101: 0 (0–0) of 2,000; 102: 2,000 (1,500–2,000) of 2,000 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 7 | 0, 0, 0 | 0, 0, 0 | 0, 71, 367 | 0, 0, 0 |
| 400k | 2 | sync | d1 | 3 | 1,013 (1,013–1,013) / 994 (990–995) · 2,775 (2,686–2,935) / 2,828 (2,737–2,997) | 3,294 (3,282–3,296) | 101: 2,000 (2,000–2,000) of 2,000; 102: 2,000 (2,000–2,000) of 2,000 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 233, 0 | 0, 0, 0 |
| 400k | 2 | async | d1 +3 blocks | 1 | 1,013 / 1,000 · 2,791 / 2,854 | 34,379 | 101: 0 of 2,000; 102: 0 of 2,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 400k | 1 | async | d1 +dense(15) | 2 | 13 (13–13) / 0 (0–0) · 5.9 (5.8–6.1) / 50.1 (8.1–92.1) | 21,635 (21,388–21,883) | 101: 0 (0–0) of 2,000; 102: 0 (0–0) of 2,000 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 0, 0 |
| 400k | 2 | async | d1 +dense(15) | 1 | 1,013 / 995 · 2,822 / 2,888 | 35,733 | 101: 0 of 2,000; 102: 0 of 2,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | 13 / 5 · 10,858 / 67,020 | 445,150 | 101: 0 of 2,000; 102: 0 of 2,000 | 0, 0*, 0* | 0, 0*, 0* | 0, 0*, 0* | 0, 0*, 0* | 0, 1*, 2* | 0, 0*, 0* | 0, 1347*, 0* | 0, 5*, 5* |
| 500k | 1 | async | d1 | 3 | 13 (13–13) / 0 (0–0) · 5.6 (5.6–5.8) / 8 (8–431) | 23,842 (22,808–25,945) | 101: 0 (0–0) of 2,000; 102: 0 (0–0) of 2,000 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 |
| 500k | 2 | async | d1 | 3 | 1,013 (1,013–1,013) / 1,000 (997–1,000) · 3,533 (3,430–3,562) / 3,592 (3,487–3,625) | 27,527 (26,732–27,836) | 101: 0 (0–0) of 2,000; 102: 0 (0–0) of 2,000 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 |
| 500k | 4 | async | d1 | 3 | 513 (513–513) / 500 (493–500) · 3,365 (3,358–3,390) / 3,399 (3,394–3,427) | 4,461 (4,401–4,976) | 101: 2,000 (1,500–2,000) of 2,000; 102: 2,000 (2,000–2,000) of 2,000 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 0, 0 | 0, 373, 0 | 0, 0, 0 |

`*` marks a run that completed with a finding (exit 4, 5, 7 or 8). It is in no
median.

#### Per run

Every run with a side report, in start order. A run that is not `ok` is in no
median. A phase that did not complete prints `incomplete` instead of its
partial numbers.

| run id | harness run tag | started (UTC) | exit | status | steady achieved /s · p99 ms | burst achieved /s · p99 ms | reconn achieved /s · p99 ms | slowdb achieved /s · p99 ms | load1 before | load1 during mean | lowest reclaimable-approx MiB (driver vm_stat; not MemAvailable) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| d1-200k-fe1-async-r1 | dae611bb | 2026-09-17T19:29:56 | 0 | ok | 500.0 · 20.5 | 1558.5 · 1886.5 | 500.0 · 127.9 | 3.2 · 30348.2 | 7.53 | 4.98 | 62338 |
| d1-400k-fe1-async-r1 | c5fe5d1e | 2026-09-17T19:40:07 | 0 | ok | 499.4 · 18.5 | 1623.0 · 4025.9 | 500.0 · 779.5 | 1.7 · 29993.0 | 6.95 | 4.57 | 62537 |
| d1-500k-fe1-async-r1 | 375b5df2 | 2026-09-17T19:50:04 | 0 | ok | 499.4 · 13.2 | 1561.4 · 4744.3 | 500.0 · 887.0 | 1.8 · 30023.1 | 5.01 | 4.49 | 62600 |
| d1-400k-fe1-async-wt-r1 | 0256a9e8 | 2026-09-18T15:08:56 | 5 | FINDING (exit 5: ACK/commit divergence); not in medians | 112.6 · 40408.0 | 114.2 · 12133.6 | 134.9 · 56277.0 | 1.5 · 30223.8 | 4.67 | 3.96 | 61102 |
| d1-400k-fe2-async-r1 | 653cee64 | 2026-09-18T15:19:38 | 0 | ok | 499.3 · 37.5 | 1490.2 · 2607.7 | 500.0 · 939.8 | 2.4 · 29861.3 | 4.77 | 5.06 | 61580 |
| d1-400k-fe4-async-r1 | 150f1ca7 | 2026-09-18T15:29:46 | 0 | ok | 500.0 · 15.0 | 1662.7 · 1634.3 | 500.0 · 1101.6 | 3.6 · 28281.8 | 3.18 | 4.87 | 62354 |
| d1-400k-fe2-sync-r1 | e0b7127c | 2026-09-18T15:40:21 | 0 | ok | 500.0 · 32.8 | 1402.8 · 1848.9 | 500.0 · 1354.8 | 2.5 · 29708.4 | 6.57 | 3.92 | 63343 |
| d1-400k-fe1-async-r2 | 0e75a6bd | 2026-09-18T15:50:28 | 0 | ok | 499.3 · 25.1 | 1503.1 · 4397.1 | 500.0 · 1015.5 | 1.7 · 30514.1 | 3.53 | 3.98 | 62812 |
| d1-400k-fe2-async-r2 | 30ec5c4e | 2026-09-18T16:00:24 | 0 | ok | 499.3 · 23.2 | 1445.6 · 2463.8 | 500.0 · 869.6 | 2.6 · 29650.8 | 2.91 | 5.03 | 62495 |
| d1-400k-fe4-async-r2 | a953b44a | 2026-09-18T16:10:33 | 0 | ok | 500.0 · 16.1 | 1423.0 · 1971.9 | 500.0 · 1309.1 | 4.4 · 41425.4 | 4.32 | 5.61 | 60959 |
| d1-400k-fe2-sync-r2 | f229a920 | 2026-09-18T16:21:30 | 0 | ok | 500.0 · 8.8 | 1616.5 · 1666.7 | 500.0 · 966.3 | 2.4 · 29751.3 | 2.62 | 3.12 | 65361 |
| d1-400k-fe1-async-r3 | fce088fe | 2026-09-18T16:32:07 | 0 | ok | 499.4 · 8.6 | 1755.6 · 3357.1 | 500.0 · 705.2 | 1.7 · 30010.0 | 1.56 | 2.76 | 66918 |
| d1-400k-fe2-async-r3 | 5d224baa | 2026-09-18T16:42:06 | 0 | ok | 499.3 · 11.4 | 1521.7 · 3098.8 | 500.0 · 845.5 | 2.4 · 29710.0 | 4.13 | 4.31 | 64049 |
| d1-400k-fe4-async-r3 | 67dd5efc | 2026-09-18T16:52:13 | 0 | ok | 500.0 · 13.2 | 1504.2 · 1881.8 | 500.0 · 1381.3 | 4.4 · 41190.9 | 4.27 | 4.57 | 63017 |
| d1-400k-fe2-sync-r3 | f561b568 | 2026-09-18T17:03:10 | 0 | ok | 500.0 · 2559.8 | 1504.7 · 1812.2 | 500.0 · 1264.9 | 2.2 · 29369.3 | 5.62 | 3.92 | 63642 |
| d1-400k-fe2-async-blocks3-r1 | eaa07995 | 2026-09-18T17:13:34 | 0 | ok | 482.5 · 51.1 | 1600.0 · 1688.4 | 500.0 · 827.7 | 3.8 · 29334.1 | 3.82 | 4.53 | 61940 |
| d1-dense20k-fe1-async-r1 | 1e81170b | 2026-09-18T17:24:04 | 6 | NOT COMPLETED (exit 6); not in medians | 500.0 · 13.9 | 1643.0 · 2077.7 | 500.0 · 146.0 | 3.3 · 30516.7 | 3.95 | 3.95 | 64576 |
| d1-dense400k-fe1-async-r1 | 33bf557e | 2026-09-18T17:34:12 | 0 | ok | 497.0 · 3903.5 | 1454.6 · 5193.5 | 500.0 · 999.2 | 1.7 · 30247.2 | 4.34 | 5.9 | 61789 |
| d1-500k-fe2-async-r1 | e981913b | 2026-09-18T17:48:57 | 0 | ok | 499.3 · 17.4 | 1496.5 · 2179.2 | 500.0 · 867.0 | 2.3 · 29833.6 | 3.98 | 5.32 | 63704 |
| d1-500k-fe4-async-r1 | f5991b2e | 2026-09-18T17:59:20 | 0 | ok | 500.0 · 22.5 | 1468.0 · 1854.3 | 500.0 · 1814.6 | 2.3 · 26762.1 | 4.7 | 5.95 | 61964 |
| d1-500k-fe1-async-r2 | 8fedf497 | 2026-09-18T18:10:11 | 0 | ok | 499.4 · 26.7 | 1481.4 · 4869.6 | 500.0 · 865.0 | 1.8 · 30285.0 | 4.68 | 4.91 | 64763 |
| d1-500k-fe2-async-r2 | 37fd4356 | 2026-09-18T18:20:17 | 0 | ok | 499.3 · 28.1 | 1504.3 · 3083.1 | 500.0 · 885.0 | 2.3 · 29568.7 | 2.54 | 5.22 | 64014 |
| d1-500k-fe4-async-r2 | 5d2c0def | 2026-09-18T18:30:41 | 0 | ok | 500.0 · 13.3 | 1401.4 · 1978.1 | 500.0 · 1512.7 | 4.5 · 40746.6 | 3.23 | 4.52 | 63840 |
| d1-500k-fe1-async-r3 | 623b7af4 | 2026-09-18T18:42:05 | 0 | ok | 499.2 · 13.3 | 1550.0 · 3828.1 | 500.0 · 765.6 | 1.6 · 30029.2 | 4.13 | 4.49 | 67311 |
| d1-500k-fe2-async-r3 | 9a5fd144 | 2026-09-18T18:52:10 | 0 | ok | 499.3 · 8.1 | 1522.9 · 2289.4 | 500.0 · 903.9 | 2.1 · 29926.1 | 3.97 | 6.03 | 68106 |
| d1-500k-fe4-async-r3 | 5668ffea | 2026-09-18T19:02:35 | 0 | ok | 500.0 · 13.0 | 1514.5 · 1879.0 | 500.0 · 1449.2 | 2.4 · 27345.3 | 7.92 | 5.48 | 64919 |
| d1-400k-fe1-async-wt-r2 | 87d986a0 | 2026-09-18T19:13:24 | 0 | ok | 117.3 · 38296.3 | 107.2 · 39016.9 | 122.5 · 36511.8 | 1.8 · 30385.8 | 4.26 | 4.31 | 65832 |
| d1-400k-fe1-async-wt-r3 | d155ab56 | 2026-09-18T19:23:41 | 5 | FINDING (exit 5: ACK/commit divergence); not in medians | 117.9 · 37518.8 | 111.0 · 36759.9 | 133.2 · 32761.5 | 1.9 · 30299.1 | 2.53 | 4.0 | 68032 |
| d1-dense400k-fe1-async-r2 | c494d03d | 2026-09-18T19:35:07 | 0 | ok | 499.3 · 21.9 | 1527.0 · 4378.1 | 500.0 · 774.7 | 1.7 · 30019.4 | 2.8 | 4.25 | 66796 |
| d1-dense400k-fe2-async-r1 | 2c3228ca | 2026-09-18T19:49:05 | 0 | ok | 499.3 · 19.8 | 1466.6 · 2255.1 | 500.0 · 1008.2 | 2.5 · 29620.5 | 3.97 | 4.95 | 65862 |

#### Phases outside the four tabulated ones (per run)

| run id | status | phase | completed | achieved / offered (target) /s | ACK p50 / p99 / max ms | in artifact |
|---|---|---|---|---|---|---|
| d1-200k-fe1-async-r1 | ok | warm_up | true | 499.8 / 500.0 (500) | 1.1 / 378.0 / 644.5 | false |
| d1-400k-fe1-async-r1 | ok | warm_up | true | 499.9 / 500.0 (500) | 1.3 / 4.0 / 25.4 | false |
| d1-500k-fe1-async-r1 | ok | warm_up | true | 499.9 / 500.0 (500) | 1.3 / 4.0 / 15.7 | false |
| d1-400k-fe1-async-wt-r1 | FINDING (exit 5: ACK/commit divergence); not in medians | warm_up | true | 181.6 / 244.9 (500) | 9204.6 / 17245.4 / 17274.3 | false |
| d1-400k-fe2-async-r1 | ok | warm_up | true | 500.0 / 500.0 (500) | 2.5 / 89.6 / 150.5 | false |
| d1-400k-fe4-async-r1 | ok | warm_up | true | 489.3 / 500.0 (500) | 1.4 / 1225.6 / 1269.0 | false |
| d1-400k-fe2-sync-r1 | ok | warm_up | true | 499.9 / 500.0 (500) | 1.2 / 351.6 / 569.3 | false |
| d1-400k-fe1-async-r2 | ok | warm_up | true | 500.0 / 500.0 (500) | 1.3 / 3.1 / 16.4 | false |
| d1-400k-fe2-async-r2 | ok | warm_up | true | 500.0 / 500.0 (500) | 2.5 / 39.1 / 56.3 | false |
| d1-400k-fe4-async-r2 | ok | warm_up | true | 486.0 / 500.0 (500) | 2.0 / 1771.6 / 1796.1 | false |
| d1-400k-fe2-sync-r2 | ok | warm_up | true | 499.9 / 500.0 (500) | 1.2 / 372.3 / 595.9 | false |
| d1-400k-fe1-async-r3 | ok | warm_up | true | 500.0 / 500.0 (500) | 1.1 / 3.3 / 22.5 | false |
| d1-400k-fe2-async-r3 | ok | warm_up | true | 499.9 / 500.0 (500) | 2.2 / 6.4 / 21.0 | false |
| d1-400k-fe4-async-r3 | ok | warm_up | true | 488.3 / 500.0 (500) | 1.5 / 1295.2 / 1331.9 | false |
| d1-400k-fe2-sync-r3 | ok | warm_up | true | 499.8 / 500.0 (500) | 1.3 / 395.2 / 646.6 | false |
| d1-400k-fe2-async-blocks3-r1 | ok | warm_up | true | 500.0 / 500.0 (500) | 2.4 / 8.5 / 17.1 | false |
| d1-dense20k-fe1-async-r1 | NOT COMPLETED (exit 6); not in medians | warm_up | true | 499.8 / 500.0 (500) | 1.2 / 296.1 / 490.2 | false |
| d1-dense400k-fe1-async-r1 | ok | warm_up | true | 500.0 / 500.0 (500) | 1.3 / 3.4 / 20.8 | false |
| d1-dense400k-fe1-async-r1 | ok | dense_cadence | true | 415.2 / 500.0 (500) | 1.1 / 381.0 / 1042.6 | false |
| d1-500k-fe2-async-r1 | ok | warm_up | true | 500.0 / 500.0 (500) | 2.4 / 37.6 / 86.8 | false |
| d1-500k-fe4-async-r1 | ok | warm_up | true | 499.4 / 500.0 (500) | 1.6 / 461.4 / 763.4 | false |
| d1-500k-fe1-async-r2 | ok | warm_up | true | 499.9 / 500.0 (500) | 1.3 / 4.7 / 27.9 | false |
| d1-500k-fe2-async-r2 | ok | warm_up | true | 499.9 / 500.0 (500) | 2.4 / 31.0 / 82.7 | false |
| d1-500k-fe4-async-r2 | ok | warm_up | true | 498.1 / 500.0 (500) | 1.3 / 371.5 / 604.8 | false |
| d1-500k-fe1-async-r3 | ok | warm_up | true | 500.0 / 500.0 (500) | 1.2 / 3.7 / 28.4 | false |
| d1-500k-fe2-async-r3 | ok | warm_up | true | 499.9 / 500.0 (500) | 2.5 / 10.5 / 22.6 | false |
| d1-500k-fe4-async-r3 | ok | warm_up | true | 499.5 / 500.0 (500) | 1.3 / 441.2 / 684.5 | false |
| d1-400k-fe1-async-wt-r2 | ok | warm_up | true | 150.5 / 197.8 (500) | 8109.3 / 16762.1 / 16793.1 | false |
| d1-400k-fe1-async-wt-r3 | FINDING (exit 5: ACK/commit divergence); not in medians | warm_up | true | 148.5 / 197.7 (500) | 13054.0 / 17673.0 / 17704.8 | false |
| d1-dense400k-fe1-async-r2 | ok | warm_up | true | 499.9 / 500.0 (500) | 1.2 / 11.8 / 138.6 | false |
| d1-dense400k-fe1-async-r2 | ok | dense_cadence | true | 409.7 / 500.0 (500) | 1.1 / 391.6 / 811.8 | false |
| d1-dense400k-fe2-async-r1 | ok | warm_up | true | 499.9 / 500.0 (500) | 2.4 / 87.1 / 146.3 | false |
| d1-dense400k-fe2-async-r1 | ok | dense_cadence | true | 399.4 / 500.0 (500) | 1.2 / 352.7 / 669.8 | false |

### Capacity-evidence verdicts (#447)

Validator inputs (`validator.forecast_used` shares/s,
`validator.ack_p99_limit_used_milliseconds`): forecast peak 2000 shares/s, ACK
p99 limit 1000 ms. Runs in the medians only.

| Window | fe | repl | plan | runs | artifact written | valid | slowest artifact phase /s (min–max) | worst artifact ACK p99 ms (min–max) | suggested forecast (min–max) | suggested ACK p99 limit ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 200k | 1 | async | d1 | 1 | 1 | 0 | 3.2 | 30,348 | 1.58 | 15,000 |
| 400k | 1 | async | d1 | 3 | 3 | 0 | 1.7 | 29,993–30,514 | 0.83–0.85 | 15,000 |
| 400k | 2 | async | d1 | 3 | 3 | 0 | 2.4–2.6 | 29,651–29,861 | 1.19–1.32 | 15,000 |
| 400k | 4 | async | d1 | 3 | 3 | 0 | 3.6–4.4 | 28,282–41,425 | 1.78–2.22 | 15,000 |
| 400k | 2 | sync | d1 | 3 | 3 | 0 | 2.2–2.5 | 29,369–29,751 | 1.10–1.23 | 15,000 |
| 400k | 2 | async | d1 +3 blocks | 1 | 1 | 0 | 3.8 | 29,334 | 1.92 | 15,000 |
| 400k | 1 | async | d1 +dense(15) | 2 | 2 | 0 | 1.7 | 30,019–30,247 | 0.82–0.84 | 15,000 |
| 400k | 2 | async | d1 +dense(15) | 1 | 1 | 0 | 2.5 | 29,621 | 1.23 | 15,000 |
| 400k | 1 | async | d1 +wal_sync_method=fsync_writethrough | 1 | 1 | 0 | 1.8 | 38,296 | 0.92 | 15,000 |
| 500k | 1 | async | d1 | 3 | 3 | 0 | 1.6–1.8 | 30,023–30,285 | 0.81–0.89 | 15,000 |
| 500k | 2 | async | d1 | 3 | 3 | 0 | 2.1–2.3 | 29,569–29,926 | 1.06–1.17 | 15,000 |
| 500k | 4 | async | d1 | 3 | 3 | 0 | 2.3–4.5 | 26,762–40,747 | 1.14–2.25 | 15,000 |

First entry of `validator.verdict.error_chain` per refused artifact, with its
counts folded to `N`:

- `capacity run did not acknowledge every offered valid share: offered=N
  acknowledged=N rejected=N` — 27 runs

Runs outside the medians carry no row above. Their `validator` block says:

- `d1-400k-fe1-async-wt-r1`: artifact_written = true, valid = false
- `d1-dense20k-fe1-async-r1`: artifact_written = false, withheld_reason: `the
  run aborted: 20 submit(s) offered before the dense_cadence phase were still
  outstanding 25.0 s later, so its 0 ms delay could not be applied without
  them finishing under it`
- `d1-400k-fe1-async-wt-r3`: artifact_written = true, valid = false

### Runs outside the medians

**Runs that completed with a finding.**

Exit 4, 5, 7 and 8 runs completed every phase but carry a finding, so none of
their numbers is in a median above. They are in the per-run table, flagged.

| run id | window | fe | repl | plan | started (UTC) | exit | meaning | finding, from the report |
|---|---|---|---|---|---|---|---|---|
| d1-400k-fe1-async-wt-r1 | 400k | 1 | async | d1 | 2026-09-18T15:08:56 | 5 | ACK/commit divergence | ACK/commit divergences: 0; unknown-outcome commits: 0; mid-run no-response commits: 1; harness-bug rejections: 0 |
| d1-400k-fe1-async-wt-r3 | 400k | 1 | async | d1 | 2026-09-18T19:23:41 | 5 | ACK/commit divergence | ACK/commit divergences: 0; unknown-outcome commits: 0; mid-run no-response commits: 2; harness-bug rejections: 0 |

**Runs that did not complete.**

These runs did not complete (harness exit 2, 3 or 6, an exit the harness does
not document, a run the driver ended, or a phase with `completed: false`).
None of their numbers is in any median. The reason text is the report's own,
verbatim except that terminal colour codes are removed; `(argv)` marks a
configuration value the reduced report does not carry, read from the driver's
recorded command line instead.

| run id | window | fe | repl | plan | started (UTC) | exit | meaning | ended_by | phases completed | reason, from the report | frontend log refusals (kind × lines) | why this script excluded it |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| d1-dense20k-fe1-async-r1 | 20k | 1 | async | d1 | 2026-09-18T17:24:04 | 6 | aborted | harness | warm_up, steady_state, burst, reconnect, slow_database | aborted: `20 submit(s) offered before the dense_cadence phase were still outstanding 25.0 s later, so its 0 ms delay could not be applied without them finishing under it` | none | harness exit 6 (aborted); report.aborted is set |

### Replication premise (#447)

One short probe ran before the matrix, as in #271: 2 frontends, 200 sessions,
the short plan at 50 shares/s, the 20k window.

| Run id | Exit | Declared | Observed at entry | Observed after load | Agreed | Share difficulty agreed |
|---|---|---|---|---|---|---|
| d1-premise-fe2-async-short50 | 0 | async | async | async | true | true |

The synchronous probe was not repeated. The premise is checked inside every
run, and it agreed in all of them: 34 runs declared `async` and 3 declared
`sync`; each observed its declared mode at entry and again after the load, and
every run's share difficulty agreed.

### Harness observations (#447)

Each of these came out of a run. Only the first is changed here.

1. **Frontend CPU was read in the wrong unit on Apple silicon (fixed).**
   `measure.rs` divided `proc_pid_rusage`'s `ri_user_time + ri_system_time`
   by 1e9 as if they were nanoseconds. They are Mach absolute-time ticks,
   125/3 ns each on this host, so every `cpu_seconds` and `cpu_cores_mean`
   was about 42 times too small. The change that adds this part scales by
   `mach_timebase_info` and adds two tests; one of them burns a known amount
   of CPU and fails against the old arithmetic. Nothing compared the reading
   with CPU really spent before. Linux hosts, including #271's, were never
   affected.
2. **The memory floor is skipped without a word on macOS.** When
   `MemAvailable` cannot be read the 1 s check does nothing, the run proceeds
   unguarded, and the side report's `min_mem_available_kib` is `null`. The
   `null` is honest; the silence is not. A refusal at entry, or a line in the
   report saying the floor did not operate, would be.
3. **`pg_stat_statements` is never loaded on macOS.** The harness preloads it
   when `pg_stat_statements.so` exists in `pkglibdir`; Homebrew ships
   `pg_stat_statements.dylib`. Every lock-statement cell in this part is
   `n/a` for that reason.
4. **The default `TMPDIR` on macOS makes the managed cluster unstartable.**
   PostgreSQL logs `Unix-domain socket path ... is too long (maximum 103
   bytes)` and the harness exits 2 with `pg_ctl failed`, without that line,
   because the cluster root and its log are removed with the failure.
5. **The dense-cadence attribution keys on a message the server no longer
   sends.** See [Dense cadence at D1](#dense-cadence-at-d1).
6. **PostgreSQL warns `there is already a transaction in progress` in every
   D1 run's frontend log.** That is `BEGIN` arriving on a connection that
   already has a transaction open. It appears 10,303 times across 33 of the 33
   D1 runs: 8,431 in `slow_database` and 1,866 in the drain after it, where
   appends are being cancelled at the share-commit deadline (`share commit
   deadline passed before COMMIT was sent`). The other 6 are elsewhere. 5 are
   in `warm_up` of `d1-400k-fe1-async-wt-r1`, `steady_state` of
   `d1-400k-fe1-async-wt-r2`, phases that logged the same cancellations. 1 is
   in `steady_state` of `d1-dense400k-fe1-async-r1`, a phase that logged no
   such cancellation, so the deadline is not the only trigger. That pattern
   suggests a cancelled attempt
   leaving its transaction open for the connection's next user. Every run in
   which it appears reconciled exactly, so no effect on the ledger was
   observed. Its cause and its consequences were not investigated here, and
   nothing in the repository mentions it.

### Reproducing a run (#447)

Build both binaries in release mode from `a1937054`, in a clean checkout that
nothing else modifies. Then run the harness with PostgreSQL 16 server binaries
and a short `TMPDIR`:

```sh
cargo build --locked --release -p qbit-prism-load -p qbit-prism-server
TMPDIR=/tmp/d1t target/release/qbit-prism-load \
  --server-bin target/release/qbit-prism-server \
  --pg-bin-dir <pg-bin> \
  <args> \
  --out <out>
```

Every run carries the same tail, written `<common>` below: `--sessions 2000
--plan d1 --forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000
--min-mem-available-mib 6144`.

| Run ids | `<args>` |
|---|---|
| `d1-premise-fe2-async-short50` | `--frontends 2 --sessions 200 --window-shares 20000 --replication async --plan short --rate 50 --forecast-peak-shares-per-second 2000 --ack-p99-limit-ms 1000 --min-mem-available-mib 6144` |
| `d1-{200,400,500}k-fe1-async-r1` | `--frontends 1 --window-shares {200000,400000,500000} --replication async <common>` |
| `d1-400k-fe{1,2,4}-async-r{1,2,3}`, `d1-500k-fe{1,2,4}-async-r{1,2,3}` | `--frontends {1,2,4} --window-shares {400000,500000} --replication async <common>` |
| `d1-400k-fe2-sync-r{1,2,3}` | `--frontends 2 --window-shares 400000 --replication sync <common>` |
| `d1-400k-fe1-async-wt-r{1,2,3}` | as `d1-400k-fe1-async-r*`, with `--pg-bin-dir` pointing at the wrapper directory described under [The flush control](#the-flush-control) |
| `d1-400k-fe2-async-blocks3-r1` | `--frontends 2 --window-shares 400000 --replication async --scheduled-blocks 3 --keep-artifacts <common>` |
| `d1-dense20k-fe1-async-r1` | `--frontends 1 --window-shares 20000 --replication async --cadence dense --scheduled-blocks 15 <common>` |
| `d1-dense400k-fe{1,2}-async-r*` | `--frontends {1,2} --window-shares 400000 --replication async --cadence dense --scheduled-blocks 15 <common>` |
| `d1-20k-fe1-async-r{1,2,3}` | `--frontends 1 --window-shares 20000 --replication async <common>`, which is #271's `w11-20k-fe1-async-r*` argument list |
| `d1-20k-fe1-async-build5d0042f6-r{1,2,3}` | the same, with both binaries built from `5d0042f6` in its own clean worktree |
| `d1-20k-fe1-async-cross-{newharness-oldserver,oldharness-newserver}-r1` | the same, with `--server-bin` from the other build and `--allow-unverified-server-revision`; both were refused at startup, see [The build control](#the-build-control) |

Runs were driven one at a time, detached from any interactive session, with a
60 s cooldown and a whole-run ceiling of 3,600 s that no run reached. The
longest took 790 s. The exit codes are those in
[Reproducing a run](#reproducing-a-run) above.

### What was not measured (#447)

- **The #291 rehearsal host.** It had not been named. Every rate in this part
  belongs to a laptop whose commits do not force the drive cache, and the
  flush control shows how much that is worth. These runs settle that the
  production window runs, reconciles and lands blocks on this build. They do
  not settle what rate the rehearsal host sustains.
- **Synchronous replication beyond one configuration,** and on a standby that
  does not share the primary's disk. The managed standby is on the same SSD
  and, like the primary, does not force a flush, so the absence of a
  synchronous cost here says little about a deployment.
- **Spread for the found-block run,** which is one run.
- **200k beyond one run at one frontend.** It was a blocked size to clear, not
  a configuration to characterise.
- **A reduced-rate dense run.** It was planned as the fallback if the phase
  could not start at D1. The phase started at D1 at the production window, so
  the fallback was not needed.
- **Which of the server and the harness lowered the 2,000 shares/s ceiling**
  between `5d0042f6` and `a1937054`. Both cross runs were refused, correctly;
  see [The build control](#the-build-control).
- **#271's build at the production windows.** It cannot run them.

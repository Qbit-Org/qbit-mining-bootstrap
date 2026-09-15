# PRISM throughput measurements (#271)

This document records the #271 throughput matrix as measured by the
`qbit-prism-load` harness. The harness drives real `qbit-prism-server`
frontends over real Stratum sockets, with real proof of work, against a
PostgreSQL primary and standby that it manages. Every number below comes from
one host and one build. None of these numbers sets a CI floor. None is a
capacity claim for other hardware.

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
  startup, with the verbatim refusal in [Blocked sizes](#blocked-sizes).
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
  to start the dense phase; see [Dense cadence](#dense-cadence).

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

**#271 stays open for this result.**

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

- **Dense cadence.** The block was dropped after its first run showed the
  dense phase cannot start at D1 on this host.
- **100k at 4 frontends.** The configuration does not fit on this host, so
  no repeat was attempted.
- **100k at 2 frontends, and a third 100k repeat.** Both were cut from the
  plan; the 20k block settles the frontend dimension.

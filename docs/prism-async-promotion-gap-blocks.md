# Blocks on gap-issued work after an asynchronous promotion (#619)

This note investigates [#619](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/619).
Under the [D3 decision](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/260)
(one primary and one asynchronous standby), a promotion can lose the shares
of the last replication gap. A frontend that survives the promotion still holds
work whose payout window names those lost shares. It issues jobs on that work,
accepts a block proof on one of them, enqueues and offers the candidate, and
qbitd puts the block on chain. The landing then fails for good with
`window range incomplete`.

The note covers both modes reported on #619, what is lost and how often, the
money at stake, the options, and a recommendation. It changes no product code.

**Status: decided (d)** (operator, 2026-10-01), and implemented; see
[Decision and implementation](#decision-and-implementation). The analysis
below is the investigation as it stood before the decision.

**Base.** `3.x.x` at `4f4b218d08b9f64a2ac0cc29ce97caf59cb50fb6`. Every
`path:line` citation is at that commit, relative to
`crates/qbit-prism-server/src/` unless it starts with `crates/` or `docs/`.

**Evidence labels.** **Verified by running** means a command was run on
minidev-alex1 and its output is quoted below. **Read from code** means the
claim comes from reading the source at the base commit and was not executed.
Estimates are labelled as estimates.

## Summary

- **Both modes have one root cause.** Every check that admits work, issues a
  job or enqueues a candidate compares versions that a promotion does not
  change: the payout revision, the prior-balances digest, the template, the tip
  and the readiness generation. The only share-history checks are
  `probe_share_rows` probes, which test whether a `share_seq` *exists*. A
  promotion can lose rows, and PostgreSQL's sequence then hands the lost
  numbers out again to other shares. So a window captured on the old primary
  stays authority on the promoted one. #466's timeline witness protects the
  refresh's delta read (`ledger/window/snapshot_delta.rs:153-196`), but not
  work that is already prepared, published or held.
- **Mode 2 is an authority bug, not a landing-predicate bug, and needs no
  decision.** The landing predicate is right: it refuses to count reissued
  rows as the window. What is wrong is upstream:
  - the refresh keeps pre-promotion work published (`coordinator.rs:1165-1199`);
  - the prepared-record repair re-creates that work on the promoted primary
    after checking only that its endpoint `share_seq` values exist
    (`ledger/jobs/compact_issued.rs:194-209`).
- **Both modes reproduce deterministically on this host without qbitd.** The
  new `tests/b619_gap_work_after_promotion.rs` uses a real PostgreSQL 16 async
  pair, a native `Coordinator` and the fake node. Both tests ended with
  `window range incomplete: expected 15 shares, read 0` after the node
  accepted the block, in 15 of 15 runs each. The promoted sequence resumed at
  `share_seq` 34 every time, the same 34–46 reuse #474 B reported.
- **Money at stake per affected block.** Today the block is on chain and is
  never landed.
  - With direct payouts, the ledger never records its coinbase. The positive
    carry-forward balances that coinbase paid out are paid again by later
    landings, at most the as-issued float `F`. The block's accruals are never
    credited.
  - With CTV settlement on (the mainnet guide's setting), the covenant outputs
    commit to fanout transactions that can only be rebuilt from the lost window.
    There is no pool spend key, so those outputs stay unspendable until the
    window is recovered from the old primary's storage.
- **Recommendation: option (d).** Revalidate the window inside the
  transaction that uses it. This is (a)'s refusal, scoped by an exact
  durability test rather than (c)'s blunt epoch fence.
  - Refuse a block proof `stale-job`, before its enqueue, when the current
    primary does not hold the window's last row as the window's own row.
  - Stop issuing work prepared on another writer timeline.
  - Make the repair check identity, not existence.
  - No migration is needed. Do not take (b): the window is gone, so a landing
    cannot be audited or settled without inventing numbers, and in CTV mode
    it cannot unlock the covenant outputs anyway.

## Background

- **D3** ([#260](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/260),
  decided 2026-09-11) sets `synchronous_standby_names=''`, so share ACKs never
  wait for the standby. The accepted loss is the shares in the replication-lag
  window. The decision comment puts that lag at tens of milliseconds, about 0
  to 1 share at today's rates and about 200 at the historic 2,000/s peak. It
  equals the standby's downtime if the standby was out.
- **#529, #570 and #585** cover blocks found *in* the gap. #570 waits, within a
  bound, for the standby to flush a found block's offer reservation before
  `submitblock`, so a block offered from the old primary survives the
  promotion with its candidate. #619 is a different case: the candidate is
  enqueued and offered *on the promoted primary*, after the promotion, so its
  own rows are never at risk. What the promoted primary lacks is the block's
  *window*.
- **#466** repaired the refresh's retained-window delta read across replaced
  history. `leaf_witness` reads the insertion timeline,
  `left(pg_walfile_name(pg_current_wal_lsn()),8)`, in the same statement that
  counts the range (`ledger/window/snapshot_delta.rs:157-196`). A changed
  timeline sends the refresh to the full scan
  (`ledger/window/snapshot_delta.rs:479-512`). This protects *new* work only.
- **#474 slice B (PR #620)** found #619 with live miners and a real qbitd. Its
  gated drill (`live_regtest … async_live_miners`) now submits the block on
  the gap-issued job after the promotion and requires it to be refused as
  stale or to pay only promoted rows; it needs qbitd.

## How the defect happens

### Why lost sequence numbers come back (read from code, verified by running)

- `share_seq` comes from the sequence `qbit_share_ledger_share_seq_seq`
  (`crates/qbit-prism-server/migrations/016_share_ledger_partition_catalog.sql:487`). The INSERT at
  `ledger/window.rs:855` takes its default.
- PostgreSQL WAL-logs a sequence 32 values ahead. A standby that replayed up
  to the last logged value resumes, after promotion, at most 33 past the last
  share it received. The old primary may have used numbers beyond that point.
  The promoted primary then hands them out again to other shares.
- Every native ledger writer appends under `ORDER_LOCK`: the share append
  (`ledger/window.rs:684-727`) and the deferred-share credit of settlement and
  reconciliation (`ledger/blocks.rs:295`, `:514`, then `:677`). So
  `share_seq` order is commit order. A physical standby replays a prefix of
  the WAL, so the promoted ledger holds exactly the rows up to some
  `share_seq`, and every later number is either absent or a *different*
  share.
- **Verified by running.** The standby received shares 1..=20 and 80 more were
  credited in the gap (21..=100). The promoted primary's first new share was
  `share_seq` 34 in all 15 runs, and the timeline moved from `00000001` to
  `00000002` in each of the 9 runs that recorded it.
- **Clock.** The ledger clock (`qbit_prism_cluster.ledger_clock_ms`) is
  replicated. A reissued row's `accepted_at` is
  `GREATEST(replicated clock, promoted host's clock)` (`ledger/window.rs:850`).
  A window anchored in the gap has `anchor_ms` above every row the old primary
  had credited by then (`ledger/window.rs:903`). Reissued rows therefore land
  *after* the anchor, unless the promoted host's clock runs behind the old
  primary's by more than the time since the anchor.

### The checks that admit gap work (read from code)

| Where | What it checks | What it misses |
| --- | --- | --- |
| Refresh, same template (`coordinator.rs:1165-1199`) | template fingerprint, fee, payout revision, balances digest, cached window inside `PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS` (default 60 s, `config.rs:600`), and `current.bundle.is_some() \|\| share_seq unchanged` | The accepted cutoff is ignored whenever a bundle exists. The writer timeline is not read. Pre-promotion work stays published for up to the reanchor interval. |
| Refresh reuse of the cached window (`coordinator.rs:1240-1255`, `coordinator/refresh_window.rs:55-68`) | the same inputs plus accepted cutoff equality | No timeline. Reuse needs the cutoffs to coincide, so it is unlikely, not impossible. |
| Issuance authority (`coordinator/tip_observation.rs:205-222`, `:557-604`, `:641-746`) | the prepared identity, readiness generation, tip stamp and `identity.revision == revision` (`:734`) | Nothing about the share ledger. |
| `build_job` (`coordinator.rs:2898-2955`) | issues from the in-memory published `prepared` (`:2910`) | — |
| Issued-job save and repair (`coordinator/prepared_storage.rs:21-121`; `ledger/jobs/compact_issued.rs:194-209`) | A lost prepared record returns `PreparedMissing`. The repair re-inserts it from memory after `probe_share_rows(first, last)`. | The probe only checks that both `share_seq` values exist (`ledger/window.rs:1143-1157`). Reissued endpoints pass. |
| `save_compact_prepared` (`ledger/jobs/prepared.rs:224-237`) | the same endpoint existence probe | The same. Its own comment says it is "no substitute for read_window's count/digest checks". |
| Stratum submit (`stratum.rs:1528-1533`) | finds the job in the session's in-memory list | Never reads the job row, which was lost with the gap. |
| `submit_share` (`coordinator/miner_submit.rs:310-470`) | fee, tip, parent (`:368`), payout revision (`:417-439`), readiness | Nothing about the window's rows. The share's `job_issued_at` is the gap anchor (`:469`). |
| Candidate enqueue (`ledger/candidates.rs:651-667`) | `probe_share_rows(first, first)`. If missing, an ALERT log, then **enqueue anyway** "so the block reaches the node" | A reissued first row passes silently. A lost one is offered anyway. |
| Landing (`coordinator.rs:2101-2143` → `ledger/window.rs:268-344`) | Probes both endpoints (`:319`), then reads pages under `accepted AND share_seq … AND accepted_at<=anchor AND job_issued_at<=anchor` (`:1238-1240`), then count and digest (`:1428-1435`). | This is correct, and it is the first check that notices. The miss is reported as `Incomplete`, with `got: 0` synthesized when an endpoint is missing (`:319-325`). `classify_window_error` (`coordinator.rs:443-449`) then keeps the candidate in `reconciliation`. |

### Mode 1: a job issued in the gap, held by a surviving session

1. The frontend prepares work over gap shares. In the test, a template change
   makes the refresh rebuild. The old primary saves the prepared record.
2. A session is issued a job on that work. Its row is saved on the old primary
   only.
3. The promotion loses the gap: the shares, the prepared record and the job
   row.
4. The session submits a block proof on the job. The job is found in session
   memory, and `submit_share` checks pass: same parent, same revision. The
   share append and the candidate enqueue commit on the promoted primary. The
   enqueue's first-row probe fails and logs its ALERT, then enqueues anyway.
5. The offer reaches the node, which accepts it. The landing's endpoint probe
   fails, so it reports `expected 15 shares, read 0`, and the candidate stays
   in `reconciliation` with `offer_outcome='accepted'`.

This happens even after the frontend has published fresh, correct work from
the promoted ledger. The held job stays submittable while its parent is the
tip and the revision is unchanged. Same-tip retention keeps retired jobs for
`PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS` (30 s by default).

### Mode 2: gap work issued to a new session after the promotion

1. As in mode 1, the frontend holds gap work as its published work.
2. After the promotion, the promoted primary credits new shares that reuse
   the gap window's numbers, 86..=100 in the test.
3. The next refresh sees the same template, revision and balances inside the
   60 s reanchor interval, so it keeps the gap work published
   (`coordinator.rs:1165-1199`).
4. A brand-new session is issued a job on it. `persist_issued_job` finds the
   prepared record missing. The repair's endpoint probe passes on the
   reissued rows 86 and 100, so the repair writes the stale prepared record,
   and then the job, onto the promoted primary.
5. A block on that job is accepted, enqueued (the first-row probe passes
   silently) and offered. The landing's endpoint probe passes. The page read
   returns 0 rows, because all 15 rows were credited after the window's
   anchor. The result is `expected 15 shares, read 0`.

This is exactly #619's second comment ("15 rows present, yet the landing read
0"). The repair step makes it worse: the promoted primary now durably holds a
prepared record that another frontend can resume from.

### Why durable records are not the problem (read from code)

A prepared record or job row that *did* replicate is consistent with the
promoted ledger:

- its window's rows committed before the snapshot read them;
- the record was written after that read;
- WAL order and the prefix property then put those rows on the standby too.

**Stale authority lives only in frontend memory:** published prepared work,
the cached window and session-held jobs. It also lives in the repair path,
which writes that memory back into the database. A fix therefore does not
have to touch stored formats.

## Reproduction

`crates/qbit-prism-server/tests/b619_gap_work_after_promotion.rs` held two
`#[ignore]`d tests, each naming #619, before the fix. They now assert the
fixed behaviour and are gated; the output below is the evidence from before
the fix.

**The fixture:**

- PostgreSQL 16 primary with one async standby on a physical slot, reached
  through a replication relay the test can sever;
- a stable writer relay;
- one native `Coordinator` built by `support/fake_qbitd.rs`'s
  `coordinator_config`, so the reanchor interval stays at its 60 s default;
- the in-process fake node, which accepts `submitblock` once
  `accept_blocks()` is called.

**The drill:**

1. Credit 20 shares, then wait for the standby to replay them.
2. Sever the replication link and credit 80 more shares.
3. Move the template, so the refresh prepares the gap work, window 86..=100.
4. Issue one job on it.
5. Fence the writers, stop the old primary, `pg_promote(true,60)`, and route
   the writer relay to the promoted standby.

Each test asserts the outcome #619 needs, whichever option is chosen:

- Mode 1: the block is either refused (`stale-job` or `unknown-job` with no
  outbox row) or landed (`submitted` plus a `qbit_pool_blocks` row).
- Mode 2: a new session must be issued a job whose window reads whole on the
  promoted ledger. A refusal is retried, with a refresh between, up to five
  times, so a fix may refuse the gap work but may not starve the session.
  The block on the issued job must then be refused or landed as in mode 1.

**Verified by running** on minidev-alex1:

```
PRISM_TEST_PG_BIN_DIR=/usr/lib/postgresql/16/bin PRISM_TEST_REQUIRE_INTEGRATION=1 \
  cargo test --locked -p qbit-prism-server --test b619_gap_work_after_promotion \
  -- --ignored --nocapture --test-threads=1
```

The command was run 15 times across three revisions of the test: 6 runs
before the timeline and lost-record evidence was added, 6 after, and 3 on the
committed test, whose mode 2 issuance check was tightened in review. Every
run failed both tests at the #619 assertion, in 3.6–6.5 s per run. Each run
of the committed test printed:

```
#619 drill: 20 shares replicated, 80 credited in the gap (150280 WAL bytes unreplicated); gap work window 86..=100 (15 shares, anchor …); its prepared record and the held job's row are gone; timeline 00000001 -> 00000002
#619 mode 1: replicated through share_seq 20; gap 21..=100 lost; post-promotion credits reused share_seq Some(34)..=Some(46); held job's window 86..=100 (15 shares, …); fresh work's window 19..=46 (15 shares, …); block …: miner answered Ok(()); outbox Some(("reconciliation", Some("accepted"), Some("landing failed after the offer (node accepted the offer): window range incomplete: expected 15 shares, read 0; rows pruned or missing, or a different predicate (#268 owns recovery)"))); claim processing Some(Ok(())); qbit_pool_blocks row: false
#619 mode 2: replicated through share_seq 20; gap 21..=100 lost; post-promotion credits reused share_seq Some(34)..=Some(100); after the promotion the refresh kept the gap work published: true; issuance refusals before a job: []; new session's job … on window 86..=100 (15 shares, …) (issued job and repaired prepared record saved on the promoted primary: true); read on the promoted ledger: Err(Incomplete { expected: 15, got: 0 }); block …: miner answered Ok(()); outbox Some(("reconciliation", Some("accepted"), Some("landing failed after the offer (node accepted the offer): window range incomplete: expected 15 shares, read 0; …"))); …; qbit_pool_blocks row: false
test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.15s
```

Before the failing assertion, mode 2 also checks that each of the 15 rows now
at 86..=100 is a different share, credited after the gap work's anchor. Mode
1 also checks that the fresh work the frontend prepared after the promotion
reads whole, so #466 covers new work.

**Not run on this host:**

- #620's live drill (`live_regtest … async_live_miners`): there is no qbitd
  here.
- Any CTV-settlement landing: the fixture runs with `ctv_enabled: false`.
- Load.

The Stratum socket layer is not exercised either: the tests call the same
`MiningBackend` methods a session calls. The in-memory job lookup is read from
code (`stratum.rs:1528-1533`). The enqueue's ALERT is also read from code: the
test installs no log subscriber. The run shows the ledger held no
`share_seq` 86 at enqueue in mode 1, which is the condition of that branch.

## What is lost, when, and how often

**What.** A block found after the promotion on work whose window includes
shares the promoted primary did not receive. The block is valid and goes on
chain, but the pool can never land it.

**When.** It needs both of these:

1. Some frontend had prepared work after the standby's last received WAL
   position, and that work is still issuable after the promotion. That means
   jobs held by sessions until the parent changes, plus 30 s of same-tip
   retention; or published work kept up to the 60 s reanchor interval while
   the template does not change.
2. A block is found on that work before it retires.

**How often (estimate, not measured).** Per promotion:

```
P ≈ P(gap work exists) × p × E / T
```

- `p` is the pool's share of network hashrate.
- `T` is the block interval.
- `E` is the time gap work keeps being hashed after the promotion. Miners
  switch to the newest job, so `E` ends at the next refresh that publishes
  other work: up to the 60 s reanchor while the template is stable, sooner
  when the template changes. The table takes the upper range.

The illustrative inputs are the ones #504 used: `p` = 30% and `T` = 60 s.

| Standby state at the loss | P(gap work exists) | E | P per promotion |
| --- | --- | --- | --- |
| Streaming, lag `L` ≈ 10–50 ms, refresh every `R` ≈ 1–30 s | ≈ `L/R`: 0.03%–5% | 30–60 s (upper range) | ≈ 0.005%–1.5% |
| Lagging past the 5 s alert, or disconnected (the gap is its downtime) | ≈ 1 | 30–60 s (upper range) | ≈ 15%–30% |

So a healthy failover rarely loses a block this way. A failover after the
standby fell behind loses one in roughly every 3 to 7. #291's drill will
measure the real lag, refresh cadence and promotion time on production
hardware, and this estimate should be recomputed from them.

**Under D3 the accepted loss grows.** Today it is: acknowledged shares in the
last gap, plus blocks found in the gap that were never offered or whose offer
logged an unconfirmed standby wait. It should also name blocks found **after**
the promotion on work whose window lay in the gap.

## Money at stake

**Whose shares the coinbase pays.** The coinbase pays the window the old
primary had: the gap shares, acknowledged to their miners and lost with the
gap. It also pays the prior balances as issued (`ledger/candidates.rs:634-650`
writes that snapshot on the promoted primary at enqueue).

**What the promoted ledger holds.**

- Mode 1: none of the window's rows.
- Mode 2: 15 *other* shares at the same numbers. They were credited after the
  promotion and are paid by later windows.

Either way the landing correctly refuses to treat them as this block's window.

**Today: offered, on chain, never landed.** There is no `qbit_pool_blocks`
row, no audit bundle and no carry-forward settlement. The candidate sits in
`reconciliation` and is never offered again.

- **Direct payouts** (`PRISM_CTV_SETTLEMENT_ENABLED=0`). Miners hold their
  coinbase amounts on chain. The ledger never learns of the block:
  - **Overpay.** Positive as-issued balances that this coinbase paid out are
    still owed on the promoted ledger, and later landings pay them again. Per
    account, a coinbase pays at most `max(0, issued(m))` above its gross
    (`ledger/divergence.rs:1-21`). So the unrecorded double payment is at most
    `F`, the positive as-issued float. The ledger-ops guide sizes `F` at about
    2.9·10⁷ sats at 2,000 sub-floor miners. Later blocks' miners fund it,
    because the coinbase allocates by candidate balance. Unlike a #478
    capture, no debt is recorded.
  - **Underpay.** The block's accruals are never credited. Miners below the
    payout floor lose the gross this block owed them.
  - **No audit.** The coinbase's distribution cannot be verified by miners
    and appears in no pool record. HA promotion step 6 already treats a block
    on chain with no candidate as an explicit accounting-loss
    reconciliation (`docs/prism-ha-reference-architecture.md:480-497`), but
    it has no entry for a block that has a candidate yet cannot land.
- **CTV settlement on** (`docs/mainnet-deployment.md:460`). The settlement
  routes these recipients through covenant outputs rather than direct coinbase
  outputs (`crates/qbit-prism/src/settlement.rs:177-181`):
  - recipients below `ctv_direct_floor`;
  - recipients beyond the direct-output budget.

  Those outputs commit to fanout transactions, and "there is **no pool spend
  key**" (`crates/qbit-prism/src/ctv.rs:1-12`). Fanout artifacts are persisted only
  at landing, from the rebuilt window (`ledger/blocks.rs:246`, `:927-951`).
  The job's bundle keeps only "presence of the original CTV fanout set,
  without retaining its outputs" (`coordinator/compact_runtime.rs:27-36`). So
  without the lost window nobody can build the fanout, and the covenant
  amount stays unspendable.

  The window can still be recovered from the fenced old primary's storage,
  if it survives (the window's rows on the old timeline), or from a frontend
  whose cached refresh window still holds it. Otherwise that part of the
  block's value is lost for good.
- **If the block is refused before its offer** (options a, c and d), the pool
  loses that block's coinbase value: the block reward the pool would have
  mined. Nothing reaches the chain and the ledger stays exact.

Read from code, not run: the CTV path and the divergence bound. The fixture
ran with direct payouts and checked only that no `qbit_pool_blocks` row was
written.

## Options

Each option is judged on six points:

- **EP-STATE:** a stale result must not win. Revalidate the authorizing
  version in the transaction that publishes.
- **Miners:** what the miner sees.
- **Chain:** the consensus and coinbase effect.
- **Migration:** whether a schema change is needed.
- **Mode 2:** whether the option fixes it.
- **Tests:** the test plan.

### (a) Refuse a block proof whose window is not durable on the current primary

The check goes in the candidate enqueue, which runs inside the share-append
transaction under `ORDER_LOCK` (`ledger/window.rs:684-727`) or in
`enqueue_candidate_observed` for block-only proofs. It must refuse, not log
and enqueue anyway as `ledger/candidates.rs:658-666` does today.

- **EP-STATE.** Correct if the test is exact. It runs on the primary that
  will hold the candidate, in the writing transaction. A promotion between
  `submit_share`'s checks and the enqueue is therefore still caught: the
  stale proof cannot win.
- **Miners.** `stale-job` for that proof, and no credit for its share. Plain
  shares on the same job can still be credited, because their value depends
  on future windows, not this one.
- **Chain.** The block is never submitted, so the pool loses one coinbase.
  The block is chain-valid but stays unpublished.
- **Migration.** None. `WindowRef` already carries `anchor_ms` and
  `last_share_seq`.
- **Mode 2.** Not fixed on its own: a bad job is still issued, and only its
  block is refused.
- **Tests.**
  - Mode 1 of `b619_gap_work_after_promotion` flips to "refused before
    enqueue".
  - An out-of-order test: a proof that passed `submit_share` before the
    promotion and enqueues after it is refused.
  - A pruned-prefix case keeps today's "enqueue anyway" (see (d)).

### (b) Land paying only rows the promoted primary holds and record the divergence

- **Feasibility.** The coinbase is fixed. A landing would have to record the
  block from its on-chain outputs and the as-issued balances, and treat the
  window as whatever the promoted ledger holds: nothing, in both modes.
  - Per-account gross cannot be recovered. On-chain amounts are a weighted
    allocation of the reward by candidate balance
    (`crates/qbit-prism/src/lib.rs:1405-1577`), and sub-floor accounts have none.
  - So the settlement would have to invent a rule. One rule debits paid
    accounts by their whole on-chain amount, which claws back acknowledged
    work from miners. Another only zeroes the floats this coinbase paid, so
    the block's accruals are dropped.
  - The canonical audit cannot be produced at all.
- **EP-STATE.** It deliberately publishes a result whose authorizing state is
  gone. The divergence would have to be recorded atomically with the
  landing, as #478 does.
- **Miners.** The proof is accepted and the coinbase pays them. The block
  shows as divergent and unauditable.
- **Chain.** Unchanged.
- **CTV.** It does not help. The covenant outputs still cannot be spent
  without the window.
- **Migration.** Yes, migration 021: a new landing kind and divergence
  records, changes to the public audit API (a block with no verifiable
  window), and integrity-report changes. EP-COMPAT applies to all of them.
- **Mode 2.** Not fixed.
- **Tests.** A divergent landing with exact records. Integrity reports stay
  explainable. CTV must be shown not to regress.

**#478 is not a precedent.** #478 chose capture because the as-issued window
*was* in the ledger, so the block could land canonically with a bounded,
recorded debt. Here the window is gone.

### (c) Fence work prepared under an earlier writer term

The term is the PostgreSQL timeline, which #466 already reads. The work
records it at capture, as `LeafWitness` does: runtime only, never persisted.
Anything that issues, resumes or enqueues refuses work whose term differs
from the primary's current one.

- **EP-STATE.** Correct only if every comparison runs in the transaction that
  writes, for example the enqueue. A frontend-cached "current timeline" can
  be stale.
- **False refusals.** It also refuses work prepared before the cut whose
  window *did* replicate, which is most pre-promotion work. A planned,
  fenced switchover with full replay changes the timeline too, so its blocks
  for one job lifetime would be refused for no loss.
- **Miners.** `stale-job` on every proof on pre-promotion work. Sessions get
  new work at the next refresh.
- **Chain.** As (a), plus false refusals.
- **Migration.** None in memory. Stored issued and prepared records need no
  term, because a record that replicated is consistent with the promoted
  ledger.
- **Mode 2.** Fixed, if the refresh reuse tests and issuance compare the
  term.
- **Tests.**
  - Both b619 tests flip.
  - A fenced switchover counts the refusals it causes.
  - A resumed-job test shows durable records are still honoured.

### (d) Revalidate the window where it is used (recommended)

This combines (a)'s outcome with (c)'s scoping, through one exact and cheap
test.

**The durability test.** `share_seq` order is commit order, and the promoted
ledger is a commit-order prefix. So a window is held by the current primary
**if and only if** the primary holds the window's *last* row and that row is
the window's own. The first-row probe stays as the retention test: retention
removes only prefixes, and a missing last row is never retention. The test
can be made:

1. **Without new data:** the last row must exist and satisfy the window's own
   predicate, `accepted AND accepted_at<=anchor AND job_issued_at<=anchor`.
   This is the predicate #466's `leaf_witness` already applies to an endpoint
   (`ledger/window/snapshot_delta.rs:171-175`). It is one primary-key lookup,
   pruned to one leaf. It cannot be fooled unless the promoted host's clock
   runs behind the old primary's by more than the time from the anchor to the
   reissue, which is seconds. The landing's count and digest stay as the
   final proof.
2. **Exactly, still with no migration:** keep the last row's `share_id` in
   the runtime `Prepared`, as it is captured from the snapshot, and pass it to
   the enqueue. Stored records, which are self-consistent, fall back to (1).

**Where the test applies:**

- **Candidate enqueue** (`ledger/candidates.rs:651-667`). Refuse a candidate
  whose last row fails (1) or (2), returning `stale-job` with a new
  `StaleJobCause` and a WARN naming the block. Keep "enqueue anyway" only
  for the pruned-prefix case it was written for.
- **Repair and prepared save** (`ledger/jobs/compact_issued.rs:199-208`,
  `ledger/jobs/prepared.rs:224-237`). Replace the existence probe on
  `last_share_seq` with the same test, so a stale record is never written
  back.
- **Refresh reuse and issuance.** Carry the timeline captured with the window
  in `Prepared` and `RefreshWindow`. Treat a timeline change as a reason to
  rebuild in the same-template branch (`coordinator.rs:1165-1199`) and in
  `RefreshWindow::reusable`. Have the issuance authority re-run the test when
  the timeline differs.

**Judged on the six points:**

- **EP-STATE.** The authorizing state, the window's rows, is revalidated in
  the transaction that writes the candidate, the prepared record or the
  job. The timeline only decides when work is retired early, so a stale
  cached timeline cannot let a stale result win.
- **Miners.** `stale-job` only for proofs on windows the primary really lost.
  Pre-promotion work whose window replicated keeps working. New sessions get
  fresh work.
- **Chain.** As (a), but only for genuinely lost windows.
- **Migration.** None.
- **Mode 2.** Fixed.
- **Tests.**
  - Both b619 tests flip: mode 1 is refused before enqueue; in mode 2 the
    refresh rebuilds and the new session's window reads whole.
  - A fenced switchover with full replay: a held pre-switch job's block still
    lands.
  - The out-of-order test from (a).
  - A unit test of the probe against a reissued row, a missing row and the
    original row.
  - An EP-ERRORS case: the probe's database error stays a definite pre-COMMIT
    failure, via `enqueue_failed_before_commit`, and is never answered as
    `stale-job`.

### (e) Optional: publish only standby-durable work

Before publishing new same-tip work, wait within a bound for the dedicated
standby's `flush_lsn` to reach the snapshot's WAL position. This reuses #570's
wait (#529's prototype measured p99 2.5–3.9 ms with a healthy standby), off
the share-ACK path.

With a healthy standby, no issued window can then lie in a gap, which removes
the healthy-failover row of the frequency table. A lagging or absent standby
still produces gap work, so (e) supplements (d) and does not replace it. New
tips must never wait for it.

## Recommendation

1. **Fix mode 2 now, without a decision.** It is an authority bug.
   - The refresh must rebuild across a writer-timeline change.
   - Issuance must not hand out work prepared on another timeline unless the
     window passes the durability test.
   - The repair and `save_compact_prepared` must check identity, not
     existence.
2. **Decide mode 1 as (d): refuse before the enqueue.** Refuse a block proof
   `stale-job` when its window's last row is not the window's own on the
   current primary, using the test in the enqueue transaction.
   - The block is lost to the pool, but nothing unlandable reaches the chain.
     The ledger stays exact, and in CTV mode no covenant output is stranded.
   - The test refuses only genuinely lost windows, so planned switchovers and
     replicated pre-promotion work are unaffected.
   - It needs no migration, and it fits #474 B's contract: "resume or
     truthfully refuse original work under its existing authority".
   - (b) is rejected: it cannot be computed or audited without the window,
     and it strands CTV outputs anyway.
3. **Amend the accepted D3 loss** in `docs/prism-ha-reference-architecture.md`
   and #291. Add: "blocks found after a promotion on work whose window lay in
   the replication gap, refused `stale-job`".
4. **Add a runbook entry** for blocks that already slipped through: a
   candidate in `reconciliation` with `window range incomplete` after a
   promotion.
   - Classify it as an accounting-loss reconciliation, as step 6 does.
   - In CTV mode, recover the window rows from the fenced old primary to
     rebuild the fanout before that storage is reused.
   - Consider (e) once #291 measures the healthy-failover exposure.

The trade-off for the decision owner: (d) gives up, rarely, a block that is
valid on chain and that today pays its miners (direct mode). In return it
keeps the books and the covenant outputs sound.

If production ran direct payouts only, a #478-style divergent capture could be
argued instead. It would still need a landing that is not canonical, a
migration and an accounting rule chosen without the window.

## Other defects found

- **Misattributed landing error (EP-OBSERVABILITY).** After a promotion,
  `classify_window_error` (`coordinator.rs:443-449`) says "rows pruned or
  missing, or a different predicate", and a digest mismatch says
  "corruption". Neither names replaced or lost history, which is what
  happened. *Fixed with (d):* a failed landing read probes the window first
  and, when this primary does not hold it, says `window not held by this
  primary … (#619 …)`.
- **Mode 2's silent enqueue.** The enqueue's ALERT
  (`ledger/candidates.rs:658-666`) fires only when the first row is missing.
  A reissued first row passes silently, so mode 2 has no pre-offer signal at
  all. *Fixed with (d):* the enqueue checks the last row first and refuses
  with a WARN; the ALERT remains for a pruned prefix only.
- **Same-template reuse ignores cutoff regression** (read from code). The
  same-template reuse test ignores the accepted cutoff whenever a bundle
  exists (`coordinator.rs:1176`). After a promotion the ledger's cutoff can
  sit below the published window's last row (46 against 100 in mode 1's run),
  and on an unchanged template the work would still be kept. This is part of
  mode 2's cause. *Covered by (d):* within one timeline the cutoff never
  regresses, and a timeline change now forces the rebuild.
- **Constant `writer_epoch`** (not fixed). `qbit_share_ledger.writer_epoch` is always
  written as 0 (`ledger/window.rs:855`). It looks like a writer term but
  carries none. Do not mistake it for one in a fix.

## Decision and implementation

The operator chose option (d) on 2026-10-01, together with the mode 2
authority fix; not (b) and not (e). No migration and no setting were added.

- **Writer timeline.** `WriterTimeline` (`ledger/window.rs`) is #466's
  `left(pg_walfile_name(pg_current_wal_lsn()),8)`, read in the transaction
  that scans a refresh window, in the refresh probe and in the clock and
  revision read issuance already makes, so fresh work costs no extra round
  trip. `Prepared` carries the timeline its window was read on; work
  reconstructed from a stored record carries none. It is runtime-only.
- **Durability test.** `probe_window_holding` checks, in one statement, that
  the window's last row exists and matches the window's own predicate
  (`accepted`, accepted and issued by the anchor), and whether its first row
  exists. Missing first row with the last row held is `PrefixPruned`
  (retention); anything else that fails is `NotHeld`.
- **Mode 1.** The candidate enqueue (`persist_prepared_candidate`) runs the
  test inside the transaction that would write the candidate and returns the
  typed `WindowNotHeld`, rolling back the share it carried. `submit_share`
  answers it `stale-job`. The ledger counts it at the refusal under the new
  `qbit_prism_stale_job_rejections_total{cause="window_not_held"}`, so a
  refusal that finishes after the miner's acknowledgement deadline is counted
  too, and logs a WARN naming the block and #619.
- **Mode 2.** The refresh rebuilds when the probe's timeline differs from the
  published work's, labelled under the new
  `qbit_prism_refresh_seconds{trigger="writer_timeline"}`, and never reuses a
  cached window across a timeline. Issuance (`work_authority_in_epoch`)
  admits work from another timeline, or with no timeline, only while the
  window passes the test; a refusal there is deferred with the cause
  `window_not_held …`, not `payout snapshot stale`. `save_compact_prepared`
  and the issued-job repair require the test before writing a prepared record.
- **Landing text.** A landing whose window read fails probes the window and,
  when it is `NotHeld`, records `window not held by this primary …` instead of
  pruning or corruption.

**Precondition: synchronized database clocks.** The durability test compares
time, not identity. A share reissued under a lost number fails it only because
the promoted host stamps it after the gap work's anchor. If the promoted
host's clock lags the old primary's by more than the time from the last gap
work to the first reissued share (seconds), a reissued row passes, the block
is offered, and it ends in `reconciliation` with a digest mismatch: the
pre-fix outcome. The HA reference and the ledger runbook state this
precondition. `candidate_window_switch` pins it: a reissued last row stamped
before the anchor passes the test, and only the landing's digest refuses the
window.

The identity guard of option (d)(2) narrows the precondition but does not
remove it:

- It would carry the last row's `share_id` in the runtime `Prepared` and
  compare it in the enqueue. That needs no migration.
- It would exact-match all work prepared in memory: held jobs and published
  work, which are modes 1 and 2.
- Work reconstructed from a stored record has no `share_id` to compare
  without a migration, so it would keep the time test. Such a record holds a
  gap window only if a release without this fix re-created it.

The guard is not built here. Take it if clock synchronization across the
database hosts cannot be assured.

**A window with no rows at all.** One whose newest row is absent is now refused
`stale-job` rather than enqueued anyway. For work young enough to be mined,
retention never drops a window's newest row, so this is lost history.

**Verified by running** (minidev-alex1, PostgreSQL 16). The four tests in
`b619_gap_work_after_promotion.rs` cover mode 1, mode 2, a fenced switchover
with full replay, and a durable job resumed after an async promotion.

- **With the fix:** all four pass in 5 of 5 runs, plus 5 earlier runs on the
  final product code.
- **Against the product code of `06703682`:** in 5 of 5 runs, mode 1's gap
  block is offered and stuck in `reconciliation` with `window range
  incomplete: expected 15 shares, read 0`, and mode 2's gap work is issued
  after the promotion. The switchover and resume blocks land there too; those
  two tests fail only because the `window_not_held` series does not exist yet.
- **The enqueue's two cases** are in `candidate_window_switch`: a pruned
  prefix still publishes with its ALERT, and a missing or reissued last row
  is refused. `window_switch_tests` pins the landing's `window not held`
  text for a last row reissued after the enqueue.

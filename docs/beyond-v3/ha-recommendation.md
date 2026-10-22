# High availability for the PRISM pair

**beyond-v3 exploration. DO NOT MERGE.** Written 2026-10-09 for Robert Clarke.
This is research and design only: nothing here was run against the pair.
- Every number is either measured, with its source, or marked as an estimate.
- Claims about PRISM cite `v3.0.0-rc.6`.
- The payout and availability models behind several numbers are in
  [`models/`](models/README.md).

**The question.** PRISM 3 runs mainnet on two PhoenixNAP bare-metal nodes in one
datacenter, joined by a private VLAN:
- **A** is today's database writer;
- **B** holds the async standby.

Both run Stratum frontends. Miners reach them through the AWS Hashbalancer,
today by way of a second HAProxy load balancer in front of the pair;
qbit-tools #1324 (open) would route the Hashbalancer to the two nodes
directly.
If A dies, PRISM is down until a human runs the failover. How do we make one
node dying a non-event, within these redlines?
- two nodes in one datacenter, as today, with no third voter or witness;
- no runbook;
- at most a few seconds of lost shares;
- no double payouts and no lost payout data.

---

## 1. Recommendation

**Automate the failover now, without changing what any coinbase pays. Then
anchor payouts to the chain, and build PRISM 4.0 as two always-on nodes.**

### 1.1 What to do, in order

**0. This week: fix the writer lock wedge.** This is a live production risk,
independent of HA.
- **What happens:** every frontend, including B's idle one, takes
  `SETTLEMENT_LOCK` and `ORDER_LOCK` on A's database every ~2 s, across several
  round trips. If B's host dies, or the VLAN drops, in the middle of one of
  those transactions, A's backend keeps the locks until Linux gives up on the
  connection:
  - about 15 min if A's last reply was still unacknowledged;
  - otherwise 2 h idle plus 9 × 75 s keepalive probes, about 2 h 11 min.
- **The effect:** every share append fails at its 5 s lock timeout, and the
  writer never looks dead, so nothing fails over.
- **Why nothing stops it:** neither PRISM 3's connection setup nor the pair's
  PostgreSQL configuration, as their repositories define them, sets
  `tcp_keepalives_*` or `tcp_user_timeout`. PRISM 2.x set 30/10/3 keepalives on
  every session; PRISM 3 retired them.
- **The fix:** server-side keepalives (for example 5/2/3) and a
  `tcp_user_timeout` of about 10 s.
  - They end only sessions whose peer host is dead or unreachable, so they
    never touch a live idle transaction.
  - First confirm on A that production carries no inherited role or
    `ALTER SYSTEM` settings: it was restored physically from the 2.x cluster.
  - Do **not** add `idle_in_transaction_session_timeout` to `prism_writer`.
    rc.6 rejected it because PRISM's legitimate transactions sit idle between
    statements (`docs/prism-ledger-ops.md`, #482).
- **How likely:** red team B estimates that 0.1–0.5% of B host deaths or VLAN
  cuts land inside such a transaction.

**Step 1 (PRISM 3.x): automatic failover, coinbases unchanged.** This is the
step that ends the outage.
1. **Routing first.** The Hashbalancer follows what each node reports about
   itself:
   - a fail-closed "ready" check;
   - a "writer-local" check that is a lease;
   - passive error detection, paced failback, and no Terraform swap per
     failover.

   qbit-tools #1324's TCP-only checks would send miners to a frontend with no
   writer. PRISM's Stratum socket listens from startup, before the database
   connect (`crates/qbit-prism-server/src/server.rs:33-41` vs `:80`), and its
   accept loop never consults readiness. So a frontend whose writer is gone
   still completes every TCP handshake.
2. **A supervisor on each node** runs the failover steps that were drilled by
   hand, under the safety kernel in Appendix B:
   - only the standby fences, by a PhoenixNAP power-off it has confirmed,
     after the writer is silent on every path;
   - standby eligibility is a lease that is re-earned after every boot;
   - every boot is passive;
   - a split brain fences the stale side;
   - rejoin is automatic, and salvages the old primary's unique records first.
3. **PRISM guards that change no coinbase:**
   - publish only work whose inputs are already on the standby;
   - land a found block from the chain if its row was lost;
   - at promotion, revoke the dead node's claims, bump the payout revision, and
     stop and page if the chain holds one of our blocks we can't account for;
   - accept Stratum connections only once ready.
4. **Result:**
   - If A dies, there is no writer for about **20–30 s** in the typical case,
     with no human. That figure assumes PhoenixNAP confirms a power-off in
     under ~10 s, which nobody has measured.
   - B dying, the VLAN breaking, or a node returning causes **no mining
     outage**. The public API, which reads only from B's replica, is down
     while B is (qbit-tools #1217).
   - Payouts are at least as exact as today.
   - In red team D's model this captures nearly all of the availability gain:
     from about 8 expected pool blocks lost a year today to about 0.2.
5. **Cost:** about 4–6 months elapsed, including model checking and drills.
   That is my estimate; red team D put this shape at 3.5–5.5 engineer-months.

**Step 2 (3.x, shadow mode first; needs your approval): chain-anchored payouts.**
- **R1:** each job takes its prior balances from its own parent chain: the sum
  of as-issued deltas over the pool blocks in its ancestry.
- **R4:** a node that cannot account for one of our blocks on that chain pays
  no carried balances until it can.
- **What it buys:**
  - it retires the double payment PRISM 3 already allows and records as debt
    (#478);
  - payouts stay exact even if the fence or the supervisor is wrong;
  - it is the foundation of step 3.
- **Evidence:** red team A's model found no double payout in 19.15 million
  exhaustive combinations of the sum-based rules (6.39 million of them under
  exactly Appendix A's strict rules), and none in 21,600 timed two-node runs.
  Looser readings of the same idea fail (Appendix A).

**Step 3 (PRISM 4.0): two always-on nodes.**
- **The design:**
  - each node appends shares to its own log;
  - a payout window is a cut across both logs;
  - a log transport keyed by identity repairs its own gaps;
  - every node derives balances from the chain;
  - both frontends are active.
- **Why it is the end state:** it comes closest to a non-event. There is no
  failover, no fence, no promotion, and no PhoenixNAP credential on the nodes.
  - A node dying costs only its own miners' reconnect time and its last
    milliseconds of shares, which come back if its disk does.
  - The credential matters because PhoenixNAP's API has no power-only scope.
    The scope a fence needs is account-wide and also allows destructive
    actions.
  - The global `ORDER_LOCK` and the per-share write to the cluster singleton
    go.
- **Cost:** about 8–13 months (estimate).
- **Timing:** its availability gain over step 1 is small at today's scale
  (~0.2 → ~0.07 expected pool blocks a year). It becomes large if miners
  abandon PRISM during a 20–30 s gap. Decide its timing from that measurement
  (§4.1), and prototype its three riskiest parts first.

### 1.2 Why

**Two nodes cannot elect a writer by themselves.** PRISM's HA reference says
so: "the pair alone does not supply a partition-safe election". From inside the
pair, a dead peer and a cut link look the same.
- Every database that fails over on its own needs a third voter: Patroni's
  DCS "has to run with 3 or 5 nodes", and pg_auto_failover's monitor is a
  witness.
- Every database that stays writable on both sides lets the copies diverge. In
  a local drill (PostgreSQL bidirectional logical replication), two paydowns of
  the same balance merged to −1,000 with no error.
- For a single-writer database, the only tie-breaker left is an external
  fence: a PhoenixNAP power-off.

**Automation is where most of the value is, and it must be built carefully.**
- Red team D's model, assuming 4 unplanned losses per node per year at ~100
  pool blocks a day:
  - today's manual failover, assumed to take 30 minutes per loss of A: about
    8.3 expected pool blocks lost a year;
  - an automatic fenced failover: about 0.2;
  - two always-on nodes: about 0.07.
- Red team B then raised 16 findings against a naively automated failover,
  including:
  - a split brain with no exit;
  - a stale standby made eligible by PostgreSQL's `streaming` status, which a
    standby reports from the moment it connects, all through its catch-up;
  - three holes in today's boot guard;
  - a zombie writer after a premature `powered-off`;
  - the lock wedge above.

  Appendix B is the corrected rule set, and §4.2 the drills it must pass.

**The qbit chain can arbitrate payouts.**
- A payout happens only inside a block, and the chain orders blocks.
- Every PRISM coinbase already commits, in its witness reserved value, to its
  full payout manifest: each account's prior balance, gross, on-chain amount,
  settlement fee and carry (`crates/qbit-prism/src/lib.rs:2089-2107`,
  `:310-345`).
- Suppose a job takes its prior balances from its own parent chain, and refuses
  to pay carry over a pool block it cannot account for. Two blocks can then
  both pay one balance only if one descends from the other, and the
  descendant's job already counted the ancestor's payment.
- That holds however many writers, promotions or partitions there are,
  provided every builder runs the rules and recognises every pool block
  (Appendix A).
- It also closes the ~1 s window after each own block lands, in which PRISM 3
  can pay a carry twice today (#478).

**Once payouts are chain-anchored, only the share order still needs a single
writer, and TIDES does not need a global one.**
- TIDES needs a deterministic order inside a consistent cut. Each job already
  has its own window, and the verifier needs an order, not commit order.
- Per-node logs therefore remove the last reason to fail over.
- They need their own control plane. Red team C showed that plain PostgreSQL
  16 logical replication silently loses rows when a node is restored from
  backup, and halts on content-keyed tables. At the pair's settings it also
  takes about 60 s to notice a dead link, then does not move to a second path
  when the first is proxied or silent. Hence the identity-keyed transport in
  step 3.

### 1.3 Redline scorecard

| Redline | Today | Step 1 | Steps 1 + 2 | Step 3 (4.0) |
|---|---|---|---|---|
| Two nodes, one DC, no witness | Yes | Yes. The PhoenixNAP power-off is the fence. | Yes | Yes. No fence at all. |
| No human when a node dies | No | Yes for single failures, if the API can confirm the power-off. If not, page. | Same | Yes |
| At most a few seconds of shares lost | No | Partly: ~20–30 s of refused shares when A dies (typical); none when B dies | Same | B's miners lose nothing. A's miners reconnect in ~8–30 s with tightened balancer checks, 20–60 s with today's (estimates). A's unreplicated tail (milliseconds) is lost, and comes back if its disk does. |
| No double payouts | No. A failover can double-pay without a record; #478 double-pays with a record. | No failover double payment while the fence is correct. #478's recorded race remains. | Yes | Yes |
| No lost payout data | No. A lost candidate row can strand CTV outputs. | Yes for single failures | Yes | Yes for single failures. A double fault that destroys a disk can still lose data. |

### 1.4 What I would not do

- **A third voter or witness** (Patroni, pg_auto_failover, Raft or quorum
  stores, a qdevice): excluded by the redlines.
- **Multi-master for settlement** (bidirectional logical replication, pgEdge
  Spock, EDB PGD): it converges double paydowns quietly.
- **DRBD:**
  - it relies on the same fence;
  - it adds a kernel module, a disk re-layout and a cluster stack;
  - it pays a PostgreSQL crash recovery on every failover.
- **Synchronous replication as the main mechanism:**
  - PostgreSQL keeps a committing transaction's locks while it waits for the
    standby, and PRISM's settlement transactions hold `ORDER_LOCK`, so a
    missing standby stalls share appends until a fence lets the primary
    degrade;
  - a cancelled wait returns a commit that was never replicated;
  - a commit waited 1,124 s on a stopped standby despite
    `statement_timeout=2s`.

  The post-commit wait in step 1 protects what matters, outside every lock.
- **Promoting without a confirmed fence**, for example when the PhoenixNAP API
  is down:
  - red team A: without automatic salvage-and-import, a split brain loses
    payout data even when it cannot double-pay;
  - red team B: it has no automatic exit.
- **TCP-only Hashbalancer checks** as the HA end state.

---

## 2. The options compared

Effort figures are my estimates, built from the research and red-team
reports. Red team D puts the step 1 shape at 3.5–5.5 engineer-months. The
accounting-redesign report puts 4.0 at 6–9 months, and red team C adds 2–4 for
its control plane. They are not a plan.
The "blocks lost" column comes from red team D's model:
- ~100 pool blocks a day, so each minute without a writer costs about 0.07
  expected blocks;
- 4 unplanned losses per node per year;
- mid-case timings.

The column is for comparing options. It is not a forecast.

| Option | One writer by | Payouts exact by | A dies | B dies | VLAN breaks, both alive | Node returns | Depends on | Effort (estimate) | Blocks lost a year (model) | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| **0. Today:** manual failover, async standby (D3) | A human, a PhoenixNAP power-off, the boot guard | Best-effort #529 wait; #619 refusal | Down until a human acts: 64–101 s in planned switchover drills without load, with the operator at the keyboard; likely tens of minutes for a real failure | No mining outage, except the lock wedge (§1.1, step 0); the public API is down while B is | No outage | Manual rejoin | A human | — | ~8.3 | **Unacceptable** |
| **1. Automate with today's helper rules** | Standby fences A | As today | 20–30 s when it works | No outage | No outage | Automatic | PhoenixNAP API | ~2–3 months | — (not modelled) | **Unsafe**: split brain with no exit, boot-guard holes, stale standby promoted (red team B) |
| **2. ★ Step 1:** automatic fenced failover, coinbases unchanged | Safety kernel (App. B): standby-only power fence, leases, passive boot, split-brain self-fence | Publish only standby-durable work; adopt from chain; scan at promotion | **20–30 s**, no human | No outage | No outage | Automatic, with salvage | PhoenixNAP API for promotion | ~4–6 months with drills | **~0.2** | **Do now** |
| **3. ★ Steps 1 + 2:** + chain-anchored payouts | As 2 | **The chain** (R1–R4, App. A); holds even if fencing fails | As 2 | As 2 | As 2 | As 2 | As 2 | +2–3 months, plus approval | ~0.2 | **Do next** |
| **4. Synchronous replication plus fencing** (Pacemaker or custom) | Fence before promote, fence before degrade | Synchronous settlement; #478 remains | About 35 s typical: writer-first fencing makes the standby wait before it fences | Share appends stall 7–70 s (about 15 s typical) with the API up, or until a lease expires (~80 s) | Stall 3–6 s | Re-sync | PhoenixNAP API for both deaths | 2–3 months; D3 change; −12–18% throughput (lab) | ~0.34 | **Dominated by 2** |
| **5. DRBD plus Pacemaker** | DRBD single primary plus STONITH | Synchronous at the block level | 30–100 s including crash recovery | Writes freeze 15–45 s | Brief pause | Bitmap resync | PhoenixNAP API, kernel module | About 5–8 engineer-weeks, including a disk re-layout | — | **Reject** |
| **6. Per-node logs plus one fenced settlement owner** | Fence moves settlement ownership | Synchronous settlement | Shares continue; new work waits for the fence | Settlement stalls until the fence | Fence race | Shares merge | PhoenixNAP API for new work | Large | — | **Dominated by 7** |
| **7. ★ Step 3:** two always-on nodes (4.0) | Not needed: each row has one owner node | **The chain**, as 3 | B's miners unaffected; A's reconnect in ~8–30 s with tightened checks (20–60 s with today's); lost: A's unreplicated tail | Symmetric | No outage once the transport fails over (~5–15 s) | Identity-keyed catch-up, including after a restore | None | ~8–13 months | **~0.07** | **Destination** |
| Patroni, pg_auto_failover | DCS quorum, monitor | — | — | — | — | — | A third voter | — | — | **Excluded by the redlines** |
| repmgr, two nodes | Nothing without our own fencing | — | Split brain on a VLAN break | — | — | Old primary keeps running | — | — | — | **Unsafe** |
| Multi-master (logical replication both ways, Spock, PGD) | Nothing | Nothing: a double paydown merged silently in a drill | — | — | — | — | — | — | — | **Reject** |
| Distributed SQL (CockroachDB, YugabyteDB, TiDB, FoundationDB) | Raft or Paxos majority | — | — | — | — | — | Three or more nodes | — | — | **Excluded by the redlines** |
| Stateless payouts (no carry; a lottery for sub-floor amounts) | — | No carry to pay twice | — | — | — | — | — | Product change | — | **Not needed for HA** |
| Carry held on chain in covenants | — | — | — | — | — | — | — | — | — | **Not feasible:** CTV fixes outputs at creation; P2MR has no `OP_CAT` |

---

## 3. Failure-case walkthroughs

### 3.0 How to read these

- **Two writers / double payouts:** what stops two nodes acting as the writer,
  and what stops a balance being paid twice or a landed block's payout data
  being lost.
- **Recovery and shares lost:** time until miners hash productively again.
  Shares submitted while there is no writer are refused, so they count as
  lost. At the 2.x pool's ~0.6 shares/s (2026-10-06; PRISM 3's own rate is
  unmeasured) the async replication gap is about 0 shares.
  At the 394/s historic peak, with the 0.21 s peak lag measured on the pair,
  it is about 80.
- **Effort and risk:** an estimate, and the main thing that could go wrong.
- **Where the change lives:** PRISM (this repository) or the infrastructure
  (qbit-tools, the Hashbalancer, PhoenixNAP).
- **Timing assumptions:** all recovery times in this section are estimates
  from the reports unless a measurement is cited. PhoenixNAP's power-off
  latency is unmeasured, and it is the largest term in every fenced recovery
  time below. The research assumed 2 / 8 / 60 s (best / typical / worst) from
  request to `powered-off`.

### 3.1 ★ Step 1: automatic fenced failover, coinbases unchanged

| Case | Two writers / double payouts | Recovery and shares lost | Effort and risk | Where the change lives |
|---|---|---|---|---|
| **A dies** (power, hardware, kernel panic, hang) | **Two writers:** B fences A only if all of these hold:<br>• B was eligible at its last contact;<br>• A has been silent on every path: VLAN heartbeat, public-path heartbeat, PostgreSQL on the VLAN, and a new readiness-only health endpoint on its public address (today the health port is never published there), where any HTTP answer counts as alive;<br>• B itself sent the power-off and read `powered-off` twice;<br>• all of this finished within B's eligibility lease;<br>• A's watchdog interval has passed since last contact.<br>Then B promotes, revokes A's claims and bumps the revision and epoch, and only then reports writer-local. If A was only hung and wakes, its watchdog has already rebooted it into a passive boot.<br>**Payouts:** every job A published had its inputs on B before miners saw it, so B can adopt any of A's blocks from the chain. At promotion B scans the chain; an own block it cannot adopt stops it, rather than paying twice. | Detection 3 s; power-off and two `powered-off` reads (unmeasured); `pg_promote` (2.6 s measured on a production-size copy); writer switch; routing (~4 s). About **20–30 s** typical; over a minute if the API is slow; **no automatic promotion if it cannot confirm** (page). **Shares lost:** those submitted during the gap, plus the async gap (~0 today). | ~4–6 months for both parts with drills. Main risk: new safety-critical code. Mitigation: a model-checked kernel, 15 drills, two weeks observe-only. | **PRISM:** durable-before-publish, adopt-from-chain, promotion scan, claim revocation, revision bump, readiness gating, reconcile reads before it locks. **Infra:** supervisor, fence client, routing, and a public readiness endpoint (a rule change). |
| **B dies** | A stays the only writer. B cannot promote: it is down, and when it boots it is passive and ineligible until it has caught up past a fresh marker. A treats B as gone only after one of: B's stand-down, an API read of B as off, or the lease expires (~80 s). Only after that does A re-anchor windows without B's copy. New-tip work never waits. | **No outage**, no share loss. Windows may refresh up to ~80 s late. | Low | Supervisor; PRISM alone mode |
| **VLAN breaks, both alive** | Heartbeats continue on the public path, so nobody fences. B stops receiving WAL and loses eligibility; it stands down over the public path and A goes alone. | **No outage.** If A dies before the link heals, nobody can promote: page (a double fault). A supervised second replication path closes that gap. | Low to medium | Public-path heartbeat (a UFW rule and an exception to the VLAN-only rule); optional replication proxy on the second path |
| **A dead node returns** | **Passive boot:** a primary or empty data directory starts only with a permit from its own supervisor, issued after an epoch handshake with the peer (or the other cases in Appendix B, item 5). `allow-primary-alone` is retired; a peer answering "standby" is never enough on its own. A deposed primary then:<br>1. fences itself;<br>2. checks free space (data size + 25%) and exports its unique rows: shares, outbox, prepared records, balance snapshots, landings, audits and CTV artifacts;<br>3. has its pool-block records imported into the writer;<br>4. rebuilds as a standby, keeping at most one `.old`;<br>5. becomes eligible after a new marker. | **No outage.** Redundancy returns after the rebuild, estimated at tens of minutes for the production database (its post-cutover size is unmeasured). | Medium | Supervisor; PRISM salvage export and import |

### 3.2 What step 2 adds

| Case | Change |
|---|---|
| A dies | Payouts no longer depend on the fence. If the fence or the supervisor goes wrong and two writers run for a while, the chain still prevents a double payout. A node that cannot account for a pool block pays no carry until it can. |
| B dies | None |
| VLAN breaks | None |
| Node returns | Imported pool-block records end any carry-free period. Without import, the node waits. |
| Normal operation | The landing-caused #478 race closes: work on a new own tip uses that tip's own deltas at once. The capture check may still be needed for other same-tip revision bumps: maturity, fatal-state clear, policy transition (#505). |

### 3.3 ★ Step 3 (4.0): two always-on nodes

| Case | Two writers / double payouts | Recovery and shares lost | Effort and risk | Where the change lives |
|---|---|---|---|---|
| **A dies** | **Two writers:** no such thing. Each node is the only writer of its own rows, and every replicated key carries the owner. **Payouts:** every node derives balances from the chain plus the manifests it holds. It adopts any block found on work it holds inputs for, and pays no carry over a block it can't account for. | **B's miners:** unaffected. **A's miners:** back on B in about 20–60 s with today's balancer settings (red team C, inferred), or roughly 8–30 s with tightened checks (red team D's model input; unmeasured). **Lost:** A's unreplicated tail, plus A's miners' in-flight submissions. If A's disk returns, its tail merges back. | ~8–13 months. Highest code risk (money path, audit v2, two writable databases); lowest operational risk. | Mostly PRISM. Infra: active-active routing; the supervisor is retired. |
| **B dies** | Symmetric | Symmetric | — | — |
| **VLAN breaks, both alive** | Each node's log transport moves to a second path: the tailnet in red team C's design, which needs the rule change in open question 5. A plain PostgreSQL connection string does not do that, so a supervised path is needed. If every path is cut, both keep mining: R4 stops carry payments over unknown blocks, and the logs merge when the link heals. | **No outage.** About 5–15 s for the transport to move (estimate). | — | Transport control plane |
| **A dead node returns** | It catches up by identity (log, incarnation, sequence), not by WAL position. A node restored from backup neither loses nor duplicates any row its peer holds. A node with a replaced disk is rebuilt as a clone of the peer and re-personalised, losing only its own unreplicated tail. It takes no sessions until caught up. | **No outage.** No base backup when the disk survived. | — | Transport control plane; readiness gating |

### 3.4 Automate with today's helper rules (red team B's findings)

The four cases look like step 1's. But:
- **Split brain:** a deposed primary that comes back as primary (through the
  guard's holes, or a premature `powered-off`) passes every writer-local
  check, and gets new sessions because it has the fewest. Nothing ends the
  split.
- **Stale standby:** "alone" mode keyed on the standby's absence, plus an
  eligibility test on `streaming`, lets a stale B promote. PostgreSQL reports
  `streaming` from the moment the receiver connects, all through catch-up,
  and for up to 60 s after a silent link loss.
- **Lost records:** the rejoin moves the loser's unique records to `.old`, and
  no tool imports them.
- **Unfixable without new rules:** a sick database on a live host (for example
  `ENOSPC` crash loops) is a permanent outage, because the only trigger is
  silence.

The corrected rules are Appendix B.

### 3.5 Synchronous replication plus fencing

| Case | Two writers / double payouts | Recovery and shares lost | Effort and risk | Where the change lives |
|---|---|---|---|---|
| A dies | Fence before promote, with the same extra rules (volatile eligibility, passive boot, corroborated fence), plus never cancelling a sync wait. Nothing acknowledged is lost. #478 remains. | About 35 s typical (writer-first fencing makes the standby wait before it fences); no acknowledged share lost | 2–3 months. Risks: `ORDER_LOCK` held through every commit's wait for the standby (an estimated ceiling of about 445–665/s against 750/s measured today, 10–40% lower; never measured on the pair); the cancellation gap | Infra, plus a D3 change |
| B dies | Commits wait for the standby, and settlement holds `ORDER_LOCK` while waiting, so **share appends stall** until A fences B and degrades | 7–70 s (about 15 s typical) with the API; about 80 s on a lease without it | — | Infra |
| VLAN breaks | Stand-down over the public path, then degrade | Stall of 3–6 s | — | Infra |
| Node returns | Eligibility re-earned through a replicated marker | No outage | — | Infra |

### 3.6 DRBD plus Pacemaker

| Case | Two writers / double payouts | Recovery and shares lost | Effort and risk | Where the change lives |
|---|---|---|---|---|
| A dies | DRBD single primary plus STONITH through a PhoenixNAP fence agent nobody has written. DRBD's own quorum needs three nodes. Its default handler timeout resumes writes without a fence, so the timeout must be made unbounded. | 30–100 s including crash recovery; minutes after heavy-WAL periods | About 5–8 engineer-weeks, including a disk re-layout (probably a reprovision of each node). A kernel module (DRBD 9 is not in the mainline kernel), a cold standby, and the public replica must move. | Infra |
| B dies | Writes freeze until the fence (15–45 s) | Stall | — | Infra |
| VLAN breaks | Brief pause (with two Corosync links), then run without a copy | Pause | — | Infra |
| Node returns | Automatic bitmap resync | No outage | — | Infra |

### 3.7 Per-node share logs with one fenced settlement owner

Shares never stop, and a dead node's shares merge back. But new work waits for
a fence when the settlement owner dies, and a cut link is a fence race.
Chain-anchored settlement (3.3) removes the owner, so this option is dominated.

### 3.8 Excluded before the walkthrough

- **Patroni:** the DCS "has to run with 3 or 5 nodes". A two-member etcd or
  Consul tolerates no failure.
- **pg_auto_failover:** the monitor "acts both as a witness and an
  orchestrator".
- **Third-voter systems:** CockroachDB, YugabyteDB, TiDB, FoundationDB, etcd,
  Galera (without `garbd`), MySQL Group Replication and Kafka KRaft each need a
  majority of three or a witness.
- **repmgr with two nodes:** with no other standby and no validation hook,
  the standby's election returns "we win by default" when it loses the
  primary.
- **Multi-master** (drill on PostgreSQL 18.6, same rules as 16):
  - double paydowns merged to −1,000;
  - payout revisions swapped silently;
  - landing one block on both nodes froze replication;
  - a trigger-maintained balance disagreed with its rows even on a healthy link.
- **Stateless payouts:** no carry, with sub-floor amounts paid by a lottery
  seeded by the parent block's hash. They are unbiased in a toy simulation,
  but they make tiny miners' income lumpier, the operator can bias the lottery
  through window timing and transaction selection, and they aren't needed once
  R1–R4 exist.
- **Carry held on chain by covenants:** not feasible. CTV fixes outputs at
  creation, and P2MR offers no `OP_CAT` (the opcode is a reserved OP_SUCCESS
  there).

### 3.9 Partial failures

| Failure | Step 1 | Step 3 (4.0) |
|---|---|---|
| A's PostgreSQL sick (crash loop, `ENOSPC`, D-state I/O), host alive | A's supervisor sees a failing timed write, fences its own database (`fence on`) and tells B, which promotes **with no API call**: about 15–25 s | That node reports not ready; its miners move |
| A hung, not dead | Silent on every path, so B power-fences it. A's hardware watchdog (shorter than B's earliest power-off) reboots a hung supervisor into a passive boot. | Ejected by the balancer; catches up when it wakes |
| A loses the internet, keeps the VLAN | Miners move to B's frontend, which writes to A across the VLAN (the slow path; fine at today's load). A planned, lossless switchover follows. | Its miners and qbitd are cut off; the other node serves |
| A loses the VLAN, keeps the internet | Same as "VLAN breaks" | Same |
| PhoenixNAP API down when A dies | No automatic promotion: page | No effect |
| The API reports `powered-off` but A is up | Corroboration and A's own self-status poll catch it. A self-fences if it sees a power-off aimed at itself or a peer on a higher epoch. | No effect |
| B lags chronically | B is ineligible while its lag is over the bound; alert | Windows refresh late while the peer's acks are late (new-tip work reuses durable inputs); alert |
| Writer-lock wedge (a client vanishes mid-transaction) | Fixed by server-side keepalives and `tcp_user_timeout` (§1.1, step 0); the supervisor also ends a wedged backend whose client is the silent peer | Not applicable: no shared writer |

---

## 4. Phased path

### 4.1 Phase 0: now and the next few weeks (no production risk)

1. **The lock-wedge fix** (§1.1, step 0):
   - first read A's effective settings (`SHOW tcp_keepalives_idle;` and
     `SELECT * FROM pg_db_role_setting;`), because production inherited its
     cluster from 2.x;
   - set server-side `tcp_keepalives_idle`, `_interval`, `_count` and
     `tcp_user_timeout`;
   - leave `idle_in_transaction_session_timeout` off `prism_writer`, as rc.6
     and the pair runbook decided;
   - run a drill: drop the VLAN while B's reconcile holds `ORDER_LOCK`, and
     check that the lock clears within seconds.
2. **PhoenixNAP API characterisation** on a spare server of the same type, not
   on the pair:
   - at least 20 power-off/on cycles, measuring request to `powered-off`, and
     whether the status ever leads the real power cut (watch SSH and ICMP);
   - repeat-power-off and 409 behaviour, token lifetime, rate limits,
     audit-log delay;
   - ask PhoenixNAP what a server with a dead PSU or unreachable BMC reports.
3. **Miner reconnect behaviour.** Cut a frontend for 10, 20, 40 and 60 s, and
   record how PRISM's actual miners reconnect, switch pools and come back. This
   decides step 3's timing.
4. **Measurements:**
   - rc.6's cross-VLAN `ORDER_LOCK` hold and balanced-split ceiling (the
     preferred-frontend rule rests on rc.1 numbers);
   - replication lag under share load;
   - VLAN link speed, and rebuild time at production size.
5. **Your decisions** on section 5.
6. **qbit-tools #1324:** if it merges as an interim step, keep its own rule:
   A's lane stays disabled after a failover until the swap applies.

### 4.2 Phase 1: step 1 (PRISM 3.x plus qbit-tools)

**PRISM, behind cluster-wide flags (no change to what coinbases pay):**
1. **Durable before publish.** After a prepared record commits, wait until the
   standby's `flush_lsn` passes it before publishing. This is #529's wait
   moved to publication, mandatory, outside every lock.
   - New tips never wait: they reuse the last durable window anchor and fee
     sample, so their work can always be rebuilt.
   - "Alone" mode only on the supervisor's say-so.
   - This also retires #619's lost rewards.
2. **Adopt from chain.** Land an active-chain pool block with no candidate row
   by rebuilding its coinbase from the prepared records and the on-chain
   extranonces, then landing it as issued. Keep prepared records until their
   parent is 1,000 blocks deep; today they live ~2.5–3 minutes.
3. **Promotion hygiene:**
   - revoke the dead instance's candidate and fanout claims;
   - bump `payout_revision` and `chain_epoch` by a large constant;
   - scan the chain for own blocks with no row, adopt them, and stop and page
     on any that can't be adopted.
4. **Readiness and locking:**
   - `writer_path` in `/healthz`;
   - no Stratum accept before readiness;
   - reconcile reads first and takes locks only to write, which also shortens
     cross-VLAN lock holds.
5. **Salvage export and import** of a deposed primary's unique records.

**Infrastructure (qbit-tools):**
1. **The safety kernel and supervisor** (Appendix B): specify it, model-check
   it (TLA+ or Stateright, with the API as an actor that delays, fails and
   lies), then build it from the existing `qbit-prism-db` verbs.
2. **A PhoenixNAP fence client:**
   - only `GET`, `power-off` and `power-on`; never `reset`, which wipes the
     disk and reinstalls, nor `reboot`, `shutdown` or `deprovision`;
   - one credential per node, with an audit-log watcher.
3. **A second path between the nodes:**
   - heartbeats on the public path;
   - a readiness-only health endpoint on each node's public address, for the
     peer's all-path silence check and for the Hashbalancer;
   - optionally, a supervised second replication path.

   Each needs a UFW rule and an exception to the pair's rule that node-to-node
   traffic uses only the VLAN, and the endpoint is the first public health
   port (open question 5).
4. **Hashbalancer follower routing:**
   - "ready" and "writer-local lease" checks;
   - `observe layer4` with `redispatch`, `shutdown-sessions` on mark-down,
     paced failback;
   - alarms on "no writer-local line" and on "two writer-local lines".

**Qualification.** Run red team B's 15 drills, on a lab pair of the same
PhoenixNAP type first, then on the pair:
- API characterisation;
- A dies under load (crash, power-off, freeze);
- slow-promote, stale-marker and zombie races;
- a full partition with a slow API;
- B dies, hangs or reboots, then A dies during B's catch-up;
- a VLAN-only break, then A dies;
- a sick database;
- operator races;
- five consecutive failovers with rejoin;
- the lock wedge;
- routing transitions on the real AWS path;
- versions and clocks;
- an injected split brain.

Then run the supervisor in **observe-only mode for at least two weeks**: it
decides and logs, but doesn't act. It may act only after zero false fence
decisions.

### 4.3 Phase 2: step 2 (PRISM 3.x, after a shadow period)

1. **R1 and R4, as precisely specified in Appendix A:**
   - R1 is the sum over the whole ancestry;
   - R4 is strict;
   - checkpoints advance only over fully known prefixes;
   - pool-block recognition does not depend on mutable configuration;
   - activation goes through the cluster config fingerprint, so no node ever
     runs without the rules.
2. **A coinbase marker that binds the payout outputs:** a MAC under the pair's
   key, or an ed25519 signature.
   - It is needed so that a copied tag cannot stall carry payments; a MAC over
     only height and parent can be copied.
   - It costs 65 scriptSig bytes for an ed25519 signature, or roughly 17 for
     a truncated MAC plus a mode byte (estimate).
   - Mainnet coinbases already use about 28 of the 100 bytes, so a signature
     leaves about 7 (red team D).
   - Check old miner firmware first.
3. **Shadow mode.** Compute and log R1's priors next to today's for several
   weeks, and require zero unexplained differences.
4. **Then enable.** This closes the landing-caused #478 race. The capture
   check may still be needed for other same-tip revision bumps (#505). It needs
   a D2-style approval, and an update to the money-path vectors.

### 4.4 Phase 3: PRISM 4.0, two always-on nodes

**Prototype first**, in red team C's order:
1. **The log transport under faults:**
   - a restore of either node, disk replacement, slot loss;
   - a VLAN black hole;
   - schema drift, and key collisions.

   Pass means no silent gap, and no halt that needs a human.
2. **Cut, merge, dedup, clock and audit-v2 semantics**, as a deterministic
   model with property tests.
3. **The lifecycle of two writable databases:**
   - a rolling upgrade;
   - the 3 → 4 cutover;
   - a clone-and-re-personalise rebuild,

   timed on pair-class hardware.

**Then build:**
- **Per-node share logs:**
  - identity `(node, incarnation, local_seq)` with dense sequences;
  - a hybrid logical clock;
  - group commit.

  They retire the global `ORDER_LOCK` and the per-share write to the cluster
  singleton, which #738 already removes in 3.x. `extranonce1` is split per
  node.
- **An identity-keyed log transport** with gap repair and lineage checks.
  WAL-position logical replication is not safe on restore. No derived table is
  ever replicated, and every replicated key carries its owner.
- **Windows as cuts**, normally time cuts at the peer's watermark,
  deduplicated by header hash before the window is folded.
- **Audit bundles v2** with a cumulative link check, plus a chain verifier.
  "prior equals the previous block's carry" is false after a carry-free block.
- **Settlement:**
  - every node derives it from the chain, so `payout_revision` stops being
    authority;
  - fee, policy and signing changes activate at a block height;
  - so do changes to the derivation code.
- **Resume** only from a stored job record, at its stored target. Rebuilding
  from the miner's job ID lets a miner hold one job on both nodes at different
  difficulties and nearly double its credit (red team C).
- **Readiness** gated on catch-up and replication health. Keep a "my qbitd is
  behind" fence.
- **Active-active routing.** Retire promotion and the supervisor.
- **Migration:**
  - freeze PRISM 3's ledger as "log 0", with a balance checkpoint at a mature
    height;
  - run in shadow mode first: the derivation must reproduce
    `qbit_current_carry_forward_balances()` exactly.

### 4.5 What ships where

| Change | 3.x | 4.0 | Infra only |
|---|---|---|---|
| Lock-wedge settings | | | ✓ (now) |
| Durable before publish, adopt from chain, promotion hygiene, readiness, salvage | ✓ step 1 | kept | |
| Safety kernel, supervisor, fence client, public-path heartbeats | | retired | ✓ step 1 |
| Hashbalancer follower routing | | readiness only | ✓ step 1 |
| R1 (sum), strict R4, checkpoints, fingerprint activation | ✓ step 2 (approval) | kept | |
| Output-bound coinbase marker | ✓ step 2 (if firmware allows) | ✓ | |
| Per-node logs, identity-keyed transport, cuts, audit v2, chain verifier, derived settlement, active-active | | ✓ | |

---

## 5. Open questions for Robert

1. **Is #478 inside your "no double payouts"?**
   - PRISM 3 can pay some carry twice when an own block is found in the ~1 s
     after a previous own block lands. That is bounded, recorded as debt, and
     documented as an accepted cost (`docs/prism-ledger-ops.md`, "Accepted
     cost and its bound").
   - Step 1 does not change that. Step 2 removes it, but changes the payout
     rule. Open issue #505 proposes a narrower 3.x fix for the landing race.
   - Do you want step 2? It needs a D2-style approval.
2. **Carry-free work during degradation (step 2).**
   - When a node cannot account for one of our blocks, it pays no carried
     balances until it can.
   - Nothing owed is lost: carried balances wait. New gross keeps accruing,
     and the shortfall goes to the pool-fee output as swept dust, as it
     already does when eligible balances fall short.
   - That sweep is permanent, though. In red team A's production-scaled model,
     a 50-block carry-free episode raised the pool's IOU float by about
     0.19 QBIT (to about 0.21), and it stays there.
   - Do you accept that, or would you rather the node halt?
3. **The PhoenixNAP API as the only tie-breaker in step 1.** If A dies and the
   API cannot confirm the power-off, I recommend B pages and does not promote.
   - An unfenced promotion cannot double-pay once step 2 ships.
   - In red team A's model it lost payout data in every split-brain run
     without automatic salvage-and-import (none with it).
   - The split itself has no automatic exit (red team B).
   - Agree?
4. **The credential.** PhoenixNAP's API has no power-only scope. The scope
   that allows power-off is account-wide and also allows destructive actions,
   and no per-server scoping is documented.
   - Accept it, with one credential per node and an audit-log watcher?
   - Or ask PhoenixNAP for an account that holds only the pair?
   - Step 3 removes the credential.
5. **The public path as a second link between the nodes.**
   - It carries heartbeats, plus a readiness-only health endpoint that today's
     rules keep off the public address.
   - It runs between the same two nodes; it is not a third voter.
   - Does it fit your redline? It needs a UFW rule, and an exception to the
     pair's rule that node-to-node traffic "uses the VLAN addresses, never the
     public or tailnet ones".
   - A second *replication* path needs the same exception.
6. **Step 1's share loss.** A's death costs ~20–30 s of refused shares, not a
   few seconds.
   - Accept that until 4.0?
   - Or add, in 3.x, a frontend that holds sessions and spools shares through
     the gap (estimated 4–6 weeks)?
7. **When to build 4.0.**
   - Commit to it now as the beyond-v3 architecture?
   - Or wait on a trigger:
     - the miner-reconnect measurement shows miners abandon PRISM during a
       20–30 s gap;
     - the API proves unreliable;
     - load passes ~100 shares/s;
     - you want the credential off the nodes.
8. **Audit v2 (4.0).** External verifiers would check cuts across per-node logs
   instead of one share sequence. In return, they could detect a double
   payment from the chain alone, for the first time. Acceptable?

---

## Appendix A. The payout rules, precisely

A **pool block** is a block whose coinbase PRISM built. `P` is the parent a job
builds on. These definitions come from red team A's model. Each looser
reading listed below double-pays, loses money or data, or stalls carry payments
in that model. Record retention was checked by inspection, not modelled.

- **R1, ancestry prior.**
  - A job's prior balances are the starting balances at a complete checkpoint,
    plus the sum of `(gross − on-chain)` from the as-issued manifest of every
    pool block in `P`'s ancestry since that checkpoint.
  - Today's balances already sum per-block as-issued deltas
    (`qbit_current_carry_forward_balances()`). R1 changes which blocks count:
    the job's ancestry, not the database's "confirmed" set.
  - Not "the carry vector of the latest pool block". That reading forgets money
    after any carry-free block, up to the whole positive float in a
    production-scaled run.
- **R4, strict carry-free.**
  - A job may pay any positive carried balance only if the node holds, and has
    checked against each block's coinbase commitment, the manifest of every
    pool block on `P`'s chain since the checkpoint.
  - Otherwise it builds carry-free work: every prior is set to 0, so anyone
    holding the work's durable inputs can rebuild it.
  - No "last N blocks" shortcut, and no skipping of unknown blocks marked
    carry-free unless the mode is authenticated together with the outputs.
- **Checkpoints** advance only over a fully known prefix, and are recomputed
  when a missing manifest arrives. Otherwise two nodes with the same manifests
  never agree.
- **Recognition** of pool blocks has no false negatives and does not depend on
  mutable configuration: all historical tags and formats, plus matching against
  retained records. A missed pool block double-pays.
- **The marker** binds the payout outputs (step 2), so a spoofer cannot stall
  carry payments. Without it a copied tag makes R4 carry-free, and in step 1
  makes the promotion scan stop.
- **Activation is cluster-wide**, through the config fingerprint. A single
  builder running without the rules double-pays.
- **Durable before publish.**
  - Work reaches miners only after its inputs are on the standby (or peer).
  - New-tip work is derivable: it reuses a durable anchor and fee sample,
    carry-free work uses prior 0, and the coinbase carries an anchor ID (the
    step 2 marker can hold it).
  - Until the step 2 marker carries the anchor ID, step 1 finds a new-tip
    job's anchor by rebuilding from each recent durable anchor and matching
    the coinbase's manifest commitment. That costs more CPU but needs no
    coinbase change.
- **No non-durable carry-paying work** unless the peer is positively dead or
  fenced.
- **Salvage and import** a returning node's pool-block records before any
  rebuild.
- **Retention:** prepared records and inputs are kept until their parent is
  1,000 blocks deep.

**Why there is no double payout** (proof sketch):
- The policy never pays an account more than its candidate balance
  (`crates/qbit-prism/src/lib.rs:1565`).
- Two pool blocks that both count are ancestor and descendant on the active
  chain, and the descendant's job already includes the ancestor's deltas.
- Siblings at one height cannot both be on chain.
- A carry-free block's deltas are never negative.

**Model result** (`models/payout/`):
- The Python port of `apply_payout_policy` and the fanout-fee step matches all
  25 of PRISM's fixture vectors for those functions.
- **Double payouts:**
  - under exactly these definitions, none in 6,388,553 exhaustive
    chain-and-knowledge combinations;
  - none in 19.15 million across the three sum-based variants (two use the
    relaxed R4 with an honest mode byte);
  - none in 21,600 timed two-node runs (about 2.16 million pool blocks, mostly
    with the relaxed R4) covering deaths, disk loss, partitions, split brain,
    forks, deep reorgs, standby lag and spoofed markers.
- **Data loss in single faults** came from three paths in the model:
  - a carry-free fallback built at a tip change while the standby lagged;
  - "alone" work discarded by a rejoin after a partition and fence;
  - an unfenced split brain.

  Derivable work, no non-durable carry-paying work, and automatic
  salvage-and-import removed all three: no single-fault data loss, and no
  permanent carry-free state.
- **Remaining exceptions:**
  - double faults that destroy a disk;
  - carry payments stalling under a marker spoofer until the output-bound
    marker ships.
- I re-ran three exhaustive configurations and they reproduced exactly:
  - the strict rules: 6,388,553 chains, no overpay;
  - two configurations that double-pay: "no R4" and "latest block only".

## Appendix B. The step 1 safety kernel

This is condensed from red team B's corrected rule list, which builds on the
fencing research.

1. **Epochs.**
   - Each node keeps a durable record of role, timeline, epoch, sole-writer
     state, stand-down point and fence evidence.
   - It keeps a volatile record of boot ID, eligibility, leases on
     `CLOCK_MONOTONIC_RAW`, and last contact per path.
   - Every message carries the epoch.
2. **Fence before promote, within the lease.** B promotes only if all of these
   hold:
   - it was eligible at its last contact;
   - A has been silent on every path for `T_det`;
   - B itself sent `power-off` and read `powered-off` twice;
   - all of that finished before its lease expired;
   - A's watchdog interval has passed since last contact.

   The one exception is the cooperative fence of item 13: when A has fenced
   its own database and announced it, B verifies that A's PostgreSQL is silent
   on the VLAN and promotes without the API. Otherwise B never promotes
   unfenced. It never calls `reset`, `reboot`, `shutdown` or `deprovision`.
3. **Eligibility is positive and volatile.** B is eligible only if:
   - it has replayed a marker the writer wrote after B caught up, and after
     the writer last left alone mode;
   - it has not rebooted, restarted PostgreSQL or stood down since;
   - the writer's last heartbeat showed B's lag under a bound, and alone mode
     off;
   - its lease is unexpired;
   - PRISM's durability flags are on cluster-wide;
   - its clock offset is at most 100 ms.

   PostgreSQL's `streaming` status is not evidence.
4. **Alone mode** (publishing work B has not flushed) is allowed only after one
   of:
   - B's durable stand-down;
   - an API read of B as off, or evidence that B rebooted;
   - a lease period `G` longer than B's eligibility lease.

   On leaving alone mode, the writer writes a new marker once B's flush passes
   its own.
5. **Passive boot.** A primary or empty data directory starts only with a
   permit, issued after one of:
   - an epoch handshake;
   - a sole-writer record, plus a fresh API read of the peer as off, plus
     silence on every path;
   - an operator bootstrap with both directories empty.

   This replaces `allow-primary-alone`.
6. **Split brain.** A primary that learns its peer is a primary with a higher
   epoch fences itself at once and pages. Equal epochs: both fence.
7. **Writer-local is a lease.** It needs fresh evidence that the peer is not a
   primary. The Hashbalancer alarms if two writer-local lines are ever up.
8. **Self-protection.**
   - Each node polls its own PhoenixNAP status, and self-fences on any
     power-off aimed at itself.
   - A hardware watchdog, shorter than the peer's earliest power-off, is petted
     only by the main loop.
9. **Promotion order:**
   1. `pg_promote`;
   2. revoke the dead instance's claims;
   3. bump the revision and epoch;
   4. write the sole record;
   5. switch the writer endpoint;
   6. only then report writer-local.
10. **Never:**
    - `pg_rewind`;
    - deleting a data directory before its salvage export is verified;
    - cancelling backends as a "degrade".
11. **Hold.** A replicated maintenance hold with a TTL suspends:
    - fencing and promotion;
    - the alone-mode timer;
    - the cooperative fence;
    - standby power-cycling, power-on and rejoin.

    The split-brain self-fence and self-protection stay armed. The failover
    playbook, converges and planned switchovers require the hold.
12. **Interlocks.**
    - Automatic promotion stays off unless PRISM's durable-publish and
      adopt-from-chain flags are on cluster-wide.
    - Red team B also required R4. Step 1 substitutes the stop-and-page
      promotion scan (Appendix D, item 4).
    - Supervisor protocol versions must match. A mismatch means "alive but
      uncooperative": no fencing, page.
13. **Liveness rules:**
    - cooperative self-fencing for a sick database, with no API needed;
    - the lock-wedge watchdog;
    - keep waiting while a promotion's replay advances;
    - the writer may power-cycle a standby that has been silent on every
      path for at least 10 minutes, never one that answers on any path;
    - power-on of a fenced peer after 10 minutes, at most twice a day, never
      within 30 minutes of its last rejoin, under a hold, or after a rejoin
      that failed for lack of space;
    - at most one `.old` directory per node;
    - salvage before every rebuild.

## Appendix C. Evidence

| Number | Meaning | Source |
|---|---|---|
| ~0.6 shares/s; peaks 394/s (5-minute average) and 1,971/s (one second) | Load on the 2.x pool, 2026-10-06 (PRISM 3's own rate is unmeasured); historic peaks on Aug 14 | 0.6/s: 2.x pool (unpublished); peaks: #260 |
| ~100 pool blocks/day | Pool block rate in mid-September (2.x pool); the 83-day average was ~7/h. PRISM 3's current rate is unmeasured and may differ, and every "blocks lost" figure scales with it. Network target spacing is 60 s. | #413; `doc/chain-parameters.md` |
| 1,000 blocks | Coinbase maturity (~17 h) | `doc/chain-parameters.md` |
| 1.10 vs 3.29 ms; ~890/s vs 296.9/s | `ORDER_LOCK` hold and ceiling, frontend on the writer's node vs on B (rc.1, ~9 round trips under the lock) | #711 |
| 1.154 ms; 750/s | Local hold and ceiling at rc.3+ (3 statements plus COMMIT under the lock); rc.1 measured 1.473 ms in the same run | v3.0.0-rc.3 release notes |
| 0.087 / 0.123 ms; ~15 µs | Writer-endpoint `SELECT 1` from A and from B; NVMe `fdatasync` | #711 |
| under 1 ms to 0.21 s (peak) | Standby replay lag in 5 s samples during a VACUUM that wrote 0.91 GB of WAL in 86 s (2026-10-08); never measured under share load | pair evidence (unpublished) |
| 0.32 s; 2.6 s; ~2.8 s; 64–101 s | The helper's promote step on an empty database; promoting a production-size warm copy; the helper's steps in the 2026-10-05 planned switchover drills (no load); the writer gap in those drills, mostly pauses between playbook steps | pair evidence (unpublished) |
| 0 | Acknowledged shares lost in the one unplanned failover under load on the pair (300 shares/s) | v3.0.0-rc.3 release notes |
| 10.1–12.0 s; 16.1–18.0 s; 2.1–4.0 s; 50 sessions/s | Stratum load-balancer ejection when the `/healthz` probe answers or hangs; re-entry; reconnect pacing | pair runbook: lab measurements on the pair's HAProxy build and settings (unpublished); 50/s is configured pacing |
| 1,124 s | A COMMIT blocked on a stopped synchronous standby despite `statement_timeout=2s` | `docs/prism-ha-reference-architecture.md` |
| 0.9 ms p50, 2.5–3.9 ms p99 | A healthy standby confirming a flush (#529 prototype, loopback lab) | #529 |
| 12–18% | Throughput cost of synchronous replication (lab VM) | `docs/prism-throughput-measurements.md` |
| 2 h 11 min | How long a vanished client's backend can hold `ORDER_LOCK` under Linux's default keepalive (7,200 s + 9 × 75 s); about 15 min instead if its last reply was unacknowledged | red team B; kernel defaults |

**Never measured, and needed:**
- PhoenixNAP power-off latency, and what the status shows during and after a
  power cut;
- miners' reconnect and pool-switch behaviour;
- rc.6's cross-VLAN lock hold;
- replication lag under share load;
- VLAN link speed and rebuild time;
- a real VLAN partition;
- the production database's size since the cutover.

## Appendix D. Where the research disagreed, and my call

1. **Who coordinates paydowns: a fence or the chain?**
   - The alternative-databases scout: paying a balance at most once is a
     bounded decrement, so with two nodes only a power-off can coordinate it.
   - **My call: the chain.** Paydowns only take effect inside blocks, and the
     chain orders blocks. The scout's drill double-paid because the database
     landed two blocks without the ancestry constraint that R1 imposes.
   - Red team A's model supports this, under the precise rules in Appendix A.
2. **What to do first: change the money path, or automate the infrastructure?**
   - The devil's advocate argued infrastructure first. Failover code runs only
     when something fails, and fails safe as an outage. Money-path rules run on
     every block, and their payments are final.
   - **Agreed.** Step 1 changes no coinbase. Durable-before-publish and
     adopt-from-chain stay in step 1 because they turn a halt into a recovery
     and pay nothing differently.
   - Red team B made them, together with R4, a precondition for automatic
     promotion. Step 1 replaces R4 with a promotion-time scan that stops and
     pages.
3. **Writer-fences-first or standby-only fencing.**
   - **My call: standby-only, with leases**, so a fence death-match is
     impossible.
   - Revisit after the API measurement. If power-off is fast and reliable,
     writer-first protects a serving writer in a full partition.
4. **Halt or carry-free when a pool block can't be accounted for.**
   - **Step 1: stop and page, at promotion only.** It is a rare event, and a
     tag-copier cannot trigger it at will.
   - **Step 2: carry-free continuously**, with an output-bound marker.
5. **Synchronous commits, or a wait after the commit.** **My call: wait after
   the commit.** Synchronous commits hold `ORDER_LOCK` through the wait, and a
   cancelled wait leaks an unreplicated commit.
6. **Logical replication, or an identity-keyed transport, for 4.0.** **My call:
   an identity-keyed transport.** Red team C's PostgreSQL 16 drills showed
   silent row loss after a restore, replication halts on key collisions, and no
   path failover.
7. **Is 4.0 worth it?**
   - The devil's advocate: not on availability alone, about 0.13 expected
     blocks a year more than step 1.
   - **My call:** it comes closest to every redline: only the dead node's
     miners lose time while they reconnect. It needs no fence and no
     destructive-capable credential on the nodes. Commit to its design work,
     and time the build on the miner-reconnect measurement (open question 7).

## Appendix E. How this was produced

**Ten research agents** worked in parallel from read-only copies of `3.x.x`
(v3.0.0-rc.6) and of qbit-tools `testnet`:
- two code cartographers (the share and job path; settlement and CTV);
- a PhoenixNAP fencing specialist;
- a two-node PostgreSQL HA analyst;
- an accounting-redesign architect;
- a routing analyst;
- an alternative-databases scout, with local replication drills;
- a chain-anchored-payouts explorer;
- a DRBD scout;
- an evidence historian.

**Four red teams** then attacked the finalists:
- **A:** the payout rules, with the executable model in `models/payout/`;
- **B:** the 3.x failover as a running system;
- **C:** the 4.0 design, with eight PostgreSQL 16 replication drills;
- **D:** a devil's advocate, with `models/availability/`.

No production system was touched.

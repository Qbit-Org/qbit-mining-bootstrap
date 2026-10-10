# qbit-prism-dual-sim

A two-node PRISM system simulation for the 3.1 dual writer: the whole pair
on one host, a fault injector that can break any part of it, and a checker
that holds the result to the payout invariants.

It is test code. Every server it drives is the real `qbit-prism-server`
binary, every database a real PostgreSQL 16 cluster, every chain a real
regtest `qbitd`, and the miners real Stratum sessions mining real proof of
work.

## What runs

```
                        miners (qbit-prism-load client sessions)
                                      |
                              balancer stand-in
                    (HTTP /healthz checks, A preferred, B backup)
                       |                               |
              public-stratum-a                 public-stratum-b
              public-health-a                  public-health-b
                       |                               |
   qbitd A ---- frontend A (node 0, owner)   frontend B (node 1) ---- qbitd B
      \                |    \           peer-b-to-a    /    |              /
       \               |     `-- peer-a-to-b --.  .--'      |             /
        \         PostgreSQL A  <--------------'  `---> PostgreSQL B     /
         \                                                              /
          `------------------------- qbitd C (the network) ------------'
```

- **Chain.** One `qbitd` per node and a third, C, standing for the rest of
  the network. C mints every external block and a keepalive tip whenever the
  tip is 120 s old. The chain is first ramped to height 12,960, where the
  target is `1e7fffc0` (about 131,000 hashes a block), because regtest's
  proof-of-work limit makes every share a block. The ramp takes 25-40 s; it is
  done once per cache root and every scenario starts from a copy.
- **Databases.** One PostgreSQL 16 cluster per node, on loopback TCP only,
  with `fsync`, `full_page_writes` and `synchronous_commit` on. The
  frontends log in as the owner role `prism`; the peer pull as the read-only
  `prism_peer_sync`.
- **Frontends.** `qbit-prism-server run` per node, with the load harness's
  environment on regtest: vardiff off, one share difficulty that makes the
  payout window 2,000 shares, a 2e7-sat payout floor (the smallest account,
  1 of 29 sessions, earns about 1.5e7 a block, so it accrues carry and is
  paid down later), a 5% pool fee (its output also takes the swept sub-floor
  dust and must stay above the floor once its share of the fanout fee is
  carved out), and CTV settlement with every miner payout through a fanout.
  Both nodes run the same test signing seeds (CONTRACT.md D-6).
- **Balancer stand-in.** A scripted router shaped like the Hashbalancer's
  configuration for the pair (D-7): A preferred, B backup, a check every 2 s
  with a 1 s timeout through the node's public link, down after 3 failures,
  up after 2 passes, every session of a node marked down closed at once, and
  paced failback (20 sessions/s). In the dual-writer pair it checks the
  token-protected `GET /readyz`; the 3.0 pair keeps `/healthz` with
  `ok: true`. A frontend's `/healthz` answers 503 for about a second after
  every landed block while it rebuilds work, so faster checks eject healthy
  nodes, both at once. Round-robin routing models a misrouting balancer (S5).
- **Load.** `qbit-prism-load`'s client sessions for four payout accounts
  (16, 8, 4 and 1 sessions), through the balancer, reconnecting 250 ms after
  a lost connection. The client never submits a hash that is also a block, so
  the load finds no block by itself; each node has a block finder that solves
  a block on that node's work when a scenario asks, once the node has settled
  on its tip and payout revision and the finder holds freshly built work.

## Topologies

| Topology | Databases | Frontends |
| --- | --- | --- |
| `SingleWriter` (3.0) | A primary, B its asynchronous streaming standby | both write A's, B's across the writer link |
| `DualWriter` (3.1) | two independent primaries | each writes its own and pulls its peer's rows |
| `Unsynced` | two independent primaries | each writes its own; nothing between them (the checker's negative control) |

## Faults

| Fault | Injection | Heal |
| --- | --- | --- |
| frontend kill -9 | SIGKILL | restart, wait until ready |
| frontend freeze | SIGSTOP (sockets and locks held) | SIGCONT |
| PostgreSQL kill | SIGKILL the postmaster and every backend | start: crash recovery |
| PostgreSQL freeze | SIGSTOP the postmaster and every backend | SIGCONT |
| network drop | cut every link of the node, turn its `qbitd`'s network off | reopen, reconnect |
| link cut | cut only the links between the databases | reopen |

A cut link either resets every connection (a host that is gone) or becomes a
blackhole (a dead switch: nothing forwarded, nothing closed, and the held
bytes delivered on heal as TCP would). A link can also discard: every byte is
read and thrown away and no close is passed on, so a server's replies to a
dead client drain instead of blocking on a full window (S2's puller death).
An open link passes each side's close on as it came, a FIN as a FIN and a
reset (or a socket error) as a reset that ends the connection both ways.
Any link can carry latency: S2's freeze and puller-death checks put 40 ms
each way on A's link to B, a path between two sites, so a pull that holds
the sync barrier or a transaction across round trips holds it long enough
to be caught. Bytes and closes alike wait the latency, and a stream keeps
its rate.
The relays are user-space, so a blackhole is silence at the application
layer; TCP keepalives between each endpoint and its relay still succeed.

Restoring an older base backup and replacing a disk are scenario steps, as are
the locks held and grants revoked on a database (S4's transient apply, S6's
unreadable peer).

## Invariants (CONTRACT.md §4)

The census of pool blocks comes from the chain (C's active chain, every
block whose coinbase carries `/PRISM/`), never from a database's
`chain_state`. A block's origin is the node that issued its job.

| Check | Holds when |
| --- | --- |
| `inv1-no-overpay` | per account, summing `gross - onchain` over the as-issued carry rows of the census in height order, no block raises `max(0, -balance)`, except an increase the owner recorded as #478 with enough `overpay_sats` |
| `owner-balances-match-chain` | the owner's `qbit_current_carry_forward_balances()` equals those sums |
| `inv2-non-owner-carry-free` | every non-owner block has an empty prior set and digest, and `onchain <= gross` for every miner, in its audit and its carry rows |
| `inv3-landing-rows` | every census block has its block, audit bundle and snapshot, payout entries, carry rows and fanouts in every database, identical across them |
| `inv3-audits-verify` | each node serves the block's bundle, `qbit-prism-audit-verify` accepts it against the chain's coinbase with the pinned ledger key, the manifest key is the pinned one, and both nodes serve the same bundle |
| `inv3-fanouts-identical` | each database holds the bundle's fanout transactions byte for byte |
| `inv4-acked-shares-present` | every share a miner saw accepted is in every database, except a documented tail the scenario names |
| `inv4-no-double-credit` | one row per share id and per header, every row mapped to its header, and a header's row identical on both nodes |
| `inv4-windows-unchanged` | every recorded window still has exactly its shares, and no newer share is eligible for it |
| `inv4-windows-reproducible` | every recorded window recomputes to its digest from every database |
| `ledger-integrity` | `qbit_carry_forward_integrity_report()` is clean everywhere |
| `candidates-settled` | no block candidate is left unfinished, except one a scenario names as kept for reconciliation with no landing (S8's lost block) |
| `d2-local-state-not-copied` | dual-writer pairs, for rows written since the pair began writing as two primaries: no node holds a candidate its peer's frontend claimed or reserved; no candidate or offer decision is the same row on both nodes (same insert timestamp); no node holds an offer decision for a block it never had as a candidate (only the offering frontend records one); no candidate from before a cutover was reserved after it; each database identifies as its own node (D-2, D-9). After S7's rebuild this is D-16's reset |

The negative control (`checker-control`) runs two unsynced single writers
that both mine and checks that the checker fails exactly the checks that must
fail and passes the rest.

S1 and S5 also sample the sync order while they run (D-5: shares before
landings; D-10: a block arrives with all its child rows). A sampler takes the
peer blocks already on a node as its baseline before the scenario goes on,
then polls every 100 ms for new ones and, in the same statement, counts each
one's window shares there. A block counted short fails (shares only
accumulate, so a later short count proves it too), and so does a block seen
without its audit bundle or window snapshot. Each direction needs at least
one block seen whole on a poll that followed a good one. A block first seen
after a failed poll may have arrived during it, so a complete count then is
reported as unresolved; failed polls are counted and listed. The sampler
cannot see an out-of-order landing shorter than its poll interval.

## Scenarios (CONTRACT.md §5)

| Id | Test | Lanes | Needs |
| --- | --- | --- | --- |
| control | `checker_control_*` | PR, nightly | today's code |
| S1 | `s01_steady_state_*` | PR, nightly | the 3.1 stack |
| S2 | `s02_a_frontend_killed_*` (PR), `s02_a_postgres_killed_*`, `s02_a_frozen_*`, `s02_a_network_dropped_*` | PR, nightly | the 3.1 stack |
| S2, mid-pull | `s02_a_frozen_again_and_again_mid_pull_*` | nightly | the 3.1 stack |
| S2, puller death | `s02_a_puller_dying_mid_read_*` | nightly | single-statement peer reads (D1) |
| S3 | `s03_b_dies_*` | nightly | the 3.1 stack |
| S4 | `s04_link_cut_*` | PR, nightly | the 3.1 stack |
| S4, transient apply | `s04_a_transient_failure_applying_bs_block_*` | nightly | retried peer blocks (D1) |
| S5 | `s05_both_nodes_writing_*` | PR, nightly | the 3.1 stack |
| S6 | `s06_a_restored_*`, `s06_b_restored_*` | nightly | own-log recovery and the D-8 latch |
| S6, unreadable peer | `s06_a_restarts_and_serves_while_b_answers_*` (locked, refused) | nightly | the latch without a readable peer (D1) |
| S7 | `s07_b_disk_replaced_*`, `s07_a_disk_replaced_*` | nightly | `node-identity repersonalise` (D-16) |
| S8 | `s08_a_block_lost_*`, `s08_a_block_accepted_*`, `s08_without_the_peer_ingest_wait_*` | nightly | adoption (D-10, D-11) and the D-19 wait |
| S9 | `s09_a_3_0_ledger_cut_over_*` | nightly | the 3.1 stack |
| S10 | `s10_single_writer_*` | PR, nightly | today's code |
| S11 | `s11_carry_owner_transfer_*` | nightly | `carry-owner release` and `transfer` |

What each one does:

- **S1:** every miner on A; A's shares reach B within 2 s (p95), its blocks
  land and confirm on B, each peer block's window is complete on B the moment
  it arrives (D-5), and both nodes report their identity.
- **S2:** A dies (kill -9 of the frontend, PostgreSQL kill -9, SIGSTOP, or its
  whole network blackholed); miners reach B within 30 s, B finds two
  carry-free blocks, A's unsynced tail is measured, A returns, confirms B's
  blocks from its own chain view, and its next block pays the carry they
  accrued (its priors equal the chain's sums). With its PostgreSQL killed,
  A's frontend is still up and must withdraw on its own: sampled every
  200 ms from before the kill, it must stop answering ready for good within
  12 s (D4 withdraws a writer unanswered for 4 s; its stale-decision
  backstop would take 14 to 15 s), and the mark-down must follow within
  12 s.
- **S2's freeze** holds A frozen for at least 60 s while B finds a block every
  few seconds; each must land and be followed by new jobs of B's before A
  thaws (a frozen peer must never stall the survivor's writes).
- **S2, mid-pull:** both nodes take miners; A's frontend is frozen 20 times
  for 1 to 4 s; new-tip rounds aim first at a moment its puller holds D1's sync
  barrier on B's database, else at one it has a query or a transaction open
  there. During each freeze
  B must keep writing: in 16 of them a new tip arrives and B must record jobs
  on it within 15 s; in every fifth, B lands a block solved on work handed out
  before the freeze and must build work on it. D1's sync barrier, held by a frozen
  puller, would stall exactly these writes. The schedule's seed and every
  round are in `freezes.json`.
- **S2, puller death** (D1 engine review, P1 2): both nodes take miners at
  250 shares/s. For 20 s, B is sampled every 10 ms and must never show A's
  peer backend idle in transaction. Then A's database link discards (B's
  replies drain and no close reaches it, as when A's host dies; a plain kill -9
  sends a FIN), and A's frontend is killed while its puller has a transaction
  open on B. A's backends on B must hold no snapshot, transaction or lock beyond
  10 s, every miner
  session must have shares accepted on B late in the minute after the death,
  and B's share answer latency must stay flat (p95 45 to 60 s
  after the death at most twice the p95 5 to 20 s after A's mark-down, plus
  50 ms).
- **S3:** B's host dies (frontend and PostgreSQL); A's miners see no gap above
  2 s, and B catches up on return.
- **S4:** the databases' link blackholed with both nodes up: no gap at the cut
  or the heal, the balancer keeps both nodes up from the cut until a full
  mark-down's worth of checks after the catch-up (D-8: a later link loss never
  withdraws a serving node; checks that failed while a node stayed up are
  counted), A's block is absent from B until the heal, then the sync catches
  up.
- **S4, transient apply** (D1 engine review, P1 3): A holds EXCLUSIVE on its
  audit bundles (landing inserts time out at 5 s, reads go on) while B lands a
  block, and for 12 s more; at least one of A's applies must be seen waiting on
  the lock. Once the lock clears, the block must land and confirm on A, with no
  sync conflict recorded for it: retried, never skipped.
- **S5:** round-robin routing, both nodes write; four interleaved blocks land
  on both nodes, each window complete on arrival, B's blocks carry-free.
- **S6:** a plain PostgreSQL restart with the peer unreachable serves (D-17); a
  restore onto a new timeline with the peer unreachable stays unready for
  20 s (D-8, D-17); after the heal the node holds every own row the peer
  holds at its first ready reading.
- **S6, unreadable peer** (D1 engine review, P1 1): B accepts the peer role
  but every read of its share ledger hangs (an ACCESS EXCLUSIVE lock) or fails
  (SELECT revoked). A is restarted with its database untouched and must report
  ready and take miners within 120 s (D-8, D-17); then B is healed and the
  pair catches up.
- **S7:** the node's host dies, its disk is wiped and rebuilt as a promoted
  physical copy of the peer, re-personalised (D-16); only its measured tail is
  excused.
- **S8:** A dies at the instant it finds a block: with the `submitblock` held
  (the block never reaches the chain, and A may keep its candidate for
  reconciliation, never offering it again) or answered by the node and withheld
  (B adopts it; once A returns each node holds one landing of it), with D-19's
  wait on and off.
- **S9:** the 3.0 pair mines a history, is cut over live (drain, with no
  candidate left that could still be offered, promote B, migrate, identity,
  B then A in dual mode); both ledgers equal the 3.0
  writer's, every pre-cutover row is node 0, a single-writer start is refused
  (D-12), the owner's first block pays from the 3.0 balances.
- **S11:** A releases ownership while B lacks one of A's blocks; B's transfer is
  refused by the chain scan, succeeds once B has landed the block, and B then
  pays carry while A builds carry-free work.

A scenario joins its lanes' lists (`test/prism-dual-writer-gated-tests.txt`,
`test/prism-dual-writer-nightly-gated-tests.txt`) and runs in
`test/e2e-scenarios.toml` once the 3.1 stack's branches make it runnable;
until then each lane names it in its summary as not run yet.

## Bounds and retries

Every wait is bounded and named where it is made, and none retries the
behaviour under test:

- **Setup budgets** (a frontend ready in 180 s, a block landed in 60 s, the
  settle in 180 s) are generous on purpose: missing one fails the run with
  what it waited for, and the time taken is reported.
- **Bounds under test** are expectations with their measured values: the
  miner-visible gap after a node dies is at most 30 s, the balancer marks a
  dead node down within 10 s.
- **Retries that exist** are the system's own or bounded setup steps: a
  session reconnects every 250 ms; a `qbitd` peer connection is retried with
  `addnode onetry` until it holds (30 s); C mints only once it holds every
  connected node's tip (30 s), so a mint never forks away a pool block on its
  way; and a scheduled block is solved again, at most 3 times, when the work
  it was solved on turned out superseded by a tip or payout-revision change
  (refused as `stale-job`, or accepted under the stale grace without
  becoming a candidate). Each retry is on the scenario's timeline.
- **Clocks.** Only C runs on a mock clock, during the ramp and the catch-up
  that walks a cached chain up to the wall clock. A and B keep the wall
  clock: a jump of theirs would expire their block downloads and drop the
  peer.

## Running it

```sh
# PostgreSQL 16 server binaries and the pinned qbitd:
bash .github/scripts/install-prism-qbit.sh "$HOME/.prism-qbit"
export PRISM_TEST_PG_BIN_DIR=/usr/lib/postgresql/16/bin
export QBITD_BIN="$HOME/.prism-qbit/qbit-1.0.0/bin/qbitd"
# A short TMPDIR for the data directories, ideally on tmpfs (CI's
# PostgreSQL runs on tmpfs too; a busy shared disk slows qbitd's start):
export TMPDIR=/run/user/$(id -u)/dual-sim
cargo test -p qbit-prism-dual-sim --test scenarios -- \
  --ignored --exact --test-threads=1 --nocapture \
  s10_single_writer_pair_keeps_every_share_and_pays_exactly_through_a_frontend_kill
```

The test builds `qbit-prism-server` and `qbit-prism-audit-verify` into the
workspace's target directory first. Each scenario writes `report.md`,
`report.json`, `invariants.json`, `shares.jsonl`, the audit bundles it
verified and every process's log under `target/dual-sim-reports/<scenario>/`.
A failed scenario keeps its data directories under `$TMPDIR/dual-sim/`.

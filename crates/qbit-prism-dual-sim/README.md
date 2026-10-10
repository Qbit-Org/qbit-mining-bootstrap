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
bytes delivered on heal as TCP would). The relays are user-space, so a
blackhole is silence at the application layer; TCP keepalives between each
endpoint and its relay still succeed.

Restoring an older base backup and replacing a disk are scenario steps.

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
| `candidates-settled` | no block candidate is left unfinished |

The negative control (`checker-control`) runs two unsynced single writers
that both mine and checks that the checker fails exactly the checks that must
fail and passes the rest.

## Scenarios (CONTRACT.md §5)

| Id | Test | Lanes | Needs |
| --- | --- | --- | --- |
| control | `checker_control_*` | PR, nightly | today's code |
| S1 | `s01_steady_state_*` | PR, nightly | the 3.1 stack |
| S2 | `s02_a_frontend_killed_*` (PR), `s02_a_postgres_killed_*`, `s02_a_frozen_*`, `s02_a_network_dropped_*` | PR, nightly | the 3.1 stack |
| S3 | `s03_b_dies_*` | nightly | the 3.1 stack |
| S4 | `s04_link_cut_*` | PR, nightly | the 3.1 stack |
| S5 | `s05_both_nodes_writing_*` | PR, nightly | the 3.1 stack |
| S10 | `s10_single_writer_*` | PR, nightly | today's code |

A scenario joins its lanes' lists (`test/prism-dual-writer-gated-tests.txt`,
`test/prism-dual-writer-nightly-gated-tests.txt`) once the 3.1 stack's
branches make it runnable. S6 to S9 and S11 follow the stack's procedures
(CONTRACT.md D-8, D-10 to D-17, D-19).

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

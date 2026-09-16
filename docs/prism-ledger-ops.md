# PRISM Ledger Operations

This is the operating contract for the native Rust Prism server. The database
retains the original accounting tables and functions while additive migrations
provide active-active application instances. For the upgrade procedure, see
[Rust migration](prism-rust-migration.md).

## Share commit and ordering

All instances insert accepted shares into `qbit_share_ledger` in one PostgreSQL
database. A short transaction-scoped advisory lock orders insertion and snapshot
creation. PostgreSQL assigns the canonical `share_seq`; sequence gaps are
permitted, and sequence order is authoritative. Local arrival timestamps,
frontend counters, and per-worker summaries do not determine reward order.

The submission path validates identity, job, header, and target before durable
admission. A normal successful Stratum response follows the database commit,
with `synchronous_commit=on`. A share meeting the network target also persists
its complete candidate intent atomically with the share. Connection loss after
commit can lose the reply without losing the share.

An exact replay returns the existing share without another reward credit.
Conflicting reuse of a share identifier fails. The native global proof-hash
registry prevents the same newly submitted proof receiving separate credits
under different usernames or on different servers. Historical rows are retained
unchanged during migration, including any duplicates accepted by older code.

There is no Python batch queue or process-wide ledger writer lease. All healthy
instances may append; transaction locks protect a shared ordering boundary.
Database connection and statement/lock limits bound resource use. Session
extranonce allocation also uses the shared database sequence and never cycles.

## Snapshot and payout boundary

`qbit_prism_window(anchor_job_issued_at, window_weight)` selects eligible shares
newest first by `share_seq`, counting a partial oldest share when the requested
weight is reached. Both `job_issued_at` and `accepted_at` must be no later than
the anchor. The audit wrapper fixes the reward weight to eight times network
difficulty.

Native snapshots record a database-coordinated monotonic anchor, share range,
and payout revision. Share acceptance and snapshot creation use the same
ordering lock, so equal wall-clock timestamps or host clock differences cannot
let a later share enter an earlier snapshot. Published jobs bind their snapshot;
later accepted work cannot change their committed coinbase.

Payout-changing operations serialize under the settlement transaction lock.
They advance the shared payout revision, and job publication checks that its
revision remains current. A cluster fingerprint binds genesis, ledger and
manifest public keys, reward multiplier, payout policy, and CTV policy. A
mismatched instance fails startup. Coordinators can use different local resource
limits and synchronized qbit nodes on the same chain.

Compact issued-job hot writes use a per-Coordinator collector: at most 128
admitted children (pending plus active), 64 children per transaction, and one
active batch including cancellation cleanup. Collection dwells for up to 1 ms
from the oldest admission when storage is available, flushing earlier when full
or a deadline requires it. Time behind another batch and admission backpressure
remain inside the original persistence deadline. These bounds are initial
engineering choices, not evidence of a latency target or a speedup.

Groups share the complete original compact dependency identity and the expected
current revision and parent; each Coordinator is bound to its own ledger/frontend.
Only small owned child metadata is queued. Prepared reservations, inline/direct
single-job APIs, and missing-dependency cold repair keep their existing paths.
Hot batches retain `SETTLEMENT_LOCK`, then cluster `FOR SHARE`, prepared
`FOR KEY SHARE`, and template/balance `FOR KEY SHARE` locks in that order.
Shared revision, configuration, writable-state and dependency checks happen
after the row waits. Children and any retention extension through the largest
child expiry plus existing headroom commit atomically; original reservation
identity and each child's absolute expiry never change.

A conflicting child fails its whole transaction, including renewal. Other
groups can succeed independently; no SQL failure is silently replayed. Canceled
queued children are discarded. Once a batch is active, it continues while any
member still waits: an individually canceled child's immutable metadata may
commit undelivered alongside live peers, with its original expiry unchanged.
Cancellation of all active members interrupts the shared attempt; before COMMIT
this rolls back every child, including the singleton case. The minimum original
deadline and earliest child expiry still govern the whole atomic batch, even
when the earliest member has canceled. These limits can fail otherwise live
peers; avoiding that would require a different transaction partition or a retry.
A batch-local statement limit preserves stricter session
settings and otherwise caps statements at 15 seconds or the remaining original
deadline. An interrupted attempt drains rollback before starting the next batch;
cleanup is bounded at 16 seconds and discards an unresponsive connection.
Dropping the Coordinator closes admissions and resolves pending waiters.
COMMIT already started means an uncertain outcome after cancellation or lost
acknowledgement, never proof of rollback. Reconciliation must use the exact
original IDs, payloads and expiries. Committed, undelivered children retain their
dependency through their original expiries with the existing renewal headroom,
including when every caller cancels during COMMIT. Cancellation does not delete
committed metadata, undo its retention, or restart its expiry; normal pruning
still applies. An unexpired committed child also keeps its payload's
`extranonce1` referenced, preventing session allocation from reusing that value
until the child's original expiry even when its caller has canceled.
A durable row may remain undelivered after
authority revocation: the original caller still revalidates after persistence,
and Stratum never sends work before successful durable commit and revalidation.

The batch debug event records actual shared storage-attempt elapsed time and
cardinality, including pool and lock waits; cleanup is separate. Concurrent pool
and advisory wait totals overlap and cannot be subtracted to infer exclusive
service time. The one-second delivery, two-frontend non-regression, and lock-free
criteria of #275 remain unqualified by this batching change.

A pool with no historical shares issues solver-paid bootstrap work. There is no
three-miner gate. A network-valid candidate below its assigned share target is
stored without ordinary share credit. Active-chain confirmation inserts its
deferred share at network difficulty; a losing candidate receives no credit.

## Durable block candidates

`qbit_block_candidate_outbox` stores complete candidate evidence before a node
submission can be lost to a process crash. Workers claim unfinished rows with
expiring, token-fenced database claims. Multiple instances may process different
candidates; a stale claimant cannot overwrite a successor's accounting result.
Terminal candidates retain the evidence needed for replay identity while large
payloads are released. Deferred below-share-target credit is tied to the same
durable candidate lifecycle.

Since migration 011 a claim runs in two phases, the node offer first and the
accounting after it, and the row records where the block is between them:

| State | Meaning |
| --- | --- |
| `pending` | Durable and never offered. The only state a claim may offer from, and the only one that may still be abandoned: a block proven superseded before it was ever offered. |
| `offer_reserved` | The claim took the durable reservation immediately before its one `submitblock` call. The row is the unique reservation per block hash: once it commits, no claim on any frontend offers the block again, this frontend included after a crash. |
| `offered` | The node's answer is recorded in `offer_outcome` (`accepted`, `rejected` with the node's reply in `offer_reply`, or `unknown`) with the call time; the audit is still to be landed. |
| `reconciliation` | Offered, and automation could not finish it: an unknown outcome (a transport failure or timeout, a reservation whose call was lost with its frontend, or a pre-011 attempt 011 quarantined), a node rejection, a landing that failed after acceptance, a node or database error after the offer, or a block not on the active chain yet. `last_error` holds the reason. Retried `min(3600, 10 × attempt_count)` s apart with read-only chain observations only, never another `submitblock`, and never abandoned; settled terminal as `orphaned` once the chain proves a competitor at its height. |
| `orphaned` | Terminal (migration 015, #415). Reachable from the three offer states only. One coherent read-only observation proved a *different* block active at the candidate's height with at least `PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS` confirmations (default 6, counted as `tip_height − height + 1`), after the row's audit landed: the lost tip race of the 2026-09-16 mainnet orphan stall (#413). `last_error` names the competitor, the height, the confirmations and the tip. Like the other terminal states, the row releases its document, block bytes and window reference, so terminal history does not pin candidate payloads or balance snapshots. It preserves its offer metadata and reason; the landed audit and pool-block row retain the accounting evidence. Never claimed again, never offered again, and no longer counted by `qbit_prism_block_candidates_pending` / `qbit_prism_block_candidate_oldest_pending_seconds`; `qbit_prism_block_candidates_orphaned_total` counts completions observed by this process after commit and may undercount if cancellation or restart intervenes. The block's `qbit_pool_blocks` row is marked `inactive` and keeps its landed audit, so a later reorg that reactivates the block is confirmed and credited (deferred share included) by the ordinary reorg reconciler, from that preserved evidence, without this row ever reopening. `orphaned` describes the completed outbox processing decision, not the block's permanent chain status. |
| `submitted` | The block was proven on the active chain and its audit landed. The document, the block bytes and the window reference are released; the offer record stays. |
| `abandoned` | Reachable from `pending` only. |

### Candidate commands

Three operator commands read and finish the rows above. Neither `list` nor
`abandon` builds a node client, reads a signing key, loads the server
configuration or starts a listener, so neither can offer a block. `list`
needs only `PRISM_DATABASE_URL`, as `fatal-state show` does. `abandon` writes
an ordinary ledger row, so it uses the one-shot tool connection and the
stored-data settings behind it — `PRISM_DATABASE_URL`, `PRISM_INSTANCE_ID`
(generated when unset) and `PRISM_DATABASE_MAX_CONNECTIONS`. `recover` is the
one of the three that reads the node, and is described after the other two.

```sh
qbit-prism-server candidates list [--json] [--limit <1..10000, default 100>]
qbit-prism-server candidates abandon --block-hash <64 lowercase hex> --reason "<nonblank explanation>"
qbit-prism-server candidates recover --block-hash <64 lowercase hex> [--block-hash <hash> ...] [--apply] [--timeout-seconds <1..3600, default 600>]
```

`list` prints unfinished rows up to the selected limit — `pending`, `offer_reserved`, `offered`
and `reconciliation` — oldest due first. That is the oldest-due claim lane's
own ordering, so the row a server works next is the first line and parked
rows sort last. It is not ordered by height, which is read out of the
candidate document and may be unknown.

```
block_hash                                                        state           height  attempts  next_attempt                      claim                                                    sv  last_error
7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a  pending         184021  0         2026-09-16T15:03:10.792447+00:00  -                                                        1   -
5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f  offered         184020  2         2026-09-16T15:03:28.793326+00:00  expired                                                  1   -
3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c  offer_reserved  184022  1         2026-09-16T15:03:43.793265+00:00  prism-frontend-b until 2026-09-16T15:03:50.793265+00:00  1   -
9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e  reconciliation  184018  7         2026-09-16T15:04:08.793284+00:00  -                                                        1   submitblock reply was lost with the offering frontend; awaiting an autho…(truncated)
d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4  pending         184019  3         parked                            -                                                        1   block digest did not authenticate against candidate_sha256
b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1  pending         -       1         parked                            -                                                        3   candidate storage_version 3 is not supported by this server; only versio…(truncated)
```

| Column | Meaning |
| --- | --- |
| `block_hash` | The whole 64-character hash, so it can be pasted straight into `candidates abandon --block-hash`. |
| `state` | The lifecycle state from the table above. Terminal rows are finished work and never appear. |
| `height` | `candidate->'found_block'->>'block_height'`, extracted server-side. `-` (text) or `null` (`--json`) when the document holds no readable height — an unknown-`storage_version` row, for example. Never `0`. |
| `attempts` | `attempt_count`: how many claims the row has taken, not how many offers were made. |
| `next_attempt` | `next_attempt_at`, or `parked` when it is `infinity`. |
| `claim` | `<claim_instance_id> until <claim_expires_at>` for a live claim, `expired` for a claim past its expiry (the row is workable again), `-` for none. `--json` keeps the stale holder and expiry. |
| `sv` | `storage_version`. Anything other than `1` is a row this server cannot decode. |
| `last_error` | Why the last attempt stopped. Truncated in text mode only, and marked `…(truncated)` when it is; `--json` prints it whole. |

**Parked is not abandoned.** A parked row has `next_attempt_at = 'infinity'`
and a `last_error`, which is exactly what the `parked` cell means. It is still
unfinished: the claim lane moved it out of the retry schedule because this
server could not decode or validate it, and it keeps its state, its document,
its block bytes and its window reference. It is operator work, not a retry
loop, and it is not terminal until something finishes it.

`list` never loads a payload. `candidate` and `block_bytes` are not in its
projection, so the command stays usable on a production-sized row. It opens a
`default_transaction_read_only=on` pool with one connection, a 15 s statement
timeout and a 5 s lock timeout, which is why it takes no claim and why it
keeps working while the cluster is halted — precisely when it is needed. An
empty list is a **success**: the command prints `no unfinished candidates` (or
an empty `candidates` array) and exits zero, so a `set -e` runbook can wait for
exactly that. `--json` prints one
`{"schema":"qbit.prism.candidates.list.v1","candidates":[...],"limit":100,"truncated":false}` document with
every field untruncated and every unknown value as `null`. The inventory is limited
to `--limit` rows (default 100, maximum 10000). Both modes fetch one extra row to
detect omitted candidates: JSON sets `truncated: true`, while text mode writes a
warning to stderr. Parked rows sort last and may be among the omitted rows. Raise
`--limit` to inspect more; inventories larger than 10000 require a read-only
database query. A full page is complete only when `truncated` is false. These
fields describe the query's snapshot, not rows arriving after it.

`abandon` finishes one `pending` row with `storage_version = 1` **whose
document this release could replay**. Other storage versions are refused
atomically and their evidence is preserved, and so is a version-1 row holding a
pre-migration `2.x.x` document: the native claim lane parks one of those at
version 1 rather than rewriting it, so the version alone does not say who wrote
the row. The statement's shape test is the migrator's own — a native document
carries `payout_revision` and `block_hash` beside either an inline `bundle`
(pre-007) or a `window` reference (007 and later) — and anything else is
refused with exit 8 and every column left as it is. That block is still owed to
[the legacy drain](prism-rust-migration.md#two-drains-one-for-each-era), which
is the only thing that can finish it. `pending` is
the only state it touches, because it is the only unfinished state from which
no `submitblock` can yet
have been made: a row in `offer_reserved`, `offered` or `reconciliation` is the
record that a call may already have happened, and discarding it would discard
that record. The rule is the statement's `WHERE` clause, not a check the
command makes first, so a row that moves between reading and writing is still
refused. Before evaluating these predicates, `abandon` takes the settlement
lock shared with candidate landing. If a landing is still in flight when its
claim expires, the command waits, then sees the committed accounting and
refuses with exit 6. A successful abandon releases the document, the block
bytes and the six window columns, clears the claim, sets `completed_at`, and writes the
operator's `--reason` into `last_error` — exactly the columns the offline
epoch supersession writes. `next_attempt_at` is deliberately left as it is: no
lane selects a terminal row, so its value is inert, and an `infinity` left
there stays as evidence that the row had been parked.

| Exit | Outcome | Message |
| --- | --- | --- |
| 0 | Abandoned. | `abandoned <hash>: <reason>` |
| 1 | Configuration or database failure, including the two fences below. | The underlying error, as for every other command. |
| 2 | No such row. | `no candidate row for <hash>` |
| 3 | Offered to the node; never abandonable. | `candidate <hash> is in state <state>; it was offered to the node and is never abandoned. Its block may already have been submitted. Leave it to reconciliation` |
| 4 | Already terminal. | `candidate <hash> is already <submitted\|abandoned\|orphaned>; nothing to do` |
| 5 | Held by a live claim. | `candidate <hash> is held by <instance> until <expiry>; retry after the claim expires` |
| 6 | Pending, but its block has landed. | `candidate <hash> is pending but its block is already in qbit_pool_blocks; reconcile it before abandoning — abandoning would discard landed accounting` |
| 7 | Unsupported storage version; evidence preserved. | Names the version and directs legacy rows to the pinned `2.x.x` drain, newer formats to a compatible release. |
| 8 | A pre-migration `2.x.x` document parked at `storage_version = 1`; evidence preserved. | `candidate <hash> holds a pre-migration 2.x.x document at storage_version 1; evidence preserved. This release cannot replay it, and abandoning it would discard the block the legacy drain still owes: drain it with the pinned 2.x.x image, never an operator abandon` |

Codes 3, 6, 7 and 8 protect offer, accounting, storage-format and legacy-era
evidence. Code 4
is kept distinct from code 2 so that re-running a successful abandon reads as
"nothing to do" rather than as a lost row. An **expired** claim is not a live
claim, so a supported row whose owner died is abandonable without waiting;
code 5 reflects whether the claim was live at one database timestamp captured
after acquiring the settlement lock. The UPDATE and refusal diagnosis share that
timestamp, so expiry between them still reports a claim refusal rather than an
internal consistency error; a fresh invocation can abandon the now-expired row. Unsupported versions report
code 7, and an unreplayable version-1 document code 8, both ahead of claim
status: a claim expires on its own, and neither a storage format nor a document
shape becomes replayable by waiting. A pending row whose block has landed always
reports code 6, because the accounting must be reconciled.

`abandon` connects as a one-shot tool and writes through the ordinary ledger
write transaction, so it is fenced twice, both reported as exit 1:

- **A halted cluster.** While `qbit_prism_cluster.fatal_error` is set,
  `abandon` is refused at connect with `cluster halted: ...`, exactly as a
  frontend would be, and the write guard would refuse it again. Use
  [fatal-state recovery](#fatal-state-recovery) first. `list` is unaffected and
  stays available throughout.
- **A live legacy Python writer lease.** While a `qbit_ledger_writer_lease` row
  has not expired, the write fails with `live legacy Python writer lease`. This
  is what stops an operator abandon racing the `2.x.x` writer during a cutover.

Like the other one-shot commands, `abandon` writes no heartbeat: it leaves no
`qbit_prism_instances` row for `fatal-state clear` to refuse, and a live
frontend that shares its configured `PRISM_INSTANCE_ID` keeps its status and
session-owner token untouched.

`recover` lands a native-era block the node has already accepted and the
claim lane cannot finish — a window whose rebuild exceeds the lane's 60 s
rebuild deadline, for example, so the row sits in `reconciliation` retrying
forever. `abandon` is correctly refused for every state the node may already
have been offered, so before #418 such a row had no operator path. `recover`
lands it at the proven chain revision, under an explicit allowlist, through
the same in-process machinery the coordinator uses
(`land_candidate_at_revision`, driven the way `process_candidate_inner`
drives it for an offered row), and it never calls `submitblock`. It starts no
listener. The invariant is #268's: no operator command may discard or
duplicate the effect of a `submitblock` that may already have been made.
`recover` lands an already-accepted block; it never offers one. `abandon`
stays `pending`-only; `recover` does not widen it and adds no `--force`, and
it adds no candidate state and no column.

`recover` is the only one of the three that reads the node, and what it
loads depends on the mode. The plan needs `PRISM_DATABASE_URL` plus the node
RPC settings — `QBIT_RPC_URL` (or `QBIT_RPC_HOST`/`QBIT_RPC_PORT`),
`QBIT_RPC_USER`, `QBIT_RPC_PASSWORD` and `PRISM_RPC_TIMEOUT_SECONDS` — and no
signing seed, no chain setting and no instance ID. `--apply` needs the
frontend's full configuration, exactly as `self-check`, `broadcast-ctv` and
`fatal-state clear` do: database, qbit RPC, `QBIT_CHAIN`/genesis, payout
policy and the signing seeds, because the audit is rebuilt from the
candidate's stored inputs and signed with this frontend's seeds, and the
command verifies the node's genesis/chain and the cluster fingerprint at
connect. Run it from a frontend's environment. Setting `PRISM_INSTANCE_ID`
(for example `operator-recovery-INC-123`) is recommended, so that
`candidates list` names the recovery as the claim holder while it runs.
Frontends do not need to be stopped: the durable claim is the fence. A row
another instance holds is refused (exit 5), and while the recovery holds a
row no frontend's claim lane can take it. (`2.x.x` required stopping the
coordinator because its runner used the writer lease; the native command
does not.) `--apply` connects as a one-shot tool (`Ledger::connect_tool`,
#412), as `abandon` does: no heartbeat, no `qbit_prism_instances` row left
behind, a halted cluster refused at connect with `cluster halted: ...`
(exit 1), and every write refused while a legacy Python writer lease is live
(exit 1).

**Plan.** Without `--apply` the command plans and writes nothing, ever. It
reads the outbox rows for the listed hashes on a
`default_transaction_read_only=on` pool with one connection, the shape
`list` uses, so taking a claim is impossible by construction. It loads no
payload: `candidate` and `block_bytes` are not in the projection, and the
stored height is extracted server-side exactly as `list` extracts it. It
calls the node read-only — `getblockheader` for each hash and `getblockhash`
at its height — to prove each block is on the active chain and to learn its
parent, and it prints the selection ordered by height ascending, parent
before child, one table and one summary line on stdout:

```
block_hash                                                        height  state           parent                                                            claim                                              action
<hash>                                                            184018  reconciliation  <parent hash>                                                     -                                                  recover
<hash>                                                            184019  pending         <parent hash>                                                     prism-frontend-b until 2026-09-16T15:03:50+00:00   recover
<hash>                                                            184020  submitted       <parent hash>                                                     -                                                  complete
plan: 2 to recover, 1 already complete; rerun with --apply to land them
```

`action` is `recover` for an unfinished row the node proves active, or
`complete` for a `submitted` row whose accounting is proven — a `confirmed`
`qbit_pool_blocks` row and an audit row — which `--apply` verifies and
skips. `claim` is rendered as in `list`: `<instance> until <expiry>`,
`expired` or `-`. A live claim is reported in the plan but does not fail it;
it is a moment-in-time fact, and `--apply`'s own claim is the fence. When
nothing is left to recover the summary reads `plan: nothing to recover;
every listed block is already complete`. The plan exits zero whenever the
selection is valid.

The selection fails closed: every listed hash must pass, or the whole plan is
refused. The plan reports every problem it finds on stderr, one line each,
and exits with the code of the first one, in argument order. The allowlist
itself is checked at the entry boundary, before any connection is opened:
one to 32 hashes (the bound of the `2.x.x` runner, #259), each 64 lowercase
hex characters, no duplicates. Zero hashes or an out-of-range
`--timeout-seconds` is refused by the argument parser (exit 2, as for every
command); a duplicate, more than 32 or a malformed hash is refused by the
command itself (exit 1). There is no "recover everything" mode.

**Apply.** `--apply` runs the plan first, and a refused plan applies nothing.
Then, for each planned block in height order:

1. A block the plan marked `complete` rechecks its current outbox state,
   confirmed accounting, audit presence and active-chain membership before
   printing `verified <hash> at height <h>: already complete` on stdout and
   skipping it idempotently. A changed or unproven result stops the command
   without claiming that block or attempting later blocks.
2. Otherwise the command prints `recovering <hash> at height <h> from
   <state>` and takes the durable, token-fenced claim on that row by hash,
   through the existing claim mechanism: `attempt_count` increments,
   `claim_instance_id` and `claim_expires_at` name this process, and the
   ordinary lease heartbeat renews the claim while the work runs. The claim
   statement's `WHERE` is the safety property — an unfinished state, no live
   claim, `storage_version = 1` and a document this release wrote, the same
   shape test `abandon` uses. A row parked with `next_attempt_at =
   'infinity'` is claimable here, because a parked row is operator work,
   but its schedule is left untouched.
3. It decodes and authenticates the row exactly as the claim lane does
   (document digest, block digest, window columns, header hashes to
   `block_hash`), then verifies the block against the node: it is on the
   active chain at its height, the node's header height equals the
   candidate's recorded height, and the node's `previousblockhash` equals
   the parent in the candidate's block bytes.
4. A `pending` row is first adopted into `reconciliation` with the node's
   evidence, exactly as the coordinator's pre-offer probe adopts an active
   pending block. From that commit on, no claim on any frontend can ever
   offer it, whatever happens next.
5. It runs the normal post-offer landing: builder admission, the audit
   rebuild from the as-issued balance snapshot (or the current balances when
   they still hash to the reference), signature and coinbase verification,
   the durable range proof, and `land_candidate_at_revision` at the chain
   revision observed immediately before the landing transaction. An audit
   that already landed is authenticated against the block and not rebuilt
   over.
6. It observes the chain again and finishes the row as `submitted` at a
   revision proven now — the same `finish_candidate_at_revision` the
   coordinator uses — which confirms the pool block and credits any deferred
   share, then prints `recovered <hash> at height <h>`.

The final line is `recovered N, verified M already complete`, and the command
exits 0. Rerunning the same allowlist after a success prints only the
`verified` lines and exits 0.

**Deadline.** `--timeout-seconds N` (1 to 3600, default 600) is one deadline
carried across the whole operation: the plan's database read, every node RPC
call, the coordinator connection, and each candidate's claim, window read,
audit rebuild, landing transaction and confirmation, including revalidation
of already-complete blocks. A verification timeout exits 11 with
`exceeded (verifying); candidate <hash> was not verified`; it takes no claim
on that block. When the deadline expires during recovery the
command attempts a bounded release of the in-flight candidate's claim and
exits 11 when cleanup confirms release. If cleanup fails or times out, the
command exits 1 and reports that the claim may remain until its lease expires.
During landing, if the token no longer matches, it exits 1 and reports that
the candidate may have completed or changed owners; inspect the row before retrying.

**Failure and resume.** On any failure the command stops at that candidate
and attempts to release its claim (bounded). A confirmed release records the
reason in the row's `last_error`,
leaves the row in whatever unfinished state it is in — a pending row that
was adopted stays `reconciliation`, and a landed audit stays landed and is
reused by the next attempt — leaves `next_attempt_at` untouched, and exits
nonzero. Later blocks in the plan are not attempted. Repeating the same
allowlist resumes: finished blocks are verified and skipped, and the failed
one is retried.

Messages are written to stderr; only the exit-0 output above is on stdout.
Codes shared with `abandon` keep their meaning.

| Exit | Outcome | Message |
| --- | --- | --- |
| 0 | Plan printed, or every listed block recovered or verified complete. | The stdout described above. |
| 1 | Configuration, database or node failure, including unconfirmed claim cleanup, a halted cluster, a live legacy Python writer lease, a node that is unreachable or not caught up, and an allowlist the command refuses (a duplicate, more than 32 or a malformed hash). | The underlying error, as for every other command. |
| 2 | A listed hash has no outbox row. | `no candidate row for <hash>` |
| 4 | A listed row is terminal and cannot be recovered. | `candidate <hash> is already abandoned; its evidence was released and it cannot be recovered`, `candidate <hash> is already orphaned; its candidate payload was released and its accounting remains in the ledger. Leave chain changes to reconciliation`, or `candidate <hash> is submitted but its accounting is not proven complete (<what is missing>); inspect qbit_pool_blocks and qbit_pool_audit_bundles before retrying` |
| 5 | A listed row is held by a live claim (`--apply` only; the plan reports the holder). | `candidate <hash> is held by <instance> until <expiry>; retry after the claim expires` |
| 7 | Unsupported storage version; evidence preserved. | Names the version, as for `abandon`. |
| 8 | A pre-migration `2.x.x` document parked at `storage_version = 1`; evidence preserved. | As for `abandon`, ending in `drain it with the pinned 2.x.x image` |
| 9 | Not on the active chain: the node does not hold the block at its height. Nothing to recover; `recover` never offers. | `candidate <hash> is not on the active chain (<node detail>); nothing to recover. recover never offers a block: a block the node never accepted stays with the coordinator (or, while pending, may be abandoned); a block a reorg removed stays in reconciliation` |
| 10 | Selection refused: an unfinished parent is not in the allowlist, or the stored height is unreadable or disagrees with the node. | `candidate <hash> has an unfinished parent <parent> (<state>) that is not in the allowlist; add --block-hash <parent> so it lands first` or `candidate <hash> is stored at height <h> but the node holds it at height <node height>` |
| 11 | The deadline expired; cleanup confirmed release, or this attempt no longer holds the claim. Unconfirmed cleanup exits 1 instead. | `recovery deadline of <N> seconds exceeded (landing); candidate <hash> was left recoverable and its claim released`; planning and connecting say `nothing was claimed`; a claim-phase timeout says `candidate <hash> is no longer claimed by this attempt` after cleanup succeeds. |
| 12 | Landing refused or failed; the candidate was left recoverable with the reason in `last_error`. | `recovery of <hash> stopped: <reason>; the candidate was left recoverable` |

Codes 3 (offered; never abandonable) and 6 (pending but landed) do not occur
for `recover`: every unfinished state is recoverable, and a landed audit is
exactly what an idempotent landing reuses.

A native recovery, step by step:

1. Plan the exact hashes. Take them from `candidates list`, run
   `candidates recover --block-hash <hash> ...` without `--apply`, and add
   every unfinished parent the plan names (exit 10) until it exits zero and
   the table shows what you expect.
2. Apply from a frontend's environment, with a named `PRISM_INSTANCE_ID`
   such as `operator-recovery-INC-123`, by rerunning the same allowlist with
   `--apply`. Frontends may stay up.
3. Check the exit status and the final line (`recovered N, verified M
   already complete`). On a nonzero exit read the stderr line and the row's
   `last_error`, resolve the cause, and rerun the same allowlist to resume:
   finished blocks are verified and skipped, the failed one is retried.
4. Verify with `candidates list` — the recovered rows are gone, because
   `submitted` rows are finished work — and with the audit and dashboard
   reads for the landed blocks.

`recover` is the one command of the three that takes an allowlist — at most
32 hashes, named one by one, never "everything" — and none of the three
offers a block.

**Offer phase.** The minimum before the one `submitblock` call: the proof was
validated at Stratum admission and the row authenticated at claim (document
digest, block digest, window reference); an ordinary candidate passes the
cached staleness screen, where a stale cached tip or payout revision triggers
one authoritative chain probe that abandons a proven-superseded block or, for
a block the chain already holds, adopts the row into `reconciliation` with the
node's evidence and lands it without any offer; a leased candidate (#350)
skips the screen and is offered as issued. The request is prepared, the
reservation is committed, the strictly-live claim token is renewed
immediately before the bounded RPC, the call is made and its outcome
recorded. Nothing here waits for a builder permit or a window read, and on
the cached fast path nothing here takes the settlement lock; only the
authoritative probe a stale cached tip or revision triggers observes the
chain, which records the observation under `SETTLEMENT_LOCK` before the
reservation. That screen is kept: it is what keeps proven-superseded backlog
off the node.

**Queue and measurement.** Each frontend runs one submit loop, and it
processes claims serially: a claim's whole post-offer phase, its landing
included, completes before the loop takes the next claim, so a block found
while another is being settled waits in the outbox to be claimed, and on a
single frontend every block admitted in that time waits behind it. The offer
phase bound above is measured from the claim, on the cached fast path, with
isolated sequential blocks (`tests/offer_latency.rs`, which holds the builder
permits and the landing while it measures); it excludes the time a candidate
spends in the outbox before it is claimed and establishes neither
proof-to-offer latency under load nor concurrent throughput, both of which
include the serial loop's queue delay. A stale cached tip or revision never
shortens that path: the authoritative probe keeps its chain observation under
`SETTLEMENT_LOCK` and its abandon-or-adopt decision under the claim. The
ordinary payout-revision screen applies to every never-offered candidate; an
issued lease (#350) is its one approved exception. That queue has a material
economic window: the offer precedes the accounting confirmation, so an
ordinary candidate found on the offered block's tip and queued while that
block is being settled carries the revision current at its admission, and
when the settlement's confirmation advances the payout revision the screen
abandons the queued candidate as superseded work, even though its parent is
still the active tip. The screen stays: valid proof of work on a superseded
payout snapshot is not authorization to offer it, and only an issued lease is
exempt. More frontends can reduce the queue delay, with no throughput
guarantee; removing it, by settling one claim's post-offer phase while the
next claim is offered, is a separate concurrency change and not part of 011.

**Post-offer phase.** Builder admission, the audit rebuild from the as-issued
balance snapshot (or, for a row enqueued without one, from the current
balances when they still hash to the reference), signature and coinbase
verification, the durable range proof, the landing at the chain revision
observed after the rebuild, and a fresh observation of the chain before the
row is finished as `submitted`. A failure here (a stored builder version or
signer pair this binary cannot rebuild, a window read failure, a landed audit
that does not authenticate against the block, a landing refused by its fences,
a node or database error, a block not on the active chain) settles the row in
`reconciliation` with its reason and every piece of evidence. No post-offer
error returns a row to `pending` or abandons it. Only when that settlement
itself cannot be written does the error propagate, and the row keeps its
reservation or offer record for the next claim.

**Recovery.** Every unfinished state is claimable once its lease expires, on
any frontend, and counts toward the candidate backlog and retention. A
recovered `offer_reserved` row is delivery unknown: the call may or may not
have been made, so it is never offered again even when the node reports the
block unknown; it lands its audit and waits in `reconciliation` for the chain.
A recovered `offered` or `reconciliation` row lands its audit, or
authenticates an already-landed audit against the block rather than rebuilding
over it, and is finished once the block is active. A duplicate offer is
therefore impossible across crashes and takeovers. The price is that a crash
between the reservation commit and the call loses that delivery; the row
reports it as unknown and reconciles rather than retrying.

**Parked candidates.** A claim parks a row it must not use instead of
retrying it. Two cases park: an unsupported `storage_version`, or a version-1
row without its JSONB body (#285); and, since #387, a supported row whose
persisted input fails the claim's validation. That covers a missing window
reference or an inline pre-007 document, and JSON that is not a supported
candidate. It covers a `candidate_sha256` or document block hash that
disagrees with the row, and window columns that disagree with the document
or cannot hold its range. It also covers missing, truncated or mis-hashed
block bytes, an empty or non-hex coinbase suffix, stored inputs that
contradict the window reference, and an offer outcome this binary does not
know, persisted on a claimed unfinished row. (A claim selects only the four
unfinished states, so a row in an unknown state is never claimed and never
reaches this path.) The decode runs after the claim commits, and parking is
one `UPDATE` in a follow-up transaction behind the writer fence. That update
matches the row's database block hash, the claim's token and the state the
claim selected, so a replacement owner's claim, or a row that has since moved
to another state, is never parked. It clears `claim_token`,
`claim_instance_id` and `claim_expires_at`, records the reason in
`last_error` and sets `next_attempt_at` to `infinity`. The state, document,
digest, block bytes, window columns and offer record stay unchanged, and
`attempt_count` keeps the claim's increment. A validation reason reads
`candidate <block_hash>: validation <kind>: <diagnosis>`, at most 1024 bytes,
where `<kind>` is one of `lifecycle`, `window_reference`, `document`,
`document_digest`, `document_identity`, `window_columns`, `block`,
`coinbase_suffix` or `reference_invariants`. The claim still fails, so the
submit loop logs `candidate polling failed`. The ledger logs the full
diagnosis with one of three messages, and only the first means the row is
parked:

| Log message | Meaning |
| --- | --- |
| `parked a candidate that failed validation; operator action required` | The update matched exactly one row and its commit succeeded. |
| `a candidate failed validation and was not parked` | Nothing was committed: the transaction could not begin, the writer fence refused, the claim was no longer this attempt's, or the transaction failed before its commit. A row still held by that claim is claimed again after its lease and parking is retried. |
| `a candidate failed validation and whether it was parked is unknown; inspect the row` | The commit returned an error, so the outcome is unknown. |

Nothing else parks a row. A claim or decode failure that is not one of these
validation failures is not evidence about the stored candidate: a database or
connection error while claiming, an executor or join failure, a column that
cannot be read, or an error serializing the parsed document. Such a claim
parks nothing and is left to expire and be retried. A failure of the parking
transaction itself is reported as in the table above; in particular, a
parking commit error can leave the row durably parked with an unknown
outcome.

Find parked rows and their reasons with:

```sql
SELECT block_hash, state, storage_version, attempt_count, last_error, updated_at
FROM qbit_block_candidate_outbox
WHERE state IN ('pending', 'offer_reserved', 'offered', 'reconciliation')
  AND next_attempt_at = 'infinity'
ORDER BY updated_at;
```

Nothing unparks a row automatically. No binary resets `next_attempt_at`,
whether it is the one that parked the row or an earlier release. Reverting
the binary therefore leaves every parked row parked, because a claim selects
only due rows. Recovery is explicit operator work. `candidates recover`
(above) is the one command that claims a parked row, and only to land a
block the node already holds: it authenticates the row exactly as the claim
lane does and leaves the schedule untouched, so it does not make a row that
failed validation pass. First preserve the
row, from a backup or with `SELECT to_jsonb(o) FROM
qbit_block_candidate_outbox o WHERE block_hash = '<hash>'`. Then establish
why the stored evidence disagrees. Never edit the document, its digest, the
block bytes or the window columns to make a row pass: they are what the
claim authenticates. Once the cause is resolved, an operator may return the
row to the claim lanes with `UPDATE qbit_block_candidate_outbox SET
next_attempt_at = clock_timestamp() WHERE block_hash = '<hash>' AND
next_attempt_at = 'infinity'`. A row that still fails validation is parked
again with a fresh reason. Resetting the schedule of an `offer_reserved`,
`offered` or `reconciliation` row never grants another offer (see
**Recovery** above).

**Timing.** `proof_observed_at_ms` is the enqueuing frontend's wall clock as
the locally validated block proof entered the coordinator; `offered_at_ms` is
the offering frontend's wall clock immediately before its `submitblock` call.
`qbit_prism_block_submit_seconds` observes their difference once per block,
on the frontend that made the call, after the call returned; that difference
includes the wait in the outbox before the claim, which the claim-to-offer
bound above excludes. The reservation
time is never substituted for the call time; a row without a proof time
(written before 011, or by a bare enqueue) yields no sample; a negative
interval is clock skew between the two hosts and yields none; a recovery on
another frontend records none. A crash between the call and the outcome
commit may lose that block's sample with the process (unless a scrape had
already read it), and the recovery never emits one, so the histogram is at
most once per block, not exactly once across crashes.

**Rolling upgrade and quiesce.** A pre-011 frontend keeps a row `pending`
through its whole attempt and offers it after landing, or before landing for
a leased candidate; a post-011 frontend would reserve and offer such a row
again. The two must never share an outbox. Stop every pre-011 frontend and
let their claims expire (at most the 120 s lease) before migrating. Keep
supervisors and automatic restarts disabled throughout the cutover. Every
`qbit_prism_instances` row must explicitly report `stopped` or `drained`;
011 names and refuses any other instance, even with an empty outbox or a
stale heartbeat. Heartbeat age proves only that reporting stopped, not that
an idle or paused frontend cannot resume. Resolve blockers through graceful
shutdown; do not edit or delete instance evidence to bypass the check.
The scan holds a table lock through migration commit to serialize heartbeat
registration and updates. 011 also refuses any pending row with a live claim,
refuses an attempted
pending row that lacks its block bytes or window reference, quarantines every
pending row a pre-011 frontend attempted as `reconciliation` with an unknown
outcome (recovered without any offer, confirmed if the chain holds the block,
otherwise kept with its evidence for the operator), leaves never-attempted
rows pending, and declares the `candidate_offer_lifecycle` capability, which
a pre-011 binary refuses at connect and a post-011 binary requires at every
start and before any migration DDL: a database at 011 whose declaration is
missing or holds another value (a selectively restored capability table) is
refused with nothing changed, the refusal names the remedy, and the server
never repairs the declaration itself. 011 replaces only the lifecycle rules it
knows (001's state and payload rules, or #258's dual-format rule), recognised
by their definitions rather than their names; every other CHECK constraint on
the outbox that references neither `state` nor `completed_at` (by the
catalog's column dependencies) is kept exactly as it is, and one that
references either column, or a foreign constraint under one of 011's own
names, is refused by name with nothing changed, for the operator to drop and,
if it still holds for the offer lifecycle, re-create after the migration.
This conservative catalog policy writes no synthetic rows and executes no
operator predicate; it does not prove a kept CHECK compatible with later
writes, and a kept CHECK that rejects the quarantine or a lifecycle write
fails that write's transaction as any CHECK would. Signer rotation is
refused while any unfinished row stores other keys, in every unfinished
state.

**Startup fence (012).** The instance-table lock alone cannot reject an old
startup queued behind migration. Migration 012 adds a database CHECK that
requires `candidate_offer_lifecycle: 1` in every `starting` heartbeat. This
binary writes that marker; a pre-011 binary cannot finish registering even
if it checked capabilities before the cutover. The constraint is checked
after the lock wait, including an upsert that would reuse a stopped instance
ID. Shutdown and historical health evidence are retained. Migration 012 and
011 commit together on a pre-011 database. A database already at 011 must
also stop all frontends gracefully before applying 012; it uses the same
explicit shutdown check. Restart with this binary after cutover. The new
`instance_offer_startup = 1` capability rejects earlier binaries at their
ordinary startup gate, and this binary refuses a missing or changed
012 declaration. Recovery exports require the same schema and declaration.

**Orphan disposition upgrade (015).** Stop all frontends gracefully before
applying 015, including frontends already using the offer lifecycle. Keep
supervisors and automatic restarts disabled until migration finishes, and
restart only binaries that understand `candidate_orphan_disposition`. As with
011/012, every recorded instance must explicitly report `stopped` or `drained`;
an old heartbeat is not proof that a frontend cannot resume. The migration
refuses active instances before replacing the outbox constraints, and uses the
configured database lock timeout. Run it during a maintenance window: replacing
and validating CHECK constraints requires an exclusive outbox lock.
The capability gate runs at connect and does **not** evict an older frontend
that is already connected. Migration 012's startup marker proves support for
the offer lifecycle, not for the later orphan disposition; it cannot fence a
pre-015 startup already waiting on the migration lock. The shutdown and restart
procedure is therefore required, including disabling automatic restarts.
Migration 015 refuses modified named lifecycle rules and additional CHECKs that
reference `state` or `completed_at`, leaving the schema unchanged on refusal.
Resolve those constraints explicitly before retrying; do not bypass the check.

PostgreSQL and qbitd do not share a transaction. Accounting effects are
idempotent and claim-fenced. Do not infer active-chain acceptance from a
socket write or a missing RPC reply: an offered block is confirmed only by a
fresh active-chain observation.

## Chain observation epoch upgrade (018)

Migration 018 adds `qbit_prism_cluster.chain_epoch` and declares
`chain_observation_epoch = 1`. The counter changes atomically with every
accepted chain checkpoint and payout revision. Accounting-only revision
updates do not change it. Equal-work observations bind their transition to
the original epoch before node I/O, so a peer's A-to-C-to-A round trip cannot
be mistaken for accounting-only drift. Fresh retries retain that original
epoch; cancellation, unknown COMMIT outcomes and failed publication cannot
rearm them. Lower-work refusals retain the previous local tip without
restoring consumed retry authority.

Epochs order fresh observation attempts against committed cluster changes;
they do not timestamp node choices that occurred between polls. A new
observation started after a completed peer round trip can establish a new
transition from the currently accepted predecessor, using the current epoch.
That is distinct from retaining an older in-flight witness across the round
trip. Subsequent unchanged polls cannot repeat the replacement.

A scheduler tick or wake permits at most one immediate fresh retry after a
definite accounting-only refusal. Another refusal returns to normal polling,
and shutdown prevents the extra attempt. The original witness epoch and all
publication checks still apply; repeated concurrent interference has no
unconditional two-second completion guarantee.

This is an **offline development-line upgrade**, not a rolling upgrade.
Stop every earlier frontend and one-shot writer, disable automatic restarts,
and finish or cancel any old startup already past its capability check.
Every registered instance must explicitly report `stopped` or `drained`;
do not delete instance rows or treat heartbeat expiry as shutdown. The
migrator holds the registration lock through the schema commit and refuses
active instances before applying 018. That database check cannot discover an
unregistered old tool or evict an already connected process: excluding all
old writers is an operator prerequisite. Start only epoch-aware binaries
after commit. No production deployment is implied by this development change.

Existing checkpoint and accounting values are preserved; epoch begins at zero
because old processes and their observations no longer exist at the offline
boundary. Restart, accounting, policy changes and fatal-state recovery never
reset it. The capability refuses older binaries on subsequent connection,
including operator tools that use the same gate. Removing the capability or
resetting the counter is not a supported downgrade. Restoring an older full
backup requires all writers stopped and the existing accounting-reconciliation
procedure; post-upgrade shares and candidates must not be silently discarded.

Recovery evidence includes every nonzero epoch in `chain_checkpoint`, while
the zero default preserves pre/post-migration evidence equivalence. Restore
the whole cluster checkpoint, never its epoch independently. Issued balances,
WindowRef, prepared/job/candidate payloads, protocol and payout/candidate
revision fences are unchanged. A cold conflicting equal-work frontend still
waits for convergence or more work; this introduces no authoritative-node
setting. Migration numbers 016/017 belong to the separate share-partitioning
change; 018 is independently required on this branch.

## Blocks, balances, and reorgs

Core durable tables remain:

| Table | Purpose |
| --- | --- |
| `qbit_share_ledger` | Canonical accepted share history |
| `qbit_pool_blocks` | Pool blocks and active-chain/maturity state |
| `qbit_pool_payout_entries` | Per-recipient payout records |
| `qbit_payout_carry_forward` | Auditable balance deltas |
| `qbit_pool_audit_bundles` | Canonical audit metadata and stored body |
| `qbit_ctv_fanout_sets` | Committed fanout sets |
| `qbit_ctv_fanout_artifacts` | Transactions, maturity, and broadcast state |

Current balances replay active confirmed carry-forward deltas. Zero net balances
need no current row; negative balances remain visible debt offsetting future
rewards. `qbit_carry_forward_integrity_report()` checks stored prior, candidate,
and carry values against replay. The legacy operator report added
`audit_head_sha256` over active carry rows; the SQL function alone does not
calculate it. The recovery export below reproduces that exact head. Preserve it
with independent release/recovery records.

A landing writes the block's payout and carry rows from the immutable payout
manifest of the audit its coinbase commits to, whatever the canonical balances
are when it lands, and since migration 011 marks the block with that audit's
digest (`qbit_pool_blocks.as_issued_audit_sha256`) in the same transaction. A
pending, never-offered candidate whose reference balances are no longer
current is refused (it is superseded work); an offered block lands as issued.
Current balances still sum the active per-block `(gross − onchain)` deltas, so
a block that lands after another block moved a miner's balance adds its own
issued deltas, and a miner paid a carried balance on chain by both blocks
carries the difference as visible debt; nothing is paid a third time and no
commitment is rebuilt. `qbit_carry_forward_integrity_mismatches()` validates a
marked block against that manifest: the arithmetic of every manifest account,
fee recipients included; each carry row field by field against the matching
miner account; and the payout entries against every account. A marked block
whose audit or manifest is missing is a finding by itself. Blocks landed before
011 keep the sequential rule. `qbit_carry_forward_current_drift()` is unchanged.

Coinbase maturity is 1,000 blocks: a height-H payout becomes mature only at tip
height H+1,000 or later. An immature disconnected block is marked inactive, so
its balances stop contributing; it can reactivate. Terminal reversal preserves
audit history and marks payout/carry rows reversed. A mature disconnect sets a
shared fatal state and stops ordinary accounting until investigated. Other
instances must not continue with a different interpretation of that event.

## Audit storage and retention

Native accepted-block audits store the non-share bundle fields and a
`share_snapshot_sha256` reference to `qbit_prism_audit_snapshots`. A snapshot
records the canonical share interval, anchor, count, and digest. Its shares are
reconstructed from the immutable ledger. Bootstrap snapshots may retain their
synthetic share inline. The reader verifies the reconstructed share digest and
canonical bundle SHA before returning a logical v1/v1.1 bundle. A row whose
shares have been archived is served from stored canonical bytes instead: before
a partition leaves the ledger every audit whose snapshot intersects it is
sealed, the artifact rebuilt from the still-online shares and checked against
its advertised `audit_bundle_sha256`, and the bytes kept in
`canonical_audit_bytes`, exactly as an imported legacy audit keeps them.
Readers prefer stored bytes over reconstruction whatever the row's shape, so
the artifact route, the audit bundle route and the operator tools keep serving
the block under its published digest. The snapshot metadata row stays: it
records the range and digest those bytes were proved against.

Share UPDATE, DELETE, and TRUNCATE are prohibited, on the partitioned parent
and on every leaf. Migration 017 installs the immutability trigger on the
parent, and `qbit_prism_share_partition_create` installs it on each partition
it creates, because PostgreSQL does not clone a statement trigger to a
partition. Removing a share could break both future accounting and already
published audit hashes, and nothing in the native runtime deletes one: shares
are immutable and are never deleted.

Retention is not deletion. Space is reclaimed a whole partition at a time,
after the partition has been sealed, written to an archive outside PostgreSQL,
and verified against the live rows: `DETACH PARTITION ... CONCURRENTLY`, then
`DROP TABLE` of the detached relation, with the archive as the copy of record
from then on. `qbit-prism-server share-archive` is that path and the only one;
there is still no row-level prune or share-compaction command, and no `DELETE`
runs on the ledger. Keep the canonical share history and all referenced
snapshot rows online until that procedure clears them, and keep the archive
under the same retention plan as the database backups. The procedure is
[below](#share-ledger-partitions-and-retention); the decision behind it is
decision D6 in [the design record](prism-share-ledger-partitioning.md).

Imported historical external audits retain their verified canonical bytes;
they are not silently rewritten into references to potentially incomplete
legacy share history. Import preserves their published canonical SHA. Legacy
body-ref and v2 segment formats remain supported by the Rust offline loaders.
Back up external bodies and their segments until import and restore validation
are complete, and retain the original backup under the migration retention plan.

## CTV recovery and broadcasting

Committed fanout artifacts are durable database records. Broadcasting requires
a mature active parent and a current claim. Workers record transaction/package
outcomes and retry state so another instance can resume after a crash. Failed,
reorged, or completed artifacts remain visible through the audit/public API.

To reconstruct missing artifact sets from verified database audits:

```sh
qbit-prism-server backfill-ctv
```

Run `import-audits` first for historical file-backed bodies. Backfill verifies
the trusted ledger key, canonical digest, and recorded coinbase before repairing
rows; matching existing artifacts are idempotent. It processes stored audits,
replacing the former Python tool's individual-path and block-filter CLI.

To process a single batch using the normal broadcaster policy:

```sh
qbit-prism-server broadcast-ctv
```

The integrated periodic worker uses `PRISM_CTV_BROADCASTER_ENABLED=1`. An optional
CPFP wallet and fee configuration must be consistent with the intended operating
policy. Durable claims coordinate work across instances; node RPCs may still
receive an identical transaction more than once after a lost reply. The one-shot
`broadcast-ctv` verifies the node, the schema and the cluster fingerprint like a
frontend but registers no heartbeat: its claims are fenced by their own claim
tokens, so it never competes with a frontend's broadcaster for the same fanout
and leaves no `qbit_prism_instances` row for fatal-state recovery to refuse.

Confirmed fanouts are observed every five seconds until 1,000 confirmations;
afterward the latest deep checkpoint is checked every 60 seconds. A shallow
fanout disconnect returns the transaction to broadcast work. Disconnection of a
deep checkpoint halts the shared cluster for explicit reconciliation.

Between claimed fanouts a frontend's periodic pass yields to its own template
refresh: it stops at the fanout boundary while the detected tip is unpublished,
or when a tip observed after the pass began differs from the tip the pass
started from, counted by
`qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total`, and the remaining
rows stay claimable for a later pass. The unpublished-tip yield lasts at most
`PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS` (default 120) from the first
departure, the same replacement-build budget as the published-tip lease, so a
refresh that keeps failing before publication cannot strand settlement. An
observation older than the pass never yields, so a refresh that has not caught
up with the node cannot strand it either, and same-tip polls never yield.

Without transaction indexing, the broadcaster uses a durable block-scan cursor
and chain anchor. `PRISM_CTV_SPEND_SCAN_BLOCKS` bounds each pass (default 32,
range 1–256); a reorg resets the cursor. The node must retain the historical
blocks needed by that scan. A pruned/unavailable block range cannot be treated
as proof that a fanout is unspent.

For positive CPFP sponsorship, use a dedicated wallet. Broadcasters that create
or recover unsigned packages must reach the same sponsorship wallet RPC service;
unrelated wallets with the same name cannot sign each other's reserved inputs.
Other nodes can replay an already signed package without opening that wallet.
The database reserves
funding before wallet locking and preserves the exact signed child before
submission. A replacement process recovers the reservation and replays that
same package rather than selecting fresh funding after a lost reply. Funding
is not unlocked until the node observes it spent, including a mempool spend.
Automatic replacement fee bumps and abandoned-reservation release are not
implemented; retain and reconcile the durable reservation when handling those
cases manually.

## Retry, replay and deadline contract

The ledger never replays SQL automatically. After a `statement_timeout`
(`PRISM_DATABASE_STATEMENT_TIMEOUT_MS`, default 15000), a `lock_timeout`
(`PRISM_DATABASE_LOCK_TIMEOUT_MS`, default 5000), a closed connection or a lost
acknowledgement, the error reaches the caller and the ledger neither re-sends
the statement nor re-runs the transaction. This is distinct from normal
traversal: a paged read sends its page query repeatedly by design, and audit
materialization reads the snapshot row and its share range through separate
pool checkouts. A statement cancelled or a socket closed before `COMMIT`
aborts the open transaction, so the claim and state that authorized the call
remain as they were; that holds only when the transaction did not commit. A
`CommandComplete` received before `COMMIT` proves execution, not durability. A
`COMMIT` whose acknowledgement is lost leaves the outcome unknown to the
caller: the transaction may or may not have committed. The caller's next
observation must come from the durable claim and state, never from a replay.
Every later attempt is a separate public operation with its own claim.

| Operation (public API) | Class | Automatic re-execution | What runs later, and who authorizes it | Coverage |
| --- | --- | --- | --- | --- |
| Candidate terminal disposition: `finish_candidate`, `finish_candidate_at_revision` | never-retried mutation | none | The same live claim may invoke again after a reported error; otherwise the worker calls `retry_candidate`. A consumed claim is rejected and writes nothing. | dynamic: `candidate_terminal_timeout_executes_once` |
| Candidate backoff: `retry_candidate` | explicit later operation | none | Releases the claim, records the error and advances `next_attempt_at` by `min(60, attempt_count)` seconds, or `min(3600, 10 × attempt_count)` for a `reconciliation` row. It does not re-run the failed write and never changes the row's lifecycle state. A fresh `claim_candidate` after that time is the next attempt, with a new token; the old token stays rejected. | dynamic: `candidate_backoff_requires_new_claim`; `offer_lifecycle::every_unfinished_state_is_claimable_retained_and_reserved_only_once` |
| Offer lifecycle: `reserve_offer`, `record_offer`, `adopt_active_candidate`, `reconcile_candidate` | never-retried mutations, claim-fenced | none | Each is one state transition of the live claim's row (`pending` to `offer_reserved`, `offer_reserved` to `offered`, `pending` to `reconciliation`, an offered state to `reconciliation`) and is rejected once the claim is lost or the row has moved on. A reported error after `reserve_offer` leaves the reservation in place: the next claim, on any frontend, treats the row as delivery unknown and never calls `submitblock` for it. | `offer_lifecycle::every_unfinished_state_is_claimable_retained_and_reserved_only_once`, `candidate_lease_tests::token_loss_between_heartbeats_is_fenced_immediately_before_submitblock`, `candidate_lease_tests::crash_after_offer_before_landing_recovers_on_another_frontend_without_a_second_offer` |
| CTV attempt journal: `finish_fanout` | never-retried mutation; one journal row per authorized claim | none | Every recorded attempt, `failed` included, releases the claim and schedules `next_broadcast_attempt_at` (10 s per attempt, at most 3600 s). A fresh `claim_fanout` after that time is the next attempt and its own journal row; the old token stays rejected. | dynamic: `fanout_journal_timeout_executes_once`, `broadcast_retry_requires_new_claim` |
| Claims: `claim_candidate`, `claim_fanout`, `renew_candidate_claim`, `renew_fanout_claim` | public reinvocation | none | One token per block hash or fanout. Distinct hashes have independent leases. Candidate terminal dispositions acquire the shared `SETTLEMENT_LOCK`, then `ORDER_LOCK`. The cited test verifies that a disposition does not touch a sibling candidate's locked outbox row; it does not establish concurrent execution of dispositions. | dynamic: `distinct_hashes_hold_independent_claims` |
| Landing: `land_candidate`, `land_candidate_at_revision` | dependency reread, idempotent for an identical audit | none | Re-invocation re-reads the stored audit digest and header bits and accepts only an identical audit. A superseded payout revision is a reported error with no block, audit, payout, carry or fanout row written; recovery at a proven newer revision is an explicit call. | dynamic: `superseded_landing_reports_failure`; existing `ledger_postgres::active_candidate_can_land_at_proven_new_chain_revision` |
| Expired or lost claim across a halt | never revived | none | Clearing `fatal_error` restores authority for new work only; the token that expired during the halt is still rejected by every operation and a new claim is required. | dynamic: `restored_authority_requires_fresh_claim` |
| Share append: `append`, `append_at_revision` | public reinvocation, idempotent by share identity | none | A miner or frontend resubmission is a new call; the share identity and proof-hash registry return the existing row without a second credit. | inventory: `ledger_postgres::global_duplicates_idempotence_and_config_fencing`, `postgres_failover` |
| Page traversal: `read_window`, `snapshot`, audit materialization | normal page traversal | none | A page is not a retry. `read_window` pages inside one `REPEATABLE READ READ ONLY` transaction and reports an incomplete range as an error instead of a partial window. `snapshot` fixes its anchor and revision in one short transaction, then pages immutable rows at or before that anchor in a second one. Audit materialization reads the snapshot row and the share range through separate checkouts; the share count, snapshot digest and bundle digest authenticate the result, and a mismatch is an error. | inventory: `window_reference`, `window_read_oracle` |
| Dependency reread or repair: `save_issued_job` with a repair payload, `backfill_ctv`, `import_legacy_audits` | dependency reread/repair | none | Cold-path calls that re-read the durable dependency and verify its identity, or rebuild a missing row idempotently. None replays a failed write. | inventory: `issued_job_dependency`, `ledger_postgres::ctv_artifacts_wait_for_maturity_and_claims_are_fenced`, `ledger_postgres::legacy_audit_import_validates_envelope_hash_and_pinned_key` |
| Reconciliation: `pool_blocks_for_reconcile`, `reconcile_blocks`, `reconcile_blocks_at_revision`, `observe_fanout` | public reinvocation, revision fenced | none | Periodic calls. `pool_blocks_for_reconcile` is one query. A stale expected revision (for `observe_fanout`, when the observation carries one) is an error that changes nothing. | inventory: `ledger_postgres::verified_landing_reconstructs_audit_and_reorgs_are_revision_fenced` |
| Session reservation release: `SessionId::release`, drop cleanup | best-effort cleanup | none | A lost cleanup reply leaves the outcome unknown: the reservation may have been deleted or retained. A retained reservation is reclaimed once its owner is recorded as stopped. | inventory: `ledger_postgres::session_sequence` |

"Dynamic" rows are exercised by
`crates/qbit-prism-server/tests/ledger_single_execution.rs` against a disposable
PostgreSQL. The two timeout tests route one ledger pool through a test-only
protocol proxy that frames both directions of the wire and counts the
targeted statement in `Execute` and `Query` frames across every connection,
including a same-text replay the aborted transaction rejects and a second
copy of the statement inside one simple `Query` frame. Server rejections are
recorded separately from executions and every one in the observed window
must be the targeted statement's own failure, so a replay under a different
statement text, which the server refuses at `Parse` without any `Execute`,
also fails the test. An execution whose statement text the proxy did not
learn fails the test instead of escaping the count. The targeted statement
is identified while it runs by a marker NOTICE from a statement-level
fixture trigger on the durable table, not by its SQL text or its position.
They seed a real server `statement_timeout` on the terminal write and a lost
acknowledgement before and after `COMMIT`, and read the durable outcome back
directly; a re-invocation after a lost acknowledgement is checked to run on
a connection other than the one the fault closed. The durability of the
lost-`COMMIT` case is proven by the proxy observing the server's `COMMIT`
completion and by reading the state back, not by the caller's view.
"Inventory" rows are a static review of the code, backed by the existing
tests named; they were not re-tested for this contract.

```sh
PRISM_TEST_DATABASE_URL=postgresql://test_user@127.0.0.1:5432/test_db \
  cargo test --locked -p qbit-prism-server --test ledger_single_execution -- --nocapture
```

### Deadlines

The legacy writer's final-partial-batch and progress-between-batches deadline
workflow has no native counterpart. There is no batch writer: each accepted
share commits in its own transaction, so no deadline can expire inside a
batch and no batch is retried as a unit. Two native mechanisms stand in for
it:

- Issued-job late expiry. `save_issued_job` checks the job's absolute
  deadline after taking the shared settlement lock and before locking its
  dependency row, and again just before commit, so a deadline that elapsed
  while waiting on row locks is a reported error that retrying cannot reset,
  and the job is not saved. The runtime pruner runs every two seconds with
  no overlapping batches. First, `prune_expired_jobs` removes at most 4096
  expired rows as a separate statement, without advisory locks and with the
  configured database timeout. Its outer expiry recheck preserves concurrent
  renewals. Then `prune_unreferenced_blobs` starts a fresh five-second deadline,
  takes `SETTLEMENT_LOCK` then `ORDER_LOCK`, checks the shared writable fence,
  and inspects up to 256 template keys and 256 balance keys. References from
  every surviving job and every candidate retain their blobs, regardless of
  job or claim expiry. Per-table cursors advance only after commit, skip live
  prefixes and wrap to find later orphans even when no jobs expire. A halted
  cluster still permits the original expiry statement but refuses blob GC.
  Blob failure or cancellation rolls back both blob deletes and leaves the
  cursors unchanged; it does not undo previously committed expiry. Successful
  nonzero counts are logged per phase, and failures warn on each pruning tick.
- Per-item import failure and restart. `import-audits` processes one legacy
  audit at a time, each verified off the runtime threads and stored in its
  own transaction. A failing item stops the command with an error; earlier
  items stay imported, there is no partially imported item, and rerunning the
  command resumes with the rows that still lack canonical bytes.

### Native snapshot rejection and legacy segments

A native audit body references an immutable share snapshot in
`qbit_prism_audit_snapshots`. Landing rejects a snapshot that is empty, out of
canonical order, or different from the ledger's rows for its range, before any
obligation is recorded. Materialization verifies the share count, the
reconstructed share digest and the canonical bundle digest, and returns an
error instead of a repaired or substituted body. Imported canonical bytes are
checked on both read paths: raw canonical-byte serving requires the declared
digest and a JSON object, and materialization into a logical body
additionally rejects an audit envelope and parses the bytes as a flat bundle.

The legacy `audit-body-ref` and v2 segment envelopes are still parsed by the
shared audit parser and the offline loaders, for import and verification. The
legacy segment lifecycle (gap backfill from the ledger, quarantine of a
segment when the ledger has no rows, and the conflicting-duplicate raise)
belonged to the retired filesystem audit store and is not a native lifecycle:
natively, a body is either reconstructed exactly from immutable rows or served
from imported canonical bytes, and a mismatch is a read error to investigate,
never a repair or quarantine step.

## HA database and shutdown

Point every instance at the same writable primary endpoint. Do not route ledger
queries to a lagging read replica or distribute writes among independent
PostgreSQL primaries. Sum `PRISM_DATABASE_MAX_CONNECTIONS` across frontends and
reserve capacity for migrations, monitoring, backup, and failover administration.

The cluster records its highest observed cumulative chain work. A node that is
still synchronizing, follows a lower-work tip, or disagrees at equal work cannot
advance accounting. Share commits are fenced by the current revision as well.
This prevents a lagging node from reversing another frontend's accepted blocks.
For manual regtest invalidation/reconsideration, extend the intended branch past
the previous work record before expecting the pool to resume; do not clear the
record to accommodate a lagging production node.

Keep PostgreSQL `fsync=on`, `full_page_writes=on`, and
`synchronous_commit=on`. A local durable commit protects against a coordinator
crash. Zero acknowledged-share loss on database-primary failure additionally
requires synchronous standby flush and a promotion policy restricted to a
standby containing acknowledged commits. An HA endpoint does not establish this
by itself. Test primary failure and client reconnection using the actual
replication, proxy, and storage configuration.

SIGTERM closes listener admission and asks tasks to drain before the database
pool closes. The native server bounds shutdown drain to 30 seconds; unfinished
candidate/CTV intents remain in PostgreSQL and become reclaimable after their
claims expire. There is no legacy writer-lease release barrier. Observe each
frontend's health before restoring traffic after a restart.

Backups require the database, signing-key recovery material, and any unimported
external audit bodies/segments. Use independent base backups plus WAL archives
for point-in-time recovery; replication is not a replacement for backups.
Restore into isolation and verify share order, audit hashes, carry-forward
integrity, CTV state, and API reads before declaring recovery complete.

## One-way migration and isolated-restore reconciliation

Decision D5 in #260 chooses one-way native migration with forward repair. No
down-migrations are provided for native 002–010, including 006/007/008. Never
remove native guards to run a legacy writer. The publication-ordinal revert
refuses native schemas and names this recovery path. The data-loss boundary,
matching the [unreleased 3.0.0 release notes](../doc/release-notes-3.0.0.md), is:

> Before the first native share is acknowledged, restore the complete
> pre-migration database and artifact backup and restart the pinned old image
> in isolation from the migrated database. After the first native share is
> acknowledged, restoring an older database loses those accepted records.
> Any rollback that discards acknowledged history requires an explicit
> accounting reconciliation and recovery decision; it is not an ordinary
> image rollback. Keep every native frontend stopped while performing an
> isolated restore/recovery operation against its replacement database.

1. **Preconditions and stop boundary.** Pin the source/native image digests and
   retain keys, configuration, database backups/WAL and every external audit
   body/segment. Before an actual cutover, drain legacy candidates with the
   supported source release as described in the [migration guide](prism-rust-migration.md#supported-2xx-source-schemas).
   Stop all Python/native frontends, public services and CTV/candidate workers;
   block their access to the isolated restore. Keep the latest database intact.
   Treat an unknown first-ACK boundary as post-ACK. Lost replies and background
   settlement can leave committed work even before a successful native ACK.
2. **Numbered pre-migration canonical backfill rehearsal.** Restore a source
   copy and artifact backup in isolation, then run native `migrate` and
   `import-audits --root /var/lib/qbit-prism/audit` there. This is the native
   canonical backfill: it reconstructs bytes from recoverable inline or
   external bodies when the sidecar never existed, preserving the published
   SHA and verifying the trusted ledger key and coinbase. Require zero
   `missing_stored_bodies` and `missing_canonical_bytes` before migrating the
   real source. A corrupt present sidecar fails closed. Restore missing
   evidence and rerun import; do not invent replacement history. This uses
   the native equivalent and does not run the `2.x.x` command blocked by
   #174/#176. Record timings and history size; use a production-sized copy.
3. **Save and compare exact accounting evidence.** Configure protected libpq
   services `prism-source`, `prism-current` and `prism-restore` for distinct
   databases with the correct ledger `search_path`. With the source drained:

   ```sh
   umask 077
   PGSERVICE=prism-source pg_dump --format=custom --file=pre-native.dump
   PGSERVICE=prism-source psql -XqAt -v ON_ERROR_STOP=1 \
     -f scripts/prism-recovery-evidence.sql > source.rows.jsonl
   python3 scripts/prism-recovery-evidence.py source.rows.jsonl > source.summary.json
   ```

   The [exact SQL](../scripts/prism-recovery-evidence.sql) exports ordered share
   rows, block/publication order, audit SHAs, payouts, carry rows, candidate and
   CTV state in one read-only snapshot. The [streaming summarizer](../scripts/prism-recovery-evidence.py)
   records counts and digests and reproduces the legacy `audit_head_sha256`;
   compare that head to the mirrored pre-cutover report. It refuses incomplete
   exports and carry mismatches/drift. Require `unfinished_candidates` zero. Protect
   the evidence as accounting data and budget disk for the share export.
4. **Exercise the isolated pre-ACK restore.** Provision a separate empty
   database with compatible PostgreSQL, roles and extensions. Restore the full
   database and artifact backup, never over the current database:

   ```sh
   pg_restore --exit-on-error --single-transaction \
     --dbname='service=prism-restore' pre-native.dump
   PGSERVICE=prism-restore psql -XqAt -v ON_ERROR_STOP=1 \
     -f scripts/prism-recovery-evidence.sql > restored.rows.jsonl
   python3 scripts/prism-recovery-evidence.py restored.rows.jsonl > restored.summary.json
   cmp source.summary.json restored.summary.json
   ```

   Only exact equality permits testing the pinned old image on the isolated
   legacy database, with mining and broadcasting disabled. Stop it again before
   migrating the restore. For a post-ACK incident, continue to step 6 even if
   this pre-migration comparison passes.
5. **Repair forward and enforce import.** Verify `PRISM_DATABASE_URL` targets
   the isolated restore and set the original trusted
   `PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX`; no signing seed is needed for import.
   Run `time qbit-prism-server migrate` then
   `time qbit-prism-server import-audits --root /var/lib/qbit-prism/audit`.
   Repeat the export as `migrated.rows.jsonl`/`migrated.summary.json` and require
   `cmp source.summary.json migrated.summary.json` to pass. Both import
   completeness counts must be zero. Native snapshot-backed audits count as
   canonical available without duplicating their share window as stored bytes.
   Availability counts do not replace digest/signature verification. Run
   `backfill-ctv` only after this comparison and explain any intentional CTV
   repair delta in later evidence.
6. **Reconcile all post-ACK differences.** Export `prism-current` using the same
   SQL to `current.rows.jsonl`/`current.summary.json`. Compare against the
   restored summary and inspect the ordered rows for every changed digest.
   Run these queries against current state, supplying the source summary's
   integer `last_share_seq` as the psql variable `source_last_share_seq`:

   ```sql
   SELECT share_seq, share_id, miner_id, share_difficulty, credit_policy
   FROM qbit_share_ledger
   WHERE accepted AND share_seq > :source_last_share_seq ORDER BY share_seq;
   SELECT state, count(*) FROM qbit_block_candidate_outbox GROUP BY state ORDER BY state;
   SELECT qbit_carry_forward_integrity_report();
   ```

   A native share tail is accepted history the older restore would lose. Also
   reconcile block states, the carry head/balances, payouts and CTV broadcasts
   against chain evidence; share counts alone are insufficient. Preserve the
   current database and prefer forward repair with a compatible native image.
   Any plan discarding acknowledged history requires an explicit accounting
   owner's recovery decision. Do not copy rows ad hoc into the legacy schema.
7. **Verify service and record the rehearsal.** Sample exact canonical artifact
   bytes/SHA/ETag through the isolated public API, then repeat through the actual
   public edge after cutover. Use the exact `curl`, `shasum`, and ETag commands
   in [recovery step 5](prism-rust-migration.md#recovery-and-rollback); require no
   missing-canonical fallback header. Include old history, sidecar-only and
   reconstructed imports. Resolve stale missing edge cache entries before
   admission. Run production `self-check` with the intended services available;
   its JSON must show zero audit-completeness and integrity failures and its
   exit status must be zero. Lab mode reports incomplete audits without making
   that condition fatal; production mode refuses them. Retain every duration,
   row count, comparison, artifact sample and image digest. #291 rehearses this
   on production-sized history and retains the exact data-loss boundary when
   completing the 3.0.0 release notes before approving rollout.

## Share ledger indexes

`qbit_share_ledger` is append-only and every insert maintains every index,
so an index nobody scans, or INCLUDE payload nobody reads, is write
amplification without a reader. Migration 013 (#153) trimmed the secondary
indexes to the native query set below. The table is what to check against
before adding an index or a query that reads the ledger.

Migration 017 (#144) changed where that set lives, not what is in it. The
ledger is a partitioned table now, so the parent carries the primary key and
the four secondary indexes as partitioned indexes and every leaf carries one
index of its own for each of them, adopted from the release table for
`qbit_share_ledger_p0` and created with the partition for every cell after it.
The definitions below are the parent's; `pg_stat_user_indexes` reports the
leaves, one row per partition per index, and that is where sizes and scan
counts are read. Each leaf's indexes are cache-resident while the partition is
hot and are never touched again once it is not.

The one index that could not survive the conversion is the global
`UNIQUE (share_id)`: a unique index on a partitioned table must include the
partition key, and `share_id` is not it.
`qbit_share_ledger_share_id_key` is now the leaf index
`qbit_share_ledger_p0_share_id_key`, and every partition created afterwards
gets its own `<partition>_share_id_key` from
`qbit_prism_share_partition_create`. The authority on `share_id` uniqueness
across the whole ledger is `qbit_prism_share_hashes`, which the native append
already wrote one row of per share, in the same transaction as the ledger row,
keyed by the header hash with `UNIQUE (share_id)`. It is not partitioned, its
rows are never removed, it stays O(1) per share, and the append path consults
it first: a header hash it holds under the same `share_id` is an exact replay,
under another `share_id` it is the cross-identity duplicate refused as before,
and a header hash it does not hold is a new share.

| Index | Definition | Native readers | Plan |
| --- | --- | --- | --- |
| `qbit_share_ledger_pkey` | `(share_seq)` | every `share_seq` walk that projects share rows: the payout page walk of `snapshot`, the audit range reads, `qbit_prism_window`'s ranking pass, the rollup batch in `rollups.sql`, the latest-share probe | index scan, then the heap for the projected columns |
| `<partition>_share_id_key` | `(share_id)`, unique, one per leaf, no partitioned parent | the replay comparison in the append path, the block-only reconciliation probes, the vardiff evidence lookup, `share_accepted_at_ms` | index scan on the leaves a bounded probe leaves after pruning |
| `qbit_share_ledger_accepted_seq_walk_idx` (013) | `(share_seq DESC) INCLUDE (job_issued_at, accepted_at, share_difficulty) WHERE accepted` | `qbit_prism_window`'s newest-first page walk (pool snapshot, reward leaderboard), the landing durable-range count under the settlement lock, `max(share_seq)`, the rollup boundary and tail passes | index-only |
| `qbit_share_ledger_accepted_recent_idx` | `(accepted_at DESC) INCLUDE (share_difficulty, miner_id, share_seq) WHERE accepted` | pool hashrate series, leaderboard window, pool snapshot rollups, the miner summary's pool figure, evidence counts | index-only |
| `qbit_share_ledger_accepted_miner_history_idx` (013) | `(miner_id, accepted_at DESC) INCLUDE (share_difficulty, share_seq, share_id) WHERE accepted` | miner share summary, worker rows (`share_id` carries the worker name), miner hashrate series and rollups (`share_seq` against the watermark) | index-only |
| `qbit_share_ledger_accepted_block_suffix_idx` | `((lower(right(share_id, 64))), accepted_at DESC, share_seq DESC) INCLUDE (miner_id, share_difficulty, network_difficulty) WHERE accepted AND length(share_id) >= 65` | the block-solver lookup in blocks, leaderboard, reward leaderboard and pool snapshot | index scan, one row per block |

Dropped by 013, with no native reader:

- `qbit_share_ledger_accepted_seq_window_idx`, `share_seq DESC` with seven
  INCLUDE columns. Every `share_seq` walk was planned on the primary key, so
  the payload never earned an index-only read; the narrow replacement is the
  one the planner takes.
- `qbit_share_ledger_accepted_miner_recent_idx`: its `payout_order_key`
  INCLUDE had no reader.
- `qbit_share_ledger_accepted_window_idx`, `(job_issued_at, share_seq DESC)`:
  `job_issued_at` is only ever a filter on a `share_seq` walk.
- `qbit_share_ledger_template_height_idx`: no native query filters on
  `template_height`. `qbit_shares_since_template_height`, the 001 operator
  replay function, is its only caller and now reads the primary key with a
  filter; if a native consumer appears, restoring it is one
  `CREATE INDEX CONCURRENTLY ... ON qbit_share_ledger (template_height, share_seq) WHERE accepted`.

`WHERE accepted` stays on every partial index. The native writers only
insert accepted rows, but every reader excludes rejected rows by contract
(the window and rollup tests write `accepted = false` rows to prove it), and
the predicate is what lets the frozen `qbit_prism_window` walk stay
index-only without carrying the column.

**The probe floor rule for any new `share_id` query.** A `share_id` lookup
with no `share_seq` bound has to descend one leaf index per attached
partition, because uniqueness is per leaf and the executor cannot prune on a
column that is not the partition key. Every native `share_id` read of the
ledger therefore carries
`share_seq >= qbit_prism_share_probe_floor()`, a `STABLE` function that
returns the lower bound of the attached partition two below the one the next
`share_seq` lands in, read from `qbit_prism_share_partitions`, so PostgreSQL
prunes to at most three leaves holding rows (plus the empty lead) at executor
start, whatever width each partition was created with. The proof is
`Subplans Removed` in the plan of the query. Carry the same bound in any new query that looks a
share up by `share_id`; if the question is only whether a header was ever
credited, read `qbit_prism_share_hashes` instead and do not touch the ledger
at all. An unbounded probe stays correct and gets slower with every partition:
the design spike measured 31 buffers against 8.

The one place that still probes without the bound is the append path's replay
comparison, and only on a miss: when `qbit_prism_share_hashes` says the header
was credited under this `share_id` but the bounded probe does not find the row,
the append probes once more without the bound, so a replay of any online row is
still compared field by field. A row that has left the online ledger cannot be
compared and is refused as the duplicate it is
(`duplicate-share: header already credited globally, and its share is
archived`). The coordinator's replays are seconds old, so that refusal is
unreachable in practice and safe if reached.

One invariant this leaves is worth stating plainly. A row inserted around the
append path, by a direct `INSERT` from an operator or a harness, with a
`share_id` already present in another partition is not refused by PostgreSQL.
The native writers cannot create one; `share-archive plan` and the archive
verification report any `share_id` present in more than one attached leaf.

Known full scan: the boundary and tail passes of
`dashboard_hashrate_rollups.sql` bound `accepted_at` through CTE values the
planner cannot estimate, so each is a full index-only scan (of
`accepted_seq_walk_idx` after 013, of `accepted_recent_idx` before it). That
predates 013 and is a query change, not an index change.

### Measuring the trim on production

Run these on the production database after 013 and before scheduling #144,
in this order. The `ANALYZE` comes first: the statistics captured for #144
were hundreds of times below the row count, and every plan is provisional
until they are current.

```sql
ANALYZE qbit_share_ledger;

-- Statistics after the ANALYZE, against the real row count.
SELECT c.reltuples::bigint, s.n_live_tup, (SELECT count(*) FROM qbit_share_ledger) AS rows
FROM pg_class c JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE c.relname = 'qbit_share_ledger';

-- Whether vacuum has run, so the visibility map is set and index-only scans
-- do not fall back to the heap; stats_reset bounds the scan counts below.
SELECT s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze,
       s.n_tup_ins, s.n_dead_tup, age(c.relfrozenxid) AS frozen_age,
       pg_postmaster_start_time(), d.stats_reset
FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid
JOIN pg_stat_database d ON d.datname = current_database()
WHERE s.relname = 'qbit_share_ledger';

-- Every index with its size and scan count; a zero-scan index has had no
-- reader since stats_reset.
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size,
       idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes WHERE relname = 'qbit_share_ledger'
ORDER BY pg_relation_size(indexrelid) DESC;

-- The gate: the payout page walk as the pool snapshot runs it, with the
-- network difficulty of the moment. "Heap Fetches" near zero on
-- qbit_share_ledger_accepted_seq_walk_idx means the covering index earns
-- index-only reads; large Heap Fetches mean the visibility map is cold
-- (VACUUM qbit_share_ledger, then run it again).
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*), sum(counted_difficulty)
FROM qbit_prism_window(clock_timestamp(), (<network difficulty> * 8)::numeric);
```

Record the index sizes before and after 013, the scan counts, the
`Heap Fetches` lines of the page walk, and the share acknowledgement latency
histogram from `/metrics` before and after, in #153 and #144.

## Share ledger partitions and retention

Nothing ever left `qbit_share_ledger`, and everything that touches it paid for
that: every insert maintained the primary key, a global `UNIQUE (share_id)` and
four secondary indexes at ever-growing depth, and vacuum, base backups, restore
drills and cache hit rates all degraded with the lifetime share count. What
reads the table is narrow. The payout window reads the newest slice bounded by
difficulty, the audit of a landed block reads its own range, the dashboard
aggregates cover at most the last 24 hours, and a handful of probes by
`share_id` run seconds after the share was written. Since #267 a landed block's
audit is rebuilt from the ledger at read time rather than stored, so no aged
share row is needed online except by the audits that still depend on it.

Migration 017 (#144) gives the ledger a shape whole partitions can leave from,
and decision D6 of #260 says when one may: shares are immutable and are never
deleted, and
retention is detach-and-archive of a whole partition after the audits that
depend on it have been sealed. This section is the operator procedure. The
reasoning, the reader inventory and the archive format specification are in
[the design record](prism-share-ledger-partitioning.md), and the conversion
itself is in
[the migration guide](prism-rust-migration.md#migration-017-the-share-ledger-partition-conversion-applied-online).

### The layout

The ledger is `RANGE (share_seq)`. `share_seq` is the routing key every ordered
read already carries, a `bigserial` routing key cannot fail, and
`PRIMARY KEY (share_seq)` survives on the parent. There is no DEFAULT
partition: a missing partition would otherwise silently collect rows that a
later ATTACH would have to move.

Partitions are cells of a grid. Each new one is `partition_rows` wide and
starts where the highest attached one ends, so with the width unchanged cell k
covers `[k*rows, (k+1)*rows)`; `rows` is `partition_rows` in
`qbit_prism_share_partitioning`. The default is 2^24 = 16,777,216 rows, about
7.5 GB of heap plus about 20 GB of indexes at the production row shape, five
days at 39 shares per second and nine hours at 500. Names are
`qbit_share_ledger_p<n>`, n one above the highest number the catalog has ever
recorded in any state (`qbit_prism_share_partition_next_number()`): a name is
never reused, and never derived from the width. Changing the width affects
partitions created afterwards only; bounds already attached stay as they are,
and the next partition starts at the last bound with the new width. The
probe floor (`qbit_prism_share_probe_floor()`, below) is read from the
attached bounds, not from the width, so raising the width does not widen the
probes over the narrower partitions still attached. The
release table becomes `qbit_share_ledger_p0`, `[MINVALUE, bound)`, and spans
as many cells as it needs.

Two catalog tables record the settings and what happened to each partition.
PostgreSQL's `pg_inherits` remains the authority on what is attached and with
which bounds; the catalog adds what happened to a partition after it left, and
every `share-archive` command cross-checks the two.

`qbit_prism_share_partitioning`, one row:

| Column | Meaning |
| --- | --- |
| `singleton` | always true, the primary key of the one row |
| `partition_rows` | the grid width in rows (default 16,777,216, accepted range 1,048,576 to 1,073,741,824) |
| `lead_partitions` | how many empty partitions to keep attached ahead of the sequence (default 4, accepted range 1 to 64) |
| `conversion_bound` | the exclusive upper bound chosen for the release table when 017 prepared the conversion |
| `converted_at` | when the swap completed; NULL until then, and the maintenance function does nothing while it is NULL |
| `updated_at` | when this row last changed |

`qbit_prism_share_partitions`, one row per partition:

| Column | Meaning |
| --- | --- |
| `partition_name` | the relation name, the primary key |
| `lower_seq`, `upper_seq` | the recorded bounds; `lower_seq` is NULL for MINVALUE, which only `qbit_share_ledger_p0` has |
| `state` | `attached`, `detached` or `dropped` |
| `attached_at` | when the partition joined the ledger |
| `sealed_at` | when the last audit row depending on this partition got its stored canonical bytes |
| `archive_uri`, `archive_manifest_sha256` | where the archive was written and the SHA-256 of its manifest |
| `archive_rows`, `archive_rows_sha256` | the row count in the archive and the SHA-256 of the uncompressed row stream |
| `archived_at`, `archive_verified_at` | when the archive was written and when it was last re-read and compared |
| `detached_at`, `dropped_at` | when the partition left the ledger and when its relation was dropped |

The table's own CHECK constraints hold the lifecycle: an archive URI implies a
manifest digest and an `archived_at`, a verification implies an archive, a
`detached` state implies `detached_at`, a `dropped` state implies both
timestamps, and any state other than `attached` implies
`archive_verified_at IS NOT NULL`. A partition cannot be recorded as having
left without having been archived and verified first.

### Lead partitions

`qbit_prism_share_partition_ensure()` keeps `lead_partitions` empty partitions
attached above the next `share_seq`, about 67 M rows of headroom at the
defaults. Every instance calls it at startup and then every
`PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS`, default 60. The call takes
the online migration runner's advisory lock, so it is serialized per schema,
two frontends never race on one partition name, and no partition is created
while a conversion is swapping the table. It creates
nothing when the lead is intact and returns the number it created.

A new partition is a standalone table `LIKE` the parent with a validated bound
`CHECK`, its own `UNIQUE (share_id)` and the immutability trigger, and is then
attached. The attach takes only SHARE UPDATE EXCLUSIVE on the parent: appends
and reads continue. A name already held by any relation is refused and never
adopted:

```
refusing to create share ledger partition qbit_share_ledger_p7: a relation already holds that name
```

Should an insert ever find no partition for its `share_seq`, PostgreSQL
refuses it with SQLSTATE 23514 and the append path runs `ensure` once and
retries the share, so a lead that ran out costs one retry rather than a lost
share. That is the recovery, not the plan: if it is reached, the ensure task is
not running on any instance, or the lead is too small for the share rate.
Alert on the distance between the highest attached `upper_seq` and
`qbit_prism_share_next_seq()`, and raise `lead_partitions` rather than relying
on the retry:

```sql
SELECT (SELECT max(upper_seq) FROM qbit_prism_share_partitions WHERE state = 'attached')
         - qbit_prism_share_next_seq() AS lead_rows,
       (SELECT lead_partitions * partition_rows FROM qbit_prism_share_partitioning WHERE singleton) AS target_rows;
```

### The online horizon

A share row stays online while any of these holds. All five are properties of a
whole partition, `share-archive plan` checks each one and names its blocker,
and a partition leaves only when all five clear.

1. **It can still be in a payout window.** Its `share_seq` is at or above the
   floor of the current window taken at four times the requested weight,
   `qbit_prism_window(clock_timestamp(), 8 * D * 4)` with D the network
   difficulty the operator supplies, so a difficulty rise of up to 4x between
   two retention runs cannot reach into archived history.
2. **It is younger than the retention age**, 30 days by default. The longest
   dashboard read of raw rows is 24 hours, so this covers every one of them
   with margin.
3. **The hashrate rollup watermark has not reached its newest row.** While
   `qbit_hashrate_rollup_progress` is behind the partition's newest committed
   `share_seq`, or the share sequence has not yet passed the partition's upper
   bound, the permanent rollup tables do not yet hold its whole contribution.
   The mark is the newest row, not the bound: the sweep advances only to rows
   that committed, and a `share_seq` an append drew and rolled back is never
   folded.
4. **A landed block's audit still depends on it**, that is, an audit row whose
   share snapshot intersects the partition and that has no stored
   `canonical_audit_bytes`. Sealing clears this condition.
5. **An unfinished block candidate or a deferred share references it.**

### Sealing, per D6

Before a partition is detached, every audit row whose snapshot intersects it is
sealed: the canonical artifact is rebuilt from the still-online shares, its
digest is checked against the advertised `audit_bundle_sha256`, and the bytes
are stored in `canonical_audit_bytes`, exactly as imported legacy audits are
stored since #325. The block then keeps serving under its advertised digest
after its shares are gone, through the artifact route, the audit bundle route
and the operator tools alike, because readers prefer stored bytes over
reconstruction whatever the row's shape.

The price is the artifact size back per archived block, about 111 MB
uncompressed at 100,000 shares, TOAST-compressed in the column. The
alternative, pinning every landed block's window online forever, would have
left cost scaling with blocks found, which is what this work exists to stop.
Sealing is idempotent and can run ahead of any detach, so run it early and
keep it out of the critical path of the detach.

Two documented reader changes follow from a detach, and both are intended:

- A miner's `last_share_at` in the miner share summary is the newest share
  within the online horizon, so a miner whose last share is older than the
  retention age reads `null`.
- `/audit/share-window?anchor=` for an anchor inside an archived range returns
  `rows: []`. The shares are in the archive and in every sealed artifact whose
  window covers them. An anchor-to-archive index is deferred.

Everything else that was lifetime-scoped keeps working across a detach. Block
solver attribution moved onto `qbit_pool_blocks.solver_*` in migration 016,
written at landing and backfilled for every existing block.
`accepted_share_count` and `distinct_miner_count` in `/audit/latest-evidence`
and `GET /public/v1/hashrate-series?range=all` are served from the permanent
rollup tables plus the raw tail above the watermark, which condition 3 keeps
online until the sweep has folded it.

### The operator procedure

`qbit-prism-server share-archive <command>` runs as the operator against the
primary, with the frontends running. Nothing here needs a maintenance window.

| Command | Effect |
| --- | --- |
| `plan --network-difficulty D [--retention-days N] [--window-multiple M] [--check-duplicates]` | every partition with its bounds, row count, age, and each of the five conditions above with its blocker named; nothing is changed |
| `seal <partition>` | stores canonical bytes for every audit row whose snapshot intersects the partition and has none, verifying each against its advertised digest; records `sealed_at` when none is left |
| `archive <partition> --dir <root> [--force]` | writes `<root>/qbit_share_ledger/<partition>/<manifest-sha256>/rows.ndjson.gz` and `manifest.json`, records the URI, digests and row count; refused while the share sequence has not passed the partition, since appends could still land in it, and refused out of order, so the chain of manifests stays contiguous, and refused over a predecessor whose archive is not verified, so every link is to a certified manifest; `--force` writes an archive again, clearing its verification and that of every later archive, which must then be written and verified again in order, each over its verified predecessor, and is refused once a later archived partition has left the ledger |
| `verify <partition> --dir <root>` | re-reads the archive, checks both digests and that the manifest chains, without a gap, to the nearest archived partition, whose own archive has to be verified, and, while the partition is attached, streams the live rows again and compares; records `archive_verified_at` for that full comparison, and only once the share sequence has passed the partition |
| `detach <partition> --network-difficulty D [--retention-days N] [--window-multiple M] [--check-duplicates]` | requires every plan condition, sealed, archived and verified, and counts the live rows against the archive again; `DETACH PARTITION ... CONCURRENTLY`, finalized if an earlier attempt was interrupted; the table stays as a standalone relation. A partition PostgreSQL no longer holds, whether an earlier run's catalog update was lost or the DDL ran by hand, is only reconciled into the catalog as sealed and verified; otherwise it is attached again under its recorded bounds and sealed first |
| `drop <partition> --dir <root>` | requires `detached`, sealed and verified; reads the recorded archive back from disk, checking both digests against the catalog, and counts the rows against it again; `DROP TABLE`; the archive is the copy of record |
| `restore <manifest> --dir <root> [--attach]` | recreates the partition table from the archive, verifies count and digests, and optionally attaches it under its recorded bounds |

`plan` also reports the attached partition count, the lead ahead of the
sequence, and any `share_id` present in more than one attached leaf.

A session that retires the oldest partition, with the network difficulty of the
moment and the default retention age:

```sh
ARCHIVE_ROOT=/var/lib/qbit-prism/share-archive

qbit-prism-server share-archive plan --network-difficulty 402304 --retention-days 30
qbit-prism-server share-archive seal qbit_share_ledger_p0
qbit-prism-server share-archive archive qbit_share_ledger_p0 --dir "$ARCHIVE_ROOT"
qbit-prism-server share-archive verify qbit_share_ledger_p0 --dir "$ARCHIVE_ROOT"
qbit-prism-server share-archive detach qbit_share_ledger_p0 --network-difficulty 402304 --retention-days 30
qbit-prism-server share-archive drop qbit_share_ledger_p0 --dir "$ARCHIVE_ROOT"
```

Run `plan` again after `drop` and keep its output with the run. `verify`
compares the archive against the live rows only while the partition is still
attached, so the order above is the order that gets that comparison: archive,
verify, then detach. Both refuse a partition the share sequence has not passed,
because a comparison of a partition that can still receive rows proves nothing
about the rows still to come; and since ledger rows are immutable, `detach` and
`drop` each count the rows against the archive again before acting, so an
append that committed after the comparison is refused rather than lost. `drop`
also reads the archive back from disk under `--dir` right before `DROP TABLE`:
`archive_verified_at` proves the archive was complete when it was compared, not
that its files are still there, and a copy of record that went missing or was
altered in between is refused rather than made the only copy.
Between `detach` and `drop` the rows are still on disk
under the standalone relation and can be read directly by name, which is the
last chance to look at them without a restore; do not collapse those two steps
into one to save a command. Read the catalog back afterwards:

```sql
SELECT partition_name, lower_seq, upper_seq, state, sealed_at, archived_at,
       archive_verified_at, detached_at, dropped_at, archive_rows,
       archive_uri, archive_manifest_sha256, archive_rows_sha256
FROM qbit_prism_share_partitions ORDER BY upper_seq;

-- What PostgreSQL actually holds attached, which is the authority.
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) AS bounds
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'qbit_share_ledger'::regclass
ORDER BY c.relname;
```

### What to keep outside PostgreSQL

The archive is the copy of record for every share that has left the ledger, so
the archive root is accounting data. Keep it off the database volume, back it
up with the same retention and encryption as the database backups, and keep it
in the isolated-restore evidence set described in
[one-way migration and isolated-restore reconciliation](#one-way-migration-and-isolated-restore-reconciliation).
A share range that has left the online ledger is proved from its manifest
chain, not from a database export.

The layout under the root:

```
<root>/qbit_share_ledger/<partition_name>/<manifest-sha256>/
    rows.ndjson.gz     gzip; one JSON object per line, share_seq ascending
    manifest.json      UTF-8, no trailing newline
```

Every `archive` run writes into a new version directory named after the
SHA-256 of the manifest it holds, and the catalog points at that version only
once both files and their directory entries are durable: `archive_uri` is the
full path of the recorded `manifest.json` and `archive_manifest_sha256` is the
directory's name. A partition directory can hold more than one version after
`--force` or an interrupted run; the earlier ones stay on disk so that a failed
catalog update cannot destroy the copy of record, but only the version the
catalog records is that copy, and `verify` reads that one. An archive written
before this version scheme, with both files directly under the partition
directory, is still read by `verify`, which checks its digest against the
catalog as for any other.

A row is the whole ledger row with a fixed key order: difficulties are decimal
strings (`numeric(78,0)`), timestamps are microseconds since the epoch (exact
for `timestamptz`), the P2MR program is hex.

```
{"share_seq":1,"share_id":"alice:…","miner_id":"alice","payout_order_key":"…",
 "p2mr_program_hex":"…","share_difficulty":"1","network_difficulty":"100",
 "template_height":100,"job_id":"job","job_issued_at_us":1000000,
 "accepted_at_us":1758000000000000,"ntime":1,"accepted":true,"reject_reason":null,
 "credit_policy":null,"writer_id":"…","writer_epoch":0}
```

The manifest carries `schema` (`qbit.prism.share-archive.v1`), the recorded
bounds, what the rows hold (`row_count`, first and last `share_seq`, first and
last `accepted_at_us`; zero rows is a valid archive), the digests and sizes of
both the uncompressed stream and the file (`rows_sha256`, `rows_gz_sha256`,
`rows_bytes`, `rows_gz_bytes`), the chain link, the
`qbit_prism_schema_migrations` versions at archive time, and who created it
when. The full field list is in
[the design record](prism-share-ledger-partitioning.md#archive-format-v1).

**The manifest chain.** `previous_manifest_sha256` and `previous_upper_seq`
point at the archived partition with the next-lower `upper_seq`, and are null
only for the first. A gap between one manifest's `previous_upper_seq` and its
own `lower_seq` means a partition is missing from the chain; `archive` refuses
to write such a manifest and `verify` refuses to certify one, so partitions are
archived in `upper_seq` order. The manifest's own SHA-256 is what the catalog
and the next manifest record, so a manifest cannot be rewritten without
breaking both: `archive --force` clears the verification of every later
archive along with its own, and each has to be written again with `--force`,
in `upper_seq` order, and verified again. That order is enforced, not
assumed: a link proves one hop, and `verify` checks only the manifest's own
link, so both `archive` and `verify` refuse a partition whose nearest archived
predecessor has no `archive_verified_at`. A verification therefore stands for
the whole chain below it, and a repair proceeds from the rewritten partition
up, verify then archive then verify; a later partition cannot be written
against a middle manifest whose own link is obsolete, certified on that one
hop, and detached over a chain that can then never be repaired. The rewrite
is refused once a later archived partition has been detached or dropped,
because that partition can no longer be archived from its live rows; bring
the missing files back from a copy of the archive root instead.

**Verifying an archive by hand.** `rows_sha256` is the SHA-256 of the
uncompressed byte stream, so a verifier streams the file without materializing
it. Start from the version directory the catalog records, which is the
`archive_manifest_sha256` from the catalog read above (the `archive` command
prints the same value); the off-host copy has no catalog to ask, so carry the
digest along with it:

```sh
MANIFEST_SHA256=…                        # archive_manifest_sha256 for qbit_share_ledger_p0
cd "$ARCHIVE_ROOT/qbit_share_ledger/qbit_share_ledger_p0/$MANIFEST_SHA256"
gzip -dc rows.ndjson.gz | sha256sum      # must equal the manifest's rows_sha256
sha256sum rows.ndjson.gz                 # must equal the manifest's rows_gz_sha256
gzip -dc rows.ndjson.gz | wc -l          # must equal the manifest's row_count
sha256sum manifest.json                  # must equal $MANIFEST_SHA256, the directory's name,
                                         # and the next manifest's previous_manifest_sha256
gzip -dc rows.ndjson.gz | head -1        # the first row; its share_seq is first_share_seq
gzip -dc rows.ndjson.gz | tail -1        # the last row; its share_seq is last_share_seq
```

Do this on the archive copy that was written off the database host, not only on
the one the tool just produced; the point of the check is the copy that will
outlive the rows.

### Restoring a partition

`share-archive restore <manifest> --dir <root>` recreates the partition table
from the archive and verifies count and digests. With `--attach` it also
attaches it back under its recorded bounds, which makes the rows readable
through `qbit_share_ledger` again. Restore into an isolated database for
inspection wherever the question does not require the production ledger.
Attaching back into production is for reconciliation, not for routine reads:
the partition's rows are outside the online horizon by construction, so every
dashboard query that reaches them pays for a leaf that nothing else needs.
After an inspection, run `verify` and then `detach` and `drop` again rather
than leaving the partition attached.

### Measuring before and after

Acceptance criterion 4 of #144 is this same set of measurements on a
production-sized copy, before and after 017, taken by the operator. It needs
production access. Run `ANALYZE qbit_share_ledger` first, both times: the
statistics captured for #144 were hundreds of times below the row count, and
every plan is provisional until they are current. After 017 that statement
analyzes the parent and every attached leaf.

**Insert latency.** The share acknowledgement histogram
`qbit_prism_share_ack_seconds{result="accepted"}` in `/metrics` is the
end-to-end bound on a share: `mining.submit` frame arrival to completed
response write. Scrape it on every coordinator before the conversion and again
after, over comparable load, and compare percentiles from the bucket counts
rather than the average. These are elapsed ACK bounds, not measured ledger
deadlines, so read them alongside
`qbit_prism_database_pool_acquire_seconds` and
`qbit_prism_database_advisory_lock_wait_seconds` to tell an ingest change from
a database one.

**Vacuum duration.** The point of the conversion is that vacuum stops scanning
lifetime history, so measure it per partition and against the recorded
duration of the old whole-table run:

```sql
\timing on
VACUUM (VERBOSE, ANALYZE) qbit_share_ledger_p0;
VACUUM (VERBOSE, ANALYZE) qbit_share_ledger_p1;
```

Record the elapsed time, the index scan lines and the page counts of each, and
the sum against the single pre-017 `VACUUM (VERBOSE, ANALYZE) qbit_share_ledger`.

**Sizes and scan counts per partition.** After 017 there is one row per leaf
per index, which is where a partition that no reader touches becomes visible:

```sql
SELECT c.relname AS partition,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_with_indexes,
       pg_size_pretty(pg_relation_size(c.oid)) AS heap,
       s.n_live_tup, s.last_vacuum, s.last_autovacuum, s.last_analyze
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE i.inhparent = 'qbit_share_ledger'::regclass
ORDER BY c.relname;

SELECT relname AS partition, indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size,
       idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname LIKE 'qbit\_share\_ledger\_p%'
ORDER BY relname, pg_relation_size(indexrelid) DESC;
```

`idx_scan` is counted since `stats_reset`; read
`SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()`
with it.

**The page walk.** The payout window read is the query the conversion must not
regress. Run it as the pool snapshot runs it, with the network difficulty of
the moment, before and after:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*), sum(counted_difficulty)
FROM qbit_prism_window(clock_timestamp(), (<network difficulty> * 8)::numeric);
```

Record the total buffers and the `Heap Fetches` line on
`qbit_share_ledger_p<k>_accepted_seq_walk_idx`. Heap fetches near zero mean the
covering index is earning index-only reads; large heap fetches mean the
visibility map is cold, so vacuum the leaf and run it again. After 017 the walk
should touch the newest leaves only, and the older ones should not appear in
the plan at all.

**The bounded `share_id` probe.** This is the plan that proves the probe floor
is doing its job. `Subplans Removed` must be present and must account for every
leaf outside the floor's three:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT share_seq, share_id, accepted_at
FROM qbit_share_ledger
WHERE share_id = '<a recent share_id>'
  AND share_seq >= qbit_prism_share_probe_floor();
```

For contrast, and to keep the number that justifies the rule, run the same
query without the `share_seq` predicate and record its buffer count. It must be
the one that grows with the attached partition count.

#### Container evidence

Container evidence (PostgreSQL 16.15) is lock-semantics and phase-shape
evidence on a synthetic ledger, not a production absolute. It belongs here so
the production numbers above have something to be read against.

Taken on 2026-09-16 in Docker on an Apple M-series laptop: a synthetic
ledger of 1,000,000 production-shaped rows (1,000 MB with its indexes, 500
miners), `VACUUM (ANALYZE)` before each read, insert latency from 2,000
single-row inserts timed in PL/pgSQL with `clock_timestamp()`.

| Measurement | Before 017 | After 017 | Notes |
| --- | --- | --- | --- |
| Prepare, validate, swap | | 2.3 ms, 47 ms, 16.7 ms | the validation scan is the only term that grows with the table (about 300 MB of heap here); the swap renamed, created the parent, adopted five indexes, attached and created two lead partitions |
| Insert latency p50 / p95 / p99 | 0.031 / 0.048 / 0.066 ms | 0.032 / 0.047 / 0.069 ms into the release partition; 0.025 / 0.038 / 0.061 ms into a fresh lead partition | a hot leaf's indexes are small and cache-resident |
| `VACUUM (ANALYZE)` duration | 0.32 s | 0.44 s for every partition; 0.02 s for one lead partition | retention bounds the set vacuum visits |
| Page-walk plan, `qbit_prism_window` over about 200k shares | 15,809 shared buffers | 17,353 shared buffers | the same recursive page walk through the partitioned parent |
| Bounded `share_id` probe | | 8 shared buffers over the three attached leaves | `Subplans Removed` appears once the floor clears a partition; at 1 M rows the floor is still 0 |
| Unbounded `share_id` probe | | 6 shared buffers | one index descent per attached leaf; grows with the attached count, which is the cost the bound removes |

## Offline pool-fee and CTV fee-rate changes

`qbit-prism-server policy-transition --to /protected/next.env` changes the
cluster's pinned fee policy after **every frontend has stopped**. It supports
pool-fee enablement, recipient and basis-point changes, and explicit/automatic
CTV fee rates and premiums. Signing keys, genesis, username fallback, output
ordering, payout thresholds, CTV enablement and settlement layout must stay
unchanged. Signing-key rotation and multi-epoch verification are deferred.

1. Preserve the current configuration, signing material and database backup.
   Prepare a protected env file containing the target overrides. For example,
   for an already-enabled pool fee and CTV settlement:

   ```dotenv
   PRISM_POOL_FEE_BPS=200
   PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT=2000
   PRISM_CTV_FANOUT_FEE_PREMIUM_BPS=12000
   ```

   These are examples, not market-rate recommendations. The command validates
   the target CTV fee against the node's current relay and mempool floors; it
   can replace an old rate that is already below those floors. Mainnet still
   requires an explicit rate. Rates can change again before restart.

2. Disable automatic restarts and stop every frontend and standalone candidate
   or CTV worker. Graceful SIGTERM closes miner admission and drains workers;
   in the bundled stack use
   `docker compose stop --timeout 45 prism-coordinator prism-coordinator-2`.
   Confirm every `qbit_prism_instances.status.state` is `stopped`. A stale,
   `starting`, `draining`, `drained`, unknown or live heartbeat is refused by
   instance name. There is no force flag. If a crashed instance cannot record
   its own stopped marker, escalate for reviewed recovery; do not manufacture
   markers with SQL. Keep the qbit node and database available.

3. With the **current** configuration in the command's environment, run:

   ```sh
   qbit-prism-server migrate
   qbit-prism-server policy-transition --to /protected/next.env
   ```

   Migration `014` adds the immutable transition journal (`013` is used for
   the share ledger index trim). The command creates no frontend heartbeat.
   The target file overlays current `PRISM_*` and `QBIT_*` variables; omitted
   values retain their current values, and an empty value clears an optional
   setting. It supports dotenv quoting and `export`, never shell execution.
   Other stack variables are ignored. The target database URL must be unchanged.
   Both effective policies are checked with the normal configuration parser
   and pool-fee address resolution. The current fingerprint must match the
   database, and the target must actually change the policy.

4. Save the returned JSON. Under the settlement and ordering locks, with
   concurrent frontend registrations excluded, one transaction updates the
   fingerprint, increments `payout_revision` once, and journals the old/new
   public policies, revisions, database login, instance snapshot and candidate
   counts. It abandons unoffered pending candidates with `epoch-superseded` and
   releases their retained window/block payloads. A pending candidate with an
   already-landed block is refused until reconciled. Offered, offer-reserved
   and reconciliation candidates retain their original bytes, policy and keys;
   their old claims are fenced and they can resume immediately on restart
   without another offer. Parked rows stay parked for operator recovery.
   Existing audit and CTV manifest bytes are unchanged. Recovery evidence
   exports include the transition journal and its allocation sequence.

   A failure before commit leaves the policy and candidate dispositions intact.
   A timeout or lost commit response can mean success: inspect
   `qbit_prism_policy_transitions` and the cluster fingerprint/revision before
   retrying. Repeating a committed transition with the old environment fails
   the current-fingerprint check and does not increment the revision again.
   The journal contains public keys, never signing seeds or credentials.

5. Apply the effective target configuration to **all** frontends, run the normal
   `check-config`, then restart and verify health before restoring miner traffic.
   Restarting with the old policy is rejected and names the active payout
   revision. Miners disconnect during the stop and receive fresh work after
   reconnecting; revision-bound jobs prepared before the transition cannot be
   issued. Historical verification continues to use the unchanged public keys.

## Fatal-state recovery

A disconnected mature pool block or deep confirmed CTV fanout records a shared
fatal state in `qbit_prism_cluster.fatal_error`. The message names the
`block_hash` or `fanout_txid`, says `manual reconciliation required`, and names
`qbit-prism-server fatal-state clear --reason <text>` as the recovery command. Every
ledger write transaction then fails with `cluster halted: ...`, and commands
that open the ledger for writing fail at startup. Only the audited command below
ends the halt; there is no public API route and no force flag.

Clearing restores authority for new work only. It does not approve or forgive
accounting, and claims that expired during the halt still require fresh claims.

### Recovery commands

```sh
qbit-prism-server migrate
qbit-prism-server fatal-state show
qbit-prism-server fatal-state clear --reason "<nonblank explanation>"
```

Apply migration 010 with `migrate` before recovery. It adds the
`qbit_prism_fatal_state_events` audit table, works while the cluster is halted,
and does not register a frontend. Confirm on the writer:

```sql
SELECT version FROM qbit_prism_schema_migrations WHERE version = 10;
```

`fatal-state show` reads PostgreSQL only. It neither starts nor registers an
instance and needs no signing configuration. It prints JSON with `fatal_error`,
`set_at`, `block_hash`, `fanout_txid`, and `halted`, and exits nonzero while
halted and zero otherwise. `set_at` is null for a state recorded before
migration 010, whose set time is unknown. A database read failure is also a
nonzero exit, so keep the JSON with the exit status.

`fatal-state clear` uses the normal server configuration (database, qbit RPC,
chain/genesis, payout, and signing settings) to verify cluster identity. Under
the settlement, ordering, and instance locks it refuses unless:

- every stored `qbit_prism_instances` row has `status.state` `stopped` or
  `drained`;
- no live legacy writer lease exists;
- the current chain is stable and still contains every mature pool block and
  every deep confirmed fanout checkpoint; and
- normal block reconciliation leaves no unresolved disconnection and the
  carry-forward integrity report passes.

The clear and its audit `INSERT` commit in one transaction. Failures before
commit roll both back and leave the cluster halted; a lost response during
commit requires checking the durable state before retrying. The event records `fatal_error`,
`fatal_error_set_at`, `reason`, `operator_identity` (PostgreSQL `session_user`),
`database_role` (`current_user`), `cleared_at`, the `instances` snapshot, and
`reconciliation` (`genesis_hash`, `tip_hash`, `tip_height`, `blocks_checked`,
`deep_fanouts_checked`, and `integrity`).

The event identifies a database login, not a person. Prefer an individual
PostgreSQL login for `clear`. When a shared login is unavoidable, put the
incident ID, operator identity, and evidence reference in the reason.

### 1. Preserve evidence first

Collect evidence before stopping, restarting, or reconfiguring anything, and
store it with the incident record:

1. The `fatal-state show` JSON and exit status.
2. The disconnected block or fanout record, pool blocks at the affected heights,
   and their stored audit bundle digests.
3. The current qbit chain from every node the frontends use: tip hash, height,
   chainwork, and the confirmations of the named block or fanout.
4. The complete carry-forward integrity report and any reconciliation output.

Query the writer endpoint, not a replica, in a read-only session (for example
`PGOPTIONS='-c default_transaction_read_only=on' psql`). For a fanout, use its
parent block's height as `<affected_height>`.

```sql
SELECT fatal_error, updated_at, payout_revision, best_tip_hash,
       best_tip_height, best_chainwork
FROM qbit_prism_cluster WHERE singleton;

SELECT block_hash, block_height, parent_hash, coinbase_txid, chain_state,
       maturity_state, matured_at, inactive_since, disconnected_at
FROM qbit_pool_blocks WHERE block_hash = '<block_hash>';

SELECT fanout_txid, block_hash, chunk_index, settlement_status,
       confirmed_block_hash, confirmed_block_height, confirmed_depth, updated_at
FROM qbit_ctv_fanout_artifacts WHERE fanout_txid = '<fanout_txid>';

SELECT b.block_hash, b.block_height, b.chain_state, b.maturity_state,
       a.audit_bundle_sha256
FROM qbit_pool_blocks b
LEFT JOIN qbit_pool_audit_bundles a USING (block_hash)
WHERE b.block_height >= <affected_height>
ORDER BY b.block_height, b.block_hash
LIMIT 200;

SELECT fanout_txid, confirmed_block_hash, confirmed_block_height,
       confirmed_depth
FROM qbit_ctv_fanout_artifacts
WHERE settlement_status = 'confirmed' AND confirmed_depth >= 1000
ORDER BY confirmed_block_height DESC, fanout_txid DESC
LIMIT 20;

SELECT qbit_carry_forward_integrity_report();
```

```sh
qbit-cli getblockchaininfo
qbit-cli getblockheader <block_hash>
qbit-cli getrawtransaction <fanout_txid> true <confirmed_block_hash>
```

A header `confirmations` of -1 means the block is not in that node's active
chain.

### 2. Stop every frontend

Stop every `run` frontend gracefully with SIGTERM (bundled stacks:
`docker compose stop --timeout 45 prism-coordinator prism-coordinator-2`). Shutdown closes
admission and drains tasks and sessions for up to 30 seconds. Only then does the
server record `stopped`, and only if no session guard remains. If that marker
fails, the process exits with an error and its row keeps its previous status.
Keep deployment supervisors and automatic restart (Compose restart policies,
systemd units, orchestrators, HA managers) disabled until the final
verification.

Inspect every instance row:

```sql
SELECT instance_id, started_at, heartbeat_at,
       round(extract(epoch FROM clock_timestamp() - heartbeat_at)::numeric, 1)
         AS age_seconds,
       status->>'state' AS state, status->>'schema' AS schema,
       status->'ready' AS ready
FROM qbit_prism_instances
ORDER BY instance_id
LIMIT 200;
```

Every row must show `stopped` or `drained`. A running frontend's row holds its
health payload (`qbit.prism.audit-health.v1`) and no `state`. A `starting` row
never became ready. Only `run` frontends write rows: the one-shot commands
`self-check`, `import-audits`, `backfill-ctv` and `broadcast-ctv` pass the same
startup gates (schema and capabilities, the halt guard, the cluster
fingerprint) without registering, so a normal or failed exit leaves nothing
behind, and a run under a live frontend's `PRISM_INSTANCE_ID` leaves that row,
including its session-owner token, untouched. A `starting` row under a
generated UUID that an earlier 3.x.x build's one-shot command left behind is
still refused and still needs the investigation below; nothing removes it
automatically. Stale does not mean stopped: a heartbeat older than the
`self-check` window (`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15)` seconds) shows
only that reporting stopped. The process may be hung, paused, cut off from
PostgreSQL, or on an unreachable host, and
may resume. `clear` therefore rejects missing, `starting`, unready, unknown,
and old live states, however old the heartbeat.

Resolve each blocker by finding the process for that `instance_id` and stopping
it gracefully so it records its own marker. Do not insert, update, or delete
`qbit_prism_instances` rows, move `heartbeat_at`, or set or clear `fatal_error`
with SQL. The crashed-owner step in
[retained reservations](prism-session-sequence.md#retained-reservations-and-recovery)
is a different procedure and does not authorize a marker for this recovery. If
an instance cannot record `stopped` itself, stop and escalate for a separately
reviewed decision.

### 3. Reconcile before clearing

Establish whether the halt reflects the canonical chain. Compare the evidence
from independent nodes with `best_tip_hash` and `best_chainwork`. A node on a
lower-work or minority branch is repaired at the node, never by clearing.

If the block or fanout really is disconnected, resolve the chain and accounting
discrepancy (affected payouts, carry-forward balances, and fanout settlement)
through a separately reviewed reconciliation before running `clear`. This
runbook provides no SQL for that change. `clear` is not approval to forgive a
still-disconnected mature payout: it refuses while any mature pool block or deep
checkpoint is missing from the chain, and success means only that its checks
passed. Record the review reference, then repeat the evidence queries.

### 4. Clear the fatal state

Run `clear` from a host with the frontends' normal configuration, using the
operator's own database login:

```sh
set -o pipefail
qbit-prism-server fatal-state clear \
  --reason "<incident>: <operator>; <block or fanout> reconciled per <review>; evidence <ref>" \
  | tee fatal-state-clear.json
```

Save the success JSON with the incident record, along with the audit event:

```sql
SELECT cleared_at, operator_identity, database_role, reason, fatal_error,
       fatal_error_set_at, instances, reconciliation
FROM qbit_prism_fatal_state_events
ORDER BY cleared_at DESC
LIMIT 5;
```

A validation or reconciliation failure clears nothing and writes no event. Correct the reported blocker
and rerun deliberately; do not loop. If the connection drops around commit, the
outcome is unknown: check `fatal-state show` and the event table before
rerunning.

### 5. Restart and verify

1. Confirm `qbit-prism-server fatal-state show` exits zero, and save its JSON.
2. Record the ledger head before admitting traffic:

   ```sql
   SELECT share_seq, accepted_at FROM qbit_share_ledger
   WHERE accepted ORDER BY share_seq DESC LIMIT 1;
   ```

3. Re-enable supervisors and start the frontends. Each `/healthz` must return
   200 (see [health checks](#health-diagnostics-and-validation)), and each
   instance row must carry a fresh, ready health payload.
4. Confirm new accepted shares. The step 2 query must return a higher
   `share_seq` with a later `accepted_at`, and each frontend's
   `qbit_prism_accepted_shares_total` and
   `qbit_prism_share_ack_seconds_count{result="accepted"}` must increase.
5. Keep the evidence, reconciliation reference, clear JSON, audit event, and
   these checks together.

If a frontend reports `cluster halted` again, a new fatal state was recorded.
Start again from evidence collection.

## Health, diagnostics, and validation

`/healthz` returns 200 only when the process has fresh work for its observed tip
and current payout revision and job delivery can progress; otherwise it returns
503. The HTTP handler reads a published snapshot and fails closed when that
snapshot becomes stale. Database or node outages therefore cannot keep an old
green response indefinitely. A tracked task polling beyond two seconds, or an
health/metrics publication exceeding the existing freshness budget, also
returns 503 with
`ok=false`, `ready=false`, and `status="runtime-stalled"`; the corresponding
`qbit_prism_runtime_task_stalled{task="..."}` is 1. This live check requires a
surviving runtime worker to serve the probe; a completed long poll retains
metric evidence without keeping readiness failed. The publication progress
guard ends before heartbeat/prune maintenance; their asynchronous waits do not
mark a just-published snapshot runtime-stalled.

The coordinator health payload also supplies three known 2.x compatibility
aliases: `ledger_backend` is `postgres-native` for the PostgreSQL backend,
`accepted_block` is whether `found_block_count` is positive, and
`accepted_block_count` copies that count. `ready_miner_count` and `max_blocks`
remain unmapped because their native sources have not been agreed.

`/metrics` exports native process health, accepted/rejected share and block
counters, runtime workers, connections, pending builds, current-work delivery
coverage, and delivery outcomes. It also includes share ACK latency and reject
reasons, initial-work waits, candidate backlog, collector pool waits and status,
runtime lag, and RSS; the full [native family inventory](prism-native-metrics.md)
identifies the timing families declared without samples. Scrape every instance
with its own label; process counters reset after restart. Dashboard accounting
is read from PostgreSQL across instances. Detailed Python queue, writer lease, watchdog,
and incremental-refresh metrics no longer describe this runtime.

The coordinator (`run`) serves the last complete metrics publication and adds
three gauges at scrape time:

| Gauge | Meaning |
| --- | --- |
| `qbit_prism_metrics_snapshot_available` | `0` before the first publication; `1` afterward, including when stale |
| `qbit_prism_metrics_snapshot_stale` | `1` when missing or older than the freshness budget; otherwise `0` |
| `qbit_prism_metrics_snapshot_age_seconds` | Monotonic age in seconds; `-1` before the first publication |

The coordinator uses the same budget as `/healthz`:
`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15)` seconds. The setting is read as an
unsigned whole number of seconds from 1 through 86400, defaulting to 2 when
absent; invalid values fail startup. Compose supplies 5 by default. Both values
give a 15-second freshness budget. The publisher ticks at this configured
interval, so publication cadence and the staleness budget stay aligned.
Once the age exceeds that budget, a scrape sets `qbit_prism_health_state` to `0`
while retaining the other cached samples. Collector age/availability and runtime
state are overlaid from memory at scrape time; this does not refresh the cached
body timestamp or a collector's last-success timestamp.
Scraping neither renews the publication age nor queries the database.

Both `run` and `public-api` return HTTP 200 for `/metrics`, including missing
and stale observations. Inspect the freshness signals and `/healthz` rather
than treating a successful scrape as readiness. GET and HEAD responses carry:

- `Cache-Control: no-store`.
- `X-Prism-Metrics-State: fresh`, `stale`, or `unavailable`.
- `Age`: elapsed whole seconds, rounded down, including `0`; omitted when unknown.
- `Warning: 110 qbit-prism "metrics snapshot is stale; serving last complete payload"`
  only when the state is `stale`.

The public role derives these headers from its existing readiness probe age,
also exported as `qbit_prism_public_ledger_probe_age_seconds`. Its budget is
`max(3 * PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS, 15)` seconds; the probe
interval defaults to 5 seconds. Before the first completed probe the state is
`unavailable`. A recent failed probe is still `fresh`, with
`qbit_prism_public_ledger_ready 0`; freshness does not mean the database is ready.
The public role keeps its existing metrics body, without the coordinator's
three snapshot gauges.

Useful checks:

```sh
qbit-prism-server check-config
qbit-prism-server healthcheck --url http://127.0.0.1:3341/healthz
curl --silent --show-error --include --max-time 5 http://127.0.0.1:3341/metrics
qbit-prism-server self-check
bash test/prism-native-tests.sh
QBITD_BIN=/path/to/qbitd bash test/prism-native-tests.sh live
```

The database test wrapper starts a private local cluster unless
`PRISM_TEST_DATABASE_URL` is supplied. Its default mode runs the whole workspace
and the three explicit `--ignored` database targets, as CI does, and checks the
integration gate's manifest so no gated test passes without running (see
[the integration test gate](prism-integration-test-gate.md)). Live tests add
actual qbitd regtest and bounded CPU mining. Use an isolated database for tests. The native builder
benchmark measures CPU build/verify work, not end-to-end accepted-share capacity;
see [measurement](prism-payout-artifact-measurement.md) and
[optional qualification](prism-capacity-readiness.md).

The physical failover test uses disposable PostgreSQL primary/synchronous
standby processes and two ledger clients through a stable TCP endpoint. It
checks survival of acknowledged IDs, deduplication, and resumed writes after
immediate primary loss and promotion:

```sh
PRISM_TEST_PG_BIN_DIR=/usr/lib/postgresql/16/bin \
  cargo test --locked -p qbit-prism-server --test postgres_failover -- --nocapture
```

The test skips unless that server-tool directory is provided. Its test proxy
and promotion sequence do not replace validation of a production HA manager.

The collector failure/recovery test requires a disposable PostgreSQL database
and is ignored by ordinary `--all-targets` runs. Invoke it explicitly when
running without the wrapper:

```sh
PRISM_TEST_DATABASE_URL=postgresql://test_user@127.0.0.1:5432/test_db \
  cargo test --locked -p qbit-prism-server --test observability_database -- --ignored
```

It verifies valid zero values, a blocked query, pool exhaustion, and recovery;
never point test fixtures at a production database.

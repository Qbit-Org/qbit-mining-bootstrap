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
only due rows. Recovery is explicit operator work, and there is no recovery
command for it yet (#268 tracks listing and abandonment). First preserve the
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
canonical bundle SHA before returning a logical v1/v1.1 bundle.

Share UPDATE, DELETE, and TRUNCATE are prohibited. Removing a share could break
both future accounting and already published audit hashes. No supported pruning
or share-compaction command exists. Keep the canonical share history and all
referenced snapshot rows. A future archive design must preserve exact range
reconstruction and verification before relaxing this invariant.

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

| Index | Definition | Native readers | Plan |
| --- | --- | --- | --- |
| `qbit_share_ledger_pkey` | `(share_seq)` | every `share_seq` walk that projects share rows: the payout page walk of `snapshot`, the audit range reads, `qbit_prism_window`'s ranking pass, the rollup batch in `rollups.sql`, the latest-share probe | index scan, then the heap for the projected columns |
| `qbit_share_ledger_share_id_key` | `(share_id)`, unique | the duplicate-share probes on submit, `share_accepted_at_ms` | index scan |
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

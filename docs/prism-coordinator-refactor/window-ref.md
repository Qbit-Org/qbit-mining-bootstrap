# Window Reference Design (`WindowRef` and `Ledger::read_window`)

Decision: **persisted job and candidate rows reference the payout window;
they never copy it.** A `WindowRef` names the window's anchor, the digest of
the prior balances it was built on and, when the window has shares, an
immutable range of `qbit_share_ledger` rows with its digest.
`Ledger::read_window` rebuilds the window from those rows or fails.
Migrations 007 (#265, `qbit_block_candidate_outbox`) and 008 (#273,
`qbit_prism_jobs`) embed the reference in the row's JSON document and add the
same six typed columns. Everything else a rebuild needs is stored with the
reference, never re-derived from local configuration. `qbit-prism` has the
borrowed parts API (#296, merged) so builds and verification stop cloning the
window. This is the pattern `qbit_prism_audit_snapshots` already applies to
`AuditBundle.shares`.

Status: design record for #264 (PR 1 of 3), part of #261 under #260, revised
after two adversarial reviews. The first confirmed the citations, both digest
definitions, the range predicate and the immutability trigger; the second
confirmed every earlier fix, that the CHECK accepts exactly three states and
the 131 citations it checked, and found one blocker, five should-fix items
and six nits, all resolved below. The operator has confirmed the shape
below; djh58's agreement (#273) is required before merge, and the one
question left for Anatolie (#283) is narrowed below. The parts API (PR 2) is
merged as #296. Code citations are at 3.x.x `41a2afd`, after #283 landed the
`ledger.rs` split (#300); `srv/` is `crates/qbit-prism-server/`.

## Problem

PostgreSQL rejects a JSONB container whose elements exceed 268,435,455 bytes
(measured). The production window from #254 is 372,257 shares, about 232 MB
canonical, about 679 bytes per share as JSONB (measured). On PostgreSQL 16.15
with 372,000 production-shaped shares, one share array (216,972,363 bytes of
text, 583 B per share) fits; two arrays and three arrays fail with `total size
of jsonb object elements exceeds the maximum of 268435455 bytes`, and one
single-window insert into `qbit_prism_jobs` took 6.23 s and wrote 44,752,760
bytes of WAL (measured). Every window-carrying document at the base holds more
than one copy:

| Document | Copies | Where |
| --- | --- | --- |
| `StoredPrepared` (`srv/src/coordinator.rs:46`) | 3 | `snapshot.shares`, `bundle.shares`, `bundle.reward_manifest.shares`; written at `:641` |
| `Candidate` (`srv/src/ledger/candidates.rs:4`) | 2 | `bundle.shares`, `bundle.reward_manifest.shares`; written at `:182-183` |
| legacy audit import (`srv/src/ledger/migration.rs:117`) | 2 + 2 | the inline bundle as JSONB, and both copies again in `canonical_audit_bytes` (bytea) |
| landed audit body (`srv/src/ledger/blocks.rs:140`) | 1 | `reward_manifest.shares` survives `remove("shares")` |

The claim phase writes no new window value. Of the 17 JSONB columns at the
base, the window reaches three: `qbit_prism_jobs.payload` at refresh,
`qbit_block_candidate_outbox.candidate` at enqueue, and
`qbit_pool_audit_bundles.audit_bundle` at landing and import.

## `WindowRef`

```rust
// qbit-prism-server; goes in `ledger/window.rs`, the module #283 landed (#300)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowRef {
    pub anchor_ms: i64,                   // Snapshot.anchor_ms, the predicate's cutoff
    #[serde(with = "hex32")]              // lowercase 64-hex in JSON, not an integer array
    pub prior_balances_digest: [u8; 32],  // qbit_prism::prior_balances_digest(&snapshot.prior_balances)
    pub shares: Option<ShareRange>,       // None: no ledger share falls inside the difficulty window
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareRange {
    pub first_share_seq: u64,             // inclusive, >= 1
    pub last_share_seq: u64,              // inclusive, >= first_share_seq
    pub share_count: u64,                 // rows matched by the window predicate, >= 1, <= last - first + 1
    #[serde(with = "hex32")]              // lowercase 64-hex in JSON, not an integer array
    pub snapshot_sha256: [u8; 32],        // sha256(serde_json::to_vec(&shares)), same bytes as qbit_prism_audit_snapshots
}
```

A reference is built from a `Snapshot` (`srv/src/ledger/window.rs:10-16`):
`anchor_ms` is copied, the digests are computed as in [Digests](#digests),
and `shares` is `None` when `snapshot.shares` is empty, otherwise the
sequence numbers of its first and last share, `shares.len()` and the snapshot
digest. Rust holds the digests as `[u8; 32]`. A derived `Serialize` would
write each as a JSON array of 32 integers, so both digest fields carry
`#[serde(with = "hex32")]`, a small adapter module in `ledger/window.rs`
(the server's `hex = "0.4"` is declared without its `serde` feature,
`srv/Cargo.toml:18`). It serializes with `hex::encode`, which is lowercase,
and deserializes only a string of exactly 64 characters from `[0-9a-f]`,
rejecting uppercase, so the JSON form equals the column's CHECKed text byte
for byte and the payload/column comparison below is exact. Whichever of #265
and #273 lands first adds the module, with a round-trip test and rejection
tests for uppercase, short, long and non-hex input. `share_count` is added to the issue's fields
because a range can contain rejected rows and rows stamped after the anchor,
so `last - first + 1` is not the count; it also turns a pruned range into a
cheap typed error before any hashing, as `srv/src/ledger/audit.rs:63-66`
already does for audit snapshots. `payout_revision` stays outside the
reference: the `qbit_prism_jobs.payout_revision` column for 008
(`srv/migrations/002_multi_instance.sql:52`) and the `Candidate.payout_revision`
field for 007 (`srv/src/ledger/candidates.rs:8`); `read_window` returns the
current value for the caller's fence ([Revision fence and reorgs](#revision-fence-and-reorgs)).

**An empty window is routine, not an edge case.** `refresh_once`
(`srv/src/coordinator.rs:468`) sets `bundle = None` when `snapshot.shares` is
empty (`:556-557`) and stores that `StoredPrepared` on every refresh
(`:627-641`, `:655-663`). A miner who resumes on that row (`:1498-1510`) or is
issued a job from it (`:1343-1356`) gets a bundle from `build_bundle` with
`Some(worker)`, which fabricates one synthetic share, `share_seq: 1`,
`share_id: "bootstrap-share"`, `job_id: "bootstrap-job"`, from the worker's
address and program, the anchor and the template's `curtime` (`:727-745`).
Landing already special-cases that window through `inline_shares`
(`srv/src/ledger/audit.rs:124-131`). `shares: None` is the empty window; the
bootstrap share is never in the ledger, so it is never in a range, and a
candidate found on such a job stores it inline
([Stored bundle inputs](#stored-bundle-inputs)).

### Relation to `audit_body_ref`

`crates/qbit-prism/src/lib.rs:80-81` glob-exports `audit_body_ref`, whose
private file-format types `AuditBodyRef`
(`crates/qbit-prism/src/audit_body_ref.rs:36-44`) and
`AuditWindowCompletenessProof` (`:58-68`) fix the vocabulary this record
reuses:

| `audit_body_ref` | `WindowRef` | Note |
| --- | --- | --- |
| `first_share_seq`, `last_share_seq`, `share_count` | the same names in `ShareRange` | same meaning: inclusive bounds and the matched count |
| `share_slice_digest_hex` | not reused; `snapshot_sha256` instead | it hashes `CountedShare` fields (`crates/qbit-prism/src/lib.rs:2616-2634`), a build output (`:915`, carried at `:705`) that omits `network_difficulty`, `template_height`, `job_id` and `ntime`. A reference must authenticate the builder's *input* before the build runs, so `snapshot_sha256` is over `AcceptedShare` JSON, the bytes `qbit_prism_audit_snapshots` already stores |
| `bundle_without_shares`, a `Value` with the top-level `shares` removed | `AuditBundleBody`, typed | the shape landing already stores (`srv/src/ledger/blocks.rs:136-140`); it still embeds `reward_manifest.shares`, so the [caveat](#bundle-ownership) applies to the v2 file format too |

### Columns (migration 007 and migration 008)

Both migrations add the same columns, types and CHECK; 007 to
`qbit_block_candidate_outbox`, 008 to `qbit_prism_jobs`.

| Column | Type | Meaning |
| --- | --- | --- |
| `window_anchor_ms` | `bigint` | `anchor_ms` |
| `window_prior_balances_sha256` | `text` | lowercase 64-hex |
| `window_first_share_seq` | `bigint` | `shares.first_share_seq` |
| `window_last_share_seq` | `bigint` | `shares.last_share_seq` |
| `window_share_count` | `bigint` | `shares.share_count` |
| `window_snapshot_sha256` | `text` | lowercase 64-hex, the `qbit_prism_audit_snapshots` convention (`srv/migrations/002_multi_instance.sql:115`) |

Three states, and the CHECK accepts exactly these:

| State | anchor and prior digest | four range columns | Rows |
| --- | --- | --- | --- |
| no window | both NULL | all NULL | per-worker `StoredJob` rows (`srv/src/coordinator.rs:58-67`); terminal outbox rows, whose `candidate` is already NULL (`crates/qbit-prism/sql/001_share_ledger.sql:97-104`) and whose window columns 007's terminal UPDATE also NULLs ([Immutability and retention](#immutability-and-retention)); every row written before 007/008 |
| empty window | both set | all NULL | a prepared row or candidate whose window has no ledger share |
| range | both set | all set, constrained | the common case |

```sql
ALTER TABLE qbit_block_candidate_outbox            -- 007; 008 repeats this on qbit_prism_jobs
    ADD COLUMN IF NOT EXISTS window_anchor_ms bigint,
    ADD COLUMN IF NOT EXISTS window_prior_balances_sha256 text,
    ADD COLUMN IF NOT EXISTS window_first_share_seq bigint,
    ADD COLUMN IF NOT EXISTS window_last_share_seq bigint,
    ADD COLUMN IF NOT EXISTS window_share_count bigint,
    ADD COLUMN IF NOT EXISTS window_snapshot_sha256 text;
ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_window_check CHECK (
    -- no window
    (num_nulls(window_anchor_ms, window_prior_balances_sha256) = 2
     AND num_nulls(window_first_share_seq, window_last_share_seq,
                   window_share_count, window_snapshot_sha256) = 4)
    OR (num_nonnulls(window_anchor_ms, window_prior_balances_sha256) = 2
        AND window_prior_balances_sha256 ~ '^[0-9a-f]{64}$'
        AND (
            -- empty window
            num_nulls(window_first_share_seq, window_last_share_seq,
                      window_share_count, window_snapshot_sha256) = 4
            -- range
            OR (num_nonnulls(window_first_share_seq, window_last_share_seq,
                             window_share_count, window_snapshot_sha256) = 4
                AND window_first_share_seq >= 1
                AND window_last_share_seq >= window_first_share_seq
                AND window_share_count BETWEEN 1 AND window_last_share_seq - window_first_share_seq + 1
                AND window_snapshot_sha256 ~ '^[0-9a-f]{64}$'))));
```

- **Every state is distinguishable by two columns.** `window_anchor_ms IS
  NULL` is "no window"; an anchor with `window_first_share_seq IS NULL` is
  "empty window"; `window_first_share_seq IS NOT NULL` is "range". The
  `num_nulls`/`num_nonnulls` guards matter because a CHECK that evaluates to
  NULL passes; a plain conjunction would accept a row with only some columns
  of a group set.
- **A legacy row cannot be confused with an empty window.** `ADD COLUMN`
  leaves every pre-007/008 row all-NULL, and an empty window always carries
  the anchor and the prior digest. So a `prepared:` row with a NULL anchor is
  a pre-008 row, a cache miss ([Compatibility](#compatibility)); one with an
  anchor and no range is a post-008 empty window. On the outbox a pending row
  with a NULL anchor is a pre-007 row, which 007 refuses to leave behind.
- No new index; retention (D6) may add one on `window_first_share_seq`.
  `ADD CONSTRAINT` has no `IF NOT EXISTS`, so each migration guards it to stay
  re-runnable like 002 to 005.
- Not an FK to `qbit_prism_audit_snapshots`: that would tie job and candidate
  lifetime to audit retention and to the bootstrap inline-share special case
  (`srv/src/ledger/audit.rs:124-131`). The columns cost about 150 bytes of
  payload per row.

### Payload and columns

The reference lives in both places on both tables, uniformly:

- The row's small JSON document embeds `window: WindowRef`, digests as
  lowercase hex through the `hex32` adapter ([`WindowRef`](#windowref)): the
  `Candidate` for 007 (`srv/src/ledger/candidates.rs:4-14`), the
  `StoredPrepared` for 008 (`srv/src/coordinator.rs:46-55`). For 007,
  `candidate_sha256` keeps covering the whole document including the
  reference, as it covers the candidate today
  (`srv/src/ledger/candidates.rs:182-183` at enqueue, `:73-79` at claim).
- The six columns are a typed duplicate for the CHECK and the future
  retention index, written in the same statement as the document.
- Decode reads the document, then compares it with the columns returned by
  the same statement: the claim `UPDATE … RETURNING`
  (`srv/src/ledger/candidates.rs:160`) and `Ledger::job`
  (`srv/src/ledger/jobs.rs:34-38`) gain the six columns. Any disagreement,
  including a document with a range and NULL range columns, is a decode
  error, surfaced like corruption (`Decode` below).

## `Ledger::read_window`

```rust
pub struct Window {
    pub shares: Vec<AcceptedShare>,               // ascending share_seq, exactly share_count rows; empty for shares: None
    pub prior_balances: Vec<CarryForwardBalance>, // by BalanceSource; digest-checked against the reference either way
    pub payout_revision: i64,                     // the current revision, read in the same snapshot; claim's probe hint and B's eligibility input, never copied into a resumed Snapshot
}

pub enum BalanceSource {
    Current,  // qbit_current_carry_forward_balances(): a claim of a candidate that isn't leased
    AsIssued, // the qbit_prism_balance_snapshots row keyed by the reference's digest: a resume of published work, or a leased candidate's audit
}

#[derive(Debug, thiserror::Error)]                // the server crate already depends on thiserror 2 (srv/Cargo.toml)
pub enum WindowError {
    #[error("window range incomplete: expected {expected} shares, read {got}")]
    Incomplete { expected: u64, got: u64 },       // range pruned or missing, or a different predicate
    #[error("prior balances changed since the reference was written")]
    PriorBalancesChanged { expected: [u8; 32], actual: [u8; 32] }, // balances moved, not corrupt; the caller decides
    #[error("as-issued balance snapshot missing")]
    BalanceSnapshotMissing { digest: [u8; 32] },  // AsIssued only: pruned or never written; a failure, not a miss
    #[error("window snapshot digest mismatch")]
    SnapshotDigestMismatch { expected: [u8; 32], actual: [u8; 32] },
    #[error("window database error: {0}")]
    Database(#[from] sqlx::Error),                // includes SQLSTATE 57014, the statement timeout
    #[error("window decode error: {0}")]
    Decode(#[source] anyhow::Error),              // share_from_row, hex, or payload/column disagreement: corruption
}

impl Ledger {
    pub async fn read_window(
        &self,
        window: &WindowRef,
        balances: BalanceSource,
    ) -> Result<Window, WindowError>;
}
```

`WindowError` converts into `anyhow::Error` with `?` at the `anyhow`-based
call sites (`process_candidate_inner` and `resume_job` both return `anyhow`
results); callers match on the variant first, because the variant decides
the caller action ([Errors and callers](#errors-and-callers)).

### Read

`Ledger` has one pool, the primary (`srv/src/ledger.rs:43-46`); there is no
replica pool, and `read_window` must never be given one, because the balances
and the revision must be current for the fence. One transaction on that pool,
`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY` as its first
statement, so the revision, the balances and every page of the range are one
consistent snapshot; a landing that commits mid-read cannot yield a revision
that disagrees with the balances. In order:

1. `SELECT payout_revision FROM qbit_prism_cluster WHERE singleton`.
2. The balances, by `BalanceSource`. `Current` reads them through the
   `read_prior_balances` statement `Ledger::snapshot` uses
   (`srv/src/ledger/window.rs:244-252`), then checks the prior-balances digest,
   else `PriorBalancesChanged`. `AsIssued` reads `SELECT balances FROM
   qbit_prism_balance_snapshots WHERE prior_balances_digest = $1` instead. No
   row is `BalanceSnapshotMissing`, and a row whose sorted set doesn't hash to
   its key is `Decode`: stored rows are immutable, so they can only disagree
   through corruption. `AsIssued` never returns `PriorBalancesChanged`. It costs microseconds,
   so a reference whose balances moved never pays for the window. The digest
   sorts internally (`prior_balances_digest`, `crates/qbit-prism/src/lib.rs`),
   but `AuditBundle.prior_balances` serializes in vector order, and the query
   has no outer `ORDER BY`. The function orders its own rows
   (`crates/qbit-prism/sql/001_share_ledger.sql:1544`), but SQL doesn't
   guarantee that order through a table expression. So `read_prior_balances`
   itself sorts the vector with the digest's comparator: `order_key`, then
   `recipient_id`, then `p2mr_program_hex`, compared as bytes. Refresh and every rebuild share that function, so both hand the builder the
   same vector. The as-issued read sorts with the same comparator, and
   `save_job` stores the set already sorted. An outer SQL `ORDER BY` alone wouldn't do, because it follows the
   database collation, not the byte order the digest uses. A #265 unit test
   requires any permutation of the same balances to give identical canonical
   bytes after that sort.
3. If `window.shares` is `None`: commit and return `Window { shares: vec![],
   prior_balances, payout_revision }` without touching `qbit_share_ledger`.
4. Otherwise an existence probe on `first_share_seq` and `last_share_seq`,
   two primary-key lookups; a missing row is `Incomplete` in one round trip
   before any page is read.
5. The range, in **ascending keyset pages of 4096**, mandatory, inside the
   same transaction:

```sql
-- SELECT_SHARE (srv/src/ledger.rs:40) with the predicate of read_range (srv/src/ledger/audit.rs:150)
SELECT share_seq,share_id,miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,
       share_difficulty::text AS difficulty,network_difficulty::text AS network_difficulty,
       template_height,job_id,job_issued_at,accepted_at,ntime,credit_policy
  FROM qbit_share_ledger
 WHERE accepted AND share_seq > $cursor AND share_seq <= $last
   AND accepted_at<=to_timestamp($anchor::double precision/1000)
   AND job_issued_at<=to_timestamp($anchor::double precision/1000)
 ORDER BY share_seq LIMIT 4096;  -- $cursor starts at first - 1 and advances to each page's last share_seq
```

   `Ledger::snapshot` is the precedent for keyset paging over this predicate
   (`srv/src/ledger/window.rs:217-218`), not for the direction: it pages descending
   from the cutoff and reverses at `:232` because it learns the start only
   when the weight is spent. `read_window` knows both bounds, so it pages
   ascending, rows arrive in canonical order and the digest streams.
6. Per page, `shares.len()` may not exceed `share_count` (more rows is also
   `Incomplete`: the reference was built with a different predicate); after
   the last page it must equal it. Then the snapshot digest, else
   `SnapshotDigestMismatch`.

`read_window` never returns a partial window. Every error leaves the caller
with no `Window` at all. Paging keeps the raw row buffer under about 2 MB,
gives each page its own statement timeout and lets the digest stream: the
hasher takes `[`, each share's compact JSON separated by `,`, then `]`, which
is byte-identical to `serde_json::to_vec(&shares)`, so no 233 to 260 MB
buffer exists.

### Digests

`snapshot_sha256` is SHA-256 over `serde_json::to_vec(&shares)`: a compact
JSON array of `AcceptedShare` values in ascending `share_seq`, fields in
declaration order (`crates/qbit-prism/src/lib.rs:184-199`), `credit_policy`
omitted when `None`, `u128` difficulties as JSON numbers. These are the bytes
`persist_audit_snapshot` hashes at `srv/src/ledger/audit.rs:120` and
`materialize_audit_row` checks at `:68`, so `window_snapshot_sha256` equals
the `qbit_prism_audit_snapshots.snapshot_sha256` landing later inserts for the
same window. Streaming the serializer into the
hasher, as above, produces the same bytes.

**Two share digests, not interchangeable.** `snapshot_sha256` hashes the
native `AcceptedShare` serialization above. `PayoutWindow::canonical_digest_hex`
(`crates/qbit-prism/src/window.rs`), which #274 names, hashes a different,
Python-compatible canonical form with sorted keys, so the two differ for the
same shares; a compiled one-share comparison on #273 shows it. The reference
keeps `snapshot_sha256`, because it equals
`qbit_prism_audit_snapshots.snapshot_sha256` and landing's
`share_snapshot_sha256`, and existing audit hashes don't change. #274's
instruction to "use `canonical_digest_hex` for the slice digest instead of
`serde_json::to_vec(shares)`" is amended accordingly: its incremental window
must produce the native `snapshot_sha256`, or carry both.

**Cost model for #274.** A flat SHA-256 can't drop shares from the front of a
sliding window. Every advance that expires old shares rehashes the whole
window, the 0.7 to 1.4 s per non-cached refresh that the cost table budgets.
Caching each share's serialized bytes removes the serialization, not the
hash. #274's gate, under 1 s wall at 400,000 shares with a one-share delta,
therefore needs a measured native encoding and hash path. If a flat rehash
misses it, the reference digest has to become chunked, for example SHA-256
over the digests of fixed `share_seq` pages so only the edge pages rehash.
That is a versioned change to `WindowRef` that #274 owns, and the landed-audit
recovery check would then compute the flat digest separately.

`prior_balances_digest` is the digest already carried as
`ledger_window_attestation.prior_balances_digest_hex`
(`crates/qbit-prism/src/lib.rs:706`), computed at build (`:1519`) and checked
at verify (`:2585`). Its definition, `prior_balances_digest_hex`
(`:2651-2672`): sort the balances by `(order_key, recipient_id,
p2mr_program_hex)`, then for each balance feed SHA-256 with `recipient_id`,
`order_key` and `p2mr_program_hex` as a big-endian `u64` length followed by the
UTF-8 bytes (`update_string`, `:2674-2677`), then `balance_sats` as a
big-endian `i128` (`update_i128`, `:2687-2689`). The function is private
today; the parts API exports it as
`prior_balances_digest(&[CarryForwardBalance]) -> [u8; 32]`, so the server
computes the reference and `read_window` checks it with the same code.

### Threads, concurrency and deadlines

**`read_window` owns its blocking hand-offs.** It is `async`. The runtime
thread issues the statements and receives each page as sqlx wire buffers; it
runs no serde, no hashing and no `share_from_row`. For each page it moves the
rows, the running `Vec<AcceptedShare>` and the hasher into one
`tokio::task::spawn_blocking` task, which maps the rows (`share_from_row`,
`srv/src/ledger/window.rs:263-283`: two `u128` parses, five required `String`s plus
an optional sixth for `credit_policy`, two timestamps), updates the hash and
hands the state back; the balances digest runs the same way. **Callers must
not wrap `read_window` in `spawn_blocking`**; they await it on the runtime.
Dropping the future between pages rolls the transaction back and returns the
connection; an in-flight page task finishes its at most 4096 rows detached,
so the blocking work a drop can leave behind is one page, about 5 to 10 ms.
The rebuild that follows is the caller's blocking task, held in
`AbortOnDropHandle` as today (`srv/src/coordinator.rs:998`).

**Concurrency.** `read_window` acquires nothing; each call holds one pool
connection with an open snapshot for the 2 to 5 s estimated below. Bundle
builds are bounded by `build_slots` (`srv/src/coordinator.rs:82`, created at
`:300` from `PRISM_JOB_BUILD_EXECUTOR_WORKERS`, default
`min(runtime_workers, 4)` and as low as 1, `srv/src/config.rs:365-370`;
acquired at `srv/src/coordinator.rs:712` and `:997`). The caller acquires its
`build_slots` permit **before** `read_window` and holds it across the read
plus the rebuild, so at most `build_workers` windows are materialized at
once. That bound alone doesn't protect the pool.
`PRISM_DATABASE_MAX_CONNECTIONS` defaults to 16 but may be as low as 4
(`srv/src/config.rs:358-364`), and `build_workers` may exceed it, up to
`runtime_workers + 8` (`:365-370`). The pool is shared, with a 15 s acquire
timeout (`srv/src/ledger/connect.rs:25-27`), with share appends and the
candidate-lease heartbeat (`srv/src/coordinator.rs:887-907`). Four rebuilds
of distinct `storage_key`s could then hold all four connections for their
whole multi-page reads: an append or a heartbeat waits out the acquire
timeout, and a failed heartbeat drops recoverable claim work. So
`read_window` also takes a permit from a `window_reads` semaphore sized
`clamp(database_max_connections - 2, 1, build_workers)`. The caller acquires
it after its `build_slots` permit and releases it as soon as `read_window`
returns, before the rebuild, so at least two connections always stay free
for appends and heartbeats. Waiting for it counts against the caller's
deadline. A dedicated pool was rejected: it would add connections beyond the
budget operators size the database for. **Under that permit the rebuild calls the `build_audit_bundle_body*`
builders directly, or a `build_bundle` variant that takes the held permit,
never `build_bundle` itself**: `build_bundle` acquires its own permit
(`:712`), so with one worker a nested acquisition would wait on itself. The
claim path already calls the builders directly (`:997-1031`). Resume is per
reconnecting miner (`resume_job`, `:1447-1470`) and every miner of one job
reads the same `prepared:` row (`:1466`), so #273 coalesces concurrent
resumes of the same `storage_key` behind one in-flight rebuild, for example a
single-flight map next to the `Prepared` cache: later miners await the first
rebuild's `Arc<Snapshot>` and `Arc<AuditBundleBody>` instead of reading the window again. The entry shares only the slim resumed form built from that pair (below) and, for an empty window, the as-issued balances each waiter's bootstrap build reads:
each waiter still runs its own checks on its own job (worker, target,
extranonce, version mask, absolute expiry and final authority), and
cancelling a waiter neither renews a job nor hands a bootstrap bundle to
another miner. A blocking build owns its `build_slots` permit until it
finishes: the permit moves into the `spawn_blocking` closure, because
cancelling the awaiting task or dropping `AbortOnDropHandle`
(`srv/src/coordinator.rs:998`) can't stop a closure that is already running.

**A resumed job keeps no window.** `build_workers` bounds how many rebuilds
run at once, not how many rebuilt windows stay resident. A session keeps up
to 64 jobs per connection (`srv/src/stratum.rs:189`), so resumed jobs that
each held their `Arc<Snapshot>` and `Arc<AuditBundleBody>` could pin about
0.5 GB apiece. Once the rebuild has produced the job's coinbase, the resumed
`Prepared` keeps everything submit and the candidate need except the window:
- the decoded `StoredPrepared` itself, whose stored inputs the candidate
  persists: `payout_policy`, the whole nested `ctv`, `audit_builder_version`,
  `signer_keys`, `coinbase_suffix`, `template_sha256`, the `WindowRef`, and the
  snapshot scalars `anchor_ms`, `payout_revision` and `share_seq`, all O(1);
- the template and the fee;
- the body's non-window fields (`found_block` and the coinbase manifest);
- for an empty window, the miner's own `bootstrap_share`.

The candidate takes its stored inputs from there, never from local
configuration ([Stored bundle inputs](#stored-bundle-inputs)). Submit reads
nothing that depends on the window (`srv/src/coordinator.rs:1621-1666`,
`:1695`), and the whole-bundle clone at `:1696` goes with #265. A resumed window never reaches async code. The rebuild's `spawn_blocking`
closure builds the coinbase and the slim resumed form, then drops
`Snapshot.shares` and the body's `reward_manifest.shares` inside the same
closure, so only the slim form crosses back, and the single-flight entry
shares only that. Freeing about 0.5 GB of vectors and their heap-backed
fields on a runtime worker would stall candidate-lease heartbeats and share
processing. The same rule covers every other release of a rebuilt window:
the claim's rebuilt `CandidateClaim` parts after landing, and a replaced
local `Prepared`, go to `spawn_blocking(move || drop(…))` instead of being
dropped on the runtime. Windows then exist only inside rebuilds, and the
`k × 0.5 GB` bound in the cost table holds however many resumed jobs sessions
keep. Locally issued jobs share their generation's `Prepared` (`:1392-1395`),
window included, for as long as Stratum retains them
(`srv/src/stratum.rs:639-641`), as today; bounding how many local
generations that keeps resident is an acceptance note on #273, not part of
this contract. Each generation holds its window once. `Prepared` and `JobContext`
(`srv/src/coordinator.rs:26-43`) keep `snapshot: Arc<Snapshot>`, which owns
the only `Vec<AcceptedShare>`, and replace `bundle: Arc<AuditBundle>` and
`Option<Arc<AuditBundle>>` (`:29`, `:35`) with `Arc<AuditBundleBody>` (#296).
For an empty window, `JobContext` also keeps `bootstrap_share:
Option<AcceptedShare>`, the synthetic share its per-worker build fabricated
(`:727-745`, reached from `:1343-1350`). Today that share lives only in
`JobContext.bundle.shares`, and the body keeps just a `CountedShare`, which
lacks fields the stored share needs, so submit copies this one verbatim into
`Candidate.bootstrap_share`.
The body carries every field the coordinator reads from them (`found_block`
at `:1648` and `:1663`, the coinbase manifest) and no shares; the
whole-bundle clone into the candidate (`:1696`) goes with #265. An owned
`AuditBundle` beside the snapshot would need a second `Vec`, because
`into_bundle` takes it by value and two `Arc`s do not share storage: about
0.75 GB per 400k rebuild instead of 0.5 GB. Canonical bytes and verification
use `canonical_audit_bundle_bytes_from_parts` and `verify_audit_parts` over
`(&body, &snapshot.shares)`; no owned bundle is assembled, at claim either: first-time landing takes the same parts (below). For an empty window the entry carries `bundle: None` and the as-issued
`prior_balances` from `read_window`, O(recipients), until every waiter has
built its bundle, and each
resuming miner builds its own bootstrap bundle, because sharing one would put
miner A's bootstrap share in miner B's job. It does so under its own
`build_slots` permit, taken after the entry's permit is released, by calling
the builders directly: the synthetic share is the one `build_bundle`
fabricates from the worker today (`:727-745`, reached from `:1498-1510`), and
every other input comes from `StoredPrepared` (template, anchor,
`payout_policy`, `ctv`) and the issued `payout_revision` from `StoredPrepared`, plus the as-issued balances the entry carries.
It never calls `build_bundle` itself, which reads `config.ctv_enabled`,
`payout_policy`, `ctv_direct_floor` and `ctv_config` (`:755`, `:760-762`) and
would reintroduce the drift [Stored bundle inputs](#stored-bundle-inputs)
forbids.

**First-time landing.** A claim whose block has no landed audit lands from
the same parts. `CandidateClaim` holds the rebuilt `Arc<AuditBundleBody>` and
the `Arc<Vec<AcceptedShare>>` that `read_window` returned, never an assembled
`AuditBundle`. Before its transaction opens, `land_candidate_checked`
(`srv/src/ledger/blocks.rs:55-157`) does all of its whole-window work in one
`spawn_blocking` closure over those `Arc`s. That closure replaces the clone at
`:61`, and nothing serializes the window on the runtime:

- `verify_audit_parts` produces the report (`:63-67`);
- `serde_json::value::to_raw_value(&body)` gives the stored body as a
  `Box<RawValue>`, so the bind at `:141-142` copies bytes instead of
  serializing. The body has no top-level `shares`, so the whole-bundle
  `to_value` and `remove("shares")` at `:136-140` go. Until #267 normalizes
  `reward_manifest.shares` out of the stored body, it is still the largest
  value landing writes, as today, and the bind's copy of it into SQLx's
  argument buffer is the one whole-body step left on the runtime: a single
  contiguous copy with no per-share work;
- `audit_body_byte_len` comes from `write_canonical_audit_bundle_from_parts`
  into a counting writer instead of the full canonical bytes at `:143`;
- the commitment and witness leaves and the payout accounts (`:145-147`) are
  serialized there too;
- the share snapshot's ordering check and digest
  (`srv/src/ledger/audit.rs:115-120`) run there, and for a non-empty window
  the digest must equal the reference's `snapshot_sha256`.

The transaction binds only those values. `persist_audit_snapshot` takes the
digest and the reference's range instead of `&AuditBundle`; a bootstrap
bundle still writes `inline_shares` from the stored `bootstrap_share`
(`:124-131`). It keeps its fence, the re-read of the range under exact
equality (`:132-138`), but not its reader: `read_range` (`:144-152`) is one
unpaged `fetch_all` that maps every row and compares the whole window on the
runtime. The re-read uses the paged reader `read_window` uses, inside the
same transaction, and compares each page with its slice of the claim's
shares, so runtime work stays per page, as in `read_window`. None of this
waits for #267: the parts entry points are #296's.

**Deadlines.** `read_window` takes no deadline of its own. Each statement,
so each page, runs under the connection's `statement_timeout`, 15 s by
default (`srv/src/ledger/connect.rs:23-33`) and overridable through
`PRISM_DATABASE_STATEMENT_TIMEOUT_MS` up to 600000 ms (`:11-24`); a timeout
surfaces as `Database` with SQLSTATE 57014, and pool acquisition adds its own
15 s (`:27`). The worst case is bounded by `pages × statement_timeout` plus
page CPU, about 98 pages at 400k; the expected total is 2 to 5 s, which the
#264 harness must measure before #265 and #273 rely on it. The claim puts
one `tokio::time::timeout` around `read_window` plus the rebuild; a resume
relies on its caller's existing end-to-end deadline:

| Caller | Deadline | On expiry |
| --- | --- | --- |
| claim (#265) | **60 s** around `read_window` plus the rebuild only, the work this record adds. The steps after it keep their existing deadlines: observation, landing, lease renewal and `submitblock`, whose RPC timeout `PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS` may be set up to 86,400 s (`srv/src/config.rs:428`, bounded at `:138-145`). A 60 s bound around them would cancel an in-flight submission and reschedule it as a reconstruction timeout. The candidate lease is not a deadline: its heartbeat renews it every 30 s for as long as processing runs (`srv/src/coordinator.rs:887-907`), and only a failed renewal drops the work (`:950-963`). Against the estimate, 60 s is twelve times the 5 s upper bound and four statement timeouts at the 15 s default, so a rebuild that needs longer is a harness finding, not a reason to raise it | fail the attempt through `retry_candidate` (`srv/src/ledger/candidates.rs:114`) with an alert; the lease was renewed within the last 30 s of a 120 s term, so the `claim_expires_at > clock_timestamp()` condition holds and the row is rescheduled after `LEAST(60, attempt_count)` seconds (`:118`), never abandoned |
| resume (#273) | **one end-to-end deadline, the caller's existing one**: `resume_job`'s only caller already wraps it in `timeout(initial_job_timeout_seconds, …)` and maps expiry to the backend error "job resume timed out" (`srv/src/stratum.rs:1227-1232`). That deadline covers admission (the `build_slots` and `window_reads` permits), the wait on the single-flight entry, `read_window`, the rebuild and any bootstrap build. There is no inner timeout: a shorter one would reject work that completes inside today's allowance (30 s by default, `srv/src/config.rs:195`), which would be a policy change, not parity | the backend error "job resume timed out", as today; never `unknown-job` |

### Errors and callers

| Outcome | Meaning | Claim (#265) | Resume (#273) | Landing (#265, #267) | Import (#265) |
| --- | --- | --- | --- | --- | --- |
| stored `audit_builder_version` or `signer_keys` differ from this binary's (caller check before `read_window`, not a variant) | a builder or signing keys this binary does not have built the reference; the upgrade and rotation refusals ([Builder version](#builder-version), [Signing keys](#signing-keys)) keep a drained deployment from reaching it | `retry_candidate` (`srv/src/ledger/candidates.rs:114`) with an alert naming both values; never abandon, never rebuild with the current builder or keys | cache miss: `Ok(None)`; the next refresh writes a job at the current version | not a caller; landing uses the claim's parts | n/a |
| `Window.payout_revision != row revision` (caller check, not a variant) | the revision moved since the reference was written: a landing, a reorg, a resettlement, or the pool's own block reaching the tip | a hint only, like the cached tip and revision at `srv/src/coordinator.rs:972-973`: run `observe_candidate` (`:978`) and finish through `finish_candidate_at_revision` (`:979-988`) only when the block is not active and the revision or parent changed; an active block continues and lands at the observed revision (`:1039-1046`); #289 owns old-epoch candidates | not a miss by itself: B's published-work check decides eligibility, and eligible work rebuilds as issued ([Revision fence and reorgs](#revision-fence-and-reorgs)) | not a caller; landing keeps its own fence (`srv/src/ledger/blocks.rs:124-127`) | n/a |
| `PriorBalancesChanged` | the balances moved: the candidate is superseded, or it is the pool's own block, already landed | not reached when `qbit_pool_audit_bundles` already holds the block's audit, because the claim then treats landing as done, from the landed row's columns authenticated against the block's coinbase, without `read_window`, and continues to observe, renew and submit ([Revision fence and reorgs](#revision-fence-and-reorgs)); otherwise run `observe_candidate`: not active and changed is `finish_candidate_at_revision`; active is `retry_candidate` with an alert, the outcome today's `prior == bundle.prior_balances` failure has (`srv/src/ledger/blocks.rs:128-132`, reaching `submit_loop`'s retry at `srv/src/coordinator.rs:1123-1131`) | not reached for published work, which reads its as-issued balance set, not the current one ([Revision fence and reorgs](#revision-fence-and-reorgs)) | as above | n/a |
| `Incomplete` | rows pruned or missing; D6 violated, or a wrong predicate | fail the attempt through `retry_candidate` (`srv/src/ledger/candidates.rs:114`) with the error in `last_error`; never abandon automatically, #268 owns recovery | a failure, not a miss: return `Err`, which reaches the miner as a backend error through the existing path (`srv/src/stratum.rs:1227-1232`), with an alert. `unknown-job` (`:1251-1255`) stays for work that is absent, expired or deliberately incompatible | as above | the legacy window is not in the ledger: keep the bytes-only import |
| `BalanceSnapshotMissing` | `AsIssued` only: the job's balance snapshot was pruned or never written | n/a: claims read `Current` | a failure, not a miss: `Err` through the backend-error path, with an alert, as for `Incomplete` | n/a | n/a |
| `SnapshotDigestMismatch`, `Decode` | corruption, a reference built from different bytes, or payload/column disagreement | same as `Incomplete`, with an alert | same as `Incomplete` | as above | same as `Incomplete` |
| `Database` (incl. 57014) | transient | propagate: `submit_loop` hands the error to `retry_candidate` (`srv/src/coordinator.rs:1123-1131`), which releases the claim and reschedules the row after `LEAST(60, attempt_count)` seconds (`srv/src/ledger/candidates.rs:118`); lease expiry recovers the row only if that write itself fails | propagate; the reconnect fails and retries | as above | propagate |
| landing equality failure after a successful `read_window` (`srv/src/ledger/audit.rs:132-138`) | the landing transaction read a different range than the claim did, which immutability forbids | like `SnapshotDigestMismatch`: `retry_candidate` with an alert, never abandon; the error already reaches `submit_loop`'s retry path (`srv/src/coordinator.rs:1123-1131`) | n/a | the check stays | n/a |
| caller deadline expired | `read_window` plus the rebuild outran the deadline in the table above | 60 s: `retry_candidate` with an alert | the caller's own `initial_job_timeout_seconds`: the backend error "job resume timed out" (`srv/src/stratum.rs:1232`), as today | n/a | n/a |
| empty window (`shares: None`) | not an error | rebuild from `vec![bootstrap_share]` | rebuild the bootstrap bundle per miner with the builders directly, from the stored policy inputs, never `build_bundle` | lands through `inline_shares` (`srv/src/ledger/audit.rs:124-131`), unchanged | n/a |

## Revision fence and reorgs

Share rows survive a reorg: the immutability trigger below forbids UPDATE and
DELETE, so the range is always rebuildable and `Incomplete` never means "the
chain moved". What moves is the revision and, sometimes, the balances, and
neither is a supersession test on its own. `observe_chain_view` bumps
`payout_revision` on every tip with more work, the pool's own block included
(`srv/src/ledger/window.rs:66-68`); an ambiguous `submitblock` timeout makes that
routine, because the attempt fails with "block submission outcome
unresolved" (`srv/src/coordinator.rs:1085-1092`, `:1107`) while the block can
still reach the tip, and the timed-out request is never resent
(`srv/tests/rpc_deadlines.rs:196-222`). A rule of "any revision inequality is
superseded" would therefore abandon the pool's accepted blocks. The digest
alone cannot detect a reorg either: a landing that a reorg undoes can return
the balances to their earlier values. So each caller applies its own rule,
and `read_window` reads the revision in the same snapshot as the balances so
that the comparison and the digest describe one moment:

- **Claim (#265) keeps today's rule; `read_window` never decides
  supersession.** The candidate is finished only when `observe_candidate`
  reports it **not active** and the revision or the parent tip changed
  (`srv/src/coordinator.rs:975-979`), the outcome being
  `finish_candidate_at_revision` (`:979-988`); the comment at `:975-977`
  says why: an already-active block still needs its audit recovered. An
  **active** candidate is rebuilt from `read_window` and lands at the
  observed, newer revision with the digest-checked balances, as `:1039-1046`
  does today. The guard is `active_candidate_can_land_at_proven_new_chain_revision`
  (`srv/tests/ledger_postgres.rs:505-539`): it bumps the revision after the
  claim, shows that landing at the candidate's revision fails, and lands at
  the observed one. If `qbit_pool_audit_bundles` **already holds the block's
  audit**, because an earlier claim landed it and lost its lease after
  landing (`srv/src/coordinator.rs:1041-1043`, `:1067-1069`) or a reconcile
  has confirmed it, the balances have legitimately moved. The claim then skips
  `read_window` and treats landing as done, once the landed row's columns are
  authenticated against the block the candidate found instead of a stored
  digest. The candidate keeps no
  expected `audit_bundle_sha256`: computing one at submit would serialize the
  whole bundle on the share path, which [Stored bundle inputs](#stored-bundle-inputs)
  rules out. The block is the expectation, and every check is O(1):

  - the landed `coinbase_tx_hex` equals the coinbase in the candidate's
    `block_hex`, parsed with the segwit-aware parser that
    `codec::witness_merkle_leaves_from_block` uses;
  - `audit_commitment_root_hex` (`crates/qbit-prism/src/lib.rs:975`) over the
    landed `audit_commitment_leaves_hex` equals that coinbase's witness
    reserved value, which the builder sets to the audit commitment root
    (`:1711-1724`). The single leaf commits to the reward manifest and the
    payout policy manifest (`:960`);
  - `share_snapshot_sha256` equals `window_snapshot_sha256`. For an empty
    window that column is NULL under the three-state CHECK, so the comparison
    is against `sha256(serde_json::to_vec(&[bootstrap_share]))`, computed from
    the stored `bootstrap_share`: the digest `persist_audit_snapshot` writes
    for inline shares;
  - `found_block_bits` matches the header, as today's landing checks for an
    existing audit (`srv/src/ledger/blocks.rs:95-117`).

  Only then does the claim treat landing as done. It skips the rebuild and
  `land_candidate` and continues through the rest of today's flow, never
  straight to finishing. A landed audit doesn't mean a submitted block: an
  inactive block lands (`srv/src/coordinator.rs:1067-1069`) before the lease
  renewal and `submitblock` (`:1084-1092`), so an earlier claim can land and
  lose its lease before it submits. The claim observes the block (`:1039`,
  `:1070`) and finishes it only where today's code does: an active block
  (`:1040-1048`), or an inactive one whose revision or parent moved
  (`:1050-1063`, `:1071-1080`). Otherwise it renews the lease and calls
  `submitblock` (`:1084-1092`), as today. It never calls `materialize_audit_row`
  (`srv/src/ledger/audit.rs:42-87`) on the submit loop. That function reads the
  whole range in one unpaged query, serializes and clones it on the runtime,
  and builds an owned `AuditBundle`: at 400k, two share arrays that the
  cooperative 60 s deadline cannot interrupt. If #265 ever needs the landed body
  on this path, it needs #267's parts-based, paged reader under
  `spawn_blocking` first, which makes #267 a prerequisite for that path.

  `PriorBalancesChanged` on an active candidate with no landed audit has the
  outcome today's landing check has when
  `prior == bundle.prior_balances` fails (`srv/src/ledger/blocks.rs:128-132`):
  the error propagates to `submit_loop`, which logs "candidate remains
  recoverable" and calls `retry_candidate` (`srv/src/coordinator.rs:1123-1131`);
  the attempt fails with an alert and is never abandoned.
- **Resume (#273) keeps published work's authority; revision equality no
  longer decides it.** Today `srv/src/coordinator.rs:1483-1488` requires the
  prepared row's revision to equal the current one. B8 (djh58, #273) keeps
  work published at revision R0 eligible during a bounded replacement lease
  after the ledger reaches R1, and this record must not undo that:
  1. B's published-identity and lease check decides whether the stored job
     is still eligible. Only the published job gains the lease, and only
     inside it; an arbitrary older job at the same parent never does.
     Ineligible work is a cache miss, `Ok(None)`. **Interim
     constraint:** until B8 and #289 define how an active leased block lands
     against R0's balances, the lease covers only revision changes that
     leave `prior_balances_digest` unchanged, and a balance change ends it at
     once. An active leased block then carries the current balances and lands
     through today's path.
  2. Eligible work is rebuilt with its **as-issued** economics: the balances
     it was published with, not the current ones. `read_window(…, BalanceSource::AsIssued)` does this
     ([`Ledger::read_window`](#ledgerread_window)): it takes the balances from
     the job's balance snapshot (below) instead of
     `qbit_current_carry_forward_balances()`, and checks them against the
     reference's `prior_balances_digest`. It still reads the current revision in the same transaction, for the
     caller's own fence, but that revision no longer has to equal the job's.
     The rebuilt `Snapshot` keeps the issued revision,
     `StoredPrepared.payout_revision`, never `Window.payout_revision`. The job
     wire and any candidate take their revision from it, so a block found on
     R0 work carries R0. `observe_candidate` then sees the change and takes the
     superseded path instead of retrying `PriorBalancesChanged`.
  3. Submission honours the same lease. Today submit rejects work when the
     frontend's current work or the job's revision differs from the ledger's
     (`srv/src/coordinator.rs:1632-1644`), apart from parent-stale work
     inside the stale grace. That grace work is credited as a share
     (`:1669`), but a candidate is enqueued only when the block passes and the
     work isn't stale (`:1672`), so its found block is dropped. B8's lease
     therefore decides those checks at submit exactly as it decides
     eligibility at resume: work the lease covers is not stale because the
     revision changed, and a block found on it is enqueued as a candidate
     carrying R0, with its as-issued window and inputs. Work outside the
     lease keeps today's handling.
  4. A leased candidate is submitted before any terminal disposition. The
     candidate records that the lease covered it, as a `leased` flag that
     `candidate_sha256` covers. Otherwise the claim's pre-submit supersession
     checks would finish it before `submitblock` and discard the block: the
     cached-hint path (`srv/src/coordinator.rs:978-988`), the post-rebuild
     check (`:1050-1063`) and the post-land check (`:1071-1080`). So would the
     pre-submit landing (`:1067-1069`), which checks R0's balances against
     R1. A leased candidate skips all four and submits before any
     rebuild. `submitblock` needs only the stored block (`block_bytes`), so the
     claim calls neither `read_window` nor the builders first; a
     `BalanceSource::Current` read would return `PriorBalancesChanged`
     against the candidate's R0 digest and retry forever. It renews its lease,
     calls `submitblock` (`:1084-1092`), and only then meets
     `observe_candidate` (`:1095`). Whether or not the block is active, the claim then lands its audit
     before any terminal disposition. It rebuilds with
     `BalanceSource::AsIssued`, skipped when the audit has already landed as
     for any claim, and lands through today's path,
     `land_candidate_at_revision` at the observed revision
     (`srv/src/coordinator.rs:1041-1043`). The interim constraint in step 1
     allows that, because the balances equal the current ones. This is
     today's order with the RPC moved first: today an inactive block lands
     before `submitblock` (`:1065-1069`), so a late acceptance stays
     reconcilable (`:1052-1054`). Only then does the claim take today's
     outcome (`:1095-1107`). An active block finishes as submitted. On an
     inactive block, a string result, `duplicate` included, finishes it with
     the node's reason. A null result, which can still describe a known
     side-chain block (`:1093-1094`), leaves it pending through
     `retry_candidate` (`:1123-1131`). A retry resubmits the stored block with
     its audit already landed, so no answer from the node can leave the block
     without payout records. A balance change after enqueue is
     the case every candidate has today: `retry_candidate` with an alert,
     never abandoned. A rule for landing across a balance change is B8's and
     #289's to define before the lease may bridge one. Every other candidate keeps today's
     fences.

  **As-issued balances.** Once R1 lands, the current view can't return R0's
  balances. Rebuilding them from `qbit_payout_carry_forward`
  (`crates/qbit-prism/sql/001_share_ledger.sql:130-147`) would mean replaying
  which blocks were active at R0, and reorgs change that. So `save_job`
  stores the balances a job was built with in an immutable
  `qbit_prism_balance_snapshots` row keyed by `prior_balances_digest`, in the
  same transaction as the job row and only if the key is new (`ON CONFLICT DO
  NOTHING`). A revision that leaves the balances unchanged writes nothing.
  The row grows with the number of recipients with a balance, not with the
  window. In as-issued mode a missing row is `BalanceSnapshotMissing`, a failure, not a miss. Retention deletes a row once no job row and no non-terminal
  `leased` outbox row carries its digest
  ([Immutability and retention](#immutability-and-retention)); other claims
  read `Current`.
- **Landing** keeps its own fences unchanged: `require_revision`, the
  `payout_revision` equality and `prior == bundle.prior_balances`
  (`srv/src/ledger/blocks.rs:91-94,120-132`), and `persist_audit_snapshot`'s
  re-read of the range under exact equality (`srv/src/ledger/audit.rs:132-138`),
  paged rather than today's single `fetch_all`
  ([Threads, concurrency and deadlines](#threads-concurrency-and-deadlines)).
  `read_window` authenticates a rebuild for the caller; it does not replace
  the landing transaction's checks.

## Immutability and retention

A `WindowRef` is valid only while its rows exist unchanged. Today that holds
because `qbit_share_ledger` rows are immutable (the statement-level trigger at
`srv/migrations/002_multi_instance.sql:127-135` raises on UPDATE, DELETE and
TRUNCATE, so no pruning exists), `share_seq` is assigned by the insert under
`ORDER_LOCK` (`srv/src/ledger/window.rs:99,169`) so no row can later appear inside
a written range, and the anchor predicate is stable because `accepted_at` and
`job_issued_at` never change and the ledger clock only moves forward (`:164`).

`finish_candidate_at_revision` sets `candidate=NULL` in its terminal UPDATE
(`srv/src/ledger/blocks.rs:214`); after 007 the same UPDATE also sets the six
window columns NULL, so a terminal row is "no window" and the floor below
considers non-terminal rows only. It must also NULL `block_bytes`, the
`bytea` column `block_hex` moves to ([Stored bundle inputs](#stored-bundle-inputs)).
Before 007 the block left the row with `candidate`; without this, the outbox
would keep every submitted or abandoned block forever. `block_hash` stays for
duplicate detection.

This extends the Python-era invariant "audit bodies and share segments remain
digest-checked and reconstructable" ([Invariants](invariants.md), Audit
artifacts; the contract is [A1 audit artifacts](a1-audit-artifacts.md)) to
native rows. Proposed wording for the native server: *a share window
referenced by any live row is reconstructable: `qbit_share_ledger` rows are
immutable, and no retention job removes a row at or above the smallest
`window_first_share_seq` of any non-terminal outbox row, any unexpired job
row, or any `qbit_prism_audit_snapshots` row without `inline_shares`.*

D6 (#260) must honour that floor. A future prune runs in one transaction that
takes `SETTLEMENT_LOCK` and then `ORDER_LOCK`, the order every existing
dual-lock site uses (`srv/src/ledger/connect.rs:52-53`,
`srv/src/ledger/window.rs:196-197`, `srv/src/ledger/blocks.rs:181-182,248-249`),
so it cannot form an advisory-lock cycle with a refresh snapshot or a
candidate finalization. Those are the locks under which references are written
(`save_job` under `SETTLEMENT_LOCK`, `srv/src/ledger/jobs.rs:14`; enqueue under
`ORDER_LOCK`, `srv/src/ledger/window.rs:99` and
`srv/src/ledger/candidates.rs:29`). The
prune computes the floor, and deletes strictly below it. It must also keep enough history for any window a later `Ledger::snapshot`
can select, and a horizon computed from today's difficulty isn't enough: a
rise after the prune moves the next window's start earlier
(`srv/src/ledger/window.rs:190-232`), into rows already deleted. `snapshot`
wouldn't fail then. Its scan stops when the requested weight is reached or no
rows remain (`:216-231`), so it would publish an underweight window that
silently drops eligible shares. So D6 needs both of these:

- **A ratcheted horizon.** The prune keeps every row the window would need at
  a durable maximum difficulty, stored in `qbit_prism_cluster`, that rises
  with every higher difficulty observed and falls only when an operator
  lowers it, times a stated safety factor.
- **An explicit failure.** The prune records the `share_seq` it deleted below
  in `qbit_prism_cluster`. `snapshot` returns an error instead of a window
  when its scan reaches that recorded floor before the requested weight, so
  the refresh publishes no work rather than short payouts. A pool whose
  history was never pruned still gets the short window it legitimately has.

A `read_window` that began before a prune commits still sees its snapshot; one that begins after it fails
with `Incomplete` at the existence probe.

**In-flight windows.** The floor covers written references only.
`Ledger::snapshot` fixes its cutoff and releases both locks when its first
transaction commits (`srv/src/ledger/window.rs:195-208`). The page scan
(`:212-233`), the digest, the bundle and `save_job`
(`srv/src/coordinator.rs:560-668`) all come after that, while no row
references the window yet. Miners get the work only after `save_job` commits
(`:686-705`), so the gap ends there. A prune inside the gap, after a fall in
difficulty shrinks the next selectable window, could delete rows of the
window being built, and `save_job` would then publish a reference to rows
that are gone. So D6 adds a reservation:

- `Ledger::snapshot`'s first transaction, still under both locks, inserts a
  reservation row holding its cutoff and an expiry longer than any refresh
  can take. The window start isn't known yet, so the row's `first_share_seq`
  stays NULL until the scan ends and the refresh fills it in.
- `save_job` deletes the reservation in its own transaction, where the job
  row's reference takes over. It publishes nothing, a cache miss, if the
  reservation has expired or the window's `first_share_seq` row is gone,
  checked with the existence probe `read_window` uses; a prune only removes a
  prefix, so that row's presence means the whole range is present.
- The prune, holding both locks, skips its run while any unexpired
  reservation still has a NULL `first_share_seq`, and otherwise lowers its
  floor to the smallest reserved `first_share_seq`.

An age-based horizon was rejected: how far back in time a window reaches
depends on the pool's hashrate, so no fixed age is safe. Until D6 lands nothing is pruned, and none of this is needed.

**Template and balance-snapshot rows.** These are pruned today, not by D6.
`prune_expired_jobs` (`srv/src/ledger/jobs.rs:43-45`) is one unlocked
`DELETE` of up to 4,096 expired job rows. Templates change on every new
transaction set and at every reanchor (60 s by default,
`srv/src/config.rs:433`), so each distinct template becomes a new, possibly
multi-megabyte row. Without cleanup the table would grow for as long as the
frontend runs. WAL volume is no higher than today's, when every `save_job`
writes the template inside its payload, but storage would never shrink.

So #273 turns the prune into one transaction under `SETTLEMENT_LOCK` and
then `ORDER_LOCK`, the established order. The first is the lock `save_job`
holds while it inserts the template, the balance snapshot and the job row;
the second is the lock enqueue holds while it writes a `leased` candidate
that refers to a balance snapshot. The prune runs three statements:

1. It deletes the expired batch and returns the template digests that batch
   carried.
2. It deletes the `qbit_prism_templates` rows among those digests that no
   remaining job row references.
3. It deletes every `qbit_prism_balance_snapshots` row that no remaining job
   row and no non-terminal `leased` outbox row references, not only those the
   batch carried. A snapshot kept by a `leased` candidate has no job row left
   to name it once its job expires, so a batch-scoped statement would never
   look at it again after the candidate turns terminal. The scan stays small:
   the table holds only snapshots that live jobs or pending `leased`
   candidates reference, plus the ones this statement deletes.

A single statement with a data-modifying CTE wouldn't do, because its outer
`NOT EXISTS` still sees the rows the CTE deletes. The lock keeps a prune from
deleting a row between a `save_job`'s insert-or-reuse and its job insert, and
a later `save_job` just inserts a deleted row again. Indexes on the job row's `template_sha256` and `window_prior_balances_sha256`, and
on the outbox's `window_prior_balances_sha256`, keep the checks cheap.

## Stored bundle inputs

**Invariant: a rebuild reads no local configuration for any field that
reaches the signed bundle.** Today's claim rebuild takes `found_block`,
`prior_balances`, `payout_policy`, `ctv_fanout_fee_policy` and
`witness_merkle_leaves_hex` from the stored bundle but `config.ctv_direct_floor`
and `config.ctv_config` from local configuration
(`srv/src/coordinator.rs:1004-1027`), and `config.ctv_enabled` chooses the
builder (`:1003`). The fee policy is revalidated on every refresh because
relay floors change (`:525-527`). The cluster fingerprint
(`srv/src/config.rs:479-491`, pinned once in `qbit_prism_cluster` by
`configure`, `srv/src/ledger/connect.rs:128-140`) keeps live instances on one policy,
so the drift is across time, a fingerprint reset between the refresh that
built the row and its claim, not across instances. If a claiming frontend
re-derives any signed input differently, the canonical bundle changes:
`srv/src/ledger/blocks.rs:101-105` raises "existing block audit differs from
candidate", or a non-matching body lands. #265 removes the drift by storing
every builder input except the window:

| Builder input (`crates/qbit-prism/src/lib.rs:1753-1764`) | At refresh (`srv/src/coordinator.rs`) | At claim today | With `WindowRef` | Size |
| --- | --- | --- | --- | --- |
| `shares` | `snapshot.shares` (`srv/src/coordinator.rs:747`) or the bootstrap share (`:727-745`) | `source.shares` | `read_window`, or `vec![bootstrap_share]` | the window, never stored |
| `found_block` | from the template (`:719-726`) | `source.found_block` | **stored verbatim** in the candidate JSON | 4 scalars |
| `prior_balances` | `snapshot.prior_balances` (`:759`) | `source.prior_balances` | `read_window`: the current set for a claim, the as-issued set from `qbit_prism_balance_snapshots` for a resume of published work or a leased candidate's audit ([Revision fence and reorgs](#revision-fence-and-reorgs)); digest-checked and sorted with the digest's comparator ([Read](#read)) | per recipient with a balance; stored once per distinct set, keyed by its digest, never in the job or candidate row |
| `payout_policy` | `config.payout_policy` (`:760`) | `source.payout_policy` | **stored verbatim** | O(1) |
| `direct_floor_sats`, `settlement_config`, `ctv_fanout_fee_policy` | `config.ctv_direct_floor`, `config.ctv_config`, `fee` (`:761-763`) | `config.*` (`:1009-1010`), the drift, and `source.ctv_fanout_fee_policy` | **stored verbatim** as one nested `ctv: Option<{ direct_floor_sats, settlement_config, fanout_fee_policy }>`, whose presence also selects the CTV builder in place of `config.ctv_enabled` (`:755`, `:1003`) | O(1) |
| `coinbase_script_sig_suffix_hex` | `Some(suffix)` (`:764`) | `claim.candidate.coinbase_suffix_hex` (`:992`) | already stored, **required after 007**: submit always writes `Some` (`:1697`); the `Option` with `serde(default)` (`srv/src/ledger/candidates.rs:10-11`) exists for pre-007 rows, so post-007 decode rejects `None`, and the `else` branch that lands the stored bundle when the suffix is absent (`srv/src/coordinator.rs:1036-1038`) is removed with the bundle | O(1) |
| `witness_merkle_leaves_hex` | `codec::witness_merkle_leaves_hex` over the template's transactions (`:749-750`) | `source.witness_merkle_leaves_hex` | **re-derived** from the stored `block_hex` by `codec::witness_merkle_leaves_from_block`, below | not stored twice |
| signing keys | `config.manifest_seed`, `config.ledger_seed` (`:751-752`) | `config.*` (`:1001-1002`) | local: the seeds are the signer, not a field; both public keys are **stored** as `signer_keys` and checked ([Signing keys](#signing-keys)) | about 130 B |
| builder logic | the linked `qbit-prism` | the linked `qbit-prism`, possibly a newer release | **stored** as `audit_builder_version` and checked, never re-derived ([Builder version](#builder-version)) | 2 B |

`audit_commitment_leaves_hex` is a builder output, not an input. The
`bootstrap_share: Option<AcceptedShare>` that a candidate found on an empty
window carries (about 600 B) follows the same rule: its fields come from the
worker's identity and the template the issuing frontend saw, which the
claiming frontend does not have, and it sits in `shares` and
`reward_manifest.shares`, so re-deriving it is exactly the drift the invariant
forbids. Submit sets it when `context.prepared.bundle.is_none()`
(`srv/src/coordinator.rs:27`; the `None` arm at `:1343` is where the
per-worker bootstrap bundle was built). `deferred_share`
(`srv/src/ledger/candidates.rs:12-13`) is the precedent for one inline share. Decode
requires `bootstrap_share.is_some() == window.shares.is_none()` and
`found_block.anchor_job_issued_at_ms == window.anchor_ms`, because
`persist_audit_snapshot` takes the anchor from the bundle
(`srv/src/ledger/audit.rs:123`) while `read_window` takes it from the
reference.

**Witness leaves.** No server code splits a block into transactions today;
the only derivation is from the template (`srv/src/codec.rs:253-275`, used at
`srv/src/coordinator.rs:749-750`). Re-deriving from `block_hex` is sound
because `assemble_submission` appends the template's transactions verbatim
after the coinbase (`srv/src/codec.rs:490-497`). #265 adds
`codec::witness_merkle_leaves_from_block(&[u8]) -> Result<Vec<String>>`: it
skips the 80-byte header and the compact-size transaction count, as landing
does (`srv/src/ledger/blocks.rs:68-78`), parses past the coinbase with a
segwit-aware transaction parser (`parse_transaction`,
`crates/qbit-prism/src/ctv.rs:1629`, is private today and the candidate for
reuse) and `double_sha256`s each remaining transaction. Its test asserts
equality with the template-derived leaves for a block assembled from that
template. Storing the leaves instead was rejected: one 64-hex string per
transaction scales with the block and would break the row-size bound below.

Every stored input is O(1), about 1 KB in total, and `candidate_sha256`
covers all of them. The one input that scales is `block_hex`, already stored
today (`srv/src/ledger/candidates.rs:6`): it grows with the block, hex doubling the
serialized size, not with the window. So #265 moves `block_hex` out of the JSONB document into a `bytea` column, `block_bytes`, on the same row:
the JSONB value stays under 1 MB without restating #265's criterion, and the row still carries the block, which consensus bounds, not the window. The
bytes stay authenticated. The candidate JSON keeps `block_sha256`, the
SHA-256 of the `bytea`, which `candidate_sha256` covers. Claim decode checks
both that digest and that the block's 80-byte header double-SHA-256s to
`block_hash`, so altered or inconsistently written bytes fail as `Decode`
before any witness derivation, landing or submission.

### Builder version

Stored inputs reproduce a bundle only under the builder that first built it.
A release can change what the builders emit for the same inputs: a new reward
rule, a new manifest field, a canonical-schema change. It would then rebuild a
surviving reference into a different bundle. A pending candidate's coinbase
already commits to the old one, so its claim would fail the audit checks on
every retry, or a retrospectively changed body would land. The `schema`
strings (`AUDIT_BUNDLE_SCHEMA_V1` and `_V1_1`,
`crates/qbit-prism/src/lib.rs:96-97`) don't capture this, because the share
shape chooses them (`:1556-1560`), not the builder logic. The golden test
(`crates/qbit-prism/tests/audit_cli.rs:353`, digests at `:497-521`) detects
such a change for its fixture, but it cannot rebuild the old output.

- **Version.** `qbit-prism` gains `pub const AUDIT_BUILDER_VERSION: u16`,
  starting at 1, in the PR that adds 007 (#265). Both documents store it as
  `audit_builder_version` next to the other stored inputs: the `Candidate`
  for 007, where `candidate_sha256` covers it, and `StoredPrepared` for 008.
- **Bump rule.** Any change that alters `canonical_audit_bundle_bytes` for
  the same inputs bumps the constant in the same commit. The golden test keys
  its expected digests by the constant, so a new output is recorded as a new
  entry next to the old one, not by overwriting it, and review sees both.
- **Rebuild check.** A rebuild compares the stored version with the binary's
  before it calls `read_window`. A mismatch is never rebuilt with the current
  builder ([Errors and callers](#errors-and-callers)).
- **Upgrade refusal.** A release that bumps the constant ships a migration
  step, with or without DDL. Its refusal predicate counts `state='pending'`
  outbox rows whose `audit_builder_version` differs from the new value, and
  the operator drains them with the old frontends through the 007 procedure
  ([Compatibility](#compatibility)). Job rows need no refusal: a mismatched
  prepared job is a cache miss, and the next refresh writes a new one.
- **Rejected alternative.** Keeping a frozen copy of each old builder, so old
  references rebuild under it, would let a release skip the drain. This
  record drains and refuses instead: the outbox drains in minutes, and every
  retained builder is more signed-output code to audit.

### Signing keys

The seeds stay local, because they are the signer, not a field. But the
bundle embeds both public keys and their signatures
(`build_profiled_signed_manifest` and `build_ledger_window_attestation`,
`crates/qbit-prism/src/lib.rs:54` and `:1507`), so a rebuild reproduces the
original bytes only under the original keys. With unchanged seeds it does, as
the golden test's digests for fixed seeds show. The cluster fingerprint binds
both public keys (`srv/src/config.rs:479-491`), and `configure` pins it once
and refuses a mismatch (`srv/src/ledger/connect.rs:128-148`). The code has no reset
path, so rotating either key takes an operator resetting
`qbit_prism_cluster.config_fingerprint`. A candidate still pending across
that reset would be rebuilt and signed with the new keys, into bytes its
coinbase does not commit to.

- **Stored.** Both documents store `signer_keys: { manifest_key_hex,
  ledger_key_hex }`, the public keys the building frontend signed with, about
  130 B; `candidate_sha256` covers them for 007.
- **Rebuild check.** It runs next to the builder-version check, before
  `read_window`. A mismatch is never rebuilt with the local keys
  ([Errors and callers](#errors-and-callers)).
- **Rotation procedure.** A rotation follows the 007 order
  ([Compatibility](#compatibility)): with the old-key frontends running,
  drain the outbox; stop **every** old-key frontend; only then check that no
  `state='pending'` row remains; reset the fingerprint; start the new-key
  frontends. The check has to come after the stop, because a frontend still
  running could find a block after it and enqueue a candidate with the old
  keys, which no new-key frontend can rebuild.
- **Rotation refusal.** #265 extends `configure`: when it writes a
  fingerprint onto a reset (NULL) one, it refuses in the same transaction
  while any `state='pending'` outbox row stores `signer_keys` other than the
  local pair. Prepared jobs signed with the old keys are cache misses.
- **Writer fence.** A one-time check can't stop a writer that runs later, and
  today's candidate enqueue is fenced only by `ORDER_LOCK` and the revision
  (`srv/src/ledger/candidates.rs:29`), not by the fingerprint. So #265 and
  #273 re-read `qbit_prism_cluster.config_fingerprint` with `FOR SHARE` inside
  every enqueue and `save_job` transaction, and refuse the write, with an
  alert, when it isn't the writer's own. `FOR SHARE` conflicts with
  `configure`'s `FOR UPDATE` (`srv/src/ledger/connect.rs:128-148`), so a
  write and a reset can't interleave: either the write commits first and the
  refusal above sees it, or the reset does and the write fails. A frontend
  left running with old keys then fails loudly instead of creating rows no
  new-key frontend can rebuild. The fence makes a procedure violation
  visible; the stop-first order is what keeps old-key work from existing at
  all.
- **Rejected alternative.** A versioned signer that keeps old seeds to
  reproduce old signatures would keep retired private keys online, which
  defeats a rotation made because a key leaked.

## Compatibility

- **Rows written before 007.** Terminal outbox rows already have
  `candidate IS NULL` and are untouched. 007 refuses to apply while any
  `state='pending'` row still holds an inline candidate (`candidate ? 'bundle'`),
  naming the rows: one-way per D5, modelled on the `version < 3`
  pending-shape check at `srv/src/ledger/connect.rs:74-75`. After 007 there
  is no compatibility decode: a pending row with a NULL `window_anchor_ms` is
  a hard claim error telling the operator to stop the pre-007 frontend.
- **Rows written before 008.** Legacy inline prepared rows stay in place.
  Resume treats a payload with `snapshot` or `bundle` keys, or a NULL
  `window_anchor_ms` on a `prepared:` row, as a cache miss (`Ok(None)`)
  because prepared work is regenerable; the rows expire under their TTL and
  `prune_expired_jobs` (`srv/src/ledger/jobs.rs:43-44`) removes them. This is
  unambiguous now: an empty window is not NULL.
- **Mixed versions.** 3.x.x does not support a pre-007/008 and a post-007/008
  frontend on one database: the old frontend would keep writing inline
  documents the new claim path rejects and would fail to decode a reference
  job on cross-frontend resume. The one-way model is the one `Ledger::connect`
  already enforces for the legacy writer lease (`srv/src/ledger/connect.rs:66`) and
  [prism-rust-migration](../prism-rust-migration.md) documents; #285 gates
  startup on the schema version.
- **Upgrade procedure.** A `state='pending'` outbox row is cleared only by a
  frontend claiming and landing it: the CHECK at
  `crates/qbit-prism/sql/001_share_ledger.sql:97-104` keeps `candidate`
  non-NULL until the row is terminal, and `retry_candidate`
  (`srv/src/ledger/candidates.rs:114-126`) only backs off, never abandons. So "refuse
  while pending inline rows exist" plus "apply with every frontend stopped"
  would deadlock. There are two starting points. From a **native pre-007
  3.x.x deployment**, the native `submit_loop` drains the outbox, step 1
  below. From **production 2.x.x**, the 2.x.x submitter drains it before the
  cutover, and leftovers are refused twice: by the `version < 3` gate
  (`srv/src/ledger/connect.rs:74-75`) on the first native connect and by #285's
  006; #265's migration test covers "a 2.x.x schema with only terminal
  rows". The order, with the 2.x.x frontends doing step 1 and any repeat in
  that case, is:
  1. With the pre-007 frontends **running**, drain the outbox. `submit_loop`
     (`srv/src/coordinator.rs:1112-1135`) polls every 100 ms and claims each
     pending row; a row whose revision or parent is superseded is finished by
     `finish_candidate_at_revision` (`:979-988`); a failed attempt is retried
     after `LEAST(60, attempt_count)` seconds (`srv/src/ledger/candidates.rs:118`). A row
     that keeps failing is #268's escape hatch; the procedure never abandons
     it. No switch stops candidate production while keeping `submit_loop`, so
     a block can be found at any moment until the frontends stop, and a zero
     count taken while they run proves nothing.
  2. Stop every frontend. From here nothing creates, claims or finishes a
     candidate.
  3. Only now verify `SELECT count(*) FROM qbit_block_candidate_outbox WHERE
     state='pending'` is 0. If it is not, a block was found between the drain
     and the stop: start the pre-007 frontends again, let `submit_loop` drain
     it, and repeat from step 2.
  4. Start one post-008 frontend. `Ledger::connect` applies migrations in one
     transaction (the base schema at `srv/src/ledger/connect.rs:77-80`, versioned
     steps after it), so it applies 006 (#285), then 007, then 008, each
     refusal predicate running in that transaction and naming the rows it
     found. Then start the rest; #285's startup gate keeps a pre-007/008
     frontend from joining. 007's refusal is the backstop: if a pending
     inline row is present anyway, the migration transaction fails naming it,
     nothing is applied, and the operator returns to step 1 with the pre-007
     frontends.
- **Refusal predicates compose.** 006 (#285) refuses on its own pending
  shape (#258's storage-version-2); 007 refuses on `EXISTS(SELECT 1 FROM
  qbit_block_candidate_outbox WHERE state='pending' AND candidate ? 'bundle')`.
  Each predicate runs inside its own `version < N` step of the migration
  transaction, in migration order and one-way, never unconditionally at
  startup. The pending-shape check at `srv/src/ledger/connect.rs:74-75` is the
  model for the shape only: it sits inside `if version.unwrap_or(0) < 3`
  (`:49-95`), runs before the base schema (`:77-80`) on the first native
  connect after 2.x.x, and never runs at version 7 or above, so #265 leaves
  it alone.
- **Canonical hashes.** Under one `AUDIT_BUILDER_VERSION` and one pair of
  signing keys, a bundle rebuilt from `read_window` through the borrowing
  builders is field-for-field the bundle built at refresh, so
  `canonical_audit_bundle_bytes` and `audit_bundle_sha256` are unchanged; the
  golden test (`crates/qbit-prism/tests/audit_cli.rs:353`) pins them for its
  fixture. Across a bump or a key rotation, [Builder version](#builder-version)
  and [Signing keys](#signing-keys) apply.

## Where the code lives

#283 landed the `ledger.rs` split (#300), and `srv/src/ledger/window.rs`
exists and belongs to workstream B. It already holds `Snapshot`
(`srv/src/ledger/window.rs:10-16`), `AppendResult` (`:4-7`), the `append*`
family (`:74-185`), `snapshot` (`:190-241`), `observe_chain_view` (`:21-72`),
`read_prior_balances` (`:244-252`), `share_header_hash` (`:254-261`) and
`share_from_row` (`:263-283`). `WindowRef`, `ShareRange`, `Window`,
`WindowError` and `read_window` go there too: every one of them is B-owned
code that only B-owned callers and A's #265 claim path use, and the file
already owns the window read they extend.

`SELECT_SHARE` needs no move. It stays a `const` in the module root
(`srv/src/ledger.rs:40`), where `window.rs` and `ledger/audit.rs` both reach
it through `use super::*`. `read_range` likewise stays in A-owned
`ledger/audit.rs` (`srv/src/ledger/audit.rs:144-152`).

One question is left, and it is narrower than before the split: whether
`read_window` reuses `read_range` from `audit.rs` as it stands, or
`read_range` moves into `window.rs` so both readers share one paged
implementation. Only the second edits an A-owned file. **Pending confirmation
with Anatolie on #283**; this record does not claim that choice is settled.

## Bundle ownership

`AuditBundle` is unchanged. #296 (merged into 3.x.x as `f316eb9`) added to
`crates/qbit-prism/src/lib.rs`:

| Entry point | Purpose |
| --- | --- |
| `AuditBundleBody` | every `AuditBundle` field except the top-level `shares`; `#[serde(deny_unknown_fields)]`, so a full bundle cannot decode as a body |
| `build_audit_bundle_body`, `build_audit_bundle_body_with_coinbase_script_sig_suffix`, `build_audit_bundle_body_with_coinbase_options`, `build_audit_bundle_body_with_ctv_settlement_options`, each taking `&[AcceptedShare]` | borrowing builders mirroring the four `build_audit_bundle*` functions (`crates/qbit-prism/src/lib.rs:1567`, `:1611`, `:1658`, `:1753`) |
| `verify_audit_parts(&AuditBundleBody, &[AcceptedShare], …)` and its `verify_audit_parts*` variants | verification without an owned bundle |
| `canonical_audit_bundle_bytes_from_parts(&AuditBundleBody, &[AcceptedShare])`, `write_canonical_audit_bundle_from_parts` | canonical bytes without splicing |
| `AuditBundleBody::into_bundle(Vec<AcceptedShare>)`, `AuditBundle::into_parts()` | lossless conversion both ways |
| `prior_balances_digest(&[CarryForwardBalance]) -> [u8; 32]` | the private `prior_balances_digest_hex` (`crates/qbit-prism/src/lib.rs:2670`) made public for `WindowRef` |

An earlier draft called the body `AuditBody`; it is `AuditBundleBody` so it
reads next to `AuditBundle` and is not mistaken for the `audit_body_ref` file
format. Both paths serialize through one private `AuditBundleRef<'a>`, so the
canonical bytes are identical by construction; the four existing
`build_audit_bundle*` functions became one-line wrappers with no clone, and
no server file changed in #296. No serde `rc` feature in `qbit-prism`.

Not `Arc<[AcceptedShare]>`: #296 records why (serde `rc`, the `&durable ==
shares` equality at `srv/src/ledger/audit.rs:135`, and no saving while
`reward_manifest.shares` stays).

**Caveat.** `AuditBundleBody` still embeds `reward_manifest.shares`
(`crates/qbit-prism/src/lib.rs:234`): one `CountedShare` per counted share,
about 556 B each (computed), about 222 MB at 400k. Storing an
`AuditBundleBody` is not a fix for the JSONB ceiling; the body alone crosses
it at about 483k counted shares. #265 and #273 store a `WindowRef` and
rebuild the bundle with the borrowing builders; #267 normalizes the stored
body. The #264 head itself saves no memory; the saving arrives when #265 and
#273 stop holding owned bundles.

## Expected cost at 400k shares

Estimates from the measured facts in [Problem](#problem) plus three computed
sizes: an `AcceptedShare` about 594 B as compact JSON (581 B measured
production-shaped), a `CountedShare` about 556 B, a native production-shaped
share about 650 B. The #264 scale harness replaces every estimate.

| Quantity | Today (3 copies in a job row, 2 in a candidate) | With `WindowRef` |
| --- | --- | --- |
| rows read per rebuild | 1 JSONB row | 400,000 share rows in 98 pages + 1 cluster row + one row per recipient with a balance + 2 probe rows |
| bytes read | 700 to 780 MB JSONB; fails above 268 MB per container (measured at 372k) | about 175 MB on the wire |
| one share array as JSON | 233 to 260 MB | never materialized: the digest streams |
| decode memory | a 470 to 780 MB `serde_json::Value`, then the typed document | about 250 MB for `Vec<AcceptedShare>` plus one 2 MB page of raw rows |
| decode time | JSONB parse of the whole document (fails at production scale) | 0.4 to 0.8 s CPU for row mapping; 1 to 3 s database and transfer |
| hash time | none at claim; `candidate_sha256` over the document | 0.5 to 0.9 s to serialize 233 to 260 MB; 0.2 to 0.5 s SHA-256 |
| total per rebuild | n/a (fails) | 2 to 5 s wall, 1.1 to 2.2 s on blocking threads; the 60 s claim deadline is twelve times the upper bound |
| empty window rebuild | n/a | 2 statements, no share rows, microseconds of hashing; the bootstrap bundle has one share |
| k concurrent rebuilds | n/a | k ≤ `build_workers` (default at most 4): at most `clamp(database_max_connections - 2, 1, build_workers)` pool connections, held only during `read_window`, and about k × 0.5 GB peak, held only during the rebuild because resumed jobs keep no window (one window, owned by the `Snapshot`, plus the rebuilt body's `reward_manifest.shares`; `Prepared` holds no second copy, see [Threads, concurrency and deadlines](#threads-concurrency-and-deadlines)); k = 1 per `storage_key` under the single-flight map |
| WAL per write, measured | 44,752,760 B for one single-window insert into `qbit_prism_jobs` at 372k | under 1 KB for the six columns (about 150 B payload), plus at most one 8 KB full-page image |
| WAL per write, derived | about 134 MB per refresh at three copies and 90 MB per candidate at two, when under the ceiling; #273 estimates 220 MB per refresh for production data at 3:1 | as above |
| refresh CPU added | none | one serialization and SHA-256 of the window per non-cached refresh, 0.7 to 1.4 s on a blocking thread; landing already pays it at `srv/src/ledger/audit.rs:120` |

The window's share of WAL per refresh falls by more than four orders of
magnitude. The read cost is the price of not storing the window; it equals one
`Ledger::snapshot` scan and is paid only on claim and cross-frontend resume,
not on every refresh.

## How #265 and #273 use this

| Step | #265, migration 007, candidates and import | #273, migration 008, prepared work and job rows |
| --- | --- | --- |
| columns | the six on `qbit_block_candidate_outbox`, with `window_prior_balances_sha256` indexed for retention | the six on `qbit_prism_jobs`; per-worker `StoredJob` rows keep them NULL; plus an indexed `template_sha256` on prepared rows and an index on `window_prior_balances_sha256`, both for retention |
| document | `Candidate` (`srv/src/ledger/candidates.rs:4-14`) drops `bundle: AuditBundle` for `window: WindowRef`, `bootstrap_share: Option<AcceptedShare>`, `found_block`, `payout_policy`, `ctv: Option<{ direct_floor_sats, settlement_config, fanout_fee_policy }>`, `audit_builder_version`, `signer_keys` and a `leased` flag ([Revision fence and reorgs](#revision-fence-and-reorgs)); it keeps `block_hash`, `block_hex` (moved to a `bytea` column and authenticated by a new `block_sha256`, [Stored bundle inputs](#stored-bundle-inputs)), `job_id`, `payout_revision`, `deferred_share` and `coinbase_suffix_hex`, now required. The rebuilt parts, `Arc<AuditBundleBody>` and the window's shares, live in `CandidateClaim` (`srv/src/ledger/candidates.rs:17-20`), absent until the rebuild fills them where `srv/src/coordinator.rs:1032-1034` replaces `candidate.bundle` today; landing reads them there instead of `claim.candidate.bundle.*` (`srv/src/ledger/blocks.rs:61-153`) and never assembles an `AuditBundle` ([Threads, concurrency and deadlines](#threads-concurrency-and-deadlines)), and `observe_candidate` reads the stored `found_block.block_height` from the candidate instead of through the bundle (`srv/src/coordinator.rs:829`) | `StoredPrepared` (`srv/src/coordinator.rs:46-55`) drops `snapshot` and `bundle` for `window: WindowRef` plus the `Snapshot` scalars the reference does not carry: `share_seq`, the newest sequence at snapshot time (`srv/src/ledger/window.rs:236`), which the cached-work equivalence test compares (`srv/src/coordinator.rs:612`), and `payout_revision`, which `save_job` fences (`srv/src/ledger/jobs.rs:16-23`); resume no longer requires it to equal the current revision ([Revision fence and reorgs](#revision-fence-and-reorgs)). It stores the policy inputs `build_bundle` reads from `config` at `:760-762`: `payout_policy` and the same nested `ctv` field, which also covers the `config.ctv_enabled` choice at `:755` and absorbs today's `fee` (`:50`; `fee_policy` is `None` when CTV is off, `:319-322`). It stores `audit_builder_version` and `signer_keys` too, and keeps `fingerprint`, `generation`, `parent_of_tip` and `coinbase_suffix`. The `template`, the full `getblocktemplate` result with every transaction's hex, moves out of the payload into an immutable `qbit_prism_templates` row keyed by the SHA-256 of its bytes, written once per distinct template (`ON CONFLICT DO NOTHING`) in `save_job`'s transaction; `StoredPrepared` keeps `template_sha256`, and so does a new column on the job row that retention uses ([Immutability and retention](#immutability-and-retention)), and a resume fetches the row, checks the digest and decodes it in `spawn_blocking`. A consensus-valid template can carry megabytes of transaction hex, so without this the payload breaks #273's 1 MB whole-value criterion however small the window is; #273 qualifies the bound with a large valid template. A cross-frontend resume still builds the bundle the issuing frontend would have built. `Prepared` (`:32-43`) and `JobContext` (`:26-30`) keep the in-memory `snapshot: Arc<Snapshot>`, the only owned window, and hold `Arc<AuditBundleBody>` instead of `Arc<AuditBundle>` (`:29`, `:35`), built by borrowing (`:747` stops cloning), so neither the local frontend nor a resume keeps a second copy of the window. `Prepared` also keeps `window: WindowRef`, the reference `refresh_once` computed or the resume decoded, so both documents take the same small value |
| write | submit (`srv/src/coordinator.rs:1691-1699`) clones `Prepared.window`, the `WindowRef` `refresh_once` already computed, a few hundred bytes, so a found block never re-digests the window on the share path (the 0.7 to 1.4 s the cost table budgets once per refresh), and sets `bootstrap_share` from `JobContext.bootstrap_share` when `context.prepared.bundle.is_none()` (`:27`; the `None` arm at `:1343` is where the per-worker bootstrap bundle was built); persist (`srv/src/ledger/candidates.rs:182-183`) serializes and digests the small row before the append transaction opens and writes the columns with it | `refresh_once` (`srv/src/coordinator.rs:468`, the store at `:627-641`) computes the reference once per non-cached refresh and keeps it as `Prepared.window`, which submit clones (#265) and a resume takes from `StoredPrepared`; `save_job` writes the columns and a payload with no `shares` key; an empty snapshot writes an empty-window reference |
| read | claim decode (`srv/src/ledger/candidates.rs:73-79`) checks the digest and the columns, and that the `bytea` block hashes to the candidate's `block_sha256` with its header hashing to `block_hash` | resume (`srv/src/coordinator.rs:1447-1470`) decodes the small payload inline, so `:1469-1470` no longer needs `spawn_blocking` for it; the template fetched by `template_sha256` is decoded in `spawn_blocking` |
| fence | `process_candidate_inner` (`srv/src/coordinator.rs:966`) keeps `:972-979`; `Window.payout_revision != candidate.payout_revision` is a second hint for the same `observe_candidate` probe and never a supersession by itself ([Revision fence and reorgs](#revision-fence-and-reorgs)) | B's published-identity and lease check replaces the revision equality at `srv/src/coordinator.rs:1483-1488`; eligible work rebuilds in as-issued mode, and only ineligible work is `Ok(None)`; the same lease decides staleness at submit (`srv/src/coordinator.rs:1632-1644`, `:1672`), so a block found on leased work is enqueued with R0 ([Revision fence and reorgs](#revision-fence-and-reorgs)) |
| rebuild | under a `build_slots` permit (`:997`) and the 60 s deadline around `read_window` and the rebuild: if `qbit_pool_audit_bundles` already holds the block's audit, authenticate that row against the block's coinbase ([Revision fence and reorgs](#revision-fence-and-reorgs)), skip `read_window`, the rebuild and `land_candidate`, never call `materialize_audit_row`, and continue to observe, renew the lease and `submitblock` as today; else await `read_window(…, BalanceSource::Current)` (a `leased` candidate submits before any rebuild and rebuilds only with `AsIssued`, [Revision fence and reorgs](#revision-fence-and-reorgs)), re-derive the witness leaves from `block_hex`, run `build_audit_bundle_body_*(&window.shares, …)` directly in `spawn_blocking` (`:998-1031`; never `build_bundle`, which takes a second permit at `:712`), and put the body and `window.shares` in `CandidateClaim` as parts for landing, never `into_bundle` ([Threads, concurrency and deadlines](#threads-concurrency-and-deadlines)) | under one `build_slots` permit, the single-flight entry for the `storage_key`, within the caller's end-to-end deadline: await `read_window(…, BalanceSource::AsIssued)`, move its `Vec` into a `Snapshot` whose `payout_revision` is `StoredPrepared.payout_revision`, not `Window.payout_revision`, and rebuild `Prepared` as `(Arc<Snapshot>, Arc<AuditBundleBody>)` through the borrowing builders called directly, never `build_bundle`, or use the local incremental window once #274 lands; once the job's coinbase is built, keep only the slim resumed `Prepared`, with no window ([Threads, concurrency and deadlines](#threads-concurrency-and-deadlines)) |
| empty window | build over `&[bootstrap_share]` | the single-flight entry carries `bundle: None`; each miner gets its own bootstrap bundle, built with the builders directly under a fresh `build_slots` permit taken after the entry's permit is released, from the synthetic share `build_bundle` fabricates from the worker today (`:727-745`, reached from `:1498-1510`), the stored template, anchor, `payout_policy` and `ctv`, the as-issued balances the entry keeps until every waiter has built, and the issued `payout_revision` from `StoredPrepared`; never `build_bundle`, which reads `config` for signed fields (`:755`, `:760-762`) |
| import | `srv/src/ledger/migration.rs:117` stores `canonical_audit_bytes` plus non-share metadata and no inline body. The same PR makes both readers decode `canonical_audit_bytes`, digest-checked against `audit_bundle_sha256` and under `spawn_blocking` (a two-copy body is about 470 MB at 400k), before any `body_uri` fallback, as `audit_canonical_bytes` already does (`srv/src/ledger/audit.rs:12-23`): `Ledger::audit_bundle` (`:92-107`), which today returns only the JSON column, so `backfill-ctv` would stop on imported rows with "import legacy audits first" (`srv/src/ledger/migration.rs:133-136`); and the bundle endpoint's fallback (`srv/src/api/read_models.rs:196-231`), which today reads only `body_uri`, so every frontend would still need the legacy filesystem. Keeping the inline body instead would bring back the two-copy JSONB document this design removes | n/a |
| must not | serialize a share array into the outbox; re-digest the window at submit instead of cloning `Prepared.window`; hydrate a landed audit on the submit loop through `materialize_audit_row`; finish a recovered claim without checking the landed coinbase and audit root against `block_hex`; finish a claim only because its audit has landed; finish or land a `leased` candidate before `submitblock`; finish a submitted `leased` candidate before its audit has landed; rebuild a `leased` candidate with `BalanceSource::Current`, or before `submitblock`; drop the rebuilt `CandidateClaim` parts on a runtime thread; hand landing an assembled `AuditBundle`, or clone, serialize, digest or re-read the window unpaged on a runtime thread while landing; use block bytes whose `block_sha256` or header hash doesn't match; leave `block_bytes` set on a terminal row; hold `ORDER_LOCK` across `read_window`; call `read_window` without a `window_reads` permit; decode inline candidates on a post-007 schema; read local configuration for any stored input; rebuild a reference whose `audit_builder_version` or `signer_keys` differ from this binary's; accept a new fingerprint in `configure` while a pending row stores other `signer_keys`; enqueue a candidate without re-reading `config_fingerprint` `FOR SHARE` in the same transaction; wrap `read_window` in `spawn_blocking`; call `build_bundle` under a held `build_slots` permit; leave an imported audit readable only through `body_uri` | write a share array into `payload`; call `read_window` without a `window_reads` permit; wrap `read_window` in `spawn_blocking`; run whole-window serde on a runtime thread; call `build_bundle` for any resume rebuild, empty window included (it takes a second permit and reads `config` for signed fields); hold an owned `AuditBundle` beside the `Snapshot` in `Prepared` or `JobContext`; resume a job whose `audit_builder_version` or `signer_keys` differ from this binary's; rebuild published work with the current balances; let any job but the published one gain the replacement lease; turn a decode, digest, database or deadline failure into `unknown-job`; keep the full template in the prepared payload; insert a template or balance-snapshot row outside `save_job`'s transaction, or prune them without `SETTLEMENT_LOCK` then `ORDER_LOCK`; scope the balance-snapshot prune to an expired batch's digests; give a resumed `Snapshot` the current revision instead of `StoredPrepared.payout_revision`; run `save_job` without re-reading `config_fingerprint` `FOR SHARE` in its transaction; keep `Snapshot.shares` or `reward_manifest.shares` in a resumed job once its coinbase is built; drop a rebuilt window on a runtime thread; issue an empty-window `JobContext` without its synthetic `bootstrap_share`; drop a stored input from a resumed job before its candidate is enqueued; mark leased work stale at submit on a revision change, or drop a block found on it; bridge a change of `prior_balances_digest` with the lease before B8 and #289 define how such a block lands; release a `build_slots` permit before its blocking build finishes; change the `save_job` revision fence (`srv/src/ledger/jobs.rs:16-23`) or the cached-work reuse conditions (`srv/src/coordinator.rs:528-533`) |
| text to amend on merge | "reconstructs … through `Ledger::read_window` in `spawn_blocking`": `read_window` is awaited, only the builder runs in `spawn_blocking`; the closed field list ("stores the `WindowRef` … plus … not the bundle"): it is the [Stored bundle inputs](#stored-bundle-inputs) table, `found_block`, `payout_policy`, `ctv`, `bootstrap_share`, `audit_builder_version`, `signer_keys` and the required `coinbase_suffix_hex`; and "outbox row under 1 MB", met by moving `block_hex` into a `bytea` column | the five-field reference list: it is `anchor_ms`, `prior_balances_digest` and an optional range of four (`first_share_seq`, `last_share_seq`, `share_count`, `snapshot_sha256`) |

**#267, audit bodies.** Uses `AuditBundleBody`, `verify_audit_parts` and
`canonical_audit_bundle_bytes_from_parts` so `ledger/audit.rs` and
`api/read_models.rs` verify and serve without materializing a bundle, and
normalizes `reward_manifest.shares` out of the stored body; the
`share_snapshot_sha256` FK stays.

## Open questions

- **djh58 (#273).** The four contract points on #273 are answered here:
  published work keeps its authority and rebuilds with as-issued balances,
  reconstruction failures stay distinct from missing work under one
  end-to-end deadline, both share digests are named with #274's reuse
  instruction amended, and the template leaves the prepared payload. Still to
  confirm: the `qbit_prism_balance_snapshots` and `qbit_prism_templates`
  shapes. Legacy inline prepared rows expire as cache misses, a stop-all
  upgrade policy for the development line that C's migration procedure and
  tests must state, not a production compatibility guarantee. The 0.7 to
  1.4 s of digest CPU per non-cached refresh is an interim cost to measure,
  not an approved budget. Also for djh58 and #289: how an active leased block
  lands against as-issued balances after a balance change. Until that rule
  exists, the lease covers only revision changes that leave
  `prior_balances_digest` unchanged.
- **Anatolie (#283).** Narrowed by #300, not settled. `ledger/window.rs`
  exists under B and already holds `share_from_row`, so `WindowRef`,
  `read_window`, `Window` and `WindowError` land there with no cross-workstream
  edit, and `SELECT_SHARE` stays in the module root. What is left is only
  `read_range`: whether `read_window` reuses it where it is, in A-owned
  `ledger/audit.rs`, or it moves into `window.rs` so both readers share one
  paged implementation. Only the move edits an A-owned file; either A signs
  off on that one edit or #265 keeps calling `audit.rs`.
- **D1 (#260).** The cost section assumes 400k shares. A single share array
  has 3 to 13 % headroom under the JSONB ceiling there, and an
  `AuditBundleBody` crosses it at about 483k counted shares; a target above
  that makes #267 a P0 alongside #265 and #273.
- **D6 (#260).** Retention must never prune below the floor above, must keep a ratcheted horizon and record its floor so `snapshot` fails rather than publish an underweight window, must take
  `SETTLEMENT_LOCK` then `ORDER_LOCK`, the established order, while it
  computes and deletes, must honour the in-flight reservations
  ([Immutability and retention](#immutability-and-retention)), and must
  narrow the immutability trigger only for that job. It may add the
  `window_first_share_seq` index.
- **#265, #285 and #287.** Whether 007's refusal should also consider live
  pre-007 instances heartbeating in `qbit_prism_instances` (#285's startup
  gate), and whether the legacy audit import registers a
  `qbit_prism_audit_snapshots` row through `read_window` when the history
  exists or stays bytes-only (#287).

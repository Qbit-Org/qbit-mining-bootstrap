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
below; djh58's agreement (#273) is required before merge, and the module
location waits on Anatolie (#283). The parts API (PR 2) is merged as #296.
Code citations are at the 3.x.x base `1398bbc` (the #244 merge), which
predates #296; `srv/` is `crates/qbit-prism-server/`.

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
| `Candidate` (`srv/src/ledger.rs:39`) | 2 | `bundle.shares`, `bundle.reward_manifest.shares`; written at `:695-696` |
| legacy audit import (`srv/src/ledger/migration.rs:117`) | 2 + 2 | the inline bundle as JSONB, and both copies again in `canonical_audit_bytes` (bytea) |
| landed audit body (`srv/src/ledger/blocks.rs:140`) | 1 | `reward_manifest.shares` survives `remove("shares")` |

The claim phase writes no new window value. Of the 17 JSONB columns at the
base, the window reaches three: `qbit_prism_jobs.payload` at refresh,
`qbit_block_candidate_outbox.candidate` at enqueue, and
`qbit_pool_audit_bundles.audit_bundle` at landing and import.

## `WindowRef`

```rust
// qbit-prism-server; planned `ledger/window.rs` after #283 (pending Anatolie)
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

A reference is built from a `Snapshot` (`srv/src/ledger.rs:64-70`):
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
field for 007 (`srv/src/ledger.rs:43`); `read_window` returns the current
value for the caller's fence ([Revision fence and reorgs](#revision-fence-and-reorgs)).

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
| `share_slice_digest_hex` | not reused; `snapshot_sha256` instead | it hashes `CountedShare` fields (`crates/qbit-prism/src/lib.rs:2113-2131`), a build output (`:632`, carried at `:422`) that omits `network_difficulty`, `template_height`, `job_id` and `ntime`. A reference must authenticate the builder's *input* before the build runs, so `snapshot_sha256` is over `AcceptedShare` JSON, the bytes `qbit_prism_audit_snapshots` already stores |
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
  lowercase hex through the `hex32` adapter ([`WindowRef`](#windowref)): the `Candidate` for 007 (`srv/src/ledger.rs:39-49`), the
  `StoredPrepared` for 008 (`srv/src/coordinator.rs:46-55`). For 007,
  `candidate_sha256` keeps covering the whole document including the
  reference, as it covers the candidate today (`srv/src/ledger.rs:695-696` at
  enqueue, `:549-555` at claim).
- The six columns are a typed duplicate for the CHECK and the future
  retention index, written in the same statement as the document.
- Decode reads the document, then compares it with the columns returned by
  the same statement: the claim `UPDATE … RETURNING` (`srv/src/ledger.rs:640`)
  and `Ledger::job` (`:490-494`) gain the six columns. Any disagreement,
  including a document with a range and NULL range columns, is a decode
  error, surfaced like corruption (`Decode` below).

## `Ledger::read_window`

```rust
pub struct Window {
    pub shares: Vec<AcceptedShare>,               // ascending share_seq, exactly share_count rows; empty for shares: None
    pub prior_balances: Vec<CarryForwardBalance>, // current balances, digest-checked against the reference
    pub payout_revision: i64,                     // read in the same snapshot as the balances; resume's fence, claim's probe hint
}

#[derive(Debug, thiserror::Error)]                // the server crate already depends on thiserror 2 (srv/Cargo.toml)
pub enum WindowError {
    #[error("window range incomplete: expected {expected} shares, read {got}")]
    Incomplete { expected: u64, got: u64 },       // range pruned or missing, or a different predicate
    #[error("prior balances changed since the reference was written")]
    PriorBalancesChanged { expected: [u8; 32], actual: [u8; 32] }, // balances moved, not corrupt; the caller decides
    #[error("window snapshot digest mismatch")]
    SnapshotDigestMismatch { expected: [u8; 32], actual: [u8; 32] },
    #[error("window database error: {0}")]
    Database(#[from] sqlx::Error),                // includes SQLSTATE 57014, the statement timeout
    #[error("window decode error: {0}")]
    Decode(#[source] anyhow::Error),              // share_from_row, hex, or payload/column disagreement: corruption
}

impl Ledger {
    pub async fn read_window(&self, window: &WindowRef) -> Result<Window, WindowError>;
}
```

`WindowError` converts into `anyhow::Error` with `?` at the `anyhow`-based
call sites (`process_candidate_inner` and `resume_job` both return `anyhow`
results); callers match on the variant first, because the variant decides
the caller action ([Errors and callers](#errors-and-callers)).

### Read

`Ledger` has one pool, the primary (`srv/src/ledger.rs:33-36`); there is no
replica pool, and `read_window` must never be given one, because the balances
and the revision must be current for the fence. One transaction on that pool,
`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY` as its first
statement, so the revision, the balances and every page of the range are one
consistent snapshot; a landing that commits mid-read cannot yield a revision
that disagrees with the balances. In order:

1. `SELECT payout_revision FROM qbit_prism_cluster WHERE singleton`.
2. The balances, through the `read_prior_balances` statement
   `Ledger::snapshot` uses (`srv/src/ledger.rs:715-723`), then the
   prior-balances digest, else `PriorBalancesChanged`. It costs microseconds,
   so a reference whose balances moved never pays for the window.
3. If `window.shares` is `None`: commit and return `Window { shares: vec![],
   prior_balances, payout_revision }` without touching `qbit_share_ledger`.
4. Otherwise an existence probe on `first_share_seq` and `last_share_seq`,
   two primary-key lookups; a missing row is `Incomplete` in one round trip
   before any page is read.
5. The range, in **ascending keyset pages of 4096**, mandatory, inside the
   same transaction:

```sql
-- SELECT_SHARE (srv/src/ledger.rs:734) with the predicate of read_range (srv/src/ledger/audit.rs:150)
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
   (`srv/src/ledger.rs:434-435`), not for the direction: it pages descending
   from the cutoff and reverses at `:449` because it learns the start only
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
same window. Streaming the serializer into the hasher, as above, produces the
same bytes.

`prior_balances_digest` is the digest already carried as
`ledger_window_attestation.prior_balances_digest_hex`
(`crates/qbit-prism/src/lib.rs:423`), computed at build (`:1213`) and checked
at verify (`:2082`). Its definition, `prior_balances_digest_hex`
(`:2133-2150`): sort the balances by `(order_key, recipient_id,
p2mr_program_hex)`, then for each balance feed SHA-256 with `recipient_id`,
`order_key` and `p2mr_program_hex` as a big-endian `u64` length followed by the
UTF-8 bytes (`update_string`, `:2152-2155`), then `balance_sats` as a
big-endian `i128` (`update_i128`, `:2165-2167`). The function is private
today; the parts API exports it as
`prior_balances_digest(&[CarryForwardBalance]) -> [u8; 32]`, so the server
computes the reference and `read_window` checks it with the same code.

### Threads, concurrency and deadlines

**`read_window` owns its blocking hand-offs.** It is `async`. The runtime
thread issues the statements and receives each page as sqlx wire buffers; it
runs no serde, no hashing and no `share_from_row`. For each page it moves the
rows, the running `Vec<AcceptedShare>` and the hasher into one
`tokio::task::spawn_blocking` task, which maps the rows (`share_from_row`,
`srv/src/ledger.rs:736-756`: two `u128` parses, five required `String`s plus
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
once. **Under that permit the rebuild calls the `build_audit_bundle_body*`
builders directly, or a `build_bundle` variant that takes the held permit,
never `build_bundle` itself**: `build_bundle` acquires its own permit
(`:712`), so with one worker a nested acquisition would wait on itself. The
claim path already calls the builders directly (`:997-1031`). Resume is per
reconnecting miner (`resume_job`, `:1447-1470`) and every miner of one job
reads the same `prepared:` row (`:1466`), so #273 coalesces concurrent
resumes of the same `storage_key` behind one in-flight rebuild, for example a
single-flight map next to the `Prepared` cache: later miners await the first
rebuild's `Arc<Snapshot>` and `Arc<AuditBundle>` instead of reading the
window again. For an empty window the entry carries `bundle: None`, and each
resuming miner builds its own bootstrap bundle, because sharing one would put
miner A's bootstrap share in miner B's job. It does so under its own
`build_slots` permit, taken after the entry's permit is released, by calling
the builders directly: the synthetic share is the one `build_bundle`
fabricates from the worker today (`:727-745`, reached from `:1498-1510`), and
every other input comes from `StoredPrepared` (template, anchor,
`payout_policy`, `ctv`) and the balances and revision `read_window` returned.
It never calls `build_bundle` itself, which reads `config.ctv_enabled`,
`payout_policy`, `ctv_direct_floor` and `ctv_config` (`:755`, `:760-762`) and
would reintroduce the drift [Stored bundle inputs](#stored-bundle-inputs)
forbids.

**Deadlines.** `read_window` takes no deadline of its own. Each statement,
so each page, runs under the connection's `statement_timeout`, 15 s by
default (`srv/src/ledger.rs:92-102`) and overridable through
`PRISM_DATABASE_STATEMENT_TIMEOUT_MS` up to 600000 ms (`:80-93`); a timeout
surfaces as `Database` with SQLSTATE 57014, and pool acquisition adds its own
15 s (`:96`). The worst case is bounded by `pages × statement_timeout` plus
page CPU, about 98 pages at 400k; the expected total is 2 to 5 s, which the
#264 harness must measure before #265 and #273 rely on it. Each caller puts
one `tokio::time::timeout` around `read_window` plus the rebuild:

| Caller | Deadline | On expiry |
| --- | --- | --- |
| claim (#265) | **60 s** for the whole call. The candidate lease is not a deadline: its heartbeat renews it every 30 s for as long as processing runs (`srv/src/coordinator.rs:887-907`), and only a failed renewal drops the work (`:950-963`). Against the estimate, 60 s is twelve times the 5 s upper bound and four statement timeouts at the 15 s default, so a rebuild that needs longer is a harness finding, not a reason to raise it | fail the attempt through `retry_candidate` (`srv/src/ledger.rs:590`) with an alert; the lease was renewed within the last 30 s of a 120 s term, so the `claim_expires_at > clock_timestamp()` condition holds and the row is rescheduled after `LEAST(60, attempt_count)` seconds (`:594`), never abandoned |
| resume (#273) | strictly shorter than the caller's: `resume_job`'s only caller already wraps it in `timeout(initial_job_timeout_seconds, …)` and maps expiry to the backend error "job resume timed out" (`srv/src/stratum.rs:1215-1220`). Proposed: from the `Duration` the caller already builds, `outer = Duration::from_secs_f64(initial_job_timeout_seconds)`, the inner deadline is `outer - (outer / 2).min(Duration::from_secs(5))`, that is `outer − min(5 s, outer / 2)`: 25 s at the 30 s default (`srv/src/config.rs:195`), 2 s at a 4 s setting, always positive, strictly shorter than the outer timeout and free of `Duration` underflow, so it adds no failure mode the outer `from_secs_f64` does not already have. A plain `− 5 s` would be zero or negative for any setting at or below 5 s, which validation allows: production only requires `> 0.0` (`srv/src/config.rs:194-197`) and `srv/src/stratum.rs:449-452` parses the value without a range check. Tunable by #273 | cache miss: log and return `Ok(None)`; the share is then rejected as `unknown-job` (`srv/src/stratum.rs:1239-1243`), not answered with fresh work |

### Errors and callers

| Outcome | Meaning | Claim (#265) | Resume (#273) | Landing (#265, #267) | Import (#265) |
| --- | --- | --- | --- | --- | --- |
| `Window.payout_revision != row revision` (caller check, not a variant) | the revision moved since the reference was written: a landing, a reorg, a resettlement, or the pool's own block reaching the tip | a hint only, like the cached tip and revision at `srv/src/coordinator.rs:972-973`: run `observe_candidate` (`:978`) and finish through `finish_candidate_at_revision` (`:979-988`) only when the block is not active and the revision or parent changed; an active block continues and lands at the observed revision (`:1039-1046`); #289 owns old-epoch candidates | cache miss: return `Ok(None)`, as the revision check at `srv/src/coordinator.rs:1483-1488` does | not a caller; landing keeps its own fence (`srv/src/ledger/blocks.rs:124-127`) | n/a |
| `PriorBalancesChanged` | the balances moved: the candidate is superseded, or it is the pool's own block, already landed | not reached when `qbit_pool_audit_bundles` already holds the block's audit, because the claim then finishes from the landed audit without `read_window`; otherwise run `observe_candidate`: not active and changed is `finish_candidate_at_revision`; active is `retry_candidate` with an alert, the outcome today's `prior == bundle.prior_balances` failure has (`srv/src/ledger/blocks.rs:128-132`, reaching `submit_loop`'s retry at `srv/src/coordinator.rs:1123-1131`) | cache miss: `Ok(None)` | as above | n/a |
| `Incomplete` | rows pruned or missing; D6 violated, or a wrong predicate | fail the attempt through `retry_candidate` (`srv/src/ledger.rs:590`) with the error in `last_error`; never abandon automatically, #268 owns recovery | cache miss: log and return `Ok(None)`; the share is then rejected as `unknown-job` (`srv/src/stratum.rs:1239-1243`) | as above | the legacy window is not in the ledger: keep the bytes-only import |
| `SnapshotDigestMismatch`, `Decode` | corruption, a reference built from different bytes, or payload/column disagreement | same as `Incomplete`, with an alert | same as `Incomplete` | as above | same as `Incomplete` |
| `Database` (incl. 57014) | transient | propagate: `submit_loop` hands the error to `retry_candidate` (`srv/src/coordinator.rs:1123-1131`), which releases the claim and reschedules the row after `LEAST(60, attempt_count)` seconds (`srv/src/ledger.rs:594`); lease expiry recovers the row only if that write itself fails | propagate; the reconnect fails and retries | as above | propagate |
| landing equality failure after a successful `read_window` (`srv/src/ledger/audit.rs:132-138`) | the landing transaction read a different range than the claim did, which immutability forbids | like `SnapshotDigestMismatch`: `retry_candidate` with an alert, never abandon; the error already reaches `submit_loop`'s retry path (`srv/src/coordinator.rs:1123-1131`) | n/a | the check stays | n/a |
| caller deadline expired | `read_window` plus the rebuild outran the deadline in the table above | 60 s: `retry_candidate` with an alert | `outer − min(5 s, outer / 2)`: `Ok(None)`; the share is rejected as `unknown-job` | n/a | n/a |
| empty window (`shares: None`) | not an error | rebuild from `vec![bootstrap_share]` | rebuild the bootstrap bundle per miner with the builders directly, from the stored policy inputs, never `build_bundle` | lands through `inline_shares` (`srv/src/ledger/audit.rs:124-131`), unchanged | n/a |

## Revision fence and reorgs

Share rows survive a reorg: the immutability trigger below forbids UPDATE and
DELETE, so the range is always rebuildable and `Incomplete` never means "the
chain moved". What moves is the revision and, sometimes, the balances, and
neither is a supersession test on its own. `observe_chain_view` bumps
`payout_revision` on every tip with more work, the pool's own block included
(`srv/src/ledger.rs:283-285`); an ambiguous `submitblock` timeout makes that
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
  `land_candidate` (`:1052-1054`, `:1065-1069`) or a reconcile has confirmed
  it, the balances have legitimately moved: the claim skips `read_window` and
  finishes from the landed audit through `materialize_audit_row`
  (`srv/src/ledger/audit.rs:42-87`), which verifies the range, the snapshot
  digest and the body digest. `PriorBalancesChanged` on an active candidate
  with no landed audit has the outcome today's landing check has when
  `prior == bundle.prior_balances` fails (`srv/src/ledger/blocks.rs:128-132`):
  the error propagates to `submit_loop`, which logs "candidate remains
  recoverable" and calls `retry_candidate` (`srv/src/coordinator.rs:1123-1131`);
  the attempt fails with an alert and is never abandoned.
- **Resume (#273) treats any inequality as a cache miss.** Jobs are
  regenerable, so `Window.payout_revision != qbit_prism_jobs.payout_revision`
  returns `Ok(None)` as `:1483-1488` does today, and `PriorBalancesChanged`
  does the same.
- **Landing** keeps its own fences unchanged: `require_revision`, the
  `payout_revision` equality and `prior == bundle.prior_balances`
  (`srv/src/ledger/blocks.rs:91-94,120-132`), and `persist_audit_snapshot`'s
  re-read of the range under exact equality (`srv/src/ledger/audit.rs:132-138`).
  `read_window` authenticates a rebuild for the caller; it does not replace
  the landing transaction's checks.

## Immutability and retention

A `WindowRef` is valid only while its rows exist unchanged. Today that holds
because `qbit_share_ledger` rows are immutable (the statement-level trigger at
`srv/migrations/002_multi_instance.sql:127-135` raises on UPDATE, DELETE and
TRUNCATE, so no pruning exists), `share_seq` is assigned by the insert under
`ORDER_LOCK` (`srv/src/ledger.rs:316,386`) so no row can later appear inside
a written range, and the anchor predicate is stable because `accepted_at` and
`job_issued_at` never change and the ledger clock only moves forward (`:381`).

`finish_candidate_at_revision` sets `candidate=NULL` in its terminal UPDATE
(`srv/src/ledger/blocks.rs:214`); after 007 the same UPDATE also sets the six
window columns NULL, so a terminal row is "no window" and the floor below
considers non-terminal rows only.

This extends the Python-era invariant "audit bodies and share segments remain
digest-checked and reconstructable" ([Invariants](invariants.md), Audit
artifacts; the contract is [A1 audit artifacts](a1-audit-artifacts.md)) to
native rows. Proposed wording for the native server: *a share window
referenced by any live row is reconstructable: `qbit_share_ledger` rows are
immutable, and no retention job removes a row at or above the smallest
`window_first_share_seq` of any non-terminal outbox row, any unexpired job
row, or any `qbit_prism_audit_snapshots` row without `inline_shares`.*

D6 (#260) must honour that floor. A future prune runs in one transaction that
takes `ORDER_LOCK` and `SETTLEMENT_LOCK` (the locks under which references are
written at enqueue and `save_job`), computes the floor, and deletes strictly
below it. It must also keep a horizon at least as wide as the window the next
`Ledger::snapshot` could select, because a rise in network difficulty moves
the window start earlier (`srv/src/ledger.rs:407-449`). A read that began
before such a commit still sees its snapshot; one that begins after it fails
with `Incomplete` at the existence probe.

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
`configure`, `srv/src/ledger.rs:195-207`) keeps live instances on one policy,
so the drift is across time, a fingerprint reset between the refresh that
built the row and its claim, not across instances. If a claiming frontend
re-derives any signed input differently, the canonical bundle changes:
`srv/src/ledger/blocks.rs:101-105` raises "existing block audit differs from
candidate", or a non-matching body lands. #265 removes the drift by storing
every builder input except the window:

| Builder input (`crates/qbit-prism/src/lib.rs:1365-1376`) | At refresh (`srv/src/coordinator.rs`) | At claim today | With `WindowRef` | Size |
| --- | --- | --- | --- | --- |
| `shares` | `snapshot.shares` (`:747`) or the bootstrap share (`:727-745`) | `source.shares` | `read_window`, or `vec![bootstrap_share]` | the window, never stored |
| `found_block` | from the template (`:719-726`) | `source.found_block` | **stored verbatim** in the candidate JSON | 4 scalars |
| `prior_balances` | `snapshot.prior_balances` (`:759`) | `source.prior_balances` | `read_window`, digest-checked | per recipient with a balance; read, not stored |
| `payout_policy` | `config.payout_policy` (`:760`) | `source.payout_policy` | **stored verbatim** | O(1) |
| `direct_floor_sats`, `settlement_config`, `ctv_fanout_fee_policy` | `config.ctv_direct_floor`, `config.ctv_config`, `fee` (`:761-763`) | `config.*` (`:1009-1010`), the drift, and `source.ctv_fanout_fee_policy` | **stored verbatim** as one nested `ctv: Option<{ direct_floor_sats, settlement_config, fanout_fee_policy }>`, whose presence also selects the CTV builder in place of `config.ctv_enabled` (`:755`, `:1003`) | O(1) |
| `coinbase_script_sig_suffix_hex` | `Some(suffix)` (`:764`) | `claim.candidate.coinbase_suffix_hex` (`:992`) | already stored, **required after 007**: submit always writes `Some` (`:1697`); the `Option` with `serde(default)` (`srv/src/ledger.rs:45-46`) exists for pre-007 rows, so post-007 decode rejects `None`, and the `else` branch that lands the stored bundle when the suffix is absent (`:1036-1038`) is removed with the bundle | O(1) |
| `witness_merkle_leaves_hex` | `codec::witness_merkle_leaves_hex` over the template's transactions (`:749-750`) | `source.witness_merkle_leaves_hex` | **re-derived** from the stored `block_hex` by `codec::witness_merkle_leaves_from_block`, below | not stored twice |
| signing keys | `config.manifest_seed`, `config.ledger_seed` (`:751-752`) | `config.*` (`:1001-1002`) | local, the one exception: the fingerprint pins both public keys, and the keys are the signer, not a field | n/a |

`audit_commitment_leaves_hex` is a builder output, not an input. The
`bootstrap_share: Option<AcceptedShare>` that a candidate found on an empty
window carries (about 600 B) follows the same rule: its fields come from the
worker's identity and the template the issuing frontend saw, which the
claiming frontend does not have, and it sits in `shares` and
`reward_manifest.shares`, so re-deriving it is exactly the drift the invariant
forbids. Submit sets it when `context.prepared.bundle.is_none()`
(`srv/src/coordinator.rs:27`; the `None` arm at `:1343` is where the
per-worker bootstrap bundle was built). `deferred_share`
(`srv/src/ledger.rs:47-48`) is the precedent for one inline share. Decode
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
today (`srv/src/ledger.rs:41`): it grows with the block, hex doubling the
serialized size, not with the window. #265's "outbox row under 1 MB"
criterion must therefore be stated net of `block_hex`, or as "independent of
the window size".

## Compatibility

- **Rows written before 007.** Terminal outbox rows already have
  `candidate IS NULL` and are untouched. 007 refuses to apply while any
  `state='pending'` row still holds an inline candidate (`candidate ? 'bundle'`),
  naming the rows: one-way per D5, modelled on the `version < 3`
  pending-shape check at `srv/src/ledger.rs:143-144`. After 007 there
  is no compatibility decode: a pending row with a NULL `window_anchor_ms` is
  a hard claim error telling the operator to stop the pre-007 frontend.
- **Rows written before 008.** Legacy inline prepared rows stay in place.
  Resume treats a payload with `snapshot` or `bundle` keys, or a NULL
  `window_anchor_ms` on a `prepared:` row, as a cache miss (`Ok(None)`)
  because prepared work is regenerable; the rows expire under their TTL and
  `prune_expired_jobs` (`srv/src/ledger.rs:608-609`) removes them. This is
  unambiguous now: an empty window is not NULL.
- **Mixed versions.** 3.x.x does not support a pre-007/008 and a post-007/008
  frontend on one database: the old frontend would keep writing inline
  documents the new claim path rejects and would fail to decode a reference
  job on cross-frontend resume. The one-way model is the one `Ledger::connect`
  already enforces for the legacy writer lease (`:135`) and
  [prism-rust-migration](../prism-rust-migration.md) documents; #285 gates
  startup on the schema version.
- **Upgrade procedure.** A `state='pending'` outbox row is cleared only by a
  frontend claiming and landing it: the CHECK at
  `crates/qbit-prism/sql/001_share_ledger.sql:97-104` keeps `candidate`
  non-NULL until the row is terminal, and `retry_candidate`
  (`srv/src/ledger.rs:590-602`) only backs off, never abandons. So "refuse
  while pending inline rows exist" plus "apply with every frontend stopped"
  would deadlock. There are two starting points. From a **native pre-007
  3.x.x deployment**, the native `submit_loop` drains the outbox, step 1
  below. From **production 2.x.x**, the 2.x.x submitter drains it before the
  cutover, and leftovers are refused twice: by the `version < 3` gate
  (`srv/src/ledger.rs:143-144`) on the first native connect and by #285's
  006; #265's migration test covers "a 2.x.x schema with only terminal
  rows". The order, with the 2.x.x submitter doing step 1 in that case, is:
  1. With the pre-007 frontends **running**, drain the outbox. `submit_loop`
     (`srv/src/coordinator.rs:1112-1135`) polls every 100 ms and claims each
     pending row; a row whose revision or parent is superseded is finished by
     `finish_candidate_at_revision` (`:979-988`); a failed attempt is retried
     after `LEAST(60, attempt_count)` seconds (`srv/src/ledger.rs:594`). No
     switch stops candidate production while keeping `submit_loop`, and none
     is needed: a block found during the drain is a pending row like any
     other and drains the same way. A row that keeps failing is #268's escape
     hatch; the procedure never abandons it.
  2. Verify `SELECT count(*) FROM qbit_block_candidate_outbox WHERE
     state='pending'` is 0.
  3. Stop every frontend.
  4. Start one post-008 frontend. `Ledger::connect` applies migrations in one
     transaction (the base schema at `srv/src/ledger.rs:146-149`, versioned
     steps after it), so it applies 006 (#285), then 007, then 008, each
     refusal predicate running in that transaction and naming the rows it
     found. Then start the rest; #285's startup gate keeps a pre-007/008
     frontend from joining.
- **Refusal predicates compose.** 006 (#285) refuses on its own pending
  shape (#258's storage-version-2); 007 refuses on `EXISTS(SELECT 1 FROM
  qbit_block_candidate_outbox WHERE state='pending' AND candidate ? 'bundle')`.
  Each predicate runs inside its own `version < N` step of the migration
  transaction, in migration order and one-way, never unconditionally at
  startup. The pending-shape check at `srv/src/ledger.rs:143-144` is the
  model for the shape only: it sits inside `if version.unwrap_or(0) < 3`
  (`:118-164`), runs before the base schema (`:146-149`) on the first native
  connect after 2.x.x, and never runs at version 7 or above, so #265 leaves
  it alone.
- **Canonical hashes.** A bundle rebuilt from `read_window` through the
  borrowing builders is field-for-field the bundle built at refresh, so
  `canonical_audit_bundle_bytes` and `audit_bundle_sha256` are unchanged; the
  golden-bytes test (`crates/qbit-prism/tests/audit_cli.rs:353`) guards this.

## Where the code lives

Planned: `srv/src/ledger/window.rs`, owned by workstream B per #283, holding
`WindowRef`, `ShareRange`, `Window`, `WindowError`, `read_window`, and the
shared `SELECT_SHARE`, `share_from_row` and `read_range` that `ledger.rs` and
`ledger/audit.rs` define today; `audit.rs` (A) and `jobs.rs` (B) then import
them. Moving `read_range` out of `ledger/audit.rs` (`srv/src/ledger/audit.rs:144-152`)
edits an A-owned file, and `SELECT_SHARE` and `share_from_row` leave
`ledger.rs` (`srv/src/ledger.rs:734-756`). **Pending confirmation with
Anatolie on #283**; this record does not claim the location is settled.

## Bundle ownership

`AuditBundle` is unchanged. #296 (merged into 3.x.x as `f316eb9`) added to
`crates/qbit-prism/src/lib.rs`:

| Entry point | Purpose |
| --- | --- |
| `AuditBundleBody` | every `AuditBundle` field except the top-level `shares`; `#[serde(deny_unknown_fields)]`, so a full bundle cannot decode as a body |
| `build_audit_bundle_body`, `build_audit_bundle_body_with_coinbase_script_sig_suffix`, `build_audit_bundle_body_with_coinbase_options`, `build_audit_bundle_body_with_ctv_settlement_options`, each taking `&[AcceptedShare]` | borrowing builders mirroring the four `build_audit_bundle*` functions (`:1258`, `:1277`, `:1298`, `:1365`) |
| `verify_audit_parts(&AuditBundleBody, &[AcceptedShare], …)` and its `verify_audit_parts*` variants | verification without an owned bundle |
| `canonical_audit_bundle_bytes_from_parts(&AuditBundleBody, &[AcceptedShare])`, `write_canonical_audit_bundle_from_parts` | canonical bytes without splicing |
| `AuditBundleBody::into_bundle(Vec<AcceptedShare>)`, `AuditBundle::into_parts()` | lossless conversion both ways |
| `prior_balances_digest(&[CarryForwardBalance]) -> [u8; 32]` | the private `prior_balances_digest_hex` (`:2133`) made public for `WindowRef` |

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
| k concurrent rebuilds | n/a | k ≤ `build_workers` (default at most 4): k pool connections and about k × 0.5 GB peak (the window plus the rebuilt body's `reward_manifest.shares`); k = 1 per `storage_key` under the single-flight map |
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
| columns | the six on `qbit_block_candidate_outbox` | the six on `qbit_prism_jobs`; per-worker `StoredJob` rows keep them NULL |
| document | `Candidate` (`srv/src/ledger.rs:39-49`) drops `bundle: AuditBundle` for `window: WindowRef`, `bootstrap_share: Option<AcceptedShare>`, `found_block`, `payout_policy` and `ctv: Option<{ direct_floor_sats, settlement_config, fanout_fee_policy }>`; it keeps `block_hash`, `block_hex`, `job_id`, `payout_revision`, `deferred_share` and `coinbase_suffix_hex`, now required. The rebuilt `AuditBundle` lives in `CandidateClaim` (`srv/src/ledger.rs:52-55`), absent until the rebuild fills it where `:1032-1034` replaces `candidate.bundle` today; landing reads it there instead of `claim.candidate.bundle.*` (`srv/src/ledger/blocks.rs:61-153`), and `observe_candidate` reads the stored `found_block.block_height` from the candidate instead of through the bundle (`srv/src/coordinator.rs:829`) | `StoredPrepared` (`srv/src/coordinator.rs:46-55`) drops `snapshot` and `bundle` for `window: WindowRef` plus the `Snapshot` scalars the reference does not carry: `share_seq`, the newest sequence at snapshot time (`srv/src/ledger.rs:453`), which the cached-work equivalence test compares (`srv/src/coordinator.rs:612`), and `payout_revision`, which `save_job` fences (`srv/src/ledger.rs:472-479`) and resume compares (`srv/src/coordinator.rs:1485`). It stores the policy inputs `build_bundle` reads from `config` at `:760-762`: `payout_policy` and the same nested `ctv` field, which also covers the `config.ctv_enabled` choice at `:755` and absorbs today's `fee` (`:50`; `fee_policy` is `None` when CTV is off, `:319-322`). It keeps `template`, `fingerprint`, `generation`, `parent_of_tip` and `coinbase_suffix`, so a cross-frontend resume builds the bundle the issuing frontend would have built. `Prepared` (`:32-43`) keeps its in-memory `snapshot: Arc<Snapshot>` and `bundle: Option<Arc<AuditBundle>>` (`:34-35`) for the local frontend, built by borrowing (`:747` stops cloning) |
| write | submit (`srv/src/coordinator.rs:1691-1699`) builds the reference from the prepared `Snapshot` without cloning shares and sets `bootstrap_share` when `context.prepared.bundle.is_none()` (`:27`; the `None` arm at `:1343` is where the per-worker bootstrap bundle was built); persist (`srv/src/ledger.rs:695-696`) serializes and digests the small row before the append transaction opens and writes the columns with it | `refresh_once` (`srv/src/coordinator.rs:468`, the store at `:627-641`) computes the reference once per non-cached refresh; `save_job` writes the columns and a payload with no `shares` key; an empty snapshot writes an empty-window reference |
| read | claim decode (`srv/src/ledger.rs:549-555`) checks the digest and the columns | resume (`srv/src/coordinator.rs:1447-1470`) decodes the small payload inline; `:1469-1470` no longer needs `spawn_blocking` |
| fence | `process_candidate_inner` (`srv/src/coordinator.rs:966`) keeps `:972-979`; `Window.payout_revision != candidate.payout_revision` is a second hint for the same `observe_candidate` probe and never a supersession by itself ([Revision fence and reorgs](#revision-fence-and-reorgs)) | `:1483-1488` stays, then `Window.payout_revision` against the row's `payout_revision`; any inequality is `Ok(None)` |
| rebuild | under a `build_slots` permit (`:997`) and the 60 s whole-call deadline: if `qbit_pool_audit_bundles` already holds the block's audit, finish from it through `materialize_audit_row` without `read_window`; else await `read_window`, re-derive the witness leaves from `block_hex`, run `build_audit_bundle_body_*(&window.shares, …)` directly in `spawn_blocking` (`:998-1031`; never `build_bundle`, which takes a second permit at `:712`), and put `into_bundle(window.shares)` in `CandidateClaim` for landing | under one `build_slots` permit, the single-flight entry for the `storage_key` and the inner timeout: await `read_window`, rebuild `Prepared` through the borrowing builders called directly, never `build_bundle`, or use the local incremental window once #274 lands |
| empty window | build over `&[bootstrap_share]` | the single-flight entry carries `bundle: None`; each miner gets its own bootstrap bundle, built with the builders directly under a fresh `build_slots` permit taken after the entry's permit is released, from the synthetic share `build_bundle` fabricates from the worker today (`:727-745`, reached from `:1498-1510`), the stored template, anchor, `payout_policy` and `ctv`, and the returned balances and revision; never `build_bundle`, which reads `config` for signed fields (`:755`, `:760-762`) |
| import | `srv/src/ledger/migration.rs:117` stores `canonical_audit_bytes` plus non-share metadata and no inline body | n/a |
| must not | serialize a share array into the outbox; hold `ORDER_LOCK` across `read_window`; decode inline candidates on a post-007 schema; read local configuration for any stored input; wrap `read_window` in `spawn_blocking`; call `build_bundle` under a held `build_slots` permit | write a share array into `payload`; wrap `read_window` in `spawn_blocking`; run whole-window serde on a runtime thread; call `build_bundle` for any resume rebuild, empty window included (it takes a second permit and reads `config` for signed fields); change the `save_job` revision fence (`srv/src/ledger.rs:472-479`) or the cached-work reuse conditions (`srv/src/coordinator.rs:528-533`) |
| text to amend on merge | "reconstructs … through `Ledger::read_window` in `spawn_blocking`": `read_window` is awaited, only the builder runs in `spawn_blocking`; the closed field list ("stores the `WindowRef` … plus … not the bundle"): it is the [Stored bundle inputs](#stored-bundle-inputs) table, `found_block`, `payout_policy`, `ctv`, `bootstrap_share` and the required `coinbase_suffix_hex`; and "outbox row under 1 MB", to be stated net of `block_hex` | the five-field reference list: it is `anchor_ms`, `prior_balances_digest` and an optional range of four (`first_share_seq`, `last_share_seq`, `share_count`, `snapshot_sha256`) |

**#267, audit bodies.** Uses `AuditBundleBody`, `verify_audit_parts` and
`canonical_audit_bundle_bytes_from_parts` so `ledger/audit.rs` and
`api/read_models.rs` verify and serve without materializing a bundle, and
normalizes `reward_manifest.shares` out of the stored body; the
`share_snapshot_sha256` FK stays.

## Open questions

- **djh58 (#273).** Whether `payout_revision` stays a row column rather than
  a `WindowRef` field; whether legacy inline prepared rows are a cache miss
  rather than migrated; the inner resume timeout of
  `outer − min(5 s, outer / 2)` and the single-flight map; storing the
  policy inputs in `StoredPrepared`; and whether 0.7 to 1.4 s of digest CPU
  per non-cached refresh is acceptable until #274.
- **Anatolie (#283).** Confirm `ledger/window.rs` as the home of `WindowRef`,
  `read_window`, `SELECT_SHARE`, `share_from_row` and `read_range`, owned by
  B while A's #265 and B's #273 both call it. The move edits A-owned
  `ledger/audit.rs` (`read_range`) as well as `ledger.rs`; either A signs off
  on that one edit or B lands `window.rs` first and A switches `audit.rs`
  over in #265.
- **D1 (#260).** The cost section assumes 400k shares. A single share array
  has 3 to 13 % headroom under the JSONB ceiling there, and an
  `AuditBundleBody` crosses it at about 483k counted shares; a target above
  that makes #267 a P0 alongside #265 and #273.
- **D6 (#260).** Retention must never prune below the floor above, must keep
  a horizon wider than any window the next snapshot can select, must take
  `ORDER_LOCK` and `SETTLEMENT_LOCK` while it computes and deletes, and must
  narrow the immutability trigger only for that job. It may add the
  `window_first_share_seq` index.
- **#265, #285 and #287.** Whether 007's refusal should also consider live
  pre-007 instances heartbeating in `qbit_prism_instances` (#285's startup
  gate), and whether the legacy audit import registers a
  `qbit_prism_audit_snapshots` row through `read_window` when the history
  exists or stays bytes-only (#287).

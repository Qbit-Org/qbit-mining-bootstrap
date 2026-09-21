# Template and payout-window refresh

The refresh loop retains one immutable, anchored `Snapshot` and its native
`WindowRef`. A transaction or fee-policy change builds a new template,
coinbase, and compact prepared record using those same window inputs. It does
not select the accepted-share window again or recompute its native digest.

As-is window reuse is invalidated by a changed accepted-share cutoff, payout revision,
prior-balance digest, network difficulty, or the existing
`PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS` interval. The interval starts before the
snapshot read; rebuilding templates does not renew it. The accepted-cutoff
probe reads the latest accepted sequence without selecting share payloads;
it shares its exact SQL with the snapshot cutoff. Production appends serialize
under the ordering barrier, assign the acceptance timestamp from the ledger
clock, and reject future job timestamps, so a newly committed accepted row is
eligible at the next snapshot barrier. Rejected rows do not invalidate it.

Invalidating cached inputs does not by itself replace usable published work.
As before this split, accepted shares with an unchanged template wait for the
next template or economic change, or the original reanchor deadline; that
build captures the latest eligible shares. This preserves the existing
publication cadence under a busy share stream instead of adding a full read,
generation change, and miner update on every poll. The first accepted share
still immediately replaces an empty window. Revision, balance, and tip changes
continue through their existing refresh and publication checks.

Build inputs are selected after waiting for build admission, with a fresh
cutoff, payout state, and reanchor-age check. Shares committed after that
selection belong to the next window, as they did after the baseline snapshot
read. The reanchor interval triggers another read; it is not a replacement for
the existing absolute work deadline. Tip, readiness, revision, and balance
identity are still revalidated before publication.

`Prepared`, issued jobs, persisted records, and resumed jobs still contain
compact references rather than accepted-share arrays. The cache is owned only
by the serialized refresh loop. Its steady retained memory is one snapshot:
O(window shares + prior-balance recipients), including each share's owned
strings. Builders still need their existing transient counted-share and
artifact allocations. Old cache ownership is released on the blocking executor under build admission
before reading a replacement; an active blocking build retains its own admitted
inputs until cleanup ends. Keeping old issued jobs does not retain those rows.
No idle build permit is retained.

The serialized refresh loop owns cached inputs even if a later reservation is
cancelled. Build admission covers actual build cleanup, then ends before database
reservation waits so existing work can still resume. Cached inputs never confer
publication authority. Publication still uses the existing reservation and
publication authority
boundary, with fresh tip, readiness, revision, and balance checks. A failed publication may leave reusable cached inputs, but fast reuse of published
work requires its exact window reference to match the cache. Original issued balances,
references, and absolute expirations are unchanged. Native share-array hashes
and canonical audit hashes continue to use their existing serializers; the
Python-compatible `PayoutWindow` digest is not substituted for either.

## Issue #274 scope and evidence

This implements the September 10 refresh split. It removes repeated full-window
SQL reads during template churn; it does not introduce the
incremental `PayoutWindow` engine or a new hash recipe. The bounded delta
acquisition below separately reduces payload reads when reanchoring. Existing borrowing
builders still fold and hash the captured inputs for each changed template.
Those remaining costs need the existing 400k/500k harness and the agreed refresh
budget to determine whether the conditional incremental-engine work is needed.

`refresh_window_split` exercises the real Coordinator and PostgreSQL through the
existing execution proxy: three transaction changes two seconds apart return
zero additional accepted-share rows; a share on every unchanged-template poll
does not replace published work, and the next template or original reanchor
captures all latest eligible shares in one read. Revisions and difficulty
changes trigger fresh reads immediately. It also verifies as-issued resume,
compact storage and canonical audit reconstruction, rejection of a delayed
refresh after a newer tip observation, and cancellation during a completed
COMMIT response wait. Unit regressions cover same-revision
balance changes, the empty-to-nonempty transition, and release of retired rows
while old prepared work remains alive. Admission regressions hold the sole
build slot while the cutoff changes or the original interval expires. A delayed
snapshot COMMIT response checks that read latency does not renew the interval;
a share committed during reservation enters the next build without rewriting
the already-selected window.

The unchanged frozen money/window corpus and compact runtime tests remain the
compatibility gates. No sub-second one-share-delta result, production timing,
24-hour memory soak, or measured per-frontend RSS is claimed by this change.
The original incremental/differential criteria depend on the conditional engine
work. The 24-hour memory criterion remains unmeasured; bounded cache ownership
is a structural property, not a soak result.

## Bounded delta acquisition (#275)

A rebuild still captures a fresh anchor, accepted cutoff, payout revision and
prior balances under the original settlement/ordering locks. When the refresh
loop exclusively owns the retired window, it moves that window's shares into
acquisition under build admission. A shared `Arc`, including one still held by
cancelled blocking work, takes the full-read path. Hashing, reward folding,
publication authority and the as-is reuse predicate are unchanged.

The candidate reads at most one page of newly appended eligible share payloads,
then trims the moved vector newest-first using the full reader's saturating
weight subtraction. The crossing row stays. Trimming precedes appending, and a
large retirement also releases excess vector capacity. This retains one window
plus a bounded page, with decoding and destruction on the blocking executor.

Endpoint presence by itself is insufficient: two live endpoints can surround a
detached interior partition. Acquisition therefore records private evidence that
the full retained suffix was in one live PostgreSQL leaf, including that leaf's
`pg_inherits` tuple incarnation and current WAL insertion timeline. A later
advance requires the same timeline and live leaf for the retained range and new
cutoff. Detached, detach-pending, restored or replaced leaves invalidate that
proof; cross-partition windows use the full scan.

Even an unchanged leaf does not prove an unchanged eligible set. Sequence gaps
are normal after rollbacks and crash recovery, and immutable history permits an
INSERT into an unused old sequence. Future timestamps can also become eligible.
After trimming, one statement checks both the live leaf incarnation and the
number of currently eligible rows from the proposed first row through the fresh
cutoff. Within that writer history, retained rows are still immutable members of
that leaf; equal count proves there are no omitted eligible members in that range. Since this suffix
already reaches the weight, older rows cannot affect the full reader's answer.
A changed count falls back to a full scan at the same fresh anchor.

The subset proof depends on the existing immutable-history triggers. An operator
repair that disables or bypasses those triggers can change payloads without
changing the count or leaf incarnation. Such a repair requires restarting every
frontend to discard retained windows before trusting another refresh. Supported
partition restoration invalidates the acquisition evidence normally.

Both leaf checks also read the current WAL insertion timeline in the same
transaction as share acquisition. A supported asynchronous promotion can lose
acknowledged rows and replace their sequence values while preserving leaf OID,
catalog incarnation and count, including an interior row whose newest neighbor
survives. The changed timeline rejects that retained vector and takes the full
read. Ordinary pool rotation or reconnection to the same history keeps the
evidence usable. Query errors remain errors; they never authorize reuse.

This history proof uses the existing [D3 promotion and rejoin
contract](prism-ha-reference-architecture.md#promotion-fencing-and-the-stable-writer-endpoint):
one eligible standby, positive fencing of the old writer, and verified rewind or
a fresh base backup before rejoining the former primary. Its later promotion
advances the timeline again. Timeline numbers are not globally unique:
independently promoted sibling copies can choose the same number, but D3 does
not permit them as competing or replacement writers. Isolated restore and PITR
follow the existing [D5 stopped-frontend recovery
boundary](prism-rust-migration.md#recovery-and-rollback), which discards this
process-local evidence. These are existing deployment boundaries; no additional
frontend restart or privilege grant is required for D3 failover.

This check is an **O(window) metadata scan**, not O(delta) database work. It avoids
transferring and decoding retained payloads but still pays the count cost and
recomputes every existing digest. The optimization also falls back for empty or
short history, difficulty changes, decreasing cutoffs or anchors, absent
acquisition evidence, and deltas spanning more than 4096 sequence slots. Default
partitions span 16,777,216 sequence slots; a 400k window crosses a boundary for
roughly 2.4% of uniformly distributed dense positions, an arithmetic illustration
rather than a measured production eligibility rate. Sparse windows can advance;
production eligibility and performance still require representative measurement.

Real PostgreSQL differential tests compare serialized snapshots and `WindowRef`
at the same fresh anchor, including crossing rows, revision/balance changes,
real post-INSERT rollback gaps, retroactive inserts, future eligibility and
partition detach/reattach. Coordinator proxy tests distinguish actual payload
rows from metadata and retain their semantic and as-issued assertions. The
experiment's performance screen is at least 15% end-to-end improvement over the
exact base across at least three balanced 400k pairs, with exact durable delivery
identities and no worse memory bound. This screen is separate from the unchanged
one-second goal; no subsecond claim follows from a payload-read improvement.

# PRISM Rejection Reasons

PRISM rejection reason IDs are stable machine-readable strings. Operator logs,
Prometheus metrics, Stratum error data, ledger rejected-row fixtures, and future
dashboard/API surfaces should use these IDs instead of parsing human messages.

| Reason ID | Meaning |
| --- | --- |
| `stale-job` | The submitted share or block candidate was built on an obsolete qbit tip/template outside every qualified prior-tip credit policy. |
| `duplicate-share` | The same miner submitted a share with an already-seen header. |
| `low-difficulty` | The submitted share did not satisfy the miner's assigned share target. |
| `malformed-submit` | The `mining.submit` payload could not be parsed or assembled. |
| `unauthorized-worker` | The submit username did not match the authorized Stratum username/session. |
| `unknown-job` | The job ID was unknown or no longer active for that client. |
| `invalid-extranonce` | The extranonce field had an invalid shape for this coordinator. |
| `invalid-ntime-or-nonce` | `ntime` or `nonce` was not a 4-byte hex string. |
| `candidate-audit-mismatch` | The final PRISM audit bundle did not match the submitted coinbase. |
| `submitblock-rejected` | qbit `submitblock` rejected the candidate or did not advance to the submitted block. |
| `backend-rpc-unavailable` | A backend RPC dependency was unavailable while classifying a submission, including stale-grace parent lookup. |
| `internal-error` | An internal coordinator failure prevented normal classification. |
| `pool-closed` | The coordinator was no longer accepting shares. |
| `block-stale` | The block candidate height was stale against the active qbit tip. |
| `ledger-confirmation-failed` | The ledger did not confirm a block that qbit appeared to accept. |
| `transition-lease-expired` | The exact connection-bound prior job was known, but its absolute fanout-transition lease had expired. |
| `transition-lease-revoked` | The exact prior job's fanout-transition lease was revoked by successful replacement delivery. |

The coordinator exposes these IDs in:

- Stratum JSON-RPC error data as `{"reason_id": "<id>"}` when a rejection is
  classified.
- Prometheus as `qbit_prism_rejections_total{reason_id="<id>"}`.

For ready-pool refreshes, share validation follows the last qbit tip for which
the refresh path has a coherent, final-validated replacement bundle ready to
fan out.
`waitfornewblock` is a refresh trigger, not a publication event, and submit
handling does not race ahead by reading a merely detected tip while replacement
work is still being built. This is especially important when stale grace is
zero: miners keep receiving normal credit for the work the coordinator is still
advertising until the prepared tip, snapshot, and cancellation token are
published atomically. Direct issuance paths (initial delivery retries, Vardiff
retargets, reauthorization) stay pinned to the published snapshot during that
window for the same reason: work issued for a merely detected tip would have
every share rejected until publication. Block candidates independently recheck
qbit's live tip before `submitblock`, so an old-tip share accepted during
preparation is never sent as a stale block.

Stale-grace credited shares are accepted shares, not rejections. When
`PRISM_STRATUM_STALE_GRACE_SECONDS` is non-zero, a same-connection share whose
job parent is the parent of the current tip may be credited during that short
window, measured per connection from when it receives new-tip work (a share
stays creditable while the refresh pass has not reached its connection yet).
The window only opens on an observed tip flip, never at coordinator startup. PRISM never submits the old-tip header as a block; the share still has
to satisfy the assigned share target and is recorded with
`credit_policy=stale-grace`.

Fanout-transition credit is distinct from stale grace and is disabled when
`PRISM_STRATUM_FANOUT_TRANSITION_LEASE_SECONDS=0`. When enabled, publication of
a replacement tip arms an absolute lease only for the exact prior job already
delivered to that connection. Authority is bound to the connection,
authorization generation, job ID, source/target tip generations, template,
payout state, and difficulty generation. A successful replacement write
revokes it immediately; disconnect and reauthorization remove it; later tip
flips cannot renew it. The retained maps are capped by
`PRISM_STRATUM_FANOUT_TRANSITION_MAX_JOBS_PER_CONNECTION`.

An eligible proof is validated against its original job target, credited once
with `credit_policy=fanout-transition`, and written with a
`qbit.prism.fanout-transition-receipt.v1` audit record. It participates normally
in Vardiff and payout accounting but is unconditionally excluded from the block
candidate path. Expired and revoked exact jobs use their dedicated reason IDs;
unknown, cross-connection, reauthorized, and guessed job IDs remain
`unknown-job`.

Clean refreshes on the current tip are separate from stale grace. The
coordinator retains their original immutable validation contexts for
`PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS` (30 seconds by default), bounded
per client by `PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION`. No
cross-client count cap can discard another client's unexpired work. Production
also requires a positive `PRISM_STRATUM_MAX_CONNECTIONS`, making the pool-wide
same-tip bound the product of the connection and per-connection limits. A share
submitted against one of these contexts uses the job's original worker,
extranonce, template fingerprint, and share target; a later Vardiff change
cannot alter its target.
It is accepted normally with no credit policy while its parent remains the
current tip. When the tip changes, the same context immediately falls back to
the ordinary stale-grace rules and is never extended by the same-tip window.
Disconnecting a client removes all retained contexts for that connection.

The existing broad counters remain for compatibility:

- `qbit_prism_stale_shares_total`
- `qbit_prism_duplicate_shares_total`
- `qbit_prism_low_difficulty_shares_total`

## Stale Classification Tip Source

The per-share `stale-job` check in `mining.submit` compares the job's parent
hash against the tip for which the refresh path published coherent work, not
against a per-share `getbestblockhash` RPC. This removes the
submit-races-ahead-of-the-refresh failure mode: a submit-path RPC could observe
a new tip seconds before jobs refreshed, and with
`PRISM_STRATUM_STALE_GRACE_SECONDS=0` (mainnet-forced) that rejected every
in-flight share on the old tip. The observed tip is also the anchor the
stale-grace window and evicted-job classification already use, so all three
now agree.

Fail-safe bound: normally the published tip is trusted only while it is younger
than `PRISM_SUBMIT_TIP_MAX_AGE_SECONDS` (default 10). A detected but unpublished
replacement extends that authority through healthy bundle construction, bounded
by `PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS` and measured from the first
divergence (later detected tips do not renew it). If refresh or reconciliation
stalls beyond that budget, submits fall back to the live RPC read instead of
accepting shares against a frozen snapshot.

Rejections are counted, never logged per share or written to the ledger.
Diagnose reject spikes from `qbit_prism_rejections_total`, the
`qbit_prism_job_build_seconds` histogram, and the
`qbit_prism_tip_refresh_seconds` histograms. Per-share/per-job stdout logging
exists only behind `PRISM_HOT_PATH_LOG=1` for debugging and must stay off in
production. Prepared fanout passes validate tip and chain-view trust once per
pass (minting a validation token; per-client deliveries consult only
in-memory token state) plus a post-fanout re-validation, so per-client RPC
round trips never return to the delivery path.

Additional private metrics relevant to attribution and grace behavior:

- `qbit_prism_grace_credited_shares_total`
- `qbit_prism_fanout_transition_credited_shares_total`
- `qbit_prism_fanout_transition_leases`
- `qbit_prism_fanout_transition_retained_entries`
- `qbit_prism_fanout_transition_events_total{event="armed|accepted|expired|revoked_delivery|revoked_authorization|capacity_evicted"}`
- `qbit_prism_vardiff_idle_retargets_total`
- `qbit_prism_worker_submitted_shares_total{worker="<bounded-label>"}`
- `qbit_prism_worker_accepted_shares_total{worker="<bounded-label>"}`
- `qbit_prism_worker_grace_credited_shares_total{worker="<bounded-label>"}`
- `qbit_prism_worker_rejections_total{worker="<bounded-label>",reason_id="<id>"}`
- `qbit_prism_evicted_job_contexts{class="same_tip|stale_grace"}`
- `qbit_prism_evicted_job_submits_total{outcome="accepted_same_tip|credited_stale_grace"}`
- `qbit_prism_evicted_job_expirations_total{class="same_tip|stale_grace"}`
- `qbit_prism_evicted_job_capacity_evictions_total{scope="connection"}`

`PRISM_WORKER_METRICS_LIMIT` caps distinct worker labels. New workers beyond
the cap aggregate into `_other`.

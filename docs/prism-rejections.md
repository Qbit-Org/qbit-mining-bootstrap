# PRISM Rejection Reasons

PRISM rejection reason IDs are stable machine-readable strings. Operator logs,
Prometheus metrics, Stratum error data, ledger rejected-row fixtures, and future
dashboard/API surfaces should use these IDs instead of parsing human messages.

| Reason ID | Meaning |
| --- | --- |
| `stale-job` | The submitted share or block candidate was built on an obsolete qbit tip/template outside the stale-grace credit window. |
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

The coordinator exposes these IDs in:

- Stratum JSON-RPC error data as `{"reason_id": "<id>"}` when a rejection is
  classified.
- Prometheus as `qbit_prism_rejections_total{reason_id="<id>"}`.

Stale-grace credited shares are accepted shares, not rejections. When
`PRISM_STRATUM_STALE_GRACE_SECONDS` is non-zero, a same-connection share whose
job parent is the parent of the current tip may be credited during that short
window, measured per connection from when it receives new-tip work (a share
stays creditable while the refresh pass has not reached its connection yet).
The window only opens on an observed tip flip, never at coordinator startup. PRISM never submits the old-tip header as a block; the share still has
to satisfy the assigned share target and is recorded with
`credit_policy=stale-grace`.

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

Additional private metrics relevant to attribution and grace behavior:

- `qbit_prism_grace_credited_shares_total`
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

# PRISM Rejection Reasons

PRISM rejection reason IDs are stable machine-readable strings. Operator logs,
Prometheus metrics, Stratum error data, ledger rejected-row fixtures, and future
dashboard/API surfaces should use these IDs instead of parsing human messages.
This reference includes legacy IDs retained for historical records. The
[native metric inventory](prism-native-metrics.md) lists the current closed
share-rejection label set.

| Reason ID | Meaning |
| --- | --- |
| `stale-job` | The submitted share or block candidate no longer has valid job authority, including an obsolete tip/template outside stale grace or an expired/replaced publication lease refused before COMMIT. |
| `duplicate-share` | The same miner submitted a share with an already-seen header. |
| `low-difficulty` | The submitted share did not satisfy the miner's assigned share target. |
| `malformed-submit` | The `mining.submit` payload could not be parsed or assembled. |
| `unauthorized-worker` | The submit username did not match the authorized Stratum username/session. |
| `unknown-job` | The job ID was unknown or no longer active for that client. |
| `invalid-extranonce` | The extranonce field had an invalid shape for this coordinator. |
| `invalid-ntime-or-nonce` | `ntime` or `nonce` was not a 4-byte hex string. |
| `candidate-audit-mismatch` | Legacy taxonomy; no native share producer. The final PRISM audit bundle did not match the submitted coinbase. |
| `submitblock-rejected` | Legacy taxonomy; no native share producer. qbit `submitblock` rejected the candidate or did not advance to the submitted block. |
| `backend-rpc-unavailable` | A backend RPC dependency was unavailable while classifying a submission, including stale-grace parent lookup. |
| `internal-error` | An internal coordinator failure prevented normal classification. |
| `pool-closed` | The coordinator was no longer accepting shares. |
| `block-stale` | Legacy taxonomy; no native share producer. The block candidate height was stale against the active qbit tip. |
| `ledger-confirmation-failed` | The ledger did not record the share. For share-pass submissions, the commit was not sent or was rolled back. For block-only proofs, the block was not on the active chain when its candidate was abandoned, refused by the node after the offer, or settled as a proven orphan (#415); a later reorg or late landing can still credit it. |
| `ledger-outcome-unknown` | The ledger outcome was not known by the acknowledgement deadline; the share may still be credited (logged with `share_id`). |

The native coordinator exposes its produced reason IDs in:

- Stratum JSON-RPC error data as `{"reason_id": "<id>"}` when a rejection is
  classified.
- Prometheus as `qbit_prism_rejections_total{reason_id="<id>"}`.

For share telemetry, present unknown or empty IDs normalize to the bounded
`unrecognised` label. This is a metrics-only fallback, not a new protocol
response: the numeric error, message and reason metadata stay unchanged.
Explicit `internal-error` and missing (`None`) IDs retain the `internal-error`
label. The existing reason-less username-limit refusal is an authorization
event and never enters share-rejection telemetry. See the
[native inventory](prism-native-metrics.md) for the per-reason alert implications;
an `unrecognised` count indicates a real rejection with a reason outside the
known taxonomy, not a healthy or absent event.

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
`PRISM_STRATUM_STALE_GRACE_SECONDS=0` (then pinned on mainnet) that rejected every
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

## Commit-gate refusals

A share admitted under the published-work replacement lease must still hold
that original authority immediately before COMMIT. If its publication has
been replaced or its fixed lease/resume deadline expired, a completed typed
gate refusal produces the existing `stale-job` response:
numeric code **21**, message **`stale job`**. This is the same authority check
used before persistence, performed again after database waits. It changes
classification, not payout eligibility, stale grace, candidate authority or
the share acknowledgement deadline. No new credit is recorded for this refusal.

Gate closure alone does **not** prove stale work. Unavailable authority locks,
unhealthy/unknown readiness, an unavailable tip observation and a share
acknowledgement deadline that closes before COMMIT remain
`ledger-confirmation-failed` (code **20**). For example, a non-refresh probe
may observe a return to the published parent while its cached observation
has aged out: that does not prove the unchanged publication or original
lease expired, and the refused append remains a backend failure. The winning
closure cause is fixed atomically: a subsequent tip change cannot turn a
timeout or backend refusal into an expected stale race. Once COMMIT has
started, later lease changes cannot change its result: confirmed credit stays
accepted, a definite rollback stays failed, and an unresolved outcome stays
`ledger-outcome-unknown`. An identical share already durably credited retains
its duplicate outcome even if a later gate refuses.

If the append has not returned by the acknowledgement deadline, the existing
timeout/unknown handling still applies; gate state alone does not replace a
completed ledger result, including its immutable-row duplicate check.

The load harness follows the stable reason ID, including for mixed producer
versions and saved reports:

| Wire reason | Harness class before and after this change |
| --- | --- |
| `stale-job` | Expected race; excluded from `rejected_valid_shares`, never counted as accepted credit. |
| `ledger-confirmation-failed` | Backend refusal; remains in `rejected_valid_shares`, including the historical message `share was not committed because its commit gate closed`. |
| `ledger-outcome-unknown` | Backend/uncertain outcome; remains in `rejected_valid_shares` and reconciliation. |
| Missing or unrecognised gate reason | Unknown; no stale exemption from the message alone. |

Older producers used the generic gate-closed message for both stale authority
and backend causes. Neither that message nor proximity to a tip change can
retroactively distinguish them. The raw #447 matrix reports are unavailable
for this change, so no historical verdict recomputation is claimed; the
recorded verdicts and D1 rule remain unchanged. Any future comparison must
name its source reports and show the old/new policy explicitly, preserving
ambiguous failures unless independent causal evidence resolves them.

Rejections are counted, never logged per share or written to the ledger.
Diagnose reject spikes from `qbit_prism_rejections_total{reason_id}` and
`qbit_prism_share_ack_seconds{result="rejected"}`, then use the stale-job
causes below. The native runtime has no
per-share or per-job stdout logging; it does not read the retired Python
`PRISM_HOT_PATH_LOG` <!-- retired-setting: PRISM_HOT_PATH_LOG --> debugging switch. Prepared fanout passes validate tip and chain-view trust once per
pass (minting a validation token; per-client deliveries consult only
in-memory token state) plus a post-fanout re-validation, so per-client RPC
round trips never return to the delivery path.

## Stale-job causes

Every `stale-job` rejection keeps its wire reason, numeric code 21 and message.
`qbit_prism_stale_job_rejections_total{cause}` records which of the four
decisions below refused the share, once, at that decision. The share observation
counts the same rejection once as `reason_id="stale-job"`. Lease admission,
lease revalidation and commit-gate refusals use that coarse reason without a
label in this four-cause series; its sum is not a total of all stale responses.
`unknown-job` has no cause series. Stale-grace credit is an accepted
share, counted by `qbit_prism_grace_credited_shares_total`, not a cause.

The decisions run in this order, and a share that would fail several is
counted only under the first one it reaches. No later check runs just to choose
a cause, and backend-unavailable answers are never stale-job causes.

| Cause | Decision | What to check |
| --- | --- | --- |
| `resume_expired` | Stratum found the job's absolute resume lease expired after restoring it for a reconnected miner, before submitting to the coordinator. Message `stale job`. | Reconnect churn and how long miners take to resubmit old work: `qbit_prism_connections`, `qbit_prism_stratum_connection_refusals_total` and the retention settings `PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS` and `PRISM_STRATUM_STALE_GRACE_SECONDS`, whichever is longer. |
| `fee_floor` | The coordinator found the job's CTV fanout fee below the live relay floor, or the floor unavailable. Message `job CTV fee is below the current relay floor`. | Node relay policy and mempool minimum fee, then whether replacement work reached miners: `qbit_prism_authorized_missing_current_work` and `qbit_prism_stratum_current_tip_coverage_gap_seconds`. |
| `parent_grace` | The job's parent is not the published tip, and stale grace was unavailable, expired, or the job's parent is not the new tip's immediate parent. Block-only work (#478) is counted here after a tip change without any grace check, and so is a capture abandoned before its offer because its parent is no longer the tip. Message `stale job`. | Tip freshness and delivery: `qbit_prism_stratum_semantic_current_work_ratio`, `qbit_prism_authorized_missing_current_work`, `qbit_prism_stratum_current_tip_coverage_gap_seconds`, `qbit_prism_job_delivery_failures_total` and `qbit_prism_grace_credited_shares_total`. |
| `payout_revision` | The parent is current, but either the job's payout snapshot or the current published payout snapshot disagrees with the durable payout revision, and no share-lease exception applies. Since #478 a block-bearing proof from ordinary work on the active tip is captured instead and is not counted here. Counted here: block-only work, which a same-parent payout replacement retired (its plain shares, and the share of a block captured from it), and a capture abandoned before its offer by the overpay ceiling or with capture off. Message `stale job`. | Payout revision changes and replacement delivery: `qbit_prism_pending_job_builds`, `qbit_prism_job_delivery_successes_total`, `qbit_prism_job_delivery_failures_total` and the coverage series above. |

The native registry has no per-worker, evicted-job, job-build or tip-refresh
histograms; `docs/prism-native-metrics.md` is the complete inventory.

The native runtime does not read the retired Python worker-label cap
`PRISM_WORKER_METRICS_LIMIT` <!-- retired-setting: PRISM_WORKER_METRICS_LIMIT -->.

# PRISM Rejection Reasons

PRISM rejection reason IDs are stable machine-readable strings. Operator logs,
Prometheus metrics, Stratum error data, ledger rejected-row fixtures, and future
dashboard/API surfaces should use these IDs instead of parsing human messages.

| Reason ID | Meaning |
| --- | --- |
| `stale-job` | The submitted share or block candidate was built on an obsolete qbit tip/template. |
| `duplicate-share` | The same miner submitted a share with an already-seen header. |
| `low-difficulty` | The submitted share did not satisfy the miner's assigned share target. |
| `malformed-submit` | The `mining.submit` payload could not be parsed or assembled. |
| `unauthorized-worker` | The submit username did not match the authorized Stratum username/session. |
| `unknown-job` | The job ID was unknown or no longer active for that client. |
| `invalid-extranonce` | The extranonce field had an invalid shape for this coordinator. |
| `invalid-ntime-or-nonce` | `ntime` or `nonce` was not a 4-byte hex string. |
| `candidate-audit-mismatch` | The final PRISM audit bundle did not match the submitted coinbase. |
| `submitblock-rejected` | qbit `submitblock` rejected the candidate or did not advance to the submitted block. |
| `backend-rpc-unavailable` | A backend RPC dependency was unavailable while classifying a submission. |
| `internal-error` | An internal coordinator failure prevented normal classification. |
| `pool-closed` | The coordinator was no longer accepting shares. |
| `block-stale` | The block candidate height was stale against the active qbit tip. |
| `ledger-confirmation-failed` | The ledger did not confirm a block that qbit appeared to accept. |

The coordinator exposes these IDs in:

- Stratum JSON-RPC error data as `{"reason_id": "<id>"}` when a rejection is
  classified.
- Prometheus as `qbit_prism_rejections_total{reason_id="<id>"}`.

The existing broad counters remain for compatibility:

- `qbit_prism_stale_shares_total`
- `qbit_prism_duplicate_shares_total`
- `qbit_prism_low_difficulty_shares_total`

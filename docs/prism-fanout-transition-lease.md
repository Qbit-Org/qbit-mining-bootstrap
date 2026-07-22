# PRISM fanout transition submit lease

## Scope

PRISM publishes a coherent current-tip snapshot before it starts the socket
fanout for that snapshot. Publication must remain the global authority boundary,
but a connection whose replacement socket write has not completed still has
only the prior job. The fanout transition lease closes that gap without making
historical work generally acceptable and without changing stale-grace policy.

The lease is a separate, opt-in accounting policy. A zero lease duration is the
safe default and preserves the current behavior on every deployment.

## Security invariants

1. **Completed delivery is the only source of authority.** A lease can contain
   only a job context recorded after `mining.notify` (and its paired difficulty,
   when applicable) completed successfully. Registering a job before a socket
   write is not delivery proof.
2. **Authority is connection-local and exact.** Eligibility binds the immutable
   job context to connection ID, authorization generation, job ID, source tip,
   source tip generation, template generation, payout generation, and difficulty
   generation. A reconnect, reauthorization, cross-connection job ID, reused job
   ID, or guessed job ID cannot acquire the lease.
3. **The lifetime never slides.** The absolute deadline starts when the first
   replacement tip supersedes the delivered source generation. B->C->D churn,
   failed fanout retries, same-tip refreshes, and repeated submits do not renew
   it.
4. **Retained state is bounded twice.** The duration is bounded by
   `PRISM_STRATUM_FANOUT_TRANSITION_LEASE_SECONDS`; retained delivered jobs and
   lease/tombstone entries are bounded per connection by
   `PRISM_STRATUM_FANOUT_TRANSITION_MAX_JOBS_PER_CONNECTION`. Production already
   requires a positive global connection limit, so the pool-wide bound is the
   product of both limits.
5. **Replacement delivery is the revocation boundary.** The old lease remains
   usable while the replacement socket write is blocked. After that write
   succeeds, the connection atomically records the new delivered authority and
   revokes the old lease. A submit that already captured an eligible lease under
   the coordinator lock keeps that point-in-time decision, just as an ordinary
   share remains valid if the chain advances during hashing or persistence.
6. **Rapid churn does not accumulate generations.** If B was delivered and C
   and D are published before any replacement reaches the connection, the
   original B lease remains the only authority and retains its B->C deadline.
   Undelivered C work is never leased. Once a current replacement is delivered,
   B is revoked; a later transition can lease only the newly delivered context.
7. **Disconnect is terminal.** Disconnect removes delivered-authority,
   lease, and tombstone state. A new connection ID cannot reuse any of it.
8. **Transition work is never a block candidate.** Even when its header meets
   the network target, work accepted through this lease is share-validated and
   credited only. It cannot enter the synchronous or asynchronous block path,
   create a candidate outbox intent, call `submitblock`, or mutate accepted-block
   payout state.
9. **Existing proof checks remain authoritative.** The original immutable job
   context supplies extranonce, version mask, share target, worker identity, and
   difficulty. Header duplicate reservation happens before persistence, and a
   failed append releases that reservation exactly as it does for ordinary
   shares.

## Accounting and audit invariants

1. An eligible proof must still meet the exact job's assigned share target.
2. It is appended once to the ordinary durable accepted-share ledger and counts
   once in Vardiff, worker accounting, the reward window, and payout weight.
3. It carries the distinct `credit_policy=fanout-transition`; it is never
   recorded as normal work or `stale-grace` work.
4. Every credited transition share carries a
   `qbit.prism.fanout-transition-receipt.v1` receipt. The receipt records the
   connection and authorization generations, job ID, source/target/classified
   tips and generations, template/payout/difficulty generations, lease start,
   duration, age at classification, and absolute expiry. The audit verifier
   rejects a transition policy without a matching, internally consistent
   receipt (or a receipt on any other policy).
5. Audit bundles containing a transition receipt use a new explicit bundle
   schema. The counted-share digest commits to the receipt, so changing why the
   share was eligible invalidates the ledger-window attestation.
6. Exact expired and post-delivery attempts retain bounded tombstones long
   enough to return distinct canonical rejection reasons. Arbitrary job IDs
   remain `unknown-job`, avoiding an authority-discovery oracle.
7. Enabling the lease requires the matching ledger schema and audit verifier to
   be deployed first. Changing the environment alone must fail closed against
   an older database constraint rather than silently store an unclassified row.

## Designs considered

### Global bounded dual generation

The coordinator could retain the immediately previous global job generation for
a short time and accept any connection/job membership that still points at it.
This requires relatively little delivery-path state. It is not selected because
the authority is broader than the failure: fast clients receive unnecessary
prior authority, pool-wide capacity eviction can discard a slow client's work,
and B->C->D either drops a still-blocked B client immediately or accumulates
global generations. A global window also cannot prove that a specific socket
ever received the retained job without adding per-client delivery state anyway.

### Per-client delivered-authority lease (selected)

The coordinator records a small ordered set only after successful socket writes.
At a tip flip it arms leases from the prior tip's delivered set, preserving an
existing lease instead of extending it during churn. Submission classification
accepts a lease only through its owning `ClientState` and immutable context.
Successful replacement delivery revokes the set; expiry and disconnect clean it
up independently of fanout completion.

This design puts the bound and authority at the same scope as the incident,
does not make the global job graveyard authoritative, and composes with the
submission classification/work-routing boundaries extracted in PR #77.

## Configuration and rollout contract

Source and Compose defaults remain disabled:

```dotenv
PRISM_STRATUM_FANOUT_TRANSITION_LEASE_SECONDS=0
PRISM_STRATUM_FANOUT_TRANSITION_MAX_JOBS_PER_CONNECTION=1
```

After the ledger schema, coordinator, audit builder, and verifier have all been
deployed and qualified, a later `qbit-tools` mainnet rollout should render:

```dotenv
PRISM_STRATUM_STALE_GRACE_SECONDS=0
PRISM_STRATUM_FANOUT_TRANSITION_LEASE_SECONDS=120
PRISM_STRATUM_FANOUT_TRANSITION_MAX_JOBS_PER_CONNECTION=1
```

The 120-second absolute bound covers the observed 13-83 second fanout gaps with
operational margin and matches the existing default template-refresh failure
budget. The one-job cap protects only the most recently delivered mining job,
which is the job a conforming miner should be working, while keeping retained
memory and prior-work authority minimal. These values are documented here only;
this change does not edit any `qbit-tools` rollout configuration.

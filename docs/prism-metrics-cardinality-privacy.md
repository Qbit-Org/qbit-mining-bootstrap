# Native metrics cardinality and privacy acceptance (#278)

The run-role wire contract is tested against real `/metrics` responses, with an
independent expected set in
`crates/qbit-prism-server/tests/observability/contract.rs`. Expectations are not
generated from registry descriptors or the inventory document. The census
compares every family name and type, requires one HELP and TYPE per family, and
compares every sample name and complete label tuple. Duplicate samples fail.

The current bound is **51 families and 268 series**, including histogram
`_bucket`, `_sum`, `_count` and `le="+Inf"` series. Startup has **169 series**:
first-offer and advisory-lock metadata exist, but their 98 derived series remain
absent until observations occur, and the hashrate rollup lag declares metadata
without a sample until its loop starts, which it never does while
`PRISM_HASHRATE_ROLLUP_ENABLED=0`. The three node gauges are registered unknown
at startup and carry no labels. All 14 rejection reasons, two ACK outcomes,
two pool outcomes, six lock/outcome pairs, two collectors, ten task kinds, two
connection refusal reasons and four stale-job causes are covered. Bucket bounds
are independently pinned, including ACK's 15/20-second buckets and CTV chunk
rows' one-row bucket.

`every_http_family_and_closed_label_tuple_stays_bounded_under_varied_inputs`
drives every public enum variant and repeats varied values, missing and arbitrary
reasons, successful zero observations, failed collections and cancelled
collections. Neither observation values nor failure states may introduce names
or labels. Unknown remains -1, distinct from a fresh successful zero; the node
gauges are driven through every combination a readiness attempt can report,
including the answered zero peer count. This is a
run-role qualification; public-api family and label contracts are unchanged.

Privacy checks cover two producer-to-render paths:

- `stratum_hashes_heights_jobs_and_miners_never_enter_metrics_exposition` seeds
  synthetic parent hashes, coinbase heights, job IDs and miner names through the
  production TCP listener and codec. It verifies issued work contains the input,
  accepted submissions reach the fixture backend, and identifier-bearing backend
  errors reach the unchanged protocol response. Metrics must show the accepted
  and rejected events while exposing none of those identifiers or proof hashes.
- `candidate_identifiers_and_heights_never_enter_collector_http_metrics` inserts
  synthetic hashes, heights, job IDs and miner IDs into the real migrated outbox
  in a disposable PostgreSQL schema. The real collector and HTTP renderer must
  expose aggregate counts/age only, including after a failed collection. It
  verifies the stored input and resulting count so an unused fixture cannot pass.

Separately, `census_and_privacy_hold_through_unavailable_fresh_and_stale_http_snapshots`
checks the complete census, cache headers and unknown age semantics through the
real API router in all three freshness states. Its seeded health payload checks
separation between health and metrics publication: `/metrics` does not read
`state.health`. That absence is not evidence of a third producer privacy path;
the Stratum and candidate-collector tests provide that evidence.

Heights are forbidden in metadata, names and labels. Numeric samples are compared
across deliberately different source heights: a series that follows both input
heights fails. A legitimate count or elapsed value coincidentally equal to one
height is not treated as disclosure. Negative controls demonstrate rejection of
new families, extra labels, changed types, missing enum/bucket series, duplicate
samples, textual identifiers and numeric height attribution.

## Issue acceptance mapping

| #278 criterion | Native evidence and disposition |
| --- | --- |
| Registry, HELP/TYPE and movement | Delivered foundation; existing `observability/registry.rs` movement/histogram tests and `observability/inventory.rs` role scrapes remain in place. The new census pins the complete run-role wire set. |
| Closed rejection enum and bounded name/label census | Covered by `observability/cardinality.rs`, with actual HTTP rendering, every enum variant, repeated input variation and negative controls. |
| No hash/height disclosure in metrics | Covered by `observability/privacy.rs` and `observability/database_privacy.rs`; the latter requires disposable PostgreSQL and runs in the existing required `observability_database -- --ignored` CI suite. |
| Blocked critical work degrades native health | Existing `blocked_critical_poll_degrades_real_http_while_another_worker_remains_healthy` blocks a tracked poll for three seconds and verifies live HTTP health/metrics failure and recovery. |
| Cached OK cannot mask failure | Existing `expired_publication_fails_closed_through_the_router_and_recovers` expires cached health/metrics and checks HTTP freshness and recovery; the blocked-poll test covers failure overriding a still-fresh cached OK. |
| Wall-clock changes cannot change native freshness | Existing `wall_clock_fields_do_not_change_monotonic_health_or_mask_a_base_failure` and `age_uses_monotonic_time_and_exceeds_the_budget_strictly` cover wall fields and the monotonic stale boundary. |
| Exact 2.x `/healthz` shape | **Intentionally not identical; do not mark exact parity fulfilled.** `known_legacy_health_fields_follow_pinned_types_without_inventing_missing_values` pins only supported aliases using `health_2x_known_fields.json`. `ready_miner_count` was accepted-share participants, and `max_blocks` was the legacy pool-close cap; neither has a native equivalent. Native backend/health contracts retain their documented differences. |

The issue's combined legacy health checkbox must be split when recording
acceptance: the native failure/freshness criteria have evidence; the obsolete
exact-shape clause is an intentional difference. This qualification does not add
per-worker families, change readiness, alter acquisition deadlines, or expand
the separate SQL acquisition coverage work. The node and rollup-lag families
added by the #278 hardening remainder are label-less and carry no per-worker,
thread, file-descriptor or alert-rule additions; they are documented in
[the native inventory](prism-native-metrics.md).

## Running the focused checks

```sh
CARGO_BUILD_JOBS=2 cargo test -p qbit-prism-server --test observability
CARGO_BUILD_JOBS=2 cargo test -p qbit-prism-server --test stratum_protocol metrics_privacy
CARGO_BUILD_JOBS=2 cargo test -p qbit-prism-server --lib api::metrics_snapshot::tests
# Use only a disposable PostgreSQL database configured through the test gate:
CARGO_BUILD_JOBS=2 cargo test -p qbit-prism-server --test observability_database -- --ignored
```

See [the integration test gate](prism-integration-test-gate.md) for database
configuration. Run the existing native health and snapshot tests when verifying
the health acceptance mapping; none requires exact legacy response equality.

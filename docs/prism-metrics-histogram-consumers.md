# Reading native event histograms

Event histogram values are elapsed **seconds** supplied by the event producer.
The registry converts `Duration` to seconds and records it once per hook call;
it does not infer a wait from a configured timeout. The producer owns the timing
boundary, outcome, and whether cancellation produces an observation. Pool
acquisition and advisory-lock waits are separate measurements, not the complete
share acknowledgement or database operation duration.

The shared finite bucket upper bounds are 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
2.5, 5, 10, and 30 seconds, followed by `+Inf`. Buckets are cumulative and include
their upper bound: an observation of exactly 5 seconds increments `le="5"` and
every larger bucket; an observation greater than 5 and at most 10 seconds first
appears in `le="10"`. Exactly 15 seconds first appears in `le="30"`. A nominal
timeout does not determine a bucket: use the measured elapsed time, which can
include scheduling delay.

For waits below 10 ms, use `_sum / _count` to calculate an **average**, not a
percentile. The first bucket groups all observations at or below 10 ms, so it
cannot resolve a distribution within that interval. A histogram quantile there
is an estimate from the bucket boundaries, not a measured sub-10-ms percentile.

For example, this five-minute average advisory-lock wait preserves lock kind and
outcome while combining instances:

```promql
sum by (lock, result) (
  rate(qbit_prism_database_advisory_lock_wait_seconds_sum[5m])
)
/
sum by (lock, result) (
  rate(qbit_prism_database_advisory_lock_wait_seconds_count[5m])
)
```

The result is seconds per recorded observation; multiply by 1000 for
milliseconds. A zero observation count means no average is available, not that
the average wait was zero. Keep missing series and a zero denominator distinct
from a measured zero-duration wait. Inspect success and failure outcomes
separately when the question depends on whether acquisition completed.

Closed event-label observations and increments allocate no heap memory after
instance and platform mutex initialization, including the first observation of a
previously hidden histogram. Construction, rendering, platform mutex
initialization, and arbitrary owned labels are outside this guarantee. The test
performs a setup render to initialize the mutex without recording any events.
The scoped allocation-count regression test also covers concurrent recording;
it does not measure throughput or establish a throughput improvement.

```sh
cargo test --locked --release -p qbit-prism-server --test metrics_allocation
```

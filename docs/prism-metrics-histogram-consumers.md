# Reading native event histograms

Event histogram values are elapsed **seconds** supplied by the event producer,
except `qbit_prism_ctv_fanout_broadcaster_chunk_rows`, which counts rows (see
[Reading the CTV chunk-row histogram](#reading-the-ctv-chunk-row-histogram)).
The registry converts `Duration` to seconds and records it once per hook call;
it does not infer a wait from a configured timeout. The producer owns the timing
boundary, outcome, and whether cancellation produces an observation. Pool
acquisition and advisory-lock waits are separate measurements, not the complete
share acknowledgement or database operation duration.

The default finite bucket upper bounds are 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
2.5, 5, 10, and 30 seconds, followed by `+Inf`. Only
`qbit_prism_share_ack_seconds` adds 15 and 20 seconds, and only the chunk-row
histogram replaces the ladder. Buckets are cumulative and include
their upper bound: an observation of exactly 5 seconds increments `le="5"` and
every larger bucket; an observation greater than 5 and at most 10 seconds first
appears in `le="10"`. An ACK of exactly 15 seconds first appears in `le="15"`,
just over 15 seconds in `le="20"`, exactly 20 seconds in `le="20"`, and just over
20 seconds in `le="30"`. First-offer, pool-acquisition and advisory-lock
histograms retain their original ladder: exactly 15 seconds first appears in
`le="30"`. A nominal
timeout does not determine a bucket: use the measured elapsed time, which can
include scheduling delay.

## Share ACK deadlines, cost and consumer compatibility

The 15-second bound matches the default `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS`;
20 seconds matches that default plus the five-second reconciliation grace for
an ordinary share whose COMMIT was already in flight. Configuration can move
the commit deadline, and candidate-bearing appends have separate rules. These
fixed buckets do not adapt to either. ACK timing uses Tokio's monotonic clock
from complete submit-frame receipt through the successful response write,
including validation and response overhead; it does not measure only the ledger
commit. A bucket crossing alone cannot identify a commit failure or an unknown
ledger outcome. Inspect `result` and the rejection/late-confirmation counters
alongside latency; failed response writes produce no ACK observation.

For a fully populated scrape, there are 13 histogram label combinations: two
ACK results, one first offer, two pool outcomes, six lock/outcome pairs and two
accepted-block work publication results.
Adding these bounds to the shared ladder would add 22 series. Restricting them
to ACKs adds **four series per process** (two bounds × two results), including
at startup: ACK exposition grows from 28 to 32 series. All existing buckets,
`+Inf`, `_sum`, `_count`, family names and labels remain. Together with the
single new `rejections_total{reason_id="unrecognised"}` series, the #278 change
adds **five series per process**; scrape labels multiply the cost across targets.

Classic [`histogram_quantile`](https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile)
still interpolates within buckets. The old
(10, 30] interval becomes (10, 15], (15, 20] and (20, 30], reducing its maximum
width from 20 to 10 seconds. Quantile values and decisions at some thresholds
change: if all observations are 14 seconds, p99 estimates change from 29.8 to
14.95 seconds, which changes a comparison against 15 seconds. This is still an
estimate, not the observed p99. The checked-in `PrismShareAckP99High` expression
combines both results per instance and uses the existing one-second bound;
the threshold and expression stay as written, but upper-tail estimates change.
Other histogram families keep their previous interpolation.

For exact counts relative to an elapsed-time boundary, subtract cumulative
buckets: `le="20" - le="15"` counts ACKs in (15, 20], and `_count - le="20"`
counts ACKs strictly over 20 seconds (use matching labels and rate windows).
Keep accepted and rejected outcomes separate when interpreting those counts.
Do not treat missing new buckets on an old instance as zero. During mixed-version
aggregation, use only the common ladder (exclude `le="15"` and `le="20"`) or
separate versions; use the new bounds only after every contributing instance
has supplied them throughout the query's rate window. This also applies to the
first rate window following a restart into the new version.

The registry's family selector is shared by observation and rendering. Consumers
of `metrics::BUCKETS`, including live pool-snapshot tests and pool-alert fixtures,
continue to use the default ladder. The rendered event fixture pins all existing
samples plus the five additive series; HTTP inventory tests retain all 46
coordinator and 14 public families. See the [native inventory](prism-native-metrics.md)
for reason-label compatibility.

## Reading the CTV chunk-row histogram

`qbit_prism_ctv_fanout_broadcaster_chunk_rows` keeps its 2.x.x name and type
but counts **rows**, not seconds. A native broadcaster chunk is one claimed
fanout, so every observation is exactly 1 and the ladder is the single finite
bound `le="1"` followed by `+Inf`: `_count` is the number of claimed attempts
and `_sum` equals `_count`. Read attempt throughput from `rate(..._count[5m])`;
`histogram_quantile` over this family is an interpolation artifact, not a
measurement. The 2.x.x ladder (1, 2, 5, 10, 25, 50, 100) is not exported, so a
query for `le="10"` matches no native series, and a mixed-version aggregation
must use only `le="1"`, `+Inf`, `_sum` and `_count`. The companion
`qbit_prism_ctv_fanout_broadcaster_chunk_seconds` histogram uses the default
time ladder and records one observation per attempt, including an attempt
whose completion persistence failed.

## Reading small waits

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

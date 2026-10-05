//! A latency histogram that merges exactly across client machines.
//!
//! A percentile cannot be merged; a histogram can. Each process keeps every
//! sample in a log-linear bucket of fixed relative width, so two processes'
//! histograms add bucket by bucket into exactly the histogram one process
//! would have kept for both sample sets, and a merged percentile is as good
//! as a single process's.

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Significant bits kept per sample. Below `2^SIGNIFICANT_BITS` microseconds
/// every value is its own bucket; above, a bucket is under `2^(1 -
/// SIGNIFICANT_BITS)` of its lower bound wide, 0.2% at 10 bits.
pub const SIGNIFICANT_BITS: u32 = 10;

/// The largest relative width of a bucket, stated beside every summary.
pub const RELATIVE_PRECISION: f64 = 1.0 / (1u64 << (SIGNIFICANT_BITS - 1)) as f64;

/// Non-negative durations in whole microseconds, bucketed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Wire", into = "Wire")]
pub struct LogHistogram {
    buckets: BTreeMap<u64, u64>,
    count: u64,
    sum_micros: u64,
    min_micros: Option<u64>,
    max_micros: Option<u64>,
}

/// The lower bound of the bucket `micros` falls in.
pub fn bucket_lower_bound(micros: u64) -> u64 {
    if micros < (1 << SIGNIFICANT_BITS) {
        return micros;
    }
    let shift = (u64::BITS - micros.leading_zeros()) - SIGNIFICANT_BITS;
    (micros >> shift) << shift
}

/// The highest value in the bucket whose lower bound is `lower`.
pub fn bucket_upper_bound(lower: u64) -> u64 {
    if lower < (1 << SIGNIFICANT_BITS) {
        return lower;
    }
    let shift = (u64::BITS - lower.leading_zeros()) - SIGNIFICANT_BITS;
    lower.saturating_add((1u64 << shift) - 1)
}

impl LogHistogram {
    pub fn record_micros(&mut self, micros: u64) {
        *self.buckets.entry(bucket_lower_bound(micros)).or_insert(0) += 1;
        self.count += 1;
        self.sum_micros = self.sum_micros.saturating_add(micros);
        self.min_micros = Some(self.min_micros.map_or(micros, |min| min.min(micros)));
        self.max_micros = Some(self.max_micros.map_or(micros, |max| max.max(micros)));
    }

    /// Record a duration in milliseconds, rounded to the microsecond. A
    /// negative or non-finite value is a caller's bug and is clamped to 0.
    pub fn record_millis(&mut self, millis: f64) {
        let micros = if millis.is_finite() && millis > 0.0 {
            (millis * 1000.0).round().min(u64::MAX as f64) as u64
        } else {
            0
        };
        self.record_micros(micros);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn merge(&mut self, other: &Self) {
        for (lower, count) in &other.buckets {
            *self.buckets.entry(*lower).or_insert(0) += count;
        }
        self.count += other.count;
        self.sum_micros = self.sum_micros.saturating_add(other.sum_micros);
        self.min_micros = match (self.min_micros, other.min_micros) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_micros = match (self.max_micros, other.max_micros) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }

    /// The nearest-rank `q` quantile, in microseconds: the upper bound of the
    /// bucket that holds it, so never below the true value and at most one
    /// bucket width above it, and never outside the observed extremes.
    pub fn quantile_micros(&self, q: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let rank = ((q * self.count as f64).ceil() as u64).clamp(1, self.count);
        let mut seen = 0u64;
        for (lower, count) in &self.buckets {
            seen += count;
            if seen >= rank {
                let value = bucket_upper_bound(*lower);
                return Some(value.clamp(self.min_micros?, self.max_micros?));
            }
        }
        self.max_micros
    }

    /// Percentiles in milliseconds, with the unit and the clock they are on.
    /// No sample is `null` with a reason, never a zero (EP-OBSERVABILITY).
    pub fn summary(&self, clock: &str) -> Value {
        let millis = |micros: Option<u64>| micros.map(|value| value as f64 / 1000.0);
        if self.count == 0 {
            return json!({
                "unit": "milliseconds",
                "clock": clock,
                "samples": 0,
                "min": null, "p50": null, "p90": null, "p99": null, "p999": null,
                "max": null, "mean": null,
                "unavailable_reason": "no samples were recorded",
            });
        }
        json!({
            "unit": "milliseconds",
            "clock": clock,
            "samples": self.count,
            "min": millis(self.min_micros),
            "p50": millis(self.quantile_micros(0.50)),
            "p90": millis(self.quantile_micros(0.90)),
            "p99": millis(self.quantile_micros(0.99)),
            "p999": millis(self.quantile_micros(0.999)),
            "max": millis(self.max_micros),
            "mean": self.sum_micros as f64 / self.count as f64 / 1000.0,
            "percentile_precision": format!(
                "nearest rank, reported as the upper bound of its bucket: at most {:.1}% above \
                 the sample; min and max are exact",
                RELATIVE_PRECISION * 100.0
            ),
            "unavailable_reason": null,
        })
    }
}

/// The serialized form: the precision it was taken at, so a histogram from
/// another precision is refused rather than merged into wrong buckets.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    unit: String,
    significant_bits: u32,
    count: u64,
    sum: u64,
    min: Option<u64>,
    max: Option<u64>,
    /// `[bucket lower bound, count]`, ascending.
    buckets: Vec<(u64, u64)>,
}

impl From<LogHistogram> for Wire {
    fn from(histogram: LogHistogram) -> Self {
        Self {
            unit: "microseconds".into(),
            significant_bits: SIGNIFICANT_BITS,
            count: histogram.count,
            sum: histogram.sum_micros,
            min: histogram.min_micros,
            max: histogram.max_micros,
            buckets: histogram.buckets.into_iter().collect(),
        }
    }
}

impl TryFrom<Wire> for LogHistogram {
    type Error = anyhow::Error;

    fn try_from(wire: Wire) -> Result<Self> {
        ensure!(
            wire.unit == "microseconds",
            "a histogram in {:?} cannot be merged with one in microseconds",
            wire.unit
        );
        ensure!(
            wire.significant_bits == SIGNIFICANT_BITS,
            "a histogram kept at {} significant bits cannot be merged with one at {}",
            wire.significant_bits,
            SIGNIFICANT_BITS
        );
        let mut buckets = BTreeMap::new();
        for (lower, count) in wire.buckets {
            ensure!(
                bucket_lower_bound(lower) == lower,
                "{lower} is not a bucket lower bound at {SIGNIFICANT_BITS} significant bits"
            );
            *buckets.entry(lower).or_insert(0) += count;
        }
        let counted: u64 = buckets.values().sum();
        ensure!(
            counted == wire.count,
            "the histogram's buckets hold {counted} samples, but it says {}",
            wire.count
        );
        ensure!(
            (wire.count == 0) == (wire.min.is_none() && wire.max.is_none()),
            "a histogram's extremes are present exactly when it has samples"
        );
        Ok(Self {
            buckets,
            count: wire.count,
            sum_micros: wire.sum,
            min_micros: wire.min,
            max_micros: wire.max,
        })
    }
}

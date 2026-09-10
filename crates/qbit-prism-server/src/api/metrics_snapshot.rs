//! Scrape-time freshness for the last complete metrics publication.
use super::{env_num, HeaderValue, IntoResponse, Response};
use std::time::{Duration, Instant};

pub(super) fn health_stale_after() -> Duration {
    Duration::from_secs(
        env_num("PRISM_HEALTH_REFRESH_SECONDS", 2)
            .saturating_mul(3)
            .max(15),
    )
}

#[derive(Clone, Default)]
pub(super) struct MetricsSnapshot {
    body: String,
    published_at: Option<Instant>,
}

impl MetricsSnapshot {
    pub(super) fn published(body: String) -> Self {
        Self {
            body,
            published_at: Some(Instant::now()),
        }
    }

    pub(super) fn response(self, now: Instant, stale_after: Duration) -> Response {
        let freshness = Freshness::new(
            self.published_at
                .map(|at| now.saturating_duration_since(at).as_secs_f64()),
            stale_after.as_secs_f64(),
        );
        let mut body = if freshness.stale() {
            // Rewrite only the health sample, never another family's metadata or value.
            self.body
                .lines()
                .map(|line| {
                    if line.split_whitespace().next() == Some("qbit_prism_health_state") {
                        "qbit_prism_health_state 0"
                    } else {
                        line
                    }
                })
                .fold(String::new(), |mut body, line| {
                    body.push_str(line);
                    body.push('\n');
                    body
                })
        } else {
            self.body
        };
        for (name, help, value) in [
            (
                "available",
                "Whether a complete metrics snapshot has been published.",
                f64::from(u8::from(freshness.age_seconds.is_some())),
            ),
            (
                "stale",
                "Whether the metrics snapshot is missing or exceeds the health freshness budget.",
                f64::from(u8::from(freshness.stale())),
            ),
            (
                "age_seconds",
                "Monotonic age of the metrics snapshot, or -1 before the first publication.",
                freshness.age_seconds.unwrap_or(-1.),
            ),
        ] {
            body.push_str(&format!(
                "# HELP qbit_prism_metrics_snapshot_{name} {help}\n# TYPE qbit_prism_metrics_snapshot_{name} gauge\nqbit_prism_metrics_snapshot_{name} {value}\n"
            ));
        }
        freshness.response(body)
    }
}

/// Body and headers must describe the same observation. A fresh failed probe
/// is still fresh; readiness is reported separately by its existing metrics.
pub(super) struct Freshness {
    age_seconds: Option<f64>,
    stale_after_seconds: f64,
}

impl Freshness {
    pub(super) fn new(age_seconds: Option<f64>, stale_after_seconds: f64) -> Self {
        Self {
            age_seconds,
            stale_after_seconds,
        }
    }

    fn stale(&self) -> bool {
        self.age_seconds
            .is_none_or(|age| age > self.stale_after_seconds)
    }

    pub(super) fn response(&self, body: String) -> Response {
        let state = if self.age_seconds.is_none() {
            "unavailable"
        } else if self.stale() {
            "stale"
        } else {
            "fresh"
        };
        let mut response = ([("content-type", "text/plain; version=0.0.4")], body).into_response();
        let headers = response.headers_mut();
        headers.insert("cache-control", HeaderValue::from_static("no-store"));
        headers.insert("x-prism-metrics-state", HeaderValue::from_static(state));
        if let Some(age) = self.age_seconds {
            headers.insert("age", HeaderValue::from(age as u64));
        }
        if state == "stale" {
            headers.insert(
                "warning",
                HeaderValue::from_static(
                    "110 qbit-prism \"metrics snapshot is stale; serving last complete payload\"",
                ),
            );
        }
        response
    }
}

#[cfg(test)]
mod tests;

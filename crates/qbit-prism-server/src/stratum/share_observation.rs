//! A submit event begins at complete-frame receipt and ends only at a successful write.
use super::StratumError;
use crate::metrics::{AckResult, Metrics, RejectReason};
use serde_json::Value;
use tokio::time::Instant;

pub(super) struct ShareObservation<'a> {
    metrics: &'a Metrics,
    received_at: Instant,
}
impl<'a> ShareObservation<'a> {
    pub(super) fn begin(
        metrics: &'a Metrics,
        request: &Value,
        received_at: Instant,
    ) -> Option<Self> {
        (request.get("method").and_then(Value::as_str) == Some("mining.submit")).then_some(Self {
            metrics,
            received_at,
        })
    }
    pub(super) fn rejected(&self, error: &StratumError) {
        self.metrics
            .record_rejection(RejectReason::from_reason_id(error.reason_id.as_deref()));
    }
    pub(super) fn acknowledged(&self, result: AckResult) {
        self.metrics
            .observe_share_ack(result, self.received_at.elapsed());
    }
}

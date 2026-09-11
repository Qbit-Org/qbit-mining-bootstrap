//! A submit event begins at complete-frame receipt and ends only at a successful write.
use super::StratumError;
use crate::metrics::{AckResult, Metrics, RejectReason};
use tokio::time::Instant;

pub(super) struct ShareObservation<'a> {
    metrics: &'a Metrics,
    received_at: Option<Instant>,
}
impl<'a> ShareObservation<'a> {
    pub(super) fn begin(metrics: &'a Metrics, is_submit: bool, received_at: Instant) -> Self {
        Self {
            metrics,
            received_at: is_submit.then_some(received_at),
        }
    }
    pub(super) fn rejected(&self, error: &StratumError) {
        if self.received_at.is_some() {
            self.metrics
                .record_rejection(RejectReason::from_reason_id(error.reason_id.as_deref()));
        }
    }
    pub(super) fn acknowledged(&self, result: AckResult) {
        if let Some(received_at) = self.received_at {
            self.metrics
                .observe_share_ack(result, received_at.elapsed());
        }
    }
}

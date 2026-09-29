//! #578: a graceful shutdown never cuts a found block's offer short.
//!
//! Once an offer reservation commits, no claim on any frontend offers the
//! block again: recovery treats an `offer_reserved` row as possibly sent. A
//! shutdown that dropped the attempt between that commit and `submitblock`
//! therefore lost the block, and #570's standby wait sits inside that window.
//! An attempt holds an [`OfferSection`] from just before the reservation
//! until the node's answer is recorded (or the unsent reservation returned),
//! and a shutdown that finds one open keeps driving the attempt until the
//! section closes, within the section's own bounds, before dropping it. The
//! post-offer landing is still dropped: an `offered` row is landed by
//! whichever frontend claims it next, without another offer.
//!
//! A crash in the same window is different: nothing can know whether the
//! call was made, and recovery reconciles the reservation against the chain.
use super::*;

/// The open offer sections of this coordinator's attempts.
pub(super) struct OfferSections(watch::Sender<usize>);

impl Default for OfferSections {
    fn default() -> Self {
        Self(watch::Sender::new(0))
    }
}

/// An open offer section; closes when dropped, on every path.
pub(super) struct OfferSection<'a>(&'a watch::Sender<usize>);

impl Drop for OfferSection<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|open| *open -= 1);
    }
}

impl OfferSections {
    pub(super) fn open(&self) -> OfferSection<'_> {
        self.0.send_modify(|open| *open += 1);
        OfferSection(&self.0)
    }

    fn is_open(&self) -> bool {
        *self.0.borrow() > 0
    }

    async fn closed(&self) {
        let _ = self.0.subscribe().wait_for(|open| *open == 0).await;
    }
}

impl Coordinator {
    /// How long a shutdown waits for an open offer section: the send's own
    /// deadline, the standby wait's bound, and the lease's bound each for
    /// the reservation and the recording. The server's shutdown gives every
    /// task 30 s.
    pub(super) fn offer_section_bound(&self, lease: CandidateLease) -> Duration {
        self.config.block_submit_timeout
            + self
                .config
                .offer_standby
                .as_ref()
                .map_or(Duration::ZERO, |wait| wait.bound)
            + lease.timeout * 2
    }

    /// At shutdown: when `attempt` is inside its offer section, keep driving
    /// it until the section closes or `bound` passes. Returns the attempt's
    /// result when it finished meanwhile; otherwise the caller drops it.
    pub(super) async fn finish_offer_section<F: std::future::Future>(
        &self,
        attempt: std::pin::Pin<&mut F>,
        bound: Duration,
    ) -> Option<F::Output> {
        if !self.offer_sections.is_open() {
            return None;
        }
        tracing::warn!(
            bound_ms = bound.as_millis() as u64,
            "shutdown waits for a found block's offer in flight to be sent and recorded"
        );
        let finished = tokio::time::timeout(bound, async {
            tokio::select! {
                biased;
                result = attempt => Some(result),
                () = self.offer_sections.closed() => None,
            }
        })
        .await;
        finished.unwrap_or_else(|_| {
            tracing::error!(
                bound_ms = bound.as_millis() as u64,
                "ALERT: shutdown stopped waiting for a found block's offer in flight; its reservation may have been sent or not, and recovery reconciles it as unknown"
            );
            None
        })
    }
}

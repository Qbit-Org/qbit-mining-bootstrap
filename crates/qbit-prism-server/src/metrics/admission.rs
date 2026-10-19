//! 3.1 admission and dual-writer readiness series. Their families are
//! declared at startup with no samples, so a single-writer frontend without
//! the readiness endpoint scrapes as 3.0 did plus their HELP and TYPE lines.
use super::*;

impl Metrics {
    /// Start the admission series, once dual-writer mode or the readiness
    /// endpoint exposes admission: not admitting, starting, and every
    /// withdrawal reason at zero, so the first withdrawal shows in
    /// `increase()`.
    pub fn enable_admission(&self) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        registry.set(Family::AdmissionAdmitting, Labels::Empty, 0.);
        for state in AdmissionStateLabel::ALL {
            registry.set(
                Family::AdmissionState,
                label("state", state.as_str()),
                f64::from(*state == AdmissionStateLabel::Starting),
            );
        }
        for reason in WithdrawalReason::ALL {
            registry.register(
                Family::AdmissionWithdrawals,
                label("reason", reason.as_str()),
                0.,
            );
        }
    }

    pub fn publish_admission(&self, state: AdmissionStateLabel) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let admits = matches!(
            state,
            AdmissionStateLabel::Admitting | AdmissionStateLabel::Grace
        );
        registry.set(Family::AdmissionAdmitting, Labels::Empty, f64::from(admits));
        for each in AdmissionStateLabel::ALL {
            registry.set(
                Family::AdmissionState,
                label("state", each.as_str()),
                f64::from(*each == state),
            );
        }
    }

    pub fn record_admission_withdrawal(&self, reason: WithdrawalReason) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(
                Family::AdmissionWithdrawals,
                Labels::One(("reason", reason.as_str())),
            );
    }

    /// Dual-writer mode: whether the named listener accepts connections.
    pub fn publish_stratum_listener_accepting(&self, listener: &str, accepting: bool) {
        let listener = match listener {
            "highdiff" => StratumListenerName::Highdiff,
            _ => StratumListenerName::Default,
        };
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).set(
            Family::StratumListenerAccepting,
            Labels::One(("listener", listener.as_str())),
            f64::from(accepting),
        );
    }

    /// Dual-writer mode: the configured identity and the writer path as last
    /// probed (`None` before the first probe). The own-log latch is the peer
    /// sync's `qbit_prism_peer_sync_own_log_caught_up`.
    pub fn publish_dual_writer(
        &self,
        node_index: i16,
        carry_owner: bool,
        writer_path: Option<WriterPathLabel>,
    ) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        registry.set(
            Family::DualWriterNodeIndex,
            Labels::Empty,
            f64::from(node_index),
        );
        registry.set(
            Family::DualWriterCarryOwner,
            Labels::Empty,
            f64::from(carry_owner),
        );
        for path in WriterPathLabel::ALL {
            registry.set(
                Family::DualWriterWriterPath,
                label("path", path.as_str()),
                f64::from(Some(*path) == writer_path),
            );
        }
    }

    /// Start the readiness endpoint's answer counters at zero.
    pub fn enable_readiness_endpoint(&self) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for answer in ReadinessAnswer::ALL {
            registry.register(
                Family::ReadinessRequests,
                label("result", answer.as_str()),
                0.,
            );
        }
    }

    pub fn record_readiness_request(&self, answer: ReadinessAnswer) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .increment(
                Family::ReadinessRequests,
                Labels::One(("result", answer.as_str())),
            );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(body: &str, family: &str) -> Vec<String> {
        body.lines()
            .filter(|line| line.starts_with(family) && !line.starts_with('#'))
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn admission_series_have_no_samples_until_enabled_then_start_at_starting() {
        let metrics = Metrics::default();
        let body = metrics.render();
        for family in [
            "qbit_prism_admission_admitting",
            "qbit_prism_admission_state",
            "qbit_prism_admission_withdrawals_total",
            "qbit_prism_stratum_listener_accepting",
            "qbit_prism_dual_writer_",
            "qbit_prism_readiness_requests_total",
        ] {
            assert_eq!(samples(&body, family), Vec::<String>::new(), "{family}");
        }
        metrics.enable_admission();
        let body = metrics.render();
        assert_eq!(
            samples(&body, "qbit_prism_admission_admitting"),
            ["qbit_prism_admission_admitting 0"]
        );
        assert_eq!(
            samples(&body, "qbit_prism_admission_state"),
            [
                "qbit_prism_admission_state{state=\"admitting\"} 0",
                "qbit_prism_admission_state{state=\"grace\"} 0",
                "qbit_prism_admission_state{state=\"starting\"} 1",
                "qbit_prism_admission_state{state=\"withdrawn\"} 0",
            ]
        );
        assert_eq!(
            samples(&body, "qbit_prism_admission_withdrawals_total").len(),
            WithdrawalReason::ALL.len()
        );
    }

    #[test]
    fn admission_state_is_one_hot_and_withdrawals_count_by_reason() {
        let metrics = Metrics::default();
        metrics.enable_admission();
        metrics.publish_admission(AdmissionStateLabel::Grace);
        metrics.record_admission_withdrawal(WithdrawalReason::NotReady);
        metrics.record_admission_withdrawal(WithdrawalReason::NotReady);
        let body = metrics.render();
        assert!(body
            .lines()
            .any(|line| line == "qbit_prism_admission_admitting 1"));
        assert!(body
            .lines()
            .any(|line| line == "qbit_prism_admission_state{state=\"grace\"} 1"));
        assert!(body
            .lines()
            .any(|line| line == "qbit_prism_admission_state{state=\"starting\"} 0"));
        assert!(body
            .lines()
            .any(|line| line == "qbit_prism_admission_withdrawals_total{reason=\"not-ready\"} 2"));
        metrics.publish_admission(AdmissionStateLabel::Withdrawn);
        assert!(metrics
            .render()
            .lines()
            .any(|line| line == "qbit_prism_admission_admitting 0"));
    }

    #[test]
    fn dual_writer_series_report_identity_and_one_writer_path() {
        let metrics = Metrics::default();
        metrics.publish_dual_writer(1, false, Some(WriterPathLabel::Local));
        metrics.publish_stratum_listener_accepting("highdiff", true);
        metrics.publish_stratum_listener_accepting("default", false);
        let body = metrics.render();
        for expected in [
            "qbit_prism_dual_writer_node_index 1",
            "qbit_prism_dual_writer_carry_owner 0",
            "qbit_prism_dual_writer_writer_path{path=\"local\"} 1",
            "qbit_prism_dual_writer_writer_path{path=\"remote\"} 0",
            "qbit_prism_stratum_listener_accepting{listener=\"highdiff\"} 1",
            "qbit_prism_stratum_listener_accepting{listener=\"default\"} 0",
        ] {
            assert!(
                body.lines().any(|line| line == expected),
                "missing {expected}"
            );
        }
        metrics.publish_dual_writer(1, false, None);
        assert_eq!(
            samples(&metrics.render(), "qbit_prism_dual_writer_writer_path")
                .iter()
                .filter(|line| line.ends_with(" 1"))
                .count(),
            0,
            "no path before the first probe"
        );
    }

    #[test]
    fn readiness_answers_count_from_zero() {
        let metrics = Metrics::default();
        metrics.enable_readiness_endpoint();
        metrics.record_readiness_request(ReadinessAnswer::Unauthorized);
        let body = metrics.render();
        assert!(body
            .lines()
            .any(|line| line == "qbit_prism_readiness_requests_total{result=\"unauthorized\"} 1"));
        assert!(body
            .lines()
            .any(|line| line == "qbit_prism_readiness_requests_total{result=\"ready\"} 0"));
    }
}

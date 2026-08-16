#!/usr/bin/env python3
"""Owner-boundary tests for the extracted R1 tip-refresh and P1 payout owners.

The current services resolve the coordinator through call-time Runtime
Protocols (the old stack's standalone ``*Ports``/``*Config`` fixtures were
deliberately not built), so this suite pins the boundary itself: exact symbol
identity, private-alias compatibility, descriptor-routed legacy fields, and
the self-contained fanout delivery gate. The scheduler/trigger/publication
behavior is asserted by the upstream suites in
``tests/test_prism_tip_refresh_validation.py``,
``tests/test_prism_tip_refresh_epoch.py``,
``tests/test_prism_tip_publication_boundary.py``,
``tests/test_prism_refresh_retry_pacing.py`` and
``tests/test_prism_payout_state.py``.
"""

from __future__ import annotations

import threading
import unittest

from lab.prism import (
    payout_state,
    prism_coordinator,
    template_artifacts,
    tip_refresh,
)
from lab.prism.prism_coordinator import PrismCoordinator
from lab.prism.tip_refresh import FanoutCancellation
from tests.prism_vardiff_test_support import coordinator

TIP_A = "11" * 32


class FanoutCancellationTests(unittest.TestCase):
    def test_fanout_cancellation_closes_admission_then_drains(self) -> None:
        cancellation = FanoutCancellation()
        self.assertTrue(cancellation.begin_delivery())
        drained = threading.Event()

        def cancel_and_drain() -> None:
            cancellation.set()
            drained.set()

        thread = threading.Thread(target=cancel_and_drain)
        thread.start()
        self.assertFalse(drained.wait(0.05))
        self.assertFalse(cancellation.begin_delivery())
        cancellation.end_delivery()
        self.assertTrue(drained.wait(1.0))
        thread.join(1.0)
        self.assertFalse(thread.is_alive())

    def test_delivery_gate_release_without_admission_fails_closed(self) -> None:
        cancellation = FanoutCancellation()

        with self.assertRaisesRegex(RuntimeError, "without admission"):
            cancellation.end_delivery()

        # cancel() closes admission without waiting so workers may call it
        # while holding a client lock; is_set reports the closed gate.
        cancellation.cancel()
        self.assertTrue(cancellation.is_set())
        self.assertFalse(cancellation.begin_delivery())


class R1FacadeIdentityTests(unittest.TestCase):
    def test_facade_reexports_exact_r1_identities(self) -> None:
        self.assertIs(
            prism_coordinator.RetainedCollectionRefresh,
            tip_refresh.RetainedCollectionRefresh,
        )
        self.assertIs(prism_coordinator.RefreshResult, tip_refresh.RefreshResult)
        self.assertIs(
            prism_coordinator.TipRefreshValidationToken,
            tip_refresh.TipRefreshValidationToken,
        )
        self.assertIs(
            prism_coordinator._FanoutCancellation, tip_refresh.FanoutCancellation
        )
        self.assertIs(
            prism_coordinator.PRISM_TIP_REFRESH_SECONDS_BUCKETS,
            tip_refresh.PRISM_TIP_REFRESH_SECONDS_BUCKETS,
        )
        self.assertIs(
            prism_coordinator.PRISM_TIP_REFRESH_BUILD_PHASES,
            tip_refresh.PRISM_TIP_REFRESH_BUILD_PHASES,
        )
        # PR 80 removed the result/cancellation-stage re-exports.
        self.assertFalse(hasattr(prism_coordinator, "PRISM_TIP_REFRESH_RESULTS"))
        self.assertFalse(
            hasattr(prism_coordinator, "PRISM_TIP_REFRESH_CANCELLATION_STAGES")
        )
        # The owner is a leaf: it never imports the coordinator module.
        self.assertFalse(hasattr(tip_refresh, "PrismCoordinator"))

    def test_p1_reexports_and_private_aliases_preserve_identity(self) -> None:
        self.assertIs(
            prism_coordinator.PayoutStateArtifact, payout_state.PayoutStateArtifact
        )
        self.assertIs(
            prism_coordinator.PayoutStateCandidate, payout_state.PayoutStateCandidate
        )
        # PR 80 removed the PublishedPayoutState compatibility re-export.
        self.assertFalse(hasattr(prism_coordinator, "PublishedPayoutState"))
        self.assertIs(
            prism_coordinator.PayoutLedgerArtifact, payout_state.PayoutLedgerArtifact
        )
        self.assertIs(
            prism_coordinator.TemplateRefreshBlocked,
            payout_state.TemplateRefreshBlocked,
        )
        self.assertIs(
            prism_coordinator.TemplateRefreshSuperseded,
            payout_state.TemplateRefreshSuperseded,
        )
        # PR 80 removed the four underscore payout aliases; the coordinator
        # keeps only the unaliased exception it still raises.
        self.assertFalse(hasattr(prism_coordinator, "_AcceptedBlockPayoutTransition"))
        self.assertFalse(hasattr(prism_coordinator, "_PayoutDeliveryAdmission"))
        self.assertFalse(hasattr(prism_coordinator, "_PayoutStateDeliveryGate"))
        self.assertFalse(hasattr(prism_coordinator, "_PayoutStatePublicationBlocked"))
        self.assertIs(
            prism_coordinator.PayoutStatePublicationBlocked,
            payout_state.PayoutStatePublicationBlocked,
        )
        # template_artifacts re-imports the exception trio from the final P1
        # owner so the interim import path stays identity-compatible.
        self.assertIs(
            template_artifacts.TemplateRefreshBlocked,
            payout_state.TemplateRefreshBlocked,
        )
        self.assertIs(
            template_artifacts.TemplateRefreshSuperseded,
            payout_state.TemplateRefreshSuperseded,
        )


class R1DescriptorRoutingTests(unittest.TestCase):
    def test_tip_state_descriptors_route_to_the_r1_owner(self) -> None:
        server = coordinator()
        service = server._ensure_tip_refresh_service()
        self.assertIs(server._ensure_tip_refresh_service(), service)

        server.tip_refresh_job_count = 7
        self.assertEqual(service.tip_refresh_job_count, 7)
        self.assertEqual(server.tip_refresh_job_count, 7)

        server.current_tip_first_seen = (TIP_A, 5.0)
        self.assertEqual(service.current_tip_first_seen, (TIP_A, 5.0))
        with server.lock:
            self.assertEqual(server._current_published_tip_hash_locked(), TIP_A)

        # The refresh lock is owner state reached through the legacy name.
        self.assertIs(server._tip_refresh_lock, service._tip_refresh_lock)

    def test_unset_tip_state_reads_raise_attribute_error_like_lazy_fields(
        self,
    ) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.lock = threading.RLock()

        with self.assertRaises(AttributeError):
            _ = server.current_tip_first_seen
        with self.assertRaises(AttributeError):
            _ = server.tip_refresh_job_count

        server.tip_refresh_job_count = 3
        self.assertEqual(server.tip_refresh_job_count, 3)

    def test_payout_state_descriptors_route_to_the_p1_owner(self) -> None:
        server = coordinator()
        service = server._ensure_payout_state_service()

        server._payout_state_generation = 9
        self.assertEqual(server._payout_state_generation, 9)
        current = server._published_payout_state
        self.assertIs(current, server._published_payout_state)
        # The generation adapter reads the same owner state.
        self.assertEqual(server._current_payout_generation(), 9)
        self.assertIsNotNone(service)


if __name__ == "__main__":
    unittest.main()

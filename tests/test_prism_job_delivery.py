#!/usr/bin/env python3
"""Direct contract tests for the coordinator-free S2 delivery boundary.

The current owner keeps one call-time ``JobDeliveryRuntime`` typed port and
descriptor-routed compatibility state (the old stack's per-capability port
dataclasses and authority objects were deliberately not built). These cases
pin the boundary itself: symbol identity, descriptor routing and adoption,
the narrow ``next_job_id`` / ``complete_delivery`` adapters, and legacy
retained-state conversion. Delivery behavior (stamping, retained jobs,
initial jobs, difficulty adjacency) is asserted by the upstream suites in
``tests/test_prism_retained_jobs.py``, ``tests/test_prism_initial_job_delivery.py``,
``tests/test_prism_tip_refresh_delivery.py`` and
``tests/test_prism_job_builder.py`` (``JobContextStampTests``).
"""

from __future__ import annotations

from collections import OrderedDict
import time
from types import SimpleNamespace
import unittest

import lab.prism.job_delivery as job_delivery_module
from lab.prism import prism_coordinator
from lab.prism.job_delivery import (
    EvictedJobEntry,
    JobBuildFailed,
    PendingInitialJob,
    PrismJobContext,
)
from lab.prism.prism_coordinator import (
    EvictedJobEntry as FacadeEvictedJobEntry,
    PendingInitialJob as FacadePendingInitialJob,
    PrismJobContext as FacadePrismJobContext,
)
from tests.prism_vardiff_test_support import client, coordinator

TIP_A = "11" * 32


class S2FacadeIdentityTests(unittest.TestCase):
    def test_facade_reexports_exact_s2_identities(self) -> None:
        self.assertIs(FacadePrismJobContext, PrismJobContext)
        self.assertIs(FacadeEvictedJobEntry, EvictedJobEntry)
        self.assertIs(FacadePendingInitialJob, PendingInitialJob)
        self.assertIs(prism_coordinator._JobBuildFailed, JobBuildFailed)
        self.assertEqual(
            prism_coordinator.PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
            job_delivery_module.PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
        )
        # The owner is a leaf: it never imports the coordinator module.
        self.assertFalse(hasattr(job_delivery_module, "PrismCoordinator"))


class S2DescriptorRoutingTests(unittest.TestCase):
    def test_coordinator_descriptors_route_s2_state_to_owner(self) -> None:
        server = coordinator()
        service = server._ensure_job_delivery_service()

        self.assertIs(server.jobs, service.jobs)
        self.assertIs(server.pending_initial_jobs, service.pending_initial_jobs)
        self.assertIs(server.evicted_job_graveyard, service.evicted_job_graveyard)

        server.job_counter = 5
        self.assertEqual(service.job_counter, 5)
        service.initial_job_sent_count = 3
        self.assertEqual(server.initial_job_sent_count, 3)

    def test_coordinator_adopts_replaced_jobs_and_pending_initial_mappings(
        self,
    ) -> None:
        server = coordinator()
        service = server._ensure_job_delivery_service()

        jobs_replacement: dict[str, PrismJobContext] = {}
        server.jobs = jobs_replacement
        self.assertIs(service.jobs, jobs_replacement)

        pending_replacement: dict[object, PendingInitialJob] = {}
        server.pending_initial_jobs = pending_replacement
        self.assertIs(service.pending_initial_jobs, pending_replacement)


class S2NarrowAdapterTests(unittest.TestCase):
    def test_next_job_id_allocates_through_the_owner_counter(self) -> None:
        server = coordinator()
        service = server._ensure_job_delivery_service()
        server.job_counter = 41

        self.assertEqual(service.next_job_id(), "prism-42")
        # The coordinator adapter is a pure delegate over the same counter.
        self.assertEqual(server._next_job_delivery_id(), "prism-43")
        self.assertEqual(server.job_counter, 43)

    def test_complete_delivery_notes_tip_work_before_progress_proof(self) -> None:
        server = coordinator()
        service = server._ensure_job_delivery_service()
        state = client()
        events: list[tuple[str, object]] = []
        server.note_tip_work_delivered = (  # type: ignore[method-assign]
            lambda delivered_client, parent: events.append(
                ("tip", (delivered_client, parent))
            )
        )
        server._record_progress_delivery = (  # type: ignore[method-assign]
            lambda delivered_client, context, monotonic: events.append(
                ("progress", (delivered_client, context, monotonic))
            )
        )
        context = SimpleNamespace(template={"previousblockhash": TIP_A})

        service.complete_delivery(state, context, 3.5)

        self.assertEqual(
            [name for name, _payload in events], ["tip", "progress"]
        )
        self.assertEqual(events[0][1], (state, TIP_A))
        self.assertEqual(events[1][1], (state, context, 3.5))

        # The coordinator adapter routes through the same owner method.
        events.clear()
        server._complete_job_delivery(state, context, 4.5)
        self.assertEqual(
            [name for name, _payload in events], ["tip", "progress"]
        )


class RetainedStateAdoptionTests(unittest.TestCase):
    def test_ensure_evicted_job_state_converts_legacy_tuple_entries(self) -> None:
        server = coordinator()
        service = server._ensure_job_delivery_service()
        live_client = SimpleNamespace(connection_id=7)
        server.clients = [live_client]
        context = SimpleNamespace(template={"previousblockhash": TIP_A})
        evicted_at = time.monotonic()
        # A legacy plain-dict graveyard with tuple entries, seeded through the
        # compatibility field exactly as pre-extraction tests did.
        server.evicted_job_graveyard = {
            "prism-legacy": (context, 7, evicted_at),
        }

        server._ensure_evicted_job_state()

        graveyard = service.evicted_job_graveyard
        self.assertIsInstance(graveyard, OrderedDict)
        entry = graveyard["prism-legacy"]
        self.assertIsInstance(entry, EvictedJobEntry)
        self.assertIs(entry.context, context)
        self.assertEqual(entry.connection_id, 7)
        self.assertEqual(entry.evicted_monotonic, evicted_at)
        self.assertEqual(entry.previousblockhash, TIP_A)
        self.assertIs(entry.client, live_client)
        # Conversion rebuilds the per-connection retained indexes.
        self.assertIn(7, service.evicted_jobs_by_connection)
        self.assertIn("prism-legacy", service.evicted_jobs_by_connection[7])

    def test_ensure_evicted_job_state_rebuilds_indexes_on_tip_change(self) -> None:
        server = coordinator()
        service = server._ensure_job_delivery_service()
        server.clients = set()
        context = SimpleNamespace(template={"previousblockhash": TIP_A})
        entry = EvictedJobEntry(
            context=context,
            connection_id=3,
            evicted_monotonic=time.monotonic(),
            previousblockhash=TIP_A,
            client=None,
        )
        service.evicted_job_graveyard = OrderedDict({"prism-kept": entry})
        service.evicted_jobs_by_connection = {}
        service.evicted_job_index_tip_hash = "stale-tip"

        server._ensure_evicted_job_state()

        self.assertIn(3, service.evicted_jobs_by_connection)
        self.assertIn("prism-kept", service.evicted_jobs_by_connection[3])
        self.assertEqual(
            service.evicted_job_index_tip_hash,
            server._current_published_tip_hash_locked(),
        )


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Durable worker-difficulty wiring at the vardiff ownership boundary."""

from __future__ import annotations

from decimal import Decimal
import inspect
from itertools import product
from types import SimpleNamespace
import threading
import time
import unittest
from unittest import mock

from lab.auxpow import vardiff
from lab.prism.prism_coordinator import PrismCoordinator
from lab.prism.stratum_session import ClientState
from lab.prism.vardiff_service import (
    MAX_PENDING_VARDIFF_DURABLE_WRITES,
    PRISM_VARDIFF_DURABLE_PRUNE_BATCH,
    PRISM_VARDIFF_DURABLE_PRUNE_MAX_BATCHES,
    VardiffService,
    _PendingDurableWrite,
)
from lab.prism.worker_difficulty_store import (
    MemoryWorkerDifficultyStore,
    PostgresWorkerDifficultyStore,
)
from tests.prism_vardiff_test_support import worker_identity


def lane_config() -> vardiff.VardiffConfig:
    return vardiff.VardiffConfig(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("1024"),
        max_difficulty=Decimal("4294967296"),
        retarget_interval_seconds=Decimal("300"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("16384"),
        max_step_down_factor=Decimal("4"),
    )


def runtime(
    store: MemoryWorkerDifficultyStore,
    *,
    ttl_seconds: float = 900.0,
    enabled: bool = True,
    max_entries: int = 32,
) -> SimpleNamespace:
    return SimpleNamespace(
        worker_difficulty_store=store,
        vardiff_resume_enabled=enabled,
        vardiff_resume_ttl_seconds=ttl_seconds,
        vardiff_resume_max_entries=max_entries,
        vardiff_resume_max_start_factor=Decimal("1024"),
        vardiff_config=lane_config(),
        share_difficulty=Decimal("16384"),
        lock=threading.RLock(),
        clients=set(),
    )


def client(
    *,
    listener: str = "default",
    username: str = "miner-a.rig-1",
    difficulty: Decimal = Decimal("16384"),
) -> ClientState:
    state = ClientState(
        sock=object(),
        address=("127.0.0.1", 1),
        connection_id=1,
        extranonce1_hex="00000001",
        listener_name=listener,
        listener_vardiff_config=lane_config(),
        share_difficulty=difficulty,
    )
    state.worker = worker_identity(username)
    state.username = username
    return state


class DurableVardiffResumeTests(unittest.TestCase):
    def service(self, runtime_namespace: SimpleNamespace) -> VardiffService:
        """A service whose persistence lane is closed when the test ends."""
        built = VardiffService(runtime_namespace)
        self.addCleanup(built.shutdown_durable_executor)
        return built

    def drained(self, built: VardiffService) -> VardiffService:
        """Wait out the asynchronous persistence lane.

        Durable writes are deliberately fire-and-forget so mining never waits
        on them; tests that assert on the store have to say so explicitly.
        """
        self.assertTrue(built.flush_durable_writes(timeout=5.0))
        return built

    def test_durable_cache_is_loaded_before_any_listener_accepts(self) -> None:
        source = inspect.getsource(PrismCoordinator._serve_with_listener_stack)
        preload = source.index("self._ensure_vardiff_service()")

        self.assertLess(preload, source.index("self._start_secondary_accept_service"))
        self.assertLess(preload, source.index("self.accept_loop"))

    def test_share_backed_retarget_survives_service_restart(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        first = self.service(runtime(durable))
        state = client()
        state.vardiff_last_accepted_difficulty = Decimal("16384")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.1):
            first.record_session_difficulty(
                state,
                Decimal("500000"),
                share_backed=True,
            )
        self.drained(first)

        with (
            mock.patch("lab.prism.vardiff_service.time.time", return_value=1001.0),
            mock.patch(
                "lab.prism.vardiff_service.time.monotonic",
                return_value=2000.0,
            ),
        ):
            restarted = self.service(runtime(durable))
            retained, outcome = restarted.session_difficulty_store.lookup(
                ("default", "miner-a.rig-1"),
                now=2000.0,
            )

        self.assertEqual((retained, outcome), (Decimal("500000"), "hit"))
        self.assertEqual(restarted.vardiff_durable_preload_records, 1)
        self.assertEqual(first.vardiff_durable_write_outcome_counts["applied"], 1)

    def test_unproven_disconnect_step_up_is_not_persisted(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable))
        state = client(difficulty=Decimal("500000"))
        state.vardiff_last_accepted_difficulty = Decimal("16384")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        self.drained(service).record_session_difficulty(state, share_backed=True)
        self.drained(service)

        self.assertEqual(len(durable), 0)
        self.assertEqual(
            service.session_difficulty_store.lookup(
                ("default", "miner-a.rig-1"),
                now=state.vardiff_window_started_monotonic,
            )[0],
            Decimal("500000"),
        )

    def test_fast_initial_step_is_not_durable_until_proven(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable))
        state = client()
        state.vardiff_last_accepted_difficulty = Decimal("16384")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        service.record_session_difficulty(
            state,
            Decimal("1048576"),
            share_backed=True,
            previous_difficulty=Decimal("16384"),
            initial_convergence=True,
        )
        self.drained(service)
        self.assertEqual(len(durable), 0)

        state.share_difficulty = Decimal("1048576")
        state.vardiff_last_accepted_difficulty = Decimal("1048576")
        state.vardiff_last_accepted_wall_ms = 1_000_100
        service.record_session_difficulty(state, share_backed=True)
        self.drained(service)
        self.assertEqual(
            durable.load_recent(evidence_after_ms=1_000_000, limit=1)[0].difficulty,
            Decimal("1048576"),
        )

    def test_idle_step_down_replaces_higher_durable_row_across_restart(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable))
        state = client(difficulty=Decimal("1048576"))
        state.vardiff_last_accepted_difficulty = Decimal("1048576")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.1):
            service.record_session_difficulty(state, share_backed=True)
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.2):
            service.record_session_difficulty(
                state,
                Decimal("65536"),
                share_backed=False,
                previous_difficulty=Decimal("1048576"),
            )
        self.drained(service)
        with (
            mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.3),
            mock.patch("lab.prism.vardiff_service.time.monotonic", return_value=2000.0),
        ):
            restarted = self.service(runtime(durable))

        self.assertEqual(
            restarted.session_difficulty_store.lookup(
                ("default", "miner-a.rig-1"),
                now=2000.0,
            )[0],
            Decimal("65536"),
        )

    def test_retention_kill_switch_prevents_durable_writes(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable, enabled=False))
        state = client()
        state.vardiff_last_accepted_difficulty = Decimal("16384")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        service.record_session_difficulty(
            state,
            Decimal("500000"),
            share_backed=True,
        )
        self.drained(service)

        self.assertEqual(len(durable), 0)
        self.assertEqual(
            sum(service.vardiff_durable_write_outcome_counts.values()),
            0,
        )

    def test_startup_prune_removes_all_expired_rows_not_only_cache_limit(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        for index in range(5):
            durable.upsert(
                listener="default",
                worker_username=f"expired-{index}",
                difficulty=Decimal("16384"),
                evidence_at_ms=1_000,
                now_ms=1_000,
            )
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.0):
            service = VardiffService(
                runtime(durable, ttl_seconds=1.0, max_entries=2)
            )

        self.assertEqual(len(durable), 0)
        self.assertEqual(service.vardiff_durable_pruned_records, 5)

    def test_exact_difficulty_share_evidence_persists_on_disconnect(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable))
        state = client(difficulty=Decimal("500000"))
        state.vardiff_last_accepted_difficulty = Decimal("500000")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.2):
            service.record_session_difficulty(state, share_backed=True)
        self.drained(service)

        recent = durable.load_recent(evidence_after_ms=999_999, limit=10)
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0].difficulty, Decimal("500000"))

    def test_preload_preserves_remaining_ttl(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        durable.upsert(
            listener="default",
            worker_username="miner-a.rig-1",
            difficulty=Decimal("500000"),
            evidence_at_ms=100_000,
            now_ms=100_000,
        )
        with (
            mock.patch("lab.prism.vardiff_service.time.time", return_value=109.0),
            mock.patch(
                "lab.prism.vardiff_service.time.monotonic",
                return_value=1000.0,
            ),
        ):
            service = self.service(runtime(durable, ttl_seconds=10.0))

        self.assertEqual(
            service.session_difficulty_store.lookup(
                ("default", "miner-a.rig-1"),
                now=1000.5,
            )[0],
            Decimal("500000"),
        )
        self.assertEqual(
            service.session_difficulty_store.lookup(
                ("default", "miner-a.rig-1"),
                now=1001.1,
            ),
            (None, "expired"),
        )

    def test_preload_is_lane_and_exact_username_scoped(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        for listener, username, difficulty in (
            ("default", "miner-a.rig-1", Decimal("500000")),
            ("highdiff", "miner-a.rig-1", Decimal("750000")),
            ("default", "miner-a.RIG-1", Decimal("250000")),
        ):
            durable.upsert(
                listener=listener,
                worker_username=username,
                difficulty=difficulty,
                evidence_at_ms=1_000_000,
                now_ms=1_000_000,
            )
        with (
            mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.1),
            mock.patch(
                "lab.prism.vardiff_service.time.monotonic",
                return_value=2000.0,
            ),
        ):
            service = self.service(runtime(durable))

        for key, expected in (
            (("default", "miner-a.rig-1"), Decimal("500000")),
            (("highdiff", "miner-a.rig-1"), Decimal("750000")),
            (("default", "miner-a.RIG-1"), Decimal("250000")),
        ):
            self.assertEqual(
                service.session_difficulty_store.lookup(key, now=2000.0)[0],
                expected,
            )

    def test_store_failure_falls_back_without_breaking_retention(self) -> None:
        class FailingStore(MemoryWorkerDifficultyStore):
            def upsert(self, **kwargs: object):  # type: ignore[no-untyped-def]
                raise RuntimeError("database unavailable")

        durable = FailingStore()
        service = self.service(runtime(durable))
        state = client()
        state.vardiff_last_accepted_difficulty = Decimal("16384")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        service.record_session_difficulty(
            state,
            Decimal("500000"),
            share_backed=True,
        )
        self.drained(service)

        self.assertEqual(service.vardiff_durable_write_outcome_counts["failed"], 1)
        self.assertEqual(
            service.session_difficulty_store.lookup(
                ("default", "miner-a.rig-1"),
                now=state.vardiff_window_started_monotonic,
            )[0],
            Decimal("500000"),
        )

    def test_durable_write_never_waits_on_the_client_job_lock(self) -> None:
        # The payload is snapshotted by record_session_difficulty, so the lane
        # has no reason to touch client state -- and a lane that waited on
        # job_update_lock would let one client's slow job send stall every
        # other client's write behind it on the single worker.
        invoked = threading.Event()

        def run_json(_sql: str) -> dict[str, object]:
            invoked.set()
            return {"applied": True, "stored": None}

        service = self.service(runtime(MemoryWorkerDifficultyStore()))
        service.worker_difficulty_store = PostgresWorkerDifficultyStore(run_json)
        state = client()
        state.vardiff_last_accepted_difficulty = Decimal("16384")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        with state.job_update_lock:
            service.record_session_difficulty(
                state,
                Decimal("500000"),
                share_backed=True,
            )
            self.assertTrue(invoked.wait(5.0))

        self.assertEqual(
            service.vardiff_durable_write_outcome_counts["applied"],
            1,
        )

    def test_one_clients_held_job_lock_cannot_stall_another_clients_write(
        self,
    ) -> None:
        service = self.service(runtime(MemoryWorkerDifficultyStore()))
        blocked = client(username="blocked.rig")
        blocked.vardiff_last_accepted_difficulty = Decimal("16384")
        blocked.vardiff_last_accepted_wall_ms = 1_000_000
        other = client(username="other.rig")
        other.vardiff_last_accepted_difficulty = Decimal("16384")
        other.vardiff_last_accepted_wall_ms = 1_000_000

        with blocked.job_update_lock:
            service.record_session_difficulty(
                blocked, Decimal("500000"), share_backed=True
            )
            service.record_session_difficulty(
                other, Decimal("500000"), share_backed=True
            )
            self.assertTrue(service.flush_durable_writes(timeout=5.0))

        self.assertEqual(
            service.vardiff_durable_write_outcome_counts["applied"],
            2,
        )

    def test_saturated_lane_evicts_resume_hints_before_step_downs(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable, max_entries=4096))

        # Park the lane so the queue shape is deterministic: no worker is
        # concurrently draining entries out from under the assertions.
        with mock.patch.object(service, "_ensure_durable_worker_locked"):
            capacity = MAX_PENDING_VARDIFF_DURABLE_WRITES
            for index in range(capacity):
                service._enqueue_durable_write(
                    _PendingDurableWrite(
                        key=("default", f"hint-{index}"),
                        downward_only=False,
                        difficulty=Decimal("16384"),
                        evidence_at_ms=1_000_000,
                        now_ms=1_000_000,
                    )
                )
            self.assertEqual(service.durable_pending_depth(), capacity)
            # Turn one hint into a composite whose correction matters if its
            # evidence is stale. It must receive the same queue priority as a
            # pure step-down even though its primary operation is an upsert.
            service._enqueue_durable_write(
                _PendingDurableWrite(
                    key=("default", "hint-0"),
                    downward_only=True,
                    difficulty=Decimal("1024"),
                    evidence_at_ms=None,
                    now_ms=1_000_100,
                )
            )
            # New step-downs must still get in by evicting the remaining pure
            # hints rather than any safety-critical correction.
            for index in range(8):
                service._enqueue_durable_write(
                    _PendingDurableWrite(
                        key=("default", f"stepdown-{index}"),
                        downward_only=True,
                        difficulty=Decimal("1024"),
                        evidence_at_ms=None,
                        now_ms=1_000_000,
                    )
                )
            self.assertEqual(service.durable_pending_depth(), capacity)
            with service._vardiff_durable_lock:
                queued = list(service._vardiff_durable_pending.values())

        self.assertEqual(sum(1 for entry in queued if entry.downward_only), 8)
        self.assertEqual(
            sum(1 for entry in queued if entry.has_downward_correction),
            9,
        )
        self.assertEqual(service.vardiff_durable_downward_dropped, 0)
        self.assertEqual(
            service.vardiff_durable_write_outcome_counts["dropped"],
            8,
        )

    def test_a_step_down_the_lane_cannot_keep_is_counted_and_logged(self) -> None:
        service = self.service(runtime(MemoryWorkerDifficultyStore()))

        with mock.patch.object(service, "_ensure_durable_worker_locked"):
            for index in range(MAX_PENDING_VARDIFF_DURABLE_WRITES + 4):
                service._enqueue_durable_write(
                    _PendingDurableWrite(
                        key=("default", f"stepdown-{index}"),
                        downward_only=True,
                        difficulty=Decimal("1024"),
                        evidence_at_ms=None,
                        now_ms=1_000_000,
                    )
                )
            depth = service.durable_pending_depth()

        # Bounded, and the losses are loud rather than silent.
        self.assertEqual(depth, MAX_PENDING_VARDIFF_DURABLE_WRITES)
        self.assertGreaterEqual(service.vardiff_durable_downward_dropped, 4)
        self.assertIn(
            "qbit_prism_vardiff_durable_downward_dropped_total",
            "\n".join(service.metrics_lines()),
        )

    def test_queued_writes_coalesce_by_worker_identity(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable))

        with mock.patch.object(service, "_ensure_durable_worker_locked"):
            for difficulty in ("1048576", "262144", "65536"):
                service._enqueue_durable_write(
                    _PendingDurableWrite(
                        key=("default", "miner-a.rig-1"),
                        downward_only=True,
                        difficulty=Decimal(difficulty),
                        evidence_at_ms=None,
                        now_ms=1_000_000,
                    )
                )
            self.assertEqual(service.durable_pending_depth(), 1)
            with service._vardiff_durable_lock:
                merged = service._vardiff_durable_pending[
                    ("default", "miner-a.rig-1")
                ]

        # Three step-downs collapse to the lowest, which is what applying all
        # three in order would have left behind.
        self.assertEqual(merged.difficulty, Decimal("65536"))
        self.assertTrue(merged.downward_only)
        self.assertEqual(service.vardiff_durable_coalesced, 2)

    def test_a_later_step_down_clamps_a_queued_evidence_write(self) -> None:
        service = self.service(runtime(MemoryWorkerDifficultyStore()))
        upsert = _PendingDurableWrite(
            key=("default", "miner-a.rig-1"),
            downward_only=False,
            difficulty=Decimal("1048576"),
            evidence_at_ms=1_000_000,
            now_ms=1_000_000,
        )
        lower = _PendingDurableWrite(
            key=("default", "miner-a.rig-1"),
            downward_only=True,
            difficulty=Decimal("65536"),
            evidence_at_ms=None,
            now_ms=1_000_100,
        )

        merged = upsert.merged_with(lower)

        # upsert-then-lower must leave the minimum difficulty AND the upsert's
        # evidence, so the merge cannot discard the share evidence.
        self.assertFalse(merged.downward_only)
        self.assertEqual(merged.difficulty, Decimal("65536"))
        self.assertEqual(merged.evidence_at_ms, 1_000_000)
        # A later evidence write supersedes the step-down if it applies, but
        # must retain the correction for the branch where the store already
        # has still-newer evidence and rejects the queued upsert.
        lower_then_upsert = lower.merged_with(upsert)
        self.assertEqual(lower_then_upsert.difficulty, upsert.difficulty)
        self.assertEqual(
            lower_then_upsert.stale_downward_difficulty,
            lower.difficulty,
        )

    def test_coalescing_keeps_newer_evidence_when_it_was_queued_first(self) -> None:
        newer = _PendingDurableWrite(
            key=("default", "miner-a.rig-1"),
            downward_only=False,
            difficulty=Decimal("1048576"),
            evidence_at_ms=1_000_100,
            now_ms=1_000_100,
        )
        older = _PendingDurableWrite(
            key=newer.key,
            downward_only=False,
            difficulty=Decimal("65536"),
            evidence_at_ms=1_000_000,
            now_ms=1_000_200,
        )

        merged = newer.merged_with(older)

        # Applying newer then older leaves the newer row standing because the
        # store rejects the second write as stale. Coalescing must preserve the
        # same evidence and difficulty.
        self.assertEqual(merged.evidence_at_ms, newer.evidence_at_ms)
        self.assertEqual(merged.difficulty, newer.difficulty)
        self.assertEqual(merged.now_ms, newer.now_ms)

    def test_coalesced_step_down_still_applies_when_upsert_is_stale(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable))
        durable.upsert(
            listener="default",
            worker_username="miner-a.rig-1",
            difficulty=Decimal("1048576"),
            evidence_at_ms=1_000_200,
            now_ms=1_000_200,
        )
        stale_upsert = _PendingDurableWrite(
            key=("default", "miner-a.rig-1"),
            downward_only=False,
            difficulty=Decimal("262144"),
            evidence_at_ms=1_000_100,
            now_ms=1_000_300,
        )
        lower = _PendingDurableWrite(
            key=stale_upsert.key,
            downward_only=True,
            difficulty=Decimal("65536"),
            evidence_at_ms=None,
            now_ms=1_000_400,
        )

        service._apply_durable_write(stale_upsert.merged_with(lower))

        stored = durable._entries[stale_upsert.key]
        self.assertEqual(stored.difficulty, Decimal("65536"))
        self.assertEqual(stored.evidence_at_ms, 1_000_200)
        self.assertEqual(stored.updated_at_ms, 1_000_400)
        self.assertEqual(service.vardiff_durable_write_outcome_counts["stale"], 1)
        self.assertEqual(service.vardiff_durable_write_outcome_counts["lowered"], 1)

    def test_coalescing_is_order_equivalent_for_upserts_and_step_downs(self) -> None:
        key = ("default", "miner-a.rig-1")
        operation_specs = (
            (False, Decimal("16"), 1_000_000),
            (False, Decimal("64"), 1_000_100),
            (False, Decimal("8"), 1_000_100),
            (True, Decimal("8"), None),
            (True, Decimal("32"), None),
            (True, Decimal("128"), None),
        )
        seeds = (
            None,
            (999_900, Decimal("4")),
            (999_900, Decimal("256")),
            (1_000_050, Decimal("32")),
            (1_000_200, Decimal("256")),
        )

        def apply(store, pending: _PendingDurableWrite) -> None:
            if pending.downward_only:
                store.apply_downward(
                    listener=key[0],
                    worker_username=key[1],
                    difficulty=pending.difficulty,
                    now_ms=pending.now_ms,
                )
                return
            result = store.upsert(
                listener=key[0],
                worker_username=key[1],
                difficulty=pending.difficulty,
                evidence_at_ms=int(pending.evidence_at_ms or 0),
                now_ms=pending.now_ms,
            )
            stale_downward = pending._stale_downward()
            if not result.applied and stale_downward is not None:
                store.apply_downward(
                    listener=key[0],
                    worker_username=key[1],
                    difficulty=stale_downward[0],
                    now_ms=stale_downward[1],
                )

        for length in range(1, 5):
            for specs in product(operation_specs, repeat=length):
                operations = [
                    _PendingDurableWrite(
                        key=key,
                        downward_only=downward_only,
                        difficulty=difficulty,
                        evidence_at_ms=evidence_at_ms,
                        now_ms=1_001_000 + index,
                    )
                    for index, (downward_only, difficulty, evidence_at_ms) in enumerate(
                        specs
                    )
                ]
                merged = operations[0]
                for operation in operations[1:]:
                    merged = merged.merged_with(operation)
                for seed in seeds:
                    sequential = MemoryWorkerDifficultyStore()
                    coalesced = MemoryWorkerDifficultyStore()
                    if seed is not None:
                        for store in (sequential, coalesced):
                            store.upsert(
                                listener=key[0],
                                worker_username=key[1],
                                difficulty=seed[1],
                                evidence_at_ms=seed[0],
                                now_ms=999_000,
                            )
                    for operation in operations:
                        apply(sequential, operation)
                    apply(coalesced, merged)
                    self.assertEqual(
                        coalesced._entries,
                        sequential._entries,
                        (specs, seed, merged),
                    )

    def test_shutdown_drains_step_downs_instead_of_cancelling_them(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        # Live evidence, so the service's startup prune leaves these standing.
        live_ms = max(0, int(time.time() * 1000))
        for index in range(6):
            durable.upsert(
                listener="default",
                worker_username=f"stepdown-{index}",
                difficulty=Decimal("1048576"),
                evidence_at_ms=live_ms,
                now_ms=live_ms,
            )
        service = self.service(runtime(durable))

        # Queue with the lane parked, so every entry is still pending when
        # shutdown begins -- the case where cancelling would lose them.
        with mock.patch.object(service, "_ensure_durable_worker_locked"):
            for index in range(6):
                service._enqueue_durable_write(
                    _PendingDurableWrite(
                        key=("default", f"stepdown-{index}"),
                        downward_only=True,
                        difficulty=Decimal("65536"),
                        evidence_at_ms=None,
                        now_ms=1_000_100,
                    )
                )
        self.assertEqual(service.durable_pending_depth(), 6)
        service.shutdown_durable_executor()

        self.assertEqual(service.durable_pending_depth(), 0)
        self.assertEqual(service.vardiff_durable_downward_dropped, 0)
        for index in range(6):
            self.assertEqual(
                durable._entries[("default", f"stepdown-{index}")].difficulty,
                Decimal("65536"),
            )

    def test_shutdown_drain_stays_bounded_when_the_store_hangs(self) -> None:
        service = self.service(runtime(MemoryWorkerDifficultyStore()))
        service._vardiff_durable_operation_timeout_seconds = 0.3

        with mock.patch.object(service, "_ensure_durable_worker_locked"):
            for index in range(40):
                service._enqueue_durable_write(
                    _PendingDurableWrite(
                        key=("default", f"stepdown-{index}"),
                        downward_only=True,
                        difficulty=Decimal("65536"),
                        evidence_at_ms=None,
                        now_ms=1_000_100,
                    )
                )
        # Each statement is bounded by the store's own operation deadline; the
        # drain's job is to bound how many of them shutdown will wait for.
        with mock.patch.object(
            service,
            "_apply_durable_write",
            side_effect=lambda pending: time.sleep(0.02),
        ):
            started = time.monotonic()
            service.shutdown_durable_executor()
            elapsed = time.monotonic() - started

        # Bounded by the ledger operation budget, and whatever the budget
        # could not cover is accounted for rather than vanishing.
        self.assertLess(elapsed, 3.0)
        self.assertEqual(service.durable_pending_depth(), 0)
        self.assertGreater(service.vardiff_durable_downward_dropped, 0)

    def test_step_down_never_suppresses_later_share_backed_evidence(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable))
        state = client(difficulty=Decimal("1048576"))
        state.vardiff_last_accepted_difficulty = Decimal("1048576")
        state.vardiff_last_accepted_wall_ms = 1_000_000

        # A share-backed write, then an idle step-down whose wall clock is
        # LATER than the share evidence that follows it.
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.1):
            service.record_session_difficulty(state, share_backed=True)
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1200.0):
            service.record_session_difficulty(
                state,
                Decimal("65536"),
                share_backed=False,
                previous_difficulty=Decimal("1048576"),
            )
        self.drained(service)
        stored = durable._entries[("default", "miner-a.rig-1")]
        self.assertEqual(stored.difficulty, Decimal("65536"))
        # The correction carries no evidence of its own, so evidence_at still
        # points at the last accepted share.
        self.assertEqual(stored.evidence_at_ms, 1_000_000)

        # Genuine share evidence recorded a moment after the last accepted
        # share -- but before the step-down's wall clock -- must still apply.
        state.share_difficulty = Decimal("500000")
        state.vardiff_last_accepted_difficulty = Decimal("500000")
        state.vardiff_last_accepted_wall_ms = 1_000_050
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1300.0):
            service.record_session_difficulty(state, share_backed=True)
        self.drained(service)

        self.assertEqual(
            durable._entries[("default", "miner-a.rig-1")].difficulty,
            Decimal("500000"),
        )
        self.assertEqual(
            service.vardiff_durable_write_outcome_counts["stale"],
            0,
        )

    def test_step_down_never_inserts_a_row_for_an_unknown_worker(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        service = self.service(runtime(durable))
        state = client(difficulty=Decimal("1048576"))

        service.record_session_difficulty(
            state,
            Decimal("65536"),
            share_backed=False,
            previous_difficulty=Decimal("1048576"),
        )
        self.drained(service)

        # Nothing stored means nothing a restart could resurrect.
        self.assertEqual(len(durable), 0)
        self.assertEqual(
            service.vardiff_durable_write_outcome_counts["unchanged"],
            1,
        )

    def test_periodic_prune_runs_without_any_successful_write(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.0):
            service = self.service(runtime(durable, ttl_seconds=1.0))
        self.assertEqual(service.vardiff_durable_pruned_records, 0)

        # Rows this process never wrote -- inherited from a previous run.
        for index in range(3):
            durable.upsert(
                listener="default",
                worker_username=f"expired-{index}",
                difficulty=Decimal("16384"),
                evidence_at_ms=1_000,
                now_ms=1_000,
            )

        # Not yet due: the interval is what drives it, not write traffic.
        self.assertFalse(service.request_durable_prune_if_due())
        self.assertEqual(len(durable), 3)

        service._vardiff_durable_next_prune_monotonic = time.monotonic() - 1.0
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=2000.0):
            self.assertTrue(service.request_durable_prune_if_due())
            self.assertTrue(service.flush_durable_writes(timeout=5.0))
            for _ in range(500):
                if not durable._entries:
                    break
                time.sleep(0.005)

        self.assertEqual(len(durable), 0)
        self.assertEqual(service.vardiff_durable_pruned_records, 3)

    def test_periodic_prune_runs_when_idle_sweeps_are_disabled(self) -> None:
        pruned = threading.Event()

        class RecordingStore(MemoryWorkerDifficultyStore):
            def prune(self, **kwargs: object) -> int:  # type: ignore[override]
                deleted = super().prune(**kwargs)  # type: ignore[arg-type]
                pruned.set()
                return deleted

        durable = RecordingStore()
        service = self.service(runtime(durable, ttl_seconds=1.0))
        pruned.clear()  # Ignore the service's empty startup-prune call.
        durable.upsert(
            listener="default",
            worker_username="expired",
            difficulty=Decimal("16384"),
            evidence_at_ms=1_000,
            now_ms=1_000,
        )
        service._vardiff_durable_next_prune_monotonic = time.monotonic() - 1.0

        # No idle-sweep runtime fields or thread are involved: the durable
        # lane's scheduler must wake and prune on its own.
        service.start_durable_prune_scheduler()
        service._vardiff_durable_wake.set()

        self.assertTrue(pruned.wait(5.0))
        self.assertEqual(len(durable), 0)

    def test_serve_starts_durable_pruning_before_accepting(self) -> None:
        source = inspect.getsource(PrismCoordinator._serve_with_listener_stack)
        scheduler = source.index("start_durable_prune_scheduler")

        self.assertLess(scheduler, source.index("self.accept_loop"))

    def test_prune_metric_is_a_counter_covering_every_prune(self) -> None:
        service = self.service(runtime(MemoryWorkerDifficultyStore()))
        text = "\n".join(service.metrics_lines())

        self.assertIn(
            "# TYPE qbit_prism_vardiff_durable_pruned_total counter",
            text,
        )
        self.assertIn("qbit_prism_vardiff_durable_pruned_total 0", text)
        self.assertNotIn("qbit_prism_vardiff_durable_pruned gauge", text)
        self.assertIn(
            "# TYPE qbit_prism_vardiff_durable_prune_failures_total counter",
            text,
        )

    def test_step_down_after_shutdown_is_applied_inline_not_dropped(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        live_ms = max(0, int(time.time() * 1000))
        durable.upsert(
            listener="default",
            worker_username="miner-a.rig-1",
            difficulty=Decimal("1048576"),
            evidence_at_ms=live_ms,
            now_ms=live_ms,
        )
        service = self.service(runtime(durable))
        service.shutdown_durable_executor()

        # A disconnect landing after the lane closed still has to correct the
        # stored row; queueing it would strand it past the drain.
        service._enqueue_durable_write(
            _PendingDurableWrite(
                key=("default", "miner-a.rig-1"),
                downward_only=True,
                difficulty=Decimal("65536"),
                evidence_at_ms=None,
                now_ms=live_ms + 100,
            )
        )

        self.assertEqual(
            durable._entries[("default", "miner-a.rig-1")].difficulty,
            Decimal("65536"),
        )
        self.assertEqual(service.vardiff_durable_downward_dropped, 0)
        self.assertEqual(
            service.vardiff_durable_write_outcome_counts["lowered"],
            1,
        )
        # An optional resume hint arriving after the drain is still dropped.
        service._enqueue_durable_write(
            _PendingDurableWrite(
                key=("default", "other.rig"),
                downward_only=False,
                difficulty=Decimal("500000"),
                evidence_at_ms=live_ms,
                now_ms=live_ms,
            )
        )
        self.assertEqual(
            service.vardiff_durable_write_outcome_counts["dropped"],
            1,
        )

    def test_every_prune_statement_carries_a_finite_limit(self) -> None:
        limits: list[object] = []

        class RecordingStore(MemoryWorkerDifficultyStore):
            def prune(self, **kwargs: object) -> int:  # type: ignore[override]
                limits.append(kwargs.get("limit"))
                return super().prune(**kwargs)  # type: ignore[arg-type]

        durable = RecordingStore()
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.0):
            service = self.service(runtime(durable, ttl_seconds=1.0))
        service._vardiff_durable_next_prune_monotonic = time.monotonic() - 1.0
        service.request_durable_prune_if_due()
        service._prune_durable_worker_difficulties_if_requested()

        self.assertTrue(limits)
        for limit in limits:
            self.assertIsInstance(limit, int)
            self.assertGreater(limit, 0)

    def test_prune_batches_until_the_backlog_drains(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.0):
            service = self.service(runtime(durable, ttl_seconds=1.0))
        batches: list[int] = []

        def batched(**kwargs: object) -> int:
            batches.append(int(kwargs["limit"]))  # type: ignore[arg-type]
            # Two full batches, then a short one: the loop must stop there.
            return (
                PRISM_VARDIFF_DURABLE_PRUNE_BATCH
                if len(batches) <= 2
                else 7
            )

        with mock.patch.object(durable, "prune", side_effect=batched):
            pruned = service._prune_expired_rows(1_000)

        self.assertEqual(len(batches), 3)
        self.assertEqual(pruned, 2 * PRISM_VARDIFF_DURABLE_PRUNE_BATCH + 7)
        self.assertEqual(
            batches,
            [PRISM_VARDIFF_DURABLE_PRUNE_BATCH] * 3,
        )

    def test_prune_batching_is_capped_per_pass(self) -> None:
        durable = MemoryWorkerDifficultyStore()
        with mock.patch("lab.prism.vardiff_service.time.time", return_value=1000.0):
            service = self.service(runtime(durable, ttl_seconds=1.0))

        with mock.patch.object(
            durable,
            "prune",
            side_effect=lambda **kwargs: PRISM_VARDIFF_DURABLE_PRUNE_BATCH,
        ) as always_full:
            service._prune_expired_rows(1_000)

        # An endless backlog must not hold the lane for an unbounded pass.
        self.assertEqual(
            always_full.call_count,
            PRISM_VARDIFF_DURABLE_PRUNE_MAX_BATCHES,
        )

    def test_metrics_expose_only_bounded_durable_outcomes(self) -> None:
        service = self.service(runtime(MemoryWorkerDifficultyStore()))
        text = "\n".join(service.metrics_lines())

        self.assertIn("qbit_prism_vardiff_durable_preloaded 0", text)
        for outcome in ("applied", "stale", "failed", "dropped"):
            self.assertIn(
                f'qbit_prism_vardiff_durable_writes_total{{outcome="{outcome}"}} 0',
                text,
            )
        self.assertIn(
            'qbit_prism_vardiff_durable_backend{backend="memory"} 1',
            text,
        )


if __name__ == "__main__":
    unittest.main()

"""Direct tests for the coordinator-facing CTV runtime service."""

from __future__ import annotations

from contextlib import contextmanager
import threading
import unittest
from unittest.mock import patch

from lab.prism.coordinator_shutdown import ShutdownInProgress
from lab.prism.ctv_broadcaster_daemon import (
    CtvFanoutChunkResult,
    CtvFanoutDaemonResult,
)
from lab.prism.ctv_runtime import (
    CTV_BROADCAST_STATE_COMPONENT,
    CtvRuntimeConfig,
    CtvRuntimeService,
)
from lab.prism.prism_coordinator import PrismCoordinator


class StopAfterOnePass:
    def is_set(self) -> bool:
        return False

    def wait(self, _timeout: float) -> bool:
        return True


class PrismCtvRuntimeTests(unittest.TestCase):
    @staticmethod
    def config(**overrides: object) -> CtvRuntimeConfig:
        values: dict[str, object] = {
            "enabled": True,
            "wallet": None,
            "fee_sats": 0,
            "limit": 7,
            "chunk_size": 2,
            "interval_seconds": 30.0,
        }
        values.update(overrides)
        return CtvRuntimeConfig(**values)  # type: ignore[arg-type]

    def test_run_once_constructs_daemon_and_holds_exact_writer_admission(self) -> None:
        ledger = object()
        admission: list[str] = []
        captured: dict[str, object] = {}
        heartbeats: list[bool] = []

        def rpc_call(*_args: object, **_kwargs: object) -> None:
            return None

        class FakeBroadcaster:
            def __init__(self, call: object, *, funding_wallet: str | None) -> None:
                captured["rpc_call"] = call
                captured["wallet"] = funding_wallet

        class FakeDaemon:
            def __init__(
                self,
                daemon_ledger: object,
                broadcaster: object,
                *,
                fee_sats: int,
            ) -> None:
                captured["ledger"] = daemon_ledger
                captured["broadcaster"] = broadcaster
                captured["fee_sats"] = fee_sats

            def run_once(self, **kwargs: object) -> CtvFanoutDaemonResult:
                captured.update(kwargs)
                progress_callback = kwargs["progress_callback"]
                chunk_callback = kwargs["chunk_callback"]
                tip_refresh_pending = kwargs["tip_refresh_pending"]
                assert callable(progress_callback)
                assert callable(chunk_callback)
                assert callable(tip_refresh_pending)
                progress_callback()
                chunk_callback(
                    CtvFanoutChunkResult(processed_count=1, elapsed_seconds=0.25)
                )
                captured["tip_pending"] = tip_refresh_pending()
                return CtvFanoutDaemonResult(1, 0, 1, 0)

        @contextmanager
        def writer_admission(component: str):
            admission.append(f"enter:{component}")
            try:
                yield
            finally:
                admission.append(f"exit:{component}")

        service = CtvRuntimeService(
            rpc_call=rpc_call,
            ledger=ledger,
            writer_admission=writer_admission,
            tip_refresh_pending=lambda: False,
            heartbeat=lambda: heartbeats.append(True),
            stop_event=threading.Event(),
            config=self.config(wallet="fee-wallet", fee_sats=900),
            daemon_type=FakeDaemon,  # type: ignore[arg-type]
            broadcaster_type=FakeBroadcaster,  # type: ignore[arg-type]
        )

        result = service.run_once(progress_callback=service.record_progress)

        self.assertEqual(result.updated_count, 1)
        self.assertEqual(
            admission,
            [
                f"enter:{CTV_BROADCAST_STATE_COMPONENT}",
                f"exit:{CTV_BROADCAST_STATE_COMPONENT}",
            ],
        )
        self.assertIs(captured["ledger"], ledger)
        self.assertIs(captured["rpc_call"], rpc_call)
        self.assertEqual(captured["wallet"], "fee-wallet")
        self.assertEqual(captured["fee_sats"], 900)
        self.assertEqual(captured["limit"], 7)
        self.assertEqual(captured["chunk_size"], 2)
        self.assertFalse(captured["tip_pending"])
        self.assertEqual(len(heartbeats), 2)
        metrics = "\n".join(service.metrics_lines())
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_processed_rows_total 1",
            metrics,
        )
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_chunk_seconds_sum 0.250000",
            metrics,
        )

    def test_closed_writer_admission_does_not_construct_daemon(self) -> None:
        constructed: list[bool] = []

        class RejectingAdmission:
            def __enter__(self) -> None:
                raise ShutdownInProgress("closed")

            def __exit__(self, *_args: object) -> None:
                return None

        class UnexpectedDaemon:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                constructed.append(True)

        service = CtvRuntimeService(
            rpc_call=lambda *_args, **_kwargs: None,
            ledger=object(),
            writer_admission=lambda _component: RejectingAdmission(),
            tip_refresh_pending=lambda: False,
            heartbeat=lambda: None,
            stop_event=threading.Event(),
            config=self.config(),
            daemon_type=UnexpectedDaemon,  # type: ignore[arg-type]
        )

        with self.assertRaises(ShutdownInProgress):
            service.run_once()

        self.assertEqual(constructed, [])

    def test_loop_records_completion_before_wait_and_preserves_summary_and_spec(self) -> None:
        clock_values = iter((10.0, 112.0))
        heartbeats: list[float] = []
        wait_observation: dict[str, object] = {}

        class ObservedStop(StopAfterOnePass):
            def wait(self, timeout: float) -> bool:
                wait_observation["timeout"] = timeout
                wait_observation["pass_count"] = service.pass_count
                return True

        @contextmanager
        def writer_admission(_component: str):
            yield

        class OnePassDaemon:
            def run_once(self, **_kwargs: object) -> CtvFanoutDaemonResult:
                return CtvFanoutDaemonResult(0, 0, 0, 0, True)

        service = CtvRuntimeService(
            rpc_call=lambda *_args, **_kwargs: None,
            ledger=object(),
            writer_admission=writer_admission,
            tip_refresh_pending=lambda: False,
            heartbeat=lambda: heartbeats.append(1.0),
            stop_event=ObservedStop(),
            config=self.config(),
            monotonic=lambda: next(clock_values),
        )
        service.daemon = OnePassDaemon()  # type: ignore[assignment]

        service.loop()

        self.assertEqual(wait_observation, {"timeout": 30.0, "pass_count": 1})
        self.assertEqual(len(heartbeats), 2)
        metrics = "\n".join(service.metrics_lines())
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_pass_seconds_sum 102.000000",
            metrics,
        )
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total 1",
            metrics,
        )
        specification = service.background_service_spec()
        self.assertEqual(specification.name, "ctv_fanout_broadcaster")
        self.assertEqual(specification.thread_name, "prism-ctv-fanout-broadcaster")
        self.assertEqual(specification.join_timeout, 1.0)
        self.assertTrue(specification.watchdog_monitored)
        self.assertEqual(
            service.startup_summary(),
            "prism coordinator: CTV fanout broadcaster enabled "
            "mode=direct fee_bits=0 wallet=none interval=30s limit=7 chunk_size=2",
        )

    def test_cpfp_wallet_validation_remains_at_daemon_construction(self) -> None:
        @contextmanager
        def writer_admission(_component: str):
            yield

        service = CtvRuntimeService(
            rpc_call=lambda *_args, **_kwargs: None,
            ledger=object(),
            writer_admission=writer_admission,
            tip_refresh_pending=lambda: False,
            heartbeat=lambda: None,
            stop_event=threading.Event(),
            config=self.config(wallet=None, fee_sats=1),
        )

        with self.assertRaisesRegex(
            ValueError,
            "ctv_broadcaster_wallet is required",
        ):
            service.make_daemon()

    def test_lazy_handoff_consumes_overrides_and_updates_only_live_config(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.ctv_broadcaster_limit = 7

        self.assertEqual(
            server.__dict__["_ctv_runtime_compat_config"],
            {"limit": 7},
        )

        runtime = server._ensure_ctv_runtime()

        self.assertEqual(runtime.config.limit, 7)
        self.assertNotIn("_ctv_runtime_compat_config", server.__dict__)

        server.ctv_broadcaster_limit = 11

        self.assertIs(server._ensure_ctv_runtime(), runtime)
        self.assertEqual(runtime.config.limit, 11)
        self.assertNotIn("_ctv_runtime_compat_config", server.__dict__)
        self.assertEqual(server._legacy_ctv_runtime_config().limit, 100)

    def test_wallet_or_fee_config_change_rebuilds_daemon_for_next_pass(self) -> None:
        constructions: list[tuple[str | None, int]] = []
        runs: list[tuple[int, int]] = []

        class FakeBroadcaster:
            def __init__(
                self,
                _rpc_call: object,
                *,
                funding_wallet: str | None,
            ) -> None:
                self.wallet = funding_wallet

        class FakeDaemon:
            def __init__(
                self,
                _ledger: object,
                broadcaster: FakeBroadcaster,
                *,
                fee_sats: int,
            ) -> None:
                constructions.append((broadcaster.wallet, fee_sats))

            def run_once(self, **kwargs: object) -> CtvFanoutDaemonResult:
                runs.append((int(kwargs["limit"]), int(kwargs["chunk_size"])))
                return CtvFanoutDaemonResult(0, 0, 0, 0)

        @contextmanager
        def writer_admission(_component: str):
            yield

        service = CtvRuntimeService(
            rpc_call=lambda *_args, **_kwargs: None,
            ledger=object(),
            writer_admission=writer_admission,
            tip_refresh_pending=lambda: False,
            heartbeat=lambda: None,
            stop_event=threading.Event(),
            config=self.config(),
            daemon_type=FakeDaemon,  # type: ignore[arg-type]
            broadcaster_type=FakeBroadcaster,  # type: ignore[arg-type]
        )

        service.run_once()
        first = service.daemon
        service.replace_config(limit=11, chunk_size=4)
        service.run_once()
        self.assertIs(service.daemon, first)

        service.replace_config(wallet="fee-wallet", fee_sats=900)
        service.run_once()

        self.assertIsNot(service.daemon, first)
        self.assertEqual(constructions, [(None, 0), ("fee-wallet", 900)])
        self.assertEqual(runs, [(7, 2), (11, 4), (11, 4)])

    def test_concurrent_live_config_updates_retain_both_frozen_fields(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        runtime = server._ensure_ctv_runtime()
        start = threading.Barrier(3)
        errors: list[BaseException] = []

        def update_limit() -> None:
            try:
                start.wait(timeout=2.0)
                server.ctv_broadcaster_limit = 17
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        def update_chunk_size() -> None:
            try:
                start.wait(timeout=2.0)
                server.ctv_broadcaster_chunk_size = 3
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [
            threading.Thread(target=update_limit),
            threading.Thread(target=update_chunk_size),
        ]
        for thread in threads:
            thread.start()
        start.wait(timeout=2.0)
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        snapshot = runtime.config
        self.assertEqual(snapshot.limit, 17)
        self.assertEqual(snapshot.chunk_size, 3)

    def test_lazy_handoff_is_singleton_under_deterministic_contention(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        arrivals = threading.Barrier(2)
        underlying_lock = threading.Lock()
        candidates: list[CtvRuntimeService] = []
        results: list[CtvRuntimeService] = []
        errors: list[BaseException] = []

        class CoordinatedInitLock:
            def __enter__(self) -> None:
                arrivals.wait(timeout=2.0)
                underlying_lock.acquire()

            def __exit__(self, *_args: object) -> None:
                underlying_lock.release()

        server._ctv_runtime_init_lock = CoordinatedInitLock()  # type: ignore[assignment]
        original_make = server._make_ctv_runtime_service

        def counted_make(
            config: CtvRuntimeConfig | None = None,
        ) -> CtvRuntimeService:
            candidate = original_make(config)
            candidates.append(candidate)
            return candidate

        server._make_ctv_runtime_service = counted_make  # type: ignore[method-assign]

        def ensure_runtime() -> None:
            try:
                results.append(server._ensure_ctv_runtime())
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=ensure_runtime) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(len(results), 2)
        self.assertIs(results[0], candidates[0])
        self.assertIs(results[1], candidates[0])
        marker = object()
        results[0].daemon = marker  # type: ignore[assignment]
        self.assertIs(results[1].daemon, marker)
        results[0].record_progress()
        results[1].record_progress()
        self.assertEqual(results[0].processed_rows_total, 2)

    def test_daemon_getter_waits_for_paused_lazy_handoff_publish(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        daemon_marker = object()
        server.ctv_fanout_broadcast_daemon = daemon_marker  # type: ignore[assignment]
        underlying_lock = threading.Lock()
        getter_attempted = threading.Event()
        factory_popped_compat = threading.Event()
        release_factory = threading.Event()
        getter_returned = threading.Event()
        runtime_results: list[CtvRuntimeService] = []
        daemon_results: list[object | None] = []
        errors: list[BaseException] = []

        class ObservedInitLock:
            def __enter__(self) -> None:
                if threading.current_thread().name == "ctv-daemon-getter":
                    getter_attempted.set()
                underlying_lock.acquire()

            def __exit__(self, *_args: object) -> None:
                underlying_lock.release()

        server._ctv_runtime_init_lock = ObservedInitLock()  # type: ignore[assignment]
        original_make = server._make_ctv_runtime_service

        def paused_make(
            config: CtvRuntimeConfig | None = None,
        ) -> CtvRuntimeService:
            runtime = original_make(config)
            if "_ctv_runtime_compat_daemon" in server.__dict__:
                raise AssertionError("compat daemon was not consumed")
            factory_popped_compat.set()
            if not release_factory.wait(timeout=2.0):
                raise AssertionError("test did not release runtime publication")
            return runtime

        server._make_ctv_runtime_service = paused_make  # type: ignore[method-assign]

        def initialize() -> None:
            try:
                runtime_results.append(server._ensure_ctv_runtime())
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        def read_daemon() -> None:
            try:
                daemon_results.append(server.ctv_fanout_broadcast_daemon)
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                getter_returned.set()

        initializer = threading.Thread(target=initialize, name="ctv-runtime-initializer")
        getter = threading.Thread(target=read_daemon, name="ctv-daemon-getter")
        initializer.start()
        self.assertTrue(factory_popped_compat.wait(timeout=2.0))
        getter.start()
        self.assertTrue(getter_attempted.wait(timeout=2.0))
        self.assertFalse(getter_returned.is_set())

        release_factory.set()
        initializer.join(timeout=2.0)
        getter.join(timeout=2.0)

        self.assertFalse(initializer.is_alive())
        self.assertFalse(getter.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(runtime_results), 1)
        self.assertEqual(daemon_results, [daemon_marker])
        self.assertIs(runtime_results[0].daemon, daemon_marker)
        self.assertNotIn("_ctv_runtime_compat_daemon", server.__dict__)

    def test_coordinator_facade_routes_every_retained_loop_patch_seam(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.stop_event = StopAfterOnePass()  # type: ignore[assignment]
        server.ctv_broadcaster_limit = 7
        server.ctv_broadcaster_chunk_size = 2
        server.ctv_broadcaster_interval_seconds = 30.0
        server.tip_refresh_is_pending = lambda: False  # type: ignore[method-assign]
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        events: list[object] = []

        @contextmanager
        def writer_admission(component: str):
            events.append(("admit", component))
            yield

        server._writer_operation = writer_admission  # type: ignore[method-assign]

        class FacadeDaemon:
            def run_once(self, **kwargs: object) -> CtvFanoutDaemonResult:
                progress_callback = kwargs["progress_callback"]
                chunk_callback = kwargs["chunk_callback"]
                assert callable(progress_callback)
                assert callable(chunk_callback)
                progress_callback()
                chunk_callback(
                    CtvFanoutChunkResult(processed_count=2, elapsed_seconds=0.25)
                )
                return CtvFanoutDaemonResult(2, 0, 2, 0, True)

        server.ctv_fanout_broadcast_daemon = FacadeDaemon()  # type: ignore[assignment]
        server._record_ctv_fanout_broadcaster_progress = (  # type: ignore[method-assign]
            lambda: events.append("progress")
        )
        server.observe_ctv_fanout_broadcaster_chunk = (  # type: ignore[method-assign]
            lambda result: events.append(("chunk", result.processed_count))
        )
        server.observe_ctv_fanout_broadcaster_pass = (  # type: ignore[method-assign]
            lambda elapsed: events.append(("pass", elapsed))
        )
        server._record_ctv_fanout_broadcaster_yield = (  # type: ignore[method-assign]
            lambda: events.append("yield")
        )
        original_run_once = server.run_ctv_fanout_broadcaster_once

        def patched_run_once(**kwargs: object) -> CtvFanoutDaemonResult:
            events.append("run_once")
            return original_run_once(**kwargs)  # type: ignore[arg-type]

        server.run_ctv_fanout_broadcaster_once = patched_run_once  # type: ignore[method-assign]

        with patch("builtins.print"):
            server.ctv_fanout_broadcaster_loop()

        self.assertEqual(
            [event if isinstance(event, str) else event[0] for event in events],
            ["run_once", "admit", "progress", "chunk", "pass", "yield"],
        )
        self.assertEqual(events[1], ("admit", CTV_BROADCAST_STATE_COMPONENT))
        runtime = server._ensure_ctv_runtime()
        specification = runtime.background_service_spec()
        self.assertIs(getattr(specification.target, "__self__", None), runtime)
        self.assertIs(getattr(specification.target, "__func__", None), CtvRuntimeService.loop)


if __name__ == "__main__":
    unittest.main()

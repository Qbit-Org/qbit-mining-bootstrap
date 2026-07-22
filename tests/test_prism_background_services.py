#!/usr/bin/env python3

from __future__ import annotations

import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from lab.prism.background_services import (
    BackgroundServiceRegistry,
    BackgroundServiceSpec,
    WatchdogPorts,
    WatchdogService,
)
from lab.prism.prism_coordinator import PrismCoordinator


class DormantThread:
    def __init__(
        self,
        *,
        target: object,
        name: str,
        daemon: bool,
    ) -> None:
        self.target = target
        self.name = name
        self.daemon = daemon
        self.start_count = 0
        self.join_timeouts: list[float | None] = []

    def start(self) -> None:
        self.start_count += 1

    def join(self, timeout: float | None = None) -> None:
        self.join_timeouts.append(timeout)


class FlakyThreadFactory:
    def __init__(self) -> None:
        self.attempts = 0
        self.threads: list[DormantThread] = []

    def __call__(self, **kwargs: object) -> DormantThread:
        factory = self

        class FlakyDormantThread(DormantThread):
            def start(self) -> None:
                factory.attempts += 1
                if factory.attempts == 1:
                    raise RuntimeError("thread start failed")
                super().start()

        thread = FlakyDormantThread(**kwargs)  # type: ignore[arg-type]
        self.threads.append(thread)
        return thread


class WatchdogServiceTests(unittest.TestCase):
    def test_publication_failure_exits_after_one_bounded_wait(self) -> None:
        events: list[object] = []
        service = WatchdogService(
            WatchdogPorts(
                wait_for_stop=lambda timeout: events.append(("wait", timeout))
                or False,
                interval_seconds=lambda: 2.0,
                monotonic=lambda: 10.0,
                publication_failure_expired=lambda now: now == 10.0,
                publication_budget_seconds=lambda: 30.0,
                liveness_enabled=lambda: True,
                overdue_heartbeats=lambda _now: ["worker"],
                liveness_timeout_seconds=lambda: 60.0,
                log=lambda message: events.append(message),
                exit_process=lambda code: events.append(("exit", code)),
            )
        )

        service.run()

        self.assertEqual(events[0], ("wait", 2.0))
        self.assertIn("publication-progress watchdog firing", str(events[1]))
        self.assertEqual(events[2], ("exit", 1))

    def test_requested_stop_does_not_sample_or_exit(self) -> None:
        service = WatchdogService(
            WatchdogPorts(
                wait_for_stop=lambda _timeout: True,
                interval_seconds=lambda: 1.0,
                monotonic=lambda: self.fail("stopped watchdog sampled time"),
                publication_failure_expired=lambda _now: False,
                publication_budget_seconds=lambda: 1.0,
                liveness_enabled=lambda: True,
                overdue_heartbeats=lambda _now: [],
                liveness_timeout_seconds=lambda: 1.0,
                log=lambda _message: self.fail("stopped watchdog logged"),
                exit_process=lambda _code: self.fail("stopped watchdog exited"),
            )
        )

        service.run()


class ContentionObservedLock:
    """Lock that exposes an acquire attempt made while another caller owns it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.contended = threading.Event()

    def __enter__(self) -> ContentionObservedLock:
        if self._lock.locked():
            self.contended.set()
        self._lock.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self._lock.release()


def specification(
    name: str,
    *,
    join_timeout: float = 1.0,
    watchdog_monitored: bool = False,
) -> BackgroundServiceSpec:
    return BackgroundServiceSpec(
        name=name,
        thread_name=f"prism-{name}",
        target=lambda: None,
        daemon=True,
        join_timeout=join_timeout,
        watchdog_monitored=watchdog_monitored,
    )


class BackgroundServiceRegistryTests(unittest.TestCase):
    def test_named_start_is_idempotent_and_retains_exact_thread(self) -> None:
        registry = BackgroundServiceRegistry(
            [specification("poll", watchdog_monitored=True)],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )

        first = registry.start("poll")
        second = registry.start("poll")
        snapshot = registry.snapshot("poll")

        self.assertIs(first, second)
        self.assertIs(snapshot.thread, first)
        self.assertTrue(snapshot.started)
        self.assertEqual(first.start_count, 1)  # type: ignore[attr-defined]
        self.assertEqual(first.name, "prism-poll")
        self.assertTrue(first.daemon)

    def test_drain_threads_are_only_started_services_in_registration_order(self) -> None:
        registry = BackgroundServiceRegistry(
            [
                specification("poll", join_timeout=1.0),
                specification("writer", join_timeout=5.0),
                specification("optional", join_timeout=2.0),
            ],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )
        poll = registry.start("poll")
        writer = registry.start("writer")

        self.assertEqual(
            registry.threads_to_drain(),
            ((poll, 1.0), (writer, 5.0)),
        )

    def test_watchdog_names_derive_from_the_same_service_records(self) -> None:
        registry = BackgroundServiceRegistry(
            [
                specification("poll", watchdog_monitored=True),
                specification("health"),
                specification("writer", watchdog_monitored=True),
            ],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )

        registry.start("writer")

        self.assertEqual(registry.watchdog_service_names(), ("poll", "writer"))
        self.assertEqual(
            registry.watchdog_service_names(started_only=True),
            ("writer",),
        )

    def test_duplicate_service_or_thread_names_are_rejected(self) -> None:
        registry = BackgroundServiceRegistry([specification("poll")])

        with self.assertRaisesRegex(ValueError, "already registered: poll"):
            registry.register(specification("poll"))
        with self.assertRaisesRegex(ValueError, "thread name is already registered"):
            registry.register(
                BackgroundServiceSpec(
                    name="other",
                    thread_name="prism-poll",
                    target=lambda: None,
                    daemon=True,
                    join_timeout=1.0,
                    watchdog_monitored=False,
                )
            )

    def test_start_failure_rolls_back_and_retry_runs_start_hook_once(self) -> None:
        factory = FlakyThreadFactory()
        registry = BackgroundServiceRegistry(
            [specification("poll", watchdog_monitored=True)],
            thread_factory=factory,
        )
        started: list[str] = []

        with self.assertRaisesRegex(RuntimeError, "thread start failed"):
            registry.start(
                "poll",
                on_started=lambda service: started.append(service.name),
            )

        failed = registry.snapshot("poll")
        self.assertFalse(failed.started)
        self.assertIsNone(failed.thread)
        self.assertEqual(started, [])

        thread = registry.start(
            "poll",
            on_started=lambda service: started.append(service.name),
        )

        self.assertEqual(factory.attempts, 2)
        self.assertIs(thread, factory.threads[1])
        self.assertEqual(started, ["poll"])

    def test_started_thread_retries_a_failed_start_hook_without_restarting(self) -> None:
        registry = BackgroundServiceRegistry(
            [specification("poll", watchdog_monitored=True)],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )
        hook_attempts = 0

        def flaky_hook(_service: BackgroundServiceSpec) -> None:
            nonlocal hook_attempts
            hook_attempts += 1
            if hook_attempts == 1:
                raise RuntimeError("start hook failed")

        with self.assertRaisesRegex(RuntimeError, "start hook failed"):
            registry.start("poll", on_started=flaky_hook)

        started = registry.snapshot("poll")
        self.assertTrue(started.started)
        self.assertIsNotNone(started.thread)

        retried = registry.start("poll", on_started=flaky_hook)

        self.assertIs(retried, started.thread)
        self.assertEqual(retried.start_count, 1)  # type: ignore[attr-defined]
        self.assertEqual(hook_attempts, 2)

    def test_dynamic_registration_is_equivalent_or_fails_clearly(self) -> None:
        def target() -> None:
            return None

        registered = specification("dynamic")
        registered = BackgroundServiceSpec(
            name=registered.name,
            thread_name=registered.thread_name,
            target=target,
            daemon=registered.daemon,
            join_timeout=registered.join_timeout,
            watchdog_monitored=registered.watchdog_monitored,
            registration_identity=("dynamic", 1),
        )
        registry = BackgroundServiceRegistry()

        self.assertTrue(registry.register_if_absent(registered))
        self.assertFalse(
            registry.register_if_absent(
                BackgroundServiceSpec(
                    name="dynamic",
                    thread_name="prism-dynamic",
                    target=lambda: None,
                    daemon=True,
                    join_timeout=1.0,
                    watchdog_monitored=False,
                    registration_identity=("dynamic", 1),
                )
            )
        )
        with self.assertRaisesRegex(ValueError, "incompatible.*dynamic"):
            registry.register_if_absent(
                BackgroundServiceSpec(
                    name="dynamic",
                    thread_name="prism-dynamic",
                    target=target,
                    daemon=True,
                    join_timeout=1.0,
                    watchdog_monitored=False,
                    registration_identity=("dynamic", 2),
                )
            )

    def test_post_start_registration_keeps_registration_order_for_drain(self) -> None:
        registry = BackgroundServiceRegistry(
            [specification("first")],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )
        first = registry.start("first")
        registry.register(specification("second", join_timeout=2.0))
        registry.register(specification("third", join_timeout=3.0))
        third = registry.start("third")
        second = registry.start("second")

        self.assertEqual(
            registry.threads_to_drain(),
            ((first, 1.0), (second, 2.0), (third, 3.0)),
        )


class CoordinatorBackgroundServiceIntegrationTests(unittest.TestCase):
    @staticmethod
    def coordinator_with_optional_services(enabled: bool) -> PrismCoordinator:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.blockwait_enabled = enabled
        server.vardiff_idle_sweep_seconds = 1.0 if enabled else 0.0
        server.stratum_initial_job_timeout_seconds = 1.0 if enabled else 0.0
        server.ctv_broadcaster_enabled = enabled
        server.watchdog_enabled = enabled
        server.audit_bind = "127.0.0.1" if enabled else None
        server.audit_port = 8080 if enabled else 0
        return server

    def test_optional_process_services_are_absent_when_disabled(self) -> None:
        server = self.coordinator_with_optional_services(False)

        registry = server._make_background_service_registry()

        self.assertEqual(
            registry.service_names(),
            ("qbit_blockpoll", "block_submitter", "share_writer"),
        )

    def test_audit_http_starts_only_after_both_initial_snapshot_attempts(
        self,
    ) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.audit_bind = "127.0.0.1"
        server.audit_port = 3341
        events: list[str] = []
        server.start_health_snapshot_refresher = lambda: events.append(  # type: ignore[method-assign]
            "health"
        )
        server.start_metrics_snapshot_refresher = lambda: events.append(  # type: ignore[method-assign]
            "metrics"
        )
        server._ensure_audit_http_facade = lambda: SimpleNamespace(  # type: ignore[method-assign]
            start=lambda: events.append("http")
        )

        with patch("builtins.print"):
            server.start_audit_server()

        self.assertEqual(events, ["health", "metrics", "http"])

    def test_process_service_specs_preserve_names_and_join_order(self) -> None:
        server = self.coordinator_with_optional_services(True)

        registry = server._make_background_service_registry()

        self.assertEqual(
            registry.service_names(),
            (
                "qbit_blockpoll",
                "block_submitter",
                "qbit_blockwait",
                "vardiff_idle_sweep",
                "initial_job_timeout_sweep",
                "share_writer",
                "ctv_fanout_broadcaster",
                "watchdog",
                "health_snapshot_refresher",
                "metrics_snapshot_refresher",
            ),
        )
        expected = {
            "qbit_blockpoll": ("prism-qbit-block-poll", 1.0, True),
            "block_submitter": ("prism-block-submitter", 1.0, True),
            "qbit_blockwait": ("prism-qbit-block-wait", 1.0, True),
            "vardiff_idle_sweep": ("prism-vardiff-idle-sweep", 1.0, True),
            "initial_job_timeout_sweep": ("prism-initial-job-timeouts", 1.0, False),
            "share_writer": ("prism-share-writer", 5.0, True),
            "ctv_fanout_broadcaster": (
                "prism-ctv-fanout-broadcaster",
                1.0,
                True,
            ),
            "watchdog": ("prism-watchdog", 1.0, False),
            "health_snapshot_refresher": (
                "prism-health-snapshot-refresher",
                1.0,
                False,
            ),
            "metrics_snapshot_refresher": (
                "prism-metrics-snapshot-refresher",
                1.0,
                False,
            ),
        }
        for name, properties in expected.items():
            service = registry.snapshot(name).specification
            self.assertEqual(
                (service.thread_name, service.join_timeout, service.watchdog_monitored),
                properties,
            )

    def test_monitored_service_start_seeds_its_own_watchdog_key(self) -> None:
        server = self.coordinator_with_optional_services(False)
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server._background_services = BackgroundServiceRegistry(
            [specification("tracked", watchdog_monitored=True)],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )

        server._start_background_service("tracked")

        self.assertEqual(tuple(server._heartbeats), ("tracked",))
        server._heartbeats["tracked"] = 1.0
        server._start_background_service("tracked")
        self.assertEqual(server._heartbeats["tracked"], 1.0)

    def test_concurrent_wrapper_starts_seed_one_heartbeat_and_one_thread(self) -> None:
        server = self.coordinator_with_optional_services(False)
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server._background_services = BackgroundServiceRegistry(
            [specification("tracked", watchdog_monitored=True)],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )
        observed_registry_lock = ContentionObservedLock()
        server._background_services._lock = observed_registry_lock  # type: ignore[assignment]
        heartbeat_entered = threading.Event()
        release_heartbeat = threading.Event()
        heartbeat_names: list[str] = []
        results: list[object] = []
        errors: list[BaseException] = []

        def blocked_heartbeat(name: str) -> None:
            heartbeat_names.append(name)
            heartbeat_entered.set()
            if not release_heartbeat.wait(5):
                raise AssertionError("heartbeat test interleaving timed out")

        server._record_heartbeat = blocked_heartbeat  # type: ignore[method-assign]

        def start() -> None:
            try:
                results.append(server._start_background_service("tracked"))
            except BaseException as exc:
                errors.append(exc)

        first = threading.Thread(target=start)
        second = threading.Thread(target=start)
        first.start()
        self.assertTrue(heartbeat_entered.wait(5))
        second.start()
        second_contended_inside_start = observed_registry_lock.contended.wait(5)
        release_heartbeat.set()
        first.join(5)
        second.join(5)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertTrue(second_contended_inside_start)
        self.assertEqual(errors, [])
        self.assertEqual(heartbeat_names, ["tracked"])
        self.assertEqual(len(results), 2)
        self.assertIs(results[0], results[1])
        self.assertEqual(results[0].start_count, 1)  # type: ignore[union-attr]

    def test_wrapper_start_failure_rolls_back_heartbeat_and_retries_cleanly(self) -> None:
        server = self.coordinator_with_optional_services(False)
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        factory = FlakyThreadFactory()
        server._background_services = BackgroundServiceRegistry(
            [specification("tracked", watchdog_monitored=True)],
            thread_factory=factory,
        )

        with self.assertRaisesRegex(RuntimeError, "thread start failed"):
            server._start_background_service("tracked")

        self.assertEqual(server._heartbeats, {})
        self.assertFalse(server._background_services.snapshot("tracked").started)

        thread = server._start_background_service("tracked")

        self.assertIs(thread, factory.threads[1])
        self.assertEqual(tuple(server._heartbeats), ("tracked",))
        self.assertTrue(server._background_services.snapshot("tracked").started)

    def test_concurrent_secondary_starts_register_and_start_once(self) -> None:
        server = self.coordinator_with_optional_services(False)
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server._background_services = BackgroundServiceRegistry(
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )
        profile = SimpleNamespace(
            heartbeat_name="stratum_accept_highdiff",
            name="highdiff",
        )
        listener = SimpleNamespace()
        registration_barrier = threading.Barrier(2)
        original_register = server._background_services.register_if_absent

        def synchronized_register(service: BackgroundServiceSpec) -> bool:
            registration_barrier.wait(timeout=5)
            return original_register(service)

        server._background_services.register_if_absent = synchronized_register  # type: ignore[method-assign]
        heartbeat_names: list[str] = []
        server._record_heartbeat = heartbeat_names.append  # type: ignore[method-assign]
        results: list[object] = []
        errors: list[BaseException] = []

        def start() -> None:
            try:
                results.append(
                    server._start_secondary_accept_service(  # type: ignore[arg-type]
                        listener,
                        profile,
                    )
                )
            except BaseException as exc:
                errors.append(exc)

        callers = [threading.Thread(target=start) for _ in range(2)]
        for caller in callers:
            caller.start()
        for caller in callers:
            caller.join(5)

        self.assertTrue(all(not caller.is_alive() for caller in callers))
        self.assertEqual(errors, [])
        self.assertEqual(heartbeat_names, ["stratum_accept_highdiff"])
        self.assertEqual(len(results), 2)
        self.assertIs(results[0], results[1])
        self.assertEqual(results[0].start_count, 1)  # type: ignore[union-attr]
        self.assertEqual(
            server._background_services.service_names(),
            ("stratum_accept_highdiff",),
        )

    def test_secondary_listener_is_named_monitored_and_bounded_for_drain(self) -> None:
        server = self.coordinator_with_optional_services(False)
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server._background_services = BackgroundServiceRegistry(
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )
        profile = SimpleNamespace(
            heartbeat_name="stratum_accept_highdiff",
            name="highdiff",
        )

        thread = server._start_secondary_accept_service(  # type: ignore[arg-type]
            SimpleNamespace(),
            profile,
        )

        snapshot = server._background_services.snapshot("stratum_accept_highdiff")
        self.assertEqual(thread.name, "prism-stratum-accept-highdiff")
        self.assertTrue(snapshot.specification.watchdog_monitored)
        self.assertEqual(snapshot.specification.join_timeout, 1.0)
        self.assertEqual(tuple(server._heartbeats), ("stratum_accept_highdiff",))
        self.assertEqual(
            server._background_services.threads_to_drain(),
            ((thread, 1.0),),
        )

    def test_health_loop_always_clears_running_flag(self) -> None:
        server = self.coordinator_with_optional_services(False)
        server.stop_event = threading.Event()
        server.stop_event.set()
        service = server._ensure_observability_service()
        service.replace_lock_for_test(threading.RLock())
        service.set_loop_running_for_test(True)

        server.health_snapshot_loop()

        self.assertFalse(service.state().health_refresh_loop_running)


if __name__ == "__main__":
    unittest.main()

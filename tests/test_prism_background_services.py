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


def make_watchdog_ports(**overrides: object) -> WatchdogPorts:
    """Inert watchdog ports; individual tests override what they exercise."""
    ports: dict[str, object] = {
        "wait_for_stop": lambda _timeout: True,
        "interval_seconds": lambda: 1.0,
        "fatal_exit_requested": lambda: False,
        "publication_state": lambda _now: (None, 0.0, 1.0, 1.0),
        "hard_exit": lambda _reason: None,
        "liveness_enabled": lambda: True,
        "overdue_heartbeats": lambda _now: [],
        "liveness_timeout_seconds": lambda: 60.0,
        "coordination_budget_seconds": lambda: 900.0,
        "publication_budget_seconds": lambda: 30.0,
        "ensure_job_cache_state": lambda: None,
        "publication_failure_expired": lambda _now: False,
        "publication_divergence_since": lambda: None,
        "lease_release_timeout_seconds": lambda: 0.05,
        "shutdown_controller": lambda: None,
        "request_shutdown": lambda: None,
        "release_ledger_lease": lambda _deadline: True,
        "lease_failure_reason": lambda: None,
        "exit_process": lambda _code: None,
        "log": lambda _message: None,
    }
    ports.update(overrides)
    return WatchdogPorts(**ports)  # type: ignore[arg-type]


class WatchdogServiceTests(unittest.TestCase):
    def test_publication_failure_exits_after_one_bounded_wait(self) -> None:
        events: list[object] = []

        def hard_exit(reason: str) -> None:
            events.append(("hard_exit", reason))
            raise SystemExit(1)

        service = WatchdogService(
            make_watchdog_ports(
                wait_for_stop=lambda timeout: events.append(("wait", timeout))
                or False,
                interval_seconds=lambda: 2.0,
                publication_state=lambda _now: ("publication", 0.0, 900.0, 30.0),
                hard_exit=hard_exit,
            )
        )

        with self.assertRaises(SystemExit):
            service.run()

        self.assertEqual(events[0], ("wait", 2.0))
        self.assertEqual(events[1], ("hard_exit", "publication"))
        self.assertIn(
            "publication-progress watchdog firing",
            str(service.failure_detail),
        )

    def test_coordination_classification_precedes_publication(self) -> None:
        events: list[object] = []

        def hard_exit(reason: str) -> None:
            events.append(("hard_exit", reason))
            raise SystemExit(1)

        service = WatchdogService(
            make_watchdog_ports(
                wait_for_stop=lambda _timeout: False,
                publication_state=lambda _now: (
                    "coordination",
                    12.5,
                    10.0,
                    30.0,
                ),
                hard_exit=hard_exit,
            )
        )

        with self.assertRaises(SystemExit):
            service.run()

        self.assertEqual(events, [("hard_exit", "coordination")])
        self.assertIn("coordination-blocked", str(service.failure_detail))
        self.assertIn("streak_age=12.500s", str(service.failure_detail))

    def test_requested_stop_does_not_sample_or_exit(self) -> None:
        service = WatchdogService(
            make_watchdog_ports(
                wait_for_stop=lambda _timeout: True,
                fatal_exit_requested=lambda: False,
                publication_state=lambda _now: self.fail(
                    "stopped watchdog sampled publication state"
                ),
                hard_exit=lambda _reason: self.fail("stopped watchdog exited"),
                exit_process=lambda _code: self.fail("stopped watchdog exited"),
                log=lambda _message: self.fail("stopped watchdog logged"),
            )
        )

        service.run()

    def test_fatal_stop_request_exits_nonzero(self) -> None:
        events: list[object] = []
        service = WatchdogService(
            make_watchdog_ports(
                wait_for_stop=lambda _timeout: True,
                fatal_exit_requested=lambda: True,
                exit_process=lambda code: events.append(("exit", code)),
                log=lambda message: events.append(("log", message)),
            )
        )

        service.run()

        self.assertEqual(events[0][0], "log")
        self.assertIn("fatal block-work restart requested", str(events[0][1]))
        self.assertEqual(events[1], ("exit", 1))

    def test_claimed_coordination_exit_is_terminal(self) -> None:
        service = WatchdogService(
            make_watchdog_ports(coordination_budget_seconds=lambda: 5.0)
        )
        service.record_coordination_blocked_refresh(100.0)
        state = service.publication_watchdog_state(106.0)
        self.assertEqual(state[0], "coordination")
        # A refresh completing after the claim cannot cancel the exit.
        service.clear_coordination_blocked_streak()
        self.assertGreater(
            service.coordination_blocked_streak_age_seconds(107.0),
            0.0,
        )

    def test_publication_recheck_uses_divergence_snapshot(self) -> None:
        # The cheap preflight fired, but the locked divergence recheck shows
        # no divergence old enough: no exit may be claimed.
        service = WatchdogService(
            make_watchdog_ports(
                publication_failure_expired=lambda _now: True,
                publication_divergence_since=lambda: None,
            )
        )
        state = service.publication_watchdog_state(100.0)
        self.assertIsNone(state[0])
        divergent = WatchdogService(
            make_watchdog_ports(
                publication_budget_seconds=lambda: 30.0,
                publication_failure_expired=lambda _now: True,
                publication_divergence_since=lambda: 50.0,
            )
        )
        self.assertEqual(divergent.publication_watchdog_state(100.0)[0], "publication")

    def test_hard_exit_bounds_release_and_exits_unconditionally(self) -> None:
        events: list[object] = []
        controller = SimpleNamespace(
            begin_shutdown=lambda label: events.append(("begin", label)) or True,
            wait_for_writer_quiescence=lambda budget: (True, 0.0, []),
            wait_for_lease_handling=lambda: True,
        )
        service = WatchdogService(
            make_watchdog_ports(
                shutdown_controller=lambda: controller,
                request_shutdown=lambda: events.append(("request", None)),
                release_ledger_lease=lambda deadline: events.append(
                    ("release", True)
                )
                or True,
                exit_process=lambda code: events.append(("exit", code)),
                log=lambda message: events.append(("log", message)),
                lease_release_timeout_seconds=lambda: 0.2,
            )
        )

        service.hard_exit("liveness")

        self.assertEqual(events[0], ("request", None))
        self.assertEqual(events[1], ("begin", "watchdog_liveness"))
        self.assertEqual(events[2], ("release", True))
        self.assertEqual(events[3][0], "log")
        self.assertIn("lease_handled=True", str(events[3][1]))
        self.assertEqual(events[-1], ("exit", 1))

    def test_hard_exit_withholds_release_when_quiescence_fails(self) -> None:
        events: list[object] = []
        controller = SimpleNamespace(
            begin_shutdown=lambda _label: True,
            wait_for_writer_quiescence=lambda _budget: (False, 0.05, ["writer"]),
        )
        service = WatchdogService(
            make_watchdog_ports(
                shutdown_controller=lambda: controller,
                release_ledger_lease=lambda _deadline: events.append(
                    ("release", None)
                )
                or True,
                exit_process=lambda code: events.append(("exit", code)),
                log=lambda message: events.append(("log", message)),
            )
        )

        service.hard_exit("publication")

        self.assertNotIn(("release", None), events)
        self.assertIn("lease_handled=False", str(events[0][1]))
        self.assertEqual(events[-1], ("exit", 1))

    def test_lease_heartbeat_diagnostic_uses_lease_failure_reason(self) -> None:
        events: list[object] = []
        controller = SimpleNamespace(
            begin_shutdown=lambda _label: True,
            wait_for_writer_quiescence=lambda _budget: (True, 0.0, []),
        )
        service = WatchdogService(
            make_watchdog_ports(
                shutdown_controller=lambda: controller,
                lease_failure_reason=lambda: "lease heartbeat stopped",
                exit_process=lambda code: events.append(("exit", code)),
                log=lambda message: events.append(("log", message)),
            )
        )

        service.hard_exit("lease_heartbeat")

        self.assertIn("lease heartbeat stopped", str(events[0][1]))
        self.assertEqual(events[-1], ("exit", 1))


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


class ContentionObservedLock:
    """Lock that exposes an acquire attempt made while another caller owns it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.contended = threading.Event()

    def __enter__(self) -> "ContentionObservedLock":
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

    def test_every_process_service_is_registered_unconditionally(self) -> None:
        # The registry declares every spec regardless of the live enable
        # flags; serve() applies the conditional at each named start. A
        # disabled coordinator therefore composes the identical registry.
        disabled = self.coordinator_with_optional_services(False)
        enabled = self.coordinator_with_optional_services(True)

        self.assertEqual(
            disabled._make_background_service_registry().service_names(),
            enabled._make_background_service_registry().service_names(),
        )

    def test_audit_http_starts_only_after_both_snapshot_refreshers(self) -> None:
        # Upstream #120 ordering: both cached-snapshot refreshers are armed
        # before the listener facade binds; the warm-up itself runs inside
        # their background loops so the bind path never blocks on it.
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

        # The watchdog spec is deliberately absent from the composed
        # registry: serve() registers it inline at its start boundary so the
        # publication-progress watchdog exists before synchronous startup
        # prewarm/recovery work.
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

    def test_health_refresher_start_marks_latch_before_thread_dispatch(self) -> None:
        # Upstream #120 contract: the running latch is set before the
        # registry can start the refresher thread, and it is a one-shot
        # latch (the loop never clears it), so a second start call returns
        # without touching the registry again.
        server = self.coordinator_with_optional_services(False)
        server.stop_event = threading.Event()
        server.stop_event.set()
        latch_at_dispatch: list[bool] = []

        class LatchObservingThread(DormantThread):
            def start(self) -> None:
                latch_at_dispatch.append(
                    server._ensure_observability_service()
                    .state()
                    .health_refresh_loop_running
                )
                super().start()

        server._background_services = BackgroundServiceRegistry(
            [specification("health_snapshot_refresher")],
            thread_factory=LatchObservingThread,  # type: ignore[arg-type]
        )

        server.start_health_snapshot_refresher()

        started = server._background_services.snapshot("health_snapshot_refresher")
        self.assertEqual(latch_at_dispatch, [True])
        self.assertTrue(started.started)
        self.assertTrue(
            server._ensure_observability_service()
            .state()
            .health_refresh_loop_running
        )

        server.start_health_snapshot_refresher()

        self.assertEqual(started.thread.start_count, 1)  # type: ignore[union-attr]
        self.assertTrue(
            server._ensure_observability_service()
            .state()
            .health_refresh_loop_running
        )


class ScriptedStopEvent:
    """Stop signal whose transitions the test scripts explicitly."""

    def __init__(self) -> None:
        self.stopped = False
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return self.stopped

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        return self.stopped


class ScriptedRollupLedger:
    """Rollup-capable ledger stub yielding one scripted result per pass."""

    def __init__(self, results: list[object], *, on_exhausted=None) -> None:
        self.results = list(results)
        self.calls: list[int] = []
        self.on_exhausted = on_exhausted

    def advance_hashrate_rollups(self, *, batch_limit: int) -> dict[str, object]:
        self.calls.append(batch_limit)
        result = self.results.pop(0)
        if not self.results and self.on_exhausted is not None:
            self.on_exhausted()
        if isinstance(result, BaseException):
            raise result
        return result  # type: ignore[return-value]


class HashrateRollupMaintenanceServiceTests(unittest.TestCase):
    @staticmethod
    def rollup_coordinator(ledger: object) -> PrismCoordinator:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.blockwait_enabled = False
        server.vardiff_idle_sweep_seconds = 0.0
        server.stratum_initial_job_timeout_seconds = 0.0
        server.ctv_broadcaster_enabled = False
        server.watchdog_enabled = False
        server.audit_bind = None
        server.audit_port = 0
        server.ledger = ledger
        server.hashrate_rollup_enabled = True
        server.hashrate_rollup_interval_seconds = 15.0
        server.hashrate_rollup_batch_shares = 50000
        return server

    def test_rollup_service_registers_only_for_rollup_capable_ledgers(self) -> None:
        # The capability conditional is the one intentional exception to the
        # registered-unconditionally rule: a memory ledger never grows
        # advance_hashrate_rollups at runtime, so the service either exists
        # for the life of the process or never will.
        capable = self.rollup_coordinator(ScriptedRollupLedger([]))
        registry = capable._make_background_service_registry()
        self.assertIn("hashrate-rollup-maintenance", registry.service_names())
        service = registry.snapshot("hashrate-rollup-maintenance").specification
        self.assertEqual(
            (service.thread_name, service.join_timeout, service.watchdog_monitored),
            ("prism-hashrate-rollup-maintenance", 1.0, False),
        )

        memory_backed = self.rollup_coordinator(SimpleNamespace())
        self.assertNotIn(
            "hashrate-rollup-maintenance",
            memory_backed._make_background_service_registry().service_names(),
        )

    def test_rollup_start_honours_enable_flag_and_ledger_capability(self) -> None:
        started = self.rollup_coordinator(ScriptedRollupLedger([]))
        started._heartbeats = {}
        started._watchdog_pauses = {}
        started._heartbeats_lock = threading.Lock()
        started._background_services = BackgroundServiceRegistry(
            [specification("hashrate-rollup-maintenance")],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )
        with patch("builtins.print"):
            started._start_hashrate_rollup_maintenance_if_enabled()
        self.assertTrue(
            started._background_services.snapshot("hashrate-rollup-maintenance").started
        )

        disabled = self.rollup_coordinator(ScriptedRollupLedger([]))
        disabled.hashrate_rollup_enabled = False
        disabled._background_services = BackgroundServiceRegistry(
            [specification("hashrate-rollup-maintenance")],
            thread_factory=DormantThread,  # type: ignore[arg-type]
        )
        disabled._start_hashrate_rollup_maintenance_if_enabled()
        self.assertFalse(
            disabled._background_services.snapshot("hashrate-rollup-maintenance").started
        )

        # A memory-backed coordinator has no registered service to start, so
        # the guard must return before asking the registry for the name.
        memory_backed = self.rollup_coordinator(SimpleNamespace())
        memory_backed._background_services = BackgroundServiceRegistry([])
        memory_backed._start_hashrate_rollup_maintenance_if_enabled()

    def test_rollup_loop_drains_backlog_then_sleeps_the_interval(self) -> None:
        # Catch-up looping is the backfill mechanism: while the ledger
        # reports more history behind the watermark the loop advances again
        # immediately, and only a caught-up pass reaches the interval wait.
        stop = ScriptedStopEvent()
        ledger = ScriptedRollupLedger(
            [
                {"scanned": 3, "last_share_seq": 3, "caught_up": False},
                {"scanned": 3, "last_share_seq": 6, "caught_up": False},
                {"scanned": 2, "last_share_seq": 8, "caught_up": True},
            ],
            on_exhausted=lambda: setattr(stop, "stopped", True),
        )
        server = self.rollup_coordinator(ledger)
        server.stop_event = stop  # type: ignore[assignment]
        server.hashrate_rollup_batch_shares = 3

        with patch("builtins.print"):
            server.hashrate_rollup_maintenance_loop()

        self.assertEqual(ledger.calls, [3, 3, 3])
        self.assertEqual(stop.waits, [15.0])

    def test_rollup_passes_run_inside_the_writer_admission(self) -> None:
        """Each pass is a writer operation the shutdown controller counts.

        The maintenance statement renews the writer lease on exact identity,
        which an orderly release keeps -- so a pass outside the admission
        could slip past the stop check, renew the just-released lease, and
        mutate rollups after shutdown reported the writer quiesced. Running
        the ledger call inside _writer_operation makes quiescence wait for
        an in-flight pass.
        """
        stop = ScriptedStopEvent()
        server = self.rollup_coordinator(None)
        server.stop_event = stop  # type: ignore[assignment]
        controller = server._ensure_shutdown_controller()
        observed: list[int] = []

        class AdmissionObservingLedger(ScriptedRollupLedger):
            def advance_hashrate_rollups(self, *, batch_limit: int) -> dict[str, object]:
                observed.append(controller.active_writers.get("hashrate_rollup_maintenance", 0))
                return super().advance_hashrate_rollups(batch_limit=batch_limit)

        server.ledger = AdmissionObservingLedger(
            [{"scanned": 0, "last_share_seq": 8, "caught_up": True}],
            on_exhausted=lambda: setattr(stop, "stopped", True),
        )

        with patch("builtins.print"):
            server.hashrate_rollup_maintenance_loop()

        self.assertEqual([1], observed)
        self.assertEqual(
            0, controller.active_writers.get("hashrate_rollup_maintenance", 0)
        )

    def test_rollup_loop_stops_cleanly_when_admission_is_closed(self) -> None:
        """A refused admission is the orderly shutdown, not a failure.

        The race this pins: shutdown begins after the loop's stop check but
        before the pass enters the admission. The pass must be refused, and
        the loop must exit without logging an error or running the ledger.
        """
        stop = ScriptedStopEvent()
        ledger = ScriptedRollupLedger([])
        server = self.rollup_coordinator(ledger)
        server.stop_event = stop  # type: ignore[assignment]
        server._ensure_shutdown_controller().request_shutdown(None)

        with patch("builtins.print"), patch("traceback.print_exc") as print_exc:
            server.hashrate_rollup_maintenance_loop()

        self.assertEqual([], ledger.calls)
        self.assertEqual([], stop.waits)
        self.assertEqual(0, print_exc.call_count)

    def test_rollup_loop_logs_the_error_and_retries_next_interval(self) -> None:
        stop = ScriptedStopEvent()
        ledger = ScriptedRollupLedger(
            [
                RuntimeError("rollup pass failed"),
                {"scanned": 0, "last_share_seq": 8, "caught_up": True},
            ],
            on_exhausted=lambda: setattr(stop, "stopped", True),
        )
        server = self.rollup_coordinator(ledger)
        server.stop_event = stop  # type: ignore[assignment]

        with patch("builtins.print"), patch("traceback.print_exc") as print_exc:
            server.hashrate_rollup_maintenance_loop()

        # The failed pass reaches the interval wait instead of hot-looping,
        # then the next pass succeeds and the scripted stop ends the loop.
        self.assertEqual(ledger.calls, [50000, 50000])
        self.assertEqual(stop.waits, [15.0, 15.0])
        self.assertEqual(print_exc.call_count, 1)


if __name__ == "__main__":
    unittest.main()

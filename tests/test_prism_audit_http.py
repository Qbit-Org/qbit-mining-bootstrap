#!/usr/bin/env python3
"""Direct routing and bounded lifecycle tests for the PRISM HTTP facade."""

from __future__ import annotations

import json
import threading
import time
import unittest
from unittest.mock import patch
import urllib.request

from lab.prism.audit_http import AuditHttpConfig, AuditHttpFacade


class FakeAuditHttpPort:
    def __init__(self) -> None:
        self.health_entered: threading.Event | None = None
        self.health_release: threading.Event | None = None
        self.health_calls = 0
        self.metrics_calls = 0

    def cached_health_payload(self) -> tuple[int, dict[str, object]]:
        self.health_calls += 1
        if self.health_entered is not None:
            self.health_entered.set()
        if self.health_release is not None:
            self.health_release.wait(2.0)
        return 200, {"ok": True, "schema": "health-fixture"}

    def cached_metrics_payload(self) -> tuple[int, str]:
        self.metrics_calls += 1
        return 200, "qbit_prism_cached_fixture 1\n"

    def latest_evidence_payload(self) -> dict[str, object] | None:
        return None

    def owed_balances_payload(self) -> dict[str, object]:
        return {"balances": []}

    def carry_forward_integrity_payload(self) -> dict[str, object]:
        return {"ok": True}

    def miner_status_payload(self, recipient_id: str) -> dict[str, object]:
        return {"recipient_id": recipient_id}

    def public_payload(
        self,
        path: str,
        query: dict[str, list[str]],
    ) -> tuple[int, object]:
        raise AssertionError((path, query))


class AuditHttpFacadeTests(unittest.TestCase):
    def test_start_failure_releases_lifecycle_lock_before_join(self) -> None:
        class NeverReadyServer:
            def __init__(self, address: tuple[str, int], _handler: object) -> None:
                self.server_address = address
                self.ready = threading.Event()
                self.shutdown_requested = threading.Event()
                self.closed = False
                self.serve_committed = False

            def serve_unless_startup_cancelled(self, *, poll_interval: float) -> None:
                del poll_interval
                self.serve_committed = True
                self.shutdown_requested.wait(2.0)

            def cancel_startup(self) -> bool:
                return self.serve_committed

            def shutdown(self) -> None:
                self.shutdown_requested.set()

            def server_close(self) -> None:
                self.closed = True

        facade = AuditHttpFacade(
            FakeAuditHttpPort(),  # type: ignore[arg-type]
            AuditHttpConfig(
                "127.0.0.1",
                0,
                join_timeout_seconds=0.5,
            ),
        )
        started = time.monotonic()
        with patch(
            "lab.prism.audit_http._BoundedThreadingHttpServer",
            NeverReadyServer,
        ), self.assertRaisesRegex(RuntimeError, "did not enter"):
            facade.start()

        self.assertLess(time.monotonic() - started, 1.25)
        state = facade.state()
        self.assertEqual(state.lifecycle, "stopped")
        self.assertFalse(state.thread_alive)
        self.assertIsNone(state.bound_address)

    def test_start_timeout_cancels_thread_delayed_before_serve_forever(self) -> None:
        release_thread = threading.Event()
        serve_forever_called = threading.Event()
        real_thread = threading.Thread

        class DelayedThread(real_thread):
            def run(self) -> None:
                release_thread.wait(2.0)
                super().run()

            def join(self, timeout: float | None = None) -> None:
                release_thread.set()
                super().join(timeout)

        class TrackedServer:
            instance: TrackedServer | None = None

            def __init__(self, address: tuple[str, int], _handler: object) -> None:
                self.server_address = address
                self.ready = threading.Event()
                self.cancelled = False
                self.closed = False
                TrackedServer.instance = self

            def serve_unless_startup_cancelled(self, *, poll_interval: float) -> None:
                del poll_interval
                if not self.cancelled:
                    serve_forever_called.set()

            def cancel_startup(self) -> bool:
                self.cancelled = True
                return False

            def shutdown(self) -> None:
                raise AssertionError("shutdown called before serve_forever")

            def server_close(self) -> None:
                self.closed = True

        facade = AuditHttpFacade(
            FakeAuditHttpPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0, join_timeout_seconds=0.5),
        )
        with patch("lab.prism.audit_http.threading.Thread", DelayedThread), patch(
            "lab.prism.audit_http._BoundedThreadingHttpServer",
            TrackedServer,
        ), self.assertRaisesRegex(RuntimeError, "did not enter"):
            facade.start()

        self.assertFalse(serve_forever_called.is_set())
        assert TrackedServer.instance is not None
        self.assertTrue(TrackedServer.instance.cancelled)
        self.assertTrue(TrackedServer.instance.closed)
        self.assertEqual(facade.state().lifecycle, "stopped")

    def test_configuration_bounds_listener_and_join_values(self) -> None:
        with self.assertRaises(ValueError):
            AuditHttpConfig("", 1)
        with self.assertRaises(ValueError):
            AuditHttpConfig("127.0.0.1", -1)
        with self.assertRaises(ValueError):
            AuditHttpConfig("127.0.0.1", 65_536)
        with self.assertRaises(ValueError):
            AuditHttpConfig("127.0.0.1", 1, join_timeout_seconds=-1)

    def test_start_serves_cached_payloads_and_stop_is_idempotent(self) -> None:
        port = FakeAuditHttpPort()
        facade = AuditHttpFacade(
            port,  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0),
        )
        state = facade.start()
        self.assertEqual(state.lifecycle, "running")
        self.assertTrue(state.thread_alive)
        self.assertIsNotNone(state.bound_address)
        assert state.bound_address is not None
        base_url = f"http://127.0.0.1:{state.bound_address[1]}"

        with urllib.request.urlopen(base_url + "/healthz", timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(
                json.loads(response.read()),
                {"ok": True, "schema": "health-fixture"},
            )
            self.assertTrue(
                response.headers["Server"].startswith(
                    "QbitPrismAudit/0.1 Python/"
                )
            )
        with urllib.request.urlopen(base_url + "/metrics", timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(
                response.headers["Content-Type"],
                "text/plain; version=0.0.4",
            )
            self.assertEqual(response.read(), b"qbit_prism_cached_fixture 1\n")

        self.assertEqual(port.health_calls, 1)
        self.assertEqual(port.metrics_calls, 1)
        self.assertTrue(facade.stop())
        self.assertTrue(facade.stop())
        stopped = facade.state()
        self.assertEqual(stopped.lifecycle, "stopped")
        self.assertFalse(stopped.thread_alive)
        self.assertIsNone(stopped.bound_address)

    def test_unexpected_serve_exit_closes_listener_before_restart(self) -> None:
        class ExitServer:
            instances: list[ExitServer] = []

            def __init__(self, address: tuple[str, int], _handler: object) -> None:
                if self.instances and not self.instances[-1].closed.is_set():
                    raise OSError("previous listener is still open")
                self.server_address = address
                self.ready = threading.Event()
                self.release = threading.Event()
                self.closed = threading.Event()
                self.instances.append(self)

            def serve_unless_startup_cancelled(self, *, poll_interval: float) -> None:
                del poll_interval
                self.ready.set()
                self.release.wait(2.0)

            def cancel_startup(self) -> bool:
                return True

            def shutdown(self) -> None:
                self.release.set()

            def server_close(self) -> None:
                self.closed.set()

        facade = AuditHttpFacade(
            FakeAuditHttpPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0),
        )
        with patch(
            "lab.prism.audit_http._BoundedThreadingHttpServer",
            ExitServer,
        ):
            self.assertEqual(facade.start().lifecycle, "running")
            first = ExitServer.instances[0]
            first.release.set()
            self.assertTrue(first.closed.wait(1.0))
            deadline = time.monotonic() + 1.0
            while facade.state().bound_address is not None:
                if time.monotonic() >= deadline:
                    self.fail("exited audit HTTP listener was not retired")
                time.sleep(0.01)

            exited = facade.state()
            self.assertEqual(exited.lifecycle, "exited")
            self.assertFalse(exited.thread_alive)
            self.assertEqual(facade.start().lifecycle, "running")
            self.assertEqual(len(ExitServer.instances), 2)
            self.assertTrue(facade.stop())

    def test_stop_before_start_is_safe(self) -> None:
        facade = AuditHttpFacade(FakeAuditHttpPort())  # type: ignore[arg-type]

        self.assertTrue(facade.stop())
        self.assertEqual(facade.state().lifecycle, "stopped")

    def test_active_request_thread_does_not_delay_listener_shutdown(self) -> None:
        port = FakeAuditHttpPort()
        port.health_entered = threading.Event()
        port.health_release = threading.Event()
        facade = AuditHttpFacade(
            port,  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0),
        )
        state = facade.start()
        assert state.bound_address is not None
        outcome: list[object] = []

        def request() -> None:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{state.bound_address[1]}/healthz",
                    timeout=2,
                ) as response:
                    outcome.append(response.read())
            except Exception as exc:
                outcome.append(exc)

        request_thread = threading.Thread(target=request)
        request_thread.start()
        self.assertTrue(port.health_entered.wait(1.0))

        self.assertTrue(facade.stop())
        self.assertFalse(port.health_release.is_set())

        port.health_release.set()
        request_thread.join(2.0)
        self.assertFalse(request_thread.is_alive())
        self.assertEqual(outcome, [b'{"ok": true, "schema": "health-fixture"}\n'])

    def test_bind_failure_does_not_publish_a_thread_or_socket(self) -> None:
        first = AuditHttpFacade(
            FakeAuditHttpPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0),
        )
        first_state = first.start()
        assert first_state.bound_address is not None
        second = AuditHttpFacade(
            FakeAuditHttpPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", first_state.bound_address[1]),
        )
        try:
            with self.assertRaises(OSError):
                second.start()
            failed = second.state()
            self.assertEqual(failed.lifecycle, "new")
            self.assertFalse(failed.thread_alive)
            self.assertIsNone(failed.bound_address)
        finally:
            self.assertTrue(second.stop())
            self.assertTrue(first.stop())


if __name__ == "__main__":
    unittest.main()

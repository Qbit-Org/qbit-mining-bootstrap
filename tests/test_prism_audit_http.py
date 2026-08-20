#!/usr/bin/env python3
"""Direct routing and bounded lifecycle tests for the PRISM HTTP facade."""

from __future__ import annotations

import json
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

from lab.prism.audit_http import (
    AuditHttpConfig,
    AuditHttpFacade,
    _BoundedThreadingHttpServer,
)
from lab.prism.observability import (
    METRICS_STALE_WARNING,
    METRICS_STATE_FRESH,
    METRICS_STATE_HEADER,
    METRICS_STATE_STALE,
    METRICS_STATE_UNAVAILABLE,
    MetricsSnapshotResponse,
)


class FakeAuditHttpPort:
    def __init__(self) -> None:
        self.health_entered: threading.Event | None = None
        self.health_release: threading.Event | None = None
        self.health_calls = 0
        self.metrics_calls = 0
        self.metrics_response = MetricsSnapshotResponse(
            status=200,
            body="qbit_prism_cached_fixture 1\n",
            state=METRICS_STATE_FRESH,
            age_seconds=0,
        )

    def cached_health_payload(self) -> tuple[int, dict[str, object]]:
        self.health_calls += 1
        if self.health_entered is not None:
            self.health_entered.set()
        if self.health_release is not None:
            self.health_release.wait(2.0)
        return 200, {"ok": True, "schema": "health-fixture"}

    def cached_metrics_payload(self) -> MetricsSnapshotResponse:
        self.metrics_calls += 1
        return self.metrics_response

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

    def test_metrics_publishes_freshness_in_response_metadata(self) -> None:
        """A scraper must tell fresh from stale from unavailable (issue #184).

        None of these assertions parse the Prometheus body: the whole point of
        the response metadata is that a consumer does not have to.
        """

        port = FakeAuditHttpPort()
        facade = AuditHttpFacade(
            port,  # type: ignore[arg-type]
            AuditHttpConfig(bind="127.0.0.1", port=0),
        )
        self.addCleanup(facade.stop)
        state = facade.start()
        assert state.bound_address is not None
        url = f"http://127.0.0.1:{state.bound_address[1]}/metrics"

        with urllib.request.urlopen(url, timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers[METRICS_STATE_HEADER], "fresh")
            self.assertEqual(response.headers["Age"], "0")
            self.assertIsNone(response.headers["Warning"])
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            self.assertEqual(response.read(), b"qbit_prism_cached_fixture 1\n")

        # A stale complete payload is served, not refused: 200 is what keeps
        # Prometheus from discarding the last document that still exists.
        port.metrics_response = MetricsSnapshotResponse(
            status=200,
            body="qbit_prism_cached_fixture 1\n",
            state=METRICS_STATE_STALE,
            age_seconds=42,
        )
        with urllib.request.urlopen(url, timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers[METRICS_STATE_HEADER], "stale")
            self.assertEqual(response.headers["Age"], "42")
            self.assertEqual(response.headers["Warning"], METRICS_STALE_WARNING)
            self.assertEqual(
                response.headers["Content-Type"],
                "text/plain; version=0.0.4",
            )
            # The prior complete document, unchanged.
            self.assertEqual(response.read(), b"qbit_prism_cached_fixture 1\n")

        # Warm-up still refuses, and says so out of band. No Age and no stale
        # warning: there is no served payload for either to describe.
        port.metrics_response = MetricsSnapshotResponse(
            status=503,
            body="qbit_prism_metrics_snapshot_available 0\n",
            state=METRICS_STATE_UNAVAILABLE,
            age_seconds=None,
        )
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(url, timeout=2)
        refusal = raised.exception
        self.addCleanup(refusal.close)
        self.assertEqual(refusal.status, 503)
        self.assertEqual(refusal.headers[METRICS_STATE_HEADER], "unavailable")
        self.assertIsNone(refusal.headers["Age"])
        self.assertIsNone(refusal.headers["Warning"])
        self.assertEqual(
            refusal.read(),
            b"qbit_prism_metrics_snapshot_available 0\n",
        )
        self.assertEqual(port.metrics_calls, 3)

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

    def test_stop_racing_startup_before_serve_commit_returns_within_bound(
        self,
    ) -> None:
        """The documented old-layer gap: stop() used to call the blocking
        BaseServer.shutdown() whenever the serve thread was alive, so a stop
        racing startup before serve_forever() committed waited forever on a
        loop that never starts. The bounded stop must return within its
        configured budget and still release the real listener socket."""
        real_thread = threading.Thread
        stalled_started = threading.Event()
        release_stall = threading.Event()

        class StalledThread(real_thread):
            def run(self) -> None:
                stalled_started.set()
                release_stall.wait(5.0)
                super().run()

        class RecordingBounded(_BoundedThreadingHttpServer):
            instances: list[_BoundedThreadingHttpServer] = []

            def __init__(self, *args: object, **kwargs: object) -> None:
                super().__init__(*args, **kwargs)  # type: ignore[arg-type]
                RecordingBounded.instances.append(self)

        facade = AuditHttpFacade(
            FakeAuditHttpPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0, join_timeout_seconds=0.25),
        )
        start_outcome: list[object] = []

        def run_start() -> None:
            try:
                start_outcome.append(facade.start())
            except Exception as exc:
                start_outcome.append(exc)

        starter = real_thread(target=run_start)
        with patch("lab.prism.audit_http.threading.Thread", StalledThread), patch(
            "lab.prism.audit_http._BoundedThreadingHttpServer",
            RecordingBounded,
        ):
            starter.start()
            self.assertTrue(stalled_started.wait(2.0))
            # The serve thread is alive but has not committed to
            # serve_forever(); stop() must still return within its bound.
            stop_started = time.monotonic()
            stopped = facade.stop()
            elapsed = time.monotonic() - stop_started

            self.assertLess(elapsed, 1.5)
            self.assertFalse(stopped)
            self.assertEqual(facade.state().lifecycle, "stop_timeout")

            release_stall.set()
            starter.join(5.0)
        self.assertFalse(starter.is_alive())
        self.assertEqual(len(start_outcome), 1)
        self.assertIsInstance(start_outcome[0], RuntimeError)
        self.assertEqual(len(RecordingBounded.instances), 1)
        # The cancelled serve thread never entered serve_forever, and the
        # listener socket is closed so a successor can bind the port.
        self.assertEqual(RecordingBounded.instances[0].socket.fileno(), -1)
        state = facade.state()
        self.assertEqual(state.lifecycle, "stopped")
        self.assertFalse(state.thread_alive)
        self.assertIsNone(state.bound_address)

    def test_stop_reports_timeout_when_committed_loop_refuses_to_exit(self) -> None:
        """A committed serve loop that ignores shutdown must not block stop()
        past its budget; the facade reports stop_timeout (the state the
        coordinator's audit_http_stop shutdown log is keyed on)."""

        class WedgedServer:
            instance: WedgedServer | None = None

            def __init__(self, address: tuple[str, int], _handler: object) -> None:
                self.server_address = address
                self.ready = threading.Event()
                self.release = threading.Event()
                self.closed = False
                WedgedServer.instance = self

            def serve_unless_startup_cancelled(self, *, poll_interval: float) -> None:
                del poll_interval
                self.ready.set()
                self.release.wait(5.0)

            def cancel_startup(self) -> bool:
                return True

            def shutdown(self) -> None:
                # Deliberately ignore the request: the loop stays wedged.
                return None

            def server_close(self) -> None:
                self.closed = True

        facade = AuditHttpFacade(
            FakeAuditHttpPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0, join_timeout_seconds=0.25),
        )
        with patch(
            "lab.prism.audit_http._BoundedThreadingHttpServer",
            WedgedServer,
        ):
            self.assertEqual(facade.start().lifecycle, "running")
            stop_started = time.monotonic()
            stopped = facade.stop()
            elapsed = time.monotonic() - stop_started

            self.assertFalse(stopped)
            self.assertLess(elapsed, 1.5)
            self.assertEqual(facade.state().lifecycle, "stop_timeout")
            assert WedgedServer.instance is not None
            self.assertTrue(WedgedServer.instance.closed)
            WedgedServer.instance.release.set()

    def test_concurrent_stops_against_a_live_listener_all_return(self) -> None:
        facade = AuditHttpFacade(
            FakeAuditHttpPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0),
        )
        self.assertEqual(facade.start().lifecycle, "running")
        results: list[bool] = []
        errors: list[BaseException] = []

        def run_stop() -> None:
            try:
                results.append(facade.stop())
            except BaseException as exc:
                errors.append(exc)

        stoppers = [threading.Thread(target=run_stop) for _ in range(3)]
        for stopper in stoppers:
            stopper.start()
        for stopper in stoppers:
            stopper.join(5.0)

        self.assertTrue(all(not stopper.is_alive() for stopper in stoppers))
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))
        state = facade.state()
        self.assertEqual(state.lifecycle, "stopped")
        self.assertFalse(state.thread_alive)


if __name__ == "__main__":
    unittest.main()

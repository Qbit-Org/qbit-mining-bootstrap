"""Socket-level admission and request-header deadlines for the public API."""

import gc
import http.client
import socket
import threading
import time
import unittest
import weakref
from unittest.mock import patch

from lab.prism import public_read_service as public


class PublicHttpLimitsTests(unittest.TestCase):
    def setUp(self):
        self.admitted = threading.Event()
        handler = public.make_handler(public.PublicReadService(object()))
        admitted = self.admitted
        self.handler_refs = []
        handler_refs = self.handler_refs

        class ObservedHandler(handler):
            def setup(self):
                super().setup()
                handler_refs.append(weakref.ref(self))
                admitted.set()

        self.server = public.BoundedPublicHTTPServer(
            ("127.0.0.1", 0), ObservedHandler, max_connections=1,
            timeout_seconds=0.3,
        )
        self.thread = threading.Thread(
            target=lambda: self.server.serve_forever(poll_interval=0.01), daemon=True,
        )
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def connect(self):
        client = socket.create_connection(self.server.server_address, timeout=2)
        self.addCleanup(client.close)
        return client

    def assert_closed(self, client):
        try:
            self.assertEqual(client.recv(4096), b"")
        except ConnectionResetError:
            pass

    def wait_for_capacity(self):
        # Wait on the actual admission boundary; no arbitrary recovery sleep.
        self.assertTrue(self.server._connection_slots.acquire(timeout=2))
        self.server._connection_slots.release()

    def test_excess_connections_close_and_idle_worker_retires(self):
        idle = self.connect()
        self.assertTrue(self.admitted.wait(2))
        self.assert_closed(self.connect())
        self.assert_closed(idle)
        self.wait_for_capacity()
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        self.addCleanup(client.close)
        client.request("GET", "/missing")
        response = client.getresponse()
        self.assertEqual(response.status, 404)
        self.assertEqual(response.getheader("Connection"), "close")
        response.read()

    def test_slow_headers_cannot_extend_total_deadline(self):
        client = self.connect()
        self.assertTrue(self.admitted.wait(2))
        dispatched = threading.Event()
        handler = self.server.RequestHandlerClass
        original = handler.do_GET

        def observe_dispatch(request):
            dispatched.set()
            original(request)

        patcher = patch.object(handler, "do_GET", observe_dispatch)
        patcher.start()
        self.addCleanup(patcher.stop)
        client.sendall(b"GET /missing HTTP/1.1\r\nX-Slow: ")
        stopped = threading.Event()

        def drip():
            while not stopped.wait(0.03):
                try:
                    client.sendall(b"x")
                except OSError:
                    return

        writer = threading.Thread(target=drip, daemon=True)
        writer.start()
        started = time.monotonic()
        try:
            self.assert_closed(client)
            self.assertLess(time.monotonic() - started, 1.5)
        finally:
            stopped.set()
            writer.join(2)
        self.wait_for_capacity()
        self.assertFalse(dispatched.is_set(), "expired headers reached application dispatch")

    def test_keep_alive_request_cannot_retain_worker(self):
        client = self.connect()
        client.sendall(b"GET /missing HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
        body = b""
        while chunk := client.recv(4096):
            body += chunk
        self.assertIn(b"404", body)
        self.assertIn(b"Connection: close", body)
        self.wait_for_capacity()

    def test_completed_handler_retires_without_cyclic_gc(self):
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            client = self.connect()
            client.sendall(b"GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n")
            while client.recv(4096):
                pass
            self.wait_for_capacity()
            self.assertEqual(len(self.handler_refs), 1)
            self.assertIsNone(self.handler_refs[0]())
        finally:
            if was_enabled:
                gc.enable()

    def test_worker_start_failure_returns_admission_slot(self):
        request, peer = socket.socketpair()
        self.addCleanup(request.close)
        self.addCleanup(peer.close)
        with patch.object(public.ThreadingHTTPServer, "process_request", side_effect=RuntimeError):
            with self.assertRaises(RuntimeError):
                self.server.process_request(request, ("127.0.0.1", 1))
        self.wait_for_capacity()

    def test_invalid_limits_refuse_before_binding(self):
        for kwargs in (
            {"max_connections": 0}, {"max_connections": 1025},
            {"timeout_seconds": 0}, {"timeout_seconds": float("nan")},
            {"timeout_seconds": float("inf")},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(public.PublicReadConfigurationError):
                public.BoundedPublicHTTPServer(self.server.server_address, object, **kwargs)

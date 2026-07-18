#!/usr/bin/env python3
"""Stratum listeners across a fast restart.

The coordinator must bind its listen sockets before the slow parts of startup
(qbit readiness, policy validation, block-work recovery) so miners
reconnecting through a ~2s restart park in the kernel accept backlog instead
of getting connection refused, and must tolerate a predecessor process that
still holds the port while draining its shutdown.
"""

from __future__ import annotations

import socket
import threading
import time
import types
import unittest
from contextlib import ExitStack
from decimal import Decimal

from lab.auxpow import vardiff
from lab.prism.prism_coordinator import (
    PrismCoordinator,
    StratumListenerProfile,
)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_vardiff_config() -> vardiff.VardiffConfig:
    return vardiff.VardiffConfig(
        enabled=False,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("0.000000001"),
        max_difficulty=Decimal("1024"),
        retarget_interval_seconds=Decimal("90"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("1"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("0.4"),
        retarget_tolerance=Decimal("0.25"),
    )


def make_profile(port: int, name: str = "default") -> StratumListenerProfile:
    return StratumListenerProfile(
        name=name,
        bind="127.0.0.1",
        port=port,
        share_difficulty=Decimal("1"),
        vardiff_config=make_vardiff_config(),
        heartbeat_name=f"stratum_accept_{name}",
    )


def make_coordinator(
    profiles: list[StratumListenerProfile],
    *,
    bind_retry_seconds: float = 5.0,
) -> PrismCoordinator:
    server = PrismCoordinator.__new__(PrismCoordinator)
    server.listener_profiles = profiles
    server.stratum_listen_backlog = 128
    server.stratum_bind_retry_seconds = bind_retry_seconds
    return server


class OpenStratumListenersTest(unittest.TestCase):
    def test_bound_listeners_hold_connections_before_accept(self) -> None:
        """A connection completes its handshake in the kernel backlog even
        though nothing has called accept() yet — the state miners sit in while
        startup recovery runs."""
        ports = [free_port(), free_port()]
        server = make_coordinator(
            [make_profile(ports[0]), make_profile(ports[1], name="highdiff")]
        )
        with ExitStack() as stack:
            listeners = server.open_stratum_listeners(stack)
            self.assertEqual(len(listeners), 2)
            for port in ports:
                with socket.create_connection(("127.0.0.1", port), timeout=2):
                    pass

    def test_bind_retries_until_predecessor_releases_port(self) -> None:
        port = free_port()
        predecessor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        predecessor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        predecessor.bind(("127.0.0.1", port))
        predecessor.listen(1)
        self.addCleanup(predecessor.close)
        releaser = threading.Timer(0.3, predecessor.close)
        releaser.start()
        self.addCleanup(releaser.cancel)
        server = make_coordinator([make_profile(port)], bind_retry_seconds=5.0)
        started = time.monotonic()
        with ExitStack() as stack:
            listeners = server.open_stratum_listeners(stack)
            self.assertEqual(len(listeners), 1)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 5.0)

    def test_bind_retry_aborts_on_shutdown_signal(self) -> None:
        """A SIGTERM during the bind retry must stop the port contention
        immediately and gracefully instead of running out the retry window."""
        port = free_port()
        predecessor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        predecessor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        predecessor.bind(("127.0.0.1", port))
        predecessor.listen(1)
        self.addCleanup(predecessor.close)
        server = make_coordinator([make_profile(port)], bind_retry_seconds=30.0)
        server.stop_event = threading.Event()
        server.stop_event.set()
        started = time.monotonic()
        with ExitStack() as stack:
            self.assertIsNone(server.open_stratum_listeners(stack))
        self.assertLess(time.monotonic() - started, 2.0)

    def test_zero_retry_window_fails_fast(self) -> None:
        port = free_port()
        predecessor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        predecessor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        predecessor.bind(("127.0.0.1", port))
        predecessor.listen(1)
        self.addCleanup(predecessor.close)
        server = make_coordinator([make_profile(port)], bind_retry_seconds=0.0)
        started = time.monotonic()
        with ExitStack() as stack:
            with self.assertRaises(OSError):
                server.open_stratum_listeners(stack)
        self.assertLess(time.monotonic() - started, 1.0)


class ServeBindsBeforeRecoveryTest(unittest.TestCase):
    def test_listeners_accept_tcp_connections_during_block_work_recovery(self) -> None:
        """serve() must already be listening when block-work recovery runs, so
        a restart's recovery window never bounces reconnecting miners."""
        port = free_port()
        server = make_coordinator([make_profile(port)], bind_retry_seconds=0.0)
        server.bind = "127.0.0.1"
        server.port = port
        server.share_difficulty = Decimal("1")
        server.min_ready_miners = 1
        server.vardiff_config = make_vardiff_config()
        server.max_blocks = 1
        server.blockpoll_seconds = 0.1
        server.version_mask = 0x1FFFE000
        server.version_mask_selection = types.SimpleNamespace(
            source="test", detail="fixed"
        )
        server.ledger = types.SimpleNamespace(backend_name="fake")
        server.rpc = types.SimpleNamespace(call=lambda *args, **kwargs: 0)
        server.audit_bind = None
        server.audit_port = 0
        server.blockwait_enabled = False
        server.vardiff_idle_sweep_seconds = 0.0
        server.ctv_broadcaster_enabled = False
        server.watchdog_enabled = False
        server.stop_event = threading.Event()
        server.lock = threading.RLock()
        server.clients = set()
        server.validate_live_chain_identity = lambda: None  # type: ignore[method-assign]
        server.validate_live_template_and_fee_policy = lambda: None  # type: ignore[method-assign]
        server.prism_payout_policy = lambda: {}  # type: ignore[method-assign]
        server.replay_recovered_shares = lambda: 0  # type: ignore[method-assign]

        observed: dict[str, bool] = {}

        def fake_replay_pending_block_candidates() -> int:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                observed["listening_during_recovery"] = True
            server.stop_event.set()
            return 0

        server.replay_pending_block_candidates = (  # type: ignore[method-assign]
            fake_replay_pending_block_candidates
        )

        server.serve()

        self.assertTrue(observed.get("listening_during_recovery"))
        # The listen port must be free again once serve() returns, so a
        # successor process can bind without waiting.
        replacement = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.addCleanup(replacement.close)
        replacement.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        replacement.bind(("127.0.0.1", port))

    def test_shutdown_during_rpc_readiness_wait_releases_ports(self) -> None:
        """A SIGTERM while serve() waits for qbit RPC must abort startup and
        free the bound listen ports promptly, so a successor's bind retry
        window is never starved by a dying predecessor."""
        port = free_port()
        server = make_coordinator([make_profile(port)], bind_retry_seconds=0.0)
        server.stop_event = threading.Event()

        def failing_rpc_call(*args: object, **kwargs: object) -> int:
            raise ConnectionError("qbit not ready")

        server.rpc = types.SimpleNamespace(call=failing_rpc_call)
        stopper = threading.Timer(0.2, server.stop_event.set)
        stopper.start()
        self.addCleanup(stopper.cancel)
        started = time.monotonic()
        server.serve()
        self.assertLess(time.monotonic() - started, 5.0)
        replacement = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.addCleanup(replacement.close)
        replacement.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        replacement.bind(("127.0.0.1", port))


if __name__ == "__main__":
    unittest.main()

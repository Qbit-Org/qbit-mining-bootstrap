#!/usr/bin/env python3
"""Failed-refresh spacing and same-tip template reuse (retry-storm pacing).

A blocked refresh used to re-attempt within one 0.25s trigger slice, and every
attempt re-issued a full getblocktemplate before rediscovering the blockage.
These tests pin the two throttles: consecutive failed passes are spaced by
PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS (new tips and successes stay
immediate), and same-tip passes inside the PRISM_TEMPLATE_CACHE_SECONDS window
reuse the cached template instead of refetching it.
"""

from __future__ import annotations

import time
import unittest
from unittest.mock import patch

from lab.prism.prism_coordinator import PrismCoordinator, TemplateRefreshBlocked
from tests.test_prism_coordinator_job_cache import (
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
)

NEW_TIP = "22" * 32


def _block_refreshes(server: PrismCoordinator) -> None:
    """Force every subsequent refresh pass to fail after snapshot acquisition."""
    server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
        lambda _tip: False
    )


class FailedTipRefreshSpacingTests(unittest.TestCase):
    def _fail_one_poll(self, server: PrismCoordinator) -> None:
        with self.assertRaisesRegex(TemplateRefreshBlocked, "untrusted"):
            server.poll_qbit_tip_template_once()

    def test_failed_pass_arms_holdoff_and_gates_the_trigger(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()
        server.blockpoll_seconds = 30.0
        server.tip_refresh_failure_holdoff_seconds = 0.3

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertEqual(server._tip_refresh_failure_holdoff_remaining(), 0.0)

        _block_refreshes(server)
        with patch("lab.prism.prism_coordinator.random.uniform", return_value=0.0):
            self._fail_one_poll(server)
        deadline = server._tip_refresh_failure_holdoff_until
        self.assertIsNotNone(deadline)
        assert deadline is not None
        self.assertGreater(server._tip_refresh_failure_holdoff_remaining(), 0.0)

        server._schedule_tip_refresh_retry()
        self.assertTrue(server._wait_for_blockpoll_trigger())
        # The trigger only released after the spacing window, never before.
        self.assertGreaterEqual(time.monotonic(), deadline)
        self.assertFalse(server._tip_refresh_retry.is_set())

    def test_holdoff_wait_keeps_blockpoll_heartbeat_alive(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()
        server.blockpoll_seconds = 30.0
        server.tip_refresh_failure_holdoff_seconds = 0.4

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        _block_refreshes(server)
        with patch("lab.prism.prism_coordinator.random.uniform", return_value=0.0):
            self._fail_one_poll(server)

        server._schedule_tip_refresh_retry()
        started = time.monotonic()
        self.assertTrue(server._wait_for_blockpoll_trigger())
        self.assertGreaterEqual(time.monotonic() - started, 0.3)
        # The deliberately held poller must keep beating so a holdoff longer
        # than the watchdog timeout cannot read as a hung loop.
        beat = server._heartbeats.get("qbit_blockpoll")
        self.assertIsNotNone(beat)
        assert beat is not None
        self.assertGreaterEqual(beat, started)

    def test_trigger_without_prior_failure_stays_immediate(self) -> None:
        server, _rpc = coordinator()
        server.blockpoll_seconds = 30.0

        server._schedule_tip_refresh_retry()
        started = time.monotonic()
        self.assertTrue(server._wait_for_blockpoll_trigger())
        self.assertLess(time.monotonic() - started, 0.2)

    def test_new_tip_observation_releases_the_holdoff(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()
        server.blockpoll_seconds = 30.0
        server.tip_refresh_failure_holdoff_seconds = 30.0

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        _block_refreshes(server)
        self._fail_one_poll(server)
        self.assertGreater(server._tip_refresh_failure_holdoff_remaining(), 0.0)

        # Blockwait's push path: record the detection, then arm the poller
        # trigger. The armed holdoff must not delay that wake even though the
        # newer tip is not yet published as share-validation authority.
        self.assertTrue(server.observe_tip_for_refresh(NEW_TIP))
        self.assertEqual(server._tip_refresh_failure_holdoff_remaining(), 0.0)
        server._schedule_tip_refresh_retry()
        started = time.monotonic()
        self.assertTrue(server._wait_for_blockpoll_trigger())
        self.assertLess(time.monotonic() - started, 0.2)

    def test_successful_pass_clears_the_holdoff(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()
        server.tip_refresh_failure_holdoff_seconds = 30.0
        original_reconcile = server.ensure_reorg_reconciled_for_tip

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        _block_refreshes(server)
        self._fail_one_poll(server)
        self.assertGreater(server._tip_refresh_failure_holdoff_remaining(), 0.0)

        server.ensure_reorg_reconciled_for_tip = original_reconcile  # type: ignore[method-assign]
        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertIsNone(server._tip_refresh_failure_holdoff_until)
        self.assertEqual(server._tip_refresh_failure_holdoff_remaining(), 0.0)

    def test_zero_holdoff_restores_unspaced_retries(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()
        server.tip_refresh_failure_holdoff_seconds = 0.0

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        _block_refreshes(server)
        self._fail_one_poll(server)

        self.assertIsNone(server._tip_refresh_failure_holdoff_until)
        self.assertEqual(server._tip_refresh_failure_holdoff_remaining(), 0.0)

    def test_holdoff_includes_bounded_jitter(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_failure_holdoff_seconds = 1.0

        before = time.monotonic()
        server._note_tip_refresh_attempt_failed()
        deadline = server._tip_refresh_failure_holdoff_until

        self.assertIsNotNone(deadline)
        assert deadline is not None
        delay = deadline - before
        self.assertGreaterEqual(delay, 1.0)
        self.assertLessEqual(delay, 1.3)


class SameTipTemplateReuseTests(unittest.TestCase):
    def test_same_tip_poll_within_window_skips_getblocktemplate(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]
        server.clients = {state}

        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            self.assertEqual(rpc.count("getblocktemplate"), 1)
            self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(rpc.count("getblocktemplate"), 1)
        self.assertEqual(server.job_cache_hit_counts["template"], 1)
        # The reused pass rebuilt nothing: the client's job is still the one
        # delivered from the originally fetched template.
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )

    def test_blocked_passes_cost_one_template_per_tip(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertEqual(rpc.count("getblocktemplate"), 1)

        _block_refreshes(server)
        for _attempt in range(5):
            with self.assertRaisesRegex(TemplateRefreshBlocked, "untrusted"):
                server.poll_qbit_tip_template_once()

        self.assertEqual(rpc.count("getblocktemplate"), 1)
        self.assertEqual(server.job_cache_hit_counts["template"], 5)

    def test_tip_change_bypasses_template_reuse(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertEqual(rpc.count("getblocktemplate"), 1)

        rpc.tip = NEW_TIP
        rpc.template = base_template(height=11, prevhash=NEW_TIP)
        self.assertEqual(server.poll_qbit_tip_template_once(), 0)

        self.assertEqual(rpc.count("getblocktemplate"), 2)
        assert server.tip_template_snapshot is not None
        self.assertEqual(server.tip_template_snapshot.bestblockhash, NEW_TIP)

    def test_zero_window_disables_template_reuse(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()
        server.template_cache_seconds = 0.0

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertEqual(server.poll_qbit_tip_template_once(), 0)

        self.assertEqual(rpc.count("getblocktemplate"), 2)
        self.assertEqual(server.job_cache_hit_counts["template"], 0)

    def test_expired_window_refetches_the_template(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.clients = set()
        server.template_cache_seconds = 0.05

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        time.sleep(0.06)
        self.assertEqual(server.poll_qbit_tip_template_once(), 0)

        self.assertEqual(rpc.count("getblocktemplate"), 2)


if __name__ == "__main__":
    unittest.main()

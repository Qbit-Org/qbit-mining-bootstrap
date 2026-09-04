#!/usr/bin/env python3
"""Issue #207: the periodic self-check must leave the Rust daemon prepared.

The daemon here is an in-process fake that serves ``prepare_window`` with
the real Python fold, exactly like the subprocess fixture the window-pipeline
suite uses, but addressable from the test: it counts full preparations and
advances, answers ``needs_full`` for a digest it does not hold, and can be
evicted on demand. That makes three coordinator contracts checkable without
a Rust build:

- ordinary in-band drift keeps the cached weight and digest warm, so the
  next build advances through the daemon without a second oracle read;
- an oversized re-center adopts the live-weight digest AND prepares the
  daemon for it before the next advance, so the deliberate re-center never
  pays a second full ``snapshot_at_job_issue`` under the writer lock;
- a genuine eviction still reports ``window_daemon_state_lost`` and takes
  the existing recovery path, and the strings stay distinguishable from a
  deliberate re-center that could not prepare the daemon.
"""
# ruff: noqa: F403, F405

from __future__ import annotations

import os
import unittest

from lab.prism.bundle_compiler import PreparedWindowOutcome
from lab.prism.payout_state import PRISM_REWARD_WINDOW_MULTIPLIER
from lab.prism.payout_state import PRISM_SNAPSHOT_WINDOW_MARGIN
from lab.prism.share_ledger import (
    AcceptedShareRecord,
    DaemonShareWindowMirror,
    IncrementalShareWindow,
    IncrementalWindowFallback,
    PendingShare,
)
from tests.prism_coordinator_test_support import *


def _canonical_items(window: IncrementalShareWindow) -> bytes:
    return b",".join(
        page.canonical_json_items
        for page in window.pages
        if page.canonical_json_items
    )


class InProcessFakeDaemon:
    """``prepare_payout_window`` served by the shipped Python fold.

    Held windows are addressed by digest alone, like the real daemon; an
    advance against a digest that is not held answers ``needs_full``.
    """

    def __init__(self) -> None:
        self.held: dict[str, IncrementalShareWindow] = {}
        self.full_prepares = 0
        self.advances = 0
        self.needs_full_answers = 0
        self.full_prepare_statuses: list[str] = []
        # Statuses to answer the next full preparations with instead of
        # folding (fault injection for the unprepared-adoption test).
        self.inject_full_statuses: list[str] = []

    def evict(self) -> None:
        self.held.clear()

    def __call__(
        self,
        *,
        mode: str,
        records_json: list[dict[str, object]],
        anchor_job_issued_at_ms: int,
        append_invalidation_epoch: int,
        base_digest: str | None = None,
        window_weight: int | None = None,
        page_size: int | None = None,
        wait_for_daemon: bool = True,
    ) -> PreparedWindowOutcome | None:
        records = [
            AcceptedShareRecord(
                **{
                    key: value
                    for key, value in record_json.items()
                    if value is not None or key == "credit_policy"
                }
            )
            for record_json in records_json
        ]
        anchor = int(anchor_job_issued_at_ms)
        if mode == "full":
            self.full_prepares += 1
            if self.inject_full_statuses:
                status = self.inject_full_statuses.pop(0)
                self.full_prepare_statuses.append(status)
                return PreparedWindowOutcome(status=status)
            assert window_weight is not None
            window = IncrementalShareWindow.from_full_snapshot(
                records,
                anchor_job_issued_at_ms=anchor,
                window_weight=int(window_weight),
                page_size=int(page_size or 512),
            )
            digest = window.json_records().canonical_json_sha256()
            self.held = {digest: window}
            self.full_prepare_statuses.append("prepared")
            return PreparedWindowOutcome(
                status="prepared",
                share_snapshot_sha256=digest,
                record_count=window.record_count,
                window_items=_canonical_items(window),
            )
        assert mode == "advance", mode
        base = self.held.get(str(base_digest))
        if base is None:
            self.needs_full_answers += 1
            return PreparedWindowOutcome(
                status="needs_full",
                error=f"prepared window {base_digest} is not held",
            )
        old_items = _canonical_items(base)
        try:
            advanced, stats = base.advance(
                records,
                anchor_job_issued_at_ms=anchor,
            )
        except IncrementalWindowFallback as error:
            return PreparedWindowOutcome(status="fallback", error=str(error))
        digest = advanced.json_records().canonical_json_sha256()
        new_items = _canonical_items(advanced)
        drop = 0
        while drop <= len(old_items):
            retained = old_items[drop:]
            if new_items[: len(retained)] == retained:
                break
            drop += 1
        appended = new_items[len(old_items) - drop :]
        self.held = {digest: advanced}
        self.advances += 1
        return PreparedWindowOutcome(
            status="prepared",
            share_snapshot_sha256=digest,
            record_count=advanced.record_count,
            added_rows=stats.added_rows,
            expired_rows=stats.expired_rows,
            touched_pages=stats.touched_pages,
            retained_drop_bytes=drop,
            appended_items=appended,
        )


def _append_heavy_share(
    ledger: SingleWriterShareLedger,
    *,
    share_seq: int,
    accepted_at_ms: int,
    share_difficulty: int,
) -> None:
    """A share heavy enough that a handful of them fill the reward window."""
    ledger.append(
        PendingShare(
            share_id=f"heavy-{share_seq}",
            miner_id=f"miner-{share_seq % 3}",
            order_key=f"miner-{share_seq % 3}",
            p2mr_program_hex=f"{share_seq % 256:02x}" * 32,
            share_difficulty=share_difficulty,
            network_difficulty=share_difficulty,
            template_height=9,
            job_id=f"job-{share_seq}",
            job_issued_at_ms=accepted_at_ms - 1,
            accepted_at_ms=accepted_at_ms,
            ntime=1_700_000_000 + share_seq,
        )
    )


class DaemonRecenterTests(unittest.TestCase):
    """Issue #207 regression tests against the in-process fake daemon."""

    def setUp(self) -> None:
        patcher = patch.dict(os.environ, {"PRISM_WINDOW_PIPELINE_RUST": "1"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def _server(
        self,
    ) -> tuple[PrismCoordinator, IncrementalRecordingLedger, object, InProcessFakeDaemon]:
        ledger = IncrementalRecordingLedger()
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        difficulty = int(artifacts.network_difficulty)
        # Twenty shares each worth one unit of network difficulty: the
        # window at the live weight (16 * difficulty) retains sixteen, so a
        # re-center to a smaller weight changes the retained set and hence
        # the digest the daemon must hold.
        for share_seq in range(1, 21):
            _append_heavy_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_000 + share_seq * 10,
                share_difficulty=difficulty,
            )
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 60.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        daemon = InProcessFakeDaemon()
        server.prepare_payout_window = daemon  # type: ignore[method-assign]
        return server, ledger, artifacts, daemon

    @staticmethod
    def _oracle_digest(
        ledger: IncrementalRecordingLedger,
        *,
        anchor_ms: int,
        window_weight: int,
    ) -> str:
        records = list(
            SingleWriterShareLedger.snapshot_at_job_issue(
                ledger,
                anchor_ms,
                window_weight=None,
            )
        )
        window = IncrementalShareWindow.from_full_snapshot(
            records,
            anchor_job_issued_at_ms=anchor_ms,
            window_weight=window_weight,
        )
        return window.json_records().canonical_json_sha256()

    def test_oversized_recenter_leaves_the_daemon_prepared(self) -> None:
        server, ledger, artifacts, daemon = self._server()
        difficulty = int(artifacts.network_difficulty)
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(0, 0, difficulty)
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")
            self.assertEqual(daemon.full_prepares, 1)
            self.assertEqual(ledger.full_snapshot_calls, 1)

            # Difficulty collapses far enough (cached/live = 4/3 > 5/4) that
            # the periodic self-check re-centers the window at the live
            # weight; the adopted digest differs from the daemon's.
            server.payout_artifact_min_build_interval_seconds = 0.0
            server.payout_artifact_full_rescan_seconds = 0.0
            clock_ms[0] = 1_000_020
            collapsed_difficulty = difficulty * 3 // 4
            live_weight = (
                PRISM_REWARD_WINDOW_MULTIPLIER
                * PRISM_SNAPSHOT_WINDOW_MARGIN
                * collapsed_difficulty
            )
            checked = server._build_payout_ledger_artifact(
                0, 0, collapsed_difficulty
            )
            assert checked is not None
            self.assertEqual(checked.window_build_mode, "self_check_match")
            self.assertEqual(
                checked.window_full_rescan_reason, "periodic_self_check"
            )
            # The deliberate re-center is flagged on the build itself.
            self.assertTrue(checked.window_self_check_recentered)
            self.assertFalse(initial.window_self_check_recentered)
            self.assertEqual(ledger.full_snapshot_calls, 2)
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, DaemonShareWindowMirror)
            self.assertEqual(int(cached.window.window_weight), live_weight)
            self.assertIsNone(cached.daemon_unprepared_reason)
            # Byte-exact parity with the in-process oracle at the live
            # weight, and the daemon holds exactly that digest.
            self.assertNotEqual(
                checked.share_snapshot_sha256, initial.share_snapshot_sha256
            )
            self.assertEqual(
                checked.share_snapshot_sha256,
                self._oracle_digest(
                    ledger,
                    anchor_ms=int(checked.snapshot_anchor_ms or 0),
                    window_weight=live_weight,
                ),
            )
            self.assertEqual(daemon.full_prepares, 2)
            self.assertIn(checked.share_snapshot_sha256, daemon.held)

            # The next build advances incrementally: no second full ledger
            # snapshot, no needs_full, no window_daemon_state_lost.
            server.payout_artifact_full_rescan_seconds = 3_600.0
            _append_heavy_share(
                ledger,
                share_seq=21,
                accepted_at_ms=1_000_030,
                share_difficulty=difficulty,
            )
            clock_ms[0] = 1_000_040
            advanced = server._build_payout_ledger_artifact(
                0, 0, collapsed_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertIsNone(advanced.window_full_rescan_reason)
            self.assertEqual(advanced.window_delta_rows, 1)
            self.assertEqual(ledger.full_snapshot_calls, 2)
            self.assertEqual(daemon.needs_full_answers, 0)
            self.assertEqual(daemon.full_prepares, 2)
            self.assertEqual(
                advanced.share_snapshot_sha256,
                self._oracle_digest(
                    ledger,
                    anchor_ms=int(advanced.snapshot_anchor_ms or 0),
                    window_weight=live_weight,
                ),
            )

    def test_in_band_drift_keeps_the_cached_weight_and_digest_warm(self) -> None:
        server, ledger, artifacts, daemon = self._server()
        difficulty = int(artifacts.network_difficulty)
        cached_weight = (
            PRISM_REWARD_WINDOW_MULTIPLIER
            * PRISM_SNAPSHOT_WINDOW_MARGIN
            * difficulty
        )
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(0, 0, difficulty)
            assert initial is not None
            self.assertEqual(daemon.full_prepares, 1)

            # ASERT-sized drift: inside the band and far from the 1.25x
            # re-center threshold. The self-check compares and adopts at
            # the cached weight, whose digest the daemon already holds.
            server.payout_artifact_min_build_interval_seconds = 0.0
            server.payout_artifact_full_rescan_seconds = 0.0
            clock_ms[0] = 1_000_020
            drifted_difficulty = (difficulty * 101 + 99) // 100
            checked = server._build_payout_ledger_artifact(
                0, 0, drifted_difficulty
            )
            assert checked is not None
            self.assertEqual(checked.window_build_mode, "self_check_match")
            self.assertEqual(
                checked.window_full_rescan_reason, "periodic_self_check"
            )
            self.assertFalse(checked.window_self_check_recentered)
            self.assertEqual(
                checked.share_snapshot_sha256, initial.share_snapshot_sha256
            )
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, DaemonShareWindowMirror)
            self.assertEqual(int(cached.window.window_weight), cached_weight)
            self.assertIsNone(cached.daemon_unprepared_reason)
            # Warm: no second full preparation was needed.
            self.assertEqual(daemon.full_prepares, 1)
            self.assertIn(checked.share_snapshot_sha256, daemon.held)

            server.payout_artifact_full_rescan_seconds = 3_600.0
            _append_heavy_share(
                ledger,
                share_seq=21,
                accepted_at_ms=1_000_030,
                share_difficulty=difficulty,
            )
            clock_ms[0] = 1_000_040
            advanced = server._build_payout_ledger_artifact(
                0, 0, drifted_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertEqual(advanced.window_delta_rows, 1)
            self.assertEqual(ledger.full_snapshot_calls, 2)
            self.assertEqual(daemon.needs_full_answers, 0)
            self.assertEqual(daemon.full_prepares, 1)

    def test_genuine_daemon_eviction_still_reports_window_daemon_state_lost(
        self,
    ) -> None:
        server, ledger, artifacts, daemon = self._server()
        difficulty = int(artifacts.network_difficulty)
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(0, 0, difficulty)
            assert initial is not None
            # A respawn or LRU eviction: the daemon forgets its prepared
            # state while the coordinator mirror stays digest-verified.
            daemon.evict()
            _append_heavy_share(
                ledger,
                share_seq=21,
                accepted_at_ms=1_000_010,
                share_difficulty=difficulty,
            )
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            rebuilt = server._build_payout_ledger_artifact(0, 0, difficulty)
            assert rebuilt is not None
            self.assertEqual(rebuilt.window_build_mode, "full_rescan")
            self.assertEqual(
                rebuilt.window_full_rescan_reason,
                "window_daemon_state_lost",
            )
            # The existing recovery path: one oracle read re-seeds the
            # daemon through the full preparation and the cache stays a
            # daemon mirror the next build can advance.
            self.assertEqual(daemon.needs_full_answers, 1)
            self.assertEqual(daemon.full_prepares, 2)
            self.assertEqual(ledger.full_snapshot_calls, 2)
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, DaemonShareWindowMirror)
            self.assertIn(rebuilt.share_snapshot_sha256, daemon.held)
            self.assertEqual(
                rebuilt.share_snapshot_sha256,
                self._oracle_digest(
                    ledger,
                    anchor_ms=int(rebuilt.snapshot_anchor_ms or 0),
                    window_weight=PRISM_REWARD_WINDOW_MULTIPLIER
                    * PRISM_SNAPSHOT_WINDOW_MARGIN
                    * difficulty,
                ),
            )
            # A genuine eviction is never flagged as a re-center, so
            # telemetry can tell the two apart.
            self.assertFalse(rebuilt.window_self_check_recentered)

    def test_unprepared_recenter_is_distinguishable_from_daemon_state_loss(
        self,
    ) -> None:
        # When the daemon cannot be prepared at re-center time (busy under
        # another build), the adopted mirror is tagged with the daemon's own
        # outcome and the next build's rebuild names that outcome rather
        # than blaming a daemon eviction; recovery is the existing
        # full-oracle path. The reason stays inside the closed vocabulary
        # the #224 telemetry owner exports.
        server, ledger, artifacts, daemon = self._server()
        difficulty = int(artifacts.network_difficulty)
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(0, 0, difficulty)
            assert initial is not None
            server.payout_artifact_min_build_interval_seconds = 0.0
            server.payout_artifact_full_rescan_seconds = 0.0
            clock_ms[0] = 1_000_020
            collapsed_difficulty = difficulty * 3 // 4
            daemon.inject_full_statuses = ["busy"]
            checked = server._build_payout_ledger_artifact(
                0, 0, collapsed_difficulty
            )
            assert checked is not None
            self.assertEqual(checked.window_build_mode, "self_check_match")
            self.assertEqual(
                checked.window_full_rescan_reason, "periodic_self_check"
            )
            self.assertTrue(checked.window_self_check_recentered)
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertEqual(cached.daemon_unprepared_reason, "window_daemon_busy")
            self.assertEqual(daemon.full_prepare_statuses, ["prepared", "busy"])

            server.payout_artifact_full_rescan_seconds = 3_600.0
            _append_heavy_share(
                ledger,
                share_seq=21,
                accepted_at_ms=1_000_030,
                share_difficulty=difficulty,
            )
            clock_ms[0] = 1_000_040
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, collapsed_difficulty
            )
            assert rebuilt is not None
            self.assertEqual(rebuilt.window_build_mode, "full_rescan")
            self.assertEqual(rebuilt.window_full_rescan_reason, "window_daemon_busy")
            self.assertNotEqual(
                rebuilt.window_full_rescan_reason, "window_daemon_state_lost"
            )
            self.assertFalse(rebuilt.window_self_check_recentered)
            self.assertEqual(daemon.needs_full_answers, 1)
            self.assertEqual(daemon.full_prepares, 3)
            recovered = server._incremental_payout_artifact_window
            assert recovered is not None
            self.assertIsNone(recovered.daemon_unprepared_reason)
            self.assertIn(rebuilt.share_snapshot_sha256, daemon.held)


if __name__ == "__main__":
    unittest.main()

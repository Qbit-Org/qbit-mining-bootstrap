#!/usr/bin/env python3
"""Focused PRISM coordinator metrics tests."""
# ruff: noqa: F821

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest import mock
from tests import prism_coordinator_test_support as _job_support
from tests import prism_vardiff_test_support as _vardiff_support


def install_isolated_coordinator(
    test_case: unittest.TestCase,
    factory: object,
) -> None:
    assert callable(factory)

    def isolated(*args: object, **kwargs: object) -> object:
        result = factory(*args, **kwargs)
        server = result[0] if isinstance(result, tuple) else result
        temporary = tempfile.TemporaryDirectory()
        server.audit_dir = Path(temporary.name) / "audit"
        server.evidence_path = Path(temporary.name) / "state" / "evidence.json"

        def cleanup() -> None:
            store = server.__dict__.get("_audit_artifact_store")
            if store is not None:
                store.close()
            temporary.cleanup()

        test_case.addCleanup(cleanup)
        return result

    globals()["coordinator"] = isolated


class _VardiffSupportTestCase(unittest.TestCase):
    def setUp(self) -> None:
        globals().update(
            {name: getattr(_vardiff_support, name) for name in _vardiff_support.__all__}
        )
        install_isolated_coordinator(self, _vardiff_support.coordinator)


class _JobSupportTestCase(unittest.TestCase):
    def setUp(self) -> None:
        globals().update(
            {name: getattr(_job_support, name) for name in _job_support.__all__}
        )
        install_isolated_coordinator(self, _job_support.coordinator)


class PrismCoordinatorVardiffTests(_VardiffSupportTestCase):
    def test_metrics_include_issue_scope_operational_gauges(self) -> None:
        server = coordinator()
        server.submitted_share_count = 10
        server.stale_share_count = 2
        server.duplicate_share_count = 1
        server.low_difficulty_share_count = 3
        server.grace_credited_share_count = 6
        server.idle_retarget_count = 7
        server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB] = 2
        server.rejection_counts_by_reason["duplicate-share"] = 1
        server.rejection_counts_by_reason["low-difficulty"] = 3
        server.tip_refresh_job_count = 4
        server.post_accept_refresh_failure_count = 5
        server.connection_limit_rejection_counts = {"global": 2, "username": 3}
        server.accept_resource_exhaustion_count = 4
        server.connection_setup_failure_count = 5
        server._ensure_vardiff_idle_state()
        with server._vardiff_idle_lock:
            server.vardiff_idle_clients_inspected = 8
            server.vardiff_idle_skip_counts["busy"] = 2
            server.vardiff_idle_queue_depth = 1
            server.vardiff_idle_inflight = 2
            server.vardiff_idle_task_failures = 1
        server._observe_vardiff_idle_seconds("sweep", 0.005)
        server._observe_vardiff_idle_seconds("task", 0.01)

        metrics = server.metrics_payload()

        self.assertIn("qbit_prism_submitted_shares_total 10", metrics)
        self.assertIn("qbit_prism_stale_shares_total 2", metrics)
        self.assertIn("qbit_prism_duplicate_shares_total 1", metrics)
        self.assertIn("qbit_prism_low_difficulty_shares_total 3", metrics)
        self.assertIn("qbit_prism_grace_credited_shares_total 6", metrics)
        self.assertIn("qbit_prism_stratum_active_connections 0", metrics)
        self.assertIn("qbit_prism_stratum_connection_limit 384", metrics)
        self.assertIn("qbit_prism_stratum_peak_active_connections 0", metrics)
        self.assertIn("qbit_prism_stratum_pending_initial_jobs 0", metrics)
        self.assertIn("qbit_prism_stratum_pending_initial_job_limit 128", metrics)
        self.assertIn(
            "qbit_prism_stratum_oldest_genuinely_pending_initial_job_seconds 0.0",
            metrics,
        )
        self.assertIn(
            "qbit_prism_stratum_current_tip_coverage_gap_seconds 0.0",
            metrics,
        )
        self.assertIn("qbit_prism_stratum_current_tip_job_coverage 1.0", metrics)
        self.assertIn("qbit_prism_stratum_handler_threads 0", metrics)
        self.assertIn("qbit_prism_job_delivery_queue_depth 0", metrics)
        self.assertIn("qbit_prism_job_delivery_active_workers 0", metrics)
        self.assertIn(
            'qbit_prism_stratum_connection_limit_rejections_total{scope="global"} 2',
            metrics,
        )
        self.assertIn(
            'qbit_prism_stratum_connection_limit_rejections_total{scope="username"} 3',
            metrics,
        )
        self.assertIn("qbit_prism_stratum_accept_resource_exhaustions_total 4", metrics)
        self.assertIn("qbit_prism_stratum_connection_setup_failures_total 5", metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="stale-job"} 2', metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="duplicate-share"} 1', metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="low-difficulty"} 3', metrics)
        self.assertIn("qbit_prism_tip_refresh_jobs_total 4", metrics)
        self.assertIn("qbit_prism_post_accept_refresh_failures_total 5", metrics)
        self.assertIn("qbit_prism_vardiff_idle_retargets_total 7", metrics)
        self.assertIn("qbit_prism_vardiff_idle_clients_inspected_total 8", metrics)
        self.assertIn('qbit_prism_vardiff_idle_skips_total{reason="busy"} 2', metrics)
        self.assertIn("qbit_prism_vardiff_idle_queue_depth 1", metrics)
        self.assertIn("qbit_prism_vardiff_idle_inflight 2", metrics)
        self.assertIn("qbit_prism_vardiff_idle_task_failures_total 1", metrics)
        self.assertIn("qbit_prism_vardiff_idle_sweep_seconds_count 1", metrics)
        self.assertIn("qbit_prism_vardiff_idle_retarget_task_seconds_count 1", metrics)
        self.assertIn("qbit_prism_stale_share_percent 20", metrics)
        self.assertIn("qbit_prism_coinbase_weight_headroom_bytes 1999750", metrics)
        self.assertIn("qbit_prism_vardiff_enabled 1", metrics)
        self.assertIn("qbit_prism_qbitd_initial_block_download 0", metrics)
        self.assertIn("qbit_prism_qbitd_peers 4", metrics)

    def test_hot_lock_and_pending_candidate_metrics_are_bounded(self) -> None:
        server = coordinator()
        server.lock = _ObservedRLock()
        lock_wait_started = threading.Event()

        def contend() -> None:
            lock_wait_started.set()
            with server.lock:
                pass

        with server.lock:
            waiter = threading.Thread(target=contend)
            waiter.start()
            self.assertTrue(lock_wait_started.wait(1))
            time.sleep(0.02)
        waiter.join(1)
        self.assertFalse(waiter.is_alive())

        ledger = SingleWriterShareLedger()
        block_hash = "bc" * 32
        ledger.persist_block_candidate_intent(
            {
                "schema": "qbit.prism.block-candidate-intent.v1",
                "block_hash_hex": block_hash,
                "block_hex": "00",
            }
        )
        server.ledger = ledger
        before_attempt = "\n".join(
            server.coordinator_lock_metrics_lines()
            + server.block_submitter_metrics_lines()
        )
        self.assertIn("qbit_prism_coordinator_lock_contentions_total 1", before_attempt)
        self.assertIn("qbit_prism_block_candidates_pending 1", before_attempt)
        self.assertRegex(
            before_attempt,
            r"qbit_prism_block_candidate_oldest_unattempted_seconds 0\.\d+",
        )

        self.assertTrue(ledger.mark_block_candidate_attempted(block_hash=block_hash))
        after_attempt = "\n".join(server.block_submitter_metrics_lines())
        self.assertIn(
            "qbit_prism_block_candidate_oldest_unattempted_seconds 0.000000",
            after_attempt,
        )

    def test_metrics_include_bounded_worker_share_and_rejection_counters(self) -> None:
        server = coordinator()
        server.worker_metrics_limit = 1

        server.note_worker_submitted_share("miner-a")
        server.note_worker_accepted_share("miner-a", PRISM_CREDIT_POLICY_STALE_GRACE)
        server.note_worker_submitted_share("miner-b")
        server.record_rejection(PRISM_REJECTION_LOW_DIFFICULTY, worker="miner-b")

        metrics = server.metrics_payload()

        self.assertIn('qbit_prism_worker_submitted_shares_total{worker="miner-a"} 1', metrics)
        self.assertIn('qbit_prism_worker_accepted_shares_total{worker="miner-a"} 1', metrics)
        self.assertIn('qbit_prism_worker_grace_credited_shares_total{worker="miner-a"} 1', metrics)
        self.assertIn('qbit_prism_worker_submitted_shares_total{worker="_other"} 1', metrics)
        self.assertIn(
            'qbit_prism_worker_rejections_total{worker="_other",reason_id="low-difficulty"} 1',
            metrics,
        )
    def test_metrics_include_ctv_broadcaster_progress_and_pass_duration(self) -> None:
        server = coordinator()
        server._record_ctv_fanout_broadcaster_progress()
        server._record_ctv_fanout_broadcaster_progress()
        server.observe_ctv_fanout_broadcaster_pass(102.0)
        server.observe_ctv_fanout_broadcaster_chunk(
            SimpleNamespace(processed_count=1, elapsed_seconds=0.25)
        )
        server._record_ctv_fanout_broadcaster_yield()

        metrics = server.metrics_payload()

        self.assertIn("qbit_prism_ctv_fanout_broadcaster_processed_rows_total 2", metrics)
        self.assertIn(
            'qbit_prism_ctv_fanout_broadcaster_pass_seconds_bucket{le="60"} 0',
            metrics,
        )
        self.assertIn(
            'qbit_prism_ctv_fanout_broadcaster_pass_seconds_bucket{le="120"} 1',
            metrics,
        )
        self.assertIn("qbit_prism_ctv_fanout_broadcaster_pass_seconds_sum 102.000000", metrics)
        self.assertIn("qbit_prism_ctv_fanout_broadcaster_pass_seconds_count 1", metrics)
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total 1",
            metrics,
        )
        self.assertIn(
            'qbit_prism_ctv_fanout_broadcaster_chunk_seconds_bucket{le="0.25"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_ctv_fanout_broadcaster_chunk_rows_bucket{le="1"} 1',
            metrics,
        )
    def test_zero_worker_metric_limit_uses_overflow_bucket(self) -> None:
        server = coordinator()
        server.worker_metrics_limit = 0

        server.note_worker_submitted_share("miner-a")

        self.assertEqual(set(server.worker_share_counts), {"_other"})
        self.assertEqual(server.worker_share_counts["_other"]["submitted"], 1)
    def test_unauthorized_submit_does_not_admit_payload_worker_metric_label(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.worker_metrics_limit = 1

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["spoofed-miner", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNAUTHORIZED_WORKER)
        self.assertNotIn("spoofed-miner", server.worker_share_counts)
        self.assertEqual(server.worker_share_counts["miner-a"]["submitted"], 0)
        self.assertEqual(
            server.worker_rejection_counts[("miner-a", PRISM_REJECTION_UNAUTHORIZED_WORKER)],
            1,
        )

        server.note_worker_submitted_share("miner-a")

        self.assertNotIn(PRISM_WORKER_METRICS_OVERFLOW_LABEL, server.worker_share_counts)
        self.assertEqual(server.worker_share_counts["miner-a"]["submitted"], 1)
    def test_metrics_include_audit_artifact_storage_gauges(self) -> None:
        server = coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            files = {
                f"prism-audit-bundle-body-{'aa' * 32}-{'bb' * 32}.json": b"abc",
                f"prism-audit-share-segment-1-1-{'cc' * 32}.json": b"defg",
                f"prism-live-audit-bundle-1-{'dd' * 32}.json": b"hi",
                f"prism-live-audit-bundle-candidate-{'ee' * 32}.json": b"j",
                f".prism-live-audit-bundle-candidate-{'ff' * 32}.json.tmp": b"klmno",
                "operator-note.txt": b"pqrstu",
            }
            for name, body in files.items():
                (Path(tempdir) / name).write_bytes(body)

            metrics = server.metrics_payload()

        self.assertIn('qbit_prism_audit_artifact_bytes{kind="body"} 3', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="body"} 1', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="share_segment"} 4', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="share_segment"} 1', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="live_bundle"} 2', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="live_bundle"} 1', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="candidate"} 6', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="candidate"} 2', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="other"} 6', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="other"} 1', metrics)
        self.assertIn("qbit_prism_audit_artifact_scan_error 0", metrics)
    def test_metrics_payload_reads_one_a1_snapshot(self) -> None:
        server = coordinator()
        snapshot = {
            "body": {"files": 1, "bytes": 2},
            "share_segment": {"files": 3, "bytes": 4},
            "live_bundle": {"files": 5, "bytes": 6},
            "candidate": {"files": 7, "bytes": 8},
            "other": {"files": 9, "bytes": 10},
            "scan_error": 0,
        }
        reader = mock.Mock(return_value=snapshot)
        server.audit_artifact_metrics = reader  # type: ignore[method-assign]

        metrics = server.metrics_payload()

        reader.assert_called_once_with()
        self.assertIn('qbit_prism_audit_artifact_files{kind="body"} 1', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="other"} 10', metrics)
        self.assertIn("qbit_prism_audit_artifact_scan_error 0", metrics)
    def _pending_append(self, tag: str, accepted_at_ms: int = 2) -> PendingShareAppend:
        from lab.prism.share_ledger import PendingShare

        return PendingShareAppend(
            pending_share=PendingShare(
                share_id=f"miner-a:{tag}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=10,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1_700_000_000,
            ),
            username="miner-a",
            job_id="job-1",
            block_hash_hex=tag * 32,
            collection_only=False,
            credit_policy=None,
        )

class PrismCoordinatorReliabilityTests(_VardiffSupportTestCase):
    def _bare_coordinator(self) -> PrismCoordinator:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.stop_event = threading.Event()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 120.0
        server.watchdog_interval_seconds = 15.0
        return server
    def test_overdue_heartbeats_flags_only_stale_subsystems(self) -> None:
        server = self._bare_coordinator()
        server._record_heartbeat("stratum_accept")
        server._record_heartbeat("qbit_blockpoll")
        now = time.monotonic()

        self.assertEqual(server._overdue_heartbeats(now), [])

        with server._heartbeats_lock:
            server._heartbeats["qbit_blockpoll"] = now - 1_000.0

        self.assertEqual(server._overdue_heartbeats(now), ["qbit_blockpoll"])

    def test_block_submitter_retry_wait_heartbeats_in_bounded_slices(self) -> None:
        server = self._bare_coordinator()
        server.watchdog_timeout_seconds = 0.3
        clock = {"now": 0.0}
        waits: list[float] = []
        overdue_samples: list[list[str]] = []

        class AdvancingStopEvent:
            def is_set(self) -> bool:
                return False

            def wait(self, timeout: float) -> bool:
                waits.append(timeout)
                clock["now"] += timeout
                overdue_samples.append(server._overdue_heartbeats(clock["now"]))
                return False

        server.stop_event = AdvancingStopEvent()  # type: ignore[assignment]
        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            self.assertFalse(server._wait_for_block_candidate_retry(1.0))

        self.assertEqual(waits, [0.25, 0.25, 0.25, 0.25])
        self.assertEqual(overdue_samples, [[], [], [], []])
        self.assertEqual(server._heartbeats["block_submitter"], 1.0)

    def test_blocked_candidate_phase_remains_watchdog_eligible(self) -> None:
        server, state, _recording = submit_coordinator()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 0.05
        entered_submission = threading.Event()
        release_submission = threading.Event()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="fa" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
        )

        def blocked_submission(_candidate: PrismBlockCandidate) -> bool:
            entered_submission.set()
            release_submission.wait(2)
            return True

        server.submit_block_candidate = blocked_submission  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)
        server._record_heartbeat("block_submitter")
        submitter = threading.Thread(target=server.submit_next_block_candidate)
        submitter.start()
        try:
            self.assertTrue(entered_submission.wait(1))
            time.sleep(0.08)
            self.assertEqual(
                server._overdue_heartbeats(time.monotonic()),
                ["block_submitter"],
            )
        finally:
            release_submission.set()
            submitter.join(2)
        self.assertFalse(submitter.is_alive())

    def test_progressing_ctv_pass_longer_than_watchdog_timeout_stays_healthy(self) -> None:
        server = self._bare_coordinator()
        server.ctv_broadcaster_limit = 200
        server.ctv_broadcaster_interval_seconds = 30.0
        clock = {"now": 0.0}
        overdue_samples: list[list[str]] = []
        seen_limits: list[int] = []

        class StopAfterOnePass:
            def is_set(self) -> bool:
                return False

            def wait(self, timeout: float) -> bool:
                return True

        class ProgressingDaemon:
            def run_once(self, *, limit: int, progress_callback: object, **_kwargs: object) -> object:
                seen_limits.append(limit)
                assert callable(progress_callback)
                for _ in range(3):
                    clock["now"] += 80.0
                    overdue_samples.append(server._overdue_heartbeats(clock["now"]))
                    progress_callback()
                return SimpleNamespace(
                    scanned_count=3,
                    submitted_count=0,
                    updated_count=3,
                    failed_count=0,
                    yielded_to_tip_refresh=False,
                )

        server.stop_event = StopAfterOnePass()  # type: ignore[assignment]
        server.ctv_fanout_broadcast_daemon = ProgressingDaemon()

        with patch("lab.prism.prism_coordinator.time.monotonic", side_effect=lambda: clock["now"]), patch(
            "builtins.print"
        ):
            server.ctv_fanout_broadcaster_loop()

        self.assertGreater(clock["now"], server.watchdog_timeout_seconds)
        self.assertEqual(seen_limits, [200])
        self.assertEqual(overdue_samples, [[], [], []])
        self.assertEqual(server.ctv_broadcaster_processed_rows_total, 3)
        self.assertEqual(server.ctv_broadcaster_pass_count, 1)
    def test_ctv_pass_completion_heartbeat_precedes_interval_wait(self) -> None:
        server = self._bare_coordinator()
        server.ctv_broadcaster_limit = 200
        server.ctv_broadcaster_interval_seconds = 30.0
        clock = {"now": 0.0}
        wait_observation: dict[str, object] = {}

        class StopAfterIntervalWait:
            def is_set(self) -> bool:
                return False

            def wait(self, timeout: float) -> bool:
                wait_observation["timeout"] = timeout
                wait_observation["heartbeat"] = server._heartbeats["ctv_fanout_broadcaster"]
                wait_observation["overdue_after_wait"] = server._overdue_heartbeats(
                    clock["now"] + timeout
                )
                return True

        class IncidentDurationDaemon:
            def run_once(self, *, limit: int, progress_callback: object, **_kwargs: object) -> object:
                clock["now"] += 102.0
                return SimpleNamespace(
                    scanned_count=200,
                    submitted_count=0,
                    updated_count=200,
                    failed_count=0,
                    yielded_to_tip_refresh=False,
                )

        server.stop_event = StopAfterIntervalWait()  # type: ignore[assignment]
        server.ctv_fanout_broadcast_daemon = IncidentDurationDaemon()

        with patch("lab.prism.prism_coordinator.time.monotonic", side_effect=lambda: clock["now"]), patch(
            "builtins.print"
        ):
            server.ctv_fanout_broadcaster_loop()

        self.assertEqual(wait_observation["timeout"], 30.0)
        self.assertEqual(wait_observation["heartbeat"], 102.0)
        self.assertEqual(wait_observation["overdue_after_wait"], [])
    def test_ctv_pass_without_progress_remains_watchdog_eligible(self) -> None:
        server = self._bare_coordinator()
        server.ctv_broadcaster_limit = 200
        server.ctv_broadcaster_interval_seconds = 30.0
        clock = {"now": 0.0}
        entered_row = threading.Event()
        release_row = threading.Event()

        class BlockingDaemon:
            def run_once(self, *, limit: int, progress_callback: object, **_kwargs: object) -> object:
                entered_row.set()
                release_row.wait()
                return SimpleNamespace(
                    scanned_count=1,
                    submitted_count=0,
                    updated_count=1,
                    failed_count=0,
                    yielded_to_tip_refresh=False,
                )

        server.ctv_fanout_broadcast_daemon = BlockingDaemon()
        broadcaster_thread = threading.Thread(target=server.ctv_fanout_broadcaster_loop)
        with patch("lab.prism.prism_coordinator.time.monotonic", side_effect=lambda: clock["now"]), patch(
            "builtins.print"
        ):
            broadcaster_thread.start()
            self.assertTrue(entered_row.wait(timeout=1.0))
            clock["now"] = server.watchdog_timeout_seconds + 1.0
            self.assertEqual(
                server._overdue_heartbeats(clock["now"]),
                ["ctv_fanout_broadcaster"],
            )
            server.stop_event.set()
            release_row.set()
            broadcaster_thread.join(timeout=1.0)

        self.assertFalse(broadcaster_thread.is_alive())
    def test_watchdog_pause_suppresses_known_long_critical_section(self) -> None:
        server = self._bare_coordinator()
        server._record_heartbeat("stratum_accept")
        server._record_heartbeat("qbit_blockpoll")
        now = time.monotonic()
        with server._heartbeats_lock:
            server._heartbeats["stratum_accept"] = now - 1_000.0
            server._heartbeats["qbit_blockpoll"] = now - 1_000.0

        self.assertEqual(server._overdue_heartbeats(now), ["qbit_blockpoll", "stratum_accept"])

        with server._watchdog_paused("qbit_blockpoll", "stratum_accept"):
            self.assertEqual(server._overdue_heartbeats(now + 1_000.0), [])

        self.assertEqual(server._overdue_heartbeats(time.monotonic()), [])
    def test_block_submit_pause_names_cover_registered_refresh_and_idle_threads(self) -> None:
        server = self._bare_coordinator()
        for name in ("stratum_accept", "qbit_blockpoll", "qbit_blockwait", "vardiff_idle_sweep"):
            server._record_heartbeat(name)
        now = time.monotonic()
        with server._heartbeats_lock:
            for name in server._heartbeats:
                server._heartbeats[name] = now - 1_000.0

        pause_names = server._registered_watchdog_heartbeat_names(
            "qbit_blockpoll",
            "qbit_blockwait",
            "vardiff_idle_sweep",
            "stratum_accept",
        )

        with server._watchdog_paused(*pause_names):
            self.assertEqual(server._overdue_heartbeats(now + 1_000.0), [])
    def test_pause_names_skip_removed_blockwait_without_resurrecting_heartbeat(self) -> None:
        server = self._bare_coordinator()
        server._record_heartbeat("qbit_blockpoll")
        server._record_heartbeat("qbit_blockwait")
        server._remove_watchdog_heartbeat("qbit_blockwait")

        pause_names = server._registered_watchdog_heartbeat_names("qbit_blockpoll", "qbit_blockwait")

        self.assertEqual(pause_names, ("qbit_blockpoll",))
        with server._watchdog_paused(*pause_names):
            pass
        self.assertNotIn("qbit_blockwait", server._heartbeats)

class PrismStampedJobFloorTests(_VardiffSupportTestCase):
    """The listener floor must hold on the wire, not just in vardiff policy.

    Stamped jobs are the single choke point for every mining.set_difficulty
    the coordinator sends, and marketplace verification judges the first one.
    The regression here is a young chain: qbit network difficulty below the
    high-diff floor used to drag the advertised difficulty down with it.
    """
    def stamp_coordinator(self) -> PrismCoordinator:
        server = coordinator()
        server.job_counter = 0
        server.share_weights_by_username = {}
        server.default_share_weight = 1
        return server
    def cached_bundle(self) -> CachedJobBundle:
        # bits 207fffff: regtest-grade network difficulty (~4.7e-10), far
        # below the 500k marketplace floor.
        qbit_target = target_from_compact("207fffff")
        base_job = direct_stratum.DirectQbitStratumJob(
            job_id="prism-template-base",
            previousblockhash_display="00" * 32,
            prevhash="00" * 32,
            coinb1="",
            coinb2="",
            full_coinbase_prefix="",
            full_coinbase_suffix="",
            merkle_branch=(),
            transaction_hexes=(),
            version="20000000",
            nbits="207fffff",
            ntime="6553f100",
            qbit_target=qbit_target,
            share_target=qbit_target,
            share_difficulty=Decimal("1"),
            extranonce1_hex="ffffffff",
            extranonce2_size=8,
            clean_jobs=True,
        )
        return CachedJobBundle(
            key=("test",),
            template=gbt_template("00" * 32),
            template_fingerprint="fp",
            coinbase_manifest={},
            shares_json=[],
            prior_balances=[],
            found_block={"network_difficulty": 1},
            collection_only=False,
            issued_at_ms=12345,
            base_job=base_job,
            built_monotonic=time.monotonic(),
        )
    def highdiff_client(self) -> ClientState:
        state = client()
        state.worker = worker_identity()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.minimum_advertised_difficulty = Decimal("500000")
        state.share_difficulty = Decimal("500000")
        return state
    def test_post_block_refresh_runs_on_scheduler_worker(self) -> None:
        server, _state, _ledger = submit_coordinator()
        server.watchdog_timeout_seconds = 0.12
        seen: list[tuple[str, tuple[int, str] | None]] = []
        service = server._ensure_tip_refresh_service()
        execute_started = threading.Event()
        release_execute = threading.Event()
        repeated_heartbeat = threading.Event()
        caller_heartbeats: list[float] = []

        def heartbeat(name: str) -> None:
            server._record_heartbeat(name)
            if name == "block_submitter":
                caller_heartbeats.append(time.monotonic())
                if len(caller_heartbeats) >= 5:
                    repeated_heartbeat.set()

        def fake_execute(trigger: object) -> int:
            seen.append(
                (
                    threading.current_thread().name,
                    getattr(trigger, "post_accept_block"),
                )
            )
            execute_started.set()
            self.assertTrue(release_execute.wait(2.0))
            return 0

        service.reconfigure_ports_for_test(heartbeat=heartbeat)
        service._execute_refresh_trigger = fake_execute  # type: ignore[method-assign]
        results: list[int] = []
        caller = threading.Thread(
            target=lambda: results.append(
                server.refresh_jobs_after_accepted_block(
                    block_height=10,
                    block_hash="bb" * 32,
                    heartbeat_name="block_submitter",
                )
            )
        )
        caller.start()
        self.assertTrue(execute_started.wait(1.0))
        self.assertTrue(repeated_heartbeat.wait(1.0))
        self.assertGreaterEqual(
            caller_heartbeats[-1] - caller_heartbeats[0],
            server.watchdog_timeout_seconds,
        )
        self.assertNotIn(
            "block_submitter",
            server._overdue_heartbeats(time.monotonic()),
        )
        release_execute.set()
        caller.join(1.0)
        self.assertFalse(caller.is_alive())
        self.assertEqual(results, [0])
        server.refresh_jobs_after_accepted_block(block_height=11, block_hash="cc" * 32)
        self.assertEqual(
            seen,
            [
                ("prism-tip-refresh-scheduler", (10, "bb" * 32)),
                ("prism-tip-refresh-scheduler", (11, "cc" * 32)),
            ],
        )

class HealthSnapshotTests(_JobSupportTestCase):
    def test_health_payload_uses_aggregate_stats_not_all_shares(self) -> None:
        ledger = FakeLedger()
        server, _ = coordinator(ledger=ledger)
        mark_progress_healthy(server)
        payload = server.health_payload()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["accepted_share_count"], 3)
        self.assertEqual(payload["ready_miner_count"], 3)
        self.assertGreaterEqual(ledger.stats_calls, 1)
    def test_cached_health_payload_computes_inline_without_refresher(self) -> None:
        server, _ = coordinator()
        mark_progress_healthy(server)
        status, payload = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])
    def test_cached_health_payload_serves_snapshot_and_flags_staleness(self) -> None:
        server, _ = coordinator()
        mark_progress_healthy(server)
        server.refresh_health_snapshot()
        server._health_refresh_loop_running = True

        status, payload = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertIn("snapshot_age_seconds", payload)

        # Even if the ledger becomes unusable, the snapshot keeps serving.
        server.ledger = None  # type: ignore[assignment]
        status, payload = server.cached_health_payload()
        self.assertEqual(status, 200)

        server._health_snapshot_monotonic = time.monotonic() - 1_000
        status, payload = server.cached_health_payload()
        self.assertEqual(status, 503)
        self.assertFalse(payload["ok"])
    def test_accepted_share_stats_falls_back_to_all_shares(self) -> None:
        server, _ = coordinator(ledger=SingleWriterShareLedger())
        self.assertEqual(server.accepted_share_stats(), (0, 0))
    def test_single_writer_ledger_stats(self) -> None:
        ledger = SingleWriterShareLedger()
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 0, "distinct_miner_count": 0},
        )

class JobBuildMetricsTests(_JobSupportTestCase):
    def test_metrics_include_job_build_histogram_and_cache_counters(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.build_job_for_client(client(1), clean_jobs=True)
        server.build_job_for_client(client(2), clean_jobs=True)
        server.observe_job_build_elapsed(0.3, {"bundle": 0.2, "stamp": 0.01})
        server._observe_tip_refresh_build_phase("payout_state_derivation", 0.25)
        server._record_tip_refresh_ipc_bytes("input", 123)

        metrics = server.metrics_payload()

        self.assertIn('qbit_prism_job_build_seconds_bucket{le="0.5"} 1', metrics)
        self.assertIn('qbit_prism_job_build_seconds_bucket{le="+Inf"} 1', metrics)
        self.assertIn("qbit_prism_job_build_seconds_count 1", metrics)
        self.assertIn('qbit_prism_job_cache_hits_total{cache="bundle"} 1', metrics)
        self.assertIn('qbit_prism_job_cache_misses_total{cache="bundle"} 1', metrics)
        self.assertIn('qbit_prism_job_build_phase_seconds_total{phase="bundle"} 0.2', metrics)
        self.assertIn(
            'qbit_prism_tip_refresh_bundle_phase_seconds_count{phase="payout_state_derivation"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_tip_refresh_builder_ipc_bytes_total{direction="input"} 123',
            metrics,
        )
        self.assertIn("qbit_prism_tip_refresh_bundle_queue_depth 0", metrics)
        self.assertIn("qbit_prism_tip_refresh_bundle_inflight 0", metrics)
        self.assertIn("qbit_prism_connected_clients 0", metrics)
    def test_metrics_split_payout_preparation_publication_and_delivery(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]

        self.assertEqual(server._advance_payout_state_generation(), 1)
        self.assertTrue(server.maybe_send_job(state, clean_jobs=True))

        metrics = server.metrics_payload()

        self.assertIn("qbit_prism_payout_preparation_seconds_count 1", metrics)
        self.assertIn("qbit_prism_payout_publish_seconds_count 1", metrics)
        self.assertIn(
            "qbit_prism_payout_invalidation_first_delivery_seconds_count 1",
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_gate_wait_seconds_count{generation="current"} 1',
            metrics,
        )
        self.assertIn("qbit_prism_payout_candidates_discarded_total 0", metrics)

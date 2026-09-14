"""Real helper, MVCC transport, corruption, cancellation and ownership tests."""

import gc
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import weakref
from dataclasses import replace
from unittest.mock import patch

from lab.prism import payout_state as payout_state_module
from lab.prism import window_oracle as oracle
from lab.prism.accepted_preview_telemetry import FULL_RESCAN_PATH_HELPER
from lab.prism.bundle_compiler import _iter_prepare_window_request_chunks
from lab.prism.metrics import MetricsRenderer
from lab.prism.payout_state import TemplateRefreshSuperseded
from lab.prism.share_ledger import (
    DaemonShareJsonSequence, IncrementalShareWindow, PsqlShareLedger,
)
from lab.prism.window_ownership import window_ownership_snapshot
from lab.prism import window_ownership as ownership
from tests import test_prism_share_ledger as ledger_tests
from tests.test_prism_share_ledger import row_payload
from tests import test_prism_payout_window_daemon_recenter as recenter_tests
from tests import prism_coordinator_test_support as support


class StreamLedger:
    def __init__(self, rows):
        self.rows = rows
        self.reads = []

    def spool_snapshot_at_job_issue(self, anchor, *, window_weight, sink):
        self.reads.append((anchor, window_weight))
        sink.extend(self.rows)

    def snapshot_at_job_issue(self, *args, **kwargs):
        raise AssertionError("coordinator must not request materialized records")


class FakeClock:
    """A monotonic clock the test advances by hand.

    Never frozen: every read moves it a microsecond so no deadline loop in
    the build can spin forever. Every other ``time`` attribute is the real one.
    """

    def __init__(self):
        self.now = time.monotonic()

    def monotonic(self):
        self.now += 1e-6
        return self.now

    def __getattr__(self, name):
        return getattr(time, name)


class WindowOracleTests(unittest.TestCase):
    def test_real_helper_matches_independent_fold_and_recenter(self):
        rows = [row_payload(i) for i in reversed(range(1, 1030))]
        rows[0]["share_id"] += "-☃-😀"
        rows[-1]["network_difficulty"] = str(2 ** 160)
        ledger = StreamLedger(rows)
        anchor = 99999
        comparison = IncrementalShareWindow.from_full_snapshot(
            [PsqlShareLedger._record_from_json(row) for row in rows],
            anchor_job_issued_at_ms=anchor, window_weight=1027 * 4096,
        )
        expected = IncrementalShareWindow.from_full_snapshot(
            comparison.records(), anchor_job_issued_at_ms=anchor, window_weight=3 * 4096 + 1,
        )
        with patch.object(IncrementalShareWindow, "from_full_snapshot", side_effect=AssertionError("parent fold")), patch.object(
            PsqlShareLedger, "_record_from_json", side_effect=AssertionError("parent records"),
        ):
            result = oracle.snapshot_window(
                ledger, anchor=anchor, weight=3 * 4096 + 1,
                comparison_weight=1027 * 4096, append_epoch=7,
            )
        self.assertEqual(ledger.reads, [(anchor, 1027 * 4096)])
        self.assertEqual(result.comparison_digest, comparison.json_records().canonical_json_sha256())
        self.assertEqual(result.window.share_snapshot_sha256, expected.json_records().canonical_json_sha256())
        self.assertEqual(list(result.window.json_records()), list(expected.json_records()))

    def test_native_retry_resets_spool_and_preserves_one_snapshot(self):
        fixture = ledger_tests.PayoutWindowRowDecodingTests()
        native, events, cursors = fixture.native_client(results=[])
        native.outcomes.extend([
            dict(rows=[row_payload(1), row_payload(2)], fail_at_batch=2,
                 error=native.OperationalError("lost connection")),
            dict(rows=[row_payload(3), row_payload(4)]),
        ])
        with tempfile.TemporaryFile() as spool:
            sink = oracle.SnapshotSink(spool, lambda: None)
            result = native.run_json_rows("SELECT rows", retry_safe=True, row_sink=sink, batch_size=1)
            self.assertIs(result, sink)
            spool.seek(0)
            self.assertEqual([json.loads(line)["share_seq"] for line in spool], [3, 4])
        self.assertTrue(all(cursor.closed for cursor in cursors))
        self.assertEqual(events.count("execute:SELECT rows"), 2)

    def test_shipped_psql_transport_spools_without_records(self):
        fixture = ledger_tests.PayoutWindowRowDecodingTests()
        fixture.setUpClass()
        try:
            ledger = fixture.psql_ledger("rows", batch_size=2)
            with patch.dict(os.environ, {"FAKE_PSQL_ROWS": "5"}), patch.object(
                PsqlShareLedger, "_record_from_json", side_effect=AssertionError("parent records"),
            ):
                result = oracle.snapshot_window(ledger, anchor=999999, weight=10**10)
            self.assertEqual(result.window.record_count, 5)
        finally:
            fixture.tearDownClass()

    def test_oracle_budget_bounds_the_database_read_before_helper_start(self):
        fixture = ledger_tests.PayoutWindowRowDecodingTests()
        fixture.setUpClass()
        try:
            ledger = fixture.psql_ledger("hang")
            started = time.monotonic()
            with patch.object(oracle, "WINDOW_ORACLE_TIMEOUT_SECONDS", 0.1):
                with self.assertRaises(TimeoutError):
                    oracle.snapshot_window(ledger, anchor=9999, weight=4096)
            self.assertLess(time.monotonic() - started, 2)
            self.assertIsNone(ledger._remaining_operation_timeout())
        finally:
            fixture.tearDownClass()

    def test_invalid_ledger_is_rejected_without_fallback(self):
        for rows in ([row_payload(1), row_payload(1)], [dict(row_payload(1), share_difficulty="0")]):
            with self.subTest(rows=len(rows)), self.assertRaises(oracle.WindowOracleError):
                oracle.snapshot_window(StreamLedger(rows), anchor=9999, weight=4096)

    def test_result_protocol_rejects_corruption_and_oversized_metadata(self):
        request = dict(version=1, anchor=9999, weight=4096, comparison_weight=4096, append_epoch=9)
        items = b""
        header = dict(request, count=0, size=0, digest=hashlib.sha256(b"[]").hexdigest(),
                      comparison_digest=hashlib.sha256(b"[]").hexdigest())
        for changes, suffix in (
            ({"version": True}, items),
            ({"anchor": 10000}, items), ({"append_epoch": 10}, items),
            ({"count": 1}, items), ({"count": True}, items),
            ({"size": oracle.WINDOW_ORACLE_BYTES + 1}, items),
            ({"digest": "0" * 64}, items), ({"comparison_digest": "x" * 64}, items),
            ({"comparison_digest": "0" * 64}, items),
            ({}, b"extra"), ({"size": 5}, b"short"),
        ):
            with self.subTest(changes=changes), self.assertRaises(oracle.WindowOracleError):
                oracle._read_result(io.BytesIO(json.dumps(dict(header, **changes)).encode() + b"\n" + suffix), request, lambda: None)
        for data in (b"not json\n", b"x" * (oracle.WINDOW_ORACLE_HEADER_BYTES + 1), b"{}"):
            with self.assertRaises(oracle.WindowOracleError):
                oracle._read_result(io.BytesIO(data), request, lambda: None)

    def test_limits_fail_before_launch_and_release_admission(self):
        with patch.object(oracle, "WINDOW_ORACLE_RECORDS", 1), patch.object(oracle.subprocess, "Popen") as spawn:
            with self.assertRaises(oracle.WindowOracleError):
                oracle.snapshot_window(StreamLedger([row_payload(1), row_payload(2)]), anchor=9999, weight=4096)
            spawn.assert_not_called()
        self.assertEqual(oracle.snapshot_window(StreamLedger([]), anchor=9999, weight=4096).window.record_count, 0)

    def test_timeout_kills_reaps_and_closes_files_without_credentials(self):
        processes, files = [], []
        popen, temporary = subprocess.Popen, tempfile.TemporaryFile

        def spawn(command, **kwargs):
            self.assertIn("-I", command)
            self.assertTrue(kwargs["close_fds"])
            self.assertNotIn("PGPASSWORD", kwargs["env"])
            self.assertNotIn("SIGNING_SEED", kwargs["env"])
            process = popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
            processes.append(process)
            return process

        def opened(*args, **kwargs):
            file = temporary(*args, **kwargs)
            files.append(file)
            return file

        with patch.object(oracle.subprocess, "Popen", side_effect=spawn), patch.object(
            oracle.tempfile, "TemporaryFile", side_effect=opened,
        ), patch.object(oracle, "WINDOW_ORACLE_TIMEOUT_SECONDS", 0.15), patch.dict(
            os.environ, {"PGPASSWORD": "test-only", "SIGNING_SEED": "test-only"},
        ):
            with self.assertRaisesRegex(oracle.WindowOracleError, "deadline"):
                oracle.snapshot_window(StreamLedger([]), anchor=9999, weight=4096)
        self.assertEqual(len(processes), 1)
        self.assertIsNotNone(processes[0].returncode)
        self.assertTrue(all(file.closed for file in files))

    def test_cancellation_during_admission_does_not_start_another_read(self):
        ledger = StreamLedger([])
        acquired = oracle._ADMISSION.acquire(timeout=1)
        self.assertTrue(acquired)
        checks = []

        def check():
            checks.append(True)
            if len(checks) > 1:
                raise TemplateRefreshSuperseded("cancelled")

        try:
            with self.assertRaises(TemplateRefreshSuperseded):
                oracle.snapshot_window(ledger, anchor=9999, weight=4096, check=check)
        finally:
            oracle._ADMISSION.release()
        self.assertEqual(ledger.reads, [])

    def test_daemon_prepare_splices_canonical_items_without_parsing(self):
        result = oracle.snapshot_window(StreamLedger([row_payload(1)]), anchor=9999, weight=4096)
        shares = result.window.json_records()
        with patch.object(DaemonShareJsonSequence, "_records", side_effect=AssertionError("parent parse")):
            data = b"".join(_iter_prepare_window_request_chunks(dict(request="prepare_window", records=shares)))
        self.assertEqual(json.loads(data)["records"], list(shares))

    def test_full_scan_discards_concurrently_invalidated_helper_result(self):
        fixture = recenter_tests.DaemonRecenterTests()
        with patch.dict(os.environ, {"PRISM_WINDOW_PIPELINE_RUST": "1"}):
            server, ledger, artifacts, daemon = fixture._server()
            service = server._ensure_payout_state_service()
            ledger.spool_snapshot_at_job_issue = lambda *args, **kwargs: None
            result = oracle.snapshot_window(StreamLedger([row_payload(1)]), anchor=9999, weight=4096)

            def invalidated(*args, **kwargs):
                server._payout_ledger_append_invalidation_epoch += 1
                return result

            with patch.object(service, "_isolated_window_oracle", side_effect=invalidated):
                with self.assertRaises(TemplateRefreshSuperseded):
                    service._full_payout_window_oracle(
                        snapshot_anchor_ms=9999, snapshot_window_weight=4096,
                        reason="cold_start", observed_monotonic=1, append_invalidation_epoch=0,
                    )
            self.assertIsNone(server._incremental_payout_artifact_window)

    def test_full_scan_helper_failure_keeps_its_ledger_read_time(self):
        # Review finding (PR 335): the helper branch noted ``ledger_read``
        # only after the oracle returned, so a slow SQL failure or a helper
        # timeout left the failed build with no ledger time. The in-process
        # branch already timed its read in a finally; the helper read must too.
        fixture = recenter_tests.DaemonRecenterTests()
        server, ledger, artifacts, daemon = fixture._server()
        service = server._ensure_payout_state_service()
        ledger.spool_snapshot_at_job_issue = lambda *args, **kwargs: None
        result = oracle.snapshot_window(StreamLedger([row_payload(1)]), anchor=9999, weight=4096)
        clock = FakeClock()
        read_seconds = 2.5

        def slow_read(outcome):
            def read(*args, **kwargs):
                clock.now += read_seconds
                if isinstance(outcome, BaseException):
                    raise outcome
                return outcome
            return read

        def full_scan():
            return service._full_payout_window_oracle(
                snapshot_anchor_ms=9999, snapshot_window_weight=4096,
                reason="cold_start", observed_monotonic=1, append_invalidation_epoch=0,
            )

        family = "qbit_prism_payout_window_build_phase_seconds"

        def cell(metrics, phase, outcome, product):
            prefix = f'{family}_{product}{{phase="{phase}",outcome="{outcome}"}} '
            values = [entry for entry in metrics if entry.startswith(prefix)]
            self.assertEqual(len(values), 1, prefix)
            return float(values[0].split()[-1])

        with patch.object(payout_state_module, "time", clock):
            # A successful helper read keeps its attribution and its path.
            phases = service._begin_window_build_phases()
            try:
                with patch.object(service, "_isolated_window_oracle", side_effect=slow_read(result)):
                    materialized, path = full_scan()
            finally:
                service._finish_window_build_phases()
            self.assertEqual(path, FULL_RESCAN_PATH_HELPER)
            self.assertEqual(materialized.mode, "full_rescan")
            self.assertGreaterEqual(phases["ledger_read"], read_seconds)
            self.assertLess(phases["ledger_read"], read_seconds + 0.01)
            self.assertIsNotNone(server._incremental_payout_artifact_window)

            # A read that dies still raises its own error, publishes no
            # window, and owns the wall-clock it spent.
            for error in (
                RuntimeError("postgres statement deadline expired"),
                TimeoutError("window oracle helper timed out"),
                oracle.WindowOracleError("window oracle deadline exceeded"),
            ):
                with self.subTest(error=type(error).__name__):
                    server._incremental_payout_artifact_window = None
                    phases = service._begin_window_build_phases()
                    try:
                        with patch.object(service, "_isolated_window_oracle", side_effect=slow_read(error)):
                            with self.assertRaises(type(error)) as raised:
                                full_scan()
                    finally:
                        service._finish_window_build_phases()
                    self.assertIs(raised.exception, error)
                    self.assertGreaterEqual(phases["ledger_read"], read_seconds)
                    self.assertLess(phases["ledger_read"], read_seconds + 0.01)
                    self.assertIsNone(server._incremental_payout_artifact_window)

            # Through the whole build, that time lands under the failed outcome.
            with patch.object(
                service, "_isolated_window_oracle",
                side_effect=slow_read(TimeoutError("window oracle helper timed out")),
            ):
                self.assertIsNone(server._build_payout_ledger_artifact(0, 0, int(artifacts.network_difficulty)))
            metrics = server.payout_state_metrics_lines()
            self.assertEqual(cell(metrics, "ledger_read", "failed", "count"), 1)
            self.assertGreaterEqual(cell(metrics, "ledger_read", "failed", "sum"), read_seconds)
            self.assertEqual(cell(metrics, "ledger_read", "completed", "count"), 0)

    def test_ready_fallback_and_seed_keep_the_canonical_view(self):
        server, rpc = support.coordinator()
        support.install_fake_bundle_builder(server)
        # The fake compiler normally walks shares to record its inputs. Keep
        # its valid summary while forbidding any parent walk of the real view.
        build = server.build_audit_bundle

        def summary(**kwargs):
            self.assertIsInstance(kwargs["shares"], DaemonShareJsonSequence)
            kwargs["shares"] = [PsqlShareLedger._record_from_json(row_payload(1)).to_prism_json()]
            return build(**kwargs)

        server.build_audit_bundle = summary
        stream = StreamLedger([row_payload(1)])
        server.ledger.spool_snapshot_at_job_issue = stream.spool_snapshot_at_job_issue
        artifacts = server.store_template_artifacts(dict(rpc.template))
        with patch.object(DaemonShareJsonSequence, "_records", side_effect=AssertionError("parent parsed ready snapshot")):
            bundle = server.build_shared_job_bundle(artifacts, support.worker())
        self.assertIs(bundle.prepared_ledger_artifact.shares_json, bundle.shares_json)
        self.assertIsNone(bundle.shares_json._parsed)
        self.assertEqual(len(stream.reads), 1)

    def test_self_check_reuses_verified_bytes_and_retires_temporary_mirror(self):
        fixture = recenter_tests.DaemonRecenterTests()
        with patch.dict(os.environ, {"PRISM_WINDOW_PIPELINE_RUST": "1"}):
            server, ledger, artifacts, daemon = fixture._server()
            clock = [1000000]
            with patch("lab.prism.prism_coordinator.now_ms", side_effect=lambda: clock[0]):
                original = server._build_payout_ledger_artifact(0, 0, int(artifacts.network_difficulty))
                server.payout_artifact_min_build_interval_seconds = 0
                server.payout_artifact_full_rescan_seconds = 0
                clock[0] += 20
                checked = server._build_payout_ledger_artifact(0, 0, int(artifacts.network_difficulty))
                self.assertEqual(checked.window_build_mode, "self_check_match")
                self.assertIs(original.shares_json.canonical_items, checked.shares_json.canonical_items)
                self.assertIsNone(checked.shares_json._parsed)
                self.assertFalse(hasattr(checked.shares_json, "pages"))

    def test_periodic_helper_scan_keeps_its_ledger_read_time(self):
        # Review finding (PR 335): the isolated periodic self-check called
        # the helper with no ``ledger_read`` timing at all, even on success,
        # while the legacy in-process branch beside it timed its read in a
        # finally. The helper read must own its wall-clock on every exit: a
        # matched check and a timed-out one keep their build outcome and
        # both attribute the time the helper spent.
        fixture = recenter_tests.DaemonRecenterTests()
        with patch.dict(os.environ, {"PRISM_WINDOW_PIPELINE_RUST": "1"}):
            server, ledger, artifacts, daemon = fixture._server()
            service = server._ensure_payout_state_service()
            difficulty = int(artifacts.network_difficulty)
            clock = FakeClock()
            read_seconds = 2.5
            real_helper = service._isolated_window_oracle
            helper_calls = []

            def slow_helper(error):
                def read(*args, **kwargs):
                    helper_calls.append(kwargs.get("comparison_weight"))
                    clock.now += read_seconds
                    if error is not None:
                        raise error
                    return real_helper(*args, **kwargs)
                return read

            family = "qbit_prism_payout_window_build_phase_seconds"

            def ledger_read_completed(product):
                prefix = f'{family}_{product}{{phase="ledger_read",outcome="completed"}} '
                values = [
                    entry for entry in server.payout_state_metrics_lines()
                    if entry.startswith(prefix)
                ]
                self.assertEqual(len(values), 1, prefix)
                return float(values[0].split()[-1])

            clock_ms = [1_000_000]
            with patch.object(payout_state_module, "time", clock), patch(
                "lab.prism.prism_coordinator.now_ms", side_effect=lambda: clock_ms[0],
            ):
                initial = server._build_payout_ledger_artifact(0, 0, difficulty)
                self.assertIsNotNone(initial)
                self.assertEqual(initial.window_build_mode, "full_rescan")
                # Every later build runs the periodic runtime-check: the
                # debounce and the check interval are both disarmed, as in
                # the mirror self-check test above.
                server.payout_artifact_min_build_interval_seconds = 0
                server.payout_artifact_full_rescan_seconds = 0
                for error, mode, reason in (
                    (None, "self_check_match", "periodic_self_check"),
                    (
                        TimeoutError("window oracle helper timed out"),
                        "incremental_self_check_failed",
                        "periodic_self_check_failed",
                    ),
                ):
                    with self.subTest(mode=mode):
                        clock_ms[0] += 20
                        count_before = ledger_read_completed("count")
                        sum_before = ledger_read_completed("sum")
                        del helper_calls[:]
                        with patch.object(
                            service, "_isolated_window_oracle", side_effect=slow_helper(error),
                        ):
                            checked = server._build_payout_ledger_artifact(0, 0, difficulty)
                        # The check went through the helper once, and its
                        # classification is unchanged: a failed check still
                        # completes the build on the validated delta.
                        self.assertEqual(len(helper_calls), 1)
                        self.assertIsNotNone(checked)
                        self.assertEqual(checked.window_build_mode, mode)
                        self.assertEqual(checked.window_full_rescan_reason, reason)
                        # The completed build owns the helper's wall-clock.
                        self.assertEqual(ledger_read_completed("count") - count_before, 1)
                        elapsed = ledger_read_completed("sum") - sum_before
                        self.assertGreaterEqual(elapsed, read_seconds)
                        self.assertLess(elapsed, read_seconds + 0.01)


class WindowOwnershipTests(unittest.TestCase):
    def test_gc_retirement_can_reenter_the_accounting_lock(self):
        # Registry allocation can trigger collection on the same thread.
        # A cycle makes its weakref callback run precisely inside the lock.
        gc.collect()
        before = window_ownership_snapshot()

        class Holder:
            pass

        holder = Holder()
        holder.self = holder
        ownership.track_window(holder, b"reentrant-buffer", kind="mirror")
        reference = weakref.ref(holder)
        del holder
        with ownership._LOCK:
            gc.collect()
        self.assertIsNone(reference())
        self.assertEqual(window_ownership_snapshot(), before)

    def test_weak_accounting_deduplicates_aliases_and_observes_retirement(self):
        gc.collect()
        enabled = gc.isenabled()
        gc.disable()
        try:
            before = window_ownership_snapshot()
            result = oracle.snapshot_window(StreamLedger([row_payload(1)]), anchor=9999, weight=4096)
            shares = result.window.json_records()
            retained = weakref.ref(shares)
            self.assertIs(shares.canonical_items, result.window.canonical_items)
            current = window_ownership_snapshot()
            self.assertEqual(current["canonical_buffers"] - before["canonical_buffers"], 1)
            self.assertEqual(current["canonical_bytes"] - before["canonical_bytes"], len(shares.canonical_items))
            list(shares)
            self.assertEqual(window_ownership_snapshot()["parsed_records"] - before["parsed_records"], 1)
            del result
            self.assertIsNotNone(retained())
            del shares
            self.assertIsNone(retained())
            self.assertEqual(window_ownership_snapshot(), before)
        finally:
            if enabled:
                gc.enable()

    def test_component_metrics_count_pages_retained_beside_a_mirror(self):
        fixture = recenter_tests.DaemonRecenterTests()
        with patch.dict(os.environ, {"PRISM_WINDOW_PIPELINE_RUST": "1"}):
            server, ledger, artifacts, daemon = fixture._server()
            server._build_payout_ledger_artifact(0, 0, int(artifacts.network_difficulty))
            cached = server._incremental_payout_artifact_window
            window = IncrementalShareWindow.from_full_snapshot(
                [PsqlShareLedger._record_from_json(row_payload(1))],
                anchor_job_issued_at_ms=9999, window_weight=4096,
            )
            server._incremental_payout_artifact_window = replace(cached, shares_json=window.json_records())
            lines = MetricsRenderer(server).component_cardinality_metrics_lines()
            self.assertIn('qbit_prism_component_entries{component="payout_window_pages"} 1', lines)
            self.assertIn('qbit_prism_component_entries{component="payout_window_records"} 1', lines)


if __name__ == "__main__":
    unittest.main()

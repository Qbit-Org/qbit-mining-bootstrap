"""Issue #340 integration with #335's production spool/isolated-oracle route.

The 2.x.x base does not yet include #335. These tests are an explicit
integration gate, not replacement coverage for the legacy scheduler tests.
"""

import importlib.util
import threading
import unittest
import weakref

from tests import test_prism_job_build_exception_retention as harness
from tests.test_prism_share_ledger import row_payload

ORACLE_AVAILABLE = importlib.util.find_spec("lab.prism.window_oracle") is not None


def configure_oracle_failure(case, *, mode="compiler", records=1024,
                             cycle=0, production_rows=False, wait_seconds=20):
    """Wire only the ledger input and compiler outcome; the oracle stays real."""
    if not ORACLE_AVAILABLE:
        raise RuntimeError("requires PR #335's window_oracle; run in the integration checkout")
    from lab.prism.share_ledger import DaemonShareJsonSequence
    from lab.prism.window_ownership import window_ownership_snapshot

    reached, release = threading.Event(), threading.Event()
    references, measured = {}, {}
    baseline = window_ownership_snapshot()

    def pause():
        reached.set()
        if not release.wait(wait_seconds):
            raise AssertionError("oracle failure gate was not released")

    def stream(anchor, *, window_weight, sink):
        case.ledger.snapshot_calls += 1
        references["sink"] = weakref.ref(sink)
        references["source_spool"] = weakref.ref(sink.spool)
        if mode in ("ledger", "helper"):
            sink.append(row_payload(1))
            pause()
            if mode == "ledger":
                raise OSError(5, "injected spool read failure")
            # The real isolated child rejects the row; do not mock its result.
            sink.append(dict(row_payload(2), share_difficulty="0"))
        elif production_rows:
            from tests.perf.window_oracle_retirement import ReplayLedger
            ReplayLedger(records, cycle).spool_snapshot_at_job_issue(
                anchor, window_weight=window_weight, sink=sink,
            )
        else:
            for index in range(1, records + 1):
                sink.append(row_payload(index))

    def compile_bundle(**kwargs):
        shares = kwargs["shares"]
        assert isinstance(shares, DaemonShareJsonSequence)
        assert len(shares) == records
        assert shares._parsed is None
        references["canonical_sequence"] = weakref.ref(shares)
        measured["payload_bytes"] = len(shares.canonical_items)
        if mode == "parsed_compiler":
            # A consumer that legitimately materializes the rows holds them
            # for its own span only; the sequence never owns the parse.
            with shares.retained() as rows:
                rows[0]
                measured["parsed_records"] = len(shares._parsed)
                measured["at_failure"] = {k: v - baseline[k] for k, v in window_ownership_snapshot().items()}
            measured["after_consumer"] = window_ownership_snapshot()["parsed_records"] - baseline["parsed_records"]
        else:
            measured["parsed_records"] = 0 if shares._parsed is None else len(shares._parsed)
            measured["at_failure"] = {k: v - baseline[k] for k, v in window_ownership_snapshot().items()}
        pause()
        raise ValueError("injected compiler failure after isolated oracle")

    case.ledger.spool_snapshot_at_job_issue = stream
    case.server.build_audit_bundle = compile_bundle
    return reached, release, references, measured, baseline


@unittest.skipUnless(ORACLE_AVAILABLE, "#335 absent from base; separate integration checkout required")
class JobBuildOracleFailureRetentionTests(unittest.TestCase):
    def exercise(self, mode, count):
        from lab.prism.window_oracle import WindowOracleError
        from lab.prism.window_ownership import window_ownership_snapshot

        case = harness.JobBuildExceptionRetentionTests()
        case.setUp()
        try:
            reached, release, refs, measured, baseline = configure_oracle_failure(case, mode=mode)
            threads, outcomes = case.run_waiters(count)
            self.assertTrue(reached.wait(harness.WAIT_SECONDS))
            case.await_joined_flight(count)
            release.set()
            case.join_all(threads)
            refs.update(case.tracked_references())
            case.assert_released(refs)
            self.assertEqual(window_ownership_snapshot(), baseline)
            for slot in ("_job_build_active", "_job_build_retiring", "_job_build_pending"):
                self.assertIsNone(getattr(case.service, slot))
            self.assertEqual(case.server._payout_window_inflight_scan_anchors, {})
            error_type = {"ledger": OSError, "helper": WindowOracleError}.get(mode, ValueError)
            self.assertTrue(all(o["type"] is error_type for o in outcomes), outcomes)
            self.assertTrue(all(not o["stored_is_error"] for o in outcomes))
            self.assertEqual(case.service.shared_bundle_build_counts["failed"], 1)
            self.assertEqual(len(case.ledger.snapshots), 0)
            if "compiler" in mode:
                self.assertEqual(measured["payload_bytes"], 336646)
                self.assertEqual(measured["parsed_records"], 1024 if mode == "parsed_compiler" else 0)
                self.assertEqual(measured["at_failure"]["parsed_records"], measured["parsed_records"])
                if mode == "parsed_compiler":
                    self.assertEqual(measured["after_consumer"], 0)
                self.assertEqual(measured["at_failure"]["canonical_buffers"], 1)
        finally:
            case.tearDown()

    def test_spool_ledger_failure(self):
        for count in (1, 3):
            with self.subTest(waiters=count):
                self.exercise("ledger", count)

    def test_real_helper_failure(self):
        for count in (1, 3):
            with self.subTest(waiters=count):
                self.exercise("helper", count)

    def test_unparsed_canonical_compiler_failure(self):
        for count in (1, 3):
            with self.subTest(waiters=count):
                self.exercise("compiler", count)

    def test_parsed_canonical_compiler_failure(self):
        for count in (1, 3):
            with self.subTest(waiters=count):
                self.exercise("parsed_compiler", count)


if __name__ == "__main__":
    unittest.main()

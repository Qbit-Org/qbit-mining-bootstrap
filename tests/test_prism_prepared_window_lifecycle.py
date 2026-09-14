"""Issue #339: coordinator decisions against the actual serve daemon.

Opt in with PRISM_TOOL_BIN_DIR pointing to an explicitly built binary.
The ledger and RPC are isolated fixtures; preparation and bundle compilation
use the production coordinator and real Rust process.
"""

from __future__ import annotations

import dataclasses
import io
import os
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from lab.prism.bundle_compiler import _ShareWindowSerialization
from lab.prism.window_lifecycle import (
    EVENT_REASONS,
    WindowLifecycleTelemetry,
    capture_artifact_reuse,
    refuse_artifact_reuse,
)
from tests.prism_coordinator_test_support import (
    IncrementalRecordingLedger,
    canonical_json_sha256,
    coordinator,
)
from tests.test_prism_payout_window_daemon_recenter import _append_heavy_share


def lifecycle_server():
    ledger = IncrementalRecordingLedger()
    server, rpc = coordinator(ledger=ledger)
    server.signing_seed_hex = "42" * 32  # Public test seeds, no authority.
    server.ledger_attestation_signing_seed_hex = "43" * 32
    server.prism_ctv_settlement_config = lambda **_kwargs: None
    server.bundle_build_timeout_seconds = 120.0
    server.payout_artifact_min_build_interval_seconds = 0.0
    server.payout_artifact_full_rescan_seconds = 3600.0
    server._pool_ready_latched = True
    artifacts = server.store_template_artifacts(dict(rpc.template))
    return server, ledger, artifacts


def unrelated_build(server, identity: int, *, cancellation=None):
    """Successful historical/candidate-window upload in the shared cache."""
    shares = [
        {
            "share_seq": identity,
            "share_id": f"other-{identity}",
            "miner_id": "other",
            "order_key": "other",
            "p2mr_program_hex": "51" * 32,
            "share_difficulty": 16,
            "network_difficulty": 1,
            "template_height": 9,
            "job_id": f"other-job-{identity}",
            "ntime": 1_700_000_000,
            "job_issued_at_ms": 990_000,
            "accepted_at_ms": 990_001,
        }
    ]
    digest = canonical_json_sha256(shares)
    serialization = _ShareWindowSerialization(
        key=(digest, 1, 16), share_count=1, share_snapshot_sha256=digest
    )
    try:
        return server.build_audit_bundle(
            shares=shares,
            found_block={
                "block_height": 10,
                "coinbase_value_sats": 50_00000000,
                "network_difficulty": 1,
                "anchor_job_issued_at_ms": 1_000_000,
            },
            prior_balances=[],
            coinbase_script_sig_suffix_hex="00",
            summary_only=True,
            share_serialization=serialization,
            cancellation=cancellation,
        )
    finally:
        serialization.retire_spool()


@unittest.skipUnless(
    os.environ.get("PRISM_TOOL_BIN_DIR"), "explicit real daemon required"
)
class PreparedWindowLifecycleTests(unittest.TestCase):
    def setUp(self):
        binary = (
            Path(os.environ["PRISM_TOOL_BIN_DIR"]) / "qbit-prism-build-audit-bundle"
        )
        self.assertTrue(binary.is_file(), f"build {binary} first")
        env = patch.dict(
            os.environ, {"PRISM_WINDOW_PIPELINE_RUST": "1", "PRISM_BUILDER_SERVE": "1"}
        )
        env.start()
        self.addCleanup(env.stop)
        self.server, self.ledger, self.artifacts = lifecycle_server()
        self.addCleanup(self.server._ensure_bundle_compiler().shutdown_serve_builder)
        self.addCleanup(self.server.retire_share_window_spool)
        self.addCleanup(self.server.shutdown_job_build_executor)
        self.clock = 1_000_000
        clock = patch(
            "lab.prism.prism_coordinator.now_ms", side_effect=lambda: self.clock
        )
        clock.start()
        self.addCleanup(clock.stop)
        for i in range(1, 21):
            _append_heavy_share(
                self.ledger,
                share_seq=i,
                accepted_at_ms=999_000 + i,
                share_difficulty=int(self.artifacts.network_difficulty),
            )

    def build_window(self):
        return self.server._build_payout_ledger_artifact(
            0, 0, self.artifacts.network_difficulty
        )

    def test_build_upload_pressure_does_not_force_another_snapshot(self):
        initial = self.build_window()
        compiler = self.server._ensure_bundle_compiler()
        process = compiler._serve_builder.process
        for identity in range(100, 104):
            unrelated_build(self.server, identity)
        self.clock += 20
        advanced = self.build_window()
        self.assertIs(compiler._serve_builder.process, process)
        self.assertEqual(initial.share_snapshot_sha256, advanced.share_snapshot_sha256)
        self.assertEqual(self.ledger.full_snapshot_calls, 1)
        self.assertEqual(advanced.window_build_mode, "incremental")

    def test_other_preparations_really_evict_and_recover_the_old_base(self):
        initial = self.build_window()
        compiler = self.server._ensure_bundle_compiler()
        process = compiler._serve_builder.process
        for identity in (100, 101):
            row = dict(initial.shares_json[0], share_id=f"replacement-{identity}")
            self.assertEqual(
                self.server.prepare_payout_window(
                    mode="full",
                    records_json=[row],
                    anchor_job_issued_at_ms=self.clock,
                    append_invalidation_epoch=0,
                    window_weight=16,
                ).status,
                "prepared",
            )
        self.clock += 20
        recovered = self.build_window()
        self.assertIs(compiler._serve_builder.process, process)
        self.assertEqual(self.ledger.full_snapshot_calls, 2)
        self.assertEqual(
            recovered.window_full_rescan_reason, "window_daemon_state_lost"
        )
        counts, _, _ = compiler.window_lifecycle.snapshot()
        self.assertEqual(counts["prepare_advance", "evicted_by_prepare"], 1)
        self.clock += 20
        self.build_window()
        self.assertEqual(self.ledger.full_snapshot_calls, 2)

    def test_process_replacement_is_separate_from_cache_eviction(self):
        initial = self.build_window()
        compiler = self.server._ensure_bundle_compiler()
        process = compiler._serve_builder.process
        process.kill()  # Task-owned disposable daemon only.
        process.wait(timeout=5)
        self.clock += 20
        recovered = self.build_window()
        self.assertIsNot(compiler._serve_builder.process, process)
        self.assertEqual(recovered.share_snapshot_sha256, initial.share_snapshot_sha256)
        counts, events, _ = compiler.window_lifecycle.snapshot()
        self.assertEqual(counts["retire", "exited"], 1)
        self.assertEqual(counts["prepare_advance", "not_held"], 1)
        self.assertEqual(
            [e["daemon_generation"] for e in events if e["operation"] == "spawn"],
            [1, 2],
        )

    def test_busy_preserves_process_and_base(self):
        initial = self.build_window()
        compiler = self.server._ensure_bundle_compiler()
        process = compiler._serve_builder.process
        with compiler._serve_builder_lock:
            busy = self.server.prepare_payout_window(
                mode="advance",
                records_json=[],
                anchor_job_issued_at_ms=self.clock,
                append_invalidation_epoch=0,
                base_digest=initial.share_snapshot_sha256,
                wait_for_daemon=False,
            )
        self.assertEqual(busy.status, "busy")
        self.clock += 20
        self.build_window()
        self.assertIs(compiler._serve_builder.process, process)
        self.assertEqual(self.ledger.full_snapshot_calls, 1)

    def test_deliberate_recenter_keeps_the_real_daemon_prepared(self):
        initial = self.build_window()
        difficulty = int(self.artifacts.network_difficulty) * 3 // 4
        self.server.payout_artifact_full_rescan_seconds = 0.0
        self.clock += 20
        checked = self.server._build_payout_ledger_artifact(0, 0, difficulty)
        self.assertTrue(checked.window_self_check_recentered)
        self.assertNotEqual(
            initial.share_snapshot_sha256, checked.share_snapshot_sha256
        )
        self.server.payout_artifact_full_rescan_seconds = 3600.0
        self.clock += 20
        advanced = self.server._build_payout_ledger_artifact(0, 0, difficulty)
        self.assertEqual(advanced.share_snapshot_sha256, checked.share_snapshot_sha256)
        self.assertEqual(advanced.window_build_mode, "incremental")
        self.assertEqual(self.ledger.full_snapshot_calls, 2)

    def test_uploaded_only_base_is_distinct_from_unseen_or_evicted(self):
        self.build_window()
        unrelated_build(self.server, 100)
        compiler = self.server._ensure_bundle_compiler()
        digest = next(reversed(compiler._serve_builder.uploaded_windows))
        outcome = self.server.prepare_payout_window(
            mode="advance",
            records_json=[],
            base_digest=digest,
            anchor_job_issued_at_ms=self.clock,
            append_invalidation_epoch=0,
        )
        self.assertEqual(outcome.status, "needs_full")
        self.assertEqual(outcome.base_state, "uploaded_only")

    def test_same_digest_replacement_retains_anchor_fencing(self):
        initial = self.build_window()
        outcome = self.server.prepare_payout_window(
            mode="full",
            records_json=list(initial.shares_json),
            anchor_job_issued_at_ms=self.clock + 100,
            append_invalidation_epoch=0,
            window_weight=16 * int(self.artifacts.network_difficulty),
        )
        self.assertEqual(outcome.share_snapshot_sha256, initial.share_snapshot_sha256)
        self.clock += 20
        recovered = self.build_window()
        self.assertEqual(
            recovered.window_full_rescan_reason, "incremental_invariant_failed"
        )
        self.assertEqual(self.ledger.full_snapshot_calls, 2)

    def test_template_refresh_uses_artifact_and_declares_frozen_anchor(self):
        initial = self.build_window()
        self.assertTrue(self.server._install_payout_ledger_artifact(initial))
        for i in range(3):
            self.clock += 2000
            template = dict(self.artifacts.template, curtime=1_700_000_001 + i)
            self.artifacts = self.server.store_template_artifacts(template)
            bundle = self.server.shared_job_bundle(self.artifacts, mode="ready")
            self.assertEqual(
                bundle.found_block["anchor_job_issued_at_ms"],
                initial.snapshot_anchor_ms,
            )
            self.assertEqual(
                bundle.build_key.share_snapshot_sha256, initial.share_snapshot_sha256
            )
        self.assertEqual(self.ledger.full_snapshot_calls, 1)

    def test_ready_fallback_carries_the_actual_admission_refusal(self):
        bundle = self.server.shared_job_bundle(self.artifacts, mode="ready")
        self.assertIsNotNone(bundle.prepared_ledger_artifact)
        counts, _, _ = self.server._ensure_bundle_compiler().window_lifecycle.snapshot()
        self.assertEqual(counts["ready_snapshot", "absent"], 1)
        self.clock += 2000
        self.artifacts = self.server.store_template_artifacts(
            dict(self.artifacts.template, curtime=1_700_000_001)
        )
        self.server.shared_job_bundle(self.artifacts, mode="ready")
        self.assertEqual(self.ledger.full_snapshot_calls, 1)


class WindowLifecycleAttributionTests(unittest.TestCase):
    def test_metrics_and_diagnostics_are_bounded_under_churn(self):
        telemetry = WindowLifecycleTelemetry()
        with (
            redirect_stdout(io.StringIO()) as output,
            patch("lab.prism.window_lifecycle.time.monotonic", return_value=100),
        ):
            for i in range(1000):
                telemetry.note(
                    "prepare_advance", "not_held", base_digest=f"{i:064x}", ignored=[i]
                )
            telemetry.note("prepare_advance", "untrusted arbitrary category")
        counts, events, suppressed = telemetry.snapshot()
        self.assertEqual(len(counts), sum(map(len, EVENT_REASONS.values())))
        self.assertEqual(len(events), 64)
        self.assertEqual(suppressed, 999)
        self.assertEqual(len(output.getvalue().splitlines()), 2)
        self.assertEqual(counts["prepare_advance", "unknown"], 1)
        self.assertNotIn("base_digest", "\n".join(telemetry.metrics_lines()))
        self.assertTrue(all("ignored" not in event for event in events))

    def test_nested_and_concurrent_probe_reasons_do_not_cross_requests(self):
        with capture_artifact_reuse() as outer:
            refuse_artifact_reuse("absent")
            with capture_artifact_reuse() as inner:
                refuse_artifact_reuse("difficulty")
            thread = threading.Thread(target=lambda: refuse_artifact_reuse("balances"))
            thread.start()
            thread.join()
        self.assertEqual(outer.reason, "absent")
        self.assertEqual(inner.reason, "difficulty")

    def test_validity_fences_have_distinct_refusals(self):
        from tests.test_prism_payout_window_daemon_recenter import DaemonRecenterTests

        server, _, artifacts, _ = DaemonRecenterTests()._server()
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            for changes, expected in (
                ({"payout_state_generation": 1}, "payout_generation"),
                ({"network_difficulty": 1}, "difficulty"),
                ({"append_invalidation_epoch": 1}, "append_epoch"),
                ({"snapshot_anchor_ms": None}, "anchor_missing"),
            ):
                server._payout_ledger_artifact = dataclasses.replace(
                    artifact, **changes
                )
                with capture_artifact_reuse() as probe:
                    self.assertIsNone(
                        server._usable_payout_ledger_artifact(
                            0, artifacts.network_difficulty
                        )
                    )
                self.assertEqual(probe.reason, expected)


if __name__ == "__main__":
    unittest.main()

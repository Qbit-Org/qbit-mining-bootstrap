#!/usr/bin/env python3
"""Coordinator-side tests for the Rust payout-window pipeline slice.

Why these tests exist: the byte-level equivalence of the Rust fold is proven
by the parity oracle (``tests.test_window_pipeline_parity`` with the
``rust-daemon`` adapter); what still needs proof is the coordinator's side of
the contract -- that the ``PRISM_WINDOW_PIPELINE_RUST`` switch defaults to
Python and fails closed, that the daemon path preserves the append-epoch and
scan-anchor fences, and that every non-success shape (daemon death,
``needs_full`` after a respawn, an ``advance`` invariant failure, a protocol
mismatch, a value outside the daemon's declared integer widths) degrades
exactly like its in-process counterpart instead of inventing a new failure
mode. The daemon here is the fake serve builder, which serves prepare_window
with the real Python fold so mirror digests verify byte-exactly without a
Rust build, and which declares the real daemon's integer widths.
"""
# ruff: noqa: F403, F405

from __future__ import annotations

import hashlib
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path

from lab.prism.coordinator_config import (
    env_window_pipeline_rust,
    load_coordinator_config,
)
from lab.prism.share_ledger import (
    DaemonShareJsonSequence,
    DaemonShareWindowMirror,
    DaemonWindowMirrorDivergence,
    IncrementalShareWindow,
)
from tests.prism_coordinator_test_support import *


def _canonical_items(window: IncrementalShareWindow) -> bytes:
    return b",".join(
        page.canonical_json_items
        for page in window.pages
        if page.canonical_json_items
    )


def _python_window_for(
    ledger: IncrementalRecordingLedger,
    *,
    anchor_ms: int,
    network_difficulty: int,
) -> IncrementalShareWindow:
    """The in-process oracle over the same ledger rows, counters untouched."""
    records = list(
        SingleWriterShareLedger.snapshot_at_job_issue(
            ledger,
            anchor_ms,
            window_weight=None,
        )
    )
    return IncrementalShareWindow.from_full_snapshot(
        records,
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=8 * 2 * int(network_difficulty),
    )


class WindowPipelineSwitchTests(unittest.TestCase):
    """PRISM_WINDOW_PIPELINE_RUST: default Python, strict values, inert off."""

    def test_switch_defaults_to_python(self) -> None:
        self.assertFalse(env_window_pipeline_rust(environ={}))

    def test_switch_accepts_boolean_tokens(self) -> None:
        for raw in ("1", "true", "YES", "On"):
            with self.subTest(raw=raw):
                self.assertTrue(
                    env_window_pipeline_rust(
                        environ={"PRISM_WINDOW_PIPELINE_RUST": raw}
                    )
                )
        for raw in ("0", "false", "NO", "Off"):
            with self.subTest(raw=raw):
                self.assertFalse(
                    env_window_pipeline_rust(
                        environ={"PRISM_WINDOW_PIPELINE_RUST": raw}
                    )
                )

    def test_switch_fails_closed_on_non_boolean_values(self) -> None:
        for raw in ("2", "banana", "tru"):
            with self.subTest(raw=raw):
                with self.assertRaisesRegex(
                    SystemExit, "PRISM_WINDOW_PIPELINE_RUST must be a boolean"
                ):
                    env_window_pipeline_rust(
                        environ={"PRISM_WINDOW_PIPELINE_RUST": raw}
                    )

    def test_coordinator_config_load_validates_the_switch_at_startup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = {
                "QBIT_RPC_HOST": "qbit.example",
                "QBIT_RPC_USER": "rpc-user",
                "QBIT_RPC_PASSWORD": "rpc-password",
                "PRISM_ALLOW_MEMORY_LEDGER": "1",
                "PRISM_ALLOW_TEST_SIGNING_SEEDS": "1",
                "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "1",
                "PRISM_AUDIT_DIR": str(root),
                "PRISM_EVIDENCE_PATH": str(root / "evidence.json"),
            }
            config = load_coordinator_config(base)
            self.assertFalse(config.jobs.window_pipeline_rust_enabled)
            enabled = load_coordinator_config(
                {**base, "PRISM_WINDOW_PIPELINE_RUST": "1"}
            )
            self.assertTrue(enabled.jobs.window_pipeline_rust_enabled)
            with self.assertRaisesRegex(
                SystemExit, "PRISM_WINDOW_PIPELINE_RUST must be a boolean"
            ):
                load_coordinator_config(
                    {**base, "PRISM_WINDOW_PIPELINE_RUST": "maybe"}
                )


class DaemonShareWindowMirrorTests(unittest.TestCase):
    """The digest-verified byte mirror and its lazy JSON sequence."""

    def _window(self) -> IncrementalShareWindow:
        records = [
            stamped_pending_share(999_900 + seq) for seq in range(3)
        ]
        ledger = SingleWriterShareLedger()
        appended = [ledger.append(record) for record in records]
        return IncrementalShareWindow.from_full_snapshot(
            appended,
            anchor_job_issued_at_ms=1_000_000,
            window_weight=1_000,
        )

    def test_full_mirror_verifies_and_parses_lazily(self) -> None:
        window = self._window()
        items = _canonical_items(window)
        digest = window.json_records().canonical_json_sha256()
        mirror = DaemonShareWindowMirror.from_full_items(
            anchor_job_issued_at_ms=1_000_000,
            window_weight=1_000,
            page_size=512,
            record_count=window.record_count,
            canonical_items=items,
            share_snapshot_sha256=digest,
        )
        sequence = mirror.json_records()
        self.assertIsInstance(sequence, DaemonShareJsonSequence)
        self.assertEqual(len(sequence), window.record_count)
        self.assertIsNone(sequence._parsed)
        self.assertEqual(sequence.canonical_json_sha256(), digest)
        # The digest never forces a parse; iteration does, exactly once.
        self.assertIsNone(sequence._parsed)
        self.assertEqual(list(sequence), list(window.json_records()))
        self.assertEqual(sequence[0], window.json_records()[0])

    def test_full_mirror_rejects_bytes_that_do_not_hash_to_the_digest(self) -> None:
        window = self._window()
        digest = window.json_records().canonical_json_sha256()
        with self.assertRaises(DaemonWindowMirrorDivergence):
            DaemonShareWindowMirror.from_full_items(
                anchor_job_issued_at_ms=1_000_000,
                window_weight=1_000,
                page_size=512,
                record_count=window.record_count,
                canonical_items=_canonical_items(window) + b" ",
                share_snapshot_sha256=digest,
            )

    def test_full_mirror_rejects_a_count_the_bytes_refute(self) -> None:
        window = self._window()
        items = _canonical_items(window)
        digest = window.json_records().canonical_json_sha256()
        for declared in (window.record_count - 1, window.record_count + 1):
            with self.subTest(declared=declared):
                # Eager, beside the digest check: the bytes are right, only
                # the count beside them is wrong, and that must not be left
                # for whichever consumer first parses the stream.
                with self.assertRaisesRegex(
                    DaemonWindowMirrorDivergence,
                    f"holds {window.record_count} records where {declared}",
                ):
                    DaemonShareWindowMirror.from_full_items(
                        anchor_job_issued_at_ms=1_000_000,
                        window_weight=1_000,
                        page_size=512,
                        record_count=declared,
                        canonical_items=items,
                        share_snapshot_sha256=digest,
                    )

    def test_full_mirror_rejects_a_stream_that_is_not_whole_records(
        self,
    ) -> None:
        window = self._window()
        items = _canonical_items(window)
        for broken in (items + b",", b"," + items, items[:-1]):
            with self.subTest(broken=broken[:8]):
                digest = hashlib.sha256(b"[" + broken + b"]").hexdigest()
                with self.assertRaises(DaemonWindowMirrorDivergence):
                    DaemonShareWindowMirror.from_full_items(
                        anchor_job_issued_at_ms=1_000_000,
                        window_weight=1_000,
                        page_size=512,
                        record_count=window.record_count,
                        canonical_items=broken,
                        share_snapshot_sha256=digest,
                    )

    def test_advanced_applies_drop_and_append_surgery_exactly(self) -> None:
        window = self._window()
        mirror = DaemonShareWindowMirror.from_full_items(
            anchor_job_issued_at_ms=1_000_000,
            window_weight=1_000,
            page_size=512,
            record_count=window.record_count,
            canonical_items=_canonical_items(window),
            share_snapshot_sha256=window.json_records().canonical_json_sha256(),
        )
        ledger = SingleWriterShareLedger(first_share_seq=10)
        delta = [ledger.append(stamped_pending_share(1_000_010))]
        advanced_window, _stats = window.advance(
            delta,
            anchor_job_issued_at_ms=1_000_020,
        )
        old_items = _canonical_items(window)
        new_items = _canonical_items(advanced_window)
        self.assertTrue(new_items.startswith(old_items))
        appended = new_items[len(old_items):]
        advanced = mirror.advanced(
            anchor_job_issued_at_ms=1_000_020,
            record_count=advanced_window.record_count,
            retained_drop_bytes=0,
            appended_items=appended,
            share_snapshot_sha256=(
                advanced_window.json_records().canonical_json_sha256()
            ),
        )
        self.assertEqual(advanced.canonical_items, new_items)
        self.assertEqual(advanced.anchor_job_issued_at_ms, 1_000_020)
        # The advance's count is counted from the surgery, not accepted.
        with self.assertRaisesRegex(
            DaemonWindowMirrorDivergence,
            "records where",
        ):
            mirror.advanced(
                anchor_job_issued_at_ms=1_000_020,
                record_count=advanced_window.record_count + 1,
                retained_drop_bytes=0,
                appended_items=appended,
                share_snapshot_sha256=(
                    advanced_window.json_records().canonical_json_sha256()
                ),
            )
        with self.assertRaises(DaemonWindowMirrorDivergence):
            mirror.advanced(
                anchor_job_issued_at_ms=1_000_020,
                record_count=advanced_window.record_count,
                retained_drop_bytes=1,
                appended_items=appended,
                share_snapshot_sha256=(
                    advanced_window.json_records().canonical_json_sha256()
                ),
            )

    def test_anchor_only_advance_reuses_bytes_and_checks_the_digest(self) -> None:
        window = self._window()
        digest = window.json_records().canonical_json_sha256()
        mirror = DaemonShareWindowMirror.from_full_items(
            anchor_job_issued_at_ms=1_000_000,
            window_weight=1_000,
            page_size=512,
            record_count=window.record_count,
            canonical_items=_canonical_items(window),
            share_snapshot_sha256=digest,
        )
        advanced = mirror.advanced(
            anchor_job_issued_at_ms=1_000_050,
            record_count=window.record_count,
            retained_drop_bytes=0,
            appended_items=b"",
            share_snapshot_sha256=digest,
        )
        self.assertIs(advanced.canonical_items, mirror.canonical_items)
        with self.assertRaisesRegex(
            DaemonWindowMirrorDivergence,
            "records where",
        ):
            mirror.advanced(
                anchor_job_issued_at_ms=1_000_050,
                record_count=window.record_count + 1,
                retained_drop_bytes=0,
                appended_items=b"",
                share_snapshot_sha256=digest,
            )
        with self.assertRaises(DaemonWindowMirrorDivergence):
            mirror.advanced(
                anchor_job_issued_at_ms=1_000_050,
                record_count=window.record_count,
                retained_drop_bytes=0,
                appended_items=b"",
                share_snapshot_sha256="0" * 64,
            )


class DaemonWindowPipelineTestCase(unittest.TestCase):
    """Shared coordinator/fake-daemon fixture for the window-pipeline tests."""

    def _server(self) -> tuple[PrismCoordinator, IncrementalRecordingLedger, object]:
        ledger = IncrementalRecordingLedger()
        append_incremental_share(ledger, share_seq=1, accepted_at_ms=999_900)
        append_incremental_share(ledger, share_seq=2, accepted_at_ms=999_910)
        append_incremental_share(ledger, share_seq=3, accepted_at_ms=999_920)
        server, rpc = coordinator(ledger=ledger)
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 60.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        self.addCleanup(server.shutdown_serve_builder)
        return server, ledger, artifacts

    def _rust_env(self, mode: str = "ok", **extra: str):
        values = {
            "PRISM_WINDOW_PIPELINE_RUST": "1",
            "FAKE_SERVE_BUILDER_MODE": mode,
            **extra,
        }
        return patch.dict(os.environ, values)

    def _fake_daemon_command(self):
        return patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=list(FAKE_SERVE_BUILDER_COMMAND),
        )

    def _serve_counts(self, server: PrismCoordinator) -> dict[str, int]:
        with server._serve_builder_metrics_lock:
            return dict(server.serve_builder_counts)


class WindowPipelineRustMaterializationTests(DaemonWindowPipelineTestCase):
    """The daemon-backed materialization path against the fake serve builder."""

    def test_full_then_advance_materializes_daemon_windows_byte_exactly(
        self,
    ) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, DaemonShareWindowMirror)
            self.assertIsInstance(initial.shares_json, DaemonShareJsonSequence)
            # The routine path must not have parsed the mirror's dicts.
            self.assertIsNone(initial.shares_json._parsed)
            oracle = _python_window_for(
                ledger,
                anchor_ms=int(initial.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                initial.share_snapshot_sha256,
                oracle.json_records().canonical_json_sha256(),
            )
            self.assertEqual(ledger.full_snapshot_calls, 1)

            append_incremental_share(ledger, share_seq=4, accepted_at_ms=1_000_010)
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertEqual(advanced.window_delta_rows, 1)
            self.assertEqual(ledger.full_snapshot_calls, 1)
            self.assertEqual(ledger.delta_snapshot_calls, 1)
            advanced_oracle = _python_window_for(
                ledger,
                anchor_ms=int(advanced.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                advanced.share_snapshot_sha256,
                advanced_oracle.json_records().canonical_json_sha256(),
            )
            # Forcing the rare-path parse must reproduce the oracle's dicts.
            self.assertEqual(
                list(advanced.shares_json),
                list(advanced_oracle.json_records()),
            )
        counts = self._serve_counts(server)
        self.assertEqual(counts["window_prepares"], 2)
        self.assertEqual(counts["spawns"], 1)
        self.assertEqual(counts["fallbacks"], 0)

    def test_prepared_window_serves_builds_without_upload_or_parse(self) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            serialization = server._share_window_serialization_for_artifact(
                artifact,
                artifact.shares_json,
            )
            summary = server.build_audit_bundle(
                shares=artifact.shares_json,
                found_block={
                    "block_height": 101,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": int(artifacts.network_difficulty),
                    "anchor_job_issued_at_ms": int(
                        artifact.snapshot_anchor_ms or 0
                    ),
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
                payout_policy={"policy": "day-one"},
                share_serialization=serialization,  # type: ignore[arg-type]
                append_invalidation_epoch=artifact.append_invalidation_epoch,
            )
            self.assertEqual(summary["transport"], "serve")
            # The prepared window is the build cache entry: no upload rode
            # along, and the routine build never materialized the dicts.
            self.assertFalse(summary["request_had_window"])
            self.assertIsNone(artifact.shares_json._parsed)
        counts = self._serve_counts(server)
        self.assertEqual(counts["window_uploads"], 0)
        self.assertEqual(counts["requests"], 1)

    def test_epoch_bump_between_daemon_call_and_install_discards_artifact(
        self,
    ) -> None:
        server, _ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            with server._job_cache_lock:
                server._payout_ledger_append_invalidation_epoch += 1
            self.assertFalse(server._install_payout_ledger_artifact(artifact))
            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)

    def test_inflight_scan_anchor_spans_the_daemon_round_trip(self) -> None:
        server, _ledger, artifacts = self._server()
        observed_anchors: list[dict[int, int]] = []
        real_prepare = server.prepare_payout_window

        def observing_prepare(**kwargs: object) -> object:
            with server._job_cache_lock:
                observed_anchors.append(
                    dict(server._payout_window_inflight_scan_anchors)
                )
            return real_prepare(**kwargs)

        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ), patch.object(server, "prepare_payout_window", observing_prepare):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
        assert artifact is not None
        self.assertEqual(len(observed_anchors), 1)
        self.assertIn(
            int(artifact.snapshot_anchor_ms or 0),
            observed_anchors[0].values(),
        )
        with server._job_cache_lock:
            self.assertEqual(server._payout_window_inflight_scan_anchors, {})

    def test_daemon_death_mid_prepare_degrades_to_python_same_build(self) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(mode="crash-during-prepare"), (
            self._fake_daemon_command()
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            self.assertEqual(artifact.window_build_mode, "full_rescan")
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, IncrementalShareWindow)
            oracle = _python_window_for(
                ledger,
                anchor_ms=int(artifact.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                artifact.share_snapshot_sha256,
                oracle.json_records().canonical_json_sha256(),
            )
            self.assertTrue(server._install_payout_ledger_artifact(artifact))
        counts = self._serve_counts(server)
        self.assertEqual(counts["fallbacks"], 1)
        self.assertEqual(counts["window_prepares"], 0)

    def test_needs_full_after_respawn_reprepares_identical_digest(self) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            # The daemon dies between generations; its prepared state dies
            # with it, while the coordinator's mirror stays digest-verified.
            client = server._serve_builder
            assert client is not None
            client.process.kill()
            client.process.wait(timeout=5.0)

            append_incremental_share(ledger, share_seq=4, accepted_at_ms=1_000_010)
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert rebuilt is not None
            self.assertEqual(rebuilt.window_build_mode, "full_rescan")
            self.assertEqual(
                rebuilt.window_full_rescan_reason,
                "window_daemon_state_lost",
            )
            oracle = _python_window_for(
                ledger,
                anchor_ms=int(rebuilt.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                rebuilt.share_snapshot_sha256,
                oracle.json_records().canonical_json_sha256(),
            )
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, DaemonShareWindowMirror)
        counts = self._serve_counts(server)
        self.assertEqual(counts["spawns"], 2)
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_advance_invariant_failure_full_rescans_without_retiring_daemon(
        self,
    ) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(mode="prepare-fallback"), (
            self._fake_daemon_command()
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            append_incremental_share(ledger, share_seq=4, accepted_at_ms=1_000_010)
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert rebuilt is not None
            self.assertEqual(rebuilt.window_build_mode, "full_rescan")
            self.assertEqual(
                rebuilt.window_full_rescan_reason,
                "incremental_invariant_failed",
            )
            client = server._serve_builder
            assert client is not None
            self.assertIsNone(client.process.poll())
        counts = self._serve_counts(server)
        self.assertEqual(counts["fallbacks"], 0)
        self.assertEqual(counts["spawns"], 1)
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_protocol_mismatch_retires_daemon_and_degrades_to_python(self) -> None:
        server, _ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(FAKE_SERVE_BUILDER_PROTOCOL="1"), (
            self._fake_daemon_command()
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, IncrementalShareWindow)
            self.assertIsNone(server._serve_builder)
        counts = self._serve_counts(server)
        self.assertEqual(counts["fallbacks"], 1)
        self.assertEqual(counts["window_prepares"], 0)

    def test_switch_defaults_to_python_pipeline(self) -> None:
        server, _ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with patch.dict(os.environ) as _env, self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            os.environ.pop("PRISM_WINDOW_PIPELINE_RUST", None)
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
        assert artifact is not None
        cached = server._incremental_payout_artifact_window
        assert cached is not None
        self.assertIsInstance(cached.window, IncrementalShareWindow)
        counts = self._serve_counts(server)
        self.assertEqual(counts["spawns"], 0)
        self.assertEqual(counts["window_prepares"], 0)

    def test_switch_is_inert_without_the_daemon_transport(self) -> None:
        server, _ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(PRISM_BUILDER_SERVE="0"), (
            self._fake_daemon_command()
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
        assert artifact is not None
        cached = server._incremental_payout_artifact_window
        assert cached is not None
        self.assertIsInstance(cached.window, IncrementalShareWindow)
        counts = self._serve_counts(server)
        self.assertEqual(counts["spawns"], 0)
        self.assertEqual(counts["window_prepares"], 0)

    def test_flipping_the_switch_off_rebuilds_from_the_oracle(self) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            with self._rust_env():
                initial = server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
                assert initial is not None
                cached = server._incremental_payout_artifact_window
                assert cached is not None
                self.assertIsInstance(cached.window, DaemonShareWindowMirror)
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            with patch.dict(
                os.environ, {"PRISM_WINDOW_PIPELINE_RUST": "0"}
            ):
                rebuilt = server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
        assert rebuilt is not None
        self.assertEqual(rebuilt.window_build_mode, "full_rescan")
        self.assertEqual(
            rebuilt.window_full_rescan_reason,
            "window_pipeline_mode_changed",
        )
        cached = server._incremental_payout_artifact_window
        assert cached is not None
        self.assertIsInstance(cached.window, IncrementalShareWindow)
        self.assertEqual(ledger.full_snapshot_calls, 2)


class DaemonWindowContentionTests(DaemonWindowPipelineTestCase):
    """A daemon another build owns is degraded around, never retired.

    The prepare path used to stamp its deadline before an untimed blocking
    acquire, so a preparation that queued behind a slow build arrived inside
    the lock with its whole budget already spent: the first write read as a
    daemon timeout, and the anomaly handler SIGKILLed a daemon that had done
    nothing wrong. Waiting out a lock is not a daemon fault.
    """

    def _lock_holder(self, server: PrismCoordinator, hold_seconds: float):
        """Own the serve lock from another thread for a bounded window."""
        taken = threading.Event()
        release = threading.Event()

        def hold() -> None:
            server._serve_builder_lock.acquire()
            try:
                taken.set()
                release.wait(hold_seconds)
            finally:
                server._serve_builder_lock.release()

        thread = threading.Thread(target=hold, daemon=True)
        thread.start()
        self.assertTrue(taken.wait(5.0))
        self.addCleanup(thread.join, 10.0)
        self.addCleanup(release.set)
        return release

    def _worker_counts(self, server: PrismCoordinator) -> dict[str, int]:
        with server._job_build_scheduler_lock:
            return dict(server.job_build_worker_counts)

    def test_contended_daemon_degrades_without_being_retired(self) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertIsInstance(
                server._incremental_payout_artifact_window.window,
                DaemonShareWindowMirror,
            )
            daemon = server._serve_builder
            assert daemon is not None

            append_incremental_share(ledger, share_seq=4, accepted_at_ms=1_000_010)
            server.payout_artifact_min_build_interval_seconds = 0.0
            server.bundle_build_timeout_seconds = 0.2
            clock_ms[0] = 1_000_020
            # Held past this preparation's whole budget: the pre-fix code
            # blocked here, then read its own exhausted deadline as a sick
            # daemon and killed it.
            self._lock_holder(server, 3.0)
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert rebuilt is not None
            self.assertEqual(rebuilt.window_build_mode, "full_rescan")
            self.assertEqual(
                rebuilt.window_full_rescan_reason,
                "window_daemon_busy",
            )
            # Degraded to the in-process fold for this materialization, and
            # still byte-exact against the oracle.
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, IncrementalShareWindow)
            oracle = _python_window_for(
                ledger,
                anchor_ms=int(rebuilt.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                rebuilt.share_snapshot_sha256,
                oracle.json_records().canonical_json_sha256(),
            )
            # The daemon is the same live process it was before the contest.
            self.assertIs(server._serve_builder, daemon)
            self.assertIsNone(daemon.process.poll())
        counts = self._serve_counts(server)
        self.assertEqual(counts["fallbacks"], 0)
        self.assertEqual(counts["spawns"], 1)
        # One refusal for the advance, one for the full re-preparation.
        self.assertEqual(counts["window_prepare_contended"], 2)
        self.assertEqual(self._worker_counts(server)["terminations"], 0)
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_deadline_is_stamped_after_the_daemon_lock_is_taken(self) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        server.bundle_build_timeout_seconds = 1.0
        with self._rust_env(
            mode="prepare-slow",
            FAKE_SERVE_BUILDER_PREPARE_DELAY_SECONDS="0.6",
        ), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertIsInstance(
                server._incremental_payout_artifact_window.window,
                DaemonShareWindowMirror,
            )

            append_incremental_share(ledger, share_seq=4, accepted_at_ms=1_000_010)
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            # Most of the budget is spent waiting; the round trip that
            # follows needs more than what would be left of it. Charging the
            # wait against the exchange is what used to retire the daemon.
            self._lock_holder(server, 0.7)
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertIsNone(advanced.window_full_rescan_reason)
            self.assertIsInstance(
                server._incremental_payout_artifact_window.window,
                DaemonShareWindowMirror,
            )
            oracle = _python_window_for(
                ledger,
                anchor_ms=int(advanced.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                advanced.share_snapshot_sha256,
                oracle.json_records().canonical_json_sha256(),
            )
        counts = self._serve_counts(server)
        self.assertEqual(counts["fallbacks"], 0)
        self.assertEqual(counts["spawns"], 1)
        self.assertEqual(counts["window_prepares"], 2)
        self.assertEqual(ledger.full_snapshot_calls, 1)

    def test_urgent_preview_materialization_never_queues_for_the_daemon(
        self,
    ) -> None:
        server, _ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        server.bundle_build_timeout_seconds = 30.0
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            clock_ms[0] = 1_000_020
            # An accepted-block preview publishes on the critical path. It
            # takes _payout_state_prepare_lock non-blocking for exactly that
            # reason, so its materialization must not then wait on the serve
            # lock either.
            self._lock_holder(server, 30.0)
            started = time.monotonic()
            preview = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                bypass_build_interval=True,
            )
            waited = time.monotonic() - started
            assert preview is not None
        self.assertLess(waited, 5.0)
        self.assertEqual(preview.window_build_mode, "full_rescan")
        self.assertEqual(
            preview.window_full_rescan_reason,
            "window_daemon_busy",
        )
        counts = self._serve_counts(server)
        self.assertEqual(counts["fallbacks"], 0)
        self.assertEqual(counts["window_prepare_contended"], 2)


class DaemonWindowRecordCountTests(DaemonWindowPipelineTestCase):
    """A declared record count is reconciled with the bytes, eagerly.

    The construction-time digest pins the bytes and says nothing about the
    count beside them, so a miscount used to surface only at whichever
    consumer first parsed the stream -- possibly inside a serve request that
    had already written its prefix, leaving the daemon's stdin holding an
    unterminated line for the next request to concatenate onto.
    """

    def test_miscounted_window_is_refused_at_materialization(self) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(mode="prepare-miscount"), (
            self._fake_daemon_command()
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            # Refused where it was built: the mirror never reaches the cache,
            # so no consumer can be the first to learn of the divergence.
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, IncrementalShareWindow)
            self.assertNotIsInstance(
                artifact.shares_json,
                DaemonShareJsonSequence,
            )
            oracle = _python_window_for(
                ledger,
                anchor_ms=int(artifact.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                artifact.share_snapshot_sha256,
                oracle.json_records().canonical_json_sha256(),
            )
            # A wrong count is the daemon's answer being wrong, not the
            # daemon being sick: it stays registered and alive.
            client = server._serve_builder
            assert client is not None
            self.assertIsNone(client.process.poll())
        with server._payout_artifact_executor_lock:
            events = dict(server.payout_artifact_event_counts)
        self.assertEqual(events.get("window_mirror_divergence"), 1)
        self.assertEqual(self._serve_counts(server)["fallbacks"], 0)

    def test_half_written_request_retires_the_poisoned_daemon(self) -> None:
        server, _ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            assert isinstance(artifact.shares_json, DaemonShareJsonSequence)
            # A sequence that only refuses itself once iterated, standing in
            # for whatever could still reach a build holding the serve lock.
            poisoned = DaemonShareJsonSequence(
                canonical_items=artifact.shares_json.canonical_items,
                record_count=len(artifact.shares_json) + 1,
            )
            found_block = {
                "block_height": 101,
                "coinbase_value_sats": 50_00000000,
                "network_difficulty": int(artifacts.network_difficulty),
                "anchor_job_issued_at_ms": int(artifact.snapshot_anchor_ms or 0),
            }
            serialization = server._share_window_serialization_for_artifact(
                artifact,
                poisoned,
            )
            daemon = server._serve_builder
            assert daemon is not None
            # Force the window to ride along: the divergence has to be
            # raised after the request prefix is already on the daemon's
            # stdin, which is the case the narrow catch used to miss.
            daemon.uploaded_windows.clear()
            with self.assertRaises(DaemonWindowMirrorDivergence):
                server.build_audit_bundle(
                    shares=poisoned,
                    found_block=found_block,
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                    summary_only=True,
                    payout_policy={"policy": "day-one"},
                    share_serialization=serialization,  # type: ignore[arg-type]
                    append_invalidation_epoch=artifact.append_invalidation_epoch,
                )
            # The request never reached its terminating newline, so the
            # daemon went with it rather than answering "malformed serve
            # request" to the next innocent build.
            self.assertIsNone(server._serve_builder)
            self.assertIsNotNone(daemon.process.poll())
            self.assertIsNone(server._incremental_payout_artifact_window)

            # The next honest build still gets the daemon transport.
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert rebuilt is not None
            summary = server.build_audit_bundle(
                shares=rebuilt.shares_json,
                found_block=found_block,
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
                payout_policy={"policy": "day-one"},
                share_serialization=(
                    server._share_window_serialization_for_artifact(
                        rebuilt,
                        rebuilt.shares_json,
                    )
                ),  # type: ignore[arg-type]
                append_invalidation_epoch=rebuilt.append_invalidation_epoch,
            )
            self.assertEqual(summary["transport"], "serve")
        with server._payout_artifact_executor_lock:
            events = dict(server.payout_artifact_event_counts)
        self.assertEqual(events.get("window_mirror_divergence"), 1)


class DaemonWindowIntegerDomainTests(DaemonWindowPipelineTestCase):
    """A value outside the daemon's declared widths degrades to Python; the daemon lives.

    The daemon carries ``share_difficulty`` in u128 (and the other integers
    in u64/u32/i64); Python carries them unbounded. A window holding a value
    above those widths used to come back as a malformed-request error, which
    the coordinator read as an anomaly: it SIGKILLed a healthy daemon,
    degraded once, and respawned on the next materialization only to kill
    it again -- a spawn-and-kill loop, once per materialization, for as long
    as the window held the value. The daemon now declines such a request as
    ``out_of_range`` and keeps its state; the coordinator folds the window
    in-process and never touches the daemon.
    """

    def _append_wide_share(
        self,
        ledger: SingleWriterShareLedger,
        *,
        share_seq: int,
        accepted_at_ms: int,
    ) -> None:
        """A share whose difficulty is one above u128: inside Python, outside the daemon."""
        ledger.append(
            PendingShare(
                share_id=f"wide-{share_seq}",
                miner_id=f"miner-{share_seq % 3}",
                order_key=f"miner-{share_seq % 3}",
                p2mr_program_hex=f"{share_seq % 256:02x}" * 32,
                share_difficulty=2**128 + 1,
                network_difficulty=1,
                template_height=9,
                job_id=f"job-{share_seq}",
                job_issued_at_ms=accepted_at_ms - 1,
                accepted_at_ms=accepted_at_ms,
                ntime=1_700_000_000 + share_seq,
            )
        )

    def _worker_counts(self, server: PrismCoordinator) -> dict[str, int]:
        with server._job_build_scheduler_lock:
            return dict(server.job_build_worker_counts)

    def test_out_of_range_snapshot_folds_in_python_and_keeps_the_daemon(self) -> None:
        server, ledger, artifacts = self._server()
        self._append_wide_share(ledger, share_seq=4, accepted_at_ms=999_930)
        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            self.assertEqual(artifact.window_build_mode, "full_rescan")
            # Declined by the daemon, folded in-process: a Python window is
            # cached, byte-exact against the oracle, with the wide digits.
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, IncrementalShareWindow)
            oracle = _python_window_for(
                ledger,
                anchor_ms=int(artifact.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                artifact.share_snapshot_sha256,
                oracle.json_records().canonical_json_sha256(),
            )
            self.assertTrue(
                any(
                    int(record["share_difficulty"]) == 2**128 + 1
                    for record in artifact.shares_json
                )
            )
            self.assertTrue(server._install_payout_ledger_artifact(artifact))
            # The daemon was never retired: same live process, registered.
            daemon = server._serve_builder
            assert daemon is not None
            self.assertIsNone(daemon.process.poll())

            # The next materialization still holds the value. Before the fix
            # this is where the loop turned: a fresh spawn, declined again,
            # killed again. Now the degraded Python cache advances in
            # process and the daemon is left alone.
            append_incremental_share(ledger, share_seq=5, accepted_at_ms=1_000_010)
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertIs(server._serve_builder, daemon)
            self.assertIsNone(daemon.process.poll())
            advanced_oracle = _python_window_for(
                ledger,
                anchor_ms=int(advanced.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                advanced.share_snapshot_sha256,
                advanced_oracle.json_records().canonical_json_sha256(),
            )
        counts = self._serve_counts(server)
        self.assertEqual(counts["spawns"], 1)
        self.assertEqual(counts["fallbacks"], 0)
        # window_prepares counts completed round trips, declined ones
        # included: one, and it was the decline. The Python-advancing
        # second materialization never asked the daemon at all.
        self.assertEqual(counts["window_prepares"], 1)
        self.assertEqual(counts["window_prepare_out_of_range"], 1)
        self.assertEqual(self._worker_counts(server)["terminations"], 0)

    def test_out_of_range_delta_rebuilds_in_python_and_keeps_the_daemon(self) -> None:
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(), self._fake_daemon_command(), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, DaemonShareWindowMirror)
            daemon = server._serve_builder
            assert daemon is not None

            # The wide share arrives as a delta against a daemon-held mirror:
            # the daemon declines the advance, the full re-preparation is
            # declined the same way, and the in-process fold takes over.
            self._append_wide_share(ledger, share_seq=4, accepted_at_ms=1_000_010)
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert rebuilt is not None
            self.assertEqual(rebuilt.window_build_mode, "full_rescan")
            self.assertEqual(
                rebuilt.window_full_rescan_reason,
                "window_value_out_of_range",
            )
            cached = server._incremental_payout_artifact_window
            assert cached is not None
            self.assertIsInstance(cached.window, IncrementalShareWindow)
            oracle = _python_window_for(
                ledger,
                anchor_ms=int(rebuilt.snapshot_anchor_ms or 0),
                network_difficulty=int(artifacts.network_difficulty),
            )
            self.assertEqual(
                rebuilt.share_snapshot_sha256,
                oracle.json_records().canonical_json_sha256(),
            )
            self.assertIs(server._serve_builder, daemon)
            self.assertIsNone(daemon.process.poll())
        counts = self._serve_counts(server)
        self.assertEqual(counts["spawns"], 1)
        self.assertEqual(counts["fallbacks"], 0)
        # Three completed round trips: the initial full preparation
        # succeeded, then the advance and the full re-preparation were
        # both declined.
        self.assertEqual(counts["window_prepares"], 3)
        self.assertEqual(counts["window_prepare_out_of_range"], 2)
        self.assertEqual(self._worker_counts(server)["terminations"], 0)
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_rejection_envelopes_carry_the_category_not_just_the_message(self) -> None:
        # The fold_invalid/fallback outcomes name their condition in a stable
        # category beside the diagnostic message, so the coordinator (and the
        # parity adapter) never have to match on prose that may drift.
        server, ledger, artifacts = self._server()
        clock_ms = [1_000_000]
        with self._rust_env(mode="prepare-fallback"), (
            self._fake_daemon_command()
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            outcome = server.prepare_payout_window(
                mode="advance",
                records_json=[],
                anchor_job_issued_at_ms=1_000_010,
                append_invalidation_epoch=0,
                base_digest=initial.share_snapshot_sha256,
            )
            assert outcome is not None
            self.assertEqual(outcome.status, "fallback")
            self.assertEqual(outcome.rejection, "delta_not_append")
            duplicate = dict(initial.shares_json[0])
            declined = server.prepare_payout_window(
                mode="full",
                records_json=[duplicate, duplicate],
                anchor_job_issued_at_ms=1_000_000,
                append_invalidation_epoch=0,
                window_weight=1_000,
                page_size=512,
            )
            assert declined is not None
            self.assertEqual(declined.status, "fold_invalid")
            self.assertEqual(declined.rejection, "duplicate_share_seq")


class DaemonWindowRetagTests(DaemonWindowPipelineTestCase):
    """A retagged window advances in place, Rust switch on or off.

    _retire_payout_windows_for_late_append clears the cache only when the
    late row predates the cached anchor, and otherwise retags it precisely so
    the next delta build advances the window it already has. A daemon that
    refused an older-tagged base turned every such retag into a full DB walk,
    fold and upload -- the common case made worse, under exactly the replay
    bursts that produce the bumps.
    """

    def _retag_after_late_append(self, server: PrismCoordinator) -> int:
        cached = server._incremental_payout_artifact_window
        assert cached is not None
        anchor_ms = int(cached.window.anchor_job_issued_at_ms)
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1
            epoch = int(server._payout_ledger_append_invalidation_epoch)
        # Stamped above the cached anchor: the armed artifact was affected,
        # this incremental window was not, so the coordinator retags it.
        server._retire_payout_windows_for_late_append(
            stamped_pending_share(anchor_ms + 1_000),
            epoch,
        )
        retagged = server._incremental_payout_artifact_window
        assert retagged is not None
        self.assertEqual(retagged.append_invalidation_epoch, epoch)
        return epoch

    def test_retagged_window_advances_incrementally_on_both_pipelines(
        self,
    ) -> None:
        for rust in (False, True):
            with self.subTest(rust=rust):
                server, ledger, artifacts = self._server()
                clock_ms = [1_000_000]
                env = (
                    self._rust_env()
                    if rust
                    else patch.dict(
                        os.environ,
                        {"PRISM_WINDOW_PIPELINE_RUST": "0"},
                    )
                )
                with env, self._fake_daemon_command(), patch(
                    "lab.prism.prism_coordinator.now_ms",
                    side_effect=lambda: clock_ms[0],
                ):
                    initial = server._build_payout_ledger_artifact(
                        0, 0, artifacts.network_difficulty
                    )
                    assert initial is not None
                    self.assertEqual(ledger.full_snapshot_calls, 1)
                    self._retag_after_late_append(server)

                    append_incremental_share(
                        ledger, share_seq=4, accepted_at_ms=1_000_010
                    )
                    server.payout_artifact_min_build_interval_seconds = 0.0
                    clock_ms[0] = 1_000_020
                    advanced = server._build_payout_ledger_artifact(
                        0, 0, artifacts.network_difficulty
                    )
                    assert advanced is not None
                    self.assertEqual(advanced.window_build_mode, "incremental")
                    self.assertIsNone(advanced.window_full_rescan_reason)
                    self.assertEqual(advanced.window_delta_rows, 1)
                    # The retag survived the advance: no oracle was paid.
                    self.assertEqual(ledger.full_snapshot_calls, 1)
                    oracle = _python_window_for(
                        ledger,
                        anchor_ms=int(advanced.snapshot_anchor_ms or 0),
                        network_difficulty=int(artifacts.network_difficulty),
                    )
                    self.assertEqual(
                        advanced.share_snapshot_sha256,
                        oracle.json_records().canonical_json_sha256(),
                    )
                    cached = server._incremental_payout_artifact_window
                    assert cached is not None
                    self.assertIsInstance(
                        cached.window,
                        DaemonShareWindowMirror if rust else IncrementalShareWindow,
                    )


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Coordinator-side tests for the Rust payout-window pipeline slice.

Why these tests exist: the byte-level equivalence of the Rust fold is proven
by the parity oracle (``tests.test_window_pipeline_parity`` with the
``rust-daemon`` adapter); what still needs proof is the coordinator's side of
the contract -- that the ``PRISM_WINDOW_PIPELINE_RUST`` switch defaults to
Python and fails closed, that the daemon path preserves the append-epoch and
scan-anchor fences, and that every non-success shape (daemon death,
``needs_full`` after a respawn, an ``advance`` invariant failure, a protocol
mismatch) degrades exactly like its in-process counterpart instead of
inventing a new failure mode. The daemon here is the fake serve builder,
which serves prepare_window with the real Python fold so mirror digests
verify byte-exactly without a Rust build.
"""
# ruff: noqa: F403, F405

from __future__ import annotations

import os
import tempfile
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
        with self.assertRaises(DaemonWindowMirrorDivergence):
            mirror.advanced(
                anchor_job_issued_at_ms=1_000_050,
                record_count=window.record_count,
                retained_drop_bytes=0,
                appended_items=b"",
                share_snapshot_sha256="0" * 64,
            )


class WindowPipelineRustMaterializationTests(unittest.TestCase):
    """The daemon-backed materialization path against the fake serve builder."""

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


if __name__ == "__main__":
    unittest.main()

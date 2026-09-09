#!/usr/bin/env python3
"""Bounded canonical candidate finalization and audit body publication (#255).

End to end through the real Rust builder and verifier when the tools are
available: the compiler streams any share sequence into the builder and
hands back a bounded view of the canonical artifact; the audit store
publishes the canonical bundle, the compact body and its share segments
from that artifact through bounded chunks while preserving exact canonical
bytes, digests, signature and coinbase validation, inode identity binding
and idempotent retries; and accepted-block persistence consumes the view.
The fake-builder cases cover the compiler seam without the tools.
"""

from __future__ import annotations

from collections.abc import Sequence
import gc
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock
import weakref
from typing import Any

from lab.prism.audit_artifacts import (
    AuditArtifactConfig,
    AuditArtifactStore,
    gzip_canonical_bundle_bytes,
)
from lab.prism.audit_bundle_view import (
    SCAN_CHUNK_BYTES,
    CanonicalAuditBundleView,
    LazyRecordSequence,
)
from lab.prism.bundle_compiler import _iter_build_input_chunks
from lab.prism.prism_tools import prism_tool_command
from lab.prism.share_json_stream import iter_json_object_text_chunks
from lab.prism.share_ledger import PsqlShareLedger
from tests.prism_coordinator_test_support import coordinator as make_coordinator
from tests.prism_vardiff_test_support import fake_audit_bundle_popen


BLOCK_HASH = "a1" * 32
PARENT_HASH = "b2" * 32
ANCHOR_MS = 1_700_000_000_000
COINBASE_VALUE_SATS = 50_00000000
SIGNING_SEED = "42" * 32
LEDGER_SEED = "43" * 32
_TOOLS_STATE: dict[str, bool] = {}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def real_tools_available() -> bool:
    """True when the qbit-prism builder and verifier can run here."""
    if "available" in _TOOLS_STATE:
        return _TOOLS_STATE["available"]
    if not os.environ.get("PRISM_TOOL_BIN_DIR"):
        local = _repo_root() / "target" / "release"
        if (local / "qbit-prism-build-audit-bundle").exists():
            os.environ["PRISM_TOOL_BIN_DIR"] = str(local)
    available = True
    for name in ("qbit-prism-build-audit-bundle", "qbit-prism-audit-verify"):
        try:
            completed = subprocess.run(
                prism_tool_command(name) + ["--help"],
                capture_output=True,
                timeout=600,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            available = False
            break
        if completed.returncode != 0:
            available = False
            break
    _TOOLS_STATE["available"] = available
    return available


def share_record(index: int, *, oversized_bytes: int = 0) -> dict[str, object]:
    record: dict[str, object] = {
        "share_seq": index + 1,
        "share_id": "Z" * oversized_bytes if oversized_bytes else f"share-{index + 1}",
        "miner_id": f"miner-{index % 3}",
        "order_key": f"o{index % 3}",
        "p2mr_program_hex": ("%02x" % (index % 3)) * 32,
        "share_difficulty": 1,
        "network_difficulty": 1000,
        "template_height": 10,
        "job_id": "job-1",
        "job_issued_at_ms": ANCHOR_MS - 1000,
        "accepted_at_ms": ANCHOR_MS - 500,
        "ntime": 1_700_000_000,
        "credit_policy": None if index % 2 else "standard",
    }
    return record


def found_block(height: int = 10) -> dict[str, object]:
    return {
        "block_height": height,
        "coinbase_value_sats": COINBASE_VALUE_SATS,
        "network_difficulty": 1000,
        "anchor_job_issued_at_ms": ANCHOR_MS,
    }


class GuardedShareSequence(Sequence):
    """A replayable share window that refuses to be copied into a list.

    ``list(seq)`` consults ``__len__`` for its length hint; the streaming
    encoder only iterates. Indexing is allowed for the segment slices.
    """

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records
        self.iterations = 0

    def __iter__(self):  # type: ignore[no-untyped-def]
        self.iterations += 1
        yield from self._records

    def __len__(self) -> int:
        raise AssertionError("share window must not be materialized")

    def __getitem__(self, index: int | slice) -> Any:
        return self._records[index]


def coordinator_server() -> Any:
    result = make_coordinator()
    server = result[0] if isinstance(result, tuple) else result
    server.signing_seed_hex = SIGNING_SEED
    server.ledger_attestation_signing_seed_hex = LEDGER_SEED
    return server


class FakeLeasePsqlShareLedger(PsqlShareLedger):
    """Scripted psql ledger: every statement is recorded, results are queued."""

    def __init__(self, lease_results: list[dict[str, object]], **kwargs: Any) -> None:
        self.lease_results = list(lease_results)
        self.lease_queries: list[str] = []
        kwargs.setdefault("native_client_mode", "0")
        super().__init__(
            psql_command="psql postgresql://example.invalid/qbit",
            lease_retry_sleep=lambda _seconds: None,
            **kwargs,
        )

    def _make_writer_lease_guard(self, _database_url: str | None) -> Any:
        class FakeGuard:
            held = True

            def try_acquire(self) -> bool:
                return True

            def close(self) -> None:
                self.held = False

        return FakeGuard()

    def _run_json(self, sql: str) -> Any:
        self.lease_queries.append(sql)
        if not self.lease_results:
            raise AssertionError("unexpected extra ledger statement")
        return self.lease_results.pop(0)


def acquired_lease() -> dict[str, object]:
    return {
        "acquired": True,
        "writer_id": "writer-a",
        "writer_epoch": 1,
        "writer_session_token": "session-a",
    }


def persist_result() -> dict[str, object]:
    return {
        "backend": "postgres-psql",
        "share_count": 0,
        "block_count": 1,
        "bundle_count": 1,
        "payout_entry_count": 3,
        "carry_forward_count": 3,
        "onchain_output_count": 0,
    }


def payload_from_sql(sql: str) -> dict[str, Any]:
    match = re.search(r"\$(qbit_prism_json(?:_x)*)\$(.*)\$\1\$::jsonb", sql, re.S)
    if match is None:
        raise AssertionError("persist statement carries no payload literal")
    return json.loads(match.group(2))


class _RealBuildMixin:
    def build_candidate(
        self,
        store: AuditArtifactStore,
        server: Any,
        shares: Sequence[dict[str, object]],
        *,
        block_hash: str = BLOCK_HASH,
        height: int = 10,
        cancellation: Any = None,
    ) -> tuple[Any, CanonicalAuditBundleView]:
        candidate = store.issue_candidate(block_hash=block_hash)
        adopted: list[Path] = []

        def adopt(path: Path, value: os.stat_result) -> None:
            store.adopt_compiler_candidate(candidate, path=path, value=value)
            adopted.append(path)

        parent_fd = store.duplicate_root_directory_fd()
        try:
            view = server.build_audit_bundle(
                shares=shares,
                found_block=found_block(height),
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                witness_merkle_leaves_hex=["ab" * 32],
                canonical_output_path=candidate.path,
                canonical_output_parent_fd=parent_fd,
                canonical_output_adopter=adopt,
                cancellation=cancellation,
            )
        except BaseException:
            store.discard_candidate(candidate)
            raise
        finally:
            os.close(parent_fd)
        assert adopted == [candidate.path]
        return candidate, view

    def verify_candidate(
        self,
        store: AuditArtifactStore,
        candidate: Any,
        view: CanonicalAuditBundleView,
        *,
        height: int = 10,
    ) -> dict[str, Any]:
        verified = store.verify_candidate(
            candidate,
            coinbase_tx_hex=view["signed_coinbase_manifest"]["manifest"]["coinbase_tx_hex"],
            expected_coinbase_value_sats=COINBASE_VALUE_SATS,
            expected_block_height=height,
            trusted_writer_public_key_hex=AuditArtifactStore.trusted_writer_key(
                None,
                view,
                allow_embedded_test_key=True,
            ),
            trust_source="embedded_test_only",
        )
        store.require_current_verified_candidate(verified, candidate)
        assert verified.canonical_copy_eligible
        return dict(verified.report)


@unittest.skipUnless(real_tools_available(), "qbit-prism builder and verifier are unavailable")
class RealBuilderBoundedFinalizationTests(_RealBuildMixin, unittest.TestCase):
    def make_store(self, root: Path, *, share_segment_size: int) -> tuple[AuditArtifactStore, mock.Mock]:
        canonicalizer = mock.Mock(
            side_effect=AssertionError("bounded publication must not canonicalize a bundle"),
        )
        store = AuditArtifactStore(
            AuditArtifactConfig(
                root=root,
                evidence_path=root / "evidence.json",
                share_segment_size=share_segment_size,
            ),
            canonicalizer=canonicalizer,
        )
        self.addCleanup(store.close)
        return store, canonicalizer

    def test_canonical_build_returns_bounded_view_from_a_streamed_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, _canonicalizer = self.make_store(root, share_segment_size=100)
            server = coordinator_server()
            records = [share_record(index) for index in range(1300)]
            window = GuardedShareSequence(records)
            candidate, view = self.build_candidate(store, server, window)
            try:
                self.assertIsInstance(view, CanonicalAuditBundleView)
                self.assertGreaterEqual(window.iterations, 1)
                raw = candidate.path.read_bytes()
                self.assertEqual(view.sha256_hex, hashlib.sha256(raw).hexdigest())
                self.assertEqual(view.byte_length, len(raw))
                loaded = json.loads(raw)
                self.assertEqual(view, loaded)
                self.assertEqual(loaded, view)
                self.assertIsInstance(view["shares"], LazyRecordSequence)
                self.assertEqual(len(view["shares"]), 1300)
                self.assertIsInstance(view["reward_manifest"]["shares"], LazyRecordSequence)
                self.assertEqual(view["shares"][0]["share_seq"], 1)
                self.assertEqual(view["shares"][-1]["share_seq"], 1300)
                # The typed Rust order is preserved; the sorted v1 identity
                # encoding is a different contract and is not used here.
                self.assertEqual(list(view)[:3], ["schema", "shares", "found_block"])
                report = self.verify_candidate(store, candidate, view)
                self.assertEqual(report["audit_bundle_sha256_hex"], view.sha256_hex)
                self.assertEqual(
                    AuditArtifactStore.trusted_writer_key(None, view, allow_embedded_test_key=True),
                    AuditArtifactStore.trusted_writer_key(None, loaded, allow_embedded_test_key=True),
                )
                self.assertEqual(
                    view["payout_policy_manifest"]["accounts"],
                    loaded["payout_policy_manifest"]["accounts"],
                )
            finally:
                view.close()
                store.discard_candidate(candidate)

    def test_bounded_publication_preserves_bytes_digests_and_compact_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, canonicalizer = self.make_store(root, share_segment_size=250)
            server = coordinator_server()
            records = [share_record(index) for index in range(3000)]
            candidate, view = self.build_candidate(store, server, GuardedShareSequence(records))
            try:
                raw = candidate.path.read_bytes()
                self.assertGreater(len(raw), 2 * SCAN_CHUNK_BYTES)
                report = self.verify_candidate(store, candidate, view)
                expected = report["audit_bundle_sha256_hex"]
                body_uri = str(store.body_path(BLOCK_HASH, expected))
                payload = {"block_hash": BLOCK_HASH, "audit_bundle_sha256": expected}
                real_loads = json.loads
                real_dumps = json.dumps

                def bounded_loads(text: Any, *args: Any, **kwargs: Any) -> Any:
                    if len(text) > SCAN_CHUNK_BYTES:
                        raise AssertionError("publication decoded a window-sized document")
                    return real_loads(text, *args, **kwargs)

                def bounded_dumps(value: Any, *args: Any, **kwargs: Any) -> str:
                    encoded = real_dumps(value, *args, **kwargs)
                    if len(encoded) > SCAN_CHUNK_BYTES:
                        raise AssertionError("publication encoded a window-sized document")
                    return encoded

                with mock.patch("lab.prism.audit_artifacts.json.loads", side_effect=bounded_loads), mock.patch(
                    "lab.prism.audit_artifacts.json.dumps",
                    side_effect=bounded_dumps,
                ):
                    published = store.prepare_external_audit_body(
                        payload,
                        view,
                        body_uri=body_uri,
                        canonical_bundle_path=candidate.path,
                    )
                self.assertEqual(published, body_uri)
                canonicalizer.assert_not_called()
                body_path = Path(body_uri)
                body = json.loads(body_path.read_bytes())
                self.assertEqual(body["schema"], "qbit.prism.audit-bundle.v2")
                self.assertEqual(body["share_count"], 3000)
                parts = body["share_window_proof"]["share_parts"]
                self.assertEqual(len(parts), 12)
                self.assertTrue(all(part["kind"] == "segment_range" for part in parts))
                # Exact canonical bytes and their reproducible compression.
                self.assertEqual(store.read_canonical_audit_bundle(BLOCK_HASH, expected), raw)
                self.assertEqual(store.stored_canonical_bundle_sha256(BLOCK_HASH, expected), expected)
                gz_path = store.canonical_bundle_path(BLOCK_HASH, expected)
                self.assertEqual(gz_path.read_bytes(), gzip_canonical_bundle_bytes(raw))
                self.assertEqual(gzip.decompress(gz_path.read_bytes()), raw)
                # The compact body reconstructs to the exact logical bundle and
                # the reconstruction's Rust canonical digest is the advertised
                # one (the reader canonicalizes; the publication never did).
                canonicalizer.side_effect = None
                canonicalizer.return_value = None
                from lab.prism.bundle_compiler import canonical_bundle_bytes

                canonicalizer.side_effect = canonical_bundle_bytes
                reconstructed = store.read_external_body(body_uri, expected_sha256=expected)
                self.assertEqual(reconstructed, json.loads(raw))
                self.assertEqual(reconstructed, view)
                canonicalizer.side_effect = AssertionError("retry must not canonicalize")
                self.assertEqual(
                    store.audit_body_byte_len(body_uri, view, candidate.path),
                    body_path.stat().st_size,
                )
                metrics = store.metrics_snapshot()
                self.assertEqual(metrics["canonical_bundle"]["files"], 1)
                self.assertEqual(metrics["body"]["files"], 1)
                self.assertEqual(metrics["share_segment"]["files"], 12)
                # An exact retry after the body landed is a durable no-op that
                # neither rewrites nor reconstructs.
                before = {
                    path: (path.stat().st_ino, path.stat().st_mtime_ns)
                    for path in root.iterdir()
                    if path.is_file()
                }
                with mock.patch.object(
                    store,
                    "resolve_audit_bundle_v2",
                    side_effect=AssertionError("retry must not reconstruct the window"),
                ), mock.patch.object(
                    store,
                    "read_audit_share_segment",
                    side_effect=AssertionError("retry must not load a segment whole"),
                ):
                    retried = store.prepare_external_audit_body(
                        payload,
                        view,
                        body_uri=body_uri,
                        canonical_bundle_path=candidate.path,
                    )
                self.assertEqual(retried, body_uri)
                after = {
                    path: (path.stat().st_ino, path.stat().st_mtime_ns)
                    for path in root.iterdir()
                    if path.is_file()
                }
                self.assertEqual(before, after)
                # The candidate artifact itself is a dot-prefixed .tmp name;
                # no writer temporaries may remain beside it.
                self.assertEqual(
                    [path for path in root.glob(".*.tmp") if "candidate" not in path.name],
                    [],
                )
            finally:
                view.close()
                store.discard_candidate(candidate)

    def test_view_without_a_canonical_path_publishes_from_its_own_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, canonicalizer = self.make_store(root, share_segment_size=0)
            server = coordinator_server()
            candidate, view = self.build_candidate(store, server, [share_record(i) for i in range(7)])
            try:
                raw = candidate.path.read_bytes()
                expected = view.sha256_hex
                body_uri = str(store.body_path(BLOCK_HASH, expected))
                published = store.prepare_external_audit_body(
                    {"block_hash": BLOCK_HASH, "audit_bundle_sha256": expected},
                    view,
                    body_uri=body_uri,
                )
                self.assertEqual(published, body_uri)
                # No segmenting configured: the body is the literal canonical
                # bundle, copied from the artifact source.
                self.assertEqual(Path(body_uri).read_bytes(), raw)
                self.assertEqual(store.read_canonical_audit_bundle(BLOCK_HASH, expected), raw)
                canonicalizer.assert_not_called()
                self.assertEqual(store.audit_body_byte_len(None, view), len(raw))
            finally:
                view.close()
                store.discard_candidate(candidate)

    def test_tampered_or_foreign_artifacts_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, _canonicalizer = self.make_store(root, share_segment_size=4)
            server = coordinator_server()
            candidate, view = self.build_candidate(store, server, [share_record(i) for i in range(9)])
            other, other_view = self.build_candidate(
                store,
                server,
                [share_record(i) for i in range(10)],
                block_hash=PARENT_HASH,
            )
            try:
                raw = candidate.path.read_bytes()
                expected = view.sha256_hex
                payload = {"block_hash": BLOCK_HASH, "audit_bundle_sha256": expected}
                body_uri = str(store.body_path(BLOCK_HASH, expected))
                # A view over a different artifact than the canonical path.
                with self.assertRaisesRegex(RuntimeError, "does not match logical bundle"):
                    store.prepare_external_audit_body(
                        payload,
                        other_view,
                        body_uri=body_uri,
                        canonical_bundle_path=candidate.path,
                    )
                # A dictionary that differs in one record.
                tampered = json.loads(raw)
                tampered["shares"][4]["share_difficulty"] = 2
                with self.assertRaisesRegex(RuntimeError, "does not match logical bundle"):
                    store.validate_canonical_source(candidate.path, expected, tampered)
                store.validate_canonical_source(candidate.path, expected, json.loads(raw))
                store.validate_canonical_source(candidate.path, expected, view)
                # The artifact rewritten under the view after the scan.
                candidate.path.write_bytes(raw + b" ")
                with self.assertRaisesRegex(RuntimeError, "sha256 mismatch"):
                    store.prepare_external_audit_body(
                        payload,
                        view,
                        body_uri=body_uri,
                        canonical_bundle_path=candidate.path,
                    )
                with self.assertRaises(Exception):
                    store.prepare_external_audit_body(payload, view, body_uri=body_uri)
                self.assertEqual(list(root.glob("prism-audit-bundle-body-*")), [])
            finally:
                view.close()
                other_view.close()
                store.discard_candidate(candidate)
                store.discard_candidate(other)

    def test_oversized_single_field_publishes_through_bounded_writers(self) -> None:
        oversized = 2 * SCAN_CHUNK_BYTES + 3
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, canonicalizer = self.make_store(root, share_segment_size=3)
            server = coordinator_server()
            records = [
                share_record(index, oversized_bytes=oversized if index == 4 else 0)
                for index in range(11)
            ]
            candidate, view = self.build_candidate(store, server, records)
            try:
                raw = candidate.path.read_bytes()
                self.assertGreater(len(raw), 2 * oversized)
                report = self.verify_candidate(store, candidate, view)
                expected = report["audit_bundle_sha256_hex"]
                body_uri = str(store.body_path(BLOCK_HASH, expected))
                published = store.prepare_external_audit_body(
                    {"block_hash": BLOCK_HASH, "audit_bundle_sha256": expected},
                    view,
                    body_uri=body_uri,
                    canonical_bundle_path=candidate.path,
                )
                self.assertEqual(published, body_uri)
                canonicalizer.assert_not_called()
                self.assertEqual(store.read_canonical_audit_bundle(BLOCK_HASH, expected), raw)
                from lab.prism.bundle_compiler import canonical_bundle_bytes

                canonicalizer.side_effect = canonical_bundle_bytes
                self.assertEqual(
                    store.read_external_body(body_uri, expected_sha256=expected),
                    json.loads(raw),
                )
                segments = sorted(root.glob("prism-audit-share-segment-slot-*.json"))
                self.assertEqual(len(segments), 4)
                self.assertTrue(any(path.stat().st_size > oversized for path in segments))
            finally:
                view.close()
                store.discard_candidate(candidate)

    def test_multiple_generations_publish_and_release_independently(self) -> None:
        gc.disable()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                store, _canonicalizer = self.make_store(root, share_segment_size=20)
                server = coordinator_server()
                digests: list[str] = []
                refs: list[weakref.ref] = []
                for generation in range(3):
                    block_hash = ("%02x" % (0xC0 + generation)) * 32
                    records = [share_record(index) for index in range(40 + generation)]
                    candidate, view = self.build_candidate(
                        store,
                        server,
                        GuardedShareSequence(records),
                        block_hash=block_hash,
                        height=10 + generation,
                    )
                    try:
                        report = self.verify_candidate(store, candidate, view, height=10 + generation)
                        expected = report["audit_bundle_sha256_hex"]
                        body_uri = str(store.body_path(block_hash, expected))
                        self.assertEqual(
                            store.prepare_external_audit_body(
                                {"block_hash": block_hash, "audit_bundle_sha256": expected},
                                view,
                                body_uri=body_uri,
                                canonical_bundle_path=candidate.path,
                            ),
                            body_uri,
                        )
                        digests.append(expected)
                        refs.append(weakref.ref(view))
                        source_fd = view.source.fileno()
                    finally:
                        view.close()
                        store.discard_candidate(candidate)
                    with self.assertRaises(OSError):
                        os.fstat(source_fd)
                    del view
                    self.assertIsNone(refs[-1]())
                self.assertEqual(len(set(digests)), 3)
                metrics = store.metrics_snapshot()
                self.assertEqual(metrics["body"]["files"], 3)
                self.assertEqual(metrics["canonical_bundle"]["files"], 3)
                self.assertEqual(metrics["candidate"]["files"], 0)
        finally:
            gc.enable()

    def test_cancellation_during_output_scan_cleans_the_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, _canonicalizer = self.make_store(root, share_segment_size=0)
            server = coordinator_server()

            class Cancelled(RuntimeError):
                pass

            class Cancellation:
                def __init__(self) -> None:
                    self.scan_checks = 0

                def is_set(self) -> bool:
                    return False

                def raise_if_cancelled(self, phase: str) -> None:
                    if phase == "builder output scan":
                        self.scan_checks += 1
                        if self.scan_checks == 2:
                            raise Cancelled(phase)

            cancellation = Cancellation()
            records = [share_record(index) for index in range(1500)]
            with self.assertRaises(Cancelled):
                self.build_candidate(store, server, records, cancellation=cancellation)
            self.assertEqual(cancellation.scan_checks, 2)
            self.assertEqual(list(root.glob(".prism-live-audit-bundle-candidate-*")), [])
            self.assertEqual(store.metrics_snapshot()["candidate"]["files"], 0)

    def test_accepted_block_persistence_consumes_the_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "audit"
            root.mkdir()
            canonicalizer = mock.Mock(side_effect=AssertionError("persistence must not canonicalize"))
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {"existing_block": False, "existing_body_uri": None},
                    persist_result(),
                ],
                audit_body_dir=root,
                audit_bundle_canonicalizer=canonicalizer,
                audit_share_segment_size=50,
            )
            store = ledger._audit_artifact_store
            assert store is not None
            server = coordinator_server()
            candidate, view = self.build_candidate(store, server, GuardedShareSequence(
                [share_record(index) for index in range(120)]
            ))
            try:
                raw = candidate.path.read_bytes()
                report = self.verify_candidate(store, candidate, view)
                persistence = ledger.persist_accepted_block(
                    block_hash=BLOCK_HASH,
                    block_height=10,
                    parent_hash=PARENT_HASH,
                    final_bundle=view,
                    audit_report=report,
                    canonical_bundle_path=candidate.path,
                )
                canonicalizer.assert_not_called()
                body_uri = str(store.body_path(BLOCK_HASH, report["audit_bundle_sha256_hex"]))
                self.assertEqual(persistence["body_uri"], body_uri)
                self.assertEqual(persistence["audit_body_byte_len"], Path(body_uri).stat().st_size)
                payload = payload_from_sql(ledger.lease_queries[-1])
                loaded = json.loads(raw)
                self.assertIsNone(payload["audit_bundle"])
                self.assertEqual(payload["body_uri"], body_uri)
                self.assertEqual(payload["accounts"], loaded["payout_policy_manifest"]["accounts"])
                self.assertEqual(payload["witness_merkle_leaves_hex"], loaded["witness_merkle_leaves_hex"])
                self.assertEqual(
                    payload["audit_commitment_leaves_hex"],
                    loaded["audit_commitment_leaves_hex"],
                )
                self.assertEqual(payload["found_block_network_difficulty"], 1000)
                self.assertEqual(payload["schema_version"], loaded["schema"])
                self.assertEqual(store.read_canonical_audit_bundle(BLOCK_HASH, view.sha256_hex), raw)
            finally:
                view.close()
                store.discard_candidate(candidate)

    def test_inline_persistence_lane_splices_canonical_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, _canonicalizer = self.make_store(root, share_segment_size=0)
            server = coordinator_server()
            candidate, view = self.build_candidate(store, server, [share_record(i) for i in range(5)])
            try:
                raw = candidate.path.read_bytes()
                report = self.verify_candidate(store, candidate, view)
                ledger = FakeLeasePsqlShareLedger([acquired_lease(), persist_result()])
                persistence = ledger.persist_accepted_block(
                    block_hash=BLOCK_HASH,
                    block_height=10,
                    parent_hash=PARENT_HASH,
                    final_bundle=view,
                    audit_report=report,
                )
                self.assertEqual(persistence["body_uri"], "")
                self.assertEqual(persistence["audit_body_byte_len"], len(raw))
                payload = payload_from_sql(ledger.lease_queries[-1])
                self.assertEqual(payload["audit_bundle"], json.loads(raw))
                self.assertIsNone(payload["body_uri"])
                self.assertIn(
                    json.dumps(json.loads(raw), separators=(",", ":"))[:64],
                    ledger.lease_queries[-1],
                )
            finally:
                view.close()
                store.discard_candidate(candidate)


class CompilerInputStreamingTests(unittest.TestCase):
    def test_build_input_chunks_match_json_dumps_for_lists_and_sequences(self) -> None:
        records = [share_record(index) for index in range(700)]
        payload = {
            "found_block": found_block(),
            "prior_balances": [],
            "payout_policy": {"p2mr_spend_input_bytes": 1},
            "coinbase_script_sig_suffix_hex": "00",
            "witness_merkle_leaves_hex": ["ab" * 32],
            "shares": records,
            "ctv_settlement": None,
        }
        expected = json.dumps(payload, separators=(",", ":"))
        self.assertEqual(
            "".join(iter_json_object_text_chunks(payload, array_keys=("shares",))),
            expected,
        )
        for shares in (records, tuple(records), GuardedShareSequence(records)):
            with self.subTest(kind=type(shares).__name__):
                chunks = list(
                    _iter_build_input_chunks(
                        {**payload, "shares": shares},
                        array_keys=("shares", "compact_share_identities", "compact_shares"),
                        batch_records=64,
                        chunk_chars=4096,
                    )
                )
                self.assertEqual("".join(chunks), expected)
                self.assertTrue(all(chunk.isascii() for chunk in chunks))
                self.assertGreater(len(chunks), 1)

    def test_compiler_streams_a_sequence_without_copying_it(self) -> None:
        server = coordinator_server()
        captured: dict[str, object] = {}
        records = [share_record(index) for index in range(300)]
        window = GuardedShareSequence(records)
        canonical = json.dumps({"ok": True, "shares": records[:2]}, separators=(",", ":"))
        with tempfile.TemporaryDirectory() as tmp, mock.patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            fake_audit_bundle_popen(captured, output_text=canonical),
        ):
            output_path = Path(tmp) / "candidate.audit.json"
            bundle = server.build_audit_bundle(
                shares=window,
                found_block=found_block(),
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                canonical_output_path=output_path,
            )
            try:
                self.assertIsInstance(bundle, CanonicalAuditBundleView)
                self.assertEqual(captured["payload"]["shares"], records)
                self.assertEqual(window.iterations, 1)
                self.assertEqual(bundle, json.loads(canonical))
                self.assertIsInstance(bundle["shares"], LazyRecordSequence)
                self.assertEqual(output_path.read_bytes(), canonical.encode())
            finally:
                bundle.close()

    def test_compiler_rejects_non_sequence_windows_before_writing(self) -> None:
        server = coordinator_server()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TypeError):
                server.build_audit_bundle(
                    shares="not-a-window",  # type: ignore[arg-type]
                    found_block=found_block(),
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                    canonical_output_path=Path(tmp) / "candidate.audit.json",
                )


if __name__ == "__main__":
    unittest.main()

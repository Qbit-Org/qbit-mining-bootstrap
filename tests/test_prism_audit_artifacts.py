#!/usr/bin/env python3

import hashlib
import json
import os
from dataclasses import replace as dataclass_replace
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from lab.prism.audit_artifacts import (
    AuditArtifactConfig,
    AuditArtifactStore,
    AuditPublicationIdentity,
    LIVE_ENVELOPE_SCHEMA,
    LIVE_EVIDENCE_SCHEMA,
    OwnedCandidateArtifact,
    _FileIdentity,
    canonical_audit_bundle_bytes,
)
from lab.prism.bundle_compiler import canonical_bundle_bytes
from lab.prism.share_ledger import SingleWriterShareLedger


BLOCK_A = "aa" * 32
BLOCK_B = "bb" * 32
DIGEST = "11" * 32


class _TestAuditArtifactStore(AuditArtifactStore):
    """Keep publication fixtures terse while supplying a real verifier identity."""

    def publish_success(self, **kwargs: object):  # type: ignore[no-untyped-def]
        report = kwargs.get("report")
        identity = kwargs.get("identity")
        if "publication_floor_sequence" not in kwargs:
            if not isinstance(identity, AuditPublicationIdentity):
                raise AssertionError("publication test identity is required")
            kwargs["publication_floor_sequence"] = identity.sequence
        if "verification_identity" not in kwargs:
            if not isinstance(report, dict):
                raise AssertionError("publication test report is required")
            kwargs["verification_identity"] = self.build_verification_identity(
                trust_source="configured",
                trusted_writer_public_key_hex="44" * 32,
                literal_sha256=DIGEST,
                literal_byte_len=123,
                report=report,
            )
        with self.publication_order_guard():
            return super().publish_success(**kwargs)  # type: ignore[arg-type]


class AuditArtifactStoreTest(unittest.TestCase):
    def make_store(self, root: Path, **kwargs: object) -> AuditArtifactStore:
        evidence_path = Path(kwargs.pop("evidence_path", root / "evidence.json"))
        return _TestAuditArtifactStore(
            AuditArtifactConfig(
                root=root,
                evidence_path=evidence_path,
                live_bundle_retention=int(kwargs.pop("live_bundle_retention", 5)),
                candidate_retention_seconds=int(
                    kwargs.pop("candidate_retention_seconds", 86_400)
                ),
                share_segment_size=int(kwargs.pop("share_segment_size", 0)),
                verifier_timeout_seconds=float(
                    kwargs.pop("verifier_timeout_seconds", 60.0)
                ),
            ),
            **kwargs,
        )

    @staticmethod
    def transfer_candidate(
        store: AuditArtifactStore,
        candidate: OwnedCandidateArtifact,
    ) -> None:
        path = candidate.path
        with path.open("rb") as handle:
            store.adopt_compiler_candidate(
                candidate,
                path=path,
                value=os.fstat(handle.fileno()),
            )

    @staticmethod
    def report(
        digest: str = DIGEST,
        *,
        block_height: int = 1,
    ) -> dict[str, object]:
        return {
            "schema": "qbit.prism.audit-verification-report.v1",
            "block_height": block_height,
            "audit_bundle_sha256_hex": digest,
            "reward_manifest_sha256_hex": "44" * 32,
            "payout_policy_manifest_sha256_hex": "55" * 32,
            "prism_audit_commitment_leaf_hex": "66" * 32,
            "audit_commitment_root_hex": "77" * 32,
            "coinbase_txid": "22" * 32,
            "coinbase_wtxid": "88" * 32,
            "coinbase_manifest_sha256_hex": "33" * 32,
            "coinbase_tx_hex": "00",
            "coinbase_value_sats": 1,
            "min_output_sats": 1,
            "onchain_output_count": 0,
            "accrued_account_count": 0,
        }

    @staticmethod
    def persistence(digest: str = DIGEST) -> dict[str, object]:
        return {"audit_bundle_sha256": digest, "body_uri": ""}

    def test_paths_reject_untrusted_hashes_and_stay_in_resolved_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "audit"
            store = self.make_store(root)
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            self.assertEqual(candidate.path.parent, root.resolve())
            self.assertEqual(
                store.live_envelope_path(block_height=0, block_hash=BLOCK_A).parent,
                root.resolve(),
            )
            for invalid in ("a", "gg" * 32, "../" + BLOCK_A, BLOCK_A + "/x"):
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    store.issue_candidate(block_hash=invalid)

    def test_publication_identity_and_live_path_reject_lossy_types(self) -> None:
        invalid_identities = (
            (True, 1, BLOCK_A),
            ("1", 1, BLOCK_A),
            (-1, 1, BLOCK_A),
            (1, False, BLOCK_A),
            (1, "1", BLOCK_A),
            (1, -1, BLOCK_A),
            (1, 1, BLOCK_A.upper()),
        )
        for sequence, height, block_hash in invalid_identities:
            with self.subTest(
                sequence=sequence,
                height=height,
                block_hash=block_hash,
            ), self.assertRaises(ValueError):
                AuditPublicationIdentity(sequence, height, block_hash)  # type: ignore[arg-type]
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            for height in (True, "1"):
                with self.subTest(path_height=height), self.assertRaises(ValueError):
                    store.live_envelope_path(  # type: ignore[arg-type]
                        block_height=height,
                        block_hash=BLOCK_A,
                    )
            identity = AuditPublicationIdentity(1, 1, BLOCK_A)
            for floor in (True, "1", -1):
                with self.subTest(publication_floor=floor), self.assertRaises(
                    ValueError
                ):
                    store.publish_success(
                        identity=identity,
                        publication_floor_sequence=floor,  # type: ignore[arg-type]
                        report=self.report(),
                        persistence=self.persistence(),
                        evidence={},
                        created_at="now",
                    )
            with self.assertRaisesRegex(RuntimeError, "exceeds"):
                store.publish_success(
                    identity=identity,
                    publication_floor_sequence=0,
                    report=self.report(),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="now",
                )
            with self.assertRaisesRegex(RuntimeError, "behind"):
                store.publish_success(
                    identity=identity,
                    publication_floor_sequence=2,
                    report=self.report(),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="now",
                )

    def test_live_publication_canonicalizes_persistence_digest_for_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            publication = store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(DIGEST.upper()),
                evidence={},
                created_at="now",
            )
            self.assertEqual(
                publication.evidence["persistence"]["audit_bundle_sha256"],  # type: ignore[index]
                DIGEST,
            )
            store.close()
            restarted = self.make_store(root)
            latest = restarted.latest_evidence()
            self.assertIsNotNone(latest)
            assert latest is not None
            self.assertEqual(latest["persistence"]["audit_bundle_sha256"], DIGEST)

    def test_root_final_symlink_is_rejected_and_ancestor_is_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real = base / "real"
            real.mkdir()
            ancestor = base / "ancestor"
            ancestor.symlink_to(real, target_is_directory=True)
            store = self.make_store(ancestor / "audit")
            self.assertEqual(store.root, (real / "audit").resolve())
            target = base / "target"
            target.mkdir()
            final_link = base / "final-link"
            final_link.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(RuntimeError, "non-symlink"):
                self.make_store(final_link)

    def test_root_directory_authority_swap_matrix_fails_closed_and_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "audit"
            evidence_path = base / "state" / "evidence.json"
            store = self.make_store(root, evidence_path=evidence_path)
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            candidate = store.issue_candidate(block_hash=BLOCK_B)
            store.write_compatibility_candidate(candidate, {"candidate": True})
            body = store.body_path(BLOCK_A, DIGEST)
            store._write_immutable_bytes(body, b"body")

            pinned_root = base / "audit-pinned"
            root.rename(pinned_root)
            root.mkdir()
            sentinel = root / "operator-sentinel"
            sentinel.write_bytes(b"operator")

            self.assertIsNone(store.latest_evidence())
            self.assertEqual(store.publication_sequence_floor(), 0)
            self.assertEqual(store.metrics_snapshot()["scan_error"], 1)
            self.assertEqual(store.prune_best_effort().errors, 1)
            with self.assertRaisesRegex(RuntimeError, "root identity"):
                store._write_immutable_bytes(body, b"replacement")
            with self.assertRaisesRegex(RuntimeError, "root identity"):
                store._read_owned_regular_bytes(body)
            with self.assertRaisesRegex(RuntimeError, "root identity"):
                store.discard_candidate(candidate)
            with self.assertRaisesRegex(RuntimeError, "root identity"):
                store.publish_success(
                    identity=AuditPublicationIdentity(2, 2, BLOCK_B),
                    report=self.report(block_height=2),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="blocked",
                )
            self.assertEqual(sentinel.read_bytes(), b"operator")
            self.assertEqual(sorted(path.name for path in root.iterdir()), [sentinel.name])

            replacement = base / "audit-replacement"
            root.rename(replacement)
            pinned_root.rename(root)
            self.assertEqual(store.publication_sequence_floor(), 1)
            self.assertEqual(store.latest_evidence()["block_hash"], BLOCK_A)  # type: ignore[index]
            self.assertEqual(store._read_owned_regular_bytes(body)[0], b"body")
            store.discard_candidate(candidate)
            self.assertFalse(candidate.path.exists())
            self.assertEqual((replacement / sentinel.name).read_bytes(), b"operator")

    def test_external_evidence_parent_swap_rolls_back_publication_and_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "audit"
            state = base / "state"
            evidence_path = state / "evidence.json"
            store = self.make_store(root, evidence_path=evidence_path)
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            original_write = store._write_mutable_json
            pinned_state = base / "state-pinned"
            replacement_state = base / "state-replacement"

            def swap_before_evidence(
                path: Path,
                *args: object,
                **kwargs: object,
            ) -> object:
                if path == store.evidence_path:
                    state.rename(pinned_state)
                    state.mkdir()
                    (state / "operator-sentinel").write_bytes(b"operator")
                return original_write(path, *args, **kwargs)

            new_envelope = store.live_envelope_path(
                block_height=2,
                block_hash=BLOCK_B,
            )
            with mock.patch.object(
                store,
                "_write_mutable_json",
                side_effect=swap_before_evidence,
            ), self.assertRaisesRegex(RuntimeError, "evidence parent identity"):
                store.publish_success(
                    identity=AuditPublicationIdentity(2, 2, BLOCK_B),
                    report=self.report(block_height=2),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="blocked",
                )
            self.assertFalse(new_envelope.exists())
            self.assertEqual(
                (state / "operator-sentinel").read_bytes(),
                b"operator",
            )
            self.assertIsNone(store.latest_evidence())
            self.assertEqual(store.publication_sequence_floor(), 0)

            state.rename(replacement_state)
            pinned_state.rename(state)
            self.assertEqual(store.publication_sequence_floor(), 1)
            self.assertEqual(store.latest_evidence()["block_hash"], BLOCK_A)  # type: ignore[index]

    def test_live_prune_rechecks_evidence_authority_before_removal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "audit"
            state = base / "state"
            pinned_state = base / "state-pinned"
            store = self.make_store(
                root,
                evidence_path=state / "evidence.json",
                live_bundle_retention=0,
            )
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            stale = store.live_envelope_path(block_height=2, block_hash=BLOCK_B)
            stale.write_text("stale", encoding="utf-8")
            real_unlink = store._unlink_scanned_owned
            swapped = False

            def swap_after_scan(
                path: Path,
                identity: _FileIdentity,
                *,
                require_all_authorities: bool = False,
            ) -> bool:
                nonlocal swapped
                if require_all_authorities and not swapped:
                    swapped = True
                    state.rename(pinned_state)
                    state.mkdir()
                    (state / "operator-sentinel").write_bytes(b"operator")
                return real_unlink(
                    path,
                    identity,
                    require_all_authorities=require_all_authorities,
                )

            with mock.patch.object(
                store,
                "_unlink_scanned_owned",
                side_effect=swap_after_scan,
            ):
                result = store.prune_best_effort()
            self.assertTrue(swapped)
            self.assertEqual(result.live_removed, 0)
            self.assertGreaterEqual(result.errors, 1)
            self.assertTrue(stale.exists())
            self.assertEqual(
                (state / "operator-sentinel").read_bytes(),
                b"operator",
            )

    def test_candidate_prune_rechecks_reservations_after_active_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(
                Path(tmp),
                candidate_retention_seconds=0,
            )
            scan_waiting = threading.Event()
            release_scan = threading.Event()
            result: list[object] = []
            errors: list[BaseException] = []
            real_listdir = os.listdir

            def pause_before_scan(fd: int) -> list[str]:
                if fd == store._root_fd:
                    scan_waiting.set()
                    if not release_scan.wait(5):
                        raise AssertionError("timed out waiting to release prune scan")
                return real_listdir(fd)

            def prune() -> None:
                try:
                    result.append(store.prune_best_effort())
                except BaseException as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            with mock.patch(
                "lab.prism.audit_artifacts.os.listdir",
                side_effect=pause_before_scan,
            ):
                thread = threading.Thread(target=prune)
                thread.start()
                self.assertTrue(scan_waiting.wait(5))
                candidate = store.issue_candidate(block_hash=BLOCK_A)
                store.write_compatibility_candidate(candidate, {"reserved": True})
                expected = candidate.path.read_bytes()
                release_scan.set()
                thread.join(timeout=5)

            self.assertFalse(errors)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].candidate_removed, 0)  # type: ignore[union-attr]
            self.assertEqual(candidate.path.read_bytes(), expected)
            store.discard_candidate(candidate)

    def test_observer_scans_fail_closed_on_midscan_authority_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "audit"
            store = self.make_store(
                root,
                candidate_retention_seconds=0,
                live_bundle_retention=0,
            )
            candidate = root / f"prism-live-audit-bundle-candidate-{BLOCK_A}.json"
            candidate.write_bytes(b"candidate")
            real_lstat = store._owned_lstat

            def run_with_swap(operation: object, label: str) -> object:
                pinned = base / f"{label}-pinned"
                replacement = base / f"{label}-replacement"
                swapped = False

                def swap_then_reject(path: Path) -> os.stat_result:
                    nonlocal swapped
                    if not swapped:
                        swapped = True
                        root.rename(pinned)
                        root.mkdir()
                        (root / "operator-sentinel").write_bytes(b"operator")
                        raise RuntimeError("authority changed")
                    return real_lstat(path)

                with mock.patch.object(
                    store,
                    "_owned_lstat",
                    side_effect=swap_then_reject,
                ):
                    result = operation()  # type: ignore[operator]
                self.assertTrue(swapped)
                self.assertEqual(
                    (root / "operator-sentinel").read_bytes(),
                    b"operator",
                )
                root.rename(replacement)
                pinned.rename(root)
                return result

            metrics = run_with_swap(store.metrics_snapshot, "metrics")
            self.assertEqual(metrics["scan_error"], 1)  # type: ignore[index]
            self.assertTrue(candidate.exists())
            retained = run_with_swap(store.prune_best_effort, "prune")
            self.assertGreaterEqual(retained.errors, 1)  # type: ignore[union-attr]
            self.assertEqual(retained.candidate_removed, 0)  # type: ignore[union-attr]
            self.assertTrue(candidate.exists())

    def test_directory_authority_reconfigure_and_close_are_atomic_and_fd_clean(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            old_root = base / "old-root"
            old_evidence = base / "old-state" / "evidence.json"
            store = self.make_store(old_root, evidence_path=old_evidence)
            old_root_fd = store._root_fd
            old_publication_lock_fd = store._publication_lock_fd
            old_evidence_fd = store._evidence_parent_fd
            real_open = store._open_directory_authority
            calls = 0
            prepared_root_fd: int | None = None

            def fail_second(path: Path) -> tuple[int, tuple[int, int]]:
                nonlocal calls, prepared_root_fd
                calls += 1
                if calls == 2:
                    raise OSError("second authority")
                result = real_open(path)
                prepared_root_fd = result[0]
                return result

            new_root = base / "new-root"
            new_evidence = base / "new-state" / "evidence.json"
            with mock.patch.object(
                store,
                "_open_directory_authority",
                side_effect=fail_second,
            ), self.assertRaisesRegex(OSError, "second authority"):
                store.reconfigure(root=new_root, evidence_path=new_evidence)
            self.assertEqual(store.root, old_root.resolve())
            self.assertEqual(store.evidence_path, old_evidence.resolve())
            os.fstat(old_root_fd)
            os.fstat(old_evidence_fd)
            self.assertIsNotNone(prepared_root_fd)
            assert prepared_root_fd is not None
            with self.assertRaises(OSError):
                os.fstat(prepared_root_fd)

            real_os_close = os.close
            old_fds = {
                old_root_fd,
                old_publication_lock_fd,
                old_evidence_fd,
            }

            def close_then_report_error(fd: int) -> None:
                real_os_close(fd)
                if fd in old_fds:
                    raise OSError("close status unavailable")

            with mock.patch(
                "lab.prism.audit_artifacts.os.close",
                side_effect=close_then_report_error,
            ):
                store.reconfigure(root=new_root, evidence_path=new_evidence)
            new_root_fd = store._root_fd
            new_publication_lock_fd = store._publication_lock_fd
            new_evidence_fd = store._evidence_parent_fd
            for closed_fd in (
                old_root_fd,
                old_publication_lock_fd,
                old_evidence_fd,
            ):
                with self.assertRaises(OSError):
                    os.fstat(closed_fd)
            store.close()
            store.close()
            self.assertIsNone(store.latest_evidence())
            self.assertEqual(store.publication_sequence_floor(), 0)
            metrics = store.metrics_snapshot()
            self.assertEqual(metrics["scan_error"], 1)
            for kind in (
                "body",
                "share_segment",
                "live_bundle",
                "candidate",
                "other",
            ):
                self.assertEqual(metrics[kind], {"files": 0, "bytes": 0})
            retention = store.prune_best_effort()
            self.assertEqual(retention.live_removed, 0)
            self.assertEqual(retention.candidate_removed, 0)
            self.assertEqual(retention.errors, 1)
            with self.assertRaisesRegex(RuntimeError, "closed"):
                store.issue_candidate(block_hash=BLOCK_A)
            for closed_fd in (
                new_root_fd,
                new_publication_lock_fd,
                new_evidence_fd,
            ):
                with self.assertRaises(OSError):
                    os.fstat(closed_fd)

    def test_reconfigure_hides_transient_new_authority_state_from_readers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            old_store = self.make_store(
                base / "old-root",
                evidence_path=base / "old-state" / "evidence.json",
            )
            old_store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="old",
            )
            new_root = base / "new-root"
            new_evidence = base / "new-state" / "evidence.json"
            prepared = self.make_store(new_root, evidence_path=new_evidence)
            prepared.publish_success(
                identity=AuditPublicationIdentity(2, 2, BLOCK_B),
                report=self.report(block_height=2),
                persistence=self.persistence(),
                evidence={},
                created_at="new",
            )
            prepared.close()

            real_load = old_store._load_current_evidence_locked
            load_entered = threading.Event()
            release_load = threading.Event()
            reader_started = threading.Event()
            reader_values: list[dict[str, object] | None] = []
            errors: list[BaseException] = []

            def blocked_load() -> None:
                load_entered.set()
                if not release_load.wait(5):
                    raise AssertionError("timed out waiting to release evidence load")
                real_load()

            def reconfigure() -> None:
                try:
                    old_store.reconfigure(
                        root=new_root,
                        evidence_path=new_evidence,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            def read_latest() -> None:
                reader_started.set()
                try:
                    reader_values.append(old_store.latest_evidence())
                except BaseException as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            with mock.patch.object(
                old_store,
                "_load_current_evidence_locked",
                side_effect=blocked_load,
            ):
                reconfigure_thread = threading.Thread(target=reconfigure)
                reconfigure_thread.start()
                self.assertTrue(load_entered.wait(5))
                reader_thread = threading.Thread(target=read_latest)
                reader_thread.start()
                self.assertTrue(reader_started.wait(5))
                reader_thread.join(timeout=0.05)
                self.assertTrue(reader_thread.is_alive())
                release_load.set()
                reconfigure_thread.join(timeout=5)
                reader_thread.join(timeout=5)

            self.assertFalse(errors)
            self.assertFalse(reconfigure_thread.is_alive())
            self.assertFalse(reader_thread.is_alive())
            self.assertEqual(len(reader_values), 1)
            assert reader_values[0] is not None
            self.assertEqual(reader_values[0]["block_hash"], BLOCK_B)
            self.assertEqual(old_store.publication_sequence_floor(), 2)

    def test_publication_guard_is_required_and_internal_file_is_hidden(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = AuditArtifactStore(
                AuditArtifactConfig(
                    root=root,
                    evidence_path=root / "evidence.json",
                ),
                canonicalizer=canonical_bundle_bytes,
            )
            identity = AuditPublicationIdentity(1, 1, BLOCK_A)
            with self.assertRaisesRegex(RuntimeError, "guard is required"):
                store.publish_success(
                    identity=identity,
                    publication_floor_sequence=1,
                    report=self.report(),
                    persistence=self.persistence(),
                    evidence={},
                    verification_identity={},
                    created_at="now",
                )
            with self.assertRaisesRegex(RuntimeError, "guard is required"):
                store.adopt_legacy_publication_identity(
                    identity,
                    publication_floor_sequence=1,
                )
            self.assertFalse(store.evidence_path.exists())
            metrics = store.metrics_snapshot()
            self.assertEqual(metrics["scan_error"], 0)
            for kind in (
                "body",
                "share_segment",
                "live_bundle",
                "candidate",
                "other",
            ):
                self.assertEqual(metrics[kind], {"files": 0, "bytes": 0})

            lock_path = root / ".prism-audit-publication.lock"
            parked = root / ".prism-audit-publication.lock.parked"
            lock_path.rename(parked)
            lock_path.write_bytes(b"replacement")
            with self.assertRaisesRegex(RuntimeError, "lock identity changed"):
                with store.publication_order_guard():
                    pass
            self.assertEqual(store.prune_best_effort().errors, 1)
            lock_path.unlink()
            parked.rename(lock_path)
            with store.publication_order_guard():
                with store.publication_order_guard():
                    pass
                old_authority = (
                    store.root,
                    store.evidence_path,
                    store._root_fd,
                    store._publication_lock_fd,
                    store._evidence_parent_fd,
                )
                next_root = root / "nested-reconfigure"
                with self.assertRaisesRegex(RuntimeError, "inside publication guard"):
                    store.reconfigure(
                        root=next_root,
                        evidence_path=next_root / "evidence.json",
                    )
                self.assertEqual(
                    (
                        store.root,
                        store.evidence_path,
                        store._root_fd,
                        store._publication_lock_fd,
                        store._evidence_parent_fd,
                    ),
                    old_authority,
                )
                self.assertFalse(next_root.exists())
                for fd in old_authority[2:]:
                    os.fstat(fd)

    def test_close_waits_for_guard_and_is_rejected_inside_owned_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            guard_entered = threading.Event()
            release_guard = threading.Event()
            close_attempting = threading.Event()
            close_finished = threading.Event()

            def hold_guard() -> None:
                with store.publication_order_guard():
                    guard_entered.set()
                    release_guard.wait(5)

            def close_store() -> None:
                close_attempting.set()
                store.close()
                close_finished.set()

            guard_thread = threading.Thread(target=hold_guard)
            guard_thread.start()
            self.assertTrue(guard_entered.wait(5))
            close_thread = threading.Thread(target=close_store)
            close_thread.start()
            self.assertTrue(close_attempting.wait(5))
            self.assertFalse(close_finished.wait(0.05))
            release_guard.set()
            guard_thread.join(timeout=5)
            close_thread.join(timeout=5)
            self.assertFalse(guard_thread.is_alive())
            self.assertFalse(close_thread.is_alive())
            self.assertTrue(close_finished.is_set())
            with self.assertRaisesRegex(RuntimeError, "closed"):
                with store.publication_order_guard():
                    pass

            second = self.make_store(root / "second")
            old_fds = (
                second._root_fd,
                second._publication_lock_fd,
                second._evidence_parent_fd,
            )
            with second.publication_order_guard():
                with self.assertRaisesRegex(RuntimeError, "inside publication guard"):
                    second.close()
                for fd in old_fds:
                    os.fstat(fd)
            second.close()

    def test_publication_lock_symlink_is_rejected_on_open_and_reconfigure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            outside = base / "outside"
            outside.write_bytes(b"outside")
            bad_root = base / "bad-root"
            bad_root.mkdir()
            (bad_root / ".prism-audit-publication.lock").symlink_to(outside)
            with self.assertRaisesRegex(RuntimeError, "cannot be opened safely"):
                AuditArtifactStore(
                    AuditArtifactConfig(
                        root=bad_root,
                        evidence_path=bad_root / "evidence.json",
                    )
                )

            good_root = base / "good-root"
            store = self.make_store(good_root)
            old_state = (
                store.root,
                store._root_fd,
                store._publication_lock_fd,
                store._evidence_parent_fd,
            )
            with self.assertRaisesRegex(RuntimeError, "cannot be opened safely"):
                store.reconfigure(root=bad_root)
            self.assertEqual(store.root, old_state[0])
            for fd in old_state[1:]:
                os.fstat(fd)
            self.assertEqual(outside.read_bytes(), b"outside")

    def test_two_store_and_subprocess_publication_guards_serialize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = self.make_store(root)
            second = self.make_store(root)
            attempting = threading.Event()
            entered = threading.Event()
            release = threading.Event()

            def hold_second() -> None:
                attempting.set()
                with second.publication_order_guard():
                    entered.set()
                    release.wait(5)

            with first.publication_order_guard():
                thread = threading.Thread(target=hold_second)
                thread.start()
                self.assertTrue(attempting.wait(5))
                self.assertFalse(entered.wait(0.05))
                with self.assertRaisesRegex(RuntimeError, "another store"):
                    with second.publication_order_guard():
                        pass
            self.assertTrue(entered.wait(5))
            release.set()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())

            script = """
import sys
from pathlib import Path
from lab.prism.audit_artifacts import AuditArtifactConfig, AuditArtifactStore
root = Path(sys.argv[1])
store = AuditArtifactStore(AuditArtifactConfig(root=root, evidence_path=root / 'evidence.json'))
with store.publication_order_guard():
    print('locked', flush=True)
    sys.stdin.readline()
"""
            child = subprocess.Popen(
                [sys.executable, "-c", script, str(root)],
                cwd=Path(__file__).resolve().parents[1],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                assert child.stdin is not None
                assert child.stdout is not None
                assert child.stderr is not None
                ready, _writable, _exceptional = select.select(
                    [child.stdout],
                    [],
                    [],
                    5,
                )
                self.assertEqual(ready, [child.stdout], "child guard handshake timed out")
                self.assertEqual(child.stdout.readline().strip(), "locked")
                parent_attempting = threading.Event()
                parent_entered = threading.Event()

                def enter_parent() -> None:
                    parent_attempting.set()
                    with first.publication_order_guard():
                        parent_entered.set()

                parent_thread = threading.Thread(target=enter_parent)
                parent_thread.start()
                self.assertTrue(parent_attempting.wait(5))
                self.assertFalse(parent_entered.wait(0.05))
                child.stdin.write("\n")
                child.stdin.flush()
                child.wait(timeout=5)
                parent_thread.join(timeout=5)
                self.assertEqual(child.returncode, 0, child.stderr.read())
                self.assertTrue(parent_entered.is_set())
                self.assertFalse(parent_thread.is_alive())
            finally:
                if child.poll() is None:
                    try:
                        assert child.stdin is not None
                        child.stdin.write("\n")
                        child.stdin.flush()
                        child.wait(timeout=2)
                    except (BrokenPipeError, subprocess.TimeoutExpired):
                        child.kill()
                        child.wait(timeout=2)
                for stream in (child.stdin, child.stdout, child.stderr):
                    if stream is not None:
                        stream.close()

    def test_peer_publication_reconciles_stale_replay_repair_and_prune(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stale = self.make_store(root, live_bundle_retention=2)
            first = stale.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={"confirmation": {"confirmed_count": 1}},
                created_at="first",
            )
            peer = self.make_store(root, live_bundle_retention=2)
            second = peer.publish_success(
                identity=AuditPublicationIdentity(2, 2, BLOCK_B),
                report=self.report(block_height=2),
                persistence=self.persistence(),
                evidence={"confirmation": {"confirmed_count": 1}},
                created_at="second",
            )

            replay = stale.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                publication_floor_sequence=2,
                report=self.report(),
                persistence=self.persistence(),
                evidence={"confirmation": {"confirmed_count": 1}},
                created_at="stale",
            )
            self.assertFalse(replay.published)
            self.assertEqual(stale.latest_evidence()["block_hash"], BLOCK_B)  # type: ignore[index]

            stale._invalidated_legacy_identity = AuditPublicationIdentity(
                1,
                1,
                BLOCK_A,
            )
            with self.assertRaisesRegex(RuntimeError, "identity conflict"):
                stale.publish_success(
                    identity=AuditPublicationIdentity(2, 2, BLOCK_A),
                    publication_floor_sequence=2,
                    report=self.report(block_height=2),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="conflict",
                )
            stale.reconfigure(live_bundle_retention=0)
            result = stale.prune_best_effort()
            self.assertTrue(second.envelope_path.exists())
            self.assertFalse(first.envelope_path.exists())
            self.assertEqual(result.live_removed, 1)

    def test_floor_publication_fence_blocks_allocator_and_stale_inverse_repair(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root, live_bundle_retention=2)
            ledger = SingleWriterShareLedger()
            balance_lock = threading.RLock()

            def persist(block_hash: str, height: int) -> None:
                ledger.persist_accepted_block(
                    block_hash=block_hash,
                    block_height=height,
                    parent_hash="00" * 32,
                    final_bundle={},
                    audit_report={},
                )

            persist(BLOCK_A, 1)
            confirmed_a = ledger.confirm_accepted_block(
                block_hash=BLOCK_A,
                active_tip_height=1,
            )
            identity_a = AuditPublicationIdentity(
                int(confirmed_a["audit_publication_sequence"]),
                1,
                BLOCK_A,
            )
            floor_read = threading.Event()
            release_publisher = threading.Event()
            allocator_attempting = threading.Event()
            allocator_allocated = threading.Event()
            errors: list[BaseException] = []
            publications: list[object] = []

            def publish_a() -> None:
                try:
                    with balance_lock:
                        with store.publication_order_guard():
                            floor = ledger.audit_publication_sequence_floor()
                            floor_read.set()
                            if not release_publisher.wait(5):
                                raise AssertionError("publisher release timed out")
                            publications.append(
                                store.publish_success(
                                    identity=identity_a,
                                    publication_floor_sequence=floor,
                                    report=self.report(block_height=1),
                                    persistence=self.persistence(),
                                    evidence={"confirmation": confirmed_a},
                                    created_at="a",
                                )
                            )
                except BaseException as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            def allocate_and_publish_b() -> None:
                try:
                    allocator_attempting.set()
                    with balance_lock:
                        with store.publication_order_guard():
                            persist(BLOCK_B, 2)
                            confirmed_b = ledger.confirm_accepted_block(
                                block_hash=BLOCK_B,
                                active_tip_height=2,
                            )
                            allocator_allocated.set()
                            identity_b = AuditPublicationIdentity(
                                int(confirmed_b["audit_publication_sequence"]),
                                2,
                                BLOCK_B,
                            )
                            publications.append(
                                store.publish_success(
                                    identity=identity_b,
                                    publication_floor_sequence=(
                                        ledger.audit_publication_sequence_floor()
                                    ),
                                    report=self.report(block_height=2),
                                    persistence=self.persistence(),
                                    evidence={"confirmation": confirmed_b},
                                    created_at="b",
                                )
                            )
                except BaseException as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            publisher = threading.Thread(target=publish_a)
            publisher.start()
            allocator: threading.Thread | None = None
            try:
                self.assertTrue(floor_read.wait(5))
                allocator = threading.Thread(target=allocate_and_publish_b)
                allocator.start()
                self.assertTrue(allocator_attempting.wait(5))
                unexpectedly_acquired = balance_lock.acquire(blocking=False)
                if unexpectedly_acquired:
                    balance_lock.release()
                self.assertFalse(unexpectedly_acquired)
                self.assertFalse(allocator_allocated.is_set())
            finally:
                release_publisher.set()
                publisher.join(timeout=5)
                if allocator is not None:
                    allocator.join(timeout=5)
            self.assertFalse(publisher.is_alive())
            assert allocator is not None
            self.assertFalse(allocator.is_alive())
            self.assertFalse(errors)
            self.assertTrue(allocator_allocated.is_set())
            self.assertEqual(len(publications), 2)
            self.assertEqual(ledger.audit_publication_sequence_floor(), 2)
            latest = store.latest_evidence()
            assert latest is not None
            self.assertEqual(latest["block_hash"], BLOCK_B)

            stale_envelope = store.live_envelope_path(
                block_height=1,
                block_hash=BLOCK_A,
            )
            stale_envelope.unlink()
            durable_before = store.evidence_path.read_bytes()
            durable_identity = _FileIdentity.from_stat(store.evidence_path.stat())
            with balance_lock:
                with store.publication_order_guard():
                    stale = store.publish_success(
                        identity=identity_a,
                        publication_floor_sequence=(
                            ledger.audit_publication_sequence_floor()
                        ),
                        report=self.report(block_height=1),
                        persistence=self.persistence(),
                        evidence={"confirmation": confirmed_a},
                        created_at="stale",
                    )
            self.assertFalse(stale.published)
            self.assertFalse(stale_envelope.exists())
            self.assertEqual(store.evidence_path.read_bytes(), durable_before)
            self.assertTrue(durable_identity.matches(store.evidence_path.stat()))

    def test_reconfigure_busy_and_post_swap_failure_are_atomic_and_fd_clean(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            old_root = base / "old-root"
            old_evidence = base / "old-state" / "evidence.json"
            store = self.make_store(old_root, evidence_path=old_evidence)
            new_root = base / "new-root"
            blocker = self.make_store(
                new_root,
                evidence_path=base / "new-state" / "evidence.json",
            )
            blocker_entered = threading.Event()
            release_blocker = threading.Event()

            def hold_new_root() -> None:
                with blocker.publication_order_guard():
                    blocker_entered.set()
                    release_blocker.wait(5)

            blocker_thread = threading.Thread(target=hold_new_root)
            blocker_thread.start()
            self.assertTrue(blocker_entered.wait(5))
            old_fds = (
                store._root_fd,
                store._publication_lock_fd,
                store._evidence_parent_fd,
            )
            with self.assertRaisesRegex(RuntimeError, "guard is busy"):
                store.reconfigure(
                    root=new_root,
                    evidence_path=base / "new-state" / "evidence.json",
                )
            self.assertEqual(store.root, old_root.resolve())
            for fd in old_fds:
                os.fstat(fd)
            release_blocker.set()
            blocker_thread.join(timeout=5)
            self.assertFalse(blocker_thread.is_alive())

            target_root = base / "target-root"
            target_evidence = base / "target-state" / "evidence.json"
            prepared_fds: list[int] = []
            real_open_directory = store._open_directory_authority
            real_open_lock = store._open_publication_lock_authority

            def record_directory(path: Path) -> tuple[int, tuple[int, int]]:
                result = real_open_directory(path)
                prepared_fds.append(result[0])
                return result

            def record_lock(
                root: Path,
                root_fd: int,
                root_identity: tuple[int, int],
            ) -> tuple[int, _FileIdentity]:
                result = real_open_lock(root, root_fd, root_identity)
                prepared_fds.append(result[0])
                return result

            with mock.patch.object(
                store,
                "_open_directory_authority",
                side_effect=record_directory,
            ), mock.patch.object(
                store,
                "_open_publication_lock_authority",
                side_effect=record_lock,
            ), mock.patch.object(
                store,
                "_reload_current_evidence_locked",
                side_effect=RuntimeError("injected post-swap load failure"),
            ), self.assertRaisesRegex(RuntimeError, "post-swap load"):
                store.reconfigure(
                    root=target_root,
                    evidence_path=target_evidence,
                )
            self.assertEqual(store.root, old_root.resolve())
            self.assertEqual(store.evidence_path, old_evidence.resolve())
            self.assertEqual(
                (
                    store._root_fd,
                    store._publication_lock_fd,
                    store._evidence_parent_fd,
                ),
                old_fds,
            )
            for fd in old_fds:
                os.fstat(fd)
            for fd in prepared_fds:
                with self.assertRaises(OSError):
                    os.fstat(fd)

            boundary_root = base / "boundary-root"
            boundary_evidence = base / "boundary-state" / "evidence.json"
            boundary_prepared_fds: list[int] = []
            explicit_old_validations = 0
            real_validate = store._validate_publication_lock_identity

            def record_boundary_directory(
                path: Path,
            ) -> tuple[int, tuple[int, int]]:
                result = real_open_directory(path)
                boundary_prepared_fds.append(result[0])
                return result

            def record_boundary_lock(
                root: Path,
                root_fd: int,
                root_identity: tuple[int, int],
            ) -> tuple[int, _FileIdentity]:
                result = real_open_lock(root, root_fd, root_identity)
                boundary_prepared_fds.append(result[0])
                return result

            def fail_final_old_validation(**kwargs: object) -> None:
                nonlocal explicit_old_validations
                if kwargs.get("root_fd") == old_fds[0]:
                    explicit_old_validations += 1
                    if explicit_old_validations == 2:
                        raise RuntimeError("injected final old-authority loss")
                real_validate(**kwargs)  # type: ignore[arg-type]

            with mock.patch.object(
                store,
                "_open_directory_authority",
                side_effect=record_boundary_directory,
            ), mock.patch.object(
                store,
                "_open_publication_lock_authority",
                side_effect=record_boundary_lock,
            ), mock.patch.object(
                store,
                "_validate_publication_lock_identity",
                side_effect=fail_final_old_validation,
            ), self.assertRaisesRegex(RuntimeError, "final old-authority"):
                store.reconfigure(
                    root=boundary_root,
                    evidence_path=boundary_evidence,
                )
            self.assertEqual(store.root, old_root.resolve())
            self.assertEqual(store.evidence_path, old_evidence.resolve())
            self.assertEqual(
                (
                    store._root_fd,
                    store._publication_lock_fd,
                    store._evidence_parent_fd,
                ),
                old_fds,
            )
            for fd in old_fds:
                os.fstat(fd)
            for fd in boundary_prepared_fds:
                with self.assertRaises(OSError):
                    os.fstat(fd)

            store.reconfigure(
                root=boundary_root,
                evidence_path=boundary_evidence,
            )
            self.assertEqual(store.root, boundary_root.resolve())
            self.assertEqual(store.evidence_path, boundary_evidence.resolve())
            for fd in old_fds:
                with self.assertRaises(OSError):
                    os.fstat(fd)

    def test_strict_artifact_classification_rejects_lookalikes(self) -> None:
        self.assertEqual(
            AuditArtifactStore.artifact_kind(
                f"prism-audit-bundle-body-{BLOCK_A}-{DIGEST}.json"
            ),
            "body",
        )
        self.assertEqual(
            AuditArtifactStore.artifact_kind(
                f"prism-live-audit-bundle-2-{BLOCK_A}.json"
            ),
            "live_bundle",
        )
        self.assertEqual(
            AuditArtifactStore.artifact_kind(
                f".prism-live-audit-bundle-candidate-{BLOCK_A}.json.tmp"
            ),
            "candidate",
        )
        for name in (
            f"prism-live-audit-bundle-02-{BLOCK_A}.json",
            f"prism-live-audit-bundle-2-{BLOCK_A}.json.bak",
            "prism-live-audit-bundle-candidate-operator.json",
        ):
            self.assertEqual(AuditArtifactStore.artifact_kind(name), "other")

    def test_a1_only_consumes_an_injected_j1_canonical_capability(self) -> None:
        with mock.patch(
            "lab.prism.audit_artifacts.subprocess.run",
            side_effect=AssertionError("A1 must not own canonicalizer subprocesses"),
        ):
            self.assertEqual(
                canonical_audit_bundle_bytes({"x": 1}, lambda _value: b"typed"),
                b"typed",
            )
            with self.assertRaisesRegex(RuntimeError, "J1 canonical"):
                canonical_audit_bundle_bytes({"x": 1})

        completed = mock.Mock(returncode=0, stdout=b"rust-typed", stderr=b"")
        with mock.patch(
            "lab.prism.bundle_compiler.subprocess.run",
            return_value=completed,
        ) as run:
            self.assertEqual(canonical_bundle_bytes({"x": "miner-é"}), b"rust-typed")
        self.assertIn("qbit-prism-audit-canonicalize", " ".join(run.call_args.args[0]))

    def test_candidate_cleanup_removes_only_its_adopted_inode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            candidate.path.write_bytes(b"first")
            self.transfer_candidate(store, candidate)
            candidate.path.unlink()
            candidate.path.write_bytes(b"replacement")
            store.discard_candidate(candidate)
            self.assertEqual(candidate.path.read_bytes(), b"replacement")

    def test_unadopted_candidate_collision_is_preserved_and_released(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            candidate.path.write_bytes(b"competitor")
            with self.assertRaises(FileExistsError):
                store.write_compatibility_candidate(candidate, {"x": 1})
            self.assertEqual(candidate.path.read_bytes(), b"competitor")
            store.reconfigure(root=root / "next")

    def test_unadopted_discard_never_deletes_a_created_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            candidate.path.write_bytes(b"not-transferred")
            store.discard_candidate(candidate)
            self.assertEqual(candidate.path.read_bytes(), b"not-transferred")

    def test_reconfigure_rejects_active_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root / "a")
            store.issue_candidate(block_hash=BLOCK_A)
            with self.assertRaisesRegex(RuntimeError, "candidates are active"):
                store.reconfigure(root=root / "b")

    def test_compatibility_candidate_failure_cleans_owned_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            with mock.patch("os.fsync", side_effect=OSError("fsync")):
                with self.assertRaises(OSError):
                    store.write_compatibility_candidate(candidate, {"x": 1})
            self.assertFalse(candidate.path.exists())

    def test_verify_candidate_binds_literal_identity_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            candidate.path.write_bytes(b"canonical")
            self.transfer_candidate(store, candidate)
            digest = hashlib.sha256(b"canonical").hexdigest()
            verified = store.verify_candidate(
                candidate,
                coinbase_tx_hex="00",
                expected_coinbase_value_sats=1,
                trusted_writer_public_key_hex="44" * 32,
                verifier=lambda *_args, **_kwargs: self.report(digest),
            )
            self.assertTrue(verified.canonical_copy_eligible)
            self.assertEqual(verified.literal_sha256, digest)
            self.assertEqual(
                verified.verification_identity,
                AuditArtifactStore.build_verification_identity(
                    trust_source="configured",
                    trusted_writer_public_key_hex="44" * 32,
                    literal_sha256=digest,
                    literal_byte_len=len(b"canonical"),
                    report=self.report(digest),
                ),
            )

    def test_verifier_retry_identity_never_reuses_prior_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            candidate.path.write_bytes(b"canonical")
            self.transfer_candidate(store, candidate)
            digest = hashlib.sha256(b"canonical").hexdigest()

            with self.assertRaisesRegex(RuntimeError, "failed attempt"):
                store.verify_candidate(
                    candidate,
                    coinbase_tx_hex="00",
                    expected_coinbase_value_sats=1,
                    trusted_writer_public_key_hex="44" * 32,
                    verifier=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                        RuntimeError("failed attempt")
                    ),
                )
            verified = store.verify_candidate(
                candidate,
                coinbase_tx_hex="00",
                expected_coinbase_value_sats=1,
                trusted_writer_public_key_hex="44" * 32,
                verifier=lambda *_args, **_kwargs: self.report(digest),
            )
            store.require_current_verified_candidate(verified, candidate)

            store.discard_candidate(candidate)
            replacement = store.issue_candidate(block_hash=BLOCK_A)
            replacement.path.write_bytes(b"canonical")
            self.transfer_candidate(store, replacement)
            with self.assertRaisesRegex(RuntimeError, "another candidate"):
                store.require_current_verified_candidate(verified, replacement)
            with self.assertRaisesRegex(RuntimeError, "later failure"):
                store.verify_candidate(
                    replacement,
                    coinbase_tx_hex="00",
                    expected_coinbase_value_sats=1,
                    trusted_writer_public_key_hex="44" * 32,
                    verifier=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                        RuntimeError("later failure")
                    ),
                )

    def test_verify_candidate_rejects_post_verifier_swap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            candidate.path.write_bytes(b"canonical")
            self.transfer_candidate(store, candidate)

            def verifier(_path: Path, *_args: object, **_kwargs: object) -> dict[str, object]:
                candidate.path.unlink()
                candidate.path.write_bytes(b"replacement")
                return self.report(hashlib.sha256(b"replacement").hexdigest())

            with self.assertRaisesRegex(RuntimeError, "changed during"):
                store.verify_candidate(
                    candidate,
                    coinbase_tx_hex="00",
                    expected_coinbase_value_sats=1,
                    trusted_writer_public_key_hex="44" * 32,
                    verifier=verifier,
                )

    def test_verifier_report_requires_complete_coinbase_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for missing in (
                "audit_bundle_sha256_hex",
                "reward_manifest_sha256_hex",
                "payout_policy_manifest_sha256_hex",
                "prism_audit_commitment_leaf_hex",
                "audit_commitment_root_hex",
                "coinbase_txid",
                "coinbase_wtxid",
                "coinbase_manifest_sha256_hex",
                "coinbase_tx_hex",
                "coinbase_value_sats",
                "min_output_sats",
                "onchain_output_count",
                "accrued_account_count",
            ):
                with self.subTest(missing=missing):
                    store = self.make_store(Path(tmp) / missing)
                    candidate = store.issue_candidate(block_hash=BLOCK_A)
                    candidate.path.write_bytes(b"canonical")
                    self.transfer_candidate(store, candidate)
                    report = self.report(hashlib.sha256(b"canonical").hexdigest())
                    report.pop(missing)
                    with self.assertRaises((RuntimeError, ValueError)):
                        store.verify_candidate(
                            candidate,
                            coinbase_tx_hex="00",
                            expected_coinbase_value_sats=1,
                            trusted_writer_public_key_hex="44" * 32,
                            verifier=lambda *_args, _report=report, **_kwargs: _report,
                        )
                    store.discard_candidate(candidate)

    def test_verifier_report_schema_types_and_height_fail_closed(self) -> None:
        cases = (
            ("schema", "wrong"),
            ("block_height", "1"),
            ("block_height", 2),
            ("coinbase_value_sats", 1.5),
            ("min_output_sats", True),
            ("onchain_output_count", -1),
            ("accrued_account_count", 1.5),
        )
        for field, value in cases:
            with self.subTest(field=field, value=value):
                report = self.report()
                report[field] = value
                with self.assertRaises(RuntimeError):
                    AuditArtifactStore._validate_verifier_report(
                        report,
                        coinbase_tx_hex="00",
                        expected_coinbase_value_sats=1,
                        expected_block_height=1,
                    )

    def test_trust_precedence_and_embedded_test_key_gate(self) -> None:
        bundle = {
            "ledger_window_attestation": {
                "signature": {"public_key_hex": "55" * 32}
            }
        }
        self.assertEqual(
            AuditArtifactStore.trusted_writer_key(
                "44" * 32,
                bundle,
                allow_embedded_test_key=True,
            ),
            "44" * 32,
        )
        with self.assertRaisesRegex(RuntimeError, "configured"):
            AuditArtifactStore.trusted_writer_key(
                None,
                bundle,
                allow_embedded_test_key=False,
            )
        self.assertEqual(
            AuditArtifactStore.trusted_writer_key(
                None,
                bundle,
                allow_embedded_test_key=True,
            ),
            "55" * 32,
        )

    def test_verifier_reads_unlinked_snapshot_during_candidate_aba(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            candidate = store.issue_candidate(block_hash=BLOCK_A)
            candidate.path.write_bytes(b"canonical")
            self.transfer_candidate(store, candidate)
            original = candidate.path.stat()
            digest = hashlib.sha256(b"canonical").hexdigest()

            def verifier(path: Path, *_args: object, **_kwargs: object) -> dict[str, object]:
                self.assertEqual(path.read_bytes(), b"canonical")
                candidate.path.write_bytes(b"replacement")
                candidate.path.write_bytes(b"canonical")
                os.utime(
                    candidate.path,
                    ns=(original.st_atime_ns, original.st_mtime_ns),
                )
                return self.report(digest)

            verified = store.verify_candidate(
                candidate,
                coinbase_tx_hex="00",
                expected_coinbase_value_sats=1,
                trusted_writer_public_key_hex="44" * 32,
                expected_block_height=1,
                verifier=verifier,
            )
            self.assertEqual(verified.literal_sha256, digest)

    def test_verifier_subprocess_timeout_nonzero_malformed_and_oversize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle = root / "bundle.json"
            bundle.write_text("{}", encoding="utf-8")
            cases = (
                (
                    [sys.executable, "-c", "import time; time.sleep(2)"],
                    "timed out",
                    0.05,
                ),
                ([sys.executable, "-c", "import sys; sys.stderr.write('bad'); sys.exit(2)"], "failed: bad", 1.0),
                ([sys.executable, "-c", "print('not-json')"], "invalid JSON", 1.0),
                ([sys.executable, "-c", "import sys; sys.stdout.write('x' * 1100000)"], "output exceeded", 1.0),
            )
            for command, message, timeout in cases:
                with self.subTest(message=message):
                    store = self.make_store(
                        root / message.replace(" ", "-"),
                        verifier_timeout_seconds=timeout,
                    )
                    with mock.patch(
                        "lab.prism.audit_artifacts.prism_tool_command",
                        return_value=command,
                    ), self.assertRaisesRegex(RuntimeError, message):
                        store.verify_bundle(
                            bundle,
                            "00",
                            "44" * 32,
                            expected_coinbase_value_sats=1,
                        )

    def test_verifier_timeout_kills_descendants_that_inherit_output_pipes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle = root / "bundle.json"
            marker = root / "descendant-survived"
            bundle.write_text("{}", encoding="utf-8")
            descendant = (
                "import pathlib,time; time.sleep(0.4); "
                f"pathlib.Path({str(marker)!r}).write_text('survived')"
            )
            parent = (
                "import subprocess,sys,time; "
                f"subprocess.Popen([sys.executable,'-c',{descendant!r}],"
                "stdout=sys.stdout,stderr=sys.stderr); time.sleep(5)"
            )
            store = self.make_store(
                root / "audit",
                verifier_timeout_seconds=0.05,
            )
            with mock.patch(
                "lab.prism.audit_artifacts.prism_tool_command",
                return_value=[sys.executable, "-c", parent],
            ), self.assertRaisesRegex(RuntimeError, "timed out"):
                store.verify_bundle(
                    bundle,
                    "00",
                    "44" * 32,
                    expected_coinbase_value_sats=1,
                )
            time.sleep(0.5)
            self.assertFalse(marker.exists())

    def test_publication_uses_durable_ordinal_not_height(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp), live_bundle_retention=0)
            newer = store.publish_success(
                identity=AuditPublicationIdentity(8, 101, BLOCK_B),
                report=self.report(block_height=101),
                persistence=self.persistence(),
                evidence={"audit_report": self.report(), "persistence": self.persistence()},
                created_at="now",
            )
            stale = store.publish_success(
                identity=AuditPublicationIdentity(7, 100, BLOCK_A),
                report=self.report(block_height=100),
                persistence=self.persistence(),
                evidence={"audit_report": self.report(), "persistence": self.persistence()},
                created_at="later",
            )
            self.assertTrue(newer.published)
            self.assertFalse(stale.published)
            self.assertEqual(store.latest_evidence()["block_hash"], BLOCK_B)  # type: ignore[index]
            deep_reorg = store.publish_success(
                identity=AuditPublicationIdentity(9, 99, BLOCK_A),
                report=self.report(block_height=99),
                persistence=self.persistence(),
                evidence={"audit_report": self.report(), "persistence": self.persistence()},
                created_at="latest",
            )
            self.assertTrue(deep_reorg.published)
            self.assertEqual(store.latest_evidence()["block_hash"], BLOCK_A)  # type: ignore[index]

    def test_publication_and_startup_bind_report_height(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            with self.assertRaisesRegex(RuntimeError, "block height"):
                store.publish_success(
                    identity=AuditPublicationIdentity(1, 2, BLOCK_A),
                    report=self.report(block_height=1),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="now",
                )
            store.publish_success(
                identity=AuditPublicationIdentity(1, 2, BLOCK_A),
                report=self.report(block_height=2),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            payload = json.loads(store.evidence_path.read_text(encoding="utf-8"))
            payload["audit_report"]["block_height"] = 1
            store.evidence_path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertIsNone(self.make_store(root).latest_evidence())

    def test_equal_ordinal_conflict_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(block_height=1),
                persistence=self.persistence(),
                evidence={"audit_report": self.report(), "persistence": self.persistence()},
                created_at="now",
            )
            with self.assertRaisesRegex(RuntimeError, "conflict"):
                store.publish_success(
                    identity=AuditPublicationIdentity(1, 1, BLOCK_B),
                    report=self.report(block_height=1),
                    persistence=self.persistence(),
                    evidence={"audit_report": self.report(), "persistence": self.persistence()},
                    created_at="later",
                )

    def test_exact_replay_rejects_changed_coinbase_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            identity = AuditPublicationIdentity(1, 1, BLOCK_A)
            store.publish_success(
                identity=identity,
                report=self.report(block_height=1),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            changed = self.report(block_height=1)
            changed["coinbase_txid"] = "99" * 32
            with self.assertRaisesRegex(RuntimeError, "replay payload conflict"):
                store.publish_success(
                    identity=identity,
                    report=changed,
                    persistence=self.persistence(),
                    evidence={},
                    created_at="later",
                )

    def test_exact_replay_is_stable_but_changed_evidence_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            identity = AuditPublicationIdentity(3, 4, BLOCK_A)
            first = store.publish_success(
                identity=identity,
                report=self.report(block_height=4),
                persistence=self.persistence(),
                evidence={"confirmation": {"confirmed_count": 1}},
                created_at="now",
            )
            replay = store.publish_success(
                identity=identity,
                report=self.report(block_height=4),
                persistence=self.persistence(),
                evidence={"confirmation": {"confirmed_count": 1}},
                created_at="later",
            )
            self.assertTrue(first.published)
            self.assertFalse(replay.published)
            advanced_floor_replay = store.publish_success(
                identity=identity,
                publication_floor_sequence=4,
                report=self.report(block_height=4),
                persistence=self.persistence(),
                evidence={"confirmation": {"confirmed_count": 1}},
                created_at="later-still",
            )
            self.assertFalse(advanced_floor_replay.published)
            with self.assertRaisesRegex(RuntimeError, "replay payload conflict"):
                store.publish_success(
                    identity=identity,
                    report=self.report(block_height=4),
                    persistence=self.persistence(),
                    evidence={"confirmation": {"confirmed_count": 2}},
                    created_at="later",
                )

    def test_exact_replay_repairs_invalid_mutable_evidence_but_rejects_envelope_replacement(
        self,
    ) -> None:
        identity = AuditPublicationIdentity(3, 4, BLOCK_A)
        for missing in ("evidence", "envelope", "both"):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                store = self.make_store(root)
                first = store.publish_success(
                    identity=identity,
                    report=self.report(block_height=4),
                    persistence=self.persistence(),
                    evidence={"confirmation": {"confirmed_count": 1}},
                    created_at="now",
                )
                if missing in {"evidence", "both"}:
                    store.evidence_path.unlink()
                if missing in {"envelope", "both"}:
                    first.envelope_path.unlink()
                before_advanced_floor_attempt = (
                    first.envelope_path.read_bytes()
                    if first.envelope_path.exists()
                    else None,
                    store.evidence_path.read_bytes()
                    if store.evidence_path.exists()
                    else None,
                )
                with self.assertRaisesRegex(RuntimeError, "behind"):
                    store.publish_success(
                        identity=identity,
                        publication_floor_sequence=4,
                        report=self.report(block_height=4),
                        persistence=self.persistence(),
                        evidence={"confirmation": {"confirmed_count": 1}},
                        created_at="advanced-floor",
                    )
                self.assertEqual(
                    (
                        first.envelope_path.read_bytes()
                        if first.envelope_path.exists()
                        else None,
                        store.evidence_path.read_bytes()
                        if store.evidence_path.exists()
                        else None,
                    ),
                    before_advanced_floor_attempt,
                )
                repaired = store.publish_success(
                    identity=identity,
                    report=self.report(block_height=4),
                    persistence=self.persistence(),
                    evidence={"confirmation": {"confirmed_count": 1}},
                    created_at="later",
                )
                self.assertTrue(repaired.published)
                self.assertTrue(store.evidence_path.exists())
                self.assertTrue(first.envelope_path.exists())
                store.close()
                restarted = self.make_store(root)
                self.assertEqual(
                    restarted.latest_evidence()["block_hash"],  # type: ignore[index]
                    BLOCK_A,
                )

        for replacement_bytes in (
            b"not-json",
            b'{"operator":"replacement"}',
        ):
            for restart in (False, True):
                with self.subTest(
                    evidence_replacement=replacement_bytes,
                    restart=restart,
                ), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    store = self.make_store(root)
                    first = store.publish_success(
                        identity=identity,
                        report=self.report(block_height=4),
                        persistence=self.persistence(),
                        evidence={"confirmation": {"confirmed_count": 1}},
                        created_at="now",
                    )
                    envelope_bytes = first.envelope_path.read_bytes()
                    store.evidence_path.write_bytes(replacement_bytes)
                    if restart:
                        store.close()
                        store = self.make_store(root)
                        self.assertIsNone(store.latest_evidence())
                    with self.assertRaisesRegex(RuntimeError, "behind"):
                        store.publish_success(
                            identity=identity,
                            publication_floor_sequence=4,
                            report=self.report(block_height=4),
                            persistence=self.persistence(),
                            evidence={"confirmation": {"confirmed_count": 1}},
                            created_at="advanced-floor",
                        )
                    self.assertEqual(
                        store.evidence_path.read_bytes(),
                        replacement_bytes,
                    )
                    repaired = store.publish_success(
                        identity=identity,
                        report=self.report(block_height=4),
                        persistence=self.persistence(),
                        evidence={"confirmation": {"confirmed_count": 1}},
                        created_at="later",
                    )
                    self.assertTrue(repaired.published)
                    self.assertEqual(first.envelope_path.read_bytes(), envelope_bytes)
                    self.assertEqual(
                        json.loads(store.evidence_path.read_text(encoding="utf-8"))[
                            "block_hash"
                        ],
                        BLOCK_A,
                    )

        for restart in (False, True):
            with self.subTest(
                envelope_replacement=True,
                restart=restart,
            ), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                store = self.make_store(root)
                first = store.publish_success(
                    identity=identity,
                    report=self.report(block_height=4),
                    persistence=self.persistence(),
                    evidence={"confirmation": {"confirmed_count": 1}},
                    created_at="now",
                )
                first.envelope_path.write_bytes(b'{"operator":"replacement"}')
                replacement_bytes = first.envelope_path.read_bytes()
                if restart:
                    store.close()
                    store = self.make_store(root)
                    self.assertIsNone(store.latest_evidence())
                with self.assertRaisesRegex(RuntimeError, "(conflicts|invalid)"):
                    store.publish_success(
                        identity=identity,
                        report=self.report(block_height=4),
                        persistence=self.persistence(),
                        evidence={"confirmation": {"confirmed_count": 1}},
                        created_at="later",
                    )
                self.assertEqual(first.envelope_path.read_bytes(), replacement_bytes)

    def test_newer_publication_repairs_invalid_evidence_and_repoints_it(self) -> None:
        for next_block_hash, next_height in ((BLOCK_A, 4), (BLOCK_B, 5)):
            for restart in (False, True):
                with self.subTest(
                    next_block_hash=next_block_hash,
                    restart=restart,
                ), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    store = self.make_store(root)
                    first = store.publish_success(
                        identity=AuditPublicationIdentity(3, 4, BLOCK_A),
                        report=self.report(block_height=4),
                        persistence=self.persistence(),
                        evidence={"confirmation": {"confirmed_count": 1}},
                        created_at="now",
                    )
                    first_envelope_bytes = first.envelope_path.read_bytes()
                    store.evidence_path.write_bytes(b'{"operator":"replacement"}')
                    if restart:
                        store.close()
                        store = self.make_store(root)
                        self.assertIsNone(store.latest_evidence())
                    second = store.publish_success(
                        identity=AuditPublicationIdentity(
                            4,
                            next_height,
                            next_block_hash,
                        ),
                        publication_floor_sequence=4,
                        report=self.report(block_height=next_height),
                        persistence=self.persistence(),
                        evidence={"confirmation": {"confirmed_count": 1}},
                        created_at="later",
                    )
                    self.assertTrue(second.published)
                    self.assertEqual(
                        first.envelope_path.read_bytes(),
                        first_envelope_bytes,
                    )
                    self.assertTrue(second.envelope_path.exists())
                    self.assertEqual(
                        json.loads(store.evidence_path.read_text(encoding="utf-8"))[
                            "block_hash"
                        ],
                        next_block_hash,
                    )

    def test_restart_repair_requires_the_fresh_durable_publication_floor(self) -> None:
        identity = AuditPublicationIdentity(3, 4, BLOCK_A)
        for damage in ("missing_envelope", "malformed_evidence", "both_missing"):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                store = self.make_store(root)
                first = store.publish_success(
                    identity=identity,
                    report=self.report(block_height=4),
                    persistence=self.persistence(),
                    evidence={"confirmation": {"confirmed_count": 1}},
                    created_at="now",
                )
                if damage in {"missing_envelope", "both_missing"}:
                    first.envelope_path.unlink()
                if damage == "malformed_evidence":
                    store.evidence_path.write_bytes(b"not-json")
                elif damage == "both_missing":
                    store.evidence_path.unlink()
                store.close()

                restarted = self.make_store(root)
                self.assertIsNone(restarted.latest_evidence())
                evidence_before = (
                    restarted.evidence_path.read_bytes()
                    if restarted.evidence_path.exists()
                    else None
                )
                with self.assertRaisesRegex(RuntimeError, "behind"):
                    restarted.publish_success(
                        identity=AuditPublicationIdentity(2, 5, BLOCK_B),
                        publication_floor_sequence=3,
                        report=self.report(block_height=5),
                        persistence=self.persistence(),
                        evidence={"confirmation": {"confirmed_count": 1}},
                        created_at="stale",
                    )
                self.assertFalse(
                    restarted.live_envelope_path(
                        block_height=5,
                        block_hash=BLOCK_B,
                    ).exists()
                )
                self.assertEqual(
                    restarted.evidence_path.read_bytes()
                    if restarted.evidence_path.exists()
                    else None,
                    evidence_before,
                )
                repaired = restarted.publish_success(
                    identity=identity,
                    publication_floor_sequence=3,
                    report=self.report(block_height=4),
                    persistence=self.persistence(),
                    evidence={"confirmation": {"confirmed_count": 1}},
                    created_at="repair",
                )
                self.assertTrue(repaired.published)
                self.assertTrue(first.envelope_path.exists())
                self.assertEqual(
                    json.loads(restarted.evidence_path.read_text(encoding="utf-8"))[
                        "block_hash"
                    ],
                    BLOCK_A,
                )

    def test_failed_invalid_evidence_repair_preserves_bytes_and_revokes_stale_pin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            identity = AuditPublicationIdentity(3, 4, BLOCK_A)
            first = store.publish_success(
                identity=identity,
                report=self.report(block_height=4),
                persistence=self.persistence(),
                evidence={"confirmation": {"confirmed_count": 1}},
                created_at="now",
            )
            latest_before = store.latest_evidence()
            envelope_before = first.envelope_path.read_bytes()
            corrupt_evidence = b'{"operator":"replacement"}'
            store.evidence_path.write_bytes(corrupt_evidence)
            original_fsync = store._fsync_directory
            failed = False

            def fail_evidence_parent(parent: Path) -> None:
                nonlocal failed
                if parent == store.evidence_path.parent and not failed:
                    failed = True
                    raise OSError("injected evidence durability failure")
                original_fsync(parent)

            with mock.patch.object(
                store,
                "_fsync_directory",
                side_effect=fail_evidence_parent,
            ), self.assertRaisesRegex(OSError, "evidence durability"):
                store.publish_success(
                    identity=identity,
                    publication_floor_sequence=3,
                    report=self.report(block_height=4),
                    persistence=self.persistence(),
                    evidence={"confirmation": {"confirmed_count": 1}},
                    created_at="later",
                )

            self.assertTrue(failed)
            self.assertEqual(store.evidence_path.read_bytes(), corrupt_evidence)
            self.assertEqual(first.envelope_path.read_bytes(), envelope_before)
            self.assertIsNotNone(latest_before)
            self.assertIsNone(store.latest_evidence())
            self.assertIsNone(store._current_envelope)
            self.assertIsNone(store._current_identity)

    def test_exact_replay_reuses_nonidentity_global_stats_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = AuditPublicationIdentity(3, 4, BLOCK_A)
            store = self.make_store(root)
            store.publish_success(
                identity=identity,
                report=self.report(block_height=4),
                persistence={**self.persistence(), "share_count": 10},
                evidence={
                    "confirmation": {"confirmed_count": 1},
                    "accepted_share_count": 10,
                    "distinct_miner_count": 2,
                    "job_share_count": 3,
                },
                created_at="now",
            )
            restarted = self.make_store(root)
            replay = restarted.publish_success(
                identity=identity,
                report=self.report(block_height=4),
                persistence={**self.persistence(), "share_count": 99},
                evidence={
                    "confirmation": {"confirmed_count": 1},
                    "accepted_share_count": 99,
                    "distinct_miner_count": 20,
                    "job_share_count": 3,
                },
                created_at="later",
            )
            self.assertFalse(replay.published)
            self.assertEqual(replay.evidence["accepted_share_count"], 10)
            self.assertEqual(replay.evidence["distinct_miner_count"], 2)
            self.assertEqual(replay.evidence["job_share_count"], 3)
            self.assertEqual(replay.evidence["persistence"]["share_count"], 10)  # type: ignore[index]
            with self.assertRaisesRegex(RuntimeError, "replay payload conflict"):
                restarted.publish_success(
                    identity=identity,
                    report=self.report(block_height=4),
                    persistence={**self.persistence(), "share_count": 100},
                    evidence={
                        "confirmation": {"confirmed_count": 1},
                        "accepted_share_count": 100,
                        "distinct_miner_count": 21,
                        "job_share_count": 4,
                    },
                    created_at="later",
                )

    def test_verification_identity_is_restart_stable_and_tamper_evident(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            report = self.report(block_height=4)
            verification = AuditArtifactStore.build_verification_identity(
                trust_source="configured",
                trusted_writer_public_key_hex="44" * 32,
                literal_sha256="99" * 32,
                literal_byte_len=456,
                report=report,
            )
            identity = AuditPublicationIdentity(3, 4, BLOCK_A)
            store.publish_success(
                identity=identity,
                report=report,
                persistence=self.persistence(),
                evidence={},
                verification_identity=verification,
                created_at="now",
            )
            restarted = self.make_store(root)
            latest = restarted.latest_evidence()
            assert latest is not None
            self.assertEqual(latest["audit_verification_identity"], verification)
            replay = restarted.publish_success(
                identity=identity,
                report=report,
                persistence=self.persistence(),
                evidence={},
                verification_identity=verification,
                created_at="later",
            )
            self.assertFalse(replay.published)

            changed_report = dict(report)
            changed_report["coinbase_wtxid"] = "aa" * 32
            variants = (
                (
                    report,
                    AuditArtifactStore.build_verification_identity(
                        trust_source="configured",
                        trusted_writer_public_key_hex="55" * 32,
                        literal_sha256="99" * 32,
                        literal_byte_len=456,
                        report=report,
                    ),
                ),
                (
                    report,
                    AuditArtifactStore.build_verification_identity(
                        trust_source="embedded_test_only",
                        trusted_writer_public_key_hex="44" * 32,
                        literal_sha256="99" * 32,
                        literal_byte_len=456,
                        report=report,
                    ),
                ),
                (
                    report,
                    AuditArtifactStore.build_verification_identity(
                        trust_source="configured",
                        trusted_writer_public_key_hex="44" * 32,
                        literal_sha256="aa" * 32,
                        literal_byte_len=456,
                        report=report,
                    ),
                ),
                (
                    report,
                    AuditArtifactStore.build_verification_identity(
                        trust_source="configured",
                        trusted_writer_public_key_hex="44" * 32,
                        literal_sha256="99" * 32,
                        literal_byte_len=457,
                        report=report,
                    ),
                ),
                (
                    changed_report,
                    AuditArtifactStore.build_verification_identity(
                        trust_source="configured",
                        trusted_writer_public_key_hex="44" * 32,
                        literal_sha256="99" * 32,
                        literal_byte_len=456,
                        report=changed_report,
                    ),
                ),
            )
            for variant_report, changed in variants:
                with self.subTest(changed=changed), self.assertRaisesRegex(
                    RuntimeError,
                    "replay payload conflict",
                ):
                    restarted.publish_success(
                        identity=identity,
                        report=variant_report,
                        persistence=self.persistence(),
                        evidence={},
                        verification_identity=changed,
                        created_at="later",
                    )

    def test_startup_rejects_each_verification_identity_tamper(self) -> None:
        cases = (
            ("trust_source", "embedded_test_only"),
            ("ledger_writer_public_key_hex", "55" * 32),
            ("literal_sha256_hex", "99" * 32),
            ("literal_byte_len", 999),
            ("identity_sha256_hex", "88" * 32),
        )
        for field, value in cases:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                store = self.make_store(root)
                store.publish_success(
                    identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                    report=self.report(),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="now",
                )
                payload = json.loads(store.evidence_path.read_text(encoding="utf-8"))
                payload["audit_verification_identity"][field] = value
                store.evidence_path.write_text(json.dumps(payload), encoding="utf-8")
                self.assertIsNone(self.make_store(root).latest_evidence())

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            payload = json.loads(store.evidence_path.read_text(encoding="utf-8"))
            payload["audit_verification_identity"]["report"]["coinbase_txid"] = (
                "99" * 32
            )
            store.evidence_path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertIsNone(self.make_store(root).latest_evidence())

    def test_higher_ordinal_replaces_same_height_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            store.publish_success(
                identity=AuditPublicationIdentity(1, 10, BLOCK_A),
                report=self.report(block_height=10),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            replacement = store.publish_success(
                identity=AuditPublicationIdentity(2, 10, BLOCK_B),
                report=self.report(block_height=10),
                persistence=self.persistence(),
                evidence={},
                created_at="later",
            )
            self.assertTrue(replacement.published)
            self.assertEqual(store.latest_evidence()["block_hash"], BLOCK_B)  # type: ignore[index]

    def test_publication_rejects_body_for_wrong_block_or_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            identity = AuditPublicationIdentity(1, 1, BLOCK_A)
            for body in (
                store.body_path(BLOCK_B, DIGEST),
                store.body_path(BLOCK_A, "77" * 32),
            ):
                with self.subTest(body=body), self.assertRaisesRegex(
                    RuntimeError,
                    "body URI does not match",
                ):
                    store.publish_success(
                        identity=identity,
                        report=self.report(block_height=1),
                        persistence={
                            "audit_bundle_sha256": DIGEST,
                            "body_uri": str(body),
                        },
                        evidence={},
                        created_at="now",
                    )

    def test_restart_preserves_publication_ordinal_fence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            store.publish_success(
                identity=AuditPublicationIdentity(5, 5, BLOCK_B),
                report=self.report(block_height=5),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            restarted = self.make_store(root)
            stale = restarted.publish_success(
                identity=AuditPublicationIdentity(4, 6, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="later",
            )
            self.assertFalse(stale.published)
            self.assertEqual(restarted.latest_evidence()["block_hash"], BLOCK_B)  # type: ignore[index]

    def test_invalid_evidence_can_be_repaired_by_durable_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "evidence.json").write_text("broken", encoding="utf-8")
            store = self.make_store(root)
            publication = store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            self.assertTrue(publication.published)
            self.assertEqual(self.make_store(root).latest_evidence()["block_hash"], BLOCK_A)  # type: ignore[index]

    def test_startup_rejects_coinbase_tampered_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            payload = json.loads(store.evidence_path.read_text(encoding="utf-8"))
            payload["coinbase_txid"] = "99" * 32
            store.evidence_path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertIsNone(self.make_store(root).latest_evidence())

    def test_startup_rejects_noncanonical_durable_identity_fields(self) -> None:
        canonical_digest = "ab" * 32

        def publish_fixture(root: Path, *, with_body: bool = False) -> AuditArtifactStore:
            store = self.make_store(root)
            persistence = self.persistence(canonical_digest)
            if with_body:
                persistence["body_uri"] = str(
                    store.body_path(BLOCK_A, canonical_digest)
                )
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(canonical_digest),
                persistence=persistence,
                evidence={},
                created_at="now",
            )
            return store

        cases = (
            ("height-string", lambda evidence, _envelope: evidence.__setitem__("block_height", "1")),
            ("height-bool", lambda evidence, _envelope: evidence.__setitem__("block_height", True)),
            ("sequence-string", lambda evidence, _envelope: evidence["audit_publication_identity"].__setitem__("sequence", "1")),
            ("sequence-bool", lambda evidence, _envelope: evidence["audit_publication_identity"].__setitem__("sequence", True)),
            ("coinbase-value-string", lambda evidence, _envelope: evidence.__setitem__("coinbase_value_sats", "1")),
            ("block-hash-uppercase", lambda evidence, _envelope: evidence.__setitem__("block_hash", BLOCK_A.upper())),
            ("persistence-digest-uppercase", lambda evidence, _envelope: evidence["persistence"].__setitem__("audit_bundle_sha256", canonical_digest.upper())),
            ("report-hash-uppercase", lambda evidence, _envelope: evidence["audit_report"].__setitem__("audit_bundle_sha256_hex", canonical_digest.upper())),
            ("envelope-height-string", lambda _evidence, envelope: envelope.__setitem__("block_height", "1")),
            ("envelope-value-bool", lambda _evidence, envelope: envelope.__setitem__("coinbase_value_sats", True)),
        )
        for name, mutate in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                store = publish_fixture(root)
                evidence = json.loads(store.evidence_path.read_text(encoding="utf-8"))
                envelope_path = Path(evidence["audit_bundle_path"])
                envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
                mutate(evidence, envelope)
                store.evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
                envelope_path.write_text(json.dumps(envelope), encoding="utf-8")
                self.assertIsNone(self.make_store(root).latest_evidence())

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = publish_fixture(root, with_body=True)
            evidence = json.loads(store.evidence_path.read_text(encoding="utf-8"))
            envelope_path = Path(evidence["audit_bundle_path"])
            envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
            (root / "sub").mkdir()
            body_name = Path(evidence["persistence"]["body_uri"]).name
            noncanonical = str(root / "sub" / ".." / body_name)
            evidence["persistence"]["body_uri"] = noncanonical
            envelope["body_uri"] = noncanonical
            store.evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
            envelope_path.write_text(json.dumps(envelope), encoding="utf-8")
            self.assertIsNone(self.make_store(root).latest_evidence())

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = publish_fixture(root)
            evidence = json.loads(store.evidence_path.read_text(encoding="utf-8"))
            original_cwd = Path.cwd()
            try:
                os.chdir(root)
                evidence["audit_bundle_path"] = Path(
                    evidence["audit_bundle_path"]
                ).name
                store.evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
                self.assertIsNone(self.make_store(root).latest_evidence())
            finally:
                os.chdir(original_cwd)

    def test_prune_failure_does_not_undo_durable_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            with mock.patch.object(store, "prune_best_effort", side_effect=OSError("prune")):
                result = store.publish_success(
                    identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                    report=self.report(block_height=1),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="now",
                )
            self.assertTrue(result.published)
            self.assertEqual(store.latest_evidence()["block_hash"], BLOCK_A)  # type: ignore[index]

    def test_evidence_failure_preserves_old_reference_and_skips_prune(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp), live_bundle_retention=0)
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={"audit_report": self.report(), "persistence": self.persistence()},
                created_at="old",
            )
            old = store.evidence_path.read_bytes()
            original = store._write_mutable_json

            def fail_evidence(path: Path, *args: object, **kwargs: object) -> object:
                if path == store.evidence_path:
                    raise OSError("evidence")
                return original(path, *args, **kwargs)

            with mock.patch.object(store, "_write_mutable_json", side_effect=fail_evidence):
                with self.assertRaises(OSError):
                    store.publish_success(
                        identity=AuditPublicationIdentity(2, 2, BLOCK_B),
                        report=self.report(block_height=2),
                        persistence=self.persistence(),
                        evidence={"audit_report": self.report(), "persistence": self.persistence()},
                        created_at="new",
                    )
            self.assertEqual(store.evidence_path.read_bytes(), old)
            self.assertTrue(
                store.live_envelope_path(block_height=1, block_hash=BLOCK_A).exists()
            )

    def test_publication_fsyncs_each_parent_in_commit_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "artifacts"
            evidence_path = base / "state" / "evidence.json"
            store = self.make_store(root, evidence_path=evidence_path)
            fsync_parents: list[Path] = []
            real_fsync = store._fsync_directory

            def record(parent: Path) -> None:
                fsync_parents.append(parent)
                real_fsync(parent)

            with mock.patch.object(
                store,
                "_fsync_directory",
                side_effect=record,
            ):
                store.publish_success(
                    identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                    report=self.report(),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="now",
                )

            self.assertGreaterEqual(len(fsync_parents), 2)
            self.assertEqual(fsync_parents[:2], [root.resolve(), evidence_path.parent.resolve()])

    def test_second_parent_fsync_failure_preserves_recoverable_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "artifacts"
            evidence_path = base / "state" / "evidence.json"
            store = self.make_store(root, evidence_path=evidence_path)
            fsync_parents: list[Path] = []
            real_fsync = store._fsync_directory
            failed = False

            def fail_evidence_parent_once(parent: Path) -> None:
                nonlocal failed
                fsync_parents.append(parent)
                if parent == evidence_path.parent.resolve() and not failed:
                    failed = True
                    raise OSError("evidence parent fsync")
                real_fsync(parent)

            envelope = store.live_envelope_path(
                block_height=1,
                block_hash=BLOCK_A,
            )
            with mock.patch.object(
                store,
                "_fsync_directory",
                side_effect=fail_evidence_parent_once,
            ), self.assertRaisesRegex(OSError, "evidence parent fsync"):
                store.publish_success(
                    identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                    report=self.report(),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="now",
                )

            self.assertEqual(
                fsync_parents[:2],
                [root.resolve(), evidence_path.parent.resolve()],
            )
            self.assertFalse(envelope.exists())
            self.assertFalse(evidence_path.exists())
            restarted = self.make_store(root, evidence_path=evidence_path)
            self.assertIsNone(restarted.latest_evidence())

    def test_evidence_failure_preserves_competing_envelope_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp), live_bundle_retention=0)
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={},
                created_at="old",
            )
            competitor_path = store.live_envelope_path(
                block_height=2,
                block_hash=BLOCK_B,
            )
            original = store._write_mutable_json

            def replace_then_fail(
                path: Path,
                *args: object,
                **kwargs: object,
            ) -> object:
                if path == store.evidence_path:
                    competitor_path.unlink()
                    competitor_path.write_bytes(b"competitor")
                    raise OSError("evidence")
                return original(path, *args, **kwargs)

            with mock.patch.object(
                store,
                "_write_mutable_json",
                side_effect=replace_then_fail,
            ), self.assertRaises(OSError):
                store.publish_success(
                    identity=AuditPublicationIdentity(2, 2, BLOCK_B),
                    report=self.report(block_height=2),
                    persistence=self.persistence(),
                    evidence={},
                    created_at="new",
                )

            self.assertEqual(competitor_path.read_bytes(), b"competitor")
            self.assertEqual(store.latest_evidence()["block_hash"], BLOCK_A)  # type: ignore[index]

    def test_directory_fsync_failure_rolls_back_previous_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            path = store.root / "operator.json"
            path.write_bytes(b"old")
            real = store._fsync_directory
            calls = 0

            def fail_once(parent: Path) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("dir fsync")
                real(parent)

            with mock.patch.object(store, "_fsync_directory", side_effect=fail_once):
                with self.assertRaises(OSError):
                    store._write_mutable_bytes(path, b"new")
            self.assertEqual(path.read_bytes(), b"old")

    def test_directory_fsync_rollback_preserves_competing_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            path = store.root / "mutable.json"
            path.write_bytes(b"old")
            real = store._fsync_directory
            calls = 0

            def swap_then_fail(parent: Path) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    path.unlink()
                    path.write_bytes(b"competitor")
                    raise OSError("dir fsync")
                real(parent)

            with mock.patch.object(store, "_fsync_directory", side_effect=swap_then_fail):
                with self.assertRaises(OSError):
                    store._write_mutable_bytes(path, b"new")
            self.assertEqual(path.read_bytes(), b"competitor")

    def test_identity_cleanup_never_moves_an_existing_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            path = root / "owned.json"
            path.write_bytes(b"owned-a")
            owned = _FileIdentity.from_stat(path.stat())
            path.unlink()
            path.write_bytes(b"competitor-b")
            replacement = path.stat()
            # Force the Linux ABA case even on filesystems that do not
            # immediately reuse the unlinked inode in this test.
            owned = dataclass_replace(
                owned,
                device=replacement.st_dev,
                inode=replacement.st_ino,
            )
            with mock.patch.object(
                store,
                "_owned_replace",
                side_effect=AssertionError("replacement must not move"),
            ):
                self.assertFalse(store._remove_identity_safe(path, owned))

            self.assertEqual(path.read_bytes(), b"competitor-b")
            quarantines = list(root.glob(".owned.json.*.cleanup"))
            self.assertEqual(quarantines, [])

    def test_identity_cleanup_never_relocates_nonregular_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            path = root / "owned.json"
            path.write_bytes(b"owned")
            owned = _FileIdentity.from_stat(path.stat())
            path.unlink()
            path.mkdir()

            self.assertFalse(store._remove_identity_safe(path, owned))
            self.assertTrue(path.is_dir())
            self.assertEqual(list(root.glob(".owned.json.*.cleanup")), [])

            path.rmdir()
            target = root / "operator"
            target.write_bytes(b"operator")
            path.symlink_to(target)
            self.assertFalse(store._remove_identity_safe(path, owned))
            self.assertTrue(path.is_symlink())
            self.assertEqual(path.read_bytes(), b"operator")

    def test_atomic_stage_faults_preserve_target_and_clean_owned_temps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            path = store.root / "mutable.json"
            path.write_bytes(b"old")
            with mock.patch("os.fsync", side_effect=OSError("file fsync")):
                with self.assertRaises(OSError):
                    store._write_mutable_bytes(path, b"new")
            self.assertEqual(path.read_bytes(), b"old")
            self.assertEqual(list(store.root.glob(".*.tmp")), [])

            real_replace = os.replace

            def fail_publish(
                source: object,
                target: object,
                **kwargs: object,
            ) -> None:
                target_path = Path(target)
                if kwargs.get("dst_dir_fd") == store._root_fd:
                    target_path = store.root / target_path
                if target_path == path:
                    raise OSError("replace")
                real_replace(source, target, **kwargs)

            with mock.patch("os.replace", side_effect=fail_publish):
                with self.assertRaises(OSError):
                    store._write_mutable_bytes(path, b"new")
            self.assertEqual(path.read_bytes(), b"old")
            self.assertEqual(list(store.root.glob(".*.tmp")), [])

            token = "66" * 16
            stage = store.root / f".{path.name}.{token}.tmp"
            stage.write_bytes(b"preexisting")
            with mock.patch(
                "lab.prism.audit_artifacts.uuid.uuid4",
                return_value=mock.Mock(hex=token),
            ):
                with self.assertRaises(FileExistsError):
                    store._write_mutable_bytes(path, b"new")
            self.assertEqual(stage.read_bytes(), b"preexisting")
            self.assertEqual(path.read_bytes(), b"old")

    def test_immutable_primitive_rejects_arbitrary_owned_root_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            target = store.root / "operator-selected.json"
            with self.assertRaisesRegex(RuntimeError, "owned body or segment"):
                store._write_immutable_bytes(target, b"payload")
            self.assertFalse(target.exists())

    def test_immutable_publish_fsync_failure_is_recoverable_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            target = store.body_path(BLOCK_A, DIGEST)
            real_fsync = store._fsync_directory
            calls = 0

            def fail_once(parent: Path) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("dir fsync")
                real_fsync(parent)

            with mock.patch.object(
                store,
                "_fsync_directory",
                side_effect=fail_once,
            ), self.assertRaisesRegex(OSError, "dir fsync"):
                store._write_immutable_bytes(target, b"payload")
            self.assertFalse(target.exists())
            self.assertEqual(list(store.root.glob(".*.tmp")), [])

            store._write_immutable_bytes(target, b"payload")
            with mock.patch.object(
                store,
                "_fsync_directory",
                wraps=real_fsync,
            ) as fsync:
                store._write_immutable_bytes(target, b"payload")
            fsync.assert_called_once_with(target.parent)
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                store._write_immutable_bytes(target, b"different")
            self.assertEqual(target.read_bytes(), b"payload")

    def test_compatibility_none_override_is_stable_and_defensive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={"nested": {"value": 1}},
                created_at="now",
            )
            store.set_latest_evidence_for_compatibility(None)
            self.assertIsNone(store.latest_evidence())
            seed = {"nested": {"value": 2}}
            store.set_latest_evidence_for_compatibility(seed)
            seed["nested"]["value"] = 3
            first = store.latest_evidence()
            assert first is not None
            self.assertEqual(first["nested"]["value"], 2)
            first["nested"]["value"] = 4
            self.assertEqual(store.latest_evidence()["nested"]["value"], 2)  # type: ignore[index]

    def test_compact_body_and_segment_headers_are_identity_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(
                Path(tmp),
                canonicalizer=lambda value: json.dumps(
                    value,
                    separators=(",", ":"),
                ).encode(),
                share_segment_size=2,
            )
            logical = {"schema": "example", "shares": [{"share_seq": 1}]}
            digest = hashlib.sha256(
                json.dumps(logical, separators=(",", ":")).encode()
            ).hexdigest()
            body_path = store.body_path(BLOCK_A, digest)
            wrapper = {
                "schema": "qbit.prism.audit-body-ref.v1",
                "block_hash": BLOCK_B,
                "audit_bundle_sha256": digest,
                "audit_bundle_schema": "example",
                "share_count": 1,
                "share_segment_size": 2,
                "shares_key_index": 1,
                "bundle_without_shares": {"schema": "example"},
                "share_parts": [{
                    "kind": "inline",
                    "first_share_seq": 1,
                    "last_share_seq": 1,
                    "share_count": 1,
                    "shares": [{"share_seq": 1}],
                }],
            }
            body_path.write_bytes(store.storage_json_bytes(wrapper))
            with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
                store.read_external_body(str(body_path), expected_sha256=digest)

            segment = {
                "schema": "qbit.prism.audit-share-segment.v1",
                "first_share_seq": 1,
                "last_share_seq": 2,
                "share_count": 2,
                "shares": [{"share_seq": 1}],
            }
            segment_bytes = store.storage_json_bytes(segment)
            segment_digest = hashlib.sha256(segment_bytes).hexdigest()
            segment_path = store.root / (
                f"prism-audit-share-segment-1-2-{segment_digest}.json"
            )
            segment_path.write_bytes(segment_bytes)
            with self.assertRaisesRegex(RuntimeError, "header mismatch"):
                store.read_audit_share_segment(
                    {
                        "kind": "segment",
                        "first_share_seq": 1,
                        "last_share_seq": 2,
                        "share_count": 2,
                        "sha256": segment_digest,
                        "body_uri": str(segment_path),
                    },
                    parent_body_uri=str(body_path),
                )

    def test_memory_ledger_reactivation_preserves_publication_ordinal(self) -> None:
        ledger = SingleWriterShareLedger()
        self.assertEqual(ledger.audit_publication_sequence_floor(), 0)

        def persist(block_hash: str, height: int) -> None:
            ledger.persist_accepted_block(
                block_hash=block_hash,
                block_height=height,
                parent_hash=BLOCK_B,
                final_bundle={},
                audit_report={},
            )

        persist(BLOCK_A, 1)
        self.assertEqual(
            ledger.pool_block_state(block_hash=BLOCK_A)["chain_state"],  # type: ignore[index]
            "prepared",
        )
        first = ledger.confirm_accepted_block(block_hash=BLOCK_A, active_tip_height=1)
        replay = ledger.confirm_accepted_block(block_hash=BLOCK_A, active_tip_height=1)
        self.assertEqual(ledger.audit_publication_sequence_floor(), 1)
        self.assertEqual(
            first["audit_publication_sequence"],
            replay["audit_publication_sequence"],
        )
        persist(BLOCK_A, 1)
        self.assertEqual(
            ledger.pool_block_state(block_hash=BLOCK_A)["chain_state"],  # type: ignore[index]
            "confirmed",
        )
        self.assertEqual(
            ledger.mark_pool_block_inactive(block_hash=BLOCK_A, active_tip_height=2)[
                "inactive_count"
            ],
            1,
        )
        persist(BLOCK_A, 1)
        inactive_state = ledger.pool_block_state(block_hash=BLOCK_A)
        self.assertEqual(inactive_state["chain_state"], "inactive")  # type: ignore[index]
        self.assertEqual(
            inactive_state["audit_publication_sequence"],  # type: ignore[index]
            first["audit_publication_sequence"],
        )
        wrong_height = ledger.reactivate_pool_block(
            block_hash=BLOCK_A,
            active_tip_height=0,
        )
        self.assertEqual(wrong_height["reactivated_count"], 0)
        self.assertNotIn("audit_publication_sequence", wrong_height)
        reactivated = ledger.reactivate_pool_block(
            block_hash=BLOCK_A,
            active_tip_height=1,
        )
        self.assertEqual(
            reactivated["audit_publication_sequence"],
            first["audit_publication_sequence"],
        )
        self.assertEqual(ledger.audit_publication_sequence_floor(), 1)
        replay_reactivation = ledger.reactivate_pool_block(
            block_hash=BLOCK_A,
            active_tip_height=1,
        )
        self.assertEqual(replay_reactivation["reactivated_count"], 0)
        self.assertNotIn("audit_publication_sequence", replay_reactivation)
        self.assertEqual(
            ledger.mark_pool_block_inactive(
                block_hash=BLOCK_A,
                active_tip_height=2,
            )["inactive_count"],
            1,
        )
        self.assertEqual(
            ledger.reverse_immature_block(
                block_hash=BLOCK_A,
                active_tip_height=2,
            )["reversed_count"],
            1,
        )
        persist(BLOCK_A, 1)
        reversed_state = ledger.pool_block_state(block_hash=BLOCK_A)
        self.assertEqual(reversed_state["chain_state"], "reversed")  # type: ignore[index]
        self.assertEqual(reversed_state["maturity_state"], "reversed")  # type: ignore[index]
        self.assertEqual(
            reversed_state["audit_publication_sequence"],  # type: ignore[index]
            reactivated["audit_publication_sequence"],
        )
        self.assertEqual(
            ledger.confirm_accepted_block(
                block_hash=BLOCK_A,
                active_tip_height=1,
            ),
            {"backend": "memory", "confirmed_count": 0},
        )
        self.assertEqual(
            ledger.reactivate_pool_block(
                block_hash=BLOCK_A,
                active_tip_height=1,
            ),
            {"backend": "memory", "reactivated_count": 0},
        )
        persist(BLOCK_B, 2)
        next_publication = ledger.confirm_accepted_block(
            block_hash=BLOCK_B,
            active_tip_height=2,
        )
        self.assertEqual(next_publication["audit_publication_sequence"], 2)
        self.assertEqual(ledger.audit_publication_sequence_floor(), 2)
        with self.assertRaisesRegex(RuntimeError, "mature pool block"):
            ledger.reverse_immature_block(
                block_hash=BLOCK_B,
                active_tip_height=1002,
            )
        rejected_hash = "cc" * 32
        persist(rejected_hash, 3)
        self.assertEqual(
            ledger.reject_prepared_block(
                block_hash=rejected_hash,
                active_tip_height=3,
            )["rejected_count"],
            1,
        )
        self.assertEqual(
            ledger.reject_prepared_block(
                block_hash=rejected_hash,
                active_tip_height=3,
            )["rejected_count"],
            0,
        )
        rejected_state = ledger.pool_block_state(block_hash=rejected_hash)
        self.assertEqual(rejected_state["chain_state"], "rejected")  # type: ignore[index]
        self.assertEqual(rejected_state["maturity_state"], "reversed")  # type: ignore[index]

    def test_memory_ledger_rejects_unknown_wrong_height_and_inactive_confirmation(self) -> None:
        ledger = SingleWriterShareLedger()
        self.assertEqual(
            ledger.confirm_accepted_block(
                block_hash=BLOCK_A,
                active_tip_height=1,
            )["confirmed_count"],
            0,
        )
        ledger.persist_accepted_block(
            block_hash=BLOCK_A,
            block_height=1,
            parent_hash=BLOCK_B,
            final_bundle={},
            audit_report={},
        )
        self.assertEqual(
            ledger.confirm_accepted_block(
                block_hash=BLOCK_A,
                active_tip_height=2,
            )["confirmed_count"],
            0,
        )
        ledger.confirm_accepted_block(block_hash=BLOCK_A, active_tip_height=1)
        self.assertEqual(
            ledger.mark_pool_block_inactive(
                block_hash=BLOCK_A,
                active_tip_height=2,
            )["inactive_count"],
            1,
        )
        self.assertEqual(
            ledger.mark_pool_block_inactive(
                block_hash=BLOCK_A,
                active_tip_height=2,
            )["inactive_count"],
            0,
        )
        inactive_confirmation = ledger.confirm_accepted_block(
            block_hash=BLOCK_A,
            active_tip_height=1,
        )
        self.assertEqual(inactive_confirmation["confirmed_count"], 0)
        self.assertNotIn("audit_publication_sequence", inactive_confirmation)

    def test_invalid_evidence_disables_live_prune_but_not_candidate_prune(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "evidence.json").write_text("not-json", encoding="utf-8")
            live = root / f"prism-live-audit-bundle-1-{BLOCK_A}.json"
            live.write_text("{}", encoding="utf-8")
            candidate = root / f"prism-live-audit-bundle-candidate-{BLOCK_A}.json"
            candidate.write_text("{}", encoding="utf-8")
            store = self.make_store(
                root,
                live_bundle_retention=0,
                candidate_retention_seconds=0,
            )
            result = store.prune_best_effort()
            self.assertTrue(live.exists())
            self.assertFalse(candidate.exists())
            self.assertEqual(result.live_removed, 0)
            self.assertEqual(result.candidate_removed, 1)

    def test_legacy_evidence_requires_durable_identity_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            envelope = store.live_envelope_path(block_height=2, block_hash=BLOCK_B)
            envelope.write_text(
                json.dumps(
                    {
                        "schema": LIVE_ENVELOPE_SCHEMA,
                        "block_hash": BLOCK_B,
                        "block_height": 2,
                        "audit_bundle_sha256": DIGEST,
                        "body_uri": "",
                        "body_filename": None,
                        "coinbase_txid": "22" * 32,
                        "coinbase_manifest_sha256": "33" * 32,
                        "coinbase_tx_hex": "00",
                        "coinbase_value_sats": 1,
                    }
                ),
                encoding="utf-8",
            )
            legacy = {
                "schema": LIVE_EVIDENCE_SCHEMA,
                "block_hash": BLOCK_B,
                "block_height": 2,
                "audit_bundle_path": str(envelope),
                "audit_report": self.report(block_height=2),
                "persistence": self.persistence(),
                "coinbase_txid": "22" * 32,
                "coinbase_manifest_sha256_hex": "33" * 32,
                "coinbase_tx_hex": "00",
                "coinbase_value_sats": 1,
            }
            store.evidence_path.write_text(json.dumps(legacy), encoding="utf-8")
            restarted = self.make_store(root)
            with self.assertRaisesRegex(RuntimeError, "validated publication identity"):
                restarted.publish_success(
                    identity=AuditPublicationIdentity(10, 1, BLOCK_A),
                    report=self.report(),
                    persistence=self.persistence(),
                    evidence={"audit_report": self.report(), "persistence": self.persistence()},
                    created_at="later",
                )
            with restarted.publication_order_guard():
                restarted.adopt_legacy_publication_identity(
                    AuditPublicationIdentity(20, 2, BLOCK_B),
                    publication_floor_sequence=20,
                )
            with self.assertRaisesRegex(RuntimeError, "never exact-replay"):
                restarted.publish_success(
                    identity=AuditPublicationIdentity(10, 1, BLOCK_A),
                    report=self.report(block_height=1),
                    persistence=self.persistence(),
                    evidence={
                        "audit_report": self.report(),
                        "persistence": self.persistence(),
                    },
                    created_at="later",
                )

            # The disk marker never grants order or pin authority by itself.
            second_process = self.make_store(root)
            self.assertIsNone(second_process.latest_evidence())
            self.assertEqual(second_process.publication_sequence_floor(), 0)
            self.assertEqual(
                second_process.legacy_evidence_identity(),
                AuditPublicationIdentity(20, 2, BLOCK_B),
            )
            with second_process.publication_order_guard():
                second_process.adopt_legacy_publication_identity(
                    AuditPublicationIdentity(20, 2, BLOCK_B),
                    publication_floor_sequence=20,
                )
            self.assertEqual(second_process.publication_sequence_floor(), 20)
            self.assertEqual(
                second_process.latest_evidence()["block_hash"],  # type: ignore[index]
                BLOCK_B,
            )
            for stale_identity in (
                AuditPublicationIdentity(20, 2, BLOCK_B),
                AuditPublicationIdentity(19, 3, BLOCK_A),
            ):
                with self.subTest(stale_identity=stale_identity), self.assertRaisesRegex(
                    RuntimeError,
                    "never exact-replay",
                ):
                    second_process.publish_success(
                        identity=stale_identity,
                        report=self.report(block_height=stale_identity.block_height),
                        persistence=self.persistence(),
                        evidence={},
                        created_at="stale",
                    )
            repair = second_process.publish_success(
                identity=AuditPublicationIdentity(21, 3, BLOCK_A),
                report=self.report(block_height=3),
                persistence=self.persistence(),
                evidence={},
                created_at="repair",
            )
            self.assertTrue(repair.published)

    def test_legacy_proof_token_is_revoked_by_peer_inode_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            seed = self.make_store(root)
            envelope = seed.live_envelope_path(
                block_height=2,
                block_hash=BLOCK_B,
            )
            envelope.write_text(
                json.dumps(
                    {
                        "schema": LIVE_ENVELOPE_SCHEMA,
                        "block_hash": BLOCK_B,
                        "block_height": 2,
                        "audit_bundle_sha256": DIGEST,
                        "body_uri": "",
                        "body_filename": None,
                        "coinbase_txid": "22" * 32,
                        "coinbase_manifest_sha256": "33" * 32,
                        "coinbase_tx_hex": "00",
                        "coinbase_value_sats": 1,
                    }
                ),
                encoding="utf-8",
            )
            seed.evidence_path.write_text(
                json.dumps(
                    {
                        "schema": LIVE_EVIDENCE_SCHEMA,
                        "block_hash": BLOCK_B,
                        "block_height": 2,
                        "audit_bundle_path": str(envelope),
                        "audit_report": self.report(block_height=2),
                        "persistence": self.persistence(),
                        "coinbase_txid": "22" * 32,
                        "coinbase_manifest_sha256_hex": "33" * 32,
                        "coinbase_tx_hex": "00",
                        "coinbase_value_sats": 1,
                    }
                ),
                encoding="utf-8",
            )
            seed.close()
            identity = AuditPublicationIdentity(20, 2, BLOCK_B)
            proven = self.make_store(root)
            with proven.publication_order_guard():
                proven.adopt_legacy_publication_identity(
                    identity,
                    publication_floor_sequence=20,
                )
            self.assertIsNotNone(proven._legacy_proof_token)
            self.assertEqual(proven.publication_sequence_floor(), 20)
            original_evidence_inode = _FileIdentity.from_stat(
                proven.evidence_path.stat()
            )

            peer = self.make_store(root)
            with peer.publication_order_guard():
                peer.adopt_legacy_publication_identity(
                    identity,
                    publication_floor_sequence=20,
                )
            self.assertFalse(
                original_evidence_inode.matches(proven.evidence_path.stat())
            )

            proven.prune_best_effort()
            self.assertIsNone(proven._legacy_proof_token)
            self.assertIsNone(proven.latest_evidence())
            self.assertEqual(proven.publication_sequence_floor(), 0)
            self.assertEqual(proven.legacy_evidence_identity(), identity)

    def test_unprovable_legacy_evidence_is_repairable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp))
            store._evidence_state = "legacy"
            store._latest_evidence = {"block_hash": BLOCK_A, "block_height": 1}
            store._current_identity = AuditPublicationIdentity(0, 1, BLOCK_A)
            store.invalidate_unprovable_legacy_evidence()
            repaired = store.publish_success(
                identity=AuditPublicationIdentity(2, 2, BLOCK_B),
                report=self.report(block_height=2),
                persistence=self.persistence(),
                evidence={},
                created_at="now",
            )
            self.assertTrue(repaired.published)

    def test_disk_legacy_invalidation_is_sticky_until_durable_repair(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root, live_bundle_retention=0)
            envelope = store.live_envelope_path(block_height=2, block_hash=BLOCK_B)
            envelope.write_text(
                json.dumps(
                    {
                        "schema": LIVE_ENVELOPE_SCHEMA,
                        "block_hash": BLOCK_B,
                        "block_height": 2,
                        "audit_bundle_sha256": DIGEST,
                        "body_uri": "",
                        "body_filename": None,
                        "coinbase_txid": "22" * 32,
                        "coinbase_manifest_sha256": "33" * 32,
                        "coinbase_tx_hex": "00",
                        "coinbase_value_sats": 1,
                    }
                ),
                encoding="utf-8",
            )
            legacy = {
                "schema": LIVE_EVIDENCE_SCHEMA,
                "block_hash": BLOCK_B,
                "block_height": 2,
                "audit_bundle_path": str(envelope),
                "audit_report": self.report(block_height=2),
                "persistence": self.persistence(),
                "coinbase_txid": "22" * 32,
                "coinbase_manifest_sha256_hex": "33" * 32,
                "coinbase_tx_hex": "00",
                "coinbase_value_sats": 1,
            }
            store.evidence_path.write_text(json.dumps(legacy), encoding="utf-8")
            restarted = self.make_store(root, live_bundle_retention=0)
            restarted.invalidate_unprovable_legacy_evidence()
            self.assertIsNone(restarted.latest_evidence())
            self.assertEqual(restarted.prune_best_effort().live_removed, 0)
            repaired = restarted.publish_success(
                identity=AuditPublicationIdentity(21, 3, BLOCK_A),
                report=self.report(block_height=3),
                persistence=self.persistence(),
                evidence={},
                created_at="repair",
            )
            self.assertTrue(repaired.published)
            self.assertEqual(restarted.latest_evidence()["block_hash"], BLOCK_A)  # type: ignore[index]

    def test_retention_preserves_current_and_unowned_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root, live_bundle_retention=0)
            publication = store.publish_success(
                identity=AuditPublicationIdentity(1, 1, BLOCK_A),
                report=self.report(),
                persistence=self.persistence(),
                evidence={"audit_report": self.report(), "persistence": self.persistence()},
                created_at="now",
            )
            lookalike = root / f"prism-live-audit-bundle-1-{BLOCK_B}.json.bak"
            lookalike.write_text("operator", encoding="utf-8")
            link = root / f"prism-live-audit-bundle-2-{BLOCK_B}.json"
            link.symlink_to(lookalike)
            store.prune_best_effort()
            self.assertTrue(publication.envelope_path.exists())
            self.assertTrue(lookalike.exists())
            self.assertTrue(link.is_symlink())

    def test_retention_ties_pin_active_candidate_and_never_remove_bodies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(
                root,
                live_bundle_retention=2,
                candidate_retention_seconds=0,
            )
            current = store.publish_success(
                identity=AuditPublicationIdentity(1, 0, BLOCK_A),
                report=self.report(block_height=0),
                persistence=self.persistence(),
                evidence={},
                created_at="current",
            )
            first = store.live_envelope_path(block_height=1, block_hash=BLOCK_A)
            second = store.live_envelope_path(block_height=2, block_hash=BLOCK_B)
            first.write_text("{}", encoding="utf-8")
            second.write_text("{}", encoding="utf-8")
            os.utime(first, ns=(1, 1))
            os.utime(second, ns=(1, 1))
            active = store.issue_candidate(block_hash=BLOCK_A)
            active.path.write_bytes(b"active")
            self.transfer_candidate(store, active)
            body = store.body_path(BLOCK_A, DIGEST)
            body.write_bytes(b"body")
            result = store.prune_best_effort()
            self.assertEqual(result.live_removed, 1)
            self.assertTrue(current.envelope_path.exists())
            self.assertTrue(active.path.exists())
            self.assertTrue(body.exists())
            store.discard_candidate(active)

    def test_body_segment_and_evidence_symlinks_fail_no_follow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            outside = root / "outside"
            outside.write_text("{}", encoding="utf-8")
            body = store.body_path(BLOCK_A, DIGEST)
            body.symlink_to(outside)
            with self.assertRaisesRegex(RuntimeError, "not retrievable"):
                store.read_external_body(str(body), expected_sha256=DIGEST)
            store.evidence_path.symlink_to(outside)
            self.assertIsNone(self.make_store(root).latest_evidence())

    def test_metrics_ignore_symlinks_and_classify_malformed_as_other(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            body = root / f"prism-audit-bundle-body-{BLOCK_A}-{DIGEST}.json"
            body.write_bytes(b"body")
            malformed = root / "prism-audit-bundle-body-operator.json"
            malformed.write_bytes(b"x")
            (root / f"prism-live-audit-bundle-1-{BLOCK_A}.json").symlink_to(body)
            metrics = store.metrics_snapshot()
            self.assertEqual(metrics["body"], {"files": 1, "bytes": 4})
            self.assertEqual(metrics["other"], {"files": 1, "bytes": 1})

    def test_mutable_share_slot_merge_preserves_prior_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(Path(tmp), share_segment_size=4)
            uri, _digest = store.write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=4,
                first_share_seq=1,
                last_share_seq=2,
                shares=[{"share_seq": 1}, {"share_seq": 2}],
            )
            store.write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=4,
                first_share_seq=3,
                last_share_seq=4,
                shares=[{"share_seq": 3}, {"share_seq": 4}],
            )
            payload = json.loads(Path(uri).read_text(encoding="utf-8"))
            self.assertEqual([row["share_seq"] for row in payload["shares"]], [1, 2, 3, 4])

    def test_concurrent_mutable_share_slot_merge_is_serialized_and_lossless(
        self,
    ) -> None:
        rows = [
            {
                "share_seq": share_seq,
                "worker": f"miner-{share_seq}",
                "accepted": True,
            }
            for share_seq in range(1, 5)
        ]
        scenarios = (
            ("disjoint", rows[:2], rows[2:]),
            ("identical_overlap", rows[:3], rows[1:]),
        )

        for scenario, first_shares, second_shares in scenarios:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as tmp:
                store = self.make_store(Path(tmp), share_segment_size=4)
                first_name = f"audit-slot-{scenario}-first"
                second_name = f"audit-slot-{scenario}-second"
                first_at_write = threading.Event()
                release_first = threading.Event()
                second_lock_attempt = threading.Event()
                second_lock_acquired = threading.Event()
                second_at_write = threading.Event()
                write_call_lock = threading.Lock()
                write_call_threads: list[str] = []
                results: dict[str, tuple[str, str]] = {}
                errors: dict[str, BaseException] = {}
                original_lock = store._lock
                original_write_mutable_bytes = store._write_mutable_bytes

                class ObservedLock:
                    def __enter__(self) -> "ObservedLock":
                        if threading.current_thread().name == second_name:
                            second_lock_attempt.set()
                        original_lock.acquire()
                        if threading.current_thread().name == second_name:
                            second_lock_acquired.set()
                        return self

                    def __exit__(
                        self,
                        _exc_type: object,
                        _exc_value: object,
                        _traceback: object,
                    ) -> None:
                        original_lock.release()

                def gated_write(path: Path, payload: bytes) -> None:
                    thread_name = threading.current_thread().name
                    with write_call_lock:
                        write_call_threads.append(thread_name)
                    if thread_name == first_name:
                        first_at_write.set()
                        if not release_first.wait(timeout=5.0):
                            raise AssertionError("first share-slot writer was not released")
                    elif thread_name == second_name:
                        second_at_write.set()
                    original_write_mutable_bytes(path, payload)

                def write_range(label: str, shares: list[dict[str, object]]) -> None:
                    try:
                        results[label] = store.write_audit_share_segment_range(
                            segment_first_share_seq=1,
                            segment_last_share_seq=4,
                            first_share_seq=int(shares[0]["share_seq"]),
                            last_share_seq=int(shares[-1]["share_seq"]),
                            shares=shares,
                        )
                    except BaseException as exc:
                        errors[label] = exc

                first_thread = threading.Thread(
                    target=write_range,
                    args=("first", first_shares),
                    name=first_name,
                )
                second_thread = threading.Thread(
                    target=write_range,
                    args=("second", second_shares),
                    name=second_name,
                )
                second_started = False
                with mock.patch.object(
                    store,
                    "_lock",
                    ObservedLock(),
                ), mock.patch.object(
                    store,
                    "_write_mutable_bytes",
                    side_effect=gated_write,
                ):
                    first_thread.start()
                    try:
                        self.assertTrue(
                            first_at_write.wait(timeout=5.0),
                            "first writer did not reach the gated mutable write",
                        )
                        second_thread.start()
                        second_started = True
                        self.assertTrue(
                            second_lock_attempt.wait(timeout=5.0),
                            "second writer did not attempt the store lock",
                        )
                        self.assertTrue(first_thread.is_alive())
                        self.assertTrue(second_thread.is_alive())
                        self.assertFalse(second_lock_acquired.is_set())
                        self.assertFalse(second_at_write.is_set())
                    finally:
                        release_first.set()
                        first_thread.join(timeout=5.0)
                        if second_started:
                            second_thread.join(timeout=5.0)

                self.assertFalse(first_thread.is_alive())
                self.assertFalse(second_thread.is_alive())
                active_names = {thread.name for thread in threading.enumerate()}
                self.assertNotIn(first_name, active_names)
                self.assertNotIn(second_name, active_names)
                self.assertEqual(errors, {})
                self.assertTrue(second_lock_acquired.is_set())
                self.assertTrue(second_at_write.is_set())
                self.assertEqual(write_call_threads, [first_name, second_name])

                self.assertEqual(results["first"][0], results["second"][0])
                slot_path = Path(results["first"][0])
                expected_payload = store.audit_share_segment_payload(
                    first_share_seq=1,
                    last_share_seq=4,
                    shares=rows,
                )
                expected_bytes = store.storage_json_bytes(expected_payload)
                stored_bytes = slot_path.read_bytes()
                self.assertEqual(stored_bytes, expected_bytes)
                stored = json.loads(stored_bytes)
                sequences = [int(row["share_seq"]) for row in stored["shares"]]
                self.assertEqual(sequences, [1, 2, 3, 4])
                self.assertEqual(len(sequences), len(set(sequences)))
                self.assertEqual(stored["shares"], rows)

                inputs = {"first": first_shares, "second": second_shares}
                for label, shares in inputs.items():
                    incoming_payload = store.audit_share_segment_payload(
                        first_share_seq=int(shares[0]["share_seq"]),
                        last_share_seq=int(shares[-1]["share_seq"]),
                        shares=shares,
                    )
                    expected_digest = hashlib.sha256(
                        store.storage_json_bytes(incoming_payload)
                    ).hexdigest()
                    self.assertEqual(results[label][1], expected_digest)

                before_retry = slot_path.stat()
                for label, shares in inputs.items():
                    retry = store.write_audit_share_segment_range(
                        segment_first_share_seq=1,
                        segment_last_share_seq=4,
                        first_share_seq=int(shares[0]["share_seq"]),
                        last_share_seq=int(shares[-1]["share_seq"]),
                        shares=shares,
                    )
                    self.assertEqual(retry, results[label])
                after_retry = slot_path.stat()
                self.assertEqual(slot_path.read_bytes(), expected_bytes)
                self.assertEqual(
                    (after_retry.st_dev, after_retry.st_ino, after_retry.st_mtime_ns),
                    (before_retry.st_dev, before_retry.st_ino, before_retry.st_mtime_ns),
                )

                conflicting_share = {**rows[1], "worker": "conflicting-miner"}
                with self.assertRaisesRegex(
                    RuntimeError,
                    "conflicts at share_seq 2",
                ):
                    store.write_audit_share_segment_range(
                        segment_first_share_seq=1,
                        segment_last_share_seq=4,
                        first_share_seq=2,
                        last_share_seq=2,
                        shares=[conflicting_share],
                    )
                after_conflict = slot_path.stat()
                self.assertEqual(slot_path.read_bytes(), expected_bytes)
                self.assertEqual(
                    (
                        after_conflict.st_dev,
                        after_conflict.st_ino,
                        after_conflict.st_mtime_ns,
                    ),
                    (after_retry.st_dev, after_retry.st_ino, after_retry.st_mtime_ns),
                )


if __name__ == "__main__":
    unittest.main()

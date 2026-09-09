#!/usr/bin/env python3
"""Audit root enumeration through a fresh directory description.

Some kernels and overlay filesystems serve a stale or truncated ``readdir``
through a long-lived directory descriptor once the directory has been
mutated after its first enumeration. Observed on an aarch64 Docker host: a
conflict snapshot written moments earlier was absent from the next scan
through the pinned root descriptor, so quarantine deduplication created a
second copy and metrics and retention saw an empty root. The store now
reopens ``.`` relative to the pinned descriptor for every scan, keeping the
inode authority while never trusting the pinned description's own offset
or cache.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from typing import Any

from lab.prism.audit_artifacts import AuditArtifactConfig, AuditArtifactStore


BLOCK_HASH = "a1" * 32


class FreshDirectoryScanTests(unittest.TestCase):
    """Root enumeration never trusts the pinned descriptor's own readdir."""

    def make_store(self, root: Path) -> AuditArtifactStore:
        store = AuditArtifactStore(
            AuditArtifactConfig(
                root=root,
                evidence_path=root / "evidence.json",
                share_segment_size=4,
                candidate_retention_seconds=0,
            ),
            canonicalizer=lambda bundle: json.dumps(bundle, separators=(",", ":")).encode(),
        )
        self.addCleanup(store.close)
        return store

    def test_scans_survive_a_stale_pinned_descriptor_enumeration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            real_listdir = os.listdir
            stale_reads = 0

            def stale_listdir(target: Any = None) -> list[str]:
                nonlocal stale_reads
                if isinstance(target, int) and target == store._root_fd:
                    # The defect this guards against: a long-lived directory
                    # description that stops reporting entries created after
                    # its first enumeration.
                    stale_reads += 1
                    return []
                return real_listdir(target)

            rows = [{"share_seq": seq, "worker": f"miner-{seq}"} for seq in (1, 2)]
            with mock.patch("lab.prism.audit_artifacts.os.listdir", side_effect=stale_listdir):
                uri, _digest = store.write_audit_share_segment_range(
                    segment_first_share_seq=1,
                    segment_last_share_seq=4,
                    first_share_seq=1,
                    last_share_seq=2,
                    shares=rows,
                )
                slot_bytes = Path(uri).read_bytes()
                first = store.quarantine_audit_share_segment(Path(uri), expected_bytes=slot_bytes)
                second = store.quarantine_audit_share_segment(Path(uri), expected_bytes=slot_bytes)
                self.assertEqual(first, second)
                self.assertEqual(len(list(root.glob("*.conflict-*"))), 1)
                metrics = store.metrics_snapshot()
                self.assertEqual(metrics["scan_error"], 0)
                self.assertEqual(metrics["share_segment"]["files"], 1)
                stale = store.issue_candidate(block_hash=BLOCK_HASH)
                stale.path.write_bytes(b"stale")
                store.release_candidate(stale)
                os.utime(stale.path, ns=(1, 1))
                result = store.prune_best_effort()
                self.assertEqual(result.errors, 0)
                self.assertEqual(result.candidate_removed, 1)
                self.assertFalse(stale.path.exists())
            self.assertEqual(stale_reads, 0)

    def test_scan_rejects_a_swapped_root_before_enumerating(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "audit"
            store = self.make_store(root)
            (root / f"prism-audit-bundle-body-{BLOCK_HASH}-{'11' * 32}.json").write_bytes(b"x")
            self.assertEqual(store.metrics_snapshot()["body"]["files"], 1)
            root.rename(base / "pinned")
            root.mkdir()
            try:
                metrics = store.metrics_snapshot()
                self.assertEqual(metrics["scan_error"], 1)
                with self.assertRaises(RuntimeError):
                    store._scan_root_names()
            finally:
                root.rmdir()
                (base / "pinned").rename(root)
            self.assertEqual(store.metrics_snapshot()["body"]["files"], 1)

    def test_scan_opens_and_closes_a_fresh_description_each_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.make_store(root)
            (root / "prism-audit-bundle-body-operator.json").write_bytes(b"x")
            opened: list[int] = []
            real_open = os.open
            real_close = os.close

            def recording_open(*args: Any, **kwargs: Any) -> int:
                fd = real_open(*args, **kwargs)
                if kwargs.get("dir_fd") == store._root_fd and args[0] == ".":
                    opened.append(fd)
                return fd

            closed: list[int] = []

            def recording_close(fd: int) -> None:
                closed.append(fd)
                real_close(fd)

            with mock.patch("lab.prism.audit_artifacts.os.open", side_effect=recording_open), mock.patch(
                "lab.prism.audit_artifacts.os.close",
                side_effect=recording_close,
            ):
                names = store._scan_root_names()
                store.metrics_snapshot()
            self.assertIn("prism-audit-bundle-body-operator.json", names)
            self.assertEqual(len(opened), 2)
            self.assertTrue(all(fd in closed for fd in opened))
            self.assertNotIn(store._root_fd, closed)


if __name__ == "__main__":
    unittest.main()

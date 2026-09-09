#!/usr/bin/env python3
"""Bounded canonical audit artifact views (issue #255).

The view must reproduce ``json.load`` of the artifact member for member
while never holding a window-sized Python structure: the share arrays are
replayable lazy sequences, one oversized record only grows its own decode,
UTF-8 and escapes may straddle read windows, the descriptor is released
without cyclic garbage collection, and the streaming encoder reproduces the
compact ``json.dumps`` bytes exactly.
"""

from __future__ import annotations

import gc
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import time
import tracemalloc
import unittest
from unittest import mock
import weakref

from lab.prism.audit_bundle_view import (
    SCAN_CHUNK_BYTES,
    ArtifactSource,
    CanonicalArtifactError,
    CanonicalArtifactSyntaxError,
    CanonicalAuditBundleView,
    LazyRecordSequence,
    LazyRecordSlice,
    iter_json_byte_chunks,
    iter_json_chunks,
    json_chunks_sha256_and_size,
)


def make_share(index: int, *, oversized_bytes: int = 0) -> dict[str, object]:
    share: dict[str, object] = {
        "share_seq": index + 1,
        "share_id": f"share-{index + 1}",
        "miner_id": "mïner-ü" if index % 7 == 0 else f"miner-{index % 5}",
        "order_key": f"o{index % 5}",
        "p2mr_program_hex": ("%02x" % (index % 5)) * 32,
        "share_difficulty": 2**100 + index,
        "network_difficulty": 2**127 + 1,
        "template_height": 10,
        "job_id": "job-1",
        "job_issued_at_ms": 1_700_000_000_000 - 1000,
        "accepted_at_ms": 1_700_000_000_000 - 500,
        "ntime": 1_700_000_000,
    }
    if index % 3 == 0:
        share["credit_policy"] = None
    elif index % 3 == 1:
        share["credit_policy"] = 'quote"back\\slash\n\ttab ☃'
    if oversized_bytes:
        share["share_id"] = "Z" * oversized_bytes
    return share


def make_bundle(count: int, *, oversized_index: int | None = None, oversized_bytes: int = 0) -> dict[str, object]:
    shares = [
        make_share(
            index,
            oversized_bytes=oversized_bytes if index == oversized_index else 0,
        )
        for index in range(count)
    ]
    return {
        "schema": "qbit.prism.audit-bundle.v1",
        "shares": shares,
        "found_block": {"block_height": 10, "network_difficulty": 2**90},
        "prior_balances": [],
        "reward_manifest": {
            "schema": "qbit.prism.reward-manifest.v1",
            "included_share_count": count,
            "shares": [
                {"share_seq": index + 1, "counted_difficulty": 5, "credit_policy": None}
                for index in range(count)
            ],
            "entitlements": [{"recipient_id": "a", "weight": 1}],
        },
        "payout_policy_manifest": {
            "accounts": [{"recipient_id": "a", "nested": {"deep": [1, {"x": [1, 2]}]}}]
        },
        "signed_coinbase_manifest": {"manifest": {"coinbase_tx_hex": "00"}},
    }


def write_document(root: Path, name: str, raw: bytes) -> Path:
    path = root / name
    path.write_bytes(raw)
    return path


def compact(bundle: dict[str, object], *, ensure_ascii: bool = True, separators: tuple[str, str] = (",", ":")) -> bytes:
    return json.dumps(bundle, separators=separators, ensure_ascii=ensure_ascii).encode("utf-8")


def scan(path: Path, **kwargs: object) -> CanonicalAuditBundleView:
    return CanonicalAuditBundleView.scan_path(path, **kwargs)  # type: ignore[arg-type]


class ViewParityTests(unittest.TestCase):
    def test_view_reproduces_json_load_across_row_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for count in (0, 1, 255, 256, 257, 511, 512, 513, 1300):
                for ensure_ascii, separators in (
                    (True, (",", ":")),
                    (False, (", ", ": ")),
                ):
                    with self.subTest(count=count, ascii=ensure_ascii):
                        bundle = make_bundle(count)
                        raw = compact(bundle, ensure_ascii=ensure_ascii, separators=separators)
                        path = write_document(root, f"bundle-{count}.json", raw)
                        view = scan(path, chunk_bytes=4096, stride=16)
                        try:
                            self.assertEqual(view.sha256_hex, hashlib.sha256(raw).hexdigest())
                            self.assertEqual(view.byte_length, len(raw))
                            self.assertEqual(list(view), list(bundle))
                            self.assertEqual(view.key_index("shares"), 1)
                            self.assertIsInstance(view["shares"], LazyRecordSequence)
                            self.assertIsInstance(view["reward_manifest"]["shares"], LazyRecordSequence)
                            self.assertEqual(len(view["shares"]), count)
                            # Full logical equality, both operand orders.
                            self.assertTrue(view == bundle)
                            self.assertTrue(bundle == view)
                            self.assertFalse(view != bundle)
                            self.assertFalse(bundle != view)
                            # Replayable: two complete iterations agree.
                            self.assertEqual(list(view["shares"]), bundle["shares"])
                            self.assertEqual(list(view["shares"]), bundle["shares"])
                            self.assertEqual(
                                list(view["reward_manifest"]["shares"]),
                                bundle["reward_manifest"]["shares"],
                            )
                            if count:
                                shares = view["shares"]
                                self.assertEqual(shares[0], bundle["shares"][0])
                                self.assertEqual(shares[-1], bundle["shares"][-1])
                                self.assertEqual(shares[count // 2], bundle["shares"][count // 2])
                                self.assertIsInstance(shares[3:9], LazyRecordSlice)
                                self.assertEqual(shares[3:9], bundle["shares"][3:9])
                                self.assertEqual(shares[::7], bundle["shares"][::7])
                                self.assertEqual(
                                    [list(batch) for batch in shares.batches(100)],
                                    [bundle["shares"][i : i + 100] for i in range(0, count, 100)],
                                )
                                with self.assertRaises(IndexError):
                                    shares[count]
                                with self.assertRaises(IndexError):
                                    shares[-count - 1]
                        finally:
                            view.close()

    def test_default_windows_and_strides_agree_with_small_ones(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(2000)
            raw = compact(bundle, ensure_ascii=False)
            path = write_document(Path(tmp), "bundle.json", raw)
            default_view = scan(path)
            tiny_view = scan(path, chunk_bytes=7, stride=3)
            try:
                self.assertEqual(default_view, bundle)
                self.assertEqual(tiny_view, bundle)
                for index in (0, 1, 2, 3, 999, 1998, 1999):
                    self.assertEqual(tiny_view["shares"][index], bundle["shares"][index])
                    self.assertEqual(default_view["shares"][index], bundle["shares"][index])
            finally:
                default_view.close()
                tiny_view.close()

    def test_oversized_single_field_is_isolated_to_its_own_decode(self) -> None:
        oversized = 3 * SCAN_CHUNK_BYTES + 17
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(257, oversized_index=128, oversized_bytes=oversized)
            raw = compact(bundle)
            path = write_document(Path(tmp), "bundle.json", raw)
            view = scan(path)
            try:
                self.assertEqual(view, bundle)
                self.assertEqual(view["shares"][128], bundle["shares"][128])
                # Reading an ordinary record never pays for the oversized one.
                tracemalloc.start()
                try:
                    tracemalloc.reset_peak()
                    self.assertEqual(view["shares"][0], bundle["shares"][0])
                    _current, peak = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
                self.assertLess(peak, oversized)
                tracemalloc.start()
                try:
                    tracemalloc.reset_peak()
                    self.assertEqual(view["shares"][128], bundle["shares"][128])
                    _current, peak = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
                # One oversized record costs a small multiple of itself
                # (decoded text plus the string), never the window.
                self.assertLess(peak, 8 * oversized)
            finally:
                view.close()

    def test_null_and_absent_members_stay_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(6)
            raw = compact(bundle)
            path = write_document(Path(tmp), "bundle.json", raw)
            view = scan(path)
            try:
                self.assertEqual(view, bundle)
                absent = json.loads(raw)
                del absent["shares"][0]["credit_policy"]
                self.assertNotEqual(view, absent)
                nulled = json.loads(raw)
                nulled["shares"][2]["credit_policy"] = None
                self.assertNotEqual(view, nulled)
                self.assertEqual(view["shares"][0]["share_difficulty"], 2**100)
                self.assertEqual(view["found_block"]["network_difficulty"], 2**90)
            finally:
                view.close()

    def test_equality_mismatches_are_detected_streaming(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(600)
            raw = compact(bundle)
            path = write_document(Path(tmp), "bundle.json", raw)
            view = scan(path, chunk_bytes=1024)
            try:
                last_record = json.loads(raw)
                last_record["shares"][-1]["share_seq"] = 999_999
                self.assertNotEqual(view, last_record)
                self.assertNotEqual(last_record, view)
                extra_key = {**json.loads(raw), "extra": 1}
                self.assertNotEqual(view, extra_key)
                missing_key = json.loads(raw)
                del missing_key["prior_balances"]
                self.assertNotEqual(view, missing_key)
                counted = json.loads(raw)
                counted["reward_manifest"]["shares"][300]["counted_difficulty"] = 6
                self.assertNotEqual(view, counted)
                shorter = json.loads(raw)
                shorter["shares"].pop()
                self.assertNotEqual(view, shorter)
                longer = json.loads(raw)
                longer["shares"].append(make_share(600))
                self.assertNotEqual(view, longer)
                self.assertNotEqual(view["shares"], "not a sequence")
                self.assertEqual(view["shares"][10:20], list(view["shares"])[10:20])
            finally:
                view.close()


class ViewRobustnessTests(unittest.TestCase):
    def test_malformed_documents_fail_as_json_decode_errors_and_release_fd(self) -> None:
        cases = [
            b"[1,2]",
            b'{"a":1',
            b'{"a":1}x',
            b'{"shares":[1,2,}',
            b'{"shares":[{"a":1}',
            b'{"a":"\xff"}',
            b'{"shares":[{"a":1}]}{',
            b'{"shares":[{"a":1}],}',
            b'{1:2}',
            b"",
            b'{"shares":[{"a":"unterminated}]}',
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for index, raw in enumerate(cases):
                with self.subTest(document=raw):
                    path = write_document(Path(tmp), f"bad-{index}.json", raw)
                    fd = os.open(path, os.O_RDONLY)
                    with self.assertRaises(json.JSONDecodeError) as caught:
                        CanonicalAuditBundleView.scan(fd, chunk_bytes=4)
                    self.assertIsInstance(caught.exception, CanonicalArtifactSyntaxError)
                    self.assertIsInstance(caught.exception, CanonicalArtifactError)
                    with self.assertRaises(OSError):
                        os.fstat(fd)

    def test_not_a_regular_file_is_rejected_and_fd_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fd = os.open(tmp, os.O_RDONLY)
            with self.assertRaises(CanonicalArtifactError):
                ArtifactSource(fd)
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_identity_change_is_detected_by_lazy_reads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(40)
            raw = compact(bundle)
            path = write_document(Path(tmp), "bundle.json", raw)
            view = scan(path)
            try:
                header = view["found_block"]
                with path.open("ab") as handle:
                    handle.write(b" ")
                self.assertEqual(view["found_block"], header)
                with self.assertRaises(CanonicalArtifactError) as caught:
                    list(view["shares"])
                self.assertNotIsInstance(caught.exception, json.JSONDecodeError)
                with self.assertRaises(CanonicalArtifactError):
                    view.verify_identity()
                with self.assertRaises(CanonicalArtifactError):
                    list(view.iter_bytes())
            finally:
                view.close()

    def test_descriptor_is_released_without_cyclic_gc(self) -> None:
        gc.disable()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                bundle = make_bundle(30)
                raw = compact(bundle)
                path = write_document(Path(tmp), "bundle.json", raw)
                view = scan(path)
                fd = view.source.fileno()
                shares = view["shares"]
                self.assertEqual(shares[3], bundle["shares"][3])
                ref = weakref.ref(view)
                source_ref = weakref.ref(view.source)
                del view
                # The lazy sequence still owns the source; the descriptor
                # stays open exactly as long as a legitimate owner exists.
                self.assertIsNone(ref())
                self.assertIsNotNone(source_ref())
                self.assertEqual(shares[4], bundle["shares"][4])
                del shares
                self.assertIsNone(source_ref())
                with self.assertRaises(OSError):
                    os.fstat(fd)
                # Explicit close is idempotent and fails lazy reads closed.
                view = scan(path)
                lazy = view["shares"]
                view.close()
                view.close()
                self.assertTrue(view.closed)
                self.assertEqual(view["schema"], bundle["schema"])
                with self.assertRaises(CanonicalArtifactError):
                    list(lazy)
        finally:
            gc.enable()

    def test_scan_stops_within_one_read_when_cancelled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(3000)
            raw = compact(bundle)
            path = write_document(Path(tmp), "bundle.json", raw)
            calls: list[int] = []

            class Cancelled(RuntimeError):
                pass

            def checkpoint() -> None:
                calls.append(1)
                if len(calls) == 3:
                    raise Cancelled("cancelled")

            fd = os.open(path, os.O_RDONLY)
            with self.assertRaises(Cancelled):
                CanonicalAuditBundleView.scan(fd, chunk_bytes=8192, cancellation=checkpoint)
            self.assertEqual(len(calls), 3)
            with self.assertRaises(OSError):
                os.fstat(fd)


class StreamingEncoderTests(unittest.TestCase):
    def test_encoder_reproduces_compact_json_with_lazy_members(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(700)
            raw = compact(bundle)
            path = write_document(Path(tmp), "bundle.json", raw)
            view = scan(path, chunk_bytes=2048, stride=8)
            try:
                storage = {
                    "schema": "example",
                    "bundle_without_shares": view.without("shares"),
                    "count": 700,
                    "parts": [
                        {"kind": "inline", "shares": view["shares"][0:5]},
                        {"kind": "inline", "shares": view["shares"][695:700]},
                    ],
                    "plain": {"list": [1, 2.5, None, True, "ü"], "empty": [], "obj": {}},
                    "lazy_only": view["shares"],
                }
                expected_storage = {
                    "schema": "example",
                    "bundle_without_shares": {k: v for k, v in bundle.items() if k != "shares"},
                    "count": 700,
                    "parts": [
                        {"kind": "inline", "shares": bundle["shares"][0:5]},
                        {"kind": "inline", "shares": bundle["shares"][695:700]},
                    ],
                    "plain": {"list": [1, 2.5, None, True, "ü"], "empty": [], "obj": {}},
                    "lazy_only": bundle["shares"],
                }
                expected = json.dumps(expected_storage, separators=(",", ":"))
                for batch_records, chunk_chars in ((1, 1), (7, 100), (256, 65536)):
                    chunks = list(
                        iter_json_chunks(
                            storage,
                            batch_records=batch_records,
                            chunk_chars=chunk_chars,
                        )
                    )
                    self.assertEqual("".join(chunks), expected)
                    self.assertTrue(all(chunk.isascii() for chunk in chunks))
                digest, size = json_chunks_sha256_and_size(iter_json_byte_chunks(storage))
                self.assertEqual(digest, hashlib.sha256(expected.encode()).hexdigest())
                self.assertEqual(size, len(expected))
                with self.assertRaises(TypeError):
                    json.dumps(storage)
            finally:
                view.close()

    def test_encoder_matches_json_dumps_for_plain_values(self) -> None:
        values = [
            {},
            [],
            {"a": [1, [2, [3, {"b": None}]]], "c": "d"},
            {"k": {1: "int key"}},
            [{"x": 1}, {"y": [1, 2]}, "s", 3],
            "solo",
            12,
        ]
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    "".join(iter_json_chunks(value, batch_records=2, chunk_chars=3)),
                    json.dumps(value, separators=(",", ":")),
                )


def _write_synthetic_bundle(path: Path, count: int) -> tuple[bytes, int]:
    """Write a count-share bundle by streaming, returning (sha256, size)."""
    digest = hashlib.sha256()
    size = 0
    with path.open("wb") as handle:

        def emit(text: str) -> None:
            nonlocal size
            data = text.encode("utf-8")
            digest.update(data)
            size += len(data)
            handle.write(data)

        emit('{"schema":"qbit.prism.audit-bundle.v1","shares":[')
        for index in range(count):
            if index:
                emit(",")
            emit(json.dumps(make_share(index), separators=(",", ":")))
        emit('],"found_block":{"block_height":10,"network_difficulty":1000},"prior_balances":[],')
        emit('"reward_manifest":{"schema":"qbit.prism.reward-manifest.v1","shares":[')
        for index in range(count):
            if index:
                emit(",")
            emit(
                json.dumps(
                    {"share_seq": index + 1, "counted_difficulty": 5},
                    separators=(",", ":"),
                )
            )
        emit('],"entitlements":[]},"payout_policy_manifest":{"accounts":[]},')
        emit('"signed_coinbase_manifest":{"manifest":{"coinbase_tx_hex":"00"}}}')
    return digest.digest(), size


class StressWindowTests(unittest.TestCase):
    """Window-sized artifacts; the 375k case runs when explicitly enabled."""

    def _exercise(self, count: int, *, peak_limit: int) -> dict[str, float]:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "canonical.json"
            expected_digest, size = _write_synthetic_bundle(path, count)
            tracemalloc.start()
            try:
                tracemalloc.reset_peak()
                started = time.monotonic()
                view = scan(path)
                scan_seconds = time.monotonic() - started
                try:
                    self.assertEqual(view.sha256_hex, expected_digest.hex())
                    self.assertEqual(view.byte_length, size)
                    self.assertEqual(len(view["shares"]), count)
                    self.assertEqual(len(view["reward_manifest"]["shares"]), count)
                    for index in (0, 1, count // 2, count - 2, count - 1):
                        self.assertEqual(view["shares"][index], make_share(index))
                    started = time.monotonic()
                    walked = 0
                    for record in view["shares"]:
                        walked += 1
                        if walked % 50_000 == 0:
                            self.assertEqual(record["share_seq"], walked)
                    walk_seconds = time.monotonic() - started
                    self.assertEqual(walked, count)
                    encoded = 0
                    started = time.monotonic()
                    for chunk in iter_json_byte_chunks({"shares": view["shares"]}):
                        encoded += len(chunk)
                    encode_seconds = time.monotonic() - started
                    _current, peak = tracemalloc.get_traced_memory()
                finally:
                    view.close()
            finally:
                tracemalloc.stop()
            self.assertGreater(encoded, 0)
            self.assertLess(peak, peak_limit, f"peak traced memory {peak} exceeded {peak_limit}")
            return {
                "count": count,
                "bytes": size,
                "scan_seconds": scan_seconds,
                "walk_seconds": walk_seconds,
                "encode_seconds": encode_seconds,
                "peak_traced_bytes": peak,
            }

    def test_twenty_thousand_shares_stay_bounded(self) -> None:
        # ~7.5 MB of JSON; peak traced memory stays a few read windows.
        report = self._exercise(20_000, peak_limit=6 * 1024 * 1024)
        self.assertGreater(report["bytes"], 5_000_000)

    @unittest.skipUnless(
        os.environ.get("PRISM_BOUNDED_AUDIT_STRESS"),
        "set PRISM_BOUNDED_AUDIT_STRESS=1 to run the 375k-share artifact case",
    )
    def test_375k_shares_across_generations(self) -> None:
        reports = []
        for generation in range(int(os.environ.get("PRISM_BOUNDED_AUDIT_GENERATIONS", "2"))):
            reports.append(self._exercise(375_000, peak_limit=32 * 1024 * 1024))
        for report in reports:
            print(f"bounded-audit-stress {json.dumps(report)}", flush=True)


class BoundedScanTests(unittest.TestCase):
    """The scan holds at most about two read chunks whatever a value's size."""

    def test_checkpoint_index_lives_on_disk_and_falls_back_to_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(3000)
            raw = compact(bundle)
            path = write_document(Path(tmp), "bundle.json", raw)
            view = scan(path, stride=16)
            try:
                stats = view.scan_stats
                self.assertFalse(stats.index_in_memory)
                # 3000 shares and 3000 counted shares at a stride of 16, plus
                # the two leaf arrays' single checkpoints.
                self.assertEqual(stats.checkpoint_entries, 2 * 188)
                self.assertFalse(view["shares"]._index.in_memory)
                self.assertEqual(view["shares"][2999], bundle["shares"][2999])
                self.assertEqual(view["reward_manifest"]["shares"][1234], bundle["reward_manifest"]["shares"][1234])
            finally:
                view.close()
            with mock.patch("lab.prism.audit_bundle_view.tempfile.TemporaryFile", side_effect=OSError("no temp")):
                fallback = scan(path, stride=16)
            try:
                self.assertTrue(fallback.scan_stats.index_in_memory)
                self.assertEqual(fallback, bundle)
                self.assertEqual(fallback["shares"][2999], bundle["shares"][2999])
            finally:
                fallback.close()
            # A closed view fails lazy reads closed rather than reading a
            # released index or descriptor.
            view = scan(path, stride=16)
            lazy = view["shares"]
            view.close()
            with self.assertRaises(CanonicalArtifactError):
                lazy[5]

    def test_oversized_values_are_skipped_without_being_held(self) -> None:
        chunk = 4096
        oversized = 6 * SCAN_CHUNK_BYTES + 11  # far above the soft limit
        with tempfile.TemporaryDirectory() as tmp:
            bundle = make_bundle(300, oversized_index=150, oversized_bytes=oversized)
            bundle["shares"][151]["nested"] = {"deep": [1, {"x": '"}]{[\\'}], "s": "a\\\"b"}
            bundle["shares"][152]["credit_policy"] = "quote\"in\\side" + "é" * 3000
            bundle["reward_manifest"]["shares"][150]["blob"] = "Y" * (3 * chunk + 1)
            bundle["witness_merkle_leaves_hex"] = ["ab" * 32] * 500
            raw = compact(bundle, ensure_ascii=False)
            path = write_document(Path(tmp), "bundle.json", raw)
            tracemalloc.start()
            try:
                tracemalloc.reset_peak()
                view = scan(path, chunk_bytes=chunk)
                _current, scan_peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            try:
                stats = view.scan_stats
                # The oversized record was walked structurally: the largest
                # decoded window stayed a couple of chunks, not the record.
                self.assertLess(stats.window_high_water_chars, 4 * chunk)
                self.assertGreaterEqual(stats.max_value_chars, oversized)
                self.assertEqual(stats.oversized_values, 1)
                self.assertLess(scan_peak, oversized)
                self.assertEqual(view.sha256_hex, hashlib.sha256(raw).hexdigest())
                self.assertEqual(len(view["shares"]), 300)
                self.assertEqual(len(view["witness_merkle_leaves_hex"]), 500)
                # Consumers that need the record decode it whole; that
                # decode is the remaining record-sized allocation.
                self.assertEqual(view["shares"][150], bundle["shares"][150])
                self.assertEqual(view["shares"][151], bundle["shares"][151])
                self.assertEqual(view["shares"][152], bundle["shares"][152])
                self.assertEqual(view, bundle)
                self.assertEqual(list(view["shares"][149:153]), bundle["shares"][149:153])
            finally:
                view.close()

    def test_structural_walk_rejects_unterminated_values(self) -> None:
        cases = [
            b'{"shares":[{"a":"' + b"x" * 5000,
            b'{"shares":[{"a":[1,2}]}',
            b'{"shares":[{"a":"\\' ,
            b'{"shares":[,]}',
            b'{"shares":[1,]}',
            b'{"shares":["abc]}',
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for index, raw in enumerate(cases):
                with self.subTest(document=raw[:24]):
                    path = write_document(Path(tmp), f"bad-{index}.json", raw)
                    with self.assertRaises(json.JSONDecodeError):
                        scan(path, chunk_bytes=16)


class ArtifactSourceTests(unittest.TestCase):
    def test_iter_bytes_and_sha256_cover_the_whole_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw = os.urandom(3 * SCAN_CHUNK_BYTES + 5)
            path = write_document(Path(tmp), "blob.bin", raw)
            source = ArtifactSource(os.open(path, os.O_RDONLY), path=path)
            try:
                self.assertEqual(b"".join(source.iter_bytes(chunk_bytes=1000)), raw)
                self.assertEqual(source.sha256_hex(), hashlib.sha256(raw).hexdigest())
                self.assertEqual(source.pread(len(raw) - 3, 10), raw[-3:])
                self.assertEqual(source.pread(len(raw), 10), b"")
            finally:
                source.close()
            self.assertTrue(source.closed)
            with self.assertRaises(CanonicalArtifactError):
                source.fileno()

    def test_chunk_reader_adapter_streams_sequentially(self) -> None:
        from lab.prism.audit_artifacts import _ChunkReader

        with tempfile.TemporaryDirectory() as tmp:
            raw = bytes(range(256)) * 700
            path = write_document(Path(tmp), "blob.bin", raw)
            source = ArtifactSource(os.open(path, os.O_RDONLY), path=path)
            try:
                reader = io.BufferedReader(_ChunkReader(source), buffer_size=4096)
                self.assertEqual(reader.read(), raw)
            finally:
                source.close()


if __name__ == "__main__":
    unittest.main()

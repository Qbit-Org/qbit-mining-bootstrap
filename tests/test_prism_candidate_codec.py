#!/usr/bin/env python3
"""Bounded candidate codec regressions (issue #255).

Two contracts are pinned here. First, byte identity with the historical
v1 identity oracle: for every valid fixture the chunked body concatenates
to exactly ``json.dumps(block_candidate_identity(intent), sort_keys=True,
separators=(",", ":"))`` and ``candidate_sha256`` is the oracle's digest.
Second, boundedness by mechanism, not by timing: every ``json.dumps`` call
the encoder makes is instrumented and asserted to cover at most the batch
record cap, and every chunk it emits is asserted to be the fixed chunk
size except the last. Nothing here measures elapsed time.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism import candidate_codec as codec  # noqa: E402
from lab.prism.share_ledger import (  # noqa: E402
    AcceptedShareRecord,
    DaemonShareJsonSequence,
    IncrementalShareJsonSequence,
    _IncrementalShareWindowPage,
    block_candidate_identity,
    block_candidate_identity_sha256,
    sha256_json_hex,
)

HASH_A = "aa" * 32
PARENT = "bb" * 32
RUN_SLOW = os.environ.get("PRISM_CANDIDATE_CODEC_SLOW") == "1"


def oracle_bytes(intent: dict[str, Any]) -> bytes:
    """The historical identity JSON, computed the historical way."""
    identity = block_candidate_identity({**intent, "block_hash_hex": intent["block_hash_hex"].lower()})
    return json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")


def share(index: int) -> dict[str, Any]:
    return {
        "share_seq": index,
        "share_id": f"miner-{index % 7}:é\U0001F600{'x' * (index % 3)}",
        "miner_id": f"m{index % 7}",
        "order_key": "k",
        "p2mr_program_hex": "ab" * 32,
        "share_difficulty": 10**30 + index,
        "network_difficulty": 1.5e16 if index % 5 else 3,
        "template_height": 5,
        "job_id": 'j"q\\',
        "job_issued_at_ms": 1_700_000_000_000 + index,
        "accepted_at_ms": 1_700_000_000_001 + index,
        "ntime": 1_700_000_000,
        "credit_policy": None if index % 2 else "stale-grace",
    }


def intent_for(share_count: int, **overrides: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "schema": codec.CANDIDATE_INTENT_SCHEMA,
        "block_hash_hex": HASH_A.upper(),
        "block_hex": "00" * 100,
        "coinbase_tx_hex": "01",
        "parent_hash": PARENT,
        "expected_height": 10,
        "template": {"previousblockhash": PARENT, "height": 10, "coinbasevalue": 5},
        "shares_json": [share(index) for index in range(share_count)],
        "prior_balances": [{"recipient_id": "a", "balance_sats": 1}],
        "found_block": {"network_difficulty": 1.0, "witness": [1, 2, {"z": None}]},
        "prospective_prior_balances": None,
        "witness_merkle_leaves_hex": ["cc" * 32] * 3,
        "extranonce1_hex": "00",
        "extranonce2_hex": "01",
        "username": "u",
        "pending_share": {"share_id": "s", "accepted_at_ms": 123},
        "credit_share_on_accept": False,
        "collection_only": False,
    }
    fields.update(overrides)
    return fields


class _Instrumented:
    """Instrument ``json.dumps`` inside the codec and collect chunk facts."""

    def __init__(self) -> None:
        self.dumps_inputs: list[int] = []
        self.dumps_outputs: list[int] = []
        self.chunks: list[codec.BodyChunk] = []
        self._real = json.dumps

    def dumps(self, value: Any, *args: Any, **kwargs: Any) -> str:
        text = self._real(value, *args, **kwargs)
        self.dumps_inputs.append(len(value) if isinstance(value, (list, tuple)) else 1)
        self.dumps_outputs.append(len(text))
        return text

    def collect(self, chunk: codec.BodyChunk) -> None:
        self.chunks.append(chunk)


class CodecByteIdentityTests(unittest.TestCase):
    def assert_body_matches_oracle(self, fields: dict[str, Any], *, chunk_bytes: int = 4096) -> codec.PreparedCandidateIntent:
        prepared = codec.prepare_candidate_intent(fields, chunk_bytes=chunk_bytes)
        got = bytearray()
        chunks: list[codec.BodyChunk] = []

        def collect(chunk: codec.BodyChunk) -> None:
            chunks.append(chunk)
            got.extend(chunk.data)

        prepared.body.write_chunks(collect)
        expected = oracle_bytes(fields)
        self.assertEqual(bytes(got), expected)
        self.assertEqual(prepared.candidate_sha256, hashlib.sha256(expected).hexdigest())
        self.assertEqual(prepared.candidate_sha256, sha256_json_hex(block_candidate_identity({**fields, "block_hash_hex": HASH_A})))
        self.assertEqual(block_candidate_identity_sha256(prepared), prepared.candidate_sha256)
        # Fixed chunk boundaries: every chunk but the last is exactly the size.
        self.assertEqual([chunk.ordinal for chunk in chunks], list(range(len(chunks))))
        self.assertTrue(all(len(chunk.data) == chunk_bytes for chunk in chunks[:-1]))
        self.assertEqual(sum(len(chunk.data) for chunk in chunks), prepared.manifest.byte_count)
        self.assertEqual(len(chunks), prepared.manifest.chunk_count)
        for chunk in chunks:
            self.assertEqual(hashlib.sha256(chunk.data).hexdigest(), chunk.sha256)
        return prepared

    def test_share_counts_around_batch_boundaries(self) -> None:
        for count in (0, 1, 255, 256, 257, 511, 512, 513, 1300):
            with self.subTest(count=count):
                prepared = self.assert_body_matches_oracle(intent_for(count))
                self.assertEqual(prepared.manifest.share_count, count)
                self.assertIs(prepared["shares_json"], prepared.shares)
                self.assertEqual(prepared["pending_share"]["accepted_at_ms"], 123)
                self.assertTrue(prepared.accepted_at_present)

    @unittest.skipUnless(RUN_SLOW, "set PRISM_CANDIDATE_CODEC_SLOW=1 for the 375,000-share oracle")
    def test_stress_window_matches_oracle(self) -> None:
        prepared = self.assert_body_matches_oracle(intent_for(375_000), chunk_bytes=codec.CANDIDATE_BODY_CHUNK_BYTES)
        self.assertEqual(prepared.manifest.share_count, 375_000)

    def test_giant_string_field_spans_chunk_and_slice_boundaries(self) -> None:
        big = ("é\"\\" + "x" * 1000 + "\U0001F600") * 300
        prepared = self.assert_body_matches_oracle(intent_for(3, block_hex=big), chunk_bytes=1000)
        self.assertGreater(prepared.manifest.span_count, 1)

    def test_giant_field_inside_one_record(self) -> None:
        fields = intent_for(600)
        fields["shares_json"][300]["share_id"] = "y" * (5 * 1024 * 1024)
        self.assert_body_matches_oracle(fields, chunk_bytes=codec.CANDIDATE_BODY_CHUNK_BYTES)

    def test_first_oversized_record_is_bounded_before_encoding(self) -> None:
        fields = intent_for(260)
        fields["shares_json"][0]["share_id"] = "😀" * 100_000
        fields["shares_json"][257]["share_id"] = "x" * (5 * 1024 * 1024)
        expected = oracle_bytes(fields)
        observer = _Instrumented()
        with mock.patch.object(codec.json, "dumps", observer.dumps):
            prepared = codec.prepare_candidate_intent(fields)
            prepared.body.write_chunks(observer.collect)
        self.assertEqual(b"".join(chunk.data for chunk in observer.chunks), expected)
        self.assertLessEqual(max(observer.dumps_outputs), codec.CODEC_BATCH_TARGET_BYTES)

    def test_large_integers_floats_nulls_and_field_order(self) -> None:
        fields = intent_for(
            2,
            found_block={"network_difficulty": 2**200, "float": 1e300, "tiny": 5e-324, "neg": -0.0, "none": None},
            extension_zzz={"b": [1, {"a": None}], "a": ""},
            extension_aaa=[[], {}, "", 0, False],
        )
        prepared = self.assert_body_matches_oracle(fields)
        self.assertEqual(list(prepared), list(fields))

    def test_absent_and_null_stamp_are_distinct(self) -> None:
        absent = intent_for(2)
        del absent["pending_share"]["accepted_at_ms"]
        prepared_absent = self.assert_body_matches_oracle(absent)
        self.assertFalse(prepared_absent.accepted_at_present)
        null = intent_for(2, pending_share={"share_id": "s", "accepted_at_ms": None})
        prepared_null = self.assert_body_matches_oracle(null)
        self.assertTrue(prepared_null.accepted_at_present)
        self.assertNotEqual(prepared_absent.candidate_sha256, prepared_null.candidate_sha256)

    def test_stamp_drift_keeps_identity(self) -> None:
        first = codec.prepare_candidate_intent(intent_for(3))
        second = codec.prepare_candidate_intent(intent_for(3, pending_share={"share_id": "s", "accepted_at_ms": 999}))
        self.assertEqual(first.candidate_sha256, second.candidate_sha256)
        self.assertEqual(second.accepted_at_ms, 999)

    def test_candidate_only_intent_without_shares(self) -> None:
        fields = {"schema": codec.CANDIDATE_INTENT_SCHEMA, "block_hash_hex": HASH_A, "block_hex": "00"}
        prepared = self.assert_body_matches_oracle(fields)
        self.assertFalse(prepared.has_shares)
        self.assertNotIn("shares_json", prepared)

    def test_empty_and_large_balances_and_witness_arrays(self) -> None:
        empty = intent_for(1, prior_balances=[], witness_merkle_leaves_hex=[], prospective_prior_balances=[])
        self.assert_body_matches_oracle(empty)
        large = intent_for(
            1,
            prior_balances=[{"recipient_id": f"r{index}", "balance_sats": index} for index in range(5000)],
            prospective_prior_balances=[["a", "b", "c", index] for index in range(5000)],
            witness_merkle_leaves_hex=["cc" * 32] * 20_000,
        )
        prepared = self.assert_body_matches_oracle(large)
        self.assertGreaterEqual(prepared.manifest.span_count, 4)


class CodecRejectionTests(unittest.TestCase):
    def test_nan_and_infinity_are_rejected_before_credit(self) -> None:
        for value in (float("nan"), float("inf"), -float("inf")):
            with self.subTest(value=value), self.assertRaises(codec.CandidateCodecError):
                codec.prepare_candidate_intent(intent_for(1, found_block={"network_difficulty": value}))
            with self.subTest(value=value, where="share"), self.assertRaises(codec.CandidateCodecError):
                codec.prepare_candidate_intent(intent_for(0, shares_json=[{"a": value}]))

    def test_unsupported_types_raise_type_error_like_json_dumps(self) -> None:
        for value in (b"bytes", {1, 2}, object()):
            with self.subTest(value=type(value).__name__), self.assertRaises(TypeError):
                codec.prepare_candidate_intent(intent_for(1, found_block={"blob": value}))

    def test_jsonb_incompatible_text_is_rejected_everywhere(self) -> None:
        nul = chr(0)
        high, low = "\ud83d", "\ude00"
        cases = {
            "nul in field": intent_for(1, username="a" + nul + "b"),
            "nul in key": intent_for(1, **{"ext" + nul: 1}),
            "nul in share": intent_for(0, shares_json=[{"a": "x" + nul}]),
            "nul in giant": intent_for(1, block_hex="y" * 70000 + nul),
            "nul in nested share list": intent_for(0, shares_json=[{"a": [nul]}]),
            "lone high": intent_for(1, username=high),
            "lone low in share": intent_for(0, shares_json=[{"a": low}]),
            "lone high in giant": intent_for(1, block_hex="y" * 70000 + high),
            "nul in non-string-key dict": intent_for(1, found_block={1: "x" + nul}),
            "lone in non-string-key dict": intent_for(1, found_block={1: high}),
            "high then high": intent_for(0, shares_json=[{"a": high + high + low}]),
        }
        for label, fields in cases.items():
            with self.subTest(label=label), self.assertRaises(codec.CandidateCodecError):
                codec.prepare_candidate_intent(fields)

    def test_jsonb_compatible_escapes_are_accepted(self) -> None:
        high, low = "\ud83d", "\ude00"
        for label, fields in {
            "valid pair": intent_for(1, username="\U0001F600", shares_json=[{"a": "\U0001F600"}]),
            "adjacent lone units forming a pair": intent_for(1, username=high + low),
            "escaped backslash-u text": intent_for(1, username="\\u0000", shares_json=[{"a": "\\u0000", "b": "\\\\ud83d"}]),
            "pair at slice edge": intent_for(1, block_hex="y" * 65535 + "\U0001F600" + "z"),
            "literal surrogate pair at slice edge": intent_for(1, block_hex="y" * (codec.CODEC_STRING_SLICE_CHARS - 1) + high + low + "z"),
        }.items():
            with self.subTest(label=label):
                prepared = codec.prepare_candidate_intent(fields)
                got = bytearray()
                prepared.body.write_chunks(lambda chunk: got.extend(chunk.data))
                self.assertEqual(bytes(got), oracle_bytes(fields))

    def test_missing_hash_and_bad_share_container(self) -> None:
        with self.assertRaises(codec.CandidateCodecError):
            codec.prepare_candidate_intent({"schema": "x", "shares_json": []})
        with self.assertRaises(codec.CandidateCodecError):
            codec.prepare_candidate_intent(intent_for(0, shares_json="not a sequence"))

    def test_changed_source_cannot_publish(self) -> None:
        fields = intent_for(5)
        prepared = codec.prepare_candidate_intent(fields)
        fields["shares_json"].append(share(99))
        with self.assertRaises(codec.CandidateBodyIntegrityError):
            prepared.body.write_chunks(lambda chunk: None)

    def test_mutated_prepared_intent_is_re_prepared(self) -> None:
        prepared = codec.prepare_candidate_intent(intent_for(2))
        original = prepared.candidate_sha256
        prepared.pop("collection_only")
        self.assertTrue(prepared.dirty)
        refreshed = codec.prepare_candidate_intent(prepared)
        self.assertNotEqual(refreshed.candidate_sha256, original)
        self.assertEqual(refreshed.candidate_sha256, hashlib.sha256(oracle_bytes(dict(prepared))).hexdigest())


class CodecBoundednessTests(unittest.TestCase):
    def test_every_dumps_call_and_chunk_is_bounded(self) -> None:
        fields = intent_for(3000, prior_balances=[{"recipient_id": f"r{i}", "balance_sats": i} for i in range(3000)])
        probe = _Instrumented()
        with mock.patch.object(codec.json, "dumps", probe.dumps):
            prepared = codec.prepare_candidate_intent(fields, chunk_bytes=codec.CANDIDATE_BODY_CHUNK_BYTES)
            prepared.body.write_chunks(probe.collect)
        self.assertTrue(probe.dumps_inputs)
        self.assertLessEqual(max(probe.dumps_inputs), codec.CODEC_BATCH_RECORDS)
        self.assertLessEqual(max(probe.dumps_outputs), codec.CODEC_BATCH_OVERSIZED_BYTES)
        self.assertTrue(all(len(chunk.data) <= codec.CANDIDATE_BODY_CHUNK_BYTES for chunk in probe.chunks))

    def test_daemon_sequence_is_copied_verbatim_without_parsing(self) -> None:
        records = [share(index) for index in range(2000)]
        records[700]["share_id"] = 'trap},{"accepted_at_ms":1'
        items = b",".join(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode() for record in records)
        daemon = DaemonShareJsonSequence(items, len(records))
        fields = intent_for(0, shares_json=daemon)
        prepared = codec.prepare_candidate_intent(fields, chunk_bytes=4096)
        got = bytearray()
        prepared.body.write_chunks(lambda chunk: got.extend(chunk.data))
        self.assertIsNone(daemon._parsed, "the daemon sequence must not be parsed to stage it")
        self.assertEqual(bytes(got), oracle_bytes(intent_for(0, shares_json=records)))
        self.assertEqual(prepared.manifest.share_count, 2000)

    def test_page_backed_window_pages_are_copied_verbatim(self) -> None:
        records = tuple(
            AcceptedShareRecord(share_seq=i, share_id=f"s{i}", miner_id="m", order_key="k", p2mr_program_hex="ab" * 32, share_difficulty=1, network_difficulty=2, template_height=3, job_id="j", job_issued_at_ms=4, accepted_at_ms=5, ntime=6)
            for i in range(1100)
        )
        pages = tuple(_IncrementalShareWindowPage.from_records(records[i : i + 512]) for i in range(0, 1100, 512))
        window = IncrementalShareJsonSequence(pages=pages, record_count=1100)
        prepared = codec.prepare_candidate_intent(intent_for(0, shares_json=window), chunk_bytes=4096)
        got = bytearray()
        prepared.body.write_chunks(lambda chunk: got.extend(chunk.data))
        self.assertEqual(bytes(got), oracle_bytes(intent_for(0, shares_json=list(window))))
        self.assertEqual(prepared.manifest.page_count, 3)

    def test_replay_header_is_bounded_and_defers_oversized_facts(self) -> None:
        bounded = codec.replay_header_from_fields(intent_for(1))
        self.assertFalse(bounded["oversized"])
        self.assertEqual(bounded["pending_share"]["accepted_at_ms"], 123)
        oversized = codec.replay_header_from_fields(intent_for(1, username="u" * 1000, pending_share={"share_id": "s" * 1000, "accepted_at_ms": 123}))
        self.assertTrue(oversized["oversized"])
        self.assertIsNone(oversized["username"])
        self.assertIsNone(oversized["pending_share"]["share_id"])
        self.assertEqual(oversized["pending_share"]["accepted_at_ms"], 123)
        self.assertLess(len(json.dumps(oversized)), 4096)


class SpoolRoundTripTests(unittest.TestCase):
    def _spool(self, prepared: codec.PreparedCandidateIntent) -> codec.SpoolCandidateBody:
        directory = tempfile.mkdtemp()
        self.addCleanup(lambda: [os.unlink(os.path.join(directory, name)) for name in os.listdir(directory)] and os.rmdir(directory) if os.path.isdir(directory) else None)
        return codec.write_spool_from_body(os.path.join(directory, "b.body"), os.path.join(directory, "b.idx"), prepared.body)

    def test_pages_decode_and_random_access_use_the_disk_index(self) -> None:
        for count in (0, 1, 256, 1300):
            with self.subTest(count=count):
                fields = intent_for(count, prior_balances=[{"recipient_id": f"r{i}", "balance_sats": i} for i in range(4000 if count == 1300 else 1)])
                prepared = codec.prepare_candidate_intent(fields, chunk_bytes=4096)
                spool = self._spool(prepared)
                hydrated = codec.prepared_intent_from_spool(spool, accepted_at_present=True, accepted_at_ms=123)
                shares = hydrated["shares_json"]
                self.assertEqual(len(shares), count)
                self.assertEqual(list(shares), fields["shares_json"])
                if count:
                    self.assertEqual(shares[0], fields["shares_json"][0])
                    self.assertEqual(shares[-1], fields["shares_json"][-1])
                    self.assertEqual(shares[count // 2], fields["shares_json"][count // 2])
                self.assertEqual(shares.canonical_json_sha256(), hashlib.sha256(json.dumps(fields["shares_json"], sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest())
                self.assertEqual(list(hydrated["prior_balances"]), fields["prior_balances"])
                if count == 1300:
                    self.assertIsInstance(hydrated["prior_balances"], codec.SpoolJsonArraySequence)
                self.assertEqual(hydrated["pending_share"]["accepted_at_ms"], 123)
                regot = bytearray()
                hydrated.body.write_chunks(lambda chunk: regot.extend(chunk.data))
                self.assertEqual(bytes(regot), oracle_bytes(fields))
                self.assertEqual(codec.prepare_candidate_intent(dict(hydrated), chunk_bytes=4096).candidate_sha256, prepared.candidate_sha256)
                paths = (spool.path, spool.index.path)
                spool.close()
                self.assertFalse(any(os.path.exists(path) for path in paths))

    def test_hint_pages_from_a_daemon_body_are_validated_and_corrected(self) -> None:
        records = [share(index) for index in range(2000)]
        records[700]["share_id"] = 'trap},{"accepted_at_ms":1'
        items = b",".join(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode() for record in records)
        prepared = codec.prepare_candidate_intent(intent_for(0, shares_json=DaemonShareJsonSequence(items, 2000)), chunk_bytes=4096)
        spool = self._spool(prepared)
        span = spool.index.span("shares_json")
        self.assertFalse(span.pages_exact)
        sequence = codec.SpoolJsonArraySequence(spool, span)
        self.assertEqual(sequence[1999], records[1999])
        self.assertEqual(list(sequence), records)
        self.assertTrue(spool.index.span("shares_json").pages_exact)
        self.assertEqual(sequence[700], records[700])
        spool.close()

    def test_giant_string_and_oversized_record_decode_by_slices(self) -> None:
        big = ("é\"\\" + "x" * 1000 + "\U0001F600") * 300
        fields = intent_for(600, block_hex=big)
        fields["shares_json"][300]["share_id"] = "y" * (5 * 1024 * 1024)
        prepared = codec.prepare_candidate_intent(fields, chunk_bytes=codec.CANDIDATE_BODY_CHUNK_BYTES)
        spool = self._spool(prepared)
        hydrated = codec.prepared_intent_from_spool(spool, accepted_at_present=True, accepted_at_ms=123)
        self.assertEqual(hydrated["block_hex"], big)
        self.assertEqual(hydrated["shares_json"][300], fields["shares_json"][300])
        spool.close()

    def test_truncated_and_corrupt_spools_fail_closed(self) -> None:
        prepared = codec.prepare_candidate_intent(intent_for(300), chunk_bytes=4096)
        spool = self._spool(prepared)
        with open(spool.path, "r+b") as handle:
            handle.seek(10)
            handle.write(b"Z")
        with self.assertRaises(codec.CandidateBodyIntegrityError):
            list(spool.iter_chunks())
        with open(spool.path, "r+b") as handle:
            handle.truncate(100)
        with self.assertRaises(codec.CandidateBodyIntegrityError):
            list(spool.iter_chunks())
        spool.close()

    def test_manifest_and_span_validation(self) -> None:
        manifest = codec.prepare_candidate_intent(intent_for(3)).manifest
        self.assertEqual(codec.CandidateBodyManifest.from_json(manifest.to_json()), manifest)
        for broken in (
            {**manifest.to_json(), "chunk_count": manifest.chunk_count + 1},
            {**manifest.to_json(), "storage_version": 3},
            {**manifest.to_json(), "shares_end": manifest.byte_count + 1},
            {**manifest.to_json(), "candidate_sha256": "zz"},
        ):
            with self.subTest(broken=broken), self.assertRaises(codec.CandidateBodyIntegrityError):
                codec.CandidateBodyManifest.from_json(broken)
        with self.assertRaises(codec.CandidateBodyIntegrityError):
            codec.FieldSpan.from_json({"field": "x", "kind": "blob", "start": 0, "end": 1})

    def test_jsonb_equivalence_matches_postgres_rules(self) -> None:
        from decimal import Decimal

        self.assertTrue(codec.jsonb_equivalent({"a": Decimal("1.0"), "b": [1, 2]}, {"b": [Decimal(1), 2], "a": 1}))
        self.assertTrue(codec.jsonb_equivalent(Decimal("1E+16"), 10**16))
        self.assertFalse(codec.jsonb_equivalent(True, 1))
        self.assertFalse(codec.jsonb_equivalent({"a": 1}, {"a": 1, "b": None}))
        self.assertFalse(codec.jsonb_equivalent([1, 2], [2, 1]))


if __name__ == "__main__":
    unittest.main()

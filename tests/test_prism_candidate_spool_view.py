#!/usr/bin/env python3
"""Bounded candidate spool hydration regressions (issue #255).

Every C call the reader makes is instrumented and asserted against the
view module's hard threshold, for a body carrying an oversized share
record, an oversized nested array and oversized top-level strings. The
remaining tests pin the adapter contract root builds on: streamed strings
that never split an escape or a surrogate pair, mapping/sequence views
that compare with plain values, verbatim byte access that preserves every
digest, random access through the disk index, legacy hinted pages, failure
classification (corrupt versus closed versus resource pressure), ownership
under retained exceptions with the cyclic collector disabled, and the
supervised isolated-record helper.
"""

from __future__ import annotations

import contextlib
import gc
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism import audit_bundle_view as audit  # noqa: E402
from lab.prism import candidate_codec as codec  # noqa: E402
from lab.prism import candidate_spool_view as views  # noqa: E402
from lab.prism.share_ledger import block_candidate_identity  # noqa: E402

HASH_A = "aa" * 32
PARENT = "bb" * 32
COMPACT = (",", ":")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=COMPACT).encode("utf-8")


def oracle_bytes(intent: dict[str, Any]) -> bytes:
    identity = block_candidate_identity({**intent, "block_hash_hex": intent["block_hash_hex"].lower()})
    return canonical(identity)


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


class _LoadsProbe:
    """Record the input and output size of every ``json.loads`` call."""

    def __init__(self) -> None:
        self.inputs: list[int] = []
        self.outputs: list[int] = []
        self._real = json.loads

    def loads(self, data: Any, *args: Any, **kwargs: Any) -> Any:
        value = self._real(data, *args, **kwargs)
        self.inputs.append(len(data))
        if isinstance(value, (str, list, tuple, dict)):
            self.outputs.append(len(value))
        else:
            self.outputs.append(1)
        return value


class SpoolFixture(unittest.TestCase):
    def bounded(self, threshold: int) -> Any:
        """Lower the view threshold for a test, keeping the invariants.

        The encoder spans a top-level field only above its fast-path
        ceiling and the reader slices strings below the threshold, so both
        knobs follow the threshold down; in production the ordering is the
        same (64 KiB fast path, 64 KiB slices, 1 MiB threshold).
        """
        stack = contextlib.ExitStack()
        stack.enter_context(mock.patch.object(views, "SPOOL_VIEW_DECODE_BYTES", threshold))
        stack.enter_context(mock.patch.object(codec, "CODEC_FAST_PATH_BYTES", min(codec.CODEC_FAST_PATH_BYTES, threshold)))
        stack.enter_context(mock.patch.object(views, "SPOOL_VIEW_STRING_SLICE_BYTES", min(views.SPOOL_VIEW_STRING_SLICE_BYTES, threshold)))
        return stack

    def spool(self, fields: dict[str, Any], *, chunk_bytes: int = 4096) -> tuple[codec.PreparedCandidateIntent, codec.SpoolCandidateBody]:
        prepared = codec.prepare_candidate_intent(fields, chunk_bytes=chunk_bytes)
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        body = codec.write_spool_from_body(
            os.path.join(directory, "b.body"), os.path.join(directory, "b.idx"), prepared.body
        )
        self.addCleanup(body.close)
        return prepared, body

    def hydrate(self, fields: dict[str, Any], **kwargs: Any) -> tuple[codec.PreparedCandidateIntent, codec.SpoolCandidateBody]:
        _prepared, body = self.spool(fields, **kwargs)
        hydrated = codec.prepared_intent_from_spool(body, accepted_at_present=True, accepted_at_ms=123)
        return hydrated, body


class BoundednessTests(SpoolFixture):
    def test_every_reader_c_call_is_bounded_by_the_threshold(self) -> None:
        threshold = 32 * 1024
        big = ("é\"\\" + "y" * 500 + "\U0001F600") * 400  # ~200 KiB encoded
        fields = intent_for(700, username=big, block_hex="ab" * 40_000, coinbase_tx_hex="cd" * 40_000)
        fields["shares_json"][300]["share_id"] = "s" * (6 * threshold)
        fields["shares_json"][300]["nested"] = [{"k": i, "v": "w" * 100} for i in range(2000)]
        fields["found_block"]["witness"] = [[i, i * 1.5, f"leaf-{i}"] for i in range(20_000)]
        fields["pending_share"] = {"share_id": "p" * (3 * threshold), "accepted_at_ms": 123, "miner_id": "m"}
        probe = _LoadsProbe()
        reads: list[int] = []
        real_read = codec.SpoolCandidateBody.read_span

        def read_span(self_: Any, start: int, end: int) -> bytes:
            reads.append(end - start)
            return real_read(self_, start, end)

        with self.bounded(threshold), \
                mock.patch.object(codec.json, "loads", probe.loads), \
                mock.patch.object(views.json, "loads", probe.loads), \
                mock.patch.object(codec.SpoolCandidateBody, "read_span", read_span):
            hydrated, _body = self.hydrate(fields)
            # Touch everything: every share, every member of the oversized
            # record, every top-level fact, every streamed string.
            self.assertEqual(len(list(hydrated["shares_json"])), 700)
            record = hydrated["shares_json"][300]
            self.assertIsInstance(record, views.SpoolObjectView)
            self.assertEqual(dict(record).keys(), fields["shares_json"][300].keys())
            self.assertEqual(record["share_id"], fields["shares_json"][300]["share_id"])
            self.assertEqual(list(record["nested"]), fields["shares_json"][300]["nested"])
            self.assertIsInstance(hydrated["username"], views.SpoolStringView)
            self.assertEqual("".join(hydrated["username"].iter_text_chunks()), big)
            self.assertIsInstance(hydrated["block_hex"], views.SpoolStringView)
            self.assertEqual(hydrated["block_hex"], "ab" * 40_000)
            self.assertEqual(hydrated["coinbase_tx_hex"], "cd" * 40_000)
            self.assertIsInstance(hydrated["coinbase_tx_hex"], str)
            witness = hydrated["found_block"]["witness"]
            self.assertIsInstance(witness, views.SpoolArrayView)
            self.assertEqual(len(witness), 20_000)
            self.assertEqual(list(witness), fields["found_block"]["witness"])
            self.assertEqual(witness[19_999], fields["found_block"]["witness"][19_999])
            self.assertEqual(hydrated["pending_share"]["accepted_at_ms"], 123)
            self.assertEqual(hydrated["pending_share"]["share_id"], "p" * (3 * threshold))
            header = hydrated.replay_header()
            self.assertTrue(header["oversized"])
            self.assertIsNone(header["username"])
            self.assertEqual(header["pending_share"]["accepted_at_ms"], 123)
        self.assertTrue(probe.inputs)
        # The skeleton is the one call bounded by structure rather than the
        # threshold: every non-spanned top-level field is under the encoder's
        # fast-path ceiling, and this intent has twenty of them.
        skeleton_bound = 20 * threshold
        self.assertLessEqual(max(probe.inputs), max(threshold + 2, skeleton_bound))
        self.assertLessEqual(sorted(probe.inputs)[-2], threshold + 2)
        self.assertLessEqual(max(reads), max(threshold, views.SPOOL_VIEW_READ_BYTES))
        string_outputs = [size for size, inp in zip(probe.outputs, probe.inputs) if inp <= views.SPOOL_VIEW_STRING_SLICE_BYTES + 16]
        self.assertTrue(string_outputs)
        self.assertLessEqual(max(probe.outputs), max(threshold, codec.CODEC_BATCH_RECORDS * 4))

    def test_share_id_above_the_page_decode_ceiling_is_a_streamed_string(self) -> None:
        # The exact defect: one record over CODEC_PAGE_DECODE_BYTES made the
        # page "not valid JSON". It is now an object view whose share_id
        # streams; every other record on the page stays a plain dict.
        giant = "g" * (codec.CODEC_PAGE_DECODE_BYTES + 1024 * 1024)
        fields = intent_for(600)
        fields["shares_json"][300]["share_id"] = giant
        probe = _LoadsProbe()
        with mock.patch.object(codec.json, "loads", probe.loads), mock.patch.object(views.json, "loads", probe.loads):
            hydrated, body = self.hydrate(fields, chunk_bytes=codec.CANDIDATE_BODY_CHUNK_BYTES)
            shares = hydrated["shares_json"]
            record = shares[300]
            self.assertIsInstance(record, views.SpoolObjectView)
            self.assertIsInstance(record["share_id"], views.SpoolStringView)
            self.assertEqual(record["share_id"].byte_length, len(giant) + 2)
            self.assertEqual(record["share_id"].text_length(), len(giant))
            self.assertTrue(record["share_id"] == giant)
            self.assertEqual(record["share_seq"], 300)
            self.assertEqual(record, fields["shares_json"][300])
            self.assertIsInstance(shares[299], dict)
            self.assertIsInstance(shares[301], dict)
            self.assertEqual(shares[299], fields["shares_json"][299])
            self.assertEqual(shares[599], fields["shares_json"][599])
        self.assertLessEqual(max(probe.inputs), views.SPOOL_VIEW_DECODE_BYTES + 2)
        digest = hashlib.sha256()
        body.write_chunks(lambda chunk: digest.update(chunk.data))
        self.assertEqual(digest.hexdigest(), hashlib.sha256(oracle_bytes(fields)).hexdigest())


class RoundTripTests(SpoolFixture):
    def test_small_and_unicode_strings_round_trip_at_every_slice_cut(self) -> None:
        unit = "\U0001F600" * 5 + "\\" * 7 + 'q"' + "é" + "\U0001F600" + "\\\\u0041" + "\n\t"
        text = unit * 40
        fields = intent_for(1, username=text, block_hex="0" * 10)
        with self.bounded(256):
            hydrated, _body = self.hydrate(fields)
            view = hydrated["username"]
            self.assertIsInstance(view, views.SpoolStringView)
            encoded = json.dumps(text)
            for slice_bytes in range(32, 97):
                with self.subTest(slice_bytes=slice_bytes):
                    chunks = list(view.iter_text_chunks(slice_bytes=slice_bytes))
                    self.assertEqual("".join(chunks), text)
                    self.assertTrue(all(chunks))
            self.assertEqual(view, text)
            self.assertNotEqual(view, text + "x")
            self.assertTrue(text == view)
            self.assertEqual("".join(view.iter_encoded_chunks()), encoded)
            self.assertEqual(b"".join(view.iter_byte_chunks(chunk_bytes=7)), encoded.encode("ascii"))
            self.assertEqual(view.canonical_json_sha256(), hashlib.sha256(encoded.encode()).hexdigest())
            self.assertEqual(views.materialize_spool_views(view), text)
            self.assertEqual("".join(views.iter_string_text_chunks(view)), text)
            self.assertEqual(list(views.iter_string_text_chunks("plain")), ["plain"])
            with self.assertRaises(TypeError):
                str(view)
            self.assertIn("SpoolStringView", repr(view))
            self.assertTrue(view)
            # Small strings are plain values with the historical shape.
            self.assertEqual(hydrated["block_hex"], "0" * 10)
            self.assertIsInstance(hydrated["block_hex"], str)
            self.assertEqual(hydrated["shares_json"][0]["share_id"], share(0)["share_id"])

    def test_raw_utf8_and_split_numbers_survive_tiny_reads(self) -> None:
        # A body written with raw UTF-8 (a legacy encoder that did not
        # escape) and read five bytes at a time: no code point, escape or
        # number token may be split by a read boundary.
        witness = [
            1, -2, 3.5e-7, 12345678901234567890, -0.0, 1e300, True, None,
            'trap},{"k":1', "\\\\\"", "☃ snow \U0001F600", {"n": [1.25, "x", {"deep": [[], {}]}]}, [], {},
        ] * 40
        fields = intent_for(1, found_block={"network_difficulty": 1.0, "witness": witness})
        _prepared, body = self.spool(fields)
        raw_path = body.path + ".raw"
        text = json.loads(body.read_span(0, body.manifest.byte_count).decode("ascii"))
        raw = json.dumps(text, sort_keys=True, separators=COMPACT, ensure_ascii=False).encode("utf-8")
        with open(raw_path, "wb") as handle:
            handle.write(raw)
        self.addCleanup(lambda: os.path.exists(raw_path) and os.unlink(raw_path))
        start = raw.index(b'"found_block":{') + len(b'"found_block":')
        end = raw.index(b',"parent_hash"')
        raw_body = mock.Mock(spec=codec.SpoolCandidateBody)
        raw_body.alive = True
        raw_body.read_span = lambda s, e: raw[s:e]
        with mock.patch.object(views, "SPOOL_VIEW_READ_BYTES", 5), \
                mock.patch.object(views, "SPOOL_VIEW_STRING_SLICE_BYTES", 33), \
                self.bounded(40):
            found = views.decode_body_span(raw_body, start, end)
            self.assertIsInstance(found, views.SpoolObjectView)
            items = found["witness"]
            self.assertIsInstance(items, views.SpoolArrayView)
            self.assertEqual(len(items), len(witness))
            self.assertEqual(list(items), witness)
            self.assertEqual(items[10], witness[10])
            self.assertEqual(items[-1], witness[-1])
            self.assertEqual(items[3:6], tuple(witness[3:6]))
            self.assertEqual(items, witness)
            self.assertEqual(found, fields["found_block"])
            self.assertEqual("".join(items.iter_encoded_chunks()), json.dumps(witness, sort_keys=True, separators=COMPACT, ensure_ascii=False))

    def test_random_access_and_slicing_through_the_disk_index(self) -> None:
        records = [share(index) for index in range(1300)]
        records[700]["share_id"] = 'trap},{"accepted_at_ms":1'
        records[511]["nested"] = [{"a": 1}, {"b": "\\\"},{"}]
        records[900]["share_id"] = "big" * 30_000
        fields = intent_for(0, shares_json=records)
        with self.bounded(16 * 1024):
            hydrated, _body = self.hydrate(fields)
            shares = hydrated["shares_json"]
            self.assertIsInstance(shares, codec.SpoolJsonArraySequence)
            self.assertEqual(len(shares), 1300)
            for index in (0, 1, 255, 256, 511, 700, 899, 900, 901, 1299, -1, -300):
                with self.subTest(index=index):
                    self.assertEqual(shares[index], records[index])
            self.assertEqual(shares[898:903], tuple(records[898:903]))
            self.assertEqual(shares[::400], tuple(records[::400]))
            with self.assertRaises(IndexError):
                shares[1300]
            with self.assertRaises(TypeError):
                shares["x"]
            self.assertIsInstance(shares[900], views.SpoolObjectView)
            self.assertIsInstance(shares[900]["share_id"], views.SpoolStringView)
            self.assertEqual(list(shares), records)
            self.assertTrue(shares == records)
            self.assertFalse(shares == records[:-1])
            expected_array = canonical(records)
            self.assertEqual(shares.canonical_json_sha256(), hashlib.sha256(expected_array).hexdigest())
            self.assertEqual(b"".join(shares.iter_byte_chunks()), expected_array)
            staged = bytearray(b"[")
            for data, _count in shares.canonical_share_pages():
                if staged[-1:] != b"[" and staged[-1:] != b",":
                    staged += b","
                staged += data
            staged += b"]"
            self.assertEqual(json.loads(bytes(staged)), records)


class RestagingTests(SpoolFixture):
    def test_hydrated_body_and_every_field_keep_their_bytes(self) -> None:
        fields = intent_for(600, username="u" * 200_000, block_hex="ef" * 100_000)
        fields["shares_json"][300]["share_id"] = "y" * 300_000
        fields["found_block"]["witness"] = [{"leaf": i} for i in range(30_000)]
        fields["pending_share"] = {"share_id": "p" * 200_000, "accepted_at_ms": 123}
        expected = oracle_bytes(fields)
        with self.bounded(64 * 1024):
            hydrated, body = self.hydrate(fields)
            got = bytearray()
            hydrated.body.write_chunks(lambda chunk: got.extend(chunk.data))
            self.assertEqual(bytes(got), expected)
            self.assertIs(codec.prepare_candidate_intent(hydrated), hydrated)
            for name in ("username", "block_hex"):
                view = hydrated[name]
                self.assertIsInstance(view, views.SpoolStringView)
                self.assertEqual(b"".join(view.iter_byte_chunks()), canonical(fields[name]))
            found = hydrated["found_block"]
            self.assertIsInstance(found, views.SpoolObjectView)
            self.assertEqual(b"".join(found.iter_byte_chunks()), canonical(fields["found_block"]))
            self.assertEqual(found.canonical_json_sha256(), hashlib.sha256(canonical(fields["found_block"])).hexdigest())
            self.assertEqual(b"".join(found["witness"].iter_byte_chunks()), canonical(fields["found_block"]["witness"]))
            record = hydrated["shares_json"][300]
            self.assertEqual(b"".join(record.iter_byte_chunks()), canonical(fields["shares_json"][300]))
            self.assertEqual(b"".join(record["share_id"].iter_byte_chunks()), canonical(fields["shares_json"][300]["share_id"]))
            # Re-staging the hydrated body to a second spool reproduces it
            # byte for byte, oversized page and all, and hydrates equal.
            directory = tempfile.mkdtemp()
            self.addCleanup(shutil.rmtree, directory, True)
            second = codec.write_spool_from_body(os.path.join(directory, "c.body"), os.path.join(directory, "c.idx"), hydrated.body)
            self.addCleanup(second.close)
            self.assertEqual(second.manifest, body.manifest)
            again = codec.prepared_intent_from_spool(second, accepted_at_present=True, accepted_at_ms=123)
            self.assertEqual(again["shares_json"][300], fields["shares_json"][300])
            self.assertEqual(again["found_block"], fields["found_block"])
            # The shares sequence re-encodes through the verbatim page
            # protocol, streaming the oversized page as raw pieces.
            restaged = codec.prepare_candidate_intent(intent_for(0, shares_json=hydrated["shares_json"]), chunk_bytes=4096)
            regot = bytearray()
            restaged.body.write_chunks(lambda chunk: regot.extend(chunk.data))
            self.assertEqual(bytes(regot), oracle_bytes(intent_for(0, shares_json=fields["shares_json"])))

    def test_streaming_encoder_and_materializer_accept_spool_views(self) -> None:
        fields = intent_for(2, username="w" * 5000, found_block={"network_difficulty": 1.0, "witness": list(range(3000))})
        with self.bounded(1024):
            hydrated, _body = self.hydrate(fields)
            username = hydrated["username"]
            found = hydrated["found_block"]
            self.assertIsInstance(username, views.SpoolStringView)
            self.assertIsInstance(found, views.SpoolObjectView)
            payload = {"u": username, "f": found, "w": found["witness"], "n": 1}
            plain = {"u": fields["username"], "f": fields["found_block"], "w": fields["found_block"]["witness"], "n": 1}
            self.assertEqual("".join(audit.iter_json_chunks(payload, sort_keys=True)), json.dumps(plain, sort_keys=True, separators=COMPACT))
            self.assertEqual("".join(audit.iter_json_chunks(payload)), json.dumps(plain, separators=COMPACT))
            self.assertEqual(audit.materialize_json(payload), plain)
            self.assertEqual(views.materialize_spool_views(payload), plain)
            self.assertEqual(audit.streamed_sha256_json_hex(payload), hashlib.sha256(canonical(plain)).hexdigest())
            self.assertTrue(views.is_spool_view(username))
            self.assertFalse(views.is_spool_view("x"))


class MappingViewTests(SpoolFixture):
    def test_object_view_mapping_protocol_and_pending_stamp(self) -> None:
        members: dict[str, Any] = {f"k{i:04d}": {"v": i, "s": "x" * 50} for i in range(600)}
        members["share_id"] = "S" * 5000
        members["accepted_at_ms"] = 123
        # The body stores keys sorted and the header projection keeps at
        # most 32 members (the historical shape), so the pending share stays
        # small in members while its share_id alone crosses the threshold.
        pending = {"share_id": "S" * 5000, "accepted_at_ms": 123, "miner_id": "m"}
        fields = intent_for(1, pending_share=pending, found_block=dict(members))
        with self.bounded(2048), \
                mock.patch.object(views, "SPOOL_VIEW_INDEX_MEMORY_ENTRIES", 8):
            hydrated, _body = self.hydrate(fields)
            pending = hydrated["pending_share"]
            self.assertIsInstance(pending, dict)
            self.assertEqual(pending["accepted_at_ms"], 123)
            self.assertIsInstance(pending["share_id"], views.SpoolStringView)
            self.assertEqual(pending["share_id"], "S" * 5000)
            self.assertEqual(pending["miner_id"], "m")
            self.assertEqual(set(pending), {"share_id", "accepted_at_ms", "miner_id"})
            header = hydrated.replay_header()
            self.assertTrue(header["oversized"])
            self.assertEqual(header["pending_share"]["accepted_at_ms"], 123)
            self.assertIsNone(header["pending_share"]["share_id"])
            self.assertTrue(header["accepted_at_present"])
            found = hydrated["found_block"]
            self.assertIsInstance(found, views.SpoolObjectView)
            # The body stores keys sorted, as the historical json.loads did.
            ordered = sorted(members)
            self.assertEqual(len(found), len(members))
            self.assertEqual(list(found), ordered)
            self.assertEqual(list(found.keys()), ordered)
            self.assertEqual([key for key, _ in found.items()], ordered)
            self.assertEqual(list(found.values())[1], members[ordered[1]])
            self.assertEqual(found.get("absent", "d"), "d")
            self.assertIn("k0599", found)
            self.assertNotIn("nope", found)
            self.assertNotIn(5, found)
            with self.assertRaises(KeyError):
                found["nope"]
            self.assertEqual(found, members)
            self.assertTrue(members == found)
            self.assertNotEqual(found, {**members, "extra": 1})
            self.assertNotEqual(found, {**members, "k0000": 0})
            self.assertEqual(dict(found), members)
            self.assertEqual(views.materialize_spool_views(found), members)
            self.assertTrue(found._ensure_indexed().spilled)
            self.assertIn("members=602", repr(found))
            found.close()
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                found["k0001"]
            # Without the key cache (too many members) lookups scan keys.
            with mock.patch.object(views, "SPOOL_VIEW_KEY_CACHE_ENTRIES", 4):
                again = views.SpoolObjectView(found.body, *found.byte_range)
                self.assertEqual(again["k0450"], members["k0450"])
                self.assertIsNone(again._keys)
                self.assertEqual(again["share_id"], members["share_id"])
                with self.assertRaises(KeyError):
                    again["missing"]

    def test_decoded_metadata_boundary_and_lazy_block(self) -> None:
        fields = intent_for(1, coinbase_tx_hex="ab" * 3000, block_hex="cd" * 3000, extranonce1_hex="ef" * 3000, username="u" * 6000)
        with self.bounded(1024):
            hydrated, _body = self.hydrate(fields)
            for name in ("coinbase_tx_hex", "extranonce1_hex"):
                self.assertIsInstance(hydrated[name], str)
                self.assertEqual(hydrated[name], fields[name])
            self.assertIsInstance(hydrated["block_hex"], views.SpoolStringView)
            self.assertEqual(hydrated["block_hex"].byte_length, len(fields["block_hex"]) + 2)
            self.assertEqual(b"".join(hydrated["block_hex"].iter_byte_chunks()), canonical(fields["block_hex"]))
            self.assertIsInstance(hydrated["username"], views.SpoolStringView)
            with self.assertRaises(TypeError) as caught:
                str(hydrated["username"])
            self.assertNotIsInstance(caught.exception, codec.CandidateBodyIntegrityError)
            self.assertEqual(codec.decode_json_string_span(hydrated.body, hydrated.body.index.span("username")), fields["username"])
            self.assertIn("block_hex", hydrated)
            self.assertEqual(hydrated.block_hash, HASH_A)


class FailureTests(SpoolFixture):
    def test_corrupt_truncated_missing_and_closed_bodies_are_classified(self) -> None:
        fields = intent_for(300, username="u" * 20_000)
        fields["found_block"]["witness"] = list(range(5000))
        with self.bounded(4096):
            hydrated, body = self.hydrate(fields)
            username = hydrated["username"]
            witness = hydrated["found_block"]["witness"]
            self.assertEqual(username, fields["username"])
            self.assertEqual(len(witness), 5000)
            span = body.index.span("username")
            # An invalid escape inside the streamed string: corruption.
            with open(body.path, "r+b") as handle:
                handle.seek(span.start + 10)
                handle.write(b"\\x")
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                list(username.iter_text_chunks())
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                list(body.iter_chunks())
            # Unbalanced delimiters in a lazy array: corruption.
            witness_span = body.index.span("found_block")
            with open(body.path, "r+b") as handle:
                handle.seek(witness_span.start + 20)
                handle.write(b"]")
            fresh = views.SpoolObjectView(body, witness_span.start, witness_span.end)
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                fresh["witness"]
            # A page index that lies about its span: corruption, not a crash.
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                codec.SpoolJsonArraySequence(body, replace(body.index.span("shares_json"), start=10**9))[0]
            # Truncation inside the share array: the read is shorter than
            # the manifest (username follows the shares in key order).
            with open(body.path, "r+b") as handle:
                handle.truncate(body.index.span("shares_json").start + 100)
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                hydrated["shares_json"][299]
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                b"".join(username.iter_byte_chunks())
            # A vanished index file: corruption-class, never a bare OSError.
            os.unlink(body.index.path)
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                hydrated["shares_json"][0]
            # A released body: every view fails closed.
            hydrated.release()
            self.assertFalse(body.alive)
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                username.iter_text_chunks().__next__()
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                views.SpoolArrayView(body, witness_span.start, witness_span.end)[0]
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                hydrated["shares_json"][1]

    def test_mismatched_delimiters_and_empty_values_are_corruption(self) -> None:
        raw = b'{"a":[1,2}'
        fake = mock.Mock(spec=codec.SpoolCandidateBody)
        fake.alive = True
        fake.read_span = lambda s, e: raw[s:e]
        with self.bounded(2):
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                views.decode_body_span(fake, 5, 10)
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                views.decode_body_span(fake, 5, 5)
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                list(views.iter_member_spans(fake, 1, 9))
        with self.assertRaises(codec.CandidateBodyIntegrityError):
            list(views.iter_json_string_text_chunks(fake, 0, 4))

    def test_legacy_hinted_pages_stay_readable_ignoring_bad_hints(self) -> None:
        records = [share(index) for index in range(900)]
        records[400]["share_id"] = 'trap},{"share_seq":9' * 20
        records[650]["nested"] = [{"b": "\\\"},{"}] * 3
        fields = intent_for(0, shares_json=records)
        _prepared, body = self.spool(fields)
        span = body.index.span("shares_json")
        data = body.read_span(span.start, span.end)
        # A legacy index whose hints came from a substring scan: one lands
        # inside a quoted trap, one inside a nested object.
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        legacy_path = os.path.join(directory, "legacy.body")
        shutil.copyfile(body.path, legacy_path)
        index = codec.SpoolFieldIndex.create(os.path.join(directory, "legacy.idx"))
        hints = [span.start + 1]
        hints.append(span.start + data.index(b"trap},{") + 5)
        hints.append(span.start + data.index(b'\\"},{"') + 4)
        hinted = replace(span, page_count=len(hints), pages_exact=False)
        index.begin_field(hinted)
        index.append_pages("shares_json", [(offset, 0) for offset in hints])
        legacy = codec.SpoolCandidateBody(legacy_path, body.manifest, index)
        self.addCleanup(legacy.close)
        sequence = codec.SpoolJsonArraySequence(legacy, hinted)
        with self.bounded(8192):
            self.assertEqual(sequence[400], records[400])
            self.assertEqual(sequence[899], records[899])
            self.assertEqual(sequence[0], records[0])
            self.assertEqual(list(sequence), records)
            self.assertFalse(legacy.index.span("shares_json").pages_exact)
            self.assertIsNotNone(sequence._walk_index)
            staged = codec.prepare_candidate_intent(intent_for(0, shares_json=sequence), chunk_bytes=4096)
            self.assertEqual(staged.candidate_sha256, hashlib.sha256(oracle_bytes(fields)).hexdigest())
            sequence.close()


class OwnershipTests(SpoolFixture):
    def test_scratch_indices_close_on_error_and_by_refcount_with_gc_disabled(self) -> None:
        fields = intent_for(1, found_block={"network_difficulty": 1.0, "witness": list(range(4000))})
        created: list[Any] = []
        real_temporary_file = tempfile.TemporaryFile

        def recording_temporary_file(*args: Any, **kwargs: Any) -> Any:
            handle = real_temporary_file(*args, **kwargs)
            created.append(handle)
            return handle

        gc_was_enabled = gc.isenabled()
        gc.disable()
        self.addCleanup(lambda: gc.enable() if gc_was_enabled else None)
        with self.bounded(512), \
                mock.patch.object(views, "SPOOL_VIEW_INDEX_MEMORY_ENTRIES", 4), \
                mock.patch.object(views.tempfile, "TemporaryFile", recording_temporary_file):
            hydrated, body = self.hydrate(fields)
            found = hydrated["found_block"]
            # 1. An error while the index is being built closes its scratch
            #    file before the exception leaves, even though the retained
            #    exception keeps the frame (and the index) alive.
            real_spans = views.iter_item_spans

            def failing_spans(*args: Any, **kwargs: Any) -> Any:
                for count, item in enumerate(real_spans(*args, **kwargs)):
                    if count == 50:
                        raise OSError("disk went away")
                    yield item

            retained: list[BaseException] = []
            with mock.patch.object(views, "iter_item_spans", failing_spans):
                try:
                    len(found["witness"])
                except OSError as exc:
                    retained.append(exc)
            self.assertEqual(len(retained), 1)
            self.assertEqual(len(created), 1)
            self.assertTrue(created[0].closed)
            # 2. A successful index spills to a scratch file that closes when
            #    the last view referencing it is dropped -- by reference
            #    count alone, since no view takes part in a cycle.
            witness = found["witness"]
            self.assertEqual(len(witness), 4000)
            self.assertEqual(len(created), 2)
            self.assertFalse(created[1].closed)
            fd = created[1].fileno()
            os.fstat(fd)
            del witness
            self.assertTrue(created[1].closed)
            with self.assertRaises(OSError):
                os.fstat(fd)
            # 3. Resource pressure while spilling is retryable, and leaves
            #    no descriptor behind.
            with mock.patch.object(views.tempfile, "TemporaryFile", side_effect=OSError("no temp")):
                with self.assertRaises(audit.ArtifactResourcePressure) as pressure:
                    len(views.SpoolArrayView(body, *found["witness"].byte_range))
            self.assertNotIsInstance(pressure.exception, codec.CandidateBodyIntegrityError)
            self.assertEqual(len(created), 2)
            # 4. Explicit release: the spool pair is unlinked while a view
            #    (kept alive by a retained traceback) still exists, and the
            #    view fails closed instead of reading a stale file.
            witness = found["witness"]
            paths = (body.path, body.index.path)
            try:
                raise RuntimeError("retained while views are alive")
            except RuntimeError as exc:
                retained.append(exc)
            hydrated.release()
            self.assertFalse(any(os.path.exists(path) for path in paths))
            with self.assertRaises(codec.CandidateBodyIntegrityError):
                witness[0]
            views.close_spool_views(hydrated.facts)
            self.assertTrue(all(handle.closed for handle in created))
            del retained

    def test_hydration_failure_leaves_no_open_scratch(self) -> None:
        # A corrupt member value inside a lazy pending_share: hydration's
        # shallow copy fails after the (spilled) member index was built, the
        # exception is retained, and the scratch file is closed anyway.
        pending = {f"k{i:03d}": {"s": "x" * 20, "v": i} for i in range(40)}
        pending["accepted_at_ms"] = 123
        fields = intent_for(2, pending_share=pending)
        with self.bounded(512):
            _prepared, body = self.spool(fields)
        span = body.index.span("pending_share")
        data = body.read_span(span.start, span.end)
        with open(body.path, "r+b") as handle:
            handle.seek(span.start + data.index(b'"v":7}') + 4)
            handle.write(b"x")
        created: list[Any] = []
        real_temporary_file = tempfile.TemporaryFile

        def recording(*args: Any, **kwargs: Any) -> Any:
            handle = real_temporary_file(*args, **kwargs)
            created.append(handle)
            return handle

        with self.bounded(512), \
                mock.patch.object(views, "SPOOL_VIEW_INDEX_MEMORY_ENTRIES", 2), \
                mock.patch.object(views.tempfile, "TemporaryFile", recording):
            retained: list[BaseException] = []
            try:
                codec.prepared_intent_from_spool(body, accepted_at_present=True, accepted_at_ms=123)
            except codec.CandidateBodyIntegrityError as exc:
                retained.append(exc)
        self.assertEqual(len(retained), 1)
        self.assertEqual(len(created), 1)
        self.assertTrue(all(handle.closed for handle in created))
        del retained


class HelperSupervisionTests(unittest.TestCase):
    def source(self, raw: bytes) -> audit.ArtifactSource:
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        path = os.path.join(directory, "record.json")
        with open(path, "wb") as handle:
            handle.write(raw)
        source = audit.ArtifactSource(os.open(path, os.O_RDONLY), path=Path(path))
        self.addCleanup(source.close)
        return source

    def helper_threads(self) -> list[str]:
        return [thread.name for thread in threading.enumerate() if thread.name.startswith("prism-audit-record-helper")]

    def test_hanging_helper_obeys_the_deadline_and_is_reaped(self) -> None:
        raw = b'{"a":"' + b"x" * 5000 + b'"}'
        source = self.source(raw)
        real_popen = subprocess.Popen
        children: list[subprocess.Popen[bytes]] = []

        def sleeping_child(_args: Any, **kwargs: Any) -> Any:
            process = real_popen([sys.executable, "-c", "import time; time.sleep(60)"], **kwargs)
            children.append(process)
            return process

        started = time.monotonic()
        with mock.patch.object(audit.subprocess, "Popen", sleeping_child):
            with self.assertRaises(audit.ArtifactResourcePressure):
                audit.normalize_record_isolated(source, 0, len(raw), timeout_seconds=0.3)
        self.assertLess(time.monotonic() - started, 5.0)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].poll())
        self.assertTrue(all(pipe.closed for pipe in (children[0].stdin, children[0].stdout, children[0].stderr)))
        self.assertEqual(self.helper_threads(), [])

    def test_cancellation_abandons_the_helper(self) -> None:
        raw = b'{"a":"' + b"x" * 5000 + b'"}'
        source = self.source(raw)
        real_popen = subprocess.Popen
        children: list[subprocess.Popen[bytes]] = []
        calls = {"count": 0}

        class Cancelled(Exception):
            pass

        def cancellation() -> None:
            calls["count"] += 1
            if calls["count"] > 2:
                raise Cancelled()

        def sleeping_child(_args: Any, **kwargs: Any) -> Any:
            process = real_popen([sys.executable, "-c", "import time; time.sleep(60)"], **kwargs)
            children.append(process)
            return process

        with mock.patch.object(audit.subprocess, "Popen", sleeping_child):
            with self.assertRaises(Cancelled):
                audit.normalize_record_isolated(source, 0, len(raw), timeout_seconds=30.0, cancellation=cancellation)
        self.assertIsNotNone(children[0].poll())
        self.assertEqual(self.helper_threads(), [])

    def test_admission_slots_are_shared_and_deadline_bounded(self) -> None:
        raw = b'{"a":1}'
        source = self.source(raw)
        admission = audit.HelperAdmission(1)
        admission.acquire(lambda: None)
        try:
            with mock.patch.object(audit.subprocess, "Popen") as spawn:
                with self.assertRaises(audit.ArtifactResourcePressure):
                    audit.normalize_record_isolated(source, 0, len(raw), timeout_seconds=0.2, admission=admission)
                spawn.assert_not_called()
        finally:
            admission.release()
        # The slot is released after use, so a second run proceeds.
        record = audit.normalize_record_isolated(source, 0, len(raw), admission=admission)
        try:
            self.assertEqual(record, {"a": 1})
        finally:
            record.close()
        admission.acquire(lambda: None)
        admission.release()

    def test_helper_exit_statuses_and_noisy_diagnostics_are_classified(self) -> None:
        raw = b'{"a":"' + b"x" * 100 + b'"}'
        source = self.source(raw)
        real_popen = subprocess.Popen

        def child(script: str) -> Any:
            def spawn(_args: Any, **kwargs: Any) -> Any:
                return real_popen([sys.executable, "-c", script], **kwargs)
            return spawn

        noisy = "import sys; sys.stdin.read(); sys.stderr.write('e' * 1000000); sys.stderr.flush(); sys.exit(2)"
        with mock.patch.object(audit.subprocess, "Popen", child(noisy)):
            with self.assertRaises(json.JSONDecodeError) as malformed:
                audit.normalize_record_isolated(source, 0, len(raw), timeout_seconds=30.0)
        self.assertIsInstance(malformed.exception, audit.CanonicalArtifactSyntaxError)
        self.assertLessEqual(len(str(malformed.exception)), audit.RAW_RECORD_HELPER_DIAGNOSTIC_BYTES + 100)
        with mock.patch.object(audit.subprocess, "Popen", child("import sys; sys.stdin.read(); sys.exit(3)")):
            with self.assertRaises(audit.ArtifactResourcePressure):
                audit.normalize_record_isolated(source, 0, len(raw), timeout_seconds=30.0)
        with mock.patch.object(audit.subprocess, "Popen", child("import sys; sys.stdin.read(); sys.stdout.write('not json\\n'); sys.stdout.flush()")):
            with self.assertRaises(audit.ArtifactResourcePressure):
                audit.normalize_record_isolated(source, 0, len(raw), timeout_seconds=30.0)
        with mock.patch.object(audit.tempfile, "TemporaryFile", side_effect=OSError("disk full")):
            with self.assertRaises(audit.ArtifactResourcePressure):
                audit.normalize_record_isolated(source, 0, len(raw), timeout_seconds=30.0)
        self.assertEqual(self.helper_threads(), [])
        record = audit.normalize_record_isolated(source, 0, len(raw), member_limit=16)
        try:
            self.assertIn("a", record.omitted_members)
            self.assertEqual(record, {"a": "x" * 100})
        finally:
            record.close()

    def test_cancellation_reaches_lazy_audit_reads(self) -> None:
        bundle = {"shares": [{"share_seq": i, "share_id": "s" * 3000} for i in range(40)]}
        source_path = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, source_path, True)
        path = source_path / "bundle.json"
        path.write_bytes(json.dumps(bundle, separators=COMPACT).encode())
        state = {"cancel": False}

        class Cancelled(Exception):
            pass

        def cancellation() -> None:
            if state["cancel"]:
                raise Cancelled()

        view = audit.CanonicalAuditBundleView.scan_path(path, cancellation=cancellation, chunk_bytes=1024)
        try:
            self.assertEqual(view["shares"][39]["share_seq"], 39)
            state["cancel"] = True
            with self.assertRaises(Cancelled):
                view["shares"][39]
            with mock.patch.object(audit, "RECORD_DECODE_SOFT_LIMIT_BYTES", 512):
                with self.assertRaises(Cancelled):
                    view["shares"][3]
        finally:
            view.close()


if __name__ == "__main__":
    unittest.main()

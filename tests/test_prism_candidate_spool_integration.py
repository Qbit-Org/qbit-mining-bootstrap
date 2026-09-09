"""Lazy candidate fields remain usable by the replay and membership callers."""

import gc
import json
from pathlib import Path
import tempfile
import hashlib
import unittest
from unittest import mock
import weakref

from lab.prism.block_candidates import block_candidate_from_intent
from lab.prism.candidate_codec import prepare_candidate_intent, prepared_intent_from_spool, write_spool_from_body
from lab.prism.candidate_window import copy_text_chunks, recorded_share_ids
from tests.prism_postgres_candidate_gate import intent


class CandidateSpoolIntegrationTests(unittest.TestCase):
    def test_metadata_aggregate_and_giant_key_keep_native_decodes_bounded(self):
        fields = intent("cd" * 32, 1, credit=False)
        fields.update({f"extra_{index}": "x" * 24000 for index in range(60)})
        fields["found_block"]["k" * (2 * 1024 * 1024)] = True
        prepared = prepare_candidate_intent(fields)
        loads = json.loads
        sizes = []

        def bounded_loads(data, *args, **kwargs):
            sizes.append(len(data))
            return loads(data, *args, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            body = write_spool_from_body(str(Path(directory) / "body"), str(Path(directory) / "index"), prepared.body)
            try:
                with mock.patch("json.loads", side_effect=bounded_loads):
                    hydrated = prepared_intent_from_spool(body, accepted_at_present=True, accepted_at_ms=123)
                    self.assertEqual(hydrated["found_block"]["network_difficulty"], 1000)
                    self.assertEqual(hydrated["extra_59"], fields["extra_59"])
                self.assertLessEqual(max(sizes), 1024 * 1024)
            finally:
                body.close()

    def test_large_block_and_share_identifier_survive_replay_without_coercion(self):
        fields = intent("ab" * 32, 3, credit=False)
        fields["block_hex"] = "ab" * (1024 * 1024)
        fields["shares_json"][0]["share_id"] = "id" * (1024 * 1024)
        prepared = prepare_candidate_intent(fields)
        enabled = gc.isenabled()
        with tempfile.TemporaryDirectory() as directory:
            gc.disable()
            try:
                body = write_spool_from_body(str(Path(directory) / "body"), str(Path(directory) / "index"), prepared.body)
                observed = weakref.ref(body)
                hydrated = prepared_intent_from_spool(
                    body, accepted_at_present=prepared.accepted_at_present,
                    accepted_at_ms=prepared.accepted_at_ms,
                )
                candidate = block_candidate_from_intent(hydrated)
                self.assertTrue(callable(getattr(candidate.submission.block_hex, "iter_byte_chunks", None)))
                self.assertEqual(candidate.submission.block_hex.byte_length, len(fields["block_hex"]) + 2)
                # A changed hydrated intent must re-encode its lazy fields,
                # rather than accidentally reusing the original digest/body.
                hydrated["username"] = "changed"
                updated = prepare_candidate_intent(hydrated)
                from tests.test_prism_candidate_codec import oracle_bytes
                fields["username"] = "changed"
                self.assertEqual(updated.candidate_sha256, hashlib.sha256(oracle_bytes(fields)).hexdigest())
                del updated
                copied = b"".join(copy_text_chunks(recorded_share_ids(candidate.context.shares_json)))
                expected = b"".join(copy_text_chunks(recorded_share_ids(fields["shares_json"])))
                self.assertEqual(copied, expected)
                del candidate, hydrated, body
                self.assertIsNone(observed(), "replay's caller graph retained the body without cyclic GC")
                self.assertEqual(list(Path(directory).iterdir()), [])
            finally:
                if enabled:
                    gc.enable()


if __name__ == "__main__":
    unittest.main()

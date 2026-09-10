"""Streamed strings from a hydrated body survive every replay consumer (#255)."""

from dataclasses import replace
import json
import threading
import unittest
from types import SimpleNamespace

from lab.auxpow.stratum_codec import header_hash_hex
from lab.prism import candidate_codec as codec
from lab.prism import candidate_spool_view as views
from lab.prism import recover_pending_blocks as recovery
from lab.prism.block_candidates import block_candidate_from_intent, block_candidate_intent
from lab.prism.share_ledger import PsqlShareLedger, block_candidate_identity_sha256
from tests.test_prism_candidate_spool_view import SpoolFixture, intent_for

THRESHOLD = 32 * 1024
HEADER = bytes([7]) * 80
PENDING_SHARE = {
    "share_id": "u:" + "ab" * 32,
    "miner_id": "m",
    "order_key": "m",
    "p2mr_program_hex": "ab" * 32,
    "share_difficulty": 100,
    "network_difficulty": 1000,
    "template_height": 10,
    "job_id": "job",
    "job_issued_at_ms": 100,
    "accepted_at_ms": 123,
    "ntime": 1_700_000_000,
    "credit_policy": None,
}


def credited_fields() -> dict:
    # Above the lowered threshold the hydrated block_hex is a streamed view,
    # as a raw block of roughly 512 KiB is at the production threshold.
    return intent_for(
        3,
        block_hash_hex=header_hash_hex(HEADER),
        block_hex=HEADER.hex() + "ab" * 40_000,
        pending_share=dict(PENDING_SHARE),
        credit_share_on_accept=True,
    )


class StreamedBlockHexConsumerTests(SpoolFixture):
    def test_replayed_credit_intent_re_encodes_a_streamed_block(self):
        fields = credited_fields()
        with self.bounded(THRESHOLD):
            hydrated, _body = self.hydrate(fields)
            self.assertIsInstance(hydrated["block_hex"], views.SpoolStringView)
            replayed = replace(block_candidate_from_intent(hydrated), durable_replay=True)
            plain = replace(block_candidate_from_intent(fields), durable_replay=True)
            self.assertEqual(
                block_candidate_intent(replayed).candidate_sha256,
                block_candidate_intent(plain).candidate_sha256,
            )

    def test_streamed_username_is_decoded_for_the_worker_identity(self):
        fields = {**credited_fields(), "username": "q" * 40_000 + ".worker"}
        with self.bounded(THRESHOLD):
            hydrated, _body = self.hydrate(fields)
            self.assertIsInstance(hydrated["username"], views.SpoolStringView)
            replayed = replace(block_candidate_from_intent(hydrated), durable_replay=True)
            self.assertEqual(replayed.context.worker.username, fields["username"])
            self.assertEqual(replayed.client.username, fields["username"])
            plain = replace(block_candidate_from_intent(fields), durable_replay=True)
            self.assertEqual(
                block_candidate_intent(replayed).candidate_sha256,
                block_candidate_intent(plain).candidate_sha256,
            )

    def test_offline_recovery_reads_the_header_of_a_streamed_block(self):
        fields = credited_fields()
        block = recovery.RecoveryBlock(fields["block_hash_hex"], 10, fields["parent_hash"], "pending")
        with self.bounded(THRESHOLD):
            hydrated, _body = self.hydrate(fields)
            coordinator = SimpleNamespace(
                block_candidate_from_intent=block_candidate_from_intent,
                ledger=SimpleNamespace(hydrate_block_candidate_intent=lambda _row, **_kwargs: hydrated),
                stop_event=threading.Event(),
            )
            row = {
                "candidate": None,
                "candidate_sha256": hydrated.candidate_sha256,
                "storage_version": 2,
                "body_id": "ab" * 16,
                "replay_header": {},
                "byte_count": 0,
                "chunk_count": 0,
                "chunk_bytes": 0,
                "share_count": 0,
                "body_state": "sealed",
            }
            candidate = recovery.decode_candidate(coordinator, block, row)
            self.assertEqual(candidate.submission.block_hash_hex, block.block_hash)
            self.assertIsInstance(candidate.submission.block_hex, views.SpoolStringView)

    def test_legacy_document_omits_absent_shares_and_materializes_views(self):
        candidate_only = intent_for(0)
        del candidate_only["shares_json"]
        prepared = codec.prepare_candidate_intent(candidate_only)
        self.assertFalse(prepared.has_shares)
        document = PsqlShareLedger._legacy_candidate_document(prepared)
        self.assertNotIn("shares_json", document)
        self.assertEqual(block_candidate_identity_sha256(document), prepared.candidate_sha256)

        fields = credited_fields()
        with self.bounded(THRESHOLD):
            hydrated, _body = self.hydrate(fields)
            document = PsqlShareLedger._legacy_candidate_document(hydrated)
            self.assertIsInstance(document["block_hex"], str)
            self.assertEqual(document["block_hex"], fields["block_hex"])
            json.dumps(document)
            self.assertEqual(block_candidate_identity_sha256(document), hydrated.candidate_sha256)


if __name__ == "__main__":
    unittest.main()

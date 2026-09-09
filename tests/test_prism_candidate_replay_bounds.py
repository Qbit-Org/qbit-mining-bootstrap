"""Descriptor backpressure bounds obligations and preserves complete replay."""

from types import SimpleNamespace
import unittest
from unittest import mock

from lab.prism.candidate_codec import replay_header_from_fields
from lab.prism.candidate_store import CandidateHeaderPage
from tests.prism_postgres_candidate_gate import intent
from tests.prism_vardiff_test_support import submit_coordinator


class ReplayDescriptorBoundsTests(unittest.TestCase):
    def test_overflow_pauses_before_adoption_and_replays_every_row_after_drain(self):
        for page_size in (2, 1024):
            with self.subTest(page_size=page_size):
                server, _state, _recording = submit_coordinator()
                service = server._ensure_block_candidate_service()
                rows = []
                for index in range(1, 12):
                    fields = intent(f"{index:064x}", 0, credit=True)
                    rows.append({"block_hash": fields["block_hash_hex"], "storage_version": 1,
                                 "candidate_sha256": "ab" * 32, "cursor": index,
                                 "header": replay_header_from_fields(fields)})
                pending = list(rows)
                queries = []

                def headers(*, limit, after_cursor, max_bytes):
                    remaining = [row for row in pending if row["cursor"] > (after_cursor or 0)]
                    page = remaining[:limit]
                    queries.append((after_cursor, len(page)))
                    return CandidateHeaderPage(tuple(page), page[-1]["cursor"] if page else None,
                                               len(remaining) <= limit, len(page), False)

                server.ledger = SimpleNamespace(candidate_hydration_deferred=True,
                                                pending_block_candidate_headers=headers)
                server.config = SimpleNamespace(block=SimpleNamespace(replay_page_size=page_size))
                server._run_block_submitter_ledger_call = lambda _key, _phase, call, **_kwargs: call()
                service._collapse_superseded_block_candidates = lambda rows, **_kwargs: rows
                previews = set()
                server._begin_accepted_block_payout_preview = lambda key, **_kwargs: previews.add(key)
                server._clear_accepted_block_payout_preview = previews.discard
                server._finish_pending_share_candidate = mock.Mock()
                server._note_block_replay_enumeration_owed()
                seen = []
                writer = service.ports.share_writer()
                with mock.patch.object(writer, "adopt_pending_share"), \
                        mock.patch("lab.prism.block_candidates.MAX_BLOCK_REPLAY_DESCRIPTORS_IN_MEMORY", 3), \
                        mock.patch("builtins.print"):
                    while pending:
                        queued = server.replay_pending_block_candidates()
                        self.assertEqual(queued, min(3, len(pending)))
                        self.assertLessEqual(len(service._block_replay_floor_holders), 3)
                        self.assertEqual(len(previews), queued)
                        if len(pending) > 3:
                            self.assertTrue(server._block_replay_enumeration_owed())
                            before = len(queries)
                            self.assertEqual(server.replay_pending_block_candidates(), 0)
                            self.assertEqual(len(queries), before)
                        else:
                            self.assertFalse(server._block_replay_enumeration_owed())
                        while not service._block_replay_candidate_queue.empty():
                            descriptor = service._block_replay_candidate_queue.get_nowait()
                            seen.append(descriptor.block_hash)
                            pending[:] = [row for row in pending if row["block_hash"] != descriptor.block_hash]
                            service._drop_replay_descriptor(descriptor)
                        self.assertEqual(service._block_replay_floor_holders, {})
                        self.assertEqual(previews, set())
                self.assertEqual(seen, [row["block_hash"] for row in rows])
                self.assertEqual(server._finish_pending_share_candidate.call_count, len(rows))

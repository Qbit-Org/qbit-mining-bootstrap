"""Issue #332, defect 4: window owners that outlive their mirror.

Two contracts. A :class:`DaemonShareJsonSequence` never owns its parsed
tuple: consumers hold it for exactly their span and it releases by reference
count, so a sequence that outlives its mirror pins canonical bytes only,
never a window of dicts. And the weak ownership registry attributes every
owner by kind and bounded creation site and exports the oldest owner's age,
so a retained owner is attributable from metrics alone.

GC stays disabled inside the release tests: every assertion here is about
reference-count release, never about what a collection would reclaim.
"""

from __future__ import annotations

import gc
import json
import threading
import time
import unittest
import weakref
from types import SimpleNamespace
from unittest import mock

from lab.prism import candidate_codec as codec
from lab.prism import share_ledger as share_ledger_module
from lab.prism import window_ownership as ownership
from lab.prism.bundle_compiler import (
    _ShareWindowSerialization,
    _compact_share_payload,
    _compact_share_tail_chunks,
    _iter_build_input_chunks,
)
from lab.prism.metrics import MetricsRenderer
from lab.prism.share_ledger import (
    DaemonShareJsonSequence,
    DaemonShareWindowMirror,
    DaemonWindowMirrorDivergence,
    IncrementalShareWindow,
    PsqlShareLedger,
)
from lab.prism.window_ownership import (
    window_ownership_breakdown,
    window_ownership_snapshot,
)
from tests.test_prism_candidate_codec import intent_for, oracle_bytes
from tests.test_prism_payout_window_daemon_recenter import _canonical_items
from tests.test_prism_share_ledger import row_payload

MODULE = __name__.rsplit(".", 1)[-1]


def mirror_of(count: int, *, first: int = 1) -> DaemonShareWindowMirror:
    """A verified daemon mirror holding ``count`` canonical records."""
    records = [PsqlShareLedger._record_from_json(row_payload(i)) for i in range(first, first + count)]
    window = IncrementalShareWindow.from_full_snapshot(
        records, anchor_job_issued_at_ms=10**9, window_weight=10**12,
    )
    assert window.record_count == count
    mirror = DaemonShareWindowMirror.from_full_items(
        anchor_job_issued_at_ms=10**9, window_weight=10**12, page_size=512,
        record_count=count, canonical_items=_canonical_items(window),
        share_snapshot_sha256=window.json_records().canonical_json_sha256(),
    )
    return mirror


class NoGCTestCase(unittest.TestCase):
    def setUp(self) -> None:
        gc.collect()
        self._gc_enabled = gc.isenabled()
        gc.disable()

    def tearDown(self) -> None:
        if self._gc_enabled:
            gc.enable()
        gc.collect()


class ParsedRepresentationTests(NoGCTestCase):
    """The parsed tuple lives with its consumers, never with the sequence."""

    def setUp(self) -> None:
        super().setUp()
        self.mirror = mirror_of(40)
        self.sequence = self.mirror.json_records()
        self.before = window_ownership_snapshot()
        self.walks = 0
        original = share_ledger_module._walk_canonical_items

        def counting(items: bytes):
            self.walks += 1
            return original(items)

        patcher = mock.patch.object(share_ledger_module, "_walk_canonical_items", counting)
        patcher.start()
        self.addCleanup(patcher.stop)

    def parsed_delta(self) -> int:
        return window_ownership_snapshot()["parsed_records"] - self.before["parsed_records"]

    def test_iteration_holds_the_parse_only_while_walking(self) -> None:
        sequence = self.sequence
        self.assertIsNone(sequence._parsed)
        iterator = iter(sequence)
        first = next(iterator)
        self.assertEqual(first["share_seq"], 1)
        # The walk owns the tuple: telemetry sees a full parsed window.
        self.assertEqual(self.parsed_delta(), 40)
        self.assertIsNotNone(sequence._parsed)
        self.assertEqual(sum(1 for _ in iterator), 39)
        # Exhausted: the generator frame released the holder.
        self.assertEqual(self.parsed_delta(), 0)
        self.assertIsNone(sequence._parsed)
        # An abandoned walk releases too, by reference count alone.
        iterator = iter(sequence)
        next(iterator)
        self.assertEqual(self.parsed_delta(), 40)
        del iterator
        self.assertEqual(self.parsed_delta(), 0)
        self.assertEqual(self.walks, 2)

    def test_indexing_slicing_and_reversal_release_immediately(self) -> None:
        sequence = self.sequence
        self.assertEqual(sequence[0]["share_seq"], 1)
        self.assertEqual(sequence[-1]["share_seq"], 40)
        self.assertEqual([r["share_seq"] for r in sequence[3:6]], [4, 5, 6])
        self.assertEqual([r["share_seq"] for r in reversed(sequence)][:2], [40, 39])
        self.assertEqual(self.parsed_delta(), 0)
        self.assertIsNone(sequence._parsed)
        # Each read parsed for itself; ``reversed`` walked once, never per index.
        self.assertEqual(self.walks, 4)

    def test_retained_scope_shares_one_parse_with_every_reader(self) -> None:
        sequence = self.sequence
        with sequence.retained() as records:
            self.assertEqual(len(records), 40)
            self.assertEqual(self.parsed_delta(), 40)
            self.assertIs(sequence[0], records[0])
            self.assertIs(next(iter(sequence)), records[0])
            self.assertIs(next(reversed(sequence)), records[-1])
            with sequence.retained() as nested:
                self.assertIs(nested, records)
            # The outer pin still holds the tuple.
            self.assertIs(sequence._parsed, records)
            self.assertEqual(self.parsed_delta(), 40)
        self.assertEqual(self.walks, 1)
        self.assertIsNone(sequence._parsed)
        self.assertEqual(self.parsed_delta(), 0)
        # The next reader parses afresh.
        list(sequence)
        self.assertEqual(self.walks, 2)

    def test_concurrent_readers_share_the_live_parse(self) -> None:
        sequence = self.sequence
        started, release = threading.Event(), threading.Event()
        seen: list[object] = []

        def reader() -> None:
            iterator = iter(sequence)
            seen.append(next(iterator))
            started.set()
            release.wait(10)
            seen.append(sum(1 for _ in iterator))

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            self.assertTrue(started.wait(10))
            # A second consumer joins the walk in progress: same tuple.
            self.assertIs(sequence[0], seen[0])
            self.assertEqual(self.walks, 1)
        finally:
            release.set()
            thread.join(10)
        self.assertEqual(seen[1], 39)
        self.assertEqual(self.parsed_delta(), 0)

    def test_delayed_release_callback_cannot_zero_a_newer_parse(self) -> None:
        # Codex review on #489: the old holder's release callback used to
        # check the slot outside the registry lock, so a callback delayed
        # behind that lock could erase the rows a newer parse noted first.
        sequence = self.sequence
        holder = sequence._records()
        stale_reference = sequence._parsed_ref
        self.assertEqual(self.parsed_delta(), 40)
        # A newer parse installed its own slot and noted its rows before the
        # stale callback ran: the guarded note must stand down.
        sequence._parsed_ref = weakref.ref(holder)
        ownership.note_parsed_window(sequence, 0, guard=lambda: sequence._parsed_ref is stale_reference)
        self.assertEqual(self.parsed_delta(), 40, "the newer slot's note must stand")
        # A callback whose holder is still the published one zeroes as before.
        current_reference = sequence._parsed_ref
        ownership.note_parsed_window(sequence, 0, guard=lambda: sequence._parsed_ref is current_reference)
        self.assertEqual(self.parsed_delta(), 0)
        ownership.note_parsed_window(sequence, 40)
        sequence._parsed_ref = stale_reference
        del holder
        self.assertEqual(self.parsed_delta(), 0)

    def test_release_race_with_concurrent_reparse_keeps_live_count(self) -> None:
        sequence = self.sequence
        rounds = 0
        for _ in range(50):
            holder = sequence._records()
            walked = threading.Event()
            published = threading.Event()

            def reparse() -> None:
                # Wait for the old holder to be dropped while the registry
                # lock is held: its callback is queued behind the lock.
                walked.wait(10)
                latest = sequence._records()
                published.set()
                walked.clear()
                walked.wait(10)
                del latest

            worker = threading.Thread(target=reparse)
            worker.start()
            with ownership._LOCK:
                del holder
                walked.set()
                time.sleep(0.001)
            published.wait(10)
            with sequence.retained():
                self.assertEqual(self.parsed_delta(), 40)
            walked.set()
            worker.join(10)
            rounds += 1
        self.assertEqual(rounds, 50)
        self.assertEqual(self.parsed_delta(), 0)

    def test_parse_failure_publishes_nothing(self) -> None:
        miscounted = DaemonShareJsonSequence(self.mirror.canonical_items, 41)
        with self.assertRaisesRegex(DaemonWindowMirrorDivergence, "parsed 40 records where 41"):
            miscounted[0]
        self.assertIsNone(miscounted._parsed)
        self.assertEqual(self.parsed_delta(), 0)
        with self.assertRaises(DaemonWindowMirrorDivergence):
            with miscounted.retained():
                pass
        self.assertIsNone(miscounted._pinned)
        self.assertEqual(miscounted._pins, 0)

    def test_sequence_outliving_its_mirror_pins_bytes_but_no_dicts(self) -> None:
        sequence = self.sequence
        buffer = sequence.canonical_items
        mirror_ref = weakref.ref(self.mirror)
        list(sequence)
        del self.mirror
        self.assertIsNone(mirror_ref())
        # The retained owner (a superseded artifact would hold it like
        # this) costs its canonical buffer and nothing the collector walks.
        current = window_ownership_snapshot()
        self.assertEqual(current["owners"] - self.before["owners"], -1)
        self.assertEqual(current["canonical_buffers"], self.before["canonical_buffers"])
        self.assertEqual(current["parsed_records"], self.before["parsed_records"])
        self.assertIs(sequence.canonical_items, buffer)
        # And it releases the buffer with itself, without a collection.
        sequence_ref = weakref.ref(sequence)
        del sequence, self.sequence
        self.assertIsNone(sequence_ref())
        current = window_ownership_snapshot()
        self.assertEqual(current["canonical_buffers"], self.before["canonical_buffers"] - 1)
        self.assertEqual(current["canonical_bytes"], self.before["canonical_bytes"] - len(buffer))


class RetiredConsumerReleaseTests(NoGCTestCase):
    """The consumers that force a parse release it when they complete."""

    def setUp(self) -> None:
        super().setUp()
        self.baseline = window_ownership_snapshot()
        self.mirror = mirror_of(30)
        # A failed assertion must not leave this case's owners alive into
        # the next case's baseline.
        self.addCleanup(self.__dict__.pop, "mirror", None)

    def assert_released(self, *references: weakref.ref) -> None:
        for reference in references:
            self.assertIsNone(reference())
        self.assertEqual(window_ownership_snapshot(), self.baseline)

    def test_durable_candidate_intent_stages_bytes_and_releases(self) -> None:
        sequence = self.mirror.json_records()
        fields = intent_for(0, shares_json=sequence)
        prepared = codec.prepare_candidate_intent(fields, chunk_bytes=4096)
        staged = bytearray()
        prepared.body.write_chunks(lambda chunk: staged.extend(chunk.data))
        self.assertIsNone(sequence._parsed, "the intent stages canonical bytes, never dicts")
        self.assertEqual(window_ownership_snapshot()["parsed_records"], self.baseline["parsed_records"])
        self.assertEqual(bytes(staged), oracle_bytes(intent_for(0, shares_json=list(sequence))))
        self.assertIsNone(sequence._parsed)
        # The mirror is replaced first, as in production; the intent is the
        # last legitimate consumer and takes the sequence with it.
        references = (weakref.ref(self.mirror), weakref.ref(sequence))
        del self.mirror
        self.assertIsNotNone(references[1]())
        del sequence, fields, prepared, staged
        self.assert_released(*references)

    def test_found_block_audit_walks_release_their_parse(self) -> None:
        sequence = self.mirror.json_records()
        parsed_during: list[int] = []
        identities, compact = _compact_share_payload(sequence)
        self.assertEqual(len(compact), 30)
        self.assertIsNone(sequence._parsed)
        chunks = _compact_share_tail_chunks(sequence)
        self.assertTrue(chunks)
        self.assertIsNone(sequence._parsed)
        # The serialization slot caches the encoded tail, never the rows.
        serialization = _ShareWindowSerialization(
            key=(self.mirror.share_snapshot_sha256, 30, 0), share_count=30,
            share_snapshot_sha256=self.mirror.share_snapshot_sha256,
        )

        def observe(shares):
            with shares.retained():
                parsed_during.append(window_ownership_snapshot()["parsed_records"] - self.baseline["parsed_records"])
            return _compact_share_tail_chunks(shares)

        with mock.patch("lab.prism.bundle_compiler._compact_share_tail_chunks", observe):
            self.assertEqual(serialization.compact_tail_chunks(sequence), chunks)
        self.assertEqual(parsed_during, [30])
        self.assertIsNone(sequence._parsed)
        self.assertEqual(window_ownership_snapshot()["parsed_records"], self.baseline["parsed_records"])
        references = (weakref.ref(self.mirror), weakref.ref(sequence))
        del self.mirror, sequence, identities, compact, chunks, serialization
        self.assert_released(*references)

    def test_one_shot_builder_input_splices_bytes_without_a_parse(self) -> None:
        sequence = self.mirror.json_records()
        payload = {"found_block": {"block_height": 1}, "shares": sequence}
        with mock.patch.object(DaemonShareJsonSequence, "_records", side_effect=AssertionError("one-shot input parsed the window")):
            encoded = b"".join(
                chunk if isinstance(chunk, bytes) else chunk.encode("utf-8")
                for chunk in _iter_build_input_chunks(payload, array_keys=("shares",))
            )
        expected = json.dumps({"found_block": {"block_height": 1}, "shares": list(sequence)}, separators=(",", ":")).encode()
        self.assertEqual(json.loads(encoded), json.loads(expected))
        self.assertIsNone(sequence._parsed)
        references = (weakref.ref(self.mirror), weakref.ref(sequence))
        del self.mirror, sequence, payload
        self.assert_released(*references)


class OwnershipAttributionTests(NoGCTestCase):
    """Owners are attributable by kind, creation site and age from metrics alone."""

    def owners_by_site(self) -> dict[tuple[str, str], int]:
        return window_ownership_breakdown()["owners"]

    def test_sequence_site_names_the_minting_function(self) -> None:
        mirror = mirror_of(3)

        def audit_consumer():
            return mirror.json_records()

        before = self.owners_by_site()
        sequence = audit_consumer()
        explicit = mirror.json_records(site="explicit-site")
        after = self.owners_by_site()
        self.assertEqual(after.get(("sequence", f"{MODULE}.audit_consumer"), 0) - before.get(("sequence", f"{MODULE}.audit_consumer"), 0), 1)
        self.assertEqual(after.get(("sequence", "explicit-site"), 0), 1)
        del sequence, explicit
        after = self.owners_by_site()
        self.assertNotIn(("sequence", f"{MODULE}.audit_consumer"), after)
        self.assertNotIn(("sequence", "explicit-site"), after)

    def test_mirror_site_skips_construction_passthroughs(self) -> None:
        before = self.owners_by_site()
        mirror = mirror_of(3)
        self.assertEqual(self.owners_by_site().get(("mirror", f"{MODULE}.mirror_of"), 0) - before.get(("mirror", f"{MODULE}.mirror_of"), 0), 1)
        advanced = mirror.advanced(
            anchor_job_issued_at_ms=10**9 + 1, record_count=3, retained_drop_bytes=0,
            appended_items=b"", share_snapshot_sha256=mirror.share_snapshot_sha256,
        )
        # ``advanced`` is a passthrough: the label names this test.
        self.assertEqual(self.owners_by_site().get(("mirror", f"{MODULE}.test_mirror_site_skips_construction_passthroughs"), 0), 1)
        del mirror, advanced
        self.assertNotIn(("mirror", f"{MODULE}.test_mirror_site_skips_construction_passthroughs"), self.owners_by_site())

    def test_parsed_records_follow_the_holder_by_site(self) -> None:
        mirror = mirror_of(5)
        sequence = mirror.json_records(site="parser")
        self.assertNotIn(("sequence", "parser"), window_ownership_breakdown()["parsed_records"])
        with sequence.retained():
            self.assertEqual(window_ownership_breakdown()["parsed_records"][("sequence", "parser")], 5)
        self.assertNotIn(("sequence", "parser"), window_ownership_breakdown()["parsed_records"])
        # Retiring a parsed owner takes its rows out of the site row even
        # while a consumer still holds the tuple.
        holder = sequence._records()
        self.assertEqual(window_ownership_breakdown()["parsed_records"][("sequence", "parser")], 5)
        del sequence
        self.assertNotIn(("sequence", "parser"), window_ownership_breakdown()["parsed_records"])
        self.assertEqual(len(holder.records), 5)
        del holder

    def test_oldest_owner_age_by_kind(self) -> None:
        clock = [100.0]
        with mock.patch.object(ownership, "_clock", lambda: clock[0]):
            first = mirror_of(2)
            oldest_sequence = first.json_records(site="age-test")
            clock[0] = 150.0
            newer = first.json_records(site="age-test")
            clock[0] = 160.0
            ages = window_ownership_breakdown()["oldest_owner_age_seconds"]
            self.assertEqual(ages["sequence"], 60.0)
            self.assertEqual(ages["mirror"], 60.0)
            # Retiring the oldest owner moves the gauge to the next one; it
            # does not keep growing for a kind whose owners all retire.
            del oldest_sequence
            self.assertEqual(window_ownership_breakdown()["oldest_owner_age_seconds"]["sequence"], 10.0)
            del newer
            self.assertNotIn("sequence", window_ownership_breakdown()["oldest_owner_age_seconds"])
            del first

    def test_site_cap_bounds_labels_ever_admitted_not_labels_live(self) -> None:
        class Owner:
            pass

        def track(index: int) -> Owner:
            owner = Owner()
            ownership.track_window(owner, b"x" * (index + 1), kind="probe", site=f"probe-site-{index}")
            return owner

        admitted = set(ownership._ADMITTED_SITES)
        try:
            with mock.patch.object(ownership, "MAX_TRACKED_WINDOW_SITES", len(admitted) + 2):
                owners = [track(index) for index in range(4)]
                by_site = self.owners_by_site()
                self.assertEqual(by_site[("probe", "probe-site-0")], 1)
                self.assertEqual(by_site[("probe", "probe-site-1")], 1)
                self.assertEqual(by_site[("probe", ownership.OTHER_WINDOW_SITE)], 2)
                # Retirement clears the live rows but not the admission: a
                # retired label's slot is never handed to an unseen label,
                # so the series population Prometheus keeps stays bounded.
                del owners[:]
                self.assertFalse([key for key in self.owners_by_site() if key[0] == "probe"])
                owners = [track(index) for index in range(4, 8)]
                by_site = self.owners_by_site()
                self.assertEqual([key for key in by_site if key[0] == "probe"], [("probe", ownership.OTHER_WINDOW_SITE)])
                self.assertEqual(by_site[("probe", ownership.OTHER_WINDOW_SITE)], 4)
                # An admitted label keeps its own row when it recurs.
                owners.append(track(0))
                self.assertEqual(self.owners_by_site()[("probe", "probe-site-0")], 1)
                del owners[:]
        finally:
            ownership._ADMITTED_SITES.difference_update(
                key for key in tuple(ownership._ADMITTED_SITES) if key[0] == "probe"
            )

    def test_breakdown_holds_no_owner_and_survives_retirement_reentry(self) -> None:
        class Owner:
            pass

        owner = Owner()
        ownership.track_window(owner, b"bytes", kind="probe", site="reentry")
        self.addCleanup(ownership._ADMITTED_SITES.discard, ("probe", "reentry"))
        reference = weakref.ref(owner)
        breakdown = window_ownership_breakdown()
        self.assertEqual(breakdown["owners"][("probe", "reentry")], 1)
        del owner
        self.assertIsNone(reference())
        self.assertNotIn(("probe", "reentry"), window_ownership_breakdown()["owners"])
        del breakdown


class OwnershipMetricsExportTests(unittest.TestCase):
    def test_lines_carry_kind_site_and_oldest_age(self) -> None:
        clock = [10.0]
        with mock.patch.object(ownership, "_clock", lambda: clock[0]):
            mirror = mirror_of(2)
            sequence = mirror.json_records(site='odd"site\\name')
            clock[0] = 12.5
            with sequence.retained():
                lines = MetricsRenderer(SimpleNamespace()).window_ownership_metrics_lines()
        del sequence, mirror
        self.assertIn('qbit_prism_window_ownership_owners_by_site{kind="sequence",site="odd\\"site\\\\name"} 1', lines)
        self.assertIn('qbit_prism_window_ownership_parsed_records_by_site{kind="sequence",site="odd\\"site\\\\name"} 2', lines)
        self.assertIn('qbit_prism_window_ownership_oldest_owner_age_seconds{kind="sequence"} 2.500000', lines)
        self.assertIn("# TYPE qbit_prism_window_ownership_owners_by_site gauge", lines)
        self.assertIn("# TYPE qbit_prism_window_ownership_oldest_owner_age_seconds gauge", lines)
        # The scalar family is unchanged ahead of the labelled one.
        self.assertLess(lines.index("# TYPE qbit_prism_window_ownership_owners gauge"),
                        lines.index("# TYPE qbit_prism_window_ownership_owners_by_site gauge"))


if __name__ == "__main__":
    unittest.main()

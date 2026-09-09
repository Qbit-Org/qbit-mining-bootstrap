from __future__ import annotations

import gc
import os
import shlex
import shutil
import unittest
import weakref

from lab.prism.candidate_window import (
    COPY_TEXT_MAX_BYTES,
    COPY_TEXT_MAX_RECORDS,
    copy_text_chunks,
    disk_window_covers,
    recorded_share_ids,
)


class CandidateWindowTests(unittest.TestCase):
    def test_membership_checks_omissions_not_equality_or_order(self) -> None:
        self.assertTrue(disk_window_covers(iter(["b", "a", "a", "extra"]), [
            {"share_id": "a"}, {"share_id": "b"},
        ]))
        self.assertFalse(disk_window_covers(iter(["a", "extra"]), [
            {"share_id": "a"}, {"share_id": "late"},
        ]))
        self.assertTrue(disk_window_covers(iter([]), []))
        self.assertFalse(disk_window_covers(iter([]), [{"share_id": "a"}]))

    def test_legacy_string_and_non_dictionary_semantics(self) -> None:
        recorded = [None, {"share_id": 42}, {}, {"share_id": "é"}]
        self.assertTrue(disk_window_covers(recorded_share_ids(recorded), [
            "ignored", {"share_id": "42"}, {"share_id": None}, {"share_id": "é"},
        ]))
        self.assertFalse(disk_window_covers(recorded_share_ids(recorded), [
            {"share_id": "e\u0301"},
        ]))

    def test_copy_escapes_commands_nulls_and_delimiters(self) -> None:
        chunks = list(copy_text_chunks(["\\.\nDROP TABLE x;", "\\N", "a\tb\rc", ""]))
        self.assertEqual(
            b"".join(chunks), b"\\\\.\\nDROP TABLE x;\n\\\\N\na\\tb\\rc\n\n",
        )

    def test_oversized_field_obeys_byte_bound_and_checks_deadline(self) -> None:
        value = "🌐\n\\\té" * 30000
        checks = 0

        def check() -> None:
            nonlocal checks
            checks += 1

        count = 0
        for chunk in copy_text_chunks([value], check=check):
            self.assertLessEqual(len(chunk), COPY_TEXT_MAX_BYTES)
            count += 1
        self.assertEqual(checks, count)
        self.assertGreater(checks, 30)

    def test_cancel_during_long_field_stops_consumption(self) -> None:
        calls = 0

        def cancel() -> None:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise InterruptedError("cancelled")

        with self.assertRaises(InterruptedError):
            for _chunk in copy_text_chunks(["x" * 50000], check=cancel):
                pass
        self.assertEqual(calls, 3)

    def test_tiny_records_still_have_a_work_bound(self) -> None:
        consumed = 0
        at_check = 0

        def values():
            nonlocal consumed
            for _ in range(1300):
                consumed += 1
                yield ""

        def check():
            nonlocal at_check
            self.assertLessEqual(consumed - at_check, COPY_TEXT_MAX_RECORDS)
            at_check = consumed

        for chunk in copy_text_chunks(values(), check=check):
            self.assertLessEqual(len(chunk), COPY_TEXT_MAX_BYTES)
        self.assertEqual(at_check, 1300)

    def test_record_graphs_release_without_cyclic_gc(self) -> None:
        class Row(dict):
            pass

        references = []

        def rows():
            for index in range(2000):
                row = Row(share_id=f"share-{index}", payload=bytearray(10000))
                references.append(weakref.ref(row))
                yield row

        enabled = gc.isenabled()
        gc.disable()
        try:
            self.assertTrue(disk_window_covers(recorded_share_ids(rows()), [
                {"share_id": "share-1999"},
            ]))
            self.assertTrue(all(reference() is None for reference in references))
        finally:
            if enabled:
                gc.enable()


@unittest.skipUnless(os.environ.get("PRISM_TEST_DATABASE_URL"), "requires disposable PostgreSQL")
class CandidateWindowPostgresTests(unittest.TestCase):
    """Run with an initialized disposable database, never a production DSN."""

    def setUp(self) -> None:
        import psycopg
        from lab.prism.share_ledger import PsqlShareLedger

        self.connection = psycopg.connect(os.environ["PRISM_TEST_DATABASE_URL"], autocommit=True)
        self.addCleanup(self.connection.close)
        self.connection.execute("TRUNCATE qbit_share_ledger CASCADE")
        self.ledger = PsqlShareLedger(
            database_url=os.environ["PRISM_TEST_DATABASE_URL"], psql_command="psql",
            native_client_mode="native", writer_id="candidate-window-tests",
        )
        self.addCleanup(self.ledger.close)
        self.addCleanup(self.ledger.release_writer_lease)

    def insert(self, share_id: str, *, accepted_at_ms: int = 1000) -> None:
        self.connection.execute("""
            INSERT INTO qbit_share_ledger (
                share_id, miner_id, payout_order_key, p2mr_program,
                share_difficulty, network_difficulty, template_height, job_id,
                job_issued_at, ntime, accepted_at, credit_policy, accepted,
                writer_id, writer_epoch
            ) VALUES (%s, 'miner', 'miner', decode(repeat('aa', 32), 'hex'),
                1, 1000000, 1, 'job', to_timestamp(0), 0,
                to_timestamp(%s::double precision / 1000.0), NULL, true, 'test', 1)
        """, (share_id, accepted_at_ms))

    def covers(self, rows) -> bool:
        return self.ledger.candidate_window_covers(
            rows, anchor_job_issued_at_ms=2000, network_difficulty=1000000,
        )

    def test_native_exact_subset_and_late_visible_append(self) -> None:
        self.insert("a")
        self.assertTrue(self.covers([{"share_id": "a"}, {"share_id": "extra"}]))
        self.insert("late")
        self.assertFalse(self.covers([{"share_id": "a"}]))
        self.assertTrue(self.covers([{"share_id": "late"}, {"share_id": "a"}]))

    def test_native_copy_special_values_and_no_temporary_table_leak(self) -> None:
        for share_id in ("\\.\nSELECT 1;", "\\N", "🌐\té\r\n", "extra"):
            self.insert(share_id)
        self.assertTrue(self.covers([
            {"share_id": value}
            for value in ("extra", "\\.\nSELECT 1;", "🌐\té\r\n", "\\N")
        ]))
        with self.ledger._native.connection() as connection:
            self.assertIsNone(connection.execute(
                "SELECT to_regclass('pg_temp.qbit_candidate_recorded_ids')"
            ).fetchone()[0])

    def test_native_failure_rolls_back_private_copy_and_can_retry(self) -> None:
        self.insert("a")

        def failing_rows():
            yield {"share_id": "a"}
            raise InterruptedError("stop")

        with self.assertRaises(InterruptedError):
            self.covers(failing_rows())
        self.assertTrue(self.covers([{"share_id": "a"}]))

    def test_native_expired_deadline_does_not_publish_partial_answer(self) -> None:
        from lab.prism.share_ledger import LedgerOperationTimeout

        self.insert("a")
        with self.ledger.operation_timeout(1):
            self.ledger._operation_timeout_local.deadline = self.ledger._monotonic() - 1
            with self.assertRaises(LedgerOperationTimeout):
                self.covers([{"share_id": "a"}])
        self.assertTrue(self.covers([{"share_id": "a"}]))

    @unittest.skipUnless(shutil.which("psql"), "requires PostgreSQL client")
    def test_psql_stream_roundtrip_and_failure(self) -> None:
        # Use the existing instance's guards/gates and an independent psql
        # connection. The writer lease is not read or updated by this call.
        native = self.ledger._native
        old_command = self.ledger._command
        self.ledger._native = None
        self.ledger._command = shlex.split(
            "psql " + shlex.quote(os.environ["PRISM_TEST_DATABASE_URL"])
        )
        try:
            value = "\\.\nSELECT 1;🌐\t\\N"
            self.insert(value)
            self.assertTrue(self.covers([{"share_id": value}, {"share_id": "extra"}]))
            self.assertFalse(self.covers([{"share_id": "extra"}]))
            self.assertEqual(self.connection.execute(
                "SELECT count(*) FROM qbit_share_ledger"
            ).fetchone()[0], 1)
        finally:
            self.ledger._native = native
            self.ledger._command = old_command


if __name__ == "__main__":
    unittest.main()

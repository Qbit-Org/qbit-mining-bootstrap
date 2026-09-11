"""Two-session PostgreSQL regressions for immutable candidate publication."""

from __future__ import annotations

import os
import threading
import time
import unittest
from unittest import mock
import uuid

from lab.prism.candidate_codec import prepare_candidate_intent
from lab.prism.candidate_store import retire_orphan_bodies_sql
from lab.prism.share_ledger import PsqlShareLedger
from tests.prism_postgres_candidate_gate import intent, pending_share_for


@unittest.skipUnless(os.environ.get("PRISM_TEST_DATABASE_URL"), "requires disposable PostgreSQL")
class CandidateStorageRaceTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from psycopg.conninfo import make_conninfo

        self.admin = psycopg.connect(os.environ["PRISM_TEST_DATABASE_URL"], autocommit=True)
        self.addCleanup(self.admin.close)
        schema = "candidate_race_" + uuid.uuid4().hex
        self.admin.execute(f'CREATE SCHEMA "{schema}"')
        self.addCleanup(lambda: self.admin.execute(f'DROP SCHEMA "{schema}" CASCADE'))
        url = make_conninfo(os.environ["PRISM_TEST_DATABASE_URL"], options=f"-csearch_path={schema}")
        self.ledger = PsqlShareLedger(
            database_url=url, psql_command="psql", native_client_mode="native",
            writer_id="candidate-race", initialize_schema=True,
        )
        self.addCleanup(self.ledger.close)
        self.addCleanup(self.ledger.release_writer_lease)
        self.first = psycopg.connect(url)
        self.second = psycopg.connect(url)
        self.addCleanup(self.first.close)
        self.addCleanup(self.second.close)
        self.prepared = prepare_candidate_intent(intent(uuid.uuid4().hex * 2, 4, credit=False))

    def stage_without_sealing(self):
        execute = self.ledger._run_json
        captured = []

        def capture(sql):
            if "observed_chunk_count" in sql:
                captured.append(sql)
                return {"sealed": 1}
            return execute(sql)

        with mock.patch.object(self.ledger, "_run_json", side_effect=capture):
            body_id = self.ledger.stage_candidate_body(self.prepared)
        self.assertEqual(len(captured), 1)
        return body_id, captured[0]

    def publication(self):
        body_id = self.ledger.stage_candidate_body(self.prepared)
        with mock.patch.object(self.ledger, "stage_candidate_body", return_value=body_id), \
                mock.patch.object(self.ledger, "_run_fenced_json", return_value={"inserted": 1}) as execute:
            self.ledger.persist_block_candidate_intent(self.prepared)
        return body_id, execute.call_args.args[0]

    def retire_sql(self):
        return retire_orphan_bodies_sql(
            {**self.ledger._writer_identity_payload(), "stale_staging_seconds": 0},
            jsonb=self.ledger._jsonb_literal,
        )

    def race(self, first_sql, second_sql):
        """Commit the first statement only after the second demonstrably waits."""
        initial = self.first.execute(first_sql).fetchone()
        result = []

        def run():
            try:
                cursor = self.second.execute(second_sql)
                result.append(cursor.fetchone() if cursor.description else None)
                self.second.commit()
            except Exception as exc:
                result.append(exc)
                self.second.rollback()

        thread = threading.Thread(target=run)
        thread.start()
        deadline = time.monotonic() + 10
        blocked = False
        try:
            while thread.is_alive() and time.monotonic() < deadline:
                row = self.admin.execute(
                    "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s",
                    (self.second.info.backend_pid,),
                ).fetchone()
                if row and row[0] == "Lock":
                    blocked = True
                    break
                time.sleep(0.01)
        finally:
            self.first.commit()
            thread.join(10)
            if thread.is_alive():
                self.second.cancel()
                thread.join(5)
        self.assertTrue(blocked, "second statement never reached the row-lock barrier")
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(result), 1)
        return initial, result[0]

    def extra_part_sql(self, body_id, kind):
        if kind == "chunk":
            return (
                "INSERT INTO qbit_block_candidate_body_chunk VALUES "
                f"('{body_id}', 999999, '\\x00', '{'0' * 64}') RETURNING ordinal"
            )
        return (
            "INSERT INTO qbit_block_candidate_body_page VALUES "
            f"('{body_id}', 'shares_json', 999999, 0, 0) RETURNING page_ordinal"
        )

    def test_seal_first_rejects_late_chunk(self):
        body_id, seal = self.stage_without_sealing()
        first, second = self.race(seal, self.extra_part_sql(body_id, "chunk"))
        self.assertEqual(first[0]["sealed"], 1)
        self.assertIsInstance(second, Exception)
        self.assertIn("accepts no parts", str(second))

    def test_seal_first_rejects_late_page(self):
        body_id, seal = self.stage_without_sealing()
        _, second = self.race(seal, self.extra_part_sql(body_id, "page"))
        self.assertIsInstance(second, Exception)
        self.assertIn("accepts no parts", str(second))

    def assert_part_first_prevents_seal(self, kind):
        body_id, seal = self.stage_without_sealing()
        _, second = self.race(self.extra_part_sql(body_id, kind), seal)
        if isinstance(second, Exception):
            self.assertTrue("incomplete" in str(second) or "could not serialize" in str(second), str(second))
        else:
            self.assertEqual(second[0]["sealed"], 0, "seal missed the part committed during its lock wait")
        state = self.first.execute("SELECT state FROM qbit_block_candidate_body WHERE body_id = %s", (body_id,)).fetchone()[0]
        self.assertEqual(state, "staging")

    def test_chunk_first_is_included_in_seal_validation(self):
        self.assert_part_first_prevents_seal("chunk")

    def test_page_first_is_included_in_seal_validation(self):
        self.assert_part_first_prevents_seal("page")

    def test_repeatable_read_sealer_cannot_miss_concurrent_upload(self):
        self.second.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        self.assert_part_first_prevents_seal("page")

    def test_publication_first_prevents_orphan_retirement(self):
        body_id, publish = self.publication()
        first, second = self.race(publish, self.retire_sql())
        self.assertEqual(first[0]["inserted"], 1)
        if isinstance(second, Exception):
            self.assertIn("referenced by a pending outbox row", str(second))
        else:
            self.assertEqual(second[0]["retired"], [])
        self.assertEqual(self.first.execute("SELECT state FROM qbit_block_candidate_body WHERE body_id = %s", (body_id,)).fetchone()[0], "sealed")

    def test_retirement_first_prevents_publication(self):
        body_id, publish = self.publication()
        first, second = self.race(self.retire_sql(), publish)
        self.assertEqual(first[0]["retired"], [body_id])
        self.assertEqual(second[0]["error"], "block candidate body is not sealed")
        self.assertEqual(self.first.execute("SELECT count(*) FROM qbit_block_candidate_outbox WHERE body_id = %s", (body_id,)).fetchone()[0], 0)

    def test_repeatable_read_retirement_cannot_miss_publication(self):
        body_id, publish = self.publication()
        self.second.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        _, second = self.race(publish, self.retire_sql())
        self.assertIsInstance(second, Exception)
        self.assertIn("could not serialize", str(second))
        self.assertEqual(self.first.execute("SELECT state FROM qbit_block_candidate_body WHERE body_id = %s", (body_id,)).fetchone()[0], "sealed")

    def test_database_publication_guard_invalidates_older_retirement_snapshot(self):
        body_id = self.ledger.stage_candidate_body(self.prepared)
        publish = (
            "INSERT INTO qbit_block_candidate_outbox "
            "(block_hash, candidate_sha256, storage_version, body_id) VALUES "
            f"('{self.prepared.block_hash}', '{self.prepared.candidate_sha256}', 2, '{body_id}') "
            "RETURNING body_id"
        )
        self.second.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        _, second = self.race(publish, self.retire_sql())
        self.assertIsInstance(second, Exception)
        self.assertIn("could not serialize", str(second))
        self.assertEqual(self.first.execute("SELECT state FROM qbit_block_candidate_body WHERE body_id = %s", (body_id,)).fetchone()[0], "sealed")

    def test_content_comparator_rejects_forced_digest_collision(self):
        body_id = self.ledger.stage_candidate_body(self.prepared)
        fields = dict(self.prepared)
        fields["username"] = "distinct"
        different = prepare_candidate_intent(fields)
        # The append caller has already accepted a matching digest. Exercise
        # its independent content check with deliberately different bytes.
        self.assertFalse(self.ledger.compare_candidate_body(body_id, different))

    def test_content_comparator_preserves_jsonb_numeric_equality(self):
        body_id = self.ledger.stage_candidate_body(self.prepared)
        fields = dict(self.prepared)
        fields["found_block"] = {"network_difficulty": 1000.0}
        equivalent = prepare_candidate_intent(fields)
        self.assertNotEqual(equivalent.candidate_sha256, self.prepared.candidate_sha256)
        self.assertTrue(self.ledger.compare_candidate_body(body_id, equivalent))

    def test_takeover_after_comparison_cannot_credit_share(self):
        self.ledger.persist_block_candidate_intent(self.prepared)
        compare = self.ledger.compare_candidate_body

        def compare_then_takeover(*args):
            result = compare(*args)
            self.first.execute(
                "UPDATE qbit_ledger_writer_lease SET writer_session_token = %s WHERE singleton",
                (uuid.uuid4().hex,),
            )
            self.first.commit()
            return result

        with mock.patch.object(self.ledger, "compare_candidate_body", side_effect=compare_then_takeover):
            with self.assertRaisesRegex(RuntimeError, "writer lease is not active"):
                self.ledger.append_batch([(pending_share_for(self.prepared), self.prepared)])
        self.assertEqual(self.first.execute("SELECT count(*) FROM qbit_share_ledger").fetchone()[0], 0)
        self.assertEqual(self.first.execute("SELECT state, share_id FROM qbit_block_candidate_outbox").fetchone(), ("pending", None))

    def test_body_retired_before_publication_is_restaged_without_duplicate_credit(self):
        publish = self.ledger._publish_candidate_batch
        retired = []

        def retire_once(payloads, count):
            if not retired:
                body_id = payloads[0]["candidate"]["body_id"]
                self.first.execute(
                    "UPDATE qbit_block_candidate_body SET state = 'retired', retired_at = clock_timestamp() WHERE body_id = %s",
                    (body_id,),
                )
                self.first.commit()
                retired.append(body_id)
            return publish(payloads, count)

        with mock.patch.object(self.ledger, "_publish_candidate_batch", side_effect=retire_once):
            self.ledger.append_batch([(pending_share_for(self.prepared), self.prepared)])
        self.assertEqual(self.first.execute("SELECT count(*) FROM qbit_share_ledger").fetchone()[0], 1)
        row = self.first.execute("SELECT state, body_id FROM qbit_block_candidate_outbox").fetchone()
        self.assertEqual(row[0], "pending")
        self.assertNotEqual(row[1], retired[0])


if __name__ == "__main__":
    unittest.main()

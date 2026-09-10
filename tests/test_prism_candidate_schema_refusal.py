"""Every storage version refuses a database without the 002 migration (#255)."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from lab.prism import recover_pending_blocks as recovery
from lab.prism.candidate_store import IncompatibleCandidateSchema
from lab.prism.share_ledger import PsqlShareLedger

# (has_body_table, declared capability): nothing migrated, and a partial
# migration whose capability row (declared last) never landed.
UNMIGRATED = ((False, None), (True, None))


def ledger(storage_version, has_body_table, declared):
    value = PsqlShareLedger.__new__(PsqlShareLedger)
    value._candidate_storage_version = storage_version

    def run_json(sql):
        if "to_regclass" in sql:
            return {"has_body_table": has_body_table, "has_capabilities": declared is not None}
        return {"declared": declared}

    value._run_json = run_json
    return value


def reader(has_body_table, declared):
    def execute(sql, _params=None):
        if "to_regclass" in sql:
            row = {"has_body_table": has_body_table, "has_capabilities": declared is not None}
        else:
            row = {"capability_value": declared}
        return SimpleNamespace(fetchone=lambda: row)

    value = recovery.RecoveryReader.__new__(recovery.RecoveryReader)
    value.connection = SimpleNamespace(execute=execute)
    return value


class LedgerSchemaRefusalTests(unittest.TestCase):
    def test_unmigrated_schema_is_refused_for_every_storage_version(self):
        for storage_version in (1, 2):
            for has_body_table, declared in UNMIGRATED:
                with self.subTest(storage_version=storage_version, has_body_table=has_body_table), \
                        self.assertRaisesRegex(IncompatibleCandidateSchema, "002_candidate_bodies.sql"):
                    ledger(storage_version, has_body_table, declared).verify_candidate_schema()

    def test_migrated_schema_is_accepted_and_a_newer_one_refused(self):
        for storage_version in (1, 2):
            with self.subTest(storage_version=storage_version):
                self.assertEqual(
                    ledger(storage_version, True, 2).verify_candidate_schema(),
                    {"declared": 2, "has_body_table": True},
                )
                with self.assertRaisesRegex(IncompatibleCandidateSchema, "rollback floor"):
                    ledger(storage_version, True, 3).verify_candidate_schema()


class RecoverySchemaRefusalTests(unittest.TestCase):
    def test_reader_applies_the_ledger_rule(self):
        for has_body_table, declared in UNMIGRATED:
            with self.subTest(has_body_table=has_body_table), \
                    self.assertRaisesRegex(recovery.RecoveryError, "002_candidate_bodies.sql"):
                reader(has_body_table, declared).require_candidate_schema()
        with self.assertRaisesRegex(recovery.RecoveryError, "rollback floor"):
            reader(True, 3).require_candidate_schema()
        reader(True, 2).require_candidate_schema()

    def test_plan_refuses_an_unmigrated_database_before_reading_candidates(self):
        legacy = reader(False, None)
        legacy.metadata = Mock()
        rpc = SimpleNamespace(call=Mock())
        with self.assertRaisesRegex(recovery.RecoveryError, "002_candidate_bodies.sql"):
            recovery.plan_recovery(legacy, rpc, ["ab" * 32])
        legacy.metadata.assert_not_called()
        rpc.call.assert_not_called()


if __name__ == "__main__":
    unittest.main()

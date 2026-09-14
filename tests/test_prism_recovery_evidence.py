"""Fail closed on incomplete or inconsistent accounting reconciliation exports."""
import importlib.util
import json
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prism-recovery-evidence.py"
spec = importlib.util.spec_from_file_location("recovery_evidence", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def record(kind, row):
    return json.dumps({"kind": kind, "row": row}) + "\n"


def closing(**overrides):
    report = {"mismatch_count": 0, "current_drift_count": 0, "checked_active_rows": 0}
    report.update(overrides)
    return [record("integrity", report), record("complete", True)]


class RecoveryEvidenceTests(unittest.TestCase):
    def test_empty_history_has_legacy_zero_head(self):
        report = module.summarize(iter(closing()))
        self.assertEqual(report["audit_head_sha256"], "00" * 32)
        self.assertEqual(report["accepted_shares"], 0)

    def test_legacy_head_keeps_ascii_escaping_for_unicode_recipients(self):
        rows = [record("active_carry", {"recipient_id": "Zoë🔑", "prior_balance_sats": "123"})]
        report = module.summarize(iter(rows + closing(checked_active_rows=1)))
        # Pinned from the legacy escaped bytes, including the UTF-16 surrogate
        # pair for the key emoji; emitting raw UTF-8 produces a different head.
        self.assertEqual(report["audit_head_sha256"],
                         "a93f3b26140a6a5e39f4b67f5925b56767196b50aa9823eb4da178e5534c6925")

    def test_interrupted_exports_and_trailing_records_fail(self):
        for lines in ([], closing()[:-1], [record("complete", True)], closing() * 2):
            with self.subTest(lines=lines), self.assertRaises(ValueError):
                module.summarize(iter(lines))

    def test_integrity_mismatch_and_missing_counts_fail(self):
        for report in (
            {"mismatch_count": 1}, {"current_drift_count": 1},
            {"checked_active_rows": 1}, {"mismatch_count": None},
        ):
            with self.subTest(report=report), self.assertRaises(ValueError):
                module.summarize(iter(closing(**report)))

    def test_share_order_is_authoritative_and_gaps_are_valid(self):
        shares = [record("shares", {"share_seq": seq, "accepted": True}) for seq in (1, 5, 8)]
        report = module.summarize(iter(shares + closing()))
        self.assertEqual(report["accepted_shares"], 3)
        self.assertEqual(report["last_share_seq"], 8)
        for invalid in (list(reversed(shares)), shares + shares[-1:]):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                module.summarize(iter(invalid + closing()))
        changed = shares[:2] + [record("shares", {"share_seq": 8, "accepted": False})]
        other = module.summarize(iter(changed + closing()))
        self.assertEqual(other["accepted_shares"], 2)
        self.assertNotEqual(report["records"]["shares"]["sha256"], other["records"]["shares"]["sha256"])

    def test_unknown_records_fail_instead_of_skipping_accounting(self):
        with self.assertRaises(ValueError):
            module.summarize(iter([record("future_format", {})] + closing()))

    def test_recovery_obligations_change_summary_without_share_or_ctv_state_changes(self):
        baseline = module.summarize(iter(closing()))
        for kind, row, change in (
            ("ctv_checkpoints", {"fanout_txid": "ab", "confirmed_depth": 999},
             {"confirmed_depth": 1000}),
            ("cpfp_packages", {"fanout_txid": "ab", "signed_child_hex": None},
             {"signed_child_hex": "deadbeef"}),
            ("cpfp_retired_funding", {"funding_txid": "cd", "wallet_lock_released": False},
             {"wallet_lock_released": True}),
            ("ctv_broadcast_attempts", {"attempt_seq": 1, "submit_result": None},
             {"submit_result": {"accepted": True}}),
            ("deferred_shares", {"block_hash": "ef", "share": {"miner_id": "alice"}},
             {"share": {"miner_id": "bob"}}),
        ):
            with self.subTest(kind=kind):
                self.assertEqual(baseline["records"][kind]["count"], 0)
                added = module.summarize(iter([record(kind, row)] + closing()))
                changed = module.summarize(iter([record(kind, row | change)] + closing()))
                self.assertEqual(added["records"][kind]["count"], 1)
                self.assertEqual(changed["records"][kind]["count"], 1)
                self.assertNotEqual(baseline["records"][kind]["sha256"],
                                    added["records"][kind]["sha256"])
                self.assertNotEqual(added["records"][kind]["sha256"],
                                    changed["records"][kind]["sha256"])
                for other in baseline["records"]:
                    if other != kind:
                        self.assertEqual(baseline["records"][other], changed["records"][other])

    def test_fatal_state_and_recovery_history_change_summary_independently(self):
        baseline = module.summarize(iter(closing()))
        fatal = {"fatal_error": "fanout disconnected", "fatal_error_set_at": None}
        event = {
            "event_id": 1, "cleared_at": "2026-09-14T21:00:00+00:00",
            "operator_identity": "operator", "database_role": "prism",
            "reason": "reconciled", **fatal,
            "instances": [], "reconciliation": {"blocks_checked": 1},
        }
        for kind, row in (("fatal_state", fatal), ("fatal_state_events", event)):
            with self.subTest(kind=kind):
                self.assertEqual(baseline["records"][kind]["count"], 0)
                added = module.summarize(iter([record(kind, row)] + closing()))
                self.assertEqual(added["records"][kind]["count"], 1)
                self.assertNotEqual(added, baseline)
                for field in row:
                    with self.subTest(field=field):
                        changed = module.summarize(iter([
                            record(kind, row | {field: "changed"})] + closing()))
                        self.assertNotEqual(added["records"][kind]["sha256"],
                                            changed["records"][kind]["sha256"])
                for other in baseline["records"]:
                    if other != kind:
                        self.assertEqual(baseline["records"][other], added["records"][other])


if __name__ == "__main__":
    unittest.main()

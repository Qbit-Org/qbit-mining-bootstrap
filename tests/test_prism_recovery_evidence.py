"""Fail closed on incomplete or inconsistent accounting reconciliation exports."""
import hashlib
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


def carry(seq, height, label, program, *, gross, prior, onchain=0):
    """An active carry row as the export writes it, consistent in itself."""
    candidate = prior + gross
    return {
        "carry_forward_seq": seq, "block_hash": f"b{height}", "block_height": height,
        "recipient_id": label, "order_key": label, "p2mr_program_hex": program,
        "gross_amount_sats": gross, "prior_balance_sats": str(prior),
        "candidate_balance_sats": str(candidate), "onchain_amount_sats": onchain,
        "settlement_fee_sats": 0, "carry_forward_balance_sats": str(candidate - onchain),
        "action": "onchain" if onchain else "accrued", "maturity_state": "mature",
    }


class RecoveryEvidenceTests(unittest.TestCase):
    def test_empty_history_has_legacy_zero_head(self):
        report = module.summarize(iter(closing()))
        self.assertEqual(report["audit_head_sha256"], "00" * 32)
        self.assertEqual(report["accepted_shares"], 0)

    def test_legacy_head_keeps_ascii_escaping_for_unicode_recipients(self):
        row = carry(1, 1, "Zoë🔑", "aa", gross=123, prior=0)
        report = module.summarize(iter([record("active_carry", row)] + closing(checked_active_rows=1)))
        # The legacy escaped bytes, with the UTF-16 surrogate pair for the key
        # emoji; emitting raw UTF-8 produces a different head.
        legacy = (b'{"action":"accrued","block_hash":"b1","block_height":1,'
                  b'"candidate_balance_sats":"123","carry_forward_balance_sats":"123",'
                  b'"carry_forward_seq":1,"gross_amount_sats":123,"maturity_state":"mature",'
                  b'"onchain_amount_sats":0,"order_key":"Zo\\u00eb\\ud83d\\udd11",'
                  b'"p2mr_program_hex":"aa","prior_balance_sats":"0",'
                  b'"recipient_id":"Zo\\u00eb\\ud83d\\udd11","settlement_fee_sats":0}')
        self.assertEqual(report["audit_head_sha256"],
                         hashlib.sha256(bytes(32) + legacy).hexdigest())
        raw = legacy.replace(b"Zo\\u00eb\\ud83d\\udd11", "Zoë🔑".encode())
        self.assertNotEqual(report["audit_head_sha256"],
                            hashlib.sha256(bytes(32) + raw).hexdigest())

    def test_legacy_chain_runs_per_payout_program_across_case_labels(self):
        # Union's shape (#708): one program, an uppercase label carrying the
        # balance a lowercase label accrued, interleaved by height, one row
        # per program and block.
        upper, lower = "QB1ZABC", "qb1zabc"
        rows = [
            carry(1, 10, lower, "aa", gross=100, prior=0, onchain=0),
            carry(2, 11, upper, "aa", gross=50, prior=100, onchain=120),
            carry(3, 12, lower, "aa", gross=7, prior=30, onchain=0),
            carry(4, 13, upper, "aa", gross=0, prior=37, onchain=0),
            carry(5, 13, "other", "bb", gross=9, prior=0, onchain=0),
        ]
        lines = [record("active_carry", row) for row in rows]
        # A 2.x validator partitions by label, so it lists the three rows
        # whose label partition sum differs; 025's program rule lists none.
        label_rule = [{"carry_forward_seq": seq, "block_hash": f"b{height}"}
                      for seq, height in ((2, 11), (3, 12), (4, 13))]
        notes = []
        source = module.summarize(iter(lines + closing(
            checked_active_rows=5, mismatch_count=3, mismatches=label_rule)), notes)
        migrated = module.summarize(iter(lines + closing(checked_active_rows=5)))
        self.assertEqual(source, migrated)
        self.assertEqual(source["carry_forward_integrity"]["mismatch_count"], 0)
        self.assertEqual(source["carry_forward_integrity"]["mismatches"], [])
        self.assertIn("#708", notes[0])
        # A legacy finding the per-label rule does not make there is not
        # cleared: the first row starts both chains at zero.
        for unexplained in ({"carry_forward_seq": 1, "block_hash": "b10"},
                            {"carry_forward_seq": 5, "block_hash": "b13"}):
            with self.subTest(finding=unexplained), self.assertRaisesRegex(ValueError, "no chain rule"):
                module.summarize(iter(lines + closing(
                    checked_active_rows=5, mismatch_count=4, mismatches=label_rule + [unexplained])))

    def test_a_clean_report_is_summarized_as_the_database_wrote_it(self):
        report = {"schema": "qbit.prism.carry-forward-integrity.v1", "checked_active_rows": 1,
                  "mismatch_count": 0, "current_drift_count": 0, "current_drift": [], "mismatches": []}
        row = record("active_carry", carry(1, 10, "alice", "aa", gross=1, prior=0))
        summary = module.summarize(iter([row, record("integrity", report), record("complete", True)]))
        self.assertEqual(summary["carry_forward_integrity"], report)

    def test_malformed_reports_and_amounts_fail_closed(self):
        row = carry(1, 10, "alice", "aa", gross=1, prior=0)
        for lines in (
            [record("integrity", [0]), record("complete", True)],
            [record("active_carry", row)] + closing(checked_active_rows=1, mismatch_count=1, mismatches=[1]),
            [record("active_carry", row | {"prior_balance_sats": " 0"})] + closing(checked_active_rows=1),
            [record("active_carry", row | {"carry_forward_balance_sats": "0_001"})]
            + closing(checked_active_rows=1),
        ):
            with self.subTest(lines=lines), self.assertRaises(ValueError):
                module.summarize(iter(lines))

    def test_a_real_legacy_chain_break_fails_in_any_label(self):
        for field, value in (("prior_balance_sats", "99"), ("candidate_balance_sats", "8"),
                             ("carry_forward_balance_sats", "8")):
            rows = [carry(1, 10, "qb1zabc", "aa", gross=100, prior=0),
                    carry(2, 11, "QB1ZABC", "aa", gross=0, prior=100) | {field: value}]
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "payout program"):
                module.summarize(iter([record("active_carry", row) for row in rows]
                                      + closing(checked_active_rows=2)))

    def test_as_issued_findings_still_fail(self):
        marked = record("blocks", {"block_hash": "b11", "as_issued_audit_sha256": "ab" * 32})
        row = record("active_carry", carry(1, 11, "alice", "aa", gross=5, prior=0))
        for finding in ({"carry_forward_seq": 1, "block_hash": "b11"},
                        {"carry_forward_seq": None, "block_hash": "b11"},
                        {"carry_forward_seq": None, "block_hash": "unmarked"}):
            with self.subTest(finding=finding), self.assertRaisesRegex(ValueError, "as-issued"):
                module.summarize(iter([marked, row] + closing(
                    checked_active_rows=1, mismatch_count=1, mismatches=[finding])))

    def test_marked_rows_answer_to_their_manifest_not_the_running_chain(self):
        # A divergent landing (#478) carries its manifest's prior, whatever
        # the canonical balance was: the database's manifest rule judges it.
        marked = record("blocks", {"block_hash": "b11", "as_issued_audit_sha256": "ab" * 32})
        rows = [carry(1, 10, "alice", "aa", gross=100, prior=0),
                carry(2, 11, "alice", "aa", gross=10, prior=40)]
        lines = [marked] + [record("active_carry", row) for row in rows]
        report = module.summarize(iter(lines + closing(checked_active_rows=2)))
        self.assertEqual(report["records"]["active_carry"]["count"], 2)

    def test_an_incomplete_mismatch_list_and_disordered_records_fail(self):
        row = record("active_carry", carry(1, 10, "alice", "aa", gross=1, prior=0))
        later = record("active_carry", carry(2, 11, "alice", "aa", gross=1, prior=1))
        for lines in (
            [row] + closing(checked_active_rows=1, mismatch_count=2,
                            mismatches=[{"carry_forward_seq": 1, "block_hash": "b10"}]),
            [later, row] + closing(checked_active_rows=2),
            [row, record("blocks", {"block_hash": "b9"})] + closing(checked_active_rows=1),
        ):
            with self.subTest(lines=lines), self.assertRaises(ValueError):
                module.summarize(iter(lines))

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

    def test_offered_or_unknown_candidates_cannot_look_drained(self):
        for state in ("offer_reserved", "offered", "reconciliation", "future_state"):
            with self.subTest(state=state):
                report = module.summarize(iter([record("candidates", {"state": state})] + closing()))
                self.assertEqual(report["pending_candidates"], 0)
                self.assertEqual(report["unfinished_candidates"], 1)
        terminal = [record("candidates", {"state": state}) for state in ("submitted", "abandoned")]
        self.assertEqual(module.summarize(iter(terminal + closing()))["unfinished_candidates"], 0)

    def test_recovery_obligations_change_summary_without_share_or_ctv_state_changes(self):
        baseline = module.summarize(iter(closing()))
        for kind, row, change in (
            ("share_sequence", {"last_value": 40, "is_called": True},
             {"last_value": 2}),
            ("share_sequence", {"last_value": 40, "is_called": True},
             {"is_called": False}),
            ("sequences", {"sequence": "qbit_pool_payout_entries_payout_entry_seq_seq",
                           "last_value": 40, "is_called": True},
             {"last_value": 2}),
            ("sequences", {"sequence": "qbit_audit_publication_sequence_seq",
                           "last_value": 40, "is_called": True},
             {"is_called": False}),
            ("share_hashes", {"header_hash": "ab", "share_id": "first"},
             {"share_id": "later"}),
            ("candidate_balances", {"prior_balances_digest": "ab", "balances_sha256": "cd"},
             {"balances_sha256": "ef"}),
            ("audit_bodies", {"block_hash": "ab", "audit_bundle": {"schema": "native"}},
             {"audit_bundle": {"schema": "corrupted"}}),
            ("audit_snapshots", {"snapshot_sha256": "cd", "share_count": 3},
             {"share_count": 2}),
            ("ctv_checkpoints", {"fanout_txid": "ab", "confirmed_depth": 999},
             {"confirmed_depth": 1000}),
            ("ctv_retry_progress", {"fanout_txid": "ab", "broadcast_attempt_count": 40},
             {"next_broadcast_attempt_at": "2026-09-14T21:00:00"}),
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
        checkpoint = {"best_chainwork": "2", "best_tip_hash": "aa" * 32, "best_tip_height": 7}
        config = {"config_fingerprint": "fingerprint-one"}
        revision = {"payout_revision": 1}
        clock = {"ledger_clock_ms": 1758000000000}
        event = {
            "event_id": 1, "cleared_at": "2026-09-14T21:00:00+00:00",
            "operator_identity": "operator", "database_role": "prism",
            "reason": "reconciled", **fatal,
            "instances": [], "reconciliation": {"blocks_checked": 1},
        }
        for kind, row in (("fatal_state", fatal), ("fatal_state_events", event),
                          ("chain_checkpoint", checkpoint), ("cluster_config", config),
                          ("payout_revision", revision), ("ledger_clock", clock)):
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

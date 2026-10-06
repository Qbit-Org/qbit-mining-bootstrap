"""The parallel summary (--jobs N) is the serial summary (--jobs 1), byte for byte.

Each case writes an export and runs the summarizer serially and in parallel,
with chunk sizes small enough to put a range boundary at every line, and
compares the exit code, stdout and stderr exactly: the summary, its notes and
every refusal.
"""
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prism-recovery-evidence.py"
spec = importlib.util.spec_from_file_location("recovery_evidence_jobs", SCRIPT)
module = importlib.util.module_from_spec(spec)
# Registered, so the forked workers find `_parse_range` by name, as they do
# for the script run as __main__.
sys.modules[spec.name] = module
spec.loader.exec_module(module)

# (jobs, chunk bytes): a boundary after every line, after a few lines, and
# one range for the whole file.
PARALLEL = ((2, 1), (3, 37), (4, 300), (3, 1 << 20))


def record(kind, row, *, ascii_only=True):
    # psql writes jsonb as raw UTF-8; ascii_only=False keeps multibyte
    # characters in the file, so ranges must split between them correctly.
    return json.dumps({"kind": kind, "row": row}, ensure_ascii=ascii_only) + "\n"


def carry(seq, height, label, program, *, gross, prior, onchain=0):
    candidate = prior + gross
    return {
        "carry_forward_seq": seq, "block_hash": f"b{height}", "block_height": height,
        "recipient_id": label, "order_key": label, "p2mr_program_hex": program,
        "gross_amount_sats": gross, "prior_balance_sats": str(prior),
        "candidate_balance_sats": str(candidate), "onchain_amount_sats": onchain,
        "settlement_fee_sats": 0, "carry_forward_balance_sats": str(candidate - onchain),
        "action": "onchain" if onchain else "accrued", "maturity_state": "mature",
    }


def closing(**overrides):
    report = {"schema": "qbit.prism.carry-forward-integrity.v1", "mismatch_count": 0,
              "current_drift_count": 0, "checked_active_rows": 0, "current_drift": [], "mismatches": []}
    report.update(overrides)
    return [record("integrity", report), record("complete", True)]


def full_export():
    """Every kind the export writes, in its order, with the #708 shape so a
    note is printed, and non-ASCII text in several kinds."""
    lines = [record("shares", {"share_seq": seq, "share_id": f"s{seq}", "accepted": seq % 3 != 0,
                               "miner_id": "Zoë🔑" if seq % 5 == 0 else f"m{seq}"},
                    ascii_only=seq % 2 == 0)
             for seq in (1, 2, 4, 7, 8, 9, 15, 16, 23, 42, 43, 44, 100)]
    lines.append(record("shares", {"share_seq": 101, "accepted": 1}))
    lines.append(record("share_sequence", {"last_value": 101, "is_called": True}))
    lines += [record("sequences", {"sequence": name, "last_value": 7, "is_called": True})
              for name in ("qbit_pool_payout_entries_payout_entry_seq_seq", "qbit_audit_publication_sequence_seq")]
    lines += [record("share_hashes", {"header_hash": f"{index:064x}", "share_id": f"s{index}"})
              for index in range(40)]
    lines.append(record("blocks", {"block_hash": "b10", "block_height": 10}))
    lines.append(record("blocks", {"block_hash": "b11", "block_height": 11, "as_issued_audit_sha256": "ab" * 32}))
    lines.append(record("audits", {"block_hash": "b10", "audit_bundle_sha256": "cd" * 32}))
    lines.append(record("audit_bodies", {"block_hash": "b10", "audit_bundle": {
        "schema": "native", "shares": [{"miner": "Ünïcødé", "weight": index} for index in range(30)]}},
        ascii_only=False))
    lines.append(record("audit_snapshots", {"snapshot_sha256": "ef" * 32, "share_count": 3}))
    lines.append(record("carry", {"carry_forward_seq": 1, "label": "qb1zabc"}))
    lines.append(record("payouts", {"payout_entry_seq": 1, "amount_sats": "12"}))
    lines += [record("candidates", {"block_hash": f"c{index}", "state": state})
              for index, state in enumerate(("submitted", "pending", "abandoned", "orphaned", "reconciliation"))]
    lines.append(record("candidate_balances", {"prior_balances_digest": "ab", "balances_sha256": "cd"}))
    for kind in ("ctv_sets", "ctv_artifacts", "ctv_checkpoints", "ctv_retry_progress", "ctv_broadcast_attempts",
                 "cpfp_packages", "cpfp_retired_funding", "deferred_shares", "fatal_state", "fatal_state_events",
                 "policy_transitions", "chain_checkpoint", "cluster_config", "payout_revision", "ledger_clock"):
        lines.append(record(kind, {"kind_field": kind, "note": "žluťoučký"}, ascii_only=False))
    upper, lower = "QB1ZABC", "qb1zabc"
    rows = [
        carry(1, 10, lower, "aa", gross=100, prior=0),
        carry(2, 11, upper, "aa", gross=50, prior=100, onchain=120),
        carry(3, 12, lower, "aa", gross=7, prior=30),
        carry(4, 13, upper, "aa", gross=0, prior=37),
        carry(5, 13, "Zoë🔑", "bb", gross=9, prior=0),
    ]
    lines += [record("active_carry", row, ascii_only=index % 2 == 0) for index, row in enumerate(rows)]
    label_rule = [{"carry_forward_seq": 3, "block_hash": "b12"}, {"carry_forward_seq": 4, "block_hash": "b13"}]
    return lines + closing(checked_active_rows=5, mismatch_count=2, mismatches=label_rule)


class ParallelSummaryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.count = 0

    def tearDown(self):
        self.directory.cleanup()

    def write(self, content):
        self.count += 1
        path = self.root / f"export-{self.count}.jsonl"
        path.write_bytes(content if isinstance(content, bytes) else "".join(content).encode("utf-8"))
        return path

    def run_summary(self, path, *args):
        result = subprocess.run([sys.executable, str(SCRIPT), *args, str(path)], capture_output=True, timeout=120)
        return result.returncode, result.stdout, result.stderr

    def assert_same(self, content):
        """Summarize `content` serially and in parallel; return the serial result."""
        path = self.write(content)
        serial = self.run_summary(path, "--jobs", "1")
        for jobs, chunk in PARALLEL:
            with self.subTest(jobs=jobs, chunk=chunk):
                self.assertEqual(self.run_summary(path, "--jobs", str(jobs), "--chunk-bytes", str(chunk)), serial)
        return serial

    def test_a_complete_export_summarizes_identically_with_its_note(self):
        code, stdout, stderr = self.assert_same(full_export())
        self.assertEqual(code, 0, stderr)
        report = json.loads(stdout)
        self.assertEqual(report["records"]["shares"]["count"], 14)
        self.assertEqual(report["last_share_seq"], 101)
        self.assertEqual(report["records"]["share_hashes"]["count"], 40)
        self.assertEqual(report["records"]["active_carry"]["count"], 5)
        self.assertEqual(report["pending_candidates"], 1)
        self.assertEqual(report["unfinished_candidates"], 2)
        self.assertIn(b"note: the database's per-label legacy carry rule reported 2 finding(s)", stderr)

    def test_line_endings_and_a_missing_final_newline_are_read_as_text_mode_reads_them(self):
        lines = full_export()
        text = "".join(lines)
        for content in (text.replace("\n", "\r\n"), text.replace("\n", "\r"),
                        "".join(line.replace("\n", "\r\n") if index % 3 else line
                                for index, line in enumerate(lines)),
                        text[:-1], "﻿" + text):
            with self.subTest(content=content[:40]):
                self.assert_same(content)

    def test_share_order_refusals_at_and_inside_range_boundaries(self):
        shares = [record("shares", {"share_seq": seq, "accepted": True}) for seq in (1, 2, 3, 9, 10)]
        for bad in (
            shares + [record("shares", {"share_seq": 10, "accepted": True})],
            shares[:2] + [record("shares", {"share_seq": 1, "accepted": False})] + shares[2:],
            [shares[0], record("shares", {"share_seq": "7", "accepted": True})] + shares[1:],
            shares[:3] + [record("shares", {"share_seq": 11, "accepted": "x"})],
            shares[:3] + [record("shares", {"share_seq": 11.5, "accepted": "1"}), record("shares", {"share_seq": 11, "accepted": True})],
            shares[:3] + [record("shares", {"accepted": True})],
            shares[:3] + [record("shares", [11, True])],
            shares[:3] + [record("shares", "eleven")],
        ):
            with self.subTest(bad=bad[-1]):
                code, _, stderr = self.assert_same(bad + closing())
                self.assertEqual(code, 1, stderr)
        # Mixed flag types that int() reads alike stay accepted.
        code, stdout, _ = self.assert_same(shares[:3] + [
            record("shares", {"share_seq": 12, "accepted": "1"}), record("shares", {"share_seq": 13.0, "accepted": 0}),
            record("shares", {"share_seq": 14, "accepted": False})] + closing())
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout)["last_share_seq"], 14)

    def test_malformed_lines_refuse_at_the_same_line_with_the_same_message(self):
        good = full_export()
        middle = len(good) // 2
        for bad, expected in (
            (good[:middle] + ['{"kind": "shares", "row": {"share_seq": 1\n'] + good[middle:], b"Expecting"),
            (good[:middle] + ["\n"] + good[middle:], b"Expecting value"),
            (good[:middle] + ['{"row": {}}\n'] + good[middle:], b"'kind'"),
            (good[:middle] + ['{"kind": "shares"}\n'] + good[middle:], b"'row'"),
            (good[:middle] + ['"just a string"\n'] + good[middle:], b"string indices"),
            (good[:middle] + ['{"kind": ["shares"], "row": {}}\n'] + good[middle:], b"unhashable"),
            (good[:middle] + [record("future_format", {})] + good[middle:], b"unknown evidence kind"),
            (good[:middle] + [record("audit_bodies", {"x": 1})[:-5]], b"Expecting"),
            (good + ['{"kind": "shares", "row":\n'], b"records follow completion marker"),
            (good + [record("shares", {"share_seq": 1000, "accepted": True})], b"records follow completion marker"),
            (good[:-1], b"incomplete export"),
            (good[:-2] + [record("complete", True)], b"invalid completion marker"),
            (good[:-1] + [good[-2]] + good[-1:], b"duplicate integrity report"),
            (good[:-1] + [record("payouts", {"payout_entry_seq": 9})] + good[-1:], b"accounting records follow"),
            ([record("active_carry", carry(1, 10, "a", "aa", gross=1, prior=0)),
              record("blocks", {"block_hash": "b9"})] + closing(checked_active_rows=1), b"block records follow"),
        ):
            with self.subTest(expected=expected):
                code, stdout, stderr = self.assert_same(bad)
                self.assertEqual((code, stdout), (1, b""))
                self.assertIn(expected, stderr)

    def test_carry_and_integrity_refusals_are_the_serial_ones(self):
        row = carry(1, 10, "alice", "aa", gross=1, prior=0)
        later = carry(2, 11, "alice", "aa", gross=1, prior=1)
        marked = record("blocks", {"block_hash": "b11", "as_issued_audit_sha256": "ab" * 32})
        for bad in (
            [record("active_carry", later), record("active_carry", row)] + closing(checked_active_rows=2),
            [record("active_carry", row), record("active_carry", later | {"prior_balance_sats": "99"})]
            + closing(checked_active_rows=2),
            [record("active_carry", row | {"carry_forward_balance_sats": "0_001"})] + closing(checked_active_rows=1),
            [marked, record("active_carry", later)] + closing(
                checked_active_rows=1, mismatch_count=1, mismatches=[{"carry_forward_seq": 2, "block_hash": "b11"}]),
            [record("active_carry", row)] + closing(
                checked_active_rows=1, mismatch_count=1, mismatches=[{"carry_forward_seq": 1, "block_hash": "b10"}]),
            [record("active_carry", row)] + closing(checked_active_rows=2),
            closing(current_drift_count=1),
            closing(mismatch_count=1),
        ):
            with self.subTest(bad=bad[0][:60]):
                code, _, stderr = self.assert_same(bad)
                self.assertEqual(code, 1, stderr)
                self.assertIn(b"recovery evidence failed:", stderr)

    def test_invalid_utf8_is_summarized_again_serially_with_the_serial_refusal(self):
        good = "".join(full_export()).encode("utf-8")
        middle = len(good) // 2
        for bad in (good[:middle] + b"\xff\xfe" + good[middle:], b"\xc3" + good, good + b"\x80\n"):
            with self.subTest(position=bad.find(b"\xff")):
                code, _, stderr = self.assert_same(bad)
                self.assertEqual(code, 1)
                self.assertIn(b"codec can't decode", stderr)

    def test_paths_that_are_not_regular_files_fail_as_before(self):
        for path in (self.root / "missing.jsonl", self.root):
            serial = self.run_summary(path, "--jobs", "1")
            self.assertEqual(self.run_summary(path, "--jobs", "3", "--chunk-bytes", "7"), serial)
            self.assertEqual(serial[0], 1)

    def test_the_parallel_path_parses_in_workers_and_reads_no_line_serially(self):
        path = self.write(full_export())
        with path.open(encoding="utf-8") as source:
            expected = module.summarize(source)
        line = module._Summary.line

        def refuse(self, text):
            raise AssertionError("the parallel path read a line serially")

        module._Summary.line = refuse
        try:
            notes = []
            report = module.summarize_file(path, notes, jobs=3, chunk_bytes=50)
        finally:
            module._Summary.line = line
        self.assertEqual(report, expected)
        self.assertEqual(len(notes), 1)

    def test_from_an_unreadable_range_on_the_file_is_read_serially_once(self):
        lines = full_export()
        content = "".join(lines).encode("utf-8")
        cut = content.index(b'"kind": "blocks"')
        path = self.write(content[:cut] + b"\xff" + content[cut:])
        read = []
        line = module._Summary.line

        def counted(self, text):
            read.append(text)
            return line(self, text)

        module._Summary.line = counted
        try:
            with self.assertRaises(UnicodeDecodeError):
                module.summarize_file(path, [], jobs=3, chunk_bytes=64)
        finally:
            module._Summary.line = line
        # The lines the workers' ranges checked are read again but not
        # checked again: only lines from the unreadable range on are.
        self.assertLess(len(read), len(lines) // 2)

    def test_a_copy_the_workers_cannot_find_by_name_summarizes_serially(self):
        copy_spec = importlib.util.spec_from_file_location("recovery_evidence_unregistered", SCRIPT)
        copy = importlib.util.module_from_spec(copy_spec)
        copy_spec.loader.exec_module(copy)
        path = self.write(full_export())
        with path.open(encoding="utf-8") as source:
            expected = copy.summarize(source)
        self.assertFalse(copy._workers_available())
        self.assertEqual(copy.summarize_file(path, [], jobs=3, chunk_bytes=50), expected)

    def test_one_job_is_the_serial_summarizer(self):
        path = self.write(full_export())
        serial = module.summarize
        calls = []

        def counted(lines, notes=None, summary=None):
            calls.append(summary)
            return serial(lines, notes, summary)

        module.summarize = counted
        try:
            module.summarize_file(path, [], jobs=1)
        finally:
            module.summarize = serial
        self.assertEqual(calls, [None])

    def test_a_host_that_cannot_start_workers_summarizes_serially(self):
        path = self.write(full_export())
        with path.open(encoding="utf-8") as source:
            expected = module.summarize(source)

        class NoFork(module.ProcessPoolExecutor):
            def submit(self, *args, **kwargs):
                raise BlockingIOError(11, "Resource temporarily unavailable")

        class NoPool:
            def __init__(self, *args, **kwargs):
                raise OSError(38, "Function not implemented")

        pool = module.ProcessPoolExecutor
        for broken in (NoFork, NoPool):
            with self.subTest(broken=broken.__name__):
                module.ProcessPoolExecutor = broken
                try:
                    self.assertEqual(module.summarize_file(path, [], jobs=3, chunk_bytes=50), expected)
                finally:
                    module.ProcessPoolExecutor = pool

    def test_ranges_end_after_newlines_and_the_last_reads_to_the_end(self):
        for content in (b"", b"a\n", b"a\nbb\nccc\n", b"a\nbb\nccc", b"\n\n\n", b"x" * 10):
            path = self.write(content)
            for chunk in (1, 2, 3, 5, 100):
                with self.subTest(content=content, chunk=chunk), path.open("rb") as source:
                    ranges = list(module._ranges(source, chunk))
                    self.assertEqual(ranges[-1][1], None)
                    self.assertEqual(ranges[0][0], 0)
                    for (start, end), (following, _) in zip(ranges, ranges[1:]):
                        self.assertEqual(end, following)
                        self.assertGreater(end, start)
                        self.assertEqual(content[end - 1:end], b"\n")
        # A line still being written when the ranges are cut is never split:
        # the open last range is read wherever the file ends by then.
        path = self.write(b"a\nbb\n" + b"c" * 50)
        with path.open("rb") as source:
            ranges = list(module._ranges(source, 1))
        with path.open("ab") as source:
            source.write(b"c\n")
        lines = []
        for start, end in ranges:
            _, count, _items = module._parse_range(str(path), start, end)
            lines.append(count)
        self.assertEqual(sum(lines), 3)

    def test_an_uncaught_error_prints_the_serial_traceback(self):
        # Python 3.14's stack-overflow message reports the C stack used, which
        # depends on the process's argv and environment size: two serial runs
        # with different arguments differ there. Every frame must not.
        def masked(result):
            code, stdout, stderr = result
            return code, stdout, re.sub(rb"used \d+ kB", b"used N kB", stderr)

        deep = '{"kind": "audit_bodies", "row": ' + "[" * 200000 + "]" * 200000 + "}\n"
        lines = full_export()
        path = self.write(lines[:20] + [deep] + lines[20:])
        serial = masked(self.run_summary(path, "--jobs", "1"))
        self.assertNotEqual(serial[0], 0)
        self.assertIn(b"RecursionError", serial[2])
        self.assertIn(b"in summarize", serial[2])
        for jobs, chunk in PARALLEL:
            with self.subTest(jobs=jobs, chunk=chunk):
                self.assertEqual(
                    masked(self.run_summary(path, "--jobs", str(jobs), "--chunk-bytes", str(chunk))), serial)


if __name__ == "__main__":
    unittest.main()

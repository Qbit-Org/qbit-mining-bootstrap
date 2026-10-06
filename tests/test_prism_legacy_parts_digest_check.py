"""List the legacy audit rows import-audits refuses for 2.x's window-proof parts
digest, and write their sidecars in shards (#731)."""
import contextlib
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/prism_legacy_parts_digest_check.py"
SIDECAR_TOOL = REPO / "scripts/prism_legacy_range_sidecars.py"
SHARDED = REPO / "scripts/prism_legacy_range_sidecars_sharded.sh"
FIXTURE = REPO / "tests/fixtures/prism-legacy-v2-audit-body"
spec = importlib.util.spec_from_file_location("legacy_parts_digest_check", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
tool_spec = importlib.util.spec_from_file_location("legacy_range_sidecars", SIDECAR_TOOL)
tool = importlib.util.module_from_spec(tool_spec)
tool_spec.loader.exec_module(tool)

PREFIX = "/var/lib/qbit-mining-pool/prism/audit"
SEGMENT = "qbit.prism.audit-share-segment.v1"
BODY_REF = "qbit.prism.audit-body-ref.v1"
V2 = "qbit.prism.audit-bundle.v2"
PROOF = "qbit.prism.window-completeness-proof.v1"
HEADER = "block_hash,body_uri,audit_bundle_sha256"
SLOT = "prism-audit-share-segment-slot-1-8.json"
CANONICALIZE_BIN = os.environ.get("PRISM_AUDIT_CANONICALIZE_BIN", "")
# The sharded runner splits with GNU split's round robin and resolves paths
# with GNU realpath -m; BSD split has no --version.
GNU_COREUTILS = shutil.which("split") is not None and subprocess.run(
    ["split", "--version"], capture_output=True).returncode == 0

# The 3.x canonicalizer's contract, for the shapes these tests write: refuse a
# v2 body whose parts digest is not the sorted-key form 3.x recomputes, resolve
# the share parts, check each range digest as serde_json writes it, require
# sha256(canonical bundle) == audit_bundle_sha256, and print the canonical
# bytes. Its canonical form is the bundle as compact JSON.
STUB = textwrap.dedent("""\
    import hashlib, json, sys
    from pathlib import Path

    def serde(value, sort_keys=False):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False,
                          sort_keys=sort_keys).encode()

    path = Path(sys.argv[sys.argv.index("--input") + 1])
    body = json.loads(path.read_bytes())
    if body["schema"] == "qbit.prism.audit-body-ref.v1":
        parts = body["share_parts"]
    else:
        proof = body["share_window_proof"]
        parts = proof["share_parts"]
        expected = proof.get("share_parts_digest_hex")
        if expected and hashlib.sha256(serde({"share_parts": parts}, True)).hexdigest() != expected:
            sys.exit("qbit-prism-audit-canonicalize: invalid audit body ref: "
                     "window proof share_parts_digest_hex mismatch")
    shares = []
    for part in parts:
        if part["kind"] == "inline":
            shares += part["shares"]
            continue
        slot = Path(part["body_uri"])
        slot = slot if slot.is_absolute() else path.parent / slot
        segment = json.loads(slot.read_bytes())
        first, last = part["first_share_seq"], part["last_share_seq"]
        selected = [s for s in segment["shares"] if first <= s["share_seq"] <= last]
        payload = {"schema": "qbit.prism.audit-share-segment.v1", "first_share_seq": first,
                   "last_share_seq": last, "share_count": len(selected), "shares": selected}
        if hashlib.sha256(serde(payload)).hexdigest() != part["range_sha256"]:
            sys.exit("qbit-prism-audit-canonicalize: audit body ref hash mismatch")
        shares += selected
    bundle = dict(body["bundle_without_shares"])
    bundle["shares"] = shares
    canonical = serde(bundle)
    if hashlib.sha256(canonical).hexdigest() != body["audit_bundle_sha256"]:
        sys.exit("qbit-prism-audit-canonicalize: audit body ref hash mismatch")
    sys.stdout.buffer.write(canonical)
""")


def compact(value, ascii_only=True):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=ascii_only).encode()


def digest_as_2x_wrote_it(parts):
    """2.x's share_parts_digest_hex: insertion order, ASCII-escaped."""
    return hashlib.sha256(compact({"share_parts": parts})).hexdigest()


def digest_as_3x_recomputes_it(parts):
    return hashlib.sha256(json.dumps({"share_parts": parts}, separators=(",", ":"),
                                     sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def share(seq):
    return {
        "share_seq": seq, "share_id": f"{seq:064x}",
        "miner_id": f"qb1zexample.rig{seq:02d}", "order_key": "qb1zexample",
        "p2mr_program_hex": "ab" * 32, "share_difficulty": 2 ** 40 + seq,
        "network_difficulty": 2 ** 42, "template_height": 53313, "job_id": f"job-{seq}",
        "job_issued_at_ms": 1755815340000 + seq, "accepted_at_ms": 1755815341000 + seq,
        "ntime": 1755815340,
    }


def write_stub(directory):
    canonicalizer = Path(directory) / "qbit-prism-audit-canonicalize"
    canonicalizer.write_text(f"#!{sys.executable}\n" + STUB)
    canonicalizer.chmod(canonicalizer.stat().st_mode | stat.S_IXUSR)
    return canonicalizer


class Tree:
    """A restored audit tree of 2.x v2 bodies without sidecars, and an empty
    import root. Each block's body has the same six shares in one slot."""

    def __init__(self, root, *, blocks=1):
        self.root = Path(root)
        self.audit = self.root / "audit"
        self.audit.mkdir(parents=True)
        self.import_root = self.root / "import-root"
        self.import_root.mkdir()
        self.shares = [share(seq) for seq in range(1, 7)]
        self.bundle_without_shares = {"found_block": {"block_height": 53313}, "schema": "v1.1"}
        bundle = dict(self.bundle_without_shares)
        bundle["shares"] = self.shares
        self.canonical = compact(bundle, False)
        self.digest = hashlib.sha256(self.canonical).hexdigest()
        slot = {"schema": SEGMENT, "first_share_seq": 1, "last_share_seq": 8,
                "share_count": len(self.shares), "shares": self.shares}
        (self.audit / SLOT).write_bytes(compact(slot))
        self.parts = [self.range_part(1, 4), self.range_part(5, 6)]
        self.blocks = [f"{index + 1:02x}" * 32 for index in range(blocks)]
        for block in self.blocks:
            self.write_body(block, digest_as_2x_wrote_it(self.parts))
        self.rows = self.root / "rows.csv"
        self.write_rows()

    def range_part(self, first, last):
        """A segment_range part with 2.x's keys, in 2.x's order."""
        selected = self.shares[first - 1:last]
        payload = {"schema": SEGMENT, "first_share_seq": first, "last_share_seq": last,
                   "share_count": len(selected), "shares": selected}
        return {"kind": "segment_range", "segment_first_share_seq": 1,
                "segment_last_share_seq": 8, "first_share_seq": first, "last_share_seq": last,
                "share_count": len(selected), "range_sha256": hashlib.sha256(compact(payload)).hexdigest(),
                "body_uri": f"{PREFIX}/{SLOT}"}

    def body_name(self, block):
        return f"prism-audit-bundle-body-{block}-{self.digest}.json"

    def body_uri(self, block):
        return f"{PREFIX}/{self.body_name(block)}"

    def write_body(self, block, parts_digest, *, parts=None, schema=V2, digest=None, where=None):
        proof = {"schema": PROOF, "share_segment_size": 8, "first_share_seq": 1,
                 "last_share_seq": len(self.shares), "share_count": len(self.shares)}
        if parts_digest is not None:
            proof["share_parts_digest_hex"] = parts_digest
        proof["share_parts"] = self.parts if parts is None else parts
        body = {"schema": schema, "audit_bundle_sha256": digest or self.digest,
                "share_count": len(self.shares), "bundle_without_shares": self.bundle_without_shares,
                "share_window_proof": proof}
        path = (where or self.audit) / self.body_name(block)
        path.write_bytes(compact(body))
        return path

    def write_rows(self, rows=None):
        rows = rows if rows is not None else [
            (block, self.body_uri(block), self.digest) for block in self.blocks]
        self.rows.write_text(HEADER + "\n" + "".join(f"{b},{u},{d}\n" for b, u, d in rows))

    def sidecar(self, block):
        return self.import_root / f"prism-audit-bundle-canonical-{block}-{self.digest}.json.gz"


def run_checker(argv):
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = module.main([str(arg) for arg in argv])
    rows = [json.loads(line) for line in stdout.getvalue().splitlines()]
    return code, rows, json.loads(stderr.getvalue())


def check(tree, *extra):
    return run_checker(["--rows", tree.rows, "--audit-root", tree.audit,
                        "--sidecar-dir", tree.import_root, *extra])


class PartsDigestCheckTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def test_a_2x_body_without_a_sidecar_is_refused_and_listed(self):
        tree = Tree(self.dir, blocks=2)
        affected = self.dir / "affected.csv"
        code, rows, summary = check(tree, "--affected-csv", affected)
        self.assertEqual(code, 3)
        self.assertEqual([row["status"] for row in rows], ["2x-order-only"] * 2)
        self.assertEqual([(row["parts"], row["has_inline"]) for row in rows], [(2, False)] * 2)
        self.assertEqual(summary, {"import_refuses": 2, "totals": {"2x-order-only": 2}})
        # The refused rows, in the sidecar tool's --rows format.
        self.assertEqual(affected.read_text(), tree.rows.read_text())

    def test_a_sidecar_in_the_import_root_means_import_never_reads_the_body(self):
        tree = Tree(self.dir, blocks=2)
        (tree.audit / tree.body_name(tree.blocks[0])).unlink()
        tree.sidecar(tree.blocks[0]).write_bytes(b"")
        code, rows, summary = check(tree)
        self.assertEqual(code, 3)
        self.assertEqual([row["status"] for row in rows], ["has-sidecar", "2x-order-only"])
        self.assertEqual(rows[0]["sidecar"], str(tree.sidecar(tree.blocks[0])))
        self.assertEqual(summary["import_refuses"], 1)
        tree.sidecar(tree.blocks[1]).write_bytes(b"")
        code, rows, summary = check(tree)
        self.assertEqual((code, summary["import_refuses"]), (0, 0))

    def test_a_digest_in_the_form_3x_recomputes_passes(self):
        tree = Tree(self.dir)
        tree.write_body(tree.blocks[0], digest_as_3x_recomputes_it(tree.parts))
        code, [row], summary = check(tree)
        self.assertEqual((code, row["status"]), (0, "ok-in-3x"))
        self.assertEqual(summary, {"import_refuses": 0, "totals": {"ok-in-3x": 1}})

    def test_a_digest_in_neither_form_is_refused_and_listed(self):
        tree = Tree(self.dir)
        tree.write_body(tree.blocks[0], "00" * 32)
        affected = self.dir / "affected.csv"
        code, [row], summary = check(tree, "--affected-csv", affected)
        self.assertEqual((code, row["status"]), (3, "matches-neither"))
        self.assertEqual(summary["import_refuses"], 1)
        self.assertEqual(affected.read_text(), tree.rows.read_text())

    def test_unknown_keys_and_absent_segment_bounds_are_dropped_as_3x_drops_them(self):
        tree = Tree(self.dir)
        parts = [dict(tree.parts[0], note="2.x kept it, 3.x has no such field"),
                 dict(tree.parts[1], segment_first_share_seq=None, segment_last_share_seq=None)]
        seen_by_3x = [{key: value for key, value in part.items()
                       if key != "note" and value is not None} for part in parts]
        tree.write_body(tree.blocks[0], digest_as_3x_recomputes_it(seen_by_3x), parts=parts)
        code, [row], _ = check(tree)
        self.assertEqual((code, row["status"]), (0, "ok-in-3x"))
        tree.write_body(tree.blocks[0], digest_as_2x_wrote_it(parts), parts=parts)
        code, [row], _ = check(tree)
        self.assertEqual((code, row["status"]), (3, "2x-order-only"))

    def test_a_v2_body_without_the_digest_is_not_checked(self):
        tree = Tree(self.dir)
        tree.write_body(tree.blocks[0], None)
        code, [row], _ = check(tree)
        self.assertEqual((code, row["status"]), (0, "no-parts-digest"))

    def test_a_body_ref_or_a_plain_bundle_is_not_checked(self):
        tree = Tree(self.dir, blocks=2)
        tree.write_body(tree.blocks[0], "00" * 32, schema=BODY_REF)
        bundle = dict(tree.bundle_without_shares, shares=tree.shares)
        (tree.audit / tree.body_name(tree.blocks[1])).write_bytes(compact(bundle))
        code, rows, summary = check(tree)
        self.assertEqual(code, 0)
        self.assertEqual([(row["status"], row["schema"]) for row in rows],
                         [("no-window-proof", BODY_REF), ("no-window-proof", "v1.1")])
        self.assertEqual(summary["import_refuses"], 0)

    def test_body_problems_fail_the_run_but_are_not_listed_for_the_sidecar_tool(self):
        tree = Tree(self.dir, blocks=4)
        missing, garbled, malformed, other_digest = tree.blocks
        (tree.audit / tree.body_name(missing)).unlink()
        (tree.audit / tree.body_name(garbled)).write_bytes(b"{not json")
        tree.write_body(malformed, "00" * 32, parts="not a list")
        tree.write_body(other_digest, digest_as_2x_wrote_it(tree.parts), digest="11" * 32)
        tree.write_rows([(block, tree.body_uri(block), tree.digest) for block in tree.blocks]
                        + [("ff" * 32, "", tree.digest)])
        affected = self.dir / "affected.csv"
        code, rows, summary = check(tree, "--affected-csv", affected)
        self.assertEqual(code, 3)
        self.assertEqual([row["status"] for row in rows],
                         ["body-missing", "body-unreadable", "body-unreadable",
                          "body-digest-differs", "body-missing"])
        self.assertIn("JSONDecodeError", rows[1]["error"])
        self.assertEqual(rows[2]["error"], "share_window_proof.share_parts is not a list of objects")
        self.assertEqual(summary["import_refuses"], 0)
        self.assertEqual(affected.read_text(), HEADER + "\n")

    def test_the_uri_prefix_maps_onto_the_audit_root_and_relative_uris_onto_the_import_root(self):
        tree = Tree(self.dir, blocks=4)
        prefixed, file_uri, relative, absolute = tree.blocks
        # Import resolves a relative URI against its --root, the sidecar dir.
        (tree.audit / tree.body_name(relative)).rename(tree.import_root / tree.body_name(relative))
        elsewhere = self.dir / "elsewhere"
        elsewhere.mkdir()
        (tree.audit / tree.body_name(absolute)).rename(elsewhere / tree.body_name(absolute))
        tree.write_rows([
            (prefixed, tree.body_uri(prefixed), tree.digest),
            (file_uri, "file://" + tree.body_uri(file_uri), tree.digest),
            (relative, tree.body_name(relative), tree.digest),
            (absolute, str(elsewhere / tree.body_name(absolute)), tree.digest),
        ])
        code, rows, _ = check(tree)
        self.assertEqual([row["status"] for row in rows], ["2x-order-only"] * 4)
        self.assertEqual([row["body"] for row in rows], [
            str(tree.audit / tree.body_name(prefixed)), str(tree.audit / tree.body_name(file_uri)),
            str(tree.import_root / tree.body_name(relative)),
            str(elsewhere / tree.body_name(absolute))])
        # Without --sidecar-dir a relative URI resolves against the audit root.
        code, rows, _ = run_checker(["--rows", tree.rows, "--audit-root", tree.audit])
        self.assertEqual(rows[2]["status"], "body-missing")
        self.assertEqual(rows[2]["body"], str(tree.audit / tree.body_name(relative)))

    def test_the_csv_alias_and_body_file_arguments(self):
        tree = Tree(self.dir)
        by_rows = check(tree)
        by_csv = run_checker(["--csv", tree.rows, "--audit-root", tree.audit,
                              "--sidecar-dir", tree.import_root])
        self.assertEqual(by_rows, by_csv)
        body = tree.audit / tree.body_name(tree.blocks[0])
        code, [row], _ = run_checker([body])
        self.assertEqual((code, row["status"], row["body"]), (3, "2x-order-only", str(body)))
        self.assertNotIn("block_hash", row)

    def test_bad_arguments_are_refused(self):
        tree = Tree(self.dir)
        tree.rows.write_text("block_hash,body_uri\n")
        for argv in (["--rows", tree.rows], [], ["--jobs", "0", tree.rows]):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    module.main([str(arg) for arg in argv])
                self.assertEqual(raised.exception.code, 2)

    def test_parallel_jobs_report_exactly_what_one_process_does(self):
        # Run as a script: a pool's workers import the checker by its path.
        tree = Tree(self.dir, blocks=6)
        tree.write_body(tree.blocks[1], digest_as_3x_recomputes_it(tree.parts))
        tree.write_body(tree.blocks[2], "00" * 32)
        tree.sidecar(tree.blocks[3]).write_bytes(b"")
        (tree.audit / tree.body_name(tree.blocks[4])).unlink()
        runs = []
        for jobs in ("1", "3"):
            affected = self.dir / f"affected-{jobs}.csv"
            run = subprocess.run(
                [sys.executable, str(SCRIPT), "--rows", str(tree.rows), "--audit-root",
                 str(tree.audit), "--sidecar-dir", str(tree.import_root), "--jobs", jobs,
                 "--affected-csv", str(affected)], capture_output=True, text=True)
            runs.append((run.returncode, run.stdout, run.stderr, affected.read_text()))
        self.assertEqual(runs[0], runs[1])
        self.assertEqual(runs[0][0], 3)
        self.assertEqual([json.loads(line)["status"] for line in runs[0][1].splitlines()],
                         ["2x-order-only", "ok-in-3x", "matches-neither", "has-sidecar",
                          "body-missing", "2x-order-only"])

    def test_the_affected_rows_feed_the_sidecar_tool_and_clear_the_refusals(self):
        tree = Tree(self.dir, blocks=2)
        affected = self.dir / "affected.csv"
        check(tree, "--affected-csv", affected)
        report = self.dir / "report.jsonl"
        with contextlib.redirect_stdout(io.StringIO()):
            code = tool.main(["--rows", str(affected), "--audit-root", str(tree.audit),
                              "--canonicalizer", str(write_stub(self.dir)),
                              "--sidecar-dir", str(tree.import_root),
                              "--work-dir", str(self.dir / "work"), "--report", str(report),
                              "--write"])
        rows = [json.loads(line) for line in report.read_text().splitlines()]
        self.assertEqual((code, [row["status"] for row in rows]), (0, ["sidecar-written"] * 2))
        self.assertIn("share_parts_digest_hex mismatch", rows[0]["original_error"])
        for block in tree.blocks:
            self.assertEqual(gzip.decompress(tree.sidecar(block).read_bytes()), tree.canonical)
        code, rows, summary = check(tree)
        self.assertEqual((code, summary), (0, {"import_refuses": 0, "totals": {"has-sidecar": 2}}))


@unittest.skipUnless(GNU_COREUTILS, "the sharded runner needs GNU coreutils")
class ShardedRunnerTests(unittest.TestCase):
    """prism_legacy_range_sidecars_sharded.sh over the sidecar tool, with a
    canonicalizer stub that refuses 2.x's parts digest as 3.x does."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.canonicalizer = write_stub(self.dir)

    def run_shards(self, tree, out, *, shards="2", rows=None):
        return subprocess.run(
            ["bash", str(SHARDED), str(rows or tree.rows), str(tree.audit), str(tree.import_root),
             str(self.canonicalizer), str(SIDECAR_TOOL), str(out), shards],
            capture_output=True, text=True)

    def statuses(self, out):
        return sorted(json.loads(line)["status"] for report in sorted(Path(out).glob("report-*.jsonl"))
                      for line in report.read_text().splitlines())

    def test_shards_write_every_sidecar_once_and_a_rerun_verifies_them(self):
        tree = Tree(self.dir, blocks=5)
        run = self.run_shards(tree, self.dir / "out1")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        shards = sorted((self.dir / "out1").glob("shard-*.csv"))
        self.assertEqual([shard.name for shard in shards], ["shard-00.csv", "shard-01.csv"])
        sharded = [line for shard in shards for line in shard.read_text().splitlines()[1:]]
        self.assertTrue(all(shard.read_text().startswith(HEADER + "\n") for shard in shards))
        self.assertEqual(sorted(sharded), sorted(tree.rows.read_text().splitlines()[1:]))
        self.assertEqual(self.statuses(self.dir / "out1"), ["sidecar-written"] * 5)
        for block in tree.blocks:
            self.assertEqual(gzip.decompress(tree.sidecar(block).read_bytes()), tree.canonical)
        self.assertIn("temporary files left in IMPORT_ROOT: 0", run.stdout)
        rerun = self.run_shards(tree, self.dir / "out2", shards="3")
        self.assertEqual(rerun.returncode, 0, rerun.stdout + rerun.stderr)
        self.assertEqual(self.statuses(self.dir / "out2"), ["sidecar-ok"] * 5)

    def test_a_shard_that_fails_fails_the_run(self):
        tree = Tree(self.dir, blocks=4)
        (tree.audit / tree.body_name(tree.blocks[0])).unlink()
        run = self.run_shards(tree, self.dir / "out")
        self.assertNotEqual(run.returncode, 0)
        self.assertEqual(self.statuses(self.dir / "out"), ["body-missing"] + ["sidecar-written"] * 3)
        self.assertEqual(sorted(path.read_text().strip() for path in (self.dir / "out").glob("exit-*")),
                         ["0", "3"])

    def test_the_out_dir_guards_and_an_empty_rows_file(self):
        tree = Tree(self.dir)
        inside = tree.import_root / "out"
        run = self.run_shards(tree, inside)
        self.assertEqual((run.returncode, inside.exists()), (2, False))
        self.assertIn("OUT_DIR must not be inside IMPORT_ROOT", run.stderr)
        busy = self.dir / "busy"
        busy.mkdir()
        (busy / "left-over").write_text("")
        run = self.run_shards(tree, busy)
        self.assertEqual(run.returncode, 2)
        self.assertIn("is not empty", run.stderr)
        header_only = self.dir / "header-only.csv"
        header_only.write_text(HEADER + "\n")
        run = self.run_shards(tree, self.dir / "out", rows=header_only)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn("nothing to do", run.stdout)
        self.assertEqual(list((self.dir / "out").iterdir()), [])
        usage = subprocess.run(["bash", str(SHARDED), "rows.csv"], capture_output=True, text=True)
        self.assertEqual(usage.returncode, 2)
        self.assertIn("usage: prism_legacy_range_sidecars_sharded.sh", usage.stderr)


@unittest.skipUnless(CANONICALIZE_BIN, "set PRISM_AUDIT_CANONICALIZE_BIN, as "
                     "`make test-prism-legacy-range-sidecars` does")
class RealCanonicalizerTests(unittest.TestCase):
    """The real 3.x canonicalizer against a body 2.x's own writer produced
    (tests/fixtures/prism-legacy-v2-audit-body)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        [self.fixture_body] = FIXTURE.glob("prism-audit-bundle-body-*.json")
        self.body = json.loads(self.fixture_body.read_bytes())
        self.digest = self.body["audit_bundle_sha256"]

    def canonicalize(self, path):
        return subprocess.run([CANONICALIZE_BIN, "--input", str(path)], capture_output=True)

    def test_the_canonicalizer_refuses_exactly_what_the_checker_lists(self):
        proof = self.body["share_window_proof"]
        # The checker's 2.x form reproduces the digest 2.x's writer stored.
        self.assertEqual(module.digest_2x(proof["share_parts"]), proof["share_parts_digest_hex"])
        code, [row], _ = run_checker([self.fixture_body])
        self.assertEqual((code, row["status"]), (3, "2x-order-only"))
        refused = self.canonicalize(self.fixture_body)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn(b"window proof share_parts_digest_hex mismatch", refused.stderr)

        # With the slots beside the body, the digest's form alone decides.
        for path in FIXTURE.glob("prism-audit-share-segment-*.json"):
            shutil.copy(path, self.dir / path.name)
        for part in proof["share_parts"]:
            part["body_uri"] = Path(part["body_uri"]).name
        body = self.dir / "body.json"
        for digest, status, accepted in ((module.digest_2x, "2x-order-only", False),
                                         (module.digest_3x, "ok-in-3x", True)):
            with self.subTest(status=status):
                proof["share_parts_digest_hex"] = digest(proof["share_parts"])
                body.write_bytes(compact(self.body))
                code, [row], _ = run_checker([body])
                self.assertEqual(row["status"], status)
                run = self.canonicalize(body)
                self.assertEqual(run.returncode == 0, accepted, run.stderr)
                if accepted:
                    self.assertEqual(hashlib.sha256(run.stdout).hexdigest(), self.digest)
                else:
                    self.assertIn(b"share_parts_digest_hex mismatch", run.stderr)

    @unittest.skipUnless(GNU_COREUTILS, "the sharded runner needs GNU coreutils")
    def test_the_sharded_runner_proves_2x_bodies_with_the_real_canonicalizer(self):
        audit = self.dir / "audit"
        audit.mkdir()
        import_root = self.dir / "import-root"
        import_root.mkdir()
        for path in FIXTURE.glob("prism-audit-share-segment-*.json"):
            shutil.copy(path, audit / path.name)
        blocks = [f"{index:02x}" * 32 for index in (0xa1, 0xb2, 0xc3)]
        rows = self.dir / "rows.csv"
        lines = [HEADER]
        for block in blocks:
            name = f"prism-audit-bundle-body-{block}-{self.digest}.json"
            shutil.copy(self.fixture_body, audit / name)
            lines.append(f"{block},{PREFIX}/{name},{self.digest}")
        rows.write_text("\n".join(lines) + "\n")
        affected = self.dir / "affected.csv"
        code, _, summary = run_checker(["--rows", rows, "--audit-root", audit,
                                        "--sidecar-dir", import_root, "--affected-csv", affected])
        self.assertEqual((code, summary["import_refuses"]), (3, 3))

        def shards(out):
            run = subprocess.run(
                ["bash", str(SHARDED), str(affected), str(audit), str(import_root),
                 CANONICALIZE_BIN, str(SIDECAR_TOOL), str(out), "2"],
                capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
            return sorted(json.loads(line)["status"] for report in Path(out).glob("report-*.jsonl")
                          for line in report.read_text().splitlines())

        self.assertEqual(shards(self.dir / "out1"), ["sidecar-written"] * 3)
        for block in blocks:
            sidecar = import_root / f"prism-audit-bundle-canonical-{block}-{self.digest}.json.gz"
            self.assertEqual(hashlib.sha256(gzip.decompress(sidecar.read_bytes())).hexdigest(),
                             self.digest)
        code, _, summary = run_checker(["--rows", rows, "--audit-root", audit,
                                        "--sidecar-dir", import_root])
        self.assertEqual((code, summary), (0, {"import_refuses": 0, "totals": {"has-sidecar": 3}}))
        self.assertEqual(shards(self.dir / "out2"), ["sidecar-ok"] * 3)


if __name__ == "__main__":
    unittest.main()

"""Prove legacy bodies whose range digests use Python's escaped JSON (#709)."""
import contextlib
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/prism_legacy_range_sidecars.py"
spec = importlib.util.spec_from_file_location("legacy_range_sidecars", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

PREFIX = "/var/lib/qbit-mining-pool/prism/audit"
SEGMENT = "qbit.prism.audit-share-segment.v1"
BODY_REF = "qbit.prism.audit-body-ref.v1"
V2 = "qbit.prism.audit-bundle.v2"
CANONICALIZE_BIN = os.environ.get("PRISM_AUDIT_CANONICALIZE_BIN", "")

# The 3.x canonicalizer's contract, for the shapes these tests write: resolve
# the share parts, check each range digest as serde_json writes it (raw
# UTF-8), require sha256(canonical bundle) == audit_bundle_sha256, and print
# the canonical bytes. Its canonical form is the bundle as compact JSON.
STUB = textwrap.dedent("""\
    import hashlib, json, sys
    from pathlib import Path

    def serde(value):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()

    path = Path(sys.argv[sys.argv.index("--input") + 1])
    body = json.loads(path.read_bytes())
    if body["schema"] == "qbit.prism.audit-body-ref.v1":
        parts = body["share_parts"]
    else:
        parts = body["share_window_proof"]["share_parts"]
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


def compact(value, ascii_only):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=ascii_only).encode()


def share(seq, worker):
    return {
        "share_seq": seq, "share_id": f"{seq:064x}",
        "miner_id": f"qb1zexample.{worker}", "order_key": "qb1zexample",
        "p2mr_program_hex": "ab" * 32, "share_difficulty": 2 ** 70 + seq,
        "network_difficulty": 2 ** 72, "template_height": 53313, "job_id": f"job-{seq}",
        "job_issued_at_ms": 1755815340000 + seq, "accepted_at_ms": 1755815341000 + seq,
        "ntime": 1755815340,
    }


def range_digest(first, last, shares, ascii_only):
    payload = {"schema": SEGMENT, "first_share_seq": first, "last_share_seq": last,
               "share_count": len(shares), "shares": shares}
    return hashlib.sha256(compact(payload, ascii_only)).hexdigest()


def escaped_range_part(first, last, shares, body_uri):
    """A range part as 1.x and 2.x wrote it: the digest is Python's escaped JSON."""
    return {"kind": "segment_range", "first_share_seq": first, "last_share_seq": last,
            "share_count": len(shares), "range_sha256": range_digest(first, last, shares, True),
            "body_uri": body_uri}


def parted_body(schema, digest, bundle_without_shares, share_count, parts):
    common = {"schema": schema, "audit_bundle_sha256": digest, "share_count": share_count,
              "bundle_without_shares": bundle_without_shares}
    if schema == BODY_REF:
        return dict(common, share_parts=parts)
    return dict(common, share_window_proof={
        "schema": "qbit.prism.window-completeness-proof.v1", "first_share_seq": 1,
        "last_share_seq": share_count, "share_count": share_count, "share_parts": parts})


class Fixture:
    """A restored audit tree holding one 1.x-style parted body."""

    def __init__(self, root, *, schema=BODY_REF, workers=None, absolute_slot_uri=True):
        self.root = Path(root)
        workers = workers or ["rig01", "rig02", "rig03", "Bjørn-rig", "rig05", "Bjørn-rig"]
        self.shares = [share(seq, worker) for seq, worker in enumerate(workers, start=1)]
        self.bundle_without_shares = {"found_block": {"block_height": 53313}, "schema": "v1.1"}
        bundle = dict(self.bundle_without_shares)
        bundle["shares"] = self.shares
        self.canonical = compact(bundle, False)
        self.digest = hashlib.sha256(self.canonical).hexdigest()
        self.block = "00" * 16 + "11e43" + "0" * 27
        (self.root / "segments").mkdir(parents=True)
        (self.root / "bodies").mkdir()
        slot = {"schema": SEGMENT, "first_share_seq": 1, "last_share_seq": len(self.shares),
                "share_count": len(self.shares), "shares": self.shares}
        self.slot = self.root / "segments/slot-1-10.json"
        self.slot.write_bytes(compact(slot, True))
        parts = [
            escaped_range_part(1, 3, self.shares[:3], "../segments/slot-1-10.json"),
            escaped_range_part(4, 6, self.shares[3:], (f"{PREFIX}/segments/slot-1-10.json"
                                                       if absolute_slot_uri
                                                       else "../segments/slot-1-10.json")),
        ]
        body = parted_body(schema, self.digest, self.bundle_without_shares, len(self.shares),
                           parts)
        self.body = self.root / f"bodies/{self.block}.json"
        self.body.write_bytes(compact(body, True))
        self.rows = self.root / "rows.csv"
        self.write_rows(f"{PREFIX}/bodies/{self.block}.json")
        self.sidecar_dir = self.root / "sidecars"
        self.sidecar_dir.mkdir()
        self.sidecar = (self.sidecar_dir /
                        f"prism-audit-bundle-canonical-{self.block}-{self.digest}.json.gz")

    def write_rows(self, body_uri, block=None):
        self.rows.write_text("block_hash,body_uri,audit_bundle_sha256\n"
                             f"{block or self.block},{body_uri},{self.digest}\n")


class LegacyRangeSidecarTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.canonicalizer = Path(self.tmp.name) / "qbit-prism-audit-canonicalize"
        self.canonicalizer.write_text(f"#!{sys.executable}\n" + STUB)
        self.canonicalizer.chmod(self.canonicalizer.stat().st_mode | stat.S_IXUSR)

    def fixture(self, **kwargs):
        return Fixture(Path(self.tmp.name) / "audit", **kwargs)

    def run_tool(self, fx, *, write, paths=None):
        report = Path(self.tmp.name) / "report.jsonl"
        paths = paths or {"--audit-root": fx.root, "--sidecar-dir": fx.sidecar_dir,
                          "--work-dir": Path(self.tmp.name) / "work"}
        argv = ["--rows", str(fx.rows), "--uri-prefix", PREFIX,
                "--canonicalizer", str(self.canonicalizer), "--report", str(report)]
        for flag, path in paths.items():
            argv += [flag, str(path)]
        if write:
            argv.append("--write")
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = module.main(argv)
        rows = [json.loads(line) for line in report.read_text().splitlines()]
        return code, rows, stderr.getvalue()

    def test_escaped_range_digest_is_reproduced_and_the_body_proven(self):
        fx = self.fixture()
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual(code, 0)
        self.assertEqual(row["status"], "sidecar-written")
        self.assertEqual([part["status"] for part in row["parts"]],
                         ["ok-in-3x", "python-escaped"])
        self.assertIn({"share_seq": 4, "field": "miner_id", "text": "qb1zexample.Bjørn-rig"},
                      row["parts"][1]["non_ascii"])
        self.assertEqual(gzip.decompress(fx.sidecar.read_bytes()), fx.canonical)

    def test_v2_body_is_proven_through_a_body_ref_rewrite(self):
        fx = self.fixture(schema=V2)
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (0, "sidecar-written"))
        self.assertEqual(gzip.decompress(fx.sidecar.read_bytes()), fx.canonical)

    def test_dry_run_classifies_without_writing(self):
        fx = self.fixture()
        code, [row], _ = self.run_tool(fx, write=False)
        self.assertEqual((code, row["status"]), (0, "proven"))
        self.assertFalse(fx.sidecar.exists())

    def test_an_ascii_body_already_verifies_and_gets_no_sidecar(self):
        fx = self.fixture(workers=["rig01", "rig02", "rig03", "rig04", "rig05", "rig06"],
                          absolute_slot_uri=False)
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (0, "ok-in-3x"))
        self.assertFalse(fx.sidecar.exists())

    def test_a_tree_mounted_away_from_its_uris_still_gets_a_proven_sidecar(self):
        # The body names its slot by the production path, which this mount
        # does not have; the mapped rewrite proves it and the sidecar spares
        # import the slot lookup.
        fx = self.fixture(workers=["rig01", "rig02", "rig03", "rig04", "rig05", "rig06"])
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (0, "sidecar-written"))
        self.assertEqual([part["status"] for part in row["parts"]], ["ok-in-3x", "ok-in-3x"])
        self.assertEqual(gzip.decompress(fx.sidecar.read_bytes()), fx.canonical)

    def test_relative_paths_do_not_depend_on_the_work_directory(self):
        # The second range keeps its slot, named by the production path; the
        # rewritten body-ref, read from the work directory, must still find it.
        fx = self.fixture(workers=["Bjørn-rig", "rig02", "rig03", "rig04", "rig05", "rig06"])
        # A relative body_uri resolves against --audit-root, as import's does
        # against --root.
        fx.write_rows(f"bodies/{fx.block}.json")
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.tmp.name)
        code, [row], _ = self.run_tool(fx, write=True, paths={
            "--audit-root": "audit", "--sidecar-dir": "audit/sidecars", "--work-dir": "work"})
        self.assertEqual((code, row["status"]), (0, "sidecar-written"))
        self.assertEqual([part["status"] for part in row["parts"]],
                         ["python-escaped", "ok-in-3x"])
        self.assertEqual(gzip.decompress(fx.sidecar.read_bytes()), fx.canonical)

    def test_a_valid_existing_sidecar_is_kept_and_counts_as_done(self):
        fx = self.fixture()
        original = gzip.compress(fx.canonical)
        fx.sidecar.write_bytes(original)
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (0, "sidecar-ok"))
        self.assertEqual(fx.sidecar.read_bytes(), original)

    def test_write_refuses_to_overwrite_an_existing_sidecar(self):
        fx = self.fixture()
        fx.sidecar.write_bytes(b"not the canonical bundle")
        code, [row], stderr = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (3, "sidecar-bad"))
        self.assertIn(str(fx.sidecar), stderr)
        self.assertEqual(fx.sidecar.read_bytes(), b"not the canonical bundle")

    def test_a_sidecar_with_bytes_after_its_gzip_member_is_bad(self):
        # Import reads only the first member, so anything after it is refused
        # rather than trusted.
        fx = self.fixture()
        fx.sidecar.write_bytes(gzip.compress(fx.canonical) + gzip.compress(b"\n"))
        code, [row], stderr = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (3, "sidecar-bad"))
        self.assertIn(str(fx.sidecar), stderr)

    def test_a_sidecar_linked_from_outside_the_import_root_is_bad(self):
        # Import refuses a sidecar that resolves outside its --root, however
        # good the bytes.
        fx = self.fixture()
        outside = Path(self.tmp.name) / "elsewhere.json.gz"
        outside.write_bytes(gzip.compress(fx.canonical))
        fx.sidecar.symlink_to(outside)
        code, [row], stderr = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (3, "sidecar-bad"))
        self.assertIn(str(fx.sidecar), stderr)

    def test_write_refuses_a_sidecar_that_appears_while_proving(self):
        fx = self.fixture()
        target = fx.sidecar
        self.assertEqual(module.write_sidecar(target, b"first"), "sidecar-written")
        self.assertEqual(module.write_sidecar(target, b"second"), "sidecar-conflict")
        self.assertEqual(gzip.decompress(target.read_bytes()), b"first")
        self.assertEqual([path.name for path in fx.sidecar_dir.iterdir()], [target.name])

    def test_a_corrupt_body_is_reported_and_gets_no_sidecar(self):
        fx = self.fixture()
        slot = json.loads(fx.slot.read_bytes())
        slot["shares"][4]["share_difficulty"] += 1  # damage inside the failing range
        fx.slot.write_bytes(compact(slot, True))
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual(code, 3)
        self.assertEqual(row["status"], "canonicalizer-refused")
        self.assertEqual(row["parts"][1]["status"], "unexplained")
        self.assertFalse(fx.sidecar.exists())

    def test_an_unreadable_body_is_reported_and_gets_no_sidecar(self):
        fx = self.fixture()
        fx.body.write_bytes(b'{"schema": "qbit.prism.audit-body-ref.v1", "share_')
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (3, "body-unreadable"))
        self.assertFalse(fx.sidecar.exists())

    def test_a_missing_slot_is_reported(self):
        fx = self.fixture()
        fx.slot.unlink()
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (3, "slot-incomplete"))
        self.assertFalse(fx.sidecar.exists())

    def test_a_row_that_is_not_a_hash_identity_is_refused(self):
        fx = self.fixture()
        fx.write_rows(f"{PREFIX}/bodies/{fx.block}.json", block="../../escape")
        code, [row], _ = self.run_tool(fx, write=True)
        self.assertEqual((code, row["status"]), (3, "bad-row"))
        self.assertEqual(list(fx.sidecar_dir.iterdir()), [])

    def test_write_needs_a_sidecar_dir(self):
        fx = self.fixture()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            module.main(["--rows", str(fx.rows), "--audit-root", str(fx.root),
                         "--canonicalizer", str(self.canonicalizer), "--write"])


@unittest.skipUnless(CANONICALIZE_BIN, "set PRISM_AUDIT_CANONICALIZE_BIN, as "
                     "`make test-prism-legacy-range-sidecars` does")
class RealCanonicalizerTests(unittest.TestCase):
    """The real 3.x canonicalizer refuses a 1.x-style body whose range digest
    is escaped JSON, and proves the tool's rewrite. The bundle comes from the
    qbit-prism-build-audit-bundle beside it, with non-ASCII worker names."""

    def real_bundle(self, tmp):
        fixture = json.loads(
            (REPO / "crates/qbit-prism/fixtures/power-law-accrual.prism-fixture.json").read_bytes())
        for accepted in fixture["shares"][1::3]:
            accepted["miner_id"] += ".Bjørn-rig"
        # The fixture's carry-forward, as crates/qbit-prism/tests/audit_cli.rs passes it.
        prior = [{"recipient_id": "miner-whale", "order_key": "01",
                  "p2mr_program_hex": "11" * 32, "balance_sats": 4_800}]
        builder_input = Path(tmp) / "input.json"
        builder_input.write_bytes(compact({"found_block": fixture["found_block"],
                                           "shares": fixture["shares"],
                                           "prior_balances": prior}, False))
        builder = Path(CANONICALIZE_BIN).with_name("qbit-prism-build-audit-bundle")
        return subprocess.run(
            [str(builder), "--input", str(builder_input), "--signing-key-seed-hex", "42" * 32,
             "--ledger-signing-key-seed-hex", "43" * 32, "--canonical-output"],
            capture_output=True, check=True).stdout

    def test_real_canonicalizer_refuses_the_body_and_proves_the_rewrite(self):
        for schema in (BODY_REF, V2):
            with self.subTest(schema=schema), tempfile.TemporaryDirectory() as tmp:
                canonical = self.real_bundle(tmp)
                digest = hashlib.sha256(canonical).hexdigest()
                bundle = json.loads(canonical)
                shares = bundle.pop("shares")
                root = Path(tmp) / "audit"
                (root / "segments").mkdir(parents=True)
                slot = {"schema": SEGMENT, "first_share_seq": 1, "last_share_seq": len(shares),
                        "share_count": len(shares), "shares": shares}
                (root / "segments/slot.json").write_bytes(compact(slot, True))
                parts = [escaped_range_part(1, 1, shares[:1], "segments/slot.json"),
                         escaped_range_part(2, len(shares), shares[1:], "segments/slot.json")]
                body = parted_body(schema, digest, bundle, len(shares), parts)
                (root / "body.json").write_bytes(compact(body, True))
                refused, error = module.canonicalize(CANONICALIZE_BIN, root / "body.json")
                self.assertIsNone(refused)
                self.assertIn("hash mismatch", error)

                rows = Path(tmp) / "rows.csv"
                rows.write_text("block_hash,body_uri,audit_bundle_sha256\n"
                                f"{'cd' * 32},{PREFIX}/body.json,{digest}\n")
                sidecars = Path(tmp) / "sidecars"
                sidecars.mkdir()
                report = Path(tmp) / "report.jsonl"
                with contextlib.redirect_stdout(io.StringIO()):
                    code = module.main(["--rows", str(rows), "--audit-root", str(root),
                                        "--canonicalizer", CANONICALIZE_BIN,
                                        "--sidecar-dir", str(sidecars),
                                        "--work-dir", str(Path(tmp) / "work"),
                                        "--report", str(report), "--write"])
                [row] = [json.loads(line) for line in report.read_text().splitlines()]
                self.assertEqual((code, row["status"]), (0, "sidecar-written"))
                self.assertEqual([part["status"] for part in row["parts"]],
                                 ["ok-in-3x", "python-escaped"])
                [sidecar] = sidecars.iterdir()
                self.assertEqual(gzip.decompress(sidecar.read_bytes()), canonical)


if __name__ == "__main__":
    unittest.main()

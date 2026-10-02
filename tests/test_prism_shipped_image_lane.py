"""Tests for scripts/prism_shipped_image_lane.py (#544, #487 L6)."""

from __future__ import annotations

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_shipped_image_lane as lane  # noqa: E402

# Bitcoin's genesis block: one transaction, so its merkle root is that txid.
GENESIS_COINBASE = (
    "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d"
    "0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f"
    "66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe"
    "5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7"
    "ba0b8d578a4c702b6bf11d5fac00000000"
)
GENESIS_HASH = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"


def genesis_job(**overrides: object) -> dict:
    job = {
        "job_id": "1",
        "prevhash": "00" * 32,
        "coinb1": GENESIS_COINBASE[:20],
        "coinb2": GENESIS_COINBASE[20:],
        "branch": [],
        "version": "00000001",
        "nbits": "1d00ffff",
        "ntime": "495fab29",
        "clean": True,
    }
    job.update(overrides)
    return job


class Header(unittest.TestCase):
    def test_it_rebuilds_the_genesis_header_from_a_notify(self) -> None:
        header = lane.block_header(genesis_job(), b"", b"", 0x7C2BAC1D)
        self.assertEqual(len(header), 80)
        self.assertEqual(lane.double_sha256(header)[::-1].hex(), GENESIS_HASH)

    def test_extranonces_sit_between_the_two_coinbase_halves(self) -> None:
        split = genesis_job(coinb1=GENESIS_COINBASE[:16], coinb2=GENESIS_COINBASE[24:])
        extranonce1 = bytes.fromhex(GENESIS_COINBASE[16:20])
        extranonce2 = bytes.fromhex(GENESIS_COINBASE[20:24])
        header = lane.block_header(split, extranonce1, extranonce2, 0x7C2BAC1D)
        self.assertEqual(lane.double_sha256(header)[::-1].hex(), GENESIS_HASH)

    def test_prevhash_words_are_byte_swapped_as_qbit_prism_miner_does(self) -> None:
        internal = bytes(range(32))
        stratum = b"".join(internal[i : i + 4][::-1] for i in range(0, 32, 4)).hex()
        header = lane.block_header(genesis_job(prevhash=stratum), b"", b"", 0)
        self.assertEqual(header[4:36], internal)

    def test_the_merkle_branch_folds_on_the_right(self) -> None:
        sibling = "11" * 32
        header = lane.block_header(genesis_job(branch=[sibling]), b"", b"", 0)
        txid = lane.double_sha256(bytes.fromhex(GENESIS_COINBASE))
        self.assertEqual(header[36:68], lane.double_sha256(txid + bytes.fromhex(sibling)))


class Solving(unittest.TestCase):
    def test_difficulty_one_is_the_diff1_target(self) -> None:
        self.assertEqual(lane.difficulty_target(1.0), 0xFFFF << 208)
        self.assertEqual(lane.difficulty_target(1e-30), (1 << 256) - 1)

    def test_a_solved_share_meets_half_the_share_target(self) -> None:
        job = genesis_job(nbits="207fffff")
        extranonce2, nonce = lane.solve_share(job, b"\x00\x00\x00\x01", 8, 1e-9)
        header = lane.block_header(job, b"\x00\x00\x00\x01", bytes.fromhex(extranonce2), int(nonce, 16))
        self.assertEqual(len(bytes.fromhex(extranonce2)), 8)
        self.assertLessEqual(int.from_bytes(lane.double_sha256(header), "little"),
                             lane.difficulty_target(1e-9) // 2)

    def test_notify_params_map_to_named_fields(self) -> None:
        message = {"method": "mining.notify",
                   "params": ["j", "p", "c1", "c2", ["b"], "v", "n", "t", True]}
        self.assertEqual(lane.notify_job(message), {
            "job_id": "j", "prevhash": "p", "coinb1": "c1", "coinb2": "c2", "branch": ["b"],
            "version": "v", "nbits": "n", "ntime": "t", "clean": True,
        })


class Refusals(unittest.TestCase):
    def test_the_reason_id_names_a_refusal(self) -> None:
        # As the lane's first run recorded them from the shipped images.
        stale = {"id": 3, "result": None, "error": [21, "stale job", {"reason_id": "unknown-job"}]}
        duplicate = {"id": 4, "result": None, "error": [22, "duplicate share", {"reason_id": "duplicate-share"}]}
        self.assertEqual(lane.refusal_reason(stale), "unknown-job")
        self.assertEqual(lane.refusal_reason(duplicate), "duplicate-share")

    def test_an_accepted_or_silent_answer_is_not_a_refusal(self) -> None:
        self.assertIsNone(lane.refusal_reason({"id": 3, "result": True, "error": None}))
        self.assertIsNone(lane.refusal_reason({"id": 3, "result": None, "error": None}))

    def test_a_reason_without_details_falls_back_to_the_message(self) -> None:
        self.assertEqual(lane.refusal_reason({"result": False, "error": [23, "low difficulty"]}), "low difficulty")


class ClientOutput(unittest.TestCase):
    def test_cpuminer_totals_come_from_its_last_result_line(self) -> None:
        output = "\n".join([
            "[2026-09-29 10:00:00] 1 Submitted Diff 1e-09, Block 2, Job 1",
            "[2026-09-29 10:00:00] 1 A1 S0 R0 BLOCK SOLVED 1, 0.001 sec (2ms)",
            "[2026-09-29 10:00:01] 2 Submitted Diff 1e-09, Block 3, Job 2",
            "[2026-09-29 10:00:01] 2 Accepted 2 S0 R0 B1, 0.900 sec (3ms)",
            "[2026-09-29 10:00:02] 3 Submitted Diff 1e-09, Block 3, Job 2",
            "[2026-09-29 10:00:02] 3 A2 Stale 1 R0 B1, 0.500 sec (3ms)",
            "[2026-09-29 10:00:03] 4 Submitted Diff 1e-09, Block 4, Job 3",
            "[2026-09-29 10:00:03] 4 A2 S1 Rejected 1 B1, 0.500 sec (3ms)",
            "[2026-09-29 10:00:04] 5 Submitted Diff 1e-09, Block 4, Job 3",
        ])
        tally = lane.parse_cpuminer(output)
        self.assertEqual((tally.submitted, tally.accepted, tally.stale, tally.rejected, tally.blocks),
                         (5, 2, 1, 1, 1))
        self.assertEqual(tally.unanswered, 1)

    def test_cpuminer_with_no_results_has_nothing_accepted(self) -> None:
        tally = lane.parse_cpuminer("[2026-09-29 10:00:00] Stratum connection failed\n")
        self.assertEqual((tally.submitted, tally.accepted), (0, 0))

    def test_prism_miner_totals_come_from_its_summary(self) -> None:
        lines = [
            json.dumps({"event": "job", "job_id": "a"}),
            json.dumps({"event": "submit", "block_target_met": True, "request_id": 10}),
            json.dumps({"event": "share", "accepted": True}),
            json.dumps({"event": "submit", "block_target_met": False, "request_id": 11}),
            "2026-09-29T10:00:00Z INFO not json",
            json.dumps({"event": "summary", "submitted": 3, "accepted": 1, "rejected": 1}),
        ]
        tally = lane.parse_prism_miner("\n".join(lines))
        self.assertEqual((tally.submitted, tally.accepted, tally.rejected, tally.blocks), (3, 1, 1, 1))
        self.assertEqual(tally.unanswered, 1)

    def test_prism_miner_without_a_summary_fails(self) -> None:
        with self.assertRaises(lane.LaneFailure):
            lane.parse_prism_miner(json.dumps({"event": "job"}))


class Reconciliation(unittest.TestCase):
    def tally(self, submitted: int, accepted: int, rejected: int = 0) -> lane.ClientTally:
        return lane.ClientTally(submitted=submitted, accepted=accepted, rejected=rejected)

    def test_every_acknowledged_share_in_the_ledger_reconciles(self) -> None:
        self.assertEqual(lane.reconcile_client("c", self.tally(10, 9, 1), 9), [])

    def test_an_unanswered_submission_leaves_the_count_unprovable(self) -> None:
        # One acknowledged share lost and one unanswered share committed
        # would leave rows == acknowledged; counts cannot tell, so it fails.
        problems = lane.reconcile_client("c", self.tally(12, 9, 1), 9)
        self.assertEqual(len(problems), 1)
        self.assertIn("2 submissions were unanswered", problems[0])

    def test_a_missing_acknowledged_share_fails(self) -> None:
        problems = lane.reconcile_client("c", self.tally(10, 9, 1), 8)
        self.assertEqual(problems, ["c: 9 shares acknowledged but only 8 in the ledger"])

    def test_rows_beyond_the_acknowledged_shares_fail(self) -> None:
        problems = lane.reconcile_client("c", self.tally(10, 9, 1), 10)
        self.assertEqual(problems, ["c: 10 ledger rows but only 9 shares acknowledged"])

    def test_no_accepted_share_fails(self) -> None:
        self.assertIn("c: no share was accepted", lane.reconcile_client("c", self.tally(0, 0), 0))


class Wiring(unittest.TestCase):
    def test_the_ha_overlay_is_applied_last_and_the_lane_env_file_wins(self) -> None:
        command = lane.compose_command("p", Path("/w/lane.env"), "up", "-d")
        self.assertEqual(command[:4], ["docker", "compose", "--project-name", "p"])
        files = [command[i + 1] for i, arg in enumerate(command) if arg == "-f"]
        self.assertEqual(files, ["compose.yaml", "compose.prism-ha.yaml"])
        env_files = [command[i + 1] for i, arg in enumerate(command) if arg == "--env-file"]
        self.assertEqual(env_files, [lane.UPSTREAM_ENV_FILE, ".env.example", "/w/lane.env"])
        self.assertEqual(command[-4:], ["--profile", "prism", "up", "-d"])

    def test_the_upstream_pins_are_the_ones_prepare_qbit_source_reads(self) -> None:
        # prepare-qbit-source.sh and the Makefile prefer config/upstream.env.
        self.assertTrue((ROOT / "config/upstream.env").is_file())
        self.assertEqual(lane.UPSTREAM_ENV_FILE, "config/upstream.env")
        prepare = (ROOT / "scripts/prepare-qbit-source.sh").read_text(encoding="utf-8")
        self.assertIn('if [[ -f "${ROOT_DIR}/config/upstream.env" ]]; then', prepare)

    def test_every_compose_file_and_env_file_exists(self) -> None:
        for name in (*lane.COMPOSE_FILES, *lane.ENV_FILES):
            self.assertTrue((ROOT / name).is_file(), name)

    def test_the_lane_env_carries_the_seeds_and_instance_ids(self) -> None:
        text = lane.lane_env(Path("/src/qbit"), "11" * 32, "22" * 32, "ab" * 32)
        values = dict(line.split("=", 1) for line in text.splitlines() if not line.startswith("#"))
        self.assertEqual(values["QBIT_SRC_DIR"], "/src/qbit")
        self.assertEqual(values["PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX"], "22" * 32)
        self.assertEqual(values["PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX"], "ab" * 32)
        self.assertEqual(values["PRISM_PUBLIC_STRATUM_URL"], "stratum+tcp://127.0.0.1:3340")
        self.assertEqual([values["PRISM_HA_INSTANCE_ID_1"], values["PRISM_HA_INSTANCE_ID_2"]],
                         list(lane.INSTANCE_IDS))

    def test_the_lane_uses_the_overlays_default_host_ports(self) -> None:
        overlay = (ROOT / "compose.prism-ha.yaml").read_text(encoding="utf-8")
        self.assertIn("PRISM_HA_STRATUM_PORT_HOST_2:-3343}", overlay)
        self.assertIn("PRISM_HA_HEALTH_PORT_HOST_1:-127.0.0.1:3341}", overlay)
        self.assertIn("PRISM_HA_HEALTH_PORT_HOST_2:-127.0.0.1:3344}", overlay)
        self.assertEqual((lane.STRATUM_PORTS, lane.HEALTH_PORTS), ((3340, 3343), (3341, 3344)))

    def test_the_checks_are_unique_kebab_names(self) -> None:
        self.assertEqual(len(set(lane.CHECKS)), len(lane.CHECKS))
        for name in lane.CHECKS:
            self.assertRegex(name, r"^[a-z0-9][a-z0-9-]*$")

    @unittest.skipUnless(shutil.which("openssl"), "openssl is not installed")
    def test_the_writer_key_matches_qbit_pool_builder(self) -> None:
        # `cargo run -p qbit-pool-builder -- --signing-key-seed-hex 22...22
        # --print-public-key-hex` prints this key.
        self.assertEqual(lane.ed25519_public_key_hex("22" * 32),
                         "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0")

    def test_a_short_seed_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            lane.ed25519_public_key_hex("22" * 31)

    def test_the_address_guard_refuses_sql_metacharacters(self) -> None:
        self.assertIsNotNone(lane.ADDRESS.match("qbrt1" + "q" * 58))
        self.assertIsNone(lane.ADDRESS.match("qbrt1q' OR '1'='1"))


class FakeQbitd:
    """Just enough of qbitd's JSON-RPC for the lane's node phase (#637).

    createwallet is held until `release` is set: the node is still deriving
    the wallet's keys. Holding it on an event, not a sleep, decides the race
    with the client timeout. `on_listwallets` runs on each listwallets call
    and may finish the wallet; listwallets shows a wallet only once it is
    finished, as qbitd adds it to its list after creating and loading it.
    """

    ADDRESS = "qbrt1q" + "q" * 58

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.wallets: list[str] = []
        self.release = threading.Event()
        self.on_listwallets = lambda count: None
        self.createwallet_error: dict | None = None
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args) -> None:
                pass

            def do_POST(self) -> None:
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                method, params = request["method"], request["params"]
                fake.calls.append(method)
                error = None
                if method == "createwallet":
                    # Bounded so a test that never releases cannot hang.
                    fake.release.wait(timeout=60)
                    error = fake.createwallet_error
                    if error is None:
                        fake.wallets.append(params[0])
                    result = None if error else {"name": params[0]}
                elif method == "listwallets":
                    fake.on_listwallets(fake.calls.count("listwallets"))
                    result = list(fake.wallets)
                elif method == "getnewaddress":
                    result = fake.ADDRESS
                else:
                    result = {"method": method}
                body = json.dumps({"result": result, "error": error, "id": request["id"]}).encode()
                try:
                    self.send_response(500 if error else 200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except OSError:
                    pass  # The client gave up on this call.

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def finish_wallet(self) -> None:
        self.release.set()
        # The held createwallet has added the wallet once it returns from the wait.
        deadline = time.monotonic() + 10
        while not self.wallets and self.createwallet_error is None and time.monotonic() < deadline:
            time.sleep(0.01)

    def __enter__(self) -> "FakeQbitd":
        self.thread.start()
        self.port_patch = mock.patch.object(lane, "QBIT_RPC_PORT", self.server.server_address[1])
        self.port_patch.start()
        return self

    def __exit__(self, *exc) -> None:
        self.release.set()
        self.port_patch.stop()
        self.server.shutdown()
        self.server.server_close()


class WalletCreation(unittest.TestCase):
    """#637: createwallet outlasting its client timeout must not fail the lane."""

    def setUp(self) -> None:
        # A short client timeout stands in for the lane's 30 s; create=True
        # lets the same test run against the driver before the fix.
        patch = mock.patch.object(lane, "RPC_TIMEOUT", 0.3, create=True)
        patch.start()
        self.addCleanup(patch.stop)
        self.qbitd = FakeQbitd().__enter__()
        self.addCleanup(self.qbitd.__exit__, None, None, None)

    def test_a_node_phase_whose_createwallet_outlasts_the_client_timeout_waits_for_the_wallet(self) -> None:
        # The node finishes the wallet after the client has given up, as in
        # run 36786483193: still loading at the first poll, loaded by the second.
        self.qbitd.on_listwallets = lambda count: count == 2 and self.qbitd.finish_wallet()
        with tempfile.TemporaryDirectory() as work:
            (Path(work) / "lane.env").write_text("", encoding="utf-8")
            driver = lane.Lane(work=Path(work), out=Path(work), project="p", load_seconds=1,
                               cpuminer_threads=1, cpuminer_diff_multiplier=1.0,
                               prism_miner_hashes_per_second=1)
            with mock.patch.object(lane.Lane, "compose"):
                addresses = driver.start()
        self.assertEqual(addresses["bootstrap"], FakeQbitd.ADDRESS)
        self.assertEqual(self.qbitd.calls.count("createwallet"), 1)
        self.assertEqual(self.qbitd.calls.count("listwallets"), 2)
        self.assertEqual(driver.report["wallet"]["outcome"], "loaded after createwallet timed out")
        self.assertIn("node", driver.report["phases"])

    def test_a_wallet_created_within_the_timeout_is_not_polled_for(self) -> None:
        self.qbitd.release.set()
        self.assertEqual(lane.create_wallet(lane.Rpc(), "l6", seconds=5)["outcome"], "created")
        self.assertEqual(self.qbitd.calls, ["createwallet"])

    def test_a_wallet_that_never_loads_fails_within_the_one_deadline(self) -> None:
        started = time.monotonic()
        with self.assertRaises(lane.LaneFailure) as caught:
            lane.create_wallet(lane.Rpc(), "l6", seconds=1.5)
        elapsed = time.monotonic() - started
        self.assertIn("waiting for the node to load wallet 'l6' after createwallet timed out", str(caught.exception))
        self.assertIn("listwallets shows []", str(caught.exception))
        # The client timeout and the wait share the 1.5 s, plus at most one poll interval.
        self.assertLess(elapsed, 3.0)
        self.assertEqual(self.qbitd.calls.count("createwallet"), 1)

    def test_a_createwallet_error_fails_at_once_without_polling(self) -> None:
        self.qbitd.createwallet_error = {"code": -4, "message": "Wallet file verification failed."}
        self.qbitd.release.set()
        with self.assertRaisesRegex(lane.LaneFailure, "qbit RPC createwallet failed: .*verification"):
            lane.create_wallet(lane.Rpc(), "l6", seconds=5)
        self.assertNotIn("listwallets", self.qbitd.calls)

    def test_an_rpc_timeout_names_the_method(self) -> None:
        with self.assertRaisesRegex(lane.RpcTimeout, r"^qbit RPC createwallet timed out after 0\.3s$"):
            lane.Rpc().call("createwallet", ["l6"])


class Summary(unittest.TestCase):
    def report(self, **fields) -> dict:
        return {"checks": {}, "phases": {"total": 44.1}, **fields}

    def summary(self, report: dict) -> str:
        with mock.patch("sys.stdout", io.StringIO()) as out, mock.patch.dict("os.environ", {}, clear=True):
            lane.write_summary(report)
        return out.getvalue()

    def test_an_error_names_the_phase_it_stopped(self) -> None:
        text = self.summary(self.report(error="RpcTimeout: qbit RPC listwallets timed out after 30s",
                                        failed_phase="node", failed_phase_seconds=36.2))
        self.assertIn("Error in the `node` phase after 36.2s: `RpcTimeout: qbit RPC listwallets timed out", text)
        self.assertIn("| `ha-frontends-ready` | not reached |", text)

    def test_an_error_before_any_phase_has_no_phase(self) -> None:
        self.assertIn("Error: `OSError: x`", self.summary(self.report(error="OSError: x", failed_phase=None)))

    def test_execute_records_the_phase_that_failed(self) -> None:
        with tempfile.TemporaryDirectory() as work:
            (Path(work) / "lane.env").write_text("", encoding="utf-8")
            driver = lane.Lane(work=Path(work), out=Path(work), project="p", load_seconds=1,
                               cpuminer_threads=1, cpuminer_diff_multiplier=1.0,
                               prism_miner_hashes_per_second=1)

            def start() -> dict:
                driver.begin("node")
                raise lane.RpcTimeout("qbit RPC createwallet timed out after 30s")

            with mock.patch.object(driver, "start", start), mock.patch.object(driver, "collect"), \
                    mock.patch.object(driver, "down"), mock.patch("sys.stdout", io.StringIO()), \
                    mock.patch.dict("os.environ", {}, clear=True):
                self.assertFalse(driver.execute())
            report = json.loads((Path(work) / "l6-report.json").read_text(encoding="utf-8"))
        self.assertEqual(report["failed_phase"], "node")
        self.assertEqual(report["error"], "RpcTimeout: qbit RPC createwallet timed out after 30s")
        self.assertIn("failed_phase_seconds", report)
        self.assertNotIn("node", report["phases"])


class Triggers(unittest.TestCase):
    def test_dockerfiles_compose_files_and_image_inputs_run_the_lane(self) -> None:
        for path in ("docker/qbit/Dockerfile", "lab/miner-sim/Dockerfile", "Dockerfile",
                     "compose.yaml", "compose.prism-ha.yaml", ".env.example",
                     "config/prism-postgres/replica-entrypoint.sh", "docker/qbit/qbit-entrypoint.sh",
                     "lab/real-miner/real_miner.py", "docker/real-miner/cpuminer-regtest.patch",
                     "scripts/prism_shipped_image_lane.py", ".github/workflows/prism-load-nightly.yml",
                     "config/upstream.env", "config/upstream.env.example"):
            with self.subTest(path=path):
                self.assertTrue(lane.watched([path]))

    def test_other_changes_do_not(self) -> None:
        for path in ("crates/qbit-prism-server/src/lib.rs", "docs/compose.yaml", "README.md",
                     "tests/test_prism_shipped_image_lane.py", "lab/prism-notes.md", ""):
            with self.subTest(path=path):
                self.assertFalse(lane.watched([path]))

    def test_one_watched_path_among_many_is_enough(self) -> None:
        self.assertTrue(lane.watched(["README.md\n", "compose.production.yaml\n"]))

    def test_the_changed_command_prints_the_verdict(self) -> None:
        import subprocess
        script = ROOT / "scripts" / "prism_shipped_image_lane.py"
        for stdin, verdict in (("README.md\nlab/prism/Dockerfile\n", "true"), ("README.md\n", "false")):
            result = subprocess.run([sys.executable, str(script), "changed"], input=stdin,
                                    text=True, capture_output=True, check=True)
            self.assertEqual(result.stdout.strip(), verdict)

    def test_the_workflow_gates_the_lane_on_the_changes_job(self) -> None:
        workflow = (ROOT / ".github/workflows/prism-load-nightly.yml").read_text(encoding="utf-8")
        self.assertIn("python3 scripts/prism_shipped_image_lane.py changed", workflow)
        self.assertIn("needs.changes.outputs.images == 'true'", workflow)
        self.assertIn("github.base_ref == '3.x.x'", workflow)
        # A change list at the API's cap may be truncated, so it runs the lane.
        self.assertIn("cap=300", workflow)
        self.assertIn("cap=3000", workflow)
        self.assertIn("(( count >= cap ))", workflow)
        # The load nightly's guard must not start the preset matrix on a push.
        self.assertIn("github.event_name != 'push'", workflow)


if __name__ == "__main__":
    unittest.main()

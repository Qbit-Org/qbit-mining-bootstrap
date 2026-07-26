#!/usr/bin/env python3
"""Live regression and load tests for the ckpool rejects observability patch.

These tests drive a real patched ckpool binary against tests/fake_qbit_rpc.py
and a raw Stratum client, covering every reject reason bucket, both
block-candidate outcomes, the atomic 60-second rejects.status writer, and
exact tally conservation under concurrent submission load.

The suite needs a compiled binary and is skipped unless
QBIT_CKPOOL_REJECTS_BIN points at one. Build it with:

    make test-ckpool-rejects-observability

which stages the pinned upstream ckpool, applies the qbit patches, builds,
and re-runs this module with the gate set.
"""

from __future__ import annotations

import hashlib
import json
import math
import multiprocessing
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "tests"))

from stratum_client import StratumClient, StratumTimeout  # noqa: E402

FAKE_QBIT_RPC = ROOT_DIR / "tests" / "fake_qbit_rpc.py"
CKPOOL_BIN_ENV = "QBIT_CKPOOL_REJECTS_BIN"
CKPOOL_BIN = os.environ.get(CKPOOL_BIN_ENV, "")

# Assigned share difficulty pinned via mindiff == startdiff == maxdiff. A
# dyadic value keeps count * diff sums exactly representable.
SHARE_DIFF = 2.0**-20
# Network target advertised by the fake node: bits 1e00ffff is exactly 256x
# easier than diff-1, so ckpool's network_diff lands on the regtest floor
# value 1/256 and block candidates stay minable from pure Python.
NETWORK_BITS = "1e00ffff"
NETWORK_TARGET = "000000ffff" + "0" * 54
TRUEDIFFONE = 0xFFFF << 208
# Integer hash ceilings mirroring ckpool's double math with safety margins so
# borderline rounding can never flip a classification.
SHARE_HASH_CEILING = int(TRUEDIFFONE / SHARE_DIFF) * 99 // 100
BLOCK_TARGET = 0xFFFF << 216
BLOCK_HASH_CEILING = BLOCK_TARGET * 99 // 100
NOT_BLOCK_FLOOR = BLOCK_TARGET * 102 // 100

USER_ADDRESS = "qbrt1staticqbitaddress"
REASONS = (
    "accepted",
    "above_target",
    "stale",
    "duplicate",
    "invalid_job",
    "invalid_ntime",
    "invalid_version",
    "malformed",
)
BLOCK_OUTCOMES = ("block_accepted", "block_rejected")
ALL_BUCKETS = REASONS + BLOCK_OUTCOMES


def dsha(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def flip_words(data: bytes) -> bytes:
    """Byte-swap each 32-bit word, mirroring ckpool's flip_32/flip_80."""
    return b"".join(data[i : i + 4][::-1] for i in range(0, len(data), 4))


@dataclass
class MiningJob:
    job_id: str
    prevhash: str
    coinb1: str
    coinb2: str
    merkles: list[str]
    bbversion: str
    nbit: str
    ntime: str
    clean: bool

    @classmethod
    def from_notify(cls, message: dict[str, object]) -> "MiningJob":
        params = message["params"]
        assert isinstance(params, list) and len(params) >= 9
        return cls(
            job_id=str(params[0]),
            prevhash=str(params[1]),
            coinb1=str(params[2]),
            coinb2=str(params[3]),
            merkles=[str(entry) for entry in params[4]],
            bbversion=str(params[5]),
            nbit=str(params[6]),
            ntime=str(params[7]),
            clean=bool(params[8]),
        )


def flipped_header_base(job: MiningJob, enonce1: str, nonce2: str, ntime_hex: str) -> bytes:
    """Reproduce ckpool's submission_diff hashing input with a zero nonce.

    ckpool builds an 80-byte header from the notify hex fields, inserts the
    word-flipped merkle root, then hashes flip_80(header). The returned
    buffer is that flipped header; the nonce occupies bytes 76:80 as the
    little-endian nonce value.
    """
    coinbase = bytes.fromhex(job.coinb1 + enonce1 + nonce2 + job.coinb2)
    root = dsha(coinbase)
    for branch in job.merkles:
        root = dsha(root + bytes.fromhex(branch))

    header = bytearray(80)
    header[0:4] = bytes.fromhex(job.bbversion)
    header[4:36] = bytes.fromhex(job.prevhash)
    header[36:68] = flip_words(root)
    header[68:72] = struct.pack(">I", int(ntime_hex, 16))
    header[72:76] = bytes.fromhex(job.nbit)
    return flip_words(bytes(header))


def share_hash_int(base: bytes, nonce: int) -> int:
    buf = bytearray(base)
    buf[76:80] = struct.pack("<I", nonce)
    return int.from_bytes(dsha(bytes(buf)), "little")


def find_nonce(base: bytes, ceiling: int, floor: int = 0, start: int = 0) -> str:
    """Find a nonce whose ckpool share hash lies in (floor, ceiling]."""
    nonce = start
    while True:
        value = share_hash_int(base, nonce)
        if value <= ceiling and value > floor:
            return f"{nonce:08x}"
        nonce += 1


def find_failing_nonce(base: bytes, start: int = 0) -> str:
    """Find a nonce guaranteed to be above the share target (and thus also
    never a block candidate), keeping reject submissions deterministic."""
    nonce = start
    while True:
        if share_hash_int(base, nonce) > SHARE_HASH_CEILING * 2:
            return f"{nonce:08x}"
        nonce += 1


def _scan_block_nonce(args: tuple[bytes, int, int]) -> str | None:
    base, start, count = args
    for nonce in range(start, start + count):
        if share_hash_int(base, nonce) <= BLOCK_HASH_CEILING:
            return f"{nonce:08x}"
    return None


def find_block_nonce(base: bytes, timeout: float = 300.0) -> str:
    """Mine a block-candidate nonce across all cores; ~16M expected hashes."""
    chunk = 1 << 18
    deadline = time.monotonic() + timeout
    processes = max(2, multiprocessing.cpu_count())
    offsets = ((base, start, chunk) for start in range(0, 1 << 32, chunk))
    with multiprocessing.Pool(processes) as pool:
        for result in pool.imap_unordered(_scan_block_nonce, offsets):
            if result is not None:
                pool.terminate()
                return result
            if time.monotonic() > deadline:
                pool.terminate()
                raise TimeoutError("no block-candidate nonce found in time")
    raise TimeoutError("nonce space exhausted without a block candidate")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def free_low_port() -> int:
    """Stratum port at or below 4000: this ckpool tree treats higher ports
    as highdiff solo ports, while the deployed pool listens on 3333."""
    for port in range(2000, 4001):
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("no free low port available")


class MinerClient:
    """Stratum client wrapper tracking jobs and share responses."""

    def __init__(self, port: int, workername: str) -> None:
        self.client = StratumClient("127.0.0.1", port, connect_timeout=10.0, read_timeout=0.2)
        self.workername = workername
        self.enonce1 = ""
        self.enonce2_size = 8
        self.jobs: list[MiningJob] = []
        self.responses: dict[int, dict[str, object]] = {}
        self.difficulty: float | None = None

    def close(self) -> None:
        self.client.close()

    def _pump(self, deadline: float) -> None:
        try:
            message = self.client.recv_message(deadline)
        except StratumTimeout:
            return
        if message.get("method") == "mining.notify":
            self.jobs.append(MiningJob.from_notify(message))
        elif message.get("method") == "mining.set_difficulty":
            params = message.get("params")
            if isinstance(params, list) and params:
                self.difficulty = float(params[0])
        elif "id" in message and message.get("id") is not None:
            self.responses[int(message["id"])] = message

    def wait_response(self, request_id: int, timeout: float = 10.0) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if request_id in self.responses:
                return self.responses.pop(request_id)
            self._pump(time.monotonic() + 0.2)
        raise AssertionError(f"no response for request {request_id} within {timeout}s")

    def drain(self, duration: float) -> None:
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            self._pump(deadline)

    def handshake(self) -> None:
        sub_id = self.client.send_request("mining.subscribe", ["rejects-observability/1"])
        result = self.wait_response(sub_id)["result"]
        assert isinstance(result, list) and len(result) >= 3, f"bad subscribe result: {result}"
        self.enonce1 = str(result[1])
        self.enonce2_size = int(result[2])
        # btcsolo generates a personalised job during authorisation and may
        # queue its notify before the auth ack, so count jobs from here.
        jobs_seen = len(self.jobs)
        auth_id = self.client.send_request("mining.authorize", [self.workername, "x"])
        response = self.wait_response(auth_id)
        assert response.get("result") is True, f"authorize failed: {response}"
        deadline = time.monotonic() + 10.0
        while len(self.jobs) <= jobs_seen and time.monotonic() < deadline:
            self._pump(time.monotonic() + 0.2)
        assert len(self.jobs) > jobs_seen, "no personalised job arrived with authorize"
        self.drain(0.5)

    @property
    def job(self) -> MiningJob:
        return self.jobs[-1]

    def nonce2(self, index: int) -> str:
        return f"{index:0{self.enonce2_size * 2}x}"

    def submit(self, params: list[object]) -> int:
        return self.client.send_request("mining.submit", params)

    def submit_share(
        self,
        job: MiningJob,
        nonce2: str,
        nonce: str,
        *,
        ntime: str | None = None,
        job_id: str | None = None,
        version: str | None = None,
    ) -> int:
        params: list[object] = [
            self.workername,
            job_id if job_id is not None else job.job_id,
            nonce2,
            ntime if ntime is not None else job.ntime,
            nonce,
        ]
        if version is not None:
            params.append(version)
        return self.submit(params)

    def wait_for_clean_job(self, prevhash: str, timeout: float = 30.0) -> MiningJob:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for job in reversed(self.jobs):
                if job.prevhash != prevhash:
                    return job
            self._pump(time.monotonic() + 0.2)
        raise AssertionError("no post-block job arrived in time")


class CkpoolHarness:
    """fake qbit RPC + patched ckpool in btcsolo mode, as deployed."""

    def __init__(self, tmpdir: Path, *, submitblock_results: str = "") -> None:
        self.tmpdir = tmpdir
        self.rpc_port = free_port()
        self.stratum_port = free_low_port()
        self.submitblock_results = submitblock_results
        self.rpc_process: subprocess.Popen[str] | None = None
        self.ckpool_process: subprocess.Popen[str] | None = None
        self.logdir = tmpdir / "logs"
        self.status_path = self.logdir / "pool" / "rejects.status"

    def __enter__(self) -> "CkpoolHarness":
        rpc_args = [
            sys.executable,
            str(FAKE_QBIT_RPC),
            "--host",
            "127.0.0.1",
            "--port",
            str(self.rpc_port),
            "--log-requests",
            "0",
            "--bits",
            NETWORK_BITS,
            "--target",
            NETWORK_TARGET,
        ]
        if self.submitblock_results:
            rpc_args += ["--submitblock-results", self.submitblock_results]
        self.rpc_process = subprocess.Popen(
            rpc_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        assert self.rpc_process.stdout is not None
        line = self.rpc_process.stdout.readline()
        if "fake qbit RPC listening" not in line:
            raise RuntimeError(f"fake RPC did not start: {line}")

        config = {
            "btcd": [
                {
                    "url": f"127.0.0.1:{self.rpc_port}",
                    "auth": "qbitrpc",
                    "pass": "test",
                    "notify": False,
                }
            ],
            "btcaddress": USER_ADDRESS,
            "btcsig": "/rejects-observability/",
            "blockpoll": 100,
            "update_interval": 30,
            "version_mask": "1fffe000",
            "serverurl": [f"127.0.0.1:{self.stratum_port}"],
            "mindiff": SHARE_DIFF,
            "startdiff": SHARE_DIFF,
            "maxdiff": SHARE_DIFF,
            "logdir": str(self.logdir),
        }
        config_path = self.tmpdir / "ckpool.conf"
        config_path.write_text(json.dumps(config, indent=1), encoding="utf-8")
        sockdir = self.tmpdir / "sock"
        sockdir.mkdir()
        self.ckpool_process = subprocess.Popen(
            [
                CKPOOL_BIN,
                "-B",
                "-k",
                "-c",
                str(config_path),
                "-n",
                "rejectslab",
                "--sockdir",
                str(sockdir),
                "--loglevel",
                "6",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self._wait_for_stratum()
        return self

    def _wait_for_stratum(self, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.ckpool_process is not None and self.ckpool_process.poll() is not None:
                raise RuntimeError(f"ckpool exited early:\n{self.ckpool_output()}")
            try:
                with socket.create_connection(("127.0.0.1", self.stratum_port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f"ckpool stratum port never opened:\n{self.ckpool_output()}")

    def ckpool_output(self) -> str:
        if self.ckpool_process is None or self.ckpool_process.stdout is None:
            return "<no process>"
        try:
            return self.ckpool_process.communicate(timeout=2)[0] or "<no output>"
        except subprocess.TimeoutExpired:
            return "<ckpool still running>"

    def control(self, payload: dict[str, object] | None = None) -> dict[str, object]:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.rpc_port}/control",
            data=json.dumps(payload or {}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    def read_status(self) -> dict[str, object] | None:
        try:
            return json.loads(self.status_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None

    def wait_for_status(self, predicate, timeout: float = 150.0) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        last: dict[str, object] | None = None
        while time.monotonic() < deadline:
            if self.ckpool_process is not None and self.ckpool_process.poll() is not None:
                raise AssertionError(f"ckpool died while waiting:\n{self.ckpool_output()}")
            status = self.read_status()
            if status is not None:
                # Every observed document must be complete and well formed;
                # the atomic rename contract means readers never see partial
                # JSON even while submissions are in flight.
                last = status
                if predicate(status):
                    return status
            time.sleep(0.25)
        raise AssertionError(f"rejects.status never matched; last:\n{json.dumps(last, indent=1)}")

    def __exit__(self, *_exc: object) -> None:
        for process in (self.ckpool_process, self.rpc_process):
            if process is None:
                continue
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
            if process.stdout is not None:
                process.stdout.close()


def bucket(status_entry: dict[str, object], name: str) -> tuple[int, float]:
    entry = status_entry[name]
    assert isinstance(entry, dict)
    return int(entry["count"]), float(entry["diff"])


def worker_entry(status: dict[str, object], workername: str) -> dict[str, object]:
    workers = status["workers"]
    assert isinstance(workers, list)
    for entry in workers:
        if entry.get("workername") == workername:
            return entry
    raise AssertionError(f"worker {workername} missing from status: {workers}")


def assert_snapshot_consistent(
    test: unittest.TestCase, status: dict[str, object], *, expect_equal: bool = False
) -> None:
    """Pool and worker tallies are copied under one lock hold, so within any
    single document per-bucket worker sums can never exceed the pool totals;
    once all submitting workers are listed and traffic has settled, they are
    equal. Checked on every observed document, including mid-load reads."""
    pool = status["pool"]
    workers = status["workers"]
    assert isinstance(pool, dict) and isinstance(workers, list)
    for name in ALL_BUCKETS:
        pool_count, pool_diff = bucket(pool, name)
        worker_counts = sum(bucket(entry, name)[0] for entry in workers)
        worker_diffs = sum(bucket(entry, name)[1] for entry in workers)
        if expect_equal:
            test.assertEqual(worker_counts, pool_count, f"{name} counts diverge: {status}")
            test.assertTrue(
                math.isclose(worker_diffs, pool_diff, rel_tol=1e-9, abs_tol=1e-15),
                f"{name} diffs diverge: {worker_diffs} != {pool_diff}",
            )
        else:
            test.assertLessEqual(worker_counts, pool_count, f"{name} counts exceed pool: {status}")
            test.assertLessEqual(
                worker_diffs,
                pool_diff * (1 + 1e-9) + 1e-15,
                f"{name} diffs exceed pool: {worker_diffs} > {pool_diff}",
            )


def assert_share_response(
    test: unittest.TestCase, response: dict[str, object], *, accepted: bool, error: str | None
) -> None:
    """Share responses are part of upstream behavior and must not change."""
    if accepted:
        test.assertIs(response.get("result"), True, response)
        test.assertIsNone(response.get("error"), response)
    else:
        test.assertIsNot(response.get("result"), True, response)
        test.assertEqual(response.get("error"), error, response)


@unittest.skipUnless(
    CKPOOL_BIN,
    f"set {CKPOOL_BIN_ENV} to a patched ckpool binary "
    "(make test-ckpool-rejects-observability builds one)",
)
class CkpoolRejectsObservabilityTests(unittest.TestCase):
    maxDiff = None

    def expect_counts(self, entry: dict[str, object], expected: dict[str, int]) -> None:
        for reason in ALL_BUCKETS:
            count, diff = bucket(entry, reason)
            self.assertEqual(count, expected.get(reason, 0), f"{reason} count in {entry}")
            if reason in REASONS:
                self.assertTrue(
                    math.isclose(diff, count * SHARE_DIFF, rel_tol=1e-9, abs_tol=1e-18),
                    f"{reason} diff {diff} != {count} * {SHARE_DIFF}",
                )

    def test_reason_taxonomy_block_outcomes_and_atomic_writer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with CkpoolHarness(
                Path(tmp), submitblock_results="rejected,null"
            ) as harness:
                first_status = harness.wait_for_status(lambda status: True, timeout=30.0)
                self.assertEqual(first_status["version"], 1)
                self.assertEqual(first_status["poolinstance"], "rejectslab")
                self.assertEqual(first_status["interval"], 60)
                first_update = int(first_status["lastupdate"])

                # A worker that authorises but never submits must not grow
                # the export: rejects.status lists submitting workers only.
                idle = MinerClient(harness.stratum_port, f"{USER_ADDRESS}.idle0")
                idle.handshake()
                idle.close()

                miner = MinerClient(harness.stratum_port, f"{USER_ADDRESS}.rig0")
                try:
                    miner.handshake()
                    self.assertIsNotNone(miner.difficulty)
                    self.assertTrue(
                        math.isclose(miner.difficulty, SHARE_DIFF, rel_tol=1e-12),
                        f"unexpected assigned difficulty {miner.difficulty}",
                    )
                    job = miner.job
                    base = flipped_header_base(job, miner.enonce1, miner.nonce2(0), job.ntime)

                    # malformed: too few params.
                    response = miner.wait_response(
                        miner.submit([miner.workername, job.job_id, miner.nonce2(0), job.ntime])
                    )
                    assert_share_response(self, response, accepted=False, error="Invalid array size")

                    # invalid_version: version bits outside the negotiated
                    # mask (nothing was negotiated, so any bits qualify).
                    response = miner.wait_response(
                        miner.submit_share(
                            job,
                            miner.nonce2(0),
                            find_failing_nonce(base),
                            version="1fffe000",
                        )
                    )
                    assert_share_response(self, response, accepted=False, error="Invalid version mask")

                    # invalid_job: unknown job id.
                    response = miner.wait_response(
                        miner.submit_share(
                            job, miner.nonce2(0), find_failing_nonce(base), job_id="deadbeef1234"
                        )
                    )
                    assert_share_response(self, response, accepted=False, error="Invalid JobID")

                    # invalid_ntime: rolled far beyond the allowed window.
                    bad_ntime = f"{int(job.ntime, 16) + 0x10000:08x}"
                    bad_ntime_base = flipped_header_base(
                        job, miner.enonce1, miner.nonce2(0), bad_ntime
                    )
                    response = miner.wait_response(
                        miner.submit_share(
                            job,
                            miner.nonce2(0),
                            find_failing_nonce(bad_ntime_base),
                            ntime=bad_ntime,
                        )
                    )
                    assert_share_response(self, response, accepted=False, error="Ntime out of range")

                    # above_target: verified-failing nonces.
                    for attempt in range(3):
                        response = miner.wait_response(
                            miner.submit_share(
                                job, miner.nonce2(0), find_failing_nonce(base, start=attempt * 7 + 1000)
                            )
                        )
                        assert_share_response(self, response, accepted=False, error="Above target")

                    # accepted: a real share meeting the assigned difficulty
                    # but explicitly not a block candidate.
                    share_nonce = find_nonce(base, SHARE_HASH_CEILING, floor=NOT_BLOCK_FLOOR)
                    response = miner.wait_response(
                        miner.submit_share(job, miner.nonce2(0), share_nonce)
                    )
                    assert_share_response(self, response, accepted=True, error=None)

                    # duplicate: byte-identical resubmission.
                    response = miner.wait_response(
                        miner.submit_share(job, miner.nonce2(0), share_nonce)
                    )
                    assert_share_response(self, response, accepted=False, error="Duplicate")

                    # block candidate #1: fake node rejects the submitblock.
                    block_base = flipped_header_base(
                        job, miner.enonce1, miner.nonce2(1), job.ntime
                    )
                    block_nonce = find_block_nonce(block_base)
                    response = miner.wait_response(
                        miner.submit_share(job, miner.nonce2(1), block_nonce), timeout=30.0
                    )
                    assert_share_response(self, response, accepted=True, error=None)

                    # block candidate #2: identical resubmission is a
                    # duplicate share, but block submission still happens and
                    # the fake node accepts it this time, advancing the chain.
                    response = miner.wait_response(
                        miner.submit_share(job, miner.nonce2(1), block_nonce), timeout=30.0
                    )
                    assert_share_response(self, response, accepted=False, error="Duplicate")

                    # stale: the accepted block advanced the template; shares
                    # against the old job are stale once the clean job lands.
                    miner.wait_for_clean_job(job.prevhash)
                    response = miner.wait_response(
                        miner.submit_share(job, miner.nonce2(0), find_failing_nonce(base, start=5000))
                    )
                    assert_share_response(self, response, accepted=False, error="Stale")
                    # A duplicate of an old-job share is also just stale now:
                    # stale classification precedes duplicate detection.
                    response = miner.wait_response(
                        miner.submit_share(job, miner.nonce2(0), share_nonce)
                    )
                    assert_share_response(self, response, accepted=False, error="Stale")

                    expected = {
                        "accepted": 2,
                        "above_target": 3,
                        "stale": 2,
                        "duplicate": 2,
                        "invalid_job": 1,
                        "invalid_ntime": 1,
                        "invalid_version": 1,
                        "malformed": 1,
                        "block_accepted": 1,
                        "block_rejected": 1,
                    }

                    def totals_match(status: dict[str, object]) -> bool:
                        pool = status["pool"]
                        return all(
                            bucket(pool, reason)[0] == count for reason, count in expected.items()
                        )

                    status = harness.wait_for_status(totals_match)
                finally:
                    miner.close()

                # The block submissions must be byte-identical and exactly two:
                # observability never alters what reaches the node.
                control = harness.control()
                params = control["submitblock_params"]
                self.assertEqual(len(params), 2, control)
                self.assertEqual(params[0], params[1])
                self.assertEqual(int(control["height"]), 2)

                pool = status["pool"]
                self.expect_counts(pool, expected)
                for outcome in BLOCK_OUTCOMES:
                    _count, diff = bucket(pool, outcome)
                    # Block-candidate difficulty is the actual share diff,
                    # at least the network difficulty the candidate met.
                    self.assertGreaterEqual(diff, 1 / 256 * 0.99)

                worker = worker_entry(status, f"{USER_ADDRESS}.rig0")
                self.expect_counts(worker, expected)
                assert_snapshot_consistent(self, status, expect_equal=True)

                # The authorised-but-idle worker is excluded from the export.
                listed = [entry["workername"] for entry in status["workers"]]
                self.assertEqual(listed, [f"{USER_ADDRESS}.rig0"])

                # 60s cadence: the matching write is a later write of the
                # same file, landing near a 60-second boundary.
                last_update = int(status["lastupdate"])
                delta = last_update - first_update
                self.assertGreaterEqual(delta, 55)
                self.assertLessEqual(delta, 185)
                cycles = max(1, round(delta / 60))
                self.assertLessEqual(abs(delta - cycles * 60), 8, f"delta {delta}")

                # Legacy pool.status keeps its original three-document shape.
                pool_status_lines = (
                    (harness.logdir / "pool" / "pool.status")
                    .read_text(encoding="utf-8")
                    .strip()
                    .splitlines()
                )
                self.assertEqual(len(pool_status_lines), 3)
                legacy = json.loads(pool_status_lines[0])
                self.assertIn("runtime", legacy)
                self.assertNotIn("accepted", legacy)

    def test_concurrent_load_conservation_and_atomicity(self) -> None:
        clients = 4
        plan = {
            "accepted": 40,
            "above_target": 400,
            "invalid_job": 120,
            "invalid_ntime": 120,
            "invalid_version": 80,
            "malformed": 80,
        }
        per_client_total = sum(plan.values())

        with tempfile.TemporaryDirectory() as tmp:
            with CkpoolHarness(Path(tmp)) as harness:
                harness.wait_for_status(lambda status: True, timeout=30.0)

                errors: list[str] = []
                started = threading.Barrier(clients + 1)
                elapsed: dict[str, float] = {}

                def run_client(index: int) -> None:
                    workername = f"{USER_ADDRESS}.load{index}"
                    miner = MinerClient(harness.stratum_port, workername)
                    try:
                        miner.handshake()
                        job = miner.job
                        base_by_n2: dict[str, bytes] = {}

                        def base_for(nonce2: str) -> bytes:
                            if nonce2 not in base_by_n2:
                                base_by_n2[nonce2] = flipped_header_base(
                                    job, miner.enonce1, nonce2, job.ntime
                                )
                            return base_by_n2[nonce2]

                        started.wait(timeout=60)
                        begin = time.monotonic()
                        pending: dict[int, str] = {}

                        def flush() -> None:
                            while pending:
                                ready = [rid for rid in pending if rid in miner.responses]
                                for rid in ready:
                                    expected_error = pending.pop(rid)
                                    response = miner.responses.pop(rid)
                                    if expected_error == "":
                                        if response.get("result") is not True:
                                            errors.append(f"{workername}: {response}")
                                    elif response.get("error") != expected_error:
                                        errors.append(
                                            f"{workername}: expected {expected_error}: {response}"
                                        )
                                if pending:
                                    miner._pump(time.monotonic() + 0.2)

                        # Interleave reasons so rejects never run 60s
                        # uninterrupted and acceptance resets the reject
                        # window, mirroring mixed real traffic.
                        schedule: list[tuple[str, int]] = []
                        for reason, count in plan.items():
                            schedule.extend((reason, i) for i in range(count))
                        schedule.sort(key=lambda item: (item[1], item[0]))

                        nonce2_index = 0
                        for reason, i in schedule:
                            if reason == "accepted":
                                nonce2 = miner.nonce2(nonce2_index)
                                nonce2_index += 1
                                nonce = find_nonce(
                                    base_for(nonce2), SHARE_HASH_CEILING, floor=NOT_BLOCK_FLOOR
                                )
                                rid = miner.submit_share(job, nonce2, nonce)
                                pending[rid] = ""
                            elif reason == "above_target":
                                nonce = find_failing_nonce(base_for(miner.nonce2(0)), start=i * 3)
                                rid = miner.submit_share(job, miner.nonce2(0), nonce)
                                pending[rid] = "Above target"
                            elif reason == "invalid_job":
                                rid = miner.submit_share(
                                    job,
                                    miner.nonce2(0),
                                    find_failing_nonce(base_for(miner.nonce2(0))),
                                    job_id=f"deadbeef{i:04x}",
                                )
                                pending[rid] = "Invalid JobID"
                            elif reason == "invalid_ntime":
                                bad_ntime = f"{int(job.ntime, 16) + 0x10000 + i:08x}"
                                rid = miner.submit_share(
                                    job,
                                    miner.nonce2(0),
                                    find_failing_nonce(
                                        flipped_header_base(
                                            job, miner.enonce1, miner.nonce2(0), bad_ntime
                                        )
                                    ),
                                    ntime=bad_ntime,
                                )
                                pending[rid] = "Ntime out of range"
                            elif reason == "invalid_version":
                                rid = miner.submit_share(
                                    job,
                                    miner.nonce2(0),
                                    find_failing_nonce(base_for(miner.nonce2(0))),
                                    version="1fffe000",
                                )
                                pending[rid] = "Invalid version mask"
                            else:  # malformed
                                rid = miner.submit(
                                    [workername, job.job_id, miner.nonce2(0), job.ntime]
                                )
                                pending[rid] = "Invalid array size"
                            if len(pending) >= 64:
                                flush()
                        flush()
                        elapsed[workername] = time.monotonic() - begin
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"{workername}: {exc!r}")
                    finally:
                        miner.close()

                threads = [
                    threading.Thread(target=run_client, args=(index,), daemon=True)
                    for index in range(clients)
                ]
                for thread in threads:
                    thread.start()
                started.wait(timeout=60)
                submit_begin = time.monotonic()

                # Poll the status file while the storm runs: every read must
                # parse as complete JSON thanks to the tmp+rename contract,
                # and every document must be an internally consistent
                # snapshot even with submissions in flight.
                polls = 0
                while any(thread.is_alive() for thread in threads):
                    status = harness.read_status()
                    if status is not None:
                        self.assertIn("pool", status)
                        assert_snapshot_consistent(self, status)
                        polls += 1
                    time.sleep(0.05)
                for thread in threads:
                    thread.join(timeout=120)
                self.assertEqual(errors, [])
                self.assertGreater(polls, 0)
                submit_elapsed = time.monotonic() - submit_begin

                # Low-overhead sanity: the full mixed workload including all
                # acks stays far below the reject-flood thresholds. Generous
                # bound for slow shared CI runners.
                self.assertLess(submit_elapsed, 55.0, f"per-client elapsed: {elapsed}")

                def totals_match(status: dict[str, object]) -> bool:
                    pool = status["pool"]
                    return (
                        bucket(pool, "accepted")[0] == clients * plan["accepted"]
                        and bucket(pool, "above_target")[0] == clients * plan["above_target"]
                        and bucket(pool, "malformed")[0] == clients * plan["malformed"]
                    )

                status = harness.wait_for_status(totals_match)
                pool = status["pool"]

                # Exact conservation across four concurrent submitters:
                # nothing lost, nothing double counted, no stray buckets,
                # and worker rows summing exactly to the pool totals.
                pool_expected = {reason: clients * count for reason, count in plan.items()}
                self.expect_counts(pool, pool_expected)
                total = sum(bucket(pool, reason)[0] for reason in REASONS)
                self.assertEqual(total, clients * per_client_total)
                assert_snapshot_consistent(self, status, expect_equal=True)

                workers = status["workers"]
                self.assertIsInstance(workers, list)
                self.assertEqual(len(workers), clients)
                for index in range(clients):
                    entry = worker_entry(status, f"{USER_ADDRESS}.load{index}")
                    self.expect_counts(entry, plan)

                # Bounded output: per-worker aggregation only, no per-share
                # or per-client growth.
                self.assertLess(harness.status_path.stat().st_size, 64 * 1024)
                self.assertFalse(
                    (harness.logdir / "pool" / "rejects.status.tmp").exists(),
                    "temporary status file left behind after settling",
                )

                # No block submissions may happen under pure share load.
                self.assertEqual(int(harness.control()["submits"]), 0)


if __name__ == "__main__":
    unittest.main()

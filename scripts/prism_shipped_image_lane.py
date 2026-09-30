#!/usr/bin/env python3
"""#487 lane L6 (#544): the shipped images under the Compose `prism` profile
with the HA overlay, against a real regtest node.

The lane builds the images the way an operator does (`docker compose build`
from compose.yaml), brings up `compose.yaml` + `compose.prism-ha.yaml` with
the `prism` profile (qbitd, the PostgreSQL primary and its replica, two
mining frontends and the public API), and holds it to these checks, which
test/e2e-scenarios.toml cites as the lane's evidence:

- `ha-frontends-ready`: both frontends answer `/healthz` with `ok: true` and
  distinct instance IDs, and each one's `self-check` observes both as live.
- `cross-frontend-resume`: #281 criterion 1 on the images. Work issued by
  frontend 1 is accepted once on frontend 2 over a real socket; another
  worker cannot submit it, and a replay on either frontend is rejected.
- `cpuminer-found-block`: cpuminer-opt (`docker/real-miner`), a Stratum client
  we did not write, has shares accepted on frontend 1 and solves a block the
  node keeps on its active chain.
- `prism-miner-found-block`: the same for `qbit-prism-miner` on frontend 2.
- `found-block-audit-verifies`: each client's latest found block's audit
  bundle, served identically by both frontends, verifies with
  `qbit-prism-audit-verify` against the node's coinbase and the lane's
  ledger writer key.
- `ledger-reconciles`: every share a client saw acknowledged is in the
  ledger (and nothing beyond its unanswered submissions), each credited
  header is credited once, both frontends wrote shares, and both frontends'
  `/audit/latest` report the ledger's accepted-share count.
- `public-api-lists-found-blocks`: the public API, reading the replica,
  lists every verified block.

Usage (the workflow runs the three in order; `run` tears the stack down):

    python3 scripts/prism_shipped_image_lane.py prepare --work DIR
    python3 scripts/prism_shipped_image_lane.py build --work DIR
    python3 scripts/prism_shipped_image_lane.py run --work DIR --out OUT

`changed` reads changed file names on stdin and prints `true` when one is a
path the lane watches (WATCHED_PATHS), `false` otherwise; the workflow's
`changes` job uses it to decide whether a PR or push runs the lane.

`prepare` stages the pinned qbit source (scripts/prepare-qbit-source.sh) and
writes DIR/lane.env: fresh signing seeds for this run, the ledger writer key
derived from them, the public Stratum URL the public API requires, and the
frontends' instance IDs. Nothing else overrides the checked-in lab defaults
in the upstream pins (config/upstream.env, as the Makefile and
prepare-qbit-source.sh read it) and .env.example. Requires Docker Compose
2.24.4 or newer, openssl and a free host port set 3340-3344 and 18452.
"""

from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass, field
import fnmatch
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "prism-shipped-images"

# The lane's named checks. test/e2e-scenarios.toml cites them as the L6
# evidence and scripts/check_e2e_scenarios.py holds the two to each other.
CHECKS = (
    "ha-frontends-ready",
    "cross-frontend-resume",
    "cpuminer-found-block",
    "prism-miner-found-block",
    "found-block-audit-verifies",
    "ledger-reconciles",
    "public-api-lists-found-blocks",
)

# What a PR or push must touch to run the lane: every Dockerfile, the Compose
# files, and what the images the lane builds are built or configured from.
# fnmatch patterns, whose `*` also matches `/`.
WATCHED_PATHS = (
    "Dockerfile",
    "*/Dockerfile",
    "compose*.yaml",
    ".env.example",
    "config/upstream.env",
    "config/upstream.env.example",
    "config/qbit/*",
    "config/prism-postgres/*",
    "docker/qbit/*",
    "docker/real-miner/*",
    "lab/prism/*",
    "lab/real-miner/*",
    "scripts/prepare-qbit-source.sh",
    "scripts/prism_shipped_image_lane.py",
    ".github/workflows/prism-load-nightly.yml",
)

COMPOSE_FILES = ("compose.yaml", "compose.prism-ha.yaml")
# The upstream pins as the Makefile and scripts/prepare-qbit-source.sh pick
# them: config/upstream.env, or its example when a checkout has none. The
# images must build from the same pins as the qbit source they stage.
UPSTREAM_ENV_FILE = (
    "config/upstream.env" if (ROOT / "config/upstream.env").is_file() else "config/upstream.env.example"
)
ENV_FILES = (UPSTREAM_ENV_FILE, ".env.example")
INSTANCE_IDS = ("l6-frontend-1", "l6-frontend-2")
# compose.prism-ha.yaml's default host ports.
STRATUM_PORTS = (3340, 3343)
HEALTH_PORTS = (3341, 3344)
PUBLIC_API_PORT = 3342
QBIT_RPC_PORT = 18452
# The lab defaults in .env.example; the lane does not override them.
QBIT_RPC_USER = "qbitrpc"
QBIT_RPC_PASSWORD = "change-this"
POSTGRES_USER = "qbit"
POSTGRES_DB = "qbit"
COORDINATOR_IMAGE = "qbit-lab-prism-coordinator:local"
REAL_MINER_IMAGE = "qbit-lab-real-miner:local"
WALLET = "l6"
DIFF1_TARGET = 0xFFFF << 208
# An Ed25519 private key in PKCS#8 DER is this prefix and the 32-byte seed.
ED25519_PKCS8_PREFIX = bytes.fromhex("302e020100300506032b657004220420")

# cpuminer-opt prints its running totals after every share result, e.g.
# "12 Accepted 12 S0 R0 B3, 1.234 sec (5ms)" or "13 A13 S0 R0 BLOCK SOLVED 4, ...".
CPUMINER_RESULT = re.compile(
    r"\b(?:A|Accepted )(\d+) (?:S|Stale )(\d+) (?:R|Rejected )(\d+) (?:B|BLOCK SOLVED )(\d+),"
)
CPUMINER_SUBMIT = re.compile(r"\b\d+ Submitted Diff ")
# Client logs kept in the artifact, and lines echoed to the job log, per client.
CLIENT_LOG_BYTES = 32 * 1024 * 1024
CLIENT_ECHO_LINES = 400

# Addresses go into SQL literals, so hold them to the bech32 alphabet first.
ADDRESS = re.compile(r"^[a-z0-9]{8,120}$")


class LaneFailure(RuntimeError):
    pass


def log(message: str) -> None:
    print(f"{PREFIX}: {message}", flush=True)


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in tests/test_prism_shipped_image_lane.py).
# ---------------------------------------------------------------------------


def watched(paths) -> bool:
    """Whether any changed path is one the lane watches."""
    return any(
        fnmatch.fnmatchcase(path, pattern)
        for path in (line.strip() for line in paths) if path
        for pattern in WATCHED_PATHS
    )


def ed25519_public_key_hex(seed_hex: str) -> str:
    """The Ed25519 public key for a 32-byte seed, as qbit-pool-builder prints it."""
    seed = bytes.fromhex(seed_hex)
    if len(seed) != 32:
        raise ValueError("an Ed25519 seed is 32 bytes")
    der = subprocess.run(
        ["openssl", "pkey", "-inform", "DER", "-pubout", "-outform", "DER"],
        input=ED25519_PKCS8_PREFIX + seed,
        capture_output=True,
        check=True,
    ).stdout
    return der[-32:].hex()


def lane_env(qbit_src: Path, manifest_seed: str, attestation_seed: str, writer_key: str) -> str:
    lines = [
        "# Written by scripts/prism_shipped_image_lane.py prepare; fresh seeds per run.",
        f"QBIT_SRC_DIR={qbit_src}",
        f"PRISM_MANIFEST_SIGNING_SEED_HEX={manifest_seed}",
        f"PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX={attestation_seed}",
        f"PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX={writer_key}",
        f"PRISM_PUBLIC_STRATUM_URL=stratum+tcp://127.0.0.1:{STRATUM_PORTS[0]}",
        f"PRISM_HA_INSTANCE_ID_1={INSTANCE_IDS[0]}",
        f"PRISM_HA_INSTANCE_ID_2={INSTANCE_IDS[1]}",
    ]
    return "\n".join(lines) + "\n"


def read_env_file(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def compose_command(project: str, env_file: Path, *args: str, profiles: tuple[str, ...] = ("prism",)) -> list[str]:
    command = ["docker", "compose", "--project-name", project]
    for name in (*ENV_FILES, str(env_file)):
        command += ["--env-file", name]
    for name in COMPOSE_FILES:
        command += ["-f", name]
    for profile in profiles:
        command += ["--profile", profile]
    return command + list(args)


@dataclass
class ClientTally:
    """What one client saw: its submissions and the answers it got."""

    submitted: int = 0
    accepted: int = 0
    rejected: int = 0
    stale: int = 0
    blocks: int = 0

    @property
    def unanswered(self) -> int:
        return max(0, self.submitted - self.accepted - self.rejected - self.stale)


class CpuminerOutput:
    """cpuminer-opt's totals, read a line at a time (its output can be large)."""

    def __init__(self) -> None:
        self.tally = ClientTally()

    def feed(self, line: str) -> None:
        if CPUMINER_SUBMIT.search(line):
            self.tally.submitted += 1
            return
        result = CPUMINER_RESULT.search(line)
        if result:
            # Each result line carries the running totals.
            (self.tally.accepted, self.tally.stale, self.tally.rejected,
             self.tally.blocks) = (int(value) for value in result.groups())

    def result(self) -> ClientTally:
        return self.tally


class PrismMinerOutput:
    """qbit-prism-miner's JSON events: its summary has the totals."""

    def __init__(self) -> None:
        self.summary: dict | None = None
        self.blocks = 0

    def feed(self, line: str) -> None:
        line = line.strip()
        if not line.startswith("{"):
            return
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            return
        if event.get("event") == "summary":
            self.summary = event
        elif event.get("event") == "submit" and event.get("block_target_met") is True:
            self.blocks += 1

    def result(self) -> ClientTally:
        if self.summary is None:
            raise LaneFailure("qbit-prism-miner printed no summary")
        return ClientTally(
            submitted=int(self.summary["submitted"]),
            accepted=int(self.summary["accepted"]),
            rejected=int(self.summary["rejected"]),
            blocks=self.blocks,
        )


def parse_cpuminer(output: str) -> ClientTally:
    parser = CpuminerOutput()
    for line in output.splitlines():
        parser.feed(line)
    return parser.result()


def parse_prism_miner(output: str) -> ClientTally:
    parser = PrismMinerOutput()
    for line in output.splitlines():
        parser.feed(line)
    return parser.result()


def reconcile_client(name: str, tally: ClientTally, ledger_rows: int) -> list[str]:
    """Why a client's acknowledged shares and the ledger disagree; empty when they agree.

    Every acknowledged share must be in the ledger. The ledger may also hold
    shares the client submitted but stopped before hearing about, and no more.
    """
    problems = []
    if tally.accepted == 0:
        problems.append(f"{name}: no share was accepted")
    if ledger_rows < tally.accepted:
        problems.append(
            f"{name}: {tally.accepted} shares acknowledged but only {ledger_rows} in the ledger"
        )
    if ledger_rows > tally.accepted + tally.unanswered:
        problems.append(
            f"{name}: {ledger_rows} ledger rows exceed {tally.accepted} acknowledged plus "
            f"{tally.unanswered} unanswered submissions"
        )
    return problems


def double_sha256(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def difficulty_target(difficulty: float) -> int:
    target = Fraction(DIFF1_TARGET) / Fraction(difficulty)
    return min(int(target), (1 << 256) - 1)


def block_header(job: dict, extranonce1: bytes, extranonce2: bytes, nonce: int) -> bytes:
    """The 80-byte header a notify describes, built as qbit-prism-miner builds it."""
    coinbase = bytes.fromhex(job["coinb1"]) + extranonce1 + extranonce2 + bytes.fromhex(job["coinb2"])
    merkle = double_sha256(coinbase)
    for sibling in job["branch"]:
        merkle = double_sha256(merkle + bytes.fromhex(sibling))
    previous = bytes.fromhex(job["prevhash"])
    previous = b"".join(previous[i : i + 4][::-1] for i in range(0, 32, 4))
    return (
        int(job["version"], 16).to_bytes(4, "little")
        + previous
        + merkle
        + int(job["ntime"], 16).to_bytes(4, "little")
        + int(job["nbits"], 16).to_bytes(4, "little")
        + nonce.to_bytes(4, "little")
    )


def solve_share(job: dict, extranonce1: bytes, extranonce2_size: int, difficulty: float) -> tuple[str, str]:
    """An (extranonce2, nonce) pair whose header meets half the share target.

    Half keeps the proof clear of any rounding at the boundary; on the lab's
    share difficulty that takes a handful of hashes.
    """
    target = difficulty_target(difficulty) // 2
    extranonce2 = (1).to_bytes(extranonce2_size, "little")
    for nonce in range(1 << 32):
        header = block_header(job, extranonce1, extranonce2, nonce)
        if int.from_bytes(double_sha256(header), "little") <= target:
            return extranonce2.hex(), f"{nonce:08x}"
    raise LaneFailure("no nonce meets the share target")


def notify_job(message: dict) -> dict:
    params = message["params"]
    keys = ("job_id", "prevhash", "coinb1", "coinb2", "branch", "version", "nbits", "ntime", "clean")
    return dict(zip(keys, params))


# ---------------------------------------------------------------------------
# Talking to the stack.
# ---------------------------------------------------------------------------


def run(command: list[str], *, check: bool = True, capture: bool = True, timeout: float | None = None,
        input_text: str | None = None) -> subprocess.CompletedProcess:
    process = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=capture, timeout=timeout, input=input_text
    )
    if check and process.returncode != 0:
        detail = (process.stderr or process.stdout or "").strip()[-4000:] if capture else ""
        raise LaneFailure(f"{' '.join(command[:6])} ... exited {process.returncode}: {detail}")
    return process


def http_json(url: str, *, timeout: float = 5.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read())


def until(what: str, seconds: float, probe, interval: float = 1.0):
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = probe()
            if value:
                return value
        except Exception as error:  # noqa: BLE001 - reported when the wait expires
            last_error = error
        time.sleep(interval)
    raise LaneFailure(f"timed out after {seconds:.0f}s waiting for {what}; last error: {last_error}")


class Rpc:
    def __init__(self) -> None:
        credentials = base64.b64encode(f"{QBIT_RPC_USER}:{QBIT_RPC_PASSWORD}".encode()).decode()
        self.headers = {"Authorization": f"Basic {credentials}", "Content-Type": "application/json"}

    def call(self, method: str, params: list | None = None, wallet: str | None = None):
        url = f"http://127.0.0.1:{QBIT_RPC_PORT}/" + (f"wallet/{wallet}" if wallet else "")
        body = json.dumps({"jsonrpc": "1.0", "id": method, "method": method, "params": params or []})
        request = urllib.request.Request(url, data=body.encode(), headers=self.headers)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read())
        except urllib.error.HTTPError as error:
            payload = json.loads(error.read() or b"{}")
        if payload.get("error"):
            raise LaneFailure(f"qbit RPC {method} failed: {payload['error']}")
        return payload["result"]


class Stratum:
    """A line-delimited Stratum V1 client, just enough for the resume check."""

    def __init__(self, port: int, timeout: float = 20.0) -> None:
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.timeout = timeout
        self.buffer = b""
        self.next_id = 1
        self.job: dict | None = None
        self.difficulty: float | None = None
        self.extranonce1 = b""
        self.extranonce2_size = 0

    def close(self) -> None:
        self.sock.close()

    def _message(self, deadline: float) -> dict:
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LaneFailure("Stratum read timed out")
            self.sock.settimeout(remaining)
            chunk = self.sock.recv(65536)
            if not chunk:
                raise LaneFailure("Stratum server closed the connection")
            self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        message = json.loads(line)
        if message.get("method") == "mining.notify":
            self.job = notify_job(message)
        elif message.get("method") == "mining.set_difficulty":
            self.difficulty = float(message["params"][0])
        return message

    def request(self, method: str, params: list) -> dict:
        request_id = self.next_id
        self.next_id += 1
        self.sock.sendall((json.dumps({"id": request_id, "method": method, "params": params}) + "\n").encode())
        deadline = time.monotonic() + self.timeout
        while True:
            message = self._message(deadline)
            if message.get("id") == request_id and "method" not in message:
                return message

    def login(self, username: str) -> None:
        subscribed = self.request("mining.subscribe", ["prism-shipped-images/1"])
        if subscribed.get("error"):
            raise LaneFailure(f"subscribe rejected: {subscribed['error']}")
        self.extranonce1 = bytes.fromhex(subscribed["result"][1])
        self.extranonce2_size = int(subscribed["result"][2])
        authorized = self.request("mining.authorize", [username, "x"])
        if authorized.get("result") is not True:
            raise LaneFailure(f"authorize {username} rejected: {authorized.get('error')}")
        deadline = time.monotonic() + self.timeout
        while self.job is None or self.difficulty is None:
            self._message(deadline)


# ---------------------------------------------------------------------------
# The lane.
# ---------------------------------------------------------------------------


@dataclass
class Lane:
    work: Path
    out: Path
    project: str
    load_seconds: int
    cpuminer_threads: int
    cpuminer_diff_multiplier: float
    prism_miner_hashes_per_second: int
    env_file: Path = field(init=False)
    env: dict[str, str] = field(init=False)
    rpc: Rpc = field(default_factory=Rpc)
    report: dict = field(default_factory=dict)
    checks: dict[str, dict] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.env_file = self.work / "lane.env"
        self.env = read_env_file(self.env_file)
        self.report = {"schema": "qbit.prism.shipped-image-lane.v1", "project": self.project,
                       "load_seconds": self.load_seconds, "phases": {}, "checks": self.checks}

    # -- helpers --------------------------------------------------------

    def compose(self, *args: str, check: bool = True, timeout: float | None = 600,
                profiles: tuple[str, ...] = ("prism",)) -> subprocess.CompletedProcess:
        return run(compose_command(self.project, self.env_file, *args, profiles=profiles),
                   check=check, timeout=timeout)

    def psql(self, sql: str) -> str:
        return self.compose("exec", "-T", "prism-postgres", "psql", "-XqAt", "-v", "ON_ERROR_STOP=1",
                            "-U", POSTGRES_USER, "-d", POSTGRES_DB, "-c", sql, timeout=60).stdout.strip()

    def scalar(self, sql: str) -> int:
        return int(self.psql(sql) or 0)

    def passed(self, name: str, **evidence) -> None:
        self.checks[name] = {"passed": True, **evidence}
        log(f"PASS {name}: {json.dumps(evidence, sort_keys=True)}")

    def failed(self, name: str, problems: list[str], **evidence) -> None:
        self.checks[name] = {"passed": False, "problems": problems, **evidence}
        for problem in problems:
            log(f"FAIL {name}: {problem}")

    def phase(self, name: str, started: float) -> None:
        self.report["phases"][name] = round(time.monotonic() - started, 1)

    # -- phases ---------------------------------------------------------

    def start(self) -> dict[str, str]:
        started = time.monotonic()
        self.compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "180", "qbitd")
        until("the qbitd RPC", 60, lambda: self.rpc.call("getblockchaininfo"))
        self.rpc.call("createwallet", [WALLET])
        addresses = {
            role: self.rpc.call("getnewaddress", ["", "p2mr"], wallet=WALLET)
            for role in ("bootstrap", "cpuminer", "prism-miner", "resume", "thief")
        }
        # A fresh regtest chain is in initial block download until a block
        # with a current timestamp arrives; the frontends refuse to serve
        # work before that.
        for role, address in addresses.items():
            if not ADDRESS.match(address):
                raise LaneFailure(f"the node's {role} address {address!r} is not a bech32 string")
        self.rpc.call("generatetoaddress", [1, addresses["bootstrap"]])
        self.phase("node", started)
        started = time.monotonic()
        self.compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "420")
        self.phase("stack", started)
        self.report["addresses"] = addresses
        return addresses

    def self_check(self, service: str) -> dict:
        process = self.compose("exec", "-T", service, "qbit-prism-server", "self-check",
                               check=False, timeout=120)
        try:
            report = json.loads(process.stdout)
        except json.JSONDecodeError as error:
            raise LaneFailure(f"{service} self-check printed no JSON report (exit {process.returncode}): "
                              f"{process.stderr.strip()[-2000:]}") from error
        report["_exit"] = process.returncode
        report["_stderr_tail"] = process.stderr.strip()[-2000:]
        return report

    def check_frontends(self, self_checks: dict[str, dict]) -> None:
        problems = []
        health = {}
        for port in HEALTH_PORTS:
            body = until(f"frontend /healthz on {port}", 180,
                         lambda port=port: (lambda b: b if b.get("ok") is True else None)(
                             http_json(f"http://127.0.0.1:{port}/healthz")))
            health[port] = body.get("instance_id")
        if sorted(health.values()) != sorted(INSTANCE_IDS):
            problems.append(f"healthz instance ids {health} are not {INSTANCE_IDS}")
        observed = {}
        for service, report in self_checks.items():
            live = report.get("live_instances") or {}
            observed[service] = {"status": live.get("status"), "count": live.get("count"),
                                 "instance_ids": live.get("instance_ids"), "ok": report.get("ok"),
                                 "exit": report.get("_exit"), "error": report.get("_error"),
                                 "stderr_tail": report.get("_stderr_tail")}
            if live.get("status") != "observed" or live.get("count") != 2 or \
                    sorted(live.get("instance_ids") or []) != sorted(INSTANCE_IDS):
                problems.append(f"{service} self-check observes {observed[service]}, not both frontends")
        if problems:
            self.failed("ha-frontends-ready", problems, healthz=health, self_check=observed)
        else:
            self.passed("ha-frontends-ready", healthz=health, self_check=observed)

    def check_resume(self, addresses: dict[str, str]) -> None:
        """#281 criterion 1: work issued on frontend 1, resumed on frontend 2."""
        try:
            self._check_resume(addresses)
        except (LaneFailure, OSError, KeyError, ValueError) as error:
            self.failed("cross-frontend-resume", [f"{type(error).__name__}: {error}"])

    def _check_resume(self, addresses: dict[str, str]) -> None:
        username = f"{addresses['resume']}.resume"
        thief = f"{addresses['thief']}.thief"
        answers = {}
        issuer = Stratum(STRATUM_PORTS[0])
        try:
            issuer.login(username)
            job = issuer.job
            extranonce2, nonce = solve_share(job, issuer.extranonce1, issuer.extranonce2_size,
                                             issuer.difficulty)
            issued_extranonce1 = issuer.extranonce1
        finally:
            issuer.close()
        submit = [username, job["job_id"], extranonce2, job["ntime"], nonce]
        clients = []
        try:
            resumed = Stratum(STRATUM_PORTS[1])
            clients.append(resumed)
            resumed.login(username)
            answers["resumed_on_frontend_2"] = resumed.request("mining.submit", submit)
            stranger = Stratum(STRATUM_PORTS[1])
            clients.append(stranger)
            stranger.login(thief)
            answers["other_worker_on_frontend_2"] = stranger.request("mining.submit", [thief, *submit[1:]])
            answers["replay_on_frontend_2"] = resumed.request("mining.submit", submit)
            back = Stratum(STRATUM_PORTS[0])
            clients.append(back)
            back.login(username)
            answers["replay_on_frontend_1"] = back.request("mining.submit", submit)
        finally:
            for client in clients:
                client.close()
        problems = []
        if resumed.extranonce1 == issued_extranonce1:
            problems.append("frontend 2 reused frontend 1's session extranonce")
        if answers["resumed_on_frontend_2"].get("result") is not True:
            problems.append(f"frontend 2 refused frontend 1's work: {answers['resumed_on_frontend_2']}")
        for name in ("other_worker_on_frontend_2", "replay_on_frontend_2", "replay_on_frontend_1"):
            if answers[name].get("result") is True:
                problems.append(f"{name} was accepted: {answers[name]}")
        self.report["resume_answers"] = answers
        self.report["resume_job"] = job["job_id"]
        # The ledger half (one credited row, none for the other worker) is
        # checked with the rest of the ledger after the load phase.
        self.checks["cross-frontend-resume"] = {"passed": not problems, "problems": problems,
                                                "answers": {k: v.get("result") for k, v in answers.items()}}
        for problem in problems:
            log(f"FAIL cross-frontend-resume: {problem}")

    def load(self, addresses: dict[str, str]) -> dict[str, ClientTally]:
        started = time.monotonic()
        network = f"{self.project}_default"
        seconds = str(self.load_seconds)
        clients = {
            "cpuminer": [
                "docker", "run", "--rm", "--name", f"{self.project}-cpuminer", "--network", network,
                REAL_MINER_IMAGE, "timeout", "-s", "INT", seconds, "cpuminer", "-a", "sha256d",
                "-o", "stratum+tcp://prism-coordinator:3340", "-u", f"{addresses['cpuminer']}.cpuminer",
                "-p", "x", "-t", str(self.cpuminer_threads), "--no-color",
                "--diff-multiplier", f"{self.cpuminer_diff_multiplier:g}",
            ],
            "prism-miner": [
                "docker", "run", "--rm", "--name", f"{self.project}-prism-miner", "--network", network,
                "--no-healthcheck", COORDINATOR_IMAGE, "qbit-prism-miner",
                "--address", "prism-coordinator-2:3340", "--username", f"{addresses['prism-miner']}.prism-miner",
                "--threads", "1", "--hashes-per-second", str(self.prism_miner_hashes_per_second),
                "--duration-seconds", seconds,
            ],
        }
        processes = {}
        parsers = {"cpuminer": CpuminerOutput(), "prism-miner": PrismMinerOutput()}
        readers = []
        for name, command in clients.items():
            log(f"starting {name} for {seconds}s")
            process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       text=True, bufsize=1)
            processes[name] = process

            def pump(process=process, parser=parsers[name], name=name):
                # Parse every line, but keep the job log and the artifact
                # bounded: a client that floods its pool must not fill either.
                logged = echoed = 0
                with (self.out / f"{name}.log").open("w", encoding="utf-8") as sink:
                    for line in process.stdout:
                        parser.feed(line)
                        if logged < CLIENT_LOG_BYTES:
                            sink.write(line)
                            logged += len(line)
                            if logged >= CLIENT_LOG_BYTES:
                                sink.write(f"[{PREFIX}: log truncated at {CLIENT_LOG_BYTES} bytes]\n")
                        if echoed < CLIENT_ECHO_LINES:
                            print(f"[{name}] {line.rstrip()}", flush=True)
                            echoed += 1

            reader = threading.Thread(target=pump, daemon=True)
            reader.start()
            readers.append(reader)
        # Both frontends are serving a miner now; ask each what it sees.
        time.sleep(min(60, self.load_seconds / 2))
        self_checks = {}
        for service in ("prism-coordinator", "prism-coordinator-2"):
            try:
                self_checks[service] = self.self_check(service)
            except (LaneFailure, subprocess.TimeoutExpired) as error:
                self_checks[service] = {"_error": str(error)}
        self.report["self_check"] = self_checks
        codes = {}
        for name, process in processes.items():
            try:
                codes[name] = process.wait(timeout=self.load_seconds + 120)
            except subprocess.TimeoutExpired:
                process.kill()
                codes[name] = process.wait()
        for reader in readers:
            reader.join(timeout=30)
        self.report["client_exit"] = codes
        self.phase("load", started)
        # timeout(1) exits 124 when it stopped cpuminer at the deadline; any
        # other status means cpuminer stopped on its own before then.
        if codes["cpuminer"] != 124:
            raise LaneFailure(f"cpuminer exited {codes['cpuminer']} before its {seconds}s run ended")
        if codes["prism-miner"] != 0:
            raise LaneFailure(f"qbit-prism-miner exited {codes['prism-miner']}")
        self.check_frontends(self_checks)
        return {name: parser.result() for name, parser in parsers.items()}

    def quiesce(self) -> int:
        """The ledger's accepted count once it holds still and both frontends report it."""
        started = time.monotonic()

        def settled():
            before = self.scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted")
            latest = [http_json(f"http://127.0.0.1:{port}/audit/latest") for port in HEALTH_PORTS]
            after = self.scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted")
            counts = [body.get("accepted_share_count") for body in latest]
            self.report["audit_latest_counts"] = counts
            return after if before == after and all(count == after for count in counts) else None

        count = until("both frontends to report the ledger's accepted-share count", 90, settled, interval=2)
        self.phase("quiesce", started)
        return count

    def check_ledger(self, addresses: dict[str, str], tallies: dict[str, ClientTally], accepted: int) -> None:
        rows = {role: self.scalar(f"SELECT count(*) FROM qbit_share_ledger WHERE accepted AND miner_id='{address}'")
                for role, address in addresses.items()}
        credited = self.scalar("SELECT count(*) FROM qbit_prism_share_hashes")
        writers = {
            writer: int(count) for writer, count in (line.split("|", 1) for line in self.psql(
                "SELECT writer_id, count(*) FROM qbit_share_ledger WHERE accepted GROUP BY writer_id ORDER BY 1"
            ).splitlines() if line)
        }
        problems = []
        for role, tally in tallies.items():
            problems += reconcile_client(role, tally, rows[role])
        if accepted != credited:
            problems.append(f"{accepted} accepted shares but {credited} credited headers")
        if not set(INSTANCE_IDS) <= set(writers):
            problems.append(f"shares were written by {sorted(writers)}, not both frontends")
        if rows["thief"] != 0:
            problems.append(f"the other worker has {rows['thief']} credited shares")
        if rows["bootstrap"] != 0:
            problems.append(f"the bootstrap address has {rows['bootstrap']} credited shares")
        resume = self.checks.get("cross-frontend-resume")
        if resume is not None:
            resume_problems = []
            if rows["resume"] != 1:
                resume_problems.append(f"the resumed work is credited {rows['resume']} times, not once")
            resume["ledger_rows"] = rows["resume"]
            resume["problems"] += resume_problems
            resume["passed"] = not resume["problems"]
            for problem in resume_problems:
                log(f"FAIL cross-frontend-resume: {problem}")
            if resume["passed"]:
                log(f"PASS cross-frontend-resume: {json.dumps(resume, sort_keys=True)}")
        evidence = {"accepted": accepted, "credited_headers": credited,
                    "rows": rows, "writers": writers,
                    "clients": {role: tally.__dict__ | {"unanswered": tally.unanswered}
                                for role, tally in tallies.items()}}
        if problems:
            self.failed("ledger-reconciles", problems, **evidence)
        else:
            self.passed("ledger-reconciles", **evidence)
        self.report["tallies"] = evidence["clients"]

    def check_blocks(self, addresses: dict[str, str]) -> None:
        rows = [line.split("|") for line in self.psql(
            "SELECT block_hash, block_height, solver_miner_id FROM qbit_pool_blocks "
            "WHERE chain_state='confirmed' ORDER BY block_height"
        ).splitlines() if line]
        self.report["confirmed_blocks"] = len(rows)
        latest = {}
        for block_hash, height, solver in rows:
            for role in ("cpuminer", "prism-miner"):
                if solver == addresses[role]:
                    latest[role] = (block_hash, int(height))
        verified = {}
        audit_problems = []
        for role, check in (("cpuminer", "cpuminer-found-block"), ("prism-miner", "prism-miner-found-block")):
            count = sum(1 for _, _, solver in rows if solver == addresses[role])
            if role not in latest:
                self.failed(check, [f"no confirmed block solved by {role}"], blocks=count)
                continue
            block_hash, height = latest[role]
            active = self.rpc.call("getblockhash", [height])
            if active != block_hash:
                self.failed(check, [f"{block_hash} at {height} is not on the node's active chain ({active})"],
                            blocks=count)
                continue
            self.passed(check, blocks=count, latest=block_hash, height=height)
            try:
                self.verify_audit(block_hash, height)
                verified[role] = block_hash
            except LaneFailure as error:
                audit_problems.append(f"{role} block {block_hash}: {error}")
        if audit_problems or len(verified) < 2:
            if len(verified) < 2 and not audit_problems:
                audit_problems.append("not every client has a found block to verify")
            self.failed("found-block-audit-verifies", audit_problems, verified=verified)
        else:
            self.passed("found-block-audit-verifies", verified=verified)
        self.check_public_blocks(list(verified.values()))

    def verify_audit(self, block_hash: str, height: int) -> None:
        bundles = []
        for port in HEALTH_PORTS:
            body = http_json(f"http://127.0.0.1:{port}/audit/blocks/{block_hash}/bundle", timeout=30)
            bundles.append(body.get("audit_bundle"))
        if bundles[0] is None or bundles[0] != bundles[1]:
            raise LaneFailure("the frontends do not serve one identical audit bundle")
        block = self.rpc.call("getblock", [block_hash, 2])
        coinbase = block["tx"][0].get("hex") or self.rpc.call(
            "getrawtransaction", [block["tx"][0]["txid"], False, block_hash])
        path = self.out / f"audit-bundle-{height}-{block_hash[:16]}.json"
        path.write_text(json.dumps(bundles[0]), encoding="utf-8")
        path.chmod(0o644)
        process = run(["docker", "run", "--rm", "--network", "none", "--no-healthcheck",
                       "-v", f"{path}:/work/bundle.json:ro", COORDINATOR_IMAGE,
                       "qbit-prism-audit-verify", "/work/bundle.json", "--coinbase-tx-hex", coinbase,
                       "--ledger-writer-public-key-hex", self.env["PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX"]],
                      check=False, timeout=120)
        (self.out / f"audit-verify-{height}.txt").write_text(process.stdout + process.stderr, encoding="utf-8")
        if process.returncode != 0:
            raise LaneFailure(f"qbit-prism-audit-verify exited {process.returncode}: {process.stderr.strip()}")

    def check_public_blocks(self, hashes: list[str]) -> None:
        if not hashes:
            self.failed("public-api-lists-found-blocks", ["no verified block to look for"])
            return

        def listed():
            seen, page = set(), 1
            while True:
                body = http_json(f"http://127.0.0.1:{PUBLIC_API_PORT}/public/v1/blocks?limit=100&page={page}")
                text = json.dumps(body)
                seen |= {h for h in hashes if h in text}
                pages = (body.get("pagination") or {}).get("total_pages") or 0
                if seen == set(hashes) or page >= pages:
                    return seen == set(hashes)
                page += 1

        try:
            until("the public API to list the verified blocks", 120, listed, interval=3)
        except LaneFailure as error:
            self.failed("public-api-lists-found-blocks", [str(error)], blocks=hashes)
            return
        self.passed("public-api-lists-found-blocks", blocks=hashes)

    def collect(self) -> None:
        for args, name in ((("ps", "--all"), "compose-ps.txt"),
                           (("logs", "--no-color", "--timestamps"), "compose-logs.txt")):
            process = self.compose(*args, check=False, timeout=180)
            (self.out / name).write_text(process.stdout + process.stderr, encoding="utf-8")

    def down(self) -> None:
        for name in ("cpuminer", "prism-miner"):
            run(["docker", "rm", "-f", f"{self.project}-{name}"], check=False)
        self.compose("down", "-v", "--remove-orphans", check=False, timeout=300,
                     profiles=("prism", "real-miner-smoke"))

    def execute(self) -> bool:
        started = time.monotonic()
        error = None
        try:
            addresses = self.start()
            self.check_resume(addresses)
            tallies = self.load(addresses)
            accepted = self.quiesce()
            self.check_ledger(addresses, tallies, accepted)
            self.check_blocks(addresses)
        except (LaneFailure, subprocess.TimeoutExpired, OSError, KeyError, ValueError) as caught:
            error = f"{type(caught).__name__}: {caught}"
            log(f"ERROR {error}")
        finally:
            try:
                self.collect()
            finally:
                self.down()
        self.phase("total", started)
        missing = [name for name in CHECKS if name not in self.checks]
        passed = error is None and not missing and all(c["passed"] for c in self.checks.values())
        self.report.update(error=error, not_reached=missing, passed=passed)
        (self.out / "l6-report.json").write_text(json.dumps(self.report, indent=2, sort_keys=True) + "\n",
                                                 encoding="utf-8")
        write_summary(self.report)
        return passed


def write_summary(report: dict) -> None:
    lines = ["### L6 shipped images: Compose `prism` profile with the HA overlay", "",
             "| Check | Result |", "| --- | --- |"]
    for name in CHECKS:
        check = report["checks"].get(name)
        if check is None:
            result = "not reached"
        elif check["passed"]:
            result = "pass"
        else:
            result = "**fail**: " + "; ".join(check.get("problems", []))
        lines.append(f"| `{name}` | {result} |")
    lines += ["", f"Phases (s): `{json.dumps(report['phases'])}`"]
    if report.get("tallies"):
        lines.append(f"Clients: `{json.dumps(report['tallies'], sort_keys=True)}`")
    if report.get("error"):
        lines.append(f"Error: `{report['error']}`")
    text = "\n".join(lines) + "\n"
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(text)


def prepare(work: Path) -> None:
    work.mkdir(parents=True, exist_ok=True)
    qbit_src = run(["bash", "scripts/prepare-qbit-source.sh"], timeout=900).stdout.strip().splitlines()[-1]
    manifest_seed, attestation_seed = secrets.token_hex(32), secrets.token_hex(32)
    text = lane_env(Path(qbit_src), manifest_seed, attestation_seed, ed25519_public_key_hex(attestation_seed))
    path = work / "lane.env"
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)
    log(f"staged qbit source at {qbit_src}; wrote {path}")


def build(work: Path, project: str) -> None:
    env_file = work / "lane.env"
    started = time.monotonic()
    # The frontends and the public API share one image; build it once.
    for services, profiles in ((("qbitd", "prism-coordinator"), ("prism",)),
                               (("real-miner",), ("prism", "real-miner-smoke"))):
        run(compose_command(project, env_file, "build", *services, profiles=profiles),
            capture=False, timeout=2400)
    log(f"built the images in {time.monotonic() - started:.0f}s")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("changed", help="print whether stdin's changed paths run the lane")
    for name in ("prepare", "build", "run"):
        command = commands.add_parser(name)
        command.add_argument("--work", type=Path, required=True)
        command.add_argument("--project", default="qbit-l6")
        if name == "run":
            command.add_argument("--out", type=Path, required=True)
            command.add_argument("--load-seconds", type=int, default=240)
            command.add_argument("--cpuminer-threads", type=int, default=2)
            # cpuminer submits every hash that meets the pool's difficulty
            # without waiting for answers. At the lab's 1e-9 floor that is
            # most hashes: its connection floods, it never reads the next
            # job and it mines a stale one for the rest of the run. Hashing
            # locally to a harder target keeps it to a few shares a second
            # until vardiff takes over; every share still meets regtest's
            # network target, so each accepted one is a block.
            command.add_argument("--cpuminer-diff-multiplier", type=float, default=1e7)
            command.add_argument("--prism-miner-hashes-per-second", type=int, default=20000)
    args = parser.parse_args(argv)
    if args.command == "changed":
        print("true" if watched(sys.stdin) else "false")
        return 0
    work = args.work.resolve()
    if args.command == "prepare":
        prepare(work)
        return 0
    if args.command == "build":
        build(work, args.project)
        return 0
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    out.chmod(0o755)
    lane = Lane(work=work, out=out, project=args.project, load_seconds=args.load_seconds,
                cpuminer_threads=args.cpuminer_threads,
                cpuminer_diff_multiplier=args.cpuminer_diff_multiplier,
                prism_miner_hashes_per_second=args.prism_miner_hashes_per_second)
    return 0 if lane.execute() else 1


if __name__ == "__main__":
    sys.exit(main())

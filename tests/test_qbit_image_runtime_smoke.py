#!/usr/bin/env python3
"""Contract tests for the real-qbitd runtime image smoke script.

The smoke itself only ever runs against a freshly built qbitd image in CI, so
these tests drive it against a deterministic stand-in for the docker CLI. That
keeps the assertions it makes -- in particular the ``getblockheader.bits``
contract block-candidate collapse depends on -- from silently disappearing
without a red test.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".github" / "scripts" / "test-qbit-image-runtime.sh"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
IMAGE = "qbit-runtime-smoke-fixture:test"
GENESIS_HASH = "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206"

sys.path.insert(0, str(ROOT))

from lab.prism.block_candidates import _collapse_scaled_difficulty  # noqa: E402
from lab.prism.template_artifacts import scaled_network_difficulty  # noqa: E402


# A stand-in for the docker CLI covering exactly the subcommands the smoke
# issues. Anything else exits 97 so an unmodelled call fails loudly instead of
# being mistaken for a passing smoke.
FAKE_DOCKER = r'''#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

STATE = Path(os.environ["FAKE_DOCKER_STATE"])
IMAGE = os.environ["QBIT_IMAGE"]
ENTRYPOINT = '["/usr/bin/tini","--","/usr/local/bin/qbit-entrypoint.sh"]'


def record_path(name):
    return STATE / ("container-" + name + ".json")


def load(name):
    path = record_path(name)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save(name, record):
    record_path(name).write_text(json.dumps(record), encoding="utf-8")


def out(text):
    sys.stdout.write(text if text.endswith("\n") else text + "\n")


def run(argv):
    args = argv[1:]
    if "--rm" in args:
        tail = args[args.index("--rm") + 1:]
        daemon_args = tail[1:]
        if "-maxtipage=1" in daemon_args:
            sys.stderr.write(
                "qbit-entrypoint: caller-provided -maxtipage=1 is not allowed\n"
            )
            return 1
        if "-version" in daemon_args:
            out("qbit daemon version v0.0.0-fixture")
        return 0

    name = args[args.index("--name") + 1]
    env = {}
    index = 0
    while index < len(args):
        if args[index] == "--env":
            key, _, value = args[index + 1].partition("=")
            env[key] = value
            index += 2
            continue
        index += 1
    daemon_args = args[args.index(IMAGE) + 1:]
    save(
        name,
        {
            "env": env,
            "args": daemon_args,
            "chain": "regtest" if "-regtest" in daemon_args else "main",
            "running": True,
            "exit_code": 0,
        },
    )
    out("f" * 64)
    return 0


def exec_(argv):
    name = argv[1]
    record = load(name)
    if record is None or not record["running"]:
        return 1
    rest = argv[2:]
    if rest[0] == "sh":
        lines = ["qbitd"] + list(record["args"])
        if record["env"].get("QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED") == "0":
            lines.append(
                "-maxtipage="
                + record["env"]["QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"]
            )
        out("\n".join(lines))
        return 0
    if rest[0] != "qbit-cli":
        return 97
    positional = [token for token in rest[1:] if not token.startswith("-")]
    method, params = positional[0], positional[1:]
    if method == "getblockchaininfo":
        out(json.dumps({"chain": record["chain"], "blocks": 0}, indent=2))
        return 0
    if method == "getbestblockhash":
        out(os.environ["FAKE_DOCKER_BEST_HASH"])
        return 0
    if method == "getblockheader":
        header = json.loads(os.environ["FAKE_DOCKER_HEADER"])
        header["hash"] = params[0]
        out(json.dumps(header, indent=2))
        return 0
    return 97


def container(argv):
    if argv[1] != "inspect":
        return 97
    rest = argv[2:]
    template = None
    if rest[0] == "--format":
        template = rest[1]
        rest = rest[2:]
    record = load(rest[0])
    if record is None:
        sys.stderr.write("Error: No such container: " + rest[0] + "\n")
        return 1
    if template is None:
        out("[]")
    elif "Running" in template:
        out("true" if record["running"] else "false")
    elif "ExitCode" in template:
        out(str(record["exit_code"]))
    else:
        return 97
    return 0


def main(argv):
    with (STATE / "commands.log").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(argv) + "\n")
    command = argv[0]
    if command == "image":
        if argv[1] != "inspect":
            return 97
        if "--format" in argv:
            out(ENTRYPOINT)
        else:
            out("[]")
        return 0
    if command == "run":
        return run(argv)
    if command == "exec":
        return exec_(argv)
    if command == "container":
        return container(argv)
    if command == "top":
        if load(argv[1]) is None:
            return 1
        out("PID   COMMAND\n1     tini\n7     qbitd")
        return 0
    if command == "stop":
        name = argv[-1]
        record = load(name)
        if record is None:
            return 1
        record["running"] = False
        record["exit_code"] = int(os.environ.get("FAKE_DOCKER_EXIT_CODE", "0"))
        save(name, record)
        out(name)
        return 0
    if command == "rm":
        path = record_path(argv[-1])
        if not path.exists():
            return 1
        path.unlink()
        out(argv[-1])
        return 0
    if command == "logs":
        return 0
    return 97


sys.exit(main(sys.argv[1:]))
'''


class QbitImageRuntimeSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        root = Path(self.temp_dir.name)
        self.state = root / "state"
        self.state.mkdir()
        bin_dir = root / "bin"
        bin_dir.mkdir()
        fake_docker = bin_dir / "docker"
        fake_docker.write_text(FAKE_DOCKER, encoding="utf-8")
        fake_docker.chmod(0o755)
        self.bin_dir = bin_dir

    def run_smoke(
        self,
        *,
        bits: object = "207fffff",
        header: dict[str, object] | None = None,
        best_hash: str = GENESIS_HASH,
    ) -> subprocess.CompletedProcess[str]:
        if header is None:
            header = {"height": 0, "version": 1}
            if bits is not None:
                header["bits"] = bits
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}{os.pathsep}{env['PATH']}",
                "QBIT_IMAGE": IMAGE,
                "FAKE_DOCKER_STATE": str(self.state),
                "FAKE_DOCKER_HEADER": json.dumps(header),
                "FAKE_DOCKER_BEST_HASH": best_hash,
            }
        )
        env.pop("QBIT_CI_IMAGE", None)
        return subprocess.run(
            ["bash", str(SCRIPT)],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=self.temp_dir.name,
        )

    def docker_commands(self) -> list[list[str]]:
        log = self.state / "commands.log"
        if not log.exists():
            return []
        return [
            json.loads(line)
            for line in log.read_text(encoding="utf-8").splitlines()
            if line
        ]

    def assert_smoke_failed(
        self, result: subprocess.CompletedProcess[str], needle: str
    ) -> None:
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("smoke: PASS", result.stdout)
        self.assertIn("qbit runtime image smoke: FAIL", result.stderr)
        self.assertIn(needle, result.stderr)

    def test_smoke_passes_and_reports_production_scaled_difficulty(self) -> None:
        result = self.run_smoke(bits="207fffff")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("qbit runtime image smoke: PASS", result.stdout)
        expected = _collapse_scaled_difficulty("207fffff")
        self.assertIsNotNone(expected)
        self.assertIn(
            f"bits 207fffff scaled difficulty {expected}",
            result.stdout,
        )

    def test_smoke_reads_the_header_of_the_running_regtest_container(self) -> None:
        result = self.run_smoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        commands = self.docker_commands()
        header_reads = [
            command
            for command in commands
            if command[0] == "exec" and "getblockheader" in command
        ]
        self.assertEqual(len(header_reads), 1, commands)
        (header_read,) = header_reads
        self.assertTrue(header_read[1].endswith("-regtest"), header_read)
        self.assertIn(GENESIS_HASH, header_read)
        # The header must be read while the node is up: its stop follows.
        stop_index = next(
            index
            for index, command in enumerate(commands)
            if command[0] == "stop" and command[-1] == header_read[1]
        )
        self.assertLess(commands.index(header_read), stop_index)

    def test_smoke_derives_mainnet_scale_difficulty_without_shell_overflow(self) -> None:
        # 1b0404cb scales past 2**63, where bash's signed arithmetic would wrap
        # negative. The reported value must still be the production integer.
        result = self.run_smoke(bits="1b0404cb")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        expected = scaled_network_difficulty("1b0404cb")
        self.assertGreater(expected, 2**63)
        self.assertEqual(expected, _collapse_scaled_difficulty("1b0404cb"))
        self.assertIn(f"bits 1b0404cb scaled difficulty {expected}", result.stdout)

    def test_smoke_fails_when_header_bits_are_too_short(self) -> None:
        self.assert_smoke_failed(
            self.run_smoke(bits="7fffff"), "is not eight hex characters: 7fffff"
        )

    def test_smoke_fails_when_header_bits_are_too_long(self) -> None:
        self.assert_smoke_failed(
            self.run_smoke(bits="207fffff0"), "is not eight hex characters: 207fffff0"
        )

    def test_smoke_fails_when_header_bits_are_not_hexadecimal(self) -> None:
        self.assert_smoke_failed(
            self.run_smoke(bits="207fzzzz"), "is not eight hex characters: 207fzzzz"
        )

    def test_smoke_fails_when_header_omits_bits(self) -> None:
        self.assert_smoke_failed(
            self.run_smoke(bits=None), "is not eight hex characters: <missing>"
        )

    def test_smoke_fails_when_header_bits_are_not_a_string(self) -> None:
        self.assert_smoke_failed(
            self.run_smoke(header={"height": 0, "bits": 545259519}),
            "is not eight hex characters: <missing>",
        )

    def test_smoke_fails_when_header_bits_carry_a_zero_target(self) -> None:
        # Eight hex characters, but PRISM derives no positive work from them,
        # which is exactly the shape collapse fails its page closed on.
        result = self.run_smoke(bits="00000000")

        self.assertIsNone(_collapse_scaled_difficulty("00000000"))
        self.assert_smoke_failed(result, "did not derive PRISM's scaled difficulty")
        self.assertIn("collapse rejected the compact bits", result.stderr)

    def test_smoke_fails_when_best_block_hash_is_not_a_block_hash(self) -> None:
        self.assert_smoke_failed(
            self.run_smoke(best_hash="not-a-block-hash"),
            "getbestblockhash is not a block hash",
        )

    def test_smoke_preserves_the_existing_lifecycle_checks(self) -> None:
        result = self.run_smoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        commands = self.docker_commands()
        self.assertTrue(
            any(command[0] == "top" for command in commands),
            "process-chain check disappeared",
        )
        self.assertTrue(
            any(
                command[0] == "run" and "-maxtipage=1" in command
                for command in commands
            ),
            "caller-provided -maxtipage rejection check disappeared",
        )
        argv_probes = [
            command
            for command in commands
            if command[0] == "exec" and "sh" in command
        ]
        self.assertEqual(len(argv_probes), 2, commands)
        started = [
            command[command.index("--name") + 1]
            for command in commands
            if command[0] == "run" and "--name" in command
        ]
        self.assertEqual(len(started), 3, commands)
        for name in started:
            self.assertTrue(
                any(command[0] == "rm" and command[-1] == name for command in commands),
                f"{name} was never removed after a clean exit",
            )

    def test_smoke_fails_when_a_container_exits_uncleanly(self) -> None:
        env_backup = os.environ.get("FAKE_DOCKER_EXIT_CODE")
        os.environ["FAKE_DOCKER_EXIT_CODE"] = "3"
        try:
            result = self.run_smoke()
        finally:
            if env_backup is None:
                os.environ.pop("FAKE_DOCKER_EXIT_CODE", None)
            else:
                os.environ["FAKE_DOCKER_EXIT_CODE"] = env_backup
        self.assert_smoke_failed(result, "exited with status 3 after SIGTERM")


class QbitImageRuntimeSmokeSourceTests(unittest.TestCase):
    """Static guards keeping the assertion wired up and formula-free."""

    def setUp(self) -> None:
        self.script = SCRIPT.read_text(encoding="utf-8")

    def test_regtest_container_is_asserted_against_the_header_contract(self) -> None:
        self.assertIn(
            'assert_header_bits_contract "${REGTEST_CONTAINER}" -regtest',
            self.script,
        )

    def test_scaled_difficulty_comes_from_the_production_helpers(self) -> None:
        self.assertIn(
            "from lab.prism.block_candidates import _collapse_scaled_difficulty",
            self.script,
        )
        self.assertIn(
            "from lab.prism.template_artifacts import scaled_network_difficulty",
            self.script,
        )
        # The scale and the powLimit belong to the production helpers. A copy
        # of either here could drift away from collapse with no test noticing.
        for constant in ("1000000", "1_000_000", "207fffff"):
            with self.subTest(constant=constant):
                self.assertNotIn(constant, self.script)

    def test_ci_runs_the_smoke_with_an_interpreter_for_the_helpers(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        job = workflow.split("Build and smoke real qbitd image", 1)[1]
        self.assertIn("bash .github/scripts/test-qbit-image-runtime.sh", job)
        self.assertIn("uses: actions/setup-python@v6", job)


if __name__ == "__main__":
    unittest.main()

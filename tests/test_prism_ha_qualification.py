import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location(
    "ha_qualification", Path(__file__).resolve().parents[1] / "scripts/prism_ha_qualification.py")
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


def rendered_fixture():
    env = qualification.fixture_environment()
    services = {}
    for index, name in enumerate(qualification.FRONTENDS, 1):
        services[name] = {
            "environment": {
                "PRISM_INSTANCE_ID": env[f"PRISM_HA_INSTANCE_ID_{index}"],
                "QBIT_RPC_URL": env[f"PRISM_HA_RPC_URL_{index}"],
                "PRISM_AUDIT_PORT": env["PRISM_HA_AUDIT_PORT"],
                "PRISM_AUDIT_BIND": env["PRISM_HA_AUDIT_BIND"],
                "PRISM_DATABASE_URL": env["PRISM_DATABASE_URL"],
                "PRISM_STRATUM_PORT": "3340",
                "PRISM_STRATUM_HIGHDIFF_PORT": env["PRISM_STRATUM_HIGHDIFF_PORT"],
            },
            "ports": [
                {"host_ip": "127.0.0.1", "target": 3340, "published": str(18440 if index == 1 else 18443)},
                {"host_ip": "127.0.0.1", "target": 18441, "published": str(18441 if index == 1 else 18444)},
                {"host_ip": "127.0.0.1", "target": 18446, "published": str(18442 if index == 1 else 18445)},
            ],
        }
    return {"services": services}


class QualificationTests(unittest.TestCase):
    def test_inherited_secrets_and_external_docker_target_are_not_interpolated(self):
        with patch.dict(os.environ, {"PRISM_DATABASE_URL": "secret", "QBIT_RPC_URL": "secret",
                                     "DOCKER_HOST": "tcp://external.invalid:2375"}):
            env = qualification.fixture_environment()
        self.assertNotIn("secret", json.dumps(env))
        self.assertNotIn("DOCKER_HOST", env)
        self.assertNotIn("QBIT_RPC_URL", env)

    def test_projection_omits_all_non_allowlisted_environment(self):
        document = rendered_fixture()
        document["services"][qualification.FRONTENDS[0]]["environment"]["SIGNING_SECRET"] = "secret"
        result = qualification.check_render(document, qualification.fixture_environment())
        self.assertNotIn("secret", json.dumps(result))
        self.assertNotIn("postgresql", json.dumps(result))
        self.assertEqual(len(result), 2)

    def test_wrong_writer_instance_rpc_health_and_port_fail(self):
        document = rendered_fixture()
        for key in ("PRISM_INSTANCE_ID", "QBIT_RPC_URL", "PRISM_DATABASE_URL", "PRISM_AUDIT_PORT", "PRISM_AUDIT_BIND", "PRISM_STRATUM_HIGHDIFF_PORT"):
            with self.subTest(key=key):
                bad = copy.deepcopy(document)
                bad["services"][qualification.FRONTENDS[1]]["environment"][key] = "secret"
                with self.assertRaisesRegex(ValueError, "differs from fixture") as raised:
                    qualification.check_render(bad, qualification.fixture_environment())
                self.assertNotIn("secret", str(raised.exception))
        document["services"][qualification.FRONTENDS[1]]["ports"][0]["host_ip"] = "0.0.0.0"
        with self.assertRaisesRegex(ValueError, "loopback ports"):
            qualification.check_render(document, qualification.fixture_environment())

    def test_high_difficulty_and_unexpected_exposed_ports_fail(self):
        document = rendered_fixture()
        document["services"][qualification.FRONTENDS[1]]["ports"][2]["host_ip"] = "0.0.0.0"
        with self.assertRaisesRegex(ValueError, "loopback ports"):
            qualification.check_render(document, qualification.fixture_environment())
        document = rendered_fixture()
        document["services"][qualification.FRONTENDS[0]]["ports"].append(
            {"host_ip": "0.0.0.0", "target": 5432, "published": "5432"})
        with self.assertRaisesRegex(ValueError, "loopback ports"):
            qualification.check_render(document, qualification.fixture_environment())

    def test_render_never_prints_failure_stdout_or_stderr(self):
        failure = subprocess.CompletedProcess([], 1, "secret config", "secret diagnostics")
        with patch.object(qualification.subprocess, "run", return_value=failure) as call:
            with self.assertRaisesRegex(RuntimeError, "withheld") as raised:
                qualification.render()
        self.assertNotIn("secret", str(raised.exception))
        self.assertIn(os.devnull, call.call_args.args[0])

    def test_cleanup_only_stops_the_exact_owned_data_directory(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent) / "owned"
            data = root / "prism-load-unique" / "primary"
            data.mkdir(parents=True)
            (data / "PG_VERSION").write_text("16")
            with patch.object(qualification.subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], 0), subprocess.CompletedProcess([], 0),
                subprocess.CompletedProcess([], 3),
            ]) as call:
                result = qualification.cleanup_owned(root, Path("/fixture/bin"))
            self.assertTrue(result["owned_parent_removed"])
            self.assertEqual(len(call.call_args_list), 3)
            for args in call.call_args_list:
                self.assertEqual(args.args[0][:3], ["/fixture/bin/pg_ctl", "-D", str(data)])

    def test_unknown_exit_keeps_owned_directory(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent) / "owned"
            data = root / "prism-load-unique" / "primary"
            data.mkdir(parents=True)
            (data / "PG_VERSION").write_text("16")
            with patch.object(qualification.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)):
                with self.assertRaisesRegex(RuntimeError, "unverified"):
                    qualification.cleanup_owned(root, Path("/fixture/bin"))
            self.assertTrue(data.exists())

    def test_symlink_and_unknown_entries_are_never_cleaned(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent) / "owned"
            root.mkdir()
            outside = Path(parent) / "outside"
            outside.mkdir()
            (root / "prism-load-other").symlink_to(outside)
            with patch.object(qualification.subprocess, "run") as call:
                with self.assertRaisesRegex(RuntimeError, "unexpected entry"):
                    qualification.cleanup_owned(root, Path("/fixture/bin"))
            call.assert_not_called()
            self.assertTrue(outside.exists())

    def test_partial_initialization_has_no_running_postmaster(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent) / "owned"
            data = root / "prism-load-unique" / "primary"
            data.mkdir(parents=True)
            with patch.object(qualification.subprocess, "run") as call:
                qualification.cleanup_owned(root, Path("/fixture/bin"))
            call.assert_not_called()
            self.assertFalse(root.exists())

    def test_wrapper_records_ownership_before_start_and_sets_explicit_locale(self):
        with tempfile.TemporaryDirectory() as parent:
            base = Path(parent)
            binary = base / "pg_ctl"
            binary.write_text("fixture")
            binary.chmod(0o700)
            owned = base / "owned"
            owned.mkdir()
            args = SimpleNamespace(example_bin=binary, runtime_tests=binary, ack_tests=binary,
                                   pg_bin_dir=base, out=base / "evidence")

            def launch(command, **kwargs):
                self.assertTrue((args.out / "resource-census.json").is_file())
                self.assertEqual(kwargs["env"]["LC_ALL"], "C")
                self.assertEqual(kwargs["env"]["TMPDIR"], str(owned))
                self.assertNotIn("PRISM_DATABASE_URL", kwargs["env"])
                self.assertTrue(kwargs["start_new_session"])
                functional = args.out / "functional"
                functional.mkdir()
                (functional / "ha-functional.json").write_text(json.dumps({
                    "result": "passed", "cleanup": {"complete": True},
                }))
                return SimpleNamespace(pid=42, returncode=0, communicate=lambda **kw: ("", ""), poll=lambda: 0)

            with patch.object(qualification.tempfile, "mkdtemp", return_value=str(owned)), \
                    patch.object(qualification.subprocess, "Popen", side_effect=launch), \
                    patch.object(qualification.subprocess, "run", side_effect=[
                        subprocess.CompletedProcess([], 0, "a" * 40), subprocess.CompletedProcess([], 0, ""),
                    ]):
                result = qualification.run(args)
            self.assertEqual(result["functional"], "passed")
            census = json.loads((args.out / "resource-census.json").read_text())
            self.assertEqual(census["process_exit"], 0)
            self.assertTrue(census["cleanup"]["owned_parent_removed"])
            self.assertFalse(owned.exists())

    def test_failed_process_start_still_cleans_and_records_unknown_exit(self):
        with tempfile.TemporaryDirectory() as parent:
            base = Path(parent)
            binary = base / "pg_ctl"
            binary.write_text("fixture")
            binary.chmod(0o700)
            owned = base / "owned"
            owned.mkdir()
            args = SimpleNamespace(example_bin=binary, runtime_tests=binary, ack_tests=binary,
                                   pg_bin_dir=base, out=base / "evidence")
            with patch.object(qualification.tempfile, "mkdtemp", return_value=str(owned)), \
                    patch.object(qualification.subprocess, "Popen", side_effect=OSError("start failed")), \
                    patch.object(qualification.subprocess, "run", side_effect=[
                        subprocess.CompletedProcess([], 0, "a" * 40), subprocess.CompletedProcess([], 0, ""),
                    ]):
                with self.assertRaisesRegex(OSError, "start failed"):
                    qualification.run(args)
            census = json.loads((args.out / "resource-census.json").read_text())
            self.assertIsNone(census["process_exit"])
            self.assertTrue(census["cleanup"]["owned_parent_removed"])
            self.assertFalse(owned.exists())

    def test_wrapper_sigterm_stops_owned_child_and_records_cleanup(self):
        with tempfile.TemporaryDirectory() as parent:
            base = Path(parent)
            binary = base / "pg_ctl"
            binary.write_text(
                f"#!{sys.executable}\n"
                "import pathlib, sys, time\n"
                "out = pathlib.Path(sys.argv[sys.argv.index('--out') + 1])\n"
                "out.mkdir()\n"
                "(out / 'ready').touch()\n"
                "time.sleep(60)\n")
            binary.chmod(0o700)
            out = base / "evidence"
            process = subprocess.Popen([
                sys.executable, str(qualification.ROOT / "scripts/prism_ha_qualification.py"),
                "run", "--example-bin", str(binary), "--runtime-tests", str(binary),
                "--ack-tests", str(binary), "--pg-bin-dir", str(base), "--out", str(out),
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 10
                while not (out / "functional" / "ready").exists():
                    self.assertIsNone(process.poll(), "fixture wrapper exited before readiness")
                    self.assertLess(time.monotonic(), deadline, "fixture readiness timed out")
                    time.sleep(0.01)
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=10)
                self.assertNotEqual(process.returncode, 0)
                census = json.loads((out / "resource-census.json").read_text())
                self.assertEqual(census["process_exit"], -signal.SIGTERM)
                self.assertIn("interrupted", census["failure"])
                self.assertTrue(census["cleanup"]["owned_parent_removed"])
                self.assertFalse(Path(census["owned_parent"]).exists())
                with self.assertRaises(ProcessLookupError):
                    os.kill(census["process_pid"], 0)
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.communicate(timeout=10)

    def test_diagnostic_copy_failure_preserves_original_interruption(self):
        with tempfile.TemporaryDirectory() as parent:
            base = Path(parent)
            binary = base / "pg_ctl"
            binary.write_text("fixture")
            binary.chmod(0o700)
            owned = base / "owned"
            cluster = owned / "prism-load-fixture"
            cluster.mkdir(parents=True)
            (cluster / "primary.log").write_text("fixture diagnostics")
            args = SimpleNamespace(example_bin=binary, runtime_tests=binary, ack_tests=binary,
                                   pg_bin_dir=base, out=base / "evidence")
            process = SimpleNamespace(pid=42, returncode=-signal.SIGTERM, poll=lambda: -signal.SIGTERM,
                                      communicate=Mock(side_effect=[KeyboardInterrupt(), ("", "")]))
            with patch.object(qualification.tempfile, "mkdtemp", return_value=str(owned)), \
                    patch.object(qualification.subprocess, "Popen", return_value=process), \
                    patch.object(qualification.os, "killpg") as kill, \
                    patch.object(qualification.shutil, "copyfile", side_effect=OSError("copy failed")), \
                    patch.object(qualification.subprocess, "run", side_effect=[
                        subprocess.CompletedProcess([], 0, "a" * 40), subprocess.CompletedProcess([], 0, ""),
                    ]):
                with self.assertRaisesRegex(RuntimeError, "functional run failed"):
                    qualification.run(args)
            kill.assert_called_once_with(42, signal.SIGTERM)
            census = json.loads((args.out / "resource-census.json").read_text())
            self.assertIn("interrupted", census["failure"])
            self.assertIn("diagnostics", census["failure"])
            self.assertTrue(census["cleanup"]["owned_parent_removed"])


if __name__ == "__main__":
    unittest.main()

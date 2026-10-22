"""Shell launcher boundary; real SQLx/Compose checks live in the image suite."""
import json
import itertools
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
VALID = "PRISM public database configuration valid; authentication is checked by readiness"


class CheckEnvPublicCredentialLauncherTests(unittest.TestCase):
    def run_preflight(self, docker_body, *, settings=None, deploy=None, args=()):
        with tempfile.TemporaryDirectory(prefix="public-preflight-launcher-") as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "config").mkdir()
            shutil.copyfile(ROOT / "scripts/check-env.sh", root / "scripts/check-env.sh")
            shutil.copyfile(ROOT / ".env.example", root / ".env.example")
            shutil.copyfile(ROOT / "config/upstream.env.example", root / "config/upstream.env.example")
            docker = root / "docker"
            docker.write_text(f"#!{sys.executable}\nimport os, sys\n{docker_body}\n", encoding="utf-8")
            docker.chmod(0o755)
            env = {"PATH": f"{root}:{os.environ['PATH']}", **(settings or {})}
            if deploy is not None:
                env_file = root / "deployment.env"
                env_file.write_text(deploy, encoding="utf-8")
                env["DEPLOY_ENV_FILE"] = str(env_file)
            result = subprocess.run(
                ["/bin/bash", str(root / "scripts/check-env.sh"), "--public-reader-only", *args],
                cwd=root, env=env, capture_output=True, text=True, timeout=10,
            )
            self.assertFalse("fixture-secret" in result.stdout + result.stderr,
                             "launcher diagnostics exposed credential data")
            return result

    def test_original_exported_inputs_and_explicit_empty_overrides_reach_compose(self):
        for value in ("", "postgres://reader:fixture-secret@db/reader"):
            with self.subTest(empty=not value):
                result = self.run_preflight(
                    'assert os.environ["PRISM_PUBLIC_DATABASE_URL"] == os.environ["EXPECTED_DSN"]\n'
                    'assert os.environ["PRISM_PUBLIC_POSTGRES_PASSWORD"] == ""\n'
                    'assert os.environ["QBIT_PRODUCTION"] == "1"\n'
                    'assert "PRISM_MANIFEST_SIGNING_SEED_HEX" not in os.environ\n'
                    'assert not any("fixture-secret" in arg for arg in sys.argv)\n'
                    'assert sys.argv[-2:] == ["qbit-prism-server", "check-public-database-config"]\n'
                    'assert all(arg in sys.argv for arg in ["--rm", "--no-deps", "--pull", "never", "-T"])\n'
                    f'print({VALID!r})',
                    settings={"PRISM_PUBLIC_DATABASE_URL": value, "EXPECTED_DSN": value,
                              "PRISM_PUBLIC_POSTGRES_PASSWORD": "", "QBIT_PRODUCTION": "1"},
                    deploy="PRISM_PUBLIC_DATABASE_URL=postgres://file-reader:fixture-secret@db/file\n"
                           "PRISM_PUBLIC_POSTGRES_PASSWORD=fixture-secret-file\nQBIT_PRODUCTION=0\n",
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_env_file_only_inputs_stay_in_file_and_overlay_order_is_preserved(self):
        result = self.run_preflight(
            'assert "PRISM_PUBLIC_DATABASE_URL" not in os.environ\n'
            'assert "PRISM_PUBLIC_POSTGRES_PASSWORD" not in os.environ\n'
            'assert sys.argv[sys.argv.index("--env-file") + 1].endswith("upstream.env.example")\n'
            'assert sys.argv.index("compose.production.yaml") < sys.argv.index("compose.prism-external-db.yaml")\n'
            'assert sys.argv.index("compose.prism-external-db.yaml") < sys.argv.index("compose.prism-ha.yaml")\n'
            f'print({VALID!r})',
            deploy="PRISM_PUBLIC_DATABASE_URL=postgres://reader:fixture-secret@db/reader\n",
            args=("--compose-file", "compose.production.yaml", "--compose-file", "compose.prism-external-db.yaml",
                  "--compose-file", "compose.prism-ha.yaml"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_arbitrary_interpolation_inputs_keep_caller_precedence(self):
        result = self.run_preflight(
            'assert os.environ["READER_SECRET"] == "fixture-secret-caller"\n'
            f'print({VALID!r})',
            settings={"READER_SECRET": "fixture-secret-caller"},
            deploy="READER_SECRET=fixture-secret-file\n"
                   "PRISM_PUBLIC_POSTGRES_PASSWORD=${READER_SECRET}\n",
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unknown_launcher_failures_and_empty_results_fail_without_raw_diagnostics(self):
        for body in ('print("postgres://reader:fixture-secret@db/reader", file=sys.stderr)\nsys.exit(1)',
                     'print("fixture-secret")'):
            result = self.run_preflight(body)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("public reader validation", result.stderr)

    def test_config_errors_remain_distinct_from_launcher_failures(self):
        result = self.run_preflight(
            'print("Error: invalid public PRISM_DATABASE_URL: fixture-secret", file=sys.stderr)\nsys.exit(1)')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("public reader: invalid public PRISM_DATABASE_URL", result.stderr)

    def test_connection_options_keep_the_specific_value_free_diagnostic(self):
        result = self.run_preflight(
            'print("Error: invalid public PRISM_DATABASE_URL connection options: fixture-secret", file=sys.stderr)\nsys.exit(1)')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("public reader: invalid public PRISM_DATABASE_URL connection options", result.stderr)

    def test_make_lab_prepares_image_before_mandatory_validation(self):
        result = self.run_preflight(
            'from pathlib import Path\n'
            'prepared = Path("prepared")\n'
            'if "build" in sys.argv:\n'
            '    assert sys.argv[-1] == "prism-public-api"\n'
            '    prepared.touch()\n'
            'else:\n'
            '    assert prepared.exists()\n'
            '    assert "compose.prism-external-db.yaml" in sys.argv\n'
            f'    print({VALID!r})',
            args=("--make-deployment", "--compose-file", "compose.prism-external-db.yaml"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_production_selectors_never_build_and_apply_production_before_overlays(self):
        for settings, deploy in (({"QBIT_PRODUCTION": "1"}, None),
                                 ({"QBIT_TOOLS_PRODUCTION": "1"}, None),
                                 ({"QBIT_CHAIN": "mainnet"}, None),
                                 ({}, "QBIT_TOOLS_PRODUCTION=1\n")):
            with self.subTest(settings=settings, env_file=deploy is not None):
                result = self.run_preflight(
                    'assert "build" not in sys.argv and "pull" not in sys.argv\n'
                    'production = next(i for i, arg in enumerate(sys.argv) if arg.endswith("/compose.production.yaml"))\n'
                    'assert production < sys.argv.index("compose.prism-external-db.yaml")\n'
                    f'print({VALID!r})',
                    settings=settings, deploy=deploy,
                    args=("--make-deployment", "--compose-file", "compose.prism-external-db.yaml"),
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_lab_build_failure_is_not_skipped_and_never_exposes_build_output(self):
        result = self.run_preflight(
            'assert "build" in sys.argv\n'
            'print("fixture-secret", file=sys.stderr)\nsys.exit(1)', args=("--make-deployment",),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("public reader image preparation failed", result.stderr)

    def test_digest_pinned_lab_artifacts_are_validated_without_build_or_pull(self):
        result = self.run_preflight(
            'assert "build" not in sys.argv and "pull" not in sys.argv\n'
            f'print({VALID!r})',
            settings={"PRISM_COORDINATOR_IMAGE": "fixture/prism@sha256:" + "a" * 64},
            args=("--make-deployment",),
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_lab_preparation_still_rejects_invalid_qbit_chain_selection(self):
        result = self.run_preflight('raise AssertionError("Docker must not run")',
                                    settings={"QBIT_CHAIN": "regtest", "QBIT_CHAIN_FLAG": "-chain=main"},
                                    args=("--make-deployment",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_CHAIN=regtest requires QBIT_CHAIN_FLAG=-regtest", result.stderr)

    def test_real_make_doctor_and_launch_share_overlays_and_prepare_fresh_image(self):
        for target, bitcoin_chain in itertools.product(("doctor", "up-prism-pool"), ("regtest", "mainnet", "testnet4")):
            with self.subTest(target=target, bitcoin_chain=bitcoin_chain), tempfile.TemporaryDirectory(prefix="public-reader-make-") as directory:
                root = Path(directory)
                for name in ("Makefile", "scripts/check-env.sh", ".env.example", "config/upstream.env.example",
                             "docker/qbit/qbit-entrypoint.sh"):
                    destination = root / name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(ROOT / name, destination)
                for name in ("CMakeLists.txt", "src/CMakeLists.txt", "test/functional/test_framework/auxpow.py"):
                    path = root / "qbit" / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.touch()
                (root / "scripts/prepare-qbit-source.sh").write_text(
                    '#!/bin/sh\nprintf "%s/qbit\\n" "$PWD"\n', encoding="utf-8")
                (root / "bin").mkdir()
                docker = root / "bin/docker"
                docker.write_text(
                    f'#!{sys.executable}\nimport json, sys\nfrom pathlib import Path\n'
                    'with Path("calls.jsonl").open("a") as log: log.write(json.dumps(sys.argv[1:]) + "\\n")\n'
                    'if "build" in sys.argv: Path("prepared").touch()\n'
                    'if "run" in sys.argv:\n'
                    '    assert Path("prepared").exists()\n'
                    f'    print({VALID!r})\n'
                    'if "--environment" in sys.argv:\n'
                    '    print("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX=fixture-public-key")\n'
                    '    print("PRISM_MANIFEST_SIGNING_SEED_HEX=fixture-test-seed")\n'
                    '    print("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX=fixture-test-seed")\n', encoding="utf-8")
                docker.chmod(0o755)
                overlays = ["compose.prism-external-db.yaml", "compose.prism-ha.yaml"]
                result = subprocess.run(
                    ["make", "--no-print-directory", target, "MINING_LANES=prism", "QBIT_PROVIDER=source",
                     f"COMPOSE_OVERLAY_FILES={' '.join(overlays)}"], cwd=root,
                    # Disabled Bitcoin-lane settings must not gate a PRISM lab.
                    env={"PATH": f"{root / 'bin'}:{os.environ['PATH']}", "BITCOIN_CHAIN": bitcoin_chain,
                         "BITCOIN_CHAIN_FLAG": "-regtest"},
                    text=True, capture_output=True, timeout=15,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]
                build = next(i for i, args in enumerate(calls) if "build" in args)
                validation = next(i for i, args in enumerate(calls) if "run" in args)
                self.assertLess(build, validation)
                for args in calls:
                    if args[0] != "compose":
                        continue
                    self.assertLess(args.index(overlays[0]), args.index(overlays[1]))
                if target == "up-prism-pool":
                    launch = next(i for i, args in enumerate(calls) if "up" in args)
                    self.assertLess(validation, launch)
                    self.assertNotIn("prism-postgres", calls[launch])


if __name__ == "__main__":
    unittest.main()

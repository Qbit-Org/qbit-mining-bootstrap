"""Shell launcher boundary; real SQLx/Compose checks live in the image suite."""
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


if __name__ == "__main__":
    unittest.main()

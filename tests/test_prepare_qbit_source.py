#!/usr/bin/env python3

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "prepare-qbit-source.sh"


class PrepareQbitSourceTests(unittest.TestCase):
    def make_checkout(self, root: Path) -> tuple[Path, str]:
        checkout = root / "qbit"
        for relative in (
            "CMakeLists.txt",
            "src/CMakeLists.txt",
            "test/functional/test_framework/auxpow.py",
        ):
            path = checkout / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"committed:{relative}\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q", str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(checkout),
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            check=True,
        )
        commit = subprocess.check_output(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            text=True,
        ).strip()
        return checkout, commit

    def run_script(
        self,
        checkout: Path,
        *,
        script: Path = SCRIPT,
        **overrides: str,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "QBIT_PROVIDER": "source",
                "QBIT_SRC_DIR": str(checkout),
                "QBIT_PRODUCTION": "1",
                "QBIT_CHAIN": "mainnet",
                **overrides,
            }
        )
        return subprocess.run(
            ["/bin/bash", str(script)],
            cwd=script.parents[1],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def isolated_script_root(self, directory: Path) -> tuple[Path, Path]:
        root = directory / "bootstrap"
        (root / "scripts").mkdir(parents=True)
        (root / "config").mkdir()
        script = root / "scripts" / "prepare-qbit-source.sh"
        shutil.copyfile(SCRIPT, script)
        shutil.copyfile(ROOT / ".env.example", root / ".env.example")
        shutil.copyfile(ROOT / "config" / "upstream.env.example", root / "config" / "upstream.env")
        return root, script

    def test_production_source_stages_exact_commit_without_dirty_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            checkout, commit = self.make_checkout(Path(tmp))
            (checkout / "CMakeLists.txt").write_text("dirty\n", encoding="utf-8")
            (checkout / "untracked-secret.txt").write_text("do not stage\n", encoding="utf-8")

            result = self.run_script(checkout, QBIT_GIT_COMMIT=commit.upper())

            self.assertEqual(result.returncode, 0, result.stderr)
            staged = Path(result.stdout.strip())
            self.assertEqual(
                (staged / "CMakeLists.txt").read_text(encoding="utf-8"),
                "committed:CMakeLists.txt\n",
            )
            self.assertFalse((staged / "untracked-secret.txt").exists())
            self.assertEqual((staged / ".qbit-source-commit").read_text().strip(), commit)

    def test_production_source_requires_full_commit_pin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            checkout, _ = self.make_checkout(Path(tmp))

            result = self.run_script(checkout, QBIT_GIT_COMMIT="")

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requires QBIT_GIT_COMMIT as exactly 40 hex characters", result.stderr)

    def test_deploy_env_file_supplies_the_source_pin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checkout, commit = self.make_checkout(root)
            deploy_env = root / "mainnet.env"
            deploy_env.write_text(f"QBIT_GIT_COMMIT={commit}\n", encoding="utf-8")

            result = self.run_script(checkout, DEPLOY_ENV_FILE=str(deploy_env))

            self.assertEqual(result.returncode, 0, result.stderr)
            staged = Path(result.stdout.strip())
            self.assertEqual((staged / ".qbit-source-commit").read_text().strip(), commit)

    def test_deployment_env_omits_repository_env_from_source_staging(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            checkout, _ = self.make_checkout(directory)
            root, script = self.isolated_script_root(directory)
            (root / ".env").write_text(f"QBIT_GIT_COMMIT={'f' * 40}\n", encoding="utf-8")
            deploy_env = root / "mainnet.env"
            deploy_env.write_text("", encoding="utf-8")

            result = self.run_script(
                checkout,
                script=script,
                DEPLOY_ENV_FILE=str(deploy_env),
                QBIT_GIT_COMMIT="",
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires QBIT_GIT_COMMIT as exactly 40 hex characters", result.stderr)
        self.assertNotIn("is not present in qbit source", result.stderr)

    def test_external_environment_overrides_deployment_source_pin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checkout, commit = self.make_checkout(root)
            deploy_env = root / "mainnet.env"
            deploy_env.write_text(f"QBIT_GIT_COMMIT={'f' * 40}\n", encoding="utf-8")

            result = self.run_script(
                checkout,
                DEPLOY_ENV_FILE=str(deploy_env),
                QBIT_GIT_COMMIT=commit,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            staged = Path(result.stdout.strip())
            self.assertEqual((staged / ".qbit-source-commit").read_text().strip(), commit)


if __name__ == "__main__":
    unittest.main()

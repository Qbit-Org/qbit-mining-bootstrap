#!/usr/bin/env python3
"""Contract tests for the pinned qbit installer the PRISM native CI job runs.

The installer only ever fetches from GitHub in CI, so these tests drive it
against a loopback HTTP server that plays the release download route and the
release-asset API route, each scripted to answer with the pinned bytes, other
bytes, or a gateway error. That keeps the fallback order, the digest check
before extraction, and the cache handling from regressing without a red test.
"""

from __future__ import annotations

import hashlib
import http.server
import io
import os
import re
import subprocess
import tarfile
import tempfile
import threading
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".github" / "scripts" / "install-prism-qbit.sh"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"

VERSION = "1.0.0"
ASSET = f"qbit-{VERSION}-x86_64-linux-gnu.tar.gz"
ASSET_ID = "478569140"
PINNED_SHA256 = "ae121af03263b55d530e3f3e8719a71362d0950cba82f54a2bc2d6c437a029b5"
RELEASE_PATH = f"/release/{ASSET}"
API_PATH = f"/api/assets/{ASSET_ID}"


def make_tarball(qbitd_body: str) -> bytes:
    """A release-shaped tarball: qbit-<version>/bin/qbitd, executable."""
    payload = qbitd_body.encode("utf-8")
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo(f"qbit-{VERSION}/bin/qbitd")
        info.size = len(payload)
        info.mode = 0o755
        archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


class RouteHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server API
        self.server.requests.append((self.path, self.headers.get("Accept")))
        response = self.server.responses.get(self.path)
        if response is None:
            body = b"no such route"
            self.send_response(404)
        elif isinstance(response, int):
            body = b"gateway timeout"
            self.send_response(response)
        else:
            body = response
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        pass


class RouteServer(http.server.ThreadingHTTPServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), RouteHandler)
        self.responses: dict[str, bytes | int] = {}
        self.requests: list[tuple[str, str | None]] = []

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}{path}"

    def count(self, path: str) -> int:
        return sum(1 for seen, _accept in self.requests if seen == path)


class InstallPrismQbitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.good = make_tarball("#!/bin/sh\necho pinned\n")
        self.good_sha256 = hashlib.sha256(self.good).hexdigest()
        self.other = make_tarball("#!/bin/sh\necho other\n")
        self.assertNotEqual(self.good_sha256, hashlib.sha256(self.other).hexdigest())
        self.server = RouteServer()
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.tmp = Path(tempfile.mkdtemp(prefix="install-prism-qbit-"))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.tmp)], check=False))
        self.destination = self.tmp / "dest"
        self.cache = self.tmp / "cache"
        self.github_env = self.tmp / "github.env"
        # Exercise the production script with fixture pins in a disposable
        # copy; the shipped installer has no environment override for them.
        script = SCRIPT.read_text(encoding="utf-8")
        substitutions = {
            "sha256": self.good_sha256,
            "release_url": self.server.url(RELEASE_PATH),
            "asset_api_url": self.server.url(API_PATH),
            "retries": "1",
            "retry_max_time": "5",
        }
        for name, value in substitutions.items():
            script, count = re.subn(rf"(?m)^{name}=.*$", f'{name}="{value}"', script)
            self.assertEqual(count, 1, name)
        self.script = self.tmp / "install-prism-qbit.sh"
        self.script.write_text(script, encoding="utf-8")

    def run_script(self, *, cache: bool = True) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        for proxy in ("http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"):
            env.pop(proxy, None)
        env.update(
            {
                "GITHUB_ENV": str(self.github_env),
            }
        )
        if cache:
            env["PRISM_QBIT_CACHE_DIR"] = str(self.cache)
        else:
            env.pop("PRISM_QBIT_CACHE_DIR", None)
        return subprocess.run(
            ["bash", str(self.script), str(self.destination)],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    @property
    def qbitd(self) -> Path:
        return self.destination / f"qbit-{VERSION}" / "bin" / "qbitd"

    def assert_installed(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(os.access(self.qbitd, os.X_OK), self.qbitd)
        self.assertEqual(self.qbitd.read_text(encoding="utf-8"), "#!/bin/sh\necho pinned\n")
        self.assertEqual(
            self.github_env.read_text(encoding="utf-8"),
            f"QBITD_BIN={self.qbitd}\n",
        )

    def test_release_route_download_is_verified_extracted_and_cached(self) -> None:
        self.server.responses[RELEASE_PATH] = self.good
        self.server.responses[API_PATH] = self.good

        result = self.run_script()

        self.assert_installed(result)
        self.assertEqual(self.server.count(RELEASE_PATH), 1, self.server.requests)
        self.assertEqual(self.server.count(API_PATH), 0, self.server.requests)
        self.assertEqual(
            hashlib.sha256((self.cache / ASSET).read_bytes()).hexdigest(),
            self.good_sha256,
        )
        self.assertFalse((self.cache / f"{ASSET}.partial").exists())

    def test_works_without_a_cache_directory(self) -> None:
        self.server.responses[RELEASE_PATH] = self.good

        result = self.run_script(cache=False)

        self.assert_installed(result)
        self.assertFalse(self.cache.exists())

    def test_gateway_error_on_release_route_falls_back_to_the_asset_api(self) -> None:
        self.server.responses[RELEASE_PATH] = 504
        self.server.responses[API_PATH] = self.good

        result = self.run_script()

        self.assert_installed(result)
        # The original request plus the one configured retry, then the API.
        self.assertEqual(self.server.count(RELEASE_PATH), 2, self.server.requests)
        self.assertEqual(self.server.count(API_PATH), 1, self.server.requests)
        api_accept = [accept for path, accept in self.server.requests if path == API_PATH]
        self.assertEqual(api_accept, ["application/octet-stream"])
        self.assertIn("release download route", result.stderr)
        self.assertIn("release-asset API route", result.stderr)

    def test_release_route_bytes_with_the_wrong_digest_are_rejected_before_extraction(
        self,
    ) -> None:
        self.server.responses[RELEASE_PATH] = self.other
        self.server.responses[API_PATH] = self.good

        result = self.run_script()

        self.assert_installed(result)
        self.assertEqual(self.server.count(RELEASE_PATH), 1, self.server.requests)
        self.assertEqual(self.server.count(API_PATH), 1, self.server.requests)
        self.assertIn("digest mismatch", result.stderr)
        self.assertIn(f"expected {self.good_sha256}", result.stderr)

    def test_fails_when_every_route_serves_the_wrong_digest(self) -> None:
        self.server.responses[RELEASE_PATH] = self.other
        self.server.responses[API_PATH] = self.other

        result = self.run_script()

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.qbitd.exists())
        self.assertFalse((self.cache / ASSET).exists())
        self.assertEqual(result.stderr.count("digest mismatch"), 2, result.stderr)
        self.assertIn("both failed", result.stderr)
        self.assertEqual(self.github_env.exists(), False)

    def test_fails_when_every_route_is_unavailable(self) -> None:
        self.server.responses[RELEASE_PATH] = 504
        self.server.responses[API_PATH] = 502

        result = self.run_script()

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.qbitd.exists())
        self.assertFalse(self.cache.exists())
        self.assertIn("the release download route did not deliver", result.stderr)
        self.assertIn("the release-asset API route did not deliver", result.stderr)
        self.assertIn("both failed", result.stderr)

    def test_verified_cached_tarball_needs_no_download(self) -> None:
        self.cache.mkdir()
        (self.cache / ASSET).write_bytes(self.good)
        self.server.responses[RELEASE_PATH] = 504
        self.server.responses[API_PATH] = 504

        result = self.run_script()

        self.assert_installed(result)
        self.assertEqual(self.server.requests, [])
        self.assertIn("using the cached", result.stderr)

    def test_cached_tarball_with_the_wrong_digest_is_replaced_not_extracted(self) -> None:
        self.cache.mkdir()
        (self.cache / ASSET).write_bytes(self.other)
        self.server.responses[RELEASE_PATH] = self.good

        result = self.run_script()

        self.assert_installed(result)
        self.assertIn("discarding the cached", result.stderr)
        self.assertEqual(self.server.count(RELEASE_PATH), 1, self.server.requests)
        self.assertEqual(
            hashlib.sha256((self.cache / ASSET).read_bytes()).hexdigest(),
            self.good_sha256,
        )

    def test_script_pins_the_v1_release_asset_and_both_github_routes(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        self.assertRegex(script, rf"(?m)^version={re.escape(VERSION)}$")
        self.assertRegex(script, rf"(?m)^asset_id={ASSET_ID}$")
        self.assertRegex(script, rf"(?m)^sha256={PINNED_SHA256}$")
        self.assertEqual(set(re.findall(r"PRISM_QBIT_\w+", script)), {"PRISM_QBIT_CACHE_DIR"})
        self.assertIn(
            "https://github.com/Qbit-Org/qbit/releases/download/v${version}/${asset}",
            script,
        )
        self.assertIn(
            "https://api.github.com/repos/Qbit-Org/qbit/releases/assets/${asset_id}",
            script,
        )
        self.assertIn("--header 'Accept: application/octet-stream'", script)
        self.assertIn("--retry-all-errors", script)
        self.assertNotIn("Authorization", script)
        self.assertNotIn("GITHUB_TOKEN", script)
        self.assertTrue(os.access(SCRIPT, os.X_OK), SCRIPT)

    def test_ci_native_job_restores_installs_and_saves_the_pinned_tarball(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        native = workflow.split("\n  prism-native-postgres:", 1)[1].split("\n  docker-builds:", 1)[0]
        self.assertIn("uses: actions/cache/restore@", native)
        self.assertIn("uses: actions/cache/save@", native)
        self.assertEqual(
            native.count("hashFiles('.github/scripts/install-prism-qbit.sh')"), 2, native
        )
        self.assertIn("if: steps.prism-qbit-cache.outputs.cache-hit != 'true'", native)
        self.assertIn(
            'PRISM_QBIT_CACHE_DIR="${HOME}/.cache/prism-qbit" \\\n'
            '            bash .github/scripts/install-prism-qbit.sh "${RUNNER_TEMP}/prism-qbit"',
            native,
        )
        # CI must run the pins as written in the script: only the cache
        # directory is configured from the workflow.
        self.assertEqual(set(re.findall(r"PRISM_QBIT_\w+", native)), {"PRISM_QBIT_CACHE_DIR"})
        # The inline download the script replaced must not come back.
        self.assertNotIn("releases/download/v1.0.0", native)
        self.assertNotIn(PINNED_SHA256, native)


if __name__ == "__main__":
    unittest.main()

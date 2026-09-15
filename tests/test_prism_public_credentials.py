#!/usr/bin/env python3
"""Public-reader credential boundary through real Compose merges and startup.

Only synthetic inputs are rendered; neither the caller's .env nor its database
settings are read. Image integration is enabled by PRISM_CREDENTIAL_TEST_IMAGE
in the existing image smoke gate (or explicitly by a local caller).
"""

from __future__ import annotations

import itertools
import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_PASSWORD = "fixture-bootstrap-only-383"
READER_PASSWORD = "fixture-reader-only-383"
WRITER_URL = f"postgresql://prism_bootstrap:{BOOTSTRAP_PASSWORD}@prism-postgres:5432/prism_fixture"
READER_URL = f"postgresql://prism_reader:{READER_PASSWORD}@prism-postgres-replica:5432/prism_fixture"
PASSWORDLESS_URL = "postgresql://prism_reader@prism-postgres-replica:5432/prism_fixture"
STACKS = tuple(
    tuple(name for enabled, name in zip(flags, (
        "compose.production.yaml", "compose.prism-external-db.yaml", "compose.prism-ha.yaml",
    )) if enabled)
    for flags in itertools.product((False, True), repeat=3)
)


def fixture_env(*, production: bool = False, **overrides: str) -> dict[str, str]:
    inherited = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "XDG_CONFIG_HOME")
    return {
        **{key: os.environ[key] for key in inherited if key in os.environ},
        "QBIT_SRC_DIR": str(ROOT),
        "QBIT_PRODUCTION": "1" if production else "0",
        "QBIT_CHAIN": "mainnet" if production else "regtest",
        "QBIT_RPC_PASSWORD": "fixture-rpc-only-383",
        "PRISM_POSTGRES_USER": "prism_bootstrap",
        "PRISM_POSTGRES_PASSWORD": BOOTSTRAP_PASSWORD,
        "PRISM_POSTGRES_DB": "prism_fixture",
        "PRISM_DATABASE_URL": WRITER_URL,
        "PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://pool.example.invalid:3340",
        # Ambient libpq credentials must not become container credentials.
        "PGPASSWORD": "fixture-ambient-password-383",
        **overrides,
    }


def compose_args(stack: tuple[str, ...], extra_file: str | None = None,
                 project: str = "prism-reader-credentials") -> list[str]:
    args = ["docker", "compose", "--env-file", str(ROOT / "config/upstream.env.example")]
    for name in ("compose.yaml", *stack):
        args.extend(("-f", str(ROOT / name)))
    if extra_file:
        args.extend(("-f", extra_file))
    return [*args, "--project-name", project, "--profile", "prism"]


def render_public(stack: tuple[str, ...], **overrides: str) -> dict:
    result = subprocess.run(
        [*compose_args(stack), "config", "--format", "json"],
        cwd=ROOT, env=fixture_env(production="compose.production.yaml" in stack, **overrides),
        text=True, capture_output=True, check=True,
    )
    # Never print the complete rendered stack: assertions concern this service.
    return json.loads(result.stdout)["services"]["prism-public-api"]


class PublicCredentialComposeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if shutil.which("docker") is None:
            raise unittest.SkipTest("docker CLI is not installed")
        subprocess.run(["docker", "compose", "version"], check=True, capture_output=True)

    def assert_reader_boundary(self, public: dict, url: str, password: str = "") -> None:
        environment = public["environment"]
        self.assertEqual(environment["PRISM_DATABASE_URL"], url)
        self.assertEqual(environment.get("PGPASSWORD", ""), password)
        self.assertNotIn("PRISM_POSTGRES_PASSWORD", environment)
        self.assertNotIn("PRISM_PUBLIC_POSTGRES_PASSWORD", environment)
        self.assertNotIn(BOOTSTRAP_PASSWORD, json.dumps(public))
        self.assertNotIn("fixture-ambient-password-383", json.dumps(public))
        self.assertEqual(public["command"], ["qbit-prism-server", "public-api"])
        self.assertEqual(
            public["healthcheck"]["test"],
            ["CMD", "qbit-prism-server", "healthcheck", "--public-api"],
        )

    def test_dedicated_dsn_excludes_bootstrap_password_in_every_stack(self) -> None:
        for stack in STACKS:
            with self.subTest(stack=stack):
                self.assert_reader_boundary(
                    render_public(stack, PRISM_PUBLIC_DATABASE_URL=READER_URL), READER_URL,
                )

    def test_separate_carrier_has_only_public_reader_provenance(self) -> None:
        for stack, password in itertools.product(STACKS, (None, "", "fixture-bad-reader", READER_PASSWORD)):
            with self.subTest(stack=stack, password=password):
                overrides = {"PRISM_PUBLIC_DATABASE_URL": PASSWORDLESS_URL}
                if password is not None:
                    overrides["PRISM_PUBLIC_POSTGRES_PASSWORD"] = password
                self.assert_reader_boundary(render_public(stack, **overrides), PASSWORDLESS_URL, password or "")

    def test_missing_and_empty_public_dsn_preserve_supported_defaults(self) -> None:
        for stack, url in itertools.product(STACKS, (None, "")):
            with self.subTest(stack=stack, url=url):
                overrides = {} if url is None else {"PRISM_PUBLIC_DATABASE_URL": url}
                public = render_public(stack, **overrides)
                external = "compose.prism-external-db.yaml" in stack
                expected = WRITER_URL if external else WRITER_URL.replace("@prism-postgres:", "@prism-postgres-replica:")
                self.assertEqual(public["environment"]["PRISM_DATABASE_URL"], expected)
                self.assertEqual(public["environment"].get("PGPASSWORD", ""), "")
                self.assertEqual(public["environment"]["PRISM_PUBLIC_REPLICA_MODE"], "off" if external else "require")


@unittest.skipUnless(os.environ.get("PRISM_CREDENTIAL_TEST_IMAGE"), "set PRISM_CREDENTIAL_TEST_IMAGE for disposable image integration")
class PublicCredentialRuntimeTests(unittest.TestCase):
    """Run the actual merged public service, image entrypoint and HTTP probe."""

    @classmethod
    def command(cls, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=ROOT, env=fixture_env(), text=True,
                              capture_output=True, check=check, timeout=180)

    @classmethod
    def sql(cls, query: str, *, reader: bool = False, check: bool = True) -> subprocess.CompletedProcess:
        return cls.command(
            "docker", "exec", "--env", f"PGPASSWORD={READER_PASSWORD if reader else BOOTSTRAP_PASSWORD}",
            cls.primary, "psql", "-h", "127.0.0.1", "-U", "prism_reader" if reader else "prism_bootstrap",
            "-d", "prism_fixture", "-v", "ON_ERROR_STOP=1", "-Atc", query, check=check,
        )

    @classmethod
    def setUpClass(cls) -> None:
        cls.image = os.environ["PRISM_CREDENTIAL_TEST_IMAGE"]
        prefix = f"prism-reader-383-{uuid.uuid4().hex[:10]}"
        cls.project = prefix
        cls.network = f"{prefix}-net"
        cls.primary, cls.replica, cls.public = (f"{prefix}-{role}" for role in ("primary", "replica", "public"))
        cls.addClassCleanup(cls.command, "docker", "network", "rm", cls.network, check=False)
        for container in (cls.primary, cls.replica, cls.public):
            cls.addClassCleanup(cls.command, "docker", "rm", "--force", "--volumes", container, check=False)
        cls.command("docker", "network", "create", cls.network)
        cls.command(
            "docker", "run", "-d", "--name", cls.primary, "--network", cls.network,
            "--network-alias", "prism-postgres", "--env", "POSTGRES_USER=prism_bootstrap",
            "--env", f"POSTGRES_PASSWORD={BOOTSTRAP_PASSWORD}", "--env", "POSTGRES_DB=prism_fixture",
            "--env", "POSTGRES_HOST_AUTH_METHOD=scram-sha-256", "postgres:16",
        )
        for _ in range(60):
            if cls.sql("SELECT 1", check=False).returncode == 0:
                break
            time.sleep(1)
        else:
            raise AssertionError("disposable PostgreSQL did not become ready")
        cls.command("docker", "run", "--rm", "--network", cls.network, "--no-healthcheck",
                    "--env", f"PRISM_DATABASE_URL={WRITER_URL}", cls.image, "qbit-prism-server", "migrate")
        cls.sql(f"""
            CREATE ROLE prism_reader LOGIN PASSWORD '{READER_PASSWORD}'
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
            GRANT CONNECT ON DATABASE prism_fixture TO prism_reader;
            GRANT USAGE ON SCHEMA public TO prism_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO prism_reader;
            GRANT pg_read_all_stats TO prism_reader;
        """)
        # Use the shipped standby entrypoint against a disposable primary only.
        cls.command("docker", "exec", cls.primary, "sh", "-c",
                    'printf "host replication prism_bootstrap all scram-sha-256\\n" >> "$PGDATA/pg_hba.conf"')
        cls.sql("SELECT pg_reload_conf()")
        cls.command(
            "docker", "run", "-d", "--name", cls.replica, "--network", cls.network,
            "--network-alias", "prism-postgres-replica", "--env", "PRISM_POSTGRES_USER=prism_bootstrap",
            "--env", f"PRISM_POSTGRES_PASSWORD={BOOTSTRAP_PASSWORD}", "--env", "PRISM_POSTGRES_DB=prism_fixture",
            "--volume", f"{ROOT / 'config/prism-postgres/replica-entrypoint.sh'}:/replica-entrypoint.sh:ro",
            "--entrypoint", "bash", "postgres:16", "/replica-entrypoint.sh",
        )
        for _ in range(90):
            result = cls.command("docker", "exec", cls.replica, "psql", "-U", "prism_bootstrap",
                                 "-d", "prism_fixture", "-Atc", "SELECT pg_is_in_recovery()", check=False)
            if result.returncode == 0 and result.stdout.strip() == "t":
                break
            time.sleep(1)
        else:
            raise AssertionError("disposable standby did not become ready")

    def start_public(self, stack: tuple[str, ...], *, lab_default: bool = False, **overrides: str) -> None:
        self.command("docker", "rm", "--force", self.public, check=False)
        # Only adapt placement: retain the merged environment, command,
        # healthcheck and the image's real entrypoint. No production services,
        # host data mounts or published host ports are started by this test.
        with tempfile.TemporaryDirectory(prefix="prism-reader-compose-") as directory:
            overlay = Path(directory) / "runtime.yaml"
            overlay.write_text(
                "services:\n  prism-public-api:\n"
                f"    container_name: {self.public}\n"
                "    restart: 'no'\n    pull_policy: never\n"
                "    ports: !override []\n    networks: !override [fixture]\n"
                "networks:\n  fixture:\n    external: true\n"
                f"    name: {self.network}\n", encoding="utf-8",
            )
            subprocess.run(
                [*compose_args(stack, str(overlay), self.project), "up", "--detach", "--no-deps", "--no-build",
                 "--pull", "never", "prism-public-api"],
                cwd=ROOT, env=fixture_env(production="compose.production.yaml" in stack,
                                         PRISM_COORDINATOR_IMAGE=self.image, **overrides),
                text=True, capture_output=True, check=True, timeout=90,
            )
        config = self.command("docker", "inspect", "--format", "{{json .Config}}", self.public).stdout
        if not lab_default:
            self.assertNotIn(BOOTSTRAP_PASSWORD, config)
        self.assertNotIn("fixture-ambient-password-383", config)
        if lab_default:
            self.assertIn("PGPASSWORD=", json.loads(config)["Env"])

    def response(self, path: str) -> tuple[int, dict]:
        result = self.command("docker", "exec", self.public, "curl", "--silent", "--show-error",
                              "--max-time", "2", "--write-out", "\n%{http_code}",
                              f"http://127.0.0.1:3342{path}")
        body, code = result.stdout.rsplit("\n", 1)
        return int(code), json.loads(body)

    def assert_health(self, healthy: bool) -> None:
        deadline = time.monotonic() + 30
        while True:
            try:
                status, body = self.response("/healthz")
                if body.get("state") != "starting":
                    break
            except subprocess.CalledProcessError:
                pass
            if time.monotonic() >= deadline:
                self.fail("public HTTP listener did not report readiness within 30 seconds")
            time.sleep(0.25)
        self.assertEqual(status, 200 if healthy else 503, body)
        self.assertIs(body["ok"], healthy, body)
        self.assertIs(body["database_ready"], healthy, body)
        if not healthy:
            self.assertIn("password authentication failed", body.get("error", ""), body)
        self.assertNotIn(BOOTSTRAP_PASSWORD, json.dumps(body))
        self.assertNotIn(READER_PASSWORD, json.dumps(body))
        probe = self.command("docker", "exec", self.public, "qbit-prism-server", "healthcheck", "--public-api", check=False)
        self.assertEqual(probe.returncode == 0, healthy, probe.stderr)
        # Blocks exercises application-table reads without requiring a node RPC.
        status, body = self.response("/public/v1/blocks")
        self.assertEqual(status, 200 if healthy else 503, body)

    def test_reader_dsn_and_http_health_in_every_stack(self) -> None:
        for stack in STACKS:
            with self.subTest(stack=stack):
                self.start_public(stack, PRISM_PUBLIC_DATABASE_URL=READER_URL)
                self.assert_health(True)

    def test_supported_default_dsns_still_connect(self) -> None:
        for stack in ((), ("compose.prism-external-db.yaml",)):
            with self.subTest(stack=stack):
                self.start_public(stack, lab_default=True)
                self.assert_health(True)

    def test_passwordless_dsn_authentication_and_dsn_precedence(self) -> None:
        stack = ("compose.production.yaml", "compose.prism-external-db.yaml", "compose.prism-ha.yaml")
        for password in (None, "", "fixture-bad-reader", READER_PASSWORD):
            with self.subTest(password=password):
                overrides = {"PRISM_PUBLIC_DATABASE_URL": PASSWORDLESS_URL}
                if password is not None:
                    overrides["PRISM_PUBLIC_POSTGRES_PASSWORD"] = password
                self.start_public(stack, **overrides)
                self.assert_health(password == READER_PASSWORD)
        # An explicitly incorrect DSN password must not be repaired by even a
        # valid reader carrier; the DSN is authoritative. An empty query
        # password likewise wins over the carrier in SQLx.
        for password in ("fixture-bad-reader", ""):
            with self.subTest(dsn_password=password):
                self.start_public(stack, PRISM_PUBLIC_DATABASE_URL=f"{PASSWORDLESS_URL}?password={password}",
                                  PRISM_PUBLIC_POSTGRES_PASSWORD=READER_PASSWORD)
                self.assert_health(False)

    def test_reader_cannot_write_even_without_session_read_only_guard(self) -> None:
        self.assertEqual(self.sql("SELECT count(*) FROM qbit_pool_blocks", reader=True).returncode, 0)
        denied = self.sql("SET default_transaction_read_only=off; UPDATE qbit_pool_blocks SET inactive_since=inactive_since WHERE false",
                          reader=True, check=False)
        self.assertNotEqual(denied.returncode, 0)
        self.assertIn("permission denied", denied.stderr)
        self.assertEqual(self.sql("SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication FROM pg_roles WHERE rolname=current_user", reader=True).stdout.strip(), "f")


if __name__ == "__main__":
    unittest.main()

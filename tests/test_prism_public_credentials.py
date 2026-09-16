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
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_PASSWORD = "fixture-bootstrap-only-383"
READER_PASSWORD = "fixture-reader-only-383"
WRITER_URL = f"postgresql://prism_bootstrap:{BOOTSTRAP_PASSWORD}@prism-postgres:5432/prism_fixture"
READER_URL = f"postgresql://prism_reader:{READER_PASSWORD}@prism-postgres-replica:5432/prism_fixture"
PASSWORDLESS_URL = "postgresql://prism_reader@prism-postgres-replica:5432/prism_fixture"
LITERAL_PASSWORD = "reader$MISSING #:@/383\\tail"
LITERAL_PASSWORDLESS_URL = PASSWORDLESS_URL.replace("prism_reader@", "prism_literal_reader@")
LITERAL_URL = LITERAL_PASSWORDLESS_URL.replace("prism_literal_reader@", f"prism_literal_reader:{quote(LITERAL_PASSWORD, safe='')}@")
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
                 project: str = "prism-reader-credentials", env_file: Path | None = None) -> list[str]:
    args = ["docker", "compose", "--env-file", str(ROOT / "config/upstream.env.example")]
    if env_file:
        args.extend(("--env-file", str(env_file)))
    for name in ("compose.yaml", *stack):
        args.extend(("-f", str(ROOT / name)))
    if extra_file:
        args.extend(("-f", extra_file))
    return [*args, "--project-name", project, "--profile", "prism"]


def render_public(stack: tuple[str, ...], *, env_file: Path | None = None, **overrides: str) -> dict:
    result = subprocess.run(
        [*compose_args(stack, env_file=env_file), "config", "--format", "json"],
        cwd=ROOT, env=fixture_env(production="compose.production.yaml" in stack, **overrides),
        text=True, capture_output=True, check=False,
    )
    if result.returncode:
        raise AssertionError(f"synthetic Compose render failed: {result.stderr}")
    # Never print the complete rendered stack: assertions concern this service.
    return json.loads(result.stdout)["services"]["prism-public-api"]


class PublicCredentialComposeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if shutil.which("docker") is None:
            raise unittest.SkipTest("docker CLI is not installed")
        result = subprocess.run(["docker", "compose", "version"], text=True, capture_output=True)
        if result.returncode and "not a docker command" in result.stderr:
            raise unittest.SkipTest("docker compose is unavailable")
        if result.returncode:
            raise AssertionError(f"docker compose version failed: {result.stderr}")

    def assert_reader_boundary(self, public: dict, url: str, password: str = "") -> None:
        environment = public["environment"]
        self.assertEqual(environment["PRISM_DATABASE_URL"], url)
        # Compose escapes dollars when serializing its reusable config; actual
        # container values are checked verbatim by the runtime test below.
        self.assertEqual(environment["PGPASSWORD"], password.replace("$", "$$"))
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
        for stack, url, password in itertools.product(STACKS, (None, ""), ("", READER_PASSWORD)):
            with self.subTest(stack=stack, url=url, carrier_set=bool(password)):
                overrides = {} if url is None else {"PRISM_PUBLIC_DATABASE_URL": url}
                overrides["PRISM_PUBLIC_POSTGRES_PASSWORD"] = password
                public = render_public(stack, **overrides)
                external = "compose.prism-external-db.yaml" in stack
                expected = WRITER_URL if external else WRITER_URL.replace("@prism-postgres:", "@prism-postgres-replica:")
                self.assertEqual(public["environment"]["PRISM_DATABASE_URL"], expected)
                self.assertEqual(public["environment"]["PGPASSWORD"], password)
                self.assertEqual(public["environment"]["PRISM_PUBLIC_REPLICA_MODE"], "off" if external else "require")

    def test_literal_env_file_password_and_encoded_dsn_in_every_stack(self) -> None:
        with tempfile.TemporaryDirectory(prefix="prism-reader-env-") as directory:
            env_file = Path(directory) / "reader.env"
            # Match the documented form for both Compose and the shell-based
            # preflight loader. The caller's environment has no carrier.
            env_file.write_text(f"PRISM_PUBLIC_DATABASE_URL={LITERAL_PASSWORDLESS_URL}\n"
                                f"PRISM_PUBLIC_POSTGRES_PASSWORD='{LITERAL_PASSWORD}'\n", encoding="utf-8")
            sourced = subprocess.run(
                ["bash", "-c", 'source "$1"; printf "%s" "$PRISM_PUBLIC_POSTGRES_PASSWORD"', "fixture", str(env_file)],
                env=fixture_env(), text=True, capture_output=True, check=True,
            )
            self.assertEqual(sourced.stdout, LITERAL_PASSWORD)
            for stack in STACKS:
                with self.subTest(stack=stack):
                    self.assert_reader_boundary(render_public(stack, env_file=env_file),
                                                LITERAL_PASSWORDLESS_URL, LITERAL_PASSWORD)
                    self.assert_reader_boundary(render_public(stack, PRISM_PUBLIC_DATABASE_URL=LITERAL_URL),
                                                LITERAL_URL)


@unittest.skipUnless(os.environ.get("PRISM_CREDENTIAL_TEST_IMAGE"), "set PRISM_CREDENTIAL_TEST_IMAGE for disposable image integration")
class PublicCredentialRuntimeTests(unittest.TestCase):
    """Run the actual merged public service, image entrypoint and HTTP probe."""

    @classmethod
    def command(cls, *args: str, check: bool = True, input: str | None = None,
                env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=ROOT, env=fixture_env(**(env or {})), text=True,
                              input=input, capture_output=True, check=check, timeout=180)

    @classmethod
    def sql(cls, query: str, *, reader: bool = False, check: bool = True) -> subprocess.CompletedProcess:
        return cls.command(
            "docker", "exec", "-i", "--env", "PGPASSWORD",
            cls.primary, "psql", "-h", "127.0.0.1", "-U", "prism_reader" if reader else "prism_bootstrap",
            "-d", "prism_fixture", "-v", "ON_ERROR_STOP=1", "-Atf", "-", input=query, check=check,
            env={"PGPASSWORD": READER_PASSWORD if reader else BOOTSTRAP_PASSWORD},
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
            "--env", "POSTGRES_PASSWORD", "--env", "POSTGRES_DB=prism_fixture",
            "--env", "POSTGRES_HOST_AUTH_METHOD=scram-sha-256", "postgres:16",
            env={"POSTGRES_PASSWORD": BOOTSTRAP_PASSWORD},
        )
        for _ in range(60):
            if cls.sql("SELECT 1", check=False).returncode == 0:
                break
            time.sleep(1)
        else:
            raise AssertionError("disposable PostgreSQL did not become ready")
        cls.command("docker", "run", "--rm", "--network", cls.network, "--no-healthcheck",
                    "--env", "PRISM_DATABASE_URL", cls.image, "qbit-prism-server", "migrate",
                    env={"PRISM_DATABASE_URL": WRITER_URL})
        cls.sql(f"""
            CREATE ROLE prism_reader LOGIN PASSWORD '{READER_PASSWORD}'
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
            GRANT CONNECT ON DATABASE prism_fixture TO prism_reader;
            GRANT USAGE ON SCHEMA public TO prism_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO prism_reader;
            GRANT pg_read_all_stats TO prism_reader;
            CREATE ROLE prism_literal_reader LOGIN PASSWORD '{LITERAL_PASSWORD.replace("'", "''")}'
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
            GRANT prism_reader TO prism_literal_reader;
            CREATE ROLE qbit LOGIN PASSWORD 'change-this'
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
            GRANT prism_reader TO qbit;
        """)
        # Use the shipped standby entrypoint against a disposable primary only.
        cls.command("docker", "exec", cls.primary, "sh", "-c",
                    'printf "host replication prism_bootstrap all scram-sha-256\\n" >> "$PGDATA/pg_hba.conf"')
        cls.sql("SELECT pg_reload_conf()")
        cls.command(
            "docker", "run", "-d", "--name", cls.replica, "--network", cls.network,
            "--network-alias", "prism-postgres-replica", "--env", "PRISM_POSTGRES_USER=prism_bootstrap",
            "--env", "PRISM_POSTGRES_PASSWORD", "--env", "PRISM_POSTGRES_DB=prism_fixture",
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

    def start_public(self, stack: tuple[str, ...], *, lab_default: bool = False,
                     env_file: Path | None = None, **overrides: str) -> dict:
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
                [*compose_args(stack, str(overlay), self.project, env_file), "up", "--detach", "--no-deps", "--no-build",
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
        self.assertTrue(any(item.startswith("PGPASSWORD=") for item in json.loads(config)["Env"]))
        return json.loads(config)

    def preflight(self, stack: tuple[str, ...], *, env_file: Path | None = None,
                  **overrides: str) -> subprocess.CompletedProcess:
        # The actual shell preflight runs the public image with the same merged
        # service environment. Place its one-shot container on our disposable
        # network; no production services, volumes or ports are started.
        with tempfile.TemporaryDirectory(prefix="prism-reader-preflight-") as directory:
            fixture_root = Path(directory)
            (fixture_root / "scripts").mkdir()
            (fixture_root / "config").mkdir()
            for source, destination in (
                (ROOT / "scripts/check-env.sh", fixture_root / "scripts/check-env.sh"),
                (ROOT / ".env.example", fixture_root / ".env.example"),
                (ROOT / "config/upstream.env.example", fixture_root / "config/upstream.env.example"),
                (ROOT / "compose.yaml", fixture_root / "compose.yaml"),
            ):
                shutil.copyfile(source, destination)
            overlay = fixture_root / "placement.yaml"
            overlay.write_text(
                "services:\n  prism-public-api:\n    networks: !override [fixture]\n"
                "networks:\n  fixture:\n    external: true\n"
                f"    name: {self.network}\n", encoding="utf-8")
            args = ["bash", str(fixture_root / "scripts/check-env.sh"), "--public-reader-only"]
            for name in (*stack, str(overlay)):
                args.extend(("--compose-file", str(ROOT / name)))
            if env_file:
                overrides["DEPLOY_ENV_FILE"] = str(env_file)
            result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=60,
                                    env=fixture_env(production="compose.production.yaml" in stack,
                                                    **{"PRISM_COORDINATOR_IMAGE": self.image,
                                                       "COMPOSE_PROJECT_NAME": self.project, **overrides}))
        diagnostics = result.stdout + result.stderr
        for credential in (BOOTSTRAP_PASSWORD, READER_PASSWORD, LITERAL_PASSWORD, "fixture-bad-reader",
                           "change-this", "ch%61nge-this"):
            self.assertFalse(credential in diagnostics, "preflight exposed a credential")
        return result

    def assert_config_exit(self, expected: str) -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            state = json.loads(self.command("docker", "inspect", "--format", "{{json .State}}", self.public).stdout)
            if not state["Running"]:
                self.assertNotEqual(state["ExitCode"], 0)
                logs = self.command("docker", "logs", self.public)
                diagnostics = logs.stdout + logs.stderr
                for credential in (BOOTSTRAP_PASSWORD, READER_PASSWORD, LITERAL_PASSWORD, "change-this",
                                   "ch%61nge-this", "fixture-secret"):
                    self.assertFalse(credential in diagnostics, "startup exposed a credential")
                self.assertTrue(expected in diagnostics, "missing configuration diagnostic")
                return
            time.sleep(0.1)
        self.fail("invalid configuration reached service startup instead of exiting")

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
                self.fail("synthetic public listener did not report readiness within 30 seconds")
            time.sleep(0.25)
        self.assertEqual(status, 200 if healthy else 503, body)
        self.assertIs(body["ok"], healthy, body)
        self.assertIs(body["database_ready"], healthy, body)
        # In require mode the existing replica guard may report warming up
        # after authentication prevented its first snapshot. Keep those public
        # failure-message semantics with the readiness/redaction owner.
        if not healthy and "replica" not in body:
            self.assertIn("password authentication failed", body.get("error", ""), body)
        self.assertNotIn(BOOTSTRAP_PASSWORD, json.dumps(body))
        self.assertNotIn(READER_PASSWORD, json.dumps(body))
        self.assertNotIn(LITERAL_PASSWORD, json.dumps(body))
        probe = self.command("docker", "exec", self.public, "qbit-prism-server", "healthcheck", "--public-api", check=False)
        self.assertEqual(probe.returncode == 0, healthy, probe.stderr)
        # Blocks exercises application-table reads without requiring a node RPC.
        status, body = self.response("/public/v1/blocks")
        self.assertEqual(status, 200 if healthy else 503, body)

    def test_reader_dsn_and_http_health_in_every_stack(self) -> None:
        for stack in STACKS:
            with self.subTest(stack=stack):
                self.assertEqual(self.preflight(stack, PRISM_PUBLIC_DATABASE_URL=READER_URL).returncode, 0)
                self.start_public(stack, PRISM_PUBLIC_DATABASE_URL=READER_URL)
                self.assert_health(True)

    def test_supported_default_dsns_still_connect(self) -> None:
        for stack, dsn in itertools.product(STACKS, (None, "")):
            with self.subTest(stack=stack, dsn_absent=dsn is None):
                overrides = {} if dsn is None else {"PRISM_PUBLIC_DATABASE_URL": dsn}
                self.assertEqual(self.preflight(stack, **overrides).returncode, 0)
                self.start_public(stack, lab_default=True, **overrides)
                self.assert_health(True)

    def test_shipped_lab_password_remains_supported(self) -> None:
        overrides = {"PRISM_PUBLIC_DATABASE_URL": "", "PRISM_POSTGRES_USER": "qbit",
                     "PRISM_POSTGRES_PASSWORD": "change-this"}
        self.assertEqual(self.preflight((), **overrides).returncode, 0)
        self.start_public((), lab_default=True, **overrides)
        self.assert_health(True)

    def test_literal_env_file_password_and_encoded_dsn_authenticate(self) -> None:
        stack = ("compose.production.yaml", "compose.prism-external-db.yaml", "compose.prism-ha.yaml")
        with tempfile.TemporaryDirectory(prefix="prism-reader-env-") as directory:
            env_file = Path(directory) / "reader.env"
            env_file.write_text(f"PRISM_PUBLIC_DATABASE_URL={LITERAL_PASSWORDLESS_URL}\n"
                                f"PRISM_PUBLIC_POSTGRES_PASSWORD='{LITERAL_PASSWORD}'\n", encoding="utf-8")
            self.assertEqual(self.preflight(stack, env_file=env_file).returncode, 0)
            config = self.start_public(stack, env_file=env_file)
            self.assertIn(f"PGPASSWORD={LITERAL_PASSWORD}", config["Env"])
            self.assert_health(True)
        self.assertEqual(self.preflight(stack, PRISM_PUBLIC_DATABASE_URL=LITERAL_URL).returncode, 0)
        config = self.start_public(stack, PRISM_PUBLIC_DATABASE_URL=LITERAL_URL)
        self.assertIn("PGPASSWORD=", config["Env"])
        self.assert_health(True)

    def test_interpolated_carrier_preserves_original_caller_precedence(self) -> None:
        stack = ("compose.production.yaml", "compose.prism-external-db.yaml", "compose.prism-ha.yaml")
        with tempfile.TemporaryDirectory(prefix="prism-reader-interpolation-") as directory:
            env_file = Path(directory) / "reader.env"
            env_file.write_text(f"PRISM_PUBLIC_DATABASE_URL={PASSWORDLESS_URL}\n"
                                "READER_SECRET=change-this\n"
                                "PRISM_PUBLIC_POSTGRES_PASSWORD=${READER_SECRET}\n", encoding="utf-8")
            self.assertEqual(self.preflight(stack, env_file=env_file, READER_SECRET=READER_PASSWORD).returncode, 0)
            self.start_public(stack, env_file=env_file, READER_SECRET=READER_PASSWORD)
            self.assert_health(True)

    def test_passwordless_dsn_authentication_and_dsn_precedence(self) -> None:
        for stack, password in itertools.product(STACKS, (None, "", "fixture-bad-reader", READER_PASSWORD)):
            with self.subTest(stack=stack, absent=password is None, empty=password == "", valid=password == READER_PASSWORD):
                overrides = {"PRISM_PUBLIC_DATABASE_URL": PASSWORDLESS_URL}
                if password is not None:
                    overrides["PRISM_PUBLIC_POSTGRES_PASSWORD"] = password
                self.assertEqual(self.preflight(stack, **overrides).returncode, 0)
                self.start_public(stack, **overrides)
                self.assert_health(password == READER_PASSWORD)
        # An explicitly incorrect DSN password must not be repaired by even a
        # valid reader carrier; the DSN is authoritative. An empty query
        # password likewise wins over the carrier in SQLx.
        stack = ("compose.production.yaml", "compose.prism-external-db.yaml", "compose.prism-ha.yaml")
        for password in ("fixture-bad-reader", ""):
            with self.subTest(dsn_password=password):
                self.assertEqual(self.preflight(stack, PRISM_PUBLIC_DATABASE_URL=f"{PASSWORDLESS_URL}?password={password}",
                                               PRISM_PUBLIC_POSTGRES_PASSWORD=READER_PASSWORD).returncode, 0)
                self.start_public(stack, PRISM_PUBLIC_DATABASE_URL=f"{PASSWORDLESS_URL}?password={password}",
                                  PRISM_PUBLIC_POSTGRES_PASSWORD=READER_PASSWORD)
                self.assert_health(False)

    def test_malformed_and_production_default_credentials_fail_preflight_and_startup(self) -> None:
        cases = (
            ("malformed", "fixture-secret", "", "invalid public PRISM_DATABASE_URL"),
            ("invalid-option", PASSWORDLESS_URL + "?sslmode=fixture-secret", "", "invalid public PRISM_DATABASE_URL"),
            ("uri-default", PASSWORDLESS_URL.replace("prism_reader@", "prism_reader:change-this@"), "", "non-default database credentials"),
            ("encoded-default", PASSWORDLESS_URL.replace("prism_reader@", "prism_reader:ch%61nge-this@"), "", "non-default database credentials"),
            ("carrier-default", PASSWORDLESS_URL, "change-this", "non-default database credentials"),
            ("query-default", PASSWORDLESS_URL + "?password=ch%61nge-this", READER_PASSWORD, "non-default database credentials"),
        )
        for stack in STACKS:
            for label, dsn, carrier, message in cases:
                if "default" in label and "compose.production.yaml" not in stack:
                    continue
                with self.subTest(stack=stack, case=label):
                    overrides = {"PRISM_PUBLIC_DATABASE_URL": dsn, "PRISM_PUBLIC_POSTGRES_PASSWORD": carrier}
                    self.assertNotEqual(self.preflight(stack, **overrides).returncode, 0)
                    self.start_public(stack, **overrides)
                    self.assert_config_exit(message)

    def test_missing_image_is_a_failed_preflight_without_building(self) -> None:
        absent_image = f"prism-credential-missing-{uuid.uuid4().hex}:local"
        self.assertNotEqual(self.preflight((), PRISM_COORDINATOR_IMAGE=absent_image).returncode, 0)
        result = self.command("docker", "image", "inspect", absent_image, check=False)
        self.assertNotEqual(result.returncode, 0, "preflight implicitly built an image")

    def test_reader_cannot_write_even_without_session_read_only_guard(self) -> None:
        self.assertEqual(self.sql("SELECT count(*) FROM qbit_pool_blocks", reader=True).returncode, 0)
        denied = self.sql("SET default_transaction_read_only=off; UPDATE qbit_pool_blocks SET inactive_since=inactive_since WHERE false",
                          reader=True, check=False)
        self.assertNotEqual(denied.returncode, 0)
        self.assertIn("permission denied", denied.stderr)
        self.assertEqual(self.sql("SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication FROM pg_roles WHERE rolname=current_user", reader=True).stdout.strip(), "f")


if __name__ == "__main__":
    unittest.main()

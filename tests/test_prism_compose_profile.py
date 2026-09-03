#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PrismComposeProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        docker = shutil.which("docker")
        if docker is None:
            raise unittest.SkipTest("docker CLI is not installed")

        version = subprocess.run(
            [docker, "compose", "version"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if version.returncode != 0:
            raise unittest.SkipTest(f"docker compose is unavailable: {version.stderr.strip()}")

        env = os.environ.copy()
        env.update(
            {
                "QBIT_SRC_DIR": str(ROOT),
                "QBIT_NODE_EXTRA_ARG": "-listen=1",
                "PRISM_STRATUM_PORT": "43340",
                "PRISM_STRATUM_PORT_HOST": "127.0.0.1:43340",
                "PRISM_STRATUM_HIGHDIFF_PORT": "44334",
                "PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://public-pool.example:3335",
                "PRISM_PUBLIC_STRATUM_HIGHDIFF_URL": "stratum+tcp://public-pool.example:4334",
                "PRISM_PUBLIC_POOL_FEE_BPS": "200",
                "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS": "20971520",
                "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS": "15",
                "PRISM_MAX_DIRECT_COINBASE_OUTPUTS": "7",
                "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION": "999",
                "PRISM_RESERVED_COINBASE_OUTPUTS": "1",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "25",
                "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS": "13000",
                "PRISM_COINBASE_OUTPUT_POLICY": "pool-fee-first",
                "PRISM_CTV_BROADCASTER_ENABLED": "1",
                "PRISM_CTV_BROADCASTER_WALLET": "fanout-broadcaster",
                "PRISM_CTV_BROADCASTER_FEE_BITS": "0",
                "PRISM_CTV_BROADCASTER_LIMIT": "7",
                "PRISM_CTV_BROADCASTER_CHUNK_SIZE": "3",
                "PRISM_CTV_BROADCASTER_INTERVAL_SECONDS": "11",
                "PRISM_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT": "9",
                "PRISM_CTV_BROADCAST_RETRY_BACKOFF_SECONDS": "17",
                "PRISM_BLOCKWAIT_ENABLED": "0",
                "PRISM_BLOCKWAIT_TIMEOUT_SECONDS": "13",
                "PRISM_COORDINATION_BLOCKED_EXIT_SECONDS": "37",
                "PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS": "23",
                "PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS": "29",
                "PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS": "91",
                "PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS": "601",
                "PRISM_METRICS_REFRESH_SECONDS": "17",
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "4",
                "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS": "31",
                "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION": "65",
                "PRISM_TIP_REFRESH_EPOCH_FANOUT": "1",
                "PRISM_TIP_REFRESH_MAX_WORKERS": "7",
                "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS": "19",
                "PRISM_WORKER_METRICS_LIMIT": "8",
                "PRISM_STRATUM_MAX_CONNECTIONS": "1900",
                "PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME": "400",
                "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS": "120",
                "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS": "27",
                "PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS": "29",
                "PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS": "2",
                "PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS": "9",
                "PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES": "2048",
                "PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS": "1800",
                "PRISM_COORDINATOR_NOFILE_SOFT": "60000",
                "PRISM_COORDINATOR_NOFILE_HARD": "65000",
            }
        )
        completed = subprocess.run(
            [
                docker,
                "compose",
                "--env-file",
                str(ROOT / "config/upstream.env.example"),
                "--env-file",
                str(ROOT / ".env.example"),
                "-f",
                str(ROOT / "compose.yaml"),
                "--project-name",
                "qbit-prism-compose-test",
                "--profile",
                "prism",
                "config",
                "--format",
                "json",
            ],
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(
                "docker compose --profile prism config failed\n"
                f"stdout:\n{completed.stdout}\n"
                f"stderr:\n{completed.stderr}"
            )
        cls.config = json.loads(completed.stdout)

    def test_prism_profile_services_render(self) -> None:
        services = self.config["services"]

        self.assertIn("qbitd", services)
        self.assertIn("prism-postgres", services)
        self.assertIn("prism-coordinator", services)

    def test_qbit_prelaunch_tip_age_is_absent_by_default(self) -> None:
        env = self.config["services"]["qbitd"]["environment"]

        self.assertEqual(env["QBIT_PRODUCTION"], "0")
        self.assertEqual(env["QBIT_TOOLS_PRODUCTION"], "0")
        self.assertEqual(env["QBIT_CHAIN"], "regtest")
        self.assertEqual(env["CKPOOL_NON_TEST_READINESS_GATE"], "1")
        self.assertEqual(env["QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"], "")
        self.assertIsNone(env["QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"])

    def test_qbit_extra_argument_remains_one_independent_argv_entry(self) -> None:
        command = self.config["services"]["qbitd"]["command"]

        self.assertIn("-listen=1", command)
        self.assertEqual(command.count("-listen=1"), 1)

    def test_prism_coordinator_gets_required_environment(self) -> None:
        env = self._service_environment("prism-coordinator")

        self.assertEqual(env["QBIT_RPC_HOST"], "qbitd")
        self.assertEqual(env["QBIT_PRODUCTION"], "0")
        self.assertEqual(env["QBIT_TOOLS_PRODUCTION"], "0")
        self.assertEqual(env["PRISM_DATABASE_URL"], "postgresql://qbit:change-this@prism-postgres:5432/qbit")
        self.assertEqual(env["PRISM_POSTGRES_INIT_SCHEMA"], "1")
        self.assertEqual(env["PRISM_POSTGRES_READ_CONCURRENCY"], "4")
        self.assertEqual(env["PRISM_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS"], "15")
        self.assertEqual(env["PRISM_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS"], "30")
        self.assertEqual(env["PRISM_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS"], "10")
        self.assertEqual(env["PRISM_POSTGRES_TCP_KEEPALIVES_COUNT"], "3")
        self.assertEqual(env["PRISM_LEDGER_LEASE_TTL_SECONDS"], "60")
        self.assertEqual(env["PRISM_LEDGER_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS"], "5")
        self.assertEqual(env["PRISM_LEDGER_LEASE_ACQUIRE_ATTEMPTS"], "5")
        self.assertEqual(env["PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS"], "9")
        self.assertEqual(env["PRISM_WATCHDOG_ENABLED"], "1")
        self.assertEqual(env["PRISM_WATCHDOG_TIMEOUT_SECONDS"], "120")
        self.assertEqual(env["PRISM_WATCHDOG_INTERVAL_SECONDS"], "15")
        self.assertEqual(env["PRISM_BLOCKWAIT_ENABLED"], "0")
        self.assertEqual(env["PRISM_BLOCKWAIT_TIMEOUT_SECONDS"], "13")
        self.assertEqual(env["PRISM_COORDINATION_BLOCKED_EXIT_SECONDS"], "37")
        self.assertEqual(env["PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS"], "23")
        self.assertEqual(env["PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS"], "29")
        self.assertEqual(
            env["PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS"],
            "91",
        )
        self.assertEqual(
            env["PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS"],
            "601",
        )
        self.assertEqual(env["PRISM_METRICS_REFRESH_SECONDS"], "17")
        self.assertEqual(env["PRISM_STRATUM_STALE_GRACE_SECONDS"], "4")
        self.assertEqual(env["PRISM_STRATUM_SAME_TIP_JOB_RETENTION_SECONDS"], "31")
        self.assertEqual(
            env["PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION"],
            "65",
        )
        self.assertEqual(env["PRISM_TIP_REFRESH_EPOCH_FANOUT"], "1")
        self.assertEqual(env["PRISM_TIP_REFRESH_MAX_WORKERS"], "7")
        self.assertEqual(env["PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS"], "19")
        self.assertEqual(env["PRISM_WORKER_METRICS_LIMIT"], "8")
        self.assertEqual(env["PRISM_STRATUM_MAX_CONNECTIONS"], "1900")
        self.assertEqual(env["PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME"], "400")
        self.assertEqual(env["PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS"], "120")
        self.assertEqual(env["PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS"], "27")
        self.assertEqual(env["PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS"], "29")
        self.assertEqual(
            env["PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS"],
            "2",
        )
        self.assertEqual(env["PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES"], "2048")
        self.assertEqual(env["PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS"], "1800")
        self.assertEqual(env["PRISM_STRATUM_BIND"], "0.0.0.0")
        self.assertEqual(env["PRISM_STRATUM_PORT"], "43340")
        self.assertEqual(env["PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS"], "30")
        self.assertEqual(env["PRISM_AUDIT_BIND"], "127.0.0.1")
        self.assertEqual(env["PRISM_AUDIT_PORT"], "3341")
        self.assertEqual(env["PRISM_AUDIT_SHARE_SEGMENT_SIZE"], "10000")
        self.assertEqual(env["PRISM_AUDIT_LIVE_BUNDLE_RETENTION"], "5")
        self.assertEqual(env["PRISM_AUDIT_CANDIDATE_RETENTION_SECONDS"], "86400")
        self.assertEqual(env["PRISM_STOP_AFTER_BLOCK"], "0")
        self.assertEqual(env["PRISM_CTV_SETTLEMENT_ENABLED"], "1")
        self.assertEqual(env["PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS"], "20971520")
        self.assertEqual(env["PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS"], "15")
        self.assertEqual(env["PRISM_MAX_DIRECT_COINBASE_OUTPUTS"], "7")
        self.assertEqual(env["PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION"], "999")
        self.assertEqual(env["PRISM_RESERVED_COINBASE_OUTPUTS"], "1")
        self.assertEqual(env["PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT"], "25")
        self.assertEqual(env["PRISM_CTV_FANOUT_FEE_PREMIUM_BPS"], "13000")
        self.assertEqual(env["PRISM_COINBASE_OUTPUT_POLICY"], "pool-fee-first")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_ENABLED"], "1")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_WALLET"], "fanout-broadcaster")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_FEE_BITS"], "0")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_LIMIT"], "7")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_CHUNK_SIZE"], "3")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_INTERVAL_SECONDS"], "11")
        self.assertEqual(env["PRISM_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT"], "9")
        self.assertEqual(env["PRISM_CTV_BROADCAST_RETRY_BACKOFF_SECONDS"], "17")
        self.assertEqual(env["PRISM_USERNAME_FALLBACK_ADDRESS"], "")
        self.assertEqual(env["PRISM_ALLOW_MEMORY_LEDGER"], "0")
        self.assertEqual(env["PRISM_ALLOW_TEST_SIGNING_SEEDS"], "0")
        self.assertEqual(env["PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY"], "0")

    def test_prism_stratum_port_publish_is_configurable(self) -> None:
        ports = self.config["services"]["prism-coordinator"].get("ports", [])

        self.assertTrue(
            any(
                str(port.get("target")) == "43340"
                and str(port.get("published")) == "43340"
                and port.get("host_ip") == "127.0.0.1"
                for port in ports
                if isinstance(port, dict)
            ),
            f"did not find configurable PRISM Stratum port in {ports!r}",
        )

    def test_prism_audit_directory_is_persistent(self) -> None:
        volumes = self.config["services"]["prism-coordinator"].get("volumes", [])

        self.assertTrue(
            any(
                volume.get("type") == "volume"
                and volume.get("source") == "prism-audit-data"
                and volume.get("target") == "/var/lib/qbit-prism/audit"
                for volume in volumes
                if isinstance(volume, dict)
            ),
            f"did not find persistent PRISM audit volume in {volumes!r}",
        )

    def test_prism_services_restart_for_auto_recovery(self) -> None:
        # The coordinator restarts on crashes and watchdog exits, while clean
        # bounded-run exits (PRISM_STOP_AFTER_BLOCK / PRISM_MAX_BLOCKS) stay
        # stopped. Postgres should still come back after daemon/host restarts.
        self.assertEqual(self.config["services"]["prism-coordinator"].get("restart"), "on-failure")
        self.assertEqual(self.config["services"]["prism-postgres"].get("restart"), "unless-stopped")

    def test_prism_healthcheck_propagates_http_503_failure(self) -> None:
        healthcheck = self.config["services"]["prism-coordinator"]["healthcheck"]
        command = " ".join(healthcheck["test"])

        # urllib raises HTTPError for a 503, so the container command exits
        # non-zero instead of treating any HTTP response as healthy.
        self.assertIn("/healthz", command)
        self.assertIn("urllib.request.urlopen", command)
        self.assertEqual(healthcheck["start_period"], "15s")
        self.assertEqual(healthcheck["retries"], 3)

    def test_prism_coordinator_descriptor_limit_is_configurable(self) -> None:
        nofile = self.config["services"]["prism-coordinator"]["ulimits"]["nofile"]

        self.assertEqual(nofile, {"soft": 60000, "hard": 65000})

    # -- the extracted public read tier (issue #145) ------------------------

    def test_public_api_runs_the_public_read_service_from_the_prism_image(self) -> None:
        service = self.config["services"]["prism-public-api"]

        self.assertEqual(service["command"], ["python3", "-m", "lab.prism.public_read_service"])
        self.assertEqual(
            service["build"]["dockerfile"],
            "lab/prism/Dockerfile",
        )
        self.assertEqual(
            service["image"],
            self.config["services"]["prism-coordinator"]["image"],
        )
        self.assertEqual(service.get("restart"), "on-failure")

    def test_public_api_survives_coordinator_restarts(self) -> None:
        # The public tier must not depend on the coordinator: independence from
        # coordinator restarts is a large part of why the surface was extracted.
        # It waits on the standby it reads, which is also not the coordinator's
        # primary -- so a primary restart does not take the dashboard with it.
        depends_on = self.config["services"]["prism-public-api"].get("depends_on", {})

        self.assertIn("prism-postgres-replica", depends_on)
        self.assertEqual(
            depends_on["prism-postgres-replica"]["condition"],
            "service_healthy",
        )
        self.assertNotIn("prism-coordinator", depends_on)

    def test_public_api_mounts_the_audit_volume_read_only(self) -> None:
        volumes = self.config["services"]["prism-public-api"].get("volumes", [])
        audit = [
            volume
            for volume in volumes
            if isinstance(volume, dict) and volume.get("source") == "prism-audit-data"
        ]

        self.assertEqual(1, len(audit), f"expected one audit mount in {volumes!r}")
        self.assertTrue(
            audit[0].get("read_only"),
            f"the audit artifact volume must be mounted read-only: {audit[0]!r}",
        )

    def test_public_api_receives_the_moved_public_knobs(self) -> None:
        env = self._service_environment("prism-public-api")

        self.assertEqual(env["PRISM_PUBLIC_STRATUM_URL"], "stratum+tcp://public-pool.example:3335")
        self.assertEqual(env["PRISM_PUBLIC_STRATUM_HIGHDIFF_URL"], "stratum+tcp://public-pool.example:4334")
        self.assertEqual(env["PRISM_PUBLIC_POOL_FEE_BPS"], "200")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_ENABLED"], "1")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_TTL_SECONDS"], "5")
        self.assertEqual(env["PRISM_PUBLIC_AGGREGATE_CACHE_TTL_SECONDS"], "30")
        self.assertEqual(env["PRISM_PUBLIC_CONFIG_CACHE_TTL_SECONDS"], "300")
        self.assertEqual(env["PRISM_PUBLIC_ARTIFACT_CACHE_TTL_SECONDS"], "86400")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_MAX_ENTRIES"], "512")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_DEBUG_HEADERS"], "0")
        self.assertEqual(
            env["PRISM_DATABASE_URL"],
            "postgresql://qbit:change-this@prism-postgres-replica:5432/qbit",
        )
        self.assertEqual(env["PRISM_AUDIT_DIR"], "/var/lib/qbit-prism/audit")
        self.assertEqual(env["QBIT_RPC_HOST"], "qbitd")

    def test_public_api_receives_the_advertised_stratum_ports(self) -> None:
        # The service runs no Stratum listener, but mining-configuration still
        # renders from these: PRISM_STRATUM_PORT is the primary endpoint's
        # fallback port and the High-diff endpoint appears only when
        # PRISM_STRATUM_HIGHDIFF_PORT is set. Without the pass-through the
        # rendered body silently drops the high-diff listener the coordinator
        # is running.
        env = self._service_environment("prism-public-api")

        self.assertEqual(env["PRISM_STRATUM_PORT"], "43340")
        self.assertEqual(env["PRISM_STRATUM_HIGHDIFF_PORT"], "44334")

    def test_the_coordinator_no_longer_carries_the_moved_public_knobs(self) -> None:
        # The coordinator serves no route that reads these any more; leaving
        # them behind would imply it still did.
        env = self._service_environment("prism-coordinator")

        for name in (
            "PRISM_PUBLIC_STRATUM_URL",
            "PRISM_PUBLIC_POOL_NAME",
            "PRISM_PUBLIC_CACHE_ENABLED",
            "PRISM_PUBLIC_CACHE_TTL_SECONDS",
            "PRISM_PUBLIC_ARTIFACT_CACHE_TTL_SECONDS",
            "PRISM_PUBLIC_EXPLORER_TX_URL_PREFIX",
        ):
            with self.subTest(name=name):
                self.assertNotIn(name, env)

    def test_the_reward_window_cache_knob_stays_on_both(self) -> None:
        # Read by the ledger itself, and both processes construct one.
        for service in ("prism-coordinator", "prism-public-api"):
            with self.subTest(service=service):
                env = self._service_environment(service)
                self.assertEqual(env["PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS"], "30")

    def test_public_api_never_receives_writer_or_signing_configuration(self) -> None:
        # A read tier that could acquire a writer lease or sign a manifest
        # would not be a read tier.
        env = self._service_environment("prism-public-api")

        self.assertEqual(env.get("PRISM_ALLOW_MEMORY_LEDGER"), "0")
        for name in env:
            self.assertFalse(
                name.startswith("PRISM_LEDGER_WRITER_"),
                f"{name} must not reach the public read tier",
            )
        for name in (
            "PRISM_MANIFEST_SIGNING_SEED_HEX",
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
            "PRISM_POSTGRES_INIT_SCHEMA",
        ):
            with self.subTest(name=name):
                self.assertNotIn(name, env)

    def test_public_api_publishes_its_port_and_healthcheck(self) -> None:
        service = self.config["services"]["prism-public-api"]
        published = {
            str(port.get("published"))
            for port in service.get("ports", [])
            if isinstance(port, dict)
        }
        self.assertIn("3342", published)

        command = " ".join(service["healthcheck"]["test"])
        self.assertIn("/healthz", command)
        self.assertIn("urllib.request.urlopen", command)

    def test_primary_pins_the_hba_file_carrying_the_replication_rules(self) -> None:
        postgres = self.config["services"]["prism-postgres"]

        self.assertEqual(
            postgres["command"],
            ["postgres", "-c", "hba_file=/etc/postgresql/pg_hba.conf"],
        )
        mounts = [
            volume
            for volume in postgres.get("volumes", [])
            if isinstance(volume, dict)
            and volume.get("target") == "/etc/postgresql/pg_hba.conf"
        ]
        self.assertEqual(
            len(mounts), 1, f"missing pg_hba mount in {postgres.get('volumes')!r}"
        )
        self.assertTrue(mounts[0].get("read_only", False))
        self.assertEqual(
            mounts[0].get("source"),
            str(ROOT / "config/prism-postgres/pg_hba.conf"),
        )

    def test_primary_hba_file_authorizes_replication_and_keeps_local_access(
        self,
    ) -> None:
        rules = [
            line.split()
            for line in (ROOT / "config/prism-postgres/pg_hba.conf")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

        # The image's initdb bootstrap, pg_isready and `docker exec psql` all
        # come in over the unix socket, and this file replaces the generated
        # one, so losing the local rule would break the existing healthcheck.
        self.assertEqual(rules[0][:4], ["local", "all", "all", "trust"])
        self.assertIn(["host", "all", "all", "127.0.0.1/32", "scram-sha-256"], rules)
        self.assertIn(["host", "all", "all", "::1/128", "scram-sha-256"], rules)
        self.assertIn(["host", "all", "all", "0.0.0.0/0", "scram-sha-256"], rules)
        self.assertIn(["host", "all", "all", "::/0", "scram-sha-256"], rules)
        # Compose cannot interpolate this file, so the replication rules name
        # the default superuser literally.
        self.assertIn(
            ["host", "replication", "qbit", "0.0.0.0/0", "scram-sha-256"], rules
        )
        self.assertIn(["host", "replication", "qbit", "::/0", "scram-sha-256"], rules)

    def test_replica_bootstraps_from_the_primary(self) -> None:
        replica = self.config["services"]["prism-postgres-replica"]
        env = self._service_environment("prism-postgres-replica")

        self.assertEqual(replica["image"], "postgres:16-alpine")
        self.assertEqual(
            replica["depends_on"]["prism-postgres"]["condition"], "service_healthy"
        )
        self.assertEqual(
            replica["entrypoint"], ["/usr/local/bin/prism-replica-entrypoint.sh"]
        )
        self.assertEqual(
            env["PRISM_POSTGRES_REPLICATION_SLOT"], "prism_public_replica"
        )
        # The data directory is cloned by pg_basebackup, so these credentials
        # exist for the replication connection and the healthcheck's psql.
        self.assertEqual(env["POSTGRES_USER"], "qbit")
        self.assertEqual(env["POSTGRES_DB"], "qbit")
        self.assertEqual(env["PGPASSWORD"], env["POSTGRES_PASSWORD"])

        targets = {
            volume.get("target"): volume
            for volume in replica.get("volumes", [])
            if isinstance(volume, dict)
        }
        self.assertEqual(
            targets["/var/lib/postgresql/data"].get("source"),
            "prism-postgres-replica-data",
        )
        self.assertEqual(
            targets["/usr/local/bin/prism-replica-entrypoint.sh"].get("source"),
            str(ROOT / "config/prism-postgres/replica-entrypoint.sh"),
        )

    def test_replica_bootstrap_script_retries_and_creates_the_slot(self) -> None:
        script = (ROOT / "config/prism-postgres/replica-entrypoint.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("pg_basebackup", script)
        self.assertIn("--wal-method=stream", script)
        self.assertIn("--write-recovery-conf", script)
        self.assertIn('--slot="$REPLICATION_SLOT"', script)
        self.assertIn("--create-slot", script)
        # A cold `docker compose up` must survive the primary not being
        # reachable yet, on a bounded budget rather than forever.
        self.assertIn("PRISM_POSTGRES_REPLICA_BASEBACKUP_ATTEMPTS:-60", script)
        self.assertIn("PRISM_POSTGRES_REPLICA_BASEBACKUP_RETRY_SECONDS:-5", script)
        # Hand off to the stock entrypoint, which skips init on a populated
        # data directory and drops privileges to the postgres account.
        self.assertIn("exec docker-entrypoint.sh postgres", script)
        # A bind mount aimed by mistake at a real cluster must stop the script
        # rather than be cleared by the incomplete-bootstrap path below it.
        self.assertIn("pg_controldata", script)

    def test_replica_healthcheck_requires_a_live_standby(self) -> None:
        command = " ".join(
            self.config["services"]["prism-postgres-replica"]["healthcheck"]["test"]
        )

        # An out-of-band promotion must report unhealthy rather than quietly
        # become a second writable cluster behind the public read tier.
        self.assertIn("pg_is_in_recovery()", command)
        self.assertIn("pg_isready", command)

    def test_public_read_service_reads_the_replica_not_the_primary(self) -> None:
        env = self._service_environment("prism-public-api")

        self.assertIn("prism-postgres-replica", env["PRISM_DATABASE_URL"])
        self.assertNotIn("@prism-postgres:", env["PRISM_DATABASE_URL"])
        self.assertEqual(env["PRISM_PUBLIC_REPLICA_MODE"], "require")
        depends_on = self.config["services"]["prism-public-api"]["depends_on"]
        self.assertEqual({"prism-postgres-replica"}, set(depends_on))
        self.assertEqual(
            "service_healthy", depends_on["prism-postgres-replica"]["condition"]
        )

        # The coordinator keeps the primary; the split is the point.
        coordinator = self._service_environment("prism-coordinator")
        self.assertIn("@prism-postgres:", coordinator["PRISM_DATABASE_URL"])

    def _service_environment(self, name: str) -> dict[str, str]:
        raw_env = self.config["services"][name]["environment"]
        if isinstance(raw_env, dict):
            return {str(key): str(value) for key, value in raw_env.items()}
        if isinstance(raw_env, list):
            result = {}
            for item in raw_env:
                key, _, value = str(item).partition("=")
                result[key] = value
            return result
        raise TypeError(f"unexpected environment shape for {name}: {type(raw_env).__name__}")


if __name__ == "__main__":
    unittest.main()

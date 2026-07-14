#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from lab.prism import capacity_evidence


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "prism-capacity-evidence.json"
NOW = datetime(2026, 7, 13, 12, 30, tzinfo=timezone.utc)


class PrismCapacityEvidenceTests(unittest.TestCase):
    def payload(self, *, qualification: bool = True) -> dict[str, object]:
        payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
        if qualification:
            payload["artifact_kind"] = "qualification"
            payload["run_id"] = "91d514da-2c6f-4a8e-8964-b5f64c46ba18"
        return payload

    def expected_configuration(self) -> dict[str, str]:
        return {
            "PRISM_STRATUM_SHARE_DIFF": "1024.0",
            "PRISM_STRATUM_VARDIFF": "1",
            "PRISM_STRATUM_VARDIFF_TARGET_SECONDS": "15",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
            "PRISM_STRATUM_VARDIFF_RETARGET_SECONDS": "90",
            "PRISM_STRATUM_VARDIFF_MAX_STEP_UP": "4",
            "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN": "4",
            "PRISM_STRATUM_VARDIFF_EWMA_ALPHA": "0.4",
            "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE": "0.25",
            "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS": "15",
            "PRISM_SHARE_COMMIT_BATCH_SIZE": "64",
            "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS": "5",
            "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS": "15",
            "PRISM_STRATUM_SEND_TIMEOUT_SECONDS": "20",
        }

    def expected_subject(self) -> dict[str, str]:
        return {
            "coordinator_revision": "a" * 40,
            "coordinator_image_digest": "sha256:" + "b" * 64,
            "postgres_server_version": "16.4",
            "database_profile_sha256": "c" * 64,
        }

    def validate(
        self,
        payload: dict[str, object] | None = None,
        **overrides: object,
    ) -> capacity_evidence.CapacityEvidenceSummary:
        options: dict[str, object] = {
            "expected_configuration": self.expected_configuration(),
            "expected_subject": self.expected_subject(),
            "expected_forecast_peak_shares_per_second": "100",
            "expected_ack_p99_limit_milliseconds": "50",
            "current_time": NOW,
        }
        options.update(overrides)
        return capacity_evidence.validate_capacity_evidence(
            payload if payload is not None else self.payload(),
            **options,
        )

    def test_qualification_proves_two_times_capacity_and_identifier_reconciliation(self) -> None:
        summary = self.validate()

        self.assertEqual(summary.measured_shares_per_second, Decimal("200"))
        self.assertEqual(summary.capacity_multiple, Decimal("2"))
        self.assertEqual(summary.acknowledged_shares, 120000)
        self.assertEqual(summary.ack_p50_milliseconds, Decimal("6"))
        self.assertEqual(summary.ack_p99_milliseconds, Decimal("40"))

    def test_committed_fixture_is_example_only(self) -> None:
        payload = self.payload(qualification=False)

        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "example capacity evidence"):
            self.validate(payload)

        summary = self.validate(payload, allow_example=True)
        self.assertEqual(summary.capacity_multiple, Decimal("2"))

    def test_qualification_requires_external_policy_and_subject(self) -> None:
        missing_options = (
            ("expected_configuration", "expected deployment configuration"),
            ("expected_subject", "expected deployment subject"),
            ("expected_forecast_peak_shares_per_second", "externally configured forecast"),
            ("expected_ack_p99_limit_milliseconds", "externally configured ACK p99"),
        )
        for option, message in missing_options:
            with self.subTest(option=option):
                with self.assertRaisesRegex(capacity_evidence.EvidenceError, message):
                    self.validate(**{option: None})

    def test_rejects_stale_and_future_qualification_artifacts(self) -> None:
        stale = self.payload()
        stale["generated_at"] = "2026-07-10T12:00:00Z"
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "older than"):
            self.validate(stale)

        future = self.payload()
        future["generated_at"] = "2026-07-14T12:00:00Z"
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "too far in the future"):
            self.validate(future)

    def test_runtime_restart_can_revalidate_binding_without_time_bomb(self) -> None:
        stale = self.payload()
        stale["generated_at"] = "2026-01-01T00:00:00Z"

        summary = self.validate(stale, enforce_freshness=False)

        self.assertEqual(summary.capacity_multiple, Decimal("2"))

    def test_rejects_subject_for_different_runtime_or_database(self) -> None:
        for key in self.expected_subject():
            with self.subTest(key=key):
                expected = self.expected_subject()
                expected[key] = "different"
                with self.assertRaisesRegex(capacity_evidence.EvidenceError, f"subject.{key}"):
                    self.validate(expected_subject=expected)

    def test_rejects_weakened_postgres_durability(self) -> None:
        for setting in ("fsync", "full_page_writes", "synchronous_commit"):
            with self.subTest(setting=setting):
                payload = self.payload()
                durability = payload["durability"]
                assert isinstance(durability, dict)
                durability[setting] = "off"
                with self.assertRaisesRegex(capacity_evidence.EvidenceError, f"durability.{setting}"):
                    self.validate(payload)

    def test_binds_every_load_affecting_configuration_value(self) -> None:
        alternatives = {
            "PRISM_STRATUM_SHARE_DIFF": "2048",
            "PRISM_STRATUM_VARDIFF": "0",
            "PRISM_STRATUM_VARDIFF_TARGET_SECONDS": "20",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "2048",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "8192",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "131072",
            "PRISM_STRATUM_VARDIFF_RETARGET_SECONDS": "120",
            "PRISM_STRATUM_VARDIFF_MAX_STEP_UP": "2",
            "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN": "2",
            "PRISM_STRATUM_VARDIFF_EWMA_ALPHA": "0.5",
            "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE": "0.3",
            "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS": "20",
            "PRISM_SHARE_COMMIT_BATCH_SIZE": "32",
            "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS": "7",
            "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS": "20",
            "PRISM_STRATUM_SEND_TIMEOUT_SECONDS": "25",
        }
        self.assertEqual(set(alternatives), set(capacity_evidence.CONFIGURATION_KEYS))
        for key, alternative in alternatives.items():
            with self.subTest(key=key):
                expected = self.expected_configuration()
                expected[key] = alternative
                with self.assertRaisesRegex(capacity_evidence.EvidenceError, f"configuration.{key}"):
                    self.validate(expected_configuration=expected)

    def test_rejects_vardiff_start_outside_qualified_range(self) -> None:
        payload = self.payload()
        configuration = payload["configuration"]
        assert isinstance(configuration, dict)
        configuration["PRISM_STRATUM_VARDIFF_MAX_DIFF"] = "2048"

        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "minimum <= start <= maximum"):
            self.validate(payload)

    def test_rejects_invalid_vardiff_math_parameters(self) -> None:
        for key, value, message in (
            ("PRISM_STRATUM_VARDIFF_MAX_STEP_UP", "0.5", "must be at least 1"),
            ("PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN", "0.5", "must be at least 1"),
            ("PRISM_STRATUM_VARDIFF_EWMA_ALPHA", "1.1", "must not exceed 1"),
        ):
            with self.subTest(key=key):
                payload = self.payload()
                configuration = payload["configuration"]
                assert isinstance(configuration, dict)
                configuration[key] = value
                with self.assertRaisesRegex(capacity_evidence.EvidenceError, message):
                    self.validate(payload)

    def test_rejects_lab_difficulty_profile(self) -> None:
        payload = self.payload()
        configuration = payload["configuration"]
        assert isinstance(configuration, dict)
        configuration["PRISM_STRATUM_SHARE_DIFF"] = "1e-9"

        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "lab-only 1e-9"):
            self.validate(payload)

    def test_rejects_self_declared_forecast_or_latency_limit(self) -> None:
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "forecast_peak.*deployment"):
            self.validate(expected_forecast_peak_shares_per_second="101")
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "ack_p99_limit.*deployment"):
            self.validate(expected_ack_p99_limit_milliseconds="51")

    def test_rejects_ack_limit_beyond_commit_timeout(self) -> None:
        payload = self.payload()
        payload["ack_p99_limit_milliseconds"] = 16000

        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "cannot exceed.*COMMIT_TIMEOUT"):
            self.validate(payload, expected_ack_p99_limit_milliseconds="16000")

    def test_rejects_phase_duration_mismatch_and_trivial_phases(self) -> None:
        mismatch = self.payload()
        phases = mismatch["phases"]
        assert isinstance(phases, dict)
        steady = phases["steady_state"]
        assert isinstance(steady, dict)
        steady["duration_seconds"] = 399
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "phase durations"):
            self.validate(mismatch)

        trivial = self.payload()
        phases = trivial["phases"]
        assert isinstance(phases, dict)
        reconnect = phases["reconnect"]
        assert isinstance(reconnect, dict)
        reconnect["duration_seconds"] = 59
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "at least 60"):
            self.validate(trivial)

    def test_requires_two_times_capacity_in_every_fault_phase(self) -> None:
        payload = self.payload()
        phases = payload["phases"]
        assert isinstance(phases, dict)
        steady = phases["steady_state"]
        reconnect = phases["reconnect"]
        assert isinstance(steady, dict) and isinstance(reconnect, dict)
        for field in (
            "offered_valid_shares",
            "acknowledged_shares",
            "postgres_unique_committed_shares",
        ):
            steady[field] = 90000
            reconnect[field] = 10000

        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "phases.reconnect sustained rate"):
            self.validate(payload)

    def test_rejects_token_fault_phases(self) -> None:
        reconnect_payload = self.payload()
        phases = reconnect_payload["phases"]
        assert isinstance(phases, dict)
        reconnect = phases["reconnect"]
        assert isinstance(reconnect, dict)
        reconnect["reconnect_events"] = 1
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "reconnect_events must be at least 10"):
            self.validate(reconnect_payload)

        database_payload = self.payload()
        phases = database_payload["phases"]
        assert isinstance(phases, dict)
        slow_database = phases["slow_database"]
        assert isinstance(slow_database, dict)
        slow_database["database_delay_milliseconds"] = 1
        with self.assertRaisesRegex(
            capacity_evidence.EvidenceError,
            "database_delay_milliseconds must be at least 10",
        ):
            self.validate(database_payload)

    def test_rejects_valid_share_loss_and_identifier_mismatch(self) -> None:
        missing = self.payload()
        missing["missing_acknowledged_share_ids"] = 1
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "ACK-to-Postgres reconciliation"):
            self.validate(missing)

        digest_mismatch = self.payload()
        digest_mismatch["postgres_share_ids_sha256"] = "f" * 64
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "identifier digests differ"):
            self.validate(digest_mismatch)

        rejected = self.payload()
        rejected["rejected_valid_shares"] = 1
        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "every offered valid share"):
            self.validate(rejected)

    def test_rejects_phase_latency_over_external_limit(self) -> None:
        payload = self.payload()
        phases = payload["phases"]
        assert isinstance(phases, dict)
        slow_database = phases["slow_database"]
        assert isinstance(slow_database, dict)
        latency = slow_database["ack_latency_milliseconds"]
        assert isinstance(latency, dict)
        latency["p99"] = 51

        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "required 50ms"):
            self.validate(payload)

    def test_rejects_unknown_schema_fields(self) -> None:
        payload = self.payload()
        payload["unchecked_claim"] = True

        with self.assertRaisesRegex(capacity_evidence.EvidenceError, "unknown fields"):
            self.validate(payload)

    def test_loader_rejects_duplicate_json_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "duplicate.json"
            path.write_text('{"schema":"first","schema":"second"}', encoding="utf-8")

            with self.assertRaisesRegex(capacity_evidence.EvidenceError, "duplicate JSON key"):
                capacity_evidence.load_capacity_evidence(path)

    def test_cli_requires_explicit_test_override_for_example_fixture(self) -> None:
        command = [sys.executable, str(ROOT / "scripts" / "prism_capacity_evidence.py"), str(FIXTURE)]
        rejected = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
        accepted = subprocess.run(
            [*command, "--allow-example-evidence-for-tests"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("example capacity evidence", rejected.stderr)
        self.assertEqual(accepted.returncode, 0, accepted.stderr)


if __name__ == "__main__":
    unittest.main()

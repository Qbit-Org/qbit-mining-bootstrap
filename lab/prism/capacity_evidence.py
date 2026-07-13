"""Strict validation for PRISM production-capacity evidence artifacts."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping


SCHEMA = "qbit-prism-capacity-evidence/v2"
TEST_PATH = "stratum-to-postgres"
QUALIFICATION_ARTIFACT = "qualification"
EXAMPLE_ARTIFACT = "example"
DEFAULT_MAX_AGE_SECONDS = 24 * 60 * 60
DEFAULT_MAX_FUTURE_SKEW_SECONDS = 5 * 60
MINIMUM_PHASE_DURATION_SECONDS = Decimal("60")
MINIMUM_RECONNECT_EVENTS = 10
MINIMUM_DATABASE_DELAY_MILLISECONDS = Decimal("10")

DIFFICULTY_CONFIGURATION_KEYS = (
    "PRISM_STRATUM_SHARE_DIFF",
    "PRISM_STRATUM_VARDIFF_MIN_DIFF",
    "PRISM_STRATUM_VARDIFF_START_DIFF",
    "PRISM_STRATUM_VARDIFF_MAX_DIFF",
)
DECIMAL_CONFIGURATION_KEYS = (
    *DIFFICULTY_CONFIGURATION_KEYS,
    "PRISM_STRATUM_VARDIFF_TARGET_SECONDS",
    "PRISM_STRATUM_VARDIFF_RETARGET_SECONDS",
    "PRISM_STRATUM_VARDIFF_MAX_STEP_UP",
    "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN",
    "PRISM_STRATUM_VARDIFF_EWMA_ALPHA",
    "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE",
    "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS",
    "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS",
    "PRISM_STRATUM_SEND_TIMEOUT_SECONDS",
)
INTEGER_CONFIGURATION_KEYS = (
    "PRISM_STRATUM_VARDIFF",
    "PRISM_SHARE_COMMIT_BATCH_SIZE",
    "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS",
)
CONFIGURATION_KEYS = (*DECIMAL_CONFIGURATION_KEYS, *INTEGER_CONFIGURATION_KEYS)
SUBJECT_KEYS = (
    "coordinator_revision",
    "coordinator_image_digest",
    "postgres_server_version",
    "database_profile_sha256",
)
REQUIRED_PHASES = ("steady_state", "reconnect", "slow_database")

_RFC3339_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
_HEX_40_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_HEX_64_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")

_TOP_LEVEL_KEYS = {
    "schema",
    "artifact_kind",
    "test_path",
    "generated_at",
    "run_id",
    "subject",
    "durability",
    "configuration",
    "forecast_peak_shares_per_second",
    "test_duration_seconds",
    "offered_valid_shares",
    "acknowledged_shares",
    "postgres_unique_committed_shares",
    "rejected_valid_shares",
    "missing_acknowledged_share_ids",
    "unexpected_committed_share_ids",
    "acknowledged_share_ids_sha256",
    "postgres_share_ids_sha256",
    "ack_latency_milliseconds",
    "ack_p99_limit_milliseconds",
    "phases",
}
_DURABILITY_KEYS = {"fsync", "full_page_writes", "synchronous_commit"}
_LATENCY_KEYS = {"p50", "p99"}
_COMMON_PHASE_KEYS = {
    "completed",
    "duration_seconds",
    "offered_valid_shares",
    "acknowledged_shares",
    "postgres_unique_committed_shares",
    "rejected_valid_shares",
    "missing_acknowledged_share_ids",
    "unexpected_committed_share_ids",
    "acknowledged_share_ids_sha256",
    "postgres_share_ids_sha256",
    "ack_latency_milliseconds",
}


class EvidenceError(ValueError):
    """Raised when a capacity evidence document is incomplete or inconsistent."""


@dataclass(frozen=True)
class CapacityEvidenceSummary:
    generated_at: datetime
    measured_shares_per_second: Decimal
    capacity_multiple: Decimal
    acknowledged_shares: int
    ack_p50_milliseconds: Decimal
    ack_p99_milliseconds: Decimal
    ack_p99_limit_milliseconds: Decimal


@dataclass(frozen=True)
class _PhaseSummary:
    duration_seconds: Decimal
    offered_valid_shares: int
    acknowledged_shares: int
    committed_shares: int
    p50_milliseconds: Decimal
    p99_milliseconds: Decimal


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise EvidenceError(f"{field} must be a JSON object")
    return value


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError(f"capacity evidence contains duplicate JSON key {key!r}")
        result[key] = value
    return result


def _exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing:
        raise EvidenceError(f"{field} is missing required fields: {', '.join(missing)}")
    if unknown:
        raise EvidenceError(f"{field} contains unknown fields: {', '.join(unknown)}")


def _decimal(value: object, field: str, *, allow_zero: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise EvidenceError(f"{field} must be a finite decimal number")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise EvidenceError(f"{field} must be a finite decimal number") from exc
    if not parsed.is_finite() or parsed < 0 or (not allow_zero and parsed == 0):
        qualifier = "non-negative" if allow_zero else "positive"
        raise EvidenceError(f"{field} must be a finite {qualifier} decimal number")
    return parsed


def _integer(value: object, field: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise EvidenceError(f"{field} must be an integer")
    try:
        parsed = int(str(value))
    except ValueError as exc:
        raise EvidenceError(f"{field} must be an integer") from exc
    if str(parsed) != str(value).strip():
        raise EvidenceError(f"{field} must be an integer")
    if parsed < 0 or (not allow_zero and parsed == 0):
        qualifier = "non-negative" if allow_zero else "positive"
        raise EvidenceError(f"{field} must be a {qualifier} integer")
    return parsed


def _configuration_decimal(value: object, field: str, *, allow_zero: bool = False) -> Decimal:
    parsed = _decimal(value, field, allow_zero=allow_zero)
    if parsed == Decimal("0.000000001"):
        raise EvidenceError(f"{field} uses the lab-only 1e-9 difficulty")
    return parsed


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or _HEX_64_PATTERN.fullmatch(value) is None:
        raise EvidenceError(f"{field} must be 64 lowercase hex characters")
    return value


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or _RFC3339_PATTERN.fullmatch(value) is None:
        raise EvidenceError("generated_at must be an RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as exc:
        raise EvidenceError("generated_at must be an RFC 3339 timestamp") from exc
    return parsed.astimezone(timezone.utc)


def _validate_timestamp_freshness(
    generated_at: datetime,
    *,
    current_time: datetime | None,
    max_age_seconds: int,
    max_future_skew_seconds: int,
) -> None:
    if max_age_seconds <= 0:
        raise EvidenceError("capacity evidence maximum age must be positive")
    if max_future_skew_seconds < 0:
        raise EvidenceError("capacity evidence future skew must be non-negative")
    now = current_time or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise EvidenceError("capacity evidence current time must include a timezone")
    now = now.astimezone(timezone.utc)
    if generated_at > now + timedelta(seconds=max_future_skew_seconds):
        raise EvidenceError("capacity evidence generated_at is too far in the future")
    if generated_at < now - timedelta(seconds=max_age_seconds):
        raise EvidenceError(
            f"capacity evidence is older than the allowed {max_age_seconds} seconds"
        )


def _validate_subject(
    value: object,
    expected: Mapping[str, str] | None,
    *,
    allow_example: bool,
) -> None:
    subject = _mapping(value, "subject")
    _exact_keys(subject, set(SUBJECT_KEYS), "subject")
    revision = subject["coordinator_revision"]
    if not isinstance(revision, str) or _HEX_40_PATTERN.fullmatch(revision) is None:
        raise EvidenceError("subject.coordinator_revision must be 40 lowercase hex characters")
    image_digest = subject["coordinator_image_digest"]
    if not isinstance(image_digest, str) or _IMAGE_DIGEST_PATTERN.fullmatch(image_digest) is None:
        raise EvidenceError(
            "subject.coordinator_image_digest must be sha256 followed by 64 lowercase hex characters"
        )
    postgres_version = subject["postgres_server_version"]
    if not isinstance(postgres_version, str) or not postgres_version.strip():
        raise EvidenceError("subject.postgres_server_version must be a non-empty string")
    _sha256(subject["database_profile_sha256"], "subject.database_profile_sha256")

    if expected is None:
        if allow_example:
            return
        raise EvidenceError("expected deployment subject is required for qualification evidence")
    for key in SUBJECT_KEYS:
        if key not in expected or not expected[key]:
            raise EvidenceError(f"expected deployment subject is missing {key}")
        if subject[key] != expected[key]:
            raise EvidenceError(f"subject.{key} does not match the deployment value")


def _validate_durability(value: object) -> None:
    durability = _mapping(value, "durability")
    _exact_keys(durability, _DURABILITY_KEYS, "durability")
    for key in sorted(_DURABILITY_KEYS):
        if durability[key] != "on":
            raise EvidenceError(f"durability.{key} must be 'on'")


def _normalize_configuration(value: object, field: str) -> dict[str, Decimal | int]:
    configuration = _mapping(value, field)
    _exact_keys(configuration, set(CONFIGURATION_KEYS), field)
    normalized: dict[str, Decimal | int] = {}
    for key in DECIMAL_CONFIGURATION_KEYS:
        allow_zero = key in {
            "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE",
            "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS",
            "PRISM_STRATUM_SEND_TIMEOUT_SECONDS",
        }
        parser = _configuration_decimal if key in DIFFICULTY_CONFIGURATION_KEYS else _decimal
        normalized[key] = parser(configuration[key], f"{field}.{key}", allow_zero=allow_zero)
    for key in INTEGER_CONFIGURATION_KEYS:
        allow_zero = key in {
            "PRISM_STRATUM_VARDIFF",
            "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS",
        }
        normalized[key] = _integer(configuration[key], f"{field}.{key}", allow_zero=allow_zero)
    if normalized["PRISM_STRATUM_VARDIFF"] not in {0, 1}:
        raise EvidenceError(f"{field}.PRISM_STRATUM_VARDIFF must be 0 or 1")

    min_diff = normalized["PRISM_STRATUM_VARDIFF_MIN_DIFF"]
    start_diff = normalized["PRISM_STRATUM_VARDIFF_START_DIFF"]
    max_diff = normalized["PRISM_STRATUM_VARDIFF_MAX_DIFF"]
    assert isinstance(min_diff, Decimal)
    assert isinstance(start_diff, Decimal)
    assert isinstance(max_diff, Decimal)
    if not min_diff <= start_diff <= max_diff:
        raise EvidenceError(
            f"{field} vardiff values must satisfy minimum <= start <= maximum"
        )
    for key in ("PRISM_STRATUM_VARDIFF_MAX_STEP_UP", "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN"):
        if normalized[key] < 1:
            raise EvidenceError(f"{field}.{key} must be at least 1")
    if normalized["PRISM_STRATUM_VARDIFF_EWMA_ALPHA"] > 1:
        raise EvidenceError(f"{field}.PRISM_STRATUM_VARDIFF_EWMA_ALPHA must not exceed 1")
    return normalized


def _validate_configuration(
    value: object,
    expected: Mapping[str, str] | None,
    *,
    allow_example: bool,
) -> dict[str, Decimal | int]:
    normalized = _normalize_configuration(value, "configuration")
    if expected is None:
        if allow_example:
            return normalized
        raise EvidenceError("expected deployment configuration is required for qualification evidence")
    expected_normalized = _normalize_configuration(expected, "expected configuration")
    for key in CONFIGURATION_KEYS:
        if normalized[key] != expected_normalized[key]:
            raise EvidenceError(f"configuration.{key} does not match the deployment value")
    return normalized


def _validate_latency(value: object, field: str, p99_limit: Decimal) -> tuple[Decimal, Decimal]:
    latency = _mapping(value, field)
    _exact_keys(latency, _LATENCY_KEYS, field)
    p50 = _decimal(latency["p50"], f"{field}.p50", allow_zero=True)
    p99 = _decimal(latency["p99"], f"{field}.p99", allow_zero=True)
    if p50 > p99:
        raise EvidenceError(f"{field} p50 latency cannot exceed p99 latency")
    if p99 > p99_limit:
        raise EvidenceError(f"{field} p99 latency {p99}ms exceeds the required {p99_limit}ms")
    return p50, p99


def _validate_share_counts(value: Mapping[str, Any], field: str) -> tuple[int, int, int]:
    offered = _integer(value["offered_valid_shares"], f"{field}.offered_valid_shares")
    acknowledged = _integer(value["acknowledged_shares"], f"{field}.acknowledged_shares")
    committed = _integer(
        value["postgres_unique_committed_shares"],
        f"{field}.postgres_unique_committed_shares",
    )
    rejected = _integer(
        value["rejected_valid_shares"], f"{field}.rejected_valid_shares", allow_zero=True
    )
    missing = _integer(
        value["missing_acknowledged_share_ids"],
        f"{field}.missing_acknowledged_share_ids",
        allow_zero=True,
    )
    unexpected = _integer(
        value["unexpected_committed_share_ids"],
        f"{field}.unexpected_committed_share_ids",
        allow_zero=True,
    )
    ack_digest = _sha256(
        value["acknowledged_share_ids_sha256"], f"{field}.acknowledged_share_ids_sha256"
    )
    committed_digest = _sha256(
        value["postgres_share_ids_sha256"], f"{field}.postgres_share_ids_sha256"
    )
    if rejected != 0 or offered != acknowledged:
        raise EvidenceError(
            f"{field} did not acknowledge every offered valid share: "
            f"offered={offered} acknowledged={acknowledged} rejected={rejected}"
        )
    if acknowledged != committed or missing != 0 or unexpected != 0:
        raise EvidenceError(
            f"{field} failed ACK-to-Postgres reconciliation: acknowledged={acknowledged} "
            f"committed={committed} missing={missing} unexpected={unexpected}"
        )
    if ack_digest != committed_digest:
        raise EvidenceError(f"{field} ACK and Postgres share-identifier digests differ")
    return offered, acknowledged, committed


def _validate_phase(
    name: str,
    value: object,
    *,
    forecast: Decimal,
    p99_limit: Decimal,
) -> _PhaseSummary:
    field = f"phases.{name}"
    phase = _mapping(value, field)
    expected_keys = set(_COMMON_PHASE_KEYS)
    if name == "reconnect":
        expected_keys.add("reconnect_events")
    elif name == "slow_database":
        expected_keys.add("database_delay_milliseconds")
    _exact_keys(phase, expected_keys, field)
    if phase["completed"] is not True:
        raise EvidenceError(f"{field}.completed must be true")
    duration = _decimal(phase["duration_seconds"], f"{field}.duration_seconds")
    if duration < MINIMUM_PHASE_DURATION_SECONDS:
        raise EvidenceError(
            f"{field}.duration_seconds must be at least {MINIMUM_PHASE_DURATION_SECONDS}"
        )
    offered, acknowledged, committed = _validate_share_counts(phase, field)
    p50, p99 = _validate_latency(phase["ack_latency_milliseconds"], f"{field}.ack_latency_milliseconds", p99_limit)
    measured_rate = Decimal(acknowledged) / duration
    if measured_rate < forecast * 2:
        raise EvidenceError(
            f"{field} sustained rate must be at least 2x forecast peak: "
            f"measured={measured_rate} forecast={forecast}"
        )
    if name == "reconnect":
        reconnect_events = _integer(phase["reconnect_events"], f"{field}.reconnect_events")
        if reconnect_events < MINIMUM_RECONNECT_EVENTS:
            raise EvidenceError(
                f"{field}.reconnect_events must be at least {MINIMUM_RECONNECT_EVENTS}"
            )
    elif name == "slow_database":
        database_delay = _decimal(
            phase["database_delay_milliseconds"],
            f"{field}.database_delay_milliseconds",
        )
        if database_delay < MINIMUM_DATABASE_DELAY_MILLISECONDS:
            raise EvidenceError(
                f"{field}.database_delay_milliseconds must be at least "
                f"{MINIMUM_DATABASE_DELAY_MILLISECONDS}"
            )
    return _PhaseSummary(duration, offered, acknowledged, committed, p50, p99)


def validate_capacity_evidence(
    payload: object,
    *,
    expected_configuration: Mapping[str, str] | None = None,
    expected_subject: Mapping[str, str] | None = None,
    expected_forecast_peak_shares_per_second: object | None = None,
    expected_ack_p99_limit_milliseconds: object | None = None,
    current_time: datetime | None = None,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    max_future_skew_seconds: int = DEFAULT_MAX_FUTURE_SKEW_SECONDS,
    enforce_freshness: bool = True,
    allow_example: bool = False,
) -> CapacityEvidenceSummary:
    document = _mapping(payload, "document")
    _exact_keys(document, _TOP_LEVEL_KEYS, "document")
    if document["schema"] != SCHEMA:
        raise EvidenceError(f"schema must be {SCHEMA!r}")
    if document["test_path"] != TEST_PATH:
        raise EvidenceError(f"test_path must be {TEST_PATH!r}")

    artifact_kind = document["artifact_kind"]
    if artifact_kind not in {QUALIFICATION_ARTIFACT, EXAMPLE_ARTIFACT}:
        raise EvidenceError("artifact_kind must be 'qualification' or 'example'")
    if artifact_kind == EXAMPLE_ARTIFACT and not allow_example:
        raise EvidenceError("example capacity evidence is rejected outside explicit test validation")
    try:
        run_id = uuid.UUID(str(document["run_id"]))
    except (ValueError, AttributeError) as exc:
        raise EvidenceError("run_id must be a UUID") from exc
    if artifact_kind == QUALIFICATION_ARTIFACT and run_id.int == 0:
        raise EvidenceError("qualification evidence requires a non-zero run_id")

    generated_at = _parse_timestamp(document["generated_at"])
    if artifact_kind == QUALIFICATION_ARTIFACT and enforce_freshness:
        _validate_timestamp_freshness(
            generated_at,
            current_time=current_time,
            max_age_seconds=max_age_seconds,
            max_future_skew_seconds=max_future_skew_seconds,
        )
    _validate_subject(document["subject"], expected_subject, allow_example=allow_example)
    _validate_durability(document["durability"])
    configuration = _validate_configuration(
        document["configuration"], expected_configuration, allow_example=allow_example
    )

    forecast = _decimal(
        document["forecast_peak_shares_per_second"],
        "forecast_peak_shares_per_second",
    )
    if expected_forecast_peak_shares_per_second is None:
        if not allow_example:
            raise EvidenceError("externally configured forecast peak share rate is required")
    else:
        expected_forecast = _decimal(
            expected_forecast_peak_shares_per_second,
            "expected forecast peak shares per second",
        )
        if forecast != expected_forecast:
            raise EvidenceError(
                "forecast_peak_shares_per_second does not match the deployment value"
            )

    p99_limit = _decimal(
        document["ack_p99_limit_milliseconds"],
        "ack_p99_limit_milliseconds",
    )
    if expected_ack_p99_limit_milliseconds is None:
        if not allow_example:
            raise EvidenceError("externally configured ACK p99 limit is required")
    else:
        expected_p99_limit = _decimal(
            expected_ack_p99_limit_milliseconds,
            "expected ACK p99 limit milliseconds",
        )
        if p99_limit != expected_p99_limit:
            raise EvidenceError("ack_p99_limit_milliseconds does not match the deployment value")
    commit_timeout_milliseconds = (
        configuration["PRISM_SHARE_COMMIT_TIMEOUT_SECONDS"] * Decimal(1000)
    )
    if p99_limit > commit_timeout_milliseconds:
        raise EvidenceError(
            "ack_p99_limit_milliseconds cannot exceed PRISM_SHARE_COMMIT_TIMEOUT_SECONDS"
        )

    duration = _decimal(document["test_duration_seconds"], "test_duration_seconds")
    offered, acknowledged, committed = _validate_share_counts(document, "capacity run")
    measured_rate = Decimal(acknowledged) / duration
    capacity_multiple = measured_rate / forecast
    if capacity_multiple < Decimal(2):
        raise EvidenceError(
            "measured sustained rate must be at least 2x forecast peak: "
            f"measured={measured_rate} forecast={forecast}"
        )
    p50, p99 = _validate_latency(
        document["ack_latency_milliseconds"], "ack_latency_milliseconds", p99_limit
    )

    phases = _mapping(document["phases"], "phases")
    _exact_keys(phases, set(REQUIRED_PHASES), "phases")
    phase_summaries = {
        name: _validate_phase(name, phases[name], forecast=forecast, p99_limit=p99_limit)
        for name in REQUIRED_PHASES
    }
    if sum((phase.duration_seconds for phase in phase_summaries.values()), Decimal(0)) != duration:
        raise EvidenceError("phase durations must equal test_duration_seconds")
    if sum(phase.offered_valid_shares for phase in phase_summaries.values()) != offered:
        raise EvidenceError("phase offered-share totals must equal the capacity-run total")
    if sum(phase.acknowledged_shares for phase in phase_summaries.values()) != acknowledged:
        raise EvidenceError("phase acknowledged-share totals must equal the capacity-run total")
    if sum(phase.committed_shares for phase in phase_summaries.values()) != committed:
        raise EvidenceError("phase committed-share totals must equal the capacity-run total")

    return CapacityEvidenceSummary(
        generated_at=generated_at,
        measured_shares_per_second=measured_rate,
        capacity_multiple=capacity_multiple,
        acknowledged_shares=acknowledged,
        ack_p50_milliseconds=p50,
        ack_p99_milliseconds=p99,
        ack_p99_limit_milliseconds=p99_limit,
    )


def load_capacity_evidence(
    path: Path,
    **validation_options: object,
) -> CapacityEvidenceSummary:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except OSError as exc:
        raise EvidenceError(f"cannot read {path}: {exc.strerror or exc}") from exc
    except json.JSONDecodeError as exc:
        raise EvidenceError(f"{path} is not valid JSON: {exc.msg}") from exc
    return validate_capacity_evidence(payload, **validation_options)

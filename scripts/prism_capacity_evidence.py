#!/usr/bin/env python3
"""Validate a PRISM Stratum-to-Postgres capacity qualification artifact."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lab.prism.capacity_evidence import (  # noqa: E402
    CONFIGURATION_KEYS,
    DEFAULT_MAX_AGE_SECONDS,
    CapacityEvidenceSummary,
    EvidenceError,
    load_capacity_evidence,
    validate_capacity_evidence,
)


def _name_values(values: list[str], option: str) -> dict[str, str]:
    expected: dict[str, str] = {}
    for value in values:
        key, separator, configured_value = value.partition("=")
        if not separator or not key or not configured_value:
            raise EvidenceError(f"{option} must use NAME=VALUE, got {value!r}")
        expected[key] = configured_value
    return expected


def _expected_configuration(values: list[str]) -> dict[str, str]:
    expected = _name_values(values, "--expect")
    unknown = sorted(set(expected) - set(CONFIGURATION_KEYS))
    if unknown:
        raise EvidenceError(f"--expect contains unknown configuration keys: {', '.join(unknown)}")
    return expected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence_file", type=Path)
    parser.add_argument(
        "--expect",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="bind evidence to one deployment configuration value",
    )
    parser.add_argument("--expect-coordinator-revision")
    parser.add_argument("--expect-coordinator-image-digest")
    parser.add_argument("--expect-postgres-server-version")
    parser.add_argument("--expect-database-profile-sha256")
    parser.add_argument("--forecast-peak-shares-per-second")
    parser.add_argument("--ack-p99-limit-milliseconds")
    parser.add_argument(
        "--max-age-seconds",
        type=int,
        default=DEFAULT_MAX_AGE_SECONDS,
        help="maximum qualification-artifact age (default: 86400)",
    )
    parser.add_argument(
        "--allow-example-evidence-for-tests",
        action="store_true",
        help="accept committed example artifacts; never use for a deployment gate",
    )
    args = parser.parse_args(argv)

    expected_subject_values = {
        "coordinator_revision": args.expect_coordinator_revision,
        "coordinator_image_digest": args.expect_coordinator_image_digest,
        "postgres_server_version": args.expect_postgres_server_version,
        "database_profile_sha256": args.expect_database_profile_sha256,
    }
    expected_subject = (
        {key: value for key, value in expected_subject_values.items() if value is not None}
        or None
    )
    try:
        expected_configuration = _expected_configuration(args.expect) if args.expect else None
        summary = load_capacity_evidence(
            args.evidence_file,
            expected_configuration=expected_configuration,
            expected_subject=expected_subject,
            expected_forecast_peak_shares_per_second=args.forecast_peak_shares_per_second,
            expected_ack_p99_limit_milliseconds=args.ack_p99_limit_milliseconds,
            max_age_seconds=args.max_age_seconds,
            allow_example=args.allow_example_evidence_for_tests,
        )
    except EvidenceError as exc:
        print(f"capacity evidence invalid: {exc}", file=sys.stderr)
        return 1
    print(
        "capacity evidence valid: "
        f"rate={summary.measured_shares_per_second} shares/s "
        f"capacity={summary.capacity_multiple}x "
        f"ACK p50={summary.ack_p50_milliseconds}ms "
        f"p99={summary.ack_p99_milliseconds}ms "
        f"committed={summary.acknowledged_shares}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

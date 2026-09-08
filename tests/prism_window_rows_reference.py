"""Reference check for the per-row payout-window reads (issue #236).

The payout-window snapshot and delta project one JSON object per row and
decode it in bounded batches. Their selection CTEs did not change, so the
pre-#236 statement -- the same CTE with the rows folded into one ``json_agg``
value -- is a byte-exact oracle for membership, order and field values. This
module rebuilds that oracle from the statement the ledger actually sends and
compares the two on a live PostgreSQL, whichever backend the ledger runs.

Used by ``test/test-prism-postgres-ledger.sh`` (psql subprocess backend) and
``test/test-prism-postgres-native-ledger.sh`` (pooled psycopg backend).
"""

from __future__ import annotations

import re
from typing import Any, Callable

from lab.prism.share_ledger import AcceptedShareRecord, PsqlShareLedger

_ROW_PROJECTION = re.compile(
    r"SELECT (?P<object>json_build_object\(.*?\))\nFROM rows\nORDER BY share_seq ASC;",
    re.S,
)


def aggregate_reference_sql(row_sql: str) -> str:
    """Turn the per-row statement back into its pre-#236 ``json_agg`` form."""
    rewritten, count = _ROW_PROJECTION.subn(
        lambda match: (
            "SELECT COALESCE(json_agg("
            + match.group("object")
            + " ORDER BY share_seq ASC), '[]'::json)\nFROM rows;"
        ),
        row_sql,
    )
    if count != 1:
        raise AssertionError(
            "expected exactly one per-row projection in the window statement, "
            f"found {count}"
        )
    if "json_agg" in row_sql:
        raise AssertionError("the per-row statement still carries a json_agg")
    return rewritten


def capture_window_sql(
    ledger: PsqlShareLedger,
    call: Callable[[], list[AcceptedShareRecord]],
) -> tuple[str, list[AcceptedShareRecord]]:
    """Run ``call`` on ``ledger`` and return the row statement it sent."""
    statements: list[str] = []
    original = ledger._run_retry_safe_read_json_rows

    def recording(sql: str, **kwargs: Any) -> list[Any]:
        statements.append(sql)
        return original(sql, **kwargs)

    ledger._run_retry_safe_read_json_rows = recording  # type: ignore[method-assign]
    try:
        records = call()
    finally:
        del ledger._run_retry_safe_read_json_rows
    if len(statements) != 1:
        raise AssertionError(
            f"expected one row statement per window read, saw {len(statements)}"
        )
    return statements[0], records


def assert_rows_match_aggregate(
    ledger: PsqlShareLedger,
    call: Callable[[], list[AcceptedShareRecord]],
    *,
    label: str,
    expected_len: int | None = None,
) -> list[AcceptedShareRecord]:
    """Assert the per-row read equals the ``json_agg`` oracle, record for record."""
    row_sql, records = capture_window_sql(ledger, call)
    reference_sql = aggregate_reference_sql(row_sql)
    reference = [
        ledger._record_from_json(item) for item in ledger._run_json(reference_sql)
    ]
    if records != reference:
        detail = (
            f"rows={len(records)} reference={len(reference)}"
            if len(records) != len(reference)
            else next(
                f"first mismatch at index {index}: {left!r} != {right!r}"
                for index, (left, right) in enumerate(zip(records, reference))
                if left != right
            )
        )
        raise SystemExit(f"{label}: per-row window read differs from json_agg oracle ({detail})")
    if expected_len is not None and len(records) != expected_len:
        raise SystemExit(f"{label}: expected {expected_len} records, got {len(records)}")
    return records

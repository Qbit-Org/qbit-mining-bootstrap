#!/usr/bin/env python3
"""Backfill PRISM CTV fanout artifact rows from audit artifacts."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.share_ledger import PsqlShareLedger, canonical_hex, require_mapping, sha256_json_hex


HEX64_RE = re.compile(r"(?<![0-9a-fA-F])([0-9a-fA-F]{64})(?![0-9a-fA-F])")


@dataclass(frozen=True)
class CtvFanoutBackfillInput:
    source: str
    block_hash: str
    manifest_set: dict[str, Any]
    manifest_set_sha256: str


def load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc


def infer_block_hash_from_path(path: Path) -> str | None:
    matches = HEX64_RE.findall(path.name)
    if not matches:
        return None
    return canonical_hex(matches[-1], name="block_hash", expected_bytes=32)


def block_hash_from_payload(payload: dict[str, Any]) -> str | None:
    raw = payload.get("block_hash")
    if raw is None:
        found_block = payload.get("found_block")
        if isinstance(found_block, dict):
            raw = found_block.get("block_hash")
    if raw is None:
        return None
    return canonical_hex(str(raw), name="block_hash", expected_bytes=32)


def ctv_manifest_source(payload: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    if isinstance(payload.get("audit_bundle"), dict):
        payload = require_mapping(payload["audit_bundle"], "audit_bundle")

    if isinstance(payload.get("ctv_fanout_manifest_set"), dict):
        return require_mapping(payload["ctv_fanout_manifest_set"], "ctv_fanout_manifest_set"), None
    if isinstance(payload.get("manifest_set"), dict):
        manifest_set_sha256 = payload.get("manifest_set_sha256")
        return require_mapping(payload["manifest_set"], "manifest_set"), (
            None if manifest_set_sha256 is None else str(manifest_set_sha256)
        )
    if isinstance(payload.get("manifests"), list):
        return payload, None
    raise ValueError("payload does not contain a CTV fanout manifest set")


def backfill_input_from_payload(
    payload: dict[str, Any],
    *,
    source: str,
    block_hash: str | None = None,
    manifest_set_sha256: str | None = None,
) -> CtvFanoutBackfillInput:
    payload_block_hash = block_hash_from_payload(payload)
    resolved_block_hash = block_hash or payload_block_hash
    if resolved_block_hash is None:
        raise ValueError("block hash is required for CTV fanout backfill")
    resolved_block_hash = canonical_hex(resolved_block_hash, name="block_hash", expected_bytes=32)

    manifest_set, payload_manifest_set_sha256 = ctv_manifest_source(payload)
    resolved_manifest_set_sha256 = manifest_set_sha256 or payload_manifest_set_sha256 or sha256_json_hex(manifest_set)
    resolved_manifest_set_sha256 = canonical_hex(
        resolved_manifest_set_sha256,
        name="manifest_set_sha256",
        expected_bytes=32,
    )
    return CtvFanoutBackfillInput(
        source=source,
        block_hash=resolved_block_hash,
        manifest_set=manifest_set,
        manifest_set_sha256=resolved_manifest_set_sha256,
    )


def backfill_input_from_path(
    path: Path,
    *,
    block_hash: str | None = None,
    manifest_set_sha256: str | None = None,
) -> CtvFanoutBackfillInput:
    payload = load_json_file(path)
    if not isinstance(payload, dict):
        raise SystemExit(f"{path}: top-level JSON must be an object")
    inferred_hash = block_hash or block_hash_from_payload(payload) or infer_block_hash_from_path(path)
    try:
        return backfill_input_from_payload(
            payload,
            source=str(path),
            block_hash=inferred_hash,
            manifest_set_sha256=manifest_set_sha256,
        )
    except ValueError as exc:
        raise SystemExit(f"{path}: {exc}") from exc


def psql_command_from_env(explicit: str | None) -> str:
    if explicit:
        return explicit
    psql_command = os.environ.get("PRISM_POSTGRES_PSQL_COMMAND", "")
    if psql_command:
        return psql_command
    database_url = os.environ.get("PRISM_DATABASE_URL", "")
    if database_url:
        return f"psql {shlex.quote(database_url)}"
    raise SystemExit("set PRISM_POSTGRES_PSQL_COMMAND or PRISM_DATABASE_URL, or pass --psql-command")


def db_block_hashes_for_height(ledger: PsqlShareLedger, height: int) -> list[str]:
    if height < 0:
        raise SystemExit("--db-block-height values must be non-negative")
    rows = ledger._run_json(
        f"""
SELECT COALESCE(json_agg(block_hash ORDER BY found_at DESC), '[]'::json)
FROM qbit_pool_blocks
WHERE block_height = {int(height)}
  AND chain_state <> 'reversed';
"""
    )
    if not isinstance(rows, list):
        raise RuntimeError("block-height lookup returned non-array JSON")
    return [
        canonical_hex(str(block_hash), name="block_hash", expected_bytes=32)
        for block_hash in rows
    ]


def backfill_input_from_db(ledger: PsqlShareLedger, *, block_hash: str) -> CtvFanoutBackfillInput:
    block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
    row = ledger.audit_bundle(block_hash=block_hash)
    if row is None:
        raise SystemExit(f"no audit bundle found for block {block_hash}")
    audit_bundle = row.get("audit_bundle")
    if not isinstance(audit_bundle, dict):
        raise SystemExit(f"audit bundle body is unavailable for block {block_hash}")
    try:
        return backfill_input_from_payload(
            audit_bundle,
            source=f"db:{block_hash}",
            block_hash=block_hash,
        )
    except ValueError as exc:
        raise SystemExit(f"db:{block_hash}: {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill qbit_ctv_fanout_artifacts from PRISM CTV audit artifacts."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="local audit bundle, audit API payload, or CTV manifest-set JSON files",
    )
    parser.add_argument(
        "--path-block-hash",
        help="block hash for a single path when it cannot be inferred from JSON or filename",
    )
    parser.add_argument(
        "--path-manifest-set-sha256",
        help="manifest-set sha256 for a single path; defaults to canonical JSON sha256",
    )
    parser.add_argument("--db-block-hash", action="append", default=[], help="load an audit bundle from DB by block hash")
    parser.add_argument(
        "--db-block-height",
        action="append",
        type=int,
        default=[],
        help="load audit bundle(s) from DB by block height",
    )
    parser.add_argument("--psql-command", help="psql command; defaults to PRISM_POSTGRES_PSQL_COMMAND/PRISM_DATABASE_URL")
    parser.add_argument("--writer-id", default=os.environ.get("PRISM_LEDGER_WRITER_ID", "prism-ctv-fanout-backfill"))
    parser.add_argument("--writer-epoch", type=int, default=int(os.environ.get("PRISM_LEDGER_WRITER_EPOCH", "1")))
    parser.add_argument("--writer-session-token", default=os.environ.get("PRISM_LEDGER_WRITER_SESSION_TOKEN"))
    parser.add_argument(
        "--lease-ttl-seconds",
        type=float,
        default=float(os.environ.get("PRISM_LEDGER_LEASE_TTL_SECONDS", "60")),
    )
    parser.add_argument(
        "--no-init-schema",
        action="store_true",
        help="do not run the idempotent PRISM schema repair before backfilling",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if len(args.paths) != 1 and (args.path_block_hash or args.path_manifest_set_sha256):
        raise SystemExit("--path-block-hash and --path-manifest-set-sha256 require exactly one path")

    path_inputs = [
        backfill_input_from_path(
            path,
            block_hash=args.path_block_hash,
            manifest_set_sha256=args.path_manifest_set_sha256,
        )
        for path in args.paths
    ]
    db_hashes = [
        canonical_hex(block_hash, name="db_block_hash", expected_bytes=32)
        for block_hash in args.db_block_hash
    ]
    if not path_inputs and not db_hashes and not args.db_block_height:
        raise SystemExit("provide at least one path, --db-block-hash, or --db-block-height")

    ledger = PsqlShareLedger(
        psql_command=psql_command_from_env(args.psql_command),
        writer_id=args.writer_id,
        writer_epoch=args.writer_epoch,
        writer_session_token=args.writer_session_token,
        initialize_schema=not args.no_init_schema,
        lease_ttl_seconds=args.lease_ttl_seconds,
    )

    for height in args.db_block_height:
        height_hashes = db_block_hashes_for_height(ledger, height)
        if not height_hashes:
            raise SystemExit(f"no non-reversed PRISM blocks found at height {height}")
        db_hashes.extend(height_hashes)

    inputs = path_inputs + [
        backfill_input_from_db(ledger, block_hash=block_hash)
        for block_hash in dict.fromkeys(db_hashes)
    ]
    results = []
    for item in inputs:
        result = ledger.persist_ctv_fanout_manifest_set(
            block_hash=item.block_hash,
            manifest_set=item.manifest_set,
            manifest_set_sha256=item.manifest_set_sha256,
        )
        results.append(
            {
                "source": item.source,
                "block_hash": item.block_hash,
                "manifest_set_sha256": item.manifest_set_sha256,
                **result,
            }
        )

    print(
        json.dumps(
            {
                "schema": "qbit.prism.ctv-fanout-backfill.v1",
                "input_count": len(inputs),
                "results": results,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

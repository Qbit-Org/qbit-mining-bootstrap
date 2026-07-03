#!/usr/bin/env python3
"""Single-writer accepted-share ledger helpers for the direct PRISM coordinator."""

from __future__ import annotations

import json
import copy
import hashlib
import os
import math
import shlex
import subprocess
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import BoundedSemaphore, Lock
from typing import Any, Callable

from lab.prism.prism_tools import prism_tool_command


@dataclass(frozen=True)
class AcceptedShareRecord:
    share_seq: int
    share_id: str
    miner_id: str
    order_key: str
    p2mr_program_hex: str
    share_difficulty: int
    network_difficulty: int
    template_height: int
    job_id: str
    job_issued_at_ms: int
    accepted_at_ms: int
    ntime: int

    def to_prism_json(self) -> dict[str, object]:
        return {
            "share_seq": self.share_seq,
            "share_id": self.share_id,
            "miner_id": self.miner_id,
            "order_key": self.order_key,
            "p2mr_program_hex": self.p2mr_program_hex,
            "share_difficulty": self.share_difficulty,
            "network_difficulty": self.network_difficulty,
            "template_height": self.template_height,
            "job_id": self.job_id,
            "job_issued_at_ms": self.job_issued_at_ms,
            "accepted_at_ms": self.accepted_at_ms,
            "ntime": self.ntime,
        }


@dataclass(frozen=True)
class PrismWindowShare:
    share: AcceptedShareRecord
    counted_difficulty: Decimal


@dataclass(frozen=True)
class PendingShare:
    share_id: str
    miner_id: str
    order_key: str
    p2mr_program_hex: str
    share_difficulty: int
    network_difficulty: int
    template_height: int
    job_id: str
    job_issued_at_ms: int
    accepted_at_ms: int
    ntime: int


class SingleWriterShareLedger:
    """Assigns canonical share_seq values and returns immutable snapshots.

    The direct Stratum coordinator should append accepted shares through one
    instance of this class. Later Postgres integration can keep this API shape
    while moving storage to `qbit_share_ledger`.
    """

    def __init__(self, *, first_share_seq: int = 1):
        if first_share_seq < 1:
            raise ValueError("first_share_seq must be >= 1")
        self._next_share_seq = first_share_seq
        self._shares: list[AcceptedShareRecord] = []
        self._share_ids: set[str] = set()
        self._ctv_fanout_sets: dict[str, dict[str, Any]] = {}
        self._ctv_fanout_statuses: dict[str, dict[str, Any]] = {}
        self._ctv_fanout_attempts: dict[str, list[dict[str, Any]]] = {}
        self._lock = Lock()

    def append(self, pending: PendingShare) -> AcceptedShareRecord:
        if pending.share_difficulty <= 0:
            raise ValueError("share_difficulty must be positive")
        if pending.network_difficulty <= 0:
            raise ValueError("network_difficulty must be positive")
        with self._lock:
            if pending.share_id in self._share_ids:
                raise ValueError("duplicate share_id")
            record = AcceptedShareRecord(
                share_seq=self._next_share_seq,
                share_id=pending.share_id,
                miner_id=pending.miner_id,
                order_key=pending.order_key,
                p2mr_program_hex=pending.p2mr_program_hex,
                share_difficulty=pending.share_difficulty,
                network_difficulty=pending.network_difficulty,
                template_height=pending.template_height,
                job_id=pending.job_id,
                job_issued_at_ms=pending.job_issued_at_ms,
                accepted_at_ms=pending.accepted_at_ms,
                ntime=pending.ntime,
            )
            self._shares.append(record)
            self._share_ids.add(pending.share_id)
            self._next_share_seq += 1
            return record

    def snapshot_at_job_issue(self, anchor_job_issued_at_ms: int) -> list[AcceptedShareRecord]:
        with self._lock:
            return [
                replace(share)
                for share in self._shares
                if share.job_issued_at_ms <= anchor_job_issued_at_ms
                and share.accepted_at_ms <= anchor_job_issued_at_ms
            ]

    def all_shares(self) -> list[AcceptedShareRecord]:
        with self._lock:
            return [replace(share) for share in self._shares]

    def accepted_share_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "accepted_share_count": len(self._shares),
                "distinct_miner_count": len({share.miner_id for share in self._shares}),
            }

    def current_owed_balances(self) -> list[dict[str, object]]:
        return []

    def current_prior_balances(self) -> list[dict[str, object]]:
        return []

    def carry_forward_integrity_report(self) -> dict[str, object]:
        return {
            "schema": "qbit.prism.carry-forward-integrity.v1",
            "backend": "memory",
            "checked_active_rows": 0,
            "audit_chain_version": "qbit.prism.carry-forward-active-delta-chain.v1",
            "audit_row_count": 0,
            "audit_head_sha256": "00" * 32,
            "mismatch_count": 0,
            "mismatches": [],
        }

    def audit_share_window(
        self,
        *,
        anchor_job_issued_at_ms: int,
        network_difficulty: int,
    ) -> list[dict[str, object]]:
        return []

    def audit_block_payouts(self, *, block_hash: str) -> list[dict[str, object]]:
        return []

    def recipient_payout_history(self, *, recipient_id: str, limit: int = 50) -> list[dict[str, object]]:
        return []

    def audit_bundle(self, *, block_hash: str) -> dict[str, object] | None:
        return None

    def audit_bundle_by_commitment(self, *, commitment_leaf_hex: str) -> dict[str, object] | None:
        return None

    def persist_ctv_fanout_manifest_set(
        self,
        *,
        block_hash: str,
        manifest_set: dict[str, Any],
        manifest_set_sha256: str,
    ) -> dict[str, int | str]:
        payload = ctv_fanout_recovery_payload(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256=manifest_set_sha256,
        )
        with self._lock:
            existing = self._ctv_fanout_sets.get(block_hash)
            if existing is not None and existing != payload:
                raise RuntimeError("existing CTV fanout manifest set does not match payload")
            self._ctv_fanout_sets[block_hash] = copy.deepcopy(payload)
            for artifact in payload["artifacts"]:
                fanout_txid = str(artifact["fanout_txid"])
                existing_status = self._ctv_fanout_statuses.get(fanout_txid)
                status_payload = {
                    **copy.deepcopy(artifact),
                    "schema": "qbit.prism.ctv-fanout-status.v1",
                    "block_hash": block_hash,
                    "manifest_set_sha256": manifest_set_sha256,
                    "settlement_status": existing_status.get("settlement_status", "awaiting_maturity")
                    if existing_status
                    else "awaiting_maturity",
                    "broadcast_attempts": self._ctv_fanout_attempts.get(fanout_txid, []),
                }
                audit_bundle_sha256 = payload.get("audit_bundle_sha256")
                if audit_bundle_sha256 is not None:
                    status_payload["audit_bundle_sha256"] = audit_bundle_sha256
                self._ctv_fanout_statuses[fanout_txid] = status_payload
        return {
            "backend": "memory",
            "fanout_set_count": 1,
            "fanout_artifact_count": len(payload["artifacts"]),
        }

    def audit_ctv_fanout_manifest_set(self, *, block_hash: str) -> dict[str, object] | None:
        with self._lock:
            payload = self._ctv_fanout_sets.get(block_hash)
            return copy.deepcopy(payload) if payload is not None else None

    def audit_ctv_fanouts(self, *, block_hash: str) -> list[dict[str, object]]:
        with self._lock:
            payload = self._ctv_fanout_sets.get(block_hash)
            if payload is None:
                return []
            return copy.deepcopy(payload["artifacts"])

    def ctv_fanout_status(self, *, fanout_txid: str) -> dict[str, object] | None:
        with self._lock:
            payload = self._ctv_fanout_statuses.get(fanout_txid)
            return copy.deepcopy(payload) if payload is not None else None

    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]:
        limit = max(1, min(int(limit), 1_000))
        with self._lock:
            rows = [
                copy.deepcopy(payload)
                for payload in self._ctv_fanout_statuses.values()
                if payload.get("settlement_status") not in {"confirmed", "reorged"}
            ]
        rows.sort(key=lambda row: (str(row.get("block_hash", "")), int(row.get("chunk_index", 0))))
        return rows[:limit]

    def dashboard_pending_fanout_rows(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        with self._lock:
            rows = [
                copy.deepcopy(payload)
                for payload in self._ctv_fanout_statuses.values()
                if payload.get("settlement_status") not in {"confirmed", "reorged"}
            ]
        rows.sort(key=lambda row: (str(row.get("block_hash", "")), int(row.get("chunk_index", 0))))
        offset = (page - 1) * limit
        return {
            "pagination": public_api.pagination(page, limit, len(rows)),
            "rows": rows[offset : offset + limit],
        }

    def dashboard_public_artifact(self, *, sha256: str) -> dict[str, object] | None:
        with self._lock:
            for payload in self._ctv_fanout_sets.values():
                if payload.get("audit_bundle_sha256") == sha256:
                    audit_bundle = payload.get("audit_bundle")
                    return copy.deepcopy(audit_bundle) if isinstance(audit_bundle, dict) else None
                if payload.get("manifest_set_sha256") == sha256:
                    manifest_set = payload.get("manifest_set")
                    return copy.deepcopy(manifest_set) if isinstance(manifest_set, dict) else None
                for artifact in payload.get("artifacts", []):
                    if not isinstance(artifact, dict):
                        continue
                    if artifact.get("manifest_sha256") == sha256:
                        manifest = artifact.get("manifest")
                        return copy.deepcopy(manifest) if isinstance(manifest, dict) else None
        return None

    def update_ctv_fanout_status(self, *, fanout_txid: str, settlement_status: str) -> dict[str, int | str]:
        validate_ctv_fanout_status(settlement_status)
        with self._lock:
            if fanout_txid not in self._ctv_fanout_statuses:
                raise RuntimeError("unknown CTV fanout txid")
            self._ctv_fanout_statuses[fanout_txid]["settlement_status"] = settlement_status
        return {"backend": "memory", "updated_count": 1}

    def record_ctv_fanout_broadcast_attempt(
        self,
        *,
        fanout_txid: str,
        attempt_status: str,
        package_tx_hexes: list[str] | None = None,
        package_txids: list[str] | None = None,
        submit_result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, int | str]:
        validate_ctv_fanout_attempt_status(attempt_status)
        with self._lock:
            if fanout_txid not in self._ctv_fanout_statuses:
                raise RuntimeError("unknown CTV fanout txid")
            attempts = self._ctv_fanout_attempts.setdefault(fanout_txid, [])
            attempt = {
                "attempt_seq": len(attempts) + 1,
                "attempt_status": attempt_status,
                "package_tx_hexes": package_tx_hexes or [],
                "package_txids": package_txids or [],
                "submit_result": submit_result,
                "error": error,
            }
            attempts.append(attempt)
            self._ctv_fanout_statuses[fanout_txid]["broadcast_attempts"] = copy.deepcopy(attempts)
            if attempt_status in {"submitted", "accepted"}:
                self._ctv_fanout_statuses[fanout_txid]["settlement_status"] = "broadcast_submitted"
            elif attempt_status in {"rejected", "failed"}:
                self._ctv_fanout_statuses[fanout_txid]["settlement_status"] = "failed"
        return {"backend": "memory", "attempt_count": len(attempts)}

    def metrics(self) -> dict[str, int]:
        return {
            "shares": len(self),
            "blocks": 0,
            "confirmed_blocks": 0,
            "inactive_blocks": 0,
            "rejected_blocks": 0,
            "reversed_blocks": 0,
            "payout_entries": 0,
            "owed_accounts": 0,
        }

    def dashboard_pool_snapshot(
        self,
        *,
        current_network_difficulty: int | str | Decimal,
        generated_at: str,
    ) -> dict[str, object]:
        from lab.prism import public_api

        shares = self.all_shares()
        now = datetime.now(timezone.utc)
        window_weight = _reward_window_weight(current_network_difficulty)
        window_shares = _prism_window_shares(
            shares,
            anchor_job_issued_at_ms=int(now.timestamp() * 1000),
            requested_window_weight=window_weight,
        )
        newest = max((row.share.accepted_at_ms for row in window_shares), default=None)
        oldest = min((row.share.accepted_at_ms for row in window_shares), default=None)
        return {
            "hashrate_ths": {
                "h1": public_api.hashrate_ths_from_difficulty(
                    _share_difficulty_between(shares, now - timedelta(hours=1), now),
                    60 * 60,
                ),
                "h3": public_api.hashrate_ths_from_difficulty(
                    _share_difficulty_between(shares, now - timedelta(hours=3), now),
                    3 * 60 * 60,
                ),
                "h24": public_api.hashrate_ths_from_difficulty(
                    _share_difficulty_between(shares, now - timedelta(hours=24), now),
                    24 * 60 * 60,
                ),
            },
            "participants_3h": len(
                {
                    share.miner_id
                    for share in shares
                    if now - timedelta(hours=3) <= _datetime_from_ms(share.accepted_at_ms) <= now
                }
            ),
            "blocks_found_total": 0,
            "prism_blocks_total": 0,
            "total_mined_bits": 0,
            "latest_block": None,
            "reward_window": {
                "window_multiplier": 8,
                "requested_window_weight": public_api.decimal_string(window_weight),
                "oldest_share_accepted_at": _iso_from_ms(oldest),
                "newest_share_accepted_at": _iso_from_ms(newest),
                "included_share_count": len(window_shares),
            },
        }

    def dashboard_miner_reward_window(
        self,
        *,
        recipient_id: str,
        current_network_difficulty: int | str | Decimal,
    ) -> dict[str, object]:
        from lab.prism import public_api

        window_weight = _reward_window_weight(current_network_difficulty)
        window_shares = _prism_window_shares(
            self.all_shares(),
            anchor_job_issued_at_ms=int(datetime.now(timezone.utc).timestamp() * 1000),
            requested_window_weight=window_weight,
        )
        miner_difficulty = sum(
            row.counted_difficulty
            for row in window_shares
            if row.share.miner_id == recipient_id
        )
        pool_difficulty = sum(row.counted_difficulty for row in window_shares)
        share_percent = None
        if pool_difficulty > 0:
            share_percent = public_api.decimal_string(miner_difficulty * Decimal(100) / pool_difficulty)
        return {
            "accepted_difficulty": public_api.decimal_string(miner_difficulty),
            "pool_accepted_difficulty": public_api.decimal_string(pool_difficulty),
            "share_percent": share_percent,
        }

    def dashboard_blocks(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        return {"pagination": public_api.pagination(page, limit, 0), "rows": []}

    def dashboard_miner_lifetime_earnings_bits(self, *, recipient_id: str) -> int:
        return 0

    def dashboard_miner_payout_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        return {"pagination": public_api.pagination(page, limit, 0), "rows": []}

    def dashboard_miner_earning_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        return {"pagination": public_api.pagination(page, limit, 0), "rows": []}

    def dashboard_leaderboard(self, *, page: int, limit: int, search: str | None = None) -> dict[str, object]:
        from lab.prism import public_api

        now = datetime.now(timezone.utc)
        started = now - timedelta(hours=3)
        shares = [
            share
            for share in self.all_shares()
            if started <= _datetime_from_ms(share.accepted_at_ms) <= now
            and (not search or search.lower() in share.miner_id.lower())
        ]
        by_miner: dict[str, dict[str, object]] = {}
        for share in shares:
            row = by_miner.setdefault(
                share.miner_id,
                {
                    "recipient_id": share.miner_id,
                    "difficulty": 0,
                    "last_share_at_ms": share.accepted_at_ms,
                },
            )
            row["difficulty"] = int(row["difficulty"]) + int(share.share_difficulty)
            row["last_share_at_ms"] = max(int(row["last_share_at_ms"]), share.accepted_at_ms)
        total_difficulty = sum(int(row["difficulty"]) for row in by_miner.values())
        pool_hashrate_ths = public_api.hashrate_ths_from_difficulty(total_difficulty, 3 * 60 * 60)
        ranked = sorted(
            by_miner.values(),
            key=lambda row: (-int(row["difficulty"]), str(row["recipient_id"])),
        )
        rows: list[dict[str, object]] = []
        for index, row in enumerate(ranked, start=1):
            percent = None
            if total_difficulty:
                percent = public_api.decimal_string(Decimal(int(row["difficulty"])) * Decimal(100) / Decimal(total_difficulty))
            hashrate_ths = public_api.hashrate_ths_from_difficulty(int(row["difficulty"]), 3 * 60 * 60)
            rows.append(
                {
                    "rank": index,
                    "recipient_id": row["recipient_id"],
                    "display_name": None,
                    "hashrate_ths_3h": hashrate_ths,
                    "share_percent": percent,
                    "hash_percent": _hash_percent(hashrate_ths, pool_hashrate_ths),
                    "blocks_found": 0,
                    "last_share_at": _iso_from_ms(int(row["last_share_at_ms"])),
                }
            )
        offset = (page - 1) * limit
        return {
            "started_at": public_api.iso_datetime(started),
            "ended_at": public_api.iso_datetime(now),
            "totals": {
                "pool_hashrate_ths": pool_hashrate_ths,
                "pool_accepted_share_difficulty": str(total_difficulty),
                "participant_count": len(ranked),
            },
            "pagination": public_api.pagination(page, limit, len(ranked)),
            "rows": rows[offset : offset + limit],
        }

    def dashboard_hashrate_series(
        self,
        *,
        subject_type: str,
        subject_id: str | None,
        range_id: str,
        bucket: str,
    ) -> list[dict[str, object]]:
        from lab.prism import public_api

        now = datetime.now(timezone.utc)
        started = _series_start(now, range_id)
        bucket_seconds = {"5m": 300, "1h": 3600, "1d": 86400}[bucket]
        buckets: dict[int, dict[str, int]] = {}
        for share in self.all_shares():
            accepted_at = _datetime_from_ms(share.accepted_at_ms)
            if accepted_at < started or accepted_at > now:
                continue
            if subject_type == "miner" and share.miner_id != subject_id:
                continue
            bucket_epoch = int(accepted_at.timestamp()) // bucket_seconds * bucket_seconds
            entry = buckets.setdefault(bucket_epoch, {"count": 0, "difficulty": 0})
            entry["count"] += 1
            entry["difficulty"] += int(share.share_difficulty)
        return [
            {
                "timestamp": public_api.iso_datetime(datetime.fromtimestamp(bucket_epoch, timezone.utc)),
                "hashrate_ths": public_api.hashrate_ths_from_difficulty(entry["difficulty"], bucket_seconds),
                "accepted_share_count": entry["count"],
                "accepted_share_difficulty": str(entry["difficulty"]),
            }
            for bucket_epoch, entry in sorted(buckets.items())
        ]

    def persist_accepted_block(
        self,
        *,
        block_hash: str,
        block_height: int,
        parent_hash: str,
        final_bundle: dict[str, Any],
        audit_report: dict[str, Any],
    ) -> dict[str, int | str]:
        return {
            "backend": "memory",
            "share_count": len(self),
            "block_count": 0,
            "payout_entry_count": 0,
            "carry_forward_count": 0,
        }

    def reverse_immature_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        return {
            "backend": "memory",
            "reversed_count": 0,
        }

    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        return {
            "backend": "memory",
            "rejected_count": 0,
        }

    def confirm_accepted_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        return {
            "backend": "memory",
            "confirmed_count": 1,
        }

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        return []

    def mark_pool_block_inactive(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        return {
            "backend": "memory",
            "inactive_count": 0,
        }

    def reactivate_pool_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        return {
            "backend": "memory",
            "reactivated_count": 0,
        }

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, int | str]:
        return {
            "backend": "memory",
            "matured_count": 0,
        }

    @property
    def backend_name(self) -> str:
        return "memory"

    def __len__(self) -> int:
        with self._lock:
            return len(self._shares)


def _datetime_from_ms(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc)


def _iso_from_ms(timestamp_ms: int | None) -> str | None:
    if timestamp_ms is None:
        return None
    from lab.prism import public_api

    return public_api.iso_datetime(_datetime_from_ms(timestamp_ms))


def _share_difficulty_between(shares: list[AcceptedShareRecord], started_at: datetime, ended_at: datetime) -> int:
    return sum(
        int(share.share_difficulty)
        for share in shares
        if started_at <= _datetime_from_ms(share.accepted_at_ms) <= ended_at
    )


def _reward_window_weight(current_network_difficulty: int | str | Decimal) -> Decimal:
    difficulty = Decimal(str(current_network_difficulty))
    if difficulty < 0:
        difficulty = Decimal(0)
    return difficulty * Decimal(8)


def _hash_percent(hashrate_ths: str, pool_hashrate_ths: str) -> str | None:
    from lab.prism import public_api

    pool_hashrate = Decimal(str(pool_hashrate_ths))
    if pool_hashrate <= 0:
        return None
    return public_api.decimal_string(Decimal(str(hashrate_ths)) * Decimal(100) / pool_hashrate)


def _solver_worker_name_sql(share_id_column: str) -> str:
    """SQL expression deriving a worker name from a solving share's share_id.

    A solving share's share_id is "<stratum_username>:<block_hash>" (see
    pending_share_from_submission), and the stratum username is
    "<payout_address>[.<worker_name>]". Strip the trailing ":<block_hash>" segment
    to recover the username, then take the substring after the first '.'. Returns
    NULL when the share is absent (unattributed block) or carried no worker suffix,
    matching the nullable-string schema and mirroring the worker-name derivation in
    dashboard_miner_worker_rows.
    """
    username = f"regexp_replace({share_id_column}, ':[^:]*$', '')"
    dot = f"position('.' IN {username})"
    return (
        f"CASE WHEN {share_id_column} IS NULL THEN null "
        f"WHEN {dot} > 0 THEN NULLIF(substring({username} FROM {dot} + 1), '') "
        f"ELSE null END"
    )


def _prism_window_shares(
    shares: list[AcceptedShareRecord],
    *,
    anchor_job_issued_at_ms: int,
    requested_window_weight: int | Decimal,
) -> list[PrismWindowShare]:
    if requested_window_weight <= 0:
        return []
    requested = Decimal(str(requested_window_weight))
    total = Decimal(0)
    window_shares: list[PrismWindowShare] = []
    eligible = [
        share
        for share in shares
        if share.job_issued_at_ms <= anchor_job_issued_at_ms and share.accepted_at_ms <= anchor_job_issued_at_ms
    ]
    for share in sorted(eligible, key=lambda item: item.share_seq, reverse=True):
        if total >= requested:
            break
        share_difficulty = Decimal(int(share.share_difficulty))
        counted_difficulty = min(share_difficulty, requested - total)
        total += counted_difficulty
        window_shares.append(PrismWindowShare(share=share, counted_difficulty=counted_difficulty))
    return window_shares


def _series_start(now: datetime, range_id: str) -> datetime:
    if range_id == "1w":
        return now - timedelta(days=7)
    if range_id == "1m":
        return now - timedelta(days=30)
    if range_id == "6m":
        return now - timedelta(days=180)
    return datetime.fromtimestamp(0, timezone.utc)


class PsqlShareLedger:
    """Postgres-backed implementation of the coordinator share-ledger API.

    The process that owns this object is the single logical writer. It delegates
    sequence assignment to `qbit_share_ledger.share_seq` and uses the canonical
    SQL schema under `crates/qbit-prism/sql`.
    """

    def __init__(
        self,
        *,
        psql_command: str,
        writer_id: str = "prism-coordinator",
        writer_epoch: int = 1,
        writer_session_token: str | None = None,
        initialize_schema: bool = False,
        schema_path: Path | None = None,
        lease_retry_sleep: Callable[[float], None] | None = None,
        lease_retry_max_sleep_seconds: float = 15.0,
        lease_ttl_seconds: float = 60.0,
        read_concurrency: int = 4,
        audit_body_dir: str | Path | None = None,
        audit_bundle_canonicalizer: Callable[[dict[str, Any]], bytes] | None = None,
    ):
        if writer_epoch < 0:
            raise ValueError("writer_epoch must be >= 0")
        lease_retry_max_sleep_seconds = float(lease_retry_max_sleep_seconds)
        if lease_retry_max_sleep_seconds <= 0:
            raise ValueError("lease_retry_max_sleep_seconds must be positive")
        lease_ttl_seconds = float(lease_ttl_seconds)
        if not math.isfinite(lease_ttl_seconds) or lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be finite and positive")
        read_concurrency = int(read_concurrency)
        if read_concurrency <= 0:
            raise ValueError("read_concurrency must be positive")
        self._command = shlex.split(psql_command)
        if not self._command:
            raise ValueError("psql_command must not be empty")
        self._writer_id = writer_id
        self._writer_epoch = writer_epoch
        self._writer_session_token = writer_session_token or uuid.uuid4().hex
        self._lease_ttl_seconds = lease_ttl_seconds
        # SQL fragment for the writer-lease expiry. The lease is refreshed on
        # every append (the dominant liveness signal during active mining), so a
        # short TTL bounds how long a same-identity replacement writer waits
        # after an *ungraceful* crash. Graceful shutdown releases the lease
        # outright (see release_writer_lease), making restarts near-instant.
        self._lease_interval_sql = f"make_interval(secs => {lease_ttl_seconds})"
        self._lease_retry_sleep = lease_retry_sleep or time.sleep
        self._lease_retry_max_sleep_seconds = lease_retry_max_sleep_seconds
        self._lease_retry_min_sleep_seconds = min(0.25, self._lease_retry_max_sleep_seconds)
        self._lock = Lock()
        self._read_semaphore = BoundedSemaphore(read_concurrency)
        self._audit_body_dir = Path(audit_body_dir) if audit_body_dir else None
        self._audit_bundle_canonicalizer = audit_bundle_canonicalizer
        if initialize_schema:
            path = schema_path or Path(__file__).resolve().parents[2] / "crates/qbit-prism/sql/001_share_ledger.sql"
            self._run_sql(path.read_text(encoding="utf-8"))
        self._ensure_writer_lease()

    @property
    def backend_name(self) -> str:
        return "postgres-psql"

    def append(self, pending: PendingShare) -> AcceptedShareRecord:
        if pending.share_difficulty <= 0:
            raise ValueError("share_difficulty must be positive")
        if pending.network_difficulty <= 0:
            raise ValueError("network_difficulty must be positive")
        payload = {
            **pending.__dict__,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
existing_share AS (
    SELECT share_seq
    FROM qbit_share_ledger
    WHERE share_id = (SELECT data->>'share_id' FROM payload)
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
inserted AS (
    INSERT INTO qbit_share_ledger (
        share_id,
        miner_id,
        payout_order_key,
        p2mr_program,
        share_difficulty,
        network_difficulty,
        template_height,
        job_id,
        job_issued_at,
        ntime,
        accepted_at,
        accepted,
        writer_id,
        writer_epoch
    )
    SELECT
        data->>'share_id',
        data->>'miner_id',
        data->>'order_key',
        decode(data->>'p2mr_program_hex', 'hex'),
        (data->>'share_difficulty')::numeric,
        (data->>'network_difficulty')::numeric,
        (data->>'template_height')::bigint,
        data->>'job_id',
        to_timestamp(((data->>'job_issued_at_ms')::double precision / 1000.0)),
        (data->>'ntime')::bigint,
        to_timestamp(((data->>'accepted_at_ms')::double precision / 1000.0)),
        true,
        data->>'writer_id',
        (data->>'writer_epoch')::bigint
    FROM payload, lease
    WHERE NOT EXISTS (SELECT 1 FROM existing_share)
    RETURNING *
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN EXISTS (SELECT 1 FROM existing_share) THEN
        json_build_object('error', 'duplicate share_id')
    ELSE
        (SELECT json_build_object(
            'share_seq', share_seq,
            'share_id', share_id,
            'miner_id', miner_id,
            'order_key', payout_order_key,
            'p2mr_program_hex', encode(p2mr_program, 'hex'),
            'share_difficulty', share_difficulty::text,
            'network_difficulty', network_difficulty::text,
            'template_height', template_height,
            'job_id', job_id,
            'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
            'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
            'ntime', ntime
        ) FROM inserted)
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return self._record_from_json(result)

    def snapshot_at_job_issue(self, anchor_job_issued_at_ms: int) -> list[AcceptedShareRecord]:
        sql = f"""
WITH rows AS (
    SELECT *
    FROM qbit_share_ledger
    WHERE accepted
      AND job_issued_at <= to_timestamp(({int(anchor_job_issued_at_ms)}::double precision / 1000.0))
      AND accepted_at <= to_timestamp(({int(anchor_job_issued_at_ms)}::double precision / 1000.0))
    ORDER BY share_seq ASC
)
SELECT COALESCE(json_agg(json_build_object(
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'network_difficulty', network_difficulty::text,
    'template_height', template_height,
    'job_id', job_id,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'ntime', ntime
) ORDER BY share_seq ASC), '[]'::json)
FROM rows;
"""
        with self._lock:
            return [self._record_from_json(item) for item in self._run_json(sql)]

    def all_shares(self) -> list[AcceptedShareRecord]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'network_difficulty', network_difficulty::text,
    'template_height', template_height,
    'job_id', job_id,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'ntime', ntime
) ORDER BY share_seq ASC), '[]'::json)
FROM qbit_share_ledger
WHERE accepted;
"""
        with self._lock:
            return [self._record_from_json(item) for item in self._run_json(sql)]

    def accepted_share_stats(self) -> dict[str, int]:
        """Aggregate counts without materializing the full share history.

        Health checks and readiness gates only need these two numbers; the
        full ``all_shares`` fetch grows with ledger history and is far too
        heavy to run per health probe or per job build.
        """
        sql = """
SELECT json_build_object(
    'accepted_share_count', count(*),
    'distinct_miner_count', count(DISTINCT miner_id)
)
FROM qbit_share_ledger
WHERE accepted;
"""
        with self._lock:
            stats = self._run_json(sql)
        return {
            "accepted_share_count": int(stats["accepted_share_count"]),
            "distinct_miner_count": int(stats["distinct_miner_count"]),
        }

    def current_owed_balances(self) -> list[dict[str, object]]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'recipient_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'balance_sats', owed_balance_sats::text
) ORDER BY payout_order_key, miner_id, encode(p2mr_program, 'hex')), '[]'::json)
FROM qbit_current_owed_balances()
WHERE owed_balance_sats > 0;
"""
        with self._lock:
            balances = self._run_json(sql)
        for balance in balances:
            balance["balance_sats"] = int(balance["balance_sats"])
        return balances

    def current_prior_balances(self) -> list[dict[str, object]]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'recipient_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'balance_sats', balance_sats::text
) ORDER BY payout_order_key, miner_id, encode(p2mr_program, 'hex')), '[]'::json)
FROM qbit_current_carry_forward_balances();
"""
        with self._lock:
            balances = self._run_json(sql)
        for balance in balances:
            balance["balance_sats"] = int(balance["balance_sats"])
        return balances

    def carry_forward_integrity_report(self) -> dict[str, object]:
        sql = "SELECT qbit_carry_forward_integrity_report();"
        with self._lock:
            report = self._run_json(sql)
            audit_head = self._carry_forward_audit_head_locked()
        report["backend"] = "postgres-psql"
        report.update(audit_head)
        report["checked_active_rows"] = int(report["checked_active_rows"])
        report["mismatch_count"] = int(report["mismatch_count"])
        for row in report.get("mismatches", []):
            for key in (
                "prior_balance_sats",
                "expected_prior_balance_sats",
                "candidate_balance_sats",
                "expected_candidate_balance_sats",
                "carry_forward_balance_sats",
                "expected_carry_forward_balance_sats",
            ):
                row[key] = int(row[key])
        return report

    def _carry_forward_audit_head_locked(self) -> dict[str, object]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'carry_forward_seq', carry_forward_seq,
    'block_hash', block_hash,
    'block_height', block_height,
    'recipient_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'gross_amount_sats', gross_amount_sats,
    'prior_balance_sats', prior_balance_sats::text,
    'candidate_balance_sats', candidate_balance_sats::text,
    'onchain_amount_sats', onchain_amount_sats,
    'settlement_fee_sats', settlement_fee_sats,
    'carry_forward_balance_sats', carry_forward_balance_sats::text,
    'action', action,
    'maturity_state', maturity_state
) ORDER BY block_height ASC, carry_forward_seq ASC), '[]'::json)
FROM (
    SELECT ledger.*
    FROM qbit_payout_carry_forward ledger
    JOIN qbit_pool_blocks block
      ON block.block_hash = ledger.block_hash
    WHERE ledger.maturity_state <> 'reversed'
      AND block.chain_state = 'confirmed'
      AND block.maturity_state <> 'reversed'
) active;
"""
        rows = self._run_json(sql)
        previous = bytes.fromhex("00" * 32)
        version = "qbit.prism.carry-forward-active-delta-chain.v1"
        for row in rows:
            row_json = json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
            previous = hashlib.sha256(previous + row_json).digest()
        return {
            "audit_chain_version": version,
            "audit_row_count": len(rows),
            "audit_head_sha256": previous.hex() if rows else "00" * 32,
        }

    def audit_share_window(
        self,
        *,
        anchor_job_issued_at_ms: int,
        network_difficulty: int,
    ) -> list[dict[str, object]]:
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'window_multiplier', window_multiplier::text,
    'requested_window_weight', requested_window_weight::text,
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'counted_difficulty', counted_difficulty::text,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint
) ORDER BY share_seq DESC), '[]'::json)
FROM qbit_audit_share_window(
    to_timestamp(({int(anchor_job_issued_at_ms)}::double precision / 1000.0)),
    {int(network_difficulty)}::numeric
);
"""
        with self._lock:
            rows = self._run_json(sql)
        for row in rows:
            for key in (
                "window_multiplier",
                "requested_window_weight",
                "share_difficulty",
                "counted_difficulty",
            ):
                row[key] = int(row[key])
        return rows

    def audit_block_payouts(self, *, block_hash: str) -> list[dict[str, object]]:
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block_hash,
    'block_height', block_height,
    'coinbase_txid', coinbase_txid,
    'payout_manifest_sha256', payout_manifest_sha256,
    'chain_state', chain_state,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'onchain_amount_sats', onchain_amount_sats,
    'carry_forward_balance_sats', carry_forward_balance_sats::text,
    'action', action,
    'maturity_state', maturity_state
) ORDER BY payout_order_key, miner_id, encode(p2mr_program, 'hex')), '[]'::json)
FROM qbit_audit_block_payouts({self._text_literal(block_hash)});
"""
        with self._lock:
            rows = self._run_json(sql)
        for row in rows:
            row["carry_forward_balance_sats"] = int(row["carry_forward_balance_sats"])
        return rows

    def recipient_payout_history(self, *, recipient_id: str, limit: int = 50) -> list[dict[str, object]]:
        if not recipient_id:
            raise ValueError("recipient_id is required")
        limit = max(1, min(int(limit), 250))
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block.block_hash,
    'block_height', block.block_height,
    'coinbase_txid', block.coinbase_txid,
    'payout_manifest_sha256', block.payout_manifest_sha256,
    'recipient_id', payout.miner_id,
    'order_key', payout.payout_order_key,
    'p2mr_program_hex', encode(payout.p2mr_program, 'hex'),
    'onchain_amount_sats', payout.onchain_amount_sats,
    'carry_forward_balance_sats', payout.carry_forward_balance_sats::text,
    'action', payout.action,
    'maturity_state', payout.maturity_state,
    'created_at', payout.created_at::text
) ORDER BY payout.block_height DESC, payout.payout_entry_seq DESC), '[]'::json)
FROM (
    SELECT *
    FROM qbit_pool_payout_entries
    WHERE miner_id = {self._text_literal(recipient_id)}
    ORDER BY block_height DESC, payout_entry_seq DESC
    LIMIT {limit}
) payout
JOIN qbit_pool_blocks block
  ON block.block_hash = payout.block_hash;
"""
        with self._lock:
            rows = self._run_json(sql)
        for row in rows:
            row["carry_forward_balance_sats"] = int(row["carry_forward_balance_sats"])
        return rows

    def dashboard_miner_lifetime_earnings_bits(self, *, recipient_id: str) -> int:
        if not recipient_id:
            raise ValueError("recipient_id is required")
        sql = f"""
SELECT json_build_object(
    'lifetime_earnings_bits',
    COALESCE((
        SELECT sum(carry.gross_amount_sats)
        FROM qbit_payout_carry_forward carry
        JOIN qbit_pool_blocks block
          ON block.block_hash = carry.block_hash
        WHERE carry.miner_id = {self._text_literal(recipient_id)}
          AND carry.maturity_state <> 'reversed'
          AND block.chain_state <> 'reversed'
          AND block.maturity_state <> 'reversed'
    ), 0)
);
"""
        payload = self._run_read_json(sql)
        return int(payload["lifetime_earnings_bits"])

    def dashboard_miner_share_summary(self, *, recipient_id: str) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS now_at
),
pool AS (
    SELECT COALESCE(sum(share_difficulty), 0)::text AS h3_difficulty
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND accepted_at >= bounds.now_at - interval '3 hours'
      AND accepted_at <= bounds.now_at
),
miner_rollups AS (
    SELECT
        count(*) FILTER (WHERE accepted_at >= bounds.now_at - interval '3 hours') AS accepted_3h,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '1 minute'), 0)::text AS m1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '5 minutes'), 0)::text AS m5_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '10 minutes'), 0)::text AS m10_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '3 hours'), 0)::text AS h3_difficulty,
        COALESCE(sum(share_difficulty), 0)::text AS h24_difficulty
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND miner_id = {self._text_literal(recipient_id)}
      AND accepted_at >= bounds.now_at - interval '24 hours'
      AND accepted_at <= bounds.now_at
),
miner_last AS (
    SELECT
        to_char(max(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_share_at
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND miner_id = {self._text_literal(recipient_id)}
      AND accepted_at <= bounds.now_at
)
SELECT json_build_object(
    'accepted_3h', (SELECT accepted_3h FROM miner_rollups),
    'm1_difficulty', (SELECT m1_difficulty FROM miner_rollups),
    'm5_difficulty', (SELECT m5_difficulty FROM miner_rollups),
    'm10_difficulty', (SELECT m10_difficulty FROM miner_rollups),
    'h3_difficulty', (SELECT h3_difficulty FROM miner_rollups),
    'h24_difficulty', (SELECT h24_difficulty FROM miner_rollups),
    'pool_h3_difficulty', (SELECT h3_difficulty FROM pool),
    'last_share_at', (SELECT last_share_at FROM miner_last)
);
"""
        payload = self._run_read_json(sql)
        miner_3h_difficulty = str(payload.get("h3_difficulty") or "0")
        pool_3h_difficulty = str(payload.get("pool_h3_difficulty") or "0")
        m1_difficulty = str(payload.get("m1_difficulty") or "0")
        m5_difficulty = str(payload.get("m5_difficulty") or "0")
        m10_difficulty = str(payload.get("m10_difficulty") or "0")
        h24_difficulty = str(payload.get("h24_difficulty") or "0")
        return {
            "hashrate_ths": {
                "m1": public_api.hashrate_ths_from_difficulty(m1_difficulty, 60),
                "m5": public_api.hashrate_ths_from_difficulty(m5_difficulty, 5 * 60),
                "m10": public_api.hashrate_ths_from_difficulty(m10_difficulty, 10 * 60),
                "h3": public_api.hashrate_ths_from_difficulty(miner_3h_difficulty, 3 * 60 * 60),
                "h24": public_api.hashrate_ths_from_difficulty(h24_difficulty, 24 * 60 * 60),
            },
            "accepted_3h": int(payload.get("accepted_3h") or 0),
            "accepted_difficulty_3h": miner_3h_difficulty,
            "last_share_at": payload.get("last_share_at"),
            "share_percent": public_api.percent_string(miner_3h_difficulty, pool_3h_difficulty),
        }

    def dashboard_miner_reward_window(
        self,
        *,
        recipient_id: str,
        current_network_difficulty: int | str | Decimal,
    ) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        window_weight = _reward_window_weight(current_network_difficulty)
        window_weight_sql = public_api.decimal_string(window_weight)
        sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
),
window_rows AS (
    SELECT window_row.*
    FROM bounds
    CROSS JOIN LATERAL qbit_prism_window(bounds.ended_at, {window_weight_sql}::numeric) AS window_row
),
totals AS (
    SELECT
        COALESCE(sum(counted_difficulty), 0)::text AS pool_counted_difficulty,
        COALESCE(sum(counted_difficulty) FILTER (WHERE miner_id = {self._text_literal(recipient_id)}), 0)::text AS miner_counted_difficulty
    FROM window_rows
)
SELECT json_build_object(
    'pool_counted_difficulty', (SELECT pool_counted_difficulty FROM totals),
    'miner_counted_difficulty', (SELECT miner_counted_difficulty FROM totals)
);
"""
        payload = self._run_read_json(sql)
        pool_difficulty = Decimal(str(payload["pool_counted_difficulty"]))
        miner_difficulty = Decimal(str(payload["miner_counted_difficulty"]))
        share_percent = None
        if pool_difficulty > 0:
            share_percent = public_api.decimal_string(miner_difficulty * Decimal(100) / pool_difficulty)
        return {
            "accepted_difficulty": public_api.decimal_string(miner_difficulty),
            "pool_accepted_difficulty": public_api.decimal_string(pool_difficulty),
            "share_percent": share_percent,
        }

    def dashboard_miner_payout_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        offset = (page - 1) * limit
        sql = f"""
WITH filtered AS (
    SELECT
        payout.payout_entry_seq,
        payout.block_hash,
        payout.block_height,
        payout.miner_id,
        payout.payout_order_key,
        payout.p2mr_program,
        payout.onchain_amount_sats,
        payout.carry_forward_balance_sats,
        payout.action,
        payout.maturity_state,
        payout.created_at,
        block.coinbase_txid,
        block.payout_manifest_sha256
    FROM qbit_pool_payout_entries payout
    JOIN qbit_pool_blocks block
      ON block.block_hash = payout.block_hash
    WHERE payout.miner_id = {self._text_literal(recipient_id)}
      AND payout.maturity_state <> 'reversed'
      AND block.chain_state <> 'reversed'
      AND block.maturity_state <> 'reversed'
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY block_height DESC, payout_entry_seq DESC
    LIMIT {int(limit)} OFFSET {int(offset)}
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'block_hash', block_hash,
            'block_height', block_height,
            'coinbase_txid', coinbase_txid,
            'payout_manifest_sha256', payout_manifest_sha256,
            'recipient_id', miner_id,
            'order_key', payout_order_key,
            'p2mr_program_hex', encode(p2mr_program, 'hex'),
            'onchain_amount_sats', onchain_amount_sats,
            'carry_forward_balance_sats', carry_forward_balance_sats::text,
            'action', action,
            'maturity_state', maturity_state,
            'created_at', created_at::text
        ) ORDER BY block_height DESC, payout_entry_seq DESC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows = payload["rows"]
        for row in rows:
            row["carry_forward_balance_sats"] = int(row["carry_forward_balance_sats"])
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": [public_api.miner_payout_row(row) for row in rows],
        }

    def dashboard_miner_earning_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        offset = (page - 1) * limit
        sql = f"""
WITH filtered AS (
    SELECT
        carry.carry_forward_seq,
        carry.block_hash,
        carry.block_height,
        carry.miner_id,
        carry.payout_order_key,
        carry.p2mr_program,
        carry.gross_amount_sats,
        carry.onchain_amount_sats,
        carry.settlement_fee_sats,
        carry.carry_forward_balance_sats,
        carry.action,
        carry.maturity_state,
        carry.created_at,
        block.found_at,
        block.coinbase_txid,
        block.payout_manifest_sha256
    FROM qbit_payout_carry_forward carry
    JOIN qbit_pool_blocks block
      ON block.block_hash = carry.block_hash
    WHERE carry.miner_id = {self._text_literal(recipient_id)}
      AND carry.maturity_state <> 'reversed'
      AND block.chain_state <> 'reversed'
      AND block.maturity_state <> 'reversed'
),
page_base AS (
    SELECT *
    FROM filtered
    ORDER BY block_height DESC, carry_forward_seq DESC
    LIMIT {int(limit)} OFFSET {int(offset)}
),
block_totals AS (
    SELECT block_hash, sum(gross_amount_sats) AS block_gross_amount_sats
    FROM qbit_payout_carry_forward
    WHERE block_hash IN (SELECT block_hash FROM page_base)
    GROUP BY block_hash
),
page_rows AS (
    SELECT
        page_base.*,
        block_totals.block_gross_amount_sats,
        CASE
            WHEN block_totals.block_gross_amount_sats > 0 THEN
                (page_base.gross_amount_sats::numeric * 100::numeric / block_totals.block_gross_amount_sats::numeric)::text
            ELSE '0'
        END AS reward_share_percent
    FROM page_base
    JOIN block_totals
      ON block_totals.block_hash = page_base.block_hash
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'block_hash', block_hash,
            'block_height', block_height,
            'coinbase_txid', coinbase_txid,
            'payout_manifest_sha256', payout_manifest_sha256,
            'recipient_id', miner_id,
            'order_key', payout_order_key,
            'p2mr_program_hex', encode(p2mr_program, 'hex'),
            'gross_amount_sats', gross_amount_sats,
            'onchain_amount_sats', onchain_amount_sats,
            'settlement_fee_sats', settlement_fee_sats,
            'carry_forward_balance_sats', carry_forward_balance_sats::text,
            'action', action,
            'maturity_state', maturity_state,
            'created_at', created_at::text,
            'found_at', found_at::text,
            'block_gross_amount_sats', block_gross_amount_sats,
            'reward_share_percent', reward_share_percent
        ) ORDER BY block_height DESC, carry_forward_seq DESC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows = payload["rows"]
        for row in rows:
            row["carry_forward_balance_sats"] = int(row["carry_forward_balance_sats"])
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": [public_api.miner_earning_row(row) for row in rows],
        }

    def dashboard_miner_worker_rows(
        self,
        *,
        recipient_id: str,
        page: int,
        limit: int,
        search: str | None,
        hide_inactive: bool,
    ) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        offset = (page - 1) * limit
        filters = ["true"]
        if search:
            filters.append(f"strpos(lower(worker_name), {self._text_literal(search.lower())}) > 0")
        if hide_inactive:
            filters.append("active")
        where_filter = " AND ".join(filters)
        sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS now_at
),
named AS (
    SELECT
        CASE
            WHEN username = {self._text_literal(recipient_id)} THEN 'default'
            WHEN left(username, {len(recipient_id) + 1}) = {self._text_literal(recipient_id + ".")} THEN COALESCE(NULLIF(substr(username, {len(recipient_id) + 2}), ''), 'default')
            WHEN position('.' IN username) > 0 THEN COALESCE(NULLIF(substring(username FROM position('.' IN username) + 1), ''), 'default')
            ELSE 'default'
        END AS worker_name,
        share_difficulty,
        accepted_at
    FROM (
        SELECT
            regexp_replace(share_id, ':[^:]*$', '') AS username,
            share_difficulty,
            accepted_at
        FROM qbit_share_ledger
        WHERE accepted
          AND miner_id = {self._text_literal(recipient_id)}
    ) shares
),
grouped AS (
    SELECT
        worker_name,
        max(accepted_at) AS last_share_at,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= (SELECT now_at FROM bounds) - interval '1 minute'), 0)::text AS m1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= (SELECT now_at FROM bounds) - interval '3 hours'), 0)::text AS h3_difficulty,
        max(accepted_at) >= (SELECT now_at FROM bounds) - interval '10 minutes' AS active
    FROM named
    GROUP BY worker_name
),
filtered AS (
    SELECT *
    FROM grouped
    WHERE {where_filter}
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY active DESC, worker_name ASC
    LIMIT {int(limit)} OFFSET {int(offset)}
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'active_count', (SELECT count(*) FROM grouped WHERE active),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'worker_name', worker_name,
            'status', CASE WHEN active THEN 'active' ELSE 'inactive' END,
            'last_share_at', to_char(last_share_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'm1_difficulty', m1_difficulty,
            'h3_difficulty', h3_difficulty
        ) ORDER BY active DESC, worker_name ASC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows = [
            {
                "worker_name": row["worker_name"],
                "status": row["status"],
                "last_share_at": row["last_share_at"],
                "hashrate_ths_60s": public_api.hashrate_ths_from_difficulty(row["m1_difficulty"], 60),
                "hashrate_ths_3h": public_api.hashrate_ths_from_difficulty(row["h3_difficulty"], 3 * 60 * 60),
            }
            for row in payload["rows"]
        ]
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": rows,
            "active_count": int(payload["active_count"]),
        }

    def audit_bundle(self, *, block_hash: str) -> dict[str, object] | None:
        sql = f"""
SELECT COALESCE(
    (
        SELECT json_build_object(
            'block_hash', block_hash,
            'audit_bundle_sha256', audit_bundle_sha256,
            'coinbase_tx_hex', coinbase_tx_hex,
            'audit_bundle', audit_bundle,
            'body_uri', body_uri
        )
        FROM qbit_pool_audit_bundles
        WHERE block_hash = {self._text_literal(block_hash)}
    ),
    'null'::json
);
"""
        with self._lock:
            row = self._run_json(sql)
        return self._resolve_audit_bundle_row(row)

    def audit_bundle_by_commitment(self, *, commitment_leaf_hex: str) -> dict[str, object] | None:
        leaf = self._text_literal(commitment_leaf_hex)
        sql = f"""
SELECT COALESCE(
    (
        SELECT json_build_object(
            'block_hash', bundle.block_hash,
            'audit_commitment_leaf_hex', {leaf},
            'audit_bundle_sha256', bundle.audit_bundle_sha256,
            'coinbase_tx_hex', bundle.coinbase_tx_hex,
            'audit_bundle', bundle.audit_bundle,
            'body_uri', bundle.body_uri
        )
        FROM qbit_pool_audit_bundles bundle
        JOIN qbit_pool_blocks block ON block.block_hash = bundle.block_hash
        WHERE bundle.audit_commitment_leaves_hex ? {leaf}
           OR bundle.witness_merkle_leaves_hex ? {leaf}
           OR bundle.audit_bundle->'audit_commitment_leaves_hex' ? {leaf}
           OR bundle.audit_bundle->'witness_merkle_leaves_hex' ? {leaf}
        ORDER BY block.block_height DESC, bundle.created_at DESC, bundle.block_hash
        LIMIT 1
    ),
    'null'::json
);
"""
        with self._lock:
            row = self._run_json(sql)
        return self._resolve_audit_bundle_row(row)

    def persist_ctv_fanout_manifest_set(
        self,
        *,
        block_hash: str,
        manifest_set: dict[str, Any],
        manifest_set_sha256: str,
    ) -> dict[str, int | str]:
        payload = {
            **ctv_fanout_recovery_payload(
                block_hash=block_hash,
                manifest_set=manifest_set,
                manifest_set_sha256=manifest_set_sha256,
            ),
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
block_row AS (
    SELECT block_hash
    FROM qbit_pool_blocks
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
existing_set AS (
    SELECT *
    FROM qbit_ctv_fanout_sets
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
inserted_set AS (
    INSERT INTO qbit_ctv_fanout_sets (
        block_hash,
        manifest_set_json,
        manifest_set,
        manifest_set_sha256,
        settlement_mode,
        parent_coinbase_txid,
        parent_coinbase_tx_hex,
        fanout_count,
        fanout_output_sum_sats,
        covenant_output_value_sats
    )
    SELECT
        data->>'block_hash',
        data->>'manifest_set_json',
        data->'manifest_set',
        data->>'manifest_set_sha256',
        data->>'settlement_mode',
        data->>'parent_coinbase_txid',
        data->>'parent_coinbase_tx_hex',
        (data->>'fanout_count')::integer,
        (data->>'fanout_output_sum_sats')::bigint,
        (data->>'covenant_output_value_sats')::bigint
    FROM payload, lease, block_row
    WHERE NOT EXISTS (SELECT 1 FROM existing_set)
    RETURNING block_hash
),
artifacts AS (
    SELECT data, artifact
    FROM payload,
         jsonb_array_elements(data->'artifacts') AS artifact
),
matching_existing_set AS (
    SELECT existing_set.block_hash
    FROM existing_set, payload
    WHERE existing_set.manifest_set = data->'manifest_set'
      AND existing_set.manifest_set_json = data->>'manifest_set_json'
      AND existing_set.manifest_set_sha256 = data->>'manifest_set_sha256'
      AND existing_set.settlement_mode = data->>'settlement_mode'
      AND existing_set.parent_coinbase_txid = data->>'parent_coinbase_txid'
      AND existing_set.parent_coinbase_tx_hex = data->>'parent_coinbase_tx_hex'
      AND existing_set.fanout_count = (data->>'fanout_count')::integer
      AND existing_set.fanout_output_sum_sats = (data->>'fanout_output_sum_sats')::bigint
      AND existing_set.covenant_output_value_sats = (data->>'covenant_output_value_sats')::bigint
),
expected_artifact_rows AS (
    SELECT
        artifact->>'fanout_txid' AS fanout_txid,
        data->>'block_hash' AS block_hash,
        data->>'manifest_set_sha256' AS manifest_set_sha256,
        artifact->>'manifest_json' AS manifest_json,
        artifact->'manifest' AS manifest,
        artifact->>'manifest_sha256' AS manifest_sha256,
        artifact->>'precommitment_sha256' AS precommitment_sha256,
        artifact->>'ctv_hash' AS ctv_hash,
        artifact->>'commitment_witness_leaf_hex' AS commitment_witness_leaf_hex,
        (artifact->>'chunk_index')::integer AS chunk_index,
        (artifact->>'chunk_count')::integer AS chunk_count,
        artifact->>'parent_coinbase_txid' AS parent_coinbase_txid,
        (artifact->>'parent_coinbase_vout')::integer AS parent_coinbase_vout,
        artifact->>'fanout_tx_template_hex' AS fanout_tx_template_hex,
        artifact->>'fanout_tx_hex' AS fanout_tx_hex,
        (artifact->>'anchor_vout')::integer AS anchor_vout,
        (artifact->>'covenant_output_value_sats')::bigint AS covenant_output_value_sats,
        (artifact->>'fanout_output_sum_sats')::bigint AS fanout_output_sum_sats
    FROM artifacts
),
existing_artifact_rows AS (
    SELECT
        fanout_txid,
        block_hash,
        manifest_set_sha256,
        manifest_json,
        manifest,
        manifest_sha256,
        precommitment_sha256,
        ctv_hash,
        commitment_witness_leaf_hex,
        chunk_index,
        chunk_count,
        parent_coinbase_txid,
        parent_coinbase_vout,
        fanout_tx_template_hex,
        fanout_tx_hex,
        anchor_vout,
        covenant_output_value_sats,
        fanout_output_sum_sats
    FROM qbit_ctv_fanout_artifacts
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
artifact_extra AS (
    SELECT * FROM existing_artifact_rows
    EXCEPT ALL
    SELECT * FROM expected_artifact_rows
),
inserted_artifacts AS (
    INSERT INTO qbit_ctv_fanout_artifacts (
        fanout_txid,
        block_hash,
        manifest_set_sha256,
        manifest_json,
        manifest,
        manifest_sha256,
        precommitment_sha256,
        ctv_hash,
        commitment_witness_leaf_hex,
        chunk_index,
        chunk_count,
        parent_coinbase_txid,
        parent_coinbase_vout,
        fanout_tx_template_hex,
        fanout_tx_hex,
        anchor_vout,
        covenant_output_value_sats,
        fanout_output_sum_sats
    )
    SELECT
        expected.fanout_txid,
        expected.block_hash,
        expected.manifest_set_sha256,
        expected.manifest_json,
        expected.manifest,
        expected.manifest_sha256,
        expected.precommitment_sha256,
        expected.ctv_hash,
        expected.commitment_witness_leaf_hex,
        expected.chunk_index,
        expected.chunk_count,
        expected.parent_coinbase_txid,
        expected.parent_coinbase_vout,
        expected.fanout_tx_template_hex,
        expected.fanout_tx_hex,
        expected.anchor_vout,
        expected.covenant_output_value_sats,
        expected.fanout_output_sum_sats
    FROM expected_artifact_rows expected
    WHERE EXISTS (SELECT 1 FROM lease)
      AND (
          EXISTS (SELECT 1 FROM inserted_set)
          OR (
              EXISTS (SELECT 1 FROM matching_existing_set)
              AND NOT EXISTS (SELECT 1 FROM artifact_extra)
          )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM existing_artifact_rows existing
          WHERE existing.fanout_txid = expected.fanout_txid
      )
    RETURNING fanout_txid
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM block_row) = 0 THEN
        json_build_object('error', 'unknown PRISM block')
    WHEN (SELECT count(*) FROM existing_set) > 0
      AND (SELECT count(*) FROM matching_existing_set) = 0 THEN
        json_build_object('error', 'existing CTV fanout manifest set does not match payload')
    WHEN (SELECT count(*) FROM existing_set) > 0
      AND EXISTS (SELECT 1 FROM artifact_extra) THEN
        json_build_object('error', 'existing CTV fanout artifacts do not match payload')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'fanout_set_count', CASE
                WHEN (SELECT count(*) FROM inserted_set) > 0 THEN (SELECT count(*) FROM inserted_set)
                ELSE (SELECT count(*) FROM existing_set)
            END,
            'fanout_artifact_count',
                (SELECT count(*) FROM existing_artifact_rows)
                + (SELECT count(*) FROM inserted_artifacts)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "fanout_set_count": int(result["fanout_set_count"]),
            "fanout_artifact_count": int(result["fanout_artifact_count"]),
        }

    def audit_ctv_fanout_manifest_set(self, *, block_hash: str) -> dict[str, object] | None:
        sql = f"SELECT qbit_audit_block_fanouts({self._text_literal(block_hash)});"
        return self._run_read_json(sql)

    def audit_ctv_fanouts(self, *, block_hash: str) -> list[dict[str, object]]:
        payload = self.audit_ctv_fanout_manifest_set(block_hash=block_hash)
        if payload is None:
            return []
        artifacts = payload.get("artifacts", [])
        if not isinstance(artifacts, list):
            return []
        return artifacts

    def ctv_fanout_status(self, *, fanout_txid: str) -> dict[str, object] | None:
        sql = f"SELECT qbit_fanout_status({self._text_literal(fanout_txid)});"
        return self._run_read_json(sql)

    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]:
        limit = max(1, min(int(limit), 1_000))
        sql = f"""
SELECT COALESCE(json_agg(row_payload ORDER BY block_height ASC, chunk_index ASC), '[]'::json)
FROM (
    SELECT json_build_object(
        'schema', 'qbit.prism.ctv-fanout-status.v1',
        'fanout_txid', artifact.fanout_txid,
        'block_hash', artifact.block_hash,
        'block_height', block.block_height,
        'parent_hash', block.parent_hash,
        'chain_state', block.chain_state,
        'maturity_state', block.maturity_state,
        'coinbase_txid', block.coinbase_txid,
        'payout_manifest_sha256', block.payout_manifest_sha256,
        'audit_bundle_sha256', bundle.audit_bundle_sha256,
        'manifest_set_sha256', artifact.manifest_set_sha256,
        'manifest_sha256', artifact.manifest_sha256,
        'precommitment_sha256', artifact.precommitment_sha256,
        'ctv_hash', artifact.ctv_hash,
        'commitment_witness_leaf_hex', artifact.commitment_witness_leaf_hex,
        'chunk_index', artifact.chunk_index,
        'chunk_count', artifact.chunk_count,
        'parent_coinbase_txid', artifact.parent_coinbase_txid,
        'parent_coinbase_vout', artifact.parent_coinbase_vout,
        'fanout_tx_hex', artifact.fanout_tx_hex,
        'anchor_vout', artifact.anchor_vout,
        'covenant_output_value_sats', artifact.covenant_output_value_sats,
        'fanout_output_sum_sats', artifact.fanout_output_sum_sats,
        'settlement_status', artifact.settlement_status,
        'updated_at', artifact.updated_at::text,
        'broadcast_attempts', COALESCE(
            (
                SELECT json_agg(json_build_object(
                    'attempt_seq', attempt.attempt_seq,
                    'attempted_at', attempt.attempted_at::text,
                    'attempt_status', attempt.attempt_status,
                    'package_tx_hexes', attempt.package_tx_hexes,
                    'package_txids', attempt.package_txids,
                    'submit_result', attempt.submit_result,
                    'error', attempt.error
                ) ORDER BY attempt.attempt_seq ASC)
                FROM qbit_ctv_fanout_broadcast_attempts attempt
                WHERE attempt.fanout_txid = artifact.fanout_txid
            ),
            '[]'::json
        )
    ) AS row_payload,
    block.block_height,
    artifact.chunk_index
    FROM qbit_ctv_fanout_artifacts artifact
    JOIN qbit_pool_blocks block
      ON block.block_hash = artifact.block_hash
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = artifact.block_hash
    WHERE artifact.settlement_status NOT IN ('confirmed', 'reorged')
    ORDER BY block.block_height ASC, artifact.chunk_index ASC
    LIMIT {limit}
) pending;
"""
        return self._run_read_json(sql)

    def dashboard_pending_fanout_rows(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        offset = (page - 1) * limit
        sql = f"""
WITH filtered AS (
    SELECT
        artifact.fanout_txid,
        artifact.block_hash,
        block.block_height,
        block.parent_hash,
        block.chain_state,
        block.maturity_state,
        block.coinbase_txid,
        block.payout_manifest_sha256,
        bundle.audit_bundle_sha256,
        artifact.manifest_set_sha256,
        artifact.manifest_sha256,
        artifact.precommitment_sha256,
        artifact.ctv_hash,
        artifact.commitment_witness_leaf_hex,
        artifact.chunk_index,
        artifact.chunk_count,
        artifact.parent_coinbase_txid,
        artifact.parent_coinbase_vout,
        artifact.fanout_tx_hex,
        artifact.anchor_vout,
        artifact.covenant_output_value_sats,
        artifact.fanout_output_sum_sats,
        artifact.settlement_status,
        artifact.updated_at
    FROM qbit_ctv_fanout_artifacts artifact
    JOIN qbit_pool_blocks block
      ON block.block_hash = artifact.block_hash
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = artifact.block_hash
    WHERE artifact.settlement_status NOT IN ('confirmed', 'reorged')
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY block_height ASC, chunk_index ASC
    LIMIT {int(limit)} OFFSET {int(offset)}
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'schema', 'qbit.prism.ctv-fanout-status.v1',
            'fanout_txid', fanout_txid,
            'block_hash', block_hash,
            'block_height', block_height,
            'parent_hash', parent_hash,
            'chain_state', chain_state,
            'maturity_state', maturity_state,
            'coinbase_txid', coinbase_txid,
            'payout_manifest_sha256', payout_manifest_sha256,
            'audit_bundle_sha256', audit_bundle_sha256,
            'manifest_set_sha256', manifest_set_sha256,
            'manifest_sha256', manifest_sha256,
            'precommitment_sha256', precommitment_sha256,
            'ctv_hash', ctv_hash,
            'commitment_witness_leaf_hex', commitment_witness_leaf_hex,
            'chunk_index', chunk_index,
            'chunk_count', chunk_count,
            'parent_coinbase_txid', parent_coinbase_txid,
            'parent_coinbase_vout', parent_coinbase_vout,
            'fanout_tx_hex', fanout_tx_hex,
            'anchor_vout', anchor_vout,
            'covenant_output_value_sats', covenant_output_value_sats,
            'fanout_output_sum_sats', fanout_output_sum_sats,
            'settlement_status', settlement_status,
            'updated_at', updated_at::text,
            'broadcast_attempts', COALESCE(
                (
                    SELECT json_agg(json_build_object(
                        'attempt_seq', attempt.attempt_seq,
                        'attempted_at', attempt.attempted_at::text,
                        'attempt_status', attempt.attempt_status,
                        'package_tx_hexes', attempt.package_tx_hexes,
                        'package_txids', attempt.package_txids,
                        'submit_result', attempt.submit_result,
                        'error', attempt.error
                    ) ORDER BY attempt.attempt_seq ASC)
                    FROM qbit_ctv_fanout_broadcast_attempts attempt
                    WHERE attempt.fanout_txid = fanout_txid
                ),
                '[]'::json
            )
        ) ORDER BY block_height ASC, chunk_index ASC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": payload["rows"],
        }

    def dashboard_public_artifact(self, *, sha256: str) -> dict[str, object] | None:
        sha256 = str(sha256).lower()
        lit = self._text_literal(sha256)
        sql = f"""
WITH audit AS (
    SELECT audit_bundle, audit_bundle_sha256, body_uri
    FROM qbit_pool_audit_bundles
    WHERE audit_bundle_sha256 = {lit}
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT json_build_object(
    'audit_bundle', (SELECT audit_bundle FROM audit),
    'audit_bundle_sha256', (SELECT audit_bundle_sha256 FROM audit),
    'body_uri', (SELECT body_uri FROM audit),
    'has_audit_row', (SELECT count(*) FROM audit) > 0,
    'fallback', COALESCE(
        (
            SELECT manifest_set
            FROM qbit_ctv_fanout_sets
            WHERE manifest_set_sha256 = {lit}
            ORDER BY created_at DESC
            LIMIT 1
        ),
        (
            SELECT manifest
            FROM qbit_ctv_fanout_artifacts
            WHERE manifest_sha256 = {lit}
            ORDER BY updated_at DESC
            LIMIT 1
        )
    )
);
"""
        row = self._run_read_json(sql)
        if not isinstance(row, dict):
            return None
        if row.get("has_audit_row"):
            body = row.get("audit_bundle")
            if body is None:
                body = self._read_external_body(row.get("body_uri"), expected_sha256=sha256)
            if body is not None:
                return body
        fallback = row.get("fallback")
        return fallback if isinstance(fallback, dict) else None

    def update_ctv_fanout_status(self, *, fanout_txid: str, settlement_status: str) -> dict[str, int | str]:
        validate_ctv_fanout_status(settlement_status)
        payload = {
            "fanout_txid": fanout_txid,
            "settlement_status": settlement_status,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
updated AS (
    UPDATE qbit_ctv_fanout_artifacts
    SET settlement_status = (SELECT data->>'settlement_status' FROM payload),
        updated_at = clock_timestamp()
    FROM lease
    WHERE fanout_txid = (SELECT data->>'fanout_txid' FROM payload)
    RETURNING fanout_txid
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'updated_count', (SELECT count(*) FROM updated)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {"backend": str(result["backend"]), "updated_count": int(result["updated_count"])}

    def record_ctv_fanout_broadcast_attempt(
        self,
        *,
        fanout_txid: str,
        attempt_status: str,
        package_tx_hexes: list[str] | None = None,
        package_txids: list[str] | None = None,
        submit_result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, int | str]:
        validate_ctv_fanout_attempt_status(attempt_status)
        next_status = None
        if attempt_status in {"submitted", "accepted"}:
            next_status = "broadcast_submitted"
        elif attempt_status in {"rejected", "failed"}:
            next_status = "failed"
        payload = {
            "fanout_txid": fanout_txid,
            "attempt_status": attempt_status,
            "package_tx_hexes": package_tx_hexes or [],
            "package_txids": package_txids or [],
            "submit_result": submit_result,
            "error": error,
            "next_status": next_status,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
inserted AS (
    INSERT INTO qbit_ctv_fanout_broadcast_attempts (
        fanout_txid,
        attempt_status,
        package_tx_hexes,
        package_txids,
        submit_result,
        error
    )
    SELECT
        data->>'fanout_txid',
        data->>'attempt_status',
        data->'package_tx_hexes',
        data->'package_txids',
        data->'submit_result',
        data->>'error'
    FROM payload, lease
    RETURNING attempt_seq
),
updated AS (
    UPDATE qbit_ctv_fanout_artifacts
    SET settlement_status = (SELECT data->>'next_status' FROM payload),
        updated_at = clock_timestamp()
    FROM inserted
    WHERE fanout_txid = (SELECT data->>'fanout_txid' FROM payload)
      AND (SELECT data->>'next_status' FROM payload) IS NOT NULL
    RETURNING fanout_txid
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'attempt_count', (SELECT count(*) FROM inserted),
            'updated_count', (SELECT count(*) FROM updated)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "attempt_count": int(result["attempt_count"]),
            "updated_count": int(result["updated_count"]),
        }

    def metrics(self) -> dict[str, int]:
        sql = """
SELECT json_build_object(
    'shares', (SELECT count(*) FROM qbit_share_ledger WHERE accepted),
    'blocks', (SELECT count(*) FROM qbit_pool_blocks),
    'confirmed_blocks', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'confirmed'),
    'inactive_blocks', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'inactive'),
    'rejected_blocks', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'rejected'),
    'reversed_blocks', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'reversed'),
    'payout_entries', (SELECT count(*) FROM qbit_pool_payout_entries),
    'owed_accounts', (SELECT count(*) FROM qbit_current_owed_balances() WHERE owed_balance_sats > 0)
);
"""
        with self._lock:
            metrics = self._run_json(sql)
        return {str(key): int(value) for key, value in metrics.items()}

    def dashboard_pool_snapshot(
        self,
        *,
        current_network_difficulty: int | str | Decimal,
        generated_at: str,
    ) -> dict[str, object]:
        from lab.prism import public_api

        window_weight = _reward_window_weight(current_network_difficulty)
        window_weight_sql = public_api.decimal_string(window_weight)
        sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
),
latest_block_row AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256
    FROM qbit_pool_blocks block
    WHERE block.chain_state <> 'reversed'
    ORDER BY block.block_height DESC, block.found_at DESC
    LIMIT 1
),
latest_block AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256,
        bundle.audit_bundle_sha256,
        solver.miner_id AS solver_recipient_id,
        solver.share_id AS solver_share_id
    FROM latest_block_row block
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = block.block_hash
    LEFT JOIN LATERAL (
        SELECT share.miner_id, share.share_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
),
window_rows AS (
    SELECT window_row.*
    FROM bounds
    CROSS JOIN LATERAL qbit_prism_window(bounds.ended_at, {window_weight_sql}::numeric) AS window_row
),
window_summary AS (
    SELECT
        count(*) AS included_share_count,
        to_char(min(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS oldest_share_accepted_at,
        to_char(max(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS newest_share_accepted_at
    FROM window_rows
),
rollups AS (
    SELECT
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '1 hour'), 0)::text AS h1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '3 hours'), 0)::text AS h3_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '24 hours'), 0)::text AS h24_difficulty,
        count(DISTINCT miner_id) FILTER (WHERE accepted_at >= bounds.ended_at - interval '3 hours') AS participants_3h
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND accepted_at >= bounds.ended_at - interval '24 hours'
      AND accepted_at <= bounds.ended_at
)
SELECT json_build_object(
    'h1_difficulty', (SELECT h1_difficulty FROM rollups),
    'h3_difficulty', (SELECT h3_difficulty FROM rollups),
    'h24_difficulty', (SELECT h24_difficulty FROM rollups),
    'participants_3h', (SELECT participants_3h FROM rollups),
    'blocks_found_total', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state <> 'reversed'),
    'prism_blocks_total', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state <> 'reversed'),
    'total_mined_bits', COALESCE((
        SELECT sum(carry.gross_amount_sats)
        FROM qbit_payout_carry_forward carry
        JOIN qbit_pool_blocks block
          ON block.block_hash = carry.block_hash
        WHERE block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
          AND carry.maturity_state <> 'reversed'
    ), 0),
    'latest_block', COALESCE((
        SELECT json_build_object(
            'height', latest_block.block_height,
            'hash', latest_block.block_hash,
            'found_at', to_char(latest_block.found_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'age_seconds', GREATEST(0, floor(extract(epoch FROM (clock_timestamp() - latest_block.found_at)))::bigint),
            'solver_recipient_id', COALESCE(latest_block.solver_recipient_id, ''),
            'solver_worker_name', {_solver_worker_name_sql("latest_block.solver_share_id")}
        )
        FROM latest_block
    ), 'null'::json),
    'oldest_share_accepted_at', (SELECT oldest_share_accepted_at FROM window_summary),
    'newest_share_accepted_at', (SELECT newest_share_accepted_at FROM window_summary),
    'included_share_count', (SELECT included_share_count FROM window_summary)
);
"""
        row = self._run_read_json(sql)
        return {
            "hashrate_ths": {
                "h1": public_api.hashrate_ths_from_difficulty(row["h1_difficulty"], 60 * 60),
                "h3": public_api.hashrate_ths_from_difficulty(row["h3_difficulty"], 3 * 60 * 60),
                "h24": public_api.hashrate_ths_from_difficulty(row["h24_difficulty"], 24 * 60 * 60),
            },
            "participants_3h": int(row["participants_3h"]),
            "blocks_found_total": int(row["blocks_found_total"]),
            "prism_blocks_total": int(row["prism_blocks_total"]),
            "total_mined_bits": int(row["total_mined_bits"]),
            "latest_block": row["latest_block"],
            "reward_window": {
                "window_multiplier": 8,
                "requested_window_weight": public_api.decimal_string(window_weight),
                "oldest_share_accepted_at": row["oldest_share_accepted_at"],
                "newest_share_accepted_at": row["newest_share_accepted_at"],
                "included_share_count": int(row["included_share_count"]),
            },
        }

    def dashboard_blocks(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        offset = (page - 1) * limit
        explorer_prefix = os.environ.get("PRISM_PUBLIC_EXPLORER_BLOCK_URL_PREFIX")
        sql = f"""
WITH total AS (
    SELECT count(*) AS total_count
    FROM qbit_pool_blocks
    WHERE chain_state <> 'reversed'
),
page_blocks AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256
    FROM qbit_pool_blocks block
    WHERE block.chain_state <> 'reversed'
    ORDER BY block.block_height DESC, block.found_at DESC
    LIMIT {int(limit)} OFFSET {int(offset)}
),
rows AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256,
        COALESCE(bundle.found_block_network_difficulty::text, bundle.audit_bundle#>>'{{found_block,network_difficulty}}') AS audit_network_difficulty,
        COALESCE(bundle.found_block_bits, bundle.audit_bundle#>>'{{found_block,bits}}') AS audit_bits,
        COALESCE(bundle.found_block_coinbase_value_sats::text, bundle.audit_bundle#>>'{{found_block,coinbase_value_sats}}') AS audit_coinbase_value_sats,
        bundle.audit_bundle_sha256,
        solver.miner_id AS solver_recipient_id,
        solver.share_difficulty::text AS solver_share_difficulty,
        solver.network_difficulty::text AS solver_network_difficulty,
        solver.share_id AS solver_share_id
    FROM page_blocks block
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = block.block_hash
    LEFT JOIN LATERAL (
        SELECT share.miner_id, share.share_difficulty, share.network_difficulty, share.share_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
)
SELECT json_build_object(
    'total_count', (SELECT total_count FROM total),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'height', rows.block_height,
            'hash', rows.block_hash,
            'found_at', to_char(rows.found_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'network_difficulty', COALESCE(rows.audit_network_difficulty, rows.solver_network_difficulty, '0'),
            'bits', COALESCE(rows.audit_bits, '00000000'),
            'solver_recipient_id', COALESCE(rows.solver_recipient_id, ''),
            'solver_worker_name', {_solver_worker_name_sql("rows.solver_share_id")},
            'solver_share_difficulty', rows.solver_share_difficulty,
            'reward_window_weight', CASE
                WHEN rows.audit_network_difficulty IS NULL THEN null
                ELSE (rows.audit_network_difficulty::numeric * 8::numeric)::text
            END,
            'coinbase_value_bits', COALESCE(rows.audit_coinbase_value_sats::bigint, 0),
            'audit_bundle_sha256', rows.audit_bundle_sha256,
            'payout_manifest_sha256', rows.payout_manifest_sha256,
            'explorer_url', null
        ) ORDER BY rows.block_height DESC, rows.found_at DESC)
        FROM rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows = payload["rows"]
        if explorer_prefix:
            for row in rows:
                row["explorer_url"] = explorer_prefix.rstrip("/") + "/" + str(row["hash"])
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": rows,
        }

    def dashboard_leaderboard(self, *, page: int, limit: int, search: str | None = None) -> dict[str, object]:
        from lab.prism import public_api

        offset = (page - 1) * limit
        search_filter = ""
        if search:
            search_filter = f"WHERE strpos(lower(miner_id), {self._text_literal(search.lower())}) > 0"
        sql = f"""
	WITH snapshot_clock AS (
	    SELECT clock_timestamp() AS ended_at
	),
	bounds AS (
	    SELECT ended_at, ended_at - interval '3 hours' AS started_at
	    FROM snapshot_clock
	),
	windowed AS (
	    SELECT ledger.*
	    FROM qbit_share_ledger ledger, bounds
	    WHERE ledger.accepted
	      AND ledger.accepted_at >= bounds.started_at
	      AND ledger.accepted_at <= bounds.ended_at
	),
grouped AS (
    SELECT
        miner_id,
        sum(share_difficulty) AS accepted_share_difficulty,
        max(accepted_at) AS last_share_at
    FROM windowed
    GROUP BY miner_id
),
filtered AS (
    SELECT *
    FROM grouped
    {search_filter}
),
blocks AS (
    SELECT solver.miner_id, count(*) AS blocks_found
    FROM qbit_pool_blocks block
    JOIN LATERAL (
        SELECT share.miner_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
    WHERE block.chain_state <> 'reversed'
    GROUP BY solver.miner_id
),
totals AS (
    SELECT
        COALESCE(sum(accepted_share_difficulty), 0) AS total_difficulty,
        count(*) AS participant_count
    FROM filtered
),
ranked AS (
    SELECT
        row_number() OVER (ORDER BY accepted_share_difficulty DESC, filtered.miner_id ASC) AS rank,
        filtered.miner_id,
        filtered.accepted_share_difficulty,
        filtered.last_share_at,
        COALESCE(blocks.blocks_found, 0) AS blocks_found
    FROM filtered
    LEFT JOIN blocks
      ON blocks.miner_id = filtered.miner_id
),
page_rows AS (
    SELECT *
    FROM ranked
    ORDER BY rank ASC
    LIMIT {int(limit)} OFFSET {int(offset)}
)
SELECT json_build_object(
    'started_at', (SELECT to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') FROM bounds),
    'ended_at', (SELECT to_char(ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') FROM bounds),
    'total_difficulty', (SELECT total_difficulty::text FROM totals),
    'participant_count', (SELECT participant_count FROM totals),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'rank', page_rows.rank,
            'recipient_id', page_rows.miner_id,
            'display_name', null,
            'accepted_share_difficulty', page_rows.accepted_share_difficulty::text,
            'share_percent', CASE
                WHEN (SELECT total_difficulty FROM totals) > 0 THEN
                    (page_rows.accepted_share_difficulty * 100::numeric / (SELECT total_difficulty FROM totals))::text
                ELSE null
            END,
            'blocks_found', page_rows.blocks_found,
            'last_share_at', to_char(page_rows.last_share_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        ) ORDER BY page_rows.rank ASC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows: list[dict[str, object]] = []
        total_difficulty = str(payload["total_difficulty"])
        pool_hashrate_ths = public_api.hashrate_ths_from_difficulty(total_difficulty, 3 * 60 * 60)
        for row in payload["rows"]:
            share_percent = row["share_percent"]
            hashrate_ths = public_api.hashrate_ths_from_difficulty(
                row["accepted_share_difficulty"],
                3 * 60 * 60,
            )
            rows.append(
                {
                    "rank": int(row["rank"]),
                    "recipient_id": row["recipient_id"],
                    "display_name": row["display_name"],
                    "hashrate_ths_3h": hashrate_ths,
                    "share_percent": public_api.decimal_string(share_percent) if share_percent is not None else None,
                    "hash_percent": _hash_percent(hashrate_ths, pool_hashrate_ths),
                    "blocks_found": int(row["blocks_found"]),
                    "last_share_at": row["last_share_at"],
                }
            )
        participant_count = int(payload["participant_count"])
        return {
            "started_at": payload["started_at"],
            "ended_at": payload["ended_at"],
            "totals": {
                "pool_hashrate_ths": pool_hashrate_ths,
                "pool_accepted_share_difficulty": total_difficulty,
                "participant_count": participant_count,
            },
            "pagination": public_api.pagination(page, limit, participant_count),
            "rows": rows,
        }

    def dashboard_hashrate_series(
        self,
        *,
        subject_type: str,
        subject_id: str | None,
        range_id: str,
        bucket: str,
    ) -> list[dict[str, object]]:
        from lab.prism import public_api

        bucket_seconds = {"5m": 300, "1h": 3600, "1d": 86400}[bucket]
        range_filter = {
            "1w": "AND ledger.accepted_at >= bounds.ended_at - interval '7 days'",
            "1m": "AND ledger.accepted_at >= bounds.ended_at - interval '30 days'",
            "6m": "AND ledger.accepted_at >= bounds.ended_at - interval '180 days'",
            "window": "AND ledger.accepted_at >= bounds.ended_at - interval '3 hours'",
            "all": "",
        }[range_id]
        subject_filter = ""
        if subject_type == "miner":
            subject_filter = f"AND ledger.miner_id = {self._text_literal(str(subject_id))}"
        sql = f"""
	WITH bounds AS (
	    SELECT clock_timestamp() AS ended_at
	),
	bucketed AS (
	    SELECT
	        floor(extract(epoch FROM ledger.accepted_at) / {int(bucket_seconds)})::bigint * {int(bucket_seconds)} AS bucket_epoch,
	        count(*) AS accepted_share_count,
	        sum(ledger.share_difficulty) AS accepted_share_difficulty
	    FROM qbit_share_ledger ledger, bounds
	    WHERE ledger.accepted
	      AND ledger.accepted_at <= bounds.ended_at
	      {range_filter}
	      {subject_filter}
	    GROUP BY bucket_epoch
	)
SELECT COALESCE(json_agg(json_build_object(
    'timestamp', to_char(to_timestamp(bucket_epoch) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'accepted_share_count', accepted_share_count,
    'accepted_share_difficulty', accepted_share_difficulty::text
) ORDER BY bucket_epoch ASC), '[]'::json)
FROM bucketed;
"""
        rows = self._run_read_json(sql)
        return [
            {
                "timestamp": row["timestamp"],
                "hashrate_ths": public_api.hashrate_ths_from_difficulty(row["accepted_share_difficulty"], bucket_seconds),
                "accepted_share_count": int(row["accepted_share_count"]),
                "accepted_share_difficulty": str(row["accepted_share_difficulty"]),
            }
            for row in rows
        ]

    def _externalize_audit_body(
        self,
        block_hash: str,
        audit_bundle_sha256: str,
        final_bundle: dict[str, Any],
    ) -> str | None:
        """Write the audit-bundle body to the external store and return its path.

        Returns None when no body store is configured, in which case the caller
        keeps the body inline in Postgres (legacy behavior). Externalizing the
        body is what stops the per-block audit_bundle JSONB from growing with the
        full accepted-share history.
        """
        if self._audit_body_dir is None:
            return None
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        audit_bundle_sha256 = canonical_hex(
            str(audit_bundle_sha256),
            name="audit_bundle_sha256",
            expected_bytes=32,
        )
        body_bytes = self._canonical_audit_body_bytes_for_sha(final_bundle, audit_bundle_sha256)
        return self._write_external_audit_body(block_hash, audit_bundle_sha256, body_bytes)

    def _canonical_audit_body_bytes_for_sha(
        self,
        final_bundle: dict[str, Any],
        audit_bundle_sha256: str,
    ) -> bytes:
        body_bytes = self._canonical_audit_bundle_bytes(final_bundle)
        actual_sha256 = sha256_bytes_hex(body_bytes)
        if actual_sha256 != str(audit_bundle_sha256).lower():
            raise RuntimeError(
                "audit bundle sha256 mismatch: "
                f"expected {str(audit_bundle_sha256).lower()}, got {actual_sha256}"
            )
        return body_bytes

    def _write_external_audit_body(
        self,
        block_hash: str,
        audit_bundle_sha256: str,
        body_bytes: bytes,
    ) -> str | None:
        if self._audit_body_dir is None:
            return None
        self._audit_body_dir.mkdir(parents=True, exist_ok=True)
        body_path = self._audit_body_path(block_hash, audit_bundle_sha256)
        if body_path.exists():
            existing = body_path.read_bytes()
            if existing != body_bytes:
                raise RuntimeError(f"existing audit bundle body does not match payload at {body_path}")
            return str(body_path)
        tmp_path = body_path.with_name(f".{body_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with tmp_path.open("xb") as handle:
                handle.write(body_bytes)
                handle.flush()
                os.fsync(handle.fileno())
            tmp_path.replace(body_path)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
        return str(body_path)

    def _canonical_audit_bundle_bytes(self, final_bundle: dict[str, Any]) -> bytes:
        if self._audit_bundle_canonicalizer is not None:
            canonical = self._audit_bundle_canonicalizer(final_bundle)
            return canonical.encode() if isinstance(canonical, str) else bytes(canonical)
        completed = subprocess.run(
            prism_tool_command("qbit-prism-audit-canonicalize")
            + [
                "--input",
                "-",
            ],
            input=json.dumps(final_bundle).encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            stderr = completed.stderr.decode(errors="replace").strip()
            raise RuntimeError(f"qbit-prism-audit-canonicalize failed: {stderr}")
        return completed.stdout

    def _audit_body_path(self, block_hash: str, audit_bundle_sha256: str) -> Path:
        if self._audit_body_dir is None:
            raise RuntimeError("audit body store is not configured")
        root = self._audit_body_dir.resolve()
        body_path = root / f"prism-audit-bundle-body-{block_hash}-{audit_bundle_sha256}.json"
        return self._resolve_audit_body_path(body_path)

    def _resolve_audit_body_path(self, body_uri: object) -> Path:
        body_path = Path(str(body_uri)).expanduser().resolve()
        if self._audit_body_dir is not None:
            root = self._audit_body_dir.resolve()
            try:
                body_path.relative_to(root)
            except ValueError as exc:
                raise RuntimeError(f"audit bundle body path escapes audit body store: {body_uri}") from exc
        return body_path

    def _external_audit_body_write_plan(self, payload: dict[str, Any]) -> str | None:
        """Refresh the writer lease and decide whether this persist may write a body.

        The audit body store lives outside Postgres, so file writes cannot be in
        the same transaction as the ledger insert. This preflight keeps stale
        writers from creating artifacts by requiring the DB lease to be current
        before any filesystem side effect.
        """
        if self._audit_body_dir is None:
            return None
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
existing_block AS (
    SELECT
        block_hash,
        block_height,
        parent_hash,
        coinbase_txid,
        payout_manifest_sha256
    FROM qbit_pool_blocks
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
matching_existing_block AS (
    SELECT existing_block.block_hash
    FROM existing_block, payload
    WHERE existing_block.block_height = (data->>'block_height')::bigint
      AND existing_block.parent_hash = data->>'parent_hash'
      AND existing_block.coinbase_txid = data->>'coinbase_txid'
      AND existing_block.payout_manifest_sha256 = data->>'payout_manifest_sha256'
),
existing_bundle AS (
    SELECT block_hash, audit_bundle_sha256, coinbase_tx_hex, body_uri
    FROM qbit_pool_audit_bundles
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
matching_existing_bundle AS (
    SELECT existing_bundle.block_hash
    FROM existing_bundle, payload
    WHERE existing_bundle.audit_bundle_sha256 = data->>'audit_bundle_sha256'
      AND existing_bundle.coinbase_tx_hex = data->>'coinbase_tx_hex'
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (SELECT count(*) FROM matching_existing_block) = 0 THEN
        json_build_object('error', 'existing block metadata does not match payload')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (SELECT count(*) FROM matching_existing_bundle) = 0 THEN
        json_build_object('error', 'existing audit bundle does not match payload')
    ELSE
        json_build_object(
            'existing_block', (SELECT count(*) FROM existing_block) > 0,
            'existing_body_uri', (SELECT body_uri FROM existing_bundle LIMIT 1)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        existing_body_uri = result.get("existing_body_uri")
        if existing_body_uri:
            return str(existing_body_uri)
        if result.get("existing_block"):
            return None
        return str(self._audit_body_path(payload["block_hash"], payload["audit_bundle_sha256"]))

    def _prepare_external_audit_body(
        self,
        payload: dict[str, Any],
        final_bundle: dict[str, Any],
    ) -> str | None:
        if self._audit_body_dir is None:
            return None
        payload = {
            **payload,
            "block_hash": canonical_hex(str(payload["block_hash"]), name="block_hash", expected_bytes=32),
            "audit_bundle_sha256": canonical_hex(
                str(payload["audit_bundle_sha256"]),
                name="audit_bundle_sha256",
                expected_bytes=32,
            ),
        }
        body_bytes = self._canonical_audit_body_bytes_for_sha(
            final_bundle,
            str(payload["audit_bundle_sha256"]),
        )
        body_uri = self._external_audit_body_write_plan(payload)
        if body_uri is None:
            return None
        body_path = self._resolve_audit_body_path(body_uri)
        if body_path.exists():
            if body_path.read_bytes() != body_bytes:
                raise RuntimeError(f"existing audit bundle body does not match payload at {body_path}")
            return str(body_path)
        canonical_body_path = self._audit_body_path(
            str(payload["block_hash"]),
            str(payload["audit_bundle_sha256"]),
        )
        if body_path != canonical_body_path:
            raise RuntimeError(
                "existing audit bundle body pointer does not match canonical external path: "
                f"{body_uri}"
            )
        restored_body_uri = self._write_external_audit_body(
            str(payload["block_hash"]),
            str(payload["audit_bundle_sha256"]),
            body_bytes,
        )
        if restored_body_uri is None:
            raise RuntimeError("audit body store is not configured")
        restored_body_path = Path(restored_body_uri).resolve()
        if restored_body_path != body_path:
            raise RuntimeError(
                "existing audit bundle body pointer does not match canonical external path: "
                f"{body_uri}"
            )
        return restored_body_uri

    def _read_external_body(
        self,
        body_uri: object,
        *,
        expected_sha256: object | None = None,
    ) -> dict[str, object] | None:
        if not body_uri:
            return None
        try:
            body_path = self._resolve_audit_body_path(body_uri)
            body_bytes = body_path.read_bytes()
        except OSError as exc:
            raise RuntimeError(
                f"audit bundle body is not retrievable at {body_uri}: {exc}"
            ) from exc
        if expected_sha256:
            expected = str(expected_sha256).lower()
            actual = sha256_bytes_hex(body_bytes)
            if actual != expected:
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {body_uri}: expected {expected}, got {actual}"
                )
        try:
            body = json.loads(body_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: {exc}") from exc
        return body

    def _resolve_audit_bundle_row(self, row: object) -> dict[str, object] | None:
        """Return an audit-bundle row with its body resolved inline.

        Reads the externalized body from body_uri when the inline JSONB is NULL,
        so legacy inline rows and externalized rows present an identical shape to
        callers.
        """
        if not isinstance(row, dict):
            return None
        result = dict(row)
        body = result.get("audit_bundle")
        if body is None:
            body = self._read_external_body(
                result.get("body_uri"),
                expected_sha256=result.get("audit_bundle_sha256"),
            )
        result.pop("body_uri", None)
        if body is None:
            return None
        result["audit_bundle"] = body
        return result

    def persist_accepted_block(
        self,
        *,
        block_hash: str,
        block_height: int,
        parent_hash: str,
        final_bundle: dict[str, Any],
        audit_report: dict[str, Any],
    ) -> dict[str, int | str]:
        manifest = final_bundle["signed_coinbase_manifest"]["manifest"]
        found_block = final_bundle.get("found_block") or {}
        audit_bundle_sha256 = canonical_hex(
            str(audit_report["audit_bundle_sha256_hex"]),
            name="audit_bundle_sha256",
            expected_bytes=32,
        )
        block_hash = canonical_hex(str(block_hash), name="block_hash", expected_bytes=32)
        parent_hash = canonical_hex(str(parent_hash), name="parent_hash", expected_bytes=32)
        payload = {
            "block_hash": block_hash,
            "block_height": block_height,
            "parent_hash": parent_hash,
            "coinbase_txid": audit_report["coinbase_txid"],
            "payout_manifest_sha256": audit_report["coinbase_manifest_sha256_hex"],
            "audit_bundle_sha256": audit_bundle_sha256,
            "coinbase_tx_hex": audit_report["coinbase_tx_hex"],
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        body_uri = self._prepare_external_audit_body(payload, final_bundle)
        payload = {
            **payload,
            # Externalized rows store the body in body_uri and NULL here; legacy
            # rows (no body store configured) keep the inline body.
            "audit_bundle": None if body_uri is not None else final_bundle,
            "body_uri": body_uri,
            "schema_version": str(final_bundle.get("schema") or "qbit.prism.audit-bundle.v1"),
            "found_block_network_difficulty": found_block.get("network_difficulty"),
            "found_block_bits": found_block.get("bits"),
            "found_block_coinbase_value_sats": found_block.get("coinbase_value_sats"),
            "audit_commitment_leaves_hex": final_bundle.get("audit_commitment_leaves_hex"),
            "witness_merkle_leaves_hex": final_bundle.get("witness_merkle_leaves_hex"),
            "accounts": final_bundle["payout_policy_manifest"]["accounts"],
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
existing_block AS (
    SELECT
        block_hash,
        block_height,
        parent_hash,
        coinbase_txid,
        payout_manifest_sha256
    FROM qbit_pool_blocks
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
inserted_block AS (
    INSERT INTO qbit_pool_blocks (
        block_hash,
        block_height,
        parent_hash,
        coinbase_txid,
        payout_manifest_sha256
    )
    SELECT
        data->>'block_hash',
        (data->>'block_height')::bigint,
        data->>'parent_hash',
        data->>'coinbase_txid',
        data->>'payout_manifest_sha256'
    FROM payload, lease
    WHERE NOT EXISTS (SELECT 1 FROM existing_block)
    RETURNING block_hash
),
accounts AS (
    SELECT
        data,
        account->>'recipient_id' AS miner_id,
        account->>'order_key' AS payout_order_key,
        decode(account->>'p2mr_program_hex', 'hex') AS p2mr_program,
        (account->>'gross_amount_sats')::bigint AS gross_amount_sats,
        (account->>'prior_balance_sats')::numeric AS prior_balance_sats,
        (account->>'candidate_balance_sats')::numeric AS candidate_balance_sats,
        (account->>'onchain_amount_sats')::bigint AS onchain_amount_sats,
        COALESCE((account->>'settlement_fee_sats')::bigint, 0) AS settlement_fee_sats,
        (account->>'carry_forward_balance_sats')::numeric AS carry_forward_balance_sats,
        account->>'action' AS action
    FROM payload,
         jsonb_array_elements(data->'accounts') AS account
),
bundle_insert AS (
    INSERT INTO qbit_pool_audit_bundles (
        block_hash,
        audit_bundle,
        audit_bundle_sha256,
        coinbase_tx_hex,
        body_uri,
        schema_version,
        found_block_network_difficulty,
        found_block_bits,
        found_block_coinbase_value_sats,
        audit_commitment_leaves_hex,
        witness_merkle_leaves_hex
    )
    SELECT
        data->>'block_hash',
        CASE WHEN jsonb_typeof(data->'audit_bundle') = 'object' THEN data->'audit_bundle' ELSE NULL END,
        data->>'audit_bundle_sha256',
        data->>'coinbase_tx_hex',
        data->>'body_uri',
        data->>'schema_version',
        (data->>'found_block_network_difficulty')::numeric,
        data->>'found_block_bits',
        (data->>'found_block_coinbase_value_sats')::bigint,
        CASE WHEN jsonb_typeof(data->'audit_commitment_leaves_hex') = 'array' THEN data->'audit_commitment_leaves_hex' ELSE NULL END,
        CASE WHEN jsonb_typeof(data->'witness_merkle_leaves_hex') = 'array' THEN data->'witness_merkle_leaves_hex' ELSE NULL END
    FROM payload, inserted_block
    RETURNING block_hash
),
payout_insert AS (
    INSERT INTO qbit_pool_payout_entries (
        block_hash,
        block_height,
        miner_id,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action
    )
    SELECT
        data->>'block_hash',
        (data->>'block_height')::bigint,
        miner_id,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action
    FROM accounts, inserted_block
    RETURNING payout_entry_seq
),
carry_insert AS (
    INSERT INTO qbit_payout_carry_forward (
        block_height,
        block_hash,
        miner_id,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        prior_balance_sats,
        candidate_balance_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action
    )
    SELECT
        (data->>'block_height')::bigint,
        data->>'block_hash',
        miner_id,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        prior_balance_sats,
        candidate_balance_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action
    FROM accounts, inserted_block
    RETURNING carry_forward_seq
),
matching_existing_block AS (
    SELECT existing_block.block_hash
    FROM existing_block, payload
    WHERE existing_block.block_height = (data->>'block_height')::bigint
      AND existing_block.parent_hash = data->>'parent_hash'
      AND existing_block.coinbase_txid = data->>'coinbase_txid'
      AND existing_block.payout_manifest_sha256 = data->>'payout_manifest_sha256'
),
existing_bundle AS (
    SELECT block_hash, audit_bundle_sha256, coinbase_tx_hex
    FROM qbit_pool_audit_bundles
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
matching_existing_bundle AS (
    -- audit_bundle_sha256 is computed over the full bundle content, so matching
    -- it (plus the coinbase tx) proves an identical body without comparing the
    -- JSONB directly, which is NULL for externalized rows.
    SELECT existing_bundle.block_hash
    FROM existing_bundle, payload
    WHERE existing_bundle.audit_bundle_sha256 = data->>'audit_bundle_sha256'
      AND existing_bundle.coinbase_tx_hex = data->>'coinbase_tx_hex'
),
expected_payout_rows AS (
    SELECT
        data->>'block_hash' AS block_hash,
        (data->>'block_height')::bigint AS block_height,
        miner_id,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action
    FROM accounts
),
existing_payout_rows AS (
    SELECT
        block_hash,
        block_height,
        miner_id,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action
    FROM qbit_pool_payout_entries
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
payout_missing AS (
    SELECT * FROM expected_payout_rows
    EXCEPT ALL
    SELECT * FROM existing_payout_rows
),
payout_extra AS (
    SELECT * FROM existing_payout_rows
    EXCEPT ALL
    SELECT * FROM expected_payout_rows
),
expected_carry_rows AS (
    SELECT
        (data->>'block_height')::bigint AS block_height,
        data->>'block_hash' AS block_hash,
        miner_id,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        prior_balance_sats,
        candidate_balance_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action
    FROM accounts
),
existing_carry_rows AS (
    SELECT
        block_height,
        block_hash,
        miner_id,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        prior_balance_sats,
        candidate_balance_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action
    FROM qbit_payout_carry_forward
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
carry_missing AS (
    SELECT * FROM expected_carry_rows
    EXCEPT ALL
    SELECT * FROM existing_carry_rows
),
carry_extra AS (
    SELECT * FROM existing_carry_rows
    EXCEPT ALL
    SELECT * FROM expected_carry_rows
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (SELECT count(*) FROM matching_existing_block) = 0 THEN
        json_build_object('error', 'existing block metadata does not match payload')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (SELECT count(*) FROM matching_existing_bundle) = 0 THEN
        json_build_object('error', 'existing audit bundle does not match payload')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (
          EXISTS (SELECT 1 FROM payout_missing)
          OR EXISTS (SELECT 1 FROM payout_extra)
      ) THEN
        json_build_object('error', 'existing payout entries do not match payload')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (
          EXISTS (SELECT 1 FROM carry_missing)
          OR EXISTS (SELECT 1 FROM carry_extra)
      ) THEN
        json_build_object('error', 'existing carry-forward rows do not match payload')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'share_count', (SELECT count(*) FROM qbit_share_ledger WHERE accepted),
            'block_count', CASE
                WHEN (SELECT count(*) FROM inserted_block) > 0 THEN (SELECT count(*) FROM inserted_block)
                ELSE (SELECT count(*) FROM existing_block)
            END,
            'bundle_count', CASE
                WHEN (SELECT count(*) FROM inserted_block) > 0 THEN (SELECT count(*) FROM bundle_insert)
                ELSE (SELECT count(*) FROM existing_bundle)
            END,
            'payout_entry_count', CASE
                WHEN (SELECT count(*) FROM inserted_block) > 0 THEN (SELECT count(*) FROM payout_insert)
                ELSE (SELECT count(*) FROM existing_payout_rows)
            END,
            'carry_forward_count', CASE
                WHEN (SELECT count(*) FROM inserted_block) > 0 THEN (SELECT count(*) FROM carry_insert)
                ELSE (SELECT count(*) FROM existing_carry_rows)
            END,
            'onchain_output_count', {int(manifest["payout_count"])}
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "share_count": int(result["share_count"]),
            "block_count": int(result["block_count"]),
            "bundle_count": int(result["bundle_count"]),
            "payout_entry_count": int(result["payout_entry_count"]),
            "carry_forward_count": int(result["carry_forward_count"]),
            "onchain_output_count": int(result["onchain_output_count"]),
        }

    def reverse_immature_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'reversed_count', qbit_reverse_immature_pool_block(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "reversed_count": int(result["reversed_count"]),
        }

    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'rejected_count', qbit_reject_prepared_pool_block(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "rejected_count": int(result["rejected_count"]),
        }

    def confirm_accepted_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'confirmed_count', qbit_confirm_pool_block(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "confirmed_count": int(result["confirmed_count"]),
        }

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block_hash,
    'block_height', block_height,
    'parent_hash', parent_hash,
    'chain_state', chain_state,
    'maturity_state', maturity_state
) ORDER BY block_height ASC, block_hash ASC), '[]'::json)
FROM qbit_pool_blocks
WHERE chain_state IN ('confirmed', 'inactive')
  AND maturity_state = 'immature'
;
"""
        with self._lock:
            rows = self._run_json(sql)
        for row in rows:
            row["block_height"] = int(row["block_height"])
        return rows

    def mark_pool_block_inactive(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'inactive_count', qbit_mark_pool_block_inactive(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "inactive_count": int(result["inactive_count"]),
        }

    def reactivate_pool_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'reactivated_count', qbit_reactivate_pool_block(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "reactivated_count": int(result["reactivated_count"]),
        }

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
WITH lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = {self._text_literal(self._writer_id)}
      AND writer_epoch = {int(self._writer_epoch)}
      AND writer_session_token = {self._text_literal(self._writer_session_token)}
    RETURNING writer_id
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'matured_count', qbit_mark_mature_pool_payouts({int(active_tip_height)})
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "matured_count": int(result["matured_count"]),
        }

    def __len__(self) -> int:
        sql = "SELECT json_build_object('count', count(*)) FROM qbit_share_ledger WHERE accepted;"
        with self._lock:
            return int(self._run_json(sql)["count"])

    def _run_fenced_json(self, sql: str) -> Any:
        with self._lock:
            return self._run_json(sql)

    def _run_read_json(self, sql: str) -> Any:
        with self._read_semaphore:
            return self._run_json(sql)

    def _ensure_writer_lease(self) -> None:
        while True:
            result = self._try_acquire_writer_lease()
            if result.get("acquired"):
                return
            if not self._can_wait_for_writer_lease(result):
                raise RuntimeError(
                    "qbit ledger writer lease is held by "
                    f"{result.get('writer_id')} epoch={result.get('writer_epoch')} "
                    f"session={result.get('writer_session_token')} "
                    f"until {result.get('lease_expires_at')}"
                )
            wait_seconds = max(0.0, float(result.get("lease_wait_seconds") or 0.0))
            sleep_seconds = min(
                self._lease_retry_max_sleep_seconds,
                max(self._lease_retry_min_sleep_seconds, wait_seconds),
            )
            print(
                "prism ledger writer lease held until "
                f"{result.get('lease_expires_at')}; waiting {sleep_seconds:.3g}s before retry "
                f"(holder writer={result.get('writer_id')} epoch={result.get('writer_epoch')} "
                f"session={result.get('writer_session_token')})",
                flush=True,
            )
            self._lease_retry_sleep(sleep_seconds)

    def _try_acquire_writer_lease(self) -> dict[str, Any]:
        payload = {
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
upsert AS (
INSERT INTO qbit_ledger_writer_lease (
    singleton,
    writer_id,
    writer_epoch,
    writer_session_token,
    lease_expires_at
)
SELECT
    true,
    data->>'writer_id',
    (data->>'writer_epoch')::bigint,
    data->>'writer_session_token',
    clock_timestamp() + {self._lease_interval_sql}
FROM payload
ON CONFLICT (singleton) DO UPDATE
SET writer_id = EXCLUDED.writer_id,
    writer_epoch = EXCLUDED.writer_epoch,
    writer_session_token = EXCLUDED.writer_session_token,
    lease_expires_at = EXCLUDED.lease_expires_at,
    updated_at = clock_timestamp()
WHERE (
        qbit_ledger_writer_lease.writer_id = EXCLUDED.writer_id
        AND qbit_ledger_writer_lease.writer_epoch = EXCLUDED.writer_epoch
        AND qbit_ledger_writer_lease.writer_session_token = EXCLUDED.writer_session_token
    )
   OR qbit_ledger_writer_lease.lease_expires_at <= clock_timestamp()
RETURNING writer_id, writer_epoch, writer_session_token
)
SELECT COALESCE(
    (
        SELECT json_build_object(
            'acquired', true,
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token
        )
        FROM upsert
    ),
    (
        SELECT json_build_object(
            'acquired', false,
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token,
            'lease_expires_at', lease_expires_at::text,
            'lease_wait_seconds', GREATEST(
                0,
                EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp()))
            )
        )
        FROM qbit_ledger_writer_lease
        WHERE singleton
    )
);
"""
        result = self._run_json(sql)
        if not isinstance(result, dict):
            raise RuntimeError("psql writer lease query returned non-object JSON")
        return result

    def _can_wait_for_writer_lease(self, result: dict[str, Any]) -> bool:
        try:
            holder_epoch = int(result.get("writer_epoch"))
        except (TypeError, ValueError):
            return False
        return (
            result.get("writer_id") == self._writer_id
            and holder_epoch == self._writer_epoch
            and result.get("lease_expires_at") is not None
        )

    def release_writer_lease(self) -> bool:
        """Expire this writer's lease so a same-identity replacement can take
        over immediately instead of waiting out the lease TTL.

        Best-effort, intended for graceful shutdown. Only the exact
        ``(writer_id, writer_epoch, writer_session_token)`` this process holds is
        expired, so a lease already reassigned to another writer is left
        untouched. Returns True if a held lease row was expired.
        """
        payload = {
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
released AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() - interval '1 second',
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
)
SELECT json_build_object('released', (SELECT count(*) FROM released));
"""
        result = self._run_fenced_json(sql)
        if not isinstance(result, dict):
            raise RuntimeError("psql writer lease release returned non-object JSON")
        return int(result.get("released", 0)) > 0

    def _run_json(self, sql: str) -> Any:
        output = self._run_sql(sql).strip()
        if not output:
            raise RuntimeError("psql query returned no JSON")
        return json.loads(output.splitlines()[-1])

    def _run_sql(self, sql: str) -> str:
        cmd = [
            *self._command,
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--tuples-only",
            "--no-align",
            "--quiet",
        ]
        completed = subprocess.run(
            cmd,
            input=sql,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "psql command failed "
                f"(exit {completed.returncode}): {completed.stderr.strip()}"
            )
        return completed.stdout

    @staticmethod
    def _record_from_json(payload: dict[str, Any]) -> AcceptedShareRecord:
        return AcceptedShareRecord(
            share_seq=int(payload["share_seq"]),
            share_id=str(payload["share_id"]),
            miner_id=str(payload["miner_id"]),
            order_key=str(payload["order_key"]),
            p2mr_program_hex=str(payload["p2mr_program_hex"]),
            share_difficulty=int(payload["share_difficulty"]),
            network_difficulty=int(payload["network_difficulty"]),
            template_height=int(payload["template_height"]),
            job_id=str(payload["job_id"]),
            job_issued_at_ms=int(payload["job_issued_at_ms"]),
            accepted_at_ms=int(payload["accepted_at_ms"]),
            ntime=int(payload["ntime"]),
        )

    @staticmethod
    def _jsonb_literal(payload: object) -> str:
        raw = json.dumps(payload, separators=(",", ":"))
        tag = "qbit_prism_json"
        while f"${tag}$" in raw:
            tag += "_x"
        return f"${tag}${raw}${tag}$::jsonb"

    @staticmethod
    def _text_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"


CTV_FANOUT_STATUSES = {
    "awaiting_maturity",
    "broadcastable",
    "broadcast_submitted",
    "confirmed",
    "reorged",
    "failed",
}

CTV_FANOUT_ATTEMPT_STATUSES = {"planned", "submitted", "accepted", "rejected", "failed"}


def validate_ctv_fanout_status(status: str) -> None:
    if status not in CTV_FANOUT_STATUSES:
        raise ValueError(f"unsupported CTV fanout status: {status}")


def validate_ctv_fanout_attempt_status(status: str) -> None:
    if status not in CTV_FANOUT_ATTEMPT_STATUSES:
        raise ValueError(f"unsupported CTV fanout attempt status: {status}")


def ctv_fanout_recovery_payload(
    *,
    block_hash: str,
    manifest_set: dict[str, Any],
    manifest_set_sha256: str,
) -> dict[str, Any]:
    block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
    manifest_set_sha256 = canonical_hex(
        manifest_set_sha256,
        name="manifest_set_sha256",
        expected_bytes=32,
    )
    manifests_raw = manifest_set.get("manifests")
    if not isinstance(manifests_raw, list) or not manifests_raw:
        raise ValueError("manifest_set.manifests must be a non-empty array")

    manifests = sorted(
        (require_mapping(manifest, "manifest") for manifest in manifests_raw),
        key=lambda item: int(require_mapping(item.get("precommitment"), "precommitment")["chunk_index"]),
    )
    first_precommitment = require_mapping(manifests[0].get("precommitment"), "precommitment")
    block_height_value = manifest_set.get("block_height", first_precommitment.get("block_height"))
    block_height = int(block_height_value) if block_height_value is not None else None
    fanout_count = int(manifest_set.get("fanout_count", len(manifests)))
    if fanout_count != len(manifests):
        raise ValueError("manifest_set.fanout_count must equal the number of manifests")
    settlement_mode = str(manifest_set.get("settlement_mode", first_precommitment.get("settlement_mode", "")))
    if settlement_mode not in {"hybrid_coinbase_ctv_fanout", "ctv_fanout"}:
        raise ValueError("manifest_set.settlement_mode must be a CTV settlement mode")
    parent_coinbase_txid = canonical_hex(
        str(manifest_set.get("parent_coinbase_txid", manifests[0].get("parent_coinbase_txid", ""))),
        name="parent_coinbase_txid",
        expected_bytes=32,
    )
    parent_coinbase_tx_hex = canonical_hex(
        str(manifests[0].get("parent_coinbase_tx_hex", "")),
        name="parent_coinbase_tx_hex",
    )
    fanout_output_sum_sats = int(manifest_set.get("fanout_output_sum_sats", 0))
    covenant_output_value_sats = int(manifest_set.get("covenant_output_value_sats", 0))

    artifacts: list[dict[str, Any]] = []
    for expected_index, manifest in enumerate(manifests):
        precommitment = require_mapping(manifest.get("precommitment"), "precommitment")
        precommitment_block_height = precommitment.get("block_height")
        if block_height is not None and precommitment_block_height is not None and int(precommitment_block_height) != block_height:
            raise ValueError("CTV fanout block height mismatch")
        chunk_index = int(precommitment["chunk_index"])
        chunk_count = int(precommitment["chunk_count"])
        if chunk_index != expected_index:
            raise ValueError("CTV fanout chunks must be contiguous from zero")
        if chunk_count != fanout_count:
            raise ValueError("CTV fanout chunk_count must equal fanout_count")
        artifact_parent_txid = canonical_hex(
            str(manifest.get("parent_coinbase_txid", "")),
            name="manifest.parent_coinbase_txid",
            expected_bytes=32,
        )
        if artifact_parent_txid != parent_coinbase_txid:
            raise ValueError("CTV fanout parent coinbase txid mismatch")
        fanout_fee_sats = int(precommitment.get("fanout_fee_sats", 0))
        raw_anchor_vout = precommitment.get("anchor_vout")
        if fanout_fee_sats > 0 and raw_anchor_vout is not None:
            raise ValueError("built-in-fee CTV fanout must not include a CPFP anchor")
        if fanout_fee_sats == 0 and raw_anchor_vout is None:
            raise ValueError("zero-fee CTV fanout must include a CPFP anchor")
        artifact = {
            "fanout_txid": canonical_hex(
                str(manifest["fanout_txid"]),
                name="fanout_txid",
                expected_bytes=32,
            ),
            "manifest_json": canonical_json_text(manifest),
            "manifest": copy.deepcopy(manifest),
            "manifest_sha256": sha256_json_hex(manifest),
            "precommitment_sha256": canonical_hex(
                str(manifest["precommitment_sha256_hex"]),
                name="precommitment_sha256_hex",
                expected_bytes=32,
            ),
            "ctv_hash": canonical_hex(
                str(precommitment["ctv_hash_hex"]),
                name="ctv_hash_hex",
                expected_bytes=32,
            ),
            "commitment_witness_leaf_hex": canonical_hex(
                str(manifest["commitment_witness_leaf_hex"]),
                name="commitment_witness_leaf_hex",
            ),
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "parent_coinbase_txid": artifact_parent_txid,
            "parent_coinbase_vout": int(manifest["parent_coinbase_vout"]),
            "fanout_tx_template_hex": canonical_hex(
                str(precommitment["fanout_tx_template_hex"]),
                name="fanout_tx_template_hex",
            ),
            "fanout_tx_hex": canonical_hex(str(manifest["fanout_tx_hex"]), name="fanout_tx_hex"),
            "anchor_vout": None if raw_anchor_vout is None else int(raw_anchor_vout),
            "covenant_output_value_sats": int(manifest["covenant_output_value_sats"]),
            "fanout_output_sum_sats": int(precommitment["fanout_output_sum_sats"]),
            "settlement_status": "awaiting_maturity",
        }
        if block_height is not None:
            artifact["block_height"] = block_height
        artifacts.append(artifact)

    if sum(int(artifact["fanout_output_sum_sats"]) for artifact in artifacts) != fanout_output_sum_sats:
        raise ValueError("CTV fanout output sum mismatch")
    if sum(int(artifact["covenant_output_value_sats"]) for artifact in artifacts) != covenant_output_value_sats:
        raise ValueError("CTV covenant output value sum mismatch")

    payload = {
        "schema": "qbit.prism.ctv-fanout-recovery.v1",
        "block_hash": block_hash,
        "manifest_set_sha256": manifest_set_sha256,
        "manifest_set_json": canonical_json_text(manifest_set),
        "settlement_mode": settlement_mode,
        "parent_coinbase_txid": parent_coinbase_txid,
        "parent_coinbase_tx_hex": parent_coinbase_tx_hex,
        "fanout_count": fanout_count,
        "fanout_output_sum_sats": fanout_output_sum_sats,
        "covenant_output_value_sats": covenant_output_value_sats,
        "manifest_set": copy.deepcopy(manifest_set),
        "artifacts": artifacts,
    }
    if block_height is not None:
        payload["block_height"] = block_height
    audit_bundle_sha256 = manifest_set.get("audit_bundle_sha256")
    if audit_bundle_sha256 is not None:
        payload["audit_bundle_sha256"] = canonical_hex(
            str(audit_bundle_sha256),
            name="audit_bundle_sha256",
            expected_bytes=32,
        )
    audit_bundle = manifest_set.get("audit_bundle")
    if isinstance(audit_bundle, dict):
        payload["audit_bundle"] = copy.deepcopy(audit_bundle)
    return payload


def require_mapping(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def sha256_json_hex(payload: object) -> str:
    return hashlib.sha256(canonical_json_text(payload).encode()).hexdigest()


def sha256_bytes_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical_json_text(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def canonical_hex(value: str, *, name: str, expected_bytes: int | None = None) -> str:
    lowered = value.lower()
    if not lowered:
        raise ValueError(f"{name} must not be empty")
    try:
        bytes.fromhex(lowered)
    except ValueError as exc:
        raise ValueError(f"{name} must be hex") from exc
    if expected_bytes is not None and len(lowered) != expected_bytes * 2:
        raise ValueError(f"{name} must be {expected_bytes * 2} hex characters")
    if len(lowered) % 2 != 0:
        raise ValueError(f"{name} must have an even number of hex characters")
    return lowered

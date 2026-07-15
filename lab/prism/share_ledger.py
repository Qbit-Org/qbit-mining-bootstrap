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

AUDIT_BODY_REF_SCHEMA = "qbit.prism.audit-body-ref.v1"
AUDIT_BUNDLE_V2_SCHEMA = "qbit.prism.audit-bundle.v2"
AUDIT_SHARE_SEGMENT_SCHEMA = "qbit.prism.audit-share-segment.v1"
AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA = "qbit.prism.window-completeness-proof.v1"
DEFAULT_AUDIT_SHARE_SEGMENT_SIZE = 10_000
DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT = 20
DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS = 300
VALID_CREDIT_POLICIES = frozenset({"stale-grace"})


def validate_credit_policy(credit_policy: str | None) -> str | None:
    if credit_policy is None:
        return None
    if credit_policy not in VALID_CREDIT_POLICIES:
        raise ValueError(f"unsupported credit_policy: {credit_policy!r}")
    return credit_policy


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
    credit_policy: str | None = None

    def to_prism_json(self) -> dict[str, object]:
        payload: dict[str, object] = {
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
        if self.credit_policy is not None:
            payload["credit_policy"] = self.credit_policy
        return payload


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
    credit_policy: str | None = None


class SingleWriterShareLedger:
    """Assigns canonical share_seq values and returns immutable snapshots.

    The direct Stratum coordinator should append accepted shares through one
    instance of this class. Later Postgres integration can keep this API shape
    while moving storage to `qbit_share_ledger`.
    """

    def __init__(
        self,
        *,
        first_share_seq: int = 1,
        ctv_broadcast_attempt_detail_limit: int = DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
        ctv_broadcast_retry_backoff_seconds: int = DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
    ):
        if first_share_seq < 1:
            raise ValueError("first_share_seq must be >= 1")
        ctv_broadcast_attempt_detail_limit = int(ctv_broadcast_attempt_detail_limit)
        if ctv_broadcast_attempt_detail_limit < 0:
            raise ValueError("ctv_broadcast_attempt_detail_limit must be non-negative")
        ctv_broadcast_retry_backoff_seconds = int(ctv_broadcast_retry_backoff_seconds)
        if ctv_broadcast_retry_backoff_seconds < 0:
            raise ValueError("ctv_broadcast_retry_backoff_seconds must be non-negative")
        self._ctv_broadcast_attempt_detail_limit = ctv_broadcast_attempt_detail_limit
        self._ctv_broadcast_retry_backoff_seconds = ctv_broadcast_retry_backoff_seconds
        self._next_share_seq = first_share_seq
        self._shares: list[AcceptedShareRecord] = []
        self._share_ids: set[str] = set()
        self._shares_by_id: dict[str, AcceptedShareRecord] = {}
        self._block_candidate_outbox: dict[str, dict[str, Any]] = {}
        self._ctv_fanout_sets: dict[str, dict[str, Any]] = {}
        self._ctv_fanout_statuses: dict[str, dict[str, Any]] = {}
        self._ctv_fanout_attempts: dict[str, list[dict[str, Any]]] = {}
        self._lock = Lock()

    def append(self, pending: PendingShare) -> AcceptedShareRecord:
        if pending.share_difficulty <= 0:
            raise ValueError("share_difficulty must be positive")
        if pending.network_difficulty <= 0:
            raise ValueError("network_difficulty must be positive")
        credit_policy = validate_credit_policy(pending.credit_policy)
        with self._lock:
            if pending.share_id in self._share_ids:
                existing = self._shares_by_id[pending.share_id]
                if self._pending_matches_record(pending, existing, credit_policy=credit_policy):
                    return replace(existing)
                raise ValueError("duplicate share_id payload mismatch")
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
                credit_policy=credit_policy,
            )
            self._shares.append(record)
            self._share_ids.add(pending.share_id)
            self._shares_by_id[pending.share_id] = record
            self._next_share_seq += 1
            return record

    @staticmethod
    def _pending_matches_record(
        pending: PendingShare,
        record: AcceptedShareRecord,
        *,
        credit_policy: str | None,
    ) -> bool:
        return (
            pending.share_id == record.share_id
            and pending.miner_id == record.miner_id
            and pending.order_key == record.order_key
            and pending.p2mr_program_hex.lower() == record.p2mr_program_hex.lower()
            and int(pending.share_difficulty) == int(record.share_difficulty)
            and int(pending.network_difficulty) == int(record.network_difficulty)
            and int(pending.template_height) == int(record.template_height)
            and pending.job_id == record.job_id
            and int(pending.job_issued_at_ms) == int(record.job_issued_at_ms)
            and int(pending.accepted_at_ms) == int(record.accepted_at_ms)
            and int(pending.ntime) == int(record.ntime)
            and credit_policy == record.credit_policy
        )

    def append_batch(
        self,
        entries: list[tuple[PendingShare, dict[str, Any] | None]],
    ) -> list[AcceptedShareRecord]:
        """Atomically append a small coordinator group-commit batch.

        The in-memory backend is used by tests and local demonstrations.  Its
        lock provides the same all-at-once visibility expected from the
        Postgres implementation.
        """
        records: list[AcceptedShareRecord] = []
        with self._lock:
            # Validate the complete batch before mutating either collection.
            seen_ids: set[str] = set()
            seen_blocks: set[str] = set()
            for pending, candidate in entries:
                if pending.share_id in seen_ids:
                    raise ValueError("duplicate share_id in append batch")
                seen_ids.add(pending.share_id)
                credit_policy = validate_credit_policy(pending.credit_policy)
                existing = self._shares_by_id.get(pending.share_id)
                if existing is not None and not self._pending_matches_record(
                    pending, existing, credit_policy=credit_policy
                ):
                    raise ValueError("duplicate share_id payload mismatch")
                if candidate is not None:
                    block_hash = str(candidate.get("block_hash_hex", "")).lower()
                    if not block_hash:
                        raise ValueError("block candidate is missing block_hash_hex")
                    if block_hash in seen_blocks:
                        raise ValueError("duplicate block candidate in append batch")
                    seen_blocks.add(block_hash)
                    outbox = self._block_candidate_outbox.get(block_hash)
                    candidate_sha256 = block_candidate_identity_sha256(candidate)
                    if outbox is not None and (
                        outbox["share_id"] not in {None, pending.share_id}
                        or
                        outbox["candidate_sha256"] != candidate_sha256
                        or (
                            outbox["candidate"] is not None
                            and block_candidate_identity(outbox["candidate"])
                            != block_candidate_identity(candidate)
                        )
                    ):
                        raise ValueError("block candidate payload mismatch")

            for pending, candidate in entries:
                existing = self._shares_by_id.get(pending.share_id)
                if existing is None:
                    credit_policy = validate_credit_policy(pending.credit_policy)
                    existing = AcceptedShareRecord(
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
                        credit_policy=credit_policy,
                    )
                    self._shares.append(existing)
                    self._share_ids.add(pending.share_id)
                    self._shares_by_id[pending.share_id] = existing
                    self._next_share_seq += 1
                records.append(replace(existing))
                if candidate is not None:
                    block_hash = str(candidate["block_hash_hex"]).lower()
                    self._block_candidate_outbox.setdefault(
                        block_hash,
                        {
                            "block_hash": block_hash,
                            "share_id": pending.share_id,
                            "candidate": candidate,
                            "candidate_sha256": block_candidate_identity_sha256(candidate),
                            "state": "pending",
                            "attempt_count": 0,
                            "last_error": None,
                        },
                    )
                    self._block_candidate_outbox[block_hash]["share_id"] = pending.share_id
        return records

    def persist_block_candidate_intent(self, candidate: dict[str, Any]) -> bool:
        """Persist candidate work before a below-share-target synchronous submit."""
        block_hash = str(candidate.get("block_hash_hex", "")).lower()
        if not block_hash:
            raise ValueError("block candidate is missing block_hash_hex")
        candidate_sha256 = block_candidate_identity_sha256(candidate)
        with self._lock:
            existing = self._block_candidate_outbox.get(block_hash)
            if existing is not None:
                if existing["candidate_sha256"] != candidate_sha256:
                    raise ValueError("block candidate payload mismatch")
                return False
            self._block_candidate_outbox[block_hash] = {
                "block_hash": block_hash,
                "share_id": None,
                "candidate": candidate,
                "candidate_sha256": candidate_sha256,
                "state": "pending",
                "attempt_count": 0,
                "last_error": None,
            }
            return True

    def pending_block_candidates(self, *, limit: int = 32) -> list[dict[str, Any]]:
        return [
            row["candidate"]
            for row in self.pending_block_candidate_rows(limit=limit)
        ]

    def pending_block_candidate_rows(self, *, limit: int = 32) -> list[dict[str, Any]]:
        """Return pending payloads together with their authoritative row keys."""
        with self._lock:
            return [
                {
                    "block_hash": str(row["block_hash"]),
                    "candidate": (
                        dict(row["candidate"])
                        if isinstance(row["candidate"], dict)
                        else row["candidate"]
                    ),
                }
                for row in self._block_candidate_outbox.values()
                if row["state"] == "pending"
            ][:limit]

    def mark_block_candidate_submitted(self, *, block_hash: str) -> bool:
        return self._finish_block_candidate(block_hash=block_hash, state="submitted", error=None)

    def mark_block_candidate_abandoned(self, *, block_hash: str, error: str) -> bool:
        return self._finish_block_candidate(block_hash=block_hash, state="abandoned", error=error)

    def _finish_block_candidate(self, *, block_hash: str, state: str, error: str | None) -> bool:
        with self._lock:
            row = self._block_candidate_outbox.get(block_hash.lower())
            if row is None:
                return False
            row["state"] = state
            row["last_error"] = error
            row["attempt_count"] = int(row["attempt_count"]) + 1
            row["candidate"] = None
            return True

    def snapshot_at_job_issue(
        self,
        anchor_job_issued_at_ms: int,
        *,
        window_weight: int | None = None,
    ) -> list[AcceptedShareRecord]:
        # window_weight is a bound hint for the large Postgres ledger; the
        # in-memory ledger is small, so it returns the full eligible set (a
        # superset of the reward window, which is digest-neutral).
        del window_weight
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
                if existing_status:
                    for key in _ctv_broadcast_summary_fields():
                        if key in existing_status:
                            status_payload[key] = copy.deepcopy(existing_status[key])
                else:
                    status_payload.update(_empty_ctv_broadcast_summary())
                audit_bundle_sha256 = payload.get("audit_bundle_sha256")
                if audit_bundle_sha256 is not None:
                    status_payload["audit_bundle_sha256"] = audit_bundle_sha256
                status_payload["broadcast_attempt_summary"] = _ctv_broadcast_attempt_summary(status_payload)
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
        now = datetime.now(timezone.utc)
        with self._lock:
            rows = [
                copy.deepcopy(payload)
                for payload in self._ctv_fanout_statuses.values()
                if payload.get("settlement_status") not in {"confirmed", "reorged", "failed"}
                and _ctv_broadcast_attempt_due(payload.get("next_broadcast_attempt_at"), now)
            ]
        rows.sort(key=lambda row: (str(row.get("block_hash", "")), int(row.get("chunk_index", 0))))
        return rows[:limit]

    def dashboard_pending_fanout_rows(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        now = datetime.now(timezone.utc)
        with self._lock:
            rows = [
                copy.deepcopy(payload)
                for payload in self._ctv_fanout_statuses.values()
                if payload.get("settlement_status") not in {"confirmed", "reorged", "failed"}
                and _ctv_broadcast_attempt_due(payload.get("next_broadcast_attempt_at"), now)
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
            attempted_at = datetime.now(timezone.utc)
            status_payload = self._ctv_fanout_statuses[fanout_txid]
            total_attempts = int(status_payload.get("broadcast_attempt_count") or 0) + 1
            attempt = {
                "attempt_seq": total_attempts,
                "attempted_at": attempted_at,
                "attempt_status": attempt_status,
                "package_tx_hexes": package_tx_hexes or [],
                "package_txids": package_txids or [],
                "submit_result": submit_result,
                "error": error,
            }
            if self._ctv_broadcast_attempt_detail_limit > 0:
                if len(attempts) >= self._ctv_broadcast_attempt_detail_limit:
                    del attempts[0 : len(attempts) - self._ctv_broadcast_attempt_detail_limit + 1]
                attempts.append(attempt)
            counts = copy.deepcopy(status_payload.get("broadcast_attempt_status_counts") or {})
            if not isinstance(counts, dict):
                counts = {}
            counts[attempt_status] = int(counts.get(attempt_status) or 0) + 1
            next_attempt_at = None
            retry_backoff_seconds = 0
            if attempt_status == "planned" and self._ctv_broadcast_retry_backoff_seconds > 0:
                retry_backoff_seconds = self._ctv_broadcast_retry_backoff_seconds
                next_attempt_at = attempted_at + timedelta(seconds=retry_backoff_seconds)
            status_payload.update(
                {
                    "broadcast_attempt_count": total_attempts,
                    "broadcast_attempt_detail_count": len(attempts),
                    "first_broadcast_attempt_at": status_payload.get("first_broadcast_attempt_at") or attempted_at,
                    "last_broadcast_attempt_at": attempted_at,
                    "last_broadcast_attempt_status": attempt_status,
                    "last_broadcast_package_tx_hexes": package_tx_hexes or [],
                    "last_broadcast_package_txids": package_txids or [],
                    "last_broadcast_submit_result": submit_result,
                    "last_broadcast_error": error,
                    "broadcast_attempt_status_counts": counts,
                    "next_broadcast_attempt_at": next_attempt_at,
                    "broadcast_retry_backoff_seconds": retry_backoff_seconds,
                }
            )
            self._ctv_fanout_statuses[fanout_txid]["broadcast_attempts"] = copy.deepcopy(attempts)
            if attempt_status in {"submitted", "accepted"}:
                self._ctv_fanout_statuses[fanout_txid]["settlement_status"] = "broadcast_submitted"
            elif attempt_status in {"rejected", "failed"}:
                self._ctv_fanout_statuses[fanout_txid]["settlement_status"] = "failed"
            self._ctv_fanout_statuses[fanout_txid]["broadcast_attempt_summary"] = _ctv_broadcast_attempt_summary(
                self._ctv_fanout_statuses[fanout_txid]
            )
        return {"backend": "memory", "attempt_count": 1, "updated_count": 1 if attempt_status in {"submitted", "accepted", "rejected", "failed"} else 0}

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
            "ctv_fanouts_failed": len(
                [
                    payload
                    for payload in self._ctv_fanout_statuses.values()
                    if payload.get("settlement_status") == "failed"
                ]
            ),
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

    def dashboard_miner_pending_maturity_bits(self, *, recipient_id: str) -> int:
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
        audit_share_segment_size: int = 0,
        ctv_broadcast_attempt_detail_limit: int = DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
        ctv_broadcast_retry_backoff_seconds: int = DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
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
        audit_share_segment_size = int(audit_share_segment_size)
        if audit_share_segment_size < 0:
            raise ValueError("audit_share_segment_size must be non-negative")
        self._audit_share_segment_size = audit_share_segment_size
        ctv_broadcast_attempt_detail_limit = int(ctv_broadcast_attempt_detail_limit)
        if ctv_broadcast_attempt_detail_limit < 0:
            raise ValueError("ctv_broadcast_attempt_detail_limit must be non-negative")
        self._ctv_broadcast_attempt_detail_limit = ctv_broadcast_attempt_detail_limit
        ctv_broadcast_retry_backoff_seconds = int(ctv_broadcast_retry_backoff_seconds)
        if ctv_broadcast_retry_backoff_seconds < 0:
            raise ValueError("ctv_broadcast_retry_backoff_seconds must be non-negative")
        self._ctv_broadcast_retry_backoff_seconds = ctv_broadcast_retry_backoff_seconds
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
        credit_policy = validate_credit_policy(pending.credit_policy)
        payload = {
            **pending.__dict__,
            "credit_policy": credit_policy,
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
        credit_policy,
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
        data->>'credit_policy',
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
            'ntime', ntime,
            'credit_policy', credit_policy
        ) FROM inserted)
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return self._record_from_json(result)

    def append_batch(
        self,
        entries: list[tuple[PendingShare, dict[str, Any] | None]],
    ) -> list[AcceptedShareRecord]:
        """Commit accepted shares and optional block intents in one transaction.

        Replaying the exact same payload is idempotent.  Reusing a share ID or
        block hash with different content fails the whole batch.  Postgres
        assigns the share sequence and makes every row visible before this
        method returns, which is the coordinator's Stratum ACK boundary.
        """
        if not entries:
            return []
        payloads: list[dict[str, Any]] = []
        share_ids: set[str] = set()
        block_hashes: set[str] = set()
        for pending, candidate in entries:
            if pending.share_difficulty <= 0:
                raise ValueError("share_difficulty must be positive")
            if pending.network_difficulty <= 0:
                raise ValueError("network_difficulty must be positive")
            if pending.share_id in share_ids:
                raise ValueError("duplicate share_id in append batch")
            share_ids.add(pending.share_id)
            candidate_payload = candidate
            if candidate_payload is not None:
                block_hash = str(candidate_payload.get("block_hash_hex", "")).lower()
                if not block_hash:
                    raise ValueError("block candidate is missing block_hash_hex")
                if block_hash in block_hashes:
                    raise ValueError("duplicate block candidate in append batch")
                block_hashes.add(block_hash)
                candidate_payload = {**candidate_payload, "block_hash_hex": block_hash}
            payloads.append(
                {
                    "share": {
                        **pending.__dict__,
                        "credit_policy": validate_credit_policy(pending.credit_policy),
                    },
                    "candidate": candidate_payload,
                    "candidate_sha256": (
                        block_candidate_identity_sha256(candidate_payload)
                        if candidate_payload is not None
                        else None
                    ),
                }
            )
        payload = {
            "entries": payloads,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH input AS (
    SELECT
        {self._jsonb_literal(payload)} AS root,
        set_config('synchronous_commit', 'on', true) AS durability
),
payload AS (
    SELECT
        item->'share' AS data,
        NULLIF(item->'candidate', 'null'::jsonb) AS candidate,
        item->>'candidate_sha256' AS candidate_sha256,
        ordinality
    FROM input,
         jsonb_array_elements(root->'entries') WITH ORDINALITY AS rows(item, ordinality)
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM input
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = root->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (root->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = root->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
share_mismatch AS (
    SELECT data->>'share_id' AS share_id
    FROM payload
    JOIN qbit_share_ledger ledger ON ledger.share_id = data->>'share_id'
    WHERE ledger.miner_id IS DISTINCT FROM data->>'miner_id'
       OR ledger.payout_order_key IS DISTINCT FROM data->>'order_key'
       OR ledger.p2mr_program IS DISTINCT FROM decode(data->>'p2mr_program_hex', 'hex')
       OR ledger.share_difficulty IS DISTINCT FROM (data->>'share_difficulty')::numeric
       OR ledger.network_difficulty IS DISTINCT FROM (data->>'network_difficulty')::numeric
       OR ledger.template_height IS DISTINCT FROM (data->>'template_height')::bigint
       OR ledger.job_id IS DISTINCT FROM data->>'job_id'
       OR ledger.job_issued_at IS DISTINCT FROM to_timestamp((data->>'job_issued_at_ms')::double precision / 1000.0)
       OR ledger.accepted_at IS DISTINCT FROM to_timestamp((data->>'accepted_at_ms')::double precision / 1000.0)
       OR ledger.ntime IS DISTINCT FROM (data->>'ntime')::bigint
       OR ledger.credit_policy IS DISTINCT FROM data->>'credit_policy'
),
candidate_mismatch AS (
    SELECT payload.candidate->>'block_hash_hex' AS block_hash
    FROM payload
    JOIN qbit_block_candidate_outbox outbox
      ON outbox.block_hash = payload.candidate->>'block_hash_hex'
    WHERE payload.candidate IS NOT NULL
      AND ((outbox.share_id IS NOT NULL AND outbox.share_id IS DISTINCT FROM payload.data->>'share_id')
           OR outbox.candidate_sha256 IS DISTINCT FROM payload.candidate_sha256
           OR (outbox.candidate IS NOT NULL
               AND (outbox.candidate #- '{{pending_share,accepted_at_ms}}')
                   IS DISTINCT FROM (payload.candidate #- '{{pending_share,accepted_at_ms}}')))
),
batch_ok AS (
    SELECT 1 AS ok
    WHERE EXISTS (SELECT 1 FROM lease)
      AND NOT EXISTS (SELECT 1 FROM share_mismatch)
      AND NOT EXISTS (SELECT 1 FROM candidate_mismatch)
),
inserted_shares AS (
    INSERT INTO qbit_share_ledger (
        share_id, miner_id, payout_order_key, p2mr_program,
        share_difficulty, network_difficulty, template_height, job_id,
        job_issued_at, ntime, accepted_at, credit_policy, accepted,
        writer_id, writer_epoch
    )
    SELECT
        data->>'share_id', data->>'miner_id', data->>'order_key',
        decode(data->>'p2mr_program_hex', 'hex'),
        (data->>'share_difficulty')::numeric,
        (data->>'network_difficulty')::numeric,
        (data->>'template_height')::bigint, data->>'job_id',
        to_timestamp((data->>'job_issued_at_ms')::double precision / 1000.0),
        (data->>'ntime')::bigint,
        to_timestamp((data->>'accepted_at_ms')::double precision / 1000.0),
        data->>'credit_policy', true, root->>'writer_id',
        (root->>'writer_epoch')::bigint
    FROM payload, input, batch_ok
    WHERE NOT EXISTS (
        SELECT 1 FROM qbit_share_ledger existing
        WHERE existing.share_id = payload.data->>'share_id'
    )
    ORDER BY payload.ordinality
    ON CONFLICT (share_id) DO NOTHING
    RETURNING qbit_share_ledger.*
),
inserted_candidates AS (
    INSERT INTO qbit_block_candidate_outbox (
        block_hash, share_id, candidate, candidate_sha256
    )
    SELECT
        payload.candidate->>'block_hash_hex', payload.data->>'share_id',
        payload.candidate, payload.candidate_sha256
    FROM payload, batch_ok
    WHERE payload.candidate IS NOT NULL
    ON CONFLICT (block_hash) DO UPDATE
    SET share_id = EXCLUDED.share_id,
        updated_at = clock_timestamp()
    WHERE qbit_block_candidate_outbox.share_id IS NULL
      AND qbit_block_candidate_outbox.candidate_sha256 = EXCLUDED.candidate_sha256
    RETURNING block_hash
),
records AS (
    SELECT ledger.*, payload.ordinality
    FROM payload
    JOIN qbit_share_ledger ledger ON ledger.share_id = payload.data->>'share_id'
    UNION ALL
    SELECT inserted_shares.*, payload.ordinality
    FROM inserted_shares
    JOIN payload ON payload.data->>'share_id' = inserted_shares.share_id
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    WHEN EXISTS (SELECT 1 FROM share_mismatch) THEN
        json_build_object(
            'error', 'duplicate share_id payload mismatch',
            'share_ids', (SELECT json_agg(share_id ORDER BY share_id) FROM share_mismatch)
        )
    WHEN EXISTS (SELECT 1 FROM candidate_mismatch) THEN
        json_build_object(
            'error', 'block candidate payload mismatch',
            'block_hashes', (SELECT json_agg(block_hash ORDER BY block_hash) FROM candidate_mismatch)
        )
    ELSE json_build_object(
        'records', (
            SELECT json_agg(json_build_object(
                'share_seq', records.share_seq,
                'share_id', records.share_id,
                'miner_id', records.miner_id,
                'order_key', records.payout_order_key,
                'p2mr_program_hex', encode(records.p2mr_program, 'hex'),
                'share_difficulty', records.share_difficulty::text,
                'network_difficulty', records.network_difficulty::text,
                'template_height', records.template_height,
                'job_id', records.job_id,
                'job_issued_at_ms', round(extract(epoch FROM records.job_issued_at) * 1000)::bigint,
                'accepted_at_ms', round(extract(epoch FROM records.accepted_at) * 1000)::bigint,
                'ntime', records.ntime,
                'credit_policy', records.credit_policy
            ) ORDER BY records.ordinality)
            FROM records
        )
    )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        records = result.get("records")
        if not isinstance(records, list) or len(records) != len(entries):
            raise RuntimeError("Postgres share batch returned an incomplete result")
        return [self._record_from_json(record) for record in records]

    def persist_block_candidate_intent(self, candidate: dict[str, Any]) -> bool:
        """Persist candidate work that is not yet eligible for share credit."""
        block_hash = str(candidate.get("block_hash_hex", "")).lower()
        if not block_hash:
            raise ValueError("block candidate is missing block_hash_hex")
        candidate = {**candidate, "block_hash_hex": block_hash}
        candidate_sha256 = block_candidate_identity_sha256(candidate)
        sql = f"""
WITH durability AS (
    SELECT set_config('synchronous_commit', 'on', true)
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM durability
    WHERE singleton
      AND writer_id = {self._text_literal(self._writer_id)}
      AND writer_epoch = {int(self._writer_epoch)}
      AND writer_session_token = {self._text_literal(self._writer_session_token)}
    RETURNING writer_id
),
existing AS (
    SELECT candidate_sha256
    FROM qbit_block_candidate_outbox
    WHERE block_hash = {self._text_literal(block_hash)}
),
inserted AS (
    INSERT INTO qbit_block_candidate_outbox (
        block_hash, share_id, candidate, candidate_sha256
    )
    SELECT
        {self._text_literal(block_hash)}, NULL,
        {self._jsonb_literal(candidate)}, {self._text_literal(candidate_sha256)}
    FROM lease
    WHERE NOT EXISTS (SELECT 1 FROM existing)
    ON CONFLICT (block_hash) DO NOTHING
    RETURNING block_hash
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    WHEN EXISTS (
        SELECT 1 FROM existing
        WHERE candidate_sha256 <> {self._text_literal(candidate_sha256)}
    ) THEN
        json_build_object('error', 'block candidate payload mismatch')
    ELSE
        json_build_object('inserted', (SELECT count(*) FROM inserted))
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return int(result.get("inserted", 0)) > 0

    def pending_block_candidates(self, *, limit: int = 32) -> list[dict[str, Any]]:
        return [
            row["candidate"]
            for row in self.pending_block_candidate_rows(limit=limit)
        ]

    def pending_block_candidate_rows(self, *, limit: int = 32) -> list[dict[str, Any]]:
        """Return pending payloads together with their authoritative row keys."""
        if limit <= 0:
            return []
        sql = f"""
SELECT COALESCE(
    json_agg(
        json_build_object('block_hash', block_hash, 'candidate', candidate)
        ORDER BY created_at, block_hash
    ),
    '[]'::json
)
FROM (
    SELECT candidate, created_at, block_hash
    FROM qbit_block_candidate_outbox
    WHERE state = 'pending'
    ORDER BY created_at, block_hash
    LIMIT {int(limit)}
) pending;
"""
        with self._lock:
            return list(self._run_json(sql))

    def mark_block_candidate_submitted(self, *, block_hash: str) -> bool:
        return self._finish_block_candidate(block_hash=block_hash, state="submitted", error=None)

    def mark_block_candidate_abandoned(self, *, block_hash: str, error: str) -> bool:
        return self._finish_block_candidate(block_hash=block_hash, state="abandoned", error=error)

    def _finish_block_candidate(self, *, block_hash: str, state: str, error: str | None) -> bool:
        if state not in {"submitted", "abandoned"}:
            raise ValueError("invalid block candidate terminal state")
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
),
updated AS (
    UPDATE qbit_block_candidate_outbox
    SET state = {self._text_literal(state)},
        attempt_count = attempt_count + 1,
        last_error = {self._text_literal(error) if error is not None else 'NULL'},
        updated_at = clock_timestamp(),
        completed_at = clock_timestamp(),
        candidate = NULL
    FROM lease
    WHERE block_hash = {self._text_literal(block_hash.lower())}
      AND state = 'pending'
    RETURNING block_hash
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object('updated', (SELECT count(*) FROM updated))
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return int(result.get("updated", 0)) > 0

    def snapshot_at_job_issue(
        self,
        anchor_job_issued_at_ms: int,
        *,
        window_weight: int | None = None,
    ) -> list[AcceptedShareRecord]:
        anchor = (
            f"to_timestamp(({int(anchor_job_issued_at_ms)}::double precision / 1000.0))"
        )
        if window_weight is None:
            # Whole accepted history up to the anchor. Kept for callers that
            # want the full ledger (tools/tests); the coordinator passes a
            # window_weight so the hot job-build path stays bounded.
            rows_cte = f"""
WITH rows AS (
    SELECT *
    FROM qbit_share_ledger
    WHERE accepted
      AND job_issued_at <= {anchor}
      AND accepted_at <= {anchor}
)"""
        else:
            # Only the most-recent shares whose cumulative difficulty covers
            # window_weight -- a superset of the reward window the audit bundle
            # selects. compute_prism_window re-sorts by share_seq DESC and stops
            # at 8x network difficulty, dropping anything older, so a superset
            # yields the identical counted window and digest. Bounding the walk
            # here keeps the job-build ledger phase O(window), not O(ledger
            # history), and stops it growing without bound as the ledger grows.
            rows_cte = f"""
WITH RECURSIVE eligible AS (
    (
        SELECT ledger.*, ledger.share_difficulty::numeric AS cumulative_difficulty
        FROM qbit_share_ledger ledger
        WHERE ledger.accepted
          AND ledger.job_issued_at <= {anchor}
          AND ledger.accepted_at <= {anchor}
        ORDER BY ledger.share_seq DESC
        LIMIT 1
    )
    UNION ALL
    SELECT next_ledger.*, eligible.cumulative_difficulty + next_ledger.share_difficulty
    FROM eligible
    CROSS JOIN LATERAL (
        SELECT ledger.*
        FROM qbit_share_ledger ledger
        WHERE ledger.accepted
          AND ledger.job_issued_at <= {anchor}
          AND ledger.accepted_at <= {anchor}
          AND ledger.share_seq < eligible.share_seq
        ORDER BY ledger.share_seq DESC
        LIMIT 1
    ) next_ledger
    WHERE eligible.cumulative_difficulty < {int(window_weight)}::numeric
),
rows AS (SELECT * FROM eligible)"""
        sql = rows_cte + """
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
    'ntime', ntime,
    'credit_policy', credit_policy
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
    'ntime', ntime,
    'credit_policy', credit_policy
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
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'credit_policy', credit_policy
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

    def dashboard_miner_pending_maturity_bits(self, *, recipient_id: str) -> int:
        if not recipient_id:
            raise ValueError("recipient_id is required")
        sql = f"""
SELECT json_build_object(
    'pending_maturity_bits',
    COALESCE(sum(GREATEST(carry.onchain_amount_sats - carry.settlement_fee_sats, 0)), 0)
)
FROM qbit_payout_carry_forward carry
JOIN qbit_pool_blocks block
  ON block.block_hash = carry.block_hash
WHERE carry.miner_id = {self._text_literal(recipient_id)}
  AND carry.action = 'onchain'
  AND carry.maturity_state = 'immature'
  AND block.chain_state = 'confirmed'
  AND block.maturity_state = 'immature';
"""
        payload = self._run_read_json(sql)
        return int(payload["pending_maturity_bits"])

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
),
resolved AS (
    SELECT
        page_rows.*,
        fanout.fanout_txid,
        fanout.fanout_vout,
        fanout.fanout_amount_sats,
        fanout.fanout_fee_sats,
        fanout.fanout_gross_amount_sats,
        fanout.fanout_status
    FROM page_rows
    LEFT JOIN LATERAL (
        SELECT
            artifact.fanout_txid,
            artifact.settlement_status AS fanout_status,
            (output.value->>'vout')::integer AS fanout_vout,
            (output.value->>'amount_sats')::bigint AS fanout_amount_sats,
            COALESCE((output.value->>'fee_sats')::bigint, 0) AS fanout_fee_sats,
            COALESCE(
                (output.value->>'gross_amount_sats')::bigint,
                (output.value->>'amount_sats')::bigint
            ) AS fanout_gross_amount_sats
        FROM qbit_ctv_fanout_artifacts artifact
        CROSS JOIN LATERAL jsonb_array_elements(artifact.manifest->'precommitment'->'outputs') AS output(value)
        WHERE artifact.block_hash = page_rows.block_hash
          AND page_rows.action = 'onchain'
          AND output.value->>'recipient_id' = page_rows.miner_id
          AND output.value->>'order_key' = page_rows.payout_order_key
          AND output.value->>'p2mr_program_hex' = encode(page_rows.p2mr_program, 'hex')
        ORDER BY artifact.chunk_index ASC, (output.value->>'vout')::integer ASC
        LIMIT 1
    ) fanout ON TRUE
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
            'created_at', created_at::text,
            'fanout_txid', fanout_txid,
            'fanout_vout', fanout_vout,
            'fanout_amount_sats', fanout_amount_sats,
            'fanout_fee_sats', fanout_fee_sats,
            'fanout_gross_amount_sats', fanout_gross_amount_sats,
            'fanout_status', fanout_status
        ) ORDER BY block_height DESC, payout_entry_seq DESC)
        FROM resolved
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
            'block_hash', bundle.block_hash,
            'block_height', block.block_height,
            'payout_manifest_sha256', block.payout_manifest_sha256,
            'audit_bundle_sha256', bundle.audit_bundle_sha256,
            'coinbase_tx_hex', bundle.coinbase_tx_hex,
            'audit_bundle', bundle.audit_bundle,
            'body_uri', bundle.body_uri
        )
        FROM qbit_pool_audit_bundles bundle
        JOIN qbit_pool_blocks block
          ON block.block_hash = bundle.block_hash
        WHERE bundle.block_hash = {self._text_literal(block_hash)}
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
            'block_height', block.block_height,
            'payout_manifest_sha256', block.payout_manifest_sha256,
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
        'broadcast_attempt_count', artifact.broadcast_attempt_count,
        'broadcast_attempt_detail_count', artifact.broadcast_attempt_detail_count,
        'first_broadcast_attempt_at', artifact.first_broadcast_attempt_at::text,
        'last_broadcast_attempt_at', artifact.last_broadcast_attempt_at::text,
        'last_broadcast_attempt_status', artifact.last_broadcast_attempt_status,
        'last_broadcast_package_tx_hexes', artifact.last_broadcast_package_tx_hexes,
        'last_broadcast_package_txids', artifact.last_broadcast_package_txids,
        'last_broadcast_submit_result', artifact.last_broadcast_submit_result,
        'last_broadcast_error', artifact.last_broadcast_error,
        'broadcast_attempt_status_counts', artifact.broadcast_attempt_status_counts,
        'next_broadcast_attempt_at', artifact.next_broadcast_attempt_at::text,
        'broadcast_retry_backoff_seconds', artifact.broadcast_retry_backoff_seconds,
        'broadcast_attempt_summary', json_build_object(
            'attempt_count', artifact.broadcast_attempt_count,
            'detail_count', artifact.broadcast_attempt_detail_count,
            'first_attempt_at', artifact.first_broadcast_attempt_at::text,
            'last_attempt_at', artifact.last_broadcast_attempt_at::text,
            'last_attempt_status', artifact.last_broadcast_attempt_status,
            'last_package_tx_hexes', artifact.last_broadcast_package_tx_hexes,
            'last_package_txids', artifact.last_broadcast_package_txids,
            'last_submit_result', artifact.last_broadcast_submit_result,
            'last_error', artifact.last_broadcast_error,
            'status_counts', artifact.broadcast_attempt_status_counts,
            'next_attempt_at', artifact.next_broadcast_attempt_at::text,
            'retry_backoff_seconds', artifact.broadcast_retry_backoff_seconds
        ),
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
    WHERE artifact.settlement_status NOT IN ('confirmed', 'reorged', 'failed')
      AND (
          artifact.next_broadcast_attempt_at IS NULL
          OR artifact.next_broadcast_attempt_at <= clock_timestamp()
      )
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
        artifact.updated_at,
        artifact.broadcast_attempt_count,
        artifact.broadcast_attempt_detail_count,
        artifact.first_broadcast_attempt_at,
        artifact.last_broadcast_attempt_at,
        artifact.last_broadcast_attempt_status,
        artifact.last_broadcast_package_tx_hexes,
        artifact.last_broadcast_package_txids,
        artifact.last_broadcast_submit_result,
        artifact.last_broadcast_error,
        artifact.broadcast_attempt_status_counts,
        artifact.next_broadcast_attempt_at,
        artifact.broadcast_retry_backoff_seconds
    FROM qbit_ctv_fanout_artifacts artifact
    JOIN qbit_pool_blocks block
      ON block.block_hash = artifact.block_hash
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = artifact.block_hash
    WHERE artifact.settlement_status NOT IN ('confirmed', 'reorged', 'failed')
      AND (
          artifact.next_broadcast_attempt_at IS NULL
          OR artifact.next_broadcast_attempt_at <= clock_timestamp()
      )
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
            'broadcast_attempt_count', broadcast_attempt_count,
            'broadcast_attempt_detail_count', broadcast_attempt_detail_count,
            'first_broadcast_attempt_at', first_broadcast_attempt_at::text,
            'last_broadcast_attempt_at', last_broadcast_attempt_at::text,
            'last_broadcast_attempt_status', last_broadcast_attempt_status,
            'last_broadcast_package_tx_hexes', last_broadcast_package_tx_hexes,
            'last_broadcast_package_txids', last_broadcast_package_txids,
            'last_broadcast_submit_result', last_broadcast_submit_result,
            'last_broadcast_error', last_broadcast_error,
            'broadcast_attempt_status_counts', broadcast_attempt_status_counts,
            'next_broadcast_attempt_at', next_broadcast_attempt_at::text,
            'broadcast_retry_backoff_seconds', broadcast_retry_backoff_seconds,
            'broadcast_attempt_summary', json_build_object(
                'attempt_count', broadcast_attempt_count,
                'detail_count', broadcast_attempt_detail_count,
                'first_attempt_at', first_broadcast_attempt_at::text,
                'last_attempt_at', last_broadcast_attempt_at::text,
                'last_attempt_status', last_broadcast_attempt_status,
                'last_package_tx_hexes', last_broadcast_package_tx_hexes,
                'last_package_txids', last_broadcast_package_txids,
                'last_submit_result', last_broadcast_submit_result,
                'last_error', last_broadcast_error,
                'status_counts', broadcast_attempt_status_counts,
                'next_attempt_at', next_broadcast_attempt_at::text,
                'retry_backoff_seconds', broadcast_retry_backoff_seconds
            ),
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

    def dashboard_public_artifact_exists(self, *, sha256: str) -> bool:
        sha256 = str(sha256).lower()
        lit = self._text_literal(sha256)
        sql = f"""
WITH audit AS (
    SELECT audit_bundle, body_uri
    FROM qbit_pool_audit_bundles
    WHERE audit_bundle_sha256 = {lit}
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT json_build_object(
    'has_audit_row', (SELECT count(*) FROM audit) > 0,
    'audit_bundle_inline', (SELECT audit_bundle IS NOT NULL FROM audit),
    'body_uri', (SELECT body_uri FROM audit),
    'fallback_exists',
        EXISTS (
            SELECT 1
            FROM qbit_ctv_fanout_sets
            WHERE manifest_set_sha256 = {lit}
        )
        OR EXISTS (
            SELECT 1
            FROM qbit_ctv_fanout_artifacts
            WHERE manifest_sha256 = {lit}
        )
);
"""
        row = self._run_read_json(sql)
        if not isinstance(row, dict):
            return False
        if row.get("has_audit_row"):
            if row.get("audit_bundle_inline"):
                return True
            body_uri = row.get("body_uri")
            if not body_uri:
                return False
            return self._external_body_available_for_sha(body_uri, sha256)
        return bool(row.get("fallback_exists"))

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
            "attempt_detail_limit": self._ctv_broadcast_attempt_detail_limit,
            "retry_backoff_seconds": self._ctv_broadcast_retry_backoff_seconds,
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
artifact_row AS (
    SELECT fanout_txid
    FROM qbit_ctv_fanout_artifacts
    WHERE fanout_txid = (SELECT data->>'fanout_txid' FROM payload)
),
existing_detail_count AS (
    SELECT count(*)::bigint AS detail_count
    FROM qbit_ctv_fanout_broadcast_attempts
    WHERE fanout_txid = (SELECT data->>'fanout_txid' FROM payload)
),
pruned AS (
    DELETE FROM qbit_ctv_fanout_broadcast_attempts old_attempt
    USING payload, artifact_row
    WHERE old_attempt.fanout_txid = artifact_row.fanout_txid
      AND old_attempt.attempt_seq IN (
          SELECT retained.attempt_seq
          FROM qbit_ctv_fanout_broadcast_attempts retained
          WHERE retained.fanout_txid = artifact_row.fanout_txid
          ORDER BY retained.attempt_seq DESC
          OFFSET GREATEST((data->>'attempt_detail_limit')::integer - 1, 0)
      )
    RETURNING old_attempt.attempt_seq
),
pruned_count AS (
    SELECT count(*)::bigint AS pruned_count FROM pruned
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
    FROM payload, lease, artifact_row, pruned_count
    WHERE (data->>'attempt_detail_limit')::integer > 0
    RETURNING attempt_seq
),
inserted_count AS (
    SELECT count(*)::bigint AS inserted_count FROM inserted
),
updated AS (
    UPDATE qbit_ctv_fanout_artifacts artifact
    SET settlement_status = COALESCE(data->>'next_status', artifact.settlement_status),
        updated_at = clock_timestamp(),
        broadcast_attempt_count = artifact.broadcast_attempt_count + 1,
        broadcast_attempt_detail_count = CASE
            WHEN (data->>'attempt_detail_limit')::integer <= 0 THEN 0
            ELSE LEAST(
                (data->>'attempt_detail_limit')::bigint,
                GREATEST(
                    0,
                    existing_detail_count.detail_count
                    - pruned_count.pruned_count
                    + inserted_count.inserted_count
                )
            )
        END,
        first_broadcast_attempt_at = COALESCE(artifact.first_broadcast_attempt_at, clock_timestamp()),
        last_broadcast_attempt_at = clock_timestamp(),
        last_broadcast_attempt_status = data->>'attempt_status',
        last_broadcast_package_tx_hexes = data->'package_tx_hexes',
        last_broadcast_package_txids = data->'package_txids',
        last_broadcast_submit_result = data->'submit_result',
        last_broadcast_error = data->>'error',
        broadcast_attempt_status_counts = jsonb_set(
            COALESCE(artifact.broadcast_attempt_status_counts, '{{}}'::jsonb),
            ARRAY[data->>'attempt_status'],
            to_jsonb(
                COALESCE((artifact.broadcast_attempt_status_counts->>(data->>'attempt_status'))::bigint, 0)
                + 1
            ),
            true
        ),
        next_broadcast_attempt_at = CASE
            WHEN data->>'attempt_status' = 'planned'
              AND (data->>'retry_backoff_seconds')::bigint > 0 THEN
                clock_timestamp() + make_interval(secs => (data->>'retry_backoff_seconds')::double precision)
            ELSE NULL
        END,
        broadcast_retry_backoff_seconds = CASE
            WHEN data->>'attempt_status' = 'planned' THEN (data->>'retry_backoff_seconds')::bigint
            ELSE 0
        END
    FROM payload, lease, artifact_row, existing_detail_count, pruned_count, inserted_count
    WHERE artifact.fanout_txid = artifact_row.fanout_txid
    RETURNING artifact.fanout_txid, artifact.broadcast_attempt_count, artifact.broadcast_attempt_detail_count
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM artifact_row) = 0 THEN
        json_build_object('error', 'unknown CTV fanout txid')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'attempt_count', (SELECT count(*) FROM inserted),
            'updated_count', (SELECT count(*) FROM updated),
            'broadcast_attempt_count', (SELECT broadcast_attempt_count FROM updated LIMIT 1),
            'broadcast_attempt_detail_count', (SELECT broadcast_attempt_detail_count FROM updated LIMIT 1)
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
    'owed_accounts', (SELECT count(*) FROM qbit_current_owed_balances() WHERE owed_balance_sats > 0),
    'ctv_fanouts_failed', (SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE settlement_status = 'failed')
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

    def _external_audit_storage_bytes(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
        final_bundle: dict[str, Any],
        canonical_body_bytes: bytes,
    ) -> bytes:
        v2_bundle = self._audit_bundle_v2(
            block_hash=block_hash,
            audit_bundle_sha256=audit_bundle_sha256,
            final_bundle=final_bundle,
        )
        if v2_bundle is not None:
            return self._storage_json_bytes(v2_bundle)
        body_ref = self._audit_body_ref(
            block_hash=block_hash,
            audit_bundle_sha256=audit_bundle_sha256,
            final_bundle=final_bundle,
        )
        if body_ref is None:
            return canonical_body_bytes
        return self._storage_json_bytes(body_ref)

    def _audit_body_ref(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
        final_bundle: dict[str, Any],
    ) -> dict[str, Any] | None:
        if self._audit_body_dir is None or self._audit_share_segment_size <= 0:
            return None
        shares = final_bundle.get("shares")
        if not isinstance(shares, list) or not shares:
            return None
        share_parts = self._audit_share_parts(shares)
        if share_parts is None or not any(part.get("kind") == "segment" for part in share_parts):
            return None
        bundle_without_shares = copy.deepcopy(final_bundle)
        shares_key_index = list(bundle_without_shares).index("shares")
        bundle_without_shares.pop("shares", None)
        return {
            "schema": AUDIT_BODY_REF_SCHEMA,
            "block_hash": block_hash,
            "audit_bundle_sha256": audit_bundle_sha256,
            "audit_bundle_schema": str(final_bundle.get("schema") or ""),
            "share_count": len(shares),
            "share_segment_size": self._audit_share_segment_size,
            "shares_key_index": shares_key_index,
            "bundle_without_shares": bundle_without_shares,
            "share_parts": share_parts,
        }

    def _audit_bundle_v2(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
        final_bundle: dict[str, Any],
    ) -> dict[str, Any] | None:
        if self._audit_body_dir is None or self._audit_share_segment_size <= 0:
            return None
        shares = final_bundle.get("shares")
        if not isinstance(shares, list) or not shares:
            return None
        share_parts = self._audit_share_range_parts(shares)
        if share_parts is None:
            return None
        bundle_without_shares = copy.deepcopy(final_bundle)
        shares_key_index = list(bundle_without_shares).index("shares")
        bundle_without_shares.pop("shares", None)
        reward_manifest = final_bundle.get("reward_manifest")
        proof: dict[str, Any] = {
            "schema": AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA,
            "share_segment_size": self._audit_share_segment_size,
            "first_share_seq": int(shares[0]["share_seq"]),
            "last_share_seq": int(shares[-1]["share_seq"]),
            "share_count": len(shares),
            "share_parts_digest_hex": sha256_bytes_hex(
                self._storage_json_bytes({"share_parts": share_parts})
            ),
            "share_parts": share_parts,
        }
        if isinstance(reward_manifest, dict):
            for key in (
                "anchor_job_issued_at_ms",
                "anchor_share_seq",
                "newest_share_seq",
                "oldest_share_seq",
                "included_share_count",
                "requested_window_weight",
                "counted_window_weight",
                "share_slice_digest_hex",
            ):
                if key in reward_manifest:
                    proof[key] = copy.deepcopy(reward_manifest[key])
        return {
            "schema": AUDIT_BUNDLE_V2_SCHEMA,
            "block_hash": block_hash,
            "audit_bundle_sha256": audit_bundle_sha256,
            "logical_audit_bundle_schema": str(final_bundle.get("schema") or ""),
            "share_count": len(shares),
            "shares_key_index": shares_key_index,
            "bundle_without_shares": bundle_without_shares,
            "share_window_proof": proof,
        }

    def _audit_share_parts(self, shares: list[Any]) -> list[dict[str, Any]] | None:
        share_seqs: list[int] = []
        for share in shares:
            if not isinstance(share, dict):
                return None
            try:
                share_seq = int(share["share_seq"])
            except (KeyError, TypeError, ValueError):
                return None
            share_seqs.append(share_seq)
        if any(current + 1 != nxt for current, nxt in zip(share_seqs, share_seqs[1:])):
            return None

        parts: list[dict[str, Any]] = []
        index = 0
        segment_size = self._audit_share_segment_size
        while index < len(shares):
            first_seq = share_seqs[index]
            segment_start = ((first_seq - 1) // segment_size) * segment_size + 1
            segment_end = segment_start + segment_size - 1
            end = index
            while end < len(shares) and share_seqs[end] <= segment_end:
                end += 1
            chunk = shares[index:end]
            chunk_seqs = share_seqs[index:end]
            if len(chunk) == segment_size and chunk_seqs[0] == segment_start and chunk_seqs[-1] == segment_end:
                segment_uri, segment_sha256 = self._write_audit_share_segment(
                    first_share_seq=segment_start,
                    last_share_seq=segment_end,
                    shares=chunk,
                )
                parts.append(
                    {
                        "kind": "segment",
                        "first_share_seq": segment_start,
                        "last_share_seq": segment_end,
                        "share_count": len(chunk),
                        "sha256": segment_sha256,
                        "body_uri": segment_uri,
                    }
                )
            else:
                parts.append(
                    {
                        "kind": "inline",
                        "first_share_seq": chunk_seqs[0],
                        "last_share_seq": chunk_seqs[-1],
                        "share_count": len(chunk),
                        "shares": copy.deepcopy(chunk),
                    }
                )
            index = end
        return parts

    def _audit_share_range_parts(self, shares: list[Any]) -> list[dict[str, Any]] | None:
        share_seqs: list[int] = []
        for share in shares:
            if not isinstance(share, dict):
                return None
            try:
                share_seq = int(share["share_seq"])
            except (KeyError, TypeError, ValueError):
                return None
            share_seqs.append(share_seq)
        if any(current + 1 != nxt for current, nxt in zip(share_seqs, share_seqs[1:])):
            return None

        parts: list[dict[str, Any]] = []
        index = 0
        segment_size = self._audit_share_segment_size
        while index < len(shares):
            first_seq = share_seqs[index]
            segment_start = ((first_seq - 1) // segment_size) * segment_size + 1
            segment_end = segment_start + segment_size - 1
            end = index
            while end < len(shares) and share_seqs[end] <= segment_end:
                end += 1
            chunk = shares[index:end]
            chunk_seqs = share_seqs[index:end]
            segment_uri, range_sha256 = self._write_audit_share_segment_range(
                segment_first_share_seq=segment_start,
                segment_last_share_seq=segment_end,
                first_share_seq=chunk_seqs[0],
                last_share_seq=chunk_seqs[-1],
                shares=chunk,
            )
            parts.append(
                {
                    "kind": "segment_range",
                    "segment_first_share_seq": segment_start,
                    "segment_last_share_seq": segment_end,
                    "first_share_seq": chunk_seqs[0],
                    "last_share_seq": chunk_seqs[-1],
                    "share_count": len(chunk),
                    "range_sha256": range_sha256,
                    "body_uri": segment_uri,
                }
            )
            index = end
        return parts

    def _audit_share_segment_payload(self, *, first_share_seq: int, last_share_seq: int, shares: list[Any]) -> dict[str, Any]:
        return {
            "schema": AUDIT_SHARE_SEGMENT_SCHEMA,
            "first_share_seq": first_share_seq,
            "last_share_seq": last_share_seq,
            "share_count": len(shares),
            "shares": copy.deepcopy(shares),
        }

    def _write_audit_share_segment(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
        shares: list[Any],
    ) -> tuple[str, str]:
        if self._audit_body_dir is None:
            raise RuntimeError("audit body store is not configured")
        segment = self._audit_share_segment_payload(
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
            shares=shares,
        )
        segment_bytes = self._storage_json_bytes(segment)
        segment_sha256 = sha256_bytes_hex(segment_bytes)
        segment_path = self._audit_body_dir.resolve() / (
            f"prism-audit-share-segment-{first_share_seq}-{last_share_seq}-{segment_sha256}.json"
        )
        if segment_path.exists():
            if segment_path.read_bytes() != segment_bytes:
                raise RuntimeError(f"existing audit share segment does not match payload at {segment_path}")
        else:
            self._write_bytes_atomically(segment_path, segment_bytes)
        return str(segment_path), segment_sha256

    def _write_audit_share_segment_range(
        self,
        *,
        segment_first_share_seq: int,
        segment_last_share_seq: int,
        first_share_seq: int,
        last_share_seq: int,
        shares: list[Any],
    ) -> tuple[str, str]:
        if self._audit_body_dir is None:
            raise RuntimeError("audit body store is not configured")
        if not shares:
            raise RuntimeError("audit share segment range cannot be empty")
        segment_path = self._audit_body_dir.resolve() / (
            f"prism-audit-share-segment-slot-{segment_first_share_seq}-{segment_last_share_seq}.json"
        )
        merged_shares = copy.deepcopy(shares)
        if segment_path.exists():
            try:
                existing = json.loads(segment_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"existing audit share segment is not valid JSON at {segment_path}") from exc
            if not isinstance(existing, dict) or existing.get("schema") != AUDIT_SHARE_SEGMENT_SCHEMA:
                raise RuntimeError(f"existing audit share segment has invalid schema at {segment_path}")
            existing_shares = existing.get("shares")
            if not isinstance(existing_shares, list):
                raise RuntimeError(f"existing audit share segment has no shares at {segment_path}")
            merged_shares = self._merge_audit_share_ranges(
                existing_shares,
                shares,
                segment_path=segment_path,
            )
        segment_first = int(merged_shares[0]["share_seq"])
        segment_last = int(merged_shares[-1]["share_seq"])
        segment = self._audit_share_segment_payload(
            first_share_seq=segment_first,
            last_share_seq=segment_last,
            shares=merged_shares,
        )
        segment_bytes = self._storage_json_bytes(segment)
        if not segment_path.exists() or segment_path.read_bytes() != segment_bytes:
            self._write_bytes_atomically(segment_path, segment_bytes)

        range_payload = self._audit_share_segment_payload(
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
            shares=shares,
        )
        range_sha256 = sha256_bytes_hex(self._storage_json_bytes(range_payload))
        return str(segment_path), range_sha256

    def _merge_audit_share_ranges(
        self,
        existing_shares: list[Any],
        incoming_shares: list[Any],
        *,
        segment_path: Path,
    ) -> list[Any]:
        if not existing_shares:
            return copy.deepcopy(incoming_shares)
        existing_by_seq = self._audit_shares_by_seq(existing_shares, segment_path=segment_path)
        incoming_by_seq = self._audit_shares_by_seq(incoming_shares, segment_path=segment_path)
        for share_seq, incoming in incoming_by_seq.items():
            existing = existing_by_seq.get(share_seq)
            if existing is not None and existing != incoming:
                raise RuntimeError(f"existing audit share segment conflicts at share_seq {share_seq} in {segment_path}")
        merged_by_seq = {**existing_by_seq, **incoming_by_seq}
        ordered_seqs = sorted(merged_by_seq)
        if any(current + 1 != nxt for current, nxt in zip(ordered_seqs, ordered_seqs[1:])):
            raise RuntimeError(f"existing audit share segment would become non-contiguous at {segment_path}")
        return [copy.deepcopy(merged_by_seq[share_seq]) for share_seq in ordered_seqs]

    def _audit_shares_by_seq(self, shares: list[Any], *, segment_path: Path) -> dict[int, Any]:
        by_seq: dict[int, Any] = {}
        for share in shares:
            if not isinstance(share, dict):
                raise RuntimeError(f"audit share segment has invalid share payload at {segment_path}")
            try:
                share_seq = int(share["share_seq"])
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError(f"audit share segment has invalid share_seq at {segment_path}") from exc
            existing = by_seq.get(share_seq)
            if existing is not None and existing != share:
                raise RuntimeError(f"audit share segment has duplicate conflicting share_seq {share_seq} at {segment_path}")
            by_seq[share_seq] = copy.deepcopy(share)
        ordered = sorted(by_seq)
        if any(current + 1 != nxt for current, nxt in zip(ordered, ordered[1:])):
            raise RuntimeError(f"audit share segment has non-contiguous share_seq values at {segment_path}")
        return by_seq

    def _storage_json_bytes(self, payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    def _write_bytes_atomically(self, path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with tmp_path.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            tmp_path.replace(path)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass

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
        self._write_bytes_atomically(body_path, body_bytes)
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

    def _audit_body_byte_len(self, body_uri: object | None, final_bundle: dict[str, Any]) -> int:
        if body_uri:
            return self._resolve_audit_body_path(body_uri).stat().st_size
        return len(self._canonical_audit_bundle_bytes(final_bundle))

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
            if not self._external_body_matches_sha(body_path, str(payload["audit_bundle_sha256"])):
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
        storage_bytes = self._external_audit_storage_bytes(
            block_hash=str(payload["block_hash"]),
            audit_bundle_sha256=str(payload["audit_bundle_sha256"]),
            final_bundle=final_bundle,
            canonical_body_bytes=body_bytes,
        )
        restored_body_uri = self._write_external_audit_body(
            str(payload["block_hash"]),
            str(payload["audit_bundle_sha256"]),
            storage_bytes,
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
        try:
            body = json.loads(body_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: {exc}") from exc
        if isinstance(body, dict) and body.get("schema") == AUDIT_BODY_REF_SCHEMA:
            return self._resolve_audit_body_ref(body, expected_sha256=expected_sha256, body_uri=body_uri)
        if isinstance(body, dict) and body.get("schema") == AUDIT_BUNDLE_V2_SCHEMA:
            return self._resolve_audit_bundle_v2(body, expected_sha256=expected_sha256, body_uri=body_uri)
        if expected_sha256:
            expected = str(expected_sha256).lower()
            actual = sha256_bytes_hex(body_bytes)
            if actual != expected:
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {body_uri}: expected {expected}, got {actual}"
                )
        return body

    def _external_body_matches_sha(self, body_path: Path, expected_sha256: str) -> bool:
        try:
            self._read_external_body(str(body_path), expected_sha256=expected_sha256)
        except RuntimeError:
            return False
        return True

    def _external_body_available_for_sha(self, body_uri: object, expected_sha256: str) -> bool:
        try:
            body_path = self._resolve_audit_body_path(body_uri)
            body_bytes = body_path.read_bytes()
        except (OSError, RuntimeError):
            return False
        try:
            body = json.loads(body_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = None
        if isinstance(body, dict) and body.get("schema") == AUDIT_BODY_REF_SCHEMA:
            if str(body.get("audit_bundle_sha256") or "").lower() != expected_sha256:
                return False
            bundle_without_shares = body.get("bundle_without_shares")
            share_parts = body.get("share_parts")
            if not isinstance(bundle_without_shares, dict) or not isinstance(share_parts, list):
                return False
            expected_share_count = int(body.get("share_count") or 0)
            actual_share_count = 0
            for part in share_parts:
                if not isinstance(part, dict):
                    return False
                kind = part.get("kind")
                if kind == "segment":
                    if not self._audit_share_segment_available(part, parent_body_uri=body_uri):
                        return False
                elif kind in {"segment_range", "segment_prefix"}:
                    if not self._audit_share_segment_available(part, parent_body_uri=body_uri):
                        return False
                elif kind == "inline":
                    inline_shares = part.get("shares")
                    if not isinstance(inline_shares, list) or len(inline_shares) != int(part.get("share_count") or 0):
                        return False
                else:
                    return False
                actual_share_count += int(part.get("share_count") or 0)
            return actual_share_count == expected_share_count
        if isinstance(body, dict) and body.get("schema") == AUDIT_BUNDLE_V2_SCHEMA:
            try:
                self._resolve_audit_bundle_v2(body, expected_sha256=expected_sha256, body_uri=body_uri)
            except RuntimeError:
                return False
            return True
        return sha256_bytes_hex(body_bytes) == expected_sha256

    def _audit_share_segment_available(self, part: dict[str, Any], *, parent_body_uri: object) -> bool:
        try:
            self._read_audit_share_segment(part, parent_body_uri=parent_body_uri)
        except RuntimeError:
            return False
        return True

    def _resolve_audit_body_ref(
        self,
        body_ref: dict[str, Any],
        *,
        expected_sha256: object | None,
        body_uri: object,
    ) -> dict[str, object]:
        expected = str(expected_sha256).lower() if expected_sha256 else None
        declared_sha256 = str(body_ref.get("audit_bundle_sha256") or "").lower()
        if expected and declared_sha256 != expected:
            raise RuntimeError(
                f"audit bundle body hash mismatch at {body_uri}: expected {expected}, got {declared_sha256}"
            )
        bundle_without_shares = body_ref.get("bundle_without_shares")
        if not isinstance(bundle_without_shares, dict):
            raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: missing bundle_without_shares")
        share_parts = body_ref.get("share_parts")
        if not isinstance(share_parts, list):
            raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: missing share_parts")
        shares: list[Any] = []
        for part in share_parts:
            if not isinstance(part, dict):
                raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: invalid share part")
            kind = part.get("kind")
            if kind == "segment":
                shares.extend(self._read_audit_share_segment(part, parent_body_uri=body_uri))
            elif kind in {"segment_range", "segment_prefix"}:
                shares.extend(self._read_audit_share_segment(part, parent_body_uri=body_uri))
            elif kind == "inline":
                inline_shares = part.get("shares")
                if not isinstance(inline_shares, list):
                    raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: invalid inline shares")
                if len(inline_shares) != int(part.get("share_count") or 0):
                    raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: inline share count mismatch")
                shares.extend(copy.deepcopy(inline_shares))
            else:
                raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: invalid share part kind")
        expected_share_count = int(body_ref.get("share_count") or 0)
        if len(shares) != expected_share_count:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: expected "
                f"{expected_share_count} shares, reconstructed {len(shares)}"
            )
        shares_key_index_raw = body_ref.get("shares_key_index")
        shares_key_index = len(bundle_without_shares) if shares_key_index_raw is None else int(shares_key_index_raw)
        bundle: dict[str, object] = {}
        shares_inserted = False
        for index, (key, value) in enumerate(bundle_without_shares.items()):
            if index == shares_key_index:
                bundle["shares"] = shares
                shares_inserted = True
            bundle[str(key)] = copy.deepcopy(value)
        if not shares_inserted:
            bundle["shares"] = shares
        if expected:
            actual = sha256_bytes_hex(self._canonical_audit_bundle_bytes(bundle))
            if actual != expected:
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {body_uri}: expected {expected}, got {actual}"
                )
        return bundle

    def _resolve_audit_bundle_v2(
        self,
        body: dict[str, Any],
        *,
        expected_sha256: object | None,
        body_uri: object,
    ) -> dict[str, object]:
        expected = str(expected_sha256).lower() if expected_sha256 else None
        declared_sha256 = str(body.get("audit_bundle_sha256") or "").lower()
        if expected and declared_sha256 != expected:
            raise RuntimeError(
                f"audit bundle body hash mismatch at {body_uri}: expected {expected}, got {declared_sha256}"
            )
        bundle_without_shares = body.get("bundle_without_shares")
        if not isinstance(bundle_without_shares, dict):
            raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: missing bundle_without_shares")
        proof = body.get("share_window_proof")
        if not isinstance(proof, dict) or proof.get("schema") != AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA:
            raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: missing share_window_proof")
        share_parts = proof.get("share_parts")
        if not isinstance(share_parts, list):
            raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: missing share_parts")
        expected_parts_digest = str(proof.get("share_parts_digest_hex") or "").lower()
        if expected_parts_digest:
            actual_parts_digest = sha256_bytes_hex(self._storage_json_bytes({"share_parts": share_parts}))
            if actual_parts_digest != expected_parts_digest:
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {body_uri}: share_parts_digest_hex mismatch"
                )
        shares: list[Any] = []
        for part in share_parts:
            if not isinstance(part, dict):
                raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: invalid share part")
            shares.extend(self._read_audit_share_segment(part, parent_body_uri=body_uri))
        expected_share_count = int(body.get("share_count") or proof.get("share_count") or 0)
        if len(shares) != expected_share_count:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: expected "
                f"{expected_share_count} shares, reconstructed {len(shares)}"
            )
        if shares:
            first_share_seq = int(shares[0].get("share_seq")) if isinstance(shares[0], dict) else None
            last_share_seq = int(shares[-1].get("share_seq")) if isinstance(shares[-1], dict) else None
            if int(proof.get("first_share_seq") or 0) != first_share_seq:
                raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: proof first_share_seq mismatch")
            if int(proof.get("last_share_seq") or 0) != last_share_seq:
                raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: proof last_share_seq mismatch")
        reward_manifest = bundle_without_shares.get("reward_manifest")
        proof_share_digest = str(proof.get("share_slice_digest_hex") or "")
        if proof_share_digest and isinstance(reward_manifest, dict):
            reward_share_digest = str(reward_manifest.get("share_slice_digest_hex") or "")
            if not proof_share_digest.lower() == reward_share_digest.lower():
                raise RuntimeError(f"audit bundle body is not valid JSON at {body_uri}: proof share digest mismatch")
        shares_key_index_raw = body.get("shares_key_index")
        shares_key_index = len(bundle_without_shares) if shares_key_index_raw is None else int(shares_key_index_raw)
        bundle: dict[str, object] = {}
        shares_inserted = False
        for index, (key, value) in enumerate(bundle_without_shares.items()):
            if index == shares_key_index:
                bundle["shares"] = shares
                shares_inserted = True
            bundle[str(key)] = copy.deepcopy(value)
        if not shares_inserted:
            bundle["shares"] = shares
        actual = sha256_bytes_hex(self._canonical_audit_bundle_bytes(bundle))
        if declared_sha256 and actual != declared_sha256:
            raise RuntimeError(
                f"audit bundle body hash mismatch at {body_uri}: expected {declared_sha256}, got {actual}"
            )
        return bundle

    def _read_audit_share_segment(self, part: dict[str, Any], *, parent_body_uri: object) -> list[Any]:
        body_uri = part.get("body_uri")
        kind = str(part.get("kind") or "")
        try:
            body_path = self._resolve_audit_body_path(body_uri)
            segment_bytes = body_path.read_bytes()
        except OSError as exc:
            raise RuntimeError(
                f"audit bundle body is not retrievable at {parent_body_uri}: share segment {body_uri}: {exc}"
            ) from exc
        expected_sha256 = str(part.get("sha256") or "").lower()
        if kind == "segment" and sha256_bytes_hex(segment_bytes) != expected_sha256:
            raise RuntimeError(
                f"audit bundle body hash mismatch at {parent_body_uri}: "
                f"share segment {body_uri} expected {expected_sha256}, got {sha256_bytes_hex(segment_bytes)}"
            )
        try:
            segment = json.loads(segment_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: share segment {body_uri}: {exc}"
            ) from exc
        if not isinstance(segment, dict) or segment.get("schema") != AUDIT_SHARE_SEGMENT_SCHEMA:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: invalid share segment {body_uri}"
            )
        shares = segment.get("shares")
        if not isinstance(shares, list):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: share segment {body_uri} has no shares"
            )
        expected_count = int(part.get("share_count") or 0)
        first_share_seq = int(part.get("first_share_seq") or 0)
        last_share_seq = int(part.get("last_share_seq") or 0)
        selected_shares = self._select_audit_share_segment_range(
            shares,
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
            parent_body_uri=parent_body_uri,
            body_uri=body_uri,
        )
        if len(selected_shares) != expected_count:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: share segment {body_uri} "
                f"expected {expected_count} shares, found {len(selected_shares)}"
            )
        if kind == "segment_range":
            expected_range_sha256 = str(part.get("range_sha256") or "").lower()
            actual_range_sha256 = sha256_bytes_hex(
                self._storage_json_bytes(
                    self._audit_share_segment_payload(
                        first_share_seq=first_share_seq,
                        last_share_seq=last_share_seq,
                        shares=selected_shares,
                    )
                )
            )
            if actual_range_sha256 != expected_range_sha256:
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {parent_body_uri}: "
                    f"share segment range {body_uri} expected {expected_range_sha256}, got {actual_range_sha256}"
                )
        elif kind == "segment_prefix":
            expected_prefix_sha256 = str(part.get("prefix_sha256") or "").lower()
            actual_prefix_sha256 = sha256_bytes_hex(
                self._storage_json_bytes(
                    self._audit_share_segment_payload(
                        first_share_seq=first_share_seq,
                        last_share_seq=last_share_seq,
                        shares=selected_shares,
                    )
                )
            )
            if actual_prefix_sha256 != expected_prefix_sha256:
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {parent_body_uri}: "
                    f"share segment prefix {body_uri} expected {expected_prefix_sha256}, got {actual_prefix_sha256}"
                )
        elif kind != "segment":
            raise RuntimeError(f"audit bundle body is not valid JSON at {parent_body_uri}: invalid share part kind")
        return copy.deepcopy(selected_shares)

    def _select_audit_share_segment_range(
        self,
        shares: list[Any],
        *,
        first_share_seq: int,
        last_share_seq: int,
        parent_body_uri: object,
        body_uri: object,
    ) -> list[Any]:
        selected: list[Any] = []
        previous_seq: int | None = None
        for share in shares:
            if not isinstance(share, dict):
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {parent_body_uri}: share segment {body_uri} has invalid share"
                )
            share_seq = int(share.get("share_seq") or 0)
            if previous_seq is not None and previous_seq + 1 != share_seq:
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {parent_body_uri}: share segment {body_uri} is not contiguous"
                )
            previous_seq = share_seq
            if first_share_seq <= share_seq <= last_share_seq:
                selected.append(share)
        if selected:
            if int(selected[0].get("share_seq") or 0) != first_share_seq:
                selected = []
            elif int(selected[-1].get("share_seq") or 0) != last_share_seq:
                selected = []
        if not selected and first_share_seq <= last_share_seq:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: "
                f"share segment {body_uri} does not contain requested range"
            )
        return selected

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
        audit_body_byte_len = self._audit_body_byte_len(body_uri, final_bundle)
        payload = {
            **payload,
            # Externalized rows store the body in body_uri and NULL here; legacy
            # rows (no body store configured) keep the inline body.
            "audit_bundle": None if body_uri is not None else final_bundle,
            "body_uri": body_uri,
            "audit_body_byte_len": audit_body_byte_len,
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
        audit_body_byte_len,
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
        (data->>'audit_body_byte_len')::bigint,
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
            "audit_bundle_sha256": audit_bundle_sha256,
            "body_uri": str(body_uri) if body_uri is not None else "",
            "audit_body_byte_len": audit_body_byte_len,
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
            credit_policy=(
                str(payload["credit_policy"])
                if payload.get("credit_policy") is not None
                else None
            ),
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


def _ctv_broadcast_summary_fields() -> tuple[str, ...]:
    return (
        "broadcast_attempt_count",
        "broadcast_attempt_detail_count",
        "first_broadcast_attempt_at",
        "last_broadcast_attempt_at",
        "last_broadcast_attempt_status",
        "last_broadcast_package_tx_hexes",
        "last_broadcast_package_txids",
        "last_broadcast_submit_result",
        "last_broadcast_error",
        "broadcast_attempt_status_counts",
        "next_broadcast_attempt_at",
        "broadcast_retry_backoff_seconds",
    )


def _empty_ctv_broadcast_summary() -> dict[str, Any]:
    return {
        "broadcast_attempt_count": 0,
        "broadcast_attempt_detail_count": 0,
        "first_broadcast_attempt_at": None,
        "last_broadcast_attempt_at": None,
        "last_broadcast_attempt_status": None,
        "last_broadcast_package_tx_hexes": [],
        "last_broadcast_package_txids": [],
        "last_broadcast_submit_result": None,
        "last_broadcast_error": None,
        "broadcast_attempt_status_counts": {},
        "next_broadcast_attempt_at": None,
        "broadcast_retry_backoff_seconds": 0,
    }


def _ctv_broadcast_attempt_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempt_count": int(payload.get("broadcast_attempt_count") or 0),
        "detail_count": int(payload.get("broadcast_attempt_detail_count") or 0),
        "first_attempt_at": payload.get("first_broadcast_attempt_at"),
        "last_attempt_at": payload.get("last_broadcast_attempt_at"),
        "last_attempt_status": payload.get("last_broadcast_attempt_status"),
        "last_package_tx_hexes": copy.deepcopy(payload.get("last_broadcast_package_tx_hexes") or []),
        "last_package_txids": copy.deepcopy(payload.get("last_broadcast_package_txids") or []),
        "last_submit_result": copy.deepcopy(payload.get("last_broadcast_submit_result")),
        "last_error": payload.get("last_broadcast_error"),
        "status_counts": copy.deepcopy(payload.get("broadcast_attempt_status_counts") or {}),
        "next_attempt_at": payload.get("next_broadcast_attempt_at"),
        "retry_backoff_seconds": int(payload.get("broadcast_retry_backoff_seconds") or 0),
    }


def _ctv_broadcast_attempt_due(value: object, now: datetime) -> bool:
    if value is None:
        return True
    if isinstance(value, datetime):
        candidate = value
    else:
        text = str(value).strip()
        if not text:
            return True
        if " " in text and "T" not in text:
            text = text.replace(" ", "T", 1)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        elif text.endswith("+00"):
            text = text[:-3] + "+00:00"
        try:
            candidate = datetime.fromisoformat(text)
        except ValueError:
            return True
    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=timezone.utc)
    return candidate <= now


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


def block_candidate_identity(candidate: dict[str, Any]) -> dict[str, Any]:
    """Candidate payload with its volatile acknowledgment stamp removed.

    A miner can resubmit the same solved block after a transient submit
    outcome, and the rebuilt intent differs from the persisted one only in
    pending_share.accepted_at_ms. That drift must stay idempotent against the
    durable outbox while any other divergence remains a hard payload
    mismatch. The stored payload keeps its original stamp; only comparisons
    use this identity form.
    """
    pending_share = candidate.get("pending_share")
    if isinstance(pending_share, dict) and "accepted_at_ms" in pending_share:
        candidate = {
            **candidate,
            "pending_share": {**pending_share, "accepted_at_ms": None},
        }
    return candidate


def block_candidate_identity_sha256(candidate: dict[str, Any]) -> str:
    return sha256_json_hex(block_candidate_identity(candidate))


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

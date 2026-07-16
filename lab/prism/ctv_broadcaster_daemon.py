#!/usr/bin/env python3
"""Durable CTV fanout broadcaster loop glue.

The lower-level :mod:`lab.prism.ctv_broadcaster` module knows how to talk to a
qbit node and funding wallet. This module connects that engine to the PRISM
ledger: read pending fanout artifacts, derive live status, submit broadcastable
packages, journal attempts, and update durable state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from lab.prism.ctv_broadcaster import (
    AWAITING_MATURITY,
    BROADCAST,
    BROADCASTABLE,
    CONFIRMED,
    REORGED,
    BroadcastAttempt,
    CtvFanoutBroadcaster,
    FanoutArtifact,
)


LEDGER_STATUS_BY_BROADCASTER_STATUS = {
    AWAITING_MATURITY: "awaiting_maturity",
    BROADCASTABLE: "broadcastable",
    BROADCAST: "broadcast_submitted",
    CONFIRMED: "confirmed",
    REORGED: "reorged",
}


class CtvFanoutLedger(Protocol):
    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]: ...

    def update_ctv_fanout_status(self, *, fanout_txid: str, settlement_status: str) -> dict[str, int | str]: ...

    def record_ctv_fanout_broadcast_attempt(
        self,
        *,
        fanout_txid: str,
        attempt_status: str,
        package_tx_hexes: list[str] | None = None,
        package_txids: list[str] | None = None,
        submit_result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, int | str]: ...


@dataclass(frozen=True)
class CtvFanoutDaemonResult:
    scanned_count: int
    submitted_count: int
    updated_count: int
    failed_count: int


def artifact_from_status_row(row: dict[str, object]) -> FanoutArtifact:
    anchor_vout = row.get("anchor_vout")
    return FanoutArtifact(
        fanout_txid=str(row["fanout_txid"]),
        fanout_tx_hex=str(row["fanout_tx_hex"]),
        anchor_vout=None if anchor_vout is None else int(anchor_vout),
        coinbase_txid=str(row["parent_coinbase_txid"]),
        coinbase_block_hash=str(row["block_hash"]),
        coinbase_height=int(row["block_height"]),
        parent_coinbase_vout=int(row["parent_coinbase_vout"]),
    )


class CtvFanoutBroadcastDaemon:
    def __init__(self, ledger: CtvFanoutLedger, broadcaster: CtvFanoutBroadcaster, *, fee_sats: int) -> None:
        if fee_sats < 0:
            raise ValueError("fee_sats must be non-negative")
        self.ledger = ledger
        self.broadcaster = broadcaster
        self.fee_sats = fee_sats

    def run_once(
        self,
        *,
        limit: int = 100,
        progress_callback: Callable[[], None] | None = None,
    ) -> CtvFanoutDaemonResult:
        """Process one batch, notifying after each row path has completed."""
        rows = self.ledger.pending_ctv_fanout_statuses(limit=limit)
        submitted_count = 0
        updated_count = 0
        failed_count = 0

        def record_progress() -> None:
            if progress_callback is not None:
                progress_callback()

        for row in rows:
            if str(row.get("settlement_status") or row.get("status") or "") in {
                "confirmed",
                "reorged",
                "failed",
            }:
                record_progress()
                continue
            if not broadcast_attempt_due(row.get("next_broadcast_attempt_at")):
                record_progress()
                continue
            artifact = artifact_from_status_row(row)
            attempt = self.broadcaster.broadcast(artifact, self.fee_sats)
            if attempt.submitted:
                submitted_count += 1
                self._journal_attempt(attempt, attempt_status="submitted")
                # record_ctv_fanout_broadcast_attempt moves the durable row to
                # broadcast_submitted, so do not double-update here.
                record_progress()
                continue

            if attempt.fee_sats is not None:
                attempt_status = "planned" if attempt.package_msg == "error" else "rejected"
                self._journal_attempt(attempt, attempt_status=attempt_status)
                failed_count += 1
                record_progress()
                continue

            next_status = LEDGER_STATUS_BY_BROADCASTER_STATUS.get(attempt.status)
            if next_status is not None:
                self.ledger.update_ctv_fanout_status(
                    fanout_txid=attempt.fanout_txid,
                    settlement_status=next_status,
                )
                updated_count += 1
                record_progress()
                continue

            self._journal_attempt(attempt, attempt_status="failed")
            failed_count += 1
            record_progress()

        return CtvFanoutDaemonResult(
            scanned_count=len(rows),
            submitted_count=submitted_count,
            updated_count=updated_count,
            failed_count=failed_count,
        )

    def _journal_attempt(self, attempt: BroadcastAttempt, *, attempt_status: str) -> None:
        package_txids = [attempt.fanout_txid]
        if attempt.child_txid is not None:
            package_txids.append(attempt.child_txid)
        self.ledger.record_ctv_fanout_broadcast_attempt(
            fanout_txid=attempt.fanout_txid,
            attempt_status=attempt_status,
            package_txids=package_txids,
            submit_result={
                "package_msg": attempt.package_msg,
                "submitted": attempt.submitted,
                "fee_sats": attempt.fee_sats,
            },
            error=None if attempt.submitted else attempt.detail,
        )


def broadcast_attempt_due(value: object) -> bool:
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
    return candidate <= datetime.now(timezone.utc)

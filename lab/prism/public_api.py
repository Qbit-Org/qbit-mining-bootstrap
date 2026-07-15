#!/usr/bin/env python3
"""Public dashboard read-model API wrappers for PRISM."""

from __future__ import annotations

import json
import math
import os
import threading
import time
import urllib.parse
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable

from lab.prism.ctv_broadcaster import COINBASE_MATURITY
from lab.prism import direct_stratum

TERAHASH = Decimal(1_000_000_000_000)
QBIT_DIFFICULTY_SCALE = 1_000_000
QBIT_POW_LIMIT_BITS = "207fffff"
QBIT_POW_LIMIT_TARGET = direct_stratum.target_from_compact_hex(QBIT_POW_LIMIT_BITS)
HASHES_PER_QBIT_SCALED_DIFFICULTY = (
    Decimal(2**256) / Decimal(QBIT_POW_LIMIT_TARGET) / Decimal(QBIT_DIFFICULTY_SCALE)
)
MAX_SEARCH_LENGTH = 128
MAX_RECIPIENT_ID_LENGTH = 256

PUBLIC_ERROR_CODES = {
    "not_found": "not_found",
    "rate_limited": "rate_limited",
    "internal_error": "internal_error",
    "qbit_rpc_unavailable": "upstream_unavailable",
}


@dataclass(frozen=True)
class PublicCachePolicy:
    ttl_seconds: int
    stale_while_revalidate_seconds: int
    immutable: bool = False


@dataclass
class _PublicCacheEntry:
    status: int
    payload: object
    stored_at: float
    expires_at: float


@dataclass
class _PublicInflight:
    event: threading.Event
    result: tuple[int, object, str, int] | None = None
    exception: BaseException | None = None


class PublicResponseCache:
    """Small in-process cache for public read-model responses."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: OrderedDict[
            tuple[str, tuple[tuple[str, tuple[str, ...]], ...]],
            _PublicCacheEntry,
        ] = OrderedDict()
        self._inflight: dict[
            tuple[str, tuple[tuple[str, tuple[str, ...]], ...]],
            _PublicInflight,
        ] = {}

    def get_or_compute(
        self,
        *,
        key: tuple[str, tuple[tuple[str, tuple[str, ...]], ...]],
        ttl_seconds: int,
        compute: Callable[[], tuple[int, object]],
    ) -> tuple[int, object, str, int]:
        if ttl_seconds <= 0:
            status, payload = compute()
            return status, payload, "BYPASS", 0

        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                if entry.expires_at > now:
                    self._entries.move_to_end(key)
                    return entry.status, entry.payload, "HIT", max(0, int(now - entry.stored_at))
                # Expired: drop the stale slot so it cannot count toward the bound.
                del self._entries[key]
            inflight = self._inflight.get(key)
            if inflight is None:
                inflight = _PublicInflight(event=threading.Event())
                self._inflight[key] = inflight
                owner = True
            else:
                owner = False

        if not owner:
            # Coalesce onto the owner's single origin call. Reuse its result even
            # when it was not cacheable (BYPASS), and re-raise its exception, so
            # waiters never re-run an expensive or failing compute().
            inflight.event.wait()
            if inflight.exception is not None:
                raise inflight.exception
            if inflight.result is not None:
                return inflight.result
            # Owner finished without recording a result; recompute defensively.
            return self.get_or_compute(key=key, ttl_seconds=ttl_seconds, compute=compute)

        try:
            status, payload = compute()
            if 200 <= status < 300 and cacheable_payload_size(payload):
                now = time.monotonic()
                with self._lock:
                    self._entries[key] = _PublicCacheEntry(
                        status=status,
                        payload=payload,
                        stored_at=now,
                        expires_at=now + ttl_seconds,
                    )
                    self._entries.move_to_end(key)
                    max_entries = public_cache_max_entries()
                    if len(self._entries) > max_entries:
                        # Reap expired entries before LRU eviction so dead slots
                        # never push out still-fresh keys.
                        for stale_key in [
                            stored_key
                            for stored_key, stored in self._entries.items()
                            if stored_key != key and stored.expires_at <= now
                        ]:
                            del self._entries[stale_key]
                    while len(self._entries) > max_entries:
                        self._entries.popitem(last=False)
                inflight.result = (status, payload, "MISS", 0)
            else:
                inflight.result = (status, payload, "BYPASS", 0)
            return inflight.result
        except BaseException as exc:
            inflight.exception = exc
            raise
        finally:
            with self._lock:
                self._inflight.pop(key, None)
            inflight.event.set()


def _is_hex64(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdefABCDEF" for char in value)


def _canonical_cache_path(path: str) -> str:
    """Lowercase 64-hex hash path segments so casing variants share one cache entry.

    dispatch() resolves block, fanout, and artifact hashes through clean_hash(),
    which lowercases hex. The cache key applies the same canonicalization so the
    same resource requested with different hash casing shares one entry. Only
    genuine 64-hex segments are folded, so non-hash routes (the literal
    /fanouts/pending list, or invalid ids that dispatch rejects) keep distinct
    keys and never collide with a canonicalized one.
    """
    if path.startswith("/public/v1/blocks/") and path.endswith("/settlement-artifacts"):
        inner = path[len("/public/v1/blocks/"):-len("/settlement-artifacts")].strip()
        if _is_hex64(inner):
            return f"/public/v1/blocks/{inner.lower()}/settlement-artifacts"
        return path
    if path.startswith("/public/v1/artifacts/"):
        inner = path[len("/public/v1/artifacts/"):].strip()
        if _is_hex64(inner):
            return f"/public/v1/artifacts/{inner.lower()}"
        return path
    if path.startswith("/public/v1/fanouts/"):
        inner = path[len("/public/v1/fanouts/"):].strip()
        if _is_hex64(inner):
            return f"/public/v1/fanouts/{inner.lower()}"
        return path
    return path


def public_cache_key(path: str, query: dict[str, list[str]]) -> tuple[str, tuple[tuple[str, tuple[str, ...]], ...]]:
    return _canonical_cache_path(path), tuple(sorted((key, tuple(values)) for key, values in query.items()))


def public_cache_policy(path: str) -> PublicCachePolicy:
    if not env_bool("PRISM_PUBLIC_CACHE_ENABLED", default=True):
        return PublicCachePolicy(ttl_seconds=0, stale_while_revalidate_seconds=0)
    if path == "/public/v1/mining-configuration":
        return PublicCachePolicy(
            ttl_seconds=env_nonnegative_int("PRISM_PUBLIC_CONFIG_CACHE_TTL_SECONDS", 300),
            stale_while_revalidate_seconds=env_nonnegative_int(
                "PRISM_PUBLIC_CONFIG_CACHE_STALE_WHILE_REVALIDATE_SECONDS",
                3600,
            ),
        )
    if path.startswith("/public/v1/artifacts/"):
        return PublicCachePolicy(
            ttl_seconds=env_nonnegative_int("PRISM_PUBLIC_ARTIFACT_CACHE_TTL_SECONDS", 86400),
            stale_while_revalidate_seconds=env_nonnegative_int(
                "PRISM_PUBLIC_ARTIFACT_CACHE_STALE_WHILE_REVALIDATE_SECONDS",
                86400,
            ),
            immutable=True,
        )
    return PublicCachePolicy(
        ttl_seconds=env_nonnegative_int("PRISM_PUBLIC_CACHE_TTL_SECONDS", 5),
        stale_while_revalidate_seconds=env_nonnegative_int(
            "PRISM_PUBLIC_CACHE_STALE_WHILE_REVALIDATE_SECONDS",
            30,
        ),
    )


def public_cache_headers(policy: PublicCachePolicy, *, cache_state: str, age_seconds: int) -> dict[str, str]:
    headers = {
        "Cache-Control": "public, max-age=0, must-revalidate",
        "Age": str(max(0, age_seconds)),
    }
    if env_bool("PRISM_PUBLIC_CACHE_DEBUG_HEADERS", default=False):
        headers["X-Prism-Public-Cache"] = cache_state.lower()
    if policy.ttl_seconds > 0:
        shared_policy = f"public, max-age={policy.ttl_seconds}"
        if policy.stale_while_revalidate_seconds > 0:
            shared_policy += f", stale-while-revalidate={policy.stale_while_revalidate_seconds}"
        if policy.immutable:
            shared_policy += ", immutable"
        headers["CDN-Cache-Control"] = shared_policy
        headers["Vercel-CDN-Cache-Control"] = shared_policy
    return headers


def public_error_headers() -> dict[str, str]:
    return {"Cache-Control": "no-store"}


def public_cache_max_entries() -> int:
    return max(1, env_nonnegative_int("PRISM_PUBLIC_CACHE_MAX_ENTRIES", 512))


def cacheable_payload_size(payload: object) -> bool:
    max_bytes = env_nonnegative_int("PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES", 1_048_576)
    if max_bytes <= 0:
        return False
    body_size = len(json.dumps(payload, sort_keys=True).encode()) + 1
    return body_size <= max_bytes


def env_bool(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_nonnegative_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(0, parsed)


class PublicApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def dispatch(coordinator: Any, path: str, query: dict[str, list[str]]) -> tuple[int, object]:
    if path == "/public/v1/pool-summary":
        return 200, pool_summary(coordinator)
    if path == "/public/v1/blocks":
        page, limit = pagination_params(query)
        return 200, blocks(coordinator, page=page, limit=limit)
    if path == "/public/v1/leaderboard":
        page, limit = pagination_params(query)
        search = search_param(query)
        window = first_query_value(query, "window") or "3h"
        if window != "3h":
            raise PublicApiError(400, "invalid_window", "window must be 3h")
        return 200, leaderboard(coordinator, page=page, limit=limit, search=search)
    if path == "/public/v1/hashrate-series":
        subject = first_query_value(query, "subject") or "pool"
        range_id = first_query_value(query, "range") or "1m"
        bucket = first_query_value(query, "bucket") or "auto"
        return 200, hashrate_series(coordinator, subject=subject, range_id=range_id, bucket=bucket)
    if path == "/public/v1/mining-configuration":
        return 200, mining_configuration(coordinator)
    if path.startswith("/public/v1/miners/"):
        suffix = path.removeprefix("/public/v1/miners/")
        parts = suffix.split("/")
        if len(parts) == 2 and parts[1] == "earnings":
            recipient_id = urllib.parse.unquote(parts[0])
            page, limit = pagination_params(query)
            return 200, miner_earnings(coordinator, recipient_id=recipient_id, page=page, limit=limit)
        if len(parts) == 2 and parts[1] == "payouts":
            recipient_id = urllib.parse.unquote(parts[0])
            page, limit = pagination_params(query)
            return 200, miner_payouts(coordinator, recipient_id=recipient_id, page=page, limit=limit)
        if len(parts) == 2 and parts[1] == "workers":
            recipient_id = urllib.parse.unquote(parts[0])
            page, limit = pagination_params(query)
            search = search_param(query)
            hide_inactive = (first_query_value(query, "hide_inactive") or "true").lower() in {"1", "true", "yes", "on"}
            return 200, miner_workers(
                coordinator,
                recipient_id=recipient_id,
                page=page,
                limit=limit,
                search=search,
                hide_inactive=hide_inactive,
            )
        if len(parts) == 1:
            return 200, miner(coordinator, recipient_id=urllib.parse.unquote(suffix))
        raise PublicApiError(404, "not_found", "unknown public dashboard endpoint")
    if path.startswith("/public/v1/blocks/") and path.endswith("/settlement-artifacts"):
        block_hash = clean_hash(path.removeprefix("/public/v1/blocks/").removesuffix("/settlement-artifacts"))
        return 200, settlement_artifacts(coordinator, block_hash=block_hash)
    if path == "/public/v1/fanouts/pending":
        page, limit = pagination_params(query)
        return 200, pending_fanouts(coordinator, page=page, limit=limit)
    if path.startswith("/public/v1/fanouts/"):
        fanout_txid = clean_hash(path.removeprefix("/public/v1/fanouts/"), name="fanout txid")
        return 200, fanout(coordinator, fanout_txid=fanout_txid)
    if path.startswith("/public/v1/artifacts/"):
        sha256 = clean_hash(path.removeprefix("/public/v1/artifacts/"), name="artifact sha256")
        return 200, artifact(coordinator, sha256=sha256)
    raise PublicApiError(404, "not_found", "unknown public dashboard endpoint")


def error_payload(code: str, message: str) -> dict[str, object]:
    error_code = PUBLIC_ERROR_CODES.get(code)
    if error_code is None:
        error_code = "bad_request" if code.startswith("invalid_") else "internal_error"
    return {
        "schema": "prism.dashboard.error.v1",
        "error": {
            "code": error_code,
            "message": message,
            "request_id": None,
        },
    }


def pool_summary(coordinator: Any) -> dict[str, object]:
    generated_at = utc_now_iso()
    network = network_summary(coordinator)
    ledger_snapshot = coordinator.ledger.dashboard_pool_snapshot(
        current_network_difficulty=network["network_difficulty"],
        generated_at=generated_at,
    )
    return {
        "schema": "prism.dashboard.pool-summary.v1",
        "generated_at": generated_at,
        "network": network,
        "pool": {
            "name": os.environ.get("PRISM_PUBLIC_POOL_NAME", "PRISM"),
            "hashrate_ths": ledger_snapshot["hashrate_ths"],
            "participants_3h": ledger_snapshot["participants_3h"],
            "blocks_found_total": ledger_snapshot["blocks_found_total"],
            "prism_blocks_total": ledger_snapshot["prism_blocks_total"],
            "total_mined_bits": ledger_snapshot["total_mined_bits"],
            "expected_time_to_block_seconds": expected_time_to_block_seconds(
                hashrate_ths=str(ledger_snapshot["hashrate_ths"]["h3"]),
                network_difficulty=str(network["network_difficulty"]),
            ),
            "latest_block": ledger_snapshot["latest_block"],
            "reward_window": ledger_snapshot["reward_window"],
        },
    }


def blocks(coordinator: Any, *, page: int, limit: int) -> dict[str, object]:
    generated_at = utc_now_iso()
    payload = coordinator.ledger.dashboard_blocks(page=page, limit=limit)
    for row in payload["rows"]:
        row.setdefault("bits", "00000000")
        if row.get("bits") is None:
            row["bits"] = "00000000"
        row.setdefault("network_difficulty", "0")
        if row.get("network_difficulty") is None:
            row["network_difficulty"] = "0"
    return {
        "schema": "prism.dashboard.blocks.v1",
        "generated_at": generated_at,
        "pagination": payload["pagination"],
        "rows": payload["rows"],
    }


def leaderboard(coordinator: Any, *, page: int, limit: int, search: str | None) -> dict[str, object]:
    generated_at = utc_now_iso()
    payload = coordinator.ledger.dashboard_leaderboard(page=page, limit=limit, search=search)
    return {
        "schema": "prism.dashboard.leaderboard.v1",
        "generated_at": generated_at,
        "window": {
            "id": "3h",
            "started_at": payload["started_at"],
            "ended_at": payload["ended_at"],
        },
        "totals": payload["totals"],
        "pagination": payload["pagination"],
        "rows": payload["rows"],
    }


def hashrate_series(coordinator: Any, *, subject: str, range_id: str, bucket: str) -> dict[str, object]:
    if range_id not in {"1w", "1m", "6m", "all"}:
        raise PublicApiError(400, "invalid_range", "range must be one of 1w, 1m, 6m, all")
    if bucket == "auto":
        bucket = auto_bucket(range_id)
    if bucket not in {"5m", "1h", "1d"}:
        raise PublicApiError(400, "invalid_bucket", "bucket must be one of auto, 5m, 1h, 1d")
    if subject == "pool":
        subject_type = "pool"
        subject_id = None
    elif subject.startswith("miner:") and subject.removeprefix("miner:"):
        subject_type = "miner"
        subject_id = subject.removeprefix("miner:")
    else:
        raise PublicApiError(400, "invalid_subject", "subject must be pool or miner:{recipient_id}")
    generated_at = utc_now_iso()
    return {
        "schema": "prism.dashboard.hashrate-series.v1",
        "generated_at": generated_at,
        "subject": {"type": subject_type, "id": subject_id},
        "range": range_id,
        "bucket": bucket,
        "unit": "ths",
        "points": coordinator.ledger.dashboard_hashrate_series(
            subject_type=subject_type,
            subject_id=subject_id,
            range_id=range_id,
            bucket=bucket,
        ),
    }


def mining_configuration(coordinator: Any) -> dict[str, object]:
    port = int(getattr(coordinator, "port", os.environ.get("PRISM_STRATUM_PORT", "3340")))
    host = os.environ.get("PRISM_PUBLIC_STRATUM_HOST") or str(getattr(coordinator, "bind", "127.0.0.1"))
    url = os.environ.get("PRISM_PUBLIC_STRATUM_URL") or f"stratum+tcp://{host}:{port}"
    stratum_endpoints = [mining_endpoint("Primary", url, fallback_port=port)]
    highdiff_endpoint = public_highdiff_stratum_endpoint(host=host)
    if highdiff_endpoint is not None:
        stratum_endpoints.append(highdiff_endpoint)
    # Read the fee through the same clamped/robust helper the reward estimate uses so
    # the displayed fee and estimated_reward_bits never diverge (and a bad env value
    # can't 500 this endpoint).
    pool_fee_bps = public_pool_fee_bps()
    return {
        "schema": "prism.dashboard.mining-configuration.v1",
        "generated_at": utc_now_iso(),
        "active_configuration_id": "default",
        "configurations": [
            {
                "id": "default",
                "label": os.environ.get("PRISM_PUBLIC_CONFIGURATION_LABEL", "PRISM default"),
                "description": os.environ.get(
                    "PRISM_PUBLIC_CONFIGURATION_DESCRIPTION",
                    "Default PRISM Stratum endpoint using the pool's current block template and payout policy.",
                ),
                "pool_fee_bps": pool_fee_bps,
                "block_template_policy": os.environ.get(
                    "PRISM_PUBLIC_BLOCK_TEMPLATE_POLICY",
                    "pool-selected qbit block template with PRISM payout settlement",
                ),
                "stratum_endpoints": stratum_endpoints,
            }
        ],
    }


def mining_endpoint(label: str, url: str, *, fallback_port: int) -> dict[str, object]:
    return {
        "label": label,
        "url": url,
        "protocol": "stratum_v1",
        "default_port": stratum_url_default_port(url, fallback_port=fallback_port),
    }


def public_highdiff_stratum_endpoint(*, host: str) -> dict[str, object] | None:
    highdiff_port = optional_tcp_port(os.environ.get("PRISM_STRATUM_HIGHDIFF_PORT"))
    if highdiff_port is None:
        return None
    url = (
        os.environ.get("PRISM_PUBLIC_STRATUM_HIGHDIFF_URL")
        or public_stratum_url_with_port(os.environ.get("PRISM_PUBLIC_STRATUM_URL"), highdiff_port)
        or f"stratum+tcp://{host}:{highdiff_port}"
    )
    return mining_endpoint("High-diff", url, fallback_port=highdiff_port)


def public_stratum_url_with_port(url: str | None, port: int) -> str | None:
    if url is None or url.strip() == "":
        return None
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError:
        return None
    if not parsed.scheme or parsed.hostname is None:
        return None
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{parsed.scheme}://{host}:{port}"


def optional_tcp_port(value: str | None) -> int | None:
    if value is None or value.strip() == "":
        return None
    try:
        port = int(value)
    except ValueError:
        return None
    return port if 0 < port < 65536 else None


def stratum_url_default_port(url: str, *, fallback_port: int) -> int:
    try:
        port = urllib.parse.urlparse(url).port
    except ValueError:
        return fallback_port
    return port if port is not None else fallback_port


def public_minimum_payout_bits() -> int:
    value = first_optional_nonnegative_int(
        os.environ.get("PRISM_PUBLIC_MINIMUM_PAYOUT_BITS"),
        os.environ.get("PRISM_PAYOUT_MIN_OUTPUT_BITS"),
        os.environ.get("PRISM_PAYOUT_MIN_OUTPUT_SATS"),
    )
    return value if value is not None else 0


def public_pool_fee_bps() -> int:
    """Pool fee (basis points) surfaced to the dashboard, clamped to [0, 10000]."""
    try:
        bps = int(os.environ.get("PRISM_PUBLIC_POOL_FEE_BPS") or "0")
    except (TypeError, ValueError):
        return 0
    return max(0, min(10_000, bps))


def latest_block_coinbase_value_bits(coordinator: Any) -> int | None:
    """Best-available estimate of the coinbase reward for the next block.

    Uses the most recently found block's coinbase value as the reference for an
    upcoming block. Returns None when no block is available yet (fresh pool) so
    callers can omit the estimate rather than showing a fabricated amount.
    """
    try:
        payload = coordinator.ledger.dashboard_blocks(page=1, limit=1)
    except Exception:
        return None
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not rows:
        return None
    try:
        value = int(rows[0].get("coinbase_value_bits"))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def estimated_next_block_reward_bits(
    *, share_percent: str | None, expected_coinbase_bits: object, pool_fee_bps: int
) -> int | None:
    """Miner's expected reward if the pool solves the next block.

    Their reward-window share (share_percent) applied to the miner-distributable
    portion of the expected coinbase (coinbase minus the pool fee). Returns None
    when the share or a coinbase reference is unavailable, so the dashboard can
    fall back to showing only the share percentage.
    """
    if share_percent is None or expected_coinbase_bits is None:
        return None
    try:
        coinbase = Decimal(str(expected_coinbase_bits))
        percent = Decimal(str(share_percent))
    except (ArithmeticError, ValueError):
        return None
    if not coinbase.is_finite() or not percent.is_finite() or coinbase <= 0 or percent < 0:
        return None
    fee_bps = max(0, min(10_000, pool_fee_bps))
    distributable = coinbase * Decimal(10_000 - fee_bps) / Decimal(10_000)
    reward = distributable * percent / Decimal(100)
    return max(0, int(reward))


def miner(coordinator: Any, *, recipient_id: str) -> dict[str, object]:
    recipient_id = clean_recipient_id(recipient_id)
    network = network_summary(coordinator)
    share_summary = miner_share_summary(coordinator.ledger, recipient_id)
    reward_window = miner_reward_window(
        coordinator.ledger,
        recipient_id,
        current_network_difficulty=network["network_difficulty"],
    )
    owed_balance_bits = owed_balance_for_recipient(coordinator.ledger, recipient_id)
    payouts = payout_rows(coordinator, recipient_id=recipient_id, page=1, limit=5)
    recent_payouts = payouts["rows"]
    workers_payload = miner_worker_rows(
        coordinator.ledger,
        recipient_id=recipient_id,
        page=1,
        limit=5,
        search=None,
        hide_inactive=False,
    )
    workers = workers_payload["rows"]
    reward_window_percent = reward_window["share_percent"]
    minimum_payout_bits = public_minimum_payout_bits()
    lifetime_earnings_bits = lifetime_earnings_for_recipient(coordinator.ledger, recipient_id)
    pending_maturity_bits = pending_maturity_bits_for_recipient(coordinator.ledger, recipient_id)
    estimated_reward_bits = estimated_next_block_reward_bits(
        share_percent=reward_window_percent,
        expected_coinbase_bits=latest_block_coinbase_value_bits(coordinator),
        pool_fee_bps=public_pool_fee_bps(),
    )
    return {
        "schema": "prism.dashboard.miner.v1",
        "generated_at": utc_now_iso(),
        "recipient_id": recipient_id,
        "display_name": None,
        "owed_balance_bits": owed_balance_bits,
        "lifetime_earnings_bits": lifetime_earnings_bits,
        "pending_maturity_bits": pending_maturity_bits,
        "unpaid_earnings_bits": owed_balance_bits,
        "minimum_payout_bits": minimum_payout_bits,
        "hashrate_ths": share_summary["hashrate_ths"],
        "shares": {
            "accepted_3h": share_summary["accepted_3h"],
            "accepted_difficulty_3h": share_summary["accepted_difficulty_3h"],
            "last_share_at": share_summary["last_share_at"],
        },
        "estimated_next_block": {
            "share_percent": reward_window_percent,
            "estimated_reward_bits": estimated_reward_bits,
        },
        "estimated_time_to_minimum_payout_seconds": 0 if owed_balance_bits >= minimum_payout_bits else None,
        "reward_window_percent": reward_window_percent,
        "workers_currently_hashing": int(workers_payload.get("active_count", sum(1 for row in workers if row["status"] == "active"))),
        "workers": workers[:5],
        "recent_payouts": recent_payouts,
    }


def miner_earnings(coordinator: Any, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
    recipient_id = clean_recipient_id(recipient_id)
    return {
        "schema": "prism.dashboard.miner-earnings.v1",
        "generated_at": utc_now_iso(),
        "recipient_id": recipient_id,
        **earnings_rows(coordinator, recipient_id=recipient_id, page=page, limit=limit),
    }


def miner_payouts(coordinator: Any, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
    recipient_id = clean_recipient_id(recipient_id)
    return {
        "schema": "prism.dashboard.miner-payouts.v1",
        "generated_at": utc_now_iso(),
        "recipient_id": recipient_id,
        **payout_rows(coordinator, recipient_id=recipient_id, page=page, limit=limit),
    }


def miner_workers(
    coordinator: Any,
    *,
    recipient_id: str,
    page: int,
    limit: int,
    search: str | None,
    hide_inactive: bool,
) -> dict[str, object]:
    recipient_id = clean_recipient_id(recipient_id)
    payload = miner_worker_rows(
        coordinator.ledger,
        recipient_id=recipient_id,
        page=page,
        limit=limit,
        search=search,
        hide_inactive=hide_inactive,
    )
    return {
        "schema": "prism.dashboard.miner-workers.v1",
        "generated_at": utc_now_iso(),
        "recipient_id": recipient_id,
        "pagination": payload["pagination"],
        "rows": payload["rows"],
    }


def settlement_artifacts(coordinator: Any, *, block_hash: str) -> dict[str, object]:
    generated_at = utc_now_iso()
    payload = coordinator.ledger.audit_ctv_fanout_manifest_set(block_hash=block_hash)
    if payload is None:
        payload = direct_coinbase_settlement_payload(coordinator.ledger, block_hash=block_hash)
    if payload is None:
        raise PublicApiError(404, "not_found", "unknown PRISM settlement artifact block")
    artifacts = payload.get("artifacts", [])
    if not isinstance(artifacts, list):
        artifacts = []
    artifact_links: list[dict[str, object]] = []
    audit_bundle_sha256 = nullable_str(payload.get("audit_bundle_sha256"))
    if audit_bundle_sha256 and public_artifact_available(coordinator.ledger, sha256=audit_bundle_sha256):
        artifact_links.append(artifact_link("audit_bundle", audit_bundle_sha256, None))
    manifest_set_sha256 = nullable_str(payload.get("manifest_set_sha256"))
    if manifest_set_sha256:
        manifest_set_json = nullable_str(payload.get("manifest_set_json"))
        artifact_links.append(
            artifact_link(
                "ctv_manifest_set",
                manifest_set_sha256,
                len(manifest_set_json.encode()) if manifest_set_json is not None else None,
            )
        )
    fanout_rows = settlement_fanout_rows(coordinator.ledger, payload, artifacts)
    return {
        "schema": "prism.dashboard.settlement-artifacts.v1",
        "generated_at": generated_at,
        "block_hash": block_hash,
        "block_height": first_int(payload.get("block_height"), *(row.get("block_height") for row in fanout_rows), default=0),
        "settlement_mode": str(payload.get("settlement_mode") or "hybrid_coinbase_ctv_fanout"),
        "audit_bundle_sha256": audit_bundle_sha256,
        "payout_manifest_sha256": nullable_str(payload.get("payout_manifest_sha256")),
        "artifact_links": artifact_links,
        "fanouts": [fanout_public_row(row) for row in fanout_rows],
    }


def direct_coinbase_settlement_payload(ledger: Any, *, block_hash: str) -> dict[str, object] | None:
    getter = getattr(ledger, "audit_bundle", None)
    if not callable(getter):
        return None
    try:
        row = getter(block_hash=block_hash)
    except RuntimeError as exc:
        if audit_bundle_body_read_failed(exc):
            return None
        raise
    if not isinstance(row, dict):
        return None
    bundle = row.get("audit_bundle")
    if not isinstance(bundle, dict):
        return None
    if audit_bundle_settlement_mode(bundle) != "direct_coinbase":
        return None
    return {
        "block_hash": optional_hex_hash(row.get("block_hash")) or block_hash,
        "block_height": first_int(
            row.get("block_height"),
            audit_bundle_section_value(bundle, "found_block", "block_height"),
            audit_bundle_section_value(bundle, "reward_manifest", "block_height"),
            audit_bundle_section_value(bundle, "ledger_window_attestation", "block_height"),
            default=0,
        ),
        "settlement_mode": "direct_coinbase",
        "audit_bundle_sha256": nullable_str(row.get("audit_bundle_sha256")),
        "payout_manifest_sha256": nullable_str(row.get("payout_manifest_sha256")),
        "artifacts": [],
    }


def audit_bundle_body_read_failed(exc: RuntimeError) -> bool:
    message = str(exc)
    return (
        message.startswith("audit bundle body is not retrievable")
        or message.startswith("audit bundle body hash mismatch")
        or message.startswith("audit bundle body is not valid JSON")
    )


def audit_bundle_settlement_mode(bundle: dict[str, object]) -> str | None:
    decision = bundle.get("settlement_mode_decision")
    if not isinstance(decision, dict):
        return None
    return nullable_str(decision.get("mode"))


def audit_bundle_section_value(bundle: dict[str, object], section_name: str, key: str) -> object:
    section = bundle.get(section_name)
    if not isinstance(section, dict):
        return None
    return section.get(key)


def pending_fanouts(coordinator: Any, *, page: int, limit: int) -> dict[str, object]:
    generated_at = utc_now_iso()
    read_model = getattr(coordinator.ledger, "dashboard_pending_fanout_rows", None)
    if callable(read_model):
        payload = read_model(page=page, limit=limit)
        rows = payload["rows"]
        pagination_payload = payload["pagination"]
    else:
        raise PublicApiError(500, "internal_error", "internal server error")
    return {
        "schema": "prism.dashboard.pending-fanouts.v1",
        "generated_at": generated_at,
        "pagination": pagination_payload,
        "rows": [fanout_public_row(row) for row in rows],
    }


def fanout(coordinator: Any, *, fanout_txid: str) -> dict[str, object]:
    payload = coordinator.ledger.ctv_fanout_status(fanout_txid=fanout_txid)
    if payload is None:
        raise PublicApiError(404, "not_found", "unknown PRISM CTV fanout")
    return {
        "schema": "prism.dashboard.fanout.v1",
        "generated_at": utc_now_iso(),
        "fanout": fanout_public_row(payload),
    }


def artifact(coordinator: Any, *, sha256: str) -> object:
    getter = getattr(coordinator.ledger, "dashboard_public_artifact", None)
    if not callable(getter):
        raise PublicApiError(404, "not_found", "unknown public PRISM artifact")
    payload = getter(sha256=sha256)
    if payload is None:
        raise PublicApiError(404, "not_found", "unknown public PRISM artifact")
    return payload


def public_artifact_available(ledger: Any, *, sha256: str) -> bool:
    exists = getattr(ledger, "dashboard_public_artifact_exists", None)
    if callable(exists):
        return bool(exists(sha256=sha256))
    getter = getattr(ledger, "dashboard_public_artifact", None)
    return bool(callable(getter) and getter(sha256=sha256) is not None)


def settlement_fanout_rows(ledger: Any, payload: dict[str, object], artifacts: list[object]) -> list[dict[str, object]]:
    parent_fields = _fanout_parent_fields(payload)
    rows: list[dict[str, object]] = []
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        row = {**artifact, **parent_fields}
        fanout_txid = optional_hex_hash(row.get("fanout_txid"))
        if fanout_txid:
            live_status = ledger.ctv_fanout_status(fanout_txid=fanout_txid)
            if isinstance(live_status, dict):
                row = {**row, **live_status}
        rows.append(row)
    return rows


def fanout_public_row(payload: dict[str, object]) -> dict[str, object]:
    fanout_tx_hex = str(payload.get("fanout_tx_hex") or "")
    attempts = payload.get("broadcast_attempts")
    last_attempt = None
    if isinstance(attempts, list) and attempts:
        last_attempt = attempts[-1]
    last_broadcast_attempt_at = payload.get("last_broadcast_attempt_at")
    last_broadcast_error = payload.get("last_broadcast_error")
    if last_broadcast_attempt_at is None and isinstance(last_attempt, dict):
        last_broadcast_attempt_at = last_attempt.get("attempted_at")
    if last_broadcast_error is None and isinstance(last_attempt, dict):
        last_broadcast_error = last_attempt.get("error")
    status = str(payload.get("settlement_status") or payload.get("status") or "awaiting_maturity")
    manifest_sha256 = str(payload.get("manifest_sha256") or "")
    explorer_prefix = os.environ.get("PRISM_PUBLIC_EXPLORER_TX_URL_PREFIX")
    fanout_txid = str(payload.get("fanout_txid") or "")
    block_height = first_int(payload.get("block_height"), default=0)
    anchor_vout = nullable_int(payload.get("anchor_vout"))
    fee_bits = fanout_fee_bits(payload)
    return {
        "fanout_txid": fanout_txid,
        "block_hash": str(payload.get("block_hash") or ""),
        "block_height": block_height,
        "status": status,
        "broadcastable_at_height": fanout_broadcastable_at_height(payload, block_height=block_height),
        "manifest_set_sha256": str(payload.get("manifest_set_sha256") or ""),
        "manifest_sha256": manifest_sha256,
        "manifest_url": artifact_url(manifest_sha256) if manifest_sha256 else None,
        "audit_bundle_sha256": nullable_str(payload.get("audit_bundle_sha256")),
        "parent_coinbase_txid": str(payload.get("parent_coinbase_txid") or ""),
        "parent_coinbase_vout": int(payload.get("parent_coinbase_vout", 0)),
        "anchor_vout": anchor_vout,
        "covenant_output_value_bits": int(payload.get("covenant_output_value_sats", 0)),
        "fanout_output_sum_bits": int(payload.get("fanout_output_sum_sats", 0)),
        "fanout_fee_bits": fee_bits,
        "fanout_tx_hex": fanout_tx_hex,
        "fanout_tx_sha256": fanout_transaction_hash(payload, fanout_tx_hex),
        "cpfp_anchor_spendable": status == "broadcastable" and anchor_vout is not None and fee_bits == 0,
        "last_broadcast_attempt_at": public_timestamp(last_broadcast_attempt_at),
        "last_broadcast_error": nullable_str(last_broadcast_error),
        "explorer_url": explorer_prefix.rstrip("/") + "/" + fanout_txid if explorer_prefix and fanout_txid else None,
    }


def fanout_fee_bits(payload: dict[str, object]) -> int:
    explicit_fee = payload.get("fanout_fee_sats")
    if explicit_fee is not None:
        try:
            return max(0, int(explicit_fee))
        except (TypeError, ValueError):
            return 0
    try:
        covenant_value = int(payload.get("covenant_output_value_sats", 0))
        output_sum = int(payload.get("fanout_output_sum_sats", 0))
    except (TypeError, ValueError):
        return 0
    return max(0, covenant_value - output_sum)


def nullable_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fanout_broadcastable_at_height(payload: dict[str, object], *, block_height: int) -> int | None:
    explicit_height = payload.get("broadcastable_at_height")
    if explicit_height is not None:
        try:
            return int(explicit_height)
        except (TypeError, ValueError):
            return None
    if block_height <= 0:
        return None
    return block_height + COINBASE_MATURITY


def artifact_link(kind: str, sha256: str, byte_length: int | None) -> dict[str, object]:
    return {
        "kind": kind,
        "sha256": sha256,
        "url": artifact_url(sha256),
        "content_type": "application/json",
        "byte_length": byte_length,
    }


def artifact_url(sha256: str) -> str:
    return f"/public/v1/artifacts/{sha256}"


def _fanout_parent_fields(payload: dict[str, object]) -> dict[str, object]:
    fields: dict[str, object] = {}
    for key in ("block_hash", "block_height", "audit_bundle_sha256", "manifest_set_sha256"):
        value = payload.get(key)
        if value is not None:
            fields[key] = value
    return fields


def network_summary(coordinator: Any) -> dict[str, object]:
    try:
        blockchain_info = coordinator.rpc.call("getblockchaininfo")
    except Exception as exc:
        raise PublicApiError(503, "qbit_rpc_unavailable", "qbit RPC getblockchaininfo failed") from exc
    if not isinstance(blockchain_info, dict):
        raise PublicApiError(503, "qbit_rpc_unavailable", "qbit RPC getblockchaininfo returned an invalid payload")
    chain = str(blockchain_info.get("chain") or os.environ.get("QBIT_CHAIN") or "qbit")
    template: dict[str, object] = {}
    try:
        raw_template = coordinator.rpc.call("getblocktemplate", [{"rules": qbit_gbt_rules(chain)}])
        if isinstance(raw_template, dict):
            template = raw_template
    except Exception:
        template = {}
    try:
        network_info = coordinator.rpc.call("getnetworkinfo")
    except Exception:
        network_info = {}
    raw_bits = first_present(template.get("bits"), blockchain_info.get("bits"))
    bits = str(raw_bits if raw_bits is not None else "00000000").lower()
    if len(bits) != 8 or any(char not in "0123456789abcdef" for char in bits):
        raise PublicApiError(503, "qbit_rpc_unavailable", "qbit RPC returned invalid compact bits")
    # network_difficulty is reported in PRISM's scaled difficulty units
    # (QBIT_DIFFICULTY_SCALE, see scaled_network_difficulty) so it lines up with the
    # scaled per-share difficulties recorded in the ledger. Every downstream consumer
    # assumes those units: the reward-window weight is network_difficulty * 8 compared
    # against summed share_difficulty, and expected_time_to_block_seconds multiplies by
    # HASHES_PER_QBIT_SCALED_DIFFICULTY. Derive it from the compact bits -- the same
    # source the coordinator's share pipeline uses (build_job_for_client) -- rather than
    # the raw getblockchaininfo.difficulty / getblocktemplate difficulty float, which is
    # ~QBIT_DIFFICULTY_SCALE (1e6) times smaller. Feeding the raw value here made the ETA
    # round to 0 and collapsed the reward window to a single share.
    if raw_bits is not None:
        try:
            difficulty = scaled_network_difficulty(bits)
        except ValueError as exc:
            raise PublicApiError(503, "qbit_rpc_unavailable", "qbit RPC returned invalid compact bits") from exc
    else:
        difficulty = 1
    try:
        difficulty_string = decimal_string(difficulty)
    except Exception as exc:
        raise PublicApiError(503, "qbit_rpc_unavailable", "qbit RPC returned invalid network difficulty") from exc
    peers = first_nonnegative_int(network_info.get("connections") if isinstance(network_info, dict) else None)
    return {
        "name": chain,
        "height": first_nonnegative_int(blockchain_info.get("blocks"), blockchain_info.get("headers")),
        "tip_hash": str(blockchain_info.get("bestblockhash") or "0" * 64).lower(),
        "bits": bits,
        "network_difficulty": difficulty_string,
        "initial_block_download": bool(blockchain_info.get("initialblockdownload", False)),
        "peers": peers,
    }


def first_present(*values: object) -> object | None:
    for value in values:
        if value is not None:
            return value
    return None


def first_nonnegative_int(*values: object, default: int = 0) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            continue
    return default


def expected_time_to_block_seconds(*, hashrate_ths: str, network_difficulty: str) -> int | None:
    hashrate = Decimal(hashrate_ths)
    if hashrate <= 0:
        return None
    difficulty = Decimal(network_difficulty)
    seconds = difficulty * HASHES_PER_QBIT_SCALED_DIFFICULTY / (hashrate * TERAHASH)
    if seconds < 0:
        return None
    return max(0, int(seconds.to_integral_value()))


def hashrate_ths_from_difficulty(total_difficulty: int | str | Decimal, seconds: int) -> str:
    if seconds <= 0:
        return "0"
    difficulty = Decimal(str(total_difficulty))
    if difficulty <= 0:
        return "0"
    return decimal_string(difficulty * HASHES_PER_QBIT_SCALED_DIFFICULTY / Decimal(seconds) / TERAHASH)


def pagination(page: int, limit: int, total_count: int) -> dict[str, int]:
    total_pages = math.ceil(total_count / limit) if total_count else 0
    return {
        "page": page,
        "limit": limit,
        "total_count": total_count,
        "total_pages": total_pages,
    }


def pagination_params(query: dict[str, list[str]]) -> tuple[int, int]:
    try:
        page = int(first_query_value(query, "page") or "1")
        limit = int(first_query_value(query, "limit") or "15")
    except ValueError as exc:
        raise PublicApiError(400, "invalid_pagination", "page and limit must be integers") from exc
    if page < 1:
        raise PublicApiError(400, "invalid_page", "page must be >= 1")
    if limit < 1 or limit > 100:
        raise PublicApiError(400, "invalid_limit", "limit must be between 1 and 100")
    return page, limit


def clean_hash(value: str, *, name: str = "hash") -> str:
    value = value.strip()
    if len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise PublicApiError(400, "invalid_hash", f"{name} must be 64 hex characters")
    return value.lower()


def clean_recipient_id(value: str) -> str:
    value = value.strip()
    if not value:
        raise PublicApiError(400, "invalid_recipient_id", "recipient_id is required")
    if len(value) > MAX_RECIPIENT_ID_LENGTH:
        raise PublicApiError(400, "invalid_recipient_id", "recipient_id must be 256 characters or fewer")
    return value


def first_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if values:
        return values[0]
    return None


def search_param(query: dict[str, list[str]]) -> str | None:
    search = first_query_value(query, "search")
    if search is not None and len(search) > MAX_SEARCH_LENGTH:
        raise PublicApiError(400, "invalid_search", "search must be 128 characters or fewer")
    return search


def auto_bucket(range_id: str) -> str:
    if range_id == "1w":
        return "1h"
    if range_id == "1m":
        return "1h"
    return "1d"


def qbit_gbt_rules(chain: str) -> list[str]:
    rules = ["segwit"]
    if "signet" in chain.strip().lower():
        rules.append("signet")
    return rules


def target_from_compact(bits_hex: str) -> int:
    return direct_stratum.target_from_compact_hex(bits_hex)


def scaled_network_difficulty(bits_hex: str) -> int:
    template_target = target_from_compact(bits_hex)
    if template_target <= 0:
        raise ValueError("compact bits target must be positive")
    if QBIT_POW_LIMIT_TARGET <= 0:
        raise ValueError("pow limit target must be positive")
    return max(1, (QBIT_POW_LIMIT_TARGET * QBIT_DIFFICULTY_SCALE) // template_target)


def utc_now_iso() -> str:
    return iso_datetime(datetime.now(timezone.utc))


def iso_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def decimal_string(value: int | str | Decimal) -> str:
    decimal = Decimal(str(value))
    if decimal == decimal.to_integral_value():
        return str(decimal.quantize(Decimal(1)))
    return format(decimal.normalize(), "f")


def nullable_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def optional_hex_hash(value: object) -> str | None:
    text = nullable_str(value)
    if text is None:
        return None
    text = text.lower()
    if len(text) == 64 and all(char in "0123456789abcdef" for char in text):
        return text
    return None


def fanout_transaction_hash(payload: dict[str, object], fanout_tx_hex: str) -> str:
    fanout_txid = optional_hex_hash(payload.get("fanout_txid"))
    if fanout_txid is not None:
        return fanout_txid
    derived_txid = txid_from_tx_hex(fanout_tx_hex)
    if derived_txid is not None:
        return derived_txid
    raise PublicApiError(500, "internal_error", "fanout transaction hash unavailable")


def txid_from_tx_hex(value: str) -> str | None:
    if not value:
        return None
    try:
        return direct_stratum.transaction_txid_display(value)
    except ValueError:
        return None


def first_int(*values: object, default: int) -> int:
    value = first_optional_int(*values)
    return value if value is not None else default


def first_optional_int(*values: object) -> int | None:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def first_optional_nonnegative_int(*values: object) -> int | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        try:
            parsed = int(text)
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            return parsed
    return None


def public_timestamp(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    if text.endswith("+00"):
        return text[:-3] + "Z"
    if text.endswith("+00:00"):
        return text[:-6] + "Z"
    return text


def shares_for_recipient(ledger: Any, recipient_id: str) -> list[object]:
    return [share for share in ledger.all_shares() if str(getattr(share, "miner_id", "")) == recipient_id]


def miner_share_summary(ledger: Any, recipient_id: str) -> dict[str, object]:
    read_model = getattr(ledger, "dashboard_miner_share_summary", None)
    if callable(read_model):
        return read_model(recipient_id=recipient_id)
    shares = shares_for_recipient(ledger, recipient_id)
    pool_3h_difficulty = difficulty_since(ledger.all_shares(), hours=3)
    miner_3h_difficulty = difficulty_since(shares, hours=3)
    return {
        "hashrate_ths": miner_hashrate_rollups(shares),
        "accepted_3h": share_count_since(shares, hours=3),
        "accepted_difficulty_3h": str(miner_3h_difficulty),
        "last_share_at": last_share_at(shares),
        "share_percent": percent_string(miner_3h_difficulty, pool_3h_difficulty),
    }


def miner_reward_window(ledger: Any, recipient_id: str, *, current_network_difficulty: object) -> dict[str, object]:
    read_model = getattr(ledger, "dashboard_miner_reward_window", None)
    if callable(read_model):
        return read_model(
            recipient_id=recipient_id,
            current_network_difficulty=current_network_difficulty,
        )
    return {
        "accepted_difficulty": "0",
        "pool_accepted_difficulty": "0",
        "share_percent": None,
    }


def _share_time(share: object) -> datetime:
    return datetime.fromtimestamp(int(getattr(share, "accepted_at_ms")) / 1000, timezone.utc)


def difficulty_since(shares: list[object], *, hours: int = 0, minutes: int = 0) -> int:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours, minutes=minutes)
    return sum(int(getattr(share, "share_difficulty")) for share in shares if cutoff <= _share_time(share) <= now)


def share_count_since(shares: list[object], *, hours: int = 0, minutes: int = 0) -> int:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours, minutes=minutes)
    return sum(1 for share in shares if cutoff <= _share_time(share) <= now)


def last_share_at(shares: list[object]) -> str | None:
    if not shares:
        return None
    return iso_datetime(max(_share_time(share) for share in shares))


def miner_hashrate_rollups(shares: list[object]) -> dict[str, str]:
    return {
        "m1": hashrate_ths_from_difficulty(difficulty_since(shares, minutes=1), 60),
        "m5": hashrate_ths_from_difficulty(difficulty_since(shares, minutes=5), 5 * 60),
        "m10": hashrate_ths_from_difficulty(difficulty_since(shares, minutes=10), 10 * 60),
        "h3": hashrate_ths_from_difficulty(difficulty_since(shares, hours=3), 3 * 60 * 60),
        "h24": hashrate_ths_from_difficulty(difficulty_since(shares, hours=24), 24 * 60 * 60),
    }


def percent_string(part: int | str | Decimal, total: int | str | Decimal) -> str | None:
    part_decimal = Decimal(str(part))
    total_decimal = Decimal(str(total))
    if total_decimal <= 0:
        return None
    return decimal_string(part_decimal * Decimal(100) / total_decimal)


def owed_balance_for_recipient(ledger: Any, recipient_id: str) -> int:
    return sum(
        int(balance.get("balance_sats", 0))
        for balance in ledger.current_owed_balances()
        if str(balance.get("recipient_id")) == recipient_id
    )


def payout_rows(coordinator: Any, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
    read_model = getattr(coordinator.ledger, "dashboard_miner_payout_rows", None)
    if callable(read_model):
        return read_model(recipient_id=recipient_id, page=page, limit=limit)
    history = coordinator.ledger.recipient_payout_history(recipient_id=recipient_id, limit=1_000)
    rows = [miner_payout_row(row) for row in history]
    offset = (page - 1) * limit
    return {"pagination": pagination(page, limit, len(rows)), "rows": rows[offset : offset + limit]}


def earnings_rows(coordinator: Any, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
    read_model = getattr(coordinator.ledger, "dashboard_miner_earning_rows", None)
    if callable(read_model):
        return read_model(recipient_id=recipient_id, page=page, limit=limit)
    history = coordinator.ledger.recipient_payout_history(recipient_id=recipient_id, limit=1_000)
    rows = [miner_earning_row(row) for row in history]
    offset = (page - 1) * limit
    return {"pagination": pagination(page, limit, len(rows)), "rows": rows[offset : offset + limit]}


def miner_worker_rows(
    ledger: Any,
    *,
    recipient_id: str,
    page: int,
    limit: int,
    search: str | None,
    hide_inactive: bool,
) -> dict[str, object]:
    read_model = getattr(ledger, "dashboard_miner_worker_rows", None)
    if callable(read_model):
        return read_model(
            recipient_id=recipient_id,
            page=page,
            limit=limit,
            search=search,
            hide_inactive=hide_inactive,
        )
    rows = worker_rows(ledger, recipient_id=recipient_id)
    active_count = sum(1 for row in rows if row["status"] == "active")
    if search:
        rows = [row for row in rows if search.lower() in str(row["worker_name"]).lower()]
    if hide_inactive:
        rows = [row for row in rows if row["status"] == "active"]
    offset = (page - 1) * limit
    return {
        "pagination": pagination(page, limit, len(rows)),
        "rows": rows[offset : offset + limit],
        "active_count": active_count,
    }


def miner_payout_row(row: dict[str, object]) -> dict[str, object]:
    onchain = str(row.get("action")) == "onchain"
    fanout_txid = nullable_str(row.get("fanout_txid")) if onchain else None
    coinbase_txid = nullable_str(row.get("coinbase_txid")) if onchain else None
    if fanout_txid:
        # The recipient's output is committed inside a CTV fanout rather than
        # paid directly from the coinbase: report the fanout transaction and
        # the committed output value (gross entitlement minus the fanout fee
        # share), which is what actually lands on-chain.
        transaction_kind = "ctv_fanout"
        txid = fanout_txid
        onchain_amount_sats = first_int(
            row.get("fanout_amount_sats"),
            row.get("onchain_amount_sats"),
            default=0,
        )
    elif coinbase_txid:
        transaction_kind = "coinbase"
        txid = coinbase_txid
        onchain_amount_sats = int(row.get("onchain_amount_sats", 0))
    else:
        transaction_kind = "carry_forward"
        txid = None
        onchain_amount_sats = int(row.get("onchain_amount_sats", 0))
    explorer_prefix = os.environ.get("PRISM_PUBLIC_EXPLORER_TX_URL_PREFIX")
    return {
        "block_height": int(row.get("block_height", 0)),
        "block_hash": str(row.get("block_hash") or ""),
        "created_at": required_public_timestamp(row.get("created_at")),
        "transaction_id": txid,
        "transaction_kind": transaction_kind,
        "onchain_amount_bits": onchain_amount_sats,
        "carry_forward_balance_bits": int(row.get("carry_forward_balance_sats", 0)),
        "action": str(row.get("action") or "accrued"),
        "maturity_state": str(row.get("maturity_state") or "immature"),
        "explorer_url": explorer_prefix.rstrip("/") + "/" + txid if explorer_prefix and txid else None,
    }


def lifetime_earnings_for_recipient(ledger: Any, recipient_id: str) -> int:
    read_model = getattr(ledger, "dashboard_miner_lifetime_earnings_bits", None)
    if callable(read_model):
        return int(read_model(recipient_id=recipient_id))
    history = ledger.recipient_payout_history(recipient_id=recipient_id, limit=1_000)
    return sum(gross_earning_bits_from_row(row) for row in history)


def pending_maturity_bits_for_recipient(ledger: Any, recipient_id: str) -> int:
    read_model = getattr(ledger, "dashboard_miner_pending_maturity_bits", None)
    if callable(read_model):
        return int(read_model(recipient_id=recipient_id))
    history = ledger.recipient_payout_history(recipient_id=recipient_id, limit=1_000)
    return sum(
        max(0, int(row.get("onchain_amount_sats", 0)) - int(row.get("settlement_fee_sats", 0)))
        for row in history
        if row.get("action") == "onchain" and row.get("maturity_state") == "immature"
    )


def miner_earning_row(row: dict[str, object]) -> dict[str, object]:
    gross = gross_earning_bits_from_row(row)
    settlement_fee = int(row.get("settlement_fee_sats", 0))
    net = max(0, gross - settlement_fee)
    block_hash = str(row.get("block_hash") or "")
    explorer_prefix = os.environ.get("PRISM_PUBLIC_EXPLORER_BLOCK_URL_PREFIX")
    return {
        "block_height": int(row.get("block_height", 0)),
        "block_hash": block_hash,
        "found_at": required_public_timestamp(row.get("found_at", row.get("created_at"))),
        "reward_share_percent": reward_share_percent_from_row(row, gross),
        "gross_earning_bits": gross,
        "settlement_fee_bits": settlement_fee,
        "net_earning_bits": net,
        "maturity_state": str(row.get("maturity_state") or "immature"),
        "settlement_artifacts_url": f"/public/v1/blocks/{block_hash}/settlement-artifacts" if block_hash else None,
        "explorer_url": explorer_prefix.rstrip("/") + "/" + block_hash if explorer_prefix and block_hash else None,
    }


def worker_rows(ledger: Any, *, recipient_id: str) -> list[dict[str, object]]:
    shares = shares_for_recipient(ledger, recipient_id)
    by_worker: dict[str, list[object]] = {}
    for share in shares:
        by_worker.setdefault(worker_name_from_share(share, recipient_id), []).append(share)
    rows = [
        {
            "worker_name": worker_name,
            "status": "active" if share_count_since(worker_shares, minutes=10) > 0 else "inactive",
            "last_share_at": last_share_at(worker_shares),
            "hashrate_ths_60s": hashrate_ths_from_difficulty(difficulty_since(worker_shares, minutes=1), 60),
            "hashrate_ths_3h": hashrate_ths_from_difficulty(difficulty_since(worker_shares, hours=3), 3 * 60 * 60),
        }
        for worker_name, worker_shares in by_worker.items()
    ]
    rows.sort(key=lambda row: (row["status"] != "active", str(row["worker_name"])))
    return rows


def worker_name_from_share(share: object, recipient_id: str) -> str:
    username = str(getattr(share, "share_id", "")).rsplit(":", 1)[0]
    if not username:
        return "default"
    if username == recipient_id:
        return "default"
    if username.startswith(recipient_id + "."):
        worker_name = username[len(recipient_id) + 1 :]
        return worker_name or "default"
    if "." in username:
        worker_name = username.split(".", 1)[1]
        return worker_name or "default"
    return "default"


def gross_earning_bits_from_row(row: dict[str, object]) -> int:
    fallback_gross = int(row.get("onchain_amount_sats", 0)) + int(row.get("carry_forward_balance_sats", 0))
    return int(row.get("gross_amount_sats", fallback_gross))


def reward_share_percent_from_row(row: dict[str, object], gross: int) -> str:
    raw_percent = nullable_str(row.get("reward_share_percent"))
    if raw_percent is not None:
        return decimal_string(raw_percent)
    block_gross = first_optional_int(
        row.get("block_gross_amount_sats"),
        row.get("total_gross_amount_sats"),
        row.get("block_total_gross_amount_sats"),
    )
    if block_gross is None:
        raise PublicApiError(500, "internal_error", "internal server error")
    return percent_string(gross, block_gross) or "0"


def required_public_timestamp(value: object) -> str:
    timestamp = public_timestamp(value)
    if timestamp is None:
        raise PublicApiError(500, "internal_error", "internal server error")
    return timestamp

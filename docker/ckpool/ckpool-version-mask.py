#!/usr/bin/env python3
"""Resolve the ckpool BIP310 version rolling mask for qbit."""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, request


HEX_MASK_RE = re.compile(r"^(?:0x)?[0-9a-fA-F]{1,8}$")
DYNAMIC_MODES = {"1", "true", "yes", "on", "auto", "dynamic", "advertised"}
STATIC_MODES = {"0", "false", "no", "off", "static", "configured", "manual"}
QBIT_VERSION_ROLLING_MASK = 0x1FFFE000
QBIT_VERSION_ROLLING_MASK_HEX = f"{QBIT_VERSION_ROLLING_MASK:08x}"
DEFAULT_PROBE_ATTEMPTS = 3
DEFAULT_PROBE_RETRY_SECONDS = 2.0
MAX_PROBE_ATTEMPTS = 10
MAX_CONFIGURED_PROBE_SECONDS = 60


class ProbeError(RuntimeError):
    """Raised when the live getblocktemplate probe did not produce a template.

    Dynamic mode fails closed on this: the configured fallback mask describes
    an operator intent, not what the node actually accepts, so a template that
    could not be retrieved must never be papered over with it.
    """


@dataclass(frozen=True)
class ResolveResult:
    selected_mask: str
    source: str
    detail: str
    advertised_mask: str | None = None


def normalize_mask(value: Any, *, field: str) -> str:
    if isinstance(value, int):
        if value < 0 or value > 0xFFFFFFFF:
            raise ValueError(f"{field} must fit uint32")
        return f"{value:08x}"

    text = str(value).strip()
    if not HEX_MASK_RE.fullmatch(text):
        raise ValueError(f"{field} must be 1 to 8 hex chars")

    if text.lower().startswith("0x"):
        text = text[2:]
    return f"{int(text, 16):08x}"


def mode_is_dynamic(mode: str) -> bool:
    normalized = mode.strip().lower()
    if normalized in DYNAMIC_MODES:
        return True
    if normalized in STATIC_MODES:
        return False
    raise ValueError(
        "CKPOOL_VERSION_MASK_MODE must be one of "
        f"{', '.join(sorted(DYNAMIC_MODES | STATIC_MODES))}"
    )


def chain_name() -> str:
    return os.environ.get("QBIT_CHAIN", "regtest").strip().lower() or "regtest"


def gbt_rules(chain: str) -> list[str]:
    rules = ["segwit"]
    if chain.strip().lower() == "signet":
        rules.append("signet")
    return rules


def configured_mask(fallback_mask: str) -> str:
    try:
        return normalize_mask(fallback_mask, field="CKPOOL_VERSION_MASK")
    except ValueError as exc:
        raise ValueError(f"invalid fallback CKPOOL_VERSION_MASK: {exc}") from exc


def select_version_mask(template: dict[str, Any], fallback_mask: str) -> ResolveResult:
    fallback = configured_mask(fallback_mask)

    if "versionrollingmask" not in template:
        # A template that a ready node produced without the field describes a
        # node that does not advertise one. That is a node-version
        # compatibility case, not a failed probe, so the configured mask still
        # applies.
        return ResolveResult(fallback, "fallback", "missing_versionrollingmask")

    advertised = template.get("versionrollingmask")
    try:
        selected = normalize_mask(advertised, field="versionrollingmask")
    except ValueError as exc:
        raise ValueError(f"invalid getblocktemplate.versionrollingmask: {exc}") from exc

    if selected == "00000000":
        # Zero is a valid answer: the node is telling the pool that version
        # rolling is disabled. Honour it rather than substituting a fallback.
        return ResolveResult(selected, "qbit_getblocktemplate", "disabled_by_zero_mask", str(advertised))

    return ResolveResult(selected, "qbit_getblocktemplate", "advertised", str(advertised))


def rpc_getblocktemplate(*, host: str, port: str, user: str, password: str, chain: str, timeout: float) -> dict[str, Any]:
    payload = json.dumps(
        {
            "jsonrpc": "1.0",
            "id": "ckpool-version-mask",
            "method": "getblocktemplate",
            "params": [{"rules": gbt_rules(chain)}],
        }
    ).encode("utf-8")
    credentials = f"{user}:{password}".encode("utf-8")
    req = request.Request(
        f"http://{host}:{port}",
        data=payload,
        headers={
            "Authorization": f"Basic {base64.b64encode(credentials).decode('ascii')}",
            "Content-Type": "application/json",
        },
    )
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = json.load(resp)
    except error.HTTPError as exc:
        if exc.code in (401, 403):
            exc.close()
            raise ValueError(f"RPC authentication refused (HTTP {exc.code}); check RPC credentials") from exc
        # qbitd reports JSON-RPC errors with an HTTP 500 and the reason in the
        # body. Surface that reason so a fail-closed startup says why.
        try:
            body = json.load(exc)
        except (OSError, ValueError):
            raise RuntimeError(f"HTTP {exc.code}: {exc.reason}") from exc
        finally:
            exc.close()
        if isinstance(body, dict) and body.get("error"):
            raise RuntimeError(body["error"]) from exc
        raise RuntimeError(f"HTTP {exc.code}: {exc.reason}") from exc

    if not isinstance(body, dict):
        raise RuntimeError("getblocktemplate response was not an object")
    if body.get("error"):
        raise RuntimeError(body["error"])
    result = body.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("getblocktemplate result was not an object")
    return result


def probe_settings() -> tuple[float, int, float]:
    def number(name: str, default: int | float, parse: Any) -> Any:
        raw = os.environ.get(name, "") or str(default)
        try:
            return parse(raw)
        except ValueError as exc:
            raise ValueError(f"{name} has an invalid numeric value: {raw!r}") from exc

    timeout = number("CKPOOL_VERSION_MASK_RPC_TIMEOUT_SECONDS", 5, float)
    attempts = number("CKPOOL_VERSION_MASK_PROBE_ATTEMPTS", DEFAULT_PROBE_ATTEMPTS, int)
    retry_seconds = number("CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS", DEFAULT_PROBE_RETRY_SECONDS, float)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("CKPOOL_VERSION_MASK_RPC_TIMEOUT_SECONDS must be finite and positive")
    if not 1 <= attempts <= MAX_PROBE_ATTEMPTS:
        raise ValueError(f"CKPOOL_VERSION_MASK_PROBE_ATTEMPTS must be between 1 and {MAX_PROBE_ATTEMPTS}")
    if not math.isfinite(retry_seconds) or retry_seconds < 0:
        raise ValueError("CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS must be finite and nonnegative")
    if attempts * timeout + (attempts - 1) * retry_seconds > MAX_CONFIGURED_PROBE_SECONDS:
        raise ValueError(f"configured version-mask probe timeouts and delays must total at most {MAX_CONFIGURED_PROBE_SECONDS} seconds")

    return timeout, attempts, retry_seconds


def probe_template(*, sleep: Any = time.sleep) -> dict[str, Any]:
    """Fetch a live template, retrying a bounded number of transient failures.

    Missing RPC credentials are a configuration fault and are not retried.
    """
    timeout, attempts, retry_seconds = probe_settings()
    try:
        user = os.environ["QBIT_RPC_USER"]
        password = os.environ["QBIT_RPC_PASSWORD"]
    except KeyError as exc:
        raise ValueError(f"{exc.args[0]} is required to resolve the version mask") from exc

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return rpc_getblocktemplate(
                host=os.environ.get("QBIT_RPC_HOST", "qbitd"),
                port=os.environ.get("QBIT_RPC_PORT", "18452"),
                user=user,
                password=password,
                chain=chain_name(),
                timeout=timeout,
            )
        except (OSError, RuntimeError, json.JSONDecodeError, error.URLError, http.client.HTTPException) as exc:
            last_error = exc
            if attempt >= attempts:
                break
            print(
                "ckpool version mask: getblocktemplate probe failed "
                f"(attempt {attempt}/{attempts}): {exc}",
                file=sys.stderr,
            )
            if retry_seconds:
                sleep(retry_seconds)

    raise ProbeError(
        f"getblocktemplate probe failed after {attempts} attempt(s): {last_error}. "
        "Dynamic mode requires a live template. If templates are intentionally "
        "unavailable during prelaunch, explicitly set CKPOOL_VERSION_MASK_MODE=static."
    )


def validate_config() -> str:
    """Validate mask configuration without contacting the node.

    Lets startup reject a malformed mode or configured mask before it spends
    the readiness wait, without resolving a mask that early.
    """
    fallback = os.environ.get("CKPOOL_VERSION_MASK", QBIT_VERSION_ROLLING_MASK_HEX)
    mode = os.environ.get("CKPOOL_VERSION_MASK_MODE", "dynamic")
    dynamic = mode_is_dynamic(mode)
    configured = configured_mask(fallback)
    if dynamic:
        probe_settings()
    return f"mode={'dynamic' if dynamic else 'static'} configured={configured}"


def resolve_from_env(*, sleep: Any = time.sleep) -> ResolveResult:
    validate_config()
    fallback = os.environ.get("CKPOOL_VERSION_MASK", QBIT_VERSION_ROLLING_MASK_HEX)
    mode = os.environ.get("CKPOOL_VERSION_MASK_MODE", "dynamic")
    if not mode_is_dynamic(mode):
        return ResolveResult(configured_mask(fallback), "fallback", "static_mode")

    template = probe_template(sleep=sleep)
    return select_version_mask(template, fallback)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Resolve the ckpool BIP310 version rolling mask for qbit."
    )
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help=(
            "validate mask mode, configured mask and dynamic probe settings, "
            "without contacting qbit; prints nothing on stdout"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.validate_config:
        try:
            summary = validate_config()
        except ValueError as exc:
            print(f"ckpool version mask: error={exc}", file=sys.stderr)
            return 1
        print(f"ckpool version mask: config ok {summary}", file=sys.stderr)
        return 0

    try:
        result = resolve_from_env()
    except (ValueError, ProbeError) as exc:
        print(f"ckpool version mask: error={exc}", file=sys.stderr)
        return 1

    advertised = result.advertised_mask if result.advertised_mask is not None else "-"
    print(
        "ckpool version mask: "
        f"selected={result.selected_mask} source={result.source} "
        f"detail={result.detail} advertised={advertised}",
        file=sys.stderr,
    )
    print(result.selected_mask)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Resolve the ckpool BIP310 version rolling mask for qbit."""

from __future__ import annotations

import base64
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any
from urllib import error, request


HEX_MASK_RE = re.compile(r"^(?:0x)?[0-9a-fA-F]{1,8}$")
DYNAMIC_MODES = {"1", "true", "yes", "on", "auto", "dynamic", "advertised"}
STATIC_MODES = {"0", "false", "no", "off", "static", "configured", "manual"}
QBIT_VERSION_ROLLING_MASK = 0x1FFFE000
QBIT_VERSION_ROLLING_MASK_HEX = f"{QBIT_VERSION_ROLLING_MASK:08x}"


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


def gbt_rules(chain: str) -> list[str]:
    rules = ["segwit"]
    if chain.strip().lower() == "signet":
        rules.append("signet")
    return rules


def select_version_mask(template: dict[str, Any], fallback_mask: str) -> ResolveResult:
    try:
        fallback = normalize_mask(fallback_mask, field="CKPOOL_VERSION_MASK")
    except ValueError as exc:
        raise ValueError(f"invalid fallback CKPOOL_VERSION_MASK: {exc}") from exc

    if "versionrollingmask" not in template:
        return ResolveResult(fallback, "fallback", "missing_versionrollingmask")

    advertised = template.get("versionrollingmask")
    try:
        selected = normalize_mask(advertised, field="versionrollingmask")
    except ValueError as exc:
        raise ValueError(f"invalid getblocktemplate.versionrollingmask: {exc}") from exc

    if selected == "00000000":
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
    with request.urlopen(req, timeout=timeout) as resp:
        body = json.load(resp)

    if body.get("error"):
        raise RuntimeError(body["error"])
    result = body.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("getblocktemplate result was not an object")
    return result


def resolve_from_env() -> ResolveResult:
    fallback = os.environ.get("CKPOOL_VERSION_MASK", QBIT_VERSION_ROLLING_MASK_HEX)
    mode = os.environ.get("CKPOOL_VERSION_MASK_MODE", "dynamic")
    if not mode_is_dynamic(mode):
        selected = normalize_mask(fallback, field="CKPOOL_VERSION_MASK")
        return ResolveResult(selected, "fallback", "static_mode")

    try:
        timeout = float(os.environ.get("CKPOOL_VERSION_MASK_RPC_TIMEOUT_SECONDS", "5"))
    except ValueError:
        timeout = 5.0

    try:
        template = rpc_getblocktemplate(
            host=os.environ.get("QBIT_RPC_HOST", "qbitd"),
            port=os.environ.get("QBIT_RPC_PORT", "18452"),
            user=os.environ["QBIT_RPC_USER"],
            password=os.environ["QBIT_RPC_PASSWORD"],
            chain=os.environ.get("QBIT_CHAIN", "regtest"),
            timeout=timeout,
        )
    except (KeyError, OSError, RuntimeError, json.JSONDecodeError, error.URLError) as exc:
        selected = normalize_mask(fallback, field="CKPOOL_VERSION_MASK")
        return ResolveResult(selected, "fallback", f"probe_error:{exc}")

    return select_version_mask(template, fallback)


def main() -> int:
    try:
        result = resolve_from_env()
    except ValueError as exc:
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

#!/usr/bin/env python3
"""Deterministic stand-in for qbit-prism-build-audit-bundle in daemon tests.

Invoked through a patched prism_tool_command: with --serve it speaks the
JSONL daemon protocol (echoing enough request state for assertions), and
without it it echoes the one-shot stdin payload, so the same command list
serves both the daemon path and its transparent one-shot fallback.
FAKE_SERVE_BUILDER_MODE selects daemon misbehavior for anomaly tests.
"""

import json
import os
import sys


def one_shot() -> None:
    payload = json.load(sys.stdin)
    json.dump({"received": payload, "transport": "one-shot"}, sys.stdout)
    sys.stdout.flush()


def serve() -> None:
    mode = os.environ.get("FAKE_SERVE_BUILDER_MODE", "ok")
    protocol = 99 if mode == "protocol-mismatch" else 1
    sys.stdout.write(
        json.dumps(
            {
                "event": "handshake",
                "tool": "qbit-prism-build-audit-bundle",
                "protocol": protocol,
            }
        )
        + "\n"
    )
    sys.stdout.flush()
    if mode == "crash-before-response":
        sys.stdin.readline()
        sys.exit(1)
    if mode == "hang-after-request":
        sys.stdin.readline()
        # Block until the coordinator kills the daemon (supersession) or
        # closes stdin; never respond.
        sys.stdin.read()
        sys.exit(0)
    cache: dict[str, dict[str, object]] = {}
    hits = 0
    misses = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        request = json.loads(line)
        key = request["window_key"]["share_snapshot_sha256"]
        had_window = bool(request.get("compact_shares"))
        if had_window:
            cache[key] = {
                "compact_share_identities": request.get(
                    "compact_share_identities", []
                ),
                "compact_shares": request["compact_shares"],
            }
            while len(cache) > 2:
                cache.pop(next(iter(cache)))
            misses += 1
        elif key in cache:
            hits += 1
        else:
            sys.stdout.write(
                json.dumps(
                    {
                        "ok": False,
                        "error": f"share window {key} is not cached",
                        "needs_window": True,
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
            continue
        response = {
            "ok": True,
            "summary": {
                "found_block": request["found_block"],
                "signed_coinbase_manifest": {"manifest": {}},
                "payout_policy_manifest": {"accounts": []},
                "window": cache[key],
                "request_had_window": had_window,
                "transport": "serve",
            },
            "metrics": {
                "input_deserialization_seconds": 0.001,
                "phases_seconds": {"payout_state_derivation": 0.002},
                "output_serialization_seconds": 0.003,
            },
            "window_cache": {
                "hit": not had_window,
                "hits": hits,
                "misses": misses,
                "entries": len(cache),
            },
        }
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    if "--serve" in sys.argv[1:]:
        serve()
    else:
        one_shot()

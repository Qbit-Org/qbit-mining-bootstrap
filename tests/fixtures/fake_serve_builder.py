#!/usr/bin/env python3
"""Deterministic stand-in for qbit-prism-build-audit-bundle in daemon tests.

Invoked through a patched prism_tool_command: with --serve it speaks the
JSONL daemon protocol (echoing enough request state for assertions), and
without it it echoes the one-shot stdin payload, so the same command list
serves both the daemon path and its transparent one-shot fallback.
FAKE_SERVE_BUILDER_MODE selects daemon misbehavior for anomaly tests.

prepare_window requests are served by the real Python fold
(``IncrementalShareWindow``), so mirror digests verify exactly and the
window-pipeline tests exercise the coordinator against byte-faithful daemon
behavior without a Rust build; the Rust implementation itself is proven by
the parity oracle's ``rust-daemon`` adapter. Additional modes fault-inject
the prepare protocol: ``prepare-forget-windows`` answers every advance with
``needs_full``, ``prepare-fallback`` answers every advance with ``fallback``,
``prepare-error`` answers prepare requests with a generic error, and
``crash-during-prepare`` dies on the first prepare request.
"""

import json
import os
import sys
from pathlib import Path


def one_shot() -> None:
    payload = json.load(sys.stdin)
    json.dump({"received": payload, "transport": "one-shot"}, sys.stdout)
    sys.stdout.flush()


def _fold_windows():
    """Import the shipped fold lazily; the repo root is not on script paths."""
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from lab.prism.share_ledger import (  # noqa: PLC0415 - script-local import
        AcceptedShareRecord,
        IncrementalShareWindow,
        IncrementalWindowFallback,
    )

    return AcceptedShareRecord, IncrementalShareWindow, IncrementalWindowFallback


def _write_response(payload: dict, raw_section: bytes | None = None) -> None:
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()
    if raw_section is not None:
        sys.stdout.buffer.write(raw_section + b"\n")
        sys.stdout.buffer.flush()


def _canonical_items(window) -> bytes:
    return b",".join(
        page.canonical_json_items
        for page in window.pages
        if page.canonical_json_items
    )


def _handle_prepare_window(request: dict, prepared: dict, mode: str) -> None:
    if mode == "crash-during-prepare":
        sys.exit(1)
    if mode == "prepare-error":
        _write_response(
            {
                "ok": False,
                "request": "prepare_window",
                "error": "injected prepare failure",
            }
        )
        return
    record_type, window_type, fallback_type = _fold_windows()
    records = [
        record_type(
            **{
                key: value
                for key, value in record_json.items()
                if value is not None or key == "credit_policy"
            }
        )
        for record_json in request.get("records", [])
    ]
    epoch = int(request.get("append_invalidation_epoch", 0))
    anchor = int(request["anchor_job_issued_at_ms"])
    prepare_mode = request.get("mode")
    if prepare_mode == "full":
        window = window_type.from_full_snapshot(
            records,
            anchor_job_issued_at_ms=anchor,
            window_weight=int(request["window_weight"]),
            page_size=int(request.get("page_size", 512)),
        )
        digest = window.json_records().canonical_json_sha256()
        items = _canonical_items(window)
        prepared.clear()
        prepared[digest] = (window, epoch)
        _write_response(
            {
                "ok": True,
                "request": "prepare_window",
                "share_snapshot_sha256": digest,
                "record_count": window.record_count,
                "added_rows": 0,
                "expired_rows": 0,
                "touched_pages": 0,
                "window_items_len": len(items),
            },
            items,
        )
        return
    if prepare_mode != "advance":
        _write_response(
            {
                "ok": False,
                "request": "prepare_window",
                "error": f"unsupported prepare_window mode: {prepare_mode}",
            }
        )
        return
    base_digest = str(request.get("base_digest", ""))
    held = prepared.get(base_digest)
    if (
        mode == "prepare-forget-windows"
        or held is None
        or held[1] != epoch
    ):
        _write_response(
            {
                "ok": False,
                "request": "prepare_window",
                "needs_full": True,
                "error": f"prepared window {base_digest} is not held",
            }
        )
        return
    if mode == "prepare-fallback":
        _write_response(
            {
                "ok": False,
                "request": "prepare_window",
                "fallback": True,
                "error": "injected advance invariant failure",
            }
        )
        return
    base_window = held[0]
    old_items = _canonical_items(base_window)
    try:
        advanced, stats = base_window.advance(
            records,
            anchor_job_issued_at_ms=anchor,
        )
    except fallback_type as error:
        _write_response(
            {
                "ok": False,
                "request": "prepare_window",
                "fallback": True,
                "error": str(error),
            }
        )
        return
    digest = advanced.json_records().canonical_json_sha256()
    new_items = _canonical_items(advanced)
    # Byte surgery from the old stream to the new: expiry only drops a
    # prefix and appends only extend the tail, so the retained old suffix is
    # a prefix of the new stream. Scanning for the smallest such drop keeps
    # the fake honest against the fold it just ran.
    drop = 0
    while drop <= len(old_items):
        retained = old_items[drop:]
        if new_items[: len(retained)] == retained:
            break
        drop += 1
    appended = new_items[len(old_items) - drop :]
    prepared.clear()
    prepared[digest] = (advanced, epoch)
    _write_response(
        {
            "ok": True,
            "request": "prepare_window",
            "share_snapshot_sha256": digest,
            "record_count": advanced.record_count,
            "added_rows": stats.added_rows,
            "expired_rows": stats.expired_rows,
            "touched_pages": stats.touched_pages,
            "retained_drop_bytes": drop,
            "appended_items_len": len(appended),
        },
        appended,
    )


def serve() -> None:
    mode = os.environ.get("FAKE_SERVE_BUILDER_MODE", "ok")
    protocol = int(os.environ.get("FAKE_SERVE_BUILDER_PROTOCOL", "2"))
    if mode == "protocol-mismatch":
        protocol = 99
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
    prepared: dict[str, tuple[object, int]] = {}
    hits = 0
    misses = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        request = json.loads(line)
        if request.get("request") == "prepare_window":
            _handle_prepare_window(request, prepared, mode)
            continue
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
            # Promote like the real daemon: a hit entry moves to
            # most-recent position before any later eviction.
            cache[key] = cache.pop(key)
        elif key in prepared:
            # The unified cache: a prepared window serves builds directly.
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
                "window": cache.get(key, {"prepared": True}),
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
                "entries": len(cache) + len(prepared),
            },
        }
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    if "--serve" in sys.argv[1:]:
        serve()
    else:
        one_shot()

#!/usr/bin/env python3
"""Prove legacy audit bodies whose share-segment range digests use Python's
escaped JSON, and write the canonical sidecars `import-audits` reads first.

Background (#709): 1.x and 2.x hashed each `segment_range` and
`segment_prefix` part as sha256 of
`json.dumps({schema, first_share_seq, last_share_seq, share_count, shares},
separators=(",", ":"))`, with the default ensure_ascii=True, so non-ASCII
text is written as \\uXXXX escapes. 3.x re-serializes the same shares with
serde_json, which writes raw UTF-8. A range whose shares carry any non-ASCII
string (a worker name, say) therefore fails 3.x's range check, and
`import-audits` refuses the body. The content itself is intact.

For each row (`block_hash,body_uri,audit_bundle_sha256`) the tool:
1. checks an existing sidecar, if `--sidecar-dir` holds one: it must be one
   gzip member whose bytes hash to the digest, because import reads it before
   the body and stops on a bad one;
2. runs the 3.x canonicalizer on the body as it is: a body that already
   verifies needs nothing;
3. otherwise rewrites the failing range parts of a body-ref or v2 body as
   inline parts holding the slot's shares, and runs the canonicalizer on the
   rewritten body-ref. The canonicalizer itself requires
   sha256(canonical bundle) == audit_bundle_sha256, so a zero exit proves the
   content is exactly what was committed;
4. with `--write`, gzips the proven canonical bytes (level 9, mtime 0) to
   `<sidecar-dir>/prism-audit-bundle-canonical-<block>-<digest>.json.gz`. An
   existing file is never replaced. `import-audits` re-verifies the sidecar
   against the digest, the coinbase and the ledger key.

Without `--write` it only classifies. The exit status is 0 only when every row
is `ok-in-3x`, `sidecar-ok`, `proven` or, with `--write`, `sidecar-written`;
the per-row report says what stopped the others.

Usage:
  prism_legacy_range_sidecars.py --rows rows.csv \\
      --audit-root /var/lib/qbit-mining-pool/prism/audit \\
      --canonicalizer target/release/qbit-prism-audit-canonicalize \\
      --sidecar-dir <the directory passed to import-audits --root> [--write]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import zlib
from pathlib import Path

SEGMENT_SCHEMA = "qbit.prism.audit-share-segment.v1"
BODY_REF_SCHEMA = "qbit.prism.audit-body-ref.v1"
V2_SCHEMA = "qbit.prism.audit-bundle.v2"
# 3.x AcceptedShare field order (crates/qbit-prism/src/lib.rs); credit_policy
# is serialized only when set.
SHARE_FIELDS = (
    "share_seq", "share_id", "miner_id", "order_key", "p2mr_program_hex",
    "share_difficulty", "network_difficulty", "template_height", "job_id",
    "job_issued_at_ms", "accepted_at_ms", "ntime",
)
DIGEST_KEY = {"segment_range": "range_sha256", "segment_prefix": "prefix_sha256"}
CSV_COLUMNS = {"block_hash", "body_uri", "audit_bundle_sha256"}
HEX64 = re.compile(r"[0-9a-f]{64}")
OK_STATUSES = {"ok-in-3x", "sidecar-ok", "proven", "sidecar-written"}


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def compact(value: object, *, ascii_only: bool) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=ascii_only).encode("utf-8")


def accepted_share(share: dict) -> dict:
    out = {key: share[key] for key in SHARE_FIELDS if key in share}
    if share.get("credit_policy") is not None:
        out["credit_policy"] = share["credit_policy"]
    return out


def segment_payload(first: int, last: int, shares: list) -> dict:
    return {
        "schema": SEGMENT_SCHEMA,
        "first_share_seq": first,
        "last_share_seq": last,
        "share_count": len(shares),
        "shares": shares,
    }


def map_uri(uri: str, prefix: str, root: Path, base: Path) -> Path:
    path = uri[len("file://"):] if uri.startswith("file://") else uri
    if prefix and (path == prefix or path.startswith(prefix.rstrip("/") + "/")):
        return root / path[len(prefix):].lstrip("/")
    candidate = Path(path)
    return candidate if candidate.is_absolute() else base / candidate


def sidecar_path(sidecar_dir: Path, block: str, digest: str) -> Path:
    return sidecar_dir / f"prism-audit-bundle-canonical-{block}-{digest}.json.gz"


def canonicalize(canonicalizer: str, body: Path) -> tuple[bytes | None, str]:
    run = subprocess.run([canonicalizer, "--input", str(body)], capture_output=True)
    if run.returncode != 0:
        return None, run.stderr.decode("utf-8", "replace").strip()[-400:]
    return run.stdout, ""


def probe_part(part: dict, prefix: str, root: Path, base: Path) -> dict:
    """Classify one share part by which serialization reproduces its digest."""
    kind = part.get("kind")
    probe = {"kind": kind, "first": part.get("first_share_seq"), "last": part.get("last_share_seq")}
    if kind not in DIGEST_KEY:
        probe["status"] = "not-a-range-part"
        return probe
    slot = map_uri(part.get("body_uri") or "", prefix, root, base)
    probe["slot"] = str(slot)
    if not slot.is_file():
        probe["status"] = "slot-missing"
        return probe
    first, last = int(part["first_share_seq"]), int(part["last_share_seq"])
    try:
        segment = json.loads(slot.read_bytes())
        selected = sorted(
            (share for share in segment["shares"] if first <= int(share["share_seq"]) <= last),
            key=lambda share: int(share["share_seq"]),
        )
    except (OSError, KeyError, TypeError, ValueError) as exc:
        probe["status"] = "slot-unreadable"
        probe["error"] = f"{type(exc).__name__}: {exc}"[:200]
        return probe
    if len(selected) != int(part["share_count"]):
        probe["status"] = f"slot-has-{len(selected)}-of-{part['share_count']}-shares"
        return probe
    shares = [accepted_share(share) for share in selected]
    expected = str(part.get(DIGEST_KEY[kind]) or "").lower()
    payload = segment_payload(first, last, shares)
    if sha256_hex(compact(payload, ascii_only=False)) == expected:
        probe["status"] = "ok-in-3x"
    elif sha256_hex(compact(payload, ascii_only=True)) == expected:
        probe["status"] = "python-escaped"
    else:
        probe["status"] = "unexplained"
    probe["non_ascii"] = [
        {"share_seq": share.get("share_seq"), "field": key, "text": value}
        for share in shares for key, value in share.items()
        if isinstance(value, str) and not value.isascii()
    ][:20]
    probe["shares"] = shares
    return probe


def rewrite(body: dict, digest: str, parts: list, probes: list, prefix: str, root: Path,
            base: Path) -> dict:
    """A body-ref copy with every failing range part inlined from its slot.

    A v2 body's own proof checks (its parts digest and reward manifest) are not
    rerun on the copy; the bundle digest commits to the whole bundle, the
    reward manifest included, so matching it proves the content as well."""
    new_parts = []
    for part, probe in zip(parts, probes):
        if probe["status"] in ("python-escaped", "unexplained"):
            new_parts.append({
                "kind": "inline",
                "first_share_seq": part["first_share_seq"],
                "last_share_seq": part["last_share_seq"],
                "share_count": part["share_count"],
                "shares": probe["shares"],
            })
        else:
            kept = dict(part)
            if "body_uri" in kept:
                kept["body_uri"] = str(map_uri(kept["body_uri"], prefix, root, base))
            new_parts.append(kept)
    rewritten = {
        "schema": BODY_REF_SCHEMA,
        "audit_bundle_sha256": digest,
        "share_count": body["share_count"],
        "bundle_without_shares": body["bundle_without_shares"],
        "share_parts": new_parts,
    }
    if "shares_key_index" in body:
        rewritten["shares_key_index"] = body["shares_key_index"]
    return rewritten


def write_sidecar(target: Path, canonical: bytes) -> str:
    """Write the gzip sidecar without ever replacing an existing file."""
    sink = io.BytesIO()
    with gzip.GzipFile(fileobj=sink, mode="wb", compresslevel=9, mtime=0) as handle:
        handle.write(canonical)
    payload = sink.getvalue()
    temp = target.with_name(target.name + f".tmp-{os.getpid()}")
    with open(temp, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        # A hard link publishes the complete file atomically and refuses an
        # existing target.
        os.link(temp, target)
    except FileExistsError:
        return "sidecar-conflict"
    except OSError:
        # No hard links on this filesystem: exclusive create still refuses
        # an existing target.
        try:
            with open(target, "xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            return "sidecar-conflict"
    finally:
        temp.unlink()
    return "sidecar-written"


def existing_sidecar_status(target: Path, digest: str) -> str:
    """`sidecar-ok` only for one complete gzip member, the form this tool
    writes, whose bytes hash to the digest. Import reads the first member and
    requires that hash, from a file that resolves inside its --root."""
    if not target.resolve().is_relative_to(target.parent.resolve()):
        return "sidecar-bad"
    try:
        inflater = zlib.decompressobj(wbits=31)
        payload = inflater.decompress(target.read_bytes())
    except (OSError, zlib.error):
        return "sidecar-bad"
    if not inflater.eof or inflater.unused_data:
        return "sidecar-bad"
    return "sidecar-ok" if sha256_hex(payload) == digest else "sidecar-bad"


def share_parts(body: dict) -> list | None:
    if body.get("schema") == BODY_REF_SCHEMA:
        return body["share_parts"]
    if body.get("schema") == V2_SCHEMA:
        return body["share_window_proof"]["share_parts"]
    return None


def process(row: dict, args: argparse.Namespace) -> dict:
    # The sidecar name keeps the block hash as stored: import looks it up
    # verbatim. Its digest check compares lowercase hex.
    block = (row.get("block_hash") or "").strip()
    digest = (row.get("audit_bundle_sha256") or "").strip()
    result = {"block_hash": block, "audit_bundle_sha256": digest}
    if not (HEX64.fullmatch(block.lower()) and HEX64.fullmatch(digest)):
        result["status"] = "bad-row"
        return result
    target = sidecar_path(args.sidecar_dir, block, digest) if args.sidecar_dir else None
    if target is not None and os.path.lexists(target):
        result["sidecar"] = str(target)
        result["status"] = existing_sidecar_status(target, digest)
        return result
    # Import loads the body from its canonical path, so relative slot URIs
    # resolve against the real file's directory.
    body_uri = (row.get("body_uri") or "").strip()
    body_path = map_uri(body_uri, args.uri_prefix, args.audit_root, args.audit_root).resolve()
    result["body"] = str(body_path)
    if not body_uri or not body_path.is_file():
        result["status"] = "body-missing"
        return result
    canonical, error = canonicalize(args.canonicalizer, body_path)
    if canonical is not None and sha256_hex(canonical) == digest:
        result["status"] = "ok-in-3x"
        return result
    result["original_error"] = error or "canonical bytes differ from the digest"
    base = body_path.parent
    try:
        body = json.loads(body_path.read_bytes())
        parts = share_parts(body)
        if parts is None:
            result["status"] = "not-a-parted-body"
            result["schema"] = body.get("schema")
            return result
        if str(body.get("audit_bundle_sha256", "")).lower() != digest:
            result["status"] = "body-digest-differs"
            return result
        probes = [probe_part(part, args.uri_prefix, args.audit_root, base) for part in parts]
        result["parts"] = [{k: v for k, v in probe.items() if k != "shares"} for probe in probes]
        if any(probe["status"].startswith("slot-") for probe in probes):
            result["status"] = "slot-incomplete"
            return result
        # Failing range parts are inlined; the others keep their slots, at the
        # mapped paths. The canonicalizer's digest check is the proof either way.
        rewritten = rewrite(body, digest, parts, probes, args.uri_prefix, args.audit_root, base)
    except (OSError, AttributeError, KeyError, TypeError, ValueError) as exc:
        result["status"] = "body-unreadable"
        result["error"] = f"{type(exc).__name__}: {exc}"[:200]
        return result
    args.work_dir.mkdir(parents=True, exist_ok=True)
    rewritten_path = args.work_dir / f"rewritten-{block}.json"
    rewritten_path.write_bytes(compact(rewritten, ascii_only=False))
    canonical, error = canonicalize(args.canonicalizer, rewritten_path)
    if canonical is None:
        result["status"] = "canonicalizer-refused"
        result["error"] = error
        return result
    if sha256_hex(canonical) != digest:
        result["status"] = "digest-mismatch"
        return result
    if not args.write:
        result["status"] = "proven"
        return result
    result["sidecar"] = str(target)
    result["status"] = write_sidecar(target, canonical)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--rows", required=True, type=Path,
                        help="CSV with header block_hash,body_uri,audit_bundle_sha256")
    parser.add_argument("--audit-root", required=True, type=Path,
                        help="where the audit tree is mounted; relative body URIs resolve here")
    parser.add_argument("--uri-prefix", default="/var/lib/qbit-mining-pool/prism/audit",
                        help="body_uri prefix that --audit-root replaces")
    parser.add_argument("--canonicalizer", required=True,
                        help="qbit-prism-audit-canonicalize built at the release pin")
    parser.add_argument("--sidecar-dir", type=Path,
                        help="the import-audits --root directory; required with --write")
    parser.add_argument("--work-dir", type=Path, default=Path("legacy-range-sidecars-work"),
                        help="where rewritten body-refs are kept for review")
    parser.add_argument("--report", type=Path, default=Path("legacy-range-sidecars.jsonl"))
    parser.add_argument("--write", action="store_true", help="write proven sidecars")
    args = parser.parse_args(argv)
    if args.write and args.sidecar_dir is None:
        parser.error("--write needs --sidecar-dir")
    if args.sidecar_dir is not None and not args.sidecar_dir.is_dir():
        parser.error(f"--sidecar-dir {args.sidecar_dir} is not a directory")
    if shutil.which(args.canonicalizer) is None:
        parser.error(f"--canonicalizer {args.canonicalizer} is not an executable")
    # The canonicalizer resolves a rewritten body-ref's relative slot paths
    # against the work directory, so every kept path is made absolute.
    args.audit_root = args.audit_root.absolute()
    args.work_dir = args.work_dir.absolute()
    if args.sidecar_dir is not None:
        args.sidecar_dir = args.sidecar_dir.absolute()
    totals: dict[str, int] = {}
    with open(args.rows, newline="", encoding="utf-8") as rows:
        reader = csv.DictReader(rows)
        missing = CSV_COLUMNS - set(reader.fieldnames or ())
        if missing:
            parser.error(f"--rows lacks the column(s) {', '.join(sorted(missing))}")
        with open(args.report, "w", encoding="utf-8") as report:
            for row in reader:
                result = process(row, args)
                totals[result["status"]] = totals.get(result["status"], 0) + 1
                report.write(json.dumps(result, ensure_ascii=False) + "\n")
                if result["status"] in ("sidecar-conflict", "sidecar-bad"):
                    print(f"{result['status']}: {result['sidecar']}", file=sys.stderr)
    print(json.dumps({"totals": totals, "report": str(args.report)}, sort_keys=True))
    return 0 if set(totals) <= OK_STATUSES else 3


if __name__ == "__main__":
    sys.exit(main())

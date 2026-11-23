#!/usr/bin/env python3
"""List the legacy audit rows `import-audits` refuses with "window proof
share_parts_digest_hex mismatch", without reading any share slot.

2.x writes an audit-bundle.v2 body's window-proof `share_parts_digest_hex` as
sha256 of `json.dumps({"share_parts": parts}, separators=(",", ":"))`: each
part's keys in insertion order (`kind` first), non-ASCII escaped. A 3.x
without #731's fix (#809), 3.1.0 and earlier, recomputes it from the parsed
parts through `serde_json::json!`, and serde_json without `preserve_order`
sorts every object's keys, so it refuses every 2.x v2 body whose parts digest
covers a part (crates/qbit-prism/src/audit_body_ref.rs). With the fix, 3.1.1
and later, 3.x recomputes 2.x's form, over the fields it parses, and refuses
any other, the sorted-key one included. `import-audits` reads a row's
canonical sidecar before its body, so only a v2 body without one in the
import root reaches that check; prism_legacy_range_sidecars.py writes the
missing sidecars.

By default the statuses below are those of a 3.x without the fix, the
cutover's release. On a release with the fix, that listing holds rows import
no longer refuses (the sidecar tool reports such a row `ok-in-3x` and writes
nothing for it), so the run exits 3 for bodies import accepts. Pass
`--with-731-fix` to model the fixed import instead: a body whose digest is
2.x's form over the parts as 3.x parses them (each part's known fields, and
an inline part's shares as 3.x's AcceptedShare, in declaration order) is
`ok-in-3x`, and one of any other form `matches-neither`; `2x-order-only` does
not occur. Either mode checks the parts digest only, not that 3.x can parse
each field: a missing or mistyped field can pass here and be refused by import.

For each row (`block_hash,body_uri,audit_bundle_sha256`) the status is:
  has-sidecar      the import root holds the row's canonical sidecar, which
                   import reads instead of the body (its bytes are verified by
                   prism_legacy_range_sidecars.py and by import, not here)
  2x-order-only    the v2 digest is 2.x's: import refuses the body (without
                   --with-731-fix only)
  matches-neither  the v2 digest is neither form (with --with-731-fix: not
                   the form 3.x recomputes): import refuses the body
  ok-in-3x         the v2 digest is the form 3.x recomputes
  no-parts-digest  a v2 body without the digest, which 3.x does not check
  no-window-proof  not a v2 body (a body-ref or a plain bundle): not checked
  body-missing, body-unreadable, body-digest-differs: the body itself
Without --with-731-fix the model keeps an inline part's shares as stored,
where 3.x reserializes them; `has_inline` flags those bodies. With it, inline
shares are reserialized as prism_legacy_range_sidecars.py models 3.x's
AcceptedShare, which this script loads from beside itself.

`--affected-csv` writes the refused rows (2x-order-only, matches-neither) in
the input's format, for prism_legacy_range_sidecars.py --rows. The exit status
is 0 only when every row is has-sidecar, ok-in-3x, no-parts-digest or
no-window-proof, in either mode; the summary on stderr counts each status, and
with --with-731-fix also says `"with_731_fix": true`.

Usage:
  prism_legacy_parts_digest_check.py --rows rows.csv \\
      --audit-root /var/lib/qbit-mining-pool/prism/audit \\
      --sidecar-dir <the directory passed to import-audits --root> \\
      [--affected-csv affected.csv] [--jobs 8] [--with-731-fix] > report.jsonl
  prism_legacy_parts_digest_check.py [--with-731-fix] BODY.json [BODY.json ...]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import multiprocessing
import os
import sys
from pathlib import Path

V2_SCHEMA = "qbit.prism.audit-bundle.v2"
# 3.x's SharePart fields per kind (crates/qbit-prism/src/audit_body_ref.rs);
# serde skips a segment range's segment bounds when they are absent.
FIELDS = {
    "segment": ("first_share_seq", "last_share_seq", "share_count", "sha256", "body_uri"),
    "segment_range": ("segment_first_share_seq", "segment_last_share_seq", "first_share_seq",
                      "last_share_seq", "share_count", "range_sha256", "body_uri"),
    "segment_prefix": ("first_share_seq", "last_share_seq", "share_count", "prefix_sha256",
                       "body_uri"),
    "inline": ("first_share_seq", "last_share_seq", "share_count", "shares"),
}
OPTIONAL = ("segment_first_share_seq", "segment_last_share_seq")
REFUSED = ("2x-order-only", "matches-neither")
OK_STATUSES = {"has-sidecar", "ok-in-3x", "no-parts-digest", "no-window-proof"}
CSV_COLUMNS = ("block_hash", "body_uri", "audit_bundle_sha256")


def digest_2x(parts: list) -> str:
    """2.x's share_parts_digest_hex (lab/prism/audit_artifacts.py on 2.x.x)."""
    return hashlib.sha256(
        json.dumps({"share_parts": parts}, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


_ACCEPTED_SHARE = None
SIDECAR_TOOL = Path(__file__).resolve().with_name("prism_legacy_range_sidecars.py")


def load_accepted_share():
    """prism_legacy_range_sidecars.py's model of 3.x's AcceptedShare, the one
    the sidecar tool proves ranges with: its fields in declaration order,
    unknown keys dropped, a null `credit_policy` left out. Loaded by path, and
    only for --with-731-fix, so the default mode needs no other file."""
    global _ACCEPTED_SHARE
    if _ACCEPTED_SHARE is None:
        spec = importlib.util.spec_from_file_location("prism_legacy_range_sidecars", SIDECAR_TOOL)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _ACCEPTED_SHARE = module.accepted_share
    return _ACCEPTED_SHARE


def accepted_share(share: dict) -> dict:
    return load_accepted_share()(share)


def part_as_3x(part: dict, shares_as_3x: bool = False) -> dict:
    """A part as 3.x parses it: its kind's known fields, in declaration order,
    absent segment bounds skipped. With `shares_as_3x`, an inline part's
    shares are parsed too, as 3.x with #731's fix re-serializes them;
    otherwise they are kept as stored."""
    kind = part.get("kind")
    out = {"kind": kind}
    for field in FIELDS.get(kind, ()):
        if field in OPTIONAL and part.get(field) is None:
            continue
        if field in part:
            value = part[field]
            if shares_as_3x and field == "shares" and isinstance(value, list):
                value = [accepted_share(share) if isinstance(share, dict) else share
                         for share in value]
            out[field] = value
    return out


def digest_3x(parts: list) -> str:
    """What 3.x without #731's fix recomputes: the parsed parts' known fields,
    keys sorted, raw UTF-8."""
    view = {"share_parts": [part_as_3x(part) for part in parts]}
    return hashlib.sha256(
        json.dumps(view, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def digest_3x_fixed(parts: list) -> str:
    """What 3.x with #731's fix (#809) recomputes: 2.x's form (`digest_2x`)
    over the parts as 3.x parses them, inline shares included. A body whose
    stored parts or shares differ from that, in key order, unknown keys or a
    null `credit_policy`, is refused even if 2.x's digest covers it."""
    return digest_2x([part_as_3x(part, shares_as_3x=True) for part in parts])


def map_uri(uri: str, prefix: str, root: Path | None, base: Path | None) -> Path:
    """prism_legacy_range_sidecars.py's mapping: the URI prefix becomes the
    audit root, and a relative URI resolves against import's --root."""
    path = uri[len("file://"):] if uri.startswith("file://") else uri
    if root is not None and prefix and (
        path == prefix or path.startswith(prefix.rstrip("/") + "/")
    ):
        return root / path[len(prefix):].lstrip("/")
    candidate = Path(path)
    return candidate if candidate.is_absolute() or base is None else base / candidate


def classify_body(path: Path, digest: str | None, with_731_fix: bool = False) -> dict:
    result: dict = {"body": str(path)}
    try:
        body = json.loads(path.read_bytes())
    except FileNotFoundError:
        result["status"] = "body-missing"
        return result
    except (OSError, ValueError) as exc:
        result["status"] = "body-unreadable"
        result["error"] = f"{type(exc).__name__}: {exc}"[:200]
        return result
    if (digest is not None and isinstance(body, dict) and "audit_bundle_sha256" in body
            and str(body["audit_bundle_sha256"]).lower() != digest.lower()):
        result["status"] = "body-digest-differs"
        return result
    if not isinstance(body, dict) or body.get("schema") != V2_SCHEMA:
        result["status"] = "no-window-proof"
        result["schema"] = body.get("schema") if isinstance(body, dict) else None
        return result
    if digest is not None and str(body.get("audit_bundle_sha256", "")).lower() != digest.lower():
        result["status"] = "body-digest-differs"
        return result
    proof = body.get("share_window_proof")
    parts = proof.get("share_parts") if isinstance(proof, dict) else None
    if not isinstance(parts, list) or not all(isinstance(part, dict) for part in parts):
        result["status"] = "body-unreadable"
        result["error"] = "share_window_proof.share_parts is not a list of objects"
        return result
    if proof.get("share_parts_digest_hex") is None:
        result["status"] = "no-parts-digest"
        return result
    expected = str(proof["share_parts_digest_hex"]).lower()
    result["parts"] = len(parts)
    result["has_inline"] = any(part.get("kind") == "inline" for part in parts)
    if with_731_fix:
        result["status"] = "ok-in-3x" if digest_3x_fixed(parts) == expected else "matches-neither"
    elif digest_3x(parts) == expected:
        result["status"] = "ok-in-3x"
    elif digest_2x(parts) == expected:
        result["status"] = "2x-order-only"
    else:
        result["status"] = "matches-neither"
    return result


def classify_row(job: tuple[dict, dict]) -> dict:
    row, options = job
    block = (row.get("block_hash") or "").strip()
    digest = (row.get("audit_bundle_sha256") or "").strip()
    body_uri = (row.get("body_uri") or "").strip()
    sidecar_dir = options["sidecar_dir"]
    if sidecar_dir is not None and block and digest:
        sidecar = sidecar_dir / f"prism-audit-bundle-canonical-{block}-{digest}.json.gz"
        if os.path.lexists(sidecar):
            result = {"status": "has-sidecar", "sidecar": str(sidecar)}
            return {"block_hash": block, "audit_bundle_sha256": digest, **result}
    if not body_uri:
        result = {"status": "body-missing"}
    else:
        path = map_uri(body_uri, options["uri_prefix"], options["audit_root"],
                       sidecar_dir or options["audit_root"])
        try:
            result = classify_body(path, digest or None, options.get("with_731_fix", False))
        except Exception as exc:  # one malformed body must not end the run
            result = {"body": str(path), "status": "body-unreadable",
                      "error": f"{type(exc).__name__}: {exc}"[:200]}
    if block or digest:
        result = {"block_hash": block, "audit_bundle_sha256": digest, **result}
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("bodies", nargs="*", type=Path, help="body files to check directly")
    parser.add_argument("--rows", "--csv", type=Path,
                        help="CSV with header block_hash,body_uri,audit_bundle_sha256")
    parser.add_argument("--audit-root", type=Path, help="where the audit tree is mounted")
    parser.add_argument("--uri-prefix", default="/var/lib/qbit-mining-pool/prism/audit",
                        help="body URI prefix that --audit-root replaces")
    parser.add_argument("--sidecar-dir", type=Path,
                        help="the import-audits --root directory: a row with a sidecar "
                             "there is not checked further")
    parser.add_argument("--affected-csv", type=Path,
                        help="write the rows import refuses, in the --rows format")
    parser.add_argument("--jobs", type=int, default=1, help="parallel processes (default 1)")
    parser.add_argument("--with-731-fix", action="store_true",
                        help="model a 3.x with #731's fix (#809), 3.1.1 or later, which "
                             "recomputes 2.x's parts digest over the parts as it parses "
                             "them; needs prism_legacy_range_sidecars.py beside this script")
    args = parser.parse_args(argv)
    if args.rows is None and not args.bodies:
        parser.error("give --rows or body files")
    if args.jobs < 1:
        parser.error("--jobs must be at least 1")
    if args.with_731_fix:
        # Refuse up front rather than report each inline body unreadable.
        try:
            load_accepted_share()
        except Exception as exc:
            parser.error(f"--with-731-fix needs {SIDECAR_TOOL}: {type(exc).__name__}: {exc}")
    options = {
        "audit_root": args.audit_root.absolute() if args.audit_root else None,
        "uri_prefix": args.uri_prefix,
        "sidecar_dir": args.sidecar_dir.absolute() if args.sidecar_dir else None,
        "with_731_fix": args.with_731_fix,
    }
    rows: list[dict] = [{"body_uri": str(body.absolute())} for body in args.bodies]
    if args.rows is not None:
        with open(args.rows, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            missing = set(CSV_COLUMNS) - set(reader.fieldnames or ())
            if missing:
                parser.error(f"--rows lacks the column(s) {', '.join(sorted(missing))}")
            rows.extend(reader)
    jobs = [(row, options) for row in rows]
    totals: dict[str, int] = {}
    refused: list[dict] = []
    pool = multiprocessing.Pool(args.jobs) if args.jobs > 1 and len(jobs) > 1 else None
    try:
        results = pool.imap(classify_row, jobs) if pool else map(classify_row, jobs)
        for row, result in zip(rows, results):
            status = result["status"]
            totals[status] = totals.get(status, 0) + 1
            if status in REFUSED and row.get("block_hash"):
                refused.append({column: row.get(column, "") for column in CSV_COLUMNS})
            print(json.dumps(result, sort_keys=True, ensure_ascii=False))
    finally:
        if pool:
            pool.close()
            pool.join()
    if args.affected_csv is not None:
        with open(args.affected_csv, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(refused)
    summary = {"import_refuses": sum(totals.get(status, 0) for status in REFUSED),
               "totals": dict(sorted(totals.items()))}
    if args.with_731_fix:
        summary["with_731_fix"] = True
    print(json.dumps(summary, sort_keys=True), file=sys.stderr)
    return 0 if set(totals) <= OK_STATUSES else 3


if __name__ == "__main__":
    sys.exit(main())

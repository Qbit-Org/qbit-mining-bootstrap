#!/usr/bin/env python3
"""Fail when a published alert rule's UID would stop Grafana provisioning it.

Grafana refuses a rule UID longer than 40 characters or with a character
outside `[A-Za-z0-9_-]`, and a provisioning file that carries one stops
Grafana from starting (#757). A UID is also the rule's identity within a
Grafana organization, and the published files are provisioned into the same
one, so a UID must be unique across every file, not only within its own.

The published files are `docs/*-alert-rules.json`, each `{"rules": [...]}`;
the two named in PUBLISHED must exist, so renaming one cannot silently drop
it from this check.

Usage: python3 scripts/check_prism_alert_uids.py [--root DIR]

Exits 0 when every UID is valid and unique and 1 otherwise, naming each rule.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-prism-alert-uids"
PUBLISHED = ("docs/prism-native-alert-rules.json", "docs/prism-postgres-alert-rules.json")
PATTERN = "docs/*-alert-rules.json"
MAX_LENGTH = 40
CHARACTERS = re.compile(r"[A-Za-z0-9_-]+")
# The whole contract in one pattern, for checks of rendered provisioning files.
GRAFANA_UID = re.compile(r"[A-Za-z0-9_-]{1,40}")


def published_files(root: Path) -> tuple[list[str], list[str]]:
    found = {path.relative_to(root).as_posix() for path in root.glob(PATTERN) if path.is_file()}
    missing = [f"{name} is missing" for name in PUBLISHED if name not in found]
    return sorted(found | set(PUBLISHED)), missing


def check(root: Path) -> tuple[list[str], int]:
    """Return every problem and the number of UIDs read."""
    names, problems = published_files(root)
    seen: dict[str, str] = {}
    count = 0
    for name in names:
        try:
            rules = json.loads((root / name).read_text(encoding="utf-8"))["rules"]
        except FileNotFoundError:
            continue
        except (json.JSONDecodeError, KeyError, TypeError) as error:
            problems.append(f"{name}: not a {{\"rules\": [...]}} document: {error!r}")
            continue
        if not isinstance(rules, list) or not rules:
            problems.append(f"{name}: \"rules\" must be a non-empty list")
            continue
        for index, rule in enumerate(rules):
            title = rule.get("title") if isinstance(rule, dict) else None
            where = f"{name}: rules[{index}] ({title or 'untitled'})"
            uid = rule.get("uid") if isinstance(rule, dict) else None
            if not isinstance(uid, str):
                problems.append(f"{where}: uid must be a string, got {uid!r}")
                continue
            count += 1
            if len(uid) > MAX_LENGTH:
                problems.append(
                    f"{where}: uid {uid!r} is {len(uid)} characters; "
                    f"Grafana allows at most {MAX_LENGTH}"
                )
            if not CHARACTERS.fullmatch(uid):
                problems.append(f"{where}: uid {uid!r} must be non-empty and use only [A-Za-z0-9_-]")
            if uid in seen:
                problems.append(f"{where}: uid {uid!r} is also used by {seen[uid]}")
            else:
                seen[uid] = where
    return problems, count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--root", type=Path, default=ROOT, help="checkout to check (default: this checkout)")
    args = parser.parse_args(argv)
    problems, count = check(args.root.resolve())
    for problem in problems:
        print(f"{PREFIX}: {problem}", file=sys.stderr)
    if problems:
        print(f"{PREFIX}: {len(problems)} problem(s) with published alert rule UIDs", file=sys.stderr)
        return 1
    print(f"{PREFIX}: {count} rule UIDs in {PATTERN} are unique and within Grafana's limits")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

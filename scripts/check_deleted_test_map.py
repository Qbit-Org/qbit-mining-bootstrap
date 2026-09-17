#!/usr/bin/env python3
"""Fail when the deleted-test map stops being checkable evidence.

`docs/prism-deleted-test-map.md` maps each Python test file the native cutover
deleted to the 3.x.x test that replaces it, or to the open issue that will. It
is parity evidence for the cutover go/no-go, and nothing used to read it: the
issues it linked closed one by one without the map changing.

Usage: python3 scripts/check_deleted_test_map.py [--root DIR] [--check-issues] [--summary]

Without flags the check is offline and is what required CI runs:

- every table is one the map defines and every row parses (a line this script
  cannot read is an error, never a skipped row);
- every `path::name` reference names a file in the repository and a test
  function in it (`#[test]`-style attribute in Rust, `def test_` in Python);
- a status is one of the five the legend defines, the row's text leads with
  it, a full row cites a test and an open gap row links the issue that owns it;
- the summary table and the per-section counts equal the rows;
- the needs triage index lists exactly the needs triage rows;
- every issue link points at this repository's issue of the same number, and
  an owner arrow is followed by a link rather than a bare number.

`--check-issues` additionally asks GitHub for the state of every issue the map
names as the owner of uncovered behaviour and fails if one is closed. An owner
is any issue link inside a table row, or an arrow link (`→ [#N](...)`) anywhere
in the map. A closed issue is history: a row mentions it as plain `#N`, and
prose may link it without an arrow. Reads `GITHUB_TOKEN` or `GH_TOKEN` when set.

`--summary` prints the row counts and the rows that are still open, for the
sign-off comment on the qualification issue.

Exits 0 when the map holds, 1 when it does not, and 2 when `--check-issues`
could not learn an issue's state (network, rate limit, unexpected reply): an
unknown state is reported as unknown, never as open.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
import http.client
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-deleted-test-map"
MAP = Path("docs") / "prism-deleted-test-map.md"
REPOSITORY = "Qbit-Org/qbit-mining-bootstrap"
DEFAULT_API_URL = "https://api.github.com"

FULL = "full"
PARTIAL = "partial"
OPEN_GAP = "open gap"
NEEDS_TRIAGE = "needs triage"
RETIRED = "retired"
STATUSES = (FULL, PARTIAL, OPEN_GAP, NEEDS_TRIAGE, RETIRED)

FILE_HEADER = ("2.x.x file", "Covered", "3.x.x replacement or status")
CASE_HEADER = ("2.x.x case", "Covered", "3.x.x replacement or status")
TRIAGE_HEADER = ("2.x.x file", "Area", "Reason")
SUMMARY_HEADER = ("Rows", "Full", "Partial", "Open gap", "Needs triage", "Retired")
SUMMARY_STATUSES = (FULL, PARTIAL, OPEN_GAP, NEEDS_TRIAGE, RETIRED)
SECTION_LIST_LEAD = "Sections and row counts:"

UNESCAPED_PIPE = re.compile(r"(?<!\\)\|")
# Rows may carry `<!-- retired-setting: NAME -->` markers for check_prism_settings.py.
HTML_COMMENT = re.compile(r"<!--.*?-->")
SEPARATOR_CELL = re.compile(r"^:?-{3,}:?$")
NAME_CELL = re.compile(r"^`(?P<name>[^`]+)`$")
CODE_SPAN = re.compile(r"`([^`\n]+)`")
REFERENCE = re.compile(
    r"^(?P<path>[A-Za-z0-9_][A-Za-z0-9_./-]*\.(?P<ext>rs|py))::(?P<name>[A-Za-z_][A-Za-z0-9_]*)$"
)
ISSUE_LINK = re.compile(r"(?P<arrow>(?:→|->)\s*)?\[#(?P<label>[0-9]+)\]\((?P<url>[^)\s]*)\)")
BARE_OWNER = re.compile(r"(?:→|->)\s*#[0-9]+")
SECTION_LIST_ITEM = re.compile(r"^- (?P<name>.+) \((?P<count>[0-9]+)\)$")
RUST_FN = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*[(<]"
)
RUST_TEST_ATTRIBUTE = re.compile(r"#\[\s*(?:[A-Za-z_][A-Za-z0-9_]*::)*test\b")
PYTHON_DEF = re.compile(r"^\s*(?:async\s+)?def\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(")

REQUEST_TIMEOUT_SECONDS = 10.0
ATTEMPTS = 3


@dataclass(frozen=True)
class Row:
    line: int
    name: str
    status: str
    text: str
    section: str


@dataclass(frozen=True)
class TriageRow:
    line: int
    name: str
    area: str
    text: str


@dataclass(frozen=True)
class IssueLink:
    line: int
    number: int
    url: str
    arrow: bool


@dataclass
class ParsedMap:
    file_rows: list[Row] = field(default_factory=list)
    case_rows: list[Row] = field(default_factory=list)
    triage_rows: list[TriageRow] = field(default_factory=list)
    summaries: list[tuple[int, list[str]]] = field(default_factory=list)
    section_list: list[tuple[int, str, int]] = field(default_factory=list)
    links: list[IssueLink] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def at(line: int, message: str) -> str:
    return f"{MAP.as_posix()}:{line}: {message}"


def split_cells(line: str) -> list[str] | None:
    stripped = HTML_COMMENT.sub("", line).strip()
    if not stripped.endswith("|") or stripped.endswith("\\|") or len(stripped) < 2:
        return None
    return [cell.strip() for cell in UNESCAPED_PIPE.split(stripped)[1:-1]]


def parse_row(parsed: ParsedMap, number: int, cells: list[str], section: str, *, case: bool) -> None:
    if len(cells) != 3:
        parsed.errors.append(
            at(number, f"row has {len(cells)} cells, expected 3 (write a literal pipe inside a cell as \\|)")
        )
        return
    name = NAME_CELL.match(cells[0])
    if name is None:
        parsed.errors.append(at(number, f"first cell must be one `code` name, found {cells[0]!r}"))
        return
    status = cells[1]
    if status not in STATUSES:
        parsed.errors.append(at(number, f"unknown status {status!r}; the legend defines: {', '.join(STATUSES)}"))
        return
    row = Row(number, name.group("name"), status, cells[2], section)
    (parsed.case_rows if case else parsed.file_rows).append(row)


def parse_table(parsed: ParsedMap, table: list[tuple[int, str]], section: str) -> None:
    rows: list[tuple[int, list[str]]] = []
    for number, line in table:
        cells = split_cells(line)
        if cells is None:
            parsed.errors.append(at(number, "table line does not end with an unescaped pipe"))
            if not rows:
                return
            continue
        rows.append((number, cells))
    header_line, header = rows[0]
    kind = tuple(header)
    if kind not in (FILE_HEADER, CASE_HEADER, TRIAGE_HEADER, SUMMARY_HEADER):
        parsed.errors.append(at(header_line, f"unrecognised table header {header!r}; this check does not know how to read it"))
        return
    if len(rows) < 2 or not all(SEPARATOR_CELL.match(cell) for cell in rows[1][1]) or len(rows[1][1]) != len(header):
        parsed.errors.append(at(header_line, "table header is not followed by a separator row of the same width"))
        return
    for number, cells in rows[2:]:
        if kind == SUMMARY_HEADER:
            parsed.summaries.append((number, cells))
        elif kind == TRIAGE_HEADER:
            name = NAME_CELL.match(cells[0]) if len(cells) == 3 else None
            if name is None:
                parsed.errors.append(at(number, "needs triage index row must be `file` | area | reason"))
                continue
            parsed.triage_rows.append(TriageRow(number, name.group("name"), cells[1], cells[2]))
        else:
            parse_row(parsed, number, cells, section, case=kind == CASE_HEADER)


def parse_map(text: str) -> ParsedMap:
    parsed = ParsedMap()
    section = ""
    table: list[tuple[int, str]] = []
    in_section_list = False
    in_fence = False
    lines = text.splitlines()
    for number, line in enumerate([*lines, ""], start=1):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
        if in_fence:
            continue
        if line.startswith("|"):
            table.append((number, line))
            continue
        if table:
            parse_table(parsed, table, section)
            table = []
        if line.startswith("## "):
            section = line[3:].strip()
        if line.strip() == SECTION_LIST_LEAD:
            in_section_list = True
        elif in_section_list and line.startswith("- "):
            item = SECTION_LIST_ITEM.match(line.rstrip())
            if item is None:
                parsed.errors.append(at(number, "section list item must read `- <section heading> (<row count>)`"))
            else:
                parsed.section_list.append((number, item.group("name"), int(item.group("count"))))
        elif in_section_list and (line.strip() or parsed.section_list):
            # A blank line may separate the lead from its items; anything after them ends the list.
            in_section_list = False
    for number, line in enumerate(lines, start=1):
        for link in ISSUE_LINK.finditer(line):
            parsed.links.append(
                IssueLink(number, int(link.group("label")), link.group("url"), link.group("arrow") is not None)
            )
        if BARE_OWNER.search(line):
            parsed.errors.append(
                at(number, "an owner arrow must be followed by an issue link, not a bare number, so its state can be checked")
            )
    return parsed


def rust_functions(text: str) -> tuple[set[str], set[str]]:
    """Every `fn` name, and the subset whose attribute block marks a test."""
    lines = text.splitlines()
    functions: set[str] = set()
    tests: set[str] = set()
    for index, line in enumerate(lines):
        found = RUST_FN.match(line)
        if found is None:
            continue
        functions.add(found.group("name"))
        attributes: list[str] = []
        cursor = index - 1
        while cursor >= 0:
            above = lines[cursor].strip()
            if not above:
                break
            if not above.startswith("//") and above.endswith(("{", "}", ";")):
                break
            if not above.startswith("//"):
                attributes.append(above)
            cursor -= 1
        if RUST_TEST_ATTRIBUTE.search(" ".join(reversed(attributes))):
            tests.add(found.group("name"))
    return functions, tests


def python_functions(text: str) -> tuple[set[str], set[str]]:
    functions = {found.group("name") for line in text.splitlines() if (found := PYTHON_DEF.match(line))}
    return functions, {name for name in functions if name.startswith("test_")}


class References:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.cache: dict[str, tuple[set[str], set[str]] | str] = {}

    def load(self, path: str, extension: str) -> tuple[set[str], set[str]] | str:
        if path not in self.cache:
            if ".." in PurePosixPath(path).parts:
                self.cache[path] = "path leaves the repository"
            else:
                target = self.root / path
                try:
                    text = target.read_text(encoding="utf-8")
                except FileNotFoundError:
                    self.cache[path] = "no such file"
                except (OSError, UnicodeDecodeError) as error:
                    self.cache[path] = f"unreadable: {error}"
                else:
                    self.cache[path] = rust_functions(text) if extension == "rs" else python_functions(text)
        return self.cache[path]

    def problem(self, path: str, extension: str, name: str) -> str | None:
        loaded = self.load(path, extension)
        if isinstance(loaded, str):
            return f"`{path}::{name}`: {loaded}"
        functions, tests = loaded
        keyword = "fn" if extension == "rs" else "def"
        if name not in functions:
            return f"`{path}::{name}`: no `{keyword} {name}` in that file"
        if name not in tests:
            wanted = "a #[test]-style attribute above it" if extension == "rs" else "a `test_` name"
            return f"`{path}::{name}`: `{keyword} {name}` exists but is not a test function ({wanted} is required)"
        return None


def references_in(text: str) -> tuple[list[tuple[str, str, str]], list[str]]:
    """Well-formed `path::name` references, and spans that look like one and are not."""
    good: list[tuple[str, str, str]] = []
    malformed: list[str] = []
    for span in CODE_SPAN.findall(text):
        if "::" not in span:
            continue
        found = REFERENCE.match(span)
        if found is not None:
            good.append((found.group("path"), found.group("ext"), found.group("name")))
            continue
        left = span.split("::", 1)[0]
        if "/" in left or left.endswith((".rs", ".py")):
            malformed.append(span)
    return good, malformed


def check_references(text: str, root: Path) -> list[str]:
    errors: list[str] = []
    references = References(root)
    for number, line in enumerate(text.splitlines(), start=1):
        good, malformed = references_in(line)
        for span in malformed:
            errors.append(at(number, f"`{span}` looks like a test reference but is not `<path>.rs|.py::<function>`"))
        for path, extension, name in dict.fromkeys(good):
            problem = references.problem(path, extension, name)
            if problem is not None:
                errors.append(at(number, problem))
    return errors


def check_rows(parsed: ParsedMap) -> list[str]:
    errors: list[str] = []
    linked_lines = {link.line for link in parsed.links}
    for label, rows in (("file", parsed.file_rows), ("case", parsed.case_rows)):
        seen: dict[str, int] = {}
        for row in rows:
            if row.name in seen:
                errors.append(at(row.line, f"{label} `{row.name}` already has a row at line {seen[row.name]}"))
            seen.setdefault(row.name, row.line)
    for row in [*parsed.file_rows, *parsed.case_rows]:
        lead = next((status for status in STATUSES if row.text.startswith(status)), None)
        if row.status == FULL:
            if lead not in (None, FULL):
                errors.append(at(row.line, f"status is full but the text leads with {lead!r}"))
            if not references_in(row.text)[0]:
                errors.append(at(row.line, "a full row must cite at least one `path::name` test"))
        elif lead != row.status:
            errors.append(at(row.line, f"status is {row.status!r} but the text does not lead with it"))
        if row.status == OPEN_GAP and row.line not in linked_lines:
            errors.append(at(row.line, "an open gap row must link the issue that closes the gap"))
    return errors


def check_triage_index(parsed: ParsedMap) -> list[str]:
    errors: list[str] = []
    rows = {row.name: row for row in parsed.file_rows if row.status == NEEDS_TRIAGE}
    index: dict[str, TriageRow] = {}
    for entry in parsed.triage_rows:
        if entry.name in index:
            errors.append(at(entry.line, f"`{entry.name}` is listed twice in the needs triage index"))
        index.setdefault(entry.name, entry)
    for name, row in rows.items():
        if name not in index:
            errors.append(at(row.line, f"`{name}` is needs triage but missing from the needs triage index"))
    for name, entry in index.items():
        row = rows.get(name)
        if row is None:
            errors.append(at(entry.line, f"`{name}` is in the needs triage index but has no needs triage row"))
        elif entry.area != row.section:
            errors.append(at(entry.line, f"area {entry.area!r} is not the section the row sits in, {row.section!r}"))
    return errors


def check_counts(parsed: ParsedMap) -> list[str]:
    errors: list[str] = []
    if len(parsed.summaries) != 1:
        return [at(1, f"expected exactly one summary row under {' | '.join(SUMMARY_HEADER)}, found {len(parsed.summaries)}")]
    line, cells = parsed.summaries[0]
    if len(cells) != len(SUMMARY_HEADER) or not all(cell.isascii() and cell.isdigit() for cell in cells):
        return [at(line, f"summary row must be {len(SUMMARY_HEADER)} whole numbers, found {cells!r}")]
    declared = [int(cell) for cell in cells]
    actual = Counter(row.status for row in parsed.file_rows)
    if declared[0] != len(parsed.file_rows):
        errors.append(at(line, f"summary says {declared[0]} rows, the file tables hold {len(parsed.file_rows)}"))
    for status, count in zip(SUMMARY_STATUSES, declared[1:]):
        if count != actual[status]:
            errors.append(at(line, f"summary says {count} {status}, the rows say {actual[status]}"))

    per_section = Counter(row.section for row in parsed.file_rows)
    if not parsed.section_list:
        errors.append(at(1, f"no `{SECTION_LIST_LEAD}` list found"))
    listed: set[str] = set()
    for item_line, name, count in parsed.section_list:
        if name in listed:
            errors.append(at(item_line, f"section {name!r} is listed twice"))
        listed.add(name)
        if name not in per_section:
            errors.append(at(item_line, f"section list names {name!r}, which has no file rows under a heading of that name"))
        elif count != per_section[name]:
            errors.append(at(item_line, f"section list says {count} rows in {name!r}, the section holds {per_section[name]}"))
    for name in per_section:
        if name not in listed and parsed.section_list:
            errors.append(at(1, f"section {name!r} holds {per_section[name]} rows and is missing from the section list"))
    return errors


def check_links(parsed: ParsedMap, repository: str) -> list[str]:
    errors: list[str] = []
    for link in parsed.links:
        expected = f"https://github.com/{repository}/issues/{link.number}"
        if link.url != expected:
            errors.append(at(link.line, f"[#{link.number}] links {link.url!r}, expected {expected}"))
    return errors


def check_offline(text: str, root: Path, repository: str) -> tuple[ParsedMap, list[str]]:
    parsed = parse_map(text)
    errors = list(parsed.errors)
    errors += check_rows(parsed)
    errors += check_triage_index(parsed)
    errors += check_counts(parsed)
    errors += check_links(parsed, repository)
    errors += check_references(text, root)
    return parsed, errors


def owner_links(parsed: ParsedMap) -> dict[int, list[int]]:
    """Issue number to the lines that name it as the owner of uncovered behaviour."""
    row_lines = {row.line for row in [*parsed.file_rows, *parsed.case_rows, *parsed.triage_rows]}
    owners: dict[int, list[int]] = {}
    for link in parsed.links:
        if link.arrow or link.line in row_lines:
            lines = owners.setdefault(link.number, [])
            if link.line not in lines:
                lines.append(link.line)
    return owners


class StateUnknown(Exception):
    """GitHub did not say whether the issue is open; carries the reason."""


def fetch_issue_state(api_url: str, repository: str, number: int, token: str | None, deadline: float) -> str:
    request = urllib.request.Request(
        f"{api_url}/repos/{repository}/issues/{number}",
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": PREFIX,
        },
    )
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    reason = "no attempt was made"
    for attempt in range(ATTEMPTS):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise StateUnknown(f"ran out of time ({reason})")
        try:
            with urllib.request.urlopen(request, timeout=min(REQUEST_TIMEOUT_SECONDS, remaining)) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            reason = f"HTTP {error.code}"
            if error.code == 404:
                raise StateUnknown("HTTP 404: no such issue, or not visible to this token") from None
            if error.code < 500:
                raise StateUnknown(f"{reason} (rate limit or permissions; set GITHUB_TOKEN)") from None
        except (OSError, http.client.HTTPException) as error:
            # URLError and TimeoutError are OSErrors; a torn reply is an HTTPException.
            if isinstance(getattr(error, "reason", None), ssl.SSLCertVerificationError):
                raise StateUnknown(
                    f"TLS certificate verification failed ({error.reason.verify_message}); "
                    "this Python has no usable CA bundle, set SSL_CERT_FILE"
                ) from None
            reason = f"request failed: {error!r}"
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise StateUnknown("reply was not JSON") from None
        else:
            state = payload.get("state") if isinstance(payload, dict) else None
            if state not in ("open", "closed"):
                raise StateUnknown(f"reply carried state {state!r}, expected 'open' or 'closed'")
            return state
        if attempt + 1 < ATTEMPTS:
            time.sleep(max(0.0, min(0.5 * (attempt + 1), deadline - time.monotonic())))
    raise StateUnknown(reason)


def check_issues(
    parsed: ParsedMap, api_url: str, repository: str, token: str | None, timeout_seconds: float
) -> tuple[list[str], list[str]]:
    closed: list[str] = []
    unknown: list[str] = []
    deadline = time.monotonic() + timeout_seconds
    for number, lines in sorted(owner_links(parsed).items()):
        where = ", ".join(str(line) for line in lines)
        try:
            state = fetch_issue_state(api_url, repository, number, token, deadline)
        except StateUnknown as error:
            unknown.append(f"#{number} (map lines {where}): state unknown: {error}")
            continue
        if state == "closed":
            closed.append(
                f"#{number} is closed but the map names it as the owner of uncovered behaviour on "
                f"{MAP.as_posix()} lines {where}; name the tests that landed, retire the remainder with a "
                "reason, or point it at an open issue"
            )
    return closed, unknown


def summary(parsed: ParsedMap) -> str:
    counts = Counter(row.status for row in parsed.file_rows)
    owners_by_line: dict[int, list[int]] = {}
    for number, owner_lines in sorted(owner_links(parsed).items()):
        for line in owner_lines:
            owners_by_line.setdefault(line, []).append(number)
    lines = [
        f"{len(parsed.file_rows)} rows: {counts[FULL]} full, {counts[PARTIAL]} partial, {counts[RETIRED]} retired, "
        f"{counts[OPEN_GAP]} open gap, {counts[NEEDS_TRIAGE]} needs triage.",
    ]
    for status in (OPEN_GAP, NEEDS_TRIAGE):
        rows = [row for row in parsed.file_rows if row.status == status]
        if rows:
            lines += ["", f"{status.capitalize()} ({len(rows)}):"]
            for row in rows:
                linked = ", ".join(f"#{number}" for number in owners_by_line.get(row.line, []))
                lines.append(f"- `{row.name}`" + (f" (links {linked})" if linked else ""))
    owned = [row for row in [*parsed.file_rows, *parsed.case_rows] if row.status not in (OPEN_GAP, NEEDS_TRIAGE) and row.line in owners_by_line]
    if owned:
        lines += ["", f"Rows with a remainder owned by an open issue ({len(owned)}):"]
        for row in owned:
            owners = ", ".join(f"#{number}" for number in owners_by_line[row.line])
            lines.append(f"- `{row.name}` ({row.status}, owner {owners})")
    return "\n".join(lines)


def positive_finite(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from None
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError(f"{value!r} must be a finite number of seconds above zero")
    return number


def http_url(value: str) -> str:
    parts = urllib.parse.urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise argparse.ArgumentTypeError(f"{value!r} must be an http(s) URL")
    return value.rstrip("/")


def repository_name(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value):
        raise argparse.ArgumentTypeError(f"{value!r} must be <owner>/<name>")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=ROOT, help="repository root (default: this checkout)")
    parser.add_argument("--check-issues", action="store_true", help="ask GitHub whether each owner issue is still open")
    parser.add_argument("--summary", action="store_true", help="print the row counts and the rows that are still open")
    parser.add_argument("--repository", type=repository_name, default=REPOSITORY, help=f"default: {REPOSITORY}")
    parser.add_argument(
        "--api-url",
        type=http_url,
        default=http_url(os.environ.get("GITHUB_API_URL") or DEFAULT_API_URL),
        help=f"GitHub API base (default: $GITHUB_API_URL or {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--timeout-seconds", type=positive_finite, default=60.0, help="total time allowed for --check-issues (default: 60)"
    )
    args = parser.parse_args(argv)

    path = args.root / MAP
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        print(f"{PREFIX}: cannot read {path}: {error}", file=sys.stderr)
        return 1

    parsed, errors = check_offline(text, args.root, args.repository)
    unknown: list[str] = []
    if args.check_issues:
        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or None
        closed, unknown = check_issues(parsed, args.api_url, args.repository, token, args.timeout_seconds)
        errors += closed

    for error in errors:
        print(f"{PREFIX}: {error}", file=sys.stderr)
    for entry in unknown:
        print(f"{PREFIX}: {entry}", file=sys.stderr)
    if errors:
        print(f"{PREFIX}: {len(errors)} problem(s) in {MAP.as_posix()}", file=sys.stderr)
        return 1
    if unknown:
        print(f"{PREFIX}: could not determine the state of {len(unknown)} issue(s); nothing is assumed open", file=sys.stderr)
        return 2

    if args.summary:
        print(summary(parsed))
    else:
        checked = f", {len(owner_links(parsed))} owner issue(s) open" if args.check_issues else ""
        print(f"{PREFIX}: ok ({len(parsed.file_rows)} file rows, {len(parsed.case_rows)} case rows{checked})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

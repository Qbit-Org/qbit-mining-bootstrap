#!/usr/bin/env python3
"""Review gate for PRISM checkout sites, not a Rust type checker.

Walk the production module graph, tokenize Rust (comments and SQL strings are
opaque), and inventory SQLx-shaped calls regardless of receiver names. Mutable
executor borrows are SQL on an existing connection; their nested acquire calls
are still scanned. Everything else needs an exact, counted classification.
Pool/Executor/Acquire/AuditReader helper signatures also enroll their callers,
so passing a pool to an existing generic helper cannot bypass the gate.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path("crates/qbit-prism-server/src")
INVENTORY = Path("scripts/prism_pool_acquires.json")
OPERATIONS = {
    "acquire", "try_acquire", "begin", "try_begin", "begin_with", "try_begin_with", "execute", "execute_many",
    "fetch", "fetch_all", "fetch_one", "fetch_optional", "fetch_many",
    "prepare", "prepare_with", "describe",
}
EXECUTORS = OPERATIONS - {"acquire", "try_acquire", "begin", "try_begin", "begin_with", "try_begin_with"}
CLASSES = {"timed", "delegated", "borrowed", "startup", "operator", "public", "compatibility", "non-database"}
TOKEN = re.compile(
    r'\s+|//[^\n]*|/\*|(?:br|r)(?P<hash>\#*)"|(?:b|c)?"(?:\\.|[^"\\])*"'
    r"|b?'(?:\\.|[^'\\\n])'|[A-Za-z_][A-Za-z_0-9]*|::|->|=>|[^\s]",
    re.DOTALL,
)


@dataclass(frozen=True)
class Token:
    value: str
    line: int


def tokenize(source: str) -> list[Token]:
    result = []
    pos = 0
    line = 1
    while pos < len(source):
        match = TOKEN.match(source, pos)
        if match is None:
            raise ValueError(f"cannot tokenize line {line}")
        value = match.group()
        end = match.end()
        if value == "/*":
            depth = 1
            while depth:
                comment = re.search(r"/\*|\*/", source[end:])
                if comment is None:
                    raise ValueError("unterminated block comment")
                depth += 1 if comment.group() == "/*" else -1
                end += comment.end()
        elif match.group("hash") is not None:
            marker = '"' + match.group("hash")
            close = source.find(marker, end)
            if close < 0:
                raise ValueError("unterminated raw string")
            end = close + len(marker)
            result.append(Token(source[pos:end], line))
        elif not value.isspace() and not value.startswith("//"):
            result.append(Token(value, line))
        line += source[pos:end].count("\n")
        pos = end
    return result


def pairs(tokens: list[Token]) -> dict[int, int]:
    stack = []
    result = {}
    for i, token in enumerate(tokens):
        if token.value in {"(", "[", "{"}:
            stack.append(i)
        elif token.value in {")", "]", "}"}:
            if not stack:
                raise ValueError(f"unmatched delimiter at line {token.line}")
            start = stack.pop()
            if tokens[start].value != {")": "(", "]": "[", "}": "{"}[token.value]:
                raise ValueError(f"mismatched delimiter at line {token.line}")
            result[start] = i
            result[i] = start
    if stack:
        raise ValueError("unclosed delimiter")
    return result


def production_tokens(source: str) -> list[Token]:
    tokens = tokenize(source)
    matching = pairs(tokens)
    removed = set()
    for i in range(len(tokens) - 2):
        if tokens[i].value != "#" or tokens[i + 1].value != "[":
            continue
        end = matching[i + 1]
        cfg = [t.value for t in tokens[i + 2:end]]
        # Only unambiguously test-only items are removed. Other cfg branches
        # remain in the census, including both platform implementations.
        required_test = False
        if cfg[:4] == ["cfg", "(", "all", "("]:
            depth = 0
            for value in cfg[4:-2]:
                if value == "(":
                    depth += 1
                elif value == ")":
                    depth -= 1
                elif value == "test" and depth == 0:
                    required_test = True
        test_only = cfg == ["cfg", "(", "test", ")"] or required_test
        if not test_only:
            continue
        j = end + 1
        while j < len(tokens):
            value = tokens[j].value
            if value in {"(", "["}:
                j = matching[j] + 1
                continue
            if value == "{":
                j = matching[j] + 1
                break
            if value in {";", ","}:
                j += 1
                break
            j += 1
        removed.update(range(i, j))
    return [t for i, t in enumerate(tokens) if i not in removed]


def modules(root: Path) -> dict[str, list[Token]]:
    directory = root / SOURCE
    pending = [directory / "lib.rs", directory / "main.rs", *sorted((directory / "bin").glob("*.rs"))]
    found = {}
    while pending:
        path = pending.pop()
        relative = path.relative_to(root).as_posix()
        if relative in found or not path.exists():
            continue
        tokens = production_tokens(path.read_text())
        if any(tokens[i].value == "include" and tokens[i + 1].value == "!" for i in range(len(tokens) - 1)):
            raise ValueError(f"{relative}: include! needs explicit source-census support")
        found[relative] = tokens
        matching = pairs(tokens)
        base = path.parent if path.name in {"lib.rs", "main.rs", "mod.rs"} else path.with_suffix("")
        for i, token in enumerate(tokens[:-2]):
            if token.value != "mod" or tokens[i + 2].value != ";":
                continue
            name = tokens[i + 1].value
            # Inline modules change the directory of nested external modules.
            parents = [tokens[j + 1].value for j in range(i - 2)
                       if tokens[j].value == "mod" and tokens[j + 2].value == "{"
                       and matching[j + 2] > i]
            module_base = base.joinpath(*parents)
            candidates = [module_base / f"{name}.rs", module_base / name / "mod.rs"]
            # An immediately preceding #[path] may precede visibility tokens.
            for j in range(max(0, i - 12), i):
                if [t.value for t in tokens[j:j + 2]] == ["path", "="]:
                    candidates = [path.parent / json.loads(tokens[j + 2].value)]
            existing = [p for p in candidates if p.is_file()]
            if len(existing) != 1:
                raise ValueError(f"cannot resolve {relative} mod {name}: {candidates}")
            pending.append(existing[0])
    return found


def normalized(tokens: list[Token]) -> str:
    return " ".join("STRING" if '"' in t.value else t.value for t in tokens)


def functions(tokens: list[Token]):
    matching = pairs(tokens)
    occurrences = Counter()
    result = []
    for i, token in enumerate(tokens[:-1]):
        if token.value != "fn" or tokens[i + 1].value == "(":
            continue
        name = tokens[i + 1].value
        j = i + 2
        while j < len(tokens) and tokens[j].value not in {"{", ";"}:
            j = matching[j] + 1 if tokens[j].value in {"(", "["} else j + 1
        if j == len(tokens) or tokens[j].value == ";":
            continue
        occurrences[name] += 1
        result.append((i, j, matching[j], f"{name}#{occurrences[name]}", name))
    return result


def census(root: Path) -> tuple[Counter, list[dict], int]:
    sources = modules(root)
    pool_types = {"PgPool", "Pool", "Executor", "Acquire", "AuditReader", "AuditConnection", "PoolConnection"}
    changed = True
    while changed:
        before = len(pool_types)
        for tokens in sources.values():
            for i in range(1, len(tokens) - 1):
                if tokens[i].value == "as" and tokens[i - 1].value in pool_types:
                    pool_types.add(tokens[i + 1].value)
                if tokens[i - 1].value == "type":
                    end = i + 1
                    while end < len(tokens) and tokens[end].value != ";":
                        end += 1
                    if any(t.value in pool_types for t in tokens[i + 1:end]):
                        pool_types.add(tokens[i].value)
        changed = len(pool_types) != before
    helpers = set()
    for tokens in sources.values():
        for start, body, _, _, name in functions(tokens):
            signature = {t.value for t in tokens[start:body]}
            if start > 0 and tokens[start - 1].value == "async" and signature & pool_types:
                helpers.add(name)
    # Follow imported function aliases as well as direct/qualified helper calls.
    changed = True
    while changed:
        before = len(helpers)
        for tokens in sources.values():
            for i in range(1, len(tokens) - 1):
                if tokens[i].value == "as" and tokens[i - 1].value in helpers:
                    helpers.add(tokens[i + 1].value)
        changed = len(helpers) != before
    sites = Counter()
    details = []
    borrowed = 0
    for path, tokens in sorted(sources.items()):
        matching = pairs(tokens)
        definitions = functions(tokens)
        declarations = {start + 1 for start, *_ in definitions}
        for i in range(1, len(tokens) - 1):
            name = tokens[i].value
            if i in declarations or name not in OPERATIONS | helpers:
                continue
            opening = i + 1
            # Generic method calls (fetch_one::<...>) are uncommon, but must
            # not disappear from the census if introduced.
            if tokens[opening].value == "::" and tokens[opening + 1].value == "<":
                opening += 2
                depth = 1
                while depth and opening + 1 < len(tokens):
                    opening += 1
                    depth += (tokens[opening].value == "<") - (tokens[opening].value == ">")
                opening += 1
            if opening >= len(tokens) or tokens[opening].value != "(":
                continue
            enclosing = [(body, label) for _, body, end, label, _ in definitions if body < i < end]
            label = max(enclosing)[1] if enclosing else "<module>"
            args = tokens[opening + 1:matching[opening]]
            previous = tokens[i - 1].value
            if name in EXECUTORS and previous == "." and [t.value for t in args[:2]] == ["&", "mut"]:
                borrowed += 1
                continue
            # A receiver for acquire/begin, or full arguments for executor and
            # helper calls. Do not depend on a variable being named 'pool'.
            receiver = []
            if previous == "." and name not in EXECUTORS:
                j = i - 2
                while j >= 0:
                    value = tokens[j].value
                    if value in {")", "]"}:
                        j = matching[j] - 1
                        continue
                    if value in {";", "=", "{", "}", ",", "(", "=>", "&", "return", "?", ":", "*", "mut"}:
                        break
                    j -= 1
                receiver = tokens[j + 1:i]
            key = f"{path.removeprefix(str(SOURCE) + '/')}::{label}::{normalized(receiver)}{name}({normalized(args)})"
            # Removing or disabling a timer changes the classified site too.
            for j in range(i):
                if tokens[j].value == "time_pool_acquire" and tokens[j + 1].value == "(" and matching[j + 1] > i:
                    first = j + 2
                    end = first
                    while tokens[end].value != ",":
                        end = matching[end] + 1 if tokens[end].value in {"(", "[", "{"} else end + 1
                    key += f" [timer: {normalized(tokens[first:end])}]"
                    break
            sites[key] += 1
            details.append({"site": key, "line": tokens[i].line})
    return sites, details, borrowed


def check(sites: Counter, inventory: dict) -> list[str]:
    errors = []
    entries = inventory["sites"]
    policies = inventory["policies"]
    for site, count in sites.items():
        entry = entries.get(site)
        if entry is None:
            errors.append(f"unclassified ({count}): {site}")
            continue
        expected, policy = entry
        if expected != count:
            errors.append(f"changed count {expected} -> {count}: {site}")
        rule = policies.get(policy, {})
        if rule.get("class") not in CLASSES or not rule.get("owner") or not rule.get("reason"):
            errors.append(f"classification requires class, owner and reason: {site}")
    errors.extend(f"stale classification: {site}" for site in entries.keys() - sites.keys())
    used = {entry[1] for entry in entries.values()}
    errors.extend(f"stale policy: {policy}" for policy in policies.keys() - used)
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--census", action="store_true", help="print source sites without changing classifications")
    args = parser.parse_args()
    sites, details, borrowed = census(args.root)
    if args.census:
        print(json.dumps({"sites": sites, "locations": details, "borrowed_statements": borrowed}, indent=2))
        return 0
    inventory = json.loads((args.root / INVENTORY).read_text())
    errors = check(sites, inventory)
    if errors:
        print("\n".join(errors))
        return 1
    counts = Counter()
    for site, count in sites.items():
        policy = inventory["sites"][site][1]
        counts[inventory["policies"][policy]["class"]] += count
    print(f"PRISM checkout source census: {dict(sorted(counts.items()))}; {borrowed} mutable executor borrows (not fresh checkouts)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

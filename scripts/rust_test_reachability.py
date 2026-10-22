"""Conservative, offline reachability proof for deleted-test Rust citations.

Read Cargo manifests and follow module declarations, without building or running
repository code. This is deliberately not a Rust compiler: macro inclusion,
cfg_attr, feature/platform/custom cfg predicates and custom harnesses cannot
prove a citation. Only unconditional code and predicates composed of test,
all(), any() and not() are accepted. CI invokes each target explicitly, so
Cargo's default target-selection flags (test/bench) do not exclude a target.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
import re
import tomllib


VISIBILITY = r"(?:pub(?:\([^)]*\))?\s+)?"
MODULE = re.compile(VISIBILITY + r"mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*$")
FUNCTION = re.compile(VISIBILITY + r"(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*[(<]")
ATTRIBUTE = re.compile(r"#\s*(!?)\s*\[")


def manifest(path: Path) -> dict:
    with path.open("rb") as source:
        return tomllib.load(source)


def cargo_test_targets(root: Path) -> Iterator[tuple[tuple[str, str, str], Path]]:
    """Roots selected by the shard runner, limited to provable workspace members."""
    root = root.resolve()
    workspace = manifest(root / "Cargo.toml")
    settings = workspace.get("workspace", {})
    members = {root} if "package" in workspace else set()
    for pattern in settings.get("members", []):
        members.update(path.resolve() for path in root.glob(pattern) if path.is_dir())
    for pattern in settings.get("exclude", []):
        members.difference_update(path.resolve() for path in root.glob(pattern))
    for directory in sorted(members):
        if not directory.is_relative_to(root):
            raise ValueError("Cargo workspace member leaves the repository")
        data = manifest(directory / "Cargo.toml")
        package = data["package"]
        edition = package.get("edition", "2015")
        if isinstance(edition, dict):
            edition = settings.get("package", {}).get("edition", "2015")
        auto = edition != "2015" or not any(kind in data for kind in ("lib", "bin", "test", "bench", "example"))
        features = {"default"} if "default" in data.get("features", {}) else set()
        pending = list(data.get("features", {}).get("default", []))
        while pending:
            feature = pending.pop()
            if feature not in features:
                features.add(feature)
                pending.extend(data.get("features", {}).get(feature, []))
        for kind, folder, flag in (
            ("lib", "src", "autolib"), ("bin", "src/bin", "autobins"),
            ("test", "tests", "autotests"), ("bench", "benches", "autobenches"),
            ("example", "examples", "autoexamples"),
        ):
            inferred: dict[str, Path] = {}
            if kind == "lib":
                inferred[package["name"].replace("-", "_")] = directory / "src/lib.rs"
            else:
                for path in sorted((directory / folder).glob("*.rs")):
                    inferred[path.stem] = path
                for path in sorted((directory / folder).glob("*/main.rs")):
                    inferred[path.parent.name] = path
                if kind == "bin" and (directory / "src/main.rs").is_file():
                    inferred[package["name"]] = directory / "src/main.rs"
            targets = {name: {"path": str(path)} for name, path in inferred.items()} if package.get(flag, auto) else {}
            explicit = [data[kind]] if kind == "lib" and kind in data else data.get(kind, [])
            if kind == "lib" and explicit:
                targets = {}
            for target in explicit:
                name = target.get("name", package["name"].replace("-", "_"))
                path = directory / target["path"] if "path" in target else inferred.get(name)
                if kind == "lib" and path is None:
                    path = directory / "src/lib.rs"
                # An explicit target can rename an inferred source file as
                # well as override the settings of an inferred target name.
                if path is not None:
                    targets = {key: value for key, value in targets.items() if Path(value["path"]).resolve() != path.resolve()}
                targets[name] = {**target, "path": str(path) if path is not None else ""}
            for name, target in targets.items():
                if target.get("harness", True) is not True or not set(target.get("required-features", [])).issubset(features):
                    continue
                path = Path(target["path"]).resolve()
                if path.is_relative_to(root) and path.is_file():
                    yield (package["name"], kind, name), path


def cfg_enabled(expression: str) -> bool | None:
    """Prove target-independent test predicates; unknown is never treated as false."""
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*|[^\s]", expression)
    position = 0

    def parse() -> bool:
        nonlocal position
        if position >= len(tokens):
            raise ValueError("missing cfg predicate")
        name = tokens[position]
        position += 1
        if name == "test":
            return True
        if name not in ("all", "any", "not") or position >= len(tokens) or tokens[position] != "(":
            raise ValueError("unsupported cfg predicate")
        position += 1
        values = []
        while position < len(tokens) and tokens[position] != ")":
            values.append(parse())
            if position < len(tokens) and tokens[position] == ",":
                position += 1
            elif position >= len(tokens) or tokens[position] != ")":
                raise ValueError("malformed cfg predicate")
        if position >= len(tokens):
            raise ValueError("unclosed cfg predicate")
        position += 1
        if name == "not":
            if len(values) != 1:
                raise ValueError("not requires one predicate")
            return not values[0]
        return all(values) if name == "all" else any(values)

    try:
        value = parse()
        return value if position == len(tokens) else None
    except ValueError:
        return None


def attributes_enabled(attributes: list[str]) -> bool:
    for attribute in attributes:
        name = re.match(r"\s*([A-Za-z_][A-Za-z0-9_:]*)", attribute)
        if name is None:
            return False
        if name[1] == "cfg":
            predicate = re.fullmatch(r"\s*cfg\s*\((.*)\)\s*", attribute, re.DOTALL)
            if predicate is None or cfg_enabled(predicate[1]) is not True:
                return False
        elif name[1] not in (
            "path", "test", "tokio::test", "ignore", "should_panic", "allow", "warn", "deny", "forbid",
            "expect", "doc", "deprecated", "rustfmt::skip", "no_implicit_prelude",
        ):
            # In particular, cfg_attr or an unknown procedural attribute can
            # remove/rewrite the module or function, or change its source path.
            return False
    return True


class RustReachability:
    def __init__(self, root: Path, mask_code: Callable[[str], str]) -> None:
        self.root = root.resolve()
        self.mask_code = mask_code
        self.functions: dict[Path, dict[int, set[tuple[str, str, str, str]]]] = {}
        self.visited: set[tuple[tuple[str, str, str], Path, Path, str]] = set()
        for self.target, path in cargo_test_targets(self.root):
            self.file(path, path.parent)

    def file(self, path: Path, module_dir: Path, prefix: str = "", ancestors: frozenset[Path] = frozenset()) -> None:
        path = path.resolve()
        key = (self.target, path, module_dir.resolve(), prefix)
        if not path.is_relative_to(self.root) or key in self.visited or path in ancestors:
            return
        self.visited.add(key)
        text = path.read_text(encoding="utf-8")
        code = self.mask_code(text)
        # Delimiter pairs let the walk skip function bodies, macro token trees,
        # impls and other non-module scopes rather than trusting indentation.
        pairs: dict[int, int] = {}
        stack: list[int] = []
        for index, character in enumerate(code):
            if character in "([{":
                stack.append(index)
            elif character in ")]}":
                if not stack or {"(": ")", "[": "]", "{": "}"}[code[stack[-1]]] != character:
                    return
                pairs[stack.pop()] = index
        if stack:
            return
        self.scope(path, text, code, pairs, 0, len(code), module_dir, path.parent, prefix, ancestors | {path})

    def scope(
        self, path: Path, text: str, code: str, pairs: dict[int, int], start: int, end: int,
        module_dir: Path, path_dir: Path, prefix: str, ancestors: frozenset[Path],
    ) -> None:
        cursor = start
        attributes: list[str] = []
        while cursor < end:
            if code[cursor].isspace() or code[cursor] == ";":
                cursor += 1
                continue
            attribute = ATTRIBUTE.match(code, cursor)
            if attribute:
                close = pairs[attribute.end() - 1]
                value = code[attribute.end():close]
                if attribute[1]:
                    if not attributes_enabled([value]):
                        return
                else:
                    attributes.append(value)
                cursor = close + 1
                continue
            item_start = cursor
            while cursor < end and code[cursor] not in "{;":
                cursor = pairs[cursor] + 1 if code[cursor] in "([" else cursor + 1
            if cursor == end:
                return
            header = code[item_start:cursor].strip()
            module = MODULE.fullmatch(header)
            enabled = attributes_enabled(attributes)
            if enabled and module:
                name = module[1]
                custom_path = None
                for value in attributes:
                    matched = re.fullmatch(r"\s*path\s*=\s*(~+)\s*", value)
                    if matched:
                        # Recover just this literal from the original source.
                        begin = code.rfind(value, start, item_start) + matched.start(1)
                        try:
                            custom_path = tomllib.loads("path = " + text[begin:begin + len(matched[1])])["path"]
                        except tomllib.TOMLDecodeError:
                            enabled = False
                    elif re.match(r"\s*path\b", value):
                        enabled = False
                if enabled and code[cursor] == "{":
                    child_dir = path_dir / custom_path if custom_path is not None else module_dir / name
                    self.scope(path, text, code, pairs, cursor + 1, pairs[cursor], child_dir, child_dir, prefix + name + "::", ancestors)
                elif enabled:
                    candidates = (
                        [path_dir / custom_path] if custom_path is not None
                        else [module_dir / f"{name}.rs", module_dir / name / "mod.rs"]
                    )
                    candidates = [candidate for candidate in candidates if candidate.is_file()]
                    if len(candidates) == 1:
                        child = candidates[0]
                        child_dir = child.parent if custom_path is not None or child.name == "mod.rs" else child.with_suffix("")
                        self.file(child, child_dir, prefix + name + "::", ancestors)
            elif enabled and FUNCTION.match(header):
                body = cursor + 1
                while body < end:
                    if code[body].isspace():
                        body += 1
                        continue
                    inner = ATTRIBUTE.match(code, body)
                    if inner is None or not inner[1]:
                        break
                    close = pairs[inner.end() - 1]
                    enabled = enabled and attributes_enabled([code[inner.end():close]])
                    body = close + 1
                if enabled:
                    line = code.count("\n", 0, item_start) + 1
                    name = FUNCTION.match(header)[1]
                    self.functions.setdefault(path, {}).setdefault(line, set()).add((*self.target, prefix + name))
            attributes = []
            cursor = pairs[cursor] + 1 if code[cursor] == "{" else cursor + 1

    def occurrences(self, path: Path) -> dict[int, set[tuple[str, str, str, str]]]:
        return self.functions.get(path.resolve(), {})

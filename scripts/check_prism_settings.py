#!/usr/bin/env python3
"""Keep the native environment inventory and retired-setting guidance honest."""
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
NAME = re.compile(r"\bPRISM_[A-Z0-9_]+\b")
HISTORICAL = re.compile(r"<!-- retired-setting: (PRISM_[A-Z0-9_]+) -->")
OPERATOR_FILES = ("scripts/check-env.sh", ".env.example", "PRISM.md", "README.md")
GUIDANCE_TREES = ("doc", "docs")


def inventory(path):
    return {line for line in path.read_text().splitlines() if line.startswith("PRISM_")}


def guidance_paths(root):
    """Operator-facing files, excluding anything git ignores as generated output."""
    paths = [root / name for name in OPERATOR_FILES]
    paths.extend(sorted((root / "crates").glob("*/README.md")))
    for tree in GUIDANCE_TREES:
        paths.extend(sorted((root / tree).rglob("*")))
    paths = [path for path in paths if path.is_file()]
    try:
        # Outside a git checkout this exits non-zero with no output, so nothing is skipped.
        result = subprocess.run(
            ["git", "-C", str(root), "check-ignore", "-z", "--stdin"],
            input="\0".join(str(path.relative_to(root)) for path in paths),
            capture_output=True, text=True, check=False)
        ignored = {root / name for name in result.stdout.split("\0") if name}
    except FileNotFoundError:
        ignored = set()
    return [path for path in paths if path not in ignored]


def check(root):
    source = root / "crates/qbit-prism-server/src"
    known = inventory(source / "config/native-settings.txt")
    retired = inventory(source / "config/retired-settings.txt")
    errors = []
    overlap = known & retired
    if overlap:
        errors.append(f"native and retired inventories overlap: {', '.join(sorted(overlap))}")
    referenced = set()
    for path in source.rglob("*.rs"):
        if ("tests" in path.parts or "bin" in path.parts or path.stem.endswith("tests")
                or path.name in {"capacity.rs", "environment.rs"}):
            continue
        # Runtime modules put inline unit-test modules after their implementation;
        # a cfg(test) declaration of an external module does not end runtime code.
        runtime = re.split(r"#\[cfg\(test\)\]\s*mod\s+\w+\s*\{", path.read_text())[0]
        referenced.update(re.findall(r'"(PRISM_[A-Z0-9_]+)"', runtime))
    generated = {
        f"PRISM_PUBLIC_{kind}CACHE_{suffix}_SECONDS"
        for kind in ("", "CONFIG_", "AGGREGATE_", "ARTIFACT_")
        for suffix in ("TTL", "STALE_WHILE_REVALIDATE")
    }
    generated.update(f"{name}_FILE" for name in (
        "PRISM_MANIFEST_SIGNING_SEED_HEX",
        "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
        "PRISM_OPERATOR_BEARER_TOKEN",
    ))
    missing = referenced - known
    if missing:
        errors.append(f"native settings missing from inventory: {', '.join(sorted(missing))}")
    unused = known - referenced - generated
    if unused:
        errors.append(f"inventory settings without a native reader: {', '.join(sorted(unused))}")
    for path in guidance_paths(root):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            # A patch's removed lines document removal, not live recommendations.
            if path.suffix == ".patch" and line.startswith("-"):
                continue
            historical = set(HISTORICAL.findall(line)) if path.suffix == ".md" else set()
            body = HISTORICAL.sub("", line)
            for name in sorted((set(NAME.findall(body)) & retired) - historical):
                errors.append(f"{path.relative_to(root)}:{number}: retired setting presented as live: {name}")
    return errors


def main():
    errors = check(ROOT)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("PRISM setting inventories and operator guidance agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())

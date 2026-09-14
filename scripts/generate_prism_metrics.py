#!/usr/bin/env python3
"""Generate the sole PRISM inventory from registry descriptors and compatibility annotations."""

import argparse
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
START = "<!-- generated-inventory:start -->"
END = "<!-- generated-inventory:end -->"


def generate():
    source = (ROOT / "crates/qbit-prism-server/src/metrics/registry.rs").read_text()
    labels_source = (ROOT / "crates/qbit-prism-server/src/metrics/labels.rs").read_text()
    metadata = json.loads((ROOT / "docs/prism-metric-metadata.json").read_text())
    enums = {
        name: re.findall(r'=> "([^"]+)"', body)
        for name, body in re.findall(r"labels!\((\w+)\s*\{(.*?)\}\);", labels_source, re.S)
    }
    registry = re.findall(
        r'\w+: (Counter|Gauge|Histogram), "([^"]+)", "([^"]+)";', source
    )
    suffixes = {suffix for _, suffix, _ in registry}
    native_metadata = metadata["native"]
    unknown_native = sorted(set(native_metadata) - suffixes)
    if unknown_native:
        raise SystemExit("metadata names absent from registry: " + ", ".join(unknown_native))
    rows = []
    for kind, suffix, meaning in registry:
        annotation = metadata["native"].get(suffix, {})
        label_parts = []
        for key, enum in annotation.get("labels", {}).items():
            if enum not in enums:
                raise SystemExit(f"metadata label enum {enum!r} for {suffix!r} is absent from labels.rs")
            label_parts.append(f"`{key}=" + ",".join(enums[enum]) + "`")
        labels = "; ".join(label_parts) or "none"
        if annotation.get("status"):
            meaning += " " + annotation["status"]
        legacy = ", ".join(f"`{name}`" for name in annotation.get("replaces", [])) or "none"
        rows.append(("qbit_prism_" + suffix, kind.lower(), labels, "run", meaning, legacy))
    for name, annotation in metadata["public"].items():
        if not name.startswith("qbit_prism_"):
            raise SystemExit(f"public metadata family is not a PRISM family: {name}")
        if not annotation.get("role") or not annotation.get("type") or not annotation.get("meaning"):
            raise SystemExit(f"public metadata for {name!r} is missing type, role or meaning")
        rows.append((name, annotation["type"], annotation["labels"],
                     annotation["role"], annotation["meaning"], f"`{name}`"))
    assert len(rows) == len({row[0] for row in rows}), "duplicate family"
    lines = [START, "<!-- Run: python3 scripts/generate_prism_metrics.py -->", "",
             "| Family | Type | Labels | Role | Meaning / status | 2.x.x name replaced |",
             "| --- | --- | --- | --- | --- | --- |"]
    for name, *columns in sorted(rows):
        lines.append("| `" + name + "` | " + " | ".join(columns) + " |")
    return "\n".join(lines) + "\n\n" + END


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    path = ROOT / "docs/prism-native-metrics.md"
    old = path.read_text()
    before, rest = old.split(START)
    _, after = rest.split(END)
    new = before + generate() + after
    if args.check:
        if new != old:
            raise SystemExit("PRISM inventory is stale: run python3 scripts/generate_prism_metrics.py")
    else:
        path.write_text(new)


if __name__ == "__main__":
    main()

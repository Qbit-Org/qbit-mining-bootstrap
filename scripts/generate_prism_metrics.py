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
    rows = []
    for kind, suffix, meaning in re.findall(
        r'\w+: (Counter|Gauge|Histogram), "([^"]+)", "([^"]+)";', source
    ):
        annotation = metadata["native"].get(suffix, {})
        labels = "; ".join(
            f"`{key}=" + ",".join(enums[enum]) + "`"
            for key, enum in annotation.get("labels", {}).items()
        ) or "none"
        if annotation.get("status"):
            meaning += " " + annotation["status"]
        legacy = ", ".join(f"`{name}`" for name in annotation.get("replaces", [])) or "none"
        rows.append(("qbit_prism_" + suffix, kind.lower(), labels, "run", meaning, legacy))
    for name, annotation in metadata["public"].items():
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

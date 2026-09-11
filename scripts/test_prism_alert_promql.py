#!/usr/bin/env python3
"""Check the authored PromQL and unknown/stale/availability contracts with promtool."""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    return json.loads((ROOT / path).read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promtool", default=shutil.which("promtool"))
    args = parser.parse_args()
    if not args.promtool:
        parser.error("install promtool or pass --promtool /path/to/promtool")
    rules = [rule for path in ["docs/prism-native-alert-rules.json",
                              "docs/prism-postgres-alert-rules.json"]
             for rule in load(path)["rules"]]
    expressions = {rule["title"]: rule["expr"].replace("__NETWORK__", "mainnet") for rule in rules}
    tests = []
    for scenario in load("tests/fixtures/prism-alert-scenarios.json"):
        tests.append({
            "name": scenario["name"], "interval": "1m",
            "input_series": [{"series": metric, "values": f"{value}+0x6"}
                             for metric, value in scenario["series"].items()],
            "promql_expr_test": [{
                "expr": "sum(" + expressions[name] + ")", "eval_time": "6m",
                "exp_samples": [] if value is None else [{"labels": "{}", "value": value}],
            } for name, value in scenario["expect"].items()],
        })
    with tempfile.TemporaryDirectory(prefix="prism-alert-promql-") as directory:
        path = Path(directory)
        # JSON is valid YAML; no Python YAML dependency is needed by this check.
        (path / "rules.yml").write_text(json.dumps({"groups": [{
            "name": "prism-contract", "rules": [{"record": name.replace(" ", "_"), "expr": expr}
                                                for name, expr in expressions.items()],
        }]}))
        (path / "tests.yml").write_text(json.dumps({"evaluation_interval": "1m", "tests": tests}))
        subprocess.run([args.promtool, "check", "rules", str(path / "rules.yml")], check=True)
        subprocess.run([args.promtool, "test", "rules", str(path / "tests.yml")], check=True)
    print(f"{len(rules)} expressions; {len(tests)} scenarios; "
          f"{sum(len(test['promql_expr_test']) for test in tests)} assertions passed")


if __name__ == "__main__":
    main()

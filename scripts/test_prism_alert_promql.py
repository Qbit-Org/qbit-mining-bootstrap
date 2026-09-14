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
    parser.add_argument("--scenarios", type=Path,
                        default=ROOT / "tests/fixtures/prism-alert-scenarios.json")
    args = parser.parse_args()
    if not args.promtool:
        parser.error("install promtool or pass --promtool /path/to/promtool")
    rules = [rule for path in ["docs/prism-native-alert-rules.json",
                              "docs/prism-postgres-alert-rules.json"]
             for rule in load(path)["rules"]]
    expressions = {rule["title"]: rule["expr"].replace("__NETWORK__", "mainnet") for rule in rules}
    tests = []
    for scenario in json.loads(args.scenarios.read_text()):
        tests.append({
            "name": scenario["name"], "interval": "1s",
            "input_series": [{"series": metric, "values": value if isinstance(value, str) else f"{value}+0x360"}
                             for metric, value in scenario["series"].items()],
            "promql_expr_test": [{
                "expr": "sum(" + expressions[name] + ")", "eval_time": scenario.get("eval_time", "6m"),
                "exp_samples": [] if value is None else [{"labels": "{}", "value": value}],
            } for name, value in scenario["expect"].items()],
        })
        if "alerts" in scenario:
            tests[-1]["alert_rule_test"] = [{
                "eval_time": scenario.get("eval_time", "6m"), "alertname": name,
                "exp_alerts": [{"exp_labels": {}, "exp_annotations": {}}] if active else [],
            } for name, active in scenario["alerts"].items()]
    with tempfile.TemporaryDirectory(prefix="prism-alert-promql-") as directory:
        path = Path(directory)
        # JSON is valid YAML; no Python YAML dependency is needed by this check.
        checked_rules = [{"record": "prism_contract_" + name.replace(" ", "_"), "expr": expr}
                         for name, expr in expressions.items()]
        for rule in load("docs/prism-postgres-alert-rules.json")["rules"]:
            assert rule["evaluator"] == "gt" and rule["threshold"] == 0
            checked_rules.append({"alert": rule["title"], "expr": f"({expressions[rule['title']]}) > 0",
                                  "for": rule["for"]})
        (path / "rules.yml").write_text(json.dumps({"groups": [{
            "name": "prism-contract", "rules": checked_rules,
        }]}))
        (path / "tests.yml").write_text(json.dumps({"rule_files": [str(path / "rules.yml")],
                                                  "evaluation_interval": "1s", "tests": tests}))
        subprocess.run([args.promtool, "check", "rules", str(path / "rules.yml")], check=True)
        subprocess.run([args.promtool, "test", "rules", str(path / "tests.yml")], check=True)
    print(f"{len(rules)} expressions; {len(tests)} scenarios; "
          f"{sum(len(test['promql_expr_test']) for test in tests)} assertions passed")


if __name__ == "__main__":
    main()

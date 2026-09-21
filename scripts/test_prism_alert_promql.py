#!/usr/bin/env python3
"""Check the authored PromQL and unknown/stale/availability contracts with promtool."""

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from prism_pool_wait_scenarios import (
    NEGATIVE_CONTROL_SCENARIOS, TITLE as POOL_WAIT_TITLE,
    old_pool_wait_expression, pool_wait_tests,
)

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    return json.loads((ROOT / path).read_text())


def check_pool_wait_negative_control(promtool, path, alert, expression, tests):
    """The original guards must fail these otherwise passing positive cases."""
    old_alert = dict(alert, expr=f"({old_pool_wait_expression(expression)}) > 0")
    negative_tests = [{key: value for key, value in test.items() if key != "promql_expr_test"}
                      for test in tests if test["name"] in NEGATIVE_CONTROL_SCENARIOS]
    assert len(negative_tests) == len(NEGATIVE_CONTROL_SCENARIOS)
    rules_path, tests_path = path / "old-pool-rule.yml", path / "old-pool-tests.yml"
    rules_path.write_text(json.dumps({"groups": [{"name": "old-pool-rule", "rules": [old_alert]}]}))
    tests_path.write_text(json.dumps({"rule_files": [str(rules_path)],
                                     "evaluation_interval": "1s", "tests": negative_tests}))
    subprocess.run([promtool, "check", "rules", str(rules_path)], check=True)
    result = subprocess.run([promtool, "test", "rules", str(tests_path)],
                            capture_output=True, text=True)
    output = result.stdout + result.stderr
    # The same inputs and expectations have already passed with the new rule;
    # only its expression changes. Do not accept a parser or process failure.
    assert result.returncode == 1 and re.search(r"got:\s*\[\]", output) and all(
        name in output for name in NEGATIVE_CONTROL_SCENARIOS
    ), "old pool rule did not produce the expected false negatives:\n" + output
    print(f"Original pool rule fails {len(negative_tests)} required positive scenarios, as expected")


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
    pool_rule = next(rule for rule in rules if rule["title"] == POOL_WAIT_TITLE)
    pool_alert, pool_tests = pool_wait_tests(pool_rule, expressions[POOL_WAIT_TITLE])
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
                "exp_alerts": [{"exp_labels": labels, "exp_annotations": {}}
                               for labels in (active if isinstance(active, list) else ([{}] if active else []))],
            } for name, active in scenario["alerts"].items()]
    with tempfile.TemporaryDirectory(prefix="prism-alert-promql-") as directory:
        path = Path(directory)
        # JSON is valid YAML; no Python YAML dependency is needed by this check.
        checked_rules = [{"record": "prism_contract_" + name.replace(" ", "_"), "expr": expr}
                         for name, expr in expressions.items()]
        checked_rules.append(pool_alert)
        tests.extend(pool_tests)
        for rule in rules:
            if not (rule["uid"].startswith("qbit-prism-revision-work-") or rule in load("docs/prism-postgres-alert-rules.json")["rules"]):
                continue
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
        check_pool_wait_negative_control(args.promtool, path, pool_alert,
                                         expressions[POOL_WAIT_TITLE], pool_tests)
    print(f"{len(rules)} expressions; {len(tests)} scenarios; "
          f"{sum(len(test['promql_expr_test']) for test in tests)} expression assertions; "
          f"{sum(len(test.get('alert_rule_test', [])) for test in tests)} alert assertions passed")


if __name__ == "__main__":
    main()

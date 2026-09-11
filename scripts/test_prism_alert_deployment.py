#!/usr/bin/env python3
"""Render the review-only patch and check provisioning against its frozen source.

Requires Jinja2 and PyYAML. Uses synthetic deployment variables and the unchanged
shared Grafana macro from --snapshot; does not contact Grafana or qbit-tools.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

import jinja2
from jinja2.meta import find_undeclared_variables
import yaml

from generate_prism_alerts import ROOT, deployment_template, load


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    args = parser.parse_args()
    manifest = load("tests/fixtures/prism-deployed-alerts.json")
    relative = Path("ansible/roles/qbit_monitoring_stack/templates/grafana-alert-rules-prism.yml.j2")
    original = (args.snapshot / relative.name).read_bytes()
    assert hashlib.sha256(original).hexdigest() == manifest["sha256"]
    generated = deployment_template(original.decode())
    with tempfile.TemporaryDirectory(prefix="prism-provisioning-") as directory:
        target = Path(directory) / relative
        target.parent.mkdir(parents=True)
        target.write_bytes(original)
        patch = str(ROOT / "docs/prism-alert-rules-qbit-tools.patch")
        subprocess.run(["git", "apply", "--check", patch], cwd=directory, check=True)
        subprocess.run(["git", "apply", patch], cwd=directory, check=True)
        assert target.read_text() == generated, "patch differs from the specification"

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(args.snapshot),
                             undefined=jinja2.StrictUndefined)
    env.filters.update(bool=bool, quote=json.dumps, to_json=json.dumps)
    needed = set()
    for template in [original.decode(), generated,
                     (args.snapshot / "grafana-alert-rule-group.yml.j2").read_text()]:
        needed |= find_undeclared_variables(env.parse(template))
    variables = {key: 1 for key in needed if key != "undef"}
    gates = [key for key in variables if key.endswith(("_enabled", "_effective"))]
    for key in variables:
        if key in gates:
            variables[key] = True
        elif key.endswith("_window"):
            variables[key] = "5m"
        elif key.endswith("_for"):
            variables[key] = "3m"
        elif key.endswith("_ratio"):
            variables[key] = 0.5
        elif "evaluation_interval" in key:
            variables[key] = "1m"
    variables.update(qbit_monitoring_stack_network="mainnet",
                     qbit_monitoring_stack_grafana_dashboard_folder="Qbit",
                     qbit_monitoring_stack_prism_alert_preview_publication_warning_seconds="1",
                     qbit_monitoring_stack_prism_alert_preview_publication_critical_seconds="5",
                     qbit_monitoring_stack_prism_alert_lease_monitor_wake_recovery_threshold=0.5,
                     qbit_monitoring_stack_alert_rule_group_name="qbit-prism-alerts")
    external = {row["uid"] for row in manifest["alerts"] if row["external"]}
    all_old = {row["uid"] for row in manifest["alerts"]}
    all_new = external | {row["uid"] for path in ["docs/prism-native-alert-rules.json",
                                                 "docs/prism-postgres-alert-rules.json"]
                          for row in load(path)["rules"]}

    def rules(document):
        rows = [rule for group in document.get("groups", []) or [] for rule in group["rules"]]
        result = {rule["uid"]: rule for rule in rows}
        assert len(rows) == len(result), "duplicate UIDs"
        return result

    combinations = [{}, {key: False for key in gates}] + [{key: False} for key in gates]
    for index, overrides in enumerate(combinations):
        values = {**variables, **overrides}
        old = yaml.safe_load(env.from_string(original.decode()).render(**values))
        new = yaml.safe_load(env.from_string(generated).render(**values))
        before, after = rules(old), rules(new)
        assert new["apiVersion"] == 1
        assert before.keys() & external == after.keys() & external, overrides
        for uid in before.keys() & external:
            assert before[uid] == after[uid], (uid, overrides)
        deletions = [row["uid"] for row in new["deleteRules"]]
        assert len(deletions) == len(set(deletions)) == 27
        assert all(row["orgId"] == 1 for row in new["deleteRules"])
        assert set(deletions) == all_old - all_new
        assert not after.keys() & set(deletions)
        if index == 0:
            assert len(before) == 78 and len(after) == 61
        if index == 1:
            assert len(new["groups"]) == 1 and len(after) == 2
        for rule in load("docs/prism-postgres-alert-rules.json")["rules"]:
            rendered = after[rule["uid"]]
            assert rendered["noDataState"] == rendered["execErrState"] == "Alerting"
            assert rendered["for"] == "1m"
    assert (args.snapshot / relative.name).read_bytes() == original
    print(f"Patch applies cleanly; {len(combinations)} Jinja gate combinations passed; "
          "78 original / 61 proposed rules; 34 external definitions preserved; 27 explicit deletions")


if __name__ == "__main__":
    main()

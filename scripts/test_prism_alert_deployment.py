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
import re

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
    native_uids = {row["uid"] for row in load("docs/prism-native-alert-rules.json")["rules"]}

    def rules(document):
        rows = [rule for group in document.get("groups", []) or [] for rule in group["rules"]]
        result = {rule["uid"]: rule for rule in rows}
        assert len(rows) == len(result), "duplicate UIDs"
        return result

    combinations = [{}, {key: False for key in gates}] + [{key: False} for key in gates]
    duration = re.compile(r"^[0-9]+(?:ms|s|m|h|d|w|y)$")
    for index, overrides in enumerate(combinations):
        values = {**variables, **overrides}
        old = yaml.safe_load(env.from_string(original.decode()).render(**values))
        new = yaml.safe_load(env.from_string(generated).render(**values))
        before, after = rules(old), rules(new)
        for rule in after.values():
            assert duration.fullmatch(str(rule["for"])), (rule["uid"], rule["for"])
            assert "{{" not in str(rule["for"]) and '"' not in str(rule["for"])
        assert new["apiVersion"] == 1
        assert before.keys() & external == after.keys() & external, overrides
        for uid in before.keys() & external:
            assert before[uid] == after[uid], (uid, overrides)
        deletions = [row["uid"] for row in new["deleteRules"]]
        assert len(deletions) == len(set(deletions))
        assert all(row["orgId"] == 1 for row in new["deleteRules"])
        assert set(deletions) == (all_old - all_new) | (native_uids - after.keys()), overrides
        assert not after.keys() & set(deletions)
        if index == 0:
            assert len(before) == 78 and len(after) == 79
            assert len(deletions) == 26
            assert after["qbit-prism-candidate-oldest-critical"]["labels"]["severity"] == "critical"
            assert after["qbit-prism-candidate-oldest-critical"]["labels"].get("page") == "true"
            assert after["qbit-prism-semantic-coverage-critical"]["labels"]["severity"] == "critical"
            assert after["qbit-prism-semantic-coverage-critical"]["labels"].get("page") == "true"
            # #493: every native paging rule dwells, stays quiet on no data or
            # evaluation errors, and conjoins an availability gate on its own
            # target. External (sidecar) paging rules keep their own producers.
            paging = {uid: rule for uid, rule in after.items()
                      if uid in native_uids and rule["labels"].get("page") == "true"}
            assert {"qbit-prism-candidate-oldest-critical", "qbit-prism-semantic-coverage-critical",
                    "qbit-prism-revision-work-pending-critical",
                    "qbit-prism-candidate-landing-failed-critical",
                    "qbit-prism-work-refresh-stalled-critical",
                    "qbit-prism-database-unavailable",
                    "qbit-prism-block-submission-held"} <= set(paging), sorted(paging)
            for uid, rule in paging.items():
                assert rule["for"] != "0s", uid
                assert rule["noDataState"] == "OK", uid
                assert rule["execErrState"] == "OK", uid
                expr = json.dumps(rule)
                assert " and on(job, instance, network) " in expr, uid
                fresh = "qbit_prism_metrics_snapshot_available{" in expr and "qbit_prism_metrics_snapshot_stale{" in expr
                scraped = "up{" in expr and "== 1" in expr
                assert fresh or scraped, uid
            assert paging["qbit-prism-revision-work-pending-critical"]["for"] == "1m"
            # #525: a refresh that keeps failing pages on this frontend's own gauge.
            assert paging["qbit-prism-work-refresh-stalled-critical"]["for"] == "1m"
            # #581: a database outage pages on its own rule, on the live
            # collector gauge and a successful scrape, never on the snapshot.
            database = json.dumps(paging["qbit-prism-database-unavailable"])
            assert paging["qbit-prism-database-unavailable"]["for"] == "2m"
            assert "collector=\\\"database\\\"" in database
            assert "qbit_prism_metrics_snapshot" not in database
            # #666: a frontend held by PRISM_BLOCK_SUBMIT_ENABLED pages a minute
            # after it starts, on a successful scrape: the gauge is fixed at
            # start, so a stale snapshot must not hide it.
            held = json.dumps(paging["qbit-prism-block-submission-held"])
            assert paging["qbit-prism-block-submission-held"]["for"] == "1m"
            assert "qbit_prism_block_submission_enabled{" in held and "!= bool 1" in held
            assert "qbit_prism_metrics_snapshot" not in held
            # #493: the tracking-unknown and unlanded warnings dwell and never page.
            for uid, dwell in [("qbit-prism-revision-work-unknown", "2m"),
                               ("qbit-prism-accepted-block-unlanded", "3m"),
                               ("qbit-prism-block-candidate-stuck", "3m"),
                               ("qbit-prism-candidate-landing-failed", "3m")]:
                assert uid not in paging and after[uid]["labels"].get("page") is None, uid
                assert after[uid]["labels"]["severity"] == "warning" and after[uid]["for"] == dwell, uid
            assert after["qbit-prism-revision-work-unknown"]["noDataState"] == "Alerting"
            assert after["qbit-prism-accepted-block-unlanded"]["noDataState"] == "OK"
            assert after["qbit-prism-revision-work-pending"]["for"] == "0s"
            # #529: a found block offered without a confirmed standby copy
            # warns at once, never pages, and is quiet on no data.
            standby = after["qbit-prism-block-offer-standby-unconfirmed"]
            assert "qbit-prism-block-offer-standby-unconfirmed" not in paging
            assert standby["labels"]["severity"] == "warning" and standby["labels"].get("page") is None
            assert standby["for"] == "0s" and standby["noDataState"] == "OK"
            assert 'outcome=~\\"absent|lagging|failed\\"' in json.dumps(standby), json.dumps(standby)
            # #493: the candidate paging rule reads the unacknowledged age, not the all-unfinished age.
            critical = json.dumps(after["qbit-prism-candidate-oldest-critical"])
            assert "qbit_prism_block_candidate_oldest_unacknowledged_seconds{" in critical
            assert " or on(job, instance, network) qbit_prism_block_candidate_oldest_pending_seconds{" in critical, "no mixed-version fallback"
            assert paging["qbit-prism-candidate-landing-failed-critical"]["for"] == "1m"
            # #602: slow landing windows ticket and warn, and never page; the
            # pool-wide resubmission reject ratio warns.
            for uid, severity, dwell in [("qbit-prism-landing-ack-p99-slow", "ticket", "1m"),
                                         ("qbit-prism-landing-ack-p99-high", "warning", "1m"),
                                         ("qbit-prism-pool-resubmission-reject-ratio", "warning", "5m")]:
                assert uid not in paging and after[uid]["labels"].get("page") is None, uid
                assert after[uid]["labels"]["severity"] == severity and after[uid]["for"] == dwell, uid
                assert after[uid]["noDataState"] == after[uid]["execErrState"] == "OK", uid
        if index == 1:
            assert len(new["groups"]) == 1 and len(after) == 2
        for rule in load("docs/prism-postgres-alert-rules.json")["rules"]:
            rendered = after[rule["uid"]]
            assert rendered["noDataState"] == rendered["execErrState"] == "Alerting"
            assert rendered["for"] == "1m"
    tuned = dict(variables, qbit_monitoring_stack_prism_alert_connected_clients_for="9m",
                 qbit_monitoring_stack_prism_alert_block_candidate_age_for="7m",
                 qbit_monitoring_stack_prism_alert_semantic_coverage_warning_for="11m")
    rendered = yaml.safe_load(env.from_string(generated).render(**tuned))
    tuned_rules = rules(rendered)
    assert tuned_rules["qbit-prism-connected-clients"]["for"] == "9m"
    assert tuned_rules["qbit-prism-block-candidate-oldest"]["for"] == "7m"
    assert tuned_rules["qbit-prism-semantic-work-coverage"]["for"] == "11m"
    assert (args.snapshot / relative.name).read_bytes() == original
    print(f"Patch applies cleanly; {len(combinations)} Jinja gate combinations passed; "
          "78 original / 79 proposed rules; 34 external definitions preserved; "
          "26 baseline deletions plus every native UID disabled by its gate")


if __name__ == "__main__":
    main()

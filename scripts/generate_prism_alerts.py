#!/usr/bin/env python3
"""Render migration tables and a review-only qbit-tools patch from the frozen snapshot."""

import argparse
import difflib
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
START = "<!-- generated-migration:start -->"
END = "<!-- generated-migration:end -->"


def load(path):
    return json.loads((ROOT / path).read_text())


def specification_digest():
    # Include the source inputs and the generator/fixture contract itself.  A
    # stale generator or deployment manifest must invalidate the checked-in
    # patch just as surely as a changed rule specification.
    return hashlib.sha256(b"".join((ROOT / path).read_bytes() for path in [
        "docs/prism-native-alert-rules.json", "docs/prism-postgres-alert-rules.json",
        "docs/prism-postgres-exporter-queries.yaml", "docs/prism-alert-migration.json",
        "tests/fixtures/prism-deployed-alerts.json", "scripts/generate_prism_alerts.py",
    ])).hexdigest()


def postgres_rules():
    """D3: compare durable WAL prefixes, not the last reported replay latency."""
    target = 'job="qbit-postgres-primary",network="__NETWORK__"'
    standby = target + ',application_name="prism_standby_1"'
    identity = "job, instance, network, application_name"

    def metric(name):
        return "pg_stat_replication_" + name + "{" + standby + "}"

    def unknown(sample, age=0):
        # An absent/negative sample or missing recent history remains unknown.
        # With 1s scrapes allow at most one missed interval (2s sample age).
        elapsed = f"(time() - timestamp({sample}))"
        return f"(max(({sample} < bool 0) + ({elapsed} < bool {age}) + ({elapsed} > bool {age + 2})) or vector(1))"

    common = [f"(max({name}{{{target}}} != bool {healthy}) or vector(1))"
              for name, healthy in [("up", 1), ("pg_up", 1), ("pg_exporter_last_scrape_error", 0)]]
    common += [f"((count(up{{{target}}}) != bool 1) or vector(1))", unknown(metric("count"))]
    primary_hi, primary_lo = [metric("primary_flush_lsn_" + half) for half in ["hi", "lo"]]
    replay_hi, replay_lo = [metric("replay_lsn_" + half) for half in ["hi", "lo"]]
    old_hi, old_lo = primary_hi + " offset 5s", primary_lo + " offset 5s"
    # Lexicographic comparison retains all 64 LSN bits, including low-word wrap.
    behind = (f"({replay_hi} < bool on({identity}) {old_hi}) + "
              f"(({replay_hi} == bool on({identity}) {old_hi}) * on({identity}) "
              f"({replay_lo} < bool on({identity}) {old_lo}))")
    lag_terms = common + [unknown(value) for value in [primary_hi, primary_lo, replay_hi, replay_lo]]
    lag_terms += [unknown(value, 5) for value in [old_hi, old_lo]]
    lag_terms += [unknown(metric("async")), f"(max({metric('async')} != bool 1) or vector(1))"]
    lag_terms += [f"(max({behind}) or vector(1))"]
    disconnected_terms = common + [f"(max({metric('count')} != bool 1) or vector(1))"]
    original = load("docs/prism-postgres-alert-rules.json")["rules"]
    for rule, terms in zip(original, [lag_terms, disconnected_terms], strict=True):
        rule["expr"] = "clamp_max(" + " + ".join(terms) + ", 1)"
    original[0]["description"] = (
        "D3: standby replay has not reached the primary durable WAL prefix observed five seconds earlier, "
        "or position/history/exporter observations are unknown, stale or failed, or the required async topology is invalid. "
        "Deployment-provided: requires primary SQL query metrics and one-second scrapes; verify in #281/#291. "
        "Idle caught-up positions are healthy even when PostgreSQL reports NULL replay_lag. "
        "The five-second comparison is sampled, with up to one scrape interval of timing uncertainty.")
    original[1]["description"] = (
        "D3: the primary reports zero or ambiguous rows for prism_standby_1, or exporter/query observations "
        "are missing, stale or failed. Deployment-provided: requires the primary exporter and one-second "
        "scrapes; verify in #281/#291. This is the dedicated asynchronous HA standby, not the public read replica.")
    return {"rules": original}


def migration_tables():
    migration = load("docs/prism-alert-migration.json")
    lines = [START, "<!-- Run: python3 scripts/generate_prism_alerts.py -->", "",
             "## Every deployed alert", "",
             "78 definitions: **22 migrated, 34 unchanged external, 22 with no replacement**.",
             "This includes definitions behind deployment Jinja flags; the snapshot does not record their effective values.", "",
             "| Deployed UID | Deployed alert | Native rule / disposition | Reason |",
             "| --- | --- | --- | --- |"]
    for alert in migration["deployed_alerts"]:
        replacement = ", ".join(f"`{name}`" for name in alert["replacement"]) or "no replacement"
        lines.append(f"| `{alert['uid']}` | {alert['title']} | {replacement} ({alert['disposition']}) | {alert['reason']} |")
    lines += ["", "## Every historical name", "",
              "All **46** distinct tokens in the historical appendix are mapped below, including histogram suffixes and names retained by native producers.", "",
              "| Historical 2.x.x name | Native replacement / disposition | Reason |",
              "| --- | --- | --- |"]
    for metric in migration["retired_names"]:
        replacement = ", ".join(f"`{name}`" for name in metric["replacement"]) or "no replacement"
        lines.append(f"| `{metric['name']}` | {replacement} | {metric['reason']} |")
    lines += ["", "## Native rule evidence", "",
              "The executable PromQL and Grafana conditions are in [prism-native-alert-rules.json](prism-native-alert-rules.json).",
              "These are the only rewritten native rules; the historical appendix is not an active rule specification.", "",
              "| Rule | Threshold evidence | Producer contract |", "| --- | --- | --- |"]
    for rule in load("docs/prism-native-alert-rules.json")["rules"]:
        refs = ", ".join(f"[{ref.split('/')[-1]}](../{ref})" for ref in rule["producer_refs"])
        lines.append(f"| {rule['title']} | {rule['basis']} | {refs} |")
    return "\n".join(lines) + "\n\n" + END


def jinja_rules(rules):
    # Documentation-only fields never enter the Grafana provisioning contract.
    rows = [{key: value for key, value in rule.items()
             if key not in {"role", "basis", "producer_refs"}} for rule in rules]
    text = json.dumps(rows, indent=2).replace("__NETWORK__", '" ~ qbit_monitoring_stack_network ~ "')
    # Preserve the deployment's per-network tuning surface for the native
    # rules whose operational bounds were configurable in 2.x.x.  The JSON
    # contract keeps the current defaults; the generated Jinja expression
    # resolves overrides at render time.
    overrides = {
        "qbit-prism-connected-clients": ("qbit_monitoring_stack_prism_alert_connected_clients_threshold", "qbit_monitoring_stack_prism_alert_connected_clients_for", "10"),
        "qbit-prism-candidate-oldest": ("qbit_monitoring_stack_prism_alert_candidate_oldest_warning_seconds", "qbit_monitoring_stack_prism_alert_candidate_oldest_warning_for", "15"),
        "qbit-prism-candidate-oldest-critical": ("qbit_monitoring_stack_prism_alert_candidate_oldest_critical_seconds", "qbit_monitoring_stack_prism_alert_candidate_oldest_critical_for", "60"),
        "qbit-prism-semantic-work-coverage": ("qbit_monitoring_stack_prism_alert_semantic_coverage_warning_ratio", "qbit_monitoring_stack_prism_alert_semantic_coverage_warning_for", "0.95"),
        "qbit-prism-semantic-coverage-critical": ("qbit_monitoring_stack_prism_alert_semantic_coverage_critical_ratio", "qbit_monitoring_stack_prism_alert_semantic_coverage_critical_for", "0.5"),
    }
    for uid, (threshold_var, for_var, literal) in overrides.items():
        marker = '\"uid\": \"' + uid + '\"'
        start = text.find(marker)
        if start < 0:
            continue
        end = text.find('\"uid\": \"', start + len(marker))
        block_end = end if end >= 0 else len(text)
        block = text[start:block_end]
        block = block.replace(literal, '{{ ' + threshold_var + ' | default(' + literal + ') }}', 1)
        block = block.replace('\"for\": \"3m\"', '\"for\": \"{{ ' + for_var + ' | default(\"3m\") }}\"', 1)
        text = text[:start] + block + text[block_end:]
    return text


def deployment_template(snapshot):
    manifest = load("tests/fixtures/prism-deployed-alerts.json")
    assert hashlib.sha256(snapshot.encode()).hexdigest() == manifest["sha256"], "snapshot differs from the reviewed qbit-tools commit"
    rules = load("docs/prism-native-alert-rules.json")["rules"]
    ha = load("docs/prism-postgres-alert-rules.json")["rules"]
    external_uids = {row["uid"] for row in manifest["alerts"] if row["external"]}
    kept_uids = external_uids | {rule["uid"] for rule in rules}
    deleted = [row["uid"] for row in manifest["alerts"] if row["uid"] not in kept_uids]
    # Preserve deployment-owned sidecar, backup and Docker lifecycle definitions.
    blocks = re.findall(r"\n  \{\n.*?\n  \}", snapshot, re.S)
    sidecar = [block for block in blocks if '"prism_stratum_rule"' in block]
    variables = [line for line in snapshot.splitlines()
                 if line.startswith(("{% set prism_stratum_", "{% set prism_backup_"))]
    tail = snapshot[snapshot.index("{% if qbit_monitoring_stack_prism_backup_alerts_effective"):]
    tail = tail[:tail.index('{% set qbit_monitoring_stack_alert_rule_group_name =')]
    output = ["{# Native 3.x.x cutover draft, bootstrap #279. Apply only with the native image.",
              "   Specification SHA-256: " + specification_digest(),
              "   Generated from the reviewed a007a142 snapshot; backup, sidecar and lifecycle rules retain their producers.",
              "   Native numeric bounds marked provisional must be measured in #291 before production cutover.",
              "   The D3 group requires PRIMARY postgres_exporter query metrics; see bootstrap docs/prism-alert-migration.md.",
              "   Retired UIDs are explicitly deleted: omitting a provisioned rule would leave it active in Grafana.",
              "#}"] + variables
    output += ["{# D3 deployment-provided queries.yaml stanza; install on the PRIMARY exporter:",
               (ROOT / "docs/prism-postgres-exporter-queries.yaml").read_text(), "#}",
               "{% set prism_native_rules = " + jinja_rules([r for r in rules if r["role"] == "run"]) + " %}",
               "{% set prism_public_read_alert_rules = " + jinja_rules([r for r in rules if r["role"] == "public-api"]) + " %}",
               "{% set prism_database_alert_rules = " + jinja_rules(ha) + " %}",
               "{% set prism_external_stratum_rules = [" + ",".join(sidecar) + "\n] %}",
               "{% set alert_rules = prism_native_rules if (qbit_monitoring_stack_prism_alerts_enabled | bool) else [] %}",
               "{% if (qbit_monitoring_stack_prism_alerts_enabled | bool) and (qbit_monitoring_stack_prism_stratum_alerts_enabled | bool) %}",
               "{% set alert_rules = alert_rules + prism_external_stratum_rules %}", "{% endif %}",
               "{% if qbit_monitoring_stack_prism_public_read_alerts_enabled | bool %}",
               "{% set alert_rules = alert_rules + prism_public_read_alert_rules %}", "{% endif %}",
               "{% if not (qbit_monitoring_stack_prism_alert_connected_clients_enabled | bool) %}",
               "{% set alert_rules = alert_rules | rejectattr('connected_clients_rule', 'defined') | list %}", "{% endif %}",
               tail,
               '{% set qbit_monitoring_stack_alert_rule_group_name = "qbit-prism-alerts" %}',
               '{% from "grafana-alert-rule-group.yml.j2" import render_alert_rule_group with context %}',
               "{% set native_partitions = namespace(standard=[], paging=[]) %}",
               "{% for rule in alert_rules %}",
               "{% if rule.severity == 'critical' and (rule.labels | default({})).get('page') == 'true' %}",
               "{% set native_partitions.paging = native_partitions.paging + [rule] %}",
               "{% else %}", "{% set native_partitions.standard = native_partitions.standard + [rule] %}",
               "{% endif %}", "{% endfor %}", "apiVersion: 1", "deleteRules:"]
    for uid in deleted:
        output += ["  - orgId: 1", "    uid: " + uid]
    output += ["{# Disabled native rules must be deleted too, including reused legacy UIDs. #}",
               "{% set emitted_uids = alert_rules | map(attribute='uid') | list %}",
               "{% for rule in prism_native_rules + prism_public_read_alert_rules %}",
               "{% if rule.uid not in emitted_uids %}",
               "  - orgId: 1", "    uid: {{ rule.uid }}", "{% endif %}", "{% endfor %}",
               "groups:", "{% if native_partitions.standard %}",
               "{{ render_alert_rule_group('qbit-prism-alerts', qbit_monitoring_stack_grafana_alert_standard_evaluation_interval, native_partitions.standard) }}", "{% endif %}",
               "{% if native_partitions.paging %}",
               "{{ render_alert_rule_group('qbit-prism-alerts-paging', qbit_monitoring_stack_grafana_alert_paging_evaluation_interval, native_partitions.paging) }}", "{% endif %}",
               "{{ render_alert_rule_group('qbit-prism-postgres-ha-alerts', qbit_monitoring_stack_grafana_alert_standard_evaluation_interval, prism_database_alert_rules) }}"]
    return "\n".join(output) + "\n"


def update(path, value, check):
    if check:
        if path.read_text() != value:
            raise SystemExit(f"stale generated artifact: {path.relative_to(ROOT)}")
    else:
        path.write_text(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, help="read-only snapshot directory, required to regenerate the deployment patch")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    update(ROOT / "docs/prism-postgres-alert-rules.json", json.dumps(postgres_rules(), indent=2) + "\n", args.check)
    doc = ROOT / "docs/prism-alert-migration.md"
    before, rest = doc.read_text().split(START)
    _, after = rest.split(END)
    update(doc, before + migration_tables() + after, args.check)
    if args.check:
        assert specification_digest() in (ROOT / "docs/prism-alert-rules-qbit-tools.patch").read_text(), "deployment patch must be regenerated from the current specifications"
    if args.snapshot:
        snapshot = (args.snapshot / "grafana-alert-rules-prism.yml.j2").read_text()
        target = "ansible/roles/qbit_monitoring_stack/templates/grafana-alert-rules-prism.yml.j2"
        patch = "".join(difflib.unified_diff(snapshot.splitlines(True), deployment_template(snapshot).splitlines(True),
                                           fromfile="a/" + target, tofile="b/" + target))
        update(ROOT / "docs/prism-alert-rules-qbit-tools.patch", patch, args.check)


if __name__ == "__main__":
    main()

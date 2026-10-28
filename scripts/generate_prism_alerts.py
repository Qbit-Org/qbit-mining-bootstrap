#!/usr/bin/env python3
"""Render the revision-work rule expressions, migration tables and a review-only qbit-tools patch from the frozen snapshot."""

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


# #758: the revision-work rules' miner gates come from here, so the warnings
# and their critical twin cannot drift apart.
TARGET = 'job="qbit-prism",network="__NETWORK__"'
TARGETS = "on(job, instance, network)"
# Longer than the critical's 307-second floor plus its one-minute dwell, so a
# stall that drives every miner away before it pages still warns for minutes;
# a backup that has been idle longer stays quiet. The rule texts say it too.
MINER_WINDOW_MINUTES = 15
MINER_WINDOW = f"{MINER_WINDOW_MINUTES}m"
REVISION_WORK_PREFIX = ("Acceptance is observed locally; completion requires an actual successful "
                        "mining.notify write at the post-landing revision. ")


def prism(name):
    return name + "{" + TARGET + "}"


def miners_now():
    """At least one authorized miner on the reporting frontend now."""
    return f"({prism('qbit_prism_authorized_clients')} > 0)"


def miners_recently():
    """At least one authorized miner on the reporting frontend within MINER_WINDOW."""
    return f"(max_over_time({prism('qbit_prism_authorized_clients')}[{MINER_WINDOW}]) > 0)"


def miner_gated(term):
    # The term where the frontend served miners recently, else a present zero,
    # so the rule's missing-series fallbacks still key on the term's presence.
    return f"(({term} and {TARGETS} {miners_recently()}) or {TARGETS} (0 * {term}))"


def native_rules():
    """Generate the revision-work expressions; everything else is authored in the file."""
    spec = load("docs/prism-native-alert-rules.json")
    assert spec["network_placeholder"] == "__NETWORK__"
    pending = prism("qbit_prism_accepted_block_revision_work_pending_seconds")
    unknown = prism("qbit_prism_accepted_block_revision_work_tracking_unknown")
    up = prism("up")
    configured = f"or {TARGETS} (0 * {up} + 1)"
    expressions = {
        "PrismAcceptedRevisionWorkPending":
            f"clamp_max(({miner_gated(f'({pending} > bool 1)')} {configured}) + {TARGETS} ({up} == bool 0), 1)",
        "PrismAcceptedRevisionWorkTrackingUnknown":
            f"clamp_max({miner_gated(f'({pending} < bool 0)')} + {TARGETS} "
            f"({miner_gated(f'({unknown} > bool 0)')} {configured}), 1)",
        "PrismAcceptedRevisionWorkPendingCritical":
            f"(({pending} >= bool 307) and {TARGETS} ({pending} >= 0)) "
            f"and {TARGETS} {miners_now()} and {TARGETS} ({up} == 1)",
    }
    window = f"{MINER_WINDOW_MINUTES} minutes"
    bases = {
        "PrismAcceptedRevisionWorkPending":
            "Provisional warning at more than one second, with no additional dwell: the #275 budget is a target, "
            "not a measured production percentile; measure in #291. Missing series on a configured target and "
            f"failed scrapes also warn. Counts only on a frontend that has had an authorized miner within the last {window} "
            "(#758): a frontend with no miners has no mining.notify write that can close its wait, so a long-idle "
            "backup frontend would otherwise warn after every pool block, while a stall that drives every miner away "
            f"keeps warning here for {window} after the last one leaves, after the critical rule, gated on miners now, "
            "has resolved. The missing-series and failed-scrape warnings are not gated; a miner count absent for the "
            "whole window closes the gate, as an absent count does for the critical rule. Unknown tracking (a -1 age, "
            "the tracking-unknown gauge, or a missing unknown gauge) is owned by PrismAcceptedRevisionWorkTrackingUnknown, "
            "which dwells so a single failed refresh does not warn (#493); a known pending age beside unknown tracking "
            "still warns here and remains eligible for the critical rule. An offer the node accepted but has not reported "
            "active is an unlanded acceptance, excluded from this age and owned by PrismAcceptedBlockUnlanded (#493).",
        "PrismAcceptedRevisionWorkTrackingUnknown":
            "Unknown or incomplete local landing tracking (#493 point 7): the pending age is -1 (no known wait remains "
            "but tracking is unknown), the tracking-unknown gauge is one beside any known age, or the unknown gauge is "
            "missing on a configured target during a mixed-version rollout. The -1 age and the tracking-unknown gauge "
            f"count only on a frontend that has had an authorized miner within the last {window}, the pending warning's "
            "gate (#758). When this frontend cannot know the first revision that includes a block's landing, a committed "
            "revision proven to include it ends the unknown wait at the first eligible write at or above it, without a "
            "sample (#758): any local settlement that found the block already confirmed proves its own revision, and a "
            "first confirmation that committed without a bump (the fatal early return) proves only the revisions after "
            "it. A serving frontend so clears the wait at its next such write; a long-idle backup keeps it, quietly, "
            "until a miner receives work. The missing-gauge warning is not gated. Two-minute dwell: a failed template "
            "refresh marks tracking unknown only until the next successful refresh, one poll interval later (two seconds "
            "by default), so a single transient refresh error cannot span the eight consecutive 15-second scrapes this "
            "dwell requires, while a lost or cancelled settlement COMMIT reply persists until a later settlement of the "
            "block proves a covering revision and this frontend writes at it, and an unsettled orphan verdict, "
            "bounded-tracking saturation and lost ordering history persist until terminal evidence or restart; all still "
            "warn. No-data and evaluation errors alert so a missing measurement stays visible after the same dwell. "
            "Non-paging: the critical rule owns known stalls and gates out unknown ages itself; the pending warning owns "
            "the no-dwell budget signal. Measure the dwell in #291.",
        "PrismAcceptedRevisionWorkPendingCritical":
            "Provisional critical floor of 307 seconds comes from the #413 incident duration, not an early-warning result. "
            "Gates (#493): the age must be known (not -1), the target scraped, and at least one authorized miner on the "
            "reporting frontend, because a frontend with no connected miners has no mining.notify write that can close "
            "its wait and would otherwise page after every pool block. The pending gauge is a live overlay rendered on "
            "every scrape, so it is not gated on the metrics snapshot: a database-pool stall that also stalls the health "
            "publisher must still page. The miner count is a snapshot family and may be a few refresh intervals old while "
            "the snapshot is stale; a false page on a frontend whose miners left inside that window is accepted in exchange "
            "for never missing a real stall. If every miner leaves a frontend during a stall, this page resolves and "
            "nothing else pages for that frontend: the coverage rules cannot fire with zero authorized clients and the "
            "connected-clients rule is a cluster-wide non-paging warning; the pending warning keeps reporting the age "
            f"for {window} after the last miner leaves (#758), and both report it again once a miner reconnects. "
            "One-minute dwell: the gauge is monotonic during a stall, so the dwell only absorbs a brief gate gap and stays "
            "well under the five-minute hold that delayed #413. No-data and evaluation errors are OK: the warning rules "
            "own missing or unknown measurements. The floor and dwell are literals in this specification, not deployment "
            "variables. A lost race on the offering frontend never counts here (#493 point 1): an offer the node accepted "
            "without an active-chain observation is an unlanded acceptance, excluded from the known pending age until an "
            "active-chain observation lands it or its orphan is proven, and reported by accepted_block_unlanded_seconds "
            "and PrismAcceptedBlockUnlanded instead. Measure thresholds and scrape/evaluation delay in #291; no "
            "operational deployment is claimed.",
    }
    rules = [rule for rule in spec["rules"] if rule["title"] in expressions]
    assert len(rules) == len(expressions)
    for rule in rules:
        rule["expr"] = expressions[rule["title"]]
        rule["basis"] = bases[rule["title"]]
        rule["description"] = REVISION_WORK_PREFIX + bases[rule["title"]]
    return spec


# How the operator docs state the window; checked against MINER_WINDOW_MINUTES.
WINDOW_PROSE = [r"authorized\s+miner\s+in\s+the\s+last\s+(\d+)\s+minutes",
                r"for\s+(\d+)\s+minutes\s+after\s+(?:the|its)\s+last",
                r"qbit_prism_authorized_clients\[(\d+)m\]"]


def check_window_prose():
    """#758: the operator docs state the recent-miner window the rules use."""
    for path in ["docs/prism-alert-migration.md", "docs/prism-native-metrics.md"]:
        text = (ROOT / path).read_text()
        found = [int(match.group(1)) for pattern in WINDOW_PROSE for match in re.finditer(pattern, text)]
        if not found:
            raise SystemExit(f"{path} no longer states the recent-miner window")
        if any(minutes != MINER_WINDOW_MINUTES for minutes in found):
            raise SystemExit(f"{path} states a recent-miner window other than MINER_WINDOW_MINUTES={MINER_WINDOW_MINUTES}")


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
             "78 definitions: **27 migrated, 34 unchanged external, 17 with no replacement**.",
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
    overrides = {
        "qbit-prism-connected-clients": ("qbit_monitoring_stack_prism_alert_connected_clients_threshold", "qbit_monitoring_stack_prism_alert_connected_clients_for", "10"),
        "qbit-prism-block-candidate-oldest": ("qbit_monitoring_stack_prism_alert_block_candidate_age_warning_seconds", "qbit_monitoring_stack_prism_alert_block_candidate_age_for", "15"),
        "qbit-prism-candidate-oldest-critical": ("qbit_monitoring_stack_prism_alert_block_candidate_age_critical_seconds", "qbit_monitoring_stack_prism_alert_block_candidate_age_for", "60"),
        "qbit-prism-semantic-work-coverage": ("qbit_monitoring_stack_prism_alert_semantic_coverage_warning_ratio", "qbit_monitoring_stack_prism_alert_semantic_coverage_warning_for", "0.95"),
        "qbit-prism-semantic-coverage-critical": ("qbit_monitoring_stack_prism_alert_semantic_coverage_critical_ratio", "qbit_monitoring_stack_prism_alert_semantic_coverage_critical_for", "0.5"),
    }
    canonical = {r["uid"] for r in rules}
    missing = set(overrides) - canonical
    if missing and (canonical & set(overrides)):
        raise AssertionError(f"override target UID missing from rendered rules: {sorted(missing)}")
    for uid, (threshold, dwell, literal) in overrides.items():
        marker = '"uid": "' + uid + '"'
        start = text.find(marker)
        if start < 0: continue
        end = text.find('"uid": "', start + len(marker))
        block_end = end if end >= 0 else len(text)
        block = text[start:block_end]
        expr = re.search(r'("expr": ")(.*?)(",\n)', block)
        if expr and literal in expr.group(2):
            before, after = expr.group(2).split(literal, 1)
            rebuilt = expr.group(1) + before + '" ~ (' + threshold + ' | default(' + literal + ')) ~ "' + after + expr.group(3)
            block = block[:expr.start()] + rebuilt + block[expr.end():]
        block = block.replace('"for": "3m"', '"for": (' + dwell + ' | default(\'3m\'))', 1)
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
    check_window_prose()
    update(ROOT / "docs/prism-native-alert-rules.json", json.dumps(native_rules(), indent=2) + "\n", args.check)
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

# Local PR draft — coordinator review required

Target: `Qbit-Org/qbit-mining-bootstrap`, base `3.x.x`, head
`djh58/prism-b7-alert-migration`. No push or PR creation has occurred.

Proposed title: `docs(prism): generate native metric inventory and migrate deployed alerts`

Proposed body:

The native runtime no longer emits the Python series consumed by deployed PRISM
alerts. This change generates the single 52-family inventory from the registry,
checks running `run` and `public-api` role scrapes in both directions, and maps
all 78 deployed definitions plus all 46 historical metric names. The deployment
artifact migrates 22 alert definitions, preserves 34 external definitions, and
records why 22 have no replacement; it renders 25 native/public rules and two
required primary/dedicated-standby rules alongside the external rules.

The rules preserve unknown, stale and failed observations, retain the deployed
network-wide connected-client floor, and compare durable WAL progress over time
so healthy idle replication does not fire on NULL last-reported latency. The
review-only qbit-tools diff preserves external definitions and explicitly deletes
retired UIDs and native UIDs disabled by their deployment gates. No runtime
producer, VERSION, CHANGELOG, qbit-tools checkout or deployment was changed.

Validation: all 12 ungated observability tests passed; promtool parsed all 27
expressions and passed 38 synthetic scenarios plus nine scenarios using actual
SQL observations from disposable PostgreSQL 16 primary/async-standby clusters.
The patch applies cleanly and renders successfully across ten Jinja gate
combinations with exact external-definition preservation and correct UID deletion
sets. Formatting, whitespace and both generated-artifact checks passed. See
[verification](prism-alert-verification.md) and [review ledger](prism-alert-review-ledger.json)
for the executed/not-executed record and local review findings.

Independent Fable and thermo reviews remain pending coordinator routing; GitHub
CI and bot lanes have not run because the branch is unpushed. Production exporter
integration, Grafana provisioning, the 400k regression/500k headroom load gates
and #291 qualification remain outstanding. Thresholds without native measurement
are explicitly provisional; first-offer and advisory-lock rules remain deferred.
The dedicated HA topology stays one primary plus one asynchronous standby, with
ACKs waiting for local durability only. Applying the deployment diff requires
separate operator review and the native cutover.

Closes #279.

Before publication, replace pending-lane statements with the actual independent
results and confirm the final reviewed SHA; do not infer approval from this draft.

# Validation Strategy

Validation follows risk and invalidation scope. Focused checks run per slice;
broad checks run at cumulative milestones and once more on the final tree.

## Per slice

Run:

```sh
python3 -m py_compile <changed Python modules and tests>
python3 -m unittest <direct owner tests> <tight coordinator regressions>
git diff --check
```

Inspect the complete slice diff and confirm no leaf module imports
`lab.prism.prism_coordinator`, including lazily inside a function. A leaf that
needs coordinator-owned behaviour reaches it through an injected seam that
carries the full call shape, rather than asking the coordinator for a fact and
re-deriving the decision itself. `BlockCandidatePorts.submit_candidate`, whose
production implementation is
`PrismCoordinator._land_block_candidate_submission`, is the pattern to copy:
the leaf states what it has (an already-created node submission, an
already-held disposition) and the coordinator resolves the entrypoint per
call.

The invariant covers runtime modules under `lab/prism/`. `job_build_benchmark.py`
is a standalone benchmark entrypoint rather than a leaf owner and still imports
the coordinator directly; it is the one accepted exception.

Add PostgreSQL, Rust, or process tests immediately when the slice changes those
boundaries; do not defer a directly relevant failure to a milestone.

## Cumulative PRISM regression

Use the current shard names discovered in `tests/`. At minimum include the
candidate, vardiff, retained-job, share-writer, payout, job-builder, metrics,
tip-refresh, publication-boundary, initial/reconnect, hot-path, shutdown, CTV,
progress-health, audit, and public-API suites. The exact module list may grow as
new direct owner suites are added.

## Full Python discovery

Full discovery includes fixtures that create temporary Git repositories, so
scope the global-config override to this command only:

```sh
GIT_CONFIG_GLOBAL=/dev/null \
  python3 -m unittest discover -s tests -p 'test_*.py'
```

Never export `GIT_CONFIG_GLOBAL` and never use it for repository commits.

## PostgreSQL, Rust, and live validation

Run when a milestone touches persistence, payout, shares, audit, candidates,
vardiff, or finalization:

```sh
make test-prism-postgres-ledger
cargo test -p qbit-prism
```

At the concurrency/finalization and final milestones, run:

```sh
make test-prism-stratum-regtest-live
make test-prism-stratum-postgres-regtest-live
```

If qbitd, qbit-cli, PostgreSQL, Docker, Rust, credentials, or another real
prerequisite is unavailable, record `UNAVAILABLE`, the missing prerequisite,
and the exact unrun command. Do not call it a pass.

## Docker compile, lint, and build

Lint only in Docker:

```sh
docker run --rm \
  -e PYTHONPYCACHEPREFIX=/tmp/pycache \
  -v "$PWD:/work:ro" -w /work \
  python:3.14-slim-trixie \
  python -m compileall -q docker lab tests examples scripts

docker run --rm \
  -v "$PWD:/work" -w /work \
  ghcr.io/astral-sh/ruff:0.12.5 \
  check --select E4,E7,E9,F lab/prism tests

docker build -f lab/prism/Dockerfile -t qbit-prism-refactor-check .
```

Focused slices may lint only their changed Python paths. Milestones lint the
complete PRISM paths. The image build is required after B2 and on the final
tree.

## Final structural and diff audit

Before declaring completion:

- no leaf service imports the coordinator;
- mutable domain state has one owner and no drifting scalar mirror;
- coordinator worker loops and large domain state machines have moved;
- compatibility re-exports/delegates have a demonstrated caller or are gone;
- no blocking I/O occurs under coordinator/session locks;
- queues, executors, cancellation, and shutdown joins are bounded;
- public schemas, metrics, environment defaults, and wire payloads are stable;
- `git diff --check`, status, target-branch diff, and generated-artifact hygiene
  are clean;
- cumulative regression, full discovery, PostgreSQL/Rust, Docker lint/build,
  and live-or-unavailable evidence all reflect the final tree.

## Final evidence

Recorded on 2026-08-16 on the re-derived stack tip, integrated through
`origin/1.x.x` at `36267d6` (#120). The Docker daemon on this host requires
root; Docker-backed checks ran via `sudo`.

| Check | Result |
| --- | --- |
| PRISM Python discovery (`test_prism_*.py`) | 1,601 passed |
| full Python discovery | 1,956 passed, 2 skipped (ckpool binary absent); `GIT_CONFIG_GLOBAL=/dev/null` was scoped only to this command |
| PostgreSQL ledger integration | passed via `sudo bash test/test-prism-postgres-ledger.sh`, including the typed share-replay assertions, the #120 carry-forward equivalence/drift checks, and the three A1 publication gates (transition parity, migration M0-M11, cross-process fencing C1-C3) |
| Rust workspace | 222 passed (`cargo test --locked --workspace --all-targets`), including the audit CLI canonicalizer round-trip regressions |
| Docker Python compile | passed |
| Docker Ruff (`E4,E7,E9,F`) | 54 findings, all F821 on lazily evaluated type annotations that deliberately reference coordinator-owned types without importing them (never evaluated at runtime under `from __future__ import annotations`); every removable finding was cleaned in the final ownership layer |
| PRISM image | built and tagged (`qbit-prism-refactor-check`) |
| Compose | 34 configuration-validation tests passed (mainnet contract, prism profile, mining profiles), including the metrics-refresh and #120 environment pass-throughs |
| structural and target diff audit | passed; see [Structure](structure.md) |

`make test-prism-stratum-regtest-live` and
`make test-prism-stratum-postgres-regtest-live` are `UNAVAILABLE`: their
doctor stops at `Required executable not found: qbitd` before either live
test can run. `qbitd` and `qbit-cli` are absent from the host path.

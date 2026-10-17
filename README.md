# ci-evidence

The PRISM load lanes' trend rows (#551, S6 of #487; decision 3). This orphan
branch is written only by the scheduled and `v*` tag jobs of the lane
workflows, through `scripts/prism_load_trend.py append` on `3.x.x`. It is
append-only: rows are added, never edited or removed, and a push is never
forced.

- `trend/<lane>/YYYY-MM.jsonl`: one JSON row per line, schema
  `qbit.prism.load-trend-row.v1`, month of the row's `recorded_at` (UTC).
- Raw bundles that outlive the 90-day Actions artifacts are release assets:
  the rolling `ci-evidence/YYYY-MM` pre-release, or a tag's release for the
  full suite. A row's `asset_url` points at its bundle once promoted.

`scripts/prism_load_regress.py` reads these rows for the report-only
regression rule.

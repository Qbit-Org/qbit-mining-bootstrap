#!/usr/bin/env python3
"""Fast regression tests for the ckpool rejects-observability patch wiring.

These run in the default unit tier with no network or compiler. The live
behavior of the patched binary is covered by
tests/test_ckpool_rejects_observability.py via
`make test-ckpool-rejects-observability`.
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "tests"))
DOCKERFILE = ROOT_DIR / "docker" / "ckpool" / "Dockerfile"
PATCH = ROOT_DIR / "docker" / "ckpool" / "qbit-rejects-observability.patch"
BUILD_SCRIPT = ROOT_DIR / "scripts" / "build-patched-ckpool.sh"

# Applying order matters: the observability patch is generated against the
# tree that already has the regtest and signet patches applied.
PATCH_ORDER = (
    "qbit-regtest.patch",
    "qbit-signet-gbt.patch",
    "qbit-rejects-observability.patch",
)

# The stable exporter-facing bucket names written to rejects.status.
REASON_BUCKETS = (
    "accepted",
    "above_target",
    "stale",
    "duplicate",
    "invalid_job",
    "invalid_ntime",
    "invalid_version",
    "malformed",
)
BLOCK_BUCKETS = ("block_accepted", "block_rejected")

# The complete set of source lines the patch is allowed to remove. The
# observability feature must stay purely additive around share validation
# and block submission; any new removal is a conscious contract change that
# must update this list and re-run the live suite.
EXPECTED_REMOVED_LINES = sorted(
    [
        "\t\tblock_reject(bval);",
        "\t\tblock_reject(val);",
        "\tif (unlikely(!sdata->current_workbase))",
        "static void block_reject(json_t *val)",
        "static void block_reject(json_t *val);",
    ]
)


def patch_lines() -> list[str]:
    return PATCH.read_text(encoding="utf-8").splitlines()


class CkpoolRejectsPatchWiringTests(unittest.TestCase):
    def test_dockerfile_copies_and_applies_patch_in_order(self) -> None:
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")
        self.assertIn(
            "COPY docker/ckpool/qbit-rejects-observability.patch "
            "/tmp/qbit-rejects-observability.patch",
            dockerfile,
        )
        apply_positions = [dockerfile.index(f"git apply /tmp/{name}") for name in PATCH_ORDER]
        self.assertEqual(apply_positions, sorted(apply_positions))

    def test_build_script_applies_same_patches_in_same_order(self) -> None:
        script = BUILD_SCRIPT.read_text(encoding="utf-8")
        positions = [script.index(f"docker/ckpool/{name}") for name in PATCH_ORDER]
        self.assertEqual(positions, sorted(positions))
        self.assertTrue(BUILD_SCRIPT.stat().st_mode & 0o111, "build script must be executable")

    def test_patch_touches_only_stratifier(self) -> None:
        touched = {
            line.split()[-1]
            for line in patch_lines()
            if line.startswith("+++ ") or line.startswith("--- ")
        }
        self.assertEqual(touched, {"a/src/stratifier.c", "b/src/stratifier.c"})

    def test_patch_removals_stay_purely_additive(self) -> None:
        removed = sorted(
            line[1:]
            for line in patch_lines()
            if line.startswith("-") and not line.startswith("---")
        )
        self.assertEqual(removed, EXPECTED_REMOVED_LINES)

    def test_patch_never_rewrites_validation_or_submission_calls(self) -> None:
        added = [
            line[1:]
            for line in patch_lines()
            if line.startswith("+") and not line.startswith("+++")
        ]
        # The share verdict variables of parse_submit stay untouched by
        # added code: observability reads them, never writes them.
        verdict_assignment = re.compile(r"\b(result|err|submit|invalid|share)\s*=[^=]")
        for line in added:
            self.assertIsNone(
                verdict_assignment.search(line), f"added line writes a verdict: {line!r}"
            )
        for identifier in ("local_block_submit(", "new_share(", "test_blocksolve("):
            self.assertFalse(
                any(identifier in line for line in added),
                f"added code must not call {identifier}",
            )

    def test_patch_defines_every_exporter_bucket(self) -> None:
        patch_text = PATCH.read_text(encoding="utf-8")
        for bucket in REASON_BUCKETS + BLOCK_BUCKETS:
            self.assertIn(f'"{bucket}"', patch_text)
        self.assertIn("pool/rejects.status", patch_text)
        self.assertIn('"%s.tmp"', patch_text)
        self.assertIn("rename(tmpname, fname)", patch_text)

    def test_reason_buckets_match_live_suite_contract(self) -> None:
        from test_ckpool_rejects_observability import BLOCK_OUTCOMES, REASONS

        self.assertEqual(tuple(REASONS), REASON_BUCKETS)
        self.assertEqual(tuple(BLOCK_OUTCOMES), BLOCK_BUCKETS)

    def test_makefile_wires_live_suite(self) -> None:
        makefile = (ROOT_DIR / "Makefile").read_text(encoding="utf-8")
        self.assertIn("test-ckpool-rejects-observability:", makefile)
        self.assertIn("scripts/build-patched-ckpool.sh", makefile)
        self.assertIn("QBIT_CKPOOL_REJECTS_BIN", makefile)

    def test_readme_documents_exporter_contract(self) -> None:
        readme = (ROOT_DIR / "ckpool" / "README.md").read_text(encoding="utf-8")
        self.assertIn("rejects.status", readme)
        for bucket in REASON_BUCKETS + BLOCK_BUCKETS:
            self.assertIn(bucket, readme)


if __name__ == "__main__":
    unittest.main()

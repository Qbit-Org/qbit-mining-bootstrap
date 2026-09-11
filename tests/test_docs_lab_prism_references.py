#!/usr/bin/env python3
"""Guard against docs that name Python coordinator code deleted in #244 (issue #303).

Two contracts over every tracked file under ``docs/``:

a. No runnable ``python -m lab.…`` or ``python lab/….py`` command invokes a
   module or script absent from the branch. There is no allowlist.
b. Every ``lab/prism/…`` path or ``lab.prism.…`` module reference resolves to a
   tracked file or directory. GitHub links pinned to a 40-hex commit SHA are
   stable history and exempt. Pre-existing residue that #303 declares out of
   scope is ratcheted in ``RATCHET``: the count may shrink, never grow.
"""

from __future__ import annotations

import re
import subprocess
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# Dangling-reference ceilings for residue #303 leaves to its own follow-up.
# `docs/prism-overload-alerts.md` and `docs/prism-coordinator-refactor/` are
# #244 residue with the same problem as capacity-readiness and get the same
# treatment as a separate change; `docs/prism-rust-migration.md` describes the
# 2.x.x lane, where the named module still exists. Each ceiling is the count
# measured when this guard landed. Lower a ceiling when its file improves;
# never raise one. `docs/prism-capacity-readiness.md` is deliberately absent.
RATCHET = {
    "docs/prism-overload-alerts.md": 32,
    "docs/prism-coordinator-refactor/README.md": 1,
    "docs/prism-coordinator-refactor/a1-audit-artifacts.md": 1,
    "docs/prism-coordinator-refactor/b3-decision.md": 3,
    "docs/prism-coordinator-refactor/validation.md": 1,
    "docs/prism-rust-migration.md": 1,
}

PATH_REFERENCE = re.compile(r"lab/prism(?:/[A-Za-z0-9_][A-Za-z0-9_.\-]*)*")
MODULE_REFERENCE = re.compile(r"\blab\.prism(?:\.[A-Za-z_][A-Za-z0-9_]*)*\b")
# `python3 -u -m lab.a.b` and `python lab/a/b.py`; single-letter flags allowed.
MODULE_COMMAND = re.compile(
    r"\bpython3?(?:\s+-[A-Za-z])*\s+-m\s+(lab(?:\.[A-Za-z_][A-Za-z0-9_]*)+)\b"
)
SCRIPT_COMMAND = re.compile(r"\bpython3?(?:\s+-[A-Za-z])*\s+(lab/[A-Za-z0-9_./\-]+\.py)\b")
PINNED_GITHUB_URL = re.compile(
    r"github\.com/[^/\s]+/[^/\s]+/(?:blob|tree|raw)/[0-9a-f]{40}/$"
)


def tracked_paths() -> frozenset[str]:
    """Every tracked file plus every directory prefix of one."""
    listing = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, stdout=subprocess.PIPE, check=True
    ).stdout.decode()
    paths = set()
    for path in filter(None, listing.split("\0")):
        parts = path.split("/")
        paths.update("/".join(parts[:depth]) for depth in range(1, len(parts) + 1))
    return frozenset(paths)


def module_candidates(module: str) -> tuple[str, ...]:
    relative = module.replace(".", "/")
    return (f"{relative}.py", f"{relative}/__init__.py", relative)


def dead_commands(text: str, tracked: frozenset[str]) -> list[tuple[int, str, str]]:
    """``(line, command, missing path)`` for each runnable command with no target."""
    found = []
    for number, line in enumerate(text.splitlines(), 1):
        for match in MODULE_COMMAND.finditer(line):
            candidates = module_candidates(match.group(1))[:2]
            if not any(candidate in tracked for candidate in candidates):
                found.append((number, match.group(0), candidates[0]))
        for match in SCRIPT_COMMAND.finditer(line):
            if match.group(1) not in tracked:
                found.append((number, match.group(0), match.group(1)))
    return found


def dangling_references(text: str, tracked: frozenset[str]) -> list[tuple[int, str]]:
    """``(line, reference)`` for each ``lab/prism`` mention that resolves to nothing."""
    found = []
    for number, line in enumerate(text.splitlines(), 1):
        for match in PATH_REFERENCE.finditer(line):
            if PINNED_GITHUB_URL.search(line[: match.start()]):
                continue
            reference = match.group(0).rstrip(".")
            if reference not in tracked:
                found.append((number, reference))
        for match in MODULE_REFERENCE.finditer(line):
            if not any(c in tracked for c in module_candidates(match.group(0))):
                found.append((number, match.group(0)))
    return found


class DocsLabPrismReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tracked = tracked_paths()
        cls.docs = sorted(
            path
            for path in cls.tracked
            if path.startswith("docs/") and (ROOT / path).is_file()
        )
        cls.texts = {
            path: (ROOT / path).read_text(encoding="utf-8", errors="replace")
            for path in cls.docs
        }

    def test_no_doc_command_invokes_a_missing_module_or_script(self) -> None:
        dead = [
            f"{path}:{number}: {command} -> {missing}"
            for path in self.docs
            for number, command, missing in dead_commands(self.texts[path], self.tracked)
        ]
        self.assertEqual(
            dead,
            [],
            "runnable commands in docs/ invoke modules or scripts absent from the branch:\n"
            + "\n".join(dead),
        )

    def test_lab_prism_references_resolve_outside_the_ratchet(self) -> None:
        dangling = [
            f"{path}:{number}: {reference}"
            for path in self.docs
            if path not in RATCHET
            for number, reference in dangling_references(self.texts[path], self.tracked)
        ]
        self.assertEqual(
            dangling,
            [],
            "docs/ name lab/prism paths that do not exist on this branch; describe the "
            "retired Python lane in words or link a commit-pinned GitHub URL:\n"
            + "\n".join(dangling),
        )

    def test_ratcheted_files_do_not_gain_dangling_references(self) -> None:
        for path, ceiling in RATCHET.items():
            with self.subTest(path=path):
                if path not in self.texts:
                    self.fail(f"{path} is no longer tracked; remove its RATCHET entry")
                found = dangling_references(self.texts[path], self.tracked)
                if len(found) > ceiling:
                    self.fail(
                        f"{path} gained dangling lab/prism references ({len(found)} > "
                        f"{ceiling}); repair them rather than raising the ceiling:\n"
                        + "\n".join(f"{path}:{n}: {ref}" for n, ref in found)
                    )
                if len(found) < ceiling:
                    self.fail(
                        f"{path} improved ({len(found)} dangling references, ceiling "
                        f"{ceiling}); lower RATCHET[{path!r}] to {len(found)} in this test"
                    )

    def test_capacity_readiness_is_not_ratcheted(self) -> None:
        self.assertNotIn("docs/prism-capacity-readiness.md", RATCHET)


class ScannerTests(unittest.TestCase):
    TRACKED = frozenset({"lab", "lab/prism", "lab/prism/Dockerfile"})

    def references(self, text: str) -> list[str]:
        return [ref for _, ref in dangling_references(text, self.TRACKED)]

    def commands(self, text: str) -> list[str]:
        return [missing for _, _, missing in dead_commands(text, self.TRACKED)]

    def test_bare_path_to_deleted_module_is_dangling(self) -> None:
        self.assertEqual(self.references("Rendered by `lab/prism/metrics.py`."), ["lab/prism/metrics.py"])

    def test_commit_pinned_github_url_is_exempt(self) -> None:
        sha = "504846cc0b72e8f86ed17f896d4ccbbe196a31dc"
        text = f"[2.x](https://github.com/o/r/blob/{sha}/lab/prism/observability.py#L317)."
        self.assertEqual(self.references(text), [])
        self.assertEqual(
            self.references("https://github.com/o/r/blob/main/lab/prism/observability.py"),
            ["lab/prism/observability.py"],
        )

    def test_existing_paths_resolve_including_sentence_ending_period(self) -> None:
        self.assertEqual(self.references("Only `lab/prism` holds lab/prism/Dockerfile."), [])
        self.assertEqual(self.references("The module `lab.prism` remains."), [])

    def test_dotted_module_to_deleted_file_is_dangling(self) -> None:
        self.assertEqual(self.references("see lab.prism.process_telemetry"), ["lab.prism.process_telemetry"])

    def test_module_commands_with_missing_targets_are_caught(self) -> None:
        self.assertEqual(
            self.commands("python3 -m lab.prism.process_telemetry rss-bound --samples s.csv \\"),
            ["lab/prism/process_telemetry.py"],
        )
        self.assertEqual(
            self.commands('docker exec "$c" python3 -m lab.prism.process_telemetry rss-sample --pid 1'),
            ["lab/prism/process_telemetry.py"],
        )
        self.assertEqual(self.commands("python -u -m lab.tool run"), ["lab/tool.py"])
        self.assertEqual(self.commands("python3 lab/prism/storm.py --decide"), ["lab/prism/storm.py"])

    def test_commands_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py"}
        text = "python3 -m lab.prism.tool\npython -m lab.pkg\npython3 lab/prism/tool.py"
        self.assertEqual(dead_commands(text, tracked), [])


if __name__ == "__main__":
    unittest.main()

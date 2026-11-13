"""#817: the required lint job checks the generated PRISM alert rules and metric inventory.

The Rust observability tests run the same two checks in rust-tests; the lint
job's steps make a stale artifact fail fast, under its own step name. This
module pins that they run and that each check refuses a hand-edited generated
file. Without --snapshot the alert check compares only the qbit-tools patch's
specification digest, not its body.

The hand-edit tests copy into a scratch root exactly the files the generator
opens when it runs --check against this tree, recorded with an audit hook, so a
generator that starts reading another document needs no change here. That
trace needs the checked-in tree to pass, so a stale artifact also fails these
tests, saying the generator's inputs are unknown.
"""

from functools import cache
from pathlib import Path
import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"

ALERTS = "scripts/generate_prism_alerts.py"
METRICS = "scripts/generate_prism_metrics.py"
JOB_HEADER = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$")
# A job's own keys, and a step's own entries: the one on its `- ` line and
# those aligned with it. YAML allows a quoted key.
JOB_KEY = re.compile(r"^    [\"']?([A-Za-z0-9_-]+)[\"']?\s*:\s*(.*?)\s*$")
# The one condition the lint job may carry: the same-tree skip (#839). It
# skips the job only when the tree-marker job found that this merge tree
# already passed, this job included, and the aggregate check accepts a skipped
# lint job only then; tests/test_tree_pass_marker.py pins both.
SAME_TREE_SKIP = (
    "${{ !cancelled() && !(needs.tree-marker.result == 'success' "
    "&& needs.tree-marker.outputs.skip == 'true') }}"
)
STEP_ENTRY = re.compile(r"^      (?:- |  )[\"']?([A-Za-z0-9_-]+)[\"']?\s*:\s*(.*?)\s*$")
# The only keys a pinned step may carry: any other can skip it (`if`), ignore
# its failure (`continue-on-error`), or run it somewhere else (`shell`,
# `working-directory`, `env`).
PINNED_STEP_KEYS = {"name", "run"}
# Runs a generator as `python3 <script> --check` does, from the repository
# root, recording every file under that root it opens. Cached bytecode is
# looked up in an empty directory, so an imported module is read from its
# source, which is what the scratch root needs.
TRACE = r"""
import json, os, runpy, sys
root, script, out = sys.argv[1:]
opened = set()
def record(event, args):
    if event == "open" and isinstance(args[0], (str, bytes, os.PathLike)):
        path = os.path.abspath(os.fsdecode(args[0]))
        if path.startswith(root + os.sep):
            opened.add(path)
sys.dont_write_bytecode = True
sys.pycache_prefix = os.path.join(os.path.dirname(out), "pycache")
# `python3 <script>` puts the script's directory first, where `-c` put "";
# under PYTHONSAFEPATH neither adds one.
if not sys.flags.safe_path:
    sys.path[0] = os.path.dirname(script)
sys.argv = [script, "--check"]
sys.addaudithook(record)
code = 0
try:
    runpy.run_path(script, run_name="__main__")
except SystemExit as exit:
    code = exit.code
finally:
    with open(out, "w") as file:
        json.dump(sorted(opened), file)
sys.exit(code)
"""


def run_check(root: Path, script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(root / script), "--check"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )


@cache
def inputs(script: str) -> tuple[str, ...]:
    """Every file under ROOT that `script --check` opens on this tree."""
    with tempfile.TemporaryDirectory() as directory:
        out = Path(directory) / "opened.json"
        run = subprocess.run(
            [sys.executable, "-c", TRACE, str(ROOT), str(ROOT / script), str(out)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if run.returncode != 0:
            raise AssertionError(f"{script} --check fails on this tree, so its inputs are unknown:\n{run.stderr}")
        opened = [Path(path) for path in json.loads(out.read_text())]
    found = sorted(
        path.relative_to(ROOT).as_posix()
        for path in opened
        if path.is_relative_to(ROOT) and path.is_file() and "__pycache__" not in path.parts
    )
    if script not in found:
        raise AssertionError(f"the trace of {script} --check did not record the script itself: {found}")
    return tuple(found)


def copy_inputs(root: Path, script: str) -> subprocess.CompletedProcess[str]:
    """Copy what `script --check` reads into `root`; the copy's own check."""
    for relative in inputs(script):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, target)
    return run_check(root, script)


def jobs(workflow: str) -> dict[str, list[str]]:
    """Each job under `jobs:` in ci.yml, as its lines."""
    found: dict[str, list[str]] = {}
    current = None
    in_jobs = False
    for line in workflow.splitlines():
        if line and not line[0].isspace() and not line.startswith("#"):
            in_jobs = line.rstrip() == "jobs:"
            current = None
            continue
        if not in_jobs:
            continue
        header = JOB_HEADER.match(line)
        if header:
            current = header.group(1)
            found[current] = []
        elif current is not None:
            found[current].append(line)
    return found


def top_level(workflow: str) -> list[str]:
    """The workflow's lines outside `jobs:`, where a workflow-wide `env:` sits."""
    found: list[str] = []
    in_jobs = False
    for line in workflow.splitlines():
        if line and not line[0].isspace() and not line.startswith("#"):
            in_jobs = line.rstrip() == "jobs:"
        if not in_jobs:
            found.append(line)
    return found


def steps(job: list[str]) -> list[list[str]]:
    """A job's steps, each as its lines from its `- ` line to the next step's."""
    found: list[list[str]] = []
    for line in job:
        if line.startswith("      - "):
            found.append([])
        if found:
            found[-1].append(line)
    return found


def own_entries(step: list[str]) -> list[tuple[str, str]]:
    """A step's own keys and their inline values, not those nested under them."""
    return [entry.groups() for line in step if (entry := STEP_ENTRY.match(line))]


class GeneratedArtifactCheckTests(unittest.TestCase):
    def test_the_required_lint_job_runs_both_checks(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        all_jobs = jobs(workflow)
        self.assertIn("lint-compile-compose", all_jobs)
        lint = all_jobs["lint-compile-compose"]
        # A job that may be skipped or may fail without failing the run does
        # not gate, nor one whose steps run under another default shell or in
        # another image. A job-level env is scanned for PYTHONOPTIMIZE below.
        job_entries = [key.groups() for line in lint if (key := JOB_KEY.match(line))]
        job_keys = {key for key, _ in job_entries}
        self.assertFalse(job_keys & {"continue-on-error", "defaults", "container"}, "lint-compile-compose")
        conditions = [value for key, value in job_entries if key == "if"]
        self.assertIn(conditions, ([], [SAME_TREE_SKIP]), "lint-compile-compose may only skip for a tree that passed")
        top = top_level(workflow)
        self.assertFalse([line for line in top if re.match(r"[\"']?defaults[\"']?\s*:", line)], "workflow defaults")
        lint_steps = steps(lint)
        for script in (ALERTS, METRICS):
            with self.subTest(script=script):
                command = f"python3 {script} --check"
                running = [step for step in lint_steps if ("run", command) in own_entries(step)]
                self.assertEqual(len(running), 1, f"lint-compile-compose needs exactly one step running {command}")
                keys = [key for key, _ in own_entries(running[0])]
                self.assertTrue(set(keys) <= PINNED_STEP_KEYS, f"{command} step carries {keys}")
                # A line that is not one of the step's own `key: value` entries
                # continues a value (`|| true` under the run line) or spells a
                # key another way (`? if`); either can change what runs.
                stray = [
                    line for line in running[0]
                    if line.strip() and not line.lstrip().startswith("#") and not STEP_ENTRY.match(line)
                ]
                self.assertFalse(stray, f"{command} step")
                # A step that regenerates the artifacts first leaves nothing stale to find.
                others = [
                    line for step in lint_steps if step is not running[0]
                    for line in step if script in line and not line.lstrip().startswith("#")
                ]
                self.assertFalse(others, f"another lint-compile-compose step runs {script}")
        # The digest check is a bare assert, which PYTHONOPTIMIZE disables,
        # whether it is set for the workflow, the job or the step.
        self.assertFalse(
            [line for line in top + lint if "PYTHONOPTIMIZE" in line and not line.lstrip().startswith("#")],
            "PYTHONOPTIMIZE",
        )
        # The branch-protected aggregate check waits on that job.
        self.assertIn("checks", all_jobs)
        checks = all_jobs["checks"]
        self.assertIn("    name: Lint, compile, and test", checks)
        needs = next(line for line in checks if line.startswith("    needs:"))
        self.assertIn("lint-compile-compose", re.findall(r"[A-Za-z0-9_-]+", needs.split(":", 1)[1]))

    def test_a_hand_edited_alert_migration_table_fails_the_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clean = copy_inputs(root, ALERTS)
            self.assertEqual(clean.returncode, 0, clean.stderr)
            doc = root / "docs/prism-alert-migration.md"
            start = "<!-- generated-migration:start -->"
            text = doc.read_text()
            self.assertIn(start, text)
            doc.write_text(text.replace(start, start + "\nhand edit", 1))
            result = run_check(root, ALERTS)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("stale generated artifact: docs/prism-alert-migration.md", result.stderr)

    def test_a_reformatted_alert_rule_file_fails_the_check(self) -> None:
        for name in ("prism-native-alert-rules.json", "prism-postgres-alert-rules.json"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                clean = copy_inputs(root, ALERTS)
                self.assertEqual(clean.returncode, 0, clean.stderr)
                rules = root / "docs" / name
                rules.write_text(json.dumps(json.loads(rules.read_text()), indent=4) + "\n")
                result = run_check(root, ALERTS)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"stale generated artifact: docs/{name}", result.stderr)

    def test_a_hand_edited_alert_rule_fails_the_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clean = copy_inputs(root, ALERTS)
            self.assertEqual(clean.returncode, 0, clean.stderr)
            rules = root / "docs/prism-native-alert-rules.json"
            text = rules.read_text()
            self.assertIn('"for": "', text)
            rules.write_text(text.replace('"for": "', '"for": "1', 1))
            result = run_check(root, ALERTS)
            self.assertNotEqual(result.returncode, 0)
            # The rule files are also generator inputs, so an edited value
            # surfaces as a deployment patch whose digest no longer matches.
            self.assertIn("AssertionError: deployment patch must be regenerated", result.stderr)

    def test_a_hand_edited_metric_inventory_fails_the_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clean = copy_inputs(root, METRICS)
            self.assertEqual(clean.returncode, 0, clean.stderr)
            inventory = root / "docs/prism-native-metrics.md"
            text = inventory.read_text()
            self.assertIn("| counter |", text)
            inventory.write_text(text.replace("| counter |", "| gauge |", 1))
            result = run_check(root, METRICS)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("stale", result.stderr)


if __name__ == "__main__":
    unittest.main()

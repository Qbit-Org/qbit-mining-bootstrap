#!/usr/bin/env python3
"""Guard against docs that name Python coordinator code deleted in #244 (issue #303).

Two contracts over every tracked file under ``docs/``:

a. No runnable ``python -m lab.…`` or ``python lab/….py`` command invokes a
   module or script absent from the branch, however it is wrapped across shell
   backslash continuation lines. A ``-m`` target must be a module file
   (``lab/a/b.py``) or a package with ``lab/a/b/__main__.py``: CPython refuses
   to run a package that has only ``__init__.py``, and a bare directory is a
   namespace package with the same refusal, while a module file inside such a
   directory runs. There is no allowlist. The check is lexical:
   it reads direct ``python``/``python3``/``python3.N`` invocations with their
   CPython option forms, quoted interpreter names and targets and ``./``
   prefixes, and does not
   follow ``cd``, ``PYTHONPATH`` or other environment indirection, aliases,
   or shell variables.
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


def quoted(target: str, group: str = "quote") -> str:
    """``target`` bare or in matching single or double quotes, which the shell strips.

    A mismatched quote is a shell syntax error: the line runs nothing and
    matches nothing. ``group`` names the capture that holds the quote, since a
    command pattern may quote both its interpreter and its target and ``re``
    rejects a group name used twice.
    """
    return rf"(?P<{group}>['\"]?){target}\b(?P={group})"


PATH_REFERENCE = re.compile(r"lab/prism(?:/[A-Za-z0-9_][A-Za-z0-9_.\-]*)*")
MODULE_REFERENCE = re.compile(r"\blab\.prism(?:\.[A-Za-z_][A-Za-z0-9_]*)*\b")
# `python3 -OO -X dev -m lab.a.b` and `python3.12 -Werror lab/a/b.py`. Every
# option form `python3 --help` lists may sit between the interpreter and its
# target: clustered flag letters, `-W`/`-X` with an attached or following
# argument, and `--check-hash-based-pycs <mode>` (CPython rejects the `=`
# spelling). `-c cmd` runs its argument and ends the option list, so it is
# deliberately not a prefix option. The interpreter name may itself be quoted
# (`"python3" -m lab.a.b`), which the shell strips before it runs; inside
# `sh -c "python3 -m lab.a.b"` no quote closes right after the name, so the
# quote group falls back to empty and the inner command is read as before.
# An option argument may be quoted the same way, attached or following:
# CPython 3.14 runs `-X 'dev'`, `-X"dev mode"`, `-W'error'` and
# `--check-hash-based-pycs "always"` exactly as their bare spellings, since
# the shell strips the quotes first. A bare argument therefore holds no quote
# character at all: `-X 'dev"` is an unterminated shell string, not an option.
PYTHON_FLAG = r"[bBdEhiIOPqsSuvVx]"
# A word the shell hands over as one argument: runs of unquoted characters
# and matching-quoted strings (`'error'::Warning`, `"dev mode"`), non-empty.
# Unquoted runs and quoted strings alternate rather than nest, so the pattern
# never has two ways to split one word and cannot backtrack exponentially.
OPTION_ARGUMENT = (
    r"(?=\S)"  # non-empty, and a lone quote is not a word
    r"[^\s'\"]*(?:(?:'[^']*'|\"[^\"]*\")[^\s'\"]*)*"
    r"(?!\S)"  # a word ends at whitespace, so an unmatched quote ends nothing
)
PYTHON_OPTION = (
    rf"-{PYTHON_FLAG}+"  # -O, -OO, -bb, -IsE
    rf"|-{PYTHON_FLAG}*[WX](?:{OPTION_ARGUMENT}|\s+{OPTION_ARGUMENT})"  # -Xdev, -X 'dev', -uWerror
    r"|--check-hash-based-pycs\s+" + quoted(r"(?:always|default|never)", group="pycs_quote")
)
# `-m` may close a flag cluster and take its module attached: CPython 3.12 runs
# `-mlab.x`, `-Im lab.x`, `-OOm lab.x` and `-Imlab.x` alike, while `-Wm lab.x`
# hands `m` to `-W` and treats `lab.x` as a script path.
MODULE_OPTION = rf"-{PYTHON_FLAG}*m\s*"


PYTHON_COMMAND = (
    quoted(r"\bpython(?:3(?:\.\d+)?)?", group="interpreter_quote")
    + rf"(?:\s+(?:{PYTHON_OPTION}))*"
)
# `python3 -m 'lab.a.b'` and `python3 "./lab/a/b.py"`. CPython resolves any
# `./` prefixes on a script path, so ``target`` holds the normalised path.
MODULE_COMMAND = re.compile(
    rf"{PYTHON_COMMAND}\s+{MODULE_OPTION}" + quoted(r"(?P<target>lab(?:\.[A-Za-z_][A-Za-z0-9_]*)+)")
)
SCRIPT_COMMAND = re.compile(
    rf"{PYTHON_COMMAND}\s+" + quoted(r"(?:\./)*(?P<target>lab/[A-Za-z0-9_./\-]+\.py)")
)
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
    """Every tracked path a prose ``lab.a.b`` mention may name: file, package or directory."""
    relative = module.replace(".", "/")
    return (f"{relative}.py", f"{relative}/__init__.py", relative)


def runnable_candidates(module: str) -> tuple[str, str]:
    """The paths ``python -m lab.a.b`` can execute: a module file or a package ``__main__``.

    Neither ``__init__.py`` nor a bare directory counts: CPython 3.14 answers
    both with "No module named lab.a.b.__main__; 'lab.a.b' is a package and
    cannot be directly executed" and exits 1, while ``lab/a/b.py`` runs even
    when ``lab/a`` is a namespace package without ``__init__.py``.
    """
    relative = module.replace(".", "/")
    return (f"{relative}.py", f"{relative}/__main__.py")


def shell_lines(text: str) -> list[tuple[int, str]]:
    """``(first line, text)`` per logical shell line, backslash continuations joined."""
    lines: list[tuple[int, str]] = []
    for number, line in enumerate(text.splitlines(), 1):
        if lines and lines[-1][1].endswith("\\"):
            first, head = lines[-1]
            lines[-1] = (first, head[:-1] + line)
        else:
            lines.append((number, line))
    return lines


def dead_commands(text: str, tracked: frozenset[str]) -> list[tuple[int, str, str]]:
    """``(first line, command, missing path)`` for each runnable command with no target.

    A dead ``-m`` command reports both paths that would make it runnable, joined
    by ``or``, so the reader is not sent to create ``lab/a/b.py`` beside a
    tracked ``lab/a/b/`` package that merely lacks ``__main__.py``.
    """
    found = []
    for number, line in shell_lines(text):
        for match in MODULE_COMMAND.finditer(line):
            candidates = runnable_candidates(match.group("target"))
            if not any(candidate in tracked for candidate in candidates):
                found.append((number, match.group(0), " or ".join(candidates)))
        for match in SCRIPT_COMMAND.finditer(line):
            if match.group("target") not in tracked:
                found.append((number, match.group(0), match.group("target")))
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
    # What `dead_commands` reports for `-m lab.prism.process_telemetry`: either
    # path would make the command runnable, so both are named.
    TELEMETRY = "lab/prism/process_telemetry.py or lab/prism/process_telemetry/__main__.py"

    def references(self, text: str) -> list[str]:
        return [ref for _, ref in dangling_references(text, self.TRACKED)]

    def commands(self, text: str) -> list[str]:
        return [missing for _, _, missing in dead_commands(text, self.TRACKED)]

    def located(self, text: str) -> list[tuple[int, str]]:
        return [(number, missing) for number, _, missing in dead_commands(text, self.TRACKED)]

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
            [self.TELEMETRY],
        )
        self.assertEqual(
            self.commands('docker exec "$c" python3 -m lab.prism.process_telemetry rss-sample --pid 1'),
            [self.TELEMETRY],
        )
        self.assertEqual(self.commands("python -u -m lab.tool run"), ["lab/tool.py or lab/tool/__main__.py"])
        self.assertEqual(self.commands("python3 lab/prism/storm.py --decide"), ["lab/prism/storm.py"])

    def test_commands_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        text = "python3 -m lab.prism.tool\npython -m lab.pkg\npython3 lab/prism/tool.py"
        self.assertEqual(dead_commands(text, tracked), [])

    # Verified on CPython 3.14: `python3 -m lab.pkg` with only `lab/pkg/__init__.py`,
    # or with a bare `lab/pkg/` directory, prints "No module named
    # lab.pkg.__main__; 'lab.pkg' is a package and cannot be directly executed"
    # and exits 1; with `lab/pkg/__main__.py` it runs, and `python3 -m
    # lab.auxpow.vardiff` runs `lab/auxpow/vardiff.py` with no `lab/auxpow/__init__.py`.
    def test_module_command_on_init_only_package_is_dead(self) -> None:
        tracked = self.TRACKED | {"lab/pkg", "lab/pkg/__init__.py"}
        self.assertEqual(
            dead_commands("python3 -m lab.pkg run", tracked),
            [(1, "python3 -m lab.pkg", "lab/pkg.py or lab/pkg/__main__.py")],
        )

    def test_module_command_on_namespace_directory_is_dead(self) -> None:
        tracked = self.TRACKED | {"lab/auxpow", "lab/auxpow/vardiff.py"}
        self.assertEqual(
            dead_commands("python3 -m lab.auxpow", tracked),
            [(1, "python3 -m lab.auxpow", "lab/auxpow.py or lab/auxpow/__main__.py")],
        )

    def test_module_command_on_package_with_main_passes(self) -> None:
        tracked = self.TRACKED | {"lab/pkg", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        self.assertEqual(dead_commands("python3 -m lab.pkg run", tracked), [])
        self.assertEqual(dead_commands("python3 -m lab.pkg", self.TRACKED | {"lab/pkg/__main__.py"}), [])

    def test_module_command_on_file_inside_namespace_package_passes(self) -> None:
        tracked = self.TRACKED | {"lab/auxpow", "lab/auxpow/vardiff.py"}
        self.assertEqual(dead_commands("python3 -m lab.auxpow.vardiff --help", tracked), [])

    def test_prose_mention_of_init_only_package_is_not_dangling(self) -> None:
        # The prose contract asks whether the name exists, not whether it runs:
        # `lab.prism.pkg` still resolves through `.py`, `__init__.py` or the directory.
        tracked = self.TRACKED | {"lab/prism/pkg", "lab/prism/pkg/__init__.py"}
        self.assertEqual(dangling_references("see lab.prism.pkg", tracked), [])
        self.assertEqual(dangling_references("see lab.prism.bare", self.TRACKED | {"lab/prism/bare"}), [])
        self.assertEqual(dangling_references("see lab.prism.gone", tracked), [(1, "lab.prism.gone")])

    def test_wrapped_module_commands_with_missing_targets_are_caught(self) -> None:
        for wrapped in (
            "python3 -m \\\n  lab.prism.process_telemetry rss-bound",
            "python3 \\\n  -m lab.prism.process_telemetry rss-bound",
            "python3 -u \\\n  -m lab.prism.process_telemetry \\\n  rss-bound",
        ):
            with self.subTest(wrapped=wrapped):
                text = f"```bash\ncd repo\n{wrapped} \\\n  --samples s.csv\n```"
                self.assertEqual(self.located(text), [(3, self.TELEMETRY)])

    def test_wrapped_script_command_with_missing_target_is_caught(self) -> None:
        text = "python3 \\\n  lab/prism/storm.py \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    def test_wrapped_commands_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        text = "python3 -m \\\n  lab.prism.tool\npython \\\n  -m lab.pkg\npython3 \\\n  lab/prism/tool.py"
        self.assertEqual(dead_commands(text, tracked), [])

    # Every option form `python3 --help` accepts ahead of `-m` or a script path.
    OPTIONS = (
        "-OO",
        "-bb -u",
        "-IsE",
        "-X dev",
        "-Xdev",
        "-W error",
        "-uWerror",
        "-X importtime=2",
        "--check-hash-based-pycs always",
    )

    def test_commands_with_real_option_forms_and_missing_targets_are_caught(self) -> None:
        for options in self.OPTIONS:
            with self.subTest(options=options):
                self.assertEqual(
                    self.commands(f"python3 {options} -m lab.prism.process_telemetry rss-bound"),
                    [self.TELEMETRY],
                )
                self.assertEqual(
                    self.commands(f"python {options} lab/prism/storm.py --decide"),
                    ["lab/prism/storm.py"],
                )

    def test_commands_with_real_option_forms_and_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        for options in self.OPTIONS:
            with self.subTest(options=options):
                text = (
                    f"python3 {options} -m lab.prism.tool\npython3.12 {options} -m lab.pkg\n"
                    f"python {options} lab/prism/tool.py"
                )
                self.assertEqual(dead_commands(text, tracked), [])

    def test_wrapped_option_forms_with_missing_targets_are_caught(self) -> None:
        for options in self.OPTIONS:
            wrapped = options.replace(" ", " \\\n  ")
            with self.subTest(options=options):
                text = f"python3 \\\n  {wrapped} \\\n  -m lab.prism.process_telemetry \\\n  rss-bound"
                self.assertEqual(self.located(text), [(1, self.TELEMETRY)])
                text = f"python3.12 {wrapped} \\\n  lab/prism/storm.py \\\n  --decide"
                self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    def test_quoted_targets_with_missing_targets_are_caught(self) -> None:
        for quote in ("'", '"'):
            with self.subTest(quote=quote):
                self.assertEqual(
                    self.commands(f"python3 -m {quote}lab.prism.process_telemetry{quote} rss-bound"),
                    [self.TELEMETRY],
                )
                self.assertEqual(
                    self.commands(f"python3 {quote}lab/prism/storm.py{quote} --decide"),
                    ["lab/prism/storm.py"],
                )

    # Each ran `json.tool` on CPython 3.14 exactly as its bare spelling: the
    # shell strips matching quotes, attached or not, before CPython sees them.
    QUOTED_OPTIONS = (
        "-X 'dev'",
        '-X "dev mode"',
        "-X'dev'",
        '-X"dev mode"',
        "-W 'error'",
        '-W"error"',
        "-uW 'error'",
        "-W 'error'::DeprecationWarning",
        "--check-hash-based-pycs 'always'",
        '--check-hash-based-pycs "never"',
    )

    def test_quoted_option_arguments_with_missing_targets_are_caught(self) -> None:
        for options in self.QUOTED_OPTIONS:
            with self.subTest(options=options):
                self.assertEqual(
                    self.commands(f"python3 {options} -m lab.prism.process_telemetry rss-bound"),
                    [self.TELEMETRY],
                )
                self.assertEqual(
                    self.commands(f"python {options} lab/prism/storm.py --decide"),
                    ["lab/prism/storm.py"],
                )

    def test_quoted_option_arguments_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        for options in self.QUOTED_OPTIONS:
            with self.subTest(options=options):
                text = (
                    f"python3 {options} -m lab.prism.tool\npython3.12 {options} -m lab.pkg\n"
                    f"python {options} lab/prism/tool.py"
                )
                self.assertEqual(dead_commands(text, tracked), [])

    def test_wrapped_quoted_option_arguments_with_missing_targets_are_caught(self) -> None:
        text = "python3 \\\n  -X \\\n  'dev mode' \\\n  -m lab.prism.process_telemetry \\\n  rss-bound"
        self.assertEqual(self.located(text), [(1, self.TELEMETRY)])
        text = "python3.12 --check-hash-based-pycs \\\n  \"always\" \\\n  lab/prism/storm.py \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    def test_mismatched_option_argument_quotes_are_not_a_command(self) -> None:
        # bash rejects `-X 'dev"` with "unexpected EOF while looking for
        # matching `''" before Python starts, so the line runs nothing; the
        # prose contract still sees the reference.
        for options in ("-X 'dev\"", "-X\"dev'", "-W \"error'", "--check-hash-based-pycs 'always\""):
            with self.subTest(options=options):
                text = f"python3 {options} -m lab.prism.x"
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.references(text), ["lab.prism.x"])
                self.assertEqual(self.commands(f"python3 {options} lab/prism/x.py"), [])

    def test_dot_relative_scripts_are_caught_as_the_bare_path(self) -> None:
        for script in ("./lab/prism/storm.py", '"./lab/prism/storm.py"', "././lab/prism/storm.py"):
            with self.subTest(script=script):
                self.assertEqual(self.commands(f"python3 {script} --decide"), ["lab/prism/storm.py"])

    # Every `-m` spelling here ran `json.tool` on CPython 3.12; `-Wm json.tool`
    # and `-Xm json.tool` did not, opening `json.tool` as a script instead.
    ADJACENT_MODULE_OPTIONS = ("-m{}", "-m'{}'", "-Im {}", "-OOm {}", "-Im{}")

    def test_adjacent_module_options_with_missing_targets_are_caught(self) -> None:
        for option in self.ADJACENT_MODULE_OPTIONS:
            with self.subTest(option=option):
                text = f"python3 {option.format('lab.prism.process_telemetry')} rss-bound"
                self.assertEqual(self.commands(text), [self.TELEMETRY])
        self.assertEqual(self.commands("python3 -Wm lab.prism.process_telemetry"), [])
        self.assertEqual(self.commands("python3 -Xm lab.prism.process_telemetry"), [])

    def test_adjacent_module_options_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        for option in self.ADJACENT_MODULE_OPTIONS:
            with self.subTest(option=option):
                text = f"python3 {option.format('lab.prism.tool')}\npython3.12 {option.format('lab.pkg')}"
                self.assertEqual(dead_commands(text, tracked), [])

    def test_quoted_and_dot_relative_forms_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        text = (
            "python3 -m 'lab.prism.tool'\npython -m \"lab.pkg\"\npython3 'lab/prism/tool.py'\n"
            "python3 \"./lab/prism/tool.py\"\npython3 ./lab/prism/tool.py"
        )
        self.assertEqual(dead_commands(text, tracked), [])

    def test_wrapped_quoted_targets_with_missing_targets_are_caught(self) -> None:
        text = "python3 -m \\\n  'lab.prism.process_telemetry' \\\n  rss-bound"
        self.assertEqual(self.located(text), [(1, self.TELEMETRY)])
        text = "python3 \\\n  \"./lab/prism/storm.py\" \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    def test_mismatched_quotes_are_not_a_command(self) -> None:
        # The shell rejects an unterminated quote before anything runs, so the
        # line is not a runnable command and is left to the prose contract,
        # which still sees the reference, as it does inside `-c` strings.
        text = "python3 -m 'lab.prism.x\""
        self.assertEqual(self.commands(text), [])
        self.assertEqual(self.references(text), ["lab.prism.x"])
        self.assertEqual(self.commands("python3 \"lab/prism/x.py'"), [])

    def test_quoted_interpreters_with_missing_targets_are_caught(self) -> None:
        # The shell strips matching quotes around the interpreter name and runs
        # Python all the same, so `"python3" -m lab.x` is as dead as the bare form.
        for quote in ("'", '"'):
            with self.subTest(quote=quote):
                for interpreter in ("python3", "python3.12", "python"):
                    text = f"{quote}{interpreter}{quote} -m lab.prism.process_telemetry rss-bound"
                    self.assertEqual(self.commands(text), [self.TELEMETRY])
                    text = f"{quote}{interpreter}{quote} lab/prism/storm.py --decide"
                    self.assertEqual(self.commands(text), ["lab/prism/storm.py"])
                self.assertEqual(
                    self.commands(f"{quote}python{quote} -OO -m {quote}lab.prism.process_telemetry{quote}"),
                    [self.TELEMETRY],
                )

    def test_quoted_interpreters_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        text = (
            "\"python3\" -m lab.prism.tool\n'python3.12' -OO -m lab.pkg\n\"python\" lab/prism/tool.py\n"
            "'python3' \"./lab/prism/tool.py\""
        )
        self.assertEqual(dead_commands(text, tracked), [])

    def test_quoted_interpreter_inside_a_shell_string_is_still_caught(self) -> None:
        # A quote opens before the interpreter but nothing closes right after
        # it, so the interpreter quote is empty and the inner command is read.
        self.assertEqual(
            self.commands('sh -c "python3 -m lab.prism.process_telemetry rss-bound"'),
            [self.TELEMETRY],
        )
        self.assertEqual(self.commands("sh -c 'python3.12 lab/prism/storm.py --decide'"), ["lab/prism/storm.py"])

    def test_mismatched_interpreter_quotes_are_not_a_command(self) -> None:
        text = "\"python3' -m lab.prism.x"
        self.assertEqual(self.commands(text), [])
        self.assertEqual(self.references(text), ["lab.prism.x"])
        self.assertEqual(self.commands("'python3\" lab/prism/x.py"), [])

    def test_wrapped_quoted_interpreter_is_caught_at_the_right_line(self) -> None:
        text = "```bash\ncd repo\n\"python3\" \\\n  -m lab.prism.process_telemetry \\\n  rss-bound\n```"
        self.assertEqual(self.located(text), [(3, self.TELEMETRY)])
        text = "'python3.12' -OO \\\n  lab/prism/storm.py \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    def test_version_suffixed_interpreters_are_scanned(self) -> None:
        self.assertEqual(
            self.commands("python3.12 -m lab.prism.process_telemetry rss-bound"),
            [self.TELEMETRY],
        )
        self.assertEqual(self.commands("python3.14 -OO lab/prism/storm.py"), ["lab/prism/storm.py"])
        self.assertEqual(self.commands("python2 -m lab.prism.process_telemetry"), [])

    def test_inline_code_is_neither_a_module_nor_a_script_command(self) -> None:
        # `-c cmd` runs its argument and ends the option list; the prose contract
        # still sees the module reference inside the code string.
        text = "python3 -c 'import lab.prism.x'"
        self.assertEqual(self.commands(text), [])
        self.assertEqual(self.references(text), ["lab.prism.x"])
        self.assertEqual(self.commands("python3 -OO -c 'import lab.prism.x'"), [])
        self.assertEqual(self.commands("python3 -c -m lab.prism.x"), [])
        self.assertEqual(self.commands("python3 -c lab/prism/x.py"), [])


if __name__ == "__main__":
    unittest.main()

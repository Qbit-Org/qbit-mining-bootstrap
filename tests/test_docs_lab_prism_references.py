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
   it reads direct ``python``/``python3``/``python3.N`` invocations, bare or
   by path (``/usr/bin/python3``), with their CPython option forms, quoted
   interpreter names and targets (plain, ANSI-C ``$'…'`` or locale ``$"…"``
   quotes), shell word concatenation of the interpreter and of the target
   (``"python"3``, ``'pyth'on3``, ``"lab.prism."deleted``) and ``./``
   prefixes, and does not follow ``cd``, ``PYTHONPATH`` or other environment
   indirection, aliases, shell variables, or backslash escapes.
b. Every ``lab/prism/…`` path or ``lab.prism.…`` module reference resolves to a
   tracked file or directory. GitHub links pinned to a 40-hex commit SHA are
   stable history and exempt. Pre-existing residue that #303 declares out of
   scope is ratcheted in ``RATCHET``: the count may shrink, never grow.

A ``.patch`` or ``.diff`` file under ``docs/`` quotes another tree's before-and-
after text and is not this repository's prose, so neither contract reads it;
every other tracked file under ``docs/`` is in scope, whatever its suffix.
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

# A unified diff under docs/ is quoted history: its `-` and `+` lines are the
# text of another tree before and after a change (`docs/prism-alert-rules-
# qbit-tools.patch` is a diff of qbit-tools' Ansible role), and editing them
# would break the patch. Neither contract reads such a file. Every other suffix
# stays in scope, so a `.json` or `.yaml` example is still checked.
QUOTED_HISTORY_SUFFIXES = frozenset({".patch", ".diff"})


def quoted(target: str, group: str = "quote") -> str:
    """``target`` bare or in matching single or double quotes, which the shell strips.

    A mismatched quote is a shell syntax error: the line runs nothing and
    matches nothing. ``group`` names the capture that holds the quote, so a
    pattern that uses this more than once can give each use its own name,
    since ``re`` rejects a group name used twice.
    """
    return rf"(?P<{group}>['\"]?){target}\b(?P={group})"


PATH_REFERENCE = re.compile(r"lab/prism(?:/[A-Za-z0-9_][A-Za-z0-9_.\-]*)*")
MODULE_REFERENCE = re.compile(r"\blab\.prism(?:\.[A-Za-z_][A-Za-z0-9_]*)*\b")
# `python3 -OO -X dev -m lab.a.b` and `python3.12 -Werror lab/a/b.py`. Every
# option form `python3 --help` lists may sit between the interpreter and its
# target: clustered flag letters, `-W`/`-X` with an attached or following
# argument, and `--check-hash-based-pycs <mode>` (CPython rejects the `=`
# spelling). `-c cmd` runs its argument and ends the option list, so it is
# deliberately not a prefix option. The interpreter name is read as a whole
# shell word, see `PYTHON_COMMAND` below. An option argument may be quoted
# the same way as a target, attached or following:
# CPython 3.14 runs `-X 'dev'`, `-X"dev mode"`, `-W'error'` and
# `--check-hash-based-pycs "always"` exactly as their bare spellings, since
# the shell strips the quotes first. A bare argument therefore holds no quote
# character at all: `-X 'dev"` is an unterminated shell string, not an option.
# The flag letters are every single-letter option `python3 --help` lists on
# CPython 3.14 other than `-c` and `-m`; `-?`, the alias of `-h`, is left out.
PYTHON_FLAG = r"[bBdEhiIOPqRsSuvVx]"
# A word the shell hands over as one argument: runs of unquoted characters
# alternating with matching-quoted strings (`'error'::Warning`, `"dev mode"`,
# `"lab.prism."deleted`), non-empty. A quoted string may carry bash's `$`
# prefix: ANSI-C quoting `$'…'` and locale quoting `$"…"` are strings the
# shell strips like plain quotes, so `python3 -m $'json.'tool`, `python3 -m
# $"json.tool"` and `$'python3' -m json.tool` all ran `json.tool` on bash 3.2.
# A `$` directly before a quote therefore belongs to the quoted piece and is
# never an unquoted character, which keeps the split of a word unique; a `$`
# before anything else (`$VAR`, `"$c"`) is an ordinary unquoted character, as
# before. The first piece is one unquoted character or one quoted string, so
# a lone or unterminated opening quote is not a word; after it, unquoted runs
# and quoted strings alternate rather than nest, so the pattern never has two
# ways to split one word and cannot backtrack exponentially. The word ends at
# whitespace, at a bash metacharacter (`|`, `&`, `;`, `(`, `)`, `<`, `>`), at
# a backtick (which closes the inline code a command sits in, and in the
# shell opens a command substitution this check does not follow), or at a
# quote that opens no string: inside `sh -c "python3 -m lab.a.b"` the closing
# `"` belongs to the enclosing string, and the inner command is read as
# before. The quotes the shell strips, with their `$` prefix, are removed by
# ``unquote`` before a word is read as a target. The backslash escapes
# `$'…'` decodes are not: bash runs `python3 -m $'json\x2etool'` as
# `json.tool`, but this check follows no backslash escape (module docstring),
# so an escape inside `$'…'` stays in the unquoted word, matches no target and
# is not reported.
WORD_BREAK = r"\s|&;()<>`"
UNQUOTED_CHARACTER = rf"[^{WORD_BREAK}'\"$]|\$(?!['\"])"
QUOTED_STRING = r"\$?'[^']*'|\$?\"[^\"]*\""


def shell_word(quoted_string: str) -> str:
    """The word pattern above, with ``quoted_string`` as its quoted piece."""
    return (
        rf"(?:{UNQUOTED_CHARACTER}|{quoted_string})"
        rf"(?:{UNQUOTED_CHARACTER})*(?:(?:{quoted_string})(?:{UNQUOTED_CHARACTER})*)*"
        rf"(?![^{WORD_BREAK}'\"])"
    )


SHELL_WORD = shell_word(QUOTED_STRING)
# The interpreter word is read with quoted pieces that hold no whitespace. A
# program name is one word, so nothing is lost, and the restriction is what
# lets the word be read from every position of a line: with arbitrary quoted
# pieces, `"$c" sh -c "python3` read from `$c` would be the one word
# `$c" sh -c "python3`, pairing the closing quote of `"$c"` with the opening
# quote of the `sh -c` string and hiding the `python3` inside it.
INTERPRETER_STRING = r"\$?'[^'\s]*'|\$?\"[^\"\s]*\""
INTERPRETER_WORD = shell_word(INTERPRETER_STRING)
PYTHON_OPTION = (
    rf"-{PYTHON_FLAG}+"  # -O, -OO, -bb, -IsE
    rf"|-{PYTHON_FLAG}*[WX](?:{SHELL_WORD}|\s+{SHELL_WORD})"  # -Xdev, -X 'dev', -uWerror
    r"|--check-hash-based-pycs\s+" + quoted(r"(?:always|default|never)", group="pycs_quote")
)
# `-m` may close a flag cluster and take its module attached: CPython 3.12 runs
# `-mlab.x`, `-Im lab.x`, `-OOm lab.x` and `-Imlab.x` alike, while `-Wm lab.x`
# hands `m` to `-W` and treats `lab.x` as a script path.
MODULE_OPTION = rf"-{PYTHON_FLAG}*m\s*"


# The interpreter is read as a whole shell word (`INTERPRETER_WORD`), like
# the target below: bash 3.2 runs `"python"3 -m json.tool`, `'pyth'on3 -m
# json.tool`, `python"3" -m json.tool` and `"/usr/bin/pyth"on3 -m json.tool`
# exactly as `python3`, so a name the shell assembles from pieces is as
# runnable as a bare or a wholly quoted one. ``dead_commands`` strips the matching quotes, takes the
# basename after the last `/` (`/usr/bin/python3` runs Python as `python3`
# does) and accepts the word only if it is wholly `python`, `python3` or
# `python3.N` (`INTERPRETER`): `mypython3`, `cpython3` and `python2` are
# other programs, and bash answers `mypython3 -m json.tool` with "command not
# found". A word starts where no unquoted word character precedes it, so
# `env python3 …`, `docker exec "$c" python3 …`, `$(python3 …)`, inline-code
# `` `python3 …` `` and the inner command of `sh -c "python3 …"` are read as
# before, while `mypython3` is one word. A word may also start right after a
# closing quote, so `"/usr/bin/"python3` and `"my"python3` are each one
# candidate and their tail `python3` another; the command patterns are
# therefore lookaheads that consume nothing, every word is a candidate
# interpreter, and ``python_commands`` drops a candidate that starts inside
# the interpreter word of the one before it. A word that opens a quote and
# never closes it (`"python3' -m lab.a.b`) is a shell syntax error and
# matches nothing, as before.
INTERPRETER = re.compile(r"python(?:3(?:\.\d+)?)?")
PYTHON_COMMAND = (
    rf"(?<![^{WORD_BREAK}'\"])(?P<interpreter>{INTERPRETER_WORD})"
    rf"(?:\s+(?:{PYTHON_OPTION}))*"
)
# The `-m` target and the script path are read as whole shell words, since
# bash hands `-m "lab.prism."deleted`, `-m lab."prism".deleted` and
# `"./lab/prism/"deleted.py` over exactly as their bare spellings (verified
# with `python3 -m "json."tool` on bash 3.2 and CPython 3.14). ``dead_commands``
# strips the matching quotes and any `./` prefixes, which CPython resolves on
# a script path, and only then reads the word against the target grammar: a
# word that then names no `lab` module or script (`$VAR`, `json.tool`,
# `lab.prism.` with nothing after the dot) is not a `lab` command.
# The `--` terminator may precede a script path (`python3 -OO -- lab/a/b.py`
# runs it on CPython 3.14) but never `-m`: after `--` CPython takes `-m` as a
# script name and fails to open a file called `-m`, so `python3 -- -m lab.a.b`
# runs nothing and is deliberately not a module command.
MODULE_COMMAND = re.compile(
    rf"(?=(?P<command>{PYTHON_COMMAND}\s+{MODULE_OPTION}(?P<word>{SHELL_WORD})))"
)
SCRIPT_COMMAND = re.compile(
    rf"(?=(?P<command>{PYTHON_COMMAND}\s+(?:--\s+)?(?P<word>{SHELL_WORD})))"
)
MODULE_TARGET = re.compile(r"lab(?:\.[A-Za-z_][A-Za-z0-9_]*)+")
SCRIPT_TARGET = re.compile(r"lab/[A-Za-z0-9_./\-]+\.py")
DOT_SEGMENTS = re.compile(r"^(?:\./)+")
MATCHING_QUOTES = re.compile(QUOTED_STRING)
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


def documented(paths) -> list[str]:
    """The tracked paths under ``docs/`` both contracts read, sorted.

    Quoted history (``QUOTED_HISTORY_SUFFIXES``) is left out. ``tracked_paths``
    lists directory prefixes as well, so the caller settles whether a path is
    a file against the working tree.
    """
    return sorted(
        path
        for path in paths
        if path.startswith("docs/") and Path(path).suffix not in QUOTED_HISTORY_SUFFIXES
    )


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


def unquote(word: str) -> str:
    """``word`` as the shell hands it to CPython: matching quotes removed, their contents kept.

    The ``$`` of an ANSI-C ``$'…'`` or locale ``$"…"`` string goes with its
    quotes, so ``$'lab.prism.'deleted`` is ``lab.prism.deleted``.
    """
    return MATCHING_QUOTES.sub(lambda match: match.group(0).removeprefix("$")[1:-1], word)


def runs_python(word: str) -> bool:
    """Whether ``word`` names CPython: unquoted, its basename is ``python``, ``python3`` or ``python3.N``."""
    return INTERPRETER.fullmatch(unquote(word).rsplit("/", 1)[-1]) is not None


def python_commands(pattern: re.Pattern[str], line: str):
    """Each match of ``pattern`` on ``line`` whose interpreter word names CPython.

    The pattern is a lookahead, so every word start is a candidate. One that
    starts inside the interpreter word of the candidate before it is the tail
    of that word after a closing quote (``python3`` in ``"/usr/bin/"python3``
    or in ``"my"python3``), not a command of its own, and is dropped whether
    or not the whole word named Python.
    """
    end = 0
    for match in pattern.finditer(line):
        if match.start() < end:
            continue
        end = match.end("interpreter")
        if runs_python(match.group("interpreter")):
            yield match


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
        for match in python_commands(MODULE_COMMAND, line):
            module = unquote(match.group("word"))
            if not MODULE_TARGET.fullmatch(module):
                continue
            candidates = runnable_candidates(module)
            if not any(candidate in tracked for candidate in candidates):
                found.append((number, match.group("command"), " or ".join(candidates)))
        for match in python_commands(SCRIPT_COMMAND, line):
            script = DOT_SEGMENTS.sub("", unquote(match.group("word")))
            if SCRIPT_TARGET.fullmatch(script) and script not in tracked:
                found.append((number, match.group("command"), script))
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
        cls.docs = [path for path in documented(cls.tracked) if (ROOT / path).is_file()]
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

    def test_quoted_history_under_docs_is_not_read(self) -> None:
        # A patch or diff quotes another tree's lines, so it is left out of
        # both contracts; prose and data files of any other suffix stay in.
        paths = {
            "docs",
            "docs/prism-alert-rules-qbit-tools.patch",
            "docs/prism-coordinator-refactor/history.diff",
            "docs/prism-capacity-readiness.md",
            "docs/prism-coordinator-refactor/dashboard.json",
            "docs/prism-alerts.yaml",
            "lab/prism/Dockerfile",
            "tests/history.patch",
        }
        self.assertEqual(
            documented(paths),
            [
                "docs/prism-alerts.yaml",
                "docs/prism-capacity-readiness.md",
                "docs/prism-coordinator-refactor/dashboard.json",
            ],
        )
        for path in self.docs:
            self.assertNotIn(Path(path).suffix, QUOTED_HISTORY_SUFFIXES, path)


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
        "-R",
        "-ER -X dev",
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

    # On CPython 3.14, `python3 -- s.py` and `python3 -O -- s.py` run the
    # script, while `python3 -- -m json.tool` fails with "can't open file
    # '.../-m'": after `--` the `-m` is a script path, not an option.
    def test_terminated_script_commands_with_missing_targets_are_caught(self) -> None:
        for options in ("--", "-OO --", "-R -X dev --"):
            with self.subTest(options=options):
                self.assertEqual(
                    self.commands(f"python3 {options} lab/prism/storm.py --decide"), ["lab/prism/storm.py"]
                )
                self.assertEqual(
                    self.commands(f"python {options} './lab/prism/storm.py'"), ["lab/prism/storm.py"]
                )

    def test_terminated_script_commands_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py"}
        text = (
            "python3 -- lab/prism/tool.py\npython3.12 -OO -- \"./lab/prism/tool.py\"\n"
            "python -R -- lab/prism/tool.py"
        )
        self.assertEqual(dead_commands(text, tracked), [])

    def test_wrapped_terminated_script_command_with_missing_target_is_caught(self) -> None:
        text = "python3 -OO \\\n  -- \\\n  lab/prism/storm.py \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    def test_terminator_before_module_option_is_not_a_command(self) -> None:
        # CPython treats everything after `--` as the script path and its
        # arguments, so `-m` names a file called `-m` that does not exist and
        # nothing runs; the prose contract still sees the module reference.
        for text in ("python3 -- -m lab.prism.x", "python3 -OO -- -m lab.prism.x"):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.references(text), ["lab.prism.x"])

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

    # Every `-m` spelling here ran `json.tool` on CPython 3.12 (`-Rm` on 3.14);
    # `-Wm json.tool` and `-Xm json.tool` did not, opening `json.tool` as a
    # script instead.
    ADJACENT_MODULE_OPTIONS = ("-m{}", "-m'{}'", "-Im {}", "-OOm {}", "-Im{}", "-Rm {}")

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

    # Each ran `json.tool` on bash 3.2 and CPython 3.14 exactly as the bare
    # spelling: the shell joins the quoted and unquoted pieces of a word and
    # strips the quotes before CPython sees one argument.
    CONCATENATED_MODULES = ('"lab.prism."{}', "'lab.prism.'{}", 'lab."prism".{}', "lab.'prism'.{}", '"lab.prism".{}')
    CONCATENATED_SCRIPTS = (
        '"lab/prism/"{}.py',
        "'lab/prism/'{}.py",
        'lab/"prism"/{}.py',
        '"./lab/prism/"{}.py',
        '"./"./lab/prism/{}.py',
        "./'lab/prism/{}.py'",
    )

    def test_concatenated_targets_with_missing_targets_are_caught_as_the_bare_target(self) -> None:
        for module in self.CONCATENATED_MODULES:
            with self.subTest(module=module):
                text = f"python3 -m {module.format('process_telemetry')} rss-bound"
                self.assertEqual(self.commands(text), [self.TELEMETRY])
        for script in self.CONCATENATED_SCRIPTS:
            with self.subTest(script=script):
                self.assertEqual(self.commands(f"python3 {script.format('storm')} --decide"), ["lab/prism/storm.py"])
        self.assertEqual(
            self.commands('python3 -X "dev" -m "lab.prism."process_telemetry'), [self.TELEMETRY]
        )

    def test_concatenated_targets_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        for module in self.CONCATENATED_MODULES:
            with self.subTest(module=module):
                self.assertEqual(dead_commands(f"python3 -m {module.format('tool')}", tracked), [])
        for script in self.CONCATENATED_SCRIPTS:
            with self.subTest(script=script):
                self.assertEqual(dead_commands(f"python3 {script.format('tool')}", tracked), [])
        self.assertEqual(dead_commands('python -m "lab".pkg run', tracked), [])

    def test_wrapped_concatenated_targets_with_missing_targets_are_caught(self) -> None:
        text = "```bash\ncd repo\npython3 -m \\\n  \"lab.prism.\"process_telemetry \\\n  rss-bound\n```"
        self.assertEqual(self.located(text), [(3, self.TELEMETRY)])
        text = "python3 \\\n  \"./lab/prism/\"storm.py \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    # Each ran on bash 3.2 and CPython 3.14 exactly as the plain spelling:
    # `python3 -m $'json.tool'`, `python3 -m $'json.'tool`, `python3 -m
    # $"json.tool"`, `$'python3' -m json.tool`, `$"python3" -m json.tool`,
    # `python3 $'lab/prism/'storm.py` and `python3 lab/prism/$'storm'.py`.
    ANSI_C_MODULES = ("$'lab.prism.{}'", "$'lab.prism.'{}", '$"lab.prism.{}"', "lab.prism.$'{}'", "lab.$'prism'.{}")
    ANSI_C_SCRIPTS = ("$'lab/prism/{}.py'", "$'lab/prism/'{}.py", '$"lab/prism/{}.py"', "lab/prism/$'{}'.py")
    ANSI_C_INTERPRETERS = ("$'python3'", '$"python3"', "$'pyth'on3", "$'/usr/bin/'python3", "$'python3.12'")

    def test_ansi_c_quoted_words_with_missing_targets_are_caught_as_the_bare_target(self) -> None:
        for module in self.ANSI_C_MODULES:
            with self.subTest(module=module):
                text = f"python3 -m {module.format('process_telemetry')} rss-bound"
                self.assertEqual(self.commands(text), [self.TELEMETRY])
        for script in self.ANSI_C_SCRIPTS:
            with self.subTest(script=script):
                self.assertEqual(self.commands(f"python3 {script.format('storm')} --decide"), ["lab/prism/storm.py"])
        for interpreter in self.ANSI_C_INTERPRETERS:
            with self.subTest(interpreter=interpreter):
                self.assertEqual(
                    self.commands(f"{interpreter} -m lab.prism.process_telemetry rss-bound"), [self.TELEMETRY]
                )
                self.assertEqual(self.commands(f"{interpreter} lab/prism/storm.py --decide"), ["lab/prism/storm.py"])
        self.assertEqual(
            self.commands("$'python3' -X $'dev' -m $'lab.prism.'process_telemetry"), [self.TELEMETRY]
        )
        self.assertEqual(
            dead_commands("$'python3' -m $'lab.prism.'process_telemetry", self.TRACKED),
            [(1, "$'python3' -m $'lab.prism.'process_telemetry", self.TELEMETRY)],
        )

    def test_ansi_c_quoted_words_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        for module in self.ANSI_C_MODULES:
            with self.subTest(module=module):
                self.assertEqual(dead_commands(f"python3 -m {module.format('tool')}", tracked), [])
        for script in self.ANSI_C_SCRIPTS:
            with self.subTest(script=script):
                self.assertEqual(dead_commands(f"python3 {script.format('tool')}", tracked), [])
        for interpreter in self.ANSI_C_INTERPRETERS:
            with self.subTest(interpreter=interpreter):
                self.assertEqual(dead_commands(f"{interpreter} -m lab.prism.tool", tracked), [])
                self.assertEqual(dead_commands(f"{interpreter} lab/prism/tool.py", tracked), [])
        self.assertEqual(dead_commands("python -m $'lab'.pkg run", tracked), [])

    def test_ansi_c_escapes_are_not_followed(self) -> None:
        # bash decodes `\x2e` to `.` and runs `python3 -m $'lab\x2eprism.x'`,
        # but backslash escapes are outside this check (module docstring): the
        # backslash stays in the word, which then names no target and no
        # interpreter. An unterminated `$'` is a syntax error like a bare one;
        # a `$` before anything but a quote is an ordinary character, so a
        # variable is still not a command.
        for text in (
            "python3 -m $'lab\\x2eprism.process_telemetry' rss-bound",
            "python3 $'lab/prism/storm\\x2epy' --decide",
            "$'pyth\\x6fn3' -m lab.prism.process_telemetry",
            "python3 -m $'lab.prism.process_telemetry\\'' rss-bound",
            "python3 -m $'lab.prism.x\"",
            "python3 $\"lab/prism/x.py'",
            "python3 -m $MODULE rss-bound",
            "python3 -m $MODULE'.x'",
            "python3 -m lab.prism.$MODULE",
            "python3 -m lab.prism.x$",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
        self.assertEqual(self.references("python3 -m $'lab.prism.x\""), ["lab.prism.x"])

    def test_wrapped_ansi_c_quoted_words_are_caught_at_the_right_line(self) -> None:
        text = "```bash\ncd repo\npython3 -m \\\n  $'lab.prism.'process_telemetry \\\n  rss-bound\n```"
        self.assertEqual(self.located(text), [(3, self.TELEMETRY)])
        text = "$'python3' \\\n  $\"lab/prism/storm.py\" \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    def test_words_that_name_no_lab_target_are_not_a_command(self) -> None:
        # `python3 -m "lab.prism."` hands CPython `lab.prism.`, which fails with
        # "No module named lab.prism." and runs nothing; a variable, another
        # package or a name outside the grammar is not a `lab` command either.
        for text in (
            'python3 -m "lab.prism."',
            'python3 -m "lab.prism." rss-bound',
            "python3 -m 'lab.prism.'",
            "python3 -m $MODULE rss-bound",
            'python3 -m "$MODULE"',
            'python3 -m "json."tool',
            "python3 -m lab.prism.x-y",
            "python3 -m lab.prism.x.",
            'python3 "$SCRIPT" --decide',
            'python3 "lab/prism/"storm --decide',
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])

    def test_target_words_end_at_shell_metacharacters_and_inline_code(self) -> None:
        # bash ends a word at `;`, `|`, `&`, `(`, `)`, `<` and `>` (`python3 -m
        # json.tool;` ran on bash 3.2), and a backtick closes the inline code a
        # command sits in, so each of these still runs the missing target.
        for text in (
            "`python3 -m lab.prism.process_telemetry`",
            "python3 -m lab.prism.process_telemetry; echo done",
            "python3 -m lab.prism.process_telemetry|tee log",
            "python3 -m lab.prism.process_telemetry>>log",
            "python3 -m lab.prism.process_telemetry&",
            "$(python3 -m lab.prism.process_telemetry)",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [self.TELEMETRY])
        self.assertEqual(self.commands("`python3 lab/prism/storm.py`; $(python3 lab/prism/storm.py)"),
                         ["lab/prism/storm.py", "lab/prism/storm.py"])

    def test_target_words_end_at_the_close_of_an_enclosing_shell_string(self) -> None:
        # Inside `sh -c "..."` the quote after the target closes the enclosing
        # string, not a string the target opened, so the inner command is read.
        self.assertEqual(
            self.commands('docker exec "$c" sh -c "python3 -m lab.prism.process_telemetry"'),
            [self.TELEMETRY],
        )
        self.assertEqual(self.commands("sh -c 'python3 lab/prism/storm.py'"), ["lab/prism/storm.py"])

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

    # Each ran `json.tool` on bash 3.2 and CPython 3.14 exactly as `python3`:
    # the shell joins the pieces of the interpreter word and strips the quotes
    # before it looks the program up, so a name spelled in pieces runs Python.
    CONCATENATED_INTERPRETERS = ('"python"3', "'pyth'on3", 'python"3"', "python''3", '"pyth"on"3"')

    def test_concatenated_interpreters_with_missing_targets_are_caught(self) -> None:
        for interpreter in self.CONCATENATED_INTERPRETERS:
            with self.subTest(interpreter=interpreter):
                text = f"{interpreter} -m lab.prism.process_telemetry rss-bound"
                self.assertEqual(self.commands(text), [self.TELEMETRY])
                self.assertEqual(
                    self.commands(f"{interpreter} lab/prism/storm.py --decide"), ["lab/prism/storm.py"]
                )
                self.assertEqual(
                    self.commands(f"{interpreter} -OO -m 'lab.prism.'process_telemetry"), [self.TELEMETRY]
                )
        self.assertEqual(self.commands("'pyth'on3.12 -m lab.prism.process_telemetry"), [self.TELEMETRY])

    def test_concatenated_interpreters_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        for interpreter in self.CONCATENATED_INTERPRETERS:
            with self.subTest(interpreter=interpreter):
                text = (
                    f"{interpreter} -m lab.prism.tool\n{interpreter} -OO -m lab.pkg\n"
                    f"{interpreter} lab/prism/tool.py"
                )
                self.assertEqual(dead_commands(text, tracked), [])

    def test_wrapped_concatenated_interpreter_is_caught_at_the_right_line(self) -> None:
        text = "```bash\ncd repo\n\"python\"3 \\\n  -m lab.prism.process_telemetry \\\n  rss-bound\n```"
        self.assertEqual(self.located(text), [(3, self.TELEMETRY)])
        text = "'pyth'on3 -OO \\\n  lab/prism/storm.py \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    # `/usr/bin/python3 -m json.tool` and `"/usr/bin/pyth"on3 -m json.tool` both
    # ran on bash 3.2: the basename after the last `/` is what names Python.
    def test_path_interpreters_with_missing_targets_are_caught_once(self) -> None:
        for interpreter in (
            "/usr/bin/python3",
            "/opt/homebrew/bin/python3.12",
            '"/usr/bin/pyth"on3',
            '"/usr/bin/"python3',
            "'/usr/bin/python'3",
            "./venv/bin/python",
        ):
            with self.subTest(interpreter=interpreter):
                self.assertEqual(
                    self.commands(f"{interpreter} -m lab.prism.process_telemetry rss-bound"), [self.TELEMETRY]
                )
                self.assertEqual(self.commands(f"{interpreter} lab/prism/storm.py --decide"), ["lab/prism/storm.py"])
        self.assertEqual(
            dead_commands('"/usr/bin/"python3 lab/prism/storm.py', self.TRACKED),
            [(1, '"/usr/bin/"python3 lab/prism/storm.py', "lab/prism/storm.py")],
        )

    def test_words_that_name_no_interpreter_are_not_a_command(self) -> None:
        # bash answers `mypython3 -m json.tool` with "command not found", and
        # `cpython3`, `python2`, `python3x` or a program merely under a `python`
        # directory are other programs; the prose contract still sees the
        # reference. A word that opens a quote it never closes is a syntax error.
        for interpreter in (
            "mypython3",
            "cpython3",
            "python2",
            "python3x",
            '"my"python3',
            "python3/bin/tool",
            "python\"3",
            "env",
        ):
            with self.subTest(interpreter=interpreter):
                text = f"{interpreter} -m lab.prism.x"
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.references(text), ["lab.prism.x"])
                self.assertEqual(self.commands(f"{interpreter} lab/prism/x.py"), [])

    def test_interpreter_words_start_after_shell_boundaries(self) -> None:
        # A launcher word before the interpreter is not consumed with it: each
        # of these still runs the missing target through `python3`.
        for text in (
            "env python3 lab/prism/storm.py",
            "env -i python3 -m lab.prism.process_telemetry",
            'docker exec "$c" python3 lab/prism/storm.py',
            "sudo -u prism /usr/bin/python3 lab/prism/storm.py",
            "nohup python3 lab/prism/storm.py &",
            "(python3 lab/prism/storm.py)",
            "$(python3 lab/prism/storm.py)",
            "`python3 lab/prism/storm.py`",
            "true && python3 lab/prism/storm.py",
            "true; python3 lab/prism/storm.py",
            'sh -c "python3 lab/prism/storm.py"',
        ):
            with self.subTest(text=text):
                self.assertEqual(len(self.commands(text)), 1, self.commands(text))

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

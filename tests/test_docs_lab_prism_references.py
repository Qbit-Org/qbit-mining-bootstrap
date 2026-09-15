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
   interpreter names, option words and targets (plain, ANSI-C ``$'…'`` or
   locale ``$"…"`` quotes), shell word concatenation of the interpreter, of
   an option word and of the target (``"python"3``, ``'pyth'on3``, ``"-"O``,
   ``"-m"lab.prism.deleted``, ``"lab.prism."deleted``) and equivalent POSIX
   script paths (``./``, ``..`` and repeated slashes),
   and does not follow ``cd``, ``PYTHONPATH`` or other environment
   indirection, aliases, shell variables, or backslash escapes. Shell comments,
   ordinary arguments and heredoc bodies run no command of their own. Command
   positions include shell lists, brace groups and loop conditions/bodies,
   the env/sudo/nohup/command/exec and docker/podman exec wrappers, and literal
   sh/bash/dash/ksh/zsh ``-c`` strings.
   Other launcher grammars, shell evaluation of stdin, and expansions inside
   quoted arguments or heredocs are outside this lexical check.
b. Every ``lab/prism/…`` path or ``lab.prism.…`` module reference resolves to a
   tracked file or directory. GitHub links pinned to a 40-hex commit SHA are
   stable history and exempt. Pre-existing residue that #303 declares out of
   scope is ratcheted in ``RATCHET``: the count may shrink, never grow.

A ``.patch`` or ``.diff`` file under ``docs/`` quotes another tree's before-and-
after text and is not this repository's prose, so neither contract reads it;
every other tracked file under ``docs/`` is in scope, whatever its suffix.
"""

from __future__ import annotations

import posixpath
import re
import shlex
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


def prose_reference_pattern(root: str, separator: str) -> re.Pattern[str]:
    # Quotes and backticks preserve literal spaces and punctuation. Bare
    # references consume whole tokens; only trailing prose punctuation is
    # removed below, so Dockerfile!old and tool*old cannot resolve as prefixes.
    # A Markdown link's ]( or ][ ends its label, outside quoted literals.
    literal = rf"{root}(?:{separator}.*?)?"
    bare = rf"{root}(?:{separator}(?:(?!\]\(|\]\[)[^\s`'\"<>])+)?"
    return re.compile(
        rf"(?P<quote>[`'\"])(?P<literal>{literal})(?P=quote)|(?P<bare>{bare})"
    )


PATH_REFERENCE = prose_reference_pattern(r"\blab/prism", "/")
MODULE_REFERENCE = prose_reference_pattern(r"\blab\.prism", r"\.")
PROSE_TRAILING_PUNCTUATION = ".,;:!?*()[]{}|"
URL_REFERENCE_PREFIX = re.compile(r"https?://\S+/$")
# A `lab/prism` path names the repository's own tree only where a path token
# starts: at the start of a line or after whitespace, behind any Markdown
# openers `( [ { < * | >` that begin that token, or after a Markdown link's
# `](`; then behind any opening quotes or backticks and at most one `./`. The
# `>` is the blockquote marker, which needs no space after it: `>lab/prism/x`
# and the nested `>>lab/prism/x` quote the root path as `> lab/prism/x` does.
# It also starts directly after the ref of a GitHub blob, tree or raw URL.
# `\b` alone matched inside `my-lab/prism`, `my*lab/prism`, `my(lab/prism`,
# `my=lab/prism`, `my>lab/prism`, `vendor/lab/prism`, `/tmp/lab/prism` and
# `https://example.com/lab/prism`, which are other paths: an opener starts a
# token only at the start of the line or after whitespace. `../lab/prism` and
# `/lab/prism` are not the root either: a scan of doc text does not know which
# directory the doc sits in, so neither spelling can be resolved. Whitespace
# still starts a token inside a code span, so `echo lab/prism/x` is read.
PATH_ROOT_PREFIX = re.compile(
    r"(?:(?:^|\s)[(\[{<*|>]*[`'\"]*(?:\./)?"
    r"|\]\([<`'\"]*(?:\./)?"
    r"|https?://github\.com/[^/\s]+/[^/\s]+/(?:blob|tree|raw)/[^/\s]+/)$"
)
# A dotted `lab.prism` module is the repository's own only where a token
# starts, as `PATH_ROOT_PREFIX` reads it (the blockquote `>lab.prism.x` and
# `>>lab.prism.x` included) but with no `./` or URL form: a module name is
# never a path. A shell `$'` or `$"` may open the token, so the prose net
# still reads `-m $'lab.prism.x"`. `\b` alone matched inside
# `vendor.lab.prism`, `my-lab.prism`, `my>lab.prism`, `../lab.prism`,
# `/tmp/lab.prism` and `https://example.com/lab.prism`, which name other
# modules or paths.
MODULE_ROOT_PREFIX = re.compile(r"(?:(?:^|\s)[(\[{<*|>]*(?:\$['\"])?[`'\"]*|\]\([<`'\"]*)$")
# `python3 -OO -X dev -m lab.a.b` and `python3.12 -Werror lab/a/b.py`. Every
# option form `python3 --help` lists may sit between the interpreter and its
# target: clustered flag letters, `-W`/`-X` with an attached or following
# argument, and `--check-hash-based-pycs <mode>` (CPython rejects the `=`
# spelling). `-c cmd` runs its argument and ends the option list, so it is
# deliberately not a prefix option. The interpreter, every option word and
# the target are each read as a whole shell word and unquoted before the
# grammar sees them, see ``command_target`` and the option patterns below.
# The flag letters are every single-letter option `python3 --help` lists on
# CPython 3.14 other than `-c`, `-m`, `-h` with its alias `-?`, and `-V`.
# `-h` and `-?` print the help text and `-V` the version, and CPython then
# exits before it reads the target: on CPython 3.14, `python3 -h -m lab.x`,
# `python3 -V lab/x.py`, `python3 -hm lab.x`, `python3 -Vm lab.x`, `python3
# -Oh -m lab.x`, `python3 -hW error -m lab.x` and `python3 -VX dev -m lab.x`
# all print and exit 0 without importing or opening anything, while `python3
# -m lab.x` and `python3 lab/x.py` run it. A command carrying one of them
# ahead of its target therefore runs nothing, so neither letter is a prefix
# option nor may close a `-m` cluster, and the target after one is left to
# the prose contract like the argument of `-c`.
PYTHON_FLAG = r"[bBdEiIOPqRsSuvx]"
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
# ``unquote`` before a word is read as an option or a target. The backslash
# escapes `$'…'` decodes are not: bash runs `python3 -m $'json\x2etool'` as
# `json.tool`, but this check follows no backslash escape (module docstring),
# so ``literal_word`` rejects a target word containing such an escape.
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
# The words after the interpreter are read one shell word at a time by
# ``command_target``, each stripped of its matching quotes by ``unquote``
# before the grammar below sees it, so an option word is as runnable quoted,
# in pieces or ANSI-C quoted as bare: bash 3.2 and CPython 3.14 ran `python3
# "-O" -m json.tool`, `python3 '-OO' -m json.tool`, `python3 $'-OO' -m
# json.tool`, `python3 "-"O -m json.tool`, `python3 '-m' json.tool`,
# `python3 "-m"json.tool`, `python3 -"m" json.tool`, `python3 "-X" dev -m
# json.tool`, `python3 '-X'dev -m json.tool`, `python3 "-X"'dev' -m
# json.tool`, `python3 "-W" error -m json.tool`, `python3 '-uW'error -m
# json.tool` and `python3 "--check-hash-based-pycs" always -m json.tool`
# exactly as their bare spellings, and `python3 '--' lab/gone.py` ran the
# script, since the shell strips the quotes before CPython sees one
# argument. A word with an unterminated or mismatched quote (`"-O'`, `-X
# "dev'`) is no shell word: bash rejects the line with "unexpected EOF while
# looking for matching `"'" before Python starts, so the command ends there
# with no target, as it does at such a target word. The grammar, applied to
# the unquoted word in this order:
# - a flag cluster (`-O`, `-OO`, `-bb`, `-IsE`) is skipped;
# - a `-W`/`-X` cluster takes its argument attached (`-Xdev`, `-uWerror`,
#   `-X=dev`, which CPython 3.14 takes as an unknown `-X` value and runs on)
#   or, with nothing attached, as the next word whatever it is (`-X dev`,
#   `-X "dev mode"`; `python3 -X -m lab.x` hands `-m` to `-X` and then opens
#   `lab.x` as a script). It is tried before the `-m` cluster, so `-Wm lab.x`
#   and `-Xm lab.x` hand `m` to `-W`/`-X` and open `lab.x` as a script, which
#   names no lab script, as CPython does;
# - `--check-hash-based-pycs` takes the next word, which unquoted must be one
#   of its three modes: CPython 3.14 rejects `--check-hash-based-pycs
#   sometimes -m lab.x` ("must be one of 'default', 'always', or 'never'")
#   before it runs anything, so that is no command;
# - `-m` may close a flag cluster and takes its module attached or as the
#   next word: CPython 3.12 runs `-mlab.x`, `-Im lab.x`, `-OOm lab.x` and
#   `-Imlab.x` alike (`-Rm` on 3.14);
# - `--` ends the options and the next word is the script path: `python3 --
#   s.py` and `python3 -O -- s.py` run the script on CPython 3.14, while
#   `python3 -- -m json.tool` fails with "can't open file '.../-m'", so its
#   script target is `-m`, which names no lab script;
# - any other word is the script path. `-c…`, `-h…`, `-?`, `-V…` and a
#   `--long` option therefore end the command as its target word, which
#   names no lab script, and what follows is left to the prose contract.
FLAG_CLUSTER = re.compile(rf"-{PYTHON_FLAG}+")
ARGUMENT_OPTION = re.compile(rf"-{PYTHON_FLAG}*[WX](?P<argument>.*)")
MODULE_OPTION = re.compile(rf"-{PYTHON_FLAG}*m(?P<module>.*)")
PYCS_OPTION = "--check-hash-based-pycs"
PYCS_MODES = frozenset({"always", "default", "never"})
OPTIONS_END = "--"
# The next shell word after whitespace, where the tokenizer stands after the
# interpreter or the word before; nothing at the end of the line or at a word
# the shell would reject.
NEXT_WORD = re.compile(rf"\s+(?P<word>{SHELL_WORD})")
# A redirection the shell removes before CPython sees its arguments, so it may
# sit anywhere among the options and target: `python3 2>/dev/null -m lab.x`,
# `python3 </dev/null -m lab.x`, `python3 -m 2>&1 lab.x` and `python3
# -X>log dev lab/x.py` run as if it were absent. A file descriptor number
# counts only as a whole word before `<` or `>`; the operator, including the
# `>&`, `<&`, `&>` and `&>>` duplications, is followed by its one target word.
# An escaped operator is word text, not a redirection, and ends the scan.
REDIRECTION = re.compile(
    rf"\s*(?:(?<=\s)[0-9]+(?=[<>]))?(?<!\\)(?:<<<|<<-?|<&|<>|<|>&|>>|>\||>|&>>?)\s*{SHELL_WORD}"
)


# The interpreter is read as a whole shell word (`INTERPRETER_WORD`), like
# the option words and the target: bash 3.2 runs `"python"3 -m json.tool`,
# `'pyth'on3 -m json.tool`, `python"3" -m json.tool` and `"/usr/bin/pyth"on3
# -m json.tool` exactly as `python3`, so a name the shell assembles from
# pieces is as runnable as a bare or a wholly quoted one. ``runs_python``
# strips the matching quotes, takes the basename after the last `/`
# (`/usr/bin/python3` runs Python as `python3` does) and accepts the word
# only if it is wholly `python`, `python3` or `python3.N` (`INTERPRETER`):
# `mypython3`, `cpython3` and `python2` are other programs, and bash answers
# `mypython3 -m json.tool` with "command not found". A word starts where no
# unquoted word character precedes it, so `env python3 …`, `docker exec "$c"
# python3 …`, `$(python3 …)`, inline-code `` `python3 …` `` and the inner
# command of `sh -c "python3 …"` are read as before, while `mypython3` is
# one word. ``python_commands`` selects executable positions and requires a
# candidate to cover its whole shell word, so the tail `python3` after the
# closing quote of `"my"python3` cannot become another command. The candidate
# remains a lookahead to expose the interpreter's start and end. A word that
# opens a quote and never closes it
# (`"python3' -m lab.a.b`) is a shell syntax error and matches nothing, as
# before. ``command_target`` reads the words after a candidate that names
# Python, and the reported command is the line from the interpreter word to
# the target word.
INTERPRETER = re.compile(r"python(?:3(?:\.\d+)?)?")
INTERPRETER_CANDIDATE = re.compile(rf"(?=(?<![^{WORD_BREAK}'\"])(?P<interpreter>{INTERPRETER_WORD}))")
# The `-m` target and the script path are read as whole shell words, since
# bash hands `-m "lab.prism."deleted`, `-m lab."prism".deleted` and
# `"./lab/prism/"deleted.py` over exactly as their bare spellings (verified
# with `python3 -m "json."tool` on bash 3.2 and CPython 3.14).
# ``command_target`` strips the matching quotes and ``dead_commands`` normalizes
# POSIX script path components without following filesystem links, then reads
# the word against the target grammar: a word that then names no `lab`
# module or script (`$VAR`, `json.tool`, `lab.prism.` with nothing after the
# dot) is not a `lab` command.
# CPython's `-m` resolves filenames, not Python identifiers: hyphens,
# leading digits, Unicode and quoted spaces can all name runnable modules.
# Keep nonempty dotted segments. ``literal_word`` rules out expansions and
# escapes before this grammar sees a module, preserving quoted literal `$`
# and backslash characters. Paths still stay outside this lexical check.
MODULE_TARGET = re.compile(r"lab(?:\.[^./\x00]+)+")
# Script filenames may also contain literal spaces, dollars, backslashes or
# glob characters when quoted. Expansion checks happen before this grammar.
SCRIPT_TARGET = re.compile(r"lab/[^\x00]+\.py")
MATCHING_QUOTES = re.compile(QUOTED_STRING)
WORD_PART = re.compile(rf"{QUOTED_STRING}|(?:{UNQUOTED_CHARACTER})+")
# A `#` opens a shell comment only where a word starts: at the start of the
# line or after whitespace or a metacharacter (`WORD_BREAK`), neither quoted
# nor escaped. bash 5.2 runs nothing from `# echo NO`, `true;#echo NO` or the
# tail of `echo a # echo NO`, while `echo '#'`, `echo "# x"`, `echo \#`,
# `echo a#b`, `echo $#`, `echo ${#a[@]}`, `echo a\ #b`, `echo $'\'#'` and
# `echo "\"#"` each print a `#` or a count and run the command after `;`.
# Inside a fenced code block (`FENCE`) the comment ends as bash ends it: at the
# end of the line, backticks included (`# was `echo NO`` and `echo ok # was
# `echo NO`` ran no `echo NO`), unless it opened inside a backtick command
# substitution, which it ends at the closing backtick (bash printed `RUN` for
# `` out=`true # echo NO`; echo RUN ``). Outside a fence a backtick opens or
# closes Markdown inline code, a shell context of its own: the comment ends at
# the next backtick and no quote carries across one, so a heading, an issue
# `#303`, a `(#anchor)` link or a "don't" hides no `` `python3 …` `` after it.
# A backslash inside a comment is comment text and continues nothing: bash
# ran the line after `# c \`.
COMMENT_BOUNDARY = re.compile(rf"[{WORD_BREAK}]")
# A line opening or closing a fenced code block: three or more backticks,
# which an info string may not contain, or tildes, at any indentation so a
# fence nested in a list item counts. A closing fence repeats the opening
# character at least as many times, with nothing after it.
FENCE = re.compile(r"\s*(`{3,}(?=[^`]*$)|~{3,})")
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


def literal_word(word: str, *, script: bool = False) -> str | None:
    """Unquote a validated shell word only when its parts need no expansion or decoding.

    Single quotes preserve every character; ANSI-C quotes preserve dollars
    but require decoding for backslashes. Double quotes still expand dollars
    and backticks, and escape `$`, backticks, double quotes and backslashes.
    A backslash before any other character in double quotes is literal.
    Script paths also exclude unquoted glob and brace syntax, whose expansion
    depends on the shell and filesystem; quoting preserves those characters.
    """
    parts = []
    for match in WORD_PART.finditer(word):
        part = match.group(0)
        if part.startswith("'"):
            part = part[1:-1]
        elif part.startswith("$'"):
            part = part[2:-1]
            if "\\" in part:
                return None
        elif part.startswith(('"', '$"')):
            part = part.removeprefix("$")[1:-1]
            if part.endswith("\\") or re.search(r'[$`]|\\[$`"\\]', part):
                return None
        elif any(character in part for character in "$\\`"):
            return None
        elif script and any(character in part for character in "*?[]{}"):
            return None
        parts.append(part)
    return "".join(parts)


def runs_python(word: str) -> bool:
    """Whether ``word`` names CPython: unquoted, its basename is ``python``, ``python3`` or ``python3.N``."""
    return INTERPRETER.fullmatch(unquote(word).rsplit("/", 1)[-1]) is not None


def shell_tokens(line: str):
    """``(kind, start, end)`` for words and operators, without entering quoted data.

    This lexer keeps escaped quotes inside their word, even though interpreting
    escapes in executable names and targets remains outside the check. An
    unmatched prose apostrophe ends at Markdown inline code; a quoted word
    with a matching close keeps its literal backticks.
    """
    position = 0
    while position < len(line):
        if line[position].isspace():
            if line[position] == "\n":
                yield "operator", position, position + 1
            position += 1
            continue
        start = position
        if line[position] in "|&;()<>`":
            position += 1
            if line[start] in "<>" and line.startswith(line[start], position):
                position += 1
                if line[start] == "<" and position < len(line) and line[position] in "<-":
                    position += 1
            elif line.startswith((">&", "<&", ">|", "&>"), start):
                # One redirection operator: its `&` or `|` separates no command.
                position += 1
                if line.startswith("&>>", start):
                    position += 1
            yield "operator", start, position
            continue
        while position < len(line) and not re.fullmatch(rf"[{WORD_BREAK}]", line[position]):
            character = line[position]
            if character == "\\":
                position += 2
            elif character in "'\"":
                quote_start = position
                ansi = character == "'" and position > start and line[position - 1] == "$"
                position += 1
                while position < len(line) and line[position] != character:
                    position += 2 if line[position] == "\\" and (character == '"' or ansi) else 1
                if position >= len(line):
                    backtick = line.find("`", quote_start)
                    if backtick != -1:
                        position = backtick
                        break
                    yield "unfinished", start, len(line)
                    return
                position += 1
            else:
                position += 1
        yield "word", start, min(position, len(line))


SHELLS = frozenset({"sh", "bash", "dash", "ksh", "zsh"})
LAUNCHERS = frozenset({"env", "sudo", "nohup", "command", "exec", "docker", "podman"})
WRAPPER_ARGUMENTS = {
    "env": {"-u", "--unset", "-C", "--chdir"},
    "sudo": {"-u", "--user", "-g", "--group", "-h", "--host", "-p", "--prompt", "-C", "--close-from", "-D", "--chdir", "-R", "--chroot", "-r", "--role", "-t", "--type", "-T", "--command-timeout"},
    "exec": {"-a"},
    "docker": {"-e", "--env", "--env-file", "-u", "--user", "-w", "--workdir", "--detach-keys"},
}
SHELL_ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\+?=")


def env_split_words(source: str) -> list[tuple[str, int]] | None:
    """Literal env -S arguments and their line offsets, without shell evaluation.

    Quotes group words, and # comments begin only at an unquoted word start.
    Shell operators are ordinary argv characters. Expansion and escape syntax
    are outside this check, just as for literal shell words.
    """
    if "$" in source or "\\" in source:
        return None
    result = []
    word = []
    quote = ""
    started = False
    number = 0
    first_line = 0
    for character in source:
        if not quote and character in " \t\n\r\v\f":
            if started:
                result.append(("".join(word), first_line))
                word, started = [], False
        elif not quote and not started and character == "#":
            break
        else:
            if not started:
                started, first_line = True, number
            if quote:
                if character == quote:
                    quote = ""
                else:
                    word.append(character)
            elif character in "'\"":
                quote = character
            else:
                word.append(character)
        number += character == "\n"
    if quote:
        return None
    if started:
        result.append(("".join(word), first_line))
    return result


def executable_word(words: list[str], offsets: list[int]) -> int | None:
    """Find the executable, expanding literal env -S argv and line offsets in place."""
    index = 0
    # Reserved words are syntax only when unquoted in shell command position;
    # after an assignment or launcher they are ordinary executable arguments.
    while index < len(words) and words[index] in {"!", "if", "then", "elif", "else", "do", "{", "while", "until", "time", "coproc"}:
        prefix = words[index]
        index += 1
        # Bash's time prefix accepts one unquoted -p; quoted spellings are
        # executable words, just as quoted time is not a reserved word.
        if prefix == "time" and index < len(words) and words[index] == "-p":
            index += 1
        # A coprocess name precedes a compound command, never a simple one.
        # Only unquoted openers handled by this loop introduce such a command.
        if prefix == "coproc" and index + 1 < len(words) and words[index + 1] in {"{", "if", "while", "until"}:
            index += 1
    # Bash recognizes += only in the leading assignment list, with an
    # unquoted name and operator. After a launcher it is an ordinary argument.
    while index < len(words) and SHELL_ASSIGNMENT.match(words[index]):
        index += 1
    while index < len(words):
        word = unquote(words[index])
        program = word.rsplit("/", 1)[-1]
        if program not in LAUNCHERS:
            return index
        index += 1
        if program in {"docker", "podman"}:
            if index == len(words) or unquote(words[index]) != "exec":
                return None
            program = "docker"
            index += 1
        terminated = False
        while index < len(words):
            option = unquote(words[index])
            # S takes the rest of a short cluster, or the next word, as its
            # source; only argument-free flags may precede it in the cluster.
            cluster = re.fullmatch(r"-[iv]*S(.*)", option, re.DOTALL) if program == "env" else None
            if cluster or program == "env" and (option == "--split-string" or option.startswith("--split-string=")):
                separate = option == "--split-string" or (cluster is not None and not cluster.group(1))
                argument = index + int(separate)
                if argument >= len(words):
                    return None
                source = literal_word(words[argument])
                if source is None:
                    return None
                if not separate:
                    source = source[cluster.start(1) if cluster else len("--split-string="):]
                split = env_split_words(source)
                if split is None:
                    return None
                # Quote every argv word so operators and shell reserved words
                # remain data when the expanded command is read below.
                quoted = ["'" + word.replace("'", "'\"'\"'") + "'" for word, _ in split]
                origin = offsets[argument]
                words[index:argument + 1] = quoted
                offsets[index:argument + 1] = [origin + offset for _, offset in split]
                continue
            if option in {"--help", "--version"} or (program == "sudo" and option in {"-l", "-ll", "--list", "-V"}):
                return None
            if program == "env" and (option == "--null" or re.match(r"-[iv]*0", option)):
                # `-0`/`--null` prints the environment NUL-terminated and takes
                # no command, so like `--help` it ends the command: GNU
                # coreutils 9.4 answered `env -0 echo hi`, `env --null echo
                # hi`, `env -i0 echo hi`, `env -0i echo hi`, `env -v0 echo
                # hi`, `env -0u FOO echo hi`, `env -0S 'echo hi'`, `env -0 -S
                # 'echo hi'` and `env -S '-0 echo hi'` alike with "cannot
                # specify --null (-0) with command" and exit 125, and printed
                # `FOO=x` NUL-terminated for `env -i -0 FOO=x`. Only
                # argument-free flags may precede the `0` in a cluster, as
                # with the sudo modes below: `env -u0 echo hi` unsets the
                # variable `0` and ran. An abbreviated `--nul` is outside this
                # check, as it is for `--help`.
                return None
            if program == "sudo" and (
                option in {"--edit", "--remove-timestamp", "--validate"}
                or re.match(r"-[ABbEHkNnPSis]*[eKv]", option)
            ):
                # Edit and credential-only modes run no command. Only flags
                # without arguments may precede the mode letter in a cluster:
                # -nv validates, while -pv gives p the prompt "v". Lowercase
                # -k resets credentials but still permits a command to run.
                return None
            if program == "command" and option in {"-v", "-V"}:
                return None  # executable lookup prints information; it runs nothing
            if option == "--":
                index += 1
                terminated = True
                break
            if not option.startswith("-"):
                break
            index += 2 if option in WRAPPER_ARGUMENTS.get(program, ()) else 1
        if program == "docker":
            index += 1  # container name
        elif program == "env" or (program == "sudo" and not terminated):
            # Only env and sudo take VAR=value words between their options and
            # the command: `env [OPTION]... [-] [NAME=VALUE]... [COMMAND
            # [ARG]...]` and `sudo [options] [VAR=value] [-i | -s] [command
            # [arg ...]]` in their usage lines. Both accept FOO+=value too,
            # setting the literal name FOO+ (sudo 1.9.15p5 exported `FOO+=x`
            # for `sudo FOO+=x env`, as GNU coreutils 9.4 env does), and both
            # receive a quoted `'FOO=x'` as the same word once the shell
            # strips the quotes. After `--` they part: env still exported FOO
            # for `env -- FOO=x sh -c 'echo $FOO'`, while sudo answered `sudo
            # -- FOO=x sh -c '…'` with "FOO=x: command not found". No other
            # launcher takes one: nohup 9.4 answered `nohup FOO=x echo hi`
            # with "failed to run command 'FOO=x'", bash 5.2.21 `command
            # FOO=x echo hi` with "FOO=x: command not found" and `exec FOO=x
            # echo hi` with "exec: FOO=x: not found", each exit 127 and each
            # running nothing, and `docker exec [OPTIONS] CONTAINER COMMAND
            # [ARG...]` runs the word after the container as the program. A
            # `FOO=x` after those is therefore the executable word. sudo's
            # own test is wider (it also took `9FOO=x`); such spellings are
            # outside this check.
            while index < len(words) and SHELL_ASSIGNMENT.match(unquote(words[index])):
                index += 1
    return None


def shell_command_argument(words: list[str], shell: str) -> int | None:
    """Locate a literal shell's command string after consuming its option arguments."""
    index = 0
    command_string = False
    noexec = False
    argument_flags = "oO" if shell in {"bash", "sh"} else "o"
    while index < len(words):
        option = unquote(words[index])
        if option in {"--help", "--version"}:
            return None
        if option == "--":
            index += 1
            break
        if shell in {"bash", "sh"} and option in {"--rcfile", "--init-file"}:
            index += 2
            continue
        if option.startswith("--"):
            index += 1
            continue
        if re.fullmatch(r"[+-][A-Za-z]+", option) is None:
            break
        index += 1
        # Process flags in order: +n/+o noexec can undo an earlier -n,
        # including options between -c and its command-string argument.
        for flag in option[1:]:
            command_string |= flag == "c"
            if flag == "n":
                noexec = option[0] == "-"
            if flag in argument_flags:
                if index >= len(words):
                    return None
                if flag == "o" and unquote(words[index]) == "noexec":
                    noexec = option[0] == "-"
                index += 1
    return index if command_string and not noexec and index < len(words) else None


def python_commands(line: str):
    """``(line offset, source, match)`` for Python in executable positions, including shell ``-c``.

    Whole words keep ordinary arguments opaque. Only a supported shell's
    literal command-string argument starts another shell context; Python's
    ``-c`` argument and strings passed to printf, echo, etc. remain data.
    """
    words: list[tuple[int, int]] = []
    redirect = False
    for kind, start, end in [*shell_tokens(line), ("operator", len(line), len(line))]:
        token = line[start:end]
        if kind == "unfinished":
            return
        if kind == "word":
            if not redirect:
                words.append((start, end))
            redirect = False
            continue
        if token.startswith(("<", ">", "&>")):
            if token[0] != "&" and words and words[-1][1] == start and re.fullmatch(r"[0-9]+", line[words[-1][0]:start]):
                words.pop()  # the adjacent number is a file descriptor, not a word
            redirect = True
            continue
        command_words = [line[a:b] for a, b in words]
        original_words = command_words.copy()
        offsets = [line[:a].count("\n") for a, _ in words]
        index = executable_word(command_words, offsets)
        if index is not None:
            command_line, spans = line, words
            if command_words != original_words:
                command_line = " ".join(command_words)
                spans = []
                position = 0
                for word in command_words:
                    spans.append((position, position + len(word)))
                    position += len(word) + 1
            first, last = spans[index]
            interpreter = command_line[first:last]
            match = INTERPRETER_CANDIDATE.match(command_line, first)
            if runs_python(interpreter) and match is not None and match.end("interpreter") == last:
                yield offsets[index], command_line, match
            elif unquote(interpreter).rsplit("/", 1)[-1] in SHELLS:
                argument = shell_command_argument(
                    command_words[index + 1:], unquote(interpreter).rsplit("/", 1)[-1]
                )
                if argument is not None:
                    argument += index + 1
                    source = literal_word(command_words[argument])
                    if source is not None:
                        for number, nested in shell_lines(source, shell_source=True):
                            for offset, source_line, match in python_commands(nested):
                                yield offsets[argument] + number - 1 + offset, source_line, match
        words = []
        redirect = False


def shell_words(line: str, position: int):
    """Each ``NEXT_WORD`` match on ``line`` from ``position``, until no word follows.

    The words end at the end of the line and at a word with an unterminated or
    mismatched quote, which bash rejects before anything runs. Redirections
    (``REDIRECTION``) between the words are skipped with their target word.
    """
    while True:
        while (redirection := REDIRECTION.match(line, position)) is not None:
            position = redirection.end()
        if (match := NEXT_WORD.match(line, position)) is None:
            return
        yield match
        position = match.end()


def command_target(line: str, position: int) -> tuple[str, str, int] | None:
    """``(kind, target, end)`` of the command whose interpreter word ends at ``position``.

    Reads the words after the interpreter against the option grammar (see
    ``FLAG_CLUSTER``) until one names the target: ``kind`` is ``"module"``
    after ``-m`` and ``"script"`` otherwise, ``target`` is the word as the
    shell hands it to CPython (matching quotes stripped, or the remainder of
    an attached ``-m``), and ``end`` is where the target word ends on the
    line. ``None`` when the words run out, at a word the shell rejects, or at
    a ``--check-hash-based-pycs`` mode CPython rejects, or when a
    target word requires shell expansion or escape decoding outside this check.
    """
    words = shell_words(line, position)
    for match in words:
        word = unquote(match.group("word"))
        if FLAG_CLUSTER.fullmatch(word):
            continue
        if (option := ARGUMENT_OPTION.fullmatch(word)) is not None:
            if not option.group("argument") and next(words, None) is None:
                return None
            continue
        if word == PYCS_OPTION:
            mode = next(words, None)
            if mode is None or unquote(mode.group("word")) not in PYCS_MODES:
                return None
            continue
        if (option := MODULE_OPTION.fullmatch(word)) is not None:
            attached = bool(option.group("module"))
            target = match if attached else next(words, None)
            if target is None or (module := literal_word(target.group("word"))) is None:
                return None
            if attached:
                module = module[option.start("module"):]
            return "module", module, target.end()
        if word == OPTIONS_END:
            target = next(words, None)
        else:
            target = match
        if target is None or (script := literal_word(target.group("word"), script=True)) is None:
            return None
        return "script", script, target.end()
    return None


def blank_comments(line: str, *, fenced: bool) -> str:
    """``line`` with each shell comment (see ``COMMENT_BOUNDARY``) turned to spaces.

    ``fenced`` says whether the line sits in a fenced code block. Quotes are
    followed as bash reads them: nothing escapes inside ``'…'``, a backslash
    escapes the next character inside ``"…"`` and ``$'…'`` and outside
    quotes. Columns are kept, so a command before a comment reads and reports
    as before.
    """
    characters = list(line)
    quote = ""
    word_start = True
    in_backticks = False
    index = 0
    while index < len(line):
        character = line[index]
        if quote and (fenced or character != "`"):
            if character == "\\" and quote != "'":
                index += 1
            elif character == quote[-1]:
                quote = ""
        elif character == "#" and word_start:
            end = line.find("`", index) if in_backticks or not fenced else -1
            end = len(line) if end == -1 else end
            characters[index:end] = " " * (end - index)
            index = end
            continue
        elif character == "\\":
            index += 1
        elif character in "'\"":
            quote = character
        elif character == "$" and line.startswith("'", index + 1):
            quote = "$'"
            index += 1
        elif character == "`":
            quote = ""
            in_backticks = not in_backticks
        word_start = not quote and COMMENT_BOUNDARY.fullmatch(character) is not None
        index += 1
    return "".join(characters)


def shell_lines(text: str, *, shell_source: bool = False) -> list[tuple[int, str]]:
    """``(first line, text)`` per logical shell line, backslash continuations joined, comments blanked.

    Each joined line is blanked again as a whole, in the fence state of its
    first line, since a quote may carry across the continuation (bash printed
    ``a # b`` for ``echo "a \\`` followed by ``# b"``). A backslash that ends
    a comment is blanked with it, so it joins nothing and the next physical
    line is a command of its own. Every physical line, joined or not, may open
    or close a fence, and a fence line itself is Markdown, not shell. Literal
    shell command strings set ``shell_source``: their comments end at the
    shell boundary and their backticks cannot open Markdown inline code.
    """
    lines: list[tuple[int, str]] = []
    fence = ""
    fence_indent = ""
    fenced = False
    heredocs: list[tuple[str, bool]] = []
    quoted = False
    for number, line in enumerate(text.splitlines(), 1):
        if heredocs:
            delimiter, strip_tabs = heredocs[0]
            body = line.removeprefix(fence_indent)
            if (body.lstrip("\t") if strip_tabs else body) == delimiter:
                heredocs.pop(0)
            lines.append((number, ""))
            continue
        marker = FENCE.match(line)
        on_fence = not shell_source and marker is not None and (
            not fence or (marker.group(1).startswith(fence) and not line[marker.end():].strip())
        )
        if lines and lines[-1][1].endswith("\\"):
            first, head = lines[-1]
            lines[-1] = (first, blank_comments(head[:-1] + line, fenced=fenced))
        elif quoted and not on_fence:
            first, head = lines[-1]
            lines[-1] = (first, blank_comments(head + "\n" + line, fenced=fenced))
        else:
            fenced = shell_source or (bool(fence) and not on_fence)
            lines.append((number, blank_comments(line, fenced=fenced)))
        if on_fence:
            fence_indent = "" if fence else line[:marker.start(1)]
            fence = "" if fence else marker.group(1)
        logical = lines[-1][1]
        tokens = list(shell_tokens(logical))
        # A prose apostrophe in the first word is not a multiline shell
        # argument. Fences and arguments after a command may carry quotes.
        quoted = bool(tokens) and tokens[-1][0] == "unfinished" and (fenced or len(tokens) > 1)
        if not quoted and not logical.endswith("\\"):
            for token_index, (kind, start, end) in enumerate(tokens[:-1]):
                operator = logical[start:end]
                if kind == "operator" and operator in {"<<", "<<-"}:
                    kind, start, end = tokens[token_index + 1]
                    if kind == "word":
                        # Delimiters undergo POSIX quote removal, not the
                        # expansions required of ordinary shell arguments.
                        try:
                            delimiter = shlex.split(logical[start:end])[0]
                        except ValueError:
                            continue  # shell-specific quote decoding is outside this check
                        heredocs.append((delimiter, operator == "<<-"))
    return lines


def dead_commands(text: str, tracked: frozenset[str]) -> list[tuple[int, str, str]]:
    """``(first line, command, missing path)`` for each runnable command with no target.

    The command is the line from the interpreter word to the target word. A
    dead ``-m`` command reports both paths that would make it runnable, joined
    by ``or``, so the reader is not sent to create ``lab/a/b.py`` beside a
    tracked ``lab/a/b/`` package that merely lacks ``__main__.py``.
    """
    found = []
    for number, line in shell_lines(text):
        for offset, source, match in python_commands(line):
            target = command_target(source, match.end("interpreter"))
            if target is None:
                continue
            kind, word, end = target
            command = source[match.start("interpreter"):end]
            if kind == "module":
                if not MODULE_TARGET.fullmatch(word):
                    continue
                candidates = runnable_candidates(word)
                if not any(candidate in tracked for candidate in candidates):
                    found.append((number + offset, command, " or ".join(candidates)))
            else:
                if not word.endswith(".py"):
                    continue
                script = posixpath.normpath(word)
                if SCRIPT_TARGET.fullmatch(script) and script not in tracked:
                    found.append((number + offset, command, script))
    return found


def dangling_references(text: str, tracked: frozenset[str]) -> list[tuple[int, str]]:
    """``(line, reference)`` for each ``lab/prism`` mention that resolves to nothing."""
    found = []
    for number, line in enumerate(text.splitlines(), 1):
        for match in PATH_REFERENCE.finditer(line):
            prefix = line[: match.start()]
            if PINNED_GITHUB_URL.search(prefix) or not PATH_ROOT_PREFIX.search(prefix):
                continue
            reference = match.group("literal") or match.group("bare").rstrip(PROSE_TRAILING_PUNCTUATION)
            if match.group("bare") and URL_REFERENCE_PREFIX.search(line[: match.start()]):
                reference = re.split(r"[?#]", reference, maxsplit=1)[0]
            normalized = posixpath.normpath(reference)
            if normalized == "lab" or normalized.startswith("lab/"):
                if reference.endswith("/"):
                    if any(path.startswith(normalized + "/") for path in tracked):
                        continue
                elif normalized in tracked:
                    continue
            found.append((number, reference))
        for match in MODULE_REFERENCE.finditer(line):
            if not MODULE_ROOT_PREFIX.search(line[: match.start("literal" if match.group("literal") else "bare")]):
                continue
            reference = match.group("literal") or match.group("bare").rstrip(PROSE_TRAILING_PUNCTUATION)
            if not any(c in tracked for c in module_candidates(reference)):
                found.append((number, reference))
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

    def test_prose_references_do_not_resolve_truncated_prefixes(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py"}
        for suffix in (
            "$old", "-deleted", "+old", "=old", "é", r"\old", "/old",
            "!old", "*old", "?old", "#old", ",old", ";old", ":old", "(old", "[old", "{old", "|old",
        ):
            for reference, path in (
                (f"lab/prism/Dockerfile{suffix}", f"lab/prism/Dockerfile{suffix}"),
                (f"lab.prism.tool{suffix}", f"lab/prism/tool{suffix}.py"),
            ):
                with self.subTest(reference=reference):
                    text = f"See {reference}."
                    self.assertEqual(dangling_references(text, tracked), [(1, reference)])
                    self.assertEqual(dangling_references(text, tracked | {path}), [])

    def test_quoted_prose_references_preserve_the_complete_literal(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py"}
        for suffix in (" old", "$old", "-deleted", ".", ",old", "#old", "?old", "[old]", "(old)", "*old", "`old", "](old", "][old"):
            for reference, path in (
                (f"lab/prism/Dockerfile{suffix}", f"lab/prism/Dockerfile{suffix}"),
                (f"lab.prism.tool{suffix}", f"lab/prism/tool{suffix.replace('.', '/')}.py"),
            ):
                for quote in ("`", "'", '"'):
                    if quote in reference:
                        continue
                    with self.subTest(reference=reference, quote=quote):
                        text = f"See {quote}{reference}{quote}."
                        self.assertEqual(dangling_references(text, tracked), [(1, reference)])
                        self.assertEqual(dangling_references(text, tracked | {path}), [])

    def test_bare_prose_references_stop_at_surrounding_punctuation(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py"}
        for reference in ("lab/prism/Dockerfile", "lab.prism.tool"):
            for surrounding in (
                "{}.", "{},", "{};", "{}:", "{}!", "{}?", "({})", "[{}]", "<{}>", "**{}**",
                "[{}](https://example.com)", "[{}][build]",
            ):
                with self.subTest(reference=reference, surrounding=surrounding):
                    self.assertEqual(dangling_references(surrounding.format(reference), tracked), [])
                    self.assertEqual(
                        dangling_references(surrounding.format(reference + "$old"), tracked),
                        [(1, reference + "$old")],
                    )
        self.assertEqual(
            dangling_references("https://github.com/o/r/blob/main/lab/prism/Dockerfile#L1", tracked),
            [],
        )

    def test_quoted_directory_references_require_a_tracked_descendant(self) -> None:
        self.assertEqual(self.references("See `lab/prism/`."), [])
        self.assertEqual(self.references("See `lab/prism/Dockerfile/`."), ["lab/prism/Dockerfile/"])

    def test_prose_references_do_not_include_unrelated_namespaces(self) -> None:
        for reference in ("lab/prismatic/tool", "lab.prismatic.tool", "lab/prism-old/tool", "lab.prism-old.tool"):
            with self.subTest(reference=reference):
                self.assertEqual(self.references(f"See `{reference}`."), [])

    def test_prose_reference_roots_require_a_leading_word_boundary(self) -> None:
        for root in ("lab/prism/deleted.py", "lab.prism.deleted"):
            for prefix in ("col", "my_", "3", "é"):
                for quote in ("", "`", "'", '"'):
                    with self.subTest(root=root, prefix=prefix, quote=quote):
                        self.assertEqual(self.references(f"See {quote}{prefix}{root}{quote}."), [])

    def test_module_references_start_at_a_token_root(self) -> None:
        root = "lab.prism.deleted"
        for text in (
            f"See {root}.", f"See `{root}`.", f"See '{root}'.", f'See "{root}".', f"{root} is gone.",
            f"[old]({root})", f"[{root}](https://example.com)", f"[{root}][old]", f"<{root}>", f"**{root}**",
            f"|{root}|", f"[old](<{root}>)", f"(`{root}`)", f'["{root}"]', f'See `"{root}"`.',
            f"Don't lose the students' `{root}`.", f"It's {root}, isn't it?", f"Run `python3 -m {root}`.",
            f"(**`{root}`**)", f"from {root} import main", f"Run `python3 -m $'{root}'`.", f'See $"{root}".',
        ):
            with self.subTest(text=text):
                self.assertEqual(self.references(text), [root])
                self.assertEqual(dangling_references(text, self.TRACKED | {"lab/prism/deleted.py"}), [])
        for prefix in (
            "vendor.", "a.", "_", "my-", "my*", "my(", "my=", "my[", "my|", "my<", "my>", "--module=", "vendor/",
            "/tmp/", "./", "../", "/", "~/", "$HOME/", "@", "+", "C:\\", "https://example.com/",
            "https://example.com/?m=", "https://github.com/o/r/blob/main/",
        ):
            for quote in ("", "`", "'", '"'):
                for text in (f"See {quote}{prefix}{root}{quote}.", f"See {prefix}{quote}{root}{quote}."):
                    with self.subTest(text=text):
                        self.assertEqual(self.references(text), [])
        for text in (f"See ${root}.", f"See `${root}`.", f"See my$'{root}'."):
            with self.subTest(text=text):
                self.assertEqual(self.references(text), [])
        for text in (
            f"See `vendor.{root}` and `{root}`.", f"See https://example.com/{root} and {root}.",
            f'See vendor."{root}" and "{root}".', f"[old](https://example.com/{root}) and [{root}](x)",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.references(text), [root])

    def test_path_references_start_at_the_repository_root(self) -> None:
        root = "lab/prism/deleted.py"
        for text in (
            f"See {root}.", f"See `{root}`.", f"See '{root}'.", f'See "{root}".', f"See `./{root}`.",
            f'See "./{root}".', f"{root} is gone.", f"[old]({root})", f"[old](./{root})", f"<{root}>",
            f"**{root}**", f"|{root}|", f"[old](<{root}>)", f"(`{root}`)", f'["{root}"]', f'See `"{root}"`.',
            f"https://github.com/o/r/blob/main/{root}#L1", f"https://github.com/o/r/tree/main/{root}",
            f"https://github.com/o/r/raw/main/{root}", f"See `https://github.com/o/r/blob/main/{root}`.",
            f'"https://github.com/o/r/blob/main/{root}"', f"Don't lose the students' `{root}`.",
            f"It's {root}, isn't it?", f"Run `echo {root}`.", f"Run `python3 {root}`.", f"(**`{root}`**)",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.references(text), [root])
                self.assertEqual(dangling_references(text, self.TRACKED | {root}), [])
        for prefix in (
            "my-", "my*", "my(", "my=", "my[", "my|", "my<", "my>", "--out=", "vendor/", "/tmp/", "../", "/", "~/",
            "a.", "$HOME/", "@", "+", "C:\\", "my-\"", "https://example.com/",
            "https://github.com/o/r/blob/main/vendor/",
        ):
            for quote in ("", "`", "'", '"'):
                with self.subTest(prefix=prefix, quote=quote):
                    self.assertEqual(self.references(f"See {quote}{prefix}{root}{quote}."), [])
        for text in (f"See `my*(**{root}`.", f"See `my[`{root}`.", f"See `x](my*{root}`."):
            with self.subTest(text=text):
                self.assertEqual(self.references(text), [])
        self.assertEqual(self.references(f"See `vendor/{root}` and `{root}`."), [root])
        self.assertEqual(self.references(f"The students' `my*{root}` and `{root}` differ."), [root])

    def test_blockquote_markers_start_reference_tokens(self) -> None:
        # A Markdown blockquote marker needs no space after it, so `>lab/prism/x`
        # and the nested `>>lab/prism/x` name the root path as `> lab/prism/x`
        # does. A `>` inside a token (`my>lab/prism`) opens nothing, as `my<`
        # does not: an opener counts only at the start of the line or after
        # whitespace.
        for root in ("lab/prism/deleted.py", "lab.prism.deleted"):
            for surrounding in (
                ">{}", ">>{}", "> >{}", "  >{}", ">`{}`", ">'{}'", '>"{}"', ">**{}**",
                ">(`{}`)", ">{} is gone.", ">>`{}`.",
            ):
                text = surrounding.format(root)
                with self.subTest(text=text):
                    self.assertEqual(self.references(text), [root])
                    self.assertEqual(dangling_references(text, self.TRACKED | {"lab/prism/deleted.py"}), [])
            for surrounding in ("my>{}", "my>>{}", "x>>{}", "`my>{}`", "<b>{}", "->{}", "=>{}"):
                text = surrounding.format(root)
                with self.subTest(text=text):
                    self.assertEqual(self.references(text), [])
        self.assertEqual(self.references(">./lab/prism/deleted.py"), ["lab/prism/deleted.py"])
        self.assertEqual(self.references("> ./lab/prism/deleted.py"), ["lab/prism/deleted.py"])

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

    def test_non_identifier_module_targets_are_caught(self) -> None:
        for module in (
            "lab.prism.deleted-module",
            "lab.prism.123module",
            "lab.prism.-module",
            "lab.prism.café",
            "lab.prism.module name",
            "lab.prism.module+name",
            "lab.prism.pkg-name.deleted-module",
        ):
            relative = module.replace(".", "/")
            missing = f"{relative}.py or {relative}/__main__.py"
            for option in (f"-m '{module}'", f'-m "{module}"', f"-OOm'{module}'"):
                with self.subTest(module=module, option=option):
                    command = f"python3 {option}"
                    self.assertEqual(dead_commands(command, self.TRACKED), [(1, command, missing)])
                    self.assertEqual(dead_commands(command, self.TRACKED | {f"{relative}.py"}), [])
                    self.assertEqual(dead_commands(command, self.TRACKED | {f"{relative}/__main__.py"}), [])
                    tracked = self.TRACKED | {relative, f"{relative}/__init__.py"}
                    self.assertEqual(dead_commands(command, tracked), [(1, command, missing)])

    def test_hyphenated_module_spellings_report_the_complete_target(self) -> None:
        missing = "lab/prism/deleted-module.py or lab/prism/deleted-module/__main__.py"
        # An existing identifier prefix must not make the full target pass.
        tracked = self.TRACKED | {"lab/prism/deleted.py"}
        for command in (
            "python3 -m lab.prism.deleted-module",
            "python3 -mlab.prism.deleted-module",
            'python3 -m "lab.prism."deleted-module',
            "python3 -m $'lab.prism.deleted-module'",
        ):
            with self.subTest(command=command):
                self.assertEqual(dead_commands(command, tracked), [(1, command, missing)])
        text = "```bash\npython3 -m \\\n  lab.prism.deleted-module --help\n```"
        self.assertEqual(self.located(text), [(2, missing)])

    def test_hyphenated_command_cannot_hide_within_reference_ratchet(self) -> None:
        prose = "The retired module `lab.prism.deleted` is historical."
        command = "python3 -m lab.prism.deleted-module"
        self.assertEqual(len(self.references(prose)), len(self.references(command)))
        self.assertEqual(
            self.commands(command),
            ["lab/prism/deleted-module.py or lab/prism/deleted-module/__main__.py"],
        )

    def test_single_quoted_literal_module_targets_are_caught(self) -> None:
        for name in ("deleted$module", r"deleted\module", "deleted`module"):
            module = f"lab.prism.{name}"
            relative = f"lab/prism/{name}"
            missing = f"{relative}.py or {relative}/__main__.py"
            for word in (f"'{module}'", f"lab.prism.'{name}'", f'"lab.prism."\'{name}\''):
                for option in (f"-m {word}", f"-OOm{word}", f'"-m"{word}'):
                    with self.subTest(name=name, option=option):
                        command = f"python3 {option}"
                        self.assertEqual(dead_commands(command, self.TRACKED), [(1, command, missing)])
                        self.assertEqual(dead_commands(command, self.TRACKED | {f"{relative}.py"}), [])
                        self.assertEqual(dead_commands(command, self.TRACKED | {f"{relative}/__main__.py"}), [])
                        tracked = self.TRACKED | {"lab/prism/deleted.py", f"{relative}/__init__.py"}
                        self.assertEqual(dead_commands(command, tracked), [(1, command, missing)])

    def test_other_quotes_preserve_only_literal_module_characters(self) -> None:
        for word, name in (
            ("$'lab.prism.deleted$module'", "deleted$module"),
            ("lab.prism.$'deleted$module'", "deleted$module"),
            (r'"lab.prism.deleted\module"', r"deleted\module"),
            (r'$"lab.prism.deleted\module"', r"deleted\module"),
        ):
            for option in (f"-m {word}", f"-OOm{word}"):
                with self.subTest(option=option):
                    command = f"python3 {option}"
                    relative = f"lab/prism/{name}"
                    missing = f"{relative}.py or {relative}/__main__.py"
                    self.assertEqual(dead_commands(command, self.TRACKED), [(1, command, missing)])
                    self.assertEqual(dead_commands(command, self.TRACKED | {f"{relative}.py"}), [])

    def test_module_expansions_and_escapes_are_not_literal_targets(self) -> None:
        for word in (
            "lab.prism.deleted$module",
            '"lab.prism.deleted$module"',
            '$"lab.prism.deleted$module"',
            r"lab.prism.deleted\module",
            r"$'lab.prism.deleted\module'",
            r'"lab.prism.deleted\\module"',
            r'"lab.prism.deleted\$module"',
            r'"lab.prism.deleted\"',
            '"lab.prism.deleted`module`"',
            "'lab.prism.deleted$module'$SUFFIX",
            "'lab.prism.deleted'\"$MODULE\"",
        ):
            for option in (f"-m {word}", f"-OOm{word}"):
                with self.subTest(option=option):
                    self.assertEqual(self.commands(f"python3 {option}"), [])

    def test_wrapped_literal_module_targets_keep_the_first_line(self) -> None:
        for name in ("deleted$module", r"deleted\module"):
            relative = f"lab/prism/{name}"
            text = f"```bash\npython3 -m \\\n  'lab.prism.{name}' --help\n```"
            self.assertEqual(self.located(text), [(2, f"{relative}.py or {relative}/__main__.py")])

    def test_literal_module_command_cannot_hide_within_reference_ratchet(self) -> None:
        prose = "The retired module `lab.prism.deleted` is historical."
        for name in ("deleted$module", r"deleted\module"):
            command = f"python3 -m 'lab.prism.{name}'"
            with self.subTest(command=command):
                self.assertEqual(len(self.references(prose)), len(self.references(command)))
                relative = f"lab/prism/{name}"
                self.assertEqual(self.commands(command), [f"{relative}.py or {relative}/__main__.py"])

    def test_quoted_literal_script_targets_are_caught(self) -> None:
        for name in ("deleted$file", r"deleted\file", "deleted`file", "deleted file", "déleted", "deleted[ab]*?{x,y}"):
            path = f"lab/prism/{name}.py"
            for word in (f"'{path}'", f"./lab/prism/'{name}'.py", f'"./lab/prism/"\'{name}.py\''):
                for options in ("", "-OO ", "-- ", "-O -- "):
                    with self.subTest(word=word, options=options):
                        command = f"python3 {options}{word}"
                        self.assertEqual(dead_commands(command, self.TRACKED), [(1, command, path)])
                        self.assertEqual(dead_commands(command, self.TRACKED | {path}), [])

    def test_other_quotes_preserve_only_literal_script_characters(self) -> None:
        for word, path in (
            ("$'lab/prism/deleted$file.py'", "lab/prism/deleted$file.py"),
            (r'"lab/prism/deleted\file.py"', r"lab/prism/deleted\file.py"),
            (r'$"lab/prism/deleted\file.py"', r"lab/prism/deleted\file.py"),
            ('"lab/prism/deleted*.py"', "lab/prism/deleted*.py"),
        ):
            for options in ("", "-- "):
                with self.subTest(word=word, options=options):
                    command = f"python3 {options}{word}"
                    self.assertEqual(dead_commands(command, self.TRACKED), [(1, command, path)])
                    self.assertEqual(dead_commands(command, self.TRACKED | {path}), [])

    def test_script_expansions_and_escapes_are_not_literal_targets(self) -> None:
        for word in (
            "lab/prism/deleted$file.py", '"lab/prism/deleted$file.py"',
            r"lab/prism/deleted\file.py", r"$'lab/prism/deleted\file.py'",
            r'"lab/prism/deleted\\file.py"', r'"lab/prism/deleted\$file.py"',
            '"lab/prism/deleted`file`.py"', "lab/prism/deleted*.py",
            "lab/prism/deleted?.py", "lab/prism/deleted[ab].py", "lab/prism/deleted{a,b}.py",
            "'lab/prism/deleted'$FILE.py",
        ):
            for options in ("", "-- "):
                with self.subTest(word=word, options=options):
                    self.assertEqual(self.commands(f"python3 {options}{word}"), [])

    def test_wrapped_literal_script_cannot_hide_within_reference_ratchet(self) -> None:
        path = "lab/prism/deleted$file.py"
        command = f"python3 -- \\\n  '{path}'"
        prose = "The retired script `lab/prism/deleted.py` is historical."
        self.assertEqual(len(self.references(prose)), len(self.references(command)))
        self.assertEqual(self.located(f"```sh\n{command}\n```"), [(2, path)])

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

    def test_equivalent_script_paths_resolve_to_tracked_files(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py"}
        for script in (
            "lab/prism/../prism/tool.py", "lab/./prism/tool.py", "lab//prism/tool.py",
            "././lab/prism/../prism//./tool.py", ".//lab/prism/tool.py",
        ):
            for word in (script, f"'{script}'", f'"{script}"'):
                with self.subTest(word=word):
                    command = f"python3 -O -- {word} --help"
                    self.assertEqual(dead_commands(command, tracked), [])
                    self.assertEqual(dangling_references(command, tracked), [])

    def test_equivalent_missing_script_paths_keep_command_and_line(self) -> None:
        for script in (
            "lab/prism/../prism/deleted.py", "lab/./prism/deleted.py",
            "lab//prism/deleted.py", "./lab/prism/../prism//./deleted.py",
        ):
            with self.subTest(script=script):
                text = f"```sh\npython3 -- \\\n  '{script}' --help\n```"
                self.assertEqual(
                    dead_commands(text, self.TRACKED),
                    [(2, f"python3 --   '{script}'", "lab/prism/deleted.py")],
                )

    def test_normalized_script_paths_outside_lab_are_not_lab_commands(self) -> None:
        for script in (
            "lab/../scripts/tool.py", "lab/prism/../../scripts/tool.py",
            "lab/../../lab/prism/tool.py", "/lab/prism/tool.py",
            "lab/../lab-old/tool.py",
        ):
            with self.subTest(script=script):
                self.assertEqual(self.commands(f"python3 '{script}'"), [])

    def test_equivalent_prose_paths_preserve_directory_and_lab_boundaries(self) -> None:
        for reference in ("lab/prism/./Dockerfile", "lab/prism/../prism//Dockerfile", "lab/prism/../prism/"):
            with self.subTest(reference=reference):
                self.assertEqual(self.references(f"See `{reference}`."), [])
        for reference in (
            "lab/prism/../prism/deleted.py", "lab/prism/../prism/Dockerfile/",
            "lab/prism/../../scripts/tool.py", "lab/prism/../../../lab/prism/Dockerfile",
        ):
            with self.subTest(reference=reference):
                self.assertEqual(
                    dangling_references(f"See `{reference}`.", self.TRACKED | {"scripts/tool.py"}),
                    [(1, reference)],
                )

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
            "python3 -m lab.prism..x",
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

    def test_bash_append_assignments_before_commands_are_scanned(self) -> None:
        for command, missing in (
            ("python3 -m lab.example.deleted", "lab/example/deleted.py or lab/example/deleted/__main__.py"),
            ("python3 lab/example/deleted.py", "lab/example/deleted.py"),
        ):
            for text in (
                f"FOO+=x {command}", f"FOO+= {command}",
                f"_FOO2+='hello world' {command}", f'FOO+="hello world" {command}',
                f"FOO=base FOO+=x BAR+=y {command}",
                f"FOO+=x 2>/dev/null {command}", f"FOO+=x command -- {command}",
                f"if FOO+=x {command}; then true; fi",
                f"bash -c 'FOO+=x {command}'",
                # env accepts FOO+ as a variable name; it does not append to FOO.
                f"env FOO+=x {command}", f"env 'FOO+=x' {command}",
                f"env -S 'FOO+=x {command}'",
            ):
                with self.subTest(text=text):
                    self.assertEqual(self.located(text), [(1, missing)])
                    self.assertEqual(dead_commands(text, {"lab/example/deleted.py"}), [])
            text = f"```bash\nFOO+=x \\\n  {command}\n```"
            self.assertEqual(self.located(text), [(2, missing)])

    def test_append_assignment_lookalikes_do_not_start_commands(self) -> None:
        command = "python3 -m lab.example.deleted"
        for prefix in (
            "'FOO+=x'", '"FOO+=x"', 'F"OO"+=x', "FOO'+'=x", "FOO+'='x",
            "1FOO+=x", "FOO++=x", "FOO+ =x", "echo FOO+=x",
            "command FOO+=x", "command -- FOO+=x", "nohup FOO+=x",
            "exec FOO+=x", "docker exec container FOO+=x",
            "env command FOO+=x", "FOO+=x command BAR+=y",
        ):
            with self.subTest(prefix=prefix):
                self.assertEqual(self.commands(f"{prefix} {command}"), [])
                self.assertEqual(
                    self.commands(f"{prefix} {command}; python3 lab/prism/storm.py"),
                    ["lab/prism/storm.py"],
                )

    def test_compound_shell_commands_are_scanned(self) -> None:
        for command, missing in (
            ("python3 -m lab.example.deleted", "lab/example/deleted.py or lab/example/deleted/__main__.py"),
            ("python3 lab/example/deleted.py", "lab/example/deleted.py"),
        ):
            for text in (
                f"{{ {command}; }}",
                f"{{ {{ env X=1 {command}; }}; }}",
                f"while {command}; do break; done",
                f"until {command}; do break; done",
                f"while true; do {{ {command}; }}; break; done",
                f"for item in one two; do {command}; done",
                f"if {{ {command}; }}; then true; fi",
                f"sh -c '{{ {command}; }}'",
            ):
                with self.subTest(text=text):
                    self.assertEqual(self.located(text), [(1, missing)])
            text = f"```sh\nwhile\n  {{ {command}; }}\ndo\n  break\ndone\n```"
            self.assertEqual(self.located(text), [(3, missing)])
            self.assertEqual(list(dead_commands(text, {"lab/example/deleted.py"})), [])

    def test_compound_shell_prefixes_in_data_are_not_commands(self) -> None:
        command = "python3 -m lab.example.deleted"
        for keyword in ("{", "while", "until"):
            for prefix in (
                f"'{keyword}'", f'"{keyword}"', f"echo {keyword}",
                f"env {keyword}", f"command -- {keyword}", f"X=1 {keyword}",
            ):
                with self.subTest(prefix=prefix):
                    self.assertEqual(self.commands(f"{prefix} {command}"), [])
        for text in (
            f"for item in while {command}; do echo done; done",
            f"{{ echo {command}; }}",
            f"while echo {command}; do break; done",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])

    def test_time_shell_prefixes_run_the_following_command(self) -> None:
        for command, missing in (
            ("python3 -m lab.example.deleted", "lab/example/deleted.py or lab/example/deleted/__main__.py"),
            ("python3 lab/example/deleted.py", "lab/example/deleted.py"),
        ):
            for text in (
                f"time {command}", f"time -p {command}", f"time ! {command}",
                f"if time -p {command}; then true; fi",
                f"while time {command}; do break; done",
                f"{{ time -p {command}; }}", f"time -p {{ {command}; }}",
                f"time X=1 {command}", f"time -p env X=1 {command}",
                f"time command -- {command}", f"time sudo -u prism {command}",
                f"env X=1 bash -c 'time -p {command}'",
            ):
                with self.subTest(text=text):
                    self.assertEqual(self.located(text), [(1, missing)])
                    self.assertEqual(dead_commands(text, {"lab/example/deleted.py"}), [])
            text = f"```bash\ntime -p \\\n  {command}\n```"
            self.assertEqual(self.located(text), [(2, missing)])

    def test_time_words_and_options_in_data_are_not_shell_prefixes(self) -> None:
        command = "python3 -m lab.example.deleted"
        for prefix in (
            "'time'", '"time"', "ti'me'", "$'time'", "'time' -p",
            "echo time", "printf '%s\\n' time -p", "env time", "X=1 time",
            "command -- time", "time echo", "time -p printf '%s\\n'",
            "time '-p'", 'time "-p"', 'time -"p"', "time -p -p", "time --help",
        ):
            with self.subTest(prefix=prefix):
                text = f"{prefix} {command}"
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.commands(text + "; python3 lab/prism/storm.py"), ["lab/prism/storm.py"])

    def test_coproc_prefixes_run_the_following_command(self) -> None:
        for command, missing in (
            ("python3 -m lab.example.deleted", "lab/example/deleted.py or lab/example/deleted/__main__.py"),
            ("python3 lab/example/deleted.py", "lab/example/deleted.py"),
        ):
            for text in (
                f"coproc {command}",
                f"coproc env X=1 {command}",
                f"coproc {{ {command}; }}",
                f"coproc worker {{ {command}; }}",
                f"coproc python3 {{ {command}; }}",
                f"coproc worker ( {command} )",
                f"coproc worker if {command}; then true; fi",
                f"coproc worker while {command}; do break; done",
                f"coproc worker until {command}; do break; done",
                f"bash -c 'coproc worker {{ {command}; }}'",
            ):
                with self.subTest(text=text):
                    self.assertEqual(self.located(text), [(1, missing)])
                    self.assertEqual(dead_commands(text, {"lab/example/deleted.py"}), [])
            text = f"```bash\ncoproc worker {{\n  {command};\n}}\n```"
            self.assertEqual(self.located(text), [(3, missing)])
            text = f"```bash\ncoproc \\\n  {command}\n```"
            self.assertEqual(self.located(text), [(2, missing)])

    def test_coproc_words_and_names_in_data_are_not_commands(self) -> None:
        command = "python3 -m lab.example.deleted"
        for prefix in (
            "'coproc'", '"coproc"', "co'proc'", "$'coproc'",
            "echo coproc", "printf '%s\\n' coproc", "env coproc",
            "command -- coproc", "X=1 coproc",
            "coproc printf", "coproc echo", "coproc worker",
            "coproc worker '{'", 'coproc worker "{"',
            "coproc worker time",
        ):
            with self.subTest(prefix=prefix):
                text = f"{prefix} {command}"
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.commands(text + "; python3 lab/prism/storm.py"), ["lab/prism/storm.py"])
        self.assertEqual(self.commands(f"coproc python3 {{ printf '%s\\n' {command}; }}"), [])

    def test_inline_code_is_neither_a_module_nor_a_script_command(self) -> None:
        # `-c cmd` runs its argument and ends the option list; the prose contract
        # still sees the module reference inside the code string.
        text = "python3 -c 'import lab.prism.x'"
        self.assertEqual(self.commands(text), [])
        self.assertEqual(self.references(text), ["lab.prism.x"])
        self.assertEqual(self.commands("python3 -OO -c 'import lab.prism.x'"), [])
        self.assertEqual(self.commands("python3 -c -m lab.prism.x"), [])
        self.assertEqual(self.commands("python3 -c lab/prism/x.py"), [])

    # Verified on CPython 3.14 with a `lab/gone.py` and a `gone.py` that raise
    # on import: each of these printed the help text or the version and exited
    # 0 without running either, while `python3 -m lab.gone` and `python3
    # gone.py` raised. `-h`, its alias `-?` and `-V` end the run before the
    # target is read wherever they sit among the options, so a command
    # carrying one runs nothing; the prose contract still sees the reference.
    HELP_AND_VERSION_OPTIONS = (
        "-h",
        "-?",
        "-V",
        "-VV",
        "-OO -h",
        "-Oh",
        "-hOO",
        "-V -X dev",
        "-VX dev",
        "-hW error",
    )

    def test_help_and_version_options_are_not_a_command(self) -> None:
        for options in self.HELP_AND_VERSION_OPTIONS:
            with self.subTest(options=options):
                text = f"python3 {options} -m lab.prism.x"
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.references(text), ["lab.prism.x"])
                self.assertEqual(self.commands(f"python3 {options} lab/prism/x.py"), [])
                self.assertEqual(self.commands(f"python3.12 {options} -m 'lab.prism.'x"), [])
        self.assertEqual(self.commands("python3 -V -- lab/prism/x.py"), [])
        # A cluster `-h` or `-V` closes with `m` prints and exits the same way.
        for option in ("-hm", "-Vm", "-Ohm", "-hIm", "-VVm"):
            with self.subTest(option=option):
                self.assertEqual(self.commands(f"python3 {option} lab.prism.x"), [])
                self.assertEqual(self.commands(f"python3 {option}lab.prism.x"), [])
                self.assertEqual(self.references(f"python3 {option} lab.prism.x"), ["lab.prism.x"])

    # Each ran `json.tool` on bash 3.2 and CPython 3.14 exactly as its bare
    # spelling: `python3 "-O" -m json.tool`, `python3 '-OO' -m json.tool`,
    # `python3 $'-OO' -m json.tool`, `python3 "-"O -m json.tool`, `python3
    # "-X" dev -m json.tool`, `python3 '-X'dev -m json.tool`, `python3
    # "-X"'dev' -m json.tool`, `python3 "-W" error -m json.tool`, `python3
    # '-uW'error -m json.tool` and `python3 "--check-hash-based-pycs" always
    # -m json.tool`. The shell strips the quotes of an option word, attached
    # or not, before CPython sees it, so a quoted option is as runnable as a
    # bare one and, ahead of a missing target, as dead.
    QUOTED_OPTION_WORDS = (
        '"-O"',
        "'-OO'",
        "$'-OO'",
        '"-"O',
        '"-X" dev',
        "'-X'dev",
        "\"-X\"'dev'",
        '"-W" error',
        "'-uW'error",
        '"--check-hash-based-pycs" always',
    )
    # `python3 '-m' json.tool`, `python3 "-m" json.tool`, `python3
    # "-m"json.tool` and `python3 -"m" json.tool` each ran `json.tool` the
    # same way, and `python3 '--' lab/gone.py` ran the script.
    QUOTED_MODULE_OPTIONS = ("'-m' {}", '"-m" {}', '"-m"{}', '-"m" {}')

    def test_quoted_option_words_with_missing_targets_are_caught(self) -> None:
        for options in self.QUOTED_OPTION_WORDS:
            with self.subTest(options=options):
                self.assertEqual(
                    self.commands(f"python3 {options} -m lab.prism.process_telemetry rss-bound"),
                    [self.TELEMETRY],
                )
                self.assertEqual(
                    self.commands(f"python {options} lab/prism/storm.py --decide"),
                    ["lab/prism/storm.py"],
                )
        for option in self.QUOTED_MODULE_OPTIONS:
            with self.subTest(option=option):
                module = option.format("lab.prism.process_telemetry")
                self.assertEqual(self.commands(f"python3 {module} rss-bound"), [self.TELEMETRY])
                self.assertEqual(self.commands(f"python3.12 -OO {module}"), [self.TELEMETRY])
        self.assertEqual(self.commands("python3 '--' lab/prism/storm.py --decide"), ["lab/prism/storm.py"])
        self.assertEqual(self.commands("python3 \"-OO\" '--' './lab/prism/storm.py'"), ["lab/prism/storm.py"])
        # The reported command runs from the interpreter word to the target
        # word, quotes and all, so the reader finds it in the document.
        self.assertEqual(
            dead_commands('python3 "-O" -m lab.prism.process_telemetry rss-bound', self.TRACKED),
            [(1, 'python3 "-O" -m lab.prism.process_telemetry', self.TELEMETRY)],
        )
        self.assertEqual(
            dead_commands('python3 "-m"lab.prism.process_telemetry', self.TRACKED),
            [(1, 'python3 "-m"lab.prism.process_telemetry', self.TELEMETRY)],
        )

    def test_quoted_option_words_with_existing_targets_pass(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py", "lab/pkg/__init__.py", "lab/pkg/__main__.py"}
        for options in self.QUOTED_OPTION_WORDS:
            with self.subTest(options=options):
                text = (
                    f"python3 {options} -m lab.prism.tool\npython3.12 {options} -m lab.pkg\n"
                    f"python {options} lab/prism/tool.py"
                )
                self.assertEqual(dead_commands(text, tracked), [])
        for option in self.QUOTED_MODULE_OPTIONS:
            with self.subTest(option=option):
                text = f"python3 {option.format('lab.prism.tool')}\npython3.12 {option.format('lab.pkg')}"
                self.assertEqual(dead_commands(text, tracked), [])
        text = "python3 '--' lab/prism/tool.py\npython3 \"-O\" \"--\" ./lab/prism/tool.py"
        self.assertEqual(dead_commands(text, tracked), [])

    def test_wrapped_quoted_option_words_are_caught_at_the_right_line(self) -> None:
        text = "```bash\ncd repo\npython3 \\\n  \"-O\" \\\n  '-m' lab.prism.process_telemetry \\\n  rss-bound\n```"
        self.assertEqual(self.located(text), [(3, self.TELEMETRY)])
        text = "python3.12 \"-X\" \\\n  dev \\\n  '--' \\\n  lab/prism/storm.py \\\n  --decide"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    # Verified on CPython 3.14 with a `lab/gone.py` that raises
    # `SystemExit("RAN")`: none of these printed RAN. `'-h' -m lab.gone` and
    # `"-hm" lab.gone` print the help text and `"-V" lab/gone.py` the
    # version, and exit 0; `"-Wm" lab.gone` and `"-Xm" lab.gone` hand `m` to
    # `-W`/`-X` and fail to open a script called `lab.gone`;
    # `"--check-hash-based-pycs" sometimes -m lab.gone` is rejected ("must be
    # one of 'default', 'always', or 'never'") before anything runs; `'--' -m
    # lab.gone` fails to open a file called `-m`; and `"-c" 'import lab.gone'`
    # runs its argument as inline code exactly as bare `-c` does, which the
    # command contract leaves alone. Quoting an option the guard leaves out
    # therefore changes nothing, and the prose contract still sees the
    # reference.
    def test_quoted_non_command_options_are_not_a_command(self) -> None:
        for text, reference in (
            ("python3 \"-c\" 'import lab.prism.x'", "lab.prism.x"),
            ("python3 '-h' -m lab.prism.x", "lab.prism.x"),
            ('python3 "-V" lab/prism/x.py', "lab/prism/x.py"),
            ('python3 "-hm" lab.prism.x', "lab.prism.x"),
            ('python3 "-Wm" lab.prism.x', "lab.prism.x"),
            ('python3 "-Xm" lab.prism.x', "lab.prism.x"),
            ('python3 "--check-hash-based-pycs" sometimes -m lab.prism.x', "lab.prism.x"),
            ("python3 '--' -m lab.prism.x", "lab.prism.x"),
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.references(text), [reference])

    def test_mismatched_option_word_quotes_are_not_a_command(self) -> None:
        # bash rejects `python3 "-O' -m lab.gone` and `python3 -X "dev' -m
        # lab.gone` with "unexpected EOF while looking for matching `"'"
        # before Python starts, so the line runs nothing; the prose contract
        # still sees the reference.
        for text, reference in (
            ("python3 \"-O' -m lab.prism.x", "lab.prism.x"),
            ("python3 -X \"dev' -m lab.prism.x", "lab.prism.x"),
            ("python3 '-OO\" lab/prism/x.py", "lab/prism/x.py"),
            ("python3 \"-m' lab.prism.x", "lab.prism.x"),
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.references(text), [reference])

    # The mode of `--check-hash-based-pycs` is a shell word like any other:
    # bash 3.2 hands `$'always'`, `alwa"ys"`, `de'fault'`, `"al"ways` and
    # `$'never'` to CPython 3.14 as the bare mode, and `python3
    # --check-hash-based-pycs $'always' -m json.tool` ran `json.tool`. The
    # word is therefore unquoted before it is checked against the three
    # modes, so a mode spelled in pieces still runs, and reports, a missing
    # target. A word that is no mode once unquoted (`"sometimes"`,
    # `$'some'times`) is rejected by CPython before anything runs, and a
    # mismatched quote is rejected by bash, so neither is a command.
    HASH_MODE_WORDS = ("$'always'", 'alwa"ys"', "'never'", '$"default"', "de'fault'", '"al"ways')

    def test_quoted_hash_mode_arguments_are_read_as_shell_words(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py"}
        for mode in self.HASH_MODE_WORDS:
            with self.subTest(mode=mode):
                text = f"python3 --check-hash-based-pycs {mode} -m lab.prism.process_telemetry rss-bound"
                self.assertEqual(self.commands(text), [self.TELEMETRY])
                self.assertEqual(
                    self.commands(f"python --check-hash-based-pycs {mode} lab/prism/storm.py"), ["lab/prism/storm.py"]
                )
                self.assertEqual(dead_commands(f"python3 --check-hash-based-pycs {mode} -m lab.prism.tool", tracked), [])
        for mode in ('"sometimes"', "$'some'times", "'always\"", "$'alwa'ys\""):
            with self.subTest(mode=mode):
                text = f"python3 --check-hash-based-pycs {mode} -m lab.prism.x"
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.references(text), ["lab.prism.x"])

    # bash 5.2 ran nothing from a comment line or from the tail of a line
    # after its comment opens (see ``COMMENT_BOUNDARY``); the prose contract
    # still counts every reference a comment names.
    def test_comment_lines_are_not_a_command(self) -> None:
        for text in (
            "# python3 -m lab.prism.process_telemetry rss-bound",
            "#python3 lab/prism/storm.py",
            "  # python3 -m lab.prism.process_telemetry",
            "```bash\n# python3 -m lab.prism.process_telemetry\n```",
            "`# python3 lab/prism/storm.py`",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
                self.assertEqual(len(self.references(text)), 1)

    def test_trailing_comments_are_not_a_command(self) -> None:
        tracked = self.TRACKED | {"lab/prism/tool.py"}
        for text in (
            "python3 -m lab.prism.tool # python3 -m lab.prism.process_telemetry",
            "echo done # python3 lab/prism/storm.py",
            "true;# python3 -m lab.prism.process_telemetry",
            "`echo done # python3 lab/prism/storm.py`",
            "python3 -m # lab.prism.process_telemetry",
            "python3 -X # dev -m lab.prism.process_telemetry",
        ):
            with self.subTest(text=text):
                self.assertEqual(dead_commands(text, tracked), [])
        self.assertEqual(
            dangling_references("python3 -m lab.prism.tool # was lab/prism/storm.py", tracked),
            [(1, "lab/prism/storm.py")],
        )

    def test_commands_before_comments_are_still_caught(self) -> None:
        self.assertEqual(
            dead_commands("python3 -m lab.prism.process_telemetry # retired in #244", self.TRACKED),
            [(1, "python3 -m lab.prism.process_telemetry", self.TELEMETRY)],
        )
        # A comment ends at the backtick that closes its inline code, and a
        # `#` inside a nested shell string is the inner shell's business.
        for text in (
            "python3 lab/prism/storm.py --decide  # see the runbook",
            "sh -c 'python3 lab/prism/storm.py' # nested",
            'sh -c "python3 lab/prism/storm.py # inner"',
            "## Run `python3 lab/prism/storm.py`",
            "Issue #303: `python3 lab/prism/storm.py`",
            "See [the runbook](#run) and `python3 lab/prism/storm.py`",
            "`true # comment` then `python3 lab/prism/storm.py`",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), ["lab/prism/storm.py"])

    def test_quoted_escaped_and_in_word_hashes_open_no_comment(self) -> None:
        # Each prefix printed its `#` or a count on bash 5.2 and then ran the
        # command after `;`, so the missing target there is still dead.
        for prefix in (
            "echo '#'",
            'echo "# x"',
            "echo \\#",
            "echo a#b",
            "echo $#",
            "echo ${#a[@]}",
            "echo a\\ #b",
            "echo $'\\'#'",
            'echo "\\"#"',
            "curl https://example.com/runbook#step",
        ):
            for command, missing in (
                ("python3 -m lab.prism.process_telemetry", self.TELEMETRY),
                ("python3 lab/prism/storm.py", "lab/prism/storm.py"),
            ):
                text = f"{prefix}; {command}"
                with self.subTest(text=text):
                    self.assertEqual(dead_commands(text, self.TRACKED), [(1, command, missing)])

    def test_backslash_ending_a_comment_continues_nothing(self) -> None:
        # bash 5.2 ran the line after `# c \` and after `echo a # c \`.
        text = "```bash\n# retired: python3 -m lab.prism.process_telemetry \\\npython3 lab/prism/storm.py\n```"
        self.assertEqual(
            dead_commands(text, self.TRACKED), [(3, "python3 lab/prism/storm.py", "lab/prism/storm.py")]
        )
        self.assertEqual(
            dangling_references(text, self.TRACKED),
            [(2, "lab.prism.process_telemetry"), (3, "lab/prism/storm.py")],
        )
        text = "python3 -m lab.prism.tool # wrapped \\\n  python3 -m lab.prism.process_telemetry"
        self.assertEqual(
            dead_commands(text, self.TRACKED | {"lab/prism/tool.py"}),
            [(2, "python3 -m lab.prism.process_telemetry", self.TELEMETRY)],
        )
        # A continuation into a comment joins it and the comment ends the
        # command there; the next line runs on its own as `-m …`.
        self.assertEqual(self.commands("python3 \\\n  # retired\n  -m lab.prism.process_telemetry"), [])
        text = "python3 \\\n  -m lab.prism.process_telemetry \\\n  rss-bound # python3 lab/prism/storm.py"
        self.assertEqual(self.located(text), [(1, self.TELEMETRY)])
        # An escaped, in-word or quoted `#` opens no comment, so the backslash
        # after it still continues the line, and a quote carried across a
        # continuation keeps its `#` literal: bash printed `a # b` for
        # `echo "a \` followed by `# b"` and ran the command after `;`.
        for text in (
            "echo \\# \\\n  && python3 lab/prism/storm.py",
            "echo \\\\#x \\\n  && python3 lab/prism/storm.py",
            "echo '#' \\\n  && python3 lab/prism/storm.py",
            'echo "a \\\n# b"; python3 lab/prism/storm.py',
        ):
            with self.subTest(text=text):
                self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])

    def test_fenced_comments_hide_their_backticks(self) -> None:
        # bash 5.2 ran no `echo NO` from `# was `echo NO`` or `echo ok # was
        # `echo NO``: in a fence the comment runs to the end of the line.
        tracked = self.TRACKED | {"lab/prism/tool.py"}
        for text in (
            "```sh\n# historical `python3 -m lab.prism.process_telemetry`\n```",
            "```bash\npython3 -m lab.prism.tool # was `python3 lab/prism/storm.py`\n```",
            "- step\n\n  ~~~bash\n  # `python3 lab/prism/storm.py` and `python3 -m lab.prism.process_telemetry`\n  ~~~",
            "````sh\n```\n# `python3 lab/prism/storm.py`\n````",
        ):
            with self.subTest(text=text):
                self.assertEqual(dead_commands(text, tracked), [])
        self.assertEqual(
            self.references("```sh\n# historical `python3 -m lab.prism.process_telemetry`\n```"),
            ["lab.prism.process_telemetry"],
        )
        # A comment inside a command substitution ends at its closing backtick:
        # bash printed `RUN` for `` out=`true # echo NO`; echo RUN ``.
        text = "```sh\nout=`true # python3 lab/prism/storm.py`; python3 -m lab.prism.process_telemetry\n```"
        self.assertEqual(self.located(text), [(2, self.TELEMETRY)])

    def test_prose_after_a_fence_ends_comments_at_inline_code(self) -> None:
        text = (
            "```sh\n# `python3 -m lab.prism.process_telemetry`\n```\n"
            "## Run `python3 lab/prism/storm.py`\n"
            "```python3 lab/prism/storm.py```\n"
            "# `python3 -m lab.prism.process_telemetry`"
        )
        # The fenced comment on line 2 hides its inline code; the same text as
        # a prose heading on line 6 does not.
        self.assertEqual(
            self.located(text), [(4, "lab/prism/storm.py"), (5, "lab/prism/storm.py"), (6, self.TELEMETRY)]
        )
        # No quote carries across inline code, so prose apostrophes neither
        # hide a command nor keep a comment open.
        self.assertEqual(self.commands("Don't run `true # python3 lab/prism/storm.py`"), [])
        self.assertEqual(self.commands("It's `python3 lab/prism/storm.py` # done"), ["lab/prism/storm.py"])

    def test_ordinary_arguments_are_not_executable_commands(self) -> None:
        for text in (
            "printf '%s\\n' 'python3 -m lab.example.deleted'",
            'echo "python3 -m lab.example.deleted"',
            "printf '%s\\n' python3 -m lab.example.deleted",
            "echo 'python3' -m lab.example.deleted",
            "printf '%s\\n' sh -c 'python3 -m lab.example.deleted'",
            "python3 -c 'python3 -m lab.example.deleted'",
            "cat <<< 'python3 -m lab.example.deleted'",
            "echo done > python3 -m lab.example.deleted",
            "'python3 -m lab.example.deleted'",
            '"python3 -m lab.example.deleted"',
            "command -v python3 -m lab.example.deleted",
            "command -V python3 -m lab.example.deleted",
            "'X=1' python3 -m lab.example.deleted",
            "'if' python3 -m lab.example.deleted",
            "env --help python3 -m lab.example.deleted",
            "sudo -l python3 -m lab.example.deleted",
            "nohup --version python3 -m lab.example.deleted",
            "docker exec --help container python3 -m lab.example.deleted",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.commands(text + "; python3 lab/prism/storm.py"), ["lab/prism/storm.py"])

    def test_env_split_string_runs_literal_argv(self) -> None:
        for options in (
            "-S 'python3 -m lab.example.deleted'",
            "--split-string 'python3 -m lab.example.deleted'",
            "--split-string='python3 -m lab.example.deleted'",
            "-S'python3 -m lab.example.deleted'",
            "-i -S 'python3 -m' lab.example.deleted",
            "-S 'python3' -m lab.example.deleted",
            "-S 'MODE=test python3 -m lab.example.deleted'",
            "-S '-u MODE python3 -m lab.example.deleted'",
            "-S 'sudo -u prism python3 -m lab.example.deleted'",
            "-S 'nohup python3 -m lab.example.deleted'",
            "-S '-S \"python3 -m\" lab.example.deleted'",
            "-S 'env -S \"python3 -m\"' lab.example.deleted",
            "-S 'python3 -m # ignored' lab.example.deleted",
            "-vS 'python3 -m lab.example.deleted'",
            "-iS 'python3 -m lab.example.deleted'",
            "-ivS'python3 -m lab.example.deleted'",
            "-viS 'python3 -m' lab.example.deleted",
            "'-vS' 'python3 -m lab.example.deleted'",
        ):
            with self.subTest(options=options):
                text = f"env {options}"
                self.assertEqual(self.located(text), [(1, "lab/example/deleted.py or lab/example/deleted/__main__.py")])
                self.assertEqual(dead_commands(text, {"lab/example/deleted.py"}), [])
        for text in (
            "env -S 'python3 lab/example/deleted.py'",
            "sudo -u prism env -S 'python3' lab/example/deleted.py",
            "docker exec container env -S 'python3 lab/example/deleted.py'",
            "env -S 'sh -c \"python3 lab/example/deleted.py\"'",
            "env -S 'sh -c' 'python3 lab/example/deleted.py'",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), ["lab/example/deleted.py"])

    def test_env_split_string_words_are_argv_not_shell_code(self) -> None:
        for text in (
            "env -S 'echo python3 -m lab.example.deleted'",
            "env -S 'echo; python3 -m lab.example.deleted'",
            "env -S 'echo | python3 -m lab.example.deleted'",
            "env -S 'echo\npython3 -m lab.example.deleted'",
            "env -S 'true # python3 -m lab.example.deleted'",
            "env -S '\"python3 -m lab.example.deleted\"'",
            "env -S 'if python3 -m lab.example.deleted'",
            "env -S 'sh -nc \"python3 -m lab.example.deleted\"'",
            "env -S 'sudo -v python3 -m lab.example.deleted'",
            "env -S 'python3 -c \"python3 -m lab.example.deleted\"'",
            "printf '%s' env -S 'python3 -m lab.example.deleted'",
            "env -S '${INTERPRETER} -m lab.example.deleted'",
            "env -S 'python3\\_ -m lab.example.deleted'",
            "env -S 'python3 -m \"lab.example.deleted'",
            "env -S",
            "env -vS",
            "env -uS 'python3 -m lab.example.deleted'",
            "env -u -vS 'python3 -m lab.example.deleted'",
            "env -CS 'python3 -m lab.example.deleted'",
            "env -C -iS 'python3 -m lab.example.deleted'",
            "env -aS 'python3 -m lab.example.deleted'",
            "env -xS 'python3 -m lab.example.deleted'",
            "env --vS 'python3 -m lab.example.deleted'",
            "env -vS '${INTERPRETER} -m lab.example.deleted'",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.commands(text + "; python3 lab/prism/storm.py"), ["lab/prism/storm.py"])
        self.assertEqual(self.commands("env -S 'python3 lab/prism/deleted;name.py'"), ["lab/prism/deleted;name.py"])
        self.assertEqual(self.commands("env -S 'python3 lab/prism/deleted#name.py'"), ["lab/prism/deleted#name.py"])
        self.assertEqual(self.commands("env -S 'python3 lab/prism/deleted|name.py'"), ["lab/prism/deleted|name.py"])
        self.assertEqual(self.commands("env -S 'python3 \"lab/prism/deleted name.py\"'"), ["lab/prism/deleted name.py"])

    def test_env_split_string_preserves_empty_words_comments_and_locations(self) -> None:
        self.assertEqual(self.commands("env -S '' python3 lab/prism/storm.py"), ["lab/prism/storm.py"])
        self.assertEqual(self.commands("env -S '# ignored' python3 lab/prism/storm.py"), ["lab/prism/storm.py"])
        self.assertEqual(self.commands("env -S '\"\" python3 lab/prism/storm.py'"), [])
        self.assertEqual(env_split_words("one#two '#three' \"#four\" # ignored"), [("one#two", 0), ("#three", 0), ("#four", 0)])
        text = "```sh\nenv -S '\npython3 lab/prism/storm.py'\n```"
        self.assertEqual(self.located(text), [(3, "lab/prism/storm.py")])
        text = "```sh\nenv -S 'sh -c \"true\npython3 lab/prism/storm.py\"'\n```"
        self.assertEqual(self.located(text), [(3, "lab/prism/storm.py")])
        for options in ("-vS '", "-iS'"):
            with self.subTest(options=options):
                text = f"```sh\nenv {options}\npython3 lab/prism/storm.py'\n```"
                self.assertEqual(self.located(text), [(3, "lab/prism/storm.py")])
        text = "```sh\nenv -S 'sh -c'\n```"
        self.assertEqual(self.commands(text), [])

    def test_only_env_and_sudo_take_assignment_arguments(self) -> None:
        # nohup 9.4 answers `nohup FOO=x echo hi` with "failed to run command
        # 'FOO=x'", bash 5.2.21 `command FOO=x echo hi` with "FOO=x: command
        # not found" and `exec FOO=x echo hi` with "exec: FOO=x: not found",
        # each exit 127, and docker exec runs the word after the container:
        # the assignment is the program, and nothing after it runs. sudo
        # 1.9.15p5 exported `FOO=x` and `FOO+=x` before a command, but ran
        # `FOO=x` as the command after `--`.
        command = "python3 -m lab.prism.deleted"
        for launcher in (
            "nohup", "command", "command -p", "exec", "exec -a name", "docker exec c",
            "podman exec -it c", "env nohup", "sudo nohup", "sudo -u prism command",
            "nohup sudo --", "sudo --", "sudo -n --", "env sudo --",
        ):
            for assignments in ("FOO=x", "FOO=x BAR=y", "FOO+=x", "'FOO=x'", 'FOO="x y"'):
                with self.subTest(launcher=launcher, assignments=assignments):
                    text = f"{launcher} {assignments} {command}"
                    self.assertEqual(self.commands(text), [])
                    self.assertEqual(self.references(text), ["lab.prism.deleted"])
                    self.assertEqual(self.commands(text + "; python3 lab/prism/storm.py"), ["lab/prism/storm.py"])
        for launcher in (
            "env", "env -i", "env --", "env -", "sudo", "sudo -u prism", "sudo -n", "nohup env",
            "sudo env", "env sudo", "nohup sudo -u prism", "command env", "exec env",
            "docker exec c env", "sudo FOO=x env", "env FOO=x sudo",
        ):
            for assignments in ("FOO=x", "FOO=x BAR=y", "FOO+=x", "'FOO=x'", '"FOO=x"', 'FOO="x y"'):
                with self.subTest(launcher=launcher, assignments=assignments):
                    text = f"{launcher} {assignments} python3 lab/prism/storm.py"
                    self.assertEqual(self.commands(text), ["lab/prism/storm.py"])
        # A leading assignment is shell syntax and precedes any launcher.
        for text in (
            "FOO=x nohup python3 lab/prism/storm.py", "FOO=x BAR+=y command python3 lab/prism/storm.py",
            "FOO=x exec python3 lab/prism/storm.py", "FOO=x docker exec c python3 lab/prism/storm.py",
            "FOO=x sudo BAR=y python3 lab/prism/storm.py",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), ["lab/prism/storm.py"])

    def test_env_null_mode_does_not_run_commands(self) -> None:
        # GNU coreutils 9.4 refuses a command after `-0`/`--null` ("cannot
        # specify --null (-0) with command", exit 125), whether the `0` stands
        # alone, follows other argument-free flags in a cluster, or arrives
        # through `-S`, so nothing after it runs.
        for options in (
            "-0", "--null", "'-0'", "-i0", "-0i", "-v0", "-iv0", "-0u FOO", "-0 -u FOO",
            "-i -0", "-0 -i", "-0S", "-0 -S", "-0 --null",
        ):
            for argument in (
                "python3 -m lab.prism.deleted", "python3 lab/prism/deleted.py",
                "sh -c 'python3 -m lab.prism.deleted'",
            ):
                for delimiter in ("", "-- "):
                    with self.subTest(options=options, argument=argument, delimiter=delimiter):
                        text = f"env {options} {delimiter}{argument}"
                        self.assertEqual(self.commands(text), [])
                        self.assertEqual(len(self.references(text)), 1)
                        self.assertEqual(
                            self.commands(text + "; python3 lab/prism/storm.py"),
                            ["lab/prism/storm.py"],
                        )
        for text in (
            "env -S '-0 python3 -m lab.prism.deleted'",
            "env -iS '-0 python3 -m lab.prism.deleted'",
            "env -0S 'python3 -m lab.prism.deleted'",
            "sudo env -0 python3 -m lab.prism.deleted",
            "nohup env --null python3 lab/prism/deleted.py",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.commands(text + "; python3 lab/prism/storm.py"), ["lab/prism/storm.py"])
        text = "```sh\nenv -0 \\\n  python3 -m lab.prism.deleted\npython3 lab/prism/storm.py\n```"
        self.assertEqual(self.located(text), [(4, "lab/prism/storm.py")])
        # `-u0` and `-u 0` unset the variable `0` and run the command.
        for options in ("-u0", "-u 0", "-iu0", "-i -u 0", "--unset=0", "--unset 0", "-S '-u 0'"):
            with self.subTest(options=options):
                self.assertEqual(self.commands(f"env {options} python3 lab/prism/storm.py"), ["lab/prism/storm.py"])

    def test_sudo_chdir_arguments_before_commands_are_consumed(self) -> None:
        for options in (
            "-D /tmp", "--chdir /tmp", "-D/tmp", "--chdir=/tmp",
            "'-D' '/tmp/work dir'", "-u prism -D /tmp --", "-D /tmp -n",
        ):
            for command, missing in (
                ("python3 -m lab.example.deleted", "lab/example/deleted.py or lab/example/deleted/__main__.py"),
                ("python3 lab/example/deleted.py", "lab/example/deleted.py"),
            ):
                with self.subTest(options=options, command=command):
                    text = f"sudo {options} {command}"
                    self.assertEqual(self.located(text), [(1, missing)])
                    self.assertEqual(dead_commands(text, {"lab/example/deleted.py"}), [])
        text = "```sh\nsudo -D \\\n  /tmp \\\n  python3 lab/example/deleted.py\n```"
        self.assertEqual(self.located(text), [(2, "lab/example/deleted.py")])
        self.assertEqual(
            self.commands("env sudo --chdir /tmp sh -c 'python3 lab/prism/storm.py'"),
            ["lab/prism/storm.py"],
        )

    def test_sudo_chdir_arguments_are_not_executable_words(self) -> None:
        for option in ("-D", "--chdir"):
            for arguments in (
                "python3 -m lab.example.deleted", "'python3 -m lab.example.deleted' true",
                "",
            ):
                with self.subTest(option=option, arguments=arguments):
                    self.assertEqual(self.commands(f"sudo {option} {arguments}"), [])

    def test_sudo_chroot_role_and_type_arguments_before_commands_are_consumed(self) -> None:
        for options in (
            "-R /", "--chroot /", "-R/", "--chroot=/", "'-R' '/srv/chroot dir'",
            "-r admin", "--role admin", "-radmin", "--role=admin", "\"-r\" 'admin'",
            "-t unconfined_t", "--type unconfined_t", "-tunconfined_t", "--type=unconfined_t",
            "'--type' unconfined_t", "-u prism -R / -r admin -t unconfined_t --", "-R / -n",
            "-D /tmp -R /", "-r python3 -t python3", "-R -e", "-r -v", "-t -K",
        ):
            for command, missing in (
                ("python3 -m lab.example.deleted", "lab/example/deleted.py or lab/example/deleted/__main__.py"),
                ("python3 lab/example/deleted.py", "lab/example/deleted.py"),
            ):
                with self.subTest(options=options, command=command):
                    text = f"sudo {options} {command}"
                    self.assertEqual(self.located(text), [(1, missing)])
                    self.assertEqual(dead_commands(text, {"lab/example/deleted.py"}), [])
        text = "```sh\nsudo -r \\\n  python3 \\\n  python3 lab/example/deleted.py\n```"
        self.assertEqual(self.located(text), [(2, "lab/example/deleted.py")])
        text = "```sh\nenv -S 'sudo --type\npython3\npython3 lab/prism/storm.py'\n```"
        self.assertEqual(self.located(text), [(4, "lab/prism/storm.py")])
        for text in (
            "env sudo --chroot / sh -c 'python3 lab/prism/storm.py'",
            "env -S 'sudo -R / python3 lab/prism/storm.py'",
            "sudo -r admin env -u MODE python3 lab/prism/storm.py",
            "nohup sudo -t unconfined_t command -- python3 lab/prism/storm.py",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), ["lab/prism/storm.py"])

    def test_sudo_chroot_role_and_type_arguments_are_not_executable_words(self) -> None:
        for option in ("-R", "--chroot", "-r", "--role", "-t", "--type"):
            for arguments in (
                "python3 -m lab.example.deleted", "python3 lab/example/deleted.py",
                "'python3 -m lab.example.deleted' true", "",
            ):
                with self.subTest(option=option, arguments=arguments):
                    self.assertEqual(self.commands(f"sudo {option} {arguments}"), [])
        for options in ("-R / -e", "-r admin --validate", "-t -e -K", "--chroot=/ -nv"):
            with self.subTest(options=options):
                text = f"sudo {options} -- python3 -m lab.prism.deleted"
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.commands(text + "; python3 lab/prism/storm.py"), ["lab/prism/storm.py"])

    def test_sudo_edit_mode_arguments_are_not_commands(self) -> None:
        for options in (
            "-e", "--edit", "'-e'", "-ne", "-en", "-Hne", "-euprism",
            "-nepPrompt", "-n -u prism --edit", "-D /tmp -e",
        ):
            for argument in (
                "python3 -m lab.prism.deleted", "python3 lab/prism/deleted.py",
                "sh -c 'python3 -m lab.prism.deleted'",
            ):
                with self.subTest(options=options, argument=argument):
                    text = f"sudo {options} -- {argument}"
                    self.assertEqual(self.commands(text), [])
                    self.assertEqual(len(self.references(text)), 1)
                    self.assertEqual(
                        self.commands(text + "; python3 lab/prism/storm.py"),
                        ["lab/prism/storm.py"],
                    )
        self.assertEqual(self.commands("sudo -e python3 -m lab.prism.deleted"), [])
        self.assertEqual(self.commands("env sudo -ne -- python3 lab/prism/deleted.py"), [])
        text = "```sh\nsudo -e \\\n  -- python3 -m lab.prism.deleted\npython3 lab/prism/storm.py\n```"
        self.assertEqual(self.located(text), [(4, "lab/prism/storm.py")])

    def test_sudo_credential_modes_do_not_run_commands(self) -> None:
        for options in (
            "-K", "--remove-timestamp", "'-K'", "-nK", "-Kn", "-HnK",
            "-v", "--validate", "'-v'", "-nv", "-vn", "-Hnv", "-vuprism",
            "-n -u prism --validate", "-p Prompt -v",
        ):
            for argument in (
                "python3 -m lab.prism.deleted", "python3 lab/prism/deleted.py",
                "sh -c 'python3 -m lab.prism.deleted'",
            ):
                for delimiter in ("", "-- "):
                    with self.subTest(options=options, argument=argument, delimiter=delimiter):
                        text = f"sudo {options} {delimiter}{argument}"
                        self.assertEqual(self.commands(text), [])
                        self.assertEqual(len(self.references(text)), 1)
                        self.assertEqual(
                            self.commands(text + "; python3 lab/prism/storm.py"),
                            ["lab/prism/storm.py"],
                        )
        self.assertEqual(self.commands("env sudo -nv -- python3 lab/prism/deleted.py"), [])
        text = "```sh\nsudo --validate \\\n  -- python3 -m lab.prism.deleted\npython3 lab/prism/storm.py\n```"
        self.assertEqual(self.located(text), [(4, "lab/prism/storm.py")])

    def test_sudo_timestamp_reset_and_mode_option_arguments_preserve_commands(self) -> None:
        for options in (
            "-k", "--reset-timestamp", "-nk", "-kn", "-kuprism", "-k -u prism",
            "-p K", "-pK", "-p -K", "--prompt=K", "-p v", "-pv", "-p -v",
            "-u v", "-uv", "--user=v", "--prompt=validate", "-u remove-timestamp",
        ):
            with self.subTest(options=options):
                self.assertEqual(
                    self.commands(f"sudo {options} python3 lab/prism/storm.py"),
                    ["lab/prism/storm.py"],
                )

    def test_sudo_option_arguments_containing_edit_flags_preserve_commands(self) -> None:
        for options in (
            "-p e", "-pe", "-peditor", "--prompt=editor", "-p -e",
            "-u editor", "-ueditor", "-geditors", "-D repo", "-Drepo",
            "--chdir=repo", "-D -e",
        ):
            with self.subTest(options=options):
                self.assertEqual(
                    self.commands(f"sudo {options} python3 lab/prism/storm.py"),
                    ["lab/prism/storm.py"],
                )

    def test_multiline_quoted_arguments_remain_data(self) -> None:
        for quote in ("'", '"'):
            with self.subTest(quote=quote):
                text = (
                    f"printf '%s\\n' {quote}first line\n"
                    "python3 -m lab.example.deleted\n"
                    f"last line{quote}; python3 lab/prism/storm.py"
                )
                self.assertEqual(self.located(text), [(3, "lab/prism/storm.py")])

    def test_heredoc_bodies_are_not_executable_commands(self) -> None:
        for delimiter in ("EOF", "'EOF'", '"EOF"', "E'O'F", r"\EOF"):
            for operator, indent in (("<<", ""), ("<<-", "\t")):
                with self.subTest(delimiter=delimiter, operator=operator):
                    text = (
                        f"cat {operator}{delimiter}\n"
                        f"{indent}python3 -m lab.example.deleted\n"
                        f"{indent}python3 lab/example/deleted.py\n"
                        f"{indent}EOF\n"
                        "python3 lab/prism/storm.py"
                    )
                    self.assertEqual(self.located(text), [(5, "lab/prism/storm.py")])

    def test_indented_fenced_heredocs_end_at_the_rendered_delimiter(self) -> None:
        for operator, tabs in (("<<", ""), ("<<-", "\t")):
            with self.subTest(operator=operator):
                text = (
                    f"- example\n\n  ```sh\n  cat {operator}'EOF'\n"
                    "  python3 -m lab.example.deleted\n"
                    f"  {tabs}EOF\n  python3 lab/prism/storm.py\n  ```"
                )
                self.assertEqual(self.located(text), [(7, "lab/prism/storm.py")])

    def test_file_descriptor_redirections_are_not_executable_words(self) -> None:
        for prefix in (
            "2>/dev/null", "2>/dev/null env", "env 2>/dev/null", "command --",
            "X='value'", "env 'X=1'",
        ):
            with self.subTest(prefix=prefix):
                self.assertEqual(self.commands(f"{prefix} python3 lab/prism/storm.py"), ["lab/prism/storm.py"])
        self.assertEqual(self.commands("printf '%s\\n' 2>/dev/null python3 -m lab.example.deleted"), [])

    def test_redirections_among_python_options_and_targets_are_removed(self) -> None:
        missing = "lab/example/deleted.py or lab/example/deleted/__main__.py"
        for redirection in (
            "2>/dev/null", "</dev/null", "2> /dev/null", "> out.log", ">>out.log", "2>&1", ">&2",
            "2>&-", "<&0", "&>/dev/null", "&>> out.log", ">|out.log", "<>tty", "<<<'data'",
            "2>'quoted file'", '>"$log"', "2>/dev/null </dev/null",
        ):
            for command in (
                f"python3 {redirection} -m lab.example.deleted",
                f"python3 -O {redirection} -m lab.example.deleted",
                f"python3 -m {redirection} lab.example.deleted",
                f"python3 -X {redirection} dev -m lab.example.deleted",
                f"python3 -W {redirection} error -m lab.example.deleted",
                f"python3 --check-hash-based-pycs {redirection} always -m lab.example.deleted",
                f"{redirection} python3 -m lab.example.deleted",
                f"env {redirection} python3 -m lab.example.deleted",
            ):
                with self.subTest(command=command):
                    self.assertEqual(self.commands(command), [missing])
                    self.assertEqual(list(dead_commands(command, {"lab/example/deleted.py"})), [])
        for command in (
            "python3 2>/dev/null lab/example/deleted.py", "python3 -- 2>&1 lab/example/deleted.py",
            "python3>/dev/null -OO lab/example/deleted.py", "python3 -O>log lab/example/deleted.py",
            "true && python3 </dev/null lab/example/deleted.py", "true & python3 2>&1 lab/example/deleted.py",
        ):
            with self.subTest(command=command):
                self.assertEqual(self.commands(command), ["lab/example/deleted.py"])
                self.assertEqual(list(dead_commands(command, {"lab/example/deleted.py"})), [])

    def test_redirected_commands_keep_their_text_and_first_line(self) -> None:
        text = "```sh\ntrue\npython3 2>/dev/null \\\n  </dev/null -m lab.example.deleted >out.log\n```"
        self.assertEqual(
            [(number, command) for number, command, _ in dead_commands(text, self.TRACKED)],
            [(3, "python3 2>/dev/null   </dev/null -m lab.example.deleted")],
        )
        text = "python3 <<'EOF' -m lab.example.deleted\npython3 -m lab.prism.gone\nEOF\npython3 2>&1 lab/prism/storm.py"
        self.assertEqual(
            self.located(text),
            [(1, "lab/example/deleted.py or lab/example/deleted/__main__.py"), (4, "lab/prism/storm.py")],
        )

    def test_redirection_text_in_data_and_escapes_is_not_removed(self) -> None:
        for text in (
            "python3 2 >/dev/null -m lab.example.deleted",
            "python3 '2'>/dev/null -m lab.example.deleted",
            "python3 2&>/dev/null -m lab.example.deleted",
            "python3 2&>>/dev/null -m lab.example.deleted",
            "python3 ٢>/dev/null -m lab.example.deleted",
            "٢>/dev/null python3 -m lab.example.deleted",
            "python3 '2>/dev/null' -m lab.example.deleted",
            'python3 "</dev/null" -m lab.example.deleted',
            "python3 2'>'/dev/null -m lab.example.deleted",
            "python3 \\>out -m lab.example.deleted",
            "python3 2\\>out -m lab.example.deleted",
            "python3 -c 2>/dev/null -m lab.example.deleted",
            "env -S 'python3 2>/dev/null -m lab.example.deleted'",
            "python3 2> ; -m lab.example.deleted",
            "printf '%s\\n' 2>&1 python3 -m lab.example.deleted",
            "echo x >&2 python3 -m lab.example.deleted",
            "echo x &>/dev/null python3 -m lab.example.deleted",
            "echo x &>> log python3 -m lab.example.deleted",
            "echo x >| log python3 -m lab.example.deleted",
            "echo x <&0 python3 -m lab.example.deleted",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])

    def test_heredoc_data_cannot_change_comments_continuations_or_fences(self) -> None:
        text = (
            "```sh\ncat <<'EOF'\n"
            "# quoted data \\\n"
            "```\n"
            "python3 -m lab.example.deleted\n"
            "EOF\n"
            "# python3 -m lab.example.deleted\n"
            "python3 lab/prism/storm.py\n```"
        )
        self.assertEqual(self.located(text), [(8, "lab/prism/storm.py")])
        # Only tabs are stripped by <<-; whitespace or trailing text cannot
        # prematurely close a heredoc and turn its remaining body into code.
        text = "cat <<-EOF\n EOF\nEOF extra\npython3 -m lab.example.deleted\n\tEOF\npython3 lab/prism/storm.py"
        self.assertEqual(self.located(text), [(6, "lab/prism/storm.py")])

    def test_multiple_heredocs_and_commands_on_the_opening_line(self) -> None:
        text = (
            "cat <<FIRST <<'SECOND'; python3 lab/prism/storm.py\n"
            "python3 -m lab.example.deleted\nFIRST\n"
            "python3 -m lab.example.deleted\nSECOND\n"
            "python3 -m lab.prism.process_telemetry"
        )
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py"), (6, self.TELEMETRY)])
        text = "python3 lab/prism/storm.py <<EOF\npython3 -m lab.example.deleted\nEOF"
        self.assertEqual(self.located(text), [(1, "lab/prism/storm.py")])
        text = "printf '%s\\n' 'cat <<EOF'\npython3 lab/prism/storm.py"
        self.assertEqual(self.located(text), [(2, "lab/prism/storm.py")])

    def test_shell_command_strings_are_scanned_in_their_own_context(self) -> None:
        for prefix in ("sh -c", "bash -lc", 'docker exec "$c" sh -c'):
            with self.subTest(prefix=prefix):
                text = f'''{prefix} "printf '%s\\n' 'python3 -m lab.example.deleted'; python3 lab/prism/storm.py"'''
                self.assertEqual(self.commands(text), ["lab/prism/storm.py"])
                text = f'''{prefix} 'true # python3 -m lab.example.deleted' '''
                self.assertEqual(self.commands(text), [])
                text = f'''{prefix} '# `python3 -m lab.example.deleted`' '''
                self.assertEqual(self.commands(text), [])
        text = "sh -c 'true\npython3 lab/prism/storm.py'"
        self.assertEqual(self.located(text), [(2, "lab/prism/storm.py")])
        self.assertEqual(self.commands("sh -c python3 -m lab.example.deleted"), [])

    def test_shell_option_arguments_before_command_strings_are_consumed(self) -> None:
        for options in (
            "-o pipefail -c", "-O extglob -c", "+o pipefail -c", "+O extglob -c",
            "-eo pipefail -c", "-co pipefail", "-oc pipefail", "-cO extglob",
            "-o pipefail -O extglob -c", "'-o' 'pipefail' '-c'",
            "--rcfile /dev/null -c", "--init-file /dev/null -c", "-c -e", "-c --",
        ):
            for command, missing in (
                ("python3 -m lab.example.deleted", "lab/example/deleted.py or lab/example/deleted/__main__.py"),
                ("python3 lab/example/deleted.py", "lab/example/deleted.py"),
            ):
                with self.subTest(options=options, command=command):
                    text = f"bash {options} '{command}'"
                    self.assertEqual(self.located(text), [(1, missing)])
                    self.assertEqual(list(dead_commands(text, {"lab/example/deleted.py"})), [])
        text = 'docker exec "$c" bash -o pipefail -c \'true\npython3 lab/example/deleted.py\''
        self.assertEqual(self.located(text), [(2, "lab/example/deleted.py")])
        self.assertEqual(self.commands("sh -o errexit -c 'python3 lab/prism/storm.py'"), ["lab/prism/storm.py"])

    def test_noexec_shell_command_strings_are_not_runnable(self) -> None:
        for shell in ("bash", "sh", "dash", "ksh", "zsh"):
            for options in (
                "-n -c", "-nc", "-cn", "-c -n", "-en -c", "'-n' '-c'",
                "-o noexec -c", "-co noexec", "-c -o noexec", "-o 'noexec' -c",
                "+n -n -c", "+o noexec -n -c", "-n +n -o noexec -c",
                "-n -o errexit -c", "-n +o errexit -c", "-n -c --",
            ):
                with self.subTest(shell=shell, options=options):
                    text = f"{shell} {options} 'python3 -m lab.prism.deleted'"
                    self.assertEqual(self.commands(text), [])
                    self.assertEqual(self.references(text), ["lab.prism.deleted"])

    def test_disabling_noexec_restores_shell_command_scanning(self) -> None:
        for shell in ("bash", "sh", "dash", "ksh", "zsh"):
            for options in (
                "+n -c", "+o noexec -c", "-n +n -c", "-n +en -c",
                "-n +o noexec -c", "-o noexec +n -c", "-o noexec +o noexec -c",
                "-n -c +n", "-c -n +o noexec", "-co noexec +n",
            ):
                with self.subTest(shell=shell, options=options):
                    text = f"{shell} {options} 'python3 lab/prism/storm.py'"
                    self.assertEqual(self.commands(text), ["lab/prism/storm.py"])
            text = f"{shell} -c 'python3 lab/prism/storm.py' -n"
            self.assertEqual(self.commands(text), ["lab/prism/storm.py"])

    def test_noexec_tracking_preserves_shell_option_arguments(self) -> None:
        for options in (
            "-o nounset -c", "--rcfile noexec -c", "--init-file noexec -c",
            "-on noexec +n -c", "-no noexec +n -c", "-oo noexec errexit +n -c",
        ):
            with self.subTest(options=options):
                text = f"bash {options} 'python3 lab/prism/storm.py'"
                self.assertEqual(self.commands(text), ["lab/prism/storm.py"])
        for options in ("-on errexit -c", "-oon errexit noexec -c"):
            with self.subTest(options=options):
                self.assertEqual(self.commands(f"bash {options} 'python3 lab/prism/storm.py'"), [])

    def test_shell_option_arguments_are_not_command_strings(self) -> None:
        command = "python3 -m lab.example.deleted"
        for text in (
            f"bash -o '{command}' -c true",
            f"bash -O '{command}' -c true",
            f"bash --rcfile '{command}' -c true",
            f"bash -o -c '{command}'",
            f"bash -o pipefail -- -c '{command}'",
            f"bash -o pipefail script.sh -c '{command}'",
            f"bash --help -c '{command}'",
            f"bash -o pipefail -c \"echo '{command}'\"",
            "bash -o", "bash -O extglob -c", "bash --rcfile",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])

    def test_shell_data_still_counts_as_dangling_prose(self) -> None:
        for text in (
            "printf '%s\\n' 'python3 -m lab.prism.deleted'",
            "cat <<'EOF'\npython3 -m lab.prism.deleted\nEOF",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.commands(text), [])
                self.assertEqual(self.references(text), ["lab.prism.deleted"])


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""The release block in docs/releasing.md refuses before it tags anything.

A pushed `v*` tag can't be moved or deleted (.github/rulesets/release-tags.json),
so the block must refuse, before `git tag`, a commit that isn't on main, a
VERSION that isn't the version, notes without a release date, CI that hasn't
passed on the commit, and an L3 verdict that a tag wouldn't carry as a pass.

These tests run the block exactly as the guide prints it, with only its
`version=` and `sha=` lines filled in, in an interactive bash fed from stdin
as a paste is (so history expansion and comment handling apply), from a
subdirectory of a temporary clone. `origin` is a bare repository; `gh` is a
fake, first on PATH, that answers from a state file; the signing key is a
throwaway one in a temporary GNUPGHOME. Git and gpg read only the temporary
HOME, GIT_CONFIG_GLOBAL and GNUPGHOME: never this checkout's or the host's
configuration. L3's decision is the real scripts/prism_l3_promote.py, which
the block reads from the commit it would tag.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
GUIDE = ROOT / "docs" / "releasing.md"
PROMOTE = ROOT / "scripts" / "prism_l3_promote.py"
REPO = "Qbit-Org/qbit-mining-bootstrap"
VERSION = "3.1.2"
TAG = f"v{VERSION}"
L3_RUN = 4242

NOTES = """# qbit-mining-bootstrap 3.1.2 Release Notes

Release date: {date}

A patch release of [3.1.1](release-notes-3.1.1.md); see [the guide](../docs/releasing.md)
and [the top](#top), or [the site](https://example.invalid/x).
"""

# The fake `gh`. It logs every call, answers `run list`, the artifact and run
# lookups and `run download` from FAKE_GH_STATE, and for `release create`
# requires the tag on the bare origin, as --verify-tag does, and keeps a copy
# of the notes file, which the block deletes on exit.
FAKE_GH = r'''
import json, os, pathlib, subprocess, sys

args = sys.argv[1:]
state = json.loads(pathlib.Path(os.environ["FAKE_GH_STATE"]).read_text())
log = pathlib.Path(os.environ["FAKE_GH_LOG"])
with log.open("a") as handle:
    handle.write(json.dumps(args) + "\n")


def option(name):
    return args[args.index(name) + 1] if name in args else None


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(1)


repo = state["repo"]
if args[:2] == ["run", "list"]:
    if state["ci"] == "unreachable":
        fail("error connecting to api.github.com")
    commit = option("--commit")
    runs = {
        "none": [],
        "success": [("completed", "success")],
        "failure": [("completed", "success"), ("completed", "failure")],
        "cancelled": [("completed", "cancelled")],
        "in_progress": [("in_progress", "")],
    }[state["ci"]]
    print(json.dumps([
        {"databaseId": 900 + n, "headSha": commit, "status": status, "conclusion": conclusion,
         "url": f"https://github.com/{repo}/actions/runs/{900 + n}"}
        for n, (status, conclusion) in enumerate(runs)
    ]))
elif args[:1] == ["api"] and args[1].startswith(f"repos/{repo}/actions/artifacts?name=prism-l3-verdict-"):
    if state["l3"] == "unreachable":
        fail("HTTP 502")
    name = args[1].split("name=", 1)[1].split("&", 1)[0]
    artifacts = [] if state["l3"] == "missing" else [{
        "id": 1, "name": name, "expired": False, "created_at": "2026-11-17T12:00:00Z",
        "workflow_run": {"id": state["l3_run"]},
    }]
    if state.get("newer"):
        artifacts.insert(0, {
            "id": 2, "name": name, "expired": False, "created_at": "2026-11-17T13:00:00Z",
            "workflow_run": {"id": state["l3_run"] + 1},
        })
    print(json.dumps({"total_count": len(artifacts), "artifacts": artifacts}))
elif args[:1] == ["api"] and args[1] == f"repos/{repo}/actions/runs/{state['l3_run'] + 1}":
    if state["newer"] == "unreadable":
        fail("HTTP 502")
    print(json.dumps({
        "id": state["l3_run"] + 1,
        "event": "workflow_dispatch" if state["newer"] == "dispatch" else "pull_request",
        "path": ".github/workflows/prism-load-l3.yml",
        "head_repository": {"full_name": repo},
        "status": "in_progress" if state["newer"] == "in_progress" else "completed",
        "conclusion": "" if state["newer"] == "in_progress" else "success",
        "html_url": f"https://github.com/{repo}/actions/runs/{state['l3_run'] + 1}",
        "pull_requests": [],
    }))
elif args[:1] == ["api"] and args[1] == f"repos/{repo}/actions/runs/{state['l3_run']}":
    print(json.dumps({
        "id": state["l3_run"],
        "event": "workflow_dispatch" if state["l3"] == "dispatch" else "pull_request",
        "path": ".github/workflows/prism-load-l3.yml",
        "head_repository": {"full_name": repo},
        "status": "completed",
        "conclusion": "failure" if state["l3"] == "failed" else "success",
        "html_url": f"https://github.com/{repo}/actions/runs/{state['l3_run']}",
        "pull_requests": [{"number": 900}],
    }))
elif args[:2] == ["run", "download"]:
    name = option("-n")
    pathlib.Path(option("-D"), "verdict.json").write_text(json.dumps({
        "schema": "qbit.prism.l3-verdict.v1",
        "tree": name.removeprefix("prism-l3-verdict-"),
        "suite": "l3-full",
        "complete": True,
        "planned": 33,
        "gated_runs": 33,
        "passed": state["l3"] != "failed",
    }))
elif args[:2] == ["release", "create"]:
    found = subprocess.run(
        ["git", "--git-dir", state["origin"], "rev-parse", "--verify", "--quiet", f"refs/tags/{args[2]}"],
        capture_output=True,
    )
    if found.returncode:
        fail(f"tag {args[2]} doesn't exist in the repository")
    log.with_suffix(".notes").write_text(pathlib.Path(option("--notes-file")).read_text())
    if state.get("release_fails"):
        fail("HTTP 422: Validation Failed")
    print(f"https://github.com/{repo}/releases/tag/{args[2]}")
else:
    fail(f"fake gh: unexpected arguments {args}")
'''

# For one test: gpg that signs but never verifies, so `git verify-tag` fails
# right after `git tag -s` made the tag.
GPG_THAT_DOES_NOT_VERIFY = """#!/bin/sh
for arg in "$@"; do
  [ "$arg" = --verify ] && exit 1
done
exec gpg "$@"
"""


def release_block() -> str:
    section = GUIDE.read_text(encoding="utf-8").split("\n## Publish the reviewed main commit\n", 1)[1]
    section = section.split("\n## ", 1)[0]
    blocks = [b for b in re.findall(r"^```bash\n(.*?)^```$", section, flags=re.M | re.S) if "git tag -s" in b]
    if len(blocks) != 1:
        raise AssertionError(f"expected one bash block with git tag -s in the section, found {len(blocks)}")
    return blocks[0]


def fill(block: str, version: str | None, sha: str | None) -> str:
    """The block with its `version=` and `sha=` lines filled in, as the guide says to."""
    for placeholder, value in (("version='<version>'", version), ("sha='<release-main-sha>'", sha)):
        if value is not None:
            block = block.replace(placeholder, placeholder.split("=", 1)[0] + f"='{value}'", 1)
    return block


def git(*args: str, cwd: Path, env: dict[str, str]) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True
    ).stdout.strip()


class ReleaseBlockTextTests(unittest.TestCase):
    """What a paste needs from the block's text."""

    def test_each_placeholder_appears_once_in_its_assignment(self) -> None:
        block = release_block()
        self.assertEqual(block.count("<version>"), 1)
        self.assertIn("\n  version='<version>'\n", block)
        self.assertEqual(block.count("<release-main-sha>"), 1)
        self.assertIn("\n  sha='<release-main-sha>'\n", block)

    def test_every_refusal_comes_before_the_tag_and_the_tag_before_the_push(self) -> None:
        lines = release_block().splitlines()
        tag = next(n for n, line in enumerate(lines) if "git tag -s" in line)
        push = next(n for n, line in enumerate(lines) if "git push" in line)
        refusals = [n for n, line in enumerate(lines) if "refuse" in line and "refuse()" not in line]
        self.assertGreater(len(refusals), 10)
        self.assertLess(max(refusals), tag)
        self.assertLess(tag, push)

    def test_it_pastes_into_zsh_and_bash_unchanged(self) -> None:
        # Interactive zsh doesn't treat # as a comment by default, and history
        # expansion in both shells rewrites ! anywhere but in != or before a blank.
        # In zsh, `$name:x` applies the history modifier x (`$sha:scripts/...`
        # would substitute) and `$name[...]` subscripts: brace such names.
        shell, in_heredoc = [], False
        for line in release_block().splitlines():
            if in_heredoc:
                in_heredoc = line != "EOF"
                continue
            shell.append(line)
            in_heredoc = "<<'EOF'" in line
        self.assertIn("git push", "\n".join(shell))
        self.assertTrue(any("<<'EOF' || refuse" in line for line in shell))
        for line in release_block().splitlines():
            self.assertFalse(line.lstrip().startswith("#"), line)
            self.assertIsNone(re.search(r"!(?![=\s])", line), line)
        for line in shell:
            self.assertIsNone(re.search(r"\$[A-Za-z_][A-Za-z0-9_]*[:\[]", line), line)


class ReleaseBlockTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        missing = [tool for tool in ("bash", "git", "gpg", "gpgconf") if shutil.which(tool) is None]
        if missing:
            raise AssertionError(f"the release block test needs {', '.join(missing)} on PATH")
        cls.home = Path(tempfile.mkdtemp(prefix="relblk-"))
        cls.bin = cls.home / "bin"
        cls.bin.mkdir()
        (cls.bin / "gh").write_text(f"#!{sys.executable}\n{FAKE_GH}", encoding="utf-8")
        (cls.bin / "gh").chmod(0o755)
        (cls.bin / "gpg-no-verify").write_text(GPG_THAT_DOES_NOT_VERIFY, encoding="utf-8")
        (cls.bin / "gpg-no-verify").chmod(0o755)
        (cls.bin / "python3").symlink_to(sys.executable)
        (cls.home / "home").mkdir()
        gnupg = cls.home / "gnupg"
        gnupg.mkdir(mode=0o700)
        cls.env = {
            "PATH": f"{cls.bin}{os.pathsep}{os.environ.get('PATH', '/usr/bin:/bin')}",
            "HOME": str(cls.home / "home"),
            "GNUPGHOME": str(gnupg),
            "GIT_CONFIG_GLOBAL": str(cls.home / "home" / "gitconfig"),
            "GIT_CONFIG_NOSYSTEM": "1",
            "HISTFILE": os.devnull,
            "PS1": "",
            "PS2": "",
            "LC_ALL": "C",
        }
        try:
            subprocess.run(
                ["gpg", "--batch", "--pinentry-mode", "loopback", "--passphrase", "",
                 "--quick-generate-key", "Release Block Test <release-block@example.invalid>",
                 "ed25519", "sign", "never"],
                env=cls.env, check=True, capture_output=True, timeout=120,
            )
            listing = subprocess.run(
                ["gpg", "--batch", "--with-colons", "--list-secret-keys"],
                env=cls.env, check=True, capture_output=True, text=True, timeout=60,
            ).stdout
        except BaseException:
            cls.tearDownClass()
            raise
        cls.key = next(line.split(":")[9] for line in listing.splitlines() if line.startswith("fpr:"))

    @classmethod
    def tearDownClass(cls) -> None:
        for command in (["gpgconf", "--kill", "all"], ["gpgconf", "--remove-socketdir"]):
            subprocess.run(command, env=cls.env, capture_output=True, timeout=60, check=False)
        shutil.rmtree(cls.home, ignore_errors=True)

    def setUp(self) -> None:
        self.base = Path(tempfile.mkdtemp(prefix="world-", dir=self.home))
        self.addCleanup(shutil.rmtree, self.base, ignore_errors=True)
        self.origin = self.base / "origin.git"
        self.work = self.base / "work"
        self.tmp = self.base / "tmp"
        self.tmp.mkdir()
        self.log = self.base / "gh.log"
        self.state_file = self.base / "gh-state.json"
        self.state = {"repo": REPO, "origin": str(self.origin), "ci": "success", "l3": "passed", "l3_run": L3_RUN}
        git("init", "--quiet", "--bare", str(self.origin), cwd=self.base, env=self.env)
        git("init", "--quiet", "-b", "main", str(self.work), cwd=self.base, env=self.env)
        for key, value in (
            ("user.name", "Release Block Test"), ("user.email", "release-block@example.invalid"),
            ("user.signingkey", self.key), ("commit.gpgsign", "false"), ("tag.gpgsign", "false"),
            ("remote.origin.url", f"https://github.com/{REPO}"),
            (f"url.{self.origin}.insteadOf", f"https://github.com/{REPO}"),
            ("remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"),
        ):
            self.git("config", key, value)
        (self.work / "scripts").mkdir()
        shutil.copy(PROMOTE, self.work / "scripts" / "prism_l3_promote.py")
        (self.work / "doc").mkdir()
        (self.work / "doc" / "release-notes-3.1.1.md").write_text("# 3.1.1\n\nRelease date: 2026-10-29\n")
        self.commit("3.1.1", "3.1.1")
        self.c311 = self.head()
        self.write_notes("2026-11-20")
        self.commit("3.1.1", "the 3.1.2 notes, before VERSION")
        self.notes_only = self.head()
        self.commit(VERSION, "3.1.2")
        self.release = self.head()
        # main moves on after the promotion, so only "$sha" tags the release.
        (self.work / "LATER").write_text("a later commit on main\n")
        self.commit(VERSION, "later")
        self.later = self.head()
        self.git("push", "--quiet", "origin", "main")
        # The promotion's signed head: the same tree, but not on main.
        self.pr_head = self.git("commit-tree", f"{self.release}^{{tree}}", "-p", self.notes_only, "-m", "head")

    def git(self, *args: str) -> str:
        return git(*args, cwd=self.work, env=self.env)

    def head(self) -> str:
        return self.git("rev-parse", "HEAD")

    def write_notes(self, date: str) -> None:
        (self.work / "doc" / f"release-notes-{VERSION}.md").write_text(NOTES.format(date=date))

    def commit(self, version: str, message: str) -> None:
        (self.work / "VERSION").write_text(version + "\n")
        self.git("add", "-A")
        self.git("commit", "--quiet", "-m", message)

    def run_block(self, sha: str | None, version: str | None = VERSION, **extra_env: str) -> subprocess.CompletedProcess[str]:
        self.state_file.write_text(json.dumps(self.state))
        env = {**self.env, "TMPDIR": str(self.tmp), "FAKE_GH_STATE": str(self.state_file),
               "FAKE_GH_LOG": str(self.log), **extra_env}
        # Interactive, so history expansion applies as in a terminal; without
        # line editing, so the shell doesn't echo the block into stderr.
        result = subprocess.run(
            ["bash", "--norc", "--noprofile", "--noediting", "-i"],
            input=fill(release_block(), version, sha) + "\nexit $?\n",
            cwd=self.work / "doc", env=env, capture_output=True, text=True, timeout=300,
        )
        self.assertEqual(list(self.tmp.iterdir()), [], "the block left its temporary files behind")
        self.assertNotIn("event not found", result.stderr)
        self.assertNotIn("command not found", result.stderr)
        return result

    def gh_calls(self) -> list[list[str]]:
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def released_notes(self) -> str:
        return self.log.with_suffix(".notes").read_text()

    def tags(self) -> tuple[str, str]:
        local = self.git("tag", "--list", TAG)
        remote = git("--git-dir", str(self.origin), "tag", "--list", TAG, cwd=self.base, env=self.env)
        return local, remote

    def assertRefused(self, result: subprocess.CompletedProcess[str], *wanted: str) -> None:
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Not tagging: ", result.stderr)
        self.assertNotIn("Not creating the release", result.stderr)
        for text in wanted:
            self.assertIn(text, result.stderr)
        self.assertEqual(self.tags(), ("", ""), "a refusal left a tag behind")
        self.assertNotIn(["release", "create"], [call[:2] for call in self.gh_calls()])

    def test_the_tested_main_commit_is_tagged_and_released(self) -> None:
        result = self.run_block(self.release)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("Not tagging", result.stderr)
        self.assertIn(f"On main: {self.release}, VERSION {VERSION}, notes dated", result.stdout)
        self.assertIn(f"CI passed: https://github.com/{REPO}/actions/runs/900", result.stdout)
        self.assertIn(f"L3 passed: https://github.com/{REPO}/actions/runs/{L3_RUN}", result.stdout)
        tagged = git("--git-dir", str(self.origin), "rev-parse", f"refs/tags/{TAG}^{{commit}}", cwd=self.base, env=self.env)
        self.assertEqual(tagged, self.release)
        self.assertEqual(self.git("cat-file", "-t", TAG), "tag")
        self.git("verify-tag", TAG)
        self.assertEqual(self.git("tag", "--list", "--format=%(contents:subject)", TAG), f"qbit-mining-bootstrap {VERSION}")
        tree = self.git("rev-parse", f"{self.release}^{{tree}}")
        calls = self.gh_calls()
        self.assertIn(["run", "list", "--repo", REPO, "--workflow", "ci.yml", "--commit", self.release,
                       "--event", "push", "--limit", "100", "--json",
                       "databaseId,headSha,status,conclusion,url"], calls)
        self.assertIn(["api", f"repos/{REPO}/actions/artifacts?name=prism-l3-verdict-{tree}&per_page=100"], calls)
        create = [call for call in calls if call[:2] == ["release", "create"]]
        self.assertEqual(len(create), 1)
        self.assertEqual(create[0][2:9], [TAG, "--repo", REPO, "--verify-tag", "--prerelease=false",
                                          "--title", f"qbit-mining-bootstrap {VERSION}"])
        notes = self.released_notes()
        self.assertIn(f"[3.1.1](https://github.com/{REPO}/blob/{TAG}/doc/release-notes-3.1.1.md)", notes)
        self.assertIn(f"[the guide](https://github.com/{REPO}/blob/{TAG}/docs/releasing.md)", notes)
        self.assertIn("[the top](#top)", notes)
        self.assertIn("[the site](https://example.invalid/x)", notes)

    def test_a_clone_of_another_repository_is_refused(self) -> None:
        self.git("config", "remote.origin.url", "https://github.com/someone/qbit-mining-bootstrap")
        self.git("config", "url." + str(self.origin) + ".insteadOf", "https://github.com/someone/qbit-mining-bootstrap")
        self.assertRefused(
            self.run_block(self.release),
            f"origin is https://github.com/someone/qbit-mining-bootstrap, not github.com/{REPO}",
        )
        self.assertEqual(self.gh_calls(), [])

    def test_a_commit_not_on_main_is_refused(self) -> None:
        # The promotion's signed head has the release tree, CI and L3, but isn't on main.
        self.assertRefused(self.run_block(self.pr_head), f"{self.pr_head} is not on origin's main")
        unknown = "0123456789abcdef0123456789abcdef01234567"
        self.assertRefused(self.run_block(unknown), f"{unknown} is not a commit in this clone")

    def test_a_version_mismatch_is_refused(self) -> None:
        self.assertRefused(self.run_block(self.notes_only), f"VERSION at {self.notes_only} is 3.1.1, not {VERSION}")
        self.assertRefused(self.run_block(self.release, version="3.1.3"), f"VERSION at {self.release} is {VERSION}, not 3.1.3")

    def test_undated_or_missing_notes_are_refused(self) -> None:
        self.write_notes("pending")
        self.commit(VERSION, "undated notes")
        undated = self.head()
        self.git("push", "--quiet", "origin", "main")
        self.assertRefused(
            self.run_block(undated),
            f"doc/release-notes-{VERSION}.md at {undated} has no 'Release date: YYYY-MM-DD' line",
        )
        self.git("rm", "--quiet", f"doc/release-notes-{VERSION}.md")
        self.commit(VERSION, "no notes")
        no_notes = self.head()
        self.git("push", "--quiet", "origin", "main")
        self.assertRefused(self.run_block(no_notes), f"{no_notes} has no doc/release-notes-{VERSION}.md")

    def test_ci_that_has_not_passed_is_refused(self) -> None:
        cases = {
            "failure": "/actions/runs/901 concluded failure",
            "cancelled": "/actions/runs/900 concluded cancelled",
            "none": f"CI has no push run on {self.release}",
            "in_progress": "/actions/runs/900 is in_progress: wait for it",
            "unreachable": "could not list CI's runs",
        }
        for ci, wanted in cases.items():
            with self.subTest(ci=ci):
                self.state["ci"] = ci
                self.assertRefused(self.run_block(self.release), wanted)

    def test_an_l3_verdict_a_tag_would_not_carry_as_a_pass_is_refused(self) -> None:
        cases = {
            "failed": f"run https://github.com/{REPO}/actions/runs/{L3_RUN} ran l3-full on this tree and not every run passed",
            "missing": "L3: a tag would run l3-full again, after the release is public: rerun: no finished",
            "dispatch": "ran on 'workflow_dispatch', not pull_request",
            "unreachable": "rerun: the artifact lookup failed",
        }
        for l3, wanted in cases.items():
            with self.subTest(l3=l3):
                self.state["l3"] = l3
                result = self.run_block(self.release)
                self.assertRefused(result, wanted, f"a tag on {self.release} would not carry a passing L3 verdict")
                if l3 != "failed":
                    self.assertIn(f"-f ref={self.release} -f tag_dry_run=true", result.stderr)

    def test_a_newer_verdict_whose_run_is_unknown_is_refused(self) -> None:
        cases = {
            "unreadable": f"run {L3_RUN + 1}: lookup failed",
            "in_progress": f"run {L3_RUN + 1}: did not finish (status 'in_progress'",
        }
        for newer, wanted in cases.items():
            with self.subTest(newer=newer):
                self.state["newer"] = newer
                self.assertRefused(self.run_block(self.release), "is not known yet", wanted)

    def test_a_newer_dry_run_verdict_does_not_stop_a_promotion(self) -> None:
        # A tag_dry_run that ran the suite uploads a verdict a tag never promotes.
        self.state["newer"] = "dispatch"
        result = self.run_block(self.release)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"L3 passed: https://github.com/{REPO}/actions/runs/{L3_RUN}", result.stdout)

    def test_placeholders_and_malformed_inputs_are_refused_before_anything_runs(self) -> None:
        cases = [
            (None, None, "version= must be the version to publish"),
            (None, VERSION, "sha= must be the promoted commit's full 40-character id"),
            (self.release[:12], VERSION, "sha= must be"),
            ("main", VERSION, "sha= must be"),
            (self.release.upper(), VERSION, "sha= must be"),
            (self.release, "v3.1.2", "version= must be"),
            (self.release, "", "version= must be"),
            (self.release, "3.1", "version= must be"),
        ]
        for sha, version, wanted in cases:
            with self.subTest(sha=sha, version=version):
                self.assertRefused(self.run_block(sha, version), wanted)
        self.assertEqual(self.gh_calls(), [])

    def test_a_tag_that_does_not_verify_is_deleted_before_anything_is_pushed(self) -> None:
        self.git("config", "gpg.program", str(self.bin / "gpg-no-verify"))
        result = self.run_block(self.release)
        self.assertNotEqual(result.returncode, 0)
        # Every check passed: it was verify-tag, after git tag -s, that stopped it.
        self.assertIn(f"L3 passed: https://github.com/{REPO}/actions/runs/{L3_RUN}", result.stdout)
        self.assertNotIn("Not tagging", result.stderr)
        self.assertEqual(self.tags(), ("", ""))
        self.assertNotIn(["release", "create"], [call[:2] for call in self.gh_calls()])

    def test_after_the_release_failed_a_second_run_publishes_the_public_tag(self) -> None:
        self.state["release_fails"] = True
        first = self.run_block(self.release)
        self.assertNotEqual(first.returncode, 0)
        self.assertIn(f"{TAG} is public, but gh release create failed", first.stderr)
        public = git("--git-dir", str(self.origin), "rev-parse", f"refs/tags/{TAG}", cwd=self.base, env=self.env)
        self.state["release_fails"] = False
        self.log.unlink()
        second = self.run_block(self.release)
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        self.assertEqual(git("--git-dir", str(self.origin), "rev-parse", f"refs/tags/{TAG}", cwd=self.base, env=self.env), public)
        self.assertEqual([call[:3] for call in self.gh_calls() if call[:2] == ["release", "create"]], [["release", "create", TAG]])
        # The rerun checks everything again, so CI gone red refuses it too,
        # and says the tag is public.
        self.state["ci"] = "failure"
        self.log.unlink()
        third = self.run_block(self.release)
        self.assertNotEqual(third.returncode, 0)
        self.assertIn(
            f"Not creating the release for {TAG}, which is public at {self.release}: CI has not passed",
            third.stderr,
        )
        self.assertNotIn(["release", "create"], [call[:2] for call in self.gh_calls()])

    def test_a_verified_local_tag_that_origin_lacks_is_pushed(self) -> None:
        # What a push that failed leaves behind.
        self.git("tag", "--sign", "-m", f"qbit-mining-bootstrap {VERSION}", TAG, self.release)
        local = self.git("rev-parse", f"refs/tags/{TAG}")
        result = self.run_block(self.release)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(git("--git-dir", str(self.origin), "rev-parse", f"refs/tags/{TAG}", cwd=self.base, env=self.env), local)

    def test_a_tag_already_at_another_commit_is_refused(self) -> None:
        self.git("tag", "--sign", "-m", "earlier", TAG, self.notes_only)
        result = self.run_block(self.release)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"this clone has a tag {TAG} at {self.notes_only} that origin doesn't", result.stderr)
        self.git("push", "--quiet", "origin", f"refs/tags/{TAG}")
        result = self.run_block(self.release)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"origin's {TAG} is at {self.notes_only}, not {self.release}", result.stderr)
        tagged = git("--git-dir", str(self.origin), "rev-parse", f"refs/tags/{TAG}^{{commit}}", cwd=self.base, env=self.env)
        self.assertEqual(tagged, self.notes_only)
        self.assertNotIn(["release", "create"], [call[:2] for call in self.gh_calls()])


if __name__ == "__main__":
    unittest.main()

# Publishing a release

`main` is the release promotion branch. `3.x.x` is the development branch and
production line for the 3.x series, whose current release is 3.1.1; `2.x.x` is
the maintenance branch of the 2.x series. A release cut from an earlier point
of a development branch gets its own branch at that cutoff, as `3.0.x` does
for 3.0.0. A version is published only when its tag and GitHub release exist.
A version bump or merged preparation PR alone does not publish it.

## Prepare one release candidate

Keep one pending release version across `VERSION`, `[workspace.package].version`
in the root `Cargo.toml` (which every crate inherits), both `Cargo.lock` files
(the root and `crates/qbit-prism-server/fuzz`), and
`doc/release-notes-<version>.md`. `scripts/check_version_skew.py`, which CI
runs, checks that every workspace crate, and every workspace crate either
lockfile lists, has `VERSION`'s version, and that `VERSION`'s notes exist.
While a release is pending, its notes exist undated; the release block
below refuses notes without a release date. Ordinary development PRs
contribute to that pending release's notes; they do not each create a patch
release.
Choose the next version after reconciling the last published release and
any identifiers already distributed to operators.

For the first 2.x release, PR #246 is the single **2.0.0** promotion into
`main`. It includes the changes temporarily labelled 2.0.1 and 2.0.2 on
`2.x.x`; neither identifier was tagged or published as a GitHub release.
Their changes and migration requirements are consolidated in
[the 2.0.0 notes](../doc/release-notes-2.0.0.md). Do not create retrospective
2.0.1 or 2.0.2 tags for those preparation commits.

For 3.0.0, the cutoff is `4c134913` on `3.x.x`, the last commit before the
3.1 dual writer. `3.0.x` starts there and takes the version bump, the 3.0.0
notes and the release's dependency fixes (#776, merged as `aabe470f`). Unlike
2.0.0, `v3.0.0` was tagged and published on `3.0.x` at `aabe470f`, before the
promotion. The promotion into `main` (#778) then gives `main` the tagged tree,
plus this guide and a README banner that doesn't name a branch; it adds no
tag. The 3.0.0-rc.N pre-releases were tagged on `3.x.x`. 3.0.0-rc.7 was
prepared (#772) but never tagged, and its changes ship in 3.0.0. The L3
matrix for the release is recorded in #779.

3.1.0 needed no release branch: `v3.1.0` was tagged and published on `3.x.x`
itself, at the squash of its version bump and notes (#782, `59194d03`), as
`v3.1.0-rc.1` was. The promotion into `main` gives `main` that tagged tree,
plus this guide, a README banner that names the version, and the released
3.0.0 notes in place of `3.x.x`'s pre-release copy; the links in the
development-line note, the migration guide and the ledger operations guide,
which pointed at the notes at the `v3.0.0` tag, point at that local copy. It
also keeps #781's disposition-poll change: 3.1.0 made #781's tokio update
itself (#782) but not the poll change, since it changed no runtime code after
its release candidate. It adds no tag. #779 records the 400k two-frontend L3
failures on 3.0.0's tree; 3.1.0 doesn't fix their cause, #716.

3.1.1, a patch release of 3.1.0, was prepared on `3.x.x` (#810, `60309f95`)
and is tagged on the promoted `main` commit, as this guide's default is: its
notes were undated at #810's squash. The promotion's review led to #818 to
#821 on `3.x.x`, and the promotion takes `3.x.x` after them (`987027fe`) as
its cutoff. `3.x.x` already carried everything `main` had, since #786
merged 3.1.0's promotion back, so the promotion gives `main` `3.x.x`'s tree
at `987027fe` plus the release date, the fix PRs' numbers in the notes and
this record.

Merge the intended development cutoff into the promotion branch and retain
fixes already made during promotion review. The repository squash-merges
every pull request but a synchronization, and every commit on `main` needs a
verified signature. A maintainer squashes the promotion into one commit,
signs it and pushes it, and GitHub's squash merge then lands it on `main` as
one commit, which GitHub makes with the signed commit's message and signs, and
no merge commit. The rulesets are in [`.github/rulesets/`](../.github/rulesets/). Check package versions together,
run Docker lint and the relevant test gates, and review upgrade and rollback
instructions. Leave the release date out while publication is unscheduled;
set the actual release date in a reviewed commit before the final merge.

## Publish the reviewed main commit

Tag the promoted `main` commit unless the release was already tagged on its
release or development branch, as 3.0.0 and 3.1.0 were. After the promotion PR
passes required checks and is merged, identify the exact resulting commit on
`main`: GitHub's squash commit, which
`gh pr view <promotion-pr> --json mergeCommit -q .mergeCommit.oid` prints. The
maintainer's signed squash is a different commit from the promotion branch's
earlier head, and GitHub's squash commit is another: the signed one stays as
the pull request's head and isn't on `main`, so it is never the one to tag.

Then run the block below. Put the version in place of `<version>` and that
commit's full 40-character id in place of `<release-main-sha>`, in its
`version=` and `sha=` lines, and paste it into bash or zsh, in any directory
of a clone whose `origin` is this repository on GitHub, with `gh` signed in,
`python3` on `PATH`, and your signing key set up for `git tag -s`. It makes
the tag as `v2.0.0` was made, a signed annotated tag with the message
`qbit-mining-bootstrap <version>`; `v3.0.0` and `v3.1.0` are signed annotated
tags on their branches' commits, with their own messages. A version with a
pre-release part, such as `3.2.0-rc.1`, is published as a pre-release:

```bash
(
  set -e -o pipefail
  version='<version>'
  sha='<release-main-sha>'
  repo=Qbit-Org/qbit-mining-bootstrap
  tag="v$version"
  stage='Not tagging'
  refuse() { printf '%s: %s\n' "$stage" "$*" >&2; exit 1; }
  printf '%s\n' "$version" | grep -Eqx '[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?' ||
    refuse "version= must be the version to publish, such as 3.1.2"
  printf '%s\n' "$sha" | grep -Eqx '[0-9a-f]{40}' ||
    refuse "sha= must be the promoted commit's full 40-character id"
  prerelease=false
  case "$version" in *-*) prerelease=true ;; esac
  url=$(git config --get remote.origin.url) || refuse "run this in a clone of $repo, with origin"
  printf '%s\n' "$url" | grep -Eiqx '(https://([^@/]+@)?github\.com/|git@github\.com:|ssh://git@github\.com/)Qbit-Org/qbit-mining-bootstrap(\.git)?/?' ||
    refuse "origin is $url, not github.com/$repo"
  tmp=$(mktemp -d)
  made=
  trap 'if [ -n "$made" ]; then git tag -d "$tag" >/dev/null; fi; rm -rf "$tmp"' EXIT
  trap 'exit 1' HUP INT TERM
  git fetch origin '+refs/heads/main:refs/remotes/origin/main' --tags ||
    refuse "could not fetch main and the tags from origin: git says why above (a local tag that differs from origin's stops it)"
  git cat-file -e "$sha^{commit}" 2>/dev/null ||
    refuse "$sha is not a commit in this clone, so it is not on origin's main"
  git merge-base --is-ancestor "$sha" refs/remotes/origin/main ||
    refuse "$sha is not on origin's main: tag the commit main gained when the promotion merged, not the pull request's head or a 3.x.x commit"
  got=$(git show "${sha}:VERSION" 2>/dev/null) || refuse "$sha has no VERSION file"
  [ "$got" = "$version" ] || refuse "VERSION at $sha is $got, not $version"
  git show "${sha}:doc/release-notes-$version.md" > "$tmp/source.md" 2>/dev/null ||
    refuse "$sha has no doc/release-notes-$version.md"
  grep -Eq '^Release date: [0-9]{4}-[0-9]{2}-[0-9]{2}' "$tmp/source.md" ||
    refuse "doc/release-notes-$version.md at $sha has no 'Release date: YYYY-MM-DD' line"
  sed -E \
    -e "s|\]\(\.\./|](https://github.com/$repo/blob/$tag/|g" \
    -e "s|\]\(([^)#/:][^):]*)\)|](https://github.com/$repo/blob/$tag/doc/\1)|g" \
    "$tmp/source.md" > "$tmp/notes.md"
  grep -q '[^[:space:]]' "$tmp/notes.md" || refuse "the release notes are blank"
  printf 'On main: %s, VERSION %s, notes dated\n' "$sha" "$version"
  remote=$(git ls-remote --tags origin "refs/tags/$tag") ||
    refuse "could not read origin's tags: look with git ls-remote --tags origin $tag"
  public=$(printf '%s\n' "$remote" | awk -v ref="refs/tags/$tag" '$2 == ref { print $1 }')
  existing=
  if git rev-parse --quiet --verify "refs/tags/$tag" >/dev/null; then
    at=$(git rev-parse "refs/tags/$tag^{commit}") || refuse "$tag does not point at a commit"
    [ "$at" = "$sha" ] || [ -n "$public" ] ||
      refuse "this clone has a tag $tag at $at that origin doesn't: delete it (git tag -d $tag) and run this again"
    [ "$at" = "$sha" ] ||
      refuse "origin's $tag is at $at, not $sha, and a v* tag can't be moved: inspect it (git show $tag)"
    [ -z "$public" ] || [ "$public" = "$(git rev-parse "refs/tags/$tag")" ] ||
      refuse "origin's $tag is not the tag this clone has: run this again"
    git verify-tag "$tag" ||
      refuse "$tag is at $sha, but its signature doesn't verify here: if someone else made it, import their public key"
    existing=1
    [ -z "$public" ] || stage="Not creating the release for $tag, which is public at $sha"
  elif [ -n "$public" ]; then
    refuse "origin has $tag, but this clone didn't fetch it: run this again"
  fi
  gh run list --repo "$repo" --workflow ci.yml --commit "$sha" --event push --limit 100 \
    --json databaseId,headSha,status,conclusion,url > "$tmp/ci.json" ||
    refuse "could not list CI's runs: check gh auth status, then look with gh run list --repo $repo --workflow ci.yml --commit $sha"
  python3 -I - "$tmp/ci.json" "$sha" "$repo" <<'EOF' || refuse "CI has not passed on $sha"
import json, sys
path, sha, repo = sys.argv[1:]
runs = json.load(open(path, encoding="utf-8"))
if not isinstance(runs, list) or not all(isinstance(run, dict) for run in runs):
    sys.exit("gh listed CI's runs in a shape this block doesn't know")
if not runs:
    sys.exit(f"CI has no push run on {sha}: if main's push run hasn't started yet, wait, and look with gh run list --repo {repo} --workflow ci.yml --commit {sha}")
problems = []
for run in runs:
    number, url = run.get("databaseId"), run.get("url")
    if run.get("headSha") != sha:
        problems.append(f"gh listed run {url} for commit {run.get('headSha')}, not {sha}")
    elif run.get("status") != "completed":
        problems.append(f"CI run {url} is {run.get('status')}: wait for it (gh run watch {number} --repo {repo}) and run this again")
    elif run.get("conclusion") != "success":
        problems.append(f"CI run {url} concluded {run.get('conclusion')}: read it (gh run view {number} --repo {repo} --log-failed); re-run a flaky job (gh run rerun {number} --repo {repo} --failed) and run this again once it passes, or fix the cause on 3.x.x and promote again (docs/releasing.md says how that promotion gets its L3 run)")
    else:
        print(f"CI passed: {url}")
sys.exit("\n".join(problems) or None)
EOF
  git show "${sha}:scripts/prism_l3_promote.py" > "$tmp/l3_promote.py" 2>/dev/null ||
    refuse "$sha has no scripts/prism_l3_promote.py"
  tree=$(git rev-parse "$sha^{tree}")
  python3 -I "$tmp/l3_promote.py" --repo "$repo" --tree "$tree" --run-id 0 --out "$tmp/l3.json" >/dev/null ||
    refuse "scripts/prism_l3_promote.py could not decide"
  python3 -I - "$tmp/l3.json" "$tree" "$sha" "$repo" <<'EOF' || refuse "a tag on $sha would not carry a passing L3 verdict"
import json, sys
path, tree, sha, repo = sys.argv[1:]
decision = json.load(open(path, encoding="utf-8"))
if not isinstance(decision, dict):
    sys.exit("scripts/prism_l3_promote.py printed a decision this block doesn't know")
verdict = decision.get("verdict")
if decision.get("promote") is not True or not isinstance(verdict, dict):
    lines = [f"L3: a tag would run l3-full again, after the release is public: {decision.get('reason')}"]
    lines += [f"  run {c.get('run')}: {c.get('rejected')}" for c in decision.get("considered") or [] if isinstance(c, dict)]
    lines.append(f"If the L3 run of the pull request that landed this commit is still going, wait for it (gh run list --repo {repo} --workflow prism-load-l3.yml --event pull_request) and run this again. A pull request into main that doesn't change VERSION gets that run only from release-candidate applied after its last change, before it merges (docs/releasing.md). To see what a tag would do: gh workflow run prism-load-l3.yml --repo {repo} -f ref={sha} -f tag_dry_run=true")
    sys.exit("\n".join(lines))
if decision.get("tree") != tree or verdict.get("tree") != tree:
    sys.exit(f"L3: the decision is for tree {decision.get('tree')}, verdict tree {verdict.get('tree')}, not {tree}")
unknown = []
for c in decision.get("considered") or []:
    why = str(c.get("rejected")) if isinstance(c, dict) else ""
    if why.startswith(("lookup failed", "its verdict could not be read")) or (why.startswith("did not finish") and "status 'completed'" not in why):
        unknown.append(f"  run {c.get('run')}: {why}")
if unknown:
    sys.exit("\n".join([f"L3: a newer verdict for this tree than run {decision.get('source_run')}'s is not known yet, and the tag's run could promote it instead:"] + unknown + ["Wait until the run finishes, or GitHub answers, and run this again"]))
if verdict.get("passed") is not True:
    sys.exit(f"L3: run {decision.get('source_url')} ran l3-full on this tree and not every run passed, so a tag would carry that failure")
print(f"L3 passed: {decision.get('source_url')}")
EOF
  if [ -z "$existing" ]; then
    git tag -s "$tag" "$sha" -m "qbit-mining-bootstrap $version"
    made=1
    git verify-tag "$tag"
  fi
  if [ -z "$public" ]; then
    made=
    git push origin "refs/tags/$tag"
  fi
  gh release create "$tag" --repo "$repo" --verify-tag --prerelease="$prerelease" \
    --title "qbit-mining-bootstrap $version" \
    --notes-file "$tmp/notes.md" ||
    { printf '%s is public, but gh release create failed: check gh release view %s --repo %s\n' "$tag" "$tag" "$repo" >&2; exit 1; }
)
```

Before it creates the tag, the block refuses, saying what is wrong and what to
do, unless all of these hold:

- `<version>` is a version, `<release-main-sha>` a full commit id, and
  `origin` this repository on GitHub, over HTTPS or SSH.
- The commit is on `origin`'s `main`, which the block fetches first into
  `refs/remotes/origin/main`, whatever the clone's own fetch settings. A local
  tag that differs from `origin`'s stops the fetch, and git names it.
- The commit's `VERSION` is the version, and its
  `doc/release-notes-<version>.md` has a `Release date: YYYY-MM-DD` line.
- CI (`ci.yml`) has at least one push run on the commit, and every one has
  finished and passed. CI isn't a required check, and it reads some things
  live, such as cargo-deny's advisories, so a commit can go red after it
  lands. A run still going, a failed or cancelled run, no run, and a listing
  that fails all refuse, with the command to look.
- A tag on the commit would promote a passing L3 run. The block asks the
  commit's own `scripts/prism_l3_promote.py`, as the tag's run of
  `prism-load-l3.yml` will: it must name a finished same-repository pull
  request run of `l3-full` on the commit's tree whose verdict passed. A failed
  verdict refuses, since the tag's run would carry it; so does no promotable
  run, a lookup that fails included, since the tag's run would then run the
  suite only after the release is public. So does a newer verdict for the
  tree whose run is still going or that GitHub didn't answer for, since the
  tag's run could promote that one instead.
- `v<version>` doesn't exist yet, or it points at the commit and its
  signature verifies (see below).

Each check reads GitHub as it is when the block runs, so after fixing the
cause, or once a run finishes, run the block again. While the tag doesn't
exist yet, a cause on `main`, such as the notes, is fixed through a reviewed
PR into `main`, and the commit that PR lands as is the one to check and tag.

The commit to tag needs a finished L3 run of `l3-full` on its tree, from the
pull request that landed it: a run on that pull request's merge result after
its last change, while `main` stays where it was until it merges (a
maintainer's squash keeps the tree). The pull request must come from a
branch of this repository, since a fork's run is never promoted. A promotion
that changes `VERSION` runs the suite on every push, and the block refuses
for L3 while that run is still going: wait for it, as it uploads its verdict
at the end. A pull request into `main` that doesn't change `VERSION`, such as
a second promotion of the same version or, as a fallback when the date
wasn't set before the promotion merged, one that dates or fixes the notes,
runs the suite only on the `labeled` event of `release-candidate`. After its
last change, remove the label if it is there, apply it, and let the run
finish before it merges. Applying a label the pull request already has
starts nothing, and applying it again while its run is going cancels that
run. If `main` moves before it merges, bring the branch up to date, squash
and sign it again, and apply the label again. The block sees only verdicts already uploaded:
between running it and the tag's run, don't start another pull request run
of the suite on the same tree (by applying `release-candidate` again), since
the tag's run promotes the newest verdict. Otherwise, run the L3 workflow with
`tag_dry_run` on the commit, which makes the tag's decision without a tag and
runs `l3-full` when there is nothing to promote. A dry run's own suite run is
not a pull request run, so a tag doesn't promote it and the block still
refuses after it: publishing on its verdict, with the tag's run repeating the
suite after publication, is the release coordinator's decision, not this
block's.

The `sed` points each inline relative link in the notes, `](path)`, at the
tagged file, since a GitHub release body can't resolve one relative to the
notes; `](#...)` anchors, absolute `](/...)` paths and URLs stay as they are.
It doesn't handle reference-style links or link titles, so keep release
notes to inline links without titles. Its expressions are delimited by `|`,
which neither the patterns nor the replacements contain, so no `sed` has to
tell a delimiter from a `#` in a bracket expression; keep `|` out of them.

The block runs in a subshell that stops at the first failing command
(`set -e`, and `pipefail` for its `printf | grep` checks), and keeps its
files in a directory of its own from `mktemp -d`, which it removes however
it exits. It has no
comments, which an interactive zsh doesn't recognize by default, no `!`
other than in `!=`, which history expansion would change, and no `$name:`,
which zsh reads as a modifier: it writes `${sha}:VERSION`. It writes
the release notes from the commit it tags before tagging, and refuses blank
ones. The commit it checks is the commit it tags: `"$sha"`, resolved once.

When it stops after creating the tag, which only a failed `git verify-tag`
does, it deletes the tag it created. When it stops at `git push` or after,
check whether the tag is public:
`git ls-remote --tags origin refs/tags/v<version>`. A public `v*` tag can
never be moved or deleted (`release-tags.json` in
[`.github/rulesets/`](../.github/rulesets/)), so it is never recreated. Run
the block again: it finds the tag, requires it to point at the commit, be
the tag `origin` has and verify, checks the commit again, skips `git tag`
and `git push` for a tag that is already there, and creates the release from
the same notes. A tag this clone has that `origin` doesn't, from a push that
failed, it pushes once it verifies. If a check refuses on such a rerun, it
says the tag is public and the release wasn't created: publishing the
release anyway is the release coordinator's decision. If
`gh release view v<version>` already shows a release, inspect it instead.

Pushing a `v*` tag also runs the L3 production-window matrix
(`prism-load-l3.yml`), which promotes the promotion PR's run on the same tree
or runs `l3-full` (#608). The block has checked that it promotes a run that
passed.

Run publication commands only when authorized to publish. If a tag or release
already exists, inspect its target and contents instead of replacing it.
Use notes from the tagged tree, then verify the GitHub release points to that
same tag. Keep the upstream qbit binary pin separate from this repository's
release version.

## Reconcile the development branch

A synchronization PR lands as it is, and the version lines accept only
commits with a verified signature. Sign the merge commit you make for it
(`git merge -S`, and `git commit -S` to finish one that stopped on
conflicts), and never squash it: the required `signed-landing` check judges
the PR by its head commit, which a person has to have signed, and that
signature answers for everything the PR brings. A head commit GitHub made,
such as an "Update branch" merge, fails. Name its branch `sync-…`, in this
repository, with nothing before `sync-` and no `/` (`sync-main-into-3.x.x`,
not `yourname/3.x.x-sync-main` or `sync-3.1.1/main`): `signed-landing` lets a
branch with such a name merge as it is, and expects every other pull request
to be one signed commit. The same goes for any pull request that merges one
version line into another.

After promotion, open a synchronization PR from a branch that merges the
released `main` commit into the latest development branch (`3.x.x` after a 3.x
release, `2.x.x` after 2.0.0), and into a release branch such as `3.0.x` that
will take patch releases. Preserve subsequent development and resolve metadata
and notes deliberately; do not reset or force-push the shared branch. Run its
checks before merging.

Land the synchronization PR with the merge button's merge commit
(`gh pr merge <pr> --merge`), not the squash merge that lands every other pull
request since #877; rebase merges are off. Never squash it into one commit:
that drops `main` from the branch's history. The
next merge between them then falls back to the old merge base and conflicts on
changes only one side has made since this synchronization, and GitHub keeps
counting each `main` commit as one the branch is behind. Two rulesets guard
the version lines, and neither has a bypass list
([`.github/rulesets/`](../.github/rulesets/)). Every commit added to a line
needs a verified signature, so the PR's commits land as they are and must be
signed, the merge of `main` included. That rule also refuses an old unsigned
commit that the branch doesn't have yet, so a synchronization can't bring one
in. And a line other than the dormant `1.x.x` and `0.1.x` takes changes only
through a pull request whose `signed-landing` check passes. Pushing the PR's
head to the line as a fast-forward, which was how earlier synchronizations
could land, is refused. Once its checks and review pass:

```sh
gh pr checks <pr>
gh pr merge <pr> --merge
```

If GitHub reports a conflict because the branch has moved, merge the branch
into the synchronization branch, sign that merge too, and run the checks
again. Other PRs into the branch
are squashed into one signed commit by a maintainer and squash-merged, and a
promotion lands on `main` the same way: GitHub makes one commit on the
branch, with the signed commit's message, and no merge commit. A promotion from `3.x.x` or
`3.0.x` can't be merged as it is: they hold three unsigned commits from
September 2026 (`1bcd61e1`, `72e099bc`, `6ed2250b`), which `main` would
refuse. So the branch reads behind `main` from each promotion until its
synchronization lands, and its ahead count keeps growing.
`git diff origin/main origin/<branch>` shows what really differs. A kept merge
base doesn't make every synchronization clean: a line the branch changed
again after the promotion's cutoff still conflicts with the promotion's copy.
Keep the branch's side unless the promotion edited that line.

For 3.1.0 and 3.0.0 this step merges the released `main` (`b9187551`, #783)
into `3.x.x`, and `main` at `b9579c67` into `3.0.x`. That commit is 3.0.0's
promotion (#778) plus #781, before 3.1.0's, because a 3.0 patch branch must
not take the 3.1 dual writer. Both branches gain this guide, the README banner
and #781's disposition-poll change. `3.0.x` also gains #781's tokio update,
and `3.x.x` the released 3.0.0 notes in place of its pre-release copy. The
next release from either branch must not drop them.

For 3.1.1 this step merges the released `main` back into `3.x.x`. It brings
only the promotion's edits: the release date and the fix PRs' numbers in the
3.1.1 notes, and this guide's 3.1.1 record. `3.0.x` takes nothing from it, since 3.1.1 is a 3.1
release.

#826, the 3.1.1 synchronization, was squashed like the earlier ones, so
`3.x.x`'s merge base with `main` stayed at `v1.1.0`. Merge commit
`b51ac732`, landed by fast-forward as a synchronization then could, corrects that: it
has `main` at `60467262` as a parent and changes no file. It takes its tree
from `3.x.x`, as `git merge -s ours` does, because #826's `d0c649b4` already
had `60467262`'s exact tree. Use `-s ours` only when a commit on the branch
has the `main` commit's exact tree, so that
`git diff <main-commit> <that-commit>` prints nothing. Otherwise it marks
`main`'s changes as merged without taking them, as it would on `3.0.x`, which
doesn't hold 3.1. `3.0.x` and `2.x.x` keep `v1.1.0` as their merge base with
`main` for now.

For 2.0.0 this step is required: PR #246 contains promotion fixes and the
consolidated metadata that were not yet on `2.x.x`. Until this synchronization
lands, the old 2.0.2 metadata on that branch is not a published release.
Bring those fixes and the 2.0.0 release record back before selecting the next
version. Start the next pending notes separately and retain published notes.

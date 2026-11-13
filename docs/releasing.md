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
`doc/release-notes-<version>.md`. `scripts/check_version_skew.py` checks the
first three agree. Ordinary development PRs contribute to
that pending release's notes; they do not each create a patch release.
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
fixes already made during promotion review. The repository allows only merge
commits, and every commit on `main` needs a verified signature. A maintainer
squashes the promotion into one commit, signs it and merges it, so the
promotion lands on `main` as that one signed commit under GitHub's merge
commit. The rulesets are in [`.github/rulesets/`](../.github/rulesets/). Check package versions together,
run Docker lint and the relevant test gates, and review upgrade and rollback
instructions. Leave the release date out while publication is unscheduled;
set the actual release date in a reviewed commit before the final merge.

## Publish the reviewed main commit

Tag the promoted `main` commit unless the release was already tagged on its
release or development branch, as 3.0.0 and 3.1.0 were. After the promotion PR
passes required checks and is merged, identify the exact resulting commit on
`main`: GitHub's merge commit. The maintainer's signed squash is a different
commit from the promotion branch's earlier head, and the merge commit is
another. Verify the resulting commit's checks, version metadata
and dated release notes before tagging it.

After confirming that the tag doesn't already exist, use the reviewed main
commit in place of `<release-main-sha>`, and the version in place of
`<version>` (as `v2.0.0` was; `v3.0.0` and `v3.1.0` are signed annotated
tags on their branches' commits, with their own messages):

```bash
(
  set -e -o pipefail
  notes=$(mktemp)
  trap 'rm -f "$notes"' EXIT
  git fetch origin main --tags
  git tag -s v<version> <release-main-sha> -m 'qbit-mining-bootstrap <version>'
  git verify-tag v<version>
  git show v<version>:doc/release-notes-<version>.md | sed -E \
    -e 's|\]\(\.\./|](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v<version>/|g' \
    -e 's|\]\(([^)#/:][^):]*)\)|](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v<version>/doc/\1)|g' \
    > "$notes"
  grep -q '[^[:space:]]' "$notes" || { echo 'the release notes are empty; not publishing' >&2; exit 1; }
  git push origin refs/tags/v<version>
  gh release create v<version> --verify-tag --title 'qbit-mining-bootstrap <version>' \
    --notes-file "$notes"
)
```

The `sed` points each inline relative link in the notes, `](path)`, at the
tagged file, since a GitHub release body can't resolve one relative to the
notes; `](#...)` anchors, absolute `](/...)` paths and URLs stay as they are.
It doesn't handle reference-style links or link titles, so keep release
notes to inline links without titles. Its expressions are delimited by `|`,
which neither the patterns nor the replacements contain, so no `sed` has to
tell a delimiter from a `#` in a bracket expression; keep `|` out of them.

The block runs in a subshell, in bash or zsh, that stops at the first
failing command, `git show` or `sed` alike (`pipefail`), and refuses notes
that are empty or blank. Creating the tag is part of it, so a tag that
already exists stops it too. The notes are rewritten from the local tag
before it is pushed, so missing or empty notes stop it before anything is
public.

When it stops, first check whether the tag is public:
`git ls-remote --tags origin refs/tags/v<version>`.
- **Not public:** delete the local tag (`git tag -d v<version>`), fix the
  cause, and run the block again. Notes are fixed on `main` through a
  reviewed PR, and the resulting commit is identified and verified as above
  before it is tagged.
- **Public:** the tag is never recreated. Check `gh release view v<version>`;
  if there is no release, run the block again without its `git tag -s` and
  `git push` lines, so the notes are still rewritten and checked.

Pushing a `v*` tag also runs the L3 production-window matrix
(`prism-load-l3.yml`), which promotes the promotion PR's run on the same tree
or runs `l3-full` (#608).

Run publication commands only when authorized to publish. If a tag or release
already exists, inspect its target and contents instead of replacing it.
Use notes from the tagged tree, then verify the GitHub release points to that
same tag. Keep the upstream qbit binary pin separate from this repository's
release version.

## Reconcile the development branch

A synchronization PR lands as it is, and the version lines accept only
commits with a verified signature. Sign the merge commit you make for it
(`git merge -S`), and never squash it. Name its branch `sync-…`, in this
repository, with nothing before `sync-` and no `/` (`sync-main-into-3.x.x`,
not `yourname/3.x.x-sync-main` or `sync-3.1.1/main`): the required
`signed-landing` check lets a branch with such a name merge as it is, and
expects every other pull request to be one signed commit. The same goes for any pull request that merges one version
line into another.

After promotion, open a synchronization PR from a branch that merges the
released `main` commit into the latest development branch (`3.x.x` after a 3.x
release, `2.x.x` after 2.0.0), and into a release branch such as `3.0.x` that
will take patch releases. Preserve subsequent development and resolve metadata
and notes deliberately; do not reset or force-push the shared branch. Run its
checks before merging.

Land the synchronization PR with the merge button, which has made a merge
commit since squash and rebase merges were turned off on 2026-11-10. Never
squash it into one commit: that drops `main` from the branch's history. The
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
are squashed into one signed commit by a maintainer and merged with the
button, and a promotion still lands on `main` as one squashed commit, signed
by a maintainer, under GitHub's merge commit. A promotion from `3.x.x` or
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

# Publishing a release

`main` is the release promotion branch. `3.x.x` is the development branch and
production line for the 3.x series, whose current release is 3.1.0; `2.x.x` is
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

Merge the intended development cutoff into the promotion branch and retain
fixes already made during promotion review. The repository allows only squash
and rebase merges, so the promotion lands on `main` as one squashed commit. Check package versions together,
run Docker lint and the relevant test gates, and review upgrade and rollback
instructions. Leave the release date out while publication is unscheduled;
set the actual release date in a reviewed commit before the final merge.

## Publish the reviewed main commit

Tag the promoted `main` commit unless the release was already tagged on its
release or development branch, as 3.0.0 and 3.1.0 were. After the promotion PR
passes required checks and is merged, identify the exact resulting commit on
`main`. A squash or rebase merge produces a different commit from the
promotion branch head. Verify the resulting commit's checks, version metadata
and dated release notes before tagging it.

After confirming that the tag doesn't already exist, use the reviewed main
commit in place of `<release-main-sha>`, and the version in place of
`<version>` (as `v2.0.0` was; `v3.0.0` and `v3.1.0` are signed annotated
tags on their branches' commits, with their own messages):

```sh
git fetch origin main --tags
git tag -s v<version> <release-main-sha> -m 'qbit-mining-bootstrap <version>'
git verify-tag v<version>
git push origin refs/tags/v<version>
git show v<version>:doc/release-notes-<version>.md > /tmp/qbit-release-notes.md
gh release create v<version> --verify-tag --title 'qbit-mining-bootstrap <version>' \
  --notes-file /tmp/qbit-release-notes.md
```

Pushing a `v*` tag also runs the L3 production-window matrix
(`prism-load-l3.yml`), which promotes the promotion PR's run on the same tree
or runs `l3-full` (#608).

Run publication commands only when authorized to publish. If a tag or release
already exists, inspect its target and contents instead of replacing it.
Use notes from the tagged tree, then verify the GitHub release points to that
same tag. Keep the upstream qbit binary pin separate from this repository's
release version.

## Reconcile the development branch

After promotion, open a synchronization PR from a branch that merges the
released `main` commit into the latest development branch (`3.x.x` after a 3.x
release, `2.x.x` after 2.0.0), and into a release branch such as `3.0.x` that
will take patch releases. Preserve subsequent development and resolve metadata
and notes deliberately; do not reset or force-push the shared branch. Run its
checks before merging.

For 3.1.0 and 3.0.0 this step merges the released `main` (`b9187551`, #783)
into `3.x.x`, and `main` at `b9579c67` into `3.0.x`. That commit is 3.0.0's
promotion (#778) plus #781, before 3.1.0's, because a 3.0 patch branch must
not take the 3.1 dual writer. Both branches gain this guide, the README banner
and #781's disposition-poll change. `3.0.x` also gains #781's tokio update,
and `3.x.x` the released 3.0.0 notes in place of its pre-release copy. The
next release from either branch must not drop them.

For 2.0.0 this step is required: PR #246 contains promotion fixes and the
consolidated metadata that were not yet on `2.x.x`. Until this synchronization
lands, the old 2.0.2 metadata on that branch is not a published release.
Bring those fixes and the 2.0.0 release record back before selecting the next
version. Start the next pending notes separately and retain published notes.

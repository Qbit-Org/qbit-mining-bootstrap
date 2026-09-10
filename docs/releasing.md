# Publishing a release

`main` is the release promotion branch; `2.x.x` is the development branch for
the 2.x series. A version is published only when its tag and GitHub release
exist. A version bump or merged preparation PR alone does not publish it.

## Prepare one release candidate

Keep one pending release version across `VERSION`, the `qbit-prism` and
`qbit-pool-builder` package versions in their `Cargo.toml` files and `Cargo.lock`,
and `doc/release-notes-<version>.md`. Ordinary development PRs contribute to
that pending release's notes; they do not each create a patch release.
Choose the next version after reconciling the last published release and
any identifiers already distributed to operators.

For the first 2.x release, PR #246 is the single **2.0.0** promotion into
`main`. It includes the changes temporarily labelled 2.0.1 and 2.0.2 on
`2.x.x`; neither identifier was tagged or published as a GitHub release.
Their changes and migration requirements are consolidated in
[the 2.0.0 notes](../doc/release-notes-2.0.0.md). Do not create retrospective
2.0.1 or 2.0.2 tags for those preparation commits.

Merge the intended development cutoff into the promotion branch and retain
fixes already made during promotion review. Check package versions together,
run Docker lint and the relevant test gates, and review upgrade and rollback
instructions. Keep `Release date: pending` until publication is scheduled;
set the actual release date in a reviewed commit before the final merge.

## Publish the reviewed main commit

After the promotion PR passes required checks and is merged, identify the
exact resulting commit on `main`. A squash or rebase merge produces a different
commit from the promotion branch head. Verify the resulting commit's checks,
version metadata and dated release notes before tagging it.

For 2.0.0, after confirming that `v2.0.0` does not already exist, use the
reviewed main commit in place of `<release-main-sha>`:

```sh
git fetch origin main --tags
git tag -s v2.0.0 <release-main-sha> -m 'qbit-mining-bootstrap 2.0.0'
git verify-tag v2.0.0
git push origin refs/tags/v2.0.0
git show v2.0.0:doc/release-notes-2.0.0.md > /tmp/qbit-release-2.0.0-notes.md
gh release create v2.0.0 --verify-tag --title 'qbit-mining-bootstrap 2.0.0' \
  --notes-file /tmp/qbit-release-2.0.0-notes.md
```

Run publication commands only when authorized to publish. If a tag or release
already exists, inspect its target and contents instead of replacing it.
Use notes from the tagged tree, then verify the GitHub release points to that
same tag. Keep the upstream qbit binary pin separate from this repository's
release version.

## Reconcile the development branch

After promotion, open a synchronization PR from a branch that merges the
released `main` commit into the latest `2.x.x`. Preserve subsequent development
and resolve metadata and notes deliberately; do not reset or force-push the
shared branch. Run its checks before merging.

For 2.0.0 this step is required: PR #246 contains promotion fixes and the
consolidated metadata that were not yet on `2.x.x`. Until this synchronization
lands, the old 2.0.2 metadata on that branch is not a published release.
Bring those fixes and the 2.0.0 release record back before selecting the next
version. Start the next pending notes separately and retain published notes.

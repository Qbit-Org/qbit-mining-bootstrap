# Repository rulesets

These files are this repository's GitHub rulesets, as they are live. GitHub
doesn't read them. After a pull request that changes one of them merges, a
repository admin applies it with `gh` from the repository root.
[`../repository-settings.json`](../repository-settings.json) holds the
merge-method setting in the same way.

| File | Enforces |
| --- | --- |
| `signed-commits.json` | A verified signature on every commit added to `main` and the version lines, `[0-9]*.*.x`, such as `3.x.x` and `3.0.x`. No bypass actors. |
| `required-checks.json` | `main`: the `signed-landing` check must pass. No bypass actors. |
| `required-checks-version-lines.json` | The version lines except the dormant `1.x.x` and `0.1.x`: pull requests only, with no approval required, squash merges and merge commits only, and the `signed-landing` check must pass. No bypass actors. Not enforced when the branch is created. |
| `public-main-minimal.json` | `main`: pull requests with one approval and resolved review threads, squash merges and merge commits only; no deletion or force push. Organization admins can bypass it when merging a pull request. |
| `version-lines.json` | Every version line, the dormant `1.x.x` and `0.1.x` included: no deletion or force push. No bypass actors. |
| `release-tags.json` | `v*` tags can't be moved or deleted. No bypass actors. |

A version line takes changes through pull requests only, with no approval
required. The pull request is required because the check judges a commit
against the branch its pull request targets: a commit that passed for one
branch says nothing about another. A version line has no rule against
deletion, and these rules aren't enforced when a branch is created: whoever
can push can delete a line and create it again at any commit. A
synchronization pull request therefore lands with the merge button: pushing
its head to the line, as a fast-forward, is refused. The dormant lines `1.x.x`
and `0.1.x` are left out of both rules; the check still runs on pull requests
into them, from `main`'s copy. The nightly job's `ci-evidence` branch is
outside these rules: a workflow makes its commits, and they aren't signed. So
are branches that match no pattern here, such as `mainnet-deployed`, `0.1.1`
and `release/3.0.0`.

Every commit that a push or merge adds to `main` and the version lines needs a
verified signature. Commits already on the branch aren't checked again, but an
old unsigned commit that a push or merge brings into an existing branch from
another one is refused like a new one. Commits on a pull request's branch
don't need one while it is open. Before it merges, a maintainer rewrites the
branch as one commit, signs that commit with their own key and pushes it. For
a pull request from a fork, that push needs "Allow edits by maintainers".
Without it, a maintainer opens the same change from a branch in this
repository. Once `signed-landing` passes, the pull request is squash-merged:
GitHub makes one commit on the target branch, carrying the signed commit's
message, and signs it itself. So the target branch gains that one commit, and
no merge commit. The commit the maintainer signed stays as the pull request's
head, which GitHub keeps as `refs/pull/<number>/head`; when
`squash-sign merge` makes the merge, the landed commit's `Signed-commit:` line
names it. A synchronization pull request, which merges `main` into a version
line, is the exception: it lands as it is, with a merge commit, so its commits
must already be signed ([docs/releasing.md](../../docs/releasing.md)). A
promotion into `main` can't land that way: `3.x.x` and `3.0.x` hold three
unsigned commits from September 2026, which `main` would refuse, so a
promotion is always squashed into one signed commit.

On `main`, the maintainer's push dismisses earlier approvals, and whoever
pushed can't give the new one. Someone else approves again, or an organization
admin merges the pull request through the review ruleset's bypass. That bypass
works only for merging a pull request, and it covers the review rules only:
nobody can bypass the required checks. CI is not a required check here, so
wait for it to pass on the signed commit before merging.

[`../workflows/signed-landing.yml`](../workflows/signed-landing.yml) reports
the `signed-landing` check on every pull request. It passes when the pull
request is one commit that a person signed with an OpenPGP key that is on
their own GitHub account, and fails until then: a red `signed-landing` on an
open pull request usually means it hasn't been squashed and signed yet. If its
message says it could not check, do what the message says, which is usually to
re-run it. GitHub's signature rule can't tell that by itself, because it
accepts any verified signature, GitHub's own included, such as on a web edit
or an "Update branch" merge. A pull request from a branch of this repository
whose name is one path segment starting with `sync-`, so nothing before
`sync-` and no `/` (`AS_IS_HEADS` in the workflow), merges as it is, with any
number of commits: name a branch that merges one version line into another
that way. The check judges its head commit instead: it passes when a person
signed that commit with an OpenPGP key that is on their own GitHub account.
The signature covers the commit's tree and its parents, so whoever signed the
head answers for everything the pull request brings. A head commit that GitHub
made fails, such as an "Update branch" merge or a web conflict resolution, so
the last commit of a `sync-` branch is the merge commit a maintainer makes and
signs. A fork's branch never merges as it is.
The check is required by a ruleset with no bypass actors, so the merge button
works only once it is green. It always runs from the default branch's copy of
the workflow, `main`'s, whatever branch the pull request targets, so a pull
request can't change the rule it is judged by. Its result belongs to the
commit, so it passes only if every open pull request that the commit heads may
land, drafts aside and whatever branch each comes from: a second pull request
that takes the same commit into another branch, where it is several commits,
fails both until it is closed or turned into a draft, either of which runs the
check again, or is made ready too and the check re-run. A run starts a few
seconds after its event, and until then the commit keeps the result it had. A
change that a workflow makes with its own token starts no run at all. It
guards against landing a pull request by mistake, not against someone set on
getting round it: a pull request could still add a workflow of its own that
reports a check of the same name. Review catches that where a branch requires
review, and the maintainers audit merged pull requests daily, from outside
this repository, and report anything that got through.

If the check stops reporting, no pull request can merge into a branch that
requires it, and that includes the pull request that fixes the workflow,
because it too is judged by `main`'s copy. GitHub blocks `pull_request_target`
workflows in public repositories unless an Actions policy allows them. If that
policy names this file by its path, renaming the file stops the check. To get
out, a repository admin disables `required-checks` and
`required-checks-version-lines` on GitHub (Settings → Rules → Rulesets),
merges the fix, and applies the files again. Each open pull request then needs
a push or an edit to be judged by the fixed copy. The same way out serves when
another required check can't run. A run that stays queued, as can happen after
an Actions incident, leaves the commit's last result in place too: cancel it,
and edit the pull request's description to start another. The check reads
GitHub with the workflow's token, and every run in the repository draws on
that token's hourly allowance of requests. A flood of pull request events can
use it up, and the check then fails for every pull request until the hour is
over.

The repository's settings allow squash merges and merge commits (Settings →
General → Pull Requests). An ordinary pull request is squash-merged. GitHub
signs the commit its squash merge makes, so GitHub's own signature satisfies
the signature rule on the branch, and what requires a person's signature is
the `signed-landing` check, which a ruleset nobody can bypass requires. A pull
request from a `sync-` branch lands as it is, with a merge commit, because
squashing it would lose its history. GitHub's rebase merge makes commits with
no signature, which the rule refuses.

## Apply a change

Define `repo` and `apply` first. This block changes nothing on GitHub:

```bash
repo=repos/Qbit-Org/qbit-mining-bootstrap
apply() {
  local name id
  name=$(jq -er 'select(.rules and .conditions) | .name' "$1") || { echo "$1: not a ruleset file"; return 1; }
  id=$(gh api "$repo/rulesets?includes_parents=false&per_page=100" --jq ".[] | select(.name == \"$name\") | .id") || return
  if [ -n "$id" ]; then
    gh api -X PUT "$repo/rulesets/$id" --input "$1" --jq '"updated \(.name) (\(.id))"'
  else
    gh api -X POST "$repo/rulesets" --input "$1" --jq '"created \(.name) (\(.id))"'
  fi
}
```

Don't apply a file that requires `signed-landing` until `signed-landing.yml`
is on `main` and the Actions policy allows it: until then nothing reports the
check, and no pull request into a branch that requires it can merge.

Then run `apply` once for each ruleset file that changed. It updates the
ruleset of that name, or creates it when GitHub has none. For example:

```bash
apply .github/rulesets/public-main-minimal.json
```

A ruleset is found by its name. To rename one, rename it on GitHub first, then
apply the file. When a required check moves from one ruleset to another, apply
the one that gains it first.

`apply` is for ruleset files. Apply a change to `repository-settings.json`
with:

```bash
gh api -X PATCH "$repo" --input .github/repository-settings.json --silent
```

The merge-method setting isn't a ruleset. To set a repository up from these
files, apply `signed-commits.json` first, allow merge commits, apply the other
files, then allow squash merges and turn rebase merges off. No step leaves the
repository unable to merge, and unsigned commits can't land after the first
one. The chain stops at the first step that fails. Fix that step and run the
chain again: applying a file twice is harmless. Run it only once
`signed-landing.yml` is on `main`, the default branch, and the Actions policy
allows it. A pull request that is already open shows the check as expected
until its next push or edit: edit its title or description to start a run.

```bash
apply .github/rulesets/signed-commits.json &&
  gh api -X PATCH "$repo" -F allow_merge_commit=true --silent &&
  apply .github/rulesets/required-checks.json &&
  apply .github/rulesets/required-checks-version-lines.json &&
  apply .github/rulesets/public-main-minimal.json &&
  apply .github/rulesets/version-lines.json &&
  apply .github/rulesets/release-tags.json &&
  gh api -X PATCH "$repo" --input .github/repository-settings.json --silent
```

## Check that GitHub matches

Run this as a repository admin. GitHub leaves the bypass actors out for anyone
else, and the check then says so instead of comparing.

```bash
repo=repos/Qbit-Org/qbit-mining-bootstrap
norm='{name, target, enforcement, conditions, rules, bypass_actors} | walk(if type == "array" then sort else . end)'
if live=$(gh api "$repo/rulesets?includes_parents=false&per_page=100" --jq '.[] | "\(.id)\t\(.name)"'); then
  names=$(jq -r 'select(.rules and .conditions) | .name' .github/rulesets/*.json)
  for f in .github/rulesets/*.json; do
    name=$(jq -er 'select(.rules and .conditions) | .name' "$f") || { echo "$f: not a ruleset file"; continue; }
    id=$(awk -F '\t' -v name="$name" '$2 == name { print $1 }' <<<"$live")
    if [ -z "$id" ]; then
      echo "$name: not on GitHub"
    elif ! got=$(gh api "$repo/rulesets/$id"); then
      echo "$name: could not be read"
    elif ! jq -e '.bypass_actors != null' <<<"$got" >/dev/null; then
      echo "$name: GitHub left out the bypass actors, so run this as a repository admin"
    elif ! want=$(jq -S "$norm" "$f") || ! have=$(jq -S "$norm" <<<"$got"); then
      echo "$name: could not be compared"
    elif [ "$want" = "$have" ]; then
      echo "$name: matches"
    else
      diff <(echo "$want") <(echo "$have")
      echo "$name: differs (lines with < are the file's, > are GitHub's)"
    fi
  done
  while IFS=$'\t' read -r id name; do
    [ -z "$name" ] || grep -qxF "$name" <<<"$names" || echo "$name: on GitHub, but has no file"
  done <<<"$live"
  if ! want=$(jq -S . .github/repository-settings.json) || ! got=$(gh api "$repo") || ! have=$(jq -S '{allow_merge_commit, allow_rebase_merge, allow_squash_merge}' <<<"$got"); then
    echo "merge settings: could not be compared"
  elif [ "$want" = "$have" ]; then
    echo "merge settings: match"
  else
    diff <(echo "$want") <(echo "$have")
    echo "merge settings: differ (lines with < are the file's, > are GitHub's)"
  fi
else
  echo "could not list the rulesets on GitHub"
fi
```

Every line should say `matches` or `match`. `includes_parents=false` leaves
out organization rulesets, which these files don't hold.

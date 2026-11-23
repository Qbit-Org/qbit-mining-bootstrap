# Skipping CI for a tree that already passed

A pull request lands as one signed commit with exactly the tree its review
settled: `squash-sign apply` squashes the branch, signs it and force-pushes it.
GitHub then runs every workflow again on that commit, although nothing in the
tree changed. `ci.yml` and `prism-load-l3.yml` skip that repeated work with a
**pass marker**, and every check they report still passes. Anything else (a
real change, a moved base, a push, a tag) runs everything as before.

The decisions are in `scripts/tree_pass_marker.py`; its tests,
`tests/test_tree_pass_marker.py`, also hold the workflow wiring below to what
the decisions assume.

## The markers

A **pass marker** is an entry in GitHub's Actions cache whose key names the
tree and the run that passed it:

```text
tree-pass-v1-<workflow file>-salt<N>-<tree>-<run id>-<run attempt>
```

- `<tree>` is the tree of the commit the workflow tests. For a `pull_request`
  run that commit is `GITHUB_SHA`, the merge of the head into the base, so a
  different head tree or a moved base is a different key. It is never a commit
  id: the signed squash is a new commit with the same tree.
- `<workflow file>` is `ci.yml` or `prism-load-l3.yml`, read from
  `GITHUB_WORKFLOW_REF`, so one workflow's marker never skips another.
- `<N>` is the repository variable `TREE_PASS_SALT` (the salt, below).
- `<run id>-<run attempt>` is the run that saved it. The run also uploads an
  artifact named `tree-pass-<sha256 of the key>` holding `marker.json`; the
  name binds the artifact to exactly this key.

A **fail marker**, `tree-fail-v1-<workflow file>-salt<N>-<tree>`, records that
a full `ci.yml` run of the tree failed.

Each run goes through four steps:

1. **Look up.** A first job (`tree-marker` in `ci.yml`, the plan job in L3)
   computes the keys and asks the cache for the newest pass marker under the
   prefix `...-<tree>-`, and for the fail marker, with
   `actions/cache/restore@v6` and `lookup-only: true`.
2. **Decide.** The run skips only when there is no fail marker and the run the
   matched key names is one GitHub's own records vouch for, in this pull
   request alone, created after it, with no base change and no deletion or
   restore of its head branch recorded on it (below). A miss, an expired or evicted
   entry, a cache service error, a missing permission, an API error, an
   incomplete listing, a step that failed or timed out, a checkout that
   failed, or an unreadable input all mean "run everything".
3. **Save.** A last job saves the pass marker only when every job the marker
   stands for ran and succeeded in this run, the marker did not skip this run,
   and the run is a same-repository `pull_request` run. It recomputes the
   prefix from its own checkout's tree and refuses one that is not this
   tree's, uploads the artifact, and saves the key only if the upload
   succeeded. A save that fails only means the next run runs everything.
4. **Record a failure.** `tree-marker-fail` runs when a same-repository pull
   request run that the marker did not skip has a job that reports `failure`
   or `cancelled`, and the run itself was not cancelled; that includes a run
   whose marker job failed or never finished. It saves the fail marker when
   any job the pass marker stands for (one `jq` over `needs`, the jobs listed,
   the marker job not among them) reports `failure` or `cancelled`. It runs
   under `!cancelled()`, so a job reporting `cancelled` there is one GitHub
   cancelled at its `timeout-minutes` (run 37689470607's PostgreSQL shard 1,
   at 45 minutes, reported exactly that). The key is the marker job's when
   that job succeeded and left a valid one, and is otherwise computed from the
   fail job's own checkout, so a failed checkout costs nothing in the usual
   case. It records the failure twice, in two stores: first an artifact
   `tree-fail-<sha256 of the fail key>` (kept 90 days, never overwritten), then
   the fail marker in the cache. Each save has its own three-minute timeout and
   continues on error, so a save that fails or stalls never stops the other,
   and a re-run never deletes an earlier attempt's artifact: its upload of the
   same name fails and leaves that record in place. One failed save therefore
   never loses the failure; the exceptions are under "Runs that do not record
   a failure".

**A marker is never restored.** `actions/cache` extracts an entry with
`tar -xf <archive> -P -C <workspace>`, which honours absolute and `../`
paths, and anyone who can push to the pull request can save an entry from a
changed workflow. Restoring one would let it write files into the job, the
marker script included, before the decision. So the lookups only ask whether
an entry exists, the run is read from the matched key, and nothing from the
cache is ever extracted.

### What GitHub must vouch for

For the run `<run id>` the matched key names, `decide` asks GitHub's REST API
and skips only when all of these hold:

- it is not this run;
- it is a run of this repository, of this workflow file, on `pull_request`,
  from the same head repository and head branch as this pull request, and its
  head commit has the tree of this pull request's head commit (so it ran the
  workflow under review);
- it was created after this pull request: the run record's `created_at` is
  later than the pull request's own `created_at`, and the same second refuses
  (of the 50 most recent closed pull requests here, checked on 2026-11-12, none
  had a run less than 3 s after its creation, so the same second only ever helps a
  script racing the pull request). Both must be GitHub's
  `YYYY-MM-DDTHH:MM:SSZ`, or the run runs everything. The run record keeps its
  first creation through every re-run (run 41093895738 reads 16:53:07 on the
  run record and 17:24:09 on its attempt 2), and only the run record's
  `created_at` is read, never its `run_started_at` or `updated_at`, which a
  re-run moves, nor the pull request's `updated_at`;
- `GET /repos/{repo}/actions/runs/{id}/artifacts?name=tree-pass-<sha256 of the
  key>` returns exactly one artifact, of that run, not expired;
- the jobs of the attempt that saved (`/runs/{id}/attempts/{n}/jobs`) show
  `Record this tree's pass marker` and `Lint, compile, and test` (in L3, the
  collation) concluded success;
- the latest execution of `Lint, compile, and test` (or the collation) in that
  run concluded success, so a re-run of it that failed stops the skip;
- `GET /repos/{repo}/pulls?head=<owner>:<head branch>&state=all&per_page=2`
  (one request for at most two) lists exactly one pull request, open, closed
  or merged, and it is this one; any other count refuses. The listing keeps a
  closed pull request after its head branch is deleted, and after a branch of
  the same name is created again (checked read-only on 2026-11-11: merged
  Qbit-Org/qbit-mining-bootstrap #838, #837, #783 and #782, whose branches
  are gone, are each still listed for their head; Qbit-Org/qbit-wallet's
  `changeset-release/main`, deleted after #104 and #132 merged and created
  again for the open #168, lists #168, #132 and #104);
- this pull request's timeline (`GET /repos/{repo}/issues/{n}/timeline`) and
  issue events (`/issues/{n}/events`) have no event whose name starts with
  `base_ref_` or `automatic_base_change_`, and no `head_ref_deleted` or
  `head_ref_restored` (below);
- in `ci.yml`, `GET /repos/{repo}/actions/caches?key=<fail key>` lists no entry
  with exactly the fail key, and `GET /repos/{repo}/actions/artifacts?name=tree-fail-<sha256 of the fail key>`
  lists no artifact of that name that is not expired. The fail lookup step
  cannot say the first: the cache action reports a cache service error as a
  miss. The second catches a failed run whose cache save failed.

It judges by those jobs, not the run's conclusion: the advisory deleted-test
map job can turn a run red without saying anything about the tree. The
timeline, the issue events, the cache list and the fail-record listing are
read 100 at a time until an empty page (at most ten non-empty pages); one that
cannot be read to its end runs everything.

The pull request checks (the listing of pull requests from the head branch,
the creation time, and the base-change and head-branch events) bind the run
to this pull request and its base. A `pull_request` run takes its workflow
file from the merge commit, and the workflow has no say over which base that
is. Without them, someone with write
access could retarget this pull request onto a branch that carries a changed
workflow, let that workflow run once on the reviewed head, upload the artifact
named for the real key and save the key in this pull request's cache scope
(the same pull request, so the same `refs/pull/N/merge`) with green jobs of
the right names, and retarget back: that run would pass every other check.

A second pull request from the same head branch is a narrower case. Its runs
save cache entries only under its own `refs/pull/M/merge`, so on its own it
cannot place a marker here; it would take a second step, a push of a changed
workflow to this pull request to save an entry naming that run. The
head-branch listing stands against all of it: a branch that any other pull
request used refuses. Two more checks close parts of it without the listing,
and they bind by creation order and by recorded events, not by which pull
request a run belongs to:

- the creation time covers every run created before this pull request was
  opened: a run of a pull request opened earlier from the same branch
  (closed, its branch deleted and created again for this one) cannot vouch
  here even if the listing stopped returning that earlier pull request;
- the head-branch events cover two pull requests open at once from one
  branch, that branch then deleted (which closes both) and created again, and
  this one reopened: the deletion is recorded on this one as
  `head_ref_deleted`, which refuses.

Neither covers a run of another pull request from the same branch created
after this one was opened: when that pull request is reopened (its branch
never deleted while this one existed), when two pull requests are open at once
from one branch into different bases and a push creates a run for the other,
or, in L3, whose triggers include `labeled`, when a closed earlier pull
request is labelled (whether GitHub starts a run for a closed pull request's
label is unverified). Against those only the listing stands.

So **a head branch that any other pull request has used, open, closed or
merged, always runs in full**, and so does **a pull request whose timeline or
issue events show any of these events**:

- `base_ref_changed`: retargeted by hand;
- `automatic_base_change_succeeded`: GitHub retargeted it by itself after its
  base branch was merged and deleted. That leaves no `base_ref_changed`
  (Qbit-Org/qbit-js#2; qbit-wallet#136 shows an automatic and a manual
  retarget as separate entries);
- `automatic_base_change_failed`: GitHub tried to retarget it and could not.
  The base did not change, but the check refuses it too rather than reason
  about what GitHub left behind;
- `base_ref_force_pushed`: its base branch was force-pushed. That is routine on
  some branches here, so a pull request into such a branch stops skipping at
  the first force-push and never skips again;
- `base_ref_deleted`: its base branch was deleted;
- any other event whose name starts with `base_ref_` or
  `automatic_base_change_`: the check matches the prefixes, not a list;
- `head_ref_deleted`: its head branch was deleted while it existed, so the
  branch may have been created again with another pull request's runs on it;
- `head_ref_restored`: its head branch was restored after such a deletion.

The two head-branch events are matched by exact name: `head_ref_force_pushed`
is routine and does not count. They do not turn skipping off for ordinary pull
requests: checked on 2026-11-12, 94 of this repository's 530 pull requests
(19 of Qbit-Org/qbit's 199) have `head_ref_deleted`, every one recorded after
the pull request merged or closed, and none has `head_ref_restored`; no
pull request that landed, and no open one, had either while it was open. A
branch deleted after the merge is recorded after the pull request closed,
when there is nothing left to skip. Merged #838 shows `head_ref_deleted` in
both the timeline and the issue events.

GitHub's [issue event types](https://docs.github.com/en/rest/using-the-rest-api/issue-event-types)
page lists `base_ref_changed`, `automatic_base_change_succeeded` and
`automatic_base_change_failed` for the issue events API and marks them not
supported for the timeline, and does not list `base_ref_force_pushed` or
`base_ref_deleted` at all. The timeline returns all of them in this
organization today, and so do the issue events (both listings of
qbit-wallet#136 hold the same four entries). So `decide` reads both
(`/issues/{n}/timeline` and `/issues/{n}/events`) and refuses on a match in
either: if one listing ever follows the docs and drops an event, the other
still shows it. (GraphQL's `timelineItems(itemTypes: [...])` would be typed and
documented, but only for the types named in the query; a new kind of base
change would pass it unseen, where the prefix match refuses it.)

The run record's own `pull_requests` cannot do this: it shows the pull
requests' current state and is empty for closed ones.

The lookup and save jobs run on GitHub-hosted `ubuntu-24.04` runners, not
Blacksmith's. On a Blacksmith runner `actions/cache` goes to Blacksmith's own
cache ([Blacksmith's caching docs](https://docs.blacksmith.sh/blacksmith-caching/dependencies-actions):
"Zero code changes are required"), whose branch scoping can be turned off in a
dashboard this repository cannot see; the scoping below is GitHub's, which
GitHub documents. GitHub's cache for this repository held no entries on
2026-11-10, so every existing cache here is Blacksmith's.

## What a skipped `ci.yml` run reports

| Check | In a run the marker skipped |
|---|---|
| Look up this tree's pass marker | success |
| Lint, compile, and Compose validation; Rust tests; Rust formatting; Rust lints; Dependency audit; Dockerfile lint; Rust PRISM dual-writer scenarios (fast lane); Prove every Rust PRISM gated test executed; ckpool rejects observability live suite; Build lightweight Docker images; Build and smoke real qbitd image | skipped (a job-level `if`) |
| Python unit tests (shard 0 to 3); Rust PRISM PostgreSQL and multi-instance contracts (shard 0 to 3) | success, on a GitHub-hosted runner, every step skipped and no PostgreSQL service started |
| Lint, compile, and test | success |
| Deleted-test map names only open issues as gap owners (advisory) | runs as always |
| Record this tree's pass marker; Record that this tree failed | skipped |

Every skip condition is the same test: the marker job **succeeded** and wrote
`skip=true`. A marker job that wrote `skip=true` and then failed skips nothing.
On a push, a dispatch or a fork's pull request the marker job is skipped and
every job runs.

GitHub counts a skipped job as passing for a required check: "A job that is
skipped will report its status as "Success". It will not prevent a pull
request from merging, even if it is a required check." But a job-level `if`
"is evaluated before `jobs.<job_id>.strategy.matrix` is applied", so a matrix
job skipped that way reports one check under its unexpanded name, and a
required `Python unit tests (shard 0)` would never report. The two matrix jobs
therefore run and skip each step instead, on a GitHub-hosted runner rather than
a Blacksmith one; the PostgreSQL service's image is empty, and "If
`jobs.<job_id>.services.<service_id>.image` is assigned an empty string, the
service will not start."

The aggregate check, `Lint, compile, and test`, is one `jq -e` over
`toJSON(needs)` with no checkout. It passes a marker skip when the marker job
succeeded with `skip=true`, only the job-level skipped jobs above report
skipped, and every other job it needs succeeded. A failure, a cancellation or
any other skip fails it. When the marker did not skip the run, it requires
every job to succeed, as it always has. The proof job reports skipped rather
than proving anything, because the shards uploaded no gate manifest in that
run: the proof is the run the marker names.

`squash-sign merge` reads each required context's check run on the head
commit and accepts `success`, `neutral` or `skipped`, so it sees the same
result. No ruleset requires a status check today (`.github/rulesets/`).

The marker does not stand for the advisory deleted-test-map job: it reads
issue state, not the tree, so it never skips and its failure records no fail
marker.

## L3

`prism-load-l3.yml` keys its marker on the plan job's tree, the tree its
verdict artifact (`prism-l3-verdict-<tree>`) and a tag's promotion
(`scripts/prism_l3_promote.py`) name. Its plan job runs on a GitHub-hosted
runner for a pull request and on Blacksmith's otherwise. It skips the build,
the preset jobs and the collation, and only:

- on a `pull_request` push (`opened`, `synchronize`, `reopened`) from this
  repository. A `labeled` event (`release-candidate`) asked for its run, so
  it runs, and so does every tag, weekly run and dispatch;
- when the plan's pinned commit is `GITHUB_SHA`. The plan checks
  `refs/pull/N/merge` out again, and if it moved after the event the workflow
  file running is not the one in the tree under test;
- when GitHub vouches for the run the marker names, as above, with the
  collation in place of `Lint, compile, and test`, in this pull request alone,
  created after it, with no base change and no head-branch deletion or restore
  on its timeline or issue events;
- when `scripts/prism_l3_promote.py` says a `v*` tag on this tree would
  promote **exactly that run**, and that it passed every gate. The promotion
  accepts any same-repository pull request run's verdict for the tree, so a
  verdict uploaded by another run (another pull request's, or a changed
  workflow's) never carries a skip;
- when that promotion passed over no newer run. A tag's promotion reads one
  page of up to 100 artifacts named `prism-l3-verdict-<tree>` (the API has
  listed the newest first whenever that was checked; GitHub does not document
  the order), drops the expired ones and any that name no run, and
  walks the rest newest first. It records every run it passes over before the
  one it promotes under `considered` and promotes that one. That includes
  this run's own artifacts (a verdict an earlier attempt uploaded, after a
  re-run), which are never promoted and never looked up. The skip refuses if
  `considered` lists any run, or is missing, is not a list or holds an entry
  that is not an object. A listed run is one whose record or verdict could not
  be read (an API error, an unreadable `verdict.json`), one unfinished,
  cancelled or timed out, a verdict that is incomplete (a preset crashed
  before its gate) or for another suite or tree, a run that was not a
  same-repository `pull_request` run, or this run. A tag still promotes the
  older run in those cases; the skip does not. The skip also refuses when a
  listed artifact is not well-formed (no integer run id, an `expired` that is
  not a boolean, no `created_at`), which a tag drops, ranks oldest, or walks
  without being able to trust: the
  decision describes those under `malformed`, and a missing `malformed` list
  refuses too. A newer complete run that
  failed is the run a tag would promote, so it refuses the skip too, and so do
  an expired verdict of the marker's run and a failed artifact listing. A run
  that had uploaded no verdict when the skip decided is seen by neither: one
  whose build failed, whose collation could not read its inputs, or that was
  cancelled before its collation never uploads one, and one still in progress
  uploads it later (below). L3 has no fail marker.

For a pull request, the plan job's first step after its checkout compares
`git rev-parse HEAD` with `GITHUB_SHA` in the workflow itself, before any
script from that checkout runs (the suite selection included). Both the marker
lookup and the decision run only if it passed, so code from a merge ref that
moved after the event never judges itself; without a decision `skip` is unset
and the suite runs. Other events skip this check and look no marker up.

Its marker is saved only when the plan, the build, every preset job and the
collation succeeded and the collation uploaded a passing verdict for the
tree. A skipped L3 run uploads no verdict, so a tag still promotes the run that
measured the tree. L3 is never a required check; its skipped preset matrix
reports one check under `${{ matrix.id }}`, as it does on every pull request
that does not change `VERSION`.

Every marker step, in both workflows, has a timeout of 1 to 3 minutes and
continues on error, so a slow API never fails the marker job or the plan (the
plan's own timeout is 20 minutes, the marker steps' add up to 11).

`prism-load-nightly.yml` and `prism-load-runner-probe.yml` never skip.

## Forcing a full run

- **The `run-full-ci` label**, on one pull request: apply it, then push or
  choose **Re-run all jobs**. The marker job reads the pull request's labels
  when it runs, so a re-run sees a label applied since. **Re-run failed jobs**
  does not: it keeps the marker job's earlier result. The label must exist
  once in the repository:
  `gh label create run-full-ci --description "Run all CI even when this tree already passed"`.
  A forced run that passes saves its own marker; one that fails saves the
  fail marker, and the tree never skips again in that pull request.
- **The salt**, for every pull request, without a commit: set the repository
  variable, for example
  `gh variable set TREE_PASS_SALT --body 2 -R Qbit-Org/qbit-mining-bootstrap`.
  Unset means 1; a value that is not a non-negative integer runs everything
  and saves nothing. Every key changes, so no marker hits. Bump it when
  something outside the tree changed that CI depends on: a runner image, a
  moved action tag (`actions/checkout@v7` and the like are tags, not pins), an
  upstream download, or a flaky pass you no longer trust.

It never skips:

- on any event but `pull_request`: pushes to the protected branches, tags,
  `merge_group`, schedules and dispatches run in full and save nothing;
- for a fork's pull request, which also saves nothing;
- when the tree changed, which includes any change to the workflows, the
  composite actions under `.github/actions/`, the scripts or this mechanism;
- when the base moved, which changes the merge result's tree;
- after a full `ci.yml` run of the tree failed in this pull request, once that
  run has finished: `ci.yml` has no concurrency group, so a run of the same
  tree that decides while the failing run is still going can skip on an older
  pass;
- when another pull request, open, closed or merged, ever had the same head
  branch, or when this pull request's timeline or issue events show its base
  retargeted (by hand or by GitHub, or a retarget GitHub could not make),
  force-pushed or deleted, or its head branch deleted or restored;
- when the run the marker names was created before this pull request or in
  the same second;
- once the cache evicts the marker (GitHub removes entries not accessed in
  over 7 days, and the oldest first once the repository's cache is full) or
  its artifact expires (30 days).

## What the marker relies on

From GitHub's [dependency caching reference](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching):

- "When a cache is created by a workflow run triggered on a pull request, the
  cache is created for the merge ref (`refs/pull/.../merge`)." ... "It cannot
  be restored by the base branch or other pull requests targeting that base
  branch." Another pull request or a protected branch's run never sees this
  pull request's markers, and this mechanism never looks one up on a push.
- A pull request's run can restore caches of "the current branch or the
  default branch", and of its base branch. The workflows save markers only
  from `pull_request` runs, so those scopes hold none of theirs; any entry
  found there still has to pass the checks above.
- "You cannot change the contents of an existing cache." A marker is never
  overwritten, so a run that finishes late writes only its own tree's key.
- A workflow run "will use the version of the workflow that is present in the
  associated commit SHA or Git ref of the event": for `pull_request` that is
  `GITHUB_SHA`, so the workflow that skips is part of the tree the key names.

The docs say a pull request's cache "can only be restored by re-runs of the
pull request". The mechanism needs a later push's run (the signed squash) to
count as one, which the docs do not spell out. If it does not, no run ever
skips and everything runs as before; nothing else depends on it.

## What remains possible

- **A newer entry that fails the checks hides a valid older one.** The prefix
  lookup returns the newest entry under the prefix. A cancelled run cannot
  save one, but anyone who can push to the pull request can save a fake one
  from a changed workflow; `decide` then refuses it and the run runs
  everything. That costs a full run, never a false pass. A fail marker saved
  the same way does the same.
- **A changed workflow on this pull request's own base branch.** A vouching
  run is a run of this pull request (the only one from its head branch) whose
  head had this head's tree, so it ran the reviewed head merged with some
  commit of this pull request's base branch. A correct workflow uploads the
  artifact for a key only after passing that key's merge tree. A workflow
  changed on the base branch itself could upload it without testing anything,
  and a later revert on the base leaves no event on the timeline. What stands
  in the way today (`.github/rulesets/`, as applied on 2026-11-11): on `main`,
  a signed commit and a reviewed pull request, but `public-main-minimal` lets
  organization admins bypass it at any time; on `3.x.x` and the other version
  lines only `required_signatures`, so any writer with a signing key can push a
  workflow change there; on an unprotected base, nothing.
- **What the events do not show.** The base-change check sees only the
  events GitHub writes, under names its docs list for one API or neither. It
  makes no claim
  about a base branch that is deleted and re-created, or changed and changed
  back, without such an event, nor about **a re-run of an old run**: a re-run
  uses its original merge commit, and so its original base and workflow file,
  and can save markers and upload artifacts long after that base changed.
  GitHub keeps re-runs available for 30 days.
- **Two pull requests whose merge results have the same tree** can never
  vouch for each other while the listing returns both: it refuses a second
  pull request from the same head branch, and a run of a different head
  branch fails the run record's head-branch check. Without the listing, the
  creation-time rule and the head-branch events close only part of it, since
  they bind by order and by recorded events, not by which pull request a run
  belongs to. The creation time refuses every run created before this pull
  request was opened. The events refuse this pull request once its head branch
  was deleted or restored while it existed. Neither refuses a run of the other
  pull request created after this one was opened: that pull request reopened,
  two open at once from one branch into different bases with a push that
  creates a run for the other, or a closed earlier one labelled in L3 (if
  GitHub starts a run for that, which is unverified). Against those only the
  listing (two pull requests, so it refuses) stands.
- **Renamed branches** (unverified). Nothing GitHub records names the pull
  request a run belonged to: a run's record gives a head branch and a commit,
  and the listing gives the pull requests opened from a branch of that name.
  No event for a rename was found on a pull request, and whether one exists
  can't be established from outside GitHub. If there is none, someone with
  write access could rename branches so that a run of another pull request,
  made on a base branch whose changed workflow uploads the artifact without
  testing anything, carries this pull request's branch name and tree, rename
  them back, and save a key that names that run in this pull request's cache.
  A later push of the same tree would then skip. That takes write access, a
  base branch that carries the changed workflow and deliberate renames.
  Someone with that access can change what CI runs more directly; the
  difference is that the diff under review would not show this.
- **A pull request converted from an issue** (unverified). `POST
  /repos/{repo}/pulls` with `issue` turns an existing issue into a pull
  request. If that pull request's `created_at` were the issue's, a writer
  could give a pull request a creation time earlier than a run of another
  pull request from the same branch, and that case would rest on the listing
  alone again. It was not tried on GitHub, as trying it creates an issue and a
  pull request.
- **Runs that do not record a failure.** A run cancelled as a whole, or one
  that failed only the advisory job, records no failure. Neither does a failed
  run whose fail job never got as far as its saves (the runner was lost, the
  job hit its timeout, or its key step failed, for one when the marker job
  left no key and the fallback checkout failed too), or whose two saves both
  failed (the artifact upload and the cache save) while no earlier attempt of
  the run had left a record. One failed save alone is caught by the other
  record. A failure recorded for the same key from another scope (both lists
  are the whole repository's) also stops the skip: that only ever costs a
  run.
- **A recorded failure stays** for its tree, even if it was a flake, until the
  salt changes, both records are deleted by hand, or both are gone on their
  own. To clear one tree, delete the cache entry,
  `gh cache delete tree-fail-v1-ci.yml-salt<N>-<tree> -R Qbit-Org/qbit-mining-bootstrap`,
  and every `tree-fail-<sha256 of that key>` artifact,
  `gh api "repos/Qbit-Org/qbit-mining-bootstrap/actions/artifacts?name=tree-fail-<sha256>" --jq '.artifacts[].id'`
  then `gh api -X DELETE repos/Qbit-Org/qbit-mining-bootstrap/actions/artifacts/<id>` for each
  (`gh cache delete` and the artifact `DELETE` need `actions: write`; the
  listing needs only `actions: read`). The cache evicts an entry not accessed in over
  7 days, and every run of that tree looks it up, which can count as an
  access, so that needs seven days with no run of the tree; the artifact
  expires after 90 days.
- **L3 sees no run whose verdict is not there yet.** A later L3 run of the
  tree whose build failed, whose collation could not read its inputs, or that
  was cancelled before its collation uploads no verdict, and the skip, like a
  tag's promotion, never sees it. A run of the tree still in progress when the
  skip decides uploads its verdict afterwards: a `release-candidate` labeled
  run, for one, has its own concurrency group, so the squash push does not
  cancel it. The skip cannot see that verdict, and a later tag promotes it if
  it is the newest. A full run of the squash push is overtaken the same way: a
  tag promotes whichever complete run of the tree uploaded its verdict last.
  (`ci.yml`'s version of this is the fail marker's, under "It never skips".)
  A run that had uploaded any verdict, even an incomplete one, before the skip
  decided refuses it, and so does this run's own earlier attempt's; an
  expired verdict is not seen.
- **Anything outside the tree.** A marker says the tree passed once, on the
  runner images, action versions and external services of that run. A flaky
  test that passed once stays passed for that tree.

## Permissions

Each REST call `decide` makes, and the `GITHUB_TOKEN` permission it needs (from
GitHub's fine-grained permission data for each endpoint):

| Call | Permission |
|---|---|
| `GET /repos/{repo}/actions/runs/{id}`, `.../artifacts?name=`, `.../attempts/{n}/jobs`, `.../jobs?filter=latest` | `actions: read` |
| `GET /repos/{repo}/actions/caches?key=` (fail marker) | `actions: read` |
| `GET /repos/{repo}/actions/artifacts?name=tree-fail-…` (fail record) | `actions: read` |
| `GET /repos/{repo}/git/commits/{sha}` (head tree) | `contents: read` |
| `GET /repos/{repo}/pulls?head=…&state=all&per_page=2` | `pull-requests: read` |
| `GET /repos/{repo}/issues/{n}/timeline` | `pull-requests: read` (or `issues: read`; either works) |
| `GET /repos/{repo}/issues/{n}/events` | `pull-requests: read` (or `issues: read`; either works) |
| `GET /repos/{repo}/pulls/{n}` (labels, in the lookup step) | `pull-requests: read` |

The marker job has exactly `actions: read`, `contents: read` and
`pull-requests: read`, and L3's plan job has the same three; neither needs
`issues`. `permissions` sets what `GITHUB_TOKEN` may do, and the cache and
artifact actions do not use `GITHUB_TOKEN`: they authenticate with the
runner's own `ACTIONS_RUNTIME_TOKEN`, so they need no entry. The save and fail
jobs need only `contents: read` (their checkout). Deleting a marker or a
record by hand (`gh cache delete`, or the artifact `DELETE`) needs
`actions: write`; the workflows never delete either (the fail record's upload
runs with `overwrite: false`, so a re-run's upload of the same name fails
instead of replacing it).

## Checking it locally

```sh
python3 -m unittest -v tests.test_tree_pass_marker
```

The tests cover the decision table (the same tree skips, a different tree,
salt or workflow runs, every lookup or API failure runs, never on a push, tag,
dispatch or fork), every check of the run a marker names and of the pull
request, the fail marker, the aggregate's `jq` rule (run with `jq`), when a
marker may be saved, and the wiring: every `ci.yml` job is skipped by the
marker at job level, step by step (the matrix jobs), or deliberately not at
all, and the save and fail jobs need all of them. They also run the marker
steps' own `run:` scripts from both workflow files under bash, against a
fixture repository and a fake `gh`.

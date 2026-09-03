"""Durable PRISM block-candidate codec, replay queue, and submitter service.

B1 owner module: the node-offer fast lane, the same-hash disposition guard,
the accounting actor and its handoff queues, startup/outbox replay and
quarantine, retry merge/pacing/finalize-only state, acceptance evidence,
block-work liveness heartbeats, and the bounded DB/RPC call workers all live
here. Post-offer accepted-block finalization (``submit_block_candidate``,
``_submit_block_candidate_serialized``, ``_land_and_confirm_block_candidate``)
stays coordinator-owned at this layer and is reached through the injected
coordinator reference; a later layer moves it to its own finalization owner.
This module never imports ``prism_coordinator``.
"""

from __future__ import annotations

import dataclasses
import inspect
import itertools
import json
import math
import queue
import threading
import time
import traceback
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field, replace as dataclass_replace
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Iterator, Protocol

from lab.prism import direct_stratum
from lab.prism.coordinator_config import (
    BLOCK_LANDING_DB_TIMEOUT_WATCHDOG_FRACTION,
    DEFAULT_ACCEPTED_PARENT_REDRIVE_ATTEMPT_MAX,
    DEFAULT_ACCEPTED_PARENT_REDRIVE_DEFER_THRESHOLD,
    DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX,
    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_MAX_SECONDS,
    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_DB_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS,
    DEFAULT_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_STUCK_CALL_EXIT_SECONDS,
    DEFAULT_PRISM_OBSERVED_TIP_ACCEPT_WINDOW_SECONDS,
    DEFAULT_PRISM_WATCHDOG_TIMEOUT_SECONDS,
    MAX_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX,
)
from lab.prism.coordinator_shutdown import ShutdownInProgress
from lab.prism.job_bundle import PRISM_JOB_BUILD_SECONDS_BUCKETS
from lab.prism.job_delivery import PrismJobContext
from lab.prism.rpc import JsonRpc
from lab.prism.share_ledger import LedgerOperationTimeout, PendingShare
from lab.prism.stratum_session import ClientState, WorkerIdentity


# Block candidates queue to a dedicated submitter thread so the miner's share
# ack never waits on audit/submitblock after the share and intent commit. The
# bound limits RAM; overflow only coalesces a wakeup because Postgres retains
# the authoritative pending candidate.
MAX_PENDING_BLOCK_CANDIDATES = 32
# Startup replay must observe every durable pending candidate before child
# job builds unblock (issue #188 fix 4): a truncated enumeration could hide
# the active parent whose payout transition is not yet armed. A full batch
# therefore re-queries with a doubled window until the result is provably
# untruncated. This cap bounds one enumeration pass's memory; at the cap the
# gate simply stays closed while the queued batch drains, and the submitter
# loop re-enumerates the shrinking remainder.
MAX_BLOCK_REPLAY_ENUMERATION_ROWS = 1024
# Ancestor re-drive bookkeeping (issue #190) is keyed by block hash and
# dropped the moment the blocking transition resolves; this bound only
# guards against a pathological stream of distinct never-resolving
# ancestors leaking entries forever. Eviction is oldest-first, and every
# deferral re-inserts its ancestor's record and its child's blocking
# entry, so for a wedge the still-blocking pair is the last thing evicted.
MAX_ANCESTOR_REDRIVE_TRACKED_HASHES = 64


@dataclass
class _AncestorRedriveRecord:
    """Per-ancestor re-drive bookkeeping (issue #190).

    One record holds every fact the mechanism tracks about an ancestor, so
    recency (the record's insertion position), the armed request, and the
    exhaustion latch can never disagree the way parallel registries could.
    Mutated only under ``coordinator.lock``.
    """

    # Consecutive finalization deferrals since the last armed pass. Frozen
    # while a request is armed-but-unconsumed: deferrals during that window
    # prove nothing the armed pass will not already act on, and counting
    # them would let the next attempts arm on single deferrals right after
    # consumption, exhausting the cap before the first pass's adoption had
    # a chance to resolve the ancestor.
    streak: int = 0
    # Re-drive passes armed against the per-ancestor cap.
    attempts: int = 0
    # Armed passes the submitter actually consumed. The resolved counter
    # keys off this, not ``attempts``: a transition that resolved through
    # the ordinary landing tail while a request sat unconsumed is the
    # mechanism standing by, not succeeding.
    consumed: int = 0
    # A forced durable-replay pass is waiting for the submitter loop.
    armed: bool = False
    # The cap is spent; deferrals fall back to exactly the pre-#190
    # behavior with the publication-progress watchdog as the backstop.
    exhausted: bool = False


DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS = 0.25
DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS = 30.0
# The primary accounting handoff queue must be bounded or the documented
# result-preserving spillover ordering can never engage; the overflow queue
# stays unbounded by design so an already-offered block is never converted
# back into a raw-submit retry.
DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH = 8
# How many definitive-acceptance accounting tasks the lane may dispatch back
# to back while non-accepted work is waiting. The accepted lane exists so a
# decided block's accounting never queues behind stale replay (see
# _enqueue_block_accounting_task), but "never behind" must not become "stale
# replay never runs": a stale accounting task that is starved leaves its
# durable outbox row pending, its offer-time accepted-block payout-preview
# barrier armed, and its disposition lease held, which is a different bug
# rather than a fixed one. The quota therefore states the fairness bound
# outright -- a non-accepted task at the head of its lane waits behind at
# most this many accepted services, whatever the accepted arrival rate.
#
# Four, not one: one accepted tip can produce more than one accepted-evidence
# task (the winner, plus a same-height idempotent replay of it), and a quota
# of one would interleave a stale task's ~6 RPCs, two ledger writes and a
# payout-balance mutation into the middle of that acceptance tail -- the very
# interleaving the lane was added to remove. Four covers a normal tip's tail
# while still bounding stale delay at four accepted services.
BLOCK_ACCOUNTING_ACCEPTED_DISPATCH_QUOTA = 4
# A collapsed row whose post-write cleanup failed has no durable replay
# source left, so the accounting lane is the only thing that can ever finish
# it. Draining it purely from that lane's idle branch made it hostage to the
# lane staying idle: sustained ordinary accounting traffic, or a
# continuously replenished invalid-candidate quarantine queue, starved the
# retry for as long as the traffic lasted. The lane therefore also offers
# one bounded retry after at most this many completed work items, where a
# work item is one finished accounting task from any of the handoff queues
# or one finished quarantine item. The offer runs at most one hash's
# still-owed steps and the cadence resets on every offer -- including one
# that finds nothing due -- so neither lane can starve the other.
DEFAULT_BLOCK_ACCOUNTING_CLEANUP_RETRY_WORK_ITEMS = 8
# Issue #198. A cleanup-retry record is the only replay source a durably
# terminal row has left, so the registry never sheds one; under a systemic
# cleanup fault it therefore grows by one record per row the collapse wins.
# At the observed 312,000-candidate storm that is an unbounded in-memory
# backlog -- each record retaining its exact pending-share floor holders and
# pinning its terminal-outcome fence -- driven by a lane that retries one
# hash per pass. The bound is applied to *admission* instead: once the
# backlog holds ``_collapse_cleanup_retry_backlog_max()`` records
# (PRISM_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX, default
# DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX) the collapse hands no
# further rows to its fenced bulk terminalization, and every row it declines
# stays durable and pending for the ordinary per-row path. Nothing already
# terminal loses its record, its holders, or its fence.
#
# A persistently engaged backpressure would otherwise print one line per
# page or per dequeued sibling; the warning is rate limited to this interval
# while the ``backlog_deferred`` counter still moves per row.
BLOCK_CANDIDATE_CLEANUP_BACKPRESSURE_LOG_SECONDS = 60.0
# The closed vocabulary the backpressure warning names its caller from, so
# the field can never carry a hash, a page cursor, or a free-form reason.
PRISM_BLOCK_CANDIDATE_CLEANUP_BACKPRESSURE_CALLERS = (
    # The replay-adoption page walk (#183/#196).
    "replay-page",
    # The dequeue-time stale sibling skip (#181 item 2).
    "dequeue",
)
# The same-hash disposition guard remembers every terminal outcome so a late
# duplicate -- queued, replayed, retried, parked, or quarantined -- joins the
# decision instead of re-offering a block qbitd already answered for. That
# memory was process-lifetime historical state: one testnet4 storm left
# 312,000 entries behind, and every decided-height collapse poll copied the
# whole registry under the global lock (~124 ms at that size, growing with
# every block the process ever disposed) while unrelated share acks waited.
#
# The registry is bounded instead, and eviction is oldest-first over the
# entries that no in-memory copy still needs: every lane that can hold a
# candidate object (or its still-owed terminal work) pins its hash and is
# never evicted, so a fenced hash cannot become offerable again by being
# forgotten. That includes the instant between lanes -- a candidate the
# submitter has taken out of a holder or queue but not yet handed to its
# disposition flight is named by the dequeued-hash pin, moved in and out
# under the same lock holds that empty and refill the lanes around it. The
# bound sits far above the number of copies the process can
# hold at once -- the bounded admission queue
# (MAX_PENDING_BLOCK_CANDIDATES), one replay enumeration batch
# (MAX_BLOCK_REPLAY_ENUMERATION_ROWS), and the single-slot retry holders --
# so ordinary operation never even reaches an eviction.
MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES = 8192
# One insert examines at most this many of the oldest entries beyond the
# overflow it has to clear, so trimming stays O(1) amortized under the global
# lock even when a run of pinned hashes sits at the front of the registry.
# A pinned entry the scan passes is moved behind the window rather than
# dropped, so every pass makes progress against the next one.
BLOCK_CANDIDATE_TERMINAL_OUTCOME_EVICTION_SCAN = 128
BLOCK_SUBMITTER_WAIT_HEARTBEAT_SLICE_SECONDS = 0.25
MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS = 2
BLOCK_CANDIDATE_RETRY_HEARTBEAT_SLICE_SECONDS = 0.25
BLOCK_CANDIDATE_INTENT_SCHEMA = "qbit.prism.block-candidate-intent.v1"
PRISM_STALE_JOB_ABANDON_CLASSES = (
    "tip_moved",
    "balance_stale",
    "append_epoch_stale",
)
# A would-be terminal abandonment refused because the candidate's own block
# hash is (or was recently observed as) part of the active chain: qbitd
# accepted the block even though this process has not completed the accepted
# success tail (for example when the direct submitblock ack was lost in
# transport and acceptance was learned from a blockwait tip observation).
# The candidate defers and retries until the tail finalizes it as submitted.
PRISM_REJECTION_BLOCK_ACCEPT_PENDING = "accepted-pending-finalization"
# The ledger reported the candidate's row terminally disposed (reorg
# quarantine, rejection, or reversal) before the confirmation landed. That is
# a routine race around a chain reorganization: terminal for this candidate,
# benign for the pool, so it must never escalate to a coordinator shutdown
# the way an unexplained confirmation failure does.
PRISM_REJECTION_LEDGER_CONFIRMATION_SUPERSEDED = "ledger-confirmation-superseded"
# Used only by lightweight embedders that bypass dataclass/coordinator
# construction. Production instances adopt their eagerly installed state at
# service construction. Serializing the fallback prevents concurrent
# first-touch callers from ever publishing different containers for the same
# state.
_STATE_BACKFILL_LOCK = threading.Lock()

# -- decided-height candidate collapse (#183) --------------------------
# Bounded, fixed-label outcome keys for the collapse selector/apply. They
# are the whole metric label space: no block hash, parent hash, or job ID
# ever becomes a Prometheus label.
PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES = (
    # Durable pending rows the selector examined.
    "considered",
    # Rows preserved because their height was never probed: the walk had
    # spent MAX_BLOCK_CANDIDATE_COLLAPSE_HEIGHT_PROBES distinct heights.
    "height_deferred",
    # Rows that satisfied every predicate clause.
    "selected",
    # Selected rows whose disposition lease was held elsewhere.
    "lease_skipped",
    # Leased rows the immediate pre-write re-read disqualified.
    "revalidation_dropped",
    # Qualified rows the fenced batch write did not transition.
    "write_lost",
    # Rows the fenced batch write actually transitioned.
    "abandoned",
    # Rows preserved because a page-scope read or write failed closed.
    "fail_closed",
    # Won rows whose post-write cleanup raised.
    "cleanup_failed",
    # Deferred cleanups a later bounded retry pass finished in full.
    "cleanup_recovered",
    # Retry passes that ran but still left at least one step owed.
    "cleanup_retry_failed",
    # -- issue #198: cleanup-retry backlog backpressure -----------------------
    # Rows that satisfied (or were never asked) predicate S and were
    # preserved -- left durable and pending for the per-row path -- because
    # the cleanup-retry backlog was at its configured admission bound. Shared
    # by the replay-page walk and the dequeue-time skip like the outcomes
    # above; the warning names the caller, the counter counts rows.
    "backlog_deferred",
    # -- issue #181 item 2: the dequeue-time stale sibling skip -------------
    # These three name the *population* the skip judged, one dequeued
    # candidate at a time, and partition it exactly:
    # dequeue_considered = dequeue_skipped + dequeue_preserved. The outcomes
    # above stay shared with the replay-adoption page path, because they
    # describe the same machinery (one selection, one lease set, one fenced
    # write, one cleanup) whichever caller drove it.
    #
    # Dequeued candidates predicate S was actually asked about: the ledger
    # can answer a fenced batch abandonment and a pool-block probe, and the
    # candidate carries a readable hash and parent.
    "dequeue_considered",
    # Dequeued candidates terminalized before any node offer.
    "dequeue_skipped",
    # Dequeued candidates handed to the ordinary offer path, for any reason
    # at all -- current work, offer evidence, an unprobed height, a lighter
    # occupant, a lost fenced write, or a read that failed closed.
    "dequeue_preserved",
)
# Issue #181 item 3: the interval from definitive node acceptance (the
# submitblock RPC returning None on the submitter thread) to the accepted
# block's payout preview becoming visible to waiting child work. Both exits
# of _publish_accepted_block_payout_preview publish -- the atomic generation
# publication, and issue #188's fenced local-retention branch, which installs
# the compact preview and notifies waiters without installing a generation --
# so the label set separates them rather than merging a degraded publication
# into the healthy one.
PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS = (
    "published",
    "degraded",
)
# The acceptance criterion is a p95 below the 5 s child wait budget
# (DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS), so the boundary at
# exactly 5.0 has to exist and the approach to it has to be resolved. The
# shared PRISM_JOB_BUILD_SECONDS_BUCKETS tuple is tuned for millisecond job
# builds and steps 1.0 -> 2.5 -> 5.0 -> 10.0 across the whole decision range;
# this dedicated tuple keeps that one unwidened while still extending well
# past the budget so a saturated tail is bucketed instead of folded into
# +Inf.
PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS = (
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    1.5,
    2.0,
    3.0,
    4.0,
    5.0,
    7.5,
    10.0,
    20.0,
    30.0,
    60.0,
)
# Acceptance stamps are keyed by block hash and consumed by the first
# publication observed for that hash. A hash that never reaches publication
# -- a terminal accounting failure, a withdrawn acceptance -- would otherwise
# retain its stamp forever, so the oldest entries are evicted past this cap.
# Max-block admission and the physical block rate keep the live population
# far below it; this is a leak bound, not a working set.
MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS = 512
# The terminal cleanup one won row owes, in the order the apply runs it.
# A deferred retry replays exactly the steps its own hash still owes, so
# the step names are a closed set: a retry can never repeat a step that
# already completed, and never invent one the apply does not run.
BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP = "payout-preview-withdrawal"
BLOCK_CANDIDATE_COLLAPSE_CLEANUP_FLOOR_STEP = "pending-share-floor"
BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS = (
    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP,
    "finalize-retry",
    "retry-state",
    "outstanding-and-tip-observation",
    "terminal-outcome",
    "abandonment-accounting",
    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_FLOOR_STEP,
)
# The per-row path abandons a candidate whose height was decided by another
# block as a tip-moved stale job; the bulk path must land in the same
# bounded reason/class buckets or the abandonment series splits in two.
# ``share_submission`` imports this module, so its PRISM_REJECTION_STALE_JOB
# is restated here rather than imported; a collapse test pins them equal.
PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON = "stale-job"
PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS = "tip_moved"
# A storm collapses thousands of rows in one apply. Logs stay bounded: at
# most this many parent/job/reason groups are printed, each with at most
# this many sample hashes, plus one remainder line.
BLOCK_CANDIDATE_COLLAPSE_LOG_GROUPS = 8
BLOCK_CANDIDATE_COLLAPSE_LOG_SAMPLE_HASHES = 3
# A systemic cleanup fault would otherwise print one line and one
# traceback per row of a storm-sized page; only the first few are
# detailed and the rest are summarized by count.
BLOCK_CANDIDATE_COLLAPSE_LOG_FAILURES = 3
# A ledger or node that cannot answer the page-scope reads fails closed on
# every poll. The counter still moves per row; the log line is rate limited
# so a persistently degraded read cannot flood the journal.
BLOCK_CANDIDATE_COLLAPSE_FAIL_CLOSED_LOG_SECONDS = 60.0
# Chain-height probes one replay walk may spend on collapse, counted in
# *distinct heights* rather than rows.
#
# The storm this path exists for is one decided height behind thousands of
# rows, and it costs a single probe no matter how wide the page is. A page
# whose rows span many heights is the opposite shape: selection would read
# ``getblockhash`` (and then the occupant's ``getblockheader``) once per
# distinct height, sequentially, on the sole block-submitter thread. A page
# holds up to MAX_BLOCK_REPLAY_ENUMERATION_ROWS rows, so that is up to 1,024
# heights and ~2,048 round trips; at a production RPC deadline the thread
# would sit in collapse for hours while a live solve waited behind it for
# submitblock. The bound makes the chain cost of a walk a constant --
# at most this many ``getblockhash`` reads and at most one occupant header
# each -- independent of how many rows or heights the backlog carries.
#
# Rows at a height the walk did not probe are simply never selected. They
# stay durable, keep every piece of evidence they had, and are adopted by
# the ordinary per-row disposition path, which disposed of them exactly this
# way before collapse existed; a later walk starts with a fresh budget and
# may probe their heights. Collapse is an optimisation over that path, so
# spending less of it is always safe and never terminalizes anything.
MAX_BLOCK_CANDIDATE_COLLAPSE_HEIGHT_PROBES = 8
# PRISM records every network difficulty in scaled integer units: the
# coordinator stamps a candidate's ``found_block.network_difficulty`` with
# ``template_artifacts.scaled_network_difficulty(template["bits"])``, which is
# ``pow_limit_target * 1_000_000 // template_target`` -- roughly the raw
# Bitcoin-style difficulty multiplied by a million. The collapse selector
# compares that stored value against the occupying block's work, so the
# occupant has to be converted into the same units from the same inputs.
# These restate the scale and the qbit powLimit rather than importing them
# from ``template_artifacts`` (or ``public_api``): both sit above this module
# in the import graph, and B1 must not grow an edge to the template/payout
# layer for two constants. ``_collapse_scaled_difficulty`` reapplies the
# formula, and a collapse test pins it equal to the production helper.
COLLAPSE_DIFFICULTY_SCALE = 1_000_000
COLLAPSE_POW_LIMIT_BITS = "207fffff"
COLLAPSE_POW_LIMIT_TARGET = direct_stratum.target_from_compact_hex(
    COLLAPSE_POW_LIMIT_BITS
)


def _pending_rows_accepts_cursor(pending_rows: Callable[..., Any]) -> bool:
    """Report whether a ledger's pending-row reader can paginate.

    Signature introspection runs first because the alternative — probing
    with the keyword and catching TypeError — spends a bounded ledger
    worker and records a landing-class ledger-call sample on every startup
    against a ledger that never supported cursors. The probe still runs
    (and still falls back) whenever a callable cannot be introspected; this
    only skips it where the answer is already knowable.
    """
    try:
        parameters = inspect.signature(pending_rows).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        parameter.name == "after_cursor"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _block_replay_cursor_key(after_cursor: object | None) -> str | None:
    """Fold one opaque enumeration cursor into a hashable dedupe-key part.

    The cursor is a JSON-safe list, so it cannot go into a tuple key as-is.
    Only identity matters here: two calls share an in-flight ledger worker
    exactly when they are the same query.
    """
    if after_cursor is None:
        return None
    return json.dumps(after_cursor, separators=(",", ":"), default=repr)


def _collapse_block_hash(value: object) -> str | None:
    """Normalize one block hash, or report it unusable.

    Every collapse comparison is between hashes from three unrelated
    sources -- a durable intent, a durable row key, and a qbitd RPC -- so
    they are folded to one canonical spelling here. Anything that is not a
    64-character hex string is unusable rather than merely differently
    spelled, and its row is never selected.
    """
    if not isinstance(value, str):
        return None
    key = value.strip().lower()
    if len(key) != 64:
        return None
    try:
        int(key, 16)
    except ValueError:
        return None
    return key


def _block_candidate_hash_of(candidate: object) -> str | None:
    """The canonical block hash one candidate object carries, if any."""
    return _collapse_block_hash(
        getattr(
            getattr(candidate, "submission", None),
            "block_hash_hex",
            None,
        )
    )


def _collapse_height(value: object) -> int | None:
    """Parse one block height, rejecting booleans and inexact numbers.

    ``bool`` is an ``int`` subclass, so a stray ``True`` would otherwise
    parse as height 1 and compare against a real tip height.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value) or value != int(value):
            return None
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip(), 10)
        except ValueError:
            return None
    return None


def _collapse_row_height(durable_row: object) -> int | None:
    """The height one durable page row claims, or None when it states none.

    The single place that knows where a fetched row records its height, so
    the cheap pre-selection peek in ``_collapse_superseded_block_candidates``
    and the decode in ``_superseded_candidate_row`` can never drift apart on
    it. This is only a peek: it validates nothing beyond the parse, and every
    fact it reads is re-read and cross-checked against the row's template by
    the decode, which is what actually decides the row.
    """
    if not isinstance(durable_row, dict):
        return None
    intent = durable_row.get("candidate")
    if not isinstance(intent, dict):
        return None
    return _collapse_height(intent.get("expected_height"))


def _collapse_difficulty(value: object) -> int | float | None:
    """Parse one stored work value, rejecting booleans and non-finite numbers.

    A NaN compares false against every threshold, so an unparsed NaN would
    silently answer "not enough work" (safe) or, inverted, "enough work"
    (not safe). Rejecting it outright keeps the clause decidable.

    An integral spelling is kept as an ``int`` rather than widened to
    ``float``. PRISM stores ``found_block.network_difficulty`` as the scaled
    *integer* ``scaled_network_difficulty`` returns, and above 2**53 a float
    conversion rounds it -- often upwards, which would make a candidate look
    strictly heavier than the identical work the chain reports and defeat
    exactly the equal-work case clause 4b/5 has to accept. Python compares
    ``int`` and ``float`` exactly, so a non-integral spelling stays usable
    without dragging the integral ones through binary floating point.
    """
    if isinstance(value, bool) or value is None:
        return None
    number: int | float
    if isinstance(value, int):
        number = value
    elif isinstance(value, float):
        number = value
    elif isinstance(value, str):
        text = value.strip()
        try:
            number = int(text, 10)
        except ValueError:
            try:
                number = float(text)
            except (OverflowError, ValueError):
                return None
    else:
        return None
    if isinstance(number, float) and not math.isfinite(number):
        return None
    if number <= 0:
        return None
    return number


def _collapse_scaled_difficulty(bits: object) -> int | None:
    """Convert one header's compact ``bits`` to PRISM-scaled difficulty units.

    This is deliberately the same integer formula
    ``template_artifacts.scaled_network_difficulty`` applies to the
    template's bits when the coordinator stamps a candidate's
    ``found_block.network_difficulty`` -- recomputed here from
    ``direct_stratum`` rather than imported, because this module must not
    grow an edge to the template/payout layer. Both sides of clause 4b/5
    therefore land on the identical integer for identical work, with no
    float rounding at the equality boundary the clause turns on.

    Returns ``None`` for anything that is not an 8-character compact hex
    string with a positive target, so a header missing ``bits``, or
    carrying a malformed or zero one, fails its page closed instead of
    being read as "no work".
    """
    if not isinstance(bits, str):
        return None
    compact = bits.strip().lower()
    if len(compact) != 8 or any(char not in "0123456789abcdef" for char in compact):
        return None
    try:
        target = direct_stratum.target_from_compact_hex(compact)
    except ValueError:
        return None
    if target <= 0:
        return None
    return max(
        1,
        (COLLAPSE_POW_LIMIT_TARGET * COLLAPSE_DIFFICULTY_SCALE) // target,
    )


class _BlockCandidateCollapseFailedClosed(Exception):
    """A page-scope chain read was unavailable or proved nothing.

    Raised, not returned, because every caller's answer is the same: this
    page selects nothing and every one of its rows is preserved for the
    ordinary per-row path.
    """


@dataclass
class _CollapsedCandidateCleanup:
    """One terminal hash's outstanding cleanup, kept for a later retry.

    After the fenced batch write the durable row is terminal, so the row is
    partitioned out of the replay page and no later enumeration will ever
    return it: a cleanup step that failed has no durable replay source and
    would otherwise leave its in-memory state -- a payout preview or its
    tombstone, a pending-share floor holder, an outstanding-hash marker --
    installed for the process lifetime. This record is that replay source.

    It holds no candidate object, no durable row, and no node submission,
    so a deferred cleanup can only finish tearing state down; there is
    nothing here to re-adopt or re-offer with. ``shares`` carries the exact
    pending-share floor holders the apply resolved, because the floor keys
    holders by object identity and the queues they were read from are
    drained long before a retry runs. ``shares_resolved`` is false only
    when the apply aborted before it could index them, in which case the
    retry re-runs the (bounded, in-memory) scan itself.
    """

    block_hash: str
    steps: frozenset[str]
    shares: tuple[PendingShare, ...] = ()
    shares_resolved: bool = True
    attempts: int = 0
    delay_seconds: float = 0.0
    not_before_monotonic: float = 0.0
    # When this hash's cleanup was first deferred. Kept across merges and
    # reschedules so the oldest-age gauge measures how long the row has
    # owed cleanup, not how long since its last failed attempt.
    deferred_monotonic: float = 0.0


@dataclass(frozen=True)
class _SupersededCandidateRow:
    """One durable pending row reduced to the facts predicate S reads."""

    block_hash: str
    parent_hash: str
    height: int
    # PRISM-scaled units (see COLLAPSE_DIFFICULTY_SCALE), as stamped by the
    # coordinator. Comparable only against a chain-side work value converted
    # into the same units by ``_collapse_scaled_difficulty``.
    network_difficulty: int | float
    job_id: str


@dataclass
class _CollapseHeightProbeBudget:
    """Distinct chain heights a collapse walk may still read, and what it read.

    One budget is created per replay walk and shared by that walk's pages,
    so the walk's chain cost is the bound whether the backlog arrives as one
    page or fifty. A pass handed no budget (a direct call, or the legacy
    single-page shape) gets a fresh full one, which is the same ceiling
    applied to a walk of one.

    Only heights are charged. The occupant header a probed height leads to is
    read at most once per probed height, so charging the height bounds both
    round trips at once and keeps the accounting to the single number the
    comment on MAX_BLOCK_CANDIDATE_COLLAPSE_HEIGHT_PROBES explains.

    The two caches live here, beside the counter, for the same reason the
    counter does: a walk that has paid for a height must not pay for it
    again on its next page. Each page builds a fresh
    ``_BlockCandidateChainView`` over this one budget, and a view that
    cached its reads only for itself would recharge the walk for the same
    height on every page -- so the one decided height behind a storm would
    exhaust the budget after eight pages and defer the entire remainder of
    the backlog. ``difficulty`` is keyed by block hash, whose work is
    immutable, so sharing it can only remove round trips. Sharing ``active``
    widens the window between a selection read and the write it feeds, which
    is precisely the window the pre-write revalidation exists to close: that
    pass builds its own budget, so its caches -- and therefore its chain
    reads -- stay fresh and independent of this walk's.
    """

    remaining: int = MAX_BLOCK_CANDIDATE_COLLAPSE_HEIGHT_PROBES
    # Height -> the active-chain block this walk read there.
    active: dict[int, str] = field(default_factory=dict)
    # Block hash -> its PRISM-scaled work, re-derived from its header bits.
    difficulty: dict[str, int] = field(default_factory=dict)

    @property
    def exhausted(self) -> bool:
        return self.remaining <= 0

    def spend(self) -> bool:
        """Claim one height probe; False when the walk has none left."""
        if self.remaining <= 0:
            return False
        self.remaining -= 1
        return True


class _BlockCandidateChainView:
    """Bounded chain reads for one collapse pass.

    The best tip is read once for the whole page and the active block at a
    height once per distinct candidate height, so a 3,120-row storm at one
    decided height costs a fixed handful of RPCs instead of one round trip
    per row. Every miss or failure is a fail-closed page, never a guess.

    A height-diverse page cannot turn "once per distinct height" into a walk
    of the node as long as the page: ``probe_budget`` caps the distinct
    heights this view will ever read, and a height past the cap is reported
    as unprobed rather than guessed at.

    "Once per distinct height" is a property of the *walk*, not of this
    view. The caches are the budget's, so a page-two view over the same
    budget already knows every height page one probed: a storm at one
    decided height costs the walk one ``getblockhash`` and one
    ``getblockheader`` however many pages it spans. Only the page-scope tip
    reads are per view, which is what keeps each page's selection running
    against a freshly read tip.
    """

    def __init__(
        self,
        service: "BlockCandidateService",
        *,
        probe_budget: _CollapseHeightProbeBudget | None = None,
    ) -> None:
        self._service = service
        self._probe_budget = (
            probe_budget
            if probe_budget is not None
            else _CollapseHeightProbeBudget()
        )
        self._active: dict[int, str] = self._probe_budget.active
        self._difficulty: dict[str, int] = self._probe_budget.difficulty
        # Per view, never on the budget: the tip is the one fact a view must
        # not inherit from an earlier page, and keeping the memo here is what
        # preserves "each page's selection runs against a freshly read tip"
        # while letting one page read it once. Both reads were already made
        # at most once per view by the page selector and the pre-write
        # revalidation, so memoizing changes no existing caller's round
        # trips; it exists so a caller that needs the tip *before* handing
        # this view to selection -- the dequeue-time skip, which gates on
        # clause 3 before it spends a durable read -- does not pay for a
        # second one.
        self._tip: str | None = None
        self._tip_height: int | None = None

    def _call(self, method: str, params: list[object] | None = None) -> object:
        coordinator = self._service._coordinator
        coordinator._record_block_submitter_phase(f"replay-collapse-{method}")
        try:
            if params is None:
                return coordinator.rpc.call(method)
            return coordinator.rpc.call(method, params)
        except Exception as exc:
            raise _BlockCandidateCollapseFailedClosed(
                f"{method} failed during candidate collapse: {exc}"
            ) from exc
        finally:
            coordinator._record_block_submitter_phase(
                f"replay-collapse-{method}:complete"
            )

    def best_tip(self) -> str:
        """The chain's best tip, read once for the life of this view."""
        if self._tip is not None:
            return self._tip
        tip = _collapse_block_hash(self._call("getbestblockhash"))
        if tip is None:
            raise _BlockCandidateCollapseFailedClosed(
                "best tip hash is not a block hash"
            )
        self._tip = tip
        return tip

    def tip_height(self) -> int:
        """The best tip's height, read once for the life of this view."""
        if self._tip_height is not None:
            return self._tip_height
        height = _collapse_height(self._call("getblockcount"))
        if height is None or height < 0:
            raise _BlockCandidateCollapseFailedClosed(
                "best tip height is not an integer"
            )
        self._tip_height = height
        return height

    def active_at(self, height: int) -> str | None:
        """The active-chain block occupying ``height``, read once per height.

        ``None`` means this walk has spent its height-probe budget and the
        height was never read. It is not "no occupant" and it is not a
        fail-closed read: it says only that the chain was never asked, so
        every caller must preserve the row rather than decide anything about
        it. A height this walk has already read is free -- on this page and
        on every later page of the same walk -- and stays answerable after
        the budget is gone.
        """
        cached = self._active.get(height)
        if cached is not None:
            return cached
        if not self._probe_budget.spend():
            return None
        active = _collapse_block_hash(self._call("getblockhash", [int(height)]))
        if active is None:
            raise _BlockCandidateCollapseFailedClosed(
                f"active block at height {height} is not a block hash"
            )
        self._active[height] = active
        return active

    def cached_difficulty(self, block_hash: str) -> int | None:
        """The work this walk already read for a hash, without a new call."""
        return self._difficulty.get(block_hash)

    def difficulty_of(self, block_hash: str) -> int:
        """One occupying block's work, in PRISM-scaled units, once per hash.

        Read from the header's compact ``bits``, not its ``difficulty``
        field. The candidate row this value is compared against carries
        ``scaled_network_difficulty(template["bits"])`` -- the raw
        difficulty times COLLAPSE_DIFFICULTY_SCALE -- while
        ``getblockheader.difficulty`` is the raw float. Comparing the two
        directly made an equal-work occupant read as a millionth of the
        candidate's work, so clause 4b/5 rejected it and a decided height
        never collapsed. Recomputing from bits with the production formula
        puts both sides on the same integer scale, and keeping it integral
        means the equal-work boundary the clause turns on is decided
        exactly rather than by float comparison.
        """
        cached = self._difficulty.get(block_hash)
        if cached is not None:
            return cached
        header = self._call("getblockheader", [block_hash])
        if not isinstance(header, dict):
            raise _BlockCandidateCollapseFailedClosed(
                f"header for {block_hash} is not an object"
            )
        difficulty = _collapse_scaled_difficulty(header.get("bits"))
        if difficulty is None:
            raise _BlockCandidateCollapseFailedClosed(
                f"header for {block_hash} carries no usable compact bits"
            )
        self._difficulty[block_hash] = difficulty
        return difficulty


@dataclass(frozen=True)
class PrismBlockCandidate:
    """A block-worthy submission queued for the block-submitter thread.

    A share that met its target is acknowledged and credited on the client
    thread, then queued here for the submitter to land the block off the hot
    path. When the hash solved the block but missed the share target (floor
    above network difficulty), credit_share_on_accept is set and the candidate
    is instead submitted synchronously by handle_submit: that share is valid
    only if the block lands, so its credit and the miner's accept/reject follow
    the block outcome directly rather than being queued.
    """

    context: PrismJobContext
    submission: direct_stratum.DirectQbitSubmission
    extranonce1_hex: str
    extranonce2_hex: str
    pending_share: PendingShare
    client: ClientState
    credit_share_on_accept: bool = False
    durable_replay: bool = False
    # When this in-process attempt became runnable: live candidates stamp
    # share-accept time, durable outbox replays stamp row-restore time. The
    # block-submit histogram measures from here to submitblock's return --
    # the race-critical interval a lost block round is decided in.
    landed_monotonic: float = field(default_factory=time.monotonic)


@dataclass
class _BlockCandidateDispositionFlight:
    """One same-hash submission guard shared by its holder and waiters."""

    # The node-offer thread acquires this guard and the accounting thread
    # releases it after durable finalization. A plain Lock permits that
    # deliberate ownership transfer; RLock does not.
    lock: threading.Lock = field(default_factory=threading.Lock)
    users: int = 0


@dataclass(frozen=True)
class _BlockCandidateDispositionLease:
    """A same-hash guard held across node offer and durable finalization."""

    block_hash: str
    flight: _BlockCandidateDispositionFlight


@dataclass(frozen=True)
class _BlockCandidateNodeSubmission:
    """Result of the latency-critical qbitd fast-lane call."""

    attempted: bool
    result: object = None
    error: BaseException | None = None


def _is_definitive_node_acceptance(
    node_submission: _BlockCandidateNodeSubmission | None,
) -> bool:
    """Report whether an offer is a *definitive* node acceptance.

    qbitd's ``submitblock`` returns JSON ``null`` for a block it accepted and
    a rejection string ("duplicate", "inconclusive", a validation reason) for
    anything else, so the only shape that names an accepted block is an
    attempted offer that returned no result and raised nothing.

    All three clauses are load-bearing, and ``error is None`` is the one that
    is easy to drop by accident: the transport failure path in
    :meth:`_submit_block_candidate_to_node` builds
    ``_BlockCandidateNodeSubmission(attempted=True, error=exc)``, which leaves
    ``result`` at its ``None`` default. A two-clause test would therefore read
    a *failed* offer as an acceptance -- the single worst misclassification
    available here, because every consumer of this predicate treats a true
    answer as "the node already has this block".

    Consumers must share this one definition rather than re-spelling it:
    :meth:`_stash_retained_block_candidate_node_submission` decides whether an
    offer may be replayed from memory, and the accounting handoff decides
    which lane a task joins. Those two answers must never diverge.
    """
    if node_submission is None:
        return False
    return (
        bool(node_submission.attempted)
        and node_submission.error is None
        and node_submission.result is None
    )


@dataclass
class _BlockSubmitterLedgerCall:
    """One still-running direct outbox call, reused across paced retries."""

    started_monotonic: float = field(default_factory=time.monotonic)
    done: threading.Event = field(default_factory=threading.Event)
    result: object = None
    error: BaseException | None = None


@dataclass
class _BlockSubmitterRpcCall:
    """One hard-deadline, single-flight submitblock transport call."""

    started_monotonic: float = field(default_factory=time.monotonic)
    done: threading.Event = field(default_factory=threading.Event)
    result: object = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _BlockCandidateAccountingTask:
    """A node-offered candidate awaiting serialized durable accounting."""

    candidate: PrismBlockCandidate
    node_submission: _BlockCandidateNodeSubmission
    disposition_lease: _BlockCandidateDispositionLease
    # Set by the accounting lane's own exception path once it has retained
    # this task's candidate for retry while still holding the disposition.
    # The loop's catch reads it so a candidate the lane retained is not
    # retained (and counted) a second time, while a failure that fired
    # before the lane could retain -- a delegate raising ahead of it -- is
    # still retained there.
    retained_for_retry: threading.Event = field(
        default_factory=threading.Event,
        compare=False,
        repr=False,
    )


class BlockSubmitterDatabaseTimeout(TimeoutError):
    """A submitter ledger phase exceeded its coordinator-side deadline."""


@dataclass(frozen=True)
class BlockCandidateAttemptResult:
    """Structured result of the landing callback used by retry/terminalization."""

    accepted: bool
    reason: str | None
    error: str

    def retryable(self, retryable_reasons: frozenset[str]) -> bool:
        return not self.accepted and (
            self.reason is None or self.reason in retryable_reasons
        )


@dataclass(frozen=True)
class BlockCandidateRunResult:
    """Result returned after one in-memory wakeup or retry slot is consumed."""

    ran: bool
    refresh_client: Any | None = None


class BlockCandidateSubmitPort(Protocol):
    """Land one candidate through the coordinator's submit entrypoint.

    The landing tail has two forms and only the coordinator can tell them
    apart: embedders and tests replace the bound ``submit_block_candidate``
    to stand in for the node submission, and that replacement is installed on
    the instance *after* this service is constructed. The port therefore
    carries the whole landing call shape -- an already-created
    ``node_submission`` when the caller made one, and whether the caller
    already owns the same-hash disposition -- and lets the coordinator resolve
    the entrypoint per call, instead of the service inspecting it through the
    runtime seam.
    """

    def __call__(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
        disposition_held: bool = False,
    ) -> bool: ...


@dataclass(frozen=True)
class BlockCandidatePorts:
    """Callback seams back into the coordinator, resolved at use time.

    ``runtime`` is the owning coordinator; service bodies reach every
    cross-owner concern (and every monkeypatch-sensitive sibling delegate)
    through it at call time, per the in-tree Runtime seam convention. The
    named callables preserve the historical narrow port surface for focused
    embedders and the layer-original B1 ownership tests.
    """

    runtime: Any
    ledger: Callable[[], Any]
    stop_event: Callable[[], threading.Event]
    writer_operation: Callable[[str], AbstractContextManager[object]]
    submit_candidate: BlockCandidateSubmitPort
    reject_terminal_prepared: Callable[[PrismBlockCandidate], None]
    begin_preview: Callable[[str, int], None]
    clear_preview: Callable[[str, bool], None]
    share_writer: Callable[[], Any]
    finish_pending_candidate: Callable[[PendingShare], None]
    refresh_after_accept: Callable[[Any], None]
    record_heartbeat: Callable[[str], None]
    replay_entrypoint: Callable[[], int]
    submit_next_entrypoint: Callable[[float | None], bool]
    next_retry_delay: Callable[[str], float]
    log: Callable[[str], None]


def _candidate_block_hex(candidate: PrismBlockCandidate) -> str:
    """Return the candidate's serialized block, never an unmaterialized one.

    ``assemble_submission`` materializes ``block_hex`` only for a hash that
    solved the block, which every candidate route requires, so a real
    submission always carries the bytes by the time a candidate exists. If one
    ever does not, the cause is a defect in that coupling and the block is
    already lost -- but an empty value written to the durable intent loses it
    twice, resurrecting after every restart as a candidate that can never be
    offered to the node. Fail on the client thread instead, before the intent
    is written. Duck-typed embedders that retain no block bytes at all keep
    the historical empty-string behaviour: the invariant is a property of the
    real submission type, and the landing path already guards them separately.

    The guard deliberately does not apply to a durable replay. A replayed
    candidate's intent already exists, and ``block_candidate_from_intent``
    rebuilds every one of them as a real ``DirectQbitSubmission`` -- so an
    embedder's tolerated empty value comes back wearing the strict type. The
    only caller that re-encodes a replayed candidate is the credit-on-accept
    append, which runs *after* the block has landed; raising there would fail
    a credit for a block already in the chain, which is strictly worse than
    re-persisting the same empty value the row already holds. Re-persisting an
    existing intent is not the write this guard exists to stop.
    """
    submission = candidate.submission
    block_hex = str(getattr(submission, "block_hex", ""))
    if (
        not block_hex
        and not candidate.durable_replay
        and isinstance(submission, direct_stratum.DirectQbitSubmission)
    ):
        raise ValueError("block candidate submission carries no serialized block")
    return block_hex


def block_candidate_intent(candidate: PrismBlockCandidate) -> dict[str, Any]:
    """Return the immutable JSON needed to resume a candidate after restart."""
    context = candidate.context
    submission = candidate.submission
    intent = {
        "schema": BLOCK_CANDIDATE_INTENT_SCHEMA,
        "block_hash_hex": str(submission.block_hash_hex).lower(),
        "block_hex": _candidate_block_hex(candidate),
        "coinbase_tx_hex": str(getattr(submission, "coinbase_tx_hex", "")),
        "parent_hash": str(context.template["previousblockhash"]).lower(),
        "expected_height": int(context.template["height"]),
        "template": {
            "previousblockhash": context.template["previousblockhash"],
            "height": int(context.template["height"]),
            "coinbasevalue": int(context.template["coinbasevalue"]),
        },
        # Materialized to a plain list: a daemon-mirror share sequence parses
        # its dicts lazily, and the durable JSON boundary needs real objects.
        "shares_json": list(context.shares_json),
        "prior_balances": context.prior_balances,
        "found_block": context.found_block,
        "prospective_prior_balances": (
            [
                list(row)
                for row in getattr(
                    context,
                    "prospective_prior_balances",
                    (),
                )
            ]
            if getattr(context, "prospective_prior_balances", None) is not None
            else None
        ),
        "witness_merkle_leaves_hex": direct_stratum.witness_merkle_leaves_hex(
            getattr(context.job, "transaction_hexes", ())
        ),
        "extranonce1_hex": candidate.extranonce1_hex,
        "extranonce2_hex": candidate.extranonce2_hex,
        "username": context.worker.username,
        "pending_share": dataclasses.asdict(candidate.pending_share),
        "credit_share_on_accept": candidate.credit_share_on_accept,
        "collection_only": bool(context.collection_only),
    }
    # Fail on the client thread before committing a share if a future field
    # introduces a value that cannot survive the durable JSON boundary.
    json.dumps(intent, separators=(",", ":"), sort_keys=True)
    return intent


def _dequeued_candidate_collapse_row(
    candidate: PrismBlockCandidate,
    *,
    pool_block_exists: bool,
) -> dict[str, Any]:
    """Shape one dequeued candidate as the durable page row predicate S reads.

    The dequeue-time skip (issue #181 item 2) judges an in-memory candidate,
    not a fetched outbox row, so it has to present one. It deliberately does
    *not* call :func:`block_candidate_intent` to do that: that function
    re-serializes the whole block, recomputes every witness merkle leaf, and
    round-trips the result through ``json.dumps`` to validate it -- work
    proportional to the block, paid per dequeued sibling, to read four
    scalars. This builds only the fields ``_superseded_candidate_row``
    actually decodes.

    Every value here is copied from the same place the durable intent copies
    it from, so the row this returns and the row the outbox holds for the
    same candidate carry identical facts; a regression pins that field by
    field rather than trusting the restatement. No predicate input is
    coerced or defaulted: a candidate whose context cannot answer one of
    them raises, and the caller fails that candidate closed onto the
    ordinary offer path. ``job_id`` is the one exception, because it is not
    a predicate input at all -- it only groups the collapse log line.
    """
    context = candidate.context
    template = context.template
    return {
        "block_hash": str(candidate.submission.block_hash_hex).lower(),
        "candidate": {
            "block_hash_hex": str(candidate.submission.block_hash_hex).lower(),
            "parent_hash": str(template["previousblockhash"]).lower(),
            "expected_height": int(template["height"]),
            "template": {
                "previousblockhash": template["previousblockhash"],
                "height": int(template["height"]),
            },
            "found_block": context.found_block,
            "pending_share": {
                "job_id": getattr(candidate.pending_share, "job_id", ""),
            },
        },
        # Read durably by the caller, never assumed: clause 2 is the fact
        # that keeps an offered, landed candidate out of a terminal set.
        "pool_block_exists": bool(pool_block_exists),
    }


def block_candidate_from_intent(intent: dict[str, Any]) -> PrismBlockCandidate:
    """Decode and validate a durable candidate intent without side effects."""
    if not isinstance(intent, dict):
        raise TypeError("block candidate intent must be an object")
    if intent.get("schema") != BLOCK_CANDIDATE_INTENT_SCHEMA:
        raise ValueError("unsupported block candidate intent schema")
    block_hash = str(intent["block_hash_hex"]).lower()
    template = dict(intent["template"])
    if str(template.get("previousblockhash", "")).lower() != str(intent["parent_hash"]).lower():
        raise ValueError("block candidate parent hash does not match template")
    if int(template.get("height", -1)) != int(intent["expected_height"]):
        raise ValueError("block candidate height does not match template")
    submission = direct_stratum.DirectQbitSubmission(
        coinbase_tx_hex=str(intent["coinbase_tx_hex"]),
        coinbase_txid_preimage_hex="",
        header_hex="",
        block_hex=str(intent["block_hex"]),
        block_hash_hex=block_hash,
        block_hash_int=int(block_hash, 16),
        share_pass=True,
        block_pass=True,
        applied_version_hex="",
    )
    context = PrismJobContext(
        job=SimpleNamespace(
            transaction_hexes=(),
            witness_merkle_leaves_hex=tuple(
                intent.get("witness_merkle_leaves_hex", [])
            ),
        ),
        template=template,
        shares_json=list(intent["shares_json"]),
        prior_balances=list(intent["prior_balances"]),
        found_block=dict(intent["found_block"]),
        share_weight=0,
        collection_only=bool(intent.get("collection_only", False)),
        worker=WorkerIdentity(
            username=str(intent["username"]),
            payout_address="",
            worker_name=None,
            script_pubkey_hex="",
            p2mr_program_hex="",
        ),
        issued_at_ms=0,
        prospective_prior_balances=(
            tuple(
                (str(row[0]), str(row[1]), str(row[2]), int(row[3]))
                for row in intent["prospective_prior_balances"]
            )
            if isinstance(intent.get("prospective_prior_balances"), list)
            else None
        ),
        # Append-invalidation epochs are process-local counters, so a
        # stamp from the process that built this candidate is meaningless
        # after a restart. The negative sentinel tells the landing epoch
        # fence to stand down and instead revalidate the recorded window
        # against the durable ledger at its declared anchor (carry
        # balances do not move on a share append, so the prior-balance
        # fence alone cannot see an omitted late row).
        payout_append_invalidation_epoch=-1,
    )
    return PrismBlockCandidate(
        context=context,
        submission=submission,
        extranonce1_hex=str(intent["extranonce1_hex"]),
        extranonce2_hex=str(intent["extranonce2_hex"]),
        pending_share=PendingShare(**dict(intent["pending_share"])),
        client=SimpleNamespace(username=str(intent["username"])),
        credit_share_on_accept=bool(intent.get("credit_share_on_accept", False)),
    )


class BlockCandidateService:
    """Own the durable replay queue, retry ordering, and submitter lifecycle."""

    def __init__(
        self,
        ports: BlockCandidatePorts,
        *,
        candidate_queue: queue.Queue[PrismBlockCandidate] | None = None,
        retry_initial_seconds: float = DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS,
        retry_max_seconds: float = DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS,
        retryable_reasons: frozenset[str] = frozenset(),
        submit_seconds_buckets: tuple[float, ...] | None = None,
    ) -> None:
        self.ports = ports
        self._coordinator = ports.runtime
        self.candidate_queue = candidate_queue or queue.Queue(
            maxsize=MAX_PENDING_BLOCK_CANDIDATES
        )
        self.retry_initial_seconds = max(0.0, float(retry_initial_seconds))
        self.retry_max_seconds = max(
            self.retry_initial_seconds,
            float(retry_max_seconds),
        )
        self.retryable_reasons = frozenset(retryable_reasons)
        self.retry_delays: dict[str, float] = {}
        self.finalize_retries: dict[str, tuple[bool, str]] = {}
        self.retry_candidate: PrismBlockCandidate | None = None
        self.wakeups_coalesced = 0
        self.retries = 0
        self.poisoned = 0
        self.dropped = 0
        self.abandoned_counts: dict[str, int] = {}
        self.outcome = threading.local()
        self._state_lock = threading.RLock()
        self._backoff_started_monotonic: float | None = None
        self._backoff_deadline_monotonic: float | None = None
        self._backoff_delay_seconds = 0.0
        # Hashes of block candidates this process may still land (durable
        # outbox pending, queued, retained for retry, or mid-disposition).
        # Membership lets every tip-observation channel recognize the pool's
        # own block the moment it becomes the chain tip.
        self._outstanding_block_candidate_hashes: set[str] = set()
        # Outstanding candidate hashes observed as the chain tip
        # (hash -> monotonic stamp). qbitd only reports a candidate hash as
        # its tip after accepting that block, so an entry here is acceptance
        # evidence that outlives transient fork views and lost submitblock
        # acks; disposition/abandon paths consult it before treating any
        # instantaneous chain probe as terminal truth.
        self._tip_observed_accepted_block_hashes: dict[str, float] = {}
        # Durable cleanup can fail after a terminal decision and force the
        # same hash through that decision again; abandonment metrics count
        # candidates, not cleanup attempts. Retired by the terminal-outcome
        # eviction pass, which is the one place that has already proved a
        # hash unpinned by every live and cleanup-owing lane.
        self._counted_block_candidate_abandonments: set[str] = set()
        self._block_submit_metrics_lock = threading.Lock()
        if submit_seconds_buckets is None:
            submit_seconds_buckets = tuple(PRISM_JOB_BUILD_SECONDS_BUCKETS)
        # Landed candidate -> submitblock-return interval. Post-submit
        # bookkeeping (audit build, persistence, outbox finalize) is
        # deliberately excluded; the outbox created_at/completed_at span
        # already covers it.
        self.block_submit_seconds_histogram: dict[str, Any] = {
            "buckets": {bucket: 0 for bucket in submit_seconds_buckets},
            "sum": 0.0,
            "count": 0,
        }
        # Issue #181 item 3. Definitive node acceptance is learned on the
        # submitter thread and the preview is published on the accounting
        # thread, so the start stamp has to outlive the offer: the hash is
        # the only identity both sides share. A stamp of None is the
        # already-observed tombstone -- it keeps a re-offer of the same hash
        # from restarting an interval that has already been measured -- and
        # both the stamps and the histograms live under one leaf lock that
        # never nests, so an observation can be taken while the publication
        # path still holds the payout-balance mutation lock.
        self._accepted_block_preview_publication_lock = threading.Lock()
        self.accepted_block_preview_publication_seconds_histogram: dict[
            str, dict[str, Any]
        ] = {
            result: {
                "buckets": {
                    bucket: 0
                    for bucket in (
                        PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_SECONDS_BUCKETS
                    )
                },
                "sum": 0.0,
                "count": 0,
            }
            for result in PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS
        }
        self._accepted_block_preview_acceptance_monotonic: dict[
            str, float | None
        ] = {}
        self._block_replay_enumeration_owed_flag = False
        # Targeted ancestor re-drive state (issue #190), all under
        # coordinator.lock. A found block whose finalization keeps deferring
        # on the same unresolved accepted-ancestor payout transition arms a
        # forced durable-replay pass for that ancestor after a configured
        # deferral streak; the counters feed the qbit_prism_accepted_parent_
        # redrive_* metrics. One record per ancestor plus one child-keyed
        # blocking index, each insertion-ordered by recency for eviction.
        self._ancestor_redrive_records: dict[str, _AncestorRedriveRecord] = {}
        self._ancestor_redrive_last_blocking: dict[str, str] = {}
        self.accepted_parent_redrive_attempt_count = 0
        self.accepted_parent_redrive_resolved_count = 0
        self.accepted_parent_redrive_exhausted_count = 0
        # Fixed-key selector/apply outcome counters for the decided-height
        # collapse. The key set is closed so the rendered series carries no
        # block hash, parent hash, or job ID in a label.
        self._block_candidate_collapse_counts: dict[str, int] = {
            outcome: 0 for outcome in PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES
        }
        self._block_candidate_collapse_fail_closed_logged_monotonic: (
            float | None
        ) = None
        # Terminal hashes whose collapse cleanup did not complete, keyed by
        # hash so the backlog is bounded by the affected rows themselves and
        # a repeated failure re-registers rather than accumulating.
        self._block_candidate_collapse_cleanup_retries: dict[
            str,
            _CollapsedCandidateCleanup,
        ] = {}

    # -- twelve historical field aliases -----------------------------------

    @property
    def _retry_block_candidate(self) -> PrismBlockCandidate | None:
        """Historical attribute spelling used by the retry-merge plumbing."""
        return self.retry_candidate

    @_retry_block_candidate.setter
    def _retry_block_candidate(self, value: PrismBlockCandidate | None) -> None:
        self.retry_candidate = value

    # -- replayed-candidate credit floor -----------------------------------

    def adopt_replayed_candidate(self, candidate: PrismBlockCandidate) -> None:
        """Adopt a decoded credit-bearing candidate's snapshot-floor holder.

        A below-target candidate can credit its older accepted stamp after
        durable replay. Registering the reconstructed PendingShare on the S3
        pending floor before the candidate becomes visible to job issuance
        keeps startup prewarm anchored below that stamp.
        """
        if candidate.credit_share_on_accept:
            self.ports.share_writer().adopt_pending_share(candidate.pending_share)

    def _release_dropped_duplicate_candidate_floor(
        self,
        candidate: PrismBlockCandidate,
    ) -> None:
        """Release a duplicate-dropped credit candidate's floor holder.

        Same-hash duplicates are distinct objects (each durable-replay decode
        adopts a freshly reconstructed PendingShare; each live stamp registers
        its own), and the pending floor keys holders by object identity, so
        the object whose disposition actually lands releases only its own
        holder. Dropping a credit-bearing duplicate without this release
        pins the job/payout snapshot anchor below its stamp until restart.
        Non-credit candidates are exempt: their holder belongs to the
        share-append commit path, which may still be in flight.
        """
        if getattr(candidate, "credit_share_on_accept", False):
            self._coordinator._finish_pending_share_candidate(
                candidate.pending_share
            )

    # -- queue admission ---------------------------------------------------

    def enqueue(self, candidate: PrismBlockCandidate) -> bool:
        # Outstanding from admission (even when the wakeup coalesces below:
        # the durable outbox row keeps the candidate replayable), so a tip
        # observation can register acceptance before the submitter drains it.
        self._register_outstanding_block_candidate(
            str(candidate.submission.block_hash_hex)
        )
        queue_obj = self.candidate_queue
        if queue_obj is None:
            queue_obj = queue.Queue(maxsize=MAX_PENDING_BLOCK_CANDIDATES)
            self.candidate_queue = queue_obj
        try:
            queue_obj.put_nowait(candidate)
            return True
        except queue.Full:
            # The candidate is already durable. A full queue merely coalesces
            # this wakeup; the submitter re-reads pending outbox rows whenever
            # it drains the queue, so no candidate is discarded.
            with self._coordinator.lock:
                self.wakeups_coalesced = int(self.wakeups_coalesced) + 1
            print(
                "prism coordinator: block candidate wakeup coalesced "
                f"hash={candidate.submission.block_hash_hex} (submitter queue full)",
                flush=True,
            )
            return False

    # -- startup enumeration gate (#120) -----------------------------------

    def _note_block_replay_enumeration_owed(self) -> None:
        """Mark that pending durable candidates have not been enumerated yet."""
        with self._coordinator.lock:
            self._block_replay_enumeration_owed_flag = True

    def _clear_block_replay_enumeration_owed(self) -> None:
        with self._coordinator.lock:
            self._block_replay_enumeration_owed_flag = False

    def _block_replay_enumeration_owed(self) -> bool:
        with self._coordinator.lock:
            return bool(self._block_replay_enumeration_owed_flag)

    def _run_startup_block_candidate_replay(self) -> bool:
        """Run best-effort pre-accept replay without dying on a slow ledger."""
        # Until the durable outbox has been enumerated once, this process
        # cannot know whether a pending accepted candidate exists. Child job
        # builds fail closed on that uncertainty instead of snapshotting a
        # payout base that may omit a pending parent's carry (issue #188);
        # replay_pending_block_candidates clears the flag on success.
        self._coordinator._note_block_replay_enumeration_owed()
        try:
            return self._coordinator._run_startup_writer_replay(
                self._coordinator.replay_pending_block_candidates
            )
        except TimeoutError:
            print(
                "prism coordinator: startup block candidate replay timed out "
                "phase=replay-outbox-query "
                f"timeout={self._coordinator._block_landing_db_timeout():g}s; "
                "continuing startup; block submitter loop will retry and "
                "job builds stay blocked until pending candidates are known",
                flush=True,
            )
            return True

    # -- durable replay and quarantine -------------------------------------

    def _ensure_block_replay_state(self) -> None:
        """Backfill replay/maintenance queues for lightweight coordinators."""
        with _STATE_BACKFILL_LOCK:
            if not hasattr(self, "_block_replay_candidate_queue"):
                self._block_replay_candidate_queue = queue.Queue()
            if not hasattr(self, "_block_replay_inflight_hashes"):
                self._block_replay_inflight_hashes: set[str] = set()
            if not hasattr(self, "_block_quarantine_queue"):
                self._block_quarantine_queue = queue.Queue()
            if not hasattr(self, "_block_quarantine_hashes"):
                self._block_quarantine_hashes: set[str] = set()

    def _enqueue_replayed_block_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Queue one durable replay behind live solves, once per process."""
        self._ensure_block_replay_state()
        block_hash = str(candidate.submission.block_hash_hex).lower()
        with self._coordinator.lock:
            duplicate = (
                block_hash in self._block_replay_inflight_hashes
                or block_hash
                in getattr(self, "_block_candidate_terminal_outcomes", {})
                # A hash a single-slot retry holder still carries has a live
                # in-memory copy whose durable row is deliberately still
                # pending. Before the #190 re-drive, the retry short-circuit
                # kept enumeration from ever seeing such a row mid-retention;
                # a forced pass reaches it, and adopting it would install a
                # second in-process copy (and re-arm its payout barrier)
                # under the holder's. Skipping is safe: the row stays durable
                # and pending, and a later pass re-reads it once the holder
                # lets go. Only the holders qualify -- the wider lane
                # registries (the outstanding set above all) name every
                # admitted hash, and skipping those would break the
                # queue-overflow recovery this enumeration exists for.
                or block_hash in self._held_block_candidate_retry_hashes()
            )
            if not duplicate:
                self._block_replay_inflight_hashes.add(block_hash)
        if duplicate:
            # The instance decode already adopted this object's credit floor
            # holder; the earlier same-hash flight owns the disposition and
            # releases only its own holder.
            self._release_dropped_duplicate_candidate_floor(candidate)
            return False
        try:
            # This is an in-memory condition update, not accounting. Install
            # it before making the candidate visible to the raw lane so
            # startup prewarm cannot build a child from the old payout base
            # after qbitd accepts but before the RPC response returns.
            self._coordinator._begin_accepted_block_payout_preview(
                block_hash,
                block_height=int(candidate.context.template["height"]),
            )
            self._block_replay_candidate_queue.put_nowait(candidate)
        except BaseException:
            with self._coordinator.lock:
                self._block_replay_inflight_hashes.discard(block_hash)
            raise
        return True

    def _queue_invalid_block_candidate_for_quarantine(
        self,
        block_hash: str,
        error: str,
        *,
        pending_share: PendingShare | None = None,
    ) -> None:
        """Move malformed-row cleanup off the node-offer lane."""
        if not block_hash:
            return
        self._ensure_block_replay_state()
        key = block_hash.lower()
        with self._coordinator.lock:
            if key in self._block_quarantine_hashes:
                return
            self._block_quarantine_hashes.add(key)
        self._block_quarantine_queue.put_nowait((key, error, pending_share))

    def _adopt_durable_block_candidate_rows(
        self,
        durable_rows: list[Any],
    ) -> int:
        """Decode and queue one fetched batch, quarantining malformed rows.

        Shared by both enumeration shapes (keyset pages and the legacy
        widening window) so a fallback pass restores rows through exactly
        the same decode, dedupe, and poison path.
        """
        queued = 0
        self._coordinator._record_block_submitter_phase("replay-restore")
        for durable_row in durable_rows:
            durable_block_hash = ""
            # Published to poison cleanup only after its durable credit
            # holder was adopted successfully (the instance decode adopts
            # the credit floor before the candidate is visible anywhere).
            decoded_candidate: PrismBlockCandidate | None = None
            try:
                if not isinstance(durable_row, dict):
                    raise ValueError("durable block candidate row is not an object")
                durable_block_hash = str(durable_row["block_hash"]).lower()
                intent = durable_row["candidate"]
                if not isinstance(intent, dict):
                    raise ValueError("durable block candidate intent is not an object")
                intent_block_hash = str(intent.get("block_hash_hex", "")).lower()
                if not durable_block_hash or intent_block_hash != durable_block_hash:
                    raise ValueError("durable block candidate row key does not match intent")
                decoded_candidate = dataclass_replace(
                    self._coordinator.block_candidate_from_intent(intent),
                    durable_replay=True,
                )
                # Durable acceptance-state reads stay in accounting. The
                # separate replay queue keeps these recovered rows behind any
                # live solve while still exposing the whole batch to qbitd.
                if self._coordinator._enqueue_replayed_block_candidate(
                    decoded_candidate
                ):
                    queued += 1
            except Exception:
                print("prism coordinator: invalid durable block candidate intent", flush=True)
                traceback.print_exc()
                self._coordinator._queue_invalid_block_candidate_for_quarantine(
                    durable_block_hash,
                    "invalid durable candidate intent",
                    pending_share=(
                        decoded_candidate.pending_share
                        if decoded_candidate is not None
                        and decoded_candidate.credit_share_on_accept
                        else None
                    ),
                )
        return queued

    # -- decided-height collapse (#183) ------------------------------------

    def _ensure_block_candidate_collapse_state(self) -> None:
        """Backfill the collapse counters and cleanup-retry registry."""
        if (
            hasattr(self, "_block_candidate_collapse_counts")
            and hasattr(self, "_block_candidate_collapse_cleanup_retries")
            and hasattr(self, "_block_candidate_collapse_cleanup_inflight")
            and hasattr(self, "_block_candidate_cleanup_backpressure_engagements")
        ):
            return
        with _STATE_BACKFILL_LOCK:
            if not hasattr(self, "_block_candidate_collapse_counts"):
                self._block_candidate_collapse_counts: dict[str, int] = {
                    outcome: 0
                    for outcome in PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES
                }
            if not hasattr(self, "_block_candidate_collapse_cleanup_retries"):
                self._block_candidate_collapse_cleanup_retries: dict[
                    str,
                    _CollapsedCandidateCleanup,
                ] = {}
            if not hasattr(self, "_block_candidate_collapse_cleanup_inflight"):
                # A retry moves its record here for the duration of the
                # attempt. It must remain part of both the admission depth
                # and the terminal-outcome pin set while the accounting lane
                # is running cleanup outside coordinator.lock.
                self._block_candidate_collapse_cleanup_inflight: dict[
                    str,
                    _CollapsedCandidateCleanup,
                ] = {}
            if not hasattr(self, "_block_candidate_cleanup_backpressure_engagements"):
                # Occasions on which the admission bound withheld at least
                # one row from bulk terminalization, and when the last
                # bounded warning was printed. Both under coordinator.lock.
                self._block_candidate_cleanup_backpressure_engagements = 0
                self._block_candidate_cleanup_backpressure_logged_monotonic: (
                    float | None
                ) = None

    def _record_block_candidate_collapse(
        self,
        outcome: str,
        count: int = 1,
    ) -> None:
        if outcome not in PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES:
            raise ValueError(
                f"unknown block candidate collapse outcome: {outcome}"
            )
        if count <= 0:
            return
        self._ensure_block_candidate_collapse_state()
        with self._coordinator.lock:
            counts = self._block_candidate_collapse_counts
            counts[outcome] = int(counts.get(outcome, 0)) + int(count)

    def block_candidate_collapse_snapshot(self) -> dict[str, int]:
        """Copied fixed-key collapse counters for metrics rendering.

        The key set is closed and carries no hash, parent, or job ID, so the
        renderer can label by outcome without any unbounded label space.
        """
        self._ensure_block_candidate_collapse_state()
        with self._coordinator.lock:
            counts = dict(self._block_candidate_collapse_counts)
        return {
            outcome: int(counts.get(outcome, 0))
            for outcome in PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES
        }

    def _block_candidate_collapse_evidence(
        self,
        hashes: Iterable[object],
        *,
        ignore_leases: frozenset[str] = frozenset(),
    ) -> frozenset[str]:
        """Which of ``hashes`` some evidence says were offered to qbitd, or are being.

        Answers membership for the hashes the caller is actually deciding --
        one durable page during selection, the leased subset during
        revalidation -- instead of materializing the union of every registry.
        The registries this reads are not all page-sized:
        ``_block_candidate_terminal_outcomes`` is historical state that grows
        with every block the process ever disposed (312,000 entries after one
        testnet4 storm), and copying it per poll cost ~124 ms with
        ``coordinator.lock`` held against every unrelated share ack. Probing
        the page's own hashes keeps the cost O(page * registries) and makes
        it independent of how much disposition history has accumulated.

        Deliberately excludes ``_outstanding_block_candidate_hashes`` and
        ``_block_replay_inflight_hashes``. Both mean "this process still owns
        eventual disposition", not "a node offer happened": live admission
        marks a candidate outstanding before it even reaches the bounded
        queue, and replay adoption marks every restored row inflight, so
        folding either one in excludes an entire storm and makes the whole
        selector a safe no-op. ``attempt_count`` is excluded for the mirror
        reason: it is stamped only *after* the node offer, so at the instant
        that matters it proves nothing.

        ``ignore_leases`` lets the pre-write revalidation re-read this
        evidence without the apply's own disposition leases disqualifying
        the very rows they were claimed for.

        Every registry read here is keyed by the canonical lowercase hash the
        rest of the submitter compares on -- the same spelling
        ``_collapse_block_hash`` folds a page row to -- which is what lets a
        membership probe replace the old lowercasing walk without changing
        which hashes count as evidence.
        """
        coordinator = self._coordinator
        coordinator._ensure_job_cache_state()
        self._ensure_block_candidate_disposition_state()
        probes: set[str] = set()
        for value in hashes:
            key = _collapse_block_hash(value)
            if key is not None:
                probes.add(key)
        if not probes:
            return frozenset()
        evidence: set[str] = set()
        with self._block_submitter_lock(
            self._block_candidate_disposition_registry_lock,
            "candidate-disposition-registry",
        ):
            flights = self._block_candidate_disposition_flights
            evidence.update(
                key
                for key in probes
                if key in flights and key not in ignore_leases
            )
        with coordinator.lock:
            for holder in (
                self.retry_candidate,
                getattr(self, "_block_accounting_deferred_retry_candidate", None),
            ):
                if holder is None:
                    continue
                held = _collapse_block_hash(
                    getattr(
                        getattr(holder, "submission", None),
                        "block_hash_hex",
                        None,
                    )
                )
                if held is not None and held in probes:
                    evidence.add(held)
            for registry in (
                self._block_disposition_waiting_retries,
                self.finalize_retries,
                getattr(self, "_block_candidate_retained_node_submissions", None),
                self._tip_observed_accepted_block_hashes,
                getattr(coordinator, "_accounted_accepted_block_hashes", None),
                self._block_candidate_terminal_outcomes,
            ):
                if not registry:
                    continue
                evidence.update(key for key in probes if key in registry)
        return frozenset(evidence)

    def _collapse_pool_block_exists(self, durable_row: object) -> bool:
        """Read one row's durable pool-block fact, or fail the page closed.

        A missing key is not "no pool block": it is a page that cannot
        answer the one fact keeping an offered, landed candidate out of a
        terminal set. Reading it as false would abandon exactly the rows
        that must never be abandoned, so the whole page fails closed. The
        fenced batch write re-asks under the writer fence regardless.
        """
        if (
            not isinstance(durable_row, dict)
            or "pool_block_exists" not in durable_row
        ):
            raise _BlockCandidateCollapseFailedClosed(
                "durable page row carries no pool block existence fact"
            )
        exists = durable_row["pool_block_exists"]
        if not isinstance(exists, bool):
            raise _BlockCandidateCollapseFailedClosed(
                "durable page row pool block existence is not a boolean"
            )
        return exists

    def _superseded_candidate_row(
        self,
        durable_row: object,
    ) -> _SupersededCandidateRow:
        """Reduce one page row to S's inputs, or fail its page closed.

        The preserved page still reaches ordinary decode/quarantine, but no
        sibling may be terminalized from a snapshot containing a malformed
        candidate fact.
        """
        if not isinstance(durable_row, dict):
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate row is not an object"
            )
        block_hash = _collapse_block_hash(durable_row.get("block_hash"))
        intent = durable_row.get("candidate")
        if block_hash is None or not isinstance(intent, dict):
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate row carries no usable hash or intent"
            )
        if _collapse_block_hash(intent.get("block_hash_hex")) != block_hash:
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate row key does not match its intent"
            )
        parent_hash = _collapse_block_hash(intent.get("parent_hash"))
        height = _collapse_row_height(durable_row)
        if parent_hash is None or height is None or height < 0:
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate intent carries no usable parent or height"
            )
        template = intent.get("template")
        if not isinstance(template, dict):
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate intent carries no template"
            )
        # The decode path refuses an intent whose template disagrees with
        # its own parent/height fields. Reading either half of a
        # disagreeing pair here would compare the chain against a fact the
        # candidate does not actually carry.
        if _collapse_block_hash(template.get("previousblockhash")) != parent_hash:
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate template parent disagrees with its intent"
            )
        if _collapse_height(template.get("height")) != height:
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate template height disagrees with its intent"
            )
        found_block = intent.get("found_block")
        if not isinstance(found_block, dict):
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate intent carries no found-block facts"
            )
        # PRISM-scaled units; see COLLAPSE_DIFFICULTY_SCALE and
        # _BlockCandidateChainView.difficulty_of for the chain-side half of
        # the comparison this feeds.
        network_difficulty = _collapse_difficulty(
            found_block.get("network_difficulty")
        )
        if network_difficulty is None:
            raise _BlockCandidateCollapseFailedClosed(
                "durable candidate intent carries no usable network difficulty"
            )
        pending_share = intent.get("pending_share")
        job_id = (
            str(pending_share.get("job_id", ""))
            if isinstance(pending_share, dict)
            else ""
        )
        return _SupersededCandidateRow(
            block_hash=block_hash,
            parent_hash=parent_hash,
            height=height,
            network_difficulty=network_difficulty,
            job_id=job_id,
        )

    def _select_superseded_block_candidates(
        self,
        durable_rows: list[Any],
        chain: _BlockCandidateChainView,
        *,
        ignore_leases: frozenset[str] = frozenset(),
    ) -> list[_SupersededCandidateRow]:
        """Apply predicate S to one fetched page of durable pending rows.

        Every clause is conjunctive and every unknown is a rejection. The
        chain view bounds the cost: one best-tip and one tip-height read for
        the page, one active-block and one header read per distinct
        candidate height, and at most
        MAX_BLOCK_CANDIDATE_COLLAPSE_HEIGHT_PROBES distinct heights for the
        whole walk. A row whose height the walk cannot afford to probe is
        counted and skipped, which preserves it for the per-row path exactly
        as any other rejected clause does.

        ``ignore_leases`` names disposition flights the *caller itself*
        holds, exactly as it does for the pre-write revalidation: those
        flights are not treated as offer evidence, and every other member of
        the evidence set still rejects. The replay-adoption page path passes
        nothing and is unchanged; the dequeue-time skip (issue #181 item 2)
        passes the single hash whose lease ``submit_next`` claimed before it
        called here, because that flight is this pass and this pass has
        made no node offer. See
        ``_skip_superseded_block_candidate_at_dequeue`` for why no other
        flight can hide behind it.
        """
        # Clause 2 is read first, for the whole page, before a single chain
        # round trip: a page that cannot answer it selects nothing at all,
        # and a ledger whose page reader never carries the fact (a
        # compatibility intent-only reader) must not spend a tip read per
        # poll to discover that again. The list is page-bounded.
        pool_block_facts = [
            self._collapse_pool_block_exists(durable_row)
            for durable_row in durable_rows
        ]
        # Evidence is asked about this page's hashes only. A malformed row
        # contributes no probe and is failed closed by
        # ``_superseded_candidate_row`` below regardless; every row that
        # survives that decode carries the row key probed here, because the
        # decode refuses an intent whose hash disagrees with it.
        evidence = self._block_candidate_collapse_evidence(
            (
                durable_row.get("block_hash")
                for durable_row in durable_rows
                if isinstance(durable_row, dict)
            ),
            ignore_leases=ignore_leases,
        )
        tip = chain.best_tip()
        tip_height = chain.tip_height()
        selected: list[_SupersededCandidateRow] = []
        seen: set[str] = set()
        height_deferred = 0
        for pool_block_exists, durable_row in zip(pool_block_facts, durable_rows):
            row = self._superseded_candidate_row(durable_row)
            if row.block_hash in seen:
                continue
            seen.add(row.block_hash)
            if pool_block_exists:
                continue
            if row.block_hash in evidence:
                continue
            # Clause 3: a candidate building on the current best tip is the
            # next block, not a superseded sibling.
            if row.parent_hash == tip:
                continue
            # Clause 4: only a height the chain has already decided, decided
            # by somebody else.
            if row.height > tip_height:
                continue
            active = chain.active_at(row.height)
            if active is None:
                # The walk has spent its bounded height probes. Nothing is
                # known about this height, so nothing may be concluded about
                # this row: it stays durable, keeps its evidence, and reaches
                # the per-row path this pass and the next walk's collapse
                # after that.
                height_deferred += 1
                continue
            if active == row.block_hash:
                continue
            # Clause 4b/5: a lower-work occupant is not a decision. The
            # per-row path can still reorg a heavier sibling into the chain,
            # and a terminal batch write would destroy that block. Both
            # sides are PRISM-scaled integers: the row as the coordinator
            # stamped it, the occupant as re-derived from its header bits.
            if chain.difficulty_of(active) < row.network_difficulty:
                continue
            selected.append(row)
        self._record_block_candidate_collapse("height_deferred", height_deferred)
        return selected

    def _revalidate_superseded_block_candidates(
        self,
        leased: list[_SupersededCandidateRow],
        chain: _BlockCandidateChainView,
    ) -> tuple[list[_SupersededCandidateRow], str]:
        """Re-read the chain under the held leases, immediately before the write.

        Selection ran before any lease existed, so a selected candidate
        could have been offered, accepted, and become the active block in
        the gap. Re-reading the best tip and each retained height's occupant
        with the leases held closes it: nothing else can drive this hash
        through a node offer while the lease is ours, and the fenced write
        is the very next thing that happens.

        The occupant's header is re-read only when the occupying hash
        changed; an unchanged occupant keeps the work value this walk
        already read for that hash, so the steady case adds no header round
        trip.
        """
        # Budgeted for exactly the heights selection already probed and no
        # more. The leased set is a subset of one bounded selection, so this
        # can never refuse a re-read the write depends on, and it cannot draw
        # on the walk's budget either -- the walk's remaining probes are for
        # pages this apply has not seen. A budget of its own is a cache of
        # its own as well: the walk's active-height reads, which may have
        # been made pages ago, are never reused here, so every leased height
        # is genuinely re-read from the chain under the held leases.
        fresh = _BlockCandidateChainView(
            self,
            probe_budget=_CollapseHeightProbeBudget(
                remaining=len({row.height for row in leased}),
            ),
        )
        tip = fresh.best_tip()
        leased_hashes = frozenset(row.block_hash for row in leased)
        evidence = self._block_candidate_collapse_evidence(
            leased_hashes,
            ignore_leases=leased_hashes,
        )
        qualified: list[_SupersededCandidateRow] = []
        for row in leased:
            if row.block_hash in evidence:
                continue
            if row.parent_hash == tip or row.block_hash == tip:
                continue
            # An occupied height is by construction at or below the tip, so
            # this read subsumes the height bound as well as clause 4b.
            active = fresh.active_at(row.height)
            if active is None or active == row.block_hash:
                # Unreachable while the budget above matches the leased
                # heights, and a drop either way: an unprobed height is a row
                # this write must not carry.
                continue
            # Same clause, same units as selection: a cache hit reuses the
            # scaled integer that pass derived, and a changed occupant is
            # re-derived from its own header bits rather than compared as a
            # raw float against the row's scaled value.
            difficulty = chain.cached_difficulty(active)
            if difficulty is None:
                difficulty = fresh.difficulty_of(active)
            if difficulty < row.network_difficulty:
                continue
            qualified.append(row)
        return qualified, tip

    def _note_block_candidate_collapse_fail_closed(
        self,
        rows: int,
        detail: object,
    ) -> bool:
        """Count a fail-closed page and log it at a bounded rate.

        Returns whether this occasion logged, so a caller holding an
        exception can attach its traceback under the same rate limit
        instead of flooding a persistently degraded read.
        """
        self._record_block_candidate_collapse("fail_closed", rows)
        now = time.monotonic()
        with self._coordinator.lock:
            last = getattr(
                self,
                "_block_candidate_collapse_fail_closed_logged_monotonic",
                None,
            )
            due = (
                last is None
                or (now - float(last))
                >= BLOCK_CANDIDATE_COLLAPSE_FAIL_CLOSED_LOG_SECONDS
            )
            if due:
                self._block_candidate_collapse_fail_closed_logged_monotonic = now
        if due:
            print(
                "prism coordinator: superseded block candidate collapse failed "
                f"closed rows={rows}: {detail}; every row keeps the per-row "
                "path",
                flush=True,
            )
        return due

    def _collapsed_candidate_floor_holders(
        self,
        abandoned: Iterable[str],
    ) -> dict[str, list[PrismBlockCandidate]]:
        """Index the in-memory candidate objects owning each collapsed hash.

        The pending-share floor keys holders by object identity, so only the
        exact object a lane is holding may be released and never a same-hash
        sibling's. A durable row that was never adopted owns no object and
        no holder, and none is invented for it: decoding one just to release
        it would adopt a fresh floor holder in order to drop it.
        """
        wanted = frozenset(abandoned)
        holders: dict[str, list[PrismBlockCandidate]] = {}
        if not wanted:
            return holders
        self._ensure_block_replay_state()
        for queue_obj in (
            self.candidate_queue,
            self._block_replay_candidate_queue,
        ):
            if queue_obj is None:
                continue
            with queue_obj.mutex:
                queued = list(queue_obj.queue)
            for item in queued:
                key = _collapse_block_hash(
                    getattr(
                        getattr(item, "submission", None),
                        "block_hash_hex",
                        None,
                    )
                )
                if key in wanted:
                    holders.setdefault(key, []).append(item)
        return holders

    def _run_collapsed_candidate_cleanup_steps(
        self,
        abandoned: tuple[str, ...],
        *,
        owed: dict[str, frozenset[str]],
        shares: dict[str, tuple[PendingShare, ...]] | None = None,
    ) -> tuple[dict[str, frozenset[str]], dict[str, tuple[PendingShare, ...]]]:
        """Run each hash's owed cleanup steps and report what it still owes.

        Every step is idempotent and independently guarded, so a hash whose
        step raises cannot strand the rest of the page mid-way, and a step
        the caller does not list is simply not run -- which is what lets a
        deferred retry replay only the remainder.

        Returns the still-owed steps per hash (absent means fully clean) and
        the floor holders this pass resolved, so a caller that has to defer
        the remainder keeps the exact holder objects it would otherwise lose
        when the queues drain.
        """
        coordinator = self._coordinator
        coordinator._record_block_submitter_phase("replay-collapse-cleanup")
        remaining: dict[str, set[str]] = {}
        detailed_failures = 0

        def note_failure(block_hash: str, step: str) -> None:
            nonlocal detailed_failures
            detailed = detailed_failures < BLOCK_CANDIDATE_COLLAPSE_LOG_FAILURES
            remaining.setdefault(block_hash, set()).add(step)
            if not detailed:
                return
            detailed_failures += 1
            print(
                "prism coordinator: collapsed block candidate cleanup failed "
                f"step={step} hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()

        # One outer acquisition for the whole page. Each withdrawal takes
        # this lock anyway (it is re-entrant); acquiring it per hash would
        # hand a storm-sized page thousands of chances to interleave with
        # descendant payout work between two halves of one hash's cleanup.
        with self._block_submitter_lock(
            coordinator._payout_balance_mutation_lock,
            "payout-balance-mutation",
        ):
            for block_hash in abandoned:
                if (
                    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP
                    not in owed.get(block_hash, frozenset())
                ):
                    continue
                try:
                    # Withdraw with published invalidation, then drop the
                    # tombstone: the durable row is already terminal, so no
                    # replay source is left for the tombstone to fence, and
                    # a retained one is exactly the preview wait storm this
                    # collapse exists to end.
                    coordinator._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    coordinator._clear_accepted_block_payout_preview(block_hash)
                except Exception:
                    note_failure(
                        block_hash,
                        BLOCK_CANDIDATE_COLLAPSE_CLEANUP_PAYOUT_STEP,
                    )
        outcome = SimpleNamespace(
            reason=PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON,
            stale_job_class=PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS,
        )
        if shares is None:
            # Unguarded on purpose: a page-scope indexing fault proves
            # nothing about any hash's remaining steps, so the caller must
            # see it as an abort and defer the whole won set.
            shares = {
                block_hash: tuple(
                    candidate.pending_share for candidate in candidates
                )
                for block_hash, candidates in (
                    self._collapsed_candidate_floor_holders(abandoned).items()
                )
            }
        for block_hash in abandoned:
            steps = owed.get(block_hash, frozenset())

            def run_step(step: str, action: Callable[[], None]) -> None:
                if step not in steps:
                    return
                try:
                    action()
                except Exception:
                    note_failure(block_hash, step)

            def clear_finalize_retry() -> None:
                with coordinator.lock:
                    self.finalize_retries.pop(block_hash, None)

            run_step("finalize-retry", clear_finalize_retry)
            run_step(
                "retry-state",
                lambda: coordinator._clear_block_candidate_retry_state(block_hash),
            )
            # Stops matching tip observations and drops the tip observation
            # stamp in the same critical section.
            run_step(
                "outstanding-and-tip-observation",
                lambda: coordinator._discard_outstanding_block_candidate(block_hash),
            )
            # Also drops the fast-lane reservation, replay-inflight marker,
            # and any parked same-hash retry object's own floor holder.
            run_step(
                "terminal-outcome",
                lambda: coordinator._record_block_candidate_terminal_outcome(
                    block_hash,
                    accepted=False,
                ),
            )
            run_step(
                "abandonment-accounting",
                lambda: coordinator._record_committed_block_candidate_abandonment(
                    block_hash,
                    outcome,
                ),
            )
            for pending_share in shares.get(block_hash, ()):
                run_step(
                    BLOCK_CANDIDATE_COLLAPSE_CLEANUP_FLOOR_STEP,
                    lambda pending_share=pending_share: (
                        coordinator._finish_pending_share_commit(pending_share)
                    ),
                )
        return (
            {
                block_hash: frozenset(steps)
                for block_hash, steps in remaining.items()
            },
            shares,
        )

    def _publish_collapsed_candidate_terminal_fence(
        self,
        abandoned: Iterable[str],
    ) -> None:
        """Fence every won hash from node submission before any cleanup runs.

        The fenced batch write is the moment a row becomes durably terminal,
        and the ``terminal-outcome`` cleanup step is what normally publishes
        that fact in memory. That step can fail, or a page-level abort can
        stop the pass before it -- and the failure leaves the hash in the
        cleanup-retry registry while a same-hash candidate may still be
        sitting in the live, replay, retry, or waiting lane. Once the apply
        releases its disposition lease that candidate is dequeued, finds no
        terminal outcome, and offers a durably abandoned block to the node.

        Publishing the outcome here closes that window with the fence the
        rest of the submitter already honours. The apply holds every won
        hash's disposition lease from before the write until after this
        call, so no same-hash lane can be inside its own guarded region
        while this runs, and none can enter one afterwards without seeing
        the fence. It is a lock-guarded assignment into a dict the
        disposition state already owns: no durable read or write, no queue
        admission, and nothing that could re-adopt or re-offer a row.

        The full ``terminal-outcome`` step still runs in cleanup for its
        other side effects -- the fast-lane reservation, the replay-inflight
        marker, and the parked same-hash retry's own floor holder -- and
        remains owed, and retried, whenever it fails.

        Contained so it cannot raise: this sits between a won write and the
        page partition, and an exception escaping here would fail the page
        open and replay-adopt rows whose durable outbox entries are gone.
        """
        try:
            self._ensure_block_candidate_disposition_state()
            published: set[str] = set()
            with self._coordinator.lock:
                outcomes = self._block_candidate_terminal_outcomes
                for block_hash in abandoned:
                    self._stamp_block_candidate_terminal_outcome(
                        outcomes,
                        block_hash,
                        False,
                    )
                    published.add(block_hash)
                self._bound_block_candidate_terminal_outcomes(
                    frozenset(published)
                )
        except Exception:
            # Degrades to the pre-publication behaviour: the cleanup step
            # still owes the outcome and the retry registry still carries it.
            print(
                "prism coordinator: collapsed block candidate terminal fence "
                "could not be published",
                flush=True,
            )
            traceback.print_exc()

    def _clean_up_collapsed_block_candidates(
        self,
        abandoned: tuple[str, ...],
    ) -> frozenset[str]:
        """Mirror per-row terminal abandonment cleanup for the rows we won.

        Driven by the hashes the fenced write returned, never by the hashes
        it was asked about: a requested-but-absent row was won by somebody
        else (or already terminal), and running this cleanup for it would
        discard state its real owner still needs.

        Returns the hashes whose cleanup did not complete. The apply owns
        the ``cleanup_failed`` accounting for the whole page: counting a
        contained failure here and a later page-level abort there would
        report one affected hash twice. Each of those hashes keeps its
        still-owed steps in the cleanup-retry registry, because its durable
        row is terminal and no enumeration will ever hand it back. Their
        terminal fence is already published by the apply, so a step this
        pass leaves owed can never leave one of them offerable to the node.
        """
        every_step = frozenset(BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS)
        remaining, shares = self._run_collapsed_candidate_cleanup_steps(
            abandoned,
            owed={block_hash: every_step for block_hash in abandoned},
        )
        for block_hash in abandoned:
            owed = remaining.get(block_hash)
            if owed:
                self._defer_collapsed_candidate_cleanup(
                    block_hash,
                    owed,
                    shares=shares.get(block_hash, ()),
                )
            else:
                # A hash the fenced write can only win once, but a direct
                # caller may repeat the cleanup; a completed pass discharges
                # whatever an earlier one left owed.
                self._discharge_collapsed_candidate_cleanup(block_hash)
        if remaining:
            print(
                "prism coordinator: collapsed block candidate cleanup failed "
                f"rows={len(remaining)} of {len(abandoned)}; their durable rows "
                "are already terminal and their cleanup is retried",
                flush=True,
            )
        return frozenset(remaining)

    # -- deferred cleanup retry --------------------------------------------

    def _collapsed_candidate_cleanup_registry(
        self,
    ) -> dict[str, _CollapsedCandidateCleanup]:
        self._ensure_block_candidate_collapse_state()
        return self._block_candidate_collapse_cleanup_retries

    def _defer_collapsed_candidate_cleanup(
        self,
        block_hash: str,
        steps: Iterable[str],
        *,
        shares: Iterable[PendingShare] = (),
        shares_resolved: bool = True,
    ) -> None:
        """Keep one terminal hash's owed cleanup steps for a later retry.

        Contained so it cannot raise: the apply's abort path calls this on
        its way to returning the won set, and an exception escaping there
        would fail the whole page closed and replay-adopt rows whose durable
        outbox entries are already gone.
        """
        try:
            owed = frozenset(steps).intersection(
                BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS
            )
            if not owed:
                return
            registry = self._collapsed_candidate_cleanup_registry()
            now = time.monotonic()
            with self._coordinator.lock:
                record = registry.pop(block_hash, None)
                if record is None:
                    record = _CollapsedCandidateCleanup(
                        block_hash=block_hash,
                        steps=owed,
                        shares=tuple(shares),
                        shares_resolved=bool(shares_resolved),
                        delay_seconds=max(0.0, float(self.retry_initial_seconds)),
                        # The first attempt is due immediately; only a failed
                        # attempt starts the backoff.
                        not_before_monotonic=now,
                        deferred_monotonic=now,
                    )
                else:
                    # The floor keys holders by object identity, so merge by
                    # identity rather than by value: two reconstructed shares
                    # for one hash are two distinct holders.
                    by_identity = {id(share): share for share in record.shares}
                    for share in shares:
                        by_identity.setdefault(id(share), share)
                    record.steps = record.steps | owed
                    record.shares = tuple(by_identity.values())
                    record.shares_resolved = record.shares_resolved and bool(
                        shares_resolved
                    )
                    record.not_before_monotonic = min(
                        record.not_before_monotonic,
                        now,
                    )
                    if not record.deferred_monotonic:
                        record.deferred_monotonic = now
                # Re-inserted at the tail so a storm-sized backlog is retried
                # round-robin instead of starving behind its oldest entry.
                registry[block_hash] = record
        except Exception:
            print(
                "prism coordinator: collapsed block candidate cleanup could "
                f"not be deferred hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()

    def _discharge_collapsed_candidate_cleanup(self, block_hash: str) -> bool:
        """Drop one hash's retry record; True when one was still owed."""
        registry = self._collapsed_candidate_cleanup_registry()
        with self._coordinator.lock:
            return registry.pop(block_hash, None) is not None

    def collapsed_candidate_cleanup_backlog(self) -> dict[str, frozenset[str]]:
        """The cleanup steps each terminal hash still owes (diagnostics)."""
        registry = self._collapsed_candidate_cleanup_registry()
        with self._coordinator.lock:
            combined: dict[str, frozenset[str]] = {}
            for source in (
                registry,
                self._block_candidate_collapse_cleanup_inflight,
            ):
                for block_hash, record in source.items():
                    combined[block_hash] = (
                        combined.get(block_hash, frozenset()) | record.steps
                    )
            return combined

    # -- cleanup-retry backlog bound (#198) --------------------------------

    def _collapse_cleanup_retry_backlog_max(self) -> int:
        """Retry records the backlog may hold before admission stops.

        Read from the coordinator attribute first, so an embedder or a test
        can pin it the way the sibling accounting knobs are pinned, then
        from the loaded ``BlockConfig``, then the shipped default. Clamp it
        into the same safe range enforced by startup validation. A direct
        runtime value below one degrades into "admit one row at a time while
        the backlog is empty"; one above the fence registry's capacity can
        never weaken the hard memory bound.
        """
        coordinator = self._coordinator
        value = getattr(
            coordinator,
            "block_candidate_cleanup_retry_backlog_max",
            None,
        )
        if value is None:
            block_config = getattr(getattr(coordinator, "config", None), "block", None)
            value = getattr(
                block_config,
                "candidate_cleanup_retry_backlog_max",
                None,
            )
        if value is None:
            value = DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX
        return min(
            MAX_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX,
            max(1, int(value)),
        )

    def _collapse_cleanup_admission_headroom(self) -> tuple[int, int, int]:
        """(rows the collapse may still admit, backlog depth, bound).

        Only the collapse callers on the block-submitter thread add records.
        The accounting lane moves one record into the in-flight registry
        while retrying it, so the owed authority remains counted until the
        attempt either discharges or re-registers it.
        """
        registry = self._collapsed_candidate_cleanup_registry()
        maximum = self._collapse_cleanup_retry_backlog_max()
        with self._coordinator.lock:
            depth = len(registry) + len(
                self._block_candidate_collapse_cleanup_inflight
            )
        return max(0, maximum - depth), depth, maximum

    def collapsed_candidate_cleanup_backlog_snapshot(self) -> dict[str, Any]:
        """Fixed-key gauges over the cleanup-retry backlog, for metrics.

        The key set is closed and carries no hash or step name. The walk is
        O(backlog) under ``coordinator.lock``, which the admission bound
        keeps at a few thousand entries at most; every value is a copy, so
        the renderer never holds a reference into the registry.

        ``terminal_outcome_pins`` counts the records whose hash currently
        holds a published terminal-outcome fence -- the entries the
        registry forbids the fence eviction from dropping. It equals the
        depth whenever the apply's fence publication succeeded; a smaller
        value names records whose fence is still owed to the
        ``terminal-outcome`` step.
        """
        self._ensure_block_candidate_collapse_state()
        self._ensure_block_candidate_disposition_state()
        maximum = self._collapse_cleanup_retry_backlog_max()
        now = time.monotonic()
        with self._coordinator.lock:
            registry = self._block_candidate_collapse_cleanup_retries
            inflight = self._block_candidate_collapse_cleanup_inflight
            outcomes = self._block_candidate_terminal_outcomes
            records = tuple(registry.values()) + tuple(inflight.values())
            depth = len(records)
            holder_ids: set[int] = set()
            pinned_hashes: set[str] = set()
            oldest: float | None = None
            for record in records:
                holder_ids.update(id(share) for share in record.shares)
                if record.block_hash in outcomes:
                    pinned_hashes.add(record.block_hash)
                stamp = float(record.deferred_monotonic)
                if oldest is None or stamp < oldest:
                    oldest = stamp
            engagements = int(
                self._block_candidate_cleanup_backpressure_engagements
            )
        return {
            "depth": depth,
            "backlog_max": maximum,
            "oldest_age_seconds": (
                -1.0 if oldest is None else max(0.0, now - oldest)
            ),
            "pending_share_holders": len(holder_ids),
            "terminal_outcome_pins": len(pinned_hashes),
            "backpressure_active": depth >= maximum,
            "backpressure_engagements": engagements,
        }

    def _note_block_candidate_cleanup_backpressure(
        self,
        *,
        caller: str,
        rows: int,
        admitted: int,
        depth: int,
        maximum: int,
    ) -> None:
        """Count rows the admission bound preserved and warn at a bounded rate.

        Every field of the warning is drawn from a closed vocabulary or is a
        count: the caller name, the rows preserved, the rows this same pass
        still admitted, the backlog depth and bound at the decision, and the
        oldest record's age. No hash, parent, page cursor, or step name ever
        appears, so a storm-long engagement cannot grow the log line's shape
        and a persistent one cannot flood the journal.
        """
        if caller not in PRISM_BLOCK_CANDIDATE_CLEANUP_BACKPRESSURE_CALLERS:
            raise ValueError(f"unknown cleanup backpressure caller: {caller}")
        if rows <= 0:
            return
        self._record_block_candidate_collapse("backlog_deferred", rows)
        now = time.monotonic()
        with self._coordinator.lock:
            self._block_candidate_cleanup_backpressure_engagements = (
                int(self._block_candidate_cleanup_backpressure_engagements) + 1
            )
            last = self._block_candidate_cleanup_backpressure_logged_monotonic
            due = (
                last is None
                or (now - float(last))
                >= BLOCK_CANDIDATE_CLEANUP_BACKPRESSURE_LOG_SECONDS
            )
            if due:
                self._block_candidate_cleanup_backpressure_logged_monotonic = now
        if not due:
            return
        oldest_age = float(
            self.collapsed_candidate_cleanup_backlog_snapshot()["oldest_age_seconds"]
        )
        print(
            "prism coordinator: collapsed block candidate cleanup backpressure "
            f"engaged caller={caller} rows_preserved={rows} admitted={admitted} "
            f"backlog={depth} backlog_max={maximum} "
            f"oldest_seconds={oldest_age:.3f}; admitting them to bulk "
            "terminalization would take the cleanup-retry backlog past its "
            "bound, so they stay durable and pending for the per-row path "
            "until it drains",
            flush=True,
        )

    def _run_one_collapsed_block_candidate_cleanup_retry(self) -> bool:
        """Retry one deferred collapse cleanup; True when one was attempted.

        Driven from the accounting lane, because a terminal hash's cleanup
        is accounting work and the durable row behind it is already gone.
        The lane offers it two ways: immediately whenever the lane is truly
        idle, and otherwise on an explicit cadence after at most
        ``_block_accounting_cleanup_retry_work_items()`` completed work
        items, so neither sustained accounting traffic nor a continuously
        replenished quarantine queue can starve a due record. The pass is
        bounded three ways: at most one hash per call, only that hash's
        still-owed steps, and only once its own backoff deadline has passed.
        It never reads or writes the outbox and never enqueues a candidate,
        so it can only finish tearing state down -- a terminal row can be
        neither re-adopted nor re-offered from here.
        """
        registry = self._collapsed_candidate_cleanup_registry()
        inflight = self._block_candidate_collapse_cleanup_inflight
        now = time.monotonic()
        with self._coordinator.lock:
            due: _CollapsedCandidateCleanup | None = None
            for record in registry.values():
                if (
                    record.block_hash not in inflight
                    and record.not_before_monotonic <= now
                ):
                    due = record
                    break
            if due is None:
                return False
            # Move, rather than drop, the authority record for the duration
            # of the attempt. Admission, gauges, and terminal-outcome
            # eviction all count the in-flight registry, so cleanup can run
            # outside coordinator.lock without opening an undercount or an
            # unfenced window. A second lane also skips the same hash.
            registry.pop(due.block_hash, None)
            inflight[due.block_hash] = due
            due.attempts += 1
        attempt_returned = False
        try:
            result = self._attempt_collapsed_block_candidate_cleanup_retry(due)
            attempt_returned = True
            return result
        finally:
            with self._coordinator.lock:
                retained = inflight.pop(due.block_hash, None)
                if (
                    not attempt_returned
                    and retained is not None
                    and due.block_hash not in registry
                ):
                    # An unexpected exception outside the contained step
                    # runner must not discard the only cleanup authority.
                    registry[due.block_hash] = retained

    def _attempt_collapsed_block_candidate_cleanup_retry(
        self,
        due: _CollapsedCandidateCleanup,
    ) -> bool:
        """Run one claimed retry while its authority remains in-flight."""
        self._coordinator._record_block_submitter_phase(
            "replay-collapse-cleanup-retry"
        )
        block_hash = due.block_hash
        owed = due.steps
        shares = due.shares
        shares_resolved = due.shares_resolved
        if (
            BLOCK_CANDIDATE_COLLAPSE_CLEANUP_FLOOR_STEP in owed
            and not shares_resolved
        ):
            # The apply aborted before it could index the floor holders. The
            # scan reads only the in-memory queues, so repeating it is cheap
            # and safe; a candidate already dropped from them released its
            # own holder on the way out.
            try:
                holders = self._collapsed_candidate_floor_holders((block_hash,))
            except Exception:
                print(
                    "prism coordinator: collapsed block candidate cleanup "
                    f"retry could not index floor holders hash={block_hash}",
                    flush=True,
                )
                traceback.print_exc()
                self._record_block_candidate_collapse("cleanup_retry_failed")
                self._reschedule_collapsed_candidate_cleanup(
                    due,
                    owed,
                    shares=shares,
                    shares_resolved=False,
                )
                return True
            by_identity = {id(share): share for share in shares}
            for candidate in holders.get(block_hash, ()):
                by_identity.setdefault(
                    id(candidate.pending_share),
                    candidate.pending_share,
                )
            shares = tuple(by_identity.values())
            shares_resolved = True
        try:
            remaining, _shares = self._run_collapsed_candidate_cleanup_steps(
                (block_hash,),
                owed={block_hash: owed},
                shares={block_hash: shares},
            )
            still_owed = remaining.get(block_hash, frozenset())
        except Exception:
            # Same reasoning as the apply's abort: a pass that died proves
            # nothing about the steps it skipped, so the hash owes them all
            # again.
            print(
                "prism coordinator: collapsed block candidate cleanup retry "
                f"aborted hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()
            still_owed = owed
        if still_owed:
            self._record_block_candidate_collapse("cleanup_retry_failed")
            self._reschedule_collapsed_candidate_cleanup(
                due,
                still_owed,
                shares=shares,
                shares_resolved=shares_resolved,
            )
            return True
        # Counted once per hash: the record is gone, so no later pass can
        # report the same recovery twice, and the series can never exceed
        # the cleanup_failed set that created these records.
        self._record_block_candidate_collapse("cleanup_recovered")
        print(
            "prism coordinator: collapsed block candidate cleanup recovered "
            f"hash={block_hash} attempts={due.attempts}",
            flush=True,
        )
        return True

    def _reschedule_collapsed_candidate_cleanup(
        self,
        record: _CollapsedCandidateCleanup,
        steps: Iterable[str],
        *,
        shares: Iterable[PendingShare],
        shares_resolved: bool,
    ) -> None:
        """Re-register a retried hash behind its own doubled backoff.

        The pacing mirrors ``next_retry_delay``: the configured candidate
        retry backoff, doubled per attempt and capped, so a systemically
        broken cleanup cannot spin the accounting lane while a transient one
        still recovers on its next tick.
        """
        initial = max(0.0, float(self.retry_initial_seconds))
        maximum = max(initial, float(self.retry_max_seconds))
        delay = min(maximum, max(initial, float(record.delay_seconds) * 2))
        self._defer_collapsed_candidate_cleanup(
            record.block_hash,
            steps,
            shares=shares,
            shares_resolved=shares_resolved,
        )
        registry = self._collapsed_candidate_cleanup_registry()
        with self._coordinator.lock:
            rescheduled = registry.get(record.block_hash)
            if rescheduled is None:
                return
            rescheduled.attempts = max(rescheduled.attempts, record.attempts)
            rescheduled.delay_seconds = delay
            rescheduled.not_before_monotonic = time.monotonic() + delay
            # The retry took the record out of the registry before the
            # attempt, so the re-registration above minted a fresh stamp;
            # the age gauge measures from the first deferral, not the
            # latest failure.
            if record.deferred_monotonic:
                rescheduled.deferred_monotonic = min(
                    rescheduled.deferred_monotonic or record.deferred_monotonic,
                    record.deferred_monotonic,
                )

    def _log_collapsed_block_candidates(
        self,
        qualified: list[_SupersededCandidateRow],
        abandoned: frozenset[str],
        *,
        considered: int,
        selected: int,
        lease_skipped: int,
        revalidation_dropped: int,
    ) -> None:
        """Emit one bounded summary instead of one line per candidate."""
        groups: dict[tuple[str, str], list[str]] = {}
        for row in qualified:
            if row.block_hash not in abandoned:
                continue
            groups.setdefault((row.parent_hash, row.job_id[:64]), []).append(
                row.block_hash
            )
        print(
            "prism coordinator: collapsed superseded block candidates "
            f"considered={considered} selected={selected} "
            f"lease_skipped={lease_skipped} "
            f"revalidation_dropped={revalidation_dropped} "
            f"abandoned={len(abandoned)} "
            f"reason={PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON} "
            f"stale_job_class={PRISM_BLOCK_CANDIDATE_COLLAPSE_STALE_JOB_CLASS} "
            f"groups={len(groups)}",
            flush=True,
        )
        ordered = sorted(groups.items(), key=lambda item: (-len(item[1]), item[0]))
        for (parent_hash, job_id), hashes in (
            ordered[:BLOCK_CANDIDATE_COLLAPSE_LOG_GROUPS]
        ):
            sample = ",".join(
                sorted(hashes)[:BLOCK_CANDIDATE_COLLAPSE_LOG_SAMPLE_HASHES]
            )
            print(
                "prism coordinator: collapsed candidate group "
                f"parent={parent_hash} job={job_id} "
                f"reason={PRISM_BLOCK_CANDIDATE_COLLAPSE_REASON} "
                f"count={len(hashes)} sample={sample}",
                flush=True,
            )
        remainder = len(ordered) - BLOCK_CANDIDATE_COLLAPSE_LOG_GROUPS
        if remainder > 0:
            print(
                "prism coordinator: collapsed candidate groups not shown "
                f"count={remainder}",
                flush=True,
            )

    def _apply_superseded_block_candidate_collapse(
        self,
        selected: list[_SupersededCandidateRow],
        chain: _BlockCandidateChainView,
        *,
        page_rows: int,
        timeout_seconds: float | None,
        call_class: str,
        held_leases: frozenset[str] = frozenset(),
    ) -> frozenset[str]:
        """Lease, revalidate, write once, then clean up exactly what we won.

        ``held_leases`` names hashes whose disposition lease the caller
        already holds and will release itself. They are neither claimed nor
        released here, and they count as leased for every step that follows
        -- including the pre-write revalidation's own self-exemption, which
        already ignores the leases this apply is operating under. The
        replay-adoption page path passes nothing and behaves exactly as
        before; the dequeue-time skip passes the one hash ``submit_next``
        leased before it dequeued anything.
        """
        coordinator = self._coordinator
        coordinator._record_block_submitter_phase("replay-collapse-lease")
        leases: dict[str, _BlockCandidateDispositionLease] = {}
        try:
            for row in selected:
                if row.block_hash in held_leases:
                    continue
                # Never block: a synchronous submit that has persisted its
                # intent but not yet claimed its own lease must not queue
                # behind a page of maintenance work.
                lease = coordinator._claim_block_candidate_disposition(
                    row.block_hash,
                    blocking=False,
                )
                if lease is not None:
                    leases[row.block_hash] = lease
            leased = [
                row
                for row in selected
                if row.block_hash in leases or row.block_hash in held_leases
            ]
            self._record_block_candidate_collapse(
                "lease_skipped",
                len(selected) - len(leased),
            )
            if not leased:
                return frozenset()
            coordinator._record_block_submitter_phase("replay-collapse-revalidate")
            try:
                qualified, tip = self._revalidate_superseded_block_candidates(
                    leased,
                    chain,
                )
            except Exception as exc:
                logged = self._note_block_candidate_collapse_fail_closed(
                    page_rows,
                    exc,
                )
                if logged and not isinstance(
                    exc,
                    _BlockCandidateCollapseFailedClosed,
                ):
                    traceback.print_exc()
                return frozenset()
            self._record_block_candidate_collapse(
                "revalidation_dropped",
                len(leased) - len(qualified),
            )
            if not qualified:
                return frozenset()
            hashes = tuple(row.block_hash for row in qualified)
            error = f"tip moved before submit: {tip}"
            coordinator._record_block_submitter_phase("replay-collapse-write")
            try:
                mark = coordinator.ledger.mark_block_candidates_abandoned
                returned = coordinator._run_block_submitter_ledger_call(
                    ("collapse-superseded", hashes),
                    "collapse-superseded",
                    lambda: mark(block_hashes=hashes, error=error),
                    timeout_seconds=timeout_seconds,
                    call_class=call_class,
                )
            except Exception as exc:
                # The rows stay pending, so nothing is lost: the whole page
                # is preserved and the per-row path disposes of it. A
                # timed-out call may still land on its bounded worker; the
                # rows it transitions simply replay as terminal, which is
                # the same convergence every other outbox mutation has.
                if self._note_block_candidate_collapse_fail_closed(page_rows, exc):
                    traceback.print_exc()
                return frozenset()
            if not isinstance(returned, (list, tuple, set, frozenset)):
                # The contract is a hash set, not a count or a boolean. A
                # ledger that answers with something else has told us
                # nothing about which rows it won.
                self._note_block_candidate_collapse_fail_closed(
                    page_rows,
                    "fenced batch abandonment did not return a hash set",
                )
                return frozenset()
            requested = frozenset(hashes)
            abandoned = frozenset(
                key
                for key in (
                    _collapse_block_hash(value) for value in (returned or ())
                )
                # A returned hash outside the request would mean the ledger
                # transitioned a row this apply never leased; refuse to run
                # cleanup for it rather than tear down somebody else's state.
                if key is not None and key in requested
            )
            self._record_block_candidate_collapse(
                "write_lost",
                len(requested - abandoned),
            )
            if not abandoned:
                return frozenset()
            self._record_block_candidate_collapse("abandoned", len(abandoned))
            # Before any cleanup, and while every won hash's lease is still
            # held: a cleanup that fails or aborts must not leave a durably
            # terminal row offerable to the node.
            self._publish_collapsed_candidate_terminal_fence(abandoned)
            try:
                cleanup_failed = self._clean_up_collapsed_block_candidates(
                    tuple(
                        row.block_hash
                        for row in qualified
                        if row.block_hash in abandoned
                    )
                )
            except Exception:
                # The rows are durably terminal whatever happened here, so
                # the caller must still partition them out of the page: a
                # cleanup fault cannot be allowed to replay-adopt a row
                # whose outbox entry is gone. An abort proves nothing about
                # the steps it skipped, so the whole won set is affected --
                # which subsumes any hash a contained step already failed.
                cleanup_failed = abandoned
                # Every won hash owes every step again, and the floor
                # holders were never indexed, so the retry re-runs that scan
                # too. Deferral is contained, so this stays on the path that
                # returns the won set.
                for block_hash in abandoned:
                    self._defer_collapsed_candidate_cleanup(
                        block_hash,
                        BLOCK_CANDIDATE_COLLAPSE_CLEANUP_STEPS,
                        shares_resolved=False,
                    )
                print(
                    "prism coordinator: collapsed block candidate cleanup "
                    f"aborted rows={len(abandoned)}; their durable rows are "
                    "already terminal and their cleanup is retried",
                    flush=True,
                )
                traceback.print_exc()
            if cleanup_failed:
                # Counted once per apply over distinct hashes. Contained
                # per-step failures and a later abort describe the same
                # rows, so this series can never exceed the won set.
                self._record_block_candidate_collapse(
                    "cleanup_failed",
                    len(cleanup_failed),
                )
            try:
                self._log_collapsed_block_candidates(
                    qualified,
                    abandoned,
                    considered=page_rows,
                    selected=len(selected),
                    lease_skipped=len(selected) - len(leased),
                    revalidation_dropped=len(leased) - len(qualified),
                )
            except Exception:
                # Diagnostics are not cleanup state. Keep a formatter fault
                # visible without claiming hashes whose terminal cleanup
                # completed successfully.
                print(
                    "prism coordinator: collapsed block candidate logging "
                    f"failed rows={len(abandoned)}",
                    flush=True,
                )
                traceback.print_exc()
            return abandoned
        finally:
            for lease in leases.values():
                coordinator._release_block_candidate_disposition(lease)

    def _collapse_superseded_block_candidates(
        self,
        durable_rows: list[Any],
        *,
        timeout_seconds: float | None,
        call_class: str,
        probe_budget: _CollapseHeightProbeBudget | None = None,
    ) -> list[Any]:
        """Terminalize this page's decided-height siblings before adoption.

        Returns the rows that still have to be adopted: exactly the page
        minus the hashes the fenced write actually transitioned. A won row's
        fetched payload is a stale copy of an intent whose durable row is
        now terminal, so it must never be replay-adopted or re-arm a payout
        barrier. Anything short of a won row -- a fail-closed read, an
        unclaimable lease, a revalidation drop, an unprobed height, a partial
        return, a failed write -- preserves the row and leaves it to the
        per-row path.

        ``probe_budget`` is the enumeration walk's shared chain-height
        budget, carrying both the walk's remaining probes and the reads it
        has already made; passes given none get a fresh one of their own. A
        spent budget ends this page only when the page has nothing at a
        height the walk already read: the counter bounds new heights, and a
        cached height costs nothing to answer from however late in the walk
        it is asked.
        """
        if not durable_rows:
            return durable_rows
        if not callable(
            getattr(
                self._coordinator.ledger,
                "mark_block_candidates_abandoned",
                None,
            )
        ):
            # A ledger with no fenced batch abandonment has no safe bulk
            # form at all. That is a structural absence, not a fail-closed
            # read, so it neither counts nor spends a chain round trip: the
            # per-row path disposes of every row exactly as before.
            return durable_rows
        if (
            probe_budget is not None
            and probe_budget.exhausted
            and not any(
                _collapse_row_height(durable_row) in probe_budget.active
                for durable_row in durable_rows
            )
        ):
            # This walk has spent its chain-height budget on earlier pages
            # *and* not one row here sits at a height those pages already
            # read. A spent counter only forbids reading a new height; a
            # height already in the walk's cache stays answerable for free,
            # and a page holding such rows still qualifies them below. This
            # page holds none, so selection could not qualify a single row
            # and the two page-scope tip reads it opens with would be two
            # more round trips for a pass that is already decided. Every row
            # is preserved.
            #
            # The peek is deliberately lenient: a row it cannot read a height
            # from contributes no cached height, which at worst preserves a
            # page the decode would have failed closed on anyway -- exactly
            # what the unconditional early return did for every such page.
            self._record_block_candidate_collapse(
                "height_deferred",
                len(durable_rows),
            )
            return durable_rows
        headroom, backlog_depth, backlog_max = (
            self._collapse_cleanup_admission_headroom()
        )
        if headroom <= 0:
            # Issue #198. The cleanup-retry backlog is at its bound: every
            # row the fenced write could win here would add one more record
            # that only the accounting lane can ever drain. Nothing is
            # selected and no chain read is spent; every row stays durable
            # and pending for the per-row path, exactly as an unprobed
            # height leaves it. Rows already terminal keep their records,
            # holders, and fences untouched -- this bounds admission, never
            # cleanup authority.
            self._note_block_candidate_cleanup_backpressure(
                caller="replay-page",
                rows=len(durable_rows),
                admitted=0,
                depth=backlog_depth,
                maximum=backlog_max,
            )
            return durable_rows
        self._coordinator._record_block_submitter_phase("replay-collapse-select")
        self._record_block_candidate_collapse("considered", len(durable_rows))
        chain = _BlockCandidateChainView(self, probe_budget=probe_budget)
        try:
            selected = self._select_superseded_block_candidates(
                durable_rows,
                chain,
            )
        except Exception as exc:
            logged = self._note_block_candidate_collapse_fail_closed(
                len(durable_rows),
                exc,
            )
            if logged and not isinstance(exc, _BlockCandidateCollapseFailedClosed):
                traceback.print_exc()
            return durable_rows
        if not selected:
            return durable_rows
        self._record_block_candidate_collapse("selected", len(selected))
        if len(selected) > headroom:
            # Predicate S is unchanged: every one of these rows satisfied
            # it and is counted as selected. Admission is what is bounded:
            # the fenced write may win at most ``headroom`` more rows before
            # the backlog reaches its bound, so only that many, in page
            # order, are handed on. The remainder is preserved, not
            # disposed of.
            self._note_block_candidate_cleanup_backpressure(
                caller="replay-page",
                rows=len(selected) - headroom,
                admitted=headroom,
                depth=backlog_depth,
                maximum=backlog_max,
            )
            selected = selected[:headroom]
        try:
            abandoned = self._apply_superseded_block_candidate_collapse(
                selected,
                chain,
                page_rows=len(durable_rows),
                timeout_seconds=timeout_seconds,
                call_class=call_class,
            )
        except Exception as exc:
            if self._note_block_candidate_collapse_fail_closed(
                len(durable_rows),
                exc,
            ):
                traceback.print_exc()
            return durable_rows
        if not abandoned:
            return durable_rows
        return [
            durable_row
            for durable_row in durable_rows
            if _collapse_block_hash(
                durable_row.get("block_hash")
                if isinstance(durable_row, dict)
                else None
            )
            not in abandoned
        ]

    # -- dequeue-time stale sibling skip (issue #181 item 2) ----------------

    def _block_candidate_dequeue_chain(self) -> _BlockCandidateChainView:
        """A chain view over the submitter's own tip-epoch height cache.

        The replay walk shares one ``_CollapseHeightProbeBudget`` across its
        pages so a storm at one decided height costs the walk one
        ``getblockhash`` and one ``getblockheader`` however many pages it
        spans. A dequeue burst has exactly that shape -- hundreds of
        siblings of one decided height, arriving one candidate at a time --
        so it shares a budget the same way, and for the same reason: without
        it every sibling would re-read the occupant and its header.

        The cache is retired by :meth:`_retire_block_candidate_dequeue_chain`
        the moment the best tip changes, so its lifetime is exactly one tip
        epoch. Only the height caches are shared: the tip itself is memoized
        per view, so every candidate's selection still runs against a
        freshly read best tip, and the pre-write revalidation builds its own
        budget and re-reads the occupant under the held lease regardless.

        Submitter-thread state, like the replay walk's budget: ``submit_next``
        is the only caller and the block-submitter thread is the only thread
        that reaches it.
        """
        budget = getattr(self, "_block_candidate_dequeue_probe_budget", None)
        if budget is None:
            budget = _CollapseHeightProbeBudget()
            self._block_candidate_dequeue_probe_budget = budget
            self._block_candidate_dequeue_probe_tip: str | None = None
        return _BlockCandidateChainView(self, probe_budget=budget)

    def _retire_block_candidate_dequeue_chain(self, tip: str) -> None:
        """Drop every height this cache read under an earlier best tip.

        The best tip names the whole active chain beneath it: a block at any
        height can only change by changing every descendant, so while the
        tip hash is unchanged every ``getblockhash(H)`` below it answers
        identically. Retiring exactly on a tip change therefore makes the
        cache valid for precisely as long as it is kept, and returns the
        bounded probe allowance so a long-lived submitter never stops
        probing new heights.

        That is a stronger statement than the walk's cross-page sharing
        needs, and the pre-write revalidation is unaffected either way: it
        builds its own budget and re-reads the occupant from the chain under
        the held lease before anything terminal happens.
        """
        if getattr(self, "_block_candidate_dequeue_probe_tip", None) == tip:
            return
        budget = self._block_candidate_dequeue_probe_budget
        budget.active.clear()
        budget.difficulty.clear()
        budget.remaining = MAX_BLOCK_CANDIDATE_COLLAPSE_HEIGHT_PROBES
        self._block_candidate_dequeue_probe_tip = tip

    def _skip_superseded_block_candidate_at_dequeue(
        self,
        candidate: PrismBlockCandidate,
        *,
        timeout_seconds: float | None = None,
        call_class: str = "fast",
    ) -> bool:
        """Terminalize one provably-stale dequeued candidate before any offer.

        Returns True when the durable row was abandoned and every piece of
        this candidate's in-memory state was torn down, so ``submit_next``
        must release its lease and consume the wakeup without offering
        anything. False means "offer it", for any reason at all: this is an
        optimisation over the per-row path and declining it is always safe.

        Why this exists (issue #181, the 2026-08-20 spike): a candidate
        whose parent is no longer the best tip and that carries no offer
        evidence is provably stale *before* the offer, and the per-row path
        spends one ``submitblock``, ~6 chain reads and two ledger writes
        discovering that -- plus an accounting task, a fast-lane
        reservation, and an accepted-block payout-preview barrier armed and
        withdrawn -- for each one. Those are the individual
        ``block candidate abandoned reason=stale-job: tip moved before
        submit`` bursts the 2026-08-21 validation still showed for
        live/fast-path siblings after #196 removed the replay population.

        The predicate is #196's, unchanged: same evidence set, same
        clauses, same fencing, same cleanup, same terminal write. This is a
        second *caller*, not a second notion of staleness. The one
        divergence is the self-exemption below.

        **The self-exemption, and why nothing else can hide behind it.**
        ``submit_next`` claims this hash's disposition lease before it gets
        here, so the hash is in ``_block_candidate_disposition_flights`` --
        a member of #196's evidence set E -- and #196's selector would
        reject the row on clause 1 forever. Exactly one flight is exempted:
        the one this pass holds the lock of, named by this candidate's own
        hash and passed as ``ignore_leases``/``held_leases``. No other
        flight can be exempted, because no other hash is ever in that set;
        and no other *holder* of this flight can hide behind the exemption,
        because a flight's registry entry is shared by its holder and its
        waiters while its lock has exactly one owner -- this pass. A pass
        merely waiting on the lock has offered nothing, and once the fenced
        write lands, ``_publish_collapsed_candidate_terminal_fence`` stamps
        the terminal outcome while the lease is still ours, so the waiter
        wakes into the fence rather than into an offer. Every other member
        of E still rejects this hash: retry holders, the deferred accounting
        retry, ``_block_disposition_waiting_retries``, ``finalize_retries``,
        ``_block_candidate_retained_node_submissions``,
        ``_tip_observed_accepted_block_hashes``,
        ``_accounted_accepted_block_hashes``,
        ``_block_candidate_terminal_outcomes``, and a ``qbit_pool_blocks``
        row in any state.

        **Chain, not observation set.** Replay adoption does not register a
        hash outstanding, so a blockwait observation of a replayed hash is
        dropped until dequeue and the in-memory tip view says nothing about
        it. Clause 3 is therefore decided against a freshly read
        ``getbestblockhash``, never against the coordinator's published tip.

        **Fail closed.** Any unreadable fact -- a chain read, the pool-block
        probe, an intent this candidate cannot answer, a fenced write that
        did not return this hash -- preserves the candidate for the offer
        path. Nothing is ever abandoned on an unknown.

        One diagnostic note: because this reuses #196's selection and apply
        verbatim, the submitter phase stamps those emit still read
        ``replay-collapse-*``. They name the machinery, which is shared, not
        the caller. The ``dequeue_considered``/``dequeue_skipped``/
        ``dequeue_preserved`` counters are what separate this caller's
        population from the replay walk's, and the one phase this method
        stamps itself -- ``dequeue-collapse-pool-block`` -- names the only
        durable read it adds.
        """
        coordinator = self._coordinator
        ledger = coordinator.ledger
        if not callable(getattr(ledger, "mark_block_candidates_abandoned", None)):
            # A ledger with no fenced batch abandonment has no safe bulk form
            # at all; a structural absence, so it neither counts nor spends a
            # round trip. Same reasoning as the page-level driver.
            return False
        pool_block_reader = getattr(ledger, "pool_block_state", None)
        if not callable(pool_block_reader):
            # Clause 2 has no durable answer here. Reading it as false would
            # abandon exactly the rows that must never be abandoned.
            return False
        block_hash = _block_candidate_hash_of(candidate)
        if block_hash is None:
            return False
        headroom, backlog_depth, backlog_max = (
            self._collapse_cleanup_admission_headroom()
        )
        if headroom <= 0:
            # Issue #198: same bound, same answer as the page walk. The
            # candidate is not considered at all -- the dequeue partition
            # counters stay exact -- and it takes the ordinary offer path,
            # whose per-row disposition has its own cleanup contract.
            self._note_block_candidate_cleanup_backpressure(
                caller="dequeue",
                rows=1,
                admitted=0,
                depth=backlog_depth,
                maximum=backlog_max,
            )
            return False
        held = frozenset((block_hash,))
        self._record_block_candidate_collapse("dequeue_considered")
        try:
            if self._block_candidate_collapse_evidence(held, ignore_leases=held):
                # Clause 1 first, because it is the only clause that costs
                # nothing: the selector re-asks it below, but asking here
                # keeps both the chain round trip and the durable pool-block
                # probe off every candidate some evidence already excludes.
                self._record_block_candidate_collapse("dequeue_preserved")
                return False
            parent_hash = _collapse_block_hash(
                candidate.context.template["previousblockhash"]
            )
            if parent_hash is None:
                raise _BlockCandidateCollapseFailedClosed(
                    "dequeued candidate carries no usable parent hash"
                )
            chain = self._block_candidate_dequeue_chain()
            tip = chain.best_tip()
            self._retire_block_candidate_dequeue_chain(tip)
            if parent_hash == tip:
                # Clause 3, short-circuited before any durable read: this is
                # the block waiting to be offered, not a superseded sibling.
                # Keeping it first is what holds the acceptance path's added
                # cost to a single getbestblockhash.
                #
                # That read is not replaceable by the coordinator's published
                # tip (``_current_published_tip_hash_locked``), tempting as a
                # free in-memory gate is. Published work is what a *blocked*
                # refresh stops updating, and a blocked refresh is #181's
                # symptom: during the incident the published tip sits at the
                # very parent every stale sibling names, so a gate on it
                # would decline to skip exactly the population this exists
                # for. The chain is asked instead, which is also what D5
                # requires for replay-adopted rows.
                self._record_block_candidate_collapse("dequeue_preserved")
                return False
            pool_block_exists = (
                coordinator._run_block_submitter_ledger_call(
                    ("dequeue-collapse-pool-block", block_hash),
                    "dequeue-collapse-pool-block",
                    lambda: pool_block_reader(block_hash=block_hash),
                    timeout_seconds=timeout_seconds,
                    call_class=call_class,
                )
                is not None
            )
            selected = self._select_superseded_block_candidates(
                [
                    _dequeued_candidate_collapse_row(
                        candidate,
                        pool_block_exists=pool_block_exists,
                    )
                ],
                chain,
                ignore_leases=held,
            )
        except Exception as exc:
            logged = self._note_block_candidate_collapse_fail_closed(1, exc)
            if logged and not isinstance(exc, _BlockCandidateCollapseFailedClosed):
                traceback.print_exc()
            self._record_block_candidate_collapse("dequeue_preserved")
            return False
        if not selected:
            self._record_block_candidate_collapse("dequeue_preserved")
            return False
        self._record_block_candidate_collapse("selected", len(selected))
        try:
            abandoned = self._apply_superseded_block_candidate_collapse(
                selected,
                chain,
                page_rows=1,
                timeout_seconds=timeout_seconds,
                call_class=call_class,
                held_leases=held,
            )
        except Exception as exc:
            if self._note_block_candidate_collapse_fail_closed(1, exc):
                traceback.print_exc()
            self._record_block_candidate_collapse("dequeue_preserved")
            return False
        if block_hash not in abandoned:
            # Anything short of a won row -- an unclaimable lease, a
            # revalidation drop, a lost fenced write -- is preserved.
            self._record_block_candidate_collapse("dequeue_preserved")
            return False
        # The floor holder is bound to the *queued object's* identity, and
        # this object has already left its lane, so the apply's own
        # pending-share step -- which indexes holders by scanning the live
        # and replay queues -- cannot see it. Release it here, through the
        # same seam ``submit_next`` uses for a dropped duplicate. Correct
        # whether the apply's cleanup completed or was deferred: a deferred
        # retry has no queue left to find this object in either.
        self._release_dropped_duplicate_candidate_floor(candidate)
        self._record_block_candidate_collapse("dequeue_skipped")
        return True

    def _block_replay_should_yield_to_live_candidates(
        self,
        probe_budget: _CollapseHeightProbeBudget,
    ) -> bool:
        """Whether this walk should hand the submitter back before another page.

        The block-submitter thread that walks these pages is the only thread
        that reaches ``submitblock``, so a backlog it keeps walking is a
        backlog that delays a live solve. Between pages -- never inside one
        page's fenced collapse, its write, or its adoption -- the walk gives
        that thread back as soon as two things hold at once:

        * a live candidate or a retry is waiting, so there is something
          strictly more valuable than finishing the enumeration; and
        * the walk has already spent its chain-height probes, so the pages it
          would go on to fetch can no longer decide a height it has not
          already read.

        Both halves matter. Yielding on a waiting candidate alone would cut
        short the same-height storm this path exists for, which walks several
        pages on a single probe. Yielding on a spent budget alone would leave
        the enumeration owed -- and job builds blocked -- for a walk that had
        nothing better to do than finish. The caller re-enumerates from the
        outbox on its next pass with a fresh budget, and every row this walk
        fetched was adopted before the yield, so the backlog still shrinks.

        A spent budget is not the same as nothing left to collapse: the pages
        past the yield may hold siblings at heights this walk already read,
        and those would have collapsed for free. What the yield gives up is
        therefore bounded and recovered -- those rows stay durable and pending,
        and the next walk re-probes their heights on a fresh budget -- while
        what it buys is the only thread that reaches ``submitblock``.
        """
        if not probe_budget.exhausted:
            return False
        with self._coordinator.lock:
            if self.retry_candidate is not None:
                return True
        queue_obj = self.candidate_queue
        return queue_obj is not None and not queue_obj.empty()

    # -- targeted ancestor re-drive (issue #190) ---------------------------

    def _ancestor_redrive_defer_threshold(self) -> int:
        return max(
            1,
            int(
                getattr(
                    self._coordinator,
                    "accepted_parent_redrive_defer_threshold",
                    DEFAULT_ACCEPTED_PARENT_REDRIVE_DEFER_THRESHOLD,
                )
            ),
        )

    def _ancestor_redrive_attempt_cap(self) -> int:
        """Re-drives one ancestor may trigger; zero disables the mechanism."""
        return max(
            0,
            int(
                getattr(
                    self._coordinator,
                    "accepted_parent_redrive_attempt_max",
                    DEFAULT_ACCEPTED_PARENT_REDRIVE_ATTEMPT_MAX,
                )
            ),
        )

    def _evict_stale_redrive_entries_locked(self) -> None:
        """Bound per-hash re-drive bookkeeping. Caller holds the runtime lock.

        Entries are dropped on resolution, so eviction only engages under a
        pathological stream of distinct never-resolving ancestors. The
        deferral path re-inserts its ancestor's record and its child's
        blocking entry on every deferral, so an actively wedged pair sits at
        the back of each registry and is the last thing an oldest-first
        eviction touches -- exhausted records included, whose survival is
        what keeps the per-ancestor cap a lifetime bound. The two registries
        are keyed by different hash spaces (ancestors vs children), so each
        is trimmed strictly against itself: a hash that is both a child and
        an ancestor must not lose its ancestor record to the eviction of a
        stale child entry.
        """
        while len(self._ancestor_redrive_records) > MAX_ANCESTOR_REDRIVE_TRACKED_HASHES:
            self._ancestor_redrive_records.pop(
                next(iter(self._ancestor_redrive_records)), None
            )
        while (
            len(self._ancestor_redrive_last_blocking)
            > MAX_ANCESTOR_REDRIVE_TRACKED_HASHES
        ):
            self._ancestor_redrive_last_blocking.pop(
                next(iter(self._ancestor_redrive_last_blocking)), None
            )

    def note_pending_parent_transition_deferral(
        self,
        block_hash: str,
        ancestor_hash: str,
    ) -> None:
        """Track one finalization deferral against its blocking ancestor.

        Called by the coordinator's pending-parent fence each time a
        candidate's finalization defers because ``ancestor_hash``'s accepted
        payout transition is unresolved. The deferral itself only re-checks
        the transition, so a streak of them proves the retry loop cannot
        resolve the ancestor on its own; on crossing the configured
        threshold a targeted durable-replay pass is armed for the submitter
        loop. Bounded per ancestor by the attempt cap, past which deferrals
        fall back to exactly the pre-#190 behavior with the
        publication-progress watchdog as the backstop.
        """
        cap = self._ancestor_redrive_attempt_cap()
        if cap <= 0:
            # Zero disables the mechanism outright: no streaks, no armed
            # passes, and no exhaustion accounting. A disabled install must
            # not emit the alert-worthy exhausted signal a spent cap means.
            return
        child = str(block_hash).lower()
        ancestor = str(ancestor_hash).lower()
        threshold = self._ancestor_redrive_defer_threshold()
        requested = False
        exhausted = False
        attempts = 0
        streak = 0
        self._ensure_block_replay_state()
        with self._coordinator.lock:
            # Re-inserted (not updated in place) so insertion order tracks
            # recency and the eviction above stays oldest-first.
            self._ancestor_redrive_last_blocking.pop(child, None)
            self._ancestor_redrive_last_blocking[child] = ancestor
            record = self._ancestor_redrive_records.pop(ancestor, None)
            if record is None:
                record = _AncestorRedriveRecord()
            self._ancestor_redrive_records[ancestor] = record
            # A replay-lane copy of the ancestor means in-process resolution
            # is already underway -- an earlier re-drive's adoption, or
            # ordinary replay. Deferrals during that window prove nothing a
            # new pass could act on (it would no-op on the adopted check),
            # so they must not bank toward attempts: burning the cap against
            # a mechanism that is mid-fix would exhaust it before slow-but-
            # working finalization completes, standing the re-drive down for
            # an ancestor it had already reached.
            ancestor_inflight = ancestor in self._block_replay_inflight_hashes
            if not record.armed and not record.exhausted and not ancestor_inflight:
                record.streak += 1
                streak = record.streak
                if record.streak >= threshold:
                    if record.attempts < cap:
                        record.attempts += 1
                        record.armed = True
                        record.streak = 0
                        attempts = record.attempts
                        self.accepted_parent_redrive_attempt_count = (
                            int(self.accepted_parent_redrive_attempt_count) + 1
                        )
                        requested = True
                    else:
                        record.exhausted = True
                        attempts = record.attempts
                        self.accepted_parent_redrive_exhausted_count = (
                            int(self.accepted_parent_redrive_exhausted_count) + 1
                        )
                        exhausted = True
            self._evict_stale_redrive_entries_locked()
        if requested:
            print(
                "prism coordinator: arming in-process ancestor re-drive "
                f"ancestor={ancestor} child={child} deferral_streak={streak} "
                f"attempt={attempts}/{cap}",
                flush=True,
            )
        if exhausted:
            print(
                "prism coordinator: ancestor re-drive attempts exhausted "
                f"ancestor={ancestor} child={child} attempts={attempts}; "
                "deferrals continue and the publication-progress watchdog "
                "remains the backstop",
                flush=True,
            )

    def note_pending_parent_transition_resolved(self, block_hash: str) -> None:
        """Drop re-drive bookkeeping once a child's ancestor fence passes."""
        child = str(block_hash).lower()
        resolved_ancestor: str | None = None
        with self._coordinator.lock:
            ancestor = self._ancestor_redrive_last_blocking.pop(child, None)
            if ancestor is None:
                return
            record = self._ancestor_redrive_records.pop(ancestor, None)
            for key in [
                key
                for key, value in self._ancestor_redrive_last_blocking.items()
                if value == ancestor
            ]:
                self._ancestor_redrive_last_blocking.pop(key, None)
            if record is not None and record.consumed > 0:
                # Counted only when a forced pass actually ran: a transition
                # that resolved through the ordinary landing tail while a
                # request sat armed-but-unconsumed is the mechanism standing
                # by, not succeeding, and must not inflate the resolved
                # series operators compare against attempts.
                self.accepted_parent_redrive_resolved_count = (
                    int(self.accepted_parent_redrive_resolved_count) + 1
                )
                resolved_ancestor = ancestor
        if resolved_ancestor is not None:
            print(
                "prism coordinator: pending ancestor payout transition "
                "resolved after in-process re-drive "
                f"ancestor={resolved_ancestor} child={child}",
                flush=True,
            )

    def _ancestor_redrive_owed(self) -> bool:
        """Whether a forced durable-replay pass is armed for any ancestor."""
        with self._coordinator.lock:
            return any(
                record.armed
                for record in self._ancestor_redrive_records.values()
            )

    def _consume_ancestor_redrive_requests(self) -> tuple[str, ...]:
        """Take (and clear) every armed re-drive; one forced pass serves all.

        Consumed up front deliberately: a pass whose enumeration fails burns
        the attempt rather than re-running unbounded, a fresh deferral
        streak re-arms the next attempt, and the per-ancestor cap bounds the
        total.
        """
        with self._coordinator.lock:
            consumed = tuple(
                ancestor
                for ancestor, record in self._ancestor_redrive_records.items()
                if record.armed
            )
            for ancestor in consumed:
                record = self._ancestor_redrive_records[ancestor]
                record.armed = False
                record.consumed = record.attempts
            return consumed

    def _block_candidate_owned_in_process(self, block_hash: str) -> bool | None:
        """Whether any live lane still names this hash, or None if unreadable.

        Reads exactly the pin set the terminal-outcome eviction trusts, via
        the shared collector, so the two proofs cannot drift apart. None
        means a leaf lane could not be read without waiting under the global
        lock; callers treat that as owned, which is the safe direction.
        """
        key = str(block_hash).lower()
        self._ensure_block_candidate_disposition_state()
        self._ensure_block_replay_state()
        with self._coordinator.lock:
            pins = self._collect_live_block_candidate_pins()
            if pins is None:
                return None
            pinned, held = pins
            if key in held:
                return True
            return any(key in registry for registry in pinned)

    def _resolve_unreplayable_ancestor_transition(self, block_hash: str) -> None:
        """Converge a stuck transition with durable state when replay cannot.

        A forced enumeration that adopts the ancestor's pending outbox row is
        the ordinary re-drive; this handles the remaining wedge shape, where
        the armed transition has no pending durable row left -- the state a
        process restart resolves simply by not rebuilding the transition. It
        is cleared here only under the same proof a fresh startup replay
        would compute: the completed enumeration found no pending row for
        the hash, no in-process lane still owns a copy that could finish (or
        withdraw) the transition itself, the durable pool-block row reports
        the block confirmed and not reversed, and a fresh chain probe places
        the block's own hash on the active chain at its height -- the exact
        predicate the landing's already-confirmed branch clears the preview
        under, which consults the durable row only for a block it has just
        proven active. The probe matters because a reorg qbitd has seen but
        the reconciler has not yet written back (reconciliation fails closed
        while a landed transition exists) leaves the durable row
        stale-confirmed, and clearing on the row alone would unfence
        descendants onto the orphaned block's carry. Anything short of the
        full proof leaves the transition alone and the watchdog remains the
        backstop.
        """
        coordinator = self._coordinator
        coordinator._ensure_job_cache_state()
        key = str(block_hash).lower()
        with coordinator._accepted_block_payout_preview_condition:
            transition = coordinator._accepted_block_payout_previews.get(key)
        if transition is None:
            return
        if self._block_candidate_owned_in_process(key) is not False:
            print(
                "prism coordinator: ancestor re-drive left the transition to "
                f"its in-process owner hash={key}",
                flush=True,
            )
            return
        block_state_reader = getattr(coordinator.ledger, "pool_block_state", None)
        if not callable(block_state_reader):
            return
        try:
            block_state = coordinator._run_block_submitter_ledger_call(
                ("redrive-pool-block-state", key),
                "redrive-pool-block-state",
                lambda: block_state_reader(block_hash=key),
            )
        except Exception:
            print(
                "prism coordinator: ancestor re-drive pool-block state read "
                f"failed hash={key}",
                flush=True,
            )
            traceback.print_exc()
            return
        confirmed = (
            isinstance(block_state, dict)
            and str(block_state.get("chain_state", "")) == "confirmed"
            and str(block_state.get("maturity_state", "")) != "reversed"
        )
        if not confirmed:
            print(
                "prism coordinator: ancestor re-drive could not resolve the "
                f"transition hash={key} (no pending outbox row, pool block "
                "not durably confirmed); watchdog remains the backstop",
                flush=True,
            )
            return
        # The durable row alone is not proof against a reorg the fenced
        # reconciler has not written back yet; require the same node-side
        # activity the landing proves before it trusts this row. Anything
        # but a definite True (probe failure included) stands down.
        chain_probe = self._block_candidate_chain_probe(
            key,
            expected_height=getattr(transition, "block_height", None),
        )
        if chain_probe is not True:
            print(
                "prism coordinator: ancestor re-drive could not prove the "
                f"durably confirmed block active on the node's chain "
                f"hash={key} probe={chain_probe}; watchdog remains the "
                "backstop",
                flush=True,
            )
            return
        # Durable state already includes this block's payout carry, so
        # removing the in-memory override changes no logical payout state --
        # the same clear the landing performs for an already-confirmed
        # exact-idempotent replay.
        coordinator._clear_accepted_block_payout_preview(key)
        print(
            "prism coordinator: ancestor re-drive cleared a stale transition "
            f"for durably confirmed block hash={key}",
            flush=True,
        )

    def _conclude_ancestor_redrive_pass(
        self,
        redrive_hashes: tuple[str, ...],
        enumeration_truncated: bool,
    ) -> None:
        """Disposition each consumed re-drive after its enumeration ran.

        A truncated pass proved nothing about missing rows, so every
        transition is left for the next attempt (a fresh deferral streak
        re-arms it) or the watchdog. A complete pass either finds the
        ancestor (re-)adopted -- ordinary replay finalization owns it from
        there -- or runs the guarded stale-transition sweep.
        """
        if not redrive_hashes:
            return
        if enumeration_truncated:
            print(
                "prism coordinator: ancestor re-drive enumeration was "
                "truncated; leaving pending transitions for the next "
                "attempt or the watchdog",
                flush=True,
            )
            return
        for redrive_hash in redrive_hashes:
            key = str(redrive_hash).lower()
            with self._coordinator.lock:
                adopted = key in self._block_replay_inflight_hashes
            if adopted:
                # The pending outbox row was (re-)adopted -- by this pass
                # or an earlier one -- so ordinary replay finalization now
                # owns resolving the transition.
                print(
                    "prism coordinator: ancestor re-drive enumerated a "
                    f"pending candidate hash={key}",
                    flush=True,
                )
                continue
            self._resolve_unreplayable_ancestor_transition(key)

    def replay_pending(self) -> int:
        """Queue durable candidate intents not completed by an earlier process."""
        self._coordinator._record_block_submitter_phase("replay-check-memory")
        # While startup enumeration is still owed, correctness requires the
        # outbox query even if live candidates are queued: job builds stay
        # blocked until pending candidates are known, and only a successful
        # enumeration can unblock them.
        enumeration_owed = self._coordinator._block_replay_enumeration_owed()
        # A targeted ancestor re-drive (issue #190) must reach the outbox
        # query even while a retained retry or queued live work exists --
        # those short-circuits are exactly what starved this path during the
        # observed wedge -- so an armed request bypasses each of them below.
        redrive_hashes = self._consume_ancestor_redrive_requests()
        redrive_owed = bool(redrive_hashes)
        # One flag for "this pass must reach and complete the outbox query".
        # The startup gate and a targeted re-drive need the same enumeration
        # semantics -- the short-circuit bypasses, the query budget, the
        # keyset pagination, and the widening-window exit below all key off
        # it together -- so the re-drive genuinely runs the code startup
        # replay runs rather than a poll-budget approximation of it.
        forced_enumeration = enumeration_owed or redrive_owed
        with self._coordinator.lock:
            if not forced_enumeration and self.retry_candidate is not None:
                return 0
        # A live wakeup is already the lowest-latency route to qbitd. Never
        # park it behind the outbox query that exists only to recover missing
        # wakeups after queue pressure or restart.
        queue_obj = self.candidate_queue
        if (
            not forced_enumeration
            and queue_obj is not None
            and not queue_obj.empty()
        ):
            return 0
        self._ensure_block_replay_state()
        if (
            not forced_enumeration
            and not self._block_replay_candidate_queue.empty()
        ):
            return 0
        # A forced enumeration gates wedge resolution (startup: job
        # issuance; re-drive: a stuck payout transition), so it runs with
        # the landing-class budget instead of the poll budget (issue #188
        # fix 4); the periodic steady-state poll keeps the tight budget. The
        # metrics class follows the budget so a slow or timed-out forced
        # enumeration surfaces on the landing series the alerts watch.
        replay_query_timeout = (
            self._coordinator._block_landing_db_timeout()
            if forced_enumeration
            else None
        )
        replay_query_call_class = "landing" if forced_enumeration else "fast"
        pending_rows = getattr(
            self._coordinator.ledger,
            "pending_block_candidate_rows",
            None,
        )
        fetch_durable_page: Callable[..., list[Any]] | None = None
        if callable(pending_rows):

            def fetch_durable_rows(limit: int) -> list[Any]:
                return self._coordinator._run_block_submitter_ledger_call(
                    ("replay-outbox-query", limit),
                    "replay-outbox-query",
                    # Restore a batch with no per-row database work. In-flight
                    # dedupe lets later rows reach qbitd even while the oldest
                    # candidate is still accounting.
                    lambda: pending_rows(limit=limit),
                    timeout_seconds=replay_query_timeout,
                    call_class=replay_query_call_class,
                )

            if _pending_rows_accepts_cursor(pending_rows):

                def fetch_durable_page(
                    limit: int,
                    *,
                    page: int,
                    after_cursor: object | None,
                ) -> list[Any]:
                    return self._coordinator._run_block_submitter_ledger_call(
                        # The page ordinal and its cursor belong in the
                        # dedupe key: a timed-out call stays registered for
                        # the next paced retry to reuse, and reusing page
                        # N's in-flight call to answer page N+1 would
                        # silently drop a whole page of pending candidates
                        # from the enumeration.
                        (
                            "replay-outbox-query",
                            limit,
                            page,
                            _block_replay_cursor_key(after_cursor),
                        ),
                        "replay-outbox-query",
                        lambda: pending_rows(
                            limit=limit,
                            after_cursor=after_cursor,
                        ),
                        timeout_seconds=replay_query_timeout,
                        call_class=replay_query_call_class,
                    )

        else:
            pending = getattr(
                self._coordinator.ledger,
                "pending_block_candidates",
                None,
            )
            if not callable(pending):
                self._coordinator._clear_block_replay_enumeration_owed()
                return 0

            def fetch_durable_rows(limit: int) -> list[Any]:
                pending_intents = self._coordinator._run_block_submitter_ledger_call(
                    ("replay-outbox-query", limit),
                    "replay-outbox-query",
                    lambda: pending(limit=limit),
                    timeout_seconds=replay_query_timeout,
                    call_class=replay_query_call_class,
                )
                return [
                    {
                        "block_hash": (
                            intent.get("block_hash_hex", "")
                            if isinstance(intent, dict)
                            else ""
                        ),
                        "candidate": intent,
                    }
                    for intent in pending_intents
                ]

        queued = 0
        enumeration_truncated = False
        enumeration_paginated = False
        # One chain-height budget for the whole walk -- both its remaining
        # probes and the reads they bought -- so a backlog split across fifty
        # pages costs the node what a single page does.
        collapse_probe_budget = _CollapseHeightProbeBudget()
        if forced_enumeration and fetch_durable_page is not None:
            # Pagination, not a widening window: the doubling loop below
            # fails closed once one page would have to hold the entire
            # backlog, so a backlog larger than the cap kept enumeration
            # owed (and every job build blocked) until it drained under the
            # cap on its own. A keyset cursor walks a backlog of any size
            # with the same bounded per-query cost, and only a page proven
            # short ends the walk.
            page = 0
            after_cursor: object | None = None
            while True:
                page += 1
                try:
                    durable_rows = fetch_durable_page(
                        MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
                        page=page,
                        after_cursor=after_cursor,
                    )
                except TypeError:
                    if page > 1:
                        # Cursor support was already proven by an earlier
                        # page, so this is a real fault and not a legacy
                        # ledger; adopted rows must not be re-adopted by a
                        # fallback pass that starts over from the top.
                        raise
                    # A ledger without cursor support keeps exactly today's
                    # windowed semantics, fail-closed truncation included.
                    break
                enumeration_paginated = True
                # Collapse this page's decided-height siblings before any of
                # it is adopted, so a row this apply terminalizes is never
                # queued and never re-arms a payout barrier. The cursor and
                # short-page proof stay bound to the *fetched* page: what the
                # collapse removes was removed from the outbox too, so the
                # enumeration is still complete.
                queued += self._adopt_durable_block_candidate_rows(
                    self._collapse_superseded_block_candidates(
                        durable_rows,
                        timeout_seconds=replay_query_timeout,
                        call_class=replay_query_call_class,
                        probe_budget=collapse_probe_budget,
                    )
                )
                print(
                    "prism coordinator: pending block candidate enumeration "
                    f"page={page} rows={len(durable_rows)}",
                    flush=True,
                )
                if len(durable_rows) < MAX_BLOCK_REPLAY_ENUMERATION_ROWS:
                    # A short page proves no pending row followed it at query
                    # time, which is the completeness the job-build gate waits
                    # on.
                    break
                if self._block_replay_should_yield_to_live_candidates(
                    collapse_probe_budget
                ):
                    enumeration_truncated = True
                    print(
                        "prism coordinator: pending block candidate "
                        f"enumeration page={page} yielded to a waiting live "
                        "candidate; job builds stay blocked until a complete "
                        "enumeration succeeds",
                        flush=True,
                    )
                    break
                next_cursor = (
                    durable_rows[-1].get("cursor")
                    if isinstance(durable_rows[-1], dict)
                    else None
                )
                if next_cursor is None or next_cursor == after_cursor:
                    # Either the ledger accepted the keyword without keying
                    # its rows, or it accepted the cursor without honouring
                    # it (a **kwargs double, say) and re-served the same
                    # page. Both leave the walk unable to advance, so fail
                    # closed instead of looping forever or declaring an
                    # unproven enumeration complete.
                    enumeration_truncated = True
                    print(
                        "prism coordinator: pending block candidate "
                        f"enumeration page={page} did not advance its "
                        "cursor; job builds stay blocked until a complete "
                        "enumeration succeeds",
                        flush=True,
                    )
                    break
                after_cursor = next_cursor
        if not enumeration_paginated:
            enumeration_limit = MAX_PENDING_BLOCK_CANDIDATES
            while True:
                durable_rows = fetch_durable_rows(enumeration_limit)
                # Same collapse-then-adopt order on the legacy widening
                # window; the truncation proof below still measures the
                # fetched window, not the surviving remainder.
                queued += self._adopt_durable_block_candidate_rows(
                    self._collapse_superseded_block_candidates(
                        durable_rows,
                        timeout_seconds=replay_query_timeout,
                        call_class=replay_query_call_class,
                        probe_budget=collapse_probe_budget,
                    )
                )
                if len(durable_rows) < enumeration_limit or not forced_enumeration:
                    # A short page proves no further pending row existed at
                    # query time -- the completeness the re-drive's
                    # stale-transition sweep below also relies on.
                    break
                if self._block_replay_should_yield_to_live_candidates(
                    collapse_probe_budget
                ):
                    enumeration_truncated = True
                    print(
                        "prism coordinator: pending block candidate "
                        f"enumeration at {enumeration_limit} rows yielded to "
                        "a waiting live candidate; job builds stay blocked "
                        "until a complete enumeration succeeds",
                        flush=True,
                    )
                    break
                # A full batch may hide more pending rows, and a hidden row could
                # be the active parent whose carry a child job must not omit.
                # Re-query with a doubled window until the result is provably
                # untruncated; in-flight dedupe makes re-seen rows free.
                if enumeration_limit >= MAX_BLOCK_REPLAY_ENUMERATION_ROWS:
                    enumeration_truncated = True
                    print(
                        "prism coordinator: pending block candidate enumeration "
                        f"still truncated at {enumeration_limit} rows; job builds "
                        "stay blocked until a complete enumeration succeeds",
                        flush=True,
                    )
                    break
                enumeration_limit = min(
                    enumeration_limit * 2,
                    MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
                )
        if not enumeration_truncated:
            # Every pending candidate is now known and its payout barrier armed
            # (or quarantined), so child job builds may proceed. A truncated
            # pass instead leaves enumeration owed: the queued batch drains,
            # and the submitter loop re-enumerates the remainder.
            self._coordinator._clear_block_replay_enumeration_owed()
            self._coordinator._record_startup_phase_once("block_replay_enumerated")
        self._conclude_ancestor_redrive_pass(redrive_hashes, enumeration_truncated)
        if queued:
            print(
                f"prism coordinator: replayed {queued} pending block candidate(s)",
                flush=True,
            )
        return queued

    def _run_one_invalid_block_candidate_quarantine(self) -> bool:
        self._ensure_block_replay_state()
        try:
            item = self._block_quarantine_queue.get_nowait()
        except queue.Empty:
            return False
        block_hash, error = item[0], item[1]
        pending_share = item[2] if len(item) > 2 else None
        completed = False
        try:
            self._coordinator._record_block_submitter_phase("replay-quarantine")
            quarantine = getattr(
                self._coordinator.ledger,
                "mark_block_candidate_abandoned",
                None,
            )
            if callable(quarantine):
                quarantined = self._coordinator._run_block_submitter_ledger_call(
                    ("replay-quarantine", block_hash),
                    "replay-quarantine",
                    lambda: quarantine(block_hash=block_hash, error=error),
                )
                self._coordinator._clear_accepted_block_payout_preview(block_hash)
                if pending_share is not None:
                    # The row's durable credit holder was adopted before the
                    # failure. Its outbox row is now terminal, so a later
                    # successful replay re-creates a fresh holder; release
                    # this one instead of clamping snapshot anchors forever.
                    self._coordinator._finish_pending_share_candidate(pending_share)
                if quarantined:
                    self._coordinator._clear_block_candidate_retry_state(block_hash)
                    self._coordinator._discard_outstanding_block_candidate(block_hash)
                    with self._coordinator.lock:
                        self.poisoned = int(self.poisoned) + 1
            completed = True
            return True
        except Exception:
            print(
                "prism coordinator: invalid candidate quarantine failed "
                f"hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()
            return True
        finally:
            self._block_quarantine_queue.task_done()
            if completed:
                with self._coordinator.lock:
                    self._block_quarantine_hashes.discard(block_hash)
            elif not self._coordinator.stop_event.is_set():
                self._block_quarantine_queue.put_nowait(
                    (block_hash, error, pending_share)
                )

    # -- retry state -------------------------------------------------------

    def wait_for_retry(self, delay_seconds: float) -> bool:
        """Wait for intentional backoff without impersonating stuck work.

        Retry waits heartbeat in bounded slices. Direct outbox calls and lock
        admission use the same phase-aware pattern; work that is not covered
        by an explicit deadline remains watchdog-eligible.
        """
        delay_seconds = max(0.0, float(delay_seconds))
        if delay_seconds <= 0:
            return self._coordinator.stop_event.is_set()
        started = time.monotonic()
        with self._state_lock:
            self._backoff_started_monotonic = started
            self._backoff_deadline_monotonic = started + delay_seconds
            self._backoff_delay_seconds = delay_seconds
        remaining = delay_seconds
        try:
            while remaining > 0:
                self._coordinator._record_block_submitter_wait("retry-backoff")
                wait_slice = min(remaining, self._block_work_wait_slice())
                if self._coordinator.stop_event.wait(wait_slice):
                    return True
                remaining = max(0.0, remaining - wait_slice)
            self._coordinator._record_block_submitter_wait("retry-backoff:complete")
            return False
        finally:
            with self._state_lock:
                self._backoff_started_monotonic = None
                self._backoff_deadline_monotonic = None
                self._backoff_delay_seconds = 0.0

    def backoff_snapshot(self) -> tuple[bool, float, float]:
        now = time.monotonic()
        with self._state_lock:
            deadline = self._backoff_deadline_monotonic
            return (
                deadline is not None,
                max(0.0, deadline - now) if deadline is not None else 0.0,
                self._backoff_delay_seconds,
            )

    def block_submit_seconds_snapshot(self) -> tuple[dict[float, int], float, int]:
        """Copied landed-to-RPC histogram state for metrics rendering."""
        with self._block_submit_metrics_lock:
            histogram = self.block_submit_seconds_histogram
            return (
                dict(histogram["buckets"]),
                float(histogram["sum"]),
                int(histogram["count"]),
            )

    def accepted_block_preview_publication_snapshot(
        self,
    ) -> dict[str, dict[str, Any]]:
        """Copied acceptance-to-preview-publication histograms, by result."""
        with self._accepted_block_preview_publication_lock:
            return {
                result: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for result, histogram in (
                    self.accepted_block_preview_publication_seconds_histogram
                ).items()
            }

    def next_retry_delay(self, block_hash: str) -> float:
        initial = max(0.0, float(self.retry_initial_seconds))
        maximum = max(initial, float(self.retry_max_seconds))
        with self._coordinator.lock:
            delays = self.retry_delays
            if delays is None:
                delays = {}
                self.retry_delays = delays
            delay = float(delays.get(block_hash, initial))
            delays[block_hash] = min(maximum, max(initial, delay * 2))
        return min(delay, maximum)

    def clear_retry_state(self, block_hash: str) -> None:
        with self._coordinator.lock:
            delays = self.retry_delays
            if delays is not None:
                delays.pop(block_hash, None)
            landing_timeouts = getattr(self, "_block_landing_timeout_counts", None)
            if landing_timeouts is not None:
                landing_timeouts.pop(block_hash, None)
            not_before = getattr(
                self,
                "_block_candidate_retry_not_before",
                None,
            )
            if not_before is not None:
                not_before.pop(block_hash, None)
            retained = getattr(
                self,
                "_block_candidate_retained_node_submissions",
                None,
            )
            if retained is not None:
                retained.pop(str(block_hash).lower(), None)
            stamped = getattr(
                self,
                "_block_candidate_retained_submission_monotonic",
                None,
            )
            if stamped is not None:
                stamped.pop(str(block_hash).lower(), None)

    def mark_attempted(self, block_hash: str) -> None:
        mark_attempted = getattr(
            self._coordinator.ledger,
            "mark_block_candidate_attempted",
            None,
        )
        if callable(mark_attempted):
            self._coordinator._run_block_submitter_ledger_call(
                ("mark-attempted", block_hash),
                "mark-attempted",
                lambda: mark_attempted(block_hash=block_hash),
            )

    def _merge_block_candidate_retry_locked(
        self,
        attribute: str,
        candidate: PrismBlockCandidate,
    ) -> None:
        """Merge one retry by parent-first order. Caller holds the runtime lock."""
        candidate_height = int(candidate.context.template["height"])
        candidate_hash = str(candidate.submission.block_hash_hex).lower()
        existing = getattr(self, attribute, None)
        if existing is None:
            setattr(self, attribute, candidate)
            return
        existing_height = int(existing.context.template["height"])
        existing_hash = str(existing.submission.block_hash_hex).lower()
        if candidate_hash == existing_hash:
            setattr(self, attribute, candidate)
            if existing is not candidate:
                # The newer same-hash object takes the slot; the displaced
                # duplicate is dropped and carries its own floor holder.
                self._release_dropped_duplicate_candidate_floor(existing)
            return
        if attribute != "_retry_block_candidate":
            if candidate_height < existing_height:
                setattr(self, attribute, candidate)
            return

        # The raw lane has one parent-first head slot, but every displaced
        # hash still needs an in-memory wakeup. Durable replay dedupe keeps a
        # replayed descendant marked in-flight, so relying on a later outbox
        # scan here could otherwise suppress it forever.
        self._ensure_block_candidate_disposition_state()
        waiting = self._block_disposition_waiting_retries
        if candidate_height < existing_height:
            waiting[existing_hash] = existing
            setattr(self, attribute, candidate)
        else:
            waiting[candidate_hash] = candidate

    def retain_for_retry(self, candidate: PrismBlockCandidate) -> None:
        """Keep the oldest unresolved candidate ahead of queued descendants."""
        candidate_hash = str(candidate.submission.block_hash_hex).lower()
        # A retained candidate will be re-disposed, so the disposition seal
        # (which stopped tip-observation matching at a terminal commit) no
        # longer applies: the terminal work did not complete. Re-register
        # immediately -- not at the next disposition -- so acceptance
        # evidence arriving during the retry backoff is not lost.
        self._coordinator._register_outstanding_block_candidate(candidate_hash)
        with self._coordinator.lock:
            self.retries = int(self.retries) + 1
            accounting_owner = (
                threading.get_ident()
                == getattr(self, "_block_accounting_thread_ident", None)
                and bool(
                    getattr(
                        self,
                        "_block_accounting_holds_disposition",
                        False,
                    )
                )
            )
            retry_attribute = (
                "_block_accounting_deferred_retry_candidate"
                if accounting_owner
                else "_retry_block_candidate"
            )
            self._merge_block_candidate_retry_locked(
                retry_attribute,
                candidate,
            )

    def _pace_block_candidate_retry(self, block_hash: str) -> None:
        """Apply per-candidate retry backoff without convoying accounting.

        On the block_accounting thread the disposition lease and writer
        admission stay held until the accounting task's finally clause, so
        sleeping here would stall every queued accounting task and keep an
        armed payout barrier blocking balance mutation for the whole backoff
        window. Record a not-before deadline instead; the dequeue path honors
        it, and replay_pending_block_candidates already short-circuits while
        the retained candidate occupies the retry slot.
        """
        delay_seconds = self._coordinator._next_block_candidate_retry_delay(block_hash)
        accounting_owner = (
            threading.get_ident()
            == getattr(self, "_block_accounting_thread_ident", None)
            and bool(
                getattr(
                    self,
                    "_block_accounting_holds_disposition",
                    False,
                )
            )
        )
        if not accounting_owner:
            self._coordinator._wait_for_block_candidate_retry(delay_seconds)
            return
        with self._coordinator.lock:
            not_before = getattr(
                self,
                "_block_candidate_retry_not_before",
                None,
            )
            if not_before is None:
                not_before = {}
                self._block_candidate_retry_not_before = not_before
            not_before[str(block_hash).lower()] = (
                time.monotonic() + delay_seconds
            )

    def _block_candidate_retry_ready_locked(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Return whether a parked retry's backoff deadline has passed.

        Caller holds the runtime lock. A ready entry is dropped so a candidate
        that later lands terminally leaves no stale pacing behind.
        """
        not_before = getattr(self, "_block_candidate_retry_not_before", None)
        if not not_before:
            return True
        block_hash = str(candidate.submission.block_hash_hex).lower()
        deadline = not_before.get(block_hash)
        if deadline is None:
            return True
        if time.monotonic() < deadline:
            return False
        not_before.pop(block_hash, None)
        return True

    # -- deadline-classed ledger call workers ------------------------------

    def _ensure_block_submitter_ledger_call_state(self) -> None:
        if not hasattr(self, "_block_submitter_ledger_calls_lock"):
            self._block_submitter_ledger_calls_lock = threading.Lock()
        if not hasattr(self, "_block_submitter_ledger_calls"):
            self._block_submitter_ledger_calls = {}
        if not hasattr(self, "_block_submitter_ledger_worker_slots"):
            self._block_submitter_ledger_worker_slots = threading.BoundedSemaphore(
                MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS
            )

    def _block_submitter_db_timeout(self) -> float:
        return max(
            0.001,
            float(
                getattr(
                    self._coordinator,
                    "block_submit_db_timeout_seconds",
                    DEFAULT_BLOCK_SUBMIT_DB_TIMEOUT_SECONDS,
                )
            ),
        )

    def _block_landing_watchdog_ceiling(self) -> float:
        """Largest landing budget the configured watchdog can tolerate.

        Landing-class work runs on the block-work thread the watchdog
        monitors, so the landing budget and the watchdog tolerance are one
        system, not two independent settings. Deriving the ceiling from the
        configured tolerance keeps them in step through every override
        instead of pinning a second literal that silently goes stale.

        A deployment that turned the watchdog off has no hard-exit hazard to
        stay under, and clamping it anyway would cost the operator budget for
        no safety at all -- so that case gets no ceiling. The attribute is
        read defensively and defaults to *enabled*: clamping is the safe
        answer when the setting cannot be determined.
        """
        if not bool(getattr(self._coordinator, "watchdog_enabled", True)):
            return float("inf")
        return max(
            0.001,
            float(
                getattr(
                    self._coordinator,
                    "watchdog_timeout_seconds",
                    DEFAULT_PRISM_WATCHDOG_TIMEOUT_SECONDS,
                )
            )
            * BLOCK_LANDING_DB_TIMEOUT_WATCHDOG_FRACTION,
        )

    def _note_block_landing_budget_clamped(
        self,
        *,
        configured_base: float,
        configured_cap: float,
        ceiling: float,
    ) -> None:
        """Say once that the watchdog is granting less than was configured.

        Without this the operator's configured landing budget is quietly
        reduced -- at the 120s default tolerance the reviewed 120s cap
        becomes 60s -- and the two states an operator most needs to tell
        apart become indistinguishable in the logs: escalation exhausted at
        the configured cap, versus escalation that never reached it because
        the ceiling stopped it first. Emitted once per process because
        _block_landing_db_timeout runs on every landing attempt and a
        per-attempt line would bury the landing's own diagnostics; the flag
        flips under the coordinator lock so concurrent first landings still
        print exactly one line. The print stays outside that lock -- no I/O
        under a coordinator-wide lock on the landing path.
        """
        with self._coordinator.lock:
            if getattr(self, "_block_landing_budget_clamp_logged", False):
                return
            self._block_landing_budget_clamp_logged = True
        watchdog_seconds = float(
            getattr(
                self._coordinator,
                "watchdog_timeout_seconds",
                DEFAULT_PRISM_WATCHDOG_TIMEOUT_SECONDS,
            )
        )
        print(
            "prism coordinator: landing db budget clamped by watchdog "
            f"configured_base={configured_base:g}s "
            f"configured_max={configured_cap:g}s "
            f"granted_base={min(configured_base, ceiling):g}s "
            f"granted_max={min(configured_cap, ceiling):g}s "
            f"ceiling={ceiling:g}s "
            f"watchdog_timeout={watchdog_seconds:g}s "
            f"fraction={BLOCK_LANDING_DB_TIMEOUT_WATCHDOG_FRACTION:g}",
            flush=True,
        )

    def _block_landing_db_timeout(self, block_hash: str | None = None) -> float:
        """Landing-class deadline, escalated after observed landing timeouts.

        The first attempt already receives the full landing budget; a known
        landing-class operation never begins at the one-second poll budget.
        Escalation doubles per timed-out landing attempt for the same block
        hash up to the reviewed cap.

        The reviewed cap is an upper bound, not the granted budget: every
        value here is clamped to the watchdog-derived ceiling, which can only
        lower it. Landing steps are spent on the watchdog-monitored block-work
        thread, so an escalated budget that outruns the watchdog tolerance is
        not a longer attempt but a hard exit mid-landing -- and because the
        escalation counts live only in memory, the restart drops back to the
        base budget and repeats the same doomed cycle (issue #125). At the
        120s default tolerance the ceiling is 60s and escalation runs
        30s -> 60s; at the 300s production tolerance the ceiling is 150s and
        the configured 120s cap is unchanged. A deployment running with the
        watchdog disabled has no ceiling at all and keeps its configured
        values in full. Any clamp is announced once (see
        _note_block_landing_budget_clamped) so a reduced budget is never a
        silent behavior change.
        """
        ceiling = self._block_landing_watchdog_ceiling()
        configured_base = max(
            0.001,
            float(
                getattr(
                    self._coordinator,
                    "block_landing_db_timeout_seconds",
                    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_SECONDS,
                )
            ),
        )
        configured_cap = max(
            configured_base,
            float(
                getattr(
                    self._coordinator,
                    "block_landing_db_timeout_max_seconds",
                    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_MAX_SECONDS,
                )
            ),
        )
        base = min(configured_base, ceiling)
        cap = min(configured_cap, ceiling)
        if base < configured_base or cap < configured_cap:
            # An infinite ceiling (watchdog disabled) can never compare below
            # a finite configured value, so a deployment that opted out of the
            # watchdog never reaches this line.
            self._note_block_landing_budget_clamped(
                configured_base=configured_base,
                configured_cap=configured_cap,
                ceiling=ceiling,
            )
        timeouts = 0
        if block_hash is not None:
            with self._coordinator.lock:
                counts = getattr(self, "_block_landing_timeout_counts", None)
                if counts is not None:
                    timeouts = int(counts.get(block_hash, 0))
        return min(cap, base * (2.0 ** min(timeouts, 8)))

    def _note_block_landing_timeout(self, block_hash: str | None) -> None:
        if block_hash is None:
            return
        with self._coordinator.lock:
            counts = getattr(self, "_block_landing_timeout_counts", None)
            if counts is None:
                counts = {}
                self._block_landing_timeout_counts = counts
            counts[block_hash] = int(counts.get(block_hash, 0)) + 1

    def _ensure_block_ledger_call_metrics(self) -> None:
        if not hasattr(self, "_block_ledger_call_metrics_lock"):
            self._block_ledger_call_metrics_lock = threading.Lock()
        if not hasattr(self, "_block_ledger_call_metrics"):
            self._block_ledger_call_metrics = {}

    def _record_block_ledger_call(
        self,
        *,
        call_class: str,
        budget_seconds: float,
        duration_seconds: float,
        timed_out: bool,
    ) -> None:
        """Track per-call-class submitter ledger latency and timeout counts."""
        self._ensure_block_ledger_call_metrics()
        with self._block_ledger_call_metrics_lock:
            stats = self._block_ledger_call_metrics.setdefault(
                call_class,
                {
                    "calls_total": 0,
                    "timeouts_total": 0,
                    "last_budget_seconds": 0.0,
                    "last_duration_seconds": 0.0,
                    "max_duration_seconds": 0.0,
                },
            )
            stats["calls_total"] = int(stats["calls_total"]) + 1
            if timed_out:
                stats["timeouts_total"] = int(stats["timeouts_total"]) + 1
            stats["last_budget_seconds"] = float(budget_seconds)
            stats["last_duration_seconds"] = float(duration_seconds)
            stats["max_duration_seconds"] = max(
                float(stats["max_duration_seconds"]), float(duration_seconds)
            )

    def block_ledger_call_class_metrics(self) -> dict[str, dict[str, float | int]]:
        self._ensure_block_ledger_call_metrics()
        with self._block_ledger_call_metrics_lock:
            return {
                call_class: dict(stats)
                for call_class, stats in self._block_ledger_call_metrics.items()
            }

    def _block_submitter_stuck_call_exit_timeout(self) -> float:
        return max(
            0.001,
            float(
                getattr(
                    self._coordinator,
                    "block_submit_stuck_call_exit_seconds",
                    DEFAULT_BLOCK_SUBMIT_STUCK_CALL_EXIT_SECONDS,
                )
            ),
        )

    def _maybe_restart_for_stuck_block_call(
        self,
        *,
        kind: str,
        started_monotonic: float,
    ) -> None:
        """Fail stop when a poisoned worker pool stays exhausted."""
        age_seconds = max(0.0, time.monotonic() - started_monotonic)
        exit_seconds = self._block_submitter_stuck_call_exit_timeout()
        if age_seconds < exit_seconds:
            return
        stop_event = getattr(self._coordinator, "stop_event", None)
        if stop_event is not None and stop_event.is_set():
            return
        print(
            "prism coordinator: block work call remained stuck; requesting "
            f"restart kind={kind} age={age_seconds:.3f}s "
            f"budget={exit_seconds:g}s",
            flush=True,
        )
        self._coordinator._fatal_exit_requested = True
        self._coordinator.request_shutdown()

    def _maybe_restart_for_exhausted_block_call_pool(
        self,
        *,
        kind: str,
        calls_lock: threading.Lock,
        calls: dict[Any, Any],
    ) -> None:
        """Age an exhausted pool even when retries reuse existing calls."""
        with calls_lock:
            active_starts = [
                pending.started_monotonic
                for pending in calls.values()
                if not pending.done.is_set()
            ]
        if len(active_starts) < MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS:
            return
        self._maybe_restart_for_stuck_block_call(
            kind=kind,
            started_monotonic=min(active_starts),
        )

    def _retire_finished_block_submitter_ledger_call(
        self,
        key: tuple[object, ...],
        call: _BlockSubmitterLedgerCall,
    ) -> None:
        """Take one finished call out of the registry, by identity.

        A finished call has no live need: it is registered only so that a
        same-key caller arriving while the worker is *still out* joins that
        one call instead of starting a second one, and a worker that has
        published ``done`` is not out any more. Leaving it behind would wait
        on an invocation that may never come -- the exact key can be a whole
        enumeration page of block hashes, and the late write itself is what
        empties that page out of the outbox -- so the registry would keep
        the completed call, and its hash tuple, for the life of the process.

        Both the worker and a waiter that observed the completion call this,
        whichever gets there first; the identity check is what keeps either
        of them from dropping a *different* call that has since taken the
        key, and the ``done`` check is what keeps them from ever dropping a
        worker that is still out. Waiters hold the call directly, so a
        removal never costs anybody the result or error it is owed: it only
        settles that the next caller for that key replays the idempotent
        operation on a fresh call.
        """
        with self._block_submitter_ledger_calls_lock:
            if not call.done.is_set():
                return
            if self._block_submitter_ledger_calls.get(key) is call:
                del self._block_submitter_ledger_calls[key]

    @contextmanager
    def _block_submitter_ledger_timeout_scope(
        self,
        timeout_seconds: float | None = None,
    ) -> Iterator[None]:
        """Apply the submitter's PostgreSQL deadline when the ledger supports it."""
        operation_timeout = getattr(
            self._coordinator.ledger,
            "operation_timeout",
            None,
        )
        if not callable(operation_timeout):
            yield
            return
        with operation_timeout(
            self._block_submitter_db_timeout()
            if timeout_seconds is None
            else timeout_seconds
        ):
            yield

    @contextmanager
    def _block_work_ledger_progress_scope(self, phase: str) -> Iterator[None]:
        """Keep a ledger admission wait visible to the block-work watchdog.

        A statement deadline only bounds work the server can cancel. Before
        any SQL is sent the ledger must first win its own writer lock or read
        semaphore, and that wait is local: no statement exists to cancel and
        nothing reports until admission succeeds. The scopes below run on the
        block-work owner thread the watchdog monitors, so an admission wait
        that stamps nothing is indistinguishable from a wedged thread and can
        cost the coordinator a hard exit while it is merely queued behind
        another writer.

        Installing the ledger's progress hook makes that wait heartbeat in
        watchdog-sized slices, using the same phase-stamping helper the
        bounded-call wait loop already uses (it no-ops safely off the owner
        thread). Ledgers predating the hook are left exactly as they were.
        """
        operation_progress = getattr(
            self._coordinator.ledger,
            "operation_progress",
            None,
        )
        if not callable(operation_progress):
            yield
            return
        with operation_progress(
            lambda: self._coordinator._record_block_submitter_wait(phase),
            slice_seconds=self._block_work_wait_slice(),
        ):
            yield

    @contextmanager
    def _block_submitter_ledger_statement_timeout_scope(self) -> Iterator[None]:
        """Give each post-submit ledger step a fresh short deadline."""
        statement_timeout = getattr(
            self._coordinator.ledger,
            "statement_timeout",
            None,
        )
        if callable(statement_timeout):
            with self._block_work_ledger_progress_scope(
                "wait-ledger-admission:submit"
            ):
                with statement_timeout(self._block_submitter_db_timeout()):
                    yield
            return
        # Duck-typed ledgers predating per-statement scopes still receive a
        # bounded operation, even though their budget spans the whole tail.
        with self._block_submitter_ledger_timeout_scope():
            yield

    @contextmanager
    def _block_landing_ledger_statement_timeout_scope(
        self,
        block_hash: str | None = None,
    ) -> Iterator[None]:
        """Give each landing-class ledger step the landing budget.

        The accounting tail that lands an accepted block (persist, prior
        balances, confirm, and the prepared-state rejection of a terminal
        candidate) runs under this scope instead of the poll-class one.
        Timed-out steps are recorded so the next attempt for the same block
        hash escalates its budget; the ledger backends already guarantee
        server-side cancellation completes and the pooled session is rolled
        back or replaced before the paced retry re-enters here.

        The guarded body also runs node RPCs and audit/build work. Only a
        ledger-originated deadline may escalate the next landing budget or
        fire the landing-timeout alert: a node RPC timeout is not a database
        cancellation, and escalating on it would page and widen PostgreSQL
        deadlines for a database that never missed one.
        """
        timeout_seconds = self._coordinator._block_landing_db_timeout(block_hash)
        scope = getattr(self._coordinator.ledger, "statement_timeout", None)
        if not callable(scope):
            scope = getattr(self._coordinator.ledger, "operation_timeout", None)
        started = time.monotonic()
        timed_out = False
        try:
            if callable(scope):
                with self._block_work_ledger_progress_scope(
                    "wait-ledger-admission:landing"
                ):
                    with scope(timeout_seconds):
                        yield
            else:
                yield
        except (LedgerOperationTimeout, BlockSubmitterDatabaseTimeout):
            timed_out = True
            self._coordinator._note_block_landing_timeout(block_hash)
            raise
        finally:
            self._coordinator._record_block_ledger_call(
                call_class="landing",
                budget_seconds=timeout_seconds,
                duration_seconds=max(0.0, time.monotonic() - started),
                timed_out=timed_out,
            )

    def _run_block_submitter_ledger_call(
        self,
        key: tuple[object, ...],
        phase: str,
        operation: Callable[[], Any],
        *,
        timeout_seconds: float | None = None,
        call_class: str = "fast",
    ) -> Any:
        """Run one direct outbox call without letting its driver wedge us.

        A timed-out call stays registered for as long as its worker is
        still out, and the next paced retry joins that same call. This
        bounds the coordinator-side wait without spawning an unbounded pile
        of threads when a fake/misbehaving driver ignores the real
        PostgreSQL statement deadline. Candidate outbox mutations are
        idempotent, so a late completion converges with replay.

        The registry therefore holds in-flight calls and nothing else: the
        worker retires its own entry the moment it publishes ``done`` (see
        _retire_finished_block_submitter_ledger_call), so a key whose retry
        never comes back cannot pin a completed call and its page of block
        hashes for the life of the process. Waiters already parked on the
        call hold it directly and still get its result or error; a caller
        arriving after the removal simply replays the operation.

        call_class labels the per-class latency/timeout metrics and must
        match the budget in use: a call given the landing deadline records
        as "landing" so the landing-timeout alert covers it, instead of
        inflating the fast-call budget gauge.
        """
        if timeout_seconds is None:
            timeout_seconds = self._block_submitter_db_timeout()
        else:
            timeout_seconds = max(0.001, float(timeout_seconds))
        self._ensure_block_submitter_ledger_call_state()
        with self._block_submitter_ledger_calls_lock:
            call = self._block_submitter_ledger_calls.get(key)
            if call is None:
                if not self._block_submitter_ledger_worker_slots.acquire(
                    blocking=False
                ):
                    oldest_started = min(
                        (
                            pending.started_monotonic
                            for pending in self._block_submitter_ledger_calls.values()
                            if not pending.done.is_set()
                        ),
                        default=time.monotonic(),
                    )
                    self._maybe_restart_for_stuck_block_call(
                        kind="ledger-worker-pool",
                        started_monotonic=oldest_started,
                    )
                    raise BlockSubmitterDatabaseTimeout(
                        f"{phase} could not acquire a bounded ledger worker"
                    )
                call = _BlockSubmitterLedgerCall()
                self._block_submitter_ledger_calls[key] = call

                def run() -> None:
                    try:
                        with self._block_submitter_ledger_timeout_scope(
                            timeout_seconds
                        ):
                            call.result = operation()
                    except BaseException as exc:
                        call.error = exc
                    finally:
                        call.done.set()
                        # The slot goes back before the removal: a caller
                        # that arrives to find this key already retired must
                        # not then be told the bounded pool is exhausted.
                        self._block_submitter_ledger_worker_slots.release()
                        self._retire_finished_block_submitter_ledger_call(
                            key,
                            call,
                        )

                threading.Thread(
                    target=run,
                    name=f"prism-block-ledger-{phase}",
                    daemon=True,
                ).start()

        deadline = time.monotonic() + timeout_seconds
        while not call.done.is_set():
            self._coordinator._record_block_submitter_wait(phase)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._maybe_restart_for_exhausted_block_call_pool(
                    kind="ledger-worker-pool",
                    calls_lock=self._block_submitter_ledger_calls_lock,
                    calls=self._block_submitter_ledger_calls,
                )
                print(
                    "prism coordinator: block submitter ledger phase timed out "
                    f"phase={phase} timeout={timeout_seconds:g}s",
                    flush=True,
                )
                self._coordinator._record_block_ledger_call(
                    call_class=call_class,
                    budget_seconds=timeout_seconds,
                    duration_seconds=timeout_seconds,
                    timed_out=True,
                )
                raise BlockSubmitterDatabaseTimeout(
                    f"{phase} exceeded {timeout_seconds:g}s"
                )
            call.done.wait(
                min(
                    remaining,
                    self._block_work_wait_slice(),
                )
            )
        self._coordinator._record_block_submitter_wait(f"{phase}:complete")
        # A server-side deadline normally completes the worker with a ledger
        # timeout error before the coordinator-side wait expires. Every
        # operation behind this wrapper is a database call, so a completed
        # call carrying a timeout error is still a timed-out call for the
        # per-class alert series.
        self._coordinator._record_block_ledger_call(
            call_class=call_class,
            budget_seconds=timeout_seconds,
            duration_seconds=max(0.0, time.monotonic() - call.started_monotonic),
            timed_out=isinstance(call.error, TimeoutError),
        )
        self._retire_finished_block_submitter_ledger_call(key, call)
        if call.error is not None:
            raise call.error
        return call.result

    # -- submitblock transport ---------------------------------------------

    def _rpc_call_with_timeout(
        self,
        method: str,
        params: list[object],
        *,
        timeout_seconds: float,
    ) -> Any:
        """Pass an explicit timeout to production RPCs and capable test doubles."""
        call = self._coordinator.rpc.call
        supports_timeout = isinstance(self._coordinator.rpc, JsonRpc)
        if not supports_timeout:
            try:
                parameters = inspect.signature(call).parameters.values()
                supports_timeout = any(
                    parameter.name == "timeout"
                    or parameter.kind is inspect.Parameter.VAR_KEYWORD
                    for parameter in parameters
                )
            except (TypeError, ValueError):
                supports_timeout = False
        if supports_timeout:
            return call(method, params, timeout=timeout_seconds)
        return call(method, params)

    def _ensure_block_submitter_rpc_call_state(self) -> None:
        if not hasattr(self, "_block_submitter_rpc_calls_lock"):
            self._block_submitter_rpc_calls_lock = threading.Lock()
        if not hasattr(self, "_block_submitter_rpc_calls"):
            self._block_submitter_rpc_calls = {}
        if not hasattr(self, "_block_submitter_rpc_worker_slots"):
            self._block_submitter_rpc_worker_slots = threading.BoundedSemaphore(
                MAX_BLOCK_SUBMITTER_STUCK_CALL_WORKERS
            )

    def _run_submitblock_rpc_with_hard_deadline(
        self,
        *,
        block_hash: str,
        block_hex: str,
        timeout_seconds: float,
    ) -> Any:
        """Bound wall time even when an RPC adapter ignores its timeout."""
        self._ensure_block_submitter_rpc_call_state()
        with self._block_submitter_rpc_calls_lock:
            call = self._block_submitter_rpc_calls.get(block_hash)
            if call is None:
                if not self._block_submitter_rpc_worker_slots.acquire(
                    blocking=False
                ):
                    oldest_started = min(
                        (
                            pending.started_monotonic
                            for pending in self._block_submitter_rpc_calls.values()
                            if not pending.done.is_set()
                        ),
                        default=time.monotonic(),
                    )
                    self._maybe_restart_for_stuck_block_call(
                        kind="rpc-worker-pool",
                        started_monotonic=oldest_started,
                    )
                    raise TimeoutError(
                        "submitblock could not acquire a bounded RPC worker"
                    )
                call = _BlockSubmitterRpcCall()
                self._block_submitter_rpc_calls[block_hash] = call

                def run() -> None:
                    try:
                        call.result = self._rpc_call_with_timeout(
                            "submitblock",
                            [block_hex],
                            timeout_seconds=timeout_seconds,
                        )
                    except BaseException as exc:
                        call.error = exc
                    finally:
                        call.done.set()
                        self._block_submitter_rpc_worker_slots.release()

                threading.Thread(
                    target=run,
                    name=f"prism-block-rpc-{block_hash[:12]}",
                    daemon=True,
                ).start()

        deadline = time.monotonic() + timeout_seconds
        while not call.done.is_set():
            self._coordinator._record_block_submitter_wait("submitblock-rpc")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._maybe_restart_for_exhausted_block_call_pool(
                    kind="rpc-worker-pool",
                    calls_lock=self._block_submitter_rpc_calls_lock,
                    calls=self._block_submitter_rpc_calls,
                )
                raise TimeoutError(
                    f"submitblock exceeded {timeout_seconds:g}s"
                )
            call.done.wait(
                min(remaining, self._block_work_wait_slice())
            )
        with self._block_submitter_rpc_calls_lock:
            if self._block_submitter_rpc_calls.get(block_hash) is call:
                self._block_submitter_rpc_calls.pop(block_hash, None)
        if call.error is not None:
            raise call.error
        return call.result

    # -- node offer --------------------------------------------------------

    def _arm_block_candidate_after_node_offer(
        self,
        candidate: PrismBlockCandidate,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> None:
        """Fence child payout work as soon as node acceptance is possible."""
        self._coordinator._stash_retained_block_candidate_node_submission(
            str(candidate.submission.block_hash_hex),
            node_submission,
        )
        ambiguous_or_landed = (
            node_submission.error is not None
            or node_submission.result in (None, "duplicate")
        )
        if not ambiguous_or_landed:
            self._coordinator._release_block_fast_lane_slot(
                str(candidate.submission.block_hash_hex)
            )
            return
        block_hash = str(candidate.submission.block_hash_hex).lower()
        expected_height = int(candidate.context.template["height"])
        self._coordinator._begin_accepted_block_payout_preview(
            block_hash,
            block_height=expected_height,
        )

    def _submit_block_candidate_to_node(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Offer the durable candidate to qbitd before any accounting work."""
        block_hash = str(candidate.submission.block_hash_hex).lower()
        self._coordinator._begin_accepted_block_payout_preview(
            block_hash,
            block_height=int(candidate.context.template["height"]),
        )
        self._coordinator._register_outstanding_block_candidate(block_hash)
        self._coordinator._record_block_submitter_phase("submitblock-rpc")
        timeout_seconds = max(
            0.001,
            float(
                getattr(
                    self._coordinator,
                    "block_submit_rpc_timeout_seconds",
                    DEFAULT_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS,
                )
            ),
        )
        try:
            result = self._coordinator._run_submitblock_rpc_with_hard_deadline(
                block_hash=block_hash,
                block_hex=str(candidate.submission.block_hex),
                timeout_seconds=timeout_seconds,
            )
        except BaseException as exc:
            self._coordinator._record_block_submitter_phase("submitblock-rpc:error")
            node_submission = _BlockCandidateNodeSubmission(
                attempted=True,
                error=exc,
            )
            self._coordinator._arm_block_candidate_after_node_offer(
                candidate,
                node_submission,
            )
            return node_submission
        self._coordinator._record_block_submitter_phase("submitblock-rpc:complete")
        landed_monotonic = getattr(candidate, "landed_monotonic", None)
        if landed_monotonic is not None:
            self._coordinator._observe_block_submit_seconds(
                time.monotonic() - float(landed_monotonic)
            )
        node_submission = _BlockCandidateNodeSubmission(
            attempted=True,
            result=result,
        )
        if _is_definitive_node_acceptance(node_submission):
            # Stamp the start of issue #181's acceptance-to-publication
            # interval from the same predicate the lane routing and the
            # retained-offer stash read, so "the node has this block" has
            # exactly one definition in this module.
            self._note_accepted_block_preview_acceptance(block_hash)
        self._coordinator._arm_block_candidate_after_node_offer(
            candidate,
            node_submission,
        )
        return node_submission

    def _node_submission_for_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Choose the node fast lane unless the pool was already closed."""
        block_hash = str(candidate.submission.block_hash_hex).lower()
        self._coordinator._record_block_submitter_phase("fast-lane-admission")
        with self._coordinator.lock:
            accounted_hashes = getattr(
                self._coordinator,
                "_accounted_accepted_block_hashes",
                set(),
            )
            accepted_count = int(self._coordinator.accepted_block_count)
            pool_closed = block_hash not in accounted_hashes and (
                accepted_count >= int(self._coordinator.max_blocks)
                or (
                    bool(self._coordinator.stop_after_block)
                    and accepted_count >= 1
                )
            )
        if pool_closed:
            return _BlockCandidateNodeSubmission(attempted=False)
        return self._coordinator._submit_block_candidate_to_node(candidate)

    def _node_submission_for_candidate_or_retained(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Reuse a retained definitive acceptance instead of re-offering.

        An in-process retry of a candidate whose earlier offer already
        returned success must not ask the node again: the re-offer answers
        "duplicate", which downgrades the classification to the moved live
        tip and leans on chain probes that may be unavailable under the
        same saturation that caused the retry. The stashed result reruns
        the landing tail as if the first pass had continued.
        """
        retained = self._coordinator._retained_block_candidate_node_submission(
            str(candidate.submission.block_hash_hex)
        )
        if retained is not None:
            return retained
        return self._coordinator._node_submission_for_candidate(candidate)

    def _node_submission_for_direct_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission:
        """Preserve active-replay semantics for non-queue embedders.

        The dedicated submitter always uses the unconditional fast lane. A
        direct caller can instead be resuming a durable active ancestor, for
        which another submit is unnecessary and some integrations do not
        retain block bytes. This compatibility probe is not on the incident
        queue-to-node path.
        """
        block_hash = str(candidate.submission.block_hash_hex).lower()
        expected_height = int(candidate.context.template["height"])
        try:
            if str(self._coordinator.rpc.call("getbestblockhash")).lower() == block_hash:
                return _BlockCandidateNodeSubmission(attempted=False)
        except Exception:
            pass
        try:
            if (
                self._coordinator.active_block_candidate_height(block_hash)
                == expected_height
            ):
                return _BlockCandidateNodeSubmission(attempted=False)
        except Exception:
            pass
        if not hasattr(candidate.submission, "block_hex"):
            return _BlockCandidateNodeSubmission(attempted=False)
        return self._coordinator._node_submission_for_candidate(candidate)

    def _account_block_candidate_after_node_submit(
        self,
        candidate: PrismBlockCandidate,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> bool:
        """Pass fast-lane evidence while tolerating legacy test embedders."""
        submit = self._coordinator.submit_block_candidate
        supports_node_submission = True
        try:
            parameters = inspect.signature(submit).parameters.values()
            supports_node_submission = any(
                parameter.name == "node_submission"
                or parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            )
        except (TypeError, ValueError):
            pass
        if supports_node_submission:
            return bool(submit(candidate, node_submission=node_submission))
        return bool(submit(candidate))

    def _submit_synchronous_block_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> bool:
        """Run the rare miner-facing path under one same-hash disposition."""
        coordinator = self._coordinator
        block_hash = str(candidate.submission.block_hash_hex).lower()
        with coordinator._block_candidate_disposition(block_hash):
            terminal_outcome = coordinator._block_candidate_terminal_outcome(
                block_hash
            )
            if terminal_outcome is not None:
                coordinator._finish_pending_share_commit(candidate.pending_share)
                return terminal_outcome
            outcome = self.outcome
            # The accepted/rejected accounting tail may already be complete
            # while only its durable outbox terminal update is retrying. A
            # synchronous same-hash waiter must join that finalize-only state;
            # another node offer/accounting pass could invert the outcome.
            with coordinator.lock:
                pending_finalize = self.finalize_retries.get(block_hash)
            if pending_finalize is not None:
                accepted, error = pending_finalize
                coordinator._finalize_block_candidate(
                    candidate,
                    block_hash=block_hash,
                    accepted=accepted,
                    error=error,
                    outcome=outcome,
                )
                return accepted
            try:
                if not coordinator._reserve_block_fast_lane_slot(block_hash):
                    raise RuntimeError(
                        "block candidate is waiting for fast-lane capacity"
                    )
                node_submission = coordinator._node_submission_for_candidate_or_retained(candidate)
                coordinator._mark_block_candidate_attempted(block_hash)
                with coordinator._block_landing_ledger_statement_timeout_scope(block_hash):
                    # The same-hash disposition is already held here, so the
                    # coordinator may run its serialized inner tail rather
                    # than the public entrypoint that would take that guard
                    # again. Which of the two applies is its call to make.
                    block_landed = self.ports.submit_candidate(
                        candidate,
                        node_submission=node_submission,
                        disposition_held=True,
                    )
            except BaseException:
                coordinator._retain_block_candidate_for_retry(candidate)
                raise

            if not block_landed:
                reason = getattr(outcome, "reason", None)
                if reason in {None, *self.retryable_reasons}:
                    coordinator._retain_block_candidate_for_retry(candidate)
                    raise RuntimeError(
                        "block candidate outcome is pending durable retry"
                    )
                abandon_error = getattr(outcome, "error", None)
                try:
                    coordinator._record_block_submitter_phase(
                        "reject-prepared-block"
                    )
                    with coordinator._block_landing_ledger_statement_timeout_scope(
                        block_hash
                    ):
                        coordinator._reject_terminal_prepared_block_candidate(candidate)
                    coordinator._record_block_submitter_phase(
                        "reject-prepared-block:complete"
                    )
                except Exception as exc:
                    # A prior attempt may have persisted prepared payout rows
                    # before this synchronous resubmit reached a false terminal
                    # verdict. Keep the outbox pending until those rows can be
                    # rejected; otherwise restart replay is removed while its
                    # balance transition remains live.
                    coordinator._defer_block_candidate(
                        "backend-rpc-unavailable",
                        "could not reject prepared state for terminal candidate",
                        worker=candidate.client.username or None,
                    )
                    coordinator._retain_block_candidate_for_retry(candidate)
                    raise RuntimeError(
                        "could not reject prepared state for terminal candidate"
                    ) from exc
                coordinator._finalize_block_candidate(
                    candidate,
                    block_hash=block_hash,
                    accepted=False,
                    error=str(abandon_error or reason),
                    outcome=outcome,
                )
                return False

            coordinator._finalize_block_candidate(
                candidate,
                block_hash=block_hash,
                accepted=True,
                error="",
                outcome=outcome,
            )
            return True

    # -- same-hash disposition guard ---------------------------------------

    def _ensure_block_candidate_disposition_state(self) -> None:
        """Backfill same-hash submission guards for lightweight embedders."""
        if (
            hasattr(self, "_block_candidate_disposition_registry_lock")
            and hasattr(self, "_block_candidate_disposition_flights")
            and hasattr(self, "_block_candidate_terminal_outcomes")
            and hasattr(self, "_block_fast_lane_reservations")
            and hasattr(self, "_block_disposition_waiting_retries")
            and hasattr(self, "_block_candidate_dequeued_hashes")
        ):
            return
        with _STATE_BACKFILL_LOCK:
            if not hasattr(self, "_block_candidate_disposition_registry_lock"):
                self._block_candidate_disposition_registry_lock = threading.Lock()
            if not hasattr(self, "_block_candidate_disposition_flights"):
                self._block_candidate_disposition_flights: dict[
                    str, _BlockCandidateDispositionFlight
                ] = {}
            if not hasattr(self, "_block_candidate_terminal_outcomes"):
                self._block_candidate_terminal_outcomes: dict[str, bool] = {}
            if not hasattr(self, "_block_fast_lane_reservations"):
                self._block_fast_lane_reservations: set[str] = set()
            if not hasattr(self, "_block_disposition_waiting_retries"):
                self._block_disposition_waiting_retries: dict[
                    str, PrismBlockCandidate
                ] = {}
            if not hasattr(self, "_block_candidate_dequeued_hashes"):
                # hash -> number of dequeued same-hash candidate objects that
                # are between a lane and their disposition flight (or the
                # waiting registry). Read and written under coordinator.lock.
                self._block_candidate_dequeued_hashes: dict[str, int] = {}

    def _claim_block_candidate_disposition(
        self,
        block_hash: str,
        *,
        blocking: bool,
    ) -> _BlockCandidateDispositionLease | None:
        """Claim one hash without making unrelated node offers wait."""
        key = block_hash.lower()
        self._ensure_block_candidate_disposition_state()
        with self._block_submitter_lock(
            self._block_candidate_disposition_registry_lock,
            "candidate-disposition-registry",
        ):
            flight = self._block_candidate_disposition_flights.get(key)
            if flight is None:
                flight = _BlockCandidateDispositionFlight()
                self._block_candidate_disposition_flights[key] = flight
            flight.users += 1
        if blocking:
            self._acquire_block_submitter_lock(
                flight.lock,
                f"candidate-disposition:{key}",
            )
            acquired = True
        else:
            acquired = flight.lock.acquire(blocking=False)
        if acquired:
            return _BlockCandidateDispositionLease(key, flight)
        self._drop_block_candidate_disposition_user(key, flight)
        return None

    def _drop_block_candidate_disposition_user(
        self,
        key: str,
        flight: _BlockCandidateDispositionFlight,
    ) -> None:
        with self._block_submitter_lock(
            self._block_candidate_disposition_registry_lock,
            "candidate-disposition-registry",
        ):
            flight.users -= 1
            if (
                flight.users == 0
                and self._block_candidate_disposition_flights.get(key) is flight
            ):
                self._block_candidate_disposition_flights.pop(key, None)

    def _release_block_candidate_disposition(
        self,
        lease: _BlockCandidateDispositionLease,
    ) -> None:
        lease.flight.lock.release()
        self._drop_block_candidate_disposition_user(
            lease.block_hash,
            lease.flight,
        )

    # -- dequeued-hash pin -------------------------------------------------

    def _pin_dequeued_block_candidate_locked(
        self,
        candidate: PrismBlockCandidate,
    ) -> str:
        """Name a candidate's hash from the instant it leaves its lane.

        Caller holds ``coordinator.lock`` and, in the same critical section,
        has just taken ``candidate`` out of the retry holder, a candidate
        queue, or the waiting registry. Until ``submit_next`` hands the hash
        to its disposition flight or back to the waiting registry -- again
        under the lock -- the candidate object lives only in that method's
        local variable, and this pin is the only thing that tells the
        terminal-outcome eviction a same-hash copy is still in memory. The
        lanes and the flight each pin the hash in their own right; without
        this entry a terminal duplicate's fence is the oldest unpinned
        outcome exactly while the pass that is about to read it holds the
        candidate in hand.

        Counted rather than set-valued so two dequeued same-hash objects
        (a second caller of ``submit_next``) cannot unpin each other.
        Returns the lowercase key the rest of the pass compares on.
        """
        block_hash = str(candidate.submission.block_hash_hex).lower()
        pins = self._block_candidate_dequeued_hashes
        pins[block_hash] = pins.get(block_hash, 0) + 1
        return block_hash

    def _unpin_dequeued_block_candidate_locked(self, block_hash: str) -> None:
        """Drop one dequeue's pin. Caller holds ``coordinator.lock``.

        Called only in the critical section that hands the hash on -- to the
        flight the claim installed, or to the waiting registry -- or where
        nothing needs it any more because the candidate object left with an
        exception and no in-memory copy remains to offer the block.
        """
        pins = self._block_candidate_dequeued_hashes
        remaining = pins.get(block_hash, 0) - 1
        if remaining > 0:
            pins[block_hash] = remaining
        else:
            pins.pop(block_hash, None)

    def _live_block_candidate_hash_registries(self) -> tuple[Any, ...]:
        """Registries whose named hash forbids evicting its terminal outcome.

        Each one either holds a candidate object that has not reached the
        node yet or the still-owed terminal work of one, and every such copy
        reads the terminal outcome before it may offer the block again.
        Dropping the outcome under one of them is exactly the window
        ``_publish_collapsed_candidate_terminal_fence`` exists to close.
        The dequeued-hash pin is the lane between lanes: the candidate
        ``submit_next`` has taken out of a holder, queue, or the waiting
        registry and not yet handed to its disposition flight.

        Read with ``coordinator.lock`` held. Backfilled state is fetched by
        ``getattr`` rather than by an ``_ensure_...`` call, which would take
        the state-backfill lock underneath the global one.
        """
        return tuple(
            registry
            for registry in (
                getattr(self, "_outstanding_block_candidate_hashes", None),
                getattr(self, "_block_replay_inflight_hashes", None),
                getattr(self, "_block_quarantine_hashes", None),
                getattr(self, "_block_fast_lane_reservations", None),
                getattr(self, "_block_disposition_waiting_retries", None),
                getattr(self, "finalize_retries", None),
                getattr(self, "_block_candidate_retained_node_submissions", None),
                getattr(self, "_block_candidate_collapse_cleanup_retries", None),
                getattr(self, "_block_candidate_collapse_cleanup_inflight", None),
                getattr(self, "_block_candidate_dequeued_hashes", None),
            )
            if registry
        )

    def _held_block_candidate_retry_hashes(self) -> frozenset[str]:
        """The hashes the single-slot retry holders are still carrying."""
        held: set[str] = set()
        for holder in (
            getattr(self, "retry_candidate", None),
            getattr(self, "_block_accounting_deferred_retry_candidate", None),
        ):
            key = _block_candidate_hash_of(holder)
            if key is not None:
                held.add(key)
        return frozenset(held)

    @staticmethod
    def _queued_block_candidate_hashes(queue_obj: object) -> frozenset[str] | None:
        """The hashes still sitting in one candidate queue, or None if unread.

        The registries above stop naming a hash the moment its terminal
        outcome is recorded -- ``_record_block_candidate_terminal_outcome``
        and the collapse cleanup's ``outstanding-and-tip-observation`` step
        clear the outstanding and replay-inflight markers by design -- so a
        *duplicate* copy of that hash still queued behind the pass that
        finished would be named by nothing. That copy is precisely the one
        the fence protects, so the queues themselves are read.

        The underlying deque is read under the queue's own mutex, because
        iterating it while a producer or consumer mutates it is not safe.
        The acquire is non-blocking: this runs with ``coordinator.lock``
        held, and waiting there for a queue's leaf lock would convoy the
        whole coordinator behind an unrelated enqueue. Failing to take it
        returns None and the caller skips the eviction pass entirely, which
        only leaves the registry over its bound until the next outcome.
        """
        if queue_obj is None:
            return frozenset()
        mutex = getattr(queue_obj, "mutex", None)
        entries = getattr(queue_obj, "queue", None)
        if mutex is None or entries is None:
            return frozenset()
        if not mutex.acquire(blocking=False):
            return None
        try:
            items = list(entries)
        finally:
            mutex.release()
        queued: set[str] = set()
        for item in items:
            key = _block_candidate_hash_of(item)
            if key is not None:
                queued.add(key)
        return frozenset(queued)

    def _in_flight_block_candidate_hashes(self) -> frozenset[str] | None:
        """The hashes with a live same-hash disposition flight, or None.

        A claimed flight means a pass is between its node offer and its
        durable finalization for that hash. Same non-blocking rule as the
        queues: the disposition registry lock is taken under
        ``coordinator.lock`` here, the reverse of the ordinary order, so it
        is only ever tried, never waited on.
        """
        lock = getattr(self, "_block_candidate_disposition_registry_lock", None)
        flights = getattr(self, "_block_candidate_disposition_flights", None)
        if lock is None or flights is None:
            return frozenset()
        if not lock.acquire(blocking=False):
            return None
        try:
            return frozenset(flights)
        finally:
            lock.release()

    def _collect_live_block_candidate_pins(
        self,
    ) -> tuple[tuple[Any, ...], frozenset[str]] | None:
        """The one live pin set every hash-liveness proof reads.

        Returns the lane registries (checked by membership) and the union of
        the retry-holder, queued (live and replay), and claimed-flight
        hashes, or None when a leaf lane could not be read without waiting
        under the global lock. Read with ``coordinator.lock`` held. Both the
        terminal-outcome eviction and the ancestor re-drive's ownership
        check (issue #190) sit on this collector, deliberately: each one's
        safety argument is that a hash named by none of these sources has no
        in-memory copy left, and a lane added to one consumer but not the
        other would silently void the proof of whichever fell behind.
        """
        pinned = self._live_block_candidate_hash_registries()
        held = set(self._held_block_candidate_retry_hashes())
        for live in (
            self._queued_block_candidate_hashes(
                getattr(self, "candidate_queue", None)
            ),
            self._queued_block_candidate_hashes(
                getattr(self, "_block_replay_candidate_queue", None)
            ),
            self._in_flight_block_candidate_hashes(),
        ):
            if live is None:
                return None
            held |= live
        return pinned, frozenset(held)

    @staticmethod
    def _stamp_block_candidate_terminal_outcome(
        outcomes: dict[str, bool],
        block_hash: str,
        accepted: bool,
    ) -> None:
        """Record one outcome as the newest entry of the FIFO registry.

        Re-recording an existing hash would otherwise keep its original
        insertion position, leaving a freshly reasserted outcome at the
        front of the eviction scan.
        """
        outcomes.pop(block_hash, None)
        outcomes[block_hash] = accepted

    def _bound_block_candidate_terminal_outcomes(
        self,
        protect: frozenset[str] = frozenset(),
    ) -> int:
        """Trim the terminal-outcome registry to its bound; return evictions.

        Called by the two writers with ``coordinator.lock`` already held and
        their own entries already stamped. ``protect`` names the hashes this
        very call published: a bulk fence page larger than the whole bound
        must not evict the outcomes it just wrote, so the registry is left
        temporarily over its bound instead.

        Eviction is oldest-first and skips every pinned hash, so the bound
        is a target rather than a hard ceiling -- a registry whose entries
        are all still live simply stays large, which is the safe direction.
        The pins are themselves bounded state, so the total stays bounded.

        The pin set is the union of the lane registries (including the
        dequeued-hash pin), the single-slot retry holders, the candidate
        objects still queued on the live and replay lanes, and the hashes
        with a claimed disposition flight. A hash held by none of those has
        no in-memory copy left that could reach a node offer; anything
        arriving for it afterwards is a fresh admission, which re-reads the
        durable outbox row -- terminal there either way -- before it can
        offer anything.

        That same proof retires the hash's counted-abandonment dedup key.
        The key exists so a re-run terminal disposition -- a collapse whose
        cleanup failed, a finalize-only replay -- counts its candidate once
        rather than once per attempt, so it may only be dropped when no
        copy is left to re-run anything. Both writer orderings are covered
        by the one rule: the direct finalize path counts before it publishes
        its terminal outcome, so its key is retired by a later pass over the
        outcome it goes on to publish, and an ambiguous false finalize
        failure has no outcome to evict at all while ``finalize_retries``
        pins it; the collapse publishes the outcome first and counts during
        cleanup, and holds the hash's disposition lease -- an in-flight pin
        -- across both. Dropping the key here therefore never lets a lane
        that is still owed accounting count its candidate twice.
        """
        outcomes = self._block_candidate_terminal_outcomes
        overflow = len(outcomes) - MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES
        if overflow <= 0:
            return 0
        pins = self._collect_live_block_candidate_pins()
        if pins is None:
            # A lane could not be read without waiting under the global
            # lock. Evicting against an incomplete pin set could unfence
            # a live copy, so nothing is evicted this round.
            return 0
        pinned, held = pins
        counted = getattr(self, "_counted_block_candidate_abandonments", None)
        window = overflow + BLOCK_CANDIDATE_TERMINAL_OUTCOME_EVICTION_SCAN
        dropped = 0
        for key in list(itertools.islice(outcomes, window)):
            if dropped >= overflow:
                break
            if (
                key in protect
                or key in held
                or any(key in registry for registry in pinned)
            ):
                # A live, replayed, retried, parked, quarantined, or
                # cleanup-owing copy of this hash is still in memory and
                # will read this outcome before it could offer the block.
                # Move it behind the scan window so it stops blocking the
                # next pass instead of dropping it.
                outcomes[key] = outcomes.pop(key)
                continue
            del outcomes[key]
            if counted is not None:
                # Retired under the very proof that let the fence go: a
                # dedup key outliving its fence is what let this set grow
                # without bound under a collapsed storm.
                counted.discard(key)
            dropped += 1
        return dropped

    def _block_candidate_terminal_outcome(self, block_hash: str) -> bool | None:
        self._ensure_block_candidate_disposition_state()
        with self._coordinator.lock:
            return self._block_candidate_terminal_outcomes.get(block_hash.lower())

    def _record_block_candidate_terminal_outcome(
        self,
        block_hash: str,
        *,
        accepted: bool,
    ) -> None:
        self._ensure_block_candidate_disposition_state()
        dropped_waiting: PrismBlockCandidate | None = None
        with self._coordinator.lock:
            key = block_hash.lower()
            self._stamp_block_candidate_terminal_outcome(
                self._block_candidate_terminal_outcomes,
                key,
                accepted,
            )
            self._bound_block_candidate_terminal_outcomes(frozenset((key,)))
            self._block_fast_lane_reservations.discard(key)
            replay_hashes = getattr(self, "_block_replay_inflight_hashes", None)
            if replay_hashes is not None:
                replay_hashes.discard(key)
            waiting = getattr(self, "_block_disposition_waiting_retries", None)
            if waiting is not None:
                dropped_waiting = waiting.pop(key, None)
        if dropped_waiting is not None:
            # The parked same-hash wakeup dies with the terminal outcome
            # recorded; its floor holder must not outlive it.
            self._release_dropped_duplicate_candidate_floor(dropped_waiting)

    def _record_committed_block_candidate_abandonment(
        self,
        block_hash: str,
        outcome: threading.local,
    ) -> None:
        """Count an abandonment only after its terminal cleanup is fixed.

        ``_abandon_block_candidate`` seals a proposed rejection before any
        prepared payout rows are removed. That cleanup can fail, in which
        case the candidate is deliberately re-registered and can still prove
        accepted on a later pass. Counting at the seal would then expose the
        same hash as both abandoned and accepted. The writer calls this only
        after cleanup succeeds or after a false finalize-only disposition is
        installed; direct accounting callers invoke it only after the full
        serialized rejection path returns.

        The dedup key that makes this once-per-candidate is retired by
        ``_bound_block_candidate_terminal_outcomes`` when that pass evicts
        the hash's terminal outcome, which is the only place that has proved
        no lane can re-enter this method for the hash.
        """
        reason = getattr(outcome, "reason", None)
        if not isinstance(reason, str) or not reason:
            return
        if reason in self.retryable_reasons:
            return
        stale_job_class = getattr(outcome, "stale_job_class", None)
        key = block_hash.lower()
        with self._coordinator.lock:
            counted_abandonments = self._counted_block_candidate_abandonments
            if key in counted_abandonments:
                return
            counted_abandonments.add(key)
            counts = self.abandoned_counts
            if counts is None:
                counts = {}
                self.abandoned_counts = counts
            counts[reason] = int(counts.get(reason, 0)) + 1
            if stale_job_class is not None:
                stale_counts = getattr(self, "stale_job_abandon_counts", None)
                if stale_counts is None:
                    stale_counts = {
                        abandon_class: 0
                        for abandon_class in PRISM_STALE_JOB_ABANDON_CLASSES
                    }
                    self.stale_job_abandon_counts = stale_counts
                stale_counts[stale_job_class] = (
                    int(stale_counts.get(stale_job_class, 0)) + 1
                )

    def _reserve_block_fast_lane_slot(self, block_hash: str) -> bool:
        """Reserve pool capacity while a node offer awaits terminal accounting."""
        key = block_hash.lower()
        self._ensure_block_candidate_disposition_state()
        with self._coordinator.lock:
            reservations = self._block_fast_lane_reservations
            if key in reservations:
                return True
            accepted_count = int(
                getattr(self._coordinator, "accepted_block_count", 0)
            )
            capacity = int(getattr(self._coordinator, "max_blocks", 2**31 - 1))
            stop_after_one = bool(
                getattr(self._coordinator, "stop_after_block", False)
            )
            reserved_count = len(reservations)
            if accepted_count + reserved_count >= capacity:
                return False
            if stop_after_one and accepted_count + reserved_count >= 1:
                return False
            reservations.add(key)
            return True

    def _release_block_fast_lane_slot(self, block_hash: str) -> None:
        self._ensure_block_candidate_disposition_state()
        with self._coordinator.lock:
            reservations = self._block_fast_lane_reservations
            if reservations is not None:
                reservations.discard(block_hash.lower())

    @contextmanager
    def _block_candidate_disposition(
        self,
        block_hash: str,
    ) -> Iterator[_BlockCandidateDispositionLease]:
        """Serialize the full accepted/abandoned decision for one hash.

        A below-share-target solve submits synchronously while the durable
        outbox can concurrently replay that same candidate. Keep both attempts
        ordered until the accepted success tail records its process-local
        completion; otherwise the replay can terminally abandon the outbox
        during the gap after durable confirmation but before audit/share
        evidence is complete.
        """
        lease = self._coordinator._claim_block_candidate_disposition(
            block_hash,
            blocking=True,
        )
        assert lease is not None
        try:
            yield lease
        finally:
            self._coordinator._release_block_candidate_disposition(lease)

    # -- accounting actor --------------------------------------------------

    def _ensure_block_accounting_state(self) -> None:
        if not hasattr(self, "_block_accounting_state_lock"):
            self._block_accounting_state_lock = threading.Lock()
        if not hasattr(self, "_block_accounting_queue"):
            depth = max(
                1,
                int(
                    getattr(
                        self._coordinator,
                        "block_accounting_queue_depth",
                        DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH,
                    )
                ),
            )
            self._block_accounting_queue = queue.PriorityQueue(maxsize=depth)
        if not hasattr(self, "_block_accounting_overflow_queue"):
            self._block_accounting_overflow_queue = queue.PriorityQueue()
        if not hasattr(self, "_block_accounting_accepted_queue"):
            # Deliberately unbounded, for the reason the overflow queue is:
            # a node offer has already happened and must never be converted
            # back into a raw-submit retry. A maxsize here would need a spill
            # target, and any queue-state-dependent spill is exactly the
            # inversion this lane removes. Max-block admission
            # (_reserve_block_fast_lane_slot, max_blocks, stop_after_block)
            # and the physical block-acceptance rate bound how many
            # unresolved real offers can exist at once.
            self._block_accounting_accepted_queue = queue.PriorityQueue()
        if not hasattr(self, "_block_accounting_sequence"):
            self._block_accounting_sequence = 0
        if not hasattr(self, "_block_accounting_thread"):
            self._block_accounting_thread = None

    def _start_block_accounting_thread(self) -> threading.Thread:
        self._ensure_block_accounting_state()
        with self._block_accounting_state_lock:
            thread = self._block_accounting_thread
            if thread is not None and thread.is_alive():
                return thread
            self._record_block_work_heartbeat("block_accounting", "starting")
            thread = threading.Thread(
                target=self._coordinator.block_accounting_loop,
                name="prism-block-accounting",
                daemon=True,
            )
            self._block_accounting_thread = thread
            thread.start()
            return thread

    def _enqueue_block_accounting_task(
        self,
        task: _BlockCandidateAccountingTask,
    ) -> bool:
        self._ensure_block_accounting_state()
        with self._block_accounting_state_lock:
            sequence = self._block_accounting_sequence
            self._block_accounting_sequence += 1
        priority = int(task.candidate.context.template["height"])
        item = (priority, sequence, task)
        if _is_definitive_node_acceptance(task.node_submission):
            # Definitive acceptance leaves the primary/overflow population
            # entirely. It does not get a better key inside it -- a key there
            # is inert exactly when it matters. Under a burst the spillover
            # rule below has already engaged, so the accepted task would be
            # put behind the spill while the stale entries still sitting in
            # the bounded primary queue -- up to
            # DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH of them -- are all served
            # ahead of it, each costing ~6 RPCs, two ledger writes and a
            # payout-balance mutation. Priority only ever orders one heap; the
            # primary-then-overflow dequeue discipline is what inverts it.
            #
            # Membership is what changes here, not the spillover rule. That
            # rule is unchanged below, and the property it exists to protect --
            # once spillover begins, later handoffs join it rather than
            # repeatedly refilling the bounded primary, so older spill entries
            # are not starved by newer arrivals -- still governs exactly the
            # population it was written for: non-definitive accounting work.
            #
            # A separate lane makes this class structurally un-spillable: no
            # queue-state-dependent rule can place it behind stale work,
            # because it shares no queue with stale work. Fairness for the
            # stale lanes stops being an emergent property of two heaps and
            # becomes the explicit dispatch quota
            # BLOCK_ACCOUNTING_ACCEPTED_DISPATCH_QUOTA, enforced in
            # block_accounting_loop and bounded there rather than argued for.
            self._block_accounting_accepted_queue.put_nowait(item)
            return True
        if not self._block_accounting_overflow_queue.empty():
            # Once spillover begins, keep later handoffs behind it instead of
            # repeatedly refilling the primary queue and starving older spill
            # entries.
            self._block_accounting_overflow_queue.put_nowait(item)
            return True
        try:
            self._block_accounting_queue.put_nowait(item)
            return True
        except queue.Full:
            # A node offer has already happened and must never be converted
            # back into a raw-submit retry. Preserve its result and lease in
            # an unbounded, process-local overflow queue; max-block admission
            # bounds the number of unresolved real offers.
            self._block_accounting_overflow_queue.put_nowait(item)
            print(
                "prism coordinator: block accounting handoff spilled "
                f"hash={task.candidate.submission.block_hash_hex}",
                flush=True,
            )
            return True

    def _call_block_candidate_writer(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission,
        disposition_held: bool,
    ) -> bool:
        """Invoke the writer while preserving duck-typed test integrations."""
        writer = self._coordinator._submit_next_block_candidate_writer
        supports_disposition_held = True
        try:
            parameters = inspect.signature(writer).parameters.values()
            supports_disposition_held = any(
                parameter.name == "disposition_held"
                or parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            )
        except (TypeError, ValueError):
            pass
        if supports_disposition_held:
            return bool(
                writer(
                    candidate,
                    node_submission=node_submission,
                    disposition_held=disposition_held,
                )
            )
        return bool(writer(candidate, node_submission=node_submission))

    def _restore_replayed_candidate_acceptance_evidence(
        self,
        candidate: PrismBlockCandidate,
    ) -> None:
        if not candidate.durable_replay:
            return
        block_hash = str(candidate.submission.block_hash_hex).lower()
        block_state = None
        state_read_failed = False
        state_reader = getattr(self._coordinator.ledger, "pool_block_state", None)
        if callable(state_reader):
            try:
                block_state = self._coordinator._run_block_submitter_ledger_call(
                    ("replay-pool-block-state", block_hash),
                    "replay-pool-block-state",
                    lambda: state_reader(block_hash=block_hash),
                )
            except Exception:
                traceback.print_exc()
                state_read_failed = True
        durable_chain_state = (
            str(block_state.get("chain_state", ""))
            if block_state is not None
            else ""
        )
        if state_read_failed or durable_chain_state in {"prepared", "confirmed"}:
            self._coordinator._register_outstanding_block_candidate(block_hash)
            with self._coordinator.lock:
                self._tip_observed_accepted_block_hashes[block_hash] = (
                    time.monotonic()
                )
            print(
                "prism coordinator: restored acceptance evidence for "
                f"replayed block candidate hash={block_hash} "
                + (
                    "after a failed durable-state read"
                    if state_read_failed
                    else f"chain_state={durable_chain_state}"
                ),
                flush=True,
            )

    def _run_block_accounting_task(
        self,
        task: _BlockCandidateAccountingTask,
    ) -> None:
        candidate = task.candidate
        with self._coordinator.lock:
            self._block_accounting_holds_disposition = True
            self._block_accounting_deferred_retry_candidate = None
        outcome = self.outcome
        outcome.refresh_client = None
        try:
            self._coordinator._restore_replayed_candidate_acceptance_evidence(
                candidate
            )
            with self._coordinator._writer_operation("accepted_block_handling"):
                self._coordinator._call_block_candidate_writer(
                    candidate,
                    node_submission=task.node_submission,
                    disposition_held=True,
                )
                refresh_client = getattr(outcome, "refresh_client", None)
                outcome.refresh_client = None
        except ShutdownInProgress:
            return
        except Exception:
            # An unexpected failure leaves the candidate to the retry lane.
            # Retain it here, while this thread still owns the disposition:
            # an accounting-owner retain routes into the deferred holder,
            # which names the hash until the finally below merges it into
            # the retry slot under coordinator.lock -- after the lease is
            # released, but with no instant in between where nothing names
            # it. The loop's catch used to retain only after this finally
            # had already released the lease, and that instant was exactly
            # the dequeued-candidate gap in its accounting-lane form: a
            # terminal-outcome eviction there could unfence the hash the
            # retried candidate was about to be re-disposed against.
            self._coordinator._retain_block_candidate_for_retry(candidate)
            task.retained_for_retry.set()
            raise
        finally:
            self._coordinator._release_block_candidate_disposition(
                task.disposition_lease
            )
            with self._coordinator.lock:
                self._block_accounting_holds_disposition = False
                deferred_retry = getattr(
                    self,
                    "_block_accounting_deferred_retry_candidate",
                    None,
                )
                self._block_accounting_deferred_retry_candidate = None
                if deferred_retry is not None:
                    self._merge_block_candidate_retry_locked(
                        "_retry_block_candidate",
                        deferred_retry,
                    )
        if refresh_client is not None and not self._coordinator.stop_event.is_set():
            self._coordinator._record_block_submitter_phase("refresh-jobs")
            self._coordinator.refresh_jobs_after_pending_accepted_block(
                refresh_client,
                heartbeat_name="block_accounting",
            )
            self._coordinator._record_block_submitter_phase("refresh-jobs:complete")

    def _block_accounting_cleanup_retry_work_items(self) -> int:
        """Completed work items the lane may run between cleanup offers.

        One work item is one finished ordinary accounting task from any of
        the handoff queues, or one finished invalid-candidate quarantine
        item. Bounded below at one so a misconfigured value degrades into
        offering the retry after every item rather than never offering it at
        all.
        """
        return max(
            1,
            int(
                getattr(
                    self._coordinator,
                    "block_accounting_cleanup_retry_work_items",
                    DEFAULT_BLOCK_ACCOUNTING_CLEANUP_RETRY_WORK_ITEMS,
                )
            ),
        )

    def _block_accounting_accepted_dispatch_quota(self) -> int:
        """Consecutive accepted dispatches allowed while stale work waits.

        Bounded below at one so a misconfigured value degrades into strict
        alternation -- one accepted task per stale task -- rather than into
        an accepted lane that can never dispatch while any stale work exists.
        """
        return max(
            1,
            int(
                getattr(
                    self._coordinator,
                    "block_accounting_accepted_dispatch_quota",
                    BLOCK_ACCOUNTING_ACCEPTED_DISPATCH_QUOTA,
                )
            ),
        )

    def block_accounting_loop(self) -> None:
        self._block_accounting_thread_ident = threading.get_ident()
        self._ensure_block_accounting_state()
        # Work items completed since the last deferred-cleanup offer. The
        # idle branch below cannot be the only driver: either lane running
        # continuously would hold a due terminal cleanup off for as long as
        # the traffic lasted, and its durable row is already gone. Counting
        # items here bounds that wait at one offer per configured cadence
        # whichever lane the traffic is on.
        work_items = 0
        # Definitive-acceptance tasks dispatched back to back since the last
        # non-accepted dispatch. Counted separately from work_items on
        # purpose: work_items paces the deferred-cleanup offer against *all*
        # completed work, while this counter measures one thing only -- how
        # long the accepted lane has been holding the stale lanes off. Never
        # fold the two together.
        accepted_run = 0
        while not self._coordinator.stop_event.is_set():
            self._coordinator._record_block_submitter_phase("accounting-queue")
            source_queue = None
            task = None
            # Selection order is the accepted lane's whole contract, and it
            # is stated as a bound rather than as a preference:
            #
            #   whenever non-accepted accounting work is available, at most
            #   `quota` accepted tasks are dispatched between two consecutive
            #   dispatches of non-accepted work.
            #
            # Equivalently, a non-accepted task at the head of its lane waits
            # behind at most `quota` accepted services, never behind an
            # unbounded stream of them, whatever the accepted arrival rate.
            # Starving it is not an option: a stale accounting task that never
            # runs leaves its outbox row pending, its offer-time payout-preview
            # barrier armed and its disposition lease held.
            if accepted_run < self._block_accounting_accepted_dispatch_quota():
                try:
                    _priority, _sequence, task = (
                        self._block_accounting_accepted_queue.get_nowait()
                    )
                    source_queue = self._block_accounting_accepted_queue
                    accepted_run += 1
                except queue.Empty:
                    pass
            if source_queue is None:
                # The stale population, dequeued with exactly the discipline
                # it has always had: bounded primary first, then the
                # unbounded spillover behind it.
                try:
                    _priority, _sequence, task = (
                        self._block_accounting_queue.get_nowait()
                    )
                    source_queue = self._block_accounting_queue
                except queue.Empty:
                    try:
                        _priority, _sequence, task = (
                            self._block_accounting_overflow_queue.get_nowait()
                        )
                        source_queue = self._block_accounting_overflow_queue
                    except queue.Empty:
                        pass
                if source_queue is not None:
                    accepted_run = 0
            if source_queue is None:
                # No stale work exists, so the quota has nothing to yield to
                # and is honoured vacuously; anything the quota was holding
                # back runs now, and the run counter restarts from this
                # dispatch rather than carrying an exhausted count forward.
                try:
                    _priority, _sequence, task = (
                        self._block_accounting_accepted_queue.get_nowait()
                    )
                    source_queue = self._block_accounting_accepted_queue
                    accepted_run = 0
                except queue.Empty:
                    pass
            if source_queue is None:
                if self._coordinator._run_one_invalid_block_candidate_quarantine():
                    work_items += 1
                    if (
                        work_items
                        >= self._block_accounting_cleanup_retry_work_items()
                    ):
                        work_items = 0
                        self._run_one_collapsed_block_candidate_cleanup_retry()
                    continue
                # A collapsed row's failed cleanup has no durable replay
                # source left, so the accounting lane is the only thing
                # that will ever finish it. A truly idle lane offers it
                # here immediately rather than waiting out a cadence; the
                # work-item cadence exists for the busy lanes, which
                # never reach this branch at all.
                work_items = 0
                if self._run_one_collapsed_block_candidate_cleanup_retry():
                    continue
                self._coordinator.stop_event.wait(self._block_work_wait_slice())
                continue
            try:
                self._coordinator._run_block_accounting_task(task)
            except Exception:
                print(
                    "prism coordinator: block accounting iteration failed; "
                    "durable candidate remains pending",
                    flush=True,
                )
                traceback.print_exc()
                retained = getattr(task, "retained_for_retry", None)
                if retained is None or not retained.is_set():
                    # The failure fired before _run_block_accounting_task's
                    # own exception path could retain the candidate -- a
                    # delegate raising ahead of it -- so it is retained here
                    # as before. A candidate the lane already retained (and
                    # merged into the retry slot) is not retained again.
                    self._coordinator._retain_block_candidate_for_retry(
                        task.candidate
                    )
            finally:
                assert source_queue is not None
                source_queue.task_done()
            work_items += 1
            if work_items >= self._block_accounting_cleanup_retry_work_items():
                # The counter resets on every offer, due record or not: a
                # cleanup backlog may therefore spend at most one attempt per
                # cadence, so it can never starve accounting or quarantine in
                # return. Deliberately left uncontained and outside the task's
                # try/finally -- the retry pops its record before attempting
                # it, and only its own failure paths re-register it, so an
                # outer catch here could swallow the last reference to a
                # terminal hash's owed steps.
                work_items = 0
                self._run_one_collapsed_block_candidate_cleanup_retry()

    # -- submitter loop ----------------------------------------------------

    def run(self) -> None:
        # Boundary stamps in _record_block_candidate_progress are gated to
        # this thread so client-thread dispositions cannot refresh the
        # submitter's liveness budget on its behalf.
        self._block_submitter_thread_ident = threading.get_ident()
        self._coordinator._start_block_accounting_thread()
        while not self.ports.stop_event().is_set():
            self._coordinator._record_block_submitter_phase("loop")
            try:
                # The in-memory wakeup is already backed by the durable
                # outbox. Drain it before any recovery query so a saturated
                # database cannot delay the first node submission.
                with self._coordinator.lock:
                    retry_ready = self.retry_candidate is not None
                queue_obj = self.candidate_queue
                replay_queue = getattr(
                    self,
                    "_block_replay_candidate_queue",
                    None,
                )
                wakeup_ready = bool(
                    (queue_obj is not None and not queue_obj.empty())
                    or (replay_queue is not None and not replay_queue.empty())
                )
                if (
                    retry_ready or wakeup_ready
                ) and self._coordinator.submit_next_block_candidate(
                    defer_accounting=True
                ):
                    if not self._ancestor_redrive_owed():
                        continue
                    # An armed ancestor re-drive (issue #190) falls through
                    # to the replay entrypoint even though immediate work
                    # succeeded: sustained live traffic taking this
                    # `continue` every pass is exactly how the wedge starved
                    # the one path that resolves a stuck ancestor.
                self.ports.replay_entrypoint()
                self._coordinator.submit_next_block_candidate(
                    timeout=1.0,
                    defer_accounting=True,
                )
            except ShutdownInProgress:
                # Admission can close after the loop condition. Durable block
                # candidates remain in the outbox for the replacement writer.
                return
            except Exception:
                phase = getattr(self, "_block_submitter_phase", "unknown")
                print(
                    "prism coordinator: block submitter iteration failed "
                    f"phase={phase}; durable candidates remain pending",
                    flush=True,
                )
                traceback.print_exc()
                retry_delay = float(self.retry_initial_seconds)
                if self._coordinator._wait_for_block_candidate_retry(retry_delay):
                    return

    def submit_next(
        self,
        timeout: float | None = None,
        *,
        defer_accounting: bool = False,
    ) -> BlockCandidateRunResult:
        """Dequeue and land one block candidate; ``ran`` is True when one ran.

        The block-submitter loop calls this continuously; tests call it
        directly (through the coordinator delegate) to drain the queue
        deterministically.
        """
        coordinator = self._coordinator
        coordinator._record_block_submitter_phase("dequeue-retry")
        # The disposition state owns the dequeued-hash pin, and its backfill
        # takes the state-backfill lock, so it runs before any coordinator
        # lock hold below rather than underneath one.
        self._ensure_block_candidate_disposition_state()
        # Every dequeue below leaves its lane and enters the dequeued-hash
        # pin in one coordinator.lock critical section. The lanes pin a
        # terminal duplicate's outcome against eviction while it sits in
        # them and the disposition flight pins it from the claim onward;
        # between the two the candidate object is only this method's local
        # variable, and the pin is what keeps the fence it is about to read
        # from being the oldest unpinned entry in the registry.
        with coordinator.lock:
            candidate = self.retry_candidate
            if candidate is not None and not self._block_candidate_retry_ready_locked(
                candidate
            ):
                # Parked by _pace_block_candidate_retry: honor the backoff
                # deadline without sleeping on the submitter or accounting
                # lane.
                candidate = None
            if candidate is not None:
                self.retry_candidate = None
                block_hash = self._pin_dequeued_block_candidate_locked(candidate)
        if candidate is None:
            queue_obj = self.candidate_queue
            self._ensure_block_replay_state()
            replay_queue = self._block_replay_candidate_queue
            if queue_obj is None and replay_queue is None:
                return BlockCandidateRunResult(False)
            deadline = (
                None
                if timeout is None
                else time.monotonic() + max(0.0, timeout)
            )
            while candidate is None:
                coordinator._record_block_submitter_phase("dequeue-queue")
                with coordinator.lock:
                    # Live discoveries always outrank durable restart replay.
                    # The non-blocking get takes the queue's own mutex under
                    # coordinator.lock -- the order the eviction's queue read
                    # already establishes -- so the entry leaves the queue
                    # and enters the pin without a gap between them.
                    for candidate_queue in (queue_obj, replay_queue):
                        if candidate_queue is None:
                            continue
                        try:
                            candidate = candidate_queue.get_nowait()
                        except queue.Empty:
                            continue
                        break
                    if candidate is None:
                        waiting = self._block_disposition_waiting_retries
                        ready_hashes = [
                            key
                            for key in waiting
                            if self._block_candidate_retry_ready_locked(
                                waiting[key]
                            )
                        ]
                        if ready_hashes:
                            waiting_hash = min(
                                ready_hashes,
                                key=lambda key: int(
                                    waiting[key].context.template["height"]
                                ),
                            )
                            candidate = waiting.pop(waiting_hash)
                    if candidate is not None:
                        block_hash = self._pin_dequeued_block_candidate_locked(
                            candidate
                        )
                if candidate is not None:
                    break
                if deadline is None:
                    return BlockCandidateRunResult(False)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return BlockCandidateRunResult(False)
                if coordinator.stop_event.wait(
                    min(remaining, self._block_work_wait_slice())
                ):
                    return BlockCandidateRunResult(False)

        try:
            lease = coordinator._claim_block_candidate_disposition(
                block_hash,
                blocking=not defer_accounting,
            )
        except BaseException:
            # The candidate object leaves with the exception (its durable
            # outbox row still replays), so no in-memory copy remains that
            # the pin would be protecting.
            with coordinator.lock:
                self._unpin_dequeued_block_candidate_locked(block_hash)
            raise
        if lease is None:
            # Another same-hash pass already spans node offer through durable
            # finalization. Keep this wakeup outside the global parent retry
            # slot until that lease transfers/releases: consuming it can lose
            # an accounting retry, while repeatedly prioritizing it can starve
            # unrelated live blocks. The waiting registry takes over the pin
            # in the same critical section.
            with coordinator.lock:
                self._block_disposition_waiting_retries[block_hash] = candidate
                self._unpin_dequeued_block_candidate_locked(block_hash)
            coordinator._wait_for_block_candidate_retry(
                float(self.retry_initial_seconds)
            )
            return BlockCandidateRunResult(True)
        # The claim installed this pass's disposition flight, which the
        # eviction reads as a pin of its own; hand the hash over to it.
        with coordinator.lock:
            self._unpin_dequeued_block_candidate_locked(block_hash)
        transferred = False
        if coordinator._block_candidate_terminal_outcome(block_hash) is not None:
            coordinator._release_block_candidate_disposition(lease)
            # Another same-hash object's disposition landed first; this
            # duplicate is dropped here and nothing later can release the
            # floor holder it carries.
            self._release_dropped_duplicate_candidate_floor(candidate)
            return BlockCandidateRunResult(True)

        outcome = self.outcome
        outcome.refresh_client = None
        coordinator._record_block_submitter_phase("finalize-registry")
        with coordinator.lock:
            pending_finalize = self.finalize_retries.get(block_hash)
        if pending_finalize is None:
            # Issue #181 item 2. A candidate whose parent is no longer the
            # best tip, whose height the chain has decided in favour of a
            # block of at least its work, and which carries no offer
            # evidence, is provably stale before any offer: skip it here
            # rather than pay a submitblock, ~6 chain reads, two ledger
            # writes, an accounting task and a payout-preview barrier to
            # learn the same thing per row.
            #
            # Placed after the lease claim and the terminal-outcome fence --
            # both of which this depends on -- and before the fast-lane
            # reservation, so a sibling on its way to being abandoned never
            # consumes max-block capacity. The skip terminalizes the durable
            # row itself (it does not drop the wakeup): the lease it holds
            # is already in #196's evidence set, so a dropped row would be
            # rejected by that selector forever, and the dequeued object's
            # pending-share floor holder can only be released by draining
            # this object.
            #
            # Guarded exactly as the node offer below is: the skip contains
            # its own failures and answers False for every one of them, but
            # an escape here would leave this hash's lease held forever and
            # drop the only object that can release its floor holder.
            try:
                stale = self._skip_superseded_block_candidate_at_dequeue(
                    candidate
                )
            except BaseException:
                try:
                    coordinator._retain_block_candidate_for_retry(candidate)
                finally:
                    coordinator._release_block_candidate_disposition(lease)
                raise
            if stale:
                coordinator._release_block_candidate_disposition(lease)
                return BlockCandidateRunResult(True)
            permanently_closed = False
            already_accounted = False
            if defer_accounting:
                with coordinator.lock:
                    accounted_hashes = getattr(
                        coordinator,
                        "_accounted_accepted_block_hashes",
                        set(),
                    )
                    already_accounted = block_hash in accounted_hashes
                    accepted_count = int(
                        getattr(coordinator, "accepted_block_count", 0)
                    )
                    permanently_closed = not already_accounted and (
                        accepted_count
                        >= int(getattr(coordinator, "max_blocks", 2**31 - 1))
                        or (
                            bool(getattr(coordinator, "stop_after_block", False))
                            and accepted_count >= 1
                        )
                    )
                if permanently_closed or already_accounted:
                    # Accounting must terminalize a durable outbox row even
                    # after pool capacity closes. An already-accounted hash
                    # likewise needs only its exact-idempotent/finalize tail.
                    node_submission = _BlockCandidateNodeSubmission(
                        attempted=False
                    )
                elif not coordinator._reserve_block_fast_lane_slot(block_hash):
                    # Capacity is provisionally occupied by another unresolved
                    # node offer. Preserve strict max-block semantics until
                    # that offer either accounts or terminates. Retained
                    # before the lease goes, as on the exception paths, so
                    # the retry holder names the hash before the flight
                    # stops doing so.
                    coordinator._retain_block_candidate_for_retry(candidate)
                    coordinator._release_block_candidate_disposition(lease)
                    coordinator._wait_for_block_candidate_retry(
                        float(self.retry_initial_seconds)
                    )
                    return BlockCandidateRunResult(True)
                else:
                    try:
                        node_submission = coordinator._node_submission_for_candidate_or_retained(candidate)
                    except BaseException:
                        try:
                            coordinator._retain_block_candidate_for_retry(candidate)
                        finally:
                            coordinator._release_block_candidate_disposition(lease)
                        raise
            else:
                try:
                    node_submission = coordinator._node_submission_for_candidate_or_retained(candidate)
                except BaseException:
                    try:
                        coordinator._retain_block_candidate_for_retry(candidate)
                    finally:
                        coordinator._release_block_candidate_disposition(lease)
                    raise
        else:
            node_submission = _BlockCandidateNodeSubmission(attempted=False)

        if defer_accounting:
            task = _BlockCandidateAccountingTask(
                candidate=candidate,
                node_submission=node_submission,
                disposition_lease=lease,
            )
            try:
                enqueued = coordinator._enqueue_block_accounting_task(task)
            except BaseException:
                try:
                    coordinator._retain_block_candidate_for_retry(candidate)
                finally:
                    coordinator._release_block_candidate_disposition(lease)
                raise
            if enqueued:
                transferred = True
                return BlockCandidateRunResult(True)
            coordinator._retain_block_candidate_for_retry(candidate)
            coordinator._release_block_candidate_disposition(lease)
            coordinator._wait_for_block_candidate_retry(
                float(self.retry_initial_seconds)
            )
            return BlockCandidateRunResult(True)

        try:
            with coordinator._writer_operation("accepted_block_handling"):
                ran = coordinator._call_block_candidate_writer(
                    candidate,
                    node_submission=node_submission,
                    disposition_held=True,
                )
                refresh_client = getattr(outcome, "refresh_client", None)
                outcome.refresh_client = None
        except ShutdownInProgress:
            # The durable outbox remains pending and the replacement process
            # will replay it. Dequeuing the in-memory wakeup during the
            # admission-close race cannot lose candidate work.
            return BlockCandidateRunResult(False)
        finally:
            if not transferred:
                coordinator._release_block_candidate_disposition(lease)
        # Fresh-job fanout is deliberately outside the writer admission. Once
        # the candidate outbox is finalized it cannot mutate the ledger, so a
        # blocked client send must not hold the writer lease during shutdown.
        if refresh_client is not None and not coordinator.stop_event.is_set():
            coordinator._record_block_submitter_phase("refresh-jobs")
            coordinator.refresh_jobs_after_pending_accepted_block(
                refresh_client,
                heartbeat_name="block_submitter",
            )
            coordinator._record_block_submitter_phase("refresh-jobs:complete")
        return BlockCandidateRunResult(ran, refresh_client)

    def attempt(self, candidate: PrismBlockCandidate) -> BlockCandidateAttemptResult:
        """Run one direct landing attempt and structure its outcome."""
        self.outcome.reason = None
        error = "candidate became stale or submission failed"
        try:
            accepted = self.ports.submit_candidate(candidate)
        except Exception:
            accepted = False
            error = "candidate submission raised an exception"
            self.ports.log(
                "prism coordinator: block candidate submission failed "
                f"hash={candidate.submission.block_hash_hex}"
            )
            traceback.print_exc()
        return BlockCandidateAttemptResult(
            accepted=accepted,
            reason=getattr(self.outcome, "reason", None),
            error=error,
        )

    def submit_writer(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
        disposition_held: bool = False,
    ) -> bool:
        """Land one dequeued block candidate inside writer admission."""
        coordinator = self._coordinator
        block_hash = str(candidate.submission.block_hash_hex).lower()
        if not disposition_held:
            # Preserve the historical direct-writer seam while keeping its
            # node offer and terminal outbox decision inside the same-hash
            # guard. Production queue/accounting calls transfer an existing
            # lease and skip this wrapper.
            with coordinator._block_candidate_disposition(block_hash):
                terminal_outcome = coordinator._block_candidate_terminal_outcome(
                    block_hash
                )
                if terminal_outcome is not None:
                    return terminal_outcome
                if node_submission is None:
                    node_submission = (
                        coordinator._node_submission_for_candidate_or_retained(
                            candidate
                        )
                    )
                return coordinator._submit_next_block_candidate_writer(
                    candidate,
                    node_submission=node_submission,
                    disposition_held=True,
                )
        outcome = self.outcome
        outcome.reason = None
        outcome.error = None
        outcome.stale_job_class = None
        coordinator._record_block_submitter_phase("finalize-registry")
        with coordinator.lock:
            pending_finalize = self.finalize_retries.get(block_hash)
        if pending_finalize is not None:
            # Finalize-only replay: node submission, terminal accounting, and
            # payout persistence already completed on the pass that armed
            # this entry. It bypasses both submitblock and attempt marking.
            accepted, error = pending_finalize
            return coordinator._finalize_block_candidate(
                candidate,
                block_hash=block_hash,
                accepted=accepted,
                error=error,
                outcome=outcome,
            )
        if node_submission is None:
            node_submission = coordinator._node_submission_for_candidate_or_retained(candidate)
        try:
            coordinator._mark_block_candidate_attempted(block_hash)
        except Exception:
            print(
                "prism coordinator: could not record block candidate attempt "
                f"hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()
            coordinator._retain_block_candidate_for_retry(candidate)
            coordinator._pace_block_candidate_retry(block_hash)
            return True
        accepted = False
        error = "candidate became stale or submission failed"
        try:
            coordinator._record_block_submitter_phase("accounting")
            with coordinator._block_landing_ledger_statement_timeout_scope(block_hash):
                # ``disposition_held`` states that the serialized inner tail
                # is available; a ``node_submission`` of None still selects
                # the historical bare entrypoint call. Both distinctions are
                # carried to the port rather than decided from here.
                accepted = self.ports.submit_candidate(
                    candidate,
                    node_submission=node_submission,
                    disposition_held=disposition_held,
                )
        except Exception:
            error = "candidate submission raised an exception"
            print(
                "prism coordinator: block candidate submission failed "
                f"hash={candidate.submission.block_hash_hex}",
                flush=True,
            )
            traceback.print_exc()
        abandon_reason = getattr(outcome, "reason", None) if outcome is not None else None
        abandon_error = getattr(outcome, "error", None) if outcome is not None else None
        if not accepted and abandon_error:
            error = str(abandon_error)
        retryable = not accepted and (
            abandon_reason is None
            or abandon_reason in self.retryable_reasons
        )
        if retryable:
            # Leave the outbox row pending. It will replay after a short pause
            # or on process restart. Keep this parent ahead of queued children:
            # a child built from its prospective balances cannot be validated
            # against the database until the parent confirmation catches up.
            print(
                "prism coordinator: retained block candidate for retry "
                f"hash={block_hash} reason={abandon_reason or 'exception'}",
                flush=True,
            )
            coordinator._retain_block_candidate_for_retry(candidate)
            coordinator._pace_block_candidate_retry(block_hash)
            return True
        if not accepted:
            try:
                coordinator._record_block_submitter_phase("reject-prepared-block")
                with coordinator._block_landing_ledger_statement_timeout_scope(block_hash):
                    coordinator._reject_terminal_prepared_block_candidate(candidate)
                coordinator._record_block_submitter_phase(
                    "reject-prepared-block:complete"
                )
            except Exception:
                # Persistence may have committed before a later RPC/transport
                # failure. Do not terminally discard the outbox row until its
                # prepared balance deltas have also reached a terminal state.
                print(
                    "prism coordinator: prepared block cleanup failed "
                    f"hash={block_hash}",
                    flush=True,
                )
                traceback.print_exc()
                coordinator._defer_block_candidate(
                    "backend-rpc-unavailable",
                    "could not reject prepared state for terminal candidate",
                    worker=candidate.client.username or None,
                )
                coordinator._retain_block_candidate_for_retry(candidate)
                coordinator._wait_for_block_candidate_retry(
                    coordinator._next_block_candidate_retry_delay(block_hash)
                )
                return True
        return coordinator._finalize_block_candidate(
            candidate,
            block_hash=block_hash,
            accepted=accepted,
            error=error,
            outcome=outcome,
        )

    def finalize(
        self,
        candidate: PrismBlockCandidate,
        *,
        block_hash: str,
        accepted: bool,
        error: str,
        outcome: threading.local,
    ) -> bool:
        """Drive a terminal candidate's durable outbox update, with backoff.

        Failure retains the candidate as a finalize-only replay: the next
        paced attempt re-enters here directly, never submit_block_candidate,
        so terminal abandonment accounting stays once-per-candidate and an
        accepted candidate's audit/persist work is not redone per retry.
        """
        coordinator = self._coordinator
        coordinator._record_block_submitter_phase("finalize-preview")
        coordinator._clear_accepted_block_payout_preview(
            block_hash,
            invalidate_published=not accepted,
        )
        finish_name = (
            "mark_block_candidate_submitted"
            if accepted
            else "mark_block_candidate_abandoned"
        )
        finish = getattr(coordinator.ledger, finish_name, None)
        if callable(finish):
            try:
                if accepted:
                    coordinator._run_block_submitter_ledger_call(
                        ("finalize", block_hash, "submitted"),
                        "finalize-outbox-submitted",
                        lambda: finish(block_hash=block_hash),
                    )
                else:
                    coordinator._run_block_submitter_ledger_call(
                        ("finalize", block_hash, "abandoned"),
                        "finalize-outbox-abandoned",
                        lambda: finish(block_hash=block_hash, error=error),
                    )
                    coordinator._record_committed_block_candidate_abandonment(
                        block_hash,
                        outcome,
                    )
                    # The invalidation tombstone is needed until the durable
                    # outbox becomes terminal. A normal return (including an
                    # already-terminal/missing row) means there is no pending
                    # replay source left for this process to guard.
                    coordinator._clear_accepted_block_payout_preview(block_hash)
            except Exception:
                # Keep the coordinator alive. The terminal-state update
                # failed, so the durable row stays pending and its replay
                # must pace like any other retained retry.
                print(
                    "prism coordinator: could not finalize durable block candidate "
                    f"hash={block_hash}",
                    flush=True,
                )
                traceback.print_exc()
                with coordinator.lock:
                    registry = self.finalize_retries
                    first_failure = block_hash not in registry
                    registry[block_hash] = (accepted, error)
                if not accepted:
                    # The durable update is ambiguous, but this process has
                    # now frozen a false finalize-only disposition. It cannot
                    # return to chain-state evaluation until restart, where
                    # these process-local counters start fresh.
                    coordinator._record_committed_block_candidate_abandonment(
                        block_hash,
                        outcome,
                    )
                # The share row already reached its terminal outcome in this
                # process; only the outbox mark is pending. Release the
                # snapshot anchor floor now (idempotent) -- holding it across
                # paced retries would clamp job snapshot anchors and
                # under-count already-durable shares in reward windows.
                coordinator._finish_pending_share_commit(candidate.pending_share)
                coordinator._retain_block_candidate_for_retry(candidate)
                if accepted and first_failure:
                    # The block is active regardless of the outbox update;
                    # post-accept fleet refresh must wait for neither ledger
                    # recovery nor the first backoff. Return unpaced so the
                    # caller refreshes immediately; the paced ladder starts
                    # from the first finalize-only replay.
                    outcome.refresh_client = candidate.client
                    return True
                coordinator._wait_for_block_candidate_retry(
                    coordinator._next_block_candidate_retry_delay(block_hash)
                )
                return True
        elif not accepted:
            # Compatibility ledgers without a durable candidate outbox have
            # no restart replay source that could require the tombstone.
            coordinator._clear_accepted_block_payout_preview(block_hash)
            coordinator._record_committed_block_candidate_abandonment(
                block_hash,
                outcome,
            )
        with coordinator.lock:
            self.finalize_retries.pop(block_hash, None)
        coordinator._clear_block_candidate_retry_state(block_hash)
        coordinator._discard_outstanding_block_candidate(block_hash)
        coordinator._record_block_candidate_terminal_outcome(
            block_hash,
            accepted=accepted,
        )
        # Terminal for this process either way: an accepted candidate credited
        # its share during the success tail (a no-op release here), and an
        # abandoned one can only be credited by restart replay, which stamps a
        # fresh PendingShare. Stop holding the snapshot anchor floor.
        coordinator._finish_pending_share_commit(candidate.pending_share)
        if accepted:
            outcome.refresh_client = candidate.client
        return True

    # -- retained node acceptance ------------------------------------------

    def _stash_retained_block_candidate_node_submission(
        self,
        block_hash: str,
        node_submission: _BlockCandidateNodeSubmission | None,
    ) -> None:
        """Record a definitive node acceptance for in-process retries.

        Recorded at the offer itself (the universal post-offer hook) so
        every retention path — writer failures, defer-accounting handoff
        failures, the accounting loop — is covered without each site having
        to remember. A retryable failure after a successful offer would
        otherwise re-offer on retry and read "duplicate", which classifies
        against the moved live tip and can only rescue the block through
        chain probes that may be unavailable under the same saturation.
        The entry is read without consuming and lives until the candidate
        reaches a terminal outcome, so repeated retryable failures keep
        reusing the same known acceptance.
        """
        if not _is_definitive_node_acceptance(node_submission):
            # Only a definitive success is safe to reuse: an ambiguous or
            # rejected offer must be re-offered so the node can resolve it.
            # The predicate is shared with the accounting handoff's lane
            # routing so the two readings of "the node has this block" cannot
            # drift apart.
            return
        with self._coordinator.lock:
            retained = getattr(
                self,
                "_block_candidate_retained_node_submissions",
                None,
            )
            if retained is None:
                retained = {}
                self._block_candidate_retained_node_submissions = retained
            retained[str(block_hash).lower()] = node_submission
            stamped = getattr(
                self,
                "_block_candidate_retained_submission_monotonic",
                None,
            )
            if stamped is None:
                stamped = {}
                self._block_candidate_retained_submission_monotonic = stamped
            stamped[str(block_hash).lower()] = time.monotonic()

    def _retained_block_candidate_node_submission(
        self,
        block_hash: str,
    ) -> _BlockCandidateNodeSubmission | None:
        with self._coordinator.lock:
            retained = getattr(
                self,
                "_block_candidate_retained_node_submissions",
                None,
            )
            if not retained:
                return None
            return retained.get(str(block_hash).lower())

    def _block_candidate_acceptance_retained(self, block_hash: str) -> bool:
        """Whether this process holds fresh first-party acceptance evidence.

        The retained stash records only definitive submitblock successes,
        so its presence proves qbitd accepted this candidate — evidence of
        the same strength as a recent own-hash tip observation, and
        available precisely when saturation makes the chain probes answer
        "unknown" (the observation registry can be empty after a definitive
        ack: blockwait only reports the newest of rapid connects). It ages
        on the same window as tip observations: acceptance at offer time
        does not prove the block stayed canonical, and an orphaned block
        never probes False — it is merely absent — so a candidate whose
        probes stay inconclusive past the window must regain
        abandonability instead of deferring forever behind a stale ack.
        """
        with self._coordinator.lock:
            stamped = getattr(
                self,
                "_block_candidate_retained_submission_monotonic",
                None,
            )
            if not stamped:
                return False
            recorded = stamped.get(str(block_hash).lower())
        if recorded is None:
            return False
        window = float(
            getattr(
                self._coordinator,
                "observed_tip_accept_window_seconds",
                DEFAULT_PRISM_OBSERVED_TIP_ACCEPT_WINDOW_SECONDS,
            )
        )
        if window <= 0:
            return True
        return (time.monotonic() - recorded) <= window

    # -- outcome recording -------------------------------------------------

    def record_deferred(
        self,
        reason: str,
        message: str,
        *,
        worker: str | None,
    ) -> None:
        """Record a retryable outcome without counting a terminal abandonment."""
        del worker
        outcome = self.outcome
        outcome.reason = reason
        outcome.error = None
        outcome.stale_job_class = None
        print(
            f"prism coordinator: block candidate deferred reason={reason}: {message}",
            flush=True,
        )

    # -- acceptance evidence -----------------------------------------------

    def _block_candidate_acceptance_recorded(self, block_hash: str) -> bool:
        """Return whether this process completed the candidate success tail."""
        self._coordinator._ensure_job_cache_state()
        with self._coordinator.lock:
            return (
                block_hash.lower()
                in self._coordinator._accounted_accepted_block_hashes
            )

    def _register_outstanding_block_candidate(self, block_hash: str) -> None:
        """Track a candidate this process may still land, for tip matching."""
        self._coordinator._ensure_job_cache_state()
        with self._coordinator.lock:
            self._outstanding_block_candidate_hashes.add(block_hash.lower())

    def _discard_outstanding_block_candidate(self, block_hash: str) -> None:
        """Stop matching tip observations once a candidate is terminal."""
        self._coordinator._ensure_job_cache_state()
        key = block_hash.lower()
        with self._coordinator.lock:
            self._outstanding_block_candidate_hashes.discard(key)
            self._tip_observed_accepted_block_hashes.pop(key, None)

    def _note_tip_observation_for_candidates(self, tip_hash: str) -> None:
        """Register a tip observation that matches an outstanding candidate.

        qbitd only ever reports the pool's own candidate hash as its chain
        tip after accepting that block, so the observation itself is
        acceptance evidence -- even when the direct submitblock ack was lost
        in transport and the accepted success tail has not run (blockwait
        typically learns of the tip before, or instead of, the ack). Every
        tip-observation channel funnels through here so later disposition
        and abandon checks can outlive transient fork views.
        """
        key = tip_hash.lower()
        self._coordinator._ensure_job_cache_state()
        newly_observed = False
        with self._coordinator.lock:
            if key in self._outstanding_block_candidate_hashes:
                newly_observed = (
                    key not in self._tip_observed_accepted_block_hashes
                )
                self._tip_observed_accepted_block_hashes[key] = time.monotonic()
        if newly_observed:
            print(
                "prism coordinator: chain tip observation matches pool block "
                f"candidate hash={key}; acceptance registered pending "
                "finalization",
                flush=True,
            )

    def _block_candidate_acceptance_observed(self, block_hash: str) -> bool:
        """Whether a recent tip observation already proved this candidate landed."""
        self._coordinator._ensure_job_cache_state()
        with self._coordinator.lock:
            observed = self._tip_observed_accepted_block_hashes.get(
                block_hash.lower()
            )
        if observed is None:
            return False
        window = float(
            getattr(
                self._coordinator,
                "observed_tip_accept_window_seconds",
                DEFAULT_PRISM_OBSERVED_TIP_ACCEPT_WINDOW_SECONDS,
            )
        )
        if window <= 0:
            return True
        return (time.monotonic() - observed) <= window

    def _block_candidate_chain_probe(
        self,
        block_hash: str,
        *,
        expected_height: int | None = None,
    ) -> bool | None:
        """Fresh chain verdict for a candidate: proven active, proven wrong, or unknown.

        Returns True when the candidate's own hash is the fresh best tip or
        an active chain header at its expected height, False when it is
        provably active at the wrong height (a corrupt intent, never a tip
        race), and None when this instantaneous view proves nothing (the
        hash absent during a tip race, or the probe itself failing).
        """
        key = block_hash.lower()
        # The two probes are independent: a best-tip lookup failure must not
        # suppress the active-header check, which subsumes it (the tip block
        # itself reports one confirmation) and can prove acceptance alone.
        try:
            if str(self._coordinator.rpc.call("getbestblockhash")).lower() == key:
                self._coordinator._note_tip_observation_for_candidates(key)
                return True
        except Exception:
            print(
                "prism coordinator: acceptance re-check best-tip probe "
                f"failed hash={key}; trying the active-header probe",
                flush=True,
            )
            traceback.print_exc()
        height: int | None = None
        try:
            height = self._coordinator.active_block_candidate_height(key)
        except Exception:
            print(
                "prism coordinator: acceptance re-check header probe failed "
                f"hash={key}; falling back to tip-observation evidence",
                flush=True,
            )
            traceback.print_exc()
        if height is None:
            return None
        if expected_height is None or int(height) == int(expected_height):
            self._coordinator._note_tip_observation_for_candidates(key)
            return True
        return False

    def _block_candidate_acceptance_pending(
        self,
        block_hash: str,
        *,
        expected_height: int | None = None,
    ) -> bool:
        """Return whether abandoning this candidate would discard an accepted block.

        A fresh probe wins in both directions: a candidate proven active is
        accepted even if its tip observation was missed, and a candidate
        proven active at the wrong height stays abandonable. When the probe
        cannot prove either way, a recent own-hash tip observation keeps the
        candidate deferring instead of terminal: qbitd accepted it once, so
        only a durably settled chain view may discard it.
        """
        probe = self._coordinator._block_candidate_chain_probe(
            block_hash,
            expected_height=expected_height,
        )
        if probe is not None:
            return probe
        return self._coordinator._block_candidate_acceptance_observed(block_hash)

    def _count_accept_pending_defer(self) -> None:
        with self._coordinator.lock:
            self.block_candidate_accept_pending_defer_count = int(
                getattr(self, "block_candidate_accept_pending_defer_count", 0)
            ) + 1

    def record_abandoned(
        self,
        reason: str,
        message: str,
        *,
        block_hash: str,
        worker: str | None,
        preserve_if_accepted: bool = False,
        expected_height: int | None = None,
        stale_job_class: str | None = None,
    ) -> bool:
        """Record a lost/failed block candidate as a BLOCK-path event.

        The share that produced the candidate was acknowledged and, when it met
        the share target, credited at submit time; the block losing its race
        afterwards does not un-earn it and is NOT a share rejection. It is
        counted under a dedicated block-abandonment counter (by reason, so a
        benign 'tip moved' race is distinguishable from a real
        submitblock-rejected/ledger failure) rather than the share-reject
        counters, which stay a true measure of shares refused to miners.

        Every terminal abandonment withdraws its payout-preview transition
        before the outcome becomes final. ``preserve_if_accepted`` closes the
        moved-tip race: if another attempt completed this hash's accepted
        success tail while withdrawal was in flight, the accepted disposition
        wins and the caller must finalize the outbox as submitted. Returns
        whether that accepted disposition won.

        Independent of that completed-tail record, a candidate whose own
        block hash is the fresh best tip, an active chain header at its
        expected height, a recent own-hash tip observation, or a fresh
        retained definitive submitblock success is an ACCEPTED block whose
        finalization is still pending (for example after a lost submitblock
        ack, or when saturation makes both probes answer "unknown" at this
        instant). Terminal abandonment would discard its payout
        accounting and withdraw its landed preview -- fencing payout
        publication for work qbitd already accepted -- so such candidates
        defer for retry instead; only hashes provably absent from the active
        chain (past the observation and retained-acceptance windows)
        abandon terminally. The terminal
        seal re-reads observation evidence atomically, so callers can order
        follow-up durable work (rejecting prepared payout rows) strictly
        afterward. Abandonment metrics commit only once that cleanup succeeds
        or a false finalize-only disposition is frozen.
        """
        coordinator = self._coordinator
        if reason in self.retryable_reasons:
            coordinator._defer_block_candidate(reason, message, worker=worker)
            return False
        if (
            stale_job_class is not None
            and stale_job_class not in PRISM_STALE_JOB_ABANDON_CLASSES
        ):
            raise ValueError(
                f"unknown stale job abandon class: {stale_job_class}"
            )
        outcome = self.outcome
        if (
            preserve_if_accepted
            and coordinator._block_candidate_acceptance_recorded(block_hash)
        ):
            # Durable accepted state already equals the prospective view, so
            # any transition recreated by the losing attempt is a no-op
            # override, not a withdrawal.
            coordinator._clear_accepted_block_payout_preview(block_hash)
            outcome.reason = None
            outcome.error = None
            return True
        chain_probe = coordinator._block_candidate_chain_probe(
            block_hash,
            expected_height=expected_height,
        )
        if chain_probe is True or (
            chain_probe is None
            and (
                coordinator._block_candidate_acceptance_observed(block_hash)
                or coordinator._block_candidate_acceptance_retained(block_hash)
            )
        ):
            self._count_accept_pending_defer()
            coordinator._defer_block_candidate(
                PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
                "candidate is on (or was recently observed or definitively "
                "accepted as) the active chain; refusing terminal "
                f"abandonment (was {reason}: {message})",
                worker=worker,
            )
            return False

        # Own the cleanup invariant here rather than relying on every caller to
        # remember it. Invalidation can block behind another candidate pass;
        # recheck the accepted record afterward before committing abandonment.
        # Capture the transition first: if late acceptance evidence forces a
        # defer below, its published preview must be restorable.
        with coordinator._accepted_block_payout_preview_condition:
            withdrawn_transition = coordinator._accepted_block_payout_previews.get(
                block_hash.lower()
            )
        coordinator._clear_accepted_block_payout_preview(
            block_hash,
            invalidate_published=True,
        )
        # The invalidation above can block long enough for the chain view to
        # heal (a buried accepted block is not always re-observed as the tip
        # while blockwait only reports the newest of rapid connects), so an
        # unknown pre-withdrawal verdict must re-probe before the terminal
        # commit. A provably wrong-height verdict is immutable (headers
        # cannot change height) and is never re-probed.
        late_probe = (
            chain_probe
            if chain_probe is False
            else coordinator._block_candidate_chain_probe(
                block_hash,
                expected_height=expected_height,
            )
        )
        with coordinator.lock:
            accepted_race_won = bool(
                preserve_if_accepted
                and block_hash.lower()
                in coordinator._accounted_accepted_block_hashes
            )
            # A blockwait observation can also register during the blocking
            # invalidation. The disposition seal must consult that evidence
            # atomically or the same blind spot reopens inside this window;
            # the probe still wins both directions. Metrics are committed
            # later, after prepared-state cleanup can no longer reverse this
            # decision.
            late_acceptance_observed = bool(
                not accepted_race_won
                and (
                    late_probe is True
                    or (
                        late_probe is not False
                        and (
                            coordinator._block_candidate_acceptance_observed(
                                block_hash
                            )
                            or coordinator._block_candidate_acceptance_retained(
                                block_hash
                            )
                        )
                    )
                )
            )
            if not accepted_race_won and not late_acceptance_observed:
                outcome.reason = reason
                outcome.error = message
                outcome.stale_job_class = stale_job_class
                # Seal the disposition in the same critical section that
                # commits it: stop matching tip observations for this hash so
                # no acceptance evidence can register between this terminal
                # decision and the caller's follow-up durable work (rejecting
                # prepared payout rows). Observation registration takes this
                # same lock, so exclusion across the gap is total. A crash
                # before the durable outbox update replays the candidate,
                # which re-registers and re-evaluates from live chain state.
                self._outstanding_block_candidate_hashes.discard(
                    block_hash.lower()
                )
                self._tip_observed_accepted_block_hashes.pop(
                    block_hash.lower(),
                    None,
                )
        if accepted_race_won:
            coordinator._clear_accepted_block_payout_preview(block_hash)
            outcome.reason = None
            outcome.error = None
            return True
        if late_acceptance_observed:
            # Restore the landed barrier the withdrawal just removed (and pop
            # its fail-closed tombstone): the candidate is still an accepted
            # block pending finalization, so descendant builders must keep
            # waiting on its preview -- not fail closed -- until the deferred
            # retry's accepted tail republishes it. Without this, retries
            # that keep deferring on observation evidence alone would leave
            # the tombstone fencing template refreshes, recreating the
            # coordination-blocked stall this path exists to prevent.
            coordinator._begin_accepted_block_payout_preview(
                block_hash,
                block_height=expected_height,
            )
            if expected_height is not None:
                coordinator._mark_accepted_block_payout_landed(
                    block_hash,
                    block_height=expected_height,
                )
            if (
                withdrawn_transition is not None
                and withdrawn_transition.preview is not None
            ):
                # The withdrawal superseded payout publication and, on a
                # lost publication race, left delivery fenced. Republish the
                # withdrawn preview now so admission reopens with the defer
                # rather than staying coordination-blocked across the
                # deferral cycles until the accepted tail republishes it.
                try:
                    coordinator._publish_accepted_block_payout_preview(
                        block_hash,
                        coordinator._materialize_prior_balance_preview(
                            withdrawn_transition.preview
                        ),
                    )
                except Exception:
                    print(
                        "prism coordinator: could not republish withdrawn "
                        f"payout preview hash={block_hash}; the scheduled "
                        "refresh will retry publication",
                        flush=True,
                    )
                    traceback.print_exc()
            self._count_accept_pending_defer()
            coordinator._defer_block_candidate(
                PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
                "acceptance evidence arrived during payout-preview "
                f"withdrawal; refusing terminal abandonment (was {reason}: "
                f"{message})",
                worker=worker,
            )
            outcome.error = None
            return False

        print(
            f"prism coordinator: block candidate abandoned reason={reason}: {message}",
            flush=True,
        )
        return False

    # -- block-work liveness -----------------------------------------------

    def _block_work_heartbeat_owner(self) -> tuple[str, str] | None:
        """Return the independent heartbeat/phase slots owned by this thread."""
        current = threading.get_ident()
        if current == getattr(self, "_block_submitter_thread_ident", None):
            return "block_submitter", "_block_submitter_phase"
        if current == getattr(self, "_block_accounting_thread_ident", None):
            return "block_accounting", "_block_accounting_phase"
        return None

    def _record_block_work_heartbeat(self, name: str, phase: str) -> None:
        """Record a phase while preserving one-argument heartbeat embedders."""
        heartbeat = self._coordinator._record_heartbeat
        try:
            heartbeat(name, phase=phase)
        except TypeError as exc:
            # Preserve the historical one-argument heartbeat seam used by
            # focused embedders. Do not hide TypeErrors raised by a heartbeat
            # implementation that did accept the keyword.
            if "unexpected keyword argument 'phase'" not in str(exc):
                raise
            heartbeat(name)

    def _record_block_submitter_phase(self, phase: str) -> None:
        """Stamp a named phase only from a dedicated block-work owner."""
        owner = self._block_work_heartbeat_owner()
        if owner is None:
            return
        heartbeat_name, phase_attribute = owner
        setattr(self, phase_attribute, phase)
        self._record_block_work_heartbeat(heartbeat_name, phase)

    def _record_block_submitter_wait(self, phase: str) -> None:
        """Heartbeat owner waits while preserving lightweight test behavior."""
        owner = self._block_work_heartbeat_owner()
        if owner is None and not hasattr(self, "_block_submitter_thread_ident"):
            self._coordinator._record_heartbeat("block_submitter")
            return
        self._coordinator._record_block_submitter_phase(phase)

    def _block_work_wait_slice(self) -> float:
        """Choose a polling slice that stays inside the configured watchdog."""
        watchdog_budget = max(
            0.001,
            float(getattr(self._coordinator, "watchdog_timeout_seconds", 120.0)),
        )
        return min(
            BLOCK_SUBMITTER_WAIT_HEARTBEAT_SLICE_SECONDS,
            max(0.001, watchdog_budget * 0.9),
        )

    def _observe_coordinator_lock_wait(self, elapsed_seconds: float) -> None:
        """Keep a sliced coordinator-lock wait visible and watchdog-safe."""
        owner = self._block_work_heartbeat_owner()
        if owner is None:
            return
        heartbeat_name, phase_attribute = owner
        current_phase = getattr(self, phase_attribute, "unknown")
        wait_phase = f"wait-lock:coordinator-state:{current_phase}"
        self._record_block_work_heartbeat(heartbeat_name, wait_phase)
        now = time.monotonic()
        log_interval = float(
            getattr(
                self._coordinator,
                "block_submit_lock_wait_log_seconds",
                DEFAULT_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS,
            )
        )
        last_log = float(
            getattr(self, "_block_submitter_last_lock_wait_log_monotonic", 0.0)
        )
        if last_log <= 0 or now - last_log >= log_interval:
            self._block_submitter_last_lock_wait_log_monotonic = now
            print(
                "prism coordinator: block submitter waiting on lock "
                f"lock=coordinator-state phase={current_phase} "
                f"elapsed={elapsed_seconds:.3f}s",
                flush=True,
            )

    def _acquire_block_submitter_lock(self, lock: Any, name: str) -> None:
        """Acquire a submit-path lock in heartbeat/logging slices."""
        owner = self._block_work_heartbeat_owner()
        if owner is None and (
            hasattr(self, "_block_submitter_thread_ident")
            or hasattr(self, "_block_accounting_thread_ident")
        ):
            lock.acquire()
            return
        started = time.monotonic()
        last_log = started
        log_interval = float(
            getattr(
                self._coordinator,
                "block_submit_lock_wait_log_seconds",
                DEFAULT_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS,
            )
        )
        while not lock.acquire(timeout=self._block_work_wait_slice()):
            phase = f"wait-lock:{name}"
            self._coordinator._record_block_submitter_wait(phase)
            now = time.monotonic()
            if now - last_log >= log_interval:
                print(
                    "prism coordinator: block submitter waiting on lock "
                    f"lock={name} elapsed={now - started:.3f}s",
                    flush=True,
                )
                last_log = now

    @contextmanager
    def _block_submitter_lock(self, lock: Any, name: str) -> Iterator[None]:
        self._coordinator._acquire_block_submitter_lock(lock, name)
        try:
            yield
        finally:
            lock.release()

    def _record_block_candidate_progress(
        self,
        phase: str = "accounting-progress",
    ) -> None:
        """Stamp the submitter heartbeat at a candidate-disposition boundary.

        Stamps come only from the dedicated submitter thread: dispositions
        also run on client connection threads (synchronous below-target
        solves), and a stamp from those threads would refresh the
        ``block_submitter`` budget while the dedicated thread might be
        wedged elsewhere. A disposition crosses several ledger and
        filesystem writes that slow down together under database pressure;
        stamping each completed phase on the owner thread keeps a
        progressing disposition inside the liveness budget while a wedged
        phase still leaves the watchdog able to recover the process -- the
        same shape as the CTV broadcaster's per-row stamping, whose name
        likewise maps to a single thread.
        """
        self._coordinator._record_block_submitter_phase(phase)

    def _observe_block_submit_seconds(self, elapsed_seconds: float) -> None:
        self._coordinator._ensure_job_cache_state()
        with self._block_submit_metrics_lock:
            histogram = self.block_submit_seconds_histogram
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + max(
                0.0, elapsed_seconds
            )
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in tuple(buckets):
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _note_accepted_block_preview_acceptance(self, block_hash: str) -> None:
        """Stamp the moment definitive node acceptance became known.

        Deliberately not ``AcceptedBlockPayoutTransition.landed_monotonic``:
        that stamp is armed *before* the RPC (see
        ``_unmark_accepted_block_payout_landed``, "the next attempt re-arms
        landed before its own RPC"), so an interval measured from it would
        include the offer itself and would restart on every retry. This one
        is taken on the submitter thread at the instant the offer resolved
        definitively, which is the earliest moment acceptance is knowable
        without a further chain probe.
        """
        key = str(block_hash).lower()
        stamped = time.monotonic()
        with self._accepted_block_preview_publication_lock:
            stamps = self._accepted_block_preview_acceptance_monotonic
            if key in stamps:
                # Either an unpublished stamp from an earlier definitive
                # offer of this hash, or its observed tombstone. Both mean
                # the interval is already owned; a re-offer must not restart
                # a measurement that is running or already recorded.
                return
            stamps[key] = stamped
            overflow = len(stamps) - MAX_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_STAMPS
            if overflow > 0:
                for stale in list(stamps)[:overflow]:
                    del stamps[stale]

    def _observe_accepted_block_preview_publication(
        self,
        block_hash: str,
        *,
        result: str,
    ) -> None:
        """Close the acceptance-to-publication interval for one hash.

        Only the first publication of a hash is measured: it is the one that
        made the preview visible to children already waiting on the
        transition, which is the latency the 5 s child wait budget is spent
        against. Later matching republications observe nothing.
        """
        key = str(block_hash).lower()
        published = time.monotonic()
        with self._accepted_block_preview_publication_lock:
            stamps = self._accepted_block_preview_acceptance_monotonic
            if key not in stamps:
                # No definitive offer of this hash was stamped in this
                # process: a replayed candidate confirmed by chain probe, or
                # a preview republished for an acceptance this process never
                # made. There is no interval to close, and inventing one from
                # the publication alone would report a zero.
                return
            started = stamps[key]
            if started is None:
                return
            histogram = self.accepted_block_preview_publication_seconds_histogram.get(
                result
            )
            if histogram is None:
                # The label set is closed by
                # PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS. Drop the
                # observation rather than growing series cardinality, and
                # leave the stamp for a labelled publication to close.
                return
            stamps[key] = None
            elapsed = max(0.0, published - float(started))
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in tuple(buckets):
                if elapsed <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1


class BlockCandidateCompatibilityField:
    """Route temporary coordinator fields to the B1 service owner."""

    def __init__(self, name: str, default: Any) -> None:
        self.name = name
        self.default = default

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return self
        service = instance.__dict__.get("_block_candidate_service")
        if service is None:
            value = instance.__dict__.get(self.name, self.default)
            if callable(value) and getattr(value, "__candidate_default_factory__", False):
                value = value()
                instance.__dict__[self.name] = value
            return value
        return _compat_get(service, self.name)

    def __set__(self, instance: Any, value: Any) -> None:
        service = instance.__dict__.get("_block_candidate_service")
        if service is None:
            instance.__dict__[self.name] = value
            return
        _compat_set(service, self.name, value)


def compatibility_default(factory: Callable[[], Any]) -> Callable[[], Any]:
    setattr(factory, "__candidate_default_factory__", True)
    return factory


class BlockCandidateStateField:
    """Route one #113-era B1 coordinator field to the service owner.

    The descriptor keeps the historical attribute name readable and writable
    on the coordinator while :class:`BlockCandidateService` owns the single
    mutable copy, mirroring the S1/S2/S3/J1/P1/G1 extraction pattern. Fields
    the service creates lazily preserve the pre-extraction ``hasattr``/
    ``getattr``-with-default semantics exactly: reading an unset field raises
    AttributeError until first write.
    """

    def __init__(self, name: str, attribute: str | None = None) -> None:
        self.name = name
        self.attribute = attribute or name

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        if instance is None:
            return self
        return getattr(
            instance._ensure_block_candidate_service(),
            self.attribute,
        )

    def __set__(self, instance: Any, value: Any) -> None:
        setattr(
            instance._ensure_block_candidate_service(),
            self.attribute,
            value,
        )


_COMPATIBILITY_FIELD_MAP = {
    "block_candidate_queue": "candidate_queue",
    "block_candidates_dropped": "dropped",
    "block_candidate_wakeups_coalesced": "wakeups_coalesced",
    "block_candidate_retry_count": "retries",
    "block_candidate_poisoned_count": "poisoned",
    "block_candidate_retry_initial_seconds": "retry_initial_seconds",
    "block_candidate_retry_max_seconds": "retry_max_seconds",
    "block_candidate_retry_delays": "retry_delays",
    "_block_candidate_finalize_retries": "finalize_retries",
    "block_candidate_abandoned_counts": "abandoned_counts",
    "_retry_block_candidate": "retry_candidate",
    "_block_candidate_outcome": "outcome",
}

# The #113-era coordinator fields routed one-to-one onto the service. The
# handful whose service spelling differs (the pre-#113 backoff quartet) map
# through their historical service names.
_STATE_FIELD_MAP = {
    "_block_replay_candidate_queue": "_block_replay_candidate_queue",
    "_block_replay_inflight_hashes": "_block_replay_inflight_hashes",
    "_block_quarantine_queue": "_block_quarantine_queue",
    "_block_quarantine_hashes": "_block_quarantine_hashes",
    "_block_candidate_disposition_registry_lock": "_block_candidate_disposition_registry_lock",
    "_block_candidate_disposition_flights": "_block_candidate_disposition_flights",
    "_block_candidate_terminal_outcomes": "_block_candidate_terminal_outcomes",
    "_block_fast_lane_reservations": "_block_fast_lane_reservations",
    "_block_disposition_waiting_retries": "_block_disposition_waiting_retries",
    "_block_candidate_dequeued_hashes": "_block_candidate_dequeued_hashes",
    "_block_accounting_state_lock": "_block_accounting_state_lock",
    "_block_accounting_queue": "_block_accounting_queue",
    "_block_accounting_overflow_queue": "_block_accounting_overflow_queue",
    "_block_accounting_accepted_queue": "_block_accounting_accepted_queue",
    "_block_accounting_sequence": "_block_accounting_sequence",
    "_block_accounting_thread": "_block_accounting_thread",
    "_block_accounting_thread_ident": "_block_accounting_thread_ident",
    "_block_accounting_holds_disposition": "_block_accounting_holds_disposition",
    "_block_accounting_deferred_retry_candidate": "_block_accounting_deferred_retry_candidate",
    "_block_accounting_phase": "_block_accounting_phase",
    "_block_submitter_thread_ident": "_block_submitter_thread_ident",
    "_block_submitter_phase": "_block_submitter_phase",
    "_block_submitter_last_lock_wait_log_monotonic": "_block_submitter_last_lock_wait_log_monotonic",
    "_block_submitter_retry_state_lock": "_state_lock",
    "_block_submitter_backoff_started_monotonic": "_backoff_started_monotonic",
    "_block_submitter_backoff_deadline_monotonic": "_backoff_deadline_monotonic",
    "_block_submitter_backoff_delay_seconds": "_backoff_delay_seconds",
    "_block_submitter_ledger_calls_lock": "_block_submitter_ledger_calls_lock",
    "_block_submitter_ledger_calls": "_block_submitter_ledger_calls",
    "_block_submitter_ledger_worker_slots": "_block_submitter_ledger_worker_slots",
    "_block_submitter_rpc_calls_lock": "_block_submitter_rpc_calls_lock",
    "_block_submitter_rpc_calls": "_block_submitter_rpc_calls",
    "_block_submitter_rpc_worker_slots": "_block_submitter_rpc_worker_slots",
    "_block_landing_timeout_counts": "_block_landing_timeout_counts",
    "_block_ledger_call_metrics_lock": "_block_ledger_call_metrics_lock",
    "_block_ledger_call_metrics": "_block_ledger_call_metrics",
    "_block_candidate_retry_not_before": "_block_candidate_retry_not_before",
    "_block_candidate_retained_node_submissions": "_block_candidate_retained_node_submissions",
    "_block_candidate_retained_submission_monotonic": "_block_candidate_retained_submission_monotonic",
    "_counted_block_candidate_abandonments": "_counted_block_candidate_abandonments",
    "_outstanding_block_candidate_hashes": "_outstanding_block_candidate_hashes",
    "_tip_observed_accepted_block_hashes": "_tip_observed_accepted_block_hashes",
    "block_candidate_accept_pending_defer_count": "block_candidate_accept_pending_defer_count",
    "accepted_parent_redrive_attempt_count": "accepted_parent_redrive_attempt_count",
    "accepted_parent_redrive_resolved_count": "accepted_parent_redrive_resolved_count",
    "accepted_parent_redrive_exhausted_count": "accepted_parent_redrive_exhausted_count",
    "stale_job_abandon_counts": "stale_job_abandon_counts",
    "_block_submit_metrics_lock": "_block_submit_metrics_lock",
    "block_submit_seconds_histogram": "block_submit_seconds_histogram",
    "_accepted_block_preview_publication_lock": (
        "_accepted_block_preview_publication_lock"
    ),
    "accepted_block_preview_publication_seconds_histogram": (
        "accepted_block_preview_publication_seconds_histogram"
    ),
    "_accepted_block_preview_acceptance_monotonic": (
        "_accepted_block_preview_acceptance_monotonic"
    ),
    "_block_replay_enumeration_owed_flag": "_block_replay_enumeration_owed_flag",
}


def _compat_get(service: BlockCandidateService, name: str) -> Any:
    return getattr(service, _COMPATIBILITY_FIELD_MAP[name])


def _compat_set(service: BlockCandidateService, name: str, value: Any) -> None:
    setattr(service, _COMPATIBILITY_FIELD_MAP[name], value)

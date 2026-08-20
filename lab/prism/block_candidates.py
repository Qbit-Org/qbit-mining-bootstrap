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
import json
import queue
import threading
import time
import traceback
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field, replace as dataclass_replace
from types import SimpleNamespace
from typing import Any, Callable, Iterator, Protocol

from lab.prism import direct_stratum
from lab.prism.coordinator_config import (
    BLOCK_LANDING_DB_TIMEOUT_WATCHDOG_FRACTION,
    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_MAX_SECONDS,
    DEFAULT_BLOCK_LANDING_DB_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_DB_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS,
    DEFAULT_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS,
    DEFAULT_BLOCK_SUBMIT_STUCK_CALL_EXIT_SECONDS,
    DEFAULT_PRISM_OBSERVED_TIP_ACCEPT_WINDOW_SECONDS,
    DEFAULT_PRISM_WATCHDOG_TIMEOUT_SECONDS,
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
DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS = 0.25
DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS = 30.0
# The primary accounting handoff queue must be bounded or the documented
# result-preserving spillover ordering can never engage; the overflow queue
# stays unbounded by design so an already-offered block is never converted
# back into a raw-submit retry.
DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH = 8
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
        "shares_json": context.shares_json,
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
        # candidates, not cleanup attempts.
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
        self._block_replay_enumeration_owed_flag = False

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

    def replay_pending(self) -> int:
        """Queue durable candidate intents not completed by an earlier process."""
        self._coordinator._record_block_submitter_phase("replay-check-memory")
        # While startup enumeration is still owed, correctness requires the
        # outbox query even if live candidates are queued: job builds stay
        # blocked until pending candidates are known, and only a successful
        # enumeration can unblock them.
        enumeration_owed = self._coordinator._block_replay_enumeration_owed()
        with self._coordinator.lock:
            if not enumeration_owed and self.retry_candidate is not None:
                return 0
        # A live wakeup is already the lowest-latency route to qbitd. Never
        # park it behind the outbox query that exists only to recover missing
        # wakeups after queue pressure or restart.
        queue_obj = self.candidate_queue
        if not enumeration_owed and queue_obj is not None and not queue_obj.empty():
            return 0
        self._ensure_block_replay_state()
        if not enumeration_owed and not self._block_replay_candidate_queue.empty():
            return 0
        # The startup enumeration gates job issuance, so it runs with the
        # landing-class budget instead of the poll budget (issue #188 fix 4);
        # the periodic steady-state poll keeps the tight budget. The metrics
        # class follows the budget so a slow or timed-out startup enumeration
        # surfaces on the landing series the alerts watch.
        replay_query_timeout = (
            self._coordinator._block_landing_db_timeout()
            if enumeration_owed
            else None
        )
        replay_query_call_class = "landing" if enumeration_owed else "fast"
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
        if enumeration_owed and fetch_durable_page is not None:
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
                queued += self._adopt_durable_block_candidate_rows(durable_rows)
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
                queued += self._adopt_durable_block_candidate_rows(durable_rows)
                if len(durable_rows) < enumeration_limit or not enumeration_owed:
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

        A timed-out call remains registered and is reused by the next paced
        retry. This bounds the coordinator-side wait without spawning an
        unbounded pile of threads when a fake/misbehaving driver ignores the
        real PostgreSQL statement deadline. Candidate outbox mutations are
        idempotent, so a late completion converges with replay.

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
                        self._block_submitter_ledger_worker_slots.release()

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
        with self._block_submitter_ledger_calls_lock:
            if self._block_submitter_ledger_calls.get(key) is call:
                self._block_submitter_ledger_calls.pop(key, None)
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
            self._block_candidate_terminal_outcomes[key] = accepted
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

    def block_accounting_loop(self) -> None:
        self._block_accounting_thread_ident = threading.get_ident()
        self._ensure_block_accounting_state()
        while not self._coordinator.stop_event.is_set():
            self._coordinator._record_block_submitter_phase("accounting-queue")
            source_queue = None
            try:
                _priority, _sequence, task = self._block_accounting_queue.get_nowait()
                source_queue = self._block_accounting_queue
            except queue.Empty:
                try:
                    _priority, _sequence, task = (
                        self._block_accounting_overflow_queue.get_nowait()
                    )
                    source_queue = self._block_accounting_overflow_queue
                except queue.Empty:
                    if self._coordinator._run_one_invalid_block_candidate_quarantine():
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
                self._coordinator._retain_block_candidate_for_retry(task.candidate)
            finally:
                assert source_queue is not None
                source_queue.task_done()

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
                    continue
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
                # Live discoveries always outrank durable restart replay.
                for candidate_queue in (queue_obj, replay_queue):
                    if candidate_queue is None:
                        continue
                    try:
                        candidate = candidate_queue.get_nowait()
                        break
                    except queue.Empty:
                        pass
                if candidate is None:
                    self._ensure_block_candidate_disposition_state()
                    with coordinator.lock:
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

        block_hash = str(candidate.submission.block_hash_hex).lower()
        lease = coordinator._claim_block_candidate_disposition(
            block_hash,
            blocking=not defer_accounting,
        )
        if lease is None:
            # Another same-hash pass already spans node offer through durable
            # finalization. Keep this wakeup outside the global parent retry
            # slot until that lease transfers/releases: consuming it can lose
            # an accounting retry, while repeatedly prioritizing it can starve
            # unrelated live blocks.
            with coordinator.lock:
                self._block_disposition_waiting_retries[block_hash] = candidate
            coordinator._wait_for_block_candidate_retry(
                float(self.retry_initial_seconds)
            )
            return BlockCandidateRunResult(True)
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
                    # that offer either accounts or terminates.
                    coordinator._release_block_candidate_disposition(lease)
                    coordinator._retain_block_candidate_for_retry(candidate)
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
            coordinator._release_block_candidate_disposition(lease)
            coordinator._retain_block_candidate_for_retry(candidate)
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
        if node_submission is None or not node_submission.attempted:
            return
        if (
            node_submission.error is not None
            or node_submission.result is not None
        ):
            # Only a definitive success is safe to reuse: an ambiguous or
            # rejected offer must be re-offered so the node can resolve it.
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
    "_block_accounting_state_lock": "_block_accounting_state_lock",
    "_block_accounting_queue": "_block_accounting_queue",
    "_block_accounting_overflow_queue": "_block_accounting_overflow_queue",
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
    "stale_job_abandon_counts": "stale_job_abandon_counts",
    "_block_submit_metrics_lock": "_block_submit_metrics_lock",
    "block_submit_seconds_histogram": "block_submit_seconds_histogram",
    "_block_replay_enumeration_owed_flag": "_block_replay_enumeration_owed_flag",
}


def _compat_get(service: BlockCandidateService, name: str) -> Any:
    return getattr(service, _COMPATIBILITY_FIELD_MAP[name])


def _compat_set(service: BlockCandidateService, name: str, value: Any) -> None:
    setattr(service, _COMPATIBILITY_FIELD_MAP[name], value)

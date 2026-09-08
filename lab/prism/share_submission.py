"""Pure submit classification and the narrow PRISM submission orchestrator."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import threading
from typing import Any, Callable, Iterable, Mapping, NoReturn

from lab.prism.block_candidates import PrismBlockCandidate
from lab.prism.job_bundle import PRISM_JOB_BUILD_SECONDS_BUCKETS
from lab.prism.job_delivery import (
    EvictedJobEntry,
    PRISM_CREDIT_POLICY_STALE_GRACE,
    PrismJobContext,
)
from lab.prism.share_ledger import PendingShare
from lab.prism.stratum_session import ClientState


PRISM_REJECTION_STALE_JOB = "stale-job"
PRISM_REJECTION_DUPLICATE_SHARE = "duplicate-share"
PRISM_REJECTION_LOW_DIFFICULTY = "low-difficulty"
PRISM_REJECTION_MALFORMED_SUBMIT = "malformed-submit"
PRISM_REJECTION_UNAUTHORIZED_WORKER = "unauthorized-worker"
PRISM_REJECTION_UNKNOWN_JOB = "unknown-job"
PRISM_REJECTION_INVALID_EXTRANONCE = "invalid-extranonce"
PRISM_REJECTION_INVALID_NTIME_OR_NONCE = "invalid-ntime-or-nonce"
PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE = "backend-rpc-unavailable"
PRISM_REJECTION_POOL_CLOSED = "pool-closed"
DEFAULT_RECENT_SHARE_CAPACITY = 50_000
# Read-to-ack latency labels for mining.submit. Accepted shares wait for the
# group commit before their ack; rejected shares (measured when the reject
# decision is made) skip it, so the pair separates commit pressure from
# thread-scheduling/GIL pressure as connection count grows.
PRISM_SHARE_ACK_RESULTS = ("accepted", "rejected")


def empty_share_ack_histograms() -> dict[str, dict[str, Any]]:
    """Zeroed per-result share-ack histograms in the rendered shape."""

    return {
        result: {
            "buckets": {bucket: 0 for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS},
            "sum": 0.0,
            "count": 0,
        }
        for result in PRISM_SHARE_ACK_RESULTS
    }


class RecentShareIndex:
    """Thread-safe insertion-ordered duplicate window with bounded memory."""

    def __init__(
        self,
        *,
        capacity: int = DEFAULT_RECENT_SHARE_CAPACITY,
        initial: Iterable[tuple[str, str]] = (),
    ) -> None:
        if capacity < 1:
            raise ValueError("recent share capacity must be positive")
        self.capacity = int(capacity)
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple[str, str], None] = OrderedDict()
        self.replace(initial)

    def reserve(self, share_key: tuple[str, str]) -> bool:
        with self._lock:
            if share_key in self._entries:
                return False
            self._entries[share_key] = None
            while len(self._entries) > self.capacity:
                self._entries.popitem(last=False)
            return True

    def release(self, share_key: tuple[str, str]) -> None:
        with self._lock:
            self._entries.pop(share_key, None)

    def replace(self, entries: Iterable[tuple[str, str]]) -> None:
        with self._lock:
            self._entries.clear()
            for share_key in entries:
                self._entries[share_key] = None
                while len(self._entries) > self.capacity:
                    self._entries.popitem(last=False)

    def snapshot(self) -> tuple[tuple[str, str], ...]:
        with self._lock:
            return tuple(self._entries)


class RecentShareCompatibilityField:
    """Temporary coordinator view over submission-owned duplicate state."""

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return self
        service = instance.__dict__.get("_share_submission_service")
        if service is not None:
            return set(service.recent_shares.snapshot())
        return instance.__dict__.get("recent_share_keys", set())

    def __set__(self, instance: Any, value: Iterable[tuple[str, str]]) -> None:
        service = instance.__dict__.get("_share_submission_service")
        if service is not None:
            service.recent_shares.replace(value)
        else:
            instance.__dict__["recent_share_keys"] = set(value)


class BlockSolvesDroppedCompatibilityField:
    """Owner-routing coordinator view over submission-owned drop accounting.

    Current callers read ``coordinator.block_solves_dropped_counts`` directly,
    so the historical attribute stays readable and writable while the
    submission owner keeps the single mutable copy. Reads return a copied
    snapshot once the owner exists; pre-owner values live in the instance
    dict until service construction adopts them, and an unset field keeps the
    pre-extraction ``getattr``-with-default semantics by raising
    AttributeError.
    """

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return self
        service = instance.__dict__.get("_share_submission_service")
        if service is not None:
            return service.dropped_solves_snapshot()
        try:
            return instance.__dict__["block_solves_dropped_counts"]
        except KeyError:
            raise AttributeError("block_solves_dropped_counts") from None

    def __set__(self, instance: Any, value: Mapping[str, int]) -> None:
        service = instance.__dict__.get("_share_submission_service")
        if service is not None:
            service.replace_dropped_solves(value)
        else:
            instance.__dict__["block_solves_dropped_counts"] = dict(value)


@dataclass(frozen=True)
class SubmitRequest:
    worker_name: str
    job_id: str
    extranonce2_hex: str
    ntime_hex: str
    nonce_hex: str
    version_bits_hex: str | None


@dataclass(frozen=True)
class SubmitRejected(ValueError):
    code: int
    reason: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class SubmitContextInput:
    active_context: PrismJobContext | None
    retained_entry: EvictedJobEntry | None
    current_tip: str
    stale_grace_eligible: bool
    # A retained entry resumed by a different connection than the one the job
    # was delivered to. Cross-connection resumes are same-tip only; the
    # stale-grace window anchors on the submitting connection's delivery
    # state, which says nothing about work another (dead) connection
    # delivered.
    cross_connection: bool = False


@dataclass(frozen=True)
class SubmitContextDecision:
    context: PrismJobContext
    current_tip: str
    source: str
    credit_policy: str | None
    retained_entry: EvictedJobEntry | None
    cross_connection: bool = False


@dataclass(frozen=True)
class SubmitControlSnapshot:
    """One bounded capture of coordinator-owned submit admission state."""

    pool_open: bool
    active_context: PrismJobContext | None
    published_tip: str | None


@dataclass(frozen=True)
class SubmitWorkDecision:
    share_key: tuple[str, str]
    block_worthy: bool
    credit_share_on_accept: bool
    route: str


@dataclass(frozen=True)
class ShareSubmissionPorts:
    reject: Callable[[SubmitRejected, str | None], NoReturn]
    control_snapshot: Callable[
        [ClientState, str],
        SubmitControlSnapshot,
    ]
    note_submitted: Callable[[str, ClientState], None]
    retained_entry: Callable[[ClientState, str], EvictedJobEntry | None]
    live_tip: Callable[[], str]
    stale_grace_eligible: Callable[[ClientState, PrismJobContext, str], bool]
    assemble: Callable[[ClientState, PrismJobContext, SubmitRequest, int], Any]
    pending_share: Callable[
        [PrismJobContext, Any, str, str | None],
        PendingShare,
    ]
    # Returns the durable candidate_outbox_state when the accepted share
    # carried a block-candidate intent whose outbox row already exists
    # (submitted/abandoned short-circuits the enqueue), else None.
    append_share: Callable[
        [
            ClientState,
            PrismJobContext,
            Any,
            PendingShare,
            str | None,
            dict[str, Any] | None,
        ],
        str | None,
    ]
    note_retained_submit: Callable[[str | None, bool], None]
    note_collection_candidate: Callable[[PrismJobContext, Any], None]
    candidate_intent: Callable[[PrismBlockCandidate], dict[str, Any]]
    finish_pending_commit: Callable[[PendingShare], None]
    record_terminal_outcome: Callable[[str, bool], None]
    submit_synchronous_candidate: Callable[
        [
            PrismBlockCandidate,
            tuple[str, str],
            str,
            EvictedJobEntry | None,
            str | None,
        ],
        bool,
    ]
    enqueue_candidate: Callable[[PrismBlockCandidate], bool]
    log: Callable[[str], None]
    log_exception: Callable[[], None]


def parse_submit_request(params: list[object]) -> SubmitRequest:
    """Parse immutable wire input without touching coordinator state."""

    if len(params) < 5:
        raise SubmitRejected(
            20,
            PRISM_REJECTION_MALFORMED_SUBMIT,
            "submit params are incomplete",
        )
    worker_name, job_id, extranonce2_hex, ntime_hex, nonce_hex = (
        str(item) for item in params[:5]
    )
    return SubmitRequest(
        worker_name=worker_name,
        job_id=job_id,
        extranonce2_hex=extranonce2_hex,
        ntime_hex=ntime_hex,
        nonce_hex=nonce_hex,
        version_bits_hex=str(params[5]) if len(params) > 5 else None,
    )


def validate_submit_request(
    request: SubmitRequest,
    *,
    authorized_username: str,
    pool_open: bool,
    extranonce2_size: int,
) -> None:
    """Validate request identity and fixed-width fields in protocol order.

    A closed pool rejects before any share accounting: post-close submits
    must not inflate global/per-worker submitted totals (the stale-percent
    denominator) or vardiff windows they can never contribute to.
    """

    if request.worker_name != authorized_username:
        raise SubmitRejected(
            20,
            PRISM_REJECTION_UNAUTHORIZED_WORKER,
            "submit username does not match authorized username",
        )
    if not pool_open:
        raise SubmitRejected(
            21,
            PRISM_REJECTION_POOL_CLOSED,
            "pool is no longer accepting shares",
        )
    if len(request.extranonce2_hex) != extranonce2_size * 2:
        raise SubmitRejected(
            20,
            PRISM_REJECTION_INVALID_EXTRANONCE,
            "unexpected extranonce2 size",
        )
    if len(request.ntime_hex) != 8 or len(request.nonce_hex) != 8:
        raise SubmitRejected(
            20,
            PRISM_REJECTION_INVALID_NTIME_OR_NONCE,
            "ntime and nonce must be 4-byte hex strings",
        )


def classify_submit_context(value: SubmitContextInput) -> SubmitContextDecision:
    """Choose current, retained, or stale-grace work from captured facts."""

    context = value.active_context
    source = "active"
    retained_entry: EvictedJobEntry | None = None
    if context is None:
        retained_entry = value.retained_entry
        if retained_entry is None:
            raise SubmitRejected(21, PRISM_REJECTION_UNKNOWN_JOB, "stale job")
        context = retained_entry.context
        source = "retained"
    parent_hash = str(context.template["previousblockhash"])
    if parent_hash == value.current_tip:
        return SubmitContextDecision(
            context=context,
            current_tip=value.current_tip,
            source=source,
            credit_policy=None,
            retained_entry=retained_entry,
            cross_connection=value.cross_connection,
        )
    if value.cross_connection:
        # Cross-connection resumes are same-tip only. A tip that moves
        # between the graveyard lookup and this classification must not
        # fall through to stale grace: that window anchors on the
        # submitting connection's delivery state, which says nothing
        # about work another (dead) connection delivered.
        raise SubmitRejected(21, PRISM_REJECTION_STALE_JOB, "stale job")
    if not value.stale_grace_eligible:
        raise SubmitRejected(21, PRISM_REJECTION_STALE_JOB, "stale job")
    return SubmitContextDecision(
        context=context,
        current_tip=value.current_tip,
        source=source,
        credit_policy=PRISM_CREDIT_POLICY_STALE_GRACE,
        retained_entry=retained_entry,
        cross_connection=value.cross_connection,
    )


def classify_submit_work(
    context: PrismJobContext,
    submission: Any,
    *,
    credit_policy: str | None,
) -> SubmitWorkDecision:
    """Classify a proof as an ordinary share or one of two block routes.

    A floor-bearing listener holds the advertised share target above the
    qbit network target while network difficulty sits below the floor, so a
    submission can solve a block yet miss the share target. Never discard a
    block over share bookkeeping: reject as low-difficulty only when the
    hash is not block-worthy. Stale-grace work is never block-worthy: its
    parent is no longer the chain tip.
    """

    block_worthy = bool(submission.block_pass) and (
        credit_policy != PRISM_CREDIT_POLICY_STALE_GRACE
    )
    if not submission.share_pass and not block_worthy:
        raise SubmitRejected(
            23,
            PRISM_REJECTION_LOW_DIFFICULTY,
            "low difficulty share",
        )
    if not block_worthy:
        route = "share"
    elif submission.share_pass:
        route = "async_block"
    else:
        route = "synchronous_block"
    return SubmitWorkDecision(
        share_key=(context.worker.username, submission.header_hex),
        block_worthy=block_worthy,
        credit_share_on_accept=route == "synchronous_block",
        route=route,
    )


class ShareSubmissionService:
    """Apply one pure submit decision through the coordinator's narrow ports.

    Beyond the hot path, the service owns the share-ACK latency histograms
    (observed by the session owner through the coordinator's
    ``_observe_share_ack_seconds`` routing seam) and the stale-grace
    ``block_solves_dropped_counts`` accounting. Prometheus rendering stays in
    the coordinator through PR 79 and reads this owner's copied snapshots.
    """

    def __init__(
        self,
        ports: ShareSubmissionPorts,
        *,
        extranonce2_size: int,
        recent_shares: RecentShareIndex | None = None,
    ) -> None:
        self.ports = ports
        self.extranonce2_size = int(extranonce2_size)
        self.recent_shares = recent_shares or RecentShareIndex()
        self._metrics_lock = threading.Lock()
        self.share_ack_histograms = empty_share_ack_histograms()
        self.block_solves_dropped_counts: dict[str, int] = {"stale_grace": 0}

    def observe_share_ack_seconds(
        self,
        result: str,
        elapsed_seconds: float,
    ) -> None:
        if result not in PRISM_SHARE_ACK_RESULTS:
            raise ValueError(f"unknown share ack result: {result}")
        with self._metrics_lock:
            histogram = self.share_ack_histograms[result]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + max(
                0.0, elapsed_seconds
            )
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def share_ack_snapshot(self) -> dict[str, dict[str, Any]]:
        """Copy the per-result share-ack histograms under the owner lock."""

        with self._metrics_lock:
            return {
                result: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for result, histogram in self.share_ack_histograms.items()
            }

    def replace_share_ack_histograms(
        self,
        histograms: Mapping[str, Mapping[str, Any]],
    ) -> None:
        """Adopt legacy pre-owner histogram state at service construction."""

        with self._metrics_lock:
            self.share_ack_histograms = {
                result: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for result, histogram in histograms.items()
            }

    def dropped_solves_snapshot(self) -> dict[str, int]:
        """Copy the bounded-policy dropped block-solve counters."""

        with self._metrics_lock:
            return dict(self.block_solves_dropped_counts)

    def replace_dropped_solves(self, counts: Mapping[str, int]) -> None:
        with self._metrics_lock:
            self.block_solves_dropped_counts = {
                str(reason): int(count) for reason, count in counts.items()
            }

    def _count_dropped_block_solve(self, reason: str) -> None:
        with self._metrics_lock:
            self.block_solves_dropped_counts[reason] = (
                int(self.block_solves_dropped_counts.get(reason, 0)) + 1
            )

    def _reject(self, rejected: SubmitRejected, *, worker: str | None) -> NoReturn:
        self.ports.reject(rejected, worker)

    def _context_decision(
        self,
        client: ClientState,
        request: SubmitRequest,
        control: SubmitControlSnapshot,
    ) -> SubmitContextDecision:
        active = control.active_context
        retained = (
            self.ports.retained_entry(client, request.job_id)
            if active is None
            else None
        )
        if active is None and retained is None:
            # Reject before any live-tip fallback: an unknown job needs no
            # RPC to classify.
            try:
                return classify_submit_context(
                    SubmitContextInput(
                        active_context=None,
                        retained_entry=None,
                        current_tip="",
                        stale_grace_eligible=False,
                    )
                )
            except SubmitRejected as rejected:
                self._reject(rejected, worker=request.worker_name)
        current_tip = (
            control.published_tip
            if control.published_tip is not None
            else self.ports.live_tip()
        )
        # Share classification (normal and stale-grace alike) is deliberately
        # point-in-time against this single tip read: a tip that advances
        # between here and the ledger append does not retroactively invalidate
        # the share, exactly as a normal current-tip share stays credited when
        # the tip moves during processing. Re-checking would add an RPC per
        # share during post-block bursts only to reject valid work over
        # processing latency. Block submission is different (chain state):
        # submit_block_candidate re-checks the tip under lock before
        # submitblock, and stale-grace shares never reach it.
        context = active if active is not None else retained.context
        cross_connection = (
            retained is not None
            and retained.connection_id != client.connection_id
        )
        stale_eligible = False
        if (
            not cross_connection
            and str(context.template["previousblockhash"]) != current_tip
        ):
            # A cross-connection retained entry never consults the window: it
            # is same-tip only, so the mismatch rejects as stale below
            # without any parent-tip RPC (matching evicted_submit_context).
            try:
                stale_eligible = self.ports.stale_grace_eligible(
                    client,
                    context,
                    current_tip,
                )
            except Exception:
                self.ports.log(
                    "prism coordinator: failed to classify evicted submit context"
                    if retained is not None
                    else "prism coordinator: failed to classify stale-grace parent tip"
                )
                self.ports.log_exception()
                self._reject(
                    SubmitRejected(
                        20,
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "failed to classify stale-grace parent tip",
                    ),
                    worker=request.worker_name,
                )
        try:
            return classify_submit_context(
                SubmitContextInput(
                    active_context=active,
                    retained_entry=retained,
                    current_tip=current_tip,
                    stale_grace_eligible=stale_eligible,
                    cross_connection=cross_connection,
                )
            )
        except SubmitRejected as rejected:
            self._reject(rejected, worker=request.worker_name)

    def handle(self, client: ClientState, params: list[object]) -> bool:
        try:
            request = parse_submit_request(params)
        except SubmitRejected as rejected:
            self._reject(rejected, worker=client.username or None)
        # Capture pool admission, active-job membership, and published-tip
        # authority together. All accounting, RPC fallback, hashing, and
        # persistence remain outside the coordinator lock behind this port.
        control = self.ports.control_snapshot(client, request.job_id)
        try:
            validate_submit_request(
                request,
                authorized_username=client.username,
                pool_open=control.pool_open,
                extranonce2_size=self.extranonce2_size,
            )
        except SubmitRejected as rejected:
            worker = (
                client.username or None
                if rejected.reason == PRISM_REJECTION_UNAUTHORIZED_WORKER
                else request.worker_name
            )
            self._reject(rejected, worker=worker)

        # Count submitted shares once, after the format checks, so the
        # per-worker counter and the aggregate submitted total cover the same
        # population; malformed extranonce/ntime submits are recorded only as
        # rejections, not submits.
        self.ports.note_submitted(request.worker_name, client)
        decision = self._context_decision(client, request, control)
        submit_version_mask = client.version_mask
        if decision.cross_connection:
            # In-flight work from the dead connection was rolled under the
            # mask negotiated there; the replacement's own mask (often still
            # 0 before mining.configure) must not judge those version bits.
            submit_version_mask = int(
                getattr(decision.context, "version_mask", 0)
            )
        try:
            submission = self.ports.assemble(
                client,
                decision.context,
                request,
                submit_version_mask,
            )
        except ValueError as error:
            self._reject(
                SubmitRejected(
                    20,
                    PRISM_REJECTION_MALFORMED_SUBMIT,
                    f"malformed submit: {error}",
                ),
                worker=request.worker_name,
            )
        # A retained job keeps its original worker even if the connection is
        # later re-authorized. Deduplication must use that immutable identity:
        # otherwise the same header can be replayed under each new username.
        share_key = (decision.context.worker.username, submission.header_hex)
        if not self.recent_shares.reserve(share_key):
            self._reject(
                SubmitRejected(
                    22,
                    PRISM_REJECTION_DUPLICATE_SHARE,
                    "duplicate share",
                ),
                worker=request.worker_name,
            )
        if (
            submission.block_pass
            and decision.credit_policy == PRISM_CREDIT_POLICY_STALE_GRACE
        ):
            # Count the dropped solve before low-difficulty classification:
            # the drop is real whether the stale-grace share passes its share
            # target (credited below) or misses it (rejected below).
            self._count_dropped_block_solve("stale_grace")
            self.ports.log(
                "prism coordinator: stale-grace block solve dropped "
                f"hash={submission.block_hash_hex} "
                f"parent={decision.context.template['previousblockhash']}"
            )
        try:
            work = classify_submit_work(
                decision.context,
                submission,
                credit_policy=decision.credit_policy,
            )
        except SubmitRejected as rejected:
            self._reject(rejected, worker=request.worker_name)

        if work.block_worthy and decision.context.collection_only:
            # Collection-mode jobs are block-worthy too: their signed
            # bootstrap manifest already commits the whole coinbase to the
            # submitting worker, so the solve settles solver-pays-all.
            self.ports.note_collection_candidate(decision.context, submission)
        pending_share = self.ports.pending_share(
            decision.context,
            submission,
            request.ntime_hex,
            decision.credit_policy,
        )
        if work.route == "share":
            try:
                self.ports.append_share(
                    client,
                    decision.context,
                    submission,
                    pending_share,
                    decision.credit_policy,
                    None,
                )
                if decision.retained_entry is not None:
                    self.ports.note_retained_submit(
                        decision.credit_policy,
                        decision.cross_connection,
                    )
            except BaseException:
                self.recent_shares.release(work.share_key)
                raise
            return False

        candidate = PrismBlockCandidate(
            context=decision.context,
            submission=submission,
            # The mined coinbase embeds the extranonce1 the job was stamped
            # with. A cross-connection resume submits through a client whose
            # own extranonce1 differs from the retained job's, and the audit
            # bundle suffix must match the coinbase actually in the block,
            # so the job's value is authoritative whenever it carries one.
            extranonce1_hex=str(
                getattr(decision.context.job, "extranonce1_hex", None)
                or client.extranonce1_hex
            ),
            extranonce2_hex=request.extranonce2_hex,
            pending_share=pending_share,
            client=client,
            credit_share_on_accept=work.credit_share_on_accept,
        )
        if work.route == "synchronous_block":
            return self._submit_synchronous(
                candidate,
                work=work,
                request=request,
                decision=decision,
            )
        return self._submit_asynchronous(
            candidate,
            work=work,
            client=client,
            decision=decision,
            submission=submission,
        )

    def _submit_synchronous(
        self,
        candidate: PrismBlockCandidate,
        *,
        work: SubmitWorkDecision,
        request: SubmitRequest,
        decision: SubmitContextDecision,
    ) -> bool:
        # The hash solved a block but missed the assigned share target
        # (possible only while the listener floor sits above network
        # difficulty). It is a valid share ONLY if the block lands, so it
        # lands synchronously behind this port: the durable-outbox boundary,
        # the terminal-state short circuit, and the low-difficulty rejection
        # for a candidate that does not land all run coordinator-side where
        # the block-submitter machinery lives.
        return self.ports.submit_synchronous_candidate(
            candidate,
            work.share_key,
            request.worker_name,
            decision.retained_entry,
            decision.credit_policy,
        )

    def _submit_asynchronous(
        self,
        candidate: PrismBlockCandidate,
        *,
        work: SubmitWorkDecision,
        client: ClientState,
        decision: SubmitContextDecision,
        submission: Any,
    ) -> bool:
        # A block-worthy submission that met the share target is a valid
        # share regardless of the block's fate: credit it now, acknowledge
        # the miner immediately, and land the block from the dedicated
        # submitter thread (ckpool/btcpool/StratumV2 semantics). An orphaned
        # candidate keeps its share credit.
        try:
            candidate_intent = self.ports.candidate_intent(candidate)
            durable_candidate_state = self.ports.append_share(
                client,
                decision.context,
                submission,
                candidate.pending_share,
                decision.credit_policy,
                candidate_intent,
            )
            if decision.retained_entry is not None:
                self.ports.note_retained_submit(
                    decision.credit_policy,
                    decision.cross_connection,
                )
        except BaseException:
            # Idempotent with the append's own release; also covers an intent
            # serialization failure before the append started.
            self.ports.finish_pending_commit(candidate.pending_share)
            self.recent_shares.release(work.share_key)
            raise
        if durable_candidate_state in {"submitted", "abandoned"}:
            # A process restart clears the in-memory disposition cache, but
            # the existing outbox row remains authoritative. Join its
            # terminal result instead of enqueueing a new node offer.
            self.ports.record_terminal_outcome(
                submission.block_hash_hex,
                durable_candidate_state == "submitted",
            )
            return False
        self.ports.enqueue_candidate(candidate)
        return False

"""Pure submit classification and the narrow PRISM submission orchestrator."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import threading
from typing import Any, Callable, Iterable, NoReturn

from lab.prism.block_candidates import PrismBlockCandidate, block_candidate_intent
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


@dataclass(frozen=True)
class SubmitContextDecision:
    context: PrismJobContext
    current_tip: str
    source: str
    credit_policy: str | None
    retained_entry: EvictedJobEntry | None


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
    assemble: Callable[[ClientState, PrismJobContext, SubmitRequest], Any]
    pending_share: Callable[
        [PrismJobContext, Any, str, str | None],
        PendingShare,
    ]
    append_share: Callable[
        [
            ClientState,
            PrismJobContext,
            Any,
            PendingShare,
            str | None,
            dict[str, Any] | None,
        ],
        None,
    ]
    note_retained_submit: Callable[[str | None], None]
    note_collection_candidate: Callable[[PrismJobContext, Any], None]
    ledger: Callable[[], Any]
    share_writer: Callable[[], Any]
    finish_pending_attempt: Callable[[PendingShare], None]
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
    """Validate request identity and fixed-width fields in protocol order."""

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
        )
    if not value.stale_grace_eligible:
        raise SubmitRejected(21, PRISM_REJECTION_STALE_JOB, "stale job")
    return SubmitContextDecision(
        context=context,
        current_tip=value.current_tip,
        source=source,
        credit_policy=PRISM_CREDIT_POLICY_STALE_GRACE,
        retained_entry=retained_entry,
    )


def classify_submit_work(
    context: PrismJobContext,
    submission: Any,
    *,
    credit_policy: str | None,
) -> SubmitWorkDecision:
    """Classify a proof as an ordinary share or one of two block routes."""

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
    """Apply one pure submit decision through the coordinator's narrow ports."""

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
        current_tip = control.published_tip or self.ports.live_tip()
        context = active or (retained.context if retained is not None else None)
        stale_eligible = False
        if (
            context is not None
            and str(context.template["previousblockhash"]) != current_tip
        ):
            try:
                stale_eligible = self.ports.stale_grace_eligible(
                    client,
                    context,
                    current_tip,
                )
            except Exception:
                self.ports.log(
                    "prism coordinator: failed to classify stale-grace parent tip"
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

        self.ports.note_submitted(request.worker_name, client)
        decision = self._context_decision(client, request, control)
        try:
            submission = self.ports.assemble(client, decision.context, request)
        except ValueError as error:
            self._reject(
                SubmitRejected(
                    20,
                    PRISM_REJECTION_MALFORMED_SUBMIT,
                    f"malformed submit: {error}",
                ),
                worker=request.worker_name,
            )
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
        try:
            work = classify_submit_work(
                decision.context,
                submission,
                credit_policy=decision.credit_policy,
            )
        except SubmitRejected as rejected:
            self._reject(rejected, worker=request.worker_name)

        if work.block_worthy and decision.context.collection_only:
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
                    self.ports.note_retained_submit(decision.credit_policy)
            except BaseException:
                self.recent_shares.release(work.share_key)
                raise
            return False

        candidate = PrismBlockCandidate(
            context=decision.context,
            submission=submission,
            extranonce1_hex=client.extranonce1_hex,
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
        persist_intent = getattr(
            self.ports.ledger(),
            "persist_block_candidate_intent",
            None,
        )
        candidate_intent_durable = False
        share_writer = self.ports.share_writer()
        try:
            intent = block_candidate_intent(candidate)
            if callable(persist_intent):
                persist_intent(intent)
                candidate_intent_durable = True
            share_writer.begin_candidate_actor(candidate.pending_share)
        except BaseException:
            if not candidate_intent_durable:
                self.ports.finish_pending_attempt(candidate.pending_share)
            self.recent_shares.release(work.share_key)
            raise
        try:
            return self.ports.submit_synchronous_candidate(
                candidate,
                work.share_key,
                request.worker_name,
                decision.retained_entry,
                decision.credit_policy,
            )
        finally:
            share_writer.finish_candidate_actor(candidate.pending_share)

    def _submit_asynchronous(
        self,
        candidate: PrismBlockCandidate,
        *,
        work: SubmitWorkDecision,
        client: ClientState,
        decision: SubmitContextDecision,
        submission: Any,
    ) -> bool:
        try:
            intent = block_candidate_intent(candidate)
        except BaseException:
            self.ports.finish_pending_attempt(candidate.pending_share)
            self.recent_shares.release(work.share_key)
            raise
        try:
            self.ports.append_share(
                client,
                decision.context,
                submission,
                candidate.pending_share,
                decision.credit_policy,
                intent,
            )
            if decision.retained_entry is not None:
                self.ports.note_retained_submit(decision.credit_policy)
        except BaseException:
            self.recent_shares.release(work.share_key)
            raise
        self.ports.enqueue_candidate(candidate)
        return False

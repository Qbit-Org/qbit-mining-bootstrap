"""Durable PRISM block-candidate codec, replay queue, and submitter service."""

from __future__ import annotations

import dataclasses
import json
import queue
import threading
import time
import traceback
from contextlib import AbstractContextManager
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable

from lab.prism import direct_stratum
from lab.prism.coordinator_shutdown import ShutdownInProgress
from lab.prism.job_delivery import PrismJobContext
from lab.prism.share_ledger import PendingShare
from lab.prism.stratum_session import ClientState, WorkerIdentity


MAX_PENDING_BLOCK_CANDIDATES = 32
DEFAULT_BLOCK_CANDIDATE_RETRY_INITIAL_SECONDS = 0.25
DEFAULT_BLOCK_CANDIDATE_RETRY_MAX_SECONDS = 30.0
BLOCK_CANDIDATE_RETRY_HEARTBEAT_SLICE_SECONDS = 0.25
BLOCK_CANDIDATE_INTENT_SCHEMA = "qbit.prism.block-candidate-intent.v1"


@dataclass(frozen=True)
class PrismBlockCandidate:
    """A durable block-worthy submission handled outside the share hot path."""

    context: PrismJobContext
    submission: direct_stratum.DirectQbitSubmission
    extranonce1_hex: str
    extranonce2_hex: str
    pending_share: PendingShare
    client: ClientState
    credit_share_on_accept: bool = False


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


@dataclass(frozen=True)
class BlockCandidatePorts:
    ledger: Callable[[], Any]
    stop_event: Callable[[], threading.Event]
    writer_operation: Callable[[str], AbstractContextManager[object]]
    submit_candidate: Callable[[PrismBlockCandidate], bool]
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


def block_candidate_intent(candidate: PrismBlockCandidate) -> dict[str, Any]:
    """Encode the immutable JSON needed to resume a candidate after restart."""

    context = candidate.context
    submission = candidate.submission
    intent = {
        "schema": BLOCK_CANDIDATE_INTENT_SCHEMA,
        "block_hash_hex": str(submission.block_hash_hex).lower(),
        "block_hex": str(getattr(submission, "block_hex", "")),
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
                for row in getattr(context, "prospective_prior_balances", ())
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
    if str(template.get("previousblockhash", "")).lower() != str(
        intent["parent_hash"]
    ).lower():
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
    ) -> None:
        self.ports = ports
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

    def adopt_replayed_candidate(self, candidate: PrismBlockCandidate) -> None:
        if candidate.credit_share_on_accept:
            self.ports.share_writer().adopt_pending_share(candidate.pending_share)

    def enqueue(self, candidate: PrismBlockCandidate) -> bool:
        try:
            self.candidate_queue.put_nowait(candidate)
            return True
        except queue.Full:
            with self._state_lock:
                self.wakeups_coalesced += 1
            self.ports.log(
                "prism coordinator: block candidate wakeup coalesced "
                f"hash={candidate.submission.block_hash_hex} "
                "(submitter queue full)"
            )
            return False

    def replay_pending(self) -> int:
        with self._state_lock:
            if self.retry_candidate is not None:
                return 0
        ledger = self.ports.ledger()
        pending_rows = getattr(ledger, "pending_block_candidate_rows", None)
        if callable(pending_rows):
            durable_rows = pending_rows(limit=MAX_PENDING_BLOCK_CANDIDATES)
        else:
            pending = getattr(ledger, "pending_block_candidates", None)
            if not callable(pending):
                return 0
            durable_rows = [
                {
                    "block_hash": (
                        intent.get("block_hash_hex", "")
                        if isinstance(intent, dict)
                        else ""
                    ),
                    "candidate": intent,
                }
                for intent in pending(limit=MAX_PENDING_BLOCK_CANDIDATES)
            ]
        if not self.candidate_queue.empty():
            return 0
        queued = 0
        for durable_row in durable_rows:
            durable_block_hash = ""
            candidate: PrismBlockCandidate | None = None
            try:
                if not isinstance(durable_row, dict):
                    raise ValueError("durable block candidate row is not an object")
                durable_block_hash = str(durable_row["block_hash"]).lower()
                intent = durable_row["candidate"]
                if not isinstance(intent, dict):
                    raise ValueError("durable block candidate intent is not an object")
                intent_block_hash = str(intent.get("block_hash_hex", "")).lower()
                if not durable_block_hash or intent_block_hash != durable_block_hash:
                    raise ValueError(
                        "durable block candidate row key does not match intent"
                    )
                decoded_candidate = block_candidate_from_intent(intent)
                self.adopt_replayed_candidate(decoded_candidate)
                # Publish the decoded object to poison cleanup only after its
                # durable credit holder was adopted successfully.
                candidate = decoded_candidate
                self.ports.begin_preview(
                    durable_block_hash,
                    int(intent["expected_height"]),
                )
                if self.enqueue(candidate):
                    queued += 1
            except Exception:
                terminalized = False
                if durable_block_hash:
                    self.ports.clear_preview(durable_block_hash, True)
                self.ports.log(
                    "prism coordinator: invalid durable block candidate intent"
                )
                traceback.print_exc()
                quarantine = getattr(
                    ledger,
                    "mark_block_candidate_abandoned",
                    None,
                )
                if durable_block_hash and callable(quarantine):
                    try:
                        quarantined = quarantine(
                            block_hash=durable_block_hash,
                            error="invalid durable candidate intent",
                        )
                        terminalized = True
                        self.ports.clear_preview(durable_block_hash, False)
                        if quarantined:
                            self.clear_retry_state(durable_block_hash)
                            with self._state_lock:
                                self.poisoned += 1
                    except Exception:
                        traceback.print_exc()
                if (
                    terminalized
                    and candidate is not None
                    and candidate.credit_share_on_accept
                ):
                    self.ports.finish_pending_candidate(candidate.pending_share)
        if queued:
            self.ports.log(
                f"prism coordinator: replayed {queued} pending block candidate(s)"
            )
        return queued

    def run(self) -> None:
        while not self.ports.stop_event().is_set():
            self.ports.record_heartbeat("block_submitter")
            try:
                self.ports.replay_entrypoint()
                self.ports.submit_next_entrypoint(1.0)
            except ShutdownInProgress:
                return

    def submit_next(self, timeout: float | None = None) -> BlockCandidateRunResult:
        with self._state_lock:
            candidate = self.retry_candidate
            if candidate is not None:
                self.retry_candidate = None
        if candidate is None:
            try:
                if timeout is None:
                    candidate = self.candidate_queue.get_nowait()
                else:
                    candidate = self.candidate_queue.get(timeout=timeout)
            except queue.Empty:
                return BlockCandidateRunResult(False)
        self.outcome.refresh_client = None
        try:
            with self.ports.writer_operation("accepted_block_handling"):
                ran = self.submit_writer(candidate)
                refresh_client = getattr(self.outcome, "refresh_client", None)
                self.outcome.refresh_client = None
        except ShutdownInProgress:
            return BlockCandidateRunResult(False)
        if refresh_client is not None and not self.ports.stop_event().is_set():
            self.ports.refresh_after_accept(refresh_client)
        return BlockCandidateRunResult(ran, refresh_client)

    def submit_writer(self, candidate: PrismBlockCandidate) -> bool:
        if not candidate.credit_share_on_accept:
            return self.submit_actor_owned(candidate)
        share_writer = self.ports.share_writer()
        share_writer.begin_candidate_actor(candidate.pending_share)
        try:
            return self.submit_actor_owned(candidate)
        finally:
            share_writer.finish_candidate_actor(candidate.pending_share)

    def attempt(self, candidate: PrismBlockCandidate) -> BlockCandidateAttemptResult:
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

    def submit_actor_owned(self, candidate: PrismBlockCandidate) -> bool:
        block_hash = str(candidate.submission.block_hash_hex).lower()
        try:
            self.mark_attempted(block_hash)
        except Exception:
            self.ports.log(
                "prism coordinator: could not record block candidate attempt "
                f"hash={block_hash}"
            )
            traceback.print_exc()
            self.retain_for_retry(candidate)
            self.wait_for_retry(self.ports.next_retry_delay(block_hash))
            return True
        with self._state_lock:
            pending_finalize = self.finalize_retries.get(block_hash)
        if pending_finalize is not None:
            accepted, error = pending_finalize
            return self.finalize(
                candidate,
                block_hash=block_hash,
                accepted=accepted,
                error=error,
            )
        result = self.attempt(candidate)
        if result.retryable(self.retryable_reasons):
            self.ports.log(
                "prism coordinator: retained block candidate for retry "
                f"hash={block_hash} reason={result.reason or 'exception'}"
            )
            self.retain_for_retry(candidate)
            self.wait_for_retry(self.ports.next_retry_delay(block_hash))
            return True
        if not result.accepted:
            try:
                self.ports.reject_terminal_prepared(candidate)
            except Exception:
                self.ports.log(
                    "prism coordinator: prepared block cleanup failed "
                    f"hash={block_hash}"
                )
                traceback.print_exc()
                self.record_deferred(
                    "backend-rpc-unavailable",
                    "could not reject prepared state for terminal candidate",
                    worker=candidate.client.username or None,
                )
                self.retain_for_retry(candidate)
                self.wait_for_retry(
                    self.ports.next_retry_delay(block_hash)
                )
                return True
        return self.finalize(
            candidate,
            block_hash=block_hash,
            accepted=result.accepted,
            error=result.error,
        )

    def finalize(
        self,
        candidate: PrismBlockCandidate,
        *,
        block_hash: str,
        accepted: bool,
        error: str,
    ) -> bool:
        """Retry only a terminal candidate's durable outbox transition."""
        self.ports.clear_preview(block_hash, not accepted)
        finish_name = (
            "mark_block_candidate_submitted"
            if accepted
            else "mark_block_candidate_abandoned"
        )
        finish = getattr(self.ports.ledger(), finish_name, None)
        if callable(finish):
            try:
                if accepted:
                    finish(block_hash=block_hash)
                else:
                    finish(block_hash=block_hash, error=error)
                    self.ports.clear_preview(block_hash, False)
            except Exception:
                self.ports.log(
                    "prism coordinator: could not finalize durable block candidate "
                    f"hash={block_hash}"
                )
                traceback.print_exc()
                with self._state_lock:
                    first_failure = block_hash not in self.finalize_retries
                    self.finalize_retries[block_hash] = (accepted, error)
                self.ports.finish_pending_candidate(candidate.pending_share)
                self.retain_for_retry(candidate, retain_share_floor=False)
                if accepted and first_failure:
                    self.outcome.refresh_client = candidate.client
                    return True
                self.wait_for_retry(
                    self.ports.next_retry_delay(block_hash)
                )
                return True
        elif not accepted:
            self.ports.clear_preview(block_hash, False)
        with self._state_lock:
            self.finalize_retries.pop(block_hash, None)
        self.clear_retry_state(block_hash)
        self.ports.finish_pending_candidate(candidate.pending_share)
        if accepted:
            self.outcome.refresh_client = candidate.client
        return True

    def retain_for_retry(
        self,
        candidate: PrismBlockCandidate,
        *,
        retain_share_floor: bool = True,
    ) -> None:
        candidate_height = int(candidate.context.template["height"])
        candidate_hash = str(candidate.submission.block_hash_hex).lower()
        if candidate.credit_share_on_accept and retain_share_floor:
            self.ports.share_writer().adopt_pending_share(candidate.pending_share)
        with self._state_lock:
            self.retries += 1
            existing = self.retry_candidate
            if existing is None:
                self.retry_candidate = candidate
            else:
                existing_height = int(existing.context.template["height"])
                existing_hash = str(existing.submission.block_hash_hex).lower()
                if candidate_hash == existing_hash or candidate_height < existing_height:
                    self.retry_candidate = candidate

    def next_retry_delay(self, block_hash: str) -> float:
        with self._state_lock:
            delay = float(
                self.retry_delays.get(block_hash, self.retry_initial_seconds)
            )
            self.retry_delays[block_hash] = min(
                self.retry_max_seconds,
                max(self.retry_initial_seconds, delay * 2),
            )
        return min(delay, self.retry_max_seconds)

    def clear_retry_state(self, block_hash: str) -> None:
        with self._state_lock:
            self.retry_delays.pop(block_hash, None)

    def mark_attempted(self, block_hash: str) -> None:
        mark_attempted = getattr(
            self.ports.ledger(),
            "mark_block_candidate_attempted",
            None,
        )
        if callable(mark_attempted):
            mark_attempted(block_hash=block_hash)

    def wait_for_retry(self, delay_seconds: float) -> bool:
        """Wait in heartbeat-sized slices while exposing intentional backoff."""
        delay_seconds = max(0.0, float(delay_seconds))
        if delay_seconds <= 0:
            return self.ports.stop_event().is_set()
        started = time.monotonic()
        with self._state_lock:
            self._backoff_started_monotonic = started
            self._backoff_deadline_monotonic = started + delay_seconds
            self._backoff_delay_seconds = delay_seconds
        remaining = delay_seconds
        try:
            while remaining > 0:
                self.ports.record_heartbeat("block_submitter")
                wait_slice = min(
                    remaining,
                    BLOCK_CANDIDATE_RETRY_HEARTBEAT_SLICE_SECONDS,
                )
                if self.ports.stop_event().wait(wait_slice):
                    return True
                remaining = max(0.0, remaining - wait_slice)
            self.ports.record_heartbeat("block_submitter")
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

    def record_deferred(
        self,
        reason: str,
        message: str,
        *,
        worker: str | None,
    ) -> None:
        del worker
        self.outcome.reason = reason
        self.ports.log(
            f"prism coordinator: block candidate deferred reason={reason}: {message}"
        )

    def record_abandoned(
        self,
        reason: str,
        message: str,
        *,
        worker: str | None,
    ) -> None:
        del worker
        if reason in self.retryable_reasons:
            self.record_deferred(reason, message, worker=None)
            return
        self.outcome.reason = reason
        with self._state_lock:
            self.abandoned_counts[reason] = self.abandoned_counts.get(reason, 0) + 1
        self.ports.log(
            f"prism coordinator: block candidate abandoned reason={reason}: {message}"
        )
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


def _compat_get(service: BlockCandidateService, name: str) -> Any:
    return getattr(service, _COMPATIBILITY_FIELD_MAP[name])


def _compat_set(service: BlockCandidateService, name: str, value: Any) -> None:
    setattr(service, _COMPATIBILITY_FIELD_MAP[name], value)

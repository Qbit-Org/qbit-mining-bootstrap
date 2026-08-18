"""PRISM active-chain reconciliation and payout-publication ownership.

The service owns same-tip flight admission, the per-tip trusted memo with its
lookup counters, the bounded reconcile-prefetch executor, and the core
reconcile state machine.  The coordinator retains its serialized cross-owner
adapter (writer admission, payout-balance mutation lock, landed-preview
fail-closed check) and thin public/private delegates; flight coalescing runs
*before* that adapter so same-tip followers never queue behind writer
admission.  Memo, lookup, and outcome state is guarded by the coordinator's
control-plane lock supplied through ``ReorgPorts.state_lock`` so tip
observation can evict memo entries atomically with its own state updates.
"""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from dataclasses import dataclass
import threading
import time
from typing import Any, Callable, Mapping

from lab.prism.coordinator_config import (
    DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
    TESTNET_QBIT_CHAINS,
)
from lab.prism.payout_state import PayoutStateCandidate, TemplateRefreshSuperseded


# Bounded number of tips whose trusted reconcile outcome is memoized. Reorg
# churn moves between a handful of competing tips; eight entries cover deep
# flip-flop churn while keeping eviction scans trivial.
PRISM_REORG_RECONCILE_MEMO_MAX_TIPS = 8
# How long an unflagged same-tip caller waits for an in-flight reconcile pass
# before falling back to its own serialized pass.
DEFAULT_PRISM_RECONCILE_FLIGHT_WAIT_SECONDS = 30.0
# Reconcile demand accounting: which caller asked, and what satisfied it.
# "overlap" is a tip-refresh join that reused the pass prefetched alongside
# its template fetch; "serial" is a full serialized pass of the caller's own.
PRISM_REORG_RECONCILE_LOOKUP_PATHS = ("tip_refresh", "job_build")
PRISM_REORG_RECONCILE_LOOKUP_SOURCES = ("memo_hit", "overlap", "serial")
# Bounded wait when joining the prefetched reconcile pass. Long enough for a
# healthy pass (DB round trips plus a few RPCs), far shorter than the
# publication-progress failure budget, so a stuck pass surfaces as the normal
# blocked-retry path instead of parking the poll loop's liveness heartbeat.
PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_SECONDS = 20.0
# Hard ceiling for the operator override: the join must stay comfortably
# below the template-refresh failure budget or the bound stops bounding.
PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_CEILING_SECONDS = 60.0
# How deeply a pool block must be buried before the stranded-prepared sweep
# will reject it.
#
# A prepared row is owned by the live submit/replay path while its outbox
# entry exists; the sweep exists only for rows that lost that owner, and it
# cannot tell the two apart from the ledger alone. Depth is the proxy. 100
# blocks is far past any legitimate in-flight finalization — a landing that
# is still retrying, an ancestor replay working its way forward, a
# confirmation racing its own audit publication all resolve within a handful
# of blocks — while still healing a genuinely stranded row within roughly
# two hours of block time rather than leaving its payout entries, carry
# forward, and CTV fanout artifacts pinned immature indefinitely.
STRANDED_PREPARED_REJECT_MIN_DEPTH = 100
# One pass's bound on the sweep. Each returned row costs a getblockhash and
# possibly a fenced ledger mutation, and the pass already walks the reorg
# watch set; the remainder is picked up by the next pass.
STRANDED_PREPARED_SWEEP_LIMIT = 64


def _no_reconcile_progress(phase: str) -> None:
    """Discard reconcile progress stamps for ports built without a recorder."""
    return None


class _ReconcileFlight:
    """One in-flight reconcile pass shared by concurrent same-tip callers."""

    __slots__ = ("event", "summary", "exception")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.summary: dict[str, object] | None = None
        self.exception: BaseException | None = None


@dataclass(frozen=True)
class ReorgPorts:
    """Dynamic infrastructure required by active-chain reconciliation.

    Callables that reach documented coordinator monkeypatch seams
    (``reconcile_with_admission``, ``reconcile_serialized``, ``ensure_tip``,
    ``chain_view_untrusted``) resolve the coordinator method at call time so
    per-instance patches intercept every internal path.
    """

    rpc_call: Callable[..., object]
    ledger: Callable[[], Any]
    ensure_job_cache_state: Callable[[], None]
    state_lock: Callable[[], AbstractContextManager[object]]
    source_tip: Callable[[], str | None]
    reserve_external_tip: Callable[[str], None]
    max_supersession_retries: Callable[[], int]
    prepare_lock: Callable[[], AbstractContextManager[object]]
    capture_source: Callable[[], tuple[int, int, str | None, str, float]]
    prepared_candidate: Callable[..., PayoutStateCandidate]
    captured_publication_required: Callable[
        [tuple[int, int, str | None, str, float]],
        bool,
    ]
    block_publication: Callable[..., None]
    publication_guard: Callable[[], AbstractContextManager[object]]
    publish_candidate: Callable[[PayoutStateCandidate], int | None]
    observe_preparation: Callable[[float], None]
    chain_view_untrusted: Callable[[], bool]
    reorg_proof_snapshot: Callable[[], tuple[str | None, int]]
    flight_wait_seconds: Callable[[], float]
    prefetch_join_timeout_seconds: Callable[[], float]
    reconcile_with_admission: Callable[..., Mapping[str, object]]
    reconcile_serialized: Callable[..., dict[str, object]]
    ensure_tip: Callable[[str], bool]
    # Liveness stamp for a reconcile pass running inline on a
    # watchdog-monitored thread. An accepted-block landing calls
    # ``ensure_tip`` on the block-work thread, and one pass is an unbounded
    # chain walk -- a getblockhash plus up to two ledger statements per
    # watched block -- so bracketing the call from outside would still leave
    # a silence proportional to the number of watched blocks. The coordinator
    # supplies its block-work phase recorder, which no-ops on every other
    # thread; ports built without one get a sink so background reconciliation
    # and focused tests are unchanged.
    record_progress: Callable[[str], None] = _no_reconcile_progress


@dataclass(frozen=True)
class ReorgState:
    """Copied bookkeeping/last-outcome snapshot for observers."""

    inactive_block_count: int
    reactivated_block_count: int
    reconcile_skip_count: int
    reconcile_error_count: int
    matured_payout_count: int
    last_tip_hash: str | None
    last_trusted: bool
    last_monotonic: float | None


def qbit_chain_view_untrusted(
    rpc_call: Callable[..., object],
    chain: str,
) -> bool:
    """Return whether the node cannot provide a coherent validated tip."""

    blockchain_info = rpc_call("getblockchaininfo")
    if not isinstance(blockchain_info, dict):
        raise RuntimeError("getblockchaininfo returned non-object")
    public_chain = chain.lower() in {
        "main",
        "mainnet",
        *TESTNET_QBIT_CHAINS,
    }
    if (
        blockchain_info.get("initialblockdownload") is not False
        if public_chain
        else bool(blockchain_info.get("initialblockdownload"))
    ):
        return True
    blocks_raw = blockchain_info.get("blocks")
    headers_raw = blockchain_info.get("headers")
    if public_chain and (blocks_raw is None or headers_raw is None):
        return True
    if blocks_raw is not None and headers_raw is not None:
        try:
            blocks = int(blocks_raw)
            headers = int(headers_raw)
            if blocks < 0 or headers < 0 or headers != blocks:
                return True
        except (TypeError, ValueError) as exc:
            raise RuntimeError("getblockchaininfo blocks/headers are not integers") from exc
    return False


class ReorgReconcilerService:
    """Own flights, trusted memo/lookup state, prefetch, and the core pass."""

    def __init__(
        self,
        ports: ReorgPorts,
        *,
        enabled: bool = True,
        cache_seconds: float = DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
        inactive_block_count: int = 0,
        reactivated_block_count: int = 0,
        reconcile_skip_count: int = 0,
        reconcile_error_count: int = 0,
        matured_payout_count: int = 0,
        last_tip_hash: str | None = None,
        last_trusted: bool = False,
        last_monotonic: float | None = None,
    ) -> None:
        self._ports = ports
        self.enabled = bool(enabled)
        self.cache_seconds = cache_seconds
        self.inactive_block_count = int(inactive_block_count)
        self.reactivated_block_count = int(reactivated_block_count)
        self.reconcile_skip_count = int(reconcile_skip_count)
        self.reconcile_error_count = int(reconcile_error_count)
        self.matured_payout_count = int(matured_payout_count)
        self.last_tip_hash = last_tip_hash
        self.last_trusted = bool(last_trusted)
        self.last_monotonic = last_monotonic
        self._reconcile_flight_lock = threading.Lock()
        # In-flight reconcile passes keyed by tip hash. Unflagged callers
        # for a tip already being reconciled await that pass's summary
        # instead of queueing a redundant serialized pass of their own.
        self._reconcile_flights: dict[str, _ReconcileFlight] = {}
        # Monotonic completion time of the last trusted reconcile pass,
        # per tip hash, guarded by the coordinator state lock. Entries are
        # unarmed only by an untrusted outcome for their own tip (or a
        # reconcile error, which clears the whole map); see note_outcome.
        self._reorg_reconcile_trusted_memo: OrderedDict[str, float] = (
            OrderedDict()
        )
        self._reconcile_prefetch_executor_lock = threading.Lock()
        # Single-worker lane that overlaps the tip-refresh reconcile pass
        # with the template fetch. Only the refresh singleflight owner
        # submits, so at most one prefetched pass is in flight; same-tip
        # followers still coalesce through _reconcile_flights.
        self._reconcile_prefetch_executor: ThreadPoolExecutor | None = None
        self._reconcile_prefetch_executor_shutdown = False
        # At most one outstanding prefetch, keyed by tip, guarded by
        # _reconcile_prefetch_executor_lock. Failed refresh attempts
        # (for example a template-RPC outage) reuse it instead of
        # queueing another serialized pass per retry.
        self._reconcile_prefetch_pending: (
            tuple[str, Future[bool], bool] | None
        ) = None
        # Reconcile demand by (caller path, satisfying source), guarded by
        # the coordinator state lock.
        self.reorg_reconcile_lookup_counts = {
            (path, source): 0
            for path in PRISM_REORG_RECONCILE_LOOKUP_PATHS
            for source in PRISM_REORG_RECONCILE_LOOKUP_SOURCES
        }

    def snapshot(self) -> ReorgState:
        with self._ports.state_lock():
            return ReorgState(
                inactive_block_count=self.inactive_block_count,
                reactivated_block_count=self.reactivated_block_count,
                reconcile_skip_count=self.reconcile_skip_count,
                reconcile_error_count=self.reconcile_error_count,
                matured_payout_count=self.matured_payout_count,
                last_tip_hash=self.last_tip_hash,
                last_trusted=self.last_trusted,
                last_monotonic=self.last_monotonic,
            )

    def reorg_reconcile_lookup_snapshot(self) -> dict[tuple[str, str], int]:
        """Copied lookup counters for the metrics renderer."""
        with self._ports.state_lock():
            return dict(self.reorg_reconcile_lookup_counts)

    def note_outcome(
        self,
        tip_hash: str | None,
        *,
        trusted: bool,
        clear_memo: bool = False,
        evict_others: bool = False,
        proof_epoch: int | None = None,
    ) -> None:
        """Record a reconcile outcome in the per-tip trusted memo.

        A trusted pass arms the memo for its own tip. An untrusted outcome
        (superseded publication, untrusted chain view) unarms only its own
        tip: it proves nothing about reconciliations already completed for
        other tips, and unarming them globally forces every job build into a
        redundant full pass. A pass that applied orphan/maturity row
        mutations passes evict_others=True: cached proofs for other tips
        were taken against pre-mutation rows and no longer hold, even if the
        chain later flips back before any tip observation lands. A reconcile
        error passes clear_memo=True; a partially applied ledger mutation
        invalidates every cached outcome. ``proof_epoch`` carries the
        tip-detection epoch the pass started its reads in: arming is refused
        when the epoch moved during the pass, so a flip away and back can
        never re-arm an entry with a proof from the closed epoch (the
        latest-detected-hash guard alone cannot see the round trip).
        """

        self._ports.ensure_job_cache_state()
        now = time.monotonic()
        with self._ports.state_lock():
            self.last_tip_hash = tip_hash
            self.last_trusted = trusted
            self.last_monotonic = now
            memo = self._reorg_reconcile_trusted_memo
            if clear_memo:
                memo.clear()
                return
            if evict_others:
                for cached_tip in list(memo):
                    if cached_tip != tip_hash:
                        del memo[cached_tip]
            if tip_hash is None:
                return
            if trusted:
                latest_tip, detection_epoch = (
                    self._ports.reorg_proof_snapshot()
                )
                if latest_tip is not None and latest_tip != tip_hash:
                    # This pass finished for a tip that is no longer the
                    # newest detected one; its epoch is over. Arming would
                    # re-add an entry the newer observation already evicted
                    # and let a flip-back reuse a pre-flip outcome.
                    return
                if proof_epoch is not None and proof_epoch != detection_epoch:
                    # The pass spanned a detection cycle; every memo
                    # consumer (refresh joins, initial-job and vardiff-idle
                    # builds) must see a full re-proof instead.
                    return
                memo[tip_hash] = now
                memo.move_to_end(tip_hash)
                while len(memo) > PRISM_REORG_RECONCILE_MEMO_MAX_TIPS:
                    memo.popitem(last=False)
            else:
                memo.pop(tip_hash, None)

    def evict_memo_for_new_tip_locked(self, tip_hash: str) -> None:
        """Drop trusted-reconcile entries for every tip except ``tip_hash``.

        Called under the coordinator state lock when a newer tip is
        detected. A detected flip ends the epoch of every previously cached
        outcome: if the chain later flips back to an earlier hash within the
        cache TTL, pool-block chain state must be re-proven by a fresh pass,
        not assumed from a pre-flip reconciliation. Detection is
        observation-sequenced, so only genuinely newer observations evict.
        """

        self._ports.ensure_job_cache_state()
        memo = self._reorg_reconcile_trusted_memo
        for cached_tip in list(memo):
            if cached_tip != tip_hash:
                del memo[cached_tip]

    def ensure_current(
        self,
        *,
        expected_tip_hash: str | None = None,
    ) -> bool:
        if not self.enabled and expected_tip_hash is None:
            return True
        current_tip = str(self._ports.rpc_call("getbestblockhash"))
        if expected_tip_hash is not None and current_tip != expected_tip_hash:
            raise TemplateRefreshSuperseded(
                "qbit tip changed while prepared work was queued "
                f"expected={expected_tip_hash} current={current_tip}"
            )
        if not self.enabled:
            return True
        if self.memo_fresh(current_tip):
            self.record_lookup("job_build", "memo_hit")
            return True
        self.record_lookup("job_build", "serial")
        return self._ports.ensure_tip(current_tip)

    def memo_fresh(self, tip_hash: str) -> bool:
        """True when a trusted pass for ``tip_hash`` is inside the cache TTL
        and the live chain view is still trusted.

        A fresh memo entry lets a caller reuse the completed pass instead of
        queueing a redundant serialized one. The memo is per tip: an
        untrusted outcome recorded for another tip never unarms this one.
        The chain-view trust check is NOT cached: headers can run ahead of
        the validated tip without the best block hash changing (an arriving
        reorg), and job issuance must pause immediately, not a TTL later.
        """
        ttl = self.cache_seconds
        if ttl <= 0:
            return False
        self._ports.ensure_job_cache_state()
        with self._ports.state_lock():
            reconciled_monotonic = self._reorg_reconcile_trusted_memo.get(
                tip_hash
            )
        return bool(
            reconciled_monotonic is not None
            and time.monotonic() - reconciled_monotonic <= ttl
            and not self._ports.chain_view_untrusted()
        )

    def record_lookup(self, path: str, source: str) -> None:
        if path not in PRISM_REORG_RECONCILE_LOOKUP_PATHS:
            raise ValueError(f"unknown reorg reconcile lookup path: {path}")
        if source not in PRISM_REORG_RECONCILE_LOOKUP_SOURCES:
            raise ValueError(f"unknown reorg reconcile lookup source: {source}")
        self._ports.ensure_job_cache_state()
        with self._ports.state_lock():
            self.reorg_reconcile_lookup_counts[(path, source)] += 1

    def prefetch_pass(
        self,
        tip_hash: str,
        prove: bool = False,
    ) -> bool:
        """One prefetched reconcile, honoring the memo like the join does.

        A prefetch that queued behind a completed same-tip pass (abandoned
        refresh attempts reuse the slot, but a replaced tip can leave one
        queued) would otherwise re-run the full serialized pass for nothing.
        A proving pass (the serial re-prove branches) bypasses the memo:
        those branches exist precisely because the entry cannot be trusted.
        """
        if not prove and self.memo_fresh(tip_hash):
            return True
        return self._ports.ensure_tip(tip_hash)

    def submit_prefetch(
        self,
        tip_hash: str,
        *,
        prove: bool = False,
    ) -> Future[bool] | None:
        """Run one reconcile pass on the prefetch worker so it overlaps the
        caller's template fetch.

        Returns ``None`` once shutdown has retired the executor; the caller
        falls back to its serial pass. At most one prefetch is outstanding:
        a refresh attempt that failed before its join (for example a
        template-RPC outage) leaves its future in the slot, and the retry
        reuses it for the same tip instead of queueing another serialized
        pass behind the first.
        """
        self._ports.ensure_job_cache_state()
        stale_future: Future[bool] | None = None
        future: Future[bool] | None = None
        with self._reconcile_prefetch_executor_lock:
            if self._reconcile_prefetch_executor_shutdown:
                return None
            pending = self._reconcile_prefetch_pending
            if pending is not None:
                pending_tip, pending_future, pending_proves = pending
                if (
                    not pending_future.done()
                    and pending_tip == tip_hash
                    and (pending_proves or not prove)
                ):
                    # A proving pass satisfies both kinds of caller; a
                    # memo-honoring pass cannot satisfy a prove request and
                    # is replaced below like a tip change.
                    return pending_future
                # Replaced tip or completed future: hand the old future off
                # for disposal outside this lock -- cancellation runs done
                # callbacks inline on this thread, and _clear_slot below
                # re-takes the lock.
                self._reconcile_prefetch_pending = None
                if not pending_future.done():
                    stale_future = pending_future
            executor = self._reconcile_prefetch_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="prism-reconcile-prefetch",
                )
                self._reconcile_prefetch_executor = executor
            try:
                future = executor.submit(
                    self.prefetch_pass, tip_hash, prove
                )
                self._reconcile_prefetch_pending = (tip_hash, future, prove)
            except RuntimeError:
                # Executor shutdown raced this submit; the serial path
                # covers it (after the stale future is disposed below).
                future = None
        if stale_future is not None and not stale_future.cancel():
            # Already running for a replaced tip; let it finish detached.
            # The slot holds the new tip, so at most one task ever waits
            # behind the running one.
            self.discard_stale_prefetch(stale_future)
        if future is None:
            return None

        def _clear_slot(done: Future[bool]) -> None:
            with self._reconcile_prefetch_executor_lock:
                pending_now = self._reconcile_prefetch_pending
                if pending_now is not None and pending_now[1] is done:
                    self._reconcile_prefetch_pending = None

        # Registered outside the slot lock: a completed future runs the
        # callback inline on this thread.
        future.add_done_callback(_clear_slot)
        return future

    @staticmethod
    def discard_stale_prefetch(future: Future[bool]) -> None:
        """Detach a prefetched pass whose tip was superseded during the
        template fetch.

        The pass cannot be cancelled mid-ledger-mutation and already records
        its own outcome/error accounting; the serial pass for the current
        tip re-surfaces any condition that still applies. Consuming the
        result here only prevents an unretrieved-exception warning.
        """

        def _consume(done: Future[bool]) -> None:
            try:
                done.result()
            except BaseException:
                pass

        future.add_done_callback(_consume)

    def join_prefetch_bounded(self, prefetch: Future[bool]) -> bool:
        """Join a reconcile pass under the poll loop's bounded budget.

        On a genuine expiry the pass keeps running in its single prefetch
        slot -- the paced retry re-joins the same future -- and the timeout
        surfaces the normal blocked-retry path, keeping the poll loop's
        liveness heartbeat fed while the pass catches up. The budget is
        clamped below the loop's failure budget so a misconfigured override
        cannot reinstate the park this bound exists to prevent.
        """

        join_timeout = min(
            PRISM_RECONCILE_PREFETCH_JOIN_TIMEOUT_CEILING_SECONDS,
            max(
                0.001,
                float(self._ports.prefetch_join_timeout_seconds()),
            ),
        )
        try:
            return prefetch.result(timeout=join_timeout)
        except TimeoutError:
            if prefetch.done():
                # The pass itself raised TimeoutError (socket.timeout is
                # TimeoutError here): not a join expiry. Propagate silently
                # so diagnosis points at the pass, not the join.
                raise
            print(
                "prism coordinator: reconcile prefetch join exceeded "
                f"{join_timeout:g}s; retrying refresh pass while it "
                "completes",
                flush=True,
            )
            raise

    def snapshot_tip_bounded(self, tip_hash: str) -> bool:
        """Run a snapshot-tip re-prove off-thread with the bounded join.

        The serial re-prove branches run the same crawling pass as the
        overlapped prefetch; routing them through the prefetch slot gives
        the poll loop one uniform bounded wait. Falls back to the direct
        pass only when the prefetch executor has already been retired at
        shutdown, whose exceptions the caller already maps to a clean exit.
        """

        prefetch = self.submit_prefetch(tip_hash, prove=True)
        if prefetch is None:
            return self._ports.ensure_tip(tip_hash)
        return self.join_prefetch_bounded(prefetch)

    def shutdown_prefetch_executor(self) -> None:
        self._ports.ensure_job_cache_state()
        with self._reconcile_prefetch_executor_lock:
            executor = self._reconcile_prefetch_executor
            self._reconcile_prefetch_executor = None
            self._reconcile_prefetch_executor_shutdown = True
            self._reconcile_prefetch_pending = None
        if executor is not None:
            # A pass blocked on writer admission aborts via
            # ShutdownInProgress on its own; never hold shutdown for it.
            executor.shutdown(wait=False, cancel_futures=True)

    def ensure_tip(
        self,
        tip_hash: str,
        *,
        _coalesce_same_tip: bool = True,
    ) -> bool:
        """Reconcile one tip, optionally bypassing same-tip flight reuse.

        Lock-owning accepted-block callers disable waiting for an existing
        leader because it may itself be waiting for the payout-balance
        mutation lock. Their own pass remains visible to ordinary followers.
        """
        if not self.enabled:
            return True
        if _coalesce_same_tip:
            summary = self._ports.reconcile_with_admission(tip_hash=tip_hash)
        else:
            summary = self._ports.reconcile_with_admission(
                tip_hash=tip_hash,
                _wait_for_same_tip_flight=False,
            )
        return not bool(summary.get("untrusted") or summary.get("superseded"))

    def reconcile_with_flights(
        self,
        *,
        tip_hash: str | None = None,
        _force_publish: bool = False,
        _source_reserved: bool = False,
        _wait_for_same_tip_flight: bool = True,
    ) -> dict[str, object]:
        """Reconcile pool blocks, coalescing same-tip concurrent callers.

        Unflagged callers asking about a tip whose pass is already in flight
        await that pass and share its summary instead of queueing another
        full serialized pass. Callers carrying side-effect obligations (a
        forced publication or an already-reserved source) and callers without
        a tip always run their own pass. Lock-owning callers may disable
        waiting for an existing flight while still registering as the visible
        leader when no same-tip flight exists.
        """
        self._ports.ensure_job_cache_state()
        if tip_hash is None or _force_publish or _source_reserved:
            return self._ports.reconcile_serialized(
                tip_hash=tip_hash,
                _force_publish=_force_publish,
                _source_reserved=_source_reserved,
            )
        with self._reconcile_flight_lock:
            flight = self._reconcile_flights.get(tip_hash)
            leading = flight is None
            if leading:
                flight = _ReconcileFlight()
                self._reconcile_flights[tip_hash] = flight
        if not leading:
            if not _wait_for_same_tip_flight:
                return self._ports.reconcile_serialized(
                    tip_hash=tip_hash
                )
            wait_seconds = float(self._ports.flight_wait_seconds())
            if flight.event.wait(timeout=wait_seconds):
                exception = flight.exception
                if exception is not None:
                    raise exception
                summary = flight.summary
                assert summary is not None
                # Followers get a copy: summaries are mutable dicts and the
                # leader's caller already holds the original.
                return dict(summary)
            # Liveness backstop: the leader outlived the wait. Run our own
            # pass; the writer lock still serializes the actual work.
            return self._ports.reconcile_serialized(
                tip_hash=tip_hash
            )
        try:
            summary = self._ports.reconcile_serialized(
                tip_hash=tip_hash
            )
            flight.summary = summary
            return summary
        except BaseException as exc:
            flight.exception = exc
            raise
        finally:
            with self._reconcile_flight_lock:
                self._reconcile_flights.pop(tip_hash, None)
            flight.event.set()

    def reconcile(
        self,
        *,
        tip_hash: str | None = None,
        force_publish: bool = False,
        source_reserved: bool = False,
    ) -> dict[str, object]:
        """The core reconcile state machine (behind the serialized adapter)."""
        summary: dict[str, object] = {
            "enabled": bool(self.enabled),
            "untrusted": False,
            "superseded": False,
            "published_generation": None,
            "watched_blocks": 0,
            "inactive_blocks": 0,
            "reactivated_blocks": 0,
            "matured_payouts": 0,
            "stranded_prepared_rejected": 0,
            "stranded_prepared_canonical": 0,
        }
        if not self.enabled:
            return summary
        self._ports.ensure_job_cache_state()
        if not source_reserved and tip_hash is not None:
            # Tip observation normally reserves this source before queueing
            # reconciliation. Direct callers only need a new source when they
            # are asking about a different tip; repeated reconciliation of the
            # same tip must not supersede otherwise valid prepared work.
            current_source_tip = self._ports.source_tip()
            if current_source_tip != tip_hash:
                self._ports.reserve_external_tip(tip_hash)

        inactive_blocks_total = 0
        reactivated_blocks_total = 0
        matured_payouts_total = 0
        stranded_prepared_rejected_total = 0
        stranded_prepared_canonical_total = 0
        supersession_retries = 0
        skip_recorded = False
        max_supersession_retries = max(
            0,
            int(self._ports.max_supersession_retries()),
        )

        proof_epoch = self._ports.reorg_proof_snapshot()[1]

        def finish(*, trusted: bool) -> dict[str, object]:
            with self._ports.state_lock():
                self.inactive_block_count += inactive_blocks_total
                self.reactivated_block_count += reactivated_blocks_total
                self.matured_payout_count += matured_payouts_total
            self.note_outcome(
                tip_hash,
                trusted=trusted,
                # Row mutations invalidate proofs cached for other tips even
                # when the mutating pass's tip was never observed (per-client
                # callers reconcile straight off getbestblockhash).
                evict_others=bool(
                    inactive_blocks_total
                    or reactivated_blocks_total
                    or matured_payouts_total
                    or stranded_prepared_rejected_total
                ),
                proof_epoch=proof_epoch,
            )
            summary["inactive_blocks"] = inactive_blocks_total
            summary["reactivated_blocks"] = reactivated_blocks_total
            summary["matured_payouts"] = matured_payouts_total
            summary["stranded_prepared_rejected"] = (
                stranded_prepared_rejected_total
            )
            summary["stranded_prepared_canonical"] = (
                stranded_prepared_canonical_total
            )
            return summary

        def retry_superseded_candidate() -> bool:
            nonlocal supersession_retries, tip_hash
            supersession_retries += 1
            if supersession_retries > max_supersession_retries:
                summary["superseded"] = True
                self._ports.block_publication()
                return False
            latest_tip = self._ports.source_tip()
            tip_hash = latest_tip or tip_hash
            return True

        while True:
            candidate_to_publish: PayoutStateCandidate | None = None
            error_candidate: PayoutStateCandidate | None = None
            attempt_trusted = True
            # The memo entry this attempt may arm must prove state for the
            # epoch its reads happen in; a detection cycle during the pass
            # (away, or away and back) refuses the arm in note_outcome.
            proof_epoch = self._ports.reorg_proof_snapshot()[1]
            try:
                with self._ports.prepare_lock():
                    prepared_started = time.monotonic()
                    captured_source = self._ports.capture_source()
                    payout_changed = False
                    payout_mutation_attempted = False
                    inactive_blocks = 0
                    reactivated_blocks = 0
                    matured_payouts = 0
                    stranded_prepared_rejected = 0
                    stranded_prepared_canonical = 0
                    summary["untrusted"] = False
                    summary["watched_blocks"] = 0
                    try:
                        if self._ports.chain_view_untrusted():
                            if not skip_recorded:
                                with self._ports.state_lock():
                                    self.reconcile_skip_count += 1
                                skip_recorded = True
                            summary["untrusted"] = True
                            attempt_trusted = False
                            if force_publish:
                                candidate_to_publish = (
                                    self._ports.prepared_candidate(
                                        captured_source
                                    )
                                )
                        else:
                            # Every stamp below marks one completed round
                            # trip. A landing thread's watchdog budget is
                            # sized for a single statement, and this pass
                            # crosses an unbounded number of them, so the
                            # stamps have to follow the work itself rather
                            # than bracket the pass as a whole.
                            self._ports.record_progress(
                                "reorg-reconcile:tip-height"
                            )
                            active_tip_height = int(
                                self._ports.rpc_call("getblockcount")
                            )
                            ledger = self._ports.ledger()
                            watch_blocks = getattr(
                                ledger,
                                "reorg_watch_blocks",
                                None,
                            )
                            if not callable(watch_blocks):
                                if (
                                    force_publish
                                    or self._ports.captured_publication_required(
                                        captured_source
                                    )
                                ):
                                    candidate_to_publish = (
                                        self._ports.prepared_candidate(
                                            captured_source,
                                            force_full_window_rescan=payout_changed,
                                        )
                                    )
                            else:
                                self._ports.record_progress(
                                    "reorg-reconcile:watch-blocks"
                                )
                                rows = watch_blocks(
                                    active_tip_height=active_tip_height
                                )
                                summary["watched_blocks"] = len(rows)

                                for row in rows:
                                    # Per row, not per pass: the row count is
                                    # bounded only by how many pool blocks the
                                    # reorg window holds, and each iteration
                                    # can spend a getblockhash plus a ledger
                                    # mutation.
                                    self._ports.record_progress(
                                        "reorg-reconcile:watch-block"
                                    )
                                    block_height = int(row["block_height"])
                                    block_hash = str(row["block_hash"]).lower()
                                    chain_state = str(row.get("chain_state", ""))
                                    if block_height > active_tip_height:
                                        if chain_state == "confirmed":
                                            payout_mutation_attempted = True
                                            inactive = (
                                                ledger.mark_pool_block_inactive(
                                                    block_hash=block_hash,
                                                    active_tip_height=active_tip_height,
                                                )
                                            )
                                            inactive_count = int(
                                                inactive.get("inactive_count", 0)
                                            )
                                            inactive_blocks += inactive_count
                                            payout_changed = (
                                                payout_changed
                                                or bool(inactive_count)
                                            )
                                        continue
                                    active_hash = str(
                                        self._ports.rpc_call(
                                            "getblockhash",
                                            [block_height],
                                        )
                                    ).lower()
                                    on_active_chain = active_hash == block_hash
                                    if (
                                        on_active_chain
                                        and chain_state == "inactive"
                                    ):
                                        payout_mutation_attempted = True
                                        with self._ports.publication_guard():
                                            reactivated = ledger.reactivate_pool_block(
                                                block_hash=block_hash,
                                                active_tip_height=active_tip_height,
                                            )
                                        reactivated_count = int(
                                            reactivated.get(
                                                "reactivated_count",
                                                0,
                                            )
                                        )
                                        reactivated_blocks += reactivated_count
                                        payout_changed = (
                                            payout_changed
                                            or bool(reactivated_count)
                                        )
                                    elif (
                                        not on_active_chain
                                        and chain_state == "confirmed"
                                    ):
                                        payout_mutation_attempted = True
                                        inactive = (
                                            ledger.mark_pool_block_inactive(
                                                block_hash=block_hash,
                                                active_tip_height=active_tip_height,
                                            )
                                        )
                                        inactive_count = int(
                                            inactive.get("inactive_count", 0)
                                        )
                                        inactive_blocks += inactive_count
                                        payout_changed = (
                                            payout_changed
                                            or bool(inactive_count)
                                        )

                                # Rows the watch loop above structurally
                                # cannot see: it selects confirmed/inactive
                                # only, because a prepared row belongs to the
                                # live submit/replay path that holds its
                                # outbox entry. When that entry is gone the
                                # row has no owner left, and an orphaned one
                                # pins its payout entries, carry forward, and
                                # CTV fanout artifacts immature forever.
                                stranded_prepared = getattr(
                                    ledger,
                                    "stranded_prepared_blocks",
                                    None,
                                )
                                if callable(stranded_prepared):
                                    self._ports.record_progress(
                                        "reorg-reconcile:stranded-prepared-blocks"
                                    )
                                    stranded_rows = stranded_prepared(
                                        active_tip_height=active_tip_height,
                                        min_depth=(
                                            STRANDED_PREPARED_REJECT_MIN_DEPTH
                                        ),
                                        limit=STRANDED_PREPARED_SWEEP_LIMIT,
                                    )
                                    for row in stranded_rows:
                                        # Per row for the same reason as the
                                        # watch loop: a getblockhash plus a
                                        # fenced ledger mutation each.
                                        self._ports.record_progress(
                                            "reorg-reconcile:stranded-prepared"
                                        )
                                        block_height = int(row["block_height"])
                                        block_hash = str(
                                            row["block_hash"]
                                        ).lower()
                                        active_hash = str(
                                            self._ports.rpc_call(
                                                "getblockhash",
                                                [block_height],
                                            )
                                        ).lower()
                                        if active_hash == block_hash:
                                            # Canonical but never confirmed:
                                            # the confirm path owns audit
                                            # publication sequencing, so this
                                            # sweep must never take the row
                                            # from under it. Report loudly and
                                            # leave it alone.
                                            stranded_prepared_canonical += 1
                                            print(
                                                "prism coordinator: stranded "
                                                "prepared pool block is "
                                                "canonical and needs operator "
                                                "review "
                                                f"hash={block_hash} "
                                                f"height={block_height} "
                                                "depth="
                                                f"{active_tip_height - block_height}",
                                                flush=True,
                                            )
                                            continue
                                        payout_mutation_attempted = True
                                        rejected = ledger.reject_prepared_block(
                                            block_hash=block_hash,
                                            active_tip_height=active_tip_height,
                                        )
                                        # The fenced ledger function cascades
                                        # payout entries, carry forward, and
                                        # fanout artifacts, and returns 0 when
                                        # the row already moved under us.
                                        rejected_count = int(
                                            rejected.get("rejected_count", 0)
                                        )
                                        stranded_prepared_rejected += (
                                            rejected_count
                                        )
                                        payout_changed = (
                                            payout_changed
                                            or bool(rejected_count)
                                        )
                                        if rejected_count:
                                            print(
                                                "prism coordinator: rejected "
                                                "orphaned stranded prepared "
                                                "pool block "
                                                f"hash={block_hash} "
                                                f"height={block_height} "
                                                "depth="
                                                f"{active_tip_height - block_height}",
                                                flush=True,
                                            )

                                mark_mature = getattr(
                                    ledger,
                                    "mark_mature_pool_payouts",
                                    None,
                                )
                                if callable(mark_mature):
                                    payout_mutation_attempted = True
                                    self._ports.record_progress(
                                        "reorg-reconcile:mature-payouts"
                                    )
                                    matured = mark_mature(
                                        active_tip_height=active_tip_height
                                    )
                                    matured_payouts = int(
                                        matured.get("matured_count", 0)
                                    )
                                    payout_changed = (
                                        payout_changed
                                        or bool(matured_payouts)
                                    )

                                inactive_blocks_total += inactive_blocks
                                reactivated_blocks_total += reactivated_blocks
                                matured_payouts_total += matured_payouts
                                stranded_prepared_rejected_total += (
                                    stranded_prepared_rejected
                                )
                                stranded_prepared_canonical_total += (
                                    stranded_prepared_canonical
                                )
                                if (
                                    payout_changed
                                    or force_publish
                                    or self._ports.captured_publication_required(
                                        captured_source
                                    )
                                ):
                                    # Candidate preparation embeds the ledger
                                    # snapshot artifact; a pass that will not
                                    # publish must not pay for one only to
                                    # discard it.
                                    self._ports.record_progress(
                                        "reorg-reconcile:prepare-candidate"
                                    )
                                    candidate_to_publish = (
                                        self._ports.prepared_candidate(
                                            captured_source,
                                            force_full_window_rescan=payout_changed,
                                        )
                                    )
                    except Exception:
                        inactive_blocks_total += inactive_blocks
                        reactivated_blocks_total += reactivated_blocks
                        matured_payouts_total += matured_payouts
                        stranded_prepared_rejected_total += (
                            stranded_prepared_rejected
                        )
                        stranded_prepared_canonical_total += (
                            stranded_prepared_canonical
                        )
                        # Durable partial mutations close admission before the
                        # preparation lock is released. Publication drains old
                        # socket sends afterward without blocking new ledger
                        # preparation or snapshot acquisition.
                        if payout_mutation_attempted:
                            # A mutator can commit server-side and still raise
                            # locally when its response is lost. The observed
                            # row counts are then unavailable, so conservatively
                            # force the same ledger re-read as a confirmed
                            # mutation. Read-only failures never reach this flag.
                            payout_changed = True
                        if payout_changed:
                            error_candidate = (
                                self._ports.prepared_candidate(
                                    captured_source,
                                    force_full_window_rescan=True,
                                )
                            )
                            self._ports.block_publication(force=True)
                        with self._ports.state_lock():
                            self.inactive_block_count += (
                                inactive_blocks_total
                            )
                            self.reactivated_block_count += (
                                reactivated_blocks_total
                            )
                            self.matured_payout_count += matured_payouts_total
                            self.reconcile_error_count += 1
                        # A pass that errored mid-mutation invalidates every
                        # cached outcome, not just its own tip's.
                        self.note_outcome(
                            tip_hash,
                            trusted=False,
                            clear_memo=True,
                        )
                        raise
                    finally:
                        self._ports.observe_preparation(
                            max(0.0, time.monotonic() - prepared_started)
                        )

                    if candidate_to_publish is not None:
                        # Atomically fence cache/build/delivery admission before
                        # releasing the ledger snapshot lock. The potentially
                        # slow drain then happens in publication() below.
                        self._ports.block_publication(force=True)
            except Exception:
                if error_candidate is not None:
                    if (
                        self._ports.publish_candidate(error_candidate)
                        is None
                    ):
                        self._ports.block_publication()
                raise

            if candidate_to_publish is not None:
                self._ports.record_progress("reorg-reconcile:publish")
                published = self._ports.publish_candidate(
                    candidate_to_publish
                )
                if published is None:
                    # Preserve durable counts and retry iteratively against the
                    # newest source. The explicit budget prevents tip churn
                    # from monopolizing preparation indefinitely; the fence
                    # stays closed between attempts.
                    if retry_superseded_candidate():
                        continue
                    return finish(trusted=False)
                summary["published_generation"] = published
            return finish(trusted=attempt_trusted)


class ReorgCompatibilityField:
    """Route retained coordinator fields to their single service owner."""

    def __init__(self, name: str, default: object) -> None:
        self.name = name
        self.default = default
        self.backing = f"_reorg_compat_{name}"

    def __get__(self, instance: Any, owner: type[Any]) -> object:
        if instance is None:
            return self
        service = instance.__dict__.get("_reorg_reconciler_service")
        if service is not None:
            return getattr(service, self.name)
        return instance.__dict__.get(self.backing, self.default)

    def __set__(self, instance: Any, value: object) -> None:
        service = instance.__dict__.get("_reorg_reconciler_service")
        if service is not None:
            setattr(service, self.name, value)
            return
        instance.__dict__[self.backing] = value

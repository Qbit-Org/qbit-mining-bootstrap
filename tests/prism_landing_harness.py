#!/usr/bin/env python3
"""Deterministic harness for PRISM's block candidate landing state machine.

Issue #128 ranks landing first by incident history, and its shape is the
reason: a found block is real money, and the landing is the one owner where
two threads of the *same* process reach the same durable rows. The lease
harness in ``prism_concurrency_harness`` interleaves two coordinators around
one row; this one interleaves the two tails one coordinator lands on.

The two tails
-------------
``lab/prism/block_candidates.py`` runs landings down two routes:

``_submit_synchronous_block_candidate``
    The miner connection thread. A share that is block-worthy at the
    assigned target lands inline on the client's own thread, which is
    normal while the listener floor sits above network difficulty.

``block_accounting_loop`` / ``_run_block_accounting_task``
    The dedicated accounting actor, which lands everything else.

The disposition lease that serialises them is **per hash**
(``block_candidates.py``), so it provides no mutual exclusion between two
*distinct* found blocks. That is the topology #133 turns on, and expressing
it is what this harness exists for.

What is real here and what is modelled
--------------------------------------
Real: ``BlockCandidateService``, ``BlockFinalizationService``,
``AuditArtifactStore`` over a genuine audit root, ``PsqlShareLedger``
emitting its shipped SQL, and the coordinator's own landing arithmetic —
budgets, watchdog ceiling, epoch fences, publication ordinal handling.

Modelled: PostgreSQL (``FakePostgres``, extended with ``qbit_pool_blocks``
and ``qbit_block_candidate_outbox``), qbitd (``LandingRpc``), the audit
bundle build and its verification, and every lock the two tails share —
installed as harness locks so a contended acquire is an interleaving point
rather than a wedged baton.

The audit root is a real directory on purpose. What #133 loses is a *file*
that is never written, and no in-memory double can fail to write it.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.prism_concurrency_harness import (  # noqa: E402
    BATON_TIMEOUT_SECONDS,
    Actor,
    Call,
    FakeLeaseGuard,
    FakePostgres,
    FakeSqlBackend,
    HarnessBase,
    HarnessCondition,
    HarnessError,
    PoolBlockRow,
)
from lab.prism.block_candidates import (  # noqa: E402
    PrismBlockCandidate,
    _BlockCandidateAccountingTask,
    _BlockCandidateNodeSubmission,
)
from lab.prism.share_ledger import (  # noqa: E402
    WRITER_LEASE_HEARTBEAT_SESSION_PREFIX,
    PendingShare,
    PsqlShareLedger,
)

PARENT_HASH = "00" * 32
DEFAULT_HEIGHT = 10


# --------------------------------------------------------------------------
# qbitd
# --------------------------------------------------------------------------


class LandingRpc:
    """The node, as the landing path sees it.

    ``submitblock`` accepts by default and the chain tip follows the last
    accepted block, because a landing that is *rejected* never reaches the
    interleavings this harness is for. A scenario overrides ``submit_result``
    to make one landing fail.
    """

    def __init__(self, *, tip: str, height: int) -> None:
        self.tip = tip
        self.height = height
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.submitted: list[str] = []
        self.submit_result: Callable[[str], object] | None = None
        self.blocks_by_height: dict[int, str] = {}
        self._registered: dict[str, tuple[str, int]] = {}

    def register(self, *, block_hex: str, block_hash: str, height: int) -> None:
        """Teach the node which block a submitted payload is."""
        self._registered[block_hex] = (block_hash, int(height))

    def call(self, method: str, params: list[object] | None = None) -> object:
        args = tuple(params or ())
        self.calls.append((method, args))
        if method == "getbestblockhash":
            return self.tip
        if method == "getblockcount":
            return self.height
        if method == "getblockhash":
            requested = int(args[0])
            return self.blocks_by_height.get(requested, self.tip)
        if method == "submitblock":
            return self._submit(str(args[0]) if args else "")
        if method == "getblockchaininfo":
            return {"initialblockdownload": False}
        if method == "getnetworkinfo":
            return {"connections": 4}
        if method == "getblockheader":
            raise RuntimeError("qbit RPC getblockheader failed: -5 Block not found")
        raise RuntimeError(f"unmodelled qbit RPC method {method}")

    def _submit(self, block_hex: str) -> object:
        self.submitted.append(block_hex)
        result = None if self.submit_result is None else self.submit_result(block_hex)
        if result in (None, "duplicate"):
            known = self._registered.get(block_hex)
            if known is not None:
                self.accept(block_hash=known[0], height=known[1])
        return result

    def accept(self, *, block_hash: str, height: int) -> None:
        """Make ``block_hash`` the active block at ``height``."""
        self.blocks_by_height[height] = block_hash
        self.tip = block_hash
        self.height = height


# --------------------------------------------------------------------------
# Candidates
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class LandingCandidate:
    """One found block, plus everything a scenario needs to talk about it."""

    block_hash: str
    block_height: int
    job_id: str
    candidate: PrismBlockCandidate

    @property
    def envelope_name(self) -> str:
        return f"{self.block_height}-{self.block_hash}"


# --------------------------------------------------------------------------
# The harness
# --------------------------------------------------------------------------


class LandingHarness(HarnessBase):
    """Front end: build found blocks, land them on either tail, read the trace."""

    def __init__(
        self,
        *,
        parent_hash: str = PARENT_HASH,
        block_height: int = DEFAULT_HEIGHT,
        lease_ttl_seconds: float = 60.0,
        watchdog_timeout_seconds: float = 120.0,
        block_landing_db_timeout_seconds: float = 30.0,
        read_concurrency: int = 4,
        capture_output: bool = True,
        baton_timeout_seconds: float = BATON_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(
            capture_output=capture_output,
            baton_timeout_seconds=baton_timeout_seconds,
        )
        self.parent_hash = parent_hash
        self.block_height = int(block_height)
        self.lease_ttl_seconds = float(lease_ttl_seconds)
        self.read_concurrency = int(read_concurrency)
        self._audit_root = Path(tempfile.mkdtemp(prefix="prism-landing-harness-"))
        self._job_counter = 0
        self.sql_backend: FakeSqlBackend | None = None
        self.guard: FakeLeaseGuard | None = None
        self.session_token = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}landing"
        # Which external-side-effect components this run refuses authority
        # for, and which ones actually asked. A scenario sets the first to
        # drive the degraded branch; the second is evidence the gate ran.
        self.withheld_lease_components: set[str] | None = None
        self.lease_fence_components: list[str] = []
        # Production phase names a scenario wants to stop at (see
        # _install_phase_breakpoints).
        self.breakpoints: set[str] = set()

        self.rpc = LandingRpc(tip=parent_hash, height=self.block_height - 1)
        self.pool = self._build_pool(
            watchdog_timeout_seconds=watchdog_timeout_seconds,
            block_landing_db_timeout_seconds=block_landing_db_timeout_seconds,
        )

        # The tails. Named for the production threads they stand in for, so a
        # checkpoint trace reads as "which tail was running".
        self.client = self.scheduler.actor("client")
        self.accounting = self.scheduler.actor("accounting")
        # Startup is its own actor: the ledger constructor acquires the
        # writer lease, which is a modelled statement and so has to run on an
        # actor thread like everything else.
        self.startup = self.scheduler.actor("startup")
        # The share writer's side of the epoch machinery. A landing's fences
        # exist to order against it, so it has to be a third schedulable
        # thread rather than something the test body does between steps.
        self.appender = self.scheduler.actor("appender")

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        super().close()
        store = self.pool.__dict__.get("_audit_artifact_store")
        if store is not None:
            # Teardown runs on the controller thread, which is not an actor,
            # so the store's locks go back to ordinary ones before its own
            # close path takes them. The scheduler is already shut down by
            # then; nothing is left to schedule.
            store._lock = threading.RLock()
            store._publication_lifecycle_lock = threading.RLock()
            store.close()
        shutil.rmtree(self._audit_root, ignore_errors=True)

    # -- construction ------------------------------------------------------

    def _build_pool(
        self,
        *,
        watchdog_timeout_seconds: float,
        block_landing_db_timeout_seconds: float,
    ) -> Any:
        # Imported here rather than at module scope: the support module pulls
        # in the whole coordinator surface, and only a landing scenario needs
        # it.
        from tests.prism_vardiff_test_support import submit_coordinator

        pool, client_state, _ledger = submit_coordinator(tip=self.parent_hash)
        self.client_state = client_state
        pool.max_blocks = 1_000
        pool.stop_after_block = False
        pool.rpc = self.rpc
        pool.watchdog_timeout_seconds = watchdog_timeout_seconds
        pool.block_landing_db_timeout_seconds = block_landing_db_timeout_seconds
        pool.block_submitter_db_timeout_seconds = block_landing_db_timeout_seconds
        pool.audit_dir = self._audit_root / "audit"
        pool.evidence_path = self._audit_root / "state" / "evidence.json"
        pool.ledger_writer_public_key_hex = "aa" * 32
        # The bundle build and its verification are the audit machine, not
        # the landing machine. #128 scopes this harness to landing, so both
        # are answered from a fixed report: a landing that turns on bundle
        # content would be testing the wrong owner.
        pool.build_audit_bundle = lambda **_kwargs: _verified_block_bundle()
        pool.verify_bundle = (
            lambda *_args, expected_block_height=DEFAULT_HEIGHT, **_kwargs: (
                _verified_audit_report(block_height=expected_block_height)
            )
        )
        # Reconciliation walks the chain through the node one block at a
        # time. It is the reorg reconciler's state machine, and #128 lists it
        # separately; a landing only needs its verdict.
        pool.ensure_reorg_reconciled_for_tip = lambda _tip, **_kwargs: True
        pool.reorg_reconciler_enabled = False
        self._install_harness_locks(pool)
        self._install_inline_outbox_worker(pool)
        self._install_inline_external_lease_fence(pool)
        self._install_phase_breakpoints(pool)
        return pool

    def _install_phase_breakpoints(self, pool: Any) -> None:
        """Let a scenario stop a tail at a phase the landing already names.

        SQL statement boundaries are the harness's default interleaving
        points, and for the lease lifecycle they were enough: every state
        transition there *is* a statement. Landing is not like that. Its
        interesting boundaries are the gaps *between* statements — the
        confirm→publish window is bounded on one side by a durable
        confirmation and on the other by a lock the publish step has not
        taken yet, and no statement runs at the instant that matters.

        The landing already names those boundaries for its own watchdog, in
        ``_record_block_submitter_phase`` and
        ``_record_block_candidate_progress``. Reusing that vocabulary means a
        scenario says where it wants to interleave in the code's own words —
        ``progress:evidence-write`` is the top of the publish step — instead
        of counting statements and hoping the count survives a refactor. An
        armed phase that never fires is a loud failure, because the scenario
        then waits at a checkpoint that never arrives.
        """
        record_progress = pool._record_block_candidate_progress
        record_phase = pool._record_block_submitter_phase

        def progress(phase: str = "accounting-progress") -> None:
            record_progress(phase)
            self._maybe_break(f"progress:{phase}")

        def submitter_phase(phase: str) -> None:
            record_phase(phase)
            self._maybe_break(f"phase:{phase}")

        pool._record_block_candidate_progress = progress
        pool._record_block_submitter_phase = submitter_phase

    def _maybe_break(self, label: str) -> None:
        if label in self.breakpoints:
            self.scheduler.checkpoint(label)

    def break_at(self, *labels: str) -> None:
        """Arm one or more phase breakpoints for every tail."""
        self.breakpoints.update(labels)

    def _install_inline_outbox_worker(self, pool: Any) -> None:
        """Run the durable outbox calls on the caller's own actor.

        This is the harness's one substantive model boundary, so it is worth
        being exact about.

        The *landing-class* ledger steps — persist, confirm, the ordinal
        read-back, the pool-block reads — already run inline on the
        block-work thread in production, so they arrive here unchanged, with
        their real budgets, their real admission gates and their real
        statements. The **outbox** marks do not: they run on a spawned
        bounded worker thread (``_run_block_submitter_ledger_call``) so a
        driver that ignores its statement deadline cannot wedge the
        submitter, and the caller waits on a ``threading.Event`` in slices.

        A baton scheduler cannot schedule that worker. It is created inside
        the wrapper with no factory seam, and the caller blocks on a real
        event while holding the baton, so the worker could only ever run
        unscheduled — which would put its statements outside the schedule
        the harness exists to control. Running the operation inline instead
        keeps everything the landing state machine depends on (the operation
        itself, its budget scope, its error propagation and the per-class
        latency metric) and drops only the liveness wrapper around it.

        What that costs is honest to state: this harness cannot express a
        *stuck* outbox driver, so the restart escalations in
        ``_maybe_restart_for_stuck_block_call`` are out of its reach. Those
        are covered example-wise in the block-candidate suite. Giving the
        harness the wrapper too needs a thread-factory seam in production,
        which belongs to whichever issue wants that interleaving.
        """
        service = pool._ensure_block_candidate_service()

        def run_inline(
            key: tuple[object, ...],
            phase: str,
            operation: Callable[[], Any],
            *,
            timeout_seconds: float | None = None,
            call_class: str = "fast",
        ) -> Any:
            budget = (
                service._block_submitter_db_timeout()
                if timeout_seconds is None
                else max(0.001, float(timeout_seconds))
            )
            started = self.clock.monotonic()

            def observe(timed_out: bool) -> None:
                pool._record_block_ledger_call(
                    call_class=call_class,
                    budget_seconds=budget,
                    duration_seconds=max(0.0, self.clock.monotonic() - started),
                    timed_out=timed_out,
                )

            try:
                with service._block_submitter_ledger_timeout_scope(budget):
                    result = operation()
            except BaseException as exc:
                observe(isinstance(exc, TimeoutError))
                raise
            observe(False)
            return result

        pool._run_block_submitter_ledger_call = run_inline

    def _install_inline_external_lease_fence(self, pool: Any) -> None:
        """Prove the guarded session on the caller's own actor.

        ``_require_fresh_ledger_lease_for_external_side_effect`` gates two
        landing steps that reach outside PostgreSQL: the fallback lane's
        ``submitblock``, and the restore of a superseded audit envelope. Both
        gates matter here — the second is the authority check #133's fix
        depends on — so the harness keeps them, and keeps the real
        verification statement behind them.

        What it cannot keep is the shape of the wait. The production fence
        runs the verification on a spawned thread and joins it in wall-clock
        time, so that it can hard-exit a process whose guard session has
        genuinely stopped answering. The baton cannot schedule that thread
        (see ``_install_inline_outbox_worker``), and a verification that runs
        unscheduled would take its statement outside the schedule. Running it
        inline preserves every outcome the landing branches on: the
        verification's own SQL, the deferred-renewal refusal, and a refusal
        that withholds restore authority.

        Out of reach, and named rather than implied: the fence's *timeout*
        arm — a guard session that answers too slowly, or not at all.
        """
        from lab.prism.background_services import guard_session_verifier
        from lab.prism.coordinator_shutdown import ShutdownInProgress
        from lab.prism.share_ledger import WriterLeaseRenewalDeferred

        def require_fresh_lease(component: str) -> None:
            if self.withheld_lease_components is not None and (
                component in self.withheld_lease_components
            ):
                raise ShutdownInProgress(
                    f"writer lease guard verification failed before {component}"
                )
            ledger = pool.ledger
            if not bool(getattr(ledger, "writer_lease_guard_required", False)):
                return
            verify = guard_session_verifier(ledger)
            if verify is None:
                raise ShutdownInProgress(
                    f"writer lease guard verification is unavailable before {component}"
                )
            self.lease_fence_components.append(component)
            try:
                result = verify()
            except BaseException as exc:
                raise ShutdownInProgress(
                    f"writer lease guard verification failed before {component}"
                ) from exc
            if isinstance(result, dict) and result.get(
                "renewal_deferred_to_own_write"
            ):
                raise WriterLeaseRenewalDeferred(
                    f"withholding {component}: writer lease renewal is deferred "
                    "behind this coordinator's own in-flight fenced write"
                )

        pool._require_fresh_ledger_lease_for_external_side_effect = (
            require_fresh_lease
        )

    def _install_harness_locks(self, pool: Any) -> None:
        """Replace every lock the two tails share with a schedulable one.

        Semantics are preserved exactly — a mutex stays a mutex, a
        reentrant lock stays reentrant, the condition keeps its lock — and
        only the blocking mechanism changes. Nothing in the code under test
        can tell the difference except that a contended wait now parks at a
        named checkpoint instead of outside the baton.

        This list is deliberately explicit rather than a sweep over
        ``threading`` objects: a lock that appears in a later slice and is
        not named here will stall the scheduler at a checkpoint that says
        exactly which wait was unmodelled, which is the failure mode worth
        having.
        """
        pool._ensure_job_cache_state()
        pool.lock = self.rlock("coordinator")
        pool._job_cache_lock = self.lock("job-cache")
        pool._payout_balance_mutation_lock = self.rlock("payout-balance-mutation")
        pool._payout_append_landing_fence_lock = self.lock("payout-append-landing-fence")
        pool._payout_state_prepare_lock = self.rlock("payout-state-prepare")
        # Rebuilt rather than reused: the condition is defined over the job
        # cache lock, and it has just been replaced.
        pool._payout_unfenced_append_drained = HarnessCondition(
            pool._job_cache_lock,
            name="unfenced-append-drained",
        )
        pool._accepted_block_payout_preview_condition = self.condition(
            "accepted-block-payout-preview"
        )
        pool._tip_refresh_lock = self.lock("tip-refresh")

    def _sql_backend_factory(
        self,
        conninfo: str,
        *,
        pool_size: int,
        application_name: str,
    ) -> FakeSqlBackend:
        self.sql_backend = FakeSqlBackend(
            self.server,
            pool_size=pool_size,
            application_name=application_name,
            tag="ledger",
        )
        return self.sql_backend

    def _lease_guard_factory(
        self,
        conninfo: str,
        *,
        advisory_lock_key: int,
    ) -> FakeLeaseGuard:
        self.guard = FakeLeaseGuard(
            self.server,
            advisory_lock_key=advisory_lock_key,
            application_name="landing-guard",
            tag="ledger.guard",
        )
        return self.guard

    def _build_ledger(self) -> PsqlShareLedger:
        ledger = PsqlShareLedger(
            psql_command="psql postgres://harness/qbit",
            database_url="postgres://harness/qbit",
            writer_id="prism-coordinator",
            writer_epoch=1,
            writer_session_token=self.session_token,
            initialize_schema=False,
            lease_ttl_seconds=self.lease_ttl_seconds,
            read_concurrency=self.read_concurrency,
            monotonic=self.clock.monotonic,
            lease_retry_sleep=self.sleep,
            sql_backend_factory=self._sql_backend_factory,
            lease_guard_factory=self._lease_guard_factory,
            # Canonicalization is a Rust binary that validates audit bundle
            # schema. It is the audit machine, not the landing machine, and
            # the bundle here is a fixed stand-in; the constructor already
            # takes the seam.
            audit_bundle_canonicalizer=_canonicalize_bundle,
        )
        # The ledger's own admission gates. Both tails queue on them, and
        # #125 is precisely about how long a caller waits here, so they have
        # to be visible to the scheduler and to the virtual clock.
        ledger._lock = self.lock("ledger-writer")
        ledger._read_semaphore = self.semaphore(
            "ledger-read-slot",
            self.read_concurrency,
        )
        # The accepted-share-stats single-flight. Both tails read the stats
        # while building their evidence, so the second one queues here behind
        # the first — which is exactly the confirm→publish gap #133 needs to
        # be *open*, not blocked.
        ledger._stats_refresh_lock = self.lock("ledger-stats-refresh")
        ledger._stats_lock = self.lock("ledger-stats")
        return ledger

    def boot(self) -> PsqlShareLedger:
        """Acquire the writer lease and attach the ledger to the pool.

        Runs on its own actor because lease acquisition is modelled SQL.
        Returns once the lease is held, so a scenario that only cares about
        landing never has to step through startup.
        """
        call = self.startup.submit(self._build_ledger, label="ledger-startup")
        self.run_until(self.startup, "done:ledger-startup")
        ledger = call.value()
        self.pool.ledger = ledger
        self._install_audit_store_locks()
        return ledger

    def _install_audit_store_locks(self) -> None:
        """Make the publication order guard schedulable.

        The guard is what serialises confirm against publish inside one
        landing, and both tails take it. #133 is a *released*-guard defect —
        the interleaving happens in the gap where neither tail holds it — so
        the point of installing a harness lock here is the opposite of
        enabling the bug: it lets a scenario prove the guard is genuinely
        released in that gap, by having the other tail take it and proceed.
        """
        store = self.pool._ensure_audit_artifact_store()
        store._lock = self.rlock("audit-store")
        store._publication_lifecycle_lock = self.rlock("audit-publication-lifecycle")

    # -- found blocks ------------------------------------------------------

    def found_block(
        self,
        block_hash: str,
        *,
        height: int | None = None,
        parent_hash: str | None = None,
        anchor_job_issued_at_ms: int = 12_000,
        append_epoch: int | None = None,
        collection_only: bool = False,
    ) -> LandingCandidate:
        """Register a job and mint the found-block candidate it produced.

        ``append_epoch`` stamps the candidate's build key with a payout
        append-invalidation epoch. It defaults to the live epoch, which is
        what an ordinary job carries; a scenario that wants the landing to
        see a *moved* epoch bumps the live one afterwards rather than lying
        about what the job was built from.
        """
        self._job_counter += 1
        job_id = f"job-{self._job_counter}"
        block_height = self.block_height if height is None else int(height)
        parent = self.parent_hash if parent_hash is None else str(parent_hash)
        if append_epoch is None:
            append_epoch = int(self.pool._payout_ledger_append_invalidation_epoch)
        context = SimpleNamespace(
            job=SimpleNamespace(
                job_id=job_id,
                share_target=(1 << 256) - 1,
                share_difficulty=1,
                transaction_hexes=(),
            ),
            template={
                "previousblockhash": parent,
                "height": block_height,
                "coinbasevalue": 50_00000000,
            },
            found_block={
                "network_difficulty": 1,
                "anchor_job_issued_at_ms": int(anchor_job_issued_at_ms),
            },
            issued_at_ms=int(anchor_job_issued_at_ms),
            collection_only=collection_only,
            worker=self.client_state.worker,
            shares_json=[],
            prior_balances=[],
            payout_append_invalidation_epoch=int(append_epoch),
        )
        self.pool.jobs[job_id] = context
        self.client_state.active_job_ids.add(job_id)
        block_hex = f"block:{block_hash}"
        self.rpc.register(
            block_hex=block_hex,
            block_hash=block_hash,
            height=block_height,
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex=block_hex,
            share_pass=False,
            block_pass=True,
        )
        candidate = PrismBlockCandidate(
            context=context,
            submission=submission,
            extranonce1_hex=self.client_state.extranonce1_hex,
            extranonce2_hex="00" * 8,
            # A real PendingShare, not a stand-in: the durable outbox intent
            # encodes it, so the row the landing replays from is only as
            # honest as this is.
            pending_share=PendingShare(
                share_id=f"miner-a:{block_hash}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=block_height - 1,
                job_id=job_id,
                job_issued_at_ms=int(anchor_job_issued_at_ms),
                accepted_at_ms=int(anchor_job_issued_at_ms),
                ntime=1_700_000_000,
            ),
            client=self.client_state,
            credit_share_on_accept=False,
        )
        return LandingCandidate(
            block_hash=block_hash,
            block_height=block_height,
            job_id=job_id,
            candidate=candidate,
        )

    # -- driving the tails -------------------------------------------------

    def _persist_intent(self, found: LandingCandidate) -> None:
        """Write the durable outbox row this candidate replays from.

        Production writes it at the pre-submit boundary, before either tail
        runs, and refuses to hold a retry slot until it is durable. The tails
        below start after that boundary, so each one writes its own row first
        — otherwise the terminal outbox update at the end of a landing would
        find nothing to mark and the durable replay source would be missing
        from every scenario.
        """
        persist_intent = getattr(self.pool.ledger, "persist_block_candidate_intent", None)
        if not callable(persist_intent):
            return
        persist_intent(self.pool.block_candidate_intent(found.candidate))

    def land_on_client_tail(self, found: LandingCandidate) -> Call:
        """Queue ``found`` down the synchronous miner-connection-thread route."""

        def run() -> bool:
            self._persist_intent(found)
            return self.pool._submit_synchronous_block_candidate(found.candidate)

        return self.client.submit(
            run,
            label=f"client-land:{found.block_hash[:4]}",
        )

    def land_on_accounting_tail(
        self,
        found: LandingCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
    ) -> Call:
        """Queue ``found`` down the accounting-actor route.

        The accounting actor's task carries the fast lane's node-offer
        evidence, so the default here is an unattempted offer: the tail then
        performs the submit itself, which is the shape both #133 and #126
        need.
        """
        submission = node_submission or _BlockCandidateNodeSubmission(attempted=False)

        def run() -> None:
            self._persist_intent(found)
            # The task the accounting loop dequeues, built the way
            # ``submit_next`` builds it: the disposition lease for this hash
            # is already claimed, and the loop hands it back afterwards. The
            # loop itself is a queue drain, so the harness supplies the task
            # rather than the queue.
            lease = self.pool._claim_block_candidate_disposition(
                found.block_hash,
                blocking=True,
            )
            if lease is None:
                raise HarnessError(
                    f"disposition for {found.block_hash} was already claimed"
                )
            self.pool._run_block_accounting_task(
                _BlockCandidateAccountingTask(
                    candidate=found.candidate,
                    node_submission=submission,
                    disposition_lease=lease,
                )
            )

        return self.accounting.submit(
            run,
            label=f"accounting-land:{found.block_hash[:4]}",
        )

    # -- the append side ---------------------------------------------------

    def publish_window_anchor(self, anchor_ms: int) -> None:
        """Expose a newer published job window's anchor to the append side.

        A seedless build hands its declared anchor to this watermark at the
        publication fence, so jobs still serving that window keep an anchor
        visible to the append-side ``predates`` checks after the build's own
        exposure retires. It is the cheapest way for a scenario to say "a
        newer window than this candidate's is live", which is the precondition
        for an append that invalidates *something* without invalidating the
        candidate under test.
        """
        anchor = int(anchor_ms)
        current = getattr(self.pool, "_payout_published_job_window_anchor_ms", None)
        self.pool._payout_published_job_window_anchor_ms = max(anchor, current or 0)

    def append_late_visible_share(
        self,
        *,
        stamp_ms: int,
        share_id: str | None = None,
    ) -> Call:
        """Make one share durable and let it advance the invalidation epoch.

        Queued on its own actor, because the append side is a third thread in
        production (the share writer) and because the bump takes the landing
        fence, which a landing may be holding. ``stamp_ms`` sets both the job
        issue and acceptance stamps, so it is exactly the quantity the
        append-side ``predates`` test compares against an anchor: the row
        invalidates a window whose declared anchor is at or after it, and no
        other.

        Returns the queued call; its value is the advanced epoch, or None when
        no live anchor predated the row.
        """
        stamp = int(stamp_ms)
        share = PendingShare(
            share_id=share_id or f"appender:{stamp}",
            miner_id="miner-b",
            order_key="miner-b",
            p2mr_program_hex="22" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=self.block_height - 1,
            job_id="append-job",
            job_issued_at_ms=stamp,
            accepted_at_ms=stamp,
            ntime=1_700_000_000,
        )

        def run() -> int | None:
            self.server.shares.append(
                {"share_seq": len(self.server.shares) + 1, "miner_id": share.miner_id}
            )
            return self.pool._record_late_visible_payout_append(share)

        return self.appender.submit(run, label=f"append:{stamp}")

    def live_append_epoch(self) -> int:
        return int(self.pool._payout_ledger_append_invalidation_epoch)

    # -- observation -------------------------------------------------------

    @property
    def audit_root(self) -> Path:
        return self._audit_root

    def live_envelope_path(self, found: LandingCandidate) -> Path:
        store = self.pool._ensure_audit_artifact_store()
        return store.live_envelope_path(
            block_height=found.block_height,
            block_hash=found.block_hash,
        )

    def envelope_written(self, found: LandingCandidate) -> bool:
        """Whether this block's published evidence pointer exists on disk.

        This is #133's whole assertion. The block is paid and audited
        either way; what the defect destroyed was the file.
        """
        return self.live_envelope_path(found).exists()

    def pool_block(self, block_hash: str) -> PoolBlockRow | None:
        return self.server.pool_blocks.get(block_hash)

    def publication_sequence(self, found: LandingCandidate) -> int | None:
        row = self.pool_block(found.block_hash)
        return None if row is None else row.audit_publication_sequence

    def publication_floor(self) -> int:
        return max(
            [
                row.audit_publication_sequence or 0
                for row in self.server.pool_blocks.values()
            ],
            default=0,
        )

    def outbox_state(self, found: LandingCandidate) -> str | None:
        row = self.server.outbox.get(found.block_hash)
        return None if row is None else row.state

    def landing_summary(self) -> dict[str, Any]:
        """A compact, order-stable digest for ``assert_deterministic``.

        Deliberately not the whole world: it names the durable facts a
        landing scenario asserts about, so two runs that agree here and on
        the checkpoint trace agree on everything a scenario claims.
        """
        return {
            "pool_blocks": [
                {
                    "block_hash": row.block_hash,
                    "block_height": row.block_height,
                    "chain_state": row.chain_state,
                    "maturity_state": row.maturity_state,
                    "audit_publication_sequence": row.audit_publication_sequence,
                }
                for row in self.server.pool_blocks.values()
            ],
            "outbox": [
                {
                    "block_hash": row.block_hash,
                    "state": row.state,
                    "attempt_count": row.attempt_count,
                    "last_error": row.last_error,
                }
                for row in self.server.outbox.values()
            ],
            "statements": self.statement_kinds(),
            "submitted_blocks": list(self.rpc.submitted),
            "envelopes": sorted(
                path.name
                for path in (self._audit_root / "audit").rglob("*.json")
                if path.is_file()
            ),
        }


# --------------------------------------------------------------------------
# Fixed audit answers
# --------------------------------------------------------------------------


def _canonicalize_bundle(bundle: dict[str, Any]) -> bytes:
    return json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _verified_block_bundle(coinbase_tx_hex: str = "c0ffee") -> dict[str, object]:
    return {
        "found_block": {"coinbase_value_sats": 50_00000000},
        "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
        "payout_policy_manifest": {"accounts": []},
        "signed_coinbase_manifest": {
            "manifest": {
                "coinbase_tx_hex": coinbase_tx_hex,
                "payout_count": 1,
            }
        },
    }


def _verified_audit_report(
    coinbase_tx_hex: str = "c0ffee",
    *,
    block_height: int = DEFAULT_HEIGHT,
) -> dict[str, object]:
    return {
        "schema": "qbit.prism.audit-verification-report.v1",
        "block_height": int(block_height),
        "coinbase_value_sats": 50_00000000,
        "reward_manifest_sha256_hex": "44" * 32,
        "payout_policy_manifest_sha256_hex": "55" * 32,
        "prism_audit_commitment_leaf_hex": "66" * 32,
        "audit_commitment_root_hex": "77" * 32,
        "coinbase_txid": "11" * 32,
        "coinbase_wtxid": "88" * 32,
        "coinbase_manifest_sha256_hex": "22" * 32,
        "audit_bundle_sha256_hex": "33" * 32,
        "coinbase_tx_hex": coinbase_tx_hex,
        "min_output_sats": 1,
        "onchain_output_count": 0,
        "accrued_account_count": 0,
    }


__all__ = [
    "DEFAULT_HEIGHT",
    "PARENT_HASH",
    "LandingCandidate",
    "LandingHarness",
    "LandingRpc",
]

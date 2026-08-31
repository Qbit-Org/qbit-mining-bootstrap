#!/usr/bin/env python3
"""Self-tests for the landing half of the deterministic concurrency harness.

``tests/test_prism_concurrency_harness.py`` defends the lease half. This file
defends the half that models block candidate landing, for the same reason: a
fake that answers an unrecognised statement plausibly drifts away from
production without anyone noticing, and every scenario built on it then
inherits the drift. When a landing scenario fails, these tests are what makes
"the code under test is wrong" the first explanation rather than "the model
is wrong".

Four claims are pinned here, each one something a landing scenario relies on
without restating:

1. **The publication-ordinal allocator follows ``qbit_confirm_pool_block``.**
   The ordinal comes from a sequence rather than from ``MAX + 1``, so a fresh
   confirmation burns a value, an exact replay burns nothing, a terminally
   disposed row reports the distinct superseded disposition, and the durable
   floor — a ``MAX`` over the table — tolerates the gap an aborted
   confirmation leaves. #133 turns on precisely the difference between the
   sequence and the floor, so the model owes the SQL an exact account of it.

2. **Transaction scope is real.** A deadline-scoped landing statement sends
   its COMMIT as a separate message, and the window in between is where the
   interesting interleavings live. A model that published a landing write at
   statement time would close that window and hide the defects inside it.

3. **Classification is anchored to production SQL.** Every landing statement
   is driven through the shipped ``PsqlShareLedger`` method that emits it, so
   a change to that SQL breaks classification loudly instead of quietly
   changing what the fake models.

4. **The synchronisation primitives behave like the ones they replace.** The
   landing topology is the first one where two threads of a single process
   contend a process-local lock, so the harness locks are load-bearing: an
   ordering they can express but a real lock cannot would be an ordering a
   scenario could "prove" and production could never reach.
"""

from __future__ import annotations

import contextlib
import sys
import unittest
from collections.abc import Callable, Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.prism_concurrency_harness import (  # noqa: E402
    Actor,
    Call,
    HarnessError,
    LandingOp,
    PoolBlockRow,
    UnsupportedStatement,
    assert_deterministic,
    classify,
)
from tests.prism_landing_harness import (  # noqa: E402
    PARENT_HASH,
    LandingHarness,
)

BLOCK_A = "aa" * 32
BLOCK_B = "bb" * 32
BLOCK_C = "cc" * 32
BLOCK_D = "dd" * 32

# The top of the publish step, named by the landing itself. A tail stopped
# here has confirmed durably and released every lock it took to do so.
PUBLISH_BOUNDARY = "progress:evidence-write"

# A successor's session token. Nothing but the token has to differ: the
# landing statements fence on the whole identity triple.
SUCCESSOR_TOKEN = "heartbeat-v1:successor"


def _run(
    harness: LandingHarness,
    actor: Actor,
    work: Callable[[], Any],
    label: str,
) -> Call:
    """Run ``work`` to completion on ``actor`` and hand back its call record.

    The call record rather than its value, because half the assertions here
    are about the error a statement raised.
    """
    call = actor.submit(work, label=label)
    harness.run_until(actor, f"done:{label}")
    return call


def _prepared(
    harness: LandingHarness,
    block_hash: str,
    *,
    height: int,
    parent_hash: str = PARENT_HASH,
    chain_state: str = "prepared",
    maturity_state: str = "immature",
) -> PoolBlockRow:
    """Seed one committed ``qbit_pool_blocks`` row.

    Landing a whole block to reach a given row state would put the landing
    state machine between the test and the allocator, which is the wrong
    thing under test here: these cases are about what the allocator does with
    a row, not about how the row got there.
    """
    row = PoolBlockRow(
        block_hash=block_hash,
        block_height=height,
        parent_hash=parent_hash,
        chain_state=chain_state,
        maturity_state=maturity_state,
    )
    harness.server.pool_blocks[block_hash] = row
    return row


@contextlib.contextmanager
def _deposed_writer(harness: LandingHarness) -> Iterator[None]:
    """Hand the writer lease to a successor for the duration of the block.

    A landing statement carries its writer identity inline, so moving the
    committed lease row is the whole of a takeover as far as one statement is
    concerned. Building a second coordinator to produce the successor would
    add a lease lifecycle to a test that is not about lease lifecycles.
    """
    live = harness.server.lease
    assert live is not None
    harness.server.lease = replace(live, writer_session_token=SUCCESSOR_TOKEN)
    try:
        yield
    finally:
        harness.server.lease = live


def _captured_sql(
    harness: LandingHarness,
    actor: Actor,
    work: Callable[[], Any],
    label: str,
) -> str:
    """Production's SQL for one landing statement, without its effects.

    Every landing statement fences on the writer lease before it writes
    anything, so running one under a deposed writer records the statement —
    classified, with its payload recovered — while leaving every table
    untouched. That is what lets a test replay genuine production SQL into a
    transaction it intends to abort, instead of hand-writing the statement
    whose fidelity is the thing being tested.
    """
    with _deposed_writer(harness):
        call = _run(harness, actor, work, label)
    assert call.error is not None, "a deposed writer's landing statement must fail"
    return harness.server.statements[-1].sql


def _read_back_sql(harness: LandingHarness, ledger: Any) -> str:
    """The pool-block state read, as ``pool_block_state`` emits it.

    A read takes no lease CTE, so it needs none of ``_captured_sql``'s
    deposition: running it is already free of effects.
    """
    _run(
        harness,
        harness.accounting,
        lambda: ledger.pool_block_state(block_hash=BLOCK_A),
        "capture-state",
    ).value()
    return harness.server.statements[-1].sql


class PublicationOrdinalAllocatorTests(unittest.TestCase):
    """``qbit_confirm_pool_block``, as the model claims to implement it."""

    def test_a_fresh_confirmation_assigns_the_next_sequence_value(self) -> None:
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)
            _prepared(harness, BLOCK_B, height=11, parent_hash=BLOCK_A)

            first = _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "confirm-a",
            ).value()
            second = _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_B,
                    active_tip_height=11,
                ),
                "confirm-b",
            ).value()

            self.assertEqual(first["confirmed_count"], 1)
            self.assertEqual(first["audit_publication_sequence"], 1)
            self.assertEqual(second["audit_publication_sequence"], 2)

            row = harness.pool_block(BLOCK_A)
            assert row is not None
            self.assertEqual(row.chain_state, "confirmed")
            # Confirmation moves the chain state only. Maturity is the reorg
            # window's business and 100 blocks away.
            self.assertEqual(row.maturity_state, "immature")

    def test_an_exact_replay_returns_one_without_burning_a_sequence_value(
        self,
    ) -> None:
        """The half of the allocator's contract a counter model would get wrong.

        A replay that burned a value would leave the row's ordinal behind the
        sequence, so the next block would jump — and the durable floor would
        rise for a confirmation that changed nothing. The proof is the *next*
        block's ordinal, not the replay's own return value.
        """
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)
            _prepared(harness, BLOCK_B, height=11, parent_hash=BLOCK_A)

            _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "confirm-a",
            ).value()
            replay = _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "replay-a",
            ).value()

            self.assertEqual(replay["confirmed_count"], 1)
            self.assertEqual(replay["audit_publication_sequence"], 1)

            after_replay = _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_B,
                    active_tip_height=11,
                ),
                "confirm-b",
            ).value()
            self.assertEqual(
                after_replay["audit_publication_sequence"],
                2,
                "the replay burned a sequence value it must not have burned",
            )

    def test_a_terminally_disposed_row_reports_the_superseded_disposition(
        self,
    ) -> None:
        """-1 is a distinct answer, not a variant of 0.

        A caller reading -1 abandons the candidate knowing the ledger
        explained itself; a caller reading 0 has to treat the state as
        unexplained. Collapsing the two would let a routine reorg race look
        like corruption, so every terminal shape the SQL names is checked
        here rather than one representative.
        """
        terminal = {
            "reorg-quarantined": ("11" * 32, {"chain_state": "inactive"}),
            "rejected": ("22" * 32, {"chain_state": "rejected"}),
            "reversed": ("33" * 32, {"chain_state": "reversed"}),
            # Confirmed but reversed: the maturity arm of the same test, and
            # the one a chain_state-only model would answer as a replay.
            "confirmed-then-reversed": (
                "44" * 32,
                {"chain_state": "confirmed", "maturity_state": "reversed"},
            ),
        }
        with LandingHarness() as harness:
            ledger = harness.boot()
            for name, (block_hash, state) in terminal.items():
                with self.subTest(disposition=name):
                    _prepared(harness, block_hash, height=10, **state)
                    result = _run(
                        harness,
                        harness.client,
                        lambda h=block_hash: ledger.confirm_accepted_block(
                            block_hash=h,
                            active_tip_height=10,
                        ),
                        f"confirm-{name}",
                    ).value()
                    self.assertEqual(result["confirmed_count"], -1)
                    self.assertNotIn("audit_publication_sequence", result)

            # None of them burned an ordinal, so nothing was allocated at all.
            self.assertEqual(
                _run(
                    harness,
                    harness.client,
                    ledger.audit_publication_sequence_floor,
                    "floor",
                ).value(),
                0,
            )

    def test_a_height_mismatch_and_a_missing_row_both_report_no_match(self) -> None:
        """0 keeps its own meaning: no row, or a live row this does not match."""
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)

            mismatched = _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=11,
                ),
                "wrong-height",
            ).value()
            missing = _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_C,
                    active_tip_height=10,
                ),
                "no-row",
            ).value()

            self.assertEqual(mismatched["confirmed_count"], 0)
            self.assertEqual(missing["confirmed_count"], 0)
            row = harness.pool_block(BLOCK_A)
            assert row is not None
            self.assertEqual(row.chain_state, "prepared")
            self.assertIsNone(row.audit_publication_sequence)

    def test_the_durable_floor_is_a_max_and_tolerates_a_burned_gap(self) -> None:
        """``nextval`` is non-transactional, so an abort leaves a hole.

        The floor read is a ``MAX`` over the table for exactly this reason: it
        follows the ordinals that became durable, not the ones the sequence
        handed out. A model that counted allocations instead would put the
        floor ahead of every published block after the first aborted
        confirmation, and #133's whole argument is about how a block's ordinal
        compares to that floor.
        """
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)
            _prepared(harness, BLOCK_C, height=12, parent_hash=BLOCK_A)
            _prepared(harness, BLOCK_D, height=13, parent_hash=BLOCK_C)

            _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "confirm-a",
            ).value()
            self.assertEqual(harness.publication_floor(), 1)

            confirm_c = _captured_sql(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_C,
                    active_tip_height=12,
                ),
                "capture-confirm-c",
            )

            def aborted_confirmation() -> int | None:
                backend = harness.server.connect(application_name="aborted")
                transaction = harness.server._begin(backend, explicit=True)
                harness.server.execute(
                    backend,
                    confirm_c,
                    transaction=transaction,
                    timeout_seconds=30.0,
                    tag="aborted",
                )
                burned = transaction.staged_pool_blocks[
                    BLOCK_C
                ].audit_publication_sequence
                harness.server._rollback(transaction)
                return burned

            burned = _run(
                harness,
                harness.client,
                aborted_confirmation,
                "abort-confirm-c",
            ).value()
            self.assertEqual(burned, 2)

            floor_after_abort = _run(
                harness,
                harness.client,
                ledger.audit_publication_sequence_floor,
                "floor-after-abort",
            ).value()
            self.assertEqual(
                floor_after_abort,
                1,
                "an aborted confirmation must not raise the durable floor",
            )

            after = _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_D,
                    active_tip_height=13,
                ),
                "confirm-d",
            ).value()
            self.assertEqual(
                after["audit_publication_sequence"],
                3,
                "the burned value must never be handed out again",
            )
            self.assertEqual(harness.publication_floor(), 3)


class LandingTransactionVisibilityTests(unittest.TestCase):
    """The confirm→COMMIT window, which is where landing defects live."""

    def test_a_staged_landing_write_is_invisible_to_another_session(self) -> None:
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)
            # Captured up front, so the reader replays one identical statement
            # on both sides of the COMMIT.
            state_sql = _read_back_sql(harness, ledger)
            reader = harness.server.connect(application_name="reader")

            def read_pool_block() -> Any:
                return harness.server.execute(
                    reader,
                    state_sql,
                    transaction=None,
                    timeout_seconds=None,
                    tag="reader",
                )

            def confirm_under_deadline() -> Any:
                # A caller deadline is what makes _NativePostgresClient wrap
                # the statement in BEGIN / statement / COMMIT, so it is also
                # what opens the window this test is about.
                with ledger.operation_timeout(30.0):
                    return ledger.confirm_accepted_block(
                        block_hash=BLOCK_A,
                        active_tip_height=10,
                    )

            landing = harness.client.submit(confirm_under_deadline, label="confirm")
            harness.run_until(harness.client, "ledger.precommit")

            assert harness.sql_backend is not None
            transaction = harness.sql_backend._in_use[0].transaction
            assert transaction is not None
            staged = transaction.staged_pool_blocks[BLOCK_A]
            self.assertEqual(staged.chain_state, "confirmed")
            self.assertEqual(staged.audit_publication_sequence, 1)

            before = _run(
                harness,
                harness.accounting,
                read_pool_block,
                "read-before-commit",
            ).value()
            self.assertEqual(before["state"]["chain_state"], "prepared")
            self.assertIsNone(before["state"]["audit_publication_sequence"])

            harness.drain([harness.client])
            landing.value()

            after = _run(
                harness,
                harness.accounting,
                read_pool_block,
                "read-after-commit",
            ).value()
            self.assertEqual(after["state"]["chain_state"], "confirmed")
            self.assertEqual(after["state"]["audit_publication_sequence"], 1)

    def test_a_transaction_sees_its_own_landing_write_before_commit(self) -> None:
        """READ COMMITTED: committed rows, plus this transaction's own writes.

        The other half of the window. A landing reads its own confirmation
        back — that is how it learns its publication ordinal — so a model that
        staged writes where even their own session could not see them would
        break every landing rather than only the interleaved ones.
        """
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)
            state_sql = _read_back_sql(harness, ledger)
            confirm_sql = _captured_sql(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "capture-confirm",
            )

            def confirm_then_read() -> Any:
                backend = harness.server.connect(application_name="own-session")
                transaction = harness.server._begin(backend, explicit=True)
                harness.server.execute(
                    backend,
                    confirm_sql,
                    transaction=transaction,
                    timeout_seconds=30.0,
                    tag="own",
                )
                own = harness.server.execute(
                    backend,
                    state_sql,
                    transaction=transaction,
                    timeout_seconds=30.0,
                    tag="own",
                )
                harness.server._commit(transaction)
                return own

            own = _run(
                harness,
                harness.client,
                confirm_then_read,
                "own-session-read",
            ).value()
            self.assertEqual(own["state"]["chain_state"], "confirmed")
            self.assertEqual(own["state"]["audit_publication_sequence"], 1)
            # And it really was the same transaction's write that it saw.
            row = harness.pool_block(BLOCK_A)
            assert row is not None
            self.assertEqual(row.chain_state, "confirmed")

    def test_a_rolled_back_landing_leaves_no_row_but_keeps_the_burned_ordinal(
        self,
    ) -> None:
        """Rollback undoes rows. It does not undo ``nextval``.

        Both halves matter to a landing scenario: a durable-looking row that
        an abort left behind would make a scenario assert a landing that never
        happened, and an ordinal handed back on abort would make the sequence
        look like the counter it deliberately is not.
        """
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)
            _prepared(harness, BLOCK_C, height=12, parent_hash=BLOCK_A)
            _prepared(harness, BLOCK_D, height=13, parent_hash=BLOCK_C)

            _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "confirm-a",
            ).value()

            found = harness.found_block(BLOCK_C, height=12, parent_hash=BLOCK_A)
            intent = harness.pool.block_candidate_intent(found.candidate)
            outbox_sql = _captured_sql(
                harness,
                harness.client,
                lambda: ledger.persist_block_candidate_intent(intent),
                "capture-outbox",
            )
            confirm_sql = _captured_sql(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_C,
                    active_tip_height=12,
                ),
                "capture-confirm",
            )

            def aborted_landing() -> None:
                backend = harness.server.connect(application_name="aborted")
                transaction = harness.server._begin(backend, explicit=True)
                for sql in (outbox_sql, confirm_sql):
                    harness.server.execute(
                        backend,
                        sql,
                        transaction=transaction,
                        timeout_seconds=30.0,
                        tag="aborted",
                    )
                self.assertIn(BLOCK_C, transaction.staged_outbox)
                self.assertNotIn(BLOCK_C, harness.server.outbox)
                harness.server._rollback(transaction)

            _run(harness, harness.client, aborted_landing, "abort").value()

            self.assertNotIn(BLOCK_C, harness.server.outbox)
            row = harness.pool_block(BLOCK_C)
            assert row is not None
            self.assertEqual(row.chain_state, "prepared")
            self.assertIsNone(row.audit_publication_sequence)

            next_ordinal = _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_D,
                    active_tip_height=13,
                ),
                "confirm-d",
            ).value()
            self.assertEqual(next_ordinal["audit_publication_sequence"], 3)


class LandingStatementClassificationTests(unittest.TestCase):
    """The classifier is pinned to the SQL PsqlShareLedger actually emits."""

    def test_every_modelled_landing_statement_is_recognised_from_real_calls(
        self,
    ) -> None:
        """Drive every ``LandingOp`` through production code and classify it.

        The fragments in ``_LANDING_SIGNATURES`` were lifted from
        ``share_ledger.py``; this is what proves they still match what that
        module generates rather than what it generated when the landing model
        was written. A full landing covers most of them on its own — that it
        does is itself part of the claim, because a scenario that never
        reaches a statement never checks it either.
        """
        with LandingHarness() as harness:
            ledger = harness.boot()
            found = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)
            landed = harness.land_on_client_tail(found)
            harness.drain([harness.client])
            self.assertIsNone(landed.error)

            # The four a successful landing never emits: nothing was
            # rejected, neither the share history nor the pool-block state
            # is read on the happy path, and the height-bounded as-of balance
            # read belongs to confirmed-ancestor replay only (issue #209).
            _prepared(harness, BLOCK_B, height=11, parent_hash=BLOCK_A)
            _run(
                harness,
                harness.client,
                lambda: ledger.reject_prepared_block(
                    block_hash=BLOCK_B,
                    active_tip_height=11,
                ),
                "reject",
            ).value()
            _run(harness, harness.client, ledger.all_shares, "shares").value()
            _run(
                harness,
                harness.client,
                lambda: ledger.pool_block_state(block_hash=BLOCK_A),
                "state",
            ).value()
            _run(
                harness,
                harness.client,
                lambda: ledger.prior_balances_after_pool_block(
                    block_hash=BLOCK_A,
                ),
                "as-of",
            ).value()

            # The replay path's two statements (issues #196 and #211). Neither
            # is on a landing's own critical path -- the enumeration recovers
            # missed wakeups and the batch abandon collapses superseded
            # siblings -- so both are driven explicitly here.
            _prepared(harness, BLOCK_C, height=12, parent_hash=BLOCK_B)
            _run(
                harness,
                harness.client,
                lambda: ledger.persist_block_candidate_intent(
                    {
                        "schema": "qbit.prism.block-candidate-intent.v1",
                        "block_hash_hex": BLOCK_C,
                        "block_hex": "00",
                    }
                ),
                "intent-c",
            ).value()
            page = _run(
                harness,
                harness.client,
                lambda: ledger.pending_block_candidate_rows(limit=32),
                "pending-page",
            ).value()
            self.assertEqual([row["block_hash"] for row in page], [BLOCK_C])
            # BLOCK_C has a prepared pool-block row, so the fenced batch write
            # re-checks it and reports nothing: the page fact and the write's
            # own veto are exercised together.
            self.assertTrue(page[0]["pool_block_exists"])
            self.assertEqual(
                _run(
                    harness,
                    harness.client,
                    lambda: ledger.mark_block_candidates_abandoned(
                        block_hashes=[BLOCK_C],
                        error="superseded by a decided height",
                    ),
                    "batch-abandon",
                ).value(),
                (),
            )

            observed = {
                kind
                for kind in harness.statement_kinds()
                if kind in {op.value for op in LandingOp}
            }
            self.assertEqual(observed, {op.value for op in LandingOp})

    def test_a_landing_table_alone_is_not_a_classification(self) -> None:
        """A signature match, not a table mention, is what answers a statement.

        The deferred markers for the landing tables are gone now that landing
        is modelled, so a statement against one of them that matches no
        signature has to reach the unmodelled-statement error rather than
        being answered by whichever evaluator matched loosest.
        """
        for sql in (
            "SELECT count(*) FROM qbit_pool_blocks;",
            "DELETE FROM qbit_block_candidate_outbox WHERE block_hash = 'ab';",
            "UPDATE qbit_pool_blocks SET maturity_state = 'mature';",
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(UnsupportedStatement) as caught:
                    classify(sql)
                self.assertIn("does not model this statement", str(caught.exception))
                self.assertIn("_LANDING_SIGNATURES", str(caught.exception))

    def test_deferred_state_machines_still_name_their_follow_up(self) -> None:
        """Landing borders two state machines #128 defers, and says so.

        A landing scenario that wanders into share submission or CTV fanout
        gets an error naming the follow-up rather than a bare model gap, and
        that remains true now that landing statements are recognised ahead of
        the deferred markers.
        """
        for sql, expected in (
            ("INSERT INTO qbit_share_ledger (share_id)", "share submission"),
            (
                "UPDATE qbit_ctv_fanouts SET state = 'broadcast'",
                "CTV fanout",
            ),
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(UnsupportedStatement) as caught:
                    classify(sql)
                self.assertIn(expected, str(caught.exception))
                self.assertIn("#128", str(caught.exception))

    def test_the_writer_identity_is_recovered_from_the_statement(self) -> None:
        """The model is never told who a session belongs to; it reads the SQL.

        The two shapes recover it differently — the pool-block functions take
        it as inlined arguments, the outbox statements inline it into their
        lease CTE — so both are checked. If either recovery broke, the fence
        below would fail open rather than closed.
        """
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)
            found = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)

            _run(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "confirm",
            ).value()
            confirm = next(
                statement
                for statement in reversed(harness.server.statements)
                if statement.kind is LandingOp.CONFIRM_BLOCK
            )
            self.assertEqual(confirm.payload["block_hash"], BLOCK_A)
            self.assertEqual(confirm.payload["active_tip_height"], 10)

            _run(
                harness,
                harness.client,
                lambda: ledger.persist_block_candidate_intent(
                    harness.pool.block_candidate_intent(found.candidate)
                ),
                "outbox",
            ).value()
            outbox = harness.server.statements[-1]
            self.assertEqual(outbox.kind, LandingOp.OUTBOX_RECORD)
            self.assertEqual(outbox.payload["block_hash"], BLOCK_A)

            for statement in (confirm, outbox):
                with self.subTest(kind=statement.kind.value):
                    self.assertEqual(
                        statement.payload["writer_id"],
                        "prism-coordinator",
                    )
                    self.assertEqual(statement.payload["writer_epoch"], 1)
                    self.assertEqual(
                        statement.payload["writer_session_token"],
                        harness.session_token,
                    )

    def test_a_deposed_writer_fails_the_lease_cte_in_the_shape_sql_dictates(
        self,
    ) -> None:
        """Same fence, two observable shapes, and the difference is real.

        ``qbit_confirm_pool_block`` and ``qbit_reject_prepared_pool_block``
        ``RAISE`` when their lease UPDATE affects no rows, so the client sees
        an error. The outbox statements are plain CTEs whose ``CASE`` reports
        the same condition as a value, so the client sees a result and its
        caller is what turns it into an error. A model that flattened the two
        would let a scenario put a fenced-out landing on the wrong side of a
        ``try``.
        """
        with LandingHarness() as harness:
            ledger = harness.boot()
            _prepared(harness, BLOCK_A, height=10)
            found = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)

            confirm_sql = _captured_sql(
                harness,
                harness.client,
                lambda: ledger.confirm_accepted_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "capture-confirm",
            )
            reject_sql = _captured_sql(
                harness,
                harness.client,
                lambda: ledger.reject_prepared_block(
                    block_hash=BLOCK_A,
                    active_tip_height=10,
                ),
                "capture-reject",
            )
            outbox_sql = _captured_sql(
                harness,
                harness.client,
                lambda: ledger.persist_block_candidate_intent(
                    harness.pool.block_candidate_intent(found.candidate)
                ),
                "capture-outbox",
            )

            assert harness.sql_backend is not None
            backend = harness.sql_backend
            with _deposed_writer(harness):
                for name, sql in (
                    ("confirm", confirm_sql),
                    ("reject_prepared", reject_sql),
                ):
                    with self.subTest(statement=name):
                        call = _run(
                            harness,
                            harness.client,
                            lambda s=sql: backend.run_json(s),
                            f"fenced-{name}",
                        )
                        self.assertIsInstance(call.error, RuntimeError)
                        self.assertEqual(
                            str(call.error),
                            "writer lease is not active",
                        )

                outbox = _run(
                    harness,
                    harness.client,
                    lambda: backend.run_json(outbox_sql),
                    "fenced-outbox",
                ).value()
                self.assertEqual(
                    outbox,
                    {"error": "writer lease is not active"},
                )

            # Fenced out means fenced out: nothing reached a table either way.
            row = harness.pool_block(BLOCK_A)
            assert row is not None
            self.assertEqual(row.chain_state, "prepared")
            self.assertEqual(harness.server.outbox, {})


class HarnessLockTests(unittest.TestCase):
    """A contended process-local lock is an interleaving point, not a stall."""

    def test_two_actors_are_mutually_excluded_and_granted_in_arrival_order(
        self,
    ) -> None:
        """FIFO, because the schedule is an input here.

        Without a queue the model could grant in an order neither a real
        futex-backed lock nor the scheduler would produce, and a scenario
        could then "prove" an interleaving production cannot reach.
        """
        with LandingHarness() as harness:
            lock = harness.lock("payout-balance-mutation")
            granted: list[str] = []

            def hold(name: str) -> Callable[[], None]:
                def run() -> None:
                    lock.acquire()
                    granted.append(name)
                    harness.scheduler.checkpoint(f"{name}:holding")
                    lock.release()

                return run

            harness.client.submit(hold("client"), label="client-hold")
            harness.accounting.submit(hold("accounting"), label="accounting-hold")
            harness.appender.submit(hold("appender"), label="appender-hold")

            harness.run_until(harness.client, "client:holding")
            # Arrival order is chosen by the controller, not by thread timing.
            parked = "lock:payout-balance-mutation"
            harness.run_until_blocked(harness.accounting, parked)
            harness.run_until_blocked(harness.appender, parked)
            self.assertEqual(granted, ["client"])
            self.assertIs(lock.owner, harness.client)

            harness.step(harness.client)
            self.assertTrue(harness.accounting.runnable())
            self.assertFalse(
                harness.appender.runnable(),
                "a later arrival must not overtake a queued waiter",
            )

            harness.run_until(harness.accounting, "accounting:holding")
            self.assertEqual(granted, ["client", "accounting"])
            self.assertFalse(harness.appender.runnable())

            harness.step(harness.accounting)
            harness.run_until(harness.appender, "appender:holding")
            self.assertEqual(granted, ["client", "accounting", "appender"])
            harness.drain([harness.client, harness.accounting, harness.appender])
            self.assertIsNone(lock.owner)

    def test_a_non_blocking_acquire_refuses_rather_than_parking(self) -> None:
        with LandingHarness() as harness:
            lock = harness.lock("tip-refresh")

            def hold() -> None:
                lock.acquire()
                harness.scheduler.checkpoint("holding")
                lock.release()

            harness.client.submit(hold, label="hold")
            harness.run_until(harness.client, "holding")

            probe = _run(
                harness,
                harness.accounting,
                lambda: lock.acquire(blocking=False),
                "try-acquire",
            )
            self.assertFalse(probe.value())
            # It refused instead of queueing, so nothing is waiting on the
            # holder and the holder owes it nothing.
            self.assertFalse(harness.accounting.blocked)
            harness.drain([harness.client, harness.accounting])

    def test_a_timed_acquire_expires_against_the_virtual_clock(self) -> None:
        with LandingHarness() as harness:
            lock = harness.lock("job-cache")

            def hold() -> None:
                lock.acquire()
                harness.scheduler.checkpoint("holding")
                lock.release()

            harness.client.submit(hold, label="hold")
            harness.run_until(harness.client, "holding")

            started = harness.clock.monotonic()
            timed = harness.accounting.submit(
                lambda: lock.acquire(timeout=5.0),
                label="timed-acquire",
            )
            harness.run_until_blocked(harness.accounting, "lock:job-cache")
            self.assertEqual(harness.accounting.wake_at, started + 5.0)

            harness.advance_to_next_deadline()
            harness.run_until(harness.accounting, "done:timed-acquire")
            self.assertFalse(timed.value())
            self.assertEqual(harness.clock.monotonic(), started + 5.0)
            # Nothing slept: the wait was measured, not endured.
            self.assertEqual(harness.sleeps, [])
            harness.drain([harness.client, harness.accounting])

    def test_re_acquiring_a_non_reentrant_lock_is_reported_not_hidden(self) -> None:
        """A real mutex deadlocks there, and a harness that hid it would lie.

        Silently granting the second acquire would turn a production deadlock
        into a green scenario, which is the single most expensive thing this
        harness could get wrong.
        """
        with LandingHarness() as harness:
            lock = harness.lock("payout-append-landing-fence")

            def double_acquire() -> None:
                lock.acquire()
                lock.acquire()

            call = _run(harness, harness.client, double_acquire, "double")
            with self.assertRaises(HarnessError) as caught:
                call.value()
            self.assertIn("production would deadlock here", str(caught.exception))


class HarnessRLockTests(unittest.TestCase):
    def test_the_owner_re_enters_and_a_waiter_waits_for_depth_zero(self) -> None:
        with LandingHarness() as harness:
            lock = harness.rlock("coordinator")

            def hold() -> None:
                lock.acquire()
                lock.acquire()
                harness.scheduler.checkpoint("depth-two")
                lock.release()
                harness.scheduler.checkpoint("depth-one")
                lock.release()

            harness.client.submit(hold, label="hold")
            waiter = harness.accounting.submit(lock.acquire, label="waiter")
            harness.run_until(harness.client, "depth-two")
            harness.run_until_blocked(harness.accounting, "lock:coordinator")

            harness.run_until(harness.client, "depth-one")
            self.assertFalse(
                harness.accounting.runnable(),
                "an outer release still leaves the lock held",
            )

            harness.step(harness.client)
            self.assertTrue(harness.accounting.runnable())
            harness.run_until(harness.accounting, "done:waiter")
            self.assertTrue(waiter.value())
            self.assertIs(lock.owner, harness.accounting)


class HarnessConditionTests(unittest.TestCase):
    """``wait`` must drop the lock, or the notifier could never take it."""

    def test_a_wait_releases_the_lock_and_a_notify_from_elsewhere_wakes_it(
        self,
    ) -> None:
        with LandingHarness() as harness:
            condition = harness.condition("accepted-block-payout-preview")
            notifier_held_the_lock: list[bool] = []
            reacquired: list[bool] = []

            def wait_for_notify() -> bool:
                with condition:
                    notified = condition.wait()
                    # Held again before wait returns, because a predicate loop
                    # written against a real condition re-reads shared state
                    # on this line.
                    reacquired.append(condition._lock.held_by_current())
                    return notified

            def notify() -> None:
                with condition:
                    notifier_held_the_lock.append(condition._lock.locked())
                    condition.notify_all()

            waiter = harness.accounting.submit(wait_for_notify, label="wait")
            harness.run_until_blocked(
                harness.accounting,
                "cond:accepted-block-payout-preview",
            )
            self.assertIsNone(
                condition._lock.owner,
                "a parked waiter must not still hold the condition's lock",
            )

            _run(harness, harness.client, notify, "notify").value()
            self.assertEqual(notifier_held_the_lock, [True])
            self.assertTrue(harness.accounting.runnable())

            harness.run_until(harness.accounting, "done:wait")
            self.assertTrue(waiter.value())
            self.assertEqual(reacquired, [True])
            self.assertIsNone(condition._lock.owner)

    def test_a_timed_wait_expires_against_the_virtual_clock(self) -> None:
        with LandingHarness() as harness:
            condition = harness.condition("unfenced-append-drained")
            started = harness.clock.monotonic()

            def wait_briefly() -> bool:
                with condition:
                    return condition.wait(7.5)

            timed = harness.appender.submit(wait_briefly, label="timed-wait")
            harness.run_until_blocked(harness.appender, "cond:unfenced-append-drained")
            self.assertEqual(harness.appender.wake_at, started + 7.5)

            harness.advance_to_next_deadline()
            harness.run_until(harness.appender, "done:timed-wait")
            self.assertFalse(timed.value(), "an expiry is not a notification")
            self.assertEqual(harness.clock.monotonic(), started + 7.5)
            self.assertEqual(harness.sleeps, [])

    def test_re_entrant_depth_survives_a_wait(self) -> None:
        """The coordinator's condition sits on a reentrant lock.

        A wait that re-acquired to depth one would leave the caller's outer
        ``with`` releasing a lock it no longer holds — a failure a long way
        from its cause. Proved behaviourally: after one release the lock is
        still held, which can only be true if the depth came back as two.
        """
        with LandingHarness() as harness:
            condition = harness.condition("accepted-block-payout-preview")
            depth_after_wait: list[bool] = []

            def nested_wait() -> None:
                condition.acquire()
                condition.acquire()
                condition.wait()
                condition.release()
                depth_after_wait.append(condition._lock.locked())
                condition.release()

            harness.accounting.submit(nested_wait, label="nested")
            harness.run_until_blocked(
                harness.accounting,
                "cond:accepted-block-payout-preview",
            )
            _run(harness, harness.client, condition.notify_all, "notify").value()
            harness.run_until(harness.accounting, "done:nested")

            self.assertEqual(depth_after_wait, [True])
            self.assertIsNone(condition._lock.owner)


class HarnessSemaphoreTests(unittest.TestCase):
    """The ledger's read slots, which a landing scenario can exhaust on purpose."""

    def test_exhaustion_parks_and_a_release_admits_one_waiter(self) -> None:
        with LandingHarness() as harness:
            semaphore = harness.semaphore("ledger-read-slot", 1)

            def hold() -> None:
                semaphore.acquire()
                harness.scheduler.checkpoint("holding-slot")
                semaphore.release()

            harness.client.submit(hold, label="hold")
            harness.run_until(harness.client, "holding-slot")
            self.assertEqual(semaphore.value, 0)

            waiter = harness.accounting.submit(semaphore.acquire, label="wait-slot")
            harness.run_until_blocked(harness.accounting, "semaphore:ledger-read-slot")

            harness.step(harness.client)
            self.assertTrue(harness.accounting.runnable())
            harness.run_until(harness.accounting, "done:wait-slot")
            self.assertTrue(waiter.value())
            self.assertEqual(semaphore.value, 0)

    def test_releasing_above_the_initial_value_is_refused(self) -> None:
        """``BoundedSemaphore``'s check, kept: an over-release is a real bug.

        A slot handed back twice inflates read concurrency for the rest of the
        run, and the scenario that then passes is measuring a pool production
        never had.
        """
        with LandingHarness() as harness:
            semaphore = harness.semaphore("ledger-read-slot", 2)
            call = _run(harness, harness.client, semaphore.release, "over-release")
            with self.assertRaises(HarnessError) as caught:
                call.value()
            self.assertIn("released too many times", str(caught.exception))


class HarnessEventTests(unittest.TestCase):
    def test_a_wait_parks_until_the_event_is_set(self) -> None:
        with LandingHarness() as harness:
            event = harness.event("block-landed")
            waiter = harness.appender.submit(event.wait, label="await-event")
            harness.run_until_blocked(harness.appender, "event:block-landed")
            self.assertFalse(event.is_set())

            _run(harness, harness.client, event.set, "set-event").value()
            self.assertTrue(harness.appender.runnable())
            harness.run_until(harness.appender, "done:await-event")
            self.assertTrue(waiter.value())

    def test_a_timed_wait_expires_against_the_virtual_clock(self) -> None:
        with LandingHarness() as harness:
            event = harness.event("block-landed")
            started = harness.clock.monotonic()
            timed = harness.appender.submit(
                lambda: event.wait(3.0),
                label="timed-event",
            )
            harness.run_until_blocked(harness.appender, "event:block-landed")
            self.assertEqual(harness.appender.wake_at, started + 3.0)

            harness.advance_to_next_deadline()
            harness.run_until(harness.appender, "done:timed-event")
            self.assertFalse(timed.value())
            self.assertEqual(harness.clock.monotonic(), started + 3.0)
            self.assertEqual(harness.sleeps, [])


def _landing_with_a_replay_in_the_publish_window(
    harness: LandingHarness,
) -> dict[str, object]:
    """One landing, with the allocator driven inside its confirm→publish gap.

    The gap is the landing model's one genuinely concurrent window — the
    landing has confirmed durably and released every lock it took to do so —
    so it is the right place to prove the allocator's answers do not depend on
    which actor the controller steps next.
    """
    ledger = harness.boot()
    harness.break_at(PUBLISH_BOUNDARY)
    found = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)
    landed = harness.land_on_client_tail(found)
    harness.run_until(harness.client, PUBLISH_BOUNDARY)

    _prepared(harness, BLOCK_B, height=11, parent_hash=BLOCK_A)
    _prepared(harness, BLOCK_C, height=12, parent_hash=BLOCK_B)
    replay = harness.accounting.submit(
        lambda: ledger.confirm_accepted_block(
            block_hash=BLOCK_A,
            active_tip_height=10,
        ),
        label="replay-a",
    )
    rejected = harness.accounting.submit(
        lambda: ledger.reject_prepared_block(
            block_hash=BLOCK_B,
            active_tip_height=11,
        ),
        label="reject-b",
    )
    harness.run_until(harness.accounting, "done:reject-b")
    harness.drain([harness.client, harness.accounting])

    # After the window rather than inside it, and deliberately so. A bare
    # confirmation allocates an ordinal that no publication ever follows, and
    # a landing still holding a lower one then fails its own publish against a
    # floor nothing published raised. That is the landing refusing to publish
    # behind the ledger, not a model artefact — but it belongs to a scenario
    # about the landing, and this one is about the allocator.
    confirmed = _run(
        harness,
        harness.accounting,
        lambda: ledger.confirm_accepted_block(
            block_hash=BLOCK_C,
            active_tip_height=12,
        ),
        "confirm-c",
    )

    return {
        "landed": landed.error is None and landed.result is True,
        "a_sequence": harness.publication_sequence(found),
        "replay": replay.value(),
        "rejected": rejected.value(),
        "confirmed": confirmed.value(),
        "floor": harness.publication_floor(),
        "summary": harness.landing_summary(),
    }


class LandingModelDeterminismTests(unittest.TestCase):
    """A scenario that is merely usually right is worse than no scenario."""

    def test_the_landing_model_is_order_stable_across_repeated_runs(self) -> None:
        outcome = assert_deterministic(
            self,
            _landing_with_a_replay_in_the_publish_window,
            harness_factory=LandingHarness,
        ).outcome
        assert isinstance(outcome, dict)

        self.assertTrue(outcome["landed"])
        self.assertEqual(outcome["a_sequence"], 1)
        # The replay ran in the gap and reported the ordinal already assigned.
        self.assertEqual(outcome["replay"]["confirmed_count"], 1)
        self.assertEqual(outcome["replay"]["audit_publication_sequence"], 1)
        self.assertEqual(outcome["rejected"]["rejected_count"], 1)
        # And it burned nothing: the next fresh confirmation takes 2.
        self.assertEqual(outcome["confirmed"]["audit_publication_sequence"], 2)
        self.assertEqual(outcome["floor"], 2)


if __name__ == "__main__":
    unittest.main()

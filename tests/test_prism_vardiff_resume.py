#!/usr/bin/env python3
"""Reconnect difficulty resume: retention store, adoption, and observability.

A reconnecting worker (same listener + exact username) resumes at its last
converged difficulty instead of the lane start, so a mass reconnect does not
re-flood the share path while every session re-climbs (the 2026-07-16
amplifier). These tests pin the store semantics, the authorize-time adoption
ordering, the plausibility clamp, the kill switch, and the new metrics.
"""

from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import tempfile
import threading
import time
import unittest
from typing import Iterator
from unittest import mock

from lab.auxpow import vardiff
from lab.prism.coordinator_config import load_coordinator_config
from lab.prism.metrics import MetricsRenderer
from lab.prism.prism_coordinator import PrismCoordinator
from lab.prism.rpc import JsonRpc
from lab.prism.stratum_session import ClientState, StratumError
from lab.prism.vardiff_service import (
    PRISM_VARDIFF_RESUME_OUTCOMES,
    SessionDifficultyStore,
    VardiffService,
    session_difficulty_key,
)
from tests.prism_vardiff_test_support import (
    PAYOUT_ADDRESS,
    AddressValidationRpc,
    coordinator,
    highdiff_vardiff_config,
    worker_identity,
)


STANDARD_LANE_START = Decimal("16384")


def standard_lane_config() -> vardiff.VardiffConfig:
    """The deployed standard lane from issue #132's policy table."""
    return vardiff.VardiffConfig(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("1024"),
        max_difficulty=Decimal("4294967296"),
        retarget_interval_seconds=Decimal("300"),
        max_step_factor=Decimal("4"),
        startup_difficulty=STANDARD_LANE_START,
        max_step_down_factor=Decimal("4"),
    )


class SessionDifficultyStoreTests(unittest.TestCase):
    def test_lru_bound_evicts_oldest_and_counts(self) -> None:
        store = SessionDifficultyStore(max_entries=2, ttl_seconds=900.0)
        now = time.monotonic()
        store.record(("default", "a"), Decimal("512"), now=now, share_backed=True)
        store.record(("default", "b"), Decimal("256"), now=now, share_backed=True)
        store.record(("default", "c"), Decimal("128"), now=now, share_backed=True)

        self.assertEqual(len(store), 2)
        self.assertEqual(store.lookup(("default", "a"), now=now), (None, "miss"))
        self.assertEqual(
            store.lookup(("default", "c"), now=now), (Decimal("128"), "hit")
        )
        self.assertEqual(store.snapshot()["evicted"], 1)

    def test_hit_refreshes_recency_but_not_ttl_and_keeps_the_entry(self) -> None:
        store = SessionDifficultyStore(max_entries=2, ttl_seconds=10.0)
        now = time.monotonic()
        store.record(("default", "a"), Decimal("512"), now=now, share_backed=True)
        store.record(("default", "b"), Decimal("1"), now=now + 1, share_backed=True)

        # A hit does not remove the entry (a miner may re-authorize) and it
        # refreshes LRU recency: the next capacity eviction takes "b".
        self.assertEqual(
            store.lookup(("default", "a"), now=now + 2), (Decimal("512"), "hit")
        )
        store.record(("default", "c"), Decimal("2"), now=now + 2, share_backed=True)
        self.assertEqual(store.lookup(("default", "b"), now=now + 2), (None, "miss"))
        self.assertEqual(
            store.lookup(("default", "a"), now=now + 6), (Decimal("512"), "hit")
        )
        # But the TTL still measures the age of the VALUE, not of the last
        # access: the entry recorded at `now` is gone at now + 11.
        self.assertEqual(
            store.lookup(("default", "a"), now=now + 11), (None, "expired")
        )
        self.assertEqual(store.snapshot()["expired"], 1)

    def test_record_silently_ignores_unusable_difficulties(self) -> None:
        store = SessionDifficultyStore(max_entries=4, ttl_seconds=10.0)
        now = time.monotonic()
        store.record(("default", "a"), Decimal("0"), now=now, share_backed=True)
        store.record(("default", "a"), Decimal("-4"), now=now, share_backed=True)
        store.record(("default", "a"), Decimal("NaN"), now=now, share_backed=True)
        store.record(
            ("default", "a"), Decimal("Infinity"), now=now, share_backed=True
        )
        self.assertEqual(len(store), 0)

    def test_non_positive_bounds_disable_retention_entirely(self) -> None:
        now = time.monotonic()
        for store in (
            SessionDifficultyStore(max_entries=0, ttl_seconds=10.0),
            SessionDifficultyStore(max_entries=8, ttl_seconds=0.0),
        ):
            store.record(("default", "a"), Decimal("512"), now=now, share_backed=True)
            self.assertEqual(len(store), 0)
            self.assertEqual(
                store.lookup(("default", "a"), now=now), (None, "disabled")
            )

    def test_prune_drops_only_expired_entries(self) -> None:
        store = SessionDifficultyStore(max_entries=4, ttl_seconds=10.0)
        now = time.monotonic()
        store.record(("default", "old"), Decimal("512"), now=now, share_backed=True)
        store.record(
            ("default", "new"), Decimal("256"), now=now + 8, share_backed=True
        )

        self.assertEqual(store.prune(now=now + 11), 1)
        self.assertEqual(len(store), 1)
        self.assertEqual(
            store.lookup(("default", "new"), now=now + 11),
            (Decimal("256"), "hit"),
        )

    def test_unchanged_value_without_share_evidence_keeps_ageing(self) -> None:
        # The disconnect seam records on EVERY disconnect. If that refreshed
        # the timestamp, a session that resumes a retained value, submits
        # nothing and disconnects would keep the value alive forever on
        # reconnect cycles shorter than the TTL.
        store = SessionDifficultyStore(max_entries=4, ttl_seconds=10.0)
        now = time.monotonic()
        store.record(("default", "a"), Decimal("512"), now=now, share_backed=True)
        store.record(
            ("default", "a"), Decimal("512"), now=now + 8, share_backed=False
        )

        self.assertEqual(
            store.lookup(("default", "a"), now=now + 9), (Decimal("512"), "hit")
        )
        self.assertEqual(
            store.lookup(("default", "a"), now=now + 11), (None, "expired")
        )

    def test_ttl_refreshes_on_re_validation_only(self) -> None:
        store = SessionDifficultyStore(max_entries=4, ttl_seconds=10.0)
        now = time.monotonic()
        # An unchanged value backed by accepted shares was re-validated.
        store.record(("default", "a"), Decimal("512"), now=now, share_backed=True)
        store.record(
            ("default", "a"), Decimal("512"), now=now + 8, share_backed=True
        )
        self.assertEqual(
            store.lookup(("default", "a"), now=now + 17), (Decimal("512"), "hit")
        )
        # A value that actually moved is new information, share-backed or not
        # (an idle step-down carries no shares but does lower the value).
        store.record(("default", "b"), Decimal("512"), now=now, share_backed=True)
        store.record(
            ("default", "b"), Decimal("128"), now=now + 8, share_backed=False
        )
        self.assertEqual(
            store.lookup(("default", "b"), now=now + 17), (Decimal("128"), "hit")
        )

    def test_non_refreshing_record_still_moves_lru_recency(self) -> None:
        # Keeping the old timestamp must not turn record() into a no-op:
        # recency and the TTL stay independent, so the LRU bound keeps
        # evicting genuinely cold entries.
        store = SessionDifficultyStore(max_entries=2, ttl_seconds=100.0)
        now = time.monotonic()
        store.record(("default", "a"), Decimal("512"), now=now, share_backed=True)
        store.record(("default", "b"), Decimal("256"), now=now + 1, share_backed=True)
        store.record(("default", "a"), Decimal("512"), now=now + 2, share_backed=False)
        store.record(("default", "c"), Decimal("128"), now=now + 3, share_backed=True)

        self.assertEqual(store.lookup(("default", "b"), now=now + 3), (None, "miss"))
        self.assertEqual(
            store.lookup(("default", "a"), now=now + 3), (Decimal("512"), "hit")
        )

    def test_session_difficulty_key_requires_a_reserved_worker(self) -> None:
        state = SimpleNamespace(listener_name="highdiff", worker=None)
        self.assertIsNone(session_difficulty_key(state))  # type: ignore[arg-type]
        state.worker = worker_identity("miner-a.rig-1")
        self.assertEqual(
            session_difficulty_key(state),  # type: ignore[arg-type]
            ("highdiff", "miner-a.rig-1"),
        )


def resume_coordinator(**attributes: object):
    """A no-environment coordinator wired for authorize round trips.

    Resume attributes must land before the first vardiff-service touch, so
    they are applied here, ahead of any authorize call.
    """
    server = coordinator()
    for name, value in attributes.items():
        setattr(server, name, value)
    server.rpc = AddressValidationRpc()
    server.username_fallback_address = None
    server.maybe_send_job = lambda client, *, clean_jobs: True
    server.jobs = {}
    return server


def connect_client(
    server,
    connection_id: int,
    *,
    listener_name: str = "default",
    lane_config: vardiff.VardiffConfig | None = None,
) -> ClientState:
    """A freshly accepted connection sitting at its lane's start difficulty."""
    config = lane_config if lane_config is not None else standard_lane_config()
    state = ClientState(
        sock=object(),
        address=("127.0.0.1", connection_id),
        connection_id=connection_id,
        extranonce1_hex=f"{connection_id:08x}",
        listener_name=listener_name,
        listener_vardiff_config=config,
        share_difficulty=config.startup_difficulty,
    )
    state.subscribed = True
    state.send = lambda payload: None  # type: ignore[method-assign]
    state.close = lambda: None  # type: ignore[method-assign]
    return state


def authorize(server, state: ClientState, password: str = "x") -> None:
    server.handle_request(
        state,
        {
            "id": 1,
            "method": "mining.authorize",
            "params": [PAYOUT_ADDRESS, password],
        },
    )


def converge_and_disconnect(server, state: ClientState, difficulty: Decimal) -> None:
    with server._client_vardiff_lock(state):
        state.share_difficulty = difficulty
        state.pending_share_difficulty = None
    server.disconnect_client(state)


@contextmanager
def frozen_clock(now: float) -> Iterator[None]:
    """Run a reconnect cycle at an exact monotonic instant.

    Retention timestamps come from time.monotonic() inside the vardiff
    service, so the whole record/lookup path has to see the injected value.
    """
    with mock.patch.object(time, "monotonic", return_value=now):
        yield


def silent_reconnect_cycle(server, connection_id: int, now: float) -> ClientState:
    """Connect, authorize, submit nothing, disconnect -- all at ``now``."""
    with frozen_clock(now):
        state = connect_client(server, connection_id)
        authorize(server, state)
        server.disconnect_client(state)
    return state


def retained_difficulty(server, now: float | None = None) -> Decimal | None:
    store = server._ensure_vardiff_service().session_difficulty_store
    retained, _outcome = store.lookup(
        ("default", PAYOUT_ADDRESS),
        now=time.monotonic() if now is None else now,
    )
    return retained


def resume_outcomes(server) -> dict[str, int]:
    outcomes = server.vardiff_convergence_snapshot()["resume_outcomes"]
    assert isinstance(outcomes, dict)
    return outcomes


class ReconnectResumeTests(unittest.TestCase):
    def test_reconnect_resumes_last_converged_difficulty(self) -> None:
        server = resume_coordinator()
        first = connect_client(server, 1)
        authorize(server, first)
        self.assertEqual(first.share_difficulty, STANDARD_LANE_START)
        converge_and_disconnect(server, first, Decimal("262144"))

        second = connect_client(server, 2)
        authorize(server, second)

        self.assertEqual(second.share_difficulty, Decimal("262144"))
        self.assertIsNone(second.pending_share_difficulty)
        self.assertEqual(second.difficulty_generation, 1)
        outcomes = resume_outcomes(server)
        self.assertEqual(outcomes["resumed"], 1)
        # The very first authorize of the worker found nothing retained.
        self.assertEqual(outcomes["miss"], 1)

    def test_expired_retention_falls_back_to_lane_start(self) -> None:
        server = resume_coordinator(vardiff_resume_ttl_seconds=60.0)
        first = connect_client(server, 1)
        authorize(server, first)
        converge_and_disconnect(server, first, Decimal("262144"))

        second = connect_client(server, 2)
        frozen_now = time.monotonic() + 61.0
        with mock.patch.object(time, "monotonic", return_value=frozen_now):
            authorize(server, second)

        self.assertEqual(second.share_difficulty, STANDARD_LANE_START)
        self.assertEqual(resume_outcomes(server)["expired"], 1)

    def test_implausible_retained_value_is_clamped_to_the_lane_ceiling(self) -> None:
        server = resume_coordinator()
        service = server._ensure_vardiff_service()
        service.session_difficulty_store.record(
            ("default", PAYOUT_ADDRESS),
            Decimal("1e15"),
            now=time.monotonic(),
            share_backed=True,
        )

        state = connect_client(server, 1)
        authorize(server, state)

        # min(lane max, lane start * default 1024x factor).
        config = standard_lane_config()
        expected = min(
            config.max_difficulty,
            config.startup_difficulty * Decimal("1024"),
        )
        self.assertEqual(expected, Decimal("16777216"))
        self.assertEqual(state.share_difficulty, expected)
        self.assertNotEqual(state.share_difficulty, Decimal("1e15"))
        self.assertEqual(resume_outcomes(server)["clamped"], 1)

    def test_retention_is_scoped_per_lane(self) -> None:
        server = resume_coordinator()
        first = connect_client(
            server,
            1,
            listener_name="highdiff",
            lane_config=highdiff_vardiff_config(),
        )
        authorize(server, first)
        converge_and_disconnect(server, first, Decimal("2000000"))

        second = connect_client(server, 2, listener_name="default")
        authorize(server, second)

        # A lane switch is a miss that behaves exactly like today.
        self.assertEqual(second.share_difficulty, STANDARD_LANE_START)
        outcomes = resume_outcomes(server)
        self.assertEqual(outcomes["miss"], 2)
        self.assertEqual(outcomes["resumed"], 0)
        self.assertEqual(outcomes["clamped"], 0)

    def test_explicit_password_difficulty_outranks_the_resume(self) -> None:
        server = resume_coordinator()
        service = server._ensure_vardiff_service()
        service.session_difficulty_store.record(
            ("default", PAYOUT_ADDRESS),
            Decimal("262144"),
            now=time.monotonic(),
            share_backed=True,
        )

        state = connect_client(server, 1)
        authorize(server, state, password="d=32768")

        self.assertEqual(state.share_difficulty, Decimal("32768"))
        self.assertIsNone(state.pending_share_difficulty)

    def test_md_only_password_clamps_the_resumed_value(self) -> None:
        server = resume_coordinator()
        service = server._ensure_vardiff_service()
        service.session_difficulty_store.record(
            ("default", PAYOUT_ADDRESS),
            Decimal("262144"),
            now=time.monotonic(),
            share_backed=True,
        )

        state = connect_client(server, 1)
        authorize(server, state, password="md=524288")

        # The md= floor applies to the just-resumed value, not the lane start.
        self.assertEqual(state.share_difficulty, Decimal("524288"))
        self.assertIsNone(state.pending_share_difficulty)

    def test_reauthorize_of_a_live_session_never_re_resumes(self) -> None:
        server = resume_coordinator()
        state = connect_client(server, 1)
        authorize(server, state)
        with server._client_vardiff_lock(state):
            state.share_difficulty = Decimal("65536")
        service = server._ensure_vardiff_service()
        service.session_difficulty_store.record(
            ("default", PAYOUT_ADDRESS),
            Decimal("262144"),
            now=time.monotonic(),
            share_backed=True,
        )

        authorize(server, state)

        self.assertEqual(state.share_difficulty, Decimal("65536"))
        self.assertEqual(resume_outcomes(server)["resumed"], 0)

    def test_silent_reconnect_cycles_never_extend_the_retention_ttl(self) -> None:
        # The review repro: with the idle step-down sweep disabled (as
        # deployed), a session resumed above its true hashrate produces no
        # accepted shares and cannot step itself down. If each disconnect
        # re-stamped the entry, cycles shorter than the TTL would keep that
        # value adoptable forever and the short TTL would protect nothing.
        server = resume_coordinator(vardiff_resume_ttl_seconds=900.0)
        # The +900.0s cycle probes the TTL boundary exactly, where the store
        # keeps the entry (expiry is strictly age > TTL). A real time.
        # monotonic() reading makes that comparison depend on float
        # rounding: (start + 900.0) - start exceeds 900.0 for roughly one in
        # five thousand captured clock values, flipping the boundary cycle
        # to "expired" and failing the test. Every record and lookup below
        # runs under frozen_clock, so an exactly-representable constant
        # makes the boundary arithmetic exact instead of lucky.
        start = 1_000_000.0
        with frozen_clock(start):
            first = connect_client(server, 1)
            authorize(server, first)
            converge_and_disconnect(server, first, Decimal("262144"))

        # Three reconnect cycles inside the TTL, each submitting nothing.
        for index, offset in enumerate((300.0, 600.0, 900.0), start=2):
            state = silent_reconnect_cycle(server, index, start + offset)
            self.assertEqual(
                state.share_difficulty,
                Decimal("262144"),
                f"cycle at +{offset}s should still resume",
            )

        # The entry ages from the original convergence at `start`, not from
        # the last silent disconnect: it is gone one cycle later.
        with frozen_clock(start + 1200.0):
            last = connect_client(server, 5)
            authorize(server, last)

        self.assertEqual(last.share_difficulty, STANDARD_LANE_START)
        outcomes = resume_outcomes(server)
        self.assertEqual(outcomes["resumed"], 3)
        self.assertEqual(outcomes["expired"], 1)
        self.assertIsNone(retained_difficulty(server, now=start + 1200.0))

    def test_reconnect_with_an_accepted_share_refreshes_the_ttl(self) -> None:
        # The counterpart: an accepted share re-validates the retained value,
        # so its TTL restarts and the session keeps resuming.
        server = resume_coordinator(vardiff_resume_ttl_seconds=900.0)
        service = server._ensure_vardiff_service()
        start = time.monotonic()
        with frozen_clock(start):
            first = connect_client(server, 1)
            authorize(server, first)
            converge_and_disconnect(server, first, Decimal("262144"))

        with frozen_clock(start + 800.0):
            second = connect_client(server, 2)
            authorize(server, second)
            self.assertEqual(second.share_difficulty, Decimal("262144"))
            # Open the vardiff window at the injected instant so the share
            # below only marks the connection, leaving the retarget path
            # (covered separately) out of this test.
            with server._client_vardiff_lock(second):
                second.vardiff_window_started_monotonic = start + 800.0
            service.note_accepted(second, Decimal("262144"))
            self.assertTrue(second.vardiff_accepted_any)
            self.assertIsNone(second.pending_share_difficulty)
            server.disconnect_client(second)

        with frozen_clock(start + 1600.0):
            third = connect_client(server, 3)
            authorize(server, third)

        self.assertEqual(third.share_difficulty, Decimal("262144"))
        outcomes = resume_outcomes(server)
        self.assertEqual(outcomes["resumed"], 2)
        self.assertEqual(outcomes["expired"], 0)

    def test_disconnect_retains_the_delivered_difficulty_not_a_pending_one(
        self,
    ) -> None:
        # pending_share_difficulty is advertised with a FUTURE job, so a
        # disconnect racing an in-flight retarget must not retain a value the
        # miner was never given. A retarget that did land records itself at
        # its own commit point.
        server = resume_coordinator()
        state = connect_client(server, 1)
        authorize(server, state)
        with server._client_vardiff_lock(state):
            state.share_difficulty = Decimal("262144")
            state.pending_share_difficulty = Decimal("1048576")

        server.disconnect_client(state)

        self.assertEqual(retained_difficulty(server), Decimal("262144"))

    def test_committed_retarget_retains_the_new_difficulty(self) -> None:
        # The `if sent:` commit point inside retarget_locked: a delivered
        # retarget retains its own value, with share evidence taken from the
        # window that drove it.
        server = resume_coordinator()
        service = server._ensure_vardiff_service()
        state = connect_client(server, 1)
        authorize(server, state)
        recorded: list[tuple[Decimal, bool]] = []
        store = service.session_difficulty_store
        store_record = store.record

        def spy(key, difficulty, *, now, share_backed):  # type: ignore[no-untyped-def]
            recorded.append((difficulty, share_backed))
            store_record(key, difficulty, now=now, share_backed=share_backed)

        store.record = spy  # type: ignore[method-assign]

        # A share-driven window at four times the target share rate.
        server.retarget_client(
            state,
            current_difficulty=STANDARD_LANE_START,
            accepted_shares=80,
            submitted_shares=80,
            accepted_difficulty=STANDARD_LANE_START * 80,
            elapsed_seconds=Decimal("300"),
        )

        self.assertEqual(state.pending_share_difficulty, Decimal("65536"))
        self.assertEqual(recorded, [(Decimal("65536"), True)])
        self.assertEqual(retained_difficulty(server), Decimal("65536"))

        # A zero-share window steps back down; it carries no share evidence,
        # but it moves the value, so the store refreshes the TTL anyway.
        server.retarget_client(
            state,
            current_difficulty=Decimal("65536"),
            accepted_shares=0,
            submitted_shares=0,
            accepted_difficulty=Decimal("0"),
            elapsed_seconds=Decimal("300"),
        )

        self.assertEqual(recorded[-1], (STANDARD_LANE_START, False))
        self.assertEqual(retained_difficulty(server), STANDARD_LANE_START)

    def test_explicit_password_difficulty_counts_an_overridden_resume(self) -> None:
        server = resume_coordinator()
        service = server._ensure_vardiff_service()
        service.session_difficulty_store.record(
            ("default", PAYOUT_ADDRESS),
            Decimal("262144"),
            now=time.monotonic(),
            share_backed=True,
        )

        state = connect_client(server, 1)
        authorize(server, state, password="d=32768")

        self.assertEqual(state.share_difficulty, Decimal("32768"))
        outcomes = resume_outcomes(server)
        # resumed stays an attempt counter; overridden subtracts the ones an
        # explicit request replaced, so resumed + clamped - overridden is the
        # number of resumes that actually stuck.
        self.assertEqual(outcomes["resumed"], 1)
        self.assertEqual(outcomes["overridden"], 1)

    def test_a_resume_that_sticks_is_not_counted_as_overridden(self) -> None:
        server = resume_coordinator()
        service = server._ensure_vardiff_service()
        service.session_difficulty_store.record(
            ("default", PAYOUT_ADDRESS),
            Decimal("262144"),
            now=time.monotonic(),
            share_backed=True,
        )

        # No password request at all, then a request that resolves to exactly
        # the resumed value: neither supersedes the resume.
        first = connect_client(server, 1)
        authorize(server, first)
        second = connect_client(server, 2)
        authorize(server, second, password="d=262144")

        self.assertEqual(first.share_difficulty, Decimal("262144"))
        self.assertEqual(second.share_difficulty, Decimal("262144"))
        outcomes = resume_outcomes(server)
        self.assertEqual(outcomes["resumed"], 2)
        self.assertEqual(outcomes["overridden"], 0)

    def test_failed_username_reservation_neither_resumes_nor_retains(self) -> None:
        server = resume_coordinator()
        service = server._ensure_vardiff_service()
        service.session_difficulty_store.record(
            ("default", PAYOUT_ADDRESS),
            Decimal("262144"),
            now=time.monotonic(),
            share_backed=True,
        )
        server.reserve_client_username = lambda client, worker: False
        state = connect_client(server, 1)

        with self.assertRaises(StratumError):
            authorize(server, state)

        self.assertFalse(state.authorized)
        self.assertEqual(state.share_difficulty, STANDARD_LANE_START)
        outcomes = resume_outcomes(server)
        self.assertEqual(outcomes["resumed"], 0)
        self.assertEqual(outcomes["clamped"], 0)
        # No reserved worker means no retention identity, so the disconnect
        # that follows the rejection writes nothing either.
        server.disconnect_client(state)
        self.assertEqual(service.session_difficulty_store.snapshot()["records"], 1)
        self.assertEqual(retained_difficulty(server), Decimal("262144"))

    def test_kill_switch_restores_lane_start_reconnects(self) -> None:
        server = resume_coordinator(vardiff_resume_enabled=False)
        first = connect_client(server, 1)
        authorize(server, first)
        converge_and_disconnect(server, first, Decimal("262144"))

        second = connect_client(server, 2)
        authorize(server, second)

        self.assertEqual(second.share_difficulty, STANDARD_LANE_START)
        snapshot = server.vardiff_convergence_snapshot()
        self.assertEqual(snapshot["retained_sessions"], 0)
        outcomes = snapshot["resume_outcomes"]
        assert isinstance(outcomes, dict)
        self.assertEqual(outcomes["disabled"], 2)
        self.assertEqual(outcomes["resumed"], 0)


class ResumeConfigTests(unittest.TestCase):
    BASE_ENV = {
        "QBIT_RPC_HOST": "127.0.0.1",
        "QBIT_RPC_USER": "user",
        "QBIT_RPC_PASSWORD": "password",
        "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "1",
        "PRISM_ALLOW_TEST_SIGNING_SEEDS": "1",
    }

    def test_defaults_and_overrides(self) -> None:
        stratum = load_coordinator_config(self.BASE_ENV).stratum
        self.assertTrue(stratum.vardiff_resume_enabled)
        self.assertEqual(stratum.vardiff_resume_ttl_seconds, 900.0)
        self.assertEqual(stratum.vardiff_resume_max_entries, 8192)
        self.assertEqual(stratum.vardiff_resume_max_start_factor, Decimal("1024"))

        tuned = load_coordinator_config(
            {
                **self.BASE_ENV,
                "PRISM_STRATUM_VARDIFF_RESUME": "0",
                "PRISM_STRATUM_VARDIFF_RESUME_TTL_SECONDS": "120",
                "PRISM_STRATUM_VARDIFF_RESUME_MAX_ENTRIES": "16",
                "PRISM_STRATUM_VARDIFF_RESUME_MAX_START_FACTOR": "64",
            }
        ).stratum
        self.assertFalse(tuned.vardiff_resume_enabled)
        self.assertEqual(tuned.vardiff_resume_ttl_seconds, 120.0)
        self.assertEqual(tuned.vardiff_resume_max_entries, 16)
        self.assertEqual(tuned.vardiff_resume_max_start_factor, Decimal("64"))

    def test_env_sizes_the_coordinator_store_end_to_end(self) -> None:
        # env -> StratumConfig -> PrismCoordinator attributes ->
        # SessionDifficultyStore. The service reads those attributes through
        # getattr defaults, so a rename anywhere in the chain would silently
        # fall back to the defaults instead of failing; this asserts the
        # configured values actually reach the store.
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = {
                **self.BASE_ENV,
                "PRISM_ALLOW_MEMORY_LEDGER": "1",
                "PRISM_AUDIT_DIR": str(root),
                "PRISM_EVIDENCE_PATH": str(root / "evidence.json"),
                "PRISM_STRATUM_VARDIFF_RESUME_TTL_SECONDS": "123",
                "PRISM_STRATUM_VARDIFF_RESUME_MAX_ENTRIES": "17",
            }
            config = load_coordinator_config(source)
            with mock.patch.object(
                JsonRpc, "call", side_effect=RuntimeError("offline")
            ):
                server = PrismCoordinator(config)

        self.assertEqual(server.vardiff_resume_ttl_seconds, 123.0)
        self.assertEqual(server.vardiff_resume_max_entries, 17)
        store = server._ensure_vardiff_service().session_difficulty_store
        self.assertEqual(store.ttl_seconds, 123.0)
        self.assertEqual(store.max_entries, 17)
        self.assertTrue(store.enabled)

    def test_kill_switch_env_disables_the_coordinator_store(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = {
                **self.BASE_ENV,
                "PRISM_ALLOW_MEMORY_LEDGER": "1",
                "PRISM_AUDIT_DIR": str(root),
                "PRISM_EVIDENCE_PATH": str(root / "evidence.json"),
                "PRISM_STRATUM_VARDIFF_RESUME": "0",
            }
            config = load_coordinator_config(source)
            with mock.patch.object(
                JsonRpc, "call", side_effect=RuntimeError("offline")
            ):
                server = PrismCoordinator(config)

        self.assertFalse(server.vardiff_resume_enabled)
        store = server._ensure_vardiff_service().session_difficulty_store
        self.assertEqual(store.max_entries, 0)
        self.assertFalse(store.enabled)

    def test_start_factor_below_one_fails_startup(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            load_coordinator_config(
                {
                    **self.BASE_ENV,
                    "PRISM_STRATUM_VARDIFF_RESUME_MAX_START_FACTOR": "0.5",
                }
            )
        self.assertEqual(
            str(raised.exception),
            "PRISM_STRATUM_VARDIFF_RESUME_MAX_START_FACTOR must be at least 1",
        )


class ServiceRuntime:
    """Lightweight VardiffRuntime fake mirroring test_prism_vardiff_service."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        # A list rather than a set: SimpleNamespace clients are unhashable,
        # and convergence_snapshot only needs an iterable membership view.
        self.clients: list[object] = []
        self.stop_event = threading.Event()
        self.vardiff_config = standard_lane_config()
        self.share_difficulty = STANDARD_LANE_START
        self.vardiff_idle_sweep_seconds = 1.0


def service_client(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = dict(
        vardiff_config=None,
        listener_vardiff_config=None,
        listener_name="default",
        pending_share_difficulty=None,
        share_difficulty=STANDARD_LANE_START,
        vardiff_window_started_monotonic=time.monotonic(),
        vardiff_window_accepted=0,
        vardiff_window_submitted=0,
        vardiff_window_work=Decimal("0"),
        vardiff_difficulty_estimate=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


class ConvergenceObservabilityTests(unittest.TestCase):
    def test_snapshot_counts_lanes_ceiling_sessions_and_retention(self) -> None:
        runtime = ServiceRuntime()
        service = VardiffService(runtime)  # type: ignore[arg-type]
        at_ceiling = service_client(
            share_difficulty=runtime.vardiff_config.max_difficulty,
            listener_name="highdiff",
        )
        converging = service_client()
        runtime.clients.extend([at_ceiling, converging])

        service.note_accepted(at_ceiling, Decimal("4"))  # type: ignore[arg-type]
        service.note_accepted(converging, Decimal("4"))  # type: ignore[arg-type]
        service.note_accepted(converging, Decimal("4"))  # type: ignore[arg-type]
        service.session_difficulty_store.record(
            ("default", "miner-a"),
            Decimal("262144"),
            now=time.monotonic(),
            share_backed=True,
        )

        snapshot = service.convergence_snapshot()

        self.assertEqual(snapshot["sessions_at_max_difficulty"], 1)
        self.assertEqual(
            snapshot["lane_accepted_shares"], {"highdiff": 1, "default": 2}
        )
        self.assertEqual(snapshot["retained_sessions"], 1)
        self.assertEqual(
            snapshot["resume_outcomes"],
            {outcome: 0 for outcome in PRISM_VARDIFF_RESUME_OUTCOMES},
        )

    def test_lane_accepted_counts_where_vardiff_is_disabled(self) -> None:
        runtime = ServiceRuntime()
        runtime.vardiff_config = vardiff.VardiffConfig(
            enabled=False,
            target_share_interval_seconds=Decimal("15"),
            min_difficulty=Decimal("1024"),
            max_difficulty=Decimal("4294967296"),
            retarget_interval_seconds=Decimal("300"),
            max_step_factor=Decimal("4"),
            startup_difficulty=STANDARD_LANE_START,
        )
        service = VardiffService(runtime)  # type: ignore[arg-type]

        service.note_accepted(service_client(), Decimal("4"))  # type: ignore[arg-type]

        snapshot = service.convergence_snapshot()
        self.assertEqual(snapshot["lane_accepted_shares"], {"default": 1})
        self.assertEqual(snapshot["sessions_at_max_difficulty"], 0)

    def test_renderer_emits_deterministic_lane_and_outcome_series(self) -> None:
        started_monotonic = time.monotonic()
        port = SimpleNamespace(
            vardiff_convergence_snapshot=lambda: {
                "sessions_at_max_difficulty": 2,
                "lane_accepted_shares": {"default": 30, "extra": 5},
                "resume_outcomes": {"resumed": 4},
                "retained_sessions": 3,
            },
            listener_profiles=[
                SimpleNamespace(name="default"),
                SimpleNamespace(name="highdiff"),
            ],
            prometheus_label_value=lambda value: value,
            started_monotonic=started_monotonic,
        )
        renderer = MetricsRenderer(port)  # type: ignore[arg-type]

        with mock.patch.object(
            time, "monotonic", return_value=started_monotonic + 10.0
        ):
            lines = renderer.vardiff_convergence_metrics_lines()
        text = "\n".join(lines)

        self.assertIn("qbit_prism_vardiff_sessions_at_max_difficulty 2", text)
        self.assertIn(
            'qbit_prism_vardiff_lane_accepted_shares_total{lane="default"} 30',
            text,
        )
        self.assertIn(
            'qbit_prism_vardiff_lane_accepted_shares_total{lane="highdiff"} 0',
            text,
        )
        self.assertIn(
            'qbit_prism_vardiff_lane_accepted_shares_total{lane="extra"} 5',
            text,
        )
        self.assertIn(
            'qbit_prism_vardiff_lane_accepted_shares_per_second{lane="default"} 3',
            text,
        )
        for outcome in PRISM_VARDIFF_RESUME_OUTCOMES:
            self.assertIn(
                f'qbit_prism_vardiff_resume_total{{outcome="{outcome}"}}',
                text,
            )
        self.assertIn('qbit_prism_vardiff_resume_total{outcome="resumed"} 4', text)
        self.assertIn("qbit_prism_vardiff_resume_retained_sessions 3", text)
        # Configured lanes lead in profile order; observed-only lanes follow.
        default_index = text.index('shares_total{lane="default"}')
        highdiff_index = text.index('shares_total{lane="highdiff"}')
        extra_index = text.index('shares_total{lane="extra"}')
        self.assertLess(default_index, highdiff_index)
        self.assertLess(highdiff_index, extra_index)


if __name__ == "__main__":
    unittest.main()

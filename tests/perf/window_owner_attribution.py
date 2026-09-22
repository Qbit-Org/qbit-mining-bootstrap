#!/usr/bin/env python3
"""Long payout-window advance replay with owner attribution (#332, defect 4).

Drives the real coordinator through many payout-window advances with the
in-process fake daemon: byte-changing and anchor-only advances, periodic
self-checks, template changes, shared bundle builds, first-job delivery to
fake clients with connect/disconnect churn and cancelled first-job requests,
and the consumers that force a parse (durable candidate intent, audit compact
walks). GC stays enabled and is never forced.

After every cycle the weak ownership registry is read. The run fails if
owners or distinct canonical buffers exceed the declared bound, or if any
parsed records remain live once the cycle's synchronous consumers returned,
and it prints the kind, creation site, age and referrer
chain of every owner that outlives its mirror by more than ``--grace``
cycles, so a holder is attributable offline without a heap census in
production. This is a local replay, not production qualification: it has no
PostgreSQL, Rust daemon, Stratum sockets or block landings.
"""

from __future__ import annotations

import argparse
import gc
import os
import sys
import time
import weakref
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
os.environ.setdefault("PRISM_WINDOW_PIPELINE_RUST", "1")
os.environ.setdefault("GIT_CONFIG_GLOBAL", "/dev/null")

from unittest.mock import patch  # noqa: E402

from lab.prism import window_ownership  # noqa: E402
from lab.prism.bundle_compiler import _compact_share_payload  # noqa: E402
from lab.prism.candidate_codec import prepare_candidate_intent  # noqa: E402
from lab.prism.job_delivery import (  # noqa: E402
    DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS as PRUNE_INTERVAL,
)
from tests import prism_coordinator_test_support as support  # noqa: E402
from tests import test_prism_payout_window_daemon_recenter as recenter  # noqa: E402
from tests.test_prism_candidate_codec import intent_for  # noqa: E402

# Declared ownership bound. Live state is the mirror, its sequence and the
# fake daemon's own page-backed window (three owners, one distinct buffer).
# Permitted job history is every superseded bundle whose contexts are still
# in the job cache or the evicted-job graveyard: one distinct sequence per
# cycle for as long as the same-tip retention TTL keeps them, and the
# graveyard prunes at most once per prune interval, so an entry can outlive
# its TTL by that interval. The bound therefore scales with
# (retention + prune interval) / cycle pacing, plus slack for the in-flight
# bundle and the armed artifact.
LIVE_OWNERS = 3
# Parsed rows may live only inside a consumer; none is in flight between
# cycles, so the bound after each cycle is exactly zero.
PARSED_RECORDS_BOUND = 0


def declared_bounds(retention_seconds: float, cycle_seconds: float) -> tuple[int, int]:
    history = int((retention_seconds + PRUNE_INTERVAL) / cycle_seconds) + 1
    owners = LIVE_OWNERS + history + 4
    return owners, owners - 1


class FakeSocket:
    """Enough of a socket for delivery, shutdown and close."""

    def settimeout(self, _timeout: object) -> None:
        return

    def setsockopt(self, *_args: object) -> None:
        return

    def shutdown(self, _how: object) -> None:
        return

    def close(self) -> None:
        return


def describe(value: object) -> str:
    kind = type(value)
    name = f"{kind.__module__}.{kind.__qualname__}"
    if isinstance(value, dict):
        return f"dict(keys={list(value)[:6]!r})"
    if isinstance(value, (list, tuple, set, frozenset)):
        return f"{name}(len={len(value)})"
    return name


def referrer_chain(target: object, depth: int, seen: set[int], own_frame: object) -> list[str]:
    lines: list[str] = []
    referrers = [
        item for item in gc.get_referrers(target)
        if item is not own_frame and id(item) not in seen
        and type(item).__name__ not in ("frame", "list_iterator", "cell")
    ]
    for item in referrers[:5]:
        seen.add(id(item))
        label = describe(item)
        owner = item
        if isinstance(item, dict):
            for candidate in gc.get_referrers(item):
                if getattr(candidate, "__dict__", None) is item:
                    keys = [key for key, value in item.items() if value is target]
                    label = f"{describe(candidate)}.{keys}"
                    owner = candidate
                    break
        lines.append("    " * (4 - depth) + f"<- {label}")
        if depth > 1:
            lines.extend(referrer_chain(owner, depth - 1, seen, own_frame))
    return lines


def run(cycles: int, clients: int, grace: int, report: int, *,
        cycle_seconds: float, retention_seconds: float) -> int:
    owner_bound, buffer_bound = declared_bounds(retention_seconds, cycle_seconds)
    print(f"declared bounds: owners<={owner_bound} buffers<={buffer_bound} "
          f"(retention {retention_seconds}s, cycle pacing {cycle_seconds}s)")
    fixture = recenter.DaemonRecenterTests()
    fixture.setUp()
    server, ledger, artifacts, daemon = fixture._server()
    support.install_fake_bundle_builder(server)
    difficulty = int(artifacts.network_difficulty)
    server.payout_artifact_min_build_interval_seconds = 0.0
    server.payout_artifact_full_rescan_seconds = 3_600.0
    server.send_job_update = lambda client, job: None  # type: ignore[method-assign]
    # Job retention TTLs are wall-clock terms; a replay cycle is milliseconds,
    # so a short retention keeps the graveyard turning over as production's
    # does over minutes.
    server.same_tip_job_retention_seconds = retention_seconds
    server.stale_grace_seconds = retention_seconds
    clock = [1_000_000]
    worker = support.worker()
    pool: list[object] = []
    next_connection = [1]
    births: dict[int, tuple[weakref.ref, int]] = {}
    failures: list[str] = []
    peak = dict(owners=0, canonical_buffers=0, parsed_records=0)
    delivery_enabled = True

    def connect() -> object:
        client = support.client(next_connection[0], worker)
        next_connection[0] += 1
        client.sock = FakeSocket()
        with server.lock:
            server.clients.add(client)
        pool.append(client)
        return client

    def deliver(client: object, *, cancel: bool = False) -> None:
        nonlocal delivery_enabled
        if not delivery_enabled:
            return
        try:
            server.schedule_initial_job(client)
            if cancel:
                server.cancel_initial_job_delivery(client)
            deadline = time.monotonic() + 10
            while client in server.pending_initial_jobs and time.monotonic() < deadline:
                time.sleep(0.001)
        except Exception as error:  # pragma: no cover - replay diagnostics
            delivery_enabled = False
            print(f"first-job delivery disabled for this replay: {error!r}")

    def disconnect(client: object) -> None:
        pool.remove(client)
        server.disconnect_client(client)

    def owners_now() -> list[tuple[int, str, str, object]]:
        found = []
        for key, entry in list(window_ownership._OWNERS.items()):
            owner = entry[0]()
            if owner is not None:
                found.append((key, entry[2], entry[5], owner))
        return found

    height = 10
    started = time.monotonic()
    with patch("lab.prism.prism_coordinator.now_ms", side_effect=lambda: clock[0]):
        for _ in range(clients):
            connect()
        for cycle in range(cycles):
            clock[0] += 1_000
            time.sleep(cycle_seconds)
            if cycle % 4 != 3:
                # Byte-changing advance; every fourth cycle is anchor-only.
                recenter._append_heavy_share(
                    ledger, share_seq=21 + cycle, accepted_at_ms=clock[0] - 5,
                    share_difficulty=difficulty,
                )
            if cycle % 50 == 49:
                server.payout_artifact_full_rescan_seconds = 0.0
            # The production preparation loop's entry: build, then arm the
            # artifact bundle builds reuse.
            server._prepare_payout_ledger_artifact(0, difficulty)
            server.payout_artifact_full_rescan_seconds = 3_600.0
            artifact = server._payout_ledger_artifact
            if cycle % 3 == 0:
                height += 1
                artifacts = server.store_template_artifacts(
                    dict(support.base_template(height=height, prevhash=f"{cycle + 1:064x}"))
                )
            bundle = server.build_shared_job_bundle(artifacts, worker)
            for index, client in enumerate(tuple(pool)):
                deliver(client, cancel=(cycle + index) % 11 == 0)
            if cycle % 7 == 6 and pool:
                disconnect(pool[cycle % len(pool)])
                deliver(connect())
            if cycle % 5 == 4:
                # The parse-forcing consumers: a durable intent stages bytes
                # without a parse; the compact audit walk parses and releases.
                prepared = prepare_candidate_intent(intent_for(0, shares_json=bundle.shares_json))
                prepared.body.write_chunks(lambda chunk: None)
                _compact_share_payload(bundle.shares_json)
                del prepared
            del bundle, artifact
            for key, kind, site, owner in owners_now():
                birth = births.get(key)
                if birth is None or birth[0]() is not owner:
                    births[key] = (weakref.ref(owner), cycle)
                del owner
            counts = window_ownership.window_ownership_snapshot()
            for name in peak:
                peak[name] = max(peak[name], counts[name])
            if counts["owners"] > owner_bound or counts["canonical_buffers"] > buffer_bound:
                failures.append(f"cycle {cycle}: owners={counts['owners']} buffers={counts['canonical_buffers']} exceed the declared bound")
            if counts["parsed_records"] != PARSED_RECORDS_BOUND:
                # Every parse-forcing consumer in this workload is synchronous
                # and finished before this read, so any live parsed rows are
                # a retained holder, whatever the owner count says.
                failures.append(f"cycle {cycle}: parsed_records={counts['parsed_records']} remain live after every consumer returned")
            if cycle % report == 0 or cycle == cycles - 1:
                breakdown = window_ownership.window_ownership_breakdown()
                oldest = {kind: round(age, 1) for kind, age in breakdown["oldest_owner_age_seconds"].items()}
                print(f"cycle {cycle:5d} owners={counts['owners']} buffers={counts['canonical_buffers']} "
                      f"bytes={counts['canonical_bytes']} parsed={counts['parsed_records']} "
                      f"clients={len(pool)} jobs={len(server.jobs)} graveyard={len(server.evicted_job_graveyard)} "
                      f"oldest={oldest}")
        elapsed = time.monotonic() - started
        print(f"elapsed {elapsed:.1f}s; peak {peak}")
        print("owners by (kind, site):")
        for (kind, site), count in sorted(window_ownership.window_ownership_breakdown()["owners"].items()):
            print(f"  {kind:9s} {site:60s} {count}")
        stale = []
        for key, kind, site, owner in owners_now():
            birth = births.get(key)
            born = birth[1] if birth and birth[0]() is owner else cycles
            if cycles - born > grace:
                stale.append((kind, site, born, owner))
            del owner
        if stale:
            print(f"{len(stale)} owner(s) outlived their mirror by more than {grace} cycles:")
            for kind, site, born, owner in stale:
                print(f"--- {kind} minted at {site}, born cycle {born}")
                for line in referrer_chain(owner, 4, set(), sys._getframe()):
                    print(line)
            del stale
        for client in tuple(pool):
            disconnect(client)
        fixture.tearDown()
    for failure in failures[:5]:
        print("FAIL", failure)
    return 1 if failures else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycles", type=int, default=1_500)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--grace", type=int, default=10)
    parser.add_argument("--report", type=int, default=100)
    parser.add_argument("--cycle-seconds", type=float, default=0.02,
                        help="pacing per advance; retention TTLs are wall-clock terms")
    parser.add_argument("--retention-seconds", type=float, default=0.2,
                        help="same-tip and stale-grace job retention for the replay")
    arguments = parser.parse_args()
    sys.exit(run(arguments.cycles, arguments.clients, arguments.grace, arguments.report,
                 cycle_seconds=arguments.cycle_seconds,
                 retention_seconds=arguments.retention_seconds))

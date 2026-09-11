#!/usr/bin/env python3
"""Immutable qbit template observations and their derived artifacts.

The repository owns the coordinator's template-artifact cache and observation
generations.  It does not import ``prism_coordinator``: every cross-domain
fact (tip publication, refresh scheduling, RPC access, cache-event metrics)
is reached through the :class:`TemplateArtifactRuntime` port, resolved at
call time so the historical coordinator monkeypatch seams keep working.
"""

from __future__ import annotations

import copy
from collections import OrderedDict
from dataclasses import dataclass, field
import hashlib
import json
import threading
import time
from typing import Any, Callable, Mapping, Protocol, Sequence

from lab.prism import direct_stratum
from lab.prism.coordinator_config import DEFAULT_PRISM_BLOCKPOLL_SECONDS
# The refresh/publication exception family is owned by the payout-state
# module (its final owner); re-imported here so every existing import path
# keeps working.
from lab.prism.payout_state import (
    PayoutStatePublicationBlocked,  # noqa: F401 - compatibility re-export
    TemplateRefreshBlocked,  # noqa: F401 - compatibility re-export
    TemplateRefreshSuperseded,
)


PRISM_TEMPLATE_FINGERPRINT_VOLATILE_KEYS = frozenset(
    {
        # qbit can legitimately advance these without making already issued
        # jobs stale. Rebuilding every miner job for clock-only changes would
        # turn the poller into continuous audit-bundle churn.
        "curtime",
        "longpollid",
        "mintime",
    }
)


def qbit_template_fingerprint(template: dict[str, Any]) -> str:
    stable_template = {
        key: value
        for key, value in template.items()
        if key not in PRISM_TEMPLATE_FINGERPRINT_VOLATILE_KEYS
    }
    encoded = json.dumps(
        stable_template,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def qbit_gbt_rules(chain: str) -> list[str]:
    rules = ["segwit"]
    if chain.strip().lower() == "signet":
        rules.append("signet")
    return rules


def target_from_compact(bits_hex: str) -> int:
    return direct_stratum.target_from_compact_hex(bits_hex)


def scaled_target_difficulty(target: int) -> int:
    if target <= 0:
        raise ValueError("target must be positive")
    pow_limit_target = target_from_compact("207fffff")
    return max(1, (pow_limit_target * 1_000_000) // target)


def scaled_network_difficulty(bits_hex: str) -> int:
    template_target = target_from_compact(bits_hex)
    return scaled_target_difficulty(template_target)


class FrozenJsonDict(dict[str, Any]):
    """JSON-compatible mapping that fails closed on mutation."""

    __slots__ = ()

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("template artifact JSON is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable

    def __copy__(self) -> "FrozenJsonDict":
        return self

    def __deepcopy__(self, _memo: dict[int, object]) -> "FrozenJsonDict":
        return self


class FrozenJsonList(list[Any]):
    """JSON-compatible sequence that fails closed on mutation."""

    __slots__ = ()

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("template artifact JSON is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable

    def __copy__(self) -> "FrozenJsonList":
        return self

    def __deepcopy__(self, _memo: dict[int, object]) -> "FrozenJsonList":
        return self


def freeze_json(value: Any) -> Any:
    """Detach and recursively freeze a JSON-like value without changing shape."""
    if isinstance(value, (FrozenJsonDict, FrozenJsonList)):
        return value
    if isinstance(value, Mapping):
        frozen_dict = FrozenJsonDict()
        dict.update(
            frozen_dict,
            ((str(key), freeze_json(item)) for key, item in value.items()),
        )
        return frozen_dict
    if isinstance(value, (list, tuple)):
        frozen_list = FrozenJsonList()
        list.extend(frozen_list, (freeze_json(item) for item in value))
        return frozen_list
    return value


def freeze_json_rows(rows: Sequence[Mapping[str, Any]]) -> FrozenJsonList:
    frozen = freeze_json(rows)
    if not isinstance(frozen, FrozenJsonList):
        raise TypeError("JSON rows must be a sequence")
    return frozen


@dataclass(frozen=True)
class CachedTemplateArtifacts:
    """Template plus everything derivable from it alone, shared by all clients.

    Derived fields are keyed by the template fingerprint: a refetch whose
    fingerprint matches (only clock fields moved) reuses the previously
    computed transaction hexes and witness merkle leaves instead of re-hashing
    the full template. Generation records observation-start order so a slow,
    older fetch cannot supersede a newer observation merely by finishing last.
    """

    template: dict[str, Any]
    fingerprint: str
    previousblockhash: str
    transaction_hexes: tuple[str, ...]
    witness_merkle_leaves_hex: tuple[str, ...]
    network_difficulty: int
    fetched_monotonic: float
    generation: int = 0


@dataclass(frozen=True)
class QbitTipTemplateSnapshot:
    bestblockhash: str
    previousblockhash: str
    template_fingerprint: str
    template_generation: int = 0
    # The observation owns this exact artifact object.  It deliberately does
    # not participate in snapshot equality: callers compare the stable
    # identity fields above, while refresh preparation consumes the exact
    # template and derivations that were observed even if the mutable cache's
    # current pointer is replaced concurrently.
    template_artifacts: CachedTemplateArtifacts | None = field(
        default=None,
        compare=False,
        repr=False,
    )


class TemplateArtifactRuntime(Protocol):
    """Typed port over the coordinator, resolved at call time.

    The repository reaches every cross-domain fact through this object so
    instance-level monkeypatches on the coordinator (the historical test
    seams) keep intercepting exactly as before the extraction.
    """

    rpc: Any
    lock: Any

    def _ensure_job_cache_state(self) -> None: ...

    def _ensure_tip_refresh_state(self) -> None: ...

    def _job_build_phases(self) -> dict[str, float]: ...

    def _record_job_cache_event(self, kind: str, *, hit: bool) -> None: ...

    def _newest_observed_tip_locked(self) -> str | None: ...

    def _published_tip_authoritative_locked(self, now: float) -> bool: ...

    def _cancel_obsolete_job_builds(
        self,
        reason: str,
        *,
        keep_published_snapshot: bool = False,
    ) -> None: ...

    def observe_tip_for_refresh(self, tip_hash: str) -> object: ...

    def _schedule_tip_refresh_retry(self) -> None: ...

    def store_template_artifacts(
        self,
        template: dict[str, Any],
        *,
        generation: int | None = None,
    ) -> CachedTemplateArtifacts | None: ...

    def current_template_artifacts(self) -> CachedTemplateArtifacts: ...


class TemplateArtifactRepository:
    """Sole owner of the template-artifact cache and observation generations.

    The repository deliberately shares the job-bundle cache lock: template
    currency and job-bundle admission were historically guarded by one
    ``_job_cache_lock`` and remaining coordinator domains still take that
    lock around combined reads.
    """

    def __init__(
        self,
        runtime: TemplateArtifactRuntime,
        *,
        lock: threading.Lock,
    ) -> None:
        self._runtime = runtime
        self._job_cache_lock = lock
        self._template_artifacts: CachedTemplateArtifacts | None = None
        self._template_artifact_generation = 0
        # Whether the generation counter was ever set explicitly (test
        # embedders seed it through the coordinator compatibility field).
        self._generation_explicit = False

    # -- compatibility adoption -------------------------------------------

    def adopt_template_artifacts(
        self,
        artifacts: CachedTemplateArtifacts | None,
    ) -> None:
        """Adopt a directly assigned artifact object (legacy test seeding).

        Mirrors the historical first-touch initializer: a test that plants
        ``_template_artifacts`` before touching the generation counter also
        seeded the counter from the planted artifacts.
        """
        self._template_artifacts = artifacts
        if artifacts is not None and not self._generation_explicit:
            self._template_artifact_generation = max(
                self._template_artifact_generation,
                int(getattr(artifacts, "generation", 0)),
            )

    def adopt_template_artifact_generation(self, generation: int) -> None:
        self._template_artifact_generation = generation
        self._generation_explicit = True

    # -- owner methods (bodies moved verbatim from the coordinator) --------

    def reserve_generation(self) -> int:
        """Reserve template ordering when a fetch starts, not when it finishes."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with self._job_cache_lock:
            self._template_artifact_generation += 1
            return self._template_artifact_generation

    def derive(
        self,
        template: dict[str, Any],
        *,
        generation: int,
    ) -> CachedTemplateArtifacts:
        runtime = self._runtime
        # Detach the observation from mutable RPC/test caller state. The
        # dataclass then owns this exact tree for the lifetime of its snapshot.
        template = copy.deepcopy(template)
        fingerprint = qbit_template_fingerprint(template)
        with self._job_cache_lock:
            previous = self._template_artifacts
        if previous is not None and previous.fingerprint == fingerprint:
            return CachedTemplateArtifacts(
                template=template,
                fingerprint=fingerprint,
                previousblockhash=str(template.get("previousblockhash", "")),
                transaction_hexes=previous.transaction_hexes,
                witness_merkle_leaves_hex=previous.witness_merkle_leaves_hex,
                network_difficulty=previous.network_difficulty,
                fetched_monotonic=time.monotonic(),
                generation=generation,
            )
        phases = runtime._job_build_phases()
        started = time.monotonic()
        transaction_hexes = direct_stratum.transaction_hexes_from_template(template)
        witness_leaves = tuple(direct_stratum.witness_merkle_leaves_hex(transaction_hexes))
        network_difficulty = scaled_network_difficulty(str(template["bits"]))
        phases["merkle"] = phases.get("merkle", 0.0) + (time.monotonic() - started)
        return CachedTemplateArtifacts(
            template=template,
            fingerprint=fingerprint,
            previousblockhash=str(template.get("previousblockhash", "")),
            transaction_hexes=transaction_hexes,
            witness_merkle_leaves_hex=witness_leaves,
            network_difficulty=network_difficulty,
            fetched_monotonic=time.monotonic(),
            generation=generation,
        )

    def store_artifacts(self, artifacts: CachedTemplateArtifacts) -> bool:
        runtime = self._runtime
        changed = False
        with self._job_cache_lock:
            previous = self._template_artifacts
            if previous is not None and artifacts.generation < previous.generation:
                return False
            self._template_artifacts = artifacts
            if previous is not None and previous.fingerprint != artifacts.fingerprint:
                changed = True
                # Retain the published snapshot's bundles alongside the new
                # fingerprint: until the replacement tip is published, direct
                # issuance still serves published-snapshot work, and pruning
                # it here would force those paths to defer for the whole
                # detected-but-unpublished window. Publication moves the
                # snapshot, so the next fingerprint change drops the old
                # entries.
                keep_fingerprints = {artifacts.fingerprint}
                with runtime.lock:
                    published_snapshot = getattr(runtime, "tip_template_snapshot", None)
                if published_snapshot is not None:
                    keep_fingerprints.add(published_snapshot.template_fingerprint)
                runtime._job_bundle_cache = OrderedDict(
                    (key, entry)
                    for key, entry in runtime._job_bundle_cache.items()
                    if entry.template_fingerprint in keep_fingerprints
                )
        if changed:
            runtime._cancel_obsolete_job_builds(
                "template fingerprint superseded",
                keep_published_snapshot=True,
            )
        return True

    def store(
        self,
        template: dict[str, Any],
        *,
        generation: int | None = None,
    ) -> CachedTemplateArtifacts | None:
        """Best-effort cache fill from an already-fetched template (blockpoll).

        Returns None instead of raising so a template the derivation cannot
        digest degrades to the legacy per-build fetch path rather than failing
        the poll. The returned artifacts describe this exact observation even
        if a newer observation already won the cache-write race; blockpoll then
        detects the mismatch before fanout.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        if generation is None:
            generation = runtime._reserve_template_artifact_generation()
        try:
            artifacts = runtime._derive_template_artifacts(
                template,
                generation=generation,
            )
        except Exception:
            return None
        runtime._store_template_artifacts(artifacts)
        return artifacts

    def issuance(self) -> CachedTemplateArtifacts:
        """Template artifacts for direct (non-refresh) job issuance.

        While a detected replacement tip is still unpublished, share
        classification remains anchored to the published tip. Direct issuance
        paths (initial delivery retries, Vardiff retargets, reauthorization)
        must therefore keep handing out published-snapshot work; issuing
        detected-tip work early would have every one of its shares rejected
        as stale until the refresh publishes. Once the published authority
        lapses (divergence lease expired), issuance falls through to the live
        template exactly like submit classification falls back to the live
        RPC read.
        """
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            published = getattr(runtime, "current_tip_first_seen", None)
            latest_detected = getattr(runtime, "latest_detected_tip", None)
            published_snapshot = getattr(runtime, "tip_template_snapshot", None)
            pinned = bool(
                published is not None
                and latest_detected is not None
                and latest_detected[0] != published[0]
                and published_snapshot is not None
                and published_snapshot.bestblockhash == published[0]
                and published_snapshot.template_artifacts is not None
                and runtime._published_tip_authoritative_locked(time.monotonic())
            )
        if pinned:
            assert published_snapshot is not None
            assert published_snapshot.template_artifacts is not None
            return published_snapshot.template_artifacts
        artifacts = runtime.current_template_artifacts()
        # The fetch above may itself be the first observation of a newer tip
        # (a template-cache miss racing ahead of blockpoll/blockwait). The
        # published tip still owns share classification, so serve its
        # snapshot; the recorded detection has already armed the refresh.
        with runtime.lock:
            published = getattr(runtime, "current_tip_first_seen", None)
            published_snapshot = getattr(runtime, "tip_template_snapshot", None)
            repinned = bool(
                published is not None
                and artifacts.previousblockhash != published[0]
                and published_snapshot is not None
                and published_snapshot.bestblockhash == published[0]
                and published_snapshot.template_artifacts is not None
                and runtime._published_tip_authoritative_locked(time.monotonic())
            )
        if repinned:
            assert published_snapshot is not None
            assert published_snapshot.template_artifacts is not None
            return published_snapshot.template_artifacts
        return artifacts

    def current(self) -> CachedTemplateArtifacts:
        """Return fresh template artifacts, fetching a template on cache miss."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        ttl = getattr(runtime, "template_cache_seconds", DEFAULT_PRISM_BLOCKPOLL_SECONDS)
        now = time.monotonic()
        with self._job_cache_lock:
            cached = self._template_artifacts
        with runtime.lock:
            observed_tip = runtime._newest_observed_tip_locked()
        cached_tip_current = (
            observed_tip is None
            or cached is None
            or cached.previousblockhash == observed_tip
        )
        if (
            cached is not None
            and cached_tip_current
            and ttl > 0
            and now - cached.fetched_monotonic <= ttl
        ):
            runtime._record_job_cache_event("template", hit=True)
            return cached
        runtime._record_job_cache_event("template", hit=False)
        generation = runtime._reserve_template_artifact_generation()
        phases = runtime._job_build_phases()
        started = time.monotonic()
        template = runtime.rpc.call(
            "getblocktemplate",
            [{"rules": qbit_gbt_rules(getattr(runtime, "qbit_chain", "regtest"))}],
        )
        if not isinstance(template, dict):
            raise RuntimeError("getblocktemplate returned non-object")
        phases["template"] = phases.get("template", 0.0) + (time.monotonic() - started)
        artifacts = runtime._derive_template_artifacts(
            template,
            generation=generation,
        )
        if runtime._store_template_artifacts(artifacts):
            # A direct fetch can be the first reader to see qbit advance.
            # Record it as a detection like every other live-tip observation,
            # or the buildability gates would treat the newer parent as
            # unknown while pinned issuance still waits on blockpoll.
            runtime.observe_tip_for_refresh(artifacts.previousblockhash)
            return artifacts
        # A later fetch completed first. Build from that current observation,
        # never from the stale response that lost the cache-write race.
        with self._job_cache_lock:
            current = self._template_artifacts
        if current is None:
            raise RuntimeError("newer template artifacts disappeared after cache race")
        runtime.observe_tip_for_refresh(current.previousblockhash)
        return current

    def artifacts_are_current(self, artifacts: CachedTemplateArtifacts) -> bool:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with self._job_cache_lock:
            current = self._template_artifacts
            return (
                current is artifacts
                or (
                    current is not None
                    and current.fingerprint == artifacts.fingerprint
                    and current.generation == artifacts.generation
                )
            )

    def fetch_coherent_snapshot(self) -> QbitTipTemplateSnapshot:
        runtime = self._runtime
        # Reserve ordering before either RPC: a fetch that started on an older
        # view must not become "newer" merely because its template arrived last.
        generation = runtime._reserve_template_artifact_generation()
        template = runtime.rpc.call(
            "getblocktemplate",
            [{"rules": qbit_gbt_rules(getattr(runtime, "qbit_chain", "regtest"))}],
        )
        if not isinstance(template, dict):
            raise RuntimeError("getblocktemplate returned non-object")
        previousblockhash = str(template.get("previousblockhash", "") or "")
        if not previousblockhash:
            raise RuntimeError("getblocktemplate omitted previousblockhash")
        # The template parent is the tip this work actually extends. Validate
        # it after fetching the template so a tip transition between these RPCs
        # cannot produce an old bestblockhash paired with newer work. Reject a
        # template that was superseded before it can enter the shared cache or
        # drive tip observation/graveyard pruning.
        bestblockhash = str(runtime.rpc.call("getbestblockhash"))
        if bestblockhash != previousblockhash:
            # Record the discovery before failing: the retry-spacing gate
            # releases on a newer DETECTED tip, and without blockwait this
            # mismatch is the only place the new tip becomes known. An
            # unrecorded discovery would hold the immediate new-tip retry
            # for the full failure holdoff.
            runtime.observe_tip_for_refresh(bestblockhash)
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshSuperseded(
                "qbit tip changed while fetching block template "
                f"template_parent={previousblockhash} current={bestblockhash}"
            )
        # The poll already paid for this template; seed the job-build cache so
        # client job builds triggered by the refresh below reuse it instead of
        # refetching one template per client.
        artifacts = runtime.store_template_artifacts(
            template,
            generation=generation,
        )
        if artifacts is not None:
            return QbitTipTemplateSnapshot(
                bestblockhash=bestblockhash,
                previousblockhash=artifacts.previousblockhash,
                template_fingerprint=artifacts.fingerprint,
                template_generation=artifacts.generation,
                template_artifacts=artifacts,
            )
        raise TemplateRefreshBlocked(
            "unable to derive exact artifacts for observed qbit template"
        )

    def current_artifacts(self) -> CachedTemplateArtifacts | None:
        with self._job_cache_lock:
            return self._template_artifacts


# Callable alias used by embedders that need a repository-compatible signature.
TemplateArtifactFetch = Callable[[], QbitTipTemplateSnapshot]


__all__ = [
    "CachedTemplateArtifacts",
    "FrozenJsonDict",
    "FrozenJsonList",
    "PRISM_TEMPLATE_FINGERPRINT_VOLATILE_KEYS",
    "PayoutStatePublicationBlocked",
    "QbitTipTemplateSnapshot",
    "TemplateArtifactRepository",
    "TemplateArtifactRuntime",
    "TemplateRefreshBlocked",
    "TemplateRefreshSuperseded",
    "freeze_json",
    "freeze_json_rows",
    "qbit_gbt_rules",
    "qbit_template_fingerprint",
    "scaled_network_difficulty",
    "scaled_target_difficulty",
    "target_from_compact",
]

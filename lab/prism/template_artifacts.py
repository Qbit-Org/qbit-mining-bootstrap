"""Immutable qbit template observations and their derived artifacts.

The repository owns template observation ordering and derivation caching.  It
does not know about the coordinator: tip publication and refresh scheduling are
supplied through narrow callbacks so detection can remain separate from R1's
published share-validation authority.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import threading
import time
from typing import Any, Callable, Iterator, Mapping, Sequence

from lab.prism import direct_stratum
from lab.prism.payout_state import (
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
)


PRISM_TEMPLATE_FINGERPRINT_VOLATILE_KEYS = frozenset(
    {
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

    def __copy__(self) -> FrozenJsonDict:
        return self

    def __deepcopy__(self, _memo: dict[int, object]) -> FrozenJsonDict:
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

    def __copy__(self) -> FrozenJsonList:
        return self

    def __deepcopy__(self, _memo: dict[int, object]) -> FrozenJsonList:
        return self


def freeze_json(value: Any) -> Any:
    """Detach and recursively freeze a JSON-like value without changing shape."""
    if isinstance(value, (FrozenJsonDict, FrozenJsonList)):
        return value
    if isinstance(value, Mapping):
        frozen = FrozenJsonDict()
        dict.update(
            frozen,
            ((str(key), freeze_json(item)) for key, item in value.items()),
        )
        return frozen
    if isinstance(value, (list, tuple)):
        frozen = FrozenJsonList()
        list.extend(frozen, (freeze_json(item) for item in value))
        return frozen
    return value


def freeze_json_rows(
    rows: Sequence[Mapping[str, Any]],
) -> FrozenJsonList:
    frozen = freeze_json(rows)
    if not isinstance(frozen, FrozenJsonList):
        raise TypeError("JSON rows must be a sequence")
    return frozen


@dataclass(frozen=True)
class CachedTemplateArtifacts:
    """One exact template observation plus template-only derivations."""

    template: dict[str, Any]
    fingerprint: str
    previousblockhash: str
    transaction_hexes: tuple[str, ...]
    witness_merkle_leaves_hex: tuple[str, ...]
    network_difficulty: int
    fetched_monotonic: float
    generation: int = 0

    def __post_init__(self) -> None:
        frozen = freeze_json(self.template)
        if not isinstance(frozen, dict):
            raise TypeError("template artifact must be a JSON object")
        object.__setattr__(self, "template", frozen)


@dataclass(frozen=True)
class QbitTipTemplateSnapshot:
    bestblockhash: str
    previousblockhash: str
    template_fingerprint: str
    template_generation: int = 0
    template_artifacts: CachedTemplateArtifacts | None = field(
        default=None,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True)
class TemplateArtifactPorts:
    fetch_template: Callable[[], object]
    fetch_bestblockhash: Callable[[], str]
    newest_observed_tip: Callable[[], str | None]
    observe_tip: Callable[[str], object]
    schedule_refresh_retry: Callable[[], None]
    pinned_issuance_artifacts: Callable[[], CachedTemplateArtifacts | None]
    repinned_issuance_artifacts: Callable[
        [CachedTemplateArtifacts], CachedTemplateArtifacts | None
    ]
    record_tip: Callable[[str], object] | None = None


@dataclass(frozen=True)
class TemplateArtifactEventSink:
    record_cache_event: Callable[[bool], None]
    record_build_phase: Callable[[str, float], None]
    artifacts_changed: Callable[[CachedTemplateArtifacts, bool], None]
    artifacts_cleared: Callable[[CachedTemplateArtifacts], None]


class TemplateArtifactRepository:
    """Sole owner of template artifact state and observation generations."""

    def __init__(
        self,
        ports: TemplateArtifactPorts,
        *,
        cache_seconds: float,
        scale_network_difficulty: Callable[[str], int],
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ports = ports
        self._cache_seconds = float(cache_seconds)
        self._scale_network_difficulty = scale_network_difficulty
        self._monotonic = monotonic
        self._lock = threading.Lock()
        # Serialize accepting an observation with all side effects caused by
        # that acceptance.  The state lock is intentionally released around
        # the event sink so it may query the repository, but a newer
        # observation cannot become current until older effects finish.
        self._publication_lock = threading.Lock()
        self._current: CachedTemplateArtifacts | None = None
        self._generation = 0
        self._event_sink: TemplateArtifactEventSink | None = None

    def bind_event_sink(self, event_sink: TemplateArtifactEventSink) -> None:
        """Bind the owning service after both repository and service exist."""
        with self._lock:
            if self._event_sink is not None:
                raise RuntimeError("template artifact event sink is already bound")
            self._event_sink = event_sink

    def _required_event_sink(self) -> TemplateArtifactEventSink:
        with self._lock:
            event_sink = self._event_sink
        if event_sink is None:
            raise RuntimeError("template artifact event sink is not bound")
        return event_sink

    @contextmanager
    def publication_admission(self) -> Iterator[None]:
        """Fence cache admission against observation publication effects."""
        with self._publication_lock:
            yield

    def reserve_generation(self) -> int:
        """Reserve ordering when a fetch starts, not when it finishes."""
        with self._lock:
            self._generation += 1
            return self._generation

    def derive(
        self,
        template: dict[str, Any],
        *,
        generation: int,
    ) -> CachedTemplateArtifacts:
        event_sink = self._required_event_sink()
        template = copy.deepcopy(template)
        fingerprint = qbit_template_fingerprint(template)
        with self._lock:
            previous = self._current
        if previous is not None and previous.fingerprint == fingerprint:
            return CachedTemplateArtifacts(
                template=template,
                fingerprint=fingerprint,
                previousblockhash=str(template.get("previousblockhash", "")),
                transaction_hexes=previous.transaction_hexes,
                witness_merkle_leaves_hex=previous.witness_merkle_leaves_hex,
                network_difficulty=previous.network_difficulty,
                fetched_monotonic=self._monotonic(),
                generation=generation,
            )
        started = self._monotonic()
        transaction_hexes = direct_stratum.transaction_hexes_from_template(template)
        witness_leaves = tuple(
            direct_stratum.witness_merkle_leaves_hex(transaction_hexes)
        )
        network_difficulty = self._scale_network_difficulty(str(template["bits"]))
        event_sink.record_build_phase(
            "merkle",
            self._monotonic() - started,
        )
        return CachedTemplateArtifacts(
            template=template,
            fingerprint=fingerprint,
            previousblockhash=str(template.get("previousblockhash", "")),
            transaction_hexes=transaction_hexes,
            witness_merkle_leaves_hex=witness_leaves,
            network_difficulty=network_difficulty,
            fetched_monotonic=self._monotonic(),
            generation=generation,
        )

    def store_artifacts(self, artifacts: CachedTemplateArtifacts) -> bool:
        event_sink = self._required_event_sink()
        with self._publication_lock:
            with self._lock:
                previous = self._current
                if (
                    previous is not None
                    and artifacts.generation < previous.generation
                ):
                    return False
                self._current = artifacts
                observation_changed = bool(
                    previous is not None
                    and (
                        previous.generation != artifacts.generation
                        or previous.fingerprint != artifacts.fingerprint
                    )
                )
                fingerprint_changed = bool(
                    previous is not None
                    and previous.fingerprint != artifacts.fingerprint
                )
            if observation_changed:
                event_sink.artifacts_changed(artifacts, fingerprint_changed)
            return True

    def store(
        self,
        template: dict[str, Any],
        *,
        generation: int | None = None,
    ) -> CachedTemplateArtifacts | None:
        if generation is None:
            generation = self.reserve_generation()
        try:
            artifacts = self.derive(template, generation=generation)
        except Exception:
            return None
        self.store_artifacts(artifacts)
        return artifacts

    def current(self) -> CachedTemplateArtifacts:
        event_sink = self._required_event_sink()
        now = self._monotonic()
        with self._lock:
            cached = self._current
        observed_tip = self._ports.newest_observed_tip()
        cached_tip_current = (
            observed_tip is None
            or cached is None
            or cached.previousblockhash == observed_tip
        )
        if (
            cached is not None
            and cached_tip_current
            and self._cache_seconds > 0
            and now - cached.fetched_monotonic <= self._cache_seconds
        ):
            event_sink.record_cache_event(True)
            return cached
        event_sink.record_cache_event(False)
        generation = self.reserve_generation()
        started = self._monotonic()
        template = self._ports.fetch_template()
        if not isinstance(template, dict):
            raise RuntimeError("getblocktemplate returned non-object")
        event_sink.record_build_phase("template", self._monotonic() - started)
        artifacts = self.derive(template, generation=generation)
        if self.store_artifacts(artifacts):
            self._ports.observe_tip(artifacts.previousblockhash)
            return artifacts
        current = self.current_artifacts()
        if current is None:
            raise RuntimeError(
                "newer template artifacts disappeared after cache race"
            )
        self._ports.observe_tip(current.previousblockhash)
        return current

    def issuance(self) -> CachedTemplateArtifacts:
        pinned = self._ports.pinned_issuance_artifacts()
        if pinned is not None:
            return pinned
        artifacts = self.current()
        return self._ports.repinned_issuance_artifacts(artifacts) or artifacts

    def fetch_coherent_snapshot(
        self,
        observed_best_tip: str | None = None,
    ) -> QbitTipTemplateSnapshot:
        event_sink = self._required_event_sink()
        if observed_best_tip is not None:
            now = self._monotonic()
            with self._lock:
                cached = self._current
            if (
                cached is not None
                and self._cache_seconds > 0
                and now - cached.fetched_monotonic <= self._cache_seconds
                and cached.previousblockhash == observed_best_tip
            ):
                event_sink.record_cache_event(True)
                return QbitTipTemplateSnapshot(
                    bestblockhash=cached.previousblockhash,
                    previousblockhash=cached.previousblockhash,
                    template_fingerprint=cached.fingerprint,
                    template_generation=cached.generation,
                    template_artifacts=cached,
                )
            event_sink.record_cache_event(False)
        generation = self.reserve_generation()
        template = self._ports.fetch_template()
        if not isinstance(template, dict):
            raise RuntimeError("getblocktemplate returned non-object")
        previousblockhash = str(template.get("previousblockhash", "") or "")
        if not previousblockhash:
            raise RuntimeError("getblocktemplate omitted previousblockhash")
        bestblockhash = str(self._ports.fetch_bestblockhash())
        if bestblockhash != previousblockhash:
            record_tip = self._ports.record_tip or self._ports.observe_tip
            record_tip(bestblockhash)
            self._ports.schedule_refresh_retry()
            raise TemplateRefreshSuperseded(
                "qbit tip changed while fetching block template "
                f"template_parent={previousblockhash} current={bestblockhash}"
            )
        artifacts = self.store(template, generation=generation)
        if artifacts is None:
            raise TemplateRefreshBlocked(
                "unable to derive exact artifacts for observed qbit template"
            )
        return QbitTipTemplateSnapshot(
            bestblockhash=bestblockhash,
            previousblockhash=artifacts.previousblockhash,
            template_fingerprint=artifacts.fingerprint,
            template_generation=artifacts.generation,
            template_artifacts=artifacts,
        )

    def current_artifacts(self) -> CachedTemplateArtifacts | None:
        with self._lock:
            return self._current

    def is_current(self, artifacts: CachedTemplateArtifacts) -> bool:
        with self._lock:
            return self._current is artifacts

    def clear_if_current(self, artifacts: CachedTemplateArtifacts) -> bool:
        event_sink = self._required_event_sink()
        with self._publication_lock:
            with self._lock:
                if self._current is not artifacts:
                    return False
                self._current = None
            event_sink.artifacts_cleared(artifacts)
            return True

    def replace_for_test(
        self,
        artifacts: CachedTemplateArtifacts | None,
    ) -> None:
        # Test-only state injection intentionally bypasses event-sink effects,
        # but still participates in the production publication fence.
        with self._publication_lock:
            with self._lock:
                self._current = artifacts
                if artifacts is not None:
                    self._generation = max(self._generation, artifacts.generation)

    def set_cache_seconds_for_test(self, seconds: float) -> None:
        self._cache_seconds = float(seconds)

    def generation_for_test(self) -> int:
        with self._lock:
            return self._generation

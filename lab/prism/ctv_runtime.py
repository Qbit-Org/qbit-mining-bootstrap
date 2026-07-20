"""Coordinator-facing lifecycle and metrics for the CTV broadcaster daemon.

The CTV broadcast engine and durable daemon remain in their existing modules.
This service owns only the process runtime seam: daemon construction, writer
admission, the dedicated loop, and its bounded-cardinality metrics.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
import threading
import time
import traceback
from typing import Any, Callable, Protocol

from lab.prism.background_services import BackgroundServiceSpec
from lab.prism.coordinator_config import CtvConfig
from lab.prism.coordinator_shutdown import ShutdownInProgress
from lab.prism.ctv_broadcaster import CtvFanoutBroadcaster
from lab.prism.ctv_broadcaster_daemon import (
    CtvFanoutBroadcastDaemon,
    CtvFanoutChunkResult,
    CtvFanoutDaemonResult,
)


CTV_FANOUT_BROADCASTER_SERVICE_NAME = "ctv_fanout_broadcaster"
CTV_BROADCAST_STATE_COMPONENT = "ctv_broadcast_state"
PRISM_CTV_BROADCASTER_SECONDS_BUCKETS = (
    1.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
)
PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS = (
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)
PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS = (1, 2, 5, 10, 25, 50, 100)


class StopSignal(Protocol):
    def is_set(self) -> bool: ...

    def wait(self, timeout: float) -> bool: ...


@dataclass(frozen=True, slots=True)
class CtvRuntimeConfig:
    enabled: bool
    wallet: str | None
    fee_sats: int
    limit: int
    chunk_size: int
    interval_seconds: float

    @classmethod
    def from_coordinator_config(cls, config: CtvConfig) -> "CtvRuntimeConfig":
        return cls(
            enabled=config.broadcaster_enabled,
            wallet=config.broadcaster_wallet,
            fee_sats=config.broadcaster_fee_sats,
            limit=config.broadcaster_limit,
            chunk_size=config.broadcaster_chunk_size,
            interval_seconds=config.broadcaster_interval_seconds,
        )


@dataclass(frozen=True, slots=True)
class CtvRuntimeMetricsSnapshot:
    pass_seconds_bucket_counts: dict[float, int]
    pass_seconds_sum: float
    pass_count: int
    processed_rows_total: int
    yielded_total: int
    chunk_seconds_bucket_counts: dict[float, int]
    chunk_rows_bucket_counts: dict[int, int]
    chunk_seconds_sum: float
    chunk_rows_sum: int
    chunk_count: int


class CtvRuntimeService:
    """Own the coordinator's CTV daemon instance, loop, and metric state."""

    def __init__(
        self,
        *,
        rpc_call: Callable[..., Any],
        ledger: object,
        writer_admission: Callable[[str], AbstractContextManager[object]],
        tip_refresh_pending: Callable[[], bool],
        heartbeat: Callable[[], None],
        stop_event: StopSignal,
        config: CtvRuntimeConfig,
        daemon_type: Callable[..., CtvFanoutBroadcastDaemon] = CtvFanoutBroadcastDaemon,
        broadcaster_type: Callable[..., CtvFanoutBroadcaster] = CtvFanoutBroadcaster,
        monotonic: Callable[[], float] = time.monotonic,
        print_exception: Callable[[], None] = traceback.print_exc,
    ) -> None:
        self._rpc_call = rpc_call
        self._ledger = ledger
        self._writer_admission = writer_admission
        self._tip_refresh_pending = tip_refresh_pending
        self._heartbeat = heartbeat
        self._stop_event = stop_event
        self._daemon_type = daemon_type
        self._broadcaster_type = broadcaster_type
        self._monotonic = monotonic
        self._print_exception = print_exception
        self._config_lock = threading.Lock()
        self._config = config

        self._daemon_lock = threading.Lock()
        self._daemon: CtvFanoutBroadcastDaemon | None = None
        self._metrics_lock = threading.Lock()
        self._pass_seconds_bucket_counts = {
            bucket: 0 for bucket in PRISM_CTV_BROADCASTER_SECONDS_BUCKETS
        }
        self._pass_seconds_sum = 0.0
        self._pass_count = 0
        self._processed_rows_total = 0
        self._yielded_total = 0
        self._chunk_seconds_bucket_counts = {
            bucket: 0 for bucket in PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS
        }
        self._chunk_rows_bucket_counts = {
            bucket: 0 for bucket in PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS
        }
        self._chunk_seconds_sum = 0.0
        self._chunk_rows_sum = 0
        self._chunk_count = 0

    @property
    def daemon(self) -> CtvFanoutBroadcastDaemon | None:
        with self._daemon_lock:
            return self._daemon

    @daemon.setter
    def daemon(self, daemon: CtvFanoutBroadcastDaemon | None) -> None:
        with self._daemon_lock:
            self._daemon = daemon

    @property
    def config(self) -> CtvRuntimeConfig:
        """Return one immutable configuration snapshot under service ownership."""
        with self._config_lock:
            return self._config

    def replace_config(self, **changes: object) -> None:
        """Support temporary coordinator compatibility properties."""
        with self._daemon_lock:
            with self._config_lock:
                previous = self._config
                replacement = replace(previous, **changes)
                self._config = replacement
            if (
                previous.wallet != replacement.wallet
                or previous.fee_sats != replacement.fee_sats
            ):
                self._daemon = None

    def make_daemon(
        self,
        config: CtvRuntimeConfig | None = None,
    ) -> CtvFanoutBroadcastDaemon:
        config = self.config if config is None else config
        if config.fee_sats > 0 and not config.wallet:
            raise ValueError(
                "ctv_broadcaster_wallet is required when "
                "ctv_broadcaster_fee_sats is positive"
            )
        broadcaster = self._broadcaster_type(
            self._rpc_call,
            funding_wallet=config.wallet,
        )
        return self._daemon_type(
            self._ledger,
            broadcaster,
            fee_sats=config.fee_sats,
        )

    def _configured_daemon(
        self,
    ) -> tuple[CtvRuntimeConfig, CtvFanoutBroadcastDaemon]:
        with self._daemon_lock:
            with self._config_lock:
                config = self._config
            if self._daemon is None:
                self._daemon = self.make_daemon(config)
            return config, self._daemon

    def run_once(
        self,
        *,
        progress_callback: Callable[[], None] | None = None,
        chunk_callback: Callable[[CtvFanoutChunkResult], None] | None = None,
    ) -> CtvFanoutDaemonResult:
        with self._writer_admission(CTV_BROADCAST_STATE_COMPONENT):
            config, daemon = self._configured_daemon()
            return daemon.run_once(
                limit=config.limit,
                progress_callback=progress_callback,
                chunk_size=config.chunk_size,
                tip_refresh_pending=self._tip_refresh_pending,
                chunk_callback=(
                    self.observe_chunk if chunk_callback is None else chunk_callback
                ),
            )

    def record_progress(self) -> None:
        self._heartbeat()
        with self._metrics_lock:
            self._processed_rows_total += 1

    def observe_pass(self, elapsed_seconds: float) -> None:
        with self._metrics_lock:
            self._pass_count += 1
            self._pass_seconds_sum += elapsed_seconds
            for bucket in PRISM_CTV_BROADCASTER_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    self._pass_seconds_bucket_counts[bucket] += 1

    def observe_chunk(self, result: CtvFanoutChunkResult) -> None:
        self._heartbeat()
        with self._metrics_lock:
            self._chunk_count += 1
            self._chunk_seconds_sum += result.elapsed_seconds
            self._chunk_rows_sum += result.processed_count
            for bucket in PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS:
                if result.elapsed_seconds <= bucket:
                    self._chunk_seconds_bucket_counts[bucket] += 1
            for bucket in PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS:
                if result.processed_count <= bucket:
                    self._chunk_rows_bucket_counts[bucket] += 1

    def record_yield(self) -> None:
        with self._metrics_lock:
            self._yielded_total += 1

    def loop(
        self,
        *,
        run_once: Callable[..., CtvFanoutDaemonResult] | None = None,
        progress_callback: Callable[[], None] | None = None,
        observe_pass: Callable[[float], None] | None = None,
        record_yield: Callable[[], None] | None = None,
    ) -> None:
        """Run the dedicated broadcaster loop until shutdown.

        The optional call ports retain the coordinator's temporary method-level
        compatibility seam; normal registered service threads use the owned
        methods directly.
        """
        pass_runner = self.run_once if run_once is None else run_once
        progress_recorder = (
            self.record_progress if progress_callback is None else progress_callback
        )
        pass_observer = self.observe_pass if observe_pass is None else observe_pass
        yield_recorder = self.record_yield if record_yield is None else record_yield
        while not self._stop_event.is_set():
            self._heartbeat()
            started = self._monotonic()
            shutdown_admission_closed = False
            try:
                try:
                    result = pass_runner(progress_callback=progress_recorder)
                except ShutdownInProgress:
                    shutdown_admission_closed = True
                    return
                finally:
                    # Stamp completion before logging or entering the interval
                    # wait. A blocked row never reaches this finally clause, so
                    # the watchdog remains able to recover a wedged operation.
                    self._heartbeat()
                    if not shutdown_admission_closed:
                        pass_observer(max(0.0, self._monotonic() - started))
            except Exception:
                print("prism coordinator: CTV fanout broadcaster pass failed", flush=True)
                self._print_exception()
            else:
                if result.yielded_to_tip_refresh:
                    yield_recorder()
                if result.scanned_count or result.submitted_count or result.failed_count:
                    print(
                        "prism coordinator: CTV fanout broadcaster "
                        f"scanned={result.scanned_count} "
                        f"submitted={result.submitted_count} "
                        f"updated={result.updated_count} "
                        f"failed={result.failed_count}",
                        flush=True,
                    )
            if self._stop_event.wait(self.config.interval_seconds):
                break

    def background_service_spec(self) -> BackgroundServiceSpec:
        return BackgroundServiceSpec(
            name=CTV_FANOUT_BROADCASTER_SERVICE_NAME,
            thread_name="prism-ctv-fanout-broadcaster",
            target=self.loop,
            daemon=True,
            join_timeout=1.0,
            watchdog_monitored=True,
        )

    def startup_summary(self) -> str:
        config = self.config
        return (
            "prism coordinator: CTV fanout broadcaster enabled "
            f"mode={'cpfp' if config.fee_sats > 0 else 'direct'} "
            f"fee_bits={config.fee_sats} "
            f"wallet={'configured' if config.wallet else 'none'} "
            f"interval={config.interval_seconds:g}s "
            f"limit={config.limit} "
            f"chunk_size={config.chunk_size}"
        )

    def metrics_snapshot(self) -> CtvRuntimeMetricsSnapshot:
        with self._metrics_lock:
            return CtvRuntimeMetricsSnapshot(
                pass_seconds_bucket_counts=dict(self._pass_seconds_bucket_counts),
                pass_seconds_sum=self._pass_seconds_sum,
                pass_count=self._pass_count,
                processed_rows_total=self._processed_rows_total,
                yielded_total=self._yielded_total,
                chunk_seconds_bucket_counts=dict(self._chunk_seconds_bucket_counts),
                chunk_rows_bucket_counts=dict(self._chunk_rows_bucket_counts),
                chunk_seconds_sum=self._chunk_seconds_sum,
                chunk_rows_sum=self._chunk_rows_sum,
                chunk_count=self._chunk_count,
            )

    @property
    def processed_rows_total(self) -> int:
        return self.metrics_snapshot().processed_rows_total

    @property
    def pass_count(self) -> int:
        return self.metrics_snapshot().pass_count

    def metrics_lines(self) -> list[str]:
        snapshot = self.metrics_snapshot()
        metric_name = "qbit_prism_ctv_fanout_broadcaster_pass_seconds"
        chunk_seconds_name = "qbit_prism_ctv_fanout_broadcaster_chunk_seconds"
        chunk_rows_name = "qbit_prism_ctv_fanout_broadcaster_chunk_rows"
        return [
            "# HELP qbit_prism_ctv_fanout_broadcaster_processed_rows_total CTV fanout rows completed by the broadcaster loop.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_processed_rows_total counter",
            f"qbit_prism_ctv_fanout_broadcaster_processed_rows_total {snapshot.processed_rows_total}",
            "# HELP qbit_prism_ctv_fanout_broadcaster_pass_seconds CTV fanout broadcaster pass wall time.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_pass_seconds histogram",
            *[
                f'{metric_name}_bucket{{le="{bucket:g}"}} '
                f"{snapshot.pass_seconds_bucket_counts.get(bucket, 0)}"
                for bucket in PRISM_CTV_BROADCASTER_SECONDS_BUCKETS
            ],
            f'{metric_name}_bucket{{le="+Inf"}} {snapshot.pass_count}',
            f"{metric_name}_sum {snapshot.pass_seconds_sum:.6f}",
            f"{metric_name}_count {snapshot.pass_count}",
            "# HELP qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total CTV broadcaster passes yielding between committed chunks for a pending tip refresh.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total counter",
            f"qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total {snapshot.yielded_total}",
            "# HELP qbit_prism_ctv_fanout_broadcaster_chunk_seconds CTV broadcaster committed chunk wall time.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_chunk_seconds histogram",
            *[
                f'{chunk_seconds_name}_bucket{{le="{bucket:g}"}} '
                f"{snapshot.chunk_seconds_bucket_counts.get(bucket, 0)}"
                for bucket in PRISM_CTV_BROADCASTER_CHUNK_SECONDS_BUCKETS
            ],
            f'{chunk_seconds_name}_bucket{{le="+Inf"}} {snapshot.chunk_count}',
            f"{chunk_seconds_name}_sum {snapshot.chunk_seconds_sum:.6f}",
            f"{chunk_seconds_name}_count {snapshot.chunk_count}",
            "# HELP qbit_prism_ctv_fanout_broadcaster_chunk_rows Rows processed per committed CTV broadcaster chunk.",
            "# TYPE qbit_prism_ctv_fanout_broadcaster_chunk_rows histogram",
            *[
                f'{chunk_rows_name}_bucket{{le="{bucket}"}} '
                f"{snapshot.chunk_rows_bucket_counts.get(bucket, 0)}"
                for bucket in PRISM_CTV_BROADCASTER_CHUNK_ROWS_BUCKETS
            ],
            f'{chunk_rows_name}_bucket{{le="+Inf"}} {snapshot.chunk_count}',
            f"{chunk_rows_name}_sum {snapshot.chunk_rows_sum}",
            f"{chunk_rows_name}_count {snapshot.chunk_count}",
        ]

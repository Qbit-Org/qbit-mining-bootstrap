#!/usr/bin/env python3
"""Cancelable subprocess and daemon transport for PRISM audit-bundle builds.

The compiler owns the complete builder wire/process domain: the compact
audit payload encoding, the share-window spool representation and its
leases, the one-shot subprocess build, and the persistent ``--serve`` daemon
protocol.  It never imports ``prism_coordinator`` or ``job_bundle``: shared
scheduler state and refresh metrics are reached through the
:class:`BundleCompilerRuntime` port (resolved at call time so coordinator
monkeypatch seams keep working), and the job-bundle exception/control types
are injected at construction exactly like the historical layer's
``superseded_error`` port.
"""

from __future__ import annotations

from collections import OrderedDict
from contextlib import ExitStack
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable, Protocol

from lab.prism.coordinator_config import (
    DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
    DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
    env_bool,
)
from lab.prism.prism_tools import prism_tool_command


PRISM_BUILDER_PHASE_METRICS_PREFIX = "qbit-prism-build-phase-metrics "
# Owner-local duplicate of the coordinator's admission poll cadence; the
# compiler must not import the coordinator for one pacing constant.
PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05
# Upper bound for one kernel transfer from the share-window spool file into
# the audit builder's stdin pipe. The kernel clamps each call to the free
# pipe capacity anyway; the bound only paces cancellation checkpoints.
PRISM_SPOOL_SPLICE_CHUNK_BYTES = 1 << 20
# JSONL protocol version this coordinator speaks with the --serve audit
# builder. The daemon announces its version in a startup handshake; any
# mismatch retires the daemon and every build falls back to one-shot mode.
PRISM_SERVE_BUILDER_PROTOCOL_VERSION = 1
# Parsed share windows the --serve daemon retains; the coordinator mirrors
# the bound to predict which uploads the daemon still holds.
PRISM_SERVE_BUILDER_WINDOW_CACHE_ENTRIES = 2


class CancellationPort(Protocol):
    """Cooperative-cancellation surface consumed by the compiler."""

    def is_set(self) -> bool: ...

    def raise_if_cancelled(self, phase: str) -> None: ...


class BundleBuildControlPort(Protocol):
    """Registered build control consumed for supersession checks."""

    cancel_event: Any
    process: Any


class BundleCompilerRuntime(Protocol):
    """Typed port over the coordinator, resolved at call time."""

    signing_seed_hex: str
    ledger_attestation_signing_seed_hex: str

    def _ensure_job_cache_state(self) -> None: ...

    def _ensure_tip_refresh_state(self) -> None: ...

    def _job_build_checkpoint(self, phase: str, cancellation: Any) -> None: ...

    def _job_build_phases(self) -> dict[str, float]: ...

    def prism_payout_policy(self) -> dict[str, object]: ...

    def prism_ctv_settlement_config(
        self,
        *,
        block_height: int,
        parent_hash: str | None,
    ) -> dict[str, object] | None: ...

    def _observe_tip_refresh_build_phase(self, name: str, elapsed: float) -> None: ...

    def _record_tip_refresh_ipc_bytes(self, direction: str, byte_count: int) -> None: ...

    def _register_job_bundle_process(self, control: Any, process: Any) -> None: ...

    def _unregister_job_bundle_process(self, control: Any, process: Any) -> None: ...


def _compact_share_payload(
    shares: list[dict[str, object]],
) -> tuple[list[tuple[str, str, str]], list[tuple[object, ...]]]:
    """Deduplicate share identities into the audit-builder compact form."""

    identity_indexes: dict[tuple[str, str, str], int] = {}
    identities: list[tuple[str, str, str]] = []
    compact_shares: list[tuple[object, ...]] = []
    for share in shares:
        identity = (
            str(share["miner_id"]),
            str(share["order_key"]),
            str(share["p2mr_program_hex"]),
        )
        identity_index = identity_indexes.get(identity)
        if identity_index is None:
            identity_index = len(identities)
            identity_indexes[identity] = identity_index
            identities.append(identity)
        compact_shares.append(
            (
                share["share_seq"],
                share["share_id"],
                identity_index,
                share["share_difficulty"],
                share["job_issued_at_ms"],
                share["accepted_at_ms"],
                share.get("credit_policy"),
            )
        )
    return identities, compact_shares


def _share_window_spool_file() -> Any:
    """Anonymous spool file for the serialized share-window payload tail.

    Created in the same temporary filesystem the builder's captured output
    and stderr already use, and unlinked from birth: a crashed coordinator
    can never strand a multi-megabyte window on disk, and closing the last
    descriptor is the entire cleanup story.
    """
    return tempfile.TemporaryFile()


@dataclass
class _ShareWindowSerialization:
    """Derived share-window forms cached per canonical window.

    For a fixed (canonical digest, count, window weight) the serialized share
    window is immutable: its canonical digest and the
    audit-builder compact payload fragments are pure functions of the
    artifact's share snapshot. Recomputing them inside every template-bump
    build serialized the whole window under the GIL each time. The compact
    fragments are derived lazily by the first audit-builder invocation --
    embedders that replace the builder never require the compact schema.

    The fragments' invariant byte encoding is additionally spooled to an
    anonymous temp file once per key, so every build after the first can feed
    the audit builder's stdin through kernel transfers instead of re-writing
    megabytes of unchanged JSON through Python. Spool access is leased:
    rotation to a new generation retires the spool, and the descriptor closes
    when the last in-flight transfer releases it, never underneath one.
    """

    key: tuple[str, int, int]
    share_count: int
    share_snapshot_sha256: str
    _source_artifact: Any = field(
        default=None,
        repr=False,
        compare=False,
    )
    _compact_lock: threading.Lock = field(
        default_factory=threading.Lock,
        repr=False,
    )
    _compact_share_identities_json: str | None = field(
        default=None,
        repr=False,
    )
    _compact_shares_json: str | None = field(default=None, repr=False)
    _spool_lock: threading.Lock = field(
        default_factory=threading.Lock,
        repr=False,
    )
    _spool_file: Any = field(default=None, repr=False)
    _spool_size: int = field(default=0, repr=False)
    _spool_failed: bool = field(default=False, repr=False)
    _spool_retired: bool = field(default=False, repr=False)
    _spool_leases: int = field(default=0, repr=False)
    # Call-time-resolved spool-file factory. The coordinator wires a lambda
    # that resolves its own module global so the historical
    # ``prism_coordinator._share_window_spool_file`` patch seam keeps working.
    _spool_factory: Callable[[], Any] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def compact_fragments(
        self,
        shares: list[dict[str, object]],
    ) -> tuple[str, str]:
        """Encoded compact fragments, computed once per generation.

        Concurrent builders block here instead of duplicating the encode; the
        window is immutable for this key, so first-writer-wins is exact.
        """
        with self._compact_lock:
            if (
                self._compact_share_identities_json is None
                or self._compact_shares_json is None
            ):
                identities, compact_shares = _compact_share_payload(shares)
                self._compact_share_identities_json = json.dumps(
                    identities,
                    separators=(",", ":"),
                )
                self._compact_shares_json = json.dumps(
                    compact_shares,
                    separators=(",", ":"),
                )
            return (
                self._compact_share_identities_json,
                self._compact_shares_json,
            )

    def acquire_spooled_tail(
        self,
        shares: list[dict[str, object]],
    ) -> tuple[Any, int] | None:
        """Lease the spooled payload tail, writing it on first use.

        Returns the open spool file and its byte size, or None when spooling
        is unavailable (creation failed earlier, a transfer poisoned it, or
        the generation was retired). The caller must pair a successful lease
        with release_spooled_tail once its transfer is finished.
        """
        with self._spool_lock:
            if self._spool_failed or self._spool_retired:
                return None
            if self._spool_file is None:
                identities_json, compact_shares_json = self.compact_fragments(
                    shares
                )
                spool = None
                spool_factory = self._spool_factory or _share_window_spool_file
                try:
                    spool = spool_factory()
                    spool.write(b',"compact_share_identities":')
                    spool.write(identities_json.encode("utf-8"))
                    spool.write(b',"compact_shares":')
                    spool.write(compact_shares_json.encode("utf-8"))
                    spool.write(b"}")
                    spool.flush()
                    size = spool.seek(0, os.SEEK_END)
                except (OSError, ValueError):
                    # Spooling is an optimization; the in-memory pipe path
                    # remains authoritative. Failure is sticky so a broken
                    # temp filesystem is not retried on every build.
                    self._spool_failed = True
                    if spool is not None:
                        try:
                            spool.close()
                        except OSError:
                            pass
                    return None
                self._spool_file = spool
                self._spool_size = int(size)
            self._spool_leases += 1
            return self._spool_file, self._spool_size

    def release_spooled_tail(self) -> None:
        with self._spool_lock:
            self._spool_leases -= 1
            self._close_spool_if_unused_locked()

    def mark_spool_failed(self) -> None:
        """Poison the spool after a transfer-side failure (still leased)."""
        with self._spool_lock:
            self._spool_failed = True

    def retire_spool(self) -> None:
        """Stop new leases and close the file once in-flight ones finish."""
        with self._spool_lock:
            self._spool_retired = True
            self._close_spool_if_unused_locked()

    def _close_spool_if_unused_locked(self) -> None:
        if (
            self._spool_leases > 0
            or self._spool_file is None
            or not (self._spool_retired or self._spool_failed)
        ):
            return
        spool, self._spool_file = self._spool_file, None
        self._spool_size = 0
        try:
            spool.close()
        except OSError:
            pass


class _ServeBuilderUnavailable(RuntimeError):
    """Daemon anomaly; the build must transparently use one-shot mode."""


@dataclass
class _ServeBuilderClient:
    """Line-oriented I/O state for one long-lived --serve audit builder."""

    process: subprocess.Popen[bytes]
    stdout_buffer: bytearray = field(default_factory=bytearray, repr=False)
    # Mirrors the daemon's bounded LRU so requests can predict whether the
    # window must ride along. Divergence is repaired by the daemon's
    # needs_window response, never trusted blindly.
    uploaded_windows: OrderedDict[str, None] = field(
        default_factory=OrderedDict,
        repr=False,
    )

    def note_uploaded_window(self, share_snapshot_sha256: str) -> None:
        self.uploaded_windows.pop(share_snapshot_sha256, None)
        self.uploaded_windows[share_snapshot_sha256] = None
        while len(self.uploaded_windows) > PRISM_SERVE_BUILDER_WINDOW_CACHE_ENTRIES:
            self.uploaded_windows.popitem(last=False)

    def close(self) -> None:
        # Tolerates lightweight process fakes used by embedders and tests,
        # which do not necessarily expose the full Popen surface.
        process = self.process
        poll = getattr(process, "poll", None)
        wait = getattr(process, "wait", None)
        if callable(poll):
            if poll() is None:
                try:
                    process.kill()
                except (ProcessLookupError, OSError, AttributeError):
                    pass
            if callable(wait):
                try:
                    wait(timeout=5.0)
                except (
                    subprocess.TimeoutExpired,
                    OSError,
                    TypeError,
                    ValueError,
                    AttributeError,
                ):
                    pass
        for stream in (
            getattr(process, "stdin", None),
            getattr(process, "stdout", None),
        ):
            if stream is None or not hasattr(stream, "close"):
                continue
            try:
                stream.close()
            except OSError:
                pass


class BundleCompiler:
    """Compile summaries or canonical bundles with exact cancellation rules.

    Owns the persistent ``--serve`` daemon state and its metrics; every
    scheduler counter, refresh metric, and payload input is reached through
    the runtime port so the coordinator remains the single owner of that
    state and its monkeypatch seams.
    """

    def __init__(
        self,
        runtime: BundleCompilerRuntime,
        *,
        superseded_error: Callable[[str], BaseException],
        cancellation_error_types: tuple[type[BaseException], ...],
        build_control_type: type,
        tool_command: Callable[[str], list[str]] | None = None,
    ) -> None:
        self._runtime = runtime
        self._superseded_error = superseded_error
        self._cancellation_error_types = tuple(cancellation_error_types)
        self._build_control_type = build_control_type
        # Call-time-resolved builder-command factory: the coordinator wires a
        # lambda over its own module global so the historical
        # ``prism_coordinator.prism_tool_command`` patch seam keeps working.
        self._tool_command = tool_command or prism_tool_command
        # Serializes daemon ownership per request. Contended builds do
        # not queue behind the daemon; they take the one-shot path.
        self._serve_builder_lock = threading.Lock()
        self._serve_builder: _ServeBuilderClient | None = None
        self._serve_builder_shutdown = False
        # Counters get their own short-lived lock so a metrics scrape
        # never waits behind _serve_builder_lock, which one request can
        # hold for a whole daemon round trip.
        self._serve_builder_metrics_lock = threading.Lock()
        # Guarded by _serve_builder_metrics_lock.
        self.serve_builder_counts = {
            "requests": 0,
            "fallbacks": 0,
            "spawns": 0,
            "window_uploads": 0,
        }
        # Guarded by _serve_builder_metrics_lock.
        self.serve_builder_window_cache_counts = {"hits": 0, "misses": 0}

    def shutdown_serve_builder(self) -> None:
        """Retire the persistent audit builder; builds revert to one-shot."""
        self._runtime._ensure_job_cache_state()
        with self._serve_builder_lock:
            self._serve_builder_shutdown = True
            client, self._serve_builder = self._serve_builder, None
        if client is not None:
            client.close()

    def _retire_serve_builder_locked(self) -> None:
        client, self._serve_builder = self._serve_builder, None
        if client is not None:
            client.close()

    def _record_live_serve_builder_termination(
        self,
        client: _ServeBuilderClient | None,
    ) -> None:
        """Account for deliberately killing a still-running daemon.

        Crash paths count themselves when the EOF surfaces; a live worker
        retired for a timeout, malformed response, protocol mismatch, or
        spool failure must land in the termination counters so its
        replacement reads as a restart, mirroring the cancellation path.
        """
        runtime = self._runtime
        if client is None:
            return
        poll = getattr(client.process, "poll", None)
        if not callable(poll) or poll() is not None:
            return
        with runtime._job_build_scheduler_lock:
            runtime.job_build_worker_counts["terminations"] += 1
            runtime._job_build_worker_restart_pending = True

    def _observe_builder_phase_metrics(self, metrics: dict[str, Any]) -> None:
        """Apply one build's builder-side phase timings to refresh metrics.

        Shared by the one-shot stderr metrics line and the --serve response
        payload so serialization_copy and the builder phase histograms stay
        comparable regardless of the transport that ran the build.
        """
        runtime = self._runtime
        phase_seconds = metrics.get("phases_seconds", {})
        if isinstance(phase_seconds, dict):
            for phase in (
                "payout_state_derivation",
                "ctv_manifest_construction",
                "coinbase_bundle_construction",
                "signing_verification",
            ):
                elapsed = phase_seconds.get(phase)
                if isinstance(elapsed, (int, float)):
                    runtime._observe_tip_refresh_build_phase(
                        phase,
                        float(elapsed),
                    )
        rust_serialization = sum(
            float(metrics.get(name, 0.0))
            for name in (
                "input_deserialization_seconds",
                "output_serialization_seconds",
            )
        )
        runtime._observe_tip_refresh_build_phase(
            "serialization_copy",
            rust_serialization,
        )

    def _spawn_serve_builder_locked(
        self,
        deadline: float,
        cancellation: CancellationPort | None,
    ) -> _ServeBuilderClient:
        runtime = self._runtime
        command = self._tool_command("qbit-prism-build-audit-bundle") + [
            "--serve",
            "--signing-key-seed-hex",
            runtime.signing_seed_hex,
            "--ledger-signing-key-seed-hex",
            runtime.ledger_attestation_signing_seed_hex,
        ]
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                # The daemon inherits the coordinator's stderr so a crash's
                # diagnostics land in the journal instead of a per-request
                # capture file that dies with the request.
                stderr=None,
                close_fds=True,
            )
        except OSError as exc:
            raise _ServeBuilderUnavailable(
                f"audit-builder daemon spawn failed: {exc}"
            ) from exc
        # The start (and any pending restart it satisfies) is recorded as
        # soon as the worker exists, exactly like the one-shot path: a
        # handshake-phase death or protocol mismatch must not leave the
        # lifecycle totals describing fewer workers than were launched.
        restarted = False
        with runtime._job_build_scheduler_lock:
            if runtime._job_build_worker_restart_pending:
                runtime.job_build_worker_counts["restarts"] += 1
                runtime._job_build_worker_restart_pending = False
                restarted = True
            runtime.job_build_worker_counts["starts"] += 1
        if restarted:
            with runtime._tip_refresh_metrics_lock:
                runtime.tip_refresh_worker_restarts += 1
        client = _ServeBuilderClient(process=process)
        try:
            assert process.stdin is not None and process.stdout is not None
            os.set_blocking(process.stdin.fileno(), False)
            os.set_blocking(process.stdout.fileno(), False)
            handshake_line = self._serve_builder_read_line(
                client,
                deadline,
                cancellation,
                None,
            )
            handshake = json.loads(handshake_line)
            if (
                not isinstance(handshake, dict)
                or handshake.get("event") != "handshake"
                or handshake.get("protocol")
                != PRISM_SERVE_BUILDER_PROTOCOL_VERSION
            ):
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon announced an unsupported protocol"
                )
        except _ServeBuilderUnavailable:
            self._record_live_serve_builder_termination(client)
            client.close()
            raise
        except (OSError, ValueError, AttributeError) as exc:
            self._record_live_serve_builder_termination(client)
            client.close()
            raise _ServeBuilderUnavailable(
                f"audit-builder daemon handshake failed: {exc}"
            ) from exc
        except BaseException:
            self._record_live_serve_builder_termination(client)
            client.close()
            raise
        return client

    def _serve_builder_read_line(
        self,
        client: _ServeBuilderClient,
        deadline: float,
        cancellation: CancellationPort | None,
        build_control: BundleBuildControlPort | None,
    ) -> bytes:
        runtime = self._runtime
        stdout = client.process.stdout
        assert stdout is not None
        file_descriptor = stdout.fileno()
        while True:
            newline_index = client.stdout_buffer.find(b"\n")
            if newline_index >= 0:
                line = bytes(client.stdout_buffer[:newline_index])
                del client.stdout_buffer[: newline_index + 1]
                return line
            if cancellation is not None:
                cancellation.raise_if_cancelled("serve builder response")
            if (
                build_control is not None
                and build_control.cancel_event.is_set()
            ):
                raise self._superseded_error(
                    "audit-builder daemon request was canceled after supersession"
                )
            if time.monotonic() >= deadline:
                # Not counted as a worker failure here: the fallback one-shot
                # runs against this same exhausted deadline and its own
                # timeout path records the failure exactly once.
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon timed out"
                )
            try:
                chunk = os.read(file_descriptor, 1 << 16)
            except (BlockingIOError, InterruptedError):
                time.sleep(min(0.02, PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS))
                continue
            except OSError as exc:
                raise _ServeBuilderUnavailable(
                    f"audit-builder daemon read failed: {exc}"
                ) from exc
            if not chunk:
                if (
                    build_control is not None
                    and build_control.cancel_event.is_set()
                ):
                    raise self._superseded_error(
                        "audit-builder daemon was terminated after supersession"
                    )
                with runtime._job_build_scheduler_lock:
                    runtime.job_build_worker_counts["crashes"] += 1
                    runtime._job_build_worker_restart_pending = True
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon exited mid-request"
                )
            client.stdout_buffer += chunk

    def _serve_builder_write(
        self,
        client: _ServeBuilderClient,
        data: bytes,
        deadline: float,
        cancellation: CancellationPort | None,
        build_control: BundleBuildControlPort | None,
    ) -> int:
        stdin = client.process.stdin
        assert stdin is not None
        file_descriptor = stdin.fileno()
        remaining = memoryview(data)
        written_total = 0
        while remaining:
            if cancellation is not None:
                cancellation.raise_if_cancelled("serve builder request")
            if (
                build_control is not None
                and build_control.cancel_event.is_set()
            ):
                raise self._superseded_error(
                    "audit-builder daemon request was canceled after supersession"
                )
            if time.monotonic() >= deadline:
                # Not counted as a worker failure here: the fallback one-shot
                # runs against this same exhausted deadline and its own
                # timeout path records the failure exactly once.
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon timed out"
                )
            try:
                written = os.write(file_descriptor, remaining)
            except (BlockingIOError, InterruptedError):
                time.sleep(min(0.02, PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS))
                continue
            except BrokenPipeError as exc:
                if (
                    build_control is not None
                    and build_control.cancel_event.is_set()
                ):
                    raise self._superseded_error(
                        "audit-builder daemon was terminated after supersession"
                    ) from exc
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon input pipe closed"
                ) from exc
            except OSError as exc:
                raise _ServeBuilderUnavailable(
                    f"audit-builder daemon write failed: {exc}"
                ) from exc
            if written <= 0:
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon input pipe closed"
                )
            written_total += written
            remaining = remaining[written:]
        return written_total

    def _serve_builder_splice_spool(
        self,
        client: _ServeBuilderClient,
        share_serialization: _ShareWindowSerialization,
        spool_file: Any,
        spool_size: int,
        deadline: float,
        cancellation: CancellationPort | None,
        build_control: BundleBuildControlPort | None,
    ) -> int:
        stdin = client.process.stdin
        assert stdin is not None
        stdin_fd = stdin.fileno()
        spool_fd = spool_file.fileno()
        offset = 0
        while offset < spool_size:
            if cancellation is not None:
                cancellation.raise_if_cancelled("serve builder request")
            if (
                build_control is not None
                and build_control.cancel_event.is_set()
            ):
                raise self._superseded_error(
                    "audit-builder daemon request was canceled after supersession"
                )
            if time.monotonic() >= deadline:
                # Not counted as a worker failure here: the fallback one-shot
                # runs against this same exhausted deadline and its own
                # timeout path records the failure exactly once.
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon timed out"
                )
            try:
                moved = os.splice(
                    spool_fd,
                    stdin_fd,
                    min(PRISM_SPOOL_SPLICE_CHUNK_BYTES, spool_size - offset),
                    offset_src=offset,
                )
            except (BlockingIOError, InterruptedError):
                time.sleep(min(0.02, PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS))
                continue
            except BrokenPipeError as exc:
                # The daemon's stdin closed; the spool itself is fine.
                if (
                    build_control is not None
                    and build_control.cancel_event.is_set()
                ):
                    raise self._superseded_error(
                        "audit-builder daemon was terminated after supersession"
                    ) from exc
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon input pipe closed"
                ) from exc
            except OSError as exc:
                if (
                    build_control is not None
                    and build_control.cancel_event.is_set()
                ):
                    raise self._superseded_error(
                        "audit-builder daemon was terminated after supersession"
                    ) from exc
                # The spool itself cannot stream. Poison it so the one-shot
                # fallback -- and every later build -- writes the cached
                # in-memory fragments instead of retrying the same transfer
                # and failing mid-stream.
                share_serialization.mark_spool_failed()
                raise _ServeBuilderUnavailable(
                    f"audit-builder daemon spool transfer failed: {exc}"
                ) from exc
            if moved <= 0:
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon input pipe closed"
                )
            offset += moved
        return offset

    def _serve_builder_request_locked(
        self,
        client: _ServeBuilderClient,
        *,
        deadline: float,
        payload: dict[str, object],
        shares: list[dict[str, object]],
        precomposed: tuple[str, str],
        share_serialization: _ShareWindowSerialization,
        cancellation: CancellationPort | None,
        build_control: BundleBuildControlPort | None,
        record_phase_metrics: bool,
    ) -> dict[str, Any]:
        """One JSONL round trip; raises _ServeBuilderUnavailable on anomaly."""
        runtime = self._runtime
        share_snapshot_sha256 = share_serialization.share_snapshot_sha256
        request_fields = dict(payload)
        request_fields["window_key"] = {
            "share_snapshot_sha256": share_snapshot_sha256,
        }
        prefix = json.dumps(request_fields, separators=(",", ":")).encode(
            "utf-8"
        )
        phases = runtime._job_build_phases()
        input_bytes = 0
        input_serialization_elapsed = 0.0

        def send_request(upload_window: bool) -> None:
            nonlocal input_bytes, input_serialization_elapsed
            serialization_started = time.monotonic()
            try:
                if not upload_window:
                    input_bytes += self._serve_builder_write(
                        client,
                        prefix + b"\n",
                        deadline,
                        cancellation,
                        build_control,
                    )
                    return
                input_bytes += self._serve_builder_write(
                    client,
                    prefix[:-1],
                    deadline,
                    cancellation,
                    build_control,
                )
                lease = (
                    share_serialization.acquire_spooled_tail(shares)
                    if hasattr(os, "splice")
                    else None
                )
                if lease is not None:
                    try:
                        spool_file, spool_size = lease
                        input_bytes += self._serve_builder_splice_spool(
                            client,
                            share_serialization,
                            spool_file,
                            spool_size,
                            deadline,
                            cancellation,
                            build_control,
                        )
                    finally:
                        share_serialization.release_spooled_tail()
                else:
                    identities_json, compact_shares_json = precomposed
                    for fragment in (
                        b',"compact_share_identities":',
                        identities_json.encode("utf-8"),
                        b',"compact_shares":',
                        compact_shares_json.encode("utf-8"),
                        b"}",
                    ):
                        input_bytes += self._serve_builder_write(
                            client,
                            fragment,
                            deadline,
                            cancellation,
                            build_control,
                        )
                input_bytes += self._serve_builder_write(
                    client,
                    b"\n",
                    deadline,
                    cancellation,
                    build_control,
                )
            finally:
                elapsed = time.monotonic() - serialization_started
                phases["input_serialization"] = (
                    phases.get("input_serialization", 0.0) + elapsed
                )
                # Accumulated across a possible needs_window re-send and
                # observed once per build so serialization_copy stays
                # comparable with the one-shot transport.
                input_serialization_elapsed += elapsed

        def read_response() -> tuple[dict[str, Any], bytes]:
            worker_started = time.monotonic()
            line = self._serve_builder_read_line(
                client,
                deadline,
                cancellation,
                build_control,
            )
            phases["worker"] = phases.get("worker", 0.0) + (
                time.monotonic() - worker_started
            )
            output_started = time.monotonic()
            try:
                value = json.loads(line)
            except ValueError as exc:
                raise _ServeBuilderUnavailable(
                    f"audit-builder daemon response was malformed: {exc}"
                ) from exc
            finally:
                phases["output_serialization"] = phases.get(
                    "output_serialization",
                    0.0,
                ) + (time.monotonic() - output_started)
            if not isinstance(value, dict):
                raise _ServeBuilderUnavailable(
                    "audit-builder daemon response was not an object"
                )
            return value, line

        upload_window = share_snapshot_sha256 not in client.uploaded_windows
        send_request(upload_window)
        response, response_line = read_response()
        if (
            response.get("ok") is not True
            and bool(response.get("needs_window"))
            and not upload_window
        ):
            # The daemon evicted this window (respawn or generation churn the
            # coordinator's mirror missed); repeat the request with the
            # window riding along.
            upload_window = True
            send_request(True)
            response, response_line = read_response()
        if response.get("ok") is not True:
            raise _ServeBuilderUnavailable(
                "audit-builder daemon error: "
                f"{response.get('error', 'unknown')}"
            )
        summary = response.get("summary")
        if not isinstance(summary, dict):
            raise _ServeBuilderUnavailable(
                "audit-builder daemon returned a malformed summary"
            )
        # Every successful response promotes the window in the mirror, hits
        # included: the daemon moves a hit entry to most-recent position, and
        # a mirror that only tracked uploads would evict a different key and
        # either re-upload a still-cached window or bounce on needs_window.
        client.note_uploaded_window(share_snapshot_sha256)
        if upload_window:
            with self._serve_builder_metrics_lock:
                self.serve_builder_counts["window_uploads"] += 1
        window_cache = response.get("window_cache")
        if isinstance(window_cache, dict):
            outcome = "hits" if bool(window_cache.get("hit")) else "misses"
            with self._serve_builder_metrics_lock:
                self.serve_builder_window_cache_counts[outcome] += 1
        if record_phase_metrics:
            runtime._observe_tip_refresh_build_phase(
                "serialization_copy",
                input_serialization_elapsed,
            )
            metrics_value = response.get("metrics")
            if isinstance(metrics_value, dict):
                try:
                    self._observe_builder_phase_metrics(metrics_value)
                except (TypeError, ValueError):
                    # Metrics are diagnostic only. A malformed timing payload
                    # must never invalidate an otherwise valid summary.
                    pass
            runtime._record_tip_refresh_ipc_bytes("input", input_bytes)
            runtime._record_tip_refresh_ipc_bytes("output", len(response_line))
        return summary

    def _build_audit_bundle_via_serve_builder(
        self,
        *,
        payload: dict[str, object],
        shares: list[dict[str, object]],
        precomposed: tuple[str, str],
        share_serialization: _ShareWindowSerialization,
        cancellation: CancellationPort | None,
        record_phase_metrics: bool,
        deadline: float,
    ) -> dict[str, Any] | None:
        """Build through the persistent daemon; None means use one-shot.

        Any daemon anomaly -- spawn failure, handshake or protocol mismatch,
        crash, timeout, malformed response -- retires the daemon and returns
        None so the caller's one-shot path runs unchanged against the same
        deadline (whatever budget the daemon consumed stays consumed).
        Cancellation and supersession raise exactly like the one-shot path
        instead of falling back.
        """
        runtime = self._runtime
        if not env_bool("PRISM_BUILDER_SERVE", "1"):
            return None
        runtime._ensure_job_cache_state()
        if not getattr(runtime, "signing_seed_hex", None) or not getattr(
            runtime,
            "ledger_attestation_signing_seed_hex",
            None,
        ):
            return None
        if not self._serve_builder_lock.acquire(blocking=False):
            # A concurrent build owns the daemon. One-shot is cheaper than
            # queueing behind a multi-second build.
            return None
        try:
            if self._serve_builder_shutdown:
                return None
            build_control = getattr(
                runtime._job_build_phase_local,
                "bundle_build_control",
                None,
            )
            if not isinstance(build_control, self._build_control_type):
                build_control = None
            try:
                client = self._serve_builder
                if client is not None and client.process.poll() is not None:
                    # The daemon died between requests (its own crash or an
                    # external kill). Record the lifecycle transition before
                    # retiring it so the immediate respawn below reads as a
                    # crash-and-restart rather than an ordinary start.
                    with runtime._job_build_scheduler_lock:
                        runtime.job_build_worker_counts["crashes"] += 1
                        runtime._job_build_worker_restart_pending = True
                    self._retire_serve_builder_locked()
                    client = None
                if client is None:
                    client = self._spawn_serve_builder_locked(
                        deadline,
                        cancellation,
                    )
                    self._serve_builder = client
                    with self._serve_builder_metrics_lock:
                        self.serve_builder_counts["spawns"] += 1
                if build_control is not None:
                    runtime._register_job_bundle_process(
                        build_control,
                        client.process,  # type: ignore[arg-type]
                    )
                try:
                    summary = self._serve_builder_request_locked(
                        client,
                        deadline=deadline,
                        payload=payload,
                        shares=shares,
                        precomposed=precomposed,
                        share_serialization=share_serialization,
                        cancellation=cancellation,
                        build_control=build_control,
                        record_phase_metrics=record_phase_metrics,
                    )
                finally:
                    if build_control is not None:
                        runtime._unregister_job_bundle_process(
                            build_control,
                            client.process,
                        )
            except self._cancellation_error_types:
                # The daemon stream is indeterminate mid-request; retire it
                # so the replacement build starts clean.
                self._retire_serve_builder_locked()
                with runtime._job_build_scheduler_lock:
                    runtime.job_build_worker_counts["terminations"] += 1
                    runtime._job_build_worker_restart_pending = True
                raise
            except _ServeBuilderUnavailable:
                self._record_live_serve_builder_termination(
                    self._serve_builder
                )
                self._retire_serve_builder_locked()
                with self._serve_builder_metrics_lock:
                    self.serve_builder_counts["fallbacks"] += 1
                return None
            except (OSError, ValueError):
                self._record_live_serve_builder_termination(
                    self._serve_builder
                )
                self._retire_serve_builder_locked()
                with self._serve_builder_metrics_lock:
                    self.serve_builder_counts["fallbacks"] += 1
                return None
            else:
                with self._serve_builder_metrics_lock:
                    self.serve_builder_counts["requests"] += 1
                return summary
        finally:
            self._serve_builder_lock.release()

    def build_audit_bundle(
        self,
        *,
        shares: list[dict[str, object]],
        found_block: dict[str, object],
        prior_balances: list[dict[str, object]],
        coinbase_script_sig_suffix_hex: str,
        witness_merkle_leaves_hex: list[str] | None = None,
        ctv_fee_parent_hash: str | None = None,
        canonical_output_path: Path | None = None,
        summary_only: bool = False,
        payout_policy: dict[str, object] | None = None,
        ctv_settlement: dict[str, object] | None = None,
        cancellation: CancellationPort | None = None,
        share_serialization: _ShareWindowSerialization | None = None,
    ) -> dict[str, Any]:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        runtime._ensure_tip_refresh_state()
        if cancellation is not None:
            runtime._job_build_checkpoint("serialization", cancellation)
        payload: dict[str, object] = {
            "found_block": found_block,
            "prior_balances": prior_balances,
            "payout_policy": (
                runtime.prism_payout_policy()
                if payout_policy is None
                else payout_policy
            ),
            "coinbase_script_sig_suffix_hex": coinbase_script_sig_suffix_hex,
            "witness_merkle_leaves_hex": witness_merkle_leaves_hex or [],
        }
        job_build_phase_local = getattr(runtime, "_job_build_phase_local", None)
        record_phase_metrics = bool(
            getattr(job_build_phase_local, "tip_refresh_metrics", False)
        )
        precomposed: tuple[str, str] | None = None
        if summary_only:
            artifact_started = time.monotonic()
            if (
                share_serialization is not None
                and share_serialization.share_count == len(shares)
            ):
                # The compact conversion and its encoded fragments are pure
                # per-generation functions of the share window; reuse the
                # cached strings instead of re-walking the window.
                precomposed = share_serialization.compact_fragments(shares)
            else:
                identities, compact_shares = _compact_share_payload(shares)
                payload["compact_share_identities"] = identities
                payload["compact_shares"] = compact_shares
            if record_phase_metrics:
                runtime._observe_tip_refresh_build_phase(
                    "serialization_copy",
                    time.monotonic() - artifact_started,
                )
        else:
            payload["shares"] = shares
        if ctv_settlement is None and payout_policy is None:
            ctv_settlement = runtime.prism_ctv_settlement_config(
                block_height=int(found_block["block_height"]),
                parent_hash=ctv_fee_parent_hash,
            )
        if ctv_settlement is not None:
            payload["ctv_settlement"] = ctv_settlement
        if canonical_output_path is not None and summary_only:
            raise ValueError("canonical output and job summary output are mutually exclusive")
        # One deadline covers the whole build regardless of transport: a
        # daemon anomaly falls back to the one-shot subprocess with only the
        # remaining budget, so a hung daemon cannot double the configured
        # limit while progress health already treats one limit as stuck.
        build_deadline = time.monotonic() + float(
            getattr(
                runtime,
                "bundle_build_timeout_seconds",
                DEFAULT_PRISM_BUNDLE_BUILD_TIMEOUT_SECONDS,
            )
        )
        if (
            summary_only
            and canonical_output_path is None
            and precomposed is not None
            and share_serialization is not None
        ):
            # The persistent builder serves the artifact-backed summary path,
            # whose window identity and cached fragments it can key on. Every
            # anomaly falls back to the one-shot subprocess below.
            served = self._build_audit_bundle_via_serve_builder(
                payload=payload,
                shares=shares,
                precomposed=precomposed,
                share_serialization=share_serialization,
                cancellation=cancellation,
                record_phase_metrics=record_phase_metrics,
                deadline=build_deadline,
            )
            if served is not None:
                return served
        command = self._tool_command("qbit-prism-build-audit-bundle") + [
            "--input",
            "-",
            "--signing-key-seed-hex",
            runtime.signing_seed_hex,
            "--ledger-signing-key-seed-hex",
            runtime.ledger_attestation_signing_seed_hex,
        ]
        command.append("--job-summary-output" if summary_only else "--canonical-output")
        if record_phase_metrics:
            command.append("--phase-metrics")
        if canonical_output_path is not None:
            canonical_output_path.parent.mkdir(parents=True, exist_ok=True)
        succeeded = False
        created_output = False
        try:
            with ExitStack() as stack:
                if canonical_output_path is None:
                    output = stack.enter_context(
                        tempfile.TemporaryFile(mode="w+", encoding="utf-8")
                    )
                else:
                    output = stack.enter_context(
                        canonical_output_path.open("x+", encoding="utf-8")
                    )
                    created_output = True
                stderr = stack.enter_context(
                    tempfile.TemporaryFile(mode="w+", encoding="utf-8")
                )
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=output,
                    stderr=stderr,
                    text=True,
                    encoding="utf-8",
                    close_fds=True,
                )
                build_control = getattr(
                    job_build_phase_local,
                    "bundle_build_control",
                    None,
                )
                if isinstance(build_control, self._build_control_type):
                    runtime._register_job_bundle_process(build_control, process)
                with runtime._job_build_scheduler_lock:
                    if runtime._job_build_worker_restart_pending:
                        runtime.job_build_worker_counts["restarts"] += 1
                        runtime._job_build_worker_restart_pending = False
                    runtime.job_build_worker_counts["starts"] += 1
                assert process.stdin is not None
                input_byte_count = 0
                worker_deadline = build_deadline
                killed_process_wait_seconds = max(
                    0.001,
                    float(
                        getattr(
                            runtime,
                            "job_build_cancel_grace_seconds",
                            DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
                        )
                    ),
                )
                coordinator = runtime
                build_control_type = self._build_control_type
                superseded_error = self._superseded_error

                class _CancelableInput:
                    def __init__(self, stream: Any) -> None:
                        self.stream = stream
                        try:
                            file_descriptor = int(stream.fileno())
                        except (AttributeError, OSError, TypeError, ValueError):
                            # Lightweight process fakes used by embedders and
                            # tests do not necessarily expose an OS pipe.
                            self.file_descriptor: int | None = None
                        else:
                            os.set_blocking(file_descriptor, False)
                            self.file_descriptor = file_descriptor

                    def check_cancelled(self) -> None:
                        if cancellation is not None:
                            cancellation.raise_if_cancelled(
                                "builder input serialization"
                            )
                        if (
                            isinstance(build_control, build_control_type)
                            and build_control.cancel_event.is_set()
                        ):
                            raise superseded_error(
                                "audit-builder input was canceled after supersession"
                            )
                        if time.monotonic() >= worker_deadline:
                            with coordinator._tip_refresh_metrics_lock:
                                coordinator.tip_refresh_worker_failures += 1
                            raise RuntimeError(
                                "qbit-prism-build-audit-bundle timed out"
                            )

                    def write(self, value: str) -> int:
                        nonlocal input_byte_count
                        self.check_cancelled()
                        if self.file_descriptor is None:
                            written = int(self.stream.write(value))
                            input_byte_count += len(value[:written].encode("utf-8"))
                            return written
                        encoded = value.encode("utf-8")
                        remaining = memoryview(encoded)
                        while remaining:
                            self.check_cancelled()
                            try:
                                written = os.write(
                                    self.file_descriptor,
                                    remaining,
                                )
                            except (BlockingIOError, InterruptedError):
                                time.sleep(
                                    min(
                                        0.02,
                                        PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
                                    )
                                )
                                continue
                            if written <= 0:
                                raise BrokenPipeError(
                                    "audit-builder input pipe closed"
                                )
                            input_byte_count += written
                            remaining = remaining[written:]
                        return len(value)

                serialization_started = time.monotonic()
                spool_lease: tuple[Any, int] | None = None
                try:
                    sink = _CancelableInput(process.stdin)

                    def write_precomposed_tail() -> None:
                        assert precomposed is not None
                        identities_json, compact_shares_json = precomposed
                        sink.write(',"compact_share_identities":')
                        sink.write(identities_json)
                        sink.write(',"compact_shares":')
                        sink.write(compact_shares_json)
                        sink.write("}")

                    if (
                        precomposed is not None
                        and share_serialization is not None
                        and sink.file_descriptor is not None
                        and hasattr(os, "splice")
                    ):
                        spool_lease = share_serialization.acquire_spooled_tail(
                            shares
                        )
                    if precomposed is not None:
                        # Per-build fields are encoded fresh; the dominant
                        # share-window fragments stream from the spool file
                        # written once per generation (kernel moves the bytes
                        # into the pipe), falling back to the cached strings
                        # whenever no spool is available.
                        prefix = json.dumps(payload, separators=(",", ":"))
                        sink.write(prefix[:-1])
                        spool_transferred = False
                        if spool_lease is not None:
                            spool_file, spool_size = spool_lease
                            spool_fd = spool_file.fileno()
                            offset = 0
                            while offset < spool_size:
                                sink.check_cancelled()
                                try:
                                    moved = os.splice(
                                        spool_fd,
                                        sink.file_descriptor,
                                        min(
                                            PRISM_SPOOL_SPLICE_CHUNK_BYTES,
                                            spool_size - offset,
                                        ),
                                        offset_src=offset,
                                    )
                                except (BlockingIOError, InterruptedError):
                                    time.sleep(
                                        min(
                                            0.02,
                                            PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
                                        )
                                    )
                                    continue
                                except BrokenPipeError:
                                    # The child's stdin closed; the spool
                                    # itself is fine and the surrounding
                                    # pipe-error handling owns this.
                                    raise
                                except OSError:
                                    # The spool cannot stream (unsupported
                                    # filesystem or an I/O error); the cached
                                    # in-memory fragments stay authoritative
                                    # for this and every later build.
                                    share_serialization.mark_spool_failed()
                                    if offset:
                                        # Part of the tail already reached the
                                        # pipe; this build cannot be repaired
                                        # by re-writing it from memory.
                                        raise
                                    break
                                if moved <= 0:
                                    raise BrokenPipeError(
                                        "audit-builder input pipe closed"
                                    )
                                offset += moved
                                input_byte_count += moved
                            spool_transferred = (
                                spool_size > 0 and offset >= spool_size
                            )
                        if not spool_transferred:
                            write_precomposed_tail()
                    else:
                        # iterencode writes bounded fragments to the child
                        # instead of allocating a second full JSON
                        # representation in Python.
                        json.dump(
                            payload,
                            sink,
                            separators=(",", ":"),
                        )
                except BrokenPipeError:
                    # Prefer the builder's diagnostic below.
                    pass
                except BaseException as exc:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=killed_process_wait_seconds)
                    except subprocess.TimeoutExpired:
                        pass
                    if isinstance(exc, self._cancellation_error_types):
                        with runtime._job_build_scheduler_lock:
                            runtime.job_build_worker_counts["terminations"] += 1
                            runtime._job_build_worker_restart_pending = True
                    raise
                finally:
                    if spool_lease is not None and share_serialization is not None:
                        share_serialization.release_spooled_tail()
                    phases = runtime._job_build_phases()
                    phases["input_serialization"] = phases.get(
                        "input_serialization",
                        0.0,
                    ) + (time.monotonic() - serialization_started)
                    try:
                        process.stdin.close()
                    except (BlockingIOError, BrokenPipeError):
                        pass
                if record_phase_metrics:
                    runtime._observe_tip_refresh_build_phase(
                        "serialization_copy",
                        time.monotonic() - serialization_started,
                    )
                    runtime._record_tip_refresh_ipc_bytes("input", input_byte_count)
                worker_started = time.monotonic()
                terminated = False
                returncode: int | None = None
                if cancellation is None and not hasattr(process, "poll"):
                    returncode = process.wait(
                        timeout=max(0.001, worker_deadline - time.monotonic())
                    )
                else:
                    while returncode is None:
                        returncode = process.poll()
                        if returncode is not None:
                            break
                        cancelled_by_control = (
                            isinstance(build_control, self._build_control_type)
                            and build_control.cancel_event.is_set()
                        )
                        cancelled_by_request = (
                            cancellation is not None and cancellation.is_set()
                        )
                        if cancelled_by_control or cancelled_by_request:
                            terminated = True
                            process.terminate()
                            try:
                                returncode = process.wait(
                                    timeout=max(
                                        0.0,
                                        float(
                                            getattr(
                                                runtime,
                                                "job_build_cancel_grace_seconds",
                                                DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
                                            )
                                        ),
                                    )
                                )
                            except subprocess.TimeoutExpired:
                                process.kill()
                                try:
                                    returncode = process.wait(
                                        timeout=killed_process_wait_seconds
                                    )
                                except subprocess.TimeoutExpired:
                                    returncode = process.poll()
                            with runtime._job_build_scheduler_lock:
                                runtime.job_build_worker_counts["terminations"] += 1
                                runtime._job_build_worker_restart_pending = True
                            break
                        if time.monotonic() >= worker_deadline:
                            process.kill()
                            try:
                                returncode = process.wait(
                                    timeout=killed_process_wait_seconds
                                )
                            except subprocess.TimeoutExpired:
                                returncode = process.poll()
                            with runtime._tip_refresh_metrics_lock:
                                runtime.tip_refresh_worker_failures += 1
                            raise RuntimeError(
                                "qbit-prism-build-audit-bundle timed out"
                            )
                        time.sleep(min(0.02, PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS))
                phases["worker"] = phases.get("worker", 0.0) + (
                    time.monotonic() - worker_started
                )
                if terminated and cancellation is not None:
                    if cancellation.is_set():
                        cancellation.raise_if_cancelled("builder worker")
                if (
                    terminated
                    and isinstance(build_control, self._build_control_type)
                    and build_control.cancel_event.is_set()
                ):
                    raise self._superseded_error(
                        "audit-builder subprocess was canceled after supersession"
                    )
                stderr.seek(0)
                error_text = stderr.read()
                if returncode != 0:
                    with runtime._job_build_scheduler_lock:
                        runtime.job_build_worker_counts["crashes"] += 1
                        runtime._job_build_worker_restart_pending = True
                    if record_phase_metrics:
                        with runtime._tip_refresh_metrics_lock:
                            runtime.tip_refresh_worker_failures += 1
                    raise RuntimeError(
                        f"qbit-prism-build-audit-bundle failed: {error_text}"
                    )
                if (
                    isinstance(build_control, self._build_control_type)
                    and build_control.cancel_event.is_set()
                ):
                    raise self._superseded_error(
                        "audit-builder result completed after supersession"
                    )
                output.flush()
                output_size = os.fstat(output.fileno()).st_size
                if record_phase_metrics:
                    runtime._record_tip_refresh_ipc_bytes("output", output_size)
                    for line in error_text.splitlines():
                        if not line.startswith(PRISM_BUILDER_PHASE_METRICS_PREFIX):
                            continue
                        raw_metrics = line.removeprefix(
                            PRISM_BUILDER_PHASE_METRICS_PREFIX
                        )
                        try:
                            metrics = json.loads(raw_metrics)
                            if isinstance(metrics, dict):
                                self._observe_builder_phase_metrics(metrics)
                        except (TypeError, ValueError, json.JSONDecodeError):
                            # Metrics are diagnostic only. A malformed timing
                            # line must never invalidate an otherwise valid
                            # signed bundle.
                            pass
                if canonical_output_path is not None:
                    os.fsync(output.fileno())
                output.seek(0)
                output_started = time.monotonic()
                if cancellation is not None:
                    cancellation.raise_if_cancelled("builder output serialization")
                bundle = json.load(output)
                phases["output_serialization"] = phases.get(
                    "output_serialization",
                    0.0,
                ) + (time.monotonic() - output_started)
                if cancellation is not None:
                    cancellation.raise_if_cancelled("builder verification")
            succeeded = True
            return bundle
        finally:
            if canonical_output_path is not None and created_output and not succeeded:
                try:
                    canonical_output_path.unlink()
                except FileNotFoundError:
                    pass


__all__ = [
    "BundleBuildControlPort",
    "BundleCompiler",
    "BundleCompilerRuntime",
    "CancellationPort",
    "PRISM_BUILDER_PHASE_METRICS_PREFIX",
    "PRISM_SERVE_BUILDER_PROTOCOL_VERSION",
    "PRISM_SERVE_BUILDER_WINDOW_CACHE_ENTRIES",
    "PRISM_SPOOL_SPLICE_CHUNK_BYTES",
    "PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS",
    "_ServeBuilderClient",
    "_ServeBuilderUnavailable",
    "_ShareWindowSerialization",
    "_compact_share_payload",
    "_share_window_spool_file",
]

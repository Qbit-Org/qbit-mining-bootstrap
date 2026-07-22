"""Cancelable subprocess adapter for PRISM audit-bundle compilation."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from typing import Any, Callable, Protocol

from lab.prism.prism_tools import prism_tool_command


PRISM_BUILDER_PHASE_METRICS_PREFIX = "qbit-prism-build-phase-metrics "
PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05


class CancellationPort(Protocol):
    def is_set(self) -> bool: ...

    def raise_if_cancelled(self, phase: str) -> None: ...


class BundleBuildControlPort(Protocol):
    cancel_event: object
    process: subprocess.Popen[str] | None


@dataclass(frozen=True)
class BundleCompilerPorts:
    payout_policy: Callable[[], dict[str, object]]
    ctv_settlement: Callable[[int, str | None], dict[str, object] | None]
    signing_seed_hex: Callable[[], str]
    ledger_signing_seed_hex: Callable[[], str]
    bundle_timeout_seconds: Callable[[], float]
    cancel_grace_seconds: Callable[[], float]
    phases: Callable[[], dict[str, float]]
    record_tip_refresh_phase: Callable[[str, float], None]
    record_ipc_bytes: Callable[[str, int], None]
    record_worker_failure: Callable[[], None]
    record_worker_event: Callable[[str], None]
    tip_refresh_metrics_enabled: Callable[[], bool]
    active_build_control: Callable[[], BundleBuildControlPort | None]
    register_process: Callable[
        [BundleBuildControlPort, subprocess.Popen[str]], None
    ]
    superseded_error: Callable[[str], BaseException]


class BundleCompiler:
    """Compile summaries or canonical bundles with exact cancellation rules."""

    def __init__(
        self,
        ports: BundleCompilerPorts,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ports = ports
        self._monotonic = monotonic

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
    ) -> dict[str, Any]:
        if cancellation is not None:
            cancellation.raise_if_cancelled("serialization")
        payload: dict[str, object] = {
            "found_block": found_block,
            "prior_balances": prior_balances,
            "payout_policy": (
                self._ports.payout_policy()
                if payout_policy is None
                else payout_policy
            ),
            "coinbase_script_sig_suffix_hex": coinbase_script_sig_suffix_hex,
            "witness_merkle_leaves_hex": witness_merkle_leaves_hex or [],
        }
        record_phase_metrics = self._ports.tip_refresh_metrics_enabled()
        serialization_copy_seconds = 0.0
        if summary_only:
            artifact_started = self._monotonic()
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
            payload["compact_share_identities"] = identities
            payload["compact_shares"] = compact_shares
            if record_phase_metrics:
                serialization_copy_seconds += self._monotonic() - artifact_started
        else:
            payload["shares"] = shares
        if ctv_settlement is None and payout_policy is None:
            ctv_settlement = self._ports.ctv_settlement(
                int(found_block["block_height"]),
                ctv_fee_parent_hash,
            )
        if ctv_settlement is not None:
            payload["ctv_settlement"] = ctv_settlement
        if canonical_output_path is not None and summary_only:
            raise ValueError(
                "canonical output and job summary output are mutually exclusive"
            )
        command = prism_tool_command("qbit-prism-build-audit-bundle") + [
            "--input",
            "-",
            "--signing-key-seed-hex",
            self._ports.signing_seed_hex(),
            "--ledger-signing-key-seed-hex",
            self._ports.ledger_signing_seed_hex(),
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
                build_control = self._ports.active_build_control()
                if build_control is not None:
                    self._ports.register_process(build_control, process)
                self._ports.record_worker_event("start")
                assert process.stdin is not None
                input_byte_count = 0
                builder_started = self._monotonic()
                worker_deadline = (
                    builder_started + self._ports.bundle_timeout_seconds()
                )
                compiler = self

                class _CancelableInput:
                    def __init__(self, stream: Any) -> None:
                        self.stream = stream
                        try:
                            file_descriptor = int(stream.fileno())
                        except (AttributeError, OSError, TypeError, ValueError):
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
                            build_control is not None
                            and build_control.cancel_event.is_set()  # type: ignore[attr-defined]
                        ):
                            raise compiler._ports.superseded_error(
                                "audit-builder input was canceled after supersession"
                            )
                        if compiler._monotonic() >= worker_deadline:
                            compiler._ports.record_worker_failure()
                            raise RuntimeError(
                                "qbit-prism-build-audit-bundle timed out"
                            )

                    def write(self, value: str) -> int:
                        nonlocal input_byte_count
                        self.check_cancelled()
                        if self.file_descriptor is None:
                            written = int(self.stream.write(value))
                            input_byte_count += len(
                                value[:written].encode("utf-8")
                            )
                            return written
                        encoded = value.encode("utf-8")
                        remaining = memoryview(encoded)
                        while remaining:
                            self.check_cancelled()
                            try:
                                written = os.write(self.file_descriptor, remaining)
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

                serialization_started = self._monotonic()
                try:
                    json.dump(
                        payload,
                        _CancelableInput(process.stdin),
                        separators=(",", ":"),
                    )
                except BrokenPipeError:
                    pass
                except BaseException:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    process.wait()
                    if (
                        cancellation is not None
                        and cancellation.is_set()
                    ) or (
                        build_control is not None
                        and build_control.cancel_event.is_set()  # type: ignore[attr-defined]
                    ):
                        self._ports.record_worker_event("termination")
                    raise
                finally:
                    input_serialization_seconds = (
                        self._monotonic() - serialization_started
                    )
                    phases = self._ports.phases()
                    phases["input_serialization"] = phases.get(
                        "input_serialization",
                        0.0,
                    ) + input_serialization_seconds
                    if record_phase_metrics:
                        serialization_copy_seconds += input_serialization_seconds
                    try:
                        process.stdin.close()
                    except (BlockingIOError, BrokenPipeError):
                        pass
                if record_phase_metrics:
                    self._ports.record_ipc_bytes("input", input_byte_count)
                worker_started = self._monotonic()
                returncode: int | None = None
                if cancellation is None and not hasattr(process, "poll"):
                    returncode = process.wait()
                else:
                    while returncode is None:
                        returncode = process.poll()
                        if returncode is not None:
                            break
                        cancelled_by_control = (
                            build_control is not None
                            and build_control.cancel_event.is_set()  # type: ignore[attr-defined]
                        )
                        cancelled_by_request = (
                            cancellation is not None and cancellation.is_set()
                        )
                        if cancelled_by_control or cancelled_by_request:
                            process.terminate()
                            try:
                                returncode = process.wait(
                                    timeout=max(
                                        0.0,
                                        self._ports.cancel_grace_seconds(),
                                    )
                                )
                            except subprocess.TimeoutExpired:
                                process.kill()
                                returncode = process.wait()
                            self._ports.record_worker_event("termination")
                            break
                        if self._monotonic() >= worker_deadline:
                            process.kill()
                            returncode = process.wait()
                            self._ports.record_worker_failure()
                            raise RuntimeError(
                                "qbit-prism-build-audit-bundle timed out"
                            )
                        time.sleep(
                            min(0.02, PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS)
                        )
                phases["worker"] = phases.get("worker", 0.0) + (
                    self._monotonic() - worker_started
                )
                if cancellation is not None and cancellation.is_set():
                    cancellation.raise_if_cancelled("builder worker")
                if (
                    build_control is not None
                    and build_control.cancel_event.is_set()  # type: ignore[attr-defined]
                ):
                    raise self._ports.superseded_error(
                        "audit-builder subprocess was canceled after supersession"
                    )
                stderr.seek(0)
                error_text = stderr.read()
                if returncode != 0:
                    self._ports.record_worker_event("crash")
                    if record_phase_metrics:
                        self._ports.record_worker_failure()
                    raise RuntimeError(
                        f"qbit-prism-build-audit-bundle failed: {error_text}"
                    )
                if (
                    build_control is not None
                    and build_control.cancel_event.is_set()  # type: ignore[attr-defined]
                ):
                    raise self._ports.superseded_error(
                        "audit-builder result completed after supersession"
                    )
                output.flush()
                output_size = os.fstat(output.fileno()).st_size
                if record_phase_metrics:
                    self._ports.record_ipc_bytes("output", output_size)
                if canonical_output_path is not None:
                    os.fsync(output.fileno())
                output.seek(0)
                output_started = self._monotonic()
                if cancellation is not None:
                    cancellation.raise_if_cancelled(
                        "builder output serialization"
                    )
                bundle = json.load(output)
                output_serialization_seconds = self._monotonic() - output_started
                phases["output_serialization"] = phases.get(
                    "output_serialization",
                    0.0,
                ) + output_serialization_seconds
                if record_phase_metrics:
                    serialization_copy_seconds += output_serialization_seconds
                    self._record_phase_metrics(
                        error_text,
                        serialization_copy_seconds=serialization_copy_seconds,
                    )
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

    def _record_phase_metrics(
        self,
        error_text: str,
        *,
        serialization_copy_seconds: float,
    ) -> None:
        rust_serialization = 0.0
        for line in error_text.splitlines():
            if not line.startswith(PRISM_BUILDER_PHASE_METRICS_PREFIX):
                continue
            raw_metrics = line.removeprefix(PRISM_BUILDER_PHASE_METRICS_PREFIX)
            try:
                metrics = json.loads(raw_metrics)
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
                            self._ports.record_tip_refresh_phase(
                                phase,
                                float(elapsed),
                            )
                rust_serialization += sum(
                    float(metrics.get(name, 0.0))
                    for name in (
                        "input_deserialization_seconds",
                        "output_serialization_seconds",
                    )
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        self._ports.record_tip_refresh_phase(
            "serialization_copy",
            serialization_copy_seconds + rust_serialization,
        )

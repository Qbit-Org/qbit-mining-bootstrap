"""Non-discovered PostgreSQL/A1 cross-process integration gate."""

from __future__ import annotations

from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import subprocess
import sys
import tempfile
import time
import traceback
from typing import Any

from lab.prism import audit_artifacts as audit_artifacts_module
from lab.prism.audit_artifacts import (
    AuditArtifactConfig,
    AuditArtifactStore,
    AuditPublicationIdentity,
)
from lab.prism.share_ledger import PsqlShareLedger
from tests import prism_postgres_a1_gate as support
from tests import prism_postgres_a1_migration_gate as migration


_PROCESS_GATE_MODULE = "tests.prism_postgres_a1_process_gate"
_C2_DIGEST = "11" * 32


class _WorkerScopedPsqlLedger(PsqlShareLedger):
    """Child-process ledger that consumes, but never owns, the parent schema."""

    def __init__(
        self,
        *,
        test_schema: str,
        application_name: str,
        **kwargs: object,
    ) -> None:
        if support.SCHEMA_PATTERN.fullmatch(test_schema) is None:
            raise support.GateFailure(f"invalid worker schema: {test_schema!r}")
        if (
            re.fullmatch(r"qbit_a1_c[23]_[a-z0-9_]+", application_name) is None
            or len(application_name) > 63
        ):
            raise support.GateFailure(
                f"invalid process worker application name: {application_name!r}"
            )
        self._worker_schema = test_schema
        self._application_name = application_name
        kwargs["psql_command"] = support.BASE_PSQL_COMMAND
        kwargs["native_client_mode"] = "psql"
        super().__init__(**kwargs)  # type: ignore[arg-type]

    def _run_sql(self, sql: str) -> str:
        return support.run_psql(
            f"SET application_name = '{self._application_name}';\n" + sql,
            schema=self._worker_schema,
        )


def _c2_report(*, block_height: int) -> dict[str, object]:
    return {
        "schema": "qbit.prism.audit-verification-report.v1",
        "block_height": block_height,
        "audit_bundle_sha256_hex": _C2_DIGEST,
        "reward_manifest_sha256_hex": "44" * 32,
        "payout_policy_manifest_sha256_hex": "55" * 32,
        "prism_audit_commitment_leaf_hex": "66" * 32,
        "audit_commitment_root_hex": "77" * 32,
        "coinbase_txid": "22" * 32,
        "coinbase_wtxid": "88" * 32,
        "coinbase_manifest_sha256_hex": "33" * 32,
        "coinbase_tx_hex": "00",
        "coinbase_value_sats": 1,
        "min_output_sats": 1,
        "onchain_output_count": 0,
        "accrued_account_count": 0,
    }


def _c2_store(root: Path, evidence_path: Path) -> AuditArtifactStore:
    return AuditArtifactStore(
        AuditArtifactConfig(
            root=root,
            evidence_path=evidence_path,
            live_bundle_retention=1,
            candidate_retention_seconds=60,
            share_segment_size=0,
        )
    )


def _c2_publish(
    store: AuditArtifactStore,
    *,
    identity: AuditPublicationIdentity,
    publication_floor_sequence: int,
    created_at: str,
) -> object:
    report = _c2_report(block_height=identity.block_height)
    verification_identity = store.build_verification_identity(
        trust_source="configured",
        trusted_writer_public_key_hex="44" * 32,
        literal_sha256=_C2_DIGEST,
        literal_byte_len=123,
        report=report,
    )
    return store.publish_success(
        identity=identity,
        publication_floor_sequence=publication_floor_sequence,
        report=report,
        persistence={
            "audit_bundle_sha256": _C2_DIGEST,
            "body_uri": "",
        },
        evidence={
            "accepted_share_count": 0,
            "distinct_miner_count": 0,
        },
        verification_identity=verification_identity,
        created_at=created_at,
    )


def _c2_authority(store: AuditArtifactStore) -> dict[str, list[int]]:
    root_value = os.fstat(store._root_fd)
    lock_value = os.fstat(store._publication_lock_fd)
    return {
        "root": [root_value.st_dev, root_value.st_ino],
        "lock": [lock_value.st_dev, lock_value.st_ino],
    }


def _c2_path_snapshot(path: Path) -> dict[str, object]:
    try:
        value = path.lstat()
    except FileNotFoundError:
        return {"exists": False}
    payload: dict[str, object] = {
        "exists": True,
        "device": value.st_dev,
        "inode": value.st_ino,
        "mode": value.st_mode,
        "size": value.st_size,
        "mtime_ns": value.st_mtime_ns,
    }
    if path.is_file() and not path.is_symlink():
        payload["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return payload


def _c2_filesystem_snapshot(
    *,
    root: Path,
    evidence_path: Path,
) -> dict[str, object]:
    return {
        "root": _c2_path_snapshot(root),
        "entries": {
            path.name: _c2_path_snapshot(path)
            for path in sorted(root.iterdir(), key=lambda value: value.name)
        },
        "evidence": _c2_path_snapshot(evidence_path),
    }


def _c2_database_snapshot(
    ledger: PsqlShareLedger,
    *,
    block_hashes: tuple[str, ...],
) -> dict[str, object]:
    quoted = ", ".join(f"'{block_hash}'" for block_hash in block_hashes)
    return ledger._run_json(
        f"""
SELECT json_build_object(
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'block_hash', block_hash,
            'block_height', block_height,
            'chain_state', chain_state,
            'maturity_state', maturity_state,
            'audit_publication_sequence', audit_publication_sequence
        ) ORDER BY block_hash)
        FROM qbit_pool_blocks
        WHERE block_hash IN ({quoted})
    ), '[]'::json),
    'floor', COALESCE((
        SELECT MAX(audit_publication_sequence)
        FROM qbit_pool_blocks
    ), 0),
    'allocator', (
        SELECT json_build_object(
            'last_value', last_value,
            'is_called', is_called
        )
        FROM qbit_audit_publication_sequence_seq
    )
);
"""
    )


def _emit_worker_event(event: str, **payload: object) -> None:
    print(
        json.dumps(
            {"event": event, **payload},
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )


def _read_worker_command(expected: str) -> dict[str, object]:
    line = sys.stdin.readline(support.PSQL_OUTPUT_LIMIT_BYTES + 2)
    if not line:
        raise support.GateFailure(f"worker command {expected!r} was not received")
    if len(line.encode("utf-8")) > support.PSQL_OUTPUT_LIMIT_BYTES:
        raise support.GateFailure("worker command exceeded 1 MiB")
    try:
        payload = json.loads(line)
    except json.JSONDecodeError as error:
        raise support.GateFailure("worker command is not JSON") from error
    if not isinstance(payload, dict) or payload.get("command") != expected:
        raise support.GateFailure(
            f"expected worker command {expected!r}, got {payload!r}"
        )
    return payload


class _JsonWorker:
    def __init__(self, config_path: Path) -> None:
        self._stderr = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        self._captured = 0
        self._event_buffer = bytearray()
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                _PROCESS_GATE_MODULE,
                "--c2-worker",
                str(config_path),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._stderr,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        migration._register_process(self.process)

    def _stderr_text(self) -> str:
        raw = os.pread(
            self._stderr.fileno(),
            support.PSQL_OUTPUT_LIMIT_BYTES + 1,
            0,
        )
        if len(raw) > support.PSQL_OUTPUT_LIMIT_BYTES:
            return "worker stderr exceeded 1 MiB"
        return raw.decode("utf-8", errors="replace").strip()

    def read_event(self, expected: str) -> dict[str, object]:
        if self.process.stdout is None:
            raise support.GateFailure("JSON worker has no stdout")
        deadline = time.monotonic() + support.PSQL_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            newline = self._event_buffer.find(b"\n")
            if newline >= 0:
                raw_line = bytes(self._event_buffer[:newline])
                del self._event_buffer[: newline + 1]
                try:
                    line = raw_line.decode("utf-8")
                except UnicodeDecodeError as error:
                    raise support.GateFailure(
                        "JSON worker emitted non-UTF-8 protocol bytes"
                    ) from error
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as error:
                    raise support.GateFailure(
                        f"JSON worker emitted invalid protocol line: {line!r}"
                    ) from error
                if not isinstance(payload, dict):
                    raise support.GateFailure("JSON worker event is not an object")
                if payload.get("event") == "error":
                    raise support.GateFailure(
                        f"JSON worker failed: {payload.get('message')}; "
                        f"stderr={self._stderr_text()}"
                    )
                if payload.get("event") != expected:
                    raise support.GateFailure(
                        f"expected JSON worker event {expected!r}, got {payload!r}"
                    )
                return payload
            remaining = max(0.0, deadline - time.monotonic())
            ready, _, _ = select.select([self.process.stdout], [], [], remaining)
            if not ready:
                break
            chunk = os.read(self.process.stdout.fileno(), 64 * 1024)
            if not chunk:
                break
            self._captured += len(chunk)
            if self._captured > support.PSQL_OUTPUT_LIMIT_BYTES:
                raise support.GateFailure("JSON worker stdout exceeded 1 MiB")
            self._event_buffer.extend(chunk)
            if (
                b"\n" not in self._event_buffer
                and len(self._event_buffer) > support.PSQL_OUTPUT_LIMIT_BYTES
            ):
                raise support.GateFailure(
                    "JSON worker event line exceeded 1 MiB"
                )
        raise support.GateFailure(
            f"JSON worker event {expected!r} timed out; "
            f"exit={self.process.poll()} stderr={self._stderr_text()}"
        )

    def send(self, command: str) -> None:
        if self.process.stdin is None or self.process.stdin.closed:
            raise support.GateFailure("JSON worker has no writable stdin")
        self.process.stdin.write(
            json.dumps(
                {"command": command},
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )
        self.process.stdin.flush()

    def wait_success(self) -> None:
        if self.process.stdin is not None and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=support.PSQL_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as error:
            support._terminate_and_reap(self.process)
            raise support.GateFailure("JSON worker timed out during exit") from error
        finally:
            migration._forget_process(self.process)
        residual = bytes(self._event_buffer)
        self._event_buffer.clear()
        if self.process.stdout is not None:
            while True:
                chunk = os.read(self.process.stdout.fileno(), 64 * 1024)
                if not chunk:
                    break
                residual += chunk
                self._captured += len(chunk)
                if self._captured > support.PSQL_OUTPUT_LIMIT_BYTES:
                    raise support.GateFailure("JSON worker stdout exceeded 1 MiB")
        stderr = self._stderr_text()
        if self.process.returncode != 0:
            raise support.GateFailure(
                f"JSON worker exited {self.process.returncode}: {stderr}"
            )
        if residual.strip():
            raise support.GateFailure(
                "JSON worker emitted unexpected residual output: "
                + residual.decode("utf-8", errors="replace").strip()
            )
        if stderr:
            raise support.GateFailure(
                f"JSON worker emitted unexpected stderr: {stderr}"
            )

    def close(self) -> None:
        if self.process.poll() is None:
            support._terminate_and_reap(self.process)
        migration._forget_process(self.process)
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream is not None and not stream.closed:
                stream.close()
        if not self._stderr.closed:
            self._stderr.close()


def _c2_worker_ledger(config: dict[str, object]) -> _WorkerScopedPsqlLedger:
    writer = config.get("writer")
    if not isinstance(writer, dict):
        raise support.GateFailure("process worker writer configuration is invalid")
    return _WorkerScopedPsqlLedger(
        test_schema=str(config["schema"]),
        application_name=str(config["application_name"]),
        writer_id=str(writer["id"]),
        writer_epoch=int(writer["epoch"]),
        writer_session_token=str(writer["token"]),
        initialize_schema=False,
    )


def _c2_worker_store(config: dict[str, object]) -> AuditArtifactStore:
    return _c2_store(
        Path(str(config["root"])),
        Path(str(config["evidence_path"])),
    )


def _c2_worker_identity(
    config: dict[str, object],
    key: str,
    *,
    sequence: int,
) -> AuditPublicationIdentity:
    value = config.get(key)
    if not isinstance(value, dict):
        raise support.GateFailure(f"process worker {key} identity is invalid")
    return AuditPublicationIdentity(
        sequence,
        int(value["height"]),
        str(value["hash"]),
    )


def _run_c2_a_worker(config: dict[str, object]) -> None:
    ledger: _WorkerScopedPsqlLedger | None = None
    store: AuditArtifactStore | None = None
    try:
        ledger = _c2_worker_ledger(config)
        store = _c2_worker_store(config)
        initial = _c2_worker_identity(
            config,
            "initial",
            sequence=int(config["initial_sequence"]),
        )
        with store.publication_order_guard():
            floor = ledger.audit_publication_sequence_floor()
            _emit_worker_event(
                "guard-acquired",
                authority=_c2_authority(store),
                floor=floor,
                pid=os.getpid(),
            )
            _read_worker_command("repair")
            publication = _c2_publish(
                store,
                identity=initial,
                publication_floor_sequence=floor,
                created_at=f"c2-{config['variant']}-a-repair",
            )
            _emit_worker_event(
                "repaired",
                published=publication.published,  # type: ignore[attr-defined]
                identity=publication.identity.to_json(),  # type: ignore[attr-defined]
                latest=store.latest_evidence(),
                authority=_c2_authority(store),
            )
        _emit_worker_event("guard-released", authority=_c2_authority(store))
        _read_worker_command("stale-check")
        stale_hashes = tuple(
            str(config[key]["hash"])  # type: ignore[index]
            for key in ("stale_confirmation", "stale_reactivation")
        )
        before = _c2_database_snapshot(ledger, block_hashes=stale_hashes)
        errors: dict[str, str] = {}
        stale_confirmation = config["stale_confirmation"]
        stale_reactivation = config["stale_reactivation"]
        assert isinstance(stale_confirmation, dict)
        assert isinstance(stale_reactivation, dict)
        operations = (
            (
                "confirmation",
                ledger.confirm_accepted_block,
                stale_confirmation,
            ),
            (
                "reactivation",
                ledger.reactivate_pool_block,
                stale_reactivation,
            ),
        )
        for label, operation, target in operations:
            try:
                operation(
                    block_hash=str(target["hash"]),
                    active_tip_height=int(target["height"]),
                )
            except Exception as error:
                message = str(error)
                if "writer lease is not active" not in message:
                    raise support.GateFailure(
                        f"stale A {label} wrong failure: {message}"
                    ) from error
                errors[label] = message
            else:
                raise support.GateFailure(
                    f"stale A {label} unexpectedly succeeded"
                )
        after = _c2_database_snapshot(ledger, block_hashes=stale_hashes)
        _emit_worker_event(
            "stale-result",
            errors=errors,
            before=before,
            after=after,
            authority=_c2_authority(store),
        )
        _read_worker_command("finish")
        store.close()
        store = None
        ledger.close()
        ledger = None
        _emit_worker_event("closed")
    finally:
        if store is not None:
            store.close()
        if ledger is not None:
            ledger.close()


def _run_c2_b_worker(config: dict[str, object]) -> None:
    ledger: _WorkerScopedPsqlLedger | None = None
    store: AuditArtifactStore | None = None
    original_flock = audit_artifacts_module.fcntl.flock
    try:
        ledger = _c2_worker_ledger(config)
        store = _c2_worker_store(config)
        writer = config["writer"]
        assert isinstance(writer, dict)
        ledger._run_sql(
            f"""
INSERT INTO qbit_a1_c2_attempts (variant, transition, writer_id)
VALUES (
    '{config['variant']}',
    '{config['transition']}',
    '{writer['id']}'
);
"""
        )
        attempted = False

        def observed_flock(fd: int, operation: int) -> object:
            nonlocal attempted
            if (
                not attempted
                and fd == store._publication_lock_fd  # type: ignore[union-attr]
                and operation & fcntl.LOCK_EX
            ):
                attempted = True
                _emit_worker_event(
                    "flock-attempt",
                    authority=_c2_authority(store),  # type: ignore[arg-type]
                    pid=os.getpid(),
                )
            return original_flock(fd, operation)

        audit_artifacts_module.fcntl.flock = observed_flock
        try:
            with store.publication_order_guard():
                if str(config["transition"]) == "confirmation":
                    target = config["target"]
                    assert isinstance(target, dict)
                    result = ledger.confirm_accepted_block(
                        block_hash=str(target["hash"]),
                        active_tip_height=int(target["height"]),
                    )
                    count = int(result["confirmed_count"])
                elif str(config["transition"]) == "reactivation":
                    target = config["target"]
                    assert isinstance(target, dict)
                    result = ledger.reactivate_pool_block(
                        block_hash=str(target["hash"]),
                        active_tip_height=int(target["height"]),
                    )
                    count = int(result["reactivated_count"])
                else:
                    raise support.GateFailure("invalid C2 B transition")
                support.assert_equal(count, 1, "C2 B transition count")
                sequence = int(result["audit_publication_sequence"])
                identity = _c2_worker_identity(
                    config,
                    "target",
                    sequence=sequence,
                )
                floor = ledger.audit_publication_sequence_floor()
                publication = _c2_publish(
                    store,
                    identity=identity,
                    publication_floor_sequence=floor,
                    created_at=f"c2-{config['variant']}-b-publication",
                )
                _emit_worker_event(
                    "transition-published",
                    transition_result=result,
                    floor=floor,
                    published=publication.published,  # type: ignore[attr-defined]
                    identity=identity.to_json(),
                    latest=store.latest_evidence(),
                    authority=_c2_authority(store),
                )
        finally:
            audit_artifacts_module.fcntl.flock = original_flock
        if not attempted:
            raise support.GateFailure("C2 B did not attempt the publication flock")
        _read_worker_command("finish")
        released = ledger.release_writer_lease()
        support.assert_equal(released, True, "C2 B writer lease release")
        store.close()
        store = None
        ledger.close()
        ledger = None
        _emit_worker_event("closed", lease_released=released)
    finally:
        audit_artifacts_module.fcntl.flock = original_flock
        if store is not None:
            store.close()
        if ledger is not None:
            try:
                ledger.release_writer_lease()
            except BaseException:
                pass
            ledger.close()


def _c3_stale_publication_attempt(
    *,
    ledger: _WorkerScopedPsqlLedger,
    store: AuditArtifactStore,
    config: dict[str, object],
) -> tuple[int, dict[str, object]]:
    floor = ledger.audit_publication_sequence_floor()
    identity = _c2_worker_identity(
        config,
        "initial",
        sequence=int(config["initial_sequence"]),
    )
    try:
        publication = _c2_publish(
            store,
            identity=identity,
            publication_floor_sequence=floor,
            created_at=f"c3-{config['variant']}-late-a",
        )
    except RuntimeError as error:
        message = str(error)
        if "behind" not in message:
            raise support.GateFailure(
                f"C3 stale A publication wrong failure: {message}"
            ) from error
        outcome = {"kind": "behind-error", "message": message}
    else:
        if publication.published:
            raise support.GateFailure("C3 stale A publication regressed evidence")
        outcome = {
            "kind": "returned",
            "published": publication.published,
            "identity": publication.identity.to_json(),
        }
    return floor, outcome


def _c3_stale_transition_check(
    ledger: _WorkerScopedPsqlLedger,
    config: dict[str, object],
) -> dict[str, object]:
    stale_hashes = tuple(
        str(config[key]["hash"])  # type: ignore[index]
        for key in ("stale_confirmation", "stale_reactivation")
    )
    before = _c2_database_snapshot(ledger, block_hashes=stale_hashes)
    errors: dict[str, str] = {}
    stale_confirmation = config["stale_confirmation"]
    stale_reactivation = config["stale_reactivation"]
    assert isinstance(stale_confirmation, dict)
    assert isinstance(stale_reactivation, dict)
    operations = (
        ("confirmation", ledger.confirm_accepted_block, stale_confirmation),
        ("reactivation", ledger.reactivate_pool_block, stale_reactivation),
    )
    for label, operation, target in operations:
        try:
            operation(
                block_hash=str(target["hash"]),
                active_tip_height=int(target["height"]),
            )
        except Exception as error:
            message = str(error)
            if "writer lease is not active" not in message:
                raise support.GateFailure(
                    f"C3 stale A {label} wrong failure: {message}"
                ) from error
            errors[label] = message
        else:
            raise support.GateFailure(f"C3 stale A {label} unexpectedly succeeded")
    after = _c2_database_snapshot(ledger, block_hashes=stale_hashes)
    return {"errors": errors, "before": before, "after": after}


def _run_c3_primary_a_worker(config: dict[str, object]) -> None:
    ledger: _WorkerScopedPsqlLedger | None = None
    store: AuditArtifactStore | None = None
    try:
        ledger = _c2_worker_ledger(config)
        store = _c2_worker_store(config)
        _emit_worker_event(
            "parked",
            authority=_c2_authority(store),
            pid=os.getpid(),
            guard="outside",
        )
        _read_worker_command("attempt")
        with store.publication_order_guard():
            floor, outcome = _c3_stale_publication_attempt(
                ledger=ledger,
                store=store,
                config=config,
            )
            _emit_worker_event(
                "stale-publication",
                authority=_c2_authority(store),
                floor=floor,
                outcome=outcome,
                latest=store.latest_evidence(),
            )
        transition_check = _c3_stale_transition_check(ledger, config)
        retention: list[dict[str, int]] = []
        for live_retention in (0, 1):
            store.reconfigure(live_bundle_retention=live_retention)
            result = store.prune_best_effort()
            retention.append(
                {
                    "retention": live_retention,
                    "live_removed": result.live_removed,
                    "candidate_removed": result.candidate_removed,
                    "errors": result.errors,
                }
            )
        _emit_worker_event(
            "post-check",
            authority=_c2_authority(store),
            transition_check=transition_check,
            retention=retention,
            latest=store.latest_evidence(),
        )
        _read_worker_command("finish")
        store.close()
        store = None
        ledger.close()
        ledger = None
        _emit_worker_event("closed")
    finally:
        if store is not None:
            store.close()
        if ledger is not None:
            ledger.close()


def _run_c3_late_a_worker(config: dict[str, object]) -> None:
    ledger: _WorkerScopedPsqlLedger | None = None
    store: AuditArtifactStore | None = None
    try:
        ledger = _c2_worker_ledger(config)
        store = _c2_worker_store(config)
        initial = config["initial"]
        assert isinstance(initial, dict)
        with store.publication_order_guard():
            result = ledger.confirm_accepted_block(
                block_hash=str(initial["hash"]),
                active_tip_height=int(initial["height"]),
            )
            sequence = int(result["audit_publication_sequence"])
            support.assert_equal(sequence, 1, "C3 late A confirmation ordinal N")
            floor = ledger.audit_publication_sequence_floor()
            support.assert_equal(floor, sequence, "C3 late A confirmed floor N")
            _emit_worker_event(
                "confirmed",
                authority=_c2_authority(store),
                pid=os.getpid(),
                identity=_c2_worker_identity(
                    config,
                    "initial",
                    sequence=sequence,
                ).to_json(),
                floor=floor,
            )
        _emit_worker_event(
            "parked",
            authority=_c2_authority(store),
            pid=os.getpid(),
            guard="outside",
        )
        _read_worker_command("attempt")
        with store.publication_order_guard():
            floor, outcome = _c3_stale_publication_attempt(
                ledger=ledger,
                store=store,
                config=config,
            )
            _emit_worker_event(
                "stale-publication",
                authority=_c2_authority(store),
                floor=floor,
                outcome=outcome,
                latest=store.latest_evidence(),
            )
        _read_worker_command("finish")
        store.close()
        store = None
        ledger.close()
        ledger = None
        _emit_worker_event("closed")
    finally:
        if store is not None:
            store.close()
        if ledger is not None:
            ledger.close()


def _run_c3_b_worker(config: dict[str, object]) -> None:
    ledger: _WorkerScopedPsqlLedger | None = None
    store: AuditArtifactStore | None = None
    original_flock = audit_artifacts_module.fcntl.flock
    try:
        ledger = _c2_worker_ledger(config)
        store = _c2_worker_store(config)
        writer = config["writer"]
        assert isinstance(writer, dict)
        ledger._run_sql(
            f"""
INSERT INTO qbit_a1_c3_attempts (variant, writer_id)
VALUES ('{config['variant']}', '{writer['id']}');
"""
        )
        attempted = False

        def observed_flock(fd: int, operation: int) -> object:
            nonlocal attempted
            if (
                not attempted
                and fd == store._publication_lock_fd  # type: ignore[union-attr]
                and operation & fcntl.LOCK_EX
            ):
                attempted = True
                _emit_worker_event(
                    "flock-attempt",
                    authority=_c2_authority(store),  # type: ignore[arg-type]
                    pid=os.getpid(),
                )
            return original_flock(fd, operation)

        audit_artifacts_module.fcntl.flock = observed_flock
        try:
            with store.publication_order_guard():
                target = config["target"]
                assert isinstance(target, dict)
                result = ledger.confirm_accepted_block(
                    block_hash=str(target["hash"]),
                    active_tip_height=int(target["height"]),
                )
                support.assert_equal(
                    result["confirmed_count"],
                    1,
                    "C3 B confirmation count",
                )
                sequence = int(result["audit_publication_sequence"])
                identity = _c2_worker_identity(
                    config,
                    "target",
                    sequence=sequence,
                )
                floor = ledger.audit_publication_sequence_floor()
                publication = _c2_publish(
                    store,
                    identity=identity,
                    publication_floor_sequence=floor,
                    created_at=f"c3-{config['variant']}-b-publication",
                )
                _emit_worker_event(
                    "transition-published",
                    authority=_c2_authority(store),
                    transition_result=result,
                    floor=floor,
                    identity=identity.to_json(),
                    published=publication.published,
                    latest=store.latest_evidence(),
                )
        finally:
            audit_artifacts_module.fcntl.flock = original_flock
        if not attempted:
            raise support.GateFailure("C3 B did not attempt the publication flock")
        _read_worker_command("finish")
        released = ledger.release_writer_lease()
        support.assert_equal(released, True, "C3 B writer lease release")
        store.close()
        store = None
        ledger.close()
        ledger = None
        _emit_worker_event("closed", lease_released=released)
    finally:
        audit_artifacts_module.fcntl.flock = original_flock
        if store is not None:
            store.close()
        if ledger is not None:
            try:
                ledger.release_writer_lease()
            except BaseException:
                pass
            ledger.close()


def _run_c2_worker(config_path: Path) -> int:
    try:
        raw = config_path.read_bytes()
        if len(raw) > support.PSQL_OUTPUT_LIMIT_BYTES:
            raise support.GateFailure("process worker configuration exceeded 1 MiB")
        config = json.loads(raw)
        if not isinstance(config, dict):
            raise support.GateFailure("process worker configuration is not an object")
        role = config.get("role")
        if role == "a":
            _run_c2_a_worker(config)
        elif role == "b":
            _run_c2_b_worker(config)
        elif role == "c3-primary-a":
            _run_c3_primary_a_worker(config)
        elif role == "c3-late-a":
            _run_c3_late_a_worker(config)
        elif role == "c3-b":
            _run_c3_b_worker(config)
        else:
            raise support.GateFailure(f"invalid C2 worker role: {role!r}")
    except BaseException as error:
        _emit_worker_event(
            "error",
            error_type=type(error).__name__,
            message=str(error),
        )
        traceback.print_exc(file=sys.stderr)
        return 1
    return 0


def _start_file_worker(
    sql: str,
) -> tuple[subprocess.Popen[str], Any, Any, Any]:
    input_file = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
    stdout_file = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
    stderr_file = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
    input_file.write(sql)
    input_file.seek(0)
    process = subprocess.Popen(
        migration._psql_process_command(),
        stdin=input_file,
        stdout=stdout_file,
        stderr=stderr_file,
        text=True,
        start_new_session=True,
    )
    migration._register_process(process)
    return process, input_file, stdout_file, stderr_file


def _close_file_worker(
    worker: tuple[subprocess.Popen[str], Any, Any, Any] | None,
) -> None:
    if worker is None:
        return
    process, input_file, stdout_file, stderr_file = worker
    if process.poll() is None:
        support._terminate_and_reap(process)
    migration._forget_process(process)
    for stream in (
        process.stdin,
        process.stdout,
        process.stderr,
        input_file,
        stdout_file,
        stderr_file,
    ):
        if stream is not None and not stream.closed:
            stream.close()


def _wait_success(
    worker: tuple[subprocess.Popen[str], Any, Any, Any],
) -> None:
    process, _input_file, stdout_file, stderr_file = worker
    migration._wait_file_process(
        process,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        expected_success=True,
        error_fragment=None,
    )


def _wait_holder_success(
    holder: subprocess.Popen[str],
    *,
    stderr_file: Any,
    label: str,
) -> None:
    try:
        holder.wait(timeout=support.PSQL_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as error:
        support._terminate_and_reap(holder)
        raise support.GateFailure(f"{label} timed out") from error
    finally:
        migration._forget_process(holder)
    stdout = ""
    if holder.stdout is not None:
        stdout = holder.stdout.read(support.PSQL_OUTPUT_LIMIT_BYTES + 1)
    stderr_file.seek(0)
    stderr = stderr_file.read(support.PSQL_OUTPUT_LIMIT_BYTES + 1)
    if (
        len(stdout.encode("utf-8")) > support.PSQL_OUTPUT_LIMIT_BYTES
        or len(stderr.encode("utf-8")) > support.PSQL_OUTPUT_LIMIT_BYTES
    ):
        raise support.GateFailure(f"{label} output exceeded 1 MiB")
    if holder.returncode != 0:
        diagnostics = stderr.strip() or stdout.strip() or "no diagnostic output"
        raise support.GateFailure(
            f"{label} failed with {holder.returncode}: {diagnostics}"
        )


def _prepared_rows_sql(block_hashes: tuple[str, ...]) -> str:
    values = ",\n".join(
        (
            f"('{block_hash}', {height}, '{'10' * 32}', '{'20' * 32}', "
            f"'{'30' * 32}', 'prepared', 'immature')"
        )
        for height, block_hash in enumerate(block_hashes, start=10)
    )
    return f"""
INSERT INTO qbit_pool_blocks (
    block_hash, block_height, parent_hash, coinbase_txid,
    payout_manifest_sha256, chain_state, maturity_state
) VALUES
{values};
"""


def _confirmation_worker_sql(
    *,
    schema: str,
    application_name: str,
    round_name: str,
    block_hash: str,
    height: int,
    writer_id: str,
    writer_epoch: int,
    writer_token: str,
    gate_a: int,
    gate_b: int,
) -> str:
    return f"""
SET application_name = '{application_name}';
SET statement_timeout = '20s';
SET lock_timeout = '20s';
SET search_path TO "{schema}", pg_catalog;
BEGIN;
SELECT pg_catalog.pg_advisory_xact_lock_shared({gate_a}, {gate_b});
DO $qbit_a1_confirm$
DECLARE
    confirmed_count integer;
BEGIN
    SELECT "{schema}".qbit_confirm_pool_block(
        '{block_hash}', {height}, '{writer_id}', {writer_epoch},
        '{writer_token}', interval '5 minutes'
    )
    INTO confirmed_count;
    IF confirmed_count <> 1 THEN
        RAISE EXCEPTION 'expected one confirmed pool block, got %', confirmed_count;
    END IF;
END;
$qbit_a1_confirm$;
INSERT INTO "{schema}".qbit_a1_confirmation_results (
    round_name,
    block_hash,
    audit_publication_sequence
)
SELECT
    '{round_name}',
    block_hash,
    audit_publication_sequence
FROM "{schema}".qbit_pool_blocks
WHERE block_hash = '{block_hash}'
  AND chain_state = 'confirmed'
  AND audit_publication_sequence IS NOT NULL;
COMMIT;
"""


def _assert_no_tagged_backends(tags: tuple[str, ...], message: str) -> None:
    quoted = ", ".join(f"'{tag}'" for tag in tags)
    count = int(
        support.run_json(
            f"""
SELECT json_build_object('count', count(*))
FROM pg_catalog.pg_stat_activity
WHERE application_name IN ({quoted});
"""
        )["count"]
    )
    support.assert_equal(count, 0, message)


def test_database_observed_confirmation_order() -> None:
    for round_index, reverse_launch in enumerate((False, True), start=1):
        round_name = f"c1-round-{round_index}"
        schema = support.create_owned_schema(f"c1_round_{round_index}")
        writer_id = f"a1-c1-writer-{round_index}"
        writer_token = f"a1-c1-token-{round_index}"
        ledger = support.ScopedPsqlLedger(
            test_schema=schema,
            writer_id=writer_id,
            writer_epoch=1,
            writer_session_token=writer_token,
            initialize_schema=True,
        )
        block_hashes = ("91" * 32, "92" * 32)
        heights = {block_hashes[0]: 10, block_hashes[1]: 11}
        gate_a = 41_000 + round_index
        gate_b = 51_000 + round_index
        holder_tag = f"qbit_a1_c1_holder_{support.RUN_TOKEN}_{round_index}"
        worker_tags = (
            f"qbit_a1_c1_worker_a_{support.RUN_TOKEN}_{round_index}",
            f"qbit_a1_c1_worker_b_{support.RUN_TOKEN}_{round_index}",
        )
        holder_input = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        holder_stderr = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        holder: subprocess.Popen[str] | None = None
        workers: list[tuple[subprocess.Popen[str], Any, Any, Any]] = []
        try:
            ledger._run_sql(
                """
CREATE TABLE qbit_a1_confirmation_results (
    completion_id bigserial PRIMARY KEY,
    round_name text NOT NULL,
    block_hash text NOT NULL UNIQUE,
    audit_publication_sequence bigint NOT NULL
);
"""
                + _prepared_rows_sql(block_hashes)
            )
            holder_input.write(
                f"""
SET application_name = '{holder_tag}';
BEGIN;
SELECT pg_catalog.pg_advisory_xact_lock({gate_a}, {gate_b});
DO $qbit_a1_wait$
DECLARE
    both_waiting boolean := false;
BEGIN
    FOR attempt IN 1..600 LOOP
        PERFORM pg_catalog.pg_stat_clear_snapshot();
        SELECT count(DISTINCT worker.pid) = 2
        INTO both_waiting
        FROM pg_catalog.pg_stat_activity worker
        WHERE worker.application_name IN (
            '{worker_tags[0]}',
            '{worker_tags[1]}'
        )
          AND pg_catalog.pg_backend_pid() = ANY(
              pg_catalog.pg_blocking_pids(worker.pid)
          )
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.pg_locks lock_state
              WHERE lock_state.pid = worker.pid
                AND lock_state.locktype = 'advisory'
                AND NOT lock_state.granted
          );
        EXIT WHEN both_waiting;
        PERFORM pg_catalog.pg_sleep(0.05);
    END LOOP;
    IF NOT both_waiting THEN
        RAISE EXCEPTION 'two confirmation advisory waiters were not observed';
    END IF;
END;
$qbit_a1_wait$;
COMMIT;
"""
            )
            holder_input.seek(0)
            holder = subprocess.Popen(
                migration._psql_process_command(),
                stdin=holder_input,
                stdout=subprocess.DEVNULL,
                stderr=holder_stderr,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            migration._register_process(holder)
            migration._wait_for_advisory_holder(
                holder,
                application_name=holder_tag,
                deadline=time.monotonic() + support.PSQL_TIMEOUT_SECONDS,
            )
            launch = [0, 1]
            if reverse_launch:
                launch.reverse()
            for worker_index in launch:
                block_hash = block_hashes[worker_index]
                workers.append(
                    _start_file_worker(
                        _confirmation_worker_sql(
                            schema=schema,
                            application_name=worker_tags[worker_index],
                            round_name=round_name,
                            block_hash=block_hash,
                            height=heights[block_hash],
                            writer_id=writer_id,
                            writer_epoch=1,
                            writer_token=writer_token,
                            gate_a=gate_a,
                            gate_b=gate_b,
                        )
                    )
                )
            _wait_holder_success(
                holder,
                stderr_file=holder_stderr,
                label="C1 holder",
            )
            for worker in workers:
                _wait_success(worker)
            results = ledger._run_json(
                f"""
SELECT json_build_object(
    'rows', COALESCE(json_agg(json_build_object(
        'completion_id', completion_id,
        'block_hash', block_hash,
        'audit_publication_sequence', audit_publication_sequence
    ) ORDER BY completion_id), '[]'::json)
)
FROM qbit_a1_confirmation_results
WHERE round_name = '{round_name}';
"""
            )["rows"]
            support.assert_equal(len(results), 2, f"{round_name} completion count")
            support.assert_equal(
                [row["completion_id"] for row in results],
                [1, 2],
                f"{round_name} exact completion identifiers",
            )
            support.assert_equal(
                [row["audit_publication_sequence"] for row in results],
                [1, 2],
                f"{round_name} completion order matches ordinal order",
            )
            support.assert_equal(
                {row["block_hash"] for row in results},
                set(block_hashes),
                f"{round_name} distinct completed blocks",
            )
            before_replay = support.allocator_state(ledger)
            for _replay_round in range(3):
                for block_hash in block_hashes:
                    state = ledger.pool_block_state(block_hash=block_hash)
                    replay = ledger.confirm_accepted_block(
                        block_hash=block_hash,
                        active_tip_height=heights[block_hash],
                    )
                    support.assert_equal(
                        replay["audit_publication_sequence"],
                        state["audit_publication_sequence"],  # type: ignore[index]
                        f"{round_name} exact replay ordinal",
                    )
            support.assert_equal(
                support.allocator_state(ledger),
                before_replay,
                f"{round_name} replay allocator immobility",
            )
            fresh_hash = "93" * 32
            ledger._run_sql(_prepared_rows_sql((fresh_hash,)))
            support.assert_equal(
                ledger.confirm_accepted_block(
                    block_hash=fresh_hash,
                    active_tip_height=10,
                )["audit_publication_sequence"],
                3,
                f"{round_name} exact next ordinal",
            )
            _assert_no_tagged_backends(
                (holder_tag, *worker_tags),
                f"{round_name} tagged backend cleanup",
            )
        finally:
            for worker in workers:
                _close_file_worker(worker)
            if holder is not None:
                if holder.poll() is None:
                    support._terminate_and_reap(holder)
                migration._forget_process(holder)
                for stream in (holder.stdin, holder.stdout, holder.stderr):
                    if stream is not None and not stream.closed:
                        stream.close()
            if not holder_stderr.closed:
                holder_stderr.close()
            if not holder_input.closed:
                holder_input.close()
            ledger.release_writer_lease()
            ledger.close()


def _c2_seed_rows(
    ledger: support.ScopedPsqlLedger,
    *,
    initial: dict[str, object],
    target: dict[str, object],
    stale_confirmation: dict[str, object],
    stale_reactivation: dict[str, object],
) -> None:
    rows = (
        (initial, "prepared"),
        (target, "prepared"),
        (stale_confirmation, "prepared"),
        (stale_reactivation, "inactive"),
    )
    values = ",\n".join(
        (
            f"('{row['hash']}', {int(row['height'])}, '{'10' * 32}', "
            f"'{'20' * 32}', '{'30' * 32}', '{chain_state}', 'immature')"
        )
        for row, chain_state in rows
    )
    ledger._run_sql(
        f"""
CREATE TABLE qbit_a1_c2_attempts (
    attempt_id bigserial PRIMARY KEY,
    variant text NOT NULL,
    transition text NOT NULL,
    writer_id text NOT NULL,
    attempted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO qbit_pool_blocks (
    block_hash,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    chain_state,
    maturity_state
) VALUES
{values};
"""
    )


def _c2_attempt_and_lease(
    ledger: support.ScopedPsqlLedger,
    *,
    variant: str,
) -> dict[str, object]:
    return ledger._run_json(
        f"""
SELECT json_build_object(
    'attempts', COALESCE((
        SELECT json_agg(json_build_object(
            'variant', variant,
            'transition', transition,
            'writer_id', writer_id
        ) ORDER BY attempt_id)
        FROM qbit_a1_c2_attempts
        WHERE variant = '{variant}'
    ), '[]'::json),
    'lease', (
        SELECT json_build_object(
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token
        )
        FROM qbit_ledger_writer_lease
        WHERE singleton
    )
);
"""
    )


def _c2_expire_writer(
    ledger: support.ScopedPsqlLedger,
    *,
    writer_id: str,
    writer_epoch: int,
    writer_token: str,
) -> None:
    expired = ledger._run_json(
        f"""
WITH expired AS (
    UPDATE qbit_ledger_writer_lease
    SET updated_at = clock_timestamp() - interval '6 minutes',
        lease_expires_at = clock_timestamp() - interval '1 minute'
    WHERE singleton
      AND writer_id = '{writer_id}'
      AND writer_epoch = {writer_epoch}
      AND writer_session_token = '{writer_token}'
    RETURNING writer_id
)
SELECT json_build_object('count', count(*))
FROM expired;
"""
    )["count"]
    support.assert_equal(expired, 1, "C2 exact A lease expiry")


def _c2_write_config(path: Path, config: dict[str, object]) -> None:
    path.write_text(
        json.dumps(config, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )


def _c2_expected_authority(root: Path) -> dict[str, list[int]]:
    root_value = root.lstat()
    lock_value = (root / ".prism-audit-publication.lock").lstat()
    return {
        "root": [root_value.st_dev, root_value.st_ino],
        "lock": [lock_value.st_dev, lock_value.st_ino],
    }


def _assert_c2_final_database(
    snapshot: dict[str, object],
    *,
    initial: dict[str, object],
    target: dict[str, object],
    stale_confirmation: dict[str, object],
    stale_reactivation: dict[str, object],
    initial_sequence: int,
    target_sequence: int,
    variant: str,
) -> None:
    rows = snapshot["rows"]
    assert isinstance(rows, list)
    by_hash = {str(row["block_hash"]): row for row in rows}
    support.assert_equal(
        by_hash[str(initial["hash"])],
        {
            "block_hash": initial["hash"],
            "block_height": initial["height"],
            "chain_state": "confirmed",
            "maturity_state": "immature",
            "audit_publication_sequence": initial_sequence,
        },
        f"C2 {variant} initial exact row",
    )
    support.assert_equal(
        by_hash[str(target["hash"])],
        {
            "block_hash": target["hash"],
            "block_height": target["height"],
            "chain_state": "confirmed",
            "maturity_state": "immature",
            "audit_publication_sequence": target_sequence,
        },
        f"C2 {variant} B exact row",
    )
    support.assert_equal(
        by_hash[str(stale_confirmation["hash"])],
        {
            "block_hash": stale_confirmation["hash"],
            "block_height": stale_confirmation["height"],
            "chain_state": "prepared",
            "maturity_state": "immature",
            "audit_publication_sequence": None,
        },
        f"C2 {variant} stale prepared C row",
    )
    support.assert_equal(
        by_hash[str(stale_reactivation["hash"])],
        {
            "block_hash": stale_reactivation["hash"],
            "block_height": stale_reactivation["height"],
            "chain_state": "inactive",
            "maturity_state": "immature",
            "audit_publication_sequence": None,
        },
        f"C2 {variant} stale inactive R row",
    )
    support.assert_equal(
        snapshot["floor"],
        target_sequence,
        f"C2 {variant} final floor",
    )
    support.assert_equal(
        snapshot["allocator"],
        {"last_value": target_sequence, "is_called": True},
        f"C2 {variant} exact allocator",
    )
    ordinals = sorted(
        int(row["audit_publication_sequence"])
        for row in rows
        if row["audit_publication_sequence"] is not None
    )
    support.assert_equal(
        len(ordinals),
        len(set(ordinals)),
        f"C2 {variant} unique durable ordinals",
    )


def _test_c2_a_wins(transition: str) -> None:
    if transition not in {"confirmation", "reactivation"}:
        raise support.GateFailure(f"invalid C2 transition: {transition!r}")
    variant_index = 1 if transition == "confirmation" else 2
    variant = transition
    schema = support.create_owned_schema(f"c2_{variant}")
    writer_a = {
        "id": f"a1-c2-{variant}-writer-a",
        "epoch": 1,
        "token": f"a1-c2-{variant}-token-a",
    }
    writer_b = {
        "id": f"a1-c2-{variant}-writer-b",
        "epoch": 2,
        "token": f"a1-c2-{variant}-token-b",
    }
    initial = {"hash": f"a{variant_index}" * 32, "height": 10}
    target = {"hash": f"b{variant_index}" * 32, "height": 20}
    stale_confirmation = {"hash": f"c{variant_index}" * 32, "height": 30}
    stale_reactivation = {"hash": f"d{variant_index}" * 32, "height": 31}
    all_hashes = tuple(
        str(value["hash"])
        for value in (
            initial,
            target,
            stale_confirmation,
            stale_reactivation,
        )
    )
    ledger = support.ScopedPsqlLedger(
        test_schema=schema,
        writer_id=str(writer_a["id"]),
        writer_epoch=int(writer_a["epoch"]),
        writer_session_token=str(writer_a["token"]),
        initialize_schema=True,
    )
    worker_a: _JsonWorker | None = None
    worker_b: _JsonWorker | None = None
    setup_store: AuditArtifactStore | None = None
    try:
        _c2_seed_rows(
            ledger,
            initial=initial,
            target=target,
            stale_confirmation=stale_confirmation,
            stale_reactivation=stale_reactivation,
        )
        initial_sequence = 1
        if transition == "reactivation":
            target_prior = ledger.confirm_accepted_block(
                block_hash=str(target["hash"]),
                active_tip_height=int(target["height"]),
            )
            support.assert_equal(
                target_prior["audit_publication_sequence"],
                1,
                "C2 reactivation target prior confirmation ordinal",
            )
            support.assert_equal(
                ledger.mark_pool_block_inactive(
                    block_hash=str(target["hash"]),
                    active_tip_height=int(target["height"]),
                )["inactive_count"],
                1,
                "C2 reactivation target prior inactive transition",
            )
            initial_sequence = 2
        initial_result = ledger.confirm_accepted_block(
            block_hash=str(initial["hash"]),
            active_tip_height=int(initial["height"]),
        )
        support.assert_equal(
            initial_result["audit_publication_sequence"],
            initial_sequence,
            f"C2 {variant} initial ordinal",
        )
        support.assert_equal(
            ledger.audit_publication_sequence_floor(),
            initial_sequence,
            f"C2 {variant} initial floor",
        )
        support.assert_equal(
            support.allocator_state(ledger),
            {"last_value": initial_sequence, "is_called": True},
            f"C2 {variant} initial allocator",
        )
        target_sequence = initial_sequence + 1
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as temp_cleanup:
            base = Path(tmp)
            root = base / "audit"
            evidence_path = base / "state" / "evidence.json"
            setup_store = _c2_store(root, evidence_path)
            temp_cleanup.callback(setup_store.close)
            initial_identity = AuditPublicationIdentity(
                initial_sequence,
                int(initial["height"]),
                str(initial["hash"]),
            )
            with setup_store.publication_order_guard():
                initial_publication = _c2_publish(
                    setup_store,
                    identity=initial_identity,
                    publication_floor_sequence=initial_sequence,
                    created_at=f"c2-{variant}-initial",
                )
            support.assert_equal(
                initial_publication.published,  # type: ignore[attr-defined]
                True,
                f"C2 {variant} initial publication",
            )
            initial_envelope = setup_store.live_envelope_path(
                block_height=int(initial["height"]),
                block_hash=str(initial["hash"]),
            )
            setup_store.close()
            setup_store = None
            evidence_path.write_bytes(b"{c2-damaged-evidence")
            expected_authority = _c2_expected_authority(root)
            common_config: dict[str, object] = {
                "schema": schema,
                "root": str(root),
                "evidence_path": str(evidence_path),
                "variant": variant,
                "transition": transition,
                "initial_sequence": initial_sequence,
                "initial": initial,
                "target": target,
                "stale_confirmation": stale_confirmation,
                "stale_reactivation": stale_reactivation,
            }
            config_a = base / "worker-a.json"
            config_b = base / "worker-b.json"
            tag_suffix = f"{variant[0]}_{support.RUN_TOKEN[:8]}"
            tag_a = f"qbit_a1_c2_a_{tag_suffix}"
            tag_b = f"qbit_a1_c2_b_{tag_suffix}"
            _c2_write_config(
                config_a,
                {
                    **common_config,
                    "role": "a",
                    "writer": writer_a,
                    "application_name": tag_a,
                },
            )
            _c2_write_config(
                config_b,
                {
                    **common_config,
                    "role": "b",
                    "writer": writer_b,
                    "application_name": tag_b,
                },
            )
            worker_a = _JsonWorker(config_a)
            temp_cleanup.callback(worker_a.close)
            a_guard = worker_a.read_event("guard-acquired")
            support.assert_equal(
                a_guard["authority"],
                expected_authority,
                f"C2 {variant} A exact root/lock inode",
            )
            support.assert_equal(
                a_guard["floor"],
                initial_sequence,
                f"C2 {variant} A floor N",
            )
            blocked_filesystem = _c2_filesystem_snapshot(
                root=root,
                evidence_path=evidence_path,
            )
            blocked_database = _c2_database_snapshot(
                ledger,
                block_hashes=all_hashes,
            )
            _c2_expire_writer(
                ledger,
                writer_id=str(writer_a["id"]),
                writer_epoch=int(writer_a["epoch"]),
                writer_token=str(writer_a["token"]),
            )
            worker_b = _JsonWorker(config_b)
            temp_cleanup.callback(worker_b.close)
            b_attempt = worker_b.read_event("flock-attempt")
            support.assert_equal(
                b_attempt["authority"],
                expected_authority,
                f"C2 {variant} B exact root/lock inode",
            )
            support.assert_equal(
                worker_b.process.poll(),
                None,
                f"C2 {variant} B remains alive while flock-blocked",
            )
            child_pids = {int(a_guard["pid"]), int(b_attempt["pid"])}
            support.assert_equal(
                len(child_pids),
                2,
                f"C2 {variant} distinct A/B OS processes",
            )
            if os.getpid() in child_pids:
                raise support.GateFailure(
                    f"C2 {variant} worker reused the parent process"
                )
            support.assert_equal(
                _c2_attempt_and_lease(ledger, variant=variant),
                {
                    "attempts": [
                        {
                            "variant": variant,
                            "transition": transition,
                            "writer_id": writer_b["id"],
                        }
                    ],
                    "lease": {
                        "writer_id": writer_b["id"],
                        "writer_epoch": writer_b["epoch"],
                        "writer_session_token": writer_b["token"],
                    },
                },
                f"C2 {variant} DB-visible B attempt and replacement lease",
            )
            support.assert_equal(
                _c2_database_snapshot(ledger, block_hashes=all_hashes),
                blocked_database,
                f"C2 {variant} B row/floor/allocator unchanged while blocked",
            )
            support.assert_equal(
                _c2_filesystem_snapshot(root=root, evidence_path=evidence_path),
                blocked_filesystem,
                f"C2 {variant} filesystem unchanged while B blocked",
            )
            worker_a.send("repair")
            repaired = worker_a.read_event("repaired")
            support.assert_equal(
                repaired["published"],
                True,
                f"C2 {variant} A exact repair published",
            )
            support.assert_equal(
                repaired["identity"],
                initial_identity.to_json(),
                f"C2 {variant} A repair identity N",
            )
            support.assert_equal(
                repaired["authority"],
                expected_authority,
                f"C2 {variant} A repair authority",
            )
            released = worker_a.read_event("guard-released")
            support.assert_equal(
                released["authority"],
                expected_authority,
                f"C2 {variant} A guard release authority",
            )
            b_done = worker_b.read_event("transition-published")
            expected_b_identity = AuditPublicationIdentity(
                target_sequence,
                int(target["height"]),
                str(target["hash"]),
            )
            support.assert_equal(
                b_done["identity"],
                expected_b_identity.to_json(),
                f"C2 {variant} B identity N+1",
            )
            support.assert_equal(
                b_done["floor"],
                target_sequence,
                f"C2 {variant} B fresh floor",
            )
            support.assert_equal(
                b_done["published"],
                True,
                f"C2 {variant} B publication",
            )
            support.assert_equal(
                b_done["authority"],
                expected_authority,
                f"C2 {variant} B publication authority",
            )
            final_database = _c2_database_snapshot(
                ledger,
                block_hashes=all_hashes,
            )
            _assert_c2_final_database(
                final_database,
                initial=initial,
                target=target,
                stale_confirmation=stale_confirmation,
                stale_reactivation=stale_reactivation,
                initial_sequence=initial_sequence,
                target_sequence=target_sequence,
                variant=variant,
            )
            latest = json.loads(evidence_path.read_text(encoding="utf-8"))
            support.assert_equal(
                latest["audit_publication_identity"],
                expected_b_identity.to_json(),
                f"C2 {variant} final evidence B/N+1",
            )
            support.assert_equal(
                latest["block_hash"],
                target["hash"],
                f"C2 {variant} final evidence B hash",
            )
            target_envelope = root / (
                f"prism-live-audit-bundle-{target['height']}-{target['hash']}.json"
            )
            support.assert_equal(
                initial_envelope.exists(),
                False,
                f"C2 {variant} retention removes unpinned A envelope",
            )
            support.assert_equal(
                target_envelope.exists(),
                True,
                f"C2 {variant} retention keeps B envelope",
            )
            support.assert_equal(
                sorted(path.name for path in root.iterdir()),
                [
                    ".prism-audit-publication.lock",
                    target_envelope.name,
                ],
                f"C2 {variant} exact retained root entries",
            )
            before_stale_database = _c2_database_snapshot(
                ledger,
                block_hashes=all_hashes,
            )
            before_stale_filesystem = _c2_filesystem_snapshot(
                root=root,
                evidence_path=evidence_path,
            )
            worker_a.send("stale-check")
            stale_result = worker_a.read_event("stale-result")
            stale_errors = stale_result["errors"]
            assert isinstance(stale_errors, dict)
            support.assert_equal(
                set(stale_errors),
                {"confirmation", "reactivation"},
                f"C2 {variant} exact stale A failure labels",
            )
            for label in ("confirmation", "reactivation"):
                if "writer lease is not active" not in str(stale_errors[label]):
                    raise support.GateFailure(
                        f"C2 {variant} stale A {label} wrong error: "
                        f"{stale_errors[label]!r}"
                    )
            support.assert_equal(
                stale_result["before"],
                stale_result["after"],
                f"C2 {variant} child-observed stale transition immobility",
            )
            support.assert_equal(
                stale_result["authority"],
                expected_authority,
                f"C2 {variant} stale A authority",
            )
            support.assert_equal(
                _c2_database_snapshot(ledger, block_hashes=all_hashes),
                before_stale_database,
                f"C2 {variant} stale A row/floor/allocator immobility",
            )
            support.assert_equal(
                _c2_filesystem_snapshot(root=root, evidence_path=evidence_path),
                before_stale_filesystem,
                f"C2 {variant} stale A filesystem immobility",
            )
            worker_a.send("finish")
            worker_a.read_event("closed")
            worker_a.wait_success()
            worker_b.send("finish")
            b_closed = worker_b.read_event("closed")
            support.assert_equal(
                b_closed["lease_released"],
                True,
                f"C2 {variant} B lease released",
            )
            worker_b.wait_success()
            _assert_no_tagged_backends(
                (tag_a, tag_b),
                f"C2 {variant} tagged child backend cleanup",
            )
    finally:
        if setup_store is not None:
            setup_store.close()
        if worker_a is not None:
            worker_a.close()
        if worker_b is not None:
            worker_b.close()
        ledger.close()


def test_c2_a_wins_confirmation_publication() -> None:
    _test_c2_a_wins("confirmation")


def _c3_seed_rows(
    ledger: support.ScopedPsqlLedger,
    *,
    initial: dict[str, object],
    target: dict[str, object],
    stale_confirmation: dict[str, object],
    stale_reactivation: dict[str, object],
) -> None:
    rows = (
        (initial, "prepared"),
        (target, "prepared"),
        (stale_confirmation, "prepared"),
        (stale_reactivation, "inactive"),
    )
    values = ",\n".join(
        (
            f"('{row['hash']}', {int(row['height'])}, '{'10' * 32}', "
            f"'{'20' * 32}', '{'30' * 32}', '{chain_state}', 'immature')"
        )
        for row, chain_state in rows
    )
    ledger._run_sql(
        f"""
CREATE TABLE qbit_a1_c3_attempts (
    attempt_id bigserial PRIMARY KEY,
    variant text NOT NULL,
    writer_id text NOT NULL,
    attempted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO qbit_pool_blocks (
    block_hash,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    chain_state,
    maturity_state
) VALUES
{values};
"""
    )


def _c3_attempt_and_lease(
    ledger: support.ScopedPsqlLedger,
    *,
    variant: str,
) -> dict[str, object]:
    return ledger._run_json(
        f"""
SELECT json_build_object(
    'attempts', COALESCE((
        SELECT json_agg(json_build_object(
            'variant', variant,
            'writer_id', writer_id
        ) ORDER BY attempt_id)
        FROM qbit_a1_c3_attempts
        WHERE variant = '{variant}'
    ), '[]'::json),
    'lease', (
        SELECT json_build_object(
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token
        )
        FROM qbit_ledger_writer_lease
        WHERE singleton
    )
);
"""
    )


def _c3_expected_database(
    *,
    initial: dict[str, object],
    target: dict[str, object],
    stale_confirmation: dict[str, object],
    stale_reactivation: dict[str, object],
) -> dict[str, object]:
    return {
        "rows": [
            {
                "block_hash": initial["hash"],
                "block_height": initial["height"],
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": 1,
            },
            {
                "block_hash": target["hash"],
                "block_height": target["height"],
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": 2,
            },
            {
                "block_hash": stale_confirmation["hash"],
                "block_height": stale_confirmation["height"],
                "chain_state": "prepared",
                "maturity_state": "immature",
                "audit_publication_sequence": None,
            },
            {
                "block_hash": stale_reactivation["hash"],
                "block_height": stale_reactivation["height"],
                "chain_state": "inactive",
                "maturity_state": "immature",
                "audit_publication_sequence": None,
            },
        ],
        "floor": 2,
        "allocator": {"last_value": 2, "is_called": True},
    }


def _assert_c3_stale_outcome(outcome: object, *, label: str) -> None:
    if not isinstance(outcome, dict):
        raise support.GateFailure(f"{label} outcome is not an object")
    if outcome.get("kind") == "returned":
        support.assert_equal(
            outcome.get("published"),
            False,
            f"{label} returns published false",
        )
        return
    if outcome.get("kind") == "behind-error" and "behind" in str(
        outcome.get("message")
    ):
        return
    raise support.GateFailure(f"{label} unexpected outcome: {outcome!r}")


def _test_c3_interleaving(variant: str) -> None:
    if variant not in {"primary", "late"}:
        raise support.GateFailure(f"invalid C3 variant: {variant!r}")
    variant_index = 3 if variant == "primary" else 4
    schema = support.create_owned_schema(f"c3_{variant}")
    writer_a = {
        "id": f"a1-c3-{variant}-writer-a",
        "epoch": 1,
        "token": f"a1-c3-{variant}-token-a",
    }
    writer_b = {
        "id": f"a1-c3-{variant}-writer-b",
        "epoch": 2,
        "token": f"a1-c3-{variant}-token-b",
    }
    initial = {"hash": f"a{variant_index}" * 32, "height": 10}
    target = {"hash": f"b{variant_index}" * 32, "height": 20}
    stale_confirmation = {"hash": f"c{variant_index}" * 32, "height": 30}
    stale_reactivation = {"hash": f"d{variant_index}" * 32, "height": 31}
    all_hashes = tuple(
        str(value["hash"])
        for value in (
            initial,
            target,
            stale_confirmation,
            stale_reactivation,
        )
    )
    ledger = support.ScopedPsqlLedger(
        test_schema=schema,
        writer_id=str(writer_a["id"]),
        writer_epoch=int(writer_a["epoch"]),
        writer_session_token=str(writer_a["token"]),
        initialize_schema=True,
    )
    worker_a: _JsonWorker | None = None
    worker_b: _JsonWorker | None = None
    setup_store: AuditArtifactStore | None = None
    try:
        _c3_seed_rows(
            ledger,
            initial=initial,
            target=target,
            stale_confirmation=stale_confirmation,
            stale_reactivation=stale_reactivation,
        )
        if variant == "primary":
            initial_result = ledger.confirm_accepted_block(
                block_hash=str(initial["hash"]),
                active_tip_height=int(initial["height"]),
            )
            support.assert_equal(
                initial_result["audit_publication_sequence"],
                1,
                "C3 primary initial ordinal N",
            )
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as temp_cleanup:
            base = Path(tmp)
            root = base / "audit"
            evidence_path = base / "state" / "evidence.json"
            setup_store = _c2_store(root, evidence_path)
            temp_cleanup.callback(setup_store.close)
            initial_identity = AuditPublicationIdentity(
                1,
                int(initial["height"]),
                str(initial["hash"]),
            )
            initial_envelope = setup_store.live_envelope_path(
                block_height=int(initial["height"]),
                block_hash=str(initial["hash"]),
            )
            if variant == "primary":
                with setup_store.publication_order_guard():
                    initial_publication = _c2_publish(
                        setup_store,
                        identity=initial_identity,
                        publication_floor_sequence=1,
                        created_at="c3-primary-initial",
                    )
                support.assert_equal(
                    initial_publication.published,
                    True,
                    "C3 primary initial publication N",
                )
            setup_store.close()
            setup_store = None
            if variant == "primary":
                initial_envelope.unlink()
                support.assert_equal(
                    evidence_path.exists(),
                    True,
                    "C3 primary damaged N retains evidence",
                )
            support.assert_equal(
                initial_envelope.exists(),
                False,
                f"C3 {variant} N envelope starts absent",
            )
            expected_authority = _c2_expected_authority(root)
            tag_suffix = f"{variant[0]}_{support.RUN_TOKEN[:8]}"
            tag_a = f"qbit_a1_c3_a_{tag_suffix}"
            tag_b = f"qbit_a1_c3_b_{tag_suffix}"
            common_config: dict[str, object] = {
                "schema": schema,
                "root": str(root),
                "evidence_path": str(evidence_path),
                "variant": variant,
                "transition": "confirmation",
                "initial_sequence": 1,
                "initial": initial,
                "target": target,
                "stale_confirmation": stale_confirmation,
                "stale_reactivation": stale_reactivation,
            }
            config_a = base / "worker-a.json"
            config_b = base / "worker-b.json"
            _c2_write_config(
                config_a,
                {
                    **common_config,
                    "role": (
                        "c3-primary-a" if variant == "primary" else "c3-late-a"
                    ),
                    "writer": writer_a,
                    "application_name": tag_a,
                },
            )
            _c2_write_config(
                config_b,
                {
                    **common_config,
                    "role": "c3-b",
                    "writer": writer_b,
                    "application_name": tag_b,
                },
            )
            worker_a = _JsonWorker(config_a)
            temp_cleanup.callback(worker_a.close)
            if variant == "late":
                confirmed = worker_a.read_event("confirmed")
                support.assert_equal(
                    confirmed["authority"],
                    expected_authority,
                    "C3 late A confirmation guard authority",
                )
                support.assert_equal(
                    confirmed["identity"],
                    initial_identity.to_json(),
                    "C3 late A confirmed identity N",
                )
                support.assert_equal(
                    confirmed["floor"],
                    1,
                    "C3 late A confirmed floor N",
                )
            parked = worker_a.read_event("parked")
            support.assert_equal(
                parked["authority"],
                expected_authority,
                f"C3 {variant} A exact root/lock inode",
            )
            support.assert_equal(
                parked["guard"],
                "outside",
                f"C3 {variant} A parks outside guard",
            )
            if variant == "late":
                support.assert_equal(
                    parked["pid"],
                    confirmed["pid"],
                    "C3 late A exits guard in same child",
                )
            support.assert_equal(
                worker_a.process.poll(),
                None,
                f"C3 {variant} parked A remains alive",
            )
            initial_database = _c2_database_snapshot(
                ledger,
                block_hashes=all_hashes,
            )
            support.assert_equal(
                initial_database["floor"],
                1,
                f"C3 {variant} durable floor N before replacement",
            )
            support.assert_equal(
                initial_database["allocator"],
                {"last_value": 1, "is_called": True},
                f"C3 {variant} allocator N before replacement",
            )
            support.assert_equal(
                initial_database["rows"],
                [
                    {
                        "block_hash": initial["hash"],
                        "block_height": initial["height"],
                        "chain_state": "confirmed",
                        "maturity_state": "immature",
                        "audit_publication_sequence": 1,
                    },
                    {
                        "block_hash": target["hash"],
                        "block_height": target["height"],
                        "chain_state": "prepared",
                        "maturity_state": "immature",
                        "audit_publication_sequence": None,
                    },
                    {
                        "block_hash": stale_confirmation["hash"],
                        "block_height": stale_confirmation["height"],
                        "chain_state": "prepared",
                        "maturity_state": "immature",
                        "audit_publication_sequence": None,
                    },
                    {
                        "block_hash": stale_reactivation["hash"],
                        "block_height": stale_reactivation["height"],
                        "chain_state": "inactive",
                        "maturity_state": "immature",
                        "audit_publication_sequence": None,
                    },
                ],
                f"C3 {variant} exact rows before replacement",
            )
            damaged_snapshot = _c2_filesystem_snapshot(
                root=root,
                evidence_path=evidence_path,
            )
            if variant == "primary":
                damaged_evidence = json.loads(
                    evidence_path.read_text(encoding="utf-8")
                )
                support.assert_equal(
                    damaged_evidence["audit_publication_identity"],
                    initial_identity.to_json(),
                    "C3 primary damaged evidence still names N",
                )
            _c2_expire_writer(
                ledger,
                writer_id=str(writer_a["id"]),
                writer_epoch=int(writer_a["epoch"]),
                writer_token=str(writer_a["token"]),
            )
            worker_b = _JsonWorker(config_b)
            temp_cleanup.callback(worker_b.close)
            b_attempt = worker_b.read_event("flock-attempt")
            support.assert_equal(
                b_attempt["authority"],
                expected_authority,
                f"C3 {variant} B exact root/lock inode",
            )
            child_pids = {int(parked["pid"]), int(b_attempt["pid"])}
            support.assert_equal(
                len(child_pids),
                2,
                f"C3 {variant} distinct A/B OS processes",
            )
            if os.getpid() in child_pids:
                raise support.GateFailure(
                    f"C3 {variant} worker reused the parent process"
                )
            b_done = worker_b.read_event("transition-published")
            expected_b_identity = AuditPublicationIdentity(
                2,
                int(target["height"]),
                str(target["hash"]),
            )
            support.assert_equal(
                b_done["authority"],
                expected_authority,
                f"C3 {variant} B publication authority",
            )
            support.assert_equal(
                b_done["identity"],
                expected_b_identity.to_json(),
                f"C3 {variant} B identity N+1",
            )
            support.assert_equal(b_done["floor"], 2, f"C3 {variant} B floor N+1")
            support.assert_equal(
                b_done["published"],
                True,
                f"C3 {variant} B publishes N+1",
            )
            support.assert_equal(
                _c3_attempt_and_lease(ledger, variant=variant),
                {
                    "attempts": [
                        {"variant": variant, "writer_id": writer_b["id"]}
                    ],
                    "lease": {
                        "writer_id": writer_b["id"],
                        "writer_epoch": writer_b["epoch"],
                        "writer_session_token": writer_b["token"],
                    },
                },
                f"C3 {variant} B attempt and active replacement lease",
            )
            expected_database = _c3_expected_database(
                initial=initial,
                target=target,
                stale_confirmation=stale_confirmation,
                stale_reactivation=stale_reactivation,
            )
            support.assert_equal(
                _c2_database_snapshot(ledger, block_hashes=all_hashes),
                expected_database,
                f"C3 {variant} exact database after B",
            )
            target_envelope = root / (
                f"prism-live-audit-bundle-{target['height']}-{target['hash']}.json"
            )
            b_evidence_bytes = evidence_path.read_bytes()
            b_envelope_bytes = target_envelope.read_bytes()
            durable_b_evidence = json.loads(b_evidence_bytes)
            durable_b_envelope = json.loads(b_envelope_bytes)
            support.assert_equal(
                durable_b_evidence["audit_publication_identity"],
                expected_b_identity.to_json(),
                f"C3 {variant} durable B evidence identity N+1",
            )
            support.assert_equal(
                durable_b_evidence["block_hash"],
                target["hash"],
                f"C3 {variant} durable B evidence hash",
            )
            support.assert_equal(
                {
                    "block_hash": durable_b_envelope["block_hash"],
                    "block_height": durable_b_envelope["block_height"],
                    "audit_bundle_sha256": durable_b_envelope[
                        "audit_bundle_sha256"
                    ],
                },
                {
                    "block_hash": target["hash"],
                    "block_height": target["height"],
                    "audit_bundle_sha256": _C2_DIGEST,
                },
                f"C3 {variant} durable B envelope identity",
            )
            b_evidence_stat = evidence_path.lstat()
            b_envelope_stat = target_envelope.lstat()
            b_filesystem = _c2_filesystem_snapshot(
                root=root,
                evidence_path=evidence_path,
            )
            support.assert_equal(
                sorted(path.name for path in root.iterdir()),
                [".prism-audit-publication.lock", target_envelope.name],
                f"C3 {variant} exact B root entries",
            )
            support.assert_equal(
                initial_envelope.exists(),
                False,
                f"C3 {variant} N remains absent after B",
            )
            worker_a.send("attempt")
            stale_publication = worker_a.read_event("stale-publication")
            support.assert_equal(
                stale_publication["authority"],
                expected_authority,
                f"C3 {variant} late A same lock inode",
            )
            support.assert_equal(
                stale_publication["floor"],
                2,
                f"C3 {variant} late A reads fresh N+1 floor",
            )
            _assert_c3_stale_outcome(
                stale_publication["outcome"],
                label=f"C3 {variant} stale N publication",
            )
            stale_latest = stale_publication["latest"]
            assert isinstance(stale_latest, dict)
            support.assert_equal(
                stale_latest["audit_publication_identity"],
                expected_b_identity.to_json(),
                f"C3 {variant} stale A reconciles B evidence",
            )
            if variant == "primary":
                post_check = worker_a.read_event("post-check")
                transition_check = post_check["transition_check"]
                assert isinstance(transition_check, dict)
                errors = transition_check["errors"]
                assert isinstance(errors, dict)
                support.assert_equal(
                    set(errors),
                    {"confirmation", "reactivation"},
                    "C3 primary exact stale lease-failure labels",
                )
                for label in ("confirmation", "reactivation"):
                    if "writer lease is not active" not in str(errors[label]):
                        raise support.GateFailure(
                            f"C3 primary stale {label} wrong error: {errors[label]!r}"
                        )
                support.assert_equal(
                    transition_check["before"],
                    transition_check["after"],
                    "C3 primary child-observed stale transition immobility",
                )
                support.assert_equal(
                    post_check["retention"],
                    [
                        {
                            "retention": 0,
                            "live_removed": 0,
                            "candidate_removed": 0,
                            "errors": 0,
                        },
                        {
                            "retention": 1,
                            "live_removed": 0,
                            "candidate_removed": 0,
                            "errors": 0,
                        },
                    ],
                    "C3 primary stale A retention 0/1 pins B",
                )
                support.assert_equal(
                    post_check["authority"],
                    expected_authority,
                    "C3 primary post-check authority",
                )
            support.assert_equal(
                _c2_database_snapshot(ledger, block_hashes=all_hashes),
                expected_database,
                f"C3 {variant} late A row/floor/allocator immobility",
            )
            support.assert_equal(
                _c3_attempt_and_lease(ledger, variant=variant)["lease"],
                {
                    "writer_id": writer_b["id"],
                    "writer_epoch": writer_b["epoch"],
                    "writer_session_token": writer_b["token"],
                },
                f"C3 {variant} B lease remains active through A checks",
            )
            support.assert_equal(
                _c2_filesystem_snapshot(root=root, evidence_path=evidence_path),
                b_filesystem,
                f"C3 {variant} exact B filesystem survives late A",
            )
            support.assert_equal(
                evidence_path.read_bytes(),
                b_evidence_bytes,
                f"C3 {variant} B evidence bytes unchanged",
            )
            support.assert_equal(
                target_envelope.read_bytes(),
                b_envelope_bytes,
                f"C3 {variant} B envelope bytes unchanged",
            )
            evidence_after = evidence_path.lstat()
            envelope_after = target_envelope.lstat()
            support.assert_equal(
                (evidence_after.st_dev, evidence_after.st_ino),
                (b_evidence_stat.st_dev, b_evidence_stat.st_ino),
                f"C3 {variant} B evidence inode unchanged",
            )
            support.assert_equal(
                (envelope_after.st_dev, envelope_after.st_ino),
                (b_envelope_stat.st_dev, b_envelope_stat.st_ino),
                f"C3 {variant} B envelope inode unchanged",
            )
            support.assert_equal(
                initial_envelope.exists(),
                False,
                f"C3 {variant} stale A never repairs N",
            )
            if variant == "primary":
                damaged_entries = damaged_snapshot["entries"]
                assert isinstance(damaged_entries, dict)
                support.assert_equal(
                    sorted(damaged_entries),
                    [".prism-audit-publication.lock"],
                    "C3 primary damaged snapshot has no N envelope",
                )
            worker_a.send("finish")
            worker_a.read_event("closed")
            worker_a.wait_success()
            worker_b.send("finish")
            b_closed = worker_b.read_event("closed")
            support.assert_equal(
                b_closed["lease_released"],
                True,
                f"C3 {variant} B lease release",
            )
            worker_b.wait_success()
            _assert_no_tagged_backends(
                (tag_a, tag_b),
                f"C3 {variant} tagged child backend cleanup",
            )
            with support.ACTIVE_CHILDREN_LOCK:
                support.assert_equal(
                    len(support.ACTIVE_CHILDREN),
                    0,
                    f"C3 {variant} active child registry cleanup",
                )
    finally:
        if setup_store is not None:
            setup_store.close()
        if worker_a is not None:
            worker_a.close()
        if worker_b is not None:
            worker_b.close()
        ledger.close()


def test_c3_b_wins_and_late_a_interleavings() -> None:
    for variant in ("primary", "late"):
        _test_c3_interleaving(variant)


def server_evidence() -> dict[str, object]:
    evidence = support.run_json(
        """
SELECT json_build_object(
    'server_version', current_setting('server_version'),
    'server_version_num', current_setting('server_version_num')
);
"""
    )
    configured_image = os.environ.get("QBIT_PRISM_GATE_IMAGE", "").strip()
    provisioned_image_digest = os.environ.get(
        "QBIT_PRISM_GATE_IMAGE_DIGEST",
        "",
    ).strip()
    if not configured_image or configured_image.casefold() == "unreported":
        raise support.GateFailure("configured PostgreSQL image evidence is required")
    if (
        not provisioned_image_digest
        or provisioned_image_digest.casefold() == "unreported"
    ):
        raise support.GateFailure(
            "provisioned PostgreSQL image digest evidence is required"
        )
    evidence["configured_image"] = configured_image
    evidence["provisioned_image_digest"] = provisioned_image_digest
    return evidence


def main() -> None:
    public_before = support.public_sentinel()
    failure: BaseException | None = None
    try:
        test_database_observed_confirmation_order()
        test_c2_a_wins_confirmation_publication()
        test_c3_b_wins_and_late_a_interleavings()
    except BaseException as error:
        failure = error
    try:
        support.cleanup_active_children()
        support.cleanup_owned_schemas()
        support.assert_equal(
            support.marker_schema_count(),
            0,
            "process gate marker cleanup",
        )
        support.assert_equal(
            support.public_sentinel(),
            public_before,
            "process gate public preservation",
        )
    except BaseException as cleanup_error:
        if failure is None:
            raise
        raise support.GateFailure(
            f"process scenario failed with {failure!r}; cleanup also failed "
            f"with {cleanup_error!r}"
        ) from cleanup_error
    else:
        support.atexit.unregister(support.cleanup_active_children)
        support.atexit.unregister(support.cleanup_owned_schemas)
    if failure is not None:
        raise failure
    print("prism postgres A1 process gate evidence " + json.dumps(server_evidence()))
    print(
        "prism postgres A1 process gate PASS "
        "C1-confirmation-order C2-A-wins-confirmation-publication "
        "C3-B-wins-late-A"
    )


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--c2-worker":
        raise SystemExit(_run_c2_worker(Path(sys.argv[2])))
    if len(sys.argv) != 1:
        raise SystemExit("unexpected process-gate arguments")
    main()

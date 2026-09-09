"""Recover allowlisted active-chain candidates using normal PRISM accounting.

Invoke in a fresh managed coordinator container with the normal coordinator
stopped. Without --apply this only reads metadata and qbitd chain identity.
Apply holds the existing writer lease and heartbeat, loads one payload at a
time, and never starts serve(), Stratum, ordinary replay, or job prewarming.
"""

from __future__ import annotations

import argparse
import hmac
import json
import signal
import threading
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Callable

from lab.auxpow.stratum_codec import header_hash_hex
from lab.prism.block_candidates import _BlockCandidateNodeSubmission
from lab.prism.coordinator_config import CoordinatorConfig, load_coordinator_config
from lab.prism.prism_coordinator import PrismCoordinator
from lab.prism.recovery_json import cooperative_json
from lab.prism.rpc import JsonRpc
from lab.prism.share_ledger import (
    PsqlShareLedger,
    block_candidate_identity_sha256,
    canonical_hex,
    database_url_from_psql_command,
)

MAX_RECOVERY_BLOCKS = 32
SUMMARY_SCHEMA = "qbit.prism.pending-block-recovery.v1"


class RecoveryError(RuntimeError):
    pass


@dataclass(frozen=True)
class RecoveryBlock:
    block_hash: str
    height: int
    parent_hash: str
    state: str


def refuse_lease_wait(_seconds: float) -> None:
    raise RecoveryError(
        "exclusive writer lease unavailable; stop the coordinator and wait "
        "for its lease release before retrying"
    )


class RecoveryCoordinator(PrismCoordinator):
    """Use the production assembly without unrelated startup/publication work."""

    def make_ledger(self, **_kwargs: Any) -> PsqlShareLedger:
        ledger = super().make_ledger(lease_retry_sleep=refuse_lease_wait)
        if not isinstance(ledger, PsqlShareLedger):
            raise RecoveryError("recovery requires the native PostgreSQL ledger")
        self.ledger = ledger  # Available for cleanup even if __init__ later fails.
        return ledger

    def _upgrade_legacy_audit_evidence(self) -> None:
        # The normal coordinator owns this unrelated startup migration.
        pass

    def _submit_block_candidate_to_node(self, *_args: Any, **_kwargs: Any) -> Any:
        # Active-chain accounting must not create a new node offer if a chain
        # observation changes during finalization. The normal verifier and
        # accounting fences still run; stop and preserve the pending intent.
        raise RecoveryError("recovery refuses to submit a block to the node")


class RecoveryReader:
    """Metadata and single-payload reads on a server-enforced read-only session."""

    def __init__(self, database_url: str) -> None:
        import psycopg
        from psycopg.rows import dict_row

        self.connection = psycopg.connect(
            database_url,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=5,
        )
        try:
            self.connection.execute("SET default_transaction_read_only = on")
            self.connection.execute("SET statement_timeout = '30s'")
            self.connection.execute("SET lock_timeout = '5s'")
        except BaseException:
            self.connection.close()
            raise

    def close(self) -> None:
        self.connection.close()

    def metadata(self, block_hash: str) -> dict[str, Any] | None:
        return self.connection.execute(
            """SELECT outbox.block_hash, outbox.state, outbox.candidate_sha256,
                      pool.block_height, pool.parent_hash, pool.chain_state,
                      EXISTS (SELECT 1 FROM qbit_pool_audit_bundles audit
                              WHERE audit.block_hash = outbox.block_hash) AS has_audit
               FROM qbit_block_candidate_outbox outbox
               LEFT JOIN qbit_pool_blocks pool USING (block_hash)
               WHERE outbox.block_hash = %s""",
            (block_hash,),
        ).fetchone()

    def candidate(self, block_hash: str) -> dict[str, Any]:
        """One pending row's payload facts, for either storage version.

        Issue #255: a version-2 row carries no ``candidate`` jsonb. Its
        body lives in ``qbit_block_candidate_body`` and is read back in
        bounded pages by the ledger (``hydrate_block_candidate_intent``),
        never through this read-only session. The columns below name the
        version explicitly so a chunked candidate is decoded through that
        route rather than mistaken for a missing or corrupt payload.
        """
        row = self.connection.execute(
            """SELECT outbox.candidate, outbox.candidate_sha256,
                      outbox.storage_version, outbox.body_id, outbox.replay_header,
                      body.byte_count, body.chunk_count, body.chunk_bytes,
                      body.share_count, body.state AS body_state
               FROM qbit_block_candidate_outbox outbox
               LEFT JOIN qbit_block_candidate_body body ON body.body_id = outbox.body_id
               WHERE outbox.block_hash = %s AND outbox.state = 'pending'""",
            (block_hash,),
        ).fetchone()
        if row is None:
            raise RecoveryError(f"pending candidate disappeared: {block_hash}")
        return row


def require_active_block(rpc: Any, block_hash: str) -> tuple[int, str]:
    header = rpc.call("getblockheader", [block_hash])
    height = header.get("height") if isinstance(header, dict) else None
    if isinstance(height, bool) or not isinstance(height, int) or height < 1:
        raise RecoveryError(f"invalid chain header for {block_hash}")
    if rpc.call("getblockhash", [height]) != block_hash:
        raise RecoveryError(f"candidate is not on the active chain: {block_hash}")
    parent = canonical_hex(
        str(header.get("previousblockhash", "")), name="parent_hash", expected_bytes=32
    )
    return height, parent


def require_completed(row: dict[str, Any] | None, block: RecoveryBlock) -> None:
    if not row or not (
        row["state"] == "submitted"
        and row["chain_state"] == "confirmed"
        and row["block_height"] == block.height
        and row["parent_hash"] == block.parent_hash
        and row["has_audit"] is True
    ):
        raise RecoveryError(f"accounting/outbox completion is not proven: {block.block_hash}")


def plan_recovery(reader: RecoveryReader, rpc: Any, hashes: list[str]) -> list[RecoveryBlock]:
    """Validate the entire allowlist before the first writer lease is acquired."""
    hashes = [canonical_hex(value, name="block_hash", expected_bytes=32) for value in hashes]
    if not hashes or len(hashes) > MAX_RECOVERY_BLOCKS or len(set(hashes)) != len(hashes):
        raise RecoveryError(f"specify 1–{MAX_RECOVERY_BLOCKS} distinct block hashes")
    blocks = []
    for block_hash in hashes:
        row = reader.metadata(block_hash)
        if not row or row["state"] not in {"pending", "submitted"}:
            raise RecoveryError(f"candidate is missing or abandoned: {block_hash}")
        height, parent = require_active_block(rpc, block_hash)
        block = RecoveryBlock(block_hash, height, parent, row["state"])
        if block.state == "submitted":
            require_completed(row, block)
        parent_row = reader.metadata(parent)
        if parent_row and parent_row["state"] == "pending" and parent not in hashes:
            raise RecoveryError(f"pending parent must be included in the allowlist: {parent}")
        blocks.append(block)
    return sorted(blocks, key=lambda block: block.height)


def load_intent(
    coordinator: PrismCoordinator, block: RecoveryBlock, row: dict[str, Any]
) -> Any:
    """The intent mapping for one pending row, by storage version (#255).

    Version 1 is the legacy whole-jsonb payload the read-only session
    already decoded (cooperatively, in this standalone process). Version 2
    is hydrated by the coordinator's ledger in bounded chunk pages into a
    spool file, and its decoded facts are exposed through the same mapping
    interface. Any other version is refused rather than skipped.
    """
    storage_version = int(row.get("storage_version") or 1)
    if storage_version == 1:
        return row["candidate"]
    if storage_version != 2:
        raise RecoveryError(
            f"unsupported candidate storage version {storage_version}: {block.block_hash}"
        )
    if row.get("body_id") is None or row.get("body_state") != "sealed":
        raise RecoveryError(f"chunked candidate body is not sealed: {block.block_hash}")
    hydrate = getattr(coordinator.ledger, "hydrate_block_candidate_intent", None)
    if not callable(hydrate):
        raise RecoveryError("the ledger cannot hydrate chunked candidate bodies")
    header_row = {
        "block_hash": block.block_hash,
        "storage_version": 2,
        "candidate_sha256": str(row["candidate_sha256"]),
        "header": row.get("replay_header") or {},
        "body": {
            "body_id": row["body_id"],
            "storage_version": 2,
            "candidate_sha256": str(row["candidate_sha256"]),
            "byte_count": row["byte_count"],
            "chunk_count": row["chunk_count"],
            "chunk_bytes": row["chunk_bytes"],
            "share_count": row["share_count"],
            "state": row["body_state"],
        },
    }
    return hydrate(header_row, cancelled=coordinator.stop_event.is_set)


def decode_candidate(
    coordinator: PrismCoordinator, block: RecoveryBlock, row: dict[str, Any]
) -> Any:
    intent = load_intent(coordinator, block, row)
    if not isinstance(intent, Mapping) or intent.get("block_hash_hex") != block.block_hash:
        raise RecoveryError("candidate payload does not match its outbox key")
    digest = block_candidate_identity_sha256(intent)
    if not hmac.compare_digest(digest, str(row["candidate_sha256"])):
        raise RecoveryError(f"candidate identity digest mismatch: {block.block_hash}")
    if (
        intent.get("expected_height") != block.height
        or intent.get("parent_hash") != block.parent_hash
    ):
        raise RecoveryError(f"candidate does not match its active-chain header: {block.block_hash}")
    header = bytes.fromhex(str(intent["block_hex"])[:160])
    if len(header) != 80 or header_hash_hex(header) != block.block_hash:
        raise RecoveryError(f"candidate block bytes do not match its hash: {block.block_hash}")
    return replace(coordinator.block_candidate_from_intent(intent), durable_replay=True)


def recover_block(
    coordinator: PrismCoordinator, reader: RecoveryReader, block: RecoveryBlock
) -> None:
    # Re-read after acquisition; the earlier plan is advisory.
    row = reader.metadata(block.block_hash)
    if row and row["state"] == "submitted":
        require_completed(row, block)
        require_active_block(coordinator.rpc, block.block_hash)
        return
    candidate = decode_candidate(coordinator, block, reader.candidate(block.block_hash))
    with coordinator._writer_operation("accepted_block_handling"):
        if coordinator.stop_event.is_set():
            raise RecoveryError("recovery interrupted")
        if require_active_block(coordinator.rpc, block.block_hash) != (
            block.height,
            block.parent_hash,
        ):
            raise RecoveryError("chain identity changed since the recovery plan")
        coordinator._require_fresh_ledger_lease_for_external_side_effect("block-recovery")
        coordinator._mark_block_candidate_attempted(block.block_hash)
        with coordinator._block_landing_ledger_statement_timeout_scope(block.block_hash):
            accepted = coordinator.submit_block_candidate(
                candidate,
                node_submission=_BlockCandidateNodeSubmission(attempted=False),
            )
        if not accepted:
            raise RecoveryError(
                f"normal finalization deferred or rejected {block.block_hash}; outbox retained"
            )
        coordinator._finalize_block_candidate(
            candidate,
            block_hash=block.block_hash,
            accepted=True,
            error="",
            outcome=coordinator._block_candidate_outcome,
        )
    require_completed(reader.metadata(block.block_hash), block)
    require_active_block(coordinator.rpc, block.block_hash)


def recovery_config(config: CoordinatorConfig) -> CoordinatorConfig:
    return replace(
        config,
        ledger=replace(
            config.ledger,
            initialize_schema=False,
            native_client_mode="on",
            writer_session_token=None,
        ),
        stop_after_block=False,
        max_blocks=2**31 - 1,
    )


def apply_recovery(
    config: CoordinatorConfig,
    reader: RecoveryReader,
    blocks: list[RecoveryBlock],
    *,
    timeout_seconds: int,
    report: Callable[[dict[str, Any]], None],
) -> None:
    coordinator = RecoveryCoordinator.__new__(RecoveryCoordinator)
    initialized = False
    deadline = None
    old_handlers = {}
    try:
        coordinator.__init__(recovery_config(config))
        initialized = True
        if coordinator._start_ledger_lease_heartbeat() is None:
            raise RecoveryError("the guarded lease heartbeat did not start")

        def interrupt(signum: int, _frame: Any) -> None:
            coordinator.request_shutdown(signum)
            raise RecoveryError("recovery interrupted; pending work remains replayable")

        for signum in (signal.SIGTERM, signal.SIGINT):
            old_handlers[signum] = signal.signal(signum, interrupt)
        deadline = threading.Timer(
            timeout_seconds,
            coordinator._watchdog_hard_exit,
            args=("pending-block recovery deadline exceeded",),
        )
        deadline.daemon = True
        deadline.start()
        coordinator.validate_live_chain_identity()
        coordinator.validate_live_template_and_fee_policy()
        coordinator.prism_payout_policy()
        for block in blocks:
            report({"event": "recovering", "block_hash": block.block_hash, "height": block.height})
            recover_block(coordinator, reader, block)
            report({"event": "completed", "block_hash": block.block_hash, "height": block.height})
    finally:
        # Keep the deadline armed through writer drainage. An uncertain write
        # must not turn a hung cleanup into an indefinitely held lease.
        try:
            if initialized:
                if not coordinator.shutdown(reason="block_recovery_finished"):
                    raise RecoveryError("writer drainage/lease release failed")
                coordinator.drain_non_writer_components()
            else:
                ledger = getattr(coordinator, "ledger", None)
                if ledger is not None:
                    ledger.release_writer_lease()
        finally:
            try:
                ledger = getattr(coordinator, "ledger", None)
                if ledger is not None:
                    ledger.close()
            finally:
                try:
                    store = coordinator.__dict__.get("_audit_artifact_store")
                    if store is not None:
                        store.close()
                finally:
                    if deadline is not None:
                        deadline.cancel()
                        deadline.join(timeout=1)
                    for signum, handler in old_handlers.items():
                        signal.signal(signum, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--block-hash", action="append", required=True, dest="hashes")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="complete accounting; normal coordinator must be stopped",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=600,
        help="processing and cleanup deadline after construction (default: 600)",
    )
    args = parser.parse_args(argv)
    if not 1 <= args.timeout_seconds <= 3600:
        parser.error("--timeout-seconds must be between 1 and 3600")
    reader = None

    def report(event: dict[str, Any]) -> None:
        print(json.dumps({"schema": SUMMARY_SCHEMA, **event}, sort_keys=True), flush=True)

    try:
        with cooperative_json():
            config = load_coordinator_config()
            database_url = config.ledger.database_url or database_url_from_psql_command(
                config.ledger.psql_command
            )
            if not database_url or config.ledger.allow_memory_ledger:
                raise RecoveryError(
                    "recovery requires PRISM_DATABASE_URL and the native PostgreSQL client"
                )
            reader = RecoveryReader(database_url)
            rpc = JsonRpc(
                host=config.rpc.host,
                port=config.rpc.port,
                user=config.rpc.user,
                password=config.rpc.password,
            )
            blocks = plan_recovery(reader, rpc, args.hashes)
            report(
                {
                    "event": "plan",
                    "apply": args.apply,
                    "blocks": [block.__dict__ for block in blocks],
                }
            )
            if args.apply:
                apply_recovery(
                    config, reader, blocks, timeout_seconds=args.timeout_seconds, report=report
                )
            report({"event": "success", "apply": args.apply})
        return 0
    except Exception as exc:
        # Driver errors can embed a credential-bearing DSN or SQL payload.
        report(
            {
                "event": "failed",
                "error": str(exc) if isinstance(exc, RecoveryError) else type(exc).__name__,
            }
        )
        return 1
    finally:
        if reader is not None:
            reader.close()


if __name__ == "__main__":
    raise SystemExit(main())

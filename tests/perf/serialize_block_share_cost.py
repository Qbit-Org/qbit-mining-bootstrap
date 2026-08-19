#!/usr/bin/env python3
"""Per-share interpreter-CPU cost of ``direct_stratum.serialize_block``.

Measures the block-serialization term on the PRISM share-accept path across
template size, so the "``serialize_block`` is O(block bytes) per accepted
share" gap left open by the issue #143 findings comment can be closed with a
number instead of an estimate.

Methodology mirrors section 1 of that comment so the results are comparable:

* every timing is **min-of-7 repetitions**, each repetition an auto-sized
  batch of calls, reported per call;
* the headline clock is **interpreter CPU**, not wall clock -- see
  ``CLOCK_RATIONALE`` below;
* single-threaded: the findings comment established this path is GIL-bound
  and does not scale with threads, so what is wanted is the per-share cost,
  not concurrency behaviour.

Three measurement groups:

1. **Anchors.** The comment's two published numbers -- a 12.4 us primitives
   floor per accepted share and a 9.0 us 0-transaction-template composite
   ``assemble_submission`` -- reproduced on the host running this script, so
   every sweep number below can be read against the same scale.
2. **Axis 1**, total block bytes from the 0-transaction baseline to the qbit
   consensus ceiling of 2,000,000 bytes (``MAX_BLOCK_WEIGHT`` with
   ``WITNESS_SCALE_FACTOR = 1``, so weight units are bytes).
3. **Axis 2**, transaction count at fixed total block bytes, which separates
   the per-transaction constant from the per-byte memcpy/hex cost.

Plus a decomposition of the serialization term into its constituent buffer
operations, and a restatement of single-core share capacity at each point.

Self-contained and re-runnable: no database, no network, no coordinator, no
third-party packages. ``python3 tests/perf/serialize_block_share_cost.py``
prints the tables; ``--json`` emits the same data as JSON.

Nothing under ``lab/`` is imported for anything but read-only measurement.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import sysconfig
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Sequence

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.auxpow import stratum_codec
from lab.prism import direct_stratum
from lab.prism.share_submission import (
    RecentShareIndex,
    parse_submit_request,
    validate_submit_request,
)


CLOCK_RATIONALE = (
    "time.thread_time_ns() is the headline clock: the driver is single "
    "threaded, so per-thread CPU is exactly the interpreter CPU this path "
    "burns, it excludes time the OS deschedules us (which perf_counter would "
    "charge us for on a non-idle host), and on this platform its resolution "
    "is far finer than process_time's. process_time and perf_counter are "
    "captured over the same batch and reported alongside so any divergence "
    "-- e.g. kernel time for multi-megabyte allocations -- is visible."
)

# Published anchors from the issue #143 findings comment, section 1.
PUBLISHED_PRIMITIVES_FLOOR_US = 12.4
PUBLISHED_ZERO_TX_COMPOSITE_US = 9.0
# Same comment: "~15.7 us for the composite submission path (primitives-ratio
# estimate)" and "64k-81k shares/s of single-core capacity".
PUBLISHED_FULL_PATH_ESTIMATE_US = 15.7
PUBLISHED_CAPACITY_LOW_PER_S = 64_000
PUBLISHED_CAPACITY_HIGH_PER_S = 81_000
# Deployed design point named by the issue: ~4,096 connections at
# PRISM_STRATUM_VARDIFF_TARGET_SECONDS=15 -> 4096/15 shares/s steady state.
DESIGN_POINT_SHARES_PER_S = 4096 / 15

# qbit consensus ceiling. crates/qbit-pool-builder/src/lib.rs:16-17 sets
# MAX_BLOCK_WEIGHT = 2_000_000 with WITNESS_SCALE_FACTOR = 1, so weight units
# are bytes; compose.yaml and the ckpool preflight both hard-pin the same
# 2000000 against live getblocktemplate.weightlimit.
MAX_BLOCK_BYTES = 2_000_000

EXTRANONCE1_HEX = "deadbeef"
EXTRANONCE2_SIZE = 8
NTIME_HEX = "68a4c1a0"
NONCE_HEX = "12345678"

# Header (80) + the transaction-count compact size + the coinbase.
HEADER_BYTES = 80

AXIS1_TARGET_BLOCK_BYTES = (0, 50_000, 100_000, 250_000, 500_000, 1_000_000, 1_500_000, 2_000_000)
AXIS1_NOMINAL_TX_BYTES = 500
AXIS2_TOTAL_BLOCK_BYTES = 1_000_000
AXIS2_TX_COUNTS = (10, 100, 500, 1_000, 2_000, 5_000)


# --------------------------------------------------------------------------
# timing harness
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Timing:
    """Per-call cost from the best of ``reps`` batched repetitions."""

    thread_cpu_us: float
    process_cpu_us: float
    wall_us: float
    batch: int
    reps: int

    def as_json(self) -> dict[str, Any]:
        return {
            "thread_cpu_us": round(self.thread_cpu_us, 6),
            "process_cpu_us": round(self.process_cpu_us, 6),
            "wall_us": round(self.wall_us, 6),
            "batch": self.batch,
            "reps": self.reps,
        }


def measure(
    fn: Callable[[], Any],
    *,
    reps: int = 7,
    min_batch_seconds: float = 0.05,
    max_batch: int = 1 << 22,
) -> Timing:
    """Min-of-``reps`` batched timing of ``fn``, reported per call.

    The batch is auto-sized so one repetition spans at least
    ``min_batch_seconds``, keeping sub-microsecond primitives well clear of
    clock granularity. The minimum is taken on thread CPU; the process-CPU and
    wall figures reported are the ones from that same winning repetition, so
    the three are internally consistent.
    """

    batch = 1
    while True:
        start = time.perf_counter_ns()
        for _ in range(batch):
            fn()
        elapsed = time.perf_counter_ns() - start
        if elapsed >= min_batch_seconds * 1e9 or batch >= max_batch:
            break
        growth = max(2, min(16, int(min_batch_seconds * 1e9 / max(elapsed, 1)) + 1))
        batch = min(max_batch, batch * growth)

    best: tuple[float, float, float] | None = None
    for _ in range(reps):
        thread_start = time.thread_time_ns()
        process_start = time.process_time_ns()
        wall_start = time.perf_counter_ns()
        for _ in range(batch):
            fn()
        wall_end = time.perf_counter_ns()
        process_end = time.process_time_ns()
        thread_end = time.thread_time_ns()
        candidate = (
            (thread_end - thread_start) / batch / 1000.0,
            (process_end - process_start) / batch / 1000.0,
            (wall_end - wall_start) / batch / 1000.0,
        )
        if best is None or candidate[0] < best[0]:
            best = candidate

    assert best is not None
    return Timing(best[0], best[1], best[2], batch=batch, reps=reps)


# --------------------------------------------------------------------------
# synthetic template material
# --------------------------------------------------------------------------


def _compact_size_hex(value: int) -> str:
    return direct_stratum.compact_size(value).hex()


def coinbase_transaction_hex(placeholder_hex: str, *, outputs: int = 1) -> str:
    """A structurally valid non-witness coinbase ending in the placeholder.

    Same shape as ``lab/prism/job_build_benchmark.py``'s
    ``synthetic_manifest_coinbase_hex``: a height push followed by the
    extranonce placeholder at the tail of the coinbase scriptSig, which is
    where ``split_coinbase_extranonce`` requires it.
    """

    script_sig = "03aabbcc" + placeholder_hex
    output = (50_00000000).to_bytes(8, "little").hex() + "0151"
    return (
        "01000000"
        + "01"
        + "00" * 32
        + "ffffffff"
        + _compact_size_hex(len(script_sig) // 2)
        + script_sig
        + "ffffffff"
        + _compact_size_hex(outputs)
        + output * outputs
        + "00000000"
    )


def synthetic_transaction_hex(size_bytes: int, seed: int) -> str:
    """A non-witness 1-in/1-out transaction of exactly ``size_bytes`` bytes.

    Padding lives in the output scriptPubKey, with 0-3 slack bytes available
    in the input scriptSig so every target size is reachable across compact
    size width boundaries. ``seed`` varies the prevout so txids differ and the
    merkle branch is not degenerate.

    Structurally valid enough for the code under measurement: it round-trips
    through ``strip_witness_transaction``, which is what the job build runs on
    every template transaction.
    """

    for cs_width in (1, 3, 5):
        for script_sig_len in range(0, 4):
            script_pubkey_len = size_bytes - 59 - cs_width - script_sig_len
            if script_pubkey_len < 0:
                continue
            if len(direct_stratum.compact_size(script_pubkey_len)) != cs_width:
                continue
            prevout = (seed & 0xFFFFFFFF).to_bytes(4, "little").hex() + "11" * 28
            return (
                "02000000"
                + "01"
                + prevout
                + "03000000"
                + _compact_size_hex(script_sig_len)
                + "ab" * script_sig_len
                + "ffffffff"
                + "01"
                + (1234).to_bytes(8, "little").hex()
                + _compact_size_hex(script_pubkey_len)
                + "51" * script_pubkey_len
                + "00000000"
            )
    raise ValueError(f"no transaction encoding reaches {size_bytes} bytes")


def base_template() -> dict[str, Any]:
    """The repo's own 0-transaction baseline template.

    Mirrors ``lab/prism/job_build_benchmark.py``'s ``base_template``; the
    transaction list is supplied separately to
    ``make_job_from_builder_manifest``.
    """

    return {
        "height": 1000,
        "previousblockhash": "11" * 32,
        "bits": "1b00ffff",
        "version": 0x20000000,
        "curtime": 1_755_000_000,
        "coinbasevalue": 50_00000000,
        "transactions": [],
    }


@dataclass
class Scenario:
    """One template size, built as a real job the accept path would receive."""

    label: str
    job: direct_stratum.DirectQbitStratumJob
    transaction_count: int
    block_bytes: int
    coinbase_bytes: int
    transaction_bytes: int
    merkle_branch_levels: int
    construction: str


def build_scenario(
    *,
    label: str,
    transaction_count: int,
    target_block_bytes: int | None,
    coinbase_outputs: int = 1,
    construction: str = "",
) -> Scenario:
    """Build a real ``DirectQbitStratumJob`` of the requested size.

    Route 1 of the two acceptable routes: transactions go through
    ``make_job_from_builder_manifest``, so ``merkle_branch_for_coinbase`` runs
    ``strip_witness_transaction`` over every one of them and the resulting job
    carries a realistic merkle branch. That matters because branch depth is
    itself a per-share cost inside ``assemble_submission``; a hand-frozen job
    with an empty branch would understate the composite at large tx counts.
    """

    placeholder_hex = EXTRANONCE1_HEX + "00" * EXTRANONCE2_SIZE
    coinbase_hex = coinbase_transaction_hex(placeholder_hex, outputs=coinbase_outputs)
    coinbase_bytes = len(coinbase_hex) // 2

    if transaction_count == 0:
        transaction_hexes: tuple[str, ...] = ()
    else:
        count_prefix = len(direct_stratum.compact_size(transaction_count + 1))
        assert target_block_bytes is not None
        payload = target_block_bytes - HEADER_BYTES - count_prefix - coinbase_bytes
        base_size, remainder = divmod(payload, transaction_count)
        if base_size < 60:
            raise ValueError(
                f"{transaction_count} transactions cannot fit in "
                f"{target_block_bytes} bytes (min 60 bytes each)"
            )
        transaction_hexes = tuple(
            synthetic_transaction_hex(base_size + (1 if index < remainder else 0), index)
            for index in range(transaction_count)
        )

    job = direct_stratum.make_job_from_builder_manifest(
        job_id=f"perf-{label}",
        template=base_template(),
        manifest={"coinbase_tx_hex": coinbase_hex},
        extranonce1_hex=EXTRANONCE1_HEX,
        extranonce2_size=EXTRANONCE2_SIZE,
        desired_share_difficulty=Decimal("1"),
        transaction_hexes=transaction_hexes,
    )

    transaction_bytes = sum(len(tx) // 2 for tx in transaction_hexes)
    block_bytes = (
        HEADER_BYTES
        + len(direct_stratum.compact_size(transaction_count + 1))
        + coinbase_bytes
        + transaction_bytes
    )
    return Scenario(
        label=label,
        job=job,
        transaction_count=transaction_count,
        block_bytes=block_bytes,
        coinbase_bytes=coinbase_bytes,
        transaction_bytes=transaction_bytes,
        merkle_branch_levels=len(job.merkle_branch),
        construction=construction,
    )


# --------------------------------------------------------------------------
# the measured terms
# --------------------------------------------------------------------------


@dataclass
class ScenarioTiming:
    scenario: Scenario
    composite: Timing
    serialize_term: Timing
    serialize_block_only: Timing
    hex_encode_only: Timing

    @property
    def term_share_percent(self) -> float:
        if self.composite.thread_cpu_us <= 0:
            return float("nan")
        return 100.0 * self.serialize_term.thread_cpu_us / self.composite.thread_cpu_us

    @property
    def composite_less_term_us(self) -> float:
        return self.composite.thread_cpu_us - self.serialize_term.thread_cpu_us


def submission_inputs(job: direct_stratum.DirectQbitStratumJob) -> dict[str, Any]:
    """Recompute the intermediates ``assemble_submission`` derives per share."""

    extranonce2_hex = "00" * EXTRANONCE2_SIZE
    version_hex = stratum_codec.apply_version_bits(job.version, None, 0)
    coinbase_without_witness, header = stratum_codec.assemble_header_from_notify_submit(
        coinb1_hex=job.coinb1,
        extranonce1_hex=job.extranonce1_hex,
        extranonce2_hex=extranonce2_hex,
        coinb2_hex=job.coinb2,
        merkle_branch_hex=list(job.merkle_branch),
        version_hex=version_hex,
        prevhash_hex=job.prevhash,
        ntime_hex=NTIME_HEX,
        nbits_hex=job.nbits,
        nonce_hex=NONCE_HEX,
    )
    full_coinbase_hex = (
        job.full_coinbase_prefix
        + job.extranonce1_hex
        + extranonce2_hex
        + job.full_coinbase_suffix
    )
    return {
        "extranonce2_hex": extranonce2_hex,
        "version_hex": version_hex,
        "coinbase_without_witness": coinbase_without_witness,
        "header": header,
        "full_coinbase_hex": full_coinbase_hex,
    }


def time_scenario(scenario: Scenario, *, reps: int, min_batch_seconds: float) -> ScenarioTiming:
    job = scenario.job
    inputs = submission_inputs(job)
    header = inputs["header"]
    full_coinbase_hex = inputs["full_coinbase_hex"]
    extranonce2_hex = inputs["extranonce2_hex"]
    transaction_hexes = job.transaction_hexes
    serialize_block = direct_stratum.serialize_block
    assemble = direct_stratum.assemble_submission
    block_bytes = serialize_block(header, full_coinbase_hex, transaction_hexes)

    kwargs = {"reps": reps, "min_batch_seconds": min_batch_seconds}
    return ScenarioTiming(
        scenario=scenario,
        # The whole per-share call as the coordinator invokes it.
        composite=measure(
            lambda: assemble(
                job,
                extranonce2_hex=extranonce2_hex,
                ntime_hex=NTIME_HEX,
                nonce_hex=NONCE_HEX,
            ),
            **kwargs,
        ),
        # direct_stratum.py:500 verbatim, the term under measurement.
        serialize_term=measure(
            lambda: serialize_block(header, full_coinbase_hex, transaction_hexes).hex(),
            **kwargs,
        ),
        serialize_block_only=measure(
            lambda: serialize_block(header, full_coinbase_hex, transaction_hexes),
            **kwargs,
        ),
        hex_encode_only=measure(block_bytes.hex, **kwargs),
    )


def decompose_term(scenario: Scenario, *, reps: int, min_batch_seconds: float) -> dict[str, Timing]:
    """Split the serialization term into the buffer operations it performs.

    ``serialize_block`` runs ``validate_hex`` (which builds an f-string field
    name, length-checks, ``bytes.fromhex``-probes and discards, then returns
    ``value.lower()`` -- a full fresh copy of the hex string) over the
    coinbase and every transaction, then ``bytes.fromhex`` decodes the
    validated result for real, joins, and the caller re-encodes with
    ``.hex()``. Each of those is timed here over the same material.
    """

    inputs = submission_inputs(scenario.job)
    hexes: tuple[str, ...] = (inputs["full_coinbase_hex"],) + scenario.job.transaction_hexes
    decoded = [bytes.fromhex(value) for value in hexes]
    joined = b"".join(decoded)
    validate_hex = stratum_codec.validate_hex
    from_hex = bytes.fromhex
    kwargs = {"reps": reps, "min_batch_seconds": min_batch_seconds}

    def fromhex_probe() -> None:
        for value in hexes:
            from_hex(value)

    def lower_copy() -> None:
        for value in hexes:
            value.lower()

    def validate_hex_calls() -> None:
        # Full validate_hex as serialize_block calls it, f-string field name
        # included -- the f-string is built per transaction even though it is
        # only ever used on the raising path.
        for index, value in enumerate(hexes):
            validate_hex(value, field_name=f"transaction_hexes[{index}]")

    def real_decode() -> None:
        # The list/generator build that produces the transaction bytes.
        [from_hex(value) for value in hexes]

    def join_only() -> None:
        b"".join(decoded)

    def hex_encode() -> None:
        joined.hex()

    def loop_floor() -> None:
        for _value in hexes:
            pass

    header = inputs["header"]
    count_prefix = direct_stratum.compact_size(len(hexes))

    def floor_single_decode_bytes_out() -> None:
        # Counterfactual A: hex inputs kept, but decoded once (no validate_hex
        # probe, no .lower() copy) and the block returned as bytes with no
        # caller-side .hex() re-encode.
        header + count_prefix + b"".join([from_hex(value) for value in hexes])

    def floor_bytes_in_bytes_out() -> None:
        # Counterfactual B: transactions already held as bytes. Pure memcpy
        # floor for producing the same block.
        header + count_prefix + b"".join(decoded)

    return {
        "validate_hex_fromhex_probe": measure(fromhex_probe, **kwargs),
        "validate_hex_lower_copy": measure(lower_copy, **kwargs),
        "validate_hex_full_calls": measure(validate_hex_calls, **kwargs),
        "real_fromhex_decode": measure(real_decode, **kwargs),
        "join_concatenation": measure(join_only, **kwargs),
        "caller_hex_reencode": measure(hex_encode, **kwargs),
        "bare_iteration_floor": measure(loop_floor, **kwargs),
        "floor_single_decode_bytes_out": measure(floor_single_decode_bytes_out, **kwargs),
        "floor_bytes_in_bytes_out": measure(floor_bytes_in_bytes_out, **kwargs),
    }


# --------------------------------------------------------------------------
# anchors
# --------------------------------------------------------------------------


@dataclass
class PrimitiveResult:
    name: str
    group: str
    timing: Timing
    note: str = ""


def measure_primitives(
    scenario: Scenario, *, reps: int, min_batch_seconds: float
) -> list[PrimitiveResult]:
    """Time every per-accepted-share primitive on the accept path in isolation.

    The findings comment reports a 12.4 us "primitives, min-of-7" floor but
    does not enumerate its primitive set, so this is a reconstruction from the
    code path rather than a replay of the original list. Two groups:
    ``assemble`` covers the primitives ``assemble_submission`` composes, and
    ``accept_path`` covers the per-accepted-share work around it that the
    coordinator does for every share (wire decode, request parse/validate,
    duplicate reservation, ack encode).
    """

    job = scenario.job
    inputs = submission_inputs(job)
    header: bytes = inputs["header"]
    coinbase_without_witness: bytes = inputs["coinbase_without_witness"]
    full_coinbase_hex: str = inputs["full_coinbase_hex"]
    extranonce2_hex: str = inputs["extranonce2_hex"]
    version_hex: str = inputs["version_hex"]
    merkle_branch = list(job.merkle_branch)
    transaction_hexes = job.transaction_hexes

    validate_hex = stratum_codec.validate_hex
    apply_version_bits = stratum_codec.apply_version_bits
    assemble_coinbase = stratum_codec.assemble_coinbase
    merkle_from_branch = stratum_codec.compute_merkle_root_from_branch_hex
    serialize_header = stratum_codec.serialize_header_from_stratum_fields
    strip_witness = direct_stratum.strip_witness_transaction
    header_hash_int = stratum_codec.header_hash_int
    header_hash_hex = stratum_codec.header_hash_hex
    serialize_block = direct_stratum.serialize_block
    submission_cls = direct_stratum.DirectQbitSubmission

    coinbase_bytes = assemble_coinbase(
        job.coinb1, job.extranonce1_hex, extranonce2_hex, job.coinb2
    )
    merkle_root = merkle_from_branch(coinbase_bytes, merkle_branch)
    block_hex = serialize_block(header, full_coinbase_hex, transaction_hexes).hex()
    header_hex = header.hex()

    username = "qbit1qperfworker.rig0"
    submit_line = json.dumps(
        {
            "id": 4171,
            "method": "mining.submit",
            "params": [username, job.job_id, extranonce2_hex, NTIME_HEX, NONCE_HEX],
        }
    )
    submit_params: list[object] = [
        username,
        job.job_id,
        extranonce2_hex,
        NTIME_HEX,
        NONCE_HEX,
    ]
    request = parse_submit_request(submit_params)
    ack = {"id": 4171, "result": True, "error": None}
    recent_shares = RecentShareIndex(capacity=50_000)
    duplicate_counter = [0]

    def reserve_fresh() -> None:
        # A fresh key every call: reserve() on a repeated key short-circuits,
        # so replaying one key would measure the duplicate path, not the
        # accepted-share path.
        duplicate_counter[0] += 1
        recent_shares.reserve((username, f"{header_hex}{duplicate_counter[0]}"))

    kwargs = {"reps": reps, "min_batch_seconds": min_batch_seconds}
    specs: list[tuple[str, str, Callable[[], Any], str]] = [
        (
            "validate_hex(extranonce2)",
            "assemble",
            lambda: validate_hex(extranonce2_hex, field_name="extranonce2"),
            "",
        ),
        (
            "apply_version_bits",
            "assemble",
            lambda: apply_version_bits(job.version, None, 0),
            "no version rolling on this share",
        ),
        (
            "list(job.merkle_branch)",
            "assemble",
            lambda: list(job.merkle_branch),
            f"{len(merkle_branch)} branch levels",
        ),
        (
            "assemble_coinbase",
            "assemble",
            lambda: assemble_coinbase(
                job.coinb1, job.extranonce1_hex, extranonce2_hex, job.coinb2
            ),
            "",
        ),
        (
            "compute_merkle_root_from_branch_hex",
            "assemble",
            lambda: merkle_from_branch(coinbase_bytes, merkle_branch),
            f"{len(merkle_branch)} double_sha256 fold levels",
        ),
        (
            "serialize_header_from_stratum_fields",
            "assemble",
            lambda: serialize_header(
                version_hex=version_hex,
                prevhash_hex=job.prevhash,
                merkle_root_serialized=merkle_root,
                ntime_hex=NTIME_HEX,
                nbits_hex=job.nbits,
                nonce_hex=NONCE_HEX,
            ),
            "",
        ),
        (
            "full_coinbase_hex concat",
            "assemble",
            lambda: job.full_coinbase_prefix
            + job.extranonce1_hex
            + extranonce2_hex
            + job.full_coinbase_suffix,
            "",
        ),
        (
            "strip_witness_transaction(coinbase) + compare",
            "assemble",
            lambda: strip_witness(full_coinbase_hex) != coinbase_without_witness,
            "",
        ),
        ("header_hash_int", "assemble", lambda: header_hash_int(header), ""),
        ("header_hash_hex", "assemble", lambda: header_hash_hex(header), ""),
        (
            "coinbase_without_witness.hex()",
            "assemble",
            coinbase_without_witness.hex,
            "",
        ),
        ("header.hex()", "assemble", header.hex, ""),
        (
            "serialize_block(...).hex()",
            "assemble",
            lambda: serialize_block(header, full_coinbase_hex, transaction_hexes).hex(),
            f"{scenario.transaction_count} tx, {scenario.block_bytes} block bytes",
        ),
        (
            "DirectQbitSubmission(...)",
            "assemble",
            lambda: submission_cls(
                coinbase_tx_hex=full_coinbase_hex,
                coinbase_txid_preimage_hex=header_hex,
                header_hex=header_hex,
                block_hex=block_hex,
                block_hash_hex=header_hex,
                block_hash_int=1,
                share_pass=True,
                block_pass=False,
                applied_version_hex=version_hex,
            ),
            "",
        ),
        (
            "json.loads(mining.submit line)",
            "accept_path",
            lambda: json.loads(submit_line),
            "",
        ),
        (
            "parse_submit_request",
            "accept_path",
            lambda: parse_submit_request(submit_params),
            "",
        ),
        (
            "validate_submit_request",
            "accept_path",
            lambda: validate_submit_request(
                request,
                authorized_username=username,
                pool_open=True,
                extranonce2_size=EXTRANONCE2_SIZE,
            ),
            "",
        ),
        (
            "RecentShareIndex.reserve",
            "accept_path",
            reserve_fresh,
            "fresh key per call, includes its own lock",
        ),
        ("json.dumps(submit ack)", "accept_path", lambda: json.dumps(ack), ""),
    ]

    results = [
        PrimitiveResult(name=name, group=group, timing=measure(fn, **kwargs), note=note)
        for name, group, fn, note in specs
    ]
    results.append(
        PrimitiveResult(
            name="harness floor (lambda: None)",
            group="harness",
            timing=measure(lambda: None, **kwargs),
            note="per-call cost of the measurement loop itself, included in every row above",
        )
    )
    return results


# --------------------------------------------------------------------------
# environment
# --------------------------------------------------------------------------


def describe_environment() -> dict[str, Any]:
    try:
        load = os.getloadavg()
    except (OSError, AttributeError):
        load = None
    gil_disabled = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    try:
        gil_enabled: bool | None = sys._is_gil_enabled()  # type: ignore[attr-defined]
    except AttributeError:
        gil_enabled = None
    cpu_brand = platform.processor()
    if sys.platform == "darwin":
        # platform.processor() is just "arm" on Apple silicon; the sysctl name
        # is the informative one and needs no third-party package.
        try:
            import subprocess

            brand = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
            if brand:
                cpu_brand = brand
        except Exception:  # pragma: no cover - informational only
            pass
    return {
        "cpu": cpu_brand,
        "machine": platform.machine(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "gil_disabled_build": gil_disabled,
        "gil_enabled_at_runtime": gil_enabled,
        "loadavg_1_5_15": load,
        "clock_rationale": CLOCK_RATIONALE,
        "clocks": {
            name: {
                "implementation": info.implementation,
                "resolution_s": info.resolution,
            }
            for name, info in (
                (name, time.get_clock_info(name))
                for name in ("thread_time", "process_time", "perf_counter")
            )
        },
    }


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------


def _fmt(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def render_report(results: dict[str, Any]) -> str:
    env = results["environment"]
    out: list[str] = []
    add = out.append

    add("# serialize_block per-share CPU cost")
    add("")
    add("## Machine")
    add("")
    add(f"- CPU: {env['cpu']} ({env['cpu_count']} logical cores)")
    add(f"- Platform: {env['platform']}")
    add(f"- Python: {env['python_version'].splitlines()[0]} ({env['python_implementation']})")
    add(
        f"- GIL: build Py_GIL_DISABLED={env['gil_disabled_build']}, "
        f"runtime enabled={env['gil_enabled_at_runtime']}"
    )
    load = env["loadavg_1_5_15"]
    if load:
        add(f"- Load average at start (1/5/15 min): {load[0]:.2f} / {load[1]:.2f} / {load[2]:.2f}")
    add(f"- Load average at end: {results['loadavg_end']}")
    add(f"- Clock: {env['clock_rationale']}")
    for name, info in env["clocks"].items():
        add(f"  - `{name}`: {info['implementation']}, resolution {info['resolution_s']:.3g} s")
    add("")

    anchors = results["anchors"]
    add("## Anchors against the issue #143 findings comment")
    add("")
    add("| anchor | published | this machine | ratio (mine/published) |")
    add("|---|---|---|---|")
    add(
        f"| primitives floor, per accepted share | {PUBLISHED_PRIMITIVES_FLOOR_US} us | "
        f"{_fmt(anchors['primitives_floor_us'])} us | "
        f"{_fmt(anchors['primitives_floor_ratio'], 3)} |"
    )
    add(
        f"| 0-tx-template composite `assemble_submission` | {PUBLISHED_ZERO_TX_COMPOSITE_US} us | "
        f"{_fmt(anchors['zero_tx_composite_us'])} us | "
        f"{_fmt(anchors['zero_tx_composite_ratio'], 3)} |"
    )
    add(
        f"| composite / primitives inflation ratio | "
        f"{_fmt(PUBLISHED_FULL_PATH_ESTIMATE_US / PUBLISHED_PRIMITIVES_FLOOR_US, 3)} (implied "
        f"by 15.7/12.4) | {_fmt(anchors['composite_over_assemble_primitives'], 3)} | "
        f"{_fmt(anchors['composite_over_assemble_primitives'] / (PUBLISHED_FULL_PATH_ESTIMATE_US / PUBLISHED_PRIMITIVES_FLOOR_US), 3)} |"
    )
    add("")
    add("Primitive breakdown at the 0-transaction baseline (min-of-7, interpreter CPU):")
    add("")
    add("| primitive | group | thread CPU us | note |")
    add("|---|---|---|---|")
    for row in anchors["primitives"]:
        add(
            f"| `{row['name']}` | {row['group']} | {_fmt(row['timing']['thread_cpu_us'], 3)} | "
            f"{row['note']} |"
        )
    add(
        f"| **sum (assemble group)** | assemble | "
        f"**{_fmt(anchors['assemble_primitives_us'], 3)}** | |"
    )
    add(
        f"| **sum (accept-path group)** | accept_path | "
        f"**{_fmt(anchors['accept_path_primitives_us'], 3)}** | |"
    )
    add(
        f"| **primitives floor (both groups)** | | "
        f"**{_fmt(anchors['primitives_floor_us'], 3)}** | |"
    )
    add("")

    add("## Axis 1 - total block bytes")
    add("")
    add(
        "| block bytes | tx count | tx size (B) | merkle levels | composite "
        "`assemble_submission` us/share | `serialize_block(...).hex()` us/share | "
        "term % of composite | composite - term us | composite wall us |"
    )
    add("|---|---|---|---|---|---|---|---|---|")
    for row in results["axis1"]:
        add(
            f"| {row['block_bytes']:,} | {row['transaction_count']:,} | "
            f"{row['nominal_tx_bytes']} | {row['merkle_branch_levels']} | "
            f"{_fmt(row['composite_us'])} | {_fmt(row['serialize_term_us'])} | "
            f"{_fmt(row['term_percent'], 1)}% | {_fmt(row['composite_less_term_us'])} | "
            f"{_fmt(row['composite_wall_us'])} |"
        )
    add("")
    add("Term split: `serialize_block` (bytes out) vs the caller's `.hex()` re-encode.")
    add("")
    add("| block bytes | `serialize_block` us | `.hex()` re-encode us | sum us | term measured us |")
    add("|---|---|---|---|---|")
    for row in results["axis1"]:
        add(
            f"| {row['block_bytes']:,} | {_fmt(row['serialize_block_only_us'])} | "
            f"{_fmt(row['hex_encode_only_us'])} | "
            f"{_fmt(row['serialize_block_only_us'] + row['hex_encode_only_us'])} | "
            f"{_fmt(row['serialize_term_us'])} |"
        )
    add("")

    add("## Axis 2 - transaction count at fixed total block bytes")
    add("")
    add(f"Total block bytes held at ~{AXIS2_TOTAL_BLOCK_BYTES:,}.")
    add("")
    add(
        "| tx count | block bytes | tx size (B) | merkle levels | composite us/share | "
        "term us/share | term % of composite |"
    )
    add("|---|---|---|---|---|---|---|")
    for row in results["axis2"]:
        add(
            f"| {row['transaction_count']:,} | {row['block_bytes']:,} | "
            f"{row['nominal_tx_bytes']} | {row['merkle_branch_levels']} | "
            f"{_fmt(row['composite_us'])} | {_fmt(row['serialize_term_us'])} | "
            f"{_fmt(row['term_percent'], 1)}% |"
        )
    add("")
    fit = results["axis2_fit"]
    add(
        f"Least-squares fit of the term against transaction count at fixed bytes: "
        f"**{_fmt(fit['per_tx_us'], 4)} us/tx** plus a "
        f"{_fmt(fit['intercept_us'])} us fixed term (R^2 = {_fmt(fit['r_squared'], 4)}). "
        f"Endpoint slope over {fit['endpoint_span'][0]}->{fit['endpoint_span'][1]} tx: "
        f"**{_fmt(fit['endpoint_per_tx_us'], 4)} us/tx**."
    )
    add("")

    add("## Decomposition of the term")
    add("")
    for block_label, parts in results["decomposition"].items():
        add(f"### {block_label}")
        add("")
        add("| part | thread CPU us | % of term |")
        add("|---|---|---|")
        term = parts["term_us"]
        for name, value in parts["parts_us"].items():
            share = 100.0 * value / term if term else float("nan")
            add(f"| {name} | {_fmt(value, 3)} | {_fmt(share, 1)}% |")
        add(f"| **measured term** | **{_fmt(term, 3)}** | 100% |")
        add(f"| residual (term - accounted parts) | {_fmt(parts['residual_us'], 3)} | "
            f"{_fmt(100.0 * parts['residual_us'] / term if term else float('nan'), 1)}% |")
        add("")
        add("Memoranda for the same material (not part of the sum above):")
        add("")
        add("| memo | thread CPU us | % of term |")
        add("|---|---|---|")
        for label, value in (
            (
                "`validate_hex` full calls incl. per-tx f-string field name",
                parts["validate_hex_full_calls_us"],
            ),
            (
                "counterfactual A: one decode, bytes out, no re-encode",
                parts["floor_single_decode_bytes_out_us"],
            ),
            (
                "counterfactual B: transactions already bytes, bytes out",
                parts["floor_bytes_in_bytes_out_us"],
            ),
            ("bare iteration over the same list", parts["bare_iteration_floor_us"]),
        ):
            add(
                f"| {label} | {_fmt(value, 3)} | "
                f"{_fmt(100.0 * value / term if term else float('nan'), 1)}% |"
            )
        add("")

    add("## Capacity")
    add("")
    add(
        "Single-core capacity restated at each Axis 1 point. `mine` is this machine's "
        "measured accept-path budget; `published-scale` rescales the findings comment's "
        "64k-81k band by the same relative collapse, so it is directly comparable to the "
        "number already in the report."
    )
    add("")
    add(
        "| block bytes | accept-path us/share (mine) | shares/s (mine) | "
        "published-scale shares/s | % of one core at ~273 shares/s |"
    )
    add("|---|---|---|---|---|")
    for row in results["capacity"]:
        add(
            f"| {row['block_bytes']:,} | {_fmt(row['accept_path_us'])} | "
            f"{row['shares_per_second_mine']:,.0f} | "
            f"{row['published_scale_low']:,.0f}-{row['published_scale_high']:,.0f} | "
            f"{_fmt(row['design_point_core_percent'], 2)}% |"
        )
    add("")
    return "\n".join(out)


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------


def _linear_fit(xs: Sequence[float], ys: Sequence[float]) -> tuple[float, float, float]:
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    slope = sxy / sxx if sxx else float("nan")
    intercept = mean_y - slope * mean_x
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    r_squared = 1.0 - ss_res / ss_tot if ss_tot else float("nan")
    return slope, intercept, r_squared


def run(args: argparse.Namespace) -> dict[str, Any]:
    environment = describe_environment()
    reps = args.reps
    min_batch_seconds = args.min_batch_seconds
    kwargs = {"reps": reps, "min_batch_seconds": min_batch_seconds}

    baseline = build_scenario(
        label="0tx",
        transaction_count=0,
        target_block_bytes=None,
        construction="repo baseline: job_build_benchmark.base_template() with transactions=[]",
    )

    primitives = measure_primitives(baseline, **kwargs)
    assemble_primitives_us = sum(
        p.timing.thread_cpu_us for p in primitives if p.group == "assemble"
    )
    accept_path_primitives_us = sum(
        p.timing.thread_cpu_us for p in primitives if p.group == "accept_path"
    )
    primitives_floor_us = assemble_primitives_us + accept_path_primitives_us

    axis1_scenarios: list[Scenario] = [baseline]
    for target in AXIS1_TARGET_BLOCK_BYTES:
        if target == 0:
            continue
        payload_estimate = target - HEADER_BYTES - 3 - baseline.coinbase_bytes
        count = max(1, round(payload_estimate / AXIS1_NOMINAL_TX_BYTES))
        axis1_scenarios.append(
            build_scenario(
                label=f"{target}B",
                transaction_count=count,
                target_block_bytes=target,
                construction=(
                    f"{count} synthetic 1-in/1-out non-witness transactions of "
                    f"~{AXIS1_NOMINAL_TX_BYTES} B, sized to hit {target} total block bytes exactly"
                ),
            )
        )

    axis1_timings = [
        time_scenario(scenario, **kwargs) for scenario in axis1_scenarios
    ]
    baseline_timing = axis1_timings[0]

    axis2_scenarios = [
        build_scenario(
            label=f"1MB-{count}tx",
            transaction_count=count,
            target_block_bytes=AXIS2_TOTAL_BLOCK_BYTES,
            construction=(
                f"{count} equal-sized synthetic transactions filling "
                f"{AXIS2_TOTAL_BLOCK_BYTES} total block bytes"
            ),
        )
        for count in AXIS2_TX_COUNTS
    ]
    axis2_timings = [time_scenario(scenario, **kwargs) for scenario in axis2_scenarios]

    slope, intercept, r_squared = _linear_fit(
        [float(t.scenario.transaction_count) for t in axis2_timings],
        [t.serialize_term.thread_cpu_us for t in axis2_timings],
    )
    first, last = axis2_timings[0], axis2_timings[-1]
    endpoint_slope = (
        last.serialize_term.thread_cpu_us - first.serialize_term.thread_cpu_us
    ) / (last.scenario.transaction_count - first.scenario.transaction_count)

    decomposition: dict[str, Any] = {}
    for timing in axis1_timings:
        scenario = timing.scenario
        if scenario.transaction_count and scenario.block_bytes not in (
            1_000_000,
            2_000_000,
        ):
            continue
        parts = decompose_term(scenario, **kwargs)
        accounted = {
            "validate_hex: bytes.fromhex probe (discarded)": parts[
                "validate_hex_fromhex_probe"
            ].thread_cpu_us,
            "validate_hex: value.lower() copy": parts["validate_hex_lower_copy"].thread_cpu_us,
            "real bytes.fromhex decode + list build": parts["real_fromhex_decode"].thread_cpu_us,
            "b''.join concatenation": parts["join_concatenation"].thread_cpu_us,
            "caller .hex() re-encode": parts["caller_hex_reencode"].thread_cpu_us,
        }
        term_us = timing.serialize_term.thread_cpu_us
        decomposition[
            f"{scenario.block_bytes:,} block bytes / {scenario.transaction_count:,} tx"
        ] = {
            "term_us": term_us,
            "parts_us": accounted,
            "residual_us": term_us - sum(accounted.values()),
            "validate_hex_full_calls_us": parts["validate_hex_full_calls"].thread_cpu_us,
            "bare_iteration_floor_us": parts["bare_iteration_floor"].thread_cpu_us,
            "floor_single_decode_bytes_out_us": parts[
                "floor_single_decode_bytes_out"
            ].thread_cpu_us,
            "floor_bytes_in_bytes_out_us": parts["floor_bytes_in_bytes_out"].thread_cpu_us,
            "raw": {name: timing_.as_json() for name, timing_ in parts.items()},
        }

    # Accept-path budget: the composite at this template size plus the
    # per-share accept-path primitives measured around it. The accept-path
    # primitives are inflated by the same composite/primitives ratio the
    # findings comment used to turn its 12.4 us floor into 15.7 us.
    ratio = (
        baseline_timing.composite.thread_cpu_us / assemble_primitives_us
        if assemble_primitives_us
        else float("nan")
    )
    baseline_accept_path_us = (
        baseline_timing.composite.thread_cpu_us + accept_path_primitives_us * ratio
    )
    capacity: list[dict[str, Any]] = []
    for timing in axis1_timings:
        accept_path_us = timing.composite.thread_cpu_us + accept_path_primitives_us * ratio
        collapse = baseline_accept_path_us / accept_path_us if accept_path_us else float("nan")
        capacity.append(
            {
                "block_bytes": timing.scenario.block_bytes,
                "transaction_count": timing.scenario.transaction_count,
                "accept_path_us": accept_path_us,
                "shares_per_second_mine": 1e6 / accept_path_us,
                "relative_to_baseline": collapse,
                "published_scale_low": PUBLISHED_CAPACITY_LOW_PER_S * collapse,
                "published_scale_high": PUBLISHED_CAPACITY_HIGH_PER_S * collapse,
                "design_point_core_percent": (
                    100.0 * DESIGN_POINT_SHARES_PER_S * accept_path_us / 1e6
                ),
            }
        )

    def scenario_json(timing: ScenarioTiming) -> dict[str, Any]:
        scenario = timing.scenario
        nominal = (
            scenario.transaction_bytes // scenario.transaction_count
            if scenario.transaction_count
            else 0
        )
        return {
            "label": scenario.label,
            "block_bytes": scenario.block_bytes,
            "transaction_count": scenario.transaction_count,
            "transaction_bytes": scenario.transaction_bytes,
            "coinbase_bytes": scenario.coinbase_bytes,
            "nominal_tx_bytes": nominal,
            "merkle_branch_levels": scenario.merkle_branch_levels,
            "construction": scenario.construction,
            "composite_us": timing.composite.thread_cpu_us,
            "composite_wall_us": timing.composite.wall_us,
            "composite_process_us": timing.composite.process_cpu_us,
            "serialize_term_us": timing.serialize_term.thread_cpu_us,
            "serialize_term_wall_us": timing.serialize_term.wall_us,
            "serialize_block_only_us": timing.serialize_block_only.thread_cpu_us,
            "hex_encode_only_us": timing.hex_encode_only.thread_cpu_us,
            "term_percent": timing.term_share_percent,
            "composite_less_term_us": timing.composite_less_term_us,
            "raw": {
                "composite": timing.composite.as_json(),
                "serialize_term": timing.serialize_term.as_json(),
                "serialize_block_only": timing.serialize_block_only.as_json(),
                "hex_encode_only": timing.hex_encode_only.as_json(),
            },
        }

    try:
        loadavg_end = "%.2f / %.2f / %.2f" % os.getloadavg()
    except (OSError, AttributeError):
        loadavg_end = "unavailable"

    return {
        "environment": environment,
        "loadavg_end": loadavg_end,
        "methodology": {
            "repetitions": reps,
            "statistic": "min-of-N over auto-sized batches, reported per call",
            "min_batch_seconds": min_batch_seconds,
            "clock": "time.thread_time_ns (headline); process_time_ns and perf_counter_ns alongside",
            "threads": 1,
            "job_construction": (
                "route 1: real DirectQbitStratumJob via "
                "direct_stratum.make_job_from_builder_manifest, so "
                "merkle_branch_for_coinbase runs strip_witness_transaction over every "
                "transaction and the job carries a realistic merkle branch"
            ),
            "consensus_ceiling_bytes": MAX_BLOCK_BYTES,
        },
        "anchors": {
            "published_primitives_floor_us": PUBLISHED_PRIMITIVES_FLOOR_US,
            "published_zero_tx_composite_us": PUBLISHED_ZERO_TX_COMPOSITE_US,
            "primitives_floor_us": primitives_floor_us,
            "primitives_floor_ratio": primitives_floor_us / PUBLISHED_PRIMITIVES_FLOOR_US,
            "assemble_primitives_us": assemble_primitives_us,
            "accept_path_primitives_us": accept_path_primitives_us,
            "zero_tx_composite_us": baseline_timing.composite.thread_cpu_us,
            "zero_tx_composite_ratio": (
                baseline_timing.composite.thread_cpu_us / PUBLISHED_ZERO_TX_COMPOSITE_US
            ),
            "composite_over_assemble_primitives": ratio,
            "primitives": [
                {
                    "name": p.name,
                    "group": p.group,
                    "note": p.note,
                    "timing": p.timing.as_json(),
                }
                for p in primitives
            ],
        },
        "axis1": [scenario_json(t) for t in axis1_timings],
        "axis2": [scenario_json(t) for t in axis2_timings],
        "axis2_fit": {
            "per_tx_us": slope,
            "intercept_us": intercept,
            "r_squared": r_squared,
            "endpoint_per_tx_us": endpoint_slope,
            "endpoint_span": [first.scenario.transaction_count, last.scenario.transaction_count],
            "fixed_total_block_bytes": AXIS2_TOTAL_BLOCK_BYTES,
        },
        "decomposition": decomposition,
        "capacity": capacity,
        "design_point_shares_per_second": DESIGN_POINT_SHARES_PER_S,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", action="store_true", help="emit the full result set as JSON")
    parser.add_argument("--reps", type=int, default=7, help="repetitions per timing (default 7)")
    parser.add_argument(
        "--min-batch-seconds",
        type=float,
        default=0.05,
        help="target duration of one timing repetition (default 0.05)",
    )
    args = parser.parse_args(argv)

    results = run(args)
    if args.json:
        json.dump(results, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_report(results))
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

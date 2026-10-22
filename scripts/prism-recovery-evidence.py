#!/usr/bin/env python3
"""Summarize prism-recovery-evidence.sql JSONL without loading share history.

Usage: python3 scripts/prism-recovery-evidence.py [--jobs N] source.rows.jsonl
No database access or mutation. The carry head is byte-compatible with 2.x's
_carry_forward_audit_head_locked (v2.0.2, 504846cc), including ASCII escaping.

Carry integrity doesn't depend on which validator the database ran (#708):
- Rows landed before migration 011, on unmarked blocks, are re-checked here as
  one running chain per payout program, as 2.x keeps their balances.
- A finding the database reports on such a row must be one its per-label rule
  (2.x's, and 011's) also finds there; the per-program chain clears it.
- Findings on marked (as-issued) blocks come from the database's manifest
  rule, which this export can't repeat, and fail as before.
- The summary keeps the database's report, less the findings cleared, so a 2.x
  source and its migrated copy summarize alike.

While migration 2's share-hash backfill is pending at fence 2 (`migrate
--defer-share-hashes`), the export holds no share_hashes rows and one
share_hashes_deferred record, the backfill's cursor, read last, after the
integrity report. The summary marks that kind deferred with it, in
records.share_hashes.deferred, and a note says so: compare share_hashes only
once 2 is recorded.

Parsing is parallel by default (--jobs, the CPUs this process may use, at
most 8): worker processes parse and canonicalize newline-aligned ranges of
the file, and this process applies every check to their results in file
order, so the summary, its notes and every refusal are byte for byte the
serial summarizer's (--jobs 1). From a range a worker cannot read as a
text-mode read does (invalid UTF-8, a read error) on, the file is read
serially from its start, lines already checked included, so decoding fails
exactly where it would.
"""

import argparse
import codecs
import collections
import hashlib
import io
import itertools
import json
import multiprocessing
import os
import pickle
import re
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

INTEGER = re.compile(r"-?[0-9]+")
BALANCE_FIELDS = ("prior_balance", "candidate_balance", "carry_forward_balance")
KINDS = (
    "shares", "share_sequence", "sequences", "share_hashes", "blocks", "audits", "audit_bodies", "audit_snapshots",
    "carry", "payouts", "candidates", "candidate_balances",
    "ctv_sets", "ctv_artifacts", "ctv_checkpoints", "ctv_retry_progress",
    "ctv_broadcast_attempts",
    "cpfp_packages", "cpfp_retired_funding", "deferred_shares",
    "fatal_state", "fatal_state_events", "policy_transitions", "chain_checkpoint", "cluster_config",
    "payout_revision", "ledger_clock", "active_carry",
)
# The kinds the summary reads beyond hashing and counting. Their rows reach
# the checks whole; every other kind reaches them as canonical bytes only.
ROW_KINDS = ("blocks", "active_carry", "candidates")
# A dict, as the summary's own lookup is: an unhashable kind must raise the
# same TypeError, whose wording differs between dicts and sets.
_KIND_LOOKUP = dict.fromkeys(KINDS)
DEFAULT_CHUNK_BYTES = 32 << 20
MAX_DEFAULT_JOBS = 8


def _amount(row, field):
    value = row[field]
    if isinstance(value, bool) or not isinstance(value, (int, str)) or (
            isinstance(value, str) and not INTEGER.fullmatch(value)):
        raise ValueError(f"active carry {field} is not an integer amount")
    return int(value)


def _off_chain(row, prior, gross, onchain):
    """Whether a row's stored balances differ from a chain at `prior`."""
    expected = (prior, prior + gross, prior + gross - onchain)
    return any(_amount(row, f"{field}_sats") != value
               for field, value in zip(BALANCE_FIELDS, expected))


def _canonical(row):
    return json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")


class _Summary:
    """Every check the summary makes, applied to records in file order.

    `line` takes one line as a text-mode read yields it. The parallel path
    feeds the same state from parsed records, and from runs of rows that
    need nothing but hashing and counting, which a run can do at once: no
    state those checks read changes inside a run of hashed rows.
    """

    def __init__(self):
        self.hashes = {kind: hashlib.sha256() for kind in KINDS}
        self.counts = dict.fromkeys(KINDS, 0)
        self.head = bytes(32)
        self.last_share_seq = 0
        self.accepted = 0
        self.pending = 0
        self.unfinished = 0
        self.integrity = None
        self.complete = False
        self.marked_blocks = set()
        self.carry_started = False
        self.carry_order = None
        # Running (gross - onchain) over earlier active rows, marked ones
        # included, as the SQL rules sum it: per program (025) and per label.
        self.by_program = {}
        self.by_label = {}
        self.legacy_breaks = 0
        self.label_breaks = set()
        # The pending share-hash backfill's cursor, when the export defers
        # the share_hashes kind.
        self.deferred = None

    def line(self, line):
        self.start()
        record = json.loads(line)
        kind, row = record["kind"], record["row"]
        self.record(kind, row)

    def start(self):
        """The check every line meets before it is parsed."""
        if self.complete:
            raise ValueError("records follow completion marker")

    def record(self, kind, row, canonical=None):
        """One parsed record; `canonical` is the row's canonical bytes, if known."""
        if kind == "complete":
            if row is not True or self.integrity is None:
                raise ValueError("invalid completion marker or missing integrity report")
            self.complete = True
        elif kind == "integrity":
            if self.integrity is not None:
                raise ValueError("duplicate integrity report")
            self.integrity = row
        elif kind in self.hashes:
            if self.integrity is not None:
                raise ValueError("accounting records follow integrity report")
            if canonical is None:
                canonical = _canonical(row)
            self.hashes[kind].update(canonical + b"\n")
            self.counts[kind] += 1
            if kind == "blocks":
                if self.carry_started:
                    raise ValueError("block records follow active carry records")
                if row.get("as_issued_audit_sha256") is not None:
                    self.marked_blocks.add(row["block_hash"])
            elif kind == "active_carry":
                self.carry_started = True
                self.head = hashlib.sha256(self.head + canonical).digest()
                order = (row["block_height"], row["carry_forward_seq"])
                if self.carry_order is not None and order <= self.carry_order:
                    raise ValueError("active carry records are not in chain order")
                self.carry_order = order
                program = row["p2mr_program_hex"]
                label = (row["recipient_id"], row["order_key"], program)
                gross = _amount(row, "gross_amount_sats")
                onchain = _amount(row, "onchain_amount_sats")
                program_prior = self.by_program.get(program, 0)
                label_prior = self.by_label.get(label, 0)
                if row["block_hash"] not in self.marked_blocks:
                    if _off_chain(row, program_prior, gross, onchain):
                        self.legacy_breaks += 1
                    if _off_chain(row, label_prior, gross, onchain):
                        self.label_breaks.add(row["carry_forward_seq"])
                self.by_program[program] = program_prior + gross - onchain
                self.by_label[label] = label_prior + gross - onchain
            elif kind == "shares":
                if row["share_seq"] <= self.last_share_seq:
                    raise ValueError("share sequence is not strictly increasing")
                self.last_share_seq = row["share_seq"]
                self.accepted += int(row["accepted"])
            elif kind == "candidates":
                self.pending += int(row["state"] == "pending")
                # Unknown states must not make a drained-work check pass.
                # `orphaned` (migration 015) is terminal and keeps its evidence.
                self.unfinished += int(row["state"] not in ("submitted", "abandoned", "orphaned"))
        elif kind == "share_hashes_deferred":
            # Migration 2's share-hash backfill is pending: the export holds
            # none of its partial mapping, and this is its cursor. Not an
            # accounting record: the export reads it last, after the
            # integrity report, to hold the cursor's lock as briefly as it
            # can.
            if self.deferred is not None:
                raise ValueError("duplicate share_hashes deferral")
            if not isinstance(row, dict):
                raise ValueError("share_hashes deferral is not an object")
            self.deferred = row
        else:
            raise ValueError(f"unknown evidence kind: {kind}")

    def rows(self, kind, count, blob):
        """`count` rows of a kind that is only hashed and counted, whose
        canonical bytes, each with its newline, are `blob`."""
        self.start()
        if self.integrity is not None:
            raise ValueError("accounting records follow integrity report")
        self.hashes[kind].update(blob)
        self.counts[kind] += count

    def shares(self, count, blob, first, last, accepted):
        """A run of share rows whose integer sequences strictly increase
        within it, from `first` to `last`, with `accepted` integer flags."""
        self.rows("shares", count, blob)
        if first <= self.last_share_seq:
            raise ValueError("share sequence is not strictly increasing")
        self.last_share_seq = last
        self.accepted += accepted

    def report(self, notes=None):
        if not self.complete:
            raise ValueError("incomplete export; psql must finish successfully")
        integrity = self.integrity
        if not isinstance(integrity, dict):
            raise ValueError("carry-forward integrity report is not an object")
        for field in ("mismatch_count", "current_drift_count", "checked_active_rows"):
            value = integrity.get(field)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"carry-forward integrity report lacks {field}")
        if integrity["current_drift_count"] != 0:
            raise ValueError("carry-forward integrity failure: current_drift_count")
        if integrity["checked_active_rows"] != self.counts["active_carry"]:
            raise ValueError("active carry count differs from integrity report")
        mismatches = integrity.get("mismatches", [])
        if not isinstance(mismatches, list) or len(mismatches) != integrity["mismatch_count"]:
            raise ValueError("carry-forward integrity report does not list every mismatch")
        if not all(isinstance(finding, dict) for finding in mismatches):
            raise ValueError("carry-forward integrity report lists a mismatch that is not an object")
        # Block, manifest and payout findings (no carry row) are as-issued only.
        as_issued = [finding for finding in mismatches
                     if finding.get("carry_forward_seq") is None
                     or finding.get("block_hash") in self.marked_blocks]
        if as_issued:
            raise ValueError(f"carry-forward integrity failure: {len(as_issued)} as-issued finding(s)")
        if self.legacy_breaks:
            raise ValueError(
                f"carry-forward integrity failure: {self.legacy_breaks} legacy row(s) break their payout program's chain")
        # What is left are legacy findings. Each must be the per-label rule's
        # (2.x's, and 011's), on a row whose program chain holds; any other is
        # a rule this summary does not know, and fails.
        unexplained = [finding for finding in mismatches
                       if finding["carry_forward_seq"] not in self.label_breaks]
        if unexplained:
            raise ValueError(
                f"carry-forward integrity failure: {len(unexplained)} legacy finding(s) no chain rule explains")
        if notes is not None and mismatches:
            notes.append(
                f"the database's per-label legacy carry rule reported {len(mismatches)} finding(s) that each "
                "payout program's chain clears: a program paid under more than one label (#708)")
        records = {kind: {"count": self.counts[kind], "sha256": self.hashes[kind].hexdigest()}
                   for kind in KINDS}
        if self.deferred is not None:
            if self.counts["share_hashes"]:
                raise ValueError("share_hashes rows were exported beside their deferral")
            records["share_hashes"]["deferred"] = self.deferred
            if notes is not None:
                notes.append(
                    "share_hashes deferred: migration 2's share-hash backfill is pending, so the export holds "
                    f"none of its mapping; the legacy shares from share_seq {self.deferred.get('next_seq')} up to "
                    f"{self.deferred.get('end_seq')} are not all mapped. Compare share_hashes once 2 is recorded")
        return {
            "schema": "qbit.prism.recovery-evidence.v1",
            "records": records,
            "accepted_shares": self.accepted,
            "last_share_seq": self.last_share_seq,
            "pending_candidates": self.pending,
            "unfinished_candidates": self.unfinished,
            "audit_chain_version": "qbit.prism.carry-forward-active-delta-chain.v1",
            "audit_head_sha256": self.head.hex(),
            # The database's report, less the findings cleared above: a clean
            # report is unchanged.
            "carry_forward_integrity": dict(integrity, mismatch_count=0, mismatches=[]),
        }


def summarize(lines, notes=None, summary=None):
    """Summarize `lines`, continuing `summary` if it already checked earlier ones."""
    if summary is None:
        summary = _Summary()
    for line in lines:
        summary.line(line)
    return summary.report(notes)


def _text_lines(text):
    """`text`'s lines as a text-mode read yields them, newline included."""
    start = 0
    while True:
        end = text.find("\n", start)
        if end < 0:
            if start < len(text):
                yield text[start:]
            return
        yield text[start:end + 1]
        start = end + 1


def _share_fields(row):
    """What the share checks read of a row: its own fields, or the row itself
    when it is not an object, so they fail exactly as on the whole row."""
    if not isinstance(row, dict):
        return row
    return {field: row[field] for field in ("share_seq", "accepted") if field in row}


class _Items:
    """A worker's results for one range, in file order: runs of rows that are
    only hashed and counted, as their canonical bytes, and every other row
    whole. A share run also carries its first and last sequence and its
    accepted sum; its sequences strictly increase."""

    def __init__(self):
        self.items = []
        self.kind = None
        self.blob = None
        self.count = 0
        self.first = self.last = None
        self.accepted = 0

    def flush(self):
        if self.kind == "shares":
            self.items.append(("shares", self.count, self.blob, self.first, self.last, self.accepted))
        elif self.kind is not None:
            self.items.append(("rows", self.kind, self.count, self.blob))
        self.kind = None

    def _append(self, kind, canonical):
        if self.kind != kind:
            self.flush()
            self.kind, self.blob, self.count = kind, bytearray(), 0
        self.blob += canonical
        self.blob += b"\n"
        self.count += 1

    def hashed(self, kind, canonical):
        self._append(kind, canonical)

    def share(self, row, canonical):
        sequence = row["share_seq"]
        if self.kind == "shares" and sequence <= self.last:
            # Out of order: a new run, whose first sequence the summary refuses.
            self.flush()
        if self.kind != "shares":
            self.first, self.accepted = sequence, 0
        self._append("shares", canonical)
        self.last = sequence
        self.accepted += int(row["accepted"])

    def record(self, kind, row, canonical):
        self.flush()
        self.items.append(("record", kind, row, canonical))


def _fast_share(row):
    """A share row the run checks can take at once: an object with an
    integer sequence and a boolean or integer accepted flag."""
    return (type(row) is dict
            and type(row.get("share_seq")) is int
            and type(row.get("accepted")) in (bool, int))


SERIAL = [("serial",)]


def _parse_range(path, start, end):
    """Parse and canonicalize one newline-aligned byte range of the export,
    from `start` to `end`, or to the end of the file when `end` is None.

    Returns where the range ended, how many lines it holds, and the items
    `_replay` feeds to the summary in order. A range a worker cannot read
    exactly as a text-mode read would (invalid UTF-8, a read error), or
    whose rows raise something other than the summarizer's own refusals, is
    `SERIAL`: the summary reads it, and everything after it, serially.
    """
    try:
        with open(path, "rb") as source:
            source.seek(start)
            data = source.read() if end is None else source.read(end - start)
        text = data.decode("utf-8")
    except (OSError, UnicodeDecodeError):
        return None, 0, SERIAL
    end = start + len(data)
    del data
    if "\r" in text:
        # Universal newlines, as text mode reads them.
        text = text.replace("\r\n", "\n").replace("\r", "\n")
    results = _Items()
    lines = 0
    for line in _text_lines(text):
        lines += 1
        try:
            record = json.loads(line)
            kind, row = record["kind"], record["row"]
            hashed = kind in _KIND_LOOKUP
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            # Raised by the summary once every earlier row has been checked.
            try:
                pickle.dumps(error)
            except Exception:
                return end, lines, SERIAL
            results.flush()
            results.items.append(("error", error))
            return end, lines, results.items
        except Exception:
            # RecursionError, MemoryError: the serial reader raises it.
            return end, lines, SERIAL
        if not hashed:
            results.record(kind, row, None)
            continue
        try:
            canonical = _canonical(row)
        except Exception:
            # Never raised for json.loads output in practice; the serial
            # summarizer raises it at this row, so let it.
            return end, lines, SERIAL
        if kind in ROW_KINDS:
            results.record(kind, row, canonical)
        elif kind == "shares":
            if _fast_share(row):
                results.share(row, canonical)
            else:
                results.record(kind, _share_fields(row), canonical)
        else:
            results.hashed(kind, canonical)
    results.flush()
    return end, lines, results.items


def _ranges(source, chunk_bytes):
    """Byte ranges of `source` that each end just after a newline, but the
    last, which is open: its worker reads to the end of the file, so a line
    still being written is never split between two ranges."""
    start = 0
    while True:
        source.seek(start + chunk_bytes)
        if not source.readline().endswith(b"\n"):
            yield start, None
            return
        end = source.tell()
        yield start, end
        start = end


class _Serial(Exception):
    """A worker could not take its range: read it, and the rest, serially."""


class _FullySerial(Exception):
    """A refusal the serial reader might not reach: summarize from scratch."""


def _replay(summary, items):
    for item in items:
        tag = item[0]
        if tag == "rows":
            summary.rows(item[1], item[2], item[3])
        elif tag == "shares":
            summary.shares(*item[1:])
        elif tag == "record":
            summary.start()
            summary.record(item[1], item[2], item[3])
        elif tag == "error":
            summary.start()
            raise item[1]
        else:
            raise _Serial()


def _decode_lookahead():
    """How far past a line's end a text-mode read may have decoded before it
    yields the line: one read chunk (TextIOWrapper's _CHUNK_SIZE, 8 KiB in
    CPython) and the bytes of a character split across it. The window is at
    least 1 MiB whatever the interpreter: a wider one only sends a refusal
    near an invalid byte through the serial reader."""
    chunk = getattr(io.TextIOWrapper(io.BytesIO(), encoding="utf-8"), "_CHUNK_SIZE", 0)
    return max(1 << 20, 2 * chunk) + 4


def _decodes(path, start):
    """Whether every byte a serial read may decode past `start`, before it
    yields the line ending there, is valid UTF-8."""
    with open(path, "rb") as source:
        source.seek(start)
        data = source.read(_decode_lookahead())
        final = not source.read(1)
    try:
        codecs.getincrementaldecoder("utf-8")().decode(data, final)
    except UnicodeDecodeError:
        return False
    return True


def default_jobs():
    """The CPUs this process may run on (its affinity), at most 8."""
    if hasattr(os, "process_cpu_count"):
        cpus = os.process_cpu_count()
    elif hasattr(os, "sched_getaffinity"):
        cpus = len(os.sched_getaffinity(0))
    else:
        cpus = os.cpu_count()
    return max(1, min(cpus or 1, MAX_DEFAULT_JOBS))


def _workers_available():
    """Whether worker processes can run `_parse_range`: forked, and able to
    find it by name, as the pool sends it (a module loaded without a
    sys.modules entry cannot be found, and the pool would wait forever)."""
    module = sys.modules.get(_parse_range.__module__)
    return (getattr(module, "_parse_range", None) is _parse_range
            and "fork" in multiprocessing.get_all_start_methods())


def summarize_file(path, notes=None, jobs=1, chunk_bytes=DEFAULT_CHUNK_BYTES):
    """Summarize the export at `path`, parsing in `jobs` worker processes.

    The result, its notes and any refusal are the serial summarizer's. One
    job, a file that is not a regular file, or a host that cannot start the
    workers is the serial summarizer.
    """
    summary, checked = None, 0
    if jobs > 1 and os.path.isfile(path) and _workers_available():
        summary = _Summary()
        try:
            with open(path, "rb") as source:
                checked = _summarize_ranges(summary, path, source, jobs, chunk_bytes)
        except _FullySerial:
            summary, checked = None, 0
        if checked is None:
            return summary.report(notes)
    # The serial summarizer; or, from a range the workers could not take on,
    # the rest of the file. Either way the file is read as the serial
    # summarizer reads it, from the start and through the same call, so its
    # decoding, any refusal and even an uncaught error are the serial ones.
    # Lines the workers' ranges already checked are read but not checked
    # again.
    with open(path, encoding="utf-8") as source:
        lines = iter(source)
        collections.deque(itertools.islice(lines, checked), maxlen=0)
        return summarize(lines, notes, summary)


def _summarize_ranges(summary, path, source, jobs, chunk_bytes):
    """Feed `summary` from parallel workers in file order. Returns None once
    the file is summarized, or the number of lines checked before a range
    the workers could not take."""
    try:
        pool = ProcessPoolExecutor(max_workers=jobs, mp_context=multiprocessing.get_context("fork"))
    except Exception:
        # No process pool on this host (no POSIX semaphores, say).
        return 0
    try:
        pending = collections.deque()
        ranges = _ranges(source, chunk_bytes)
        checked = 0
        while True:
            while len(pending) < jobs + 2:
                bounds = next(ranges, None)
                if bounds is None:
                    break
                try:
                    pending.append(pool.submit(_parse_range, path, *bounds))
                except Exception:
                    # A worker could not be started (the host is out of
                    # processes or memory): read the rest serially.
                    return checked
            if not pending:
                return None
            try:
                end, lines, items = pending.popleft().result()
            except Exception:
                # A worker that failed outright (killed, out of memory):
                # read this range, and the rest, serially.
                return checked
            try:
                _replay(summary, items)
            except _Serial:
                return checked
            except Exception:
                # Every byte up to `end` decoded, so a serial read meets this
                # refusal too, unless it decodes an invalid byte just past it
                # first; then only a serial pass from scratch is exact.
                if not _decodes(path, end):
                    raise _FullySerial() from None
                raise
            checked += lines
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def _positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument(
        "--jobs", type=_positive, default=default_jobs(),
        help=f"worker processes that parse the export (default: the CPU count, at most {MAX_DEFAULT_JOBS}); "
             "1 summarizes serially. The summary is the same either way.")
    parser.add_argument("--chunk-bytes", type=_positive, default=DEFAULT_CHUNK_BYTES, help=argparse.SUPPRESS)
    args = parser.parse_args()
    notes = []
    try:
        report = summarize_file(args.evidence, notes, args.jobs, args.chunk_bytes)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        parser.exit(1, f"recovery evidence failed: {error}\n")
    for note in notes:
        print(f"note: {note}", file=sys.stderr)
    print(json.dumps(report, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()

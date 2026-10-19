#!/usr/bin/env python3
"""Export prism-recovery-evidence.sql's rows from parallel sessions (#712).

Usage: python3 scripts/prism-recovery-evidence-parallel.py [--jobs N] > source.rows.jsonl

Prints exactly the bytes `psql -XqAt -v ON_ERROR_STOP=1 -f
scripts/prism-recovery-evidence.sql` prints, so scripts/prism-recovery-evidence.py
summarizes them unchanged. Every session connects as that psql would: from
libpq's environment (PGSERVICE and the rest), or --dbname. None may prompt for
a password, so keep it in a password file (or PGPASSWORD).

- A coordinator session runs the script's preamble, refusals included, in
  the export transaction, and exports that transaction's snapshot.
- --jobs sessions import the snapshot, run the preamble again and export one
  part each: a share_seq range of the shares, a header-hash range of the
  share hashes (bytewise, as the script orders them), or the kinds between
  or after those two, as the script exports them.
- Range boundaries are sampled from the tables, and only balance the work:
  the first and last ranges are open, so every row is in exactly one range,
  whatever the boundaries. Each share-hash range reads the whole table, so
  there are --jobs of them, and four times as many share ranges. While
  migration 2's share-hash backfill is pending, the script exports no share
  hashes, so they are one part.
- The range parts sort under SET LOCAL work_mem (--work-mem).
- Parts wait under --work-dir and are copied to stdout in the script's order.
  The last, which ends with the completion marker the summarizer requires,
  is written only once every session has succeeded; a failure leaves no
  marker, as a failed psql does. A killed export (SIGKILL) leaves its
  .prism-recovery-evidence-* directory there; remove it.
Writers must be stopped, as for the serial export.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading

SCRIPT = Path(__file__).resolve().with_name("prism-recovery-evidence.sql")
# Where the script is cut. Each must appear exactly once, so an edit that
# rewrites one fails here rather than exporting something else.
BEGIN = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n"
SHARES = ("SELECT jsonb_build_object('kind', 'shares', 'row', to_jsonb(s) - 'origin_node')\n"
          "FROM qbit_share_ledger s ORDER BY share_seq;\n")
SHARES_ORDER = " ORDER BY share_seq;"
HASHES = "\\if :has_native_share_hashes\n"
HASHES_ORDER = ' ORDER BY header_hash COLLATE "C";'
# The 2.x mapping's header and the pattern its shares must match.
LEGACY_HEADER = "lower(right(share_id,64))"
LEGACY_MATCH = "share_id ~ '[0-9a-fA-F]{64}$'"
ENDIF = "\\endif\n"
COMMIT = "COMMIT;\n"
# Range boundaries come from a sample of about this many pages per table.
SAMPLE_PAGES = 1000
SNAPSHOT = re.compile(r"[0-9A-F]+-[0-9A-F]+-[0-9]+")
WORK_MEM = re.compile(r"[1-9][0-9]*(kB|MB|GB)")
SHARE_BOUND = re.compile(r"-?[0-9]{1,19}")
HASH_BOUND = re.compile(r"[0-9a-f]{1,64}")


class ExportError(Exception):
    """Why the export failed. What was written has no completion marker."""


def _changed(what):
    return ExportError(f"{SCRIPT.name}'s {what} changed; update {Path(__file__).name} with it")


class Script:
    """prism-recovery-evidence.sql, cut where the parallel parts begin."""

    def __init__(self, text):
        if (text.count(SHARES) != 1 or text.count(HASHES) != 1
                or text.index(HASHES) < text.index(SHARES)):
            raise _changed("shares or share_hashes export")
        self.preamble, rest = text.split(SHARES)
        self.middle, rest = rest.split(HASHES)
        hashes, endif, self.tail = rest.partition(ENDIF)
        self.hashes = HASHES + hashes + endif
        if (self.preamble.count(BEGIN) != 1 or not endif or "\\if" in hashes
                or hashes.count(HASHES_ORDER) != 2 or hashes.count(LEGACY_MATCH) != 1
                or hashes.count(f"{LEGACY_HEADER} AS header_hash") != 1
                or not self.tail.endswith(COMMIT)):
            raise _changed("transaction or share_hashes export")

    def session(self, snapshot):
        """The preamble, in a transaction that first imports `snapshot`."""
        return self.preamble.replace(BEGIN, f"{BEGIN}SET TRANSACTION SNAPSHOT '{snapshot}';\n")

    def parts(self, snapshot, share_bounds, hash_bounds, work_mem):
        """(name, psql input) of every part, in the serial export's order."""
        session = self.session(snapshot)
        sort = f"SET LOCAL work_mem = '{work_mem}';\n"
        parts = []
        share_ranges = ranges([str(bound) for bound in share_bounds])
        for index, (lower, upper) in enumerate(share_ranges, 1):
            where = predicate("share_seq", lower, upper)
            parts.append((f"shares part {index} of {len(share_ranges)} ({where})",
                          session + sort + SHARES.replace(SHARES_ORDER, f" WHERE {where}{SHARES_ORDER}")
                          + COMMIT))
        parts.append(("the kinds between shares and share_hashes", session + self.middle + COMMIT))
        hash_ranges = ranges([f"'{bound}'" for bound in hash_bounds])
        for index, (lower, upper) in enumerate(hash_ranges, 1):
            where = predicate('header_hash COLLATE "C"', lower, upper)
            # Every 2.x range would match the pattern against the whole
            # ledger, which costs more than the rest of its scan: test the
            # range first. The header is that expression, so the guard
            # admits exactly the shares whose header is in the range.
            guard = predicate(f'{LEGACY_HEADER} COLLATE "C"', lower, upper)
            hashes = self.hashes.replace(HASHES_ORDER, f" WHERE {where}{HASHES_ORDER}").replace(
                LEGACY_MATCH, f"CASE WHEN {guard} THEN {LEGACY_MATCH} END")
            parts.append((f"share_hashes part {index} of {len(hash_ranges)} ({where})",
                          session + sort + hashes + COMMIT))
        parts.append(("the kinds after share_hashes", session + self.tail))
        return parts


def ranges(bounds):
    """Consecutive ranges between ascending bounds; the outer two are open."""
    edges = [None, *bounds, None]
    return list(zip(edges, edges[1:]))


def predicate(key, lower, upper):
    """SQL for lower <= key < upper. The last range also holds NULL, which
    ORDER BY puts last, so the ranges cover every row in order."""
    if lower is None:
        return "true" if upper is None else f"{key} < {upper}"
    if upper is None:
        return f"({key} >= {lower} OR {key} IS NULL)"
    return f"{key} >= {lower} AND {key} < {upper}"


def sample(relation, key, parts, where=""):
    """SQL for the boundaries splitting a sample of `relation` into `parts`."""
    if parts < 2:
        return "SELECT NULL;\n"
    fractions = ",".join(repr(index / parts) for index in range(1, parts))
    # A partitioned table's pages are its partitions'.
    percent = (f"(SELECT least(100, 100.0 * {SAMPLE_PAGES} * current_setting('block_size')::int"
               f" / greatest(1, sum(pg_relation_size(oid)))) FROM pg_class WHERE oid = '{relation}'::regclass"
               f" OR oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = '{relation}'::regclass))")
    return (f"SELECT percentile_disc(ARRAY[{fractions}]::float8[]) WITHIN GROUP (ORDER BY {key})"
            f" FROM {relation} TABLESAMPLE SYSTEM ({percent}) REPEATABLE (712){where};\n")


def bounds(line, pattern):
    """The sampled boundaries in the array psql printed."""
    line = line.strip()
    if not line:
        return []
    if not (line.startswith("{") and line.endswith("}")):
        raise ExportError(f"unexpected boundary sample {line!r}")
    # Anything else (a NULL, a quoted value) is just not a boundary.
    return [value for value in line[1:-1].split(",") if pattern.fullmatch(value)]


class Sessions:
    """The psql processes running, so that a failure stops every one."""

    def __init__(self, psql):
        self.psql = psql
        self.lock = threading.Lock()
        self.running = set()
        self.stopped = False

    def start(self, *args, stdin):
        """A psql session whose rows the driver reads from its stdout."""
        with self.lock:
            if self.stopped:
                raise ExportError("stopped")
            try:
                process = subprocess.Popen([*self.psql, *args], stdin=stdin, stdout=subprocess.PIPE)
            except OSError as error:
                raise ExportError(f"cannot run {self.psql[0]}: {error}") from error
            self.running.add(process)
            return process

    def finish(self, process):
        with self.lock:
            self.running.discard(process)

    def stop(self):
        with self.lock:
            self.stopped = True
            processes = list(self.running)
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            process.wait()


class Coordinator:
    """The session whose export transaction the parts share."""

    def __init__(self, sessions, script):
        self.sessions = sessions
        self.process = sessions.start(stdin=subprocess.PIPE)
        # Idle while the parts run: a server's idle-in-transaction limit must
        # not end the snapshot before every part has imported it.
        self.pending = script.preamble + "SET LOCAL idle_in_transaction_session_timeout = 0;\n"

    def ask(self, sql):
        """The one row `sql` prints, run after anything pending."""
        try:
            self.process.stdin.write((self.pending + sql).encode())
            self.process.stdin.flush()
            line = self.process.stdout.readline()
        except BrokenPipeError:
            line = b""
        self.pending = ""
        if not line:
            raise ExportError(f"the export transaction failed (psql exited {self.process.wait()})")
        return line.decode().rstrip("\n")

    def commit(self):
        try:
            self.process.stdin.write(COMMIT.encode())
            self.process.stdin.close()
        except BrokenPipeError:
            pass
        status = self.process.wait()
        self.sessions.finish(self.process)
        if status != 0:
            raise ExportError(f"the export transaction failed (psql exited {status})")


def run_part(sessions, name, sql, path):
    script = path.with_suffix(".sql")
    script.write_text(sql, encoding="utf-8")
    # psql stops fetching, and still exits 0, when it cannot write a row, so
    # it writes to a pipe instead, and a full disk fails the copy here.
    process = sessions.start("--file", str(script), stdin=subprocess.DEVNULL)
    try:
        with path.open("wb") as rows:
            shutil.copyfileobj(process.stdout, rows, 1 << 20)
    except BaseException:
        process.terminate()
        raise
    finally:
        process.stdout.close()
        status = process.wait()
        sessions.finish(process)
    if status != 0:
        raise ExportError(f"{name} failed (psql exited {status})")


def export(args, out):
    script = Script(SCRIPT.read_text(encoding="utf-8"))
    # -w: concurrent sessions cannot share a password prompt; the password
    # must come from a password file or PGPASSWORD.
    psql = [args.psql, "-XqAtw", "-v", "ON_ERROR_STOP=1"]
    if args.fetch_count is not None:
        psql += ["-v", f"FETCH_COUNT={args.fetch_count}"]
    if args.dbname is not None:
        psql += ["--dbname", args.dbname]
    sessions = Sessions(psql)
    work = Path(tempfile.mkdtemp(prefix=".prism-recovery-evidence-", dir=args.work_dir))
    try:
        coordinator = Coordinator(sessions, script)
        exported = coordinator.ask(
            "SELECT pg_export_snapshot(), :'has_native_share_hashes'::boolean, :'share_hashes_deferred'::boolean;\n")
        snapshot, native, deferred = (exported.split("|") + ["", ""])[:3]
        if (not SNAPSHOT.fullmatch(snapshot) or native not in ("t", "f") or deferred not in ("t", "f")
                or exported.count("|") != 2):
            raise ExportError(f"unexpected snapshot export {exported!r}")
        share_bounds = sorted({int(bound) for bound in bounds(coordinator.ask(
            sample("qbit_share_ledger", "share_seq", 4 * args.jobs)), SHARE_BOUND)})
        if deferred == "t":
            # Pending at fence 2: the script exports none of the partial
            # mapping, so splitting it would only run empty parts.
            hash_sample = sample("qbit_prism_share_hashes", 'header_hash COLLATE "C"', 1)
        elif native == "t":
            hash_sample = sample("qbit_prism_share_hashes", 'header_hash COLLATE "C"', args.jobs)
        else:
            # The mapping the script derives from 2.x, which migration 002 backfills.
            hash_sample = sample("qbit_share_ledger", f'{LEGACY_HEADER} COLLATE "C"', args.jobs,
                                 f" WHERE accepted AND {LEGACY_MATCH}")
        hash_bounds = sorted(set(bounds(coordinator.ask(hash_sample), HASH_BOUND)))
        print(f"note: exporting snapshot {snapshot} in {args.jobs} sessions: shares in "
              f"{len(share_bounds) + 1} ranges, share_hashes in {len(hash_bounds) + 1}"
              f"{' (deferred: the share-hash backfill is pending)' if deferred == 't' else ''}",
              file=sys.stderr, flush=True)
        parts = script.parts(snapshot, share_bounds, hash_bounds, args.work_mem)
        paths = [work / f"{index:05}.jsonl" for index in range(len(parts))]
        # The closing kinds first, as they hold the audit bodies and the
        # integrity report. Then the shares in order, so they are printed
        # while the share hashes are still being exported.
        last = len(parts) - 1
        middle = len(share_bounds) + 1
        order = [last, middle, *range(middle), *range(middle + 1, last)]
        done = set()
        written = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = {pool.submit(run_part, sessions, *parts[index], paths[index]): index
                       for index in order}
            try:
                for future in concurrent.futures.as_completed(futures):
                    future.result()
                    done.add(futures[future])
                    # Write what is ready, in order, short of the marker's part.
                    while written < last and written in done:
                        with paths[written].open("rb") as rows:
                            shutil.copyfileobj(rows, out, 1 << 20)
                        paths[written].unlink()
                        written += 1
            except BaseException:
                for future in futures:
                    future.cancel()
                sessions.stop()
                raise
        coordinator.commit()
        with paths[last].open("rb") as rows:
            shutil.copyfileobj(rows, out, 1 << 20)
        out.flush()
    finally:
        sessions.stop()
        shutil.rmtree(work, ignore_errors=True)


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
    return number


def work_mem(value):
    if not WORK_MEM.fullmatch(value):
        raise argparse.ArgumentTypeError(f"{value!r} is not a work_mem such as 256MB")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--jobs", type=positive, default=4,
                        help="sessions exporting parts at once, besides the coordinator (default 4)")
    parser.add_argument("--work-mem", type=work_mem, default="256MB",
                        help="work_mem of each range part's sort (default 256MB)")
    parser.add_argument("--work-dir", type=Path, default=Path("."),
                        help="where parts wait to be written in order; budget the whole export "
                             "there, as for its output (default: the current directory)")
    parser.add_argument("--fetch-count", type=positive,
                        help="psql FETCH_COUNT of every session (default: the script's)")
    parser.add_argument("--dbname", help="psql --dbname of every session (default: libpq's environment)")
    parser.add_argument("--psql", default="psql", help="psql executable (default: psql)")
    args = parser.parse_args(argv)
    # Stop every session and remove the parts on these as on ^C.
    for stop in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(stop, lambda signum, frame: sys.exit(128 + signum))
    try:
        export(args, sys.stdout.buffer)
    except (ExportError, OSError) as error:
        if isinstance(error, OSError):
            # Stdout may be what failed: discard what it still buffers rather
            # than fail again flushing it at exit.
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        parser.exit(1, f"recovery evidence export failed: {error}\n")
    except KeyboardInterrupt:
        parser.exit(130, "recovery evidence export interrupted\n")


if __name__ == "__main__":
    main()

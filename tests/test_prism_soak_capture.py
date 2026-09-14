#!/usr/bin/env python3
"""Run the 24 h soak fences of ``docs/prism-capacity-readiness.md`` on a fake clock.

The step-3 ``capture`` loop, the step-6 gate and the resident-set bound check
are extracted from the doc and run under ``sh`` and ``bash`` with shell
functions standing in for ``date``, ``sleep`` and ``docker``: ``sleep``
advances a file-backed clock instead of waiting, so a whole 24 h soak is 289
iterations, and ``docker`` answers ``inspect`` and ``exec`` from fixed bodies
with a fault injected at a chosen call. The contract under test is the one
#318's review asked for: ``capture`` ends a run itself after 86,400 s of
samples from its first, publishes ``soak-complete`` only after the final
sample, its post-read identity check and the check that its reads ended in
cadence with the previous sample's have passed, and does so by a rename
so that a failed or interrupted write leaves no marker; the gate judges a
directory only when it holds that marker and no ``soak-invalid``, so a run
cut short after the judge's 82,800 s floor cannot pass. The loop also takes
the hour-1, hour-12 and hour-24 snapshots itself, at the first sample due for
each counted from its first, so the baseline and intermediate snapshots
precede the end state. A snapshot that fails leaves ``soak-invalid`` instead
of a completion marker.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "prism-capacity-readiness.md"

PLACEHOLDER = "c=<prism-coordinator-container>\n"
RUN = "soak-20260914T000000Z"
RSS_KB = 102400
SAMPLES_24H = 86400 // 300 + 1
METRICS_LINES_PER_SAMPLE = 4

# `sleep` moves the clock; `date` reads it; `docker` counts its `inspect` calls
# so a test can change the identity, or run a snippet in the run directory, at
# a chosen call, and counts the snapshot functions' `curl -fsS` scrapes so a
# test can answer one with a cached body; every scrape's share-ack histogram
# carries the clock it was read at. Anything the fences ask of a stub that it
# does not expect is appended to `$STUB/unexpected`, which every test requires
# to stay absent.
STUBS = r"""
stub_read() { read -r stub_value < "$STUB/$1"; }
stub_bump() {
  stub_read "$1"
  stub_value=$((stub_value + 1))
  echo "$stub_value" > "$STUB/$1"
}
date() {
  case $* in
    '+%s') stub_read clock; echo "$stub_value" ;;
    '-u +%FT%TZ') stub_read clock; echo "T+$stub_value" ;;
    '-u +%Y%m%dT%H%M%SZ') echo 20260914T000000Z ;;
    *) echo "date $*" >> "$STUB/unexpected"; return 1 ;;
  esac
}
sleep() {
  stub_read clock
  stub_value=$((stub_value + $1))
  echo "$stub_value" > "$STUB/clock"
  if [ "$stub_value" = "$STUB_ROLLBACK_AT_SLEEP" ] && [ ! -e "$STUB/rolled-back" ]; then
    echo "$STUB_ROLLBACK_TO" > "$STUB/clock"
    : > "$STUB/rolled-back"
  fi
  if [ -n "$STUB_INTERRUPT_AT" ] && [ "$stub_value" -ge "$STUB_INTERRUPT_AT" ]; then
    kill -s TERM $$
  fi
}
docker() {
  case "$1 $3" in
    'inspect '*)
      stub_bump inspects
      if [ "$STUB_INSPECT_FAULT_AT" = all ] || [ "$stub_value" = "$STUB_INSPECT_FAULT_AT" ]; then
        printf '%s' "$STUB_INSPECT_OUTPUT"
        return "$STUB_INSPECT_STATUS"
      fi
      if [ "$stub_value" = "$STUB_SNIPPET_AT_INSPECT" ]; then
        eval "$STUB_SNIPPET"
      fi
      if [ -n "$STUB_RESTART_AT_INSPECT" ] && [ "$stub_value" -ge "$STUB_RESTART_AT_INSPECT" ]; then
        echo 'running 2026-09-14T00:00:00.000000000Z 1'
      else
        echo 'running 2026-09-13T00:00:00.000000000Z 0'
      fi ;;
    'exec cat')
      stub_read clock
      printf 'Name:\tqbit-prism-server\nVmRSS:\t  %s kB\nThreads:\t8\n' "$STUB_RSS_KB"
      if [ "$stub_value" = "$STUB_RSS_FAULT_AT" ]; then return 1; fi ;;
    'exec curl')
      stub_state=fresh
      if [ "$4" = -fsS ]; then
        stub_bump snapshots
        if [ "$stub_value" = "$STUB_SNAPSHOT_STALE_AT" ]; then stub_state=stale; fi
      fi
      stub_read clock
      printf 'HTTP/1.1 200 OK\r\nx-prism-metrics-state: %s\r\ncontent-type: text/plain\r\n\r\n' "$stub_state"
      printf 'qbit_prism_collector_available{collector="process"} 1\n'
      printf 'qbit_prism_process_resident_memory_bytes %s\n' "$((STUB_RSS_KB * 1024))"
      printf 'qbit_prism_connections 3\n'
      printf 'qbit_prism_share_ack_seconds_count{result="accepted"} %s\n' "$stub_value" ;;
    *) echo "docker $*" >> "$STUB/unexpected"; return 1 ;;
  esac
}
if [ -n "$STUB_INTERRUPT_ON_MV" ]; then
  mv() {
    case $2 in */"$STUB_INTERRUPT_ON_MV") kill -s TERM $$ ;; esac
    command mv "$@"
  }
fi
"""


def shell_fences(text: str) -> list[str]:
    """Every ```sh fence of the doc, with its list-item indentation removed."""
    fences = []
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        opening = re.fullmatch(r"( *)```sh", lines[index])
        if opening:
            indent = opening.group(1)
            body = []
            index += 1
            while lines[index] != indent + "```":
                line = lines[index]
                if line:
                    assert line.startswith(indent), line
                body.append(line[len(indent) :])
                index += 1
            fences.append("\n".join(body) + "\n")
        index += 1
    return fences


def fence_containing(needle: str) -> str:
    matches = [fence for fence in shell_fences(DOC.read_text()) if needle in fence]
    assert len(matches) == 1, (needle, len(matches))
    return matches[0]


FENCES = shell_fences(DOC.read_text())
CAPTURE = fence_containing("capture() {")
GATE = fence_containing("completed() {")
JUDGE = fence_containing("min_span=82800")
SHARE_ACK = fence_containing("share_ack_snapshot() (")
FULL_METRICS = fence_containing("full_metrics_snapshot() (")
START = "capture\n"
assert CAPTURE.count(PLACEHOLDER) == 1
assert GATE.endswith("}\ncompleted\n")
# `capture` calls both snapshot functions, so the three fences only define
# functions and the run starts in the fence after them.
assert CAPTURE.endswith("\n}\n") and SHARE_ACK.endswith("\n)\n") and FULL_METRICS.endswith("\n)\n")
assert FENCES.count(START) == 1
assert FENCES[FENCES.index(CAPTURE) : FENCES.index(START) + 1] == [CAPTURE, SHARE_ACK, FULL_METRICS, START]

CAPTURE_SCRIPT = STUBS + CAPTURE.replace(PLACEHOLDER, "c=stub-container\n") + SHARE_ACK + FULL_METRICS + START
GATE_SCRIPT = f"run={RUN}\n" + GATE
# What step 6 tells the operator to do: judge inside the directory, and only
# once the gate has accepted it.
GATE_AND_JUDGE_SCRIPT = f"run={RUN}\n" + GATE.rstrip("\n") + ' && (\n  cd "$run" && ' + JUDGE + ")\n"


class SoakRun:
    def __init__(self, shell: str, **env: str) -> None:
        self.work = Path(tempfile.mkdtemp(prefix="soak-capture-"))
        self.stub = self.work / "stub"
        self.stub.mkdir()
        (self.stub / "clock").write_text("0\n")
        (self.stub / "inspects").write_text("0\n")
        (self.stub / "snapshots").write_text("0\n")
        self.run = self.work / RUN
        self.shell = shell
        self.env = {**os.environ, "STUB": str(self.stub), "STUB_RSS_KB": str(RSS_KB), **env}

    def capture(self) -> subprocess.CompletedProcess[str]:
        script = self.work / "capture.sh"
        script.write_text(CAPTURE_SCRIPT)
        return self._run([self.shell, str(script)], self.work)

    def gate(self) -> subprocess.CompletedProcess[str]:
        return self._run([self.shell, "-c", GATE_SCRIPT], self.work)

    def gate_and_judge(self) -> subprocess.CompletedProcess[str]:
        return self._run([self.shell, "-c", GATE_AND_JUDGE_SCRIPT], self.work)

    def judge_alone(self) -> subprocess.CompletedProcess[str]:
        return self._run([self.shell, "-c", JUDGE], self.run)

    def _run(self, argv: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(argv, cwd=cwd, env=self.env, capture_output=True, text=True, timeout=600)

    def unexpected(self) -> str:
        path = self.stub / "unexpected"
        return path.read_text() if path.exists() else ""

    def lines(self, name: str) -> list[str]:
        return (self.run / name).read_text().splitlines()

    def entries(self) -> list[str]:
        return sorted(entry.name for entry in self.run.iterdir())

    def snapshot_clocks(self) -> dict[str, int]:
        """The fake clock each published snapshot was scraped at."""
        clocks = {}
        for name in SNAPSHOTS:
            if (self.run / name).exists():
                (count,) = re.findall(
                    r'^qbit_prism_share_ack_seconds_count\{result="accepted"\} (\d+)$', (self.run / name).read_text(), re.M,
                )
                clocks[name] = int(count)
        return clocks


COMPLETE_LINE = "T+86400: soak complete, the samples from 0 to 86400 span 86400 s"
RUN_FILES = ["soak-metrics.log", "soak-process.log", "soak-rss.csv"]
# In the order `capture` takes them, which is also the order of their scrapes.
SNAPSHOTS = ["share-ack-h01.txt", "metrics-h01.txt", "share-ack-h12.txt", "share-ack-h24.txt", "metrics-h24.txt"]


class ShareAckSnapshotTests(unittest.TestCase):
    HISTOGRAM = (
        'qbit_prism_share_ack_seconds_bucket{result="accepted",le="+Inf"} 5\n'
        'qbit_prism_share_ack_seconds_sum{result="accepted"} 0.1\n'
        'qbit_prism_share_ack_seconds_count{result="accepted"} 5\n'
    )

    def snapshot(
        self, shell: str, state: str | None, *, status: int = 0, histogram: bool = True,
    ) -> tuple[subprocess.CompletedProcess[str], str | None]:
        with tempfile.TemporaryDirectory(prefix="share-ack-snapshot-") as directory:
            work = Path(directory)
            headers = "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n"
            if state is not None:
                headers += f"x-prism-metrics-state: {state}\r\n"
            body = "qbit_prism_connections 3\n" + (self.HISTOGRAM if histogram else "")
            (work / "response").write_bytes((headers + "\r\n" + body).encode())
            # Like curl, the stub includes headers only when asked with -D -.
            script = r'''
docker() {
  case " $* " in
    *" -D - "*) cat response ;;
    *) sed '1,/^\r$/d' response ;;
  esac
  return "$SCRAPE_STATUS"
}
c=stub-container
run=.
''' + SHARE_ACK + 'share_ack_snapshot "$run/share-ack-h01.txt"\n'
            result = subprocess.run(
                [shell, "-c", script], cwd=work, capture_output=True, text=True,
                env={**os.environ, "SCRAPE_STATUS": str(status)}, timeout=10,
            )
            target = work / "share-ack-h01.txt"
            return result, target.read_text() if target.exists() else None

    def test_fresh_snapshot_keeps_headers_and_histogram(self) -> None:
        for shell in ("sh", "bash"):
            with self.subTest(shell=shell):
                result, snapshot = self.snapshot(shell, "fresh")
                self.assertEqual((result.returncode, result.stderr), (0, ""))
                self.assertEqual(
                    snapshot,
                    "HTTP/1.1 200 OK\ncontent-type: text/plain\n"
                    "x-prism-metrics-state: fresh\n\n" + self.HISTOGRAM,
                )

    def test_stale_unavailable_or_missing_header_publishes_no_snapshot(self) -> None:
        for shell in ("sh", "bash"):
            for state in ("stale", "unavailable", None):
                with self.subTest(shell=shell, state=state):
                    result, snapshot = self.snapshot(shell, state)
                    self.assertNotEqual(result.returncode, 0, result)
                    self.assertIsNone(snapshot)

    def test_failed_scrape_or_missing_histogram_publishes_no_snapshot(self) -> None:
        for shell in ("sh", "bash"):
            for status, histogram in ((28, True), (22, False), (0, False)):
                with self.subTest(shell=shell, status=status, histogram=histogram):
                    result, snapshot = self.snapshot(shell, "fresh", status=status, histogram=histogram)
                    self.assertNotEqual(result.returncode, 0, result)
                    self.assertIsNone(snapshot)


class FullMetricsSnapshotTests(unittest.TestCase):
    RESPONSE = (
        b"HTTP/1.1 200 OK\r\nx-prism-metrics-state: fresh\r\ncontent-type: text/plain\r\n\r\n"
        b"# HELP qbit_prism_connections Active connections\nqbit_prism_connections 3\n"
        b'qbit_prism_share_ack_seconds_count{result="accepted"} 5\n'
    )

    def snapshot(self, shell: str, response: bytes, *, status: int = 0, fault: str = "", existing: bool = False):
        with tempfile.TemporaryDirectory(prefix="full-metrics-snapshot-") as directory:
            work = Path(directory)
            (work / "response").write_bytes(response)
            target = work / "metrics-h01.txt"
            temporary = work / "metrics-h01.txt.tmp"
            if existing:
                target.write_bytes(b"previous snapshot\n")
            if fault == "write":
                temporary.mkdir()
            script = r'''
docker() {
  if [ "$*" != "exec stub-container curl -fsS --max-time 5 -D - http://127.0.0.1:3341/metrics" ]; then
    echo "unexpected docker invocation: $*" >&2
    return 99
  fi
  cat response
  return "$SCRAPE_STATUS"
}
case $PUBLISH_FAULT in
  rename) mv() { return 1; } ;;
  interrupt) mv() { kill -s TERM $$; } ;;
esac
c=stub-container
run=.
''' + FULL_METRICS + 'full_metrics_snapshot "$run/metrics-h01.txt"\n'
            result = subprocess.run(
                [shell, "-c", script], cwd=work, capture_output=True, text=True,
                env={**os.environ, "SCRAPE_STATUS": str(status), "PUBLISH_FAULT": fault}, timeout=10,
            )
            return result, target.read_bytes() if target.exists() else None, temporary.is_file()

    def test_fresh_snapshot_preserves_the_entire_response(self) -> None:
        for shell in ("sh", "bash"):
            for response in (self.RESPONSE, self.RESPONSE.replace(b"x-prism-metrics-state", b"X-Prism-Metrics-State")):
                with self.subTest(shell=shell, response=response):
                    result, snapshot, temporary = self.snapshot(shell, response)
                    self.assertEqual((result.returncode, result.stderr), (0, ""))
                    self.assertEqual(snapshot, response)
                    self.assertFalse(temporary)

    def test_cached_or_missing_fresh_header_publishes_nothing(self) -> None:
        for shell in ("sh", "bash"):
            for state in (b"stale", b"unavailable", None):
                response = (self.RESPONSE.replace(b"fresh", state) if state is not None
                            else self.RESPONSE.replace(b"x-prism-metrics-state: fresh\r\n", b""))
                # A fresh-looking line in the body cannot substitute for the header.
                response += b"x-prism-metrics-state: fresh\n"
                with self.subTest(shell=shell, state=state):
                    result, snapshot, _ = self.snapshot(shell, response)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("fresh state header required", result.stderr)
                    self.assertIsNone(snapshot)

    def test_http_and_partial_transport_failures_preserve_existing_evidence(self) -> None:
        for shell in ("sh", "bash"):
            for status, response in ((22, b"HTTP/1.1 500 Internal Server Error\r\n\r\nerror"), (28, self.RESPONSE[:-10])):
                for existing in (False, True):
                    with self.subTest(shell=shell, status=status, existing=existing):
                        result, snapshot, _ = self.snapshot(shell, response, status=status, existing=existing)
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("metrics scrape or temporary-file write failed", result.stderr)
                        self.assertEqual(snapshot, b"previous snapshot\n" if existing else None)

    def test_failed_or_interrupted_publication_leaves_no_final_snapshot(self) -> None:
        for shell in ("sh", "bash"):
            for fault in ("write", "rename", "interrupt"):
                with self.subTest(shell=shell, fault=fault):
                    result, snapshot, temporary = self.snapshot(shell, self.RESPONSE, fault=fault)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIsNone(snapshot)
                    if fault != "write":
                        self.assertTrue(temporary)


class SoakCaptureTests(unittest.TestCase):
    def assert_no_marker_at_all(self, soak: SoakRun) -> None:
        self.assertFalse(os.path.lexists(soak.run / "soak-complete"))

    def assert_gate_refuses_for_want_of_marker(self, soak: SoakRun) -> None:
        gate = soak.gate()
        self.assertEqual(gate.returncode, 1, gate)
        self.assertEqual(gate.stdout, "")
        self.assertIn("no soak-complete", gate.stderr)
        self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_fences_parse_under_sh_and_bash(self) -> None:
        for shell in ("sh", "bash"):
            for script in (CAPTURE_SCRIPT, GATE_SCRIPT, GATE_AND_JUDGE_SCRIPT):
                check = subprocess.run([shell, "-n"], input=script, capture_output=True, text=True)
                self.assertEqual(check.returncode, 0, (shell, check.stderr))

    def test_full_run_completes_after_24_h_and_is_judged(self) -> None:
        for shell in ("sh", "bash"):
            with self.subTest(shell=shell):
                soak = SoakRun(shell)
                result = soak.capture()
                self.assertEqual((result.returncode, result.stderr, soak.unexpected()), (0, "", ""))
                self.assertEqual(soak.entries(), sorted([*SNAPSHOTS, "soak-complete", *RUN_FILES]))
                # Each snapshot was scraped once, at the sample due for it, not at the end.
                self.assertEqual(
                    soak.snapshot_clocks(),
                    {
                        "share-ack-h01.txt": 3600, "metrics-h01.txt": 3600, "share-ack-h12.txt": 43200,
                        "share-ack-h24.txt": 86400, "metrics-h24.txt": 86400,
                    },
                )
                self.assertEqual((soak.stub / "snapshots").read_text(), "5\n")
                self.assertTrue((soak.run / "soak-complete").is_file())
                self.assertEqual(soak.lines("soak-complete"), [COMPLETE_LINE])
                rss = soak.lines("soak-rss.csv")
                self.assertEqual(len(rss), SAMPLES_24H)
                self.assertEqual((rss[0], rss[-1]), (f"0,{RSS_KB * 1024}", f"86400,{RSS_KB * 1024}"))
                process = soak.lines("soak-process.log")
                self.assertEqual(len(process), SAMPLES_24H)
                self.assertEqual(set(line.split(" ", 1)[1] for line in process), {"running 2026-09-13T00:00:00.000000000Z 0"})
                self.assertEqual(len(soak.lines("soak-metrics.log")), SAMPLES_24H * METRICS_LINES_PER_SAMPLE)
                gate = soak.gate()
                self.assertEqual((gate.returncode, gate.stdout, gate.stderr), (0, COMPLETE_LINE + "\n", ""))
                judged = soak.gate_and_judge()
                self.assertEqual((judged.returncode, judged.stderr), (0, ""))
                self.assertEqual(
                    judged.stdout.splitlines(),
                    [COMPLETE_LINE, "baseline=104857600 bound=209715200 peak=104857600 peak_at=3900 first_breach_at=none"],
                )

    def test_run_interrupted_after_23_h_is_not_judged(self) -> None:
        # The review's case: a SIGTERM in the 24th hour runs no stop path and
        # leaves a CSV the bound check's 82,800 s floor accepts on its own.
        soak = SoakRun("sh", STUB_INTERRUPT_AT="83100")
        result = soak.capture()
        self.assertEqual((result.returncode, result.stderr, soak.unexpected()), (-signal.SIGTERM, "", ""))
        self.assertEqual(soak.entries(), sorted([*SNAPSHOTS[:3], *RUN_FILES]))
        rss = soak.lines("soak-rss.csv")
        self.assertEqual((len(rss), rss[-1]), (82800 // 300 + 1, f"82800,{RSS_KB * 1024}"))
        alone = soak.judge_alone()
        self.assertEqual(alone.returncode, 0, alone)
        self.assertIn("first_breach_at=none", alone.stdout)
        self.assert_gate_refuses_for_want_of_marker(soak)

    def test_identity_change_on_the_final_sample_is_invalid(self) -> None:
        # Inspect calls: one before the loop, then two per sample; the 289th
        # sample's post-read check is call 579.
        soak = SoakRun("sh", STUB_RESTART_AT_INSPECT=str(2 * SAMPLES_24H + 1))
        result = soak.capture()
        self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
        self.assertEqual(soak.entries(), sorted([*SNAPSHOTS, "soak-invalid", *RUN_FILES]))
        invalid = soak.lines("soak-invalid")
        self.assertEqual(result.stderr, (soak.run / "soak-invalid").read_text())
        self.assertEqual(
            invalid,
            [
                "T+86400: soak invalid, the coordinator changed while the sample at 86400 was read",
                "  at start:   running 2026-09-13T00:00:00.000000000Z 0",
                "  after read: running 2026-09-14T00:00:00.000000000Z 1",
            ],
        )
        self.assertEqual(len(soak.lines("soak-rss.csv")), SAMPLES_24H)
        gate = soak.gate()
        self.assertEqual((gate.returncode, gate.stdout), (1, ""))
        self.assertIn("soak-invalid says why the run is invalid", gate.stderr)
        self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_failed_or_empty_identity_probes_invalidate_the_run(self) -> None:
        # A denied inspect must not pass by comparing empty identities, and
        # matching output from a failed command is not a successful probe.
        identity = "running 2026-09-13T00:00:00.000000000Z 0\n"
        cases = [
            (shell, fault_at, probe, stage, status, output)
            for shell in ("sh", "bash")
            for fault_at, probe, stage in (
                ("all", 1, "at the start of the run"),
                ("2", 2, "before the sample at 0"),
                ("3", 3, "after the sample at 0"),
            )
            for status, output in (("1", ""), ("0", ""), ("1", identity))
        ]
        # The early cases cover both read boundaries under both shells. One
        # full-duration post-read fault protects the last check before marker
        # publication without repeating the entire matrix for 289 samples.
        final_probe = 2 * SAMPLES_24H + 1
        cases.append(("sh", str(final_probe), final_probe, "after the sample at 86400", "1", identity))
        for shell, fault_at, probe, stage, status, output in cases:
            with self.subTest(shell=shell, fault_at=fault_at, status=status, output=output):
                soak = SoakRun(
                    shell,
                    STUB_INSPECT_FAULT_AT=fault_at,
                    STUB_INSPECT_STATUS=status,
                    STUB_INSPECT_OUTPUT=output,
                )
                result = soak.capture()
                self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
                self.assertEqual(result.stderr, (soak.run / "soak-invalid").read_text())
                self.assertIn(f"could not read the coordinator process identity {stage}", result.stderr)
                self.assertEqual((soak.stub / "inspects").read_text(), f"{probe}\n")
                rss = soak.run / "soak-rss.csv"
                self.assertEqual(len(rss.read_text().splitlines()) if rss.exists() else 0, (probe - 1) // 2)
                self.assert_no_marker_at_all(soak)
                gate = soak.gate()
                self.assertEqual((gate.returncode, gate.stdout), (1, ""))
                self.assertIn("soak-invalid says why the run is invalid", gate.stderr)
                self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_failed_rss_read_with_valid_output_invalidates_the_run(self) -> None:
        for shell in ("sh", "bash"):
            for now in (0, 86400):
                with self.subTest(shell=shell, now=now):
                    soak = SoakRun(shell, STUB_RSS_FAULT_AT=str(now))
                    result = soak.capture()
                    self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
                    self.assertEqual(result.stderr, (soak.run / "soak-invalid").read_text())
                    self.assertIn(f"no RSS sample at {now}", result.stderr)
                    self.assertIn("docker exec exited 1", result.stderr)
                    self.assertIn(f"VmRSS:\t  {RSS_KB} kB", result.stderr)
                    for name, lines_per_sample in (("soak-rss.csv", 1), ("soak-metrics.log", METRICS_LINES_PER_SAMPLE)):
                        path = soak.run / name
                        lines = path.read_text().splitlines() if path.exists() else []
                        self.assertEqual(len(lines), now // 300 * lines_per_sample)
                    self.assert_no_marker_at_all(soak)
                    gate = soak.gate()
                    self.assertEqual((gate.returncode, gate.stdout), (1, ""))
                    self.assertIn("soak-invalid says why the run is invalid", gate.stderr)
                    self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_final_sample_whose_reads_stall_for_an_hour_is_invalid(self) -> None:
        # Codex's case: the 289th sample's pre-read inspect (call 578) stalls
        # for an hour while the identity and both bodies stay healthy. `now`
        # was read before the stall, so the row is stamped 86400 for a process
        # observed at 90000; the post-read identity check passes, and the
        # completion check used to read that stale `now` and publish a marker
        # spanning 0..86400, with no next iteration to see the gap.
        for shell in ("sh", "bash"):
            with self.subTest(shell=shell):
                soak = SoakRun(
                    shell,
                    STUB_SNIPPET_AT_INSPECT=str(2 * SAMPLES_24H),
                    STUB_SNIPPET='echo 90000 > "$STUB/clock"',
                )
                result = soak.capture()
                self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
                self.assertEqual(soak.entries(), sorted([*SNAPSHOTS, "soak-invalid", *RUN_FILES]))
                self.assertEqual(result.stderr, (soak.run / "soak-invalid").read_text())
                self.assertEqual(
                    soak.lines("soak-invalid"),
                    ["T+90000: soak invalid, the reads for the sample at 86400 ended at 90000, 3900 s after the previous sample's ended at 86100"],
                )
                rss = soak.lines("soak-rss.csv")
                self.assertEqual((len(rss), rss[-1]), (SAMPLES_24H, f"86400,{RSS_KB * 1024}"))
                self.assertEqual(len(soak.lines("soak-process.log")), SAMPLES_24H)
                alone = soak.judge_alone()
                self.assertEqual(alone.returncode, 0, alone)
                self.assertIn("first_breach_at=none", alone.stdout)
                self.assert_no_marker_at_all(soak)
                gate = soak.gate()
                self.assertEqual((gate.returncode, gate.stdout), (1, ""))
                self.assertIn("soak-invalid says why the run is invalid", gate.stderr)
                self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_sample_reads_get_the_minute_the_gap_tolerance_leaves_them(self) -> None:
        # From one sample's end to the next is the 300 s sleep plus the later
        # sample's reads, so the 360 s tolerance leaves the reads 60 s. A
        # non-final sample that overran it was already caught, by the next
        # iteration's start-time check; the final sample is held to the same
        # minute by the end-time check, where it used to complete with a
        # marker for reads of any length.
        final_pre_read = str(2 * SAMPLES_24H)
        soak = SoakRun("sh", STUB_SNIPPET_AT_INSPECT=final_pre_read, STUB_SNIPPET='echo 86460 > "$STUB/clock"')
        result = soak.capture()
        self.assertEqual((result.returncode, result.stderr, soak.unexpected()), (0, "", ""))
        self.assertEqual(soak.entries(), sorted([*SNAPSHOTS, "soak-complete", *RUN_FILES]))
        late_marker = "T+86460: soak complete, the samples from 0 to 86400 span 86400 s"
        self.assertEqual(soak.lines("soak-complete"), [late_marker])
        self.assertEqual(soak.lines("soak-rss.csv")[-1], f"86400,{RSS_KB * 1024}")
        gate = soak.gate()
        self.assertEqual((gate.returncode, gate.stdout, gate.stderr), (0, late_marker + "\n", ""))

        for sample, ended, snapshots, expected in (
            (SAMPLES_24H, 86461, SNAPSHOTS, "T+86461: soak invalid, the reads for the sample at 86400 ended at 86461, 361 s after the previous sample's ended at 86100"),
            (2, 361, [], "T+361: soak invalid, the reads for the sample at 300 ended at 361, 361 s after the previous sample's ended at 0"),
        ):
            with self.subTest(sample=sample):
                soak = SoakRun("sh", STUB_SNIPPET_AT_INSPECT=str(2 * sample), STUB_SNIPPET=f'echo {ended} > "$STUB/clock"')
                result = soak.capture()
                self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
                self.assertEqual(soak.entries(), sorted([*snapshots, "soak-invalid", *RUN_FILES]))
                self.assertEqual(soak.lines("soak-invalid"), [expected])
                self.assertEqual(len(soak.lines("soak-rss.csv")), sample)
                self.assert_no_marker_at_all(soak)
                gate = soak.gate()
                self.assertEqual((gate.returncode, gate.stdout), (1, ""))
                self.assertIn("soak-invalid says why the run is invalid", gate.stderr)
                self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_hour_snapshots_are_taken_at_the_first_sample_due_from_the_first(self) -> None:
        # Codex's case: `capture` holds the shell for the whole soak, so
        # snapshot calls typed after it all read the end state. The loop takes
        # them itself. The run starts at 1234 and its first sample's reads end
        # at 1279, so no later sample is a whole number of hours after the
        # first or on the clock's own hour: each snapshot comes from the first
        # sample at or past its hour, counted from the first sample.
        for shell in ("sh", "bash"):
            with self.subTest(shell=shell):
                soak = SoakRun(shell, STUB_SNIPPET_AT_INSPECT="2", STUB_SNIPPET='echo 1279 > "$STUB/clock"')
                (soak.stub / "clock").write_text("1234\n")
                result = soak.capture()
                self.assertEqual((result.returncode, result.stderr, soak.unexpected()), (0, "", ""))
                self.assertEqual(
                    soak.snapshot_clocks(),
                    {
                        "share-ack-h01.txt": 4879, "metrics-h01.txt": 4879, "share-ack-h12.txt": 44479,
                        "share-ack-h24.txt": 87679, "metrics-h24.txt": 87679,
                    },
                )
                self.assertEqual((soak.stub / "snapshots").read_text(), "5\n")
                self.assertEqual(
                    soak.lines("soak-complete"), ["T+87679: soak complete, the samples from 1234 to 87679 span 86445 s"],
                )

    def test_hour_snapshot_that_fails_invalidates_the_run(self) -> None:
        # Snapshot scrapes 1 to 5 are SNAPSHOTS in order; the chosen one is
        # answered with a cached body. The run stops at that sample, keeps the
        # snapshots taken before it, publishes nothing under the failed name
        # and, when the failure is an hour-24 snapshot, writes no marker.
        share_ack = "  share-ack snapshot failed: fresh state header and histogram required"
        full_metrics = "  full metrics snapshot failed: fresh state header required"
        for shell in ("sh", "bash"):
            for scrape, hour, kind, detail in (
                (1, 1, "share-ack", share_ack),
                (2, 1, "full metrics", full_metrics),
                (3, 12, "share-ack", share_ack),
                (4, 24, "share-ack", share_ack),
                (5, 24, "full metrics", full_metrics),
            ):
                with self.subTest(shell=shell, scrape=scrape):
                    now = hour * 3600
                    soak = SoakRun(shell, STUB_SNAPSHOT_STALE_AT=str(scrape))
                    result = soak.capture()
                    self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
                    self.assertEqual(result.stderr, (soak.run / "soak-invalid").read_text())
                    self.assertEqual(
                        soak.lines("soak-invalid"), [f"T+{now}: soak invalid, no hour-{hour} {kind} snapshot at {now}", detail],
                    )
                    self.assertEqual((soak.stub / "snapshots").read_text(), f"{scrape}\n")
                    # full_metrics_snapshot leaves its unpublished temporary file.
                    temporary = [SNAPSHOTS[scrape - 1] + ".tmp"] if kind == "full metrics" else []
                    self.assertEqual(
                        soak.entries(), sorted([*SNAPSHOTS[: scrape - 1], *temporary, "soak-invalid", *RUN_FILES]),
                    )
                    self.assertEqual(len(soak.lines("soak-rss.csv")), now // 300 + 1)
                    self.assert_no_marker_at_all(soak)
                    gate = soak.gate()
                    self.assertEqual((gate.returncode, gate.stdout), (1, ""))
                    self.assertIn("soak-invalid says why the run is invalid", gate.stderr)
                    self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_backward_clock_step_between_samples_is_invalid_before_recording(self) -> None:
        for shell in ("sh", "bash"):
            for sleep_at, rollback_to, previous_end, samples, first_read_end in (
                (600, 0, 300, 2, 0),
                # Still later than the previous start (0), but earlier than its end (30).
                (330, 15, 30, 1, 30),
                (86400, 3600, 86100, SAMPLES_24H - 1, 0),
            ):
                with self.subTest(shell=shell, sleep_at=sleep_at):
                    soak = SoakRun(
                        shell, STUB_ROLLBACK_AT_SLEEP=str(sleep_at), STUB_ROLLBACK_TO=str(rollback_to),
                        STUB_SNIPPET_AT_INSPECT="2", STUB_SNIPPET=f'echo {first_read_end} > "$STUB/clock"',
                    )
                    result = soak.capture()
                    self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
                    self.assertEqual(result.stderr, (soak.run / "soak-invalid").read_text())
                    self.assertIn(f"clock moved backward from the previous sample's end at {previous_end} to {rollback_to}", result.stderr)
                    self.assertEqual(len(soak.lines("soak-rss.csv")), samples)
                    self.assertEqual(len(soak.lines("soak-process.log")), samples)
                    self.assert_no_marker_at_all(soak)
                    self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_backward_clock_step_during_sample_reads_is_invalid(self) -> None:
        for shell in ("sh", "bash"):
            for sample, end in ((1, -1), (2, 299), (14, 1800), (SAMPLES_24H, 86399)):
                with self.subTest(shell=shell, sample=sample, end=end):
                    soak = SoakRun(
                        shell, STUB_SNIPPET_AT_INSPECT=str(2 * sample),
                        STUB_SNIPPET=f'echo {end} > "$STUB/clock"',
                    )
                    result = soak.capture()
                    self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
                    self.assertEqual(result.stderr, (soak.run / "soak-invalid").read_text())
                    self.assertIn(f"clock moved backward during the sample from {(sample - 1) * 300} to {end}", result.stderr)
                    self.assert_no_marker_at_all(soak)
                    self.assertEqual(soak.gate_and_judge().returncode, 1)

    def test_equal_consecutive_clock_readings_remain_valid(self) -> None:
        # Equality remains valid at both boundaries; only a negative interval fails.
        soak = SoakRun("sh", STUB_ROLLBACK_AT_SLEEP="300", STUB_ROLLBACK_TO="0")
        result = soak.capture()
        self.assertEqual((result.returncode, result.stderr, soak.unexpected()), (0, "", ""))
        self.assertEqual(soak.gate_and_judge().returncode, 0)

    @unittest.skipUnless(Path("/dev/full").exists(), "needs /dev/full to fail a write")
    def test_marker_write_that_fails_publishes_no_marker_even_when_invalid_cannot_be_written(self) -> None:
        # At the final post-read check the marker's temporary file already
        # points at a full disk and `soak-invalid` at a directory that does
        # not exist: the write fails after the file was created, and the
        # invalid marker cannot be written either. Nothing is published.
        soak = SoakRun(
            "sh",
            STUB_SNIPPET_AT_INSPECT=str(2 * SAMPLES_24H + 1),
            STUB_SNIPPET='ln -s /dev/full "$run/soak-complete.tmp" && ln -s "$STUB/absent/soak-invalid" "$run/soak-invalid"',
        )
        result = soak.capture()
        self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
        self.assertIn("soak invalid, could not write", result.stderr)
        self.assertIn("soak-complete after the sample at 86400", result.stderr)
        self.assert_no_marker_at_all(soak)
        self.assertTrue((soak.run / "soak-complete.tmp").is_symlink())
        self.assertTrue(os.path.lexists(soak.run / "soak-invalid"))
        self.assertFalse((soak.run / "soak-invalid").exists())
        self.assertEqual(len(soak.lines("soak-rss.csv")), SAMPLES_24H)
        self.assert_gate_refuses_for_want_of_marker(soak)

    def test_interruption_between_marker_write_and_rename_publishes_no_marker(self) -> None:
        # Only the marker's rename is interrupted; the snapshots' renames run.
        soak = SoakRun("sh", STUB_INTERRUPT_ON_MV="soak-complete")
        result = soak.capture()
        self.assertEqual((result.returncode, result.stderr, soak.unexpected()), (-signal.SIGTERM, "", ""))
        self.assertEqual(soak.entries(), sorted([*SNAPSHOTS, "soak-complete.tmp", *RUN_FILES]))
        self.assertEqual(soak.lines("soak-complete.tmp"), [COMPLETE_LINE])
        self.assert_no_marker_at_all(soak)
        self.assert_gate_refuses_for_want_of_marker(soak)

    def test_existing_run_directory_is_never_written_into(self) -> None:
        soak = SoakRun("sh")
        soak.run.mkdir()
        (soak.run / "soak-complete").write_text("stale marker from an earlier run\n")
        result = soak.capture()
        self.assertEqual((result.returncode, soak.unexpected()), (1, ""))
        self.assertIn("File exists", result.stderr)
        self.assertEqual(soak.entries(), ["soak-complete"])
        self.assertEqual(soak.lines("soak-complete"), ["stale marker from an earlier run"])
        self.assertEqual((soak.stub / "clock").read_text(), "0\n")

    def test_gate_reads_the_markers_before_anything_else(self) -> None:
        both = SoakRun("sh")
        both.run.mkdir()
        (both.run / "soak-complete").write_text(COMPLETE_LINE + "\n")
        (both.run / "soak-invalid").write_text("T+1: soak invalid, no RSS sample at 1\n")
        gate = both.gate()
        self.assertEqual((gate.returncode, gate.stdout), (1, ""))
        self.assertIn("soak-invalid says why the run is invalid", gate.stderr)

        directory = SoakRun("sh")
        directory.run.mkdir()
        (directory.run / "soak-complete").mkdir()
        self.assert_gate_refuses_for_want_of_marker(directory)

        only_temporary = SoakRun("sh")
        only_temporary.run.mkdir()
        (only_temporary.run / "soak-complete.tmp").write_text(COMPLETE_LINE + "\n")
        self.assert_gate_refuses_for_want_of_marker(only_temporary)


if __name__ == "__main__":
    unittest.main()

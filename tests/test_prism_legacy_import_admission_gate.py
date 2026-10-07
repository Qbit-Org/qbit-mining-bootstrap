"""The go-live gate for frontends admitting traffic while import-audits runs (#734).

The hermetic tests put a fake `qbit-prism-server` and a fake `psql` on PATH;
the fake psql answers each of the gate's queries from the environment and logs
its command line. The real-schema test migrates a fresh database with a real
`qbit-prism-server` and runs the gate's SQL against it with the real psql; it
runs only when PRISM_TEST_DATABASE_URL and PRISM_ADMISSION_GATE_SERVER_BIN are
set.
"""
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import subprocess
import tempfile
import textwrap
import threading
import time
import unittest
import urllib.parse
import uuid


REPO = Path(__file__).resolve().parents[1]
GATE = REPO / "scripts/prism_legacy_import_admission_gate.sh"
PASSWORD = "s3cret-p@ss"
DSN = "postgres://gate-user:s3cret-p%40ss@db.example.invalid/prism?sslmode=verify-full"
TOKEN = "operator-token-s3cret"
STANDBY_PASSWORD = "st4ndby-pw"
STANDBY_DSN = "postgres://reader:st4ndby-pw@standby.example.invalid/prism?sslmode=verify-full"
STANDBY_URL = "postgres://reader@standby.example.invalid/prism?sslmode=verify-full"
# The writer's and the standby's system identifier and database name, as both probes print them.
ID = "7000000000000000001|prism"
TESTNET_FALLBACK = "tq1zlsq9dpxz8mennhdpr9nf9s0f2tjtq6gxs9m84k6xglhkfp92q2zszzu4m3"
TEST_DATABASE_URL = os.environ.get("PRISM_TEST_DATABASE_URL", "")
SERVER_BIN = os.environ.get("PRISM_ADMISSION_GATE_SERVER_BIN", "")

FAKE_SERVER = textwrap.dedent("""\
    #!/usr/bin/env python3
    import os, sys
    command = sys.argv[1:2]
    if command == ["check-config"]:
        print("WARNING: block submission is disabled (PRISM_BLOCK_SUBMIT_ENABLED=0)")
        if os.environ.get("FAKE_CHECK_CONFIG_FAIL"):
            print("Error: PRISM_FEE_PERCENT leaves dust unsettled")
            sys.exit(1)
    elif command == ["submission-hold"]:
        print('{"held": true, "set_by": "drill"}')
    elif command == ["healthcheck"]:
        if os.environ.get("FAKE_HEALTH_FAIL"):
            print("Error: PRISM health is not ready", file=sys.stderr)
            sys.exit(1)
    else:
        sys.exit(f"fake qbit-prism-server: unexpected {sys.argv[1:]}")
""")

# Answers each query from the environment, so a test changes one fact at a
# time, and logs its command line and PGPASSWORD. pg_stat_activity comes
# first: the import probe's patterns name qbit_pool_audit_bundles.
FAKE_PSQL = textwrap.dedent("""\
    #!/usr/bin/env python3
    import json, os, sys
    ID = "7000000000000000001|prism"
    args = sys.argv[1:]
    query = sys.stdin.read()
    with open(os.environ["FAKE_PSQL_LOG"], "a") as log:
        log.write(json.dumps({"argv": args, "pgpassword": os.environ.get("PGPASSWORD"),
                              "query": query}) + "\\n")
    failing = os.environ.get("FAKE_PSQL_FAIL")
    if failing and failing in query:
        sys.exit(os.environ.get("FAKE_PSQL_STDERR")
                 or f"psql: error: connection to {args[0]} failed")
    answers = [
        ("FROM pg_stat_activity", "FAKE_IMPORT", "1|3|0"),
        ("FROM qbit_prism_instances", "FAKE_CENSUS",
         '2|0|0|0|[{"id": "frontend-a", "age_s": 1.0, "status": "{}"}]'),
        ("FROM qbit_pool_audit_bundles", "FAKE_COMPLETENESS", "0|0"),
        ("qbit_carry_forward_integrity_report", "FAKE_INTEGRITY", "0|0"),
        ("FROM pg_settings", "FAKE_DURABILITY",
         "fsync=on full_page_writes=on synchronous_commit=on"),
        ("pg_stat_replication", "FAKE_STANDBY", "true|1|1"),
        ("FROM qbit_share_ledger", "FAKE_RECENT_MINER", "qb1zrecentminer"),
        ("pg_is_in_recovery", "FAKE_RECOVERY", "true|0/3000148|paused|3|false|off|30s|30s|" + ID),
        ("synchronous_standby_names", "FAKE_WRITER", "on||0/3000200|" + ID),
    ]
    for needle, variable, default in answers:
        if needle in query:
            print(os.environ.get(variable, default))
            sys.exit(0)
    sys.exit(f"fake psql: unexpected query {query!r}")
""")


def write_executable(path, text):
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class FakeStratum:
    """A highdiff listener that answers subscribe and authorize, then sends the
    given raw lines, or keeps sending notifications without a difficulty, and
    records the username the gate authorized with."""

    def __init__(self, difficulty=None, lines=None, chatty=False):
        self.lines = lines if lines is not None else [
            json.dumps({"id": None, "method": "mining.set_difficulty",
                        "params": [difficulty]})]
        self.chatty = chatty
        self.usernames = []
        self.server = socket.socket()
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(1)
        self.port = self.server.getsockname()[1]
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        connection, _ = self.server.accept()
        with connection, connection.makefile("rwb") as stream:
            try:
                for _ in range(2):
                    request = json.loads(stream.readline())
                    if request["method"] == "mining.authorize":
                        self.usernames.append(request["params"][0])
                    stream.write((json.dumps({"id": request["id"], "result": True,
                                              "error": None}) + "\n").encode())
                    stream.flush()
                for line in self.lines:
                    stream.write((line + "\n").encode())
                    stream.flush()
                while self.chatty and not self.stop.wait(1):
                    stream.write(b'{"id": null, "method": "mining.notify", "params": []}\n')
                    stream.flush()
            except OSError:
                pass

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
        self.server.close()


class AdmissionGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.bin = Path(self.tmp.name) / "bin"
        self.bin.mkdir()
        self.log = Path(self.tmp.name) / "psql.log"
        write_executable(self.bin / "qbit-prism-server", FAKE_SERVER)
        write_executable(self.bin / "psql", FAKE_PSQL)

    def run_gate(self, **env):
        base = {key: value for key, value in os.environ.items()
                if not key.startswith(("PRISM_", "FAKE_", "PG", "QBIT_"))}
        base.update(PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
                    PRISM_DATABASE_URL=DSN, PRISM_OPERATOR_BEARER_TOKEN=TOKEN,
                    FAKE_PSQL_LOG=str(self.log))
        base.update({key: value for key, value in env.items() if value is not None})
        for key, value in env.items():
            if value is None:
                base.pop(key, None)
        run = subprocess.run(["bash", str(GATE)], env=base, capture_output=True, text=True,
                             timeout=60)
        for secret in (PASSWORD, "s3cret-p%40ss", TOKEN, STANDBY_PASSWORD):
            self.assertNotIn(secret, run.stdout + run.stderr)
        return run

    def psql_calls(self):
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def assert_fails_with(self, line, **env):
        run = self.run_gate(**env)
        self.assertEqual(run.returncode, 1, run.stdout)
        self.assertIn(line, run.stdout)
        self.assertIn("ADMISSION GATE: FAIL", run.stdout)

    def test_every_fact_passing_admits(self):
        run = self.run_gate()
        self.assertEqual(run.returncode, 0, run.stdout)
        for line in ("PASS  check-config",
                     "INFO  check-config WARNING: block submission is disabled",
                     "PASS  heartbeat census read: [{\"id\": \"frontend-a\"",
                     "INFO  HA: 2 live, 0 stale, 0 inactive, 0 unknown (freshness 15s)",
                     "INFO  submission hold: {\"held\": true",
                     "INFO  audit completeness: 0 legacy rows pending",
                     "PASS  native audit rows complete",
                     "PASS  carry-forward integrity (mismatch 0, drift 0)",
                     "PASS  durability: fsync=on full_page_writes=on synchronous_commit=on",
                     "INFO  offer standby not configured",
                     "PASS  healthcheck (frontend ready on the observed tip)",
                     "INFO  highdiff not configured on this host",
                     "ADMISSION GATE: PASS"):
            self.assertIn(line, run.stdout)
        self.assertFalse([line for line in run.stdout.splitlines() if line.startswith("WARN")])

    def test_the_password_never_reaches_a_command_line(self):
        for url, expected in (
                (DSN, "postgres://gate-user@db.example.invalid/prism?sslmode=verify-full"),
                ("postgresql://gate-user@db.example.invalid/prism?password=s3cret-p%40ss"
                 "&sslmode=require",
                 "postgresql://gate-user@db.example.invalid/prism?sslmode=require")):
            with self.subTest(url=url):
                self.log.unlink(missing_ok=True)
                run = self.run_gate(PRISM_DATABASE_URL=url)
                self.assertEqual(run.returncode, 0, run.stdout)
                calls = self.psql_calls()
                self.assertTrue(calls)
                for call in calls:
                    self.assertEqual(call["argv"][0], expected)
                    self.assertNotIn(PASSWORD, json.dumps(call["argv"]))
                    self.assertEqual(call["pgpassword"], PASSWORD)

    def test_a_url_without_a_password_leaves_pgpassword_alone(self):
        run = self.run_gate(PRISM_DATABASE_URL="postgres://gate-user@db.example.invalid/prism")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertTrue(all(call["pgpassword"] is None for call in self.psql_calls()))

    def test_a_url_that_is_not_postgres_is_refused(self):
        run = self.run_gate(PRISM_DATABASE_URL="mysql://gate-user:pw@db.example.invalid/prism")
        self.assertEqual(run.returncode, 2)
        self.assertIn("must be a postgres:// or postgresql:// URL", run.stderr)

    def test_pending_legacy_rows_are_reported_not_failed(self):
        run = self.run_gate(FAKE_COMPLETENESS="9531|0")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("INFO  audit completeness: 9531 legacy rows pending", run.stdout)
        self.assertIn("INFO  import-audits: 1 session(s), last statement 3s ago", run.stdout)

    def test_a_missing_import_session_is_a_warning(self):
        run = self.run_gate(FAKE_COMPLETENESS="9531|0", FAKE_IMPORT="0|-|0")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("WARN  no import-audits session found", run.stdout)
        run = self.run_gate(FAKE_COMPLETENESS="9531|0", FAKE_IMPORT="0|-|4")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("INFO  import-audits progress not visible to this role", run.stdout)
        run = self.run_gate(FAKE_COMPLETENESS="9531|0", FAKE_PSQL_FAIL="FROM pg_stat_activity")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("WARN  could not read pg_stat_activity", run.stdout)

    def test_the_census_warns_as_self_check_does(self):
        run = self.run_gate(FAKE_CENSUS='1|1|0|0|[]')
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("INFO  HA: 1 live, 1 stale, 0 inactive, 0 unknown", run.stdout)
        self.assertIn("WARN  Fewer than two live frontends observed", run.stdout)
        run = self.run_gate(FAKE_CENSUS='2|0|0|1|[]')
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("WARN  Unrecognized or future-dated heartbeats; HA is unknown", run.stdout)
        run = self.run_gate(PRISM_HEALTH_REFRESH_SECONDS="09")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn("(freshness 27s)", run.stdout)
        run = self.run_gate(PRISM_HEALTH_REFRESH_SECONDS="10")
        self.assertIn("(freshness 30s)", run.stdout)
        self.assertTrue(any(["-v", "fresh=30"] == call["argv"][i:i + 2]
                            for call in self.psql_calls() for i in range(len(call["argv"]))))

    def test_incomplete_native_rows_fail(self):
        self.assert_fails_with("FAIL  2 native audit rows are incomplete",
                               FAKE_COMPLETENESS="5|2")

    def test_a_carry_forward_mismatch_or_drift_fails(self):
        self.assert_fails_with("FAIL  carry-forward integrity: mismatch_count=1 "
                               "current_drift_count=0", FAKE_INTEGRITY="1|0")
        self.assert_fails_with("current_drift_count=3", FAKE_INTEGRITY="0|3")

    def test_durability_off_fails(self):
        self.assert_fails_with(
            "FAIL  durability: fsync=on full_page_writes=on synchronous_commit=off",
            FAKE_DURABILITY="fsync=on full_page_writes=on synchronous_commit=off")

    def test_the_offer_standby_must_be_exactly_one_flushing_readable_standby(self):
        run = self.run_gate(PRISM_OFFER_STANDBY_APPLICATION_NAME=" pair-b ")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("PASS  offer standby usable", run.stdout)
        self.assertTrue(any("app=pair-b" in call["argv"] for call in self.psql_calls()))
        for answer in ("true|0|0", "true|2|2", "true|1|0", "false|1|1"):
            with self.subTest(answer=answer):
                self.assert_fails_with(
                    f"FAIL  offer standby not usable (readable|standbys|flushing = {answer})",
                    PRISM_OFFER_STANDBY_APPLICATION_NAME="pair-b", FAKE_STANDBY=answer)

    def test_a_zero_flush_wait_turns_the_offer_standby_off(self):
        for wait in ("0", " 0 ", "00"):
            with self.subTest(wait=wait):
                run = self.run_gate(PRISM_OFFER_STANDBY_APPLICATION_NAME="pair-b",
                                    PRISM_OFFER_STANDBY_FLUSH_WAIT_MS=wait,
                                    FAKE_STANDBY="true|0|0")
                self.assertEqual(run.returncode, 0, run.stdout)
                self.assertIn("INFO  offer standby not configured", run.stdout)
        self.assert_fails_with("FAIL  offer standby not usable",
                               PRISM_OFFER_STANDBY_APPLICATION_NAME="pair-b",
                               PRISM_OFFER_STANDBY_FLUSH_WAIT_MS="250", FAKE_STANDBY="true|0|0")

    def test_an_unready_frontend_fails(self):
        self.assert_fails_with("FAIL  healthcheck: Error: PRISM health is not ready",
                               FAKE_HEALTH_FAIL="1")

    def test_a_refused_configuration_fails(self):
        self.assert_fails_with("FAIL  check-config: Error: PRISM_FEE_PERCENT leaves dust",
                               FAKE_CHECK_CONFIG_FAIL="1")

    def test_a_failed_query_fails_without_showing_the_dsn(self):
        for needle, line in (
                ("FROM qbit_prism_instances", "FAIL  heartbeat census read failed"),
                ("FROM qbit_pool_audit_bundles a", "FAIL  audit completeness read failed"),
                ("qbit_carry_forward_integrity_report",
                 "FAIL  carry-forward integrity report failed"),
                ("FROM pg_settings", "FAIL  durability read failed")):
            with self.subTest(query=needle):
                self.assert_fails_with(line, FAKE_PSQL_FAIL=needle)
        self.assert_fails_with("readable|standbys|flushing = error",
                               PRISM_OFFER_STANDBY_APPLICATION_NAME="pair-b",
                               FAKE_PSQL_FAIL="pg_stat_replication")

    def test_every_query_runs_under_timeouts(self):
        # The integrity report gets 120 s; every other read 15 s; every one a 5 s lock wait.
        run = self.run_gate()
        self.assertEqual(run.returncode, 0, run.stdout)
        calls = self.psql_calls()
        self.assertTrue(calls)
        for call in calls:
            query = call["query"]
            self.assertIn("SET default_transaction_read_only = on;", query)
            self.assertIn("SET lock_timeout = '5s';", query)
            limit = "120s" if "qbit_carry_forward_integrity_report" in query else "15s"
            self.assertIn(f"SET statement_timeout = '{limit}';", query)

    def report_calls(self):
        return [call for call in self.psql_calls()
                if "qbit_carry_forward_integrity_report" in call["query"]]

    def standby_gate(self, **env):
        return self.run_gate(PRISM_INTEGRITY_REPORT_DATABASE_URL=STANDBY_DSN, **env)

    def test_the_integrity_report_can_run_on_a_standby(self):
        run = self.standby_gate()
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("INFO  integrity report on a standby: replayed to 0/3000148 (writer at "
                      "0/3000200), replay state 'paused', last replayed transaction 3s ago",
                      run.stdout)
        self.assertIn("PASS  carry-forward integrity on the standby (mismatch 0, drift 0)",
                      run.stdout)
        self.assertFalse([line for line in run.stdout.splitlines() if line.startswith("WARN")])
        report = self.report_calls()
        self.assertEqual([call["argv"][0] for call in report], [STANDBY_URL])
        self.assertEqual(report[0]["pgpassword"], STANDBY_PASSWORD)
        self.assertIn("SET statement_timeout = '600s';", report[0]["query"])
        writer = [call for call in self.psql_calls() if call["argv"][0] != STANDBY_URL]
        self.assertTrue(any("synchronous_standby_names" in call["query"] for call in writer))
        self.assertTrue(all(call["pgpassword"] == PASSWORD for call in writer))
        self.assertFalse(any(STANDBY_PASSWORD in argument
                             for call in self.psql_calls() for argument in call["argv"]))

    def test_a_standby_url_without_a_password_does_not_borrow_the_writers(self):
        run = self.run_gate(
            PRISM_INTEGRITY_REPORT_DATABASE_URL="postgres://reader@standby.example.invalid/prism")
        self.assertEqual(run.returncode, 0, run.stdout)
        standby = [call for call in self.psql_calls()
                   if call["argv"][0] == "postgres://reader@standby.example.invalid/prism"]
        self.assertEqual(len(standby), 2)
        self.assertTrue(all(call["pgpassword"] is None for call in standby))

    def test_the_integrity_standby_must_be_fit_for_the_report(self):
        for recovery, line in (
                ("false|-|-|-|false|off|30s|30s|" + ID,
                 "FAIL  PRISM_INTEGRITY_REPORT_DATABASE_URL is not a hot standby "
                 "(in recovery: false)"),
                ("true|0/3000148|paused|3|false|off|30s|30s|7000000000000000002|prism",
                 "FAIL  the integrity standby is not a replica of this writer's database"),
                ("true|0/3000148|paused|3|false|off|30s|30s|7000000000000000001|staging",
                 "FAIL  the integrity standby is not a replica of this writer's database"),
                ("true|0/3000148|paused|3|false|on|30s|30s|" + ID,
                 "FAIL  the integrity standby has hot_standby_feedback on"),
                ("true|-|paused|-|false|off|30s|30s|" + ID,
                 "FAIL  the integrity standby has replayed no transaction yet"),
                ("true|0/3000148|paused|900|false|off|30s|30s|" + ID,
                 "FAIL  the integrity standby's last replayed transaction is 900s old "
                 "(limit 300s) and it is behind the writer")):
            with self.subTest(recovery=recovery):
                self.log.unlink(missing_ok=True)
                self.assert_fails_with(line, PRISM_INTEGRITY_REPORT_DATABASE_URL=STANDBY_DSN,
                                       FAKE_RECOVERY=recovery)
                self.assertFalse(self.report_calls())

    def test_a_caught_up_standby_of_an_idle_writer_is_fresh(self):
        # Replay has reached the writer's position: an old last transaction only means an
        # idle writer, and a caught-up standby that never replayed one is current too.
        for recovery, shown in (("true|0/3000200|paused|900|true|off|30s|30s|" + ID, "900s ago"),
                                ("true|0/3000200|paused|-|true|off|30s|30s|" + ID, "none")):
            with self.subTest(shown=shown):
                run = self.standby_gate(FAKE_RECOVERY=recovery)
                self.assertEqual(run.returncode, 0, run.stdout)
                self.assertIn(f"last replayed transaction {shown}", run.stdout)
                self.assertIn("PASS  carry-forward integrity on the standby", run.stdout)
        probe = [call for call in self.psql_calls() if "pg_is_in_recovery" in call["query"]]
        self.assertTrue(probe)
        self.assertIn("writer_lsn=0/3000200", probe[-1]["argv"])

    def test_the_age_limit_can_be_raised(self):
        run = self.standby_gate(
            FAKE_RECOVERY="true|0/3000148|paused|900|false|off|30s|30s|" + ID,
            PRISM_INTEGRITY_REPORT_MAX_AGE_SECONDS="1200")
        self.assertEqual(run.returncode, 0, run.stdout)

    def test_an_unusable_age_limit_is_refused_whatever_the_route(self):
        for value in ("5m", "0", "99999999999999999999"):
            with self.subTest(value=value):
                run = self.run_gate(PRISM_INTEGRITY_REPORT_MAX_AGE_SECONDS=value)
                self.assertEqual(run.returncode, 2, run.stdout)
                self.assertIn("PRISM_INTEGRITY_REPORT_MAX_AGE_SECONDS must be a whole number of "
                              "seconds", run.stderr)

    def test_a_blank_standby_url_counts_as_unset(self):
        run = self.run_gate(PRISM_INTEGRITY_REPORT_DATABASE_URL="  ")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("PASS  carry-forward integrity (mismatch 0, drift 0)", run.stdout)

    def test_a_standby_that_is_not_yet_paused_is_a_warning(self):
        for state in ("not paused", "pause requested"):
            with self.subTest(state=state):
                run = self.standby_gate(
                    FAKE_RECOVERY=f"true|0/3000148|{state}|1|false|off|30s|30s|" + ID)
                self.assertEqual(run.returncode, 0, run.stdout)
                self.assertIn(f"WARN  the integrity standby's replay state is '{state}', "
                              "not 'paused'", run.stdout)
                self.assertIn("PASS  carry-forward integrity on the standby", run.stdout)
        # Infinite conflict delays are the documented alternative to a pause.
        run = self.standby_gate(FAKE_RECOVERY="true|0/3000148|not paused|1|false|off|-1|-1|" + ID)
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertFalse([line for line in run.stdout.splitlines() if line.startswith("WARN")])

    def test_a_writer_waiting_for_apply_is_refused(self):
        self.assert_fails_with("FAIL  the writer waits for standby apply "
                               "(synchronous_commit=remote_apply)",
                               PRISM_INTEGRITY_REPORT_DATABASE_URL=STANDBY_DSN,
                               FAKE_WRITER="remote_apply|FIRST 1 (prism_standby_1)|0/3000200|" + ID)
        self.assertFalse(self.report_calls())
        # Without synchronous standbys, remote_apply waits for nothing.
        run = self.standby_gate(FAKE_WRITER="remote_apply||0/3000200|" + ID)
        self.assertEqual(run.returncode, 0, run.stdout)

    def test_a_cancelled_report_is_named_without_the_server_details(self):
        for stderr, reason in (
                ("ERROR:  canceling statement due to conflict with recovery\n"
                 "DETAIL:  User query might have needed to see row versions that must be "
                 "removed. (standby.example.invalid)",
                 "(cancelled by a recovery conflict: pause the standby's replay or set "
                 "max_standby_streaming_delay = -1 there)"),
                ("ERROR:  canceling statement due to statement timeout "
                 "(standby.example.invalid)", "(statement timeout)")):
            with self.subTest(reason=reason):
                run = self.standby_gate(FAKE_PSQL_FAIL="qbit_carry_forward_integrity_report",
                                        FAKE_PSQL_STDERR=stderr)
                self.assertEqual(run.returncode, 1, run.stdout)
                self.assertIn(f"FAIL  carry-forward integrity report failed on the standby {reason}",
                              run.stdout)
                self.assertNotIn("standby.example.invalid", run.stdout + run.stderr)
        timeout = "ERROR:  canceling statement due to statement timeout"
        self.assert_fails_with("FAIL  carry-forward integrity report failed (statement timeout)",
                               FAKE_PSQL_FAIL="qbit_carry_forward_integrity_report",
                               FAKE_PSQL_STDERR=timeout)
        self.assert_fails_with("FAIL  heartbeat census read failed (statement timeout)",
                               FAKE_PSQL_FAIL="FROM qbit_prism_instances", FAKE_PSQL_STDERR=timeout)

    def test_the_integrity_check_can_be_skipped_and_says_so(self):
        run = self.run_gate(PRISM_GATE_INTEGRITY="skip")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("WARN  carry-forward integrity skipped (PRISM_GATE_INTEGRITY=skip)",
                      run.stdout)
        self.assertIn("ADMISSION GATE: PASS (legacy audit import pending, reported above) "
                      "(carry-forward integrity skipped)", run.stdout)
        self.assertFalse(self.report_calls())
        run = self.standby_gate(PRISM_GATE_INTEGRITY="skip")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("WARN  PRISM_INTEGRITY_REPORT_DATABASE_URL is set but not used", run.stdout)
        self.assertFalse(self.report_calls())

    def test_an_unknown_integrity_mode_fails(self):
        self.assert_fails_with("FAIL  PRISM_GATE_INTEGRITY must be empty or skip",
                               PRISM_GATE_INTEGRITY="off")
        self.assertFalse(self.report_calls())

    def test_an_integrity_url_that_is_not_postgres_is_refused(self):
        run = self.run_gate(PRISM_INTEGRITY_REPORT_DATABASE_URL="mysql://reader@standby/prism")
        self.assertEqual(run.returncode, 2)
        self.assertIn("PRISM_INTEGRITY_REPORT_DATABASE_URL must be a postgres:// or "
                      "postgresql:// URL", run.stderr)

    def test_the_frontend_environment_is_required(self):
        run = self.run_gate(PRISM_DATABASE_URL=None)
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("export the frontend environment first", run.stderr)
        self.assertNotIn("ADMISSION GATE", run.stdout)

    def probe(self, listener, **env):
        self.addCleanup(listener.close)
        run = self.run_gate(PRISM_STRATUM_HIGHDIFF_BIND="127.0.0.1",
                            PRISM_STRATUM_HIGHDIFF_PORT=str(listener.port),
                            PRISM_STRATUM_HIGHDIFF_MIN_DIFF="500000", **env)
        listener.close()
        return run

    def test_the_highdiff_floor_is_probed_as_self_check_probes_it(self):
        for difficulty, passes in ((600000, True), (500000, True), (1000, False)):
            with self.subTest(difficulty=difficulty):
                listener = FakeStratum(difficulty)
                run = self.probe(listener, PRISM_SELF_CHECK_ADDRESS="qb1zselfcheck")
                self.assertEqual(listener.usernames, ["qb1zselfcheck"])
                if passes:
                    self.assertEqual(run.returncode, 0, run.stdout)
                    self.assertIn(f"PASS  highdiff first difficulty {float(difficulty)} >= "
                                  "500000.0", run.stdout)
                else:
                    self.assertEqual(run.returncode, 1, run.stdout)
                    self.assertIn("FAIL  highdiff probe: 1000.0 < 500000.0", run.stdout)

    def test_the_highdiff_probe_authorizes_as_self_check_does(self):
        cases = (
            ({"PRISM_USERNAME_FALLBACK_ADDRESS": "qb1zfallback",
              "PRISM_POOL_FEE_ADDRESS": "qb1zfee"}, "qb1zfallback"),
            ({"PRISM_POOL_FEE_ADDRESS": "qb1zfee"}, "qb1zfee"),
            ({"QBIT_CHAIN": "testnet4", "PRISM_POOL_FEE_ADDRESS": "qb1zfee"}, TESTNET_FALLBACK),
            ({}, "qb1zrecentminer"),
            ({"PRISM_SELF_CHECK_ADDRESS": " qb1zspaced ", "PRISM_POOL_FEE_ADDRESS": "qb1zfee"},
             " qb1zspaced "),
            ({"PRISM_SELF_CHECK_ADDRESS": "   ", "PRISM_USERNAME_FALLBACK_ADDRESS": " "},
             "qb1zrecentminer"),
        )
        for env, username in cases:
            with self.subTest(env=env):
                listener = FakeStratum(600000)
                run = self.probe(listener, **env)
                self.assertEqual(run.returncode, 0, run.stdout)
                self.assertEqual(listener.usernames, [username])

    def test_an_empty_pool_without_an_address_fails_as_self_check_does(self):
        listener = FakeStratum(600000)
        run = self.probe(listener, FAKE_RECENT_MINER="")
        self.assertEqual(run.returncode, 1, run.stdout)
        self.assertIn("FAIL  highdiff probe: set PRISM_SELF_CHECK_ADDRESS", run.stdout)

    def test_highdiff_is_on_only_with_a_port(self):
        run = self.run_gate(PRISM_STRATUM_HIGHDIFF_BIND="0.0.0.0",
                            PRISM_STRATUM_HIGHDIFF_PORT="  ")
        self.assertEqual(run.returncode, 0, run.stdout)
        self.assertIn("INFO  highdiff not configured on this host", run.stdout)

    def test_the_probe_refuses_what_self_check_refuses(self):
        for name, line in (
                ("a string difficulty",
                 '{"id": null, "method": "mining.set_difficulty", "params": ["600000"]}'),
                ("an infinite difficulty",
                 '{"id": null, "method": "mining.set_difficulty", "params": [Infinity]}'),
                ("a zero difficulty",
                 '{"id": null, "method": "mining.set_difficulty", "params": [0]}')):
            with self.subTest(name):
                run = self.probe(FakeStratum(lines=[line]), PRISM_SELF_CHECK_ADDRESS="qb1z")
                self.assertEqual(run.returncode, 1, run.stdout)
                self.assertIn("FAIL  highdiff probe: invalid advertised difficulty", run.stdout)
        run = self.probe(FakeStratum(lines=['{"id": 3, "result": null, "error": false}']),
                         PRISM_SELF_CHECK_ADDRESS="qb1z")
        self.assertEqual(run.returncode, 1, run.stdout)
        self.assertIn("FAIL  highdiff probe: Stratum probe rejected: False", run.stdout)

    def test_the_probe_gives_up_after_15_seconds_in_total(self):
        started = time.monotonic()
        run = self.probe(FakeStratum(lines=[], chatty=True), PRISM_SELF_CHECK_ADDRESS="qb1z")
        self.assertLess(time.monotonic() - started, 30)
        self.assertEqual(run.returncode, 1, run.stdout)
        self.assertIn("FAIL  highdiff probe: Stratum difficulty probe timed out", run.stdout)


class RealSchemaTests(unittest.TestCase):
    """The gate's SQL against a database `qbit-prism-server migrate` created."""

    @classmethod
    def setUpClass(cls):
        if TEST_DATABASE_URL and SERVER_BIN:
            return
        # Like the gated Rust tests: where integration is required, a missing input fails.
        if os.environ.get("PRISM_TEST_REQUIRE_INTEGRATION") == "1":
            raise AssertionError("PRISM_TEST_REQUIRE_INTEGRATION=1 but PRISM_TEST_DATABASE_URL "
                                 "or PRISM_ADMISSION_GATE_SERVER_BIN is unset")
        raise unittest.SkipTest("set PRISM_TEST_DATABASE_URL and PRISM_ADMISSION_GATE_SERVER_BIN")

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.bin = Path(self.tmp.name) / "bin"
        self.bin.mkdir()
        write_executable(self.bin / "qbit-prism-server", FAKE_SERVER)
        pg_bin = os.environ.get("PRISM_TEST_PG_BIN_DIR")
        self.psql = str(Path(pg_bin) / "psql") if pg_bin else shutil.which("psql")
        self.assertIsNotNone(self.psql, "psql is required")
        name = f"prism_admission_gate_{uuid.uuid4().hex[:12]}"
        self.sql(TEST_DATABASE_URL, f"CREATE DATABASE {name}")
        self.addCleanup(self.sql, TEST_DATABASE_URL, f"DROP DATABASE IF EXISTS {name}")
        parts = urllib.parse.urlsplit(TEST_DATABASE_URL)
        self.url = urllib.parse.urlunsplit(parts._replace(path=f"/{name}"))
        migrate = subprocess.run([SERVER_BIN, "migrate"], capture_output=True, text=True,
                                 env={"PATH": os.environ["PATH"],
                                      "HOME": os.environ.get("HOME", "/"),
                                      "PRISM_DATABASE_URL": self.url}, timeout=600)
        self.assertEqual(migrate.returncode, 0, migrate.stderr)

    def sql(self, url, statement):
        run = subprocess.run([self.psql, url, "-XAtq", "-v", "ON_ERROR_STOP=1",
                              "-c", "SET session_replication_role = replica",
                              "-c", statement], capture_output=True, text=True, timeout=120)
        self.assertEqual(run.returncode, 0, run.stderr)

    def run_gate(self, **extra):
        env = {key: value for key, value in os.environ.items() if not key.startswith("PRISM_")}
        env.update(PATH=f"{self.bin}{os.pathsep}{Path(self.psql).parent}{os.pathsep}"
                        f"{os.environ['PATH']}",
                   PRISM_DATABASE_URL=self.url, **extra)
        return subprocess.run(["bash", str(GATE)], env=env, capture_output=True, text=True,
                              timeout=120)

    def test_the_gate_reads_a_migrated_schema(self):
        # The body-present CHECK needs audit_bundle or body_uri; replica role
        # skips the block foreign key these bare rows would need.
        self.sql(self.url, "INSERT INTO qbit_pool_audit_bundles"
                           "(block_hash, audit_bundle_sha256, coinbase_tx_hex, body_uri) VALUES "
                           "(repeat('a', 64), repeat('1', 64), '00', '/audit/a.json'), "
                           "(repeat('b', 64), repeat('2', 64), '00', '/audit/b.json')")
        self.sql(self.url, "INSERT INTO qbit_prism_instances(instance_id, status) VALUES "
                           "('frontend-a', '{\"schema\": \"qbit.prism.audit-health.v1\", "
                           "\"ready\": true}'), ('frontend-b', '{\"state\": \"starting\", "
                           "\"candidate_offer_lifecycle\": 1}')")
        run = self.run_gate()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        for line in ("PASS  heartbeat census read: [{\"id\": \"frontend-a\"",
                     "INFO  HA: 1 live, 0 stale, 1 inactive, 0 unknown (freshness 15s)",
                     "WARN  Fewer than two live frontends observed",
                     "INFO  audit completeness: 2 legacy rows pending",
                     "WARN  no import-audits session found",
                     "PASS  native audit rows complete",
                     "PASS  carry-forward integrity (mismatch 0, drift 0)",
                     "PASS  durability: fsync=on full_page_writes=on",
                     "ADMISSION GATE: PASS"):
            self.assertIn(line, run.stdout)

        # A native row whose snapshot is missing, and one with a snapshot but
        # no object body: self-check counts both as incomplete.
        self.sql(self.url, "INSERT INTO qbit_prism_audit_snapshots"
                           "(snapshot_sha256, first_share_seq, last_share_seq, anchor_ms, "
                           "share_count) VALUES (repeat('e', 64), 1, 1, 0, 1)")
        self.sql(self.url, "INSERT INTO qbit_pool_audit_bundles"
                           "(block_hash, audit_bundle_sha256, coinbase_tx_hex, audit_bundle, "
                           "body_uri, share_snapshot_sha256) VALUES "
                           "(repeat('c', 64), repeat('3', 64), '00', '{}', NULL, repeat('f', 64)), "
                           "(repeat('d', 64), repeat('4', 64), '00', NULL, '/audit/d.json', "
                           "repeat('e', 64))")
        run = self.run_gate()
        self.assertEqual(run.returncode, 1, run.stdout + run.stderr)
        self.assertIn("FAIL  2 native audit rows are incomplete", run.stdout)
        self.assertIn("INFO  audit completeness: 2 legacy rows pending", run.stdout)

    def test_a_primary_is_refused_as_the_integrity_standby(self):
        # The recovery probe must run on a primary too: pg_get_wal_replay_pause_state() raises
        # there, so it sits behind pg_is_in_recovery(), and the writer-position comparison and
        # the identity read must parse.
        run = self.run_gate(PRISM_INTEGRITY_REPORT_DATABASE_URL=self.url)
        self.assertEqual(run.returncode, 1, run.stdout + run.stderr)
        self.assertIn("FAIL  PRISM_INTEGRITY_REPORT_DATABASE_URL is not a hot standby "
                      "(in recovery: false)", run.stdout)
        self.assertNotIn("carry-forward integrity (mismatch", run.stdout)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Bounded #281 preparation and disposable functional exercise (never qualification)."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ("prism-coordinator", "prism-coordinator-2")


def fixture_environment():
    # Compose must not read a real .env or inherit credential-bearing overrides.
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "PRISM_HA_INSTANCE_ID_1": "qual-east-281",
        "PRISM_HA_INSTANCE_ID_2": "qual-west-281",
        "PRISM_HA_RPC_HOST_1": "node-east.invalid",
        "PRISM_HA_RPC_HOST_2": "node-west.invalid",
        "PRISM_HA_RPC_URL_1": "http://node-east.invalid:19452/",
        "PRISM_HA_RPC_URL_2": "http://node-west.invalid:19452/",
        "PRISM_HA_AUDIT_BIND": "0.0.0.0",
        "PRISM_HA_AUDIT_PORT": "18441",
        "PRISM_HA_STRATUM_PORT_HOST_1": "127.0.0.1:18440",
        "PRISM_HA_STRATUM_PORT_HOST_2": "127.0.0.1:18443",
        "PRISM_HA_HEALTH_PORT_HOST_1": "127.0.0.1:18441",
        "PRISM_HA_HEALTH_PORT_HOST_2": "127.0.0.1:18444",
        "PRISM_HA_HIGHDIFF_PORT_HOST_1": "127.0.0.1:18442",
        "PRISM_HA_HIGHDIFF_PORT_HOST_2": "127.0.0.1:18445",
        "PRISM_STRATUM_HIGHDIFF_PORT": "18446",
        "PRISM_DATABASE_URL": "postgresql://fixture:fixture@writer.invalid:5432/fixture",
        "PRISM_PUBLIC_DATABASE_URL": "postgresql://fixture:fixture@public-reader.invalid:5432/fixture",
        "PRISM_PUBLIC_REPLICA_MODE": "require",
    }


def check_render(document, expected):
    """Return only allowlisted nonsecret observations; never echo resolved config."""
    services = document["services"]
    result = {}
    for index, name in enumerate(FRONTENDS, 1):
        service = services[name]
        env = service["environment"]
        for consumer, input_name in (
            ("PRISM_INSTANCE_ID", f"PRISM_HA_INSTANCE_ID_{index}"),
            ("QBIT_RPC_URL", f"PRISM_HA_RPC_URL_{index}"),
            ("PRISM_AUDIT_PORT", "PRISM_HA_AUDIT_PORT"),
            ("PRISM_AUDIT_BIND", "PRISM_HA_AUDIT_BIND"),
            ("PRISM_DATABASE_URL", "PRISM_DATABASE_URL"),
            ("PRISM_STRATUM_HIGHDIFF_PORT", "PRISM_STRATUM_HIGHDIFF_PORT"),
        ):
            if env[consumer] != expected[input_name]:
                raise ValueError(f"{name}: effective {consumer} differs from fixture")
        ports = service["ports"]
        expected_ports = {
            ("127.0.0.1", int(env["PRISM_STRATUM_PORT"]), str(18440 if index == 1 else 18443)),
            ("127.0.0.1", 18441, str(18441 if index == 1 else 18444)),
            ("127.0.0.1", 18446, str(18442 if index == 1 else 18445)),
        }
        observed_ports = {(p.get("host_ip"), p["target"], str(p["published"])) for p in ports}
        if expected_ports != observed_ports:
            raise ValueError(f"{name}: expected loopback ports did not reach render")
        result[name] = {"instance_id": env["PRISM_INSTANCE_ID"], "fixture_rpc_matches": True,
                        "fixture_writer_matches": True, "loopback_ports_match": True}
    return result


def render():
    env = fixture_environment()
    process = subprocess.run([
        "docker", "compose", "--env-file", os.devnull, "-f", "compose.yaml",
        "-f", "compose.prism-ha.yaml", "--profile", "prism", "config", "--format", "json",
    ], cwd=ROOT, env=env, capture_output=True, text=True, timeout=30)
    if process.returncode:
        raise RuntimeError("Compose render failed; resolved configuration is deliberately withheld")
    return {"render": check_render(json.loads(process.stdout), env),
            "runtime_startup": "not executed", "live_acceptance": "not established"}


def cleanup_owned(root, pg_bin):
    """The wrapper's newly allocated, private parent is the ownership boundary."""
    entries = []
    for cluster_root in sorted(root.iterdir()):
        if cluster_root.is_symlink() or not cluster_root.is_dir() or not cluster_root.name.startswith("prism-load-"):
            raise RuntimeError("unexpected entry in owned resource directory; retained for inspection")
        for name in ("standby", "primary"):
            data = cluster_root / name
            if data.is_symlink():
                raise RuntimeError("owned data path became a symlink; refusing cleanup")
            if not data.exists():
                continue
            pg = [str(pg_bin / "pg_ctl"), "-D", str(data)]
            # initdb may fail before PG_VERSION; no postmaster could have started.
            if not (data / "PG_VERSION").exists():
                if (data / "postmaster.pid").exists():
                    raise RuntimeError("partial cluster has an unexplained postmaster; retained")
                entries.append({"data": str(data), "state": "initialization incomplete, no postmaster"})
                continue
            status = subprocess.run(pg + ["status"], capture_output=True, timeout=20).returncode
            if status == 0:
                subprocess.run(pg + ["-m", "immediate", "-w", "-t", "15", "stop"],
                               capture_output=True, timeout=20, check=True)
                status = subprocess.run(pg + ["status"], capture_output=True, timeout=20).returncode
            if status != 3 or (data / "postmaster.pid").exists():
                raise RuntimeError("owned PostgreSQL exit is unverified; retained data directory")
            entries.append({"data": str(data), "state": "stopped"})
    shutil.rmtree(root)
    return {"remaining_clusters": entries, "owned_parent_removed": not root.exists()}


def run(args):
    for binary in (args.example_bin, args.runtime_tests, args.ack_tests, args.pg_bin_dir / "pg_ctl"):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("an explicitly supplied executable is unavailable")
    if args.out.exists():
        raise ValueError("output directory must be new")
    executable_hashes = {}
    for name, binary in (("example", args.example_bin), ("runtime_tests", args.runtime_tests), ("ack_tests", args.ack_tests)):
        digest = hashlib.sha256()
        with binary.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        executable_hashes[name] = digest.hexdigest()
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                          capture_output=True, text=True, check=True).stdout.strip()
    dirty = bool(subprocess.run(["git", "status", "--porcelain"], cwd=ROOT,
                               capture_output=True, text=True, check=True).stdout)
    args.out.mkdir()
    # Short paths keep the managed driver's Unix socket below PG's path limit.
    root = Path(tempfile.mkdtemp(prefix="b281-", dir="/tmp"))
    command = [str(args.example_bin), "--pg-bin-dir", str(args.pg_bin_dir),
               "--runtime-tests", str(args.runtime_tests), "--ack-tests", str(args.ack_tests),
               "--out", str(args.out / "functional")]
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "TMPDIR": str(root), "LC_ALL": "C"}
    process = None
    status = None
    failure = None
    cleanup = None
    census = {
        "owned_parent": str(root), "process_pid": None, "process_exit": None,
        "containers_started": [], "external_databases_used": [],
        "checkout_at_invocation": {"head": head, "dirty": dirty},
        "executable_sha256": executable_hashes,
        "provenance_note": "hashes identify executed binaries; checkout SHA alone does not certify build provenance",
        "cleanup": {"owned_parent_removed": False, "state": "not yet verified"}, "failure": None,
    }
    try:
        (args.out / "resource-census.json").write_text(json.dumps(census, indent=2) + "\n")
        process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, start_new_session=True)
        census["process_pid"] = process.pid
        (args.out / "resource-census.json").write_text(json.dumps(census, indent=2) + "\n")
        try:
            stdout, stderr = process.communicate(timeout=480)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            # This process group was created by this invocation; never select by name.
            os.killpg(process.pid, signal.SIGTERM)
            try:
                stdout, stderr = process.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate(timeout=10)
            failure = "functional process exceeded deadline or was interrupted"
        status = process.returncode
        # Only fixture processes run with this cleared environment; no real DSNs.
        (args.out / "functional-process.log").write_text(stdout + stderr)
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
            status = process.returncode
        # Keep startup diagnostics before removing verified-stopped fixture data.
        # Every source is below this run's private parent, never a user database.
        try:
            for log in root.glob("prism-load-*/*.log"):
                if log.is_file() and not log.is_symlink() and not log.parent.is_symlink():
                    saved = args.out / "postgres-logs" / log.parent.name / log.name
                    saved.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(log, saved)
        except Exception:
            diagnostic_failure = "could not retain owned PostgreSQL diagnostics"
            failure = f"{failure}; {diagnostic_failure}" if failure else diagnostic_failure
        try:
            cleanup = cleanup_owned(root, args.pg_bin_dir)
        except Exception as error:
            cleanup = {"owned_parent_removed": False, "error": str(error), "retained_parent": str(root)}
        census.update(process_exit=status, cleanup=cleanup, failure=failure)
        (args.out / "resource-census.json").write_text(json.dumps(census, indent=2) + "\n")
    if status != 0 or failure or not cleanup["owned_parent_removed"]:
        raise RuntimeError("functional run failed or cleanup is unverified; inspect local evidence")
    evidence = json.loads((args.out / "functional" / "ha-functional.json").read_text())
    if evidence["result"] != "passed" or not evidence["cleanup"]["complete"]:
        raise RuntimeError("functional evidence is incomplete")
    return {"functional": "passed", "report": str(args.out / "functional" / "ha-functional.json"),
            "resource_census": str(args.out / "resource-census.json"),
            "live_acceptance": "not established"}


def terminate_as_interrupt(_signum, _frame):
    # Let run's existing interruption path stop only its own process group and
    # verify its exact PG directories. A repeated TERM must not interrupt cleanup.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("render", help="Check nondefault HA settings without starting services")
    functional = subcommands.add_parser("run", help="Only after coordinating a resource slot")
    for option in ("example-bin", "runtime-tests", "ack-tests", "pg-bin-dir", "out"):
        functional.add_argument(f"--{option}", type=lambda p: Path(p).resolve(), required=True)
    args = parser.parse_args()
    previous = signal.signal(signal.SIGTERM, terminate_as_interrupt)
    try:
        print(json.dumps(render() if args.command == "render" else run(args), indent=2))
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    main()

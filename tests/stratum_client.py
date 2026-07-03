#!/usr/bin/env python3
"""Small raw Stratum v1 client for ckpool integration probes.

The helper intentionally uses only the Python standard library so it can run in
minimal operator environments while the ckpool integration is still a spike.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone


DEFAULT_AGENT = "qbit-stratum-client/0.1"
DEFAULT_VERSION_MASK = "1fffe000"
DEFAULT_MALFORMED_USERNAMES = (
    "",
    "not-a-qbit-address",
    "qb1notavalidaddress",
    "tq1notavalidaddress",
    "bc1qwrongnetwork000000000000000000000000000000000",
    "worker.without.payout.address",
)


class StratumTimeout(TimeoutError):
    """Raised when no complete Stratum line arrives before the deadline."""


class StratumConnectionClosed(ConnectionError):
    """Raised when the peer closes the Stratum socket."""


class StratumProtocolError(RuntimeError):
    """Raised when the peer sends a non-JSON-object Stratum line."""


class StratumAssertionError(AssertionError):
    """Raised when a Stratum probe response is complete but incorrect."""


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def env_default(*names: str, default: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return default


def env_int_default(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def compact_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class Reporter:
    def __init__(self, *, jsonl: bool) -> None:
        self.jsonl = jsonl

    def emit(self, event: str, **fields: object) -> None:
        record = {"ts": utc_timestamp(), "event": event, **fields}
        if self.jsonl:
            print(compact_json(record), flush=True)
            return
        print(format_record(record), flush=True)


def format_record(record: dict[str, object]) -> str:
    timestamp = str(record["ts"])
    event = str(record["event"])

    if event == "connect":
        return f"{timestamp} connected {record['host']}:{record['port']}"
    if event == "close":
        return f"{timestamp} closed connection"
    if event == "send":
        return (
            f"{timestamp} -> {record['method']} id={record['id']} "
            f"params={compact_json(record.get('params', []))}"
        )
    if event == "notify":
        return (
            f"{timestamp} <- mining.notify job_id={record.get('job_id')} "
            f"clean_jobs={record.get('clean_jobs')} message={compact_json(record['message'])}"
        )
    if event == "recv":
        return f"{timestamp} <- {compact_json(record['message'])}"
    if event == "summary":
        return (
            f"{timestamp} summary notifies={record['notify_count']} "
            f"configure_id={record.get('configure_id')} "
            f"subscribe_id={record.get('subscribe_id')} authorize_id={record.get('authorize_id')}"
        )
    if event == "probe_result":
        status = "ACCEPTED" if record.get("accepted") else str(record.get("status", "rejected")).upper()
        return (
            f"{timestamp} {status} username={record['username']!r} "
            f"result={compact_json(record.get('result'))} error={compact_json(record.get('error'))}"
        )
    if event == "bip310_result":
        return f"{timestamp} BIP310 {str(record.get('status', 'unknown')).upper()} {record['message']}"
    if event == "timeout":
        return f"{timestamp} timeout {record['message']}"
    if event == "error":
        return f"{timestamp} error {record['message']}"

    details = {key: value for key, value in record.items() if key not in {"ts", "event"}}
    return f"{timestamp} {event} {compact_json(details)}"


class StratumClient:
    def __init__(self, host: str, port: int, *, connect_timeout: float, read_timeout: float = 1.0) -> None:
        self.host = host
        self.port = port
        self.socket = socket.create_connection((host, port), timeout=connect_timeout)
        self.socket.settimeout(read_timeout)
        self.read_timeout = read_timeout
        self.buffer = bytearray()
        self.next_request_id = 0

    def close(self) -> None:
        self.socket.close()

    def send_request(self, method: str, params: list[object]) -> int:
        self.next_request_id += 1
        request_id = self.next_request_id
        payload = {
            "id": request_id,
            "method": method,
            "params": params,
        }
        self.socket.sendall(compact_json(payload).encode("utf-8") + b"\n")
        return request_id

    def recv_message(self, deadline: float) -> dict[str, object]:
        while time.monotonic() < deadline:
            line = self._pop_line()
            if line is not None:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    message = json.loads(stripped.decode("utf-8"))
                except json.JSONDecodeError as exc:
                    raise StratumProtocolError(f"invalid JSON from Stratum peer: {exc.msg}") from exc
                if not isinstance(message, dict):
                    raise StratumProtocolError(f"expected Stratum object, got: {message!r}")
                return message

            remaining = max(0.01, min(self.read_timeout, deadline - time.monotonic()))
            self.socket.settimeout(remaining)
            try:
                chunk = self.socket.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                raise StratumConnectionClosed("Stratum peer closed the connection")
            self.buffer.extend(chunk)

        raise StratumTimeout("timed out waiting for a Stratum message")

    def _pop_line(self) -> bytes | None:
        newline_index = self.buffer.find(b"\n")
        if newline_index == -1:
            return None
        line = bytes(self.buffer[:newline_index])
        del self.buffer[: newline_index + 1]
        return line


@dataclass
class RequestIds:
    configure: int | None
    subscribe: int
    authorize: int


@dataclass
class ProbeResult:
    username: str
    status: str
    accepted: bool
    result: object | None
    error: object | None


def parse_key_value(value: str) -> tuple[str, str]:
    key, separator, parsed_value = value.partition("=")
    if not separator or not key:
        raise argparse.ArgumentTypeError("expected KEY=VALUE")
    return key, parsed_value


def configure_params(args: argparse.Namespace) -> list[object]:
    extensions = args.configure_extension or ["version-rolling"]
    extension_params: dict[str, object] = {}
    if args.version_mask:
        extension_params["version-rolling.mask"] = args.version_mask
    for key, value in args.configure_param or []:
        extension_params[key] = value
    return [extensions, extension_params]


def notify_fields(message: dict[str, object]) -> dict[str, object]:
    params = message.get("params")
    job_id: object | None = None
    clean_jobs: object | None = None
    if isinstance(params, list):
        if params:
            job_id = params[0]
        if len(params) >= 9:
            clean_jobs = params[8]
    return {"job_id": job_id, "clean_jobs": clean_jobs}


def emit_recv(reporter: Reporter, message: dict[str, object]) -> None:
    if message.get("method") == "mining.notify":
        reporter.emit("notify", message=message, **notify_fields(message))
        return
    reporter.emit("recv", message=message)


def send_and_report(
    client: StratumClient,
    reporter: Reporter,
    method: str,
    params: list[object],
    *,
    emit: bool,
) -> int:
    request_id = client.send_request(method, params)
    if emit:
        reporter.emit("send", id=request_id, method=method, params=params)
    return request_id


def response_for_request(
    client: StratumClient,
    reporter: Reporter,
    request_id: int,
    deadline: float,
    *,
    emit: bool,
) -> dict[str, object]:
    while True:
        message = client.recv_message(deadline)
        if emit:
            emit_recv(reporter, message)
        if message.get("id") == request_id:
            return message


def perform_handshake(
    client: StratumClient,
    reporter: Reporter,
    args: argparse.Namespace,
    *,
    emit: bool,
) -> RequestIds:
    configure_id = None
    if not args.no_configure:
        configure_id = send_and_report(
            client,
            reporter,
            "mining.configure",
            configure_params(args),
            emit=emit,
        )
    subscribe_id = send_and_report(
        client,
        reporter,
        "mining.subscribe",
        [args.agent],
        emit=emit,
    )
    authorize_id = send_and_report(
        client,
        reporter,
        "mining.authorize",
        [args.username, args.password],
        emit=emit,
    )
    return RequestIds(configure=configure_id, subscribe=subscribe_id, authorize=authorize_id)


def run_observe(args: argparse.Namespace) -> int:
    reporter = Reporter(jsonl=args.jsonl)
    deadline = time.monotonic() + args.timeout
    notify_count = 0

    try:
        client = StratumClient(args.host, args.port, connect_timeout=args.connect_timeout)
    except OSError as exc:
        reporter.emit("error", message=f"connect failed: {exc}")
        return 2

    reporter.emit("connect", host=args.host, port=args.port)
    try:
        request_ids = perform_handshake(client, reporter, args, emit=True)
        while notify_count < args.notify_count:
            message = client.recv_message(deadline)
            if message.get("method") == "mining.notify":
                notify_count += 1
            emit_recv(reporter, message)
    except StratumTimeout as exc:
        reporter.emit("timeout", message=f"{exc}; captured {notify_count} mining.notify message(s)")
        return 1
    except (OSError, StratumConnectionClosed, StratumProtocolError) as exc:
        reporter.emit("error", message=str(exc))
        return 2
    finally:
        client.close()
        reporter.emit("close")

    reporter.emit(
        "summary",
        notify_count=notify_count,
        configure_id=request_ids.configure,
        subscribe_id=request_ids.subscribe,
        authorize_id=request_ids.authorize,
    )
    return 0


def probe_one_username(args: argparse.Namespace, username: str, reporter: Reporter) -> ProbeResult:
    deadline = time.monotonic() + args.timeout
    client = StratumClient(args.host, args.port, connect_timeout=args.connect_timeout)
    try:
        probe_args = argparse.Namespace(**vars(args))
        probe_args.username = username
        request_ids = perform_handshake(client, reporter, probe_args, emit=args.verbose)

        while True:
            message = client.recv_message(deadline)
            if args.verbose:
                emit_recv(reporter, message)
            if message.get("id") != request_ids.authorize:
                continue

            result = message.get("result")
            error = message.get("error")
            accepted = result is True and error is None
            status = "accepted" if accepted else "rejected"
            return ProbeResult(username=username, status=status, accepted=accepted, result=result, error=error)
    except StratumTimeout:
        return ProbeResult(username=username, status="no_response", accepted=False, result=None, error="timeout")
    except (OSError, StratumConnectionClosed, StratumProtocolError) as exc:
        return ProbeResult(username=username, status="error", accepted=False, result=None, error=str(exc))
    finally:
        client.close()


def run_probe_malformed(args: argparse.Namespace) -> int:
    reporter = Reporter(jsonl=args.jsonl)
    usernames = args.malformed_username or list(DEFAULT_MALFORMED_USERNAMES)
    accepted = 0
    errors = 0

    for username in usernames:
        result = probe_one_username(args, username, reporter)
        if result.accepted:
            accepted += 1
        if result.status in {"error", "no_response"}:
            errors += 1
        reporter.emit(
            "probe_result",
            username=result.username,
            status=result.status,
            accepted=result.accepted,
            result=result.result,
            error=result.error,
        )

    if args.fail_on_accepted and accepted:
        return 1
    if errors:
        return 2
    return 0


def parse_mask(value: object, *, field: str) -> int:
    if not isinstance(value, str):
        raise StratumAssertionError(f"{field} is not a string: {value!r}")
    if len(value) != 8:
        raise StratumAssertionError(f"{field} is not an 8-character mask: {value!r}")
    try:
        return int(value, 16)
    except ValueError as exc:
        raise StratumAssertionError(f"{field} is not hexadecimal: {value!r}") from exc


def assert_configure_response(args: argparse.Namespace, result: object) -> None:
    if not isinstance(result, dict):
        raise StratumAssertionError(f"mining.configure result is not an object: {result!r}")

    requested_mask = parse_mask(args.version_mask, field="requested version mask")
    configured_mask = parse_mask(args.configured_version_mask, field="configured version mask")
    expected_mask = requested_mask & configured_mask
    expected_mask_hex = f"{expected_mask:08x}"

    for extension in ["version-rolling", "subscribe-extranonce", *args.unsupported_extension]:
        if extension not in result:
            raise StratumAssertionError(f"missing explicit result for requested extension {extension!r}")

    granted_mask_hex = result.get("version-rolling.mask")
    granted_mask = parse_mask(granted_mask_hex, field="granted version mask")
    if granted_mask != expected_mask:
        raise StratumAssertionError(
            f"version-rolling.mask was {granted_mask_hex!r}; expected {expected_mask_hex!r}",
        )
    if granted_mask & ~requested_mask:
        raise StratumAssertionError(
            f"version-rolling.mask {granted_mask_hex!r} grants bits outside requested mask {args.version_mask!r}",
        )

    expected_enabled = expected_mask != 0
    if result.get("version-rolling") is not expected_enabled:
        raise StratumAssertionError(
            f"version-rolling was {result.get('version-rolling')!r}; expected {expected_enabled!r}",
        )
    if result.get("subscribe-extranonce") is not False:
        raise StratumAssertionError("subscribe-extranonce must be explicitly false for ckpool")
    for extension in args.unsupported_extension:
        if result.get(extension) is not False:
            raise StratumAssertionError(f"unsupported extension {extension!r} must be explicitly false")


def assert_success_response(message: dict[str, object], *, method: str) -> object:
    if message.get("error") is not None:
        raise StratumAssertionError(f"{method} returned error: {message.get('error')!r}")
    if "result" not in message:
        raise StratumAssertionError(f"{method} response is missing result")
    return message["result"]


def run_probe_bip310(args: argparse.Namespace) -> int:
    reporter = Reporter(jsonl=args.jsonl)
    args.unsupported_extension = args.unsupported_extension or ["unsupported-extension"]

    request_params = [
        ["version-rolling", "subscribe-extranonce", *args.unsupported_extension],
        {
            "version-rolling.mask": args.version_mask,
            "version-rolling.min-bit-count": args.version_min_bit_count,
        },
    ]

    try:
        client = StratumClient(args.host, args.port, connect_timeout=args.connect_timeout)
    except OSError as exc:
        reporter.emit("error", message=f"connect failed: {exc}")
        return 2

    reporter.emit("connect", host=args.host, port=args.port)
    try:
        deadline = time.monotonic() + args.timeout
        configure_id = send_and_report(
            client,
            reporter,
            "mining.configure",
            request_params,
            emit=True,
        )
        configure_response = response_for_request(client, reporter, configure_id, deadline, emit=args.verbose)
        result = assert_success_response(configure_response, method="mining.configure")
        assert_configure_response(args, result)
    except StratumAssertionError as exc:
        reporter.emit("bip310_result", status="fail", message=str(exc))
        return 1
    except StratumTimeout as exc:
        reporter.emit("timeout", message=f"{exc}; waiting for mining.configure response")
        return 1
    except (OSError, StratumConnectionClosed, StratumProtocolError) as exc:
        reporter.emit("error", message=str(exc))
        return 2
    finally:
        client.close()
        reporter.emit("close")

    if not args.skip_extranonce_subscribe:
        try:
            client = StratumClient(args.host, args.port, connect_timeout=args.connect_timeout)
        except OSError as exc:
            reporter.emit("error", message=f"connect failed: {exc}")
            return 2
        reporter.emit("connect", host=args.host, port=args.port)
        try:
            deadline = time.monotonic() + args.timeout
            extranonce_id = send_and_report(
                client,
                reporter,
                "mining.extranonce.subscribe",
                [],
                emit=True,
            )
            extranonce_response = response_for_request(client, reporter, extranonce_id, deadline, emit=args.verbose)
            result = assert_success_response(extranonce_response, method="mining.extranonce.subscribe")
            if result is not False:
                raise StratumAssertionError(
                    f"mining.extranonce.subscribe result was {result!r}; expected False",
                )
        except StratumAssertionError as exc:
            reporter.emit("bip310_result", status="fail", message=str(exc))
            return 1
        except StratumTimeout as exc:
            reporter.emit("timeout", message=f"{exc}; waiting for mining.extranonce.subscribe response")
            return 1
        except (OSError, StratumConnectionClosed, StratumProtocolError) as exc:
            reporter.emit("error", message=str(exc))
            return 2
        finally:
            client.close()
            reporter.emit("close")

    reporter.emit(
        "bip310_result",
        status="pass",
        message=(
            f"mask={args.version_mask}&{args.configured_version_mask} "
            f"unsupported={compact_json(args.unsupported_extension)} extranonce_subscribe=false"
        ),
    )
    return 0


def add_common_arguments(
    parser: argparse.ArgumentParser,
    *,
    timeout_default: float = 15.0,
    timeout_help: str = "overall read timeout in seconds",
) -> None:
    parser.add_argument("--host", default=env_default("STRATUM_HOST", default="127.0.0.1"))
    parser.add_argument("--port", type=int, default=env_int_default("STRATUM_PORT", 3333))
    parser.add_argument("--connect-timeout", type=float, default=5.0)
    parser.add_argument("--timeout", type=float, default=timeout_default, help=timeout_help)
    parser.add_argument("--password", default=env_default("STRATUM_PASSWORD", "MINER_PASSWORD", default="x"))
    parser.add_argument("--agent", default=DEFAULT_AGENT)
    parser.add_argument("--jsonl", action="store_true", help="emit newline-delimited JSON events")
    parser.add_argument("--no-configure", action="store_true", help="skip mining.configure")
    parser.add_argument(
        "--configure-extension",
        action="append",
        help="extension name for mining.configure; defaults to version-rolling",
    )
    parser.add_argument(
        "--configure-param",
        action="append",
        type=parse_key_value,
        help="KEY=VALUE parameter for mining.configure",
    )
    parser.add_argument(
        "--version-mask",
        default=DEFAULT_VERSION_MASK,
        help="version-rolling.mask value sent by mining.configure",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Raw Stratum v1 helper for observing ckpool handshakes and notify messages.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    observe = subparsers.add_parser("observe", help="connect, handshake, and capture mining.notify messages")
    add_common_arguments(observe)
    observe.add_argument(
        "--username",
        default=env_default("STRATUM_USERNAME", "MINER_USERNAME", default="probe"),
        help="worker username for mining.authorize; usually a qbit payout address",
    )
    observe.add_argument("--notify-count", type=int, default=1)
    observe.set_defaults(func=run_observe)

    probe = subparsers.add_parser("probe-malformed", help="try malformed usernames against mining.authorize")
    add_common_arguments(probe, timeout_default=5.0, timeout_help="per-username read timeout in seconds")
    probe.add_argument("--username", default="probe", help=argparse.SUPPRESS)
    probe.add_argument(
        "--malformed-username",
        action="append",
        help="malformed username to try; may be repeated. Defaults to a small built-in corpus.",
    )
    probe.add_argument("--verbose", action="store_true", help="also emit send/recv transcript for each probe")
    probe.add_argument("--fail-on-accepted", action="store_true", help="exit 1 if any malformed username is accepted")
    probe.set_defaults(func=run_probe_malformed)

    bip310 = subparsers.add_parser("probe-bip310", help="assert BIP-310 mining.configure behavior")
    add_common_arguments(bip310, timeout_default=1.0, timeout_help="per-response timeout in seconds")
    bip310.set_defaults(func=run_probe_bip310, version_mask=DEFAULT_VERSION_MASK)
    bip310.add_argument(
        "--configured-version-mask",
        default=env_default("CKPOOL_VERSION_MASK", default=DEFAULT_VERSION_MASK),
        help="server-side mask to use when computing the expected negotiated mask",
    )
    bip310.add_argument(
        "--version-min-bit-count",
        type=int,
        default=16,
        help="version-rolling.min-bit-count value sent by mining.configure",
    )
    bip310.add_argument(
        "--unsupported-extension",
        action="append",
        help="unsupported extension to request and assert as false; defaults to unsupported-extension",
    )
    bip310.add_argument(
        "--skip-extranonce-subscribe",
        action="store_true",
        help="skip the standalone mining.extranonce.subscribe response assertion",
    )
    bip310.add_argument("--verbose", action="store_true", help="emit full receive transcript")

    return parser


def normalize_argv(argv: list[str]) -> list[str]:
    commands = {"observe", "probe-malformed", "probe-bip310"}
    if argv and argv[0] not in commands and argv[0] not in {"-h", "--help"}:
        return ["observe", *argv]
    return argv


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(normalize_argv(list(sys.argv[1:] if argv is None else argv)))
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.connect_timeout <= 0:
        parser.error("--connect-timeout must be positive")
    if getattr(args, "notify_count", 0) < 0:
        parser.error("--notify-count must be non-negative")
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

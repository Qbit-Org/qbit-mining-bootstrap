#!/usr/bin/env bash
# Prove that install-pinned-tool.sh rides out a release-host outage, installs
# from its verified cache with the network gone, and refuses anything that does
# not match the pinned sha256 on the download path and the cache path alike,
# without reaching the network, Docker or GitHub.
#
# A Python http.server on 127.0.0.1 stands in for the release host. Each
# scenario sets its behaviour (serve, 504 until a deadline, 404, stall, refuse)
# through a control file, and the server logs every connection so the test can
# assert request counts, not only exit statuses.
#
# The installer hardcodes its pins and its 180/210-second timing, so a hermetic
# test can neither serve bytes with the published digests nor wait out the real
# windows. Two narrow bridges close that gap and leave every guard as the
# committed code:
#   * A routing-only curl shim on PATH rewrites the pinned https://github.com/
#     URL to the loopback server and execs the real curl with every other
#     argument untouched, so the installer's own retry, deadline and error
#     handling run as written.
#   * Scripts under test are derived from the committed installer by replacing
#     exactly four whole lines: the two sha256= pins (with the digests of the
#     fixture assets, computed here with sha256sum) and the two timing lines
#     (timeout --signal=KILL and --retry ...). Each line must match exactly
#     once, and a diff against the committed file must show only those lines,
#     so an edit to any guard flows straight into the test and an edit to a
#     substituted line fails loudly. sha256sum is never shimmed: verify() runs
#     as committed, only the expected value differs. Two variants are derived:
#     a 15-second deadline for the outage scenarios and a 2-second deadline so
#     the stalled-transfer scenario finishes fast.
# The pure-failure scenarios (404, substituted bytes, poisoned cache, non-regular
# cache) also run against the unmodified committed installer, with its real pins
# and timing, through the shim alone.
#
# Bad assets and poisoned cache entries carry an execution marker: running them
# touches a sentinel file, which must never appear. One warm-cache run uses a
# cache directory whose path contains a newline: sha256sum --check cannot name
# such a file, so it passes only if the private copy, not the shared entry, is
# what is checked.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INSTALLER="${SCRIPT_DIR}/install-pinned-tool.sh"
TOOLS=(cargo-deny hadolint)

# The pins as they must stay; a quiet bump trips this test.
EXPECTED_CARGO_DENY_VERSION=0.20.2
EXPECTED_CARGO_DENY_SHA256=9f12ed4c49936e09b48bf862b595cde2fe64fcbd9d74dfacac6131ca824c8d5f
EXPECTED_HADOLINT_VERSION=2.15.1
EXPECTED_HADOLINT_SHA256=c7187db94eeeeca956519a6af171adc31453941a1e777961f6e680f697c8c507

# Derived-variant timing. Retries run every second; the outage lasts longer
# than the seven seconds the pre-#380 installer retried for.
OUTAGE_SECONDS=8
OUTAGE_DEADLINE=15
STALL_DEADLINE=2
RETRY_MAX_TIME=12
# Bound on one installer run, so a regression that hangs (a cp blocking on a
# FIFO, a 404 that retries) fails this test instead of the job.
RUN_LIMIT=20

if [[ -z "${EPOCHREALTIME:-}" ]]; then
  echo 'pinned-tool test: FAIL: bash 5 or newer is required (EPOCHREALTIME)' >&2
  exit 1
fi
START_US="${EPOCHREALTIME/./}"
scenarios=0
assertions=0
SCENARIO=""
WORK=""
SERVER_PID=""
RUN_OUTPUT=""
REQUEST_LOG=""

fail() {
  printf 'pinned-tool test: FAIL: %s\n' "$*" >&2
  if [[ -n "${SCENARIO}" ]]; then
    printf 'pinned-tool test: while running %s\n' "${SCENARIO}" >&2
  fi
  if [[ -n "${RUN_OUTPUT}" && -s "${RUN_OUTPUT}" ]]; then
    printf -- '--- installer output ---\n' >&2
    cat "${RUN_OUTPUT}" >&2
  fi
  if [[ -n "${REQUEST_LOG}" && -s "${REQUEST_LOG}" ]]; then
    printf -- '--- release-host requests ---\n' >&2
    cat "${REQUEST_LOG}" >&2
  fi
  exit 1
}

cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill -TERM "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${WORK}" ]]; then
    rm -rf -- "${WORK}"
  fi
}
trap cleanup EXIT

assert_eq() { # <actual> <expected> <what>
  assertions=$((assertions + 1))
  [[ "$1" == "$2" ]] || fail "$3: expected '$2', got '$1'"
}

assert_ne() { # <actual> <unexpected> <what>
  assertions=$((assertions + 1))
  [[ "$1" != "$2" ]] || fail "$3: got '$1'"
}

assert_ge() { # <actual> <minimum> <what>
  assertions=$((assertions + 1))
  (( $1 >= $2 )) || fail "$3: expected at least $2, got $1"
}

assert_lt() { # <actual> <bound> <what>
  assertions=$((assertions + 1))
  (( $1 < $2 )) || fail "$3: expected under $2, got $1"
}

assert_prefix() { # <string> <prefix> <what>
  assertions=$((assertions + 1))
  [[ "$1" == "$2"* ]] || fail "$3: '$1' does not start with '$2'"
}

assert_exists() { # <path> <what>
  assertions=$((assertions + 1))
  [[ -e "$1" || -L "$1" ]] || fail "$2: $1 is missing"
}

assert_absent() { # <path> <what>
  assertions=$((assertions + 1))
  [[ ! -e "$1" && ! -L "$1" ]] || fail "$2: $1 exists"
}

assert_output_contains() { # <text>
  assertions=$((assertions + 1))
  grep -qF -- "$1" "${RUN_OUTPUT}" || fail "installer output lacks '$1'"
}

assert_output_lacks() { # <text> <why>
  assertions=$((assertions + 1))
  ! grep -qF -- "$1" "${RUN_OUTPUT}" || fail "installer output contains '$1': $2"
}

assert_same_bytes() { # <path> <path> <what>
  assertions=$((assertions + 1))
  cmp -s -- "$1" "$2" || fail "$3: $1 differs from $2"
}

scenario() { # <label> <description>
  scenarios=$((scenarios + 1))
  SCENARIO="[$1] $2"
}

scenario_ok() { # [details]
  printf 'pinned-tool test: %s: ok%s\n' "${SCENARIO}" "${1:+, $1}"
  SCENARIO=""
}

seconds() { # <milliseconds>
  printf '%d.%02ds' $(( $1 / 1000 )) $(( $1 % 1000 / 10 ))
}

for command in python3 curl sha256sum timeout tar mkfifo diff cmp awk; do
  command -v "${command}" >/dev/null || fail "${command} is required"
done
[[ -f "${INSTALLER}" ]] || fail "${INSTALLER} is missing"

WORK="$(mktemp -d)"
SERVER_DIR="${WORK}/release-host"
REQUEST_LOG="${SERVER_DIR}/requests.log"
SHIM_DIR="${WORK}/shim"
CURL_LOG="${WORK}/curl-invocations.log"
FIXTURES="${WORK}/fixtures"
MARKERS="${WORK}/markers"
RUN_TMPDIR="${WORK}/installer-tmp"
RUN_OUTPUT="${WORK}/installer-output"
OUTAGE_INSTALLER="${WORK}/install-pinned-tool.outage.sh"
DEADLINE_INSTALLER="${WORK}/install-pinned-tool.deadline.sh"
mkdir -p "${SERVER_DIR}" "${SHIM_DIR}" "${FIXTURES}" "${MARKERS}" "${RUN_TMPDIR}"

# --- Pins, parsed from the committed installer ------------------------------

declare -A PIN_VERSION PIN_SHA256 PIN_URL ASSET_NAME EXPECTED_PATH
PIN=""

# parse_pin <tool> <name>: the value assigned to <name> inside the installer's
# case block for <tool>, into PIN, with surrounding double quotes removed.
parse_pin() {
  local tool="$1" name="$2" matches
  matches="$(awk -v tool="${tool}" -v name="${name}" '
    $0 == "  " tool ")" { in_block = 1; next }
    in_block && $0 == "    ;;" { in_block = 0 }
    in_block && index($0, "    " name "=") == 1 { print substr($0, length(name) + 6) }
  ' "${INSTALLER}")"
  [[ -n "${matches}" && "${matches}" != *$'\n'* ]] ||
    fail "expected exactly one ${name}= line in the ${tool} case of install-pinned-tool.sh, got '${matches}'"
  PIN="${matches#\"}"
  PIN="${PIN%\"}"
}

read_pins() {
  local tool asset_dir url
  scenario pins "versions, digests and release URLs are unchanged"
  for tool in "${TOOLS[@]}"; do
    parse_pin "${tool}" version
    PIN_VERSION[${tool}]="${PIN}"
    parse_pin "${tool}" sha256
    PIN_SHA256[${tool}]="${PIN}"
    parse_pin "${tool}" url
    PIN_URL[${tool}]="${PIN}"
  done
  assert_eq "${PIN_VERSION[cargo-deny]}" "${EXPECTED_CARGO_DENY_VERSION}" "cargo-deny version pin"
  assert_eq "${PIN_SHA256[cargo-deny]}" "${EXPECTED_CARGO_DENY_SHA256}" "cargo-deny sha256 pin"
  assert_eq "${PIN_VERSION[hadolint]}" "${EXPECTED_HADOLINT_VERSION}" "hadolint version pin"
  assert_eq "${PIN_SHA256[hadolint]}" "${EXPECTED_HADOLINT_SHA256}" "hadolint sha256 pin"

  # Expand ${version} and ${asset} the way the installer does, so the test
  # knows the asset basename the cache uses and the path the shim forwards.
  parse_pin cargo-deny asset
  asset_dir="${PIN//\$\{version\}/${PIN_VERSION[cargo-deny]}}"
  CARGO_DENY_ASSET_DIR="${asset_dir}"
  for tool in "${TOOLS[@]}"; do
    url="${PIN_URL[${tool}]}"
    url="${url//\$\{asset\}/${asset_dir}}"
    url="${url//\$\{version\}/${PIN_VERSION[${tool}]}}"
    [[ "${url}" != *'$'* ]] || fail "${tool}: cannot expand release URL ${url}"
    assert_prefix "${url}" "https://github.com/" "${tool}: release URL host the shim reroutes"
    PIN_URL[${tool}]="${url}"
    ASSET_NAME[${tool}]="${url##*/}"
    EXPECTED_PATH[${tool}]="/${url#https://github.com/}"
  done
  assert_eq "${ASSET_NAME[cargo-deny]}" \
    "cargo-deny-${EXPECTED_CARGO_DENY_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
    "cargo-deny release asset name"
  assert_eq "${ASSET_NAME[hadolint]}" "hadolint-linux-x86_64" "hadolint release asset name"
  scenario_ok
}

# --- Fixtures: installable assets whose payload records that it ran ----------

declare -A FIXTURE_ASSET FIXTURE_SHA256

write_payload() { # <path> <tool> <good|bad>
  {
    printf '#!/usr/bin/env bash\n'
    printf 'touch %q\n' "${MARKERS}/executed-$3"
    if [[ "$3" == good ]]; then
      printf 'echo %q\n' "$2 fixture ${PIN_VERSION[$2]}"
    fi
  } > "$1"
  chmod 0755 "$1"
}

make_fixtures() {
  local tool kind stage
  for tool in "${TOOLS[@]}"; do
    for kind in good bad; do
      write_payload "${FIXTURES}/${tool}-${kind}-payload" "${tool}" "${kind}"
    done
  done
  # cargo-deny ships a tarball with the binary under the asset directory. The
  # bad tarball has the same layout, so only the digest check keeps it out.
  for kind in good bad; do
    stage="${FIXTURES}/stage"
    mkdir -p "${stage}/${CARGO_DENY_ASSET_DIR}"
    cp -- "${FIXTURES}/cargo-deny-${kind}-payload" "${stage}/${CARGO_DENY_ASSET_DIR}/cargo-deny"
    tar -czf "${FIXTURES}/cargo-deny-${kind}.tar.gz" -C "${stage}" "${CARGO_DENY_ASSET_DIR}"
    rm -rf -- "${stage}"
    FIXTURE_ASSET[cargo-deny-${kind}]="${FIXTURES}/cargo-deny-${kind}.tar.gz"
    FIXTURE_ASSET[hadolint-${kind}]="${FIXTURES}/hadolint-${kind}-payload"
  done
  for tool in "${TOOLS[@]}"; do
    FIXTURE_SHA256[${tool}]="$(sha256sum -- "${FIXTURE_ASSET[${tool}-good]}" | cut -d' ' -f1)"
    [[ "${FIXTURE_SHA256[${tool}]}" != "$(sha256sum -- "${FIXTURE_ASSET[${tool}-bad]}" | cut -d' ' -f1)" ]] ||
      fail "${tool}: good and bad fixtures must differ"
  done
}

# --- Routing-only curl shim --------------------------------------------------

make_shim() {
  local real_curl
  real_curl="$(command -v curl)"
  {
    printf '#!/usr/bin/env bash\nreal_curl=%q\n' "${real_curl}"
    cat <<'SHIM'
# Routing-only curl shim: send the pinned GitHub release URL to the loopback
# release host and pass every other argument through to the real curl.
set -euo pipefail
printf '%s\n' "$*" >> "${PINNED_TOOL_TEST_CURL_LOG}"
args=()
for arg in "$@"; do
  case "${arg}" in
    https://github.com/*) arg="${PINNED_TOOL_TEST_ORIGIN}/${arg#https://github.com/}" ;;
  esac
  case "${arg}" in
    "${PINNED_TOOL_TEST_ORIGIN}"/*) ;;
    http://* | https://*)
      echo "curl shim: refusing to reach ${arg}" >&2
      exit 99
      ;;
  esac
  args+=("${arg}")
done
exec "${real_curl}" "${args[@]}"
SHIM
  } > "${SHIM_DIR}/curl"
  chmod 0755 "${SHIM_DIR}/curl"
}

# --- Loopback release host ---------------------------------------------------

write_server() {
  cat > "${WORK}/release-host.py" <<'PY'
"""Loopback stand-in for the GitHub release host, driven by a control file."""
import http.server
import os
import select
import socket
import struct
import sys
import threading
import time

ROOT = sys.argv[1]
CONTROL = os.path.join(ROOT, "control")
LOG = os.path.join(ROOT, "requests.log")
LOCK = threading.Lock()


def read_control():
    control = {}
    with open(CONTROL) as handle:
        for line in handle:
            key, _, value = line.rstrip("\n").partition("=")
            control[key] = value
    return control


def log(kind, path, status):
    with LOCK:
        with open(LOG, "a") as handle:
            handle.write("%.3f %s %s %s\n" % (time.time(), kind, path, status))


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def handle(self):
        if read_control().get("mode") == "refuse":
            # Reset the connection without reading the request.
            self.connection.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            log("REFUSED", "-", "-")
            return
        super().handle()

    def do_GET(self):
        control = read_control()
        mode = control.get("mode")
        if mode == "outage" and time.time() * 1e6 < float(control["until_us"]):
            self.reply(504)
        elif mode in ("serve", "outage"):
            with open(control["asset"], "rb") as handle:
                body = handle.read()
            self.reply(200, body)
        elif mode == "missing":
            self.reply(404)
        elif mode == "stall":
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(1 << 30))
            self.end_headers()
            self.wfile.write(b"x" * 16)
            self.wfile.flush()
            log("GET", self.path, 200)
            # Never finish the body; return once the peer has gone away.
            select.select([self.connection], [], [], 60)
        else:
            self.reply(500)

    def reply(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        log("GET", self.path, status)


def main():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port_tmp = os.path.join(ROOT, "port.tmp")
    with open(port_tmp, "w") as handle:
        handle.write("%d\n" % server.server_address[1])
    os.replace(port_tmp, os.path.join(ROOT, "port"))
    server.serve_forever()


main()
PY
}

set_server() { # <mode> [asset] [until_us]
  {
    printf 'mode=%s\n' "$1"
    [[ $# -lt 2 ]] || printf 'asset=%s\n' "$2"
    [[ $# -lt 3 ]] || printf 'until_us=%s\n' "$3"
  } > "${SERVER_DIR}/control.tmp"
  mv -f -- "${SERVER_DIR}/control.tmp" "${SERVER_DIR}/control"
}

start_server() {
  local _attempt
  write_server
  set_server missing
  : > "${REQUEST_LOG}"
  python3 "${WORK}/release-host.py" "${SERVER_DIR}" > "${SERVER_DIR}/server.log" 2>&1 &
  SERVER_PID=$!
  for _attempt in {1..100}; do
    if [[ -s "${SERVER_DIR}/port" ]]; then
      break
    fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      cat "${SERVER_DIR}/server.log" >&2 || true
      fail "release host exited before reporting its port"
    fi
    sleep 0.05
  done
  [[ -s "${SERVER_DIR}/port" ]] || fail "release host did not report its port"
  ORIGIN="http://127.0.0.1:$(< "${SERVER_DIR}/port")"
}

request_count() {
  awk 'END { print NR }' "${REQUEST_LOG}"
}

status_count() { # <status>
  awk -v status="$1" '$4 == status { n++ } END { print n + 0 }' "${REQUEST_LOG}"
}

curl_count() {
  awk 'END { print NR }' "${CURL_LOG}"
}

assert_request_paths() { # <tool>
  local paths
  paths="$(awk '{ print $3 }' "${REQUEST_LOG}" | sort -u)"
  assert_eq "${paths}" "${EXPECTED_PATH[$1]}" "$1: every request should ask for the pinned asset"
}

# The counter behind every "zero requests" assertion must be shown to count.
selfcheck_server() {
  local status=0 fetched="${WORK}/selfcheck-fetched"
  scenario harness "loopback release host serves bytes and counts refused connections"
  set_server serve "${FIXTURE_ASSET[hadolint-good]}"
  : > "${REQUEST_LOG}"
  curl --fail --silent --show-error --max-time 5 --output "${fetched}" "${ORIGIN}/selfcheck" ||
    fail "release host did not serve the fixture"
  assert_same_bytes "${fetched}" "${FIXTURE_ASSET[hadolint-good]}" "served bytes"
  assert_eq "$(request_count)" 1 "served request count"
  set_server refuse
  : > "${REQUEST_LOG}"
  curl --fail --silent --max-time 5 --output /dev/null "${ORIGIN}/selfcheck" 2>/dev/null || status=$?
  assert_ne "${status}" 0 "curl against the refusing host"
  assert_eq "$(request_count)" 1 "refused connection count"
  assert_eq "$(awk '{ print $2 }' "${REQUEST_LOG}")" REFUSED "refused connection log"
  scenario_ok
}

# --- Scripts under test, derived from the committed installer ----------------

# substitute_line <file> <from> <to>: replace the one line equal to <from>.
substitute_line() {
  local file="$1" from="$2" to="$3" line count=0
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" == "${from}" ]]; then
      printf '%s\n' "${to}"
      count=$((count + 1))
    else
      printf '%s\n' "${line}"
    fi
  done < "${file}" > "${file}.tmp"
  assert_eq "${count}" 1 "occurrences of '${from}' in install-pinned-tool.sh"
  mv -f -- "${file}.tmp" "${file}"
}

# derive_installer <output> <deadline seconds> <retry max time>
derive_installer() {
  local output="$1" i expected actual diff_status=0
  local -a from=(
    "    sha256=${PIN_SHA256[cargo-deny]}"
    "    sha256=${PIN_SHA256[hadolint]}"
    "  timeout --signal=KILL 210 \\"
    "      --retry 12 --retry-delay 15 --retry-max-time 180 \\"
  )
  local -a to=(
    "    sha256=${FIXTURE_SHA256[cargo-deny]}"
    "    sha256=${FIXTURE_SHA256[hadolint]}"
    "  timeout --signal=KILL $2 \\"
    "      --retry 12 --retry-delay 1 --retry-max-time $3 \\"
  )
  scenario derive "script under test with a $2s deadline differs only in the pins and timing"
  cp -- "${INSTALLER}" "${output}"
  for i in "${!from[@]}"; do
    substitute_line "${output}" "${from[i]}" "${to[i]}"
  done
  expected="$(for i in "${!from[@]}"; do printf -- '-%s\n+%s\n' "${from[i]}" "${to[i]}"; done | sort)"
  actual="$(diff --old-line-format='-%L' --new-line-format='+%L' --unchanged-line-format='' \
    -- "${INSTALLER}" "${output}")" || diff_status=$?
  assert_eq "${diff_status}" 1 "diff between the committed and derived installer"
  assert_eq "$(printf '%s\n' "${actual}" | sort)" "${expected}" \
    "lines that differ from the committed installer"
  bash -n "${output}" || fail "derived ${output} has a syntax error"
  scenario_ok
}

# --- Running the installer ---------------------------------------------------

RUN_STATUS=0
RUN_ELAPSED_MS=0
CACHE_ENTRY=""
CACHED_ASSET=""

# run_installer <installer> <tool> <destination> <cache dir or empty>
# Runs the installer the way CI does, under PATH with the shim first, with a
# private TMPDIR, and records its exit status, elapsed time and output.
run_installer() {
  local installer="$1" tool="$2" destination="$3" cache_dir="$4" start end
  local -a cache_env=(-u PINNED_TOOL_CACHE_DIR)
  if [[ -n "${cache_dir}" ]]; then
    cache_env=(PINNED_TOOL_CACHE_DIR="${cache_dir}")
  fi
  rm -rf -- "${MARKERS}" "${RUN_TMPDIR}"
  mkdir -p "${MARKERS}" "${RUN_TMPDIR}"
  : > "${CURL_LOG}"
  : > "${REQUEST_LOG}"
  RUN_STATUS=0
  start="${EPOCHREALTIME/./}"
  timeout --signal=KILL "${RUN_LIMIT}" \
    env "${cache_env[@]}" \
      PATH="${SHIM_DIR}:${PATH}" \
      PINNED_TOOL_TEST_ORIGIN="${ORIGIN}" \
      PINNED_TOOL_TEST_CURL_LOG="${CURL_LOG}" \
      TMPDIR="${RUN_TMPDIR}" \
      bash "${installer}" "${tool}" "${destination}" > "${RUN_OUTPUT}" 2>&1 || RUN_STATUS=$?
  end="${EPOCHREALTIME/./}"
  RUN_ELAPSED_MS=$(( (end - start) / 1000 ))
  [[ "${RUN_STATUS}" != 137 ]] || fail "${tool}: installer did not finish within ${RUN_LIMIT}s"
  assert_eq "$(find "${RUN_TMPDIR}" -mindepth 1 | awk 'END { print NR }')" 0 \
    "${tool}: files the installer left in its TMPDIR"
}

# resolve_cache_entry <installer> <tool> <cache dir> <sha256 that installer pins>
# Asks the installer itself, through --cache-metadata, where its cache entry is.
resolve_cache_entry() {
  local installer="$1" tool="$2" cache_dir="$3" sha256="$4" output="${WORK}/github-output"
  : > "${output}"
  PINNED_TOOL_CACHE_DIR="${cache_dir}" RUNNER_OS=Linux RUNNER_ARCH=X64 GITHUB_OUTPUT="${output}" \
    bash "${installer}" --cache-metadata "${tool}" >/dev/null 2>&1 ||
    fail "${tool}: --cache-metadata failed"
  assert_eq "$(sed -n 's/^cache-dir=//p' "${output}")" "${cache_dir}" "${tool}: cache-dir output"
  assert_eq "$(sed -n 's/^key=//p' "${output}")" \
    "pinned-tool-v1-Linux-X64-${tool}-${PIN_VERSION[${tool}]}-${sha256}" "${tool}: cache key"
  CACHE_ENTRY="$(sed -n 's/^path=//p' "${output}")"
  assert_eq "${CACHE_ENTRY}" "${cache_dir}/${tool}/${PIN_VERSION[${tool}]}/${sha256}" \
    "${tool}: cache entry layout"
  CACHED_ASSET="${CACHE_ENTRY}/${ASSET_NAME[${tool}]}"
}

assert_installed() { # <destination> <tool>
  assert_exists "$1/$2" "$2 should be installed"
  assert_eq "$(stat -c %a "$1/$2")" 755 "$2 install mode"
  assert_output_contains "$2 fixture ${PIN_VERSION[$2]}"
  assert_exists "${MARKERS}/executed-good" "the installed $2 should have run"
  assert_absent "${MARKERS}/executed-bad" "a bad $2 asset must never run"
}

assert_nothing_installed() { # <destination> <tool>
  assert_absent "$1" "$2 destination must not be created"
  assert_absent "${MARKERS}/executed-bad" "a bad $2 asset must never run"
  assert_absent "${MARKERS}/executed-good" "no $2 asset should have run"
}

# --- Scenarios ---------------------------------------------------------------

# The release host answers 504 for longer than the seven seconds the pre-#380
# installer retried for, then serves the asset. The install must succeed and
# publish the verified asset into the (empty) cache.
scenario_outage_then_asset() { # <tool> <cache dir>
  local tool="$1" cache_dir="$2" destination="${WORK}/destination-outage-${1}"
  scenario "${tool}" "504 for ${OUTAGE_SECONDS}s then the real asset (derived, ${OUTAGE_DEADLINE}s deadline)"
  set_server outage "${FIXTURE_ASSET[${tool}-good]}" "$(( ${EPOCHREALTIME/./} + OUTAGE_SECONDS * 1000000 ))"
  run_installer "${OUTAGE_INSTALLER}" "${tool}" "${destination}" "${cache_dir}"
  assert_eq "${RUN_STATUS}" 0 "${tool}: install after the outage"
  assert_ge "${RUN_ELAPSED_MS}" 7000 "${tool}: the outage outlasted the old seven-second window"
  assert_lt "${RUN_ELAPSED_MS}" $(( OUTAGE_DEADLINE * 1000 )) "${tool}: install finished inside the deadline"
  assert_ge "$(status_count 504)" 5 "${tool}: 504 responses (the old installer gave up after 4 attempts)"
  assert_eq "$(status_count 200)" 1 "${tool}: successful downloads"
  assert_request_paths "${tool}"
  assert_eq "$(curl_count)" 1 "${tool}: curl invocations"
  assert_installed "${destination}" "${tool}"
  assert_output_contains "install-pinned-tool: cached "
  resolve_cache_entry "${OUTAGE_INSTALLER}" "${tool}" "${cache_dir}" "${FIXTURE_SHA256[${tool}]}"
  assert_exists "${CACHED_ASSET}" "${tool}: cache entry"
  assert_eq "$(stat -c %a "${CACHED_ASSET}")" 644 "${tool}: cache entry mode"
  assert_eq "$(sha256sum -- "${CACHED_ASSET}" | cut -d' ' -f1)" "${FIXTURE_SHA256[${tool}]}" \
    "${tool}: cache entry digest"
  scenario_ok "$(seconds "${RUN_ELAPSED_MS}"), $(request_count) requests"
}

# The cache holds the verified asset and the release host refuses every
# connection: the install must succeed without a single request.
scenario_cached_network_gone() { # <tool> <cache dir>
  local tool="$1" cache_dir="$2" destination="${WORK}/destination-cached-${1}"
  scenario "${tool}" "warm verified cache with the release host refusing connections (derived)"
  set_server refuse
  run_installer "${OUTAGE_INSTALLER}" "${tool}" "${destination}" "${cache_dir}"
  assert_eq "${RUN_STATUS}" 0 "${tool}: install from cache"
  assert_eq "$(request_count)" 0 "${tool}: requests to the release host"
  assert_eq "$(curl_count)" 0 "${tool}: curl invocations"
  assert_output_contains "install-pinned-tool: using cached "
  assert_installed "${destination}" "${tool}"
  assert_lt "${RUN_ELAPSED_MS}" 5000 "${tool}: install from cache is quick"
  scenario_ok "$(seconds "${RUN_ELAPSED_MS}"), $(request_count) requests"
}

# The cached asset is copied to private scratch and the copy is checked, so the
# shared entry's path never reaches sha256sum. A cache directory whose path
# contains a newline makes that observable: sha256sum --check cannot parse a
# name with a newline, so checking the shared entry in place would fail here
# while checking the private copy under the installer's own mktemp succeeds.
scenario_cached_private_copy() { # <tool>
  local tool="$1" destination="${WORK}/destination-private-copy-${1}"
  local cache_dir="${WORK}/cache-with"$'\n'"newline" cache_entry
  scenario "${tool}" "warm cache under a path sha256sum cannot name: the private copy is checked (derived)"
  cache_entry="${cache_dir}/${tool}/${PIN_VERSION[${tool}]}/${FIXTURE_SHA256[${tool}]}"
  mkdir -p "${cache_entry}"
  cp -- "${FIXTURE_ASSET[${tool}-good]}" "${cache_entry}/${ASSET_NAME[${tool}]}"
  chmod 0644 "${cache_entry}/${ASSET_NAME[${tool}]}"
  set_server refuse
  run_installer "${OUTAGE_INSTALLER}" "${tool}" "${destination}" "${cache_dir}"
  assert_eq "${RUN_STATUS}" 0 "${tool}: install from a cache under a newline path"
  assert_eq "$(request_count)" 0 "${tool}: requests to the release host"
  assert_eq "$(curl_count)" 0 "${tool}: curl invocations"
  assert_output_contains "install-pinned-tool: using cached "
  assert_installed "${destination}" "${tool}"
  scenario_ok "$(seconds "${RUN_ELAPSED_MS}"), $(request_count) requests"
}

# The release host serves bytes that do not match the pin. The post-download
# check must reject them by name: with a cache the publish step checks its copy
# again, and without one that first check is all that stands before install.
# <variant> <installer> <tool> <sha256 that installer pins> <cache|no-cache>
scenario_substituted_download() {
  local variant="$1" installer="$2" tool="$3" sha256="$4" caching="$5" cache_dir=""
  local destination="${WORK}/destination-substituted-${1}-${3}-${5}"
  scenario "${tool}" "substituted download bytes (${variant}, ${caching})"
  if [[ "${caching}" == cache ]]; then
    cache_dir="${WORK}/cache-substituted-${1}-${3}"
    mkdir -p "${cache_dir}"
  fi
  set_server serve "${FIXTURE_ASSET[${tool}-bad]}"
  run_installer "${installer}" "${tool}" "${destination}" "${cache_dir}"
  assert_ne "${RUN_STATUS}" 0 "${tool}: install of substituted bytes"
  assert_output_contains "install-pinned-tool: ${PIN_URL[${tool}]} does not match the pinned sha256"
  assert_eq "$(request_count)" 1 "${tool}: requests (a digest failure is not retried)"
  assert_eq "$(status_count 200)" 1 "${tool}: downloads"
  assert_request_paths "${tool}"
  assert_eq "$(curl_count)" 1 "${tool}: curl invocations"
  assert_nothing_installed "${destination}" "${tool}"
  if [[ -n "${cache_dir}" ]]; then
    resolve_cache_entry "${installer}" "${tool}" "${cache_dir}" "${sha256}"
    assert_absent "${CACHED_ASSET}" "${tool}: cache entry for substituted bytes"
    assert_eq "$(find "${cache_dir}" -type f | awk 'END { print NR }')" 0 \
      "${tool}: files published into the cache"
  fi
  scenario_ok "$(seconds "${RUN_ELAPSED_MS}"), $(request_count) requests"
}

# A cache entry with the wrong bytes must be rejected without a refetch, even
# though the release host would serve the asset.
scenario_poisoned_cache() { # <variant> <installer> <tool> <sha256 that installer pins>
  local variant="$1" installer="$2" tool="$3" sha256="$4"
  local cache_dir="${WORK}/cache-poisoned-${1}-${3}"
  local destination="${WORK}/destination-poisoned-${1}-${3}"
  scenario "${tool}" "poisoned cache entry with the wrong bytes (${variant})"
  resolve_cache_entry "${installer}" "${tool}" "${cache_dir}" "${sha256}"
  mkdir -p "${CACHE_ENTRY}"
  cp -- "${FIXTURE_ASSET[${tool}-bad]}" "${CACHED_ASSET}"
  chmod 0644 "${CACHED_ASSET}"
  set_server serve "${FIXTURE_ASSET[${tool}-good]}"
  run_installer "${installer}" "${tool}" "${destination}" "${cache_dir}"
  assert_ne "${RUN_STATUS}" 0 "${tool}: install from a poisoned cache"
  assert_output_contains "does not match the pinned sha256; not using or refetching"
  assert_eq "$(request_count)" 0 "${tool}: requests (a bad entry must not be refetched)"
  assert_eq "$(curl_count)" 0 "${tool}: curl invocations"
  assert_nothing_installed "${destination}" "${tool}"
  assert_same_bytes "${CACHED_ASSET}" "${FIXTURE_ASSET[${tool}-bad]}" \
    "${tool}: poisoned entry left for removal by hand"
  scenario_ok "$(seconds "${RUN_ELAPSED_MS}"), $(request_count) requests"
}

# A cache entry that is not a regular file must be rejected before it is read:
# a bare FIFO (caught by the -f half of the guard), a symlink to a FIFO, and a
# symlink to a regular file with the right bytes (caught only by the -L half).
# <variant> <installer> <tool> <sha256 that installer pins> <bare-fifo|fifo-symlink|good-symlink>
scenario_non_regular_cache() {
  local variant="$1" installer="$2" tool="$3" sha256="$4" kind="$5" target description
  local cache_dir="${WORK}/cache-${5}-${1}-${3}"
  local destination="${WORK}/destination-${5}-${1}-${3}"
  case "${kind}" in
    bare-fifo) description="a FIFO" ;;
    fifo-symlink) description="a symlink to a FIFO" ;;
    good-symlink) description="a symlink to a digest-correct regular file" ;;
  esac
  scenario "${tool}" "cache entry is ${description} (${variant})"
  resolve_cache_entry "${installer}" "${tool}" "${cache_dir}" "${sha256}"
  mkdir -p "${CACHE_ENTRY}"
  target="${WORK}/${kind}-target-${variant}-${tool}"
  case "${kind}" in
    bare-fifo)
      mkfifo "${CACHED_ASSET}"
      ;;
    fifo-symlink)
      mkfifo "${target}"
      ln -s -- "${target}" "${CACHED_ASSET}"
      ;;
    good-symlink)
      cp -- "${FIXTURE_ASSET[${tool}-good]}" "${target}"
      ln -s -- "${target}" "${CACHED_ASSET}"
      ;;
  esac
  set_server serve "${FIXTURE_ASSET[${tool}-good]}"
  run_installer "${installer}" "${tool}" "${destination}" "${cache_dir}"
  assert_ne "${RUN_STATUS}" 0 "${tool}: install from a non-regular cache entry"
  assert_output_contains "is not a regular file"
  assert_lt "${RUN_ELAPSED_MS}" 5000 "${tool}: rejection is immediate (nothing read the entry)"
  assert_eq "$(request_count)" 0 "${tool}: requests (a bad entry must not be refetched)"
  assert_eq "$(curl_count)" 0 "${tool}: curl invocations"
  assert_nothing_installed "${destination}" "${tool}"
  assertions=$((assertions + 1))
  [[ -L "${CACHED_ASSET}" || -p "${CACHED_ASSET}" ]] ||
    fail "${tool}: the rejected entry should be left for removal by hand"
  scenario_ok "$(seconds "${RUN_ELAPSED_MS}"), $(request_count) requests"
}

# A missing asset fails at once instead of burning the retry budget.
scenario_missing_asset() { # <variant> <installer> <tool>
  local variant="$1" installer="$2" tool="$3" destination="${WORK}/destination-missing-${1}-${3}"
  scenario "${tool}" "404 fails fast (${variant})"
  set_server missing
  run_installer "${installer}" "${tool}" "${destination}" ""
  assert_ne "${RUN_STATUS}" 0 "${tool}: install of a missing asset"
  assert_lt "${RUN_ELAPSED_MS}" 5000 "${tool}: a 404 must not be retried"
  assert_eq "$(request_count)" 1 "${tool}: requests"
  assert_eq "$(status_count 404)" 1 "${tool}: 404 responses"
  assert_request_paths "${tool}"
  assert_eq "$(curl_count)" 1 "${tool}: curl invocations"
  assert_output_contains "failed (curl exit 22)"
  assert_nothing_installed "${destination}" "${tool}"
  scenario_ok "$(seconds "${RUN_ELAPSED_MS}"), $(request_count) requests"
}

# The release host accepts the connection and never finishes the body: the
# hard deadline, not curl's own timers, must end the download.
scenario_stalled_transfer() { # <tool>
  local tool="$1" destination="${WORK}/destination-stalled-${1}"
  scenario "${tool}" "stalled transfer hits the hard deadline (derived, ${STALL_DEADLINE}s deadline)"
  set_server stall
  run_installer "${DEADLINE_INSTALLER}" "${tool}" "${destination}" ""
  assert_ne "${RUN_STATUS}" 0 "${tool}: install from a stalled transfer"
  assert_ge "${RUN_ELAPSED_MS}" $(( STALL_DEADLINE * 1000 )) "${tool}: the deadline ran its course"
  assert_lt "${RUN_ELAPSED_MS}" 10000 "${tool}: the deadline, not a longer timer, ended the download"
  assert_output_contains "did not finish within"
  assert_output_lacks "does not match the pinned sha256" \
    "${tool}: the deadline must end the run before the partial download is checked"
  assert_eq "$(request_count)" 1 "${tool}: requests"
  assert_eq "$(status_count 200)" 1 "${tool}: stalled responses"
  assert_request_paths "${tool}"
  assert_nothing_installed "${destination}" "${tool}"
  scenario_ok "$(seconds "${RUN_ELAPSED_MS}"), $(request_count) requests"
}

# --- Run ---------------------------------------------------------------------

read_pins
make_fixtures
make_shim
start_server
selfcheck_server
derive_installer "${OUTAGE_INSTALLER}" "${OUTAGE_DEADLINE}" "${RETRY_MAX_TIME}"
derive_installer "${DEADLINE_INSTALLER}" "${STALL_DEADLINE}" "${RETRY_MAX_TIME}"

SHARED_CACHE="${WORK}/cache-shared"
for tool in "${TOOLS[@]}"; do
  scenario_outage_then_asset "${tool}" "${SHARED_CACHE}"
done
for tool in "${TOOLS[@]}"; do
  scenario_cached_network_gone "${tool}" "${SHARED_CACHE}"
done
for tool in "${TOOLS[@]}"; do
  scenario_cached_private_copy "${tool}"
done
for tool in "${TOOLS[@]}"; do
  for caching in cache no-cache; do
    scenario_substituted_download derived "${OUTAGE_INSTALLER}" "${tool}" "${FIXTURE_SHA256[${tool}]}" "${caching}"
    scenario_substituted_download committed "${INSTALLER}" "${tool}" "${PIN_SHA256[${tool}]}" "${caching}"
  done
done
for tool in "${TOOLS[@]}"; do
  scenario_poisoned_cache derived "${OUTAGE_INSTALLER}" "${tool}" "${FIXTURE_SHA256[${tool}]}"
  scenario_poisoned_cache committed "${INSTALLER}" "${tool}" "${PIN_SHA256[${tool}]}"
done
for tool in "${TOOLS[@]}"; do
  scenario_non_regular_cache derived "${OUTAGE_INSTALLER}" "${tool}" "${FIXTURE_SHA256[${tool}]}" bare-fifo
  scenario_non_regular_cache committed "${INSTALLER}" "${tool}" "${PIN_SHA256[${tool}]}" bare-fifo
  scenario_non_regular_cache derived "${OUTAGE_INSTALLER}" "${tool}" "${FIXTURE_SHA256[${tool}]}" fifo-symlink
  scenario_non_regular_cache committed "${INSTALLER}" "${tool}" "${PIN_SHA256[${tool}]}" fifo-symlink
  scenario_non_regular_cache derived "${OUTAGE_INSTALLER}" "${tool}" "${FIXTURE_SHA256[${tool}]}" good-symlink
done
for tool in "${TOOLS[@]}"; do
  scenario_missing_asset derived "${OUTAGE_INSTALLER}" "${tool}"
  scenario_missing_asset committed "${INSTALLER}" "${tool}"
done
for tool in "${TOOLS[@]}"; do
  scenario_stalled_transfer "${tool}"
done

printf 'pinned-tool test: PASS: %d scenarios, %d assertions in %s\n' \
  "${scenarios}" "${assertions}" "$(seconds $(( (${EPOCHREALTIME/./} - START_US) / 1000 )))"

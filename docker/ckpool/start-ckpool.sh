#!/usr/bin/env bash
set -euo pipefail

SUPERVISED_CHILD=0
if [[ "${1:-}" == "--supervised-child" ]]; then
  SUPERVISED_CHILD=1
  shift
fi
if (( $# != 0 )); then
  printf 'unexpected start-ckpool argument: %s\n' "$1" >&2
  exit 2
fi

: "${QBIT_RPC_USER:?QBIT_RPC_USER is required}"
: "${QBIT_RPC_PASSWORD:?QBIT_RPC_PASSWORD is required}"
: "${QBIT_RPC_HOST:=qbitd}"
: "${QBIT_RPC_PORT:=18452}"
: "${QBIT_CHAIN:=regtest}"
: "${QBIT_PRODUCTION:=0}"
: "${QBIT_TOOLS_PRODUCTION:=0}"
: "${CKPOOL_STRATUM_PORT:=3333}"
: "${CKPOOL_REGTEST_DIFF_FLOOR:=0.00390625}"
: "${CKPOOL_DEFAULT_MINDIFF:=1}"
: "${CKPOOL_DEFAULT_STARTDIFF:=42}"
: "${CKPOOL_MAXDIFF:=}"
: "${CKPOOL_VERSION_MASK:=1fffe000}"
: "${CKPOOL_VERSION_MASK_MODE:=dynamic}"
: "${CKPOOL_VERSION_MASK_RPC_TIMEOUT_SECONDS:=5}"
: "${QBIT_MINER_WALLET_NAME:=ckpool}"
: "${CKPOOL_BIN:=/usr/local/bin/ckpool}"
: "${CKPOOL_CONFIG_FILE:=/etc/ckpool/ckpool.conf}"
: "${CKPOOL_LOG_DIR:=/var/log/ckpool}"
: "${CKPOOL_SOCK_DIR:=/tmp/qbitlab}"
: "${CKPOOL_STATE_DIR:=/var/lib/ckpool}"
: "${QBIT_MINER_ADDRESS_FILE:=${CKPOOL_STATE_DIR}/miner-address.txt}"
: "${QBIT_MINER_ADDRESS_FILE_WAIT:=0}"
: "${CKPOOL_NOTIFY:=false}"
: "${CKPOOL_BTCSIG=/qbit-mining-bootstrap/}"
: "${CKPOOL_BLOCKPOLL:=2}"
: "${CKPOOL_DONATION:=0.0}"
: "${CKPOOL_NONCE1LENGTH:=4}"
: "${CKPOOL_NONCE2LENGTH:=8}"
: "${CKPOOL_UPDATE_INTERVAL:=30}"
: "${CKPOOL_PUBLIC_DIFF_POLICY:=explicit}"
: "${CKPOOL_NON_TEST_READINESS_GATE:=1}"
: "${CKPOOL_MIN_PEERS:=1}"
: "${CKPOOL_PREFLIGHT_RPC_TIMEOUT_SECONDS:=5}"
: "${CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS:=120}"
: "${CKPOOL_TEMPLATE_MAX_AGE_SECONDS:=120}"
: "${CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS:=30}"
: "${CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS:=5}"
: "${CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS:=120}"
: "${CKPOOL_VALIDATE_QBIT_ASSUMPTIONS:=1}"
: "${CKPOOL_REQUIRE_P2MR_PAYOUT:=}"
: "${QBIT_EXPECTED_ADDRESS_HRP:=}"
: "${QBIT_EXPECTED_GENESIS_HASH:=}"
: "${QBIT_EXPECTED_MAX_BLOCK_WEIGHT:=2000000}"
: "${QBIT_EXPECTED_WITNESS_SCALE_FACTOR:=1}"
: "${QBIT_EXPECTED_COINBASE_MATURITY:=1000}"

export QBIT_RPC_USER QBIT_RPC_PASSWORD QBIT_RPC_HOST QBIT_RPC_PORT QBIT_CHAIN QBIT_MINER_WALLET_NAME
export QBIT_PRODUCTION QBIT_TOOLS_PRODUCTION CKPOOL_STRATUM_PORT
export CKPOOL_VERSION_MASK CKPOOL_VERSION_MASK_MODE CKPOOL_VERSION_MASK_RPC_TIMEOUT_SECONDS
export CKPOOL_PUBLIC_DIFF_POLICY CKPOOL_NON_TEST_READINESS_GATE CKPOOL_MIN_PEERS
export CKPOOL_PREFLIGHT_RPC_TIMEOUT_SECONDS CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS
export CKPOOL_TEMPLATE_MAX_AGE_SECONDS CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS
export CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS
export CKPOOL_VALIDATE_QBIT_ASSUMPTIONS CKPOOL_REQUIRE_P2MR_PAYOUT
export QBIT_EXPECTED_ADDRESS_HRP QBIT_EXPECTED_GENESIS_HASH QBIT_EXPECTED_MAX_BLOCK_WEIGHT
export QBIT_EXPECTED_WITNESS_SCALE_FACTOR QBIT_EXPECTED_COINBASE_MATURITY
export CKPOOL_BIN CKPOOL_CONFIG_FILE CKPOOL_LOG_DIR CKPOOL_SOCK_DIR CKPOOL_STATE_DIR
export QBIT_MINER_ADDRESS_FILE QBIT_MINER_ADDRESS_FILE_WAIT
export CKPOOL_NOTIFY CKPOOL_BTCSIG CKPOOL_BLOCKPOLL CKPOOL_DONATION
export CKPOOL_NONCE1LENGTH CKPOOL_NONCE2LENGTH CKPOOL_UPDATE_INTERVAL

json_string() {
  python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

json_bool() {
  local value
  value="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|on) printf 'true\n' ;;
    0|false|no|off) printf 'false\n' ;;
    *)
      printf 'expected boolean value, got: %s\n' "$1" >&2
      return 1
      ;;
  esac
}

resolve_miner_address() {
  local candidate="${QBIT_MINER_ADDRESS:-}"
  if [[ -n "${candidate}" && "${candidate}" != "auto" ]]; then
    if QBIT_RPC_USER="${QBIT_RPC_USER}" QBIT_RPC_PASSWORD="${QBIT_RPC_PASSWORD}" python3 - "${candidate}" <<'PY' >/dev/null 2>&1
import base64
import json
import os
import sys
import time
from urllib import request

address = sys.argv[1]
user = os.environ["QBIT_RPC_USER"]
password = os.environ["QBIT_RPC_PASSWORD"]
host = os.environ["QBIT_RPC_HOST"]
port = os.environ["QBIT_RPC_PORT"]
payload = json.dumps({
    "jsonrpc": "1.0",
    "id": "ckpool",
    "method": "validateaddress",
    "params": [address],
}).encode()
credentials = f"{user}:{password}".encode()
req = request.Request(
    f"http://{host}:{port}",
    data=payload,
    headers={
        "Authorization": f"Basic {base64.b64encode(credentials).decode()}",
        "Content-Type": "application/json",
    },
)
with request.urlopen(req) as resp:
    body = json.load(resp)
result = body.get("result") or {}
if not result.get("isvalid"):
    raise SystemExit(1)
print(address)
PY
    then
      printf '%s\n' "${candidate}"
      return 0
    fi
    printf 'invalid QBIT_MINER_ADDRESS for the configured chain: %s\n' "${candidate}" >&2
    return 1
  fi

  QBIT_RPC_USER="${QBIT_RPC_USER}" QBIT_RPC_PASSWORD="${QBIT_RPC_PASSWORD}" python3 - <<'PY'
import base64
import json
import os
import sys
import time
from urllib import request

user = os.environ["QBIT_RPC_USER"]
password = os.environ["QBIT_RPC_PASSWORD"]
host = os.environ["QBIT_RPC_HOST"]
port = os.environ["QBIT_RPC_PORT"]
wallet_name = os.environ["QBIT_MINER_WALLET_NAME"]

def rpc(method, params=None, wallet=None):
    payload = json.dumps({
        "jsonrpc": "1.0",
        "id": "ckpool",
        "method": method,
        "params": params or [],
    }).encode()
    url = f"http://{host}:{port}"
    if wallet:
        url += "/wallet/" + wallet
    credentials = f"{user}:{password}".encode()
    req = request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Basic {base64.b64encode(credentials).decode()}",
            "Content-Type": "application/json",
        },
    )
    with request.urlopen(req) as resp:
        body = json.load(resp)
    if body.get("error"):
        raise RuntimeError(body["error"])
    return body["result"]

def get_new_address(wallet_name):
    last_error = None
    for params in ([], ["", "p2mr"]):
        try:
            address = rpc("getnewaddress", params, wallet=wallet_name)
        except Exception as exc:
            last_error = exc
            continue
        if address:
            return str(address)
    raise RuntimeError(f"{wallet_name} wallet did not return an address: {last_error}")

last_error = None
try:
    rpc("createwallet", [wallet_name])
except Exception:
    pass

for _ in range(30):
    try:
        address = get_new_address(wallet_name)
        if address:
            print(address)
            raise SystemExit(0)
    except Exception as exc:
        print(f"address validation retry failed: {exc}", file=sys.stderr)
        last_error = exc
        try:
            rpc("loadwallet", [wallet_name])
        except Exception:
            pass
        time.sleep(2)

raise SystemExit(f"failed to resolve a valid miner address from qbit: {last_error}")
PY
}

if [[ -n "${CKPOOL_MINDIFF:-}" ]]; then
  CKPOOL_MINDIFF_EXPLICIT=1
else
  CKPOOL_MINDIFF_EXPLICIT=0
fi
if [[ -n "${CKPOOL_STARTDIFF:-}" ]]; then
  CKPOOL_STARTDIFF_EXPLICIT=1
else
  CKPOOL_STARTDIFF_EXPLICIT=0
fi

if [[ -z "${CKPOOL_MINDIFF:-}" ]]; then
  if [[ "${QBIT_CHAIN}" == "regtest" ]]; then
    CKPOOL_MINDIFF="${CKPOOL_REGTEST_DIFF_FLOOR}"
  else
    CKPOOL_MINDIFF="${CKPOOL_DEFAULT_MINDIFF}"
  fi
fi
if [[ -z "${CKPOOL_STARTDIFF:-}" ]]; then
  if [[ "${QBIT_CHAIN}" == "regtest" ]]; then
    CKPOOL_STARTDIFF="${CKPOOL_REGTEST_DIFF_FLOOR}"
  else
    CKPOOL_STARTDIFF="${CKPOOL_DEFAULT_STARTDIFF}"
  fi
fi
export CKPOOL_MINDIFF CKPOOL_STARTDIFF CKPOOL_MAXDIFF CKPOOL_MINDIFF_EXPLICIT CKPOOL_STARTDIFF_EXPLICIT

DIFF_FIELDS=""
if [[ -n "${CKPOOL_MINDIFF:-}" ]]; then
  printf -v DIFF_FIELDS '%s"mindiff" : %s,\n' "${DIFF_FIELDS}" "${CKPOOL_MINDIFF}"
fi
if [[ -n "${CKPOOL_STARTDIFF:-}" ]]; then
  printf -v DIFF_FIELDS '%s"startdiff" : %s,\n' "${DIFF_FIELDS}" "${CKPOOL_STARTDIFF}"
fi
if [[ -n "${CKPOOL_MAXDIFF:-}" ]]; then
  printf -v DIFF_FIELDS '%s"maxdiff" : %s,\n' "${DIFF_FIELDS}" "${CKPOOL_MAXDIFF}"
fi

if [[ "${SUPERVISED_CHILD}" == "0" ]]; then
  # Reject implicit production payouts and invalid static policy before wallet,
  # state, or configuration creation can hide how values were supplied.
  qbit-ckpool-preflight --production-gate-only

  CKPOOL_VERSION_MASK="$(ckpool-version-mask)"
  export CKPOOL_VERSION_MASK

  if [[ "${QBIT_MINER_ADDRESS_FILE_WAIT}" == "1" ]]; then
    for _ in $(seq 1 30); do
      if [[ -s "${QBIT_MINER_ADDRESS_FILE}" ]]; then
        QBIT_MINER_ADDRESS="$(<"${QBIT_MINER_ADDRESS_FILE}")"
        break
      fi
      sleep 1
    done
  fi
  QBIT_MINER_ADDRESS="$(resolve_miner_address)"
  export QBIT_MINER_ADDRESS
  exec qbit-ckpool-preflight --supervise /bin/bash "$0" --supervised-child
fi

: "${QBIT_MINER_ADDRESS:?QBIT_MINER_ADDRESS is required in supervised child}"
mkdir -p "$(dirname "${CKPOOL_CONFIG_FILE}")" "${CKPOOL_LOG_DIR}" "${CKPOOL_SOCK_DIR}" "${CKPOOL_STATE_DIR}"
mkdir -p "$(dirname "${QBIT_MINER_ADDRESS_FILE}")"
printf '%s\n' "${QBIT_MINER_ADDRESS}" > "${QBIT_MINER_ADDRESS_FILE}"

CKPOOL_NOTIFY_JSON="$(json_bool "${CKPOOL_NOTIFY}")"
QBIT_MINER_ADDRESS_JSON="$(json_string "${QBIT_MINER_ADDRESS}")"
CKPOOL_BTCSIG_JSON="$(json_string "${CKPOOL_BTCSIG}")"
CKPOOL_LOG_DIR_JSON="$(json_string "${CKPOOL_LOG_DIR}")"

cat > "${CKPOOL_CONFIG_FILE}" <<EOF
{
"btcd" : [
  {
    "url" : "${QBIT_RPC_HOST}:${QBIT_RPC_PORT}",
    "auth" : "${QBIT_RPC_USER}",
    "pass" : "${QBIT_RPC_PASSWORD}",
    "notify" : ${CKPOOL_NOTIFY_JSON}
  }
],
"btcaddress" : ${QBIT_MINER_ADDRESS_JSON},
"btcsig" : ${CKPOOL_BTCSIG_JSON},
"blockpoll" : ${CKPOOL_BLOCKPOLL},
"donation" : ${CKPOOL_DONATION},
"nonce1length" : ${CKPOOL_NONCE1LENGTH},
"nonce2length" : ${CKPOOL_NONCE2LENGTH},
"update_interval" : ${CKPOOL_UPDATE_INTERVAL},
"version_mask" : "${CKPOOL_VERSION_MASK}",
"serverurl" : [
  "0.0.0.0:${CKPOOL_STRATUM_PORT}"
],
${DIFF_FIELDS}"logdir" : ${CKPOOL_LOG_DIR_JSON}
}
EOF

exec "${CKPOOL_BIN}" -B -c "${CKPOOL_CONFIG_FILE}" -k -n qbitlab --sockdir "${CKPOOL_SOCK_DIR}"

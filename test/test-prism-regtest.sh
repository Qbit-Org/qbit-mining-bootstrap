#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/test/test-lib.sh"
DATADIR="${DATADIR:-$(mktemp -d -t qbit-prism-regtest.XXXXXX)}"
RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_PORT="${RPC_PORT:-18454}"
WALLET_NAME="${WALLET_NAME:-prism-regtest}"
REGTEST_BLOCKS="${QBIT_PRISM_REGTEST_BLOCKS:-2}"
ASERT_MOCKTIME_BASE="${QBIT_PRISM_REGTEST_MOCKTIME_BASE:-1738713603}"
ASERT_TIME_STEP="${QBIT_PRISM_REGTEST_ASERT_TIME_STEP:-1}"
MANIFEST_SIGNING_SEED_HEX="${QBIT_PRISM_MANIFEST_SIGNING_SEED_HEX:-4242424242424242424242424242424242424242424242424242424242424242}"
LEDGER_ATTESTATION_SIGNING_SEED_HEX="${QBIT_PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX:-4343434343434343434343434343434343434343434343434343434343434343}"
COINBASE_SCRIPT_SIG_SUFFIX_HEX="${QBIT_PRISM_COINBASE_SCRIPT_SIG_SUFFIX_HEX:-111111112222222222222222}"
REORG_TEST="${QBIT_PRISM_REGTEST_REORG:-1}"
REORG_COINBASE_SCRIPT_SIG_SUFFIX_HEX="${QBIT_PRISM_REORG_COINBASE_SCRIPT_SIG_SUFFIX_HEX:-111111113333333333333333}"

resolve_qbit_binaries
read -r -a CARGO_CMD <<< "${CARGO:-cargo}"
require_executable "${CARGO_CMD[0]}"
require_executable python3
require_qbit_src_helper "test/functional/test_framework/blocktools.py" "the PRISM regtest smoke"
run_cargo() {
  "${CARGO_CMD[@]}" "$@"
}

ledger_public_key_from_bundle_json() {
  BUNDLE_JSON="$1" python3 <<'PY'
import json
import os

bundle = json.loads(os.environ["BUNDLE_JSON"])
print(bundle["ledger_window_attestation"]["signature"]["public_key_hex"])
PY
}

cleanup() {
  stop_qbitd
  remove_datadir "${DATADIR}"
}
trap cleanup EXIT

if [[ "$(ulimit -n)" == "unlimited" ]]; then
  ulimit -n 1024 || true
fi

if [[ "${REGTEST_BLOCKS}" -lt 1 ]]; then
  echo "QBIT_PRISM_REGTEST_BLOCKS must be >= 1" >&2
  exit 1
fi

"${QBITD_BIN}" \
  -regtest \
  -asert \
  -p2mronly=1 \
  -daemonwait \
  -server \
  -listen=0 \
  -maxconnections=0 \
  -dnsseed=0 \
  -fixedseeds=0 \
  -rpcbind=127.0.0.1 \
  -rpcallowip=127.0.0.1 \
  -rpcport="${RPC_PORT}" \
  -rpcuser="${RPC_USER}" \
  -rpcpassword="${RPC_PASSWORD}" \
  -fallbackfee=0.00001000 \
  -datadir="${DATADIR}" >/dev/null

wait_for_qbit_rpc_state ready 60
qbit_rpc createwallet "${WALLET_NAME}" >/dev/null

programs_json="$(
  WALLET_NAME="${WALLET_NAME}" \
  QBIT_CLI_BIN="${QBIT_CLI_BIN}" \
  DATADIR="${DATADIR}" \
  RPC_USER="${RPC_USER}" \
  RPC_PASSWORD="${RPC_PASSWORD}" \
  RPC_PORT="${RPC_PORT}" \
  FIXTURE_PATH="${ROOT_DIR}/crates/qbit-prism/fixtures/power-law-accrual.prism-fixture.json" \
  python3 <<'PY'
import json
import os
import subprocess

fixture = json.load(open(os.environ["FIXTURE_PATH"], encoding="utf-8"))

def qbit_rpc(*args: str) -> str:
    command = [
        os.environ["QBIT_CLI_BIN"],
        "-regtest",
        f"-datadir={os.environ['DATADIR']}",
        f"-rpcuser={os.environ['RPC_USER']}",
        f"-rpcpassword={os.environ['RPC_PASSWORD']}",
        f"-rpcport={os.environ['RPC_PORT']}",
        *args,
    ]
    return subprocess.check_output(command, text=True).strip()

programs = []
seen = set()
for _ in fixture["shares"]:
    address = qbit_rpc(f"-rpcwallet={os.environ['WALLET_NAME']}", "getnewaddress", "", "p2mr")
    if address in seen:
        raise SystemExit(f"duplicate generated payout address: {address}")
    seen.add(address)
    info = json.loads(qbit_rpc("getaddressinfo", address))
    script = info["scriptPubKey"]
    if not script.startswith("5220") or len(script) != 68:
        raise SystemExit(f"non-P2MR script for {address}: {script}")
    programs.append(script[4:])

print(json.dumps(programs, separators=(",", ":")))
PY
)"

prior_balances_json="[]"
bits_seen=()
block_hashes=()
bundle_paths=()
prior_before_paths=()

for block_index in $(seq 1 "${REGTEST_BLOCKS}"); do
  mocktime=$((ASERT_MOCKTIME_BASE + (block_index - 1) * ASERT_TIME_STEP))
  qbit_rpc setmocktime "${mocktime}" >/dev/null

  before_height="$(qbit_rpc getblockcount)"
  template_json="$(qbit_rpc getblocktemplate '{"rules":["segwit"]}')"
  template_bits="$(TEMPLATE_JSON="${template_json}" python3 - <<'PY'
import json
import os
print(json.loads(os.environ["TEMPLATE_JSON"])["bits"])
PY
)"
  bits_seen+=("${template_bits}")

  bundle_input_json="$(
    TEMPLATE_JSON="${template_json}" \
    PROGRAMS_JSON="${programs_json}" \
    PRIOR_BALANCES_JSON="${prior_balances_json}" \
    BLOCK_INDEX="${block_index}" \
    FIXTURE_PATH="${ROOT_DIR}/crates/qbit-prism/fixtures/power-law-accrual.prism-fixture.json" \
    COINBASE_SCRIPT_SIG_SUFFIX_HEX="${COINBASE_SCRIPT_SIG_SUFFIX_HEX}" \
    python3 <<'PY'
import copy
import json
import os

template = json.loads(os.environ["TEMPLATE_JSON"])
fixture = json.load(open(os.environ["FIXTURE_PATH"], encoding="utf-8"))
programs = json.loads(os.environ["PROGRAMS_JSON"])
prior_balances = json.loads(os.environ["PRIOR_BALANCES_JSON"])
block_index = int(os.environ["BLOCK_INDEX"])
if template.get("transactions"):
    raise SystemExit("prism regtest smoke expects an empty regtest mempool")

def target_from_compact(bits_hex: str) -> int:
    compact = int(bits_hex, 16)
    size = compact >> 24
    mantissa = compact & 0x007fffff
    if size <= 3:
        return mantissa >> (8 * (3 - size))
    return mantissa << (8 * (size - 3))

pow_limit_target = target_from_compact("207fffff")
template_target = target_from_compact(template["bits"])
network_difficulty = max(1, (pow_limit_target * 1_000_000) // template_target)
share_difficulties = [
    network_difficulty * 5,
    network_difficulty,
    max(1, network_difficulty // 4),
    10,
    1,
    1,
]
if len(share_difficulties) != len(fixture["shares"]) or len(programs) != len(fixture["shares"]):
    raise SystemExit("prism fixture shape changed; update test/test-prism-regtest.sh")

found_block = copy.deepcopy(fixture["found_block"])
found_block["block_height"] = int(template["height"])
found_block["coinbase_value_sats"] = int(template["coinbasevalue"])
found_block["network_difficulty"] = network_difficulty
time_offset_ms = (block_index - 1) * 10_000
found_block["anchor_job_issued_at_ms"] = int(found_block["anchor_job_issued_at_ms"]) + time_offset_ms

shares = copy.deepcopy(fixture["shares"])
base_seq = (block_index - 1) * len(shares)
for offset, (share, program, difficulty) in enumerate(zip(shares, programs, share_difficulties), start=1):
    share["share_seq"] = base_seq + offset
    share["share_id"] = f"{share['share_id']}-block-{block_index}"
    share["job_id"] = f"{share['job_id']}-block-{block_index}"
    share["p2mr_program_hex"] = program
    share["share_difficulty"] = difficulty
    share["network_difficulty"] = network_difficulty
    share["template_height"] = int(template["height"]) - 1
    share["job_issued_at_ms"] = int(share["job_issued_at_ms"]) + time_offset_ms
    share["accepted_at_ms"] = int(share["accepted_at_ms"]) + time_offset_ms
    share["ntime"] = int(template["curtime"])

print(json.dumps({
    "found_block": found_block,
    "shares": shares,
    "prior_balances": prior_balances,
    "coinbase_script_sig_suffix_hex": os.environ["COINBASE_SCRIPT_SIG_SUFFIX_HEX"],
}, separators=(",", ":")))
PY
  )"
  prior_before_path="${DATADIR}/prior-balances-before-${block_index}.json"
  printf '%s\n' "${prior_balances_json}" >"${prior_before_path}"
  prior_before_paths+=("${prior_before_path}")

  bundle_json="$(
    run_cargo run --quiet -p qbit-prism --bin qbit-prism-build-audit-bundle -- \
      --input - \
      --signing-key-seed-hex "${MANIFEST_SIGNING_SEED_HEX}" \
      --ledger-signing-key-seed-hex "${LEDGER_ATTESTATION_SIGNING_SEED_HEX}" <<<"${bundle_input_json}"
  )"
  ledger_public_key_hex="$(ledger_public_key_from_bundle_json "${bundle_json}")"
  bundle_path="${DATADIR}/prism-audit-bundle-${block_index}.json"
  printf '%s\n' "${bundle_json}" >"${bundle_path}"
  bundle_paths+=("${bundle_path}")

  block_hex="$(
    TEMPLATE_JSON="${template_json}" \
    BUNDLE_JSON="${bundle_json}" \
    QBIT_SRC_DIR="${QBIT_SRC_DIR}" \
    python3 <<'PY'
import io
import json
import os
import sys
from pathlib import Path

qbit_src = Path(os.environ["QBIT_SRC_DIR"])
sys.path.insert(0, str(qbit_src / "test/functional"))

from test_framework.blocktools import create_block
from test_framework.messages import CTransaction

template = json.loads(os.environ["TEMPLATE_JSON"])
bundle = json.loads(os.environ["BUNDLE_JSON"])
manifest = bundle["signed_coinbase_manifest"]["manifest"]
coinbase = CTransaction()
coinbase.deserialize(io.BytesIO(bytes.fromhex(manifest["coinbase_tx_hex"])))
block = create_block(
    hashprev=int(template["previousblockhash"], 16),
    coinbase=coinbase,
    ntime=int(template["curtime"]),
    version=int(template["version"]),
    tmpl=template,
    txlist=[tx["data"] for tx in template.get("transactions", [])],
)
block.solve()
print(block.serialize().hex())
PY
  )"

  result="$(qbit_rpc submitblock "${block_hex}")"
  after_height="$(qbit_rpc getblockcount)"
  if [[ -n "${result}" && "${result}" != "null" ]]; then
    echo "submitblock returned failure for PRISM regtest block ${block_index}: ${result}" >&2
    exit 1
  fi
  if [[ "${after_height}" -ne $((before_height + 1)) ]]; then
    echo "expected blockcount ${before_height}+1 after submitblock, got ${after_height}" >&2
    exit 1
  fi

  block_hash="$(qbit_rpc getblockhash "${after_height}")"
  block_hashes+=("${block_hash}")
  raw_block="$(qbit_rpc getblock "${block_hash}" 0)"
  coinbase_tx_hex="$(
    RAW_BLOCK="${raw_block}" \
    BUNDLE_JSON="${bundle_json}" \
    QBIT_SRC_DIR="${QBIT_SRC_DIR}" \
    python3 <<'PY'
import io
import json
import os
import sys
from pathlib import Path

qbit_src = Path(os.environ["QBIT_SRC_DIR"])
sys.path.insert(0, str(qbit_src / "test/functional"))

from test_framework.messages import CBlock

block = CBlock()
block.deserialize(io.BytesIO(bytes.fromhex(os.environ["RAW_BLOCK"])))
bundle = json.loads(os.environ["BUNDLE_JSON"])
manifest = bundle["signed_coinbase_manifest"]["manifest"]
policy_manifest = bundle["payout_policy_manifest"]
coinbase = block.vtx[0]
coinbase_tx_hex = coinbase.serialize_with_witness().hex()
if coinbase_tx_hex != manifest["coinbase_tx_hex"]:
    raise SystemExit("on-chain coinbase bytes do not match signed PRISM manifest coinbase_tx_hex")
if manifest["coinbase_script_sig_suffix_hex"] != bundle["coinbase_script_sig_suffix_hex"]:
    raise SystemExit("manifest scriptSig suffix does not match audit bundle suffix")
if len(manifest["outputs"]) < 3:
    raise SystemExit(f"expected at least 3 on-chain PRISM payouts, got {len(manifest['outputs'])}")
accrued = [account for account in policy_manifest["accounts"] if account["action"] == "accrued"]
if len(accrued) < 1:
    raise SystemExit("expected at least one accrued below-floor account")
actual = [
    (txout.nValue, bytes(txout.scriptPubKey).hex())
    for txout in coinbase.vout[:len(manifest["outputs"])]
]
expected = [
    (output["amount_sats"], output["script_pubkey_hex"])
    for output in manifest["outputs"]
]
if actual != expected:
    raise SystemExit("on-chain PRISM payout outputs do not match signed manifest")
if sum(value for value, _script in actual) != manifest["coinbase_value_sats"]:
    raise SystemExit("on-chain PRISM payout sum does not equal coinbase value")
commitment = coinbase.vout[-1]
if commitment.nValue != 0 or bytes(commitment.scriptPubKey).hex() != manifest["witness_commitment_script_hex"]:
    raise SystemExit("on-chain witness commitment does not match PRISM manifest")
print(coinbase_tx_hex)
PY
  )"

	  verify_report="$(
	    run_cargo run --quiet -p qbit-prism --bin qbit-prism-audit-verify -- \
	      "${bundle_path}" \
	      --coinbase-tx-hex "${coinbase_tx_hex}" \
	      --ledger-writer-public-key-hex "${ledger_public_key_hex}" \
	      --expected-coinbase-value-sats "$(BUNDLE_PATH="${bundle_path}" python3 -c 'import json,os; print(json.load(open(os.environ["BUNDLE_PATH"], encoding="utf-8"))["found_block"]["coinbase_value_sats"])')"
	  )"
  VERIFY_REPORT="${verify_report}" python3 <<'PY'
import json
import os

report = json.loads(os.environ["VERIFY_REPORT"])
if report["schema"] != "qbit.prism.audit-verification-report.v1":
    raise SystemExit("unexpected audit verifier report schema")
if report["onchain_output_count"] < 3:
    raise SystemExit(f"expected at least 3 verified on-chain outputs, got {report['onchain_output_count']}")
if report["accrued_account_count"] < 1:
    raise SystemExit("expected verifier to report accrued below-floor accounts")
PY

  prior_balances_json="$(
    BUNDLE_JSON="${bundle_json}" python3 <<'PY'
import json
import os

bundle = json.loads(os.environ["BUNDLE_JSON"])
balances = []
for account in bundle["payout_policy_manifest"]["accounts"]:
    carry = int(account["carry_forward_balance_sats"])
    if carry > 0:
        balances.append({
            "recipient_id": account["recipient_id"],
            "order_key": account["order_key"],
            "p2mr_program_hex": account["p2mr_program_hex"],
            "balance_sats": carry,
        })
print(json.dumps(balances, separators=(",", ":")))
PY
  )"

  network_difficulty="$(
    BUNDLE_JSON="${bundle_json}" python3 <<'PY'
import json
import os
print(json.loads(os.environ["BUNDLE_JSON"])["found_block"]["network_difficulty"])
PY
  )"
  printf 'prism regtest block PASS index=%s height=%s bits=%s scaled_network_difficulty=%s block=%s\n' \
    "${block_index}" "${after_height}" "${template_bits}" "${network_difficulty}" "${block_hash}"
done

if [[ "${REGTEST_BLOCKS}" -ge 2 ]]; then
  unique_bits="$(printf '%s\n' "${bits_seen[@]}" | sort -u | wc -l | tr -d ' ')"
  if [[ "${unique_bits}" -lt 2 ]]; then
    echo "expected ASERT compact target to change across ${REGTEST_BLOCKS} blocks; saw: ${bits_seen[*]}" >&2
    exit 1
  fi
fi

if [[ "${REORG_TEST}" == "1" && "${REGTEST_BLOCKS}" -ge 2 ]]; then
  reorg_index="${REGTEST_BLOCKS}"
  disconnected_hash="${block_hashes[$((reorg_index - 1))]}"
  disconnected_bundle_path="${bundle_paths[$((reorg_index - 1))]}"
  replacement_prior_balances_json="$(cat "${prior_before_paths[$((reorg_index - 1))]}")"

  qbit_rpc invalidateblock "${disconnected_hash}" >/dev/null
  reorg_height="$(qbit_rpc getblockcount)"
  if [[ "${reorg_height}" -ne $((reorg_index - 1)) ]]; then
    echo "expected height $((reorg_index - 1)) after invalidating ${disconnected_hash}, got ${reorg_height}" >&2
    exit 1
  fi

  replacement_mocktime=$((ASERT_MOCKTIME_BASE + (reorg_index - 1) * ASERT_TIME_STEP))
  qbit_rpc setmocktime "${replacement_mocktime}" >/dev/null
  before_height="$(qbit_rpc getblockcount)"
  template_json="$(qbit_rpc getblocktemplate '{"rules":["segwit"]}')"
  replacement_bits="$(TEMPLATE_JSON="${template_json}" python3 - <<'PY'
import json
import os
print(json.loads(os.environ["TEMPLATE_JSON"])["bits"])
PY
)"
  bundle_input_json="$(
    TEMPLATE_JSON="${template_json}" \
    PROGRAMS_JSON="${programs_json}" \
    PRIOR_BALANCES_JSON="${replacement_prior_balances_json}" \
    BLOCK_INDEX="${reorg_index}" \
    FIXTURE_PATH="${ROOT_DIR}/crates/qbit-prism/fixtures/power-law-accrual.prism-fixture.json" \
    COINBASE_SCRIPT_SIG_SUFFIX_HEX="${REORG_COINBASE_SCRIPT_SIG_SUFFIX_HEX}" \
    python3 <<'PY'
import copy
import json
import os

template = json.loads(os.environ["TEMPLATE_JSON"])
fixture = json.load(open(os.environ["FIXTURE_PATH"], encoding="utf-8"))
programs = json.loads(os.environ["PROGRAMS_JSON"])
prior_balances = json.loads(os.environ["PRIOR_BALANCES_JSON"])
block_index = int(os.environ["BLOCK_INDEX"])
if template.get("transactions"):
    raise SystemExit("prism regtest reorg replacement expects an empty regtest mempool")

def target_from_compact(bits_hex: str) -> int:
    compact = int(bits_hex, 16)
    size = compact >> 24
    mantissa = compact & 0x007fffff
    if size <= 3:
        return mantissa >> (8 * (3 - size))
    return mantissa << (8 * (size - 3))

pow_limit_target = target_from_compact("207fffff")
template_target = target_from_compact(template["bits"])
network_difficulty = max(1, (pow_limit_target * 1_000_000) // template_target)
share_difficulties = [
    network_difficulty * 5,
    network_difficulty,
    max(1, network_difficulty // 4),
    10,
    1,
    1,
]
if len(share_difficulties) != len(fixture["shares"]) or len(programs) != len(fixture["shares"]):
    raise SystemExit("prism fixture shape changed; update test/test-prism-regtest.sh")

found_block = copy.deepcopy(fixture["found_block"])
found_block["block_height"] = int(template["height"])
found_block["coinbase_value_sats"] = int(template["coinbasevalue"])
found_block["network_difficulty"] = network_difficulty
time_offset_ms = (block_index - 1) * 10_000
found_block["anchor_job_issued_at_ms"] = int(found_block["anchor_job_issued_at_ms"]) + time_offset_ms

shares = copy.deepcopy(fixture["shares"])
base_seq = (block_index - 1) * len(shares)
for offset, (share, program, difficulty) in enumerate(zip(shares, programs, share_difficulties), start=1):
    share["share_seq"] = base_seq + offset
    share["share_id"] = f"{share['share_id']}-replacement-{block_index}"
    share["job_id"] = f"{share['job_id']}-replacement-{block_index}"
    share["p2mr_program_hex"] = program
    share["share_difficulty"] = difficulty
    share["network_difficulty"] = network_difficulty
    share["template_height"] = int(template["height"]) - 1
    share["job_issued_at_ms"] = int(share["job_issued_at_ms"]) + time_offset_ms
    share["accepted_at_ms"] = int(share["accepted_at_ms"]) + time_offset_ms
    share["ntime"] = int(template["curtime"])

print(json.dumps({
    "found_block": found_block,
    "shares": shares,
    "prior_balances": prior_balances,
    "coinbase_script_sig_suffix_hex": os.environ["COINBASE_SCRIPT_SIG_SUFFIX_HEX"],
}, separators=(",", ":")))
PY
  )"

  replacement_bundle_json="$(
    run_cargo run --quiet -p qbit-prism --bin qbit-prism-build-audit-bundle -- \
      --input - \
      --signing-key-seed-hex "${MANIFEST_SIGNING_SEED_HEX}" \
      --ledger-signing-key-seed-hex "${LEDGER_ATTESTATION_SIGNING_SEED_HEX}" <<<"${bundle_input_json}"
  )"
  replacement_ledger_public_key_hex="$(ledger_public_key_from_bundle_json "${replacement_bundle_json}")"
  replacement_bundle_path="${DATADIR}/prism-audit-bundle-replacement-${reorg_index}.json"
  printf '%s\n' "${replacement_bundle_json}" >"${replacement_bundle_path}"

  block_hex="$(
    TEMPLATE_JSON="${template_json}" \
    BUNDLE_JSON="${replacement_bundle_json}" \
    QBIT_SRC_DIR="${QBIT_SRC_DIR}" \
    python3 <<'PY'
import io
import json
import os
import sys
from pathlib import Path

qbit_src = Path(os.environ["QBIT_SRC_DIR"])
sys.path.insert(0, str(qbit_src / "test/functional"))

from test_framework.blocktools import create_block
from test_framework.messages import CTransaction

template = json.loads(os.environ["TEMPLATE_JSON"])
bundle = json.loads(os.environ["BUNDLE_JSON"])
manifest = bundle["signed_coinbase_manifest"]["manifest"]
coinbase = CTransaction()
coinbase.deserialize(io.BytesIO(bytes.fromhex(manifest["coinbase_tx_hex"])))
block = create_block(
    hashprev=int(template["previousblockhash"], 16),
    coinbase=coinbase,
    ntime=int(template["curtime"]),
    version=int(template["version"]),
    tmpl=template,
    txlist=[tx["data"] for tx in template.get("transactions", [])],
)
block.solve()
print(block.serialize().hex())
PY
  )"

  result="$(qbit_rpc submitblock "${block_hex}")"
  after_height="$(qbit_rpc getblockcount)"
  if [[ -n "${result}" && "${result}" != "null" ]]; then
    echo "submitblock returned failure for PRISM replacement block: ${result}" >&2
    exit 1
  fi
  if [[ "${after_height}" -ne $((before_height + 1)) ]]; then
    echo "expected replacement blockcount ${before_height}+1 after submitblock, got ${after_height}" >&2
    exit 1
  fi

  replacement_hash="$(qbit_rpc getblockhash "${after_height}")"
  if [[ "${replacement_hash}" == "${disconnected_hash}" ]]; then
    echo "replacement block hash unexpectedly equals disconnected block ${disconnected_hash}" >&2
    exit 1
  fi
  active_tip="$(qbit_rpc getbestblockhash)"
  if [[ "${active_tip}" != "${replacement_hash}" ]]; then
    echo "expected active tip ${replacement_hash}, got ${active_tip}" >&2
    exit 1
  fi

  raw_block="$(qbit_rpc getblock "${replacement_hash}" 0)"
  replacement_coinbase_tx_hex="$(
    RAW_BLOCK="${raw_block}" \
    BUNDLE_JSON="${replacement_bundle_json}" \
    QBIT_SRC_DIR="${QBIT_SRC_DIR}" \
    python3 <<'PY'
import io
import json
import os
import sys
from pathlib import Path

qbit_src = Path(os.environ["QBIT_SRC_DIR"])
sys.path.insert(0, str(qbit_src / "test/functional"))

from test_framework.messages import CBlock

block = CBlock()
block.deserialize(io.BytesIO(bytes.fromhex(os.environ["RAW_BLOCK"])))
bundle = json.loads(os.environ["BUNDLE_JSON"])
manifest = bundle["signed_coinbase_manifest"]["manifest"]
coinbase_tx_hex = block.vtx[0].serialize_with_witness().hex()
if coinbase_tx_hex != manifest["coinbase_tx_hex"]:
    raise SystemExit("replacement coinbase bytes do not match signed PRISM manifest")
print(coinbase_tx_hex)
PY
  )"

	  run_cargo run --quiet -p qbit-prism --bin qbit-prism-audit-verify -- \
	    "${replacement_bundle_path}" \
	    --coinbase-tx-hex "${replacement_coinbase_tx_hex}" \
	    --ledger-writer-public-key-hex "${replacement_ledger_public_key_hex}" \
	    --expected-coinbase-value-sats "$(BUNDLE_PATH="${replacement_bundle_path}" python3 -c 'import json,os; print(json.load(open(os.environ["BUNDLE_PATH"], encoding="utf-8"))["found_block"]["coinbase_value_sats"])')" >/dev/null

  reorg_input_json="$(
    DISCONNECTED_HASH="${disconnected_hash}" \
    DISCONNECTED_HEIGHT="${reorg_index}" \
    DISCONNECTED_BUNDLE_PATH="${disconnected_bundle_path}" \
    REPLACEMENT_HASH="${replacement_hash}" \
    REPLACEMENT_HEIGHT="${reorg_index}" \
    REPLACEMENT_BUNDLE_PATH="${replacement_bundle_path}" \
    python3 <<'PY'
import json
import os

with open(os.environ["DISCONNECTED_BUNDLE_PATH"], encoding="utf-8") as handle:
    disconnected = json.load(handle)
with open(os.environ["REPLACEMENT_BUNDLE_PATH"], encoding="utf-8") as handle:
    replacement = json.load(handle)
print(json.dumps({
    "disconnected_block_hash": os.environ["DISCONNECTED_HASH"],
    "disconnected_block_height": int(os.environ["DISCONNECTED_HEIGHT"]),
    "disconnected_payout_policy_manifest": disconnected["payout_policy_manifest"],
    "replacement_block_hash": os.environ["REPLACEMENT_HASH"],
    "replacement_block_height": int(os.environ["REPLACEMENT_HEIGHT"]),
    "replacement_payout_policy_manifest": replacement["payout_policy_manifest"],
}, separators=(",", ":")))
PY
  )"
  reorg_report="$(
    run_cargo run --quiet -p qbit-prism --bin qbit-prism-reorg-verify -- \
      --input - <<<"${reorg_input_json}"
  )"
  VERIFY_REPORT="${reorg_report}" python3 <<'PY'
import json
import os

report = json.loads(os.environ["VERIFY_REPORT"])
if report["schema"] != "qbit.prism.reorg-verification-report.v1":
    raise SystemExit("unexpected reorg verifier report schema")
if report["reversed_entry_count"] != report["disconnected_entry_count"]:
    raise SystemExit("not all disconnected entries were reversed")
if report["replacement_entry_count"] < 1:
    raise SystemExit("replacement branch produced no maturity entries")
PY

  printf 'prism reorg PASS disconnected=%s replacement=%s replacement_bits=%s\n' \
    "${disconnected_hash}" "${replacement_hash}" "${replacement_bits}"
fi

active_tip="$(qbit_rpc getbestblockhash)"
printf 'prism regtest PASS blocks=%s asert_bits=%s active_tip=%s\n' \
  "${REGTEST_BLOCKS}" "$(IFS=,; echo "${bits_seen[*]}")" "${active_tip}"

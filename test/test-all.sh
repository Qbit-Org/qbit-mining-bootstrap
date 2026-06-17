#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/test/test-lib.sh"

resolve_qbit_binaries
require_executable python3
require_qbit_src_helper "test/functional/test_framework/blocktools.py" "the permissionless smoke test"
require_qbit_src_helper "test/functional/test_framework/auxpow.py" "the merge-mining smoke test"

bash "${ROOT_DIR}/test/test-permissionless.sh"
bash "${ROOT_DIR}/test/test-mergemining.sh"

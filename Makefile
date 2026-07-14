SHELL := /bin/bash

COMPOSE_PROJECT_NAME ?= qbit-mining-bootstrap

UPSTREAM_ENV_FILE := config/upstream.env
ifeq ("$(wildcard $(UPSTREAM_ENV_FILE))","")
UPSTREAM_ENV_FILE := config/upstream.env.example
endif

COMPOSE_ENV_FILES := --env-file $(UPSTREAM_ENV_FILE)
ifneq ("$(strip $(DEPLOY_ENV_FILE))","")
COMPOSE_ENV_FILES += --env-file $(DEPLOY_ENV_FILE)
else ifneq ("$(wildcard .env)","")
COMPOSE_ENV_FILES += --env-file .env
endif

export COMPOSE_PROJECT_NAME
export DEPLOY_ENV_FILE

COMPOSE := docker compose $(COMPOSE_ENV_FILES) -f compose.yaml --project-name $(COMPOSE_PROJECT_NAME)
PRODUCTION_COMPOSE ?= $(COMPOSE) -f compose.production.yaml
# Shared profile set for disposable test cleanup; ordinary down adds PRISM but preserves volumes.
COMPOSE_ALL_PROFILES := $(COMPOSE) --profile permissionless --profile real-miner-smoke --profile auxpow
# Destructive integration cleanup is confined to a fresh, invocation-specific
# project. Four dollar signs become the recipe shell PID after Make expansion.
TEST_COMPOSE = docker compose $(COMPOSE_ENV_FILES) -f compose.yaml --project-name qbit-mining-bootstrap-test-$@-$$$$
TEST_COMPOSE_ALL_PROFILES = $(TEST_COMPOSE) --profile permissionless --profile real-miner-smoke --profile auxpow

define WITH_RESOLVED_QBIT
set -euo pipefail; \
QBIT_SRC_RESOLVED="$$(bash scripts/prepare-qbit-source.sh)"; \
export QBIT_SRC_DIR="$$QBIT_SRC_RESOLVED"; \
export QBIT_SRC_DIR_OVERRIDE="$$QBIT_SRC_RESOLVED"; \
bash scripts/check-env.sh; \
printf 'doctor: staged qbit source=%s\n' "$$QBIT_SRC_RESOLVED";
endef

define COMPOSE_ENV_HELPERS
compose_env="$$(mktemp)"; \
trap 'rm -f "$$compose_env"' EXIT; \
$(COMPOSE) config --environment > "$$compose_env"; \
compose_env_value() { \
	awk -F= -v key="$$1" -v dflt="$${2:-}" '$$1 == key { print substr($$0, index($$0, "=") + 1); found=1 } END { if (!found) print dflt }' "$$compose_env"; \
}; \
	stratum_endpoint() { \
	case "$$1" in \
		*:*) printf '%s\n' "$$1" ;; \
		*) printf '127.0.0.1:%s\n' "$$1" ;; \
	esac; \
	}; \
	operator_build_mode() { \
		production="$$(compose_env_value QBIT_PRODUCTION 0)"; \
		tools_production="$$(compose_env_value QBIT_TOOLS_PRODUCTION 0)"; \
		chain="$$(compose_env_value QBIT_CHAIN regtest)"; \
		case "$$(printf '%s:%s:%s' "$$production" "$$tools_production" "$$chain" | tr '[:upper:]' '[:lower:]')" in \
			1:*:*|true:*:*|yes:*:*|on:*:*|*:1:*|*:true:*|*:yes:*|*:on:*|*:*:main|*:*:mainnet) printf '%s\n' no-build ;; \
			*) printf '%s\n' build ;; \
		esac; \
	};
endef

.PHONY: doctor prism-self-check require-lab-mode test-builder test-builder-regtest test-prism-regtest test-prism-postgres-ledger test-prism-postgres-throughput test-prism-stratum-regtest-live test-prism-stratum-postgres-regtest-live test-prism-combined-regtest test-compose-prism-config up up-permissionless up-permissionless-pool test-permissionless test-permissionless-p2mr test-ckpool-bip310 up-real-miner up-permissionless-real test-real-miner up-auxpow up-auxpow-bridge up-auxpow-pool up-prism up-prism-pool up-dual-pools test-auxpow test-auxpow-stratum test-auxpow-stratum-bip310 test-auxpow-stratum-age smoke-all down purge-local-volumes

require-lab-mode:
	@bash scripts/check-env.sh --require-lab

test-builder-regtest test-prism-regtest test-prism-stratum-regtest-live \
test-prism-stratum-postgres-regtest-live test-prism-combined-regtest \
test-permissionless test-permissionless-p2mr test-ckpool-bip310 \
test-real-miner test-auxpow test-auxpow-stratum \
test-auxpow-stratum-bip310 test-auxpow-stratum-age: require-lab-mode

up-permissionless up-real-miner up-auxpow up-auxpow-bridge: require-lab-mode

doctor:
	@$(WITH_RESOLVED_QBIT)

prism-self-check: export MINING_LANES=prism
prism-self-check:
	python3 scripts/prism-self-check.py

test-builder:
	cargo test --workspace

test-builder-regtest:
	@$(WITH_RESOLVED_QBIT) \
	bash test/test-builder-regtest.sh

test-prism-regtest:
	@$(WITH_RESOLVED_QBIT) \
	bash test/test-prism-regtest.sh

test-prism-postgres-ledger:
	bash test/test-prism-postgres-ledger.sh

test-prism-postgres-throughput:
	bash test/test-prism-postgres-throughput.sh

test-prism-stratum-regtest-live:
	@$(WITH_RESOLVED_QBIT) \
	bash test/test-prism-stratum-regtest-live.sh

test-prism-stratum-postgres-regtest-live:
	@$(WITH_RESOLVED_QBIT) \
	QBIT_PRISM_LIVE_POSTGRES=1 QBIT_PRISM_LIVE_AUDIT_API=1 bash test/test-prism-stratum-regtest-live.sh

test-prism-combined-regtest:
	@$(WITH_RESOLVED_QBIT) \
	bash test/test-prism-combined-regtest.sh

test-compose-prism-config:
	@QBIT_SRC_DIR="$(CURDIR)" \
	PRISM_STRATUM_PORT_HOST=127.0.0.1:43340 \
	PRISM_STRATUM_PORT=43340 \
	$(COMPOSE) --profile prism config >/dev/null

up: up-dual-pools

up-permissionless:
	@$(WITH_RESOLVED_QBIT) \
	$(COMPOSE) --profile permissionless up --build

up-permissionless-pool: export MINING_LANES=ckpool
up-permissionless-pool:
	@$(WITH_RESOLVED_QBIT) \
	$(COMPOSE_ENV_HELPERS) \
	printf 'permissionless operator mode: bring your own Stratum miner\n'; \
	printf 'connect to stratum+tcp://%s\n' "$$(stratum_endpoint "$$(compose_env_value CKPOOL_STRATUM_PORT_HOST 3333)")"; \
	printf 'use a qbit payout address as the username, or leave QBIT_MINER_ADDRESS=%s for auto-generation\n' "$$(compose_env_value QBIT_MINER_ADDRESS auto)"; \
	if [[ "$$(operator_build_mode)" == no-build ]]; then \
		$(PRODUCTION_COMPOSE) --profile permissionless up -d --no-build --pull never qbitd ckpool; \
	else \
		$(COMPOSE) --profile permissionless up --build qbitd ckpool; \
	fi

test-permissionless:
	@$(WITH_RESOLVED_QBIT) \
	$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans || true; \
	trap '$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans' EXIT; \
	tmp_log="$$(mktemp)"; \
	set +e; \
	$(TEST_COMPOSE) --profile permissionless up --build --abort-on-container-exit --exit-code-from permissionless-miner 2>&1 | tee "$${tmp_log}"; \
	compose_status="$${PIPESTATUS[0]}"; \
	set -e; \
	if ! grep -q 'permissionless lab mined a qbit block' "$${tmp_log}"; then \
		rm -f "$${tmp_log}"; \
		exit 1; \
	fi; \
	rm -f "$${tmp_log}"; \
	exit "$${compose_status}"

test-permissionless-p2mr:
	@$(WITH_RESOLVED_QBIT) \
	$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans || true; \
	trap '$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans' EXIT; \
	tmp_log="$$(mktemp)"; \
	set +e; \
	QBIT_P2MR_ONLY_ARG=-p2mronly=1 \
	QBIT_MINER_ADDRESS=auto \
	$(TEST_COMPOSE) --profile permissionless up --build --abort-on-container-exit --exit-code-from permissionless-miner 2>&1 | tee "$${tmp_log}"; \
	compose_status="$${PIPESTATUS[0]}"; \
	set -e; \
	if ! grep -q 'permissionless lab mined a qbit block' "$${tmp_log}"; then \
		rm -f "$${tmp_log}"; \
		exit 1; \
	fi; \
	rm -f "$${tmp_log}"; \
	exit "$${compose_status}"

test-ckpool-bip310:
	@$(WITH_RESOLVED_QBIT) \
	$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans || true; \
	trap '$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans' EXIT; \
	ckpool_port_host="$${CKPOOL_STRATUM_PORT_HOST:-0}"; \
	QBIT_RPC_PORT_HOST="$${QBIT_RPC_PORT_HOST:-0}" \
	QBIT_P2P_PORT_HOST="$${QBIT_P2P_PORT_HOST:-0}" \
	QBIT_ZMQ_BLOCK_PORT_HOST="$${QBIT_ZMQ_BLOCK_PORT_HOST:-0}" \
	QBIT_ZMQ_TX_PORT_HOST="$${QBIT_ZMQ_TX_PORT_HOST:-0}" \
	CKPOOL_STRATUM_PORT_HOST="$${ckpool_port_host}" \
	$(TEST_COMPOSE) --profile permissionless up --build -d qbitd ckpool; \
	port="$$(QBIT_RPC_PORT_HOST="$${QBIT_RPC_PORT_HOST:-0}" \
		QBIT_P2P_PORT_HOST="$${QBIT_P2P_PORT_HOST:-0}" \
		QBIT_ZMQ_BLOCK_PORT_HOST="$${QBIT_ZMQ_BLOCK_PORT_HOST:-0}" \
		QBIT_ZMQ_TX_PORT_HOST="$${QBIT_ZMQ_TX_PORT_HOST:-0}" \
		CKPOOL_STRATUM_PORT_HOST="$${ckpool_port_host}" \
		$(TEST_COMPOSE) port ckpool 3333 | awk -F: 'END { print $$NF }')"; \
	deadline=$$((SECONDS+120)); \
	until bash -c "exec 3<>/dev/tcp/127.0.0.1/$${port}" 2>/dev/null; do \
		if [ $$SECONDS -ge $$deadline ]; then \
			$(TEST_COMPOSE) logs ckpool; \
			printf 'ckpool BIP-310 probe: FAIL - Stratum port did not open after 120s\n'; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	configured_mask="$${CKPOOL_VERSION_MASK:-1fffe000}"; \
	run_bip310_probe() { \
		while true; do \
			set +e; \
			python3 tests/stratum_client.py probe-bip310 "$$@"; \
			probe_status="$$?"; \
			set -e; \
			if [ "$${probe_status}" -eq 0 ]; then \
				return 0; \
			fi; \
			if [ "$${probe_status}" -ne 2 ] || [ $$SECONDS -ge $$deadline ]; then \
				$(TEST_COMPOSE) logs ckpool; \
				return "$${probe_status}"; \
			fi; \
			sleep 1; \
		done; \
	}; \
	run_bip310_probe --host 127.0.0.1 --port "$${port}" --configured-version-mask "$${configured_mask}"; \
	run_bip310_probe --host 127.0.0.1 --port "$${port}" --configured-version-mask "$${configured_mask}" --version-mask 0000f000 --version-min-bit-count 3

up-real-miner:
	@$(WITH_RESOLVED_QBIT) \
	$(COMPOSE) --profile real-miner-smoke up --build

# Deprecated compatibility alias; use up-real-miner.
up-permissionless-real:
	@printf 'up-permissionless-real is deprecated; use make up-real-miner\n'
	@$(MAKE) up-real-miner

test-real-miner:
	@$(WITH_RESOLVED_QBIT) \
	$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans || true; \
	trap '$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans' EXIT; \
	$(TEST_COMPOSE) --profile real-miner-smoke up --build --abort-on-container-exit --exit-code-from real-miner

up-auxpow:
	@$(WITH_RESOLVED_QBIT) \
	$(COMPOSE) --profile auxpow up --build qbitd bitcoind auxpow-coordinator

up-auxpow-bridge:
	@$(WITH_RESOLVED_QBIT) \
	printf 'auxpow lab mode: CPU-solving bridge for disposable test chains only\n'; \
	$(COMPOSE) --profile auxpow up --build qbitd bitcoind auxpow-bridge

up-auxpow-pool: export MINING_LANES=auxpow
up-auxpow-pool:
	@$(WITH_RESOLVED_QBIT) \
	$(COMPOSE_ENV_HELPERS) \
	printf 'auxpow operator mode: miner-facing Stratum URL for parent-chain work\n'; \
	printf 'connect to stratum+tcp://%s\n' "$$(stratum_endpoint "$$(compose_env_value AUXPOW_STRATUM_PORT_HOST 3335)")"; \
	printf 'configure QBIT_MINER_ADDRESS=%s for qbit child rewards and BITCOIN_MINER_ADDRESS=%s for parent-chain rewards\n' "$$(compose_env_value QBIT_MINER_ADDRESS auto)" "$$(compose_env_value BITCOIN_MINER_ADDRESS auto)"; \
	if [[ "$$(operator_build_mode)" == no-build ]]; then \
		$(PRODUCTION_COMPOSE) --profile auxpow up -d --no-build --pull never qbitd bitcoind auxpow-stratum; \
	else \
		$(COMPOSE) --profile auxpow up --build qbitd bitcoind auxpow-stratum; \
	fi

up-prism: up-prism-pool

up-prism-pool: export MINING_LANES=prism
up-prism-pool:
	@$(WITH_RESOLVED_QBIT) \
	$(COMPOSE_ENV_HELPERS) \
	missing=0; \
	for name in PRISM_MANIFEST_SIGNING_SEED_HEX PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX; do \
		if [ -z "$$(compose_env_value "$$name")" ]; then \
			printf 'prism operator env: %s is required\n' "$$name" >&2; \
			missing=1; \
		fi; \
	done; \
	if [ "$${missing}" -ne 0 ]; then \
		printf 'prism operator env: set real PRISM signing keys in .env before running make up-prism-pool\n' >&2; \
		printf 'prism operator env: keep PRISM_ALLOW_TEST_SIGNING_SEEDS=0 and PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY=0 for deploys\n' >&2; \
		exit 1; \
	fi; \
	printf 'PRISM operator mode: direct qbit Stratum with Postgres ledger\n'; \
	printf 'connect to stratum+tcp://%s\n' "$$(stratum_endpoint "$$(compose_env_value PRISM_STRATUM_PORT_HOST 3340)")"; \
	if [ -n "$$(compose_env_value PRISM_STRATUM_HIGHDIFF_PORT "")" ]; then \
		highdiff_port_host="$$(compose_env_value PRISM_STRATUM_HIGHDIFF_PORT_HOST "")"; \
		if [ -n "$$highdiff_port_host" ]; then \
			printf 'high-diff rental port: stratum+tcp://%s\n' "$$(stratum_endpoint "$$highdiff_port_host")"; \
		else \
			printf 'high-diff listener enabled but not published; set PRISM_STRATUM_HIGHDIFF_PORT_HOST (e.g. 4334)\n'; \
		fi; \
	fi; \
	printf 'audit HTTP stays inside the coordinator namespace at %s:%s\n' "$$(compose_env_value PRISM_AUDIT_BIND 127.0.0.1)" "$$(compose_env_value PRISM_AUDIT_PORT 3341)"; \
	if [[ "$$(operator_build_mode)" == no-build ]]; then \
		$(PRODUCTION_COMPOSE) --profile prism up -d --no-build --pull never qbitd prism-postgres prism-coordinator; \
	else \
		$(COMPOSE) --profile prism up --build qbitd prism-postgres prism-coordinator; \
	fi

up-dual-pools: export MINING_LANES=ckpool,auxpow
up-dual-pools:
	@$(WITH_RESOLVED_QBIT) \
	$(COMPOSE_ENV_HELPERS) \
	printf 'dual operator mode: permissionless plus AuxPoW Stratum on one host\n'; \
	printf 'permissionless URL: stratum+tcp://%s\n' "$$(stratum_endpoint "$$(compose_env_value CKPOOL_STRATUM_PORT_HOST 3333)")"; \
	printf 'auxpow URL: stratum+tcp://%s\n' "$$(stratum_endpoint "$$(compose_env_value AUXPOW_STRATUM_PORT_HOST 3335)")"; \
	printf 'qbit child rewards use QBIT_MINER_ADDRESS=%s; AuxPoW parent rewards use BITCOIN_MINER_ADDRESS=%s\n' "$$(compose_env_value QBIT_MINER_ADDRESS auto)" "$$(compose_env_value BITCOIN_MINER_ADDRESS auto)"; \
	if [[ "$$(operator_build_mode)" == no-build ]]; then \
		$(PRODUCTION_COMPOSE) --profile permissionless --profile auxpow up -d --no-build --pull never qbitd ckpool bitcoind auxpow-stratum; \
	else \
		$(COMPOSE) --profile permissionless --profile auxpow up --build qbitd ckpool bitcoind auxpow-stratum; \
	fi

test-auxpow:
	@$(WITH_RESOLVED_QBIT) \
	$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans || true; \
	trap '$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans' EXIT; \
	$(TEST_COMPOSE) --profile auxpow up --build --abort-on-container-exit --exit-code-from auxpow-coordinator qbitd bitcoind auxpow-coordinator

test-auxpow-stratum:
	@$(WITH_RESOLVED_QBIT) \
	$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans || true; \
	trap '$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans' EXIT; \
	AUXPOW_STRATUM_VARDIFF_ENABLED=0 \
	AUXPOW_STRATUM_SHARE_DIFF=0.0001 \
	AUXPOW_STRATUM_MIN_ADVERTISED_DIFF=0.0001 \
	$(TEST_COMPOSE) --profile auxpow up --build --abort-on-container-exit --exit-code-from auxpow-real-miner qbitd bitcoind auxpow-stratum auxpow-real-miner

test-auxpow-stratum-bip310:
	@$(WITH_RESOLVED_QBIT) \
	$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans || true; \
	trap '$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans' EXIT; \
	auxpow_port_host="$${AUXPOW_STRATUM_PORT_HOST:-0}"; \
	QBIT_RPC_PORT_HOST="$${QBIT_RPC_PORT_HOST:-0}" \
	QBIT_P2P_PORT_HOST="$${QBIT_P2P_PORT_HOST:-0}" \
	QBIT_ZMQ_BLOCK_PORT_HOST="$${QBIT_ZMQ_BLOCK_PORT_HOST:-0}" \
	QBIT_ZMQ_TX_PORT_HOST="$${QBIT_ZMQ_TX_PORT_HOST:-0}" \
	BITCOIN_RPC_PORT_HOST="$${BITCOIN_RPC_PORT_HOST:-0}" \
	BITCOIN_P2P_PORT_HOST="$${BITCOIN_P2P_PORT_HOST:-0}" \
	AUXPOW_STRATUM_PORT_HOST="$${auxpow_port_host}" \
	$(TEST_COMPOSE) --profile auxpow up --build -d qbitd bitcoind auxpow-stratum; \
	port="$$(QBIT_RPC_PORT_HOST="$${QBIT_RPC_PORT_HOST:-0}" \
		QBIT_P2P_PORT_HOST="$${QBIT_P2P_PORT_HOST:-0}" \
		QBIT_ZMQ_BLOCK_PORT_HOST="$${QBIT_ZMQ_BLOCK_PORT_HOST:-0}" \
		QBIT_ZMQ_TX_PORT_HOST="$${QBIT_ZMQ_TX_PORT_HOST:-0}" \
		BITCOIN_RPC_PORT_HOST="$${BITCOIN_RPC_PORT_HOST:-0}" \
		BITCOIN_P2P_PORT_HOST="$${BITCOIN_P2P_PORT_HOST:-0}" \
		AUXPOW_STRATUM_PORT_HOST="$${auxpow_port_host}" \
		$(TEST_COMPOSE) port auxpow-stratum 3335 | awk -F: 'END { print $$NF }')"; \
	deadline=$$((SECONDS+120)); \
	until bash -c "exec 3<>/dev/tcp/127.0.0.1/$${port}" 2>/dev/null; do \
		if [ $$SECONDS -ge $$deadline ]; then \
			$(TEST_COMPOSE) logs auxpow-stratum; \
			printf 'auxpow stratum BIP-310 probe: FAIL - Stratum port did not open after 120s\n'; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	python3 tests/stratum_client.py probe-bip310 --host 127.0.0.1 --port "$${port}" --configured-version-mask "$${AUXPOW_STRATUM_VERSION_MASK:-1fffe000}" --skip-extranonce-subscribe

test-auxpow-stratum-age:
	@$(WITH_RESOLVED_QBIT) \
	export AUXPOW_STRATUM_JOB_MAX_AGE_SECONDS=10 AUXPOW_STRATUM_POLL_SECONDS=2; \
	$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans || true; \
	trap '$(TEST_COMPOSE_ALL_PROFILES) down -v --remove-orphans' EXIT; \
	$(TEST_COMPOSE) --profile auxpow up --build -d qbitd bitcoind auxpow-stratum; \
	deadline=$$((SECONDS+60)); \
	until $(TEST_COMPOSE) logs auxpow-stratum 2>/dev/null | grep -q 'reason=age'; do \
		if [ $$SECONDS -ge $$deadline ]; then \
			$(TEST_COMPOSE) logs auxpow-stratum; \
			printf 'auxpow stratum age refresh smoke: FAIL — no reason=age in logs after 60s\n'; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	printf 'auxpow stratum age refresh smoke: PASS\n'

smoke-all: test-permissionless test-auxpow

down:
	@$(COMPOSE_ALL_PROFILES) --profile prism down --remove-orphans

purge-local-volumes:
	@set -euo pipefail; \
	$(COMPOSE_ENV_HELPERS) \
	preserve_status_cleanup() { status=$$?; trap - EXIT; rm -f "$$compose_env"; exit $$status; }; \
	trap preserve_status_cleanup EXIT; \
	refuse() { printf 'purge-local-volumes: REFUSED - %s\n' "$$1" >&2; exit 2; }; \
	if ! lab_guard_output="$$(bash scripts/check-env.sh --require-lab 2>&1)"; then \
		refuse "$$lab_guard_output"; \
	fi; \
	production="$$(compose_env_value QBIT_PRODUCTION 0)"; \
	qbit_chain="$$(compose_env_value QBIT_CHAIN regtest)"; \
	qbit_chain_flag="$$(compose_env_value QBIT_CHAIN_FLAG -regtest)"; \
	bitcoin_chain="$$(compose_env_value BITCOIN_CHAIN regtest)"; \
	bitcoin_chain_flag="$$(compose_env_value BITCOIN_CHAIN_FLAG -regtest)"; \
	production_normalized="$$(printf '%s' "$$production" | tr '[:upper:]' '[:lower:]')"; \
	qbit_chain_normalized="$$(printf '%s' "$$qbit_chain" | tr '[:upper:]' '[:lower:]')"; \
	bitcoin_chain_normalized="$$(printf '%s' "$$bitcoin_chain" | tr '[:upper:]' '[:lower:]')"; \
	case "$$production_normalized" in \
		1|true|yes|on) refuse 'QBIT_PRODUCTION enables production safeguards' ;; \
	esac; \
	case "$$qbit_chain_normalized" in \
		main|mainnet) refuse "QBIT_CHAIN=$${qbit_chain} is a main-chain configuration" ;; \
	esac; \
	case "$$bitcoin_chain_normalized" in \
		main|mainnet) refuse "BITCOIN_CHAIN=$${bitcoin_chain} is a main-chain configuration" ;; \
	esac; \
	for flag in $$qbit_chain_flag; do \
		flag_normalized="$$(printf '%s' "$$flag" | tr '[:upper:]' '[:lower:]')"; \
		case "$$flag_normalized" in \
			-main|-mainnet|-chain=main|-chain=mainnet) refuse "QBIT_CHAIN_FLAG contains $${flag}" ;; \
		esac; \
	done; \
	for flag in $$bitcoin_chain_flag; do \
		flag_normalized="$$(printf '%s' "$$flag" | tr '[:upper:]' '[:lower:]')"; \
		case "$$flag_normalized" in \
			-main|-mainnet|-chain=main|-chain=mainnet) refuse "BITCOIN_CHAIN_FLAG contains $${flag}" ;; \
		esac; \
	done; \
	if [[ "$${PURGE_CONFIRM:-}" != 'delete-all-local-mining-data' ]]; then \
		printf '%s\n' 'purge-local-volumes: this permanently deletes local chain, ledger, audit, and miner state.' >&2; \
		printf '%s\n' 'Re-run only on a disposable non-production stack with PURGE_CONFIRM=delete-all-local-mining-data.' >&2; \
		exit 2; \
	fi; \
	$(COMPOSE_ALL_PROFILES) --profile prism down -v --remove-orphans

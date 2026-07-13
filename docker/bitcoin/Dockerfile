# syntax=docker/dockerfile:1.7

ARG UBUNTU_VERSION=24.04

FROM ubuntu:${UBUNTU_VERSION}

ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH
ARG BITCOIN_RELEASE_VERSION
ARG BITCOIN_RELEASE_BASE_URL
ARG BITCOIN_RELEASE_URL
ARG BITCOIN_RELEASE_SHA256_AMD64
ARG BITCOIN_RELEASE_SHA256_ARM64

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN set -eu; \
    case "${TARGETARCH}" in \
      amd64) release_arch="x86_64-linux-gnu"; release_sha256="${BITCOIN_RELEASE_SHA256_AMD64}" ;; \
      arm64) release_arch="aarch64-linux-gnu"; release_sha256="${BITCOIN_RELEASE_SHA256_ARM64}" ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    printf '%s' "${release_sha256}" | grep -Eq '^[0-9a-f]{64}$' \
      || (echo "A 64-character lowercase Bitcoin Core SHA256 is required for TARGETARCH=${TARGETARCH}" >&2 && exit 1); \
    if [ -n "${BITCOIN_RELEASE_URL}" ]; then \
      release_url="${BITCOIN_RELEASE_URL}"; \
    else \
      test -n "${BITCOIN_RELEASE_VERSION}" || (echo "BITCOIN_RELEASE_VERSION is required when BITCOIN_RELEASE_URL is unset" >&2 && exit 1); \
      test -n "${BITCOIN_RELEASE_BASE_URL}" || (echo "BITCOIN_RELEASE_BASE_URL is required when BITCOIN_RELEASE_URL is unset" >&2 && exit 1); \
      release_url="${BITCOIN_RELEASE_BASE_URL}/bitcoin-${BITCOIN_RELEASE_VERSION}-${release_arch}.tar.gz"; \
    fi; \
    curl -fsSL "${release_url}" -o /tmp/bitcoin.tar.gz; \
    printf '%s  %s\n' "${release_sha256}" /tmp/bitcoin.tar.gz | sha256sum -c -; \
    tar -xzf /tmp/bitcoin.tar.gz -C /tmp \
    && find /tmp -type f -name bitcoind -exec install -m 0755 {} /usr/local/bin/bitcoind \; \
    && find /tmp -type f -name bitcoin-cli -exec install -m 0755 {} /usr/local/bin/bitcoin-cli \; \
    && test -x /usr/local/bin/bitcoind \
    && test -x /usr/local/bin/bitcoin-cli \
    && rm -rf /tmp/bitcoin.tar.gz /tmp/*

COPY docker/bitcoin/entrypoint.sh /usr/local/bin/bitcoin-entrypoint.sh

RUN chmod 0755 /usr/local/bin/bitcoin-entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/bitcoin-entrypoint.sh"]
CMD ["-printtoconsole"]

# syntax=docker/dockerfile:1.7

ARG UBUNTU_VERSION=24.04

FROM ubuntu:${UBUNTU_VERSION} AS build-from-source

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    git \
    libboost-dev \
    libevent-dev \
    libsqlite3-dev \
    libssl-dev \
    libzmq3-dev \
    pkg-config \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/qbit
COPY --from=qbit-src . .
RUN rm -rf build

RUN cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_DAEMON=ON \
    -DBUILD_CLI=ON \
    -DBUILD_GUI=OFF \
    -DENABLE_WALLET=ON \
    -DENABLE_IPC=OFF \
    -DWITH_ZMQ=ON \
    -DBUILD_TESTS=OFF \
    -DBUILD_BENCH=OFF \
    -DBUILD_FUZZ_BINARY=OFF

RUN cmake --build build --target bitcoind bitcoin-cli -j"$(nproc)"
RUN cmake --install build --prefix /opt/qbit --component qbitd \
    && cmake --install build --prefix /opt/qbit --component qbit-cli \
    && if [ ! -x /opt/qbit/bin/qbitd ]; then \
        if [ -x build/bin/qbitd ]; then install -m 0755 -D build/bin/qbitd /opt/qbit/bin/qbitd; \
        elif [ -x build/bin/bitcoind ]; then install -m 0755 -D build/bin/bitcoind /opt/qbit/bin/qbitd; \
        else echo "missing qbit daemon build artifact" >&2; exit 1; fi; \
    fi \
    && if [ ! -x /opt/qbit/bin/qbit-cli ]; then \
        if [ -x build/bin/qbit-cli ]; then install -m 0755 -D build/bin/qbit-cli /opt/qbit/bin/qbit-cli; \
        elif [ -x build/bin/bitcoin-cli ]; then install -m 0755 -D build/bin/bitcoin-cli /opt/qbit/bin/qbit-cli; \
        else echo "missing qbit CLI build artifact" >&2; exit 1; fi; \
    fi

FROM ubuntu:${UBUNTU_VERSION} AS runtime

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libevent-core-2.1-7t64 \
    libevent-extra-2.1-7t64 \
    libevent-pthreads-2.1-7t64 \
    libsqlite3-0 \
    libssl3 \
    libzmq5 \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build-from-source /opt/qbit/bin/qbitd /usr/local/bin/qbitd
COPY --from=build-from-source /opt/qbit/bin/qbit-cli /usr/local/bin/qbit-cli
COPY --chmod=0755 docker/qbit/qbit-entrypoint.sh /usr/local/bin/qbit-entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/qbit-entrypoint.sh"]
CMD ["-printtoconsole"]

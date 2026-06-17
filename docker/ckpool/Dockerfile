# syntax=docker/dockerfile:1.7

ARG UBUNTU_VERSION=24.04

FROM ubuntu:${UBUNTU_VERSION} AS build

ARG DEBIAN_FRONTEND=noninteractive
ARG CKPOOL_GIT_URL
ARG CKPOOL_GIT_REF

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    build-essential \
    ca-certificates \
    git \
    libcurl4-openssl-dev \
    libjansson-dev \
    libtool \
    libzmq3-dev \
    pkgconf \
    yasm \
    && rm -rf /var/lib/apt/lists/*

COPY docker/ckpool/qbit-regtest.patch /tmp/qbit-regtest.patch
COPY docker/ckpool/qbit-signet-gbt.patch /tmp/qbit-signet-gbt.patch

WORKDIR /src/ckpool
RUN git init . \
    && git remote add origin "${CKPOOL_GIT_URL}" \
    && git fetch --depth 1 origin "${CKPOOL_GIT_REF}" \
    && git checkout FETCH_HEAD

RUN git apply /tmp/qbit-regtest.patch \
    && git apply /tmp/qbit-signet-gbt.patch

RUN ./autogen.sh \
    && ./configure \
    && make -C src/jansson-2.14/src -j"$(nproc)" \
    && make -C src -j"$(nproc)" ckpool

FROM ubuntu:${UBUNTU_VERSION}

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    libcurl4 \
    libjansson4 \
    libzmq5 \
    python3 \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/ckpool/src/ckpool /usr/local/bin/ckpool
COPY docker/ckpool/ckpool-version-mask.py /usr/local/bin/ckpool-version-mask
COPY docker/ckpool/qbit-ckpool-preflight.py /usr/local/bin/qbit-ckpool-preflight
COPY docker/ckpool/start-ckpool.sh /usr/local/bin/start-ckpool.sh

RUN chmod +x /usr/local/bin/ckpool-version-mask /usr/local/bin/qbit-ckpool-preflight /usr/local/bin/start-ckpool.sh

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/start-ckpool.sh"]

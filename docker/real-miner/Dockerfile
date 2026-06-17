# syntax=docker/dockerfile:1.7

ARG UBUNTU_VERSION=24.04

FROM ubuntu:${UBUNTU_VERSION} AS build

ARG DEBIAN_FRONTEND=noninteractive
ARG CPUMINER_GIT_URL
ARG CPUMINER_GIT_REF

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    build-essential \
    ca-certificates \
    git \
    libcurl4-openssl-dev \
    libgmp-dev \
    libjansson-dev \
    libtool \
    pkgconf \
    yasm \
    && rm -rf /var/lib/apt/lists/*

COPY docker/real-miner/cpuminer-regtest.patch /tmp/cpuminer-regtest.patch

WORKDIR /src/cpuminer-opt
RUN git init . \
    && git remote add origin "${CPUMINER_GIT_URL}" \
    && git fetch --depth 1 origin "${CPUMINER_GIT_REF}" \
    && git checkout FETCH_HEAD

RUN git apply /tmp/cpuminer-regtest.patch

RUN ./autogen.sh \
    && ./configure LIBS="-lcurl" \
    && make -j"$(nproc)"

FROM ubuntu:${UBUNTU_VERSION}

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    libcurl4 \
    libgmp10 \
    libjansson4 \
    libssl3 \
    tini \
    python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/cpuminer-opt/cpuminer /usr/local/bin/cpuminer
COPY lab/real-miner/real_miner.py /usr/local/bin/real_miner.py

RUN chmod +x /usr/local/bin/real_miner.py

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3", "/usr/local/bin/real_miner.py"]

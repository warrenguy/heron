# Build pinned C media driver; extract aeronmd via scripts/build-aeronmd.sh.
FROM ubuntu:24.04 AS builder

ARG AERON_REF=1.51.0
ARG AERON_REPOSITORY=https://github.com/aeron-io/aeron.git

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		build-essential \
		ca-certificates \
		curl \
		git \
		libbsd-dev \
		uuid-dev \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone --depth 1 --branch "${AERON_REF}" "${AERON_REPOSITORY}" aeron

WORKDIR /src/aeron
RUN ./cppbuild/cppbuild --no-tests --skip-archive-api \
	&& test -x cppbuild/Release/binaries/aeronmd_s

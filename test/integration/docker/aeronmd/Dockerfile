# Build pinned C media driver.
# scripts/build-aeronmd.sh: --target builder, extract to bin/aeronmd
# make aeronmd-compose-image: --target runtime-bin (COPY bin/aeronmd)
# runtime: in-image C compile when host bin/aeronmd is absent (manual docker build only)
FROM debian:bookworm-slim AS builder

ARG AERON_REF=1.53.3
ARG AERON_REPOSITORY=https://github.com/aeron-io/aeron.git
ARG AERON_DRIVER_ARTIFACT=aeronmd_s

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
	&& test -x "cppbuild/Release/binaries/${AERON_DRIVER_ARTIFACT}"

FROM debian:bookworm-slim AS runtime-base

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		libbsd0 \
		uuid-runtime \
	&& rm -rf /var/lib/apt/lists/*

ENV AERON_TERM_BUFFER_LENGTH=65536
ENV AERON_IPC_TERM_BUFFER_LENGTH=65536
ENV AERON_THREADING_MODE=SHARED

FROM runtime-base AS runtime

ARG AERON_DRIVER_ARTIFACT=aeronmd_s

COPY --from=builder \
	/src/aeron/cppbuild/Release/binaries/${AERON_DRIVER_ARTIFACT} \
	/usr/local/bin/aeronmd

ENTRYPOINT ["/usr/local/bin/aeronmd"]

FROM runtime-base AS runtime-bin

COPY bin/aeronmd /usr/local/bin/aeronmd

ENTRYPOINT ["/usr/local/bin/aeronmd"]

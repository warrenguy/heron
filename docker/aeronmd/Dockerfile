# Build pinned C media driver.
# scripts/build-aeronmd.sh: --target builder, extract to bin/aeronmd
# docker/java-interop compose: --target runtime
FROM ubuntu:24.04 AS builder

ARG AERON_REF=1.51.0
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

FROM ubuntu:24.04 AS runtime

ARG AERON_DRIVER_ARTIFACT=aeronmd_s

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		libbsd0 \
		uuid-runtime \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder \
	/src/aeron/cppbuild/Release/binaries/${AERON_DRIVER_ARTIFACT} \
	/usr/local/bin/aeronmd

ENV AERON_TERM_BUFFER_LENGTH=65536
ENV AERON_IPC_TERM_BUFFER_LENGTH=65536
ENV AERON_THREADING_MODE=SHARED

ENTRYPOINT ["/usr/local/bin/aeronmd"]

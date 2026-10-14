# syntax=docker/dockerfile:1
# Go test image for the compose smokes. SUITE names a directory under
# test/integration. The compose service command picks the tests to run.
ARG GO_VERSION=1.26.4
FROM golang:${GO_VERSION} AS builder

WORKDIR /src

COPY . .

# Keep ARG below the shared layers: BuildKit keys each later RUN on its value.
ARG SUITE

# All suite tags. SUITE picks the package. Tags only pick files inside it.
RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	go test -c -race \
	-tags=integration,archive_java,cluster_java,cluster_mixed,java_interop \
	-o /out/suite.test "./test/integration/${SUITE:?SUITE build arg is required}/"

FROM ubuntu:24.04 AS base

WORKDIR /app

COPY --from=builder /out/suite.test /app/suite.test

ENTRYPOINT ["./suite.test", "-test.v"]

# The failover smoke kills a cluster member through the host docker.sock.
FROM docker:27-cli AS docker-cli

FROM base AS failover-runtime

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli \
	/usr/local/libexec/docker/cli-plugins/docker-compose \
	/usr/local/libexec/docker/cli-plugins/docker-compose
# Not a bind mount: the host daemon cannot see container paths.
COPY test/integration/cluster_java/docker/compose.yaml \
	test/integration/cluster_java/docker/compose.failover.yaml \
	/app/compose/

FROM base AS runtime

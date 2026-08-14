# syntax=docker/dockerfile:1
ARG GO_VERSION=1.26.4
# Image pin for failover-runtime: Docker client and compose plugin only.
# Paths match docker:27-cli. Talks to host docker.sock.
ARG DOCKER_CLI_IMAGE=docker:27-cli
FROM golang:${GO_VERSION} AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY aeron ./aeron
COPY atomicbuffer ./atomicbuffer
COPY idlestrategy ./idlestrategy
COPY mmap ./mmap
COPY record ./record
COPY ringbuffer ./ringbuffer
COPY broadcast ./broadcast
COPY timerwheel ./timerwheel
COPY internal ./internal
# cluster/client + cluster/codecs (incl. clusterwire) for AsyncConnect + echo compose smokes.
# cluster open connects archive.Client; election/consensus import archivewire.
COPY archive ./archive
COPY cluster ./cluster
COPY test/integration/harness ./test/integration/harness
COPY test/integration/cluster_java ./test/integration/cluster_java

RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	go test -c -race -tags=integration,cluster_java \
	-o /out/cluster-java.test ./test/integration/cluster_java/

FROM ubuntu:24.04 AS runtime

WORKDIR /app

COPY --from=builder /out/cluster-java.test /app/cluster-java.test

# Inner -test.timeout=300s: connect poll 60s; echo+admin offer retry 10s each;
# echo egress 30s; admin egress 60s. Stacked worst case ~230s plus overhead (~70s slack).

# Outer cluster-java-smoke.sh timeout 310s: 300s test budget plus compose teardown.

CMD ["./cluster-java.test", "-test.v", "-test.count=1", "-test.timeout=300s", "-test.run", "^TestCluster(Connect|Echo)Smoke$"]

# Failover smoke kills a cluster member via host docker.sock. This stage
# supplies the Docker client and compose plugin only.
FROM ${DOCKER_CLI_IMAGE} AS docker-cli

FROM runtime AS failover-runtime

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli \
	/usr/local/libexec/docker/cli-plugins/docker-compose \
	/usr/local/libexec/docker/cli-plugins/docker-compose

COPY docker/cluster_java/compose.yaml docker/cluster_java/compose.failover.yaml /app/compose/

# Inner -test.timeout=400s: connect 90s, pre-kill echo 40s, recovery 120s,
# post-recovery echo 120s. Stacked worst case ~375s plus slack.

# Outer cluster-java-failover-smoke.sh timeout 430s: 400s test budget plus teardown.

CMD ["./cluster-java.test", "-test.v", "-test.count=1", "-test.timeout=400s", "-test.run", "^TestClusterFailoverSmoke$"]

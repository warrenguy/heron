# syntax=docker/dockerfile:1
ARG GO_VERSION=1.26.4
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
# cluster/client and cluster/codecs (incl. clusterwire) for induction client.
# election/consensus import archivewire.
COPY archive/codecs/archivewire ./archive/codecs/archivewire
COPY cluster ./cluster
COPY test/integration/harness ./test/integration/harness
# Mixed goldens live under clusterjava (same package as single-node and failover).
COPY test/integration/cluster_java/clusterjava ./test/integration/cluster_java/clusterjava
COPY test/integration/cluster_mixed ./test/integration/cluster_mixed

RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	go test -c -race -tags=integration,cluster_mixed,cluster_java \
	-o /out/cluster-mixed.test ./test/integration/cluster_mixed/

FROM ubuntu:24.04 AS java-leader-runtime

WORKDIR /app

COPY --from=builder /out/cluster-mixed.test /app/cluster-mixed.test

CMD ["./cluster-mixed.test", "-test.v", "-test.count=1", "-test.timeout=120s", "-test.run", "^TestClusterMixedJavaLeader$"]

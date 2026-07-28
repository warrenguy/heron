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
# harness ClusterMemberSet pulls cluster; election/consensus need archivewire.
COPY archive/codecs/archivewire ./archive/codecs/archivewire
COPY cluster ./cluster
COPY test/integration/harness ./test/integration/harness
COPY test/integration/java_interop ./test/integration/java_interop

RUN --mount=type=cache,target=/go/pkg/mod \
--mount=type=cache,target=/root/.cache/go-build \
go test -c -race -tags=integration,java_interop \
	-o /out/java-interop.test ./test/integration/java_interop/

FROM ubuntu:24.04 AS runtime

WORKDIR /app
COPY --from=builder /out/java-interop.test /app/java-interop.test

CMD ["./java-interop.test", "-test.v", "-test.count=1", "-test.timeout=120s", "-test.run", "TestJavaInteropDuplex"]

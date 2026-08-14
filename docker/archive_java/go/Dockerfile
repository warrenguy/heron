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
COPY timerwheel ./timerwheel
COPY internal ./internal
COPY archive ./archive
# harness ClusterMemberSet imports package cluster (not only recordinglog).
COPY cluster ./cluster
COPY test/integration/harness ./test/integration/harness
COPY test/integration/archive_java ./test/integration/archive_java

RUN --mount=type=cache,target=/go/pkg/mod \
--mount=type=cache,target=/root/.cache/go-build \
go test -c -race -tags=integration,archive_java \
	-o /out/archive-java.test ./test/integration/archive_java/

FROM ubuntu:24.04 AS runtime

WORKDIR /app
COPY --from=builder /out/archive-java.test /app/archive-java.test

CMD ["./archive-java.test", "-test.v", "-test.count=1", "-test.timeout=120s", "-test.run", "TestArchiveRecordingLog|TestArchiveTruncatePath"]

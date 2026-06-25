# syntax=docker/dockerfile:1
ARG GO_VERSION=1.26.4
FROM golang:${GO_VERSION} AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY aeron ./aeron
COPY agrona ./agrona
# cluster/client + cluster/codecs (incl. generated/sbe) for AsyncConnect + echo compose smokes.
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

# Inner -test.timeout=200s: connect poll 60s per test; echo offer retry 10s and egress
# poll 30s; stacked worst case 60+10+30+60=160s plus overhead; 40s slack.
# Outer scripts/cluster-java-smoke.sh timeout 210 stays adequate (~10s compose teardown).
CMD ["./cluster-java.test", "-test.v", "-test.count=1", "-test.timeout=200s", "-test.run", "^TestCluster(Connect|Echo)Smoke$"]

# Examples

Runnable `package main` entrypoints use `//go:build example` and are excluded from default
`go test ./...`; run them with `go run -tags=example ./examples/<name>/`. Importable
subpackages (e.g. `auctionwire`) have no `example` tag and are included in `make check`.

Upstream Java sources live in the Aeron checkout at the pin in [`specs/versions.yaml`](../specs/versions.yaml).

| Example | Upstream | Server | Default topology |
|---------|----------|--------|------------------|
| [`cluster-auction-client`](cluster-auction-client/README.md) | `io.aeron.samples.cluster.tutorial.BasicAuctionClusterClient` (aeron-samples) | `BasicAuctionClusteredServiceNode` (`BasicAuctionClusteredService`) | UDP 3-node |

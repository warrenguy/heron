# Contributing

Development workflows for this repository. User-facing overview and usage examples live in
[README.md](README.md).

## Building blocks

| Area | `github.com/warrenguy/heron/...` | Detail |
|------|-------------|--------|
| Agrona primitives | `agrona/...` | [`agrona/README.md`](agrona/README.md) |
| Cluster client | `cluster/client` | `ClusterContext`, `AsyncConnect`, `AeronCluster`, egress handlers |
| Cluster codecs | `cluster/codecs` | SBE flyweights in `gensbe/`; see **SBE codecs** below |
| Archive codecs / client | `archive/codecs`, `archive/client` | SBE flyweights in `gensbe/`; `ArchiveContext`, `AsyncConnect`, `ArchiveProxy` - see [capabilities](README.md#capabilities) |
| Transport client | `aeron` | `Connect`, `Client`, `Publication`, `Subscription`, `ExclusivePublication` |
| Transport internals | `aeron/{conductor,cnc,logbuffer}` | Conductor, CnC, logbuffer |

## Pinned dependencies

SBE tool and Aeron C media driver are pinned in [`specs/versions.yaml`](specs/versions.yaml).

## Tests

```bash
make check        # lint, pins, SBE/catalog stale (opt), unit+stress, contract-oracles (opt)
make test-all     # check + rapid + integration + java-interop + cluster-java smokes
make test-rapid                  # property tests (pgregory.net/rapid)
make test-integration            # live C aeronmd (-tags=integration)
make contract-oracles            # all three contract suites below (needs mvn + java)
make test-java-interop-contract  # Go/Java smoke: streams, payloads, timeouts (no driver)
make test-cluster-sbe-contract   # Go/Java cluster SBE wire bytes vs catalog.json (no driver)
make test-cluster-java-contract  # Go/Java cluster harness constants (no driver)
make test-java-interop           # Docker Compose smoke (bidirectional IPC pub/sub)
make test-cluster-java           # Docker Compose smoke (AsyncConnect + echo roundtrip)
make test-cluster-java-failover  # Docker Compose 3-node leader failover
```

- Contract targets run `mvn test` (Java goldens) then matching Go oracle tests; no aeronmd or
  Docker.
- `test-java-interop-contract` checks shared constants for `make test-java-interop` (e.g. stream
  1100/1101, 32x32-byte `java-N` / `go-N` payloads) before the live smoke run.
- `test-cluster-java-contract` checks shared harness constants for `make test-cluster-java`
  (e.g. IPC channels, streams 101/102, ports 9000/9002, failover topology goldens) before the live smoke run.
- `test-java-interop` and `test-cluster-java-failover` run `make aeronmd-compose-image` first to tag
  `java-interop-aeronmd`. `test-cluster-java` uses an embedded driver and does not. Direct
  `./scripts/java-interop-smoke.sh` or `./scripts/cluster-java-failover-smoke.sh` require
  `make aeronmd-compose-image` (or a prior `make test-*`).
- `make test-integration` builds `bin/aeronmd` when needed; harness sets `AERON_DIR` per test.
- Unit tests use `bench` and `testhooks` build tags.
- Property tests live in `*_rapid_test.go` behind the `rapid` tag.
- IDE/gopls: set `go.buildTags` to
  `testhooks rapid stress integration java_interop cluster_sbe cluster_java bench`.

```bash
make test                                             # unit only
go test -race -tags=bench,testhooks ./...             # ad-hoc unit
go test -race -tags=rapid,testhooks -run rapid ./...  # ad-hoc property
```

Prefer Makefile targets over bare `go test`.

## SBE codecs (cluster + archive)

Cluster wire XML: `specs/cluster-codecs-client.xml`; flyweights:
`cluster/codecs/gensbe/`. Archive control wire XML:
`specs/archive-codecs-client.xml`; flyweights: `archive/codecs/gensbe/`.
Cluster fixture oracle rows:
`test/integration/cluster_sbe/clustersbe/fixtures/catalog.json`; run
`make cluster-sbe-catalog-regen` to refresh committed `hex` from `fields`.

Pin-bump workflow:

1. Bump `aeron.ref` in [`specs/versions.yaml`](specs/versions.yaml).
2. Cluster: diff upstream
   `aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml` (client
   messages and shared types only); port into `specs/cluster-codecs-client.xml`.
3. Archive: diff upstream
   `aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml` (control
   request, response, descriptor, and recording-signal messages plus shared
   types only); port into `specs/archive-codecs-client.xml`.
4. `make sbe-generate` and `make sbe-generate-archive`; commit flyweight diffs
   if any.
5. `make test-cluster-sbe-contract`; fix failures under `cluster/codecs/` and
   `test/integration/cluster_sbe/clustersbe/`.

## Benchmarks

| Target | Tags | Packages |
|--------|------|----------|
| `make bench` (+ baseline, regression) | `bench` | `./test/bench/...` |
| `make bench-micro` | `testhooks` | `./agrona/...` `./aeron/...` white-box |

```bash
make bench             # contract suite (quick)
make bench-baseline    # .bench/baseline.txt (gitignored, per machine)
make bench-regression  # vs baseline via benchstat; exit 1 on alloc regression (CI)
make bench-micro       # white-box in-package (informational)
```

Re-baseline after pulling `main` or when perf changes are intentional.

Optional env: `BENCH_COUNT` (Makefile default 10), `BENCH_PAYLOAD_SIZES` (comma-separated
filter; CI uses `32,1400`), `BENCH_THRESHOLD` (script default 10; CI sets 30 in
`bench.yml`), `SKIP_BENCH_REGRESSION`.

## Devcontainer

VS Code / GitHub Codespaces configs live under `.devcontainer/`. Default (check) covers
`make check`; docker adds Docker-outside-of-Docker for integration and compose smokes. See
[.devcontainer/README.md](.devcontainer/README.md).

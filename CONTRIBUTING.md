# Contributing

Development workflows for this repository. User-facing overview and usage examples live in
[README.md](README.md).

## Building blocks

Importable packages under `github.com/warrenguy/heron/...`.

| Package | Detail |
|---------|--------|
| `atomicbuffer` | `AtomicBuffer` over byte slices; bit and ASCII string helpers |
| `broadcast` | One-to-many broadcast buffer: fixed Agrona wire layout for driver/CnC |
| `idlestrategy` | Agent idle pacing (`IdleStrategy`, backoff, spin/yield/sleep) |
| `mmap` | Unix mmap for driver/CnC shared-memory files |
| `record` | Shared record header constants for ring and broadcast buffers |
| `ringbuffer` | `ManyToOneRingBuffer`: fixed Agrona wire layout for CnC/driver rings |
| `wire` | Wire primitives: `CheckLength`, fixed-width int64 and bool-byte put/get |
| `cluster/client` | `Config`, `AsyncConnect`, `AeronCluster`, egress handlers |
| `cluster/codecs` | SBE flyweights in `clusterwire/`; see **SBE codecs** below |
| `cluster/consensus` | `Role`, `ElectionRunner`, `AgentHandler`, cluster counter type IDs |
| `cluster/election` | SBE flyweights in `nodewire/`; see **SBE codecs** below |
| `cluster/recordinglog` | Recording log: `Open`, `Reload`, entry compare, recovery plan over `recording.log` |
| `archive`, `archive/codecs` | Client + codecs: SBE flyweights in `archivewire/`; `Config`, `AsyncConnect`, `Proxy`, `Client` - see [archive client](README.md#archive-client) |
| `aeron` | Transport client: `Connect`, `Client`, `Publication`, `Subscription`, `ExclusivePublication` |
| `aeron/{conductor,cnc,logbuffer}` | Transport internals: conductor, CnC, logbuffer |

Low-level layout packages (`atomicbuffer`, `broadcast`, `record`, `ringbuffer`, `wire`)
follow Agrona/C driver memory layouts. `ringbuffer` and `broadcast` are layout-coupled
ports, not generic queues; see package godoc for layout and concurrency constraints.

## Examples

Runnable examples live under `examples/`. Importable subpackages (e.g. `auctionwire`) are
included in `make check`; `package main` entrypoints use the `example` build tag. See
[`examples/README.md`](examples/README.md) for run commands.

## Pinned dependencies

SBE tool and Aeron C media driver are pinned in [`specs/versions.yaml`](specs/versions.yaml).

## Tests

```bash
make check        # lint, pins, SBE stale, unit+stress, contract-oracles (needs java + mvn)
make test-all     # check + rapid + integration + cluster-archive + compose smokes
make test-rapid                  # property tests (pgregory.net/rapid)
make test-election-sim           # election fault sim soak (CI; not part of make check)
make test-replication-sim        # replication commit-safety sim soak (CI; not part of make check)
make test-integration            # live C aeronmd (-tags=integration)
make contract-oracles            # all four contract suites below (needs mvn + java)
make test-java-interop-contract  # Go/Java smoke: streams, payloads, timeouts (no driver)
make test-cluster-sbe-contract   # Go/Java cluster SBE wire bytes vs catalog.json (no driver)
make test-cluster-java-contract  # Go/Java cluster harness constants (no driver)
make test-archive-java-contract  # Go/Java archive harness constants (no driver)
make test-java-interop           # Docker Compose smoke (bidirectional IPC pub/sub)
make test-cluster-java           # Docker Compose smoke (AsyncConnect + echo roundtrip)
make test-cluster-java-failover  # Docker Compose 3-node leader failover
make test-archive-java           # Docker Compose smoke (record, recording.log, PlanRecovery, replay)
make test-cluster-mixed-java-leader # Docker Compose mixed Go/Java (java-leader)
make test-cluster-mixed-go-leader   # Docker Compose mixed Go/Java (go-leader)
```

**Contract suites** (`make contract-oracles` and the four `*-contract` targets): run `mvn test`
(Java goldens) then matching Go oracle tests. No aeronmd or Docker.

| Target | Live smoke | What it checks (examples) |
|--------|------------|---------------------------|
| `test-java-interop-contract` | `test-java-interop` | streams 1100/1101; 32x32-byte `java-N` / `go-N` payloads |
| `test-cluster-java-contract` | `test-cluster-java` / `test-cluster-mixed-java-leader` | IPC channels; streams 101/102; ports 9000/9002; failover and mixed topology goldens |
| `test-archive-java-contract` | `test-archive-java` | record/replay streams 1200/1201; IPC channels; archive dir layout |
| `test-cluster-sbe-contract` | (none; wire oracle only) | cluster SBE bytes vs `catalog.json` |

**Compose smokes** (Docker Compose). Image tag: `java-interop-aeronmd`.

| Make target | Script | Media driver |
|-------------|--------|--------------|
| `test-java-interop` | `scripts/java-interop-smoke.sh` | Compose image (built by make target) |
| `test-cluster-java-failover` | `scripts/cluster-java-failover-smoke.sh` | Compose image (built by make target) |
| `test-archive-java` | `scripts/archive-java-smoke.sh` | Compose image (built by make target) |
| `test-cluster-mixed-java-leader` | `scripts/cluster-mixed-smoke.sh java-leader` | Compose image (built by make target) |
| `test-cluster-mixed-go-leader` | `scripts/cluster-mixed-smoke.sh go-leader` | Compose image (built by make target) |
| `test-cluster-java` | (none) | Embedded (no image step) |

Scripts do not build the image. Run `make aeronmd-compose-image` first, or any of the
Compose-image rows above.

**Integration:** `make test-integration` builds `bin/aeronmd` when needed; the harness sets
`AERON_DIR` per test.

**Build tags**

| Kind | Tags / location |
|------|-----------------|
| Unit | `bench`, `testhooks` |
| Property | `rapid` (`*_rapid_test.go`) |
| IDE / gopls (`go.buildTags`) | `testhooks rapid stress integration example java_interop cluster_sbe cluster_java archive_java cluster_mixed bench` |

```bash
make test                                             # unit only
go test -race -tags=bench,testhooks ./...             # ad-hoc unit
go test -race -tags=rapid,testhooks -run rapid ./...  # ad-hoc property
```

Prefer Makefile targets over bare `go test`.

## SBE codecs (cluster + archive + node-state)

Pinned Aeron ref: `aeron.ref` in [`specs/versions.yaml`](specs/versions.yaml).

| Area | Spec XML | Flyweights | Upstream (at pin) | Port notes |
|------|----------|------------|-------------------|------------|
| Cluster wire | `specs/cluster-codecs-wire.xml` | `cluster/codecs/clusterwire/` | `aeron-cluster/.../aeron-cluster-codecs.xml` | Copy verbatim |
| Archive control | `specs/archive-codecs-client.xml` | `archive/codecs/archivewire/` | `aeron-archive/.../aeron-archive-codecs.xml` | Subset: control request, response, descriptor, recording-signal, shared types |
| Node-state file | `specs/node-state-codecs.xml` | `cluster/election/nodewire/` | `aeron-cluster/.../aeron-cluster-node-state-codecs.xml` | Keep trimmed message set unless PR expands scope |

Upstream path prefixes under the matching Aeron module:

- Cluster / node-state: `src/main/resources/cluster/`
- Archive: `src/main/resources/archive/`

**Fixture regen**

| Target | What it refreshes | Needs |
|--------|-------------------|-------|
| `make cluster-sbe-catalog-regen` | `hex` from `fields` in `test/integration/cluster_sbe/clustersbe/fixtures/catalog.json` | (none) |
| `make node-state-fixtures-regen` | committed node-state oracle bytes | mvn + java |
| `make recordinglog-fixtures-regen` | committed recording-log oracle bytes | mvn + java |

Java golden fixture layout:
[`test/integration/golden_fixtures/README.md`](test/integration/golden_fixtures/README.md).

**Pin-bump workflow**

1. Bump `aeron.ref` in `specs/versions.yaml`.
2. Diff each upstream XML against the matching Spec XML row above; update per Port notes.
3. Generate flyweights:
   - `make sbe-generate`
   - `make sbe-generate-archive`
   - `make sbe-generate-nodestate`
4. Commit spec XML and flyweight diffs if any.
5. If port notes or subset changed, update the matching entries in
   [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
6. `make check` (needs java + mvn for SBE stale and contract-oracles) and `make test-rapid`.
7. `make test-cluster-sbe-contract` for cluster wire. Fix failures under:
   - `cluster/codecs/`
   - `test/integration/cluster_sbe/clustersbe/`
8. Node-state failures from step 6: fix under
   - `cluster/election/`
   - `cluster/election/nodewire/`
   Use `make check` / `make test-rapid` (not `test-cluster-sbe-contract`).

## Benchmarks

| Target | Tags | Packages |
|--------|------|----------|
| `make bench` (+ baseline, regression) | `bench` | `./test/bench/...` |
| `make bench-micro` | `testhooks` | `BENCH_MICRO_PKGS` in Makefile (layout primitives + `./aeron/...`) |

```bash
make bench             # contract suite (quick)
make bench-baseline    # .bench/baseline.txt (gitignored, per machine)
make bench-regression  # vs baseline via benchstat; exit 1 on alloc regression (CI)
make bench-micro       # in-package (informational)
```

Re-baseline after pulling `main` or when perf changes are intentional.

Optional env:

| Variable | Role | Defaults |
|----------|------|----------|
| `BENCH_COUNT` | bench iterations | Makefile: 10 |
| `BENCH_PAYLOAD_SIZES` | comma-separated payload filter | CI: `32,1400` |
| `BENCH_THRESHOLD` | alloc regression threshold | script: 10; CI (`bench.yml`): 30 |
| `SKIP_BENCH_REGRESSION` | skip alloc regression check | unset |

## Devcontainer

VS Code / GitHub Codespaces configs live under `.devcontainer/`. Default (check) covers
`make check`; docker adds Docker-outside-of-Docker for integration and compose smokes. See
[.devcontainer/README.md](.devcontainer/README.md).

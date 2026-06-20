# Heron

Pure-Go [Aeron](https://github.com/aeron-io/aeron) transport client and Aeron Cluster client
and server.

## Constraints

- **Pure Go** - no cgo; uses `unsafe` and atomics on hot paths.
- **GOARCH** - `amd64` and `arm64` only (little-endian).
- **Media driver** - IPC/UDP uses external Aeron media driver (C or Java).

## Media driver (`aeronmd`)

Build `aeronmd` (Aeron C media driver) via Docker:

```bash
make aeronmd  # requires Docker; output bin/aeronmd
```

Aeron release pinned in [`specs/versions.yaml`](specs/versions.yaml) (`aeron.ref`).

## Usage

### Transport client

Start media driver:

```bash
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
bin/aeronmd
```

Minimal IPC pub/sub (caller goroutine pumps `DoWork`):

```go
package main

import (
	"fmt"
	"log"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
)

func main() {
	const (
		streamID = 1001
		channel  = "aeron:ipc"
	)

	client, err := aeron.Connect(aeron.Context{AeronDir: "/tmp/aeron"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	pub, err := client.AddPublication(streamID, channel)
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	sub, err := client.AddSubscription(streamID, channel)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	for !pub.IsConnected() || sub.ImageCount() == 0 {
		client.DoWork()
	}

	payload := []byte("hello")
	pos := pub.Offer(payload)
	if pos < 0 {
		log.Fatalf("offer failed: %d", pos)
	}

	received := pollOne(sub, client)
	fmt.Println(string(received))
}

func pollOne(sub *aeron.Subscription, client *aeron.Client) []byte {
	var msg []byte
	handler := func(buf atomic.AtomicBuffer, off, n int, _ *aeron.Header) {
		msg = append(msg, buf.Bytes()[off:off+n]...)
	}
	for {
		if sub.Poll(handler, 1) > 0 {
			break
		}
		client.DoWork()
	}
	return msg
}
```

Notes:

- `AddPublication` / `AddSubscription` block until the driver responds; poll `IsConnected` /
  `ImageCount` after they return.
- One goroutine should call `DoWork` and `Subscription.Poll` for a given client.
  `Publication.Offer` may run from other goroutines.
- Close publications and subscriptions before `client.Close()`.

## Development

### Building blocks

| Area | Import path | Detail |
|------|-------------|--------|
| Agrona primitives | `github.com/warrenguy/heron/agrona/...` | Per-package roles in [`agrona/README.md`](agrona/README.md) |
| Cluster client | `github.com/warrenguy/heron/cluster/client` | `ClusterContext`, egress handlers, session API |
| Cluster codecs | `github.com/warrenguy/heron/cluster/codecs` | SBE wire flyweights; see **Cluster SBE codecs** below |
| Transport client | `github.com/warrenguy/heron/aeron` | `Connect`, `Client`, `Publication`, `Subscription` |
| Transport internals | `github.com/warrenguy/heron/aeron/{conductor,cnc,logbuffer}` | Driver control protocol, CnC metadata, term framing |

`ClusterContext.IdleStrategy` defaults via `.../cluster/client.NewDefaultIdleStrategy()`;
import `.../agrona/idlestrategy` for custom strategies.

### Pinned dependencies

SBE tool and Aeron C media driver are pinned in [`specs/versions.yaml`](specs/versions.yaml).

### Tests

```bash
make check                       # lint, pins, SBE/catalog stale (opt), unit+stress
make test-rapid                  # property tests (pgregory.net/rapid)
make test-integration            # live C aeronmd (-tags=integration)
make contract-oracles            # both contract suites below (needs mvn + java)
make test-java-interop-contract  # Go/Java smoke: streams, payloads, timeouts (no driver)
make test-cluster-sbe-contract   # Go/Java cluster SBE wire bytes vs catalog.json (no driver)
make test-java-interop           # Docker Compose smoke (bidirectional IPC pub/sub)
```

- Contract targets run `mvn test` (Java goldens) then matching Go oracle tests; no aeronmd or
  Docker.
- `test-java-interop-contract` checks shared constants for `make test-java-interop` (e.g. stream
  1100/1101, 32x32-byte `java-N` / `go-N` payloads) before the live smoke run.
- `make test-integration` builds `bin/aeronmd` when needed; harness sets `AERON_DIR` per test.
- Unit tests use `bench` and `testhooks` build tags.
- Property tests live in `*_rapid_test.go` behind the `rapid` tag.
- IDE/gopls: set `go.buildTags` to `testhooks rapid stress integration bench`.

```bash
make test                                             # unit only
go test -race -tags=bench,testhooks ./...             # ad-hoc unit
go test -race -tags=rapid,testhooks -run rapid ./...  # ad-hoc property
```

Prefer Makefile targets over bare `go test`.

### Cluster SBE codecs

Cluster codecs use in-tree SBE XML (`specs/cluster-codecs-client.xml`) and generated Go
flyweights (`cluster/codecs/generated/`). Fixture oracle rows live in
`test/integration/cluster_sbe/clustersbe/fixtures/catalog.json`; run
`make cluster-sbe-catalog-regen` to refresh committed `hex` from `fields`.

Pin-bump workflow:

1. Bump `aeron.ref` in [`specs/versions.yaml`](specs/versions.yaml).
2. Diff upstream `aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml` (client messages and shared types only).
3. Port into `specs/cluster-codecs-client.xml`.
4. `make sbe-generate`; commit flyweight diff if any.
5. `make test-cluster-sbe-contract`; fix failures under `cluster/codecs/` and `test/integration/cluster_sbe/clustersbe/`.

### Benchmarks

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

## License

MIT - see [LICENSE](LICENSE).

## Third-party notices

Cluster SBE wire XML and flyweights (see **Cluster SBE codecs** above) are ported from
[Aeron](https://github.com/aeron-io/aeron) cluster codecs
([Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)).

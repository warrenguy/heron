# Heron

Pure-Go [Aeron](https://github.com/aeron-io/aeron) transport client; Aeron Cluster support
planned.

## Constraints

- **Pure Go** - no cgo; uses `unsafe` and atomics on hot paths.
- **GOARCH** - `amd64` and `arm64` only (little-endian).
- **Media driver** - IPC/UDP uses external Aeron media driver (C or Java).

## Media driver (`aeronmd`)

Build `aeronmd` (Aeron C media driver) via Docker:

```bash
make aeronmd   # requires Docker; output bin/aeronmd (see specs/versions.yaml)
```

`make test-integration` builds `aeronmd` when needed and sets `AERON_DIR` per test; override with
`AERONMD_PATH` or an `aeronmd` on `PATH`.

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
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
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
	if pos := pub.Offer(payload); pos < 0 {
		log.Fatalf("offer failed: %d", pos)
	}

	received := pollOne(sub, client)
	fmt.Println(string(received))
}

func pollOne(sub *aeron.Subscription, client *aeron.Client) []byte {
	var msg []byte
	handler := func(buf aeronatomic.AtomicBuffer, off, n int, _ *aeron.Header) {
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

```bash
make check                        # lint, pins, codec checks, unit+stress (-race)
make test-rapid                   # property tests (pgregory.net/rapid)
make test-integration             # live C aeronmd (-tags=integration)
make test-java-interop-contract   # Java + Go oracle helpers (host JDK 26, Maven, mvn)
make test-java-interop            # optional Docker Compose smoke (bidirectional IPC pub/sub)
```

`make check` / `make lint` also require [ShellCheck](https://github.com/koalaman/shellcheck) on
`PATH` (e.g. `pacman -S shellcheck`, `apt install shellcheck`).

Unit tests use the `bench` and `testhooks` build tags (`bench` contract suite;
`testhooks` conductor cross-package test hooks).

Property tests live in `*_rapid_test.go` behind the `rapid` tag (`//go:build rapid`);
CI runs them via `make test-rapid` on amd64 after `make check`.

IDE/gopls: set `go.buildTags` to `testhooks rapid stress integration bench`.

```bash
make test                                              # unit only
go test -race -tags=bench,testhooks ./...              # ad-hoc unit
go test -race -tags=rapid,testhooks -run rapid ./...   # ad-hoc property
```

Prefer Makefile targets over bare `go test`.

### Cluster SBE codecs

Cluster codecs use trimmed SBE XML (`specs/cluster-codecs-client.xml`) and generated Go
flyweights (`aeron/sbe/generated/`). `go test` uses the committed artifacts only; Java is
not required for tests.

`make check` also runs `check-cluster-codecs-stale-optional` (regen trim, diff, restore;
needs `go` and `curl` on `PATH`) and `check-sbe-stale-optional` (regenerate flyweights,
`git diff aeron/sbe/generated/`; needs Java). Either optional step is skipped with a message
when its tools are missing.

After bumping `aeron.ref` in `specs/versions.yaml`, refresh both:

```bash
make cluster-codecs-regen   # fetch upstream XML @ pin, trim (Go+curl)
make sbe-generate           # emit flyweights (Java; sbe_tool pin in versions.yaml)
```

`go generate` in `aeron/sbe` and `make sbe-generate` both invoke `scripts/generate-sbe.sh`
and require Java. On amd64 CI, `make check` runs `check-sbe-stale-optional` when Java is on `PATH`.

`cluster-codecs-regen` caches upstream under `.cache/`. For a local checkout:
`./scripts/regen-cluster-codecs.sh /path/to/aeron-cluster-codecs.xml`.

## Benchmarks

| Target | Tags | Packages |
|--------|------|----------|
| `make bench` (+ baseline, regression) | `bench` | `./test/bench/...` |
| `make bench-micro` | `testhooks` | `./aeron/...` white-box |

```bash
make bench              # contract suite (quick)
make bench-baseline     # .bench/baseline.txt (gitignored, per machine)
make bench-regression   # vs baseline via benchstat; exit 1 on alloc regression (CI)
make bench-micro        # white-box in-package (informational)
```

Re-baseline after pulling `main` or when perf changes are intentional.

Optional env: `BENCH_COUNT` (Makefile default 10), `BENCH_PAYLOAD_SIZES` (comma-separated
filter; CI uses `32,1400`), `BENCH_THRESHOLD` (script default 10; CI sets 30 in
`bench.yml`), `SKIP_BENCH_REGRESSION`.

## License

MIT - see [LICENSE](LICENSE).

## Third-party notices

`specs/cluster-codecs-client.xml` and `aeron/sbe/generated/` are derived from
[Aeron](https://github.com/aeron-io/aeron) cluster codecs
([Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)).

# Heron

Pure-Go [Aeron](https://github.com/aeron-io/aeron) transport client and Aeron Cluster client and server (consensus module and clustered service container).

IPC/UDP via external media driver (integration tests use C `aeronmd`).

## Constraints

- **Pure Go** - no cgo; hot paths use `unsafe`, atomics, and `mmap` via `golang.org/x/sys/unix`.
- **GOARCH** - `amd64` and `arm64` only (little-endian).
- **Media driver** - IPC/UDP needs a separate media driver (C or Java), not vendored here; integration tests use pinned C `aeronmd`.

## Media driver (`aeronmd`)

```bash
make aeronmd   # requires Docker; output bin/aeronmd (see specs/versions.yaml)
```

`make test-integration` builds `aeronmd` when needed and sets `AERON_DIR` per test; override with `AERONMD_PATH` or an `aeronmd` on `PATH`.

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

- `AddPublication` / `AddSubscription` block until the driver responds; poll `IsConnected` / `ImageCount` after they return.
- One goroutine should call `DoWork` and `Subscription.Poll` for a given client. `Publication.Offer` may run from other goroutines.
- Close publications and subscriptions before `client.Close()`.

## Development

```bash
make check              # lint, unit tests (-race), stress
make test-rapid         # property tests (pgregory.net/rapid), separate from make test
make test-integration
```

Unit tests use the `testhooks` build tag (conductor cross-package test hooks).
Property tests live in `*_rapid_test.go` behind the `rapid` tag (`//go:build rapid`);
CI runs them via `make test-rapid` on amd64 after `make check`.

```bash
make test                                              # unit only
go test -race -tags=testhooks ./...                    # ad-hoc unit
go test -race -tags=rapid,testhooks -run rapid ./...   # ad-hoc property
```

Prefer Makefile targets over bare `go test`.

## Benchmarks

```bash
make bench
make bench-baseline     # .bench/baseline.txt (gitignored, per machine)
make bench-regression   # vs baseline via benchstat; exit 1 on regression
```

Ad-hoc benchmarks need the `testhooks` build tag when not using the Makefile
targets: `go test -tags=testhooks -run '^$' -bench=. -benchmem ./...`.

Re-baseline after pulling `main` or when perf changes are intentional. Optional env: `BENCH_COUNT`, `BENCH_THRESHOLD`, `SKIP_BENCH_REGRESSION` (defaults in Makefile / `scripts/bench-regression.sh`).

## License

MIT - see [LICENSE](LICENSE).

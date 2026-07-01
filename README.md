# Heron

Pure-Go [Aeron](https://github.com/aeron-io/aeron) transport and Aeron Cluster client.

## Constraints

- **Pure Go** - no cgo; uses `unsafe` and atomics on hot paths.
- **GOARCH** - `amd64` and `arm64` only (little-endian).
- **Media driver** - IPC/UDP uses external Aeron media driver (C or Java).

## Capabilities

Targets parity with the Java Aeron stack (transport, cluster, archive). Requires an external
Aeron media driver (C or Java). Invoker-only today: the application calls `DoWork` and
poll methods (no background conductor thread).

### Implemented

#### Transport (`aeron`)

IPC and UDP; shared and exclusive publications; subscriptions; `Offer`, `OfferWithOffset`,
`TryClaim`; fragmented publish and term rotation; publication limits and channel status;
blocking and async add (`Add*` / `OfferAdd*` + `PollAdd*`); registration lookup; counters
reader, CNC metadata, error log; driver liveness; close linger.

#### Cluster (`cluster/client`)

`AsyncConnect` (challenge, redirect, credentials); multi-member ingress; IPC, ipcx, and UDP
egress; exclusive ingress; `AeronCluster` offer, claim, keepalive, close; `PollEgress`,
egress handlers, fragment assembly; leader-change recovery; `SendSnapshotRequest`.

#### Archive (`archive/codecs`, `archive/client`)

SBE flyweights; `ArchiveContext`; `AsyncConnect` (challenge, credentials); `ArchiveProxy`
(connect, keepalive, close session); `ControlResponsePoller`; `ControlOfferUntilPositive`;
credentials and idle strategy; `MapControlResponse`; response channel resolve (IPC/UDP).
See [`archive/client/README.md`](archive/client/README.md).

### Planned

#### Transport

Counter registration; dynamic MDC destinations; controlled `Subscription.Poll`; public
subscription image lifecycle callbacks.

#### Cluster

`ControlledEgressHandler` dispatch; general admin request API (beyond snapshot); cluster
server / consensus service.

#### Archive

Recording start/stop, replay, and list APIs; `AeronArchive` session wrapper.

#### Runtime

Dedicated background conductor and cluster session threads (optional to invoker-only duty
cycle).

### Not planned

- Embedded media driver (use external C or Java driver)

## Media driver (`aeronmd`)

Build `aeronmd` (Aeron C media driver) via Docker:

```bash
make aeronmd  # requires Docker; output bin/aeronmd
```

Aeron release pinned in [`specs/versions.yaml`](specs/versions.yaml) (`aeron.ref`).

## Usage

One goroutine per `*aeron.Client` should call `DoWork` and poll methods; `Publication.Offer`
may run from other goroutines.

### Transport client

Start media driver:

```bash
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
bin/aeronmd
```

Minimal IPC pub/sub:

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

`AddPublication` / `AddSubscription` block until the driver responds; poll `IsConnected` /
`ImageCount` (pumping `DoWork`) before `Offer` / `Poll`. Close publications and subscriptions
before `client.Close()`.

### Cluster client

Requires a running Aeron Cluster plus `aeronmd`. Pass an existing transport `*aeron.Client` in
`ClusterContext.Aeron` so the duty cycle can call `DoWork` during `AsyncConnect.Poll`.

```go
package main

import (
	"log"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/cluster/client"
)

func main() {
	transport, err := aeron.Connect(aeron.Context{AeronDir: "/tmp/aeron"})
	if err != nil {
		log.Fatal(err)
	}
	defer transport.Close()

	handlers := &client.EgressHandlers{
		MessageFunc: func(
			_ int64,
			_ int64,
			buf atomic.AtomicBuffer,
			off, n int,
			_ *aeron.Header,
		) {
			log.Printf("egress: %s", buf.Bytes()[off:off+n])
		},
	}
	ctx := client.ClusterContext{
		Aeron:          transport,
		IngressChannel: "aeron:ipc",
		EgressChannel:  "aeron:ipc",
		EgressHandler:  handlers,
	}

	ac, err := client.NewAsyncConnect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	var result *client.ConnectResult
	for result == nil {
		transport.DoWork()
		result, err = ac.Poll()
		if err != nil {
			log.Fatal(err)
		}
	}

	session, err := client.NewAeronCluster(result)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	for {
		transport.DoWork()
		if session.PollEgress() > 0 {
			continue
		}
		pos := client.IngressOfferUntilPositive(
			transport,
			session,
			client.IngressOfferConfig{},
			func() int64 {
				return session.Offer([]byte("ping"))
			},
		)
		if pos < 0 {
			log.Printf("offer: %d", pos)
		}
	}
}
```

## Contributing

- Build, test, SBE codec, and benchmark workflows: [CONTRIBUTING.md](CONTRIBUTING.md)
- Package docs: [`agrona/README.md`](agrona/README.md), [`archive/client/README.md`](archive/client/README.md)

## License

MIT - see [LICENSE](LICENSE).

## Third-party notices

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
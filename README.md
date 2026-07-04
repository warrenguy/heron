# Heron

Pure-Go [Aeron](https://github.com/aeron-io/aeron) transport, cluster, and archive clients.

## Constraints

- **Pure Go** - no cgo; uses `unsafe` for zero-copy buffer access.
- **GOARCH** - `amd64` and `arm64` only (little-endian).
- **Media driver** - external only (C or Java); no embedded driver planned.

## Capabilities

Targets parity with the Java Aeron stack (transport, cluster, archive). Invoker-only today:
the application calls `DoWork` and poll methods. Background conductor and cluster session
threads are planned.

### Transport (`aeron`)

Transport client is substantially complete: IPC and UDP publications and subscriptions,
shared and exclusive publish paths, offer and claim, fragmented publish and term rotation,
publication limits and channel status, blocking and async resource add, registration
lookup, counters reader, CNC metadata, error log, driver liveness, and close linger.

#### Planned

Counter registration; dynamic MDC destinations; `Subscription.ControlledPoll`; public
subscription image lifecycle callbacks.

### Cluster (`cluster/client`)

Session client is implemented: `AsyncConnect` (challenge, redirect, credentials),
multi-member ingress, IPC, ipcx, and UDP egress, exclusive ingress, `AeronCluster`
offer, claim, keepalive, and close, egress handlers and fragment assembly, leader-change
recovery, and `SendSnapshotRequest`.

#### Planned

`ControlledEgressHandler` dispatch; general admin request API (beyond snapshot); cluster
server / consensus service.

### Archive (`archive/client`)

Control and replay client is implemented: SBE flyweights, `ArchiveContext`, connect,
`AeronArchive` session and recording lifecycle, `ArchiveProxy`, control and recording
descriptor pollers, recording signals, replay start and stop, replay image discovery,
and fragment poll.

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
	"os"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
)

func main() {
	const (
		streamID = 1001
		channel  = "aeron:ipc"
	)

	aeronDir := os.Getenv("AERON_DIR")
	if aeronDir == "" {
		aeronDir = "/tmp/aeron"
	}

	client, err := aeron.Connect(aeron.Context{AeronDir: aeronDir})
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

Pump `transport.DoWork()` during `AsyncConnect.Poll` and in the session loop. Close the
session before the transport client.

### Archive client

Requires Aeron Archive plus `aeronmd` (default local IPC control). Pass an existing
transport `*aeron.Client` in `ArchiveContext.Aeron` for a shared `DoWork` duty cycle.

```go
package main

import (
	"log"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/client"
	"github.com/warrenguy/heron/archive/codecs"
)

func main() {
	transport, err := aeron.Connect(aeron.Context{AeronDir: "/tmp/aeron"})
	if err != nil {
		log.Fatal(err)
	}
	defer transport.Close()

	archive, err := client.Connect(client.ArchiveContext{Aeron: transport})
	if err != nil {
		log.Fatal(err)
	}
	defer archive.Close()

	archive.SetRecordingSignalHandler(client.RecordingSignalHandlerFunc(
		func(event codecs.RecordingSignalEventFields) {
			log.Printf("recording signal: recording=%d signal=%v",
				event.RecordingID, event.Signal)
		},
	))

	log.Printf("archive session %d archive id %d",
		archive.ControlSessionID(), archive.ArchiveID())

	for {
		transport.DoWork()
		archive.Poll()
	}
}
```

Blocking `Connect` and recording APIs drive `DoWork` during awaits; otherwise pump
`transport.DoWork()` then `archive.Poll()` (`Poll` never calls `DoWork`). Configure a
recording signal handler or signals drop when control events are polled.

## Contributing

Build, test, SBE codec, and benchmark workflows: [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT - see [LICENSE](LICENSE).

## Third-party notices

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

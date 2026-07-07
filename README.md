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

Runnable IPC echo with public helpers (`PollUntilConnected`, `PollUntilImageCount`,
`OfferWithRetry`, `PollOneMessage`): [`examples/ipc-echo`](examples/ipc-echo/).

`AddPublication` / `AddSubscription` block until the driver responds. Close publications and
subscriptions before `client.Close()`.

### Cluster client

External cluster stack required; setup and duty-cycle patterns are in the examples below.

- [`examples/cluster-echo-client`](examples/cluster-echo-client/) - IPC echo against
  `docker/cluster_java`
- [`examples/cluster-auction-client`](examples/cluster-auction-client/) - UDP auction
  duty cycle against upstream Java cluster

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

## Examples

Runnable end-to-end demos live under [`examples/`](examples/). Each has its own
README with prerequisites and run steps.

## Contributing

Build, test, SBE codec, and benchmark workflows: [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT - see [LICENSE](LICENSE).

## Third-party notices

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

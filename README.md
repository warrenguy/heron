# Heron

Pure-Go [Aeron](https://github.com/aeron-io/aeron) transport, cluster, and
archive clients.

Heron implements Aeron's shared-memory and wire protocols directly against an
Aeron media driver (C or Java), targeting behavioural parity with the Java
client stack. It uses no cgo (`unsafe` only for zero-copy buffer access),
supports little-endian `amd64` and `arm64`, and embeds no media driver.

## Getting started

```bash
go get github.com/warrenguy/heron
```

A media driver must be running. Build the pinned C driver and start it:

```bash
make aeronmd                 # requires Docker; output bin/aeronmd
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
bin/aeronmd
```

The Aeron release is pinned in [`specs/versions.yaml`](specs/versions.yaml).

Publish and receive a message over IPC:

```go
package main

import (
	"context"
	"log"

	"github.com/warrenguy/heron/aeron"
)

func main() {
	client, err := aeron.Connect(aeron.Context{AeronDir: "/tmp/aeron"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	pub, err := client.AddPublication(1001, "aeron:ipc")
	if err != nil {
		log.Fatal(err)
	}
	sub, err := client.AddSubscription(1001, "aeron:ipc")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := client.PollUntilConnected(ctx, pub); err != nil {
		log.Fatal(err)
	}
	if err := client.PollUntilImageCount(ctx, sub, 1); err != nil {
		log.Fatal(err)
	}

	if _, err := client.OfferWithRetry(ctx, pub, []byte("hello")); err != nil {
		log.Fatal(err)
	}
	msg, err := client.PollOneMessage(ctx, sub)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("received %q", msg)
}
```

The `(*Client)` poll helpers above call `DoWork` internally. Runnable one-shot
sample: [`examples/ipc-echo`](examples/ipc-echo/).

A long-running application drives the client directly: one goroutine per
`*aeron.Client` calls `DoWork` and the poll methods, while `Publication.Offer`
may run from other goroutines.

```go
handler := aeron.AdaptFragmentHandlerBytes(func(payload []byte, _ *aeron.Header) {
	log.Printf("received %q", payload)
})

for {
	client.DoWork()
	sub.Poll(handler, 10)
}
```

Runnable long-running sample (paced offer, idle, signal stop):
[`examples/ipc-duty-cycle`](examples/ipc-duty-cycle/).

## Cluster client

[`cluster/client`](cluster/client/) connects to an Aeron Cluster:
`Connect`/`AsyncConnect` handle authentication challenges and member redirects;
sessions offer and claim ingress, poll egress with fragment assembly, and
recover across leader changes. An external cluster stack is required:

- [`examples/cluster-echo-client`](examples/cluster-echo-client/) - IPC echo
  against `docker/cluster_java`
- [`examples/cluster-auction-client`](examples/cluster-auction-client/) - UDP
  auction duty cycle against the upstream Java cluster

## Archive client

[`archive`](archive/) connects to Aeron Archive for recording control and
replay. Pass an existing transport client in `Context.Aeron` to share one duty
cycle:

```go
transport, err := aeron.Connect(aeron.Context{AeronDir: "/tmp/aeron"})
// ...
session, err := archive.Connect(context.Background(), archive.Context{Aeron: transport})
// ...
defer session.Close()

for {
	transport.DoWork()
	session.Poll()
}
```

Blocking `Connect` and recording APIs drive `DoWork` during awaits; otherwise pump
`transport.DoWork()` then `session.Poll()` (`Poll` never calls `DoWork`). Configure a
recording signal handler or signals drop when control events are polled.

## Status

The transport client and the cluster and archive session clients are
functional against the pinned Aeron release. All clients are invoker-driven
today; background conductor and cluster session threads are planned, as are
transport counter registration, dynamic MDC destinations,
`Subscription.ControlledPoll`, `ControlledEgressHandler` dispatch, cluster
admin requests beyond snapshot, and the cluster server (consensus service).

## Examples

Runnable end-to-end demos live under [`examples/`](examples/), each with its
own README covering prerequisites and run steps.

## Contributing

Build, test, SBE codec, and benchmark workflows: [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT - see [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

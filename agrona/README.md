# agrona

Shared Agrona primitives. Used by `aeron/` and `cluster/`; may be used directly
by module importers.

| Import path | Role |
|-------------|------|
| `github.com/warrenguy/heron/agrona/idlestrategy` | Agent idle pacing (`IdleStrategy`, backoff, spin/yield/sleep) |
| `github.com/warrenguy/heron/agrona/atomic` | `AtomicBuffer` over byte slices; bit and ASCII string helpers |
| `github.com/warrenguy/heron/agrona/mmap` | Unix mmap for driver/CnC shared-memory files |
| `github.com/warrenguy/heron/agrona/record` | Shared record header constants for ring and broadcast buffers |
| `github.com/warrenguy/heron/agrona/ringbuffer` | `ManyToOneRingBuffer`: fixed Agrona wire layout for CnC/driver rings |
| `github.com/warrenguy/heron/agrona/broadcast` | One-to-many broadcast buffer: fixed Agrona wire layout for driver/CnC |

`ringbuffer` and `broadcast` are layout-coupled ports, not generic queues. See
package godoc for layout and concurrency constraints.
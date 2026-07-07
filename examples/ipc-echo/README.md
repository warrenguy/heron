# ipc-echo

Minimal IPC pub/sub using public `aeron` poll/offer helpers (no local `pollOne`).

## Prerequisites

- Go toolchain matching `go.mod`
- Docker (for `make aeronmd`) or an existing `bin/aeronmd`

## Run

From the repo root:

```bash
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
make aeronmd
```

Skip `make aeronmd` if `bin/aeronmd` already exists from prerequisites.

```bash
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
bin/aeronmd
```

In another terminal, from the repo root:

```bash
export AERON_DIR=/tmp/aeron
go run -tags=example ./examples/ipc-echo/
```

Or compile and run:

```bash
export AERON_DIR=/tmp/aeron
make build-examples
bin/ipc-echo
```

## Expected output

Status (stderr; `log` adds a timestamp prefix):

```
2026/07/06 15:04:05 ipc-echo: offered at position <n>
2026/07/06 15:04:05 ipc-echo: echo ok ("hello")
```

Payload (stdout):

```
hello
```

## Env

| Var | Default | Notes |
|-----|---------|-------|
| `AERON_DIR` | `/tmp/aeron` | Must match running `aeronmd` |

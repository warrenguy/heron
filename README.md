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

## Development

```bash
make check
make test-integration
```

Ad-hoc unit tests need the `testhooks` build tag (conductor cross-package test
hooks): `go test -tags=testhooks ./...`. Prefer `make test` / `make check`.

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

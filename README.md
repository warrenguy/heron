# Heron

Pure-Go [Aeron](https://github.com/aeron-io/aeron) transport and cluster client.

## Constraints

- **Pure Go** - no cgo. Hot paths use `unsafe`, `sync/atomic`, and `golang.org/x/sys/unix` for mmap.
- **GOARCH** - `amd64` and `arm64` only (little-endian). `make` rejects other values and requires `GOARCH` to match the host unless `ALLOW_CROSS=1`. CI matrix arches are defined in Makefile `SUPPORTED_GOARCHES` and [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
- **External media driver** - IPC/UDP requires a separately built `aeronmd` binary (C driver), not vendored in this repo.

## Media driver (`aeronmd`)

**Build** (requires Docker):

```bash
make aeronmd
```

Output is `bin/aeronmd`, pinned via [`specs/versions.yaml`](specs/versions.yaml). `make clean` removes it.

**Integration tests** - `make test-integration` builds `bin/aeronmd` if needed (requires Docker), prepends `bin/` to `PATH`, and runs tests. Override with `AERONMD_PATH` or an `aeronmd` on `PATH`. The harness sets `AERON_DIR` per test.

**Manual runs** - set `AERON_DIR` to a writable directory (often `/dev/shm/aeron-<user>`) before starting `aeronmd`.

## Development

```bash
make check              # lint + unit tests (no aeronmd)
make test-integration   # builds bin/aeronmd if needed (Docker)
```

## Benchmarks

```bash
make bench              # run all benchmarks once
make bench-baseline     # save .bench/baseline.txt (per machine; gitignored)
make bench-regression   # compare to baseline via benchstat; exit 1 on regression
```

Refresh the baseline after pulling `main` or when perf changes are intentional (`make bench-baseline`). Optional env:

- `BENCH_COUNT` (default `10`) - samples per benchmark
- `BENCH_THRESHOLD` (default `10`) - fail if a benchmark is at least this many percent slower
- `SKIP_BENCH_REGRESSION=1` - skip the regression script

`benchstat` is a module tool (`go tool benchstat`); it is listed in `go.mod` and fetched automatically.

## License

MIT - see [LICENSE](LICENSE).
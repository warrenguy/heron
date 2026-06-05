#!/usr/bin/env bash
# Compare current benchmarks to a local baseline (make bench-baseline).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

BASELINE="${BENCH_BASELINE:-${ROOT}/.bench/baseline.txt}"
COUNT="${BENCH_COUNT:-10}"
THRESHOLD="${BENCH_THRESHOLD:-10}"
PACKAGES="${BENCH_PACKAGES:-./aeron/broadcast/ ./aeron/ringbuffer/}"
SKIP="${SKIP_BENCH_REGRESSION:-}"

if [[ "${SKIP}" == 1 ]]; then
	echo "bench-regression: skipped (SKIP_BENCH_REGRESSION=1)" >&2
	exit 0
fi

if [[ ! -f "${BASELINE}" ]]; then
	echo "bench-regression: missing ${BASELINE}" >&2
	echo "bench-regression: on main, run: make bench-baseline" >&2
	exit 1
fi

if ! command -v go >/dev/null 2>&1; then
	echo "bench-regression: go not in PATH" >&2
	exit 1
fi

NEW="$(mktemp)"
trap 'rm -f "${NEW}"' EXIT

echo "bench-regression: running benchmarks (count=${COUNT})..." >&2
# shellcheck disable=SC2086
go test -bench=. -benchmem -count="${COUNT}" ${PACKAGES} >"${NEW}"

echo "bench-regression: vs ${BASELINE}" >&2
REPORT="$(mktemp)"
trap 'rm -f "${NEW}" "${REPORT}"' EXIT
go tool benchstat "${BASELINE}" "${NEW}" | tee "${REPORT}"

REGRESSIONS="$(
	awk -v threshold="${THRESHOLD}" '
		/sec\/op|ns\/op/ { in_time = 1; in_alloc = 0; next }
		/B\/op/ { in_time = 0; in_alloc = 0; next }
		/allocs\/op/ { in_time = 0; in_alloc = 1; next }
		/^geomean/ { in_time = 0; in_alloc = 0; next }
		/^$/ { in_time = 0; in_alloc = 0; next }
		/Benchmark/ && /vs base/ {
			if ($0 ~ /~ \(/) {
				next
			}
			if (in_alloc && match($0, /\+Inf%/)) {
				print $0
				next
			}
			if (match($0, /\+([0-9]+(\.[0-9]+)?)%/, m)) {
				if (m[1] + 0 >= threshold) {
					print $0
				}
			}
		}
	' "${REPORT}"
)"

if [[ -n "${REGRESSIONS}" ]]; then
	echo "bench-regression: failed (>= ${THRESHOLD}% slower or new allocs)" >&2
	echo "${REGRESSIONS}" >&2
	echo "bench-regression: if intentional, run: make bench-baseline" >&2
	exit 1
fi

echo "bench-regression: ok" >&2
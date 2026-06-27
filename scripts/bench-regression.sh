#!/usr/bin/env bash
# Compare contract benchmarks to a local baseline (make bench-baseline).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

BASELINE="${BENCH_BASELINE:-${ROOT}/.bench/baseline.txt}"
COUNT="${BENCH_COUNT:-10}"
THRESHOLD="${BENCH_THRESHOLD:-10}"
# Contract regression only: ./test/bench/... with bench build tag.
PACKAGES="${BENCH_PKGS:-./test/bench/...}"
has_test_bench=0
has_non_bench=0
for pkg in ${PACKAGES}; do
	case "${pkg}" in
	*test/bench*)
		has_test_bench=1
		;;
	*)
		has_non_bench=1
		;;
	esac
done
if [[ "${has_test_bench}" -eq 1 && "${has_non_bench}" -eq 1 ]]; then
	echo "bench-regression: cannot mix test/bench with other packages (conflicting build tags)" >&2
	exit 1
fi
UNIT_TEST_TAGS="${UNIT_TEST_TAGS:-bench}"
[[ "${UNIT_TEST_TAGS}" == testhooks ]] && UNIT_TEST_TAGS=bench
if [[ "${has_test_bench}" -eq 1 && "${UNIT_TEST_TAGS}" != *bench* ]]; then
	echo "bench-regression: test/bench packages require bench build tag (got: ${UNIT_TEST_TAGS})" >&2
	exit 1
fi
SKIP="${SKIP_BENCH_REGRESSION:-}"

workflow_notice() {
	local level="$1"
	local message="$2"
	[[ "${GITHUB_ACTIONS:-}" == true ]] || return 0
	message="${message//'%'/%25}"
	message="${message//$'\n'/%0A}"
	message="${message//$'\r'/%0D}"
	printf '%s\n' "::${level} title=bench-regression::${message}"
}

workflow_error() {
	workflow_notice error "$1"
}

workflow_warn() {
	workflow_notice warning "$1"
}

benchstat_fail() {
	fail 'benchstat failed' "" "benchstat failed (GOARCH=${GOARCH:-unknown})" \
		'benchstat failed'
}

fail() {
	echo "bench-regression: $1" >&2
	[[ -n "${2:-}" ]] && echo "bench-regression: $2" >&2
	workflow_error "${3:-$1}"
	[[ -n "${4:-}" ]] && write_step_summary "${4}"
	exit 1
}

bench_cpus() {
	[[ -f "$1" ]] || {
		echo unknown
		return
	}
	grep -m1 '^cpu:' "$1" | sed 's/^cpu:[[:space:]]*//' || echo unknown
}

count_nonempty_lines() {
	printf '%s\n' "$1" | grep -c . || true
}

summary_text_section() {
	local title="$1"
	local body="$2"
	[[ -n "${body}" ]] || return 0
	echo ""
	echo "### ${title}"
	echo ""
	echo '```text'
	printf '%s\n' "${body}"
	echo '```'
}

write_step_summary() {
	local status="$1"
	[[ -n "${GITHUB_STEP_SUMMARY:-}" ]] || return 0
	{
		echo "## Benchmark regression (${GOARCH:-unknown}): ${status}"
		echo ""
		echo "| Field | Value |"
		echo "|-------|-------|"
		echo "| Baseline CPU | ${BASELINE_CPU:-unknown} |"
		echo "| Current CPU | ${NEW_CPU:-unknown} |"
		summary_text_section 'non-zero allocs/op (contract gate)' \
			"${BENCH_ALLOC_VIOLATIONS:-}"
		summary_text_section 'allocation regressions' "${ALLOC_REGRESSIONS:-}"
		summary_text_section "time warnings (>= ${THRESHOLD}% slower, not gated)" \
			"${TIME_WARNINGS:-}"
		if [[ -f "${BENCHSTAT_TEXT:-}" && -s "${BENCHSTAT_TEXT}" ]]; then
			echo ""
			echo "### benchstat"
			echo ""
			echo '```text'
			cat "${BENCHSTAT_TEXT}"
			echo '```'
		fi
		if [[ -n "${GO_TEST_LOG:-}" && -s "${GO_TEST_LOG}" ]]; then
			echo ""
			echo "### go test stderr"
			echo ""
			echo '```text'
			tail -n 80 "${GO_TEST_LOG}"
			echo '```'
		fi
	} >>"${GITHUB_STEP_SUMMARY}"
}

if [[ "${SKIP}" == 1 ]]; then
	echo "bench-regression: skipped (SKIP_BENCH_REGRESSION=1)" >&2
	exit 0
fi

if [[ ! -f "${BASELINE}" ]]; then
	fail "missing ${BASELINE}" "on main, run: make bench-baseline" \
		"missing baseline ${BASELINE}; run make bench-baseline on main first"
fi

BASELINE_CPU="$(bench_cpus "${BASELINE}")"

if ! command -v go >/dev/null 2>&1; then
	fail 'go not in PATH' "" 'go not in PATH'
fi

NEW="$(mktemp)"
REPORT="$(mktemp)"
BENCHSTAT_TEXT="$(mktemp)"
GO_TEST_LOG="$(mktemp)"
trap 'rm -f "${NEW}" "${REPORT}" "${BENCHSTAT_TEXT}" "${GO_TEST_LOG}"' EXIT

echo "bench-regression: running benchmarks (count=${COUNT})..." >&2
# shellcheck disable=SC2086
if ! go test -run '^$' -tags="${UNIT_TEST_TAGS}" \
	-bench=. -benchmem -count="${COUNT}" ${PACKAGES} \
	>"${NEW}" 2>"${GO_TEST_LOG}"
then
	[[ -s "${GO_TEST_LOG}" ]] && cat "${GO_TEST_LOG}" >&2
	NEW_CPU="$(bench_cpus "${NEW}")"
	fail 'go test failed' "" \
		"go test failed (GOARCH=${GOARCH:-unknown}); see step summary" \
		'go test failed'
fi

if [[ "${has_test_bench}" -eq 1 ]]; then
	BENCH_ALLOC_VIOLATIONS="$(
		awk '
		/^Benchmark[^ ]/ && $NF == "allocs/op" && $(NF - 1) + 0 > 0 {
			print $1 "," $(NF - 1)
		}
		' "${NEW}"
	)"
	if [[ -n "${BENCH_ALLOC_VIOLATIONS}" ]]; then
		echo "${BENCH_ALLOC_VIOLATIONS}" >&2
		violation_count="$(count_nonempty_lines "${BENCH_ALLOC_VIOLATIONS}")"
		fail 'contract benchmark allocated' \
			'test/bench requires 0 allocs/op in the timed loop' \
			"failed: ${violation_count} contract benchmark(s) with allocs/op > 0" \
			failed
	fi
fi

NEW_CPU="$(bench_cpus "${NEW}")"
echo "bench-regression: baseline cpu: ${BASELINE_CPU}" >&2
echo "bench-regression: current cpu: ${NEW_CPU}" >&2
echo "bench-regression: vs ${BASELINE}" >&2
if ! go tool benchstat -format csv -ignore=cpu \
	"${BASELINE}" "${NEW}" >"${REPORT}"
then
	benchstat_fail
fi
if ! go tool benchstat -ignore=cpu "${BASELINE}" "${NEW}" >"${BENCHSTAT_TEXT}"
then
	benchstat_fail
fi
[[ "${GITHUB_ACTIONS:-}" != true ]] && cat "${BENCHSTAT_TEXT}"

while IFS= read -r line; do
	case "${line}" in
	time:*)
		line="${line#time:}"
		TIME_WARNINGS="${TIME_WARNINGS:+${TIME_WARNINGS}$'\n'}${line}"
		;;
	alloc:*)
		line="${line#alloc:}"
		ALLOC_REGRESSIONS="${ALLOC_REGRESSIONS:+${ALLOC_REGRESSIONS}$'\n'}${line}"
		;;
	esac
done < <(
	awk -F, -v threshold="${THRESHOLD}" '
		function pct_delta(vs,    m) {
			return match(vs, /^\+([0-9.]+)%/, m) ? m[1] + 0 : -1
		}
		function time_regress(vs) {
			return pct_delta(vs) >= threshold
		}
		function alloc_regress(vs) {
			if (vs == "?" || vs ~ /^\+Inf%/) {
				return 1
			}
			return pct_delta(vs) > 0
		}
		/^,sec\/op,/ { unit="time"; next }
		/^,allocs\/op,/ { unit="alloc"; next }
		/^,[^,]+,CI,/ { unit=""; next }
		unit == "" || $1 == "" || NF < 6 { next }
		$6 == "~" || $6 == "" { next }
		unit == "time" && time_regress($6) {
			print "time:" $1 "," $6
			next
		}
		unit == "alloc" && alloc_regress($6) {
			print "alloc:" $1 "," $6
		}
	' "${REPORT}"
)

if [[ -n "${TIME_WARNINGS:-}" ]]; then
	echo "bench-regression: warning: time >= ${THRESHOLD}% slower (not gated):" >&2
	echo "${TIME_WARNINGS}" >&2
	time_count="$(count_nonempty_lines "${TIME_WARNINGS}")"
	time_note="${time_count} time warning(s) >= ${THRESHOLD}% slower"
	time_note="${time_note} (see step summary)"
	workflow_warn "${time_note}"
fi

if [[ -n "${ALLOC_REGRESSIONS:-}" ]]; then
	echo "${ALLOC_REGRESSIONS}" >&2
	alloc_count="$(count_nonempty_lines "${ALLOC_REGRESSIONS}")"
	fail 'new or increased allocations' \
		'if intentional, run: make bench-baseline' \
		"failed: ${alloc_count} allocation regression(s) (see step summary)" \
		failed
fi

status=ok
[[ -n "${TIME_WARNINGS:-}" ]] && status='ok (time warnings)'
echo "bench-regression: ${status}" >&2
write_step_summary "${status}"

#!/usr/bin/env bash
# Selftest for check-catalog-stale trap/restore and failure paths.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${CLUSTER_SBE_CATALOG_JSON:=test/integration/cluster_sbe/clustersbe/fixtures/catalog.json}"
CATALOG_JSON="${ROOT}/${CLUSTER_SBE_CATALOG_JSON}"

sha256_file() {
	sha256sum "$1" | awk '{print $1}'
}

with_file_backup() {
	local file="$1"
	shift
	(
		set -euo pipefail
		local backup
		backup="$(mktemp)"
		cp "${file}" "${backup}"
		trap 'cp "${backup}" "${file}"; rm -f "${backup}"' EXIT
		"$@"
	)
}

# Mirror Makefile trap recipe in isolation (temp catalog + controllable regen step).
run_stale_recipe() {
	local catalog="$1" regen_mode="$2"
	local hash_before hash_after status

	hash_before="$(sha256_file "${catalog}")"
	set +e
	(
		set -euo pipefail
		local tmp
		tmp="$(mktemp)"
		cp "${catalog}" "${tmp}"
		trap 'cp "${tmp}" "${catalog}"; rm -f "${tmp}"' EXIT
		case "${regen_mode}" in
		regen-fail) exit 1 ;;
		diff-fail) printf 'regenerated-content\n' >"${catalog}" ;;
		*) echo "heron: unknown regen mode ${regen_mode}" >&2; exit 2 ;;
		esac
		diff -u "${tmp}" "${catalog}" >/dev/null
	)
	status=$?
	set -e

	hash_after="$(sha256_file "${catalog}")"
	[[ "${status}" -ne 0 && "${hash_before}" == "${hash_after}" ]]
}

test_trap_restores() {
	local mode="$1" label="$2" content dir catalog

	case "${mode}" in
	regen-fail) content=stale-check-selftest-regen-failure ;;
	diff-fail) content=stale-check-selftest-diff-failure ;;
	*) echo "heron: unknown trap test mode ${mode}" >&2; return 1 ;;
	esac

	dir="$(mktemp -d)"
	catalog="${dir}/catalog.json"
	printf '%s\n' "${content}" >"${catalog}"
	run_stale_recipe "${catalog}" "${mode}" || {
		rm -rf "${dir}"
		echo "heron: ${label} failed" >&2
		return 1
	}
	rm -rf "${dir}"
}

test_drift_detected() {
	local file="$1" sed_expr="$2" label="$3"
	if ! command -v go >/dev/null 2>&1; then
		echo "heron: skipping ${label} (go not installed)"
		return 0
	fi
	set +e
	with_file_backup "${file}" bash -ec "
		sed -i '${sed_expr}' '${file}'
		make -C '${ROOT}' --no-print-directory check-catalog-stale
	"
	local status=$?
	set -e
	[[ "${status}" -ne 0 ]] || {
		echo "heron: ${label}: expected check-catalog-stale failure" >&2
		return 1
	}
}

test_optional_skip_message() {
	local out

	if command -v go >/dev/null 2>&1; then
		echo "heron: skipping optional skip test (go installed)"
		return 0
	fi

	out="$(cd "${ROOT}" && make --no-print-directory check-catalog-stale-optional 2>&1)"
	[[ "${out}" == *"skipping check-catalog-stale (go not installed)"* ]] || {
		echo "heron: optional skip test: unexpected output: ${out}" >&2
		return 1
	}
}

main() {
	test_trap_restores regen-fail regen-failure-restores
	test_trap_restores diff-fail diff-failure-restores
	test_drift_detected "${CATALOG_JSON}" '0,/"byte": 51/s/"byte": 51/"byte": 99/' "blob drift test"
	test_drift_detected "${CATALOG_JSON}" \
		'0,/"clusterSessionId": 0/s/"clusterSessionId": 0/"clusterSessionId": 99/' \
		"fixture field drift test"
	test_optional_skip_message
	echo "heron: check-catalog-stale selftest passed"
}

main "$@"

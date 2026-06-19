#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=SCRIPTDIR/lib/versions.sh
source "${ROOT}/scripts/lib/versions.sh"
VERSIONS="${ROOT}/specs/versions.yaml"
INTEGRATION_DIR="${ROOT}/test/integration"

require_aeron_versions "${VERSIONS}" || exit 1

check_contract_pom() {
	local pom="$1" version
	version="$(sed -n 's/.*<aeron.version>\([^<]*\)<\/aeron.version>.*/\1/p' "${pom}")"
	[[ -n "${version}" ]] || {
		echo "heron: <aeron.version> not found in ${pom}" >&2
		return 1
	}
	[[ "${version}" == "${AERON_REF}" ]] || {
		echo "heron: pom aeron.version ${version} != specs aeron.ref ${AERON_REF} (${pom})" >&2
		return 1
	}
}

found=0
for pom in "${INTEGRATION_DIR}"/*/pom.xml; do
	[[ -f "${pom}" ]] || continue
	found=1
	check_contract_pom "${pom}"
done

(( found )) || {
	echo "heron: no integration pom.xml under test/integration/" >&2
	exit 1
}

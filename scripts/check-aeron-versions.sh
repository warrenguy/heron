#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=SCRIPTDIR/lib/versions.sh
source "${ROOT}/scripts/lib/versions.sh"
VERSIONS="${ROOT}/specs/versions.yaml"
POM="${ROOT}/test/integration/java_interop/pom.xml"

require_aeron_versions "${VERSIONS}" || exit 1

pom_aeron_version="$(
	sed -n 's/.*<aeron.version>\([^<]*\)<\/aeron.version>.*/\1/p' "${POM}"
)"

if [[ -z "${pom_aeron_version}" ]]; then
	echo "heron: <aeron.version> not found in ${POM}" >&2
	exit 1
fi

if [[ "${pom_aeron_version}" != "${AERON_REF}" ]]; then
	echo "heron: pom aeron.version ${pom_aeron_version} != specs aeron.ref ${AERON_REF}" >&2
	exit 1
fi

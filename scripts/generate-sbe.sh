#!/usr/bin/env bash
# Fetch pinned sbe-tool JAR and generate Go flyweights from committed wire XML.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=SCRIPTDIR/lib/versions.sh
source "${ROOT}/scripts/lib/versions.sh"
# shellcheck source=SCRIPTDIR/lib/cache-fetch.sh
source "${ROOT}/scripts/lib/cache-fetch.sh"

TARGET="${1:-cluster}"
case "${TARGET}" in
cluster)
	SCHEMA="${ROOT}/specs/cluster-codecs-wire.xml"
	OUT_DIR="${ROOT}/cluster/codecs"
	NAMESPACE="clusterwire"
	;;
archive)
	SCHEMA="${ROOT}/specs/archive-codecs-client.xml"
	OUT_DIR="${ROOT}/archive/codecs"
	NAMESPACE="archivewire"
	;;
nodestate)
	SCHEMA="${ROOT}/specs/node-state-codecs.xml"
	OUT_DIR="${ROOT}/cluster/election"
	NAMESPACE="nodewire"
	;;
*)
	echo "heron: unknown sbe target ${TARGET} (want cluster, archive, or nodestate)" >&2
	exit 1
	;;
esac

VERSIONS="${ROOT}/specs/versions.yaml"
CACHE_DIR="${ROOT}/.cache/sbe-tool"

require_sbe_versions "${VERSIONS}" || exit 1

[[ -f "${SCHEMA}" ]] || {
	echo "heron: ${SCHEMA} not found" >&2
	exit 1
}

command -v java >/dev/null || {
	echo "heron: java required for sbe-generate (not needed for go test)" >&2
	exit 1
}

IFS=':' read -r group artifact version <<<"${SBE_MAVEN_COORDINATES}"
jar="${artifact}-${version}.jar"
SBE_TOOL_JAR="${CACHE_DIR}/${jar}"

mkdir -p "${CACHE_DIR}"
group_path="${group//./\/}"
url="https://repo1.maven.org/maven2/${group_path}/${artifact}/${version}/${jar}"
if [[ ! -f "${SBE_TOOL_JAR}" ]]; then
	echo "heron: fetching ${url}"
	command -v curl >/dev/null || {
		echo "heron: curl required to fetch sbe-tool JAR" >&2
		exit 1
	}
fi
cache_fetch_if_missing "${SBE_TOOL_JAR}" "${url}"

mkdir -p "${OUT_DIR}"

echo "heron: generating SBE flyweights into ${OUT_DIR} (namespace ${NAMESPACE})"
java \
	-Dsbe.target.language=GOLANG \
	-Dsbe.go.generate.generate.flyweights=true \
	-Dsbe.output.dir="${OUT_DIR}" \
	-Dsbe.target.namespace="${NAMESPACE}" \
	-jar "${SBE_TOOL_JAR}" \
	"${SCHEMA}"

echo "heron: SBE generation complete"

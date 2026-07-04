#!/usr/bin/env bash
# Build or start the upstream Aeron basic-auction-cluster (not Heron aeronmd or Go client).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HERON_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck disable=SC1091
source "${HERON_ROOT}/scripts/lib/versions.sh"

AERON_SRC="${AERON_SRC:-${HERON_ROOT}/../aeron}"
VERSIONS="${HERON_ROOT}/specs/versions.yaml"

usage() {
	cat <<EOF
Usage: $(basename "$0") <check|build|start> [args...]

  check   Verify JAVA_HOME, Aeron checkout, ref pin, and cluster JARs
  build   Run gradlew :aeron-all:assemble :aeron-agent:shadowJar
  start   Run upstream basic-auction-cluster (pass 0|1|2 for a single node)

Environment:
  AERON_SRC   Aeron repository root (default: ${HERON_ROOT}/../aeron)
  JAVA_HOME   Required for upstream cluster scripts
EOF
}

require_aeron_prereqs() {
	if [[ -z "${JAVA_HOME:-}" ]]; then
		echo "cluster-auction-client: JAVA_HOME is not set" >&2
		return 1
	fi
	if [[ ! -d "${AERON_SRC}" ]]; then
		echo "cluster-auction-client: AERON_SRC not found: ${AERON_SRC}" >&2
		return 1
	fi
	require_aeron_versions "${VERSIONS}" || return 1

	AERON_CHECKOUT_REF="$(<"${AERON_SRC}/version.txt")"
	if [[ "${AERON_CHECKOUT_REF}" != "${AERON_REF}" ]]; then
		echo "cluster-auction-client: Aeron version.txt ${AERON_CHECKOUT_REF} !=" \
			"heron pin ${AERON_REF}" >&2
		return 1
	fi
}

require_aeron_jars() {
	local module jar missing=0
	for module in aeron-all aeron-agent; do
		jar="${AERON_SRC}/${module}/build/libs/${module}-${AERON_CHECKOUT_REF}.jar"
		if [[ ! -f "${jar}" ]]; then
			echo "cluster-auction-client: missing ${jar}" >&2
			missing=1
		fi
	done
	if (( missing )); then
		echo "cluster-auction-client: run AERON_SRC=${AERON_SRC} ./$(basename "$0") build" >&2
		return 1
	fi
}

cmd_check() {
	require_aeron_prereqs || return 1
	require_aeron_jars || return 1
	echo "cluster-auction-client: check ok (Aeron ${AERON_CHECKOUT_REF})"
}

cmd_build() {
	require_aeron_prereqs || return 1
	echo "cluster-auction-client: building Aeron ${AERON_CHECKOUT_REF} in ${AERON_SRC}"
	(
		cd "${AERON_SRC}"
		./gradlew --no-daemon :aeron-all:assemble :aeron-agent:shadowJar
	)
	require_aeron_jars || return 1
	echo "cluster-auction-client: build ok (Aeron ${AERON_CHECKOUT_REF})"
}

cluster_node_pattern='io.aeron.samples.cluster.tutorial.BasicAuctionClusteredServiceNode'

stop_cluster_nodes() {
	pkill -f "${cluster_node_pattern}" 2>/dev/null || true
}

cmd_start() {
	cmd_check
	cd "${AERON_SRC}/aeron-samples/scripts/cluster"

	trap 'stop_cluster_nodes; exit 130' INT
	trap 'stop_cluster_nodes; exit 143' TERM

	if [[ $# -eq 1 && $1 =~ ^[012]$ ]]; then
		./basic-auction-cluster "$1"
		tail -n 100 -F "logs/cluster-${1}.log"
	else
		./basic-auction-cluster "$@"
	fi
}

main() {
	if [[ $# -lt 1 ]]; then
		usage >&2
		exit 1
	fi

	case "$1" in
	check) shift; cmd_check "$@" ;;
	build) shift; cmd_build "$@" ;;
	start) shift; cmd_start "$@" ;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage >&2
		exit 1
		;;
	esac
}

main "$@"

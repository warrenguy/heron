#!/usr/bin/env bash
# Mixed Go/Java cluster compose smoke. Exactly one overlay per run.
# Requires the java-interop-aeronmd image. make test-cluster-mixed-java-leader
# (or test-cluster-mixed-go-leader) runs aeronmd-compose-image first.
#
# Isolation per run:
# - Compose project HERON_COMPOSE_PROJECT=heron-cluster-mixed-$$ with -p
# - Per-run /24 under 172.30.0.0/16 (third octet from shell PID and attempt)
#
# Each overlay uses a disjoint third-octet range. Parallel mixed smokes under
# test-docker-smokes therefore start on different /24 pools.
#
# The PID walks the range. Docker IPAM is daemon-global, so another process or
# a foreign 172.30.x.0/24 may still hold the pick. compose up then fails on pool
# overlap. run_smoke retries the next octet in range.
#
# force-recreate plus EXIT/INT/TERM traps clean this project only.
#
# Usage: cluster-mixed-smoke.sh <overlay>
#   overlay: java-leader | go-leader
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${ROOT}/docker/cluster_mixed"
COMPOSE_BASE="${COMPOSE_DIR}/compose.yaml"
GO_MOD="${ROOT}/go.mod"

usage() {
	echo "usage: $0 java-leader|go-leader" >&2
	exit 1
}

# Outer wall-clock cap for the compose wait. Same value for both overlays.
# 130s: 120s Go -test.timeout plus 10s compose teardown margin.
OUTER_TIMEOUT_S=130

# Max subnet picks before compose up gives up. Each attempt walks one octet.
SUBNET_ATTEMPTS=5

[[ "$#" -eq 1 ]] || usage

# Third-octet range per overlay. Ranges are disjoint and skip octet 0.
# A smoke must not land on the other overlay range or on 172.30.0.0/24 manual topology.
OVERLAY_ARG="$1"
case "${OVERLAY_ARG}" in
java-leader)
	OVERLAY_FILE="${COMPOSE_DIR}/compose.java-leader.yaml"
	GO_SERVICE="go-java-leader"
	OCTET_BASE=1
	OCTET_SPAN=125
	;;
go-leader)
	OVERLAY_FILE="${COMPOSE_DIR}/compose.go-leader.yaml"
	GO_SERVICE="go-go-leader"
	OCTET_BASE=126
	OCTET_SPAN=125
	;;
*)
	usage
	;;
esac

export HERON_COMPOSE_PROJECT="heron-cluster-mixed-$$"

# Export one /24 for this attempt. Hosts stay .10/.11/.12 inside that subnet.
# Multicast log interface=172.30.0.0/16 covers every third octet.
pick_subnet() {
	local attempt="$1"
	local octet=$((($$ + attempt) % OCTET_SPAN + OCTET_BASE))

	export HERON_CLUSTER_MIXED_SUBNET="172.30.${octet}.0/24"
	export HERON_CLUSTER_MIXED_IP0="172.30.${octet}.10"
	export HERON_CLUSTER_MIXED_IP1="172.30.${octet}.11"
	export HERON_CLUSTER_MIXED_IP2="172.30.${octet}.12"
	export HERON_CLUSTER_HOSTNAMES="${HERON_CLUSTER_MIXED_IP0},\
${HERON_CLUSTER_MIXED_IP1},${HERON_CLUSTER_MIXED_IP2}"
}

COMPOSE_CMD=(
	docker compose
	-p "${HERON_COMPOSE_PROJECT}"
	-f "${COMPOSE_BASE}"
	-f "${OVERLAY_FILE}"
)

compose() {
	"${COMPOSE_CMD[@]}" "$@"
}

compose_cleanup() {
	compose down -v >/dev/null 2>&1 || true
	[[ -n "${UP_LOG:-}" ]] && rm -f "${UP_LOG}"
}

if ! command -v docker >/dev/null 2>&1; then
	echo "heron: docker required" >&2
	exit 1
fi

if [[ ! -f "${GO_MOD}" ]]; then
	echo "heron: ${GO_MOD} not found" >&2
	exit 1
fi

GO_VERSION="$(awk '/^go / {print $2; exit}' "${GO_MOD}")"
if [[ -z "${GO_VERSION}" ]]; then
	echo "heron: go version required in ${GO_MOD}" >&2
	exit 1
fi

export DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 GO_VERSION
HERON_COMPOSE_UID="$(id -u)"
export HERON_COMPOSE_UID
HERON_COMPOSE_GID="$(id -g)"
export HERON_COMPOSE_GID

if ! docker image inspect java-interop-aeronmd >/dev/null 2>&1; then
	echo "heron: missing image java-interop-aeronmd; run: make aeronmd-compose-image" >&2
	exit 1
fi

UP_LOG="$(mktemp)"

trap compose_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Build the Java image via cluster-0, the Go member Archive, and the Go service.
# cluster-1 reuses the cluster-0 image. aeronmd uses the pre-tagged image.
#
# Build stays outside the outer timeout. Image build wall clock varies.
compose build cluster-0 archive-go "${GO_SERVICE}"

smoke_logs() {
	compose logs --no-color aeronmd archive-go cluster-0 cluster-1 "${GO_SERVICE}"
}

# True when Docker IPAM refused the subnet because another network holds it.
# Only that failure triggers a retry. Every other compose up failure returns.
is_pool_conflict() {
	grep -qE 'Pool overlaps|predefined address pools' "${UP_LOG}"
}

# compose up waits for depends_on healthchecks. Go readiness file is the check.
#
# Outer timeout bounds that compose wait. Go binary uses its own -test.timeout.
run_smoke() {
	local attempt status=1

	for ((attempt = 0; attempt < SUBNET_ATTEMPTS; attempt++)); do
		pick_subnet "${attempt}"

		# tee prints compose up output. UP_LOG stores the same stream for is_pool_conflict.
		if compose up -d --force-recreate --no-build 2>&1 | tee "${UP_LOG}"; then
			status=0
			break
		fi
		status="${PIPESTATUS[0]}"
		is_pool_conflict || return "${status}"
		echo "heron: subnet ${HERON_CLUSTER_MIXED_SUBNET} taken; retrying" >&2
		compose down -v >/dev/null 2>&1 || true
	done

	if [[ "${status}" -ne 0 ]]; then
		echo "heron: no free /24 after ${SUBNET_ATTEMPTS} picks" >&2
		return "${status}"
	fi

	if command -v timeout >/dev/null 2>&1; then
		timeout "${OUTER_TIMEOUT_S}" "${COMPOSE_CMD[@]}" wait "${GO_SERVICE}"
	else
		compose wait "${GO_SERVICE}"
	fi
}

run_smoke || {
	status=$?
	echo "heron: cluster mixed smoke failed (overlay=${OVERLAY_ARG})" >&2
	smoke_logs >&2 || true
	exit "${status}"
}

echo "heron: cluster mixed smoke passed (overlay=${OVERLAY_ARG})"
smoke_logs || echo "heron: warning: could not fetch logs (smoke passed)" >&2

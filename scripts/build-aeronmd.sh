#!/usr/bin/env bash
# Build pinned aeronmd in Docker and extract to bin/aeronmd.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=SCRIPTDIR/lib/versions.sh
source "${ROOT}/scripts/lib/versions.sh"
VERSIONS="${ROOT}/specs/versions.yaml"
DOCKERFILE="${ROOT}/docker/aeronmd/Dockerfile"
OUT="${ROOT}/bin/aeronmd"
IMAGE="heron-aeronmd:build"

command -v docker >/dev/null || {
	echo "heron: docker required to build aeronmd" >&2
	exit 1
}

require_aeron_versions "${VERSIONS}" || exit 1

# Must match docker/aeronmd/Dockerfile builder stage output path.
CONTAINER_BIN="/src/aeron/cppbuild/Release/binaries/${AERON_DRIVER_ARTIFACT}"

echo "heron: building aeronmd (ref ${AERON_REF})"
docker_build_args=(
	-f "${DOCKERFILE}"
	--target builder
	--build-arg "AERON_REF=${AERON_REF}"
	--build-arg "AERON_REPOSITORY=${AERON_REPOSITORY}"
	--build-arg "AERON_DRIVER_ARTIFACT=${AERON_DRIVER_ARTIFACT}"
	-t "${IMAGE}"
	"${ROOT}"
)
if docker buildx version >/dev/null 2>&1; then
	if ! docker buildx build --load "${docker_build_args[@]}"; then
		echo "heron: buildx build failed; falling back to docker build" >&2
		docker build "${docker_build_args[@]}"
	fi
else
	docker build "${docker_build_args[@]}"
fi

cid="$(docker create "${IMAGE}")"
trap 'docker rm -f "${cid}" >/dev/null 2>&1 || true' EXIT

mkdir -p "$(dirname "${OUT}")"
docker cp "${cid}:${CONTAINER_BIN}" "${OUT}"
chmod +x "${OUT}"
touch "${OUT}"

echo "heron: extracted ${OUT}"

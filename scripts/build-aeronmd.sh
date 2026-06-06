#!/usr/bin/env bash
# Build pinned aeronmd in Docker and extract to bin/aeronmd.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSIONS="${ROOT}/specs/versions.yaml"
DOCKERFILE="${ROOT}/docker/aeronmd/Dockerfile"
OUT="${ROOT}/bin/aeronmd"
IMAGE="heron-aeronmd:build"

yaml_field() {
	local key="$1"
	awk -v "key=${key}" '
		$0 ~ "^[[:space:]]+" key ":" {
			gsub(/^[[:space:]]+[^:]+:[[:space:]]*"?|"?[[:space:]]*$/, "")
			print
			exit
		}
	' "${VERSIONS}"
}

if [[ ! -f "${VERSIONS}" ]]; then
	echo "heron: ${VERSIONS} not found" >&2
	exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
	echo "heron: docker required to build aeronmd" >&2
	exit 1
fi

repository="$(yaml_field repository)"
ref="$(yaml_field ref)"
artifact="$(yaml_field driver_artifact)"

if [[ -z "${repository}" || -z "${ref}" || -z "${artifact}" ]]; then
	echo "heron: aeron.repository, aeron.ref, and aeron.driver_artifact" \
		"required in ${VERSIONS}" >&2
	exit 1
fi

echo "heron: building aeronmd (ref ${ref})"
docker_build_args=(
	-f "${DOCKERFILE}"
	--build-arg "AERON_REF=${ref}"
	--build-arg "AERON_REPOSITORY=${repository}"
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

mkdir -p "${ROOT}/bin"
docker cp "${cid}:/src/aeron/cppbuild/Release/binaries/${artifact}" "${OUT}"
chmod +x "${OUT}"

echo "heron: extracted ${OUT}"

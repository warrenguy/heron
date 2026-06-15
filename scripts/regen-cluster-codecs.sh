#!/usr/bin/env bash
# Fetch Aeron cluster codecs XML at aeron.ref and trim to cluster-codecs-client.xml.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=SCRIPTDIR/lib/versions.sh
source "${ROOT}/scripts/lib/versions.sh"
# shellcheck source=SCRIPTDIR/lib/cache-fetch.sh
source "${ROOT}/scripts/lib/cache-fetch.sh"
VERSIONS="${ROOT}/specs/versions.yaml"
UPSTREAM_REL="aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml"
CACHE_DIR="${ROOT}/.cache/cluster-codecs"
TRIM_PKG="./tools/cluster-codecs-trim"

usage() {
	echo "usage: $0 [upstream-aeron-cluster-codecs.xml]" >&2
	echo "  no arg: fetch at specs/versions.yaml aeron.ref, trim, write specs output" >&2
	echo "  path:   trim local upstream XML (offline or custom checkout)" >&2
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

if [[ $# -gt 1 ]]; then
	usage
	exit 2
fi

require_aeron_versions "${VERSIONS}" || exit 1

command -v go >/dev/null || {
	echo "heron: go required to run cluster-codecs-trim" >&2
	exit 1
}

upstream_src="${1:-}"

if [[ -n "${upstream_src}" ]]; then
	if [[ ! -f "${upstream_src}" ]]; then
		echo "heron: upstream XML not found: ${upstream_src}" >&2
		exit 1
	fi
else
	command -v curl >/dev/null || {
		echo "heron: curl required to fetch upstream XML (or pass a local path)" >&2
		exit 1
	}

	repo_path="${AERON_REPOSITORY%.git}"
	repo_path="${repo_path#https://github.com/}"
	repo_path="${repo_path#http://github.com/}"
	repo_path="${repo_path#git@github.com:}"
	if [[ -z "${repo_path}" || "${repo_path}" == "${AERON_REPOSITORY}" ]]; then
		echo "heron: unsupported aeron.repository for raw fetch: ${AERON_REPOSITORY}" >&2
		echo "heron: pass a local upstream XML path" >&2
		exit 1
	fi

	mkdir -p "${CACHE_DIR}"
	upstream_src="${CACHE_DIR}/aeron-cluster-codecs-${AERON_REF}.xml"
	url="https://raw.githubusercontent.com/${repo_path}/${AERON_REF}/${UPSTREAM_REL}"
	if [[ ! -f "${upstream_src}" ]]; then
		echo "heron: fetching ${url}"
	fi
	cache_fetch_if_missing "${upstream_src}" "${url}"
fi

echo "heron: trimming ${upstream_src}"
go -C "${ROOT}" run "${TRIM_PKG}" "${upstream_src}"

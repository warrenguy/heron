#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

tier="${HERON_TIER:-check}"

GO_VERSION="$(awk '/^go / {print $2; exit}' go.mod)"
GO_MINOR="${GO_VERSION%.*}"
go version | grep -qE "go${GO_MINOR//./\\.}\\." || {
	echo "heron: Go version mismatch (want go${GO_MINOR}.x, go.mod ${GO_VERSION})" >&2
	exit 1
}

go mod download &
download_pid=$!

go tool -n golangci-lint >/dev/null &
lint_pid=$!
go tool -n goimports >/dev/null &
imports_pid=$!
go tool -n gofumpt >/dev/null &
fumpt_pid=$!

command -v shellcheck >/dev/null || {
	echo "heron: shellcheck required" >&2
	exit 1
}
command -v java >/dev/null || {
	echo "heron: java required" >&2
	exit 1
}
command -v mvn >/dev/null || {
	echo "heron: mvn required" >&2
	exit 1
}

for pid in "${download_pid}" "${lint_pid}" "${imports_pid}" "${fumpt_pid}"; do
	wait "${pid}"
done

if command -v docker >/dev/null; then
	if ! docker compose version >/dev/null 2>&1; then
		if [[ "${tier}" == "docker" ]]; then
			echo "heron: WARN docker compose plugin unavailable (required for integration targets)" >&2
		else
			echo "heron: WARN docker compose plugin unavailable" >&2
		fi
	fi
	if [[ -S /var/run/docker.sock ]]; then
		if docker ps >/dev/null 2>&1; then
			export HERON_DOCKER_PS_OK=1
		else
			echo "heron: WARN docker socket present but not accessible" >&2
		fi
	elif [[ "${tier}" == "docker" ]]; then
		echo "heron: WARN docker config but no /var/run/docker.sock - use root devcontainer (check)" >&2
	fi
elif [[ "${tier}" == "docker" ]]; then
	echo "heron: WARN docker config but docker CLI missing" >&2
fi

bash .devcontainer/scripts/print-tier.sh

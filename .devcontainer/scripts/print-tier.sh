#!/usr/bin/env bash
# Informational tier summary; always exits 0.
set -euo pipefail

tier="${HERON_TIER:-check}"

echo "devcontainer tier: ${tier}"
echo "  details: .devcontainer/README.md"

if command -v docker >/dev/null; then
	if [[ -S /var/run/docker.sock ]]; then
		docker_ok="${HERON_DOCKER_PS_OK:-}"
		if [[ -z "${docker_ok}" ]] && docker ps >/dev/null 2>&1; then
			docker_ok=1
		fi
		if [[ "${docker_ok}" == "1" ]]; then
			echo "  docker: socket present, docker ps ok"
		else
			echo "  docker: socket present, docker ps unavailable"
		fi
	else
		echo "  docker: CLI installed, no socket"
	fi
else
	echo "  docker: not installed"
fi

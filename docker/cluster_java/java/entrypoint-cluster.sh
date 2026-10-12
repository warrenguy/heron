#!/usr/bin/env bash
set -euo pipefail

: "${AERON_DIR:?AERON_DIR required}"

# HERON_CLUSTER_* env vars are optional; ClusterNode reads them from container env.

JAVA_OPTS=(
	--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
	-Daeron.dir="${AERON_DIR}"
	-Daeron.term.buffer.length="${AERON_TERM_BUFFER_LENGTH:-65536}"
	-Daeron.ipc.term.buffer.length="${AERON_IPC_TERM_BUFFER_LENGTH:-65536}"
	-Dheron.debug=true
	# Aeron's 10s defaults are too tight for CI.
	-Daeron.driver.timeout="${AERON_DRIVER_TIMEOUT:-30s}"
	-Daeron.client.liveness.timeout="${AERON_CLIENT_LIVENESS_TIMEOUT:-30s}"
)

# Optional: raise the Java startup canvass window (Agrona getDurationInNanos units).
# Unset leaves Aeron's default (60 s). go-leader sets 120s; java-leader leaves unset.
if [[ -n "${AERON_CLUSTER_STARTUP_CANVASS_TIMEOUT:-}" ]]; then
	JAVA_OPTS+=(
		-Daeron.cluster.startup.canvass.timeout="${AERON_CLUSTER_STARTUP_CANVASS_TIMEOUT}"
	)
fi

exec java \
	"${JAVA_OPTS[@]}" \
	-cp /app/heron-cluster-java.jar \
	heron.clusterjava.ClusterNode

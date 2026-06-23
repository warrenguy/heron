#!/usr/bin/env bash
set -euo pipefail

: "${AERON_DIR:?AERON_DIR required}"

exec java \
	--add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
	-Daeron.dir="${AERON_DIR}" \
	-Daeron.term.buffer.length="${AERON_TERM_BUFFER_LENGTH:-65536}" \
	-Daeron.ipc.term.buffer.length="${AERON_IPC_TERM_BUFFER_LENGTH:-65536}" \
	-Dheron.debug=true \
	-cp /app/heron-cluster-java.jar \
	heron.clusterjava.ClusterNode

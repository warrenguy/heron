#!/usr/bin/env bash
set -euo pipefail

: "${AERON_DIR:?AERON_DIR required}"

exec java \
	--add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
	-Daeron.dir="${AERON_DIR}" \
	-cp /app/heron-java-interop.jar \
	heron.interop.JavaDuplex

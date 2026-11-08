#!/usr/bin/env bash
set -euo pipefail

: "${AERON_DIR:?AERON_DIR required}"

exec java \
	--add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
	-Daeron.dir="${AERON_DIR}" \
	-cp /app/luas-java-interop.jar \
	luas.interop.JavaDuplex

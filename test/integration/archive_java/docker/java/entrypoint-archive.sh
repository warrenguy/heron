#!/usr/bin/env bash
set -euo pipefail

: "${AERON_DIR:?AERON_DIR required}"

exec java \
	--add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
	-cp /app/luas-archive-java.jar \
	luas.archivejava.ArchiveNode

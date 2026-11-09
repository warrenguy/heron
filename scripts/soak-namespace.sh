#!/usr/bin/env bash
# Runs a test binary in private namespaces.
set -euo pipefail
exec unshare --user --map-root-user --mount --propagation private --net \
	--pid --fork --mount-proc --kill-child=SIGKILL -- \
	bash -c 'mount -t tmpfs tmpfs /run && ip link set lo up && exec "$@"' _ "$@"

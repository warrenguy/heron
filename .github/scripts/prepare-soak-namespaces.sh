#!/usr/bin/env bash
# Prepares a hosted runner for the soak's private namespaces.
set -euo pipefail

missing=()
command -v nft >/dev/null || missing+=(nftables)
command -v ip >/dev/null || missing+=(iproute2)

if ((${#missing[@]} > 0)); then
	sudo apt-get update -qq
	sudo apt-get install -y -qq "${missing[@]}"
fi

sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0

modules=(veth bridge nf_tables nft_numgen)

if ! sudo modprobe -a "${modules[@]}"; then
	sudo apt-get update -qq
	sudo apt-get install -y -qq "linux-modules-extra-$(uname -r)"
	sudo modprobe -a "${modules[@]}"
fi

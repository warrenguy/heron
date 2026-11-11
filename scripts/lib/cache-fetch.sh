#!/usr/bin/env bash
# Fetch URL to dest atomically via a same-directory temp file.
# Each fetched or cached file must match its pinned sha256.

cache_verify_sha256() {
	local file="$1"
	local label="$2"
	local want="$3"
	local got

	got="$(sha256sum "${file}")"
	got="${got%% *}"
	if [[ "${got}" == "${want}" ]]; then
		return 0
	fi
	echo "luas: sha256 mismatch for ${label}" >&2
	echo "luas: got ${got}, want ${want}" >&2
	return 1
}

cache_fetch() {
	local dest="$1"
	local url="$2"
	local want="$3"
	local dir tmp

	dir="$(dirname "${dest}")"
	mkdir -p "${dir}"
	tmp="$(mktemp "${dir}/.cache-fetch.XXXXXX")"
	if ! curl -fsSL --retry 5 --retry-all-errors --connect-timeout 15 -o "${tmp}" "${url}" ||
		! cache_verify_sha256 "${tmp}" "${url}" "${want}"; then
		rm -f "${tmp}"
		return 1
	fi
	mv "${tmp}" "${dest}"
}

cache_fetch_if_missing() {
	local dest="$1"
	local url="$2"
	local want="$3"

	if [[ -f "${dest}" ]]; then
		if cache_verify_sha256 "${dest}" "${url}" "${want}"; then
			return 0
		fi
		echo "luas: delete ${dest} to fetch it again" >&2
		return 1
	fi
	cache_fetch "${dest}" "${url}" "${want}"
}

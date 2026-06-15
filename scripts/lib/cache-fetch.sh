#!/usr/bin/env bash
# Fetch URL to dest atomically via a same-directory temp file.
cache_fetch() {
	local dest="$1"
	local url="$2"
	local dir tmp

	dir="$(dirname "${dest}")"
	mkdir -p "${dir}"
	tmp="$(mktemp "${dir}/.cache-fetch.XXXXXX")"
	if ! curl -fsSL -o "${tmp}" "${url}"; then
		rm -f "${tmp}"
		return 1
	fi
	mv "${tmp}" "${dest}"
}

cache_fetch_if_missing() {
	local dest="$1"
	local url="$2"

	if [[ -f "${dest}" ]]; then
		return 0
	fi
	cache_fetch "${dest}" "${url}"
}

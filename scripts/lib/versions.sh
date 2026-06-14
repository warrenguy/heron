#!/usr/bin/env bash
# Parse aeron pins from specs/versions.yaml.
load_aeron_versions() {
	local versions="$1"

	if [[ ! -f "${versions}" ]]; then
		echo "heron: ${versions} not found" >&2
		return 1
	fi

	local _pins
	mapfile -t _pins < <(
		awk '
			$0 ~ /^[[:space:]]+repository:/ {
				gsub(/^[[:space:]]+[^:]+:[[:space:]]*"?|"?[[:space:]]*$/, "")
				repository = $0
			}
			$0 ~ /^[[:space:]]+ref:/ {
				gsub(/^[[:space:]]+[^:]+:[[:space:]]*"?|"?[[:space:]]*$/, "")
				ref = $0
			}
			$0 ~ /^[[:space:]]+driver_artifact:/ {
				gsub(/^[[:space:]]+[^:]+:[[:space:]]*"?|"?[[:space:]]*$/, "")
				driver_artifact = $0
			}
			END {
				print repository
				print ref
				print driver_artifact
			}
		' "${versions}"
	)

	AERON_REPOSITORY="${_pins[0]:-}"
	AERON_REF="${_pins[1]:-}"
	AERON_DRIVER_ARTIFACT="${_pins[2]:-}"
}

require_aeron_versions() {
	local versions="$1"

	load_aeron_versions "${versions}" || return 1

	if [[ -z "${AERON_REPOSITORY}" || -z "${AERON_REF}" || -z "${AERON_DRIVER_ARTIFACT}" ]]; then
		echo "heron: aeron.repository, aeron.ref, and aeron.driver_artifact" \
			"required in ${versions}" >&2
		return 1
	fi
}

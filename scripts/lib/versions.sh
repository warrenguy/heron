#!/usr/bin/env bash
# Parse pins from specs/versions.yaml.

_versions_awk_preamble='
/^aeron:/ { in_aeron = 1; in_sbe = 0; next }
/^sbe_tool:/ { in_sbe = 1; in_aeron = 0; next }
/^[^[:space:]]/ && !/^aeron:/ && !/^sbe_tool:/ {
	in_aeron = 0
	in_sbe = 0
}
function strip_field() {
	gsub(/^[[:space:]]+[^:]+:[[:space:]]*"?|"?[[:space:]]*$/, "")
	gsub(/#.*$/, "")
	gsub(/[[:space:]]+$/, "")
}
'

load_aeron_versions() {
	local versions="$1"

	if [[ ! -f "${versions}" ]]; then
		echo "heron: ${versions} not found" >&2
		return 1
	fi

	local _pins
	mapfile -t _pins < <(
		awk "${_versions_awk_preamble}"'
			in_aeron && $0 ~ /^[[:space:]]+repository:/ {
				strip_field()
				repository = $0
			}
			in_aeron && $0 ~ /^[[:space:]]+ref:/ {
				strip_field()
				ref = $0
			}
			in_aeron && $0 ~ /^[[:space:]]+driver_artifact:/ {
				strip_field()
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

	if [[ ! "${AERON_REF}" =~ ^[A-Za-z0-9._-]+$ ]]; then
		echo "heron: aeron.ref must match ^[A-Za-z0-9._-]+$ (got ${AERON_REF})" >&2
		return 1
	fi
}

load_sbe_versions() {
	local versions="$1"

	if [[ ! -f "${versions}" ]]; then
		echo "heron: ${versions} not found" >&2
		return 1
	fi

	local _pins
	mapfile -t _pins < <(
		awk "${_versions_awk_preamble}"'
			in_sbe && $0 ~ /^[[:space:]]+maven_coordinates:/ {
				strip_field()
				maven_coordinates = $0
			}
			END {
				print maven_coordinates
			}
		' "${versions}"
	)

	export SBE_MAVEN_COORDINATES="${_pins[0]:-}"
}

require_sbe_versions() {
	local versions="$1"

	load_sbe_versions "${versions}" || return 1

	if [[ -z "${SBE_MAVEN_COORDINATES}" ]]; then
		echo "heron: sbe_tool.maven_coordinates required in ${versions}" >&2
		return 1
	fi

	if [[ ! "${SBE_MAVEN_COORDINATES}" =~ ^[^:]+:sbe-all:[^:]+$ ]]; then
		echo "heron: sbe_tool.maven_coordinates must match *:sbe-all:*" \
			"(got ${SBE_MAVEN_COORDINATES})" >&2
		return 1
	fi
}

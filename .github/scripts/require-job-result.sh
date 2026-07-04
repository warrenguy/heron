#!/usr/bin/env bash
# Sourced by ci-ok and bench-ok to fail closed when required jobs did not succeed.
set -euo pipefail

REQUIRE_JOB_FAILED=0

require_job() {
	local name="$1"
	local result="$2"
	local allow_skip="${3:-false}"
	local expect="success"

	if [ "$result" = success ]; then
		return 0
	fi
	if [ "$allow_skip" = true ] && [ "$result" = skipped ]; then
		return 0
	fi
	if [ "$allow_skip" = true ]; then
		expect="success or skipped"
	fi
	echo "required job ${name}: ${result} (expected ${expect})"
	REQUIRE_JOB_FAILED=1
}

require_jobs_exit() {
	local message="$1"
	if [ "${REQUIRE_JOB_FAILED}" -ne 0 ]; then
		exit 1
	fi
	echo "${message}"
}

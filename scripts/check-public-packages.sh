#!/usr/bin/env bash
# Compares the packages a default-tag build lists outside internal directories
# with scripts/public-packages.txt. Rejects a listed package that imports a test library.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# -tags= overrides tags from GOFLAGS, including a value set with go env -w.
module="$(go list -m)"
listing="$(go list -tags= -f '{{.ImportPath}}{{range .Imports}} {{.}}{{end}}' ./...)"
public="$(awk -v module="${module}/" '$1 !~ /(^|\/)internal(\/|$)/ {
	sub(module, "", $1); print
}' <<<"${listing}")"

status=0
if ! diff -u scripts/public-packages.txt <(awk '{print $1}' <<<"${public}" | LC_ALL=C sort); then
	echo "luas: public packages differ from scripts/public-packages.txt" >&2
	echo "luas: put a test helper under internal/ or behind a build tag" >&2
	echo "luas: add a new public package to the list" >&2
	status=1
fi

test_imports="$(awk '{
	for (i = 2; i <= NF; i++) {
		if ($i == "testing" || $i ~ /^(github.com\/stretchr\/testify|pgregory.net\/rapid)/) {
			print $1 " imports " $i
		}
	}
}' <<<"${public}")"
if [[ -n "${test_imports}" ]]; then
	echo "luas: public package imports a test library:" >&2
	echo "${test_imports}" >&2
	status=1
fi
exit "${status}"

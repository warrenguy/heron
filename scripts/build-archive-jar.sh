#!/usr/bin/env bash
# Build the archive Java shade jar into bin/heron-archive-java.jar.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POM="${ROOT}/test/integration/archive_java/pom.xml"
TARGET_DIR="${ROOT}/test/integration/archive_java/target"
OUT="${ROOT}/bin/heron-archive-java.jar"

command -v mvn >/dev/null || {
	echo "heron: mvn required to build archive jar" >&2
	exit 1
}
command -v java >/dev/null || {
	echo "heron: java required to build archive jar" >&2
	exit 1
}

echo "heron: packaging archive java jar"
mvn -q -f "${POM}" -DskipTests package

shopt -s nullglob
jars=("${TARGET_DIR}"/heron-archive-java-*.jar)
shopt -u nullglob
if [[ ${#jars[@]} -eq 0 ]]; then
	echo "heron: no heron-archive-java-*.jar under ${TARGET_DIR}" >&2
	exit 1
fi
if [[ ${#jars[@]} -gt 1 ]]; then
	echo "heron: expected one heron-archive-java-*.jar, found ${#jars[@]}" >&2
	exit 1
fi

mkdir -p "$(dirname "${OUT}")"
cp "${jars[0]}" "${OUT}"
echo "heron: wrote ${OUT}"

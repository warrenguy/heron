#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
	echo "heron: usage: build-java-jar.sh <module> <artifact>" >&2
	exit 1
fi

MODULE="$1"
ARTIFACT="$2"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POM="${ROOT}/test/integration/${MODULE}/pom.xml"
TARGET_DIR="${ROOT}/test/integration/${MODULE}/target"
OUT="${ROOT}/bin/${ARTIFACT}.jar"

command -v mvn >/dev/null || {
	echo "heron: mvn required to build java jar" >&2
	exit 1
}
command -v java >/dev/null || {
	echo "heron: java required to build java jar" >&2
	exit 1
}

echo "heron: packaging ${MODULE} java jar"
mvn -q -f "${POM}" -Dmaven.test.skip=true package

shopt -s nullglob
jars=("${TARGET_DIR}/${ARTIFACT}-"*.jar)
shopt -u nullglob
if [[ ${#jars[@]} -eq 0 ]]; then
	echo "heron: no ${ARTIFACT}-*.jar under ${TARGET_DIR}" >&2
	exit 1
fi
if [[ ${#jars[@]} -gt 1 ]]; then
	echo "heron: expected one ${ARTIFACT}-*.jar, found ${#jars[@]}" >&2
	exit 1
fi

mkdir -p "$(dirname "${OUT}")"
cp "${jars[0]}" "${OUT}"
echo "heron: wrote ${OUT}"

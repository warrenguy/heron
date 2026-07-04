# syntax=docker/dockerfile:1
FROM eclipse-temurin:26-jdk AS builder

RUN apt-get update \
	&& apt-get install -y --no-install-recommends maven \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY test/integration/archive_java/pom.xml test/integration/archive_java/pom.xml
COPY test/integration/archive_java/src test/integration/archive_java/src

# -DskipTests: image build only; ArchiveConstantsTest runs in CI via mvn test.
RUN --mount=type=cache,target=/root/.m2/repository \
	mvn -q -f test/integration/archive_java/pom.xml -DskipTests package

FROM eclipse-temurin:26-jre AS runtime

WORKDIR /app
COPY --from=builder \
	/src/test/integration/archive_java/target/heron-archive-java-*.jar \
	./heron-archive-java.jar
COPY --chmod=+x docker/archive_java/java/entrypoint-archive.sh ./entrypoint-archive.sh

ENTRYPOINT ["./entrypoint-archive.sh"]

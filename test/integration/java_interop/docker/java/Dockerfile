# syntax=docker/dockerfile:1
# Builder image ships Maven and JDK 26. Matches pom maven.compiler.release.
ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-26
FROM ${MAVEN_IMAGE} AS builder

WORKDIR /src
COPY test/integration/java_interop/pom.xml test/integration/java_interop/pom.xml
COPY test/integration/java_interop/src test/integration/java_interop/src

# -DskipTests: image build only; JavaPublisherTest runs in CI via mvn test.
RUN --mount=type=cache,target=/root/.m2/repository \
	mvn -q -f test/integration/java_interop/pom.xml -DskipTests package

FROM eclipse-temurin:26-jre AS runtime

WORKDIR /app
COPY --from=builder \
	/src/test/integration/java_interop/target/heron-java-interop-*.jar \
	./heron-java-interop.jar
COPY --chmod=+x test/integration/java_interop/docker/java/entrypoint-java.sh ./entrypoint-java.sh

ENTRYPOINT ["./entrypoint-java.sh"]

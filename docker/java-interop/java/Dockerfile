# syntax=docker/dockerfile:1
FROM eclipse-temurin:26-jdk AS builder

RUN apt-get update \
	&& apt-get install -y --no-install-recommends maven \
	&& rm -rf /var/lib/apt/lists/*

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
COPY --chmod=+x docker/java-interop/java/entrypoint-java.sh ./entrypoint-java.sh

ENTRYPOINT ["./entrypoint-java.sh"]

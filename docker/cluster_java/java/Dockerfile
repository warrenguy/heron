# syntax=docker/dockerfile:1
FROM eclipse-temurin:26-jdk AS builder

RUN apt-get update \
	&& apt-get install -y --no-install-recommends maven \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY test/integration/cluster_java/pom.xml test/integration/cluster_java/pom.xml
COPY test/integration/cluster_java/src test/integration/cluster_java/src

# -DskipTests: image build only; ClusterConstantsTest runs in CI via mvn test.
RUN --mount=type=cache,target=/root/.m2/repository \
	mvn -q -f test/integration/cluster_java/pom.xml -DskipTests package

FROM eclipse-temurin:26-jre AS runtime

WORKDIR /app
COPY --from=builder \
	/src/test/integration/cluster_java/target/heron-cluster-java-*.jar \
	./heron-cluster-java.jar
COPY --chmod=+x docker/cluster_java/java/entrypoint-cluster.sh ./entrypoint-cluster.sh

ENTRYPOINT ["./entrypoint-cluster.sh"]

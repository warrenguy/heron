# Third-Party Notices

Heron includes material derived from Aeron (Real Logic Limited), licensed under
the Apache License, Version 2.0. The full license text is at
<https://www.apache.org/licenses/LICENSE-2.0>.

Upstream pin: Aeron 1.51.0 (see `specs/versions.yaml`).

Modified Apache 2.0 files retain Real Logic copyright and license headers
where applicable.

## Aeron cluster Java samples

### `test/integration/cluster_java/src/main/java/heron/clusterjava/ClusterConfig.java`

- **Source:** `io.aeron.samples.cluster.ClusterConfig` (Aeron 1.51.0 aeron-samples)
- **Changes:** package renamed to `heron.clusterjava`; MediaDriver publication and IPC
  term buffer lengths pinned to `ClusterConstants.TERM_MIN_LENGTH`; archive
  `localControlChannel` set to `aeron:ipc?term-length=64k`

### `test/integration/cluster_java/src/main/java/heron/clusterjava/EchoService.java`

- **Source:** `io.aeron.samples.cluster.EchoService` (Aeron 1.51.0 aeron-samples)
- **Changes:** package renamed to `heron.clusterjava`; echo behavior removed and
  lifecycle handlers stubbed for compose harness scaffold

## Aeron cluster SBE wire schema

### `specs/cluster-codecs-client.xml`

- **Source:** `aeron-cluster-codecs.xml` (Aeron 1.51.0 aeron-cluster)
- **Changes:** subset for Heron cluster client ingress, egress, and admin wire
  codecs; consensus-module log, service-control, membership, backup, snapshot,
  and related messages plus unused shared types removed; schema id, version,
  and layout of retained client messages match upstream

## `cluster/codecs/gensbe/`

Go flyweights generated from `specs/cluster-codecs-client.xml` via the SBE tool
(see `specs/versions.yaml`). Not copied from upstream Java sources.

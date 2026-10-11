# Third-Party Notices

Heron includes material derived from Aeron (Real Logic Limited), licensed under
the Apache License, Version 2.0. The full license text is in
[LICENSE-APACHE](LICENSE-APACHE).

Upstream Aeron pin: see `specs/versions.yaml`.

Modified Apache 2.0 files retain Real Logic copyright and license headers
where applicable.

## Aeron cluster Java samples

### `test/integration/cluster_java/src/main/java/heron/clusterjava/ClusterConfig.java`

- **Source:** `io.aeron.samples.cluster.ClusterConfig` (aeron-samples)
- **Changes:** package renamed to `heron.clusterjava`; MediaDriver publication and IPC
  term buffer lengths pinned to `ClusterConstants.TERM_MIN_LENGTH`; archive
  `localControlChannel` set to `aeron:ipc?term-length=64k`

### `test/integration/cluster_java/src/main/java/heron/clusterjava/EchoService.java`

- **Source:** `io.aeron.samples.cluster.EchoService` (aeron-samples)
- **Changes:** package renamed to `heron.clusterjava`; echo behavior removed and
  lifecycle handlers stubbed for compose harness scaffold

## Aeron cluster SBE wire schema

### `specs/cluster-codecs-wire.xml`

- **Source:** `aeron-cluster-codecs.xml` (aeron-cluster) @ pinned `aeron.ref`
  in `specs/versions.yaml`
- **Changes:** verbatim copy; hand codecs
  implement client messages and consensus templates 50-55 only; unused upstream
  templates have flyweights but no hand decode

## `cluster/codecs/clusterwire/`

Go flyweights generated from `specs/cluster-codecs-wire.xml` via the SBE tool
(see `specs/versions.yaml`). Not copied from upstream Java sources.

## Aeron archive SBE wire schema

### `specs/archive-codecs-client.xml`

- **Source:** `aeron-archive-codecs.xml` (aeron-archive)
- **Changes:** subset for Heron archive client control request, response,
  descriptor, and recording-signal wire codecs; catalog framing,
  listing-with-groups, replication, segment, and legacy recording-events
  messages plus unused shared types removed; schema id, version, and layout of
  retained client messages match upstream

## `archive/codecs/archivewire/`

Go flyweights generated from `specs/archive-codecs-client.xml` via the SBE tool
(see `specs/versions.yaml`). Not copied from upstream Java sources.

## Aeron cluster node-state SBE durable file schema

### `specs/node-state-codecs.xml`

- **Source:** `aeron-cluster-node-state-codecs.xml` (aeron-cluster) @ pinned
  `aeron.ref` in `specs/versions.yaml`
- **Changes:** subset for Heron `node-state.dat` durable file codecs;
  `nodeStateHeader`, `candidateTerm`, and `nodeStateFooter` only;
  `ClusterMembers` and unused shared types deferred; schema id, version, and
  layout of retained messages match upstream

## `cluster/election/nodewire/`

Go flyweights generated from `specs/node-state-codecs.xml` via the SBE tool
(see `specs/versions.yaml`). Not copied from upstream Java sources.

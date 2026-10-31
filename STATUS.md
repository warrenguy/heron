# Status

Feature status per component, measured against the Java stack at the Aeron
release pinned in [`specs/versions.yaml`](specs/versions.yaml).

## Transport client (`aeron`)

Works:

- Connect to a C or Java media driver; driver keepalive, driver timeout and
  client timeout detection.
- Blocking and async add and remove of publications, exclusive publications and
  subscriptions.
- `Offer` (fragmented up to the maximum message length) and `TryClaim` on
  concurrent and exclusive publications.
- `Subscription` and `Image` `Poll`, `ControlledPoll` and `BoundedControlledPoll`;
  fragment and controlled fragment assemblers.
- Multi-destination subscriptions (`Subscription.AddDestination` /
  `RemoveDestination`).
- Available and unavailable image handlers.
- Counters: blocking `Client.AddCounter` and `Counter.Close`.

Partial:

- `Subscription.Poll` runs on the goroutine that calls `Client.DoWork`; only
  `Publication.Offer` may run from other goroutines.
- Closing a subscription or the client does not call `UnavailableImageHandler`
  for its images.
- `atomicbuffer.VolatileInt32At` and `VolatileInt64At` check neither bounds
  nor alignment. An empty buffer panics. Any other out-of-range offset reads
  outside the slice with no panic.

Not implemented:

- Counters: static counters, available and unavailable counter handlers.
- Publication destinations (manual MDC `addDestination` / `removeDestination`).
- `Image.reject`, `blockPoll` / `rawPoll`, `controlledPeek`,
  `ExclusivePublication.appendPadding` / `offerBlock`, publication revoke.
- `ChannelUri` and `ChannelUriStringBuilder`.
- Background conductor thread, client name, close handlers, driver termination
  request.

## Archive client (`archive`)

Covers the operations the cluster needs.

Works:

- Connect with challenge/response authentication; close.
- Start and stop recording (optionally waiting for the recording signal), extend
  recording, replay, stop replay, list a single recording, truncate.
- Non-blocking start, extend and stop recording.
- Recording signal dispatch.
- Start and stop replication of a recording from another archive, blocking or
  non-blocking, with all upstream replication options.

Not implemented:

- `listRecordings`, `listRecordingsForUri`, `listRecordingSubscriptions`,
  `findLastMatchingRecording`.
- Position queries: recording, start, stop and max recorded position.
- Bounded replay, `ReplayParams`, `stopAllReplays`, `ReplayMerge`,
  `PersistentSubscription`.
- Purge, segment operations, archive id request.
- Automatic control session keepalive: the proxy has `TryKeepAlive`, but the
  client never sends it.

## Cluster client (`cluster/client`)

Works:

- `Connect` and `AsyncConnect` with authentication challenge and leader
  redirect.
- Ingress by per member endpoints or a single channel; `Offer`, `TryClaim`,
  keepalive and the snapshot admin request.
- Egress polling with fragment reassembly; leader failover on `NewLeaderEvent`;
  close.

Partial:

- With the default exclusive ingress publication, `Offer` returns
  `UnsupportedMessageLength` for messages larger than one frame instead of
  fragmenting.
- `TryClaim` copies the payload on commit rather than writing in place.
- Errors are untyped; there is no error handler hook.

Not implemented:

- Controlled egress dispatch: `ControlledEgressHandler` and
  `Config.ControlledEgressHandler` exist but are not used.
- Egress listener extensions, vector ingress offer, `IngressSessionDecorator`.

## Consensus module (`cluster`, `cluster/consensus`, `cluster/election`, `cluster/recordinglog`)

Experimental. Runs a cluster of Go members over live media drivers, with an
external Java Archive per member.

Works:

- Election (all upstream election states); `recording.log` and
  `node-state.dat` byte-compatible with Java.
- Leader commit and quorum; log publication; log recording on every member.
- Follower catchup by archive replay within a term; divergent log truncation;
  member restart and rejoin.
- Log replication: a member behind the new leader's term base copies the
  missing log from the leader's archive, then rejoins.
- Client sessions: connect, authentication, messages, keepalive, timeout, close,
  and transfer to a new leader.
- Service ack coordination with Go service containers; cluster timers.
- Snapshots: taking and loading. `RetainLatestSnapshot` keeps only the latest
  snapshot on a stopped member.
- Restart loads the latest snapshot and replays the local log after it. It
  rebuilds sessions and timers from both.

Partial:

- Timer races match upstream. When a service schedules or cancels a timer
  close to its expiry, a follower may disagree with the leader about whether
  that timer is live. After that follower becomes leader, the timer may fire
  twice or never.
- Reusing the id of a timer that has fired carries the same risk. A follower
  may drop the new schedule, so the timer never fires once that follower
  becomes leader.
- Cluster time unit is always nanoseconds; Java members of a mixed cluster need
  a nanosecond cluster clock.
- Mixed Java/Go clusters: only a cold start with an appointed leader is
  supported.
- A Go leader needs a multicast log channel (no publication MDC destinations in
  the transport).

Not implemented:

- Termination and shutdown, cluster control and node control toggles, the module
  state machine.
- Admin requests: the client snapshot request works only against a Java
  cluster.
- Cluster counters, mark file, backup and standby, `ClusterTool`, extensions,
  authorisation service, service messages.

## Service container (`cluster/service`)

Experimental.

Works:

- Joins the cluster log and consumes it up to the commit position.
- Session open, close and message, timer, and new leadership term callbacks;
  schedule and cancel timers.
- Service ack protocol; several services per member.
- Egress to clients through `ClientSession`: the leader's service replies to a
  client session. After a leader change on a Go cluster, the new leader's
  services reconnect egress.
- `CloseClientSession` asks the consensus module to close a client session.
  Every member sees the close through the log.
- Snapshots: `TakeSnapshotHandler` records a service snapshot on a snapshot
  action. On restart, `LoadSnapshotHandler` restores it.
- Restart on a non-empty log, with or without a snapshot.

Partial:

- The caller drives `Agent.DoWork` and the transport client, and supplies the
  commit position as a function; there is no commit position counter lookup.

Not implemented:

- Termination, `onRoleChange`, `onTerminate`, background work.
- Service messages to the log, mark file, counters.

## Out of scope

- Media driver (an external C or Java driver is required).
- Archive server and archive tooling.
- Java launchers (`ClusteredMediaDriver`, `ClusteredArchive`,
  `ClusterBackupMediaDriver`).
- System property configuration.

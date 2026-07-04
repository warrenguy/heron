# cluster-auction-client

Runnable Heron example mirroring upstream Aeron `BasicAuctionClusterClient` against the
unmodified Java `BasicAuctionClusteredService` on a UDP 3-node cluster.

## Prerequisites

- `JAVA_HOME` set (upstream cluster scripts require it)
- Aeron clone at `AERON_SRC` (default: `../aeron` from the Heron repo root) checked out at
  the ref in `specs/versions.yaml`
- `aeron-all` and `aeron-agent` built in that checkout (`basic-auction-cluster` needs the
  agent shadow JAR)
- Ports 9002, 9102, and 9202 free for the default 3-node topology

The Java cluster embeds its own media drivers. The Go client uses a separate Heron
`bin/aeronmd` instance and `AERON_DIR`.

## Run end-to-end

### 1. Start the Java cluster

Pick one path. Both start the same upstream `basic-auction-cluster` launcher.

#### Quick path

From `examples/cluster-auction-client/`:

```bash
./run-upstream-cluster.sh build
./run-upstream-cluster.sh start
```

The helper checks the ref pin, builds Aeron JARs, then execs upstream
`basic-auction-cluster`. Override the checkout
with `AERON_SRC=/path/to/aeron` when needed.

#### Manual path

```bash
export AERON_SRC=/path/to/aeron
cd "$AERON_SRC"
./gradlew --no-daemon :aeron-all:assemble :aeron-agent:shadowJar

cd aeron-samples/scripts/cluster
./basic-auction-cluster
# or one node: ./basic-auction-cluster 0|1|2
```

Cluster logs are written to `logs/cluster-*.log` under `aeron-samples/scripts/cluster`. Each
node also uses `node{id}/` in that directory. With no node argument, `./run-upstream-cluster.sh
start` starts all three nodes and streams those log files to stdout via `tail -F`. To run a
single node in the background, pass `0`, `1`, or `2` and read `logs/cluster-<id>.log` (the
wrapper follows that file when started through `run-upstream-cluster.sh`).

### 2. Start the Go client

In another terminal, from the Heron repo root:

```bash
export AERON_DIR=/tmp/aeron
mkdir -p "$AERON_DIR"
make aeronmd
bin/aeronmd
```

In a third terminal, also from the Heron repo root:

```bash
go run -tags=example ./examples/cluster-auction-client/ \
  -customer-id=42 \
  -num-bids=10 \
  -bid-interval-ms=1000
```

Expect paired `Sent(...)` and `SessionMessage(...)` lines. Occasional `false` on the last
field means the bid did not beat the current high price; that is normal auction behavior.

### Optional: upstream Java client

From `$AERON_SRC/aeron-samples/scripts/cluster` with the cluster already running:

```bash
./basic-auction-client 42 10 1000
```

## CLI flags

| Flag | Default | Notes |
|------|---------|-------|
| `-customer-id` | `1` | Bidder customer id |
| `-num-bids` | `10` | Bids to send |
| `-bid-interval-ms` | `1000` | Random delay in `[0, interval)` ms before each bid, including the first |
| `-hostnames` | `localhost,localhost,localhost` | One hostname per cluster member |
| `-aeron-dir` | `$AERON_DIR` or `/tmp/aeron` | Client media driver directory |
| `-ingress-channel` | `aeron:udp` | Use `aeron:ipc` only with IPC cluster pairing |
| `-egress-channel` | `aeron:udp?endpoint=localhost:0` | Dynamic UDP bind |
| `-ingress-endpoints` | *(auto)* | Empty builds `0=host:port,...` from hostnames for UDP |
| `-port-base` | `9000` | Auto endpoint port base |
| `-ports-per-node` | `100` | Auto endpoint stride |
| `-client-facing-port-offset` | `2` | Auto endpoint offset |

When `-ingress-endpoints` is empty and ingress media is UDP, endpoints are built as
`0=localhost:9002,1=localhost:9102,2=localhost:9202` (matching upstream tutorial ports).
For `ipc`/`ipcx` ingress, endpoints are forced empty.

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Offer failed with Aeron code `-1` (`NotConnected`) | All three nodes up; ports 9002/9102/9202 listening |
| Connect timeout | `aeronmd` running; `-aeron-dir` matches the driver |
| No egress output | Cluster running; UDP egress not firewalled |
| Cluster script fails | `JAVA_HOME` set; Aeron ref matches `specs/versions.yaml`; agent JAR built |
| Stale cluster processes | Stop with `pkill -f BasicAuctionClusteredServiceNode` if a prior run left JVMs up |

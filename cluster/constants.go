package cluster

// Cluster stream ID defaults. Values and names track the Aeron cluster
// configuration classes named per constant.
const (
	// DefaultLogStreamID is LOG_STREAM_ID_DEFAULT (aeron.cluster.log.stream.id)
	// on io.aeron.cluster.ConsensusModule.Configuration. Names the cluster log
	// stream shared by the leader log publication and every member log
	// subscription.
	DefaultLogStreamID int32 = 100

	// DefaultConsensusStreamID is CONSENSUS_STREAM_ID_DEFAULT
	// (aeron.cluster.consensus.stream.id) on
	// io.aeron.cluster.ConsensusModule.Configuration. This is the member-to-member
	// consensus control stream. It carries canvass, vote, new leadership term,
	// append, and commit.
	//
	// Not 105. That value is DefaultConsensusModuleStreamID
	// (CONSENSUS_MODULE_STREAM_ID_DEFAULT on
	// io.aeron.cluster.service.ClusteredServiceContainer.Configuration) and
	// carries service-to-module traffic.
	//
	// Stream id 108 is also not SBE template id 108 (RequestServiceAck). That
	// message travels on the service control stream (DefaultServiceStreamID).
	DefaultConsensusStreamID int32 = 108

	// DefaultReplayStreamID is REPLAY_STREAM_ID_DEFAULT on
	// io.aeron.cluster.service.ClusteredServiceContainer.Configuration
	// (aeron.cluster.replay.stream.id). ConsensusModule seeds its context from
	// that container default. Distinct from DefaultLogStreamID and
	// DefaultConsensusStreamID. The replay is a separate stream on a separate
	// channel.
	DefaultReplayStreamID int32 = 103

	// DefaultIngressStreamID is INGRESS_STREAM_ID_DEFAULT (101) on
	// io.aeron.cluster.client.AeronCluster.Configuration. Client-to-leader session
	// traffic. Same value as cluster/client DefaultIngressStreamID. Kept separate
	// so the server side does not import the client package.
	DefaultIngressStreamID int32 = 101

	// DefaultEgressStreamID is EGRESS_STREAM_ID_DEFAULT (102) on
	// io.aeron.cluster.client.AeronCluster.Configuration. Same value as
	// cluster/client DefaultEgressStreamID.
	DefaultEgressStreamID int32 = 102

	// DefaultServiceStreamID is SERVICE_STREAM_ID_DEFAULT (104) on
	// io.aeron.cluster.service.ClusteredServiceContainer.Configuration. Carries
	// module-to-service control: JoinLog and RequestServiceAck.
	DefaultServiceStreamID int32 = 104

	// DefaultConsensusModuleStreamID is CONSENSUS_MODULE_STREAM_ID_DEFAULT
	// (105) on io.aeron.cluster.service.ClusteredServiceContainer.Configuration.
	// Carries service-to-module control: ServiceAck.
	//
	// Distinct from DefaultConsensusStreamID (108), the member-to-member consensus
	// control stream.
	DefaultConsensusModuleStreamID int32 = 105
)

// Port block layout. Values track io.aeron.samples.cluster.ClusterConfig:
// port = portBase + memberID*PortsPerNode + offset.
const (
	// DefaultPortBase is the sample cluster port base. MembersStringConfig with
	// a zero PortBase takes this value.
	DefaultPortBase = 9000

	// PortsPerNode is PORTS_PER_NODE.
	PortsPerNode = 100

	// ArchiveControlPortOffset is ARCHIVE_CONTROL_PORT_OFFSET.
	ArchiveControlPortOffset = 1

	// ClientFacingPortOffset is CLIENT_FACING_PORT_OFFSET (ingress).
	ClientFacingPortOffset = 2

	// MemberFacingPortOffset is MEMBER_FACING_PORT_OFFSET (consensus).
	MemberFacingPortOffset = 3

	// LogPortOffset is LOG_PORT_OFFSET.
	LogPortOffset = 4

	// TransferPortOffset is TRANSFER_PORT_OFFSET (catchup).
	TransferPortOffset = 5
)

// Channel templates. Endpoints are never baked in: every endpoint comes from the
// cluster members string and is substituted per target.
const (
	// DefaultConsensusChannel is CONSENSUS_CHANNEL_DEFAULT plus an explicit term
	// length. Upstream default is bare "aeron:udp", which inherits the driver
	// term length; a fixed 64 KiB term bounds per-publication mapped memory when
	// several members share one host.
	DefaultConsensusChannel = "aeron:udp?term-length=64k"

	// DefaultLogChannel uses the same 64 KiB term. Upstream LOG_CHANNEL_DEFAULT
	// is "aeron:udp?term-length=64m"; 64 MiB per log publication is not viable
	// for several members on one host. Raise for production throughput.
	DefaultLogChannel = "aeron:udp?term-length=64k"

	// DefaultReplayChannel is REPLAY_CHANNEL_DEFAULT. Local IPC channel the
	// Archive uses to replay this member's own cluster log recording.
	//
	// Omits term-length. ArchiveConductor builds the replay publication with the
	// recording's own term length and MTU. A term-length param here is discarded.
	DefaultReplayChannel = "aeron:ipc"
)

// MinValidPort and MaxValidPort bound every port derived from CalculatePort.
const (
	MinValidPort = 1
	MaxValidPort = 65535
)

// IsValidPort reports whether port lies in MinValidPort..MaxValidPort.
func IsValidPort(port int) bool {
	return port >= MinValidPort && port <= MaxValidPort
}

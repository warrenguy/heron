package cluster

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster/election"
)

// BuildChannelWithEndpoint returns channel with endpoint= set to endpoint.
//
// An existing endpoint= at a query or pipe boundary is replaced; otherwise the
// parameter is appended. Substitution is idempotent. Errors when channel is not
// a udp channel, when endpoint is empty, or when endpoint contains any of the
// URI delimiters pipe, question mark, or equals.
func BuildChannelWithEndpoint(channel, endpoint string) (string, error) {
	if err := checkChannelEndpoint(channel, endpoint); err != nil {
		return "", fmt.Errorf("cluster: %w", err)
	}
	idx := aeron.ChannelEndpointParamIndex(channel)
	if idx >= 0 {
		return aeron.ReplaceChannelParamValue(
			channel,
			idx,
			aeron.ChannelEndpointParam,
			endpoint,
		), nil
	}
	return aeron.AppendChannelParam(channel, aeron.ChannelEndpointParam, endpoint), nil
}

// checkChannelEndpoint returns an error when channel is not a udp channel,
// endpoint is empty, or endpoint holds a pipe, question mark, or equals sign.
func checkChannelEndpoint(channel, endpoint string) error {
	if !isUDPChannel(channel) {
		return fmt.Errorf("invalid channel: %s", channel)
	}
	if endpoint == "" {
		return errors.New("endpoint is required")
	}
	if strings.ContainsAny(endpoint, endpointDelimiters) {
		return fmt.Errorf("invalid endpoint: %s", endpoint)
	}
	return nil
}

// Topology is the resolved channel map for one member.
//
// Construct with NewTopology. Topology's own fields are fixed after
// construction: the channel strings, the ingress-endpoints string, the stream
// ids, and the member backing array are never reassigned.
//
// The election.Member values reachable through Members and ThisMember are NOT
// fixed. They are shared mutable state owned by the election state machine,
// which rewrites Vote, IsLeader, HasTerminated, LeadershipTermID,
// CandidateTermID, LogPosition, and TimeOfLastAppendPositionNs on the invoker
// goroutine every duty cycle. Handing those pointers to the election machinery
// is the whole point of the type.
//
// Concurrency: read and write member state only from the invoker goroutine that
// runs the duty cycle. Channel strings, the ingress-endpoints string, and stream
// ids are safe to read from any goroutine: nothing writes those fields after
// construction. Nothing reachable from Members or ThisMember is safe that way.
//
// Topology owns the member backing array. Members returns pointers that alias
// that array, as election.MembersPtrs documents; the array is never appended to
// or reallocated, so the pointers stay valid and ThisMember stays
// pointer-identical to its Members element.
//
// Do not copy a Topology. Every accessor takes a pointer receiver; copying the
// struct by value produces a second Topology whose memberPtrs and thisMember
// alias the original's backing array, so a later change to either copy breaks
// the pointer-identity invariant. Pass the *Topology.
type Topology struct {
	memberPtrs []*election.Member
	thisMember *election.Member

	peerConsensusChannels     map[int32]string
	consensusSubscriptionChan string
	ingressSubscriptionChan   string
	logChannel                string
	logPublicationChannel     string
	logMDC                    bool
	publicationDestinations   []string
	replayChannel             string
	serviceControlChannel     string
	ingressEndpoints          string

	consensusStreamID       int32
	ingressStreamID         int32
	logStreamID             int32
	replayStreamID          int32
	consensusModuleStreamID int32
	serviceStreamID         int32
}

// NewTopology resolves cfg into a Topology.
//
// cfg must be normalized. NewTopology validates cfg and returns its error
// unchanged; an unnormalized Config fails validation rather than being silently
// defaulted. Members are parsed once during validation, which also resolves this
// member's position in the members string.
//
// NewTopology does not retain cfg. A caller that needs the config later, for the
// cluster directory or the election seed, must keep its own copy.
func NewTopology(cfg Config) (*Topology, error) {
	members, thisIndex, err := cfg.validateMembers()
	if err != nil {
		return nil, err
	}

	memberPtrs := election.MembersPtrs(members)
	thisMember := memberPtrs[thisIndex]
	_, logMDC := classifyLogEndpoints(members)
	peerChannels := make(map[int32]string, len(memberPtrs)-1)
	var destinations []string
	for i, m := range memberPtrs {
		if i == thisIndex {
			continue
		}
		ch, err := BuildChannelWithEndpoint(cfg.ConsensusChannel, m.ConsensusEndpoint)
		if err != nil {
			return nil, err
		}
		peerChannels[m.ID] = ch

		if !logMDC {
			continue
		}

		dest, err := udpEndpointChannel(cfg.LogChannel, m.LogEndpoint)
		if err != nil {
			return nil, err
		}
		destinations = append(destinations, dest)
	}

	consensusSub, err := BuildChannelWithEndpoint(cfg.ConsensusChannel, thisMember.ConsensusEndpoint)
	if err != nil {
		return nil, err
	}
	ingressSub, err := BuildChannelWithEndpoint(cfg.IngressChannel, thisMember.IngressEndpoint)
	if err != nil {
		return nil, err
	}
	logCh, err := BuildChannelWithEndpoint(cfg.LogChannel, thisMember.LogEndpoint)
	if err != nil {
		return nil, err
	}
	logPublication := logCh
	if logMDC {
		logPublication = manualMDCPublicationChannel(cfg.LogChannel, thisMember.LogEndpoint)
	}

	return &Topology{
		memberPtrs:                memberPtrs,
		thisMember:                thisMember,
		peerConsensusChannels:     peerChannels,
		consensusSubscriptionChan: consensusSub,
		ingressSubscriptionChan:   ingressSub,
		logChannel:                logCh,
		logPublicationChannel:     logPublication,
		logMDC:                    logMDC,
		publicationDestinations:   destinations,
		replayChannel:             cfg.ReplayChannel,
		serviceControlChannel:     cfg.ServiceControlChannel,
		ingressEndpoints:          buildIngressEndpointsString(memberPtrs),
		consensusStreamID:         cfg.ConsensusStreamID,
		ingressStreamID:           cfg.IngressStreamID,
		logStreamID:               cfg.LogStreamID,
		replayStreamID:            cfg.ReplayStreamID,
		consensusModuleStreamID:   cfg.ConsensusModuleStreamID,
		serviceStreamID:           cfg.ServiceStreamID,
	}, nil
}

// Members returns member pointers in members-string order.
//
// The returned slice aliases Topology state. Callers must not append to it or
// reorder it. Pass it straight to election.ElectionDeps.Members and
// election.HostAdapterConfig.Members so both see the same pointers.
//
// Concurrency: the members themselves are mutated by the election state machine.
// Read and write them only from the invoker goroutine. See Topology.
func (t *Topology) Members() []*election.Member {
	return t.memberPtrs
}

// ThisMember returns the local member. Pointer-identical to the matching
// Members element.
//
// Concurrency: same invoker-only rule as Members. HostAdapter stamps this member
// on every leader position update.
func (t *Topology) ThisMember() *election.Member {
	return t.thisMember
}

// PeerConsensusChannels returns a fresh map of member id to consensus channel
// URI for every member except this one.
//
// Self is excluded: consensus publications target peers only. Called once at
// open, not on a duty cycle; the copy is deliberate so callers may retain and
// mutate their own map.
func (t *Topology) PeerConsensusChannels() map[int32]string {
	return maps.Clone(t.peerConsensusChannels)
}

// ConsensusChannelFor returns the consensus channel URI for publishing to
// memberID. Second return is false for an unknown member id and for this
// member's own id.
func (t *Topology) ConsensusChannelFor(memberID int32) (string, bool) {
	ch, ok := t.peerConsensusChannels[memberID]
	return ch, ok
}

// ConsensusSubscriptionChannel returns the local consensus bind channel, with
// endpoint set to this member's consensus endpoint.
func (t *Topology) ConsensusSubscriptionChannel() string {
	return t.consensusSubscriptionChan
}

// IngressSubscriptionChannel returns the local client ingress channel, with
// endpoint set to this member's members-string ingress endpoint (port offset 2).
func (t *Topology) IngressSubscriptionChannel() string {
	return t.ingressSubscriptionChan
}

// IngressEndpoints returns the cluster ingress-endpoints string resolved at
// construction, sorted by member id ascending.
//
// Format is "id=endpoint" pairs comma-joined (for example
// "0=host:port,1=host:port"). Matches cluster/client Config.IngressEndpoints.
func (t *Topology) IngressEndpoints() string {
	return t.ingressEndpoints
}

// LogChannel returns this member's log receive channel.
//
// The endpoint is this member's log endpoint. The publication channel is
// [Topology.LogPublicationChannel].
func (t *Topology) LogChannel() string {
	return t.logChannel
}

// IsLogMDC reports whether the log publication is manual multi-destination-cast.
//
// True when every member has its own log endpoint. False when all members
// share one, including a one-member cluster. The check does not test the
// address for a multicast group.
func (t *Topology) IsLogMDC() bool {
	return t.logMDC
}

// LogPublicationChannel returns the base channel of the exclusive log
// publication.
//
// Shared mode returns [Topology.LogChannel]. Manual multi-destination-cast
// mode returns the log template with control-mode=manual, control set to
// this member's log host and port 0, and ssc=false. It does not set endpoint.
// [OpenLogStreams] adds tags and alias.
func (t *Topology) LogPublicationChannel() string {
	return t.logPublicationChannel
}

// LogPublicationDestinations returns one send-destination URI per other
// member, in members-string order.
//
// Nil in shared mode. The caller receives a copy.
func (t *Topology) LogPublicationDestinations() []string {
	return slices.Clone(t.publicationDestinations)
}

// manualMDCPublicationChannel builds the manual multi-destination-cast
// publication channel from the log template.
func manualMDCPublicationChannel(template, logEndpoint string) string {
	channel := aeron.SetChannelParam(
		template,
		aeron.ChannelControlModeParam,
		logSubscriptionControlMode,
	)
	if control := controlEndpointFromLog(logEndpoint); control != "" {
		channel = aeron.SetChannelParam(channel, channelControlParam, control)
	}
	return aeron.SetChannelParam(channel, channelSpiesSimulateConnectionParam, "false")
}

// controlEndpointFromLog returns endpoint with the port replaced by 0.
// Empty when endpoint has no port.
func controlEndpointFromLog(endpoint string) string {
	colon := strings.LastIndex(endpoint, ":")
	if colon < 0 {
		return ""
	}
	return endpoint[:colon+1] + "0"
}

// ConsensusStreamID returns the member-to-member consensus stream id.
func (t *Topology) ConsensusStreamID() int32 {
	return t.consensusStreamID
}

// IngressStreamID returns the client ingress stream id.
func (t *Topology) IngressStreamID() int32 {
	return t.ingressStreamID
}

// LogStreamID returns the cluster log stream id.
func (t *Topology) LogStreamID() int32 {
	return t.logStreamID
}

// ReplayChannel returns the local channel the Archive uses to replay this
// member's own cluster log recording.
//
// No endpoint substitution. The replay is local. The members string carries no
// replay endpoint.
func (t *Topology) ReplayChannel() string {
	return t.replayChannel
}

// ReplayStreamID returns the cluster log replay stream id.
func (t *Topology) ReplayStreamID() int32 {
	return t.replayStreamID
}

// ServiceControlChannel returns the module-service control channel.
//
// No endpoint substitution. Shared by stream 104 and stream 105. Stored and
// returned verbatim from Config.
func (t *Topology) ServiceControlChannel() string {
	return t.serviceControlChannel
}

// ConsensusModuleStreamID returns the service-to-module control stream id
// (default 105). Carries ServiceAck frames.
func (t *Topology) ConsensusModuleStreamID() int32 {
	return t.consensusModuleStreamID
}

// ServiceStreamID returns the module-to-service control stream id (default
// 104). Carries JoinLog and RequestServiceAck frames.
func (t *Topology) ServiceStreamID() int32 {
	return t.serviceStreamID
}

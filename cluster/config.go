// Package cluster holds the consensus module member process assembly:
// configuration, validation, cluster channel topology derived from a cluster
// members string, and per-peer consensus publication lifecycle.
//
// This package is not the consensus agent library. The agent, log publisher, and
// log adapter live in cluster/consensus; the election state machine and member
// parsing live in cluster/election. Package cluster is the only package that
// imports both.
//
// Concurrency: Config is a value; Normalize returns a copy and mutates nothing.
// A Topology's own fields are fixed after NewTopology, but the cluster members it
// exposes are shared mutable state written by the election state machine; touch
// them only from the invoker goroutine. See [Topology].
package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/idlestrategy"
)

// Config holds member process configuration. Call Normalize before Validate and
// before NewTopology.
type Config struct {
	// MemberID is this member's cluster id. Must appear in ClusterMembers.
	MemberID int32

	// ClusterMembers is the members string in election.ParseMembers format:
	// id,ingress,consensus,log,catchup,archive[,archiveResponse][,egressResponse]
	// with a pipe after each member.
	ClusterMembers string

	// ClusterDir holds recording.log and node-state.dat.
	ClusterDir string

	// AeronDir is the directory of an already-running media driver. This package
	// never launches a driver. Required unless Aeron is set.
	AeronDir string

	// Aeron is an already-connected transport client to use instead of
	// connecting one. Nil means connect an owned client from AeronConfig and
	// close it at teardown; non-nil means the caller owns the lifecycle.
	//
	// A harness that drives several members in one process shares its assertion
	// client this way.
	Aeron *aeron.Client

	// AeronConfig is passed to aeron.Connect when Aeron is nil. Transport
	// defaults are applied by aeron.Connect, not by Normalize; Normalize only
	// bridges AeronDir into AeronConfig.AeronDir when the latter is empty.
	//
	// The duty cycle is invoker-driven, so leave AeronConfig.UseConductorInvoker
	// nil or true. Share AeronConfig.NanoClock and AeronConfig.EpochClock with
	// the election deps when a single clock source is wanted.
	AeronConfig aeron.Config

	// ConsensusChannel is the member-to-member control channel template. Must be
	// a udp channel and must not carry endpoint=; endpoints come from
	// ClusterMembers. Zero value takes DefaultConsensusChannel.
	ConsensusChannel string

	// LogChannel is the cluster log channel template. Same endpoint rule as
	// ConsensusChannel: the log endpoint comes from the members string, which for
	// cross-member delivery carries one shared multicast group on every row. Zero
	// value takes DefaultLogChannel.
	LogChannel string

	// ConsensusStreamID zero takes DefaultConsensusStreamID.
	ConsensusStreamID int32

	// LogStreamID zero takes DefaultLogStreamID.
	LogStreamID int32

	// ArchiveClientEnabled opens an archive control client during open. False
	// skips archive entirely and restricts recovery to an empty recording log.
	ArchiveClientEnabled bool

	// ArchiveControlRequestChannel and ArchiveControlResponseChannel are required
	// when ArchiveClientEnabled.
	ArchiveControlRequestChannel  string
	ArchiveControlResponseChannel string

	// ServiceCount feeds recordinglog.NewRecoveryPlanConfig. Zero for a member
	// without clustered services. Must not be negative.
	ServiceCount int

	// Election seeds the election state machine. Zero timeout fields take
	// election.DefaultElectionConfig values.
	//
	// Normalize overwrites MemberID, AppointedLeaderID, and
	// GracefulClosedLeaderID on this struct from Config.MemberID,
	// Config.AppointedLeaderID, and Config.GracefulClosedLeaderID. Setting those
	// three here has no effect once Normalize runs, and Validate rejects a value
	// that disagrees with the Config fields.
	//
	// The indirection exists because an int32 cannot distinguish "member 0" from
	// "unset", and election reads unset as election.NullMemberID, not zero.
	Election election.ElectionConfig

	// AppointedLeaderID restricts nomination to one member. Nil means no
	// appointed leader, which is what an ordinary election needs and what
	// Normalize produces from a zero Config.
	//
	// A non-nil value must name a member present in ClusterMembers. Every member
	// whose id differs then stops before the nominate transition, so a cluster
	// configured this way cannot elect a new leader if the appointed member is
	// down. Set it only for a deliberately pinned leader.
	AppointedLeaderID *int32

	// GracefulClosedLeaderID names a leader that shut down cleanly, whose vote is
	// excluded from the unanimity fast path. Nil means none, which is what
	// Normalize produces from a zero Config.
	//
	// A non-nil value must name a member present in ClusterMembers. Leaving a
	// stale value set silently drops that member from every unanimity check.
	GracefulClosedLeaderID *int32

	// Idle paces the duty cycle. Zero value takes idlestrategy.NewDefaultBackoff.
	Idle idlestrategy.IdleStrategy

	// RecordingLogFileSyncLevel and NodeStateFileSyncLevel select durability for
	// recording.log and node-state.dat. Zero takes recordinglog.FileSyncData;
	// FileSyncNone is not reachable through this type.
	RecordingLogFileSyncLevel recordinglog.FileSyncLevel
	NodeStateFileSyncLevel    recordinglog.FileSyncLevel
}

// Normalize applies defaults and returns a copy. Idempotent.
func (cfg Config) Normalize() Config {
	if cfg.ConsensusChannel == "" {
		cfg.ConsensusChannel = DefaultConsensusChannel
	}
	if cfg.LogChannel == "" {
		cfg.LogChannel = DefaultLogChannel
	}
	if cfg.ConsensusStreamID == 0 {
		cfg.ConsensusStreamID = DefaultConsensusStreamID
	}
	if cfg.LogStreamID == 0 {
		cfg.LogStreamID = DefaultLogStreamID
	}
	if cfg.Idle == nil {
		cfg.Idle = idlestrategy.NewDefaultBackoff()
	}
	if cfg.RecordingLogFileSyncLevel == 0 {
		cfg.RecordingLogFileSyncLevel = recordinglog.FileSyncData
	}
	if cfg.NodeStateFileSyncLevel == 0 {
		cfg.NodeStateFileSyncLevel = recordinglog.FileSyncData
	}
	if cfg.AeronDir != "" && cfg.AeronConfig.AeronDir == "" {
		cfg.AeronConfig.AeronDir = cfg.AeronDir
	}

	defaults := election.DefaultElectionConfig()
	if cfg.Election.ElectionTimeoutNs == 0 {
		cfg.Election.ElectionTimeoutNs = defaults.ElectionTimeoutNs
	}
	if cfg.Election.ElectionStatusIntervalNs == 0 {
		cfg.Election.ElectionStatusIntervalNs = defaults.ElectionStatusIntervalNs
	}
	if cfg.Election.LeaderHeartbeatTimeoutNs == 0 {
		cfg.Election.LeaderHeartbeatTimeoutNs = defaults.LeaderHeartbeatTimeoutNs
	}
	if cfg.Election.LeaderHeartbeatIntervalNs == 0 {
		cfg.Election.LeaderHeartbeatIntervalNs = defaults.LeaderHeartbeatIntervalNs
	}
	if cfg.Election.StartupCanvassTimeoutNs == 0 {
		cfg.Election.StartupCanvassTimeoutNs = defaults.StartupCanvassTimeoutNs
	}

	// Always stamp member id and leader sentinels so a partially populated
	// Election cannot leave zero (member 0) in place of unset.
	cfg.Election.MemberID = cfg.MemberID
	cfg.Election.AppointedLeaderID = optionalMemberID(cfg.AppointedLeaderID)
	cfg.Election.GracefulClosedLeaderID = optionalMemberID(cfg.GracefulClosedLeaderID)

	return cfg
}

// Validate checks a normalized Config. All errors carry the cluster: prefix.
//
// Call Normalize first; Validate is safe on an unnormalized value but returns
// the first failed rule rather than applying defaults.
func (cfg Config) Validate() error {
	_, _, err := cfg.validateMembers()
	return err
}

// validateMembers runs Validate and returns the parsed members plus the index of
// MemberID within them, so NewTopology can reuse both without a second
// ParseMembers or a second scan for the local member.
func (cfg Config) validateMembers() ([]election.Member, int, error) {
	if cfg.ClusterDir == "" {
		return nil, 0, errors.New("cluster: ClusterDir is required")
	}
	if cfg.AeronDir == "" && cfg.Aeron == nil {
		return nil, 0, errors.New("cluster: AeronDir is required when Aeron is not set")
	}
	if cfg.ClusterMembers == "" {
		return nil, 0, errors.New("cluster: ClusterMembers is required")
	}

	members, err := election.ParseMembers(cfg.ClusterMembers)
	if err != nil {
		return nil, 0, fmt.Errorf("cluster: invalid ClusterMembers: %w", err)
	}
	if len(members) == 0 {
		return nil, 0, errors.New("cluster: ClusterMembers must contain at least one member")
	}

	memberIndex := make(map[int32]int, len(members))
	for i, m := range members {
		if _, ok := memberIndex[m.ID]; ok {
			return nil, 0, fmt.Errorf("cluster: duplicate cluster member id %d", m.ID)
		}
		memberIndex[m.ID] = i
	}

	thisIndex, err := requireKnownMember(memberIndex, cfg.MemberID, "MemberID")
	if err != nil {
		return nil, 0, err
	}

	for _, m := range members {
		if err := validateMemberEndpoints(m); err != nil {
			return nil, 0, err
		}
	}

	if err := validateChannelTemplate(cfg.ConsensusChannel, "ConsensusChannel"); err != nil {
		return nil, 0, err
	}
	if err := validateChannelTemplate(cfg.LogChannel, "LogChannel"); err != nil {
		return nil, 0, err
	}
	if cfg.ConsensusStreamID == cfg.LogStreamID {
		return nil, 0, errors.New("cluster: ConsensusStreamID and LogStreamID must differ")
	}
	if cfg.ServiceCount < 0 {
		return nil, 0, errors.New("cluster: ServiceCount must not be negative")
	}

	if err := election.ValidateElectionConfig(cfg.Election); err != nil {
		return nil, 0, fmt.Errorf("cluster: invalid Election config: %w", err)
	}
	if cfg.Election.MemberID != cfg.MemberID {
		return nil, 0, errors.New("cluster: Election.MemberID must equal MemberID")
	}

	if cfg.AppointedLeaderID != nil {
		if _, err := requireKnownMember(
			memberIndex,
			*cfg.AppointedLeaderID,
			"AppointedLeaderID",
		); err != nil {
			return nil, 0, err
		}
	}
	if cfg.GracefulClosedLeaderID != nil {
		if _, err := requireKnownMember(
			memberIndex,
			*cfg.GracefulClosedLeaderID,
			"GracefulClosedLeaderID",
		); err != nil {
			return nil, 0, err
		}
	}

	if cfg.Election.AppointedLeaderID != optionalMemberID(cfg.AppointedLeaderID) {
		return nil, 0, errors.New(
			"cluster: Election.AppointedLeaderID must match AppointedLeaderID",
		)
	}
	if cfg.Election.GracefulClosedLeaderID != optionalMemberID(cfg.GracefulClosedLeaderID) {
		return nil, 0, errors.New(
			"cluster: Election.GracefulClosedLeaderID must match GracefulClosedLeaderID",
		)
	}

	if cfg.Idle == nil {
		return nil, 0, errors.New("cluster: IdleStrategy is required")
	}
	if cfg.ArchiveClientEnabled {
		if cfg.ArchiveControlRequestChannel == "" {
			return nil, 0, errors.New(
				"cluster: ArchiveControlRequestChannel is required when ArchiveClientEnabled",
			)
		}
		if cfg.ArchiveControlResponseChannel == "" {
			return nil, 0, errors.New(
				"cluster: ArchiveControlResponseChannel is required when ArchiveClientEnabled",
			)
		}
	}
	if err := validateFileSyncLevel(
		cfg.RecordingLogFileSyncLevel,
		"RecordingLogFileSyncLevel",
	); err != nil {
		return nil, 0, err
	}
	if err := validateFileSyncLevel(
		cfg.NodeStateFileSyncLevel,
		"NodeStateFileSyncLevel",
	); err != nil {
		return nil, 0, err
	}
	return members, thisIndex, nil
}

// optionalMemberID maps a nil Config leader pointer to election.NullMemberID.
// Non-nil keeps the pointed value, including zero (member 0).
func optionalMemberID(id *int32) int32 {
	if id == nil {
		return election.NullMemberID
	}
	return *id
}

// requireKnownMember returns the members-string index of id, and rejects an id
// missing from the ClusterMembers id set.
func requireKnownMember(memberIndex map[int32]int, id int32, field string) (int, error) {
	i, ok := memberIndex[id]
	if !ok {
		return 0, fmt.Errorf(
			"cluster: %s %d is not present in ClusterMembers",
			field,
			id,
		)
	}
	return i, nil
}

const udpChannelPrefix = "aeron:udp"

// endpointDelimiters are the channel URI characters an endpoint token must not
// contain: each one would split the parameter list at substitution.
const endpointDelimiters = "|?="

// isUDPChannel reports whether channel is a udp channel URI with an optional
// parameter list.
func isUDPChannel(channel string) bool {
	if !strings.HasPrefix(channel, udpChannelPrefix) {
		return false
	}
	rest := channel[len(udpChannelPrefix):]
	return rest == "" || strings.HasPrefix(rest, "?")
}

// validateChannelTemplate requires a udp channel template with no baked-in
// endpoint=; endpoints come from ClusterMembers.
func validateChannelTemplate(channel, field string) error {
	if !isUDPChannel(channel) {
		return errors.New("cluster: " + field + " must be a udp channel")
	}
	if aeron.ChannelEndpointParamIndex(channel) >= 0 {
		return errors.New("cluster: " + field + " must not set endpoint=")
	}
	return nil
}

// validateFileSyncLevel rejects FileSyncNone and unknown levels. Only
// FileSyncData and FileSyncFull are allowed on this Config.
func validateFileSyncLevel(level recordinglog.FileSyncLevel, field string) error {
	if level != recordinglog.FileSyncData && level != recordinglog.FileSyncFull {
		return errors.New(
			"cluster: " + field + " must be FileSyncData or FileSyncFull",
		)
	}
	return nil
}

type memberEndpoint struct {
	name      string
	value     string
	checkPort bool
}

func validateMemberEndpoints(m election.Member) error {
	// Fixed array: five required endpoints, no heap alloc on the validate path.
	endpoints := [...]memberEndpoint{
		{name: "ingress", value: m.IngressEndpoint},
		{name: "consensus", value: m.ConsensusEndpoint, checkPort: true},
		{name: "log", value: m.LogEndpoint, checkPort: true},
		{name: "catchup", value: m.CatchupEndpoint},
		{name: "archive", value: m.ArchiveEndpoint},
	}
	for _, ep := range endpoints {
		if ep.value == "" {
			return fmt.Errorf(
				"cluster: member %d has an empty %s endpoint",
				m.ID,
				ep.name,
			)
		}
		if strings.ContainsAny(ep.value, endpointDelimiters) {
			return fmt.Errorf(
				"cluster: member %d has an invalid %s endpoint: %s",
				m.ID,
				ep.name,
				ep.value,
			)
		}
		if ep.checkPort {
			if err := validateEndpointPort(m.ID, ep.name, ep.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateEndpointPort checks consensus and log endpoints carry a numeric port
// in MinValidPort..MaxValidPort. Split on the last colon so bracketed IPv6 works.
func validateEndpointPort(memberID int32, name, endpoint string) error {
	colon := strings.LastIndex(endpoint, ":")
	if colon < 0 {
		return fmt.Errorf(
			"cluster: member %d has a %s endpoint without a port: %s",
			memberID,
			name,
			endpoint,
		)
	}
	port, err := strconv.Atoi(endpoint[colon+1:])
	if err != nil || !IsValidPort(port) {
		return fmt.Errorf(
			"cluster: member %d has an out-of-range %s endpoint port: %s",
			memberID,
			name,
			endpoint,
		)
	}
	return nil
}

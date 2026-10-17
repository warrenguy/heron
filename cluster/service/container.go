package service

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
)

const (
	// DefaultControlChannel is the shared module-service control channel.
	// Matches upstream CONTROL_CHANNEL_DEFAULT.
	DefaultControlChannel = "aeron:ipc?term-length=128k"

	// DefaultServiceStreamID is the module-to-service control stream.
	// Matches upstream SERVICE_STREAM_ID_DEFAULT (104).
	DefaultServiceStreamID int32 = 104

	// DefaultConsensusModuleStreamID is the service-to-module control stream.
	// Matches upstream CONSENSUS_MODULE_STREAM_ID_DEFAULT (105).
	DefaultConsensusModuleStreamID int32 = 105

	// DefaultSnapshotChannel is the snapshot publication channel.
	// Matches upstream SNAPSHOT_CHANNEL_DEFAULT.
	DefaultSnapshotChannel = "aeron:ipc?alias=snapshot"

	// DefaultSnapshotStreamID is the snapshot publication stream.
	// Matches upstream SNAPSHOT_STREAM_ID_DEFAULT (106).
	DefaultSnapshotStreamID int32 = 106

	// archiveResponseStreamBase is the upstream service archive response
	// stream for service 0 at cluster id 0: 100 + 20 + 1.
	archiveResponseStreamBase int32 = 121

	ipcChannelPrefix = "aeron:ipc"
)

var (
	errAdderReturnedNoSubscription = errors.New("service: adder returned no subscription")
	errAdderReturnedNoPublication  = errors.New("service: adder returned no publication")
	errArchiveTransportNotClient   = errors.New(
		"service: Archive requires an *aeron.Client transport client",
	)
)

// SnapshotPublicationAdder adds the exclusive snapshot publication.
//
// Behaviour:
//   - OfferAddExclusivePublication starts an async add and returns its
//     correlation id.
//   - PollAddExclusivePublication runs one conductor tick.
//   - CancelPending abandons an in-flight add.
//
// As-built [*aeron.Client] may retry a full to-driver ring with idle sleep
// until DriverTimeout. Callers must treat OfferAddExclusivePublication as
// cold-path only.
//
// Concurrency: invoker-only.
type SnapshotPublicationAdder interface {
	OfferAddExclusivePublication(streamID int32, channel string) (int64, error)
	PollAddExclusivePublication(
		correlationID int64,
	) (*aeron.ExclusivePublication, bool, error)
	CancelPending(correlationID int64)
}

var _ SnapshotPublicationAdder = (*aeron.Client)(nil)

// TransportClient registers control handles and snapshot publications on a
// media driver.
//
// *aeron.Client satisfies this interface as-built.
type TransportClient interface {
	SubscriptionAdder
	SnapshotPublicationAdder
	AddPublication(streamID int32, channel string) (*aeron.Publication, error)
}

var _ TransportClient = (*aeron.Client)(nil)

// Config configures [Open]. Zero-value channel and stream id fields take the
// defaults stated per field. Open passes FragmentLimit and StartAck to
// [NewAgent].
type Config struct {
	// Aeron is the borrowed transport client. Required. The container does not
	// own Aeron. Open uses Aeron for the control pair. The agent uses Aeron to
	// add the log subscription at join time.
	//
	// With Archive set, Aeron must be an *aeron.Client. The archive client
	// shares Aeron. Open then calls DoWork on Aeron until the archive connect
	// completes. Call Open on the invoker goroutine.
	Aeron TransportClient

	// Service receives log-event callbacks. Required for [Open]. A nil-service
	// Agent remains a unit-only inject through [NewAgent].
	Service ClusteredService

	// CommittedPosition supplies the committed log prefix this member treats
	// as safe for application consumption. Required.
	CommittedPosition func() int64

	// ServiceID is this container's service id. Must not be negative. Zero
	// matches upstream SERVICE_ID_DEFAULT.
	ServiceID int32

	// ControlChannel is the shared control channel for the service stream and
	// the consensus module stream. Empty takes DefaultControlChannel.
	ControlChannel string

	// ServiceStreamID is the module-to-service control stream. Zero takes
	// DefaultServiceStreamID. Must differ from ConsensusModuleStreamID and
	// SnapshotStreamID after defaulting.
	ServiceStreamID int32

	// ConsensusModuleStreamID is the service-to-module control stream. Zero
	// takes DefaultConsensusModuleStreamID. Must differ from ServiceStreamID
	// and SnapshotStreamID after defaulting.
	ConsensusModuleStreamID int32

	// FragmentLimit caps fragments per log poll. Open passes FragmentLimit to
	// [NewAgent]. Zero or negative takes DefaultLogFragmentLimit there.
	FragmentLimit int

	// StartAck enables the spontaneous start ack a Java consensus module
	// consumes during recovery. Nil disables the start ack. Open passes
	// StartAck to [NewAgent]. See [StartAckConfig].
	StartAck *StartAckConfig

	// Archive configures the archive client. Nil connects none. Open leaves
	// Archive unchanged.
	//
	// Open applies WithRemoteControl, then these defaults:
	//   - empty ControlRequestChannel and ControlResponseChannel take
	//     archive.DefaultLocalControlChannel
	//   - zero ControlRequestStreamID takes archive.DefaultLocalControlStreamID
	//   - zero ControlResponseStreamID takes 121 + ServiceID
	//
	// Archive must not set Aeron, RecordingSignalHandler, LocalControlChannel,
	// or LocalControlStreamID. Both control channels must be IPC.
	// ControlResponseStreamID must differ from ControlRequestStreamID. Each
	// control stream id must differ from each container stream id after
	// defaulting.
	Archive *archive.Config

	// SnapshotChannel is the snapshot publication channel. Empty takes
	// DefaultSnapshotChannel. Must not set session-id=.
	SnapshotChannel string

	// SnapshotStreamID is the snapshot publication stream. Zero takes
	// DefaultSnapshotStreamID. Must differ from ServiceStreamID and
	// ConsensusModuleStreamID after defaulting.
	SnapshotStreamID int32
}

// StartAckConfig configures the spontaneous start ack a Java consensus module
// consumes during recovery.
//
// Behaviour: the agent queues one start entry before any delivered control
// frame.
//
// Readiness:
//   - successful start work, or a nil Service: the start entry becomes ready
//   - Start error: the start entry stays unready. Drain does not send it
//
// When the start entry is ready, drain sends it as ack id 0. Every later ack
// id shifts up by one. That is the Java numbering.
//
// Nil disables the start ack. StartAck must not be set against a consensus
// module that pairs acks only with published frames. An unpaired first queued
// entry marks ServiceAckFailure.
type StartAckConfig struct {
	// LogPosition is the recovered log position the start ack carries. Must
	// equal the consensus module's expected ack position. A cold-start Java
	// consensus module expects 0. Must not be negative. Position 0 is a
	// value, not a default.
	LogPosition int64

	// RelevantID carries the transport client id on the member's media driver
	// ([aeron.Client.ClientId]). The Java consensus module stores RelevantID
	// for service liveness tracking. The Java consensus module aborts recovery
	// when any start ack carries the -1 sentinel. RelevantID is the caller-set
	// value. Heron does not rewrite RelevantID to the sentinel.
	RelevantID int64
}

// Open assembles one service container against a transport client.
//
// Open order:
//  1. default the config
//  2. validate the config. With Archive set, build the archive config
//  3. control subscription on ServiceStreamID
//  4. control publication on ConsensusModuleStreamID
//  5. with Archive set, connect the archive client
//  6. proxy bind
//  7. agent build with control ingress, proxy, and archive client wired
//
// Behaviour: Open registers the two adds. With Archive set, Open then
// connects the archive client. The connect pumps the transport client for up
// to the archive ConnectTimeout. Open does not wait for a consensus module.
// An unavailable image frees that session's reassembly builder.
//
// Ownership: the container owns the control subscription, the control
// publication, and the archive client. The container does not own the
// transport client.
//
// Returns an error when:
//   - Aeron, Service, or CommittedPosition is nil
//   - ServiceID is negative
//   - StartAck is set and LogPosition is negative
//   - the two control stream ids collide after defaulting
//   - SnapshotChannel sets session-id=
//   - SnapshotStreamID equals a control stream id after defaulting
//   - Archive sets Aeron, RecordingSignalHandler, or LocalControlChannel
//   - an archive control channel is not IPC
//   - the archive response stream equals the request stream or a container
//     stream id after defaulting, or overflows int32
//   - either add fails or returns no handle
//   - Archive is set and Aeron is not an *aeron.Client
//   - the archive connect fails
//
// On error nothing stays open. A failed step closes the handles opened
// before it.
func Open(cfg Config) (*Container, error) {
	cfg = defaultConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	var archiveCfg archive.Config
	if cfg.Archive != nil {
		var err error
		archiveCfg, err = serviceArchiveConfig(cfg)
		if err != nil {
			return nil, err
		}
	}

	sub, err := cfg.Aeron.AddSubscription(cfg.ServiceStreamID, cfg.ControlChannel)
	sub, err = addedHandle(sub, err, errAdderReturnedNoSubscription)
	if err != nil {
		return nil, err
	}

	pub, err := cfg.Aeron.AddPublication(cfg.ConsensusModuleStreamID, cfg.ControlChannel)
	pub, err = addedHandle(pub, err, errAdderReturnedNoPublication)
	if err != nil {
		_ = sub.Close()
		return nil, err
	}

	var archiveClient *archive.Client
	if cfg.Archive != nil {
		archiveClient, err = connectServiceArchive(cfg.Aeron, archiveCfg)
		if err != nil {
			_ = pub.Close()
			_ = sub.Close()
			return nil, err
		}
	}

	proxy := NewConsensusModuleProxy()
	proxy.SetPublication(pub)
	ingress := NewSubscriptionControlIngress(sub)
	sub.SetUnavailableImageHandler(func(image *aeron.Image) {
		if image != nil {
			ingress.FreeSessionBuffer(image.SessionID())
		}
	})

	return &Container{
		agent: NewAgent(AgentConfig{
			ServiceID:         cfg.ServiceID,
			Service:           cfg.Service,
			CommittedPosition: cfg.CommittedPosition,
			Subscriptions:     cfg.Aeron,
			FragmentLimit:     cfg.FragmentLimit,
			Control:           ingress,
			ModuleProxy:       proxy,
			StartAck:          cfg.StartAck,
			Archive:           archiveClient,
		}),
		ingress:       ingress,
		controlSub:    sub,
		controlPub:    pub,
		archiveClient: archiveClient,
	}, nil
}

// serviceArchiveConfig builds the archive connect config from a copy of
// cfg.Archive, as [Config.Archive] documents. cfg must hold its defaults.
func serviceArchiveConfig(cfg Config) (archive.Config, error) {
	archiveCfg := *cfg.Archive
	switch {
	case archiveCfg.Aeron != nil:
		return archive.Config{}, errors.New("service: Archive must not set Aeron")
	case archiveCfg.RecordingSignalHandler != nil:
		return archive.Config{}, errors.New("service: Archive must not set RecordingSignalHandler")
	case archiveCfg.LocalControlChannel != "":
		return archive.Config{}, errors.New("service: Archive must not set LocalControlChannel")
	case archiveCfg.LocalControlStreamID != 0:
		return archive.Config{}, errors.New("service: Archive must not set LocalControlStreamID")
	}

	archiveCfg = archiveCfg.WithRemoteControl()
	if archiveCfg.ControlRequestChannel == "" {
		archiveCfg.ControlRequestChannel = archive.DefaultLocalControlChannel
	}
	if archiveCfg.ControlResponseChannel == "" {
		archiveCfg.ControlResponseChannel = archive.DefaultLocalControlChannel
	}
	if archiveCfg.ControlRequestStreamID == 0 {
		archiveCfg.ControlRequestStreamID = archive.DefaultLocalControlStreamID
	}
	if archiveCfg.ControlResponseStreamID == 0 {
		if cfg.ServiceID > math.MaxInt32-archiveResponseStreamBase {
			return archive.Config{}, errors.New(
				"service: ServiceID too large to derive archive ControlResponseStreamID",
			)
		}
		archiveCfg.ControlResponseStreamID = archiveResponseStreamBase + cfg.ServiceID
	}

	requestStreamID := archiveCfg.ControlRequestStreamID
	responseStreamID := archiveCfg.ControlResponseStreamID
	switch {
	case !strings.HasPrefix(archiveCfg.ControlRequestChannel, ipcChannelPrefix):
		return archive.Config{}, errors.New("service: archive ControlRequestChannel must be IPC")
	case !strings.HasPrefix(archiveCfg.ControlResponseChannel, ipcChannelPrefix):
		return archive.Config{}, errors.New("service: archive ControlResponseChannel must be IPC")
	case responseStreamID == requestStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and ControlRequestStreamID must differ",
		)
	case requestStreamID == cfg.ServiceStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlRequestStreamID and ServiceStreamID must differ",
		)
	case requestStreamID == cfg.ConsensusModuleStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlRequestStreamID and ConsensusModuleStreamID must differ",
		)
	case requestStreamID == cfg.SnapshotStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlRequestStreamID and SnapshotStreamID must differ",
		)
	case responseStreamID == cfg.ServiceStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and ServiceStreamID must differ",
		)
	case responseStreamID == cfg.ConsensusModuleStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and ConsensusModuleStreamID must differ",
		)
	case responseStreamID == cfg.SnapshotStreamID:
		return archive.Config{}, errors.New(
			"service: archive ControlResponseStreamID and SnapshotStreamID must differ",
		)
	}
	return archiveCfg, nil
}

// connectServiceArchive connects the archive client on transport. The
// returned client does not own transport.
//
// Returns an error when transport is not an *aeron.Client or the connect
// fails.
func connectServiceArchive(
	transport TransportClient,
	archiveCfg archive.Config,
) (*archive.Client, error) {
	client, ok := transport.(*aeron.Client)
	if !ok {
		return nil, errArchiveTransportNotClient
	}
	archiveCfg.Aeron = client
	return archive.Connect(context.Background(), archiveCfg)
}

func addedHandle[T any](handle *T, err, nilErr error) (*T, error) {
	if err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, nilErr
	}
	return handle, nil
}

func defaultConfig(cfg Config) Config {
	if cfg.ControlChannel == "" {
		cfg.ControlChannel = DefaultControlChannel
	}
	if cfg.ServiceStreamID == 0 {
		cfg.ServiceStreamID = DefaultServiceStreamID
	}
	if cfg.ConsensusModuleStreamID == 0 {
		cfg.ConsensusModuleStreamID = DefaultConsensusModuleStreamID
	}
	if cfg.SnapshotChannel == "" {
		cfg.SnapshotChannel = DefaultSnapshotChannel
	}
	if cfg.SnapshotStreamID == 0 {
		cfg.SnapshotStreamID = DefaultSnapshotStreamID
	}
	return cfg
}

func validateConfig(cfg Config) error {
	switch {
	case isNilOrTypedNil(cfg.Aeron):
		return errors.New("service: transport client is required")
	case isNilOrTypedNil(cfg.Service):
		return errors.New("service: clustered service is required")
	case cfg.CommittedPosition == nil:
		return errors.New("service: committed position is required")
	case cfg.ServiceID < 0:
		return errors.New("service: service id must not be negative")
	case cfg.StartAck != nil && cfg.StartAck.LogPosition < 0:
		return errors.New("service: start ack log position must not be negative")
	case cfg.ServiceStreamID == cfg.ConsensusModuleStreamID:
		return errors.New("service: ServiceStreamID and ConsensusModuleStreamID must differ")
	case aeron.ChannelParamIndex(cfg.SnapshotChannel, aeron.ChannelSessionIDParam, true) >= 0:
		return errors.New("service: SnapshotChannel must not set session-id=")
	case cfg.SnapshotStreamID == cfg.ServiceStreamID:
		return errors.New("service: SnapshotStreamID and ServiceStreamID must differ")
	case cfg.SnapshotStreamID == cfg.ConsensusModuleStreamID:
		return errors.New("service: SnapshotStreamID and ConsensusModuleStreamID must differ")
	}
	return nil
}

// isNilOrTypedNil reports an untyped nil or a nil pointer stored in an
// interface.
func isNilOrTypedNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer {
		return false
	}
	return rv.IsNil()
}

// Container is one assembled service host.
//
// Concurrency: invoker-only. Drive [Agent.DoWork] on the same goroutine as
// the member duty cycle.
//
// Lifetime: [Open] returns a ready container. [Container.Close] tears the
// container down.
//
// Ownership: owns the control subscription, the control publication, and the
// archive client. Does not own the transport client.
//
// Do not copy. Fields are pointers. A value copy shares the same handles.
type Container struct {
	agent         *Agent
	ingress       *SubscriptionControlIngress
	controlSub    *aeron.Subscription
	controlPub    *aeron.Publication
	archiveClient *archive.Client
}

// Agent returns the duty-cycle agent. Never nil on an open container. Stays
// non-nil after Close. The agent is inert after Close.
func (c *Container) Agent() *Agent {
	return c.agent
}

// ArchiveClient returns the archive client, or nil when Archive is nil or
// after Close.
//
// Ownership: the container owns the client. Callers must not close it.
func (c *Container) ArchiveClient() *archive.Client {
	return c.archiveClient
}

// Close closes the agent, then the archive client, then clears control
// reassembly, then closes the control subscription, then the control
// publication.
//
// Behaviour: [Agent.Close] unbinds any log image and closes any owned log
// subscription. Control ingress Clear drops reassembly builders. Control
// handles close next.
//
// Lifetime: idempotent. After Close the agent is inert. A second Close
// returns nil. Safe on a nil receiver.
//
// Ownership: the container does not own the transport client.
//
// Returns the joined close errors, or nil.
func (c *Container) Close() error {
	if c == nil {
		return nil
	}
	if c.agent != nil {
		c.agent.Close()
	}
	var err error
	if c.archiveClient != nil {
		err = c.archiveClient.Close()
		c.archiveClient = nil
	}
	if c.ingress != nil {
		c.ingress.Clear()
		c.ingress = nil
	}
	if c.controlSub != nil {
		err = errors.Join(err, c.controlSub.Close())
		c.controlSub = nil
	}
	if c.controlPub != nil {
		err = errors.Join(err, c.controlPub.Close())
		c.controlPub = nil
	}
	return err
}

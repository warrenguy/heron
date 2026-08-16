package service

import (
	"errors"
	"reflect"

	"github.com/warrenguy/heron/aeron"
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
)

var (
	errAdderReturnedNoSubscription = errors.New("service: adder returned no subscription")
	errAdderReturnedNoPublication  = errors.New("service: adder returned no publication")
)

// TransportClient registers control handles on a media driver.
//
// *aeron.Client satisfies this interface as-built.
type TransportClient interface {
	SubscriptionAdder
	AddPublication(streamID int32, channel string) (*aeron.Publication, error)
}

var _ TransportClient = (*aeron.Client)(nil)

// Config configures [Open]. Zero-value channel and stream id fields take the
// defaults stated per field. Open passes FragmentLimit and StartAck to
// [NewAgent].
type Config struct {
	// Aeron is the borrowed transport client. Required. The container does not
	// own Aeron and does not pump Aeron. Open uses Aeron for the control pair.
	// The agent uses Aeron to add the log subscription at join time.
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
	// DefaultServiceStreamID. Must differ from ConsensusModuleStreamID after
	// defaulting.
	ServiceStreamID int32

	// ConsensusModuleStreamID is the service-to-module control stream. Zero
	// takes DefaultConsensusModuleStreamID. Must differ from ServiceStreamID
	// after defaulting.
	ConsensusModuleStreamID int32

	// FragmentLimit caps fragments per log poll. Open passes FragmentLimit to
	// [NewAgent]. Zero or negative takes DefaultLogFragmentLimit there.
	FragmentLimit int

	// StartAck enables the spontaneous start ack a Java consensus module
	// consumes during recovery. Nil disables the start ack. Open passes
	// StartAck to [NewAgent]. See [StartAckConfig].
	StartAck *StartAckConfig
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
//  2. validate the config
//  3. control subscription on ServiceStreamID
//  4. control publication on ConsensusModuleStreamID
//  5. proxy bind
//  6. agent build with control ingress and proxy wired
//
// Behaviour: Open registers the two adds and returns. Open does not wait
// for a consensus module. An unavailable image frees that session's
// reassembly builder.
//
// Ownership: the container owns the control subscription and the control
// publication. The container does not own the transport client.
//
// Returns an error when:
//   - Aeron, Service, or CommittedPosition is nil
//   - ServiceID is negative
//   - StartAck is set and LogPosition is negative
//   - the two stream ids collide after defaulting
//   - either add fails or returns no handle
//
// On error nothing stays open. A failed second add closes the subscription
// first.
func Open(cfg Config) (*Container, error) {
	cfg = defaultConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
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
		}),
		ingress:    ingress,
		controlSub: sub,
		controlPub: pub,
	}, nil
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
// Ownership: owns the control subscription and the control publication. Does
// not own the transport client.
//
// Do not copy. Fields are pointers. A value copy shares the same handles.
type Container struct {
	agent      *Agent
	ingress    *SubscriptionControlIngress
	controlSub *aeron.Subscription
	controlPub *aeron.Publication
}

// Agent returns the duty-cycle agent. Never nil on an open container. Stays
// non-nil after Close. The agent is inert after Close.
func (c *Container) Agent() *Agent {
	return c.agent
}

// Close closes the agent, then clears control reassembly, then the control
// subscription, then the control publication.
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
// Returns the joined control-handle close errors, or nil.
func (c *Container) Close() error {
	if c == nil {
		return nil
	}
	if c.agent != nil {
		c.agent.Close()
	}
	if c.ingress != nil {
		c.ingress.Clear()
		c.ingress = nil
	}
	var err error
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

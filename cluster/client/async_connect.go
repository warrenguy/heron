package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

// ConnectState mirrors Java AsyncConnect.State step values.
type ConnectState int32

const (
	ConnectStateCreateEgressSubscription ConnectState = iota - 1
	ConnectStateCreateIngressPublications
	ConnectStateAwaitPublicationConnected
	ConnectStateSendMessage
	ConnectStatePollResponse
	ConnectStateConcludeConnect
	ConnectStateDone
)

var connectStateNames = [...]string{
	"CREATE_EGRESS_SUBSCRIPTION",
	"CREATE_INGRESS_PUBLICATIONS",
	"AWAIT_PUBLICATION_CONNECTED",
	"SEND_MESSAGE",
	"POLL_RESPONSE",
	"CONCLUDE_CONNECT",
	"DONE",
}

// String returns the Java AsyncConnect state name for diagnostics.
func (state ConnectState) String() string {
	idx := int(state) - int(ConnectStateCreateEgressSubscription)
	if idx < 0 || idx >= len(connectStateNames) {
		return fmt.Sprintf("ConnectState(%d)", state)
	}
	return connectStateNames[idx]
}

// AsyncConnect runs the non-blocking cluster connect handshake.
//
// Caller must invoke client.DoWork() on the transport client between every Poll()
// spin. Poll does not call DoWork internally.
//
// Not safe for concurrent use.
type AsyncConnect struct {
	cfg               Config
	client            *aeron.Client
	ownsAeronClient   bool
	newLeaderExplicit bool

	state ConnectState

	deadlineNs int64
	nanoClock  func() int64

	egressRegID        int64
	egressSubscription *aeron.Subscription
	egressPoller       *ConnectEgressPoller
	memberMap          MemberIngressMap
	ingressPublication IngressPublication
	leaderMemberID     int32
	clusterSessionID   int64
	correlationID      int64
	connectWireLength  int
	connectWireBuffer  []byte
	encodedChallenge   []byte

	cachedResponseChannel   string
	cachedResponseChannelOK bool

	closed bool
}

// NewAsyncConnect validates configuration, concludes cluster config, and starts
// the connect state machine at CREATE_EGRESS_SUBSCRIPTION.
//
// Pass a raw Config; NewAsyncConnect normalizes internally and must see
// zero fields for default detection. newLeaderExplicit is captured before
// Normalize; callers must not pre-normalize cfg or explicit NewLeaderTimeout
// detection will be wrong.
//
// ctx bounds owned transport attach only (aeron.Connect wait for CNC ready and
// driver heartbeat). Prefer Connect for a blocking handshake with cancel support.
// Poll does not accept context cancellation; use the manual loop only when
// stepping between Poll calls. If that loop must be cancellable, check
// cancelCtx.Err() between Poll calls and call Close on cancel. After each
// in-progress Poll, pace with cfg.IdleStrategy.Idle(0) and call
// IdleStrategy.Reset when the loop exits. Call Close to abort connect and release
// partial resources. ctx is not retained on AsyncConnect.
func NewAsyncConnect(
	ctx context.Context,
	cfg Config,
) (*AsyncConnect, error) {
	newLeaderExplicit := cfg.NewLeaderTimeout != 0
	cfg = cfg.Normalize()
	if err := cfg.ValidateForConnect(); err != nil {
		return nil, err
	}
	concluded, client, owns, err := concludeConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	memberMap, err := NewMemberIngressMapFromConfig(concluded)
	if err != nil {
		if owns && client != nil {
			_ = client.Close()
		}
		return nil, err
	}
	memberMap = EnsureChannelOnlyMemberIngressMap(memberMap)
	nanoClock := resolveNanoClock(concluded.AeronConfig.NanoClock)
	return &AsyncConnect{
		cfg:               concluded,
		client:            client,
		ownsAeronClient:   owns,
		newLeaderExplicit: newLeaderExplicit,
		state:             ConnectStateCreateEgressSubscription,
		deadlineNs:        nanoClock() + concluded.MessageTimeout.Nanoseconds(),
		nanoClock:         nanoClock,
		egressRegID:       conductor.NullCorrelationID,
		correlationID:     conductor.NullCorrelationID,
		memberMap:         memberMap,
	}, nil
}

// resolveNanoClock returns nanoClock when set, else a wall-clock nanosecond source.
func resolveNanoClock(nanoClock func() int64) func() int64 {
	if nanoClock != nil {
		return nanoClock
	}
	return func() int64 { return time.Now().UnixNano() }
}

// State returns the current connect state.
func (ac *AsyncConnect) State() ConnectState {
	if ac == nil {
		return ConnectStateCreateEgressSubscription
	}
	return ac.state
}

// Step returns the Java AsyncConnect step value for the current state.
func (ac *AsyncConnect) Step() int32 {
	return int32(ac.State())
}

// OwnsAeronClient reports whether Close closes the transport client.
func (ac *AsyncConnect) OwnsAeronClient() bool {
	if ac == nil {
		return false
	}
	return ac.ownsAeronClient
}

// Poll advances the connect handshake by one step.
//
// Returns (nil, nil) while in progress, (*ConnectResult, nil) on success, and
// (nil, err) on terminal failure.
func (ac *AsyncConnect) Poll() (*ConnectResult, error) {
	if ac == nil {
		return nil, errors.New("cluster client: nil async connect")
	}
	if ac.closed {
		return nil, errors.New("cluster client: async connect closed")
	}
	if ac.state == ConnectStateDone {
		return nil, errors.New("cluster client: async connect already completed")
	}
	if ac.state != ConnectStateConcludeConnect {
		if err := ac.checkDeadline(); err != nil {
			return nil, err
		}
	}

	switch ac.state {
	case ConnectStateCreateEgressSubscription:
		return ac.pollCreateEgressSubscription()
	case ConnectStateCreateIngressPublications:
		return ac.pollCreateIngressPublications()
	case ConnectStateAwaitPublicationConnected:
		return ac.pollAwaitPublicationConnected()
	case ConnectStateSendMessage:
		return ac.pollSendMessage()
	case ConnectStatePollResponse:
		return ac.pollPollResponse()
	case ConnectStateConcludeConnect:
		return ac.pollConcludeConnect()
	case ConnectStateDone:
		return nil, errors.New("cluster client: async connect already completed")
	default:
		return nil, fmt.Errorf("cluster client: invalid connect state %d", ac.state)
	}
}

// Close releases partial connect resources. Idempotent.
//
// After a successful Poll returns ConnectResult, resources move to the result and
// Close is a true no-op: closed is not set and Poll continues to report
// "already completed". Do not call Close on success paths.
func (ac *AsyncConnect) Close() error {
	if ac == nil || ac.closed {
		return nil
	}
	if ac.state == ConnectStateDone {
		return nil
	}
	ac.closed = true
	ac.zeroSensitiveBuffers()
	ac.memberMap.closeAll(ac.client)
	if ac.egressRegID != conductor.NullCorrelationID && ac.client != nil {
		ac.client.CancelPending(ac.egressRegID)
		ac.egressRegID = conductor.NullCorrelationID
	}
	if ac.egressSubscription != nil {
		_ = ac.egressSubscription.Close()
		ac.egressSubscription = nil
	}
	ac.egressPoller = nil
	ac.ingressPublication = nil
	if ac.ownsAeronClient && ac.client != nil {
		return ac.client.Close()
	}
	return nil
}

// concludeConfig finalizes egress channel defaults and resolves the
// transport client.
//
// ctx bounds owned transport attach via aeron.Connect only. Shared Aeron and
// test factories ignore ctx. Do not store ctx on Config.
func concludeConfig(
	ctx context.Context,
	cfg Config,
) (Config, *aeron.Client, bool, error) {
	var (
		err    error
		client *aeron.Client
	)
	cfg.EgressChannel, err = applyUDPRejoinFalse(cfg.EgressChannel)
	if err != nil {
		return Config{}, nil, false, err
	}

	if cfg.Aeron != nil {
		return cfg, cfg.Aeron, false, nil
	}
	aeronCfg := cfg.AeronConfig
	if cfg.AeronDir != "" {
		aeronCfg.AeronDir = cfg.AeronDir
	}
	if aeronCfg.ErrorHandler == nil {
		// Owned-client connect requires an explicit ErrorHandler; default panics on
		// driver errors to surface misconfiguration during handshake setup.
		aeronCfg.ErrorHandler = defaultClusterConnectErrorHandler
	}
	if factory, ok := ownedConnectClientFactoryForConfig(cfg); ok {
		client, err = factory(cfg)
	} else {
		client, err = aeron.Connect(ctx, aeronCfg)
	}
	if err != nil {
		return Config{}, nil, false, err
	}
	cfg.Aeron = client
	return cfg, client, true, nil
}

func defaultClusterConnectErrorHandler(err error) {
	panic(err)
}

func (ac *AsyncConnect) checkDeadline() error {
	if ac.nanoClock() <= ac.deadlineNs {
		return nil
	}
	connected := ac.egressSubscription != nil && ac.egressSubscription.IsConnected()
	ingressErr := ac.memberMap.firstPublicationErr()
	return errors.Join(
		fmt.Errorf(
			"cluster client: connect timeout in state %v egress=%s ingress=%s egressConnected=%t",
			ac.state,
			ac.cfg.EgressChannel,
			ac.cfg.IngressChannel,
			connected,
		),
		ingressErr,
	)
}

func (ac *AsyncConnect) pollCreateEgressSubscription() (*ConnectResult, error) {
	if ac.egressRegID == conductor.NullCorrelationID {
		regID, err := ac.client.OfferAddSubscription(
			ac.cfg.EgressStreamID,
			ac.cfg.EgressChannel,
		)
		if err != nil {
			return nil, err
		}
		ac.egressRegID = regID
		return nil, nil
	}
	sub, done, err := ac.pollAddEgressSubscription(ac.egressRegID)
	if err != nil {
		return nil, err
	}
	if !done {
		if ac.shouldRetryEgressSubscription() {
			if ac.client != nil && ac.egressRegID != conductor.NullCorrelationID {
				ac.client.CancelPending(ac.egressRegID)
			}
			ac.egressRegID = conductor.NullCorrelationID
		}
		return nil, nil
	}
	ac.egressRegID = conductor.NullCorrelationID
	ac.egressSubscription = sub
	ac.egressPoller = NewConnectEgressPoller(sub, DefaultConnectEgressFragmentLimit)
	ac.state = ConnectStateCreateIngressPublications
	return nil, nil
}

func (ac *AsyncConnect) pollCreateIngressPublications() (*ConnectResult, error) {
	allResolved := true
	for _, ingress := range ac.memberMap {
		if ingress == nil {
			continue
		}
		if ingress.Publication != nil || ingress.PublicationErr != nil {
			continue
		}
		if ingress.RegistrationID == conductor.NullCorrelationID {
			if err := ingress.asyncOfferAdd(ac.cfg, ac.client, ac.memberMap); err != nil {
				return nil, err
			}
		}
		done, err := ingress.asyncPollAdd(ac.cfg, ac.client)
		if err != nil {
			continue
		}
		if !done {
			allResolved = false
		}
	}
	if !allResolved {
		return nil, nil
	}
	if err := ac.memberMap.failIfIngressUnrecoverable(); err != nil {
		return nil, err
	}
	ac.state = ConnectStateAwaitPublicationConnected
	return nil, nil
}

func (ac *AsyncConnect) resolvedResponseChannel() (string, bool) {
	if ac.cachedResponseChannelOK {
		return ac.cachedResponseChannel, true
	}
	channel, ok := resolveResponseChannel(ac.egressSubscription)
	if ok {
		ac.cachedResponseChannel = channel
		ac.cachedResponseChannelOK = true
	}
	return channel, ok
}

func (ac *AsyncConnect) pollAwaitPublicationConnected() (*ConnectResult, error) {
	if ac.egressSubscription == nil {
		return nil, errors.New("cluster client: egress subscription missing")
	}
	if _, ok := ac.resolvedResponseChannel(); ok {
		if pub := ac.firstConnectedIngressPublication(); pub != nil {
			ac.ingressPublication = pub
			if err := ac.prepareConnectRequest(); err != nil {
				return nil, err
			}
			ac.state = ConnectStateSendMessage
			return nil, nil
		}
	}
	pollPendingMemberIngressRegistration(ac.memberMap, ac.cfg, ac.client)
	if err := ac.memberMap.failIfIngressUnrecoverable(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (ac *AsyncConnect) pollSendMessage() (*ConnectResult, error) {
	if ac.ingressPublication == nil {
		return nil, errors.New("cluster client: ingress publication missing")
	}
	// Java does not pre-check against publication maxMessageLength at prepare time;
	// MaxPositionExceeded / back-pressure from Offer is the practical connect ceiling.
	result := ac.ingressPublication.Offer(ac.connectWireBuffer[:ac.connectWireLength])
	switch result {
	case aeron.Closed, aeron.NotConnected:
		return nil, errors.New("cluster client: unexpected loss of connection")
	case aeron.BackPressured, aeron.AdminAction:
		// BackPressured retry is intentional; the connect wire is fixed size.
		return nil, nil
	case aeron.MaxPositionExceeded:
		return nil, fmt.Errorf(
			"cluster client: connect wire length %d exceeds ingress publication max message length",
			ac.connectWireLength,
		)
	}
	if result < 0 {
		return nil, fmt.Errorf("cluster client: ingress offer failed: status=%d", result)
	}
	ac.state = ConnectStatePollResponse
	return nil, nil
}

func (ac *AsyncConnect) pollPollResponse() (*ConnectResult, error) {
	if ac.egressPoller == nil {
		return nil, errors.New("cluster client: egress poller missing")
	}
	ac.egressPoller.Poll()
	if !ac.egressPoller.IsPollComplete() {
		return nil, nil
	}
	templateID := ac.egressPoller.TemplateID()
	if templateID == int32(clusterwire.SessionMessageHeaderSbeTemplateID) {
		return nil, nil
	}
	if templateID != int32(clusterwire.NewLeaderEventSbeTemplateID) &&
		ac.correlationID != conductor.NullCorrelationID &&
		ac.egressPoller.CorrelationID() != ac.correlationID {
		return nil, nil
	}
	if ac.egressPoller.IsChallenged() {
		ac.encodedChallenge = bytes.Clone(ac.egressPoller.EncodedChallenge())
		ac.clusterSessionID = ac.egressPoller.ClusterSessionID()
		ac.correlationID = conductor.NullCorrelationID
		if err := ac.prepareChallengeResponse(); err != nil {
			return nil, err
		}
		ac.state = ConnectStateSendMessage
		return nil, nil
	}
	switch templateID {
	case int32(clusterwire.NewLeaderEventSbeTemplateID):
		return nil, ac.updateMembers()
	case int32(clusterwire.SessionEventSbeTemplateID):
		switch ac.egressPoller.EventCode() {
		case EventCodeOK:
			ac.leaderMemberID = ac.egressPoller.LeaderMemberID()
			ac.clusterSessionID = ac.egressPoller.ClusterSessionID()
			ac.state = ConnectStateConcludeConnect
			return nil, nil
		case EventCodeRedirect:
			return nil, ac.updateMembers()
		case EventCodeAuthenticationRejected:
			return nil, ac.clusterExceptionFromDetail("authentication rejected")
		case EventCodeError:
			return nil, ac.clusterExceptionFromDetail("connect error")
		case EventCodeClosed, EventCodeNullValue:
			return nil, nil
		default:
			return nil, ac.clusterExceptionFromDetail(
				fmt.Sprintf("unexpected session event code %d", ac.egressPoller.EventCode()),
			)
		}
	default:
		return nil, nil
	}
}

func (ac *AsyncConnect) pollConcludeConnect() (*ConnectResult, error) {
	result, err := ac.concludeConnect()
	if err != nil {
		return nil, err
	}
	ac.transferConnectResourcesToResult()
	ac.zeroSensitiveBuffers()
	ac.state = ConnectStateDone
	return result, nil
}

func (ac *AsyncConnect) transferConnectResourcesToResult() {
	ac.egressRegID = conductor.NullCorrelationID
	ac.egressSubscription = nil
	ac.egressPoller = nil
	ac.ingressPublication = nil
	ac.memberMap = nil
	ac.client = nil
	ac.ownsAeronClient = false
}

func (ac *AsyncConnect) firstConnectedIngressPublication() IngressPublication {
	var best IngressPublication
	var bestID int32
	for memberID, ingress := range ac.memberMap {
		pub := ingress.connectedPublication()
		if pub == nil {
			continue
		}
		if best == nil || memberID < bestID {
			best, bestID = pub, memberID
		}
	}
	return best
}

// updateMembers rebuilds ingress after NewLeader/REDIRECT. Clears ingressPublication
// before apply when the leader pub is not reused (connect-specific).
func (ac *AsyncConnect) updateMembers() error {
	ac.leaderMemberID = ac.egressPoller.LeaderMemberID()
	plan, err := planMemberIngressMapRebuild(
		ac.memberMap,
		ac.leaderMemberID,
		ac.egressPoller.Detail(),
	)
	if err != nil {
		return err
	}
	if plan.reusedPub == nil {
		ac.ingressPublication = nil
	}
	newMap, leaderPub, err := applyMemberIngressMapRebuild(
		ac.client,
		ac.cfg,
		ac.memberMap,
		plan,
	)
	ac.memberMap = newMap
	if err != nil {
		return err
	}
	ac.ingressPublication = leaderPub
	ac.correlationID = conductor.NullCorrelationID
	ac.state = ConnectStateAwaitPublicationConnected
	return nil
}

func (ac *AsyncConnect) ensureCorrelationID() {
	if ac.correlationID == conductor.NullCorrelationID {
		ac.correlationID = ac.client.NextCorrelationID()
	}
}

func (ac *AsyncConnect) zeroSensitiveBuffers() {
	clear(ac.connectWireBuffer[:cap(ac.connectWireBuffer)])
	ac.connectWireBuffer = nil
	ac.connectWireLength = 0
	ac.encodedChallenge = nil
}

func (ac *AsyncConnect) clusterExceptionFromDetail(prefix string) error {
	detail := ac.egressPoller.Detail()
	if len(detail) == 0 {
		return fmt.Errorf("cluster client: %s", prefix)
	}
	return fmt.Errorf("cluster client: %s: %s", prefix, strings.TrimSpace(string(detail)))
}

func (ac *AsyncConnect) concludeConnect() (*ConnectResult, error) {
	cfg := ac.cfg
	leaderHeartbeatTimeoutNs := ac.egressPoller.LeaderHeartbeatTimeoutNs()
	if leaderHeartbeatTimeoutNs <= 0 || leaderHeartbeatTimeoutNs == math.MinInt64 {
		leaderHeartbeatTimeoutNs = 0
	}
	if leaderHeartbeatTimeoutNs > 0 && !ac.newLeaderExplicit {
		cfg.NewLeaderTimeout = 2 * time.Duration(leaderHeartbeatTimeoutNs)
	}
	channelOnly := ac.memberMap.IsChannelOnly()
	if !channelOnly {
		if _, ok := ac.memberMap.Lookup(ac.leaderMemberID); !ok {
			return nil, fmt.Errorf(
				"cluster client: leader member id %d missing from connect ingress map",
				ac.leaderMemberID,
			)
		}
	}
	for memberID, ingress := range ac.memberMap {
		if ingress == nil {
			continue
		}
		if channelOnly {
			ac.ingressPublication = ingress.Publication
			continue
		}
		if memberID == ac.leaderMemberID {
			continue
		}
		ingress.closePublication(ac.client)
	}
	if !channelOnly {
		if leaderIngress, ok := ac.memberMap.Lookup(ac.leaderMemberID); ok {
			ac.ingressPublication = leaderIngress.Publication
		}
	}
	result := &ConnectResult{
		ClusterSessionID:         ac.clusterSessionID,
		LeadershipTermID:         ac.egressPoller.LeadershipTermID(),
		LeaderMemberID:           ac.leaderMemberID,
		LeaderHeartbeatTimeoutNs: leaderHeartbeatTimeoutNs,
		IngressPublication:       ac.ingressPublication,
		EgressSubscription:       ac.egressSubscription,
		EgressImage:              ac.egressPoller.EgressImage(),
		MemberIngressMap:         ac.memberMap,
		Aeron:                    ac.client,
		OwnsAeronClient:          ac.ownsAeronClient,
		Config:                   cfg,
	}
	return result, nil
}

package client

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

// ConnectState mirrors Java AsyncConnect.State step values.
type ConnectState int

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
	ctx               ClusterContext
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

	closed bool
}

// NewAsyncConnect validates configuration, concludes cluster context, and starts
// the connect state machine at CREATE_EGRESS_SUBSCRIPTION.
//
// newLeaderExplicit is captured before Normalize; callers must not pre-normalize ctx
// or explicit NewLeaderTimeout detection will be wrong.
func NewAsyncConnect(ctx ClusterContext) (*AsyncConnect, error) {
	newLeaderExplicit := ctx.NewLeaderTimeout != 0
	ctx = ctx.Normalize()
	if err := ctx.ValidateForConnect(); err != nil {
		return nil, err
	}
	concluded, client, owns, err := concludeClusterContext(ctx)
	if err != nil {
		return nil, err
	}
	memberMap, err := NewMemberIngressMapFromContext(concluded)
	if err != nil {
		if owns && client != nil {
			_ = client.Close()
			notifyOwnedClientClosedOnMemberMapError(client)
		}
		return nil, err
	}
	memberMap = EnsureChannelOnlyMemberIngressMap(memberMap)
	nanoClock := concluded.AeronContext.NanoClock
	if nanoClock == nil {
		nanoClock = func() int64 { return time.Now().UnixNano() }
	}
	return &AsyncConnect{
		ctx:               concluded,
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
	ac.releaseMemberResources()
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

func concludeClusterContext(ctx ClusterContext) (ClusterContext, *aeron.Client, bool, error) {
	if ctx.Aeron != nil {
		return ctx, ctx.Aeron, false, nil
	}
	aeronCtx := ctx.AeronContext
	if ctx.AeronDir != "" {
		aeronCtx.AeronDir = ctx.AeronDir
	}
	if aeronCtx.ErrorHandler == nil {
		// Owned-client connect requires an explicit ErrorHandler; default panics on
		// driver errors to surface misconfiguration during handshake setup.
		aeronCtx.ErrorHandler = defaultClusterConnectErrorHandler
	}
	var (
		client *aeron.Client
		err    error
	)
	if newOwnedConnectClientHook != nil {
		client, err = newOwnedConnectClientHook(ctx)
	} else {
		client, err = aeron.Connect(aeronCtx)
	}
	if err != nil {
		return ClusterContext{}, nil, false, err
	}
	ctx.Aeron = client
	return ctx, client, true, nil
}

func defaultClusterConnectErrorHandler(err error) {
	panic(err)
}

func (ac *AsyncConnect) checkDeadline() error {
	if ac.nanoClock() <= ac.deadlineNs {
		return nil
	}
	connected := ac.egressSubscription != nil && ac.egressSubscription.IsConnected()
	return errors.Join(
		fmt.Errorf(
			"cluster client: connect timeout in state %v egress=%s ingress=%s egressConnected=%t",
			ac.state,
			ac.ctx.EgressChannel,
			ac.ctx.IngressChannel,
			connected,
		),
		ac.firstIngressPublicationErr(),
	)
}

func (ac *AsyncConnect) pollCreateEgressSubscription() (*ConnectResult, error) {
	if ac.egressRegID == conductor.NullCorrelationID {
		regID, err := ac.client.OfferAddSubscription(
			ac.ctx.EgressStreamID,
			ac.ctx.EgressChannel,
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
			if err := ingress.asyncOfferAdd(ac.ctx, ac.client, ac.memberMap); err != nil {
				return nil, err
			}
		}
		done, err := ingress.asyncPollAdd(ac.ctx, ac.client)
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
	if err := ac.failIfIngressUnrecoverable(); err != nil {
		return nil, err
	}
	ac.state = ConnectStateAwaitPublicationConnected
	return nil, nil
}

func (ac *AsyncConnect) pollAwaitPublicationConnected() (*ConnectResult, error) {
	if ac.egressSubscription == nil {
		return nil, errors.New("cluster client: egress subscription missing")
	}
	if _, ok := resolveResponseChannel(ac.egressSubscription); ok {
		if pub := ac.firstConnectedIngressPublication(); pub != nil {
			ac.ingressPublication = pub
			if err := ac.prepareConnectRequest(); err != nil {
				return nil, err
			}
			ac.state = ConnectStateSendMessage
			return nil, nil
		}
	}
	ac.pollPendingMemberIngress()
	if err := ac.failIfIngressUnrecoverable(); err != nil {
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
	if templateID == int32(gensbe.SessionMessageHeaderSbeTemplateID) {
		return nil, nil
	}
	if templateID != int32(gensbe.NewLeaderEventSbeTemplateID) &&
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
	case int32(gensbe.NewLeaderEventSbeTemplateID):
		return nil, ac.updateMembers()
	case int32(gensbe.SessionEventSbeTemplateID):
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

func (ac *AsyncConnect) firstIngressPublicationErr() error {
	for _, ingress := range ac.memberMap {
		if ingress == nil || ingress.PublicationErr == nil {
			continue
		}
		return ingress.PublicationErr
	}
	return nil
}

func (ac *AsyncConnect) failIfIngressUnrecoverable() error {
	var err error
	for _, ingress := range ac.memberMap {
		if ingress == nil {
			continue
		}
		if ingress.Publication != nil || ingress.RegistrationID != conductor.NullCorrelationID {
			return nil
		}
		if err == nil && ingress.PublicationErr != nil {
			err = ingress.PublicationErr
		}
	}
	return err
}

func (ac *AsyncConnect) firstConnectedIngressPublication() IngressPublication {
	var best IngressPublication
	var bestID int32
	for memberID, ingress := range ac.memberMap {
		if pub := connectedPublicationOf(ingress); pub != nil && (best == nil || memberID < bestID) {
			best, bestID = pub, memberID
		}
	}
	return best
}

func connectedPublicationOf(ingress *MemberIngress) IngressPublication {
	if ingress == nil || ingress.Publication == nil || !ingress.Publication.IsConnected() {
		return nil
	}
	return ingress.Publication
}

func (ac *AsyncConnect) updateMembers() error {
	ac.leaderMemberID = ac.egressPoller.LeaderMemberID()
	oldLeader, hadOldLeader := ac.memberMap.Lookup(ac.leaderMemberID)
	var oldEndpoint string
	var oldPublication IngressPublication
	var oldPublicationErr error
	if hadOldLeader {
		oldEndpoint = oldLeader.Endpoint
		oldPublication = oldLeader.Publication
		oldPublicationErr = oldLeader.PublicationErr
	}
	newMap, err := ParseIngressEndpointsBytes(ac.egressPoller.Detail())
	if err != nil {
		return err
	}
	reusePublication := false
	if newLeader, ok := newMap.Lookup(ac.leaderMemberID); ok {
		reusePublication = oldPublication != nil &&
			oldPublicationErr == nil &&
			newLeader.Endpoint == oldEndpoint
	}
	if !reusePublication {
		ac.ingressPublication = nil
	}
	for memberID, ingress := range ac.memberMap {
		if ingress == nil {
			continue
		}
		if reusePublication && memberID == ac.leaderMemberID {
			ingress.RegistrationID = conductor.NullCorrelationID
			ingress.Publication = nil
			ingress.PublicationErr = nil
			continue
		}
		ingress.closePublication(ac.client)
	}
	ac.memberMap = newMap
	newLeader, ok := ac.memberMap.Lookup(ac.leaderMemberID)
	if !ok {
		return fmt.Errorf(
			"cluster client: leader member id %d missing from redirect ingress map",
			ac.leaderMemberID,
		)
	}
	if reusePublication {
		newLeader.Publication = oldPublication
		ac.ingressPublication = oldPublication
	} else if err := newLeader.asyncOfferAdd(ac.ctx, ac.client, ac.memberMap); err != nil {
		return err
	}
	ac.correlationID = conductor.NullCorrelationID
	ac.state = ConnectStateAwaitPublicationConnected
	return nil
}

func (ac *AsyncConnect) pollPendingMemberIngress() {
	for _, ingress := range ac.memberMap {
		if ingress == nil || ingress.RegistrationID == conductor.NullCorrelationID {
			continue
		}
		_, _ = ingress.asyncPollAdd(ac.ctx, ac.client)
	}
}

func (ac *AsyncConnect) ensureCorrelationID() {
	if ac.correlationID == conductor.NullCorrelationID {
		ac.correlationID = ac.client.NextCorrelationID()
	}
}

func (ac *AsyncConnect) releaseMemberResources() {
	for _, ingress := range ac.memberMap {
		if ingress == nil {
			continue
		}
		ingress.closePublication(ac.client)
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
	ctx := ac.ctx
	leaderHeartbeatTimeoutNs := ac.egressPoller.LeaderHeartbeatTimeoutNs()
	if leaderHeartbeatTimeoutNs <= 0 || leaderHeartbeatTimeoutNs == math.MinInt64 {
		leaderHeartbeatTimeoutNs = 0
	}
	if leaderHeartbeatTimeoutNs > 0 && !ac.newLeaderExplicit {
		ctx.NewLeaderTimeout = 2 * time.Duration(leaderHeartbeatTimeoutNs)
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
		Context:                  ctx,
	}
	return result, nil
}

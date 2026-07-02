package client

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

// ConnectState mirrors Java AeronArchive.AsyncConnect step values.
type ConnectState int

const (
	ConnectStateCreateControlResponseSubscription ConnectState = iota - 1
	ConnectStateCreateControlRequestPublication
	ConnectStateAwaitPublicationConnected
	ConnectStateSendMessage
	ConnectStatePollResponse
	ConnectStateConcludeConnect
	ConnectStateDone
)

var connectStateNames = [...]string{
	"CREATE_CONTROL_RESPONSE_SUBSCRIPTION",
	"CREATE_CONTROL_REQUEST_PUBLICATION",
	"AWAIT_PUBLICATION_CONNECTED",
	"SEND_MESSAGE",
	"POLL_RESPONSE",
	"CONCLUDE_CONNECT",
	"DONE",
}

// String returns the Java AsyncConnect state name for diagnostics.
func (state ConnectState) String() string {
	idx := int(state) - int(ConnectStateCreateControlResponseSubscription)
	if idx < 0 || idx >= len(connectStateNames) {
		return fmt.Sprintf("ConnectState(%d)", state)
	}
	return connectStateNames[idx]
}

type connectTransport struct {
	responseRegID           int64
	responseSubscription    *aeron.Subscription
	responsePoller          *ControlResponsePoller
	requestRegID            int64
	requestPublication      ControlPublication
	cachedResponseChannel   string
	cachedResponseChannelOK bool
}

type connectSession struct {
	controlSessionID int64
	archiveID        int64
	serverVersion    int32
	correlationID    int64
}

type connectWireState struct {
	length           int
	buffer           []byte
	encodedChallenge []byte
}

// AsyncConnect runs the non-blocking archive connect handshake.
//
// Caller must invoke client.DoWork() on the transport client between every Poll()
// spin. Poll does not call DoWork internally.
//
// Not safe for concurrent use.
type AsyncConnect struct {
	ctx             ArchiveContext
	client          *aeron.Client
	ownsAeronClient bool

	state ConnectState

	deadlineNs int64
	nanoClock  func() int64

	transport connectTransport
	session   connectSession
	wire      connectWireState

	closed bool
}

// NewAsyncConnect validates configuration, concludes archive context, and starts
// the connect state machine at CREATE_CONTROL_RESPONSE_SUBSCRIPTION.
func NewAsyncConnect(ctx ArchiveContext) (*AsyncConnect, error) {
	ctx = ctx.Normalize()
	if err := ctx.ValidateForConnect(); err != nil {
		return nil, err
	}
	concluded, client, owns, err := concludeArchiveContext(ctx)
	if err != nil {
		return nil, err
	}
	nanoClock := resolveNanoClock(concluded.AeronContext.NanoClock)
	return &AsyncConnect{
		ctx:             concluded,
		client:          client,
		ownsAeronClient: owns,
		state:           ConnectStateCreateControlResponseSubscription,
		deadlineNs:      nanoClock() + concluded.ConnectTimeout.Nanoseconds(),
		nanoClock:       nanoClock,
		session: connectSession{
			correlationID: conductor.NullCorrelationID,
		},
		transport: connectTransport{
			responseRegID: conductor.NullCorrelationID,
			requestRegID:  conductor.NullCorrelationID,
		},
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
		return ConnectStateCreateControlResponseSubscription
	}
	return ac.state
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
		return nil, errors.New("archive client: nil async connect")
	}
	if ac.closed {
		return nil, errors.New("archive client: async connect closed")
	}
	if ac.state == ConnectStateDone {
		return nil, errors.New("archive client: async connect already completed")
	}
	if ac.state != ConnectStateConcludeConnect {
		if err := ac.checkDeadline(); err != nil {
			return nil, err
		}
	}

	switch ac.state {
	case ConnectStateCreateControlResponseSubscription:
		return ac.pollCreateControlResponseSubscription()
	case ConnectStateCreateControlRequestPublication:
		return ac.pollCreateControlRequestPublication()
	case ConnectStateAwaitPublicationConnected:
		return ac.pollAwaitPublicationConnected()
	case ConnectStateSendMessage:
		return ac.pollSendMessage()
	case ConnectStatePollResponse:
		return ac.pollPollResponse()
	case ConnectStateConcludeConnect:
		return ac.pollConcludeConnect()
	default:
		return nil, fmt.Errorf("archive client: invalid connect state %d", ac.state)
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
	ac.cancelPendingRegistration(&ac.transport.responseRegID)
	ac.cancelPendingRegistration(&ac.transport.requestRegID)
	if ac.transport.responseSubscription != nil {
		_ = ac.transport.responseSubscription.Close()
		ac.transport.responseSubscription = nil
	}
	ac.transport.responsePoller = nil
	if ac.transport.requestPublication != nil {
		_ = ac.transport.requestPublication.Close()
		ac.transport.requestPublication = nil
	}
	if ac.ownsAeronClient && ac.client != nil {
		return ac.client.Close()
	}
	return nil
}

func (ac *AsyncConnect) checkDeadline() error {
	if ac.nanoClock() <= ac.deadlineNs {
		return nil
	}
	connected := ac.transport.responseSubscription != nil &&
		ac.transport.responseSubscription.IsConnected()
	return fmt.Errorf(
		"archive client: connect timeout in state %v request=%s response=%s responseConnected=%t",
		ac.state,
		ac.ctx.EffectiveControlRequestChannel(),
		ac.ctx.EffectiveControlResponseChannel(),
		connected,
	)
}

func pollAsyncRegistration[T any](
	regID *int64,
	offer func() (int64, error),
	poll func(int64) (T, bool, error),
	onNotDone func(),
	onDone func(T),
) error {
	if *regID == conductor.NullCorrelationID {
		nextRegID, err := offer()
		if err != nil {
			return err
		}
		*regID = nextRegID
		return nil
	}
	resource, done, err := poll(*regID)
	if err != nil {
		return err
	}
	if !done {
		if onNotDone != nil {
			onNotDone()
		}
		return nil
	}
	*regID = conductor.NullCorrelationID
	onDone(resource)
	return nil
}

func (ac *AsyncConnect) pollCreateControlResponseSubscription() (*ConnectResult, error) {
	err := pollAsyncRegistration(
		&ac.transport.responseRegID,
		func() (int64, error) {
			return ac.client.OfferAddSubscription(
				ac.ctx.EffectiveControlResponseStreamID(),
				ac.ctx.EffectiveControlResponseChannel(),
			)
		},
		ac.pollAddControlResponseSubscription,
		func() {
			if ac.shouldRetryControlResponseSubscription() {
				ac.cancelPendingRegistration(&ac.transport.responseRegID)
			}
		},
		func(sub *aeron.Subscription) {
			ac.transport.responseSubscription = sub
			ac.transport.responsePoller = NewControlResponsePoller(
				sub,
				DefaultConnectControlResponseFragmentLimit,
			)
			ac.state = ConnectStateCreateControlRequestPublication
		},
	)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (ac *AsyncConnect) pollCreateControlRequestPublication() (*ConnectResult, error) {
	err := pollAsyncRegistration(
		&ac.transport.requestRegID,
		func() (int64, error) {
			return ac.client.OfferAddExclusivePublication(
				ac.ctx.EffectiveControlRequestStreamID(),
				ac.ctx.EffectiveControlRequestChannel(),
			)
		},
		ac.pollAddControlRequestPublication,
		nil,
		func(pub ControlPublication) {
			ac.transport.requestPublication = pub
			ac.state = ConnectStateAwaitPublicationConnected
		},
	)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (ac *AsyncConnect) resolvedResponseChannel() (string, bool) {
	if ac.transport.cachedResponseChannelOK {
		return ac.transport.cachedResponseChannel, true
	}
	if ac.transport.responseSubscription == nil {
		return "", false
	}
	channel, ok := resolveResponseChannel(ac.transport.responseSubscription)
	if ok {
		ac.transport.cachedResponseChannel = channel
		ac.transport.cachedResponseChannelOK = true
	}
	return channel, ok
}

func (ac *AsyncConnect) pollAwaitPublicationConnected() (*ConnectResult, error) {
	if ac.transport.responseSubscription == nil {
		return nil, errors.New("archive client: control response subscription missing")
	}
	if _, ok := ac.resolvedResponseChannel(); !ok {
		return nil, nil
	}
	if ac.transport.requestPublication == nil || !ac.transport.requestPublication.IsConnected() {
		return nil, nil
	}
	if err := ac.prepareConnectRequest(); err != nil {
		return nil, err
	}
	ac.state = ConnectStateSendMessage
	return nil, nil
}

func (ac *AsyncConnect) pollSendMessage() (*ConnectResult, error) {
	pub := ac.transport.requestPublication
	if pub == nil {
		return nil, errors.New("archive client: control publication missing")
	}
	result := pub.Offer(ac.wire.buffer[:ac.wire.length])
	switch result {
	case aeron.Closed, aeron.NotConnected:
		return nil, errors.New("archive client: unexpected loss of connection")
	case aeron.BackPressured, aeron.AdminAction:
		return nil, nil
	case aeron.UnsupportedMessageLength:
		return nil, fmt.Errorf(
			"archive client: connect wire length %d exceeds control publication max payload length %d; "+
				"exclusive connect does not fragment",
			ac.wire.length,
			pub.MaxPayloadLength(),
		)
	case aeron.MaxPositionExceeded:
		maxMessageLength := pub.MaxMessageLength()
		if ac.wire.length > maxMessageLength {
			return nil, fmt.Errorf(
				"archive client: connect wire length %d exceeds control publication max message length %d",
				ac.wire.length,
				maxMessageLength,
			)
		}
		return nil, errors.New("archive client: control publication stream position exceeded")
	}
	if result < 0 {
		return nil, fmt.Errorf("archive client: control offer failed: status=%d", result)
	}
	ac.state = ConnectStatePollResponse
	return nil, nil
}

func (ac *AsyncConnect) pollPollResponse() (*ConnectResult, error) {
	poller := ac.transport.responsePoller
	if poller == nil {
		return nil, errors.New("archive client: control response poller missing")
	}
	poller.Poll()
	if !poller.IsPollComplete() {
		return nil, nil
	}
	if ac.connectPollResponseIgnorable(poller) {
		return nil, nil
	}
	if poller.IsChallenged() {
		return ac.handleConnectChallenge(poller)
	}
	return ac.handleConnectControlResponse(poller)
}

func (ac *AsyncConnect) connectPollResponseIgnorable(poller *ControlResponsePoller) bool {
	capture, ok := poller.Capture()
	if !ok {
		return true
	}
	correlationID := ac.session.correlationID
	if correlationID != conductor.NullCorrelationID &&
		capture.CorrelationID != correlationID {
		return true
	}
	if capture.Challenged() {
		return false
	}
	return capture.TemplateID != int32(gensbe.ControlResponseSbeTemplateID)
}

func (ac *AsyncConnect) handleConnectChallenge(
	poller *ControlResponsePoller,
) (*ConnectResult, error) {
	capture, _ := poller.Capture()
	ac.wire.encodedChallenge = bytes.Clone(capture.EncodedChallenge)
	ac.session.controlSessionID = capture.ControlSessionID
	ac.session.correlationID = conductor.NullCorrelationID
	if err := ac.prepareChallengeResponse(); err != nil {
		return nil, err
	}
	ac.state = ConnectStateSendMessage
	return nil, nil
}

func (ac *AsyncConnect) handleConnectControlResponse(
	poller *ControlResponsePoller,
) (*ConnectResult, error) {
	resp, ok := poller.Response()
	if !ok {
		return nil, errors.New("archive client: connect response missing")
	}
	switch resp.Code {
	case gensbe.ControlResponseCode_OK:
		ac.session.controlSessionID = resp.ControlSessionID
		ac.session.archiveID = resp.RelevantID
		ac.session.serverVersion = resp.Version
		ac.state = ConnectStateConcludeConnect
		return nil, nil
	default:
		return nil, MapControlResponse(resp)
	}
}

func (ac *AsyncConnect) pollConcludeConnect() (*ConnectResult, error) {
	result := &ConnectResult{
		ControlSessionID:    ac.session.controlSessionID,
		ArchiveID:           ac.session.archiveID,
		ServerVersion:       ac.session.serverVersion,
		ControlPublication:  ac.transport.requestPublication,
		ControlSubscription: ac.transport.responseSubscription,
		Proxy:               NewArchiveProxy(ac.transport.requestPublication, ac.ctx),
		Poller:              ac.transport.responsePoller,
		Aeron:               ac.client,
		OwnsAeronClient:     ac.ownsAeronClient,
		Context:             ac.ctx,
	}
	ac.transferConnectResourcesToResult()
	ac.zeroSensitiveBuffers()
	ac.state = ConnectStateDone
	return result, nil
}

func (ac *AsyncConnect) transferConnectResourcesToResult() {
	ac.transport.responseRegID = conductor.NullCorrelationID
	ac.transport.requestRegID = conductor.NullCorrelationID
	ac.transport.responseSubscription = nil
	ac.transport.responsePoller = nil
	ac.transport.requestPublication = nil
	ac.client = nil
	ac.ownsAeronClient = false
}

func (ac *AsyncConnect) cancelPendingRegistration(regID *int64) {
	if *regID == conductor.NullCorrelationID || ac.client == nil {
		return
	}
	ac.client.CancelPending(*regID)
	*regID = conductor.NullCorrelationID
}

func (ac *AsyncConnect) ensureCorrelationID() {
	if ac.session.correlationID == conductor.NullCorrelationID {
		ac.session.correlationID = ac.client.NextCorrelationID()
	}
}

func (ac *AsyncConnect) zeroSensitiveBuffers() {
	clear(ac.wire.buffer[:cap(ac.wire.buffer)])
	ac.wire.buffer = nil
	ac.wire.length = 0
	ac.wire.encodedChallenge = nil
}

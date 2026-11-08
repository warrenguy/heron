//go:build testhooks

package service

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/aeron/logbuffer"
	"luas.fast/archive"
	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
)

// recordingService records ClusteredService callbacks for agent unit tests.
type recordingService struct {
	startCount int
	startErr   error

	opens    []sessionOpenRec
	closes   []sessionCloseRec
	messages []sessionMessageRec
	timers   []timerRec
	order    []string
}

type sessionOpenRec struct {
	session   ClientSession
	timestamp int64
}

type sessionCloseRec struct {
	session   ClientSession
	timestamp int64
	reason    clusterwire.CloseReason
}

type sessionMessageRec struct {
	session   ClientSession
	timestamp int64
	payload   []byte
}

type timerRec struct {
	correlationID int64
	timestamp     int64
}

func (s *recordingService) Start(Cluster) error {
	s.startCount++
	return s.startErr
}

func (s *recordingService) SessionOpen(session ClientSession, timestamp int64) {
	s.opens = append(s.opens, sessionOpenRec{session: session, timestamp: timestamp})
	s.order = append(s.order, "open")
}

func (s *recordingService) SessionClose(
	session ClientSession,
	timestamp int64,
	reason clusterwire.CloseReason,
) {
	s.closes = append(s.closes, sessionCloseRec{
		session:   session,
		timestamp: timestamp,
		reason:    reason,
	})
	s.order = append(s.order, "close")
}

func (s *recordingService) SessionMessage(
	session ClientSession,
	timestamp int64,
	payload []byte,
) {
	s.messages = append(s.messages, sessionMessageRec{
		session:   session,
		timestamp: timestamp,
		payload:   append([]byte(nil), payload...),
	})
	s.order = append(s.order, "message")
}

func (s *recordingService) TimerEvent(correlationID, timestamp int64) {
	s.timers = append(s.timers, timerRec{
		correlationID: correlationID,
		timestamp:     timestamp,
	})
	s.order = append(s.order, "timer")
}

// nltRecordingService adds NewLeadershipTermEventHandler to recordingService.
type nltRecordingService struct {
	recordingService
	nlt []nltRec
}

type nltRec struct {
	leadershipTermID    int64
	logPosition         int64
	timestamp           int64
	termBaseLogPosition int64
	leaderMemberID      int32
	logSessionID        int32
	timeUnit            clusterwire.ClusterTimeUnit
	appVersion          int32
}

func (s *nltRecordingService) NewLeadershipTermEvent(
	leadershipTermID int64,
	logPosition int64,
	timestamp int64,
	termBaseLogPosition int64,
	leaderMemberID int32,
	logSessionID int32,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) {
	s.nlt = append(s.nlt, nltRec{
		leadershipTermID:    leadershipTermID,
		logPosition:         logPosition,
		timestamp:           timestamp,
		termBaseLogPosition: termBaseLogPosition,
		leaderMemberID:      leaderMemberID,
		logSessionID:        logSessionID,
		timeUnit:            timeUnit,
		appVersion:          appVersion,
	})
	s.order = append(s.order, "nlt")
}

// snapshotRecordingService adds TakeSnapshotHandler to recordingService.
// Each TakeSnapshot call pops the first hookErrs entry. An empty queue falls
// back to hookErr.
type snapshotRecordingService struct {
	recordingService
	pubs             []*aeron.ExclusivePublication
	hookErrs         []error
	hookErr          error
	write            []byte
	closePublication bool
}

func (s *snapshotRecordingService) TakeSnapshot(
	publication *aeron.ExclusivePublication,
) error {
	s.pubs = append(s.pubs, publication)
	s.order = append(s.order, "snapshot")
	err := s.hookErr
	if len(s.hookErrs) > 0 {
		err = s.hookErrs[0]
		s.hookErrs = s.hookErrs[1:]
	}
	if err != nil {
		return err
	}
	if s.write != nil {
		if publication == nil {
			return errors.New("nil publication")
		}
		if pos := publication.Offer(s.write); pos <= 0 {
			return fmt.Errorf("offer failed: %d", pos)
		}
	}
	if s.closePublication {
		return publication.Close()
	}
	return nil
}

// stubAdder returns a fixed subscription (or error) from AddSubscription.
type stubAdder struct {
	sub   *aeron.Subscription
	err   error
	calls int
}

func (s *stubAdder) AddSubscription(int32, string) (*aeron.Subscription, error) {
	s.calls++

	if s.err != nil {
		return nil, s.err
	}
	return s.sub, nil
}

const (
	testLogChannel   = "aeron:ipc"
	testLogStreamID  = int32(100)
	testLogSessionID = int32(42)
	testMemberID     = int32(7)
)

func defaultJoinEvent(logPosition int64) logJoinEvent {
	return logJoinEvent{
		logPosition:    logPosition,
		maxLogPosition: math.MaxInt64,
		memberID:       testMemberID,
		logSessionID:   testLogSessionID,
		logStreamID:    testLogStreamID,
		isStartup:      true,
		role:           codecs.RoleLeader,
		logChannel:     testLogChannel,
	}
}

// newLogImage builds a subscription with one image on testLogChannel.
//
// Registration ids are fixed. Each call builds an independent subscription and
// image; no assertion reads either id.
func newLogImage(
	tb testing.TB,
	sessionID, streamID int32,
	joinPosition int64,
) (*aeron.Subscription, *aeron.Image) {
	tb.Helper()
	const (
		subRegistrationID   int64 = 9001
		imageRegistrationID int64 = 9101
	)
	sub := aeron.NewSubscriptionForHooks(subRegistrationID, streamID, testLogChannel)
	image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		tb,
		sub,
		sessionID,
		imageRegistrationID,
		joinPosition,
	)
	require.True(tb, aeron.SubscriptionAddImageForHooks(sub, image))
	return sub, image
}

// bindAgent joins and binds an image at logPosition with the test session.
//
// Returns the agent, bound image, and committed-position variable.
func bindAgent(
	tb testing.TB,
	svc ClusteredService,
	joinPosition int64,
) (*Agent, *aeron.Image, *int64) {
	tb.Helper()

	sub, image := newLogImage(tb, testLogSessionID, testLogStreamID, joinPosition)

	committed := joinPosition
	agent := NewAgent(AgentConfig{
		Service:           svc,
		CommittedPosition: func() int64 { return committed },
		Subscriptions:     &stubAdder{sub: sub},
	})
	agent.handleJoinLog(defaultJoinEvent(joinPosition))
	require.Equal(tb, 1, agent.PendingCoordinationAcks())
	require.Equal(tb, 0, agent.ReadyCoordinationAcks())
	require.True(tb, agent.HasPendingJoin())

	work := agent.DoWork(0)
	require.NoError(tb, agent.ServiceFailure())
	// Start (1) + bind (1)
	require.GreaterOrEqual(tb, work, 2)
	require.True(tb, agent.IsLogBound())
	require.False(tb, agent.HasPendingJoin())
	require.Equal(tb, 1, agent.ReadyCoordinationAcks())
	require.Equal(tb, testMemberID, agent.MemberID())
	require.Equal(tb, codecs.RoleLeader, agent.Role())
	require.Equal(tb, joinPosition, agent.LogPosition())

	return agent, image, &committed
}

// bindImage binds cfg onto a hooks image at joinPosition without a JoinLog
// entry. Action-hook tests use this so the action is the first queued ack.
func bindImage(
	tb testing.TB,
	cfg AgentConfig,
	joinPosition int64,
) (*Agent, *aeron.Image, *int64) {
	tb.Helper()
	sub, image := newLogImage(tb, testLogSessionID, testLogStreamID, joinPosition)
	committed := joinPosition
	cfg.CommittedPosition = func() int64 { return committed }
	if cfg.Subscriptions == nil {
		cfg.Subscriptions = &stubAdder{sub: sub}
	}
	agent := NewAgent(cfg)
	agent.adapter.SetImage(image)
	return agent, image, &committed
}

func writeAgentFrame(
	tb assertTB,
	image *aeron.Image,
	frameOffset int,
	payload []byte,
) int {
	tb.Helper()
	writeUnfragmentedFrame(tb, image, frameOffset, payload)
	return logbuffer.AlignFragmentChunkLength(len(payload))
}

func writeAgentPadding(
	tb assertTB,
	image *aeron.Image,
	frameOffset int,
	paddingLength int32,
) {
	tb.Helper()
	aeron.WriteCommittedPaddingFrameForHooks(
		aeron.ImageTermBufferForHooks(image),
		frameOffset,
		hookTermID,
		int32(frameOffset),
		paddingLength,
	)
}

// replaceLogSubscription swaps the stub adder for a new image at joinPosition.
// Returns that image.
func replaceLogSubscription(
	tb testing.TB,
	agent *Agent,
	sessionID, streamID int32,
	joinPosition int64,
) *aeron.Image {
	tb.Helper()
	sub, image := newLogImage(tb, sessionID, streamID, joinPosition)
	agent.subscriptions = &stubAdder{sub: sub}
	return image
}

// responsePoll is one scripted PollAddPublication result.
type responsePoll struct {
	publication *aeron.Publication
	done        bool
	err         error
}

// fakeResponseAdder scripts ResponsePublicationAdder results. Each poll pops
// the first scripted result for its correlation id. An empty script polls
// pending. inFlight holds each started add until a done poll, an error poll,
// or a cancel. offerErrs fails the offer of each listed stream id.
type fakeResponseAdder struct {
	offers    []transportAdd
	offerErrs map[int32]error
	nextID    int64
	script    map[int64][]responsePoll
	inFlight  map[int64]bool
	polls     []int64
	cancels   []int64
}

func newFakeResponseAdder() *fakeResponseAdder {
	return &fakeResponseAdder{
		nextID:   1,
		script:   make(map[int64][]responsePoll),
		inFlight: make(map[int64]bool),
	}
}

func (f *fakeResponseAdder) OfferAddPublication(streamID int32, channel string) (int64, error) {
	f.offers = append(f.offers, transportAdd{streamID: streamID, channel: channel})
	if err := f.offerErrs[streamID]; err != nil {
		return 0, err
	}
	id := f.nextID
	f.nextID++
	f.inFlight[id] = true
	return id, nil
}

func (f *fakeResponseAdder) PollAddPublication(
	correlationID int64,
) (*aeron.Publication, bool, error) {
	f.polls = append(f.polls, correlationID)
	queue := f.script[correlationID]
	if len(queue) == 0 {
		return nil, false, nil
	}
	result := queue[0]
	f.script[correlationID] = queue[1:]
	if result.done || result.err != nil {
		delete(f.inFlight, correlationID)
	}
	return result.publication, result.done, result.err
}

func (f *fakeResponseAdder) CancelPending(correlationID int64) {
	f.cancels = append(f.cancels, correlationID)
	delete(f.inFlight, correlationID)
}

// next scripts the next poll result for correlationID.
func (f *fakeResponseAdder) next(correlationID int64, result responsePoll) {
	f.script[correlationID] = append(f.script[correlationID], result)
}

// sessionLog writes log frames to a bound agent image and delivers them.
type sessionLog struct {
	*takeLog
	tb    testing.TB
	agent *Agent
}

// bindSessionAgent binds a recording agent with adder and sets role. A nil
// adder leaves ResponsePublications unset.
func bindSessionAgent(
	tb testing.TB,
	adder ResponsePublicationAdder,
	role codecs.Role,
) (*sessionLog, *recordingService) {
	tb.Helper()
	svc := &recordingService{}
	log := bindSessionLog(tb, AgentConfig{Service: svc, ResponsePublications: adder}, role)
	return log, svc
}

// bindSessionLog binds an agent from cfg and sets role.
func bindSessionLog(tb testing.TB, cfg AgentConfig, role codecs.Role) *sessionLog {
	tb.Helper()
	agent, image, committed := bindImage(tb, cfg, 0)
	agent.role = role
	return &sessionLog{
		takeLog: &takeLog{image: image, committed: committed},
		tb:      tb,
		agent:   agent,
	}
}

// deliver writes payload, commits it, and runs one DoWork. Returns the work
// count.
func (l *sessionLog) deliver(payload []byte) int {
	l.tb.Helper()
	l.write(l.tb, payload)
	return l.agent.DoWork(0)
}

func (l *sessionLog) open(id, timestamp int64) {
	l.tb.Helper()
	l.deliver(encodeSessionOpen(l.tb, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: id,
		Timestamp:        timestamp,
		ResponseStreamID: testResponseStreamID,
		ResponseChannel:  []byte(testResponseChannel),
	}))
	require.NoError(l.tb, l.agent.ServiceFailure())
	require.True(l.tb, l.agent.HasSession(id))
}

func (l *sessionLog) close(id, timestamp int64) {
	l.tb.Helper()
	l.closeWithReason(id, timestamp, clusterwire.CloseReason_CLIENT_ACTION)
}

func (l *sessionLog) closeWithReason(id, timestamp int64, reason clusterwire.CloseReason) {
	l.tb.Helper()
	l.deliver(encodeSessionClose(l.tb, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: id,
		Timestamp:        timestamp,
		CloseReason:      reason,
	}))
	require.NoError(l.tb, l.agent.ServiceFailure())
	require.False(l.tb, l.agent.HasSession(id))
}

const (
	testResponseStreamID = int32(102)
	testResponseChannel  = "aeron:udp?endpoint=localhost:20121"
)

// requireEgressFrame asserts the frame at stream position start on pub
// decodes to want and payload.
func requireEgressFrame(
	tb assertTB,
	pub *aeron.Publication,
	start int64,
	want codecs.SessionMessageHeaderFields,
	payload []byte,
) {
	tb.Helper()
	termLength := int64(logbuffer.TermMinLength)
	termIndex := int((start / termLength) % logbuffer.PartitionCount)
	termOffset := int(start % termLength)
	termBuffer := pub.TermBuffer(termIndex)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuffer, termOffset))
	headerLength := codecs.SessionMessageHeaderEncodedLen()
	require.Equal(tb, logbuffer.DataHeaderLength+headerLength+len(payload), frameLength)
	wire := termBuffer.Bytes()[termOffset+logbuffer.OffsetFramePayload : termOffset+frameLength]
	got, ok := codecs.TryDecodeSessionMessageHeaderAt(wire, 0)
	require.True(tb, ok)
	require.Equal(tb, want, got)
	require.Equal(tb, payload, wire[headerLength:])
}

// writeTermEventAndSnapshot writes a NewLeadershipTermEvent with NANOS at
// position 0, then a SNAPSHOT action. Returns the action frame end.
func writeTermEventAndSnapshot(
	tb testing.TB,
	image *aeron.Image,
	committed *int64,
	timestamp int64,
) int64 {
	tb.Helper()
	log := &takeLog{image: image, committed: committed}
	log.termEvent(tb, clusterwire.ClusterTimeUnit_NANOS, 0)
	return log.snapshot(tb, timestamp)
}

func collectErrors(dst *[]error) func(error) {
	return func(err error) {
		*dst = append(*dst, err)
	}
}

type terminatingReporter struct {
	terminating bool
}

func (r *terminatingReporter) IsTerminating() bool {
	return r.terminating
}

func requireFailureReported(tb testing.TB, agent *Agent, got []error, causes ...error) {
	tb.Helper()
	failure := agent.ServiceFailure()
	require.ErrorIs(tb, failure, ErrFailed)
	for _, cause := range causes {
		require.ErrorIs(tb, failure, cause)
	}
	require.Len(tb, got, 1)
	require.Same(tb, failure, got[0])
}

func requireNonFatalReport(tb testing.TB, report error, causes ...error) {
	tb.Helper()

	for _, cause := range causes {
		require.ErrorIs(tb, report, cause)
	}

	require.NotErrorIs(tb, report, ErrFailed)
}

func requireOneNonFatalReport(tb testing.TB, got []error, causes ...error) {
	tb.Helper()
	require.Len(tb, got, 1)
	requireNonFatalReport(tb, got[0], causes...)
}

type moduleProxyFailureCase struct {
	name     string
	setup    func(*testing.T) *ConsensusModuleProxy
	reported error
}

// moduleProxyFailureCases returns the shared module proxy failure setups. Only
// the terminal setup reports terminalFailure.
func moduleProxyFailureCases(terminalFailure error) []moduleProxyFailureCase {
	return []moduleProxyFailureCase{
		{
			name:  "nilProxy",
			setup: func(*testing.T) *ConsensusModuleProxy { return nil },
		},
		{
			name:  "unboundProxy",
			setup: func(*testing.T) *ConsensusModuleProxy { return NewConsensusModuleProxy() },
		},
		{
			name: "retriableNotConnected",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				return boundModuleProxyWithLimit(t, 0, false)
			},
		},
		{
			name: "retriableBackPressured",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				return boundModuleProxyWithLimit(t, 0, true)
			},
		},
		{
			name: "terminalClosed",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				proxy, pub := boundModuleProxy(t)
				require.NoError(t, pub.Close())
				return proxy
			},
			reported: terminalFailure,
		},
	}
}

// bindCloseAgent is bindSessionAgent plus a connected module proxy.
func bindCloseAgent(
	tb testing.TB,
	adder ResponsePublicationAdder,
	role codecs.Role,
) (*sessionLog, *recordingService, *aeron.Publication) {
	tb.Helper()
	log, svc := bindSessionAgent(tb, adder, role)
	proxy, pub := boundModuleProxy(tb)
	log.agent.moduleProxy = proxy
	return log, svc, pub
}

// countingArchiveControl counts Poll calls. Each Poll reports pollWork.
type countingArchiveControl struct {
	polls    int
	pollWork int
}

func (c *countingArchiveControl) Poll() int {
	c.polls++
	return c.pollWork
}

func (*countingArchiveControl) ControlSessionID() int64 { return 0 }

func (*countingArchiveControl) Config() archive.Config { return archive.Config{} }

func (*countingArchiveControl) SetRecordingSignalHandler(archive.RecordingSignalHandler) {}

func writeClusterAction(
	tb testing.TB,
	image *aeron.Image,
	committed *int64,
	fields codecs.ClusterActionRequestFields,
) int {
	tb.Helper()
	payload := encodeClusterAction(tb, fields)
	span := writeAgentFrame(tb, image, 0, payload)
	*committed = int64(span)
	return span
}

func writeAlignedAction(
	tb testing.TB,
	image *aeron.Image,
	committed *int64,
	timestamp int64,
	action clusterwire.ClusterAction,
	flags int32,
) int {
	tb.Helper()
	span := clusterActionFrameSpan()
	got := writeClusterAction(tb, image, committed, codecs.ClusterActionRequestFields{
		LeadershipTermID: 1,
		LogPosition:      int64(span),
		Timestamp:        timestamp,
		Action:           action,
		Flags:            flags,
	})
	require.Equal(tb, span, got, "encoded logPosition vs frame span")
	return span
}

// requireOffers asserts that Offer to each id returns want. A zero want
// asserts a positive result.
func requireOffers(tb testing.TB, agent *Agent, want int64, ids ...int64) {
	tb.Helper()
	for _, id := range ids {
		session, ok := agent.ClientSession(id)
		require.True(tb, ok)
		result := session.Offer([]byte("m"))
		if want == 0 {
			require.Positive(tb, result, "session %d", id)
			continue
		}
		require.Equal(tb, want, result, "session %d", id)
	}
}

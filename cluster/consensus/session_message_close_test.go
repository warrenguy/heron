//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// fakeControlledFragmentPoller is a driver-free [ControlledFragmentPoller].
// Delivers queued frames with full ControlledAction semantics.
type fakeControlledFragmentPoller struct {
	frames []fakeControlledFragment
	next   int
}

type fakeControlledFragment struct {
	buffer atomicbuffer.AtomicBuffer
	offset int
	length int
	header *aeron.Header
}

func (f *fakeControlledFragmentPoller) ControlledPoll(
	handler aeron.ControlledFragmentHandler,
	fragmentLimit int,
) int {
	if handler == nil || fragmentLimit <= 0 {
		return 0
	}
	n := 0
	for n < fragmentLimit && f.next < len(f.frames) {
		fr := f.frames[f.next]
		action := handler(fr.buffer, fr.offset, fr.length, fr.header)
		if action == aeron.ControlledAbort {
			break
		}
		f.next++
		n++
		if action == aeron.ControlledBreak {
			break
		}
	}
	return n
}

// buildBeginEndControlledFragments splits wholeWire into BEGIN and END frames
// on one term buffer with continuous term offsets for the controlled assembler.
func buildBeginEndControlledFragments(
	t *testing.T,
	wholeWire []byte,
) []fakeControlledFragment {
	t.Helper()
	require.GreaterOrEqual(t, len(wholeWire), 2)
	mid := len(wholeWire) / 2
	specs := []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: wholeWire[:mid]},
		{Flags: logbuffer.EndFlag, Payload: wholeWire[mid:]},
	}
	capacity := aeron.CommittedDataFramesTermCapacityForTest(specs)
	term := atomicbuffer.Wrap(make([]byte, capacity))
	offsets := aeron.WriteCommittedDataFramesForTest(term, 0, specs)
	out := make([]fakeControlledFragment, 0, len(offsets))
	for _, off := range offsets {
		frameLength := int(logbuffer.FrameLengthVolatile(term, off))
		require.Positive(t, frameLength)
		header := &aeron.Header{}
		header.SetBuffer(term)
		header.SetOffset(off)
		header.ResetPollFragmentState()
		out = append(out, fakeControlledFragment{
			buffer: term,
			offset: off + logbuffer.OffsetFramePayload,
			length: frameLength - logbuffer.DataHeaderLength,
			header: header,
		})
	}
	return out
}

// openLeaderSession accepts one connect and drives sessionWork until open.
// Returns the agent, log publisher, exclusive log publication, and open session.
func openLeaderSession(
	t *testing.T,
	leadershipTermID int64,
	acceptNs int64,
) (*Agent, *LogPublisher, *aeron.ExclusivePublication, *clientSession) {
	t.Helper()
	p, pub := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, leadershipTermID)
	a.acceptSessionConnect(validConnectFields(1), acceptNs)
	// open + OK
	require.Equal(t, 2, a.sessionWork(acceptNs))
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s.state)
	require.NotNil(t, s.egressPublication)
	return a, p, pub, s
}

// pendingLeaderSession accepts one connect and leaves the session pending.
func pendingLeaderSession(
	t *testing.T,
	leadershipTermID int64,
	acceptNs int64,
) (*Agent, *clientSession) {
	t.Helper()
	p, _ := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, leadershipTermID)
	a.acceptSessionConnect(validConnectFields(1), acceptNs)
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionPending, s.state)
	return a, s
}

// attachQueueClientIngress wires a fresh queue client ingress onto a.
func attachQueueClientIngress(a *Agent) *QueueClientIngress {
	q := &QueueClientIngress{}
	a.SetClientIngress(q)
	return q
}

// blockLogPublication swaps the log for a zero-limit exclusive publication.
func blockLogPublication(t *testing.T, p *LogPublisher) {
	t.Helper()
	p.SetPublication(aeron.NewTestExclusivePublicationForHooksWithLimit(t, 0, true))
}

// forEachTermWire walks term-0 DATA frames. Stops when:
//   - visit returns true
//   - maxFrames is exhausted
//   - frame length is non-positive (end of term data)
func forEachTermWire(
	t *testing.T,
	pub termBufferSource,
	maxFrames int,
	visit func(wire []byte) (done bool),
) {
	t.Helper()
	termBuf := pub.TermBuffer(0)
	off := 0
	for range maxFrames {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, off))
		if frameLength <= 0 {
			return
		}
		wire := termBuf.Bytes()[off+logbuffer.OffsetFramePayload : off+frameLength]
		if visit(wire) {
			return
		}
		off += logbuffer.AlignFrameLength(frameLength)
	}
}

// requireTermWire fails unless visit returns true for some term-0 DATA frame.
func requireTermWire(
	t *testing.T,
	pub termBufferSource,
	maxFrames int,
	visit func(wire []byte) (done bool),
) {
	t.Helper()
	found := false
	forEachTermWire(t, pub, maxFrames, func(wire []byte) bool {
		if visit(wire) {
			found = true
			return true
		}
		return false
	})
	require.True(t, found, "template not found")
}

func encodeSessionMessageWire(
	t *testing.T,
	termID, sessionID, timestamp int64,
	payload []byte,
) []byte {
	t.Helper()
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	buf := make([]byte, headerLen+len(payload))
	_, err := codecs.EncodeIngressHeaderScratch(buf, codecs.IngressHeaderFields{
		LeadershipTermID: termID,
		ClusterSessionID: sessionID,
		Timestamp:        timestamp,
	})
	require.NoError(t, err)
	copy(buf[headerLen:], payload)
	return buf
}

func encodeSessionKeepAliveFieldsWire(
	t *testing.T,
	termID, sessionID int64,
) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.SessionKeepAliveSbeSchemaVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(clusterwire.SessionKeepAliveSbeBlockLength)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeSessionKeepAlive(buf, 0, actingVersion, codecs.SessionKeepAliveFields{
		LeadershipTermID: termID,
		ClusterSessionID: sessionID,
	})
	require.True(t, ok)
	return buf[:end]
}

func encodeSessionCloseRequestWire(
	t *testing.T,
	termID, sessionID int64,
) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.SessionCloseRequestSbeSchemaVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(clusterwire.SessionCloseRequestSbeBlockLength)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeSessionCloseRequest(
		buf, 0, actingVersion, codecs.SessionCloseRequestFields{
			LeadershipTermID: termID,
			ClusterSessionID: sessionID,
		},
	)
	require.True(t, ok)
	return buf[:end]
}

func requireSessionMessageOnLog(
	t *testing.T,
	logPub *aeron.ExclusivePublication,
	termID, sessionID, timestamp int64,
	payload []byte,
) {
	t.Helper()
	requireTermWire(t, logPub, 8, func(w []byte) bool {
		fields, ok := codecs.TryDecodeSessionMessageHeaderAt(w, 0)
		if !ok {
			return false
		}
		require.Equal(t, termID, fields.LeadershipTermID)
		require.Equal(t, sessionID, fields.ClusterSessionID)
		require.Equal(t, timestamp, fields.Timestamp)
		headerLen := codecs.SessionMessageHeaderEncodedLen()
		require.Equal(t, payload, w[headerLen:])
		return true
	})
}

func requireSessionCloseEventOnLog(
	t *testing.T,
	logPub *aeron.ExclusivePublication,
	termID, sessionID, timestamp int64,
	reason clusterwire.CloseReason,
) {
	t.Helper()
	requireTermWire(t, logPub, 8, func(w []byte) bool {
		fields, ok := codecs.TryDecodeSessionCloseEventAt(w, 0)
		if !ok {
			return false
		}
		require.Equal(t, termID, fields.LeadershipTermID)
		require.Equal(t, sessionID, fields.ClusterSessionID)
		require.Equal(t, timestamp, fields.Timestamp)
		require.Equal(t, reason, fields.CloseReason)
		return true
	})
}

func TestHandleSessionMessage_happy(t *testing.T) {
	const (
		termID   int64 = 7
		acceptNs int64 = 100
		nowNs    int64 = 200
	)
	a, _, logPub, s := openLeaderSession(t, termID, acceptNs)
	payload := []byte("app-payload")
	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionMessageWire(t, termID, s.id, 999, payload))

	require.Equal(t, 1, a.pollClientIngress(nowNs))
	require.Equal(t, nowNs, s.lastActivityNs)
	requireSessionMessageOnLog(t, logPub, termID, s.id, nowNs, payload)
}

func TestHandleSessionMessage_dropGates(t *testing.T) {
	const (
		termID   int64 = 3
		acceptNs int64 = 10
		nowNs    int64 = 20
	)
	payload := []byte("x")

	cases := []struct {
		name  string
		setup func(t *testing.T) (*Agent, *clientSession, []byte)
	}{
		{
			name: "term_mismatch",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				return a, s, encodeSessionMessageWire(t, termID+1, s.id, nowNs, payload)
			},
		},
		{
			name: "non_leader",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				a.role = RoleFollower
				return a, s, encodeSessionMessageWire(t, termID, s.id, nowNs, payload)
			},
		},
		{
			name: "unknown_session",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				return a, s, encodeSessionMessageWire(t, termID, 99, nowNs, payload)
			},
		},
		{
			name: "pending_session",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, s := pendingLeaderSession(t, termID, acceptNs)
				return a, s, encodeSessionMessageWire(t, termID, s.id, nowNs, payload)
			},
		},
		{
			name: "closing_session",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				require.True(t, a.sessions.markClosing(s.id, clusterwire.CloseReason_CLIENT_ACTION))
				return a, s, encodeSessionMessageWire(t, termID, s.id, nowNs, payload)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, s, wire := tc.setup(t)
			activityBefore := s.lastActivityNs
			posBefore := a.logPublisher.Position()
			q := attachQueueClientIngress(a)
			q.Enqueue(wire)
			require.Equal(t, 1, a.pollClientIngress(nowNs))
			require.Equal(t, 0, q.Len())
			require.Equal(t, activityBefore, s.lastActivityNs)
			require.Equal(t, posBefore, a.logPublisher.Position())
		})
	}
}

func TestHandleSessionMessage_backPressureRedelivers(t *testing.T) {
	const (
		termID   int64 = 5
		acceptNs int64 = 1
		nowNs    int64 = 50
	)
	a, p, _, s := openLeaderSession(t, termID, acceptNs)
	// Replace log with back-pressured publication.
	blockLogPublication(t, p)

	payload := []byte("retry-me")
	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionMessageWire(t, termID, s.id, nowNs, payload))

	// Abort holds the head; work count is 0 (matches Image.ControlledPoll).
	require.Equal(t, 0, a.pollClientIngress(nowNs))
	require.Equal(t, 1, q.Len())
	require.Equal(t, acceptNs, s.lastActivityNs)

	// Raise limit: same message appends.
	ready := aeron.NewTestExclusivePublicationForHooks(t)
	p.SetPublication(ready)
	require.Equal(t, 1, a.pollClientIngress(nowNs))
	require.Equal(t, 0, q.Len())
	require.Equal(t, nowNs, s.lastActivityNs)
	require.Positive(t, p.Position())
}

func TestHandleSessionMessage_oversizeFailClosed(t *testing.T) {
	const (
		termID   int64 = 5
		acceptNs int64 = 1
		nowNs    int64 = 50
	)
	a, p, pub, s := openLeaderSession(t, termID, acceptNs)
	posBefore := p.Position()
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	oversize := make([]byte, pub.MaxMessageLength()-headerLen+1)
	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionMessageWire(t, termID, s.id, nowNs, oversize))

	// Fail-closed Continue: queue drains, activity and log position untouched.
	require.Equal(t, 1, a.pollClientIngress(nowNs))
	require.Equal(t, 0, q.Len())
	require.Equal(t, acceptNs, s.lastActivityNs)
	require.Equal(t, posBefore, p.Position())
}

func TestHandleSessionMessage_emptyPayload(t *testing.T) {
	const (
		termID   int64 = 2
		acceptNs int64 = 1
		nowNs    int64 = 10
	)
	a, _, _, s := openLeaderSession(t, termID, acceptNs)
	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionMessageWire(t, termID, s.id, nowNs, nil))
	require.Equal(t, 1, a.pollClientIngress(nowNs))
	require.Equal(t, nowNs, s.lastActivityNs)
}

func TestSessionMessagePayload_bounds(t *testing.T) {
	payload := []byte("abc")
	wire := encodeSessionMessageWire(t, 1, 2, 3, payload)
	layout, ok := codecs.ClusterMessageLayoutAt(wire, 0, codecs.ClusterDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, payload, codecs.SessionMessagePayloadFromLayout(wire, layout))

	// Empty payload. Helper returns empty non-nil span. Nil is the drop path.
	empty := encodeSessionMessageWire(t, 1, 2, 3, nil)
	layout, ok = codecs.ClusterMessageLayoutAt(empty, 0, codecs.ClusterDecodeMaxVarLen)
	require.True(t, ok)
	emptyPayload := codecs.SessionMessagePayloadFromLayout(empty, layout)
	require.NotNil(t, emptyPayload)
	require.Empty(t, emptyPayload)

	// Truncated: body shorter than BlockLength fails layout parse.
	truncated := wire[:layout.BodyOffset+int(layout.BlockLength)-1]
	_, ok2 := codecs.ClusterMessageLayoutAt(truncated, 0, codecs.ClusterDecodeMaxVarLen)
	require.False(t, ok2)
	// Full-wire layout on short buf. Block end past len returns nil.
	require.Nil(t, codecs.SessionMessagePayloadFromLayout(truncated, layout))
}

func TestHandleSessionKeepAlive(t *testing.T) {
	const (
		termID   int64 = 4
		acceptNs int64 = 100
		nowNs    int64 = 500
	)
	a, p, _, s := openLeaderSession(t, termID, acceptNs)
	posBefore := p.Position()
	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionKeepAliveFieldsWire(t, termID, s.id))
	require.Equal(t, 1, a.pollClientIngress(nowNs))
	require.Equal(t, nowNs, s.lastActivityNs)
	require.Equal(t, posBefore, p.Position())
}

func TestHandleSessionKeepAlive_silentGates(t *testing.T) {
	const (
		termID   int64 = 4
		acceptNs int64 = 100
		nowNs    int64 = 500
	)
	cases := []struct {
		name  string
		setup func(t *testing.T) (*Agent, *clientSession, []byte)
	}{
		{
			name: "term_mismatch",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				return a, s, encodeSessionKeepAliveFieldsWire(t, termID+1, s.id)
			},
		},
		{
			name: "unknown_session",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				return a, s, encodeSessionKeepAliveFieldsWire(t, termID, 99)
			},
		},
		{
			name: "pending_session",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, s := pendingLeaderSession(t, termID, acceptNs)
				return a, s, encodeSessionKeepAliveFieldsWire(t, termID, s.id)
			},
		},
		{
			name: "closing_session",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				require.True(t, a.sessions.markClosing(s.id, clusterwire.CloseReason_CLIENT_ACTION))
				return a, s, encodeSessionKeepAliveFieldsWire(t, termID, s.id)
			},
		},
		{
			name: "non_leader",
			setup: func(t *testing.T) (*Agent, *clientSession, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				a.role = RoleFollower
				return a, s, encodeSessionKeepAliveFieldsWire(t, termID, s.id)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, s, wire := tc.setup(t)
			activity := s.lastActivityNs
			q := attachQueueClientIngress(a)
			q.Enqueue(wire)
			require.Equal(t, 1, a.pollClientIngress(nowNs))
			require.Equal(t, activity, s.lastActivityNs)
		})
	}
}

func TestHandleSessionKeepAlive_defersTimeout(t *testing.T) {
	const (
		termID   int64 = 1
		acceptNs int64 = 0
	)
	a, _, _, s := openLeaderSession(t, termID, acceptNs)
	// Keepalive just inside the window.
	keepNs := acceptNs + a.sessionTimeoutNs - 1
	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionKeepAliveFieldsWire(t, termID, s.id))
	require.Equal(t, 1, a.pollClientIngress(keepNs))
	require.Equal(t, keepNs, s.lastActivityNs)

	// Past original accept deadline but inside refreshed window: stays open.
	pastAccept := acceptNs + a.sessionTimeoutNs + 1
	require.Equal(t, 0, a.sessionCloseWork(pastAccept))
	require.Equal(t, 1, a.OpenSessionCount())

	// Past refreshed deadline: timeout closes.
	pastKeep := keepNs + a.sessionTimeoutNs + 1
	require.Positive(t, a.sessionCloseWork(pastKeep))
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
}

func TestHandleSessionCloseRequest_happy(t *testing.T) {
	const (
		termID   int64 = 9
		acceptNs int64 = 1
		nowNs    int64 = 100
	)
	a, _, logPub, s := openLeaderSession(t, termID, acceptNs)
	egress := s.egressPublication
	require.NotNil(t, egress)

	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionCloseRequestWire(t, termID, s.id))
	require.Equal(t, 1, a.pollClientIngress(nowNs))

	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
	require.Equal(t, 0, a.PendingSessionCount())
	require.True(t, egress.IsClosed())
	_, ok := a.sessions.sessionByID(s.id)
	require.False(t, ok)

	requireSessionCloseEventOnLog(
		t, logPub, termID, s.id, nowNs, clusterwire.CloseReason_CLIENT_ACTION,
	)
}

func TestHandleSessionCloseRequest_prepareRestoresClosing(t *testing.T) {
	const (
		termID   int64 = 9
		acceptNs int64 = 1
		nowNs    int64 = 100
	)
	tests := []struct {
		name         string
		atOpen       bool
		wantRestored bool
	}{
		{name: "below the close", atOpen: true, wantRestored: true},
		{name: "below the open", wantRestored: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, _, _, s := openLeaderSession(t, termID, acceptNs)
			q := attachQueueClientIngress(a)
			q.Enqueue(encodeSessionCloseRequestWire(t, termID, s.id))
			require.Equal(t, 1, a.pollClientIngress(nowNs))
			_, ok := a.sessions.sessionByID(s.id)
			require.False(t, ok)

			prepareAt := int64(0)
			if tt.atOpen {
				prepareAt = s.openedLogPosition
			}
			_, prepared, err := a.PrepareForNewLeadership(prepareAt, nowNs)
			require.NoError(t, err)
			require.True(t, prepared)

			wantClosing := 0
			if tt.wantRestored {
				wantClosing = 1
			}
			require.Equal(t, wantClosing, a.ClosingSessionCount())
			require.Equal(t, 0, a.OpenSessionCount())
			require.Empty(t, a.sessions.uncommittedCloses)
			got, ok := a.sessions.sessionByID(s.id)
			require.Equal(t, tt.wantRestored, ok)
			if tt.wantRestored {
				require.Same(t, s, got)
				require.Equal(t, sessionClosing, got.state)
				require.Equal(t, clusterwire.CloseReason_CLIENT_ACTION, got.closeReason)
				require.Nil(t, got.egressPublication)
			}
		})
	}
}

func TestHandleSessionCloseRequest_appendRetry(t *testing.T) {
	const (
		termID   int64 = 9
		acceptNs int64 = 1
		nowNs    int64 = 100
	)
	a, p, _, s := openLeaderSession(t, termID, acceptNs)
	// Block log for close append.
	blockLogPublication(t, p)

	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionCloseRequestWire(t, termID, s.id))
	require.Equal(t, 1, a.pollClientIngress(nowNs))
	require.Equal(t, sessionClosing, s.state)
	require.Equal(t, clusterwire.CloseReason_CLIENT_ACTION, s.closeReason)
	require.Nil(t, s.egressPublication)
	require.Equal(t, 1, a.ClosingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())

	// Second close while closing drops.
	q.Enqueue(encodeSessionCloseRequestWire(t, termID, s.id))
	require.Equal(t, 1, a.pollClientIngress(nowNs+1))
	require.Equal(t, 1, a.ClosingSessionCount())

	// Unblock log: sessionCloseWork removes.
	ready := aeron.NewTestExclusivePublicationForHooks(t)
	p.SetPublication(ready)
	require.Equal(t, 1, a.sessionCloseWork(nowNs+2))
	require.Equal(t, 0, a.ClosingSessionCount())
	_, ok := a.sessions.sessionByID(s.id)
	require.False(t, ok)

	fields, ok := codecs.TryDecodeSessionCloseEventAt(termStartWire(t, ready), 0)
	require.True(t, ok)
	require.Equal(t, clusterwire.CloseReason_CLIENT_ACTION, fields.CloseReason)
	require.Equal(t, s.id, fields.ClusterSessionID)
}

func TestHandleSessionCloseRequest_dropGates(t *testing.T) {
	const (
		termID   int64 = 2
		acceptNs int64 = 1
		nowNs    int64 = 10
	)
	cases := []struct {
		name  string
		setup func(t *testing.T) (*Agent, []byte)
		check func(t *testing.T, a *Agent)
	}{
		{
			name: "wrong_term",
			setup: func(t *testing.T) (*Agent, []byte) {
				a, _, _, s := openLeaderSession(t, termID, acceptNs)
				return a, encodeSessionCloseRequestWire(t, termID+1, s.id)
			},
			check: func(t *testing.T, a *Agent) {
				require.Equal(t, 1, a.OpenSessionCount())
			},
		},
		{
			name: "unknown_session",
			setup: func(t *testing.T) (*Agent, []byte) {
				a, _, _, _ := openLeaderSession(t, termID, acceptNs)
				return a, encodeSessionCloseRequestWire(t, termID, 99)
			},
			check: func(t *testing.T, a *Agent) {
				require.Equal(t, 1, a.OpenSessionCount())
			},
		},
		{
			name: "pending_session",
			setup: func(t *testing.T) (*Agent, []byte) {
				a, s := pendingLeaderSession(t, termID, acceptNs)
				return a, encodeSessionCloseRequestWire(t, termID, s.id)
			},
			check: func(t *testing.T, a *Agent) {
				require.Equal(t, 1, a.PendingSessionCount())
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, wire := tc.setup(t)
			q := attachQueueClientIngress(a)
			q.Enqueue(wire)
			require.Equal(t, 1, a.pollClientIngress(nowNs))
			tc.check(t, a)
		})
	}
}

func TestSessionCloseWork_timeout(t *testing.T) {
	const (
		termID   int64 = 6
		acceptNs int64 = 1000
	)
	a, _, logPub, s := openLeaderSession(t, termID, acceptNs)
	egress := s.egressPublication
	require.NotNil(t, egress)

	// Just before deadline: no work.
	before := acceptNs + a.sessionTimeoutNs
	require.Equal(t, 0, a.sessionCloseWork(before))
	require.Equal(t, 1, a.OpenSessionCount())

	// Past deadline: TIMEOUT close, CLOSED egress, remove.
	past := acceptNs + a.sessionTimeoutNs + 1
	require.Equal(t, 2, a.sessionCloseWork(past)) // mark + remove
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
	require.True(t, egress.IsClosed())

	requireSessionCloseEventOnLog(
		t, logPub, termID, s.id, past, clusterwire.CloseReason_TIMEOUT,
	)

	// CLOSED SessionEvent on egress term buffer after the OK from open.
	off := 0
	foundClosed := false
	for range 8 {
		ev, next := decodeSessionEventAtTermOffset(t, egress, off)
		if ev.Code == clusterwire.EventCode_CLOSED {
			require.Equal(t, []byte("TIMEOUT"), ev.Detail)
			foundClosed = true
			break
		}
		off = next
	}
	require.True(t, foundClosed)
}

func TestSessionCloseWork_closedSendBestEffort(t *testing.T) {
	const (
		termID   int64 = 6
		acceptNs int64 = 1
	)
	a, _, _, s := openLeaderSession(t, termID, acceptNs)
	// Zero-window egress: CLOSED send fails; remove still proceeds.
	s.egressPublication = aeron.NewTestPublicationForHooksWithLimit(t, 0, true)
	past := acceptNs + a.sessionTimeoutNs + 1
	require.Positive(t, a.sessionCloseWork(past))
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
}

// Asserts the byID-desync fail-closed path on sessionCloseWork. A closing
// session on all and missing from byID must drop the all entry after a
// successful append, or the walk re-appends forever.
//
// Registry methods never produce this desync alone. The test forces the desync
// for the fail-closed path.
func TestSessionCloseWork_closingMissingFromByID(t *testing.T) {
	const (
		termID   int64 = 6
		acceptNs int64 = 1
		nowNs    int64 = 100
	)
	a, _, logPub, s := openLeaderSession(t, termID, acceptNs)
	require.True(t, a.sessions.markClosing(s.id, clusterwire.CloseReason_CLIENT_ACTION))
	require.Equal(t, sessionClosing, s.state)
	require.Len(t, a.sessions.all, 1)

	// Force registry desync for the fail-closed path.
	delete(a.sessions.byID, s.id)
	require.Len(t, a.sessions.all, 1)

	// One sessionCloseWork succeeds. Fail-closed remove drops the all entry
	// and closingCount. Without removeOrphan the walk re-appends forever and
	// OpenSessionCount stays negative.
	require.Equal(t, 1, a.sessionCloseWork(nowNs))
	require.Empty(t, a.sessions.all)
	require.Empty(t, a.sessions.byID)
	require.Equal(t, 0, a.ClosingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())

	// Further sessionCloseWork calls must not re-append or hang.
	require.Equal(t, 0, a.sessionCloseWork(nowNs+1))
	require.Equal(t, 0, a.sessionCloseWork(nowNs+2))
	require.Empty(t, a.sessions.all)
	require.Equal(t, 0, a.ClosingSessionCount())
	require.Equal(t, 0, a.OpenSessionCount())

	requireSessionCloseEventOnLog(
		t, logPub, termID, s.id, nowNs, clusterwire.CloseReason_CLIENT_ACTION,
	)
}

func TestSessionCloseWork_acceptOrder(t *testing.T) {
	const (
		termID   int64 = 1
		acceptNs int64 = 0
	)
	p, logPub := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, termID)
	a.acceptSessionConnect(validConnectFields(1), acceptNs)
	a.acceptSessionConnect(validConnectFields(2), acceptNs)
	// 2 open + 2 OK
	require.Equal(t, 4, a.sessionWork(acceptNs))
	require.Equal(t, 2, a.OpenSessionCount())

	past := acceptNs + a.sessionTimeoutNs + 1
	require.Positive(t, a.sessionCloseWork(past))
	require.Equal(t, 0, a.OpenSessionCount())

	// Close events in accept order: session 1 then 2.
	var closeIDs []int64
	forEachTermWire(t, logPub, 16, func(w []byte) bool {
		if fields, ok := codecs.TryDecodeSessionCloseEventAt(w, 0); ok {
			closeIDs = append(closeIDs, fields.ClusterSessionID)
			return len(closeIDs) == 2
		}
		return false
	})
	require.Equal(t, []int64{1, 2}, closeIDs)
}

func TestSessionCloseWork_nonLeaderKeepsClosing(t *testing.T) {
	const (
		termID   int64 = 3
		acceptNs int64 = 10
	)
	a, p, _, s := openLeaderSession(t, termID, acceptNs)
	// Mark closing with blocked append. Leave a second connect pending.
	blockLogPublication(t, p)
	require.True(t, a.sessions.markClosing(s.id, clusterwire.CloseReason_CLIENT_ACTION))
	a.releaseSessionEgress(s)
	a.acceptSessionConnect(validConnectFields(2), acceptNs)
	require.Equal(t, 1, a.ClosingSessionCount())
	require.Equal(t, 1, a.PendingSessionCount())

	// Phase: demoted past the deadline. The pending session goes. The closing
	// session stays.
	a.role = RoleFollower
	past := acceptNs + a.sessionTimeoutNs + 1
	require.Equal(t, 1, a.egressEventWork(past))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 1, a.ClosingSessionCount())
	_, ok := a.sessions.sessionByID(s.id)
	require.True(t, ok)

	// Phase: a committed SessionCloseEvent removes the closing session.
	a.handleSessionCloseEvent(codecs.SessionCloseEventFields{
		LeadershipTermID: termID,
		ClusterSessionID: s.id,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	})
	require.Equal(t, 0, a.ClosingSessionCount())
	_, ok = a.sessions.sessionByID(s.id)
	require.False(t, ok)
	require.Empty(t, a.sessions.all)
}

func TestSessionCloseWork_skipsPreOpen(t *testing.T) {
	const (
		termID   int64 = 1
		acceptNs int64 = 0
	)
	a, _ := pendingLeaderSession(t, termID, acceptNs)
	require.Equal(t, 1, a.PendingSessionCount())

	past := acceptNs + a.sessionTimeoutNs + 1
	// sessionCloseWork skips pending; any-role deadline reclaim owns pre-open.
	require.Equal(t, 0, a.sessionCloseWork(past))
	require.Equal(t, 1, a.PendingSessionCount())
}

// unappendableLogCase is one log publisher setup that cannot append
// SessionCloseEvent.
type unappendableLogCase struct {
	name  string
	apply func(t *testing.T, p *LogPublisher)
}

// unappendableLogCases returns the publisher states both close paths must
// refuse to mark. Back-pressure is excluded. A bound open publication marks and
// retries.
func unappendableLogCases() []unappendableLogCase {
	return []unappendableLogCase{
		{
			name: "unbound",
			apply: func(t *testing.T, p *LogPublisher) {
				t.Helper()
				p.SetPublication(nil)
			},
		},
		{
			name: "closed_publication",
			apply: func(t *testing.T, p *LogPublisher) {
				t.Helper()
				pub := aeron.NewTestExclusivePublicationForHooks(t)
				require.NoError(t, pub.Close())
				p.SetPublication(pub)
			},
		},
	}
}

func TestSessionCloseWork_unappendableLogSkipsTimeoutMark(t *testing.T) {
	const (
		termID   int64 = 2
		acceptNs int64 = 0
	)
	for _, tc := range unappendableLogCases() {
		t.Run(tc.name, func(t *testing.T) {
			a, p, _, s := openLeaderSession(t, termID, acceptNs)
			tc.apply(t, p)
			past := acceptNs + a.sessionTimeoutNs + 1
			require.Equal(t, 0, a.sessionCloseWork(past))
			require.Equal(t, sessionOpen, s.state)
			require.Equal(t, 1, a.OpenSessionCount())
			require.Equal(t, 0, a.ClosingSessionCount())
		})
	}
}

func TestHandleSessionCloseRequest_unappendableLogDropsRequest(t *testing.T) {
	const (
		termID   int64 = 5
		acceptNs int64 = 0
		nowNs    int64 = 50
	)
	for _, tc := range unappendableLogCases() {
		t.Run(tc.name, func(t *testing.T) {
			a, p, _, s := openLeaderSession(t, termID, acceptNs)
			egress := s.egressPublication
			tc.apply(t, p)

			q := attachQueueClientIngress(a)
			q.Enqueue(encodeSessionCloseRequestWire(t, termID, s.id))
			require.Equal(t, 1, a.pollClientIngress(nowNs))

			// Asserts the mark is skipped: the publisher cannot append
			// SessionCloseEvent, and the leader path never reclaims closing.
			require.Equal(t, sessionOpen, s.state)
			require.Equal(t, 1, a.OpenSessionCount())
			require.Equal(t, 0, a.ClosingSessionCount())
			require.Same(t, egress, s.egressPublication)

			// Asserts a bound open publication: the next request closes and removes.
			p.SetPublication(aeron.NewTestExclusivePublicationForHooks(t))
			q.Enqueue(encodeSessionCloseRequestWire(t, termID, s.id))
			require.Equal(t, 1, a.pollClientIngress(nowNs+1))
			require.Equal(t, 0, a.OpenSessionCount())
			require.Equal(t, 0, a.ClosingSessionCount())
			_, ok := a.sessions.sessionByID(s.id)
			require.False(t, ok)
		})
	}
}

func TestSessionWork_closeBeforeOKDrain(t *testing.T) {
	// sessionCloseWork runs before okPendingWork. An open session past idle
	// timeout with OK still queued is removed without hanging on okPending.
	const termID int64 = 1
	p, _ := boundLogPublisher(t)
	a := leaderWithLogPublisher(t, p, termID)
	a.acceptSessionConnect(validConnectFields(1), 0)
	require.Equal(t, 2, a.sessionWork(0))
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	// Re-queue OK and force timeout in the same sessionWork call.
	a.sessions.setOKPending(s, true)
	s.lastActivityNs = 0
	past := a.sessionTimeoutNs + 1
	require.Positive(t, a.sessionWork(past))
	require.Equal(t, 0, a.OpenSessionCount())
	require.Empty(t, a.sessions.okPending)
}

func TestQueueClientIngress_abortHold(t *testing.T) {
	q := &QueueClientIngress{}
	msg := []byte{1, 2, 3}
	q.Enqueue(msg)
	q.Enqueue([]byte{4})

	n := q.Poll(10, func([]byte, int) aeron.ControlledAction {
		return aeron.ControlledAbort
	})
	require.Equal(t, 0, n)
	require.Equal(t, 2, q.Len())

	var seen [][]byte
	n = q.Poll(10, func(buf []byte, _ int) aeron.ControlledAction {
		seen = append(seen, append([]byte(nil), buf...))
		return aeron.ControlledContinue
	})
	require.Equal(t, 2, n)
	require.Equal(t, 0, q.Len())
	require.Equal(t, [][]byte{{1, 2, 3}, {4}}, seen)
}

// TestSubscriptionClientIngress_fragmentedMessageAppends feeds a BEGIN/END
// train through SubscriptionClientIngress over a controlled poller stub.
// Reassembly delivers one whole session message that appends to the log.
func TestSubscriptionClientIngress_fragmentedMessageAppends(t *testing.T) {
	const (
		termID   int64 = 8
		acceptNs int64 = 1
		nowNs    int64 = 77
	)
	a, p, logPub, s := openLeaderSession(t, termID, acceptNs)
	payload := []byte("reassembled-payload-bytes")
	whole := encodeSessionMessageWire(t, termID, s.id, 0, payload)
	poller := &fakeControlledFragmentPoller{
		frames: buildBeginEndControlledFragments(t, whole),
	}
	ingress := NewSubscriptionClientIngress(poller)
	a.SetClientIngress(ingress)

	posBefore := p.Position()
	require.Equal(t, 2, a.pollClientIngress(nowNs))
	require.Equal(t, nowNs, s.lastActivityNs)
	require.Greater(t, p.Position(), posBefore)
	requireSessionMessageOnLog(t, logPub, termID, s.id, nowNs, payload)
}

func TestNewAgent_sessionTimeoutNs(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, DefaultSessionTimeoutNs, a.sessionTimeoutNs)

	a2 := NewAgent(AgentConfig{SessionTimeoutNs: 42})
	require.Equal(t, int64(42), a2.sessionTimeoutNs)

	require.Panics(t, func() {
		_ = NewAgent(AgentConfig{SessionTimeoutNs: -1})
	})
}

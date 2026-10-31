//go:build rapid && testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	asyncRapidForeignCorrelationID int64 = 1 << 40
	asyncRapidMaxBuilds                  = 4
)

// asyncControlModel mirrors the session control poller filter and latch.
type asyncControlModel struct {
	filter        int64
	latched       bool
	latchedFields codecs.ControlResponseFields
	pending       []asyncControlModelEvent
}

// asyncControlModelEvent is one queued fragment. A nil apply marks a response.
type asyncControlModelEvent struct {
	response codecs.ControlResponseFields
	apply    func()
}

// build mirrors ResetAwait.
func (m *asyncControlModel) build(correlationID int64) {
	m.latched = false
	m.filter = correlationID
}

// close mirrors handle Close.
func (m *asyncControlModel) close(correlationID int64) {
	m.clearMatching(correlationID)
	if m.filter == correlationID {
		m.filter = 0
	}
}

// clearMatching mirrors clearMatchingControlResponse.
func (m *asyncControlModel) clearMatching(correlationID int64) {
	if m.latchedFor(correlationID) {
		m.latched = false
	}
}

func (m *asyncControlModel) latchedFor(correlationID int64) bool {
	return m.latched &&
		m.latchedFields.ControlSessionID == controlResponseTestControlSessionID &&
		m.latchedFields.CorrelationID == correlationID
}

func (m *asyncControlModel) applyResponse(fields codecs.ControlResponseFields) bool {
	if m.filter != 0 && m.filter != fields.CorrelationID {
		return true
	}
	if m.latched {
		return false
	}
	m.latched = true
	m.latchedFields = fields
	return true
}

// queueResponse appends a response to term and to the model queue.
func (m *asyncControlModel) queueResponse(
	t *testing.T,
	term *asyncRapidTerm,
	fields codecs.ControlResponseFields,
) {
	term.append(t, encodeControlResponseWire(t, fields))
	m.pending = append(m.pending, asyncControlModelEvent{response: fields})
}

// queueEvent appends wire to term. The model runs apply when a poll reaches it.
func (m *asyncControlModel) queueEvent(
	t *testing.T,
	term *asyncRapidTerm,
	wire []byte,
	apply func(),
) {
	term.append(t, wire)
	m.pending = append(m.pending, asyncControlModelEvent{apply: apply})
}

// poll mirrors one Client.Poll.
func (m *asyncControlModel) poll() int {
	if m.latched && m.filter == 0 {
		m.latched = false
	}
	consumed := 0
	for consumed < min(len(m.pending), DefaultControlResponseFragmentLimit) {
		event := m.pending[consumed]
		if event.apply != nil {
			event.apply()
		} else if !m.applyResponse(event.response) {
			break
		}
		consumed++
	}
	m.pending = m.pending[consumed:]
	return consumed
}

func requireAsyncControlModel(rt *rapid.T, m *asyncControlModel, poller *ControlResponsePoller) {
	rt.Helper()

	require.Equal(rt, m.filter, poller.awaitCorrelationID)
	require.Equal(rt, m.latched, poller.IsPollComplete())
	if !m.latched {
		return
	}
	resp, ok := poller.Response()
	require.True(rt, ok)
	require.Equal(rt, m.latchedFields.ControlSessionID, resp.ControlSessionID)
	require.Equal(rt, m.latchedFields.CorrelationID, resp.CorrelationID)
	require.Equal(rt, m.latchedFields.RelevantID, resp.RelevantID)
	require.Equal(rt, m.latchedFields.Code, resp.Code)
}

// asyncModelHandle holds the state every model handle mirrors.
type asyncModelHandle struct {
	correlationID int64
	closed        bool
}

func (handle *asyncModelHandle) state() *asyncModelHandle {
	return handle
}

type asyncModelHandleRef interface {
	state() *asyncModelHandle
	Close()
}

// asyncHandleModel tracks every handle built. Only the newest can be open.
type asyncHandleModel[H asyncModelHandleRef] struct {
	asyncControlModel
	handles []H
}

// add records handle as the open handle and arms the filter for it.
func (m *asyncHandleModel[H]) add(rt *rapid.T, handle H) {
	rt.Helper()

	correlationID := handle.state().correlationID
	for _, prior := range m.handles {
		require.Less(rt, prior.state().correlationID, correlationID)
	}
	m.handles = append(m.handles, handle)
	m.build(correlationID)
}

// open returns the newest handle while it is open.
func (m *asyncHandleModel[H]) open() (H, bool) {
	var zero H
	last := len(m.handles) - 1
	if last < 0 || m.handles[last].state().closed {
		return zero, false
	}
	return m.handles[last], true
}

func (m *asyncHandleModel[H]) closeOpen() {
	if handle, ok := m.open(); ok {
		m.closeHandle(handle)
	}
}

// closeHandle closes handle and reports whether it was open.
func (m *asyncHandleModel[H]) closeHandle(handle H) bool {
	handle.Close()
	state := handle.state()
	if state.closed {
		return false
	}
	state.closed = true
	m.close(state.correlationID)
	return true
}

// drawCorrelationID picks a handle correlation id or a foreign id.
func (m *asyncHandleModel[H]) drawCorrelationID(rt *rapid.T, label string) int64 {
	index := rapid.IntRange(0, len(m.handles)).Draw(rt, label)
	if index == len(m.handles) {
		return asyncRapidForeignCorrelationID
	}
	return m.handles[index].state().correlationID
}

// drawHandle picks the open handle on half the draws while one is open.
func (m *asyncHandleModel[H]) drawHandle(rt *rapid.T) H {
	handle, ok := m.open()
	if ok && rapid.Bool().Draw(rt, "drawOpen") {
		return handle
	}
	return m.handles[rapid.IntRange(0, len(m.handles)-1).Draw(rt, "handleIndex")]
}

// drawAsyncOps draws the op sequence for one run. Ops range over
// [opBuild, opClose]. Half the runs lead with a disarmed prefix. The prefix
// builds one handle, then closes it. The close disarms the filter. Each client
// poll then clears the disarmed latch and latches the next response.
func drawAsyncOps[Op ~int](rt *rapid.T, opBuild, opResponse, opClientPoll, opClose Op) []Op {
	var ops []Op
	if rapid.Bool().Draw(rt, "disarmedPrefix") {
		ops = append(ops, opBuild, opClose, opResponse, opClientPoll, opResponse, opClientPoll)
	}
	stepCount := rapid.IntRange(1, 40).Draw(rt, "stepCount")
	for range stepCount {
		ops = append(ops, Op(rapid.IntRange(int(opBuild), int(opClose)).Draw(rt, "op")))
	}
	return ops
}

// Returns controlResponseTestControlSessionID on three draws in four.
func drawAsyncControlSessionID(rt *rapid.T) int64 {
	if rapid.IntRange(0, 3).Draw(rt, "controlSessionDraw") == 0 {
		return controlResponseTestControlSessionID + 1
	}
	return controlResponseTestControlSessionID
}

type asyncRapidTerm struct {
	sub        *aeron.Subscription
	image      *aeron.Image
	termBuffer atomicbuffer.AtomicBuffer
}

func newAsyncRapidTerm(t *testing.T) *asyncRapidTerm {
	t.Helper()

	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return &asyncRapidTerm{
		sub:        sub,
		image:      image,
		termBuffer: aeron.ImageTermBufferForHooks(image),
	}
}

func (term *asyncRapidTerm) newClient(aeronClient *aeron.Client, proxy *Proxy) *Client {
	term.termBuffer.SetBytes(0, term.termBuffer.Capacity(), 0)
	aeron.ImageSetSubscriberPositionForHooks(term.image, 0)
	return &Client{
		controlSessionID:    controlResponseTestControlSessionID,
		proxy:               proxy,
		controlPoller:       newControlResponsePollerForTest(term.sub),
		descriptorPoller:    NewRecordingDescriptorPoller(),
		controlSubscription: term.sub,
		controlAssembler:    NewFragmentAssembler(nil),
		aeronClient:         aeronClient,
	}
}

func (term *asyncRapidTerm) append(t *testing.T, wire []byte) {
	t.Helper()

	appendUnfragmentedWireToTerm(t, term.termBuffer, wire)
}

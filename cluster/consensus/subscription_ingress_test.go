package consensus

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// fakeFragment is one committed fragment a fakeFragmentPoller hands to a handler.
type fakeFragment struct {
	buffer atomicbuffer.AtomicBuffer
	offset int
	length int
	header *aeron.Header
}

// fakeFragmentPoller is a driver-free [FragmentPoller] for translation tests.
//
// Each Poll records the handler and limit, then delivers up to fragmentLimit
// queued frames in order. Returns the number of frames handed over.
type fakeFragmentPoller struct {
	frames   []fakeFragment
	next     int
	handlers []aeron.FragmentHandler
	polls    int
}

func (f *fakeFragmentPoller) Poll(
	handler aeron.FragmentHandler,
	fragmentLimit int,
) int {
	f.polls++
	f.handlers = append(f.handlers, handler)
	n := 0
	for n < fragmentLimit && f.next < len(f.frames) {
		fr := f.frames[f.next]
		f.next++
		handler(fr.buffer, fr.offset, fr.length, fr.header)
		n++
	}
	return n
}

// fragmentRequireT is require.TestingT plus Helper, so setup helpers accept both
// *testing.T and *rapid.T. *rapid.T is not testing.TB.
type fragmentRequireT interface {
	require.TestingT
	Helper()
}

// buildFakeFragment builds one DATA frame with payload at OffsetFramePayload.
// flags override the header flags byte after the unfragmented write.
func buildFakeFragment(tb fragmentRequireT, payload []byte, flags byte) fakeFragment {
	tb.Helper()
	raw := make([]byte, logbuffer.DataHeaderLength+len(payload)+64)
	ab := atomicbuffer.Wrap(raw)
	require.NoError(tb, logbuffer.WriteDataHeaderUnfragmented(
		ab,
		0,
		1, // sessionID
		2, // streamID
		3, // termID
		0, // termOffset
		0,
	))
	ab.PutByte(logbuffer.OffsetFlags, flags)
	copy(raw[logbuffer.OffsetFramePayload:], payload)
	header := &aeron.Header{}
	header.SetBuffer(ab)
	header.SetOffset(0)
	return fakeFragment{
		buffer: ab,
		offset: logbuffer.OffsetFramePayload,
		length: len(payload),
		header: header,
	}
}

// buildFakeFragmentAt builds a fragment with an explicit offset and length into
// a sized backing buffer. Used for bounds-guard rows.
func buildFakeFragmentAt(
	tb fragmentRequireT,
	rawLen int,
	offset int,
	length int,
	flags byte,
) fakeFragment {
	tb.Helper()
	need := max(rawLen, logbuffer.DataHeaderLength)
	raw := make([]byte, need)
	ab := atomicbuffer.Wrap(raw)
	require.NoError(tb, logbuffer.WriteDataHeaderUnfragmented(
		ab,
		0,
		1,
		2,
		3,
		0,
		0,
	))
	ab.PutByte(logbuffer.OffsetFlags, flags)
	header := &aeron.Header{}
	header.SetBuffer(ab)
	header.SetOffset(0)
	return fakeFragment{
		buffer: ab,
		offset: offset,
		length: length,
		header: header,
	}
}

func recordDeliveries(
	t *testing.T,
	s *SubscriptionConsensusIngress,
	fragmentLimit int,
) (payloads [][]byte, offsets []int, work int) {
	t.Helper()
	work = s.Poll(fragmentLimit, func(buf []byte, offset int) {
		cp := make([]byte, len(buf))
		copy(cp, buf)
		payloads = append(payloads, cp)
		offsets = append(offsets, offset)
	})
	return payloads, offsets, work
}

func TestSubscriptionConsensusIngress_nilReceiver(t *testing.T) {
	var s *SubscriptionConsensusIngress
	require.Zero(t, s.Poll(10, func([]byte, int) {}))
}

func TestSubscriptionConsensusIngress_nilPoller(t *testing.T) {
	s := NewSubscriptionConsensusIngress(nil)
	require.Zero(t, s.Poll(10, func([]byte, int) {}))
}

func TestSubscriptionConsensusIngress_nilOnFragment(t *testing.T) {
	fake := &fakeFragmentPoller{
		frames: []fakeFragment{buildFakeFragment(t, []byte{1, 2, 3}, logbuffer.Unfragmented)},
	}
	s := NewSubscriptionConsensusIngress(fake)
	require.Zero(t, s.Poll(10, nil))
	require.Zero(t, fake.polls)
}

func TestSubscriptionConsensusIngress_nonPositiveLimit(t *testing.T) {
	fake := &fakeFragmentPoller{
		frames: []fakeFragment{buildFakeFragment(t, []byte{1}, logbuffer.Unfragmented)},
	}
	s := NewSubscriptionConsensusIngress(fake)
	require.Zero(t, s.Poll(0, func([]byte, int) {}))
	require.Zero(t, s.Poll(-1, func([]byte, int) {}))
	require.Zero(t, fake.polls)
}

func TestSubscriptionConsensusIngress_zeroValueSubscription(t *testing.T) {
	s := NewSubscriptionConsensusIngress(new(aeron.Subscription))
	require.Zero(t, s.Poll(10, func([]byte, int) {
		t.Fatal("onFragment must not run for zero-value subscription")
	}))
}

func TestSubscriptionConsensusIngress_oneUnfragmentedFrame(t *testing.T) {
	payload := []byte{9, 8, 7, 6, 5}
	fake := &fakeFragmentPoller{
		frames: []fakeFragment{buildFakeFragment(t, payload, logbuffer.Unfragmented)},
	}
	s := NewSubscriptionConsensusIngress(fake)
	payloads, offsets, work := recordDeliveries(t, s, 10)
	require.Equal(t, 1, work)
	require.Len(t, payloads, 1)
	require.Equal(t, payload, payloads[0])
	require.Equal(t, 0, offsets[0])
}

func TestSubscriptionConsensusIngress_deliveredSliceIsSpan(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	// Backing buffer is larger than payload (buildFakeFragment pads +64).
	fr := buildFakeFragment(t, payload, logbuffer.Unfragmented)
	require.Greater(t, len(fr.buffer.Bytes()), len(payload))
	fake := &fakeFragmentPoller{frames: []fakeFragment{fr}}
	s := NewSubscriptionConsensusIngress(fake)

	var gotLen int
	work := s.Poll(1, func(buf []byte, offset int) {
		gotLen = len(buf)
		require.Equal(t, 0, offset)
		require.Equal(t, payload, buf)
	})
	require.Equal(t, 1, work)
	require.Equal(t, len(payload), gotLen)
	require.NotEqual(t, len(fr.buffer.Bytes()), gotLen)
}

func TestSubscriptionConsensusIngress_realCanvassDecodes(t *testing.T) {
	actingVersion := uint16(clusterwire.CanvassPositionSbeSchemaVersion)
	fields := codecs.CanvassPositionFields{
		LogLeadershipTermID: 11,
		LogPosition:         22,
		LeadershipTermID:    33,
		FollowerMemberID:    4,
		ProtocolVersion:     5,
	}
	blockLen := codecs.CanvassPositionBlockLength(actingVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(blockLen)
	require.True(t, ok)
	wire := make([]byte, wireLen)
	end, ok := codecs.EncodeCanvassPosition(wire, 0, actingVersion, fields)
	require.True(t, ok)
	wire = wire[:end]

	fake := &fakeFragmentPoller{
		frames: []fakeFragment{buildFakeFragment(t, wire, logbuffer.Unfragmented)},
	}
	s := NewSubscriptionConsensusIngress(fake)

	var decoded codecs.CanvassPositionFields
	var decodedOK bool
	work := s.Poll(1, func(buf []byte, offset int) {
		decoded, decodedOK = codecs.TryDecodeCanvassPositionAt(buf, offset)
	})
	require.Equal(t, 1, work)
	require.True(t, decodedOK)
	require.Equal(t, fields, decoded)
}

func TestSubscriptionConsensusIngress_fragmentedFlagsSkipped(t *testing.T) {
	tests := []struct {
		name  string
		flags byte
	}{
		{name: "BeginFlag only", flags: logbuffer.BeginFlag},
		{name: "EndFlag only", flags: logbuffer.EndFlag},
		{name: "neither flag", flags: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeFragmentPoller{
				frames: []fakeFragment{
					buildFakeFragment(t, []byte{1, 2, 3}, tt.flags),
				},
			}
			s := NewSubscriptionConsensusIngress(fake)
			payloads, _, work := recordDeliveries(t, s, 10)
			require.Equal(t, 1, work)
			require.Empty(t, payloads)
		})
	}
}

func TestSubscriptionConsensusIngress_boundsGuards(t *testing.T) {
	tests := []struct {
		name   string
		rawLen int
		offset int
		length int
	}{
		{name: "zero length", rawLen: 64, offset: logbuffer.OffsetFramePayload, length: 0},
		{name: "offset past buffer", rawLen: 40, offset: 100, length: 4},
		{name: "offset plus length past buffer", rawLen: 40, offset: 36, length: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeFragmentPoller{
				frames: []fakeFragment{
					buildFakeFragmentAt(t, tt.rawLen, tt.offset, tt.length, logbuffer.Unfragmented),
				},
			}
			s := NewSubscriptionConsensusIngress(fake)
			payloads, _, work := recordDeliveries(t, s, 10)
			require.Equal(t, 1, work)
			require.Empty(t, payloads)
		})
	}
}

func TestSubscriptionConsensusIngress_limitHonoured(t *testing.T) {
	frames := make([]fakeFragment, 5)
	for i := range frames {
		frames[i] = buildFakeFragment(t, []byte{byte(i)}, logbuffer.Unfragmented)
	}
	fake := &fakeFragmentPoller{frames: frames}
	s := NewSubscriptionConsensusIngress(fake)
	payloads, _, work := recordDeliveries(t, s, 3)
	require.Equal(t, 3, work)
	require.Len(t, payloads, 3)
	require.Equal(t, []byte{0}, payloads[0])
	require.Equal(t, []byte{1}, payloads[1])
	require.Equal(t, []byte{2}, payloads[2])
}

func TestSubscriptionConsensusIngress_handlerClearedAfterPoll(t *testing.T) {
	frames := []fakeFragment{
		buildFakeFragment(t, []byte{1}, logbuffer.Unfragmented),
		buildFakeFragment(t, []byte{2}, logbuffer.Unfragmented),
	}
	fake := &fakeFragmentPoller{frames: frames}
	s := NewSubscriptionConsensusIngress(fake)

	first, _, work := recordDeliveries(t, s, 1)
	require.Equal(t, 1, work)
	require.Equal(t, [][]byte{{1}}, first)
	require.Nil(t, s.onFragment)

	second, _, work := recordDeliveries(t, s, 1)
	require.Equal(t, 1, work)
	require.Equal(t, [][]byte{{2}}, second)
	require.Equal(t, [][]byte{{1}}, first)
	require.Nil(t, s.onFragment)
}

func TestSubscriptionConsensusIngress_handlerBoundOnce(t *testing.T) {
	fake := &fakeFragmentPoller{
		frames: []fakeFragment{
			buildFakeFragment(t, []byte{1}, logbuffer.Unfragmented),
			buildFakeFragment(t, []byte{2}, logbuffer.Unfragmented),
		},
	}
	s := NewSubscriptionConsensusIngress(fake)

	require.Equal(t, 1, s.Poll(1, func([]byte, int) {}))
	require.Equal(t, 1, s.Poll(1, func([]byte, int) {}))
	require.Len(t, fake.handlers, 2)
	// Function values are not comparable; compare code pointers.
	require.Equal(
		t,
		reflect.ValueOf(fake.handlers[0]).Pointer(),
		reflect.ValueOf(fake.handlers[1]).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(s.handler).Pointer(),
		reflect.ValueOf(fake.handlers[0]).Pointer(),
	)
}

func TestSubscriptionConsensusIngress_panickingConsumerClearsHandler(t *testing.T) {
	fake := &fakeFragmentPoller{
		frames: []fakeFragment{
			buildFakeFragment(t, []byte{1}, logbuffer.Unfragmented),
			buildFakeFragment(t, []byte{2}, logbuffer.Unfragmented),
		},
	}
	s := NewSubscriptionConsensusIngress(fake)

	require.Panics(t, func() {
		s.Poll(1, func([]byte, int) {
			panic("consumer")
		})
	})
	require.Nil(t, s.onFragment)

	got, _, work := recordDeliveries(t, s, 1)
	require.Equal(t, 1, work)
	require.Equal(t, [][]byte{{2}}, got)
	require.Nil(t, s.onFragment)
}

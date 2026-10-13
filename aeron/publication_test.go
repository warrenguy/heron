package aeron_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestPublication_Offer_SingleFrame(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	payload := []byte("hello-publication")
	position := pub.Offer(payload)
	require.Positive(t, position)

	_, termBuf := aeron.PublicationActiveTermBufferForTest(pub)
	wantFrameLength := int32(len(payload) + logbuffer.DataHeaderLength)
	require.Equal(t, wantFrameLength, logbuffer.FrameLengthVolatile(termBuf, 0))
	payloadEnd := logbuffer.OffsetFramePayload + len(payload)
	gotPayload := termBuf.Bytes()[logbuffer.OffsetFramePayload:payloadEnd]
	require.Equal(t, payload, gotPayload)
}

func TestPublication_Offer_goldenHeaderBytes(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	payload := []byte{0x01, 0x02, 0x03, 0x04}
	position := pub.Offer(payload)
	wantPosition := int64(logbuffer.AlignFrameLength(len(payload) + logbuffer.DataHeaderLength))
	require.Equal(t, wantPosition, position)

	_, termBuf := aeron.PublicationActiveTermBufferForTest(pub)
	want := []byte{
		0x24, 0x00, 0x00, 0x00,
		0x00,
		0xC0,
		0x01, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0xE9, 0x03, 0x00, 0x00,
		0xD2, 0x07, 0x00, 0x00,
		0x07, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	require.Equal(t, want, termBuf.Bytes()[:logbuffer.DataHeaderLength])
}

func TestPublication_OfferWithOffset(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	payload := []byte("xxpayload")
	position := pub.OfferWithOffset(payload, 2, len(payload)-2)
	require.Positive(t, position)

	_, termBuf := aeron.PublicationActiveTermBufferForTest(pub)
	gotPayload := termBuf.Bytes()[logbuffer.OffsetFramePayload : logbuffer.OffsetFramePayload+7]
	require.Equal(t, []byte("payload"), gotPayload)
}

func TestPublication_OfferWithOffset_closedBeforeBounds(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	require.NoError(t, pub.Close())

	got := pub.OfferWithOffset([]byte{1}, 99, 1)
	require.Equal(t, aeron.Closed, got)
}

func TestPublication_TryClaim_closedBeforeValidation(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	maxPayload := pub.MaxPayloadLengthForTest()
	require.NoError(t, pub.Close())

	termIndex, frameOffset, position, err := pub.TryClaim(-1)
	require.Equal(t, 0, termIndex)
	require.Equal(t, 0, frameOffset)
	require.Equal(t, aeron.Closed, position)
	require.NoError(t, err)

	termIndex, frameOffset, position, err = pub.TryClaim(maxPayload + 1)
	require.Equal(t, 0, termIndex)
	require.Equal(t, 0, frameOffset)
	require.Equal(t, aeron.Closed, position)
	require.NoError(t, err)
}

func TestPublication_OfferWithOffset_invalidBounds(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	buf := []byte{1, 2, 3}

	cases := []struct {
		name   string
		offset int
		length int
		panic  string
	}{
		{"negative offset", -1, 1, "aeron: invalid offer offset=-1"},
		{"offset past buffer", 4, 1, "aeron: invalid offer offset=4 buffer=3"},
		{"length past buffer", 0, 4, "aeron: invalid offer offset=0 length=4 buffer=3"},
		{"negative length", 0, -1, "aeron: invalid message length=-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, tc.panic, func() {
				pub.OfferWithOffset(buf, tc.offset, tc.length)
			})
		})
	}
}

func injectPublicationCounterError(
	tb testing.TB,
	pub *aeron.Publication,
	counterID int32,
	errMsg string,
) {
	tb.Helper()
	aeron.SetPublicationCounterValueErrorForTest(
		tb,
		pub,
		counterID,
		errors.New(errMsg),
	)
}

func setupPublicationWithLimitCounterValueFailure(tb testing.TB) *aeron.Publication {
	tb.Helper()

	pub, _ := setupPublicationWithLimitReader(tb, 1_000_000)
	injectPublicationCounterError(
		tb,
		pub,
		aeron.TestPublicationLimitCounterID,
		"aeron: injected limit counter failure",
	)
	return pub
}

func setupPublicationWithLimitReader(
	tb testing.TB,
	limit int64,
) (*aeron.Publication, *cnc.CountersReader) {
	tb.Helper()

	reader := aeron.NewTestCountersReaderForTest(tb, limit)
	pub := aeron.NewTestPublicationWithCounters(tb, aeron.DefaultTestPublicationLogConfig(), reader)
	return pub, reader
}

func setupPublicationWithLimitReaderAtPosition(
	tb testing.TB,
	limit int64,
	position int64,
) (*aeron.Publication, *cnc.CountersReader) {
	tb.Helper()

	reader := aeron.NewTestCountersReaderForTest(tb, limit)
	cfg := publicationLogConfigAtPosition(position)
	pub := aeron.NewTestPublicationWithCounters(tb, cfg, reader)
	return pub, reader
}

func writeTestPublicationLimit(tb testing.TB, reader *cnc.CountersReader, limit int64) {
	tb.Helper()
	require.NoError(tb, reader.SetCounterValue(aeron.TestPublicationLimitCounterID, limit))
}

func offerThenDecreaseLimitBelowPosition(
	tb testing.TB,
	pub *aeron.Publication,
	reader *cnc.CountersReader,
) (position, decreasedLimit int64) {
	tb.Helper()

	position = pub.Offer([]byte{1, 2, 3, 4})
	require.Positive(tb, position)
	decreasedLimit = position - 1
	writeTestPublicationLimit(tb, reader, decreasedLimit)
	return position, decreasedLimit
}

func requirePublicationLimitSlotMatchesCounter(
	tb testing.TB,
	pub *aeron.Publication,
	reader *cnc.CountersReader,
) {
	tb.Helper()

	gotViaSlot := aeron.PublicationLimitSlotForTest(pub)
	gotViaCounter, err := reader.CounterValue(aeron.TestPublicationLimitCounterID)
	require.NoError(tb, err)
	require.Equal(tb, gotViaCounter, gotViaSlot)
}

func requireRejectedOfferLeavesTermClean(
	tb testing.TB,
	pub *aeron.Publication,
	wantStatus int64,
	offer func() int64,
) {
	tb.Helper()

	beforeCount := aeron.PublicationActiveTermCountForTest(pub)
	beforePosition := pub.Position()
	termOffset := int(aeron.LogTermOffsetAtPositionForTest(beforePosition))
	_, termBuf := aeron.PublicationActiveTermBufferForTest(pub)
	require.Equal(tb, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))

	got := offer()
	require.Equal(tb, wantStatus, got)
	require.Equal(tb, beforePosition, pub.Position())
	require.Equal(tb, beforeCount, aeron.PublicationActiveTermCountForTest(pub))
	require.Equal(tb, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))
}

func publicationLogConfigAtPosition(position int64) aeron.TestPublicationLogConfig {
	return aeron.LogConfigAtPositionForTest(aeron.DefaultTestPublicationLogConfig, position)
}

func publicationLogConfigNearMaxPosition() aeron.TestPublicationLogConfig {
	return aeron.LogConfigNearMaxPositionForTest(aeron.DefaultTestPublicationLogConfig)
}

func publicationPositionNearMaxPosition() int64 {
	return aeron.PositionNearMaxPositionForTest()
}

func publicationLogConfigWithTermRemaining(remaining int32) aeron.TestPublicationLogConfig {
	return aeron.LogConfigWithTermRemainingForTest(aeron.DefaultTestPublicationLogConfig, remaining)
}

func publicationPositionWithTermRemaining(remaining int32) int64 {
	return aeron.PositionWithTermRemainingForTest(remaining)
}

func TestPublication_Offer_payloadLengthBoundary(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	maxPayload := pub.MaxPayloadLengthForTest()
	maxMessage := pub.MaxMessageLengthForTest()

	require.Positive(t, pub.Offer(make([]byte, maxPayload)))

	fragmentedPayload := make([]byte, maxPayload+1)
	for i := range fragmentedPayload {
		fragmentedPayload[i] = byte(i)
	}
	fragmentedPosition := pub.Offer(fragmentedPayload)
	require.Positive(t, fragmentedPosition)

	frames := readFragmentedFramesAtPosition(
		t,
		pub,
		fragmentedPosition,
		maxPayload+1,
		maxPayload,
	)
	require.Len(t, frames, 2)
	requireFragmentFlags(t, frames)
	requireReassembledPayload(t, frames, fragmentedPayload)

	require.Positive(t, pub.Offer(make([]byte, maxMessage)))

	beforePosition := pub.Position()
	require.Equal(t, aeron.MaxPositionExceeded, pub.Offer(make([]byte, maxMessage+1)))
	require.Equal(t, beforePosition, pub.Position())
}

func TestPublication_TryClaim_payloadLengthBoundary(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	maxPayload := pub.MaxPayloadLengthForTest()

	_, _, position, err := pub.TryClaim(maxPayload)
	require.NoError(t, err)
	require.Positive(t, position)

	_, _, _, err = pub.TryClaim(maxPayload + 1)
	require.EqualError(
		t,
		err,
		"aeron: claim length 1377 exceeds maxPayloadLength=1376",
	)
}

func TestPublication_LimitCounter(t *testing.T) {
	cases := []struct {
		name          string
		limit         int64
		position      int64
		want          int64
		offerProceeds bool
	}{
		{
			name:          "under limit proceeds",
			limit:         1_000_000,
			position:      500_000,
			offerProceeds: true,
		},
		{
			name:     "at limit back pressured",
			limit:    500_000,
			position: 500_000,
			want:     aeron.BackPressured,
		},
		{
			name:     "past limit back pressured",
			limit:    500_000,
			position: 600_000,
			want:     aeron.BackPressured,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := publicationLogConfigAtPosition(tc.position)
			pub := aeron.NewTestPublication(t, cfg, tc.limit)

			got := pub.Offer([]byte{1, 2, 3})
			if tc.offerProceeds {
				require.Positive(t, got)
				return
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestPublication_TryClaim_backPressureStatuses(t *testing.T) {
	cases := []struct {
		name      string
		limit     int64
		position  int64
		connected bool
		want      int64
	}{
		{
			name:      "under limit proceeds",
			limit:     1_000_000,
			position:  500_000,
			connected: true,
			want:      0,
		},
		{
			name:      "at limit back pressured",
			limit:     500_000,
			position:  500_000,
			connected: true,
			want:      aeron.BackPressured,
		},
		{
			name:      "past limit back pressured",
			limit:     500_000,
			position:  600_000,
			connected: true,
			want:      aeron.BackPressured,
		},
		{
			name:      "not connected",
			limit:     0,
			position:  0,
			connected: false,
			want:      aeron.NotConnected,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := publicationLogConfigAtPosition(tc.position)
			if !tc.connected {
				cfg.IsConnected = 0
			}
			pub := aeron.NewTestPublication(t, cfg, tc.limit)

			_, _, position, err := pub.TryClaim(8)
			require.NoError(t, err)
			if tc.want == 0 {
				require.Positive(t, position)
				return
			}
			require.Equal(t, tc.want, position)
		})
	}
}

func TestPublication_NotConnectedVsBackPressured(t *testing.T) {
	t.Run("disconnected at limit", func(t *testing.T) {
		cfg := aeron.DefaultTestPublicationLogConfig()
		cfg.IsConnected = 0
		pub := aeron.NewTestPublication(t, cfg, 0)

		got := pub.Offer([]byte{1})
		require.Equal(t, aeron.NotConnected, got)
	})

	t.Run("connected at limit", func(t *testing.T) {
		cfg := aeron.DefaultTestPublicationLogConfig()
		cfg.IsConnected = 1
		pub := aeron.NewTestPublication(t, cfg, 0)

		got := pub.Offer([]byte{1})
		require.Equal(t, aeron.BackPressured, got)
	})

	t.Run("disconnected under limit proceeds", func(t *testing.T) {
		cfg := aeron.DefaultTestPublicationLogConfig()
		cfg.IsConnected = 0
		pub := aeron.NewTestPublication(t, cfg, 1_000_000)

		require.Positive(t, pub.Offer([]byte{1, 2, 3}))
	})
}

func TestPublication_TryClaim_maxPositionExceeded(t *testing.T) {
	pub := aeron.NewTestPublication(t, publicationLogConfigNearMaxPosition(), int64(1)<<62)
	beforeCount := aeron.PublicationActiveTermCountForTest(pub)

	_, _, position, err := pub.TryClaim(64)
	require.NoError(t, err)
	require.Equal(t, aeron.MaxPositionExceeded, position)
	require.Equal(t, beforeCount, aeron.PublicationActiveTermCountForTest(pub))
}

func TestPublication_TryClaim_maxPositionExceeded_preFAA(t *testing.T) {
	position := publicationPositionNearMaxPosition()
	pub := aeron.NewTestPublication(t, publicationLogConfigNearMaxPosition(), position)
	beforeCount := aeron.PublicationActiveTermCountForTest(pub)

	termOffset := int(aeron.LogTermOffsetAtPositionForTest(position))
	_, termBuf := aeron.PublicationActiveTermBufferForTest(pub)
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))

	_, _, got, err := pub.TryClaim(8)
	require.NoError(t, err)
	require.Equal(t, aeron.MaxPositionExceeded, got)
	require.Equal(t, beforeCount, aeron.PublicationActiveTermCountForTest(pub))
	require.Equal(t, position, pub.Position())
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))
}

func TestPublication_TryClaim_negativeLength(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	termIndex, frameOffset, position, err := pub.TryClaim(-1)
	require.Equal(t, 0, termIndex)
	require.Equal(t, 0, frameOffset)
	require.Equal(t, int64(0), position)
	require.EqualError(t, err, "aeron: invalid message length=-1")
}

func TestPublication_MaxPositionExceeded(t *testing.T) {
	pub := aeron.NewTestPublication(t, publicationLogConfigNearMaxPosition(), int64(1)<<62)
	beforeCount := aeron.PublicationActiveTermCountForTest(pub)

	got := pub.Offer(make([]byte, 64))
	require.Equal(t, aeron.MaxPositionExceeded, got)
	require.Equal(t, beforeCount, aeron.PublicationActiveTermCountForTest(pub))
}

func TestPublication_MaxPositionExceeded_highLimitExactTermFill(t *testing.T) {
	const remaining int32 = 64
	cfg := publicationLogConfigWithTermRemaining(remaining)
	position := publicationPositionWithTermRemaining(remaining)
	pub := aeron.NewTestPublication(t, cfg, 1<<62)

	termOffset := int(aeron.LogTermOffsetAtPositionForTest(position))
	_, termBuf := aeron.PublicationActiveTermBufferForTest(pub)
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))

	got := pub.Offer([]byte{1})
	require.Equal(t, aeron.MaxPositionExceeded, got)
	require.Equal(t, position, pub.Position())
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))
}

func TestPublication_MaxPositionExceeded_preFAA(t *testing.T) {
	position := publicationPositionNearMaxPosition()
	pub := aeron.NewTestPublication(t, publicationLogConfigNearMaxPosition(), position)
	beforeCount := aeron.PublicationActiveTermCountForTest(pub)

	termOffset := int(aeron.LogTermOffsetAtPositionForTest(position))
	_, termBuf := aeron.PublicationActiveTermBufferForTest(pub)
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))

	got := pub.Offer([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	require.Equal(t, aeron.MaxPositionExceeded, got)
	require.Equal(t, beforeCount, aeron.PublicationActiveTermCountForTest(pub))
	require.Equal(t, position, pub.Position())
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))
}

func TestPublication_FragmentedOffer_statusPaths(t *testing.T) {
	fragmentedLength := func(pub *aeron.Publication) int {
		return pub.MaxPayloadLengthForTest() + 1
	}

	t.Run("BackPressured", func(t *testing.T) {
		pub, _ := setupPublicationWithLimitReaderAtPosition(t, 500_000, 500_000)
		payload := make([]byte, fragmentedLength(pub))
		requireRejectedOfferLeavesTermClean(t, pub, aeron.BackPressured, func() int64 {
			return pub.Offer(payload)
		})
	})

	t.Run("NotConnected", func(t *testing.T) {
		cfg := aeron.DefaultTestPublicationLogConfig()
		cfg.IsConnected = 0
		pub := aeron.NewTestPublication(t, cfg, 0)
		payload := make([]byte, fragmentedLength(pub))
		requireRejectedOfferLeavesTermClean(t, pub, aeron.NotConnected, func() int64 {
			return pub.Offer(payload)
		})
	})

	t.Run("AdminAction term mismatch", func(t *testing.T) {
		pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
		aeron.SetPublicationActiveTermCountForTest(pub, 1)
		aeron.SetPublicationRawTailForTest(
			pub,
			logbuffer.IndexByTermCount(1),
			logbuffer.PackRawTail(aeron.TestPublicationInitialTermID, 0),
		)
		payload := make([]byte, fragmentedLength(pub))
		requireRejectedOfferLeavesTermClean(t, pub, aeron.AdminAction, func() int64 {
			return pub.Offer(payload)
		})
	})

	t.Run("Closed", func(t *testing.T) {
		pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
		require.NoError(t, pub.Close())
		got := pub.Offer(make([]byte, fragmentedLength(pub)))
		require.Equal(t, aeron.Closed, got)
	})

	t.Run("MaxMessageLength after prelude", func(t *testing.T) {
		pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<40)
		maxMessage := pub.MaxMessageLengthForTest()
		payload := make([]byte, maxMessage+1)
		requireRejectedOfferLeavesTermClean(t, pub, aeron.MaxPositionExceeded, func() int64 {
			return pub.Offer(payload)
		})
	})
}

func TestPublication_MaxPositionExceeded_fragmented(t *testing.T) {
	pub := aeron.NewTestPublication(t, publicationLogConfigNearMaxPosition(), int64(1)<<62)
	maxPayload := pub.MaxPayloadLengthForTest()
	requireRejectedOfferLeavesTermClean(t, pub, aeron.MaxPositionExceeded, func() int64 {
		return pub.Offer(make([]byte, maxPayload+1))
	})
}

// Asserts fragmented Offer returns BackPressured when remaining term space
// exceeds single-frame alignment for maxPayload+1 but not the full framed
// reservation. A max-position check on framed length alone would return
// MaxPositionExceeded. Covers concurrent and exclusive publications.
func TestFragmentedOffer_backPressuredNearMaxPosition(t *testing.T) {
	const remaining int32 = 1450
	position := publicationPositionWithTermRemaining(remaining)

	t.Run("concurrent", func(t *testing.T) {
		pub := aeron.NewTestPublication(t, publicationLogConfigWithTermRemaining(remaining), position)
		got := pub.Offer(make([]byte, pub.MaxPayloadLengthForTest()+1))
		require.Equal(t, aeron.BackPressured, got)
	})

	t.Run("exclusive", func(t *testing.T) {
		ep := aeron.NewTestExclusivePublication(
			t,
			aeron.LogConfigWithTermRemainingForTest(
				aeron.DefaultTestExclusivePublicationLogConfig,
				remaining,
			),
			position,
		)
		got := ep.Offer(make([]byte, ep.MaxPayloadLengthForTest()+1))
		require.Equal(t, aeron.BackPressured, got)
	})
}

func TestPublication_AdminAction_termCountMismatch(t *testing.T) {
	setup := func(tb testing.TB) *aeron.Publication {
		tb.Helper()
		pub := aeron.NewTestPublication(tb, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
		aeron.SetPublicationActiveTermCountForTest(pub, 1)
		aeron.SetPublicationRawTailForTest(
			pub,
			logbuffer.IndexByTermCount(1),
			logbuffer.PackRawTail(7, 0),
		)
		return pub
	}

	t.Run("Offer", func(t *testing.T) {
		pub := setup(t)
		got := pub.Offer([]byte{1, 2, 3})
		require.Equal(t, aeron.AdminAction, got)
	})

	t.Run("TryClaim", func(t *testing.T) {
		pub := setup(t)
		_, _, position, err := pub.TryClaim(8)
		require.NoError(t, err)
		require.Equal(t, aeron.AdminAction, position)
	})
}

func TestPublication_Closed(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	require.NoError(t, pub.Close())

	require.Equal(t, aeron.Closed, pub.Offer([]byte{1}))
	_, _, position, err := pub.TryClaim(8)
	require.Equal(t, aeron.Closed, position)
	require.NoError(t, err)
	require.Equal(t, aeron.Closed, pub.Position())
	require.Equal(t, aeron.Closed, pub.PositionLimit())
	require.False(t, pub.IsConnected())
	require.Equal(t, aeron.Closed, pub.AvailableWindow())

	_, err = pub.ChannelStatus()
	require.EqualError(t, err, "aeron: publication is closed")
}

func TestPublication_Position_tracksOffer(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	require.Equal(t, int64(0), pub.Position())

	first := pub.Offer([]byte{1, 2, 3, 4})
	require.Positive(t, first)
	require.Equal(t, first, pub.Position())

	second := pub.Offer([]byte{5, 6, 7, 8})
	require.Positive(t, second)
	require.Greater(t, second, first)
	require.Equal(t, second, pub.Position())
}

func TestPublication_Offer_emptyMessage(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	position := pub.Offer(nil)
	require.Equal(t, int64(logbuffer.DataHeaderLength), position)
}

func TestPublication_TryClaim_Commit(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	termIndex, frameOffset, position, err := pub.TryClaim(16)
	require.NoError(t, err)
	require.Positive(t, position)
	require.GreaterOrEqual(t, frameOffset, 0)

	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("claim-payload-16b"))

	require.NoError(t, pub.Commit(termIndex, frameOffset))
	require.Equal(
		t,
		int32(16+logbuffer.DataHeaderLength),
		logbuffer.FrameLengthVolatile(termBuf, frameOffset),
	)
}

func TestPublication_TryClaim_Abort(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	termIndex, frameOffset, _, err := pub.TryClaim(8)
	require.NoError(t, err)

	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	require.NoError(t, pub.Abort(termIndex, frameOffset))
	require.True(t, logbuffer.IsPaddingFrame(termBuf, frameOffset))
	require.Equal(
		t,
		int32(8+logbuffer.DataHeaderLength),
		logbuffer.FrameLengthVolatile(termBuf, frameOffset),
	)
}

func TestPublication_ClaimFinalize_errors(t *testing.T) {
	cases := []struct {
		name    string
		op      func(*aeron.Publication) error
		setup   func(*aeron.Publication)
		wantErr string
	}{
		{
			name:    "Commit not in flight",
			op:      func(pub *aeron.Publication) error { return pub.Commit(0, 0) },
			wantErr: "aeron: claimed space not in flight",
		},
		{
			name:    "Abort not in flight",
			op:      func(pub *aeron.Publication) error { return pub.Abort(0, 0) },
			wantErr: "aeron: claimed space not in flight",
		},
		{
			name:    "Commit invalid term index",
			op:      func(pub *aeron.Publication) error { return pub.Commit(3, 0) },
			wantErr: "aeron: invalid term index 3",
		},
		{
			name:    "Abort invalid term index",
			op:      func(pub *aeron.Publication) error { return pub.Abort(3, 0) },
			wantErr: "aeron: invalid term index 3",
		},
		{
			name:    "Commit invalid frame offset",
			op:      func(pub *aeron.Publication) error { return pub.Commit(0, -1) },
			wantErr: "aeron: invalid frame offset -1",
		},
		{
			name:    "Abort invalid frame offset",
			op:      func(pub *aeron.Publication) error { return pub.Abort(0, -1) },
			wantErr: "aeron: invalid frame offset -1",
		},
		{
			name: "Commit frame offset past term",
			op: func(pub *aeron.Publication) error {
				capacity := aeron.PublicationTermBufferForTest(pub, 0).Capacity()
				return pub.Commit(0, capacity-logbuffer.DataHeaderLength+1)
			},
			wantErr: fmt.Sprintf(
				"aeron: invalid frame offset %d",
				aeron.TestPublicationTermLength-logbuffer.DataHeaderLength+1,
			),
		},
		{
			name: "Commit closed",
			op:   func(pub *aeron.Publication) error { return pub.Commit(0, 0) },
			setup: func(pub *aeron.Publication) {
				require.NoError(t, pub.Close())
			},
			wantErr: "aeron: publication is closed",
		},
		{
			name: "Abort closed",
			op:   func(pub *aeron.Publication) error { return pub.Abort(0, 0) },
			setup: func(pub *aeron.Publication) {
				require.NoError(t, pub.Close())
			},
			wantErr: "aeron: publication is closed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
			if tc.setup != nil {
				tc.setup(pub)
			}
			require.EqualError(t, tc.op(pub), tc.wantErr)
		})
	}
}

func TestPublication_IsConnected(t *testing.T) {
	cfg := aeron.DefaultTestPublicationLogConfig()
	cfg.IsConnected = 1
	pub := aeron.NewTestPublication(t, cfg, 1_000_000)
	require.True(t, pub.IsConnected())

	cfg.IsConnected = 0
	pub = aeron.NewTestPublication(t, cfg, 1_000_000)
	require.False(t, pub.IsConnected())
}

func TestPublication_ChannelStatus(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	status, err := pub.ChannelStatus()
	require.NoError(t, err)
	require.Equal(t, int64(1), status)
}

func TestPublication_ChannelStatus_counterFailure(t *testing.T) {
	pub, _ := setupPublicationWithLimitReader(t, 1_000_000)
	injectPublicationCounterError(
		t,
		pub,
		aeron.TestPublicationChannelCounterID,
		"aeron: injected channel counter failure",
	)

	_, err := pub.ChannelStatus()
	require.EqualError(t, err, "aeron: injected channel counter failure")
}

func TestPublication_AvailableWindow(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	require.Equal(t, int64(1_000_000), pub.AvailableWindow())

	position := pub.Offer([]byte{1, 2, 3, 4})
	require.Positive(t, position)
	require.Equal(t, int64(1_000_000)-position, pub.AvailableWindow())
}

func TestPublication_AvailableWindow_atLimit(t *testing.T) {
	pub, _ := setupPublicationWithLimitReaderAtPosition(t, 500_000, 500_000)
	require.Equal(t, int64(0), pub.AvailableWindow())
}

func TestPublication_limitCounterFailure_coldAccessorsReturnClosed(t *testing.T) {
	pub := setupPublicationWithLimitCounterValueFailure(t)
	require.Equal(t, aeron.Closed, pub.AvailableWindow())
	require.Equal(t, aeron.Closed, pub.PositionLimit())
}

func TestPublication_limitCounterValueFailure_hotPathUsesBoundSlot(t *testing.T) {
	// Hot path reads bound limit slot; CounterValue injection hits cold accessors only.
	pub := setupPublicationWithLimitCounterValueFailure(t)
	require.Equal(t, aeron.Closed, pub.PositionLimit())
	require.Equal(t, aeron.Closed, pub.AvailableWindow())
	require.Positive(t, pub.Offer([]byte{1}))

	_, _, position, err := pub.TryClaim(16)
	require.NoError(t, err)
	require.Positive(t, position)

	maxPayload := pub.MaxPayloadLengthForTest()
	require.Positive(t, pub.Offer(make([]byte, maxPayload+1)))
}

func TestPublication_publicationLimitSlot_matchesCounterValueAfterBind(t *testing.T) {
	cases := []struct {
		name  string
		limit int64
	}{
		{name: "small limit", limit: 128},
		{name: "large limit", limit: 1_000_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub, reader := setupPublicationWithLimitReader(t, tc.limit)
			requirePublicationLimitSlotMatchesCounter(t, pub, reader)
		})
	}

	t.Run("live update tracks driver write", func(t *testing.T) {
		const updatedLimit int64 = 2_000_000
		pub, reader := setupPublicationWithLimitReader(t, 1_000_000)
		writeTestPublicationLimit(t, reader, updatedLimit)
		require.Equal(t, updatedLimit, pub.PositionLimit())
	})

	t.Run("live limit decrease blocks Offer and updates AvailableWindow", func(t *testing.T) {
		pub, reader := setupPublicationWithLimitReader(t, 1_000_000)
		position, decreasedLimit := offerThenDecreaseLimitBelowPosition(t, pub, reader)

		requireRejectedOfferLeavesTermClean(t, pub, aeron.BackPressured, func() int64 {
			return pub.Offer([]byte{5, 6, 7, 8})
		})
		require.Equal(t, decreasedLimit-position, pub.AvailableWindow())
	})

	t.Run("fragmented Offer at limit", func(t *testing.T) {
		const limit int64 = 500_000
		pub, _ := setupPublicationWithLimitReaderAtPosition(t, limit, limit)
		payload := make([]byte, pub.MaxPayloadLengthForTest()+1)
		requireRejectedOfferLeavesTermClean(t, pub, aeron.BackPressured, func() int64 {
			return pub.Offer(payload)
		})
	})
}

func TestNewPublicationFromReadyForTest_validation(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	ready := aeron.PublicationBuffersReadyForTest()
	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)

	_, err := aeron.NewPublicationFromReadyForTest(ready, nil, counters, "aeron:ipc")
	require.EqualError(t, err, "aeron: publication log buffer is nil")

	_, err = aeron.NewPublicationFromReadyForTest(ready, log, nil, "aeron:ipc")
	require.EqualError(t, err, "aeron: publication counters view is nil")

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	emptyReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	_, err = aeron.NewPublicationFromReadyForTest(ready, log, emptyReader, "aeron:ipc")
	require.EqualError(
		t,
		err,
		"aeron: publication limit counter 1 is not active for registration 9001",
	)
}

func newIPCPublicationWithNullChannelCounter(t *testing.T) *aeron.Publication {
	t.Helper()

	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	ready := aeron.PublicationBuffersReadyForTest()
	ready.ChannelStatusCounterID = cnc.NullCounterID
	counters := aeron.NewTestCountersReaderLimitOnlyForTest(t, 1_000_000)

	pub, err := aeron.NewPublicationFromReadyForTest(ready, log, counters, "aeron:ipc")
	require.NoError(t, err)
	require.NotNil(t, pub)
	return pub
}

func TestPublication_MaxPayloadLength(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	want := int(aeron.TestPublicationMTULength) - logbuffer.DataHeaderLength
	require.Equal(t, want, pub.MaxPayloadLength())
}

func TestPublication_StreamIDSessionID(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	require.Equal(t, aeron.TestPublicationStreamID, pub.StreamID())
	require.Equal(t, aeron.TestPublicationSessionID, pub.SessionID())

	require.NoError(t, pub.Close())
	require.Equal(t, aeron.TestPublicationStreamID, pub.StreamID())
	require.Equal(t, aeron.TestPublicationSessionID, pub.SessionID())
}

func TestPublication_nullChannelStatusCounter(t *testing.T) {
	pub := newIPCPublicationWithNullChannelCounter(t)

	status, err := pub.ChannelStatus()
	require.NoError(t, err)
	require.Equal(t, aeron.ChannelEndpointStatusActive, status)
}

func TestNewPublicationFromReadyForTest_wrongLogType(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestExclusivePublicationLogConfig())
	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)

	_, err := aeron.NewPublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		counters,
		"aeron:ipc",
	)
	require.EqualError(t, err, "aeron: expected concurrent publication log type 0, got 1")
}

func TestNewPublicationFromReadyForTest_invalidMTU(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })
	meta, err := log.MetaDataBuffer()
	require.NoError(t, err)
	meta.PutInt32(logbuffer.OffsetMTULength, logbuffer.DataHeaderLength)

	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)
	_, err = aeron.NewPublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		counters,
		"aeron:ipc",
	)
	require.EqualError(t, err, "logbuffer: mtu length out of range: 32")
}

func TestNewPublicationFromReadyForTest_inactiveChannelCounter(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	reader := aeron.NewTestCountersReaderLimitOnlyForTest(t, 1_000_000)

	_, err := aeron.NewPublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		reader,
		"aeron:ipc",
	)
	require.EqualError(
		t,
		err,
		"aeron: channel status counter 2 is not allocated",
	)
}

func TestNewPublicationFromReadyForTest_reusedEndpointChannelCounter(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	reader := aeron.NewTestCountersReaderMismatchedChannelStatusForTest(
		t,
		1_000_000,
		aeron.TestPublicationRegistrationID+1,
	)

	pub, err := aeron.NewPublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		reader,
		"aeron:ipc",
	)
	require.NoError(t, err)
	require.NotNil(t, pub)
}

func TestNewPublicationFromReadyForTest_invalidMetadata(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	meta, err := log.MetaDataBuffer()
	require.NoError(t, err)
	meta.PutInt32(logbuffer.OffsetTermLength, 1000)

	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)

	_, err = aeron.NewPublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		counters,
		"aeron:ipc",
	)
	require.EqualError(t, err, "logbuffer: term length below minimum: 1000 < 65536")
}

func TestNewPublicationFromReadyForTest_closedLog(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	require.NoError(t, log.Close())

	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)

	_, err := aeron.NewPublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		counters,
		"aeron:ipc",
	)
	require.EqualError(t, err, "logbuffer: closed mapped log buffer")
}

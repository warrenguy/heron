package aeron_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func setupExclusivePublicationWithLimitReader(
	tb testing.TB,
	limit int64,
) (*aeron.ExclusivePublication, *cnc.CountersReader) {
	tb.Helper()

	reader := aeron.NewTestCountersReaderForTest(tb, limit)
	ep := aeron.NewTestExclusivePublicationWithCounters(
		tb,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		reader,
	)
	return ep, reader
}

func setupExclusivePublicationWithLimitReaderAtPosition(
	tb testing.TB,
	limit int64,
	position int64,
) (*aeron.ExclusivePublication, *cnc.CountersReader) {
	tb.Helper()

	reader := aeron.NewTestCountersReaderForTest(tb, limit)
	cfg := aeron.LogConfigAtPositionForTest(aeron.DefaultTestExclusivePublicationLogConfig, position)
	ep := aeron.NewTestExclusivePublicationWithCounters(tb, cfg, reader)
	return ep, reader
}

func setupExclusiveWithLimitCounterFailure(tb testing.TB) *aeron.ExclusivePublication {
	tb.Helper()

	ep, _ := setupExclusivePublicationWithLimitReader(tb, 1_000_000)
	injectExclusivePublicationCounterError(
		tb,
		ep,
		aeron.TestPublicationLimitCounterID,
		"aeron: injected limit counter failure",
	)
	return ep
}

func writeTestExclusivePublicationLimit(tb testing.TB, reader *cnc.CountersReader, limit int64) {
	tb.Helper()
	require.NoError(tb, reader.SetCounterValue(aeron.TestPublicationLimitCounterID, limit))
}

func offerThenDecreaseExclusiveLimitBelowPosition(
	tb testing.TB,
	ep *aeron.ExclusivePublication,
	reader *cnc.CountersReader,
) (position, decreasedLimit int64) {
	tb.Helper()

	position = ep.Offer([]byte{1, 2, 3, 4})
	require.Positive(tb, position)
	decreasedLimit = position - 1
	writeTestExclusivePublicationLimit(tb, reader, decreasedLimit)
	return position, decreasedLimit
}

func requireRejectedExclusiveOfferLeavesTermClean(
	tb testing.TB,
	ep *aeron.ExclusivePublication,
	wantStatus int64,
	offer func() int64,
) {
	tb.Helper()

	beforeCount := aeron.ExclusivePublicationActiveTermCountForTest(ep)
	beforePosition := ep.Position()
	termOffset := int(aeron.LogTermOffsetAtPositionForTest(beforePosition))
	_, termBuf := aeron.ExclusivePublicationActiveTermBufferForTest(ep)
	require.Equal(tb, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))

	got := offer()
	require.Equal(tb, wantStatus, got)
	require.Equal(tb, beforePosition, ep.Position())
	require.Equal(tb, beforeCount, aeron.ExclusivePublicationActiveTermCountForTest(ep))
	require.Equal(tb, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))
}

func injectExclusivePublicationCounterError(
	tb testing.TB,
	ep *aeron.ExclusivePublication,
	counterID int32,
	errMsg string,
) {
	tb.Helper()
	aeron.SetExclusivePublicationCounterValueErrorForTest(
		tb,
		ep,
		counterID,
		errors.New(errMsg),
	)
}

func requireExclusivePublicationLimitSlotMatchesCounter(
	tb testing.TB,
	ep *aeron.ExclusivePublication,
	reader *cnc.CountersReader,
) {
	tb.Helper()

	gotViaSlot := aeron.ExclusivePublicationLimitSlotForTest(ep)
	gotViaCounter, err := reader.CounterValue(aeron.TestPublicationLimitCounterID)
	require.NoError(tb, err)
	require.Equal(tb, gotViaCounter, gotViaSlot)
}

func requireExclusiveTermRotationPadding(
	tb testing.TB,
	ep *aeron.ExclusivePublication,
	margin int,
) {
	tb.Helper()

	termLength := aeron.TestPublicationTermLength
	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, 0)
	requireEndOfTermPaddingOnBuffer(tb, termBuf, termLength, margin)
	requireRotatedToNextTermMeta(tb, ep.MetaDataBufferForTest(), termLength, 1)
}

func TestExclusivePublication_Offer_SingleFrame(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	payload := []byte("hello-exclusive")
	position := ep.Offer(payload)
	require.Positive(t, position)

	_, termBuf := aeron.ExclusivePublicationActiveTermBufferForTest(ep)
	wantFrameLength := int32(len(payload) + logbuffer.DataHeaderLength)
	require.Equal(t, wantFrameLength, logbuffer.FrameLengthVolatile(termBuf, 0))
	payloadEnd := logbuffer.OffsetFramePayload + len(payload)
	gotPayload := termBuf.Bytes()[logbuffer.OffsetFramePayload:payloadEnd]
	require.Equal(t, payload, gotPayload)
}

func TestExclusivePublication_LimitCounter(t *testing.T) {
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
			cfg := aeron.LogConfigAtPositionForTest(
				aeron.DefaultTestExclusivePublicationLogConfig,
				tc.position,
			)
			ep := aeron.NewTestExclusivePublication(t, cfg, tc.limit)

			got := ep.Offer([]byte{1, 2, 3})
			if tc.offerProceeds {
				require.Positive(t, got)
				return
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestExclusivePublication_NotConnectedVsBackPressured(t *testing.T) {
	t.Run("disconnected at limit", func(t *testing.T) {
		cfg := aeron.DefaultTestExclusivePublicationLogConfig()
		cfg.IsConnected = 0
		ep := aeron.NewTestExclusivePublication(t, cfg, 0)

		got := ep.Offer([]byte{1})
		require.Equal(t, aeron.NotConnected, got)
	})

	t.Run("connected at limit", func(t *testing.T) {
		cfg := aeron.DefaultTestExclusivePublicationLogConfig()
		cfg.IsConnected = 1
		ep := aeron.NewTestExclusivePublication(t, cfg, 0)

		got := ep.Offer([]byte{1})
		require.Equal(t, aeron.BackPressured, got)
	})
}

func TestExclusivePublication_TermRotation_OnEndOfTerm(t *testing.T) {
	margin := 64
	ep := newTestExclusivePublicationNearTermEnd(t, margin)

	first := ep.Offer(make([]byte, margin))
	require.Equal(t, aeron.AdminAction, first)
	requireExclusiveTermRotationPadding(t, ep, margin)

	nextIndex := logbuffer.IndexByTermCount(1)
	second := ep.Offer([]byte{9, 8, 7})
	require.Positive(t, second)

	nextTermBuf := aeron.ExclusivePublicationTermBufferForTest(ep, nextIndex)
	require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(nextTermBuf, 0))
	nextTermID := int32(aeron.TestPublicationInitialTermID + 1)
	require.Equal(t, nextTermID, logbuffer.FrameTermID(nextTermBuf, 0))
	require.Equal(t, int32(0), logbuffer.FrameTermOffset(nextTermBuf, 0))
	gotPayload := nextTermBuf.Bytes()[logbuffer.OffsetFramePayload : logbuffer.OffsetFramePayload+3]
	require.Equal(t, []byte{9, 8, 7}, gotPayload)

	for range 3 {
		position := aeron.OfferRetryForTest(ep, []byte{1, 2, 3, 4, 5, 6, 7, 8})
		require.Positive(t, position)
		require.NotEqual(t, aeron.BackPressured, position)
	}
}

func TestExclusivePublication_TermRotation_TryClaim(t *testing.T) {
	margin := 64
	ep := newTestExclusivePublicationNearTermEnd(t, margin)

	_, _, position, err := ep.TryClaim(margin)
	require.NoError(t, err)
	require.Equal(t, aeron.AdminAction, position)
	requireExclusiveTermRotationPadding(t, ep, margin)

	nextIndex := logbuffer.IndexByTermCount(1)
	termIndex, frameOffset, position, err := ep.TryClaim(8)
	require.NoError(t, err)
	require.Positive(t, position)
	require.Equal(t, nextIndex, termIndex)
	require.Equal(t, 0, frameOffset)

	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("12345678"))
	require.NoError(t, ep.Commit(termIndex, frameOffset))
	require.Equal(
		t,
		int32(8+logbuffer.DataHeaderLength),
		logbuffer.FrameLengthVolatile(termBuf, frameOffset),
	)
}

func TestExclusivePublication_ClosedGuard(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	require.NoError(t, ep.Close())

	require.Equal(t, aeron.Closed, ep.Offer([]byte{1}))
	_, _, position, err := ep.TryClaim(8)
	require.Equal(t, aeron.Closed, position)
	require.NoError(t, err)
	require.Equal(t, aeron.Closed, ep.Position())
	require.Equal(t, aeron.Closed, ep.PositionLimit())
	require.False(t, ep.IsConnected())
	require.Equal(t, aeron.Closed, ep.AvailableWindow())
}

func TestExclusivePublication_MaxPositionExceeded_preReserve(t *testing.T) {
	position := aeron.PositionNearMaxPositionForTest()
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.LogConfigNearMaxPositionForTest(aeron.DefaultTestExclusivePublicationLogConfig),
		position,
	)
	requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.MaxPositionExceeded, func() int64 {
		return ep.Offer([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	})
}

// Pre-reserve guard on the last term: position+alignedLength reaches maxPossiblePosition
// before PutInt64Release; handleEndOfLog is not reached.
func TestExclusivePublication_MaxPositionExceeded_preReserve_highLimitExactTermFill(t *testing.T) {
	const remaining int32 = 64
	cfg := aeron.LogConfigWithTermRemainingForTest(
		aeron.DefaultTestExclusivePublicationLogConfig,
		remaining,
	)
	ep := aeron.NewTestExclusivePublication(t, cfg, 1<<62)
	requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.MaxPositionExceeded, func() int64 {
		return ep.Offer([]byte{1})
	})
}

func TestNewExclusivePublicationFromReady_validation(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestExclusivePublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	ready := aeron.PublicationBuffersReadyForTest()
	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)

	_, err := aeron.NewExclusivePublicationFromReadyForTest(ready, nil, counters, "aeron:ipc")
	require.EqualError(t, err, "aeron: exclusive publication log buffer is nil")

	_, err = aeron.NewExclusivePublicationFromReadyForTest(ready, log, nil, "aeron:ipc")
	require.EqualError(t, err, "aeron: exclusive publication counters view is nil")

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	emptyReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	_, err = aeron.NewExclusivePublicationFromReadyForTest(ready, log, emptyReader, "aeron:ipc")
	require.EqualError(
		t,
		err,
		"aeron: publication limit counter 1 is not active for registration 9001",
	)
}

func TestNewExclusivePublicationFromReadyForTest_wrongLogType(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)

	_, err := aeron.NewExclusivePublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		counters,
		"aeron:ipc",
	)
	require.EqualError(
		t,
		err,
		"aeron: expected exclusive publication log type 1, got 0",
	)
}

func TestNewExclusivePublicationFromReadyForTest_inactiveChannelCounter(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestExclusivePublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	reader := aeron.NewTestCountersReaderLimitOnlyForTest(t, 1_000_000)

	_, err := aeron.NewExclusivePublicationFromReadyForTest(
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

func TestNewExclusivePublicationFromReadyForTest_reusedEndpointChannelCounter(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestExclusivePublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	reader := aeron.NewTestCountersReaderMismatchedChannelStatusForTest(
		t,
		1_000_000,
		aeron.TestPublicationRegistrationID+1,
	)

	pub, err := aeron.NewExclusivePublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		reader,
		"aeron:ipc",
	)
	require.NoError(t, err)
	require.NotNil(t, pub)
}

func TestNewExclusivePublicationFromReadyForTest_invalidMetadata(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestExclusivePublicationLogConfig())
	meta, err := log.MetaDataBuffer()
	require.NoError(t, err)
	meta.PutInt32(logbuffer.OffsetTermLength, 1000)

	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)

	_, err = aeron.NewExclusivePublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		counters,
		"aeron:ipc",
	)
	require.EqualError(t, err, "logbuffer: term length below minimum: 1000 < 65536")
}

func TestNewExclusivePublicationFromReadyForTest_closedLog(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestExclusivePublicationLogConfig())
	require.NoError(t, log.Close())

	counters := aeron.NewTestCountersReaderForTest(t, 1_000_000)

	_, err := aeron.NewExclusivePublicationFromReadyForTest(
		aeron.PublicationBuffersReadyForTest(),
		log,
		counters,
		"aeron:ipc",
	)
	require.EqualError(t, err, "logbuffer: closed mapped log buffer")
}

func TestExclusivePublication_TryClaim_backPressureStatuses(t *testing.T) {
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
			cfg := aeron.LogConfigAtPositionForTest(
				aeron.DefaultTestExclusivePublicationLogConfig,
				tc.position,
			)
			if !tc.connected {
				cfg.IsConnected = 0
			}
			ep := aeron.NewTestExclusivePublication(t, cfg, tc.limit)

			_, _, position, err := ep.TryClaim(8)
			require.NoError(t, err)
			if tc.want == 0 {
				require.Positive(t, position)
				return
			}
			require.Equal(t, tc.want, position)
		})
	}
}

func TestExclusivePublication_Position_tracksOffer(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	require.Equal(t, int64(0), ep.Position())

	first := ep.Offer([]byte{1, 2, 3, 4})
	require.Positive(t, first)
	require.Equal(t, first, ep.Position())

	second := ep.Offer([]byte{5, 6, 7, 8})
	require.Positive(t, second)
	require.Greater(t, second, first)
	require.Equal(t, second, ep.Position())
}

func TestExclusivePublication_OfferWithOffset(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	payload := []byte("xxpayload")
	position := ep.OfferWithOffset(payload, 2, len(payload)-2)
	require.Positive(t, position)

	_, termBuf := aeron.ExclusivePublicationActiveTermBufferForTest(ep)
	gotPayload := termBuf.Bytes()[logbuffer.OffsetFramePayload : logbuffer.OffsetFramePayload+7]
	require.Equal(t, []byte("payload"), gotPayload)
}

func TestExclusivePublication_OfferWithOffset_closedBeforeBounds(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	require.NoError(t, ep.Close())

	got := ep.OfferWithOffset([]byte{1}, 99, 1)
	require.Equal(t, aeron.Closed, got)
}

func TestExclusivePublication_OfferWithOffset_invalidBounds(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
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
				ep.OfferWithOffset(buf, tc.offset, tc.length)
			})
		})
	}
}

func TestExclusivePublication_TryClaim_closedBeforeValidation(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	maxPayload := ep.MaxPayloadLengthForTest()
	require.NoError(t, ep.Close())

	termIndex, frameOffset, position, err := ep.TryClaim(-1)
	require.Equal(t, 0, termIndex)
	require.Equal(t, 0, frameOffset)
	require.Equal(t, aeron.Closed, position)
	require.NoError(t, err)

	termIndex, frameOffset, position, err = ep.TryClaim(maxPayload + 1)
	require.Equal(t, 0, termIndex)
	require.Equal(t, 0, frameOffset)
	require.Equal(t, aeron.Closed, position)
	require.NoError(t, err)
}

func TestExclusivePublication_TryClaim_negativeLength(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	termIndex, frameOffset, position, err := ep.TryClaim(-1)
	require.Equal(t, 0, termIndex)
	require.Equal(t, 0, frameOffset)
	require.Equal(t, int64(0), position)
	require.EqualError(t, err, "aeron: invalid message length=-1")
}

func TestExclusivePublication_TryClaim_payloadLengthBoundary(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	maxPayload := ep.MaxPayloadLengthForTest()

	_, _, position, err := ep.TryClaim(maxPayload)
	require.NoError(t, err)
	require.Positive(t, position)

	_, _, _, err = ep.TryClaim(maxPayload + 1)
	require.EqualError(
		t,
		err,
		"aeron: claim length 1377 exceeds maxPayloadLength=1376",
	)
}

func TestExclusivePublication_TryClaim_maxPositionExceeded(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.LogConfigNearMaxPositionForTest(aeron.DefaultTestExclusivePublicationLogConfig),
		int64(1)<<62,
	)
	beforeCount := aeron.ExclusivePublicationActiveTermCountForTest(ep)

	_, _, position, err := ep.TryClaim(64)
	require.NoError(t, err)
	require.Equal(t, aeron.MaxPositionExceeded, position)
	require.Equal(t, beforeCount, aeron.ExclusivePublicationActiveTermCountForTest(ep))
}

func TestExclusivePublication_TryClaim_maxPositionExceeded_preReserve(t *testing.T) {
	position := aeron.PositionNearMaxPositionForTest()
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.LogConfigNearMaxPositionForTest(aeron.DefaultTestExclusivePublicationLogConfig),
		position,
	)
	requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.MaxPositionExceeded, func() int64 {
		_, _, got, err := ep.TryClaim(8)
		require.NoError(t, err)
		return got
	})
}

func TestExclusivePublication_MaxPositionExceeded(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.LogConfigNearMaxPositionForTest(aeron.DefaultTestExclusivePublicationLogConfig),
		int64(1)<<62,
	)
	beforeCount := aeron.ExclusivePublicationActiveTermCountForTest(ep)

	got := ep.Offer(make([]byte, 64))
	require.Equal(t, aeron.MaxPositionExceeded, got)
	require.Equal(t, beforeCount, aeron.ExclusivePublicationActiveTermCountForTest(ep))
}

func TestExclusivePublication_TryClaim_Commit(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	termIndex, frameOffset, position, err := ep.TryClaim(16)
	require.NoError(t, err)
	require.Positive(t, position)

	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("claim-payload-16b"))
	require.NoError(t, ep.Commit(termIndex, frameOffset))
	require.Equal(
		t,
		int32(16+logbuffer.DataHeaderLength),
		logbuffer.FrameLengthVolatile(termBuf, frameOffset),
	)
}

func TestExclusivePublication_TryClaim_Abort(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	termIndex, frameOffset, _, err := ep.TryClaim(8)
	require.NoError(t, err)

	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
	require.NoError(t, ep.Abort(termIndex, frameOffset))
	require.True(t, logbuffer.IsPaddingFrame(termBuf, frameOffset))
}

func TestExclusivePublication_IsConnected(t *testing.T) {
	cfg := aeron.DefaultTestExclusivePublicationLogConfig()
	cfg.IsConnected = 1
	ep := aeron.NewTestExclusivePublication(t, cfg, 1_000_000)
	require.True(t, ep.IsConnected())

	cfg.IsConnected = 0
	ep = aeron.NewTestExclusivePublication(t, cfg, 1_000_000)
	require.False(t, ep.IsConnected())
}

func TestExclusivePublication_ChannelStatus(t *testing.T) {
	ep, _ := setupExclusivePublicationWithLimitReader(t, 1_000_000)
	status, err := ep.ChannelStatus()
	require.NoError(t, err)
	require.Equal(t, int64(1), status)
}

func TestExclusivePublication_ChannelStatus_counterFailure(t *testing.T) {
	ep, _ := setupExclusivePublicationWithLimitReader(t, 1_000_000)
	injectExclusivePublicationCounterError(
		t,
		ep,
		aeron.TestPublicationChannelCounterID,
		"aeron: injected channel counter failure",
	)

	_, err := ep.ChannelStatus()
	require.EqualError(t, err, "aeron: injected channel counter failure")
}

func TestExclusivePublication_AvailableWindow(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	require.Equal(t, int64(1_000_000), ep.AvailableWindow())

	position := ep.Offer([]byte{1, 2, 3, 4})
	require.Positive(t, position)
	require.Equal(t, int64(1_000_000)-position, ep.AvailableWindow())
}

func TestExclusivePublication_AvailableWindow_atLimit(t *testing.T) {
	ep, _ := setupExclusivePublicationWithLimitReaderAtPosition(t, 500_000, 500_000)
	require.Equal(t, int64(0), ep.AvailableWindow())
}

func TestExclusivePublication_limitCounterFailure_coldAccessorsReturnClosed(t *testing.T) {
	ep := setupExclusiveWithLimitCounterFailure(t)
	require.Equal(t, aeron.Closed, ep.AvailableWindow())
	require.Equal(t, aeron.Closed, ep.PositionLimit())
}

func TestExclusivePublication_limitCounterValueFailure_hotPathUsesBoundSlot(t *testing.T) {
	ep := setupExclusiveWithLimitCounterFailure(t)
	require.Equal(t, aeron.Closed, ep.PositionLimit())
	require.Equal(t, aeron.Closed, ep.AvailableWindow())
	require.Positive(t, ep.Offer([]byte{1}))

	_, _, position, err := ep.TryClaim(16)
	require.NoError(t, err)
	require.Positive(t, position)

	maxPayload := ep.MaxPayloadLengthForTest()
	require.Positive(t, ep.Offer(make([]byte, maxPayload+1)))
}

func TestExclusivePublication_publicationLimitSlot_matchesCounterValueAfterBind(t *testing.T) {
	cases := []struct {
		name  string
		limit int64
	}{
		{name: "small limit", limit: 128},
		{name: "large limit", limit: 1_000_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep, reader := setupExclusivePublicationWithLimitReader(t, tc.limit)
			requireExclusivePublicationLimitSlotMatchesCounter(t, ep, reader)
		})
	}

	t.Run("live update tracks driver write", func(t *testing.T) {
		const updatedLimit int64 = 2_000_000
		ep, reader := setupExclusivePublicationWithLimitReader(t, 1_000_000)
		writeTestExclusivePublicationLimit(t, reader, updatedLimit)
		require.Equal(t, updatedLimit, ep.PositionLimit())
	})

	t.Run("live limit decrease blocks Offer and updates AvailableWindow", func(t *testing.T) {
		ep, reader := setupExclusivePublicationWithLimitReader(t, 1_000_000)
		position, decreasedLimit := offerThenDecreaseExclusiveLimitBelowPosition(t, ep, reader)

		requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.BackPressured, func() int64 {
			return ep.Offer([]byte{5, 6, 7, 8})
		})
		require.Equal(t, decreasedLimit-position, ep.AvailableWindow())
	})
}

func TestExclusivePublication_Position_concurrentWithOffer(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		int64(1)<<62,
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = ep.Position()
			}
		}
	})

	for range 1000 {
		aeron.OfferRetryForTest(ep, []byte{1, 2, 3, 4})
	}
	close(stop)
	wg.Wait()
}

func TestExclusivePublication_nullChannelStatusCounter(t *testing.T) {
	log := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestExclusivePublicationLogConfig())
	t.Cleanup(func() { _ = log.Close() })

	ready := aeron.PublicationBuffersReadyForTest()
	ready.ChannelStatusCounterID = cnc.NullCounterID
	counters := aeron.NewTestCountersReaderLimitOnlyForTest(t, 1_000_000)

	ep, err := aeron.NewExclusivePublicationFromReadyForTest(ready, log, counters, "aeron:ipc")
	require.NoError(t, err)

	status, err := ep.ChannelStatus()
	require.NoError(t, err)
	require.Equal(t, aeron.ChannelEndpointStatusActive, status)
}

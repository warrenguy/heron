package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

func captureControlResponseOnPoller(
	t *testing.T,
	fields codecs.ControlResponseFields,
) *ControlResponsePoller {
	t.Helper()

	poller := NewControlResponsePoller(nil, 1)
	invokeHandleControlResponseFragment(t, poller, encodeControlResponseWire(t, fields), 0, -1)
	return poller
}

func TestTryMatchControlResponse_matchOK(t *testing.T) {
	poller := captureControlResponseOnPoller(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       555,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	require.True(t, poller.IsPollComplete())

	fields, err, ok := TryMatchControlResponse(
		poller,
		controlResponseTestControlSessionID,
		controlResponseTestCorrelationID,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, int64(555), fields.RelevantID)
	require.Equal(t, controlResponseTestCorrelationID, fields.CorrelationID)
}

func TestTryMatchControlResponse_noMatch(t *testing.T) {
	cases := []struct {
		name             string
		controlSessionID int64
		correlationID    int64
	}{
		{
			name:             "correlation_mismatch",
			controlSessionID: controlResponseTestControlSessionID,
			correlationID:    controlResponseTestCorrelationID + 1,
		},
		{
			name:             "control_session_mismatch",
			controlSessionID: controlResponseTestControlSessionID + 1,
			correlationID:    controlResponseTestCorrelationID,
		},
	}
	poller := captureControlResponseOnPoller(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       555,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	require.True(t, poller.IsPollComplete())

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fields, err, ok := TryMatchControlResponse(poller, tc.controlSessionID, tc.correlationID)
			require.False(t, ok)
			require.NoError(t, err)
			require.Zero(t, fields.RelevantID)
		})
	}
}

func TestTryMatchControlResponse_errorCodeMapsToError(t *testing.T) {
	poller := captureControlResponseOnPoller(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       99,
		Code:             archivewire.ControlResponseCode_ERROR,
		ErrorMessage:     []byte("boom"),
	})
	require.True(t, poller.IsPollComplete())

	fields, err, ok := TryMatchControlResponse(
		poller,
		controlResponseTestControlSessionID,
		controlResponseTestCorrelationID,
	)
	require.True(t, ok)
	require.Error(t, err)
	require.Zero(t, fields.RelevantID)
}

func TestTryMatchControlResponse_noCapture(t *testing.T) {
	cases := []struct {
		name   string
		poller *ControlResponsePoller
	}{
		{name: "nil_poller", poller: nil},
		{name: "not_poll_complete", poller: NewControlResponsePoller(nil, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fields, err, ok := TryMatchControlResponse(
				tc.poller,
				controlResponseTestControlSessionID,
				controlResponseTestCorrelationID,
			)
			require.False(t, ok)
			require.NoError(t, err)
			require.Zero(t, fields.RelevantID)
		})
	}
}

func TestTryMatchControlResponseDiscardStale(t *testing.T) {
	matchingResponse := codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       555,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	}
	cases := []struct {
		name        string
		poller      func(t *testing.T) *ControlResponsePoller
		wantMatch   bool
		wantLatched bool
	}{
		{
			name: "match_stays_latched",
			poller: func(t *testing.T) *ControlResponsePoller {
				return captureControlResponseOnPoller(t, matchingResponse)
			},
			wantMatch:   true,
			wantLatched: true,
		},
		{
			name: "stale_correlation_discarded",
			poller: func(t *testing.T) *ControlResponsePoller {
				stale := matchingResponse
				stale.CorrelationID++
				return captureControlResponseOnPoller(t, stale)
			},
		},
		{
			name: "stale_control_session_discarded",
			poller: func(t *testing.T) *ControlResponsePoller {
				stale := matchingResponse
				stale.ControlSessionID++
				return captureControlResponseOnPoller(t, stale)
			},
		},
		{
			name: "stale_challenge_discarded",
			poller: func(t *testing.T) *ControlResponsePoller {
				poller := NewControlResponsePoller(nil, 1)
				invokeHandleControlResponseFragment(
					t,
					poller,
					encodeChallengeWire(
						t,
						controlResponseTestControlSessionID,
						controlResponseTestCorrelationID,
						[]byte("challenge-bytes"),
					),
					0,
					-1,
				)
				return poller
			},
		},
		{
			name: "no_capture_no_op",
			poller: func(t *testing.T) *ControlResponsePoller {
				return NewControlResponsePoller(nil, 1)
			},
		},
		{
			name:   "nil_poller",
			poller: func(t *testing.T) *ControlResponsePoller { return nil },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := tc.poller(t)

			fields, err, ok := tryMatchControlResponseDiscardStale(
				poller,
				controlResponseTestControlSessionID,
				controlResponseTestCorrelationID,
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantMatch, ok)
			if tc.wantMatch {
				require.Equal(t, int64(555), fields.RelevantID)
			}
			if poller != nil {
				require.Equal(t, tc.wantLatched, poller.IsPollComplete())
			}
		})
	}
}

func TestTryMatchControlResponse_wrongTemplateIgnored(t *testing.T) {
	poller := NewControlResponsePoller(nil, 1)
	invokeHandleControlResponseFragment(
		t,
		poller,
		encodeChallengeWire(
			t,
			controlResponseTestControlSessionID,
			controlResponseTestCorrelationID,
			[]byte("challenge-bytes"),
		),
		0,
		-1,
	)
	require.True(t, poller.IsChallenged())

	_, err, ok := TryMatchControlResponse(
		poller,
		controlResponseTestControlSessionID,
		controlResponseTestCorrelationID,
	)
	require.False(t, ok)
	require.NoError(t, err)
}

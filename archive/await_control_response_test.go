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

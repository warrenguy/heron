//go:build testhooks

package client

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

func TestNewControlResponsePoller_freshCaptureDefaults(t *testing.T) {
	poller := NewControlResponsePoller(nil, DefaultConnectControlResponseFragmentLimit)

	require.False(t, poller.IsPollComplete())
	require.Equal(t, gensbe.RecordingSignal_NULL_VALUE, poller.RecordingSignal())
	require.Equal(t, gensbe.ControlResponseCode_NULL_VALUE, poller.Code())
	require.Equal(t, conductor.NullCorrelationID, poller.CorrelationID())
}

func TestNewControlResponsePoller_normalizesFragmentLimit(t *testing.T) {
	cases := []struct {
		name  string
		limit int
	}{
		{name: "zero", limit: 0},
		{name: "negative", limit: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := NewControlResponsePoller(nil, tc.limit)
			require.Equal(t, DefaultControlResponseFragmentLimit, poller.FragmentLimit())
		})
	}
}

func TestControlResponsePoller_Poll_zeroWhenNilSubscription(t *testing.T) {
	poller := newControlResponsePollerForTest(nil)

	require.Equal(t, 0, poller.Poll())
	require.False(t, poller.IsPollComplete())
}

func TestControlResponsePoller_Poll_controlResponseOK(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	writeUnfragmentedWireToTerm(t, termBuffer, encodeControlResponsePollerTestOKWire(t))

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.False(t, poller.IsChallenged())
	require.Equal(t, int32(gensbe.ControlResponseSbeTemplateID), poller.TemplateID())
	require.Equal(t, controlResponseTestControlSessionID, poller.ControlSessionID())
	require.Equal(t, controlResponseTestCorrelationID, poller.CorrelationID())
	require.Equal(t, int64(7), poller.RelevantID())
	require.Equal(t, gensbe.ControlResponseCode_OK, poller.Code())
	require.Equal(t, int32(520), poller.Version())
	require.Nil(t, poller.ErrorMessage())
}

func TestControlResponsePoller_Poll_controlResponseError(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	wire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       10,
		Code:             gensbe.ControlResponseCode_ERROR,
		Version:          0,
		ErrorMessage:     []byte("authentication rejected"),
	})
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, gensbe.ControlResponseCode_ERROR, poller.Code())
	require.Equal(t, int64(10), poller.RelevantID())
	require.Equal(t, []byte("authentication rejected"), poller.ErrorMessage())
}

func TestControlResponsePoller_Poll_fragmentedChallenge(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	wire := encodeChallengeWire(
		t,
		controlResponseTestControlSessionID,
		404,
		bytes.Repeat([]byte("challenge-bytes"), 32),
	)
	writeFragmentedWireToTerm(t, termBuffer, wire)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.False(t, poller.IsPollComplete())

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsChallenged())
	require.Equal(t, int32(gensbe.ChallengeSbeTemplateID), poller.TemplateID())
	require.Equal(t, controlResponseTestControlSessionID, poller.ControlSessionID())
	wantChallenge := bytes.Repeat([]byte("challenge-bytes"), 32)
	require.Equal(t, []byte(wantChallenge), poller.EncodedChallenge())
}

func TestControlResponsePoller_Poll_fragmentedControlResponseError(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	wire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       10,
		Code:             gensbe.ControlResponseCode_ERROR,
		Version:          0,
		ErrorMessage:     bytes.Repeat([]byte("authentication rejected "), 8),
	})
	writeFragmentedWireToTerm(t, termBuffer, wire)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.False(t, poller.IsPollComplete())

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, gensbe.ControlResponseCode_ERROR, poller.Code())
	wantErrorMessage := bytes.Repeat([]byte("authentication rejected "), 8)
	require.Equal(t, []byte(wantErrorMessage), poller.ErrorMessage())
}

func TestControlResponsePoller_Poll_challenge(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	writeUnfragmentedWireToTerm(t, termBuffer, encodeControlResponsePollerTestChallengeWire(t))

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsChallenged())
	require.Equal(t, int32(gensbe.ChallengeSbeTemplateID), poller.TemplateID())
	require.Equal(t, int64(404), poller.CorrelationID())
	require.Equal(t, controlResponseTestControlSessionID, poller.ControlSessionID())
	require.Equal(t, gensbe.ControlResponseCode_NULL_VALUE, poller.Code())
	require.Equal(t, conductor.NullCorrelationID, poller.RelevantID())
	require.Nil(t, poller.ErrorMessage())
	require.Equal(t, []byte("challenge-bytes"), poller.EncodedChallenge())
}

func TestControlResponsePoller_Poll_challengeEmptyPayload(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	wire := encodeChallengeWire(
		t,
		controlResponseTestControlSessionID,
		505,
		nil,
	)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsChallenged())
	require.Nil(t, poller.EncodedChallenge())
}

func TestControlResponsePoller_Poll_recordingSignalEvent(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	wire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    909,
		RecordingID:      1001,
		SubscriptionID:   2002,
		Position:         3003,
		Signal:           gensbe.RecordingSignal_START,
	})
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.False(t, poller.IsChallenged())
	require.Equal(t, int32(gensbe.RecordingSignalEventSbeTemplateID), poller.TemplateID())
	require.Equal(t, controlResponseTestControlSessionID, poller.ControlSessionID())
	require.Equal(t, int64(909), poller.CorrelationID())
	require.Equal(t, int64(1001), poller.RecordingID())
	require.Equal(t, int64(2002), poller.SubscriptionID())
	require.Equal(t, int64(3003), poller.Position())
	require.Equal(t, gensbe.RecordingSignal_START, poller.RecordingSignal())
}

func TestControlResponsePoller_Poll_unknownTemplateIgnored(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	wire := encodeKeepAliveRequestWire(t, controlResponseTestControlSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.False(t, poller.IsPollComplete())
	require.False(t, poller.IsChallenged())
}

func TestControlResponsePoller_Poll_schemaMismatchIgnored(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	wire := encodeWireWithSchemaID(t, 999)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.False(t, poller.IsPollComplete())
}

func TestControlResponsePoller_Poll_resetCaptureClearsPriorCapture(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	writeUnfragmentedWireToTerm(t, termBuffer, encodeControlResponsePollerTestOKWire(t))

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, controlResponseTestCorrelationID, poller.CorrelationID())
	require.Equal(t, controlResponseTestControlSessionID, poller.ControlSessionID())
	require.Equal(t, int64(7), poller.RelevantID())
	require.Equal(t, gensbe.ControlResponseCode_OK, poller.Code())
	require.Equal(t, int32(520), poller.Version())

	require.Equal(t, 0, poller.Poll())
	requirePollerCaptureCleared(t, poller)
}

func requirePollerCaptureCleared(t *testing.T, poller *ControlResponsePoller) {
	t.Helper()

	require.False(t, poller.IsPollComplete())
	require.Zero(t, poller.TemplateID())
	require.Zero(t, poller.ControlSessionID())
	require.Equal(t, conductor.NullCorrelationID, poller.CorrelationID())
	require.Zero(t, poller.RelevantID())
	require.Equal(t, gensbe.ControlResponseCode_NULL_VALUE, poller.Code())
	require.Zero(t, poller.Version())
	require.Nil(t, poller.ErrorMessage())
	require.Nil(t, poller.EncodedChallenge())
	require.Zero(t, poller.RecordingID())
	require.Zero(t, poller.SubscriptionID())
	require.Zero(t, poller.Position())
	require.Equal(t, gensbe.RecordingSignal_NULL_VALUE, poller.RecordingSignal())
}

func TestControlResponsePoller_Poll_resetsAfterComplete(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	writeControlResponsePollerTestOKThenChallengeWires(t, termBuffer)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, controlResponseTestCorrelationID, poller.CorrelationID())
	require.Equal(t, gensbe.ControlResponseCode_OK, poller.Code())

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsChallenged())
	require.Equal(t, int64(404), poller.CorrelationID())
	require.Equal(t, gensbe.ControlResponseCode_NULL_VALUE, poller.Code())
}

func TestControlResponsePoller_Poll_fragmentLimitTwoCapturesFirstTerminalOnly(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	writeControlResponsePollerTestOKThenChallengeWires(t, termBuffer)

	poller := NewControlResponsePoller(sub, 2)

	require.Equal(t, 2, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.False(t, poller.IsChallenged())
	require.Equal(t, controlResponseTestCorrelationID, poller.CorrelationID())
	require.Equal(t, int32(gensbe.ControlResponseSbeTemplateID), poller.TemplateID())
	require.Equal(t, gensbe.ControlResponseCode_OK, poller.Code())
}

func controlResponsePollerTestOKFields() codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       7,
		Code:             gensbe.ControlResponseCode_OK,
		Version:          520,
	}
}

func encodeControlResponsePollerTestOKWire(t *testing.T) []byte {
	t.Helper()
	return encodeControlResponseWire(t, controlResponsePollerTestOKFields())
}

func encodeControlResponsePollerTestChallengeWire(t *testing.T) []byte {
	t.Helper()
	return encodeChallengeWire(
		t,
		controlResponseTestControlSessionID,
		404,
		[]byte("challenge-bytes"),
	)
}

func writeControlResponsePollerTestOKThenChallengeWires(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
) {
	t.Helper()
	writeTwoSequentialUnfragmentedWiresToTerm(
		t,
		termBuffer,
		encodeControlResponsePollerTestOKWire(t),
		encodeControlResponsePollerTestChallengeWire(t),
	)
}

//go:build testhooks

package client

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestNewControlResponsePoller_freshCaptureDefaults(t *testing.T) {
	poller := NewControlResponsePoller(nil, DefaultConnectControlResponseFragmentLimit)

	require.False(t, poller.IsPollComplete())
	_, ok := poller.Capture()
	require.False(t, ok)
	_, ok = poller.Response()
	require.False(t, ok)
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
	capture, ok := poller.Capture()
	require.True(t, ok)
	require.Equal(t, int32(gensbe.ControlResponseSbeTemplateID), capture.TemplateID)
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, controlResponseTestControlSessionID, resp.ControlSessionID)
	require.Equal(t, controlResponseTestCorrelationID, resp.CorrelationID)
	require.Equal(t, int64(7), resp.RelevantID)
	require.Equal(t, gensbe.ControlResponseCode_OK, resp.Code)
	require.Equal(t, int32(520), resp.Version)
	require.Nil(t, resp.ErrorMessage)
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
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, gensbe.ControlResponseCode_ERROR, resp.Code)
	require.Equal(t, int64(10), resp.RelevantID)
	require.Equal(t, []byte("authentication rejected"), resp.ErrorMessage)
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
	capture, ok := poller.Capture()
	require.True(t, ok)
	require.Equal(t, int32(gensbe.ChallengeSbeTemplateID), capture.TemplateID)
	require.Equal(t, controlResponseTestControlSessionID, capture.ControlSessionID)
	wantChallenge := bytes.Repeat([]byte("challenge-bytes"), 32)
	require.Equal(t, []byte(wantChallenge), capture.EncodedChallenge)
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
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, gensbe.ControlResponseCode_ERROR, resp.Code)
	wantErrorMessage := bytes.Repeat([]byte("authentication rejected "), 8)
	require.Equal(t, []byte(wantErrorMessage), resp.ErrorMessage)
}

func TestControlResponsePoller_Poll_challenge(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	writeUnfragmentedWireToTerm(t, termBuffer, encodeControlResponsePollerTestChallengeWire(t))

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsChallenged())
	challenge, ok := poller.Challenge()
	require.True(t, ok)
	require.Equal(t, int64(404), challenge.CorrelationID)
	require.Equal(t, controlResponseTestControlSessionID, challenge.ControlSessionID)
	require.Equal(t, []byte("challenge-bytes"), challenge.EncodedChallenge)
	capture, ok := poller.Capture()
	require.True(t, ok)
	require.Equal(t, int32(gensbe.ChallengeSbeTemplateID), capture.TemplateID)
	require.Equal(t, gensbe.ControlResponseCode_NULL_VALUE, capture.Code)
	require.Equal(t, conductor.NullCorrelationID, capture.RelevantID)
	require.Nil(t, capture.ErrorMessage)
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
	capture, ok := poller.Capture()
	require.True(t, ok)
	require.Nil(t, capture.EncodedChallenge)
}

func TestControlResponsePoller_Poll_recordingSignalIgnored(t *testing.T) {
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
	require.False(t, poller.IsPollComplete())
	require.False(t, poller.IsChallenged())
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
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, controlResponseTestCorrelationID, resp.CorrelationID)
	require.Equal(t, controlResponseTestControlSessionID, resp.ControlSessionID)
	require.Equal(t, int64(7), resp.RelevantID)
	require.Equal(t, gensbe.ControlResponseCode_OK, resp.Code)
	require.Equal(t, int32(520), resp.Version)

	require.Equal(t, 0, poller.Poll())
	requirePollerCaptureCleared(t, poller)
}

func requirePollerCaptureCleared(t *testing.T, poller *ControlResponsePoller) {
	t.Helper()

	require.False(t, poller.IsPollComplete())
	_, ok := poller.Capture()
	require.False(t, ok)
	_, ok = poller.Response()
	require.False(t, ok)
	_, ok = poller.Challenge()
	require.False(t, ok)
}

func TestControlResponsePoller_Poll_resetsAfterComplete(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	writeControlResponsePollerTestOKThenChallengeWires(t, termBuffer)

	poller := newControlResponsePollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, controlResponseTestCorrelationID, resp.CorrelationID)
	require.Equal(t, gensbe.ControlResponseCode_OK, resp.Code)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsChallenged())
	challenge, ok := poller.Challenge()
	require.True(t, ok)
	require.Equal(t, int64(404), challenge.CorrelationID)
	capture, ok := poller.Capture()
	require.True(t, ok)
	require.Equal(t, gensbe.ControlResponseCode_NULL_VALUE, capture.Code)
}

func TestControlResponsePoller_Poll_fragmentLimitTwoCapturesFirstTerminalOnly(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)

	writeControlResponsePollerTestOKThenChallengeWires(t, termBuffer)

	poller := NewControlResponsePoller(sub, 2)

	require.Equal(t, 2, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.False(t, poller.IsChallenged())
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, controlResponseTestCorrelationID, resp.CorrelationID)
	capture, ok := poller.Capture()
	require.True(t, ok)
	require.Equal(t, int32(gensbe.ControlResponseSbeTemplateID), capture.TemplateID)
	require.Equal(t, gensbe.ControlResponseCode_OK, resp.Code)
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
	termBuffer atomicbuffer.AtomicBuffer,
) {
	t.Helper()
	writeTwoSequentialUnfragmentedWiresToTerm(
		t,
		termBuffer,
		encodeControlResponsePollerTestOKWire(t),
		encodeControlResponsePollerTestChallengeWire(t),
	)
}

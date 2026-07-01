package client

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

func TestHandleControlResponseFragment_truncatedBodiesIgnored(t *testing.T) {
	headerLen := codecs.MessageHeaderEncodedLen()
	require.Greater(t, headerLen, 2)

	cases := []struct {
		name string
		wire func(t *testing.T) []byte
	}{
		{
			name: "control_response",
			wire: func(t *testing.T) []byte {
				return encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    controlResponseTestCorrelationID,
					RelevantID:       7,
					Code:             gensbe.ControlResponseCode_OK,
					Version:          520,
				})
			},
		},
		{
			name: "challenge",
			wire: func(t *testing.T) []byte {
				return encodeChallengeWire(
					t,
					controlResponseTestControlSessionID,
					404,
					[]byte("challenge-bytes"),
				)
			},
		},
		{
			name: "recording_signal_event",
			wire: func(t *testing.T) []byte {
				return encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    909,
					RecordingID:      1001,
					SubscriptionID:   2002,
					Position:         3003,
					Signal:           gensbe.RecordingSignal_START,
				})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := tc.wire(t)
			require.Greater(t, len(wire), headerLen)
			truncated := wire[:headerLen+2]

			poller := NewControlResponsePoller(nil, 1)
			invokeHandleControlResponseFragment(t, poller, truncated, 0, -1)
			require.False(t, poller.IsPollComplete())
		})
	}
}

func TestHandleControlResponseFragment_shortFragmentIgnored(t *testing.T) {
	poller := NewControlResponsePoller(nil, 1)

	require.NotPanics(t, func() {
		invokeHandleControlResponseFragment(t, poller, []byte{1, 2, 3}, 0, -1)
	})
	require.False(t, poller.IsPollComplete())
}

func TestHandleControlResponseFragment_capturedBytesSurviveWireMutation(t *testing.T) {
	cases := []struct {
		name      string
		wire      func(t *testing.T) []byte
		wantText  string
		captured  func(*ControlResponsePoller) []byte
		challenge bool
	}{
		{
			name: "error_message",
			wire: func(t *testing.T) []byte {
				return encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    controlResponseTestCorrelationID,
					RelevantID:       10,
					Code:             gensbe.ControlResponseCode_ERROR,
					ErrorMessage:     []byte("auth rejected"),
				})
			},
			wantText: "auth rejected",
			captured: func(poller *ControlResponsePoller) []byte {
				return poller.ErrorMessage()
			},
		},
		{
			name: "encoded_challenge",
			wire: func(t *testing.T) []byte {
				return encodeChallengeWire(
					t,
					controlResponseTestControlSessionID,
					404,
					[]byte("challenge-bytes"),
				)
			},
			wantText: "challenge-bytes",
			captured: func(poller *ControlResponsePoller) []byte {
				return poller.EncodedChallenge()
			},
			challenge: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := tc.wire(t)

			poller := NewControlResponsePoller(nil, 1)
			invokeHandleControlResponseFragment(t, poller, wire, 0, -1)
			if tc.challenge {
				require.True(t, poller.IsChallenged())
			} else {
				require.True(t, poller.IsPollComplete())
			}

			captured := tc.captured(poller)
			require.Equal(t, []byte(tc.wantText), captured)

			idx := bytes.Index(wire, captured)
			require.NotEqual(t, -1, idx)
			for i := range captured {
				wire[idx+i] = 'X'
			}
			require.Equal(t, []byte(tc.wantText), tc.captured(poller))
		})
	}
}

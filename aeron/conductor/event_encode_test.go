package conductor

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
)

func TestEncodeEvent_outOfBounds(t *testing.T) {
	tests := []struct {
		name       string
		bodyLength int
		encode     func(atomic.AtomicBuffer) error
	}{
		{
			name:       "PublicationBuffersReady",
			bodyLength: PublicationBuffersReadyBodyLength("x") - 1,
			encode: func(buffer atomic.AtomicBuffer) error {
				return EncodePublicationBuffersReady(
					buffer,
					0,
					PublicationBuffersReady{CorrelationID: 1, LogFileName: "x"},
				)
			},
		},
		{
			name:       "SubscriptionReady",
			bodyLength: SubscriptionReadyBodyLength - 1,
			encode: func(buffer atomic.AtomicBuffer) error {
				return EncodeSubscriptionReady(
					buffer,
					0,
					SubscriptionReady{CorrelationID: 1, ChannelStatusCounterID: 2},
				)
			},
		},
		{
			name:       "AvailableImage",
			bodyLength: AvailableImageBodyLength("ab", "ipc") - 1,
			encode: func(buffer atomic.AtomicBuffer) error {
				return EncodeAvailableImage(
					buffer,
					0,
					AvailableImage{
						CorrelationID:  1,
						LogFileName:    "ab",
						SourceIdentity: "ipc",
					},
				)
			},
		},
		{
			name:       "UnavailableImage",
			bodyLength: UnavailableImageBodyLength("ipc") - 1,
			encode: func(buffer atomic.AtomicBuffer) error {
				return EncodeUnavailableImage(
					buffer,
					0,
					UnavailableImage{CorrelationID: 1, Channel: "ipc"},
				)
			},
		},
		{
			name:       "PublicationError",
			bodyLength: PublicationErrorBodyLength("x") - 1,
			encode: func(buffer atomic.AtomicBuffer) error {
				return EncodePublicationError(
					buffer,
					0,
					PublicationError{RegistrationID: 1, ErrorMessage: "x"},
				)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			buffer := atomic.Wrap(make([]byte, testCase.bodyLength))

			err := testCase.encode(buffer)
			require.ErrorIs(t, err, ErrEventBodyTooShort)
			require.EqualError(
				t,
				err,
				"conductor: event body too short: offset=0 capacity="+
					strconv.Itoa(testCase.bodyLength),
			)
		})
	}
}

package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/client"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

type mapControlResponseExpect struct {
	code       int32
	corr       int64
	msg        string
	exactError string
}

func TestMapControlResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp codecs.ControlResponseFields
		want *mapControlResponseExpect
	}{
		{
			name: "ok",
			resp: controlResponse(gensbe.ControlResponseCode_OK, 0, 7),
		},
		{
			name: "error_generic",
			resp: errorControlResponse(42, 11, "connect failed"),
			want: &mapControlResponseExpect{code: 42, corr: 11, msg: "connect failed"},
		},

		{
			name: "error_generic_empty_message",
			resp: errorControlResponse(42, 18),
			want: &mapControlResponseExpect{
				code:       42,
				corr:       18,
				exactError: "errorCode=42 correlationId=18",
			},
		},
		{
			name: "error_generic_zero_correlation",
			resp: errorControlResponse(42, 0, "connect failed"),
			want: &mapControlResponseExpect{
				code: 42,
				corr: 0,
				msg:  "connect failed (errorCode=42 correlationId=0)",
			},
		},
		{
			name: "error_generic_empty_message_zero_correlation",
			resp: errorControlResponse(42, 0),
			want: &mapControlResponseExpect{
				code:       42,
				corr:       0,
				exactError: "errorCode=42 correlationId=0",
			},
		},
		{
			name: "recording_unknown",
			resp: controlResponse(gensbe.ControlResponseCode_RECORDING_UNKNOWN, 99, 17),
			want: &mapControlResponseExpect{
				code: client.ArchiveErrorUnknownRecording,
				corr: 17,
				msg:  "unknown recording id: 99",
			},
		},
		{
			name: "subscription_unknown",
			resp: controlResponse(gensbe.ControlResponseCode_SUBSCRIPTION_UNKNOWN, 88, 19),
			want: &mapControlResponseExpect{
				code: client.ArchiveErrorUnknownSubscription,
				corr: 19,
				msg:  "unknown subscription id: 88",
			},
		},
		{
			name: "null_value_unknown",
			resp: controlResponse(gensbe.ControlResponseCode_NULL_VALUE, 0, 21),
			want: &mapControlResponseExpect{
				code: client.ArchiveErrorGeneric,
				corr: 21,
				msg:  "unexpected control response code: -2147483648",
			},
		},
		{
			name: "error_unknown_wire_code",
			resp: controlResponse(gensbe.ControlResponseCode(99), 0, 23),
			want: &mapControlResponseExpect{
				code: client.ArchiveErrorGeneric,
				corr: 23,
				msg:  "unexpected control response code: 99",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := client.MapControlResponse(tc.resp)
			if tc.want == nil {
				require.NoError(t, err)
				return
			}
			requireMapControlResponseError(t, err, tc.want)
		})
	}
}

func TestMapControlResponse_authenticationRejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		resp codecs.ControlResponseFields
		corr int64
		want string
	}{
		{
			name: "with_message",
			resp: errorControlResponse(client.ArchiveErrorAuthenticationRejected, 13, "bad credentials"),
			corr: 13,
			want: "authentication rejected: bad credentials",
		},
		{
			name: "empty_message",
			resp: errorControlResponse(client.ArchiveErrorAuthenticationRejected, 15),
			corr: 15,
			want: "authentication rejected",
		},
		{
			name: "existing_phrase",
			resp: errorControlResponse(
				client.ArchiveErrorAuthenticationRejected,
				16,
				"authentication rejected by archive",
			),
			corr: 16,
			want: "authentication rejected by archive",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := client.MapControlResponse(tc.resp)
			requireMapControlResponseError(t, err, &mapControlResponseExpect{
				code: client.ArchiveErrorAuthenticationRejected,
				corr: tc.corr,
				msg:  tc.want,
			})
		})
	}
}

func controlResponse(
	code gensbe.ControlResponseCode,
	relevantID int64,
	correlationID int64,
) codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		Code:          code,
		RelevantID:    relevantID,
		CorrelationID: correlationID,
	}
}

func errorControlResponse(
	relevantID, correlationID int64,
	message ...string,
) codecs.ControlResponseFields {
	resp := controlResponse(gensbe.ControlResponseCode_ERROR, relevantID, correlationID)
	if len(message) > 0 {
		resp.ErrorMessage = []byte(message[0])
	}
	return resp
}

func requireMapControlResponseError(t *testing.T, err error, want *mapControlResponseExpect) {
	t.Helper()

	require.Error(t, err)

	var archiveErr *client.ArchiveException
	require.ErrorAs(t, err, &archiveErr)
	require.Equal(t, want.code, archiveErr.ErrorCode)
	require.Equal(t, want.corr, archiveErr.CorrelationID)
	if want.exactError != "" {
		require.Equal(t, want.exactError, archiveErr.Error())
		return
	}
	require.Contains(t, archiveErr.Error(), want.msg)
}

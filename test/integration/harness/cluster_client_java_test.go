//go:build integration

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseJavaClusterClientLine(t *testing.T) {
	t.Parallel()

	detail := "ERROR - session failed authentication"
	cases := []struct {
		name    string
		line    string
		parsed  bool
		kind    string
		fields  map[string]string
		wantErr string
	}{
		{
			name:   "challenge",
			line:   "heron-client CHALLENGE match=true",
			parsed: true,
			kind:   "CHALLENGE",
			fields: map[string]string{"match": "true"},
		},
		{
			name:   "connected",
			line:   "heron-client CONNECTED session=7 term=2 leader=1",
			parsed: true,
			kind:   "CONNECTED",
			fields: map[string]string{"session": "7", "term": "2", "leader": "1"},
		},
		{
			name:   "sent",
			line:   "heron-client SENT first=1 last=3 position=40",
			parsed: true,
			kind:   "SENT",
			fields: map[string]string{"first": "1", "last": "3", "position": "40"},
		},
		{
			name: "new leader",
			line: "heron-client NEW_LEADER session=7 term=3 leader=0 " +
				"endpoints=0=127.0.0.1:1,1=127.0.0.1:2",
			parsed: true,
			kind:   "NEW_LEADER",
			fields: map[string]string{
				"session":   "7",
				"term":      "3",
				"leader":    "0",
				"endpoints": "0=127.0.0.1:1,1=127.0.0.1:2",
			},
		},
		{
			name: "session event",
			line: "heron-client SESSION_EVENT correlation=9 session=7 term=2 leader=1 " +
				"code=ERROR detail=" + detail,
			parsed: true,
			kind:   "SESSION_EVENT",
			fields: map[string]string{
				"correlation": "9",
				"session":     "7",
				"term":        "2",
				"leader":      "1",
				"code":        "ERROR",
				"detail":      detail,
			},
		},
		{
			name:   "message",
			line:   "heron-client MESSAGE session=7 length=4",
			parsed: true,
			kind:   "MESSAGE",
			fields: map[string]string{"session": "7", "length": "4"},
		},
		{
			name:   "error detail keeps spaces",
			line:   "heron-client ERROR class=AuthenticationException detail=" + detail,
			parsed: true,
			kind:   "ERROR",
			fields: map[string]string{
				"class":  "AuthenticationException",
				"detail": detail,
			},
		},
		{
			name:   "closed",
			line:   "heron-client CLOSED",
			parsed: true,
			kind:   "CLOSED",
			fields: map[string]string{},
		},
		{
			name: "without prefix",
			line: "session opened",
		},
		{
			name:    "unknown kind",
			line:    "heron-client NOPE match=1",
			parsed:  true,
			wantErr: "unknown kind",
		},
		{
			name:    "malformed field",
			line:    "heron-client CONNECTED session",
			parsed:  true,
			wantErr: "malformed field",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ev, ok, err := ParseJavaClusterClientLine(tc.line)
			require.Equal(t, tc.parsed, ok)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			if !tc.parsed {
				return
			}
			require.Equal(t, tc.kind, ev.Kind)
			require.Equal(t, tc.fields, ev.Fields)
			require.Equal(t, tc.line, ev.Line)
		})
	}
}

func TestJavaClusterClient_exitCodeWaitsForDrain(t *testing.T) {
	t.Parallel()
	client := &JavaClusterClient{tb: t, exitCode: 1, hasExit: true}
	client.enqueue(JavaClusterClientEvent{Kind: "ERROR"})

	_, exited := client.ExitCode()
	require.False(t, exited)
	ev, ok := client.PollEvent()
	require.True(t, ok)
	require.Equal(t, "ERROR", ev.Kind)
	code, exited := client.ExitCode()
	require.True(t, exited)
	require.Equal(t, 1, code)
}

func TestJavaClusterClientPayload(t *testing.T) {
	t.Parallel()
	require.Equal(t, []byte("jc-1"), JavaClusterClientPayload("jc", 1))
	require.Equal(t, []byte("jc-6"), JavaClusterClientPayload("jc", 6))
}

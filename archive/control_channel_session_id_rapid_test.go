//go:build rapid

package archive

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron"
)

var sessionIDRapidParamName = strings.TrimSuffix(aeron.ChannelSessionIDParam, "=")

var sessionIDRapidParamNames = []string{
	"endpoint",
	"term-length",
	"alias",
	"control",
	"control-mode",
	"tags",
	"rejoin",
	sessionIDRapidParamName,
}

// sessionIDRapidValues are drawn param values. Some embed session-id= as
// decoys. A match must start at a param boundary.
var sessionIDRapidValues = []string{
	"1",
	"64k",
	"localhost:0",
	"response",
	"tag:1",
	"x-session-id=3",
	"session-id=4",
	"a,b",
}

type sessionIDRapidParam struct {
	name  string
	value string
}

// sessionIDRapidChannel is a drawn channel. trailingQuery keeps the '?' on
// an empty query.
type sessionIDRapidChannel struct {
	media         string
	params        []sessionIDRapidParam
	trailingQuery bool
}

func (channel sessionIDRapidChannel) String() string {
	parts := make([]string, len(channel.params))
	for i, param := range channel.params {
		parts[i] = param.name + "=" + param.value
	}
	uri := "aeron:" + channel.media
	if len(parts) == 0 && !channel.trailingQuery {
		return uri
	}
	return uri + "?" + strings.Join(parts, "|")
}

// withSessionID is the oracle. It replaces the session-id value in place. It
// appends the param when absent.
func (channel sessionIDRapidChannel) withSessionID(sessionID int32) sessionIDRapidChannel {
	value := strconv.FormatInt(int64(sessionID), 10)
	channel.params = slices.Clone(channel.params)
	i := slices.IndexFunc(channel.params, func(param sessionIDRapidParam) bool {
		return param.name == sessionIDRapidParamName
	})
	if i >= 0 {
		channel.params[i].value = value
	} else {
		channel.params = append(
			channel.params,
			sessionIDRapidParam{name: sessionIDRapidParamName, value: value},
		)
	}
	return channel
}

func drawSessionIDRapidChannel(rt *rapid.T, label string) sessionIDRapidChannel {
	rt.Helper()

	names := rapid.Permutation(sessionIDRapidParamNames).Draw(rt, label+"_names")
	count := rapid.IntRange(0, len(names)).Draw(rt, label+"_count")
	params := make([]sessionIDRapidParam, count)
	for i, name := range names[:count] {
		params[i] = sessionIDRapidParam{
			name:  name,
			value: rapid.SampledFrom(sessionIDRapidValues).Draw(rt, label+"_"+name),
		}
	}
	return sessionIDRapidChannel{
		media:         rapid.SampledFrom([]string{"ipc", "udp"}).Draw(rt, label+"_media"),
		params:        params,
		trailingQuery: rapid.Bool().Draw(rt, label+"_trailing_query"),
	}
}

func TestApplyControlChannelSessionID_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		request := drawSessionIDRapidChannel(rt, "request")
		response := drawSessionIDRapidChannel(rt, "response")
		sessionID := rapid.Int32().Draw(rt, "session_id")
		cfg := Config{
			effectiveControlRequestChannel:  request.String(),
			effectiveControlResponseChannel: response.String(),
		}
		wantConfig := func(id int32) Config {
			want := cfg
			want.effectiveControlRequestChannel = request.withSessionID(id).String()
			want.effectiveControlResponseChannel = response.withSessionID(id).String()
			return want
		}

		// Phase: the first apply matches the oracle on both channels.
		once := applyControlChannelSessionID(cfg, sessionID)
		require.Equal(rt, wantConfig(sessionID), once)

		// Phase: a second apply with the same id is idempotent.
		require.Equal(rt, once, applyControlChannelSessionID(once, sessionID))

		// Phase: a second apply with another id holds only that id.
		otherID := rapid.Int32().Draw(rt, "other_session_id")
		require.Equal(rt, wantConfig(otherID), applyControlChannelSessionID(once, otherID))
	})
}

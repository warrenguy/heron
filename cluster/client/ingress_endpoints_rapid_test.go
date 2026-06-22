//go:build rapid

package client_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/client"
)

const rapidEndpointCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"

func drawIngressEndpoint(rt *rapid.T, name string) string {
	if rapid.Bool().Draw(rt, name+"_hostport") {
		host := drawIngressSafeASCII(rt, name+"Host", 1, 16)
		port := rapid.IntRange(1, 65535).Draw(rt, name+"Port")
		return host + ":" + strconv.Itoa(port)
	}
	return drawIngressSafeASCII(rt, name+"Ascii", 1, 32)
}

func drawIngressSafeASCII(rt *rapid.T, name string, minLen, maxLen int) string {
	n := rapid.IntRange(minLen, maxLen).Draw(rt, name+"Len")
	var b strings.Builder
	b.Grow(n)
	for range n {
		idx := rapid.IntRange(0, len(rapidEndpointCharset)-1).Draw(rt, name+"Char")
		b.WriteByte(rapidEndpointCharset[idx])
	}
	return b.String()
}

func requireParseIngressEndpointsError(rt *rapid.T, input, wantErr string) {
	got, err := client.ParseIngressEndpoints(input)
	require.Nil(rt, got)
	require.EqualError(rt, err, wantErr)
}

func drawValidMemberIngressMap(rt *rapid.T) client.MemberIngressMap {
	count := rapid.IntRange(1, 5).Draw(rt, "memberCount")
	seen := make(map[int32]struct{}, count)
	m := make(client.MemberIngressMap, count)
	for len(m) < count {
		memberID := int32(rapid.IntRange(0, 1<<20).Draw(rt, "memberID"))
		if _, ok := seen[memberID]; ok {
			continue
		}
		seen[memberID] = struct{}{}
		endpoint := drawIngressEndpoint(rt, fmt.Sprintf("endpoint%d", memberID))
		m[memberID] = &client.MemberIngress{
			MemberID: memberID,
			Endpoint: endpoint,
		}
	}
	return m
}

func TestFormatIngressEndpoints_nil_rapid(t *testing.T) {
	t.Parallel()
	require.Empty(t, client.FormatIngressEndpoints(nil))
}

func TestParseIngressEndpoints_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		initial := drawValidMemberIngressMap(rt)
		formatted := client.FormatIngressEndpoints(initial)
		require.NotEmpty(rt, formatted)

		parsed, err := client.ParseIngressEndpoints(formatted)
		require.NoError(rt, err)
		requireMemberIngressMapsEqual(rt, parsed, initial)
	})
}

func TestParseIngressEndpoints_invalid_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		switch rapid.IntRange(0, 4).Draw(rt, "invalidKind") {
		case 0:
			input := drawIngressSafeASCII(rt, "noEqualsPair", 1, 16)
			if rapid.Bool().Draw(rt, "prefixValidPair") {
				input = fmt.Sprintf(
					"%d=%s,%s",
					rapid.IntRange(0, 9).Draw(rt, "prefixMemberID"),
					drawIngressEndpoint(rt, "prefixEndpoint"),
					input,
				)
			}
			requireParseIngressEndpointsError(
				rt,
				input,
				"cluster client: endpoint missing '=' separator: "+input,
			)
		case 1:
			memberID := rapid.IntRange(0, 9).Draw(rt, "emptyEndpointMemberID")
			input := strconv.Itoa(memberID) + "="
			requireParseIngressEndpointsError(
				rt,
				input,
				"cluster client: ingress member endpoint is empty: "+input,
			)
		case 2:
			input := "=" + drawIngressEndpoint(rt, "emptyMemberEndpoint")
			requireParseIngressEndpointsError(
				rt,
				input,
				"cluster client: invalid ingress member id: "+input,
			)
		case 3:
			input := "x=" + drawIngressEndpoint(rt, "invalidMemberEndpoint")
			requireParseIngressEndpointsError(
				rt,
				input,
				"cluster client: invalid ingress member id: "+input,
			)
		case 4:
			input := "-1=" + drawIngressEndpoint(rt, "negativeMemberEndpoint")
			requireParseIngressEndpointsError(
				rt,
				input,
				"cluster client: invalid ingress member id: "+input,
			)
		}
	})
}

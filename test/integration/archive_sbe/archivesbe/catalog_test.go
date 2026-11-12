//go:build integration || testhooks

package archivesbe

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCatalog_rejects(t *testing.T) {
	t.Parallel()

	const (
		closeHex    = `"hex":"0000000000000000"`
		closeFields = `"fields":{"controlSessionId":1}`
	)

	fixture := func(name, template string, actingVersion int, tail string) string {
		return `{"name":"` + name + `","template":"` + template + `","actingVersion":` +
			strconv.Itoa(actingVersion) + `,` + tail + `}`
	}

	catalog := func(fixtures ...string) string {
		return `{"schemaVersion":1,"fixtures":[` + strings.Join(fixtures, ",") + `]}`
	}

	closeRow := func(name string) string {
		return fixture(name, "closeSessionRequest", 14, closeHex+`,`+closeFields)
	}

	single := func(template, tail string) string {
		return catalog(fixture("row", template, 14, tail))
	}

	cases := []struct {
		name string
		json string
		want string
	}{
		{
			name: "schemaVersion",
			json: `{"schemaVersion":2,"fixtures":[` + closeRow("row") + `]}`,
			want: "unsupported schemaVersion",
		},
		{
			name: "emptyFixtures",
			json: catalog(),
			want: "fixtures must be non-empty",
		},
		{
			name: "emptyName",
			json: catalog(closeRow("")),
			want: "empty name",
		},
		{
			name: "duplicateName",
			json: catalog(closeRow("same"), closeRow("same")),
			want: "duplicate fixture name",
		},
		{
			name: "unknownTemplate",
			json: single("notATemplate", closeHex+`,`+closeFields),
			want: "unknown template",
		},
		{
			name: "actingVersion",
			json: catalog(fixture("row", "closeSessionRequest", 15, closeHex+`,`+closeFields)),
			want: "acting version 15",
		},
		{
			name: "unknownKey",
			json: single("closeSessionRequest", closeHex+`,"fields":{"controlSessionId":1,"nope":1}`),
			want: "unknown field",
		},
		{
			name: "missingFixedKey",
			json: single("closeSessionRequest", closeHex+`,"fields":{}`),
			want: "missing field",
		},
		{
			name: "gatedKey",
			json: catalog(fixture("row", "authConnectRequest", 11, closeHex+`,"fields":{"correlationId":1,`+
				`"responseStreamId":2,"version":3,"clientInfo":"name=client"}`)),
			want: "gated field",
		},
		{
			name: "emptyVarString",
			json: single("startRecordingRequest",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"streamId":3,"sourceLocation":"LOCAL","channel":""}`),
			want: "empty string",
		},
		{
			name: "nonASCIIString",
			json: single("startRecordingRequest",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"streamId":3,"sourceLocation":"LOCAL","channel":"h\u00e9st"}`),
			want: "non-ASCII character",
		},
		{
			name: "emptyDataHex",
			json: single("challengeResponse",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"encodedCredentials":{"hex":""}}`),
			want: "empty hex",
		},
		{
			name: "missingHex",
			json: single("closeSessionRequest", closeFields),
			want: "missing hex",
		},
		{
			name: "uppercaseHex",
			json: single("closeSessionRequest", `"hex":"AAAAAAAAAAAAAAAA",`+closeFields),
			want: "hex must be lowercase",
		},
		{
			name: "oddHex",
			json: single("closeSessionRequest", `"hex":"000",`+closeFields),
			want: "even length",
		},
		{
			name: "shortHex",
			json: single("closeSessionRequest", `"hex":"00",`+closeFields),
			want: "shorter than header",
		},
		{
			name: "enumName",
			json: single("startRecordingRequest",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"streamId":3,"sourceLocation":"NOPE"}`),
			want: "unknown SourceLocation",
		},
		{
			name: "nullEnum",
			json: single("startRecordingRequest",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"streamId":3,"sourceLocation":"NULL_VAL"}`),
			want: "unknown SourceLocation",
		},
		{
			name: "int32Range",
			json: single("startRecordingRequest",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"streamId":3000000000,"sourceLocation":"LOCAL"}`),
			want: "out of int32 range",
		},
		{
			name: "int64Range",
			json: single("closeSessionRequest",
				closeHex+`,"fields":{"controlSessionId":9223372036854775808}`),
			want: "out of int64 range",
		},
		{
			name: "nonIntegral",
			json: single("closeSessionRequest", closeHex+`,"fields":{"controlSessionId":1.5}`),
			want: "expected integral number",
		},
		{
			name: "integralDecimal",
			json: single("closeSessionRequest", closeHex+`,"fields":{"controlSessionId":1.0}`),
			want: "expected integral number",
		},
		{
			name: "stringForData",
			json: single("challengeResponse",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"encodedCredentials":"abcd"}`),
			want: "expected hex object",
		},
		{
			name: "objectForAscii",
			json: single("startRecordingRequest",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"streamId":3,"sourceLocation":"LOCAL","channel":{"hex":"61"}}`),
			want: "expected string",
		},
		{
			name: "extraDataKey",
			json: single("challengeResponse",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"encodedCredentials":{"hex":"0a","ref":"x"}}`),
			want: `expected {"hex": string} object`,
		},
		{
			name: "missingDataHex",
			json: single("challengeResponse",
				closeHex+`,"fields":{"controlSessionId":1,"correlationId":2,`+
					`"encodedCredentials":{}}`),
			want: `expected {"hex": string} object`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := parseCatalog([]byte(tc.json))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

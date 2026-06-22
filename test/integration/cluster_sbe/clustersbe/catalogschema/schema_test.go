package catalogschema_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

var (
	blockLength99 = uint16(99)
	blockLengthV5 = codecs.SessionEventBlockLength(5)
)

func requireErrContains(t *testing.T, err error, substr string) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, err.Error(), substr)
}

func requireValidateErrContains(
	t *testing.T,
	mutate func(*catalogschema.CatalogFile),
	substr string,
) {
	t.Helper()
	requireErrContains(t, catalogschema.Validate(mutateCatalog(mutate)), substr)
}

func validChallengeFields() map[string]any {
	return map[string]any{
		"correlationId":    int64(1),
		"clusterSessionId": int64(2),
	}
}

func validSessionEventFields() map[string]any {
	return map[string]any{
		"clusterSessionId":                       int64(1),
		"correlationId":                          int64(2),
		"leadershipTermId":                       int64(3),
		"leaderMemberId":                         int32(4),
		"code":                                   "OK",
		catalogschema.SessionEventHeartbeatField: int64(0),
	}
}

func validChallengeFixture() catalogschema.FixtureEntry {
	return catalogschema.FixtureEntry{
		Name:          "ok",
		Template:      "challenge",
		ActingVersion: gensbe.SessionMessageHeaderSbeSchemaVersion,
		Hex:           "00",
		Fields:        validChallengeFields(),
	}
}

func validCatalog() catalogschema.CatalogFile {
	return catalogschema.CatalogFile{
		SchemaVersion: catalogschema.SchemaVersion,
		Blobs:         testBlobSpecs(),
		Fixtures:      []catalogschema.FixtureEntry{validChallengeFixture()},
	}
}

func mutateCatalog(mut func(*catalogschema.CatalogFile)) catalogschema.CatalogFile {
	c := validCatalog()
	mut(&c)
	return c
}

func mutateChallengeFields(c *catalogschema.CatalogFile, mut func(map[string]any)) {
	c.Fixtures[0].Fields = mutateFieldMap(validChallengeFields, mut)
}

func addIngress2000Blob(c *catalogschema.CatalogFile) {
	c.Blobs["ingress2000"] = ingress2000BlobSpec()
}

func newLeaderEventIngressFields(
	leadershipTermID, clusterSessionID int64,
	leaderMemberID int32,
) map[string]any {
	return map[string]any{
		"leadershipTermId": leadershipTermID,
		"clusterSessionId": clusterSessionID,
		"leaderMemberId":   leaderMemberID,
		"ingressEndpoints": map[string]any{"ref": "ingress2000"},
	}
}

func newLeaderEventIngressFixture(
	name string,
	leadershipTermID, clusterSessionID int64,
	leaderMemberID int32,
	largeVar bool,
) catalogschema.FixtureEntry {
	return catalogschema.FixtureEntry{
		Name:          name,
		Template:      "newLeaderEvent",
		ActingVersion: gensbe.SessionMessageHeaderSbeSchemaVersion,
		Hex:           "00",
		LargeVar:      largeVar,
		Fields: newLeaderEventIngressFields(
			leadershipTermID, clusterSessionID, leaderMemberID,
		),
	}
}

func setSessionEventFields(c *catalogschema.CatalogFile, mut func(map[string]any)) {
	c.Fixtures[0].Template = "sessionEvent"
	c.Fixtures[0].Fields = mutateFieldMap(validSessionEventFields, mut)
}

func setSessionEventBlockLength(
	c *catalogschema.CatalogFile,
	actingVersion uint16,
	blockLength *uint16,
) {
	setSessionEventFields(c, nil)
	c.Fixtures[0].ActingVersion = actingVersion
	c.Fixtures[0].BlockLength = blockLength
}

func TestParseJSON_malformed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		data string
	}{
		{name: "truncated", data: `{"schemaVersion":`},
		{name: "invalidSyntax", data: `{not json}`},
		{name: "wrongType", data: `{"schemaVersion":"one"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := catalogschema.ParseJSON([]byte(tc.data))
			require.Error(t, err)
		})
	}
}

func TestMarshalCanonical_roundTrip(t *testing.T) {
	t.Parallel()

	data, err := catalogschema.MarshalCanonical(validCatalog())
	require.NoError(t, err)

	parsed, err := catalogschema.ParseJSON(data)
	require.NoError(t, err)

	again, err := catalogschema.MarshalCanonical(parsed)
	require.NoError(t, err)
	require.Equal(t, data, again)
}

func TestParseJSON_maxInt64RoundTrip(t *testing.T) {
	t.Parallel()
	data := `{
		"schemaVersion": 1,
		"blobs": {},
		"fixtures": [{
			"name": "max",
			"template": "sessionMessageHeader",
			"actingVersion": 16,
			"hex": "00",
			"fields": {
				"leadershipTermId": 9223372036854775807,
				"clusterSessionId": 0,
				"timestamp": 0
			}
		}]
	}`

	catalog, err := catalogschema.ParseJSON([]byte(data))
	require.NoError(t, err)

	value, ok := catalog.Fixtures[0].Fields["leadershipTermId"]
	require.True(t, ok)

	n, err := catalogschema.Int64FromJSONValue(value)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), n)
}

func TestInt64FromJSONValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		value   any
		want    int64
		wantErr string
	}{
		{name: "jsonNumber", value: json.Number("42"), want: 42},
		{name: "maxInt64Number", value: json.Number("9223372036854775807"), want: math.MaxInt64},
		{name: "outOfRange", value: json.Number("9223372036854775808"), wantErr: "out of int64 range"},
		{name: "wrongType", value: "nope", wantErr: "expected number"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := catalogschema.Int64FromJSONValue(tc.value)
			if tc.wantErr != "" {
				requireErrContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLookupEnums(t *testing.T) {
	t.Parallel()

	eventCode, err := catalogschema.LookupEventCode("OK")
	require.NoError(t, err)
	require.Equal(t, gensbe.EventCode_OK, eventCode)

	_, err = catalogschema.LookupEventCode("NOPE")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown EventCode")

	requestType, err := catalogschema.LookupAdminRequestType("SNAPSHOT")
	require.NoError(t, err)
	require.Equal(t, gensbe.AdminRequestType_SNAPSHOT, requestType)

	responseCode, err := catalogschema.LookupAdminResponseCode("ERROR")
	require.NoError(t, err)
	require.Equal(t, gensbe.AdminResponseCode_ERROR, responseCode)
}

func TestValidate_errors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(*catalogschema.CatalogFile)
		wantErr string
	}{
		{
			name: "badSchemaVersion",
			mutate: func(c *catalogschema.CatalogFile) {
				c.SchemaVersion = 2
			},
			wantErr: "unsupported schemaVersion",
		},
		{
			name: "emptyFixtures",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures = nil
			},
			wantErr: "fixtures must be non-empty",
		},
		{
			name: "blobRepeatLen",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Blobs["challenge32"] = catalogschema.BlobSpec{}
			},
			wantErr: "repeat len must be positive",
		},
		{
			name: "blobRepeatByte",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Blobs["challenge32"] = catalogschema.BlobSpec{
					Repeat: catalogschema.BlobRepeat{Byte: 300, Len: 1},
				}
			},
			wantErr: "repeat byte out of range",
		},
		{
			name: "blobRepeatLenExceedsMax",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Blobs["challenge32"] = catalogschema.BlobSpec{
					Repeat: catalogschema.BlobRepeat{Byte: 1, Len: 300},
				}
			},
			wantErr: "repeat len 300 exceeds max",
		},
		{
			name: "missingName",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Name = ""
			},
			wantErr: "missing name",
		},
		{
			name: "missingTemplate",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Template = ""
			},
			wantErr: "missing template",
		},
		{
			name: "missingHex",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Hex = ""
			},
			wantErr: "missing hex",
		},
		{
			name: "missingFields",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Fields = nil
			},
			wantErr: "missing fields",
		},
		{
			name: "duplicateName",
			mutate: func(c *catalogschema.CatalogFile) {
				dup := validChallengeFixture()
				dup.Hex = "01"
				c.Fixtures = append(c.Fixtures, dup)
			},
			wantErr: "duplicate fixture name",
		},
		{
			name: "uppercaseHex",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Hex = "AB"
			},
			wantErr: "hex must be lowercase",
		},
		{
			name: "oddLengthHex",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Hex = "0"
			},
			wantErr: "hex must have even length",
		},
		{
			name: "invalidHexDigits",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Hex = "zz"
			},
			wantErr: "invalid hex",
		},
		{
			name: "unknownTemplate",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Template = "missing"
			},
			wantErr: "unknown template",
		},
		{
			name: "blockLengthOnNonSessionEvent",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].BlockLength = &blockLength99
			},
			wantErr: "blockLength only allowed for sessionEvent",
		},
		{
			name: "blockLengthMismatch",
			mutate: func(c *catalogschema.CatalogFile) {
				setSessionEventBlockLength(c, 5, &blockLength99)
			},
			wantErr: "blockLength 99 does not match actingVersion 5",
		},
		{
			name: "blockLengthV6Rejected",
			mutate: func(c *catalogschema.CatalogFile) {
				setSessionEventBlockLength(c, 6, &blockLengthV5)
			},
			wantErr: "blockLength not allowed for actingVersion 6",
		},
		{
			name: "missingVersionPreV2",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Template = "sessionConnectRequest"
				c.Fixtures[0].ActingVersion = 1
				c.Fixtures[0].Fields = map[string]any{
					"correlationId":    int64(1),
					"responseStreamId": int32(2),
				}
			},
			wantErr: "missing version for actingVersion < 2",
		},
		{
			name: "unknownFieldKey",
			mutate: func(c *catalogschema.CatalogFile) {
				mutateChallengeFields(c, func(fields map[string]any) {
					fields["bogus"] = 1
				})
			},
			wantErr: "unknown for template",
		},
		{
			name: "unknownBlobRef",
			mutate: func(c *catalogschema.CatalogFile) {
				mutateChallengeFields(c, func(fields map[string]any) {
					fields["encodedChallenge"] = map[string]any{"ref": "missing"}
				})
			},
			wantErr: "unknown blob ref",
		},
		{
			name: "sessionEventMissingHeartbeat",
			mutate: func(c *catalogschema.CatalogFile) {
				setSessionEventFields(c, func(fields map[string]any) {
					delete(fields, catalogschema.SessionEventHeartbeatField)
				})
			},
			wantErr: "missing leaderHeartbeatTimeoutNs",
		},
		{
			name: "sessionEventDetailTooLong",
			mutate: func(c *catalogschema.CatalogFile) {
				setSessionEventFields(c, func(fields map[string]any) {
					fields["detail"] = strings.Repeat("x", 300)
				})
			},
			wantErr: "length 300 exceeds max",
		},
		{
			name: "largeVarBlobWithoutTag",
			mutate: func(c *catalogschema.CatalogFile) {
				addIngress2000Blob(c)
			},
			wantErr: "repeat len 2000 exceeds max",
		},
		{
			name: "largeVarWithoutOversizeField",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].LargeVar = true
			},
			wantErr: "largeVar fixture must include a var field longer than",
		},
		{
			name: "largeVarOnTemplateWithoutVarFields",
			mutate: func(c *catalogschema.CatalogFile) {
				c.Fixtures[0].Template = "sessionMessageHeader"
				c.Fixtures[0].Fields = map[string]any{
					"leadershipTermId": int64(0),
					"clusterSessionId": int64(0),
					"timestamp":        int64(0),
				}
				c.Fixtures[0].LargeVar = true
			},
			wantErr: "largeVar set on template without var fields",
		},
		{
			name: "ingressEndpointsBlobRefWithoutLargeVar",
			mutate: func(c *catalogschema.CatalogFile) {
				addIngress2000Blob(c)
				c.Fixtures[0] = newLeaderEventIngressFixture(
					"newLeaderEvent_untaggedLargeRef", 6, 7, 8, false,
				)
				// A largeVar fixture references the same blob so blob len validation
				// passes; this entry lacks largeVar and must fail on field length.
				c.Fixtures = append(c.Fixtures, newLeaderEventIngressFixture(
					"newLeaderEvent_largeVarAnchor", 1, 2, 3, true,
				))
			},
			wantErr: `field "ingressEndpoints": length 2000 exceeds max 256`,
		},
		{
			name: "sessionEventDetailBlobRefRejected",
			mutate: func(c *catalogschema.CatalogFile) {
				setSessionEventFields(c, func(fields map[string]any) {
					fields["detail"] = map[string]any{"ref": "challenge32"}
				})
			},
			wantErr: `field "detail": expected string`,
		},
		{
			name: "encodedChallengePlainStringRejected",
			mutate: func(c *catalogschema.CatalogFile) {
				mutateChallengeFields(c, func(fields map[string]any) {
					fields["encodedChallenge"] = "0102"
				})
			},
			wantErr: "expected hex or ref object",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireValidateErrContains(t, tc.mutate, tc.wantErr)
		})
	}
}

func TestValidate_acceptsValidCatalog(t *testing.T) {
	t.Parallel()
	require.NoError(t, catalogschema.Validate(validCatalog()))
}

func TestValidate_acceptsLargeVarFixture(t *testing.T) {
	t.Parallel()

	catalog := validCatalog()
	addIngress2000Blob(&catalog)
	catalog.Fixtures[0] = newLeaderEventIngressFixture(
		"newLeaderEvent_largeEndpoints", 6, 7, 8, true,
	)

	require.NoError(t, catalogschema.Validate(catalog))
	entry := catalog.Fixtures[0]
	require.Equal(t, codecs.ClusterDecodeMaxVarLen, catalogschema.FixtureMaxVarLen(entry))
}

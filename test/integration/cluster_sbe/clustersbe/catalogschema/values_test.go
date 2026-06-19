package catalogschema_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

// testBlobSpecs mirrors catalog.json blob bytes for codec unit tests.
// The contract corpus is authored in catalog.json; this is local scaffolding for
// exercising hex/ref resolution, not a second source of truth.
func testBlobSpecs() map[string]catalogschema.BlobSpec {
	return map[string]catalogschema.BlobSpec{
		"challenge32": {Repeat: catalogschema.BlobRepeat{Byte: 0xAB, Len: 32}},
		"cred32":      {Repeat: catalogschema.BlobRepeat{Byte: 0xCD, Len: 32}},
		"admin16":     {Repeat: catalogschema.BlobRepeat{Byte: 0x33, Len: 16}},
	}
}

func testBlobBytes() map[string][]byte {
	return catalogschema.ResolveBlobs(testBlobSpecs())
}

func TestDecodeBytesValue(t *testing.T) {
	t.Parallel()

	blobs := testBlobBytes()
	cases := []struct {
		name  string
		value any
		want  []byte
	}{
		{name: "nil", value: nil, want: nil},
		{name: "hex", value: map[string]any{"hex": "0102ab"}, want: []byte{0x01, 0x02, 0xab}},
		{name: "ref", value: map[string]any{"ref": "challenge32"}, want: blobs["challenge32"]},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			decoded, err := catalogschema.DecodeBytesValue(tc.value, blobs)
			require.NoError(t, err)
			require.Equal(t, tc.want, decoded)
		})
	}
}

func TestDecodeBytesValue_errors(t *testing.T) {
	t.Parallel()

	blobs := testBlobBytes()
	cases := []struct {
		name    string
		value   any
		wantErr string
	}{
		{name: "wrongType", value: "not-an-object", wantErr: "expected hex or ref object"},
		{
			name:    "missingHexAndRef",
			value:   map[string]any{},
			wantErr: "expected exactly one of hex or ref",
		},
		{
			name:    "bothHexAndRef",
			value:   map[string]any{"hex": "ab", "ref": "challenge32"},
			wantErr: "expected exactly one of hex or ref",
		},
		{name: "hexWrongType", value: map[string]any{"hex": 1}, wantErr: "hex must be string"},
		{
			name:    "invalidHex",
			value:   map[string]any{"hex": "zz"},
			wantErr: "invalid hex: encoding/hex: invalid byte: U+007A 'z'",
		},
		{name: "uppercaseHex", value: map[string]any{"hex": "AB"}, wantErr: "hex must be lowercase"},
		{name: "refEmpty", value: map[string]any{"ref": ""}, wantErr: "ref must be non-empty string"},
		{name: "refWrongType", value: map[string]any{"ref": 1}, wantErr: "ref must be non-empty string"},
		{
			name:    "unknownRef",
			value:   map[string]any{"ref": "missing"},
			wantErr: `unknown blob ref "missing"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := catalogschema.DecodeBytesValue(tc.value, blobs)
			require.Error(t, err)
			require.Equal(t, tc.wantErr, err.Error())
		})
	}
}

func TestHeartbeatFromCatalog(t *testing.T) {
	t.Parallel()

	absent, err := catalogschema.HeartbeatFromCatalog(catalogschema.HeartbeatAbsentSentinel)
	require.NoError(t, err)
	require.Equal(t, int64(math.MinInt64), absent)

	okCases := []struct {
		name  string
		value any
		want  int64
	}{
		{name: "jsonNumber", value: json.Number("99"), want: 99},
		{name: "jsonNumberZero", value: json.Number("0"), want: 0},
	}
	for _, tc := range okCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := catalogschema.HeartbeatFromCatalog(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	errCases := []struct {
		name    string
		value   any
		wantErr string
	}{
		{name: "unknownSentinel", value: "gone", wantErr: `unknown heartbeat sentinel "gone"`},
		{name: "wrongType", value: true, wantErr: "expected number"},
	}
	for _, tc := range errCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := catalogschema.HeartbeatFromCatalog(tc.value)
			require.Error(t, err)
			require.Equal(t, tc.wantErr, err.Error())
		})
	}
}

func TestOptionalStringFromCatalog(t *testing.T) {
	t.Parallel()

	decoded, err := catalogschema.OptionalStringFromCatalog("detail")
	require.NoError(t, err)
	require.Equal(t, []byte("detail"), decoded)

	data, err := catalogschema.OptionalStringFromCatalog("channel")
	require.NoError(t, err)
	require.Equal(t, []byte("channel"), data)

	_, err = catalogschema.OptionalStringFromCatalog(nil)
	require.Error(t, err)
	require.Equal(t, "expected string", err.Error())

	data, err = catalogschema.OptionalStringFromCatalog("")
	require.NoError(t, err)
	require.Equal(t, []byte(""), data)

	_, err = catalogschema.OptionalStringFromCatalog(42)
	require.Error(t, err)
	require.Equal(t, "expected string", err.Error())
}

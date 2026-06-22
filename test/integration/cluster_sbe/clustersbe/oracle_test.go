//go:build integration && cluster_sbe

package clustersbe_test

import (
	"encoding/hex"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe"
)

func TestWireFixtures_oracle(t *testing.T) {
	for _, fixture := range clustersbe.Fixtures() {
		t.Run(fixture.Name, func(t *testing.T) {
			t.Parallel()
			wantWire := requireFixtureWire(t, fixture)

			requireDecodedFieldsMatch(t, wantWire, fixture.MaxVarLen(), fixture)

			goWire, ok := clustersbe.ReencodeWire(fixture)
			require.True(t, ok)
			require.Equal(t, wantWire, goWire)
		})
	}
}

func TestWireFixtures_largeVar_requiresDecodeMax(t *testing.T) {
	for _, fixture := range clustersbe.Fixtures() {
		if !fixture.LargeVar {
			continue
		}
		t.Run(fixture.Name, func(t *testing.T) {
			t.Parallel()
			wantWire := requireFixtureWire(t, fixture)

			_, ok := codecs.TryDecodeByTemplate(wantWire, 0, codecs.ClusterCodecMaxVarLen)
			require.False(t, ok, "largeVar fixture must not decode at ClusterCodecMaxVarLen")

			requireDecodedFieldsMatch(t, wantWire, fixture.MaxVarLen(), fixture)
		})
	}
}

func TestWireFixtures_tryDecodeRejectsCorruptWire(t *testing.T) {
	t.Parallel()

	fixture := fixturesByName()["sessionMessageHeader_zeros"]
	wantWire := requireFixtureWire(t, fixture)

	cases := []struct {
		name   string
		wire   []byte
		maxLen int
	}{
		{name: "truncatedHeader", wire: wantWire[:4]},
		{name: "badSchemaID", wire: corruptWireByte(wantWire, 4, 0x00)},
		{name: "blockLengthAboveMax", wire: corruptWireByte(wantWire, 0, 0xFF)},
		{name: "negativeMaxVarLen", wire: wantWire, maxLen: -1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			maxLen := tc.maxLen
			if maxLen == 0 {
				maxLen = codecs.ClusterCodecMaxVarLen
			}
			_, ok := codecs.TryDecodeByTemplate(tc.wire, 0, maxLen)
			require.False(t, ok)
		})
	}
}

func requireFixtureWire(t *testing.T, fixture clustersbe.WireFixture) []byte {
	t.Helper()
	wantWire, err := hex.DecodeString(fixture.Hex)
	require.NoError(t, err)
	return wantWire
}

func requireDecodedFieldsMatch(
	t *testing.T,
	wire []byte,
	maxLen int,
	fixture clustersbe.WireFixture,
) {
	t.Helper()
	decoded, ok := codecs.TryDecodeByTemplate(wire, 0, maxLen)
	require.True(t, ok)
	require.Equal(t, clustersbe.ExpectedDecodeFields(fixture), decoded.Fields)
}

func corruptWireByte(wire []byte, index int, value byte) []byte {
	out := slices.Clone(wire)
	out[index] = value
	return out
}

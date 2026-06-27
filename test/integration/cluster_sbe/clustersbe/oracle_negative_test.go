//go:build integration && cluster_sbe

package clustersbe_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs/gensbe"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe"
)

const sessionMessageHeaderZerosFixture = "sessionMessageHeader_zeros"

func TestReencodeWire_rejects(t *testing.T) {
	t.Parallel()

	base, ok := fixturesByName()[sessionMessageHeaderZerosFixture]
	require.True(t, ok)

	cases := []struct {
		name   string
		mutate func(*clustersbe.WireFixture)
	}{
		{
			name: "wrongFieldsType",
			mutate: func(f *clustersbe.WireFixture) {
				f.TemplateID = gensbe.SessionEventSbeTemplateID
			},
		},
		{
			name: "unsupportedTemplateID",
			mutate: func(f *clustersbe.WireFixture) {
				f.TemplateID = 999
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := base
			tc.mutate(&fixture)
			_, ok := clustersbe.ReencodeWire(fixture)
			require.False(t, ok)
		})
	}
}

func TestExpectedDecodeFields_passThrough(t *testing.T) {
	t.Parallel()

	fixture, ok := fixturesByName()[sessionMessageHeaderZerosFixture]
	require.True(t, ok)
	require.Equal(t, fixture.Fields, clustersbe.ExpectedDecodeFields(fixture))
}

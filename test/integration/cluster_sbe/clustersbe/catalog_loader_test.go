//go:build integration && cluster_sbe

package clustersbe_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe"
)

func TestCatalogEntryToWireFixtureForTest_sessionConnectRequestPreV2(t *testing.T) {
	cases := []struct {
		name    string
		entry   clustersbe.FixtureEntry
		wantErr string
	}{
		{
			name: "missingVersion",
			entry: clustersbe.FixtureEntry{
				Name:          "preV2",
				Template:      "sessionConnectRequest",
				ActingVersion: 1,
				Hex:           "00",
				Fields: map[string]any{
					"correlationId":    json.Number("103"),
					"responseStreamId": json.Number("203"),
				},
			},
			wantErr: "missing version for actingVersion < 2",
		},
		{
			name: "withVersion",
			entry: clustersbe.FixtureEntry{
				Name:          "preV2",
				Template:      "sessionConnectRequest",
				ActingVersion: 1,
				Hex:           "00",
				Fields: map[string]any{
					"correlationId":    json.Number("103"),
					"responseStreamId": json.Number("203"),
					"version":          json.Number("0"),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture, err := clustersbe.CatalogEntryToWireFixtureForTest(tc.entry, nil)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, uint16(1), fixture.ActingVersion)
		})
	}
}

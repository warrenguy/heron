package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

func modRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	require.NoError(t, err)
	return filepath.Dir(strings.TrimSpace(string(out)))
}

func committedCatalogPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(modRoot(t), defaultCatalogPath)
}

func TestRegenCatalogFromFile_matchesCommittedCatalog(t *testing.T) {
	t.Parallel()
	catalog, err := regenCatalogFromFile(committedCatalogPath(t))
	require.NoError(t, err)
	out, err := catalogschema.MarshalCanonical(catalog)
	require.NoError(t, err)
	require.Equal(t, clustersbe.CatalogJSONForTest(), out)
}

func TestRegenCatalogFromFile_errors(t *testing.T) {
	t.Parallel()

	const challengeBadRef = `{
  "schemaVersion": 1,
  "blobs": {},
  "fixtures": [{
    "name": "challenge_bad",
    "template": "challenge",
    "actingVersion": 16,
    "hex": "00",
    "fields": {
      "clusterSessionId": 1,
      "correlationId": 1,
      "encodedChallenge": {"ref": "missing"}
    }
  }]
}`
	const unknownTemplate = `{
  "schemaVersion": 1,
  "blobs": {},
  "fixtures": [{
    "name": "bad",
    "template": "nope",
    "actingVersion": 1,
    "hex": "00",
    "fields": {}
  }]
}`

	cases := []struct {
		name    string
		write   bool
		content string
		wantErr string
	}{
		{name: "missingFile", write: false, wantErr: "read catalog:"},
		{name: "malformed", write: true, content: "{", wantErr: "parse catalog:"},
		{
			name:    "emptyFixtures",
			write:   true,
			content: `{"schemaVersion":1,"blobs":{},"fixtures":[]}`,
			wantErr: "fixtures must be non-empty",
		},
		{
			name:    "unknownBlobRef",
			write:   true,
			content: challengeBadRef,
			wantErr: `unknown blob ref "missing"`,
		},
		{
			name:    "unknownTemplate",
			write:   true,
			content: unknownTemplate,
			wantErr: `unknown template "nope"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "catalog.json")
			if tc.write {
				require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o644))
			}
			_, err := regenCatalogFromFile(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestRegenCatalogFromFile_refreshesStaleHex(t *testing.T) {
	t.Parallel()
	src := committedCatalogPath(t)
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	catalog, err := catalogschema.ParseJSON(data)
	require.NoError(t, err)
	require.NotEmpty(t, catalog.Fixtures)

	stale := filepath.Join(t.TempDir(), "stale.json")
	catalog.Fixtures[0].Hex = "00"
	out, err := catalogschema.MarshalCanonical(catalog)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(stale, out, 0o644))

	regen, err := regenCatalogFromFile(stale)
	require.NoError(t, err)
	require.NotEqual(t, "00", regen.Fixtures[0].Hex)
	require.Equal(t, catalog.Fixtures[0].Name, regen.Fixtures[0].Name)
}

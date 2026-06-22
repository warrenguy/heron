//go:build integration && cluster_sbe

package clustersbe_test

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

func TestCatalog_parseAndWireParity(t *testing.T) {
	catalog, err := catalogschema.ParseJSON(clustersbe.CatalogJSONForTest())
	require.NoError(t, err)

	byName := fixturesByName()
	require.Len(t, catalog.Fixtures, len(byName))
	for _, entry := range catalog.Fixtures {
		require.NotEmpty(t, entry.Name)
		require.NotEmpty(t, entry.Template)
		require.NotEmpty(t, entry.Hex)
		require.NotNil(t, entry.Fields)
		wire := byName[entry.Name]
		require.Equal(t, wire.Hex, entry.Hex)
		require.Equal(t, catalogschema.FixtureMaxVarLen(entry), wire.MaxVarLen(), entry.Name)
	}
}

func TestCatalog_matchesGeneratorOutput(t *testing.T) {
	require.Equal(t, clustersbe.CatalogJSONForTest(), runCatalogGen(t))
}

func runCatalogGen(t *testing.T) []byte {
	t.Helper()
	root := modRoot(t)
	out := filepath.Join(t.TempDir(), "catalog.json")
	catalogPath := filepath.Join(
		root, "test/integration/cluster_sbe/clustersbe/fixtures/catalog.json",
	)
	cmd := exec.Command(
		"go", "run", "./tools/cluster-sbe-catalog-gen", "-catalog", catalogPath, "-out", out,
	)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	return data
}

// modRoot duplicated in catalog_regen_test.go (integration build tags prevent sharing).
func modRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	require.NoError(t, err)
	return filepath.Dir(strings.TrimSpace(string(out)))
}

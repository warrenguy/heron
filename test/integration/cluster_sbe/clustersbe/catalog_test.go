//go:build integration && cluster_sbe

package clustersbe_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

// catalogGenBin is the catalog generator compiled once in TestMain, and
// catalogFixture is the committed catalog its output is checked against. Building
// the tool here, before m.Run() arms go test's package-wide -timeout, keeps the
// go-toolchain compile out of that window, where a cold build cache on a loaded
// runner could otherwise eat into the budget shared by every test in the package.
var (
	catalogGenBin  string
	catalogFixture string
)

func TestMain(m *testing.M) {
	code, err := buildCatalogGen(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(code)
}

func buildCatalogGen(m *testing.M) (int, error) {
	gomod, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return 0, fmt.Errorf("go env GOMOD: %w", err)
	}
	root := filepath.Dir(strings.TrimSpace(string(gomod)))
	catalogFixture = filepath.Join(
		root, "test/integration/cluster_sbe/clustersbe/fixtures/catalog.json",
	)

	dir, err := os.MkdirTemp("", "catalog-gen")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	catalogGenBin = filepath.Join(dir, "cluster-sbe-catalog-gen")
	build := exec.Command("go", "build", "-o", catalogGenBin, "./tools/cluster-sbe-catalog-gen")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		return 0, fmt.Errorf("build cluster-sbe-catalog-gen: %w\n%s", err, out)
	}
	return m.Run(), nil
}

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
	out := filepath.Join(t.TempDir(), "catalog.json")
	output, err := exec.Command(
		catalogGenBin, "-catalog", catalogFixture, "-out", out,
	).CombinedOutput()
	require.NoError(t, err, "%s", output)
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	return data
}

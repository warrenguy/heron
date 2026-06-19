package catalogschema

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTemplateCatalog_initMaps(t *testing.T) {
	t.Parallel()

	want := len(wireTemplateCatalog)
	require.Len(t, templateIDs, want)
	require.Len(t, templateFieldKeys, want)

	for _, w := range wireTemplateCatalog {
		id, ok := templateIDs[w.name]
		require.True(t, ok, "templateIDs missing %q", w.name)
		require.Equal(t, w.id, id)

		allowed, ok := templateFieldKeys[w.name]
		require.True(t, ok, "templateFieldKeys missing %q", w.name)
		require.Equal(t, fieldKeys(w.allowedFieldKeys...), allowed)
	}
}

func TestWireTemplateCatalog_fixtureCoverage(t *testing.T) {
	t.Parallel()

	catalog, err := ParseJSON(loadCommittedCatalogJSON(t))
	require.NoError(t, err)
	require.NoError(t, Validate(catalog))

	catalogTemplates := make(map[string]struct{})
	for _, entry := range catalog.Fixtures {
		catalogTemplates[entry.Template] = struct{}{}
	}

	for _, w := range wireTemplateCatalog {
		require.Contains(
			t,
			catalogTemplates,
			w.name,
			"wireTemplateCatalog template %q has no fixture in catalog.json",
			w.name,
		)
	}

	// Validate already rejects unknown fixture templates via TemplateIDFromName;
	// equal set sizes plus the forward loop above imply bidirectional coverage.
	require.Len(t, catalogTemplates, len(wireTemplateCatalog))
}

func loadCommittedCatalogJSON(t *testing.T) []byte {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	path := filepath.Join(filepath.Dir(filename), "..", "fixtures", "catalog.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

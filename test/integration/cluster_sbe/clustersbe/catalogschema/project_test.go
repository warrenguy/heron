package catalogschema_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

func mutateFieldMap(base func() map[string]any, mut func(map[string]any)) map[string]any {
	fields := base()
	mut(fields)
	return fields
}

func TestProjectCatalogToFields_embeddedCatalogParity(t *testing.T) {
	catalog, err := catalogschema.ParseJSON(clustersbe.CatalogJSONForTest())
	require.NoError(t, err)
	require.NoError(t, catalogschema.Validate(catalog))
	blobs := catalogschema.ResolveBlobs(catalog.Blobs)
	byName := map[string]clustersbe.WireFixture{}
	for _, fixture := range clustersbe.Fixtures() {
		byName[fixture.Name] = fixture
	}
	for _, entry := range catalog.Fixtures {
		t.Run(entry.Name, func(t *testing.T) {
			t.Parallel()
			got, err := catalogschema.ProjectCatalogToFields(
				entry.Template, entry.ActingVersion, entry.Fields, blobs,
			)
			require.NoError(t, err)
			require.Equal(t, byName[entry.Name].Fields, got)
		})
	}
}

func TestProjectCatalogToFields_decodeErrors(t *testing.T) {
	t.Parallel()
	_, err := catalogschema.ProjectCatalogToFields("notATemplate", 0, nil, nil)
	require.Error(t, err)
	require.Equal(t, `unknown template "notATemplate"`, err.Error())
}

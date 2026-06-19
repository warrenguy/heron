package catalogschema_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

func mutateFieldMap(base func() map[string]any, mut func(map[string]any)) map[string]any {
	fields := base()
	mut(fields)
	return fields
}

func TestProjectCatalogToFields_decodeErrors(t *testing.T) {
	t.Parallel()
	_, err := catalogschema.ProjectCatalogToFields("notATemplate", 0, nil, nil)
	require.Error(t, err)
	require.Equal(t, `unknown template "notATemplate"`, err.Error())
}

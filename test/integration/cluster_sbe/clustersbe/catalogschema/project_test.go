package catalogschema_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

func mutateFieldMap(base func() map[string]any, mut func(map[string]any)) map[string]any {
	fields := base()
	if mut != nil {
		mut(fields)
	}
	return fields
}

func TestProjectCatalogToFields_decodeErrors(t *testing.T) {
	t.Parallel()
	_, err := catalogschema.ProjectCatalogToFields("notATemplate", 0, nil, nil)
	require.Error(t, err)
	require.Equal(t, `unknown template "notATemplate"`, err.Error())
}

func TestProjectCatalogToFields_newLeaderEvent_ingressEndpoints(t *testing.T) {
	t.Parallel()

	specs := testBlobSpecs()
	specs["ingress2000"] = ingress2000BlobSpec()
	blobs := catalogschema.ResolveBlobs(specs)

	baseFields := func(ingress any) map[string]any {
		return map[string]any{
			"leadershipTermId": json.Number("6"),
			"clusterSessionId": json.Number("7"),
			"leaderMemberId":   json.Number("8"),
			"ingressEndpoints": ingress,
		}
	}

	cases := []struct {
		name  string
		value any
		want  []byte
	}{
		{
			name:  "plainString",
			value: "localhost:40123",
			want:  []byte("localhost:40123"),
		},
		{
			name:  "ref",
			value: map[string]any{"ref": "ingress2000"},
			want:  blobs["ingress2000"],
		},
		{
			name:  "hex",
			value: map[string]any{"hex": "0102ab"},
			want:  []byte{0x01, 0x02, 0xab},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := catalogschema.ProjectCatalogToFields(
				"newLeaderEvent",
				clusterwire.SessionMessageHeaderSbeSchemaVersion,
				baseFields(tc.value),
				blobs,
			)
			require.NoError(t, err)
			fields, ok := got.(codecs.NewLeaderEventFields)
			require.True(t, ok)
			require.Equal(t, tc.want, fields.IngressEndpoints)
		})
	}
}

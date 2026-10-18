package logbuffer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateMetadataDimensions_drift(t *testing.T) {
	err := validateMetadataDimensions(65536, 4096, 65536, 8192)
	require.EqualError(
		t,
		err,
		"logbuffer: metadata layout drift: header term 65536 page 4096 != live term 65536 page 8192",
	)
}

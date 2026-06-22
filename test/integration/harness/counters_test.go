//go:build integration

package harness

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
)

func TestPublicationLimitCounterValue_counterNotFound(t *testing.T) {
	t.Parallel()
	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	capture := NewCaptureFailTB(t)
	PublicationLimitCounterValue(capture, reader, 9001)
	require.True(t, capture.Failed())
	require.Equal(
		t,
		fmt.Sprintf("publication limit counter not found for registration %d", 9001),
		capture.lastMsg,
	)
}

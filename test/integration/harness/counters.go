//go:build integration

package harness

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
)

// PublicationLimitCounterValue resolves the driver publisher-limit counter for
// registrationID via FindHeartbeatCounter and DriverPublisherLimitTypeID.
func PublicationLimitCounterValue(
	t testing.TB,
	reader *cnc.CountersReader,
	registrationID int64,
) (int32, int64) {
	t.Helper()

	limitID, ok := reader.FindHeartbeatCounter(registrationID, cnc.DriverPublisherLimitTypeID)
	if !ok {
		require.FailNow(
			t,
			fmt.Sprintf(
				"publication limit counter not found for registration %d",
				registrationID,
			),
		)
		return cnc.NullCounterID, 0
	}
	driverLimit, err := reader.CounterValue(limitID)
	require.NoError(t, err)
	return limitID, driverLimit
}

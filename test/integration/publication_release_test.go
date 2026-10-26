//go:build integration

package integration_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/test/integration/harness"
)

// publicationReleaseTimeout bounds each wait on the client and the driver.
// The driver frees an IPC publication about two timer checks after its last
// REMOVE_PUBLICATION.
const publicationReleaseTimeout = 5 * time.Second

// requireReadyPublicationReleased waits until ready reports true, runs
// extraTicks more client ticks, then calls end. It asserts the client and the
// driver drop the publication of add id.
func requireReadyPublicationReleased(
	t *testing.T,
	client *aeron.Client,
	hadClientError *atomic.Bool,
	id int64,
	ready func() bool,
	extraTicks int,
	end func(),
) {
	t.Helper()
	counters, err := client.CountersReader()
	require.NoError(t, err)

	// The client reads the ready reply before the owner polls the add.
	harness.PollClientUntilTransportClean(
		t,
		client,
		hadClientError,
		publicationReleaseTimeout,
		"client error before ready",
		func() bool { return ready() },
		func() string { return fmt.Sprintf("publication %d not ready", id) },
	)
	limitID, _ := harness.PublicationLimitCounterValue(t, counters, id)
	for range extraTicks {
		client.DoWork()
	}

	end()

	require.False(t, ready(), "client kept publication %d", id)
	harness.PollClientUntilTransportClean(
		t,
		client,
		hadClientError,
		publicationReleaseTimeout,
		"client error after end",
		func() bool {
			return !counters.IsCounterActive(limitID, id, cnc.DriverPublisherLimitTypeID)
		},
		func() string {
			return fmt.Sprintf("driver kept publisher-limit counter for %d", id)
		},
	)
}

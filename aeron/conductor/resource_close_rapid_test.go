//go:build rapid

package conductor_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

type closeAllRapidOpKind int

const (
	closeAllRapidOpInsertPublication closeAllRapidOpKind = iota
	closeAllRapidOpInsertSubscription
)

var closeAllRapidOps = []closeAllRapidOpKind{
	closeAllRapidOpInsertPublication,
	closeAllRapidOpInsertSubscription,
}

func drawCloseAllRapidUniqueRegID(
	rt *rapid.T,
	used map[int64]struct{},
	label string,
) int64 {
	rt.Helper()

	return drawPendingRapidUniqueCorrelationID(rt, rapidMapContains(used), label)
}

func assertCloseOrderPublicationsBeforeSubscriptions(
	rt *rapid.T,
	closeOrder []string,
) {
	rt.Helper()

	firstSubIdx := len(closeOrder)
	for index, kind := range closeOrder {
		if kind == "subscription" {
			firstSubIdx = index
			break
		}
	}
	for index := range firstSubIdx {
		require.Equal(rt, "publication", closeOrder[index])
	}
	for index := firstSubIdx; index < len(closeOrder); index++ {
		require.Equal(rt, "subscription", closeOrder[index])
	}
}

func TestConductor_CloseAllResources_ordering_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		lifecycle := &closeOrderResourceLifecycle{}
		conductorInstance, _ := newConductorWithCloseLingerForTest(t, 0, lifecycle)

		usedRegIDs := make(map[int64]struct{})
		stepCount := rapid.IntRange(1, 16).Draw(rt, "stepCount")
		var publicationCount int
		var subscriptionCount int

		for step := range stepCount {
			regID := drawCloseAllRapidUniqueRegID(
				rt,
				usedRegIDs,
				fmt.Sprintf("regID%d", step),
			)
			usedRegIDs[regID] = struct{}{}

			switch rapid.SampledFrom(closeAllRapidOps).Draw(rt, fmt.Sprintf("op%d", step)) {
			case closeAllRapidOpInsertPublication:
				conductorInstance.InsertPublicationForTest(
					regID,
					stubPublication{registrationID: regID},
				)
				publicationCount++
			case closeAllRapidOpInsertSubscription:
				conductorInstance.InsertSubscriptionForTest(
					regID,
					&stubResourceSubscription{registrationID: regID},
				)
				subscriptionCount++
			}
		}

		conductorInstance.CloseAllResources()

		assertCloseOrderPublicationsBeforeSubscriptions(rt, lifecycle.closeOrder)
		require.Equal(rt, publicationCount, lifecycle.retirePublicationCalls)
		require.Equal(rt, subscriptionCount, lifecycle.retireSubscriptionCalls)
		require.Equal(rt, 0, conductorInstance.PublicationCountForTest())
		require.Equal(rt, 0, conductorInstance.SubscriptionCountForTest())
	})
}

func TestConductor_CloseResource_idempotent_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance, _ := newConductorWithCloseLingerForTest(t, 0, lifecycle)
		ring := conductorInstance.DriverProxyForTest().Ring()

		regID := rapid.Int64().Draw(rt, "regID")
		closeCount := rapid.IntRange(1, 8).Draw(rt, "closeCount")
		isPublication := rapid.Bool().Draw(rt, "isPublication")

		var removeTypeID int
		if isPublication {
			removeTypeID = conductor.RemovePublication
			conductorInstance.InsertPublicationForTest(
				regID,
				stubPublication{registrationID: regID},
			)
			pub := stubPublication{registrationID: regID}
			for range closeCount {
				require.NoError(rt, conductorInstance.ClosePublication(pub))
			}
			require.Equal(rt, 1, lifecycle.retirePublicationCalls)
			require.Equal(rt, 0, conductorInstance.PublicationCountForTest())
		} else {
			removeTypeID = conductor.RemoveSubscription
			conductorInstance.InsertSubscriptionForTest(
				regID,
				&stubResourceSubscription{registrationID: regID},
			)
			sub := &stubResourceSubscription{registrationID: regID}
			for range closeCount {
				require.NoError(rt, conductorInstance.CloseSubscription(sub))
			}
			require.Equal(rt, 1, lifecycle.retireSubscriptionCalls)
			require.Equal(rt, 0, conductorInstance.SubscriptionCountForTest())
		}

		require.Equal(rt, []int{removeTypeID}, testutil.CommittedRecordTypeIDs(ring))
	})
}

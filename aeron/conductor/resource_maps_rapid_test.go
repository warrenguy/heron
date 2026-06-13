//go:build rapid

package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
)

type resourceMapOpKind int

const (
	resourceMapOpInsertLive resourceMapOpKind = iota
	resourceMapOpInsertNil
	resourceMapOpRemove
	resourceMapOpLookup
)

var (
	resourceMapOpsBase = []resourceMapOpKind{
		resourceMapOpInsertLive,
		resourceMapOpInsertNil,
		resourceMapOpLookup,
	}
	resourceMapOpsWithRemove = []resourceMapOpKind{
		resourceMapOpInsertLive,
		resourceMapOpInsertNil,
		resourceMapOpRemove,
		resourceMapOpLookup,
	}
)

type resourceMapRapidHarness struct {
	insertLive func(conductorInstance *conductor.Conductor, regID, stubRegID int64)
	insertNil  func(conductorInstance *conductor.Conductor, regID int64)
	remove     func(conductorInstance *conductor.Conductor, regID int64)
	lookup     func(conductorInstance *conductor.Conductor, regID int64) (int64, bool)
	count      func(conductorInstance *conductor.Conductor) int
}

func drawResourceMapOp(rt *rapid.T, model map[int64]int64) resourceMapOpKind {
	rt.Helper()

	if len(model) == 0 {
		return rapid.SampledFrom(resourceMapOpsBase).Draw(rt, "op")
	}
	return rapid.SampledFrom(resourceMapOpsWithRemove).Draw(rt, "op")
}

func pickExistingRegID(rt *rapid.T, model map[int64]int64) int64 {
	rt.Helper()

	keys := make([]int64, 0, len(model))
	for regID := range model {
		keys = append(keys, regID)
	}
	idx := rapid.IntRange(0, len(keys)-1).Draw(rt, "existingIdx")
	return keys[idx]
}

func assertResourceMapInvariants(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	model map[int64]int64,
	harness resourceMapRapidHarness,
) {
	rt.Helper()

	require.Equal(rt, len(model), harness.count(conductorInstance))
	for regID, wantRegID := range model {
		gotRegID, ok := harness.lookup(conductorInstance, regID)
		require.True(rt, ok)
		require.Equal(rt, wantRegID, gotRegID)
	}
}

func runResourceMapOpsRapid(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	harness resourceMapRapidHarness,
) {
	rt.Helper()

	model := make(map[int64]int64)
	stepCount := rapid.IntRange(1, 32).Draw(rt, "stepCount")

	for range stepCount {
		switch op := drawResourceMapOp(rt, model); op {
		case resourceMapOpInsertLive:
			regID := rapid.Int64().Draw(rt, "regID")
			stubRegID := rapid.Int64().Draw(rt, "stubRegID")
			harness.insertLive(conductorInstance, regID, stubRegID)
			model[regID] = stubRegID
		case resourceMapOpInsertNil:
			regID := rapid.Int64().Draw(rt, "nilRegID")
			harness.insertNil(conductorInstance, regID)
		case resourceMapOpRemove:
			regID := pickExistingRegID(rt, model)
			harness.remove(conductorInstance, regID)
			delete(model, regID)
		case resourceMapOpLookup:
			regID := rapid.Int64().Draw(rt, "lookupRegID")
			gotRegID, ok := harness.lookup(conductorInstance, regID)
			wantRegID, exists := model[regID]
			if exists {
				require.True(rt, ok)
				require.Equal(rt, wantRegID, gotRegID)
			} else {
				require.False(rt, ok)
			}
		}

		assertResourceMapInvariants(rt, conductorInstance, model, harness)
	}
}

func lookupRegistrationID(
	lookup func(int64) (conductor.PublicationHandle, bool),
	regID int64,
) (int64, bool) {
	handle, ok := lookup(regID)
	if !ok {
		return 0, false
	}
	return handle.RegistrationID(), true
}

func TestConductor_resourceMap_operations_rapid(t *testing.T) {
	cases := []struct {
		name    string
		harness resourceMapRapidHarness
	}{
		{
			name: "publication",
			harness: resourceMapRapidHarness{
				insertLive: func(conductorInstance *conductor.Conductor, regID, stubRegID int64) {
					conductorInstance.InsertPublicationForTest(
						regID,
						stubPublication{registrationID: stubRegID},
					)
				},
				insertNil: func(conductorInstance *conductor.Conductor, regID int64) {
					conductorInstance.InsertNilPublicationForTest(regID)
				},
				remove: func(conductorInstance *conductor.Conductor, regID int64) {
					conductorInstance.RemovePublicationForTest(regID)
				},
				lookup: func(conductorInstance *conductor.Conductor, regID int64) (int64, bool) {
					return lookupRegistrationID(conductorInstance.PublicationByRegID, regID)
				},
				count: func(conductorInstance *conductor.Conductor) int {
					return conductorInstance.PublicationCountForTest()
				},
			},
		},
		{
			name: "subscription",
			harness: resourceMapRapidHarness{
				insertLive: func(conductorInstance *conductor.Conductor, regID, stubRegID int64) {
					conductorInstance.InsertSubscriptionForTest(
						regID,
						stubSubscription{registrationID: stubRegID, streamID: 0},
					)
				},
				insertNil: func(conductorInstance *conductor.Conductor, regID int64) {
					conductorInstance.InsertNilSubscriptionForTest(regID)
				},
				remove: func(conductorInstance *conductor.Conductor, regID int64) {
					conductorInstance.RemoveSubscriptionForTest(regID)
				},
				lookup: func(conductorInstance *conductor.Conductor, regID int64) (int64, bool) {
					handle, ok := conductorInstance.SubscriptionByRegID(regID)
					if !ok {
						return 0, false
					}
					return handle.RegistrationID(), true
				},
				count: func(conductorInstance *conductor.Conductor) int {
					return conductorInstance.SubscriptionCountForTest()
				},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				conductorInstance := newBareConductorForResourceMapTest(t)
				runResourceMapOpsRapid(rt, conductorInstance, testCase.harness)
			})
		})
	}
}
